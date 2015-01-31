using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CSharp.RuntimeBinder;

namespace Alluvial
{
    internal class StreamCatchup<TData> : IStreamCatchup<TData>
    {
        private int isRunning;
        private readonly IStream<IStream<TData>> stream;
        private readonly int? batchCount;
        private readonly GetCursor getCursor;
        private readonly StoreCursor storeCursor;

        private readonly ConcurrentDictionary<Type, IAggregatorSubscription> aggregatorSubscriptions = new ConcurrentDictionary<Type, IAggregatorSubscription>();

        public StreamCatchup(
            IStream<IStream<TData>> stream,
            ICursor cursor = null,
            int? batchCount = null,
            GetCursor getCursor = null,
            StoreCursor storeCursor = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            Cursor = cursor;

            this.stream = stream;
            this.batchCount = batchCount;
            this.getCursor = getCursor;
            this.storeCursor = storeCursor;
        }

        public ICursor Cursor { get; set; }

        public IDisposable SubscribeAggregator<TProjection>(
            IStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            var added = aggregatorSubscriptions.TryAdd(typeof (TProjection),
                                                       new AggregatorSubscription<TProjection, TData>(aggregator, projectionStore));

            if (!added)
            {
                throw new InvalidOperationException(string.Format("Aggregator for projection of type {0} is already subscribed.", typeof (TProjection)));
            }

            return Disposable.Create(() =>
            {
                IAggregatorSubscription _;
                aggregatorSubscriptions.TryRemove(typeof (TProjection), out _);
            });
        }

        public async Task<IStreamIterator<IStream<TData>>> RunSingleBatch()
        {
            if (Interlocked.CompareExchange(ref isRunning, 1, 0) != 0)
            {
                var streamQuery = stream.CreateQuery(Cursor.ReadOnly());
                return streamQuery;
            }

            await EnsureCursorIsInitialized();

            var upstreamQuery = stream.CreateQuery(Cursor, batchCount);

            var streams = await upstreamQuery.NextBatch();

            if (streams.Any())
            {
                var batches =
                    streams.Select(
                        async s =>
                        {
                            var cursors = await GetCursorProjections(s.Id);

                            ICursor cursor = Alluvial.Cursor.Create(cursors.MinimumPosition());
                            var streamQuery = s.CreateQuery(cursor);

                            var batch = await streamQuery.NextBatch();

                            if (batch.Count > 0)
                            {
                                var aggregatorUpdates = aggregatorSubscriptions
                                    .Values
                                    .Select(subscription =>
                                    {
                                        try
                                        {
                                            return Aggregate(s.Id,
                                                             (dynamic) subscription,
                                                             batch,
                                                             streamQuery.Cursor);
                                        }
                                        catch (RuntimeBinderException exception)
                                        {
                                            throw new RuntimeBinderException(
                                                string.Format("Unable to call Aggregate, possibly due to {0} having non-public types for its generic parameters",
                                                              subscription), exception);
                                        }
                                    })
                                    .Cast<Task>();

                                await Task.WhenAll(aggregatorUpdates);
                            }
                        });

                await Task.WhenAll(batches);
            }

            await StoreCursor();

            isRunning = 0;

            return upstreamQuery;
        }

        private async Task<IEnumerable<ICursor>> GetCursorProjections(string streamId)
        {
            return (await aggregatorSubscriptions.Values
                                                 .Where(p => p.IsCursor)
                                                 .Select(p => p.GetProjection(streamId))
                                                 .AwaitAll())
                .Cast<ICursor>();
        }

        private async Task Aggregate<TProjection>(
            string streamId,
            AggregatorSubscription<TProjection, TData> subscription,
            IStreamBatch<TData> batch,
            ICursor queryCursor)
        {
            var projection = await subscription.ProjectionStore.Get(streamId);

            var projectionCursor = projection as ICursor;
            if (projectionCursor != null)
            {
                // TODO: (Aggregate) optimize: this is unnecessary if we know we know this was a brand new projection
                batch = batch.Prune(projectionCursor);
            }

            projection = await subscription.Aggregator.Aggregate(projection, batch);

            if (projectionCursor != null)
            {
                projectionCursor.AdvanceTo(queryCursor.Position);
            }

            await subscription.ProjectionStore.Put(streamId, projection);
        }

        private async Task EnsureCursorIsInitialized()
        {
            if (Cursor == null)
            {
                Cursor = await getCursor(stream.Id);
            }
        }

        private async Task StoreCursor()
        {
            await storeCursor(stream.Id, Cursor);
        }
    }
}