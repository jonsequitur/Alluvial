using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class StreamCatchup<TData> : IStreamCatchup<TData>
    {
        private int isRunning;
        private readonly IStream<IStream<TData>> stream;
        private readonly int? batchCount;
        private readonly GetCursor getCursor;
        private readonly StoreCursor storeCursor;

        private readonly ConcurrentDictionary<Type, AggregatorSubscription> aggregatorSubscriptions = new ConcurrentDictionary<Type, AggregatorSubscription>();

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
                AggregatorSubscription _;
                aggregatorSubscriptions.TryRemove(typeof (TProjection), out _);
            });
        }

        public async Task<IStreamIterator<IStream<TData>>> RunSingleBatch()
        {
            if (Interlocked.CompareExchange(ref isRunning, 1, 0) != 0)
            {
                var streamQuery = stream.CreateQuery(Alluvial.Cursor.ReadOnly(Cursor));
                return streamQuery;
            }

            await EnsureCursorIsInitialized();

            var upstreamQuery = stream.CreateQuery(Cursor, batchCount);

            var streams = await upstreamQuery.NextBatch();

            if (streams.Any())
            {
                Exception aggregatorException = null;

                var batches =
                    streams.Select(
                        s =>
                            // TODO: (RunSingleBatch) optimize: pull up the projection first and use its cursor?
                        {
                            var streamQuery = s.CreateQuery();

                            return streamQuery
                                .NextBatch()
                                .ContinueWith(async t =>
                                {
                                    var batch = t.Result;

                                    if (batch.Count > 0)
                                    {
                                        var aggregatorUpdates =
                                            aggregatorSubscriptions
                                                .Values
                                                .Select(subscription => Aggregate(s.Id,
                                                                                  (dynamic) subscription,
                                                                                  batch,
                                                                                  streamQuery.Cursor))
                                                .Cast<Task>();

                                        try
                                        {
                                            await Task.WhenAll(aggregatorUpdates);
                                        }
                                        catch (Exception exception)
                                        {
                                            aggregatorException = exception;
                                            throw;
                                        }
                                    }
                                });
                        });

                await Task.WhenAll(batches);

                if (aggregatorException != null)
                {
                    throw aggregatorException;
                }
            }

            await StoreCursor();

            isRunning = 0;

            return upstreamQuery;
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

            projection = subscription.Aggregator.Aggregate(projection, batch);

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