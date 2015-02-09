using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CSharp.RuntimeBinder;

namespace Alluvial
{
    internal class SingleStreamCatchup<TData> : IStreamCatchup<TData>
    {
        private int isRunning;
        private readonly IStream<TData> stream;
        private readonly int? batchCount;

        private readonly ConcurrentDictionary<Type, IAggregatorSubscription> aggregatorSubscriptions = new ConcurrentDictionary<Type, IAggregatorSubscription>();

        public SingleStreamCatchup(
            IStream<TData> stream,
            int? batchCount = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            this.stream = stream;
            this.batchCount = batchCount;
        }

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

        public async Task<ICursor> RunSingleBatch()
        {
            if (Interlocked.CompareExchange(ref isRunning, 1, 0) != 0)
            {
                // FIX: (RunSingleBatch): what would be a better behavior here?
                return stream.NewCursor();
            }

            var cursors = await GetCursorProjections(stream.Id);

            var cursor = cursors.Values.Minimum();
            var query = stream.CreateQuery(cursor, batchCount);

            var batch = await query.NextBatch();

            if (batch.Count > 0)
            {
                var aggregatorUpdates = aggregatorSubscriptions
                    .Values
                    .Select(subscription =>
                    {
                        try
                        {
                            return Aggregate((dynamic) subscription,
                                             batch,
                                             query.Cursor,
                                             cursors);
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

            isRunning = 0;

            return query.Cursor;
        }

        private async Task<IDictionary<Type, ICursor>> GetCursorProjections(string streamId)
        {
            return (await aggregatorSubscriptions.Where(p => p.Value.IsCursor)
                                                 .Select(async p => new
                                                 {
                                                     p.Key,
                                                     CursorProjection = await p.Value.GetProjection(streamId)
                                                 })
                                                 .AwaitAll())
                .ToDictionary(p => p.Key, p => (ICursor) p.CursorProjection);
        }

        private async Task Aggregate<TProjection>(
            AggregatorSubscription<TProjection, TData> subscription,
            IStreamBatch<TData> batch,
            ICursor queryCursor,
            IDictionary<Type, ICursor> projectionCursors)
        {
            ICursor projectionCursor;
            TProjection projection;

            if (!projectionCursors.TryGetValue(typeof (TProjection), out projectionCursor))
            {
                projection = await subscription.ProjectionStore.Get(stream.Id);
                projectionCursor = projection as ICursor;
            }
            else
            {
                projection = (TProjection) projectionCursor;
            }

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

            await subscription.ProjectionStore.Put(stream.Id, projection);
        }
    }
}