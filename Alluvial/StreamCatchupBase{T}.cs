using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial
{
    internal abstract class StreamCatchupBase<TData, TCursorPosition> : IStreamCatchup<TData, TCursorPosition>
    {
        private int isRunning;

        protected int? batchCount;

        protected readonly ConcurrentDictionary<Type, IAggregatorSubscription> aggregatorSubscriptions = new ConcurrentDictionary<Type, IAggregatorSubscription>();

        public IDisposable SubscribeAggregator<TProjection>(
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSaveProjection<TProjection> fetchAndSaveProjection)
        {
            var added = aggregatorSubscriptions.TryAdd(typeof (TProjection),
                                                       new AggregatorSubscription<TProjection, TData>(aggregator, fetchAndSaveProjection));

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

        public abstract Task<ICursor<TCursorPosition>> RunSingleBatch();

        protected async Task<ICursor<TCursorPosition>> RunSingleBatch<TCursor>(IStream<TData, TCursor> stream)
        {
            if (Interlocked.CompareExchange(ref isRunning, 1, 0) != 0)
            {
                // FIX: (RunSingleBatch): what would be a better behavior here? awaiting the running batch without triggering a new run might be best.
                return new Cursor<TCursorPosition>();
            }

            ICursor<TCursorPosition> upstreamCursor = null;

            var projections = new ConcurrentBag<object>();
            var tcs = new TaskCompletionSource<AggregationBatch<TCursor>>();

            Action runQuery = async () =>
            {
                var cursor = projections.OfType<ICursor<TCursor>>().Minimum();
                upstreamCursor = cursor as ICursor<TCursorPosition>;
                var query = stream.CreateQuery(cursor, batchCount);

                try
                {
                    var batch = await query.NextBatch();

                    tcs.SetResult(new AggregationBatch<TCursor>
                    {
                        Cursor = query.Cursor,
                        Batch = batch
                    });
                }
                catch (Exception exception)
                {
                    tcs.SetException(exception);
                }
            };

            Func<object, Task<AggregationBatch<TCursor>>> awaitData = c =>
            {
                projections.Add(c);

                if (projections.Count >= aggregatorSubscriptions.Count)
                {
                    runQuery();
                }

                return tcs.Task;
            };

            var aggregationTasks = aggregatorSubscriptions
                .Values
                .Select(v => Aggregate(stream, (dynamic) v, awaitData) as Task);

            await Task.WhenAll(aggregationTasks);

            isRunning = 0;

            return upstreamCursor;
        }

        private static Task Aggregate<TProjection, TCursor>(
            IStream<TData, TCursor> stream,
            AggregatorSubscription<TProjection, TData> subscription,
            Func<object, Task<AggregationBatch<TCursor>>> getData)
        {
            return subscription.FetchAndSaveProjection(
                stream.Id,
                async projection =>
                {
                    var aggregationBatch = await getData(projection);

                    var data = aggregationBatch.Batch;

                    projection = await subscription.Aggregator.Aggregate(projection, data);

                    var cursor = projection as ICursor<TCursor>;
                    if (cursor != null)
                    {
                        cursor.AdvanceTo(aggregationBatch.Cursor.Position);
                    }

                    return projection;
                });
        }
        
        protected struct AggregationBatch<TCursor>
        {
            public ICursor<TCursor> Cursor;
            public IStreamBatch<TData> Batch;
        }
    }
}