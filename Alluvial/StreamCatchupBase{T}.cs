using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial
{
    internal abstract class StreamCatchupBase<TData> : IStreamCatchup<TData>
    {
        private int isRunning;

        protected int? batchCount;

        private readonly ConcurrentDictionary<Type, IAggregatorSubscription> aggregatorSubscriptions = new ConcurrentDictionary<Type, IAggregatorSubscription>();

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

        public abstract Task<ICursor> RunSingleBatch();

        protected async Task<ICursor> RunSingleBatch(IStream<TData> stream)
        {
            if (Interlocked.CompareExchange(ref isRunning, 1, 0) != 0)
            {
                // FIX: (RunSingleBatch): what would be a better behavior here?
                return stream.NewCursor();
            }

            ICursor upstreamCursor = null;

            var projections = new ConcurrentBag<object>();
            var tcs = new TaskCompletionSource<AggregationBatch>();

            Action runQuery = async () =>
            {
                var cursor = projections.OfType<ICursor>().Minimum();
                var query = stream.CreateQuery(cursor, batchCount);
                upstreamCursor = query.Cursor;

                try
                {
                    var batch = await query.NextBatch();

                    tcs.SetResult(new AggregationBatch
                    {
                        UpstreamCursor = upstreamCursor,
                        Batch = batch
                    });
                }
                catch (Exception exception)
                {
                    tcs.SetException(exception);
                }
            };

            Func<object, Task<AggregationBatch>> awaitData = c =>
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

        private static Task Aggregate<TProjection>(
            IStream<TData> stream,
            AggregatorSubscription<TProjection, TData> subscription,
            Func<object, Task<AggregationBatch>> getData)
        {
            return subscription.FetchAndSaveProjection(
                stream.Id,
                async projection =>
                {
                    var aggregationBatch = await getData(projection);

                    var data = aggregationBatch.Batch;

                    var cursor = projection as ICursor;
                    if (cursor != null && !(cursor.Position is Cursor.StartingPosition))
                    {
                       // FIX data = data.Prune(cursor);
                    }

                    projection = await subscription.Aggregator.Aggregate(projection, data);

                    if (cursor != null)
                    {
                        cursor.AdvanceTo(aggregationBatch.UpstreamCursor.Position);
                    }

                    return projection;
                });
        }

        protected struct AggregationBatch
        {
            public ICursor UpstreamCursor;
            public IStreamBatch<TData> Batch;
        }
    }
}