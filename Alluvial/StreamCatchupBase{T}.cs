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

            var downstreamCursors = new ConcurrentBag<ICursor>();
            var tcs = new TaskCompletionSource<AggregationBatch>();

            Action runQuery = async () =>
            {
                var cursor = downstreamCursors.Minimum();
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

            Func<ICursor, Task<AggregationBatch>> awaitData = c =>
            {
                downstreamCursors.Add(c);

                if (downstreamCursors.Count >= aggregatorSubscriptions.Count)
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

        private async Task Aggregate<TProjection>(
            IStream<TData> stream,
            AggregatorSubscription<TProjection, TData> subscription,
            Func<ICursor, Task<AggregationBatch>> getData)
        {
            await subscription.FetchAndSaveProjection(
                stream.Id,
                async (projection, cursor) =>
                {
                    var projectionCursor = projection as ICursor;
                    cursor = cursor ?? projectionCursor ?? stream.NewCursor();

                    var aggregationBatch = await getData(cursor);

                    var data = aggregationBatch.Batch;

                    if (projectionCursor != null &&
                        !(projectionCursor.Position is Cursor.StartingPosition))
                    {
                        // TODO: (Aggregate) optimize: this is unnecessary if we know we know this was a brand new projection
                        data = data.Prune(projectionCursor);
                    }

                    projection = await subscription.Aggregator.Aggregate(projection, data);

                    cursor.AdvanceTo(aggregationBatch.UpstreamCursor.Position);

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