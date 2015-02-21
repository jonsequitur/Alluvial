using System;
using System.Threading.Tasks;
using Alluvial.Tests;

namespace Alluvial
{
    public static class StreamCatchup
    {
        public static IStreamCatchup<TData> Distribute<TData>(
            IStream<IStream<TData>> stream,
            FetchAndSaveProjection<ICursor> manageCursor,
            int? batchCount = null) 
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData>>(stream, batchCount);

            return new DistributorCatchup<TData>(
                upstreamCatchup,
                manageCursor);
        }

        public static IStreamCatchup<TData> Distribute<TData>(
            IStream<IStream<TData>> stream,
            ICursor cursor = null, 
            int? batchCount = null)
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData>>(stream, batchCount);

            return new DistributorCatchup<TData>(
                upstreamCatchup,
                cursor ?? stream.NewCursor());
        }

        public static IStreamCatchup<TData> Create<TData>(
            IStream<TData> stream,
            int? batchCount = null)
        {
            return new SingleStreamCatchup<TData>(
                stream,
                batchCount);
        }

        /// <summary>
        /// Runs the catchup query until it reaches an empty batch, then stops.
        /// </summary>
        public static async Task<ICursor> RunUntilCaughtUp<TData>(this IStreamCatchup<TData> catchup)
        {
            ICursor cursor;
            var counter = new Counter<TData>();

            using (catchup.Subscribe(async (_, batch) => counter.Count(batch), NoCursor(counter)))
            {
                int countBefore;
                do
                {
                    countBefore = counter.TotalCount;
                    cursor = await catchup.RunSingleBatch();
                } while (countBefore != counter.TotalCount);
            }

            return cursor;
        }

        private static FetchAndSaveProjection<TProjection> NoCursor<TProjection>(TProjection projection)
        {
            return async (streamId, aggregate) =>
            {
                await aggregate(projection, Cursor.None());
            };
        }

        public static IDisposable Poll<TData>(
            this IStreamCatchup<TData> catchup,
            TimeSpan pollInterval)
        {
            var canceled = false;

            Task.Run(async () =>
            {
                while (!canceled)
                {
                    await catchup.RunUntilCaughtUp();
                    await Task.Delay(pollInterval);
                }
            });

            return Disposable.Create(() =>
            {
                canceled = true;
            });
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.Subscribe(aggregator,
                                     projectionStore.AsHandler());
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.Subscribe(Aggregator.Create(aggregate),
                                     projectionStore.AsHandler());
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            FetchAndSaveProjection<TProjection> manageProjection,
            HandleAggregatorError<TProjection> onError = null)
        {
            return catchup.Subscribe(Aggregator.Create(aggregate), manageProjection);
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSaveProjection<TProjection> manageProjection,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (onError != null)
            {
                manageProjection = manageProjection.Catch(onError);
            }

            return catchup.SubscribeAggregator(aggregator, manageProjection);
        }

        internal class Counter<TData>
        {
            public Counter<TData> Count(IStreamBatch<TData> batch)
            {
                TotalCount += batch.Count;
                return this;
            }

            public int TotalCount { get; private set; }
        }
    }

    public delegate void HandleAggregatorError<TProjection>(StreamCatchupError<TProjection> error);
}