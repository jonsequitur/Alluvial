using System;
using System.Threading.Tasks;
using Alluvial.Tests;

namespace Alluvial
{
    public static class StreamCatchup
    {
        public static IStreamCatchup<TData> Distribute<TData>(
            IStream<IStream<TData>> stream,
            ICursor cursor = null, 
            int? batchCount = null,
            FetchAndSaveProjection<ICursor> manageCursor = null)
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData>>(stream, batchCount);

            return new DistributorCatchup<TData>(
                upstreamCatchup,
                cursor ?? stream.NewCursor(),
                manageCursor);
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
            var counter = new Progress<TData>();

            using (catchup.Subscribe(async (_, batch) => counter.Count(batch), NoCursor(counter)))
            {
                int countBefore;
                do
                {
                    countBefore = counter.AggregatedCount;
                    cursor = await catchup.RunSingleBatch();
                } while (countBefore != counter.AggregatedCount);
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
            return catchup.SubscribeAggregator(aggregator,
                                               projectionStore.AsHandler());
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.SubscribeAggregator(Aggregator.Create(aggregate),
                                               projectionStore.AsHandler());
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            FetchAndSaveProjection<TProjection> manageProjection)
        {
            return catchup.SubscribeAggregator(Aggregator.Create(aggregate), manageProjection);
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSaveProjection<TProjection> manageProjection)
        {
            return catchup.SubscribeAggregator(aggregator, manageProjection);
        }

        internal class Progress<TData>
        {
            public Progress<TData> Count(IStreamBatch<TData> batch)
            {
                AggregatedCount += batch.Count;
                return this;
            }

            public int AggregatedCount { get; private set; }
        }
    }
}