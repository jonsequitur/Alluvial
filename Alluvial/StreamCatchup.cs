using System;
using System.Threading.Tasks;
using Alluvial.Tests;

namespace Alluvial
{
    public static class StreamCatchup
    {
        public static IStreamCatchup<TData, TUpstreamCursor> Distribute<TData, TUpstreamCursor, TDownstreamCursor>(
            IStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor> stream,
            FetchAndSaveProjection<ICursor<TUpstreamCursor>> manageCursor,
            int? batchCount = null)
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor>(stream, batchCount);

            return new DistributorCatchup<TData, TUpstreamCursor, TDownstreamCursor>(
                upstreamCatchup,
                manageCursor);
        }

        public static IStreamCatchup<TData, TUpstreamCursor> Distribute<TData, TUpstreamCursor, TDownstreamCursor>(
            IStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor> stream,
            ICursor<TUpstreamCursor> cursor = null,
            int? batchCount = null)
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor>(stream, batchCount);

            return new DistributorCatchup<TData, TUpstreamCursor, TDownstreamCursor>(
                upstreamCatchup,
                cursor ?? stream.NewCursor());
        }

        public static IStreamCatchup<TData, TCursor> Create<TData, TCursor>(
            IStream<TData, TCursor> stream,
            int? batchCount = null)
        {
            return new SingleStreamCatchup<TData, TCursor>(
                stream,
                batchCount);
        }

        /// <summary>
        /// Runs the catchup query until it reaches an empty batch, then stops.
        /// </summary>
        public static async Task<ICursor<TCursor>> RunUntilCaughtUp<TData, TCursor>(this IStreamCatchup<TData, TCursor> catchup)
        {
            ICursor<TCursor> cursor;
            var counter = new Counter<TData>();

            using (catchup.Subscribe(async (_, batch) => counter.Count(batch), NoCursor(counter)))
            {
                int countBefore;
                do
                {
                    countBefore = counter.Value;
                    cursor = await catchup.RunSingleBatch();
                } while (countBefore != counter.Value);
            }

            return cursor;
        }

        private static FetchAndSaveProjection<TProjection> NoCursor<TProjection>(TProjection projection)
        {
            return  (streamId, aggregate) => aggregate(projection);
        }

        public static IDisposable Poll<TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            TimeSpan pollInterval)
        {
            var canceled = false;

            Task.Run(async () =>
            {
                while (!canceled)
                {
                    await catchup.RunSingleBatch();
                    await Task.Delay(pollInterval);
                }
            });

            return Disposable.Create(() =>
            {
                canceled = true;
            });
        }

        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.Subscribe(aggregator,
                                     projectionStore.AsHandler());
        }

        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.Subscribe(Aggregator.Create(aggregate),
                                     projectionStore.AsHandler());
        }

        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            FetchAndSaveProjection<TProjection> manageProjection,
            HandleAggregatorError<TProjection> onError = null)
        {
            return catchup.Subscribe(Aggregator.Create(aggregate), manageProjection);
        }

        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
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


        internal class Counter<TCursor> : Projection<int>
        {
            public Counter<TCursor> Count(IStreamBatch<TCursor> batch)
            {
                Value += batch.Count;
                return this;
            }
        }
    }

    public delegate void HandleAggregatorError<TProjection>(StreamCatchupError<TProjection> error);
}