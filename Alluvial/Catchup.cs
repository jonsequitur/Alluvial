using System;
using System.Threading.Tasks;

namespace Alluvial
{
    public static class Catchup
    {
        public static IStreamCatchup<TData> Create<TData>(
            IStream<IStream<TData>> source,
            ICursor cursor = null,
            int? batchCount = null,
            Action<CatchupConfiguration> configure = null)
        {
            var configuration = new CatchupConfiguration();
            if (configure!=null)
            {
                configure(configuration);
            }

            return new StreamCatchup<TData>(
                source, 
                cursor, 
                batchCount,
                configuration.GetCursor,
                configuration.StoreCursor);
        }

        /// <summary>
        /// Runs the catchup query until it reaches an empty batch, then stops.
        /// </summary>
        public static async Task<IStreamIterator<IStream<TData>>> RunUntilCaughtUp<TData>(this IStreamCatchup<TData> catchup)
        {
            IStreamIterator<IStream<TData>> query;
            var counter = new Progress<TData>();

            using (catchup.Subscribe<Progress<TData>, TData>((_, batch) => counter.Count(batch)))
            {
                int countBefore;
                do
                {
                    countBefore = counter.AggregatedCount; 
                    query = await catchup.RunSingleBatch();
                } while (countBefore != counter.AggregatedCount);
            }

            return query;
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

        public static IStreamCatchup<TData> Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            catchup.SubscribeAggregator(aggregator, projectionStore);
            return catchup;
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            Aggregate<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.SubscribeAggregator(Aggregator.Create(aggregate), projectionStore);
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            Action<TProjection, IStreamBatch<TData>> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.SubscribeAggregator(Aggregator.Create(aggregate), projectionStore);
        }

        public static CatchupConfiguration StoreCursor(
            this CatchupConfiguration configuration,
            StoreCursor put)
        {
            configuration.StoreCursor = put;
            return configuration;
        }

        public static CatchupConfiguration GetCursor(
            this CatchupConfiguration configuration,
            GetCursor get)
        {
            configuration.GetCursor = get;
            return configuration;
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