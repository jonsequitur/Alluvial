using System;
using System.Threading.Tasks;

namespace Alluvial
{
    public static class Catchup
    {
        public static IDataStreamCatchup<TData> Create<TData>(
            IDataStream<IDataStream<TData>> source,
            ICursor cursor = null,
            int? batchCount = null)
        {
            return new DataStreamCatchup<TData>(source, cursor, batchCount);
        }

        public static async Task<IStreamQuery<IDataStream<TData>>> RunUntilCaughtUp<TData>(this IDataStreamCatchup<TData> catchup)
        {
            IStreamQuery<IDataStream<TData>> query;
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

        public static IDataStreamCatchup<TData> Subscribe<TProjection, TData>(
            this IDataStreamCatchup<TData> catchup,
            IDataStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore)
        {
            catchup.SubscribeAggregator(aggregator, projectionStore);
            return catchup;
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IDataStreamCatchup<TData> catchup,
            Aggregate<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
          return  catchup.SubscribeAggregator(Aggregator.Create(aggregate), projectionStore);
        }

        public static IDisposable Subscribe<TProjection, TData>(
            this IDataStreamCatchup<TData> catchup,
            Action<TProjection, IStreamQueryBatch<TData>> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
          return  catchup.SubscribeAggregator(Aggregator.Create(aggregate), projectionStore);
        }

        internal class Progress<TData>
        {
            public Progress<TData> Count(IStreamQueryBatch<TData> batch)
            {
                AggregatedCount += batch.Count;
                return this;
            }

            public int AggregatedCount { get; private set; }
        }
    }
}