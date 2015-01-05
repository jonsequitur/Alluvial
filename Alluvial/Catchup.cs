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

        public static async Task<IStreamQuery<IDataStream<TProjection>>> RunUntilCaughtUp<TProjection>(this IDataStreamCatchup<TProjection> catchup)
        {
            // FIX: (RunUntilCaughtUp) 

            return await catchup.RunSingleBatch();
        }

        public static IDataStreamCatchup<TData> Subscribe<TProjection, TData>(
            this IDataStreamCatchup<TData> catchup,
            IDataStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore)
        {
            catchup.SubscribeAggregator(aggregator, projectionStore);
            return catchup;
        }

        public static IDataStreamCatchup<TData> Subscribe<TProjection, TData>(
            this IDataStreamCatchup<TData> catchup,
            Aggregate<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore)
        {
            catchup.SubscribeAggregator(Aggregator.Create(aggregate), projectionStore);
            return catchup;
        }
    }
}