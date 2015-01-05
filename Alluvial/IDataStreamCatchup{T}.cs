using System.Threading.Tasks;

namespace Alluvial
{
    public interface IDataStreamCatchup<TData>
    {
        void SubscribeAggregator<TProjection>(IDataStreamAggregator<TProjection, TData> aggregator,
                                              IProjectionStore<string, TProjection> projectionStore);

        Task<IStreamQuery<IDataStream<TData>>> RunSingleBatch();
    }
}