namespace Alluvial
{
    public interface IDataStreamAggregator<TProjection, in TData>
    {
        TProjection Aggregate(TProjection projection, IStreamQueryBatch<TData> events);
    }
}