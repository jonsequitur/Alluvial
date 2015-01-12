namespace Alluvial
{
    public delegate TProjection Aggregate<TProjection, in TData>(
        TProjection initial,
        IStreamQueryBatch<TData> events);
}