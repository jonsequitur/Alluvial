namespace Alluvial
{
    public delegate TProjection Aggregate<TProjection, in TData>(
        TProjection initial,
        IStreamBatch<TData> batch);
}