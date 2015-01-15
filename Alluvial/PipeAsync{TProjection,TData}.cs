using System.Threading.Tasks;

namespace Alluvial
{
    public delegate Task<TProjection> PipeAsync<TProjection, TData>(
        TProjection projection,
        IStreamBatch<TData> batch,
        AggregateAsync<TProjection, TData> next);
}