using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// A link in the aggregator pipeline.
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    /// <typeparam name="TData">The type of the data.</typeparam>
    /// <param name="projection">The projection.</param>
    /// <param name="batch">The batch.</param>
    /// <param name="next">The next function in the pipeline.</param>
    /// <returns>The updated projection.</returns>
    public delegate Task<TProjection> PipeAsync<TProjection, TData>(
        TProjection projection,
        IStreamBatch<TData> batch,
        AggregateAsync<TProjection, TData> next);
}