using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Applies a batch data to a projection. 
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    /// <typeparam name="TData">The type of the stream's data.</typeparam>
    /// <param name="initial">The initial state of the projection prior to the data being applied.</param>
    /// <param name="batch">The data to be applied to the projection.</param>
    /// <returns>The updated projection.</returns>
    public delegate Task<TProjection> AggregateAsync<TProjection, in TData>(
        TProjection initial,
        IStreamBatch<TData> batch);
}