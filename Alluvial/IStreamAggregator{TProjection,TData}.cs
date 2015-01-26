using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Performs ongoing aggregation of data into a projection. 
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    /// <typeparam name="TData">The type of the data.</typeparam>
    public interface IStreamAggregator<TProjection, in TData>
    {
        /// <summary>
        /// Applies a batch of data to a projection and returns the updated projection.
        /// </summary>
        Task<TProjection> Aggregate(TProjection projection, IStreamBatch<TData> events);
    }
}