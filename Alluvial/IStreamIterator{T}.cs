using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Allows sequential iteration over a stream one batch at a time.
    /// </summary>
    /// <typeparam name="TData">The type of the stream's data.</typeparam>
    /// <typeparam name="TCursor">The type of the cursor.</typeparam>
    public interface IStreamIterator<TData, TCursor> : IStreamQuery<TCursor>
    {
        /// <summary>
        /// Gets the next batch from the source stream, starting from the position in the steam where the previous batch ended.
        /// </summary>
        Task<IStreamBatch<TData>> NextBatch();
    }
}