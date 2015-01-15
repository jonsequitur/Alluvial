using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Iterates over a stream and keeps track of a cursor position as it does so.
    /// </summary>
    public interface IStreamIterator<TData> : IStreamQuery
    {
        /// <summary>
        /// Gets the next batch from the source stream, starting from the position in the steam where the previous batch ended.
        /// </summary>
        Task<IStreamBatch<TData>> NextBatch();
    }
}