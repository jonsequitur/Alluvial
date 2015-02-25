using System.Threading.Tasks;

namespace Alluvial
{
    public interface IStreamIterator<TData, TCursorPosition> : IStreamQuery<TCursorPosition>
    {
        /// <summary>
        /// Gets the next batch from the source stream, starting from the position in the steam where the previous batch ended.
        /// </summary>
        Task<IStreamBatch<TData>> NextBatch();
    }
}