using System.Threading.Tasks;

namespace Alluvial
{
    public interface IStream<TData, TCursorPosition>
    {
        /// <summary>
        /// Gets the identifier for the stream.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Fetches a batch of data from the stream.
        /// </summary>
        /// <param name="query">The query to apply to the stream.</param>
        Task<IStreamBatch<TData>> Fetch(IStreamQuery<TCursorPosition> query);

        ICursor<TCursorPosition> NewCursor();
    }
}