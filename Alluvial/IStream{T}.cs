using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// A cursorable stream of data.
    /// </summary>
    public interface IStream<TData, TCursor>
    {
        /// <summary>
        /// Gets the identifier for the stream.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Fetches a batch of data from the stream.
        /// </summary>
        /// <param name="query">The query to apply to the stream.</param>
        Task<IStreamBatch<TData>> Fetch(IStreamQuery<TCursor> query);

        /// <summary>
        /// Creates a new cursor at the start of the stream.
        /// </summary>
        ICursor<TCursor> NewCursor();
    }
}