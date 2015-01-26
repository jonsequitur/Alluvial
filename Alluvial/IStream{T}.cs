using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// An ordered stream of data.
    /// </summary>
    /// <typeparam name="TData">The type of the data.</typeparam>
    public interface IStream<TData>
    {
        /// <summary>
        /// Gets the identifier for the stream.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Fetches a batch of data from the stream.
        /// </summary>
        /// <param name="query">The query to apply to the stream.</param>
        Task<IStreamBatch<TData>> Fetch(IStreamQuery query);
    }
}