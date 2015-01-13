using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// An ordered stream of data.
    /// </summary>
    /// <typeparam name="TData">The type of the data.</typeparam>
    public interface IDataStream<TData>
    {
        /// <summary>
        /// Gets the identifier for the stream.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Fetches a batch of data from the stream.
        /// </summary>
        /// <param name="query">The query to apply to the stream.</param>
        Task<IStreamQueryBatch<TData>> Fetch(IStreamQuery<TData> query);
    }
}