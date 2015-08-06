using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// A stream of data that is separable into partitions that can be queried independently.
    /// </summary>
    /// <typeparam name="TData">The type of the data.</typeparam>
    /// <typeparam name="TCursor">The type of the cursor.</typeparam>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    public interface IPartitionedStream<TData, TCursor, out TPartition>
    {
        /// <summary>
        /// Gets the identifier for the stream.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Gets a single partition, represented as a stream.
        /// </summary>
        /// <param name="partition">The partition.</param>
        Task<IStream<TData, TCursor>> GetStream(IStreamQueryPartition<TPartition> partition);
    }
}