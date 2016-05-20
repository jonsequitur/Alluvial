using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Composes the operations of retrieving or creating a projection, performing an aggregate operation with a sequence of data, and storing the updated projection. 
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    /// <param name="projectionId">The projection identifier.</param>
    /// <param name="aggregate">The aggregate function, which is called after a projection and data are available.</param>
    /// <returns></returns>
    public delegate Task FetchAndSave<TProjection>(
        string projectionId,
        Aggregate<TProjection> aggregate);

    /// <summary>
    /// Queries a downstream stream based on an item from an upstream stream during an <see cref="Stream.IntoMany{TUpstream,TDownstream,TUpstreamCursor}(Alluvial.IStream{TUpstream,TUpstreamCursor},Alluvial.QueryDownstream{TUpstream,TDownstream,TUpstreamCursor})" /> operation.
    /// </summary>
    /// <typeparam name="TUpstream">The type of the upstream stream.</typeparam>
    /// <typeparam name="TDownstream">The type of the downstream stream.</typeparam>
    /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
    /// <param name="upstreamItem">The upstream item for which to retrieve corresponding data from the downstream stream.</param>
    /// <param name="fromCursor">The starting cursor position in the upstream stream.</param>
    /// <param name="toCursor">The ending cursor position in the upstream stream.</param>
    /// <returns></returns>
    public delegate Task<TDownstream> QueryDownstreamAsync<in TUpstream, TDownstream, in TUpstreamCursor>(
        TUpstream upstreamItem,
        TUpstreamCursor fromCursor,
        TUpstreamCursor toCursor);

    /// <summary>
    /// Queries a downstream stream based on an item from an upstream partitioned stream during an <see cref="Stream.IntoMany{TUpstream,TDownstream,TUpstreamCursor,TPartition}(Alluvial.IPartitionedStream{TUpstream,TUpstreamCursor,TPartition},Alluvial.QueryDownstream{TUpstream,TDownstream,TUpstreamCursor,TPartition})" /> operation.
    /// </summary>
    /// <typeparam name="TUpstream">The type of the upstream stream.</typeparam>
    /// <typeparam name="TDownstream">The type of the downstream stream.</typeparam>
    /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    /// <param name="upstreamItem">The upstream item for which to retrieve corresponding data from the downstream stream.</param>
    /// <param name="fromCursor">The starting cursor position in the upstream stream.</param>
    /// <param name="toCursor">The ending cursor position in the upstream stream.</param>
    /// <param name="partition">The partition.</param>
    /// <returns></returns>
    public delegate Task<TDownstream> QueryDownstreamAsync<in TUpstream, TDownstream, in TUpstreamCursor, out TPartition>(
        TUpstream upstreamItem,
        TUpstreamCursor fromCursor,
        TUpstreamCursor toCursor,
        IStreamQueryPartition<TPartition> partition);  
    
    /// <summary>
    /// Queries a downstream stream based on an item from an upstream stream during an <see cref="Stream.IntoMany{TUpstream,TDownstream,TUpstreamCursor}(Alluvial.IStream{TUpstream,TUpstreamCursor},Alluvial.QueryDownstream{TUpstream,TDownstream,TUpstreamCursor})" /> operation.
    /// </summary>
    /// <typeparam name="TUpstream">The type of the upstream stream.</typeparam>
    /// <typeparam name="TDownstream">The type of the downstream stream.</typeparam>
    /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
    /// <param name="upstreamItem">The upstream item for which to retrieve corresponding data from the downstream stream.</param>
    /// <param name="fromCursor">The starting cursor position in the upstream stream.</param>
    /// <param name="toCursor">The ending cursor position in the upstream stream.</param>
    /// <returns></returns>
    public delegate TDownstream QueryDownstream<in TUpstream, out TDownstream, in TUpstreamCursor>(
        TUpstream upstreamItem,
        TUpstreamCursor fromCursor,
        TUpstreamCursor toCursor);

    /// <summary>
    /// Queries a downstream stream based on an item from an upstream partitioned stream during an <see cref="Stream.IntoMany{TUpstream,TDownstream,TUpstreamCursor,TPartition}(Alluvial.IPartitionedStream{TUpstream,TUpstreamCursor,TPartition},Alluvial.QueryDownstream{TUpstream,TDownstream,TUpstreamCursor,TPartition})" /> operation.
    /// </summary>
    /// <typeparam name="TUpstream">The type of the upstream stream.</typeparam>
    /// <typeparam name="TDownstream">The type of the downstream stream.</typeparam>
    /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    /// <param name="upstreamItem">The upstream item for which to retrieve corresponding data from the downstream stream.</param>
    /// <param name="fromCursor">The starting cursor position in the upstream stream.</param>
    /// <param name="toCursor">The ending cursor position in the upstream stream.</param>
    /// <param name="partition">The partition.</param>
    /// <returns></returns>
    public delegate TDownstream QueryDownstream<in TUpstream, out TDownstream, in TUpstreamCursor, out TPartition>(
        TUpstream upstreamItem,
        TUpstreamCursor fromCursor,
        TUpstreamCursor toCursor,
        IStreamQueryPartition<TPartition> partition);
}