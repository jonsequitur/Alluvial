using System;
using System.Threading.Tasks;

namespace Alluvial
{
    // FIX: rename QueryDownstream

    public delegate Task FetchAndSave<TProjection>(
        string projectionId,
        Aggregate<TProjection> aggregate);

    public delegate Task<TDownstream> QueryDownstream<in TUpstream, TDownstream, in TUpstreamCursor>(
        TUpstream upstreamItem,
        TUpstreamCursor fromCursor,
        TUpstreamCursor toCursor);
    
    public delegate Task<TDownstream> QueryDownstream<in TUpstream, TDownstream, in TUpstreamCursor, out TPartition>(
        TUpstream upstreamItem,
        TUpstreamCursor fromCursor,
        TUpstreamCursor toCursor,
        IStreamQueryPartition<TPartition> partition);
}