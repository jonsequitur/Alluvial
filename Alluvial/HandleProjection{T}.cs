using System;
using System.Threading.Tasks;

namespace Alluvial
{
    // FIX: rename FetchAndSaveProjection
    // FIX: rename CallAggregatorPipeline
    // FIX: rename QueryDownstream

    public delegate Task FetchAndSaveProjection<TProjection>(
        string streamId,
        CallAggregatorPipeline<TProjection> aggregate);

    public delegate Task<TProjection> CallAggregatorPipeline<TProjection>(TProjection projection);

    public delegate Task<TDownstream> QueryDownstream<in TUpstream, TDownstream, in TUpstreamCursor>(
        TUpstream upstreamItem,
        TUpstreamCursor fromCursor,
        TUpstreamCursor toCursor);
}