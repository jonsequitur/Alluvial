using System;
using System.Threading.Tasks;

namespace Alluvial
{
    // FIX: rename FetchAndSaveProjection
    // FIX: rename QueryDownstream

    public delegate Task FetchAndSaveProjection<TProjection>(
        string projectionId,
        Aggregate<TProjection> aggregate);

    public delegate Task<TDownstream> QueryDownstream<in TUpstream, TDownstream, in TUpstreamCursor>(
        TUpstream upstreamItem,
        TUpstreamCursor fromCursor,
        TUpstreamCursor toCursor);
}