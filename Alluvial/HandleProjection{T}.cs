using System;
using System.Threading.Tasks;

namespace Alluvial
{
    public delegate Task FetchAndSaveProjection<TProjection>(
        string streamId,
        CallAggregatorPipeline<TProjection> aggregate);

    public delegate Task<TProjection> CallAggregatorPipeline<TProjection>(
        TProjection projection,
        ICursor cursor = null);
}