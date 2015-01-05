using System.Collections.Generic;

namespace Alluvial
{
    public interface IDataStreamAggregator<TProjection, in TData>
    {
        TProjection Aggregate(TProjection projection, IEnumerable<TData> events);
    }
}