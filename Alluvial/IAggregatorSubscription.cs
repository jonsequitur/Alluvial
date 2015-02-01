using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal interface IAggregatorSubscription
    {
        bool IsCursor { get; }
        Type ProjectionType { get; }
        Type StreamDataType { get; }
        Task<object> GetProjection(string streamId);
    }
}