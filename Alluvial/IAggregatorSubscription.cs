using System;

namespace Alluvial
{
    internal interface IAggregatorSubscription
    {
        bool IsCursor { get; }
        Type ProjectionType { get; }
        Type StreamDataType { get; }
    }
}