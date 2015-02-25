using System;

namespace Alluvial
{
    internal interface IAggregatorSubscription
    {
        Type ProjectionType { get; }
        Type StreamDataType { get; }
    }
}