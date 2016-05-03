using System;

namespace Alluvial
{
    internal interface IAggregatorSubscription
    {
        Type ProjectionType { get; }

        Type StreamDataType { get; }

        StreamCatchupError HandleError(
            Exception exception,
            object projection);
    }
}