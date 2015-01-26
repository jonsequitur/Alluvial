using System;

namespace Alluvial
{
    internal abstract class AggregatorSubscription
    {
        public abstract Type ProjectionType { get; }
        public abstract Type StreamDataType { get; }
    }
}