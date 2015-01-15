using System;

namespace Alluvial
{
    internal class AggregatorSubscription<TProjection, TData> : AggregatorSubscription
    {
        public AggregatorSubscription(
            IStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            if (aggregator == null)
            {
                throw new ArgumentNullException("aggregator");
            }
            ProjectionStore = projectionStore ??
                              new SingleInstanceProjectionCache<string, TProjection>();
            Aggregator = aggregator;
        }

        public IStreamAggregator<TProjection, TData> Aggregator { get; private set; }

        public IProjectionStore<string, TProjection> ProjectionStore { get; private set; }
    }
}