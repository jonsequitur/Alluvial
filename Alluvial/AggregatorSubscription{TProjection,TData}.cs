using System;

namespace Alluvial
{
    internal class AggregatorSubscription<TProjection, TData> : IAggregatorSubscription
    {
        public AggregatorSubscription(
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSaveProjection<TProjection> fetchAndSaveProjection = null)
        {
            if (aggregator == null)
            {
                throw new ArgumentNullException("aggregator");
            }

            FetchAndSaveProjection = fetchAndSaveProjection ??
                                     (async (streamId, aggregate) =>
                                     {
                                         await aggregate(Activator.CreateInstance<TProjection>());
                                     });
            Aggregator = aggregator;
            IsCursor = typeof (ICursor).IsAssignableFrom(typeof (TProjection));
        }

        public IStreamAggregator<TProjection, TData> Aggregator { get; private set; }

        public FetchAndSaveProjection<TProjection> FetchAndSaveProjection { get; private set; }

        public bool IsCursor { get; protected set; }

        public Type ProjectionType
        {
            get
            {
                return typeof (TProjection);
            }
        }

        public Type StreamDataType
        {
            get
            {
                return typeof (TData);
            }
        }
    }
}