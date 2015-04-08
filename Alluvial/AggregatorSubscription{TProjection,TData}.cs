using System;

namespace Alluvial
{
    internal class AggregatorSubscription<TProjection, TData> : IAggregatorSubscription
    {
        internal readonly HandleAggregatorError<TProjection> OnError;

        public AggregatorSubscription(
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSaveProjection<TProjection> fetchAndSaveProjection = null,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (aggregator == null)
            {
                throw new ArgumentNullException("aggregator");
            }
            
            OnError = onError ?? (error => { });

            if (onError != null)
            {
                fetchAndSaveProjection = fetchAndSaveProjection.Catch(onError);
            }

            FetchAndSaveProjection = fetchAndSaveProjection ??
                                     (async (streamId, aggregate) =>
                                     {
                                         await aggregate(Activator.CreateInstance<TProjection>());
                                     });
            Aggregator = aggregator;
        }

        public IStreamAggregator<TProjection, TData> Aggregator { get; private set; }

        public FetchAndSaveProjection<TProjection> FetchAndSaveProjection { get; private set; }

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