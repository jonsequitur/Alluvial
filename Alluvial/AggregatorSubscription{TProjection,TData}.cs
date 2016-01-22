using System;

namespace Alluvial
{
    internal class AggregatorSubscription<TProjection, TData> : IAggregatorSubscription
    {
        internal readonly HandleAggregatorError<TProjection> OnError;

        public AggregatorSubscription(
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSave<TProjection> fetchAndSave = null,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (aggregator == null)
            {
                throw new ArgumentNullException(nameof(aggregator));
            }
            
            OnError = onError ?? (error => { });

            if (onError != null)
            {
                fetchAndSave = fetchAndSave.Catch(onError);
            }

            FetchAndSave = fetchAndSave ??
                                     (async (streamId, aggregate) =>
                                     {
                                         await aggregate(Activator.CreateInstance<TProjection>());
                                     });
            Aggregator = aggregator;
        }

        public IStreamAggregator<TProjection, TData> Aggregator { get; private set; }

        public FetchAndSave<TProjection> FetchAndSave { get; private set; }

        public Type ProjectionType => typeof (TProjection);

        public Type StreamDataType => typeof (TData);
    }
}