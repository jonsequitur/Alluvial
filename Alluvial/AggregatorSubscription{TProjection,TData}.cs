using System;

namespace Alluvial
{
    internal class AggregatorSubscription<TProjection, TData> : IAggregatorSubscription
    {
        private readonly HandleAggregatorError<TProjection> onError;

        public AggregatorSubscription(
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSave<TProjection> fetchAndSave = null,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (aggregator == null)
            {
                throw new ArgumentNullException(nameof(aggregator));
            }
            
            this.onError = onError ?? (error => { });

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

        StreamCatchupError IAggregatorSubscription.HandleError(
            Exception exception, 
            object projection)
        {
            if (projection != null)
            {
                return this.HandleError(exception, (dynamic) projection);
            }

            return this.HandleError(exception, default(TProjection));
        }

        public StreamCatchupError HandleError(
            Exception exception,
            TProjection projection)
        {
            var catchupError = StreamCatchupError.Create(exception, projection);
            onError(catchupError);
            return catchupError;
        }
    }
}