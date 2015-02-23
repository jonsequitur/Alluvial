using System;

namespace Alluvial
{
    internal static class AggregatorSubscription
    {
        public static FetchAndSaveProjection<TProjection> Catch<TProjection>(
            this FetchAndSaveProjection<TProjection> fetchAndSaveProjection,
            HandleAggregatorError<TProjection> onError)
        {
            return async (id, aggregate) =>
            {
                Exception innerException = null;

                try
                {
                    await fetchAndSaveProjection(id, async (projection) =>
                    {
                        TProjection resultingProjection = default(TProjection);

                        try
                        {
                            resultingProjection = await aggregate(projection);
                        }
                        catch (Exception exception)
                        {
                            var error = CheckErrorHandler(onError, exception, projection);

                            if (!error.ShouldContinue)
                            {
                                throw;
                            }

                            innerException = exception;
                        }

                        return resultingProjection;
                    });
                }
                catch (Exception exception)
                {
                    if (exception != innerException)
                    {
                        var error = CheckErrorHandler(onError, exception);

                        if (!error.ShouldContinue)
                        {
                            throw;
                        }
                    }
                }
            };
        }

        private static StreamCatchupError<TProjection> CheckErrorHandler<TProjection>(
            this HandleAggregatorError<TProjection> onError,
            Exception exception,
            TProjection projection = default(TProjection))
        {
            var error = new StreamCatchupError<TProjection>
            {
                Exception = exception,
                Projection = projection
            };

            onError(error);

            return error;
        }
    }
}