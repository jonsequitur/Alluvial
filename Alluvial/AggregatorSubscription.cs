using System;

namespace Alluvial
{
    internal static class AggregatorSubscription
    {
        public static FetchAndSave<TProjection> Catch<TProjection>(
            this FetchAndSave<TProjection> fetchAndSave,
            HandleAggregatorError<TProjection> onError) =>
                async (id, aggregate) =>
                {
                    Exception innerException = null;

                    try
                    {
                        await fetchAndSave(id, async projection =>
                        {
                            TProjection resultingProjection;

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

                                return error.Projection;
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

        internal static StreamCatchupError<TProjection> CheckErrorHandler<TProjection>(
            this HandleAggregatorError<TProjection> onError,
            Exception exception,
            TProjection projection = default(TProjection))
        {
            var error = new StreamCatchupError<TProjection>(exception, projection);

            onError(error);

            return error;
        }
    }
}