using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Methods for creating and composing stream aggregators.
    /// </summary>
    public static class Aggregator
    {
        public static IStreamAggregator<TProjection, TData> Catch<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamBatch<TData>, Exception, bool> continueIf)
        {
            return aggregator.Pipeline(async (projection, batch, next) =>
            {
                try
                {
                    return await next(projection, batch);
                }
                catch (Exception exception)
                {
                    if (continueIf(projection, batch, exception))
                    {
                        return projection;
                    }
                    throw;
                }
            });
        }

        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            AggregateAsync<TProjection, TData> aggregate)
        {
            return new AnonymousStreamAggregator<TProjection, TData>(aggregate);
        }

        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Aggregate<TProjection, TData> aggregate)
        {
            return new AnonymousStreamAggregator<TProjection, TData>(async (initial, batch) => aggregate(initial, batch));
        }

        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Func<TProjection, IStreamBatch<TData>, Task> aggregate)
        {
            return new AnonymousStreamAggregator<TProjection, TData>(
                async (projection, batch) =>
                {
                    await aggregate(projection, batch);
                    return projection;
                });
        }

        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Action<TProjection, IStreamBatch<TData>> aggregate)
        {
            return new AnonymousStreamAggregator<TProjection, TData>(
                (projection, batch) =>
                {
                    aggregate(projection, batch);
                    return Task.FromResult(projection);
                });
        }

        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamBatch<TData>, AggregateAsync<TProjection, TData>, Task> initial)
        {
            return Create<TProjection, TData>(async (projection, batch) =>
            {
                await initial(projection,
                              batch,
                              aggregator.Aggregate);
            });
        }

        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamBatch<TData>, AggregateAsync<TProjection, TData>, Task<TProjection>> initial)
        {
            return Create<TProjection, TData>(async (projection, batch) =>
                                                  await initial(projection,
                                                                batch,
                                                                aggregator.Aggregate));
        }

        public static IStreamAggregator<TProjection, TData> Trace<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator)
        {
            return aggregator.Pipeline((projection, batch, next) =>
            {
                System.Diagnostics.Trace.WriteLine(
                    string.Format("Projection {0} / batch of {1} starts @ {2}",
                                  projection,
                                  batch.Count,
                                  (string) batch.StartsAtCursorPosition.ToString()));

                return next(projection, batch);
            });
        }
    }
}