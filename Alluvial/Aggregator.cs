using System;
using System.Linq;

namespace Alluvial
{
    /// <summary>
    /// Methods for creating and composing data stream aggregators.
    /// </summary>
    public static class Aggregator
    {
        public static IStreamAggregator<TProjection, TData> After<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> first,
            Aggregate<TProjection, TData> then)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first.Aggregate(projection, batch);
                projection = then(projection, batch);
                return projection;
            });
        }

        public static IStreamAggregator<TProjection, TData> After<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> first,
            Action<TProjection, IStreamBatch<TData>> then)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first.Aggregate(projection, batch);
                then(projection, batch);
                return projection;
            });
        }

        public static IStreamAggregator<TProjection, TData> Before<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> then,
            Aggregate<TProjection, TData> first)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first(projection, batch);
                return then.Aggregate(projection, batch);
            });
        }

        public static IStreamAggregator<TProjection, TData> Before<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> then,
            Action<TProjection, IStreamBatch<TData>> first)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                first(projection, batch);
                return then.Aggregate(projection, batch);
            });
        }

        public static IStreamAggregator<TProjection, TData> Catch<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamBatch<TData>, Exception, bool> continueIf)
        {
            return aggregator.Pipeline((projection, batch, next) =>
            {
                try
                {
                    return next(projection, batch);
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
            Aggregate<TProjection, TData> aggregate)
        {
            return new AnonymousStreamAggregator<TProjection, TData>(
                aggregate);
        }

        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Action<TProjection, IStreamBatch<TData>> aggregate)
        {
            return new AnonymousStreamAggregator<TProjection, TData>(
                (projection, batch) =>
                {
                    aggregate(projection, batch);
                    return projection;
                });
        }

        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Action<TProjection, IStreamBatch<TData>,
                Action<TProjection, IStreamBatch<TData>>> initial)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                initial(projection, batch, (p, xs) => aggregator.Aggregate(p, xs));
            });
        }

        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamBatch<TData>, Aggregate<TProjection, TData>, TProjection> initial)
        {
            return Create<TProjection, TData>((projection, batch) => initial(projection, batch, aggregator.Aggregate));
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

                projection = next(projection, batch);

                return projection;
            });
        }
    }
}