using System;
using System.Linq;

namespace Alluvial
{
    /// <summary>
    /// Methods for creating and composing data stream aggregators.
    /// </summary>
    public static class Aggregator
    {
        public static IDataStreamAggregator<TProjection, TData> After<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> first,
            Aggregate<TProjection, TData> then)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first.Aggregate(projection, batch);
                projection = then(projection, batch);
                return projection;
            });
        }

        public static IDataStreamAggregator<TProjection, TData> After<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> first,
            Action<TProjection, IStreamQueryBatch<TData>> then)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first.Aggregate(projection, batch);
                then(projection, batch);
                return projection;
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Before<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> then,
            Aggregate<TProjection, TData> first)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                projection = first(projection, batch);
                return then.Aggregate(projection, batch);
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Before<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> then,
            Action<TProjection, IStreamQueryBatch<TData>> first)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                first(projection, batch);
                return then.Aggregate(projection, batch);
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Catch<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamQueryBatch<TData>, Exception, bool> continueIf)
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

        public static IDataStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Aggregate<TProjection, TData> aggregate)
        {
            return new AnonymousDataStreamAggregator<TProjection, TData>(
                aggregate);
        }

        public static IDataStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Action<TProjection, IStreamQueryBatch<TData>> aggregate)
        {
            return new AnonymousDataStreamAggregator<TProjection, TData>(
                (projection, batch) =>
                {
                    aggregate(projection, batch);
                    return projection;
                });
        }

        public static IDataStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> aggregator,
            Action<TProjection, IStreamQueryBatch<TData>,
                Action<TProjection, IStreamQueryBatch<TData>>> initial)
        {
            return Create<TProjection, TData>((projection, batch) =>
            {
                initial(projection, batch, (p, xs) => aggregator.Aggregate(p, xs));
            });
        }

        public static IDataStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamQueryBatch<TData>, Aggregate<TProjection, TData>, TProjection> initial)
        {
            return Create<TProjection, TData>((projection, batch) => initial(projection, batch, aggregator.Aggregate));
        }

        public static IDataStreamAggregator<TProjection, TData> Trace<TProjection, TData>(
            this IDataStreamAggregator<TProjection, TData> aggregator)
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