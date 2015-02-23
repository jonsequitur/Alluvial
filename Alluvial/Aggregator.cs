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
            where TProjection : class, ICursor
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
            where TProjection : class, ICursor
        {
            return new AnonymousStreamAggregator<TProjection, TData>(
                (projection, batch) =>
                {
                    aggregate(projection, batch);
                    return Task.FromResult(projection);
                });
        }

        public static IStreamAggregator<Projection<TProjection, TData>, TData> CreateFor<TProjection, TData>(
            AggregateAsync<Projection<TProjection, TData>, TData> aggregate)
        {
            return new AnonymousStreamAggregator<Projection<TProjection, TData>, TData>(aggregate);
        }

        public static IStreamAggregator<Projection<TProjection, TData>, TData> CreateFor<TProjection, TData>(
            Aggregate<Projection<TProjection, TData>, TData> aggregate)
        {
            return new AnonymousStreamAggregator<Projection<TProjection, TData>, TData>(async (projection, batch) => aggregate(projection, batch));
        }

        public static IStreamAggregator<Projection<TProjection, TData>, TData> CreateFor<TProjection, TData>(
            Func<Projection<TProjection, TData>, IStreamBatch<TData>, Task> aggregate)
        {
            return new AnonymousStreamAggregator<Projection<TProjection, TData>, TData>(
                async (projection, batch) =>
                {
                    await aggregate(projection, batch);
                    return projection;
                });
        }

        public static IStreamAggregator<Projection<TProjection, TData>, TData> CreateFor<TProjection, TData>(
            Action<Projection<TProjection, TData>, IStreamBatch<TData>> aggregate)
        {
            return new AnonymousStreamAggregator<Projection<TProjection, TData>, TData>(
                (projection, batch) =>
                {
                    aggregate(projection, batch);
                    return Task.FromResult(projection);
                });
        }

        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamBatch<TData>, AggregateAsync<TProjection, TData>, Task> initial)
            where TProjection : class, ICursor
        {
            return Create<TProjection, TData>((projection, batch) => initial(projection,
                                                                             batch,
                                                                             aggregator.Aggregate));
        }

        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            PipeAsync<TProjection, TData> initial)
            where TProjection : ICursor
        {
            return Create<TProjection, TData>((projection, batch) =>
                                                  initial(projection,
                                                          batch,
                                                          aggregator.Aggregate));
        }

        public static IStreamAggregator<TProjection, TData> Trace<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Action<TProjection, IStreamBatch<TData>> write = null)
            where TProjection : ICursor
        {
            write = write ?? TraceDefault;

            return aggregator.Pipeline((projection, batch, next) =>
            {
                write(projection, batch);
                return next(projection, batch);
            });
        }

        private static void TraceDefault<TProjection, TData>(TProjection projection, IStreamBatch<TData> batch)
        {
            System.Diagnostics.Trace.WriteLine(
                string.Format("Aggregate: {0} / batch of {1} starts @ {2}",
                              projection,
                              batch.Count,
                              (string) batch.StartsAtCursorPosition.ToString()));
        }
    }
}