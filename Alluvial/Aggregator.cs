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
        /// <summary>
        /// Creates a stream aggregator.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregate">Performs aggregation of data from the stream.</param>
        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            AggregateAsync<TProjection, TData> aggregate)
        {
            return new AnonymousStreamAggregator<TProjection, TData>(aggregate);
        }

        /// <summary>
        /// Creates a stream aggregator.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregate">Performs aggregation of data from the stream.</param>
        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Aggregate<TProjection, TData> aggregate)
        {
            return new AnonymousStreamAggregator<TProjection, TData>(async (initial, batch) => aggregate(initial, batch));
        }

        /// <summary>
        /// Creates a stream aggregator.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregate">Performs aggregation of data from the stream.</param>
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

        /// <summary>
        /// Creates a stream aggregator.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregate">Performs aggregation of data from the stream.</param>
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
        {
            return Create<TProjection, TData>((projection, batch) => initial(projection,
                                                                             batch,
                                                                             aggregator.Aggregate));
        }

        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            PipeAsync<TProjection, TData> initial)
        {
            return Create<TProjection, TData>((projection, batch) =>
                                                  initial(projection,
                                                          batch,
                                                          aggregator.Aggregate));
        }

        public static IStreamAggregator<TProjection, TData> Trace<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Action<TProjection, IStreamBatch<TData>> write = null)
        {
            write = write ?? TraceDefault;

            return aggregator.Pipeline(async (projection, batch, next) =>
            {
                try
                {
                    write(projection, batch);
                    return await next(projection, batch);
                }
                catch (Exception exception)
                {
                    System.Diagnostics.Trace.WriteLine("[Aggregate] Exception: " + exception);
                    throw;
                }
            });
        }

        public static IStreamAggregator<TProjection, TData> Trace<TProjection, TData>(
            this AggregateAsync<TProjection, TData> aggregate,
            Action<TProjection, IStreamBatch<TData>> write = null)
        {
            return Create(aggregate).Trace();
        }

        private static void TraceDefault<TProjection, TData>(TProjection projection, IStreamBatch<TData> batch)
        {
            System.Diagnostics.Trace.WriteLine(
                string.Format("[Aggregate] {0} / batch of {1} starts @ {2}",
                              projection,
                              batch.Count,
                              (string) batch.StartsAtCursorPosition.ToString()));
        }
    }
}