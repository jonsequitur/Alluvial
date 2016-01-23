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
            AggregateAsync<TProjection, TData> aggregate) =>
                new AnonymousStreamAggregator<TProjection, TData>(aggregate);

        /// <summary>
        /// Creates a stream aggregator.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregate">Performs aggregation of data from the stream.</param>
        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Aggregate<TProjection, TData> aggregate) =>
                new AnonymousStreamAggregator<TProjection, TData>((initial, batch) =>
                                                                  aggregate(initial, batch).CompletedTask());

        /// <summary>
        /// Creates a stream aggregator.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregate">Performs aggregation of data from the stream.</param>
        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Func<TProjection, IStreamBatch<TData>, Task> aggregate) =>
                new AnonymousStreamAggregator<TProjection, TData>(
                    async (projection, batch) =>
                    {
                        await aggregate(projection, batch);
                        return projection;
                    });

        /// <summary>
        /// Creates a stream aggregator.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregate">Performs aggregation of data from the stream.</param>
        public static IStreamAggregator<TProjection, TData> Create<TProjection, TData>(
            Action<TProjection, IStreamBatch<TData>> aggregate) =>
                new AnonymousStreamAggregator<TProjection, TData>(
                    (projection, batch) =>
                    {
                        aggregate(projection, batch);
                        return Task.FromResult(projection);
                    });

        /// <summary>
        /// Wraps a stream aggregator in a decorator function having the same signature.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="initial">The prior state of the projection.</param>
        /// <returns>A new aggregator that wraps calls to the specified aggregator.</returns>
        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            Func<TProjection, IStreamBatch<TData>, AggregateAsync<TProjection, TData>, Task> initial) =>
                Create<TProjection, TData>((projection, batch) =>
                                           initial(projection,
                                                   batch,
                                                   aggregator.Aggregate));

        /// <summary>
        /// Wraps a stream aggregator in a decorator function having the same signature.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="initial">The prior state of the projection.</param>
        /// <returns>A new aggregator that wraps calls to the specified aggregator.</returns>
        public static IStreamAggregator<TProjection, TData> Pipeline<TProjection, TData>(
            this IStreamAggregator<TProjection, TData> aggregator,
            PipeAsync<TProjection, TData> initial) =>
                Create<TProjection, TData>((projection, batch) =>
                                           initial(projection,
                                                   batch,
                                                   aggregator.Aggregate));

        /// <summary>
        /// Traces calls to the specified aggregator.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="write">A delegate that can be used to specify how the arguments should be traced. If this is not provided, output is sent to <see cref="System.Diagnostics.Trace" />.</param>
        /// <returns>The original aggregator wrapped in a tracing decorator.</returns>
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

        /// <summary>
        /// Traces calls to the specified aggregator function.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="aggregate">The aggregator function.</param>
        /// <param name="write">A delegate that can be used to specify how the arguments should be traced. If this is not provided, output is sent to <see cref="System.Diagnostics.Trace" />.</param>
        /// <returns>The original aggregator wrapped in a tracing decorator.</returns>
        public static IStreamAggregator<TProjection, TData> Trace<TProjection, TData>(
            this AggregateAsync<TProjection, TData> aggregate,
            Action<TProjection, IStreamBatch<TData>> write = null) =>
                Create(aggregate).Trace(write);

        private static void TraceDefault<TProjection, TData>(
            TProjection projection,
            IStreamBatch<TData> batch)
        {
            var message = $"[Aggregate] {projection} / batch of {batch.Count} starts @ {batch.StartsAtCursorPosition}";

            System.Diagnostics.Trace.WriteLine(message);
        }
    }
}