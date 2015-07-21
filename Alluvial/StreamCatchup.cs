using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with stream catchups.
    /// </summary>
    public static class StreamCatchup
    {
        /// <summary>
        /// Creates a catchup for the specified stream.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="initialCursor">The initial cursor from which the catchup proceeds.</param>
        /// <param name="batchSize">The number of items to retrieve from the stream per batch.</param>
        /// <returns></returns>
        public static IStreamCatchup<TData, TCursor> Create<TData, TCursor>(IStream<TData, TCursor> stream, ICursor<TCursor> initialCursor = null, int? batchSize = null)
        {
            return new SingleStreamCatchup<TData, TCursor>(
                stream,
                initialCursor,
                batchSize);
        }

        /// <summary>
        /// Creates a multiple-stream catchup.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
        /// <typeparam name="TDownstreamCursor">The type of the downstream cursor.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="cursor">The initial cursor position for the catchup.</param>
        /// <param name="batchSize">The number of items to retrieve from the stream per batch.</param>
        public static IStreamCatchup<TData, TUpstreamCursor> All<TData, TUpstreamCursor, TDownstreamCursor>(
            IStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor> stream,
            ICursor<TUpstreamCursor> cursor = null,
            int? batchSize = null)
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor>(stream, batchSize: batchSize);

            return new MultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor>(
                upstreamCatchup,
                cursor ?? stream.NewCursor());
        }

        /// <summary>
        /// Creates a multiple-stream catchup.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
        /// <typeparam name="TDownstreamCursor">The type of the downstream cursor.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="batchSize">The number of items to retrieve from the stream per batch.</param>
        public static IStreamCatchup<TData, TUpstreamCursor> All<TData, TUpstreamCursor, TDownstreamCursor>(
            IStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor> stream,
            FetchAndSaveProjection<ICursor<TUpstreamCursor>> manageCursor,
            int? batchSize = null)
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor>(stream, batchSize: batchSize);

            return new MultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor>(
                upstreamCatchup,
                manageCursor);
        }

        /// <summary>
        /// Runs the catchup query until it reaches an empty batch, then stops.
        /// </summary>
        public static async Task<ICursor<TCursor>> RunUntilCaughtUp<TData, TCursor>(this IStreamCatchup<TData, TCursor> catchup)
        {
            ICursor<TCursor> cursor;
            var counter = new Counter<TData>();

            using (catchup.Subscribe(async (_, batch) => counter.Count(batch), NoCursor(counter)))
            {
                int countBefore;
                do
                {
                    countBefore = counter.Value;
                    cursor = await catchup.RunSingleBatch();
                } while (countBefore != counter.Value);
            }

            return cursor;
        }

        /// <summary>
        /// Runs catchup batches repeatedly with a specified interval after each batch.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="pollInterval">The amount of time to wait after each batch is processed.</param>
        /// <returns>A disposable that, when disposed, stops the polling.</returns>
        public static IDisposable Poll<TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            TimeSpan pollInterval)
        {
            var canceled = false;

            Task.Run(async () =>
            {
                while (!canceled)
                {
                    await catchup.RunSingleBatch();
                    await Task.Delay(pollInterval);
                }
            });

            return Disposable.Create(() => { canceled = true; });
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="projectionStore">The projection store.</param>
        /// <returns>A disposable that, when disposed, unsubscribes the aggregator.</returns>
        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.Subscribe(aggregator,
                                     projectionStore.AsHandler());
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregate">A delegate that performs an aggregate operation on projections receiving new data.</param>
        /// <param name="projectionStore">The projection store.</param>
        /// <returns>
        /// A disposable that, when disposed, unsubscribes the aggregator.
        /// </returns>
        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            return catchup.Subscribe(Aggregator.Create(aggregate),
                                     projectionStore.AsHandler());
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <returns>A disposable that, when disposed, unsubscribes the aggregator.</returns>
        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            FetchAndSaveProjection<TProjection> manageProjection,
            HandleAggregatorError<TProjection> onError = null)
        {
            return catchup.Subscribe(Aggregator.Create(aggregate), manageProjection, onError);
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregator">The aggregator.</param>
        /// <returns>A disposable that, when disposed, unsubscribes the aggregator.</returns>
        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSaveProjection<TProjection> manageProjection,
            HandleAggregatorError<TProjection> onError = null)
        {
            return catchup.SubscribeAggregator(aggregator, manageProjection, onError);
        }

        private static FetchAndSaveProjection<TProjection> NoCursor<TProjection>(TProjection projection)
        {
            return (streamId, aggregate) => aggregate(projection);
        }

        internal class Counter<TCursor> : Projection<int>
        {
            public Counter<TCursor> Count(IStreamBatch<TCursor> batch)
            {
                Value += batch.Count;
                return this;
            }
        }
    }
}