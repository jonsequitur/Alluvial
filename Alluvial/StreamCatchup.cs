using System;
using System.Collections.Generic;
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
        public static IStreamCatchup<TData, TCursor> Create<TData, TCursor>(
            IStream<TData, TCursor> stream,
            ICursor<TCursor> initialCursor = null,
            int? batchSize = null) =>
                new SingleStreamCatchup<TData, TCursor>(
                    stream,
                    initialCursor,
                    batchSize);

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
        /// <param name="manageUpstreamCursor">A delegate to fetch and store the cursor each time the query is performed.</param>
        /// <param name="batchSize">The number of items to retrieve from the stream per batch.</param>
        public static IStreamCatchup<TData, TUpstreamCursor> All<TData, TUpstreamCursor, TDownstreamCursor>(
            IStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor> stream,
            FetchAndSave<ICursor<TUpstreamCursor>> manageUpstreamCursor,
            int? batchSize = null)
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor>(
                stream,
                batchSize: batchSize);

            return new MultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor>(
                upstreamCatchup,
                manageUpstreamCursor);
        }

        /// <summary>
        /// Creates a multiple-stream catchup.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
        /// <typeparam name="TDownstreamCursor">The type of the downstream cursor.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="upstreamCursorStore">A store for the upstream cursor.</param>
        /// <param name="batchSize">The number of items to retrieve from the stream per batch.</param>
        public static IStreamCatchup<TData, TUpstreamCursor> All<TData, TUpstreamCursor, TDownstreamCursor>(
            IStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor> stream,
            IProjectionStore<string, ICursor<TUpstreamCursor>> upstreamCursorStore,
            int? batchSize = null)
        {
            var upstreamCatchup = new SingleStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor>(
                stream,
                batchSize: batchSize);

            return new MultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor>(
                upstreamCatchup,
                upstreamCursorStore.AsHandler());
        }

        /// <summary>
        /// Distributes a stream catchup the among one or more partitions using a specified distributor.
        /// </summary>
        /// <remarks>If no distributor is provided, then distribution is done in-process.</remarks>
        public static IStreamCatchup<TData, TCursor> DistributeAmong<TData, TCursor, TPartition>(
            this IPartitionedStream<TData, TCursor, TPartition> streams,
            IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            int? batchSize = null,
            FetchAndSave<ICursor<TCursor>> fetchAndSavePartitionCursor = null,
            IDistributor<IStreamQueryPartition<TPartition>> distributor = null) =>
                new DistributedSingleStreamCatchup<TData, TCursor, TPartition>(
                    streams,
                    partitions,
                    batchSize,
                    fetchAndSavePartitionCursor,
                    distributor);

        /// <summary>
        /// Distributes a stream catchup the among one or more partitions using a specified distributor.
        /// </summary>
        /// <remarks>If no distributor is provided, then distribution is done in-process.</remarks>
        public static IStreamCatchup<TData, TUpstreamCursor> DistributeAmong<TData, TUpstreamCursor, TDownstreamCursor, TPartition>(
            this IPartitionedStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor, TPartition> streams,
            IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            int? batchSize = null,
            FetchAndSave<ICursor<TUpstreamCursor>> fetchAndSavePartitionCursor = null,
            IDistributor<IStreamQueryPartition<TPartition>> distributor = null) =>
                new DistributedMultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor, TPartition>(
                    streams,
                    partitions,
                    batchSize,
                    fetchAndSavePartitionCursor,
                    distributor);

        /// <summary>
        /// Runs the catchup query until it reaches an empty batch, then stops.
        /// </summary>
        public static async Task<ICursor<TCursor>> RunUntilCaughtUp<TData, TCursor>(this IStreamCatchup<TData, TCursor> catchup)
        {
            ICursor<TCursor> cursor;
            var counter = new Counter<TData>();

            using (catchup.Subscribe((_, batch) => counter.Count(batch).CompletedTask(), NoCursor(counter)))
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
            IProjectionStore<string, TProjection> projectionStore = null) =>
                catchup.Subscribe(aggregator,
                                  projectionStore.AsHandler());

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
            IProjectionStore<string, TProjection> projectionStore = null) =>
                catchup.Subscribe(Aggregator.Create(aggregate),
                                  projectionStore.AsHandler());

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregate">An aggregator function.</param>
        /// <param name="manage">A delegate to fetch and store the projection each time the query is performed.</param>
        /// <param name="onError">A function to handle exceptions thrown during aggregation.</param>
        /// <returns>
        /// A disposable that, when disposed, unsubscribes the aggregator.
        /// </returns>
        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            FetchAndSave<TProjection> manage,
            HandleAggregatorError<TProjection> onError = null) =>
                catchup.Subscribe(Aggregator.Create(aggregate), manage, onError);

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="manage">A delegate to fetch and store the projection each time the query is performed.</param>
        /// <param name="onError">A function to handle exceptions thrown during aggregation.</param>
        /// <returns>A disposable that, when disposed, unsubscribes the aggregator.</returns>
        public static IDisposable Subscribe<TProjection, TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSave<TProjection> manage,
            HandleAggregatorError<TProjection> onError = null) =>
                catchup.SubscribeAggregator(aggregator, manage, onError);

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregate">The aggregate.</param>
        /// <returns>A disposable that, when disposed, unsubscribes the aggregator.</returns>
        public static IDisposable Subscribe<TData, TCursor>(
            this IStreamCatchup<TData, TCursor> catchup,
            Func<IStreamBatch<TData>, Task> aggregate) =>
                catchup.Subscribe(
                    Aggregator.Create<Projection<Unit, TCursor>, TData>(async (p, b) =>
                    {
                        await aggregate(b);
                        return p;
                    }),
                    new InMemoryProjectionStore<Projection<Unit, TCursor>>());

        private static FetchAndSave<TProjection> NoCursor<TProjection>(TProjection projection) =>
            (streamId, aggregate) => aggregate(projection);

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