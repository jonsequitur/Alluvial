using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with stream catchups.
    /// </summary>
    public static class StreamCatchup
    {
        /// <summary>
        /// Specifies an amount of time to wait if a stream produces no data.
        /// </summary>
        /// <typeparam name="TData">The type of the data in the stream.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="extendLeaseBy">The duration to wait.</param>
        public static IStreamCatchup<TData> Backoff<TData>(
            this IStreamCatchup<TData> catchup,
            TimeSpan? extendLeaseBy = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }

            extendLeaseBy = extendLeaseBy ?? TimeSpan.Zero;

            return catchup.Wrap(
                runSingleBatch: async lease =>
                {
                    using (var counter = catchup.Count())
                    {
                        await catchup.RunSingleBatch(lease);

                        if (counter.Value == 0)
                        {
                            await lease.Extend(extendLeaseBy.Value);
                            await lease.Expiration();
                        }
                    }
                });
        }

        /// <summary>
        /// Specifies an amount of time to wait if a stream produces no data.
        /// </summary>
        /// <typeparam name="TData">The type of the data in the stream.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="extendLeaseBy">The duration to wait.</param>
        /// <returns></returns>
        public static IDistributedStreamCatchup<TData, TPartition> Backoff<TData, TPartition>(
            this IDistributedStreamCatchup<TData, TPartition> catchup,
            TimeSpan? extendLeaseBy = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }

            extendLeaseBy = extendLeaseBy ?? TimeSpan.Zero;

            catchup.OnReceive(async (lease, next) =>
            {
                using (var counter = catchup.Count())
                {
                    await next(lease);

                    if (counter.Value == 0)
                    {
                        await lease.Extend(extendLeaseBy.Value);
                        await lease.Expiration();
                    }
                }
            });

            return catchup;
        }

        internal static Counter<TData> Count<TData>(
            this IStreamCatchup<TData> catchup)
        {
            var counter = new Counter<TData>();

            var subscription = catchup.Subscribe((_, batch) => counter.Add(batch).CompletedTask(),
                                                 NoCursor(counter));

            counter.OnDispose(subscription);

            return counter;
        }

        /// <summary>
        /// Creates a catchup for the specified stream.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="initialCursor">The initial cursor from which the catchup proceeds.</param>
        /// <param name="batchSize">The number of items to retrieve from the stream per batch.</param>
        /// <returns></returns>
        public static IStreamCatchup<TData> Create<TData, TCursor>(
            IStream<TData, TCursor> stream,
            ICursor<TCursor> initialCursor = null,
            int? batchSize = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

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
        public static IStreamCatchup<TData> All<TData, TUpstreamCursor, TDownstreamCursor>(
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
        public static IStreamCatchup<TData> All<TData, TUpstreamCursor, TDownstreamCursor>(
            IStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor> stream,
            FetchAndSave<ICursor<TUpstreamCursor>> manageUpstreamCursor,
            int? batchSize = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            if (manageUpstreamCursor == null)
            {
                throw new ArgumentNullException(nameof(manageUpstreamCursor));
            }

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
        public static IStreamCatchup<TData> All<TData, TUpstreamCursor, TDownstreamCursor>(
            IStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor> stream,
            IProjectionStore<string, ICursor<TUpstreamCursor>> upstreamCursorStore,
            int? batchSize = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            if (upstreamCursorStore == null)
            {
                throw new ArgumentNullException(nameof(upstreamCursorStore));
            }

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
        public static IDistributedStreamCatchup<TData, TPartition> CreateDistributedCatchup<TData, TCursor, TPartition>(
            this IPartitionedStream<TData, TCursor, TPartition> streams,
            IDistributor<IStreamQueryPartition<TPartition>> distributor,
            int? batchSize = null,
            FetchAndSave<ICursor<TCursor>> fetchAndSavePartitionCursor = null)
        {
            if (streams == null)
            {
                throw new ArgumentNullException(nameof(streams));
            }

            var catchup = new DistributedSingleStreamCatchup<TData, TCursor, TPartition>(
                streams,
                distributor,
                batchSize,
                fetchAndSavePartitionCursor);

            return catchup;
        }

        /// <summary>
        /// Distributes a stream catchup the among one or more partitions using a specified distributor.
        /// </summary>
        /// <remarks>If no distributor is provided, then distribution is done in-process.</remarks>
        public static IDistributedStreamCatchup<TData, TPartition> CreateDistributedCatchup<TData, TUpstreamCursor, TDownstreamCursor, TPartition>(
            this IPartitionedStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor, TPartition> streams,
            IDistributor<IStreamQueryPartition<TPartition>> distributor, 
            int? batchSize = null,
            FetchAndSave<ICursor<TUpstreamCursor>> fetchAndSavePartitionCursor = null)
        {
            if (streams == null)
            {
                throw new ArgumentNullException(nameof(streams));
            }

            var catchup = new DistributedMultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor, TPartition>(
                streams,
                distributor,
                batchSize,
                fetchAndSavePartitionCursor);

            return catchup;
        }

        /// <summary>
        /// Runs the catchup query until it reaches an empty batch, then stops.
        /// </summary>
        public static async Task RunUntilCaughtUp<TData>(
            this IStreamCatchup<TData> catchup,
            ILease lease = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }

            using (var counter = catchup.Count())
            {
                int countBefore;
                do
                {
                    countBefore = counter.Value;
                    await catchup.RunSingleBatch(lease ?? Lease.CreateDefault());
                } while (countBefore != counter.Value);
            }
        }

        /// <summary>
        /// Runs catchup batches repeatedly with a specified interval after each batch.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="pollInterval">The amount of time to wait after each batch is processed.</param>
        /// <returns>A disposable that, when disposed, stops the polling.</returns>
        public static IDisposable Poll<TData>(
            this IStreamCatchup<TData> catchup,
            TimeSpan pollInterval)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }

            var canceled = false;

            Task.Run(async () =>
            {
                while (!canceled)
                {
                    await catchup.RunSingleBatch(Lease.CreateDefault());
                    await Task.Delay(pollInterval);
                }
            });

            return Disposable.Create(() => { canceled = true; });
        }

        /// <summary>
        /// Consumes a single batch from the source stream and updates the subscribed aggregators.
        /// </summary>
        /// <param name="catchup">The catchup.</param>
        /// <returns>
        /// The updated cursor position after the batch is consumed.
        /// </returns>
        public static async Task RunSingleBatch<T>(this IStreamCatchup<T> catchup)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }

            await catchup.RunSingleBatch(Lease.CreateDefault());
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="projectionStore">The projection store.</param>
        /// <param name="onError">A function to handle exceptions thrown during aggregation.</param>
        /// <returns>A disposable that, when disposed, unsubscribes the aggregator.</returns>
        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }
            if (aggregator == null)
            {
                throw new ArgumentNullException(nameof(aggregator));
            }

            return catchup.Subscribe(aggregator,
                                     projectionStore.AsHandler(),
                                     onError);
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="manage">A delegate to fetch and store the projection each time the query is performed.</param>
        /// <param name="onError">A function to handle exceptions thrown during aggregation.</param>
        /// <returns>A disposable that, when disposed, unsubscribes the aggregator.</returns>
        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSave<TProjection> manage,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }
            if (aggregator == null)
            {
                throw new ArgumentNullException(nameof(aggregator));
            }
            if (manage == null)
            {
                throw new ArgumentNullException(nameof(manage));
            }

            return catchup.SubscribeAggregator(aggregator,
                                               manage,
                                               onError);
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregate">A delegate that performs an aggregate operation on projections receiving new data.</param>
        /// <param name="projectionStore">The projection store.</param>
        /// <param name="onError">A function to handle exceptions thrown during aggregation.</param>
        /// <returns>
        /// A disposable that, when disposed, unsubscribes the aggregator.
        /// </returns>
        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            Aggregate<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }
            if (aggregate == null)
            {
                throw new ArgumentNullException(nameof(aggregate));
            }

            return catchup.Subscribe(Aggregator.Create(aggregate),
                                     projectionStore.AsHandler(),
                                     onError);
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregate">An aggregator function.</param>
        /// <param name="manage">A delegate to fetch and store the projection each time the query is performed.</param>
        /// <param name="onError">A function to handle exceptions thrown during aggregation.</param>
        /// <returns>
        /// A disposable that, when disposed, unsubscribes the aggregator.
        /// </returns>
        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            Aggregate<TProjection, TData> aggregate,
            FetchAndSave<TProjection> manage,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }
            if (aggregate == null)
            {
                throw new ArgumentNullException(nameof(aggregate));
            }
            if (manage == null)
            {
                throw new ArgumentNullException(nameof(manage));
            }

            return catchup.Subscribe(Aggregator.Create(aggregate), manage, onError);
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregate">A delegate that performs an aggregate operation on projections receiving new data.</param>
        /// <param name="projectionStore">The projection store.</param>
        /// <param name="onError">A function to handle exceptions thrown during aggregation.</param>
        /// <returns>
        /// A disposable that, when disposed, unsubscribes the aggregator.
        /// </returns>
        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            IProjectionStore<string, TProjection> projectionStore = null,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }
            if (aggregate == null)
            {
                throw new ArgumentNullException(nameof(aggregate));
            }

            return catchup.Subscribe(Aggregator.Create(aggregate),
                                     projectionStore.AsHandler(),
                                     onError);
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregate">An aggregator function.</param>
        /// <param name="manage">A delegate to fetch and store the projection each time the query is performed.</param>
        /// <param name="onError">A function to handle exceptions thrown during aggregation.</param>
        /// <returns>
        /// A disposable that, when disposed, unsubscribes the aggregator.
        /// </returns>
        public static IDisposable Subscribe<TProjection, TData>(
            this IStreamCatchup<TData> catchup,
            AggregateAsync<TProjection, TData> aggregate,
            FetchAndSave<TProjection> manage,
            HandleAggregatorError<TProjection> onError = null)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }
            if (aggregate == null)
            {
                throw new ArgumentNullException(nameof(aggregate));
            }
            if (manage == null)
            {
                throw new ArgumentNullException(nameof(manage));
            }

            return catchup.Subscribe(Aggregator.Create(aggregate), manage, onError);
        }

        /// <summary>
        /// Subscribes the specified aggregator to a catchup.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="catchup">The catchup.</param>
        /// <param name="aggregate">A side-effecting aggregator function.</param>
        /// <returns>A disposable that, when disposed, unsubscribes the aggregator.</returns>
        public static IDisposable Subscribe<TData>(
            this IStreamCatchup<TData> catchup,
            Func<IStreamBatch<TData>, Task> aggregate)
        {
            if (catchup == null)
            {
                throw new ArgumentNullException(nameof(catchup));
            }
            if (aggregate == null)
            {
                throw new ArgumentNullException(nameof(aggregate));
            }

            return catchup.Subscribe(
                Aggregator.Create<Projection<Unit, Unit>, TData>(async (p, b) =>
                {
                    await aggregate(b);
                    return p;
                }),
                new InMemoryProjectionStore<Projection<Unit, Unit>>());
        }

        private static FetchAndSave<TProjection> NoCursor<TProjection>(TProjection projection) =>
            (streamId, aggregate) => aggregate(projection);

        internal class Counter<TCursor> : Projection<int>, IDisposable
        {
            private IDisposable onDispose;

            public Counter<TCursor> Add(IStreamBatch<TCursor> batch)
            {
                Value += batch.Count;
                return this;
            }

            public void OnDispose(IDisposable disposable)
            {
                if (onDispose != null)
                {
                    onDispose = Disposable.Create(() =>
                    {
                        onDispose.Dispose();
                        disposable.Dispose();
                    });
                }
                else
                {
                    onDispose = disposable;
                }
            }

            public void Dispose() => onDispose?.Dispose();
        }

        internal static Wrapper<TData> Wrap<TData>(
            this IStreamCatchup<TData> innerCatchup,
            Func<ILease, Task> runSingleBatch,
            [CallerMemberName] string caller = null)
        {
            return new Wrapper<TData>(innerCatchup,
                                      runSingleBatch,
                                      caller);
        }

        internal static Wrapper<TData, TPartition> Wrap<TData, TPartition>(
            this IDistributedStreamCatchup<TData, TPartition> innerCatchup,
            Func<ILease, Task> runSingleBatch,
            [CallerMemberName] string caller = null)
        {
            return new Wrapper<TData, TPartition>(innerCatchup,
                                                  runSingleBatch,
                                                  caller);
        }

        internal class Wrapper<TData> : IStreamCatchup<TData>
        {
            private readonly IStreamCatchup<TData> inner;
            private readonly Func<ILease, Task> runSingleBatch;
            private readonly string caller;

            public Wrapper(
                IStreamCatchup<TData> inner,
                Func<ILease, Task> runSingleBatch,
                string caller)
            {
                if (inner == null)
                {
                    throw new ArgumentNullException(nameof(inner));
                }
                if (runSingleBatch == null)
                {
                    throw new ArgumentNullException(nameof(runSingleBatch));
                }
                this.inner = inner;
                this.runSingleBatch = runSingleBatch;

                var wrapper = inner as Wrapper<TData>;
                if (wrapper != null)
                {
                    caller = $"{wrapper.caller} -> {caller}";
                }

                this.caller = caller;
            }

            public IDisposable SubscribeAggregator<TProjection>(
                IStreamAggregator<TProjection, TData> aggregator,
                FetchAndSave<TProjection> fetchAndSave,
                HandleAggregatorError<TProjection> onError)
                =>
                    inner.SubscribeAggregator(aggregator,
                                              fetchAndSave,
                                              onError);

            public Task RunSingleBatch(ILease lease) => runSingleBatch(lease);

            public override string ToString()
            {
                return $"StreamCatchup {caller}";
            }
        }

        internal class Wrapper<TData, TPartition> :
            Wrapper<TData>,
            IDistributedStreamCatchup<TData, TPartition>
        {
            private readonly IDistributedStreamCatchup<TData, TPartition> inner;

            public Wrapper(
                IDistributedStreamCatchup<TData, TPartition> inner,
                Func<ILease, Task> runSingleBatch,
                string caller) :
                    base(inner,
                         runSingleBatch,
                         caller)
            {
                this.inner = inner;
            }

            public void Dispose() => inner.Dispose();

            public void OnReceive(DistributorPipeAsync<IStreamQueryPartition<TPartition>> onReceive) => inner.OnReceive(onReceive);

            public void OnException(Action<Exception, Lease<IStreamQueryPartition<TPartition>>> onException) => inner.OnException(onException);

            public Task Start() => inner.Start();

            public Task<IEnumerable<IStreamQueryPartition<TPartition>>> Distribute(int count) => inner.Distribute(count);

            public Task Stop() => inner.Stop();
        }
    }
}