using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// An persistent query over a stream of data, which updates one or more stream aggregators.
    /// </summary>
    /// <typeparam name="TData">The type of the data that the catchup pushes to the aggregators.</typeparam>
    internal abstract class StreamCatchupBase<TData> : IStreamCatchup<TData>
    {
        private object batchTaskCompletionSource;
        protected readonly AggregatorSubscriptionList aggregatorSubscriptions;

        protected StreamCatchupBase(
            int? batchSize = null,
            AggregatorSubscriptionList aggregatorSubscriptions = null)
        {
            BatchSize = batchSize;
            this.aggregatorSubscriptions = aggregatorSubscriptions ??
                                           new AggregatorSubscriptionList();
        }

        protected int? BatchSize { get; }

        public IDisposable SubscribeAggregator<TProjection>(
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSave<TProjection> fetchAndSave,
            HandleAggregatorError<TProjection> onError)
        {
            var subscription = new AggregatorSubscription<TProjection, TData>(aggregator,
                                                                              fetchAndSave,
                                                                              onError);
            aggregatorSubscriptions.Add(subscription);

            return Disposable.Create(() =>
            {
                aggregatorSubscriptions.Remove(subscription);
            });
        }

        /// <summary>
        /// Consumes a single batch from the source stream and updates the subscribed aggregators.
        /// </summary>
        /// <returns>
        /// The updated cursor position after the batch is consumed.
        /// </returns>
        public abstract Task RunSingleBatch(ILease lease);

        protected async Task<ICursor<TUpstreamCursor>> RunSingleBatch<TUpstreamCursor>(
            IStream<TData, TUpstreamCursor> stream,
            bool synchronize,
            ILease lease,
            ICursor<TUpstreamCursor> initialCursor = null)
        {
            var tcs = new TaskCompletionSource<AggregationBatch<TUpstreamCursor>>();

            if (synchronize)
            {
                var exchange = Interlocked.CompareExchange<object>(ref batchTaskCompletionSource, tcs, null);

                if (exchange != null)
                {
                    Debug.WriteLine("[Catchup] RunSingleBatch returning early");
                    var batch = await ((TaskCompletionSource<AggregationBatch<TUpstreamCursor>>) batchTaskCompletionSource).Task;
                    return batch.Cursor;
                }
            }

            ICursor<TUpstreamCursor> upstreamCursor = null;

            var projections = new ConcurrentBag<object>();

            Action queryTheStream = async () =>
            {
                var cursor = initialCursor ??
                             projections.OfType<ICursor<TUpstreamCursor>>().MinOrDefault();
                upstreamCursor = cursor;
                var query = stream.CreateQuery(cursor, BatchSize);

                try
                {
                    var batch = await query.NextBatch();

                    if (!tcs.Task.IsCompleted)
                    {
                        tcs.SetResult(new AggregationBatch<TUpstreamCursor>
                        {
                            Cursor = query.Cursor,
                            Batch = batch
                        });
                    }
                }
                catch (Exception exception)
                {
                    Debug.WriteLine($"[Catchup] {exception}");
                    if (!tcs.Task.IsCompleted)
                    {
                        tcs.SetException(exception);
                    }
                }
            };

            var receivedProjectionsCount = 0;

            Func<object, Task<AggregationBatch<TUpstreamCursor>>> waitForProjectionsThenQueryTheStream = c =>
            {
                projections.Add(c);

                // we only want to query data from the stream once, when all projections have been received
                if (Interlocked.Increment(ref receivedProjectionsCount) == aggregatorSubscriptions.Count)
                {
                    queryTheStream();
                }

                return tcs.Task;
            };

            // create one aggregation task for each subscribed aggregator and await completion of all of them
            var aggregationTasks = aggregatorSubscriptions
                .Select(async subscription =>
                {
                    try
                    {
                        await Aggregate(stream,
                                        (dynamic) subscription,
                                        waitForProjectionsThenQueryTheStream);
                    }
                    catch (Exception exception)
                    {
                        var error = subscription.HandleError(
                            exception,
                            null);

                        if (!error.ShouldContinue)
                        {
                            throw;
                        }
                    }
                });

            await Task.WhenAll(aggregationTasks);

            batchTaskCompletionSource = null;

            return upstreamCursor;
        }

        private static Task Aggregate<TProjection, TUpstreamCursor>(
            IStream<TData, TUpstreamCursor> stream,
            AggregatorSubscription<TProjection, TData> subscription,
            Func<object, Task<AggregationBatch<TUpstreamCursor>>> getData) =>
                subscription.FetchAndSave(
                    stream.Id,
                    async projection =>
                    {
                        try
                        {
                            var aggregationBatch = await getData(projection);

                            var data = aggregationBatch.Batch;

                            projection = await subscription.Aggregator
                                                           .Aggregate(
                                                               projection,
                                                               data);

                            var cursor = projection as ICursor<TUpstreamCursor>;
                            cursor?.AdvanceTo(aggregationBatch.Cursor.Position);
                        }
                        catch (Exception exception)
                        {
                            var error = subscription.HandleError(
                                                        exception,
                                                        projection);
                      
                            if (!error.ShouldContinue)
                            {
                                throw;
                            }
                        }

                        return projection;
                    });

        protected struct AggregationBatch<TUpstreamCursor>
        {
            public ICursor<TUpstreamCursor> Cursor;
            public IStreamBatch<TData> Batch;
        }
    }
}