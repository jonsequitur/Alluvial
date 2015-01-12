using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class DataStreamCatchup<TData> : IDataStreamCatchup<TData>
    {
        private int isRunning;
        private readonly IDataStream<IDataStream<TData>> dataStream;
        private readonly int? batchCount;

        private readonly ConcurrentDictionary<Type, AggregatorSubscription> aggregatorSubscriptions = new ConcurrentDictionary<Type, AggregatorSubscription>();

        public DataStreamCatchup(
            IDataStream<IDataStream<TData>> dataStream,
            ICursor cursor = null,
            int? batchCount = null)
        {
            if (dataStream == null)
            {
                throw new ArgumentNullException("dataStream");
            }

            Cursor = cursor;

            this.dataStream = dataStream;
            this.batchCount = batchCount;
        }

        public ICursor Cursor { get; set; }

        public IDisposable SubscribeAggregator<TProjection>(
            IDataStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore = null)
        {
            var added = aggregatorSubscriptions.TryAdd(typeof (TProjection),
                                                       new AggregatorSubscription<TProjection, TData>(aggregator, projectionStore));

            if (!added)
            {
                throw new InvalidOperationException(string.Format("Aggregator for projection of type {0} is already subscribed.", typeof (TProjection)));
            }

            return Disposable.Create(() =>
            {
                AggregatorSubscription _;
                aggregatorSubscriptions.TryRemove(typeof (TProjection), out _);
            });
        }

        public async Task<IStreamQuery<IDataStream<TData>>> RunSingleBatch()
        {
            if (Interlocked.CompareExchange(ref isRunning, 1, 0) != 0)
            {
                var streamQuery = dataStream.CreateQuery(Alluvial.Cursor.ReadOnly(Cursor));
                return streamQuery;
            }

            await EnsureCursorIsInitialized();

            var upstreamQuery = dataStream.CreateQuery(Cursor, batchCount);

            var streams = await upstreamQuery.NextBatch();

            if (streams.Any())
            {
                Exception aggregatorException = null;

                var batches =
                    streams.Select(
                        stream =>
                            // TODO: (RunSingleBatch) optimize: pull up the projection first and use its cursor?
                        {
                            var streamQuery = stream.CreateQuery();

                            return streamQuery
                                .NextBatch()
                                .ContinueWith(async t =>
                                {
                                    var batch = t.Result;

                                    if (batch.Count > 0)
                                    {
                                        var aggregatorUpdates =
                                            aggregatorSubscriptions
                                                .Values
                                                .Select(subscription => Aggregate(stream.Id,
                                                                                  (dynamic) subscription,
                                                                                  batch,
                                                                                  streamQuery.Cursor))
                                                .Cast<Task>();

                                        try
                                        {
                                            await Task.WhenAll(aggregatorUpdates);
                                        }
                                        catch (Exception exception)
                                        {
                                            aggregatorException = exception;
                                            throw;
                                        }
                                    }
                                });
                        });

                await Task.WhenAll(batches);

                if (aggregatorException != null)
                {
                    throw aggregatorException;
                }
            }

            await SaveCursor();

            isRunning = 0;

            return upstreamQuery;
        }

        private async Task SaveCursor()
        {
            // TODO: (SaveCursor) 
        }

        private async Task Aggregate<TProjection>(
            string streamId,
            AggregatorSubscription<TProjection, TData> subscription,
            IStreamQueryBatch<TData> batch,
            ICursor queryCursor)
        {
            var projection = await subscription.ProjectionStore.Get(streamId);

            var projectionCursor = projection as ICursor;
            if (projectionCursor != null)
            {
                // TODO: (Aggregate) optimize: this is unnecessary if we know we know this was a brand new projection
                batch = batch.Prune(projectionCursor);
            }

            projection = subscription.Aggregator.Aggregate(projection, batch);

            if (projectionCursor != null)
            {
                projectionCursor.AdvanceTo(queryCursor.Position);
            }

            await subscription.ProjectionStore.Put(projection);
        }

        private async Task EnsureCursorIsInitialized()
        {
            if (Cursor == null)
            {
                // TODO: (GetCursor) retrieve from storage
                Cursor = Alluvial.Cursor.New();
            }
        }
    }
}