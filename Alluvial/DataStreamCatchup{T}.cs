using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class DataStreamCatchup<TData> : IDataStreamCatchup<TData>, IDisposable
    {
        private int isRunning;
        private readonly IDataStream<IDataStream<TData>> dataStream;
        private readonly int? batchCount;

        private readonly Dictionary<Type, AggregatorSubscription> aggregatorSubscriptions = new Dictionary<Type, AggregatorSubscription>();

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

        public void SubscribeAggregator<TProjection>(
            IDataStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore)
        {
            aggregatorSubscriptions.Add(typeof (TProjection),
                                        new AggregatorSubscription<TProjection, TData>(aggregator, projectionStore));
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
                var batches =
                    streams.Select(
                        stream => stream.CreateQuery()
                                        .NextBatch()
                                        .ContinueWith(async t =>
                                        {
                                            var batch = t.Result;

                                            if (batch.Any())
                                            {
                                                var aggregatorUpdates =
                                                    aggregatorSubscriptions.Values
                                                                           .Select(subscription => Aggregate(stream.Id,
                                                                                                             (dynamic) subscription,
                                                                                                             batch))
                                                                           .Cast<Task>();

                                                await Task.WhenAll(aggregatorUpdates);
                                            }
                                        }));

                await Task.WhenAll(batches);
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
            IStreamQueryBatch<TData> batch)
        {
            var projection = await subscription.ProjectionStore.Get(streamId);

            projection = subscription.Aggregator.Aggregate(projection, batch);

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

        public void Dispose()
        {
        }
    }

    internal abstract class AggregatorSubscription
    {
    }

    internal class AggregatorSubscription<TProjection, TData> : AggregatorSubscription
    {
        public AggregatorSubscription(
            IDataStreamAggregator<TProjection, TData> aggregator,
            IProjectionStore<string, TProjection> projectionStore)
        {
            if (projectionStore == null)
            {
                throw new ArgumentNullException("projectionStore");
            }
            if (aggregator == null)
            {
                throw new ArgumentNullException("aggregator");
            }
            ProjectionStore = projectionStore;
            Aggregator = aggregator;
        }

        public IDataStreamAggregator<TProjection, TData> Aggregator { get; private set; }
        
        public IProjectionStore<string, TProjection> ProjectionStore { get; private set; }
    }
}