using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class DataStreamCatchup<TData> : IDataStreamCatchup<TData>, IDisposable
    {
        private int isRunning = 0;
        private readonly IDataStream<IDataStream<TData>> dataStream;

        private readonly Dictionary<Type, AggregatorSubscription> aggregatorSubscriptions = new Dictionary<Type, AggregatorSubscription>();

        public DataStreamCatchup(IDataStream<IDataStream<TData>> dataStream)
        {
            if (dataStream == null)
            {
                throw new ArgumentNullException("dataStream");
            }
            this.dataStream = dataStream;
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

            var outerQuery = dataStream.CreateQuery(Cursor);

            var streams = await outerQuery.NextBatch();

            if (streams.Any())
            {
                foreach (var stream in streams)
                {
                    var innerQuery = stream.CreateQuery();

                    var batch = await innerQuery.NextBatch();

                    if (batch.Any())
                    {
                        // TODO-JOSEQU: (RunSingleBatch) parallellize projection updates
                        foreach (var subscription in aggregatorSubscriptions.Values)
                        {
                            await Aggregate(stream.Id, (dynamic) subscription, batch);
                        }
                    }
                }
            }

            await SaveCursor();

            return outerQuery;
        }

        private async Task SaveCursor()
        {
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
                // FIX-JOSEQU: (GetCursor) retrieve from storage
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