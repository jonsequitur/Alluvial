using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// An persistent query over a stream of data, which updates one or more stream aggregators.
    /// </summary>
    /// <typeparam name="TData">The type of the data that the catchup pushes to the aggregators.</typeparam>
    /// <typeparam name="TCursor">The type of the cursor.</typeparam>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    [DebuggerDisplay("{ToString()}")]
    internal class DistributedSingleStreamCatchup<TData, TCursor, TPartition> :
        StreamCatchupBase<TData>,
        IDistributedStreamCatchup<TData, TPartition>
    {
        private static readonly string catchupTypeDescription = typeof (DistributedSingleStreamCatchup<TData, TCursor, TPartition>).ReadableName();

        protected readonly IPartitionedStream<TData, TCursor, TPartition> partitionedStream;

        protected readonly FetchAndSave<ICursor<TCursor>> fetchAndSavePartitionCursor;

        public DistributedSingleStreamCatchup(
            IPartitionedStream<TData, TCursor, TPartition> partitionedStream,
            IDistributor<IStreamQueryPartition<TPartition>> distributor, 
            int? batchSize = null,
            FetchAndSave<ICursor<TCursor>> fetchAndSavePartitionCursor = null) :
                base(batchSize)
        {
            if (distributor == null)
            {
                throw new ArgumentNullException(nameof(distributor));
            }
            this.partitionedStream = partitionedStream;
            Distributor = distributor;
            this.fetchAndSavePartitionCursor = fetchAndSavePartitionCursor ??
                                               new InMemoryProjectionStore<ICursor<TCursor>>(id => Cursor.New<TCursor>()).AsHandler();

            distributor.OnReceive(ReceiveLease);
        }

        public virtual async Task ReceiveLease(
            Lease<IStreamQueryPartition<TPartition>> lease) =>
                await fetchAndSavePartitionCursor(
                    lease.ResourceName,
                    async cursor =>
                    {
                        var upstreamCatchup = new SingleStreamCatchup<TData, TCursor>(
                            await partitionedStream.GetStream(lease.Resource),
                            initialCursor: cursor,
                            batchSize: BatchSize,
                            subscriptions: new AggregatorSubscriptionList(aggregatorSubscriptions));

                        await upstreamCatchup.RunSingleBatch(lease);

                        return cursor;
                    });

        public IDistributor<IStreamQueryPartition<TPartition>> Distributor { get; }

        /// <summary>
        /// Consumes a single batch from the source stream and updates the subscribed aggregators.
        /// </summary>
        public override async Task RunSingleBatch(ILease lease)
        {
            await Distributor.DistributeAll();
        }
        
        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString() =>
            $"{catchupTypeDescription}->{partitionedStream}->{string.Join(" + ", aggregatorSubscriptions.Select(s => s.ProjectionType.ReadableName()))}";
    }
}