using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    [DebuggerDisplay("{ToString()}")]
    internal class DistributedMultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor, TPartition> :
        DistributedSingleStreamCatchup<TData, TUpstreamCursor, TPartition>
    {
        private readonly IPartitionedStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor, TPartition> partitionedStreams;

        public DistributedMultiStreamCatchup(
            IPartitionedStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor, TPartition> partitionedStream,
            int? batchSize = null,
            FetchAndSave<ICursor<TUpstreamCursor>> fetchAndSavePartitionCursor = null)
            : base(null,
                   batchSize, 
                   fetchAndSavePartitionCursor)
        {
            if (partitionedStream == null)
            {
                throw new ArgumentNullException(nameof(partitionedStream));
            }
            partitionedStreams = partitionedStream;
        }

        public async override Task ReceiveLease(
            Lease<IStreamQueryPartition<TPartition>> lease) =>
            await fetchAndSavePartitionCursor(
                lease.ResourceName,
                async cursor =>
                {
                    var upstreamCatchup =
                        new SingleStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor>(
                            await partitionedStreams.GetStream(lease.Resource),
                            initialCursor: cursor,
                            batchSize: BatchSize);

                    var downstreamCatchup = new MultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor>(
                        upstreamCatchup,
                        cursor.Clone(),
                        subscriptions: new AggregatorSubscriptionList(aggregatorSubscriptions));

                    await upstreamCatchup.RunSingleBatch(lease);

                    return cursor;
                });
    }
}