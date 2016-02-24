using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Distributors;

namespace Alluvial
{
    [DebuggerDisplay("{ToString()}")]
    internal class DistributedMultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor, TPartition> :
        DistributedSingleStreamCatchup<TData, TUpstreamCursor, TPartition>
    {
        private readonly IPartitionedStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor, TPartition> partitionedStreams;

        public DistributedMultiStreamCatchup(
            IPartitionedStream<IStream<TData, TDownstreamCursor>, TUpstreamCursor, TPartition> partitionedStream,
            IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            int? batchSize = null,
            FetchAndSave<ICursor<TUpstreamCursor>> fetchAndSavePartitionCursor = null,
            IDistributor<IStreamQueryPartition<TPartition>> distributor = null)
            : base(null,
                   partitions,
                   distributor, 
                   batchSize, 
                   fetchAndSavePartitionCursor)
        {
            if (partitionedStream == null)
            {
                throw new ArgumentNullException(nameof(partitionedStream));
            }
            partitionedStreams = partitionedStream;
        }

        protected override async Task OnReceiveLease(Lease<IStreamQueryPartition<TPartition>> lease) =>
            await fetchAndSavePartitionCursor(
                lease.ResourceName,
                async cursor =>
                {
                    IStreamCatchup<IStream<TData, TDownstreamCursor>> upstreamCatchup =
                    
                    new SingleStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor>(
                        await partitionedStreams.GetStream(lease.Resource),
                        initialCursor: cursor,
                        batchSize: BatchSize);

                    if (configureChildCatchup != null)
                    {
                        upstreamCatchup = configureChildCatchup(upstreamCatchup);
                    }

                    var downstreamCatchup = new MultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor>(
                        upstreamCatchup,
                        cursor.Clone(),
                        subscriptions: new ConcurrentDictionary<Type, IAggregatorSubscription>(aggregatorSubscriptions));

                    await upstreamCatchup.RunSingleBatch(lease);

                    return cursor;
                });

        private Func<IStreamCatchup<IStream<TData, TDownstreamCursor>>, IStreamCatchup<IStream<TData, TDownstreamCursor>>> configureChildCatchup;
    }
}