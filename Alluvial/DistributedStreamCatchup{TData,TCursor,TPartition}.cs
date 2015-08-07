using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
    internal class DistributedStreamCatchup<TData, TCursor, TPartition> : StreamCatchupBase<TData, TCursor>
    {
        private static readonly string catchupTypeDescription = typeof (DistributedStreamCatchup<TData, TCursor, TPartition>).ReadableName();

        private readonly IPartitionedStream<TData, TCursor, TPartition> partitionedStream;

        // FIX: (DistributedStreamCatchup) need a way to dispose this distributor
        private readonly IDistributor<IStreamQueryPartition<TPartition>> distributor;

        public DistributedStreamCatchup(
            IPartitionedStream<TData, TCursor, TPartition> partitionedStream,
            IEnumerable<IStreamQueryPartition<TPartition>> partitions,
            int? batchSize = null,
            FetchAndSaveProjection<ICursor<TCursor>> manageCursor = null ) : base(batchSize)
        {
            if (partitionedStream == null)
            {
                throw new ArgumentNullException("partitionedStream");
            }
            if (partitions == null)
            {
                throw new ArgumentNullException("partitions");
            }

            this.partitionedStream = partitionedStream;

            distributor = partitions.DistributeQueriesInProcess();

            manageCursor = manageCursor ?? ((id, aggregate) => aggregate(null));

            distributor
#if DEBUG
                .Trace() // TODO: (DistributedStreamCatchup) figure out a way to let people Trace this distributor
#endif
                .OnReceive(async lease =>
                {
                    await manageCursor(lease.ResourceName, async cursor =>
                    {
                        var catchup = new SingleStreamCatchup<TData, TCursor>(
                            await partitionedStream.GetStream(lease.Resource),
                            initialCursor: cursor,
                            batchSize: batchSize,
                            subscriptions: new ConcurrentDictionary<Type, IAggregatorSubscription>(aggregatorSubscriptions));

                        return await catchup.RunSingleBatch();
                    });
                });
        }

        /// <summary>
        /// Consumes a single batch from the source stream and updates the subscribed aggregators.
        /// </summary>
        /// <returns>
        /// The updated cursor position after the batch is consumed.
        /// </returns>
        public override async Task<ICursor<TCursor>> RunSingleBatch()
        {
            var resources = distributor as IEnumerable<IStreamQueryPartition<TPartition>>;
            if (resources != null)
            {
                var partitions = new ConcurrentDictionary<IStreamQueryPartition<TPartition>, Unit>(
                    resources.Select(r => new KeyValuePair<IStreamQueryPartition<TPartition>, Unit>(r, Unit.Default)));

                while (partitions.Any())
                {
                    var acquired = await distributor.Distribute(1);

                    Unit _;
                    partitions.TryRemove(acquired.Single(), out _);
                }
            }
            else
            {
                // not all distributors have a finite set of known leasable resources
                await distributor.Distribute(1);
            }

            return Cursor.New<TCursor>();
        }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("{0}->{1}->{2}",
                                 catchupTypeDescription,
                                 partitionedStream,
                                 string.Join(" + ",
                                             aggregatorSubscriptions.Select(s => s.Value.ProjectionType.ReadableName())));
        }
    }
}