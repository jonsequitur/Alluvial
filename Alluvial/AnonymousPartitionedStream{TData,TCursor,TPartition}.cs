using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousPartitionedStream<TData, TCursor, TPartition> : AnonymousStreamBase<TData, TCursor>
    {
        private readonly Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, Task<IStreamBatch<TData>>> fetch;
        private readonly IStreamQueryPartition<TPartition> partition;

        public AnonymousPartitionedStream(
            string id,
            Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>,
                Task<IStreamBatch<TData>>> fetch,
            IStreamQueryPartition<TPartition> partition,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null) : base(id, advanceCursor, newCursor)
        {
            if (fetch == null)
            {
                throw new ArgumentNullException("fetch");
            }
            if (partition == null)
            {
                throw new ArgumentNullException("partition");
            }
            this.fetch = fetch;
            this.partition = partition;
        }

        public override async Task<IStreamBatch<TData>> Fetch(IStreamQuery<TCursor> query)
        {
            var batch = await fetch(query, partition);

            AdvanceCursor(query, batch);

            return batch;
        }


    }
}