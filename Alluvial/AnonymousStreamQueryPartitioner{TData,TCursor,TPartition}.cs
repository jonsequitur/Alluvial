using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousStreamQueryPartitioner<TData, TCursor, TPartition> : IStreamQueryPartitioner<TData, TCursor, TPartition>
    {
        private readonly string id;
        private readonly Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, Task<IStreamBatch<TData>>> fetch;
        private readonly Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor;
        private readonly Func<ICursor<TCursor>> newCursor;

        public AnonymousStreamQueryPartitioner(
            string id,
            Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>,
                Task<IStreamBatch<TData>>> fetch,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }
            if (fetch == null)
            {
                throw new ArgumentNullException("fetch");
            }
            this.id = id;
            this.fetch = fetch;
            this.advanceCursor = advanceCursor;
            this.newCursor = newCursor;
        }

        public async Task<IStream<TData, TCursor>> GetStream(IStreamQueryPartition<TPartition> partition)
        {
            var streamId = string.Format("{0}[{1}]", id, partition);
            return new AnonymousPartitionedStream<TData, TCursor, TPartition>(
                streamId,
                fetch,
                partition,
                advanceCursor,
                newCursor);
        }
    }
}