using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousPartitionedStream<TData, TCursor, TPartition> : IPartitionedStream<TData, TCursor, TPartition>
    {
        private readonly string id;
        private readonly Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, Task<IStreamBatch<TData>>> fetch;
        private readonly Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor;
        private readonly Func<ICursor<TCursor>> newCursor;

        public AnonymousPartitionedStream(
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
            return new StreamPartition<TData, TCursor, TPartition>(
                streamId,
                fetch,
                partition,
                advanceCursor,
                newCursor);
        }

        private class StreamPartition<TData, TCursor, TPartition> : AnonymousStreamBase<TData, TCursor>
        {
            private readonly Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, Task<IStreamBatch<TData>>> fetch;
            private readonly IStreamQueryPartition<TPartition> partition;

            public StreamPartition(
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
}