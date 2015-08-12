using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousPartitionedStream<TData, TCursor, TPartition> : IPartitionedStream<TData, TCursor, TPartition>
    {
        private readonly Func<IStreamQueryPartition<TPartition>, Task<IStream<TData, TCursor>>> getStream;
        private readonly string id;

        public AnonymousPartitionedStream(
            string id,
            Func<IStreamQueryPartition<TPartition>, Task<IStream<TData, TCursor>>> getStream) : this(id, fetch: async (q, p) => await (await getStream(p)).Fetch(q))
        {
        }

        public AnonymousPartitionedStream(
            string id,
            Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>,
                Task<IStreamBatch<TData>>> fetch,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null)
        {
            this.id = id ??
                      string.Format(@"{0}(d:{1} / c:{2} / p:{3})",
                                    fetch.GetHashCode(),
                                    typeof (TData).ReadableName(),
                                    typeof (TCursor).ReadableName(),
                                    typeof (TPartition).ReadableName());

            getStream = async partition => new StreamPartition(
                PartitionIdFor(partition),
                fetch,
                partition,
                advanceCursor,
                newCursor);
        }

        public string Id
        {
            get
            {
                return id;
            }
        }

        public async Task<IStream<TData, TCursor>> GetStream(IStreamQueryPartition<TPartition> partition)
        {
            return await getStream(partition);
        }

        private string PartitionIdFor(IStreamQueryPartition<TPartition> partition)
        {
            return string.Format("{0}/{1}", id, partition);
        }

        private class StreamPartition : AnonymousStreamBase<TData, TCursor>
        {
            private readonly Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, Task<IStreamBatch<TData>>> fetch;
            private readonly IStreamQueryPartition<TPartition> partition;
            private readonly string id;

            public StreamPartition(
                string id,
                Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>,
                    Task<IStreamBatch<TData>>> fetch,
                IStreamQueryPartition<TPartition> partition,
                Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
                Func<ICursor<TCursor>> newCursor = null) : base(advanceCursor, newCursor)
            {
                if (id == null)
                {
                    throw new ArgumentNullException("id");
                }
             
                if (fetch == null)
                {
                    throw new ArgumentNullException("fetch");
                }
                if (partition == null)
                {
                    throw new ArgumentNullException("partition");
                }   
                
                this.id = id;
                this.fetch = fetch;
                this.partition = partition;
            }

            public override string Id
            {
                get
                {
                    return id;
                }
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