using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class SingleStreamCatchup<TData, TCursor> : StreamCatchupBase<TData, TCursor>
    {
        private readonly IStream<TData, TCursor> stream;

        public SingleStreamCatchup(
            IStream<TData, TCursor> stream,
            int? batchCount = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            this.stream = stream;
            this.batchCount = batchCount;
        }

        public override async Task<ICursor<TCursor>> RunSingleBatch()
        {
            return await RunSingleBatch(stream);
        }
    }
}