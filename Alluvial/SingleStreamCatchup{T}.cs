using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class SingleStreamCatchup<TData> : StreamCatchupBase<TData>
    {
        private readonly IStream<TData> stream;

        public SingleStreamCatchup(
            IStream<TData> stream,
            int? batchCount = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            this.stream = stream;
            this.batchCount = batchCount;
        }

        public override async Task<ICursor> RunSingleBatch()
        {
            return await RunSingleBatch(stream);
        }
    }
}