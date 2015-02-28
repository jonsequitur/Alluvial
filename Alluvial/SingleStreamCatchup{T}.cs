using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    [DebuggerDisplay("{ToString()}")]
    internal class SingleStreamCatchup<TData, TCursor> : StreamCatchupBase<TData, TCursor>
    {
        private readonly IStream<TData, TCursor> stream;
        private static readonly string catchupTypeDescription = typeof (SingleStreamCatchup<TData, TCursor>).ReadableName();

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

        public override string ToString()
        {
            return string.Format("{0}->{1}->{2}",
                                 catchupTypeDescription,
                                 stream.Id, string.Join(" + ",
                                                        aggregatorSubscriptions.Select(s => s.Value.ProjectionType.ReadableName())));
        }
    }
}