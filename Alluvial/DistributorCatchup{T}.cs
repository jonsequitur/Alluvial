using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class DistributorCatchup<TData> : StreamCatchupBase<TData>
    {
        private readonly IStreamCatchup<IStream<TData>> upstreamCatchup;

        public DistributorCatchup(IStreamCatchup<IStream<TData>> upstreamCatchup, ICursor cursor)
        {
            if (upstreamCatchup == null)
            {
                throw new ArgumentNullException("upstreamCatchup");
            }
            this.upstreamCatchup = upstreamCatchup;

            upstreamCatchup.Subscribe<ICursor, IStream<TData>>(async (_, streams) =>
            {
                await Task.WhenAll(streams.Select(RunSingleBatch));

                return cursor;
            }, 
            async (streamId, update) =>
            {
                await update(cursor);
            });
        }

        public override async Task<ICursor> RunSingleBatch()
        {
            return await upstreamCatchup.RunSingleBatch();
        }
    }
}