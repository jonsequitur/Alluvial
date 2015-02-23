using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class DistributorCatchup<TData> : StreamCatchupBase<TData>
    {
        private readonly IStreamCatchup<IStream<TData>> upstreamCatchup;

        public DistributorCatchup(
            IStreamCatchup<IStream<TData>> upstreamCatchup,
            ICursor cursor) : this(upstreamCatchup, (async (streamId, update) => await update(cursor)))
        {
        }

        public DistributorCatchup(
            IStreamCatchup<IStream<TData>> upstreamCatchup,
            FetchAndSaveProjection<ICursor> manageCursor)
        {
            if (upstreamCatchup == null)
            {
                throw new ArgumentNullException("upstreamCatchup");
            }
            if (manageCursor == null)
            {
                throw new ArgumentNullException("manageCursor");
            }
            this.upstreamCatchup = upstreamCatchup;

            upstreamCatchup.Subscribe(
                async (c, streams) =>
                {
                    await Task.WhenAll(streams.Select(RunSingleBatch));

                    return c;
                },
                manageCursor);
        }

        public override async Task<ICursor> RunSingleBatch()
        {
            return await upstreamCatchup.RunSingleBatch();
        }
    }
}