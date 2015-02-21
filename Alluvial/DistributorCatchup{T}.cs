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
            ICursor cursor)
        {
            if (upstreamCatchup == null)
            {
                throw new ArgumentNullException("upstreamCatchup");
            }
            if (cursor == null)
            {
                throw new ArgumentNullException("cursor");
            }
            this.upstreamCatchup = upstreamCatchup;

            upstreamCatchup.Subscribe<ICursor, IStream<TData>>(
                async (c, streams) =>
                {
                    await Task.WhenAll(streams.Select(RunSingleBatch));

                    return c;
                },
                (async (streamId, update) => { await update(null, cursor); }));
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