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
            ICursor cursor,
            FetchAndSaveProjection<ICursor> manageCursor = null)
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

            Cursor = cursor;

            upstreamCatchup.Subscribe(
                async (c, streams) =>
                {
                    await Task.WhenAll(streams.Select(RunSingleBatch));

                    return c;
                },
                manageCursor ??
                (async (streamId, update) =>
                {
                    await update(null, Cursor);
                }));
        }

        public ICursor Cursor { get; private set; }

        public override async Task<ICursor> RunSingleBatch()
        {
            return await upstreamCatchup.RunSingleBatch();
        }
    }
}