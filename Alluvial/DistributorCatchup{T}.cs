using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class DistributorCatchup<TData, TUpstreamCursor, TDownstreamCursor> : StreamCatchupBase<TData, TUpstreamCursor>
    {
        private readonly IStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor> upstreamCatchup;

        public DistributorCatchup(
            IStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor> upstreamCatchup,
            ICursor<TUpstreamCursor> cursor) : this(upstreamCatchup, (async (streamId, update) => await update(cursor)))
        {
        }

        public DistributorCatchup(
            IStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor> upstreamCatchup,
            FetchAndSaveProjection<ICursor<TUpstreamCursor>> manageCursor)
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

        public override async Task<ICursor<TUpstreamCursor>> RunSingleBatch()
        {
            return await upstreamCatchup.RunSingleBatch();
        }
    }
}