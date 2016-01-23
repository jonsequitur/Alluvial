using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// An persistent query over a stream of data, which updates one or more stream aggregators.
    /// </summary>
    /// <typeparam name="TData">The type of the data that the catchup pushes to the aggregators.</typeparam>
    /// <typeparam name="TUpstreamCursor">The type of the upstream cursor.</typeparam>
    /// <typeparam name="TDownstreamCursor">The type of the downstream cursors.</typeparam>
    [DebuggerDisplay("{ToString()}")]
    internal class MultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor> : StreamCatchupBase<TData, TUpstreamCursor>
    {
        private readonly IStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor> upstreamCatchup;
        private static readonly string catchupTypeDescription = typeof (MultiStreamCatchup<TData, TUpstreamCursor, TDownstreamCursor>).ReadableName();

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiStreamCatchup{TData, TUpstreamCursor, TDownstreamCursor}"/> class.
        /// </summary>
        /// <param name="upstreamCatchup">The upstream catchup.</param>
        /// <param name="upstreamCursor">The upstream cursor.</param>
        public MultiStreamCatchup(
            IStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor> upstreamCatchup,
            ICursor<TUpstreamCursor> upstreamCursor,
            ConcurrentDictionary<Type, IAggregatorSubscription> subscriptions = null) :
                this(upstreamCatchup,
                     (async (streamId, update) => await update(upstreamCursor)),
                     subscriptions)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiStreamCatchup{TData, TUpstreamCursor, TDownstreamCursor}" /> class.
        /// </summary>
        /// <param name="upstreamCatchup">The upstream catchup.</param>
        /// <param name="manageCursor">The manage cursor.</param>
        /// <exception cref="System.ArgumentNullException">
        /// upstreamCatchup
        /// or
        /// manageCursor
        /// </exception>
        /// <exception cref="ArgumentNullException">upstreamCatchup
        /// or
        /// manageCursor</exception>
        public MultiStreamCatchup(
            IStreamCatchup<IStream<TData, TDownstreamCursor>, TUpstreamCursor> upstreamCatchup,
            FetchAndSave<ICursor<TUpstreamCursor>> manageCursor,
            ConcurrentDictionary<Type, IAggregatorSubscription> subscriptions = null) : base(aggregatorSubscriptions: subscriptions)
        {
            if (upstreamCatchup == null)
            {
                throw new ArgumentNullException(nameof(upstreamCatchup));
            }
            if (manageCursor == null)
            {
                throw new ArgumentNullException(nameof(manageCursor));
            }
            this.upstreamCatchup = upstreamCatchup;

            upstreamCatchup.Subscribe(
                async (cursor, streams) =>
                {
                    // ths upstream cursor is not passed here because the downstream streams have their own independent cursors
                    await Task.WhenAll(streams.Select(s => RunSingleBatch(s, false)));

                    return cursor;
                },
                manageCursor);
        }

        /// <summary>
        /// Consumes a single batch from the source stream and updates the subscribed aggregators.
        /// </summary>
        /// <returns>
        /// The updated cursor position after the batch is consumed.
        /// </returns>
        public override async Task<ICursor<TUpstreamCursor>> RunSingleBatch() => await upstreamCatchup.RunSingleBatch();

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString() =>
            $"{catchupTypeDescription}->{upstreamCatchup}->{string.Join(" + ", aggregatorSubscriptions.Select(s => s.Value.ProjectionType.ReadableName()))}";
    }
}