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
    /// <typeparam name="TCursor">The type of the cursor.</typeparam>
    [DebuggerDisplay("{ToString()}")]
    internal class SingleStreamCatchup<TData, TCursor> : StreamCatchupBase<TData, TCursor>
    {
        private readonly IStream<TData, TCursor> stream;
        private readonly ICursor<TCursor> initialCursor;
        private static readonly string catchupTypeDescription = typeof (SingleStreamCatchup<TData, TCursor>).ReadableName();

        public SingleStreamCatchup(
            IStream<TData, TCursor> stream,
            ICursor<TCursor> initialCursor = null,
            int? batchSize = null,
            ConcurrentDictionary<Type, IAggregatorSubscription> subscriptions = null) :
                base(batchSize, subscriptions)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            this.stream = stream;
            this.initialCursor = initialCursor;
        }

        /// <summary>
        /// Consumes a single batch from the source stream and updates the subscribed aggregators.
        /// </summary>
        /// <returns>
        /// The updated cursor position after the batch is consumed.
        /// </returns>
        public override async Task<ICursor<TCursor>> RunSingleBatch()
        {
            return await RunSingleBatch(stream, initialCursor);
        }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("{0}->{1}->{2}",
                                 catchupTypeDescription,
                                 stream.Id, string.Join(" + ",
                                                        aggregatorSubscriptions.Select(s => s.Value.ProjectionType.ReadableName())));
        }
    }
}