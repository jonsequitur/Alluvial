using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with stream query batches.
    /// </summary>
    public static class StreamBatch
    {
        internal const int MaxSize = 1000;

        /// <summary>
        /// Creates a stream query batch from an enumerable sequence.
        /// </summary>
        /// <typeparam name="TData">The type of the data in the batch.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="source">The source data.</param>
        /// <param name="cursor">The cursor that marks the location of the beginning of the batch within the source stream.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException">
        /// source
        /// or
        /// cursor
        /// </exception>
        /// <exception cref="ArgumentNullException">source</exception>
        public static IStreamBatch<TData> Create<TData, TCursor>(
            IEnumerable<TData> source,
            ICursor<TCursor> cursor)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (cursor == null)
            {
                throw new ArgumentNullException(nameof(cursor));
            }

            var results = source.ToArray();

            return new StreamBatch<TData>(results, cursor.Position);
        }

        /// <summary>
        /// Represents an empty stream query batch.
        /// </summary>
        /// <typeparam name="TData">The type of the data in the source stream.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="cursor">The cursor that marks the location of the beginning of the batch within the source stream.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException">cursor</exception>
        /// <exception cref="ArgumentNullException">cursor</exception>
        public static IStreamBatch<TData> EmptyBatch<TData, TCursor>(this ICursor<TCursor> cursor)
        {
            if (cursor == null)
            {
                throw new ArgumentNullException(nameof(cursor));
            }

            return new StreamBatch<TData>(Enumerable.Empty<TData>().ToArray(),
                                          cursor.Position);
        }

        /// <summary>
        /// Removes data from a batch that occurs prior to the specified cursor.
        /// </summary>
        /// <typeparam name="TData">The type of the stream's data.</typeparam>
        /// <param name="batch">The batch.</param>
        /// <param name="cursor">The cursor.</param>
        public static IStreamBatch<TData> Prune<TData>(
            this IStreamBatch<TData> batch,
            ICursor<TData> cursor)
        {
            return Create(batch.Where(x => !cursor.HasReached(x)), cursor);
        }
    }
}