using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with stream query batches.
    /// </summary>
    public static class StreamQueryBatch
    {
        /// <summary>
        /// Creates a stream query batch from an enumerable sequence.
        /// </summary>
        /// <typeparam name="TData">The type of the data in the batch.</typeparam>
        /// <param name="source">The source data.</param>
        /// <param name="cursor">The cursor that marks the location of the beginning of the batch within the source stream.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">source</exception>
        public static IStreamQueryBatch<TData> Create<TData>(
            IEnumerable<TData> source,
            ICursor cursor)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (cursor == null)
            {
                throw new ArgumentNullException("cursor");
            }

            var results = source.ToArray();

            return new StreamQueryBatch<TData>(results, cursor.Position);
        }

        /// <summary>
        /// Represents an empty stream query batch.
        /// </summary>
        /// <typeparam name="TData">The type of the data in the source stream.</typeparam>
        /// <param name="cursor">The cursor that marks the location of the beginning of the batch within the source stream.</param>
        /// <exception cref="ArgumentNullException">cursor</exception>
        public static IStreamQueryBatch<TData> Empty<TData>(ICursor cursor)
        {
            if (cursor == null)
            {
                throw new ArgumentNullException("cursor");
            }

            return new StreamQueryBatch<TData>(Enumerable.Empty<TData>().ToArray(),
                                               cursor.Position);
        }

        /// <summary>
        /// Removes data from a batch that occurs prior to the specified cursor.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <param name="batch">The batch.</param>
        /// <param name="cursor">The cursor.</param>
        public static IStreamQueryBatch<TData> Prune<TData>(
            this IStreamQueryBatch<TData> batch,
            ICursor cursor)
        {
            return Create(batch.Where(x => !cursor.HasReached(x)), cursor);
        }
    }
}