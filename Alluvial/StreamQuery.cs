using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// Provides methods for working with stream queries.
    /// </summary>
    [DebuggerStepThrough]
    public static class StreamQuery
    {
        /// <summary>
        /// Creates a query over the specified stream.
        /// </summary>
        public static IStreamIterator<TData, TCursor> CreateQuery<TData, TCursor>(
            this IStream<TData, TCursor> stream,
            ICursor<TCursor> cursor = null,
            int? batchCount = null)
        {
            return new StreamQuery<TData, TCursor>(
                stream,
                cursor ?? stream.NewCursor())
            {
                BatchCount = batchCount
            };
        }

        /// <summary>
        /// Creates a stream query partition having the specified boundaries.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition boundaries.</typeparam>
        /// <param name="lowerBound">The lower, exclusive boundary.</param>
        /// <param name="upperBound">The upper, inclusive boundary.</param>
        /// <returns></returns>
        public static IStreamQueryPartition<TPartition> Partition<TPartition>(
            TPartition lowerBound,
            TPartition upperBound)
        {
            return new StreamQueryPartition<TPartition>
            {
                LowerBoundExclusive = lowerBound,
                UpperBoundInclusive = upperBound
            };
        }
    }
}