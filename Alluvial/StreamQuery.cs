using System;
using System.Diagnostics;

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

        public static IStreamQueryPartition<TPartition> Partition<TPartition>(TPartition lowerBound, TPartition upperBound)
        {
            return new StreamQueryPartition<TPartition>
            {
                LowerBoundInclusive = lowerBound,
                UpperBoundExclusive = upperBound
            };
        }
    }
}