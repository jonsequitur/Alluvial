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
        public static IStreamIterator<TData> CreateQuery<TData>(
            this IStream<TData> stream,
            ICursor cursor = null,
            int? batchCount = null)
        {
            return new StreamQuery<TData>(stream,
                                          cursor ?? stream.NewCursor())
            {
                BatchCount = batchCount
            };
        }
    }
}