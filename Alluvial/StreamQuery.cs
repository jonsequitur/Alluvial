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
        public static IStreamIterator<TData, TCursorPosition> CreateQuery<TData, TCursorPosition>(
            this IStream<TData, TCursorPosition> stream,
            ICursor<TCursorPosition> cursor = null,
            int? batchCount = null)
        {
            return new StreamQuery<TData, TCursorPosition>(
                stream,
                cursor ?? stream.NewCursor())
            {
                BatchCount = batchCount
            };
        }
    }
}