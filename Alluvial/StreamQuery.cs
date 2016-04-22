using System;
using System.Diagnostics;
using System.Linq;

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
            int? batchSize = null)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            return new StreamQuery<TData, TCursor>(
                stream,
                cursor ?? stream.NewCursor())
            {
                BatchSize = batchSize
            };
        }
    }
}