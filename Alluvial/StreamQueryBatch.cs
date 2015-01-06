using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial
{
    public static class StreamQueryBatch
    {
        public static IStreamQueryBatch<TData> Create<TData>(
            IEnumerable<TData> source,
            IStreamQuery<TData> query)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            var results = source.ToArray();

            return new StreamQueryBatch<TData>(results,
                                               query.Cursor.Position);
        }

        public static IStreamQueryBatch<TData> Empty<TData>(
            IStreamQuery<TData> query)
        {
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }
            return new StreamQueryBatch<TData>(Enumerable.Empty<TData>().ToArray(),
                                               query.Cursor.Position);
        }
    }
}