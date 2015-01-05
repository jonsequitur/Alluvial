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
            var results = source.ToArray();

            return new StreamQueryBatch<TData>(results,
                                               query.Cursor.Position);
        }

        public static IStreamQueryBatch<TData> Empty<TData>(
            IStreamQuery<TData> query)
        {
            return new StreamQueryBatch<TData>(Enumerable.Empty<TData>().ToArray(),
                                               query.Cursor.Position);
        }
    }
}