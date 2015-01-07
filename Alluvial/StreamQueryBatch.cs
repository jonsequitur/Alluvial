using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial
{
    public static class StreamQueryBatch
    {
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

        public static IStreamQueryBatch<TData> Empty<TData>(ICursor cursor)
        {
            if (cursor == null)
            {
                throw new ArgumentNullException("cursor");
            }

            return new StreamQueryBatch<TData>(Enumerable.Empty<TData>().ToArray(),
                                               cursor.Position);
        }

        public static IStreamQueryBatch<TData> Prune<TData>(
            this IStreamQueryBatch<TData> batch,
            ICursor cursor)
        {
            return Create(batch.Where(x => !cursor.HasReached(x)), cursor);
        }
    }
}