using System;

namespace Alluvial
{
    public static class StreamQuery
    {
        public static IStreamQuery<TData> CreateQuery<TData>(
            this IDataStream<TData> stream,
            ICursor cursor = null,
            int? batchCount = null)
        {
            return new StreamQuery<TData>(stream,
                                          cursor ?? Cursor.New())
            {
                BatchCount = batchCount
            };
        }
    }
}