using System;

namespace Alluvial
{
    public static class DataStreamSource
    {
        public static IDataStreamSource<TKey, TData> Create<TKey, TData>(Func<TKey, IDataStream<TData>> open)
        {
            return new AnonymousDataStreamSource<TKey, TData>(open);
        }
    }
}