using System;

namespace Alluvial
{
    public static class StreamSource
    {
        public static IStreamSource<TKey, TData, TCursorPosition> Create<TKey, TData, TCursorPosition>(Func<TKey, IStream<TData, TCursorPosition>> open)
        {
            return new AnonymousStreamSource<TKey, TData, TCursorPosition>(open);
        }
    }
}