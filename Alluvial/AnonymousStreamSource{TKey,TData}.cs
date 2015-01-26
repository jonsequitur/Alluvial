using System;

namespace Alluvial
{
    internal class AnonymousStreamSource<TKey, TData> : IStreamSource<TKey, TData>
    {
        private readonly Func<TKey, IStream<TData>> open;

        public AnonymousStreamSource(Func<TKey, IStream<TData>> open)
        {
            if (open == null)
            {
                throw new ArgumentNullException("open");
            }
            this.open = open;
        }

        public IStream<TData> Open(TKey key)
        {
            return open(key);
        }
    }
}