using System;

namespace Alluvial
{
    internal class AnonymousStreamSource<TKey, TData, TCursor> : IStreamSource<TKey, TData, TCursor>
    {
        private readonly Func<TKey, IStream<TData, TCursor>> open;

        public AnonymousStreamSource(Func<TKey, IStream<TData, TCursor>> open)
        {
            if (open == null)
            {
                throw new ArgumentNullException(nameof(open));
            }
            this.open = open;
        }

        public IStream<TData, TCursor> Open(TKey key)
        {
            return open(key);
        }
    }
}