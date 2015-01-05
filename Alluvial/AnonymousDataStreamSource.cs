using System;

namespace Alluvial
{
    internal class AnonymousDataStreamSource<TKey, TData> : IDataStreamSource<TKey, TData>
    {
        private readonly Func<TKey, IDataStream<TData>> open;

        public AnonymousDataStreamSource(Func<TKey, IDataStream<TData>> open)
        {
            if (open == null)
            {
                throw new ArgumentNullException("open");
            }
            this.open = open;
        }

        public IDataStream<TData> Open(TKey key)
        {
            return open(key);
        }
    }
}