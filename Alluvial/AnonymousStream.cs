using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousStream<TData, TCursor> : IStream<TData, TCursor>
    {
        private readonly Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor;
        private readonly Func<IStreamQuery<TCursor>, Task<IStreamBatch<TData>>> fetch;
        private readonly IEnumerable<TData> source;
        private readonly Func<ICursor<TCursor>> newCursor;

        public AnonymousStream(
            string id,
            Func<IStreamQuery<TCursor>, Task<IStreamBatch<TData>>> fetch,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor,
            Func<ICursor<TCursor>> newCursor = null,
            IEnumerable<TData> source = null)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }
            if (fetch == null)
            {
                throw new ArgumentNullException("fetch");
            }

            this.advanceCursor = advanceCursor ??
                                 ((query, batch) =>
                                 {
                                 });

            this.newCursor = newCursor ?? (() => Cursor.New<TCursor>());
            this.fetch = fetch;
            this.source = source;
            Id = id;
        }

        public string Id { get; private set; }

        public async Task<IStreamBatch<TData>> Fetch(IStreamQuery<TCursor> query)
        {
            var batch = await fetch(query);

            advanceCursor(query, batch);

            return batch;
        }

        public IEnumerable<TData> Source
        {
            get
            {
                return source;
            }
        }

        public ICursor<TCursor> NewCursor()
        {
            return newCursor();
        }

        public override string ToString()
        {
            return GetType().ReadableName();
        }
    }
}