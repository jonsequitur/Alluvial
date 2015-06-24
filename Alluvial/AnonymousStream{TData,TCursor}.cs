using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousStream<TData, TCursor> : AnonymousStreamBase<TData, TCursor>
    {
        private readonly Func<IStreamQuery<TCursor>, Task<IStreamBatch<TData>>> fetch;
        private readonly IEnumerable<TData> source;

        public AnonymousStream(
            string id,
            Func<IStreamQuery<TCursor>, Task<IStreamBatch<TData>>> fetch,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor<TCursor>> newCursor = null,
            IEnumerable<TData> source = null) : base(id, advanceCursor: advanceCursor, newCursor: newCursor)
        {
            if (fetch == null)
            {
                throw new ArgumentNullException("fetch");
            }

            this.fetch = fetch;
            this.source = source;
        }

        public override async Task<IStreamBatch<TData>> Fetch(IStreamQuery<TCursor> query)
        {
            var batch = await fetch(query);

            AdvanceCursor(query, batch);

            return batch;
        }

        public IEnumerable<TData> Source
        {
            get
            {
                return source;
            }
        }
    }
}