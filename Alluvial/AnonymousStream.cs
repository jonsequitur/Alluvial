using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousStream<TData> : IStream<TData>
    {
        private readonly Action<IStreamQuery, IStreamBatch<TData>> advanceCursor;
        private readonly Func<IStreamQuery, Task<IStreamBatch<TData>>> fetch;
        private readonly Func<ICursor> newCursor;

        public AnonymousStream(
            string id,
            Func<IStreamQuery, Task<IStreamBatch<TData>>> fetch,
            Action<IStreamQuery, IStreamBatch<TData>> advanceCursor = null,
            Func<ICursor> newCursor = null)
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
                                     var incrementalCursor = query.Cursor as IIncrementableCursor;
                                     if (incrementalCursor != null)
                                     {
                                         incrementalCursor.AdvanceBy(batch.Count);
                                     }
                                     else
                                     {
                                         query.Cursor.AdvanceTo(batch.Last());
                                     }
                                 });

            this.newCursor = newCursor ?? Cursor.New;
            this.fetch = fetch;
            Id = id;
        }

        public string Id { get; private set; }

        public async Task<IStreamBatch<TData>> Fetch(IStreamQuery query)
        {
            var batch = await fetch(query);

            advanceCursor(query, batch);

            return batch;
        }

        public ICursor NewCursor()
        {
            return newCursor();
        }
    }
}