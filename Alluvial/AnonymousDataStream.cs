using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial
{
    internal class AnonymousDataStream<TData> : IDataStream<TData>
    {
        private readonly Action<IStreamQuery<TData>, IStreamQueryBatch<TData>> advanceCursor;
        private readonly Func<IStreamQuery<TData>, Task<IStreamQueryBatch<TData>>> fetch;
        private string streamId;

        public AnonymousDataStream(
            string id,
            Func<IStreamQuery<TData>, Task<IStreamQueryBatch<TData>>> fetch,
            Action<IStreamQuery<TData>, IStreamQueryBatch<TData>> advanceCursor = null)
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
                                     var incrementalCursor = query.Cursor as IIncrementalCursor;
                                     if (incrementalCursor != null)
                                     {
                                         incrementalCursor.AdvanceBy(batch.Count);
                                     }
                                     else
                                     {
                                         query.Cursor.AdvanceTo(batch.Last());
                                     }
                                 });

            this.fetch = fetch;
            Id = id;
        }

        public string Id { get; private set; }

        public async Task<IStreamQueryBatch<TData>> Fetch(IStreamQuery<TData> query)
        {
            var batch = await fetch(query);

            advanceCursor(query, batch);

            return batch;
        }
    }
}