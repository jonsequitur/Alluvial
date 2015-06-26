using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal abstract class AnonymousStreamBase<TData, TCursor> : IStream<TData, TCursor>
    {
        private readonly Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor;
        protected Func<ICursor<TCursor>> newCursor;

        protected AnonymousStreamBase(string id,
                                      Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor,
                                      Func<ICursor<TCursor>> newCursor = null)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }

            this.newCursor = newCursor ?? (() => Cursor.New<TCursor>());

            this.advanceCursor = advanceCursor ??
                                 ((query, batch) => { });

            Id = id;
        }

        public string Id { get; private set; }

        public ICursor<TCursor> NewCursor()
        {
            return newCursor();
        }

        protected void AdvanceCursor(IStreamQuery<TCursor> query, IStreamBatch<TData> batch)
        {
            advanceCursor(query, batch);
        }

        public abstract Task<IStreamBatch<TData>> Fetch(IStreamQuery<TCursor> query);

        public override string ToString()
        {
            return string.Format("stream:{0} ({1})", Id, GetType().ReadableName());
        }
    }
}