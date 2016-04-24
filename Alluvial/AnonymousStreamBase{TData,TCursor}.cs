using System;
using System.Threading.Tasks;

namespace Alluvial
{
    internal abstract class AnonymousStreamBase<TData, TCursor> : IStream<TData, TCursor>
    {
        private readonly Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor;
        protected Func<ICursor<TCursor>> newCursor;

        protected AnonymousStreamBase(
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor,
            Func<ICursor<TCursor>> newCursor = null)
        {
            this.newCursor = newCursor ?? (() => Cursor.New<TCursor>());

            this.advanceCursor = advanceCursor ??
                                 ((query, batch) => { });
        }

        public abstract string Id { get; }

        public ICursor<TCursor> NewCursor() => newCursor();

        protected void AdvanceCursor(IStreamQuery<TCursor> query, IStreamBatch<TData> batch) =>
            advanceCursor(query, batch);

        public abstract Task<IStreamBatch<TData>> Fetch(IStreamQuery<TCursor> query);

        public override string ToString() => $"stream:{Id}";
    }
}