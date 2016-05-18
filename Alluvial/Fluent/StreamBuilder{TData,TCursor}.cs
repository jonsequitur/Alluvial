using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Alluvial.Fluent
{
    public class StreamBuilder<TData, TCursor> : StreamBuilder<TData>
    {
        internal StreamBuilder(StreamBuilder<TData> streamBuilder, CursorBuilder<TCursor> cursorBuilder)
        {
            if (cursorBuilder == null)
            {
                throw new ArgumentNullException(nameof(cursorBuilder));
            }
            CursorBuilder = cursorBuilder;
            StreamId = streamBuilder.StreamId;
        }

        internal Action<IStreamQuery<TCursor>, IStreamBatch<TData>> AdvanceCursor { get; set; }

        internal CursorBuilder<TCursor> CursorBuilder { get; }

        public IStream<TData, TCursor> Create(
            Func<IStreamQuery<TCursor>, Task<IEnumerable<TData>>> query) =>
                Stream.Create(query: query,
                              advanceCursor: AdvanceCursor,
                              newCursor: CursorBuilder.NewCursor,
                              id: StreamId);

        public IStream<TData, TCursor> Create(
            Func<IStreamQuery<TCursor>, IEnumerable<TData>> query) =>
                Stream.Create(
                    query: query,
                    advanceCursor: AdvanceCursor,
                    newCursor: CursorBuilder.NewCursor,
                    id: StreamId);
    }
}