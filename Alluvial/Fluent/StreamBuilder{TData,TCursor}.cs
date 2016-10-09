using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Alluvial.Fluent
{
    /// <summary>
    /// Defines the structure and behavior of a stream.
    /// </summary>
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

        /// <summary>
        /// Creates a stream instance.
        /// </summary>
        /// <param name="query">The query used to pull data from the stream.</param>
        public IStream<TData, TCursor> Create(
                Func<IStreamQuery<TCursor>, Task<IEnumerable<TData>>> query) =>
            Stream.Create(query: query,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor,
                id: StreamId);

        /// <summary>
        /// Creates a stream instance.
        /// </summary>
        /// <param name="query">The query used to pull data from the stream.</param>
        public IStream<TData, TCursor> Create(
                Func<IStreamQuery<TCursor>, IEnumerable<TData>> query) =>
            Stream.Create(
                query: query,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor,
                id: StreamId);
    }
}