using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Alluvial.Fluent
{
    public class StreamBuilder<TData, TCursor, TPartition> :
        StreamBuilder<TData, TCursor>
    {
        internal StreamBuilder(
            StreamBuilder<TData, TCursor> streamBuilder,
            CursorBuilder<TCursor> cursorBuilder) : base(streamBuilder, cursorBuilder)
        {
            AdvanceCursor = streamBuilder.AdvanceCursor;
        }

        public IPartitionedStream<TData, TCursor, TPartition> Create(Func<IStreamQuery<TCursor>, IStreamQueryRangePartition<TPartition>, Task<IEnumerable<TData>>> query) =>
            Stream.PartitionedByRange(
                query: query,
                id: StreamId,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor);

        public IPartitionedStream<TData, TCursor, TPartition> Create(Func<IStreamQuery<TCursor>, IStreamQueryRangePartition<TPartition>, IEnumerable<TData>> query) =>
            Stream.PartitionedByRange<TData, TCursor, TPartition>(
                query: (q, p) => query(q, p).CompletedTask(),
                id: StreamId,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor);
    }
}