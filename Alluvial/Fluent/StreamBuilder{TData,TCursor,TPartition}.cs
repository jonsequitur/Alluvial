using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Alluvial.Fluent
{
    public class StreamBuilder<TData, TCursor, TPartition> :
        StreamBuilder<TData, TCursor>
    {
        private readonly PartitionBuilder<TPartition> partitionBuilder;

        internal StreamBuilder(
            StreamBuilder<TData, TCursor> streamBuilder, 
            CursorBuilder<TCursor> cursorBuilder, 
            PartitionBuilder<TPartition> partitionBuilder) : base(streamBuilder, cursorBuilder)
        {
            this.partitionBuilder = partitionBuilder;
            AdvanceCursor = streamBuilder.AdvanceCursor;
        }

        public IPartitionedStream<TData, TCursor, TPartition> Create(Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, Task<IEnumerable<TData>>> query)
        {
            if (partitionBuilder.PartitionByRange)
            {
                return Stream.PartitionedByRange(
                    query: query,
                    id: StreamId,
                    advanceCursor: AdvanceCursor,
                    newCursor: CursorBuilder.NewCursor);
            }

            return Stream.PartitionedByValue(
                query: query,
                id: StreamId,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor);
        }

        public IPartitionedStream<TData, TCursor, TPartition> Create(Func<IStreamQuery<TCursor>, IStreamQueryPartition<TPartition>, IEnumerable<TData>> query)
        {
            if (partitionBuilder.PartitionByRange)
            {
                return Stream.PartitionedByRange<TData, TCursor, TPartition>(
                    query: (q, p) => query(q, p).CompletedTask(),
                    id: StreamId,
                    advanceCursor: AdvanceCursor,
                    newCursor: CursorBuilder.NewCursor);
            }

            return Stream.PartitionedByValue<TData, TCursor, TPartition>(
                query: (q, p) => query(q, p).CompletedTask(),
                id: StreamId,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor);
        }
    }
}