using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Alluvial
{
    public static partial class Stream
    {
        public static StreamBuilder<TData> Of<TData>(
            string streamId = null)
        {
            return new StreamBuilder<TData>(streamId);
        }
    }

    public static class StreamBuilderExtensions
    {
        public static StreamBuilder<TData, TCursor> Cursor<TData, TCursor>(
            this StreamBuilder<TData> source,
            Func<CursorBuilder, CursorBuilder<TCursor>> build)
        {
            return new StreamBuilder<TData, TCursor>(build(new CursorBuilder()));
        }

        public static StreamBuilder<TData, TCursor> Advance<TData, TCursor>(
            this StreamBuilder<TData, TCursor> builder,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advance)

        {
            builder.AdvanceCursor = advance;
            return builder;
        }

        public static StreamBuilder<TData, TCursor, TPartition> Advance<TData, TCursor, TPartition>(
            this StreamBuilder<TData, TCursor, TPartition> builder,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advance)
        {
            builder.AdvanceCursor = advance;
            return builder;
        }

        public static StreamBuilder<TData, TCursor, TPartition> Partition<TData, TCursor, TPartition>(
            this StreamBuilder<TData, TCursor> source,
            Func<PartitionBuilder, PartitionBuilder<TPartition>> build)
        {
            return new StreamBuilder<TData, TCursor, TPartition>(
                source,
                source.CursorBuilder,
                build);
        }
    }

    public class CursorBuilder
    {
        public CursorBuilder<TCursor> By<TCursor>()
        {
            return new CursorBuilder<TCursor>
            {
                NewCursor = () => Cursor.New<TCursor>()
            };
        }

        public CursorBuilder<TCursor> StartsAt<TCursor>(Func<ICursor<TCursor>> @new)
        {
            return new CursorBuilder<TCursor>
            {
                NewCursor = @new
            };
        }
    }

    public class CursorBuilder<TCursor>
    {
        internal Func<ICursor<TCursor>> NewCursor { get; set; }
    }

    public class PartitionBuilder
    {
        public PartitionBuilder<TPartition> ByRange<TPartition>()
        {
            return new PartitionBuilder<TPartition>();
        }

        public PartitionBuilder<TPartition> ByValue<TPartition>()
        {
            return new PartitionBuilder<TPartition>();
        }
    }

    public class PartitionBuilder<TPartition>
    {
    }

    public class StreamBuilder
    {
        internal string StreamId { get; set; }
    }

    public class StreamBuilder<TData> : StreamBuilder
    {
        public StreamBuilder(string streamId = null)
        {
            StreamId = streamId;
        }

        public IStream<TData, TData> CreateStream()
        {
            return null;
        }
    }

    public class StreamBuilder<TData, TCursor> : StreamBuilder
    {
        internal StreamBuilder(CursorBuilder<TCursor> cursorBuilder)
        {
            if (cursorBuilder == null)
            {
                throw new ArgumentNullException(nameof(cursorBuilder));
            }
            CursorBuilder = cursorBuilder;
        }

        internal Action<IStreamQuery<TCursor>, IStreamBatch<TData>> AdvanceCursor { get; set; }

        internal CursorBuilder<TCursor> CursorBuilder { get; }

        public IStream<TData, TCursor> CreateStream(
            Func<IStreamQuery<TCursor>, Task<IEnumerable<TData>>> query)
        {
            return Stream.Create(query: query,
                                 advanceCursor: AdvanceCursor,
                                 newCursor: CursorBuilder.NewCursor,
                                 id: StreamId);
        }

        public IStream<TData, TCursor> CreateStream(
            Func<IStreamQuery<TCursor>, IEnumerable<TData>> query)
        {
            return Stream.Create(
                query: query,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor,
                id: StreamId);
        }
    }

    public class StreamBuilder<TData, TCursor, TPartition> :
        StreamBuilder<TData, TCursor>
    {
        private readonly StreamBuilder<TData, TCursor> streamBuilder;
        private readonly Func<PartitionBuilder, PartitionBuilder<TPartition>> partitionBuilder;

        internal StreamBuilder(
            StreamBuilder<TData, TCursor> streamBuilder,
            CursorBuilder<TCursor> cursorBuilder,
            PartitionBuilder<TPartition> partitionBuilder) : base(cursorBuilder)
        {
            if (streamBuilder == null)
            {
                throw new ArgumentNullException(nameof(streamBuilder));
            }
            if (partitionBuilder == null)
            {
                throw new ArgumentNullException(nameof(partitionBuilder));
            }
        }

        public StreamBuilder(
            StreamBuilder<TData, TCursor> streamBuilder,
            CursorBuilder<TCursor> cursorBuilder,
            Func<PartitionBuilder, PartitionBuilder<TPartition>> partitionBuilder)
            : base(cursorBuilder)
        {
            this.streamBuilder = streamBuilder;
            this.partitionBuilder = partitionBuilder;
        }

        public IPartitionedStream<TData, TCursor, TPartition> CreateStream(Func<IStreamQuery<TCursor>, IStreamQueryRangePartition<TPartition>, Task<IEnumerable<TData>>> query)
        {
            return Stream.PartitionedByRange(
                query: query,
                id: StreamId,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor);
        }

        public IPartitionedStream<TData, TCursor, TPartition> CreateStream(Func<IStreamQuery<TCursor>, IStreamQueryRangePartition<TPartition>, IEnumerable<TData>> query)
        {
            return Stream.PartitionedByRange<TData, TCursor, TPartition>(
                query: (q, p) => query(q, p).CompletedTask(),
                id: StreamId,
                advanceCursor: AdvanceCursor,
                newCursor: CursorBuilder.NewCursor);
        }
    }
}