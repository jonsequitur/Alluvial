using System;

namespace Alluvial
{
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
        public PartitionBuilder<TPartition> By<TPartition>()
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
        internal StreamBuilder()
        {
        }

        internal Action<IStreamQuery<TCursor>, IStreamBatch<TData>> AdvanceCursor { get; set; }

        public IStream<TData, TCursor> CreateStream()
        {
            return null;
        }
    }

    public class StreamBuilder<TData, TCursor, TPartition> :
        StreamBuilder<TData, TCursor>
    {
        private readonly StreamBuilder<TData, TCursor> streamBuilder;
        private readonly Func<PartitionBuilder, PartitionBuilder<TPartition>> partitionBuilder;

        internal StreamBuilder(
            StreamBuilder<TData, TCursor> streamBuilder,
            PartitionBuilder<TPartition> partitionBuilder)
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

        public StreamBuilder(StreamBuilder<TData, TCursor> streamBuilder, Func<PartitionBuilder, PartitionBuilder<TPartition>> partitionBuilder)
        {
            this.streamBuilder = streamBuilder;
            this.partitionBuilder = partitionBuilder;
        }

        public IPartitionedStream<TData, TCursor, TPartition> CreateStream()
        {
            return null;
        }
    }

    public static class StreamBuilderExtensions
    {
        public static StreamBuilder<TData, TCursor> Cursor<TData, TCursor>(
            this StreamBuilder<TData> source,
            Func<CursorBuilder, CursorBuilder<TCursor>> build)
        {
            return new StreamBuilder<TData, TCursor>();
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
            return new StreamBuilder<TData, TCursor, TPartition>(source, build);
        }
    }
}

//
//    public static class StreamOf<TData>
//    {
//        public static IStream<TData, TCursor> CursorBy<TCursor>(
//            Func<IStreamQuery<TCursor>, Task<IEnumerable<TData>>> query,
//            string id = null,
//            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
//            Func<ICursor<TCursor>> newCursor = null)
//        {
//            return Stream.Create(
//                id: id,
//                query: query,
//                advanceCursor: advanceCursor,
//                newCursor: newCursor);
//        }
//
//        public static IStream<TData, TCursor> CursorBy<TCursor>(
//            Func<IStreamQuery<TCursor>, IEnumerable<TData>> query,
//            string id = null,
//            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
//            Func<ICursor<TCursor>> newCursor = null)
//        {
//            return Stream.Create(
//                id: id,
//                query: query,
//                advanceCursor: advanceCursor,
//                newCursor: newCursor);
//        }
//
//        public static class Cursor<TCursor>
//        {
//            public static class Partition<TPartition>
//            {
//                public static IPartitionedStream<TData, TCursor, TPartition> ByRange(
//                    Func<IStreamQuery<TCursor>, IStreamQueryRangePartition<TPartition>, Task<IEnumerable<TData>>> query,
//                    string id = null,
//                    Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
//                    Func<ICursor<TCursor>> newCursor = null)
//                {
//                    return Stream.PartitionedByRange(
//                        id: id,
//                        query: query,
//                        advanceCursor: advanceCursor,
//                        newCursor: newCursor);
//                }
//
//                public static IPartitionedStream<TData, TCursor, TPartition> ByValue(
//                    Func<IStreamQuery<TCursor>, IStreamQueryValuePartition<TPartition>, Task<IEnumerable<TData>>> query,
//                    string id = null,
//                    Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advanceCursor = null,
//                    Func<ICursor<TCursor>> newCursor = null)
//                {
//                    return Stream.PartitionedByValue(
//                        id: id,
//                        query: query,
//                        advanceCursor: advanceCursor,
//                        newCursor: newCursor);
//                }
//            }
//        }
//    }
//}