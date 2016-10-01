using System;

namespace Alluvial.Fluent
{
    public static class StreamBuilderExtensions
    {
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

        public static StreamBuilder<TData, TCursor> Cursor<TData, TCursor>(
            this StreamBuilder<TData> source,
            Func<CursorBuilder, CursorBuilder<TCursor>> build) =>
                new StreamBuilder<TData, TCursor>(source, build(new CursorBuilder()));

        public static TStreamBuilder Named<TStreamBuilder>(
            this TStreamBuilder streamBuilder,
            string id)
            where TStreamBuilder : StreamBuilder
        {
            streamBuilder.StreamId = id;
            return streamBuilder;
        }

        public static StreamBuilder<TData, TCursor, TPartition> Partition<TData, TCursor, TPartition>(
            this StreamBuilder<TData, TCursor> source,
            Func<PartitionBuilder, PartitionBuilder<TPartition>> build) =>
                new StreamBuilder<TData, TCursor, TPartition>(
                    source,
                    source.CursorBuilder,
                    build(new PartitionBuilder()));
    }
}