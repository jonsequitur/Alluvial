using System;

namespace Alluvial.Fluent
{
    /// <summary>
    /// Provides methods for defining streams.
    /// </summary>
    public static class StreamBuilderExtensions
    {
        /// <summary>
        /// Specifies how a stream's cursor will be advanced.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="builder">The stream builder.</param>
        /// <param name="advance">A delegate that is called to advance a cursor when a batch is processed.</param>
        public static StreamBuilder<TData, TCursor> Advance<TData, TCursor>(
            this StreamBuilder<TData, TCursor> builder,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advance)
        {
            if (builder == null)
            {
                throw new ArgumentNullException(nameof(builder));
            }
            if (advance == null)
            {
                throw new ArgumentNullException(nameof(advance));
            }
            builder.AdvanceCursor = advance;
            return builder;
        }

        /// <summary>
        /// Specifies how a stream's cursor will be advanced.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="builder">The stream builder.</param>
        /// <param name="advance">A delegate that is called to advance a cursor when a batch is processed.</param>
        public static StreamBuilder<TData, TCursor, TPartition> Advance<TData, TCursor, TPartition>(
            this StreamBuilder<TData, TCursor, TPartition> builder,
            Action<IStreamQuery<TCursor>, IStreamBatch<TData>> advance)
        {
            if (builder == null)
            {
                throw new ArgumentNullException(nameof(builder));
            }
            if (advance == null)
            {
                throw new ArgumentNullException(nameof(advance));
            }
            builder.AdvanceCursor = advance;
            return builder;
        }

        /// <summary>
        /// Defines cursor behaviors for a stream.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="builder">The stream builder.</param>
        /// <param name="build">A delegate called to configure the cursor.</param>
        public static StreamBuilder<TData, TCursor> Cursor<TData, TCursor>(
                this StreamBuilder<TData> builder,
                Func<CursorBuilder, CursorBuilder<TCursor>> build) =>
            new StreamBuilder<TData, TCursor>(builder, build(new CursorBuilder()));

        /// <summary>
        /// Specifies the name of the stream.
        /// </summary>
        /// <typeparam name="TStreamBuilder">The type of the stream builder.</typeparam>
        /// <param name="streamBuilder">The stream builder.</param>
        /// <param name="id">The stream identifier.</param>
        public static TStreamBuilder Named<TStreamBuilder>(
            this TStreamBuilder streamBuilder,
            string id)
            where TStreamBuilder : StreamBuilder
        {
            streamBuilder.StreamId = id;
            return streamBuilder;
        }

        /// <summary>
        /// Defines how a stream is partitioned.
        /// </summary>
        /// <typeparam name="TData">The type of the data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        /// <param name="builder">The stream builder.</param>
        /// <param name="build">A delegate called to configure the partition behavior.</param>
        public static StreamBuilder<TData, TCursor, TPartition> Partition<TData, TCursor, TPartition>(
                this StreamBuilder<TData, TCursor> builder,
                Func<PartitionBuilder, PartitionBuilder<TPartition>> build) =>
            new StreamBuilder<TData, TCursor, TPartition>(
                builder,
                builder.CursorBuilder,
                build(new PartitionBuilder()));
    }
}