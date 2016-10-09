namespace Alluvial.Fluent
{
    /// <summary>
    /// Defines how a stream is partitioned.
    /// </summary>
    public class PartitionBuilder
    {
        internal PartitionBuilder()
        {
        }

        /// <summary>
        /// Specifies that the stream is partitioned by ranges.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        public PartitionBuilder<TPartition> ByRange<TPartition>() =>
            new PartitionBuilder<TPartition>(true);

        /// <summary>
        /// Specifies that the stream is partitioned by values.
        /// </summary>
        /// <typeparam name="TPartition">The type of the partition.</typeparam>
        public PartitionBuilder<TPartition> ByValue<TPartition>() =>
            new PartitionBuilder<TPartition>(false);
    }
}