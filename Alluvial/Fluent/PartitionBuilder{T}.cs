namespace Alluvial.Fluent
{
    /// <summary>
    /// Defines how a stream is partitioned.
    /// </summary>
    public class PartitionBuilder<TPartition>
    {
        internal PartitionBuilder(bool partitionByRange)
        {
            PartitionByRange = partitionByRange;
        }

        internal bool PartitionByRange { get; }
    }
}