namespace Alluvial.Fluent
{
    public class PartitionBuilder<TPartition>
    {
        internal PartitionBuilder(bool partitionByRange)
        {
            PartitionByRange = partitionByRange;
        }

        internal bool PartitionByRange { get; }
    }
}