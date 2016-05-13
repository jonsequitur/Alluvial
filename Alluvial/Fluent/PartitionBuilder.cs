namespace Alluvial.Fluent
{
    public class PartitionBuilder
    {
        internal PartitionBuilder()
        {
        }

        public PartitionBuilder<TPartition> ByRange<TPartition>() =>
            new PartitionBuilder<TPartition>();

        public PartitionBuilder<TPartition> ByValue<TPartition>() =>
            new PartitionBuilder<TPartition>();
    }
}