namespace Alluvial
{
    /// <summary>
    /// Used to partition a query across multiple independent processes.
    /// </summary>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    public interface IStreamQueryPartition<in TPartition>
    {
        /// <summary>
        /// Determines whether the partition should contain the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        bool Contains(TPartition value);
    }
}