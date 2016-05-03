namespace Alluvial
{
    /// <summary>
    /// A stream query partition represeted by a specific value.
    /// </summary>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    public interface IStreamQueryValuePartition<TPartition> : IStreamQueryPartition<TPartition>
    {
        /// <summary>
        /// Gets the value of the partition.
        /// </summary>
        TPartition Value { get; }
    }
}