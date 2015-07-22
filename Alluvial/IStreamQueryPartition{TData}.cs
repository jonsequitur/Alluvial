namespace Alluvial
{
    /// <summary>
    /// A set of boundaries used to partition a query across multiple independent processes.
    /// </summary>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    public interface IStreamQueryPartition<out TPartition>
    {
        /// <summary>
        /// Gets the lower, exclusive, bound.
        /// </summary>
        TPartition LowerBoundExclusive { get; }

        /// <summary>
        /// Gets the upper, inclusive, bound.
        /// </summary>
        TPartition UpperBoundInclusive { get; }
    }
}