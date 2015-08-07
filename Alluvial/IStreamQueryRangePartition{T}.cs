namespace Alluvial
{
    /// <summary>
    /// A stream query partition having known boundaries.
    /// </summary>
    /// <typeparam name="TPartition">The type of the partition.</typeparam>
    public interface IStreamQueryRangePartition<TPartition> : IStreamQueryPartition<TPartition>
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