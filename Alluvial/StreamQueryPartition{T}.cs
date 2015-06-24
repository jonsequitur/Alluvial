using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Partition: {LowerBoundInclusive} to {UpperBoundExclusive}")]
    internal class StreamQueryPartition<TPartition> : IStreamQueryPartition<TPartition>
    {
        public TPartition LowerBoundExclusive { get; set; }
        public TPartition UpperBoundInclusive { get; set; }
    }
}