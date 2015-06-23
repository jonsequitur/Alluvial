using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Partition: {LowerBoundInclusive} to {UpperBoundExclusive}")]
    internal class StreamQueryPartition<TPartition> : IStreamQueryPartition<TPartition>
    {
        public TPartition LowerBoundInclusive { get; set; }
        public TPartition UpperBoundExclusive { get; set; }
    }
}