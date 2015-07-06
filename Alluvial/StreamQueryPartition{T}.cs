using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("{ToString()}")]
    internal class StreamQueryPartition<TPartition> : IStreamQueryPartition<TPartition>
    {
        public TPartition LowerBoundExclusive { get; set; }
        public TPartition UpperBoundInclusive { get; set; }

        public override string ToString()
        {
            return string.Format("partition:{0}-{1}",
                                 LowerBoundExclusive,
                                 UpperBoundInclusive);
        }
    }
}