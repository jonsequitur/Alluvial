using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("{ToString()}")]
    internal class StreamQueryRangePartition<TPartition> : IStreamQueryRangePartition<TPartition>
        where TPartition : IComparable<TPartition>
    {
        public TPartition LowerBoundExclusive { get; set; }

        public TPartition UpperBoundInclusive { get; set; }

        public virtual bool Contains(TPartition value) =>
            value.CompareTo(LowerBoundExclusive) > 0 &&
            value.CompareTo(UpperBoundInclusive) <= 0;

        public override string ToString() => $"partition:{LowerBoundExclusive}-{UpperBoundInclusive}";
    }
}