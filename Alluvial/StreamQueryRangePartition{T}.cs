using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("{ToString()}")]
    internal class StreamQueryRangePartition<TPartition> : IStreamQueryRangePartition<TPartition>
        where TPartition : IComparable<TPartition>
    {
        public StreamQueryRangePartition(TPartition lowerBoundExclusive, TPartition upperBoundInclusive)
        {
            LowerBoundExclusive = lowerBoundExclusive;
            UpperBoundInclusive = upperBoundInclusive;

            if (!IsWithinLowerBound(upperBoundInclusive))
            {
                throw new ArgumentException($"The lower bound ({lowerBoundExclusive}) must be less than the upper bound ({upperBoundInclusive}).");
            }
        }

        public TPartition LowerBoundExclusive { get; }

        public TPartition UpperBoundInclusive { get; }

        public virtual bool Contains(TPartition value) =>
            IsWithinLowerBound(value) &&
            IsWithinUpperBound(value);

        protected virtual bool IsWithinLowerBound(TPartition value) =>
            value.CompareTo(LowerBoundExclusive) > 0;

        protected virtual bool IsWithinUpperBound(TPartition value) =>
            value.CompareTo(UpperBoundInclusive) <= 0;

        public override string ToString() => $"partition:{LowerBoundExclusive}-{UpperBoundInclusive}";
    }
}