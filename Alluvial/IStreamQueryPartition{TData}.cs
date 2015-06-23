namespace Alluvial
{
    public interface IStreamQueryPartition<out TPartition>
    {
        TPartition LowerBoundInclusive { get; }

        TPartition UpperBoundExclusive { get; }
    }
}