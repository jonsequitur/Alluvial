namespace Alluvial
{
    public interface IStreamQueryPartition<out TPartition>
    {
        TPartition LowerBoundExclusive { get; }
        TPartition UpperBoundInclusive { get; }
    }
}