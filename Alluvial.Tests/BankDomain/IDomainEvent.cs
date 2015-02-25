namespace Alluvial.Tests.BankDomain
{
    public interface IDomainEvent
    {
        string AggregateId { get; set; }
        int StreamRevision { get; set; }
        string CheckpointToken { get; set; }
    }
}