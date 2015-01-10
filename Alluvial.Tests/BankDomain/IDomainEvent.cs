namespace Alluvial.Tests.BankDomain
{
    public interface IDomainEvent
    {
        string AggregateId { get; }
        int StreamRevision { get; set; }
    }
}