namespace Alluvial.Tests.BankDomain
{
    public interface IDomainEvent
    {
        string AggregateId { get; }
    }
}