namespace Alluvial.Tests.BankDomain
{
    public class ProjectionBase : Projection<IDomainEvent, int>
    {
        public string AggregateId { get; set; }
    }
}