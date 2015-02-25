namespace Alluvial.Tests.BankDomain
{
    public class ProjectionBase : Projection<IDomainEvent, int>, IMapProjection
    {
        public string AggregateId { get; set; }
    }
}