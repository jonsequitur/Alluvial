namespace Alluvial.Tests.BankDomain
{
    public class AccountClosed : IDomainEvent
    {
        public string AggregateId { get; set; }
        public int StreamRevision { get; set; }
        public string CheckpointToken { get; set; }
    }
}