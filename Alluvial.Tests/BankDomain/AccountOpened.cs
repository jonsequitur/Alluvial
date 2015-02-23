namespace Alluvial.Tests.BankDomain
{
    public class AccountOpened : IDomainEvent
    {
        public string AggregateId { get; set; }
        public int StreamRevision { get; set; }
        public string CheckpointToken { get; set; }
        public BankAccountType AccountType { get; set; }
    }
}