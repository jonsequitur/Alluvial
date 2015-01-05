using System.Collections.Generic;
using System.Linq;

namespace Alluvial.Tests.BankDomain
{
    public class BalanceProjector : IDataStreamAggregator<BalanceProjection, IDomainEvent>
    {
        public BalanceProjection Aggregate(BalanceProjection projection, IEnumerable<IDomainEvent> events)
        {
            var eventsArray = events.ToArray();
            projection.Balance += eventsArray.OfType<FundsDeposited>().Sum(f => f.Amount);
            projection.Balance -= eventsArray.OfType<FundsWithdrawn>().Sum(f => f.Amount);
            return projection;
        }
    }
}