using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Tests.BankDomain
{
    public class BalanceProjector : IStreamAggregator<BalanceProjection, IDomainEvent>
    {
        public async Task<BalanceProjection> Aggregate(BalanceProjection balanceProjection, IStreamBatch<IDomainEvent> events)
        {
            var eventsArray = events.ToArray();
            balanceProjection.Balance += eventsArray.OfType<FundsDeposited>().Sum(f => f.Amount);
            balanceProjection.Balance -= eventsArray.OfType<FundsWithdrawn>().Sum(f => f.Amount);

            balanceProjection.Value = events
                .Select(e => e.AggregateId)
                .Distinct()
                .SingleOrDefault();

            return balanceProjection;
        }
    }
}