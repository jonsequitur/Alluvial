using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Tests.BankDomain
{
    public class BalanceProjector : IStreamAggregator<BalanceProjection, IDomainEvent>
    {
        public async Task<BalanceProjection> Aggregate(BalanceProjection projection, IStreamBatch<IDomainEvent> events)
        {
            var eventsArray = events.ToArray();
            projection.Balance += eventsArray.OfType<FundsDeposited>().Sum(f => f.Amount);
            projection.Balance -= eventsArray.OfType<FundsWithdrawn>().Sum(f => f.Amount);
            return projection;
        }
    }
}