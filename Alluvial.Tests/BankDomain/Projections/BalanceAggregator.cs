using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Tests.BankDomain
{
    public class BalanceAggregator : IStreamAggregator<BalanceProjection, IDomainEvent>
    {
        public async Task<BalanceProjection> Aggregate(
            BalanceProjection projection,
            IStreamBatch<IDomainEvent> events)
        {
            projection.Balance += events.OfType<FundsDeposited>()
                                        .Sum(f => f.Amount);
            projection.Balance -= events.OfType<FundsWithdrawn>()
                                        .Sum(f => f.Amount);

            return projection;
        }
    }
}