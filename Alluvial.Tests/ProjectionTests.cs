using System;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class ProjectionTests
    {
        private string streamId;
        private IStoreEvents store;
        private IStream<EventMessage, int> stream;

        [SetUp]
        public void SetUp()
        {
            streamId = Guid.NewGuid().ToString();
            store = TestEventStore.Create();
            store.Populate(streamId);
            stream =  NEventStoreStream.ByAggregate(store, streamId);
        }

        [Test]
        public async Task Projections_can_be_built_from_whole_event_streams_on_demand()
        {
            var projector = AccountBalanceProjector();

            var balanceProjection = await stream.Aggregate(projector);

            balanceProjection.Balance.Should().Be(11.11m);
        }

        [Test]
        public async Task Projections_can_be_updated_from_a_previously_stored_state()
        {
            var projection = new BalanceProjection
            {
                Balance = 100m,
                CursorPosition = 2
            };

            var balanceProjection = await stream.Aggregate(AccountBalanceProjector(),
                                                             projection);

            balanceProjection.Balance
                             .Should()
                             .Be(111m,
                                 "the first two items in the sequence should not have been applied, and the prior projection state should have been used");
        }

        [Test]
        public async Task When_a_stream_has_no_events_after_the_projection_cursor_then_no_data_is_fetched()
        {
            var initialProjection = new BalanceProjection
            {
                Balance = 321m,
                CursorPosition = 5
            };

            var finalProjection = await stream.Aggregate(AccountBalanceProjector(),
                                                         initialProjection);

            finalProjection.ShouldBeEquivalentTo(initialProjection,
                                                 "the projection cursor is past the end of the event stream so no events should be applied");
        }

        private static IStreamAggregator<BalanceProjection, EventMessage> AccountBalanceProjector()
        {
            return Aggregator.Create<BalanceProjection, EventMessage>(
                (projection, events) =>
                {
                    var domainEvents = events.Select(e => e.Body).ToArray();

                    projection.Balance = projection.Balance
                                         - domainEvents
                                             .OfType<FundsWithdrawn>()
                                             .Sum(e => e.Amount)
                                         + domainEvents
                                             .OfType<FundsDeposited>()
                                             .Sum(e => e.Amount);
                })
                             .Pipeline(async (projection, e, next) => await next(projection ?? new BalanceProjection(), e));
        }
    }
}