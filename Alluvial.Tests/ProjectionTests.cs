using System;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using FluentAssertions;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class ProjectionTests
    {
        private string streamId;
        private IStoreEvents store;
        private NEventStoreDataStream dataStream;

        [SetUp]
        public void SetUp()
        {
            streamId = Guid.NewGuid().ToString();
            store = TestEventStore.Create();
            PopulateEventStream(store, streamId);
            dataStream = new NEventStoreDataStream(store, streamId);
        }

        [Test]
        public async Task Projections_can_be_built_from_whole_event_streams_on_demand()
        {
            var projector = AccountBalanceProjector();

            var balanceProjection = await dataStream.ProjectWith(projector);

            balanceProjection.Balance.Should().Be(11.11m);
        }

        [Test]
        public async Task Projections_can_be_updated_from_a_previously_stored_state()
        {
            var projection = new BalanceProjection
            {
                AggregateId = streamId,
                Balance = 100m,
                CursorPosition = 2
            };

            var balanceProjection = await dataStream.ProjectWith(AccountBalanceProjector(),
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
                AggregateId = streamId,
                Balance = 321m,
                CursorPosition = 5
            };

            var finalProjection = await dataStream.ProjectWith(AccountBalanceProjector(),
                                                               initialProjection);

            finalProjection.ShouldBeEquivalentTo(initialProjection,
                                                 "the projection cursor is past the end of the event stream so no events should be applied");
        }

        [Test]
        public async Task A_data_stream_can_be_mapped_at_query_time()
        {
            var domainEvents = dataStream.Map(es => es.Select(e => e.Body).OfType<IDomainEvent>());

            var query = domainEvents.CreateQuery();

            var batch = await domainEvents.Fetch(query);

            batch.Count()
                 .Should()
                 .Be(4);
        }

        [Test]
        public async Task A_mapped_data_stream_can_be_traversed_using_the_outer_query_cursor()
        {
            using (var stream = store.OpenStream(streamId))
            {
                for (var i = 0; i < 5; i++)
                {
                    stream.Add(new EventMessage
                    {
                        Body = new FundsWithdrawn
                        {
                            AggregateId = streamId,
                            Amount = 1m
                        }
                    });
                }
                stream.CommitChanges(Guid.NewGuid());
            }

            var domainEvents = dataStream.Map(es => es.Select(e => e.Body)
                                                      .OfType<FundsWithdrawn>());

            var query = domainEvents.CreateQuery();

            var batch = await query.NextBatch();

            batch.Count()
                 .Should()
                 .Be(5);
            query.Cursor.As<int>()
                 .Should()
                 .Be(9);
        }

        private static IDataStreamAggregator<BalanceProjection, EventMessage> AccountBalanceProjector()
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
                             .Before((projection, e) => projection ?? new BalanceProjection
                             {
                                 AggregateId = e.Select(m => m.Body)
                                                .OfType<IDomainEvent>()
                                                .First()
                                                .AggregateId
                             });
        }

        private static void PopulateEventStream(IStoreEvents store, string streamId)
        {
            using (var stream = store.OpenStream(streamId, 0))
            {
                stream.Add(new EventMessage
                {
                    Body = new FundsDeposited
                    {
                        AggregateId = streamId,
                        Amount = .01m
                    }
                });
                stream.Add(new EventMessage
                {
                    Body = new FundsDeposited
                    {
                        AggregateId = streamId,
                        Amount = .1m
                    }
                });
                stream.Add(new EventMessage
                {
                    Body = new FundsDeposited
                    {
                        AggregateId = streamId,
                        Amount = 1m
                    }
                });
                stream.Add(new EventMessage
                {
                    Body = new FundsDeposited
                    {
                        AggregateId = streamId,
                        Amount = 10m
                    }
                });
                stream.CommitChanges(Guid.NewGuid());
            }
        }
    }
}