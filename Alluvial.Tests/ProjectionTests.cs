using System;
using System.Collections.Generic;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using Alluvial.Tests.StreamImplementations.NEventStore;
using Microsoft.CSharp.RuntimeBinder;
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

        [Test]
        public async Task When_a_catchup_throws_outside_of_the_aggregate_function_then_an_informative_exception_can_be_observed()
        {
            // arrange
            var projections = new List<Projection<InternalType, int>>();

            var aggregator = Aggregator.Create<Projection<InternalType, int>, int>(async (projection, batch) => { projections.Add(projection); }).Trace();

            var partitionedStream = Stream
                .Partitioned<int, int, int>(
                    query: async (q, p) =>
                    {
                        return Enumerable.Range(1, 1000)
                                         .Where(i => i.IsWithinPartition(p))
                                         .Skip(q.Cursor.Position)
                                         .Take(q.BatchSize.Value);
                    },
                    advanceCursor: (query, batch) =>
                    {
                        // putting the cursor and the partition on the same field is a little weird because a batch of zero doesn't necessarily signify the end of the batch
                        if (batch.Any())
                        {
                            query.Cursor.AdvanceTo(batch.Last());
                        }
                    });

            var distributor = Partition.ByRange(1, 1000)
                                       .Among(1)
                                       .CreateInMemoryDistributor();

            var catchup = partitionedStream.CreateDistributedCatchup(distributor);

            Exception caught = null;

            catchup.Subscribe(aggregator, onError: ex => { caught = ex.Exception; });

            // act
            await catchup.RunSingleBatch();

            // assert
            caught.Should().BeOfType<RuntimeBinderException>();
        }

        internal class InternalType
        {
            
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