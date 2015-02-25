using System;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [Ignore("Still a work in progress")]
    [TestFixture]
    public class StreamCatchupErrorTests
    {
        private IStoreEvents store;
        private NEventStoreStreamSource streamSource;
        private string[] streamIds;

        [SetUp]
        public void SetUp()
        {
            // populate the event store
            store = TestEventStore.Create();

            streamIds = Enumerable.Range(1, 100)
                                  .Select(_ => Guid.NewGuid().ToString())
                                  .ToArray();

            foreach (var streamId in streamIds)
            {
                store.WriteEvents(streamId);
            }

            streamSource = new NEventStoreStreamSource(store);
        }

        [Test]
        public async Task When_an_aggregation_fails_then_the_projection_is_not_updated()
        {
            var projections = new InMemoryProjectionStore<BalanceProjection>();
            var catchup = StreamCatchup.Distribute(streamSource.EventsByAggregate().Trace());

            // subscribe a flaky projector
            catchup.Subscribe(new BalanceProjector()
                                  .Pipeline(async (projection, batch, next) =>
                                  {
                                      bool shouldThrow = true; // declared outside to fool the compiler into inferring the correct .Pipeline overload
                                      if (shouldThrow)
                                      {
                                          throw new Exception("oops");
                                      }
                                      await next(projection, batch);
                                  }),
                              projections.AsHandler(),
                              e => e.Continue());

            await catchup.RunSingleBatch();

            projections.Count().Should().Be(0);
        }

        [Test]
        public async Task When_one_projection_fails_its_cursor_is_not_advanced_while_other_projections_cursors_are_advanced()
        {
            var projections = new InMemoryProjectionStore<BalanceProjection>();

            // first catch up all the projections
            IStream<IStream<IDomainEvent, int>, string> stream = streamSource.EventsByAggregate().Trace();
            var catchup = StreamCatchup.Distribute(stream);
            var initialSubscription = catchup.Subscribe(new BalanceProjector(), projections);
            await catchup.RunUntilCaughtUp();
            initialSubscription.Dispose();

            // write some additional events
            var streamIdsWithoutErrors = streamIds.Take(5).ToList();
            var streamIdsWithErrors = streamIds.Skip(5).Take(5).ToList();
            foreach (var streamId in streamIdsWithoutErrors.Concat(streamIdsWithErrors))
            {
                store.WriteEvents(streamId, howMany: 10);
            }

            // subscribe a flaky projector
            catchup.Subscribe(new BalanceProjector()
                                  .Pipeline(async (projection, batch, next) =>
                                  {
                                      if (streamIdsWithErrors.Contains(projection.AggregateId))
                                      {
                                          throw new Exception("oops");
                                      }

                                      await next(projection, batch);
                                  }),
                              projections.AsHandler(),
                              e => e.Continue());

            await catchup.RunSingleBatch();

            var projectionsWithoutErrors = streamIdsWithoutErrors.Select(
                id => projections.Get(id).Result);
            var projectionsWithErrors = streamIdsWithErrors.Select(
                id => projections.Get(id).Result);

            foreach (var projection in projectionsWithoutErrors)
            {
                projection.CursorPosition.Should().Be(11);
                projection.Balance.Should().Be(11);
            }

            foreach (var projection in projectionsWithErrors)
            {
                projection.CursorPosition.Should().Be(1);
                projection.Balance.Should().Be(1);
            }
        }

        [Test]
        public async Task When_advancing_the_cursor_in_a_single_stream_catchup_throws_then_the_exception_is_surfaced_to_OnError()
        {
            var stream = Stream.Create<int, int>(q => Enumerable.Range(1, 100).Skip(q.Cursor.Position),
                                                 advanceCursor: (q, b) =>
                                                 {
                                                     throw new Exception("oops");
                                                 });

            StreamCatchupError<Projection<int, int>> error = null;

            var catchup = StreamCatchup.Create(stream);
            catchup.Subscribe<Projection<int, int>, int, int>(async (sum, batch) =>
            {
                sum.Value += batch.Count;
                return sum;
            },
                                                              (streamId, use) => use(new Projection<int, int>()),
                                                              onError: e => error = e);

            await catchup.RunSingleBatch();

            error.Should().BeNull();
            error.Exception.Message.Should().Contain("oops");
        }

        [Test]
        public async Task When_advancing_the_cursor_in_a_multi_stream_catchup_throws_then_the_exception_is_surfaced_to_OnError()
        {
            var stream = Stream.Create<int>(q => Enumerable.Range(1, 24).Skip(q.Cursor.Position))
                               .Requery<int, string, string, int>(i => Stream.Create<string, string>(async q => Enumerable.Range(1, 10)
                                                                                                                          .Select(ii => ii.ToString())));

            StreamCatchupError<Projection<string, int>> error = null;

            var catchup = StreamCatchup.Distribute(stream);

            catchup.Subscribe<Projection<string, int>, string, int>(async (sum, batch) => new Projection<string, int>(),
                                                                    (streamId, use) => use(null),
                                                                    onError: e => error = e);

            await catchup.RunSingleBatch();

            error.Should().BeNull();
            error.Exception.Message.Should().Contain("oops");
        }
    }
}