using System;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using Alluvial.Tests.StreamImplementations.NEventStore;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
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
        public async Task StreamCatchupError_ToString_contains_exception_message()
        {
            var error = new StreamCatchupError(new Exception("drat!"));

            error.ToString().Should().Contain("drat!");
        }

        [Test]
        public async Task StreamCatchupError_indicates_whether_the_catchup_will_continue()
        {
            var error = new StreamCatchupError(new Exception("drat!"));

            error.ToString().Should().Contain("won't continue");

            error.Continue();

            error.ToString().Should().Contain("will continue");
        }

        [Test]
        public async Task When_an_aggregation_fails_then_the_projection_is_not_updated()
        {
            var projections = new InMemoryProjectionStore<BalanceProjection>();
            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate().Trace());

            // subscribe a flaky projector
            catchup.Subscribe(new BalanceProjector()
                                  .Pipeline(async (projection, batch, next) =>
                                  {
                                      bool shouldThrow = true; // declared outside to nudge the compiler into inferring the correct .Pipeline overload
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
            var stream = streamSource.StreamPerAggregate().Trace();
            var catchup = StreamCatchup.All(stream);
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
                                      var aggregateId = batch.Select(i => i.AggregateId).First();
                                      if (streamIdsWithErrors.Contains(aggregateId))
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
                                                     throw new Exception("oops!");
                                                 });

            var error = default(StreamCatchupError<Projection<int, int>>);

            var catchup = StreamCatchup.Create(stream);
            catchup.Subscribe<Projection<int, int>, int>(async (sum, batch) =>
            {
                sum.Value += batch.Count;
                return sum;
            },
                                                              (streamId, use) => use(new Projection<int, int>()),
                                                              onError: e =>
                                                              {
                                                                  error = e;
                                                                  e.Continue();
                                                              });

            await catchup.RunSingleBatch();

            error.Should().NotBeNull();
            error.Exception.Message.Should().Contain("oops");
        }

        [Test]
        public async Task When_advancing_the_cursor_in_a_multi_stream_catchup_throws_then_the_exception_is_surfaced_to_OnError()
        {
            var streams = Enumerable.Range(1, 24)
                                    .AsStream()
                                    .IntoMany((i, from, to) => Stream.Create<string, int>(
                                        query: q => Enumerable.Range(from, to).Select(ii => ii.ToString()),
                                        advanceCursor: (q, b) => { throw new Exception("oops!"); }));

            var error = default(StreamCatchupError<Projection<string, int>>);

            var catchup = StreamCatchup.All(streams);

            catchup.Subscribe<Projection<string, int>, string>((sum, batch) => new Projection<string, int>(),
                                                                    (streamId, use) => use(null),
                                                                    onError: e =>
                                                                    {
                                                                        error = e;
                                                                        e.Continue();
                                                                    });

            await catchup.RunSingleBatch();

            error.Should().NotBeNull();
            error.Exception.Message.Should().Contain("oops");
        }
    }
}