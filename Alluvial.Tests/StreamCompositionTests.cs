using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FluentAssertions;
using Its.Log.Instrumentation;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamCompositionTests
    {
        private IStoreEvents store;
        private NEventStoreStreamSource streamSource;

        [SetUp]
        public void SetUp()
        {
            // populate the event store
            store = TestEventStore.Create();

            streamSource = new NEventStoreStreamSource(store);
        }

        [Test]
        public async Task A_stream_can_be_derived_from_an_index_projection_in_order_to_perform_a_reduce_operation()
        {
            store.WriteEvents(i => new AccountOpened
            {
                AccountType = i%2 == 0
                    ? BankAccountType.Checking
                    : BankAccountType.Savings
            }, howMany: 100);

            var eventsByAggregate = streamSource.StreamPerAggregate()
                                                .Trace()
                                                .Map(ss => ss.Select(s => s.Trace()));

            var indexCatchup = StreamCatchup.All(eventsByAggregate, batchSize: 1);
            var index = new Projection<ConcurrentBag<AccountOpened>, string>
            {
                Value = new ConcurrentBag<AccountOpened>()
            };

            // subscribe a catchup to the updates stream to build up an index
            indexCatchup.Subscribe(
                Aggregator.Create<Projection<ConcurrentBag<AccountOpened>>, IDomainEvent>((p, events) =>
                {
                    foreach (var e in events.OfType<AccountOpened>()
                                            .Where(e => e.AccountType == BankAccountType.Savings))
                    {
                        p.Value.Add(e);
                    }

                    return p;
                }).Trace(),
                async (streamId, aggregate) => await aggregate(index));

            // create a catchup over the index
            var savingsAccounts = Stream.Create<IDomainEvent, string>(
                id: "Savings accounts",
                query: q => index.Value
                                 .SkipWhile(v => q.Cursor.HasReached(v.CheckpointToken))
                                 .Take(q.BatchSize ?? 1000)                );

            var savingsAccountsCatchup = StreamCatchup.Create(savingsAccounts);

            var numberOfSavingsAccounts = new Projection<int, int>();
            savingsAccountsCatchup.Subscribe<Projection<int, int>, IDomainEvent>(
                manage: async (streamId, aggregate) =>
                {
                    numberOfSavingsAccounts = await aggregate(numberOfSavingsAccounts);
                },
                aggregate: (c, es) =>
                {
                    c.Value += es.Count;
                    return c;
                });

            using (indexCatchup.Poll(TimeSpan.FromMilliseconds(100)))
            using (savingsAccountsCatchup.Poll(TimeSpan.FromMilliseconds(50)))
            {
                await Wait.Until(() => numberOfSavingsAccounts.Value >= 50);
            }
        }

        [Test]
        public async Task One_stream_can_transparently_delegate_to_another()
        {
            var upstream = NEventStoreStream.AllEvents(store);
            store.WriteEvents(i => new AccountOpened(), 100);
            var projection = new Projection<int, string>();

            var dependentStream = Stream.Create<int, string>(
                async q =>
                {
                    var mapped = upstream.Map(e => new[] { e.Count() });
                    var batch = await mapped.Fetch(q);
                    return batch;
                });

            var catchup = StreamCatchup.Create(dependentStream, batchSize: 50);

            FetchAndSave<Projection<int, string>> manage = async (id, aggregate) =>
            {
                await aggregate(projection);
            };
            catchup.Subscribe((p, b) =>
            {
                p.Value += b.Sum();
                return p;
            }, manage);

            await catchup.RunSingleBatch();

            Console.WriteLine(projection.ToLogString());

            projection.Value.Should().Be(50);
            projection.CursorPosition.Should().Be("50");
        }

        [Test]
        public async Task One_stream_can_use_anothers_cursor_to_limit_how_far_ahead_it_looks()
        {
            var aggregateId = Guid.NewGuid().ToString();

            var upstream = NEventStoreStream.AggregateIds(store);

            store.WriteEvents(i => new AccountOpened
            {
                AggregateId = aggregateId
            }, 50);

            var streamPerAggregate = upstream.IntoManyAsync(
                async (streamId, fromCursor, toCursor) =>
                {
                    var stream = NEventStoreStream.AllEvents(store);

                    var cursor = Cursor.New(fromCursor);
                    var events = await stream.CreateQuery(cursor, int.Parse(toCursor)).NextBatch();

                    return events.Select(e => e.Body)
                                 .Cast<IDomainEvent>()
                                 .GroupBy(e => e.AggregateId)
                                 .Select(@group => @group.AsStream(cursorPosition: i => i.CheckpointToken));
                });

            var streams = await streamPerAggregate.CreateQuery(batchSize: 25).NextBatch();

            foreach (var stream in streams.SelectMany(s => s))
            {
                var batch = await stream.CreateQuery().NextBatch();
                batch.Count.Should().Be(25);
            }
        }
    }
}