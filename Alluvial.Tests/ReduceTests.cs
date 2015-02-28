using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class ReduceTests
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

            IStream<IStream<IDomainEvent, int>, string> eventsByAggregate = streamSource.EventsByAggregate()
                                                                                     .Trace()
                                                                                     .Map(ss => ss.Select(s => s.Trace()));

            var indexCatchup = StreamCatchup.Distribute(eventsByAggregate, batchCount: 1);
            var index = new Projection<ConcurrentBag<AccountOpened>, string>
            {
                Value = new ConcurrentBag<AccountOpened>()
            };

            // subscribe a catchup to the updates stream to build up an index
            indexCatchup.Subscribe(
                Aggregator.Create<Projection<ConcurrentBag<AccountOpened>>, IDomainEvent>(async (p, events) =>
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
                "Savings accounts",
                async q => index.Value.SkipWhile(v => q.Cursor.HasReached(v.CheckpointToken))
                                .Take(q.BatchCount ?? 1000));
            var savingsAccountsCatchup = StreamCatchup.Create(savingsAccounts);

            var numberOfSavingsAccounts = new Projection<int, int>();
            savingsAccountsCatchup.Subscribe<Projection<int, int>, IDomainEvent, string>(
                manageProjection: async (streamId, aggregate) =>
                {
                    numberOfSavingsAccounts = await aggregate(numberOfSavingsAccounts);
                },
                aggregate: async (c, es) =>
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

        [Ignore("Test not finished")]
        [Test]
        public async Task When_a_catchup_tracks_another_projection_then_it_does_not_get_ahead_of_it_relative_to_the_source_stream()
        {






            


            // FIX (When_a_catchup_tracks_another_projection_then_it_does_not_get_ahead_of_it_relative_to_the_source_stream) write test
            Assert.Fail("Test not written yet.");
        }
    }
}