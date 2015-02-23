using System;
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

            var updatedStreams = streamSource.EventsByAggregate()
                                             .Trace()
                                             .Map(ss => ss.Select(s => s
                                                                      .Trace()
                                                                      .Cursor(
                                                                          advanceCursor: (q, b) =>
                                                                          {
                                                                              var last = b.LastOrDefault();
                                                                              if (last != null)
                                                                              {
                                                                                  q.Cursor.AdvanceTo(last.CheckpointToken);
                                                                              }
                                                                          },
                                                                          newCursor: () => Cursor.Create(""))));

            var indexCatchup = StreamCatchup.Distribute(updatedStreams, batchCount: 1);
            var index = new Projection<List<AccountOpened>, string>
            {
                Value = new List<AccountOpened>()
            };

            // FIX: (A_stream_can_be_derived_from_an_index_projection_in_order_to_perform_a_reduce_operation) the index needs to refer to the upstream cursor but is failing when the stream-specific cursor is used

            // subscribe a catchup to the updates stream to build up an index
            indexCatchup.Subscribe<Projection<List<AccountOpened>, string>, IDomainEvent>(
                async (p, events) =>
                {
                    p.Value.AddRange(events.OfType<AccountOpened>()
                                           .Where(e => e.AccountType == BankAccountType.Savings));
                    p.AdvanceCursorTo(events.Last().CheckpointToken);
                    return p;
                },
                async (streamId, aggregate) => await aggregate(index));

            // create a catchup over the index
            var savingsAccounts = Stream.Create(
                "Savings accounts",
                async q => index.Value.Skip(q.Cursor.As<int>())
                                .Take(q.BatchCount ?? 1000));
            var savingsAccountsCatchup = StreamCatchup.Create(savingsAccounts);

            var numberOfSavingsAccounts = new Projection<int, int>();
            savingsAccountsCatchup.Subscribe<Projection<int, int>, IDomainEvent>(
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
    }
}