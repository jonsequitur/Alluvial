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

            var updatedStreams = streamSource.UpdatedStreams()
                .Trace()
                .Map(ss => ss.Select(s => s.Trace()));
            var indexCatchup = StreamCatchup.Distribute(updatedStreams, updatedStreams.NewCursor(), batchCount: 1);
            var index = new List<AccountOpened>();

            // subscribe a catchup to the updates stream to build up an index
            indexCatchup.Subscribe<List<AccountOpened>, IDomainEvent>(
                async (p, events) =>
                {
                    p.AddRange(events.OfType<AccountOpened>()
                                     .Where(e => e.AccountType == BankAccountType.Savings));
                    return p;
                },
                async (streamId, aggregate) =>
                {
                    await aggregate(index);
                });

            // create a catchup over the index
            var savingsAccounts = Stream.Create(async q => index.Skip(q.Cursor.As<int>())
                                                                .Take(q.BatchCount ?? 1000));
            var savingsAccountsCursor = Cursor.Create(0);
            var savingsAccountsCatchup = StreamCatchup.Create(savingsAccounts);

            var count = 0;
            savingsAccountsCatchup.Subscribe<int, IDomainEvent>(
                manageProjection: async (streamId, aggregate) =>
                {
                    count = await aggregate(count, savingsAccountsCursor);
                },
                aggregate: async (c, es) => c + es.Count);

            using (indexCatchup.Poll(TimeSpan.FromMilliseconds(100)))
            using (savingsAccountsCatchup.Poll(TimeSpan.FromMilliseconds(50)))
            {
                await Wait.Until(() => count >= 50);
            }
        }
    }
}