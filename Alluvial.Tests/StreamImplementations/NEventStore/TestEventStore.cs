using System;
using Alluvial.Tests.BankDomain;
using Its.Log.Instrumentation;
using NEventStore;

namespace Alluvial.Tests
{
    public static class TestEventStore
    {
        static TestEventStore()
        {
            Formatter<EventMessage>.RegisterForAllMembers();
        }

        public static IStoreEvents Create()
        {
            return Wireup.Init()
                         .UsingInMemoryPersistence()
                         .InitializeStorageEngine()
                         .UsingJsonSerialization()
                         .Build();
        }

        public static IStoreEvents Populate(this IStoreEvents store, string streamId = null)
        {
            streamId = streamId ?? Guid.NewGuid().ToString();

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

            return store;
     ;   }
    }
}