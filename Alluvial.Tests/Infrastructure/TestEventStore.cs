using System;
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
                         .TrackPerformanceInstance("example")
                         .UsingJsonSerialization()
                         .Build();
        }
    }
}