using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NEventStore;

namespace Alluvial.Tests
{
    public static class NEventStoreStream
    {
        public static IStream<EventMessage, int> ByAggregate(IStoreEvents store, string streamId)
        {
            return new NEventStoreAggregateStream(store, streamId);
        }

        public static IStream<EventMessage, string> AllEvents(IStoreEvents store)
        {
            return new NEventStoreAllEventsStream(store);
        }

        public static IStream<string, string> AggregateIds(IStoreEvents store)
        {
            return new NEventStoreAllEventsStream(store).Map(es => es.Select(e => ((IDomainEvent) e.Body).AggregateId).Distinct());
        }

        private class NEventStoreAggregateStream : IStream<EventMessage, int>
        {
            private readonly IStoreEvents store;
            private readonly string streamId;

            public NEventStoreAggregateStream(IStoreEvents store, string streamId)
            {
                if (store == null)
                {
                    throw new ArgumentNullException("store");
                }
                if (streamId == null)
                {
                    throw new ArgumentNullException("streamId");
                }
                this.store = store;
                this.streamId = streamId;
            }

            public string Id
            {
                get
                {
                    return streamId;
                }
            }

            public async Task<IStreamBatch<EventMessage>> Fetch(IStreamQuery<int> query)
            {
                var lastFetchedRevision = Math.Max(query.Cursor.Position, 0);

                int maxRevisionToFetch;

                checked
                {
                    maxRevisionToFetch = lastFetchedRevision + query.BatchSize ?? 100000;
                }

                var maxExistingRevision = store.Advanced
                                               .GetFrom("default",
                                                        streamId,
                                                        lastFetchedRevision,
                                                        int.MaxValue)
                                               .Select(c => c.StreamRevision)
                                               .LastOrDefault();

                if (maxExistingRevision <= lastFetchedRevision)
                {
                    return query.Cursor.EmptyBatch<EventMessage, int>();
                }

                var events = new List<EventMessage>();

                checked
                {
                    for (var i = lastFetchedRevision + 1; i <= maxRevisionToFetch; i++)
                    {
                        try
                        {
                            using (var stream = store.OpenStream(streamId,
                                                                 minRevision: i,
                                                                 maxRevision: i))
                            {
                                if (stream.CommittedEvents.Count == 0)
                                {
                                    break;
                                }

                                events.AddRange(stream.CommittedEvents
                                                      .Select(e =>
                                                      {
                                                          e.SetStreamRevision(stream.StreamRevision);
                                                          return e;
                                                      }));
                            }
                        }
                        catch (StreamNotFoundException)
                        {
                            break;
                        }
                    }
                }

                var batch = StreamBatch.Create(events, query.Cursor);

                if (batch.Count > 0)
                {
                    query.Cursor.AdvanceTo(batch.Max(i => i.StreamRevision()));
                }

                return batch;
            }

            public ICursor<int> NewCursor()
            {
                return Cursor.New<int>(-1);
            }

        }

        private class NEventStoreAllEventsStream : IStream<EventMessage, string>
        {
            private readonly IStoreEvents store;

            public NEventStoreAllEventsStream(IStoreEvents store)
            {
                if (store == null)
                {
                    throw new ArgumentNullException("store");
                }
                this.store = store;
            }

            public string Id
            {
                get
                {
                    return GetType().Name;
                }
            }

            public async Task<IStreamBatch<EventMessage>> Fetch(IStreamQuery<string> query)
            {
                var commits = store.Advanced.GetFrom(query.Cursor.Position);

                var batchSize = query.BatchSize ?? 100;
                var actualCount = 0;

                var events = new List<EventMessage>();
                var cursorPosition = query.Cursor.Position;

                foreach (var commit in commits)
                {
                    actualCount += commit.Events.Count;

                    if (actualCount > batchSize)
                    {
                        break;
                    }

                    events.AddRange(commit.Events);

                    foreach (var @event in commit.Events.Select(e => e.Body).OfType<IDomainEvent>())
                    {
                        @event.StreamRevision = commit.StreamRevision;
                        @event.CheckpointToken = commit.CheckpointToken;
                    }

                    cursorPosition = commit.CheckpointToken;
                }

                var batch = StreamBatch.Create(events, query.Cursor);

                if (batch.Count > 0)
                {
                    query.Cursor.AdvanceTo(cursorPosition);
                }

                return batch;
            }

            public ICursor<string> NewCursor()
            {
                return Cursor.New("");
            }
        }
    }
}