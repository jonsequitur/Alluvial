using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NEventStore;

namespace Alluvial.Tests.StreamImplementations.NEventStore
{
    public static class NEventStoreStream
    {
        public static IStream<EventMessage, int> ByAggregate(
            IStoreEvents store,
            string streamId,
            string bucketId = "default") =>
                new NEventStoreAggregateStream(store, streamId, bucketId);

        public static IStream<EventMessage, string> AllEvents(IStoreEvents store) =>
            new NEventStoreAllEventsStream(store);

        public static IStream<string, string> AggregateIds(IStoreEvents store) =>
            new NEventStoreAllEventsStream(store).Map(es => es.Select(e => ((IDomainEvent) e.Body).AggregateId).Distinct());

        private class NEventStoreAggregateStream : 
            IStream<EventMessage, int>
        {
            private readonly IStoreEvents store;
            private readonly string streamId;
            private readonly string bucketId;

            public NEventStoreAggregateStream(
                IStoreEvents store, 
                string streamId,
                string bucketId = "default")
            {
                if (store == null)
                {
                    throw new ArgumentNullException(nameof(store));
                }
                if (streamId == null)
                {
                    throw new ArgumentNullException(nameof(streamId));
                }
                if (bucketId == null)
                {
                    throw new ArgumentNullException(nameof(bucketId));
                }
                this.store = store;
                this.streamId = streamId;
                this.bucketId = bucketId;
            }

            public string Id => streamId;

            /// <summary>
            /// Fetches a batch of data from the stream.
            /// </summary>
            /// <param name="query">The query to apply to the stream.</param>
            /// <returns></returns>
            public async Task<IStreamBatch<EventMessage>> Fetch(IStreamQuery<int> query)
            {
                var minRevisionToFetch = Math.Max(query.Cursor.Position, 0);

                int maxRevisionToFetch;

                checked
                {
                    maxRevisionToFetch = minRevisionToFetch + query.BatchSize ?? 100000;
                }

                var commits = store.Advanced
                                   .GetFrom(bucketId,
                                            streamId,
                                            minRevisionToFetch + 1,
                                            int.MaxValue);

                if (maxRevisionToFetch <= minRevisionToFetch)
                {
                    return query.Cursor.EmptyBatch<EventMessage, int>();
                }

                var events = new List<EventMessage>();

                foreach (var commit in commits
                    .Where(c => c.StreamRevision > minRevisionToFetch &&
                                c.StreamRevision <= maxRevisionToFetch))
                {
                    if (!commit.Events.Any())
                    {
                        break;
                    }

                    var eventMessages = commit.Events.Select(e =>
                    {
                        e.SetStreamRevision(
                            commit.StreamRevision,
                            commit.CheckpointToken);
                        return e;
                    }).ToArray();

                    events.AddRange(eventMessages);
                }

                var batch = StreamBatch.Create(events, query.Cursor);

                if (batch.Count > 0)
                {
                    query.Cursor.AdvanceTo(batch.Max(i => i.StreamRevision()));
                }

                return batch;
            }

            ICursor<int> IStream<EventMessage, int>.NewCursor() => Cursor.New(-1);
        }

        private class NEventStoreAllEventsStream : IStream<EventMessage, string>
        {
            private readonly IStoreEvents store;

            public NEventStoreAllEventsStream(IStoreEvents store)
            {
                if (store == null)
                {
                    throw new ArgumentNullException(nameof(store));
                }
                this.store = store;
            }

            public string Id => GetType().Name;

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

            public ICursor<string> NewCursor() => Cursor.New("");
        }
    }
}