using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Pocket;

namespace Alluvial.For.ItsDomainSql
{
    public static class EventStream
    {
        /// <summary>
        /// Returns a stream of all events in the event store, in the order they were inserted.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="storableEvents">A delegate returning a query of events to include in the stream.</param>
        public static IPartitionedStream<IEvent, long, Guid> Events(
            string streamId,
            Func<IQueryable<StorableEvent>> storableEvents)
        {
            return Stream.PartitionedByRange<StorableEvent, long, Guid>(
                query: async (query, partition) =>
                       await storableEvents()
                                 .Where(e => e.Id > query.Cursor.Position)
                                 .WithinPartition(e => e.AggregateId, partition)
                                 .OrderBy(e => e.Id)
                                 .Take(query.BatchSize ?? 100)
                                 .ToArrayAsync(),
                id: streamId,
                advanceCursor: (q, b) => b.LastOrDefault()
                                          .IfNotNull()
                                          .ThenDo(u => q.Cursor.AdvanceTo(u.Id)))
                         .Map(es => es.Select(e => e.ToDomainEvent()));
        }

        /// <summary>
        /// Returns a stream of streams, each of which contains all of the events for a single aggregate.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="storableEvents">A delegate returning a query of events to include in the stream.</param>
        public static IStream<IStream<IEvent, long>, long> PerAggregate(
            string streamId,
            Func<IQueryable<StorableEvent>> storableEvents) =>
                AllChanges(streamId, storableEvents)
                    .IntoMany(
                        // for each EventStreamChange, return a stream of IEvent
                        (update, fromCursor, toCursor) =>
                        Stream.Create<IEvent, long>(
                            update.AggregateId.ToString(),
                            q => QueryAsync(storableEvents,
                                            q.BatchSize,
                                            update.AggregateId,
                                            fromCursor,
                                            toCursor),
                            (query, batch) => AdvanceCursor(batch, query, toCursor)).CompletedTask());

        /// <summary>
        /// Returns a partitioned stream of streams, each of which contains all of the events for a single aggregate.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="storableEvents">A delegate returning a query of events to include in the stream.</param>
        public static IPartitionedStream<IStream<IEvent, long>, long, Guid> PerAggregatePartitioned(
            string streamId,
            Func<IQueryable<StorableEvent>> storableEvents) =>
                AllChangesPartitioned(streamId, storableEvents)
                    .IntoMany((update, fromCursor, toCursor, partition) =>
                              Stream.Create<IEvent, long>(
                                  update.AggregateId.ToString(),
                                  q => QueryAsync(storableEvents,
                                                  q.BatchSize,
                                                  update.AggregateId,
                                                  fromCursor,
                                                  toCursor),
                                  (query, batch) => AdvanceCursor(batch, query, toCursor))
                                    .CompletedTask());

        private static IStream<EventStreamChange, long> AllChanges(
            string streamId,
            Func<IQueryable<StorableEvent>> storableEvents)
        {
            Func<IStreamQuery<long>, Task<IEnumerable<EventStreamChange>>> query =
                async streamQuery =>
                await EventStreamChanges(storableEvents(), streamQuery);

            return Stream
                .Create(
                    streamId,
                    query,
                    (q, b) => b.LastOrDefault()
                               .IfNotNull()
                               .ThenDo(u => q.Cursor.AdvanceTo(u.AbsoluteSequenceNumber)));
        }

        private static IPartitionedStream<EventStreamChange, long, Guid> AllChangesPartitioned(
            string streamId,
            Func<IQueryable<StorableEvent>> getStorableEvents)
        {
            Func<IStreamQuery<long>, IStreamQueryRangePartition<Guid>, Task<IEnumerable<EventStreamChange>>> query =
                async (streamQuery, partition) =>
                await EventStreamChanges(getStorableEvents(),
                                         streamQuery,
                                         partition);

            return Stream
                .PartitionedByRange(
                    id: streamId,
                    query: query,
                    advanceCursor: (q, b) => b.LastOrDefault()
                                              .IfNotNull()
                                              .ThenDo(u => q.Cursor.AdvanceTo(u.AbsoluteSequenceNumber)));
        }

        private static async Task<IEnumerable<EventStreamChange>> EventStreamChanges(
            IQueryable<StorableEvent> events,
            IStreamQuery<long> streamQuery,
            IStreamQueryRangePartition<Guid> partition = null)
        {
            var query = events
                .Where(e => e.Id > streamQuery.Cursor.Position);

            if (partition != null)
            {
                query = query.WithinPartition(e => e.AggregateId, partition);
            }

            var fetchedFromEventStore = await query
                                                  .OrderBy(e => e.Id)
                                                  .Select(e => new
                                                  {
                                                      e.AggregateId,
                                                      e.StreamName,
                                                      e.Id
                                                  })
                                                  .Take(streamQuery.BatchSize ?? 10)
                                                  .GroupBy(e => e.AggregateId)
                                                  .ToArrayAsync();

            var eventStreamChanges = fetchedFromEventStore
                .Select(e => new EventStreamChange(e.Key)
                {
                    AggregateType = e.FirstOrDefault()?.StreamName,
                    AbsoluteSequenceNumber = e.OrderByDescending(ee => ee.Id).FirstOrDefault()?.Id ?? 0
                })
                .OrderBy(e => e.AbsoluteSequenceNumber)
                .ToArray();

            return eventStreamChanges;
        }

        private static void AdvanceCursor(
            IStreamBatch<IEvent> batch,
            IStreamQuery<long> query,
            long toCursor) =>
                batch.LastOrDefault()
                     .IfNotNull()
                     .ThenDo(e => query.Cursor.AdvanceTo(toCursor));

        private static async Task<IEnumerable<IEvent>> QueryAsync(
            Func<IQueryable<StorableEvent>> storableEvents,
            int? batchSize,
            Guid aggregateId,
            long fromCursor,
            long toCursor)
        {
            var query = storableEvents()
                .Where(e => e.AggregateId == aggregateId)
                .Where(e => e.Id >= fromCursor && e.Id <= toCursor);

            query = query.OrderBy(e => e.Id)
                         .Take(batchSize ?? 1000);

            var events = await query.ToArrayAsync();

            return events.Select(e => e.ToDomainEvent());
        }
    }
}