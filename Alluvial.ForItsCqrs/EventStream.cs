using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Pocket;

namespace Alluvial.ForItsCqrs
{
    public static class EventStream
    {
        private static IStream<EventStreamChange, long> AllChanges(
            string streamId,
            Func<IQueryable<StorableEvent>> getStorableEvents)
        {
            return Stream
                .Create<EventStreamChange, long>(streamId,
                                                 async streamQuery => await EventStreamChanges(getStorableEvents(), streamQuery),
                                                 (q, b) => b.LastOrDefault()
                                                            .IfNotNull()
                                                            .ThenDo(u => q.Cursor.AdvanceTo(u.AbsoluteSequenceNumber)));
        }

        private static IPartitionedStream<EventStreamChange, long, Guid> AllChangesPartitioned(
            Func<IQueryable<StorableEvent>> getStorableEvents)
        {
            return Stream
                .PartitionedByRange<EventStreamChange, long, Guid>(
                    query: async (query, partition) => await EventStreamChanges(getStorableEvents(), query, partition),
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

            var fetchedFromEventStore = query
                .OrderBy(e => e.Id)
                .Select(e => new
                {
                    e.AggregateId,
                    e.StreamName,
                    e.Id
                })
                .Take(streamQuery.BatchSize ?? 10)
                .GroupBy(e => e.AggregateId)
                .ToArray();

            var eventStreamChanges = fetchedFromEventStore
                .Select(e => new EventStreamChange
                {
                    AggregateId = e.Key,
                    AggregateType = e.FirstOrDefault().StreamName,
                    AbsoluteSequenceNumber = e.OrderByDescending(ee => ee.Id).FirstOrDefault().Id
                })
                .OrderBy(e => e.AbsoluteSequenceNumber)
                .ToArray();

            return eventStreamChanges;
        }

        public static IStream<IStream<IEvent, long>, long> PerAggregate(
            string streamId,
            Func<IQueryable<StorableEvent>> getStorableEvents)
        {
            return AllChanges(streamId, getStorableEvents)
                .IntoMany(
                    async (update, fromCursor, toCursor) =>
                        Stream.Create<IEvent, long>(update.AggregateId.ToString(),
                                                    q => QueryAsync(getStorableEvents, q, update, fromCursor, toCursor),
                                                    (query, batch) => AdvanceCursor(batch, query, toCursor)));
        }

        private static void AdvanceCursor(
            IStreamBatch<IEvent> batch,
            IStreamQuery<long> query,
            long toCursor)
        {
            batch.LastOrDefault()
                 .IfNotNull()
                 .ThenDo(e => query.Cursor.AdvanceTo(toCursor));
        }

        private static async Task<IEnumerable<IEvent>> QueryAsync(
            Func<IQueryable<StorableEvent>> getStorableEvents,
            IStreamQuery<long> q,
            EventStreamChange update,
            long fromCursor,
            long toCursor)
        {
            {
                var query = getStorableEvents()
                    .Where(e => e.AggregateId == update.AggregateId)
                    .Where(e => e.Id >= fromCursor && e.Id <= toCursor);

                query = query.OrderBy(e => e.Id)
                             .Take(q.BatchSize ?? 1000);

                var events = query.ToArray();

                return events.Select(e => e.ToDomainEvent());
            }
        }
    }
}