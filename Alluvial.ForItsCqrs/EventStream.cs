using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Recipes;

namespace Alluvial.ForItsCqrs
{
    public static class EventStream
    {
        public static IStream<EventStreamChange, long> AllChanges(
            string streamId,
            Func<IQueryable<StorableEvent>> getStorableEvents,
            int? batchCount = null)
        {
            return Stream
                .Create<EventStreamChange, long>(streamId,
                    async streamQuery => await EventStreamChanges(getStorableEvents(), streamQuery),
                    (q, b) => b.LastOrDefault()
                        .IfNotNull()
                        .ThenDo(u => q.Cursor.AdvanceTo(u.AbsoluteSequenceNumber)));
        }

        private static async Task<IEnumerable<EventStreamChange>> EventStreamChanges(IQueryable<StorableEvent> events,
            IStreamQuery<long> streamQuery)
        {
            var query = events
                .Where(e => e.Id > streamQuery.Cursor.Position);

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

        public static IStream<IStream<IEvent, long>, long> PerAggregate(string streamId, Func<IQueryable<StorableEvent>, IQueryable<StorableEvent>> filter, Func<IQueryable<StorableEvent>> getStorableEvents)
        {
            return AllChanges(streamId, getStorableEvents).Trace()
                .IntoMany(
                    async (update, fromCursor, toCursor) =>
                    {
                        return Stream.Create<IEvent, long>(update.AggregateId.ToString(),
                            q => QueryAsync(filter, q, update, fromCursor, toCursor),
                            (query, batch) => AdvanceCursor(batch, query, toCursor));
                    });
        }

        private static Maybe<Unit> AdvanceCursor(IStreamBatch<IEvent> batch, IStreamQuery<long> query, long toCursor)
        {
            return batch.LastOrDefault()
                .IfNotNull()
                .ThenDo(e => query.Cursor.AdvanceTo(toCursor));
        }

        private static async Task<IEnumerable<IEvent>> QueryAsync(
            Func<IQueryable<StorableEvent>, IQueryable<StorableEvent>> filter,
            IStreamQuery<long> q,
            EventStreamChange update,
            long fromCursor,
            long toCursor)
        {
            using (var db = new EventStoreDbContext())
            {
                var query = db.Events
                    .Where(e => e.AggregateId == update.AggregateId)
                    .Where(e => e.Id >= fromCursor && e.Id <= toCursor);

                query = filter(query).OrderBy(e => e.Id)
                    .Take(q.BatchSize ?? 1000);

                var events = (await query.ToArrayAsync())
                    .ToArray();

                foreach (var e in events)
                    Trace.WriteLine(e.Id);

                return events.Select(e => e.ToDomainEvent());
            }
        }
    }
}