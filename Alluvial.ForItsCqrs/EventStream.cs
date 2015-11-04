using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Recipes;

namespace Alluvial.ForItsCqrs
{
    public class DisposableQueryable<T> : IDisposable
    {
        private readonly Action dispose;

        public DisposableQueryable(Action dispose, IQueryable<T> queryable)
        {
            if (dispose == null) throw new ArgumentNullException(nameof(dispose));
            if (queryable == null) throw new ArgumentNullException(nameof(queryable));
            this.dispose = dispose;
            Queryable = queryable;
        }

        public IQueryable<T> Queryable { get; private set; }

        public void Dispose()
        {
            dispose();
        }
    }
    public static class EventStream
    {
        
        public static IStream<EventStreamChange, long> AllChanges(
            string streamId,
            Func<DisposableQueryable<StorableEvent>> getStorableEventsQueryable,
            int? batchCount = null)
        {
            return Stream
                .Create<EventStreamChange, long>(streamId,
                    async streamQuery =>
                    {
                        
                            return await EventStreamChanges(getStorableEventsQueryable, streamQuery);
                        
                    },
                    (q, b) => b.LastOrDefault()
                        .IfNotNull()
                        .ThenDo(u => q.Cursor.AdvanceTo(u.AbsoluteSequenceNumber)));
        }

        private static async Task<IEnumerable<EventStreamChange>> EventStreamChanges(Func<DisposableQueryable<StorableEvent>> getStorableEventsQueryable,
            IStreamQuery<long> streamQuery)
        {
            using (var events = getStorableEventsQueryable())
            {
                var query = events.Queryable
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
        }

        public static IStream<IStream<IEvent, long>, long> PerAggregate(string streamId, Func<DisposableQueryable<StorableEvent>> getStorableEvents)
        {
            return AllChanges(streamId, getStorableEvents).Trace()
                .IntoMany(
                    async (update, fromCursor, toCursor) =>
                    {
                        return Stream.Create<IEvent, long>(update.AggregateId.ToString(),
                            q => QueryAsync(getStorableEvents, q, update, fromCursor, toCursor),
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
            Func<DisposableQueryable<StorableEvent>> getStorableEvents,
            IStreamQuery<long> q,
            EventStreamChange update,
            long fromCursor,
            long toCursor)
        {
            {
                using (var disposableQueryable = getStorableEvents())
                {
                    var query = disposableQueryable.Queryable
                        .Where(e => e.AggregateId == update.AggregateId)
                        .Where(e => e.Id > fromCursor && e.Id <= toCursor);

                    query = query.OrderBy(e => e.Id)
                        .Take(q.BatchSize ?? 1000);

                    var events = query.ToArray();

                    foreach (var e in events)
                        Trace.WriteLine(e.Id);

                    return events.Select(e => e.ToDomainEvent());
                }
            }
        }
    }
}