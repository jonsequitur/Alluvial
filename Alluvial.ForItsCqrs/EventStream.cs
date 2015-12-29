using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;
using Pocket;

namespace Alluvial.ForItsCqrs
{
    public static class EventStream
    {
        public static IStreamAggregator<CommandsApplied, ScheduledCommand> DeliverScheduledCommands()
        {
            return Aggregator.Create<CommandsApplied, ScheduledCommand>(
                async (projection, batch) =>
                {
                    using (var commandSchedulerDb = new CommandSchedulerDbContext())
                    {
                        foreach (var cmd in batch)
                        {
                            await Configuration.Current.DeserializeAndDeliver(cmd, commandSchedulerDb);
                            projection.Value.Add(cmd.Result);
                        }
                        await commandSchedulerDb.SaveChangesAsync();
                    }

                    return projection;
                });
        }

        public static IPartitionedStream<ScheduledCommand, long, Guid> ScheduledCommandStream(string clockName = "default")
        {
            return Stream.PartitionedByRange<ScheduledCommand, long, Guid>(
                async (q, partition) =>
                {
                    using (var db = new CommandSchedulerDbContext())
                    {
                        var batchCount = q.BatchSize ?? 5;
                        var query = db.ScheduledCommands
                                      .Due()
                                      .Where(c => c.Clock.Name == clockName)
                                      .WithinPartition(e => e.AggregateId, partition)
                                      .Take(() => batchCount);
                        var scheduledCommands = await query.ToArrayAsync();
                        return scheduledCommands;
                    }
                },
                advanceCursor: (q, b) =>
                {
                    q.Cursor.AdvanceTo(DateTimeOffset.UtcNow.Ticks);

                    // advance the cursor to the latest due time
//                    var latestDuetime = b
//                        .Select(c => c.DueTime)
//                        .Where(d => d != null)
//                        .Select(d => d.Value)
//                        .OrderBy(d => d)
//                        .LastOrDefault();
//                    if (latestDuetime != default(DateTimeOffset))
//                    {
//                        q.Cursor.AdvanceTo(latestDuetime);
//                    }
//                    else
//                    {
//                        q.Cursor.AdvanceTo(DateTimeOffset.UtcNow);
//                    }
                });
        }

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
                .PartitionedByRange<EventStreamChange, long, Guid>(async (query, partition) => await EventStreamChanges(getStorableEvents(), query, partition),
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