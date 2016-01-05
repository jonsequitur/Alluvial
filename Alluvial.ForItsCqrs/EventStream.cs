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

        public static IStream<IStream<IEvent, long>, long> PerAggregate(
            string streamId,
            Func<IQueryable<StorableEvent>> storableEvents)
        {
            return AllChanges(streamId, storableEvents)
                .IntoMany(
                    // for each EventStreamChange, return a stream of IEvent
                    async (update, fromCursor, toCursor) =>
                        Stream.Create<IEvent, long>(
                            update.AggregateId.ToString(),
                            q => QueryAsync(storableEvents,
                                            q.BatchSize,
                                            update.AggregateId,
                                            fromCursor,
                                            toCursor),
                            (query, batch) => AdvanceCursor(batch, query, toCursor)));
        }

        public static IPartitionedStream<IStream<IEvent, long>, long, Guid> PerAggregatePartitioned(
            string streamId,
            Func<IQueryable<StorableEvent>> storableEvents)
        {
            return AllChangesPartitioned(streamId, storableEvents)
                .Trace()
                .IntoMany(async (update, fromCursor, toCursor, partition) =>
                              Stream.Create<IEvent, long>(
                                  update.AggregateId.ToString(),
                                  q => QueryAsync(storableEvents,
                                                  q.BatchSize,
                                                  update.AggregateId,
                                                  fromCursor,
                                                  toCursor),
                                  (query, batch) => AdvanceCursor(batch, query, toCursor))
                                    .Trace());
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
            Func<IStreamQuery<long>, Task<IEnumerable<EventStreamChange>>> query =
                async streamQuery =>
                    await EventStreamChanges(getStorableEvents(), streamQuery);

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
            Func<IQueryable<StorableEvent>> storableEvents,
            int? batchSize,
            Guid aggregateId,
            long fromCursor,
            long toCursor)
        {
            {
                var query = storableEvents()
                    .Where(e => e.AggregateId == aggregateId)
                    .Where(e => e.Id >= fromCursor && e.Id <= toCursor);

                query = query.OrderBy(e => e.Id)
                             .Take(batchSize ?? 1000);

                var events = query.ToArray();

                return events.Select(e => e.ToDomainEvent());
            }
        }
    }
}