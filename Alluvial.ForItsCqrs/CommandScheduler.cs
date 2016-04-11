using System;
using System.Data.Entity;
using System.Linq;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;
using SchedulerClock = Microsoft.Its.Domain.Sql.CommandScheduler.Clock;
using DomainClock = Microsoft.Its.Domain.Clock;

namespace Alluvial.For.ItsDomainSql
{
    public static class CommandScheduler
    {
        /// <summary>
        /// Creates a stream aggregator that advances scheduler clocks to the current domain time, triggering any commands that are scheduled on the clock and due.
        /// </summary>
        public static IStreamAggregator<CommandsApplied, SchedulerClock> AdvanceClocks() => Aggregator.Create<CommandsApplied, SchedulerClock>(async (applied, batch) =>
        {
            var trigger = Configuration.Current.SchedulerClockTrigger();

            foreach (var clock in batch)
            {
                var result = await trigger.AdvanceClock(
                    clock.Name, 
                    DomainClock.Now());

                foreach (var x in result.SuccessfulCommands)
                {
                    applied.Value.Add(x);
                }

                foreach (var x in result.FailedCommands)
                {
                    applied.Value.Add(x);
                }
            }

            return applied;
        });

        /// <summary>
        /// Gets a partitioned stream containing clocks that have commands due.
        /// </summary>
        /// <param name="asOf">The time as of which commands are evaluated to determine whether they are due.</param>
        public static IPartitionedStream<SchedulerClock, DateTimeOffset, string> ClocksWithCommandsDue(DateTimeOffset? asOf = null) =>
            Stream.PartitionedByRange<SchedulerClock, DateTimeOffset, string>(
                query: async (query, partition) =>
                {
                    using (var db = new CommandSchedulerDbContext())
                    {
                        var q = from clock in db.Clocks.WithinPartition(c => c.Name, partition)
                                join cmd in db.ScheduledCommands.Due(asOf)
                                    on clock equals cmd.Clock
                                orderby clock.UtcNow
                                select clock;

                        return await q.Take(query.BatchSize ?? 1)
                                      .ToArrayAsync();
                    }
                },
                advanceCursor: (q, b) => q.Cursor.AdvanceTo(DomainClock.Now()));

        /// <summary>
        /// Creates a partitioned stream of scheduled commands due on a specified scheduler clock.
        /// </summary>
        /// <param name="clockName">Name of the scheduler clock.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException">Clock name cannot be null, empty, or consist entirely of whitespace.</exception>
        public static IPartitionedStream<ScheduledCommand, DateTimeOffset, Guid> CommandsDueOnClock(string clockName)
        {
            if (String.IsNullOrWhiteSpace(clockName))
            {
                throw new ArgumentException("Clock name cannot be null, empty, or consist entirely of whitespace.", nameof(clockName));
            }

            return Stream.PartitionedByRange<ScheduledCommand, DateTimeOffset, Guid>(
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
                advanceCursor: (q, b) => q.Cursor.AdvanceTo(DomainClock.Now()));
        }

        /// <summary>
        /// Creates a stream aggregator that delivers scheduled commands.
        /// </summary>
        /// <returns></returns>
        public static IStreamAggregator<CommandsApplied, ScheduledCommand> DeliverScheduledCommands() =>
            Aggregator.Create<CommandsApplied, ScheduledCommand>(
                async (applied, batch) =>
                {
                    var configuration = Configuration.Current;

                    using (var commandSchedulerDb = new CommandSchedulerDbContext())
                    {
                        foreach (var cmd in batch)
                        {
                            await configuration.DeserializeAndDeliver(
                                cmd,
                                commandSchedulerDb);

                            applied.Value.Add(cmd.Result);
                        }
                        await commandSchedulerDb.SaveChangesAsync();
                    }

                    return applied;
                });
    }
}