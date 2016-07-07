using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Fluent;
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
        /// Gets a partitioned stream containing clocks that have commands due.
        /// </summary>
        /// <param name="asOf">The time as of which commands are evaluated to determine whether they are due.</param>
        public static IPartitionedStream<SchedulerClock, DateTimeOffset, string> ClocksWithCommandsDue(DateTimeOffset? asOf = null)
        {
            var configuration = Configuration.Current;

            return Stream.Of<SchedulerClock>()
                         .Cursor(_ => _.By<DateTimeOffset>())
                         .Advance((q, b) => q.Cursor.AdvanceTo(DomainClock.Now()))
                         .Partition(_ => _.ByRange<string>())
                         .Create(async (query, partition) =>
                         {
                             using (var db = configuration.CommandSchedulerDbContext())
                             {
                                 var q = from clock in db.Clocks
                                                         .AsNoTracking()
                                                         .WithinPartition(c => c.Name, partition)
                                         join cmd in db.ScheduledCommands.Due(asOf)
                                             on clock equals cmd.Clock
                                         orderby clock.UtcNow
                                         select clock;

                                 return await q.Take(query.BatchSize ?? 1)
                                               .ToArrayAsync();
                             }
                         });
        }

        /// <summary>
        /// Creates a partitioned stream of scheduled commands due on a specified scheduler clock.
        /// </summary>
        /// <param name="clockName">Name of the scheduler clock.</param>
        /// <param name="createDbContext">A delegate to create a CommandSchedulerDbContext to be used when delivering scheduled commands.</param>
        /// <exception cref="ArgumentException">Clock name cannot be null, empty, or consist entirely of whitespace.</exception>
        public static IPartitionedStream<ScheduledCommand, DateTimeOffset, Guid> CommandsDueOnClock(
            string clockName,
            Func<CommandSchedulerDbContext> createDbContext = null)
        {
            if (string.IsNullOrWhiteSpace(clockName))
            {
                throw new ArgumentException("Clock name cannot be null, empty, or consist entirely of whitespace.", nameof(clockName));
            }

            createDbContext = createDbContext ??
                              (() => Configuration.Current.CommandSchedulerDbContext());

            return Stream.Of<ScheduledCommand>()
                         .Named($"CommandsDueOnClock({clockName})")
                         .Cursor(_ => _.By<DateTimeOffset>())
                         .Advance((q, b) => q.Cursor.AdvanceTo(DomainClock.Now()))
                         .Partition(_ => _.ByRange<Guid>())
                         .Create(async (q, partition) =>
                         {
                             using (var db = createDbContext())
                             {
                                 var batchCount = q.BatchSize ?? 5;
                                 var query = db.ScheduledCommands
                                               .AsNoTracking()
                                               .Due()
                                               .Where(c => c.Clock.Name == clockName)
                                               .WithinPartition(e => e.AggregateId, partition)
                                               .Take(() => batchCount);
                                 return await query.ToArrayAsync();
                             }
                         });
        }

        /// <summary>
        /// Creates a stream aggregator that delivers scheduled commands.
        /// </summary>
        public static IStreamAggregator<CommandsApplied, ScheduledCommand> DeliverScheduledCommands() =>
            Aggregator.Create<CommandsApplied, ScheduledCommand>(
                async (applied, batch) =>
                {
                    var configuration = Configuration.Current;

                    using (var commandSchedulerDb =  Configuration.Current.CommandSchedulerDbContext())
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

        public static IDistributor<IStreamQueryPartition<Guid>> KeepClockUpdated(
            this IDistributor<IStreamQueryPartition<Guid>> distributor, 
            string clockName = "default")
        {
            distributor.OnReceive(async (lease, next) =>
            {
                await next(lease);
                await UpdateClock(clockName);
            });

            return distributor;
        }

        private static async Task UpdateClock(string clockName)
        {
            using (var db = Configuration.Current.CommandSchedulerDbContext())
            {
                db.Clocks
                    .Single(c => c.Name == clockName)
                    .UtcNow = DomainClock.Now();
                await db.SaveChangesAsync();
            }
        }
    }
}