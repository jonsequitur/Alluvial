using System;
using System.Collections.Generic;
using System.Data.Entity;
using FluentAssertions;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using Alluvial.Distributors.Sql;
using Alluvial.Tests;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;
using NUnit.Framework;
using Clock = Microsoft.Its.Domain.Clock;
using static Alluvial.For.ItsDomainSql.Tests.TestDatabases;

namespace Alluvial.For.ItsDomainSql.Tests
{
    [TestFixture]
    public class CommandSchedulerTests
    {
        private CompositeDisposable disposables;
        private string clockName;
        private IStreamQueryRangePartition<Guid>[] partitionsByAggregateId;

        [SetUp]
        public void SetUp()
        {
            clockName = Guid.NewGuid().ToString();
            disposables = new CompositeDisposable();

            var configuration = new Configuration()
                .UseSqlEventStore(
                    c => c.UseConnectionString(EventStoreConnectionString))
                .UseSqlStorageForScheduledCommands(
                    c => c.UseConnectionString(CommandSchedulerConnectionString))
                .UseDependency<GetClockName>(c => _ => clockName)
                .TraceScheduledCommands();

            partitionsByAggregateId = Partition.AllGuids().Among(16).ToArray();

            disposables.Add(ConfigurationContext.Establish(configuration));
        }

        [TearDown]
        public void TearDown()
        {
            disposables.Dispose();
        }

        [Test]
        public async Task Commands_on_a_single_clock_can_be_processed_as_a_stream()
        {
            // arrange
            await ScheduleSomeCommands(50);

            var commandsDue = CommandScheduler.CommandsDueOnClock(clockName);

            var distributor = partitionsByAggregateId.CreateSqlBrokeredDistributor(
                new SqlBrokeredDistributorDatabase(CommandSchedulerConnectionString),
                commandsDue.Id);

            var catchup = commandsDue
                .CreateDistributedCatchup(distributor);

            var store = new InMemoryProjectionStore<CommandsApplied>();

            catchup.Subscribe(CommandScheduler.DeliverScheduledCommands().Trace(), store);

            // act
            await catchup.RunSingleBatch().Timeout();

            // assert
            store.Sum(c => c.Value.Count).Should().Be(50);

            using (var db = Configuration.Current.CommandSchedulerDbContext())
            {
                var remainingCommandsDue = await db.ScheduledCommands
                                          .Where(c => c.Clock.Name == clockName)
                                          .Due()
                                          .CountAsync();
                remainingCommandsDue.Should().Be(0);
            }
        }

        [Test]
        public async Task When_the_distributor_runs_then_the_clock_can_be_kept_updated()
        {
            // arrange
            var commandsDue = CommandScheduler.CommandsDueOnClock(clockName);

            DateTimeOffset timePriorToDeliveringCommands;
            using (var db = Configuration.Current.CommandSchedulerDbContext())
            {
                timePriorToDeliveringCommands = db.Clocks
                    .Single(c => c.Name == "default")
                    .UtcNow;
            }

            var distributor = partitionsByAggregateId
                .CreateSqlBrokeredDistributor(
                    new SqlBrokeredDistributorDatabase(CommandSchedulerConnectionString),
                    commandsDue.Id)
                .KeepClockUpdated();

            var catchup = commandsDue
                .CreateDistributedCatchup(distributor);

            catchup.Subscribe(CommandScheduler.DeliverScheduledCommands().Trace());

            // act
            await catchup.RunSingleBatch().Timeout();

            // assert
            using (var db = Configuration.Current.CommandSchedulerDbContext())
            {
                db.Clocks
                    .Single(c => c.Name == "default")
                    .UtcNow
                    .Should()
                    .BeAfter(timePriorToDeliveringCommands);
            }
        }

        private static async Task<IEnumerable<IScheduledCommand<AggregateA>>> ScheduleSomeCommands(
            int howMany = 20,
            DateTimeOffset? dueTime = null,
            Func<string> clockName = null)
        {
            if (clockName != null)
            {
                Configuration.Current.UseDependency<GetClockName>(c => _ => clockName());
            }

            var commandsScheduled = new List<IScheduledCommand<AggregateA>>();

            foreach (var id in Enumerable.Range(1, howMany).Select(_ => Guid.NewGuid()))
            {
                var scheduler = Configuration.Current.CommandScheduler<AggregateA>();
                var command = await scheduler.Schedule(id,
                                                       new CreateAggregateA
                                                       {
                                                           AggregateId = id
                                                       },
                                                       dueTime);
                commandsScheduled.Add(command);
            }

            return commandsScheduled;
        }
    }
}