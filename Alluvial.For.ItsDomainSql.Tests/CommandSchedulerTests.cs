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

namespace Alluvial.For.ItsDomainSql.Tests
{
    [TestFixture]
    public class CommandSchedulerTests
    {
        private CompositeDisposable disposables;
        private string clockName;
        private IStreamQueryRangePartition<Guid>[] partitionsByAggregateId;

        [TestFixtureSetUp]
        public void Init()
        {
            DatabaseSetup.Run();
        }

        [SetUp]
        public void SetUp()
        {
            clockName = Guid.NewGuid().ToString();
            disposables = new CompositeDisposable();

            var configuration = new Configuration()
                .UseSqlEventStore()
                .UseSqlStorageForScheduledCommands()
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

            var catchup = CommandScheduler.CommandsDueOnClock(clockName)
                                          .CreateDistributedCatchup()
                                          .DistributeSqlBrokeredLeasesAmong(
                                              partitionsByAggregateId,
                                              new SqlBrokeredDistributorDatabase(CommandSchedulerDbContext.NameOrConnectionString),
                                              CommandScheduler.CommandsDueOnClock(clockName).Id);

            var store = new InMemoryProjectionStore<CommandsApplied>();

            catchup.Subscribe(CommandScheduler.DeliverScheduledCommands().Trace(), store);

            // act
            await catchup.RunSingleBatch().Timeout();

            // assert
            store.Sum(c => c.Value.Count).Should().Be(50);

            using (var db = new CommandSchedulerDbContext())
            {
                var commandsDue = await db.ScheduledCommands
                                          .Where(c => c.Clock.Name == clockName)
                                          .Due()
                                          .CountAsync();
                commandsDue.Should().Be(0);
            }
        }

        [Test]
        public async Task All_clocks_can_be_advanced_in_parallel_using_a_distributor()
        {
            // arrange
            var commandsScheduled = await ScheduleSomeCommands(
                50,
                Clock.Now().Subtract(1.Hours()),
                clockName: () => Guid.NewGuid().ToString());

            var partitions = new[]
            {
                Partition.ByRange("", "3"),
                Partition.ByRange("3", "7"),
                Partition.ByRange("7", "b"),
                Partition.ByRange("b", "f"),
                Partition.ByRange("f", "j"),
                Partition.ByRange("j", "n"),
                Partition.ByRange("n", "r"),
                Partition.ByRange("r", "v"),
                Partition.ByRange("v", "zz")
            };

            var catchup = CommandScheduler.ClocksWithCommandsDue()
                                          .Trace()
                                          .CreateDistributedCatchup()
                                          .DistributeInMemoryAmong(partitions);
            var store = new InMemoryProjectionStore<CommandsApplied>();
            var aggregator = CommandScheduler.AdvanceClocks();
            catchup.Subscribe(aggregator, store);

            // act
            await catchup.RunUntilCaughtUp();

            // assert
            store.Count().Should().Be(partitions.Length);

            var commandsDelivered = store
                .SelectMany(_ => _.Value)
                .ToArray();

            commandsDelivered.Length
                             .Should()
                             .Be(commandsScheduled.Count());
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