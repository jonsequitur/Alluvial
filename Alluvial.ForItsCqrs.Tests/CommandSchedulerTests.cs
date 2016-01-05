using System;
using System.Data.Entity;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using FluentAssertions;
using Its.Log.Instrumentation;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;
using NUnit.Framework;

namespace Alluvial.Streams.ItsDomainSql.Tests
{
    [TestFixture]
    public class CommandSchedulerTests
    {
        private CompositeDisposable disposables;
        private String clockName;

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

            disposables.Add(ConfigurationContext.Establish(configuration));
            disposables = new CompositeDisposable();
        }

        [TearDown]
        public void TearDown()
        {
            disposables.Dispose();
        }

        [Test]
        public async Task The_command_scheduler_queue_can_be_processed_as_a_stream()
        {
            ScheduleSomeCommands(50);

            var catchup = EventStream.ScheduledCommandStream(clockName)
                                     .DistributeAmong(Partition.AllGuids()
                                                               .Among(16)
                                                               .ToArray());

            var store = new InMemoryProjectionStore<CommandsApplied>();

            catchup.Subscribe(EventStream.DeliverScheduledCommands().Trace(), store);

            await catchup.RunSingleBatch();

            Console.WriteLine(store.ToLogString());

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

        private static void ScheduleSomeCommands(
            int howMany = 20,
            DateTimeOffset? dueTime = null)
        {
            Enumerable.Range(1, howMany)
                      .Select(_ => Guid.NewGuid())
                      .ToList()
                      .ForEach(id =>
                      {
                          var scheduler = Configuration.Current.CommandScheduler<AggregateA>();
                          scheduler.Schedule(id,
                                             new CreateAggregateA(),
                                             dueTime).Wait();
                      });
        }
    }
}