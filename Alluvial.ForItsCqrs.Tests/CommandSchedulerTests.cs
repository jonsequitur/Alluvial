using System;
using System.Collections.Generic;
using System.Data.Entity;
using FluentAssertions;
using Its.Log.Instrumentation;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;
using NUnit.Framework;

namespace Alluvial.ForItsCqrs.Tests
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
            ScheduleSomeCommands();

            var catchup = ScheduledCommandStream()
                .DistributeAmong(Partition.AllGuids()
                                          .Among(15)
                                          .ToArray());

            var store = new InMemoryProjectionStore<CommandsApplied>();

            catchup.Subscribe(DeliverScheduledCommands().Trace(), store);

            await catchup.RunSingleBatch();

            Console.WriteLine(store.ToLogString());

            using (var db = new CommandSchedulerDbContext())
            {
                store.Select(p => p.Value)
                     .Sum(p => p.Count)
                     .Should()
                     .Be(await db.ScheduledCommands.Due().CountAsync());
            }
        }

        private static void ScheduleSomeCommands(DateTimeOffset? dueTime = null)
        {
            Enumerable.Range(1, 20)
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

        public IPartitionedStream<ScheduledCommand, long, Guid> ScheduledCommandStream()
        {
            return Stream.PartitionedByRange<ScheduledCommand, long, Guid>(
                async (q, partition) =>
                {
                    using (var db = new CommandSchedulerDbContext())
                    {
                        var batchCount = q.BatchSize ?? 5;
                        return await db.ScheduledCommands
                                       .Where(c => c.Clock.Name == clockName)
                                       .Due()
                                       .WithinPartition(e => e.AggregateId, partition)
                                       .Take(() => batchCount)
                                       .ToArrayAsync();
                    }
                },
                advanceCursor: (q, b) => q.Cursor.AdvanceTo(DateTime.Now.Ticks));
        }
    }

    public class CommandsApplied : Projection<IList<ScheduledCommandResult>, long>
    {
        public CommandsApplied()
        {
            Value = new List<ScheduledCommandResult>();
        }
    }
}