using System;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Fluent;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamBuilderTests
    {
        [Test]
        public async Task StreamBuilder_examples()
        {
            IPartitionedStream<string, int, Guid> partitioned1;
            partitioned1 =
                Stream.Of<string>("partitioned-1")
                      .Cursor(_ => _.StartsAt(() => Cursor.New(0)))
                      .Advance((q, b) => q.Cursor.AdvanceTo(b.Count()))
                      .Partition(_ => _.ByRange<Guid>())
                      .Create(async (query, partition) =>
                                    Enumerable.Range(1, 1000)
                                              .Select(i => i.ToString())
                                              .Skip(query.Cursor.Position)
                                        //  .Where(s => partition.Contains(s))
                                              .Take(query.BatchSize.Value));

            IPartitionedStream<int, int, string> partitioned2;
            partitioned2 =
                Stream.Of<int>("partitioned-2")
                      .Cursor(_ => _.By<int>())
                      .Partition(_ => _.ByValue<string>())
                      .Advance((q, b) => q.Cursor.AdvanceTo(1))
                      .Create(async (query, partition) =>
                                    Enumerable.Range(1, 1000)
                                              .Skip(query.Cursor.Position)
                                              .Take(query.BatchSize.Value));

            IStream<Event, DateTimeOffset> nonPartitioned;
            nonPartitioned =
                Stream.Of<Event>("nonpartitioned")
                      .Cursor(_ => _.StartsAt(() => Cursor.New<DateTimeOffset>()))
                      .Advance((q, b) => q.Cursor.AdvanceTo(b.Last().Timestamp))
                      .Create(query => Enumerable.Range(1, 1000)
                                                       .Take(query.BatchSize.Value)
                                                       .Select(_ => new Event()));
        }
    }
}