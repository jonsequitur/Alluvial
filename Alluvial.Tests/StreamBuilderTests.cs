using System;
using System.Linq;
using System.Threading.Tasks;
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
            partitioned1 = new StreamBuilder<string>("partitioned-1")
                .Cursor(_ => _.StartsAt(() => Cursor.New(0)))
                .Advance((q, b) => q.Cursor.AdvanceTo(b.Count()))
                .Partition(_ => _.By<Guid>())
                .CreateStream();

            IPartitionedStream<int, int, Guid> partitioned2;
            partitioned2 = new StreamBuilder<int>("partitioned-2")
                .Cursor(_ => _.By<int>())
                .Partition(_ => _.By<Guid>())
                .Advance((q, b) => q.Cursor.AdvanceTo(1))
                .CreateStream();

            IStream<Event, DateTimeOffset> nonPartitioned;
            nonPartitioned = new StreamBuilder<Event>("nonpartitioned")
                .Cursor(_ => _.StartsAt(() => Cursor.New<DateTimeOffset>()))
                .Advance((q, b) => q.Cursor.AdvanceTo(b.Last().Timestamp))
                .CreateStream();

            // FIX (testname) write test
            Assert.Fail("Test not written yet.");
        }
    }
}