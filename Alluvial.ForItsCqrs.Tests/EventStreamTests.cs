using System.Linq;
using FluentAssertions;
using Microsoft.Its.Domain.Sql;
using NUnit.Framework;

namespace Alluvial.ForItsCqrs.Tests
{
    [TestFixture]
    public class EventStreamTests
    {
        [Test]
        public void Trivial()
        {
            var stream = EventStream.AllChanges("My Stream ID", () => new[]
            {
                new StorableEvent {Id = 1}
            }.AsQueryable());

            var results = stream.CreateQuery().NextBatch().Result;

            results.Should().HaveCount(1);
        }
    }
}