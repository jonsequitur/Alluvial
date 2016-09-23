using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class LeaseTests
    {
        private Leasable<string> leasable;

        [SetUp]
        public void SetUp()
        {
            leasable = new Leasable<string>("hello", "hello");
        }

        [Test]
        public async Task When_an_extend_delegate_is_provided_then_when_Extend_is_called_it_is_called_with_the_specified_lease_extension_TimeSpan()
        {
            var extendedBy = TimeSpan.Zero;

            var lease = new Lease<string>(leasable,
                                          60.Seconds(),
                                          1,
                                          async ts => extendedBy = ts);

            await lease.Extend(TimeSpan.FromHours(3));

            extendedBy.Should().Be(3.Hours());
        }

        [Test]
        public async Task Lease_expiration_can_be_awaited()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var lease = new Lease<string>(leasable,
                                          100.Milliseconds(),
                                          1);

            await lease.Expiration();

            stopwatch.Elapsed
                     .Should()
                     .BeCloseTo(100.Milliseconds());
        }

        [Test]
        public async Task Lease_expiration_is_triggered_by_releasing_the_lease()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var lease = new Lease<string>(leasable,
                                          5.Seconds(),
                                          1);

            var task1 = Task.Run(async () => await lease.Expiration());

            var task2 = Task.Run(() => lease.Release().Wait());

            await Task.WhenAll(task1, task2);

            stopwatch.Elapsed
                     .Should()
                     .BeCloseTo(1.Milliseconds());
        }

        [Test]
        public async Task When_a_lease_is_extended_then_its_cancelation_token_is_extended()
        {
            var lease = new Lease<string>(leasable,
                                          200.Milliseconds(),
                                          1);

            await lease.Extend(TimeSpan.FromHours(3));

            await Task.Delay(1.Seconds());

            lease.CancellationToken.IsCancellationRequested.Should().BeFalse();
        }

        [Test]
        public async Task A_lease_can_be_extended_more_than_once_and_extensions_are_cumulative()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var lease = new Lease<string>(leasable,
                                          50.Milliseconds(),
                                          1);

            await lease.Extend(25.Milliseconds());
            await lease.Extend(25.Milliseconds());
            await lease.Extend(25.Milliseconds());
            await lease.Extend(25.Milliseconds());

            await lease.Expiration();

            stopwatch.Elapsed
                     .Should()
                     .BeCloseTo(150.Milliseconds(), 50);
        }

        [Test]
        public async Task A_lease_cannot_be_created_with_a_negative_duration()
        {
            Action create = () =>
                    new Lease(TimeSpan.FromSeconds(-1));

            create.ShouldThrow<ArgumentException>()
                  .And
                  .Message
                  .Should()
                  .Be("Lease duration cannot be negative.");
        }

        [Test]
        public async Task Leases_cannot_be_extended_by_a_negative_timespan()
        {
            var lease = new Lease<string>(leasable,
                                          20.Milliseconds(),
                                          1);

            Action extend = () => lease.Extend(-1.Seconds()).Wait();

            extend.ShouldThrow<ArgumentException>()
                  .And
                  .Message
                  .Should()
                  .Be("Lease cannot be extended by a negative timespan.");
        }
    }
}