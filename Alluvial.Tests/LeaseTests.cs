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
        public async Task When_an_expireIn_delegate_is_provided_then_when_Expire_is_called_it_is_called_with_the_specified_lease_extension_TimeSpan()
        {
            var expireIn = TimeSpan.Zero;

            var lease = new Lease<string>(leasable,
                                          60.Seconds(),
                                          1,
                                          expireIn: async ts => expireIn = ts);

            await lease.ExpireIn(TimeSpan.FromHours(3));

            expireIn.Should().Be(3.Hours());
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
        public async Task When_a_lease_is_extended_using_ExpireIn_then_its_cancelation_token_is_extended()
        {
            var lease = new Lease<string>(leasable,
                                          200.Milliseconds(),
                                          1);

            await lease.ExpireIn(TimeSpan.FromHours(3));

            await Task.Delay(1.Seconds());

            lease.CancellationToken.IsCancellationRequested.Should().BeFalse();
        }

        [Test]
        public async Task When_a_lease_is_shortened_using_ExpireIn_then_its_cancelation_token_is_shortened()
        {
            var lease = new Lease<string>(leasable,
                1.Hours(),
                1);

            await lease.ExpireIn(200.Milliseconds());

            await Task.Delay(1.Seconds());

            lease.CancellationToken.IsCancellationRequested.Should().BeTrue();
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

            Action extend = () => lease.ExpireIn(-1.Seconds()).Wait();

            extend.ShouldThrow<ArgumentException>()
                  .And
                  .Message
                  .Should()
                  .Be("Lease cannot be extended by a negative timespan.");
        }

        [Test]
        public async Task When_ExpireIn_is_called_after_the_lease_has_expired_then_it_throws()
        {
            var lease = new Lease(1.Milliseconds());

            await lease.Expiration();

            Action extend = () => lease.ExpireIn(1.Seconds()).Wait();

            extend.ShouldThrow<InvalidOperationException>()
                  .And
                  .Message
                  .Should()
                  .Be("The lease cannot be extended.");
        }

        [Test]
        public async Task Lease_ExpireIn_does_not_accept_a_negative_timespan()
        {
            var lease = new Lease<string>(leasable,
                                          20.Milliseconds(),
                                          1);

            Action extend = () => lease.ExpireIn(-1.Seconds()).Wait();

            extend.ShouldThrow<ArgumentException>()
                  .And
                  .Message
                  .Should()
                  .Be("Lease cannot be extended by a negative timespan.");
        }

        [Test]
        public async Task IsReleased_returns_false_when_the_lease_has_not_expired()
        {
            var lease = new Lease(TimeSpan.FromSeconds(1));

            await Task.Delay(15);

            lease.IsReleased.Should().BeFalse();
        }

        [Test]
        public async Task IsReleased_returns_true_when_the_lease_has_expired()
        {
            var lease = new Lease(TimeSpan.FromMilliseconds(1));

            await Task.Delay(35);

            lease.IsReleased.Should().BeTrue();
        }
    }
}