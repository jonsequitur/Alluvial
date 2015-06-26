using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FluentAssertions;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public abstract class StreamQueryDistributorTests
    {
        protected abstract IStreamQueryDistributor CreateDistributor(
            Lease[] leases,
            Func<DistributorUnitOfWork, Task> onReceive = null,
            int maxDegreesOfParallelism = 5,
            [CallerMemberName] string name = null,
            TimeSpan? waitInterval = null);

        protected abstract TimeSpan DefaultLeaseDuration { get; }

        protected Lease[] leases;

        [SetUp]
        public void SetUp()
        {
            leases = Enumerable.Range(1, 10)
                               .Select(i => new Lease(i.ToString(), DefaultLeaseDuration))
                               .ToArray();
        }

        [Test]
        public async Task When_the_distributor_is_started_then_notifications_begin()
        {
            var received = false;
            var distributor = CreateDistributor(leases, async s => { received = true; });

            received.Should().BeFalse();

            await distributor.Start();
            await distributor.Stop();

            received.Should().BeTrue();
        }

        [Test]
        public async Task No_further_acquisitions_occur_after_Dispose_is_called()
        {
            var received = 0;
            var distributor = CreateDistributor(leases, async s => { Interlocked.Increment(ref received); });

            await distributor.Start();
            await distributor.Stop();

            var receivedAsOfStop = received;

            await Task.Delay(((int) DefaultLeaseDuration.TotalMilliseconds*3));

            received.Should().Be(receivedAsOfStop);
        }

        [Test]
        public async Task Any_given_lease_is_never_handed_out_to_more_than_one_handler_at_a_time()
        {
            var random = new Random();
            var currentlyGranted = new HashSet<string>();
            var everGranted = new HashSet<string>();
            var fail = false;
            var distributor = CreateDistributor(leases, maxDegreesOfParallelism: 5).Trace();

            distributor.OnReceive(async s =>
            {
                if (currentlyGranted.Contains(s.Lease.Name))
                {
                    fail = true;
                }

                currentlyGranted.Add(s.Lease.Name);
                everGranted.Add(s.Lease.Name);

                await Task.Delay((int) (1000*random.NextDouble()));

                currentlyGranted.Remove(s.Lease.Name);
            });

            await distributor.Start();
            await Task.Delay(((int) DefaultLeaseDuration.TotalMilliseconds*3));
            await distributor.Stop();

            fail.Should().BeFalse();
            everGranted.Count.Should().BeGreaterOrEqualTo(5);
        }

        [Test]
        public async Task The_least_recently_released_lease_is_granted_next()
        {
            foreach (var lease in leases)
            {
                lease.LastGranted = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2));
                lease.LastReleased = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2));
            }

            var stalestLease = leases.Single(l => l.Name == "5");
            stalestLease.LastGranted = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2.1));
            stalestLease.LastReleased = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2.1));

            var distributor = CreateDistributor(leases, maxDegreesOfParallelism: 1, waitInterval: TimeSpan.FromMinutes(1));

            distributor.OnReceive(async w =>
            {
            });

            await distributor.Start();
            await distributor.Stop();

            stalestLease.LastReleased.Should().BeCloseTo(DateTimeOffset.UtcNow);
        }

        [Test]
        public async Task When_receiver_throws_then_work_distribution_continues()
        {
            var failed = 0;
            var received = 0;
            var distributor = CreateDistributor(leases).Trace();
            distributor.OnReceive(async s =>
            {
                Interlocked.Increment(ref received);
                if (received < 10)
                {
                    throw new Exception("dangit!");
                }
            });

            await distributor.Start();

            await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds * 2));

            await distributor.Stop();

            received.Should().BeGreaterThan(20);
        }

        [Test]
        public async Task OnReceive_can_only_be_called_once()
        {
            var distributor = CreateDistributor(leases, async l => { });
            Action callOnReceiveAgain = () => distributor.OnReceive(async l => { });

            callOnReceiveAgain.ShouldThrow<InvalidOperationException>()
                              .And.Message.Should().Be("OnReceive has already been called. It can only be called once per distributor.");
        }

        [Test]
        public async Task Unless_work_is_completed_then_lease_is_not_reissued_before_its_duration_has_passed()
        {
            var leasesGranted = new ConcurrentBag<string>();

            var distributor = CreateDistributor(leases, async l =>
            {
                leasesGranted.Add(l.Lease.Name);

                if (l.Lease.Name == "1")
                {
                    await Task.Delay(((int) DefaultLeaseDuration.TotalMilliseconds*6));
                }
            });

            await distributor.Start();
            await Task.Delay(((int) DefaultLeaseDuration.TotalMilliseconds*3));
            await distributor.Stop();

            leasesGranted.Should().ContainSingle(l => l == "1");
        }

        [Test]
        public async Task Leases_record_the_time_when_they_were_last_granted()
        {
            Lease lease = null;
            var received = default (DateTimeOffset);
            var distributor = CreateDistributor(leases, async l =>
            {
                received = DateTimeOffset.UtcNow;
                lease = l.Lease;
            });

            await distributor.Start();
            await distributor.Stop();

            lease.LastGranted
                 .Should()
                 .BeCloseTo(received);
        }

        [Test]
        public async Task Leases_record_the_time_when_they_were_last_released()
        {
            Lease lease = null;
            var received = default (DateTimeOffset);
            var distributor = CreateDistributor(leases, async l =>
            {
                received = DateTimeOffset.UtcNow;
                lease = l.Lease;
            });

            await distributor.Start();
            await distributor.Stop();

            lease.LastReleased
                 .Should()
                 .BeCloseTo(received);
        }
    }
}