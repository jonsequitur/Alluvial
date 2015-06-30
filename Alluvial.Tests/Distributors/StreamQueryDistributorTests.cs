using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FluentAssertions;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Distributors;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public abstract class StreamQueryDistributorTests
    {
        protected abstract IStreamQueryDistributor CreateDistributor(
            Func<Lease, Task> onReceive = null,
            LeasableResource[] LeasablesResource = null, int maxDegreesOfParallelism = 5,
            [CallerMemberName] string name = null,
            TimeSpan? waitInterval = null);

        protected abstract TimeSpan DefaultLeaseDuration { get; }

        protected LeasableResource[] DefaultLeasableResources;

        [SetUp]
        public void SetUp()
        {
            DefaultLeasableResources = Enumerable.Range(1, 10)
                                      .Select(i => new LeasableResource(i.ToString(), DefaultLeaseDuration))
                                      .ToArray();
        }

        [Test]
        public async Task When_the_distributor_is_started_then_notifications_begin()
        {
            var received = false;
            var distributor = CreateDistributor(async s => { received = true; });

            received.Should().BeFalse();

            await distributor.Start();
            await distributor.Stop();

            received.Should().BeTrue();
        }

        [Test]
        public async Task No_further_acquisitions_occur_after_Dispose_is_called()
        {
            var received = 0;
            var distributor = CreateDistributor(async s => { Interlocked.Increment(ref received); });

            await distributor.Start();
            Console.WriteLine("Stopping...");
            await distributor.Stop();
            await Task.Delay(10);

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
            var distributor = CreateDistributor(maxDegreesOfParallelism: 5).Trace();

            distributor.OnReceive(async s =>
            {
                if (currentlyGranted.Contains(s.LeasableResource.Name))
                {
                    fail = true;
                }

                currentlyGranted.Add(s.LeasableResource.Name);
                everGranted.Add(s.LeasableResource.Name);

                await Task.Delay((int) (1000*random.NextDouble()));

                currentlyGranted.Remove(s.LeasableResource.Name);
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
            foreach (var lease in DefaultLeasableResources)
            {
                lease.LastGranted = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2));
                lease.LastReleased = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2));
            }

            var stalestLease = DefaultLeasableResources.Single(l => l.Name == "5");
            stalestLease.LastGranted = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2.1));
            stalestLease.LastReleased = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2.1));

            var distributor = CreateDistributor(maxDegreesOfParallelism: 1, waitInterval: TimeSpan.FromMinutes(1));

            distributor.OnReceive(async w => { });

            await distributor.Start();
            await distributor.Stop();

            stalestLease.LastReleased.Should().BeCloseTo(DateTimeOffset.UtcNow);
        }

        [Test]
        public async Task When_receiver_throws_then_work_distribution_continues()
        {
            var failed = 0;
            var received = 0;
            var distributor = CreateDistributor(waitInterval: TimeSpan.FromMilliseconds(10)).Trace();
            distributor.OnReceive(async s =>
            {
                Interlocked.Increment(ref received);
                if (received < 10)
                {
                    throw new Exception("dangit!");
                }
            });

            await distributor.Start();

            await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*2));

            await distributor.Stop();

            received.Should().BeGreaterThan(20);
        }

        [Ignore("Test not finished")]
        [Test]
        public async Task When_receiver_throws_then_the_exception_can_be_observed()
        {
            // FIX (When_receiver_throws_then_the_exception_can_be_observed) write test
            Assert.Fail("Test not written yet.");
        }

        [Test]
        public async Task An_interval_can_be_specified_before_which_a_released_lease_will_be_granted_again()
        {
            var tally = new ConcurrentDictionary<string, int>();
            var distributor = CreateDistributor(waitInterval: TimeSpan.FromMilliseconds(500)).Trace();

            distributor.OnReceive(async w =>
            {
                tally.AddOrUpdate(w.LeasableResource.Name,
                                  addValueFactory: s => 1,
                                  updateValueFactory: (s, v) => v + 1);
            });

            await distributor.Start();

            await Task.Delay(100);

            await distributor.Stop();

            tally.Count.Should().Be(10);
            tally.Should().ContainKeys("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
            tally.Should().ContainValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        }

        [Test]
        public async Task When_a_lease_expires_because_the_recipient_took_too_long_then_it_is_leased_out_again()
        {
            var blocked = false;
            var tally = new ConcurrentDictionary<string, int>();
            var distributor = CreateDistributor().Trace();
            distributor.OnReceive(async w =>
            {
                if (w.LeasableResource.Name == "5" && !blocked)
                {
                    blocked = true;
                    await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*1.5));
                }

                tally.AddOrUpdate(w.LeasableResource.Name,
                                  addValueFactory: s => 1,
                                  updateValueFactory: (s, v) => v + 1);
            });

            await distributor.Start();

            await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*2));

            await distributor.Stop();
            tally.Should().ContainKey("5")
                 .And
                 .Subject["5"].Should().Be(2);

            new[] { "1", "2", "3", "4", "6", "7", "8", "9", "10" }
                .ToList()
                .ForEach(lease => { tally[lease].Should().BeGreaterThan(1); });

        }

        [Test]
        public async Task A_lease_can_be_extended()
        {
            var blocked = false;
            var tally = new ConcurrentDictionary<string, int>();
            var distributor = CreateDistributor().Trace();
            distributor.OnReceive(async w =>
            {
                if (w.LeasableResource.Name == "5" && !blocked)
                {
                    blocked = true;
                    await w.Extend(TimeSpan.FromMilliseconds((int) (DefaultLeaseDuration.TotalMilliseconds*5)));
                    await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*3));
                }

                tally.AddOrUpdate(w.LeasableResource.Name,
                                  addValueFactory: s => 1,
                                  updateValueFactory: (s, v) => v + 1);
            });

            await distributor.Start();

            await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*4));

            await distributor.Stop();

            tally.Should().ContainKey("5")
                 .And
                 .Subject["5"].Should().Be(1);
        }

        [Test]
        public async Task OnReceive_can_only_be_called_once()
        {
            var distributor = CreateDistributor(async l => { });
            Action callOnReceiveAgain = () => distributor.OnReceive(async l => { });

            callOnReceiveAgain.ShouldThrow<InvalidOperationException>()
                              .And.Message.Should().Be("OnReceive has already been called. It can only be called once per distributor.");
        }

        [Test]
        public async Task When_Start_is_called_before_OnReceive_it_throws()
        {
            var distributor = CreateDistributor();

            Action start = () => distributor.Start().Wait();

            start.ShouldThrow<InvalidOperationException>()
                 .And
                 .Message
                 .Should()
                 .Contain("call OnReceive before calling Start");
        }

        [Test]
        public async Task Unless_work_is_completed_then_lease_is_not_reissued_before_its_duration_has_passed()
        {
            var leasesGranted = new ConcurrentBag<string>();

            var distributor = CreateDistributor(async l =>
            {
                Console.WriteLine("GRANTED: " + l);
                leasesGranted.Add(l.LeasableResource.Name);

                if (l.LeasableResource.Name == "2")
                {
                    await Task.Delay(((int) DefaultLeaseDuration.TotalMilliseconds*6));
                }
            });

            await distributor.Start();
            await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*.5));
            await distributor.Stop();

            leasesGranted.Should().ContainSingle(l => l == "2");
        }

        [Test]
        public async Task Leases_record_the_time_when_they_were_last_granted()
        {
            LeasableResource leasableResource = null;
            var received = default (DateTimeOffset);
            var distributor = CreateDistributor(async l =>
            {
                received = DateTimeOffset.UtcNow;
                leasableResource = l.LeasableResource;
            });

            await distributor.Start();
            await distributor.Stop();

            leasableResource.LastGranted
                 .Should()
                 .BeCloseTo(received);
        }

        [Test]
        public async Task Leases_record_the_time_when_they_were_last_released()
        {
            LeasableResource leasableResource = null;
            var received = default (DateTimeOffset);
            var distributor = CreateDistributor(async l =>
            {
                received = DateTimeOffset.UtcNow;
                leasableResource = l.LeasableResource;
            });

            await distributor.Start();
            await distributor.Stop();

            leasableResource.LastReleased
                 .Should()
                 .BeCloseTo(received);
        }
    }
}