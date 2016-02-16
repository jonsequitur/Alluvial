using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FluentAssertions;
using Its.Log.Instrumentation;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Distributors;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public abstract class DistributorTests
    {
        protected abstract IDistributor<int> CreateDistributor(
            Func<Lease<int>, Task> onReceive = null,
            Leasable<int>[] leasables = null,
            int maxDegreesOfParallelism = 5,
            [CallerMemberName] string name = null,
            TimeSpan? waitInterval = null,
            string pool = null);

        protected abstract TimeSpan DefaultLeaseDuration { get; }

        protected abstract TimeSpan ClockDriftTolerance { get; }

        protected Leasable<int>[] DefaultLeasables;

        [SetUp]
        public void SetUp()
        {
            DefaultLeasables = Enumerable.Range(1, 10)
                                        .Select(i => new Leasable<int>(i, i.ToString()))
                                        .ToArray();
        }

        [Test]
        public async Task When_the_distributor_is_started_then_notifications_begin()
        {
            var mre = new AsyncManualResetEvent();

            var distributor = CreateDistributor(async lease => { mre.Set(); });

            await distributor.Start();
            await mre.WaitAsync().Timeout();
            await distributor.Stop();

            // no TimeoutException, success!
        }

        [Test]
        public async Task No_further_acquisitions_occur_after_Stop_is_called()
        {
            var received = 0;
            var mre = new AsyncManualResetEvent();

            var distributor = CreateDistributor(async lease =>
            {
                await Task.Delay(1);
                Interlocked.Increment(ref received);
                mre.Set();
            }, waitInterval: TimeSpan.FromMilliseconds(DefaultLeaseDuration.TotalMilliseconds*4));

            await distributor.Start();
            await mre.WaitAsync().Timeout();
            await distributor.Stop();

            var receivedAsOfStop = received;

            await Task.Delay(((int) DefaultLeaseDuration.TotalMilliseconds*4));

            received.Should().Be(receivedAsOfStop);
        }

        [Test]
        public async Task Any_given_lease_is_never_handed_out_to_more_than_one_handler_at_a_time()
        {
            var random = new Random();
            var currentlyGranted = new HashSet<string>();
            var everGranted = new HashSet<string>();
            var leasedConcurrently = "";
            var distributor = CreateDistributor(waitInterval: TimeSpan.FromSeconds(5)).Trace();
            var countDown = new AsyncCountdownEvent(10);

            distributor.OnReceive(async lease =>
            {
                lock (currentlyGranted)
                {
                    if (currentlyGranted.Contains(lease.ResourceName))
                    {
                        leasedConcurrently = lease.ResourceName;
                    }

                    currentlyGranted.Add(lease.ResourceName);
                    everGranted.Add(lease.ResourceName);
                }

                await Task.Delay((int) (1000*random.NextDouble()));

                lock (currentlyGranted)
                {
                    currentlyGranted.Remove(lease.ResourceName);
                }

                countDown.Signal();
            });

            Enumerable.Range(1, 10).ToList().ForEach(_ => { distributor.Distribute(1); });
            await countDown.WaitAsync().Timeout();

            leasedConcurrently.Should().BeEmpty();
            everGranted.Count.Should().Be(10);
        }

        [Test]
        public async Task The_least_recently_released_lease_is_granted_next()
        {
            foreach (var resource in DefaultLeasables)
            {
                resource.LeaseLastGranted = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2));
                resource.LeaseLastReleased = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2));
            }

            var stalestLease = DefaultLeasables.Single(l => l.Name == "5");
            stalestLease.LeaseLastGranted = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2.1));
            stalestLease.LeaseLastReleased = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(2.1));

            var distributor = CreateDistributor().Trace();

            Lease<int> receivedLease = null;

            distributor.OnReceive(async lease => { receivedLease = lease; });

            await distributor.Distribute(1);

            receivedLease.ResourceName.Should().Be("5");
        }

        [Test]
        public async Task When_receiver_throws_then_work_distribution_continues()
        {
            var received = 0;
            var distributor = CreateDistributor(waitInterval: TimeSpan.FromMilliseconds(100)).Trace();
            var countdown = new AsyncCountdownEvent(20);

            distributor.OnReceive(async lease =>
            {
                Interlocked.Increment(ref received);

                if (received < 10)
                {
                    throw new Exception("dangit!");
                }

                countdown.Signal();
            });

            await distributor.Start();
            await countdown.WaitAsync().Timeout();
            await distributor.Stop();

            received.Should().BeGreaterOrEqualTo(20);
        }

        [Ignore("Test not finished")]
        [Test]
        public async Task When_receiver_throws_then_the_exception_can_be_observed()
        {
            // FIX (When_receiver_throws_then_the_exception_can_be_observed) write test
            Assert.Fail("Test not written yet.");
        }

        [Test]
        public async Task A_wait_interval_can_be_specified_before_which_a_released_lease_will_be_granted_again()
        {
            var tally = new ConcurrentDictionary<string, int>();
            var distributor = CreateDistributor(waitInterval: TimeSpan.FromMilliseconds(5000)).Trace();
            var countdown = new AsyncCountdownEvent(10);

            distributor.OnReceive(async lease =>
            {
                tally.AddOrUpdate(lease.ResourceName,
                                  addValueFactory: s => 1,
                                  updateValueFactory: (s, v) => v + 1);
                countdown.Signal();
            });

            await distributor.Start();
            await countdown.WaitAsync().Timeout();
            await distributor.Stop();

            tally.Count.Should().Be(10);
            tally.Should().ContainKeys("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
            tally.Should().ContainValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        }

        [Test]
        public async Task When_a_lease_expires_because_the_recipient_took_too_long_then_it_is_leased_out_again()
        {
            var blocked = false;
            var receiveCount = 0;
            var mre = new AsyncManualResetEvent();
            var distributor = CreateDistributor(
                leasables: DefaultLeasables.Take(1).ToArray())
                .Trace();

            distributor.OnReceive(async lease =>
            {
                if (!blocked)
                {
                    blocked = true;
                    await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*3));
                }

                Interlocked.Increment(ref receiveCount);

                mre.Set();
            });

            await distributor.Start();
            await mre.WaitAsync().Timeout();
            await distributor.Stop();

            receiveCount.Should().Be(1);
        }

        [Test]
        public virtual async Task A_lease_can_be_extended()
        {
            var tally = new ConcurrentDictionary<string, int>();
            var pool = DateTimeOffset.UtcNow.Ticks.ToString();
            var distributor1 = CreateDistributor(pool: pool).Trace();
            var distributor2 = CreateDistributor(pool: pool).Trace();

            Func<Lease<int>, Task> onReceive = async lease =>
            {
                tally.AddOrUpdate(lease.ResourceName,
                                  addValueFactory: s => 1,
                                  updateValueFactory: (s, v) => v + 1);

                if (lease.ResourceName == "5")
                {
                    // extend the lease
                    await lease.Extend(TimeSpan.FromDays(2));

                    // wait longer than the lease would normally last
                    await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*5));
                }
            };

            distributor1.OnReceive(onReceive);
            distributor2.OnReceive(onReceive);
            await distributor1.Start();
            await distributor2.Start();
            await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*2.5));
            await distributor1.Stop();
            await distributor2.Stop();

            Console.WriteLine(tally.ToLogString());

            tally.Should()
                 .ContainKey("5")
                 .And
                 .Subject["5"].Should().Be(1);
        }

        [Test]
        public virtual async Task When_Extend_is_called_after_a_lease_has_expired_then_it_throws()
        {
            Exception exception = null;
            var distributor = CreateDistributor().Trace();
            var mre = new AsyncManualResetEvent();

            distributor.OnReceive(async lease =>
            {
                // wait too long, until another receiver gets the lease
                await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*1.5));

                // now try to extend the lease
                try
                {
                    await lease.Extend(TimeSpan.FromMilliseconds(1));
                }
                catch (Exception ex)
                {
                    Console.WriteLine("CAUGHT " + ex);
                    exception = ex;
                }

                mre.Set();
            });

#pragma warning disable 4014
            distributor.Distribute(1);
#pragma warning restore 4014
            await mre.WaitAsync().Timeout();
            await Task.Delay(1000);

            exception.Should().BeOfType<InvalidOperationException>();
            exception.Message.Should().Contain("lease cannot be extended");
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
                 .Contain("You must call OnReceive before starting the distributor");
        }

        [Test]
        public async Task When_Distribute_is_called_before_OnReceive_it_throws()
        {
            var distributor = CreateDistributor();

            Action distribute = () => distributor.Distribute(1).Wait();

            distribute.ShouldThrow<InvalidOperationException>()
                      .And
                      .Message
                      .Should()
                      .Contain("You must call OnReceive");
        }

        [Test]
        public async Task Unless_work_is_completed_then_lease_is_not_reissued_before_its_duration_has_passed()
        {
            var leasesGranted = new ConcurrentBag<string>();

            var distributor = CreateDistributor(async l =>
            {
                Console.WriteLine("GRANTED: " + l);
                leasesGranted.Add(l.ResourceName);

                if (l.ResourceName == "2")
                {
                    await Task.Delay(((int) DefaultLeaseDuration.TotalMilliseconds*6));
                }
            });

            await distributor.Start();
            await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*.5));
            await distributor.Stop();
            await Task.Delay(100);

            leasesGranted.Should().ContainSingle(l => l == "2");
        }

        [Test]
        public async Task The_received_lease_LastGranted_property_returns_the_time_of_the_previous_grant()
        {
            Lease<int> lease = null;
            var lastGranted = DefaultLeasables.Select(l => l.LeaseLastGranted).Distinct().Single();

            var distributor = CreateDistributor(async l =>
            {
                lease = l;
            });

            await distributor.Distribute(1);

            lease.LastGranted
                 .ToUniversalTime()
                 .Should()
                 .BeCloseTo(lastGranted,
                            precision: (int) ClockDriftTolerance.TotalMilliseconds);
        }

        [Test]
        public async Task The_received_lease_LastReleased_property_returns_the_time_of_the_previous_release()
        {
            Lease<int> lease = null;
            var lastReleased = DefaultLeasables.Select(l => l.LeaseLastReleased).Distinct().Single();
            var distributor = CreateDistributor(async l => { lease = l; });

            await distributor.Distribute(1);

            lease.LastReleased
                 .ToUniversalTime()
                 .Should()
                 .BeCloseTo(lastReleased,
                            precision: (int) ClockDriftTolerance.TotalMilliseconds);
        }

        [Test]
        public async Task Distribute_returns_a_sequence_containing_the_leases_granted()
        {
            var received = new ConcurrentBag<int>();

            var distributor = CreateDistributor(async l => { received.Add(l.Resource); });

            var returned = (await distributor.Distribute(3)).ToArray();

            returned.Length.Should().Be(3);

            foreach (var leasable in received)
            {
                returned.Should().Contain(leasable);
            }
        }

        [Test]
        public async Task Distribute_completes_only_after_the_requested_number_of_leases_has_been_acquired()
        {
            var acquireCount = 0;
            var distributor = CreateDistributor(
                onReceive: async lease =>
                {
                    Interlocked.Increment(ref acquireCount);
                },
                leasables: new[]
                {
                    new Leasable<int>(1, "one")
                    {
                        LeaseLastGranted = DateTimeOffset.UtcNow,
                        LeaseLastReleased = DateTimeOffset.UtcNow.Add(DefaultLeaseDuration),
                    }
                }, maxDegreesOfParallelism: 1);

            await distributor.Distribute(6);

            acquireCount.Should().Be(6);
        }

        [Test]
        public async Task Distributor_rate_can_be_slowed()
        {
            var receivedLeases = new ConcurrentBag<Lease<int>>();

            var distributor = CreateDistributor(
                async lease =>
                {
                    receivedLeases.Add(lease);
                    await lease.Extend(2.Seconds());
                    await Task.Delay(2.Seconds());
                },
                waitInterval: TimeSpan.FromSeconds(.1),
                maxDegreesOfParallelism: 10);

            await distributor.Start();
            await Task.Delay(1.Seconds());
            await distributor.Stop();

            receivedLeases.Count().Should().Be(10);
        }
    }
}