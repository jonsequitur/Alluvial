using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FluentAssertions;
using Its.Log.Instrumentation;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
            string pool = null,
            TimeSpan? defaultLeaseDuration = null);

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
        public async Task When_Stop_has_been_called_then_Distributor_can_be_resumed_using_Start()
        {
            var distributor = CreateDistributor(maxDegreesOfParallelism: 10)
                .ReleaseLeasesWhenWorkIsDone()
                .Trace();

            distributor.OnReceive(async lease => { });

            await distributor.Start();
            await Task.Delay(20);
            await distributor.Stop();

            Console.WriteLine("\n\n\n STOPPED \n\n\n");

            var wasCalled = false;
            distributor.OnReceive(async lease => wasCalled = true);

            await distributor.Start();
            await Task.Delay(20);
            await distributor.Stop();

            wasCalled.Should().BeTrue();
        }

        [Test]
        public async Task When_Stop_has_been_called_then_Distributor_can_be_resumed_using_Distribute()
        {
            var distributor = CreateDistributor(maxDegreesOfParallelism: 1)
                .ReleaseLeasesWhenWorkIsDone()
                .Trace();

            distributor.OnReceive(async lease => { });

            await distributor.Start();
            await Task.Delay(20);
            await distributor.Stop();

            Console.WriteLine("\n\n\n STOPPED \n\n\n");

            await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*2));

            var wasCalled = false;
            distributor.OnReceive(async lease => wasCalled = true);

            await distributor.Distribute(1);
            await Task.Delay(DefaultLeaseDuration);
            await distributor.Stop();

            wasCalled.Should().BeTrue();
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
            });

            await distributor.Start();
            await mre.WaitAsync().Timeout();
            await distributor.Stop();

            var receivedAsOfStop = received;

            await Task.Delay((int) DefaultLeaseDuration.TotalMilliseconds*2);

            received.Should().Be(receivedAsOfStop);
        }

        [Test]
        public async Task Any_given_lease_is_never_handed_out_to_more_than_one_handler_at_a_time()
        {
            var random = new Random();
            var currentlyGranted = new HashSet<string>();
            var everGranted = new HashSet<string>();
            var leasedConcurrently = "";
            var distributor = CreateDistributor().Trace();
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

            var distributor = CreateDistributor(pool: Guid.NewGuid().ToString()).Trace();

            Lease<int> receivedLease = null;

            distributor.OnReceive(async lease => { receivedLease = lease; });

            await distributor.Distribute(1);

            receivedLease.ResourceName.Should().Be("5");
        }

        [Test]
        public async Task When_receiver_throws_then_work_distribution_continues()
        {
            var received = 0;
            var distributor = CreateDistributor(defaultLeaseDuration: 1.Seconds()).Trace().ReleaseLeasesWhenWorkIsDone();
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
                    await Task.Delay((int) (DefaultLeaseDuration.TotalMilliseconds*4));
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
        public virtual async Task A_lease_can_be_extended_using_ExpireIn()
        {
            var tally = new ConcurrentDictionary<string, int>();
            var pool = DateTimeOffset.UtcNow.Ticks.ToString();
            var distributor1 = CreateDistributor(pool: pool)
                .ReleaseLeasesWhenWorkIsDone()
                .Trace();
            var distributor2 = CreateDistributor(pool: pool)
                .ReleaseLeasesWhenWorkIsDone()
                .Trace();

            Func<Lease<int>, Task> onReceive = async lease =>
            {
                tally.AddOrUpdate(lease.ResourceName,
                                  addValueFactory: s => 1,
                                  updateValueFactory: (s, v) => v + 1);

                if (lease.ResourceName == "5")
                {
                    Console.WriteLine($"GOT LEASE 5 @ {DateTime.Now}");

                    // extend the lease
                    await lease.ExpireIn(TimeSpan.FromHours(2));

                    // wait longer than the lease would normally last
                    await Task.Delay(5.Seconds());

                    Console.WriteLine($"DONE LEASE 5@ {DateTime.Now}");
                }
            };

            distributor1.OnReceive(onReceive);
            distributor2.OnReceive(onReceive);
            await Task.WhenAll(
                distributor1.Start(),
                distributor2.Start());

            await Task.Delay(2.Seconds());

            await Task.WhenAll(distributor1.Stop(),
                distributor2.Stop());

            Console.WriteLine(tally.ToLogString());

            tally.Should()
                 .ContainKey("5")
                 .And
                 .Subject["5"].Should().Be(1);
        }

        [Test]
        public async Task A_lease_can_be_shortened_using_ExpireIn()
        {
            var receivedCount = 0;

            var distributor = CreateDistributor(
                defaultLeaseDuration: 1.Hours()).Trace();

            distributor.OnReceive(async lease =>
            {
                Interlocked.Increment(ref receivedCount);
                await lease.ExpireIn(2.Milliseconds());
            });

            var numberOfLeases = DefaultLeasables.Length;

            await distributor.Distribute(numberOfLeases);
            await distributor.Distribute(numberOfLeases)
                .Timeout(5.Seconds());

            Console.WriteLine(new {receivedCount});

            receivedCount.Should().Be(numberOfLeases * 2);
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
            var lastGranted = DateTimeOffset.Parse("8/10/2016");

            var distributor = CreateDistributor(
                async l => lease = l,
                new[]
                {
                    new Leasable<int>(1, "the only lease in the pool")
                    {
                        LeaseLastGranted = lastGranted
                    }
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
            var lastReleased = DateTimeOffset.Parse("8/10/2016");

            var distributor = CreateDistributor(
                async l => lease = l,
                new[]
                {
                    new Leasable<int>(1, "the only lease in the pool")
                    {
                        LeaseLastReleased = lastReleased
                    }
                });

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
        public async Task Distributor_rate_can_be_slowed_by_extending_leases_using_ExpireIn()
        {
            var receivedLeases = new ConcurrentBag<Lease<int>>();

            var distributor = CreateDistributor(
                async lease => receivedLeases.Add(lease),
                maxDegreesOfParallelism: 10);

            distributor.OnReceive(async lease =>
            {
                await lease.ExpireIn(2.Seconds());
            });

            await distributor.Start();
            await Task.Delay(1.Seconds());
            await distributor.Stop();

            receivedLeases.Count().Should().Be(10);
        }

        [Test]
        public async Task Distribute_will_not_distribute_more_leases_than_there_are_leasables()
        {
            var leasesDistributed = 0;

            var distributor = CreateDistributor(async lease =>
            {
                Interlocked.Increment(ref leasesDistributed);
            });

            await distributor.Distribute(10000000);

            leasesDistributed.Should().Be(DefaultLeasables.Length);
        }

        [Test]
        public async Task A_lease_can_be_continuously_extended_as_work_is_being_done_using_KeepExtendingLeasesWhileWorking()
        {
            var distributor = CreateDistributor(defaultLeaseDuration: 1000.Milliseconds());
            bool? wasReleased = null;

            distributor.OnReceive(async (lease, next) =>
            {
                await Task.Delay(1500.Milliseconds());
                wasReleased = lease.IsReleased;
            });

            distributor = distributor
                .KeepExtendingLeasesWhileWorking(frequency: 600.Milliseconds())
                .ReleaseLeasesWhenWorkIsDone();

            await distributor.Distribute(1);

            wasReleased.Should().BeFalse();
        }

        [Test]
        public async Task When_a_lease_has_been_released_and_ExpireIn_is_called_then_OnException_publishes_an_exception()
        {
            // arrange
            var distributor = CreateDistributor(async lease =>
            {
                // trigger a lease expiration exception
                await lease.Release();
                await lease.ExpireIn(1.Milliseconds());
            });

            Exception handledException = null;

            distributor.OnException((exception, lease) => { handledException = exception; });

            // act
            await distributor.Distribute(1);

            await Task.Delay(10);

            // assert
            handledException.Should().NotBeNull();
        }

        [Test]
        public async Task When_a_lease_has_expired_and_ExpireIn_is_called_then_OnException_publishes_an_exception()
        {
            // arrange
            var distributor = CreateDistributor(async lease =>
            {
                // trigger a lease expiration exception
                await lease.Expiration();
                await lease.ExpireIn(1.Milliseconds());
            });

            Exception handledException = null;

            distributor.OnException((exception, lease) => { handledException = exception; });

            // act
            await distributor.Distribute(1);

            await Task.Delay(10);

            // assert
            handledException.Should().NotBeNull();
        }
    }
}