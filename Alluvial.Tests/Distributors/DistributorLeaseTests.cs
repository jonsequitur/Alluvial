using System;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class DistributorLeaseTests
    {
        [Test]
        public async Task If_a_lease_is_currently_granted_and_within_duration_then_it_is_not_expired()
        {
            var lease = new DistributorLease("1", TimeSpan.FromMinutes(1));

            var now = DateTimeOffset.Now;

            lease.LastGranted = now;
            lease.LastReleased = now - TimeSpan.FromMinutes(.5);

            lease.IsExpired(now).Should().BeFalse();
        }
        
        [Test]
        public async Task If_a_lease_is_currently_granted_but_past_duration_then_it_is_expired()
        {
            var lease = new DistributorLease("1", TimeSpan.FromMinutes(1));

            var now = DateTimeOffset.Now;

            lease.LastGranted = now;

            lease.IsExpired(now + TimeSpan.FromMinutes(1.1)).Should().BeTrue();
        }
        
        [Test]
        public async Task If_a_lease_is_not_currently_granted_then_it_is_not_expired()
        {
            var lease = new DistributorLease("1", TimeSpan.FromMinutes(1));

            var now = DateTimeOffset.Now;

            lease.LastGranted = now;
            lease.LastReleased = now + TimeSpan.FromMinutes(.5);

            lease.IsExpired(now).Should().BeFalse();
        }
        
    }
}