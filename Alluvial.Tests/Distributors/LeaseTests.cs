using System;
using System.Threading.Tasks;
using Alluvial.Distributors;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class LeaseTests
    {
        [Test]
        public async Task If_a_lease_is_currently_granted_and_within_duration_then_it_is_not_expired()
        {
            var resource = new LeasableResource("1", TimeSpan.FromMinutes(1));

            var now = DateTimeOffset.Now;

            resource.LeaseLastGranted = now;
            resource.LeaseLastReleased = now - TimeSpan.FromMinutes(.5);

            resource.IsLeaseExpired(now).Should().BeFalse();
        }
        
        [Test]
        public async Task If_a_lease_is_currently_granted_but_past_duration_then_it_is_expired()
        {
            var resource = new LeasableResource("1", TimeSpan.FromMinutes(1));

            var now = DateTimeOffset.Now;

            resource.LeaseLastGranted = now;

            resource.IsLeaseExpired(now + TimeSpan.FromMinutes(1.1)).Should().BeTrue();
        }
        
        [Test]
        public async Task If_a_lease_is_not_currently_granted_then_it_is_not_expired()
        {
            var resource = new LeasableResource("1", TimeSpan.FromMinutes(1));

            var now = DateTimeOffset.Now;

            resource.LeaseLastGranted = now;
            resource.LeaseLastReleased = now + TimeSpan.FromMinutes(.5);

            resource.IsLeaseExpired(now).Should().BeFalse();
        }
        
    }
}