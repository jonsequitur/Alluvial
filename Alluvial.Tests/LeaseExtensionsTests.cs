using System;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class LeaseExtensionsTests
    {
        [Test]
        public async Task KeepAlive_retries_extending_the_expiration_when_there_is_a_transient_exception()
        {
            // arrange
            var firstAttempt = true;

            var lease = new Lease(
                100.Milliseconds(),
                expireIn: async _ =>
                {
                    if (firstAttempt)
                    {
                        firstAttempt = false;
                        throw new TimeoutException("oops!");
                    }
                    return _;
                });

            // act
            using (lease.KeepAlive(50.Milliseconds()))
            {
                await Task.Delay(200.Milliseconds());

                // assert
                lease.IsReleased.Should().BeFalse();
            }
        }

        [Test]
        public async Task KeepAlive_does_not_retry_when_lease_is_not_eligible_for_extension()
        {
            // arrange
            var lease = new Lease(1.Seconds());

            // act
            using (lease.KeepAlive(10.Milliseconds()))
            {
                await lease.Release();
                await Task.Delay(30.Milliseconds());

                // assert
                lease.Exception.Should().NotBeNull();
                lease.Exception.Message.Should().Be("The lease cannot be extended.");
            }
        }
    }
}