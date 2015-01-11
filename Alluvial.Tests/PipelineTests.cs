using System;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class PipelineTests
    {
        [Test]
        public async Task An_aggregator_can_be_short_circuited_using_Before_and_not_calling_next()
        {
            var wasCalled = false;

            var aggregator = Aggregator.Create<BalanceProjection, IDomainEvent>((projection, events) => { wasCalled = true; })
                                       .Pipeline((projection, events, next) => { });

            aggregator.Aggregate(null, null);

            wasCalled.Should().BeFalse();
        }

        [Test]
        public async Task An_aggregator_can_be_short_circuited_using_Before_and_returning_rather_than_calling_next()
        {
            var wasCalled = false;

            var aggregator = Aggregator.Create<BalanceProjection, IDomainEvent>((projection, events) => { wasCalled = true; })
                                       .Pipeline((projection, events, next) => projection);

            var balanceProjection = new BalanceProjection();
            var returnedProjection = aggregator.Aggregate(balanceProjection, null);

            wasCalled.Should().BeFalse();
            balanceProjection.Should().BeSameAs(returnedProjection);
        }

        [Test]
        public async Task A_pipeline_can_be_used_to_continue_on_exceptions()
        {
            var aggregator = Aggregator
                .Create<BalanceProjection, IDomainEvent>((projection, events) => { throw new Exception("DRAT!"); })
                .Pipeline((projection, events, next) =>
                {
                    try
                    {
                        return next(projection, events);
                    }
                    catch (Exception)
                    {
                        return projection;
                    }
                });

            var balanceProjection = new BalanceProjection();
            var returnedProjection = aggregator.Aggregate(balanceProjection, null);

            balanceProjection.Should().BeSameAs(returnedProjection);
        }
    }
}