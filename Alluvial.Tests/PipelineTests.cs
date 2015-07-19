using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using FluentAssertions;
using Its.Log.Instrumentation;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class PipelineTests
    {
        [Test]
        public async Task An_aggregator_can_be_short_circuited_using_Pipeline_and_not_calling_next()
        {
            var wasCalled = false;

            var aggregator = Aggregator.Create<BalanceProjection, IDomainEvent>((projection, events) =>
            {
                wasCalled = true;
            })
                                       .Pipeline(async (projection, events, next) =>
                                       {
                                       });

            aggregator.Aggregate(null, null);

            wasCalled.Should().BeFalse();
        }

        [Test]
        public async Task An_aggregator_can_be_short_circuited_using_Pipeline_and_returning_rather_than_calling_next()
        {
            var wasCalled = false;

            var aggregator = Aggregator.Create<BalanceProjection, IDomainEvent>((projection, events) => wasCalled = true)
                                       .Pipeline(async (projection, events, next) => projection);

            var balanceProjection = new BalanceProjection();
            var returnedProjection = await aggregator.Aggregate(balanceProjection, null);

            wasCalled.Should().BeFalse();
            balanceProjection.Should().BeSameAs(returnedProjection);
        }

        [Test]
        public async Task A_pipeline_can_be_used_to_continue_on_exceptions()
        {
            var aggregator = Aggregator
                .Create<BalanceProjection, IDomainEvent>((projection, events) =>
                {
                    Task.Run(() =>
                    {
                        throw new Exception("DRAT!");
                    });
                })
                .Pipeline(async (projection, events, next) =>
                {
                    try
                    {
                        return await next(projection, events);
                    }
                    catch (Exception)
                    {
                        return projection;
                    }
                });

            var balanceProjection = new BalanceProjection();
            var returnedProjection = await aggregator.Aggregate(balanceProjection, null);

            balanceProjection.Should().BeSameAs(returnedProjection);
        }

        [Test]
        public async Task Pipeline_can_be_used_to_time_an_operation()
        {
            var time = new TimeSpan();
            var aggregator = Aggregator
                .Create<BalanceProjection, IDomainEvent>((projection, events) =>
                {
                    return Task.Run(() => Thread.Sleep(1000));
                })
                .Pipeline(async (projection, batch, next) =>
                {
                    var stopwatch = new Stopwatch();
                    stopwatch.Start();
                    await next(projection, batch);
                    time = stopwatch.Elapsed;
                });

            await aggregator.Aggregate(null, null);

            time.Should().BeGreaterOrEqualTo(TimeSpan.FromSeconds(1));
        }

        [Test]
        public async Task Pipelines_affect_projections_in_reverse_order_relative_to_the_Pipeline_calls_when_next_is_called_after_modifying_the_projection()
        {
            var aggregator = Aggregator.Create<Projection<List<int>, string>, string>(async (projection, events) => projection.Value.Add(1))
                                       .Pipeline(async (projection, batch, next) =>
                                       {
                                           projection.Value.Add(2);
                                           await next(projection, batch);
                                           return projection;
                                       })
                                       .Pipeline(async (projection, batch, next) =>
                                       {
                                           projection.Value.Add(3);
                                           await next(projection, batch);
                                           return projection;
                                       });

            var result = await aggregator.Aggregate(Projection.Create(new List<int>(), ""), null);

            result.Value.Should().BeInDescendingOrder();
        }

        [Test]
        public async Task Pipelines_affect_projections_in_the_same_order_as_the_Pipeline_calls_when_next_is_called_before_modifying_the_projection()
        {
            var aggregator = Aggregator.Create<Projection<List<int>, string>, string>(async (projection, events) => projection.Value.Add(1))
                                       .Pipeline(async (projection, batch, next) =>
                                       {
                                           await next(projection, batch);
                                           projection.Value.Add(2);
                                           return projection;
                                       })
                                       .Pipeline(async (projection, batch, next) =>
                                       {
                                           await next(projection, batch);
                                           projection.Value.Add(3);
                                           return projection;
                                       });

            var result = await aggregator.Aggregate(Projection.Create(new List<int>(), ""), null);

            Console.WriteLine(result.ToLogString());

            result.Value.Should().BeInAscendingOrder();
        }
    }
}