using System;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class CursorTests
    {
        [Test]
        public async Task DateTimeOffset_cursor_HasReached_with_ascending_sort()
        {
            var startAt = DateTimeOffset.Parse("2014-12-30 01:58:48 PM");

            var cursor = Cursor.Create(startAt);

            cursor.HasReached(startAt)
                  .Should()
                  .BeTrue();

            cursor.HasReached(startAt.Subtract(TimeSpan.FromMilliseconds(1)))
                  .Should()
                  .BeTrue();

            cursor.HasReached(startAt.Add(TimeSpan.FromMilliseconds(1)))
                  .Should()
                  .BeFalse();
        }

        [Test]
        public async Task DateTimeOffset_cursor_HasReached_with_descending_sort()
        {
            var startAt = DateTimeOffset.Parse("2014-12-30 01:58:48 PM");

            var cursor = Cursor.Create(startAt, ascending: false);

            cursor.HasReached(startAt)
                  .Should()
                  .BeTrue();

            cursor.HasReached(startAt.Subtract(TimeSpan.FromMilliseconds(1)))
                  .Should()
                  .BeFalse();

            cursor.HasReached(startAt.Add(TimeSpan.FromMilliseconds(1)))
                  .Should()
                  .BeTrue();
        }

        [Test]
        public async Task int_cursor_HasReached_with_ascending_sort()
        {
            var startAt = 123;

            var cursor = Cursor.Create(startAt);

            cursor.HasReached(startAt)
                  .Should()
                  .BeTrue();

            cursor.HasReached(122)
                  .Should()
                  .BeTrue();

            cursor.HasReached(124)
                  .Should()
                  .BeFalse();
        }

        [Test]
        public async Task int_cursor_HasReached_with_descending_sort()
        {
            var startAt = 123;

            var cursor = Cursor.Create(startAt, ascending: false);

            cursor.HasReached(startAt)
                  .Should()
                  .BeTrue();

            cursor.HasReached(122)
                  .Should()
                  .BeFalse();

            cursor.HasReached(124)
                  .Should()
                  .BeTrue();
        }

        [Test]
        public async Task string_cursor_HasReached_with_ascending_sort()
        {
            var cursor = Cursor.Create("j");

            cursor.HasReached("j")
                  .Should()
                  .BeTrue();

            cursor.HasReached("i")
                  .Should()
                  .BeTrue();

            cursor.HasReached("k")
                  .Should()
                  .BeFalse();
        }

        [Test]
        public async Task string_cursor_HasReached_with_descending_sort()
        {
            var cursor = Cursor.Create("j", ascending: false);

            cursor.HasReached("j")
                  .Should()
                  .BeTrue();

            cursor.HasReached("i")
                  .Should()
                  .BeFalse();

            cursor.HasReached("k")
                  .Should()
                  .BeTrue();
        }

        [Test]
        public async Task ascending_int_Cursor_position_after_AdvanceBy_is_correct()
        {
            var cursor = Cursor.Create(10, ascending: true) as IIncrementableCursor;

            cursor.AdvanceBy(5);

            ((int) cursor.Position).Should().Be(15);
        }

        [Test]
        public async Task descending_int_Cursor_position_after_AdvanceBy_is_correct()
        {
            var cursor = Cursor.Create(10, ascending: false) as IIncrementableCursor;

            cursor.AdvanceBy(5);

            ((int) cursor.Position).Should().Be(5);
        }

        [Test]
        public async Task ascending_int_Cursor_position_after_AdvanceTo_is_correct()
        {
            var cursor = Cursor.Create(10, ascending: true);

            cursor.AdvanceTo(100);

            ((int) cursor.Position).Should().Be(100);
        }

        [Test]
        public async Task descending_int_Cursor_position_after_AdvanceTo_is_correct()
        {
            var cursor = Cursor.Create(10, ascending: false);

            cursor.AdvanceTo(100);

            ((int) cursor.Position).Should().Be(100);
        }

        [Test]
        public async Task ascending_DateTimeOffset_Cursor_position_after_AdvanceBy_is_correct()
        {
            var cursor = Cursor.Create(DateTimeOffset.Parse("2015-01-01 12am +00:00"), ascending: true) as IIncrementableCursor;

            cursor.AdvanceBy(TimeSpan.FromDays(1));

            ((DateTimeOffset) cursor.Position).Should()
                                              .Be(DateTimeOffset.Parse("2015-01-02 12am +00:00"));
        }

        [Test]
        public async Task descending_DateTimeOffset_Cursor_position_after_AdvanceBy_is_correct()
        {
            var cursor = Cursor.Create(DateTimeOffset.Parse("2015-01-01 12am +00:00"), ascending: false) as IIncrementableCursor;

            cursor.AdvanceBy(TimeSpan.FromDays(1));

            ((DateTimeOffset) cursor.Position).Should()
                                              .Be(DateTimeOffset.Parse("2014-12-31 12am +00:00"));
        }

        [Test]
        public async Task ascending_DateTimeOffset_Cursor_position_after_AdvanceTo_is_correct()
        {
            var cursor = Cursor.Create(DateTimeOffset.Parse("2015-01-01 12am +00:00"), ascending: true);

            var expectedPosition = DateTimeOffset.Parse("2015-12-31 12am +00:00");
            cursor.AdvanceTo(expectedPosition);

            ((DateTimeOffset) cursor.Position).Should()
                                              .Be(expectedPosition);
        }

        [Test]
        public async Task descending_DateTimeOffset_Cursor_position_after_AdvanceTo_is_correct()
        {
            var cursor = Cursor.Create(DateTimeOffset.Parse("2015-01-01 12am +00:00"), ascending: false);

            var expectedPosition = DateTimeOffset.Parse("2014-12-31 12am +00:00");
            cursor.AdvanceTo(expectedPosition);

            ((DateTimeOffset) cursor.Position).Should()
                                              .Be(expectedPosition);
        }

        [Test]
        public async Task A_projection_implementing_ICursor_can_be_compared_to_a_cursor()
        {
            var projection = new BalanceProjection
            {
                CursorPosition = 5
            };

            Cursor.Create(5)
                  .HasReached(projection)
                  .Should()
                  .Be(true);
        }
    }
}