using System;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class CursorTests
    {
        [Test]
        public async Task A_new_cursor_has_a_position_that_is_less_than_any_int_value()
        {
            (Cursor.StartOfStream < int.MinValue).Should().BeTrue();
        }

        [Test]
        public async Task A_new_cursor_casts_to_int_as_MinValue_when_CompareTo_is_called()
        {
            int.MinValue.CompareTo(Cursor.StartOfStream).Should().Be(0);
        }

        [Test]
        public async Task A_new_cursor_has_a_position_that_is_less_than_any_long_value()
        {
            (Cursor.StartOfStream < long.MinValue).Should().BeTrue();
        }

        [Test]
        public async Task A_new_cursor_casts_to_long_as_MinValue_when_CompareTo_is_called()
        {
            long.MinValue.CompareTo(Cursor.StartOfStream).Should().Be(0);
        }

        [Test]
        public async Task A_new_cursor_has_a_position_that_is_less_than_any_DateTime_value()
        {
            (Cursor.StartOfStream < DateTime.MinValue).Should().BeTrue();
        }

        [Test]
        public async Task A_new_cursor_casts_to_DateTime_as_MinValue_when_CompareTo_is_called()
        {
            DateTime.MinValue.CompareTo(Cursor.StartOfStream).Should().Be(0);
        }

        [Test]
        public async Task A_new_cursor_has_a_position_that_is_less_than_any_DateTimeOffset_value()
        {
            (Cursor.StartOfStream < DateTimeOffset.MinValue).Should().BeTrue();
        }

        [Test]
        public async Task A_new_cursor_casts_to_DateTimeOffset_as_MinValue_when_CompareTo_is_called()
        {
            DateTimeOffset.MinValue.CompareTo(Cursor.StartOfStream).Should().Be(0);
        }

        [Test]
        public async Task A_new_cursor_has_a_position_that_is_less_than_any_String_value()
        {
            (Cursor.StartOfStream < (string) null).Should().BeTrue();
            (Cursor.StartOfStream < "").Should().BeTrue();
            (Cursor.StartOfStream < "a").Should().BeTrue();
        }

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
    }
}