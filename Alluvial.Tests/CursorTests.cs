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
        public async Task DateTimeOffset_cursor_HasCursorReached_with_ascending_sort()
        {
            var startAt = DateTimeOffset.Parse("2014-12-30 01:58:48 PM");

            var cursor = Cursor.New(startAt);

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
        public async Task int_cursor_HasCursorReached_with_ascending_sort()
        {
            var startAt = 123;

            var cursor = Cursor.New(startAt);

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
        public async Task string_cursor_HasCursorReached_with_ascending_sort()
        {
            var cursor = Cursor.New("j");

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
        public async Task ascending_int_Cursor_position_after_AdvanceTo_is_correct()
        {
            var cursor = Cursor.New(10);

            cursor.AdvanceTo(100);

            cursor.Position.Should().Be(100);
        }

        [Test]
        public async Task ascending_DateTimeOffset_Cursor_position_after_AdvanceTo_is_correct()
        {
            var cursor = Cursor.New(DateTimeOffset.Parse("2015-01-01 12am +00:00"));

            var expectedPosition = DateTimeOffset.Parse("2015-12-31 12am +00:00");
            cursor.AdvanceTo(expectedPosition);

            cursor.Position.Should()
                  .Be(expectedPosition);
        }
    }
}