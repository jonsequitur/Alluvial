using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamQueryTests
    {
        [Test]
        public async Task A_stream_can_be_queried_from_the_beginning()
        {
            var stream = Stream.Create<int>(async query =>
                                                Enumerable.Range(query.Cursor.Position, query.BatchCount.Value));

            var value = stream.CreateQuery(Cursor.Create(0), 1).NextBatch().Result.Single();

            value.Should().Be(0);
        }

        [Test]
        public async Task Stream_items_can_be_batched_and_cursored()
        {
            var stream = Stream.Create<int>(async query =>
                                                Enumerable.Range(query.Cursor.Position, query.BatchCount.Value));

            var batch = stream.CreateQuery(Cursor.Create(5), 3).NextBatch().Result;

            batch.Should().BeEquivalentTo(5, 6, 7);
        }

        [Test]
        public async Task Defining_a_key_query()
        {
            var sequences =
                Enumerable.Range(0, 100)
                          .ToDictionary(i => Guid.NewGuid(),
                                        i => Enumerable.Range(1, i)
                                                       .Select(j => j.ToString())
                                                       .ToArray());

            var keyStream = Stream.Create<Guid>(q => sequences.Keys);

            var allKeys = await keyStream.CreateQuery(Cursor.Create(0), 1000).NextBatch();

            allKeys.Distinct()
                   .Count()
                   .Should()
                   .Be(100);
        }

        [Test]
        public async Task When_a_query_does_not_reach_the_end_of_the_result_set_then_the_query_cursor_is_set_to_the_last_queried_position()
        {
            var values = Enumerable.Range(1, 20);

            var stream = values.AsStream();

            var query = stream.CreateQuery(stream.NewCursor(), 5);

            var batch = await query.NextBatch();
            batch.Should().BeEquivalentTo(new[] { 1, 2, 3, 4, 5 });
            ((int) query.Cursor.Position)
                .Should()
                .Be(5);
        }

        [Test]
        public async Task When_a_query_consumes_the_entire_result_set_then_the_query_cursor_is_set_to_the_last_result_position()
        {
            var values = Enumerable.Range(1, 20);

            var stream = values.AsStream();

            var query = stream.CreateQuery(stream.NewCursor(), 25);

            var batch = await query.NextBatch();
            batch.Should().BeEquivalentTo(values);
            ((int) query.Cursor.Position).Should().Be(20);
        }

        [Test]
        public async Task When_a_query_reaches_the_end_of_the_result_set_then_the_query_cursor_is_set_to_the_result_position()
        {
            var values = Enumerable.Range(1, 20);

            var stream = values.AsStream();

            var query = stream.CreateQuery(Cursor.Create(10), 25);

            var batch = await query.NextBatch();
            batch.Should().BeEquivalentTo(new[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 });
            ((int) query.Cursor.Position)
                .Should()
                .Be(20);
        }

        [Test]
        public async Task Streams_can_be_traversed_by_calling_Query_NextBatch_repeatedly()
        {
            var stream = Enumerable.Range(1, 25).AsStream();

            var query = stream.CreateQuery(Cursor.Create(5), 5);
            var firstBatch = await query.NextBatch();
            var secondBatch = await query.NextBatch();

            firstBatch.Should().BeEquivalentTo(6, 7, 8, 9, 10);
            secondBatch.Should().BeEquivalentTo( 11, 12, 13, 14, 15);
        }

        [Test]
        public async Task Batch_cursors_reflect_the_cursor_position_at_the_start_of_the_batch_when_starting_with_a_new_cursor()
        {
            var stream = Enumerable.Range(1, 25).AsStream();

            var batch = await stream.CreateQuery(stream.NewCursor(), 10).NextBatch();

            ((int) batch.StartsAtCursorPosition).Should().Be(0);
        }

        [Test]
        public async Task Batch_cursors_reflect_the_cursor_position_at_the_start_of_the_batch()
        {
            var stream = Enumerable.Range(1, 100).AsStream();

            var batch = await stream.CreateQuery(Cursor.Create(15), 10)
                                    .NextBatch();

            ((int) batch.StartsAtCursorPosition).Should().Be(15);
        }

        [Test]
        public async Task A_date_based_cursor_can_be_used_traverse_a_stream_from_the_beginning()
        {
            var startTime = DateTimeOffset.Parse("2014-12-29 00:00 +00:00");
            var times = Enumerable.Range(1, 24).Select(i => startTime.AddHours(i));

            var stream = Stream.Create<DateTimeOffset>(query: async q =>
                                                           times.OrderBy(time => time)
                                                                .Where(time => time > q.Cursor.As<DateTimeOffset>())
                                                                .Take(q.BatchCount ?? 100000),
                                                       advanceCursor: (q, batch) => q.Cursor.AdvanceTo(batch.Last()),
                                                       newCursor: () => Cursor.Create(default(DateTimeOffset)));

            var query = stream.CreateQuery(stream.NewCursor(), 12);

            var batch1 = await query.NextBatch();
            ((DateTimeOffset) batch1.StartsAtCursorPosition)
                .Should()
                .Be(new DateTimeOffset());
            var batch2 = await query.NextBatch();
            ((DateTimeOffset) batch2.StartsAtCursorPosition)
                .Should()
                .Be(DateTimeOffset.Parse("2014-12-29 12:00pm +00:00"));

            batch1.Count
                  .Should()
                  .Be(12);
            batch2.Count
                  .Should()
                  .Be(12);
            batch1.Concat(batch2)
                  .Should()
                  .BeEquivalentTo(times);
        }

        [Test]
        public async Task A_string_based_cursor_can_be_used_traverse_a_stream_from_the_beginning()
        {
            var alphabetStrings = Enumerable.Range(97, 26)
                                            .Select(i => new string(Convert.ToChar(i), 1))
                                            .ToArray();
            var alphabetStream = alphabetStrings.AsStream();

            var query = alphabetStream.CreateQuery(Cursor.Create(""), 13);

            var batch1 = await query.NextBatch();
            ((string) batch1.StartsAtCursorPosition)
                .Should()
                .Be("");
            var batch2 = await query.NextBatch();
            ((string) batch2.StartsAtCursorPosition)
                .Should()
                .Be("m");

            batch1.Count
                  .Should()
                  .Be(13);
            batch2.Count
                  .Should()
                  .Be(13);

            batch1.Concat(batch2)
                  .Should()
                  .BeEquivalentTo(alphabetStrings);
        }

        [Test]
        public async Task A_string_based_cursor_can_be_used_traverse_a_stream_from_the_middle()
        {
            var alphabetStrings = Enumerable.Range(97, 26).Select(i => new string(Convert.ToChar(i), 1)).ToArray();
            var alphabetStream = alphabetStrings.AsStream();

            var query = alphabetStream.CreateQuery(Cursor.Create("j"), 5);

            var batch1 = await query.NextBatch();

            ((string) batch1.StartsAtCursorPosition).Should().Be("j");

            query.Cursor
                 .As<string>()
                 .Should()
                 .Be("o");

            batch1.Should()
                  .BeEquivalentTo(new[] { "k", "l", "m", "n", "o" });
        }

        [Test]
        public async Task AsStream_can_use_non_default_ordering()
        {
            var alphabetStrings = Enumerable.Range(97, 26).Select(i => new string(Convert.ToChar(i), 1)).Reverse().ToArray();
            var alphabetStream = alphabetStrings.AsStream();

            var query = alphabetStream.CreateQuery(Cursor.Create("o", ascending: false), 5);

            var batch1 = await query.NextBatch();

            ((string) batch1.StartsAtCursorPosition).Should().Be("o");

            query.Cursor
                 .As<string>()
                 .Should()
                 .Be("j");

            batch1.Should()
                  .BeEquivalentTo(new[] { "n", "m", "l", "k", "j" });
        }
    }
}