using System;
using System.Collections.Generic;
using System.IO;
using FluentAssertions;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Its.Log.Instrumentation;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace Alluvial.Samples
{
    [TestFixture]
    public class WebstersDictionaryExample
    {
        [Test]
        [Explicit]
        public async Task Can_stream_dictionary_entries()
        {
            var firstTenEntries = WebstersDictionary.Entries().Take(10);

            firstTenEntries.Should().HaveCount(10);
        }

        [Test]
        [Explicit]
        public async Task Dictionary_as_a_stream_partitioned_by_first_character()
        {
            var stream = WebstersDictionary.EntriesAsPartitionedStream().Trace();

            var catchup = stream.CreateDistributedCatchup(batchSize: 100)
                                .DistributeInMemoryAmong(new[] { 'A', 'B', 'C' }.Select(Partition.ByValue));

            var projectionStore = new InMemoryProjectionStore<DictionarySection>();

            catchup.Subscribe<DictionarySection, DictionaryEntry>((initial, batch) =>
            {
                initial.AddRange(batch);
                return initial;
            }, projectionStore);

            await catchup.RunSingleBatch();

            Console.WriteLine(JsonConvert.SerializeObject(
                projectionStore.Select(section => section.OrderBy(e => e.Word)), Formatting.Indented));
        }
    }

    public class DictionarySection : List<DictionaryEntry>, ICursor<int>
    {
        public int Position { get;private set; }

        public void AdvanceTo(int point) => Position = point;

        public bool HasReached(int point) => Position >= point;
    } 


    public static class WebstersDictionary
    {
        public static IEnumerable<DictionaryEntry> Entries()
        {
            var client = new HttpClient();

            using (var s = client.GetStreamAsync("https://github.com/adambom/dictionary/blob/master/dictionary.json?raw=true").Result)
            using (var sr = new StreamReader(s))
            using (var reader = new JsonTextReader(sr))
            {
                var serializer = new JsonSerializer();

                // read the json from a stream
                // json size doesn't matter because only a small piece is read at a time from the HTTP request
                var p = serializer.Deserialize<JObject>(reader);

                var position = 0;

                foreach (var keyValuePair in p)
                {
                    position++;
                    yield return new DictionaryEntry
                    {
                        Word = keyValuePair.Key,
                        Definition = keyValuePair.Value.ToString(),
                        Position = position
                    };
                }
            }
        }

        public static IPartitionedStream<DictionaryEntry, int, Char> EntriesAsPartitionedStream()
        {
            var entries = Entries();

            return Stream.PartitionedByValue<DictionaryEntry, int, Char>(
                query: (query, partition) =>
                {
                    return Task.FromResult(entries.Where(e => e.Word[0].IsWithinPartition(partition))
                                                  .Skip(query.Cursor.Position)
                                                  .Take(query.BatchSize.Value));
                },
                advanceCursor: (query, batch) =>
                {
                    if (batch.Any())
                    {
                        query.Cursor.AdvanceTo(batch.Last().Position);
                    }
                });
        }
    }

    public class DictionaryEntry
    {
        static DictionaryEntry()
        {
            Formatter<DictionaryEntry>.RegisterForAllMembers();
        }

        public string Word { get; set; }
        public string Definition { get; set; }
        public int Position { get; set; }
    }
}