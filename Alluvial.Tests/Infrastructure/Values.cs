using System.Collections.Generic;

namespace Alluvial.Tests
{
    public static class Values
    {
        public static IEnumerable<string> AtoZ()
        {
            for (var c = 'a'; c <= 'z'; c++)
            {
                yield return new string(c, 1);
            }
        }
    }
}