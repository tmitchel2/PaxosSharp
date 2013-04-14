using System.Collections.Generic;
using System.Linq;

namespace PaxosSharp
{
    internal static class LinqExtensions
    {
        public static IEnumerable<IList<T>> GroupByBatchSize<T>(this IEnumerable<T> source, int batchSize)
        {
            return source
                .Select((x, i) => new { Val = x, Idx = i })
                .GroupBy(x => x.Idx / batchSize, (k, g) => g.Select(x => x.Val).ToList());
        }
    }
}