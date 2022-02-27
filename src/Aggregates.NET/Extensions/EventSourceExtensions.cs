﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates.Extensions
{
    static class EventSourceExtensions
    {
        public static string BuildParentsString(this IEnumerable<Id> parents)
        {
            if (parents == null || !parents.Any())
                return "";
            return parents.Select(x => x.ToString()).Aggregate((cur, next) => $"{cur}:{next}");
        }
    }
}
