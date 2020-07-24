using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace MoyskleyTech.LINQToSQL.Helper
{
    public static class IenumerableHelper
    {
        public static U MaxOrDefault<T, U>(this IEnumerable<T> src, Func<T, U> selector)
        {
            return src.Any() ? src.Max(selector) : default;
        }
        public static U MinOrDefault<T, U>(this IEnumerable<T> src, Func<T, U> selector)
        {
            return src.Any() ? src.Min(selector) : default;
        }
    }
}
