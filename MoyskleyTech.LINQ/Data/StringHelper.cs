using System.Collections.Generic;

namespace MoyskleyTech.LINQToSQL.Data
{
    public static class StringHelper
    {
        public static string ForgetLast(this string a, int count)
        {
            return a.Remove(a.Length - count);
        }
        public static T GetValueOrDefault<T, K>(this Dictionary<K, T> dictionary, K key)
        {
            if (dictionary.ContainsKey(key))
                return dictionary[key];
            else
                return default(T);
        }
    }
}
