using MoyskleyTech.LINQToSQL.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace MoyskleyTech.LINQToSQL.Helper
{
    public static class TypeHelper
    {
        public static bool IsJoin(this Type t)
        {
            return t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Join<,>);
        }
        public static bool IsTableQuery(this Type t)
        {
            return t.IsGenericType && t.GetGenericTypeDefinition() == typeof(TableQuery<>);
        }
        public static bool Is<T>(this Type t)
        {
            return Is(t, typeof(T));
        }
        public static bool Is(this Type t, Type t2)
        {
            return
                t == t2 ||
                t2.IsAssignableFrom(t)
                ||
            (t.IsGenericType && t.GetGenericTypeDefinition() == t2);
        }

        public static bool IsAnonymous(this Type type)
        {
            if (type.IsGenericType)
            {
                var d = type.GetGenericTypeDefinition();
                if (d.IsClass && d.IsSealed && d.Attributes.HasFlag(TypeAttributes.NotPublic))
                {
                    var attributes = d.GetCustomAttributes(typeof(CompilerGeneratedAttribute), false);
                    if (attributes != null && attributes.Length > 0)
                    {
                        //WOW! We have an anonymous type!!!
                        return true;
                    }
                }
            }
            return false;
        }

        public static bool IsAnonymousType<T>(this T instance)
        {
            return IsAnonymous(instance.GetType());
        }
    }
}
