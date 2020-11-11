using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

namespace MoyskleyTech.LINQToSQL.Data
{
    public abstract class DatabaseProxy
    {
        public abstract DBProvider Provider { get; }
        public abstract DatabaseProxyConnection Connect(string connectionString);

        public abstract string GetTypeFor(PropertyInfo element);

        public static bool IsAny(Type t, params Type[] o)
        {
            return o.Any((i) => i == t);
        }
        public static bool IsPrimaryKey(PropertyInfo element)
        {
            return element?.GetCustomAttributes(typeof(DataColumnAttribute), false)?.OfType<DataColumnAttribute>()?.FirstOrDefault()?.IsPrimary??false;
        }

        public abstract string Auto(bool isAuto);

        public virtual string GetInsertInto(string table , bool ignore)
        {
            return "INSERT " + ( ( ignore ) ? "IGNORE " : " " ) + " INTO " + table + "(";
        }
    }
    public abstract class DatabaseProxyConnection
    {
        public abstract void Open();
        public abstract void Close();
        public abstract bool IsOpen();
        public abstract DatabaseProxyCommand CreateCommand();
    }
    public abstract class DatabaseProxyCommand : IDisposable
    {
        public abstract string CommandText { get; set; }
        public abstract bool SupportsNamedParameters { get; }
        public DatabaseProxyParameter CreateParameter() => new DatabaseProxyParameter();
        public abstract void Dispose();
        public abstract int ExecuteNonQuery();
        public abstract DbDataReader ExecuteReader(System.Data.CommandBehavior behavior = System.Data.CommandBehavior.Default);
        public abstract object ExecuteScalar();
        public DatabaseProxyParameterCollection Parameters { get; } = new DatabaseProxyParameterCollection();
        public abstract long LastInsertedId { get; }
    }

    public class DatabaseProxyParameterCollection : IEnumerable<DatabaseProxyParameter>
    {
        private List<DatabaseProxyParameter> parameters = new List<DatabaseProxyParameter>();
        public void Clear() => parameters.Clear();
        public void Add(DatabaseProxyParameter param)
        {
            if (!parameters.Any((x) => x.ParameterName == param.ParameterName))
                parameters.Add(param);
            else
                throw new InvalidOperationException();
        }
        public bool Contains(string key)
        {
            return parameters.Any((x) => x.ParameterName == key);
        }
        public void AddWithValue(string key, object value)
        {
            Add(new DatabaseProxyParameter() { ParameterName = key, Value = value });
        }
        public bool RemoveAt(string key)
        {
            if (Contains(key))
                return parameters.Remove(parameters.FirstOrDefault((x) => x.ParameterName == key));
            return false;
        }
        public IEnumerator<DatabaseProxyParameter> GetEnumerator()
        {
            return ((IEnumerable<DatabaseProxyParameter>)parameters).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<DatabaseProxyParameter>)parameters).GetEnumerator();
        }
    }

    public class DatabaseProxyParameter
    {
        public string ParameterName { get; set; }
        public object Value { get; set; }
    }
}
