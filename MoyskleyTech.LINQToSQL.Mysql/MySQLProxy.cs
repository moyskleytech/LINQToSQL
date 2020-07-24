using MoyskleyTech.LINQToSQL.Data;
using MySql.Data.MySqlClient;
using System;
using System.Reflection;

namespace MoyskleyTech.LINQToSQL.Mysql
{
    public class MySQLProxy : DatabaseProxy
    {
        public override DBProvider Provider => DBProvider.MySQL;
        public override string Auto(bool isAuto)
        {
            if (isAuto)
                return "AUTO_INCREMENT";
            else
                return string.Empty;
        }

        public override DatabaseProxyConnection Connect(string connectionString)
        {
            MySqlConnection cn = new MySqlConnection();
            cn.ConnectionString = connectionString;
            return new MySQLConnectionWrapper(cn);
        }

        public override string GetTypeFor(PropertyInfo element)
        {
            if (IsAny(element.PropertyType, typeof(bool), typeof(bool?)))
                return "BOOL";
            if (IsAny(element.PropertyType, typeof(byte), typeof(short), typeof(int), typeof(long),
                typeof(sbyte), typeof(ushort), typeof(uint), typeof(ulong),
                typeof(byte?), typeof(short?), typeof(int?), typeof(long?),
                typeof(sbyte?), typeof(ushort?), typeof(uint?), typeof(ulong?)
                ))
                return "BIGINT";
            if (IsPrimaryKey(element) && IsAny(element.PropertyType, typeof(TimeSpan), typeof(TimeSpan?), typeof(string)))
                return "VARCHAR(255)";
            if (IsAny(element.PropertyType, typeof(DateTime), typeof(DateTime?)))
                return "DATETIME";
            if (IsAny(element.PropertyType, typeof(TimeSpan), typeof(TimeSpan?), typeof(string)))
                return "TEXT";
            if (IsAny(element.PropertyType, typeof(float), typeof(double), typeof(decimal),
                typeof(decimal?), typeof(float?), typeof(double?)))
                return "DOUBLE";
            return "BLOB";
        }


    }
}
