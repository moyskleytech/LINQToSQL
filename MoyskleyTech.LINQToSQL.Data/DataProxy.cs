using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using MoyskleyTech.LINQToSQL.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace MoyskleyTech.LINQToSQL.SQLite
{
    public class DataProxy : MoyskleyTech.LINQToSQL.Data.DatabaseProxy
    {
        public override DBProvider Provider => DBProvider.MSSQL;
        public override string Auto(bool isAuto)
        {
            return " IDENTITY ";
        }

        public override DatabaseProxyConnection Connect(string connectionString)
        {
            var cn = new SqlConnection(connectionString);
            return new DataConnectionProxy(cn);
        }
        public override string GetTypeFor(PropertyInfo element)
        {
            if (IsAny(element.PropertyType, typeof(bool[])))
                return "VARBINARY(max)";
            if (IsAny(element.PropertyType, typeof(bool), typeof(byte), typeof(short), typeof(int), typeof(long),
                typeof(sbyte), typeof(ushort), typeof(uint), typeof(ulong),
                typeof(bool?), typeof(byte?), typeof(short?), typeof(int?), typeof(long?),
                typeof(sbyte?), typeof(ushort?), typeof(uint?), typeof(ulong?)
                ))
                return "BIGINT";
            if (IsPrimaryKey(element) && IsAny(element.PropertyType, typeof(TimeSpan), typeof(TimeSpan?), typeof(string)))
                return "NVARCHAR(255)";
            if (IsAny(element.PropertyType, typeof(DateTime), typeof(DateTime?)))
                return "DATETIME";
            if (IsAny(element.PropertyType, typeof(TimeSpan), typeof(TimeSpan?), typeof(string)))
                return "NVARCHAR(max)";
            if (IsAny(element.PropertyType, typeof(float), typeof(double),
                typeof(float?), typeof(double?)))
                return "REAL";
            return "BLOB";
        }
    }

    internal class DataConnectionProxy : DatabaseProxyConnection
    {
        private SqlConnection cn;

        public DataConnectionProxy(SqlConnection cn)
        {
            this.cn = cn;
        }

        public override void Close()
        {
            cn.Close();
        }

        public override DatabaseProxyCommand CreateCommand()
        {
            return new DataCommandProxy(cn.CreateCommand());
        }

        public override bool IsOpen()
        {
            return cn.State == System.Data.ConnectionState.Open;
        }

        public override void Open()
        {
            cn.Open();
        }
    }

    internal class DataCommandProxy : DatabaseProxyCommand
    {
        private SqlCommand cmd;

        public DataCommandProxy(SqlCommand sqliteCommand)
        {
            this.cmd = sqliteCommand;
        }

        public override string CommandText { get => cmd.CommandText; set => cmd.CommandText = value; }

        public override bool SupportsNamedParameters => false;

        public override long LastInsertedId
        {
            get
            {
                var txt = cmd.CommandText;
                
                cmd.CommandText = "SELECT @@IDENTITY;";

                return (dynamic)cmd.ExecuteScalar();
            }
        }

        public override void Dispose()
        {
            cmd.Dispose();
        }

        public override int ExecuteNonQuery()
        {
            Set();
            return cmd.ExecuteNonQuery();
        }

        public override DbDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
        {
            Set();
            return cmd.ExecuteReader(behavior);
        }

        public override object ExecuteScalar()
        {
            Set();
            return cmd.ExecuteScalar();
        }

        private void Set()
        {
            var cmdText = cmd.CommandText;
            foreach (var param in Parameters)
            {
                var oldName = param.ParameterName;
                var newName = oldName.Replace("@", "$");
                cmd.Parameters.AddWithValue(newName, param.Value);
                cmdText = cmdText.Replace(oldName, newName);
            }
            cmd.CommandText = cmdText;
        }
    }
}
