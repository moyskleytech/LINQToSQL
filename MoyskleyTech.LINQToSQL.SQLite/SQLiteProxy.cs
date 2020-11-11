using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using MoyskleyTech.LINQToSQL.Data;
using Microsoft.Data.Sqlite;
using System.Reflection;

namespace MoyskleyTech.LINQToSQL.SQLite
{
    public class SQLiteProxy : MoyskleyTech.LINQToSQL.Data.DatabaseProxy
    {
        public override DBProvider Provider =>  DBProvider.SQLite;

        public override string Auto(bool isAuto)
        {
            return " AUTOINCREMENT ";
        }

        public override DatabaseProxyConnection Connect(string connectionString)
        {
            var cn = new SqliteConnection(connectionString);
            return new SQLiteConnectionProxy(cn);
        }
        public override string GetInsertInto(string table , bool ignore)
        {
            return "INSERT " + ( ( ignore ) ? " OR IGNORE " : " " ) + " INTO " + table + "(";
        }
        public override string GetTypeFor(PropertyInfo element)
        {
            if (IsAny(element.PropertyType, typeof(bool), typeof(byte), typeof(short), typeof(int), typeof(long),
                typeof(sbyte), typeof(ushort), typeof(uint), typeof(ulong),

                typeof(bool?), typeof(byte?), typeof(short?), typeof(int?), typeof(long?),
                typeof(sbyte?), typeof(ushort?), typeof(uint?), typeof(ulong?)
                ))
                return "INTEGER";
            if (IsAny(element.PropertyType, typeof(TimeSpan), typeof(DateTime), typeof(TimeSpan?), typeof(DateTime?), typeof(string)))
                return "TEXT";
            if (IsAny(element.PropertyType, typeof(float), typeof(double),
                typeof(float?), typeof(double?)))
                return "REAL";
            return "BLOB";
        }
    }

    internal class SQLiteConnectionProxy : DatabaseProxyConnection
    {
        private readonly SqliteConnection cn;

        public SQLiteConnectionProxy(SqliteConnection cn)
        {
            this.cn = cn;
        }

        public override void Close()
        {
            cn.Close();
        }

        public override DatabaseProxyCommand CreateCommand()
        {
            return new SQLiteCommandProxy(cn.CreateCommand());
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

    internal class SQLiteCommandProxy : DatabaseProxyCommand
    {
        private readonly SqliteCommand cmd;

        public SQLiteCommandProxy(SqliteCommand sqliteCommand)
        {
            this.cmd = sqliteCommand;
        }

        public override string CommandText { get => cmd.CommandText; set => cmd.CommandText = value; }

        public override bool SupportsNamedParameters => false;

        public override long LastInsertedId
        {
            get
            {
                cmd.CommandText = "select last_insert_rowid();";

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
