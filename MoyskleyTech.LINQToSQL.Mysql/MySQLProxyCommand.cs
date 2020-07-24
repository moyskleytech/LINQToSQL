using MoyskleyTech.LINQToSQL.Data;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.Common;

namespace MoyskleyTech.LINQToSQL.Mysql
{

    public class MySQLProxyCommand : DatabaseProxyCommand
    {
        private MySqlCommand cmd;

        public MySQLProxyCommand(MySqlCommand mySqlCommand)
        {
            this.cmd = mySqlCommand;
        }

        public override string CommandText { get => cmd.CommandText; set => cmd.CommandText = value; }

        public override bool SupportsNamedParameters => true;

        public override long LastInsertedId => cmd.LastInsertedId;

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
        public void Set()
        {
            var cmdText = cmd.CommandText;
            foreach (var param in Parameters)
                cmd.Parameters.AddWithValue(param.ParameterName, param.Value);
        }
    }
}
