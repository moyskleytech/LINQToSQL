using MoyskleyTech.LINQToSQL.Data;
using MySql.Data.MySqlClient;

namespace MoyskleyTech.LINQToSQL.Mysql
{
    public class MySQLConnectionWrapper : DatabaseProxyConnection
    {
        private MySqlConnection cn;

        public MySQLConnectionWrapper(MySqlConnection cn)
        {
            this.cn = cn;
        }

        public override void Close()
        {
            cn.Clone();
        }

        public override DatabaseProxyCommand CreateCommand()
        {
            return new MySQLProxyCommand(cn.CreateCommand());
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
}
