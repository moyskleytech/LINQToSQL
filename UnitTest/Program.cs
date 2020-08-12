using System;
using MoyskleyTech.LINQToSQL.SQLite;
using MoyskleyTech.LINQToSQL;
using MoyskleyTech.LINQToSQL.Data;
using System.Runtime.CompilerServices;
using MoyskleyTech.LINQToSQL.Data.Tables;

namespace UnitTest
{
    class Program
    {
        static void Main(string[] args)
        {
            DatabaseSettings.Instance = new DatabaseSettings() { ConnectionString = "Filename=:memory:" };
            DB db = new DB();
            db.Open();
            db.CheckSchema();

            db.Insert(new T() { ID = 1 });
            db.Insert(new T() { ID = 2 });
            db.Insert(new T() { ID = 3 });
            db.Insert(new T() { ID = 5 });
            db.Insert(new T() { ID = 7 });


            var cmd = db.Ts.Where(new SQL("ID>0"));
            Console.WriteLine(cmd.SQL.Command);

            foreach (var c in cmd.ToList()) {
                Console.WriteLine(c.ToString());
            }

            cmd = db.Ts.Named("k").Where(new SQL("not exists(select * from T t where t.ID = k.ID-1)"));
            Console.WriteLine(cmd.SQL.Command);


            foreach (var c in cmd.ToList())
            {
                Console.WriteLine(c.ToString());
            }
            bool b = new SQL("ID>0") & true;
            cmd = db.Ts.Where(new SQL("ID>0")).Where((x)=>x.ID<5);
            Console.WriteLine(cmd.SQL.Command);
        }
    }

    internal class DB : ContextIndependantDatabase
    {
        public DB() : base(new SQLiteProxy() )
        { 
            
        }
        public TableQuery<T> Ts => this.Query<T>();
    }
    [DataSource("T")]
    public class T : DBTable
    { 
        [DataColumn("ID",true,true)]
        public int ID { get; set; }
    }
}
