using System;
using MoyskleyTech.LINQToSQL.SQLite;
using MoyskleyTech.LINQToSQL;
using MoyskleyTech.LINQToSQL.Data;
using System.Runtime.CompilerServices;
using MoyskleyTech.LINQToSQL.Data.Tables;
using System.Net.Http;

namespace UnitTest
{
    class Program
    {
        static void Main(string[ ] args)
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

            foreach ( var c in cmd.ToList() )
            {
                Console.WriteLine(c.ToString());
            }

            cmd = db.Ts.Named("k").Where(new SQL("not exists(select * from T t where t.ID = k.ID-1)"));
            Console.WriteLine(cmd.SQL.Command);


            foreach ( var c in cmd.ToList() )
            {
                Console.WriteLine(c.ToString());
            }
            bool b = new SQL("ID>0") & true;
            cmd = db.Ts.Where(new SQL("ID>0")).Where((x) => x.ID < 5);
            Console.WriteLine(cmd.SQL.Command);

            var begin=DateTime.Now;
            var end=DateTime.Now;
            var fran = new {ID=4 };

            var query = from p in db.Payments
                        join c in db.Contracts
                                             on p.Contract equals c.DatabaseID
                        where p.PaymentDate >= begin && p.PaymentDate <= end &&c.Franchise == fran.ID
                        join cl in db.Clients on c.Client equals cl.ID
                        select new {p, c, cl};

            Console.WriteLine(query.SQL.Command);
            foreach ( var c in query.ToList() )
            {
                Console.WriteLine(c.ToString());
            }

            var query2 = from p in db.Productions
                         join c in db.Contracts on p.Contract equals c.DatabaseID
                         join cl in db.Users on c.Client equals cl.ID
                         select new { p , c , cl };

            Console.WriteLine(query2.SQL.Command);
            foreach ( var c2 in query2.ToList() )
            {
                Console.WriteLine(c2.ToString());
            }

            db.Insert(new Cl() { ID=1, IsClient = false });
            db.Insert(new Cl() { ID = 2 , IsClient = true });
            db.Insert(new Cl() { ID = 6 , IsClient = false });

            foreach ( var c2 in db.Users.ToList() )
            {
                Console.WriteLine(c2.ToString());
            }
            db.Users.Where(u=>u.ID<5).UpdateAll(c => c.IsClient,true);
            foreach ( var c2 in db.Users.ToList() )
            {
                Console.WriteLine(c2.ToString());
            }
        }
    }

    internal class DB : ContextIndependantDatabase
    {
        public DB() : base(new SQLiteProxy())
        {

        }
        public TableQuery<T> Ts => this.Query<T>();
        public TableQuery<P> Payments => this.Query<P>();
        public TableQuery<C> Contracts => this.Query<C>();
        public TableQuery<Cl> Clients => this.Query<Cl>().Where((x)=>x.IsClient);
        public TableQuery<Cl> Users => this.Query<Cl>();
        public TableQuery<Pr> Productions => this.Query<Pr>();
    }
    [DataSource("T")]
    public class T : DBTable
    {
        [DataColumn("ID" , true , true)]
        public int ID { get; set; }
    }
    [DataSource("P")]
    public class P : DBTable
    {
        [DataColumn("id_pay_con" , true , true)]
        public long ID { get; set; }
        [DataColumn("id_contract")]
        public long Contract { get; set; }
        [DataColumn("Total")]
        public decimal Total { get; set; }
        [DataColumn("Type")]
        public string Type { get; set; }
        [DataColumn(nameof(PaymentDate))]
        public DateTime PaymentDate { get; set; }
        [DataColumn(nameof(PaypalTransactionID))]
        public string PaypalTransactionID { get; set; }
    }
    [DataSource("Pr")]
    public class Pr : DBTable
    {
        [DataColumn("id_pay_con" , true , true)]
        public long ID { get; set; }
        [DataColumn("id_contract")]
        public long Contract { get; set; }
        [DataColumn("Total")]
        public decimal Total { get; set; }
        [DataColumn("Type")]
        public string Type { get; set; }
        [DataColumn(nameof(PaymentDate))]
        public DateTime PaymentDate { get; set; }
        [DataColumn(nameof(PaypalTransactionID))]
        public string PaypalTransactionID { get; set; }
    }
    [DataSource("C")]
    public class C : DBTable
    {
        [DataColumn("id_db_contract" , true , true)]
        public long DatabaseID { get; set; }
        [DataColumn("contract_number")]
        public string Number { get; set; }
        [DataColumn("previous_version")]
        public long? PreviousVersion { get; set; }
        [DataColumn("client_id")]
        public long Client { get; set; }
        [DataColumn("address")]
        public long Address { get; set; }
        [DataColumn(nameof(Sold))]
        public bool Sold { get; set; }
        [DataColumn(nameof(HourlyRate))]
        public decimal HourlyRate { get; set; }
        [DataColumn(nameof(Total))]
        public decimal Total { get; set; }
        [DataColumn(nameof(Deposit))]
        public decimal Deposit { get; set; }
        [DataColumn(nameof(ApproximativeBegin))]
        public DateTime ApproximativeBegin { get; set; }
        [DataColumn(nameof(ApproximativeEnd))]
        public DateTime ApproximativeEnd { get; set; }
        [DataColumn(nameof(Begin))]
        public DateTime? Begin { get; set; }
        [DataColumn(nameof(End))]
        public DateTime? End { get; set; }
        [DataColumn(nameof(Particularities))]
        public string Particularities { get; set; }
        [DataColumn(nameof(FileID))]
        public long? FileID { get; set; }
        [DataColumn(nameof(ClientSignature))]
        public byte[ ] ClientSignature { get; set; }
        [DataColumn(nameof(ContractorSignature))]
        public byte[ ] ContractorSignature { get; set; }
        [DataColumn(nameof(Franchise))]
        public long Franchise { get; set; }
    }
    [DataSource("Cl")]
    public class Cl : DBTable
    {
        [DataColumn("id_profile" , true , true)]
        public long ID { get; set; }
        [DataColumn("username")]
        public string Username { get; set; }
        [DataColumn("first_name")]
        public string FirstName { get; set; }
        [DataColumn("last_name")]
        public string LastName { get; set; }
        [DataColumn("created_on")]
        public DateTime CreatedOn { get; set; } = DateTime.Now;
        [DataColumn("isclient")]
        public bool IsClient { get; set; } = false;

    }
}
