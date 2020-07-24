using System.Linq;

namespace MoyskleyTech.LINQToSQL.Data
{
    public class SqlBuilder:SQL
    {
        public SqlBuilder() : base(null)
        {
            
        }
        public DBProvider Provider { get; set; } = DBProvider.MSSQL;

        public string SQL { get => Command; private set => Command = value; }
        public SqlBuilder SelectAll
        {
            get
            {
                return new SqlBuilder() { SQL = SQL + "SELECT * ", Provider = Provider };
            }
        }
        public SqlBuilder Select(params string[] values)
        {
            return new SqlBuilder() { SQL = SQL + "SELECT " + string.Join(",", values) + " ", Provider = Provider };
        }
        public SqlBuilder From(params string[] tables)
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + " FROM " + string.Join(",", tables) };
        }
        public SqlBuilder From(string[] tables,string[] name )
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + " FROM " +  string.Join(",", tables.Zip(name, (t, n) => t + " " + n)) };
        }
        public SqlBuilder DeleteFrom(string table)
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + "DELETE FROM " + table + " " };
        }
        public SqlBuilder LeftJoin(string table, string on)
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + " LEFT JOIN " + table + " ON " + on };
        }
        public SqlBuilder InnerJoin(string table, string on)
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + " INNER JOIN " + table + " ON " + on };
        }
        public SqlBuilder RightJoin(string table, string on)
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + " RIGHT JOIN " + table + " ON " + on };
        }
        public SqlBuilder Where(string condition)
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + " WHERE " + condition };
        }
        public SqlBuilder OrderBy(params string[] values)
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + " ORDER BY " + string.Join(",", values) };
        }
        public string And(string v1, string v2)
        {
            return "(" + v1 + " AND " + v2 + ")";
        }
        public string Or(string v1, string v2)
        {
            return "(" + v1 + " OR " + v2 + ")";
        }
        public SqlBuilder Parentesis(string condition)
        {
            return new SqlBuilder() { Provider = Provider, SQL = SQL + " ( " + condition + " ) " };
        }

        internal string IfNull(string v1, string v2)
        {
            if (Provider == DBProvider.MSSQL) return "isnull(" + v1 + "," + v2 + ")"; else return "ifnull(" + v1 + "," + v2 + ")";
        }

        internal string As(string v)
        {
            return " AS '" + v + "'";
        }
        internal string Substring(string v1, string v2, string v3)
        {
            return "substring(" + v1 + "," + v2 + "," + v3 + ")";
        }
        internal string IndexOf(string v1, string v2)
        {
            if (Provider == DBProvider.MSSQL) return "charindex(" + v2 + "," + v1 + ")";
            else return "instr(" + v1 + "," + v2 + ")";
        }
        internal string ToNumeric(string v1)
        {
            if (Provider == DBProvider.MSSQL) return "TRY_CONVERT(decimal(12,6)," + v1 + ")";
            else return "(" + v1 + "+0)";
        }

    }
}
