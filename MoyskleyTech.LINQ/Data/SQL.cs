using System;

namespace MoyskleyTech.LINQToSQL.Data
{
    public class SQL
    {
        public SQL(string s) => Command = s;
        public static SQL FromString(string s) => new SQL(s);
        public string Command { get; set; }
        public override string ToString() => Command;


        public static implicit operator bool(SQL s)
        {
            return false;
        }
        public static implicit operator string(SQL s)
        {
            return s.Command;
        }
        public static explicit operator int(SQL s)
        {
            return 0;
        }
        public static explicit operator long(SQL s)
        {
            return 0;
        }
        public static explicit operator double(SQL s)
        {
            return 0;
        }
        public static explicit operator float(SQL s)
        {
            return 0;
        }
        public static explicit operator DateTime(SQL s)
        {
            return new DateTime();
        }
        public static explicit operator byte[](SQL s)
        {
            return null;
        }
    }
    public class SQLWithParam : SQL
    {
        public SQLWithParam(string s, object[] args):base(s)
        {
            Args = args;
        }
        public object[] Args { get; set; }
        public static SQLWithParam FromStringWithParam(string s,params object[] args) => new SQLWithParam(s,args);
    }
}