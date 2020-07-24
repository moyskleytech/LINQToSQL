using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace MoyskleyTech.LINQToSQL.Data.Tables
{
    public class DBTable : IEquatable<DBTable>
    {
        [OnUpdate]
        [DataColumn("___Last___Modified___")]
        public DateTime LastModified { get; set; }

        [JsonIgnore]
        public Dictionary<string, object> Values
        {
            get
            {
                Dictionary<string, object> vals = new Dictionary<string, object>();
                foreach (var member in GetType().GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>() != null))
                {
                    var attr = member.GetCustomAttribute<DataColumnAttribute>();

                    vals[attr.Name] = ContextIndependantDatabase.GetMemberValue(this, member);
                }
                return vals;
            }
        }
        [JsonIgnore]
        public Dictionary<string, object> Keys
        {
            get
            {
                Dictionary<string, object> vals = new Dictionary<string, object>();
                foreach (var member in GetType().GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>() != null))
                {
                    var attr = member.GetCustomAttribute<DataColumnAttribute>();
                    if (attr.IsPrimary)
                        vals[attr.Name] = ContextIndependantDatabase.GetMemberValue(this, member);
                }
                return vals;
            }
        }
        public bool Equals(DBTable other)
        {
            var otherKeys = other.Keys;
            foreach (var key in this.Keys)
            {
                dynamic ours = key.Value;
                dynamic theirs = otherKeys[key.Key];
                if (ours != theirs)
                    return false;
            }
            return true;
        }

        public override string ToString()
        {
            return "{"+string.Join(",", Values.Select((x) => x.Key + ":" + x.Value))+"}";
        }
    }
}