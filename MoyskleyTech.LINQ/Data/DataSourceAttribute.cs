using System;

namespace MoyskleyTech.LINQToSQL.Data
{
    public class DataSourceAttribute : Attribute
    {
        public string TableName { get; set; }
        public bool IsTable { get; set; } = true;
        public DataSourceAttribute(string name)
        {
            TableName = name;
        }
    }
    public class OnUpdateAttribute : Attribute
    { }
}