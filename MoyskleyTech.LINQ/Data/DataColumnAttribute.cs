using System;

namespace MoyskleyTech.LINQToSQL.Data
{
    public class DataColumnAttribute : Attribute
    {
        public string ColumnName { get; set; }
        public bool IsPrimary { get; set; }
        public string Name => ColumnName;
        public bool IgnoreOnSave { get; set; } = false;
        public bool IsExternalData { get; set; }
        public bool IsAuto { get; set; }

        public DataColumnAttribute(string name, bool isPrimary=false,bool isAuto=false)
        {
            ColumnName = name;
            IsPrimary = isPrimary;
            IsAuto = isAuto;
        }
    }
}
