using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using static MoyskleyTech.LINQToSQL.Data.BaseTableQuery;

namespace MoyskleyTech.LINQToSQL.Data
{
    public class MappingContext
    {
        public MappingContext(TableMapping table)
        {
            Current = table;
        }
        public MappingContext(TableMapping table, MappingContext parent) : this(table)
        {
            Parent = parent;
        }

        public TableMapping Current { get; set; }
        public MappingContext Parent { get; set; }

        public MappingResult FindColumn(MemberExpression mem)
        {
            var result = Current.FindColumn(mem);
            if (Parent != null)
            {
                var mapResult = Parent.FindColumn(mem);
                if ((mapResult != null && result == null) || (mapResult != null && mapResult.Path.Length > result.Path.Length))
                    result = mapResult;
            }
            return result;
        }
    }
    public class MappingResult
    {
        public MappingResult(string name, string path)
        {
            Column = name;
            Path = path;
        }

        public string Column { get; set; }
        public string Path { get; set; }
    }
    public class TableMapping
    {
        public string TableSource { get; set; }
        public string Name { get; set; }
        public Type DeclaringType { get; set; }
        public Dictionary<string, string> NameForPath { get; set; } = new Dictionary<string, string>();
        public TableMapping(Type t, string source)
        {
            TableSource = source;
            DeclaringType = t;

            Build();
        }
        public TableMapping(Type t, string source, string name)
        {
            TableSource = source;
            DeclaringType = t;
            Name = name;
            Build();
        }
        public TableMapping()
        {

        }
        public void Build()
        {
            NameForPath.Clear();
            var properties = DeclaringType.GetProperties();
            var typeAttribute = DeclaringType.GetCustomAttribute<DataSourceAttribute>();
            foreach (var property in properties)
            {
                var attribute = property.GetCustomAttribute<DataColumnAttribute>();
                if (attribute != null && !attribute.IsExternalData)
                {
                    var prefix = (Name ?? typeAttribute.TableName) + ".";
                    if (prefix == "." )
                        prefix = string.Empty;
                    NameForPath[property.Name] = prefix + attribute.ColumnName;
                }
            }
            foreach (var property in properties)
            {
                var attribute = property.GetCustomAttribute<DataColumnAttribute>();
                if (attribute != null && attribute.IsExternalData)
                {
                    NameForPath[property.Name] = attribute.ColumnName;
                }
            }
        }

        public static TableMapping Join(TableMapping left, TableMapping right)
        {
            TableMapping joinMapping = new TableMapping();
            joinMapping.DeclaringType = typeof(Join<,>).MakeGenericType(left.DeclaringType, right.DeclaringType);
            joinMapping.TableSource = left.TableSource;
            foreach (var x in left.NameForPath)
                joinMapping.NameForPath["Left." + x.Key] = x.Value;
            foreach (var x in right.NameForPath)
                joinMapping.NameForPath["Right." + x.Key] = x.Value;
            return joinMapping;
        }
        internal MappingResult FindColumn(MemberExpression expr)
        {
            var path = FindPath(expr);
            if (path.StartsWith("."))
                path = path.Substring(1);
            var name = NameForPath.GetValueOrDefault(path);
            while (name == null && path.Contains('.'))
            {
                path = path.Substring(path.IndexOf(".") + 1);
                name = NameForPath.GetValueOrDefault(path);
            }
            return new MappingResult(name, path);
        }

        internal TableMapping With(List<SelectedValue> selects)
        {
            TableMapping map = new TableMapping();
            map.DeclaringType = DeclaringType;
            map.TableSource = TableSource;
            map.Name = Name;
            map.NameForPath = new Dictionary<string, string>(NameForPath);
            if (NameForPath.Count > 0)
                foreach (var val in selects)
                {
                    if (!map.NameForPath.Values.Contains(val.ColumnName))
                        map.NameForPath[val.ColumnName] = val.ColumnName;
                }
            return map;
        }

        private string FindPath(Expression expr)
        {
            if (expr is MemberExpression mem)
                return FindPath(mem.Expression) + "." + mem.Member.Name;
            if (expr is ParameterExpression par)
                return par.Name;
            return "?";
        }
    }
}
