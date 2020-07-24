using MoyskleyTech.LINQToSQL.Data.Tables;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MoyskleyTech.LINQToSQL.Helper;
using System.Globalization;

namespace MoyskleyTech.LINQToSQL.Data
{
    public class ContextIndependantDatabase
    {
        protected ContextIndependantDatabase(DatabaseProxy Proxy)
        {
            this.Proxy = Proxy;
        }
        public DatabaseProxy Proxy { get; private set; }

        DatabaseProxyConnection my_cn;
       
        public void CheckSchema()
        {
            var tables = GetType().GetProperties().Where((x) => x.PropertyType.Is(typeof(TableQuery<>))).Select((x) => x.PropertyType.GenericTypeArguments[0]).ToList();
            var mapping = (from x in tables select GetMapping((x))).ToList();
            foreach (var map in mapping)
            {
                var testQuery = "select " + string.Join(",", from x in map.NameForPath.Keys select map.DeclaringType.GetProperty(x).GetCustomAttributes(typeof(Data.DataColumnAttribute), false).OfType<DataColumnAttribute>().FirstOrDefault().Name)
                    + " from " + map.TableSource;
                //if (db.Exec(testQuery) == -1)
                Fix(map);
            }
        }
        private void Fix(TableMapping map)
        {
            var cmd = CreateCommand("select * from " + map.TableSource);
            //Add missing columns
            try
            {
                DataTable dt = new DataTable();
                dt.Load(cmd.ExecuteReader(CommandBehavior.SchemaOnly));
                foreach (var col in map.NameForPath.Select((x) => GetElement(map, x)))
                {
                    if (dt.Columns.IndexOf(col.Name) == -1)
                    {
                        Exec("ALTER TABLE " + map.TableSource + " ADD " + col.Name + " " + col.Type);
                    }
                }
            }
            catch
            {
            }
           
            var cols = from path in map.NameForPath
                       select GetElement(map, path);
            var isMultiplePrimary = cols.Count((x) => x.Primary.Length > 0) > 1;

            string createTable = "CREATE TABLE IF NOT EXISTS " + map.TableSource + "(";
            createTable += string.Join(",", from t in cols select t.Name + " " + t.Type + " " + (isMultiplePrimary ? string.Empty : t.Primary));
            if (isMultiplePrimary)
                createTable += ",PRIMARY KEY(" + string.Join(",", from t in cols where t.Primary.Length > 0 select t.Name) + ")";
            createTable += ")";
            Exec(createTable);
        }

        private (string Name, string Type, string Primary) GetElement(TableMapping map, KeyValuePair<string, string> path)
        {
            var element = map.DeclaringType.GetProperty(path.Key);
            var attribute = element.GetCustomAttributes(typeof(Data.DataColumnAttribute), false).OfType<DataColumnAttribute>().FirstOrDefault<DataColumnAttribute>();
            return (attribute.Name, Proxy.GetTypeFor(element) + " ", (attribute.IsPrimary ? "PRIMARY KEY" : string.Empty) + (attribute.IsPrimary && attribute.IsAuto ? " " + Proxy.Auto(attribute.IsAuto) : string.Empty));
        }
        public void Open()
        {
            string conn = DatabaseSettings.Instance.ConnectionString;

            my_cn = Proxy.Connect(conn);
            my_cn.Open();

        }

        public static TableMapping GetMapping(Type type)
        {
            var attr = type.GetCustomAttribute<DataSourceAttribute>();

            if (attr == null)
                return null;
            return new TableMapping(type, attr.TableName);
        }

        public int Delete<T>(T user)
        {
            var source = typeof(T).GetCustomAttribute<DataSourceAttribute>();
            Dictionary<string, object> vals = new Dictionary<string, object>();
            if (source.IsTable)
            {
                var tableName = source.TableName;
                var cmd = CreateDeleteCommand(tableName, GetWhereByID<T>());
                var count = 1;
                foreach (var pke in typeof(T).GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>()?.IsPrimary == true))
                {
                    AddCmdParam(cmd, "@pkey" + (count++), GetMemberValue(user, pke));
                }

                return cmd.ExecuteNonQuery();
            }
            return -1;
        }

        private DatabaseProxyCommand CreateDeleteCommand(string tableName, string additionnalWhere)
        {
            var cmd = CreateCommand("");
            var requete = "DELETE FROM " + tableName;

            if (!string.IsNullOrWhiteSpace(additionnalWhere))
            {
                requete += " WHERE " + additionnalWhere;
            }
            cmd.CommandText = requete;
            return cmd;
        }

        public SqlBuilder QueryBuilder { get { return new SqlBuilder() { Provider = Proxy.Provider }; } }

        public DatabaseProxyCommand CreateCommand(string cmdText, params object[] v)
        {
            var my_cmd = my_cn?.CreateCommand();

            var cmd = (DatabaseProxyCommand)my_cmd;
            cmd.CommandText = cmdText;

            int count = 1;
            foreach (var param in v)
            {
                var p = cmd.CreateParameter();
                p.Value = param;
                p.ParameterName = "@pu" + count;
                if (param == null)
                    p.Value = DBNull.Value;
                if (cmdText.Contains(p.ParameterName))
                    cmd.Parameters.Add(p);
                count++;
            }
            return cmd;
        }

        public void Close()
        {
            my_cn?.Close();
        }
        public bool IsOpen
        {
            get
            {
                return my_cn?.IsOpen() ?? false;
            }
        }
        public dynamic ExecScalar(SqlBuilder sql, params object[] param)
        {
            return ExecScalar(sql.SQL, param);
        }
        public dynamic ExecScalar(string sql, params object[] param)
        {
            var cmd = CreateCommand(sql, param);
            cmd.CommandText = sql;
            return cmd.ExecuteScalar();
        }
        public int Exec(SqlBuilder sql, params object[] param)
        {
            return Exec(sql.SQL, param);
        }
        public int Exec(string sql, params object[] param)
        {
            try
            {
                var cmd = CreateCommand(sql, param);

                return cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {
                return -1;
            }
        }

        public IEnumerable<R> WeakQuery<R>(TableQuery<R> tableQuery, string dbCommand)
        {
            return new WeakQueryEnumeratorWithSelector<R>(tableQuery, dbCommand);
        }

        public IEnumerable<T> Query<T>(SqlBuilder sql)
        {
            return Query<T>(sql.SQL);
        }
        public IEnumerable<T> Query<T>(string sql)
        {
            var cmd = CreateCommand(sql);
            var reader = cmd.ExecuteReader();

            Dictionary<string, int> columns = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);

            for (var i = 0; i < reader.FieldCount; i++)
                columns[reader.GetName(i)] = i;

            while (reader.Read())
            {
                T obj = CreateObject<T>();
                object j = obj;
                SetValues(reader, columns, ref j, typeof(T));
                obj = (T)j;
                yield return obj;
            }
            reader.Close();

            yield break;
        }
        public IEnumerable<T> WeakQuery<T>(TableQuery<T> cmd)
        {
            System.Diagnostics.Debug.WriteLine("Creating weak query for:" + cmd.SQL + "\r\n\r\n\r\n");
            WeakQueryEnumerator<T> wqe = new WeakQueryEnumerator<T>(cmd);
            return wqe;
        }
        public IEnumerable<T> Query<T>(DatabaseProxyCommand cmd)
        {
            System.Diagnostics.Debug.WriteLine("Executing SQL:" + cmd.CommandText + "\r\n\r\n\r\n");

            var reader = cmd.ExecuteReader();

            Dictionary<string, int> columns = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);

            for (var i = 0; i < reader.FieldCount; i++)
                columns[reader.GetName(i)] = i;

            while (reader.Read())
            {
                T obj = CreateObject<T>();
                object j = obj;
                SetValues(reader, columns, ref j, typeof(T));
                obj = (T)j;
                yield return obj;
            }
            reader.Close();

            yield break;
        }
        public IEnumerable<object[]> Query(DatabaseProxyCommand cmd)
        {
            System.Diagnostics.Debug.WriteLine("Executing SQL:" + cmd.CommandText + "\r\n\r\n\r\n");

            var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                object[] obj = new object[reader.FieldCount];
                reader.GetValues(obj);
                yield return obj;
            }
            reader.Close();

            yield break;
        }
        public IEnumerable<T> Query<T>(DatabaseProxyCommand cmd, TableMapping mapping)
        {
            if (mapping.NameForPath.Count == 0)
            {
                foreach (var e in Query<T>(cmd))
                    yield return e;
                yield break;
            }
            System.Diagnostics.Debug.WriteLine("Executing SQL:" + cmd.CommandText + "\r\n\r\n\r\n");

            var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                T obj = CreateObject<T>();
                object j = obj;
                SetValues(reader, mapping, ref j, typeof(T));
                obj = (T)j;
                yield return obj;
            }
            reader.Close();

            yield break;
        }
        public T QueryOneOrDefault<T>(DatabaseProxyCommand cmd)
        {
            //System.Diagnostics.Debug.WriteLine("Executing SQL:" + cmd.CommandText + "\r\n\r\n\r\n");

            var reader = cmd.ExecuteReader();

            Dictionary<string, int> columns = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);

            for (var i = 0; i < reader.FieldCount; i++)
                columns[reader.GetName(i)] = i;

            while (reader.Read())
            {
                T obj = CreateObject<T>();
                object j = obj;
                SetValues(reader, columns, ref j, typeof(T));
                obj = (T)j;
                reader.Close();
                return obj;
            }
            reader.Close();
            return default;
        }
        public T QueryOneOrDefault<T>(DatabaseProxyCommand cmd, TableMapping mapping)
        {
            if (mapping.NameForPath.Count == 0)
            {
                return QueryOneOrDefault<T>(cmd);
            }
            //System.Diagnostics.Debug.WriteLine("Executing SQL:" + cmd.CommandText + "\r\n\r\n\r\n");

            var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                T obj = CreateObject<T>();
                object j = obj;
                SetValues(reader, mapping, ref j, typeof(T));
                obj = (T)j;
                reader.Close();
                return obj;
            }
            reader.Close();
            return default;
        }
        private const BindingFlags FieldFlags = BindingFlags.NonPublic | BindingFlags.Instance;

        private static readonly string[] BackingFieldFormats = { "<{0}>i__Field", "<{0}>" };

        private void SetValues(DbDataReader reader, TableMapping mapping, ref object obj, Type t)
        {
            string[] locations;
            if (mapping.NameForPath.Count == 0)
                locations = new string[] { "_" };
            else locations = mapping.NameForPath.Select((x) => x.Key).ToArray();

            for (var i = 0; i < locations.Length; i++)
            {
                var value = reader[i];
                SetValueAt(ref obj, locations[i].Split('.'), value, t);
            }
        }

        private void SetValueAt(ref object obj, IEnumerable<string> v, object value, Type t)
        {
            var prop = t.GetProperty(v.First());
            if (v.Count() == 1)
            {
                if (v.First() == "_")
                    obj = value;
                else if (prop.SetMethod != null)
                    prop.SetValue(obj, ConvertDbToObject(value, prop.PropertyType));
                else
                {
                    var backingFieldNames = BackingFieldFormats.Select(x => string.Format(x, prop.Name)).ToList();
                    var fi = t
                        .GetFields(FieldFlags)
                        .FirstOrDefault(f => backingFieldNames.Contains(f.Name));
                    fi.SetValue(obj, ConvertDbToObject(value, prop.PropertyType));
                }
            }
            else
            {
                var val = prop.GetValue(obj);
                SetValueAt(ref val, v.Skip(1), value, prop.PropertyType);
            }
        }

        private void SetValues(DbDataReader reader, Dictionary<string, int> columns,ref object obj, Type t)
        {
            var properties = t.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).Where((x) => x.CustomAttributes.Any((a) => a.AttributeType == typeof(DataColumnAttribute)))
               .Select((f) => { return new Tuple<PropertyInfo, DataColumnAttribute>(f, (DataColumnAttribute)Attribute.GetCustomAttribute(f, typeof(DataColumnAttribute))); }).ToList();

            if (t.IsJoin())
            {
                object o = t.GetProperty("Left").GetValue(obj);
                object o2 = t.GetProperty("Right").GetValue(obj);
                SetValues(reader, columns, ref o, t.GetProperty("Left").PropertyType);
                SetValues(reader, columns, ref o2, t.GetProperty("Right").PropertyType);
            }
            if (t.IsAnonymous())
            {
                foreach (var property in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (property.PropertyType.GetCustomAttribute<DataSourceAttribute>() != null)
                    {
                        var o = property.GetValue(obj);
                        SetValues(reader, columns,ref o, property.PropertyType);
                    }
                    else
                    {
                        if (columns.ContainsKey(property.Name))
                        {
                            var backingFieldNames = BackingFieldFormats.Select(x => string.Format(x, property.Name)).ToList();
                            var fi = t
                                .GetFields(FieldFlags)
                                .FirstOrDefault(f => backingFieldNames.Contains(f.Name));
                            var value = reader.GetValue(columns[property.Name]);
                            fi.SetValue(obj, ConvertDbToObject(value, property.PropertyType));
                        }
                    }
                }
            }
            else if (columns.Count == 1 && columns.ContainsKey("_"))
            {
               obj= reader.GetValue(0);
            }
            else
                foreach (var property in properties)
                {
                    if (columns.ContainsKey(property.Item2.ColumnName))
                    {
                        var value = reader.GetValue(columns[property.Item2.ColumnName]);
                        if (value != DBNull.Value)
                            property.Item1.SetValue(obj, ConvertDbToObject(value, property.Item1.PropertyType));
                    }
                }
        }

        private static T CreateObject<T>()
        {
            return (T)CreateObject(typeof(T));
        }
        private static object CreateObject(Type t)
        {
            if (t.IsAnonymous())
            {
                var ctor = t.GetConstructors().First();
                var param = ctor.GetParameters();
                var arrayObjs = param.Select((x) => CreateObject(x.ParameterType)).ToArray();
                return ctor.Invoke(arrayObjs);
            }
            if (t.IsJoin())
            {
                object obj = Activator.CreateInstance(t);
                t.GetProperty("Left").SetValue(obj, CreateObject(t.GenericTypeArguments[0]));
                t.GetProperty("Right").SetValue(obj, CreateObject(t.GenericTypeArguments[1]));
                return obj;
            }
            if (t == typeof(string))
                return "";
            return FormatterServices.GetUninitializedObject(t);
        }
        public R ConvertDatabaseObjectToCLRObject<R>(object value)
        {
            return (R)ConvertDatabaseObjectToCLRObject(value, typeof(R));
        }
        public object ConvertDatabaseObjectToCLRObject(object value, Type outputType)
        {
            return ConvertDbToObject(value, outputType);
        }
        private object ConvertDbToObject(object value, Type fieldType)
        {
            if (value == DBNull.Value)
                return null;
            if (fieldType == typeof(DateTime))
                return Convert.ToDateTime(value, new CultureInfo("en-US"));
            var convertMethod = typeof(Convert).GetMethod("To" + fieldType.Name, new Type[] { value.GetType() });
            if (convertMethod != null)
                return convertMethod.Invoke(null, new object[] { value });
            return value;
        }
        public void AddCmdParam(DatabaseProxyCommand cmd, string name, object val)
        {
            DatabaseProxyCommand my_cmd = cmd;
            //Mysql
            if (my_cmd?.Parameters.Contains(name) ?? false)
                my_cmd.Parameters.RemoveAt(name);

            my_cmd?.Parameters.AddWithValue(name, val);
        }

        public int Update(string table, Dictionary<string, object> values, string additionnalWhere = "")
        {
            var cmd = CreateUpdateCommand(table, values, additionnalWhere);
            return cmd.ExecuteNonQuery();
        }
        public DatabaseProxyCommand CreateUpdateCommand(string table, Dictionary<string, object> values, string additionnalWhere = "")
        {
            var cmd = CreateCommand("");
            var requete = "UPDATE " + table + " SET ";
            int pnum = 0, rc = 0;
            string valuesSetter = string.Empty;
            foreach (var p in values)
            {
                if (p.Value != null)
                {
                    if (rc > 0)
                        valuesSetter += ",";

                    rc++;
                    pnum++;
                    AddCmdParam(cmd, "@p" + pnum, p.Value);

                    valuesSetter += p.Key + " =  @p" + pnum;
                }
            }
            requete += valuesSetter;
            if (!string.IsNullOrWhiteSpace(additionnalWhere))
            {
                requete += " WHERE " + additionnalWhere;
            }
            cmd.CommandText = requete;
            return cmd;
        }
        public long Insert(string table, Dictionary<string, object> values, bool ignore = false, bool forceIdentify = false)
        {
            var cmd = CreateCommand("");
            var requete = "INSERT " + ((ignore) ? "IGNORE " : " ") + " INTO " + table + "(";

            if (Proxy.Provider == DBProvider.MSSQL && !forceIdentify)
            {
                DataTable schema = new DataTable();
                cmd.CommandText = "select * from " + table;
                schema.Load(cmd.ExecuteReader(CommandBehavior.SchemaOnly));
                foreach (DataColumn dc in schema.Columns)
                {
                    if (dc.AutoIncrement)
                        if (values.ContainsKey(dc.ColumnName))
                            values.Remove(dc.ColumnName);
                }
            }

            int pnum = 0, rc = 0;
            string valuesSetter = " values(";
            foreach (var p in values)
            {
                if (p.Value != null)
                {
                    if (rc > 0)
                        valuesSetter += ",";
                    if (rc > 0)
                        requete += ",";
                    rc++;
                    pnum++;
                    AddCmdParam(cmd, "@p" + pnum, p.Value);
                    requete += p.Key;
                    valuesSetter += "@p" + pnum;
                }
            }
            requete += ")" + valuesSetter + ")";
            cmd.CommandText = requete;
            cmd.ExecuteNonQuery();
            return cmd.LastInsertedId;
        }

        public TableQuery<T> Query<T>()
        {
            return new TableQuery<T>(this);
        }
        public int Update<T>(T val, bool setUpdateFlag = true)
        {
            var source = typeof(T).GetCustomAttribute<DataSourceAttribute>();
            Dictionary<string, object> vals = new Dictionary<string, object>();
            if (source.IsTable)
            {
                var tableName = source.TableName;
                foreach (var member in typeof(T).GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>() != null))
                {
                    var attr = member.GetCustomAttribute<DataColumnAttribute>();
                    if (!attr.IgnoreOnSave)
                        vals[attr.Name] = GetMemberValue(val, member);
                    if (setUpdateFlag && member.GetCustomAttribute<OnUpdateAttribute>() != null)
                        vals[attr.Name] = DateTime.Now;
                }
                var cmd = CreateUpdateCommand(tableName, vals, GetWhereByID<T>());
                var count = 1;
                foreach (var pke in typeof(T).GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>()?.IsPrimary == true))
                {
                    AddCmdParam(cmd, "@pkey" + (count++), GetMemberValue(val, pke));
                }
                return cmd.ExecuteNonQuery();
            }
            else
                return 0;
        }
        public int Update<T>(IEnumerable<T> vals, bool setUpdateFlag = true)
        {
            return vals.Sum((v) => Update(v,setUpdateFlag));
        }
        public List<long> InsertAll<T>(IEnumerable<T> ts, bool ignore = false, bool forceIdentify = false, bool setUpdateFlag = true)
        {
            List<long> ids = new List<long>();
            if (ts != null)
                foreach (var t in ts)
                    ids.Add(Insert(t, ignore, forceIdentify, setUpdateFlag));
            return ids;
        }
        public long Insert<T>(T val, bool ignore = false, bool forceIdentity = false, bool setUpdateFlag=true)
        {
            var source = typeof(T).GetCustomAttribute<DataSourceAttribute>();
            Dictionary<string, object> vals = new Dictionary<string, object>();
            if (source.IsTable)
            {
                var tableName = source.TableName;

                foreach (var member in typeof(T).GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>() != null))
                {
                    var attr = member.GetCustomAttribute<DataColumnAttribute>();
                    if (!attr.IgnoreOnSave)
                        vals[attr.Name] = GetMemberValue(val, member);
                    if (setUpdateFlag && member.GetCustomAttribute<OnUpdateAttribute>() != null)
                        vals[attr.Name] = DateTime.Now;
                }
                return Insert(tableName, vals, ignore, forceIdentity);
            }
            else
                return -1;
        }

        public T GetByID<T>(string tableName, object value)
        {
            Type t = typeof(T);
            var primaryKeyElement = t.GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>()?.IsPrimary == true).Select((x) => x.GetCustomAttribute<DataColumnAttribute>()).FirstOrDefault();
            if (primaryKeyElement == null)
                throw new InvalidOperationException("No primary key is defined");
            var cmd = CreateCommand(QueryBuilder.SelectAll.From(tableName).Where(primaryKeyElement.ColumnName + "=@pkey").SQL, value);
            AddCmdParam(cmd, "@pkey", value);
            var resp = Query<T>(cmd).ToList().FirstOrDefault();
            cmd.Dispose();
            return resp;
        }
        public T GetByID<T>(object value)
        {
            Type t = typeof(T);
            var tableName = typeof(T).GetCustomAttribute<DataSourceAttribute>().TableName;
            var primaryKeyElement = t.GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>()?.IsPrimary == true).Select((x) => x.GetCustomAttribute<DataColumnAttribute>()).FirstOrDefault();
            if (primaryKeyElement == null)
                throw new InvalidOperationException("No primary key is defined");
            var cmd = CreateCommand(QueryBuilder.SelectAll.From(tableName).Where(primaryKeyElement.ColumnName + "=@pkey").SQL);
            AddCmdParam(cmd, "@pkey", value);
            var resp = Query<T>(cmd).ToList().FirstOrDefault();
            cmd.Dispose();
            return resp;
        }
        public string GetWhereByID<T>()
        {
            Type t = typeof(T);
            int count = 0;
            string where = "";
            foreach (var pke in t.GetMembers().Where((x) => x.GetCustomAttribute<DataColumnAttribute>()?.IsPrimary == true).Select((x) => x.GetCustomAttribute<DataColumnAttribute>()))
            {
                count++;
                if (count > 1)
                    where += " AND ";
                where += pke.ColumnName + " = @pkey" + count;
            }
            if (count == 0)
                throw new InvalidOperationException("No primary key is defined");
            return where;
        }
        public static object GetMemberValue(object obj, MemberInfo member)
        {
            if (member.MemberType == MemberTypes.Property)
            {
                var m = (PropertyInfo)member;
                return m.GetValue(obj, null);
            }
            if (member.MemberType == MemberTypes.Field)
            {
                var m = (FieldInfo)member;
                return m.GetValue(obj);
            }
            throw new NotSupportedException("MemberExpr: " + member.MemberType);
        }

    }
}
