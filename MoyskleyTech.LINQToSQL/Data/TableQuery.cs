using MoyskleyTech.LINQToSQL.Data.Tables;
using MoyskleyTech.LINQToSQL.Helper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MoyskleyTech.LINQToSQL.Data
{
    //[DebuggerStepThrough]
    public abstract class BaseTableQuery
    {
        public TableMapping Table { get; protected set; }
        public MappingContext ParentContext { get; set; }
        public abstract string GetSourceSQL();
        public abstract void BuildSourceSQL(List<string> lst);

        public abstract DatabaseProxyCommand GenerateCommand(string selectionList);
        public abstract string GenerateInnerSQL(string selectionList, List<object> args);

        public abstract bool Any();
        public abstract long Count();
        public abstract int Delete();

        protected abstract BaseTableQuery ApplyTake(int i);
        protected abstract BaseTableQuery ApplySkip(int i);
        public class Ordering
        {
            public string ColumnName { get; set; }

            public bool Ascending { get; set; }
        }
        public class SelectedValue
        {
            public string ColumnName { get; set; }

            public Expression Code { get; set; }
        }
    }
    //[DebuggerStepThrough]
    public class TableQuery<T> : BaseTableQuery, IEnumerable<T>
    {
        private BaseTableQuery _joinInner;
        private Expression _joinInnerKeySelector;
        private BaseTableQuery _joinOuter;
        private Expression _joinOuterKeySelector;
        private Expression _joinSelector;
        private int? _limit;
        private int? _offset;
        private List<Ordering> _orderBys;
        private Expression _where;
        private List<SelectedValue> _selects = new List<SelectedValue>();
        private bool _distinct;

        private TableQuery(ContextIndependantDatabase conn, TableMapping table)
        {
            Connection = conn;
            Table = table;
        }


        public TableQuery(ContextIndependantDatabase conn)
        {
            Connection = conn;
            Table = ContextIndependantDatabase.GetMapping(typeof(T));
        }


        public ContextIndependantDatabase Connection { get; private set; }

        public IEnumerator<T> GetEnumerator()
        {
            return Connection.WeakQuery(this).GetEnumerator();
        }


        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public Data.SQL SQL => Data.SQL.FromString(GenerateSQL("*", new List<object>()));

        public List<T> ToList()
        {
            var cmd = GenerateCommand("*");
            var lst = Connection.Query<T>(cmd, Table.With(_selects)).ToList();
            cmd.Dispose();
            return lst;
        }

        public T[] ToArray()
        {
            var cmd = GenerateCommand("*");
            var lst = Connection.Query<T>(cmd, Table.With(_selects)).ToArray();
            cmd.Dispose();
            return lst;
        }

        public TableQuery<U> Clone<U>()
        {
            return new TableQuery<U>(Connection, Table)
            {
                _where = _where,
                _limit = _limit,
                _offset = _offset,
                _joinInner = _joinInner,
                _joinInnerKeySelector = _joinInnerKeySelector,
                _joinOuter = _joinOuter,
                _joinOuterKeySelector = _joinOuterKeySelector,
                _joinSelector = _joinSelector,
                _orderBys = _orderBys == null ? null : new List<Ordering>(_orderBys),
                _selects = _selects == null ? null : new List<SelectedValue>(_selects),
                _distinct = _distinct
            };
        }

        internal DatabaseProxyCommand GetCommand(string selector)
        {
            return GenerateCommand(selector);
        }

        public int? Limit => _limit;

        public TableQuery<T> Where(Expression<Func<T, bool>> predExpr)
        {
            if (predExpr == null)
            {
                throw new ArgumentNullException("predExpr");
            }
            if (predExpr.NodeType != ExpressionType.Lambda)
            {
                throw new NotSupportedException("Must be a predicate");
            }
            var lambda = (LambdaExpression)predExpr;
            var pred = lambda.Body;
            var q = Clone<T>();
            q.AddWhere(pred);
            return q;
        }
        public TableQuery<T> Where(SQL predExpr)
        {
            if (predExpr == null)
            {
                throw new ArgumentNullException("predExpr");
            }
            var pred = Expression.Constant(predExpr);
            var q = Clone<T>();
            q.AddWhere(pred);
            return q;
        }

        public TableQuery<T> Take(int n)
        {
            var q = Clone<T>();

            // If there is already a limit then the limit will be the minimum
            // of the current limit and n.
            q._limit = Math.Min(q._limit ?? int.MaxValue, n);
            return q;
        }


        public override int Delete()
        {
            if (_limit != null)
            {
                //SQLite provides a limit to deletions so this would be possible to implement in the future
                //You would need to take care that the correct order was being applied.
                throw new NotSupportedException("Cannot delete if a limit has been specified");
            }
            if (_offset != null)
            {
                throw new NotSupportedException("Cannot delete if an offset has been specified");
            }

            var args = new List<object>();

            var cmdText = "delete from " + Table.TableSource + "";
            if (_where != null)
            {
                var w = CompileExpr(_where, args, new MappingContext(Table, ParentContext));
                cmdText += " where " + w.CommandText;
            }
            var command = Connection.CreateCommand(cmdText, args.ToArray());

            var result = command.ExecuteNonQuery();
            return result;
        }


        public TableQuery<T> Skip(int n)
        {
            var q = Clone<T>();

            q._offset = n + (q._offset ?? 0);

            if (q._limit != null)
            {
                q._limit -= n;
                if (q._limit < 0)
                    q._limit = 0;
            }
            return q;
        }


        public T ElementAt(int index)
        {
            return Skip(index).Take(1).First();
        }
        public T ElementAtOrDefault(int index)
        {
            return Skip(index).Take(1).FirstOrDefault();
        }

        [Obsolete]
        public IEnumerable<object[]> Select(Expression<Func<T, Object[]>> selector)
        {
            LambdaExpression lambdaExpression = selector as LambdaExpression;
            var body = lambdaExpression.Body as NewArrayExpression;

            var columns = body.Expressions.Select((x) => CompileExpr(x, null, new MappingContext(Table, ParentContext)).CommandText).ToList();

            return Connection.Query(GenerateCommand(string.Join(",", columns)));
        }
        public TableQuery<R> Select<R>(Expression<Func<T, R>> selector)
        {
            var t = typeof(R);

            //throw new InvalidOperationException("Selector must yield DatabaseSource or anonymous type");

            var join = new TableMapping();
            join.DeclaringType = t;
            join.TableSource = Table.TableSource;
            join.Name = Table.Name;

            var ret = Clone<R>();
            if (_where != null)
                ret._where = Expression.Block(Expression.Constant(new MappingContext(Table, ParentContext)), _where);
            ret.Table = join;
            if (IsDatabaseObject(t))
            {
                join.Build();
            }
            else if (selector is LambdaExpression lamb)
            {
                var paramNames = lamb.Parameters.Select((x) => x.Name).ToList();
                if (lamb.Body is NewExpression newExpr)
                {
                    for (var i = 0; i < newExpr.Arguments.Count; i++)
                    {
                        var arg = newExpr.Arguments[i];

                        if (!arg.Type.IsJoin() && (arg.Type.GetCustomAttribute<DataSourceAttribute>() == null) && arg.NodeType != ExpressionType.Parameter)
                        {
                            var nm = newExpr.Type.GetProperties().ElementAt(i).Name;
                            if (!(arg is MemberExpression mem && mem.Member.DeclaringType.IsAnonymous()))
                                ret.AddSelect(nm, Expression.Block(Expression.Constant(new MappingContext(Table, ParentContext)), arg));
                        }

                        var queryToBuildArgument = newExpr.Arguments[i];

                        var tbl = Find(queryToBuildArgument, paramNames);

                        var propName = newExpr.Type.GetProperties().ElementAt(i).Name;

                        var extraction = Extract(queryToBuildArgument);
                        foreach (var val in extraction)
                        {
                            var tblToJoin = Table;
                            foreach (var kv in tblToJoin.NameForPath)
                            {
                                var key = kv.Key;
                                if (key == (val.PathUsed))
                                {
                                    //join.NameForPath.Add(propName, kv.Value);
                                    //ret.AddSelect(propName,)
                                }
                                else if (key.StartsWith(val.PathUsed))
                                {
                                    key = key.Substring(val.PathUsed.Length);
                                    if (key.StartsWith("."))
                                        key = key.Substring(1);

                                    if (key.Length > 0)
                                        if (!propName.Contains("TransparentIdentifier"))
                                            join.NameForPath.Add(propName + "." + key, kv.Value);
                                        else
                                            join.NameForPath.Add(key, kv.Value);
                                }
                            }
                        }

                    }
                }
                else if (lamb.Body is MemberExpression me)
                {
                    var extraction = Extract(me);
                    foreach (var val in extraction)
                    {
                        var tblToJoin = Table;

                        foreach (var kv in tblToJoin.NameForPath)
                        {
                            var key = kv.Key;
                            if (key == (val.PathUsed))
                            {
                                //join.NameForPath.Add("_", kv.Value);
                                ret.AddSelect("_", Expression.Variable(me.Member.DeclaringType, kv.Value));
                                //ret.AddSelect(propName,)
                            }
                            else if (key.StartsWith(val.PathUsed))
                            {
                                key = key.Substring(val.PathUsed.Length);
                                if (key.StartsWith("."))
                                    key = key.Substring(1);

                                if (key.Length > 0)
                                    join.NameForPath.Add(key, kv.Value);
                            }
                        }
                    }
                }
                else 
                {
                    var extraction = Extract(lamb.Body);

                    ret.AddSelect("_", Expression.Block(Expression.Constant(new MappingContext(Table, ParentContext)), lamb.Body));
                    //join.NameForPath.Add("_", "0");
                }
            }
            return ret;
        }

        private void AddSelect(string name, Expression arg)
        {
            if (_selects == null)
                _selects = new List<SelectedValue>();
            _selects.Add(new SelectedValue() { Code = arg, ColumnName = name });
        }

        public IEnumerable<R> Select<R>(string selector)
        {
            return Connection.WeakQuery<R>(Clone<R>(), selector);
        }

        public TableQuery<T> OrderBy(SQL predExpr)
        {
            return AddOrderBy((predExpr), true);
        }
        public TableQuery<T> OrderBy<TValue>(Expression<Func<T, TValue>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        public TableQuery<T> OrderByDescending(SQL predExpr)
        {
            return AddOrderBy((predExpr), false);
        }
        public TableQuery<T> OrderByDescending<TValue>(Expression<Func<T, TValue>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        public TableQuery<T> ThenBy(SQL predExpr)
        {
            return AddOrderBy((predExpr), true);
        }
        public TableQuery<T> ThenBy<TValue>(Expression<Func<T, TValue>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        public TableQuery<T> ThenByDescending(SQL predExpr)
        {
            return AddOrderBy((predExpr), false);
        }
        public TableQuery<T> ThenByDescending<TValue>(Expression<Func<T, TValue>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }
        private TableQuery<T> AddOrderBy(SQL orderExpr, bool asc)
        {
            var q = Clone<T>();
            if (q._orderBys == null)
            {
                q._orderBys = new List<Ordering>();
            }

            q._orderBys.Add(new Ordering
            {
                ColumnName = orderExpr.Command,
                Ascending = asc
            });
            return q;
        }
        private TableQuery<T> AddOrderBy(Expression orderExpr, bool asc)
        {
            if (orderExpr == null)
            {
                throw new ArgumentNullException("orderExpr");
            }
            if (orderExpr.NodeType != ExpressionType.Lambda)
            {
                throw new NotSupportedException("Must be a predicate");
            }
            var lambda = (LambdaExpression)orderExpr;

            MemberExpression mem;

            var unary = lambda.Body as UnaryExpression;
            if (unary != null && unary.NodeType == ExpressionType.Convert)
            {
                mem = unary.Operand as MemberExpression;
            }
            else
            {
                mem = lambda.Body as MemberExpression;
            }

            var cmdText = CompileExpr(mem, null, new MappingContext(Table, ParentContext));

            //if (mem == null || (mem.Expression.NodeType != ExpressionType.Parameter))
            //{
            //    throw new NotSupportedException("Order By does not support: " + orderExpr);
            //}
            var q = Clone<T>();
            if (q._orderBys == null)
            {
                q._orderBys = new List<Ordering>();
            }

            q._orderBys.Add(new Ordering
            {
                ColumnName = cmdText.CommandText,
                Ascending = asc
            });
            return q;
        }

        private void AddWhere(Expression pred)
        {
            if(!(pred.Type.Is(typeof(SQL))||pred.Type == typeof(bool)))
                throw new InvalidOperationException("pred");
            if (pred == null)
            {
                throw new ArgumentNullException("pred");
            }
            if (_limit != null || _offset != null)
            {
                throw new NotSupportedException("Cannot call where after a skip or a take");
            }

            if (_where == null)
            {
                _where = pred;
            }
            else
            {
                _where = Expression.AndAlso(_where, pred);
            }
        }


        public TableQuery<Join<TInner, T>> Join<TInner, TKey>(
            TableQuery<TInner> inner,
            Expression<Func<T, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector)
        {
            var q = new TableQuery<Join<TInner, T>>(Connection, CreateJoin(Table, inner.Table))
            {
                _joinOuter = this,
                _joinOuterKeySelector = outerKeySelector,
                _joinInner = inner,
                _joinInnerKeySelector = innerKeySelector
            };
            if (_where != null)
                q.AddWhere(Expression.Block(Expression.Constant(new MappingContext(Table, ParentContext)), _where));
            if (inner._where != null)
                q.AddWhere(Expression.Block(Expression.Constant(inner.Table), inner._where));
            q._selects = _selects.Concat(inner._selects).ToList();
            return q;
        }

        private TableMapping CreateJoin(TableMapping left, TableMapping rights)
        {
            return TableMapping.Join(left, rights);
        }
        private TableMapping CreateJoin(TableMapping left, TableMapping right, Expression selector)
        {
            LambdaExpression lambdaExpression = (LambdaExpression)selector;
            var paramNames = lambdaExpression.Parameters.Select((x) => x.Name).ToList();

            var body = lambdaExpression.Body;
            if (body is NewExpression newExpr)
            {
                TableMapping join = new TableMapping();
                join.DeclaringType = lambdaExpression.Type;
                if (join.DeclaringType.Is(typeof(Func<,,>)))
                {
                    join.DeclaringType = join.DeclaringType.GetGenericArguments()[2];
                }
                join.TableSource = left.TableSource;
                join.Name = left.Name;
                //foreach (var kv in left.NameForPath)
                //    join.NameForPath.Add(kv.Key, kv.Value);
                //foreach (var kv in right.NameForPath)
                //    join.NameForPath.Add(kv.Key, kv.Value);

                for (var i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var queryToBuildArgument = newExpr.Arguments[i];

                    var tbl = Find(queryToBuildArgument, paramNames);

                    var propName = newExpr.Type.GetProperties().ElementAt(i).Name;

                    var extraction = Extract(queryToBuildArgument);

                    foreach (var val in extraction)
                    {
                        var tblToJoin = (val.UsedParam == paramNames[0]) ? left : right;
                        foreach (var kv in tblToJoin.NameForPath)
                        {
                            var key = kv.Key;
                            if (key.StartsWith(val.PathUsed))
                            {
                                key = key.Substring(val.PathUsed.Length);
                                if (key.StartsWith("."))
                                    key = key.Substring(1);
                                join.NameForPath.Add(propName + "." + key, kv.Value);
                            }
                            //join.NameForPath.Add(propName + "." + kv.Key.Substring(val.PathUsed.Length), kv.Value);
                        }
                    }
                    //if(tbl ==0)
                    //     foreach (var kv in left.NameForPath)
                    //         join.NameForPath.Add(propName+"."+kv.Key, kv.Value);
                    //else if(tbl==1)
                    //    foreach (var kv in right.NameForPath)
                    //        join.NameForPath.Add(propName + "." + kv.Key, kv.Value);

                }
                return join;
            }
            else if (body is ParameterExpression p)
            {
                var idx = paramNames.IndexOf(p.Name);
                if (idx >= 0)
                {
                    return new TableMapping[] { left, right }[idx];
                }
            }
            else if (body is MemberExpression mexpr)
            {
                var member = mexpr.Member;
                var type = member.DeclaringType;
                var map = (type == left.DeclaringType) ? left : right;
                TableMapping join = new TableMapping();
                join.DeclaringType = lambdaExpression.Type;
                join.TableSource = left.TableSource;
                join.Name = left.Name;

                join.NameForPath = new Dictionary<string, string>();
                foreach (var kp in map.NameForPath)
                {
                    if (kp.Key.StartsWith(member.Name + "."))
                    {
                        join.NameForPath[kp.Key.Substring(member.Name.Length + 1)] = kp.Value;
                    }
                }

                return join;

            }
            throw new InvalidOperationException("Could only join on anonymous type");
        }
        class ExtractResult
        {
            public string UsedParam { get; set; }
            public string PathUsed { get; set; }
        }
        private List<ExtractResult> Extract(Expression queryToBuildArgument)
        {
            var lst = new List<ExtractResult>();

            if (queryToBuildArgument is ParameterExpression par)
                lst.Add(new ExtractResult() { PathUsed = "", UsedParam = par.Name });
            if (queryToBuildArgument is BinaryExpression bin)
            {
                lst.AddRange(Extract(bin.Left));
                lst.AddRange(Extract(bin.Right));
            }
            if (queryToBuildArgument is UnaryExpression expr)
            {
                lst.AddRange(Extract(expr.Operand));
            }
            if (queryToBuildArgument is MemberExpression)
            {
                string parameter = null;
                var pathUsed = FindPath(queryToBuildArgument, ref parameter).Substring(1);
                while (pathUsed.Contains("TransparentIdentifier") && pathUsed.Contains('.'))
                {
                    pathUsed = pathUsed.Substring(pathUsed.IndexOf(".") + 1);
                }
                if (parameter != null)
                {
                    lst.Add(new ExtractResult() { PathUsed = pathUsed, UsedParam = parameter });
                }
            }
            return lst;
        }
        private string FindPath(Expression expr, ref string parameter)
        {
            if (expr is MemberExpression mem)
                return FindPath(mem.Expression, ref parameter) + "." + mem.Member.Name;
            if (expr is ParameterExpression par)
            {
                parameter = par.Name;
                return "";
            }
            return "?";
        }

        private int Find(Expression queryToBuildArgument, List<String> paramNames)
        {
            if (queryToBuildArgument is ParameterExpression par)
                return paramNames.IndexOf(par.Name);
            if (queryToBuildArgument is BinaryExpression bin)
                return Math.Max(Find(bin.Left, paramNames), Find(bin.Right, paramNames));
            if (queryToBuildArgument is UnaryExpression expr)
                return Find(expr.Operand, paramNames);
            if (queryToBuildArgument is MemberExpression mem)
                return Find(mem.Expression, paramNames);
            return -1;
        }

        public TableQuery<Join<TInner, T>> Join<TInner, TKey>(
            TableQuery<TInner> inner,
            Expression<Func<T, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<T, TInner, Join<TInner, T>>> selector)
        {
            return Join(inner, outerKeySelector, innerKeySelector);
        }

        public TableQuery<R> Join<TInner, TKey, R>(
            TableQuery<TInner> inner,
            Expression<Func<T, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<T, TInner, R>> selector)
        {
            //if (!typeof(R).IsAnonymous())
            //{
            //    throw new InvalidOperationException("Could only select anonymous types");
            //}

            var q = new TableQuery<R>(Connection, CreateJoin(Table, inner.Table, selector))
            {
                _joinOuter = this,
                _joinOuterKeySelector = outerKeySelector,
                _joinInner = inner,
                _joinInnerKeySelector = innerKeySelector
            };
            if (_where != null)
                q.AddWhere(Expression.Block(Expression.Constant(new MappingContext(Table, ParentContext)), _where));
            if (inner._where != null)
                q.AddWhere(Expression.Block(Expression.Constant(inner.Table), inner._where));

            q._selects = _selects.Concat(inner._selects).ToList();


            return q;
        }
        public DatabaseProxyCommand GenerateCommand(string selectionList, List<object> args)
        {
            var cmdText = GenerateSQL(selectionList, args);
            return Connection.CreateCommand(cmdText, args.ToArray());
        }
        public override DatabaseProxyCommand GenerateCommand(string selectionList)
        {
            List<object> args = new List<object>();
            var cmdText = GenerateSQL(selectionList, args);
            return Connection.CreateCommand(cmdText, args.ToArray());
        }
        public override string GenerateInnerSQL(string selectionList, List<object> args)
        {
            var cmdText = GenerateSQL(selectionList, args);
            return cmdText;
        }

        public override string GetSourceSQL()
        {
            var lst = new List<string>();
            BuildSourceSQL(lst);
            return string.Join(" ", lst);
        }
        public override void BuildSourceSQL(List<string> lst)
        {
            if (_joinInner != null)
            {
                List<string> vs = new List<string>();
                if (_joinInnerKeySelector is LambdaExpression jinn && _joinOuterKeySelector is LambdaExpression jout)
                {
                    _joinOuter.BuildSourceSQL(lst);//Table name | Table name join X on e
                    lst.Add("join");
                    _joinInner.BuildSourceSQL(lst);//Table name | Table name join X on e
                    lst.Add("on");
                    lst.Add(CompileExpr(jout.Body, null, new MappingContext(_joinOuter.Table, ParentContext)).CommandText);
                    lst.Add("=");
                    lst.Add(CompileExpr(jinn.Body, null, new MappingContext(_joinInner.Table, ParentContext)).CommandText);
                }
            }
            else
                lst.Add(Table.TableSource + " " + Table.Name);
        }
        private string GenerateSQL(string selectionList, List<object> args)
        {
            string cmdText;
            string joinText = string.Empty;

            if (selectionList == null)
            {
                throw new ArgumentNullException("selectionList");
            }
            //if (Table.Object.IsJoin())// _joinInner != null && _joinOuter != null)
            //{
            //    if (_joinInnerKeySelector is LambdaExpression jinn && _joinOuterKeySelector is LambdaExpression jout)
            //    {
            //        joinText = " join " + _joinInner.Table.TableSource + " on " + ;
            //    }
            //    else
            //        throw new NotSupportedException("Joins are not supported.");
            //}

            if (selectionList == "*" && Table.NameForPath.Count > 0)
                selectionList = string.Join(",", Table.NameForPath.Values.Where((x) => !_selects.Any((y) => y.ColumnName == x)));

            if (selectionList != "count(*)" && _selects.Any())
            {
                if (selectionList != "*")
                    selectionList += ",";
                else
                    selectionList = "";
                selectionList += string.Join(",", _selects.Select((e) => CompileExpr(e.Code, args, new MappingContext(Table, ParentContext)).CommandText + " as " + e.ColumnName));
            }
            if (_distinct)
                selectionList = "distinct " + selectionList;
            cmdText = "select " + selectionList + " from " + GetSourceSQL();

            if (_where != null)
            {
                var w = CompileExpr(_where, args, new MappingContext(Table, ParentContext));
                cmdText += " where " + w.CommandText;
            }
            if ((_orderBys != null) && (_orderBys.Count > 0))
            {
                var t = string.Join(", ",
                    _orderBys.Select(o => "" + o.ColumnName + "" + (o.Ascending ? "" : " desc")).ToArray());
                cmdText += " order by " + t;
            }
            if (_limit.HasValue)
            {
                cmdText += " limit " + _limit.Value;
            }
            if (_offset.HasValue)
            {
                if (!_limit.HasValue)
                {
                    cmdText += " limit -1 ";
                }
                else
                    cmdText += " offset " + _offset.Value;
            }
            return cmdText;
        }

        private CompileResult CompileExpr(Expression expr, List<object> queryArgs, MappingContext ctx)
        {
            if (expr == null)
            {
                throw new NotSupportedException("Expression is NULL");
            }
            if (expr is BlockExpression blk)
                return CompileExpr(blk.Expressions[1], queryArgs, (MappingContext)((ConstantExpression)blk.Expressions[0]).Value);
            if (expr is BinaryExpression)
            {
                var bin = (BinaryExpression)expr;

                var leftr = CompileExpr(bin.Left, queryArgs, ctx);
                var rightr = CompileExpr(bin.Right, queryArgs, ctx);


                //If either side is a parameter and is null, then handle the other side specially (for "is null"/"is not null")
                string text;
                if (leftr.CommandText.StartsWith("@pu") && leftr.Value == null)
                {
                    text = CompileNullBinaryExpression(bin, rightr);
                }
                else if (rightr.CommandText.StartsWith("@pu") && rightr.Value == null)
                {
                    text = CompileNullBinaryExpression(bin, leftr);
                }
                else if (bin.NodeType == ExpressionType.Add && (bin.Left.Type == typeof(string) || bin.Right.Type == typeof(string)))
                {
                    text = "(CONCAT(" + leftr.CommandText + "," + rightr.CommandText + "))";
                }
                else
                {
                    text = "(" + leftr.CommandText + " " + GetSqlName(bin) + " " + rightr.CommandText + ")";
                }
                return new CompileResult
                {
                    CommandText = text
                };
            }
            if (expr.NodeType == ExpressionType.Not)
            {
                var operandExpr = ((UnaryExpression)expr).Operand;
                var opr = CompileExpr(operandExpr, queryArgs, ctx);
                var val = opr.Value;
                if (val is bool)
                {
                    val = !((bool)val);
                }
                return new CompileResult
                {
                    CommandText = "NOT(" + opr.CommandText + ")",
                    Value = val
                };
            }
            if (expr.NodeType == ExpressionType.Call)
            {
                var call = (MethodCallExpression)expr;
                var args = new CompileResult[call.Arguments.Count];

                if (call.Object != null && call.Object.Type.Is(typeof(TableQuery<>)) && call.Method.Name == "Any")
                    return new CompileResult() { CommandText = "(exists(" + ((BaseTableQuery)Execute(call.Object, ctx)).GenerateInnerSQL("*", queryArgs) + "))" };
                if (call.Object != null && call.Object.Type.Is(typeof(TableQuery<>)) && call.Method.Name == "Count")
                    return new CompileResult() { CommandText = "(" + ((BaseTableQuery)Execute(call.Object, ctx)).GenerateInnerSQL("count(*)", queryArgs) + ")" };
                if (call.Object != null && call.Object.Type.Is(typeof(TableQuery<>)) && call.Method.Name == "Get" && call.Arguments[0].Type == typeof(string))
                    return new CompileResult() { CommandText = "(" + ((BaseTableQuery)Execute(call.Object, ctx)).GenerateInnerSQL((string)Execute(call.Arguments[0], null), queryArgs) + ")" };

                if (call.Object != null && call.Object.Type.Is(typeof(TableQuery<>)) && call.Method.Name == "Get" && call.Arguments[0].Type.Is<Expression>())
                {
                    var btq = ((BaseTableQuery)Execute(call.Object, ctx));
                    return new CompileResult() { CommandText = "(" + btq.GenerateInnerSQL(CompileExpr(call.Arguments[0], queryArgs, new MappingContext(btq.Table, ctx)).CommandText, queryArgs) + ")" };
                }

                var obj = call.Object != null ? CompileExpr(call.Object, queryArgs, ctx) : null;

                for (var i = 0; i < args.Length; i++)
                {
                    args[i] = CompileExpr(call.Arguments[i], queryArgs, ctx);
                }

                var sqlCall = "";

                if (call.Method.Name == "Like" && args.Length == 2)
                {
                    sqlCall = "(" + args[0].CommandText + " like " + args[1].CommandText + ")";
                }
                else if (call.Method.Name == "Contains" && args.Length == 2)
                {
                    sqlCall = "(" + args[1].CommandText + " in " + args[0].CommandText + ")";
                }
                else if (call.Method.Name == "Contains" && args.Length == 1)
                {
                    if (call.Object != null && call.Object.Type == typeof(string))
                    {
                        sqlCall = "(" + obj.CommandText + " like (CONCAT('%' , " + args[0].CommandText + " , '%')))";
                    }
                    else
                    {
                        if (obj.CommandText == "()")
                            sqlCall = "(false)";
                        else
                            sqlCall = "(" + args[0].CommandText + " in " + obj.CommandText + ")";
                    }
                }
                else if (call.Method.Name == "StartsWith" && args.Length == 1)
                {
                    sqlCall = "(" + obj.CommandText + " like (CONCAT(" + args[0].CommandText + " , '%')))";
                }
                else if (call.Method.Name == "EndsWith" && args.Length == 1)
                {
                    sqlCall = "(" + obj.CommandText + " like (CONCAT('%', " + args[0].CommandText + ")))";
                }
                else if (call.Method.Name == "Equals" && args.Length == 1)
                {
                    sqlCall = "(" + obj.CommandText + " = (" + args[0].CommandText + "))";
                }
                else if (call.Method.Name == "Length" && args.Length == 0)
                {
                    sqlCall = "(length(" + obj.CommandText + "))";
                }
                else if (call.Method.Name == "Now" && args.Length == 0)
                {
                    sqlCall = "(now())";
                }
                else if (call.Method.Name == "ToLower")
                {
                    sqlCall = "(lower(" + obj.CommandText + "))";
                }
                else if (call.Method.Name == "ToUpper")
                {
                    sqlCall = "(upper(" + obj.CommandText + "))";
                }
                else if (call.Method.Name == "Replace" && args.Length == 2)
                {
                    sqlCall = "(replace(" + obj.CommandText + ", " + args[0].CommandText + ", " + args[1].CommandText + "))";
                }
                else
                {
                    sqlCall = call.Method.Name.ToLower() + "(" +
                              string.Join(",", args.Select(a => a.CommandText).ToArray()) + ")";
                }
                return new CompileResult
                {
                    CommandText = sqlCall
                };
            }
            if (expr.NodeType == ExpressionType.Constant)
            {
                var c = (ConstantExpression)expr;
                if (c.Value is SQL s)
                {
                    if (c.Value is SQLWithParam sq)
                    {
                        int count = sq.Args.Length;
                        var cmd = string.Format(s.Command, (from x in Enumerable.Range(queryArgs.Count + 1, count) select (object)("@pu" + x)).ToArray());
                        queryArgs.AddRange(sq.Args);
                        return new CompileResult() { CommandText = cmd };
                    }
                    return new CompileResult
                    {
                        CommandText = s.ToString()
                    };
                }
                if (c.Value != null)
                    queryArgs.Add(c.Value);
                return new CompileResult
                {
                    CommandText = "@pu" + queryArgs.Count,
                    Value = c.Value
                };
            }
            if (expr.NodeType == ExpressionType.Convert)
            {
                var u = (UnaryExpression)expr;
                var ty = u.Type;

                var valr = CompileExpr(u.Operand, queryArgs, ctx);
                return new CompileResult
                {
                    CommandText = valr.CommandText,
                    Value = valr.Value != null ? ConvertTo(valr.Value, ty) : null
                };
            }
            if (expr.NodeType == ExpressionType.MemberAccess)
            {
                var mem = (MemberExpression)expr;

                if (mem.Expression != null && mem.Expression.NodeType == ExpressionType.Parameter)
                {
                    //
                    // This is a column of our table, output just the column name
                    // Need to translate it if that column name is mapped
                    //
                    return CompileColumnAccess(mem, queryArgs, ctx);
                }

                object obj = null;
                if (mem.Expression != null)
                {
                    var r = CompileExpr(mem.Expression, queryArgs, ctx);
                    if (r.CommandText == string.Empty)
                    {
                        var ret = CompileColumnAccess(mem, queryArgs, ctx);
                        if (ret.CommandText != null)
                            return ret;
                    }

                    if (r.Value == null)
                    {
                        if (mem.Member.Name == "Length")
                            return new CompileResult() { CommandText = "(length(" + r.CommandText + "))" };
                        if (mem.Member.DeclaringType == typeof(DateTime))
                            return new CompileResult() { CommandText = "(" + mem.Member.Name + "(" + r.CommandText + "))" };
                        throw new NotSupportedException("Member access failed to compile expression");
                    }
                    if (r.CommandText.StartsWith("@pu"))
                    {
                        queryArgs.RemoveAt(queryArgs.Count - 1);
                    }
                    obj = r.Value;
                }

                //
                // Get the member value
                //

                if (mem.Member.Name == "Now" && mem.Member.DeclaringType == typeof(DateTime))
                    return new CompileResult() { CommandText = "(now())" };
                var val = ContextIndependantDatabase.GetMemberValue(obj, mem.Member);

                //
                // Work special magic for enumerables
                //
                if (val != null && val is IEnumerable && !(val is string) && !(val is IEnumerable<byte>))
                {
                    var sb = new StringBuilder();
                    sb.Append("(");
                    var head = "";
                    foreach (var a in (IEnumerable)val)
                    {
                        queryArgs.Add(a);
                        sb.Append(head);
                        sb.Append("@pu" + queryArgs.Count);
                        head = ",";
                    }
                    sb.Append(")");
                    return new CompileResult
                    {
                        CommandText = sb.ToString(),
                        Value = val
                    };
                }

                if (val is SQL s)
                {
                    if (val is SQLWithParam sq)
                    {
                        int count = sq.Args.Length;
                        var cmd = string.Format(s.Command, (from x in Enumerable.Range(queryArgs.Count + 1, count) select (object)("@pu" + x)).ToArray());
                        queryArgs.AddRange(sq.Args);
                        return new CompileResult() { CommandText = cmd };
                    }
                    return new CompileResult() { CommandText = s.Command };
                }

                queryArgs.Add(val);
                return new CompileResult
                {
                    CommandText = "@pu" + queryArgs.Count,
                    Value = val
                };
            }
            if (expr.NodeType == ExpressionType.Quote && expr is UnaryExpression e)
            {
                var operand = e.Operand as LambdaExpression;
                return CompileExpr(operand.Body, queryArgs, ctx);
            }
            if (expr.NodeType == ExpressionType.Parameter)
            {
                var par = (ParameterExpression)expr;
                return new CompileResult
                {
                    CommandText = par.Name
                };
            }
            throw new NotSupportedException("Cannot compile: " + expr.NodeType);
        }

        private object Execute(Expression expr, MappingContext context)
        {
            if (expr is MethodCallExpression mce)
            {
                var left = Execute(mce.Object, context);
                var value = mce.Method.Invoke(left, mce.Arguments.Select((x) => Execute(x, context)).ToArray());
                if (value is BaseTableQuery btq)
                    btq.ParentContext = context;
                return value;
            }
            if (expr is UnaryExpression ue && expr.NodeType == ExpressionType.Quote)
            {
                if (expr.Type.Is<Expression>())
                {
                    return ue.Operand;
                }
            }
            if (expr is MemberExpression me)
            {
                var value = ContextIndependantDatabase.GetMemberValue(Execute(me.Expression, context), me.Member);
                if (value is BaseTableQuery btq)
                    btq.ParentContext = context;
                return value;
            }
            if (expr is ConstantExpression ce)
            {
                return ce.Value;
            }
            throw new InvalidOperationException();
        }

        private static bool IsDatabaseObject(Type t)
        {
            return t.GetCustomAttribute<DataSourceAttribute>() != null;
        }
        private CompileResult CompileColumnAccess(MemberExpression mem, List<object> args, MappingContext ctx)
        {
            var table = ctx;

            string columnName = ctx.FindColumn(mem)?.Column;

            if (columnName == null)
            {
                if (_selects.Any())
                {
                    var selectedValue = _selects.FirstOrDefault((x) => x.ColumnName == mem.Member.Name);
                    if (selectedValue != null)
                    {
                        var code = CompileExpr(selectedValue.Code, args, ctx);
                        return code;
                    }
                }
            }
            return new CompileResult
            {
                CommandText = columnName ?? string.Empty
            };
        }


        private object ConvertTo(object obj, Type t)
        {
            var nut = Nullable.GetUnderlyingType(t);

            if (nut != null)
            {
                if (obj == null)
                {
                    return null;
                }
                return Convert.ChangeType(obj, nut, CultureInfo.CurrentCulture);
            }
            return Convert.ChangeType(obj, t, CultureInfo.CurrentCulture);
        }

        /// <summary>
        ///     Compiles a BinaryExpression where one of the parameters is null.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="parameter">The non-null parameter</param>
        private string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter)
        {
            if (expression.NodeType == ExpressionType.Equal)
            {
                return "(" + parameter.CommandText + " is null)";
            }
            if (expression.NodeType == ExpressionType.NotEqual)
            {
                return "(!(" + parameter.CommandText + " is null))";
            }
            throw new NotSupportedException("Cannot compile Null-BinaryExpression with type " +
                                            expression.NodeType);
        }

        private string GetSqlName(BinaryExpression expr)
        {
            var n = expr.NodeType;
            if (n == ExpressionType.GreaterThan)
            {
                return ">";
            }
            if (n == ExpressionType.GreaterThanOrEqual)
            {
                return ">=";
            }
            if (n == ExpressionType.LessThan)
            {
                return "<";
            }
            if (n == ExpressionType.LessThanOrEqual)
            {
                return "<=";
            }
            if (n == ExpressionType.And)
            {
                return "&";
            }
            if (n == ExpressionType.AndAlso)
            {
                return "and";
            }
            if (n == ExpressionType.Or)
            {
                return "|";
            }
            if (n == ExpressionType.OrElse)
            {
                return "or";
            }
            if (n == ExpressionType.Equal)
            {
                return "=";
            }
            if (n == ExpressionType.NotEqual)
            {
                return "!=";
            }
            if (n == ExpressionType.Add)
            {
                if (expr.Left.Type == typeof(string))
                {
                    return "||";
                }
                return "+";

            }
            if (n == ExpressionType.Subtract)
            {
                return "-";
            }
            if (n == ExpressionType.Divide)
            {
                return "/";
            }
            if (n == ExpressionType.Multiply)
            {
                return "*";
            }

            throw new NotSupportedException("Cannot get SQL for: " + n);
        }

        public bool Any(Expression<Func<T, bool>> pred)
        {
            return Where(pred).Any();
        }
        public override bool Any()
        {
            return Count() > 0;
        }
        public override long Count()
        {
            return Convert.ToInt64(GenerateCommand("count(*)").ExecuteScalar());
        }

        public TableQuery<T> Distinct()
        {
            var query = Clone<T>();
            query._distinct = true;
            return query;
        }

        public long Count(Expression<Func<T, bool>> predExpr)
        {
            if (predExpr == null)
            {
                throw new ArgumentNullException("predExpr");
            }
            return Where(predExpr).Count();
        }


        public T First()
        {
            var query = Take(1);
            return query.ToList().First();
        }


        public T FirstOrDefault()
        {
            var query = Take(1);
            return query.ToList().FirstOrDefault();
        }

        public R Get<R>(string name)
        {
            return Connection.ConvertDatabaseObjectToCLRObject<R>(GenerateCommand(name).ExecuteScalar());
        }
        public R Get<R>(Expression<Func<T, R>> expr)
        {
            List<object> queryArgs = new List<object>();
            var compiled = CompileExpr(expr, queryArgs, new MappingContext(Table, ParentContext));
            return Connection.ConvertDatabaseObjectToCLRObject<R>(GenerateCommand(compiled.CommandText, queryArgs).ExecuteScalar());
        }

        public TableQuery<T> Named(string v)
        {
            if (!IsDatabaseObject(typeof(T)))
                throw new InvalidOperationException("Could not name a query that does not represent a Table");

            var tbl = Clone<T>();
            tbl.Table = new TableMapping() { DeclaringType = Table.DeclaringType, TableSource = Table.TableSource, Name = v };
            tbl.Table.Build();
            return tbl;
        }

        protected override BaseTableQuery ApplyTake(int i)
        {
            return Take(i);
        }

        protected override BaseTableQuery ApplySkip(int i)
        {
            return Skip(i);
        }

        private class CompileResult
        {
            public string CommandText { get; set; }
            public object Value { get; set; }
            public bool IsJoin { get; set; } = false;
        }
    }
}
