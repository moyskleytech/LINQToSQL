using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace MoyskleyTech.LINQToSQL.Data
{
    public class WeakQueryEnumerator<T> : IEnumerable<T>
    {
        TableQuery<T> tq;

        public WeakQueryEnumerator(TableQuery<T> tq)
        {
            this.tq = tq;
        }
        public IEnumerator<T> GetEnumerator()
        {
            var lmt = tq.Limit;
            if (lmt != null)
            {
                for (var i = 0; i < lmt.Value; i++)
                {
                    var el = tq.ElementAtOrDefault(i);
                    if (el != null)
                        yield return el;
                    else
                        break;
                }
            }
            else
            {
                var el = tq.FirstOrDefault();
                int i = 1;
                while (el != null)
                {
                    yield return el;
                    el = tq.ElementAtOrDefault(i++);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<T>)this).GetEnumerator();
        }
    }
    public class WeakQueryEnumeratorWithSelector<T> : IEnumerable<T>
    {
        TableQuery<T> tq;
        string selector;
        public WeakQueryEnumeratorWithSelector(TableQuery<T> tq,string sel)
        {
            this.tq = tq;
            selector = sel;
        }
        public IEnumerator<T> GetEnumerator()
        {
            var lmt = tq.Limit;
            if (lmt != null)
            {
                for (var i = 0; i < lmt.Value; i++)
                {
                    var el = ValueAt(i);
                    if (el != null)
                        yield return el;
                    else
                        break;
                }
            }
            else
            {
                var el = ValueAt(0);
                int i = 1;
                while (el != null)
                {
                    yield return el;
                    el = ValueAt(i++);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<T>)this).GetEnumerator();
        }

        private T ValueAt(int idx)
        {
            var cmd = tq.Skip(idx).Take(1).GetCommand(selector);
            return tq.Connection.QueryOneOrDefault<T>(cmd);
        }
    }
}