namespace MoyskleyTech.LINQToSQL.Data
{
    public class Join<TResult, T>
    {
        public Join()
        {
        }
        public Join(TResult l, T r)
        {
            Left = l;
            Right = r;
        }
        public TResult Left { get; set; }
        public T Right { get; set; }
    }
}