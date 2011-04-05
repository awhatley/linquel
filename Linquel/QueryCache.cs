using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;

namespace IQToolkit
{
    public class QueryCache
    {
        int maxSize;
        List<QueryCompiler.CompiledQuery> cache = new List<QueryCompiler.CompiledQuery>();
        ReaderWriterLock rwlock = new ReaderWriterLock();

        public QueryCache(int maxSize)
        {
            this.maxSize = maxSize;
        }

        public object Execute(Expression query)
        {
            object[] args;
            var cached = this.Find(query, true, out args);
            return cached.Invoke(args);
        }

        public object Execute(IQueryable query)
        {
            return this.Equals(query.Expression);
        }

        public IEnumerable<T> Execute<T>(IQueryable<T> query)
        {
            return (IEnumerable<T>)this.Execute(query.Expression);
        }

        public int Count
        {
            get
            {
                this.rwlock.AcquireReaderLock(Timeout.Infinite);
                try
                {
                    return this.cache.Count;
                }
                finally
                {
                    this.rwlock.ReleaseLock();
                }
            }
        }

        public void Clear()
        {
            this.rwlock.AcquireWriterLock(Timeout.Infinite);
            try
            {
                this.cache.Clear();
            }
            finally
            {
                this.rwlock.ReleaseWriterLock();
            }
        }

        public bool Contains(Expression query)
        {
            object[] args;
            return this.Find(query, false, out args) != null;
        }

        public bool Contains(IQueryable query)
        {
            return this.Contains(query.Expression);
        }

        private QueryCompiler.CompiledQuery Find(Expression query, bool add, out object[] args)
        {
            IQueryProvider provider = this.FindProvider(query);
            if (provider == null)
            {
                throw new ArgumentException("Cannot deduce query provider from query");
            }
            var ep = provider as IEntityProvider;
            var evaled = ep != null
                ? PartialEvaluator.Eval(query, ep.CanBeEvaluatedLocally)
                : PartialEvaluator.Eval(query);
            var pq = QueryParameterizer.Parameterize(evaled, out args);

            rwlock.AcquireReaderLock(Timeout.Infinite);
            QueryCompiler.CompiledQuery cached;
            int cacheIndex = 0;
            try
            {
                cached = this.cache.FirstOrDefault(cq => ExpressionComparer.AreEqual(cq.Query, pq, false));
                if (cached != null)
                {
                    cacheIndex = this.cache.IndexOf(cached, 0);
                }
            }
            finally
            {
                rwlock.ReleaseLock();
            }

            if ((cached == null || cacheIndex > 0) && add)
            {
                rwlock.AcquireWriterLock(Timeout.Infinite);
                try
                {
                    if (cached == null)
                    {
                        cached = new QueryCompiler.CompiledQuery(pq);
                    }
                    else
                    {
                        // don't use cacheIndex here, since it may have moved
                        this.cache.Remove(cached);
                    }
                    // put most-recently-used query at start of list
                    this.cache.Insert(0, cached);
                    if (this.cache.Count > this.maxSize)
                    {
                        this.cache.RemoveAt(this.cache.Count - 1);
                    }
                }
                finally
                {
                    rwlock.ReleaseWriterLock();
                }
            }
            return cached;
        }

        private IQueryProvider FindProvider(Expression expression)
        {
            ConstantExpression root = TypedSubtreeFinder.Find(expression, typeof(IQueryProvider)) as ConstantExpression;
            if (root == null)
            {
                root = TypedSubtreeFinder.Find(expression, typeof(IQueryable)) as ConstantExpression;
            }
            if (root != null)
            {
                IQueryProvider provider = root.Value as IQueryProvider;
                if (provider == null)
                {
                    IQueryable query = root.Value as IQueryable;
                    if (query != null)
                    {
                        provider = query.Provider;
                    }
                }
                return provider;
            }
            return null;
        }

        // convert constants into parameters
        class QueryParameterizer : ExpressionVisitor
        {
            List<ParameterExpression> parameters = new List<ParameterExpression>();
            List<object> values = new List<object>();

            internal static LambdaExpression Parameterize(Expression expression, out object[] arguments)
            {
                var visitor = new QueryParameterizer();
                var body = visitor.Visit(expression);
                arguments = visitor.values.ToArray();
                if (arguments.Length < 5)
                {
                    return Expression.Lambda(body, visitor.parameters.ToArray());
                }
                else
                {
                    arguments = new object[] { arguments };
                    return ExplicitToObjectArray.Rewrite(body, visitor.parameters);                    
                }
            }

            protected override Expression VisitConstant(ConstantExpression c)
            {
                var p = Expression.Parameter(c.Type, "p" + this.parameters.Count);
                this.parameters.Add(p);
                this.values.Add(c.Value);
                // if IQueryable, parameterize but don't replace in the tree 
                if (c.Value is IQueryable)
                    return c;
                return p;
            }
        }

        class ExplicitToObjectArray : ExpressionVisitor
        {
            IList<ParameterExpression> parameters;
            ParameterExpression array = Expression.Parameter(typeof(object[]), "array");

            private ExplicitToObjectArray(IList<ParameterExpression> parameters)
            {
                this.parameters = parameters;
            }

            internal static LambdaExpression Rewrite(Expression body, IList<ParameterExpression> parameters)
            {
                var visitor = new ExplicitToObjectArray(parameters);
                return Expression.Lambda(visitor.Visit(body), visitor.array);                  
            }

            protected override Expression VisitParameter(ParameterExpression p)
            {
                for (int i = 0, n = this.parameters.Count; i < n; i++)
                {
                    if (this.parameters[i] == p)
                    {
                        return Expression.Convert(Expression.ArrayIndex(this.array, Expression.Constant(i)), p.Type);
                    }
                }
                return p;
            }
        }
    }
}
