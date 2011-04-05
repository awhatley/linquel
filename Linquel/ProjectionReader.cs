using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Sample {

    /// <summary>
    /// A ProjectionRow is an abstract over a row based data source
    /// </summary>
    public abstract class ProjectionRow {
        public abstract object GetValue(int index);
        public abstract IEnumerable<E> ExecuteSubQuery<E>(LambdaExpression query);
    }

    /// <summary>
    /// ProjectionBuilder is a visitor that converts an projector expression
    /// that constructs result objects out of ColumnExpressions into an actual
    /// LambdaExpression that constructs result objects out of accessing fields
    /// of a ProjectionRow
    /// </summary>
    internal class ProjectionBuilder : DbExpressionVisitor {
        ParameterExpression row;
        string rowAlias;
        IList<string> columns;
        static MethodInfo miGetValue;
        static MethodInfo miExecuteSubQuery;
        
        private ProjectionBuilder(string rowAlias, IList<string> columns) {
            this.rowAlias = rowAlias;
            this.columns = columns;
            this.row = Expression.Parameter(typeof(ProjectionRow), "row");
            if (miGetValue == null) {
                miGetValue = typeof(ProjectionRow).GetMethod("GetValue");
                miExecuteSubQuery = typeof(ProjectionRow).GetMethod("ExecuteSubQuery");
            }
        }

        internal static LambdaExpression Build(Expression expression, string alias, IList<string> columns) {
            ProjectionBuilder builder = new ProjectionBuilder(alias, columns);
            Expression body = builder.Visit(expression);
            return Expression.Lambda(body, builder.row);
        }

        protected override Expression VisitColumn(ColumnExpression column) {
            if (column.Alias == this.rowAlias) {
                int iOrdinal = this.columns.IndexOf(column.Name);
                return Expression.Convert(
                    Expression.Call(typeof(System.Convert), "ChangeType", null,
                        Expression.Call(this.row, miGetValue, Expression.Constant(iOrdinal)),
                        Expression.Constant(column.Type)
                        ),                        
                        column.Type
                    );
            }
            return column;
        }

        protected override Expression VisitProjection(ProjectionExpression proj) {
            LambdaExpression subQuery = Expression.Lambda(base.VisitProjection(proj), this.row);
            Type elementType = TypeSystem.GetElementType(subQuery.Body.Type);
            MethodInfo mi = miExecuteSubQuery.MakeGenericMethod(elementType);
            return Expression.Convert(
                Expression.Call(this.row, mi, Expression.Constant(subQuery)),
                proj.Type
                );
        }
    }


    /// <summary>
    /// ProjectionReader is an implemention of IEnumerable that converts data from DbDataReader into
    /// objects via a projector function,
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class ProjectionReader<T> : IEnumerable<T>, IEnumerable {
        Enumerator enumerator;

        internal ProjectionReader(DbDataReader reader, Func<ProjectionRow, T> projector, IQueryProvider provider) {
            this.enumerator = new Enumerator(reader, projector, provider);
        }

        public IEnumerator<T> GetEnumerator() {
            Enumerator e = this.enumerator;
            if (e == null) {
                throw new InvalidOperationException("Cannot enumerate more than once");
            }
            this.enumerator = null;
            return e;
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        class Enumerator : ProjectionRow, IEnumerator<T>, IEnumerator, IDisposable {
            DbDataReader reader;
            T current;
            Func<ProjectionRow, T> projector;
            IQueryProvider provider;

            internal Enumerator(DbDataReader reader, Func<ProjectionRow, T> projector, IQueryProvider provider) {
                this.reader = reader;
                this.projector = projector;
                this.provider = provider;
            }

            public override object GetValue(int index) {
                if (index >= 0) {
                    if (this.reader.IsDBNull(index)) {
                        return null;
                    }
                    else {
                        return this.reader.GetValue(index);
                    }
                }
                throw new IndexOutOfRangeException();
            }

            public override IEnumerable<E> ExecuteSubQuery<E>(LambdaExpression query) {
                ProjectionExpression projection = (ProjectionExpression) Replacer.Replace(query.Body, query.Parameters[0], Expression.Constant(this));
                projection = (ProjectionExpression) Evaluator.PartialEval(projection, CanEvaluateLocally);
                IEnumerable<E> result = (IEnumerable<E>)this.provider.Execute(projection);
                List<E> list = new List<E>(result);
                if (typeof(IQueryable<E>).IsAssignableFrom(query.Body.Type)) {
                    return list.AsQueryable();
                }
                return list;
            }

            private static bool CanEvaluateLocally(Expression expression) {
                if (expression.NodeType == ExpressionType.Parameter ||
                    expression.NodeType.IsDbExpression()) {
                    return false;
                }
                return true;
            }

            public T Current {
                get { return this.current; }
            }

            object IEnumerator.Current {
                get { return this.current; }
            }

            public bool MoveNext() {
                if (this.reader.Read()) {
                    this.current = this.projector(this);
                    return true;
                }
                return false;
            }

            public void Reset() {
            }

            public void Dispose() {
                this.reader.Dispose();
            }
        }
    }
}
