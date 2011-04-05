using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Sample {

    /// <summary>
    /// A LINQ query provider that executes SQL queries over a DbConnection
    /// </summary>
    public class DbQueryProvider : QueryProvider {
        DbConnection connection;
        TextWriter log;

        public DbQueryProvider(DbConnection connection) {
            this.connection = connection;
        }

        public TextWriter Log {
            get { return this.log; }
            set { this.log = value; }
        }

        public override string GetQueryText(Expression expression) {
            return this.Translate(expression).CommandText;
        }

        public override object Execute(Expression expression) {
            return this.Execute(this.Translate(expression));
        }

        private object Execute(TranslateResult query) {
            Delegate projector = query.Projector.Compile();

            if (this.log != null) {
                this.log.WriteLine(query.CommandText);
                this.log.WriteLine();
            }

            DbCommand cmd = this.connection.CreateCommand();
            cmd.CommandText = query.CommandText;
            DbDataReader reader = cmd.ExecuteReader();

            Type elementType = TypeSystem.GetElementType(query.Projector.Body.Type);
            
            IEnumerable sequence = (IEnumerable) Activator.CreateInstance(
                typeof(ProjectionReader<>).MakeGenericType(elementType),
                BindingFlags.Instance | BindingFlags.NonPublic, null,
                new object[] { reader, projector, this },
                null
                );

            if (query.Aggregator != null) {
                Delegate aggregator = query.Aggregator.Compile();
                AggregateReader aggReader = (AggregateReader) Activator.CreateInstance(
                    typeof(AggregateReader<,>).MakeGenericType(elementType, query.Aggregator.Body.Type),
                    BindingFlags.Instance | BindingFlags.NonPublic, null,
                    new object[] { aggregator },
                    null
                    );
                return aggReader.Read(sequence);
            }
            else {
                return sequence;
            }
        }

        abstract class AggregateReader {
            internal abstract object Read(IEnumerable sequence);
        }

        class AggregateReader<T, S> : AggregateReader {
            Func<IEnumerable<T>, S> aggregator;
            internal AggregateReader(Func<IEnumerable<T>, S> aggregator) {
                this.aggregator = aggregator;
            }
            internal override object Read(IEnumerable sequence) {
                return this.aggregator((IEnumerable<T>)sequence);
            }
        }

        internal class TranslateResult {
            internal string CommandText { get; private set; }
            internal LambdaExpression Projector { get; private set; }
            internal LambdaExpression Aggregator { get; private set; }
            internal TranslateResult(string commandText, LambdaExpression projector, LambdaExpression aggregator) {
                this.CommandText = commandText;
                this.Projector = projector;
                this.Aggregator = aggregator;
            }
        }

        private TranslateResult Translate(Expression expression) {
            ProjectionExpression projection = expression as ProjectionExpression;
            if (projection == null) {
                expression = Evaluator.PartialEval(expression, CanBeEvaluatedLocally);
                expression = QueryBinder.Bind(this, expression);
                expression = AggregateRewriter.Rewrite(expression);
                expression = OrderByRewriter.Rewrite(expression);
                expression = UnusedColumnRemover.Remove(expression);
                expression = RedundantSubqueryRemover.Remove(expression);
                projection = (ProjectionExpression)expression;
            }
            string commandText = QueryFormatter.Format(projection.Source);
            string[] columns = projection.Source.Columns.Select(c => c.Name).ToArray();
            LambdaExpression projector = ProjectionBuilder.Build(projection.Projector, projection.Source.Alias, columns);
            return new TranslateResult(commandText, projector, projection.Aggregator);
        }

        private bool CanBeEvaluatedLocally(Expression expression) {
            // any operation on a query can't be done locally
            ConstantExpression cex = expression as ConstantExpression;
            if (cex != null) {
                IQueryable query = cex.Value as IQueryable;
                if (query != null && query.Provider == this)
                    return false;
            }
            return expression.NodeType != ExpressionType.Parameter &&
                   expression.NodeType != ExpressionType.Lambda;
        }
    }

    public class Grouping<TKey, TElement> : IGrouping<TKey, TElement> {
        TKey key;
        IEnumerable<TElement> group;

        public Grouping(TKey key, IEnumerable<TElement> group) {
            this.key = key;
            this.group = group;
        }

        public TKey Key {
            get { return this.key; }
        }

        public IEnumerator<TElement> GetEnumerator() {
            return this.group.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.group.GetEnumerator();
        }
    }
}
