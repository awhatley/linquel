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
            return Activator.CreateInstance(
                typeof(ProjectionReader<>).MakeGenericType(elementType),
                BindingFlags.Instance | BindingFlags.NonPublic, null,
                new object[] { reader, projector, this },
                null
                );
        }

        internal class TranslateResult {
            internal string CommandText;
            internal LambdaExpression Projector;
        }

        private TranslateResult Translate(Expression expression) {
            ProjectionExpression projection = expression as ProjectionExpression;
            if (projection == null) {
                expression = Evaluator.PartialEval(expression);
                projection = (ProjectionExpression)new QueryBinder().Bind(expression);
            }
            string commandText = new QueryFormatter().Format(projection.Source);
            LambdaExpression projector = new ProjectionBuilder().Build(projection.Projector, projection.Source.Alias);
            return new TranslateResult { CommandText = commandText, Projector = projector };
        }
    } 
}
