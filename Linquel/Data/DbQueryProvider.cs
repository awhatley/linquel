// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

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

namespace IQ.Data 
{
    /// <summary>
    /// A LINQ IQueryable query provider that executes database queries over a DbConnection
    /// </summary>
    public class DbQueryProvider : QueryProvider
    {
        DbConnection connection;
        QueryPolicy policy;
        QueryMapping mapping;
        QueryLanguage language;
        TextWriter log;

        public DbQueryProvider(DbConnection connection, QueryPolicy policy, TextWriter log)
        {
            this.connection = connection;
            this.policy = policy;
            this.mapping = policy.Mapping;
            this.language = mapping.Language;
            this.log = log;
        }

        public DbConnection Connection
        {
            get { return this.connection; }
        }

        public TextWriter Log
        {
            get { return this.log; }
        }

        public QueryPolicy Policy
        {
            get { return this.policy; }
        }

        public QueryMapping Mapping
        {
            get { return this.mapping; }
        }

        public QueryLanguage Language
        {
            get { return this.language; }
        }

        /// <summary>
        /// Converts the query expression into text that corresponds to the command that would be executed.
        /// Useful for debugging.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public override string GetQueryText(Expression expression)
        {
            Expression translated = this.Translate(expression);
            var selects = SelectGatherer.Gather(translated).Select(s => this.language.Format(s));
            return string.Join("\n\n", selects.ToArray());
        }

        public string GetQueryPlan(Expression expression)
        {
            Expression plan = this.GetExecutionPlan(expression);
            return DbExpressionWriter.WriteToString(plan);
        }

        /// <summary>
        /// Execute the query expression (does translation, etc.)
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public override object Execute(Expression expression)
        {
            Expression plan = this.GetExecutionPlan(expression);

            LambdaExpression lambda = expression as LambdaExpression;
            if (lambda != null)
            {
                // compile & return the execution plan so it can be used multiple times
                LambdaExpression fn = Expression.Lambda(lambda.Type, plan, lambda.Parameters);
                return fn.Compile();
            }
            else
            {
                // compile the execution plan and invoke it
                Expression<Func<object>> efn = Expression.Lambda<Func<object>>(Expression.Convert(plan, typeof(object)));
                Func<object> fn = efn.Compile();
                return fn();
            }
        }

        /// <summary>
        /// Convert the query expression into an execution plan
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        protected virtual Expression GetExecutionPlan(Expression expression)
        {
            // strip off lambda for now
            LambdaExpression lambda = expression as LambdaExpression;
            if (lambda != null)
                expression = lambda.Body;

            // translate query into client & server parts
            Expression translation = this.Translate(expression);

            Expression provider = TypedSubtreeFinder.Find(expression, typeof(DbQueryProvider));
            if (provider == null)
            {
                Expression rootQueryable = TypedSubtreeFinder.Find(expression, typeof(IQueryable));
                provider = Expression.Convert(
                    Expression.Property(rootQueryable, typeof(IQueryable).GetProperty("Provider")),
                    typeof(DbQueryProvider)
                    );
            }

            return this.policy.BuildExecutionPlan(translation, provider);
        }

        /// <summary>
        /// Do all query translations execpt building the execution plan
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        protected virtual Expression Translate(Expression expression)
        {
            // pre-evaluate local sub-trees
            expression = PartialEvaluator.Eval(expression, this.CanBeEvaluatedLocally);

            // apply mapping (binds LINQ operators too)
            expression = this.mapping.Translate(expression);

            // any policy specific translations or validations
            expression = this.policy.Translate(expression);

            // any language specific translations or validations
            expression = this.language.Translate(expression);

            // do final reduction
            expression = UnusedColumnRemover.Remove(expression);
            expression = RedundantSubqueryRemover.Remove(expression);
            expression = RedundantJoinRemover.Remove(expression);
            expression = RedundantColumnRemover.Remove(expression);

            return expression;
        }

        /// <summary>
        /// Determines whether a given expression can be executed locally. 
        /// (It contains no parts that should be translated to the target environment.)
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        protected virtual bool CanBeEvaluatedLocally(Expression expression)
        {
            // any operation on a query can't be done locally
            ConstantExpression cex = expression as ConstantExpression;
            if (cex != null)
            {
                IQueryable query = cex.Value as IQueryable;
                if (query != null && query.Provider == this)
                    return false;
            }
            MethodCallExpression mc = expression as MethodCallExpression;
            if (mc != null &&
                (mc.Method.DeclaringType == typeof(Enumerable) ||
                 mc.Method.DeclaringType == typeof(Queryable) || 
                 mc.Method.DeclaringType == typeof(Updatable))
                 )
            {
                return false;
            }
            if (expression.NodeType == ExpressionType.Convert &&
                expression.Type == typeof(object))
                return true;
            return expression.NodeType != ExpressionType.Parameter &&
                   expression.NodeType != ExpressionType.Lambda;
        }

        /// <summary>
        /// Execute an actual query specified in the target language using the sADO connection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="paramValues"></param>
        /// <returns></returns>
        public virtual IEnumerable<T> Execute<T>(QueryCommand command, Func<DbDataReader, T> fnProjector, object[] paramValues)
        {
            this.LogCommand(command, paramValues);
            DbCommand cmd = this.GetCommand(command, paramValues);
            DbDataReader reader = cmd.ExecuteReader();
            return Project(reader, fnProjector);
        }

        /// <summary>
        /// Execute an actual query that does not return mapped results
        /// </summary>
        /// <param name="query"></param>
        /// <param name="paramValues"></param>
        /// <returns></returns>
        public virtual object ExecuteCommand(QueryCommand query, object[] paramValues)
        {
            this.LogCommand(query, paramValues);
            DbCommand cmd = this.GetCommand(query, paramValues);
            return cmd.ExecuteNonQuery();
        }

        public virtual IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream)
        {
            var result = this.ExecuteBatch(query, paramSets);
            if (!stream)
            {
                return result.ToList();
            }
            else
            {
                return new EnumerateOnce<int>(result);
            }
        }

        private IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets)
        {
            this.LogCommand(query, null);
            DbCommand cmd = this.GetCommand(query, null);
            foreach (var paramValues in paramSets)
            {
                this.LogMessage("");
                this.LogParameters(query, paramValues);
                this.SetParameterValues(cmd, paramValues);
                int result = cmd.ExecuteNonQuery();
                yield return result;
            }
        }

        public virtual IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<DbDataReader, T> fnProjector, int batchSize, bool stream)
        {
            var result = this.ExecuteBatch(query, paramSets, fnProjector);
            if (!stream)
            {
                return result.ToList();
            }
            else
            {
                return new EnumerateOnce<T>(result);
            }
        }

        private IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<DbDataReader, T> fnProjector)
        {
            this.LogCommand(query, null);
            DbCommand cmd = this.GetCommand(query, null);
            cmd.Prepare();
            foreach (var paramValues in paramSets)
            {
                this.LogMessage("");
                this.LogParameters(query, paramValues);
                this.SetParameterValues(cmd, paramValues);
                var reader = cmd.ExecuteReader();
                if (reader.HasRows)
                {
                    reader.Read();
                    yield return fnProjector(reader);
                }
                else
                {
                    yield return default(T);
                }
                reader.Close();
            }
        }

        /// <summary>
        /// Converts a data reader into a sequence of objects using a projector function on each row
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="reader"></param>
        /// <param name="fnProject"></param>
        /// <returns></returns>
        public virtual IEnumerable<T> Project<T>(DbDataReader reader, Func<DbDataReader, T> fnProjector)
        {
            while (reader.Read())
            {
                yield return fnProjector(reader);
            }
        }

        /// <summary>
        /// Get an IEnumerable that will execute the specified query when enumerated
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="paramValues"></param>
        /// <returns></returns>
        public virtual IEnumerable<T> ExecuteDeferred<T>(QueryCommand query, Func<DbDataReader, T> fnProjector, object[] paramValues)
        {
            this.LogCommand(query, paramValues);
            DbCommand cmd = this.GetCommand(query, paramValues);
            DbDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                yield return fnProjector(reader);
            }
        }

        /// <summary>
        /// Get an ADO command object initialized with the command-text and parameters
        /// </summary>
        /// <param name="commandText"></param>
        /// <param name="paramNames"></param>
        /// <param name="paramValues"></param>
        /// <returns></returns>
        protected virtual DbCommand GetCommand(QueryCommand command, object[] paramValues)
        {
            // create command object (and fill in parameters)
            DbCommand cmd = this.connection.CreateCommand();
            cmd.CommandText = command.CommandText;
            for (int i = 0, n = command.Parameters.Count; i < n; i++)
            {
                DbParameter p = cmd.CreateParameter();
                p.ParameterName = command.Parameters[i].Name;
                if (paramValues != null)
                {
                    p.Value = paramValues[i] ?? DBNull.Value;
                }
                cmd.Parameters.Add(p);
            }
            return cmd;
        }

        protected virtual void SetParameterValues(DbCommand command, object[] paramValues)
        {
            if (paramValues != null)
            {
                for (int i = 0, n = command.Parameters.Count; i < n; i++)
                {
                    DbParameter p = command.Parameters[i];
                    if (p.Direction == System.Data.ParameterDirection.Input 
                     || p.Direction == System.Data.ParameterDirection.InputOutput)
                    {
                        p.Value = paramValues[i] ?? DBNull.Value;
                    }
                }
            }
        }

        protected virtual void GetParameterValues(DbCommand command, object[] paramValues)
        {
            if (paramValues != null)
            {
                for (int i = 0, n = command.Parameters.Count; i < n; i++)
                {
                    if (command.Parameters[i].Direction != System.Data.ParameterDirection.Input)
                    {
                        object value = command.Parameters[i].Value;
                        if (value == DBNull.Value)
                            value = null;
                        paramValues[i] = value;
                    }
                }
            }
        }

        protected virtual void LogMessage(string message)
        {
            if (this.log != null)
            {
                this.log.WriteLine(message);
            }
        }

        /// <summary>
        /// Write a command & parameters to the log
        /// </summary>
        /// <param name="command"></param>
        /// <param name="paramValues"></param>
        protected virtual void LogCommand(QueryCommand command, object[] paramValues)
        {
            if (this.log != null)
            {
                this.log.WriteLine(command.CommandText);
                if (paramValues != null)
                {
                    this.LogParameters(command, paramValues);
                }
            }
        }

        protected virtual void LogParameters(QueryCommand command, object[] paramValues)
        {
            if (this.log != null)
            {
                for (int i = 0, n = command.Parameters.Count; i < n; i++)
                {
                    var p = command.Parameters[i];
                    var v = paramValues[i];

                    if (v == null || v == DBNull.Value)
                    {
                        this.log.WriteLine("-- @{0} = NULL", p.Name);
                    }
                    else
                    {
                        this.log.WriteLine("-- @{0} = [{1}]", p.Name, v);
                    }
                }
            }
        }
    }
}
