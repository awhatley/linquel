// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data.SqlClient
{
    public class SqlQueryProvider : DbQueryProvider
    {
        public SqlQueryProvider(SqlConnection connection, QueryMapping mapping)
            : base(connection, mapping, null)
        {
        }

        public SqlQueryProvider(SqlConnection connection, QueryMapping mapping, TextWriter log)
            : base(connection, mapping, log)
        {
        }

        public SqlQueryProvider(SqlConnection connection, QueryMapping mapping, QueryPolicy policy, TextWriter log)
            : base(connection, mapping, policy, log)
        {
        }

        public override DbQueryProvider Create(DbConnection connection, QueryMapping mapping, QueryPolicy policy, TextWriter log)
        {
            return new SqlQueryProvider((SqlConnection)connection, mapping, policy, log);
        }

        public static string GetExpressConnectionString(string databaseFile)
        {
            return string.Format(@"Data Source=.\SQLEXPRESS;Integrated Security=True;Connect Timeout=30;User Instance=True;MultipleActiveResultSets=true;AttachDbFilename='{0}'", databaseFile);
        }

        protected override void AddParameter(DbCommand command, QueryParameter parameter, object value)
        {
            TSqlType sqlType = (TSqlType)parameter.QueryType;
            if (sqlType == null)
                sqlType = (TSqlType)this.Language.TypeSystem.GetColumnType(parameter.Type);
            var p = ((SqlCommand)command).Parameters.Add("@" + parameter.Name, sqlType.SqlDbType, sqlType.Length);
            if (sqlType.Precision != 0)
                p.Precision = (byte)sqlType.Precision;
            if (sqlType.Scale != 0)
                p.Scale = (byte)sqlType.Scale;
            p.Value = value ?? DBNull.Value;
        }

        public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream)
        {
            var result = this.ExecuteBatch(query, paramSets, batchSize);
            if (!stream)
            {
                return result.ToList();
            }
            else
            {
                return new EnumerateOnce<int>(result);
            }
        }

        private IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize)
        {
            SqlCommand cmd = (SqlCommand)this.GetCommand(query, null);
            DataTable dataTable = new DataTable();
            for (int i = 0, n = query.Parameters.Count; i < n; i++)
            {
                var qp = query.Parameters[i];
                cmd.Parameters[i].SourceColumn = qp.Name;
                dataTable.Columns.Add(qp.Name, TypeHelper.GetNonNullableType(qp.Type));
            }
            SqlDataAdapter dataAdapter = new SqlDataAdapter();
            dataAdapter.InsertCommand = cmd;
            dataAdapter.InsertCommand.UpdatedRowSource = UpdateRowSource.None;
            dataAdapter.UpdateBatchSize = batchSize;

            this.LogMessage("-- Start SQL Batching --");
            this.LogMessage("");
            this.LogCommand(query, null);

            IEnumerator<object[]> en = paramSets.GetEnumerator();
            using (en)
            {
                bool hasNext = true;
                while (hasNext)
                {
                    int count = 0;
                    for (; count < dataAdapter.UpdateBatchSize && (hasNext = en.MoveNext()); count++)
                    {
                        var paramValues = en.Current;
                        dataTable.Rows.Add(paramValues);
                        this.LogParameters(query, paramValues);
                        this.LogMessage("");
                    }
                    if (count > 0)
                    {
                        int n = dataAdapter.Update(dataTable);
                        for (int i = 0; i < count; i++)
                        {
                            yield return (i < n) ? 1 : 0;
                        }
                        dataTable.Rows.Clear();
                    }
                }
            }

            this.LogMessage(string.Format("-- End SQL Batching --"));
            this.LogMessage("");
        }
    }
}