// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data.Access
{
    using IQToolkit.Data.OleDb;

    public class AccessQueryProvider : OleDb.OleDbQueryProvider
    {
        Dictionary<QueryCommand, OleDbCommand> commandCache = new Dictionary<QueryCommand, OleDbCommand>();

        public AccessQueryProvider(OleDbConnection connection, QueryMapping mapping)
            : base(connection, mapping, QueryPolicy.Default, null)
        {
        }

        public AccessQueryProvider(OleDbConnection connection, QueryMapping mapping, TextWriter log)
            : base(connection, mapping, QueryPolicy.Default, log)
        {
        }

        public AccessQueryProvider(OleDbConnection connection, QueryMapping mapping, QueryPolicy policy, TextWriter log)
            : base(connection, mapping, policy, log)
        {
        }

        public override DbQueryProvider Create(DbConnection connection, QueryMapping mapping, QueryPolicy policy, TextWriter log)
        {
            return new AccessQueryProvider((OleDbConnection)connection, mapping, policy, log);
        }

        public static string GetAccess2000ConnectionString(string databaseFile) 
        {
            return GetConnectionString(AccessOleDbProvider2000, databaseFile);
        }

        public static string GetAccess2007ConnectionString(string databaseFile)
        {
            return GetConnectionString(AccessOleDbProvider2007, databaseFile);
        }

        public static string GetConnectionString(string provider, string databaseFile)
        {
            return string.Format("Provider={0};ole db services=0;Data Source={1}", provider, databaseFile);
        }

        public static readonly string AccessOleDbProvider2000 = "Microsoft.Jet.OLEDB.4.0";
        public static readonly string AccessOleDbProvider2007 = "Microsoft.ACE.OLEDB.12.0";

        protected override DbCommand GetCommand(QueryCommand query, object[] paramValues)
        {
            OleDbCommand cmd;
            if (!this.commandCache.TryGetValue(query, out cmd))
            {
                cmd = (OleDbCommand)this.Connection.CreateCommand();
                cmd.CommandText = query.CommandText;
                this.SetParameterValues(query, cmd, paramValues);
                cmd.Prepare();
                this.commandCache.Add(query, cmd);
            }
            else
            {
                cmd = (OleDbCommand)cmd.Clone();
                this.SetParameterValues(query, cmd, paramValues);
            }
            return cmd;
        }

        protected override OleDbType GetOleDbType(QueryType type)
        {
            TSqlType sqlType = type as TSqlType;
            if (sqlType != null)
            {
                return ToOleDbType(sqlType.SqlDbType);
            }
            return base.GetOleDbType(type);
        }

        public static OleDbType ToOleDbType(SqlDbType type)
        {
            switch (type)
            {
                case SqlDbType.BigInt:
                    return OleDbType.BigInt;
                case SqlDbType.Bit:
                    return OleDbType.Boolean;
                case SqlDbType.DateTime:
                case SqlDbType.SmallDateTime:
                    return OleDbType.DBTimeStamp;
                case SqlDbType.Int:
                    return OleDbType.Integer;
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return OleDbType.Currency;
                case SqlDbType.SmallInt:
                    return OleDbType.SmallInt;
                case SqlDbType.Timestamp:
                    return OleDbType.Binary;
                case SqlDbType.TinyInt:
                    return OleDbType.TinyInt;
                case SqlDbType.UniqueIdentifier:
                    return OleDbType.Guid;
                case SqlDbType.Variant:
                    return OleDbType.Variant;
                case SqlDbType.Xml:
                    return OleDbType.VarChar;
                case SqlDbType.Binary:
                    return OleDbType.Binary;
                case SqlDbType.Char:
                    return OleDbType.Char;
                case SqlDbType.NChar:
                    return OleDbType.WChar;
                case SqlDbType.Image:
                    return OleDbType.LongVarBinary;
                case SqlDbType.NText:
                    return OleDbType.LongVarWChar;
                case SqlDbType.NVarChar:
                    return OleDbType.VarWChar;
                case SqlDbType.Text:
                    return OleDbType.LongVarChar;
                case SqlDbType.VarBinary:
                    return OleDbType.VarBinary;
                case SqlDbType.VarChar:
                    return OleDbType.VarChar;
                case SqlDbType.Decimal:
                    return OleDbType.Decimal;
                case SqlDbType.Float:
                case SqlDbType.Real:
                    return OleDbType.Double;
                default:
                    throw new InvalidOperationException(string.Format("Unhandled sql type '{0}'.", type));
            }
        }
    }
}