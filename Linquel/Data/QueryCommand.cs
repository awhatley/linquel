// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;

namespace IQToolkit.Data
{
    public class QueryCommand
    {
        string commandText;
        ReadOnlyCollection<QueryParameter> parameters;
        ReadOnlyCollection<ColumnExpression> columns;

        public QueryCommand(string commandText, IEnumerable<QueryParameter> parameters, IEnumerable<ColumnExpression> columns)
        {
            this.commandText = commandText;
            this.parameters = parameters.ToReadOnly();
            this.columns = columns.ToReadOnly();
        }

        public string CommandText
        {
            get { return this.commandText; }
        }

        public ReadOnlyCollection<QueryParameter> Parameters
        {
            get { return this.parameters; }
        }

        public ReadOnlyCollection<ColumnExpression> Columns
        {
            get { return this.columns; }
        }
    }

    public class QueryParameter
    {
        string name;
        Type type;
        QueryType queryType;

        public QueryParameter(string name, Type type, QueryType queryType)
        {
            this.name = name;
            this.type = type;
            this.queryType = queryType;
        }

        public string Name
        {
            get { return this.name; }
        }

        public Type Type
        {
            get { return this.type; }
        }

        public QueryType QueryType
        {
            get { return this.queryType; }
        }
    }
}
