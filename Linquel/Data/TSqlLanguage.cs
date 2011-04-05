﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data
{
    /// <summary>
    /// TSQL specific QueryLanguage
    /// </summary>
    public class TSqlLanguage : QueryLanguage
    {
        TSqlTypeSystem typeSystem = new TSqlTypeSystem();

        public TSqlLanguage()
        {
        }

        public override QueryTypeSystem TypeSystem
        {
            get { return this.typeSystem; }
        }

        public override string Quote(string name)
        {
            if (name.StartsWith("[") && name.EndsWith("]"))
            {
                return name;
            }
            else if (name.IndexOf('.') > 0)
            {
                return "[" + string.Join("].[", name.Split(splitChars, StringSplitOptions.RemoveEmptyEntries)) + "]";
            }
            else
            {
                return "[" + name + "]";
            }
        }

        private static readonly char[] splitChars = new char[] { '.' };

        public override bool AllowsMultipleCommands
        {
            get { return true; }
        }

        public override bool AllowSubqueryInSelectWithoutFrom
        {
            get { return true; }
        }

        public override bool AllowDistinctInAggregates
        {
            get { return true; }
        }

        public override Expression GetGeneratedIdExpression(MemberInfo member)
        {
            return new FunctionExpression(TypeHelper.GetMemberType(member), "SCOPE_IDENTITY()", null);
        }

        public override Expression Translate(Expression expression)
        {
            // fix up any order-by's
            expression = OrderByRewriter.Rewrite(expression);

            expression = base.Translate(expression);

            // convert skip/take info into RowNumber pattern
            expression = SkipToRowNumberRewriter.Rewrite(expression);

            // fix up any order-by's we may have changed
            expression = OrderByRewriter.Rewrite(expression);

            return expression;
        }

        public override string Format(Expression expression)
        {
            return TSqlFormatter.Format(expression, this);
        }

        private static TSqlLanguage _default;

        public static TSqlLanguage Default
        {
            get
            {
                if (_default == null)
                {
                    System.Threading.Interlocked.CompareExchange(ref _default, new TSqlLanguage(), null);
                }
                return _default;
            }
        }
    }
}