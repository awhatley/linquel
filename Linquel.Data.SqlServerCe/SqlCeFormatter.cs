// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace IQToolkit.Data.SqlServerCe
{
    /// <summary>
    /// Formats a query expression into SQL Server Compact Edition language syntax
    /// </summary>
    public class SqlCeFormatter : TSqlFormatter
    {
        protected SqlCeFormatter(QueryLanguage language)
            : base(language)
        {
        }

        public static new string Format(Expression expression)
        {
            return Format(expression, new SqlCeLanguage());
        }

        public static new string Format(Expression expression, QueryLanguage language)
        {
            SqlCeFormatter formatter = new SqlCeFormatter(language);
            formatter.Visit(expression);
            return formatter.ToString();
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            if (m.Member.DeclaringType == typeof(DateTime) || m.Member.DeclaringType == typeof(DateTimeOffset))
            {
                switch (m.Member.Name)
                {
                    case "Day":
                        this.Write("DATEPART(day, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Month":
                        this.Write("DATEPART(month, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Year":
                        this.Write("DATEPART(year, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Hour":
                        this.Write("DATEPART(hour, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Minute":
                        this.Write("DATEPART(minute, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Second":
                        this.Write("DATEPART(second, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Millisecond":
                        this.Write("DATEPART(millisecond, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "DayOfWeek":
                        this.Write("(DATEPART(weekday, ");
                        this.Visit(m.Expression);
                        this.Write(") - 1)");
                        return m;
                    case "DayOfYear":
                        this.Write("(DATEPART(dayofyear, ");
                        this.Visit(m.Expression);
                        this.Write(") - 1)");
                        return m;
                }
            }
            return base.VisitMemberAccess(m);
        }
    }
}
