using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Sample {
    /// <summary>
    /// QueryBinder is a visitor that converts method calls to LINQ operations into 
    /// custom DbExpression nodes and references to class members into references to columns
    /// </summary>
    internal class QueryBinder : ExpressionVisitor {
        ColumnProjector columnProjector;
        Dictionary<ParameterExpression, Expression> map;
        int aliasCount;
        IQueryProvider provider;

        internal QueryBinder(IQueryProvider provider) {
            this.provider = provider;
            this.columnProjector = new ColumnProjector(this.CanBeColumn);
        }

        private bool CanBeColumn(Expression expression) {
            return expression.NodeType == (ExpressionType)DbExpressionType.Column;
        }

        internal Expression Bind(Expression expression) {
            this.map = new Dictionary<ParameterExpression, Expression>();
            return this.Visit(expression);
        }

        private static Expression StripQuotes(Expression e) {
            while (e.NodeType == ExpressionType.Quote) {
                e = ((UnaryExpression)e).Operand;
            }
            return e;
        }

        private string GetNextAlias() {
            return "t" + (aliasCount++);
        }

        private ProjectedColumns ProjectColumns(Expression expression, string newAlias, params string[] existingAliases) {
            return this.columnProjector.ProjectColumns(expression, newAlias, existingAliases);
        }

        protected override Expression VisitMethodCall(MethodCallExpression m) {
            if (m.Method.DeclaringType == typeof(Queryable) ||
                m.Method.DeclaringType == typeof(Enumerable)) {
                switch (m.Method.Name) {
                    case "Where":
                        return this.BindWhere(m.Type, m.Arguments[0], (LambdaExpression)StripQuotes(m.Arguments[1]));
                    case "Select":
                        return this.BindSelect(m.Type, m.Arguments[0], (LambdaExpression)StripQuotes(m.Arguments[1]));
                    case "SelectMany":
                        if (m.Arguments.Count == 2) {
                            return this.BindSelectMany(
                                m.Type, m.Arguments[0], 
                                (LambdaExpression)StripQuotes(m.Arguments[1]),
                                null
                                );
                        }
                        else if (m.Arguments.Count == 3) {
                            return this.BindSelectMany(
                                m.Type, m.Arguments[0], 
                                (LambdaExpression)StripQuotes(m.Arguments[1]), 
                                (LambdaExpression)StripQuotes(m.Arguments[2])
                                );
                        }
                        break;
                    case "Join":
                        return this.BindJoin(
                            m.Type, m.Arguments[0], m.Arguments[1],
                            (LambdaExpression)StripQuotes(m.Arguments[2]),
                            (LambdaExpression)StripQuotes(m.Arguments[3]),
                            (LambdaExpression)StripQuotes(m.Arguments[4])
                            );
                    case "OrderBy":
                        return this.BindOrderBy(m.Type, m.Arguments[0], (LambdaExpression)StripQuotes(m.Arguments[1]), OrderType.Ascending);
                    case "OrderByDescending":
                        return this.BindOrderBy(m.Type, m.Arguments[0], (LambdaExpression)StripQuotes(m.Arguments[1]), OrderType.Descending);
                    case "ThenBy":
                        return this.BindThenBy(m.Arguments[0], (LambdaExpression)StripQuotes(m.Arguments[1]), OrderType.Ascending);
                    case "ThenByDescending":
                        return this.BindThenBy(m.Arguments[0], (LambdaExpression)StripQuotes(m.Arguments[1]), OrderType.Descending);
                }
            }
            return base.VisitMethodCall(m);
        }

        private Expression BindWhere(Type resultType, Expression source, LambdaExpression predicate) {
            ProjectionExpression projection = (ProjectionExpression)this.Visit(source);
            this.map[predicate.Parameters[0]] = projection.Projector;
            Expression where = this.Visit(predicate.Body);
            string alias = this.GetNextAlias();
            ProjectedColumns pc = this.ProjectColumns(projection.Projector, alias, projection.Source.Alias);
            return new ProjectionExpression(
                new SelectExpression(resultType, alias, pc.Columns, projection.Source, where),
                pc.Projector
                );
        }

        private Expression BindSelect(Type resultType, Expression source, LambdaExpression selector) {
            ProjectionExpression projection = (ProjectionExpression)this.Visit(source);
            this.map[selector.Parameters[0]] = projection.Projector;
            Expression expression = this.Visit(selector.Body);
            string alias = this.GetNextAlias();
            ProjectedColumns pc = this.ProjectColumns(expression, alias, projection.Source.Alias);
            return new ProjectionExpression(
                new SelectExpression(resultType, alias, pc.Columns, projection.Source, null),
                pc.Projector
                );
        }

        protected virtual Expression BindSelectMany(Type resultType, Expression source, LambdaExpression collectionSelector, LambdaExpression resultSelector) {
            ProjectionExpression projection = (ProjectionExpression)this.Visit(source);
            this.map[collectionSelector.Parameters[0]] = projection.Projector;
            ProjectionExpression collectionProjection = (ProjectionExpression)this.Visit(collectionSelector.Body);
            JoinType joinType = IsTable(collectionSelector.Body) ? JoinType.CrossJoin : JoinType.CrossApply;
            JoinExpression join = new JoinExpression(resultType, joinType, projection.Source, collectionProjection.Source, null);
            string alias = this.GetNextAlias();
            ProjectedColumns pc;
            if (resultSelector == null) {
                pc = this.ProjectColumns(collectionProjection.Projector, alias, projection.Source.Alias, collectionProjection.Source.Alias);
            }
            else {
                this.map[resultSelector.Parameters[0]] = projection.Projector;
                this.map[resultSelector.Parameters[1]] = collectionProjection.Projector;
                Expression result = this.Visit(resultSelector.Body);
                pc = this.ProjectColumns(result, alias, projection.Source.Alias, collectionProjection.Source.Alias);
            }
            return new ProjectionExpression(
                new SelectExpression(resultType, alias, pc.Columns, join, null),
                pc.Projector
                );
        }

        protected virtual Expression BindJoin(Type resultType, Expression outerSource, Expression innerSource, LambdaExpression outerKey, LambdaExpression innerKey, LambdaExpression resultSelector) {
            ProjectionExpression outerProjection = (ProjectionExpression)this.Visit(outerSource);
            ProjectionExpression innerProjection = (ProjectionExpression)this.Visit(innerSource);
            this.map[outerKey.Parameters[0]] = outerProjection.Projector;
            Expression outerKeyExpr = this.Visit(outerKey.Body);
            this.map[innerKey.Parameters[0]] = innerProjection.Projector;
            Expression innerKeyExpr = this.Visit(innerKey.Body);
            this.map[resultSelector.Parameters[0]] = outerProjection.Projector;
            this.map[resultSelector.Parameters[1]] = innerProjection.Projector;
            Expression resultExpr = this.Visit(resultSelector.Body);
            JoinExpression join = new JoinExpression(resultType, JoinType.InnerJoin, outerProjection.Source, innerProjection.Source, Expression.Equal(outerKeyExpr, innerKeyExpr));
            string alias = this.GetNextAlias();
            ProjectedColumns pc = this.ProjectColumns(resultExpr, alias, outerProjection.Source.Alias, innerProjection.Source.Alias);
            return new ProjectionExpression(
                new SelectExpression(resultType, alias, pc.Columns, join, null),
                pc.Projector
                );
        }

        List<OrderExpression> thenBys;

        protected virtual Expression BindOrderBy(Type resultType, Expression source, LambdaExpression orderSelector, OrderType orderType) {
            List<OrderExpression> myThenBys = this.thenBys;
            this.thenBys = null;
            ProjectionExpression projection = (ProjectionExpression)this.Visit(source);

            this.map[orderSelector.Parameters[0]] = projection.Projector;
            List<OrderExpression> orderings = new List<OrderExpression>();
            orderings.Add(new OrderExpression(orderType, this.Visit(orderSelector.Body)));

            if (myThenBys != null) {
                for (int i = myThenBys.Count - 1; i >= 0; i--) {
                    OrderExpression tb = myThenBys[i];
                    LambdaExpression lambda = (LambdaExpression)tb.Expression;
                    this.map[lambda.Parameters[0]] = projection.Projector;
                    orderings.Add(new OrderExpression(tb.OrderType, this.Visit(lambda.Body)));
                }
            }

            string alias = this.GetNextAlias();
            ProjectedColumns pc = this.ProjectColumns(projection.Projector, alias, projection.Source.Alias);
            return new ProjectionExpression(
                new SelectExpression(resultType, alias, pc.Columns, projection.Source, null, orderings.AsReadOnly()),
                pc.Projector
                );
        }

        protected virtual Expression BindThenBy(Expression source, LambdaExpression orderSelector, OrderType orderType) {
            if (this.thenBys == null) {
                this.thenBys = new List<OrderExpression>();
            }
            this.thenBys.Add(new OrderExpression(orderType, orderSelector));
            return this.Visit(source);
        }

        private bool IsTable(Expression expression) {
            ConstantExpression c = expression as ConstantExpression;
            return c != null && IsTable(c.Value);
        }

        private bool IsTable(object value) {
            IQueryable q = value as IQueryable;
            return q != null && q.Provider == this.provider && q.Expression.NodeType == ExpressionType.Constant;
        }

        private string GetTableName(object table) {
            IQueryable tableQuery = (IQueryable)table;
            Type rowType = tableQuery.ElementType;
            return rowType.Name;
        }

        private string GetColumnName(MemberInfo member) {
            return member.Name;
        }

        private Type GetColumnType(MemberInfo member) {
            FieldInfo fi = member as FieldInfo;
            if (fi != null) {
                return fi.FieldType;
            }
            PropertyInfo pi = (PropertyInfo)member;
            return pi.PropertyType;
        }

        private IEnumerable<MemberInfo> GetMappedMembers(Type rowType) {
            return rowType.GetFields().Cast<MemberInfo>();
        }

        private ProjectionExpression GetTableProjection(object value) {
            IQueryable table = (IQueryable)value;
            string tableAlias = this.GetNextAlias();
            string selectAlias = this.GetNextAlias();
            List<MemberBinding> bindings = new List<MemberBinding>();
            List<ColumnDeclaration> columns = new List<ColumnDeclaration>();
            foreach (MemberInfo mi in this.GetMappedMembers(table.ElementType)) {
                string columnName = this.GetColumnName(mi);
                Type columnType = this.GetColumnType(mi);
                int ordinal = columns.Count;
                bindings.Add(Expression.Bind(mi, new ColumnExpression(columnType, selectAlias, columnName, ordinal)));
                columns.Add(new ColumnDeclaration(columnName, new ColumnExpression(columnType, tableAlias, columnName, ordinal)));
            }
            Expression projector = Expression.MemberInit(Expression.New(table.ElementType), bindings);
            Type resultType = typeof(IEnumerable<>).MakeGenericType(table.ElementType);
            return new ProjectionExpression(
                new SelectExpression(
                    resultType,
                    selectAlias,
                    columns,
                    new TableExpression(resultType, tableAlias, this.GetTableName(table)),
                    null
                    ),
                projector
                );
        }

        protected override Expression VisitConstant(ConstantExpression c) {
            if (this.IsTable(c.Value)) {
                return GetTableProjection(c.Value);
            }
            return c;
        }

        protected override Expression VisitParameter(ParameterExpression p) {
            Expression e;
            if (this.map.TryGetValue(p, out e)) {
                return e;
            }
            return p;
        }

        protected override Expression VisitMemberAccess(MemberExpression m) {
            Expression source = this.Visit(m.Expression);
            switch (source.NodeType) {
                case ExpressionType.MemberInit:
                    MemberInitExpression min = (MemberInitExpression)source;
                    for (int i = 0, n = min.Bindings.Count; i < n; i++) {
                        MemberAssignment assign = min.Bindings[i] as MemberAssignment;
                        if (assign != null && MembersMatch(assign.Member, m.Member)) {
                            return assign.Expression;
                        }
                    }
                    break;
                case ExpressionType.New:
                    NewExpression nex = (NewExpression)source;
                    if (nex.Members != null) {
                        for (int i = 0, n = nex.Members.Count; i < n; i++) {
                            if (MembersMatch(nex.Members[i], m.Member)) {
                                return nex.Arguments[i];
                            }
                        }
                    }
                    break;
            }
            if (source == m.Expression) {
                return m;
            }
            return MakeMemberAccess(source, m.Member);
        }

        private bool MembersMatch(MemberInfo a, MemberInfo b) {
            if (a == b) {
                return true;
            }
            if (a is MethodInfo && b is PropertyInfo) {
                return a == ((PropertyInfo)b).GetGetMethod();
            }
            else if (a is PropertyInfo && b is MethodInfo) {
                return ((PropertyInfo)a).GetGetMethod() == b;
            }
            return false;
        }

        private Expression MakeMemberAccess(Expression source, MemberInfo mi) {
            FieldInfo fi = mi as FieldInfo;
            if (fi != null) {
                return Expression.Field(source, fi);
            }
            PropertyInfo pi = (PropertyInfo)mi;
            return Expression.Property(source, pi);
        }
    }
}
