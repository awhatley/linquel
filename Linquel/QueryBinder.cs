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
        int aliasCount;
        IQueryProvider provider;
        Dictionary<ParameterExpression, Expression> map;
        Dictionary<Expression, GroupByInfo> groupByMap;
        Expression root;

        private QueryBinder(IQueryProvider provider, Expression root) {
            this.provider = provider;
            this.map = new Dictionary<ParameterExpression, Expression>();
            this.groupByMap = new Dictionary<Expression, GroupByInfo>();
            this.root = root;
        }

        internal static Expression Bind(IQueryProvider provider, Expression expression) {
            return new QueryBinder(provider, expression).Visit(expression);
        }

        private bool CanBeColumn(Expression expression) {
            switch (expression.NodeType) {
                case (ExpressionType)DbExpressionType.Column:
                case (ExpressionType)DbExpressionType.Subquery:
                case (ExpressionType)DbExpressionType.AggregateSubquery:
                case (ExpressionType)DbExpressionType.Aggregate:
                    return true;
                default:
                    return false;
            }
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
            return ColumnProjector.ProjectColumns(this.CanBeColumn, expression, newAlias, existingAliases);
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
                    case "GroupBy":
                        if (m.Arguments.Count == 2) {
                            return this.BindGroupBy(
                                m.Arguments[0],
                                (LambdaExpression)StripQuotes(m.Arguments[1]),
                                null,
                                null
                                );
                        }
                        else if (m.Arguments.Count == 3) {
                            return this.BindGroupBy(
                                m.Arguments[0],
                                (LambdaExpression)StripQuotes(m.Arguments[1]),
                                (LambdaExpression)StripQuotes(m.Arguments[2]),
                                null
                                );
                        }
                        else if (m.Arguments.Count == 4) {
                            return this.BindGroupBy(
                                m.Arguments[0], 
                                (LambdaExpression)StripQuotes(m.Arguments[1]), 
                                (LambdaExpression)StripQuotes(m.Arguments[2]), 
                                (LambdaExpression)StripQuotes(m.Arguments[3])
                                );
                        }
                        break;
                    case "Count":
                    case "Min":
                    case "Max":
                    case "Sum":
                    case "Average":
                        if (m.Arguments.Count == 1) {
                            return this.BindAggregate(m.Arguments[0], m.Method, null, m == this.root);
                        }
                        else if (m.Arguments.Count == 2) {
                            LambdaExpression selector = (LambdaExpression)StripQuotes(m.Arguments[1]);
                            return this.BindAggregate(m.Arguments[0], m.Method, selector, m == this.root);
                        }
                        break;
                }
            }
            return base.VisitMethodCall(m);
        }

        private ProjectionExpression VisitSequence(Expression source) {
            return this.ConvertToSequence(this.Visit(source));
        }

        private ProjectionExpression ConvertToSequence(Expression expr) {
            switch (expr.NodeType) {
                case (ExpressionType)DbExpressionType.Projection:
                    return (ProjectionExpression)expr;
                case ExpressionType.New:
                    NewExpression nex = (NewExpression)expr;
                    if (expr.Type.IsGenericType && expr.Type.GetGenericTypeDefinition() == typeof(Grouping<,>)) {
                        return (ProjectionExpression)nex.Arguments[1];
                    }
                    goto default;
                default:
                    throw new Exception(string.Format("The expression of type '{0}' is not a sequence", expr.Type));
            }
        }

        private Expression BindWhere(Type resultType, Expression source, LambdaExpression predicate) {
            ProjectionExpression projection = this.VisitSequence(source);
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
            ProjectionExpression projection = this.VisitSequence(source);
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
            ProjectionExpression projection = this.VisitSequence(source);
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
                Expression result = this.VisitSequence(resultSelector.Body);
                pc = this.ProjectColumns(result, alias, projection.Source.Alias, collectionProjection.Source.Alias);
            }
            return new ProjectionExpression(
                new SelectExpression(resultType, alias, pc.Columns, join, null),
                pc.Projector
                );
        }

        protected virtual Expression BindJoin(Type resultType, Expression outerSource, Expression innerSource, LambdaExpression outerKey, LambdaExpression innerKey, LambdaExpression resultSelector) {
            ProjectionExpression outerProjection = this.VisitSequence(outerSource);
            ProjectionExpression innerProjection = this.VisitSequence(innerSource);
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
            ProjectionExpression projection = this.VisitSequence(source);

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
                new SelectExpression(resultType, alias, pc.Columns, projection.Source, null, orderings.AsReadOnly(), null),
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

        protected virtual Expression BindGroupBy(Expression source, LambdaExpression keySelector, LambdaExpression elementSelector, LambdaExpression resultSelector) {
            ProjectionExpression projection = this.VisitSequence(source);
            
            this.map[keySelector.Parameters[0]] = projection.Projector;
            Expression keyExpr = this.Visit(keySelector.Body);            

            Expression elemExpr = projection.Projector;
            if (elementSelector != null) {
                this.map[elementSelector.Parameters[0]] = projection.Projector;
                elemExpr = this.Visit(elementSelector.Body);
            }
            
            // Use ProjectColumns to get group-by expressions from key expression
            ProjectedColumns keyProjection = this.ProjectColumns(keyExpr, projection.Source.Alias, projection.Source.Alias);
            IEnumerable<Expression> groupExprs = keyProjection.Columns.Select(c => c.Expression);

            // make duplicate of source query as basis of element subquery by visiting the source again
            ProjectionExpression subqueryBasis = this.VisitSequence(source);

            // recompute key columns for group expressions relative to subquery (need these for doing the correlation predicate)
            this.map[keySelector.Parameters[0]] = subqueryBasis.Projector;
            Expression subqueryKey = this.Visit(keySelector.Body);
            
            // use same projection trick to get group-by expressions based on subquery
            ProjectedColumns subqueryKeyPC = this.ProjectColumns(subqueryKey, subqueryBasis.Source.Alias, subqueryBasis.Source.Alias);
            IEnumerable<Expression> subqueryGroupExprs = subqueryKeyPC.Columns.Select(c => c.Expression);
            Expression subqueryCorrelation = this.BuildPredicateWithNullsEqual(subqueryGroupExprs, groupExprs);
            
            // compute element based on duplicated subquery
            Expression subqueryElemExpr = subqueryBasis.Projector;
            if (elementSelector != null) {
                this.map[elementSelector.Parameters[0]] = subqueryBasis.Projector;
                subqueryElemExpr = this.Visit(elementSelector.Body);
            }
            
            // build subquery that projects the desired element
            string elementAlias = this.GetNextAlias();
            ProjectedColumns elementPC = this.ProjectColumns(subqueryElemExpr, elementAlias, subqueryBasis.Source.Alias);
            ProjectionExpression elementSubquery =
                new ProjectionExpression(
                    new SelectExpression(TypeSystem.GetSequenceType(subqueryElemExpr.Type), elementAlias, elementPC.Columns, subqueryBasis.Source, subqueryCorrelation),
                    elementPC.Projector
                    );

            string alias = this.GetNextAlias();

            // make it possible to tie aggregates back to this group-by
            GroupByInfo info = new GroupByInfo(alias, elemExpr);
            this.groupByMap.Add(elementSubquery, info);

            Expression resultExpr;
            if (resultSelector != null) {
                Expression saveGroupElement = this.currentGroupElement;
                this.currentGroupElement = elementSubquery;
                // compute result expression based on key & element-subquery
                this.map[resultSelector.Parameters[0]] = keyProjection.Projector;
                this.map[resultSelector.Parameters[1]] = elementSubquery;
                resultExpr = this.Visit(resultSelector.Body);
                this.currentGroupElement = saveGroupElement;
            }
            else {
                // result must be IGrouping<K,E>
                resultExpr = Expression.New(
                    typeof(Grouping<,>).MakeGenericType(keyExpr.Type, subqueryElemExpr.Type).GetConstructors()[0],
                    new Expression[] { keyExpr, elementSubquery }
                    );
            }

            ProjectedColumns pc = this.ProjectColumns(resultExpr, alias, projection.Source.Alias);

            // make it possible to tie aggregates back to this group-by
            Expression projectedElementSubquery = ((NewExpression)pc.Projector).Arguments[1];
            this.groupByMap.Add(projectedElementSubquery, info);

            return new ProjectionExpression(
                new SelectExpression(TypeSystem.GetSequenceType(resultExpr.Type), alias, pc.Columns, projection.Source, null, null, groupExprs),
                pc.Projector
                );
        }

        private Expression BuildPredicateWithNullsEqual(IEnumerable<Expression> source1, IEnumerable<Expression> source2) {
            IEnumerator<Expression> en1 = source1.GetEnumerator();
            IEnumerator<Expression> en2 = source2.GetEnumerator();
            Expression result = null;
            while (en1.MoveNext() && en2.MoveNext()) {
                Expression compare =
                    Expression.Or(
                        Expression.And(new IsNullExpression(en1.Current), new IsNullExpression(en2.Current)),
                        Expression.Equal(en1.Current, en2.Current)
                        );
                result = (result == null) ? compare : Expression.And(result, compare);
            }
            return result;
        }

        Expression currentGroupElement;

        class GroupByInfo {
            internal string Alias { get; private set; }
            internal Expression Element { get; private set; }
            internal GroupByInfo(string alias, Expression element) {
                this.Alias = alias;
                this.Element = element;
            }
        }

        private AggregateType GetAggregateType(string methodName) {
            switch (methodName) {
                case "Count": return AggregateType.Count;
                case "Min": return AggregateType.Min;
                case "Max": return AggregateType.Max;
                case "Sum": return AggregateType.Sum;
                case "Average": return AggregateType.Average;
                default: throw new Exception(string.Format("Unknown aggregate type: {0}", methodName));
            }
        }

        private bool HasPredicateArg(AggregateType aggregateType) {
            return aggregateType == AggregateType.Count;
        }

        private Expression BindAggregate(Expression source, MethodInfo method, LambdaExpression argument, bool isRoot) {
            Type returnType = method.ReturnType;
            AggregateType aggType = this.GetAggregateType(method.Name);
            bool hasPredicateArg = this.HasPredicateArg(aggType);
            bool argumentWasPredicate = false;

            if (argument != null && hasPredicateArg) {
                // convert query.Count(predicate) into query.Where(predicate).Count()
                source = Expression.Call(typeof(Queryable), "Where", method.GetGenericArguments(), source, argument);
                argument = null;
                argumentWasPredicate = true;
            }

            ProjectionExpression projection = this.VisitSequence(source);

            Expression argExpr = null;
            if (argument != null) {
                this.map[argument.Parameters[0]] = projection.Projector;
                argExpr = this.Visit(argument.Body);
            }
            else if (!hasPredicateArg) {
                argExpr = projection.Projector;
            }

            string alias = this.GetNextAlias();
            var pc = this.ProjectColumns(projection.Projector, alias, projection.Source.Alias);
            Expression aggExpr = new AggregateExpression(returnType, aggType, argExpr);
            Type selectType = typeof(IEnumerable<>).MakeGenericType(returnType);
            SelectExpression select = new SelectExpression(selectType, alias, new ColumnDeclaration[] { new ColumnDeclaration("", aggExpr) }, projection.Source, null);

            if (isRoot) {
                ParameterExpression p = Expression.Parameter(selectType, "p");
                LambdaExpression gator = Expression.Lambda(Expression.Call(typeof(Enumerable), "Single", new Type[] { returnType }, p), p);
                return new ProjectionExpression(select, new ColumnExpression(returnType, alias, ""), gator);
            }

            SubqueryExpression subquery = new SubqueryExpression(returnType, select);

            // if we can find the corresponding group-info we can build a special AggregateSubquery node that will enable us to 
            // optimize the aggregate expression later using AggregateRewriter
            GroupByInfo info;
            if (!argumentWasPredicate && this.groupByMap.TryGetValue(projection, out info)) {
                // use the element expression from the group-by info to rebind the argument so the resulting expression is one that 
                // would be legal to add to the columns in the select expression that has the corresponding group-by clause.
                if (argument != null) {
                    this.map[argument.Parameters[0]] = info.Element;
                    argExpr = this.Visit(argument.Body);
                }
                else if (!hasPredicateArg) {
                    argExpr = info.Element;
                }
                aggExpr = new AggregateExpression(returnType, aggType, argExpr);

                // check for easy to optimize case.  If the projection that our aggregate is based on is really the 'group' argument from
                // the query.GroupBy(xxx, (key, group) => yyy) method then whatever expression we return here will automatically
                // become part of the select expression that has the group-by clause, so just return the simple aggregate expression.
                if (projection == this.currentGroupElement)
                    return aggExpr;

                return new AggregateSubqueryExpression(info.Alias, aggExpr, subquery);
            }

            return subquery;
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
                bindings.Add(Expression.Bind(mi, new ColumnExpression(columnType, selectAlias, columnName)));
                columns.Add(new ColumnDeclaration(columnName, new ColumnExpression(columnType, tableAlias, columnName)));
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
                    else if (nex.Type.IsGenericType && nex.Type.GetGenericTypeDefinition() == typeof(Grouping<,>)) {
                        if (m.Member.Name == "Key") {
                            return nex.Arguments[0];
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
