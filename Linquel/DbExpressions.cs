using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Sample {

    /// <summary>
    /// Extended node types for custom expressions
    /// </summary>
    internal enum DbExpressionType {
        Table = 1000, // make sure these don't overlap with ExpressionType
        Column,
        Select,
        Projection,
        Join
    }

    internal static class DbExpressionExtensions {
        internal static bool IsDbExpression(this ExpressionType et) {
            return ((int)et) >= 1000;
        }
    }

    /// <summary>
    /// A custom expression node that represents a table reference in a SQL query
    /// </summary>
    internal class TableExpression : Expression {
        string alias;
        string name;
        internal TableExpression(Type type, string alias, string name)
            : base((ExpressionType)DbExpressionType.Table, type) {
            this.alias = alias;
            this.name = name;
        }
        internal string Alias {
            get { return this.alias; }
        }
        internal string Name {
            get { return this.name; }
        }
    }

    /// <summary>
    /// A custom expression node that represents a reference to a column in a SQL query
    /// </summary>
    internal class ColumnExpression : Expression {
        string alias;
        string name;
        int ordinal;
        internal ColumnExpression(Type type, string alias, string name, int ordinal)
            : base((ExpressionType)DbExpressionType.Column, type) {
            this.alias = alias;
            this.name = name;
            this.ordinal = ordinal;
        }
        internal string Alias {
            get { return this.alias; }
        }
        internal string Name {
            get { return this.name; }
        }
        internal int Ordinal {
            get { return this.ordinal; }
        }
    }

    /// <summary>
    /// A declaration of a column in a SQL SELECT expression
    /// </summary>
    internal class ColumnDeclaration {
        string name;
        Expression expression;
        internal ColumnDeclaration(string name, Expression expression) {
            this.name = name;
            this.expression = expression;
        }
        internal string Name {
            get { return this.name; }
        }
        internal Expression Expression {
            get { return this.expression; }
        }
    }

    /// <summary>
    /// An SQL OrderBy order type 
    /// </summary>
    internal enum OrderType {
        Ascending,
        Descending
    }

    /// <summary>
    /// A pairing of an expression and an order type for use in a SQL Order By clause
    /// </summary>
    internal class OrderExpression {
        OrderType orderType;
        Expression expression;
        internal OrderExpression(OrderType orderType, Expression expression) {
            this.orderType = orderType;
            this.expression = expression;
        }
        internal OrderType OrderType {
            get { return this.orderType; }
        }
        internal Expression Expression {
            get { return this.expression; }
        }
    }

    /// <summary>
    /// A custom expression node used to represent a SQL SELECT expression
    /// </summary>
    internal class SelectExpression : Expression {
        string alias;
        ReadOnlyCollection<ColumnDeclaration> columns;
        Expression from;
        Expression where;
        ReadOnlyCollection<OrderExpression> orderBy;

        internal SelectExpression(
            Type type, string alias, IEnumerable<ColumnDeclaration> columns, 
            Expression from, Expression where, IEnumerable<OrderExpression> orderBy)
            : base((ExpressionType)DbExpressionType.Select, type) {
            this.alias = alias;
            this.columns = columns as ReadOnlyCollection<ColumnDeclaration>;
            if (this.columns == null) {
                this.columns = new List<ColumnDeclaration>(columns).AsReadOnly();
            }
            this.from = from;
            this.where = where;
            this.orderBy = orderBy as ReadOnlyCollection<OrderExpression>;
            if (this.orderBy == null && orderBy != null) {
                this.orderBy = new List<OrderExpression>(orderBy).AsReadOnly();
            }
        }
        internal SelectExpression(
            Type type, string alias, IEnumerable<ColumnDeclaration> columns, 
            Expression from, Expression where)
            : this(type, alias, columns, from, where, null) {
        }
        internal string Alias {
            get { return this.alias; }
        }
        internal ReadOnlyCollection<ColumnDeclaration> Columns {
            get { return this.columns; }
        }
        internal Expression From {
            get { return this.from; }
        }
        internal Expression Where {
            get { return this.where; }
        }
        internal ReadOnlyCollection<OrderExpression> OrderBy {
            get { return this.orderBy; }
        }
    }

    /// <summary>
    /// A kind of SQL join
    /// </summary>
    internal enum JoinType {
        CrossJoin,
        InnerJoin,
        CrossApply,
    }

    /// <summary>
    /// A custom expression node representing a SQL join clause
    /// </summary>
    internal class JoinExpression : Expression {
        JoinType joinType;
        Expression left;
        Expression right;
        Expression condition;
        internal JoinExpression(Type type, JoinType joinType, Expression left, Expression right, Expression condition)
            : base((ExpressionType)DbExpressionType.Join, type) {
            this.joinType = joinType;
            this.left = left;
            this.right = right;
            this.condition = condition;
        }
        internal JoinType Join {
            get { return this.joinType; }
        }
        internal Expression Left {
            get { return this.left; }
        }
        internal Expression Right {
            get { return this.right; }
        }
        internal new Expression Condition {
            get { return this.condition; }
        }
    }

    /// <summary>
    /// A custom expression representing the construction of one or more result objects from a 
    /// SQL select expression
    /// </summary>
    internal class ProjectionExpression : Expression {
        SelectExpression source;
        Expression projector;
        internal ProjectionExpression(SelectExpression source, Expression projector)
            : base((ExpressionType)DbExpressionType.Projection, source.Type) {
            this.source = source;
            this.projector = projector;
        }
        internal SelectExpression Source {
            get { return this.source; }
        }
        internal Expression Projector {
            get { return this.projector; }
        }
    }

    /// <summary>
    /// An extended expression visitor including custom DbExpression nodes
    /// </summary>
    internal class DbExpressionVisitor : ExpressionVisitor {
        protected override Expression Visit(Expression exp) {
            if (exp == null) {
                return null;
            }
            switch ((DbExpressionType)exp.NodeType) {
                case DbExpressionType.Table:
                    return this.VisitTable((TableExpression)exp);
                case DbExpressionType.Column:
                    return this.VisitColumn((ColumnExpression)exp);
                case DbExpressionType.Select:
                    return this.VisitSelect((SelectExpression)exp);
                case DbExpressionType.Join:
                    return this.VisitJoin((JoinExpression)exp);
                case DbExpressionType.Projection:
                    return this.VisitProjection((ProjectionExpression)exp);
                default:
                    return base.Visit(exp);
            }
        }
        protected virtual Expression VisitTable(TableExpression table) {
            return table;
        }
        protected virtual Expression VisitColumn(ColumnExpression column) {
            return column;
        }
        protected virtual Expression VisitSelect(SelectExpression select) {
            Expression from = this.VisitSource(select.From);
            Expression where = this.Visit(select.Where);
            ReadOnlyCollection<ColumnDeclaration> columns = this.VisitColumnDeclarations(select.Columns);
            ReadOnlyCollection<OrderExpression> orderBy = this.VisitOrderBy(select.OrderBy);
            if (from != select.From || where != select.Where || columns != select.Columns || orderBy != select.OrderBy) {
                return new SelectExpression(select.Type, select.Alias, columns, from, where, orderBy);
            }
            return select;
        }
        protected virtual Expression VisitJoin(JoinExpression join) {
            Expression left = this.VisitSource(join.Left);
            Expression right = this.VisitSource(join.Right);
            Expression condition = this.Visit(join.Condition);
            if (left != join.Left || right != join.Right || condition != join.Condition) {
                return new JoinExpression(join.Type, join.Join, left, right, condition);
            }
            return join;
        }
        protected virtual Expression VisitSource(Expression source) {
            return this.Visit(source);
        }
        protected virtual Expression VisitProjection(ProjectionExpression proj) {
            SelectExpression source = (SelectExpression)this.Visit(proj.Source);
            Expression projector = this.Visit(proj.Projector);
            if (source != proj.Source || projector != proj.Projector) {
                return new ProjectionExpression(source, projector);
            }
            return proj;
        }
        protected ReadOnlyCollection<ColumnDeclaration> VisitColumnDeclarations(ReadOnlyCollection<ColumnDeclaration> columns) {
            List<ColumnDeclaration> alternate = null;
            for (int i = 0, n = columns.Count; i < n; i++) {
                ColumnDeclaration column = columns[i];
                Expression e = this.Visit(column.Expression);
                if (alternate == null && e != column.Expression) {
                    alternate = columns.Take(i).ToList();
                }
                if (alternate != null) {
                    alternate.Add(new ColumnDeclaration(column.Name, e));
                }
            }
            if (alternate != null) {
                return alternate.AsReadOnly();
            }
            return columns;
        }
        protected ReadOnlyCollection<OrderExpression> VisitOrderBy(ReadOnlyCollection<OrderExpression> expressions) {
            if (expressions != null) {
                List<OrderExpression> alternate = null;
                for (int i = 0, n = expressions.Count; i < n; i++) {
                    OrderExpression expr = expressions[i];
                    Expression e = this.Visit(expr.Expression);
                    if (alternate == null && e != expr.Expression) {
                        alternate = expressions.Take(i).ToList();
                    }
                    if (alternate != null) {
                        alternate.Add(new OrderExpression(expr.OrderType, e));
                    }
                }
                if (alternate != null) {
                    return alternate.AsReadOnly();
                }
            }
            return expressions;
        }
    }
}
