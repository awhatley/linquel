using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Sample {

    internal enum DbExpressionType {
        Table = 1000, // make sure these don't overlap with ExpressionType
        Column,
        Select,
        Projection
    }

    internal static class DbExpressionExtensions {
        internal static bool IsDbExpression(this ExpressionType et) {
            return ((int)et) >= 1000;
        }
    }

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

    internal class SelectExpression : Expression {
        string alias;
        ReadOnlyCollection<ColumnDeclaration> columns;
        Expression from;
        Expression where;
        internal SelectExpression(Type type, string alias, IEnumerable<ColumnDeclaration> columns, Expression from, Expression where)
            : base((ExpressionType)DbExpressionType.Select, type) {
            this.alias = alias;
            this.columns = columns as ReadOnlyCollection<ColumnDeclaration>;
            if (this.columns == null) {
                this.columns = new List<ColumnDeclaration>(columns).AsReadOnly();
            }
            this.from = from;
            this.where = where;
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
    }

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
            if (from != select.From || where != select.Where || columns != select.Columns) {
                return new SelectExpression(select.Type, select.Alias, columns, from, where);
            }
            return select;
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
    }
}
