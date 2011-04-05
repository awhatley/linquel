using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Sample
{
    internal class RedundantSubqueryRemover : DbExpressionVisitor
    {
        private RedundantSubqueryRemover() 
        {
        }

        internal static Expression Remove(Expression expression)
        {
            return new RedundantSubqueryRemover().Visit(expression);
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            select = (SelectExpression)base.VisitSelect(select);

            // first remove all purely redundant subqueries
            List<SelectExpression> redundant = RedundantSubqueryGatherer.Gather(select.From);
            if (redundant != null)
            {
                select = SubqueryRemover.Remove(select, redundant);
            }

            // next attempt to merge subqueries that would have been removed by the above
            // logic except for the existence of a where clause
            while (CanMergeWithFrom(select))
            {
                SelectExpression fromSelect = (SelectExpression)select.From;

                // remove the redundant subquery
                select = SubqueryRemover.Remove(select, fromSelect);

                // merge where expressions 
                Expression where = select.Where;
                if (fromSelect.Where != null)
                {
                    if (where != null)
                    {
                        where = Expression.And(fromSelect.Where, where);
                    }
                    else
                    {
                        where = fromSelect.Where;
                    }
                }
                if (where != select.Where)
                {
                    select = new SelectExpression(select.Type, select.Alias, select.Columns, select.From, where, select.OrderBy, select.GroupBy);
                }
            }

            return select;
        }

        protected override Expression VisitSubquery(SubqueryExpression subquery)
        {
            return base.VisitSubquery(subquery);
        }

        protected override Expression VisitProjection(ProjectionExpression proj)
        {
            proj = (ProjectionExpression)base.VisitProjection(proj);
            if (proj.Source.From is SelectExpression) {
                List<SelectExpression> redundant = RedundantSubqueryGatherer.Gather(proj.Source);
                if (redundant != null) 
                {
                    proj = SubqueryRemover.Remove(proj, redundant);
                }
            }
            return proj;
        }

        private static bool CanMergeWithFrom(SelectExpression select)
        {
            SelectExpression fromSelect = select.From as SelectExpression;
            if (fromSelect == null) return false;
            return (ProjectionIsSimple(fromSelect) || ProjectionIsNameMapOnly(fromSelect))
                && (fromSelect.OrderBy == null || fromSelect.OrderBy.Count == 0)
                && (fromSelect.GroupBy == null || fromSelect.GroupBy.Count == 0);
        }

        private static bool ProjectionIsSimple(SelectExpression select)
        {
            foreach (ColumnDeclaration decl in select.Columns)
            {
                ColumnExpression col = decl.Expression as ColumnExpression;
                if (col == null || decl.Name != col.Name)
                {
                    return false;
                }
            }
            return true;
        }

        private static bool ProjectionIsNameMapOnly(SelectExpression select)
        {
            SelectExpression fromSelect = select.From as SelectExpression;
            if (fromSelect == null || select.Columns.Count != fromSelect.Columns.Count)
                return false;
            // test that all columns in 'select' are refering to columns in the same position
            // in 'fromSelect'.
            for (int i = 0, n = select.Columns.Count; i < n; i++)
            {
                ColumnExpression col = select.Columns[i].Expression as ColumnExpression;
                if (col == null || !(col.Name == fromSelect.Columns[i].Name))
                    return false;
            }
            return true;
        }

        class RedundantSubqueryGatherer : DbExpressionVisitor
        {
            List<SelectExpression> redundant;

            private RedundantSubqueryGatherer()
            {
            }

            internal static List<SelectExpression> Gather(Expression source)
            {
                RedundantSubqueryGatherer gatherer = new RedundantSubqueryGatherer();
                gatherer.Visit(source);
                return gatherer.redundant;
            }

            private static bool IsRedudantSubquery(SelectExpression select)
            {
                return (select.From is SelectExpression || select.From is TableExpression)
                    && (ProjectionIsSimple(select) || ProjectionIsNameMapOnly(select))
                    && (select.Where == null)
                    && (select.OrderBy == null || select.OrderBy.Count == 0)
                    && (select.GroupBy == null || select.GroupBy.Count == 0);
            }

            protected override Expression VisitSelect(SelectExpression select)
            {
                if (IsRedudantSubquery(select))
                {
                    if (this.redundant == null)
                    {
                        this.redundant = new List<SelectExpression>();
                    }
                    this.redundant.Add(select);
                }
                return select;
            }
        }
    }
}