﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data
{
    /// <summary>
    /// Rewrites queries with skip & take to use the nested queries with inverted ordering technique
    /// </summary>
    public class SkipToNestedOrderByRewriter : DbExpressionVisitor
    {
        private SkipToNestedOrderByRewriter()
        {
        }

        public static Expression Rewrite(Expression expression)
        {
            return new SkipToNestedOrderByRewriter().Visit(expression);
        }

        protected override Expression VisitProjection(ProjectionExpression proj)
        {
            // select * from table order by x skip s take t 
            // =>
            // select * from (select top s * from (select top s + t from table order by x) order by -x) order by x

            if (proj.Select.Skip != null && proj.Select.Take != null && proj.Select.OrderBy.Count > 0)
            {
                var skip = proj.Select.Skip;
                var take = proj.Select.Take;
                var select = proj.Select;
                var skipPlusTake = PartialEvaluator.Eval(Expression.Add(skip, take));

                select = proj.Select.SetTake(skipPlusTake).SetSkip(null);
                select = select.AddRedundantSelect(new TableAlias());
                select = select.SetTake(take);

                // propogate order-bys to new layer
                select = (SelectExpression)OrderByRewriter.Rewrite(select);
                var inverted = select.OrderBy.Select(ob => new OrderExpression(
                    ob.OrderType == OrderType.Ascending ? OrderType.Descending : OrderType.Ascending,
                    ob.Expression
                    ));
                select = select.SetOrderBy(inverted);

                select = select.AddRedundantSelect(new TableAlias());
                select = select.SetTake(Expression.Constant(0)); // temporary
                select = (SelectExpression)OrderByRewriter.Rewrite(select);
                var reverted = select.OrderBy.Select(ob => new OrderExpression(
                    ob.OrderType == OrderType.Ascending ? OrderType.Descending : OrderType.Ascending,
                    ob.Expression
                    ));
                select = select.SetOrderBy(reverted);
                select = select.SetTake(null);

                return new ProjectionExpression(select, proj.Projector, proj.Aggregator);
            }
            return proj;
        }
    }
}