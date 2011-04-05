// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data
{
    /// <summary>
    /// Rewrites top level skips to a client-side skip
    /// </summary>
    public class ClientSkipRewriter : DbExpressionVisitor
    {
        private ClientSkipRewriter()
        {
        }

        public static Expression Rewrite(Expression expression)
        {
            return new ClientSkipRewriter().Visit(expression);
        }

        protected override Expression VisitProjection(ProjectionExpression proj)
        {
            if (proj.Select.Skip != null)
            {
                Expression newTake = (proj.Select.Take != null) ? Expression.Add(proj.Select.Skip, proj.Select.Take) : null;
                if (newTake != null)
                {
                    newTake = PartialEvaluator.Eval(newTake);
                }
                var newSelect = proj.Select.SetSkip(null).SetTake(newTake);
                var elementType = TypeHelper.GetElementType(proj.Type);
                var agg = proj.Aggregator;
                var p = agg != null ? agg.Parameters[0] : Expression.Parameter(elementType, "p");
                var skip = Expression.Call(typeof(Enumerable), "Skip", new Type[]{elementType}, p, proj.Select.Skip);
                if (agg != null) {
                    agg = (LambdaExpression)DbExpressionReplacer.Replace(agg, p, skip);
                }
                else {
                    agg = Expression.Lambda(skip, p);
                }
                return new ProjectionExpression(newSelect, proj.Projector, agg);
            }
            return proj;
        }
    }
}