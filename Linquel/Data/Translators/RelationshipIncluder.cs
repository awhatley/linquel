// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQ.Data
{
    /// <summary>
    /// Adds relationship to query results depending on policy
    /// </summary>
    public class RelationshipIncluder : DbExpressionVisitor
    {
        QueryPolicy policy;
        QueryMapping mapping;
        ScopedDictionary<MemberInfo, bool> includeScope = new ScopedDictionary<MemberInfo, bool>(null);

        private RelationshipIncluder(QueryPolicy policy)
        {
            this.policy = policy;
            this.mapping = policy.Mapping;
        }

        public static Expression Include(QueryPolicy policy, Expression expression)
        {
            return new RelationshipIncluder(policy).Visit(expression);
        }

        protected override Expression VisitProjection(ProjectionExpression proj)
        {
            Expression projector = this.Visit(proj.Projector);
            return this.UpdateProjection(proj, proj.Select, projector, proj.Aggregator);
        }

        protected override Expression VisitEntity(EntityExpression entity)
        {
            MemberInitExpression init = entity.Expression as MemberInitExpression;
            if (init != null)
            {
                var save = this.includeScope;
                this.includeScope = new ScopedDictionary<MemberInfo,bool>(this.includeScope);

                Dictionary<MemberInfo, MemberBinding> existing = init.Bindings.ToDictionary(b => b.Member);
                List<MemberBinding> newBindings = null;
                foreach (var mi in this.mapping.GetMappedMembers(entity.Entity))
                {
                    if (!existing.ContainsKey(mi) && this.mapping.IsRelationship(entity.Entity, mi) && this.policy.IsIncluded(mi))
                    {
                        if (this.includeScope.ContainsKey(mi))
                        {
                            throw new NotSupportedException(string.Format("Cannot include '{0}.{1}' recursively.", mi.DeclaringType.Name, mi.Name));
                        }
                        Expression me = this.mapping.GetMemberExpression(init, entity.Entity, mi);
                        if (newBindings == null)
                        {
                            newBindings = new List<MemberBinding>(init.Bindings);
                        }
                        newBindings.Add(Expression.Bind(mi, me));
                    }
                }
                if (newBindings != null)
                {
                    entity = new EntityExpression(entity.Entity, Expression.MemberInit(init.NewExpression, newBindings));
                }

                this.includeScope = save;
            }
            return base.VisitEntity(entity);
        }
    }
}
