// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQ.Data
{
    public class MappingEntity
    {
        public string TableID { get; private set; }
        public Type Type { get; private set; }

        public MappingEntity(Type type, string tableID)
        {
            this.TableID = tableID;
            this.Type = type;
        }
    }

    /// <summary>
    /// Defines mapping information & rules for the query provider
    /// </summary>
    public abstract class QueryMapping
    {
        QueryLanguage language;

        protected QueryMapping(QueryLanguage language)
        {
            this.language = language;
        }

        /// <summary>
        /// The language related to the mapping
        /// </summary>
        public QueryLanguage Language
        {
            get { return this.language; }
        }

        /// <summary>
        /// Get the meta entity directly corresponding to the CLR type
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public virtual MappingEntity GetEntity(Type type)
        {
            return this.GetEntity(type, type.Name);
        }

        /// <summary>
        /// Get the meta entity that maps between the CLR type and the database table 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public virtual MappingEntity GetEntity(Type type, string tableID)
        {
            return new MappingEntity(type, tableID);
        }

        /// <summary>
        /// Deterimines is a property is mapped onto a column or relationship
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsMapped(MappingEntity entity, MemberInfo member)
        {
            return true;
        }

        /// <summary>
        /// Determines if a property is mapped onto a column
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsColumn(MappingEntity entity, MemberInfo member)
        {
            return this.IsMapped(entity, member) && this.language.IsScalar(TypeHelper.GetMemberType(member));
        }

        /// <summary>
        /// Determines if a property represents or is part of the entities unique identity (often primary key)
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsIdentity(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a property is computed after insert or update
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsComputed(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a property is generated on the server during insert
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsGenerated(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a property is mapped as a relationship
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsRelationship(MappingEntity entity, MemberInfo member)
        {
            return this.IsAssociationRelationship(entity, member)
                || this.IsNestedRelationship(entity, member);
        }

        /// <summary>
        /// The type of the entity on the other side of the relationship
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual MappingEntity GetRelatedEntity(MappingEntity entity, MemberInfo member)
        {
            Type relatedType = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));
            return this.GetEntity(relatedType);
        }

        /// <summary>
        /// Determines if the property is an assocation relationship.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsAssociationRelationship(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Get the members for the key properities to be joined in an association relationship
        /// </summary>
        /// <param name="association"></param>
        /// <param name="declaredTypeMembers"></param>
        /// <param name="associatedMembers"></param>
        public virtual void GetAssociationKeys(MappingEntity entity, MemberInfo association, out List<MemberInfo> declaredTypeMembers, out List<MemberInfo> associatedMembers)
        {
            declaredTypeMembers = null;
            associatedMembers = null;
        }

        /// <summary>
        /// Determines if the property is a nested relationship
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsNestedRelationship(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a relationship property refers to a single optional entity (as opposed to a collection.)
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsSingletonRelationship(MappingEntity entity, MemberInfo member)
        {
            if (!IsRelationship(entity, member))
                return false;
            Type ieType = TypeHelper.FindIEnumerable(TypeHelper.GetMemberType(member));
            return ieType == null;
        }

        /// <summary>
        /// The name of the corresponding database table
        /// </summary>
        /// <param name="rowType"></param>
        /// <returns></returns>
        public virtual string GetTableName(MappingEntity entity)
        {
            return entity.Type.Name;
        }

        /// <summary>
        /// The name of the corresponding table column
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual string GetColumnName(MappingEntity entity, MemberInfo member)
        {
            return member.Name;
        }

        /// <summary>
        /// The query language specific type for the column
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual QueryType GetColumnType(MappingEntity entity, MemberInfo member)
        {
            return this.language.TypeSystem.GetColumnType(TypeHelper.GetMemberType(member));
        }

        /// <summary>
        /// A sequence of all the mapped members
        /// </summary>
        /// <param name="rowType"></param>
        /// <returns></returns>
        public virtual IEnumerable<MemberInfo> GetMappedMembers(MappingEntity entity)
        {
            HashSet<MemberInfo> members = new HashSet<MemberInfo>(entity.Type.GetFields().Cast<MemberInfo>().Where(m => this.IsMapped(entity, m)));
            members.UnionWith(entity.Type.GetProperties().Cast<MemberInfo>().Where(m => this.IsMapped(entity, m)));
            return members.OrderBy(m => m.Name);
        }

        /// <summary>
        /// Get a query expression that selects all entities from a table
        /// </summary>
        /// <param name="rowType"></param>
        /// <returns></returns>
        public virtual ProjectionExpression GetTableQuery(MappingEntity entity)
        {
            var tableAlias = new TableAlias();
            var selectAlias = new TableAlias();
            var table = new TableExpression(tableAlias, entity, this.GetTableName(entity));

            Expression projector = this.GetTypeProjection(table, entity);
            var pc = ColumnProjector.ProjectColumns(this.Language.CanBeColumn, projector, null, selectAlias, tableAlias);

            return new ProjectionExpression(
                new SelectExpression(selectAlias, pc.Columns, table, null),
                pc.Projector
                );
        }

        /// <summary>
        /// Gets an expression that constructs an entity instance relative to a root.
        /// The root is most often a TableExpression, but may be any other experssion such as
        /// a ConstantExpression.
        /// </summary>
        /// <param name="root"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public virtual Expression GetTypeProjection(Expression root, MappingEntity entity)
        {
            // must be some complex type constructed from multiple columns
            List<MemberBinding> bindings = new List<MemberBinding>();
            foreach (MemberInfo mi in this.GetMappedMembers(entity))
            {
                if (!this.IsRelationship(entity, mi))
                {
                    Expression me = this.GetMemberExpression(root, entity, mi);
                    if (me != null)
                    {
                        bindings.Add(Expression.Bind(mi, me));
                    }
                }
            }
            return new EntityExpression(entity, Expression.MemberInit(Expression.New(entity.Type), bindings));
        }

        /// <summary>
        /// Get an expression for a mapped property relative to a root expression. 
        /// The root is either a TableExpression or an expression defining an entity instance.
        /// </summary>
        /// <param name="root"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual Expression GetMemberExpression(Expression root, MappingEntity entity, MemberInfo member)
        {
            if (this.IsAssociationRelationship(entity, member))
            {
                MappingEntity relatedEntity = this.GetRelatedEntity(entity, member);
                ProjectionExpression projection = this.GetTableQuery(relatedEntity);

                // make where clause for joining back to 'root'
                List<MemberInfo> declaredTypeMembers;
                List<MemberInfo> associatedMembers;
                this.GetAssociationKeys(entity, member, out declaredTypeMembers, out associatedMembers);

                Expression where = null;
                for (int i = 0, n = associatedMembers.Count; i < n; i++)
                {
                    Expression equal = Expression.Equal(
                        this.GetMemberExpression(projection.Projector, relatedEntity, associatedMembers[i]),
                        this.GetMemberExpression(root, entity, declaredTypeMembers[i])
                        );
                    where = (where != null) ? Expression.And(where, equal) : equal;
                }

                TableAlias newAlias = new TableAlias();
                var pc = ColumnProjector.ProjectColumns(this.Language.CanBeColumn, projection.Projector, null, newAlias, projection.Select.Alias);

                LambdaExpression aggregator = this.GetAggregator(TypeHelper.GetMemberType(member), typeof(IEnumerable<>).MakeGenericType(pc.Projector.Type));
                return new ProjectionExpression(
                    new SelectExpression(newAlias, pc.Columns, projection.Select, where),
                    pc.Projector, aggregator
                    );
            }
            else
            {
                TableExpression table = root as TableExpression;
                if (table != null)
                {
                    if (this.IsColumn(entity, member))
                    {
                        return new ColumnExpression(TypeHelper.GetMemberType(member), this.GetColumnType(entity, member), table.Alias, this.GetColumnName(entity, member));
                    }
                    else if (this.IsNestedRelationship(entity, member))
                    {
                        MappingEntity subEntity = this.GetRelatedEntity(entity, member);
                        return this.GetTypeProjection(root, subEntity);
                    }
                }
                return QueryBinder.BindMember(root, member);
            }
        }

        public virtual Expression GetInsertExpression(MappingEntity entity, Expression instance, LambdaExpression selector)
        {
            var tableAlias = new TableAlias();
            var table = new TableExpression(tableAlias, entity, this.GetTableName(entity));

            var assignments =
                from m in this.GetMappedMembers(entity)
                where this.IsColumn(entity, m) && !this.IsGenerated(entity, m)
                select new ColumnAssignment(
                    (ColumnExpression)this.GetMemberExpression(table, entity, m),
                    Expression.MakeMemberAccess(instance, m)
                    );

            Expression selectorResult = null;
            if (selector != null)
            {
                Expression where = null;
                var generatedIds = this.GetMappedMembers(entity).Where(m => this.IsIdentity(entity, m) && this.IsGenerated(entity, m));
                if (generatedIds.Any())
                {
                    where = generatedIds.Select((m, i) => 
                        Expression.Equal(this.GetMemberExpression(table, entity, m), this.language.GetGeneratedIdExpression(m))
                        ).Aggregate((x, y) => Expression.And(x, y));
                }
                else
                {
                    where = this.GetIdentityCheck(table, entity, instance);
                }

                Expression typeProjector = this.GetTypeProjection(table, entity);
                Expression selection = DbExpressionReplacer.Replace(selector.Body, selector.Parameters[0], typeProjector);
                TableAlias newAlias = new TableAlias();
                var pc = ColumnProjector.ProjectColumns(this.Language.CanBeColumn, selection, null, newAlias, tableAlias);
                ProjectionExpression proj = new ProjectionExpression(
                    new SelectExpression(newAlias, pc.Columns, table, where),
                    pc.Projector,
                    this.GetAggregator(selector.Body.Type, typeof(IEnumerable<>).MakeGenericType(selector.Body.Type))
                    );

                selectorResult = proj;
            }

            return new InsertExpression(table, assignments, selectorResult);
        }

        private Expression GetIdentityCheck(Expression root, MappingEntity entity, Expression instance)
        {
            return this.GetMappedMembers(entity)
            .Where(m => this.IsIdentity(entity, m))
            .Select(m =>
                Expression.Equal(
                    this.GetMemberExpression(root, entity, m),
                    Expression.MakeMemberAccess(instance, m)
                    ))
            .Aggregate((x, y) => Expression.And(x, y));
        }

        public virtual Expression GetUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression selector)
        {
            var tableAlias = new TableAlias();
            var table = new TableExpression(tableAlias, entity, this.GetTableName(entity));

            var where = this.GetIdentityCheck(table, entity, instance);
            if (updateCheck != null)
            {
                Expression typeProjector = this.GetTypeProjection(table, entity);
                Expression pred = DbExpressionReplacer.Replace(updateCheck.Body, updateCheck.Parameters[0], typeProjector);
                where = Expression.And(where, pred);
            }

            var assignments =
                from m in this.GetMappedMembers(entity)
                where this.IsColumn(entity, m) && !this.IsIdentity(entity, m)
                select new ColumnAssignment(
                    (ColumnExpression)this.GetMemberExpression(table, entity, m),
                    Expression.MakeMemberAccess(instance, m)
                    );

            Expression result = null;
            if (selector != null)
            {
                Expression resultWhere = this.GetIdentityCheck(table, entity, instance);
                Expression typeProjector = this.GetTypeProjection(table, entity);
                Expression selection = DbExpressionReplacer.Replace(selector.Body, selector.Parameters[0], typeProjector);
                TableAlias newAlias = new TableAlias();
                var pc = ColumnProjector.ProjectColumns(this.Language.CanBeColumn, selection, null, newAlias, tableAlias);
                result = new ProjectionExpression(
                    new SelectExpression(newAlias, pc.Columns, table, resultWhere),
                    pc.Projector,
                    this.GetAggregator(selector.Body.Type, typeof(IEnumerable<>).MakeGenericType(selector.Body.Type))
                    );
            }

            return new UpdateExpression(table, where, assignments, result);
        }

        public virtual Expression GetInsertOrUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression resultSelector)
        {
            Expression check = null;
            if (updateCheck != null)
            {
                var tableAlias = new TableAlias();
                var table = new TableExpression(tableAlias, entity, this.GetTableName(entity));

                Expression checkWhere = this.GetIdentityCheck(table, entity, instance);
                check = new ExistsExpression(new SelectExpression(new TableAlias(), null, table, checkWhere));
            }

            InsertExpression insert = (InsertExpression)this.GetInsertExpression(entity, instance, resultSelector);            
            UpdateExpression update = (UpdateExpression)this.GetUpdateExpression(entity, instance, updateCheck, resultSelector);

            return new UpsertExpression(check, insert, update);
        }

        public virtual Expression GetDeleteExpression(MappingEntity entity, Expression instance, LambdaExpression deleteCheck)
        {
            TableExpression table = new TableExpression(new TableAlias(), entity, this.GetTableName(entity));
            Expression where = null;

            if (instance != null)
            {
                where = this.GetIdentityCheck(table, entity, instance);
            }

            if (deleteCheck != null)
            {
                Expression row = this.GetTypeProjection(table, entity);
                Expression pred = DbExpressionReplacer.Replace(deleteCheck.Body, deleteCheck.Parameters[0], row);
                where = (where != null) ? Expression.And(where, pred) : pred;
            }

            return new DeleteExpression(table, where);
        }

        /// <summary>
        /// Get a function that coerces an a sequence of one type into another type.
        /// This is primarily used for aggregators stored in ProjectionExpression's, which are used to represent the 
        /// final transformation of the entire result set of a query.
        /// </summary>
        /// <param name="expectedType"></param>
        /// <param name="projector"></param>
        /// <returns></returns>
        public virtual LambdaExpression GetAggregator(Type expectedType, Type actualType)
        {
            //Type actualType = typeof(IEnumerable<>).MakeGenericType(elementType);
            Type actualElementType = TypeHelper.GetElementType(actualType);
            if (!expectedType.IsAssignableFrom(actualType))
            {
                Type expectedElementType = TypeHelper.GetElementType(expectedType);
                ParameterExpression p = Expression.Parameter(actualType, "p");
                Expression body = null;
                if (expectedType.IsAssignableFrom(actualElementType))
                {
                    body = Expression.Call(typeof(Enumerable), "SingleOrDefault", new Type[] { actualElementType }, p);
                }
                else if (expectedType.IsGenericType && expectedType.GetGenericTypeDefinition() == typeof(IQueryable<>))
                {
                    body = Expression.Call(typeof(Queryable), "AsQueryable", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else if (expectedType.IsArray && expectedType.GetArrayRank() == 1)
                {
                    body = Expression.Call(typeof(Enumerable), "ToArray", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else if (expectedType.IsAssignableFrom(typeof(List<>).MakeGenericType(actualElementType)))
                {
                    // List<T> can be assigned to expectedType
                    body = Expression.Call(typeof(Enumerable), "ToList", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else
                {
                    // some other collection type that has a constructor that takes IEnumerable<T>
                    ConstructorInfo ci = expectedType.GetConstructor(new Type[] { actualType });
                    if (ci != null)
                    {
                        body = Expression.New(ci, p);
                    }
                }
                if (body != null)
                {
                    return Expression.Lambda(body, p);
                }
            }
            return null;
        }

        private Expression CoerceElement(Type expectedElementType, Expression expression)
        {
            Type elementType = TypeHelper.GetElementType(expression.Type);
            if (expectedElementType != elementType && (expectedElementType.IsAssignableFrom(elementType) || elementType.IsAssignableFrom(expectedElementType)))
            {
                return Expression.Call(typeof(Enumerable), "Cast", new Type[] { expectedElementType }, expression);
            }
            return expression;
        }

        /// <summary>
        /// Apply mapping translations to this expression
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual Expression Translate(Expression expression)
        {
            // convert references to LINQ operators into query specific nodes
            expression = QueryBinder.Bind(this, expression);

            // move aggregate computations so they occur in same select as group-by
            expression = AggregateRewriter.Rewrite(expression);

            // do reduction so duplicate association's are likely to be clumped together
            expression = UnusedColumnRemover.Remove(expression);
            expression = RedundantColumnRemover.Remove(expression);
            expression = RedundantSubqueryRemover.Remove(expression);
            expression = RedundantJoinRemover.Remove(expression);

            // convert references to association properties into correlated queries
            expression = RelationshipBinder.Bind(this, expression);

            // clean up after ourselves! (multiple references to same association property)
            expression = RedundantColumnRemover.Remove(expression);
            expression = RedundantJoinRemover.Remove(expression);

            return expression;
        }
    }
}
