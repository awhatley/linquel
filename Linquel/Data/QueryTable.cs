// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;

namespace IQToolkit.Data
{
    public interface IQueryableTable : IQueryable
    {
        string EntityID { get; }
        Type EntityType { get; }
    }

    public interface IQueryableTable<T> : IQueryable<T>, IQueryableTable
    {
    }

    public class QueryableTable<T> : Query<T>, IQueryableTable<T>
    {
        string id;
        Type entityType;

        public QueryableTable(IQueryProvider provider, string entityId)
            : this(provider, entityId, typeof(T))
        {
        }

        public QueryableTable(IQueryProvider provider, string entityId, Type entityType)
            : base(provider)
        {
            this.id = entityId;
            this.entityType = entityType;
        }

        public string EntityID
        {
            get { return this.id; }
        }

        public Type EntityType
        {
            get { return this.entityType; }
        }
    }

    public interface IUpdatableTable : IQueryableTable, IUpdatable
    {
    }

    public interface IUpdatableTable<T> : IQueryableTable<T>, IUpdatable<T>, IUpdatableTable
    {
    }

    public class UpdatableTable<T> : QueryableTable<T>, IUpdatableTable<T>
    {
        public UpdatableTable(IQueryProvider provider, string entityId)
            : base(provider, entityId)
        {
        }

        public UpdatableTable(IQueryProvider provider, string entityId, Type entityType)
            : base(provider, entityId, entityType)
        {
        }
    }

    public class TableDispenser
    {
        IQueryProvider provider;
        Dictionary<string, IQueryableTable> tables;

        public TableDispenser(IQueryProvider provider)
        {
            this.provider = provider;
            this.tables = new Dictionary<string, IQueryableTable>();
        }

        public IQueryProvider Provider
        {
            get { return this.provider; }
        }

        public QueryableTable<T> GetQueryableTable<T>(string entityId)
        {
            return this.GetQueryableTable<T>(entityId, typeof(T));
        }

        public QueryableTable<T> GetQueryableTable<T>(string entityId, Type entityType)
        {
            IQueryableTable table;
            if (!this.tables.TryGetValue(entityId, out table))
            {
                table = new QueryableTable<T>(this.provider, entityId, entityType);
                this.tables.Add(entityId, table);
            }
            return (QueryableTable<T>)table;
        }

        public UpdatableTable<T> GetUpdatableTable<T>(string entityId)
        {
            return this.GetUpdatableTable<T>(entityId, typeof(T));
        }

        public UpdatableTable<T> GetUpdatableTable<T>(string entityId, Type entityType)
        {
            IQueryableTable table;
            if (!this.tables.TryGetValue(entityId, out table))
            {
                table = new UpdatableTable<T>(this.provider, entityId, entityType);
                this.tables.Add(entityId, table);
            }
            return (UpdatableTable<T>)table;
        }
    }
}