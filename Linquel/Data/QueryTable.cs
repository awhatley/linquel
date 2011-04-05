// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;

namespace IQ.Data
{
    public interface IQueryableTable : IQueryable
    {
        string TableID { get; }
    }

    public interface IQueryableTable<T> : IQueryable<T>, IQueryableTable
    {
    }

    public class QueryableTable<T> : Query<T>, IQueryableTable<T>
    {
        string id;

        public QueryableTable(IQueryProvider provider, string id)
            : base(provider)
        {
            this.id = id;
        }

        public QueryableTable(IQueryProvider provider)
            : this(provider, null)
        {
        }

        public string TableID
        {
            get { return this.id; }
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
        public UpdatableTable(IQueryProvider provider, string id)
            : base(provider, id)
        {
        }

        public UpdatableTable(IQueryProvider provider)
            : this(provider, null)
        {
        }
    }
}