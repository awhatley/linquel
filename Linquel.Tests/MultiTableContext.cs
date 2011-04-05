// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.IO;

namespace Test
{
    using IQToolkit;
    using IQToolkit.Data;
    using IQToolkit.Data.SqlClient;

    public class MultiTableEntity
    {
        public int ID;
        public string Value1;
        public string Value2;
        public string Value3;
    }

    public class MultiTableContext
    {
        private TableDispenser dispenser;

        public static QueryMapping StandardMapping = new AttributeMapping(new TSqlLanguage(), typeof(MultiTableContext));

        public MultiTableContext(IQueryProvider provider)
        {
            this.dispenser = new TableDispenser(provider);
        }

        public IQueryProvider Provider
        {
            get { return this.dispenser.Provider; }
        }

        [Table(Name = "TestTable1", Alias = "TT1")]
        [ExtensionTable(Name = "TestTable2", Alias = "TT2", KeyColumns = "ID", RelatedAlias = "TT1", RelatedKeyColumns = "ID")]
        [ExtensionTable(Name = "TestTable3", Alias = "TT3", KeyColumns = "ID", RelatedAlias = "TT1", RelatedKeyColumns = "ID")]
        [Column(Member = "ID", Alias = "TT1", IsPrimaryKey = true, IsGenerated = true)]
        [Column(Member = "Value1", Alias = "TT1")]
        [Column(Member = "Value2", Alias = "TT2")]
        [Column(Member = "Value3", Alias = "TT3")]
        public IUpdatable<MultiTableEntity> MultiTableEntities
        {
            get { return this.dispenser.GetUpdatableTable<MultiTableEntity>("MultiTableEntities"); }
        }
    }
}
