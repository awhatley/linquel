// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Test
{
    using IQToolkit.Data;
    using IQToolkit.Data.SqlServerCe;
    using System.Data.SqlServerCe;

    public class SqlCeTests
    {
        public static void Run(bool showTestOutput)
        {
            string constr = SqlCeQueryProvider.GetConnectionString(@"c:\data\Northwind.sdf");
            QueryMapping mapping = new AttributeMapping(SqlCeLanguage.Default, typeof(Northwind));
            var provider = new SqlCeQueryProvider(new SqlCeConnection(constr), mapping, showTestOutput ? Console.Out : null);

            provider.Connection.Open();
            try
            {
                Northwind db = new Northwind(provider);
                NorthwindTranslationTestsCore.Run(db, true);
                NorthwindExecutionTests.Run(db);
                NorthwindCUDTests.Run(db);
            }
            finally
            {
                provider.Connection.Close();
            }
        }
    }
}