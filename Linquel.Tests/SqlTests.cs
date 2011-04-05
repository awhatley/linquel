// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Test
{
    using IQToolkit.Data;
    using IQToolkit.Data.SqlClient;

    public class SqlTests
    {
        public static void Run(bool showTestOutput)
        {
            string constr = SqlQueryProvider.GetExpressConnectionString(@"c:\data\Northwind.mdf");
            QueryMapping mapping = new AttributeMapping(TSqlLanguage.Default, typeof(Northwind));
            var provider = new SqlQueryProvider(new SqlConnection(constr), mapping, showTestOutput ? Console.Out : null);

            provider.Connection.Open();
            try
            {
                Northwind db = new Northwind(provider);

                NorthwindTranslationTests.Run(db, true);
                NorthwindExecutionTests.Run(db);
                NorthwindCUDTests.Run(db);
                
                MultiTableContext mdb = new MultiTableContext(provider.Create(MultiTableContext.StandardMapping));
                MultiTableTests.Run(mdb);
            }
            finally
            {
                provider.Connection.Close();
            }
        }
    }
}