// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Test
{
    using IQToolkit;
    using IQToolkit.Data;
    using IQToolkit.Data.Access;

    public class AccessTests
    {
        public static void Run(bool showTestOutput)
        {
            //string constr = AccessQueryProvider.GetAccess2007ConnectionString(@"c:\data\Nwind.accdb");
            string constr = AccessQueryProvider.GetAccess2000ConnectionString(@"c:\data\Nwind.mdb");
            QueryMapping mapping = new AttributeMapping(AccessLanguage.Default, typeof(Northwind));
            var provider = new AccessQueryProvider(new OleDbConnection(constr), mapping, showTestOutput ? Console.Out : null);

            provider.Connection.Open();
            try
            {
                Northwind db = new Northwind(provider);
                NorthwindTranslationTestsCore.Run(db, true);
                NorthwindCUDTests.Run(db);
            }
            finally
            {
                provider.Connection.Close();
            }
        }
    }
}