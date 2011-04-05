// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Test 
{
    using IQ;
    using IQ.Data;

    public class NorthwindExecutionTests : NorthwindTestHarness
    {
        public static void Run(Northwind db)
        {
            new NorthwindExecutionTests().RunTests(db, null, null, true);
        }

        public void TestCompiledQuery()
        {
            var fn = QueryCompiler.Compile((string id) => db.Customers.Where(c => c.CustomerID == id));
            var items = fn("ALKFI").ToList();
        }

        public void TestCompiledQuerySingleton()
        {
            var fn = QueryCompiler.Compile((string id) => db.Customers.SingleOrDefault(c => c.CustomerID == id));
            Customer cust = fn("ALKFI");
        }

        public void TestCompiledQueryCount()
        {
            var fn = QueryCompiler.Compile((string id) => db.Customers.Count(c => c.CustomerID == id));
            int n = fn("ALKFI");
        }

        public void TestCompiledQueryIsolated()
        {
            var fn = QueryCompiler.Compile((Northwind n, string id) => n.Customers.Where(c => c.CustomerID == id));
            var items = fn(this.db, "ALFKI").ToList();
        }

        public void TestCompiledQueryIsolatedWithHeirarchy()
        {
            var fn = QueryCompiler.Compile((Northwind n, string id) => n.Customers.Where(c => c.CustomerID == id).Select(c => n.Orders.Where(o => o.CustomerID == c.CustomerID)));
            var items = fn(this.db, "ALFKI").ToList();
        }
    }
}