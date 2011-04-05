// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
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
    using IQToolkit;
    using IQToolkit.Data;

    public class NorthwindPerfTests : NorthwindTestHarness
    {
        public static void Run(Northwind db)
        {
            new NorthwindPerfTests().RunTests(db, null, null, true);
        }

        public static void Run(Northwind db, string testName)
        {
            new NorthwindPerfTests().RunTest(db, null, true, testName);
        }

        public void TestCompiledQuery()
        {
            int n = 50;
            int iterations = 100;

            var query = QueryCompiler.Compile((Northwind nw, int i) => nw.OrderDetails.Where(d => d.OrderID > i).Take(n));
            var result = query(db, 0).ToList();

            var compiledTime = RunTimedTest(iterations, i =>
            {
                var list = query(db, i).ToList();
            });

            var adoTime = RunTimedTest(iterations, i =>
            {
                var cmd = provider.Connection.CreateCommand();
                cmd.CommandText = "SELECT TOP (@p0) OrderID, ProductID FROM [Order Details] WHERE OrderID > @p1";
                var p = cmd.CreateParameter();
                p.ParameterName = "p0";
                p.Value = n;
                cmd.Parameters.Add(p);
                var p2 = cmd.CreateParameter();
                p2.ParameterName = "p1";
                p2.Value = i;
                cmd.Parameters.Add(p2);
                var reader = cmd.ExecuteReader();
                var list = new List<object>();
                while (reader.Read())
                {
                    var orderId = reader.IsDBNull(0) ? default(int) : reader.GetInt32(0);
                    var productId = reader.IsDBNull(1) ? default(int) : reader.GetInt32(1);
                    list.Add(new OrderDetail { OrderID = orderId, ProductID = productId });
                }
                reader.Close();
            });

            Console.WriteLine("Direct ADO : {0}", adoTime);
            Console.WriteLine("Compiled IQ: {0}  {1:#.##}x vs ADO", compiledTime, compiledTime/adoTime);
        }

        public void TestQueryCache()
        {
            int n = 50;
            int iterations = 1000;

            var notCached = RunTimedTest(iterations, i =>
            {
                var results = db.OrderDetails.Where(d => d.OrderID > i).Take(n).ToList();
                System.Diagnostics.Debug.Assert(results.Count == n);
            });

            var cache = new QueryCache(10);
            var check = RunTimedTest(iterations, i =>
            {
                var query = db.OrderDetails.Where(d => d.OrderID > i).Take(n);
                var isCached = cache.Contains(query);
            });

            var cached = RunTimedTest(iterations, i =>
            {
                var query = db.OrderDetails.Where(d => d.OrderID > i).Take(n);
                var results = cache.Execute(query).ToList();
                System.Diagnostics.Debug.Assert(results.Count == n);
            });
            System.Diagnostics.Debug.Assert(cache.Count == 1);

            var cq = QueryCompiler.Compile((Northwind nw, int i, int rows) => nw.OrderDetails.Where(d => d.OrderID > i).Take(rows));
            var compiled = RunTimedTest(iterations, i =>
            {
                var results = cq(db, i, n).ToList();
                System.Diagnostics.Debug.Assert(results.Count == n);
            });

            Console.WriteLine("compiled   : {0} sec", compiled);
            Console.WriteLine("check cache: {0}", check);
            Console.WriteLine("cached     : {0}  {1:#.##}x vs compiled", cached, cached / compiled);
            Console.WriteLine("not cached : {0}  {1:#.##}x vs compiled", notCached, notCached / compiled);
        }
    }
}