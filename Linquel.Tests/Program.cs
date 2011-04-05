// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Test
{
    using IQ;
    using IQ.Data;

    class Program
    {
        static void Main(string[] args)
        {
            string constr = @"Data Source=.\SQLEXPRESS;AttachDbFilename=C:\data\Northwind.mdf;Integrated Security=True;Connect Timeout=30;User Instance=True;MultipleActiveResultSets=true";

            using (SqlConnection con = new SqlConnection(constr))
            {
                con.Open();

                Northwind db = new Northwind(con, Console.Out);
                NorthwindTranslationTests.Run(db, true);
                NorthwindExecutionTests.Run(db);

                Console.ReadLine();
            }
        }
    }
}
