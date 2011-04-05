// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Test
{
    using IQToolkit;
    using IQToolkit.Data;

    class Program
    {
        static void Main(string[] args)
        {
            SqlTests.Run(false);
            SqlCeTests.Run(false);
            AccessTests.Run(false);
        }
    }
}
