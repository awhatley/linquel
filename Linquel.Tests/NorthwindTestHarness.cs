﻿// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Test
{
    using IQ.Data;

    public class NorthwindTestHarness : TestHarness
    {
        protected Northwind db;

        protected void RunTests(Northwind db, string baselineFile, string newBaselineFile, bool executeQueries)
        {
            this.db = db;
            DbQueryProvider provider = (DbQueryProvider)((IQueryable)db.Customers).Provider;
            base.RunTests(provider, baselineFile, newBaselineFile, executeQueries);
        }
    }
}