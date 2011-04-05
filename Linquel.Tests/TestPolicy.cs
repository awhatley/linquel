// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Reflection;
using IQ;
using IQ.Data;

namespace Test
{
    class TestPolicy : QueryPolicy
    {
        HashSet<string> included;

        internal TestPolicy(params string[] includedRelationships)
            : base(Northwind.StandardPolicy.Mapping)
        {
            this.included = new HashSet<string>(includedRelationships);
        }

        public override bool IsIncluded(MemberInfo member)
        {
            return this.included.Contains(member.Name);
        }
    }
}
