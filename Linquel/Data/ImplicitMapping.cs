// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQ.Data
{
    /// <summary>
    /// A simple query mapping that attempts to infer mapping from naming conventionss
    /// </summary>
    public class ImplicitMapping : QueryMapping
    {
        public ImplicitMapping(QueryLanguage language)
            : base(language)
        {
        }

        public override MappingEntity GetEntity(Type type)
        {
            string tableName = this.InferTableName(type);
            return this.GetEntity(type, tableName);
        }

        public override bool IsIdentity(MappingEntity entity, MemberInfo member)
        {
            // Customers has CustomerID, Orders has OrderID, etc
            if (this.IsColumn(entity, member)) 
            {
                string name = NameWithoutTrailingDigits(member.Name);
                return member.Name.EndsWith("ID") && member.DeclaringType.Name.StartsWith(member.Name.Substring(0, member.Name.Length - 2)); 
            }
            return false;
        }

        private string NameWithoutTrailingDigits(string name)
        {
            int n = name.Length - 1;
            while (n >= 0 && char.IsDigit(name[n]))
            {
                n--;
            }
            if (n < name.Length - 1)
            {
                return name.Substring(0, n);
            }
            return name;
        }

        public override bool IsAssociationRelationship(MappingEntity entity, MemberInfo member)
        {
            if (IsMapped(entity, member) && !IsColumn(entity, member))
            {
                Type otherType = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));
                return !this.Language.IsScalar(otherType);
            }
            return false;
        }

        public override void GetAssociationKeys(MappingEntity entity, MemberInfo member, out List<MemberInfo> members1, out List<MemberInfo> members2)
        {
            MappingEntity entity2 = GetRelatedEntity(entity, member);

            // find all members in common (same name)
            var map1 = this.GetMappedMembers(entity).Where(m => this.IsColumn(entity, m)).ToDictionary(m => m.Name);
            var map2 = this.GetMappedMembers(entity2).Where(m => this.IsColumn(entity2, m)).ToDictionary(m => m.Name);
            var commonNames = map1.Keys.Intersect(map2.Keys).OrderBy(k => k);
            members1 = new List<MemberInfo>();
            members2 = new List<MemberInfo>();
            foreach (string name in commonNames)
            {
                members1.Add(map1[name]);
                members2.Add(map2[name]);
            }
        }

        public override string GetTableName(MappingEntity entity)
        {
            string name = !string.IsNullOrEmpty(entity.TableID) ? entity.TableID : this.InferTableName(entity.Type);
            return this.Language.Quote(name);
        }

        private string InferTableName(Type rowType)
        {
            return SplitWords(Plural(rowType.Name));
        }

        public static string SplitWords(string name)
        {
            StringBuilder sb = null;
            bool lastIsLower = char.IsLower(name[0]);
            for (int i = 0, n = name.Length; i < n; i++)
            {
                bool thisIsLower = char.IsLower(name[i]);
                if (lastIsLower && !thisIsLower)
                {
                    if (sb == null)
                    {
                        sb = new StringBuilder();
                        sb.Append(name, 0, i);
                    }
                    sb.Append(" ");
                }
                if (sb != null)
                {
                    sb.Append(name[i]);
                }
                lastIsLower = thisIsLower;
            }
            if (sb != null)
            {
                return sb.ToString();
            }
            return name;
        }

        public static string Plural(string name)
        {
            if (name.EndsWith("x", StringComparison.InvariantCultureIgnoreCase) 
                || name.EndsWith("ch", StringComparison.InvariantCultureIgnoreCase)
                || name.EndsWith("ss", StringComparison.InvariantCultureIgnoreCase)) 
            {
                return name + "es";
            }
            else if (name.EndsWith("y", StringComparison.InvariantCultureIgnoreCase)) 
            {
                return name.Substring(0, name.Length - 1) + "ies";
            }
            else if (!name.EndsWith("s"))
            {
                return name + "s";
            }
            return name;
        }

        public static string Singular(string name)
        {
            if (name.EndsWith("es", StringComparison.InvariantCultureIgnoreCase))
            {
                string rest = name.Substring(0, name.Length - 2);
                if (rest.EndsWith("x", StringComparison.InvariantCultureIgnoreCase)
                    || name.EndsWith("ch", StringComparison.InvariantCultureIgnoreCase)
                    || name.EndsWith("ss", StringComparison.InvariantCultureIgnoreCase))
                {
                    return rest;
                }
            }
            if (name.EndsWith("ies", StringComparison.InvariantCultureIgnoreCase))
            {
                return name.Substring(0, name.Length - 3) + "y";
            }
            else if (name.EndsWith("s", StringComparison.InvariantCultureIgnoreCase)
                && !name.EndsWith("ss", StringComparison.InvariantCultureIgnoreCase))
            {
                return name.Substring(0, name.Length - 1);
            }
            return name;
        }
    }
}
