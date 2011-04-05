// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data.Common
{
    public static class FieldReader
    {
        static Dictionary<Type, MethodInfo> _readerMethods;
        static FieldReader()
        {
            _readerMethods = typeof(FieldReader).GetMethods(BindingFlags.Public|BindingFlags.Static).Where(m => m.Name.StartsWith("Get")).ToDictionary(m => m.ReturnType);
        }

        public static MethodInfo GetReaderMethod(Type type)
        {
            MethodInfo mi;
            _readerMethods.TryGetValue(type, out mi);
            return mi;
        }

        public static object GetValue(DbEntityProviderBase provider, DbDataReader reader, int ordinal, Type type)
        {
            if (reader.IsDBNull(ordinal))
            {
                return TypeHelper.GetDefault(type);
            }
            object value = reader.GetValue(ordinal);
            if (value.GetType() != type)
            {
                return provider.Convert(reader.GetValue(ordinal), type);
            }
            return value;
        }

        public static Byte GetByte(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Byte);
            }
            try
            {
                return reader.GetByte(ordinal);
            }
            catch
            {
                return (Byte)provider.Convert(reader.GetValue(ordinal), typeof(Byte));
            }
        }

        public static Byte? GetNullableByte(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Byte?);
            }
            try
            {
                return reader.GetByte(ordinal);
            }
            catch
            {
                return (Byte?)provider.Convert(reader.GetValue(ordinal), typeof(Byte));
            }
        }

        public static Char GetChar(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Char);
            }
            try
            {
                return reader.GetChar(ordinal);
            }
            catch
            {
                return (Char)provider.Convert(reader.GetValue(ordinal), typeof(Char));
            }
        }

        public static Char? GetNullableChar(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Char?);
            }
            try
            {
                return reader.GetChar(ordinal);
            }
            catch
            {
                return (Char?)provider.Convert(reader.GetValue(ordinal), typeof(Char));
            }
        }

        public static DateTime GetDateTime(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(DateTime);
            }
            try
            {
                return reader.GetDateTime(ordinal);
            }
            catch
            {
                return (DateTime)provider.Convert(reader.GetValue(ordinal), typeof(DateTime));
            }
        }

        public static DateTime? GetNullableDateTime(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(DateTime?);
            }
            try
            {
                return reader.GetDateTime(ordinal);
            }
            catch
            {
                return (DateTime?)provider.Convert(reader.GetValue(ordinal), typeof(DateTime));
            }
        }

        public static Decimal GetDecimal(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Decimal);
            }
            try
            {
                return reader.GetDecimal(ordinal);
            }
            catch
            {
                return (Decimal)provider.Convert(reader.GetValue(ordinal), typeof(Decimal));
            }
        }

        public static Decimal? GetNullableDecimal(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Decimal?);
            }
            try
            {
                return reader.GetDecimal(ordinal);
            }
            catch
            {
                return (Decimal?)provider.Convert(reader.GetValue(ordinal), typeof(Decimal));
            }
        }

        public static Double GetDouble(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Double);
            }
            try
            {
                return reader.GetDouble(ordinal);
            }
            catch
            {
                return (Double)provider.Convert(reader.GetValue(ordinal), typeof(Double));
            }
        }

        public static Double? GetNullableDouble(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Double?);
            }
            try
            {
                return reader.GetDouble(ordinal);
            }
            catch
            {
                return (Double?)provider.Convert(reader.GetValue(ordinal), typeof(Double));
            }
        }

        public static Single GetSingle(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Single);
            }
            try
            {
                return reader.GetFloat(ordinal);
            }
            catch
            {
                return (Single)provider.Convert(reader.GetValue(ordinal), typeof(Single));
            }
        }

        public static Single? GetNullableSingle(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Single?);
            }
            try
            {
                return reader.GetFloat(ordinal);
            }
            catch
            {
                return (Single?)provider.Convert(reader.GetValue(ordinal), typeof(Single));
            }
        }

        public static Guid GetGuid(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Guid);
            }
            try
            {
                return reader.GetGuid(ordinal);
            }
            catch
            {
                return (Guid)provider.Convert(reader.GetValue(ordinal), typeof(Guid));
            }
        }

        public static Guid? GetNullableGuid(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Guid?);
            }
            try
            {
                return reader.GetGuid(ordinal);
            }
            catch
            {
                return (Guid?)provider.Convert(reader.GetValue(ordinal), typeof(Guid));
            }
        }

        public static Int16 GetInt16(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Int16);
            }
            try
            {
                return reader.GetInt16(ordinal);
            }
            catch
            {
                return (Int16)provider.Convert(reader.GetValue(ordinal), typeof(Int16));
            }
        }

        public static Int16? GetNullableInt16(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Int16?);
            }
            try
            {
                return reader.GetInt16(ordinal);
            }
            catch
            {
                return (Int16?)provider.Convert(reader.GetValue(ordinal), typeof(Int16));
            }
        }

        public static Int32 GetInt32(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Int32);
            }
            try
            {
                return reader.GetInt32(ordinal);
            }
            catch
            {
                return (Int32)provider.Convert(reader.GetValue(ordinal), typeof(Int32));
            }
        }

        public static Int32? GetNullableInt32(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Int32?);
            }
            try
            {
                return reader.GetInt32(ordinal);
            }
            catch
            {
                return (Int32?)provider.Convert(reader.GetValue(ordinal), typeof(Int32));
            }
        }

        public static Int64 GetInt64(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Int64);
            }
            try
            {
                return reader.GetInt64(ordinal);
            }
            catch
            {
                return (Int64)provider.Convert(reader.GetValue(ordinal), typeof(Int64));
            }
        }

        public static Int64? GetNullableInt64(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Int64?);
            }
            try
            {
                return reader.GetInt64(ordinal);
            }
            catch
            {
                return (Int64?)provider.Convert(reader.GetValue(ordinal), typeof(Int64));
            }
        }

        public static String GetString(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(String);
            }
            try
            {
                return reader.GetString(ordinal);
            }
            catch
            {
                return (String)provider.Convert(reader.GetValue(ordinal), typeof(String));
            }
        }

        public static Byte[] GetByteArray(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Byte[]);
            }
            var value = reader.GetValue(ordinal);
            try
            {
                return (Byte[])value;
            }
            catch
            {
                return (Byte[])provider.Convert(value, typeof(Byte[]));
            }
        }

        public static Char[] GetCharArray(DbEntityProviderBase provider, DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                return default(Char[]);
            }
            var value = reader.GetValue(ordinal);
            try
            {
                return (Char[])value;
            }
            catch
            {
                return (Char[])provider.Convert(value, typeof(Char[]));
            }
        }
    }
}