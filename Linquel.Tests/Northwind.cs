// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.IO;

namespace Test
{
    using IQ;
    using IQ.Data;

    public class Customer
    {
        public string CustomerID;
        public string ContactName;
        public string Phone;
        public string City;
        public string Country;
        public List<Order> Orders;
    }

    public class Order
    {
        public int OrderID;
        public string CustomerID;
        public DateTime OrderDate;
        public Customer Customer;
        public List<OrderDetail> Details;
    }

    public class OrderDetail
    {
        public int OrderID;
        public int ProductID;
        public Product Product;
    }

    public class Product
    {
        public int ProductID;
        public string ProductName;
    }

    public class Northwind
    {
        public IQueryable<Customer> Customers;
        public IQueryable<Order> Orders;
        public IQueryable<OrderDetail> OrderDetails;
        public IQueryable<Product> Products;

        private IQueryProvider provider;

        public static QueryPolicy StandardPolicy = new QueryPolicy(new ImplicitMapping(new TSqlLanguage()));

        public Northwind(DbConnection connection, TextWriter log)
            : this(connection, log, StandardPolicy)
        {
        }

        public Northwind(DbConnection connection, TextWriter log, QueryPolicy policy)
            : this (new DbQueryProvider(connection, policy, log))
        {
        }

        public Northwind(IQueryProvider provider) 
        {
            this.provider = provider;
            this.Customers = new Query<Customer>(this.provider);
            this.Orders = new Query<Order>(this.provider);
            this.OrderDetails = new Query<OrderDetail>(this.provider);
            this.Products = new Query<Product>(this.provider);
        }

        public IQueryProvider Provider
        {
            get { return this.provider; }
        }
    }
}
