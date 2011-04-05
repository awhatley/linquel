// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
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
        public string CompanyName;
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
        public IUpdatableTable<Customer> Customers;
        public IUpdatableTable<Order> Orders;
        public IUpdatableTable<OrderDetail> OrderDetails;
        public IUpdatableTable<Product> Products;

        private IQueryProvider provider;

        public static QueryPolicy StandardPolicy = new QueryPolicy(new TestMapping(new TSqlLanguage()));

        public Northwind(DbConnection connection, TextWriter log)
            : this(connection, log, StandardPolicy)
        {
        }

        public Northwind(DbConnection connection, TextWriter log, QueryPolicy policy)
            : this(new DbQueryProvider(connection, policy, log))
        {
        }

        public Northwind(IQueryProvider provider)
        {
            this.provider = provider;
            this.Customers = new UpdatableTable<Customer>(this.provider, "Customers");
            this.Orders = new UpdatableTable<Order>(this.provider, "Orders");
            this.OrderDetails = new UpdatableTable<OrderDetail>(this.provider, "Order Details");
            this.Products = new UpdatableTable<Product>(this.provider, "Products");
        }

        public IQueryProvider Provider
        {
            get { return this.provider; }
        }
    }
}
