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
    using IQToolkit;
    using IQToolkit.Data;
    using IQToolkit.Data.SqlClient;

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
        public int? OrderID;
        public int ProductID;
        public Product Product;
    }

    public interface IEntity
    {
        int ID { get; }
    }

    public class Product : IEntity
    {
        public int ID;
        public string ProductName;
        public YesNo Discontinued;

        int IEntity.ID
        {
            get { return this.ID; }
        }
    }

    public enum YesNo
    {
        No,
        Yes
    }

    public class Employee
    {
        public int EmployeeID;
        public string LastName;
        public string FirstName;
        public string Title;
        public Address Address;
    }

    public class Address
    {
        public string Street { get; private set; }
        public string City { get; private set; }
        public string Region { get; private set; }
        public string PostalCode { get; private set; }

        public Address(string street, string city, string region, string postalCode)
        {
            this.Street = street;
            this.City = city;
            this.Region = region;
            this.PostalCode = postalCode;
        }
    }

    public class Northwind
    {
        private TableDispenser dispenser;

        public static QueryMapping StandardMapping = new AttributeMapping(TSqlLanguage.Default, typeof(Northwind));

        public Northwind(IQueryProvider provider)
        {
            this.dispenser = new TableDispenser(provider);
        }

        public IQueryProvider Provider
        {
            get { return this.dispenser.Provider; }
        }

        [Table]
        [Column(Member = "CustomerId", IsPrimaryKey = true)]
        [Column(Member = "ContactName")]
        [Column(Member = "CompanyName")]
        [Column(Member = "Phone")]
        [Column(Member = "City", DbType="NVARCHAR(20)")]
        [Column(Member = "Country")]
        [Association(Member = "Orders", KeyMembers = "CustomerID", RelatedEntityID = "Orders", RelatedKeyMembers = "CustomerID")]
        public IUpdatableTable<Customer> Customers
        {
            get { return this.dispenser.GetUpdatableTable<Customer>("Customers"); }
        }
        
        [Table]
        [Column(Member = "OrderID", IsPrimaryKey = true, IsGenerated = true)]
        [Column(Member = "CustomerID")]
        [Column(Member = "OrderDate")]
        [Association(Member = "Customer", KeyMembers = "CustomerID", RelatedEntityID = "Customers", RelatedKeyMembers = "CustomerID")]
        [Association(Member = "Details", KeyMembers = "OrderID", RelatedEntityID = "OrderDetails", RelatedKeyMembers = "OrderID")]
        public IUpdatableTable<Order> Orders
        {
            get { return this.dispenser.GetUpdatableTable<Order>("Orders"); }
        }

        [Table(Name = "Order Details")]
        [Column(Member = "OrderID", IsPrimaryKey = true)]
        [Column(Member = "ProductID", IsPrimaryKey = true)]
        [Association(Member = "Product", KeyMembers = "ProductID", RelatedEntityID = "Products", RelatedKeyMembers = "ProductID")]
        public IUpdatableTable<OrderDetail> OrderDetails
        {
            get { return this.dispenser.GetUpdatableTable<OrderDetail>("OrderDetails"); }
        }

        [Table]
        [Column(Member = "Id", Name="ProductId", IsPrimaryKey = true)]
        [Column(Member = "ProductName")]
        [Column(Member = "Discontinued")]
        public IUpdatableTable<Product> Products
        {
            get { return this.dispenser.GetUpdatableTable<Product>("Products"); }
        }

        [Table]
        [Column(Member = "EmployeeID", IsPrimaryKey = true)]
        [Column(Member = "LastName")]
        [Column(Member = "FirstName")]
        [Column(Member = "Title")]
        [Column(Member = "Address.Street", Name = "Address")]
        [Column(Member = "Address.City")]
        [Column(Member = "Address.Region")]
        [Column(Member = "Address.PostalCode")]
        public IUpdatable<Employee> Employees
        {
            get { return this.dispenser.GetUpdatableTable<Employee>("Employees"); }
        }
    }

    public class NorthwindNoAttributes
    {
        private TableDispenser dispenser;

        public NorthwindNoAttributes(IQueryProvider provider)
        {
            this.dispenser = new TableDispenser(provider);
        }

        public IQueryProvider Provider
        {
            get { return this.dispenser.Provider; }
        }

        public IUpdatableTable<Customer> Customers
        {
            get { return this.dispenser.GetUpdatableTable<Customer>("Customers"); }
        }

        public IUpdatableTable<Order> Orders
        {
            get { return this.dispenser.GetUpdatableTable<Order>("Orders"); }
        }

        public IUpdatableTable<OrderDetail> OrderDetails
        {
            get { return this.dispenser.GetUpdatableTable<OrderDetail>("OrderDetails"); }
        }
    }
}
