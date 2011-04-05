using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Sample {
    public class Customers {
        public string CustomerID;
        public string ContactName;
        public string Phone;
        public string City;
        public string Country;
    }

    public class Orders {
        public int OrderID;
        public string CustomerID;
        public DateTime OrderDate;
    }

    public class Northwind {
        public Query<Customers> Customers;
        public Query<Orders> Orders;

        public Northwind(DbConnection connection) {
            QueryProvider provider = new DbQueryProvider(connection);
            this.Customers = new Query<Customers>(provider);
            this.Orders = new Query<Orders>(provider);
        }
    }

    class Program {
        static void Main(string[] args) {
            string constr = @"…";
            using (SqlConnection con = new SqlConnection(constr)) {
                con.Open();
                Northwind db = new Northwind(con);

                string city = "London";
                var query = db.Customers.Where(c => c.City == city);

                Console.WriteLine("Query:\n{0}\n", query);

                var list = query.ToList();
                foreach (var item in list) {
                    Console.WriteLine("Name: {0}", item.ContactName);
                }

                Console.ReadLine();
            }
        }
    }
}
