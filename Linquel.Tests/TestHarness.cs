// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Test
{
    using IQ.Data;

    public class TestHarness
    {
        protected class TestFailureException : Exception
        {
            internal TestFailureException(string message)
                : base(message)
            {
            }
        }

        private delegate void TestMethod();

        protected DbQueryProvider provider;
        XmlTextWriter baselineWriter;
        Dictionary<string, string> baselines;
        bool executeTests;
        protected MethodInfo currentMethod;

        protected TestHarness()
        {
        }

        protected void RunTests(DbQueryProvider provider, string baselineFile, string newBaselineFile, bool executeTests)
        {
            this.provider = provider;
            this.executeTests = executeTests;

            ReadBaselines(baselineFile);

            if (!string.IsNullOrEmpty(newBaselineFile))
            {
                baselineWriter = new XmlTextWriter(newBaselineFile, Encoding.UTF8);
                baselineWriter.Formatting = Formatting.Indented;
                baselineWriter.Indentation = 2;
                baselineWriter.WriteStartDocument();
                baselineWriter.WriteStartElement("baselines");
            }

            int iTest = 0;
            int iPassed = 0;
            ConsoleColor originalColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Running tests: {0}", this.GetType().Name);

            try
            {
                var tests = this.GetType().GetMethods().Where(m => m.Name.StartsWith("Test"));

                foreach (MethodInfo method in tests)
                {
                    iTest++;
                    currentMethod = method;
                    string testName = method.Name.Substring(4);
                    bool passed = false;
                    Console.WriteLine();
                    Setup();
                    string reason = "";
                    try
                    {
                        Console.ForegroundColor = ConsoleColor.Gray;
                        TestMethod test = (TestMethod)Delegate.CreateDelegate(typeof(TestMethod), this, method);
                        test();
                        passed = true;
                        iPassed++;
                    }
                    catch (TestFailureException tf)
                    {
                        if (tf.Message != null)
                            reason = tf.Message;
                    }
                    finally
                    {
                        Teardown();
                    }

                    Console.ForegroundColor = passed ? ConsoleColor.Green : ConsoleColor.Red;
                    Console.WriteLine("Test {0}: {1} - {2}", iTest, method.Name, passed ? "PASSED" : "FAILED");
                    if (!passed && !string.IsNullOrEmpty(reason))
                        Console.WriteLine("Reason: {0}", reason);
                }
            }
            finally
            {
                if (baselineWriter != null)
                {
                    baselineWriter.WriteEndElement();
                    baselineWriter.Close();
                }
            }

            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("SUMMARY: {0}", this.GetType().Name);
            Console.WriteLine("Total tests run: {0}", iTest);

            Console.ForegroundColor = ConsoleColor.Green;
            if (iPassed == iTest)
            {
                Console.WriteLine("ALL tests passed!");
            }
            else
            {
                Console.WriteLine("Total tests passed: {0}", iPassed);
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Total tests failed: {0}", iTest - iPassed);
            }
            Console.ForegroundColor = originalColor;
            Console.WriteLine();
        }

        protected virtual void Setup()
        {
        }

        protected virtual void Teardown()
        {
        }

        private void WriteBaseline(string key, string text)
        {
            if (baselineWriter != null)
            {
                baselineWriter.WriteStartElement("baseline");
                baselineWriter.WriteAttributeString("key", key);
                baselineWriter.WriteWhitespace("\r\n");
                baselineWriter.WriteString(text);
                baselineWriter.WriteEndElement();
            }
        }

        private void ReadBaselines(string filename)
        {
            if (!string.IsNullOrEmpty(filename) && File.Exists(filename))
            {
                XDocument doc = XDocument.Load(filename);
                this.baselines = doc.Root.Elements("baseline").ToDictionary(e => (string)e.Attribute("key"), e => e.Value);
            }
        }

        protected void TestQuery(IQueryable query)
        {
            TestQuery((DbQueryProvider)query.Provider, query.Expression, currentMethod.Name, false);
        }

        protected void TestQuery(IQueryable query, string baselineKey)
        {
            TestQuery((DbQueryProvider)query.Provider, query.Expression, baselineKey, false);
        }

        protected void TestQuery(Expression<Func<object>> query)
        {
            TestQuery(this.provider, query.Body, currentMethod.Name, false);
        }

        protected void TestQuery(Expression<Func<object>> query, string baselineKey)
        {
            TestQuery(this.provider, query.Body, baselineKey, false);
        }

        protected void TestQueryFails(IQueryable query)
        {
            TestQuery((DbQueryProvider)query.Provider, query.Expression, currentMethod.Name, true);
        }

        protected void TestQueryFails(Expression<Func<object>> query)
        {
            TestQuery(this.provider, query.Body, currentMethod.Name, true);
        }

        protected void TestQuery(DbQueryProvider pro, Expression query, string baselineKey, bool expectedToFail)
        {
            ConsoleColor originalColor = Console.ForegroundColor;
            try
            {
                if (query.NodeType == ExpressionType.Convert && query.Type == typeof(object))
                {
                    query = ((UnaryExpression)query).Operand; // remove box
                }

                if (pro.Log != null)
                {
                    Console.ForegroundColor = ConsoleColor.Gray;
                    DbExpressionWriter.Write(pro.Log, query);
                    pro.Log.WriteLine();
                    pro.Log.WriteLine("==>");
                }

                string queryText = null;
                try
                {
                    queryText = pro.GetQueryText(query);
                    WriteBaseline(baselineKey, queryText);
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine(string.Format("Query translation failed for {0}", baselineKey));
                    Console.ForegroundColor = ConsoleColor.Gray;
                    Console.WriteLine(query.ToString());
                    throw new TestFailureException(e.Message);
                }

                string baseline = null;
                if (this.baselines != null && this.baselines.TryGetValue(baselineKey, out baseline))
                {
                    string trimAct = TrimExtraWhiteSpace(queryText).Trim();
                    string trimBase = TrimExtraWhiteSpace(baseline).Trim();
                    if (trimAct != trimBase)
                    {
                        Console.ForegroundColor = ConsoleColor.Gray;
                        Console.WriteLine(queryText);
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("Query translation does not match baseline:");
                        WriteDifferences(trimAct, trimBase);
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("---- baseline ----");
                        WriteDifferences(trimBase, trimAct);
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        throw new TestFailureException("Translation differed from baseline.");
                    }
                }

                if (this.executeTests)
                {
                    Exception caught = null;
                    try
                    {
                        object result = pro.Execute(query);
                        IEnumerable seq = result as IEnumerable;
                        if (seq != null)
                        {
                            // iterate results
                            foreach (var item in seq)
                            {
                            }
                        }
                        else
                        {
                            IDisposable disposable = result as IDisposable;
                            if (disposable != null) 
                                disposable.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                        caught = e;
                        if (!expectedToFail)
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine("Query failed to execute:");
                            Console.ForegroundColor = ConsoleColor.Gray;
                            Console.WriteLine(queryText);
                            throw new TestFailureException(e.Message);
                        }
                    }
                    if (caught == null && expectedToFail)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("Query succeeded when expected to fail");
                        Console.ForegroundColor = ConsoleColor.Gray;
                        Console.WriteLine(queryText);
                        throw new TestFailureException(null);
                    }
                }

                if (baseline == null && this.baselines != null)
                {
                    throw new TestFailureException("No baseline");
                }
            }
            finally
            {
                Console.ForegroundColor = originalColor;
            }
        }

        private string TrimExtraWhiteSpace(string s)
        {
            StringBuilder sb = new StringBuilder();
            bool lastWasWhiteSpace = false;
            foreach (char c in s)
            {
                bool isWS = char.IsWhiteSpace(c);
                if (!isWS || !lastWasWhiteSpace)
                {
                    if (isWS)
                        sb.Append(' ');
                    else
                        sb.Append(c);
                    lastWasWhiteSpace = isWS;
                }
            }
            return sb.ToString();
        }

        private void WriteDifferences(string s1, string s2)
        {
            int start = 0;
            bool same = true;
            for (int i = 0, n = Math.Min(s1.Length, s2.Length); i < n; i++)
            {
                bool matches = s1[i] == s2[i];
                if (matches != same)
                {
                    if (i > start)
                    {
                        Console.ForegroundColor = same ? ConsoleColor.Gray : ConsoleColor.White;
                        Console.Write(s1.Substring(start, i - start));
                    }
                    start = i;
                    same = matches;
                }
            }
            if (start < s1.Length)
            {
                Console.ForegroundColor = same ? ConsoleColor.Gray : ConsoleColor.White;
                Console.Write(s1.Substring(start));
            }
            Console.WriteLine();
        }
    }
}