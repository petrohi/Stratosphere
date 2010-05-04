// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using Stratosphere.Aws;

namespace Stratosphere.Table.Sdb
{
    internal sealed partial class SdbService : AmazonWebService
    {
        [ThreadStatic]
        private static double __currentBoxUsage;

        public SdbService(string id, string secret)
            : base(id, secret) { }

        public static void ResetBoxUsage() { __currentBoxUsage = 0; }
        
        public static double CurrentBoxUsage { get { return __currentBoxUsage; } }

        protected override string Version { get { return Version20090415; } }

        public XElement Execute(IEnumerable<KeyValuePair<string, string>> parameters)
        {
            XElement response = Execute(parameters, new Uri("http://sdb.amazonaws.com/"));
            __currentBoxUsage += ParseBoxUsage(response);

            return response;
        }

        private static double ParseBoxUsage(XElement responseElement)
        {
            return double.Parse(responseElement.Descendants(Sdb + "BoxUsage").First().Value);
        }

        private const string Version20090415 = "2009-04-15";

        private static readonly XNamespace Sdb = XNamespace.Get("http://sdb.amazonaws.com/doc/2009-04-15/");
    }
}