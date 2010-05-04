// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Xml.Linq;
using Stratosphere.Aws;

namespace Stratosphere.Queue.Sqs
{
    internal sealed partial class SqsService : AmazonWebService
    {
        public SqsService(string id, string secret)
            : base(id, secret) { }

        protected override string Version { get { return Version20090201; } }

        public XElement Execute(IEnumerable<KeyValuePair<string, string>> parameters)
        {
            return Execute(parameters, new Uri("http://queue.amazonaws.com/"));
        }

        private const string Version20090201 = "2009-02-01";
    }
}