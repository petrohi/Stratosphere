// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Xml;
using System.Xml.Linq;

namespace Stratosphere.Block.S3
{
    public class S3Base
    {
        public static readonly XNamespace S3 = XNamespace.Get("http://s3.amazonaws.com/doc/2006-03-01/");

        public const string GetMethod = "GET";
        public const string PostMethod = "POST";
        public const string PutMethod = "PUT";
        public const string DeleteMethod = "DELETE";

        public static readonly IEnumerable<KeyValuePair<string, string>> EmptyHeaders = new KeyValuePair<string, string>[] { };

        public static XElement GetResponse(WebRequest request)
        {
            using (WebResponse response = request.GetResponse())
            {
                if (response.ContentLength > 0)
                {
                    using (XmlReader reader = XmlReader.Create(response.GetResponseStream()))
                    {
                        return XElement.Load(reader);
                    }
                }
            }

            return null;
        }

        public static void GetResponse(WebRequest request, Action<Stream> action)
        {
            using (WebResponse response = request.GetResponse())
            {
                action(response.GetResponseStream());
            }
        }
    }
}