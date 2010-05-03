// Copyright (c) 2010, 7Clouds. All rights reserved.

using System;
using System.Linq;
using System.Net;
using System.Xml;
using System.Xml.Linq;

namespace Stratosphere.Aws
{
    public class AmazonException : Exception
    {
        private readonly string _code;

        public static bool TryCreate(WebException webException, out AmazonException amazonException)
        {
            if (webException.Response != null &&
                webException.Response.ContentLength != 0)
            {
                using (XmlReader reader = XmlReader.Create(webException.Response.GetResponseStream()))
                {
                    XElement response = XElement.Load(reader);
                    XElement code = response.Descendants().Where(e => e.Name.LocalName == "Code").FirstOrDefault();
                    XElement message = response.Descendants().Where(e => e.Name.LocalName == "Message").FirstOrDefault();

                    if (code != null && message != null)
                    {
                        amazonException = new AmazonException(message.Value, code.Value, webException);
                        return true;
                    }
                }
            }

            amazonException = null;
            return false;
        }

        public string Code { get { return _code; } }

        private AmazonException(string message, string code, WebException inner)
            : base(message, inner)
        {
            _code = code;
        }
    }
}