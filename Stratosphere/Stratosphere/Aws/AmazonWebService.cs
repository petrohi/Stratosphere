// Copyright (c) 2010, 7Clouds. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Stratosphere.Aws
{
    internal abstract partial class AmazonWebService
    {
        private readonly string _id;
        private readonly string _secret;

        public AmazonWebService(string id, string secret)
        {
            _id = id;
            _secret = secret;
        }

        public XElement Execute(IEnumerable<KeyValuePair<string, string>> parameters, Uri baseUri)
        {
            XElement responseElement;

            WebRequest request = HttpWebRequest.Create(BuildUri(baseUri, parameters));

            try
            {
                using (WebResponse response = request.GetResponse())
                {
                    using (XmlReader reader = XmlReader.Create(response.GetResponseStream()))
                    {
                        responseElement = XElement.Load(reader);
                    }
                }
            }
            catch (WebException webException)
            {
                AmazonException serviceException;

                if (AmazonException.TryCreate(webException, out serviceException))
                {
                    throw serviceException;
                }

                throw;
            }

            return responseElement;
        }
        
        protected abstract string Version { get; }
        
        private Uri BuildUri(Uri baseUri, IEnumerable<KeyValuePair<string, string>> parameters)
        {
            StringBuilder builder = new StringBuilder(baseUri.OriginalString);
            builder.Append("?");

            bool first = true;

            foreach (KeyValuePair<string, string> parameter in Sign(baseUri, PreSign().Union(parameters)))
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    builder.Append("&");
                }

                builder.Append(parameter.Key);
                builder.Append("=");
                builder.Append(EscapeUriDataStringRfc3986(parameter.Value));
            }

            return new Uri(builder.ToString());
        }

        private static string EscapeUriDataStringRfc3986(string value)
        {
            StringBuilder builder = new StringBuilder(Uri.EscapeDataString(value));

            for (int i = 0; i < UriRfc3986CharsToEscape.Length; i++)
            {
                builder.Replace(UriRfc3986CharsToEscape[i], Uri.HexEscape(UriRfc3986CharsToEscape[i][0]));
            }

            return builder.ToString();
        }

        private const string SignatureMethodHmacSHA256 = "HmacSHA256";
        private const string SignatureVersion2 = "2";

        private static readonly string[] UriRfc3986CharsToEscape = new[] { "!", "*", "'", "(", ")" };
    }
}