// Copyright (c) 2010, 7Clouds. All rights reserved.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace Stratosphere.Block.S3
{
    public sealed class S3Service : S3Base
    {
        private readonly string _serviceId;
        private readonly string _serviceSecret;

        public S3Service(string serviceId, string serviceSecret)
        {
            _serviceId = serviceId;
            _serviceSecret = serviceSecret;
        }
        
        public WebRequest CreateRequest(string method)
        {
            return CreateRequest(method, string.Empty);
        }

        public WebRequest CreateRequest(string method, string containerName)
        {
            return CreateRequest(method, containerName, string.Empty);
        }

        public WebRequest CreateRequest(string method, string containerName, string blockName)
        {
            return CreateRequest(method, containerName, blockName, string.Empty, string.Empty);
        }

        public WebRequest CreateRequest(string method, string containerName, string blockName, string contentMD5, string contentType)
        {
            return CreateRequest(method, containerName, blockName, contentMD5, contentType, EmptyHeaders, null);
        }

        public WebRequest CreateRequest(string method, string containerName, string blockName, string contentMD5, string contentType, IEnumerable<KeyValuePair<string, string>> headers, Action<Stream> action)
        {
            Uri uri;

            if (string.IsNullOrEmpty(containerName))
            {
                uri = new Uri("http://s3.amazonaws.com");
            }
            else
            {
                uri = new Uri(string.Format("http://{0}.s3.amazonaws.com/{1}", containerName, blockName));
            }

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
            request.Method = method;

            headers = WithDate(headers);

            foreach (KeyValuePair<string, string> header in headers)
            {
                request.Headers[header.Key] = header.Value;
            }

            request.Headers["Authorization"] = SignRequest(containerName, blockName, contentMD5, contentType, method, headers);

            if (action != null)
            {
                using (Stream stream = request.GetRequestStream())
                {
                    action(stream);
                }
            }

            return request;
        }

        private static IEnumerable<KeyValuePair<string, string>> WithDate(IEnumerable<KeyValuePair<string, string>> headers)
        {
            yield return new KeyValuePair<string, string>("X-Amz-Date", DateTime.UtcNow.ToString("R"));

            foreach (KeyValuePair<string, string> header in headers)
            {
                yield return header;
            }
        }

        private string SignRequest(string containerName, string blockName, string contentMD5, string contentType, string method, IEnumerable<KeyValuePair<string, string>> headers)
        {
            StringBuilder builder = new StringBuilder();

            builder.Append(method);
            builder.Append("\n");
            builder.Append(contentMD5);
            builder.Append("\n");
            builder.Append(contentType);
            builder.Append("\n\n");

            foreach (KeyValuePair<string, string> header in headers.OrderBy(h => h.Key))
            {
                builder.Append(header.Key.ToLowerInvariant());
                builder.Append(":");
                builder.Append(header.Value);
                builder.Append("\n");
            }

            builder.Append("/");

            if (!string.IsNullOrEmpty(containerName))
            {
                builder.Append(containerName);
                builder.Append("/");
                builder.Append(blockName);
            }

            byte[] signatureBytes;

            using (HMACSHA1 algorithm = new HMACSHA1(Encoding.UTF8.GetBytes(_serviceSecret)))
            {
                signatureBytes = algorithm.ComputeHash(Encoding.UTF8.GetBytes(builder.ToString()));
            }

            builder = new StringBuilder("AWS ");
            builder.Append(_serviceId);
            builder.Append(':');
            builder.Append(Convert.ToBase64String(signatureBytes));

            return builder.ToString();
        }
    }
}