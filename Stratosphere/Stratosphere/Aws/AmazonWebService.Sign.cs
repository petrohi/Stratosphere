// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Stratosphere.Aws
{
    internal abstract partial class AmazonWebService
    {
        private IEnumerable<KeyValuePair<string, string>> Sign(Uri baseUri, IEnumerable<KeyValuePair<string, string>> parameters)
        {
            StringBuilder builder = new StringBuilder("GET\n");

            builder.Append(baseUri.Host.ToLowerInvariant());
            builder.Append("\n");
            builder.Append(baseUri.AbsolutePath);
            builder.Append("\nAWSAccessKeyId=");
            builder.Append(_id);

            foreach (KeyValuePair<string, string> parameter in parameters.OrderBy((p) => p.Key))
            {
                builder.Append("&");
                builder.Append(parameter.Key);
                builder.Append("=");
                builder.Append(EscapeUriDataStringRfc3986(parameter.Value));

                yield return parameter;
            }

            byte[] signatureBytes;

            using (HMACSHA256 algorithm = new HMACSHA256(Encoding.UTF8.GetBytes(_secret)))
            {
                signatureBytes = algorithm.ComputeHash(Encoding.UTF8.GetBytes(builder.ToString()));
            }

            yield return new KeyValuePair<string, string>("AWSAccessKeyId", _id);
            yield return new KeyValuePair<string, string>("Signature", Convert.ToBase64String(signatureBytes));
        }

        private IEnumerable<KeyValuePair<string, string>> PreSign()
        {
            yield return new KeyValuePair<string, string>("Timestamp", BuildTimestamp());
            yield return new KeyValuePair<string, string>("SignatureVersion", SignatureVersion2);
            yield return new KeyValuePair<string, string>("SignatureMethod", SignatureMethodHmacSHA256);
            yield return new KeyValuePair<string, string>("Version", Version);
        }

        private static string BuildTimestamp()
        {
            DateTime timestamp = DateTime.UtcNow;
            StringBuilder builder = new StringBuilder();

            builder.AppendFormat("{0:D4}-", timestamp.Year);
            builder.AppendFormat("{0:D2}-", timestamp.Month);
            builder.AppendFormat("{0:D2}T", timestamp.Day);
            builder.AppendFormat("{0:D2}:", timestamp.Hour);
            builder.AppendFormat("{0:D2}:", timestamp.Minute);
            builder.AppendFormat("{0:D2}Z", timestamp.Second);

            return builder.ToString();
        }
    }
}