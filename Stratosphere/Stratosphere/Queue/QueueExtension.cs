// Copyright (c) 2010 7Clouds

using System.Linq;
using System.Collections.Generic;
using System.Xml;
using System.IO;
using System.Text;

namespace Stratosphere.Queue
{
    public static class QueueExtension
    {
        public static IReceipt Receive(this IQueue queue)
        {
            return queue.Receive(1).FirstOrDefault();
        }

        public static void Send<T>(this IQueue queue, T message) where T : IEnumerable<KeyValuePair<string, string>>
        {
            StringBuilder builder = new StringBuilder();

            using (XmlWriter writer = XmlWriter.Create(new StringWriter(builder)))
            {
                writer.WriteMessage(message);
            }

            queue.Send(builder.ToString());
        }

        public static T GetMessage<T>(this IReceipt receipt) where T : IDictionary<string, string>, new()
        {
            using (XmlReader reader = XmlReader.Create(new StringReader(receipt.Message)))
            {
                return reader.ReadMessage<T>();
            }
        }

        private static void WriteMessage<T>(this XmlWriter writer, T message) where T : IEnumerable<KeyValuePair<string, string>>
        {
            writer.WriteStartElement("Message");

            foreach (var pair in message)
            {
                if (string.IsNullOrEmpty(pair.Value))
                {
                    writer.WriteStartElement(XmlConvert.EncodeLocalName(pair.Key));
                    writer.WriteEndElement();
                }
                else
                {
                    writer.WriteElementString(XmlConvert.EncodeLocalName(pair.Key), pair.Value);
                }
            }

            writer.WriteEndElement();
        }

        private static T ReadMessage<T>(this XmlReader reader) where T : IDictionary<string, string>, new()
        {
            T message = new T();
            string key = null;
            StringBuilder valueBuilder = null;

            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.Element && !reader.IsEmptyElement && key == null && reader.Depth == 1)
                {
                    key = XmlConvert.DecodeName(reader.LocalName);
                    valueBuilder = new StringBuilder();
                }
                else if (reader.NodeType == XmlNodeType.EndElement && key != null && reader.Depth == 1)
                {
                    if (valueBuilder != null && valueBuilder.Length != 0)
                    {
                        message[key] = valueBuilder.ToString();
                    }

                    key = null;
                    valueBuilder = null;
                }
                else if (reader.NodeType == XmlNodeType.Text && valueBuilder != null)
                {
                    valueBuilder.Append(reader.Value);
                }
            }

            return message;
        }
    }
}
