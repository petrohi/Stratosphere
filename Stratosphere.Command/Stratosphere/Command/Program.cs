// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Stratosphere.Block;
using Stratosphere.Block.S3;
using Stratosphere.Queue;
using Stratosphere.Queue.Sqs;
using Stratosphere.Table;
using Stratosphere.Table.Sdb;
using System.Xml;

namespace Stratosphere.Command
{
    public sealed class CommandLineParser
    {
        private readonly Dictionary<string, string> _parsed = new Dictionary<string, string>();

        public CommandLineParser(IEnumerable<string> keys)
        {
            HashSet<string> keyHash = new HashSet<string>(keys);
            string[] segments = Environment.CommandLine.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            string key = null;
            StringBuilder builder = null;

            for (int i = 0; i < segments.Length; i++)
            {
                if (keyHash.Contains(segments[i]))
                {
                    if (key != null && !_parsed.ContainsKey(key))
                    {
                        _parsed.Add(key, builder.ToString());
                    }

                    key = segments[i];
                    builder = new StringBuilder();
                }
                else if (builder != null)
                {
                    if (builder.Length != 0)
                    {
                        builder.Append(' ');
                    }

                    builder.Append(segments[i]);
                }
            }

            if (key != null && !_parsed.ContainsKey(key))
            {
                _parsed.Add(key, builder.ToString());
            }
        }

        public string Get(string key)
        {
            string value;

            if (TryGet(key, out value))
            {
                return value;
            }

            return string.Empty;
        }

        public bool TryGet(string key, out string value)
        {
            return _parsed.TryGetValue(key, out value);
        }

        public bool Contains(string key)
        {
            return _parsed.ContainsKey(key);
        }
    }

    class Program
    {
        private const string ListBucketsKey = "list-buckets";
        private const string ListObjectsKey = "list-objects";
        private const string ListDomainsKey = "list-domains";
        private const string ListItemsKey = "list-items";
        private const string ListQueuesKey = "list-queues";
        private const string PutObjectKey = "put-object";
        private const string GetObjectKey = "get-object";
        private const string SaveItemsKey = "save-items";
        private const string FileNameKey = "--file-name";

        private static readonly CommandLineParser __parser = new CommandLineParser(
            new string[]
            {
                ListBucketsKey,
                ListObjectsKey,
                ListDomainsKey,
                ListItemsKey,
                ListQueuesKey,
                PutObjectKey,
                GetObjectKey,
                SaveItemsKey,
                FileNameKey
            });

        private static string ServiceId { get { return Environment.GetEnvironmentVariable("AWS_SERVICE_ID", EnvironmentVariableTarget.Process); } }
        private static string ServiceSecret { get { return Environment.GetEnvironmentVariable("AWS_SERVICE_SECRET", EnvironmentVariableTarget.Process); } }

        static void Main(string[] args)
        {
            string containerName;
            string domainName;
            string objectPath;
            string fileName;

            if (__parser.Contains(ListBucketsKey))
            {
                ListBuckets();
            }
            else if (__parser.Contains(ListDomainsKey))
            {
                ListDomains();
            }
            else if (__parser.Contains(ListQueuesKey))
            {
                ListQueues();
            }
            else if (__parser.TryGet(ListObjectsKey, out containerName))
            {
                ListObjects(containerName);
            }
            else if (__parser.TryGet(ListItemsKey, out domainName))
            {
                ListItems(domainName);
            }
            else if (__parser.TryGet(GetObjectKey, out objectPath))
            {
                GetObject(objectPath);
            }
            else if (__parser.TryGet(PutObjectKey, out objectPath))
            {
                PutObject(objectPath);
            }
            else if (
                __parser.TryGet(SaveItemsKey, out domainName) &&
                __parser.TryGet(FileNameKey, out fileName))
            {
                SaveItems(domainName, fileName);
            }
        }

        private static void ListQueues()
        {
            foreach (SqsQueue queue in SqsQueue.ListQueues(ServiceId, ServiceSecret))
            {
                Console.WriteLine(queue.Name);
            }
        }

        private static void ListDomains()
        {
            foreach (SdbTable table in SdbTable.ListTables(ServiceId, ServiceSecret))
            {
                Console.WriteLine(table.DomainName);
            }
        }

        private static void ListObjects(string containerName)
        {
            IContainer container = S3Container.GetContainer(ServiceId, ServiceSecret, containerName);

            foreach (IBlock block in container.ListBlocks())
            {
                Console.WriteLine(block.Name);
            }
        }

        private static void ListBuckets()
        {
            foreach (IContainer container in S3Container.ListContainers(ServiceId, ServiceSecret))
            {
                Console.WriteLine(container.Name);
            }
        }

        private static void ListItems(string domainName)
        {
            SdbTable table = SdbTable.Create(ServiceId, ServiceSecret, domainName);

            using (IReader reader = table.Select(new string[] { }, null))
            {
                ReadingState state;

                while ((state = reader.Read()) != ReadingState.End)
                {
                    if (state == ReadingState.BeginItem)
                    {
                        WriteItem(reader);
                    }
                    else
                    {
                        WriteAttribute(reader);
                    }
                }
            }
        }

        private static void WriteItem(IReader reader)
        {
            Console.WriteLine("\'{0}\'", reader.ItemName);
            WriteAttribute(reader);
        }

        private static void WriteAttribute(IReader reader)
        {
            Console.WriteLine("\'{0}\'=\'{1}\'", reader.AttributeName, reader.AttributeValue);
        }

        private static void PutObject(string path)
        {
            string containerName;
            string blockName;

            if (TryParsePath(path, out containerName, out blockName))
            {
                IContainer container = S3Container.GetContainer(ServiceId, ServiceSecret, containerName);
                IBlock block = container.GetBlock(blockName);

                using (FileStream input = new FileStream(blockName, FileMode.Open))
                {
                    Console.WriteLine(Path.GetFullPath(blockName));

                    block.Write((output) =>
                    {
                        byte[] buffer = new byte[BufferSize];
                        int count = 0;

                        using (output)
                        {
                            do
                            {
                                count = input.Read(buffer, 0, buffer.Length);

                                if (count != 0)
                                {
                                    output.Write(buffer, 0, count);

                                    Console.Write(".");
                                }
                            }
                            while (count != 0);
                        }
                    });
                }

                Console.WriteLine();
            }
        }

        private static void GetObject(string path)
        {
            string containerName;
            string blockName;

            if (TryParsePath(path, out containerName, out blockName))
            {
                IContainer container = S3Container.GetContainer(ServiceId, ServiceSecret, containerName);
                IBlock block = container.GetBlock(blockName);

                using (FileStream output = new FileStream(blockName, FileMode.Create))
                {
                    Console.WriteLine(Path.GetFullPath(blockName));
                    
                    block.Read((input) =>
                    {
                        byte[] buffer = new byte[BufferSize];
                        int count = 0;

                        using (input)
                        {
                            do
                            {
                                count = input.Read(buffer, 0, buffer.Length);

                                if (count != 0)
                                {
                                    output.Write(buffer, 0, count);

                                    Console.Write(".");
                                }
                            }
                            while (count != 0);
                        }
                    });
                }

                Console.WriteLine();
            }
        }

        private static void SaveItems(string domainName, string fileName)
        {
            using (FileStream stream = new FileStream(fileName, FileMode.Create))
            {
                using (XmlWriter writer = XmlWriter.Create(stream))
                {
                    writer.WriteStartDocument();
                    writer.WriteStartElement("Domain");

                    long elementCount;
                    long sizeBytes;

                    SdbTable table = SdbTable.Create(ServiceId, ServiceSecret, domainName);
                    table.GetInfo(out elementCount, out sizeBytes);
                    int count = 0;

                    using (IReader reader = table.Select(new string[] { }, null))
                    {
                        ReadingState state;
                        bool itemBegan = false;

                        while ((state = reader.Read()) != ReadingState.End)
                        {
                            if (state == ReadingState.BeginItem)
                            {
                                if (itemBegan)
                                {
                                    writer.WriteEndElement();
                                }

                                BeginSaveItem(writer, reader);

                                if (!itemBegan)
                                {
                                    itemBegan = true;
                                }

                                WriteProgress("Saved {0:F2} %", ((float)count / (float)elementCount) * 100.0);
                                count++;
                            }
                            else
                            {
                                SaveAttribute(writer, reader);
                            }
                        }

                        if (itemBegan)
                        {
                            writer.WriteEndElement();
                        }

                        writer.WriteEndElement();
                        writer.WriteEndDocument();
                    }
                }

                Console.WriteLine("{0} written {1} bytes", Path.GetFullPath(fileName), new FileInfo(fileName).Length);
            }
        }

        private static void BeginSaveItem(XmlWriter writer, IReader reader)
        {
            writer.WriteStartElement("Element");
            writer.WriteAttributeString("Id", reader.ItemName);
            
            SaveAttribute(writer, reader);
        }

        private static void SaveAttribute(XmlWriter writer, IReader reader)
        {
            writer.WriteElementString(XmlConvert.EncodeLocalName(reader.AttributeName), reader.AttributeValue);
        }

        private static bool TryParsePath(string path, out string containerName, out string blockName)
        {
            string[] segments = path.Split(new char[] { '/' });

            if (segments.Length >= 2)
            {
                containerName = segments[0];
                blockName = segments[segments.Length - 1];

                return true;
            }

            containerName = string.Empty;
            blockName = string.Empty;

            return false;
        }

        private static void WriteProgress(string format, params object[] objs)
        {
            int _left;
            int _top;

            _left = Console.CursorLeft;
            _top = Console.CursorTop;

            Console.Write(format, objs);
            Console.SetCursorPosition(_left, _top);
        }

        private const int BufferSize = 64 * 1024;
    }
}
