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
    public sealed class PrettyConsole
    {
        private struct Column
        {
            public string Name;
            public int Width;
        }

        private readonly List<Column> _columns = new List<Column>();

        public PrettyConsole() { }

        public void AddColumn(string name, int width)
        {
            _columns.Add(new Column() { Name = name, Width = width });
        }

        public void WriteHeader()
        {
            int headerWidth = 0;

            foreach (Column column in _columns)
            {
                Console.Write(column.Name);
                Console.Write(new string(' ', column.Width - column.Name.Length));

                headerWidth += column.Width;
            }

            Console.WriteLine();
            Console.WriteLine(new string('-', headerWidth));
        }

        public void WriteLine(params object[] objs)
        {
            for (int i = 0; i < _columns.Count; i++)
            {
                if (i < objs.Length)
                {
                    string s = objs[i].ToString();

                    Console.Write(s);
                    Console.Write(new string(' ', _columns[i].Width - s.Length));
                }
            }

            Console.WriteLine();
        }
    }

    public abstract class CommandLineOptions
    {
        public string Get(string name)
        {
            string value;

            if (TryGet(name, out value))
            {
                return value;
            }

            return string.Empty;
        }

        public abstract bool TryGet(string name, out string value);
        public abstract bool Contains(string name);
    }

    public sealed class CommandLineParser
    {
        private struct Option
        {
            public int ArgumentCount;
            public string HelpText;
        }

        private sealed class ParsedCommandLineOptions : CommandLineOptions
        {
            private readonly Dictionary<string, string> _parsed = new Dictionary<string, string>();

            public ParsedCommandLineOptions(string commandLine, Dictionary<string, Option> options)
            {
                string[] segments = commandLine.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                Option option;
                string name = null;
                StringBuilder builder = null;
                int count = 0;

                for (int i = 0; i < segments.Length; i++)
                {
                    if (count > 0)
                    {
                        if (builder.Length != 0)
                        {
                            builder.Append(' ');
                        }

                        builder.Append(segments[i]);

                        count--;
                    }
                    else if (options.TryGetValue(segments[i], out option))
                    {
                        if (name != null && !_parsed.ContainsKey(name))
                        {
                            _parsed.Add(name, builder.ToString());

                            name = null;
                            builder = null;
                            count = 0;
                        }

                        if (option.ArgumentCount != 0)
                        {
                            name = segments[i];
                            builder = new StringBuilder();
                            count = option.ArgumentCount;
                        }
                        else
                        {
                            _parsed.Add(segments[i], string.Empty);
                        }
                    }
                }

                if (name != null && !_parsed.ContainsKey(name))
                {
                    _parsed.Add(name, builder.ToString());
                }
            }

            public override bool TryGet(string name, out string value)
            {
                return _parsed.TryGetValue(name, out value);
            }

            public override bool Contains(string name)
            {
                return _parsed.ContainsKey(name);
            }
        }

        private readonly Dictionary<string, Option> _options = new Dictionary<string, Option>();

        public CommandLineParser() { }

        public void AddOption(string name, int argumentCount, string helpText)
        {
            _options.Add(name, new Option() { ArgumentCount = argumentCount, HelpText = helpText });
        }

        public CommandLineOptions Parse(string commandLine)
        {
            return new ParsedCommandLineOptions(commandLine, _options);
        }

        public void WriteOptions()
        {
            Console.WriteLine("Options:");
            Console.WriteLine();

            foreach (string name in _options.Keys)
            {
                Console.WriteLine(name);
            }
        }

        public void WriteOptionHelp(string name)
        {
            Option option;

            if (_options.TryGetValue(name, out option))
            {
                Console.WriteLine(option.HelpText);
            }
            else
            {
                Console.WriteLine("Unknown option");
            }
        }
    }

    class Program
    {
        private const string ListBucketsKey = "--list-buckets";
        private const string ListObjectsKey = "--list-objects";
        private const string ListDomainsKey = "--list-domains";
        private const string ListItemsKey = "--list-items";
        private const string ListQueuesKey = "--list-queues";
        private const string PutObjectKey = "--put-object";
        private const string GetObjectKey = "--get-object";
        private const string SaveItemsKey = "--save-items";
        private const string HelpKey = "--help";
        private const string FileNameKey = "--file-name";

        static Program()
        {
            __parser = new CommandLineParser();

            __parser.AddOption(ListBucketsKey, 0, "'AwsSh " + ListBucketsKey + "'\r\nList S3 buckets");
            __parser.AddOption(ListObjectsKey, 1, "'AwsSh " + ListObjectsKey + " <bucket URL>'\r\nList S3 objects");
            __parser.AddOption(ListDomainsKey, 0, "'AwsSh " + ListDomainsKey + "'\r\nList SimpleDB domains");
            __parser.AddOption(ListItemsKey, 1, "'AwsSh " + ListItemsKey + " <domain name>'\r\nList SimpleDB domain items");
            __parser.AddOption(ListQueuesKey, 0, "'AwsSh " + ListQueuesKey + "'\r\nList SQS queues");
            __parser.AddOption(PutObjectKey, 1, "'AwsSh " + PutObjectKey + " <object URL> [" + FileNameKey + " <file name>]'\r\nPuts S3 object\r\nIf " + FileNameKey + " not specified then file name must match object name");
            __parser.AddOption(GetObjectKey, 1, "'AwsSh " + GetObjectKey + " <object URL> [" + FileNameKey + " <file name>]'\r\nGets S3 object\r\nIf " + FileNameKey + " not specified file name will match object name");
            __parser.AddOption(SaveItemsKey, 1, "'AwsSh " + SaveItemsKey + " <domain name> " + FileNameKey + " <file name>'\r\nSaves SimpleDB domain items to XML file");
            __parser.AddOption(HelpKey, 1, "'AwsSh " + HelpKey + " [option]'\r\nPrint help");
            __parser.AddOption(FileNameKey, 1, "'AwsSh " + ListBucketsKey + " <file name>'\r\nSpecifies file name for corresponding option");
        }

        private static readonly CommandLineParser __parser;

        private static string ServiceId { get { return Environment.GetEnvironmentVariable("AWS_SERVICE_ID", EnvironmentVariableTarget.Process); } }
        private static string ServiceSecret { get { return Environment.GetEnvironmentVariable("AWS_SERVICE_SECRET", EnvironmentVariableTarget.Process); } }

        static void Main(string[] args)
        {
            CommandLineOptions options = __parser.Parse(Environment.CommandLine);

            string containerName;
            string domainName;
            string objectPath;
            string fileName;
            string helpOptionName;

            try
            {
                if (options.TryGet(HelpKey, out helpOptionName))
                {
                    WriteHelp(helpOptionName);
                }
                else if (options.Contains(ListBucketsKey))
                {
                    ListBuckets();
                }
                else if (options.Contains(ListDomainsKey))
                {
                    ListDomains();
                }
                else if (options.Contains(ListQueuesKey))
                {
                    ListQueues();
                }
                else if (options.TryGet(ListObjectsKey, out containerName))
                {
                    ListObjects(containerName);
                }
                else if (options.TryGet(ListItemsKey, out domainName))
                {
                    ListItems(domainName);
                }
                else if (options.TryGet(GetObjectKey, out objectPath))
                {
                    GetObject(objectPath, options.Get(FileNameKey));
                }
                else if (options.TryGet(PutObjectKey, out objectPath))
                {
                    PutObject(objectPath, options.Get(FileNameKey));
                }
                else if (
                    options.TryGet(SaveItemsKey, out domainName) &&
                    options.TryGet(FileNameKey, out fileName))
                {
                    SaveItems(domainName, fileName);
                }
                else
                {
                    WriteShortHelp();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static void WriteHelp(string optionName)
        {
            Console.WriteLine("Set AWS credentials in environment variables: AWS_SERVICE_ID and AWS_SERVICE_SECRET");
            Console.WriteLine();

            if (string.IsNullOrEmpty(optionName))
            {
                __parser.WriteOptions();

                Console.WriteLine();
                Console.WriteLine("Type 'AwsSh --help <option>' for help");
            }
            else
            {
                __parser.WriteOptionHelp(optionName);
            }
        }

        private static void WriteShortHelp()
        {
            Console.WriteLine("Type 'AwsSh --help' for help");
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
            PrettyConsole console = new PrettyConsole();
            console.AddColumn("Name", 64);
            console.AddColumn("CreationDate", 64);

            foreach (IContainer container in S3Container.ListContainers(ServiceId, ServiceSecret))
            {
                console.WriteLine(container.Name, container.CreationDate);
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

        private static void PutObject(string path, string fileName)
        {
            string containerName;
            string blockName;

            if (TryParsePath(path, out containerName, out blockName))
            {
                IContainer container = S3Container.GetContainer(ServiceId, ServiceSecret, containerName);
                IBlock block = container.GetBlock(blockName);

                if (string.IsNullOrEmpty(fileName))
                {
                    fileName = blockName;
                }

                using (FileStream input = new FileStream(fileName, FileMode.Open))
                {
                    Console.WriteLine(Path.GetFullPath(fileName));

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

        private static void GetObject(string path, string fileName)
        {
            string containerName;
            string blockName;

            if (TryParsePath(path, out containerName, out blockName))
            {
                IContainer container = S3Container.GetContainer(ServiceId, ServiceSecret, containerName);
                IBlock block = container.GetBlock(blockName);

                if (string.IsNullOrEmpty(fileName))
                {
                    fileName = blockName;
                }

                using (FileStream output = new FileStream(fileName, FileMode.Create))
                {
                    Console.WriteLine(Path.GetFullPath(fileName));
                    
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
