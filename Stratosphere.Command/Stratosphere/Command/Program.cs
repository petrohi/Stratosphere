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
            foreach (Column column in _columns)
            {
                Console.Write(column.Name);
                Console.Write(new string(' ', column.Width - column.Name.Length));
            }

            Console.WriteLine();

            foreach (Column column in _columns)
            {
                if (column.Width > 1)
                {
                    Console.Write(new string('-', column.Width - 1));
                }

                Console.Write(' ');
            }

            Console.WriteLine();
        }

        public void WriteLine(params object[] objs)
        {
            for (int i = 0; i < _columns.Count; i++)
            {
                if (i < objs.Length)
                {
                    string s = objs[i].ToString();

                    Console.Write(s);

                    if (s.Length < _columns[i].Width)
                    {
                        Console.Write(new string(' ', _columns[i].Width - s.Length));
                    }
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

    public class Program
    {
        private readonly CommandLineOptions _options;

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
        private const string ServiceIdKey = "--service-id";
        private const string ServiceSecretKey = "--service-secret";

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
            __parser.AddOption(ServiceIdKey, 1, "'AwsSh " + ServiceIdKey + " <service id>'\r\nSpecifies AWS service ID\r\nCan be also specified with AWS_SERVICE_ID environment variable");
            __parser.AddOption(ServiceSecretKey, 1, "'AwsSh " + ServiceSecretKey + " <service id>'\r\nSpecifies AWS service secret\r\nCan be also specified with AWS_SERVICE_SECRET environment variable");
        }

        private static readonly CommandLineParser __parser;

        private string ServiceId
        {
            get
            {
                string sericeId;

                if (_options.TryGet(ServiceIdKey, out sericeId))
                {
                    return sericeId;
                }

                return Environment.GetEnvironmentVariable("AWS_SERVICE_ID", EnvironmentVariableTarget.Process);
            }
        }

        private string ServiceSecret
        {
            get
            {
                string sericeSecret;

                if (_options.TryGet(ServiceSecretKey, out sericeSecret))
                {
                    return sericeSecret;
                }

                return Environment.GetEnvironmentVariable("AWS_SERVICE_SECRET", EnvironmentVariableTarget.Process);
            }
        }

        static void Main(string[] args)
        {
            new Program(__parser.Parse(Environment.CommandLine)).Run();
        }

        private void Run()
        {
            string containerName;
            string domainName;
            string objectPath;
            string fileName;
            string helpOptionName;

            try
            {
                if (_options.TryGet(HelpKey, out helpOptionName))
                {
                    WriteHelp(helpOptionName);
                }
                else if (_options.Contains(ListBucketsKey))
                {
                    ListBuckets();
                }
                else if (_options.Contains(ListDomainsKey))
                {
                    ListDomains();
                }
                else if (_options.Contains(ListQueuesKey))
                {
                    ListQueues();
                }
                else if (_options.TryGet(ListObjectsKey, out containerName))
                {
                    ListObjects(containerName);
                }
                else if (_options.TryGet(ListItemsKey, out domainName))
                {
                    ListItems(domainName);
                }
                else if (_options.TryGet(GetObjectKey, out objectPath))
                {
                    GetObject(objectPath, _options.Get(FileNameKey));
                }
                else if (_options.TryGet(PutObjectKey, out objectPath))
                {
                    PutObject(objectPath, _options.Get(FileNameKey));
                }
                else if (
                    _options.TryGet(SaveItemsKey, out domainName) &&
                    _options.TryGet(FileNameKey, out fileName))
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

        private Program(CommandLineOptions options)
        {
            _options = options;
        }

        private void WriteHelp(string optionName)
        {
            Console.WriteLine("Amazon Web Services Shell");
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

        private void WriteShortHelp()
        {
            Console.WriteLine("Type 'AwsSh --help' for help");
        }

        private void ListQueues()
        {
            PrettyConsole console = new PrettyConsole();
            console.AddColumn("Name", 64);
            console.AddColumn("MessageCount", 16);
            console.AddColumn("MessageNotVisibleCount", 22);
            console.WriteHeader();

            foreach (SqsQueue queue in SqsQueue.ListQueues(ServiceId, ServiceSecret))
            {
                long messageCount;
                long messageNotVisibleCount;

                queue.GetInfo(out messageCount, out messageNotVisibleCount);

                console.WriteLine(queue.Name, messageCount, messageNotVisibleCount);
            }
        }

        private void ListDomains()
        {
            PrettyConsole console = new PrettyConsole();
            console.AddColumn("Name", 64);
            console.AddColumn("ItemCount", 16);
            console.AddColumn("SizeBytes", 16);
            console.WriteHeader();

            foreach (SdbTable table in SdbTable.ListTables(ServiceId, ServiceSecret))
            {
                long itemCount;
                long sizeBytes;

                table.GetInfo(out itemCount, out sizeBytes);

                console.WriteLine(table.Name, itemCount, sizeBytes);
            }
        }

        private void ListObjects(string containerName)
        {
            PrettyConsole console = new PrettyConsole();
            console.AddColumn("Name", 64);
            console.AddColumn("LastModifiedDate", 24);
            console.AddColumn("SizeBytes", 16);
            console.WriteHeader();

            IContainer container = S3Container.Get(ServiceId, ServiceSecret, containerName);

            foreach (IBlock block in container.ListBlocks())
            {
                console.WriteLine(block.Name, block.LastModifiedDate, block.SizeBytes);
            }
        }

        private void ListBuckets()
        {
            PrettyConsole console = new PrettyConsole();
            console.AddColumn("Name", 64);
            console.AddColumn("CreationDate", 24);
            console.WriteHeader();

            foreach (IContainer container in S3Container.ListContainers(ServiceId, ServiceSecret))
            {
                console.WriteLine(container.Name, container.CreationDate);
            }
        }

        private void ListItems(string domainName)
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

        private void PutObject(string path, string fileName)
        {
            string containerName;
            string blockName;

            if (TryParsePath(path, out containerName, out blockName))
            {
                IContainer container = S3Container.Get(ServiceId, ServiceSecret, containerName);
                IBlock block = container.GetBlock(blockName);

                if (string.IsNullOrEmpty(fileName))
                {
                    fileName = blockName;
                }

                long currentCountBytes = 0;
                long fileSizeBytes = new FileInfo(fileName).Length;

                if (fileSizeBytes != 0)
                {
                    using (FileStream input = new FileStream(fileName, FileMode.Open))
                    {
                        block.Write((output) =>
                        {
                            byte[] buffer = new byte[BufferSize];
                            int countBytes = 0;

                            using (output)
                            {
                                do
                                {
                                    countBytes = input.Read(buffer, 0, buffer.Length);

                                    if (countBytes != 0)
                                    {
                                        output.Write(buffer, 0, countBytes);

                                        currentCountBytes += countBytes;
                                        WriteProgress("Uploaded {0:F2} %", ((float)countBytes / (float)fileSizeBytes) * 100.0);
                                    }
                                }
                                while (countBytes != 0);
                            }
                        });
                    }
                }

                Console.WriteLine("{0} uploaded {1} bytes", Path.GetFullPath(fileName), currentCountBytes);
            }
        }

        private void GetObject(string path, string fileName)
        {
            string containerName;
            string blockName;

            if (TryParsePath(path, out containerName, out blockName))
            {
                IContainer container = S3Container.Get(ServiceId, ServiceSecret, containerName);
                IBlock block = container.GetBlock(blockName);

                if (string.IsNullOrEmpty(fileName))
                {
                    fileName = blockName;
                }

                long currentCountBytes = 0;

                using (FileStream output = new FileStream(fileName, FileMode.Create))
                {
                    block.Read((input) =>
                    {
                        byte[] buffer = new byte[BufferSize];
                        int countBytes = 0;

                        using (input)
                        {
                            do
                            {
                                countBytes = input.Read(buffer, 0, buffer.Length);

                                if (countBytes != 0)
                                {
                                    output.Write(buffer, 0, countBytes);

                                    currentCountBytes += countBytes;

                                    WriteProgress("Downloaded {0} bytes", currentCountBytes);
                                }
                            }
                            while (countBytes != 0);
                        }
                    });
                }

                if (currentCountBytes != 0)
                {
                    Console.WriteLine("{0} downloaded {1} bytes", Path.GetFullPath(fileName), currentCountBytes);
                }
            }
        }

        private void SaveItems(string domainName, string fileName)
        {
            long itemCount;

            using (FileStream stream = new FileStream(fileName, FileMode.Create))
            {
                using (XmlWriter writer = XmlWriter.Create(stream))
                {
                    writer.WriteStartDocument();
                    writer.WriteStartElement("Domain");

                    long sizeBytes;

                    SdbTable table = SdbTable.Create(ServiceId, ServiceSecret, domainName);
                    table.GetInfo(out itemCount, out sizeBytes);
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

                                WriteProgress("Saved {0:F1} %", ((float)count / (float)itemCount) * 100.0);
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
            }

            Console.WriteLine("{0} saved {1} items {2} bytes", Path.GetFullPath(fileName), itemCount, new FileInfo(fileName).Length);
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

        private static string ParseContainerName(string containerName)
        {
            string[] segments = containerName.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < segments.Length; i++)
            {
                if (builder.Length != 0)
                {
                    builder.Append('/');
                }

                builder.Append(segments[i]);
            }

            return builder.ToString();
        }

        private static bool TryParsePath(string path, out string containerName, out string blockName)
        {
            string[] segments = path.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);

            if (segments.Length >= 2)
            {
                StringBuilder builder = new StringBuilder();

                for (int i = 0; i < (segments.Length - 1); i++)
                {
                    if (builder.Length != 0)
                    {
                        builder.Append('/');
                    }

                    builder.Append(segments[i]);
                }

                containerName = builder.ToString();
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
