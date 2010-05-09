// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
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
            public bool LastArgument;
            public string HelpText;
        }

        private sealed class ParsedCommandLineOptions : CommandLineOptions
        {
            private readonly Dictionary<string, string> _parsed = new Dictionary<string, string>();

            public ParsedCommandLineOptions(string commandLine, Dictionary<string, Option> options)
            {
                string[] names = options.Keys.ToArray();
                int[] positions = names.Select(n => commandLine.IndexOf(n)).ToArray();

                for (int i = 0; i < positions.Length; i++)
                {
                    if (positions[i] != -1)
                    {
                        int start = positions[i] + names[i].Length;
                        Option option = options[names[i]];
                        string value = null;

                        if (!option.LastArgument)
                        {
                            int[] following = positions.Where(p => p > start).ToArray();

                            if (following.Length != 0)
                            {
                                int end = following.Min();
                                value = commandLine.Substring(start, end - start);
                            }
                        }

                        if (value == null)
                        {
                            value = commandLine.Substring(start);
                        }

                        _parsed.Add(names[i], value.Trim().Trim(new char[] { '"' }));
                    }
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

        public void AddOption(string name, bool lastArgument, string helpText)
        {
            _options.Add(name, new Option() { LastArgument = lastArgument, HelpText = helpText });
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

        private const string CreateBucketKey = "--create-bucket";
        private const string DeleteBucketKey = "--delete-bucket";
        private const string ListBucketsKey = "--list-buckets";
        private const string ListObjectsKey = "--list-objects";
        private const string ListDomainsKey = "--list-domains";
        private const string ListItemsKey = "--list-items";
        private const string ListQueuesKey = "--list-queues";
        private const string PutObjectKey = "--put-object";
        private const string GetObjectKey = "--get-object";
        private const string DeleteObjectKey = "--delete-object";
        private const string DeletePrefixedObjectsKey = "--delete-prefixed-objects";
        private const string SaveItemsKey = "--save-items";
        private const string LoadItemsKey = "--load-items";
        private const string SelectItemsKey = "--select-items";
        private const string SelectSaveItemsKey = "--select-save-items";
        private const string CreateDomainKey = "--create-domain";
        private const string DeleteDomainKey = "--delete-domain";
        private const string DeletePrefixedDomainsKey = "--delete-prefixed-domains";
        private const string CreateQueueKey = "--create-queue";
        private const string DeleteQueueKey = "--delete-queue";
        private const string DeletePrefixedQueuesKey = "--delete-prefixed-queues";
        private const string HelpKey = "--help";
        private const string FileNameKey = "--file-name";
        private const string ServiceIdKey = "--service-id";
        private const string ServiceSecretKey = "--service-secret";

        static Program()
        {
            __parser = new CommandLineParser();

            __parser.AddOption(CreateBucketKey, false, "'AwsSh " + CreateBucketKey + " <bucket URL>'\r\nCreate S3 bucket");
            __parser.AddOption(DeleteBucketKey, false, "'AwsSh " + DeleteBucketKey + " <bucket URL>'\r\nDelete S3 bucket");
            __parser.AddOption(ListBucketsKey, false, "'AwsSh " + ListBucketsKey + "'\r\nList S3 buckets");
            __parser.AddOption(ListObjectsKey, false, "'AwsSh " + ListObjectsKey + " <bucket URL>'\r\nList S3 objects");
            __parser.AddOption(ListDomainsKey, false, "'AwsSh " + ListDomainsKey + "'\r\nList SimpleDB domains");
            __parser.AddOption(ListItemsKey, false, "'AwsSh " + ListItemsKey + " <domain name>'\r\nList SimpleDB domain items");
            __parser.AddOption(ListQueuesKey, false, "'AwsSh " + ListQueuesKey + "'\r\nList SQS queues");
            __parser.AddOption(PutObjectKey, false, "'AwsSh " + PutObjectKey + " <object URL> [" + FileNameKey + " <file name>]'\r\nPut S3 object\r\nIf " + FileNameKey + " not specified then file name must match object name");
            __parser.AddOption(GetObjectKey, false, "'AwsSh " + GetObjectKey + " <object URL> [" + FileNameKey + " <file name>]'\r\nGet S3 object\r\nIf " + FileNameKey + " not specified file name will match object name");
            __parser.AddOption(DeleteObjectKey, false, "'AwsSh " + DeleteObjectKey + " <object URL>'\r\nDelete S3 object");
            __parser.AddOption(DeletePrefixedObjectsKey, false, "'AwsSh " + DeleteObjectKey + " <object URL prefix>'\r\nDelete all S3 objects starting with prefix");
            __parser.AddOption(SaveItemsKey, false, "'AwsSh " + SaveItemsKey + " <domain name> " + FileNameKey + " <XML file name>'\r\nSave SimpleDB domain items to XML file");
            __parser.AddOption(LoadItemsKey, false, "'AwsSh " + LoadItemsKey + " <domain name> " + FileNameKey + " <XML file name>'\r\nLoad SimpleDB domain items from XML file");
            __parser.AddOption(SelectItemsKey, false, "'AwsSh " + SelectItemsKey + " <select expression>'\r\nExecute SimpleDB select expression");
            __parser.AddOption(SelectSaveItemsKey, false, "'AwsSh " + SelectSaveItemsKey + " <select expression>" + FileNameKey + " <XML file name>'\r\nExecute SimpleDB select expression and save items to XML file");
            __parser.AddOption(CreateDomainKey, false, "'AwsSh " + CreateDomainKey + " <domain name>'\r\nCreate SimpleDB domain");
            __parser.AddOption(DeleteDomainKey, false, "'AwsSh " + DeleteDomainKey + " <domain name>'\r\nDelete SimpleDB domain");
            __parser.AddOption(DeletePrefixedDomainsKey, false, "'AwsSh " + DeletePrefixedDomainsKey + " <domain name prefix>'\r\nDelete all SimpleDB domains starting with prefix");
            __parser.AddOption(CreateQueueKey, false, "'AwsSh " + CreateQueueKey + " <queue name>'\r\nCreate SQS queue");
            __parser.AddOption(DeleteQueueKey, false, "'AwsSh " + DeleteQueueKey + " <queue name>'\r\nDelete SQS queue");
            __parser.AddOption(DeletePrefixedQueuesKey, false, "'AwsSh " + DeletePrefixedQueuesKey + " <queue name prefix>'\r\nDelete all SQS queues starting with prefix");
            __parser.AddOption(HelpKey, true, "'AwsSh " + HelpKey + " [option]'\r\nPrint help");
            __parser.AddOption(FileNameKey, false, "'AwsSh " + ListBucketsKey + " <file name>'\r\nSpecify file name for corresponding option");
            __parser.AddOption(ServiceIdKey, false, "'AwsSh " + ServiceIdKey + " <service id>'\r\nSpecify AWS service ID\r\nAlternatively can be specified with AWS_SERVICE_ID environment variable");
            __parser.AddOption(ServiceSecretKey, false, "'AwsSh " + ServiceSecretKey + " <service id>'\r\nSpecify AWS service secret\r\nAlternatively can be specified with AWS_SERVICE_SECRET environment variable");
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
            string domainNamePrefix;
            string queueName;
            string queueNamePrefix;
            string selectExpression;
            string objectPath;
            string objectPathPrefix;
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
                else if (_options.TryGet(CreateBucketKey, out containerName))
                {
                    CreateBucket(containerName);
                }
                else if (_options.TryGet(DeleteBucketKey, out containerName))
                {
                    DeleteBucket(containerName);
                }
                else if (_options.TryGet(ListObjectsKey, out containerName))
                {
                    ListObjects(containerName);
                }
                else if (_options.TryGet(ListItemsKey, out domainName))
                {
                    ListItems(domainName);
                }
                else if (_options.TryGet(CreateDomainKey, out domainName))
                {
                    CreateDomain(domainName);
                }
                else if (_options.TryGet(DeleteDomainKey, out domainName))
                {
                    DeleteDomain(domainName);
                }
                else if (_options.TryGet(DeletePrefixedDomainsKey, out domainNamePrefix))
                {
                    DeleteDomains(domainNamePrefix);
                }
                else if (_options.TryGet(CreateQueueKey, out queueName))
                {
                    CreateQueue(queueName);
                }
                else if (_options.TryGet(DeleteQueueKey, out queueName))
                {
                    DeleteQueue(queueName);
                }
                else if (_options.TryGet(DeletePrefixedQueuesKey, out queueNamePrefix))
                {
                    DeleteQueues(queueNamePrefix);
                }
                else if (_options.TryGet(SelectItemsKey, out selectExpression))
                {
                    SelectItems(selectExpression);
                }
                else if (_options.TryGet(GetObjectKey, out objectPath))
                {
                    GetObject(objectPath, _options.Get(FileNameKey));
                }
                else if (_options.TryGet(PutObjectKey, out objectPath))
                {
                    PutObject(objectPath, _options.Get(FileNameKey));
                }
                else if (_options.TryGet(DeleteObjectKey, out objectPath))
                {
                    DeleteObject(objectPath);
                }
                else if (_options.TryGet(DeletePrefixedObjectsKey, out objectPathPrefix))
                {
                    DeleteObjects(objectPathPrefix);
                }
                else if (
                    _options.TryGet(SaveItemsKey, out domainName) &&
                    _options.TryGet(FileNameKey, out fileName))
                {
                    SaveItems(domainName, fileName);
                }
                else if (
                    _options.TryGet(LoadItemsKey, out domainName) &&
                    _options.TryGet(FileNameKey, out fileName))
                {
                    LoadItems(domainName, fileName);
                }
                else if (
                    _options.TryGet(SelectSaveItemsKey, out selectExpression) &&
                    _options.TryGet(FileNameKey, out fileName))
                {
                    SelectAndSaveItems(selectExpression, fileName);
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

        private void CreateBucket(string containerName)
        {
            S3Container.Create(ServiceId, ServiceSecret, containerName);
            
            Console.WriteLine("Created {0} bucket", containerName);
        }

        private void DeleteBucket(string containerName)
        {
            IContainer container = S3Container.Get(ServiceId, ServiceSecret, containerName);
            container.Delete();

            Console.WriteLine("Deleted {0} bucket", containerName);
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

        private void DeleteDomains(string domainNamePrefix)
        {
            int deletedCount = 0;

            foreach (SdbTable table in SdbTable.ListTables(ServiceId, ServiceSecret))
            {
                if (table.Name.StartsWith(domainNamePrefix))
                {
                    table.Delete();

                    deletedCount++;
                }
            }

            Console.WriteLine("Deleted {0} domains", deletedCount);
        }

        private void CreateDomain(string domainName)
        {
            SdbTable.Create(ServiceId, ServiceSecret, domainName);

            Console.WriteLine("Created {0} domain", domainName);
        }

        private void DeleteDomain(string domainName)
        {
            SdbTable table;

            if (SdbTable.TryCreate(ServiceId, ServiceSecret, domainName, null, false, out table))
            {
                table.Delete();

                Console.WriteLine("Deleted {0} domain", domainName);
            }
            else
            {
                Console.WriteLine("Domain does not exist");
            }
        }

        private void DeleteQueues(string queueNamePrefix)
        {
            int deletedCount = 0;

            foreach (SqsQueue queue in SqsQueue.ListQueues(ServiceId, ServiceSecret))
            {
                if (queue.Name.StartsWith(queueNamePrefix))
                {
                    queue.Delete();

                    deletedCount++;
                }
            }

            Console.WriteLine("Deleted {0} queues", deletedCount);
        }

        private void CreateQueue(string queueName)
        {
            SqsQueue.Create(ServiceId, ServiceSecret, queueName);

            Console.WriteLine("Created {0} queue", queueName);
        }

        private void DeleteQueue(string queueName)
        {
            SqsQueue queue;

            if (SqsQueue.TryCreate(ServiceId, ServiceSecret, queueName, false, out queue))
            {
                queue.Delete();

                Console.WriteLine("Deleted {0} queue", queueName);
            }
            else
            {
                Console.WriteLine("Queue does not exist");
            }
        }

        private void ListItems(string domainName)
        {
            SdbTable table;

            if (SdbTable.TryCreate(ServiceId, ServiceSecret, domainName, null, false, out table))
            {
                using (IReader reader = table.Select(new string[] { }, null))
                {
                    WriteItems(reader);
                }
            }
            else
            {
                Console.WriteLine("Domain does not exist");
            }
        }

        private void SelectItems(string selectExpression)
        {
            using (IReader reader = SdbTable.Select(ServiceId, ServiceSecret, selectExpression))
            {
                WriteItems(reader);
            }
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

        private void DeleteObject(string path)
        {
            string containerName;
            string blockName;

            if (TryParsePath(path, out containerName, out blockName))
            {
                IContainer container = S3Container.Get(ServiceId, ServiceSecret, containerName);
                IBlock block = container.GetBlock(blockName);
                block.Delete();

                Console.WriteLine("Deleted {0} object", path);
            }
        }

        private void DeleteObjects(string pathPrefix)
        {
            int deletedCount = 0;

            string containerName;
            string blockNamePrefix;

            if (TryParsePath(pathPrefix, out containerName, out blockNamePrefix))
            {
                IContainer container = S3Container.Get(ServiceId, ServiceSecret, containerName);

                foreach (IBlock block in container.ListBlocks())
                {
                    if (block.Name.StartsWith(blockNamePrefix))
                    {
                        block.Delete();
                        deletedCount++;
                    }
                }
            }

            Console.WriteLine("Deleted {0} queues", deletedCount);
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

        private void LoadItems(string domainName, string fileName)
        {
            long readCount = 0;

            using (FileStream stream = new FileStream(fileName, FileMode.Open))
            {
                using (XmlReader reader = XmlReader.Create(stream))
                {
                    SdbTable table = SdbTable.Create(ServiceId, ServiceSecret, domainName);

                    while (reader.Read())
                    {
                        if (reader.NodeType == XmlNodeType.Element)
                        {
                            if (reader.LocalName == "Item")
                            {
                                int depth = reader.Depth;

                                if (reader.MoveToAttribute("Name"))
                                {
                                    string itemName = reader.Value;
                                    reader.MoveToElement();

                                    table.Put(itemName, w =>
                                    {
                                        while (reader.Read() && reader.Depth > depth)
                                        {
                                            if (reader.NodeType == XmlNodeType.Element &&
                                                !reader.IsEmptyElement)
                                            {
                                                string name = XmlConvert.DecodeName(reader.LocalName);

                                                if (reader.Read())
                                                {
                                                    string value = reader.ReadContentAsString();

                                                    if (!string.IsNullOrEmpty(value))
                                                    {
                                                        w.AddAttribute(name, value);
                                                    }
                                                }
                                            }
                                        }
                                    });

                                    readCount++;
                                    WriteProgress("Loaded {0} items", readCount);
                                }
                            }
                        }
                    }
                }
            }

            Console.WriteLine("{0} loaded {1} items {2} bytes", Path.GetFullPath(fileName), readCount, new FileInfo(fileName).Length);
        }

        private void SaveItems(string domainName, string fileName)
        {
            long writtenCount = 0;
            SdbTable table;

            if (SdbTable.TryCreate(ServiceId, ServiceSecret, domainName, null, false, out table))
            {
                long expectedCount;
                long sizeBytes;

                table.GetInfo(out expectedCount, out sizeBytes);

                using (FileStream stream = new FileStream(fileName, FileMode.Create))
                {
                    using (XmlWriter writer = XmlWriter.Create(stream))
                    {
                        writer.WriteStartDocument();
                        writer.WriteStartElement("Domain");

                        using (IReader reader = table.Select(new string[] { }, null))
                        {
                            writtenCount = WriteItems(expectedCount, writer, reader);
                        }
                    }
                }

                Console.WriteLine("{0} saved {1} items {2} bytes", Path.GetFullPath(fileName), writtenCount, new FileInfo(fileName).Length);
            }
            else
            {
                Console.WriteLine("Domain does not exist");
            }
        }

        private void SelectAndSaveItems(string selectExpression, string fileName)
        {
            long writtenCount = 0;

            using (FileStream stream = new FileStream(fileName, FileMode.Create))
            {
                using (XmlWriter writer = XmlWriter.Create(stream))
                {
                    writer.WriteStartDocument();
                    writer.WriteStartElement("Domain");

                    using (IReader reader = SdbTable.Select(ServiceId, ServiceSecret, selectExpression))
                    {
                        writtenCount = WriteItems(-1, writer, reader);
                    }
                }
            }

            Console.WriteLine("{0} saved {1} items {2} bytes", Path.GetFullPath(fileName), writtenCount, new FileInfo(fileName).Length);
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

        private static void WriteItems(IReader reader)
        {
            ReadingState state;
            PrettyConsole console = new PrettyConsole();
            console.AddColumn("ItemName", 32);
            console.AddColumn("AttributeName", 32);
            console.AddColumn("AttributeValue", 32);
            console.WriteHeader();

            while ((state = reader.Read()) != ReadingState.End)
            {
                if (state == ReadingState.EmptyItem)
                {
                    console.WriteLine(reader.ItemName);
                }
                else if (state == ReadingState.Item)
                {
                    console.WriteLine(reader.ItemName, reader.AttributeName, reader.AttributeValue);
                }
                else if (state == ReadingState.Attribute)
                {
                    console.WriteLine(string.Empty, reader.AttributeName, reader.AttributeValue);
                }
                else
                {
                    console.WriteLine(string.Empty, string.Empty, reader.AttributeValue);
                }
            }
        }

        private static int WriteItems(long expectedCount, XmlWriter writer, IReader reader)
        {
            int count = 0;
            ReadingState state;
            bool itemBegan = false;

            while ((state = reader.Read()) != ReadingState.End)
            {
                if (state == ReadingState.EmptyItem ||
                    state == ReadingState.Item)
                {
                    if (itemBegan)
                    {
                        writer.WriteEndElement();
                    }

                    BeginWriteItem(writer, reader);

                    if (!itemBegan)
                    {
                        itemBegan = true;
                    }

                    if (state != ReadingState.EmptyItem)
                    {
                        WriteAttribute(writer, reader);
                    }

                    if (expectedCount != -1)
                    {
                        WriteProgress("Saved {0:F1} %", ((float)count / (float)expectedCount) * 100.0);
                    }
                    else
                    {
                        WriteProgress("Saved {0} items", count);
                    }

                    count++;
                }
                else
                {
                    WriteAttribute(writer, reader);
                }
            }

            if (itemBegan)
            {
                writer.WriteEndElement();
            }

            writer.WriteEndElement();
            writer.WriteEndDocument();

            return count;
        }

        private static void BeginWriteItem(XmlWriter writer, IReader reader)
        {
            writer.WriteStartElement("Item");
            writer.WriteAttributeString("Name", reader.ItemName);
        }

        private static void WriteAttribute(XmlWriter writer, IReader reader)
        {
            writer.WriteElementString(XmlConvert.EncodeLocalName(reader.AttributeName), reader.AttributeValue);
        }

        private const int BufferSize = 64 * 1024;
    }
}
