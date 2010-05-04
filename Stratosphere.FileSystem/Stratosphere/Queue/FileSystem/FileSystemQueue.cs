// Copyright (c) 2010 7Clouds

using System.Collections.Generic;
using System.IO;
using System.Xml;
using System.Text;
using System;
using System.Linq;
using System.Threading;

namespace Stratosphere.Queue.FileSystem
{
    public sealed partial class FileSystemQueue : IQueue
    {
        public static FileSystemQueue Create(string directoryName)
        {
            return new FileSystemQueue(directoryName, DefaultVisibilityDelayMilliseconds);
        }

        private string GetNextMessageFileName()
        {
            return Path.Combine(_directoryName, string.Format("{0}.xml", Guid.NewGuid().ToString().Replace("-", string.Empty)));
        }

        public void Send(string message)
        {
            EnsureDirectory();

            using (FileStream stream = new FileStream(GetNextMessageFileName(), FileMode.Create, FileAccess.Write))
            {
                using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8))
                {
                    writer.Write(message);
                }
            }
        }

        public IEnumerable<IReceipt> Receive(int maximumCount)
        {
            if (Directory.Exists(_directoryName))
            {
                HashSet<string> fileNames = new HashSet<string>(Directory.GetFiles(_directoryName));
                string[] notReceivedFileNames = fileNames.Where((n) =>
                    Path.HasExtension(n) && !fileNames.Contains(Path.Combine(Path.GetDirectoryName(n), Path.GetFileNameWithoutExtension(n)))).ToArray();

                for (int i = 0; i < Math.Min(maximumCount, notReceivedFileNames.Length); i++)
                {
                    TimeSpan span = DateTime.UtcNow.Subtract(File.GetLastWriteTimeUtc(notReceivedFileNames[i]));

                    if (span.TotalMilliseconds > _visibilityDelayMilliseconds)
                    {
                        string message;

                        using (Stream stream = new FileStream(notReceivedFileNames[i], FileMode.Open, FileAccess.Read))
                        {
                            using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
                            {
                                message = reader.ReadToEnd();
                            }
                        }

                        yield return new Receipt(message, notReceivedFileNames[i]);
                    }
                }
            }
        }

        public void Delete()
        {
            Directory.Delete(_directoryName, true);
        }

        private class Receipt : IReceipt
        {
            public Receipt(string message, string fileName)
            {
                _message = message;
                _fileName = fileName;
            }

            public string Message
            {
                get { return _message; }
            }

            public void Delete()
            {
                if (File.Exists(_fileName))
                {
                    File.AppendAllText(Path.Combine(
                        Path.GetDirectoryName(_fileName), Path.GetFileNameWithoutExtension(_fileName)), string.Empty);
                }
            }

            private readonly string _message;
            private readonly string _fileName;
        }

        private FileSystemQueue(string directoryName, int visibilityDelayMilliseconds)
        {
            _directoryName = directoryName;
            _visibilityDelayMilliseconds = visibilityDelayMilliseconds;
        }

        private void EnsureDirectory()
        {
            if (!Directory.Exists(_directoryName))
            {
                Directory.CreateDirectory(_directoryName);
            }
        }

        private readonly string _directoryName;
        private readonly int _visibilityDelayMilliseconds;

        private const int DefaultVisibilityDelayMilliseconds = 25;
    }
}
