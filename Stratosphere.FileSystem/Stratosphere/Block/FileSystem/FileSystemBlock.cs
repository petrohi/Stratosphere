// Copyright (c) 2010 7Clouds

using System.IO;
using System;

namespace Stratosphere.Block.FileSystem
{
    public sealed class FileSystemBlock : IBlock
    {
        public FileSystemBlock(string fileName)
        {
            _fileName = fileName;
        }

        private readonly string _fileName;

        public string Name { get { return Path.GetFileName(_fileName); } }
        public DateTime LastModifiedDate { get { return File.GetLastWriteTimeUtc(_fileName); } }
        public long SizeBytes { get { return new FileInfo(_fileName).Length; } }

        public void Read(Action<Stream> action)
        {
            if (File.Exists(_fileName))
            {
                using (FileStream stream = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    action(stream);
                }
            }
        }

        public void Write(Action<Stream> action)
        {
            using (FileStream stream = new FileStream(_fileName, FileMode.Create, FileAccess.Write))
            {
                action(stream);
            }
        }

        public void Delete()
        {
            if (File.Exists(_fileName))
            {
                File.Delete(_fileName);
            }
        }
    }
}