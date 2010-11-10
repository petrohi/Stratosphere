// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Stratosphere.Block.FileSystem
{
    public sealed partial class FileSystemContainer : FileSystemBase, IContainer
    {
        public static FileSystemContainer Create(string directoryName)
        {
            return new FileSystemContainer(directoryName);
        }

        private FileSystemContainer(string directoryName)
        {
            _directoryName = directoryName;
            
            if (!Directory.Exists(_directoryName))
            {
                Directory.CreateDirectory(_directoryName);
            }

            _creationDate = Directory.GetCreationTimeUtc(_directoryName);
        }

        private readonly string _directoryName;
        private readonly DateTime _creationDate;

        public string Name { get { return _directoryName; } }
        public DateTime CreationDate { get { return _creationDate; } }

        public IEnumerable<IBlock> ListBlocks(string prefix, int pageSize)
        {
            if (string.IsNullOrEmpty(prefix))
            {
                return Directory.GetFiles(_directoryName).Select(fn => (IBlock)new FileSystemBlock(fn));
            }
            else
            {
                return Directory.GetFiles(_directoryName, EncodeName(prefix) + "*").Select(fn => (IBlock)new FileSystemBlock(fn));
            }
        }
        
        public IBlock GetBlock(string name)
        {
            return new FileSystemBlock(Path.Combine(_directoryName, EncodeName(name)));
        }
        
        public void Delete()
        {
            if (Directory.Exists(_directoryName))
            {
                Directory.Delete(_directoryName);
            }
        }
    }
}