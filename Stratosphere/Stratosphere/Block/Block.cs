// Copyright (c) 2010 7Clouds

using System.IO;
using System;

namespace Stratosphere.Block
{
    public interface IBlock
    {
        string Name { get; }
        DateTime LastModifiedDate { get; }
        long SizeBytes { get; }

        void Read(Action<Stream> action);
        void Write(Action<Stream> action);
        void Delete();
    }
}