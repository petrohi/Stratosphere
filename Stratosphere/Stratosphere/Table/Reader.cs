// Copyright (c) 2010 7Clouds

using System;

namespace Stratosphere.Table
{
    public interface IReader : IDisposable
    {
        bool Read();
        ReaderPosition Position { get; }

        string ItemName { get; }
        string AttributeName { get; }
        string AttributeValue { get; }
    }
}
