// Copyright (c) 2010 7Clouds

using System;

namespace Stratosphere.Table
{
    public interface IReader : IDisposable
    {
        ReadingState Read();

        string ItemName { get; }
        string AttributeName { get; }
        string AttributeValue { get; }
    }
}
