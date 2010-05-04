// Copyright (c) 2010 7Clouds

namespace Stratosphere.Table
{
    public interface IPutWriter : IExpectedWriter
    {
        void ReplaceAttribute(string name, string value);
        void AddAttribute(string name, string value);
    }
}
