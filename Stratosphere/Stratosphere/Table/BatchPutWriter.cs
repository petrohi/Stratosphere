// Copyright (c) 2010 7Clouds

namespace Stratosphere.Table
{
    public interface IBatchPutWriter
    {
        void ReplaceAttribute(string itemName, string attributeName, string value);
        void AddAttribute(string itemName, string attributeName, string value);
    }
}
