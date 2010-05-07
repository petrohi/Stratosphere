// Copyright (c) 2010 7Clouds

namespace Stratosphere.Table
{
    public enum ReadingState
    {
        EmptyItem = 0,
        Item,
        Attribute,
        Value,

        End = -1
    }
}
