// Copyright (c) 2010 7Clouds

namespace Stratosphere.Table
{
    public enum ReadingState
    {
        Value = 0,
        BeginItem,
        BeginAttribute,

        End = -1
    }
}
