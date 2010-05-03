// Copyright (c) 2010, 7Clouds. All rights reserved.

namespace Stratosphere.Queue
{
    public interface IReceipt
    {
        string Message { get; }

        void Delete();
    }
}
