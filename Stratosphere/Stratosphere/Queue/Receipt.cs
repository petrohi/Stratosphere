// Copyright (c) 2010 7Clouds

namespace Stratosphere.Queue
{
    public interface IReceipt
    {
        string Message { get; }

        void Delete();
    }
}
