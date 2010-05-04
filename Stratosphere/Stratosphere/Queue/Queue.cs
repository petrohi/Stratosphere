// Copyright (c) 2010 7Clouds

using System.Collections.Generic;

namespace Stratosphere.Queue
{
    public interface IQueue
    {
        void Send(string message);
        IEnumerable<IReceipt> Receive(int maximumCount);

        void Delete();
    }
}
