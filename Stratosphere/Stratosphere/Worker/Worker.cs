// Copyright (c) 2010 7Clouds

using System.Threading;

namespace Stratosphere.Worker
{
    public interface IWorker
    {
        void Run(WaitHandle terminationHandle);
    }
}
