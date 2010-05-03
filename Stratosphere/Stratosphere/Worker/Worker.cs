// Copyright (c) 2010, 7Clouds. All rights reserved.

using System.Threading;

namespace Stratosphere.Worker
{
    public interface IWorker
    {
        void Run(WaitHandle terminationHandle);
    }
}
