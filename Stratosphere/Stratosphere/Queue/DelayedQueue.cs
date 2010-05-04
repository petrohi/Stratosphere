// Copyright (c) 2010 7Clouds

using System.Collections.Generic;
using System.Threading;

namespace Stratosphere.Queue
{
    public sealed class DelayedQueue : IQueue
    {
        private readonly IQueue _queue;
        private readonly int _delayMilliseconds;

        public DelayedQueue(IQueue queue, int delayMilliseconds)
        {
            _queue = queue;
            _delayMilliseconds = delayMilliseconds;
        }

        public void Send(string message)
        {
            _queue.Send(message);
            Delay();
        }

        public IEnumerable<IReceipt> Receive(int maximumCount)
        {
            Delay();
            return _queue.Receive(maximumCount);
        }

        public void Delete()
        {
            _queue.Delete();
            Delay();
        }

        private void Delay()
        {
            if (_delayMilliseconds != 0)
            {
                Thread.Sleep(_delayMilliseconds);
            }
        }
    }
}
