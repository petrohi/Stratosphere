// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using Stratosphere.Queue.Sqs;
using Stratosphere.Test;
using Stratosphere.Aws;

namespace Stratosphere.Queue.Test
{
    public sealed class SqsQueueTest : QueueTest
    {
        private const string QueueNameFormat = "unit_test{0}";
        private const int DelayMilliseconds = 10;

        private sealed class ReliableSqsQueue : IQueue
        {
            private readonly IQueue _queue;

            public ReliableSqsQueue(IQueue queue)
            {
                _queue = queue;
            }

            public void Send(string message)
            {
                AmazonReliability.Execute(() => { _queue.Send(message); });
            }

            public IEnumerable<IReceipt> Receive(int maximumCount)
            {
                return AmazonReliability.Execute(() => _queue.Receive(maximumCount));
            }

            public void Delete()
            {
                AmazonReliability.Execute(() => { _queue.Delete(); });
            }
        }

        protected override IQueue CreateQueue()
        {
            return new ReliableSqsQueue(new DelayedQueue(SqsQueue.Create(
                AmazonTest.ServiceId,
                AmazonTest.ServiceSecret,
                GetNextQueueName()), DelayMilliseconds));
        }

        private string GetNextQueueName()
        {
            return string.Format(QueueNameFormat, Guid.NewGuid().ToString().Replace("-", string.Empty));
        }
    }
}
