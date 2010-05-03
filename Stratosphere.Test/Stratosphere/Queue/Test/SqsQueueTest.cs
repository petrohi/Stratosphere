// Copyright (c) 2010, 7Clouds. All rights reserved.

using System;
using Stratosphere.Queue.Sqs;
using Stratosphere.Test;

namespace Stratosphere.Queue.Test
{
    public sealed class SqsQueueTest : QueueTest
    {
        private const string QueueNameFormat = "unit_test{0}";
        private const int DelayMilliseconds = 10;

        protected override IQueue CreateQueue()
        {
            return new DelayedQueue(SqsQueue.Create(
                AmazonTest.ServiceId,
                AmazonTest.ServiceSecret,
                GetNextQueueName()), DelayMilliseconds);
        }

        private string GetNextQueueName()
        {
            return string.Format(QueueNameFormat, Guid.NewGuid().ToString().Replace("-", string.Empty));
        }
    }
}
