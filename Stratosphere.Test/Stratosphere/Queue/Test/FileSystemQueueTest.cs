// Copyright (c) 2010, 7Clouds. All rights reserved.

using Stratosphere.Queue.FileSystem;
using System;
using System.IO;

namespace Stratosphere.Queue.Test
{
    public sealed class FileSystemQueueTest : QueueTest
    {
        protected override IQueue CreateQueue()
        {
            return new DelayedQueue(FileSystemQueue.Create(
                Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    string.Format(QueueDirectoryNameFormat, Guid.NewGuid().ToString().Replace("-", string.Empty)))), DelayMilliseconds);
        }

        private const string QueueDirectoryNameFormat = "q_{0}";
        private const int DelayMilliseconds = 10;
    }
}
