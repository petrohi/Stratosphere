// Copyright (c) 2010, 7Clouds. All rights reserved.

using System;
using System.IO;
using System.Threading;

namespace Stratosphere.FileSystem
{
    public static class FileSystemReliability
    {
        private const int MaximumRerunCount = 10;
        private const int BackoffDelayBaseMilliseconds = 100;

        private static void BackoffDelay(int count)
        {
            Thread.Sleep(BackoffDelayBaseMilliseconds + (count * count * BackoffDelayBaseMilliseconds));
        }

        public static void Execute(Action action)
        {
            Execute(action, null);
        }

        public static void Execute(Action action, Action<Exception> retried)
        {
            Execute(() => { action(); return 0; }, retried);
        }

        public static T Execute<T>(Func<T> action)
        {
            return Execute<T>(action, null);
        }

        public static T Execute<T>(Func<T> action, Action<Exception> retried)
        {
            int retryCount = 0;

            while (true)
            {
                try
                {
                    return action();
                }
                catch (IOException ioException)
                {
                    if (retryCount <= MaximumRerunCount)
                    {
                        if (retried != null)
                        {
                            retried(ioException);
                        }

                        BackoffDelay(retryCount);

                        retryCount++;
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }
    }
}