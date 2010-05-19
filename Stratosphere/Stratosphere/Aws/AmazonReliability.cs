// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

namespace Stratosphere.Aws
{
    public static class AmazonReliability
    {
        private const string ServiceUnavailableCode = "ServiceUnavailable";
        private const string RequestThrottledCode = "RequestThrottled";
        private const string RequestTimeoutCode = "RequestTimeout";
        private const string InternalErrorCode = "InternalError";
        
        private static readonly IEnumerable<string> RerunCodes = new string[]
        {
            ServiceUnavailableCode, 
            RequestThrottledCode, 
            RequestTimeoutCode,
            InternalErrorCode
        };

        private static readonly IEnumerable<WebExceptionStatus> RerunStatuses = new WebExceptionStatus[]
        {
            WebExceptionStatus.ConnectFailure,
            WebExceptionStatus.ReceiveFailure,
            WebExceptionStatus.SendFailure,
            WebExceptionStatus.PipelineFailure,
            WebExceptionStatus.ConnectionClosed,
            WebExceptionStatus.KeepAliveFailure,
            WebExceptionStatus.Pending,
            WebExceptionStatus.Timeout,
            WebExceptionStatus.UnknownError
        };

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
                catch (AmazonException awsException)
                {
                    if (RerunCodes.Contains(awsException.Code) && retryCount <= MaximumRerunCount)
                    {
                        if (retried != null)
                        {
                            retried(awsException);
                        }

                        BackoffDelay(retryCount);

                        retryCount++;
                    }
                    else
                    {
                        throw;
                    }
                }
                catch (WebException webException)
                {
                    if (RerunStatuses.Contains(webException.Status) && retryCount <= MaximumRerunCount)
                    {
                        if (retried != null)
                        {
                            retried(webException);
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