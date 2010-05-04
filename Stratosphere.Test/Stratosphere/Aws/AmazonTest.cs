// Copyright (c) 2010 7Clouds

using System;

namespace Stratosphere.Test
{
    public static class AmazonTest
    {
        public static string ServiceId
        {
            get
            {
                return Environment.GetEnvironmentVariable("AWS_SERVICE_ID", EnvironmentVariableTarget.Process);
            }
        }

        public static string ServiceSecret
        {
            get
            {
                return Environment.GetEnvironmentVariable("AWS_SERVICE_SECRET", EnvironmentVariableTarget.Process);
            }
        }
    }
}
