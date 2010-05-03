// Copyright (c) 2010, 7Clouds. All rights reserved.

namespace Stratosphere.Aws
{
    internal class AmazonActionBuilder : AmazonParametersBuilder
    {
        public AmazonActionBuilder(string action)
        {
            Add("Action", action);
        }
    }
}