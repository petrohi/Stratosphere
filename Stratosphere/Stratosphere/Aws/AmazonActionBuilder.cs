// Copyright (c) 2010 7Clouds

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