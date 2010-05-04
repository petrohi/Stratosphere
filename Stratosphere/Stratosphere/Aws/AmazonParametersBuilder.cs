// Copyright (c) 2010 7Clouds

using System.Collections.Generic;

namespace Stratosphere.Aws
{
    internal class AmazonParametersBuilder : List<KeyValuePair<string, string>>
    {
        public void Add(string name, string value)
        {
            Add(new KeyValuePair<string, string>(name, value));
        }

        public const string TrueString = "true";
        public const string FalseString = "false";
    }
}