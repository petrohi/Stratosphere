// Copyright (c) 2010 7Clouds

namespace Stratosphere.Table
{
    public interface IExpectedWriter
    {
        void WhenExpected(string name, string value);
        void WhenExpectedNotExists(string name);
    }
}
