// Copyright (c) 2010 7Clouds

using System;

namespace Stratosphere.Table
{
    public sealed class ExpectationException : Exception
    {
    }

    public interface IExpectedWriter
    {
        void WhenExpected(string name, string value);
        void WhenExpectedNotExists(string name);
    }
}
