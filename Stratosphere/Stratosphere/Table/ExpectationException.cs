// Copyright (c) 2010 7Clouds

using System;

namespace Stratosphere.Table
{
    public sealed class ExpectationException : Exception
    {
        public ExpectationException() : base(DefaultMessage) { }

        public ExpectationException(Exception inner) : base(DefaultMessage, inner) { }

        private const string DefaultMessage = "Expectations violated";
    }
}
