// Copyright (c) 2010 7Clouds

using System;

namespace Stratosphere.Table
{
    public sealed class ExpectationException : Exception
    {
        public ExpectationException() : base(Message) { }

        public ExpectationException(Exception inner) : base(Message, inner) { }

        private const string Message = "Expectations violated";
    }
}
