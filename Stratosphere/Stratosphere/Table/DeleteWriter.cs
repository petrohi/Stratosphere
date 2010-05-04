// Copyright (c) 2010 7Clouds

namespace Stratosphere.Table
{
    public interface IDeleteWriter : IExpectedWriter
    {
        void DeleteItem();
        void DeleteAttribute(string name);
        void DeleteAttribute(string name, string value);
    }
}
