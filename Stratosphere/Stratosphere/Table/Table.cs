// Copyright (c) 2010, 7Clouds. All rights reserved.

using System.Collections.Generic;
using System.Linq;
using System;

namespace Stratosphere.Table
{
    public interface IExpectedWriter
    {
        void WhenExpected(string name, string value);
        void WhenExpectedNotExists(string name);
    }

    public interface IPutWriter : IExpectedWriter
    {
        void ReplaceAttribute(string name, string value);
        void AddAttribute(string name, string value);
    }

    public interface IDeleteWriter : IExpectedWriter
    {
        void DeleteItem();
        void DeleteAttribute(string name);
        void DeleteAttribute(string name, string value);
    }

    public enum ReadingState
    {
        Value = 0,
        BeginItem,
        BeginAttribute,

        End = -1
    }

    public interface IReader : IDisposable
    {
        ReadingState Read();

        string ItemName { get; }
        string AttributeName { get; }
        string AttributeValue { get; }
    }

    public interface ITable
    {
        void Put(string name, Action<IPutWriter> action);
        void Delete(string name, Action<IDeleteWriter> action);
        
        IReader Select(IEnumerable<string> attributeNames, Condition condition);
        long SelectCount(Condition condition);

        void Delete();
    }
}
