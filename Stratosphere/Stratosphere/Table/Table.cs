// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;

namespace Stratosphere.Table
{
    public interface ITable
    {
        void Put(string name, Action<IPutWriter> action);
        void Delete(string name, Action<IDeleteWriter> action);
        
        IReader Select(IEnumerable<string> attributeNames, Condition condition);
        long SelectCount(Condition condition);

        void Delete();
    }
}
