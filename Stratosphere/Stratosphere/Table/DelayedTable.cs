// Copyright (c) 2010, 7Clouds. All rights reserved.

using System.Collections.Generic;
using System.Threading;
using System;

namespace Stratosphere.Table
{
    public sealed class DelayedTable : ITable
    {
        private readonly ITable _table;
        private readonly int _delayMilliseconds;

        public DelayedTable(ITable table, int delayMilliseconds)
        {
            _table = table;
            _delayMilliseconds = delayMilliseconds;
        }

        public void Put(string name, Action<IPutWriter> action)
        {
            Delay();
            _table.Put(name, action);
        }

        public void Delete(string name, Action<IDeleteWriter> action)
        {
            Delay();
            _table.Delete(name, action);
        }

        public IReader Select(IEnumerable<string> attributeNames, Condition condition)
        {
            Delay();
            return _table.Select(attributeNames, condition);
        }

        public long SelectCount(Condition condition)
        {
            Delay();
            return _table.SelectCount(condition);
        }
        
        public void Delete()
        {
            Delay();
            _table.Delete();
        }
        
        private void Delay()
        {
            if (_delayMilliseconds != 0)
            {
                Thread.Sleep(_delayMilliseconds);
            }
        }
    }
}
