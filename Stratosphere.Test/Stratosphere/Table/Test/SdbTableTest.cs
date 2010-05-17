// Copyright (c) 2010 7Clouds

using System;
using Stratosphere.Table.Sdb;
using Stratosphere.Test;
using Stratosphere.Aws;
using System.Collections.Generic;

namespace Stratosphere.Table.Test
{
    public abstract class SdbTableTestBase : TableTest
    {
        private const string DomainNameFormat = "unit_test{0}";
        protected const int DelayMilliseconds = 1000;

        protected string GetNextDomainName()
        {
            return string.Format(DomainNameFormat, Guid.NewGuid().ToString().Replace("-", string.Empty));
        }

        protected sealed class ReliableSdbTable : ITable
        {
            private readonly ITable _table;

            public ReliableSdbTable(ITable table)
            {
                _table = table;
            }

            public void Delete()
            {
                AmazonReliability.Execute(() => { _table.Delete(); });
            }

            public void Put(string name, System.Action<IPutWriter> action)
            {
                AmazonReliability.Execute(() => { _table.Put(name, action); });
            }

            public void BatchPut(Action<IBatchPutWriter> action)
            {
                AmazonReliability.Execute(() => { _table.BatchPut(action); });
            }

            public void Delete(string name, System.Action<IDeleteWriter> action)
            {
                AmazonReliability.Execute(() => { _table.Delete(name, action); });
            }

            public IReader Select(IEnumerable<string> attributeNames, Condition condition, bool withConsistency)
            {
                return AmazonReliability.Execute(() => _table.Select(attributeNames, condition, withConsistency));
            }
        }
    }

    public sealed class SdbTableTest : SdbTableTestBase
    {
        protected override ITable CreateTable()
        {
            return AmazonReliability.Execute(() => new ReliableSdbTable(new DelayedTable(SdbTable.Create(
                AmazonTest.ServiceId, AmazonTest.ServiceSecret,
                GetNextDomainName(), null),DelayMilliseconds)));
        }
    }

    public sealed class SdbTableTestWithSelectLimit : SdbTableTestBase
    {
        private const int SelectLimit = 2;

        protected override ITable CreateTable()
        {
            return AmazonReliability.Execute(() => new ReliableSdbTable(new DelayedTable(SdbTable.Create(
                AmazonTest.ServiceId, AmazonTest.ServiceSecret,
                GetNextDomainName(), SelectLimit), DelayMilliseconds)));
        }
    }
}
