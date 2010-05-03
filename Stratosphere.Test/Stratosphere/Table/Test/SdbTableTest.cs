// Copyright (c) 2010, 7Clouds. All rights reserved.

using System;
using Stratosphere.Table.Sdb;
using Stratosphere.Test;

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
    }

    public sealed class SdbTableTest : SdbTableTestBase
    {
        protected override ITable CreateTable()
        {
            return new DelayedTable(SdbTable.Create(
                AmazonTest.ServiceId, AmazonTest.ServiceSecret,
                GetNextDomainName(), true, null),DelayMilliseconds);
        }
    }

    public sealed class SdbTableTestWithSelectLimit : SdbTableTestBase
    {
        private const int SelectLimit = 2;

        protected override ITable CreateTable()
        {
            return new DelayedTable(SdbTable.Create(
                AmazonTest.ServiceId, AmazonTest.ServiceSecret,
                GetNextDomainName(), true, SelectLimit), DelayMilliseconds);
        }
    }
}
