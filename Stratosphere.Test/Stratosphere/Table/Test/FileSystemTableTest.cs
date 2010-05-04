// Copyright (c) 2010 7Clouds

using Stratosphere.Table.FileSystem;
using System;
using System.IO;

namespace Stratosphere.Table.Test
{
    public sealed class FileSystemTableTest : TableTest
    {
        protected override ITable CreateTable()
        {
            return FileSystemTable.Create(
                Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), 
                    string.Format(TableFileNameFormat, Guid.NewGuid().ToString().Replace("-", string.Empty))));
        }

        private const string TableFileNameFormat = "t_{0}.db3";
    }
}
