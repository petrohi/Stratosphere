// Copyright (c) 2010 7Clouds

using System.Xml.Linq;

namespace Stratosphere.Table.FileSystem
{
    public sealed partial class FileSystemTable
    {
        public static ITable Configure(XElement configuration)
        {
            XElement fileName = configuration.Element(XName.Get("DatabaseFileName"));

            if (fileName != null)
            {
                return Create(fileName.Value);
            }

            return null;
        }
    }
}
