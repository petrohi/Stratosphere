// Copyright (c) 2010 7Clouds

using System.Xml;
using System.Xml.Linq;

namespace Stratosphere.Block.FileSystem
{
    public sealed partial class FileSystemContainer
    {
        public static IContainer Configure(XElement configuration)
        {
            XElement directoryName = configuration.Element(XName.Get("DirectoryName"));

            if (directoryName != null)
            {
                return new FileSystemContainer(directoryName.Value);
            }

            return null;
        }
    }
}
