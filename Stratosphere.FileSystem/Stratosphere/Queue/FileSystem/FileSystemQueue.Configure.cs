// Copyright (c) 2010, 7Clouds. All rights reserved.

using System.Xml.Linq;

namespace Stratosphere.Queue.FileSystem
{
    public sealed partial class FileSystemQueue
    {
        public static IQueue Configure(XElement configuration)
        {
            XElement directoryName = configuration.Element(XName.Get("DirectoryName"));

            if (directoryName != null)
            {
                return Create(directoryName.Value);
            }

            return null;
        }
    }
}
