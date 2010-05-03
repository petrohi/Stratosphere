// Copyright (c) 2010, 7Clouds. All rights reserved.

using System.Xml;
using System.Xml.Linq;

namespace Stratosphere.Block.S3
{
    public sealed partial class S3Container
    {
        public static IContainer Configure(XElement configuration)
        {
            XElement serviceId = configuration.Element(XName.Get("ServiceId"));
            XElement serviceSecret = configuration.Element(XName.Get("ServiceSecret"));
            XElement containerName = configuration.Element(XName.Get("ContainerName"));

            if (serviceId != null && serviceSecret != null && containerName != null)
            {
                return GetContainer(serviceId.Value, serviceSecret.Value, containerName.Value);
            }

            return null;
        }
    }
}
