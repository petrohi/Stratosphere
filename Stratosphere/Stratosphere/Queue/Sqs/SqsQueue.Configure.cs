// Copyright (c) 2010 7Clouds

using System.Xml;
using System.Xml.Linq;

namespace Stratosphere.Queue.Sqs
{
    public sealed partial class SqsQueue
    {
        public static IQueue Configure(XElement configuration)
        {
            XElement serviceId = configuration.Element(XName.Get("ServiceId"));
            XElement serviceSecret = configuration.Element(XName.Get("ServiceSecret"));
            XElement queueName = configuration.Element(XName.Get("QueueName"));

            if (serviceId != null && serviceSecret != null && queueName != null)
            {
                return Create(serviceId.Value, serviceSecret.Value, queueName.Value);
            }

            return null;
        }
    }
}
