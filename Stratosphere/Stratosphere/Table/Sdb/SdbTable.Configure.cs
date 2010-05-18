// Copyright (c) 2010 7Clouds

using System.Xml;
using System.Xml.Linq;

namespace Stratosphere.Table.Sdb
{
    public sealed partial class SdbTable
    {
        public static ITable Configure(XElement configuration)
        {
            XElement serviceId = configuration.Element(XName.Get("ServiceId"));
            XElement serviceSecret = configuration.Element(XName.Get("ServiceSecret"));
            XElement domainName = configuration.Element(XName.Get("DomainName"));
            XElement ensureDomain = configuration.Element(XName.Get("EnsureDomain"));
            XElement selectLimit = configuration.Element(XName.Get("SelectLimit"));
            XElement withConsistency = configuration.Element(XName.Get("WithConsistency"));

            if (serviceId != null && serviceSecret != null && domainName != null)
            {
                return Create(serviceId.Value, serviceSecret.Value, domainName.Value,
                    withConsistency != null ? XmlConvert.ToBoolean(withConsistency.Value) : false,
                    selectLimit != null ? (int?)XmlConvert.ToInt32(selectLimit.Value) : null,
                    ensureDomain != null ? XmlConvert.ToBoolean(ensureDomain.Value) : false);
            }

            return null;
        }
    }
}
