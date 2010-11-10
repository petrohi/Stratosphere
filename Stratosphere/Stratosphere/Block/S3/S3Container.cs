// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Stratosphere.Block.S3
{
    public sealed partial class S3Container : S3Base, IContainer
    {
        public static IEnumerable<S3Container> ListContainers(string serviceId, string serviceSecret)
        {
            S3Service service = new S3Service(serviceId, serviceSecret);
            XElement response = GetResponse(service.CreateRequest(GetMethod));

            return response.Descendants(
                S3 + "Bucket").Select(b => new S3Container(service, 
                    b.Element(S3 + "Name").Value, 
                    XmlConvert.ToDateTime(b.Element(S3 + "CreationDate").Value, XmlDateTimeSerializationMode.Utc)));
        }

        public static S3Container Get(string serviceId, string serviceSecret, string name)
        {
            return new S3Container(new S3Service(serviceId, serviceSecret), name, DateTime.MinValue);
        }

        public static S3Container Create(string serviceId, string serviceSecret, string name)
        {
            S3Service service = new S3Service(serviceId, serviceSecret);
            GetResponse(service.CreateRequest(PutMethod, name));
            return new S3Container(service, name, DateTime.MinValue);
        }

        private S3Container(S3Service service, string name, DateTime creationDate)
        {
            _service = service;
            _name = name;
            _creationDate = creationDate;
        }

        private readonly S3Service _service;
        private readonly string _name;
        private readonly DateTime _creationDate;

        public string Name { get { return _name; } }
        public DateTime CreationDate { get { return _creationDate; } }

        public IEnumerable<IBlock> ListBlocks(string prefix, int pageSize)
        {
            bool isTruncated = false;
            string markerName = null;
            
            do
            {
                StringBuilder queryBuilder = new StringBuilder("?max-keys=");
                queryBuilder.Append(pageSize);
                
                if (!string.IsNullOrEmpty(prefix))
                {
                    queryBuilder.AppendFormat("&prefix={0}", prefix);
                }
                
                if (!string.IsNullOrEmpty(markerName))
                {
                    queryBuilder.AppendFormat("&marker={0}", markerName);
                }
                
                XElement response = GetResponse(_service.CreateRequest(GetMethod, _name, queryBuilder.ToString(), string.Empty));
                isTruncated = XmlConvert.ToBoolean(response.Element(S3 + "IsTruncated").Value);
                
                foreach (IBlock block in response.Elements(S3 + "Contents").Select(o => (IBlock)new S3Block(
                    _service,
                    _name,
                    o.Element(S3 + "Key").Value,
                    XmlConvert.ToDateTime(o.Element(S3 + "LastModified").Value, XmlDateTimeSerializationMode.Utc),
                    XmlConvert.ToInt64(o.Element(S3 + "Size").Value))))
                {
                    yield return block;
                    
                    markerName = block.Name;
                }
            }
            while (isTruncated);
        }

        public IBlock GetBlock(string name) { return new S3Block(_service, _name, name, DateTime.MinValue, 0); }

        public void Delete()
        {
            GetResponse(_service.CreateRequest(DeleteMethod, _name));
        }
    }
}