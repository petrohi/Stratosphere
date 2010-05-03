// Copyright (c) 2010, 7Clouds. All rights reserved.

using System.IO;
using System;
using System.Net;

namespace Stratosphere.Block.S3
{
    public sealed class S3Block : S3Base, IBlock
    {
        public S3Block(S3Service service, string containerName, string name, DateTime lastModifiedDate, long sizeBytes)
        {
            _service = service;
            _containerName = containerName;
            _name = name;
            _lastModifiedDate = lastModifiedDate;
            _sizeBytes = sizeBytes;
        }

        private readonly S3Service _service;
        private readonly string _containerName;
        private readonly string _name;
        private readonly DateTime _lastModifiedDate;
        private readonly long _sizeBytes;

        public string Name { get { return _name; } }
        public DateTime LastModifiedDate { get { return _lastModifiedDate; } }
        public long SizeBytes { get { return _sizeBytes; } }

        public void Read(Action<Stream> action)
        {
            try
            {
                GetResponse(_service.CreateRequest(GetMethod, _containerName, _name), action);
            }
            catch (WebException e)
            {
                if (e.Response != null)
                {
                    HttpWebResponse response = (HttpWebResponse)e.Response;

                    if (response.StatusCode != HttpStatusCode.NotFound)
                    {
                        throw;
                    }
                }
                else
                {
                    throw;
                }
            }
        }

        public void Write(Action<Stream> action)
        {
            GetResponse(_service.CreateRequest(PutMethod, _containerName, _name, string.Empty, string.Empty, EmptyHeaders, action));
        }

        public void Delete()
        {
            GetResponse(_service.CreateRequest(DeleteMethod, _containerName, _name));
        }
    }
}