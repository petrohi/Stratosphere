// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using Stratosphere.Aws;

namespace Stratosphere.Queue.Sqs
{
    public sealed partial class SqsQueue : IQueue
    {
        private readonly Uri _uri;
        private readonly SqsService _service;

        private class GetQueueAttributesBuilder : AmazonActionBuilder
        {
            public GetQueueAttributesBuilder()
                : base("GetQueueAttributes")
            {
                Add("AttributeName", "All");
            }
        }

        private class DeleteQueueBuilder : AmazonActionBuilder
        {
            public DeleteQueueBuilder()
                : base("DeleteQueue") { }
        }

        private class ListQueuesBuilder : AmazonActionBuilder
        {
            public ListQueuesBuilder()
                : base("ListQueues") { }
        }

        private class CreateQueueBuilder : AmazonActionBuilder
        {
            public CreateQueueBuilder(string queueName) :
                base("CreateQueue")
            {
                Add("QueueName", queueName);
            }
        }

        private class SendMessageBuilder : AmazonActionBuilder
        {
            public SendMessageBuilder(string message) :
                base("SendMessage")
            {
                Add("MessageBody", message);
            }
        }

        private class ReceiveMessageBuilder : AmazonActionBuilder
        {
            public ReceiveMessageBuilder(int maximumNumberOfMessages) :
                base("ReceiveMessage")
            {
                Add("MaxNumberOfMessages", XmlConvert.ToString(maximumNumberOfMessages));
            }
        }

        private class DeleteMessageBuilder : AmazonActionBuilder
        {
            public DeleteMessageBuilder(string receiptHandle) :
                base("DeleteMessage")
            {
                Add("ReceiptHandle", receiptHandle);
            }
        }
        
        public static IEnumerable<SqsQueue> ListQueues(string serviceId, string serviceSecret)
        {
            SqsService service = new SqsService(serviceId, serviceSecret);

            return ListQueues(service);
        }

        public static SqsQueue Create(string serviceId, string serviceSecret, string queueName)
        {
            SqsService service = new SqsService(serviceId, serviceSecret);
            return Create(service, queueName);
        }

        public static SqsQueue Create(string serviceId, string serviceSecret, string queueName, bool ensureQueue)
        {
            SqsQueue queue;

            if (TryCreate(serviceId, serviceSecret, queueName, ensureQueue, out queue))
            {
                return queue;
            }

            return null;
        }

        public static bool TryCreate(string serviceId, string serviceSecret, string queueName, bool ensureQueue, out SqsQueue queue)
        {
            SqsService service = new SqsService(serviceId, serviceSecret);
            SqsQueue[] queues = ListQueues(service).ToArray();

            foreach (SqsQueue q in queues)
            {
                if (q.Name.Equals(queueName, StringComparison.InvariantCultureIgnoreCase))
                {
                    queue = q;
                    return true;
                }
            }

            if (ensureQueue)
            {
                queue = Create(service, queueName);
                return true;
            }

            queue = null;
            return false;
        }
        
        public void GetInfo(out long messageCount, out long messageNotVisibleCount)
        {
            XElement responseElement = _service.Execute(new GetQueueAttributesBuilder(), _uri);

            messageCount = long.Parse(GetAttributeValue(responseElement, "ApproximateNumberOfMessages"));
            messageNotVisibleCount = long.Parse(GetAttributeValue(responseElement, "ApproximateNumberOfMessagesNotVisible"));
        }

        public void Send(string message)
        {
            _service.Execute(new SendMessageBuilder(message), _uri);
        }

        public IEnumerable<IReceipt> Receive(int maximumCount)
        {
            if (maximumCount > MaximumCountLimit)
            {
                throw new ArgumentOutOfRangeException();
            }

            XElement responseElement = _service.Execute(new ReceiveMessageBuilder(maximumCount) , _uri);

            foreach (XElement message in responseElement.Descendants(Sqs + "Message"))
            {
                yield return new Receipt(this,
                    message.Element(Sqs + "Body").Value,
                    message.Element(Sqs + "ReceiptHandle").Value);
            }
        }

        public void Delete()
        {
            _service.Execute(new DeleteQueueBuilder(), _uri);
        }

        public string Name { get { return UriToName(_uri); } }

        private SqsQueue(Uri uri, SqsService service)
        {
            _uri = uri;
            _service = service;
        }

        private class Receipt : IReceipt
        {
            public Receipt(SqsQueue queue, string message, string receiptHandler)
            {
                _queue = queue;
                _message = message;
                _receiptHandle = receiptHandler;
            }

            public string Message { get { return _message; } }

            public void Delete()
            {
                _queue.DeleteMessage(_receiptHandle);
            }

            private readonly SqsQueue _queue;
            private readonly string _message;
            private readonly string _receiptHandle;
        }

        private static IEnumerable<SqsQueue> ListQueues(SqsService service)
        {
            return service.Execute(new ListQueuesBuilder()).Descendants(Sqs + "QueueUrl").Select(
                url => new SqsQueue(new Uri(url.Value + "/"), service));
        }

        private static SqsQueue Create(SqsService service, string queueName)
        {
            return new SqsQueue(new Uri(service.Execute(new CreateQueueBuilder(queueName)).Descendants(Sqs + "QueueUrl").First().Value + "/"), service);
        }

        private void DeleteMessage(string receiptHandle)
        {
            _service.Execute(new DeleteMessageBuilder(receiptHandle), _uri);
        }

        private static string UriToName(Uri uri)
        {
            string[] segments = uri.Segments;
            return segments[segments.Length - 1].TrimEnd(new char[] { '/' });
        }

        private static string GetAttributeValue(XElement responseElement, string name)
        {
            foreach (XElement attributeElement in responseElement.Descendants())
            {
                XElement nameElement = attributeElement.Element(Sqs + "Name");
                XElement valueElement = attributeElement.Element(Sqs + "Value");

                if (nameElement != null &&
                    nameElement.Value == name &&
                    valueElement != null)
                {
                    return valueElement.Value;
                }
            }

            return string.Empty;
        }

        private const int MaximumCountLimit = 10;

        private static readonly XNamespace Sqs = XNamespace.Get("http://queue.amazonaws.com/doc/2009-02-01/");
    }
}