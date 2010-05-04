// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Stratosphere.Queue.Test
{
    public abstract class QueueTest : IDisposable
    {
        private readonly IQueue _queue;

        public QueueTest()
        {
            _queue = CreateQueue();
        }

        public void Dispose()
        {
            _queue.Delete();
        }

        protected abstract IQueue CreateQueue();

        protected class Message : Dictionary<string, string>
        {
            public Message() { }

            public Message(IEnumerable<KeyValuePair<string, string>> pairs) :
                base(pairs.ToDictionary((pair) => pair.Key, (pair) => pair.Value)) { }
        }

        [Fact]
        public void OneYieldSingleReceiveDelete()
        {
            IQueue q = _queue;

            List<Message> sm = new List<Message>(YieldMessages());
            sm.ForEach((m) => q.Send(m));

            List<Message> rm = new List<Message>();

            for (int i = 0; i < sm.Count * 2; i++)
            {
                IReceipt rt = q.Receive();

                if (rt != null)
                {
                    rm.Add(rt.GetMessage<Message>());
                    rt.Delete();
                }
            }

            AssertSentReceivedMessages(
                sm,
                rm);
        }

        [Fact]
        public void ManyYieldsSingleReceiveDelete()
        {
            IQueue q = _queue;

            List<Message> sm = new List<Message>();

            for (int i = 0; i < ManyYeildCount; i++)
            {
                sm.AddRange(YieldMessages());
            }

            sm.ForEach((m) => q.Send(m));

            List<Message> rm = new List<Message>();

            for (int i = 0; i < sm.Count * 2; i++)
            {
                IReceipt rt = q.Receive();

                if (rt != null)
                {
                    rm.Add(rt.GetMessage<Message>());
                    rt.Delete();
                }
            }

            AssertSentReceivedMessages(
                sm,
                rm);
        }

        [Fact]
        public void ManyYieldsMultiReceiveDelete()
        {
            IQueue q = _queue;

            List<Message> sm = new List<Message>();

            for (int i = 0; i < ManyYeildCount; i++)
            {
                sm.AddRange(YieldMessages());
            }
            
            sm.ForEach((m) => q.Send(m));

            List<Message> rm = new List<Message>();

            for (int i = 0; i < sm.Count * 2; i++)
            {
                foreach (IReceipt rt in q.Receive(7).ToList())
                {
                    if (rt != null)
                    {
                        rm.Add(rt.GetMessage<Message>());
                        rt.Delete();
                    }
                }
            }

            AssertSentReceivedMessages(
                sm,
                rm);
        }

        [Fact]
        public void ManyYieldsSeriesMultiReceiveDelete()
        {
            IQueue q = _queue;

            List<Message> sm = new List<Message>();

            for (int i = 0; i < ManyYeildCount; i++)
            {
                sm.AddRange(YieldMessages());
            }

            sm.ForEach((m) => q.Send(m));

            List<Message> rm = new List<Message>();

            for (int i = 0; i < sm.Count * 2; i++)
            {
                foreach (IReceipt rt in q.Receive(7).Union(q.Receive(2).Union(q.Receive(10))).ToList())
                {
                    if (rt != null)
                    {
                        rm.Add(rt.GetMessage<Message>());
                        rt.Delete();
                    }
                }
            }

            AssertSentReceivedMessages(
                sm,
                rm);
        }

        private IEnumerable<Message> YieldMessages()
        {
            yield return new Message();
            yield return new Message(Pairs(Pair("X", "A")));
            yield return new Message(Pairs(Pair("X", "A"), Pair("XX", "AA")));
            yield return new Message(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA")));
            yield return new Message(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA")));
            yield return new Message(Pairs(Pair("X", "A1"), Pair("XX", "AA1"), Pair("XXX", "AAA")));
            yield return new Message(Pairs(Pair("X", "A1"), Pair("XX", "AA1"), Pair("XXX", "AAA1")));
            yield return new Message(Pairs(Pair("X", "A1"), Pair("XX", "AA1"), Pair("XXX", "AAA1")));
            yield return new Message(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA1")));
            yield return new Message(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1")));
            yield return new Message(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA")));
            yield return new Message(Pairs(Pair("XX", "AA"), Pair("XXX", "AAA")));
            yield return new Message(Pairs(Pair("XXX", "AAA")));
            yield return new Message();
        }

        private class MessageEqualityComparer : IEqualityComparer<Message>, IComparer<Message>
        {
            public bool Equals(Message m0, Message m1)
            {
                return (Compare(m0, m1) == 0);
            }

            public int GetHashCode(Message message)
            {
                int hashCode = 0;

                foreach (var pair in message)
                {
                    hashCode ^= (pair.Key.GetHashCode() ^ pair.Value.GetHashCode());
                }

                return hashCode;
            }

            public int Compare(Message m0, Message m1)
            {
                var pairs0 = m0.OrderBy((p) => p.Key + p.Value).ToArray();
                var pairs1 = m1.OrderBy((p) => p.Key + p.Value).ToArray();

                int lengthCompare = (pairs0.Length - pairs1.Length);

                if (lengthCompare != 0)
                {
                    return lengthCompare;
                }

                for (int i = 0; i < pairs0.Length; i++)
                {
                    int keyCompare = pairs0[i].Key.CompareTo(pairs1[i].Key);

                    if (keyCompare != 0)
                    {
                        return keyCompare;
                    }

                    int valueCompare = pairs0[i].Value.CompareTo(pairs1[i].Value);

                    if (valueCompare != 0)
                    {
                        return valueCompare;
                    }
                }

                return 0;
            }
        }

        private static void AssertSentReceivedMessages(IEnumerable<Message> sent, IEnumerable<Message> received)
        {
            var sentGroup = sent.GroupBy((m) => m, new MessageEqualityComparer()).OrderBy((g) => g.Key, new MessageEqualityComparer()).ToArray();
            var receivedGroup = received.GroupBy((m) => m, new MessageEqualityComparer()).OrderBy((g) => g.Key, new MessageEqualityComparer()).ToArray();

            Assert.Equal(sentGroup.Length, receivedGroup.Length);

            for (int i = 0; i < sentGroup.Length; i++)
            {
                Assert.Equal(sentGroup[i].Key, receivedGroup[i].Key, new MessageEqualityComparer());
                Assert.True(sentGroup.Count() <= received.Count());
            }
        }

        private static IEnumerable<KeyValuePair<string, string>> Pairs(params KeyValuePair<string, string>[] ps)
        {
            return ps;
        }

        private static KeyValuePair<string, string> Pair(string key, string value)
        {
            return new KeyValuePair<string, string>(key, value);
        }

        private const int ManyYeildCount = 5;
    }
}
