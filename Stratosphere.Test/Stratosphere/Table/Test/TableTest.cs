// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Stratosphere.Table.Test
{
    public abstract class TableTest : IDisposable
    {
        private ITable _table;

        public TableTest()
        {
            _table = CreateTable();
        }

        public void Dispose()
        {
            _table.Delete();
        }

        protected abstract ITable CreateTable();

        protected class Element : Dictionary<string, string>
        {
            public Element() { }

            public Element(IEnumerable<KeyValuePair<string, string>> pairs) :
                base(pairs.ToDictionary((pair) => pair.Key, (pair) => pair.Value)) { }
        }

        [Fact]
        public void Merge()
        {
            ITable t = _table;

            var es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            t.Set("e0", new Element());
            t.Set("e1", new Element());
            t.Set("e2", new Element());

            es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);

            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA"))));
            t.Set("e1", new Element(Pairs(Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB"))));
            t.Set("e2", new Element(Pairs(Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"))));

            es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB"));
            AssertPairs(es, "e2", Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"));

            t.Set("e0", new Element(Pairs(Pair("X", "A1"), Pair("XXXX", "AAAA"), Pair("XXXXX", null))));
            t.Set("e1", new Element(Pairs(Pair("YY", "BB1"), Pair("YYY", "BBB1"), Pair("YYYY", string.Empty))));
            t.Set("e2", new Element(Pairs(Pair("Z", string.Empty), Pair("ZZZ", "CCC1"))));

            es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB1"));
            AssertPairs(es, "e2", Pair("ZZ", "CC"), Pair("ZZZ", "CCC1"));

            t.Set("e0", new Element(Pairs(Pair("X", "A2"), Pair("XXXX", "AAAA1"))));
            t.Set("e1", new Element(Pairs(Pair("YYYY", "BBBB"), Pair("YYY", "BBB2"))));
            t.Set("e2", new Element(Pairs(Pair("ZZ", string.Empty), Pair("ZZZ", null))));

            es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);

            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A2"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA1"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB2"), Pair("YYYY", "BBBB"));

            t.Set("e0", new Element(Pairs(Pair("X", string.Empty), Pair("XX", string.Empty), Pair("XXX", string.Empty), Pair("XXXX", string.Empty))));
            t.Set("e1", new Element(Pairs(Pair("Y", null), Pair("YY", null), Pair("YYY", null))));

            es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);

            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("YYYY", "BBBB"));

            t.Set("e1", new Element(Pairs(Pair("YYYY", null))));

            es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);

            AssertIdentities(es);
        }

        [Fact]
        public void EnumerateByIdentity()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
        }

        [Fact]
        public void EnumerateByPair()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("X", "A"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XX", "AA"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("X", "A1"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XX", "AA1"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA1"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA2"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXXX", "AAAA"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);
        }

        [Fact]
        public void EnumerateByKeyIsNull()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithAttributeIsNull("X")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXXXX")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XXX", "AAA"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"))));

            es = t.Get<Element>(Condition.WithAttributeIsNull("X")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeIsNull("XX")).ToDictionary(p => p.Key, p => p.Value);
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertIdentities(es, "e0");

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXXXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));
        }

        [Fact]
        public void EnumerateByEmptyGroupCondition()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.And(new Condition[] { })).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");

            es = t.Get<Element>(Condition.And(Condition.And(new Condition[] { }), Condition.And(new Condition[] { }))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
        }

        [Fact]
        public void EnumerateByPairAndPair()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XXX", "AAA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XX", "AA")), Condition.WithAttributeValue(Pair("XXX", "AAA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XX", "AA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XXX", "AAA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XX", "AA1")), Condition.WithAttributeValue(Pair("XXX", "AAA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);
        }
        
        [Fact]
        public void EnumerateByKeyIsNullAndPair()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("X"), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("XXXXX"), Condition.WithAttributeValue(Pair("X", "A")))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XXX", "AAA"))));
            t.Set("e1", new Element(Pairs(Pair("XXX", "AAA"), Pair("XXXX", "AAAA"))));
            t.Set("e2", new Element(Pairs(Pair("XX", "AA"), Pair("XXXX", "AAAA"))));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("X"), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("XX", "AA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("X"), Condition.WithAttributeValue(Pair("XXXX", "AAAA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));
            AssertPairs(es, "e2", Pair("XX", "AA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("XX"), Condition.WithAttributeValue(Pair("XXX", "AAA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("XXX"), Condition.WithAttributeValue(Pair("X", "A")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("XXX"), Condition.WithAttributeValue(Pair("XX", "AA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);
        }

        [Fact]
        public void EnumerateByIdenityAndPair()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.And(Condition.WithItemName("e0"), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XXX", "AAA")), Condition.WithItemName("e1"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithItemName("e2"), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithItemName("e1"), Condition.WithAttributeValue(Pair("X", "A1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithItemName("e0"), Condition.WithAttributeValue(Pair("XX", "AA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XXX", "AAA1")), Condition.WithItemName("e2"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);
        }

        private void AssertIdentities(Dictionary<string, Element> elements, params string[] ids)
        {
            Assert.Equal(ids.Length, elements.Count);

            for (int i = 0; i < ids.Length; i++)
            {
                Assert.True(elements.ContainsKey(ids[i]));
                Assert.NotNull(elements[ids[i]]);
            }
        }

        private void AssertPairs(Dictionary<string, Element> elements, string id, params KeyValuePair<string, string>[] ps)
        {
            Assert.True(elements.ContainsKey(id));
            Assert.NotNull(elements[id]);
            Assert.Equal(ps.Length, elements[id].Count);

            for (int i = 0; i < ps.Length; i++)
            {
                Assert.True(elements[id].ContainsKey(ps[i].Key));
                Assert.Equal(elements[id][ps[i].Key], ps[i].Value);
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
    }
}
