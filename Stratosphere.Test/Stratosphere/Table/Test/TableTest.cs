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
        public void WriteRead()
        {
            ITable t = _table;

            var pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(0, pairs.Length);

            t.Put("e0", w => w.AddAttribute("X", "A"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(1, pairs.Length);
            Assert.Equal(pairs[0].Key, "e0");
            Assert.Equal(pairs[0].Value, "A");

            t.Put("e1", w => w.AddAttribute("X", "A"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(2, pairs.Length);
            Assert.Equal(pairs[0].Key, "e0");
            Assert.Equal(pairs[0].Value, "A");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A");

            t.Put("e1", w => w.AddAttribute("X", "A1"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(3, pairs.Length);
            Assert.Equal(pairs[0].Key, "e0");
            Assert.Equal(pairs[0].Value, "A");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A");
            Assert.Equal(pairs[2].Key, "e1");
            Assert.Equal(pairs[2].Value, "A1");

            t.Put("e1", w => w.AddAttribute("X", "A2"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(4, pairs.Length);
            Assert.Equal(pairs[0].Key, "e0");
            Assert.Equal(pairs[0].Value, "A");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A");
            Assert.Equal(pairs[2].Key, "e1");
            Assert.Equal(pairs[2].Value, "A1");
            Assert.Equal(pairs[3].Key, "e1");
            Assert.Equal(pairs[3].Value, "A2");

            t.Put("e0", w => w.AddAttribute("X", "A"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(4, pairs.Length);
            Assert.Equal(pairs[0].Key, "e0");
            Assert.Equal(pairs[0].Value, "A");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A");
            Assert.Equal(pairs[2].Key, "e1");
            Assert.Equal(pairs[2].Value, "A1");
            Assert.Equal(pairs[3].Key, "e1");
            Assert.Equal(pairs[3].Value, "A2");

            t.Put("e0", w => w.ReplaceAttribute("X", "A1"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(4, pairs.Length);
            Assert.Equal(pairs[0].Key, "e0");
            Assert.Equal(pairs[0].Value, "A1");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A");
            Assert.Equal(pairs[2].Key, "e1");
            Assert.Equal(pairs[2].Value, "A1");
            Assert.Equal(pairs[3].Key, "e1");
            Assert.Equal(pairs[3].Value, "A2");

            t.Delete("e1", w => w.DeleteAttribute("X", "A"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(3, pairs.Length);
            Assert.Equal(pairs[0].Key, "e0");
            Assert.Equal(pairs[0].Value, "A1");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A1");
            Assert.Equal(pairs[2].Key, "e1");
            Assert.Equal(pairs[2].Value, "A2");

            t.Delete("e0", w => w.DeleteAttribute("X", "A"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(3, pairs.Length);
            Assert.Equal(pairs[0].Key, "e0");
            Assert.Equal(pairs[0].Value, "A1");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A1");
            Assert.Equal(pairs[2].Key, "e1");
            Assert.Equal(pairs[2].Value, "A2");

            t.Delete("e0", w => w.DeleteAttribute("X"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(2, pairs.Length);
            Assert.Equal(pairs[0].Key, "e1");
            Assert.Equal(pairs[0].Value, "A1");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A2");

            t.Delete("e0", w => w.DeleteAttribute("X"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(2, pairs.Length);
            Assert.Equal(pairs[0].Key, "e1");
            Assert.Equal(pairs[0].Value, "A1");
            Assert.Equal(pairs[1].Key, "e1");
            Assert.Equal(pairs[1].Value, "A2");

            t.Put("e1", w => w.ReplaceAttribute("X", "A"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(1, pairs.Length);
            Assert.Equal(pairs[0].Key, "e1");
            Assert.Equal(pairs[0].Value, "A");

            t.Delete("e1", w => w.DeleteAttribute("X"));

            pairs = t.Select("X").OrderBy(p => p.Key + p.Value).ToArray();
            Assert.Equal(0, pairs.Length);
        }

        [Fact]
        public void Set()
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
        public void GetByItemName()
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
        public void GetItemNameByItemName()
        {
            ITable t = _table;

            var es = t.Get<Element>(new string[] { TableExtension.ItemNameAttribute }, Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(new string[] { TableExtension.ItemNameAttribute }, Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(new string[] { TableExtension.ItemNameAttribute }, Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0");

            es = t.Get<Element>(new string[] { TableExtension.ItemNameAttribute }, Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1");
        }

        [Fact]
        public void GetAttributesByItemName()
        {
            ITable t = _table;

            var es = t.Get<Element>(new string[] { "X" }, Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(new string[] { "X", "XX" }, Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(new string[] { "X" }, Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"));

            es = t.Get<Element>(new string[] { "X", "XX" }, Condition.WithItemName("e0")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"));

            es = t.Get<Element>(new string[] { "X" }, Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"));

            es = t.Get<Element>(new string[] { "X", "XX" }, Condition.WithItemName("e1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"));
        }

        [Fact]
        public void GetByAttributeValueBetween()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithAttributeBetween("X", "A", "B")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.WithAttributeBetween("X", "A", "C")).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"))));
            t.Set("e1", new Element(Pairs(Pair("X", "B"))));
            t.Set("e2", new Element(Pairs(Pair("X", "C"))));
            t.Set("e3", new Element(Pairs(Pair("X", "D"))));

            es = t.Get<Element>(Condition.WithAttributeBetween("X", "A", "B")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");

            es = t.Get<Element>(Condition.WithAttributeBetween("X", "A", "C")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");

            es = t.Get<Element>(Condition.WithAttributeBetween("X", "B", "C")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");
        }

        [Fact]
        public void GetByAttributeValueIn()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "B" })).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "C" })).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"))));
            t.Set("e1", new Element(Pairs(Pair("X", "B"))));
            t.Set("e2", new Element(Pairs(Pair("X", "C"))));
            t.Set("e3", new Element(Pairs(Pair("X", "D"))));

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "B" })).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "C", "B" })).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "C" })).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e2");

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "C", "B" })).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");

            Assert.Throws<ArgumentException>(() => t.Get<Element>(Condition.WithAttributeIn("X", new string[] { })).ToArray());
        }

        [Fact]
        public void GetByAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithAttributeValue(Pair("X", "A"))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XX", "AA"))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("X", "A"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("X", ValueTest.LessOrEqual, "A")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("X", ValueTest.GreaterThan, "A")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("X", ValueTest.NotEqual, "A")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XX", "AA"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XX", ValueTest.LessOrEqual, "AA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XX", ValueTest.GreaterThan, "AA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XX", ValueTest.NotEqual, "AA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.LessOrEqual, "AAA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.GreaterThan, "AAA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.NotEqual, "AAA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("X", "A1"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("X", ValueTest.LessThan,  "A1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XX", "AA1"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XX", ValueTest.LessThan, "AA1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA1"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.LessThan, "AAA1")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA2"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.LessThan, "AAA2")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.GreaterOrEqual, "AAA2")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXXX", "AAAA"))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue("XXXX", ValueTest.LessThan, "AAAA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue("XXXX", ValueTest.GreaterOrEqual, "AAAA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue("XXXX", ValueTest.NotEqual, "AAAA")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);
        }

        [Fact]
        public void GetByAttributeIsNullIsNotNull()
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

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("X")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XX")).ToDictionary(p => p.Key, p => p.Value);
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertIdentities(es, "e0");

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("XX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("XXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("XXXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXXXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("XXXXX")).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);
        }

        [Fact]
        public void GetByEmptyGroupCondition()
        {
            ITable t = _table;

            var es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.And(new Condition[] { })).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.And(Condition.And(new Condition[] { }), Condition.And(new Condition[] { }))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>().ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");

            es = t.Get<Element>(Condition.And(new Condition[] { })).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");

            es = t.Get<Element>(Condition.And(Condition.And(new Condition[] { }), Condition.And(new Condition[] { }))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
        }

        [Fact]
        public void GetByAttributeValueAndAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XXX", "AAA")))).ToDictionary(p => p.Key, p => p.Value);
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
        public void GetByAttributeValueOrAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XXX", "AAA")))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XXX", "AAA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("XX", "AA")), Condition.WithAttributeValue(Pair("XXX", "AAA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XX", "AA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XXX", "AAA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("XX", "AA1")), Condition.WithAttributeValue(Pair("XXX", "AAA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XX", "AA1")), Condition.WithAttributeValue(Pair("XXX", "AAA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));
        }
        
        [Fact]
        public void GetByAttributeIsNullAndAttributeValue()
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
        public void GetByAttributeIsNotNullAndAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XX"), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXXXX"), Condition.WithAttributeValue(Pair("X", "A")))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            t.Set("e0", new Element(Pairs(Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e1", new Element(Pairs(Pair("XXX", "AAA"), Pair("XXXX", "AAAA"))));
            t.Set("e2", new Element(Pairs(Pair("XX", "AA"), Pair("XXXX", "AAAA"))));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXXX"), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("XX", "AA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXX"), Condition.WithAttributeValue(Pair("XXXX", "AAAA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XX"), Condition.WithAttributeValue(Pair("XXX", "AAA")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXX"), Condition.WithAttributeValue(Pair("X", "A")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXX"), Condition.WithAttributeValue(Pair("XXXX", "AAAA1")))).ToDictionary(p => p.Key, p => p.Value);
            AssertIdentities(es);
        }

        [Fact]
        public void GetByItemNameAndPAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithItemName("e0"), Condition.WithAttributeValue(Pair("XX", "AA")))).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(0, es.Count);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XXX", "AAA")), Condition.WithItemName("e1"))).ToDictionary(p => p.Key, p => p.Value);
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
