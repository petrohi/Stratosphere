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
        public void WriteExpected()
        {
            ITable t = _table;

            t.Put("e0", w => { w.WhenExpectedNotExists("X"); w.AddAttribute("X", "A"); });
            t.Put("e0", w => { w.WhenExpectedNotExists("XX"); w.AddAttribute("XX", "AA"); });
            t.Put("e0", w => { w.WhenExpectedNotExists("XXX"); w.AddAttribute("XXX", "AAA"); });
            Assert.Throws<ExpectationException>(() => t.Delete("e0", w => { w.WhenExpectedNotExists("XXX"); w.DeleteAttribute("X"); }));
            t.Delete("e0", w => { w.WhenExpectedNotExists("XXXX"); w.DeleteAttribute("XXX"); });
            t.Delete("e0", w => { w.WhenExpectedNotExists("XXX"); w.DeleteAttribute("XX"); });
            t.Delete("e0", w => { w.WhenExpectedNotExists("XX"); w.DeleteAttribute("X"); });
            t.Put("e0", w => { w.WhenExpectedNotExists("X"); w.AddAttribute("X", "A"); });
            t.Put("e0", w => { w.WhenExpectedNotExists("XX"); w.AddAttribute("XX", "AA"); });
            t.Put("e0", w => { w.WhenExpectedNotExists("XXX"); w.AddAttribute("XXX", "AAA"); });
            Assert.Throws<ExpectationException>(() => t.Delete("e0", w => { w.WhenExpectedNotExists("XXX"); w.DeleteAttribute("X"); }));

            Assert.Throws<ExpectationException>(() => t.Put("e1", w => { w.WhenExpected("X", "A"); w.AddAttribute("XX", "AA"); }));
            Assert.Throws<ExpectationException>(() => t.Put("e1", w => { w.WhenExpected("XX", "AA"); w.AddAttribute("XXX", "AAA"); }));
            Assert.Throws<ExpectationException>(() => t.Put("e1", w => { w.WhenExpected("XXX", "AAA"); w.AddAttribute("XXXX", "AAAA"); }));
            Assert.Throws<ExpectationException>(() => t.Delete("e1", w => { w.WhenExpected("XXX", "AAA"); w.DeleteAttribute("XXXX", "AAAA"); }));
            Assert.Throws<ExpectationException>(() => t.Delete("e1", w => { w.WhenExpected("XX", "AA"); w.DeleteAttribute("XXX", "AAA"); }));
            Assert.Throws<ExpectationException>(() => t.Delete("e1", w => { w.WhenExpected("X", "A"); w.DeleteAttribute("XX", "AA"); }));
            t.Put("e1", w => { w.AddAttribute("X", "A"); });
            t.Put("e1", w => { w.WhenExpected("X", "A"); w.AddAttribute("XX", "AA"); });
            t.Put("e1", w => { w.WhenExpected("XX", "AA"); w.AddAttribute("XXX", "AAA"); });
            t.Put("e1", w => { w.WhenExpected("XXX", "AAA"); w.AddAttribute("XXXX", "AAAA"); });
            Assert.Throws<ExpectationException>(() => t.Put("e1", w => { w.WhenExpected("XXXXX", "AAAAA"); w.AddAttribute("XXX", "AAA"); }));
            Assert.Throws<ExpectationException>(() => t.Put("e1", w => { w.WhenExpected("X", "A1"); w.AddAttribute("XX", "AA"); }));
            t.Delete("e1", w => { w.WhenExpected("XXX", "AAA"); w.DeleteAttribute("XXXX", "AAAA"); });
            t.Delete("e1", w => { w.WhenExpected("XX", "AA"); w.DeleteAttribute("XXX", "AAA"); });
            t.Delete("e1", w => { w.WhenExpected("X", "A"); w.DeleteAttribute("XX", "AA"); });
            t.Put("e1", w => { w.WhenExpected("X", "A"); w.AddAttribute("XX", "AA"); });
            t.Put("e1", w => { w.WhenExpected("XX", "AA"); w.AddAttribute("XXX", "AAA"); });
            t.Put("e1", w => { w.WhenExpected("XXX", "AAA"); w.AddAttribute("XXXX", "AAAA"); });
        }

        [Fact]
        public void Set()
        {
            ITable t = _table;

            var es = t.Get<Element>();
            AssertIdentities(es);

            t.Set("e0", new Element());
            t.Set("e1", new Element());
            t.Set("e2", new Element());

            es = t.Get<Element>();

            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA"))));
            t.Set("e1", new Element(Pairs(Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB"))));
            t.Set("e2", new Element(Pairs(Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"))));

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB"));
            AssertPairs(es, "e2", Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"));

            t.Set("e0", new Element(Pairs(Pair("X", "A1"), Pair("XXXX", "AAAA"), Pair("XXXXX", (string)null))));
            t.Set("e1", new Element(Pairs(Pair("YY", "BB1"), Pair("YYY", "BBB1"), Pair("YYYY", string.Empty))));
            t.Set("e2", new Element(Pairs(Pair("Z", string.Empty), Pair("ZZZ", "CCC1"))));

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB1"));
            AssertPairs(es, "e2", Pair("ZZ", "CC"), Pair("ZZZ", "CCC1"));

            t.Set("e0", new Element(Pairs(Pair("X", "A2"), Pair("XXXX", "AAAA1"))));
            t.Set("e1", new Element(Pairs(Pair("YYYY", "BBBB"), Pair("YYY", "BBB2"))));
            t.Set("e2", new Element(Pairs(Pair("ZZ", string.Empty), Pair("ZZZ", (string)null))));

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A2"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA1"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB2"), Pair("YYYY", "BBBB"));

            t.Set("e0", new Element(Pairs(Pair("X", string.Empty), Pair("XX", string.Empty), Pair("XXX", string.Empty), Pair("XXXX", string.Empty))));
            t.Set("e1", new Element(Pairs(Pair("Y", (string)null), Pair("YY", (string)null), Pair("YYY", (string)null))));

            es = t.Get<Element>();

            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("YYYY", "BBBB"));

            t.Set("e1", new Element(Pairs(Pair("YYYY", (string)null))));

            es = t.Get<Element>();

            AssertIdentities(es);
        }

        [Fact]
        public void SetBatch()
        {
            ITable t = _table;

            var es = t.Get<Element>();
            AssertIdentities(es);

            t.Set(Pairs(
                Pair("e0", new Element()),
                Pair("e1", new Element()),
                Pair("e2", new Element())));

            es = t.Get<Element>();

            AssertIdentities(es);

            t.Set(Pairs(
                Pair("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA")))),
                Pair("e1", new Element(Pairs(Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB")))),
                Pair("e2", new Element(Pairs(Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"))))));

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB"));
            AssertPairs(es, "e2", Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"));

            t.Set(Pairs(
                Pair("e0", new Element(Pairs(Pair("X", "A1"), Pair("XXXX", "AAAA"), Pair("XXXXX", (string)null)))),
                Pair("e1", new Element(Pairs(Pair("YY", "BB1"), Pair("YYY", "BBB1"), Pair("YYYY", string.Empty)))),
                Pair("e2", new Element(Pairs(Pair("Z", string.Empty), Pair("ZZZ", "CCC1"))))));

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB1"));
            AssertPairs(es, "e2", Pair("ZZ", "CC"), Pair("ZZZ", "CCC1"));

            t.Set(Pairs(
                Pair("e0", new Element(Pairs(Pair("X", "A2"), Pair("XXXX", "AAAA1")))),
                Pair("e1", new Element(Pairs(Pair("YYYY", "BBBB"), Pair("YYY", "BBB2")))),
                Pair("e2", new Element(Pairs(Pair("ZZ", string.Empty), Pair("ZZZ", (string)null))))));

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A2"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA1"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB2"), Pair("YYYY", "BBBB"));

            t.Set(Pairs(
                Pair("e0", new Element(Pairs(Pair("X", string.Empty), Pair("XX", string.Empty), Pair("XXX", string.Empty), Pair("XXXX", string.Empty)))),
                Pair("e1", new Element(Pairs(Pair("Y", (string)null), Pair("YY", (string)null), Pair("YYY", (string)null))))));

            es = t.Get<Element>();

            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("YYYY", "BBBB"));

            t.Set(Pairs(
                Pair("e1", new Element(Pairs(Pair("YYYY", (string)null))))));

            es = t.Get<Element>();

            AssertIdentities(es);
        }

        [Fact]
        public void SetDeleteEmpty()
        {
            ITable t = _table;

            var es = t.Get<Element>();
            AssertIdentities(es);

            t.Set("e0", new Element(), true);
            t.Set("e1", new Element(), true);
            t.Set("e2", new Element(), true);

            es = t.Get<Element>();

            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA"))), true);
            t.Set("e1", new Element(Pairs(Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB"))), true);
            t.Set("e2", new Element(Pairs(Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"))), true);

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB"));
            AssertPairs(es, "e2", Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"));

            t.Set("e0", new Element(Pairs(Pair("X", "A1"), Pair("XXXX", "AAAA"), Pair("XXXXX", (string)null))), true);
            t.Set("e1", new Element(Pairs(Pair("YY", "BB1"), Pair("YYY", "BBB1"), Pair("YYYY", string.Empty))), true);
            t.Set("e2", new Element(Pairs(Pair("Z", string.Empty), Pair("ZZZ", "CCC1"))), true);

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB1"));
            AssertPairs(es, "e2", Pair("ZZ", "CC"), Pair("ZZZ", "CCC1"));

            t.Set("e0", new Element(Pairs(Pair("X", "A2"), Pair("XXXX", "AAAA1"))), true);
            t.Set("e1", new Element(Pairs(Pair("YYYY", "BBBB"), Pair("YYY", "BBB2"))), true);
            t.Set("e2", new Element(Pairs(Pair("ZZ", string.Empty), Pair("ZZZ", (string)null))), true);

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A2"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA1"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB2"), Pair("YYYY", "BBBB"));

            t.Set("e0", new Element(Pairs(Pair("X", string.Empty), Pair("XX", string.Empty), Pair("XXX", string.Empty), Pair("XXXX", string.Empty))), true);
            t.Set("e1", new Element(Pairs(Pair("Y", (string)null), Pair("YY", (string)null), Pair("YYY", (string)null))), true);

            es = t.Get<Element>();

            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("YYYY", "BBBB"));

            t.Set("e1", new Element(Pairs(Pair("YYYY", (string)null))), true);

            es = t.Get<Element>();

            AssertIdentities(es);
        }

        [Fact]
        public void SetBatchDeleteEmpty()
        {
            ITable t = _table;

            var es = t.Get<Element>();
            AssertIdentities(es);

            t.Set(Pairs(
                Pair("e0", new Element()),
                Pair("e1", new Element()),
                Pair("e2", new Element())), true);

            es = t.Get<Element>();

            AssertIdentities(es);

            t.Set(Pairs(
                Pair("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA")))),
                Pair("e1", new Element(Pairs(Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB")))),
                Pair("e2", new Element(Pairs(Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"))))), true);

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB"), Pair("YYY", "BBB"));
            AssertPairs(es, "e2", Pair("Z", "C"), Pair("ZZ", "CC"), Pair("ZZZ", "CCC"));

            t.Set(Pairs(
                Pair("e0", new Element(Pairs(Pair("X", "A1"), Pair("XXXX", "AAAA"), Pair("XXXXX", (string)null)))),
                Pair("e1", new Element(Pairs(Pair("YY", "BB1"), Pair("YYY", "BBB1"), Pair("YYYY", string.Empty)))),
                Pair("e2", new Element(Pairs(Pair("Z", string.Empty), Pair("ZZZ", "CCC1"))))), true);

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB1"));
            AssertPairs(es, "e2", Pair("ZZ", "CC"), Pair("ZZZ", "CCC1"));

            t.Set(Pairs(
                Pair("e0", new Element(Pairs(Pair("X", "A2"), Pair("XXXX", "AAAA1")))),
                Pair("e1", new Element(Pairs(Pair("YYYY", "BBBB"), Pair("YYY", "BBB2")))),
                Pair("e2", new Element(Pairs(Pair("ZZ", string.Empty), Pair("ZZZ", (string)null))))), true);

            es = t.Get<Element>();

            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A2"), Pair("XX", "AA"), Pair("XXX", "AAA"), Pair("XXXX", "AAAA1"));
            AssertPairs(es, "e1", Pair("Y", "B"), Pair("YY", "BB1"), Pair("YYY", "BBB2"), Pair("YYYY", "BBBB"));

            t.Set(Pairs(
                Pair("e0", new Element(Pairs(Pair("X", string.Empty), Pair("XX", string.Empty), Pair("XXX", string.Empty), Pair("XXXX", string.Empty)))),
                Pair("e1", new Element(Pairs(Pair("Y", (string)null), Pair("YY", (string)null), Pair("YYY", (string)null))))), true);

            es = t.Get<Element>();

            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("YYYY", "BBBB"));

            t.Set(Pairs(
                Pair("e1", new Element(Pairs(Pair("YYYY", (string)null))))), true);

            es = t.Get<Element>();

            AssertIdentities(es);
        }

        [Fact]
        public void Count()
        {
            ITable t = _table;

            Assert.Equal(0, t.SelectCount());

            t.Set("e0", new Element(Pairs(Pair("X", "A"))));
            Assert.Equal(1, t.SelectCount());

            t.Set("e1", new Element(Pairs(Pair("X", "A1"))));
            Assert.Equal(2, t.SelectCount());

            t.Set("e2", new Element(Pairs(Pair("X", "A1"))));
            Assert.Equal(3, t.SelectCount());

            Assert.Equal(3, t.SelectCount(Condition.WithAttributeIsNotNull("X")));
            Assert.Equal(3, t.SelectCount(Condition.WithAttributeIsNull("XX")));
            Assert.Equal(1, t.SelectCount(Condition.WithAttributeValue("X", "A")));
            Assert.Equal(2, t.SelectCount(Condition.WithAttributeValue("X", "A1")));
        }

        [Fact]
        public void GetByItemName()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithItemName("e0"));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithItemName("e1"));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.WithItemName("e0"));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithItemName("e1"));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
        }

        [Fact]
        public void GetItemNameByItemName()
        {
            ITable t = _table;

            var es = t.Get<Element>(new string[] { TableExtension.ItemNameAttribute }, Condition.WithItemName("e0"));
            AssertIdentities(es);

            es = t.Get<Element>(new string[] { TableExtension.ItemNameAttribute }, Condition.WithItemName("e1"));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(new string[] { TableExtension.ItemNameAttribute }, Condition.WithItemName("e0"));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0");

            es = t.Get<Element>(new string[] { TableExtension.ItemNameAttribute }, Condition.WithItemName("e1"));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1");
        }

        [Fact]
        public void GetAttributesByItemName()
        {
            ITable t = _table;

            var es = t.Get<Element>(new string[] { "X" }, Condition.WithItemName("e0"));
            AssertIdentities(es);

            es = t.Get<Element>(new string[] { "X", "XX" }, Condition.WithItemName("e1"));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(new string[] { "X" }, Condition.WithItemName("e0"));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"));

            es = t.Get<Element>(new string[] { "X", "XX" }, Condition.WithItemName("e0"));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"));

            es = t.Get<Element>(new string[] { "X" }, Condition.WithItemName("e1"));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"));

            es = t.Get<Element>(new string[] { "X", "XX" }, Condition.WithItemName("e1"));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"));
        }

        [Fact]
        public void GetByAttributeValueBetween()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithAttributeBetween("X", "A", "B"));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeBetween("X", "A", "C"));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"))));
            t.Set("e1", new Element(Pairs(Pair("X", "B"))));
            t.Set("e2", new Element(Pairs(Pair("X", "C"))));
            t.Set("e3", new Element(Pairs(Pair("X", "D"))));

            es = t.Get<Element>(Condition.WithAttributeBetween("X", "A", "B"));
            AssertIdentities(es, "e0", "e1");

            es = t.Get<Element>(Condition.WithAttributeBetween("X", "A", "C"));
            AssertIdentities(es, "e0", "e1", "e2");

            es = t.Get<Element>(Condition.WithAttributeBetween("X", "B", "C"));
            AssertIdentities(es, "e1", "e2");
        }

        [Fact]
        public void GetByAttributeValueIn()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "B" }));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "C" }));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"))));
            t.Set("e1", new Element(Pairs(Pair("X", "B"))));
            t.Set("e2", new Element(Pairs(Pair("X", "C"))));
            t.Set("e3", new Element(Pairs(Pair("X", "D"))));

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "B" }));
            AssertIdentities(es, "e0", "e1");

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "C", "B" }));
            AssertIdentities(es, "e1", "e2");

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "C" }));
            AssertIdentities(es, "e0", "e2");

            es = t.Get<Element>(Condition.WithAttributeIn("X", new string[] { "A", "C", "B" }));
            AssertIdentities(es, "e0", "e1", "e2");

            Assert.Throws<ArgumentException>(() => t.Get<Element>(Condition.WithAttributeIn("X", new string[] { })).ToArray());
        }

        [Fact]
        public void GetByAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithAttributeValue(Pair("X", "A")));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XX", "AA")));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("X", "A")));
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("X", ValueTest.LessOrEqual, "A"));
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("X", ValueTest.GreaterThan, "A"));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("X", ValueTest.NotEqual, "A"));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XX", "AA")));
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XX", ValueTest.LessOrEqual, "AA"));
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XX", ValueTest.GreaterThan, "AA"));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XX", ValueTest.NotEqual, "AA"));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA")));
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.LessOrEqual, "AAA"));
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.GreaterThan, "AAA"));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.NotEqual, "AAA"));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("X", "A1")));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("X", ValueTest.LessThan,  "A1"));
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XX", "AA1")));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XX", ValueTest.LessThan, "AA1"));
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA1")));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.LessThan, "AAA1"));
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXX", "AAA2")));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.LessThan, "AAA2"));
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.WithAttributeValue("XXX", ValueTest.GreaterOrEqual, "AAA2"));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue(Pair("XXXX", "AAAA")));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue("XXXX", ValueTest.LessThan, "AAAA"));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue("XXXX", ValueTest.GreaterOrEqual, "AAAA"));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeValue("XXXX", ValueTest.NotEqual, "AAAA"));
            AssertIdentities(es);
        }

        [Fact]
        public void GetByAttributeIsNullIsNotNull()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.WithAttributeIsNull("X"));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXXXX"));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XXX", "AAA"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"))));

            es = t.Get<Element>(Condition.WithAttributeIsNull("X"));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("X"));
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XX"));
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertIdentities(es, "e0");

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("XX"));
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXX"));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("XXX"));
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXXX"));
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("XXXX"));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNull("XXXXX"));
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A2"), Pair("XX", "AA1"), Pair("XXXX", "AAAA2"));

            es = t.Get<Element>(Condition.WithAttributeIsNotNull("XXXXX"));
            AssertIdentities(es);
        }

        [Fact]
        public void GetByEmptyGroupCondition()
        {
            ITable t = _table;

            var es = t.Get<Element>();
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(new Condition[] { }));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.And(new Condition[] { }), Condition.And(new Condition[] { })));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>();
            AssertIdentities(es, "e0", "e1", "e2");

            es = t.Get<Element>(Condition.And(new Condition[] { }));
            AssertIdentities(es, "e0", "e1", "e2");

            es = t.Get<Element>(Condition.And(Condition.And(new Condition[] { }), Condition.And(new Condition[] { })));
            AssertIdentities(es, "e0", "e1", "e2");
        }

        [Fact]
        public void GetByAttributeValueAndAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XXX", "AAA"))));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XXX", "AAA"))));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XX", "AA")), Condition.WithAttributeValue(Pair("XXX", "AAA"))));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XX", "AA1"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XXX", "AAA1"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XX", "AA1")), Condition.WithAttributeValue(Pair("XXX", "AAA1"))));
            AssertIdentities(es);
        }

        [Fact]
        public void GetByAttributeValueOrAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XXX", "AAA"))));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A")), Condition.WithAttributeValue(Pair("XXX", "AAA"))));
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("XX", "AA")), Condition.WithAttributeValue(Pair("XXX", "AAA"))));
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XX", "AA1"))));
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XXX", "AAA1"))));
            AssertIdentities(es, "e0", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("XX", "AA1")), Condition.WithAttributeValue(Pair("XXX", "AAA1"))));
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.Or(Condition.WithAttributeValue(Pair("X", "A1")), Condition.WithAttributeValue(Pair("XX", "AA1")), Condition.WithAttributeValue(Pair("XXX", "AAA1"))));
            AssertIdentities(es, "e0", "e1", "e2");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));
        }
        
        [Fact]
        public void GetByAttributeIsNullAndAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("X"), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("XXXXX"), Condition.WithAttributeValue(Pair("X", "A"))));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XXX", "AAA"))));
            t.Set("e1", new Element(Pairs(Pair("XXX", "AAA"), Pair("XXXX", "AAAA"))));
            t.Set("e2", new Element(Pairs(Pair("XX", "AA"), Pair("XXXX", "AAAA"))));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("X"), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("XX", "AA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("X"), Condition.WithAttributeValue(Pair("XXXX", "AAAA"))));
            AssertIdentities(es, "e1", "e2");
            AssertPairs(es, "e1", Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));
            AssertPairs(es, "e2", Pair("XX", "AA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("XX"), Condition.WithAttributeValue(Pair("XXX", "AAA"))));
            AssertIdentities(es, "e0", "e1");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XXX", "AAA"));
            AssertPairs(es, "e1", Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("XXX"), Condition.WithAttributeValue(Pair("X", "A"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNull("XXX"), Condition.WithAttributeValue(Pair("XX", "AA1"))));
            AssertIdentities(es);
        }

        [Fact]
        public void GetByAttributeIsNotNullAndAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XX"), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXXXX"), Condition.WithAttributeValue(Pair("X", "A"))));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e1", new Element(Pairs(Pair("XXX", "AAA"), Pair("XXXX", "AAAA"))));
            t.Set("e2", new Element(Pairs(Pair("XX", "AA"), Pair("XXXX", "AAAA"))));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXXX"), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("XX", "AA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXX"), Condition.WithAttributeValue(Pair("XXXX", "AAAA"))));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("XXX", "AAA"), Pair("XXXX", "AAAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XX"), Condition.WithAttributeValue(Pair("XXX", "AAA"))));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXX"), Condition.WithAttributeValue(Pair("X", "A"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeIsNotNull("XXX"), Condition.WithAttributeValue(Pair("XXXX", "AAAA1"))));
            AssertIdentities(es);
        }

        [Fact]
        public void GetByItemNameAndPAttributeValue()
        {
            ITable t = _table;

            var es = t.Get<Element>(Condition.And(Condition.WithItemName("e0"), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XXX", "AAA")), Condition.WithItemName("e1")));
            AssertIdentities(es);

            t.Set("e0", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"))));
            t.Set("e1", new Element(Pairs(Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"))));
            t.Set("e2", new Element(Pairs(Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"))));

            es = t.Get<Element>(Condition.And(Condition.WithItemName("e0"), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es, "e0");
            AssertPairs(es, "e0", Pair("X", "A"), Pair("XX", "AA"), Pair("XXX", "AAA1"));

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XXX", "AAA")), Condition.WithItemName("e1")));
            AssertIdentities(es, "e1");
            AssertPairs(es, "e1", Pair("X", "A"), Pair("XX", "AA1"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithItemName("e2"), Condition.WithAttributeValue(Pair("XX", "AA"))));
            AssertIdentities(es, "e2");
            AssertPairs(es, "e2", Pair("X", "A1"), Pair("XX", "AA"), Pair("XXX", "AAA"));

            es = t.Get<Element>(Condition.And(Condition.WithItemName("e1"), Condition.WithAttributeValue(Pair("X", "A1"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithItemName("e0"), Condition.WithAttributeValue(Pair("XX", "AA1"))));
            AssertIdentities(es);

            es = t.Get<Element>(Condition.And(Condition.WithAttributeValue(Pair("XXX", "AAA1")), Condition.WithItemName("e2")));
            AssertIdentities(es);
        }

        private static Dictionary<string, Element> ToDictionary(IEnumerable<KeyValuePair<string, Element>> elements)
        {
            var d = new Dictionary<string, Element>();

            foreach (KeyValuePair<string, Element> pair in elements)
            {
                if (!d.ContainsKey(pair.Key))
                {
                    d.Add(pair.Key, pair.Value);
                }
            }

            return d;
        }

        private void AssertIdentities(IEnumerable<KeyValuePair<string, Element>> elements, params string[] ids)
        {
            var d = ToDictionary(elements);

            Assert.Equal(ids.Length, d.Count);

            for (int i = 0; i < ids.Length; i++)
            {
                Assert.True(d.ContainsKey(ids[i]));
                Assert.NotNull(d[ids[i]]);
            }
        }

        private void AssertPairs(IEnumerable<KeyValuePair<string, Element>> elements, string id, params KeyValuePair<string, string>[] ps)
        {
            var d = ToDictionary(elements);

            Assert.True(d.ContainsKey(id));
            Assert.NotNull(d[id]);
            Assert.Equal(ps.Length, d[id].Count);

            for (int i = 0; i < ps.Length; i++)
            {
                Assert.True(d[id].ContainsKey(ps[i].Key));
                Assert.Equal(d[id][ps[i].Key], ps[i].Value);
            }
        }

        private static IEnumerable<KeyValuePair<T, V>> Pairs<T, V>(params KeyValuePair<T, V>[] ps)
        {
            return ps;
        }

        private static KeyValuePair<T, V> Pair<T, V>(T key, V value)
        {
            return new KeyValuePair<T, V>(key, value);
        }
    }
}
