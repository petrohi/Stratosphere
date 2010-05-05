// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Stratosphere.Aws;

namespace Stratosphere.Table.Sdb
{
    public sealed partial class SdbTable : ITable
    {
        private readonly SdbService _service;
        private readonly string _domainName;
        private readonly int? _selectLimit;

        private class ListDomainsBuilder : AmazonActionBuilder
        {
            public ListDomainsBuilder() :
                base("ListDomains") { }
        }

        public static IEnumerable<SdbTable> ListTables(string serviceId, string serviceSecret)
        {
            return ListTables(serviceId, serviceSecret, null);
        }

        public static IEnumerable<SdbTable> ListTables(string serviceId, string serviceSecret, int? selectLimit)
        {
            SdbService service = new SdbService(serviceId, serviceSecret);
            XElement responseElement = service.Execute(new ListDomainsBuilder());

            foreach (string domainName in responseElement.Descendants(Sdb + "DomainName").Select(n => n.Value))
            {
                yield return new SdbTable(service, domainName, selectLimit);
            }
        }

        public static SdbTable Create(string serviceId, string serviceSecret, string domainName)
        {
            return Create(serviceId, serviceSecret, domainName, false, null);
        }

        private class CreateDomainBuilder : DomainActionBuilder
        {
            public CreateDomainBuilder(string domainName) :
                base("CreateDomain", domainName) { }
        }

        public static SdbTable Create(string serviceId, string serviceSecret, string domainName, bool ensureDomain, int? selectLimit)
        {
            SdbService service = new SdbService(serviceId, serviceSecret);
            
            if (ensureDomain)
            {
                XElement responseElement = service.Execute(new ListDomainsBuilder());

                if (!responseElement.Descendants(Sdb + "DomainName").Select(n => n.Value).Contains(domainName))
                {
                    service.Execute(new CreateDomainBuilder(domainName));
                }
            }

            return new SdbTable(service, domainName, selectLimit);
        }

        public static void ResetBoxUsage() { SdbService.ResetBoxUsage(); }

        public static double CurrentBoxUsage { get { return SdbService.CurrentBoxUsage; } }

        private class DomainMetadataBuilder : DomainActionBuilder
        {
            public DomainMetadataBuilder(string domainName) :
                base("DomainMetadata", domainName) { }
        }

        public void GetInfo(out long itemCount, out long sizeBytes)
        {
            XElement responseElement = _service.Execute(new DomainMetadataBuilder(_domainName));

            itemCount = long.Parse(responseElement.Descendants(Sdb + "ItemCount").First().Value);
            sizeBytes =
                long.Parse(responseElement.Descendants(Sdb + "ItemNamesSizeBytes").First().Value) +
                long.Parse(responseElement.Descendants(Sdb + "AttributeNamesSizeBytes").First().Value) +
                long.Parse(responseElement.Descendants(Sdb + "AttributeValuesSizeBytes").First().Value);
        }

        private class DomainActionBuilder : AmazonActionBuilder
        {
            public DomainActionBuilder(string action, string domainName)
                : base(action)
            {
                Add("DomainName", domainName);
            }
        }

        private class ItemBuilder : DomainActionBuilder
        {
            public ItemBuilder(string action, string domainName, string name)
                : base(action, domainName)
            {
                Add("ItemName", name);
            }
        }

        private class ExpectedItemBuilder : ItemBuilder, IExpectedWriter
        {
            private int _expectedIndex;

            public ExpectedItemBuilder(string action, string domainName, string name)
                : base(action, domainName, name) { }

            public void WhenExpected(string name, string value)
            {
                Add(string.Format("Expected.{0}.Value", _expectedIndex), value);
                Add(string.Format("Expected.{0}.Exists", _expectedIndex), TrueString);
                WhenExpected(name);
            }

            public void WhenExpectedNotExists(string name)
            {
                Add(string.Format("Expected.{0}.Exists", _expectedIndex), FalseString);
                WhenExpected(name);
            }

            private void WhenExpected(string name)
            {
                Add(string.Format("Expected.{0}.Name", _expectedIndex), name);

                _expectedIndex++;
            }
        }

        private class PutItemBuilder : ExpectedItemBuilder, IPutWriter
        {
            private int _attributeIndex;

            public PutItemBuilder(string domainName, string name)
                : base("PutAttributes", domainName, name) { }

            public void ReplaceAttribute(string name, string value)
            {
                Add(string.Format("Attribute.{0}.Replace", _attributeIndex), TrueString);
                AddAttribute(name, value);
            }

            public void AddAttribute(string name, string value)
            {
                Add(string.Format("Attribute.{0}.Name", _attributeIndex), name);
                Add(string.Format("Attribute.{0}.Value", _attributeIndex), value);

                _attributeIndex++;
            }
        }

        private class DeleteItemBuilder : ExpectedItemBuilder, IDeleteWriter
        {
            private int _attributeIndex;

            public DeleteItemBuilder(string domainName, string name)
                : base("DeleteAttributes", domainName, name) { }

            public void DeleteItem() { }

            public void DeleteAttribute(string name)
            {
                Add(string.Format("Attribute.{0}.Name", _attributeIndex), name);

                _attributeIndex++;
            }

            public void DeleteAttribute(string name, string value)
            {
                Add(string.Format("Attribute.{0}.Value", _attributeIndex), value);
                DeleteAttribute(name);
            }
        }

        public void Put(string name, Action<IPutWriter> action)
        {
            if (!string.IsNullOrEmpty(name))
            {
                PutItemBuilder builder = new PutItemBuilder(_domainName, name);

                if (action != null)
                {
                    action(builder);
                }

                _service.Execute(builder);
            }
        }

        public void Delete(string name, Action<IDeleteWriter> action)
        {
            if (!string.IsNullOrEmpty(name))
            {
                DeleteItemBuilder builder = new DeleteItemBuilder(_domainName, name);

                if (action != null)
                {
                    action(builder);
                }

                _service.Execute(builder);
            }
        }

        private class DeleteDomainBuilder : DomainActionBuilder
        {
            public DeleteDomainBuilder(string domainName) :
                base("DeleteDomain", domainName) { }
        }

        public void Delete()
        {
            _service.Execute(new DeleteDomainBuilder(_domainName));
        }

        private class Reader : IReader
        {
            private readonly IEnumerable<XElement> _items;

            private IEnumerator<XElement> _itemEnumerator;
            private IEnumerator<XElement> _attributeEnumerator;
            private IEnumerator<XElement> _valueEnumerator;

            public Reader(IEnumerable<XElement> items)
            {
                _items = items;
            }

            private bool MoveNextValue()
            {
                if (_valueEnumerator != null)
                {
                    return _valueEnumerator.MoveNext();
                }

                return false;
            }

            private bool MoveNextAttribute()
            {
                if (_attributeEnumerator != null)
                {
                    while (_attributeEnumerator.MoveNext())
                    {
                        _valueEnumerator = _attributeEnumerator.Current.Elements(Sdb + "Value").GetEnumerator();

                        if (MoveNextValue())
                        {
                            return true;
                        }
                    }
                }

                return false;
            }

            private bool MoveNextItem()
            {
                if (_itemEnumerator != null)
                {
                    while (_itemEnumerator.MoveNext())
                    {
                        _attributeEnumerator = _itemEnumerator.Current.Elements(Sdb + "Attribute").GetEnumerator();

                        if (MoveNextAttribute())
                        {
                            return true;
                        }
                    }
                }

                return false;
            }

            public ReadingState Read()
            {
                if (_itemEnumerator == null)
                {
                    _itemEnumerator = _items.GetEnumerator();
                }

                if (MoveNextValue())
                {
                    return ReadingState.Value;
                }
                else if (MoveNextAttribute())
                {
                    return ReadingState.BeginAttribute;
                }
                else if (MoveNextItem())
                {
                    return ReadingState.BeginItem;
                }

                return ReadingState.End;
            }

            public string ItemName
            {
                get
                {
                    return _itemEnumerator.Current.Element(Sdb + "Name").Value;
                }
            }

            public string AttributeName
            {
                get
                {
                    if (_attributeEnumerator != null)
                    {
                        return _attributeEnumerator.Current.Element(Sdb + "Name").Value;
                    }

                    return string.Empty;
                }
            }

            public string AttributeValue
            {
                get
                {
                    if (_valueEnumerator != null)
                    {
                        return _valueEnumerator.Current.Value;
                    }

                    return string.Empty;
                }
            }

            public void Dispose()
            {
                if (_valueEnumerator != null)
                {
                    _valueEnumerator.Dispose();
                }

                if (_attributeEnumerator != null)
                {
                    _attributeEnumerator.Dispose();
                }

                if (_itemEnumerator != null)
                {
                    _itemEnumerator.Dispose();
                }
            }
        }

        public IReader Select(IEnumerable<string> attributeNames, Condition condition)
        {
            return new Reader(SelectPairs(attributeNames, condition));
        }

        public long SelectCount(Condition condition)
        {
            using (IReader reader = Select(new string[] { "count(*)" }, condition))
            {
                if (reader.Read() != ReadingState.End &&
                    reader.AttributeName == "Count")
                {
                    return long.Parse(reader.AttributeValue);
                }
            }

            return 0;
        }

        private class SelectBuilder : AmazonActionBuilder
        {
            public SelectBuilder(string domainName, IEnumerable<string> attributeNames, Condition condition, string nextToken, int? selectLimit)
                : base("Select")
            {
                StringBuilder builder = new StringBuilder("select ");
                builder.Append(BuildSelectList(attributeNames.Distinct()));
                builder.Append(" from ");
                builder.Append(domainName);
                builder.Append(BuildWhereClause(condition));

                if (selectLimit.HasValue)
                {
                    builder.Append(" limit ");
                    builder.Append(selectLimit.Value);
                }

                Add("SelectExpression", builder.ToString());

                if (!string.IsNullOrEmpty(nextToken))
                {
                    Add("NextToken", nextToken);
                }
            }

            private static string ContinueBuildWhereClause(Condition condition)
            {
                ItemNameCondition itemNameCondition;
                AttributeValueCondition attributeValueCondition;
                AttributeIsNullCondition attributeIsNullCondition;
                AttributeIsNotNullCondition attributeIsNotNullCondition;
                GroupCondition groupCondition;

                if ((itemNameCondition = condition as ItemNameCondition) != null)
                {
                    return string.Format("itemName()='{0}'", (string)itemNameCondition.ItemName);
                }
                else if ((attributeValueCondition = condition as AttributeValueCondition) != null)
                {
                    return string.Format("{0}='{1}'", attributeValueCondition.Pair.Key, attributeValueCondition.Pair.Value);
                }
                else if ((attributeIsNullCondition = condition as AttributeIsNullCondition) != null)
                {
                    return string.Format("{0} is null", attributeIsNullCondition.Name);
                }
                else if ((attributeIsNotNullCondition = condition as AttributeIsNotNullCondition) != null)
                {
                    return string.Format("{0} is not null", attributeIsNotNullCondition.Name);
                }
                else if ((groupCondition = condition as GroupCondition) != null)
                {
                    if (groupCondition.IsEmpty)
                    {
                        return string.Empty;
                    }
                    else
                    {
                        StringBuilder builder = new StringBuilder();

                        foreach (Condition inner in groupCondition.Group)
                        {
                            string innerExpression = ContinueBuildWhereClause(inner);

                            if (!string.IsNullOrEmpty(innerExpression))
                            {
                                if (builder.Length != 0)
                                {
                                    if (groupCondition.Operator == GroupConditionOperator.And)
                                    {
                                        builder.Append(" and ");
                                    }
                                    else if (groupCondition.Operator == GroupConditionOperator.Or)
                                    {
                                        builder.Append(" or ");
                                    }
                                    else
                                    {
                                        throw new ArgumentException("Group operator is not supported");
                                    }
                                }
                                else
                                {
                                    builder.Append("(");
                                }

                                builder.Append(innerExpression);
                            }
                        }

                        if (builder.Length != 0)
                        {
                            builder.Append(")");
                            return builder.ToString();
                        }

                        return string.Empty;
                    }
                }

                throw new ArgumentException("Condition is not supported");
            }

            private static string BuildWhereClause(Condition condition)
            {
                if (condition != null)
                {
                    string expression = ContinueBuildWhereClause(condition);

                    if (!string.IsNullOrEmpty(expression))
                    {
                        return string.Format(" where {0}", expression);
                    }
                }

                return string.Empty;
            }

            private static string BuildSelectList(IEnumerable<string> attributeKeys)
            {
                StringBuilder builder = new StringBuilder();

                foreach (string key in attributeKeys)
                {
                    if (builder.Length != 0)
                    {
                        builder.Append(',');
                    }

                    builder.Append(key);
                }

                if (builder.Length == 0)
                {
                    builder.Append('*');
                }

                return builder.ToString();
            }
        }

        private IEnumerable<XElement> SelectPairs(IEnumerable<string> attributeNames, Condition condition)
        {
            string nextToken = null;

            do
            {
                XElement responseElement = _service.Execute(
                    new SelectBuilder(_domainName, attributeNames, condition, nextToken, _selectLimit));

                foreach (XElement itemElement in responseElement.Descendants(Sdb + "Item"))
                {
                    yield return itemElement;
                }

                XElement nextTokenElement = responseElement.Descendants(Sdb + "NextToken").FirstOrDefault();

                if (nextTokenElement == null)
                {
                    nextToken = null;
                }
                else
                {
                    nextToken = nextTokenElement.Value;
                }
            }
            while (nextToken != null);
        }

        public string Name { get { return _domainName; } }

        private SdbTable(SdbService service, string domainName, int? selectLimit)
        {
            _service = service;
            _domainName = domainName;
            _selectLimit = selectLimit;
        }

        private static readonly XNamespace Sdb = XNamespace.Get("http://sdb.amazonaws.com/doc/2009-04-15/");
    }
}
