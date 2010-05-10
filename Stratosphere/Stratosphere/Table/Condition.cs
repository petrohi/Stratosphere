// Copyright (c) 2010 7Clouds

using System.Collections.Generic;

namespace Stratosphere.Table
{
    public enum ValueTest
    {
        Equal = 0,
        NotEqual,
        LessThan,
        GreaterThan,
        LessOrEqual,
        GreaterOrEqual,
        Like,
        NotLike
    }

    public abstract class Condition
    {
        public static Condition WithItemName(string itemName) { return new ItemNameCondition(itemName); }
        public static Condition WithAttributeValue(string name, ValueTest test, string value) { return new AttributeValueCondition(new KeyValuePair<string, string>(name, value), test); }
        public static Condition WithAttributeValue(string name, string value) { return new AttributeValueCondition(new KeyValuePair<string, string>(name, value), ValueTest.Equal); }
        public static Condition WithAttributeValue(KeyValuePair<string, string> pair) { return new AttributeValueCondition(pair, ValueTest.Equal); }
        public static Condition WithAttributeBetween(string name, string lowerValue, string upperValue) { return new AttributeValueBetweenCondition(name, lowerValue, upperValue); }
        public static Condition WithAttributeIn(string name, IEnumerable<string> values) { return new AttributeValueInCondition(name, values); }
        public static Condition WithAttributeIsNull(string key) { return new AttributeIsNullCondition(key); }
        public static Condition WithAttributeIsNotNull(string key) { return new AttributeIsNotNullCondition(key); }
        public static Condition EveryAttribute(AttributeCondition condition) { return new EveryAttributeCondition(condition); }
        public static Condition And(IEnumerable<Condition> group) { return new GroupCondition(GroupOperator.And, group); }
        public static Condition Or(IEnumerable<Condition> group) { return new GroupCondition(GroupOperator.Or, group); }

        public static Condition And(Condition c0, Condition c1)
        {
            return And(new Condition[] { c0, c1 });
        }

        public static Condition And(Condition c0, Condition c1, Condition c2)
        {
            return And(new Condition[] { c0, c1, c2 });
        }

        public static Condition And(Condition c0, Condition c1, Condition c2, Condition c3)
        {
            return And(new Condition[] { c0, c1, c2, c3 });
        }

        public static Condition Or(Condition c0, Condition c1)
        {
            return Or(new Condition[] { c0, c1 });
        }

        public static Condition Or(Condition c0, Condition c1, Condition c2)
        {
            return Or(new Condition[] { c0, c1, c2 });
        }

        public static Condition Or(Condition c0, Condition c1, Condition c2, Condition c3)
        {
            return Or(new Condition[] { c0, c1, c2, c3 });
        }
    }

    public sealed class ItemNameCondition : Condition
    {
        public ItemNameCondition(string itemName)
        {
            _itemName = itemName;
        }

        public string ItemName { get { return _itemName; } }

        private readonly string _itemName;
    }

    public sealed class EveryAttributeCondition : Condition
    {
        public EveryAttributeCondition(AttributeCondition condition)
        {
            _condition = condition;
        }

        public AttributeCondition Condition { get { return _condition; } }

        private readonly AttributeCondition _condition;
    }

    public abstract class AttributeCondition : Condition { }

    public sealed class AttributeValueCondition : AttributeCondition
    {
        public AttributeValueCondition(KeyValuePair<string, string> pair, ValueTest test)
        {
            _pair = pair;
            _test = test;
        }

        public KeyValuePair<string, string> Pair { get { return _pair; } }
        public ValueTest Test { get { return _test; } }

        private readonly KeyValuePair<string, string> _pair;
        private readonly ValueTest _test;
    }

    public sealed class AttributeIsNullCondition : AttributeCondition
    {
        public AttributeIsNullCondition(string name)
        {
            _name = name;
        }

        public string Name { get { return _name; } }

        private readonly string _name;
    }

    public sealed class AttributeIsNotNullCondition : AttributeCondition
    {
        public AttributeIsNotNullCondition(string name)
        {
            _name = name;
        }

        public string Name { get { return _name; } }

        private readonly string _name;
    }

    public sealed class AttributeValueBetweenCondition : AttributeCondition
    {
        public AttributeValueBetweenCondition(string name, string lowerValue, string upperValue)
        {
            _name = name;
            _lowerValue = lowerValue;
            _upperValue = upperValue;
        }

        public string Name { get { return _name; } }
        public string LowerValue { get { return _lowerValue; } }
        public string UpperValue { get { return _upperValue; } }

        private readonly string _name;
        private readonly string _lowerValue;
        private readonly string _upperValue;
    }

    public sealed class AttributeValueInCondition : AttributeCondition
    {
        public AttributeValueInCondition(string name, IEnumerable<string> values)
        {
            _name = name;
            _values = values;
        }

        public string Name { get { return _name; } }
        public IEnumerable<string> Values { get { return _values; } }

        private readonly string _name;
        private readonly IEnumerable<string> _values;
    }

    public enum GroupOperator
    {
        And,
        Or
    }

    public sealed class GroupCondition : Condition
    {
        public GroupCondition(GroupOperator op, IEnumerable<Condition> group)
        {
            _op = op;
            _group = new List<Condition>(group);
        }

        public GroupOperator Operator { get { return _op; } }
        public IEnumerable<Condition> Group { get { return _group; } }

        public bool IsEmpty { get { return (_group.Count == 0); } }

        private readonly GroupOperator _op;
        private readonly List<Condition> _group;
    }
}