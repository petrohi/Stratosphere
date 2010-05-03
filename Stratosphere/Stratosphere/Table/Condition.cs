// Copyright (c) 2010, 7Clouds. All rights reserved.

using System.Collections.Generic;

namespace Stratosphere.Table
{
    public abstract class Condition
    {
        public static Condition WithItemName(string itemName) { return new ItemNameCondition(itemName); }
        public static Condition WithAttributeValue(string name, string value) { return new AttributeValueCondition(new KeyValuePair<string, string>(name, value)); }
        public static Condition WithAttributeValue(KeyValuePair<string, string> pair) { return new AttributeValueCondition(pair); }
        public static Condition WithAttributeIsNull(string key) { return new AttributeIsNullCondition(key); }
        public static Condition WithAttributeIsNotNull(string key) { return new AttributeIsNotNullCondition(key); }
        public static Condition And(IEnumerable<Condition> group) { return new GroupCondition(GroupConditionOperator.And, group); }
        public static Condition Or(IEnumerable<Condition> group) { return new GroupCondition(GroupConditionOperator.Or, group); }

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

    public sealed class AttributeValueCondition : Condition
    {
        public AttributeValueCondition(KeyValuePair<string, string> pair)
        {
            _pair = pair;
        }

        public KeyValuePair<string, string> Pair { get { return _pair; } }

        private readonly KeyValuePair<string, string> _pair;
    }

    public sealed class AttributeIsNullCondition : Condition
    {
        public AttributeIsNullCondition(string name)
        {
            _name = name;
        }

        public string Name { get { return _name; } }

        private readonly string _name;
    }

    public sealed class AttributeIsNotNullCondition : Condition
    {
        public AttributeIsNotNullCondition(string name)
        {
            _name = name;
        }

        public string Name { get { return _name; } }

        private readonly string _name;
    }

    public enum GroupConditionOperator
    {
        And,
        Or
    }

    public sealed class GroupCondition : Condition
    {
        public GroupCondition(GroupConditionOperator op, IEnumerable<Condition> group)
        {
            _op = op;
            _group = new List<Condition>(group);
        }

        public GroupConditionOperator Operator { get { return _op; } }
        public IEnumerable<Condition> Group { get { return _group; } }

        public bool IsEmpty { get { return (_group.Count == 0); } }

        private readonly GroupConditionOperator _op;
        private readonly List<Condition> _group;
    }
}