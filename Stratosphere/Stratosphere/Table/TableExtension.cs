// Copyright (c) 2010 7Clouds

using System.Collections.Generic;
using System.Linq;

namespace Stratosphere.Table
{
    public static class TableExtension
    {
        public static void Erase(this ITable table)
        {
            string[] itemNames = table.Select().ToArray();

            foreach (string itemName in itemNames)
            {
                table.Delete(itemName, w => w.DeleteItem());
            }
        }

        public static void Set<T>(this ITable table, string itemName, T itemData)
            where T : IEnumerable<KeyValuePair<string, string>>
        {
            Set(table, itemName, itemData, false);
        }

        public static void Set<T>(this ITable table, string itemName, T itemData, bool deleteEmpty)
            where T : IEnumerable<KeyValuePair<string, string>>
        {
            if (!string.IsNullOrEmpty(itemName))
            {
                var putAttributes = itemData.Where(pair => !string.IsNullOrEmpty(pair.Key)).ToArray();

                if (putAttributes.Length != 0)
                {
                    table.Put(itemName, w =>
                    {
                        foreach (var attribute in putAttributes)
                        {
                            w.ReplaceAttribute(attribute.Key, attribute.Value != null ? attribute.Value : string.Empty);
                        }
                    });
                }

                if (deleteEmpty)
                {
                    string[] deleteAttributeNames = itemData.Where(
                        pair => !string.IsNullOrEmpty(pair.Key) && string.IsNullOrEmpty(pair.Value)).Select(pair => pair.Key).ToArray();

                    if (deleteAttributeNames.Length != 0)
                    {
                        table.Delete(itemName, w =>
                        {
                            foreach (string name in deleteAttributeNames)
                            {
                                w.DeleteAttribute(name);
                            }
                        });
                    }
                }
            }
        }

        public static void Set<T>(this ITable table, IEnumerable<KeyValuePair<string, T>> items)
            where T : IEnumerable<KeyValuePair<string, string>>
        {
            Set(table, items, false);
        }

        public static void Set<T>(this ITable table, IEnumerable<KeyValuePair<string, T>> items, bool deleteEmpty)
            where T : IEnumerable<KeyValuePair<string, string>>
        {
            var batchPutItems = items.Where(item => !string.IsNullOrEmpty(item.Key)).Select(item =>
                new KeyValuePair<string, KeyValuePair<string, string>[]>(item.Key, item.Value.Where(pair => 
                    !string.IsNullOrEmpty(pair.Key)).ToArray())).Where(item => item.Value.Length != 0).ToArray();

            if (batchPutItems.Length != 0)
            {
                table.BatchPut(w =>
                {
                    foreach (var item in batchPutItems)
                    {
                        foreach (var attribute in item.Value)
                        {
                            w.ReplaceAttribute(item.Key, attribute.Key, attribute.Value != null ? attribute.Value : string.Empty);
                        }
                    }
                });
            }

            if (deleteEmpty)
            {
                var deleteItems = items.Where(item => !string.IsNullOrEmpty(item.Key)).Select(item =>
                     new KeyValuePair<string, string[]>(item.Key, item.Value.Where(pair => 
                         !string.IsNullOrEmpty(pair.Key) && string.IsNullOrEmpty(pair.Value)).Select(pair => pair.Key).ToArray())).Where(item => 
                             item.Value.Length != 0).ToArray();

                if (deleteItems.Length != 0)
                {
                    foreach (var item in deleteItems)
                    {
                        if (item.Value.Length != 0)
                        {
                            table.Delete(item.Key, w =>
                            {
                                foreach (var name in item.Value)
                                {
                                    w.DeleteAttribute(name);
                                }
                            });
                        }
                    }
                }
            }
        }

        public static IEnumerable<KeyValuePair<string, T>> Get<T>(this ITable table)
            where T : IDictionary<string, string>, new()
        {
            return Get<T>(table, null);
        }

        public static IEnumerable<KeyValuePair<string, T>> Get<T>(this ITable table, Condition condition)
            where T : IDictionary<string, string>, new()
        {
            return Get<T>(table, new string[] { }, condition);
        }

        public static IEnumerable<KeyValuePair<string, T>> Get<T>(this ITable table, IEnumerable<string> attributeNames, Condition condition)
            where T : IDictionary<string, string>, new()
        {
            return Get<T>(table, attributeNames, condition, null);
        }

        public static IEnumerable<KeyValuePair<string, T>> Get<T>(this ITable table, IEnumerable<string> attributeNames, Condition condition, bool? withConsistency)
            where T : IDictionary<string, string>, new()
        {
            return Get<T>(table, attributeNames, condition, withConsistency, null);
        }

        public static IEnumerable<KeyValuePair<string, T>> Get<T>(this ITable table, IEnumerable<string> attributeNames, Condition condition, bool? withConsistency, int? selectLimit)
            where T : IDictionary<string, string>, new()
        {
            using (IReader reader = table.Select(attributeNames, condition, withConsistency, selectLimit))
            {
                T itemData = default(T);
                string itemName = null;

                while (reader.Read())
                {
                    if (reader.Position == ReaderPosition.Item)
                    {
                        if (itemName != null)
                        {
                            if (itemData.Count != 0)
                            {
                                yield return new KeyValuePair<string, T>(itemName, itemData);
                            }

                            itemName = null;
                            itemData = default(T);
                        }

                        itemName = reader.ItemName;
                        itemData = new T();
                    }

                    if (!string.IsNullOrEmpty(reader.AttributeValue))
                    {
                        itemData[reader.AttributeName] = reader.AttributeValue;
                    }
                }

                if (itemName != null && itemData.Count != 0)
                {
                    yield return new KeyValuePair<string, T>(itemName, itemData);
                }
            };
        }

        public static IEnumerable<KeyValuePair<string, string>> Select(this ITable table, string attributeName)
        {
            return Select(table, attributeName, null);
        }

        public static IEnumerable<KeyValuePair<string, string>> Select(this ITable table, string attributeName, Condition condition)
        {
            return Select(table, attributeName, condition, null);
        }

        public static IEnumerable<KeyValuePair<string, string>> Select(this ITable table, string attributeName, Condition condition, bool? withConsistency)
        {
            return Select(table, attributeName, condition, withConsistency, null);
        }

        public static IEnumerable<KeyValuePair<string, string>> Select(this ITable table, string attributeName, Condition condition, bool? withConsistency, int? selectLimit)
        {
            using (IReader reader = table.Select(new string[] { attributeName }, condition, withConsistency, selectLimit))
            {
                while (reader.Read())
                {
                    if (reader.Position != ReaderPosition.EmptyItem)
                    {
                        yield return new KeyValuePair<string, string>(reader.ItemName, reader.AttributeValue);
                    }
                }
            }
        }

        public static IReader Select(this ITable table, IEnumerable<string> attributeNames, Condition condition)
        {
            return table.Select(attributeNames, condition, null);
        }

        public static IReader Select(this ITable table, IEnumerable<string> attributeNames, Condition condition, bool? withConsistency)
        {
            return table.Select(attributeNames, condition, withConsistency, null);
        }

        public static IEnumerable<string> Select(this ITable table)
        {
            return Select(table, (Condition)null);
        }

        public static IEnumerable<string> Select(this ITable table, Condition condition)
        {
            return Select(table, condition, null);
        }

        public static IEnumerable<string> Select(this ITable table, Condition condition, bool? withConsistency)
        {
            return Select(table, condition, withConsistency, null);
        }

        public static IEnumerable<string> Select(this ITable table, Condition condition, bool? withConsistency, int? selectLimit)
        {
            using (IReader reader = table.Select(new string[] { ItemNameAttribute }, condition, withConsistency, selectLimit))
            {
                while (reader.Read())
                {
                    yield return reader.ItemName;
                }
            }
        }

        public static long SelectCount(this ITable table)
        {
            return SelectCount(table, null);
        }

        public static long SelectCount(this ITable table, Condition condition)
        {
            return SelectCount(table, condition, null);
        }

        public static long SelectCount(this ITable table, Condition condition, bool? withConsistency)
        {
            using (IReader reader = table.Select(new string[] { CountAttribute }, condition, withConsistency, null))
            {
                if (reader.Read())
                {
                    return long.Parse(reader.AttributeValue);
                }
            }

            return 0;
        }

        public const string ItemNameAttribute = "itemName()";
        public const string CountAttribute = "count(*)";
    }
}
