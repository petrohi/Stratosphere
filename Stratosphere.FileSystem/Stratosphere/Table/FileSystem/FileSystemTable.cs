// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;

namespace Stratosphere.Table.FileSystem
{
    public sealed partial class FileSystemTable : ITable
    {
        public static FileSystemTable Create(string fileName)
        {
            return new FileSystemTable(fileName);
        }

        public void Delete()
        {
            lock (_guard)
            {
                if (File.Exists(_fileName))
                {
                    File.Delete(_fileName);
                }
            }
        }

        private FileSystemTable(string fileName)
        {
            _fileName = fileName;
        }

        private const string ConnectionStringFormat = "Data Source={0}";

        private IDbConnection EnsureConnection()
        {
            IDbConnection connection = new SQLiteConnection();

            lock (_guard)
            {
                bool exists = File.Exists(_fileName);

                connection.ConnectionString = string.Format(ConnectionStringFormat, _fileName);
                connection.Open();

                if (!exists)
                {
                    using (IDbCommand command = connection.CreateCommand())
                    {
                        StringBuilder builder = new StringBuilder();

                        builder.Append("create table item(num integer primary key, name text not null);");
                        builder.Append("create table attribute(inum integer not null, name text not null, value text not null);");
                        builder.Append("create unique index item_uq on item(name);");
                        builder.Append("create unique index attribute_uq on attribute(inum, name, value);");

                        command.CommandText = builder.ToString();
                        command.ExecuteNonQuery();
                    }
                }
            }

            return connection;
        }

        private readonly string _fileName;
        private readonly object _guard = new object();

        private const int MaximumValueLengthBytes = 1024;

        private class ItemBuilder : TransactionBuilder
        {
            private long _number;

            protected long Number { get { return _number; } }

            public ItemBuilder(IDbConnection connection, string name) :
                base(connection)
            {
                AddAction(() =>
                {
                    object scalar;

                    using (IDbCommand command = CreateCommand())
                    {
                        command.CommandText = "select num from item where name=@name";
                        AddParameter(command, "@name", name);

                        scalar = command.ExecuteScalar();
                    }

                    if (scalar != null && scalar != DBNull.Value)
                    {
                        _number = (long)scalar;
                    }
                    else
                    {
                        using (IDbCommand command = CreateCommand())
                        {
                            command.CommandText = "insert into item(name) values(@name);select last_insert_rowid();";
                            AddParameter(command, "@name", name);

                            scalar = command.ExecuteScalar();
                        }

                        _number = (long)scalar;
                    }
                });
            }
        }

        private class ExpectedItemBuilder : ItemBuilder, IExpectedWriter
        {
            public ExpectedItemBuilder(IDbConnection connection, string name) :
                base(connection, name) { }

            public void WhenExpected(string name, string value)
            {
                AddAction(() =>
                {
                    using (IDbCommand command = CreateCommand())
                    {
                        command.CommandText = "select a.inum from attribute a where a.inum=@inum and a.name=@aname and a.value=@avalue";
                        AddParameter(command, "@inum", Number);
                        AddParameter(command, "@aname", name);
                        AddParameter(command, "@avalue", value);

                        if (command.ExecuteScalar() == null)
                        {
                            throw new ExpectationException();
                        }
                    }
                });
            }

            public void WhenExpectedNotExists(string name)
            {
                AddAction(() =>
                {
                    using (IDbCommand command = CreateCommand())
                    {
                        command.CommandText = "select a.inum from attribute a where a.inum=@inum and a.name=@aname";
                        AddParameter(command, "@inum", Number);
                        AddParameter(command, "@aname", name);

                        if (command.ExecuteScalar() != null)
                        {
                            throw new ExpectationException();
                        }
                    }
                });
            }
        }

        private class PutItemBuilder : ExpectedItemBuilder, IPutWriter
        {
            public PutItemBuilder(IDbConnection connection, string name) :
                base(connection, name) { }

            public void ReplaceAttribute(string name, string value)
            {
                AddAction(() =>
                {
                    using (IDbCommand command = CreateCommand())
                    {
                        command.CommandText = "delete from attribute where inum=@inum and name=@name";
                        AddParameter(command, "@inum", Number);
                        AddParameter(command, "@name", name);

                        command.ExecuteNonQuery();
                    }
                });

                AddAttribute(name, value);
            }

            public void AddAttribute(string name, string value)
            {
                AddAction(() =>
                {
                    using (IDbCommand command = CreateCommand())
                    {
                        command.CommandText = "insert or replace into attribute(inum, name, value) values(@inum, @name, @value)";
                        AddParameter(command, "@inum", Number);
                        AddParameter(command, "@name", name);
                        AddParameter(command, "@value", value);

                        command.ExecuteNonQuery();
                    }
                });
            }
        }

        private class DeleteItemBuilder : ExpectedItemBuilder, IDeleteWriter
        {
            public DeleteItemBuilder(IDbConnection connection, string name) :
                base(connection, name) { }

            public void DeleteItem()
            {
                AddAction(() =>
                {
                    using (IDbCommand command = CreateCommand())
                    {
                        command.CommandText = "delete from attribute where inum=@num;delete from item where num=@num";
                        AddParameter(command, "@num", Number);

                        command.ExecuteNonQuery();
                    }
                });
            }

            public void DeleteAttribute(string name)
            {
                AddAction(() =>
                {
                    using (IDbCommand command = CreateCommand())
                    {
                        command.CommandText = "delete from attribute where inum=@inum and name=@name";
                        AddParameter(command, "@inum", Number);
                        AddParameter(command, "@name", name);

                        command.ExecuteNonQuery();
                    }
                });
            }

            public void DeleteAttribute(string name, string value)
            {
                AddAction(() =>
                {
                    using (IDbCommand command = CreateCommand())
                    {
                        command.CommandText = "delete from attribute where inum=@inum and name=@name and value=@value";
                        AddParameter(command, "@inum", Number);
                        AddParameter(command, "@name", name);
                        AddParameter(command, "@value", value);

                        command.ExecuteNonQuery();
                    }
                });
            }
        }

        private class TransactionBuilder : IDisposable
        {
            private IDbConnection _connection;
            private IDbTransaction _transaction;
            private List<Action> _actions = new List<Action>();
            private bool _isCompleted;

            protected static void AddParameter<T>(IDbCommand command, string name, T value)
            {
                IDbDataParameter parameter = command.CreateParameter();
                parameter.ParameterName = name;
                parameter.Value = value;
                command.Parameters.Add(parameter);
            }

            protected IDbCommand CreateCommand()
            {
                IDbCommand command = _connection.CreateCommand();
                command.Transaction = _transaction;
                return command;
            }

            protected void AddAction(Action action)
            {
                _actions.Add(action);
            }

            public TransactionBuilder(IDbConnection connection)
            {
                _connection = connection;
                _transaction = connection.BeginTransaction();
            }

            public void Dispose()
            {
                if (!_isCompleted)
                {
                    _transaction.Rollback();
                }
            }

            public void Complete()
            {
                _isCompleted = true;

                try
                {
                    foreach (Action action in _actions)
                    {
                        action();
                    }

                    _transaction.Commit();
                }
                catch
                {
                    _transaction.Rollback();

                    throw;
                }
            }
        }

        public void Put(string name, Action<IPutWriter> action)
        {
            if (!string.IsNullOrEmpty(name))
            {
                using (IDbConnection connectin = EnsureConnection())
                {
                    using (PutItemBuilder builder = new PutItemBuilder(connectin, name))
                    {
                        action(builder);
                        builder.Complete();
                    }
                }
            }
        }

        public void Delete(string name, Action<IDeleteWriter> action)
        {
            if (!string.IsNullOrEmpty(name))
            {
                using (IDbConnection connection = EnsureConnection())
                {
                    using (DeleteItemBuilder builder = new DeleteItemBuilder(connection, name))
                    {
                        action(builder);
                        builder.Complete();
                    }
                }
            }
        }

        private class Reader : IReader
        {
            private readonly IDbConnection _connection;
            private readonly IDbCommand _command;

            private IDataReader _reader;
            private int _parameterIndex;
            private string _lastItemName;
            private string _lastAttributeName;
            private ReaderPosition _position;

            public Reader(IDbConnection connection, IEnumerable<string> attributeNames, Condition condition)
            {
                _connection = connection;
                _command = _connection.CreateCommand();

                try
                {
                    BuildCommand(_command, attributeNames, condition);
                }
                catch
                {
                    Dispose();

                    throw;
                }
            }

            public bool Read()
            {
                if (_reader == null)
                {
                    _reader = _command.ExecuteReader();
                }

                if (_reader.Read())
                {
                    if (_lastItemName != ItemName)
                    {
                        _lastItemName = ItemName;

                        if (_reader.IsDBNull(1))
                        {
                            _lastAttributeName = null;

                            _position = ReaderPosition.EmptyItem;
                        }
                        else
                        {
                            _lastAttributeName = AttributeName;

                            _position = ReaderPosition.Item;
                        }
                    }
                    else if (_lastAttributeName != AttributeName)
                    {
                        _lastAttributeName = AttributeName;

                        _position = ReaderPosition.Attribute;
                    }
                    else
                    {
                        _position = ReaderPosition.Value;
                    }

                    return true;
                }

                _position = ReaderPosition.None;

                return false;
            }

            public ReaderPosition Position { get { return _position; } }

            public string ItemName
            {
                get
                {
                    if (_reader != null)
                    {
                        return _reader.GetString(0);
                    }

                    return string.Empty;
                }
            }

            public string AttributeName
            {
                get
                {
                    if (_reader != null && !_reader.IsDBNull(1))
                    {
                        return _reader.GetString(1);
                    }

                    return string.Empty;
                }
            }

            public string AttributeValue
            {
                get
                {
                    if (_reader != null && !_reader.IsDBNull(2))
                    {
                        return _reader.GetValue(2).ToString();
                    }

                    return string.Empty;
                }
            }

            public void Dispose()
            {
                if (_reader != null)
                {
                    _reader.Dispose();
                }

                _command.Dispose();
                _connection.Dispose();
            }

            private void BuildCommand(IDbCommand command, IEnumerable<string> attributeNames, Condition condition)
            {
                StringBuilder builder = new StringBuilder("select ");

                string[] attributeNamesArray = attributeNames == null ? new string[] { } : attributeNames.ToArray();
                bool isSelectCount = attributeNamesArray.Length == 1 && attributeNamesArray[0] == TableExtension.CountAttribute;
                bool isSelectItemName = attributeNamesArray.Length == 1 && attributeNamesArray[0] == TableExtension.ItemNameAttribute;

                if (isSelectCount)
                {
                    builder.Append("'Domain', 'Count', count(i.num) from item i");
                }
                else if (isSelectItemName)
                {
                    builder.Append("i.name, null, null from item i");
                }
                else
                {
                    builder.Append("i.name, a.name, a.value from item i left join attribute a on i.num=a.inum");
                }

                builder.Append(" where (select count(*) from attribute a where a.inum=i.num) != 0");

                if (condition != null)
                {
                    string clause = ContinueBuildConditionWhereClause(command, condition);

                    if (!string.IsNullOrEmpty(clause))
                    {
                        builder.Append(" and (");
                        builder.Append(clause);
                        builder.Append(")");
                    }
                }

                if (attributeNamesArray.Length != 0 && !isSelectCount && !isSelectItemName)
                {
                    string clause = ContinueBuildSelectionWhereClause(command, attributeNamesArray);

                    if (!string.IsNullOrEmpty(clause))
                    {
                        builder.Append(" and (");
                        builder.Append(clause);
                        builder.Append(")");
                    }
                }

                if (!isSelectCount)
                {
                    if (isSelectItemName)
                    {
                        builder.Append(" order by i.name");
                    }
                    else
                    {
                        builder.Append(" order by i.name, a.name");
                    }
                }

                command.CommandText = builder.ToString();
            }

            private string FormatParameterName(string format)
            {
                string name = string.Format(format, _parameterIndex);
                _parameterIndex++;
                return name;
            }

            private static string GetValueTestOperator(ValueTest test)
            {
                switch (test)
                {
                    case ValueTest.Equal:
                        return "=";

                    case ValueTest.NotEqual:
                        return "!=";

                    case ValueTest.LessThan:
                        return "<";

                    case ValueTest.GreaterThan:
                        return ">";

                    case ValueTest.LessOrEqual:
                        return "<=";

                    case ValueTest.GreaterOrEqual:
                        return ">=";

                    case ValueTest.Like:
                        return " like ";

                    case ValueTest.NotLike:
                        return " not like ";
                }

                throw new ArgumentException("Value test is not supported");
            }

            private string ContinueBuildAttributeConditionWhereClause(IDbCommand command, AttributeCondition condition, bool everyAttribute)
            {
                AttributeValueCondition valueCondition;
                AttributeIsNullCondition isNullCondition;
                AttributeIsNotNullCondition isNotNullCondition;
                AttributeValueBetweenCondition valueBetweenCondition;
                AttributeValueInCondition valueInCondition;

                string nameParameterName = FormatParameterName("@aname{0}");
                AddParameter(command, nameParameterName, condition.Name);

                if ((valueCondition = condition as AttributeValueCondition) != null)
                {
                    string valueParameterName = FormatParameterName("@avalue{0}");
                    AddParameter(command, valueParameterName, valueCondition.Value);

                    return string.Format("i.num in (select a.inum from attribute a where a.name={0} and a.value{1}{2})",
                        nameParameterName, GetValueTestOperator(valueCondition.Test), valueParameterName);
                }
                else if ((isNullCondition = condition as AttributeIsNullCondition) != null)
                {
                    return string.Format("i.num not in (select a.inum from attribute a where a.name={0})",
                        nameParameterName);
                }
                else if ((isNotNullCondition = condition as AttributeIsNotNullCondition) != null)
                {
                    return string.Format("i.num in (select a.inum from attribute a where a.name={0})",
                        nameParameterName);
                }
                else if ((valueBetweenCondition = condition as AttributeValueBetweenCondition) != null)
                {
                    string lowerValueParameterName = FormatParameterName("@avalue{0}");
                    string upperValueParameterName = FormatParameterName("@avalue{0}");

                    AddParameter(command, lowerValueParameterName, valueBetweenCondition.LowerValue);
                    AddParameter(command, upperValueParameterName, valueBetweenCondition.UpperValue);

                    return string.Format("i.num in (select a.inum from attribute a where a.name={0} and a.value between {1} and {2})",
                        nameParameterName, lowerValueParameterName, upperValueParameterName);
                }
                else if ((valueInCondition = condition as AttributeValueInCondition) != null)
                {
                    StringBuilder builder = new StringBuilder();
                    builder.AppendFormat("i.num in (select a.inum from attribute a where a.name={0} and a.value in(", nameParameterName);

                    string[] values = valueInCondition.Values.ToArray();

                    if (values.Length == 0)
                    {
                        throw new ArgumentException("Value in condition values are empty");
                    }

                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i != 0)
                        {
                            builder.Append(",");
                        }

                        string valueParameterName = FormatParameterName("@avalue{0}");
                        AddParameter(command, valueParameterName, values[i]);

                        builder.AppendFormat(valueParameterName);
                    }

                    builder.Append("))");
                    return builder.ToString();
                }

                throw new ArgumentException("Condition is not supported");
            }

            private string ContinueBuildSelectionWhereClause(IDbCommand command, string[] attributeNames)
            {
                if (attributeNames.Length != 0)
                {
                    StringBuilder builder = new StringBuilder("a.name in (");

                    for (int i = 0; i < attributeNames.Length; i++)
                    {
                        if (i != 0)
                        {
                            builder.Append(",");
                        }

                        string parameterName = FormatParameterName("@aname{0}");
                        AddParameter(command, parameterName, attributeNames[i]);

                        builder.AppendFormat(parameterName);
                    }

                    builder.Append(")");

                    return builder.ToString();
                }

                return string.Empty;
            }

            private string ContinueBuildConditionWhereClause(IDbCommand command, Condition condition)
            {
                ItemNameCondition identityCondition;
                AttributeCondition attributeCondition;
                EveryAttributeCondition everyAttributeCondition;
                GroupCondition groupCondition;

                if ((identityCondition = condition as ItemNameCondition) != null)
                {
                    string parameterName = FormatParameterName("@iname{0}");
                    AddParameter(command, parameterName, identityCondition.ItemName.ToString());

                    return string.Format("i.name={0}", parameterName);
                }
                else if ((attributeCondition = condition as AttributeCondition) != null)
                {
                    return ContinueBuildAttributeConditionWhereClause(command, attributeCondition, false);
                }
                else if ((everyAttributeCondition = condition as EveryAttributeCondition) != null)
                {
                    return ContinueBuildAttributeConditionWhereClause(command, everyAttributeCondition.Condition, true);
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
                            string innerExpression = ContinueBuildConditionWhereClause(command, inner);

                            if (!string.IsNullOrEmpty(innerExpression))
                            {
                                if (builder.Length != 0)
                                {
                                    if (groupCondition.Operator == GroupOperator.And)
                                    {
                                        builder.Append(" and ");
                                    }
                                    else if (groupCondition.Operator == GroupOperator.Or)
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

            private static void AddParameter<T>(IDbCommand command, string name, T value)
            {
                IDbDataParameter parameter = command.CreateParameter();
                parameter.ParameterName = name;
                parameter.Value = value;
                command.Parameters.Add(parameter);
            }
        }

        public IReader Select(IEnumerable<string> attributeNames, Condition condition, bool withConsistency)
        {
            return new Reader(EnsureConnection(), attributeNames, condition);
        }
    }
}
