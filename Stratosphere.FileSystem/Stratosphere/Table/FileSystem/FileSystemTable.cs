// Copyright (c) 2010 7Clouds

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
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

            public void  WhenExpected(string name, string value)
            {
 	            throw new NotImplementedException();
            }

            public void  WhenExpectedNotExists(string name)
            {
 	            throw new NotImplementedException();
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

            public Reader(IDbConnection connection, IEnumerable<string> attributeNames, Condition condition)
            {
                _connection = connection;
                _command = _connection.CreateCommand();
                BuildCommand(_command, attributeNames, condition);
            }

            public ReadingState Read()
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
                        _lastAttributeName = AttributeName;

                        return ReadingState.Item;
                    }
                    
                    if (_lastAttributeName != AttributeName)
                    {
                        _lastAttributeName = AttributeName;

                        return ReadingState.Attribute;
                    }

                    return ReadingState.Value;
                }

                return ReadingState.End;
            }

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
                    if (_reader != null)
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
                    if (_reader != null)
                    {
                        return _reader.GetString(2);
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
                StringBuilder builder = new StringBuilder("select i.name, a.name, a.value from item i join attribute a on i.num=a.inum");
                builder.Append(BuildWhereClause(command, condition));
                builder.Append(" order by i.name, a.name");

                command.CommandText = builder.ToString();
            }

            private string FormatParameterName(string format)
            {
                string name = string.Format(format, _parameterIndex);
                _parameterIndex++;
                return name;
            }

            private string ContinueBuildWhereClause(IDbCommand command, Condition condition)
            {
                ItemNameCondition identityCondition;
                AttributeValueCondition valueCondition;
                AttributeIsNullCondition keyIsNullCondition;
                AttributeIsNotNullCondition keyIsNotNullCondition;
                GroupCondition groupCondition;

                if ((identityCondition = condition as ItemNameCondition) != null)
                {
                    string parameterName = FormatParameterName("@iname{0}");
                    AddParameter(command, parameterName, identityCondition.ItemName.ToString());

                    return string.Format("i.name={0}", parameterName);
                }
                else if ((valueCondition = condition as AttributeValueCondition) != null && valueCondition.Test == ValueTest.Equal)
                {
                    string nameParameterName = FormatParameterName("@aname{0}");
                    string valueParameterName = FormatParameterName("@avalue{0}");

                    AddParameter(command, nameParameterName, valueCondition.Pair.Key);
                    AddParameter(command, valueParameterName, valueCondition.Pair.Value);

                    return string.Format("i.num in (select a.inum from attribute a where a.name={0} and a.value={1})",
                        nameParameterName, valueParameterName);
                }
                else if ((keyIsNullCondition = condition as AttributeIsNullCondition) != null)
                {
                    string parameterName = FormatParameterName("@aname{0}");
                    AddParameter(command, parameterName, keyIsNullCondition.Name);

                    return string.Format("i.num not in (select a.inum from attribute a where a.name={0})",
                        parameterName);
                }
                else if ((keyIsNotNullCondition = condition as AttributeIsNotNullCondition) != null)
                {
                    string parameterName = FormatParameterName("@aname{0}");
                    AddParameter(command, parameterName, keyIsNotNullCondition.Name);

                    return string.Format("i.num in (select a.inum from attribute a where a.name={0})",
                        parameterName);
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
                            string innerExpression = ContinueBuildWhereClause(command, inner);

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

            private string BuildWhereClause(IDbCommand command, Condition condition)
            {
                if (condition != null)
                {
                    string expression = ContinueBuildWhereClause(command, condition);

                    if (!string.IsNullOrEmpty(expression))
                    {
                        StringBuilder builder = new StringBuilder(" where ");
                        builder.Append(expression);
                        return builder.ToString();
                    }
                }

                return string.Empty;
            }

            private static void AddParameter<T>(IDbCommand command, string name, T value)
            {
                IDbDataParameter parameter = command.CreateParameter();
                parameter.ParameterName = name;
                parameter.Value = value;
                command.Parameters.Add(parameter);
            }
        }

        public IReader Select(IEnumerable<string> attributeNames, Condition condition)
        {
            return new Reader(EnsureConnection(), attributeNames, condition);
        }

        public long SelectCount(Condition condition)
        {
            long count = 0;

            using (IReader reader = Select(new string[] { "itemName()" }, condition))
            {
                while (reader.Read() != ReadingState.End)
                {
                    count ++;
                }
            }

            return count;
        }
    }
}
