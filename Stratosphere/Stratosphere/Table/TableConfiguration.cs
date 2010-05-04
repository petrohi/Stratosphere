// Copyright (c) 2010 7Clouds

using System.Collections.Generic;
using System.Xml.Linq;
using System.Reflection;
using System;

namespace Stratosphere.Table
{
    public static class TableConfiguration
    {
        public static ITable Configure(XElement configuration)
        {
            ITable table = null;
            XElement tableConfiguration = configuration.Element(XName.Get("Table"));

            if (tableConfiguration != null)
            {
                XElement assemblyName = tableConfiguration.Element(XName.Get("AssemblyName"));
                XElement typeName = tableConfiguration.Element(XName.Get("TypeName"));
                XElement delayMilliseconds = tableConfiguration.Element(XName.Get("ConsistencyDelayMilliseconds"));

                if (assemblyName != null && typeName != null)
                {
                    Assembly assembly = Assembly.Load(assemblyName.Value);

                    if (assembly != null)
                    {
                        Type type = assembly.GetType(typeName.Value);

                        if (type != null)
                        {
                            MethodInfo method = type.GetMethod("Configure", new Type[] { typeof(XElement) });

                            table = method.Invoke(null, new object[] { tableConfiguration }) as ITable;

                            if (table != null && delayMilliseconds != null)
                            {
                                table = new DelayedTable(table, int.Parse(delayMilliseconds.Value));
                            }
                        }
                    }
                }
            }

            return table;
        }
    }
}
