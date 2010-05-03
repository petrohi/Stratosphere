// Copyright (c) 2010, 7Clouds. All rights reserved.

using System.Collections.Generic;
using System.Xml.Linq;
using System.Reflection;
using System;

namespace Stratosphere.Queue
{
    public static class QueueConfiguration
    {
        public static IQueue Configure(XElement configuration)
        {
            IQueue queue = null;
            XElement tableConfiguration = configuration.Element(XName.Get("Queue"));

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

                            queue = method.Invoke(null, new object[] { tableConfiguration }) as IQueue;

                            if (queue != null && delayMilliseconds != null)
                            {
                                queue = new DelayedQueue(queue, int.Parse(delayMilliseconds.Value));
                            }
                        }
                    }
                }
            }

            return queue;
        }
    }
}
