// Copyright (c) 2010 7Clouds

using System;
using System.Reflection;
using System.Xml.Linq;

namespace Stratosphere.Block
{
    public static class ContainerConfiguration
    {
        public static IContainer Configure(XElement configuration)
        {
            IContainer container = null;
            XElement containerConfiguration = configuration.Element(XName.Get("Container"));

            if (containerConfiguration != null)
            {
                XElement assemblyName = containerConfiguration.Element(XName.Get("AssemblyName"));
                XElement typeName = containerConfiguration.Element(XName.Get("TypeName"));

                if (assemblyName != null && typeName != null)
                {
                    Assembly assembly = Assembly.Load(assemblyName.Value);

                    if (assembly != null)
                    {
                        Type type = assembly.GetType(typeName.Value);

                        if (type != null)
                        {
                            MethodInfo method = type.GetMethod("Configure", new Type[] { typeof(XElement) });

                            container = method.Invoke(null, new object[] { containerConfiguration }) as IContainer;
                        }
                    }
                }
            }

            return container;
        }
    }
}
