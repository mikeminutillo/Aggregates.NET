﻿using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates
{
    [ExcludeFromCodeCoverage]
    public static class SIConfigure
    {
        public static Configure SimpleInjector(this Configure config, SimpleInjector.Container container)
        {
            container.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
            container.Options.AllowOverridingRegistrations = true;
            // Without this the InstancePerCall component cannot be registered because it is an IDisposable
            container.Options.EnableAutoVerification = false;

            config.Container = new Internal.Container(container);

            return config;
        }
    }
}
