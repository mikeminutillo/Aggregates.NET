using Microsoft.Extensions.DependencyInjection;
using NServiceBus.Pipeline;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates.Internal
{
    [ExcludeFromCodeCoverage]
    internal class ScopedProviderBehavior : Behavior<IIncomingPhysicalMessageContext>
    {
        private readonly IServiceProvider _provider;
        public ScopedProviderBehavior(IServiceProvider provider)
        {
            _provider = provider;
        }

        public override async Task Invoke(IIncomingPhysicalMessageContext context, Func<Task> next)
        {
            using (var child = _provider.CreateScope())
            {
                context.Extensions.Set(child);
                context.Extensions.Set(child.ServiceProvider);
                await next().ConfigureAwait(false);
            }
        }
    }
    [ExcludeFromCodeCoverage]
    internal class ScopedProviderRegistration : RegisterStep
    {
        public ScopedProviderRegistration() : base(
            stepId: "ScopedProvider",
            behavior: typeof(ScopedProviderBehavior),
            description: "Provides a scoped service provider",
            factoryMethod: (b) => new ScopedProviderBehavior(b.Build<IServiceProvider>())
        )
        {
            InsertBefore("MutateIncomingTransportMessage");
        }
    }
}
