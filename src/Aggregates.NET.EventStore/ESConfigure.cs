using Aggregates.Contracts;
using Aggregates.Extensions;
using Aggregates.Internal;
using EventStore.ClientAPI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates
{
    [ExcludeFromCodeCoverage]
    public static class ESConfigure
    {
        public static Settings EventStore(this Settings config, params IEventStoreConnection[] connections)
        {
            Settings.RegistrationTasks.Add((container, settings) =>
            {

                container.AddSingleton<IEventStoreConsumer>((factory) =>
                    new EventStoreConsumer(
                        factory.GetRequiredService<ILoggerFactory>(),
                        factory.GetRequiredService<ISettings>(),
                        factory.GetRequiredService<IMetrics>(),
                        factory.GetRequiredService<IMessageSerializer>(),
                        factory.GetRequiredService<IVersionRegistrar>(),
                        connections,
                        factory.GetRequiredService<IEventMapper>()
                        ));
                container.AddSingleton<IStoreEvents>((factory) =>
                    new StoreEvents(
                        factory.GetRequiredService<ILoggerFactory>(),
                        factory.GetRequiredService<ISettings>(),
                        factory.GetRequiredService<IServiceProvider>(),
                        factory.GetRequiredService<IMetrics>(),
                        factory.GetRequiredService<IMessageSerializer>(),
                        factory.GetRequiredService<IEventMapper>(),
                        factory.GetRequiredService<IVersionRegistrar>(),
                        connections
                        ));

                return Task.CompletedTask;
            });

            // These tasks are needed for any endpoint connected to the eventstore
            // Todo: when implementing another eventstore, dont copy this, do it a better way
            Settings.StartupTasks.Add(async (provider, settings) =>
            {
                var subscribers = provider.GetServices<IEventSubscriber>();

                await subscribers.WhenAllAsync(x => x.Setup(
                    settings.Endpoint,
                    Assembly.GetEntryAssembly().GetName().Version)
                ).ConfigureAwait(false);

                await subscribers.WhenAllAsync(x => x.Connect()).ConfigureAwait(false);

                // Only setup children projection if client wants it
                if (settings.TrackChildren)
                {
                    var tracker = provider.GetService<ITrackChildren>();
                    await tracker.Setup(settings.Endpoint, Assembly.GetEntryAssembly().GetName().Version).ConfigureAwait(false);
                }

            });
            Settings.ShutdownTasks.Add(async (container, settings) =>
            {
                var subscribers = container.GetServices<IEventSubscriber>();

                await subscribers.WhenAllAsync(x => x.Shutdown()).ConfigureAwait(false);
            });

            return config;
        }
    }
}
