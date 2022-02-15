using Aggregates.Contracts;
using Aggregates.Extensions;
using Aggregates.Internal;
using EventStore.ClientAPI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates
{
    [ExcludeFromCodeCoverage]
    public static class ESConfigure
    {
        public class ESSettings
        {
            internal Dictionary<string, (IPEndPoint endpoint, ConnectionSettingsBuilder settings)> _definedConnections = new Dictionary<string, (IPEndPoint endpoint, ConnectionSettingsBuilder settings)>();

            public ESSettings AddClient(ConnectionSettingsBuilder settings, IPEndPoint endpoint, string name)
            {
                if (_definedConnections.ContainsKey(name))
                    throw new ArgumentException($"Eventstore client [{name}] already defined");
                // need to do this because endpoint is not inside the eventstore data we have available
                _definedConnections[name] = (endpoint, settings);// EventStoreConnection.Create(settings, endpoint, name));
                return this;
            }
        }
        public static Settings EventStore(this Settings config, Action<ESSettings> eventStoreConfig)
        {
            var esSettings = new ESSettings();
            eventStoreConfig(esSettings);

            if (!esSettings._definedConnections.Any())
                throw new ArgumentException("Atleast 1 eventstore client must be defined");

            // Prevents creation of event store connections until Provider is available
            // since logger needs ILogger defined
            Func<IServiceProvider, ISettings, IEventStoreConnection[]> connections = (provider, settings) => esSettings._definedConnections.Select(x =>
            {
                x.Value.settings
                    .LimitAttemptsForOperationTo(settings.Retries)
                    .UseCustomLogger(provider.GetRequiredService<EventStore.ClientAPI.ILogger>())
                    .EnableVerboseLogging();

                return EventStoreConnection.Create(x.Value.settings, x.Value.endpoint, x.Key);
            }).ToArray();

            Settings.RegistrationTasks.Add((container, settings) =>
            {
                container.AddSingleton<EventStore.ClientAPI.ILogger, EventStoreLogger>();
                //container.AddSingleton<IEventStoreClient>(factory => new EventStoreClient(factory.GetRequiredService<ILogger<EventStoreClient>>(), connections));
                //container.AddSingleton<IEventStoreConsumer>((factory) =>
                //    new EventStoreConsumer(
                //        factory.GetRequiredService<ILoggerFactory>(),
                //        factory.GetRequiredService<ISettings>(),
                //        factory.GetRequiredService<IMetrics>(),
                //        factory.GetRequiredService<IMessageSerializer>(),
                //        factory.GetRequiredService<IVersionRegistrar>(),
                //        connections,
                //        factory.GetRequiredService<IEventMapper>()
                //        ));
                //container.AddSingleton<IStoreEvents>((factory) =>
                //    new StoreEvents(
                //        factory.GetRequiredService<ILoggerFactory>(),
                //        factory.GetRequiredService<ISettings>(),
                //        factory.GetRequiredService<IServiceProvider>(),
                //        factory.GetRequiredService<IMetrics>(),
                //        factory.GetRequiredService<IMessageSerializer>(),
                //        factory.GetRequiredService<IEventMapper>(),
                //        factory.GetRequiredService<IVersionRegistrar>(),
                //        connections
                //        ));

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
