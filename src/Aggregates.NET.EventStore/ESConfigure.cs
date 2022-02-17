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
            Func<IServiceProvider, IEnumerable<(IPEndPoint endpoint, IEventStoreConnection connection)>> connections = (provider) => esSettings._definedConnections.Select(x =>
            {
                x.Value.settings
                    .LimitAttemptsForOperationTo(config.Retries)
                    .UseCustomLogger(provider.GetRequiredService<EventStore.ClientAPI.ILogger>())
                    .EnableVerboseLogging();

                return (x.Value.endpoint, EventStoreConnection.Create(x.Value.settings, x.Value.endpoint, x.Key));
            }).ToArray();

            Settings.RegistrationTasks.Add((container, settings) =>
            {
                container.AddSingleton<EventStore.ClientAPI.ILogger, EventStoreLogger>();
                container.AddTransient<IEnumerable<(IPEndPoint endpoint, IEventStoreConnection connection)>>(connections);
                container.AddTransient<IEventStoreClient, EventStoreClient>();
                container.AddTransient<IStoreEvents, StoreEvents>();
                container.AddTransient<IEventStoreConsumer, EventStoreConsumer>();


                return Task.CompletedTask;
            });

            // These tasks are needed for any endpoint connected to the eventstore
            // Todo: when implementing another eventstore, dont copy this, do it a better way
            Settings.StartupTasks.Add(async (provider, settings) =>
            {
                var connection = provider.GetRequiredService<IEventStoreClient>();

                await connection.Connect().ConfigureAwait(false);

                var subscriber = provider.GetRequiredService<IEventSubscriber>();

                await subscriber.Setup(
                    settings.Endpoint,
                    settings.EndpointVersion)
                .ConfigureAwait(false);

                await subscriber.Connect().ConfigureAwait(false);

                // Only setup children projection if client wants it
                if (settings.TrackChildren)
                {
                    var tracker = provider.GetService<ITrackChildren>();
                    // Use Aggregates.net version because its our children projection nothing to do with user code
                    await tracker.Setup(settings.Endpoint, settings.AggregatesVersion).ConfigureAwait(false);
                }

            });
            Settings.ShutdownTasks.Add(async (container, settings) =>
            {
                var subscriber = container.GetRequiredService<IEventSubscriber>();

                await subscriber.Shutdown();
            });

            return config;
        }
    }
}
