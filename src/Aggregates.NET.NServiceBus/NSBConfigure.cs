using Aggregates.Contracts;
using Aggregates.Internal;
using NServiceBus;
using NServiceBus.Configuration.AdvancedExtensibility;
using NServiceBus.Pipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NServiceBus.Transport;
using System.Threading.Tasks;
using System.Diagnostics.CodeAnalysis;
using NServiceBus.MessageInterfaces;
using NServiceBus.Settings;
using NServiceBus.Unicast.Messages;
using NServiceBus.Unicast;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace Aggregates
{
    [ExcludeFromCodeCoverage]
    public static class NSBConfigure
    {
        public static Configure NServiceBus(this Configure config, EndpointConfiguration endpointConfig)
        {
            IStartableEndpointWithExternallyManagedContainer startableEndpoint = null;

            {
                var settings = endpointConfig.GetSettings();
                var conventions = endpointConfig.Conventions();

                settings.Set(NSBDefaults.AggregatesSettings, config);
                settings.Set(NSBDefaults.AggregatesConfiguration, config.Configuration);

                // set the configured endpoint name to the one NSB config was constructed with
                config.SetEndpointName(settings.Get<string>("NServiceBus.Routing.EndpointName"));

                conventions.DefiningCommandsAs(type => typeof(Messages.ICommand).IsAssignableFrom(type));
                conventions.DefiningEventsAs(type => typeof(Messages.IEvent).IsAssignableFrom(type));
                conventions.DefiningMessagesAs(type => typeof(Messages.IMessage).IsAssignableFrom(type));

                endpointConfig.AssemblyScanner().ScanAppDomainAssemblies = true;
                endpointConfig.EnableCallbacks();
                endpointConfig.EnableInstallers();

                endpointConfig.UseSerialization<Internal.AggregatesSerializer>();
                endpointConfig.EnableFeature<Feature>();
            }


            Configure.RegistrationTasks.Add((container, settings) =>
            {

                container.AddSingleton<IEventMapper, EventMapper>();

                container.AddScoped<UnitOfWork.IDomain, NSBUnitOfWork>();

                container.AddSingleton<IEventFactory, EventFactory>();
                container.AddSingleton<IMessageDispatcher, Dispatcher>();
                container.AddSingleton<IMessaging, NServiceBusMessaging>();

                container.AddSingleton<IMessageSession>((_) => Bus.Instance);

                var nsbSettings = endpointConfig.GetSettings();

                nsbSettings.Set("SlowAlertThreshold", config.SlowAlertThreshold);
                nsbSettings.Set("CommandDestination", config.CommandDestination);


                endpointConfig.MakeInstanceUniquelyAddressable(settings.UniqueAddress);
                endpointConfig.LimitMessageProcessingConcurrencyTo(1);
                // NSB doesn't have an endpoint name setter other than the constructor, hack it in
                nsbSettings.Set("NServiceBus.Routing.EndpointName", settings.Endpoint);

                startableEndpoint = EndpointWithExternallyManagedServiceProvider.Create(endpointConfig, container);

                return Task.CompletedTask;
            });

            // Split creating the endpoint and starting the endpoint into 2 seperate jobs for certain (MICROSOFT) DI setup

            Configure.SetupTasks.Add((container, settings) =>
            {
                return Aggregates.Bus.Start(container, startableEndpoint);
            });

            return config;
        }

    }
}
