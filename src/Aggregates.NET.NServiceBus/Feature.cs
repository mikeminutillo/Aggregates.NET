﻿using Aggregates.Contracts;
using Aggregates.Extensions;
using Aggregates.Internal;
using Aggregates.Messages;
using Microsoft.Extensions.Logging;
using NServiceBus;
using NServiceBus.Features;
using NServiceBus.MessageInterfaces;
using NServiceBus.Settings;
using NServiceBus.Unicast;
using NServiceBus.Unicast.Messages;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aggregates
{
    [ExcludeFromCodeCoverage]
    class Feature : NServiceBus.Features.Feature
    {
        public Feature()
        {
        }
        protected override void Setup(FeatureConfigurationContext context)
        {
            var settings = context.Settings;
            var aggSettings = settings.Get<Configure>(NSBDefaults.AggregatesSettings);
            var container = aggSettings.Container;

            context.Pipeline.Register<ExceptionRejectorRegistration>();

            if (!aggSettings.Passive)
            {
                MutationManager.RegisterMutator("domain unit of work", typeof(UnitOfWork.IDomain));


                context.Pipeline.Register<UowRegistration>();
                context.Pipeline.Register<CommandAcceptorRegistration>();
                context.Pipeline.Register<SagaBehaviourRegistration>();

                // Remove NSBs unit of work since we do it ourselves
                context.Pipeline.Remove("ExecuteUnitOfWork");

                // bulk invoke only possible with consumer feature because it uses the eventstore as a sink when overloaded
                context.Pipeline.Replace("InvokeHandlers", (b) =>
                    new BulkInvokeHandlerTerminator(b.Build<ILoggerFactory>(), b.Build<IMetrics>(), b.Build<IEventMapper>()),
                    "Replaces default invoke handlers with one that supports our custom delayed invoker");
            }

            context.Pipeline.Register<LocalMessageUnpackRegistration>();
            context.Pipeline.Register<LogContextProviderRegistration>();

            if (aggSettings.SlowAlertThreshold.HasValue)
                context.Pipeline.Register<TimeExecutionRegistration>();
            var types = settings.GetAvailableTypes();

            var messageMetadataRegistry = settings.Get<MessageMetadataRegistry>();
            context.Pipeline.Register<MessageIdentifierRegistration>();
            context.Pipeline.Register<MessageDetyperRegistration>();

            // Register all service handlers in my IoC so query processor can use them
            foreach (var type in types.Where(IsServiceHandler))
                container.Register(type, Lifestyle.PerInstance);

            context.Pipeline.Register<MutateIncomingRegistration>();
            context.Pipeline.Register<MutateOutgoingRegistration>();

            // We are sending IEvents, which NSB doesn't like out of the box - so turn that check off
            context.Pipeline.Remove("EnforceSendBestPractices");


            context.RegisterStartupTask(builder => new EndpointRunner(context.Settings.InstanceSpecificQueue(), aggSettings, aggSettings.StartupTasks, aggSettings.ShutdownTasks));
        }
        private static bool IsServiceHandler(Type type)
        {
            if (type.IsAbstract || type.IsGenericTypeDefinition)
                return false;

            return type.GetInterfaces()
                .Where(@interface => @interface.IsGenericType)
                .Select(@interface => @interface.GetGenericTypeDefinition())
                .Any(genericTypeDef => genericTypeDef == typeof(IProvideService<,>));
        }
    }

    [ExcludeFromCodeCoverage]
    class EndpointRunner : FeatureStartupTask
    {
        private readonly String _instanceQueue;
        private readonly Configure _config;
        private readonly IEnumerable<Func<Configure, Task>> _startupTasks;
        private readonly IEnumerable<Func<Configure, Task>> _shutdownTasks;

        public EndpointRunner(String instanceQueue, Configure config, IEnumerable<Func<Configure, Task>> startupTasks, IEnumerable<Func<Configure, Task>> shutdownTasks)
        {
            _instanceQueue = instanceQueue;
            _config = config;
            _startupTasks = startupTasks;
            _shutdownTasks = shutdownTasks;
        }
        protected override async Task OnStart(IMessageSession session)
        {
            var logFactory = _config.Container.Resolve<ILoggerFactory>();
            var logger = logFactory.CreateLogger("EndpointRunner");

            // Subscribe to BulkMessage, because it wraps messages and is not used in a handler directly
            if (!_config.Passive)
                await session.Subscribe<BulkMessage>().ConfigureAwait(false);

            logger.InfoEvent("Startup", "Starting on {Queue}", _instanceQueue);

            await session.Publish<EndpointAlive>(x =>
            {
                x.Endpoint = _instanceQueue;
                x.Instance = Defaults.Instance;
            }).ConfigureAwait(false);

            // Don't stop the bus from completing setup
            ThreadPool.QueueUserWorkItem(_ =>
            {
                _startupTasks.WhenAllAsync(x => x(_config)).Wait();
            });
        }
        protected override async Task OnStop(IMessageSession session)
        {
            var logFactory = _config.Container.Resolve<ILoggerFactory>();
            var logger = logFactory.CreateLogger("EndpointRunner");

            logger.InfoEvent("Shutdown", "Stopping on {Queue}", _instanceQueue);
            await session.Publish<EndpointDead>(x =>
            {
                x.Endpoint = _instanceQueue;
                x.Instance = Defaults.Instance;
            }).ConfigureAwait(false);

            await _shutdownTasks.WhenAllAsync(x => x(_config)).ConfigureAwait(false);
        }
    }
}
