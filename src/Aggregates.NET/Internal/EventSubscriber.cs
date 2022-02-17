﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Aggregates.Exceptions;
using Aggregates.Extensions;
using Aggregates.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Aggregates.Internal
{
    class EventSubscriber : IEventSubscriber
    {

        private readonly ILogger Logger;

        private string _endpoint;
        private Version _version;

        private readonly IMessaging _messaging;

        private readonly IEventStoreConsumer _consumer;
        private readonly IMessageDispatcher _dispatcher;

        private bool _disposed;


        public EventSubscriber(ILogger<EventSubscriber> logger, IMessaging messaging, IEventStoreConsumer consumer, IMessageDispatcher dispatcher)
        {
            Logger = logger;
            _messaging = messaging;
            _consumer = consumer;
            _dispatcher = dispatcher;

        }

        public async Task Setup(string endpoint, Version version)
        {
            _endpoint = endpoint;
            // Changes which affect minor version require a new projection, ignore revision and build numbers
            _version = new Version(version.Major, version.Minor);

            // Todo: creating the projection is dependant on EventStore - which defeats the purpose of the different assembly
            var discoveredEvents =
                _messaging.GetHandledTypes().Where(x => typeof(IEvent).IsAssignableFrom(x)).OrderBy(x => x.FullName).ToList();

            if (!discoveredEvents.Any())
            {
                Logger.WarnEvent("Initiation", $"Event consuming is enabled but we did not detect any IEvent handlers");
                return;
            }

            Logger.InfoEvent("Setup", "Setup projection for discovered events {Events}", discoveredEvents.Select(x => x.Name).Aggregate((cur,next) => $"{cur}{Environment.NewLine}{next}"));
            await _consumer.SetupProjection(_endpoint, _version, discoveredEvents.ToArray());
        }

        public Task Connect()
        {
            Logger.InfoEvent("Connect", "Connecting to event projection");
            return _consumer.ConnectToProjection(_endpoint, _version, onEvent);
        }
        public Task Shutdown()
        {
            return Task.CompletedTask;
        }


        private async Task onEvent(IEvent @event, IDictionary<string, string> headers)
        {
            var message = new FullMessage
            {
                Message = @event,
                Headers = headers
            };


            await _dispatcher.SendLocal(message).ConfigureAwait(false);
        }



        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
        }

    }
}
