using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Microsoft.Extensions.Logging;

namespace Aggregates.Internal
{
    [ExcludeFromCodeCoverage]
    internal class EventStoreConsumer : IEventStoreConsumer
    {
        private readonly Microsoft.Extensions.Logging.ILogger Logger;

        private readonly IMetrics _metrics;
        private readonly IVersionRegistrar _registrar;
        private readonly IEventStoreClient _client;

        private readonly bool _allEvents;

        public EventStoreConsumer(ILogger<EventStoreConsumer> logger, IMetrics metrics, ISettings settings, IVersionRegistrar registrar, IEventStoreClient client)
        {
            Logger = logger;
            _metrics = metrics;
            _registrar = registrar;
            _client = client;

            _allEvents = settings.AllEvents;
        }

        public async Task SetupProjection(string endpoint, Version version, Type[] eventTypes)
        {
            // Dont use "-" we dont need category projection projecting our projection
            var stream = $"{endpoint}.{version}".Replace("-", "");

            await _client.EnableProjection("$by_category").ConfigureAwait(false);
            // Link all events we are subscribing to to a stream
            var functions =
                eventTypes
                    .Select(
                        eventType => $"'{_registrar.GetVersionedName(eventType)}': processEvent")
                    .Aggregate((cur, next) => $"{cur},\n{next}");

            // endpoint will get all events regardless of version of info
            // it will be up to them to handle upgrades
            if (_allEvents)
                functions = "$any: processEvent";

            // Don't tab this '@' will create tabs in projection definition
            var definition = @"
function processEvent(s,e) {{
    linkTo('{1}', e);
}}
fromCategories([{0}]).
when({{
{2}
}});";

            var appDefinition = string.Format(definition, $"'{StreamTypes.Domain}'", stream, functions);
            await _client.CreateProjection($"{stream}.app.projection", appDefinition).ConfigureAwait(false);
        }

        public async Task ConnectToProjection(string endpoint, Version version, IEventStoreConsumer.EventAppeared callback)
        {
            // Dont use "-" we dont need category projection projecting our projection
            var stream = $"{endpoint}.{version}".Replace("-", "");

            await _client.ConnectPinnedPersistentSubscription(stream, endpoint,
                (eventStream, eventNumber, @event) =>
                {
                    var headers = @event.Descriptor.Headers;
                    headers[$"{Defaults.PrefixHeader}.EventId"] = @event.EventId.ToString();
                    headers[$"{Defaults.PrefixHeader}.EventStream"] = eventStream;
                    headers[$"{Defaults.PrefixHeader}.EventPosition"] = eventNumber.ToString();

                    return callback(@event.Event, headers);
                });
        }

    }
}
