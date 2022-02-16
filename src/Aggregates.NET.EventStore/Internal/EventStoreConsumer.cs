﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Aggregates.Extensions;
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

        private readonly StreamIdGenerator _streamIdGen;
        private readonly bool _allEvents;

        public EventStoreConsumer(ILogger<EventStoreConsumer> logger, IMetrics metrics, ISettings settings, IVersionRegistrar registrar, IEventStoreClient client)
        {
            Logger = logger;
            _metrics = metrics;
            _registrar = registrar;
            _client = client;

            _streamIdGen = settings.Generator;
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
        public async Task SetupChildrenProjection(string endpoint, Version version)
        {

            // Todo: is it necessary to ensure JSON.parse works?
            // everything in DOMAIN should be from us - so all metadata will be parsable


            // this projection will parse PARENTS metadata in our events and create partitioned states representing all the CHILDREN
            // of an entity.

            // Don't tab this '@' will create tabs in projection definition
            var definition = @"
options({{
    $includeLinks: false
}});

function createParents(parents) {{
    if(!parents || !parents.length || parents.length === 0)
        return '';

    return parents.map(function(x) {{ return x.StreamId; }}).join(':');
}}

fromCategory('{0}')
.partitionBy(function(event) {{
    let metadata = JSON.parse(event.metadataRaw);
    if(metadata.Parents === null || metadata.Parents.length == 0)
        return undefined;
    let lastParent = metadata.Parents.pop();
        
    let streamId = 'CHILDREN' + '-' + metadata.Bucket + '-[' + createParents(metadata.Parents) + ']-' + lastParent.EntityType + '-' + lastParent.StreamId;
        
    return streamId;
}})
.when({{
    $init: function() {{
        return {{
            Children: []
        }};
    }},
    $any: function(state, event) {{
        let metadata = JSON.parse(event.metadataRaw);
        if(metadata.Version !== 0)
            return state;
            
        state.Children.push({{ EntityType: metadata.EntityType, StreamId: metadata.StreamId }});
        return state;
    }}
}})
.outputState();";

            Logger.DebugEvent("Setup", "Setup children tracking projection {Name}", $"aggregates.net.children.{version}");
            var appDefinition = string.Format(definition, StreamTypes.Domain);
            await _client.CreateProjection($"aggregates.net.children.{version}", appDefinition).ConfigureAwait(false);
        }

        private IParentDescriptor[] getParents(IEntity entity)
        {
            if (entity == null)
                return null;
            if (!(entity is IChildEntity))
                return null;

            var child = entity as IChildEntity;

            var parents = getParents(child.Parent)?.ToList() ?? new List<IParentDescriptor>();
            parents.Add(new ParentDescriptor { EntityType = _registrar.GetVersionedName(child.Parent.GetType()), StreamId = child.Parent.Id });
            return parents.ToArray();
        }
        public async Task<ChildrenProjection> GetChildrenData<TParent>(Version version, TParent parent) where TParent : IHaveEntities<TParent>
        {
            var parents = getParents(parent);

            var parentEntityType = _registrar.GetVersionedName(typeof(TParent));

            Logger.DebugEvent("Children", "Getting children for entity type {EntityType} stream id {Id}", parentEntityType, parent.Id);

            var stream = _streamIdGen(parentEntityType, StreamTypes.Children, parent.Bucket, parent.Id, parents?.Select(x => x.StreamId).ToArray());

            // ES generated stream name
            var fullStream = $"$projections-aggregates.net.children.{version}-{stream}-result";

            var stateEvents = await _client.GetEventsBackwards(fullStream, count: 1).ConfigureAwait(false);
            if (!stateEvents.Any())
                return null;

            var state = stateEvents[0];
            var children = state.Event as ChildrenProjection;
            return children;
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
