using EventStore.ClientAPI;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using Aggregates.Extensions;
using System.Threading.Tasks;
using System.Threading;
using Aggregates.Contracts;
using System.Net;
using EventStore.ClientAPI.Common;
using EventStore.ClientAPI.Exceptions;
using Aggregates.Exceptions;
using Aggregates.Messages;
using EventStore.ClientAPI.Projections;
using System.Text.RegularExpressions;

namespace Aggregates.Internal
{
    class EventStoreClient : IEventStoreClient, IDisposable
    {

        private readonly Microsoft.Extensions.Logging.ILogger Logger;
        private readonly IMessageSerializer _serializer;
        private readonly IVersionRegistrar _registrar;
        private readonly ISettings _settings;
        private readonly IMetrics _metrics;
        private readonly IEventMapper _mapper;
        private readonly IEnumerable<IMutate> _mutators;

        private readonly Dictionary<string, (IEventStoreClient.Status Status, IPEndPoint endpoint, IEventStoreConnection Connection)> _connections;

        private readonly object _subLock = new object();
        private readonly List<EventStoreCatchUpSubscription> _subscriptions;
        private readonly List<EventStorePersistentSubscriptionBase> _persistentSubs;

        private readonly CancellationTokenSource _csc;
        private bool _disposed;

        public EventStoreClient(ILogger<EventStoreClient> logger, IMessageSerializer serializer, IVersionRegistrar registrar, ISettings settings, IMetrics metrics, IEventMapper mapper, IEnumerable<IMutate> mutators, IEnumerable<(IPEndPoint endpoint, IEventStoreConnection connection)> connections)
        {
            Logger = logger;
            _serializer = serializer;
            _registrar = registrar;
            _settings = settings;
            _metrics = metrics;
            _mapper = mapper;
            _mutators = mutators;

            _csc = new CancellationTokenSource();

            _connections = new Dictionary<string, (IEventStoreClient.Status Status, IPEndPoint endpoint, IEventStoreConnection Connection)>();
            _subscriptions = new List<EventStoreCatchUpSubscription>();
            _persistentSubs = new List<EventStorePersistentSubscriptionBase>();

            foreach (var connection in connections)
            {
                connection.connection.Connected += Connection_Connected;
                connection.connection.Disconnected += Connection_Disconnected;
                connection.connection.Closed += Connection_Closed;
                connection.connection.AuthenticationFailed += Connection_AuthenticationFailed;
                connection.connection.ErrorOccurred += Connection_ErrorOccurred;
                connection.connection.Reconnecting += Connection_Reconnecting;

                Logger.InfoEvent("Initiate", "Registered EventStore client {ClientName}", connection.connection.ConnectionName);
                _connections[connection.connection.ConnectionName] = (IEventStoreClient.Status.Disconnected, connection.endpoint, connection.connection);
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            Close().Wait();
        }

        public bool Connected => _connections.All(x => x.Value.Status == IEventStoreClient.Status.Connected);

        public Task Connect()
        {
            Logger.InfoEvent("Connect", "Connecting to eventstore {Servers}", _connections.Select(x => x.Value.endpoint.ToString()).Aggregate((cur,next) => $"{cur}, {next}"));
            return Task.WhenAll(_connections.Select(x => x.Value.Connection.ConnectAsync()));
        }
        public async Task Close()
        {
            Logger.InfoEvent("Close", "Shutting down eventstore client, stopping subscriptions");
            foreach (var sub in _subscriptions)
                sub.Stop(TimeSpan.FromSeconds(5));
            foreach (var sub in _persistentSubs)
                sub.Stop(TimeSpan.FromSeconds(5));

            _csc.Cancel();

            foreach (var con in _connections.Where(x => x.Value.Status != IEventStoreClient.Status.Closed))
                con.Value.Connection.Close();

            Func<bool> closed = () => _connections.All(x => x.Value.Status == IEventStoreClient.Status.Closed);

            var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (!closed() && !cancel.IsCancellationRequested)
            {
                await Task.Delay(100);
            }

            if (!closed())
            {
                foreach (var con in _connections.Where(x => x.Value.Status != IEventStoreClient.Status.Closed))
                    Logger.ErrorEvent("Close", "Failed to close eventstore connection {ConnectionName}, timed out", con.Key);
                throw new Exception("Failed to close all eventstore connections, timed out");
            }
        }

        public Task<bool> SubscribeToStreamStart(string stream, IEventStoreClient.EventAppeared callback)
        {
            if (!Connected)
                throw new InvalidOperationException("Eventstore is not connected");

            var clientsToken = CancellationTokenSource.CreateLinkedTokenSource(_csc.Token);
            foreach (var connection in _connections)
            {
                Logger.InfoEvent("EndSubscribe", "Subscribe to begining of [{Stream:l}] store {Store}", stream, connection.Value.endpoint);

                var settings = new CatchUpSubscriptionSettings(1000, 50, Logger.IsEnabled(LogLevel.Debug), true);
                var startingNumber = 0L;
                try
                {
                    var subscription = connection.Value.Connection.SubscribeToStreamFrom(stream,
                        startingNumber,
                        settings,
                        eventAppeared: (sub, e) => eventAppeared(sub, e, clientsToken.Token, callback),
                        subscriptionDropped: (sub, reason, ex) =>
                            subscriptionDropped(sub, reason, ex, clientsToken.Token,
                            () => SubscribeToStreamEnd(stream, callback)
                        ));
                    lock (_subLock) _subscriptions.Add(subscription);

                }
                catch (OperationTimedOutException)
                {
                    // If one fails, cancel all the others
                    clientsToken.Cancel();
                }
            }
            return Task.FromResult(!clientsToken.IsCancellationRequested);
        }

        public async Task<bool> SubscribeToStreamEnd(string stream, IEventStoreClient.EventAppeared callback)
        {
            if (!Connected)
                throw new InvalidOperationException("Eventstore is not connected");

            var clientsToken = CancellationTokenSource.CreateLinkedTokenSource(_csc.Token);
            foreach (var connection in _connections)
            {
                try
                {
                    Logger.InfoEvent("EndSubscribe", "Subscribe to end of [{Stream:l}] store {Store}", stream, connection.Value.endpoint);
                    // Subscribe to the end
                    var lastEvent =
                        await connection.Value.Connection.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1, true).ConfigureAwait(false);

                    var settings = new CatchUpSubscriptionSettings(1000, 50, Logger.IsEnabled(LogLevel.Debug), true);

                    var startingNumber = 0L;
                    if (lastEvent.Status == SliceReadStatus.Success)
                        startingNumber = lastEvent.Events[0].OriginalEventNumber;

                    var subscription = connection.Value.Connection.SubscribeToStreamFrom(stream,
                        startingNumber,
                        settings,
                        eventAppeared: (sub, e) => eventAppeared(sub, e, clientsToken.Token, callback),
                        subscriptionDropped: (sub, reason, ex) =>
                            subscriptionDropped(sub, reason, ex, clientsToken.Token,
                            () => SubscribeToStreamEnd(stream, callback)
                        ));
                    lock (_subLock) _subscriptions.Add(subscription);
                }
                catch (OperationTimedOutException)
                {
                    // If one fails, cancel all the others
                    clientsToken.Cancel();
                }
            }
            return !clientsToken.IsCancellationRequested;
        }

        public async Task<bool> ConnectPinnedPersistentSubscription(string stream, string group, IEventStoreClient.EventAppeared callback)
        {
            if (!Connected)
                throw new InvalidOperationException("Eventstore is not connected");

            var settings = PersistentSubscriptionSettings.Create()
                .WithMaxRetriesOf(_settings.Retries)
                .ResolveLinkTos()
                .WithNamedConsumerStrategy(SystemConsumerStrategies.Pinned);
            if (_settings.ExtraStats)
                settings.WithExtraStatistics();
            if (!_settings.DevelopmentMode)
                settings.StartFromBeginning();

            foreach (var connection in _connections)
            {
                var store = connection.Value.Connection;
                try
                {
                    await store.CreatePersistentSubscriptionAsync(stream, group, settings,
                        store.Settings.DefaultUserCredentials).ConfigureAwait(false);
                    Logger.InfoEvent("CreatePinned", "Creating pinned subscription to [{Stream:l}] group [{Group:l}]", stream, group);
                }
                catch (InvalidOperationException)
                {
                    // Already created
                }

                var subCancel = CancellationTokenSource.CreateLinkedTokenSource(_csc.Token);

                try
                {
                    var subscription = await store.ConnectToPersistentSubscriptionAsync(stream, group,
                        eventAppeared: (sub, e) => eventAppeared(sub, e, subCancel.Token, callback),
                        // auto reconnect to subscription
                        subscriptionDropped: (sub, reason, ex) => 
                            subscriptionDropped(sub, reason, ex, subCancel.Token, 
                            () => ConnectPinnedPersistentSubscription(stream, group, callback)
                        ),
                        autoAck: true).ConfigureAwait(false);

                    lock (_subLock) _persistentSubs.Add(subscription);
                }
                catch (OperationTimedOutException)
                {
                    return false;
                }
            }
            return true;
        }

        public async Task<bool> EnableProjection(string name)
        {
            if (!Connected)
                throw new InvalidOperationException("Eventstore is not connected");

            foreach (var connection in _connections)
            {
                var httpSchema = getHttpSchema(connection.Value.Connection.Settings);

                var endpoint = changePort(connection.Value.endpoint, connection.Value.Connection.Settings.GossipPort);

                var manager = new ProjectionsManager(connection.Value.Connection.Settings.Log,
                    endpoint, TimeSpan.FromSeconds(30), httpSchema: httpSchema);
                try
                {
                    await manager.EnableAsync(name, connection.Value.Connection.Settings.DefaultUserCredentials).ConfigureAwait(false);
                }
                catch (OperationTimedOutException)
                {
                    return false;
                }
            }
            return true;
        }


        public async Task<bool> CreateProjection(string name, string definition)
        {
            if (!Connected)
                throw new InvalidOperationException("Eventstore is not connected");

            // Normalize new lines
            definition = definition.Replace(Environment.NewLine, "\n");

            foreach (var connection in _connections)
            {

                var httpSchema = getHttpSchema(connection.Value.Connection.Settings);

                var endpoint = changePort(connection.Value.endpoint, connection.Value.Connection.Settings.GossipPort);

                var manager = new ProjectionsManager(connection.Value.Connection.Settings.Log,
                    endpoint, TimeSpan.FromSeconds(30), httpSchema: httpSchema);

                try
                {
                    var existing = await manager.GetQueryAsync(name).ConfigureAwait(false);

                    // Remove all whitespace and new lines that could be different on different platforms and don't affect actual projection
                    var fixedExisting = Regex.Replace(existing, @"\s+", string.Empty);
                    var fixedDefinition = Regex.Replace(definition, @"\s+", string.Empty);

                    // In development mode - update the projection definition regardless of versioning
                    if (_settings.DevelopmentMode)
                    {
                        await manager.UpdateQueryAsync(name, definition, connection.Value.Connection.Settings.DefaultUserCredentials).ConfigureAwait(false);
                    }
                    else
                    {
                        if (!string.Equals(fixedExisting, fixedDefinition, StringComparison.OrdinalIgnoreCase))
                        {
                            Logger.FatalEvent("Initialization",
                                $"Projection [{name}] already exists and is a different version! If you've upgraded your code don't forget to bump your app's version!\nExisting:\n{existing}\nDesired:\n{definition}");
                            throw new EndpointVersionException(name, existing, definition);
                        }
                    }
                }
                catch (ProjectionCommandFailedException)
                {
                    try
                    {
                        // Projection doesn't exist 
                        await manager.CreateContinuousAsync(name, definition, false, connection.Value.Connection.Settings.DefaultUserCredentials)
                                .ConfigureAwait(false);
                    }
                    catch (ProjectionCommandFailedException e)
                    {
                        Logger.ErrorEvent("Projection", "Failed to create projection [{Name}]: {Error}", name, e.Message);
                    }
                }
            }
            return true;
        }
        private string getHttpSchema(ConnectionSettings settings)
        {
            return settings.UseSslConnection ? "https" : "http";
        }
        private EndPoint changePort(EndPoint endpoint, int newPort)
        {
            if (endpoint is IPEndPoint)
                return new IPEndPoint((endpoint as IPEndPoint).Address, newPort);
            if (endpoint is DnsEndPoint)
                return new DnsEndPoint((endpoint as DnsEndPoint).Host, newPort);

            throw new ArgumentOutOfRangeException(nameof(endpoint), endpoint?.GetType(),
                    "An invalid endpoint has been provided");
        }


        private async Task eventAppeared(EventStoreCatchUpSubscription sub, ResolvedEvent e, CancellationToken token, IEventStoreClient.EventAppeared callback)
        {
            // Don't care about metadata streams
            if (e.Event == null || e.Event.EventStreamId[0] == '$')
                return;

            if (token.IsCancellationRequested)
            {
                Logger.WarnEvent("Cancelation", "Event [{EventId:l}] from  stream [{Stream}] appeared while closing catchup subscription", e.OriginalEvent.EventId, e.OriginalStreamId);
                token.ThrowIfCancellationRequested();
            }
            try
            {
                await deserializeEvent(e, callback).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.ErrorEvent("Event Failed", ex, "Event processing failed - Stream: [{Stream:l}] Position: {StreamPosition} {ExceptionType} - {ExceptionMessage}", e.Event.EventStreamId, e.Event.EventNumber, ex.GetType().Name, ex.Message);
                //throw;
            }
        }
        private async Task eventAppeared(EventStorePersistentSubscriptionBase sub, ResolvedEvent e, CancellationToken token, IEventStoreClient.EventAppeared callback)
        {
            // Don't care about metadata streams
            if (e.Event == null || e.Event.EventStreamId[0] == '$')
            {
                // Dont need when autoAck is on
                //sub.Acknowledge(e.OriginalEvent.EventId);
                return;
            }

            if (token.IsCancellationRequested)
            {
                Logger.WarnEvent("Cancelation", "Event [{EventId:l}] from stream [{Stream}] appeared while closing subscription", e.OriginalEvent.EventId, e.OriginalStreamId);
                sub.Fail(e, PersistentSubscriptionNakEventAction.Retry, "Shutting down");
                token.ThrowIfCancellationRequested();
            }

            try
            {
                await deserializeEvent(e, callback).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.ErrorEvent("Event Failed", ex, "Event processing failed - Stream: [{Stream:l}] Position: {StreamPosition} {ExceptionType} - {ExceptionMessage}", e.Event.EventStreamId, e.Event.EventNumber, ex.GetType().Name, ex.Message);
                sub.Fail(e, PersistentSubscriptionNakEventAction.Park, ex.GetType().Name);
                // don't throw, stops subscription and causes reconnect
                //throw;
            }
        }

        private Task deserializeEvent(ResolvedEvent e, IEventStoreClient.EventAppeared callback)
        {
            var metadata = e.Event.Metadata;
            var data = e.Event.Data;

            IEventDescriptor descriptor;

            try
            {
                descriptor = _serializer.Deserialize<EventDescriptor>(metadata);
            }
            catch (SerializationException)
            {
                // Try the old format
                descriptor = _serializer.Deserialize<LegacyEventDescriptor>(metadata);
            }

            if (descriptor.Compressed)
                data = data.Decompress();

            var eventType = _registrar.GetNamedType(e.Event.EventType);
            _mapper.Initialize(eventType);

            var payload = _serializer.Deserialize(eventType, data) as IEvent;

            return callback(e.Event.EventStreamId, e.Event.EventNumber, new FullEvent
            {
                Descriptor = descriptor,
                Event = payload,
                EventId = e.Event.EventId
            });
        }

        private void subscriptionDropped(EventStoreCatchUpSubscription sub, SubscriptionDropReason reason, Exception ex, CancellationToken token, Func<Task> disconnected)
        {
            Logger.InfoEvent("Disconnect", "Subscription has been dropped because {Reason}: {ExceptionType} - {ExceptionMessage}", reason, ex.GetType().Name, ex.Message);

            lock (_subLock) _subscriptions.Remove(sub);
            if (reason == SubscriptionDropReason.UserInitiated) return;
            if (token.IsCancellationRequested) return;

            // Run via task because we are currently on the thread that would process a reconnect and we shouldn't block it
            Task.Run(disconnected);
        }
        private void subscriptionDropped(EventStorePersistentSubscriptionBase sub, SubscriptionDropReason reason, Exception ex, CancellationToken token, Func<Task> disconnected)
        {
            Logger.InfoEvent("Disconnect", "Subscription has been dropped because {Reason}: {ExceptionType} - {ExceptionMessage}", reason, ex.GetType().Name, ex.Message);

            lock (_subLock) _persistentSubs.Remove(sub);
            if (reason == SubscriptionDropReason.UserInitiated) return;
            if (token.IsCancellationRequested) return;

            // Run via task because we are currently on the thread that would process a reconnect and we shouldn't block it
            Task.Run(disconnected);
        }










        private void Connection_Reconnecting(object sender, ClientReconnectingEventArgs e)
        {
            _connections[e.Connection.ConnectionName] = (IEventStoreClient.Status.Reconnecting, _connections[e.Connection.ConnectionName].endpoint, e.Connection);
            Logger.InfoEvent("Reconnect", "EventStore client {ClientName} restarting", e.Connection.ConnectionName);
        }

        private void Connection_ErrorOccurred(object sender, ClientErrorEventArgs e)
        {
            Logger.ErrorEvent("Error", e.Exception, "EventStore client {ClientName} raised error {ExceptionType} - {ExceptionMessage}", e.Connection.ConnectionName, e.Exception?.GetType().Name, e.Exception?.Message);
        }

        private void Connection_AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
        {
            _connections[e.Connection.ConnectionName] = (IEventStoreClient.Status.Closed, _connections[e.Connection.ConnectionName].endpoint, e.Connection);
            Logger.FatalEvent("Auth", "EventStore client {ClientName} failed to authenticate: {Reason}", e.Connection.ConnectionName, e.Reason);
        }

        private void Connection_Closed(object sender, ClientClosedEventArgs e)
        {
            _connections[e.Connection.ConnectionName] = (IEventStoreClient.Status.Closed, _connections[e.Connection.ConnectionName].endpoint, e.Connection);
            Logger.InfoEvent("Closed", "EventStore client {ClientName} closed: {Reason}", e.Connection.ConnectionName, e.Reason);
        }

        private void Connection_Disconnected(object sender, ClientConnectionEventArgs e)
        {
            _connections[e.Connection.ConnectionName] = (IEventStoreClient.Status.Disconnected, _connections[e.Connection.ConnectionName].endpoint, e.Connection);
            Logger.InfoEvent("Disconnect", "EventStore client {ClientName} disconnected from {Endpoint}", e.Connection.ConnectionName, e.RemoteEndPoint);
        }

        private void Connection_Connected(object sender, ClientConnectionEventArgs e)
        {
            _connections[e.Connection.ConnectionName] = (IEventStoreClient.Status.Connected, _connections[e.Connection.ConnectionName].endpoint, e.Connection);
            Logger.InfoEvent("Connect", "EventStore client {ClientName} connected to {Endpoint}", e.Connection.ConnectionName, e.RemoteEndPoint);
        }

        public async Task<IFullEvent[]> GetEvents(string stream, long? start = null, int? count = null)
        {
            var shard = Math.Abs(stream.GetHash() % _connections.Count());

            var sliceStart = start ?? StreamPosition.Start;
            StreamEventsSlice current;

            var events = new List<ResolvedEvent>();
            var client = _connections.ElementAt(shard);
            using (var ctx = _metrics.Begin("EventStore Read Time"))
            {
                do
                {
                    var readsize = _settings.ReadSize;
                    if (count.HasValue)
                        readsize = Math.Min(count.Value - events.Count, _settings.ReadSize);

                    current =
                        await client.Value.Connection.ReadStreamEventsForwardAsync(stream, sliceStart, readsize, false)
                            .ConfigureAwait(false);

                    events.AddRange(current.Events);
                    sliceStart = current.NextEventNumber;
                } while (!current.IsEndOfStream && (!count.HasValue || (events.Count != count.Value)));

                if (ctx.Elapsed > TimeSpan.FromSeconds(1))
                    Logger.InfoEvent("Slow Alarm", "Reading {Events} events size {Size} stream [{Stream:l}] elapsed {Milliseconds} ms", events.Count, events.Sum(x => x.Event.Data.Length), stream, ctx.Elapsed.TotalMilliseconds);
                Logger.DebugEvent("Read", "{Events} events size {Size} stream [{Stream:l}] elapsed {Milliseconds} ms", events.Count, events.Sum(x => x.Event.Data.Length), stream, ctx.Elapsed.TotalMilliseconds);
            }

            if (current.Status == SliceReadStatus.StreamNotFound)
                throw new NotFoundException(stream, client.Value.endpoint);


            var translatedEvents = events.Select(e =>
            {
                var metadata = e.Event.Metadata;
                var data = e.Event.Data;

                IEventDescriptor descriptor;

                try
                {
                    descriptor = _serializer.Deserialize<EventDescriptor>(metadata);
                }
                catch (SerializationException)
                {
                    // Try the old format
                    descriptor = _serializer.Deserialize<LegacyEventDescriptor>(metadata);
                }

                if (descriptor == null || descriptor.EventId == Guid.Empty)
                {
                    // Assume we read from a children projection
                    // (theres no way to set metadata in projection states)

                    var children = _serializer.Deserialize<ChildrenProjection>(data);
                    if (!(children is IEvent))
                        throw new UnknownMessageException(e.Event.EventType);

                    return new FullEvent
                    {
                        Descriptor = null,
                        Event = children as IEvent,
                        EventId = Guid.Empty
                    };
                }
                if (descriptor.Compressed)
                    data = data.Decompress();

                var eventType = _registrar.GetNamedType(e.Event.EventType);

                var @event = _serializer.Deserialize(eventType, data);

                if (!(@event is IEvent))
                    throw new UnknownMessageException(e.Event.EventType);

                // Special case if event was written without a version - substitue the position from store
                //if (descriptor.Version == 0)
                //    descriptor.Version = e.Event.EventNumber;

                return (IFullEvent)new FullEvent
                {
                    Descriptor = descriptor,
                    Event = @event as IEvent,
                    EventId = e.Event.EventId
                };
            }).ToArray();

            return translatedEvents;
        }

        public async Task<IFullEvent[]> GetEventsBackwards(string stream, long? start = null, int? count = null)
        {
            var shard = Math.Abs(stream.GetHash() % _connections.Count());

            var events = new List<ResolvedEvent>();
            var sliceStart = StreamPosition.End;

            var client = _connections.ElementAt(shard);
            StreamEventsSlice current;
            if (start.HasValue || count == 1)
            {
                // Interesting, ReadStreamEventsBackwardAsync's [start] parameter marks start from begining of stream, not an offset from the end.
                // Read 1 event from the end, to figure out where start should be
                var result = await client.Value.Connection.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1, false).ConfigureAwait(false);
                sliceStart = result.NextEventNumber - start ?? 0;

                // Special case if only 1 event is requested no reason to read any more
                if (count == 1)
                    events.AddRange(result.Events);
            }

            if (!count.HasValue || count > 1)
            {
                using (var ctx = _metrics.Begin("EventStore Read Time"))
                {
                    do
                    {
                        var take = Math.Min((count ?? int.MaxValue) - events.Count, _settings.ReadSize);

                        current =
                            await client.Value.Connection.ReadStreamEventsBackwardAsync(stream, sliceStart, take, false)
                                .ConfigureAwait(false);

                        events.AddRange(current.Events);

                        sliceStart = current.NextEventNumber;
                    } while (!current.IsEndOfStream);


                    if (ctx.Elapsed > TimeSpan.FromSeconds(1))
                        Logger.InfoEvent("Slow Alarm", "Reading {Events} backward events size {Size} stream [{Stream:l}] elapsed {Milliseconds} ms", events.Count, events.Sum(x => x.Event.Data.Length), stream, ctx.Elapsed.TotalMilliseconds);
                    Logger.DebugEvent("BackwardsRead", "{Events} events size {Size} stream [{Stream:l}] elapsed {Milliseconds} ms", events.Count, events.Sum(x => x.Event.Data.Length), stream, ctx.Elapsed.TotalMilliseconds);
                }

                if (current.Status == SliceReadStatus.StreamNotFound)
                    throw new NotFoundException(stream, client.Value.endpoint);

            }

            var translatedEvents = events.Select(e =>
            {
                var metadata = e.Event.Metadata;
                var data = e.Event.Data;


                IEventDescriptor descriptor;

                try
                {
                    descriptor = _serializer.Deserialize<EventDescriptor>(metadata);
                }
                catch (SerializationException)
                {
                    // Try the old format
                    descriptor = _serializer.Deserialize<LegacyEventDescriptor>(metadata);
                }

                if (descriptor == null || descriptor.EventId == Guid.Empty)
                {
                    // Assume we read from a children projection
                    // (theres no way to set metadata in projection states)

                    var children = _serializer.Deserialize<ChildrenProjection>(data);
                    if (!(children is IEvent))
                        throw new UnknownMessageException(e.Event.EventType);
                    return new FullEvent
                    {
                        Descriptor = null,
                        Event = children as IEvent,
                        EventId = Guid.Empty
                    };
                }
                if (descriptor.Compressed)
                    data = data.Decompress();

                var eventType = _registrar.GetNamedType(e.Event.EventType);

                var @event = _serializer.Deserialize(eventType, data);

                if (!(@event is IEvent))
                    throw new UnknownMessageException(e.Event.EventType);
                // Special case if event was written without a version - substitute the position from store
                //if (descriptor.Version == 0)
                //    descriptor.Version = e.Event.EventNumber;

                return (IFullEvent)new FullEvent
                {
                    Descriptor = descriptor,
                    Event = @event as IEvent,
                    EventId = e.Event.EventId
                };

            }).ToArray();

            return translatedEvents;
        }

        public async Task<bool> VerifyVersion(string stream, long expectedVersion)
        {
            var size = await Size(stream).ConfigureAwait(false);
            return size == expectedVersion;
        }

        public async Task<long> Size(string stream)
        {
            var shard = Math.Abs(stream.GetHash() % _connections.Count());

            var client = _connections.ElementAt(shard);
            var result = await client.Value.Connection.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1, false).ConfigureAwait(false);
            var size = result.Status == SliceReadStatus.Success ? result.NextEventNumber : 0;
            Logger.DebugEvent("Size", "[{Stream:l}] size {Size}", stream, size);
            return size;

        }

        public Task<long> WriteEvents(string stream, IFullEvent[] events,
            IDictionary<string, string> commitHeaders, long? expectedVersion = null)
        {

            var translatedEvents = events.Select(e =>
            {
                IMutating mutated = new Mutating(e.Event, e.Descriptor.Headers ?? new Dictionary<string, string>());

                foreach (var mutator in _mutators)
                    mutated = mutator.MutateOutgoing(mutated);

                var mappedType = e.Event.GetType();
                if (!mappedType.IsInterface)
                    mappedType = _mapper.GetMappedTypeFor(mappedType) ?? mappedType;

                var descriptor = new EventDescriptor
                {
                    EventId = e.EventId ?? Guid.NewGuid(),
                    CommitHeaders = (commitHeaders ?? new Dictionary<string, string>()).Merge(new Dictionary<string, string>
                    {
                        [Defaults.InstanceHeader] = Defaults.Instance.ToString(),
                        [Defaults.EndpointHeader] = _settings.Endpoint,
                        [Defaults.EndpointVersionHeader] = _settings.EndpointVersion.ToString(),
                        [Defaults.AggregatesVersionHeader] = _settings.AggregatesVersion.ToString(),
                        [Defaults.MachineHeader] = Environment.MachineName,
                    }),
                    Compressed = _settings.Compression.HasFlag(Compression.Events),
                    EntityType = e.Descriptor.EntityType,
                    StreamType = e.Descriptor.StreamType,
                    Bucket = e.Descriptor.Bucket,
                    StreamId = e.Descriptor.StreamId,
                    Parents = e.Descriptor.Parents,
                    Version = e.Descriptor.Version,
                    Timestamp = e.Descriptor.Timestamp,
                    Headers = e.Descriptor.Headers,
                };

                var eventType = _registrar.GetVersionedName(mappedType);
                foreach (var header in mutated.Headers)
                    e.Descriptor.Headers[header.Key] = header.Value;


                var @event = _serializer.Serialize(mutated.Message);

                if (_settings.Compression.HasFlag(Compression.Events))
                {
                    descriptor.Compressed = true;
                    @event = @event.Compress();
                }
                var metadata = _serializer.Serialize(descriptor);

                return new EventData(
                    descriptor.EventId,
                    eventType,
                    !descriptor.Compressed,
                    @event,
                    metadata
                );
            }).ToArray();

            return DoWrite(stream, translatedEvents, expectedVersion);
        }

        public async Task WriteMetadata(string stream, long? maxCount = null, long? truncateBefore = null,
            TimeSpan? maxAge = null,
            TimeSpan? cacheControl = null, bool force = false, IDictionary<string, string> custom = null)
        {

            var shard = Math.Abs(stream.GetHash() % _connections.Count());
            var client = _connections.ElementAt(shard);

            Logger.DebugEvent("Metadata", "Metadata to stream [{Stream:l}] [ MaxCount: {MaxCount}, MaxAge: {MaxAge}, CacheControl: {CacheControl}, Custom: {Custom} ]", stream, maxCount, maxAge, cacheControl, custom.AsString());

            var existing = await client.Value.Connection.GetStreamMetadataAsync(stream).ConfigureAwait(false);


            var metadata = StreamMetadata.Build();

            if ((maxCount ?? existing.StreamMetadata?.MaxCount).HasValue)
                metadata.SetMaxCount((maxCount ?? existing.StreamMetadata?.MaxCount).Value);
            if ((truncateBefore ?? existing.StreamMetadata?.TruncateBefore).HasValue)
                metadata.SetTruncateBefore(Math.Max(truncateBefore ?? 0, (truncateBefore ?? existing.StreamMetadata?.TruncateBefore).Value));
            if ((maxAge ?? existing.StreamMetadata?.MaxAge).HasValue)
                metadata.SetMaxAge((maxAge ?? existing.StreamMetadata?.MaxAge).Value);
            if ((cacheControl ?? existing.StreamMetadata?.CacheControl).HasValue)
                metadata.SetCacheControl((cacheControl ?? existing.StreamMetadata?.CacheControl).Value);

            var customs = existing.StreamMetadata?.CustomKeys;
            // Make sure custom metadata is preserved
            if (customs != null && customs.Any())
            {
                foreach (var key in customs)
                    metadata.SetCustomProperty(key, existing.StreamMetadata.GetValue<string>(key));
            }

            if (custom != null)
            {
                foreach (var kv in custom)
                    metadata.SetCustomProperty(kv.Key, kv.Value);
            }

            try
            {
                try
                {
                    await client.Value.Connection.SetStreamMetadataAsync(stream, existing.MetastreamVersion, metadata).ConfigureAwait(false);

                }
                catch (WrongExpectedVersionException e)
                {
                    throw new VersionException(e.Message, e);
                }
                catch (CannotEstablishConnectionException e)
                {
                    throw new PersistenceException(e.Message, e);
                }
                catch (OperationTimedOutException e)
                {
                    throw new PersistenceException(e.Message, e);
                }
                catch (EventStoreConnectionException e)
                {
                    throw new PersistenceException(e.Message, e);
                }
            }
            catch (Exception ex)
            {
                Logger.WarnEvent("MetadataFailure", ex, "{ExceptionType} - {ExceptionMessage}", ex.GetType().Name, ex.Message);
                throw;
            }
        }

        public async Task<string> GetMetadata(string stream, string key)
        {
            var shard = Math.Abs(stream.GetHash() % _connections.Count());
            var client = _connections.ElementAt(shard);

            var existing = await client.Value.Connection.GetStreamMetadataAsync(stream).ConfigureAwait(false);

            Logger.DebugEvent("Read", "Metadata stream [{Stream:l}] {Metadata}", stream, existing.StreamMetadata?.AsJsonString());
            string property = "";
            if (!existing.StreamMetadata?.TryGetValue(key, out property) ?? false)
                property = "";
            return property;
        }

        private async Task<long> DoWrite(string stream, EventData[] events, long? expectedVersion = null)
        {
            var shard = Math.Abs(stream.GetHash() % _connections.Count());
            var client = _connections.ElementAt(shard);

            long nextVersion;
            using (var ctx = _metrics.Begin("EventStore Write Time"))
            {
                try
                {
                    var result = await
                        client.Value.Connection.AppendToStreamAsync(stream, expectedVersion ?? ExpectedVersion.Any, events)
                            .ConfigureAwait(false);

                    nextVersion = result.NextExpectedVersion;
                }
                catch (WrongExpectedVersionException e)
                {
                    throw new VersionException(e.Message, e);
                }
                catch (CannotEstablishConnectionException e)
                {
                    throw new PersistenceException(e.Message, e);
                }
                catch (OperationTimedOutException e)
                {
                    throw new PersistenceException(e.Message, e);
                }
                catch (EventStoreConnectionException e)
                {
                    throw new PersistenceException(e.Message, e);
                }
                catch (InvalidOperationException e)
                {
                    throw new PersistenceException(e.Message, e);
                }

                if (ctx.Elapsed > TimeSpan.FromSeconds(1))
                    Logger.InfoEvent("Slow Alarm", "Writting {Events} events size {Size} stream [{Stream:l}] version {ExpectedVersion} took {Milliseconds} ms", events.Count(), events.Sum(x => x.Data.Length), stream, expectedVersion, ctx.Elapsed.TotalMilliseconds);
                Logger.DebugEvent("Write", "{Events} events size {Size} stream [{Stream:l}] version {ExpectedVersion} took {Milliseconds} ms", events.Count(), events.Sum(x => x.Data.Length), stream, expectedVersion, ctx.Elapsed.TotalMilliseconds);
            }
            return nextVersion;
        }

    }
}
