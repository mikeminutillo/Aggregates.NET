using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Aggregates.Exceptions;
using Aggregates.Extensions;
using Aggregates.Messages;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Aggregates.Internal
{
    [ExcludeFromCodeCoverage]
    class StoreEvents : IStoreEvents
    {
        private readonly Microsoft.Extensions.Logging.ILogger Logger;
        private readonly StreamIdGenerator _generator;
        private readonly IEventStoreClient _client;
        private readonly IVersionRegistrar _registrar;
        private readonly IEnumerable<IMutate> _mutators;

        public StoreEvents(ILogger<StoreEvents> logger, StreamIdGenerator generator, IVersionRegistrar registrar, IEnumerable<IMutate> mutators, IEventStoreClient client)
        {
            _generator = generator;
            _client = client;
            _registrar = registrar;
            _mutators = mutators;

            Logger = logger;
        }

        public Task<IFullEvent[]> GetEvents<TEntity>(string bucket, Id streamId, Id[] parents, long? start = null, int? count = null) where TEntity : IEntity
        {
            var stream = _generator(_registrar.GetVersionedName(typeof(TEntity)), StreamTypes.Domain, bucket, streamId, parents);
            return _client.GetEvents(stream, start, count);
        }


        public Task<IFullEvent[]> GetEventsBackwards<TEntity>(string bucket, Id streamId, Id[] parents, long? start = null, int? count = null) where TEntity : IEntity
        {
            var stream = _generator(_registrar.GetVersionedName(typeof(TEntity)), StreamTypes.Domain, bucket, streamId, parents);
            return _client.GetEventsBackwards(stream, start, count);
        }
        public async Task<ISnapshot> GetSnapshot<TEntity>(string bucket, Id streamId, Id[] parents) where TEntity : IEntity
        {
            var stream = _generator(_registrar.GetVersionedName(typeof(TEntity)), StreamTypes.Snapshot, bucket, streamId, parents);
            var @event = (await _client.GetEventsBackwards(stream, count: 1).ConfigureAwait(false)).FirstOrDefault();
            return new Snapshot
            {
                EntityType = @event.Descriptor.EntityType,
                Bucket = @event.Descriptor.Bucket,
                StreamId = @event.Descriptor.StreamId,
                Timestamp = @event.Descriptor.Timestamp,
                Version = @event.Descriptor.Version,
                Payload = @event.Event
            };
        }
        public Task WriteSnapshot<TEntity>(ISnapshot snapshot, IDictionary<string, string> commitHeaders) where TEntity : IEntity
        {
            var stream = _generator(_registrar.GetVersionedName(typeof(TEntity)), StreamTypes.Snapshot, snapshot.Bucket, snapshot.StreamId, snapshot.Parents.Select(x => x.StreamId).ToArray());
            var e = new FullEvent
            {
                Descriptor = new EventDescriptor
                {
                    EntityType = snapshot.EntityType,
                    StreamType = StreamTypes.Snapshot,
                    Bucket = snapshot.Bucket,
                    StreamId = snapshot.StreamId,
                    Parents = snapshot.Parents,
                    Timestamp = DateTime.UtcNow,
                    Version = snapshot.Version,
                    Headers = new Dictionary<string, string>(),
                    CommitHeaders = commitHeaders
                },
                Event = snapshot.Payload as IState,
            };

            return _client.WriteEvents(stream, new[] { e }, commitHeaders);
        }

        public async Task<bool> VerifyVersion<TEntity>(string bucket, Id streamId, Id[] parents,
            long expectedVersion) where TEntity : IEntity
        {
            var size = await Size<TEntity>(bucket, streamId, parents).ConfigureAwait(false);
            return size == expectedVersion;
        }


        public Task<long> Size<TEntity>(string bucket, Id streamId, Id[] parents) where TEntity : IEntity
        {
            var stream = _generator(_registrar.GetVersionedName(typeof(TEntity)), StreamTypes.Domain, bucket, streamId, parents);
            return _client.Size(stream);
        }

        public Task<long> WriteEvents<TEntity>(string bucket, Id streamId, Id[] parents, IFullEvent[] events, IDictionary<string, string> commitHeaders, long? expectedVersion = null) where TEntity : IEntity
        {
            var stream = _generator(_registrar.GetVersionedName(typeof(TEntity)), StreamTypes.Domain, bucket, streamId, parents);
            return _client.WriteEvents(stream, events, commitHeaders, expectedVersion);
        }



        public Task WriteMetadata<TEntity>(string bucket, Id streamId, Id[] parents, long? maxCount = null, long? truncateBefore = null, TimeSpan? maxAge = null,
            TimeSpan? cacheControl = null, bool force = false, IDictionary<string, string> custom = null) where TEntity : IEntity
        {
            var stream = _generator(_registrar.GetVersionedName(typeof(TEntity)), StreamTypes.Domain, bucket, streamId, parents);
            return _client.WriteMetadata(stream, maxCount, truncateBefore, maxAge, cacheControl, force, custom);
        }

        public Task<string> GetMetadata<TEntity>(string bucket, Id streamId, Id[] parents, string key) where TEntity : IEntity
        {
            var stream = _generator(_registrar.GetVersionedName(typeof(TEntity)), StreamTypes.Domain, bucket, streamId, parents);
            return _client.GetMetadata(stream, key);
        }

    }
}
