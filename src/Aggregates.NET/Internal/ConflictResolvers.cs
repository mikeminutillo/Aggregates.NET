﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Aggregates.Exceptions;
using Aggregates.Extensions;
using Aggregates.Messages;
using Microsoft.Extensions.Logging;

namespace Aggregates.Internal
{
    class ConcurrencyStrategy : Enumeration<ConcurrencyStrategy, ConcurrencyConflict>
    {
        public static ConcurrencyStrategy Throw = new ConcurrencyStrategy(ConcurrencyConflict.Throw, "Throw");
        public static ConcurrencyStrategy Ignore = new ConcurrencyStrategy(ConcurrencyConflict.Ignore, "Ignore");
        public static ConcurrencyStrategy Discard = new ConcurrencyStrategy(ConcurrencyConflict.Discard, "Discard");
        public static ConcurrencyStrategy ResolveStrongly = new ConcurrencyStrategy(ConcurrencyConflict.ResolveStrongly, "ResolveStrongly");
        public static ConcurrencyStrategy ResolveWeakly = new ConcurrencyStrategy(ConcurrencyConflict.ResolveWeakly, "ResolveWeakly");
        public static ConcurrencyStrategy Custom = new ConcurrencyStrategy(ConcurrencyConflict.Custom, "Custom");

        public ConcurrencyStrategy(ConcurrencyConflict value, string displayName) : base(value, displayName)
        {
        }

        public IResolveConflicts Build(IContainer container, Type type = null)
        {
            switch (this.Value)
            {
                case ConcurrencyConflict.Throw:
                    return container.Resolve<ThrowConflictResolver>();
                case ConcurrencyConflict.Ignore:
                    return container.Resolve<IgnoreConflictResolver>();
                case ConcurrencyConflict.Discard:
                    return container.Resolve<DiscardConflictResolver>();
                case ConcurrencyConflict.ResolveStrongly:
                    return container.Resolve<ResolveStronglyConflictResolver>();
                case ConcurrencyConflict.ResolveWeakly:
                    return container.Resolve<ResolveWeaklyConflictResolver>();
                case ConcurrencyConflict.Custom:
                    return (IResolveConflicts)container.Resolve(type);
            };
            throw new InvalidOperationException($"Unknown conflict resolver: {this.Value}");
        }
    }

    internal class ThrowConflictResolver : IResolveConflicts
    {
        public Task Resolve<TEntity, TState>(TEntity entity, Guid commitId, IDictionary<string, string> commitHeaders) where TEntity : IEntity<TState> where TState : class, IState, new()
        {
            throw new ConflictResolutionFailedException(typeof(TEntity), entity.Bucket, entity.Id, entity.GetParentIds(), "No conflict resolution attempted");
        }
    }

    /// <summary>
    /// Conflict from the store is ignored, events will always be written
    /// </summary>
    internal class IgnoreConflictResolver : IResolveConflicts
    {
        internal readonly ILogger Logger;

        private readonly IStoreEvents _store;
        private readonly IOobWriter _oobStore;

        public IgnoreConflictResolver(ILoggerFactory factory, IStoreEvents store, IOobWriter oobStore)
        {
            _store = store;
            _oobStore = oobStore;
            Logger = factory.CreateLogger<IgnoreConflictResolver>();
        }

        public async Task Resolve<TEntity, TState>(TEntity entity, Guid commitId, IDictionary<string, string> commitHeaders) where TEntity : IEntity<TState> where TState : class, IState, new()
        {
            var state = entity.State;

            Logger.DebugEvent("Resolver", "Resolving {Events} conflicting events to stream [{Stream:l}] type [{EntityType:l}] bucket [{Bucket:l}]", entity.Uncommitted.Count(), entity.Id, typeof(TEntity).FullName, entity.Bucket);

            var domainEvents = entity.Uncommitted.Where(x => x.Descriptor.StreamType == StreamTypes.Domain).ToArray();
            var oobEvents = entity.Uncommitted.Where(x => x.Descriptor.StreamType == StreamTypes.OOB).ToArray();

            await _store.WriteEvents<TEntity>(entity.Bucket, entity.Id, entity.GetParentIds(), domainEvents, commitHeaders).ConfigureAwait(false);
            await _oobStore.WriteEvents<TEntity>(entity.Bucket, entity.Id, entity.GetParentIds(), oobEvents, commitId, commitHeaders).ConfigureAwait(false);
        }
    }
    /// <summary>
    /// Conflicted events are discarded
    /// </summary>
    internal class DiscardConflictResolver : IResolveConflicts
    {
        internal readonly ILogger Logger;
        public DiscardConflictResolver(ILoggerFactory factory)
        {
            Logger = factory.CreateLogger<DiscardConflictResolver>();
        }

        public Task Resolve<TEntity, TState>(TEntity entity, Guid commitId, IDictionary<string, string> commitHeaders) where TEntity : IEntity<TState> where TState : class, IState, new()
        {
            Logger.DebugEvent("Resolver", "Discarding {Events} conflicting events to stream [{Stream:l}] type [{EntityType:l}] bucket [{Bucket:l}]", entity.Uncommitted.Count(), entity.Id, typeof(TEntity).FullName, entity.Bucket);

            return Task.CompletedTask;
        }
    }
    /// <summary>
    /// Pull latest events from store, merge into stream and re-commit
    /// </summary>
    internal class ResolveStronglyConflictResolver : IResolveConflicts
    {
        internal readonly ILogger Logger;

        private readonly IStoreEntities _store;

        public ResolveStronglyConflictResolver(ILoggerFactory factory, IStoreEntities store)
        {
            _store = store;
            Logger = factory.CreateLogger<ResolveStronglyConflictResolver>();
        }

        public async Task Resolve<TEntity, TState>(TEntity entity, Guid commitId, IDictionary<string, string> commitHeaders) where TEntity : IEntity<TState> where TState : class, IState, new()
        {
            Logger.DebugEvent("Resolver", "Resolving {Events} conflicting events to stream [{Stream:l}] type [{EntityType:l}] bucket [{Bucket:l}]", entity.Uncommitted.Count(), entity.Id, typeof(TEntity).FullName, entity.Bucket);

            // Get the latest clean entity
            var latestEntity = await _store.Get<TEntity, TState>(entity.Bucket, entity.Id, entity is IChildEntity ? (entity as IChildEntity).Parent : null).ConfigureAwait(false);

            Logger.DebugEvent("Behind", "Stream is {Count} events behind store", latestEntity.Version - entity.Version);

            var state = latestEntity.State;
            try
            {
                foreach (var u in entity.Uncommitted)
                {
                    if (u.Descriptor.StreamType == StreamTypes.Domain)
                    {
                        try
                        {
                            latestEntity.Conflict(u.Event as IEvent);
                        }
                        catch (DiscardEventException)
                        {
                            // event should be discarded
                        }
                    }
                    else if (u.Descriptor.StreamType == StreamTypes.OOB)
                    {
                        // Todo: small hack
                        string id = "";
                        bool transient = true;
                        int daysToLive = -1;

                        id = u.Descriptor.Headers[Defaults.OobHeaderKey];

                        if (u.Descriptor.Headers.ContainsKey(Defaults.OobTransientKey))
                            bool.TryParse(u.Descriptor.Headers[Defaults.OobTransientKey], out transient);
                        if (u.Descriptor.Headers.ContainsKey(Defaults.OobDaysToLiveKey))
                            int.TryParse(u.Descriptor.Headers[Defaults.OobDaysToLiveKey], out daysToLive);

                        latestEntity.Raise(u.Event as IEvent, id, transient, daysToLive);
                    }
                }
            }
            catch (NoRouteException e)
            {
                Logger.WarnEvent("ResolveFailure", e, "Failed to resolve conflict: {ExceptionType} - {ExceptionMessage}", e.GetType().Name, e.Message);
                throw new ConflictResolutionFailedException(typeof(TEntity), entity.Bucket, entity.Id, entity.GetParentIds(), "Failed to resolve conflict", e);
            }

            await _store.Commit<TEntity, TState>(latestEntity, commitId, commitHeaders).ConfigureAwait(false);
        }
    }

    [Versioned("ConflictingEvents", "Aggregates", 1)]
    internal class ConflictingEvents : IMessage
    {
        public string EntityType { get; set; }
        public string Bucket { get; set; }
        public Id StreamId { get; set; }
        public IEnumerable<Tuple<string, Id>> Parents { get; set; }

        public IEnumerable<IFullEvent> Events { get; set; }
    }
    /// <summary>
    /// Save conflicts for later processing, can only be used if the stream can never fail to merge
    /// </summary>
    internal class ResolveWeaklyConflictResolver :
        IResolveConflicts
    {
        internal readonly ILogger Logger;

        private readonly IStoreEvents _eventstore;
        private readonly IDelayedChannel _delay;


        public ResolveWeaklyConflictResolver(IStoreEvents eventstore, IDelayedChannel delay, ILoggerFactory factory)
        {
            _eventstore = eventstore;
            _delay = delay;
            Logger = factory.CreateLogger<ResolveWeaklyConflictResolver>();
        }


        public Task Resolve<TEntity, TState>(TEntity entity, Guid commitId, IDictionary<string, string> commitHeaders) where TEntity : IEntity<TState> where TState : class, IState, new()
        {
            throw new NotImplementedException();
        }
    }




}
