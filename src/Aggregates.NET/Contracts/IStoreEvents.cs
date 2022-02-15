using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Aggregates.Messages;

namespace Aggregates.Contracts
{
    public interface IStoreEvents
    {
        Task<IFullEvent[]> GetEvents<TEntity>(string bucket, Id streamId, Id[] parents, long? start = null, int? count = null) where TEntity : IEntity;
        Task<IFullEvent[]> GetEventsBackwards<TEntity>(string bucket, Id streamId, Id[] parents, long? start = null, int? count = null) where TEntity : IEntity;
        Task<ISnapshot> GetSnapshot<TEntity>(string bucket, Id streamId, Id[] parents) where TEntity : IEntity;
        Task WriteSnapshot<TEntity>(ISnapshot snapshot, IDictionary<string, string> commitHeaders) where TEntity : IEntity;

        Task<bool> VerifyVersion<TEntity>(string bucket, Id streamId, Id[] parents, long expectedVersion) where TEntity : IEntity;
        /// <summary>
        /// Returns the next expected version of the stream
        /// </summary>
        Task<long> WriteEvents<TEntity>(string bucket, Id streamId, Id[] parents, IFullEvent[] events, IDictionary<string, string> commitHeaders, long? expectedVersion = null) where TEntity : IEntity;

        Task<long> Size<TEntity>(string bucket, Id streamId, Id[] parents) where TEntity : IEntity;

        Task WriteMetadata<TEntity>(string bucket, Id streamId, Id[] parents, long? maxCount = null, long? truncateBefore = null, TimeSpan? maxAge = null, TimeSpan? cacheControl = null, bool force = false, IDictionary<string, string> custom = null) where TEntity : IEntity;

        Task<string> GetMetadata<TEntity>(string bucket, Id streamId, Id[] parents, string key) where TEntity : IEntity;
        
    }
}
