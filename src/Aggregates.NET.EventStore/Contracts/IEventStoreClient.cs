using EventStore.ClientAPI;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates.Contracts
{
    interface IEventStoreClient
    {
        public delegate Task EventAppeared(string eventStream, long eventNumber, IFullEvent @event);
        public enum Status
        {
            Disconnected,
            Connected,
            Reconnecting,
            Closed
        }
        bool Connected { get; }


        Task Connect();
        Task Close();

        Task<bool> SubscribeToStreamStart(string stream, IEventStoreClient.EventAppeared callback);
        Task<bool> SubscribeToStreamEnd(string stream, IEventStoreClient.EventAppeared callback);

        Task<bool> CreateProjection(string name, string definition);
        Task<bool> EnableProjection(string name);
        Task<bool> ConnectPinnedPersistentSubscription(string stream, string group, EventAppeared callback);

        Task<IFullEvent[]> GetEventsBackwards(string stream, long? start = null, int? count = null);
        Task<IFullEvent[]> GetEvents(string stream, long? start = null, int? count = null);
        Task<bool> VerifyVersion(string stream, long expectedVersion);
        Task<long> WriteEvents(string stream, IFullEvent[] events, IDictionary<string, string> commitHeaders, long? expectedVersion = null);
        Task<long> Size(string stream);
        Task WriteMetadata(string stream, long? maxCount = null, long? truncateBefore = null, TimeSpan? maxAge = null, TimeSpan? cacheControl = null, bool force = false, IDictionary<string, string> custom = null);
        Task<string> GetMetadata(string stream, string key);
    }
}
