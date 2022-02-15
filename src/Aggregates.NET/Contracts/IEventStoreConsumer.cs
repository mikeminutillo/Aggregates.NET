using Aggregates.Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aggregates.Contracts
{
    public interface IEventStoreConsumer
    {
        public delegate Task EventAppeared(IEvent @event, IDictionary<string, string> headers);

        Task SetupProjection(string endpoint, Version version, Type[] eventTypes);
        Task ConnectToProjection(string endpoint, Version version, EventAppeared callback);
    }
}
