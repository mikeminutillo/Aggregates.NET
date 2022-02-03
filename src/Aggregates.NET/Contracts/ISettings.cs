using System;
using System.Collections.Generic;
using System.Text;

namespace Aggregates.Contracts
{
    public interface ISettings
    {
        IServiceProvider ServiceProvider { get; }
        Version EndpointVersion { get; }
        Version AggregatesVersion { get; }

        // Log settings
        TimeSpan? SlowAlertThreshold { get; }
        bool ExtraStats { get; }

        // Data settings
        StreamIdGenerator Generator { get; }
        int ReadSize { get; }
        Compression Compression { get;}

        // Messaging settings
        string Endpoint { get;  }
        string UniqueAddress { get;  }
        int Retries { get;  }
        int ParallelMessages { get;  }
        int ParallelEvents { get; }
        int MaxConflictResolves { get; }

        // Delayed cache settings
        int FlushSize { get; }
        TimeSpan FlushInterval { get;  }
        TimeSpan DelayedExpiration { get; }
        int MaxDelayed { get; }

        bool AllEvents { get; }
        bool Passive { get; }
        bool TrackChildren { get; }

        // Disable certain "production" features related to versioning 
        bool DevelopmentMode { get; }

        string CommandDestination { get; }

        string MessageContentType { get; }
    }
}
