using Aggregates.Contracts;
using Aggregates.Exceptions;
using Aggregates.Extensions;
using Aggregates.Internal;
using Aggregates.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aggregates
{
    public class Configuration : IConfiguration
    {

        public IServiceProvider ServiceProvider { get; internal set; }

        public bool Setup => Settings != null;
        public ISettings Settings { get; internal set; }

        public async Task Start(IServiceProvider serviceProvider)
        {
            if (Settings == null)
                throw new InvalidOperationException("Settings must be built");

            ServiceProvider = serviceProvider;

            try
            {
                await Configure.SetupTasks.WhenAllAsync(x => x(serviceProvider, Settings)).ConfigureAwait(false);
            }
            catch
            {
                Settings = null;
                throw;
            }
        }


        public async static Task<IConfiguration> Build(IServiceCollection serviceCollection, Action<Configure> settings)
        {
            if (serviceCollection == null)
                throw new ArgumentException("Must designate the service collection");

            var config = new Configure();
            settings(config);


            var aggConfig = new Configuration();

            aggConfig.Settings = config;

            try
            {
                serviceCollection.AddSingleton<ISettings>(config);
                serviceCollection.AddSingleton<IConfiguration>(aggConfig);
                await Configure.RegistrationTasks.WhenAllAsync(x => x(serviceCollection, config)).ConfigureAwait(false);
            }
            catch
            {
                throw;
            }
            return aggConfig;
        }
    }


    public class Configure : ISettings
    {
        public IServiceProvider ServiceProvider { get; internal set; }

        public Version EndpointVersion { get; private set; }
        public Version AggregatesVersion { get; private set; }

        // Log settings
        public TimeSpan? SlowAlertThreshold { get; internal set; }
        public bool ExtraStats { get; internal set; }

        // Data settings
        public StreamIdGenerator Generator { get; internal set; }
        public int ReadSize { get; internal set; }
        public Compression Compression { get; internal set; }

        // Messaging settings
        public string Endpoint { get; internal set; }
        public string UniqueAddress { get; internal set; }
        public int Retries { get; internal set; }
        public int ParallelMessages { get; internal set; }
        public int ParallelEvents { get; internal set; }
        public int MaxConflictResolves { get; internal set; }

        // Delayed cache settings
        public int FlushSize { get; internal set; }
        public TimeSpan FlushInterval { get; internal set; }
        public TimeSpan DelayedExpiration { get; internal set; }
        public int MaxDelayed { get; internal set; }

        public bool AllEvents { get; internal set; }
        public bool Passive { get; internal set; }
        public bool TrackChildren { get; internal set; }

        // Disable certain "production" features related to versioning 
        public bool DevelopmentMode { get; internal set; }

        public string CommandDestination { get; internal set; }

        public string MessageContentType { get; internal set; }

        internal static List<Func<IServiceCollection, ISettings, Task>> RegistrationTasks;
        internal static List<Func<IServiceProvider, ISettings, Task>> SetupTasks;
        internal static List<Func<IServiceProvider, ISettings, Task>> StartupTasks;
        internal static List<Func<IServiceProvider, ISettings, Task>> ShutdownTasks;
                
        public Configure()
        {
            EndpointVersion = Assembly.GetEntryAssembly()?.GetName().Version ?? new Version(0, 0, 0);
            AggregatesVersion = Assembly.GetExecutingAssembly()?.GetName().Version ?? new Version(0, 0, 0);

            RegistrationTasks = new List<Func<IServiceCollection, ISettings, Task>>();
            SetupTasks = new List<Func<IServiceProvider, ISettings, Task>>();
            StartupTasks = new List<Func<IServiceProvider, ISettings, Task>>();
            ShutdownTasks = new List<Func<IServiceProvider, ISettings, Task>>();

            Endpoint = "demo";
            // Set sane defaults
            Generator = new StreamIdGenerator((type, streamType, bucket, stream, parents) => $"{streamType}-{bucket}-[{parents.BuildParentsString()}]-{type}-{stream}");
            ReadSize = 100;
            Compression = Compression.None;
            UniqueAddress = Guid.NewGuid().ToString("N");
            Retries = 10;
            ParallelMessages = 10;
            ParallelEvents = 10;
            MaxConflictResolves = 3;
            FlushSize = 500;
            FlushInterval = TimeSpan.FromMinutes(1);
            DelayedExpiration = TimeSpan.FromMinutes(5);
            MaxDelayed = 5000;
            MessageContentType = "";

            RegistrationTasks.Add((container, settings) =>
            {

                container.AddSingleton<IRandomProvider>(new RealRandomProvider());
                container.AddSingleton<ITimeProvider>(new RealTimeProvider());

                // Provide a "default" logger so user doesnt need to provide if they dont want to
                container.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);

                container.AddTransient<IProcessor, Processor>();
                container.AddSingleton<IVersionRegistrar, VersionRegistrar>();

                if (!settings.Passive)
                {
                    // A library which managing UOW needs to register the domain unit of work. 
                    // DI containers are designed to append registrations if multiple are present
                    //container.Register<UnitOfWork.IDomain, Internal.UnitOfWork>(Lifestyle.UnitOfWork);

                    container.AddScoped<IDelayedChannel, DelayedChannel>();
                    container.AddTransient<IRepositoryFactory, RepositoryFactory>();
                    container.AddSingleton<IStoreSnapshots, StoreSnapshots>();
                    container.AddSingleton<ISnapshotReader, SnapshotReader>();
                    container.AddSingleton<IOobWriter, OobWriter>();
                    container.AddSingleton<IStoreEntities, StoreEntities>();
                    container.AddSingleton<IDelayedCache, DelayedCache>();

                    // todo: multiple interfaces are not supported (could be wrong actually GetServices exists)
                    container.AddSingleton<IEventSubscriber, EventSubscriber>();
                    container.AddSingleton<IEventSubscriber, DelayedSubscriber>();
                    container.AddSingleton<IEventSubscriber, EventSubscriber>();

                    container.AddSingleton<ITrackChildren, TrackChildren>();

                }
                container.AddSingleton<IMetrics, NullMetrics>();

                container.AddSingleton<StreamIdGenerator>(Generator);

                container.AddSingleton<Action<Exception, string, Error>>((ex, error, message) =>
                {
                    message.Message = $"{message} - {ex.GetType().Name}: {ex.Message}";
                    message.Trace = ex.AsString();
                });

                container.AddSingleton<Action<Accept>>((_) =>
                {
                });

                container.AddSingleton<Action<BusinessException, Reject>>((ex, message) =>
                {
                    message.Exception = ex;
                    message.Message= $"{ex.GetType().Name} - {ex.Message}";
                });

                return Task.CompletedTask;
            });
            StartupTasks.Add((container, settings) =>
            {
                return Task.CompletedTask;
            });
        }
        public Configure SetEndpointName(string endpoint)
        {
            Endpoint = endpoint;
            return this;
        }
        public Configure SetSlowAlertThreshold(TimeSpan? threshold)
        {
            SlowAlertThreshold = threshold;
            return this;
        }
        public Configure SetExtraStats(bool extra)
        {
            ExtraStats = extra;
            return this;
        }
        public Configure SetStreamIdGenerator(StreamIdGenerator generator)
        {
            Generator = generator;
            return this;
        }
        public Configure SetReadSize(int readsize)
        {
            ReadSize = readsize;
            return this;
        }
        public Configure SetCompression(Compression compression)
        {
            Compression = compression;
            return this;
        }
        public Configure SetUniqueAddress(string address)
        {
            UniqueAddress = address;
            return this;
        }
        public Configure SetRetries(int retries)
        {
            Retries = retries;
            return this;
        }
        public Configure SetParallelMessages(int parallel)
        {
            ParallelMessages = parallel;
            return this;
        }
        public Configure SetParallelEvents(int parallel)
        {
            ParallelEvents = parallel;
            return this;
        }
        public Configure SetMaxConflictResolves(int attempts)
        {
            MaxConflictResolves = attempts;
            return this;
        }
        public Configure SetFlushSize(int size)
        {
            FlushSize = size;
            return this;
        }
        public Configure SetFlushInterval(TimeSpan interval)
        {
            FlushInterval = interval;
            return this;
        }
        public Configure SetDelayedExpiration(TimeSpan expiration)
        {
            DelayedExpiration = expiration;
            return this;
        }
        public Configure SetMaxDelayed(int max)
        {
            MaxDelayed = max;
            return this;
        }
        public Configure SetCommandDestination(string destination)
        {
            CommandDestination = destination;
            return this;
        }
        /// <summary>
        /// Passive means the endpoint doesn't need a unit of work, it won't process events or commands
        /// </summary>
        /// <returns></returns>
        public Configure SetPassive()
        {
            Passive = true;
            return this;
        }
        public Configure ReceiveAllEvents()
        {
            AllEvents = true;
            return this;
        }
        public Configure SetTrackChildren(bool track = true)
        {
            TrackChildren = track;
            return this;
        }
        public Configure SetDevelopmentMode(bool mode = true)
        {
            DevelopmentMode = mode;
            return this;
        }


        public Configure AddMetrics<TImplementation>() where TImplementation : class, IMetrics
        {
            RegistrationTasks.Add((container, settings) =>
            {
                container.AddSingleton<IMetrics, TImplementation>();
                return Task.CompletedTask;
            });
            return this;
        }
        public Configure AddLogging(ILoggerFactory factory)
        {
            RegistrationTasks.Add((container, settings) =>
            {
                container.AddSingleton<ILoggerFactory>(factory);
                return Task.CompletedTask;
            });
            return this;
        }
    }
}
