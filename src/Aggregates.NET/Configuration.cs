using Aggregates.Contracts;
using Aggregates.Exceptions;
using Aggregates.Extensions;
using Aggregates.Internal;
using Microsoft.Extensions.DependencyInjection;
using System;
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
                // verify certain agg.net stuff now we have a container
                var uow = serviceProvider.GetService<UnitOfWork.IUnitOfWork>();
                if (uow == null)
                    throw new InvalidOperationException("No unit of work defined, use setting 'Domain' or 'Application' depending on endpoint type");
                // i didnt want to make this interface explicit to avoid the user being able to do `ctx.Uow().End()` in his handlers like a silly
                if (!(uow is UnitOfWork.IBaseUnitOfWork))
                    throw new InvalidOperationException($"Unit of work {uow.GetType().Name} needs to also implement {typeof(UnitOfWork.IBaseUnitOfWork)}");

                await Internal.Settings.SetupTasks.WhenAllAsync(x => x(serviceProvider, Settings)).ConfigureAwait(false);
            }
            catch
            {
                Settings = null;
                throw;
            }
        }


        public async static Task<IConfiguration> Build(IServiceCollection serviceCollection, Action<Settings> settings)
        {
            if (serviceCollection == null)
                throw new ArgumentException("Must designate the service collection");

            var config = new Settings();
            var aggConfig = new Configuration();

            aggConfig.Settings = config;
            config.Configuration = aggConfig;

            settings(config);

            try
            {
                serviceCollection.AddSingleton<ISettings>(config);
                serviceCollection.AddSingleton<IConfiguration>(aggConfig);
                await Internal.Settings.RegistrationTasks.WhenAllAsync(x => x(serviceCollection, config)).ConfigureAwait(false);
            }
            catch
            {
                throw;
            }
            return aggConfig;
        }
    }
}
