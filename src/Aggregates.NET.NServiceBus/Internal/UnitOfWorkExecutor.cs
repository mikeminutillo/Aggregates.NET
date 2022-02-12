using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Aggregates.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Pipeline;

namespace Aggregates.Internal
{
    public class UnitOfWorkExecutor : Behavior<IIncomingLogicalMessageContext>
    {
        private readonly ILogger Logger;
        private static readonly ConcurrentDictionary<string, dynamic> Bags = new ConcurrentDictionary<string, dynamic>();

        private readonly ISettings _settings;
        private readonly IServiceProvider _provider;
        private readonly IMetrics _metrics;

        public UnitOfWorkExecutor(ILogger<UnitOfWorkExecutor> logger, ISettings settings, IServiceProvider provider, IMetrics metrics)
        {
            Logger = logger;
            _settings = settings;
            _provider = provider;
            _metrics = metrics;
        }

        public override async Task Invoke(IIncomingLogicalMessageContext context, Func<Task> next)
        {
            // Child container with resolved domain and app uow used by downstream
            using (_provider.CreateScope())
            {

                // Only SEND messages deserve a UnitOfWork
                if (context.GetMessageIntent() != MessageIntentEnum.Send && context.GetMessageIntent() != MessageIntentEnum.Publish)
                {
                    await next().ConfigureAwait(false);
                    return;
                }
                if (context.Message.MessageType == typeof(Messages.Accept) || context.Message.MessageType == typeof(Messages.Reject))
                {
                    // If this happens the callback for the message took too long (likely due to a timeout)
                    // normall NSB will report an exception for "No Handlers" - this will just log a warning and ignore
                    Logger.WarnEvent("Overdue", "Overdue Accept/Reject {MessageType} callback - your timeouts might be too short", context.Message.MessageType.FullName);
                    return;
                }

                var domainUOW = _provider.GetRequiredService<Aggregates.UnitOfWork.IDomain>();
                Aggregates.UnitOfWork.IApplication appUOW = null;

                // IUnitOfWork might not be defined by user
                appUOW = _provider.GetService<Aggregates.UnitOfWork.IApplication>();
                if (appUOW != null)
                {
                    appUOW.Bag = new System.Dynamic.ExpandoObject();
                    // if this is a retry pull the bag from the registry
                    if (Bags.TryRemove(context.MessageId, out var bag))
                        appUOW.Bag = bag;
                }

                // Set into the context because DI can be slow
                context.Extensions.Set(domainUOW);
                context.Extensions.Set(appUOW);
                context.Extensions.Set(_settings);
                context.Extensions.Set(_settings.Configuration);

                var commitableUow = domainUOW as Aggregates.UnitOfWork.IUnitOfWork;
                var commitableAppUow = appUOW as Aggregates.UnitOfWork.IUnitOfWork;
                try
                {
                    _metrics.Increment("Messages Concurrent", Unit.Message);
                    using (_metrics.Begin("Message Duration"))
                    {
                        if (context.Message.Instance is Messages.ICommand)
                            await commitableUow.Begin().ConfigureAwait(false);

                        if (commitableAppUow != null)
                            await commitableAppUow.Begin().ConfigureAwait(false);

                        await next().ConfigureAwait(false);

                        if (context.Message.Instance is Messages.ICommand)
                            await commitableUow.End().ConfigureAwait(false);
                        if (commitableAppUow != null)
                            await commitableAppUow.End().ConfigureAwait(false);
                    }

                }
                catch (Exception e)
                {
                    Logger.WarnEvent("UOWException", e, "Received exception while processing message {MessageType}", context.Message.MessageType.FullName);
                    _metrics.Mark("Message Errors", Unit.Errors);
                    var trailingExceptions = new List<Exception>();

                    try
                    {
                        // Todo: if one throws an exception (again) the others wont work.  Fix with a loop of some kind
                        if (context.Message.Instance is Messages.ICommand)
                            await commitableUow.End(e).ConfigureAwait(false);
                        if (appUOW != null)
                        {
                            await commitableAppUow.End(e).ConfigureAwait(false);
                            Bags.TryAdd(context.MessageId, appUOW.Bag);
                        }
                    }
                    catch (Exception endException)
                    {
                        trailingExceptions.Add(endException);
                    }


                    if (trailingExceptions.Any())
                    {
                        trailingExceptions.Insert(0, e);
                        throw new System.AggregateException(trailingExceptions);
                    }
                    throw;

                }
                finally
                {
                    _metrics.Decrement("Messages Concurrent", Unit.Message);
                }
            }
        }
    }
    [ExcludeFromCodeCoverage]
    internal class UowRegistration : RegisterStep
    {
        public UowRegistration() : base(
            stepId: "UnitOfWorkExecution",
            behavior: typeof(UnitOfWorkExecutor),
            description: "Begins and Ends unit of work for your application",
            factoryMethod: (b) => new UnitOfWorkExecutor(b.Build<ILogger<UnitOfWorkExecutor>>(), b.Build<ISettings>(), b.Build<IServiceProvider>(), b.Build<IMetrics>())
        )
        {
            InsertAfterIfExists("FailureReply");
        }
    }
}

