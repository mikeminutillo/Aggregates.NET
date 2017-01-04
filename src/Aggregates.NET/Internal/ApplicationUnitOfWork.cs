﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Aggregates.Extensions;
using Metrics;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Logging;
using NServiceBus.Pipeline;

namespace Aggregates.Internal
{
    internal class ApplicationUnitOfWork : Behavior<IIncomingLogicalMessageContext>
    {
        private static readonly ILog Logger = LogManager.GetLogger("ApplicationUnitOfWork");

        private static readonly Meter MessagesMeter = Metric.Meter("Messages", Unit.Items);
        private static readonly Timer MessagesTimer = Metric.Timer("Message Duration", Unit.Items);
        private static readonly Timer BeginTimer = Metric.Timer("UOW Begin Duration", Unit.Items);
        private static readonly Timer ProcessTimer = Metric.Timer("UOW Process Duration", Unit.Items);
        private static readonly Timer EndTimer = Metric.Timer("UOW End Duration", Unit.Items);
        private static readonly Counter MessagesConcurrent = Metric.Counter("Messages Concurrent", Unit.Items);

        private static readonly Meter ErrorsMeter = Metric.Meter("UOW Errors", Unit.Errors);

        private readonly IPersistence _persistence;

        public ApplicationUnitOfWork(IPersistence persistence)
        {
            _persistence = persistence;
        }

        public override async Task Invoke(IIncomingLogicalMessageContext context, Func<Task> next)
        {
            MessagesConcurrent.Increment();

            var uows = new Stack<IApplicationUnitOfWork>();
            try
            {
                MessagesMeter.Mark();
                using (MessagesTimer.NewContext())
                {
                    using (BeginTimer.NewContext())
                    {
                        foreach (var uow in context.Builder.BuildAll<IApplicationUnitOfWork>())
                        {
                            uow.Builder = context.Builder;

                            int retries;
                            if (!context.Extensions.TryGet(Defaults.Retries, out retries))
                                retries = 0;
                            uow.Retries = retries;

                            var savedBag =
                                await _persistence.Remove(context.MessageId, uow.GetType()).ConfigureAwait(false);

                            uow.Bag = savedBag ?? new ContextBag();
                            Logger.Write(LogLevel.Debug, () => $"Running UOW.Begin for message {context.MessageId} on {uow.GetType().FullName}");
                            await uow.Begin().ConfigureAwait(false);
                            uows.Push(uow);
                        }
                    }

                    using (ProcessTimer.NewContext())
                    {
                        await next().ConfigureAwait(false);
                    }

                    using (EndTimer.NewContext())
                    {
                        foreach (var uow in uows.Generate())
                        {
                            Logger.Write(LogLevel.Debug, () => $"Running UOW.End for message {context.MessageId} on {uow.GetType().FullName}");
                            try
                            {
                                await uow.End().ConfigureAwait(false);
                            }
                            finally
                            {
                                await _persistence.Save(context.MessageId, uow.GetType(), uow.Bag).ConfigureAwait(false);
                            }
                        }
                    }
                    await _persistence.Clear(context.MessageId).ConfigureAwait(false);
                }

            }
            catch (Exception e)
            {
                Logger.Warn(
                    $"Caught exception '{e.GetType().FullName}' while executing message {context.MessageId} {context.Message.MessageType.FullName}");
                ErrorsMeter.Mark();
                var trailingExceptions = new List<Exception>();
                using (EndTimer.NewContext())
                {
                    foreach (var uow in uows.Generate())
                    {
                        try
                        {
                            Logger.Write(LogLevel.Debug,
                                () => $"Running UOW.End with exception [{e.GetType().Name}] for message {context.MessageId} on {uow.GetType().FullName}");
                            await uow.End(e).ConfigureAwait(false);
                        }
                        catch (Exception endException)
                        {
                            trailingExceptions.Add(endException);
                        }
                        await _persistence.Save(context.MessageId, uow.GetType(), uow.Bag).ConfigureAwait(false);
                    }
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
                MessagesConcurrent.Decrement();
            }
        }
    }
    internal class ApplicationUowRegistration : RegisterStep
    {
        public ApplicationUowRegistration() : base(
            stepId: "ApplicationUnitOfWork",
            behavior: typeof(ApplicationUnitOfWork),
            description: "Begins and Ends unit of work for your application"
        )
        {
            InsertAfterIfExists("ExecuteUnitOfWork");
        }
    }
}
