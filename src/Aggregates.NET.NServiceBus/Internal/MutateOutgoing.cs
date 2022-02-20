﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Aggregates.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NServiceBus;
using NServiceBus.Pipeline;

namespace Aggregates.Internal
{
    public class MutateOutgoing : Behavior<IOutgoingLogicalMessageContext>
    {
        private readonly ILogger Logger;

        public MutateOutgoing(ILogger<MutateOutgoing> logger)
        {
            Logger = logger;
        }
        public override Task Invoke(IOutgoingLogicalMessageContext context, Func<Task> next)
        {
            // Set aggregates.net message and corr id
            if (context.Headers.ContainsKey(Headers.MessageId))
                context.Headers[$"{Defaults.PrefixHeader}.{Defaults.MessageIdHeader}"] = context.Headers[Headers.MessageId];
            if(context.Headers.ContainsKey(Headers.CorrelationId))
                context.Headers[$"{Defaults.PrefixHeader}.{Defaults.CorrelationIdHeader}"] = context.Headers[Headers.CorrelationId];

            if (context.GetMessageIntent() == MessageIntentEnum.Reply)
                return next();


            // gets the child provider
            var provider = context.Extensions.Get<IServiceProvider>();
            var mutators = provider.GetServices<Func<IMutate>>();

            IMutating mutated = new Mutating(context.Message.Instance, context.Headers ?? new Dictionary<string, string>());

            if (!mutators.Any()) return next();


            foreach (var mutator in mutators)
            {
                try
                {
                    mutated = mutator().MutateOutgoing(mutated);
                }
                catch (Exception ex)
                {
                    Logger.WarnEvent("MutateFailure", ex, "Failed to run mutator {Mutator}", mutator.GetType().FullName);
                }
            }
            
            foreach (var header in mutated.Headers)
                context.Headers[header.Key] = header.Value;
            context.UpdateMessage(mutated.Message);

            return next();
        }
    }
    [ExcludeFromCodeCoverage]
    internal class MutateOutgoingRegistration : RegisterStep
    {
        public MutateOutgoingRegistration() : base(
            stepId: "MutateOutgoing",
            behavior: typeof(MutateOutgoing),
            description: "runs mutators on outgoing messages"
        )
        {
            InsertAfter("MutateOutgoingMessages");
        }
    }

}
