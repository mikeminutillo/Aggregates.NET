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
    public class MutateIncoming : Behavior<IIncomingLogicalMessageContext>
    {
        private readonly IServiceProvider _provider;
        private readonly ILogger Logger;
        
        public MutateIncoming(ILoggerFactory logFactory, IServiceProvider provider)
        {
            Logger = logFactory.CreateLogger("MutateIncoming");
            _provider = provider;
        }
        
        public override Task Invoke(IIncomingLogicalMessageContext context, Func<Task> next)
        {
            if (context.GetMessageIntent() == MessageIntentEnum.Reply)
                return next();

            IMutating mutated = new Mutating(context.Message.Instance, context.Headers ?? new Dictionary<string, string>());

            var mutators = _provider.GetServices<IMutate>();
            if (!mutators.Any()) return next();


            foreach (var mutator in mutators)
            {
                try
                {
                    mutated = mutator.MutateIncoming(mutated);
                }
                catch(Exception e)
                {
                    Logger.WarnEvent("MutateFailure", e, "Failed to run mutator {Mutator}", mutator.GetType().FullName);
                }

            }
            
            foreach (var header in mutated.Headers)
                context.Headers[header.Key] = header.Value;
            context.UpdateMessageInstance(mutated.Message);

            return next();
        }
    }
    [ExcludeFromCodeCoverage]
    internal class MutateIncomingRegistration : RegisterStep
    {
        public MutateIncomingRegistration() : base(
            stepId: "MutateIncoming",
            behavior: typeof(MutateIncoming),
            description: "runs mutators on incoming messages"
        )
        {
            InsertAfter("MutateIncomingMessages");
        }
    }

}
