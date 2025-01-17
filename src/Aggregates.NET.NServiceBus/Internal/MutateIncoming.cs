﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Aggregates.Contracts;
using Aggregates.Extensions;
using Microsoft.Extensions.Logging;
using NServiceBus;
using NServiceBus.Pipeline;

namespace Aggregates.Internal
{
    public class MutateIncoming : Behavior<IIncomingLogicalMessageContext>
    {
        private readonly Configure _settings;
        private readonly ILogger Logger;
        
        public MutateIncoming(ILoggerFactory logFactory, Configure settings)
        {
            Logger = logFactory.CreateLogger("MutateIncoming");
            _settings = settings;
        }
        
        public override Task Invoke(IIncomingLogicalMessageContext context, Func<Task> next)
        {
            if (context.GetMessageIntent() == MessageIntentEnum.Reply)
                return next();

            IMutating mutated = new Mutating(context.Message.Instance, context.Headers ?? new Dictionary<string, string>());

            var mutators = MutationManager.Registered.ToList();
            if (!mutators.Any()) return next();

            IContainer container;
            if (!context.Extensions.TryGet<IContainer>(out container))
                container = _settings.Container.GetChildContainer();

            foreach (var type in mutators)
            {
                try
                {
                    var mutator = (IMutate)container.Resolve(type);
                    mutated = mutator.MutateIncoming(mutated);
                }
                catch(Exception e)
                {
                    Logger.WarnEvent("MutateFailure", e, "Failed to run mutator {Mutator}", type.FullName);
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
