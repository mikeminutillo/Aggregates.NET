using Aggregates.Contracts;
using Aggregates.Extensions;
using Aggregates.Messages;
using Microsoft.Extensions.Logging;
using NServiceBus;
using NServiceBus.Pipeline;
using NServiceBus.Transport;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates.Internal
{
    public class ExceptionRejector : Behavior<IIncomingLogicalMessageContext>
    {
        private readonly ILogger Logger;

        private readonly IMetrics _metrics;

        public ExceptionRejector(ILoggerFactory logFactory, ISettings settings, IMetrics metrics)
        {
            Logger = logFactory.CreateLogger("ExceptionRejector");
            _metrics = metrics;

        }

        public override async Task Invoke(IIncomingLogicalMessageContext context, Func<Task> next)
        {
            var messageId = context.MessageId;
            var retries = 0;

            try
            {
                await next().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                // Special exception we dont want to retry or reply
                if (e is BusinessException || context.MessageHandled)
                    return;
                

                // at this point the message has failed, so a THROW will move it to the error queue

                // Only send reply if the message is a SEND, else we risk endless reply loops as message failures bounce back and forth
                if (context.GetMessageIntent() != MessageIntentEnum.Send && context.GetMessageIntent() != MessageIntentEnum.Publish)
                    return;

                // At this point message is dead - should be moved to error queue, send message to client that their request was rejected due to error 
                _metrics.Mark("Message Faults", Unit.Errors);

                Logger.ErrorEvent("Fault", e, "[{MessageId:l}] has failed {Retries} times\n{@Headers}\n{ExceptionType} - {ExceptionMessage}", messageId, retries, context.MessageHeaders, e.GetType().Name, e.Message);
                // Only need to reply if the client expects it
                if (!context.MessageHeaders.ContainsKey(Defaults.RequestResponse) || context.MessageHeaders[Defaults.RequestResponse] != "1")
                    throw;

                // if part of saga be sure to transfer that header
                var replyOptions = new ReplyOptions();
                if (context.MessageHeaders.TryGetValue(Defaults.SagaHeader, out var sagaId))
                    replyOptions.SetHeader(Defaults.SagaHeader, sagaId);

                // Tell the sender the command was not handled due to a service exception
                var rejection = context.Builder.Build<Action<Exception, string, Error>>();
                // Wrap exception in our object which is serializable
                await context.Reply<Error>((message) => rejection(e,
                            $"Rejected message after {retries} attempts!", message), replyOptions)
                        .ConfigureAwait(false);

                // Should be the last throw for this message - if RecoveryPolicy is properly set the message will be sent over to error queue
                throw;

            }
        }
    }
    [ExcludeFromCodeCoverage]
    internal class ExceptionRejectorRegistration : RegisterStep
    {
        public ExceptionRejectorRegistration() : base(
            stepId: "ExceptionRejector",
            behavior: typeof(ExceptionRejector),
            description: "handles exceptions and retries",
            factoryMethod: (b) => new ExceptionRejector(b.Build<ILoggerFactory>(), b.Build<ISettings>(), b.Build<IMetrics>())
        )
        {
            InsertBefore("MutateIncomingMessages");
        }
    }
}
