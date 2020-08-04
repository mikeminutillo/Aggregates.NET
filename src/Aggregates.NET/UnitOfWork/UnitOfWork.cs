using Aggregates.Contracts;
using Aggregates.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates.UnitOfWork
{
    public abstract class UnitOfWork : IUnitOfWork, IMutate
    {
        internal static readonly ILog Logger = LogProvider.GetLogger("UnitOfWork");

        public object CurrentMessage { get; internal set; }
        public IDictionary<string, string> CurrentHeaders { get; internal set; }


        protected virtual Task Begin()
        {
            return Task.CompletedTask;
        }
        protected virtual Task End(Exception ex)
        {
            return Task.CompletedTask;
        }

        Task IUnitOfWork.Begin()
        {
            return Begin();
        }
        Task IUnitOfWork.End(Exception ex)
        {
            return End(ex);
        }

        protected virtual IMutating MutateIncoming(IMutating command)
        {
            return command;
        }
        protected virtual IMutating MutateOutgoing(IMutating command)
        {
            return command;
        }

        IMutating IMutate.MutateIncoming(IMutating command)
        {
            CurrentMessage = command.Message;
            CurrentHeaders = new Dictionary<string, string>(command.Headers);


            return MutateIncoming(command);
        }
        IMutating IMutate.MutateOutgoing(IMutating command)
        {
            return MutateOutgoing(command);
        }

    }
}
