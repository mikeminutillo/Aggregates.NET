using System;
using System.Collections.Generic;
using System.Text;

namespace Aggregates.Sagas
{
    [Versioned("StartCommandSaga", "Aggregates", 1)]
    public class StartCommandSaga : Messages.IMessage
    {
        public string SagaId { get; set; }
        public Messages.IMessage Originating { get; set; }
        public Messages.ICommand[] Commands { get; set; }
        public Messages.ICommand[] AbortCommands { get; set; }
    }
    [Versioned("ContinueCommandSaga", "Aggregates", 1)]
    public class ContinueCommandSaga : Messages.IMessage
    {
        public string SagaId { get; set; }
    }
    [Versioned("AbortCommandSaga", "Aggregates", 1)]
    public class AbortCommandSaga : Messages.IMessage
    {
        public string SagaId { get; set; }
    }
}
