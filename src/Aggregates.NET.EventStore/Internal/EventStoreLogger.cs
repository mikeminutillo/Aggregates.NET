using Microsoft.Extensions.Logging;
using System;
using Aggregates.Extensions;
using System.Collections.Generic;
using System.Text;

namespace Aggregates.Internal
{
    class EventStoreLogger : EventStore.ClientAPI.ILogger
    {
        private readonly ILogger _logger;
        public EventStoreLogger(ILogger<EventStoreLogger> logger)
        {
            _logger = logger;
        }
        public void Debug(string format, params object[] args)
        {
            try
            {
                if (args.Length == 0)
                    _logger.DebugEvent("EventStore", format);
                else
                    _logger.DebugEvent("EventStore", format, args);
            }
            catch { }
        }

        public void Debug(Exception ex, string format, params object[] args)
        {
            try
            {
                if (args.Length == 0)
                    _logger.DebugEvent("EventStore", ex, format);
                else
                    _logger.DebugEvent("EventStore", ex, format, args);
            }
            catch { }
        }

        public void Error(string format, params object[] args)
        {
            try
            {
                if (args.Length == 0)
                    _logger.ErrorEvent("EventStore", format);
                else
                    _logger.ErrorEvent("EventStore", format, args);
            }
            catch { }
        }

        public void Error(Exception ex, string format, params object[] args)
        {
            try
            {
                if (args.Length == 0)
                    _logger.ErrorEvent("EventStore", ex, format);
                else
                    _logger.ErrorEvent("EventStore", ex, format, args);
            }
            catch { }
        }

        public void Info(string format, params object[] args)
        {
            try
            {
                if (args.Length == 0)
                    _logger.InfoEvent("EventStore", format);
                else
                    _logger.InfoEvent("EventStore", format, args);
            }
            catch { }
        }

        public void Info(Exception ex, string format, params object[] args)
        {
            try
            {
                if (args.Length == 0)
                    _logger.InfoEvent("EventStore", ex, format);
                else
                    _logger.InfoEvent("EventStore", ex, format, args);
            }
            catch { }
        }
    }
}
