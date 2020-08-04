using System;
using System.Collections.Generic;
using System.Text;

namespace Aggregates.UnitOfWork.Application
{
    public class UnitOfWork : Aggregates.UnitOfWork.UnitOfWork, IApplication
    {
        public dynamic Bag { get; private set; }
    }
}
