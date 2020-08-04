using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates.UnitOfWork
{
    interface IUnitOfWork
    {
        Task Begin();
        Task End(Exception ex = null);
    }
}
