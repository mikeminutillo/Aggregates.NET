using Aggregates.UnitOfWork;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aggregates.Contracts
{
    public interface ITrackChildren
    {
        Task Setup(string endpoint, Version version);
        Task<TEntity[]> GetChildren<TEntity, TParent>(IDomain uow, TParent parent) where TEntity : IChildEntity<TParent> where TParent : IHaveEntities<TParent>;
    }
}
