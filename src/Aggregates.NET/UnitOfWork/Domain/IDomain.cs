using Aggregates.Contracts;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aggregates.UnitOfWork.Domain
{
    interface IDomain
    {
        IRepository<T> For<T>() where T : IEntity;
        IRepository<TEntity, TParent> For<TEntity, TParent>(TParent parent) where TEntity : IChildEntity<TParent> where TParent : IHaveEntities<TParent>;

        Guid CommitId { get; }
    }
}
