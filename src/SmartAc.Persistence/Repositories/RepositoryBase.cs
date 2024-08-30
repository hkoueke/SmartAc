using Microsoft.EntityFrameworkCore;
using SmartAc.Domain.Abstractions;
using System.Linq.Expressions;

namespace SmartAc.Persistence.Repositories;

internal abstract class RepositoryBase<TEntity> : IRepository<TEntity> where TEntity : class, IAggregateRoot
{
    private readonly SmartAcContext _dbContext;

    protected RepositoryBase(SmartAcContext dbContext) => _dbContext = dbContext;

    public virtual void Add(TEntity entity) => _dbContext.Add(entity);

    public virtual void AddRange(IEnumerable<TEntity> entities) => _dbContext.AddRange(entities);

    public virtual void Remove(TEntity entity) => _dbContext.Remove(entity);

    public virtual void RemoveRange(IEnumerable<TEntity> entities) => _dbContext.RemoveRange(entities);

    public virtual void Update(TEntity entity) => _dbContext.Update(entity);

    public virtual async Task<IEnumerable<TEntity>> FindAllAsync(
        Expression<Func<TEntity, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        return await _dbContext.Set<TEntity>().Where(predicate).ToListAsync(cancellationToken);
    }

    public virtual IQueryable<TEntity> GetQueryable() => _dbContext.Set<TEntity>().AsQueryable();

    public virtual ValueTask<TEntity?> FindAsync(object id, CancellationToken cancellationToken = default)
        => _dbContext.Set<TEntity>().FindAsync(new[] { id }, cancellationToken: cancellationToken);
}
