using System.Linq.Expressions;

namespace SmartAc.Domain.Abstractions;

public interface IRepository<T> where T : class, IAggregateRoot
{
    public Task<IEnumerable<T>> FindAllAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default);

    public ValueTask<T?> FindAsync(object id, CancellationToken cancellationToken = default);

    public IQueryable<T> GetQueryable();

    public void Add(T entity);

    public void AddRange(IEnumerable<T> entities);

    public void Update(T entity);

    public void Remove(T entity);

    public void RemoveRange(IEnumerable<T> entities);
}
