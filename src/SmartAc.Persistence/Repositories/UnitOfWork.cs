using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using SmartAc.Domain.Abstractions;
using System.Data;

namespace SmartAc.Persistence.Repositories;

internal sealed class UnitOfWork : IUnitOfWork
{
    private readonly SmartAcContext _context;

    public UnitOfWork(SmartAcContext context) => _context = context;

    public async Task<IDbTransaction> BeginTransactionAsync(
        IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        var transaction = await _context.Database.BeginTransactionAsync(isolationLevel, cancellationToken);
        return transaction.GetDbTransaction();
    }

    public Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
        => _context.SaveChangesAsync(cancellationToken);
}