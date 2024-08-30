using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SmartAc.Application.Abstractions.Messaging;
using SmartAc.Domain.Abstractions;
using System.Data;

namespace SmartAc.Application.PipelineBehaviors;

internal sealed class UnitOfWorkBehavior<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse>
    where TRequest : class, ICommand<TResponse>
{
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<UnitOfWorkBehavior<TRequest, TResponse>> _logger;

    public UnitOfWorkBehavior(
        IUnitOfWork unitOfWork,
        ILogger<UnitOfWorkBehavior<TRequest, TResponse>> logger)
    {
        _unitOfWork = unitOfWork;
        _logger = logger;
    }

    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken)
    {
        //using var transactionScope = new TransactionScope(TransactionScopeOption.Required,
        //    new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted });
        using var transaction = 
            await _unitOfWork.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken);

        try
        {
            var response = await next();
            await _unitOfWork.SaveChangesAsync(cancellationToken);
            transaction.Commit();
            return response;
        }
        catch (DbUpdateException e)
        {
            transaction.Rollback();
            _logger.LogError(e, e.Message);
            throw;
        }
    }
}