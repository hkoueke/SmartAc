using MediatR;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace SmartAc.Application.PipelineBehaviors;

internal sealed class LoggingBehavior<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse>
    where TRequest : notnull
{
    private readonly ILogger<LoggingBehavior<TRequest, TResponse>> _logger;

    public LoggingBehavior(ILogger<LoggingBehavior<TRequest, TResponse>> logger)
    {
        _logger = logger;
    }

    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken)
    {
        var requestName = typeof(TRequest).Name;

        var stopwatch = Stopwatch.StartNew();

        _logger.LogInformation(
            "Starting request '{@RequestName}' at {@DateTimeUtc}.",
            requestName, DateTimeOffset.UtcNow);

        var result = await next();

        _logger.LogInformation(
            "Request '{@RequestName}' completed at {@DateTimeUtc}, in {ElapsedMs} ms.",
            requestName, DateTimeOffset.UtcNow, stopwatch.ElapsedMilliseconds);

        return result;
    }
}