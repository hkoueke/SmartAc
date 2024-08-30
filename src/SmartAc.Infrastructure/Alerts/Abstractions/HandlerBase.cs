using SmartAc.Application.Options;

namespace SmartAc.Infrastructure.Alerts.Abstractions;

internal abstract class HandlerBase<TItem> : IHandler<TItem>
{
    protected readonly SensorOptions SensorOptions;

    private IHandler<TItem>? _nextHandler;

    protected HandlerBase(SensorOptions options) => SensorOptions = options;

    public virtual bool Handle(TItem item) => _nextHandler?.Handle(item) ?? default;

    public IHandler<TItem> SetNext(IHandler<TItem> handler)
    {
        _nextHandler = handler;
        return handler;
    }
}