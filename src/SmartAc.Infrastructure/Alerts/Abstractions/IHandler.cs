namespace SmartAc.Infrastructure.Alerts.Abstractions;

internal interface IHandler<T>
{
    public IHandler<T> SetNext(IHandler<T> handler);
    public bool Handle(T item);
}