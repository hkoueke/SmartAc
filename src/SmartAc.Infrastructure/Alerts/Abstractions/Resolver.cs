using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Domain.Readings;

namespace SmartAc.Infrastructure.Alerts.Abstractions;

internal abstract class Resolver : HandlerBase<ResolverContext>
{
    protected Resolver(SensorOptions options) : base(options)
    {
    }

    public override bool Handle(ResolverContext context) => IsResolved(context) || base.Handle(context);

    public abstract bool IsResolved(ResolverContext context);
}

internal sealed record ResolverContext(DeviceReading Reading, AlertType AlertType);
