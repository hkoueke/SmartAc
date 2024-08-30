using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Domain.Readings;
using SmartAc.Infrastructure.Alerts.Abstractions;

namespace SmartAc.Infrastructure.Alerts.Resolvers;

internal sealed class SensorHealthyResolver : Resolver
{
    public SensorHealthyResolver(SensorOptions options) : base(options)
    {
    }

    public override bool IsResolved(ResolverContext context) => 
        context.AlertType == AlertType.PoorHealth && context.Reading.Health == DeviceHealth.Ok;
}