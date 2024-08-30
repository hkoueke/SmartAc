using SmartAc.Application.Extensions;
using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Infrastructure.Alerts.Abstractions;

namespace SmartAc.Infrastructure.Alerts.Resolvers;

internal sealed class HumidityInRangeResolver : Resolver
{
    public HumidityInRangeResolver(SensorOptions options) : base(options)
    {
    }

    public override bool IsResolved(ResolverContext context) => 
        context.AlertType == AlertType.OutOfRangeHumidity
            && context.Reading.Humidity.InRange(SensorOptions.HumidityPctMin, SensorOptions.HumidityPctMax);
}