using SmartAc.Application.Extensions;
using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Infrastructure.Alerts.Abstractions;

namespace SmartAc.Infrastructure.Alerts.Resolvers;

internal sealed class TempInRangeResolver : Resolver
{
    public TempInRangeResolver(SensorOptions options) : base(options)
    {
    }

    public override bool IsResolved(ResolverContext context) 
        => context.AlertType == AlertType.OutOfRangeTemp
            && context.Reading.Temperature.InRange(SensorOptions.TemperatureMin,
                                                   SensorOptions.TemperatureMax);
}