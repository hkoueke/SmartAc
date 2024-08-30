using SmartAc.Application.Extensions;
using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Infrastructure.Alerts.Abstractions;

namespace SmartAc.Infrastructure.Alerts.Resolvers;

internal sealed class MonoxideInRangeResolver : Resolver
{
    public MonoxideInRangeResolver(SensorOptions options) : base(options)
    {
    }

    public override bool IsResolved(ResolverContext context) 
        => context.AlertType == AlertType.OutOfRangeCo
            && context.Reading.CarbonMonoxide.InRange(SensorOptions.CarbonMonoxidePpmMin,
                                                      SensorOptions.CarbonMonoxidePpmMax);
}