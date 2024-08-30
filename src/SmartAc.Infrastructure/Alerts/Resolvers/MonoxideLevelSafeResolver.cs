using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Infrastructure.Alerts.Abstractions;

namespace SmartAc.Infrastructure.Alerts.Resolvers;

internal sealed class MonoxideLevelSafeResolver : Resolver
{
    public MonoxideLevelSafeResolver(SensorOptions options) : base(options)
    {
    }

    public override bool IsResolved(ResolverContext context) 
        => context.AlertType == AlertType.DangerousCoLevel
            && context.Reading.CarbonMonoxide < SensorOptions.CarbonMonoxideDangerLevel;
}