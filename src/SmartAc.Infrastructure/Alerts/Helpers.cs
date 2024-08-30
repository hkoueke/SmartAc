using SmartAc.Application.Options;
using SmartAc.Infrastructure.Alerts.Abstractions;
using SmartAc.Infrastructure.Alerts.Handlers;
using SmartAc.Infrastructure.Alerts.Resolvers;

namespace SmartAc.Infrastructure.Alerts;

internal static class Helpers
{
    public static Processor GetProcessor(SensorOptions options)
    {
        var processor = new AlertProducerHandler(options);
        processor.SetNext(new AlertResolverHandler(options));

        return processor;
    }

    public static Resolver GetResolver(SensorOptions options)
    {
        var resolver = new TempInRangeResolver(options);
        resolver
            .SetNext(new SensorHealthyResolver(options))
            .SetNext(new MonoxideLevelSafeResolver(options))
            .SetNext(new MonoxideInRangeResolver(options))
            .SetNext(new HumidityInRangeResolver(options));

        return resolver;
    }
}
