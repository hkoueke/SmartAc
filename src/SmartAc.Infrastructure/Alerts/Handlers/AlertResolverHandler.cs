using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Domain.Devices;
using SmartAc.Infrastructure.Alerts.Abstractions;

namespace SmartAc.Infrastructure.Alerts.Handlers;

internal sealed class AlertResolverHandler : Processor
{
    public AlertResolverHandler(SensorOptions options) : base(options)
    {
    }

    public override void Process(Device device)
    {
        if (!device.Alerts.Any(x => x.AlertState == AlertState.New))
        {
            return;
        }

        Resolver resolver = Helpers.GetResolver(SensorOptions);

        foreach (var reading in device.DeviceReadings)
        {
            var alert = device.Alerts
                .Where(a => a.AlertState == AlertState.New)
                .Where(a => resolver.Handle(new ResolverContext(reading, a.AlertType)))
                .FirstOrDefault();

            alert?.UpdateState(AlertState.Resolved, reading.RecordedDateTimeUtc);
        }
    }
}

