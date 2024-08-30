using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Domain.Devices;
using SmartAc.Infrastructure.Alerts.Abstractions;
using SmartAc.Infrastructure.Extensions;

namespace SmartAc.Infrastructure.Alerts.Handlers;

internal sealed class AlertProducerHandler : Processor
{
    public AlertProducerHandler(SensorOptions options) : base(options)
    {
    }

    public override void Process(Device device)
    {
        foreach (var reading in device.DeviceReadings)
        {
            //Get alerts (if any) produced by the current reading
            var alerts = reading
                .GetAlerts(SensorOptions)
                .OrderBy(x => x.ReportedDateTimeUtc);

            //Get Alerts that does exist in the collection of device alerts
            var existingAlerts = alerts
                .Where(a => device.Alerts.Any(d => d.AlertType == a.AlertType))
                .ToList();

            //Insert Alerts that does not exist in the collection of device alerts
            var newAlerts = alerts.Where(a => !device.Alerts.Any(x => x.AlertType == a.AlertType));

            device.AddAlerts(newAlerts);

            foreach (var alert in existingAlerts)
            {
                //get matching alert from device Alerts collection
                var deviceAlert = device.Alerts.Where(x => x.AlertType == alert.AlertType).First();

                //Compute how many minutes separate reporting of the same AlertType
                var diff = Math.Abs((alert.ReportedDateTimeUtc - deviceAlert.ReportedDateTimeUtc).TotalMinutes);

                //open the alert and update if it is not too aged. Else close it and create a new Alert
                //with the same AlertType
                var alertState = (diff < SensorOptions.ReadingAgeInMinutes) switch
                {
                    true when deviceAlert.AlertState == AlertState.Resolved => AlertState.New,
                    _ => AlertState.Resolved
                };

                deviceAlert.UpdateState(alertState, alert.ReportedDateTimeUtc);

                if (alertState == AlertState.Resolved)
                {
                    device.AddAlerts(new[] { alert });
                }
            }

            reading.MarkAsProcessed(DateTimeOffset.UtcNow);
        }
    }
}

