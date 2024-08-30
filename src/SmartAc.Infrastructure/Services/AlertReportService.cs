using Microsoft.EntityFrameworkCore;
using SmartAc.Application.Abstractions.Reporting;
using SmartAc.Domain.Alerts;
using SmartAc.Persistence;

namespace SmartAc.Infrastructure.Services;

internal sealed class AlertReportService : IAlertReportService
{
    private readonly SmartAcContext _context;

    public AlertReportService(SmartAcContext context)
    {
        _context = context;
    }

    public async Task<IEnumerable<AlertReport>> ComputeAlertReportsAsync(
        string deviceSerialNumber,
        AlertState? alertState,
        CancellationToken cancellationToken = default)
    {
        var queryData =
            from alert in _context.Alerts.AsNoTracking()
            where alert.DeviceSerialNumber == deviceSerialNumber && (!alertState.HasValue || alert.AlertState == alertState)
            join reading in _context.DeviceReadings.AsNoTracking() on alert.DeviceSerialNumber equals reading.DeviceSerialNumber
            group new { alert, reading } by alert.AlertId into grouped
            select new
            {
                AlertId = grouped.Key,
                Alert = grouped.Select(x => new
                {
                    x.alert.AlertId,
                    x.alert.DeviceSerialNumber,
                    x.alert.CreatedDateTimeUtc,
                    x.alert.ReportedDateTimeUtc,
                    x.alert.LastReportedDateTimeUtc,
                    x.alert.AlertState,
                    x.alert.AlertType,
                    x.alert.Message
                }).First(),
                Readings = grouped.Select(x => new
                {
                    x.reading.DeviceReadingId,
                    x.reading.Health,
                    x.reading.Temperature,
                    x.reading.CarbonMonoxide,
                    x.reading.Humidity
                })
            };

        var reports = new List<AlertReport>();

        await foreach (var group in queryData.AsAsyncEnumerable().WithCancellation(cancellationToken))
        {
            var readings = group.Readings;
            var report = new AlertReport
            {
                DeviceSerialNumber = group.Alert.DeviceSerialNumber,
                AlertType = group.Alert.AlertType,
                AlertState = group.Alert.AlertState,
                CreatedDateTimeUtc = group.Alert.CreatedDateTimeUtc,
                ReportedDateTimeUtc = group.Alert.ReportedDateTimeUtc,
                LastReportedDateTimeUtc = group.Alert.LastReportedDateTimeUtc,
                Message = group.Alert.Message,
                MinValue = readings.Any() ? group.Alert.AlertType switch
                {
                    AlertType.OutOfRangeTemp => readings.Min(x => x.Temperature),
                    AlertType.OutOfRangeCo => readings.Min(x => x.CarbonMonoxide),
                    AlertType.OutOfRangeHumidity => readings.Min(x => x.Humidity),
                    AlertType.DangerousCoLevel => readings.Min(x => x.CarbonMonoxide),
                    _ => 0m,
                } : 0m,
                MaxValue = readings.Any() ? group.Alert.AlertType switch
                {
                    AlertType.OutOfRangeTemp => readings.Max(x => x.Temperature),
                    AlertType.OutOfRangeCo => readings.Max(x => x.CarbonMonoxide),
                    AlertType.OutOfRangeHumidity => readings.Max(x => x.Humidity),
                    AlertType.DangerousCoLevel => readings.Max(x => x.CarbonMonoxide),
                    _ => 0m,
                } : 0m
            };

            reports.Add(report);
        }

        return reports.OrderByDescending(x => x.ReportedDateTimeUtc);
    }
}
