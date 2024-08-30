using SmartAc.Domain.Alerts;

namespace SmartAc.Application.Abstractions.Reporting;

public interface IAlertReportService
{
    public Task<IEnumerable<AlertReport>> ComputeAlertReportsAsync(
        string deviceSerialNumber,
        AlertState? alertState,
        CancellationToken cancellationToken = default);
}
