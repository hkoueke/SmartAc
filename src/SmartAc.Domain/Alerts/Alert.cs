using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SmartAc.Domain.Alerts;

public sealed class Alert
{
    private Alert()
    {
    }

    public Alert(AlertType alertType, string deviceSerialNumber, DateTimeOffset reportedDateTime, string message)
    {
        ArgumentNullException.ThrowIfNull(deviceSerialNumber);
        ArgumentNullException.ThrowIfNull(message);

        AlertType = alertType;
        DeviceSerialNumber = deviceSerialNumber;
        ReportedDateTimeUtc = reportedDateTime;
        Message = message;
    }

    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int AlertId { get; private set; }

    public AlertType AlertType { get; private set; }

    public string Message { get; private set; } = null!;

    public AlertState AlertState { get; private set; } = AlertState.New;

    public DateTimeOffset CreatedDateTimeUtc { get; private set; } = DateTimeOffset.UtcNow;

    public DateTimeOffset ReportedDateTimeUtc { get; private set; }

    public DateTimeOffset? LastReportedDateTimeUtc { get; private set; }

    public string DeviceSerialNumber { get; private set; } = null!;

    public void UpdateState(AlertState alertState, DateTimeOffset lastReportedDateTime)
    {
        AlertState = alertState;
        LastReportedDateTimeUtc = lastReportedDateTime;
    }
}