using SmartAc.Domain.Alerts;
using System.Text.Json.Serialization;

namespace SmartAc.Application.Abstractions.Reporting;

public sealed record class AlertReport
{
    public string DeviceSerialNumber { get; init; } = null!;

    public AlertType AlertType { get; init; }

    public AlertState AlertState { get; init; }

    public DateTimeOffset ReportedDateTimeUtc { get; init; }

    public DateTimeOffset CreatedDateTimeUtc { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? LastReportedDateTimeUtc { get; init; }

    public string Message { get; init; } = null!;

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public decimal MinValue { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public decimal MaxValue { get; init; }

}
