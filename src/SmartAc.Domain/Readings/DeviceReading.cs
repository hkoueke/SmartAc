using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SmartAc.Domain.Readings;
public sealed class DeviceReading
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int DeviceReadingId { get; private set; }

    public decimal Temperature { get; set; }

    public decimal Humidity { get; set; }

    public decimal CarbonMonoxide { get; set; }

    public DeviceHealth Health { get; set; }

    public DateTimeOffset RecordedDateTimeUtc { get; set; }

    public DateTimeOffset ReceivedDateTimeUtc { get; private set; } = DateTimeOffset.UtcNow;

    public DateTimeOffset? ProcessedOnDateTimeUtc { get; private set; }

    public string DeviceSerialNumber { get; set; } = string.Empty;

    public void MarkAsProcessed(DateTimeOffset dateTimeUtc) => ProcessedOnDateTimeUtc = dateTimeUtc;
}