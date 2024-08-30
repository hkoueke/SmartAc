using SmartAc.Domain.Readings;

namespace SmartAc.Application.Features.DeviceReadings.StoreReadings;

public record SensorReading(
    DateTimeOffset RecordedDateTime,
    decimal Temperature,
    decimal Humidity,
    decimal CarbonMonoxide,
    DeviceHealth Health)
{
    public DeviceReading ToDeviceReading(string serialNumber) => new()
    {
        DeviceSerialNumber = serialNumber,
        RecordedDateTimeUtc = RecordedDateTime,
        Temperature = Temperature,
        Humidity = Humidity,
        CarbonMonoxide = CarbonMonoxide,
        Health = Health,
    };
}
