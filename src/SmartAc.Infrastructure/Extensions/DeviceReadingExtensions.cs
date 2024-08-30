using SmartAc.Application.Extensions;
using SmartAc.Application.Options;
using SmartAc.Domain.Alerts;
using SmartAc.Domain.Readings;

namespace SmartAc.Infrastructure.Extensions;

internal static class DeviceReadingExtensions
{
    public static IEnumerable<Alert> GetAlerts(this DeviceReading reading, SensorOptions sensorOptions)
    {
        if (!reading.Temperature.InRange(sensorOptions.TemperatureMin, sensorOptions.TemperatureMax))
        {
            yield return new Alert(
                AlertType.OutOfRangeTemp,
                reading.DeviceSerialNumber,
                reading.RecordedDateTimeUtc,
                $"Sensor {reading.DeviceSerialNumber} reported out-of-range Temperature");
        }

        if (!reading.CarbonMonoxide.InRange(sensorOptions.CarbonMonoxidePpmMin, sensorOptions.CarbonMonoxidePpmMax))
        {
            yield return new Alert(
                AlertType.OutOfRangeCo,
                reading.DeviceSerialNumber,
                reading.RecordedDateTimeUtc,
                $"Sensor {reading.DeviceSerialNumber} reported out-of-range carbon Monoxide levels");
        }

        if (reading.CarbonMonoxide >= sensorOptions.CarbonMonoxideDangerLevel)
        {
            yield return new Alert(
                AlertType.DangerousCoLevel,
                reading.DeviceSerialNumber,
                reading.RecordedDateTimeUtc,
                $"Sensor {reading.DeviceSerialNumber} - Reported CO value has exceeded danger limit");
        }

        if (!reading.Humidity.InRange(sensorOptions.HumidityPctMin, sensorOptions.HumidityPctMax))
        {
            yield return new Alert(
                AlertType.OutOfRangeHumidity,
                reading.DeviceSerialNumber,
                reading.RecordedDateTimeUtc,
                $"Sensor {reading.DeviceSerialNumber} reported out-of-range humidity levels");
        }

        if (reading.Health != DeviceHealth.Ok)
        {
            yield return new Alert(
                AlertType.PoorHealth,
                reading.DeviceSerialNumber,
                reading.RecordedDateTimeUtc,
                $"Sensor {reading.DeviceSerialNumber} is reporting health problem: {reading.Health}");
        }
    }
}