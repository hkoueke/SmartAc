using ErrorOr;
using SmartAc.Application.Abstractions.Messaging;

namespace SmartAc.Application.Features.DeviceReadings.StoreReadings;

public sealed record class StoreReadingCommand(
    string DeviceSerialNumber,
    IEnumerable<SensorReading> Readings) : ICommand<ErrorOr<Success>>;
