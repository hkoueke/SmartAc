using ErrorOr;
using SmartAc.Application.Abstractions.Messaging;
using SmartAc.Domain.Devices;
using SmartAc.Domain.Readings;

namespace SmartAc.Application.Features.DeviceReadings.StoreReadings;

internal sealed class StoreReadingCommandHandler : ICommandHandler<StoreReadingCommand, ErrorOr<Success>>
{
    private readonly IDeviceRepository _repository;

    public StoreReadingCommandHandler(IDeviceRepository repository) => _repository = repository;

    public async Task<ErrorOr<Success>> Handle(StoreReadingCommand request, CancellationToken cancellationToken)
    {
        IEnumerable<DeviceReading> readings = request.Readings
            .Select(r => r.ToDeviceReading(request.DeviceSerialNumber));

        var device = await _repository.FindAsync(request.DeviceSerialNumber, cancellationToken);

        if (device is null)
        {
            return Error.NotFound(
                "Device.notFound",
                $"Device No. {request.DeviceSerialNumber} does not exist");
        }

        device.AddReadings(readings);

        return new Success();
    }
}