using ErrorOr;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SmartAc.Application.Abstractions.Authentication;
using SmartAc.Application.Abstractions.Messaging;
using SmartAc.Application.Constants;
using SmartAc.Domain.Devices;
using SmartAc.Domain.Registrations;

namespace SmartAc.Application.Features.Devices.Registration;

internal sealed class RegisterDeviceCommandHandler : ICommandHandler<RegisterDeviceCommand, ErrorOr<string>>
{
    private readonly IDeviceRepository _repository;
    private readonly ILogger<RegisterDeviceCommandHandler> _logger;
    private readonly ISmartAcJwtService _jwtService;

    public RegisterDeviceCommandHandler(
        IDeviceRepository repository,
        ISmartAcJwtService jwtService,
        ILogger<RegisterDeviceCommandHandler> logger)
    {
        _repository = repository;
        _jwtService = jwtService;
        _logger = logger;
    }

    public async Task<ErrorOr<string>> Handle(RegisterDeviceCommand request, CancellationToken cancellationToken)
    {
        var device = await _repository
            .GetQueryable()
            .Include(d => d.DeviceRegistrations)
            .SingleOrDefaultAsync(d => d.SerialNumber == request.SerialNumber && d.SharedSecret == request.SharedSecret, cancellationToken: cancellationToken);

        if (device is null)
        {
            return Error.NotFound(
               "Device.NotFound",
               $"Device with serial number '{request.SerialNumber}' and provided secret was not found");
        }

        var (tokenId, jwtToken) =
            _jwtService.GenerateJwtFor(request.SerialNumber, JwtServiceConstants.JwtScopeDeviceIngestionService);

        var registration = new DeviceRegistration
        {
            DeviceSerialNumber = device.SerialNumber,
            TokenId = tokenId
        };

        device.AddRegistration(registration, request.FirmwareVersion);

        _repository.Update(device);

        _logger.LogInformation("JWT Token created for device with serial number '{SerialNumber}'", request.SerialNumber);

        return jwtToken;
    }
}