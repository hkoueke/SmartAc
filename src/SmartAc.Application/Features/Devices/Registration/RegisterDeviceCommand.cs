using ErrorOr;
using SmartAc.Application.Abstractions.Messaging;

namespace SmartAc.Application.Features.Devices.Registration;

public sealed record RegisterDeviceCommand(string SerialNumber, string SharedSecret, string FirmwareVersion) 
    : ICommand<ErrorOr<string>>;
