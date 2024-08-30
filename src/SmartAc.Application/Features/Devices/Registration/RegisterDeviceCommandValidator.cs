using FluentValidation;

namespace SmartAc.Application.Features.Devices.Registration;

internal sealed class RegisterDeviceCommandValidator : AbstractValidator<RegisterDeviceCommand>
{
    public RegisterDeviceCommandValidator()
    {
        RuleFor(x => x.SerialNumber)
            .NotEmpty()
            .MaximumLength(32);

        RuleFor(x => x.SharedSecret)
            .NotEmpty()
            .MaximumLength(32);
    }
}
