using FluentValidation;

namespace SmartAc.Application.Features.DeviceReadings.StoreReadings;

internal sealed class ReadingsValidator : AbstractValidator<IEnumerable<SensorReading>>
{
    public ReadingsValidator()
    {
        ValidatorOptions.Global.PropertyChainSeparator = ".";

        RuleFor(x => x)
            .NotEmpty()
            .WithName("Sensor readings");

        RuleForEach(x => x)
            .ChildRules(r => r.RuleFor(x => x).SetValidator(new SensorReadingValidator()));
    }
}

internal sealed class SensorReadingValidator : AbstractValidator<SensorReading>
{
    public SensorReadingValidator()
    {
        RuleFor(x => x.Temperature).PrecisionScale(5, 2, true);
        RuleFor(x => x.CarbonMonoxide).PrecisionScale(5, 2, true);
        RuleFor(x => x.Humidity).PrecisionScale(5, 2, true);
        RuleFor(x => x.Health).IsInEnum();
    }
}