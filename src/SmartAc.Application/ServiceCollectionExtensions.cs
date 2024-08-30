using FluentValidation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SmartAc.Application.Options;
using SmartAc.Application.PipelineBehaviors;

namespace SmartAc.Application;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplication(this IServiceCollection services, IConfiguration configuration)
    {
        RegisterServices(services);
        RegisterOptions(services);
        return services;
    }

    private static void RegisterServices(IServiceCollection services)
    {
        //FluentValidation
        services.AddValidatorsFromAssemblyContaining<AssemblyReference>(includeInternalTypes: true);

        //MediatR
        services.AddMediatR(config =>
        {
            config.RegisterServicesFromAssemblyContaining<AssemblyReference>();
            config.AddOpenBehavior(typeof(LoggingBehavior<,>));
            config.AddOpenBehavior(typeof(ValidationBehavior<,>));
            config.AddOpenBehavior(typeof(UnitOfWorkBehavior<,>));
        });
    }

    private static void RegisterOptions(IServiceCollection services)
    {
        services
            .AddOptions<SensorOptions>()
            .BindConfiguration(nameof(SensorOptions));
    }
}