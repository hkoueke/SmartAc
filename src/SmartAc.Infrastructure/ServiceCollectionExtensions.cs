using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Quartz;
using SmartAc.Application.Abstractions.Authentication;
using SmartAc.Application.Abstractions.Reporting;
using SmartAc.Infrastructure.BackgroundJobs.Setup;
using SmartAc.Infrastructure.Options;
using SmartAc.Infrastructure.Services;

namespace SmartAc.Infrastructure;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        RegisterServices(services);
        RegisterBackgroundJobs(services);
        RegisterOptions(services);

        return services;
    }

    private static void RegisterOptions(IServiceCollection services)
    {
        services
            .AddOptions<JobOptions>()
            .BindConfiguration(nameof(JobOptions))
            .ValidateDataAnnotations()
            .ValidateOnStart();
    }

    private static void RegisterBackgroundJobs(IServiceCollection services)
    {
        services.AddQuartz(configure => configure.UseMicrosoftDependencyInjectionJobFactory());
        services.AddQuartzHostedService(options => options.WaitForJobsToComplete = true);
        services.ConfigureOptions<ReadingProcessingJobSetup>();
    }

    private static void RegisterServices(IServiceCollection services)
    {
        services.AddTransient<ISmartAcJwtService, SmartAcJwtService>();
        services.AddTransient<IAlertReportService, AlertReportService>();
    }
}