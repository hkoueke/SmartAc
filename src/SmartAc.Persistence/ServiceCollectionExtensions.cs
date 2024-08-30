using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SmartAc.Domain.Abstractions;
using SmartAc.Domain.Devices;
using SmartAc.Persistence.Repositories;

namespace SmartAc.Persistence;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddPersistence(this IServiceCollection services, IConfiguration configuration)
    {
        RegisterDatabase(services, configuration);
        return services;
    }

    private static void RegisterDatabase(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString =
            configuration.GetConnectionString("smartac") ?? "Data Source=SmartAc.db";

        services.AddDbContext<SmartAcContext>(options =>
        {
            options
            .UseSqlite(
                connectionString,
                options => options.UseQuerySplittingBehavior(QuerySplittingBehavior.SplitQuery))
            .LogTo(Console.WriteLine, new[] { RelationalEventId.CommandExecuted }, LogLevel.Information);

            //.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTrackingWithIdentityResolution);
            //.UseExceptionProcessor()
        });

        services.AddScoped<IDeviceRepository, DeviceRepository>();       
        services.AddScoped<IUnitOfWork, UnitOfWork>();
    }
}