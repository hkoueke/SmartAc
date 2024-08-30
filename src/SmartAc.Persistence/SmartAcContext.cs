using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using SmartAc.Domain.Alerts;
using SmartAc.Domain.Devices;
using SmartAc.Domain.Readings;
using SmartAc.Domain.Registrations;
using System.Reflection.Metadata;

namespace SmartAc.Persistence;

public sealed class SmartAcContext : DbContext
{
    public SmartAcContext(DbContextOptions<SmartAcContext> options)
        : base(options)
    {
    }

    public DbSet<Device> Devices { get; set; } = null!;
    public DbSet<Alert> Alerts { get; set; } = null!;
    public DbSet<DeviceReading> DeviceReadings { get; set; } = null!;
    public DbSet<DeviceRegistration> DeviceRegistrations { get; set; } = null!;

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        configurationBuilder
            .Properties<DateTimeOffset>()
            .HaveConversion<DateTimeOffsetToStringConverter>(); // SqlLite workaround for DateTimeOffset sorting

        configurationBuilder
            .Properties<decimal>()
            .HaveConversion<double>(); // SqlLite workaround for decimal aggregations
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(AssemblyReference).Assembly);
    }
}
