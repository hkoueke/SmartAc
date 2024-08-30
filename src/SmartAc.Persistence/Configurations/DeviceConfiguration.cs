using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using SmartAc.Domain.Devices;

namespace SmartAc.Persistence.Configurations;

internal sealed class DeviceConfiguration : IEntityTypeConfiguration<Device>
{
    public void Configure(EntityTypeBuilder<Device> builder)
    {
        builder.ToTable(TableNameConstants.Devices);

        builder.HasKey(x => x.SerialNumber);

        builder
            .Property(x => x.SerialNumber)
            .HasMaxLength(32);

        builder
            .Property(x => x.SharedSecret)
            .HasMaxLength(32);

        builder
            .HasMany(x => x.DeviceReadings)
            .WithOne()
            .IsRequired();

        builder
            .HasMany(x => x.DeviceRegistrations)
            .WithOne()
            .IsRequired();

        builder
            .HasMany(x => x.Alerts)
            .WithOne()
            .IsRequired();

        builder
            .HasIndex(x => x.SharedSecret)
            .IsUnique();
    }
}