using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using SmartAc.Domain.Registrations;

namespace SmartAc.Persistence.Configurations;

internal sealed class DeviceRegistrationConfiguration : IEntityTypeConfiguration<DeviceRegistration>
{
    public void Configure(EntityTypeBuilder<DeviceRegistration> builder)
    {
        builder.ToTable(TableNameConstants.DeviceRegistrations);

        builder.HasKey(x => x.DeviceRegistrationId);

        builder.Property(x => x.DeviceSerialNumber)
               .IsRequired()
               .HasMaxLength(32); 
        
        builder.Property(x => x.TokenId)
               .IsRequired();
    }
}