using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using SmartAc.Domain.Alerts;


namespace SmartAc.Persistence.Configurations;

internal sealed class AlertConfiguration : IEntityTypeConfiguration<Alert>
{
    public void Configure(EntityTypeBuilder<Alert> builder)
    {
        builder.ToTable(TableNameConstants.Alerts);
        builder.HasKey(x => x.AlertId);

        builder
            .Property(x => x.Message)
            .HasMaxLength(200);
    }
}