using System.ComponentModel.DataAnnotations;

namespace SmartAc.Infrastructure.Options;

public sealed class JobOptions
{
    [Range(50, 150)]
    public int BatchSize { get; set; } = 50;

    [Range(5, 30)]
    public int AlertProcessingDelayInSeconds { get; set; } = 5;
}