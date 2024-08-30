using Microsoft.Extensions.Options;
using Quartz;
using SmartAc.Infrastructure.Options;

namespace SmartAc.Infrastructure.BackgroundJobs.Setup;

internal sealed class ReadingProcessingJobSetup : SetupBase<JobOptions>, IConfigureOptions<QuartzOptions>
{
    public ReadingProcessingJobSetup(IOptionsMonitor<JobOptions> options) : base(options)
    {
    }

    public void Configure(QuartzOptions options)
    {
        var jobKey = JobKey.Create(nameof(DeviceReadingProcessorJob));

        options
            .AddJob<DeviceReadingProcessorJob>(builder => builder.WithIdentity(jobKey))
            .AddTrigger(trigger => trigger
                .ForJob(jobKey)
                .WithIdentity(jobKey.ToString() + "-trigger")
                .WithSimpleSchedule(schedule =>
                {
                    schedule
                        .WithIntervalInSeconds(Options.AlertProcessingDelayInSeconds)
                        .RepeatForever();
                }));
    }
}
