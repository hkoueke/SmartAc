using SmartAc.Domain.Abstractions;
using SmartAc.Domain.Alerts;
using SmartAc.Domain.Readings;
using SmartAc.Domain.Registrations;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SmartAc.Domain.Devices;

public sealed class Device : IAggregateRoot
{
    private readonly List<DeviceRegistration> _registrations = new();
    private readonly List<DeviceReading> _readings = new();
    private readonly List<Alert> _alerts = new();

    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public string SerialNumber { get; set; } = string.Empty;

    public string SharedSecret { get; set; } = string.Empty;

    public string FirmwareVersion { get; private set; } = string.Empty;

    public IEnumerable<DeviceRegistration> DeviceRegistrations => _registrations;

    public IEnumerable<DeviceReading> DeviceReadings => _readings;

    public IEnumerable<Alert> Alerts => _alerts;

    public DateTimeOffset? FirstRegistrationDate { get; private set; }

    public DateTimeOffset? LastRegistrationDate { get; private set; }

    public void AddRegistration(DeviceRegistration registration, string firmwareVersion)
    {
        //Deactivate current registrations
        foreach (var r in _registrations.Where(x => x.Active))
        {
            r.Active = false;
        }

        //UpdateState registration dates
        FirstRegistrationDate ??= registration.RegistrationDate;
        LastRegistrationDate = registration.RegistrationDate;

        //Set firmware version
        FirmwareVersion = firmwareVersion;

        //Add new registration to collection
        _registrations.Add(registration);
    }

    public void AddReadings(IEnumerable<DeviceReading> readings) => _readings.AddRange(readings);

    public void AddAlerts(IEnumerable<Alert> alerts) => _alerts.AddRange(alerts);
   
}