
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SmartAc.Domain.Registrations;
public sealed class DeviceRegistration
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int DeviceRegistrationId { get; private set; }

    public string DeviceSerialNumber { get; set; } = null!;

    public DateTimeOffset RegistrationDate { get; set; } = DateTimeOffset.Now;

    public string TokenId { get; set; } = null!;

    public bool Active { get; set; } = true;
}