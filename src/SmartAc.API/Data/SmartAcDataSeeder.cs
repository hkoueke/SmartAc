using SmartAc.Domain.Devices;
using SmartAc.Persistence;

namespace SmartAc.API.Data;

internal sealed class SmartAcDataSeeder
{
    public static void Seed(SmartAcContext smartAcContext)
    {
        if (smartAcContext.Devices.Any()) return;

        var testData = new List<Device>
        {
            new()
            {
                SerialNumber = "test-ABC-123-XYZ-001",
                SharedSecret = "secret-ABC-123-XYZ-001"
            },
            new()
            {
                SerialNumber = "test-ABC-123-XYZ-002",
                SharedSecret = "secret-ABC-123-XYZ-002"
            },
            new()
            {
                SerialNumber = "test-ABC-123-XYZ-003",
                SharedSecret = "secret-ABC-123-XYZ-003"
            },
            new()
            {
                SerialNumber = "test-ABC-123-XYZ-004",
                SharedSecret = "secret-ABC-123-XYZ-004"
            },
            new()
            {
                SerialNumber = "test-ABC-123-XYZ-005",
                SharedSecret = "secret-ABC-123-XYZ-005"
            }
        };

        smartAcContext.Devices.AddRange(testData);
        smartAcContext.SaveChanges();
    }
}