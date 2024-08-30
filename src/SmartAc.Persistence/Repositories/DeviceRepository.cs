using SmartAc.Domain.Devices;

namespace SmartAc.Persistence.Repositories;

internal sealed class DeviceRepository : RepositoryBase<Device>, IDeviceRepository
{
    public DeviceRepository(SmartAcContext dbContext) : base(dbContext)
    {
    }
}
