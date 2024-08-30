using ErrorOr;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SmartAc.API.Bindings;
using SmartAc.API.Filters;
using SmartAc.Application.Features.DeviceReadings.StoreReadings;
using SmartAc.Application.Features.Devices.Registration;
using SmartAc.Application.Features.Devices.Reporting;
using System.ComponentModel.DataAnnotations;

namespace SmartAc.API.Controllers;

[ApiController]
[Route("api/v1/device")]
[Authorize(Policy = "DeviceIngestion")]
public sealed class DeviceIngestionController : ControllerBase
{
    private readonly ISender _sender;
    public DeviceIngestionController(ISender sender) => _sender = sender;

    /// <summary>
    /// Allow smart ac devices to register themselves  
    /// and get a jwt token for subsequent operations
    /// </summary>
    /// <param name="serialNumber">Unique device identifier burned into ROM</param>
    /// <param name="sharedSecret">Unique device shareable secret burned into ROM</param>
    /// <param name="firmwareVersion">Device firmware version at the moment of registering</param>
    /// <returns>A jwt token</returns>
    /// <response code="200">If the registration was successful.</response>
    /// <response code="400">If any of the required data is not present or is invalid.</response>
    /// <response code="404">If there is no matching device found.</response>
    [HttpPost("{serialNumber}/registration")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [AllowAnonymous, ValidateFirmware]
    public async Task<IActionResult> RegisterDevice(
        [Required][FromRoute] string serialNumber,
        [Required][FromHeader(Name = "x-device-shared-secret")] string sharedSecret,
        [Required][FromQuery] string firmwareVersion)
    {
        ErrorOr<string> registerResult = await
            _sender.Send(new RegisterDeviceCommand(serialNumber, sharedSecret, firmwareVersion));

        return registerResult.Match(
            onValue: Ok,
            onError: errors => errors.First().ToActionResult());
    }

    /// <summary>
    /// Allow smart ac devices to send sensor readings in batch.
    /// This will additionally trigger analysis over the sensor readings
    /// to generate alerts based on it.
    /// </summary>
    /// <param name="serialNumber">Unique device identifier burned into ROM.</param>
    /// <param name="sensorReadings">Collection of sensor readings send by a device.</param>
    /// <response code="202">If sensor readings are successfully accepted.</response>
    /// <response code="400">If readings are not properly formatted</response>
    /// <response code="401">If device is not authenticated.</response>
    /// <response code="403">If device is not authorized.</response>
    [HttpPost("readings/batch")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    public async Task<IActionResult> AddSensorReadings(
        [ModelBinder(BinderType = typeof(DeviceInfoBinder))] string serialNumber,
        [FromBody] IEnumerable<SensorReading> sensorReadings)
    {
        var result = await _sender.Send(new StoreReadingCommand(serialNumber, sensorReadings));

        return result.Match(
            onValue: _ => Accepted(),
            onError: errors => errors.First().ToActionResult());
    }

    /// <summary>
    /// Allow smart ac devices to read their alerts
    /// 
    /// </summary>
    /// <param name="serialNumber">Unique device identifier burned into ROM.</param>
    /// <param name="queryParams">Query queryParams for data filtering and paging.</param>
    /// <response code="200">If query is accepted.</response>
    /// <response code="401">If device is not authenticated.</response>
    /// <response code="403">If device is not authorized.</response>
    /// <returns>A List of alerts matching the request queryParams.</returns>
    [HttpGet("alerts")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    public async Task<IActionResult> GetSensorAlerts(
        [ModelBinder(BinderType = typeof(DeviceInfoBinder))] string serialNumber,
        [FromQuery] QueryParams queryParams)
    {
        var report = await
            _sender.Send(new GetAlertReportsQuery(serialNumber, queryParams));

        return Ok(report);
    }
}