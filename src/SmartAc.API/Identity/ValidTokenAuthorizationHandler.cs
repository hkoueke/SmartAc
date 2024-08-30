using Microsoft.AspNetCore.Authorization;
using Microsoft.EntityFrameworkCore;
using SmartAc.Persistence;
using System.IdentityModel.Tokens.Jwt;

namespace SmartAc.API.Identity;

internal sealed class ValidTokenAuthorizationHandler : AuthorizationHandler<ValidTokenRequirement>
{
    private readonly SmartAcContext _smartAcContext;
    public ValidTokenAuthorizationHandler(SmartAcContext smartAcContext)
    {
        _smartAcContext = smartAcContext;
    }

    protected override async Task HandleRequirementAsync(
        AuthorizationHandlerContext context, ValidTokenRequirement requirement)
    {
        string? tokenId =
            context.User.Claims.FirstOrDefault(claim => claim.Type == JwtRegisteredClaimNames.Jti)?.Value;

        var deviceSerialNumber = context.User.Identity?.Name;

        if (tokenId is null || deviceSerialNumber is null)
        {
            return;
        }

        var isTokenValid = await _smartAcContext.Devices
            .AnyAsync(d => d.DeviceRegistrations.Any(x => 
                x.DeviceSerialNumber == deviceSerialNumber &&
                x.TokenId == tokenId &&
                x.Active));

        if (isTokenValid)
        {
            context.Succeed(requirement);
        }
    }
}