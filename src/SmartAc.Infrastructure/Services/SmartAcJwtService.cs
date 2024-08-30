using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using SmartAc.Application.Abstractions.Authentication;
using SmartAc.Application.Constants;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace SmartAc.Infrastructure.Services;

public sealed class SmartAcJwtService : ISmartAcJwtService
{
    private readonly IConfiguration _configuration;

    public SmartAcJwtService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public (string tokenId, string token) GenerateJwtFor(string targetId, string role)
    {
        var tokenId = Guid.NewGuid().ToString();

        var claimsIdentity = new ClaimsIdentity(new[]
        {
            new Claim(JwtRegisteredClaimNames.Sub, targetId),
            new Claim(JwtRegisteredClaimNames.Jti, tokenId),
            new Claim(ClaimTypes.Role, JwtServiceConstants.JwtScopeApiKey),
            new Claim(ClaimTypes.Role, role)
        });

        byte[] encodedKey = Encoding.UTF8.GetBytes(_configuration["JwtOptions:Key"]);

        var tokenDescriptor = new SecurityTokenDescriptor
        {
            Subject = claimsIdentity,
            Issuer = _configuration["JwtOptions:Issuer"],
            Audience = _configuration["JwtOptions:Audience"],
            IssuedAt = DateTime.UtcNow,
            Expires = DateTime.UtcNow.AddDays(30),
            SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(encodedKey), SecurityAlgorithms.HmacSha256)
        };

        var tokenHandler = new JwtSecurityTokenHandler();
        SecurityToken? token = tokenHandler.CreateToken(tokenDescriptor);
        var jwt = tokenHandler.WriteToken(token);

        return (tokenId, jwt);
    }
}