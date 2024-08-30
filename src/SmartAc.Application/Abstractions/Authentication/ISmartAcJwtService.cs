namespace SmartAc.Application.Abstractions.Authentication;

public interface ISmartAcJwtService
{
    (string tokenId, string token) GenerateJwtFor(string targetId, string role);
}