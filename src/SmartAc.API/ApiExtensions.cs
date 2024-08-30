using ErrorOr;
using Microsoft.AspNetCore.Mvc;

namespace SmartAc.API;

internal static class ApiExtensions
{
    internal static IActionResult ToActionResult(this Error error)
    {
        var content = new { error.Code, error.Description };
        return error.Type switch
        {
            ErrorType.Unauthorized => new UnauthorizedObjectResult(content),
            ErrorType.Validation => new BadRequestObjectResult(content),
            ErrorType.Conflict => new ConflictObjectResult(content),
            ErrorType.NotFound => new NotFoundObjectResult(content),
            _ => new UnprocessableEntityObjectResult(content)
        };
    }
}
