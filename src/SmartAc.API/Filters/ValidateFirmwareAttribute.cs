using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using System.Text.RegularExpressions;

namespace SmartAc.API.Filters;

[AttributeUsage(AttributeTargets.Method)]
internal sealed class ValidateFirmwareAttribute : ActionFilterAttribute
{
    private const string Pattern = 
        @"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$";

    //[RegexGenerator(@"\d+", RegexOptions.Compiled, 50)]
    private readonly Regex _regex = new(Pattern, RegexOptions.Compiled, TimeSpan.FromMilliseconds(50));

    public override void OnActionExecuting(ActionExecutingContext context)
    {
        string firmwareVersion = context.HttpContext.Request.Query["firmwareVersion"];

        if (_regex.IsMatch(firmwareVersion))
        {
            return;
        }

        context.ModelState.AddModelError(
            "firmwareVersion",
            "The firmware value does not match semantic versioning format.");

        var problemDetails = new ValidationProblemDetails(context.ModelState);
        context.Result = new BadRequestObjectResult(problemDetails);
    }
}
