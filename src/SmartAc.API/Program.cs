using Microsoft.AspNetCore.Authorization;
using SmartAc.API;
using SmartAc.API.Identity;
using SmartAc.API.Middlewares;
using SmartAc.Application;
using SmartAc.Infrastructure;
using SmartAc.Persistence;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddApplication(builder.Configuration)
                .AddInfrastructure(builder.Configuration)
                .AddPersistence(builder.Configuration);

builder.Services.AddSingleton<ExceptionHandlingMiddleware>();

builder.Services.AddScoped<IAuthorizationHandler, ValidTokenAuthorizationHandler>();

builder.Services
    //.ConfigureApiBehaviorOptions(options =>
    .AddControllers()
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
    });

builder.Services.AddOpenApiDocumentation();

builder.Services.AddJwtAuthentication(builder.Configuration);

builder.Services.AddResponseCompression();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.EnsureDatabaseSetup();
}

app.UseHttpsRedirection();

app.UseResponseCompression();

app.UseMiddleware<ExceptionHandlingMiddleware>();

app.UseAuthentication();

app.UseAuthorization();

app.UseOpenApiDocumentation();

app.MapSmartAcControllers();

app.Run();
