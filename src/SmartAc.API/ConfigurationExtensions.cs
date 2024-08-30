using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using SmartAc.API.Data;
using SmartAc.API.Identity;
using SmartAc.Application.Constants;
using SmartAc.Persistence;
using System.Reflection;
using System.Security.Claims;
using System.Text;


namespace SmartAc.API;

internal static class ConfigurationExtensions
{
    public static void AddOpenApiDocumentation(this IServiceCollection services)
    {
        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen(c =>
        {
            c.SwaggerDoc("v1", new OpenApiInfo
            {
                Title = "SmartAC API",
                Description = "SmartAC Device Reporting API",
                Version = "v1"
            });

            c.AddSecurityDefinition("BearerAuth", new OpenApiSecurityScheme
            {
                Name = "Authorization",
                Type = SecuritySchemeType.Http,
                Scheme = "bearer",
                BearerFormat = "JWT",
                Description = "JWT Authorization header using the Bearer scheme."
            });

            c.AddSecurityRequirement(new OpenApiSecurityRequirement
            {
                {
                    new OpenApiSecurityScheme
                    {
                        Reference = new OpenApiReference
                        {
                            Type = ReferenceType.SecurityScheme,
                            Id = "BearerAuth"
                        }
                    },
                    Array.Empty<string>()
                }
            });

            var xmlFilename = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
            c.IncludeXmlComments(Path.Combine(AppContext.BaseDirectory, xmlFilename));
        });
    }

    public static void AddJwtAuthentication(this IServiceCollection services, IConfiguration configuration)
    {
        services
            .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options =>
            {
                options.SaveToken = true;
                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = true,
                    ValidateAudience = true,
                    ValidateLifetime = true,
                    ValidAudience = configuration["JwtOptions:Audience"],
                    ValidIssuer = configuration["JwtOptions:Issuer"],
                    IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(configuration["JwtOptions:Key"])),
                    ValidAlgorithms = new[] { SecurityAlgorithms.HmacSha256 },
                    RoleClaimType = ClaimTypes.Role,
                    NameClaimType = ClaimTypes.NameIdentifier
                };
            });

        services.AddAuthorization(options =>
        {
            options.InvokeHandlersAfterFailure = false;

            options.AddPolicy("DeviceAdmin", policy =>
                policy.RequireRole(JwtServiceConstants.JwtScopeDeviceAdminService)
            );

            options.AddPolicy("DeviceIngestion", policy =>
            {
                policy.Requirements.Add(new ValidTokenRequirement());
                policy.RequireRole(JwtServiceConstants.JwtScopeDeviceIngestionService);
            });
        });

        services.AddHttpContextAccessor();
    }

    public static void UseOpenApiDocumentation(this WebApplication app)
    {
        app.UseSwagger();
        app.UseSwaggerUI(c => { c.SwaggerEndpoint("/swagger/v1/swagger.json", "SmartAC API V1"); });
    }

    public static void MapSmartAcControllers(this WebApplication app)
    {
        app.MapControllers();
        app.MapGet("/", () => Results.Redirect("/swagger")).ExcludeFromDescription();
    }

    public static void EnsureDatabaseSetup(this WebApplication app)
    {
        using var scope = app.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<SmartAcContext>();
        //db.Database.EnsureDeleted();
        db.Database.EnsureCreated();
        SmartAcDataSeeder.Seed(db);
    }
}