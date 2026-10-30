using System.ComponentModel.DataAnnotations;

namespace Swift.Api.Rejection.Model;

public class RejectionDto
{
    private string _reason = string.Empty;

    [Required(ErrorMessage = "El RFK del mensaje entrante no puede ser vacío.")]
    public string SamhRfk { get; set; } = string.Empty;

    [Required(ErrorMessage = "La referencia del mensaje entrante no puede ser vacía.")]
    public string SenderReference { get; set; } = string.Empty;

    [Required(ErrorMessage = "El banco a quien enviar el mensaje no puede ser vacío")]
    public string ReceivingBank { get; set; } = string.Empty;

    public string MessageType { get; set; } = string.Empty;
    public DateTime ValueDate { get; set; }
    public string Currency { get; set; } = string.Empty;
    public double Amount { get; set; }

    [Required]
    public string Reason
    {
        get
        {
            var list = _reason
                .Chunk(50)
                .Select(x => new string(x).TrimStart())
                .ToList();

            return string.Join("\r\n", list);
        }
        set => _reason = value;
    }
}

using System.Text;
using System.Text.RegularExpressions;
using Swift.Api.Rejection.Model;
using Swift.Api.Rejection.Services.Interfaces;
using Microsoft.Extensions.Logging;

namespace Swift.Api.Rejection.Services;

public class MtCreationService : IMessageCreationService
{
    private const string ReasonMessage = "WE AUTHORIZE TO DEBIT OUR ACCOUNT HELD WITH YOU AND RETURN FUNDS " +
                                         "TO THE ORDERING BANK FOR THE FOLLOWING REASON, ";
    private const string ReasonMessage2 = "PLEASE INFORM THEM THAT WE CONSIDER THIS PAYMENT AS NULL AND VOID";
    private readonly ILogger<MtCreationService> _logger;

    public MtCreationService(ILogger<MtCreationService> logger)
    {
        _logger = logger;
    }

    public Result<MessageDto> GenerateMessage(RejectionDto rejectionDto)
    {
        if (string.IsNullOrWhiteSpace(rejectionDto.MessageType) ||
            string.IsNullOrWhiteSpace(rejectionDto.Currency) ||
            rejectionDto.Amount <= 0)
        {
            _logger.LogWarning(
                "GenerateMessage received optional fields with non-ideal values. MessageTypeEmpty={MessageTypeEmpty}, CurrencyEmpty={CurrencyEmpty}, Amount={Amount}, SamhRfk={SamhRfk}",
                string.IsNullOrWhiteSpace(rejectionDto.MessageType),
                string.IsNullOrWhiteSpace(rejectionDto.Currency),
                rejectionDto.Amount,
                rejectionDto.SamhRfk);
        }

        var reason = $"{ReasonMessage}{rejectionDto.Reason} {ReasonMessage2}";
        var reasonMessageMatches = SplitToLines(reason, 44);
        var reasonMessageList = reasonMessageMatches.Select(m => $"/TEXT/{m.Value}".Trim());
        
        var ts = DateTime.Now - DateTime.Today;
        var secondsSinceMidnight = (int) ts.TotalSeconds;
        var ourReference = $"{DateTime.Today:yyyyMMdd}-{secondsSinceMidnight}";
        
        var sb = new StringBuilder();
        sb.Append(
            $"{{1:F01BOTKMXMXXXXX0000000000}}{{2:I199{rejectionDto.ReceivingBank}XXXXN}}{{3:{{108:xxxxx}}}}{{4:\r\n");
        sb.Append($":20:{ourReference}\r\n");
        sb.Append(":79:/REJT/50a\r\n");
        sb.Append("/RR03/PAYMENT MUST BE MADE THROUGH SPID\r\n");
        sb.Append($"/MREF/{rejectionDto.SenderReference}\r\n");
        sb.Append($"/TEXT/WE REFER TO THE FOLLOWING {rejectionDto.MessageType}\r\n");
        sb.Append($"/TEXT/WITH AN AMOUNT {rejectionDto.Currency} {rejectionDto.Amount:0.00}\r\n");
        sb.Append(string.Join("\r\n", reasonMessageList));
        sb.Append("\r\n");
        sb.Append("/TEXT/BEST REGARDS\r\n");
        sb.Append("/TEXT/PAYMENTS OPERATIONS\r\n");
        sb.Append("/TEXT/MUFG BANK MEXICO, S.A.\r\n");
        sb.Append("-}");

        var mt = new MessageDto
        {
            MessageType = ServiceConstants.Messages.Mt199,
            OurReference = ourReference,
            Message = sb.ToString()
        };
        
        return Result<MessageDto>.Success(mt);
    }
    
    private MatchCollection SplitToLines(string stringToSplit, int maximumLineLength)
    {
        return Regex.Matches(stringToSplit, "(.{1," + maximumLineLength + @"})(?:\s|$)");
    }
}

using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Swift.Api.Rejection.Model;
using Swift.Api.Rejection.Services;
using Swift.Api.Rejection.Services.Interfaces;

namespace Swift.Api.Rejection.Controllers;

[ApiController]
[Route("api/[controller]")]
public class RejectionController : ControllerBase
{
    private readonly IMessageCreationService _messageCreationService;
    private readonly ILogger<RejectionController> _logger;
    private readonly IInsertionService _insertionService;
    private const string InternalErrorMessage = "Ocurrió un error interno.";

    public RejectionController(IMessageCreationService messageCreationService,
        ILogger<RejectionController> logger,
        IInsertionService insertionService)
    {
        _messageCreationService = messageCreationService;
        _insertionService = insertionService;
        _logger = logger;
    }

    [Authorize(Policy = ServiceConstants.Authorization.WritePolicy)]
    [HttpPost("mt")]
    public async Task<IActionResult> GenerateMtRejection([FromBody] RejectionDto? rejection)
    {
        try
        {
            if (rejection is null)
            {
                _logger.LogInformation("Rejection request rejected: empty body.");
                return BadRequest("RejectionDto requerido.");
            }

            _logger.LogInformation("New request for {MessageType} with ICN {SamhRfk}", rejection.MessageType,
                rejection.SamhRfk);
            _logger.LogDebug("Received rejection request {@Rejection}", rejection);
            var messageResult = _messageCreationService.GenerateMessage(rejection);

            if (!messageResult.IsSuccess)
            {
                _logger.LogError("Couldn't create the message:\n\t\t\t\t{Reason}", messageResult.ErrorMessage);
                return StatusCode(500, InternalErrorMessage);
            }

            if (messageResult.Data == null)
            {
                _logger.LogError("Message creation succeeded but returned null data for ICN {SamhRfk}", rejection.SamhRfk);
                return StatusCode(500, InternalErrorMessage);
            }

            var message = messageResult.Data;
            _logger.LogDebug("Created MT-199 with contents:\n{@Message}", message.Message);
            _logger.LogInformation("Created MT-199");
            var result = await _insertionService.InsertMessage($"R.{rejection.SamhRfk}", message);

            if (result.IsSuccess)
            {
                _logger.LogInformation("Successfully inserted the MT-199 in the database.");
                return Ok();
            }

            _logger.LogError("Couldn't insert the message for sending:\n\t\t\t\t{Reason}", result.ErrorMessage);
            return StatusCode(500, InternalErrorMessage);
        }
        catch (Exception ex)
        {
            _logger.LogError("An error occurred while creating or inserting the message: {Exception}", ex);
            return StatusCode(500, InternalErrorMessage);
        }
    }
}

using System.Net;
using FluentValidation;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Swift.Api.Rejection.Extensions;
using Swift.Api.Rejection.Model;
using Swift.Api.Rejection.Services.Interfaces;

namespace Swift.Api.Rejection.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AuthController : ControllerBase
{
    private readonly IAuthService _authService;
    private readonly ILogger<AuthController> _logger;
    private readonly IValidator<Credentials> _credentialsValidator;
    private const string InternalErrorMessage = "Ocurrió un error interno.";

    public AuthController(
        ILogger<AuthController> logger,
        IAuthService authService,
        IValidator<Credentials> credentialsValidator)
    {
        _logger = logger;
        _authService = authService;
        _credentialsValidator = credentialsValidator;
    }

    [HttpPost]
    [AllowAnonymous]
    public async Task<IActionResult> Login([FromBody] Credentials? credentials)
    {
        try
        {
            if (credentials is null)
            {
                _logger.LogInformation("Login request rejected: empty body.");
                return BadRequest("Credenciales requeridas.");
            }

            _logger.LogInformation("An attempt of login is made for {User}.", credentials.UserLogin);
            var validationResult = await _credentialsValidator.ValidateAsync(credentials);

            if (!validationResult.IsValid)
            {
                var errors = validationResult.ErrorsToString();
                var response = BadRequest(errors);
                _logger.LogInformation("Invalid credentials: {@Response}", response);
                return response;
            }

            var loginResult = await _authService.Login(credentials);

            if (loginResult.IsDisabled)
            {
                var response = Unauthorized("Usuario deshabilitado.");
                _logger.LogInformation("User {User} is disabled.", credentials.UserLogin);
                return response;
            }

            if (!loginResult.Succeeded)
            {
                var response = Unauthorized(loginResult.Message);
                _logger.LogInformation("Incorrect credentials supplied for {User}.", credentials.UserLogin);
                return response;
            }

            if (loginResult.Data == null)
            {
                _logger.LogError("Login result for {User} succeeded but returned null user data.", credentials.UserLogin);
                return StatusCode((int)HttpStatusCode.InternalServerError, InternalErrorMessage);
            }

            var user = loginResult.Data;
            MufgToken token;
            try
            {
                token = _authService.BuildToken(user);
            }
            catch (ArgumentException ex)
            {
                _logger.LogError("There is no sufficient info to create JWT: {Message}", ex.Message);
                return StatusCode(500);
            }

            _logger.LogInformation("Login successful for {User}.", user.UserLogin);
            return Ok(token.ToString());
        }
        catch (Exception ex)
        {
            _logger.LogError("Server error while trying to login an user: {Mensaje}\r\n{StackTrace}",
                ex.Message, ex.StackTrace);
            return StatusCode((int)HttpStatusCode.InternalServerError, InternalErrorMessage);
        }
    }
}

using Microsoft.Data.SqlClient;
using Swift.Api.Rejection.Model;
using Swift.Api.Rejection.Services.Interfaces;
using System.Data;

namespace Swift.Api.Rejection.Services;

public class InsertionService : IInsertionService
{
    private readonly IConnectionStringProvider _connectionStringProvider;
    private readonly ILogger<IInsertionService> _logger;

    public InsertionService(IConnectionStringProvider connectionStringProvider,
        ILogger<IInsertionService> logger)
    {
        _connectionStringProvider = connectionStringProvider;
        _logger = logger;
    }

    public async Task<Result> InsertMessage(string folio, MessageDto message)
    {
        try
        {
            _logger.LogDebug("Start insertion on [InterfacesT24] for the following folio {Folio}", folio);
            _logger.LogDebug("Searching for SqlServerTOMI ConnectionString");
            var connectionString = _connectionStringProvider.GetDecryptedConnectionString(ServiceConstants.Connections.SqlServerTomi);
            
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            _logger.LogDebug("Successfully opened the connection to [InterfacesT24]");
            
            await using var command = new SqlCommand("usp_swf_insert_t24_msg", connection);
            command.CommandType = CommandType.StoredProcedure;
            _logger.LogInformation("Creating call to SP [dbo].[usp_swf_insert_t24_msg] in [InterfacesT24]");
            
            command.Parameters.Add(new SqlParameter("@folio", folio));
            _logger.LogInformation("Param @folio with value {Folio}", folio);
            
            command.Parameters.Add(new SqlParameter("@tipo_operacion", ServiceConstants.Messages.SwiftOutOperationType));
            _logger.LogInformation("Param @tipo_operacion with value {value}", ServiceConstants.Messages.SwiftOutOperationType);
            
            command.Parameters.Add(new SqlParameter("@cve_mensaje", message.MessageType));
            _logger.LogInformation("Param @cve_mensaje with value {value}", message.MessageType);
            
            command.Parameters.Add(new SqlParameter("@transaction_ref", message.OurReference));
            _logger.LogInformation("Param @transaction_ref with value {ourRef}", message.OurReference );
            
            command.Parameters.Add(new SqlParameter("@swf_msg", message.Message));
            _logger.LogInformation("Param @swf_msg contains message (Check Verbose if needed)");
            _logger.LogTrace("Param @swf_msg contains value\n{value}", message);

            var inserted = await command.ExecuteNonQueryAsync() > 0;
            if (inserted)
            {
                _logger.LogInformation("Successfully inserted message in [InterfacesT24]");
                return Result.Success();
            }
            
            _logger.LogError("Command executed correctly, but didn't changed any rows, most probably the ICN already exists...");
            return Result.Error($"Ya existe un MT-199 con el ICN {folio}");
        }
        catch (Exception ex)
        {
            _logger.LogError("Exception: {@Exception}", ex);
            return Result.Error(ex.Message);
        }
        
    }
}

using Comunes.Configuracion;
using Dapper;
using Microsoft.Data.SqlClient;
using Swift.Api.Rejection.Model;
using Swift.Api.Rejection.Model.Results;
using Swift.Api.Rejection.Model.Security;
using Swift.Api.Rejection.Services.Interfaces;
using System.Data;
using System.Security.Claims;

namespace Swift.Api.Rejection.Services;

public class AuthService : IAuthService
{
    private readonly IConfiguration _configuration;
    private readonly IConnectionStringProvider _connectionStringProvider;
    private readonly ILogger<AuthService> _logger;
    public AuthService(IConfiguration configuration,
        IConnectionStringProvider connectionStringProvider,
        ILogger<AuthService> logger)
    {
        _configuration = configuration;
        _connectionStringProvider = connectionStringProvider;
        _logger = logger;
    }
    public async Task<LoginResult<User>> Login(Credentials credentials)
    {
        _logger.LogDebug("Searching for SqlServerTOMI ConnectionString");
        var connectionString = _connectionStringProvider.GetDecryptedConnectionString(ServiceConstants.Connections.SqlServerTomi);

        await using var connection = new SqlConnection(connectionString);
        _logger.LogInformation("Searching for user with username: {Username}", credentials.UserLogin);
        var user = (await connection.QueryAsync<User, Role, User>(
            "usp_Rejection_LoginUsuario",
            (user, role) =>
            {
                user.Role.Add(role);
                return user;
            },
            new { UserLogin = credentials.UserLogin },
            commandType: CommandType.StoredProcedure,
            splitOn: "RoleId"
        )).FirstOrDefault();

        if (user is null)
        {
            _logger.LogInformation("User {Username} not found", credentials.UserLogin);
            return LoginResult<User>.Failed;
        }

        if (user.IsLocked || !user.IsActive)
        {
            _logger.LogInformation("Disabled user {Username} attempted login", credentials.UserLogin);
            return LoginResult<User>.Disabled(user, "Usuario deshabilitado.");
        }

        var correctPass = user.VerifyPasswordHash(credentials.UserPass);
        if (!correctPass) return LoginResult<User>.Unauthorized(user , "El usuario y/o la contraseña son incorrectos.");
        
        _logger.LogInformation("Found user with username: {Username}", user.UserLogin);
        return LoginResult<User>.Success(user);
    }

    public async Task<IEnumerable<Permission>> GetRoles(IEnumerable<int> roleIds)
    {
        _logger.LogDebug("Searching for SqlServerTOMI ConnectionString");
        var connectionString = _connectionStringProvider.GetDecryptedConnectionString(ServiceConstants.Connections.SqlServerTomi);

        var roles = roleIds?.Distinct().ToArray() ?? Array.Empty<int>();
        if (roles.Length == 0)
        {
            return Array.Empty<Permission>();
        }

        var roleIdsCsv = string.Join(",", roles);
        await using var connection = new SqlConnection(connectionString);
        _logger.LogInformation("Searching permissions for {N} role ids", roles.Length);
        var permissions = await connection.QueryAsync<Permission>(
            "usp_Rejection_GetPermissionsByRole",
            new { RoleIdsCsv = roleIdsCsv },
            commandType: CommandType.StoredProcedure
        );

        return permissions;
    }

    public MufgToken BuildToken(User user)
    {
        var roles = string.Join(",", user.Role.Select(r => r.RoleId));
        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, user.UserLogin),
            new(ClaimTypes.NameIdentifier, user.UserId.ToString()),
            new(ClaimTypes.System, ServiceConstants.Authorization.SystemClaimValue),
            new(ClaimTypes.Role, roles),
        };

        var key = _configuration["Jwt:Key"] ?? throw new ArgumentException("There is no given JWT Key.");
        var issuer = _configuration["Jwt:Issuer"] ?? throw new ArgumentException("There is no given JWT Issuer.");
        var audience = _configuration["Jwt:Audience"] ?? throw new ArgumentException("There is no given JWT Audience.");
        return new MufgToken(key, issuer, audience, claims);
    }
}

﻿using FluentValidation;
using Swift.Api.Rejection.Model;

namespace Swift.Api.Rejection.Validators;

public class CredentialsValidator : AbstractValidator<Credentials>
{
    public CredentialsValidator()
    {
        RuleFor(c => c.UserLogin)
            .NotEmpty();
        RuleFor(c => c.UserPass)
            .NotEmpty();
    }
}

﻿using Swift.Api.Rejection.Model.Security;

namespace Swift.Api.Rejection.Model;

public class User
{
    public int UserId { get; set; }
    public string UserLogin { get; set; } = string.Empty;
    public string UserPass { get; set; } = string.Empty;

    public List<Role> Role { get; set; } = new();
    
    public bool IsActive { get; set; }
    public bool IsLocked { get; set; }

    public bool VerifyPasswordHash(string password)
    {
        // var codified = BCrypt.Net.BCrypt.HashPassword();
        return BCrypt.Net.BCrypt.Verify(password, UserPass);
    }
}

﻿namespace Swift.Api.Rejection.Model.Security;

public class Role
{
    public int RoleId { get; set; }
    public string RoleName { get; set; } = string.Empty;
    public bool RoleEnabled { get; set; }

    public List<Permission> Permissions { get; set; } = new();
}

﻿namespace Swift.Api.Rejection.Model.Security;

public class Permission
{
    public int PermissionId { get; set; }
    public string PermissionName { get; set; } = string.Empty;
    public bool PermissionEnabled { get; set; }
}


namespace Swift.Api.Rejection.Model;

public class Result<T>
 {
     public T Data { get; private set; } = default!;
     public bool IsSuccess { get; private set; }
     public string ErrorMessage { get; private set; } = string.Empty;
     
     public static Result<T> Success(T data)
     {
         return new Result<T>
         {
             Data = data,
             IsSuccess = true
         };
     }
     
     public static Result<T> Error(string errorMessage)
     {
         return new Result<T>
         {
             IsSuccess = false,
             ErrorMessage = errorMessage
         };
     }
 }
 
public class Result
{
    public bool IsSuccess { get; private set; }
    public string ErrorMessage { get; private set; } = string.Empty;
    
    public static Result Success()
    {
        return new Result
        {
            IsSuccess = true
        };
    }
    
    public static Result Error(string errorMessage)
    {
        return new Result
        {
            IsSuccess = false,
            ErrorMessage = errorMessage
        };
    }
}


namespace Swift.Api.Rejection.Model.Results;

public class LoginResult<T> where T : class
{
    public bool Succeeded { get; set; }
    public bool IsDisabled { get; set; }
    public string Message { get; set; } = string.Empty;
    public T? Data { get; set; }

    public static LoginResult<T> Success(T data)
    {
        return new LoginResult<T>
        {
            Succeeded = true,
            Data = data
        };
    }
    
    public static LoginResult<T> Unauthorized(T data, string message)
    {
        return new LoginResult<T>
        {
            Succeeded = false,
            Message = message,
            Data = data
        };
    }

    public static LoginResult<T> Disabled(T data, string message)
    {
        return new LoginResult<T>
        {
            Succeeded = false,
            IsDisabled = true,
            Message = message,
            Data = data
        };
    }

    public static LoginResult<T> Failed { get; } = new();
}


﻿using Swift.Api.Rejection.Model;
using Swift.Api.Rejection.Model.Results;
using Swift.Api.Rejection.Model.Security;

namespace Swift.Api.Rejection.Services.Interfaces;

public interface IAuthService
{
    public Task<LoginResult<User>> Login(Credentials credentials);
    public Task<IEnumerable<Permission>> GetRoles(IEnumerable<int> roleIds);
    public MufgToken BuildToken(User user);
}


﻿using Swift.Api.Rejection.Model;

namespace Swift.Api.Rejection.Services.Interfaces;

public interface IInsertionService
{
    public Task<Result> InsertMessage(string folio, MessageDto message);
}


﻿using Swift.Api.Rejection.Model;

namespace Swift.Api.Rejection.Services.Interfaces;

public interface IMessageCreationService
{
    public Result<MessageDto> GenerateMessage(RejectionDto rejectionDto);
}



Program.cs
using FluentValidation;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using Serilog;
using Swift.Api.Rejection.Model;
using Swift.Api.Rejection.Security;
using Swift.Api.Rejection.Services;
using Swift.Api.Rejection.Services.Conexion;
using Swift.Api.Rejection.Services.Interfaces;
using Swift.Api.Rejection.Validators;
using System.Reflection;
using System.Text;

Serilog.ILogger? startupLogger = null;

try
{
    var builder = WebApplication.CreateBuilder(args);

    builder.Logging.ClearProviders();
    startupLogger = new LoggerConfiguration()
        .ReadFrom.Configuration(builder.Configuration)
        .CreateLogger();

    builder.Host.UseSerilog((context, loggerConfiguration) =>
    {
        loggerConfiguration.ReadFrom.Configuration(context.Configuration);
    });

    builder.Services.AddScoped<IMessageCreationService, MtCreationService>();
    builder.Services.AddScoped<IInsertionService, InsertionService>();
    builder.Services.AddScoped<IAuthService, AuthService>();
    builder.Services.AddTransient<IValidator<Credentials>, CredentialsValidator>();
    builder.Services.AddScoped<IConnectionStringProvider, ConnectionStringProvider>();

    builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
        .AddJwtBearer(options =>
        {
            var key = builder.Configuration["Jwt:Key"];
            if (string.IsNullOrWhiteSpace(key))
                throw new SecurityTokenInvalidSigningKeyException("No se encontro llave para firmar el JWT en la configuración.");

            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = true,
                ValidateAudience = true,
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,
                ValidIssuer = builder.Configuration["Jwt:Issuer"],
                ValidAudience = builder.Configuration["Jwt:Audience"],
                IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(key))
            };
        });

    builder.Services.AddMufgAuthorizationPolicies();
    builder.Services.AddHealthChecks();
    builder.Services.AddControllers();

    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen(options =>
    {
        var jwtSecurityScheme = new OpenApiSecurityScheme
        {
            Scheme = "Bearer",
            BearerFormat = "JWT",
            Name = "Authorization",
            In = ParameterLocation.Header,
            Type = SecuritySchemeType.Http,
            Description = "Introduce el token generado por medio del método Auth.",

            Reference = new OpenApiReference
            {
                Id = JwtBearerDefaults.AuthenticationScheme,
                Type = ReferenceType.SecurityScheme
            }
        };
        options.AddSecurityDefinition(jwtSecurityScheme.Reference.Id, jwtSecurityScheme);
        options.AddSecurityRequirement(new OpenApiSecurityRequirement { { jwtSecurityScheme, ArraySegment<string>.Empty } });
    });

    var app = builder.Build();
    startupLogger.Information("{Application} started successfully.", Assembly.GetExecutingAssembly().GetName().Name);
    if (app.Environment.IsDevelopment())
    {
        startupLogger.Information("{Application} is on Development mode.", Assembly.GetExecutingAssembly().GetName().Name);
        app.UseSwagger();
        app.UseSwaggerUI();
    }

    app.UseAuthentication();
    app.UseAuthorization();

    app.MapHealthChecks("/health");
    app.MapControllers();

    app.Run();
}
catch (Exception ex)
{
    startupLogger?.Fatal(ex, "Unhandled exception during application startup.");
    throw;
}
finally
{
    Log.CloseAndFlush();
}


using Comunes.Configuracion;
using Swift.Api.Rejection.Services.Interfaces;

namespace Swift.Api.Rejection.Services.Conexion;

public class ConnectionStringProvider : IConnectionStringProvider
{
    private readonly IConfiguration _configuration;

    public ConnectionStringProvider(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public string GetDecryptedConnectionString(string connectionName)
    {
        var ruta = _configuration["ConfigPath"];
        if (string.IsNullOrWhiteSpace(ruta))
        {
            throw new InvalidOperationException("No se encontró 'ConfigPath' en appsettings.");
        }

        var connectionString = ServiciosConfigConexionHelper.ObtenerCadenaConexionDescrifrada(ruta, connectionName);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException($"No se encontró una cadena de conexión para '{connectionName}'.");
        }

        return connectionString;
    }
}


using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Comunes.Configuracion
{
    public static class ServiciosConfigConexionHelper
    {
        public static string ObtenerCadenaConexionDescrifrada(
            string rutaServiciosConfig,
            string nombreConexion)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(rutaServiciosConfig))
                    throw new ArgumentException("La ruta del archivo servicios.config no puede estar vacía.", nameof(rutaServiciosConfig));

                if (!File.Exists(rutaServiciosConfig))
                    throw new FileNotFoundException("No existe el archivo servicios.config.", rutaServiciosConfig);

                if (string.IsNullOrWhiteSpace(nombreConexion))
                    throw new ArgumentException("El nombre de la conexión no puede estar vacío.", nameof(nombreConexion));

                var xmlConfiguracion = File.ReadAllText(rutaServiciosConfig);
                if (!TryParseXmlSeguro(xmlConfiguracion, out var configuracion, out var xmlError) || configuracion is null)
                {
                    throw new InvalidOperationException($"El contenido de servicios.config no es un XML válido. {xmlError}");
                }

                string keyBase64 = ObtenerValorGeneral(configuracion, "VECTOR_KEY");
                if (!TryDecodeBase64(keyBase64, out var key))
                {
                    Trace.TraceError("VECTOR_KEY inválida en servicios.config. No se pudo decodificar Base64.");
                    throw new InvalidOperationException("El valor de VECTOR_KEY no tiene un formato Base64 válido.");
                }

                var conexiones = configuracion.Root
                    ?.Element("conexiones")
                    ?.Elements("add");

                if (conexiones == null)
                {
                    throw new InvalidOperationException("No existe el nodo 'conexiones' en servicios.config.");
                }

                string? cadenaConexion = conexiones
                    .Where(x => string.Equals(
                        (string?)x.Attribute("name"),
                        nombreConexion,
                        StringComparison.OrdinalIgnoreCase))
                    .Select(x => (string?)x.Attribute("connectionString"))
                    .FirstOrDefault();

                if (string.IsNullOrWhiteSpace(cadenaConexion))
                {
                    throw new InvalidOperationException(
                        $"No se encontró la cadena de conexión '{nombreConexion}' en servicios.config.");
                }

                return DesencriptarValorSiAplica(cadenaConexion, key);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error al obtener la cadena de conexión '{nombreConexion}' desde servicios.config.",
                    ex);
            }
        }

        private static bool TryParseXmlSeguro(string xml, out XDocument? xmlDoc, out string? error)
        {
            xmlDoc = null;
            error = null;

            if (string.IsNullOrWhiteSpace(xml))
            {
                error = "XML vacío o nulo.";
                return false;
            }

            try
            {
                var settings = new XmlReaderSettings
                {
                    DtdProcessing = DtdProcessing.Prohibit,
                    XmlResolver = null
                };

                using var sr = new StringReader(xml);
                using var xr = XmlReader.Create(sr, settings);
                xmlDoc = XDocument.Load(xr, LoadOptions.None);
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                return false;
            }
        }

        private static string ObtenerValorGeneral(XDocument configuracion, string nombre)
        {
            try
            {
                XElement? generales = configuracion.Root?.Element("generales");

                if (generales == null)
                {
                    throw new InvalidOperationException(
                        "No existe el nodo 'generales' en servicios.config.");
                }

                string? valor = generales
                    .Element(nombre)?
                    .Attribute("value")?
                    .Value;

                if (string.IsNullOrWhiteSpace(valor))
                {
                    throw new InvalidOperationException(
                        $"No existe el parámetro general '{nombre}' en servicios.config.");
                }

                return valor;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error al obtener el parámetro general '{nombre}' desde servicios.config.",
                    ex);
            }
        }


        private static string DesencriptarValorSiAplica(string valor, byte[] key)
        {
            try
            {
                if (!TryDecodeBase64(valor, out var cifrado))
                    return valor;

                if (cifrado.Length <= 16 || cifrado.Length % 16 != 0)
                    return valor;

                return Desencriptar(cifrado, key);
            }
            catch (CryptographicException)
            {
                return valor;
            }
        }

        private static bool TryDecodeBase64(string value, out byte[] decoded)
        {
            decoded = Array.Empty<byte>();
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            var buffer = new byte[(value.Length / 4) * 3 + 3];
            if (!Convert.TryFromBase64String(value, buffer, out var bytesWritten))
            {
                return false;
            }

            decoded = buffer.AsSpan(0, bytesWritten).ToArray();
            return true;
        }

        private static string Desencriptar(byte[] cipherText, byte[] key)
        {
            try
            {
                using (Aes aesAlg = Aes.Create())
                {
                    aesAlg.Key = key;
                    aesAlg.Mode = CipherMode.CBC;
                    aesAlg.Padding = PaddingMode.PKCS7;

                    const int ivLength = 16;
                    if (cipherText.Length <= ivLength)
                    {
                        throw new InvalidOperationException("El texto cifrado no contiene un IV válido.");
                    }

                    byte[] iv = new byte[ivLength];
                    byte[] encrypted = new byte[cipherText.Length - ivLength];

                    Buffer.BlockCopy(cipherText, 0, iv, 0, ivLength);
                    Buffer.BlockCopy(cipherText, ivLength, encrypted, 0, encrypted.Length);

                    aesAlg.IV = iv;

                    byte[] decrypted = aesAlg.CreateDecryptor().TransformFinalBlock(
                        encrypted,
                        0,
                        encrypted.Length);

                    return Encoding.UTF8.GetString(decrypted);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    "No se pudo desencriptar la cadena de conexión. Verificar VECTOR_KEY y formato con IV embebido.",
                    ex);
            }
        }
    }
}


namespace Swift.Api.Rejection.Model;

public static class ServiceConstants
{
    public static class Authorization
    {
        public const string WritePolicy = "WritePolicy";
        public const string WritePermission = "WRITE";
        public const string SystemClaimValue = "SwiftRejectionApi";
    }

    public static class Connections
    {
        public const string SqlServerTomi = "SqlServerTOMI";
    }

    public static class Messages
    {
        public const string Mt199 = "MT-199";
        public const string SwiftOutOperationType = "SWIFT_OUT";
    }
}


Swift.Api.Rejection.csproj
<Project Sdk="Microsoft.NET.Sdk.Web">

    <PropertyGroup>
        <TargetFramework>net8.0</TargetFramework>
        <Nullable>enable</Nullable>
        <ImplicitUsings>enable</ImplicitUsings>
        <PublishWithAspNetCoreTargetManifest>true</PublishWithAspNetCoreTargetManifest>
        <UserSecretsId>Swift.Api.Rejection-LocalSecrets</UserSecretsId>
    </PropertyGroup>

    <ItemGroup>
        <PackageReference Include="BCrypt.Net-Next" Version="4.0.3" />
        <PackageReference Include="Dapper" Version="2.1.35" />
        <PackageReference Include="FluentValidation" Version="11.9.2" />
        <PackageReference Include="Microsoft.AspNetCore.Authentication.JwtBearer" Version="6.0.32" />
        <PackageReference Include="Microsoft.Data.SqlClient" Version="5.2.1" />
        <PackageReference Include="Serilog" Version="4.0.0" />
        <PackageReference Include="Serilog.AspNetCore" Version="8.0.1" />
        <PackageReference Include="Serilog.Sinks.File" Version="6.0.0" />
        <PackageReference Include="Swashbuckle.AspNetCore" Version="6.5.0" />
    </ItemGroup>

</Project>


Swift.Api.Rejection.sln
﻿
Microsoft Visual Studio Solution File, Format Version 12.00
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Swift.Api.Rejection", "Swift.Api.Rejection.csproj", "{68BD3C4F-0952-4807-8B1A-94D934020B7D}"
EndProject
Global
	GlobalSection(SolutionConfigurationPlatforms) = preSolution
		Debug|Any CPU = Debug|Any CPU
		Release|Any CPU = Release|Any CPU
	EndGlobalSection
	GlobalSection(ProjectConfigurationPlatforms) = postSolution
		{68BD3C4F-0952-4807-8B1A-94D934020B7D}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{68BD3C4F-0952-4807-8B1A-94D934020B7D}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{68BD3C4F-0952-4807-8B1A-94D934020B7D}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{68BD3C4F-0952-4807-8B1A-94D934020B7D}.Release|Any CPU.Build.0 = Release|Any CPU
	EndGlobalSection
EndGlobal


global.json
{
  "sdk": {
    "version": "8.0.0",
    "rollForward": "latestMinor",
    "allowPrerelease": false
  }
}


appsettings.json
{
  "Serilog": {
    "Using": [
      "Serilog.Sinks.File",
      "Serilog.Sinks.Console"
    ],
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Microsoft": "Warning"
      }
    },
    "WriteTo": [
      {
        "Name": "File",
        "Args": {
          "path": "D:\\ServicioAPI\\RejectionAPI\\logs\\SwiftApi_Rejection.log",
          "rollingInterval": "Month",
          "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.ff}|[{Level:u3}]|{Message:lj}{NewLine}"
        }
      }
    ]
  },
  "Jwt": {
    "Issuer": "http://localhost:13337",
    "Audience": "http://localhost:13337",
    "Key": ""
  },
  "ConfigPath": "D:\\ServiceCMS\\Parametros\\servicios.config",
  "AllowedHosts": "*"
}


appsettings.Development.json
{
  "Serilog": {
    "Using": [
      "Serilog.Sinks.File",
      "Serilog.Sinks.Console"
    ],
    "MinimumLevel": {
      "Default": "Debug",
      "Override": {
        "Microsoft": "Warning"
      }
    },
    "WriteTo": [
      {
        "Name": "File",
        "Args": {
          "path": "D:\\ServicioAPI\\RejectionAPI\\logs\\SwiftApi_Rejection.log",
          "rollingInterval": "Day",
          "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.ff}|[{Level:u3}]|{Message:lj}{NewLine}"
        }
      },
      {
        "Name": "Console",
        "Args": {
          "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.ff}|[{Level}]|{Message:lj}{NewLine}"
        }
      }
    ]
  },
  "Jwt": {
    "Issuer": "http://localhost:13337",
    "Audience": "http://localhost:13337",
    "Key": ""
  },
  "ConfigPath": "D:\\ServiceCMS\\Parametros\\servicios.config"
}


USE [InterfacesT24];
GO

ALTER PROCEDURE dbo.usp_Rejection_GetPermissionsByRole
    @RoleIdsCsv NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT DISTINCT
        M.idPermission AS PermissionId,
        P.permissionName,
        P.isEnabled AS PermissionEnabled
    FROM dbo.TB_SRV_API_MASTER M WITH (NOLOCK)
    INNER JOIN dbo.TB_SRV_API_PERMISSION P ON P.id = M.idPermission
    INNER JOIN STRING_SPLIT(@RoleIdsCsv, ',') R ON TRY_CAST(R.value AS INT) = M.idRol;
END
GO



