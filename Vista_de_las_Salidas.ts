AssemblyInfo.cs
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("MUFG_T24")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("MUFG_T24")]
[assembly: AssemblyCopyright("Copyright ©  2025")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible
// to COM components.  If you need to access a type in this assembly from
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("6c793f61-7dc9-474f-b3c1-5a170f57c6d5")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version
//      Build Number
//      Revision
//
[assembly: AssemblyVersion("1.0.0.0")]
[assembly: AssemblyFileVersion("1.0.0.0")]


App.config
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <configSections>
    <section name="oracle.manageddataaccess.client" type="OracleInternal.Common.ODPMSectionHandler, Oracle.ManagedDataAccess, Version=4.122.23.1, Culture=neutral, PublicKeyToken=89b483f429c47342" />
  </configSections>
  <system.data>
    <DbProviderFactories>
      <remove invariant="Oracle.ManagedDataAccess.Client" />
      <add name="ODP.NET, Managed Driver" invariant="Oracle.ManagedDataAccess.Client" description="Oracle Data Provider for .NET, Managed Driver" type="Oracle.ManagedDataAccess.Client.OracleClientFactory, Oracle.ManagedDataAccess, Version=4.122.23.1, Culture=neutral, PublicKeyToken=89b483f429c47342" />
    </DbProviderFactories>
  </system.data>
  <runtime>
    <assemblyBinding xmlns="urn:schemas-microsoft-com:asm.v1">
      <dependentAssembly>
        <assemblyIdentity name="System.Memory" publicKeyToken="cc7b13ffcd2ddd51" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-4.0.5.0" newVersion="4.0.5.0" />
      </dependentAssembly>
      <dependentAssembly>
        <assemblyIdentity name="System.Runtime.CompilerServices.Unsafe" publicKeyToken="b03f5f7f11d50a3a" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-6.0.3.0" newVersion="6.0.3.0" />
      </dependentAssembly>
      <dependentAssembly>
        <assemblyIdentity name="System.Buffers" publicKeyToken="cc7b13ffcd2ddd51" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-4.0.5.0" newVersion="4.0.5.0" />
      </dependentAssembly>
      <dependentAssembly>
        <assemblyIdentity name="System.Threading.Tasks.Extensions" publicKeyToken="cc7b13ffcd2ddd51" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-9.9.9.9" newVersion="4.2.1.0" />
      </dependentAssembly>
      <dependentAssembly>
        <assemblyIdentity name="System.Numerics.Vectors" publicKeyToken="b03f5f7f11d50a3a" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-4.1.6.0" newVersion="4.1.6.0" />
      </dependentAssembly>
    </assemblyBinding>
  </runtime>
</configuration>


using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Xml.Linq;
using EncriptacionUtilidades = MUFG_T24.Encriptacion_utilidades.Encriptacion;

namespace MUFG_T24.Core.Configuracion
{
    /// <summary>
    /// Lectura centralizada de configuración general y de servicios desde el archivo host recifrado.
    /// </summary>
    public static class ObtencionConfguracionGeneral
    {
        private static XDocument _configDesencriptada;
        private static XDocument _configHost;

        private static readonly object _lock = new object();
        private static bool _inicializado = false;

        public static void Inicializar()
        {
            if (_inicializado)
                return;

            lock (_lock)
            {
                if (_inicializado)
                    return;

                try
                {
                    string rutaHostConfig = EnumRutasGenerales.Archivo_Config_CMS;

                    if (!File.Exists(rutaHostConfig))
                        throw new Exception("No existe el config de la aplicacion host: " + rutaHostConfig);

                    _configHost = XDocument.Load(rutaHostConfig);

                    XElement servicioConfiguracion = _configHost.Root.Element("servicioconfiguracion");

                    if (servicioConfiguracion == null)
                        throw new Exception("No existe el nodo servicioConfiguracion");

                    XElement primerServicio = servicioConfiguracion.Elements().FirstOrDefault();

                    if (primerServicio == null)
                        throw new Exception("No existen servicios dentro de servicioConfiguracion");

                    _configDesencriptada = new XDocument(_configHost);

                    string keybase64 = ObtenerValorGeneral("VECTOR_KEY");

                    DesencriptarValoresConfig(_configDesencriptada, keybase64);

                    _inicializado = true;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Error al Inicializar la configuración encriptada.", ex);
                }
            }
        }

        public static ServicioConfiguracionSettings ObtenerServicio(string nombreServicio)
        {
            try
            {
                AsegurarInicializado();

                if (string.IsNullOrWhiteSpace(nombreServicio))
                    throw new InvalidOperationException("El nombre del servicio no puede estar vacio");

                string nombreServicioNormalizado = nombreServicio.Trim();

                XElement servicioConfiguracion = _configHost.Root?.Element("servicioconfiguracion");

                if (servicioConfiguracion == null) throw new InvalidOperationException("No Existe el nodo servicioConfiguracion");

                XElement nodoServicio = servicioConfiguracion
                    .Elements()
                    .FirstOrDefault(x => string.Equals(
                        x.Name.LocalName,
                        nombreServicioNormalizado,
                        StringComparison.OrdinalIgnoreCase));

                if (nodoServicio == null)
                {
                    throw new Exception("No Existe la configuracion del servicio: " + nombreServicioNormalizado);
                }

                string nombreServicioConf = (string)nodoServicio.Attribute("NombreServicio") ?? "";
                ServicioConfiguracionSettings servicio = new ServicioConfiguracionSettings
                {
                    NombreServicio = string.Equals(nombreServicioConf, "", StringComparison.Ordinal) ? nodoServicio.Name.LocalName : nombreServicioConf,
                    Log = (string)nodoServicio.Attribute("LOG"),
                    Timer = ConversionConfiguracion.ObtenerEnteroConfiguracion(nodoServicio, "Timer", 0),
                    configuracion = (string)nodoServicio.Attribute("configuracion"),
                    TimerEnable = ConversionConfiguracion.ObtenerBooleanoConfiguracion(nodoServicio, "TimerEnable", false),
                    TamanoMaximoArchivoLogKB = ConversionConfiguracion.ObtenerEnteroConfiguracion((string)nodoServicio.Attribute("TamanoMaximoArchivoLogKB") ?? (string)nodoServicio.Attribute("TamanoMaxLog") ?? ObtenerValorGeneral("TAM_LOG_DEFAULT"), "TamanoMaximoArchivoLogKB", 0),
                    ForzaEjecucionServicios = ConversionConfiguracion.ObtenerBooleanoConfiguracion(nodoServicio, "ForzaEjecucionServicios", false),
                    ForzarPruebas = ConversionConfiguracion.ObtenerBooleanoConfiguracion(nodoServicio, "ForzarPruebas", false),
                    CantidadPaquete = ConversionConfiguracion.ObtenerEnteroConfiguracion(nodoServicio, "CantidadPaquete", 0),
                    LimitePaquetesPendientes = ConversionConfiguracion.ObtenerEnteroConfiguracion(nodoServicio, "LimitePaquetesPendientes", 0),
                    OperacionesPermitidas = (string)nodoServicio.Attribute("OperacionesPermitidas") ?? "",
                    LimiteConcurrencia = ConversionConfiguracion.ObtenerEnteroConfiguracion(nodoServicio, "LimiteConcurrencia", 0),
                    AliasProceso = (string)nodoServicio.Attribute("AliasProceso") ?? string.Empty,
                    AliasProcesoFin = (string)nodoServicio.Attribute("AliasProcesoFin") ?? string.Empty,
                    ServicioPlan = (string)nodoServicio.Attribute("ServicioPlan") ?? string.Empty,
                    MaxTimeout = ConversionConfiguracion.ObtenerEnteroConfiguracion(nodoServicio, "MaxTimeout", 0),
                    MonedaDefault = (string)nodoServicio.Attribute("MonedaDefault") ?? string.Empty,

                    TimeoutDespachadoMs = ConversionConfiguracion.ObtenerEnteroConfiguracion(nodoServicio, "TimeoutDespachadoMs", 30000),
                    DelayEntreDespachoMs = ConversionConfiguracion.ObtenerEnteroConfiguracion(nodoServicio, "DelayEntreDespachoMs", 300),

                    ListaTipos = string.Join(",",
                        ((string)nodoServicio.Attribute("ListaTipos") ?? string.Empty)
                        .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(x => $"'{x.Trim()}")),

                    CorreoParaDefault = (string)nodoServicio.Attribute("CorreoParaDefault") ?? string.Empty,
                    SujetoDefault = (string)nodoServicio.Attribute("SujetoDefault") ?? string.Empty,
                    CorreoDeDefault = (string)nodoServicio.Attribute("CorreoDeDefault") ?? string.Empty,
                    CorreoCopiaDefault = (string)nodoServicio.Attribute("CorreoCopiaDefault") ?? string.Empty,
                    CorreoSenderTest = (string)nodoServicio.Attribute("CorreoSenderTest") ?? string.Empty,

                    AceptaDiferenciaEnFechas = ConversionConfiguracion.ObtenerBooleanoConfiguracion(nodoServicio, "AceptaDiferenciaEnFechas", false),
                    FORZA_SALDOS_REPLICA_COMPLETA = ConversionConfiguracion.ObtenerBooleanoConfiguracion(nodoServicio, "FORZA_SALDOS_REPLICA_COMPLETA", false),
                    SwiftStandardReleaseSanitized = ConversionConfiguracion.ObtenerBooleanoConfiguracion(nodoServicio, "SwiftStandardReleaseSanitized", false),
                    CrearTablasAutomaticamente = ConversionConfiguracion.ObtenerBooleanoConfiguracion(nodoServicio, "CrearTablasAutomaticamente", false),

                    RutaTOMISAT = (string)nodoServicio.Attribute("RutaTOMISAT") ?? "",
                    IPSAT = (string)nodoServicio.Attribute("IPSAT") ?? "",
                    UserSAT = (string)nodoServicio.Attribute("UserSAT") ?? "",
                    RutaRemota = (string)nodoServicio.Attribute("RutaRemota") ?? "",
                    RutaToSend = (string)nodoServicio.Attribute("RutaToSend") ?? "",
                    RutaToSendBackup = (string)nodoServicio.Attribute("RutaToSendBackup") ?? "",
                    RutaToReceive = (string)nodoServicio.Attribute("RutaToReceive") ?? "",
                    RutaToReceiveBackup = (string)nodoServicio.Attribute("RutaToReceiveBackup") ?? "",

                    ArchivoCER = (string)nodoServicio.Attribute("ArchivoCER") ?? "",
                    ArchivoPEM = (string)nodoServicio.Attribute("ArchivoPEM") ?? "",
                };
                return servicio;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error al obtener la configuración del servicio '{nombreServicio}", ex);
            }
        }

        public static string ObtenerDatosGenerales(string dato)
        {
            try
            {
                AsegurarInicializado();

                string valor = _configDesencriptada.Root
                    .Element("appSettings")?
                    .Elements("add")
                    .Where(x => (string)x.Attribute("key") == dato)
                    .Select(x => (string)x.Attribute("value"))
                    .FirstOrDefault();

                if (string.IsNullOrWhiteSpace(valor)) valor = _configDesencriptada.Root
                    .Element("generales")?
                    .Element(dato)?
                    .Attribute("value")?
                    .Value;

                if (string.IsNullOrWhiteSpace(valor)) throw new Exception($"El dato {dato} no fue encontrado");

                return valor;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error al obtener el dato general {dato}.", ex);
            }
        }

        public static string ObtenerCadeaConexion(global::MUFG_T24.Comunes.BasedatosProveedor? proveedor, global::MUFG_T24.Comunes.NombreBasedatos? nombreBasedatos)
        {
            try
            {
                AsegurarInicializado();

                if (!proveedor.HasValue)
                    throw new InvalidOperationException("El proveedor de base de datos es obligatorio para obtener la cadena de conexión.");

                string clave = nombreBasedatos.HasValue
                    ? string.Concat(proveedor.Value, nombreBasedatos.Value)
                    : proveedor.Value.ToString();

                string cadenaConexion = _configDesencriptada.Root
                    .Element("conexiones")
                    .Descendants("add")
                    .Where(x => string.Equals(
                        (string)x.Attribute("name"),
                        clave,
                        StringComparison.OrdinalIgnoreCase))
                    .Select(x => (string)x.Attribute("connectionString"))
                    .FirstOrDefault();

                if (string.IsNullOrEmpty(cadenaConexion))
                {
                    throw new InvalidOperationException(
                        $"Cadena de conexión '{clave}' no encontrada en configuración.");
                }

                if (!cadenaConexion.Contains("="))
                {
                    throw new Exception($"ConnectionString NO desencriptado o inválido: {clave}");
                }

                try
                {
                    var builder = new System.Data.SqlClient.SqlConnectionStringBuilder(cadenaConexion);
                }
                catch
                {
                    throw new Exception($"ConnectionString inválido (no parseable): {clave}");
                }

                return cadenaConexion;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Error al obtener cadena de conexión.", ex);
            }
        }

        public static string ObtenerGeneral(string nombre)
        {
            AsegurarInicializado();
            return ObtenerValorGeneral(nombre);
        }

        public static string ObtenerLlaveEncriptacion()
        {
            return ObtenerDatosGenerales("LLAVE_COMUNICACION");
        }

        public static string ObtenerValorGeneral(string nombre)
        {
            try
            {
                string valor = _configDesencriptada.Root
                    .Element("generales")?
                    .Element(nombre)?
                    .Attribute("value")?
                    .Value;

                if (string.IsNullOrEmpty(valor))
                {
                    throw new InvalidOperationException(
                        $"No existe el parametro general: {valor}");
                }
                return valor;

            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Error al obtener el valor general.", ex);
            }
        }

        private static void AsegurarInicializado()
        {
            try
            {
                if (_configDesencriptada == null || _configHost == null)
                {
                    throw new InvalidOperationException("La configuracion segura no ha sido inicializada. Ejecutar ConfiguracionSegura.Inicializar() primero.");

                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Configuración no inicializada. Ejecutar Inicializada() antes de consumir valores", ex);
            }
        }

        private static void DesencriptarValoresConfig(XDocument doc, string key)
        {
            try
            {
                XElement conexiones = doc.Root.Element("conexiones");

                if (conexiones != null)
                {
                    foreach (XElement add in conexiones.Elements("add"))
                    {
                        DesencriptarAtributo(add, "connectionString", key);
                        DesencriptarAtributo(add, "providerName", key);
                    }
                }

                XElement appSettings = doc.Root.Element("appSettings");

                if (appSettings != null)
                {
                    foreach (XElement add in appSettings.Elements("add"))
                    {
                        DesencriptarAtributo(add, "value", key);
                    }
                }

                XElement general = doc.Root.Element("generales");

                if (general != null)
                {
                    foreach (XElement item in general.Elements())
                    {
                        string nombre = item.Name.LocalName;

                        if (nombre == "VECTOR_KEY") continue;

                        DesencriptarAtributo(item, "value", key);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Error al desencriptar valores del archivo de configuración", ex);
            }
        }

        private static void DesencriptarAtributo(XElement elemento, string nombreAtributo, string key)
        {
            try
            {
                XAttribute atributo = elemento.Attribute(nombreAtributo);

                if (atributo == null || string.IsNullOrWhiteSpace(atributo.Value))
                    return;

                try
                {
                    byte[] cifrado = Convert.FromBase64String(atributo.Value);

                    if (cifrado.Length <= 16 || cifrado.Length % 16 != 0)
                        return;
                }
                catch
                {
                    return;
                }

                try
                {
                    atributo.Value = EncriptacionUtilidades.Decrypt(atributo.Value, key);
                }
                catch (CryptographicException ex)
                {
                    throw new InvalidOperationException(
                        $"No se pudo desencriptar '{nombreAtributo}' en '{ObtenerIdentificadorElemento(elemento)}'. " +
                        "Verificar que el valor haya sido recifrado con AES nuevo, IV embebido y la misma llave VECTOR_KEY.",
                        ex);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error al desencriptar atributo '{nombreAtributo}' en nodo '{ObtenerIdentificadorElemento(elemento)}'",
                    ex);
            }
        }

        private static string ObtenerIdentificadorElemento(XElement elemento)
        {
            string identificador = (string)elemento.Attribute("name")
                ?? (string)elemento.Attribute("key")
                ?? elemento.Name.LocalName;

            return elemento.Name.LocalName + ":" + identificador;
        }

    }
}


using MUFG_T24.Infraestructura.Logging;
using System;
using System.IO;
using System.Reflection;

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Clase para inciar rutas
    /// </summary>
    [Obsolete("Legacy compatibility only. Use Logging.Info/Warn/Error/Write directly.")]
    public static class Config
    {
        public static RutasConfiguracion Rutas { get; set; } = new RutasConfiguracion();

        [Obsolete("Legacy compatibility only.")]
        public static int TamanoMaximoArchivoKB { get; set; } = 100;
        public static string NombreServicioLog { get; set; }
        public static string RutaLogServicio { get; set; }
        public static bool AjustarRutas { get; set; } = false;

        /// <summary>
        /// Inicia rutas del sistema.
        /// </summary>
        [Obsolete("Legacy compatibility only.")]
        public static void IniciarRutas(RutasConfiguracion rutas = null)
        {
            rutas = rutas ?? new RutasConfiguracion();

            if (!AjustarRutas)
            {
                string rutaEnsablado = Assembly.GetExecutingAssembly().Location;
                int ultimoSeparador = rutaEnsablado.LastIndexOf(Path.DirectorySeparatorChar);
                rutas.MUFG_Work_Path = rutaEnsablado.Substring(0, ultimoSeparador);

                if (!rutas.MUFG_Work_Path.EndsWith(Path.DirectorySeparatorChar.ToString()))
                {
                    rutas.MUFG_Work_Path += Path.DirectorySeparatorChar.ToString();
                }

                rutas.MUFG_Servicios_Archivo_Configuracion = EnumRutasGenerales.Archivo_Config_CMS;
                string nombreCarpeta = rutas.MUFG_Work_Path.Substring(rutas.MUFG_Work_Path.Length - 4, 3);
                rutas.MUFG_Servicios_Path_Log = EnumRutasGenerales.Dir_Logs + nombreCarpeta + EnumRutasGenerales.Dir_CMS;
                rutas.MUFG_File_Errores = rutas.MUFG_Servicios_Path_Log + "ERRORES.txt";
            }
            else
            {
                NormalizarRutaLog(rutas);
            }

            Rutas = rutas;
            Logging.Init(rutas, NombreServicioLog, TamanoMaximoArchivoKB);
        }

        [Obsolete("Legacy compatibility only.")]
        private static void NormalizarRutaLog(RutasConfiguracion rutas)
        {
            if (rutas == null || string.IsNullOrWhiteSpace(rutas.MUFG_Servicios_Path_Log) || string.IsNullOrWhiteSpace(RutaLogServicio))
                return;

            string rutaActual = rutas.MUFG_Servicios_Path_Log.Trim();
            string rutaConfigurada = RutaLogServicio.Trim();

            if (!rutaConfigurada.EndsWith(Path.DirectorySeparatorChar.ToString()))
                rutaConfigurada += Path.DirectorySeparatorChar;

            if (!string.IsNullOrWhiteSpace(NombreServicioLog)
                && string.Equals(rutaActual, rutaConfigurada + NombreServicioLog, StringComparison.OrdinalIgnoreCase))
            {
                rutas.MUFG_Servicios_Path_Log = rutaConfigurada;
            }
        }

        [Obsolete("Legacy compatibility only.")]
        public static string ObtenerRutaLog()
        {
            return Rutas?.MUFG_Servicios_Path_Log;
        }


        [Obsolete("Legacy compatibility only.")]
        public static void CreaRutaArchivoSalida(string rutaArchivo)
        {
            if (string.IsNullOrWhiteSpace(rutaArchivo))
            {
                throw new ArgumentException("El directorio del log no puede ser vacío.");
            }
            try
            {
                string directorio = Path.GetDirectoryName(rutaArchivo);
                if (!Directory.Exists(directorio))
                {
                    Directory.CreateDirectory(directorio);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error al establecer el archivo de logs", ex);
            }
        }

        [Obsolete("Legacy compatibility only.")]
        public static string ObtenerNombreArchivoLog(string nombreArchivo = null)
        {
            if (Rutas == null || string.IsNullOrWhiteSpace(Rutas.MUFG_Servicios_Path_Log))
            {
                throw new InvalidOperationException("El archivo de log no ha sido inicializado");
            }

            return $"{Rutas.MUFG_Servicios_Path_Log}_{DateTime.Now:yyyyMMdd}";
        }

        /// <summary>
        /// Escribe en archivo de log
        /// </summary>
        /// <param name="mensaje"></param>
        /// <param name="nombreArchivo"></param>
        /// <exception cref="Exception"></exception>
        public static void EscribirLog(string mensaje, string nombreArchivo = "Mufg")
        {
            EscribirLog(mensaje, nombreArchivo, false);
        }

        /// <summary>
        /// Escribe en archivo de log.
        /// </summary>
        public static void EscribirLog(string mensaje, bool masked)
        {
            EscribirLog(mensaje, "Mufg", masked);
        }

        /// <summary>
        /// Escribe en archivo de log.
        /// </summary>
        public static void EscribirLog(string mensaje, string nombreArchivo, bool masked)
        {
            Logging.WriteLegacy(mensaje, nombreArchivo, masked);
        }

    }
}

namespace MUFG_T24.Logeo_utilidades
{
    [Obsolete("Legacy compatibility only. Use MUFG_T24.Infraestructura.Logging.Logging.")]
    public static class Logging
    {
        public static void Init(global::MUFG_T24.Comunes.RutasConfiguracion rutas, string filePrefix = "Mufg", int maxMb = 100)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Init(rutas, filePrefix, maxMb);

        public static void Init(string pathLog, string filePrefix = "Mufg", int maxMb = 100)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Init(pathLog, filePrefix, maxMb);

        public static IDisposable BeginWorkerScope()
            => global::MUFG_T24.Infraestructura.Logging.Logging.BeginWorkerScope();

        public static IDisposable BeginOperacionScope(string operacion, string folio)
            => global::MUFG_T24.Infraestructura.Logging.Logging.BeginOperacionScope(operacion, folio);

        public static void Write(string msg)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Write(msg);

        public static void Write(string msg, bool masked)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Write(msg, masked);

        public static void WriteLegacy(string msg)
            => global::MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(msg);

        public static void WriteLegacy(string msg, bool masked)
            => global::MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(msg, masked);

        public static void WriteLegacy(string msg, string nombreArchivo)
            => global::MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(msg, nombreArchivo);

        public static void WriteLegacy(string msg, string nombreArchivo, bool masked)
            => global::MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(msg, nombreArchivo, masked);

        public static void Info(string msg)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Info(msg);

        public static void Info(string msg, bool masked)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Info(msg, masked);

        public static void Warn(string msg)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Warn(msg);

        public static void Warn(string msg, bool masked)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Warn(msg, masked);

        public static void Error(string msg)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Error(msg);

        public static void Error(string msg, bool masked)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Error(msg, masked);

        public static void Error(Exception ex, string ctx = null)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Error(ex, ctx);

        public static void Error(Exception ex, string ctx, bool masked)
            => global::MUFG_T24.Infraestructura.Logging.Logging.Error(ex, ctx, masked);

        public static void WriteConContexto(string contexto, string msg)
            => global::MUFG_T24.Infraestructura.Logging.Logging.WriteConContexto(contexto, msg);

        public static void Shutdown()
            => global::MUFG_T24.Infraestructura.Logging.Logging.Shutdown();
    }
}


namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Modelo para rutas de configuracion
    /// </summary>
    public class RutasConfiguracion
    {       
        public bool Ajustar_rutas { get; set; }
        public string MUFG_Work_Path { get; set; }
        public string MUFG_Servicios_Archivo_Configuracion { get; set; }
        public string MUFG_Servicios_Path_Log { get; set; }
        public string MUFG_File_Errores { get; set; }
        public string ARCHIVO_FINAL { get; set; }

        /// <summary>
        /// Inicializa una nueva instancia de RutasConfiguracion.
        /// </summary>
        public RutasConfiguracion()
        {
            Ajustar_rutas = true;
            MUFG_Work_Path = EnumRutasGenerales.Servicio_CMS;
            MUFG_Servicios_Archivo_Configuracion = EnumRutasGenerales.Archivo_Config_CMS;

            MUFG_Servicios_Path_Log = EnumRutasGenerales.Path_Log_CMS;
            MUFG_File_Errores = EnumRutasGenerales.Path_Log_CMS + "ERRORES.txt";

            // RUTA MICROSOFT/SECRETS : APRA ARCHIVO ENCRIPTADO

            ARCHIVO_FINAL = EnumRutasGenerales.Archivo_Config_CMS;
        }
    }
}

using MUFG_T24.Sellado_Utilidades;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MUFG_T24.Configuracion_utilidades
{
    /// <summary>
    /// Representa Configuracion.
    /// </summary>
    public class Configuracion
    {
        //General
        public string NombreServicio { get; set; } = null;
        public string AliasProceso { get; set; } = null;
        public string AliasProcesoFin { get; set; } = null;
        public string ServicioPlan { get; set; } = null;
        public int? Pid { get; set; } = null;
        public string NombreProceso { get; set; } = null;
        public string NombreDisplay { get; set; } = null;

        //Rutas
        public string PathLog { get; set; } = null;
        public string RutaArchivoConfiguracion { get; set; } = null;
        public string configuracion { get; set; } = null;

        //Timer
        public bool? TimerEnabled { get; set; } = null;
        public double? TimerInterval { get; set; } = null;

        //Operación
        public string ListaTipos { get; set; } = null;

        //Parametrización
        public bool ForzaEjecucionServicios { get; set; } = false;
        public string FechaSistema { get; set; } = null;
        public TimeSpan? HoraSistema { get; set; } = null;
        public string FechaSistemaJuliana { get; set; } = null;
        public string FechaSistemaJulianaT24 { get; set; } = null;
        public bool? EsDiaHabil { get; set; } = null;
        public Dictionary<string, string> ParametrosSistema { get; set; } = null;
        public bool? EsHorarioLectura { get; set; } = null;
        public bool? EsHorarioOperacion { get; set; } = null;
        public bool? EnDiaHabil { get; set; } = null;
        public bool CrearTablasAutomaticamente { get; set; } = false;


        //SQL
        public int? MaxTimeout { get; set; } = null;

        //Negocio
        public string MonedaDefault { get; set; } = null;

        //Socket
        public string PuertoDespachable { get; set; } = null;

        //Sellado       
        public string ArchivoCER { get; set; } = null;
        public string ArchivoPEM { get; set; } = null;

        public string SerieCertificado { get; set; } = null;
        public Sellado Sellado { get; set; } = null;

        //Despachador
        public int? CantidadPaquete { get; set; } = null;
        public int? LimitePaquetesPendientes { get; set; } = null;
        public int? LimiteConcurrencia { get; set; } = null;
        public int? TimeoutDespachadoMs { get; set; } = 30000;
        public int? DelayEntreDespachoMs { get; set; } = 300;

        //Logging
        public string TamanoMaximoArchivoLogKB { get; set; } = null;

        //TCP
        public int? IntentosPorPuerto { get; set; } = null;
        public int? CantidadPuertos { get; set; } = null;
        public string IPTcp { get; set; } = null;
        public string PuertoTCP { get; set; } = null;

        //Logeo
        public string Usuario { get; set; } = null;
        public string Contrasena { get; set; } = null;
        public string IP { get; set; } = null;
        public int? Puerto { get; set; } = null;
        public byte? ServicioCep { get; set; } = null;

        //CDA
        public bool? EnContingencia { get; set; } = null;

        //Excepciones
        public Exception UltimaExcepcion { get; set; } = null;

        //Operacion seccionada
        public string[] OperacionesPermitidas { get; set; } = null;

        //Email
        public string CorreoParaDefault { get; set; } = null;
        public string SujetoDefault { get; set; } = null;
        public string CorreoDeDefault { get; set; } = null;
        public string CorreoCopiaDefault { get; set; } = null;
        public string CorreoSenderTest { get; set; } = null;

        //Banderas
        public bool? AceptaDiferenciaEnFechas { get; set; } = false;
        public bool FORZA_SALDOS_REPLICA_COMPLETA { get; set; } = false;
        public bool? SwiftStandardReleaseSanitized { get; set; } = false;

        /// <summary>
        /// Método clonación
        /// </summary>
        public Configuracion Clone() => (Configuracion)this.MemberwiseClone();
    }
}

namespace MUFG_T24.Enums_utilidades
{
    /// <summary>
    /// Constantes de nombres de procedimientos almacenados de MUFG.
    /// </summary>
    public static class ConstantesProcedimientosMUFG
    {
        public const string UspActualizarControladoraDeServicios = "usp_Comun_ActualizarControladoraDeServicios";
        public const string UspActualizarEstadoServicio = "usp_Comun_ActualizarEstadoServicio";
        public const string UspActualizarEjecucion = "usp_Comun_ActualizarEjecucion";
        public const string UspActualizarEnUso = "dbo.usp_Comun_ActualizarEnUso";
        public const string UspObtenerFechaSistema = "usp_Comun_ObtenerFechaSistema";
        public const string UspObtenerFechaSistemaJuliana = "usp_Comun_ObtenerFechaSistemaJuliana";
        public const string UspObtenerColMonedas = "usp_Comun_ObtenerColMonedas";
        public const string UspObtenerDiasNoHabiles = "usp_Comun_ObtenerDiasNoHabiles";
        public const string UspObtenerParametrosSistema = "usp_Comun_ObtenerParametrosSistema";
        public const string UspTerminarControladoraDeServicios = "usp_comun_TerminarControladoraDeServicios";
        public const string UspObtenerPuerto = "usp_comun_ObtenerPuerto";
        public const string UspInicioProceso = "usp_comun_InicioProceso";
        public const string UspTerminarProceso = "usp_comun_TerminarProceso";
        public const string UspTerminarEjecucion = "usp_comun_TerminarEjecucion";
        public const string UspT24MWOperaciones = "usp_comun_T24_MW_Operaciones";
        public const string UspFechaPasada = "dbo.usp_despachador_MarcarErrorFechaPasada";
    }
}

namespace MUFG_T24.Enums_utilidades
{
    public static class ConstantesParametrosSistema
    {
        public const string HoraOPInicio = "HORA_OP_INICIO";
        public const string HoraOPFin = "HORA_OP_FIN";
        public const string HoraLecInicio = "HORA_LECTURA_INICIO";
        public const string HoraLecFin = "HORA_LECTURA_FIN";
        public const string HoraSaldoInicio = "HORA_SALDO_INICIO";
        public const string HoraSaldoFin = "HORA_SALDO_FIN";
        public const string HoraReplicaInicio = "HORA_REPLICA_INICIO";
        public const string HoraReplicaFin = "HORA_REPLICA_FIN";
        public const string HoraOPCepFin = "HORA_OP_CEP_FIN";

        public const string HoraGphInicio = "HORA_GPH_INICIO";
        public const string HoraGphFin = "HORA_GPH_FIN";
        public const string RutaArchivosGphDescarga = "RUTA_ARCHIVOS_GPH_DESCARGA";
        public const string RutaArchivosGphTransmite = "RUTA_ARCHIVOS_GPH_TRANSMITE";
        public const string RutaLlavesGph = "RUTA_LLAVES_GPH";
        public const string RutaArchivosGphXml = "RUTA_ARCHIVOS_GPH_XML";
        public const string RemotePathSftpGphDownload = "REMOTE_PATH_SFTP_GPH_DOWNLOAD";
        public const string RemoteHostNameGph = "REMOTE_HOST_NAME_GPH";
        public const string UserSftpGph = "USER_SFTP_GPH";
        public const string SshHostPortNumberGph = "SSH_HOST_PORT_NUMBER_GPH";
        public const string SshHostKeyGph = "SSH_HOST_KEY_GPH";
        public const string SshPrivateKeyPathGph = "SSH_PRIVATE_KEY_PATH_GPH";
        public const string RutaArchivosGphDescargaBackup = "RUTA_ARCHIVOS_GPH_DESCARGA_BACKUP";
        public const string GphDescargaEmailErrorTo = "GPH_DESCARGA_EMAIL_ERROR_TO";
        public const string GphDescargaEmailErrorFrom = "GPH_DESCARGA_EMAIL_ERROR_FROM";
        public const string TimeoutSesionSftpGph = "TIMEOUT_SESION_SFTP_GPH";
        public const string LlavePublicaPgp = "LLAVE_PUBLICA_PGP";
        public const string LlavePrivadaPgp = "LLAVE_PRIVADA_PGP";
        public const string ClavePgp = "CLAVE_PGP";
        public const string MailSubject = "MAIL_SUBJECT";
        public const string MailSenderTest = "MAIL_SENDER_TEST";
        public const string MailTo = "MAIL_TO";

        public const string DIGSIG_CERT_NUMSER = "DIGSIG_CERT_NUMSER";
        public const string DIGSIG_CERT_PSW = "DIGSIG_CERT_PSW";
        public const string HORA_SPEI_CANCELACION = "HORA_SPEI_CANCELACION";

        public const string AnalizerAutomaticShutdown = "AnalizerAutomaticShutdown";
        public const string AnalizerAutomaticReactivation = "AnalizerAutomaticReactivation";
        public const string AnalizerAutomaticReactivationTimeSec = "AnalizerAutomaticReactivationTimeSec";
    }
}

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Constantes de nombre de servicio.
    /// </summary>
    public static class ConstantesServicios
    {
        public const string NombreDisplay = "CMS-";

        public const string BTMUM_64 = "BTMUM_64";
        public const string MUFG_T24 = "MUFG_T24";
        public const string CEP = "CEP";
        public const string CEP_ControladorServicios = "CEP_ControladorServicios";
        public const string CEP_Despachador = "CEP_Despachador";
        public const string SPEI_CEP_ENV_CDA = "SPEI_CEP_ENV_CDA";
        public const string SPEI_CEP_GEN_CDA = "SPEI_CEP_GEN_CDA";
        public const string SPEI_CONEXION = "SPEI_CONEXION";
        public const string SPID_CEP_ENV_CDA = "SPID_CEP_ENV_CDA";
        public const string SPID_CEP_GEN_CDA = "SPID_CEP_GEN_CDA";
        public const string EdoCuenta = "EdoCuenta";
        public const string Servicio_Lectura_LUM = "Servicio_Lectura_LUM";
        public const string ServicioEntradasLumina = "ServicioEntradasLumina";
        public const string LUMINA_ACTUALIZAESTATUS = "LUMINA_ACTUALIZAESTATUS";
        public const string LUMDataserver = "LUMDataserver";
        public const string GPH_ArchivosSalida = "GPH_ArchivosSalida";
        public const string GPH_Descarga = "GPH_Descarga";
        public const string GPH_ETL = "GPH_ETL";
        public const string ActualizaEstatus = "ActualizaEstatus";
        public const string Calypso_Conector = "Calypso_Conector";
        public const string Calypso_Forex = "Calypso_Forex";
        public const string Calypso_Liquidacion = "Calypso_Liquidacion";
        public const string Autorizacion = "Autorizacion";
        public const string Comisiones = "Comisiones";
        public const string Conector = "Conector";
        public const string ControladorServicios = "ControladorServicios";
        public const string Despachador = "Despachador";
        public const string Email = "Email";
        public const string Entradas = "Entradas";
        public const string FCM_Conector = "FCM_Conector";
        public const string FondosTransferencias = "FondosTransferencias";
        public const string ForexOnline = "ForexOnline";
        public const string HotScan = "HotScan";
        public const string Impresion = "Impresion";
        public const string Lectura = "Lectura";
        public const string Replicas = "Replicas";
        public const string RSA_eco = "RSA_eco";
        public const string RSA_email = "RSA_email";
        public const string RSA_remplazo_masivo = "RSA_remplazo_masivo";
        public const string Saldos = "Saldos";
        public const string Solicitudes = "Solicitudes";
        public const string SPID_Validador = "SPID_Validador";
        public const string T24 = "T24";
        public const string TAS_Forex = "TAS_Forex";
        public const string TAS_Liquidacion = "TAS_Liquidacion";
        public const string LIDE = "LIDE";
        public const string NEPE = "NEPE";
        public const string REF = "REF";
        public const string SHCP = "SHCP";
        public const string SATValidador = "SATValidador";
        public const string SAT_NEPE = "SAT_NEPE";
        public const string Usuarios = "Usuarios";
        public const string SPID_Accionistas = "SPID_Accionistas";
        public const string SPID_FIEL = "SPID_FIEL";
        public const string Swf_IN_AU = "Swf_IN_AU";
        public const string Swf_IN_ControladorServicios = "Swf_IN_ControladorServicios";
        public const string Swf_IN_ControladorServiciosMQ = "Swf_IN_ControladorServiciosMQ";
        public const string Swf_IN_ETL = "Swf_IN_ETL";
        public const string Swf_IN_FT = "Swf_IN_FT";
        public const string Swf_IN_MQ = "Swf_IN_MQ";
        public const string Swf_IN_MQ_MX = "Swf_IN_MQ_MX";
        public const string LAIKA = "LAIKA";
        public const string Swf_OUT_ControladorServicios = "Swf_OUT_ControladorServicios";
        public const string Swf_OUT_ControladorServiciosMQ = "Swf_OUT_ControladorServiciosMQ";
        public const string Swf_OUT_ETL = "Swf_OUT_ETL";
        public const string Swf_OUT_MQ = "Swf_OUT_MQ";
        public const string Swf_OUT_MQ_GCMS = "Swf_OUT_MQ_GCMS";
        public const string Swf_OUT_PK_DB = "Swf_OUT_PK_DB";
        public const string Swf_OUT_Read = "Swf_OUT_Read";
        public const string Swf_OUT_Send = "Swf_OUT_Send";
        public const string Swf_OUT_ISO = "Swf_OUT_ISO";
        public const string Swift_Api_Rejection = "Swift_Api_Rejection";
        public const string Carga = "Carga";
        public const string Extractor = "Extractor";
        public const string Files = "Files";
        public const string SFTP = "SFTP";
        public const string Calypso_inMT300 = "Calypso_inMT300";
        public const string TomiCalypsoEnvioCorreos = "TomiCalypsoEnvioCorreos";
        public const string Zeus_ControladorServicios = "Zeus_ControladorServicios";
        public const string Zeus_Files = "Zeus_Files";
        public const string ControladorServiciosCDL = "ControladorServiciosCDL";
        public const string CDL_IN_MQ = "CDL_IN_MQ";
    }
}

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Constantes de tablas TOMBIN usadas por FCM.
    /// </summary>
    public static class ConstantesTablasTombin
    {
        public const string EntradasSpei = "tb_tombin_nf_entradas_spei";
        public const string EntradasSpei2 = "tb_tombin_nf_entradas_spei_2";
        public const string Fxd = "tb_tombin_nf_fxd";
        public const string FxdLocal = "tb_tombin_nf_fxd_local";
        public const string FxdGph = "tb_tombin_nf_fxd_gph";
        public const string FxdH2H = "tb_tombin_nf_fxd_h2h";
        public const string FxdNominaDetalle = "tb_tombin_nf_fxd_nomina_detalle";
        public const string FxdPapDetalle = "tb_tombin_nf_fxd_pap_detalle";
        public const string EntradasSpid = "tb_tombin_nf_entradas_spid";
        public const string Spid = "tb_tombin_nf_spid";
        public const string SpidLocal = "tb_tombin_nf_spid_local";
        public const string SpidGph = "tb_tombin_nf_spid_gph";
        public const string SpidH2H = "tb_tombin_nf_spid_h2h";
        public const string SpidPapDetalle = "tb_tombin_nf_spid_pap_detalle";
        public const string EntradasTef = "tb_tombin_nf_entradas_tef";
        public const string Tef = "tb_tombin_nf_tef";
        public const string TefGph = "tb_tombin_nf_tef_gph";
        public const string TefH2H = "tb_tombin_nf_tef_h2h";
        public const string TefPapDetalle = "tb_tombin_nf_tef_pap_detalle";
        public const string SerPay = "tb_tombin_nf_ser_pay";
        public const string TaxPay = "tb_tombin_nf_tax_pay";
    }
}

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Enum para Procedimeintos almacenados
    /// </summary>
    public static class EnumNombreSp
    {
        #region OPERACIONES
        public static readonly string SP_T24_OPERACIONES = "usp_ConsultaOperacionesMWT24_MUFGLibreria";
        public static readonly string SP_T24_CUENTAS_FAD = "usp_ConsultaDatosCuentaFAD_MUFGLibreria";
        public static readonly string SP_T24_CUENTAS_NOSTRO = "usp_ConsultaDatosCuentaNOSTRO_MUFGLibreria";
        public static readonly string SP_T24_CUENTAS_SWF_SALIDA_DESTINO = "usp_ConsultaDireccionesSwfSalida_MUFGLibreria";
        public static readonly string SP_SWF_BUSCA_SW_END2END_REF = "usp_swf_busca_SW_END2END_REF";
        public static readonly string SP_SWF_INSERTA_SW_END2END_REF = "usp_swf_inserta_SW_END2END_REF";
        public static readonly string SP_SWF_ACTUALIZA_SW_END2END_REF = "usp_swf_actualiza_SW_END2END_REF";
        public static readonly string SP_BUSCA_FECHA_T24 = "MUFG_MX2_ConsultaFechaJulian_MUFGLibreria";
        public static readonly string SP_BUSCA_FOLIO_INTERCAMBIO_T24 = "usp_ConsultaFolioIntercambioT24_MUFGLibreria";
        public static readonly string SP_INSERTA_FOLI_OPERACION = "usp_InsertaFolioOperacion_MUFGLibreria";
        public static readonly string SP_ESQUEMA_TRX_T24 = "usp_ConsultaEsquemaComision_MUFGLibreria";
        public static readonly string SP_NET_CUENTA_TRX_T24 = "usp_TNET_COM_BY_ACCT_TRX_TYPE";
        public static readonly string SP_SIN_ESQUEMA_TRX_T24 = "usp_ConsultaSinEsquemaComision_MUFGLibreria";
        public static readonly string SP_GET_VERSION_CONFIG = "usp_ConsultaParametrosVesion_MUFGLibreria";
        public static readonly string SP_GET_DEFAULTS_CONFIG = "usp_ConsultaParametrosVesionPorDefecto_MUFGLibreria";
        public static readonly string SP_OFS_TOMBIN_NF_ENTRADAS = "usp_InsertaOFSTablaTrabajo_MUFGLibreria";
        public static readonly string SP_T24_ADD_TRX = "uSP_T24_ADD_TRX";
        public static readonly string SP_INSERTA_ALERTA = "usp_InsertaAlerta_MUFGLibreria";
        #endregion OPERACIONES

        #region PROCESOS SWF
        public static readonly string SP_VW_T24_SWF_MESSAGES = "MUFG_MX2_ConsultaMensajesSwiftT24_MUFGLibreria";
        #endregion PROCESOS SWF

        #region SPID
        public static readonly string SP_SPID_CLIENTES = "usp_ConsultaEstatusFIELSPIDClientes_MUFGLibreria";
        public static readonly string SP_SPID_ACCIONISTAS_VALIDADOS = "usp_ConsultaEstatusAccionistasValidados_MUFGLibreria";
        public static readonly string SP_SPID_SOLICITUDES_CUENTAS_CLIENTES = "usp_ConsultaCDLCuentaSPID_MUFGLibreria";
        #endregion SPID

        #region CUENTA CLIENTE
        public static readonly string SP_T24_CUSTOMER_ACCOUNT = "MUFG_MX2_ConsultaDatosCuentaCliente_MUFGLibreria";
        public static readonly string SP_T24_DATOS_CUENTA_CLIENTE = "MUFG_MX2_ConsultaDatosNumeroCuentaCliente_MUFGLibreria";
        public static readonly string SP_T24_ObtenerCustomerT24 = "MUFG_MX2__ObtenerCustomerT24";
        public static readonly string SP_TOMI_VW_ADDRESS_CUSTOMER_ALL = "MUFG_MX2_ConsultaDireccionCuentaCliente_MUFGLibreria";
        #endregion CUENTA CLIENTE

        #region FONDOS
        public static readonly string SP_T24_FUNDS_TRANSFER_ID = "MUFG_MX2_ConsultaCuentaDebitoFT_MUFGLibreria";
        public static readonly string SP_T24_FUNDS_TRANSFER = "MUFG_MX2_ConsultaFTT24MW_MUFGLibreria";
        public static readonly string SP_T24_LOANS_AND_DEPOSITS = "MUFG_MX2_ConsultaCuentaPrestamoDeposito_MUFGLibreria";
        #endregion FONDOS

        #region FOREX
        public static readonly string SP_CMM_SOLICITUDESFOREX = "usp_ConsultaEstatusSolicitudesForex_MUFGLibreria";

        #endregion FOREX

        #region INTERCAMBIO
        public static readonly string SP_INTERCAMBIO = "usp_ConsultaEstatusMensajeTblIntercambio_MUFGLibreria";
        #endregion INTERCAMBIO

        #region PARAMETROS
        public static readonly string SP_ACTUALIZA_EJECUCION = "usp_ActualizaEjecucion_MUFGLibreria";
        public static readonly string SP_ACTUALIZA_TERMINACION = "usp_ActualizaTerminacion_HotScan";
        public static readonly string SP_OBTENER_PUERTO = "usp_ConsultaPuertoDisponible_MUFGLibreria";
        public static readonly string SP_ESCUCHA_PUERTO = "usp_ActualizaPuertoEscucha_MUFGLibreria";
        public static readonly string SP_OBTENER_FOLIO_MAESTRO = "usp_ConsultaFolioMaestro_MUFGLibreria";
        public static readonly string SP_OBTENER_FT_SPEI = "usp_ConsultaFTSPEI_MUFGLibreria";
        public static readonly string SP_OBTENER_ULTIMO_PROCESO = "usp_ConsultaAliasUltimoProceso_MUFGLibreria";
        public static readonly string SP_INICIA_PROCESO = "usp_ActualizaInicioProceso_MUFGLibreria";
        public static readonly string SP_TERMINA_PROCESO = "usp_ActualizaTerminaProceso_MUFGLibreria";
        public static readonly string SP_ACTUALIZA_ERROR_PROCESO = "usp_ActualizaErrorProceso_MUFGLibreria";
        public static readonly string SP_ACTUALIZA_ERROR_PROCESO_SWF = "usp_ActualizaErrorProcesoSwift_MUFGLibreria";
        public static readonly string SP_ULTIMO_PROCESO_SWF = "usp_ConsultaUltimoProcesoSwift_MUFGLibreria";
        public static readonly string SP_INICIA_PROCESO_SWF = "[usp_ActualizaInicioProcesoSwift_MUFGLibreria]";
        public static readonly string SP_TERMINA_PROCESO_SWF = "usp_ActualizaTerminaProcesoSwift_MUFGLibreria";
        public static readonly string SP_ES_DIA_HABIL = "usp_EsDiaHabil";
        public static readonly string SP_FECHA_SERVIDOR = "usp_ConsultaFechaServidorSQL_MUFGLibreria";
        #endregion PARAMETROS
    }
}

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Enum para Servicios
    /// </summary>
    public enum NombreServicio
    {
        BTMUM_64, MUFG_T24, CEP, CEP_ControladorServicios, CEP_Despachador, SPEI_CEP_ENV_CDA,
        SPEI_CEP_GEN_CDA, SPEI_CONEXION, SPID_CEP_ENV_CDA, SPID_CEP_GEN_CDA, EdoCuenta, Servicio_Lectura_LUM,
        ServicioEntradasLumina, LUMINA_ACTUALIZAESTATUS, LUMDataserver, GPH_ArchivosSalida, GPH_Descarga,
        GPH_ETL, ActualizaEstatus, Calypso_Conector, Calypso_Forex, Calypso_Liquidacion, Autorizacion,
        Comisiones, Conector, ControladorServicios, Despachador, Email, Entradas, FCM_Conector, FondosTransferencias,
        ForexOnline, HotScan, Impresion, Lectura, Replicas, RSA_eco, RSA_email, RSA_remplazo_masivo, Saldos,
        Solicitudes, SPID_Validador, T24, TAS_Forex, TAS_Liquidacion, LIDE, NEPE, REF, SHCP, SATValidador, Usuarios,
        SPID_Accionistas, SPID_FIEL, Swf_IN_AU, Swf_IN_ControladorServicios, Swf_IN_ControladorServiciosMQ,
        Swf_IN_ETL, Swf_IN_FT, Swf_IN_MQ, Swf_IN_MQ_MX, LAIKA, Swf_OUT_ControladorServicios, Swf_OUT_ControladorServiciosMQ,
        Swf_OUT_ETL, Swf_OUT_MQ, Swf_OUT_MQ_GCMS, Swf_OUT_PK_DB, Swf_OUT_Read, Swf_OUT_Send, Swf_OUT_ISO,
        Swift_Api_Rejection, Carga, Extractor, Files, SFTP, Calypso_inMT300, TomiCalypsoEnvioCorreos, Zeus_ControladorServicios, Zeus_Files,
        Conector_HSM, Conector_RSA_Admin,Conector_RSA_Auth, Conector_RSA_Sync
    }
}

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Enum para centralizar conexiones a BD
    /// </summary>
    public enum NombreBasedatos
    {
        TOMI, CMS, ROUTER, T24_VW, MW, DWH, SPEI, SPEI1, SPEI2, STG, ValidacionSAT, SPID, TEF, INSIGHT, MINDS, REUTERS, DEALTRACKER, CALYPSO, C360, TOMSER, BOTLOG
    }
}

using MUFG_T24.Enums_utilidades;
using System;
using System.Diagnostics;
using System.Reflection;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.ControladorServicios_Utilidades
{
    /// <summary>
    /// Base para centralizar el ciclo de vida y manejo robusto de errores de un Windows Service.
    /// </summary>
    public abstract class WindowsServiceHostBase : ServiceBase
    {
        private CancellationTokenSource _cts;
        private System.Timers.Timer _timerServicio;
        private Task _startupTask = Task.CompletedTask;
        private Task _runningTask = Task.CompletedTask;
        private Task _backgroundTask = Task.CompletedTask;
        private readonly SemaphoreSlim _tickLock = new SemaphoreSlim(1, 1);
        private int _startupFailureSignaled;

        protected abstract string NombreServicioLog { get; }
        protected abstract bool TimerEnabled { get; }
        protected abstract double TimerIntervalMs { get; }
        protected virtual TimeSpan WaitTimeout => TimeSpan.FromSeconds(60);
        protected virtual TimeSpan CleanupTimeout => TimeSpan.FromSeconds(30);

        /// <summary>
        /// Habilita la validación centralizada del contrato de inicialización:
        /// Pid, NombreProceso y NombreDisplay.
        /// </summary>
        protected virtual bool ValidarContratoInicializacion => true;

        /// <summary>
        /// Indica si el servicio ejecuta una tarea de fondo de larga duración (ej. un TCP listener)
        /// además o en lugar del timer. Por defecto deshabilitado para preservar el comportamiento
        /// de los servicios basados en timer existentes.
        /// </summary>
        protected virtual bool EjecutarTareaLarga => false;

        /// <summary>
        /// Tarea de fondo de larga duración (ej. loop de un TCP listener). Sólo se invoca cuando
        /// <see cref="EjecutarTareaLarga"/> es true.
        /// </summary>
        protected virtual Task TareaLargaAsync(CancellationToken ct) => Task.CompletedTask;

        /// <summary>
        /// Permite señalizar de forma síncrona la detención de la tarea de fondo (ej. detener un
        /// TcpListener) para desbloquear operaciones de espera bloqueantes como AcceptTcpClientAsync.
        /// </summary>
        protected virtual void DetenerTareaLarga() { }

        /// <summary>
        /// Indica si el servicio cuenta con los recursos mínimos para ejecutar la terminación particular.
        /// </summary>
        protected virtual bool InicializacionCompleta => true;

        /// <summary>
        /// Libera recursos particulares del servicio al finalizar el ciclo de terminación.
        /// </summary>
        protected virtual void LiberarRecursosTerminacion() { }

        protected abstract void Inicializacion(string[] args, CancellationToken ct);
        protected abstract Task ParametrizacionesAsync(CancellationToken ct);
        protected abstract Task OnTimerAsync(CancellationToken ct);
        protected abstract Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask);

#if DEBUG
        private bool _debugSingleShot;
#endif

        /// <summary>
        /// Utilidad para ejecutar como consola en modo DEBUG.
        /// </summary>
        public void DebugRun(string[] args)
        {
#if DEBUG
            if (!Environment.UserInteractive)
                throw new InvalidOperationException("DebugRun sólo debe ejecutarse desde una consola interactiva en modo DEBUG.");

            OnStart(args);
            Console.WriteLine("Servicio iniciado en modo DEBUG. Presiona ENTER para detener...");
            Console.ReadLine();
            OnStop();
#else
            throw new InvalidOperationException("DebugRun no está disponible en compilaciones RELEASE.");
#endif
        }

        /// <summary>
        /// Contrato OnStart.
        /// </summary>
        protected override void OnStart(string[] args)
        {
            try
            {
                ServiceName = NombreServicioLog;

                _cts = new CancellationTokenSource();
                _startupTask = Task.Run(() => StartupAsync(args, _cts.Token));
            }
            catch (Exception ex)
            {
                HandleStartupFailure(ex);
            }
        }

        /// <summary>
        /// Contrato OnStop.
        /// </summary>
        protected override void OnStop()
        {
            try
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|OnStop|Finalizando Servicio.");

                var waitTimeout = WaitTimeout;
                var cleanupTimeout = CleanupTimeout;

                if (!Environment.UserInteractive)
                    RequestAdditionalTime((int)(waitTimeout + cleanupTimeout + TimeSpan.FromSeconds(5)).TotalMilliseconds);

                _timerServicio?.Stop();
                _cts?.Cancel();
                DetenerTareaLarga();

                using (CancellationTokenSource waitCts = new CancellationTokenSource(waitTimeout))
                using (CancellationTokenSource cleanupCts = new CancellationTokenSource(cleanupTimeout))
                {
                    TimeSpan totalTimeout = waitTimeout + cleanupTimeout;
                    Task terminacionTask = Task.Run(() => EjecutarTerminacionAsync(waitCts.Token, cleanupCts.Token, _startupTask, Task.WhenAll(_runningTask, _backgroundTask)));

                    Task completedTask = Task.WhenAny(terminacionTask, Task.Delay(totalTimeout)).GetAwaiter().GetResult();

                    if (completedTask != terminacionTask)
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"WARN|OnStop|La terminación excedió {totalTimeout.TotalSeconds} segundos. Se continúa cierre para no bloquear SCM.");
                        return;
                    }

                    terminacionTask.GetAwaiter().GetResult();
                }

                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|OnStop|Terminación exitosa de bases de datos.");
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|OnStop|{ex}");
            }
            finally
            {
                _timerServicio?.Dispose();
                _cts?.Dispose();
            }
        }

        private async Task EjecutarTerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            try
            {
                if (!InicializacionCompleta)
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|OnStop|Terminación omitida por inicialización incompleta.");
                    return;
                }

                await TerminacionAsync(waitCt, cleanupCt, startupTask, runningTask).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                //Apagado normal
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|TerminacionAsync|{ex}");
            }
            finally
            {
                try
                {
                    LiberarRecursosTerminacion();
                }
                catch (Exception ex)
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|LiberarRecursosTerminacion|{ex}");
                }
            }
        }

        /// <summary>
        /// Espera a que una tarea activa finalice o se cancele por timeout.
        /// </summary>
        protected static async Task EsperarTareaActivaAsync(Task task, string descripcion, CancellationToken ct)
        {
            if (task == null || task.IsCompleted)
                return;

            Task timeoutTask = Task.Delay(Timeout.Infinite, ct);
            Task completedTask = await Task.WhenAny(task, timeoutTask).ConfigureAwait(false);

            if (completedTask != task)
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"WARN|OnStop|Timeout esperando terminación de {descripcion}.");
        }

        /// <summary>
        /// Wrapper público del método de espera de tarea activa.
        /// </summary>
        public static Task EsperarTareaActivaWrapperAsync(Task task, string descripcion, CancellationToken ct)
            => EsperarTareaActivaAsync(task, descripcion, ct);

        private void InicializarTimer()
        {
            _timerServicio = new System.Timers.Timer
            {
                Enabled = false,
                AutoReset = true,
                Interval = TimerIntervalMs
            };

            _timerServicio.Elapsed += async (sender, e) =>
            {
                if (_cts?.IsCancellationRequested == true)
                    return;

                try
                {
                    await OnTimerElapsedAsync(_cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Apagado normal
                }
                catch (Exception ex)
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Elapsed|{ex}");
                }
            };
        }

        private async Task StartupAsync(string[] args, CancellationToken ct)
        {
            try
            {
                Inicializacion(args, ct);
                InicializarTimer();

                await ParametrizacionesAsync(ct).ConfigureAwait(false);

                if (ValidarContratoInicializacion)
                    ValidarContratoInicializacionProceso();

                if (!ct.IsCancellationRequested && TimerEnabled && _timerServicio != null)
                {
                    try
                    {
                        _timerServicio.Start();
                    }
                    catch (ObjectDisposedException)
                    {
                        // Apagado normal
                    }
                }

                if (!ct.IsCancellationRequested && EjecutarTareaLarga)
                {
                    _backgroundTask = TareaLargaAsync(ct);
                }
            }
            catch (OperationCanceledException)
            {
                // Apagado normal
            }
            catch (Exception ex)
            {
                HandleStartupFailure(ex);
                throw;
            }
        }

        private void ValidarContratoInicializacionProceso()
        {
            object configuracion = ObtenerConfiguracionProceso();
            if (configuracion == null)
            {
                throw new InvalidOperationException("No fue posible validar el contrato de inicialización: no se encontró un objeto de configuración con propiedades Pid, NombreProceso y NombreDisplay.");
            }

            Type tipo = configuracion.GetType();
            PropertyInfo propiedadPid = tipo.GetProperty("Pid", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            PropertyInfo propiedadNombreProceso = tipo.GetProperty("NombreProceso", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            PropertyInfo propiedadNombreDisplay = tipo.GetProperty("NombreDisplay", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

            object pid = propiedadPid?.GetValue(configuracion, null);
            string nombreProceso = propiedadNombreProceso?.GetValue(configuracion, null) as string;
            string nombreDisplay = propiedadNombreDisplay?.GetValue(configuracion, null) as string;

            int pidNumero = pid is int pidEntero ? pidEntero : 0;
            bool pidValido = pidNumero > 0;
            if (!pidValido || string.IsNullOrWhiteSpace(nombreProceso) || string.IsNullOrWhiteSpace(nombreDisplay))
            {
                throw new InvalidOperationException($"Contrato de inicialización inválido. Pid='{pid}', NombreProceso='{nombreProceso}', NombreDisplay='{nombreDisplay}'.");
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Inicializacion|Contrato validado: PID={pidNumero}, NombreProceso='{nombreProceso}', NombreDisplay='{nombreDisplay}'.");
        }

        private object ObtenerConfiguracionProceso()
        {
            Type hostType = GetType();
            BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

            object porConvencion = hostType.GetField("_configuracionBase", flags)?.GetValue(this)
                                ?? hostType.GetField("_configuracion", flags)?.GetValue(this)
                                ?? hostType.GetProperty("Configuracion", flags)?.GetValue(this, null)
                                ?? hostType.GetProperty("ConfiguracionBase", flags)?.GetValue(this, null);
            if (porConvencion != null)
                return porConvencion;

            foreach (FieldInfo field in hostType.GetFields(flags))
            {
                object value = field.GetValue(this);
                if (EsConfiguracionConContrato(value))
                    return value;
            }

            foreach (PropertyInfo property in hostType.GetProperties(flags))
            {
                if (property.GetIndexParameters().Length > 0)
                    continue;

                object value;
                try
                {
                    value = property.GetValue(this, null);
                }
                catch
                {
                    continue;
                }

                if (EsConfiguracionConContrato(value))
                    return value;
            }

            return null;
        }

        private static bool EsConfiguracionConContrato(object instancia)
        {
            if (instancia == null)
                return false;

            Type tipo = instancia.GetType();
            BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

            return tipo.GetProperty("Pid", flags) != null
                && tipo.GetProperty("NombreProceso", flags) != null
                && tipo.GetProperty("NombreDisplay", flags) != null;
        }

        private async Task OnTimerElapsedAsync(CancellationToken ct)
        {
#if DEBUG
            if (_debugSingleShot)
                return;

            _debugSingleShot = true;
#endif

            if (!await _tickLock.WaitAsync(0, ct).ConfigureAwait(false))
                return;

            try
            {
                await _startupTask.ConfigureAwait(false);
                _runningTask = OnTimerAsync(ct);
                await _runningTask.ConfigureAwait(false);
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Realiza_timer|Se presenta exception: {ex}");
            }
            finally
            {
                _runningTask = Task.CompletedTask;
                _tickLock.Release();
            }
        }

        private void HandleStartupFailure(Exception ex)
        {
            if (Interlocked.Exchange(ref _startupFailureSignaled, 1) == 1)
                return;

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|OnStart|{ex}");
            TryWriteEventLog($"Error en OnStart: {ex}");

            ExitCode = 1;
            _ = Task.Run(() => Stop());
        }

        private void TryWriteEventLog(string message)
        {
            try
            {
                if (!EventLog.SourceExists(NombreServicioLog))
                {
                    EventLog.CreateEventSource(NombreServicioLog, "Application");
                }

                EventLog.WriteEntry(NombreServicioLog,
                    message,
                    EventLogEntryType.Error);
            }
            catch
            {
                //No-op
            }
        }
    }
}

using System;
using System.Diagnostics;
using System.Security.AccessControl;
using System.Security.Principal;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.ControladorServicios_Utilidades
{
    /// <summary>
    /// Ejecuta servicios Windows con manejo robusto de excepciones globales.
    /// </summary>
    public static class WindowsServiceProgramRunner
    {
        /// <summary>
        /// Ejecuta el host del servicio con manejo común de errores.
        /// </summary>
        public static void Run(string serviceName, Func<ServiceBase> serviceFactory, string[] args, Action<ServiceBase, string[]> debugRunner = null)
        {
            if (string.IsNullOrWhiteSpace(serviceName))
                throw new ArgumentException("El nombre del servicio es obligatorio para controlar la instancia única.", nameof(serviceName));

            if (serviceFactory == null)
                throw new ArgumentNullException(nameof(serviceFactory));

            RegisterGlobalExceptionHandlers(serviceName);

            string mutexName = ConstruirNombreMutex(serviceName);
            using (Mutex instanceMutex = CrearMutexGlobal(mutexName))
            {
                bool mutexAdquirido = false;
                try
                {
                    try
                    {
                        mutexAdquirido = instanceMutex.WaitOne(0, false);
                    }
                    catch (AbandonedMutexException)
                    {
                        mutexAdquirido = true;
                    }

                    if (!mutexAdquirido)
                    {
                        string mensaje = $"Ya existe una instancia en ejecución del servicio '{serviceName}'. Mutex='{mutexName}'.";
                        TryWriteEventLog(serviceName, mensaje);
                        Trace.TraceError(mensaje);
                        Environment.ExitCode = 1;
                        return;
                    }

#if DEBUG
                    var service = serviceFactory();

                    if (debugRunner != null)
                        debugRunner(service, args);
                    else
                        ServiceBase.Run(service);
#else
                    ServiceBase.Run(serviceFactory());
#endif
                }
                catch (Exception ex)
                {
                    TryWriteEventLog(serviceName, ex.ToString());
                    Environment.ExitCode = 1;
                }
                finally
                {
                    if (mutexAdquirido)
                        instanceMutex.ReleaseMutex();
                }
            }
        }

        private static Mutex CrearMutexGlobal(string mutexName)
        {
            var mutexSecurity = new MutexSecurity();
            var authenticatedUsers = new SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, null);
            mutexSecurity.AddAccessRule(new MutexAccessRule(
                authenticatedUsers,
                MutexRights.Synchronize | MutexRights.Modify,
                AccessControlType.Allow));

            bool creado;
            return new Mutex(false, mutexName, out creado, mutexSecurity);
        }

        private static string ConstruirNombreMutex(string serviceName)
        {
            var nombreNormalizado = new StringBuilder(serviceName.Length);
            foreach (char caracter in serviceName.Trim().ToUpperInvariant())
            {
                nombreNormalizado.Append(char.IsLetterOrDigit(caracter) || caracter == '-' || caracter == '_'
                    ? caracter
                    : '_');
            }

            return $"Global\\MUFG_SERVICE_{nombreNormalizado}";
        }

        private static void RegisterGlobalExceptionHandlers(string serviceName)
        {
            AppDomain.CurrentDomain.UnhandledException += (sender, e) =>
            {
                var ex = e.ExceptionObject as Exception;
                var message = $"UnhandledException|Terminating={e.IsTerminating}|{ex ?? new Exception("Excepción no administrada no tipada.")}";
                TryWriteEventLog(serviceName, message);
            };

            TaskScheduler.UnobservedTaskException += (sender, e) =>
            {
                try
                {
                    TryWriteEventLog(serviceName, $"UnobservedTaskException|{e.Exception}");
                }
                finally
                {
                    e.SetObserved();
                }
            };
        }

        private static void TryWriteEventLog(string source, string message)
        {
            try
            {
                if (!EventLog.SourceExists(source))
                {
                    EventLog.CreateEventSource(source, "Application");
                }

                EventLog.WriteEntry(source, message, EventLogEntryType.Error);
            }
            catch (Exception logEx)
            {
                Trace.TraceError($"Error registrando en EventLog: {logEx}");
                Trace.TraceError(message);
            }
        }
    }
}


using MUFG_T24.Enums_utilidades;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.ControladorServicios_Utilidades
{
    /// <summary>
    /// Base para servicios tipo despachador que trabajan por señalización (semáforo)
    /// en lugar de procesamiento periódico por timer.
    /// </summary>
    public abstract class WindowsServiceDespachador : WindowsServiceHostBase
    {
        private readonly SemaphoreSlim _semaforoDespacho = new SemaphoreSlim(0, 1);
        private readonly object _wakeLock = new object();

        protected override bool TimerEnabled => false;
        protected override double TimerIntervalMs => 1000;
        protected override bool EjecutarTareaLarga => true;

        /// <summary>
        /// Si se habilita, publica una señal periódica para evitar inanición
        /// cuando haya pendientes en memoria y no se haya emitido una señal explícita.
        /// </summary>
        protected virtual bool HabilitarDespertarSafety => true;

        /// <summary>
        /// Intervalo del despertar de seguridad.
        /// </summary>
        protected virtual TimeSpan IntervaloDespertarSafety => TimeSpan.FromSeconds(30);

        protected sealed override Task OnTimerAsync(CancellationToken ct) => Task.CompletedTask;

        /// <summary>
        /// Señaliza al loop del despachador para procesar una iteración.
        /// </summary>
        protected void DespertarDespachador()
        {
            lock (_wakeLock)
            {
                if (_semaforoDespacho.CurrentCount == 0)
                    _semaforoDespacho.Release();
            }
        }

        protected override void DetenerTareaLarga()
        {
            DespertarDespachador();
        }

        protected sealed override async Task TareaLargaAsync(CancellationToken ct)
        {
            Task safetyTask = Task.CompletedTask;

            if (HabilitarDespertarSafety)
            {
                safetyTask = Task.Run(async () =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        try
                        {
                            await Task.Delay(IntervaloDespertarSafety, ct).ConfigureAwait(false);
                            DespertarDespachador();
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (Exception ex)
                        {
                            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|SafetyDespachador|{ex}");
                        }
                    }
                }, ct);
            }

            try
            {
                await AntesDeLoopDespachadorAsync(ct).ConfigureAwait(false);

                while (!ct.IsCancellationRequested)
                {
                    await _semaforoDespacho.WaitAsync(ct).ConfigureAwait(false);
                    if (ct.IsCancellationRequested)
                        break;

                    await IteracionDespachadorAsync(ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Apagado normal
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|LoopDespachador|{ex}");
                throw;
            }
            finally
            {
                try
                {
                    await AlFinalizarLoopDespachadorAsync(ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|FinalizarLoopDespachador|{ex}");
                }

                try
                {
                    await safetyTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Apagado normal
                }
                catch (Exception ex)
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|SafetyDespachadorStop|{ex}");
                }
            }
        }

        /// <summary>
        /// Hook de inicialización previa al loop del despachador.
        /// </summary>
        protected virtual Task AntesDeLoopDespachadorAsync(CancellationToken ct) => Task.CompletedTask;

        /// <summary>
        /// Hook de limpieza posterior al loop del despachador.
        /// </summary>
        protected virtual Task AlFinalizarLoopDespachadorAsync(CancellationToken ct) => Task.CompletedTask;

        /// <summary>
        /// Iteración principal del loop del despachador activada por semáforo.
        /// </summary>
        protected virtual Task IteracionDespachadorAsync(CancellationToken ct) => Task.CompletedTask;
    }
}


using System;
using System.Collections.Generic;

namespace MUFG_T24.Modelos
{
    /// <summary>
    /// Resultado estándar del despachador de paquetes TCP.
    /// </summary>
    public enum ConstantesRespuestaResultadoDespachador
    {
        OK = 1,
        PARCIAL = 2,
        ERROR = 3
    }

    /// <summary>
    /// Solicitud estándar para procesamiento por paquete TCP.
    /// </summary>
    public class SolicitudPaqueteDto
    {
        public string Servicio { get; set; }
        public string Tipo { get; set; }
        public long PID { get; set; }
        public DateTime EnviadoEn { get; set; }
        public IEnumerable<SolicitudOperacionDto> Operaciones { get; set; }
        public int Version { get; set; } = 1;
    }

    /// <summary>
    /// Operación incluida en una solicitud de paquete TCP.
    /// </summary>
    public class SolicitudOperacionDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
    }

    /// <summary>
    /// Respuesta estándar para procesamiento por paquete TCP.
    /// </summary>
    public class RespuestaPaqueteDto
    {
        public int Version { get; set; } = 1;
        public string Servicio { get; set; }
        public string Tipo { get; set; }
        public int Resultado { get; set; }
        public RespuestaResumenDto Resumen { get; set; }
        public RespuestaTiempoDto Tiempos { get; set; }
        public List<RespuestaErrorDto> Errores { get; set; } = new List<RespuestaErrorDto>();
    }

    /// <summary>
    /// Resumen estándar de procesamiento por paquete TCP.
    /// </summary>
    public class RespuestaResumenDto
    {
        public RespuestaResumenDto() { }

        public RespuestaResumenDto(int total)
        {
            Total = total;
        }

        public int Total { get; set; }
        public int Procesadas { get; set; }
        public int Ok { get; set; }
        public int Error { get; set; }
        public bool FallaCritica { get; set; }
        public string MensajeFallaCritica { get; set; }

        public int CalcularResultado()
        {
            if (FallaCritica)
                return (int)ConstantesRespuestaResultadoDespachador.ERROR;

            if (Error == 0)
                return (int)ConstantesRespuestaResultadoDespachador.OK;

            if (Ok > 0)
                return (int)ConstantesRespuestaResultadoDespachador.PARCIAL;

            return (int)ConstantesRespuestaResultadoDespachador.ERROR;
        }
    }

    /// <summary>
    /// Tiempos estándar de respuesta por paquete TCP.
    /// </summary>
    public class RespuestaTiempoDto
    {
        public DateTime Inicio { get; set; }
        public DateTime Fin { get; set; }
        public long DuracionMs => (long)(Fin - Inicio).TotalMilliseconds;
    }

    /// <summary>
    /// Error estándar de respuesta por paquete TCP.
    /// </summary>
    public class RespuestaErrorDto
    {
        public long Folio { get; set; }
        public string Codigo { get; set; }
        public string Mensaje { get; set; }
        public bool Reintentable { get; set; }
    }

}


using MUFG_T24.Encriptacion_utilidades;
using MUFG_T24.Enums_utilidades;
using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.ControladorServicios_Utilidades
{
    /// <summary>
    /// Base para servicios TCP que procesan la solicitud como string desencriptado.
    /// </summary>
    public abstract class WindowsTcpStringServiceHostBase : WindowsTcpServiceHostBase
    {
        protected override bool ValidarContratoInicializacion => true;

        protected override async Task ProcesarClienteAsync(TcpClient cliente, CancellationToken token)
        {
            using (cliente)
            using (var stream = cliente.GetStream())
            {
                try
                {
                    string solicitud = await LeerSolicitudAsync(stream, token).ConfigureAwait(false);
                    string respuesta = await ProcesarSolicitudAsync(solicitud, token).ConfigureAwait(false);
                    await EnviarRespuestaAsync(stream, respuesta ?? string.Empty, token).ConfigureAwait(false);
                }
                catch (Exception ex) when (!(ex is OperationCanceledException))
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ProcesarClienteAsync|{ex}");
                    await EnviarRespuestaAsync(stream, string.Empty, token).ConfigureAwait(false);
                }
            }
        }

        protected abstract Task<string> ProcesarSolicitudAsync(string solicitud, CancellationToken token);

        private async Task<string> LeerSolicitudAsync(NetworkStream stream, CancellationToken token)
        {
            byte[] lengthBuffer = await ReadExactAsync(stream, 4, token).ConfigureAwait(false);
            int length = BitConverter.ToInt32(lengthBuffer, 0);

            if (length <= 0)
                throw new InvalidDataException("La longitud del paquete recibido debe ser mayor a cero.");

            if (length > MaxFrameSizeBytes)
                throw new InvalidDataException($"La longitud del paquete recibido ({length}) excede el máximo permitido ({MaxFrameSizeBytes}).");

            byte[] payloadBuffer = await ReadExactAsync(stream, length, token).ConfigureAwait(false);
            string payload = Encoding.UTF8.GetString(payloadBuffer);

            string llave = Encriptacion.GenerarLlave();
            return Encriptacion.Decrypt(payload, llave);
        }
    }
}

using MUFG_T24.Enums_utilidades;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.ControladorServicios_Utilidades
{
    /// <summary>
    /// Base para servicios Windows que exponen un listener TCP de larga duración.
    /// </summary>
    public abstract class WindowsTcpServiceHostBase : WindowsServiceHostBase
    {
        private TcpListener _tcpListener;
        private SemaphoreSlim _limiteConcurrencia;
        private int? _puertoTcp;
        private int _limiteConcurrenciaTcp = 5;

        protected override bool TimerEnabled => false;
        protected override double TimerIntervalMs => 1000;
        protected override bool EjecutarTareaLarga => true;

        protected virtual IPAddress DireccionTcp => IPAddress.Loopback;
        protected virtual int MaxFrameSizeBytes => 1024 * 1024;

        protected void ConfigurarTcpListener(int puerto, int limiteConcurrencia = 5)
        {
            if (puerto <= 0)
                throw new ArgumentOutOfRangeException(nameof(puerto), "El puerto TCP debe ser mayor a cero.");

            _puertoTcp = puerto;
            _limiteConcurrenciaTcp = Math.Max(1, limiteConcurrencia);
            _limiteConcurrencia = new SemaphoreSlim(_limiteConcurrenciaTcp);
        }

        protected override Task OnTimerAsync(CancellationToken ct) => Task.CompletedTask;

        protected override Task TareaLargaAsync(CancellationToken ct)
        {
            if (!_puertoTcp.HasValue)
                throw new InvalidOperationException("Debe invocarse ConfigurarTcpListener durante ParametrizacionesAsync antes de iniciar el listener TCP.");

            return TcpListenerStartAsync(_puertoTcp.Value, ct);
        }

        protected override void DetenerTareaLarga()
        {
            try { _tcpListener?.Stop(); } catch { }
        }

        protected async Task EsperarClientesActivosAsync(CancellationToken ct)
        {
            SemaphoreSlim limiteConcurrencia = ObtenerLimiteConcurrencia();

            for (int i = 0; i < _limiteConcurrenciaTcp; i++)
            {
                await limiteConcurrencia.WaitAsync(ct).ConfigureAwait(false);
            }
        }

        protected abstract Task ProcesarClienteAsync(TcpClient cliente, CancellationToken token);

        protected async Task<byte[]> ReadExactAsync(NetworkStream stream, int length, CancellationToken ct)
        {
            if (length <= 0)
                throw new InvalidDataException("La longitud del paquete recibido debe ser mayor a cero.");

            if (length > MaxFrameSizeBytes)
                throw new InvalidDataException($"La longitud del paquete recibido ({length}) excede el máximo permitido ({MaxFrameSizeBytes}).");

            byte[] buffer = new byte[length];
            int offset = 0;

            while (offset < length)
            {
                int read = await stream.ReadAsync(buffer, offset, length - offset, ct).ConfigureAwait(false);
                if (read == 0)
                    throw new IOException("Cliente cerró la conexión");

                offset += read;
            }

            return buffer;
        }

        protected async Task EnviarRespuestaAsync(NetworkStream stream, string respuesta, CancellationToken ct)
        {
            try
            {
                byte[] data = Encoding.UTF8.GetBytes(respuesta);
                byte[] length = BitConverter.GetBytes(data.Length);

                await stream.WriteAsync(length, 0, length.Length, ct).ConfigureAwait(false);
                await stream.WriteAsync(data, 0, data.Length, ct).ConfigureAwait(false);
                await stream.FlushAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|EnviarRespuestaAsync|{ex}");
            }
        }

        private async Task TcpListenerStartAsync(int puerto, CancellationToken token)
        {
            _tcpListener = new TcpListener(DireccionTcp, puerto);
            _tcpListener.Start();

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|TcpListenerStartAsync|Listener TCP activo en {DireccionTcp}:{puerto}");

            try
            {
                using (token.Register(() => { try { _tcpListener.Stop(); } catch { } }))
                {
                    while (!token.IsCancellationRequested)
                    {
                        TcpClient client = null;

                        try
                        {
                            client = await _tcpListener.AcceptTcpClientAsync().ConfigureAwait(false);
                        }
                        catch (SocketException ex) when (!token.IsCancellationRequested)
                        {
                            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|TcpListenerStartAsync|Listener TCP activo en {DireccionTcp}:{puerto}, error: {ex.Message}");
                        }
                        catch (ObjectDisposedException) when (token.IsCancellationRequested)
                        {
                            break;
                        }
                        catch (Exception ex) when (!(ex is OperationCanceledException))
                        {
                            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|TcpListenerStartAsync|{ex}");
                        }

                        if (client != null)
                            _ = DispatcherAsync(client, token);
                    }
                }
            }
            finally
            {
                try { _tcpListener.Stop(); } catch { }
            }
        }

        private async Task DispatcherAsync(TcpClient cliente, CancellationToken token)
        {
            bool adquirido = false;
            SemaphoreSlim limiteConcurrencia = ObtenerLimiteConcurrencia();

            try
            {
                await limiteConcurrencia.WaitAsync(token).ConfigureAwait(false);
                adquirido = true;
                await ProcesarClienteAsync(cliente, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                try { cliente?.Close(); } catch { }
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|DispatcherAsync|{ex}");
            }
            finally
            {
                if (adquirido)
                    limiteConcurrencia.Release();
            }
        }

        private SemaphoreSlim ObtenerLimiteConcurrencia()
        {
            if (_limiteConcurrencia == null)
                _limiteConcurrencia = new SemaphoreSlim(_limiteConcurrenciaTcp);

            return _limiteConcurrencia;
        }
    }
}


using MUFG_T24.Encriptacion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Modelos;
using System;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.ControladorServicios_Utilidades
{
    /// <summary>
    /// Base para servicios TCP que procesan el contrato estándar de paquete solicitud/respuesta.
    /// </summary>
    public abstract class WindowsTcpPaqueteServiceHostBase : WindowsTcpServiceHostBase
    {
        protected override bool ValidarContratoInicializacion => true;
        protected virtual string NombreServicioRespuesta => NombreServicioLog;

        protected override async Task ProcesarClienteAsync(TcpClient cliente, CancellationToken token)
        {
            using (cliente)
            using (var stream = cliente.GetStream())
            {
                DateTime inicio = DateTime.UtcNow;

                try
                {
                    SolicitudPaqueteDto paquete = await LeerSolicitudAsync(stream, token).ConfigureAwait(false);
                    string[] operaciones = paquete.Operaciones?.Select(o => o.Folio.ToString()).ToArray() ?? new string[0];

                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|ProcesarClienteAsync|Se comenzará a procesar las operaciones de tipo {paquete.Tipo} ({operaciones.Length} registros).");

                    RespuestaResumenDto resumen = await ProcesarPaqueteAsync(paquete, operaciones, token).ConfigureAwait(false);
                    DateTime fin = DateTime.UtcNow;

                    RespuestaPaqueteDto respuesta = CrearRespuesta(paquete, resumen, inicio, fin);
                    await EnviarRespuestaAsync(stream, JsonSerializer.Serialize(respuesta), token).ConfigureAwait(false);
                }
                catch (Exception ex) when (!(ex is OperationCanceledException))
                {
                    DateTime fin = DateTime.UtcNow;
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ProcesarClienteAsync|{ex}");

                    RespuestaPaqueteDto respuesta = CrearRespuestaError(inicio, fin);
                    await EnviarRespuestaAsync(stream, JsonSerializer.Serialize(respuesta), token).ConfigureAwait(false);
                }
            }
        }

        protected abstract Task<RespuestaResumenDto> ProcesarPaqueteAsync(SolicitudPaqueteDto paquete, string[] operaciones, CancellationToken token);

        private async Task<SolicitudPaqueteDto> LeerSolicitudAsync(NetworkStream stream, CancellationToken token)
        {
            byte[] lengthBuffer = await ReadExactAsync(stream, 4, token).ConfigureAwait(false);
            int length = BitConverter.ToInt32(lengthBuffer, 0);

            if (length <= 0)
                throw new InvalidDataException("La longitud del paquete recibido debe ser mayor a cero.");

            if (length > MaxFrameSizeBytes)
                throw new InvalidDataException($"La longitud del paquete recibido ({length}) excede el máximo permitido ({MaxFrameSizeBytes}).");

            byte[] payloadBuffer = await ReadExactAsync(stream, length, token).ConfigureAwait(false);
            string json = Encoding.UTF8.GetString(payloadBuffer);

            string llave = Encriptacion.GenerarLlave();
            string cifrado = Encriptacion.Decrypt(json, llave);
            SolicitudPaqueteDto paquete = JsonSerializer.Deserialize<SolicitudPaqueteDto>(cifrado);

            if (paquete == null)
                throw new InvalidDataException("No fue posible deserializar la solicitud TCP.");

            return paquete;
        }

        private RespuestaPaqueteDto CrearRespuesta(SolicitudPaqueteDto paquete, RespuestaResumenDto resumen, DateTime inicio, DateTime fin)
        {
            return new RespuestaPaqueteDto
            {
                Servicio = NombreServicioRespuesta,
                Tipo = paquete.Tipo,
                Resultado = resumen.CalcularResultado(),
                Resumen = resumen,
                Tiempos = new RespuestaTiempoDto
                {
                    Inicio = inicio,
                    Fin = fin
                }
            };
        }

        private RespuestaPaqueteDto CrearRespuestaError(DateTime inicio, DateTime fin)
        {
            return new RespuestaPaqueteDto
            {
                Servicio = NombreServicioRespuesta,
                Resultado = (int)ConstantesRespuestaResultadoDespachador.ERROR,
                Resumen = new RespuestaResumenDto
                {
                    Total = 0,
                    Procesadas = 0,
                    Ok = 0,
                    Error = 0
                },
                Tiempos = new RespuestaTiempoDto
                {
                    Inicio = inicio,
                    Fin = fin
                }
            };
        }
    }
}


using MUFG_T24.Comunes;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.Inicializacion_utilidades
{
    /// <summary>
    /// Implementación de control operativo del servicio en base de datos.
    /// </summary>
    /// <remarks>
    /// Se recomienda registrar <see cref="ControlServicio"/> con ciclo de vida <c>Transient</c> o <c>Scoped</c>. Es <c>thread-safe</c>.
    /// </remarks>
    public partial class ControlServicio : RepositorioMaestro, IControlServicio
    {
        private readonly IAlertas _alertas;

        /// <summary>
        /// Inicializa una nueva instancia de <see cref="ControlServicio"/>.
        /// </summary>
        /// <remarks>
        /// Utiliza SQL Server sobre la base de datos InterfacesT24.
        /// Requiere <see cref="IAlertas"/> por inyección de dependencias.
        /// </remarks>
        public ControlServicio(IAlertas alertas)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _alertas = alertas ?? throw new ArgumentNullException(nameof(alertas));
        }

        /// <summary>
        /// Marca el estado operativo del servicio en base de datos.
        /// </summary>
        /// <remarks>
        /// Con valores en <paramref name="pid"/>, <paramref name="nombreProceso"/> y <paramref name="nombreDisplay"/>
        /// registra servicio en ejecución. Con valores nulos en esos parámetros registra servicio detenido.
        /// ------------------------------------
        /// Esta firma es de compatibilidad y su consumo es opcional; para escenarios especiales utilice el método tipado.
        /// </remarks>
        /// <param name="pid">Identificador del proceso en ejecución; puede ser <c>null</c> durante apagado.</param>
        /// <param name="nombreProceso">Nombre del proceso en ejecución; puede ser <c>null</c> durante apagado.</param>
        /// <param name="nombreDisplay">Nombre de display del servicio; puede ser <c>null</c> durante apagado.</param>
        /// <param name="nombreServicio">Nombre técnico del servicio a actualizar.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns><c>true</c> únicamente cuando el estado resultante es <see cref="EstadoResultadoOperacionServicio.Success"/>; en otro caso <c>false</c>.</returns>
        public async Task<bool> MarcarPuertoEnUsoAsync(int? pid, string nombreProceso, string nombreDisplay, string nombreServicio, CancellationToken ct = default)
        {
            try
            {
                ResultadoOperacionServicio resultado = await MarcarPuertoEnUsoConResultadoAsync(pid, nombreProceso, nombreDisplay, nombreServicio, ct).ConfigureAwait(false);
                return resultado.Estado == EstadoResultadoOperacionServicio.Success;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (OperacionNoExitosaException)
            {
                return false;
            }
        }

        /// <summary>
        /// Marca el estado operativo del servicio en base de datos devolviendo un resultado tipado.
        /// </summary>
        /// <remarks>
        /// El resultado es opcional para consumidores; de forma general basta con la implementación interna
        /// de manejo común (log/alerta/throw). Use el resultado solo en casos excepcionales donde requiera
        /// lógica específica adicional.
        /// ------------------------------------
        /// ** A futuro se podrá cambiar Exito por Afectacion para determinar la efectividad del procedimiento.
        /// Actualmente se evita debido a la imposibilidad de retornar un valor de afectación desde el procedimiento almacenado.
        /// </remarks>
        /// <param name="pid">Identificador del proceso en ejecución; puede ser <c>null</c> durante apagado.</param>
        /// <param name="nombreProceso">Nombre del proceso en ejecución; puede ser <c>null</c> durante apagado.</param>
        /// <param name="nombreDisplay">Nombre de display del servicio; puede ser <c>null</c> durante apagado.</param>
        /// <param name="nombreServicio">Nombre técnico del servicio a actualizar.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Resultado tipado de la operación.</returns>
        public async Task<ResultadoOperacionServicio> MarcarPuertoEnUsoConResultadoAsync(int? pid, string nombreProceso, string nombreDisplay, string nombreServicio, CancellationToken ct = default)
        {
            ResultadoOperacionServicio resultadoOperacion;

            try
            {
                StoredProcedureResult<ResultadoVacioDto> resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                    ConstantesProcedimientosMUFG.UspActualizarEnUso,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@giPID", pid.HasValue ? (object)pid.Value : DBNull.Value));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@gsProcessName", (object)nombreProceso ?? DBNull.Value));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@gsDisplayName", (object)nombreDisplay ?? DBNull.Value));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@gsNombre_Servicio", nombreServicio));
                    }, ct).ConfigureAwait(false);

                //Retorna segun la afectación del procedimiento...
                bool exito = resultado != null && resultado.Exito;
                resultadoOperacion = new ResultadoOperacionServicio
                {
                    Exito = exito,
                    Estado = exito ? EstadoResultadoOperacionServicio.Success : EstadoResultadoOperacionServicio.PermanentError,
                    Mensaje = exito
                        ? "Estado operativo del servicio actualizado correctamente."
                        : "No se pudo actualizar en base de datos la disponibilidad del puerto."
                };
            }
            //Rechaza si el token de cancelación fue solicitado...
            catch (OperationCanceledException ex)
            {
                resultadoOperacion = new ResultadoOperacionServicio
                {
                    Exito = false,
                    Estado = EstadoResultadoOperacionServicio.Canceled,
                    Mensaje = "Operación cancelada al actualizar estado operativo del servicio.",
                    Excepcion = ex
                };
            }
            //Rechaza si ocurrio un error en la operación...
            catch (Exception ex)
            {
                resultadoOperacion = new ResultadoOperacionServicio
                {
                    Exito = false,
                    Estado = EstadoResultadoOperacionServicio.TransientError,
                    Mensaje = "No se pudo actualizar en base de datos la disponibilidad del puerto.",
                    Excepcion = ex
                };

                MUFG_T24.Infraestructura.Logging.Logging.Error($"|OnStop|No se pudo actualizar en base de datos la disponibilidad del puerto: {ex}");
                await _alertas.InsertarAlerta(null, null, nombreServicio, ConstantesClaveAviso.Critico,
                    "No se pudo marcar en base de datos el puerto como disponible",
                    pid.GetValueOrDefault(), nombreProceso, ex).ConfigureAwait(false);

            }

            //Rechaza si la operación no fue exitosa (afectación)...
            if (!resultadoOperacion.Exito)
            {
                if (resultadoOperacion.Excepcion != null)
                {
                    throw resultadoOperacion.Excepcion;
                }

                throw new OperacionNoExitosaException(resultadoOperacion.Mensaje, resultadoOperacion.Estado);
            }

            return resultadoOperacion;
        }

        /// <summary>
        /// Marca una operación con error de fecha pasada.
        /// </summary>
        /// <remarks>
        /// Esta firma es de compatibilidad y su consumo es opcional; para escenarios especiales utilice el método tipado.
        /// </remarks>
        /// <param name="folio">Folio de la operación a marcar.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns><c>true</c> únicamente cuando el estado resultante es <see cref="EstadoResultadoOperacionServicio.Success"/>; en otro caso <c>false</c>.</returns>
        public async Task<bool> MarcarErrorFechaPasada(long folio, CancellationToken ct = default)
        {
            try
            {
                ResultadoOperacionServicio resultado = await MarcarErrorFechaPasadaConResultadoAsync(folio, ct).ConfigureAwait(false);
                return resultado.Estado == EstadoResultadoOperacionServicio.Success;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (OperacionNoExitosaException)
            {
                return false;
            }
        }

        /// <summary>
        /// Marca una operación con error de fecha pasada devolviendo un resultado tipado.
        /// </summary>
        /// <remarks>
        /// El resultado es opcional para consumidores; de forma general basta con la implementación interna
        /// de manejo común (throw en no éxito). Use el resultado solo en casos excepcionales donde requiera
        /// lógica específica adicional.
        /// </remarks>
        /// <param name="folio">Folio de la operación a marcar.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Resultado tipado de la operación.</returns>
        public async Task<ResultadoOperacionServicio> MarcarErrorFechaPasadaConResultadoAsync(long folio, CancellationToken ct = default)
        {
            ResultadoOperacionServicio resultadoOperacion;

            try
            {
                StoredProcedureResult<ResultadoVacioDto> resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                    ConstantesProcedimientosMUFG.UspFechaPasada,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    }, ct).ConfigureAwait(false);

                //Retorna segun la afectación del procedimiento...
                bool exito = resultado != null && resultado.Exito;
                resultadoOperacion = new ResultadoOperacionServicio
                {
                    Exito = exito,
                    Estado = exito ? EstadoResultadoOperacionServicio.Success : EstadoResultadoOperacionServicio.PermanentError,
                    Mensaje = exito
                        ? "Operación marcada correctamente con error de fecha pasada."
                        : "No se pudo marcar la operación con error de fecha pasada."
                };
            }
            //Rechaza si el token de cancelación fue solicitado...
            catch (OperationCanceledException ex)
            {
                resultadoOperacion = new ResultadoOperacionServicio
                {
                    Exito = false,
                    Estado = EstadoResultadoOperacionServicio.Canceled,
                    Mensaje = "Operación cancelada al marcar error de fecha pasada.",
                    Excepcion = ex
                };
            }
            //Rechaza si ocurrio un error en la operación...
            catch (Exception ex)
            {
                resultadoOperacion = new ResultadoOperacionServicio
                {
                    Exito = false,
                    Estado = EstadoResultadoOperacionServicio.TransientError,
                    Mensaje = "No se pudo marcar la operación con error de fecha pasada.",
                    Excepcion = ex
                };
            }

            if (!resultadoOperacion.Exito)
            {
                if (resultadoOperacion.Excepcion != null)
                {
                    throw resultadoOperacion.Excepcion;
                }

                throw new OperacionNoExitosaException(resultadoOperacion.Mensaje, resultadoOperacion.Estado);
            }

            return resultadoOperacion;
        }

    }
}


using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.Inicializacion_utilidades
{
    /// <summary>
    /// Implementación de control de parametrización de servicio (fecha/hora/puerto/parámetros en BD).
    /// </summary>
    /// <remarks>
    /// Se recomienda registrar <see cref="Parametrizacion"/> con ciclo de vida <c>Transient</c> o <c>Scoped</c>. Es <c>thread-safe</c>.
    /// </remarks>
    public class Parametrizacion : RepositorioMaestro, IParametrizacion
    {
        /// <summary>
        /// Inicializa una nueva instancia de Parametrizacion.
        /// </summary>
        /// <remarks>
        /// Siempre hace uso de SQL Server para la base de datos InterfacesT24.
        /// </remarks>
        public Parametrizacion()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene la hora local del sistema en formato <see cref="TimeSpan"/>.
        /// </summary>
        /// <returns>Hora actual del día.</returns>
        public TimeSpan ObtenerHoraSistema()
        {
            return DateTime.Now.TimeOfDay;
        }

        /// <summary>
        /// Obtiene la identidad del proceso actual (PID, nombre de proceso y nombre de display).
        /// </summary>
        /// <param name="configuracion">Instancia de configuración a completar con los datos del proceso actual.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>La misma instancia de <paramref name="configuracion"/> con los datos del proceso actual.</returns>
        public async Task<Configuracion> ObtenerProcesoActualAsync(Configuracion configuracion, CancellationToken ct = default)
        {
            //Rechaza si la configuración base es nula...
            if (configuracion == null)
                throw new ArgumentNullException(nameof(configuracion));

            //Obtiene el PID y nombre del proceso actual...
            using (Process currentProcess = Process.GetCurrentProcess())
            {
                configuracion.Pid = currentProcess.Id;
                configuracion.NombreProceso = currentProcess.ProcessName;
            }

            //Rechaza si no se pudo obtener el PID del proceso actual...
            if (!configuracion.Pid.HasValue)
                throw new InvalidOperationException("No fue posible obtener el identificador del proceso actual.");

            int pid = configuracion.Pid.Value;

            //Obtiene el nombre de display del proceso actual (para Windows Service) o un valor de prueba (para Debug)...
#if DEBUG
            configuracion.NombreDisplay = "PRUEBAS";
#else
            configuracion.NombreDisplay = await ObtieneDisplayName(pid);
#endif

            //Inicializa el archivo log...
            Logging.Info("|OnStart|Inicializando Servicio.");
            Logging.Info($"|OnStart|Proceso iniciado con las caracteristicas: PID={configuracion.Pid}, ProcessName='{configuracion.NombreProceso}', servicio='{configuracion.NombreServicio}', displayName='{configuracion.NombreDisplay}' buscando puerto donde escuchar.");
            Logging.Info($"|OnStart|Versiones: Servicio={System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString()}, MUFG.dll={Comunes.Comunes.ObtenerVersion()}, Framework={Environment.Version.ToString()}.");

            return configuracion;
        }

        /// <summary>
        /// Valida los datos críticos del proceso actual en la configuración del servicio.
        /// </summary>
        /// <param name="configuracion">Instancia de configuración a validar.</param>
        public void ValidarProcesoActual(Configuracion configuracion)
        {
            //Se rechaza si la configuración base es nula...
            if (configuracion == null)
                throw new InvalidOperationException("No se obtuvo la configuración base del proceso actual.");

            //Se rechaza si no se pudo obtener el PID del proceso actual...
            if (!configuracion.Pid.HasValue)
                throw new InvalidOperationException("No se obtuvo el PID del proceso actual.");

            //Se rechaza si no se pudo obtener el nombre del proceso actual...
            if (string.IsNullOrWhiteSpace(configuracion.NombreProceso))
                throw new InvalidOperationException("No se obtuvo el NombreProceso del proceso actual.");

            //Se rechaza si no se pudo obtener el nombre de display del proceso actual...
            if (string.IsNullOrWhiteSpace(configuracion.NombreDisplay))
                throw new InvalidOperationException("No se obtuvo el NombreDisplay del proceso actual.");

            //Se rechaza si no se pudo obtener el nombre del servicio actual...
            if (string.IsNullOrWhiteSpace(configuracion.NombreServicio))
                throw new InvalidOperationException("No se obtuvo el NombreServicio del proceso actual.");
        }

        /// <summary>
        /// Obtiene la identidad del proceso actual y valida la configuración crítica resultante.
        /// </summary>
        /// <param name="configuracion">Instancia de configuración base del servicio.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Instancia de configuración validada con los datos del proceso actual.</returns>
        public async Task<Configuracion> ObtenerProcesoActualValidadoAsync(Configuracion configuracion, CancellationToken ct)
        {
            var configuracionActual = await ObtenerProcesoActualAsync(configuracion, ct);
            ValidarProcesoActual(configuracionActual);
            return configuracionActual;
        }

        /// <summary>
        /// Obtiene la fecha del sistema en formato <c>yyyyMMdd</c> desde base de datos.
        /// </summary>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Fecha del sistema en formato <c>yyyyMMdd</c>.</returns>
        public async Task<string> ObtenerFechaSistemaAsync(CancellationToken ct = default)
        {
            var resultado = await EjecutarStoredProcedureQuery<FechaSistemaDto>(
                ConstantesProcedimientosMUFG.UspObtenerFechaSistema,
                null, ct);
            FechaSistemaDto fechaSistema = resultado.Data.Count > 0 ? resultado.Data[0] : null;

            //Se rechaza si no se pudo obtener la fecha del sistema desde la base de datos...
            if (fechaSistema == null)
            {
                Logging.Error($"|ObtenerFechaSistemaAsync|No se obtuvo la fecha del sistema desde '{ConstantesProcedimientosMUFG.UspObtenerFechaSistema}'.");
                throw new InvalidOperationException("No se obtuvo la fecha del sistema requerida para inicializar el servicio.");
            }

            Logging.Info($"|ObtenerFechaSistemaAsync|Fecha del sistema '{fechaSistema.Fecha.ToString("yyyyMMdd")}'.");
            return fechaSistema.Fecha.ToString("yyyyMMdd");
        }

        /// <summary>
        /// Valida la fecha del sistema obtenida para el flujo del servicio.
        /// </summary>
        /// <param name="fechaSistema">Fecha del sistema a validar.</param>
        public void ValidarFechaSistema(string fechaSistema)
        {
            //Se rechaza si no se pudo obtener la fecha del sistema...
            if (fechaSistema == null)
                throw new InvalidOperationException("No se obtuvo la FechaSistema.");
        }

        /// <summary>
        /// Obtiene la fecha del sistema y valida el valor resultante.
        /// </summary>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Fecha del sistema validada en formato <c>yyyyMMdd</c>.</returns>
        public async Task<string> ObtenerFechaSistemaValidadaAsync(CancellationToken ct = default)
        {
            var fechaSistema = await ObtenerFechaSistemaAsync(ct).ConfigureAwait(false);
            ValidarFechaSistema(fechaSistema);
            return fechaSistema;
        }

        /// <summary>
        /// Obtiene la fecha juliana del sistema en formato <c>yyyyMMdd</c> desde base de datos.
        /// </summary>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Fecha juliana del sistema en formato <c>yyyyMMdd</c>.</returns>
        public async Task<string> ObtenerFechaSistemaJulianaAsync(CancellationToken ct = default)
        {
            var resultado = await EjecutarStoredProcedureQuery<FechaSistemaDto>(
                ConstantesProcedimientosMUFG.UspObtenerFechaSistemaJuliana,
                null,
                ct);
            FechaSistemaDto fechaSistema = resultado.Data.Count > 0 ? resultado.Data[0] : null;

            //Se rechaza si no se pudo obtener la fecha juliana del sistema..
            if (fechaSistema == null)
            {
                Logging.Error($"|FechaSistemaJuliana|No se obtuvo la fecha juliana del sistema desde '{ConstantesProcedimientosMUFG.UspObtenerFechaSistemaJuliana}'.");
                throw new InvalidOperationException("No se obtuvo la fecha juliana del sistema requerida para inicializar el servicio.");
            }

            Logging.Info($"|FechaSistemaJuliana='{fechaSistema.Fecha.ToString("yyyyMMdd")}'.");
            return fechaSistema.Fecha.ToString("yyyyMMdd");
        }

        /// <summary>
        /// Obtiene parámetros del sistema a partir de un arreglo de definiciones.
        /// </summary>
        /// <param name="parametros">Definiciones de parámetros a consultar.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Diccionario con clave de parámetro y su valor.</returns>
        public async Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(ParametroSistemaDefinicion[] parametros, CancellationToken ct = default)
        {
            //Normaliza las definiciones de parámetros y crea la cadena de parámetros para el procedimiento almacenado...
            ParametroSistemaDefinicion[] definiciones = NormalizarDefinicionesParametros(parametros);
            string strParametros = CrearCadenaParametrosSistema(definiciones);

            var resultado = await EjecutarStoredProcedureQuery<ParametroDTO>(
                ConstantesProcedimientosMUFG.UspObtenerParametrosSistema, 
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@strParametros", strParametros));
                }, ct);
            Dictionary<string, string> parametrosSistema = new Dictionary<string, string>();

            //Llena el diccionario validando duplicados...
            foreach (var parametro in resultado.Data)
            {
                ct.ThrowIfCancellationRequested();

                if (parametrosSistema.ContainsKey(parametro.Id_parametro))
                {
                    Logging.Error($"|ObtenerParametrosSistema|El parámetro '{parametro.Id_parametro}' está duplicado en la base de datos.");
                    continue;
                }
                parametrosSistema.Add(parametro.Id_parametro, parametro.Valor);
            }

            //Determina si los parámetros requeridos están presentes y no vacíos...
            foreach (ParametroSistemaDefinicion definicion in definiciones)
            {
                ct.ThrowIfCancellationRequested();

                //Si el parámetro no está presente o está vacío...
                if (!parametrosSistema.TryGetValue(definicion.Clave, out var valor) ||
                    string.IsNullOrWhiteSpace(valor))
                {
                    string obligatoriedad = definicion.Requerido ? "requerido" : "opcional";
                    string mensaje = $"|ObtenerParametrosSistema|El parámetro {obligatoriedad} '{definicion.Clave}' no está definido o está vacío.";

                    if (definicion.Requerido)
                        Logging.Error(mensaje);
                    else
                        Logging.Warn(mensaje);

                    //Se rechaza si no lo está y es requerido...
                    if (definicion.Requerido)
                        throw new InvalidOperationException($"El parámetro requerido '{definicion.Clave}' no está definido o está vacío.");
                }
                //Si el parámetro está presente...
                else
                {
                    //Se registra su valor (enmascarando si es sensible)...
                    string mensajeParametro = $"|ObtenerParametrosSistema|Parámetro '{definicion.Clave}' = '{valor}'.";
                    if (definicion.Sensible)
                        Logging.Info(mensajeParametro, valor);
                    else
                        Logging.Info(mensajeParametro);
                }
            }

            return parametrosSistema;
        }

        /// <summary>
        /// SOBRECARGA: Obtiene parámetros del sistema a partir de una cadena simple.
        /// </summary>
        /// <remarks>
        /// Utiliza esta sobrecarga cuando solo se cuenta con una cadena de parámetros obligatorios y no sensibles.
        /// </remarks>
        /// <param name="strParametros">Cadena con parámetros obligatorios y no sensibles, separados por coma.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Diccionario con clave de parámetro y su valor.</returns>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametros, CancellationToken ct = default)
            => ObtenerParametrosSistemaAsync(ConvertirCadenaADefinicionesParametros(strParametros, true, false), ct);

        /// <summary>
        /// SOBRECARGA: Obtiene parámetros del sistema combinando parámetros por defecto y definiciones específicas.
        /// </summary>
        /// <remarks>
        /// Utiliza esta sobrecarga cuando existe una cadena base de parámetros obligatorios y no sensibles,
        /// y además se requiere complementar o sobrescribir reglas mediante un arreglo de definiciones específicas.
        /// </remarks>
        /// <param name="strParametrosDefault">Cadena base de parámetros obligatorios y no sensibles.</param>
        /// <param name="parametrosEspecificos">Definiciones específicas para complementar o sobrescribir la base.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Diccionario con clave de parámetro y su valor.</returns>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, ParametroSistemaDefinicion[] parametrosEspecificos, CancellationToken ct = default)
        {
            var porDefecto = ConvertirCadenaADefinicionesParametros(strParametrosDefault, requerido: true, sensible: false);
            var especificos = NormalizarDefinicionesParametros(parametrosEspecificos);
            var definiciones = CombinarDefinicionesParametros(porDefecto, especificos);
            return ObtenerParametrosSistemaAsync(definiciones, ct);
        }

        /// <summary>
        /// SOBRECARGA LEGACY: Compatibilidad con consumidores que no envían CancellationToken.
        /// </summary>
        /// <param name="strParametrosDefault">Cadena de parámetros obligatorios y no sensibles.</param>
        /// <param name="strParametrosObligatoriosSensibles">Cadena de parámetros obligatorios y sensibles.</param>
        /// <returns>Diccionario con clave de parámetro y su valor.</returns>
        [Obsolete("Sobrecarga legacy para compatibilidad. Use ObtenerParametrosSistemaAsync(string, string, CancellationToken).", false)]
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, string strParametrosObligatoriosSensibles)
            => ObtenerParametrosSistemaAsync(strParametrosDefault, strParametrosObligatoriosSensibles, CancellationToken.None);

        /// <summary>
        /// SOBRECARGA: Obtiene parámetros del sistema combinando listas de no sensibles y sensibles.
        /// </summary>
        /// <remarks>
        /// Utiliza esta sobrecarga cuando se tienen dos cadenas: una de parámetros obligatorios y no sensibles,
        /// y otra de parámetros obligatorios y sensibles.
        /// </remarks>
        /// <param name="strParametrosDefault">Cadena de parámetros obligatorios y no sensibles.</param>
        /// <param name="strParametrosObligatoriosSensibles">Cadena de parámetros obligatorios y sensibles.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Diccionario con clave de parámetro y su valor.</returns>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, string strParametrosObligatoriosSensibles, CancellationToken ct = default)
        {
            var porDefecto = ConvertirCadenaADefinicionesParametros(strParametrosDefault, requerido: true, sensible: false);
            var obligatoriosSensibles = ConvertirCadenaADefinicionesParametros(strParametrosObligatoriosSensibles, requerido: true, sensible: true);
            var definiciones = CombinarDefinicionesParametros(porDefecto, obligatoriosSensibles);
            return ObtenerParametrosSistemaAsync(definiciones, ct);
        }

        /// <summary>
        /// HELPER: Convierte una cadena de parámetros a definiciones de parámetros.
        /// </summary>
        /// <param name="strParametros">Cadena de parámetros separada por coma.</param>
        /// <param name="requerido">Indica si cada parámetro se marca como requerido.</param>
        /// <param name="sensible">Indica si cada parámetro se marca como sensible.</param>
        /// <returns>Arreglo de definiciones de parámetros normalizadas.</returns>
        private static ParametroSistemaDefinicion[] ConvertirCadenaADefinicionesParametros(string strParametros, bool requerido, bool sensible)
        {
            //Se rechaza si la cadena de parámetros es nula o vacía, retornando un arreglo vacío...
            if (string.IsNullOrWhiteSpace(strParametros))
                return Array.Empty<ParametroSistemaDefinicion>();

            //Retorna un arreglo de definiciones de parámetros, normalizando y filtrando los valores vacíos...
            return strParametros
                .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(clave => clave.Trim().Trim('\''))
                .Where(clave => !string.IsNullOrWhiteSpace(clave))
                .Select(clave => new ParametroSistemaDefinicion
                {
                    Clave = clave,
                    Requerido = requerido,
                    Sensible = sensible
                })
                .ToArray();
        }

        /// <summary>
        /// HELPER: Combina dos arreglos de definiciones de parámetros.
        /// </summary>
        /// <param name="parametrosDefault">Definiciones base de parámetros.</param>
        /// <param name="parametrosEspecificos">Definiciones específicas con prioridad sobre la base.</param>
        /// <returns>Arreglo combinado de definiciones de parámetros.</returns>
        private static ParametroSistemaDefinicion[] CombinarDefinicionesParametros(ParametroSistemaDefinicion[] parametrosDefault, ParametroSistemaDefinicion[] parametrosEspecificos)
        {
            var combinados = new Dictionary<string, ParametroSistemaDefinicion>(StringComparer.OrdinalIgnoreCase);

            foreach (var parametro in NormalizarDefinicionesParametros(parametrosDefault))
                combinados[parametro.Clave] = parametro;

            foreach (var parametro in NormalizarDefinicionesParametros(parametrosEspecificos))
                combinados[parametro.Clave] = parametro;

            return combinados.Values.ToArray();
        }

        /// <summary>
        /// HELPER: Normaliza un arreglo de definiciones de parámetros.
        /// </summary>
        /// <param name="parametros">Definiciones de parámetros a normalizar.</param>
        /// <returns>Arreglo normalizado de definiciones de parámetros.</returns>
        private static ParametroSistemaDefinicion[] NormalizarDefinicionesParametros(ParametroSistemaDefinicion[] parametros)
        {
            //Se rechaza si la cadena de parámetros es nula o vacía, retornando un arreglo vacío..
            if (parametros == null)
                return Array.Empty<ParametroSistemaDefinicion>();

            //Retorna un arreglo de definiciones de parámetros, normalizando y filtrando los valores vacíos...
            return parametros
                .Where(parametro => parametro != null && !string.IsNullOrWhiteSpace(parametro.Clave))
                .Select(parametro => new ParametroSistemaDefinicion
                {
                    Clave = parametro.Clave.Trim().Trim('\''),
                    Requerido = parametro.Requerido,
                    Sensible = parametro.Sensible
                })
                .Where(parametro => !string.IsNullOrWhiteSpace(parametro.Clave))
                .ToArray();
        }

        /// <summary>
        /// HELPER: Crea la cadena de parámetros para consulta a partir de definiciones.
        /// </summary>
        /// <param name="parametros">Definiciones de parámetros.</param>
        /// <returns>Cadena de parámetros delimitada por comas y entrecomillada.</returns>
        private static string CrearCadenaParametrosSistema(ParametroSistemaDefinicion[] parametros)
        {
            return string.Join(",", parametros.Select(parametro => $"'{parametro.Clave.Replace("'", "''")}'"));
        }

        /// <summary>
        /// Obtiene parámetros adicionales del sistema fuera del flujo de inicialización.
        /// </summary>
        /// <remarks>
        /// Utiliza esta sobrecarga cuando se requiere consultar parámetros en tiempo de ejecución,
        /// separando la lista de parámetros obligatorios y no sensibles de la lista obligatoria y sensible.
        /// </remarks>
        /// <param name="strParametros">Cadena de parámetros obligatorios y no sensibles.</param>
        /// <param name="strParametrosObligatoriosSensibles">Cadena de parámetros obligatorios y sensibles.</param>
        /// <returns>Diccionario con clave de parámetro y su valor.</returns>
        public async Task<Dictionary<string, string>> ObtenerParametrosSistemaAdicionalesAsync(string strParametros, string strParametrosObligatoriosSensibles = null, CancellationToken ct = default)
        {
            var parametrosObligatoriosNoSensibles = ConvertirCadenaADefinicionesParametros(strParametros, requerido: true, sensible: false);
            var parametrosObligatoriosSensibles = ConvertirCadenaADefinicionesParametros(strParametrosObligatoriosSensibles, requerido: true, sensible: true);
            var definiciones = CombinarDefinicionesParametros(parametrosObligatoriosNoSensibles, parametrosObligatoriosSensibles);

            return await ObtenerParametrosSistemaAsync(definiciones, ct);
        }

        /// <summary>
        /// Obtiene el puerto configurado para un servicio y valida su disponibilidad.
        /// </summary>
        /// <param name="nombreServicio">Nombre del servicio.</param>
        /// <param name="pid">Identificador del proceso.</param>
        /// <param name="ct">Token de cancelación opcional.</param>
        /// <returns>Puerto configurado para el servicio en formato texto.</returns>
        public async Task<string> ObtenerPuertos(string nombreServicio, int pid, CancellationToken ct = default)
        {
            var resultado = await EjecutarStoredProcedureQuery<ObtenerPuertosDto>
                (ConstantesProcedimientosMUFG.UspObtenerPuerto, 
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreServicio", nombreServicio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@pid", pid));
                }, ct);
            ObtenerPuertosDto puertoServicio = resultado.Data.Count > 0 ? resultado.Data[0] : null;

            //Se rechaza si no se pudo obtener el puerto configurado para el servicio...
            if (puertoServicio == null)
            {
                Logging.Error($"|ObtenerPuertos|No se obtuvo puerto para el servicio '{nombreServicio}' y PID '{pid}'.");
                throw new InvalidOperationException($"No se obtuvo el puerto requerido para inicializar el servicio '{nombreServicio}'.");
            }

            //Se valida la disponibilidad del puerto configurado para el servicio...
            await ValidarPuertoDisponibleAsync(puertoServicio.Puerto, nombreServicio, pid, ct);
            return puertoServicio.Puerto.ToString();
        }

        /// <summary>
        /// Valida la disponibilidad de un puerto para un servicio y proceso específicos.
        /// </summary>
        /// <param name="puerto">Puerto a validar.</param>
        /// <param name="nombreServicio">Nombre del servicio.</param>
        /// <param name="pid">Identificador del proceso.</param>
        /// <param name="ct">Token de cancelación para la espera entre reintentos.</param>
        /// <returns>Tarea de validación.</returns>
        /// <exception cref="InvalidOperationException">Se lanza cuando el puerto es inválido o no está disponible.</exception>
        private async Task ValidarPuertoDisponibleAsync(int puerto, string nombreServicio, int pid, CancellationToken ct)
        {
            if (puerto < IPEndPoint.MinPort || puerto > IPEndPoint.MaxPort)
            {
                MUFG_T24.Infraestructura.Logging.Logging.Error($"|ObtenerPuertos|El puerto '{puerto}' configurado para el servicio '{nombreServicio}' y PID '{pid}' no es válido.");
                throw new InvalidOperationException($"El puerto configurado para el servicio '{nombreServicio}' no es válido.");
            }

            Logging.Info($"|ObtenerPuertos|Validando disponibilidad del puerto {puerto} para el servicio '{nombreServicio}'.");

            bool puertoLibre = false;
            for (int intento = 0; intento <= 10; intento++)
            {
                TcpListener listener = null;
                try
                {
                    listener = new TcpListener(IPAddress.Parse("127.0.0.1"), puerto)
                    {
                        ExclusiveAddressUse = true
                    };
                    listener.Start();
                    puertoLibre = true;
                    break;
                }
                catch (SocketException ex)
                {
                    Logging.Info($"|ObtenerPuertos|Validando puerto intento {intento}, excepción: {ex}");
                }
                finally
                {
                    if (listener != null)
                    {
                        listener.Stop();
                    }
                }

                if (intento < 10)
                {
                    await Task.Delay(500, ct);
                }
            }

            if (!puertoLibre)
            {
                Logging.Error($"|ObtenerPuertos|El puerto {puerto} del servicio '{nombreServicio}' está en uso por otro proceso.");
                throw new InvalidOperationException($"El puerto {puerto} configurado para el servicio '{nombreServicio}' está en uso por otro proceso.");
            }

            Logging.Info($"|ObtenerPuertos|Puerto disponible {puerto} para el servicio '{nombreServicio}'.");
        }

        /// <summary>
        /// Detiene el servicio de Windows y establece el código de salida en <c>1</c>.
        /// </summary>
        /// <param name="serviceHost">Instancia del servicio de Windows a detener.</param>
        public void DetenerServicio(ServiceBase serviceHost)
        {
            if (serviceHost == null)
                throw new ArgumentNullException(nameof(serviceHost));

            serviceHost.ExitCode = 1;
            serviceHost.Stop();
        }

    }
}


using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Conexiones;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Management;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace MUFG_T24.Repositorios
{
    /// <summary>
    /// Clase Maestro encargada de ejecutar las transacciones que se usaran en todos los repositorios
    /// </summary>
    public abstract class RepositorioMaestro
    {
        private sealed class ResultadoStoredProcedureException : Exception
        {
            public ResultadoStoredProcedureException(string message)
                : base(message) { }
        }

        protected readonly IBasedatosConexion basedatosConexion;
        protected readonly IBasedatosConexion basedatosConexionVistasT24;

        /// <summary>
        /// Inicializa una nueva instancia de RepositorioMaestro.
        /// </summary>
        protected RepositorioMaestro(
            BasedatosProveedor proveedor,
            NombreBasedatos nombreBasedatos,
            BasedatosProveedor? proveedorVistasT24,
            NombreBasedatos? nombreBasedatosVistasT24)
        {
            ConfiguracionEncriptadaHelper.Inicializar();
            //EncriptarArchivoConfiguracion();
            //ConexionBasedatosFactory.ObtenerDatosGenerales("Bandera");
            this.basedatosConexion = ConexionBasedatosFactory.CrearConexion(proveedor, nombreBasedatos);
            if (proveedorVistasT24 != null || nombreBasedatosVistasT24 != null)
                this.basedatosConexionVistasT24 = ConexionBasedatosFactory.CrearConexion(proveedorVistasT24, nombreBasedatosVistasT24);
        }

        /// <summary>
        /// Agrega un parámetro a un comando de base de datos.
        /// </summary>
        protected void AgregarParametro(IDbCommand cmd, string nombre, object valor, DbType tipo, int? tamaño = null)
        {
            IDbDataParameter parametro = basedatosConexion.CrearParametro(nombre, valor);
            parametro.DbType = tipo;
            if (tamaño.HasValue)
                parametro.Size = tamaño.Value;
            cmd.Parameters.Add(parametro);
        }

        /// <summary>
        /// Ejecuta comandos que devuelven datos
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parametros"></param>
        /// <param name="tipoComando"></param>
        /// <returns>DataTable</returns>
        protected async Task<StoredProcedureResult<T>> EjecutarReader<T>(string query, Action<IDbCommand> parametros = null,
            CommandType tipoComando = CommandType.Text, bool esVistasT24 = false, int? commandTimeout = null, CancellationToken ct = default) where T : new()
        {
            ct.ThrowIfCancellationRequested();
            StoredProcedureResult<T> spResult = new StoredProcedureResult<T>();
            Type tipoDTO = typeof(T);
            DbConnection _bdConexion;
            DbCommand _comando;
            if (esVistasT24)
            {
                _bdConexion = basedatosConexionVistasT24.CrearConexion();
                _bdConexion.ConnectionString = ConexionBasedatosFactory.ObtenerCadenaConexion(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW);
                _comando = basedatosConexionVistasT24.CrearComando(query, _bdConexion);
            }
            else
            {
                _bdConexion = basedatosConexion.CrearConexion();
                _comando = basedatosConexion.CrearComando(query, _bdConexion);
            }
            using (DbConnection conexion = _bdConexion)
            {
                using (DbCommand cmd = _comando)
                {
                    try
                    {
                        cmd.CommandType = tipoComando;
                        if (commandTimeout.HasValue)
                        {
                            cmd.CommandTimeout = commandTimeout.Value;
                        }

                        parametros?.Invoke(cmd);
                        ConvertirParametrosNullADbNull(cmd);

                        await conexion.OpenAsync(ct).ConfigureAwait(false);
                        using (DbDataReader lector = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
                        {
                            Dictionary<string, int> columnas = ObtenerColumnasReader(lector);
                            PropertyInfo[] propiedades = tipoDTO.GetProperties();
                            MethodInfo cloneMetodo = tipoDTO.GetMethod("Clone", Type.EmptyTypes);

                            while (await lector.ReadAsync(ct).ConfigureAwait(false))
                            {
                                EvaluarResultadoPorColumnas(spResult, lector, columnas);

                                T instanciaDTO = new T();

                                foreach (PropertyInfo prop in propiedades)
                                {
                                    try
                                    {
                                        if (!prop.CanWrite)
                                            continue;

                                        int ordinal;
                                        if (!columnas.TryGetValue(prop.Name, out ordinal))
                                            continue;

                                        object dbValue = lector.GetValue(ordinal);

                                        object valorConvertido;
                                        bool conversionExitosa = IntentarConvertirValor(
                                            dbValue,
                                            prop.PropertyType,
                                            out valorConvertido);

                                        if (conversionExitosa)
                                        {
                                            prop.SetValue(instanciaDTO, valorConvertido, null);
                                        }
                                        else
                                        {
                                            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                                                string.Format(
                                                    "WARN|EjecutarReader|No se pudo convertir valor|Query:{0}|DTO:{1}|Propiedad:{2}|TipoDestino:{3}|Valor:{4}|ValorMeta:{5}|TipoOrigen:{6}",
                                                    query,
                                                    tipoDTO.FullName,
                                                    prop.Name,
                                                    prop.PropertyType.FullName,
                                                    "***Enmascarado***",
                                                    dbValue == null || dbValue == DBNull.Value
                                                        ? "EsNull=true;Longitud=0"
                                                        : "EsNull=false;Longitud=" + (Convert.ToString(dbValue, CultureInfo.InvariantCulture) ?? string.Empty).Length.ToString(CultureInfo.InvariantCulture),
                                                    dbValue == null || dbValue == DBNull.Value ? "NULL" : dbValue.GetType().FullName));
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                                            string.Format(
                                                ConstantesClaveAviso.Error + "|EjecutarReader|Query:{0}|DTO:{1}|Propiedad:{2}|Error:{3}",
                                                query,
                                                tipoDTO.FullName,
                                                prop.Name,
                                                ex));
                                    }
                                }

                                if (cloneMetodo != null)
                                {
                                    T cloneInstancia = (T)cloneMetodo.Invoke(instanciaDTO, null);

                                    ResetearObjeto(instanciaDTO);
                                    spResult.Data.Add(cloneInstancia);
                                }
                                else
                                {
                                    spResult.Data.Add(instanciaDTO);
                                }
                            }
                        }

                        spResult.Exito = string.IsNullOrEmpty(spResult.MensajeError);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (ResultadoStoredProcedureException)
                    {
                        throw;
                    }
                    catch (SqlException sqlEx)
                    {
                        spResult.Exito = false;
                        spResult.Exception = sqlEx;
                        spResult.MensajeError = string.Format("Error SQL:{0} {1}", sqlEx.Number, sqlEx.Message);
                    }
                    catch (Exception ex)
                    {
                        spResult.Exito = false;
                        spResult.Exception = ex;
                        spResult.MensajeError = string.Format("Error general ejecutando query: {0}", ex.Message);

                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                            string.Format(
                                ConstantesClaveAviso.Error + "|EjecutarReader|Query:{0}|DTO:{1}|Error:{2}",
                                query,
                                tipoDTO.FullName,
                                ex));
                    }
                }
            }
            return spResult;
        }

        /// <summary>
        /// Ejecuta SP para consultas que devuelvan una lista de objectos
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="nombreSP"></param>
        /// <param name="parametros"></param>
        /// <returns>StoredProcedureResult<typeparamref name="T"/></returns>
        protected Task<StoredProcedureResult<T>> EjecutarStoredProcedureQuery<T>(string nombreSP, Action<IDbCommand> parametros = null,
            bool esVistasT24 = false, int? commandTimeout = null, bool validarAfectacion = false) where T : new()
        {
            return EjecutarStoredProcedureQuery<T>(nombreSP, parametros, CancellationToken.None, esVistasT24, commandTimeout, validarAfectacion);
        }

        protected Task<StoredProcedureResult<T>> EjecutarStoredProcedureQuery<T>(string nombreSP, Action<IDbCommand> parametros,
            bool esVistasT24) where T : new()
        {
            return EjecutarStoredProcedureQuery<T>(nombreSP, parametros, CancellationToken.None, esVistasT24, null, false);
        }

        /// <summary>
        /// Ejecuta EjecutarStoredProcedureQuery.
        /// </summary>
        protected async Task<StoredProcedureResult<T>> EjecutarStoredProcedureQuery<T>(string nombreSP, Action<IDbCommand> parametros,
            CancellationToken ct, bool esVistasT24 = false, int? commandTimeout = null, bool validarAfectacion = false) where T : new()
        {
            ct.ThrowIfCancellationRequested();
            StoredProcedureResult<T> spResult = new StoredProcedureResult<T>();
            Type tipoDTO = typeof(T);
            DbConnection _bdConexion;
            DbCommand _comando;

            // Valida y crea la conexion a la base de datos
            if (esVistasT24)
            {
                _bdConexion = basedatosConexionVistasT24.CrearConexion();
                _bdConexion.ConnectionString = ConexionBasedatosFactory.ObtenerCadenaConexion(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW);
                _comando = basedatosConexionVistasT24.CrearComando(nombreSP, _bdConexion);
            }
            else
            {
                _bdConexion = basedatosConexion.CrearConexion();
                _comando = basedatosConexion.CrearComando(nombreSP, _bdConexion);
            }

            using (DbConnection conexion = _bdConexion)
            {
                using (DbCommand comando = _comando)
                {
                    try
                    {
                        comando.CommandType = CommandType.StoredProcedure;
                        if (commandTimeout.HasValue)
                        {
                            comando.CommandTimeout = commandTimeout.Value;
                        }

                        // Configuración de parámetros
                        parametros?.Invoke(comando);

                        ConvertirParametrosNullADbNull(comando);
                      
                        if (comando.Parameters.Contains("@exito"))
                        {
                            comando.Parameters["@exito"].DbType = DbType.Boolean;
                        }

                        await conexion.OpenAsync(ct).ConfigureAwait(false);

                        using (DbDataReader lector = await comando.ExecuteReaderAsync(ct).ConfigureAwait(false))
                        {
                            Dictionary<string, int> columnas = ObtenerColumnasReader(lector);
                            PropertyInfo[] propiedades = tipoDTO.GetProperties();
                            MethodInfo cloneMetodo = tipoDTO.GetMethod("Clone", Type.EmptyTypes);

                            while (await lector.ReadAsync(ct).ConfigureAwait(false))
                            {
                                EvaluarAfectoPorFilasAfectadas(spResult, lector, columnas);
                                EvaluarResultadoPorColumnas(spResult, lector, columnas);

                                T instanciaDTO = new T();

                                foreach (PropertyInfo prop in propiedades)
                                {
                                    try
                                    {
                                        if (!prop.CanWrite)
                                            continue;

                                        int ordinal;
                                        if (!columnas.TryGetValue(prop.Name, out ordinal))
                                            continue;

                                        object dbValue = lector.GetValue(ordinal);

                                        object valorConvertido;
                                        bool conversionExitosa = IntentarConvertirValor(
                                            dbValue,
                                            prop.PropertyType,
                                            out valorConvertido);

                                        if (conversionExitosa)
                                        {
                                            prop.SetValue(instanciaDTO, valorConvertido, null);
                                        }
                                        else
                                        {
                                            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                                                string.Format(
                                                    "WARN|EjecutarStoredProcedureQuery|No se pudo convertir valor|SP:{0}|DTO:{1}|Propiedad:{2}|TipoDestino:{3}|Valor:{4}|ValorMeta:{5}|TipoOrigen:{6}",
                                                    nombreSP,
                                                    tipoDTO.FullName,
                                                    prop.Name,
                                                    prop.PropertyType.FullName,
                                                    "***Enmascarado***",
                                                    dbValue == null || dbValue == DBNull.Value
                                                        ? "EsNull=true;Longitud=0"
                                                        : "EsNull=false;Longitud=" + (Convert.ToString(dbValue, CultureInfo.InvariantCulture) ?? string.Empty).Length.ToString(CultureInfo.InvariantCulture),
                                                    dbValue == null || dbValue == DBNull.Value ? "NULL" : dbValue.GetType().FullName));
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                                            string.Format(
                                                ConstantesClaveAviso.Error + "|EjecutarStoredProcedureQuery|SP:{0}|DTO:{1}|Propiedad:{2}|Error:{3}",
                                                nombreSP,
                                                tipoDTO.FullName,
                                                prop.Name,
                                                ex));
                                    }
                                }

                                if (cloneMetodo != null)
                                {
                                    T cloneInstancia = (T)cloneMetodo.Invoke(instanciaDTO, null);

                                    ResetearObjeto(instanciaDTO);
                                    spResult.Data.Add(cloneInstancia);
                                }
                                else
                                {
                                    spResult.Data.Add(instanciaDTO);
                                }
                            }
                        }

                        EvaluarResultadoPorParametrosSalida(spResult, comando);
                        EvaluarAfectoPorData(spResult);

                        FinalizarResultadoStoredProcedure(spResult, nombreSP);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (ResultadoStoredProcedureException)
                    {
                        throw;
                    }
                    catch (SqlException sqlEx)
                    {
                        spResult.Exito = false;
                        spResult.Exception = sqlEx;
                        spResult.MensajeError = $"Error SQL:{sqlEx.Number} {sqlEx.Message}";
                    }
                    catch (Exception ex)
                    {
                        spResult.Exito = false;
                        spResult.Exception = ex;
                        spResult.MensajeError = $"Error general ejecutando SP {nombreSP}: {ex.Message}";

                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                            string.Format(
                                ConstantesClaveAviso.Error + "|EjecutarStoredProcedureQuery|SP:{0}|DTO:{1}|Error:{2}",
                                nombreSP,
                                tipoDTO.FullName,
                                ex));
                        throw;
                    }
                }
            }
            ValidarResultadoStoredProcedure(spResult, nombreSP, validarAfectacion);
            RegistrarAdvertenciaSinAfectacion(spResult, nombreSP, validarAfectacion);
            return spResult;
        }

        /// <summary>
        /// Ejecuta EjecutarStoredProcedureQuery.
        /// </summary>
        protected async Task<StoredProcedureResult<T>> EjecutarStoredProcedureQuery<T>(string nombreSP, Action<IDbCommand> parametros,
            Func<DbDataReader, T> mapearRegistro, CancellationToken ct, bool esVistasT24 = false, int? commandTimeout = null, bool validarAfectacion = false)
        {
            if (mapearRegistro == null)
                throw new ArgumentNullException(nameof(mapearRegistro));

            ct.ThrowIfCancellationRequested();
            StoredProcedureResult<T> spResult = new StoredProcedureResult<T>();
            DbConnection _bdConexion;
            DbCommand _comando;

            if (esVistasT24)
            {
                _bdConexion = basedatosConexionVistasT24.CrearConexion();
                _bdConexion.ConnectionString = ConexionBasedatosFactory.ObtenerCadenaConexion(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW);
                _comando = basedatosConexionVistasT24.CrearComando(nombreSP, _bdConexion);
            }
            else
            {
                _bdConexion = basedatosConexion.CrearConexion();
                _comando = basedatosConexion.CrearComando(nombreSP, _bdConexion);
            }

            using (DbConnection conexion = _bdConexion)
            using (DbCommand comando = _comando)
            {
                try
                {
                    comando.CommandType = CommandType.StoredProcedure;
                    if (commandTimeout.HasValue)
                    {
                        comando.CommandTimeout = commandTimeout.Value;
                    }

                    parametros?.Invoke(comando);
                    ConvertirParametrosNullADbNull(comando);

                    if (comando.Parameters.Contains("@exito"))
                    {
                        comando.Parameters["@exito"].DbType = DbType.Boolean;
                    }

                    await conexion.OpenAsync(ct).ConfigureAwait(false);

                    using (DbDataReader lector = await comando.ExecuteReaderAsync(ct).ConfigureAwait(false))
                    {
                        Dictionary<string, int> columnas = ObtenerColumnasReader(lector);

                        while (await lector.ReadAsync(ct).ConfigureAwait(false))
                        {
                            EvaluarAfectoPorFilasAfectadas(spResult, lector, columnas);
                            EvaluarResultadoPorColumnas(spResult, lector, columnas);
                            spResult.Data.Add(mapearRegistro(lector));
                        }
                    }

                    EvaluarResultadoPorParametrosSalida(spResult, comando);
                    EvaluarAfectoPorData(spResult);

                    FinalizarResultadoStoredProcedure(spResult, nombreSP);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (ResultadoStoredProcedureException)
                {
                    throw;
                }
                catch (SqlException sqlEx)
                {
                    spResult.Exito = false;
                    spResult.Exception = sqlEx;
                    spResult.MensajeError = $"Error SQL:{sqlEx.Number} {sqlEx.Message}";
                }
                catch (Exception ex)
                {
                    spResult.Exito = false;
                    spResult.Exception = ex;
                    spResult.MensajeError = $"Error general ejecutando SP {nombreSP}: {ex.Message}";

                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                        string.Format(
                            ConstantesClaveAviso.Error + "|EjecutarStoredProcedureQuery|SP:{0}|Mapper:{1}|Error:{2}",
                            nombreSP,
                            typeof(T).FullName,
                            ex));
                }
            }

            ValidarResultadoStoredProcedure(spResult, nombreSP, validarAfectacion);
            RegistrarAdvertenciaSinAfectacion(spResult, nombreSP, validarAfectacion);
            return spResult;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerColumnasReader.
        /// </summary>
        private Dictionary<string, int> ObtenerColumnasReader(DbDataReader lector)
        {
            Dictionary<string, int> columnas = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < lector.FieldCount; i++)
            {
                string nombreColumna = lector.GetName(i);

                if (!columnas.ContainsKey(nombreColumna))
                {
                    columnas.Add(nombreColumna, i);
                }
            }

            return columnas;
        }

        /// <summary>
        /// Inicializa una nueva instancia de EvaluarAfectoPorFilasAfectadas.
        /// </summary>
        private void EvaluarAfectoPorFilasAfectadas(StoredProcedureResult spResult, DbDataReader lector, Dictionary<string, int> columnas)
        {
            int ordinal;
            if (!columnas.TryGetValue("FilasAfectadas", out ordinal))
                return;

            object valor = lector.GetValue(ordinal);
            if (valor == null || valor == DBNull.Value)
            {
                spResult.Afecto = null;
                return;
            }

            int filasAfectadas = Convert.ToInt32(valor);
            EvaluarAfectoPorFilasAfectadas(spResult, filasAfectadas);
        }

        /// <summary>
        /// Inicializa una nueva instancia de EvaluarAfectoPorFilasAfectadas.
        /// </summary>
        private void EvaluarAfectoPorFilasAfectadas(StoredProcedureResult spResult, int filasAfectadas)
        {
            spResult.FilasAfectadas = filasAfectadas;

            if (filasAfectadas > 0)
                spResult.Afecto = true;
            else if (filasAfectadas == 0)
                spResult.Afecto = false;
            else
                spResult.Afecto = null;
        }

        private void EvaluarResultadoPorColumnas(StoredProcedureResult spResult, DbDataReader lector, Dictionary<string, int> columnas)
        {
            int ordinal;

            if (columnas.TryGetValue("Exito", out ordinal))
            {
                object valorExito = lector.GetValue(ordinal);
                if (valorExito != null && valorExito != DBNull.Value)
                    spResult.Exito = ConvertirABooleano(valorExito);
            }

            string mensajeError = ObtenerTextoColumna(lector, columnas, "MensajeError")
                ?? ObtenerTextoColumna(lector, columnas, "ErrorMessage")
                ?? ObtenerTextoColumna(lector, columnas, "IdentificadorError")
                ?? ObtenerTextoColumna(lector, columnas, "IDENTIFICADORERROR");

            if (!string.IsNullOrWhiteSpace(mensajeError))
                spResult.MensajeError = mensajeError;
        }

        private string ObtenerTextoColumna(DbDataReader lector, Dictionary<string, int> columnas, string nombreColumna)
        {
            int ordinal;
            if (!columnas.TryGetValue(nombreColumna, out ordinal))
                return null;

            object valor = lector.GetValue(ordinal);
            if (valor == null || valor == DBNull.Value)
                return null;

            string texto = valor.ToString();
            return string.IsNullOrWhiteSpace(texto) ? null : texto;
        }

        private void EvaluarResultadoPorParametrosSalida(StoredProcedureResult spResult, IDbCommand comando)
        {
            if (comando?.Parameters == null)
                return;

            IDataParameter parametroExito = ObtenerParametro(comando, "@exito");
            if (parametroExito?.Value != null && parametroExito.Value != DBNull.Value)
                spResult.Exito = ConvertirABooleano(parametroExito.Value);

            IDataParameter parametroMensajeError = ObtenerParametro(comando, "@mensajeError")
                ?? ObtenerParametro(comando, "@MensajeError")
                ?? ObtenerParametro(comando, "@ErrorMessage");

            if (parametroMensajeError?.Value == null || parametroMensajeError.Value == DBNull.Value)
                return;

            string mensajeError = parametroMensajeError.Value.ToString();
            if (!string.IsNullOrWhiteSpace(mensajeError))
                spResult.MensajeError = mensajeError;
        }

        private IDataParameter ObtenerParametro(IDbCommand comando, string nombreParametro)
        {
            if (comando?.Parameters == null || !comando.Parameters.Contains(nombreParametro))
                return null;

            return comando.Parameters[nombreParametro] as IDataParameter;
        }

        private void FinalizarResultadoStoredProcedure(StoredProcedureResult spResult, string nombreSP)
        {
            if (!string.IsNullOrWhiteSpace(spResult.MensajeError))
            {
                spResult.Exito = false;
                throw new ResultadoStoredProcedureException(
                    string.Format("Error ejecutando SP {0}: {1}", nombreSP, spResult.MensajeError));
            }

            if (!spResult.Exito)
                spResult.Exito = true;
        }

        private void RegistrarAdvertenciaSinAfectacion(StoredProcedureResult spResult, string nombreSP, bool validarAfectacion)
        {
            if (validarAfectacion || !spResult.Exito)
                return;

            if (spResult.Afecto == true)
                return;

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                string.Format(
                    "WARN|EjecutarStoredProcedureQuery|SP:{0}|ExitoTecnico:true|Afecto:{1}|Mensaje:El SP no afectó filas ni devolvió resultados en modo no estricto.",
                    nombreSP,
                    spResult.Afecto.HasValue ? spResult.Afecto.Value.ToString() : "NULL"));
        }

        /// <summary>
        /// Ejecuta EvaluarAfectoPorData.
        /// </summary>
        private void EvaluarAfectoPorData<T>(StoredProcedureResult<T> spResult)
        {
            if (!spResult.Afecto.HasValue)
                spResult.Afecto = spResult.Data.Count > 0;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ValidarResultadoStoredProcedure.
        /// </summary>
        private void ValidarResultadoStoredProcedure(StoredProcedureResult spResult, string nombreSP, bool validarAfectacion)
        {
            if (!validarAfectacion)
                return;

            if (!spResult.Exito)
            {
                throw spResult.Exception ?? new Exception(
                    string.Format("Error ejecutando SP {0}: {1}", nombreSP, spResult.MensajeError));
            }

            if (spResult.Afecto == true)
                return;

            if (spResult.Afecto == false)
            {
                throw new Exception(
                    string.Format("El SP {0} se ejecutó correctamente, pero no afectó filas ni devolvió resultados.", nombreSP));
            }

            throw new Exception(
                string.Format("El SP {0} se ejecutó correctamente, pero no se pudo determinar si afectó filas o devolvió resultados.", nombreSP));
        }

        /// <summary>
        /// Inicializa una nueva instancia de IntentarConvertirValor.
        /// </summary>
        private bool IntentarConvertirValor(object dbValue, Type tipoDestino, out object valorConvertido)
        {
            valorConvertido = null;

            try
            {
                if (dbValue == null || dbValue == DBNull.Value)
                {
                    if (EsTipoNullable(tipoDestino))
                    {
                        valorConvertido = null;
                        return true;
                    }

                    valorConvertido = ObtenerValorDefault(tipoDestino);
                    return true;
                }

                Type tipoRealDestino = Nullable.GetUnderlyingType(tipoDestino) ?? tipoDestino;
                Type tipoOrigen = dbValue.GetType();

                if (tipoRealDestino.IsAssignableFrom(tipoOrigen))
                {
                    valorConvertido = dbValue;
                    return true;
                }

                if (tipoRealDestino.IsEnum)
                {
                    if (dbValue is string)
                    {
                        valorConvertido = Enum.Parse(tipoRealDestino, dbValue.ToString(), true);
                        return true;
                    }

                    object valorEnumBase = Convert.ChangeType(dbValue, Enum.GetUnderlyingType(tipoRealDestino));
                    valorConvertido = Enum.ToObject(tipoRealDestino, valorEnumBase);
                    return true;
                }

                if (tipoRealDestino == typeof(Guid))
                {
                    valorConvertido = new Guid(dbValue.ToString());
                    return true;
                }

                if (tipoRealDestino == typeof(bool))
                {
                    valorConvertido = ConvertirABooleano(dbValue);
                    return true;
                }

                if (tipoRealDestino == typeof(string))
                {
                    valorConvertido = dbValue.ToString();
                    return true;
                }

                valorConvertido = Convert.ChangeType(
                    dbValue,
                    tipoRealDestino,
                    System.Globalization.CultureInfo.InvariantCulture);

                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de EsTipoNullable.
        /// </summary>
        private bool EsTipoNullable(Type tipo)
        {
            if (!tipo.IsValueType)
                return true;

            return Nullable.GetUnderlyingType(tipo) != null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirParametrosNullADbNull.
        /// </summary>
        private static void ConvertirParametrosNullADbNull(IDbCommand comando)
        {
            if (comando == null || comando.Parameters == null)
                return;

            foreach (IDataParameter parametro in comando.Parameters)
            {
                if ((parametro.Direction == ParameterDirection.Input ||
                     parametro.Direction == ParameterDirection.InputOutput) &&
                    parametro.Value == null)
                {
                    parametro.Value = DBNull.Value;
                }
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerValorDefault.
        /// </summary>
        private object ObtenerValorDefault(Type tipo)
        {
            if (!tipo.IsValueType)
                return null;

            return Activator.CreateInstance(tipo);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirABooleano.
        /// </summary>
        private bool ConvertirABooleano(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return false;

            if (valor is bool)
                return (bool)valor;

            string texto = valor.ToString().Trim();

            if (texto == "1")
                return true;

            if (texto == "0")
                return false;

            if (string.Equals(texto, "true", StringComparison.OrdinalIgnoreCase))
                return true;

            if (string.Equals(texto, "false", StringComparison.OrdinalIgnoreCase))
                return false;

            if (string.Equals(texto, "S", StringComparison.OrdinalIgnoreCase))
                return true;

            if (string.Equals(texto, "N", StringComparison.OrdinalIgnoreCase))
                return false;

            if (string.Equals(texto, "Y", StringComparison.OrdinalIgnoreCase))
                return true;

            if (string.Equals(texto, "YES", StringComparison.OrdinalIgnoreCase))
                return true;

            if (string.Equals(texto, "NO", StringComparison.OrdinalIgnoreCase))
                return false;

            return Convert.ToBoolean(valor);
        }

        /// <summary>
        /// Ejecuta SP para INSERT, UPDATE, DELETE y devuelve el ID generado por Identity
        /// </summary>
        /// <param name="nombreSP"></param>
        /// <param name="parametros"></param>
        /// <returns>StoredProcedureResult</returns>
        protected Task<StoredProcedureResult> EjecutarStoredProceduredNonQuery(string nombreSP, Action<IDbCommand> parametros, bool esVistasT24 = false)
        {
            return EjecutarStoredProceduredNonQuery(nombreSP, parametros, CancellationToken.None, esVistasT24);
        }

        /// <summary>
        /// Inicializa una nueva instancia de EjecutarStoredProceduredNonQuery.
        /// </summary>
        protected async Task<StoredProcedureResult> EjecutarStoredProceduredNonQuery(string nombreSP, Action<IDbCommand> parametros, CancellationToken ct, bool esVistasT24 = false)
        {
            ct.ThrowIfCancellationRequested();
            StoredProcedureResult spResult = new StoredProcedureResult();
            DbConnection _bdConexion;
            DbCommand _comando;
            if (esVistasT24)
            {
                _bdConexion = basedatosConexionVistasT24.CrearConexion();
                _bdConexion.ConnectionString = ConexionBasedatosFactory.ObtenerCadenaConexion(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW);
                _comando = basedatosConexionVistasT24.CrearComando(nombreSP, _bdConexion);
            }
            else
            {
                _bdConexion = basedatosConexion.CrearConexion();
                _comando = basedatosConexion.CrearComando(nombreSP, _bdConexion);
            }
            using (DbConnection conexion = _bdConexion)
            {
                using (DbCommand comando = _comando)
                {
                    try
                    {
                        comando.CommandType = CommandType.StoredProcedure;
                        parametros?.Invoke(comando);

                        if (comando.Parameters.Contains("@exito"))
                        {
                            comando.Parameters["@exito"].DbType = DbType.Boolean;
                        }

                        await conexion.OpenAsync(ct);
                        await comando.ExecuteNonQueryAsync(ct);
                        EvaluarResultadoPorParametrosSalida(spResult, comando);

                        FinalizarResultadoStoredProcedure(spResult, nombreSP);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (ResultadoStoredProcedureException)
                    {
                        throw;
                    }
                    catch (SqlException sqlEx)
                    {
                        spResult.Exito = false;
                        spResult.Exception = sqlEx;
                        spResult.MensajeError = $"Error SQL:{sqlEx.Number} {sqlEx.Message}";
                    }
                    catch (Exception ex)
                    {
                        spResult.Exito = false;
                        spResult.Exception = ex;
                        spResult.MensajeError = $"Error general ejecutando SP {nombreSP}: {ex.Message}";
                    }
                }
            }
            return spResult;
        }

        /// <summary>
        /// Metodo para NonQuery, si la ejecucion de vuelve datos, los guarda en Data
        /// Para funciones que necesitan devolver una consulta despues de un Insert/Update/Delete
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="nombreSP"></param>
        /// <param name="parametros"></param>
        /// <param name="mapearEntidad"></param>
        /// <param name="esVistasT24"></param>
        /// <returns>StoredProcedureResult<T></returns>
        protected async Task<StoredProcedureResult<T>> EjecutarStoredProceduredNonQueryConResultado<T>(
            string nombreSP,
            Action<IDbCommand> parametros,
            Func<IDataReader, T> mapearEntidad,
            bool esVistasT24 = false)
        {
            StoredProcedureResult<T> spResult = new StoredProcedureResult<T>();
            DbConnection _bdConexion;
            DbCommand _comando;
            if (esVistasT24)
            {
                _bdConexion = basedatosConexionVistasT24.CrearConexion();
                _comando = basedatosConexionVistasT24.CrearComando(nombreSP, _bdConexion);
            }
            else
            {
                _bdConexion = basedatosConexion.CrearConexion();
                _comando = basedatosConexion.CrearComando(nombreSP, _bdConexion);
            }
            using (DbConnection conexion = basedatosConexion.CrearConexion())
            {
                using (DbCommand comando = basedatosConexion.CrearComando(nombreSP, conexion))
                {
                    try
                    {
                        comando.CommandType = CommandType.StoredProcedure;
                        parametros?.Invoke(comando);

                        await conexion.OpenAsync();

                        // Lectura de consulta, si devuelve sino no entra
                        using (DbDataReader lector = await comando.ExecuteReaderAsync())
                        {
                            Dictionary<string, int> columnas = ObtenerColumnasReader(lector);

                            while (await lector.ReadAsync())
                            {
                                EvaluarAfectoPorFilasAfectadas(spResult, lector, columnas);
                                EvaluarResultadoPorColumnas(spResult, lector, columnas);
                                spResult.Data.Add(mapearEntidad(lector));
                            }
                        }
                        if (!spResult.Afecto.HasValue)
                            spResult.FilasAfectadas = spResult.Data.Count;

                        EvaluarAfectoPorData(spResult);
                        FinalizarResultadoStoredProcedure(spResult, nombreSP);
                    }
                    catch (ResultadoStoredProcedureException)
                    {
                        throw;
                    }
                    catch (SqlException sqlEx)
                    {
                        spResult.Exito = false;
                        spResult.Exception = sqlEx;
                        spResult.MensajeError = $"Error SQL:{sqlEx.Number} {sqlEx.Message}";
                    }
                    catch (Exception ex)
                    {
                        spResult.Exito = false;
                        spResult.Exception = ex;
                        spResult.MensajeError = $"Error general ejecutando SP {nombreSP}: {ex.Message}";
                    }
                }
            }
            return spResult;
        }

        /// <summary>
        /// Ejecuta una operacion sobre una conexion de BD abierta y devuelve informacion especifica del proveedor utilizado
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="selector"></param>
        /// <returns>T</returns>
        private T GetDbInfo<T>(Func<DbConnection, T> selector)
        {
            using (DbConnection conexion = basedatosConexion.CrearConexion())
            {
                conexion.Open();
                if (conexion is DbConnection dbCon)
                {
                    return selector(dbCon);
                }
                return default;
            }
        }

        #region Informacion de proveedor base de datos
        /// <summary>
        /// Inicializa una nueva instancia de ObtenerVersionServidor.
        /// </summary>
        protected string ObtenerVersionServidor() => GetDbInfo(c => c.ServerVersion ?? EsquemaColumnas.VERSION_DESCONOCIDA);

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerNombreBD.
        /// </summary>
        protected string ObtenerNombreBD() => GetDbInfo(c => c.Database.ToString() ?? EsquemaColumnas.BASEDATOS_DESCONICIDA);

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerNombreServidor.
        /// </summary>
        protected string ObtenerNombreServidor() => GetDbInfo(c => c.DataSource.ToString() ?? EsquemaColumnas.SERVIDOR_DESCONOCIDO);
        #endregion

        /// <summary>
        /// Reinicia las propiedades de un objeto a sus props' por defecto
        /// </summary>
        /// <param name="objetoResetear"></param>
        public static void ResetearObjeto(object objetoResetear)
        {
            if (objetoResetear == null) return;

            Type tipoObjeto = objetoResetear.GetType();
            foreach (PropertyInfo propiedad in tipoObjeto.GetProperties())
            {
                if (propiedad.CanWrite)
                {
                    object valorPorDefecto = null;
                    if (propiedad.PropertyType.IsValueType)
                    {
                        valorPorDefecto = Activator.CreateInstance(propiedad.PropertyType);
                    }
                    propiedad.SetValue(objetoResetear, valorPorDefecto);
                }
            }
        }

        /// <summary>
        /// Obtiene DataTable
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="parametrosConfig"></param>
        /// <returns>DataTable</returns>
        public async Task<DataTable> EjecutarEsquemaProcedimientoAlmacenado(string nombreSp, Action<IDbCommand> parametrosConfig = null)
        {
            using (DbConnection conexion = basedatosConexion.CrearConexion())
            {
                using (DbCommand comando = basedatosConexion.CrearComando(nombreSp, conexion))
                {
                    comando.CommandType = CommandType.StoredProcedure;
                    parametrosConfig?.Invoke(comando);

                    await conexion.OpenAsync();
                    using (DbDataReader lector = await comando.ExecuteReaderAsync(CommandBehavior.SchemaOnly))
                    {
                        return CrearEsquemaDataTable(lector.GetSchemaTable());
                    }
                }
            }
        }

        /// <summary>
        /// Crea DT con esquema
        /// </summary>
        /// <param name="esquema"></param>
        /// <returns>DataTable</returns>
        private static DataTable CrearEsquemaDataTable(DataTable esquema)
        {
            if (esquema == null)
                return null;

            DataTable tabla = new DataTable();

            foreach (DataRow fila in esquema.Rows)
            {
                tabla.Columns.Add(CrearColumnas(fila));
            }
            return tabla;
        }

        /// <summary>
        /// Crear las columnas y propiedades
        /// </summary>
        /// <param name="fila"></param>
        /// <returns>DataColumn</returns>
        private static DataColumn CrearColumnas(DataRow fila)
        {
            DataColumn columna = new DataColumn
            {
                ColumnName = fila.Field<string>(EsquemaColumnas.NOMBRECOLUMNA),
                DataType = fila.Field<Type>(EsquemaColumnas.TIPODATO),
                AllowDBNull = fila.Field<bool>(EsquemaColumnas.PERMITIRDBNULL),
                Unique = fila.Field<bool>(EsquemaColumnas.NOMBRECOLUMNA)
            };

            AgregarPropiedad(columna, EsquemaColumnas.PRESICION, fila[EsquemaColumnas.PRESICIONNUMERICA]);
            AgregarPropiedad(columna, EsquemaColumnas.ESCALA, fila[EsquemaColumnas.ESCALANUMERICA]);
            AgregarPropiedad(columna, EsquemaColumnas.TAMAÑO, fila[EsquemaColumnas.TAMAÑOCOLUMNAS]);
            AgregarPropiedad(columna, EsquemaColumnas.TIPODATOPROVEEDOR, fila[EsquemaColumnas.TIPODATOPROVEEDOR]);

            return columna;
        }

        /// <summary>
        /// Agrega las propiedades a la columna
        /// </summary>
        /// <param name="columna"></param>
        /// <param name="nombre"></param>
        /// <param name="valor"></param>
        private static void AgregarPropiedad(DataColumn columna, string nombre, object valor)
        {
            if (valor != DBNull.Value)
            {
                columna.ExtendedProperties[nombre] = valor;
            }
        }

        /// <summary>
        /// Actualiza el estado del controlador de servicios
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="nombreServicio"></param>
        /// <returns>StoredProcedureResult</returns>
        public Task<StoredProcedureResult> ActualizaEJecucion(string nombreServicio)
        {
            return ActualizaEJecucion(nombreServicio, CancellationToken.None);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ActualizaEJecucion.
        /// </summary>
        public async Task<StoredProcedureResult> ActualizaEJecucion(string nombreServicio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(
                EnumNombreSp.SP_ACTUALIZA_EJECUCION, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreServicio", nombreServicio, 50));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@fecha", DateTime.Now.ToString(EnumParametrosGenerales.FECHA_BD)));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@hora", DateTime.Now.ToLongTimeString()));

                cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
            }, ct);
            if (!spResult.Exito)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(ConstantesClaveAviso.Error + "|Actualiza_Ejecucion|Se presenta exception: " + spResult.MensajeError.ToString());
                throw new Exception(spResult.MensajeError.ToString());
            }

            return spResult;
        }

        /// <summary>
        /// Actualiza el estado de terminacion del servicio controlador
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="nombreServicio"></param>
        /// <returns>StoredProcedureResult</returns>
        public Task<StoredProcedureResult> ActualizaTerminacion(string nombreServicio)
        {
            return ActualizaTerminacion(nombreServicio, CancellationToken.None);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ActualizaTerminacion.
        /// </summary>
        public async Task<StoredProcedureResult> ActualizaTerminacion(string nombreServicio, CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(
                    EnumNombreSp.SP_ACTUALIZA_TERMINACION, cmd =>
                {
                    //@fecha CHAR(10),
                    //@hora            NVARCHAR(20),
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreServicio", nombreServicio, 50));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@fecha", DateTime.Now.ToString(EnumParametrosGenerales.FECHA_BD)));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@hora", DateTime.Now.ToLongTimeString()));
                    cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
                }, ct);

                if (!spResult.Exito)
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(ConstantesClaveAviso.Error + "|Actualiza_Terminacion|Se presenta exception: " + spResult.MensajeError);
                }

                return spResult;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(ConstantesClaveAviso.Error + "|Actualiza_Terminacion|Se presenta exception: " + ex.Message);
                return new StoredProcedureResult
                {
                    Exito = false,
                    MensajeError = ex.Message,
                    Exception = ex
                };
            }
        }

        /// <summary>
        /// Valida si puede inicar el servicio
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <returns>bool</returns>
        public Task<bool> EsDiaHabil()
        {
            return EsDiaHabil(CancellationToken.None);
        }

        /// <summary>
        /// Inicializa una nueva instancia de EsDiaHabil.
        /// </summary>
        public async Task<bool> EsDiaHabil(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            bool resultado = false;
            try
            {
                // Se va a forzar el inicio de los servicios?
                string strValorConfig = ConexionBasedatosFactory.ObtenerDatosGenerales("FORZA_EJECUCION_SERVICIOS");
                if (strValorConfig == "1")
                {
                    resultado = true;
                    return resultado;
                }

                // Es fin de semana
                if (DateTime.Now.DayOfWeek == DayOfWeek.Saturday || DateTime.Now.DayOfWeek == DayOfWeek.Sunday)
                {
                    resultado = false;
                }
                else
                {
                    resultado = true;
                    StoredProcedureResult<CampoGeneral> resultadoSp = await EjecutarStoredProcedureQuery<CampoGeneral>(
                        EnumNombreSp.SP_ES_DIA_HABIL,
                        cmd => cmd.Parameters.Add(
                            basedatosConexion.CrearParametro("@fecha",
                                DateTime.Now.Date.ToString(EnumParametrosGenerales.FECHA_HORA0_BD)
                            )
                        ),
                        ct
                    );

                    if (resultadoSp.Data.FirstOrDefault()?.Num_regs > 0)
                        resultado = false;
                }

                return resultado;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                string strMsjError = $"Error: no se pudo recuperar dia habil. Excepcion: {ex.Message}";
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(strMsjError, NombreServicio.MUFG_T24.ToString());
                throw;
            }
            finally
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|EsDiaHabil|Resultado retornado: {resultado}");
            }
        }

        /// <summary>
        /// Para obtener el folio maestro
        /// Legacy: dameFolioMaestro
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="folio"></param>
        /// <param name="tipoOperacion"></param>
        /// <returns>string</returns>
        public async Task<string> ObtenerFolioMaestro(string folio, string tipoOperacion)
        {
            StoredProcedureResult<CampoGeneral> spResult = await EjecutarStoredProcedureQuery<CampoGeneral>(
                EnumNombreSp.SP_OBTENER_FOLIO_MAESTRO,
                cmd =>
                {
                    // @tipoOperacion		VARCHAR(10),
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", ConvertirFolioAParametro(folio, nameof(ObtenerFolioMaestro))));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", tipoOperacion));
                });

            CampoGeneral folioMaestro = spResult.Data.Count > 0 ? spResult.Data[0] : null;
            if (folioMaestro == null)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ObtenerFolioMaestro|No se obtuvo folio maestro para folio '{folio}' y tipo operación '{tipoOperacion}'.");
                throw new InvalidOperationException($"No se obtuvo el folio maestro requerido para el folio '{folio}'.");
            }

            return folioMaestro.Folio_maestro.ToString();
        }

        private object ConvertirFolioAParametro(string folio, string contexto)
        {
            int folioConvertido;
            if (int.TryParse(folio, NumberStyles.Integer, CultureInfo.InvariantCulture, out folioConvertido))
                return folioConvertido;

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|{contexto}|No fue posible convertir el folio '{folio}' a entero. Se enviará DBNull.Value al procedimiento.");
            return DBNull.Value;
        }

        /// <summary>
        /// Para obtener el FT que se enviará como Clave de Rastreo en SPEI
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="folio"></param>
        /// <param name="tipoOperacion"></param>
        /// <returns>string</returns>
        public async Task<string> ObtenerFtSpei(string folio)
        {
            CampoGeneral resultado = (await EjecutarStoredProcedureQuery<CampoGeneral>(
                EnumNombreSp.SP_OBTENER_FT_SPEI,
                cmd =>
                {
                    // @op_clave as numeric(18,0)
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio, 65));
                }
            )).Data.FirstOrDefault();

            if (resultado == null)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ObtenerFtSpei|No se obtuvo FT SPEI para folio '{folio}'.");
                return null;
            }

            return resultado.ToString();
        }

        /// <summary>
        /// Obtiene el ultimo proceso en ejecucion
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="folio"></param>
        /// <param name="tipoOperacion"></param>
        /// <returns>string</returns>
        public async Task<string> ObtenerUltimoProceso(ProcesoDTO proceso)
        {
            CampoGeneral resultado = (await EjecutarStoredProcedureQuery<CampoGeneral>(
                EnumNombreSp.SP_OBTENER_ULTIMO_PROCESO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", proceso.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", proceso.TipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@servicio", proceso.Servicio));
                }
            )).Data.FirstOrDefault();

            if (resultado is null)
            {
                return "";
            }

            return resultado.Alias_proceso;
        }

        /// <summary>
        /// Marca el inicia proceso
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="folio"></param>
        /// <param name="tipoOperacion"></param>
        /// <param name="servicio"></param>
        /// <returns>StoredProcedureResult</returns>
        public Task<StoredProcedureResult> IniciaProceso(ProcesoDTO proceso)
        {
            return IniciaProceso(proceso, CancellationToken.None);
        }

        /// <summary>
        /// Inicializa una nueva instancia de IniciaProceso.
        /// </summary>
        public async Task<StoredProcedureResult> IniciaProceso(ProcesoDTO proceso, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(
                EnumNombreSp.SP_INICIA_PROCESO, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pId", proceso.PId));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreProceso", proceso.NombreProceso, 50));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", ConvertirFolioAParametro(proceso.Folio, nameof(IniciaProceso))));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", proceso.TipoOperacion, 10));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@servicio", proceso.Servicio, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@aliasProceso", proceso.AliasProceso, 25));

                cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
            }, ct);
            return spResult;
        }

        /// <summary>
        /// Marca el final del proceso
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="folio"></param>
        /// <param name="tipoOperacion"></param>
        /// <param name="nombreProceso"></param>
        /// <param name="servicio"></param>
        /// <param name="aliasProceso"></param>
        /// <param name="pId"></param>
        /// <returns>StoredProcedureResult</returns>
        public Task<StoredProcedureResult> TerminaProceso(ProcesoDTO proceso)
        {
            return TerminaProceso(proceso, CancellationToken.None);
        }

        /// <summary>
        /// Inicializa una nueva instancia de TerminaProceso.
        /// </summary>
        public async Task<StoredProcedureResult> TerminaProceso(ProcesoDTO proceso, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(
                EnumNombreSp.SP_TERMINA_PROCESO, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pId", proceso.PId));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreProceso", proceso.NombreProceso, 50));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", ConvertirFolioAParametro(proceso.Folio, nameof(TerminaProceso))));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", proceso.TipoOperacion, 10));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@servicio", proceso.Servicio, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@exepcion", proceso.Excepcion?.Message ?? "null"));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@aliasProceso", proceso.AliasProceso, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));

            }, ct);
            return spResult;
        }

        /// <summary>
        /// Marca el proceso con errores
        /// </summary>
        /// <param name="proceso"></param>
        /// <param name="nombreSp"></param>
        /// <returns></returns>
        public Task<StoredProcedureResult> ActualizaErrorProceso(ProcesoDTO proceso)
        {
            return ActualizaErrorProceso(proceso, CancellationToken.None);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ActualizaErrorProceso.
        /// </summary>
        public async Task<StoredProcedureResult> ActualizaErrorProceso(ProcesoDTO proceso, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(
                EnumNombreSp.SP_ACTUALIZA_ERROR_PROCESO, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pId", proceso.PId));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreProceso", proceso.NombreProceso, 50));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", ConvertirFolioAParametro(proceso.Folio, nameof(ActualizaErrorProceso))));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", proceso.TipoOperacion, 10));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@servicio", proceso.Servicio, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@exepcion", proceso.Excepcion?.Message ?? "null"));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@aliasProceso", proceso.AliasProceso, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));

            }, ct);
            return spResult;
        }

        /// <summary>
        /// Para obtener el DisplayName del servicio
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="proceso"></param>
        /// <returns>string</returns>
        public Task<string> ObtieneDisplayName(int procesoId)
        {
            try
            {
                using (var buscador = new ManagementObjectSearcher(
                    $"SELECT DisplayName FROM Win32_Service WHERE ProcessId = {procesoId}"))
                {
                    foreach (ManagementObject obj in buscador.Get())
                    {
                        return Task.FromResult(obj["DisplayName"]?.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ObtieneDisplayName|{ex}");
            }

            return Task.FromResult<string>(null);
        }

        /// <summary>
        /// Obtiene el ultimo proceso en ejecucion en tablas de Swift
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="procesoId"></param>
        /// <returns></returns>
        public async Task<string> ObtieneUltimoProcesoSwift(ProcesoDTO proceso)
        {
            CampoGeneral resultado = (await EjecutarStoredProcedureQuery<CampoGeneral>(
               EnumNombreSp.SP_ULTIMO_PROCESO_SWF,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", proceso.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", proceso.TipoOperacion));
                }
            )).Data.FirstOrDefault();

            if (resultado == null)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ObtieneUltimoProcesoSwift|No se obtuvo último proceso Swift para folio '{proceso.Folio}' y tipo operación '{proceso.TipoOperacion}'.");
                return null;
            }

            return resultado.Alias_proceso;
        }

        /// <summary>
        /// Marca el inicia proceso en tablas de Swift
        /// </summary>
        /// <param name="proceso"></param>
        /// <param name="nombreSp"></param>
        /// <returns>StoredProcedureResult</returns>
        public async Task<StoredProcedureResult> IniciaProcesoSwift(ProcesoDTO proceso)
        {
            StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(
                EnumNombreSp.SP_INICIA_PROCESO_SWF, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", proceso.Folio, 29));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", proceso.TipoOperacion, 10));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@aliasProceso", proceso.AliasProceso, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));

            });
            return spResult;
        }

        /// <summary>
        /// Marca el final del proceso en tablas de Swift
        /// </summary>
        /// <param name="proceso"></param>
        /// <param name="nombreSp"></param>
        /// <returns>StoredProcedureResult</returns>
        public async Task<StoredProcedureResult> TerminaProcesoSwift(ProcesoDTO proceso)
        {
            StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(
                EnumNombreSp.SP_TERMINA_PROCESO_SWF, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", proceso.Folio, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", proceso.TipoOperacion, 10));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@aliasProceso", proceso.AliasProceso, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
            });
            return spResult;
        }

        /// <summary>
        /// Marca el proceso con errores en tablas de Swift
        /// </summary>
        /// <param name="proceso"></param>
        /// <param name="nombreSp"></param>
        /// <returns>StoredProcedureResult</returns>
        public async Task<StoredProcedureResult> ActualizaErrorProcesoSwift(ProcesoDTO proceso)
        {
            StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(
                EnumNombreSp.SP_ACTUALIZA_ERROR_PROCESO_SWF, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", proceso.Folio, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", proceso.TipoOperacion, 10));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@aliasProceso", proceso.AliasProceso, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@exepcion", proceso.Excepcion?.Message ?? "null"));
                cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
            });
            return spResult;
        }

        #region METODOS CALYPSO

        /// <summary>
        /// Obtiene short name de calypso
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="cdl"></param>
        /// <returns>string</returns>
        public async Task<string> CalypsoObtenerNombreCorto(string cdl, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();

            LegalEntity resultado = (await EjecutarReader<LegalEntity>(
                "SELECT short_name FROM LEGAL_ENTITY WHERE LPAD(external_ref,8,'0') = :cdl", cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro(":cdl", cdl.PadLeft(8, '0')));
                }, ct: ct
            )).Data.FirstOrDefault();

            ct.ThrowIfCancellationRequested();

            if (resultado == null || string.IsNullOrWhiteSpace(resultado.SHORT_NAME))
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|CalypsoObtenerNombreCorto|No se obtuvo nombre corto de Calypso para CDL '{cdl}'.");
                throw new InvalidOperationException($"No se obtuvo nombre corto de Calypso para CDL '{cdl}'.");
            }

            return resultado.SHORT_NAME;
        }

        /// <summary>
        /// Obtiene consumo de lineas
        /// </summary>
        /// <param name="nombreSp"></param>
        /// <param name="cdl"></param>
        /// <returns>DataTable</returns>
        public async Task<DataTable> CalypsoConsultaLimiteLineas(string cdl = "")
        {
            StringBuilder sql = new StringBuilder();
            sql.AppendLine("SELECT A.LIMIT_ID, A.ATTR1_VALUE, SUBSTR(SUBSTR(A.ATTR1_VALUE,INSTR(A.ATTR1_VALUE,'CD',1,1),10),3,8) AS CDL, A.LIMIT_MAX AS MONTO_LINEA, A.LIMIT_CCY AS CCY_LINEA, C.AMOUNT AS SALDO_UTILIZADO, C.SANCTION_AMOUNT AS SALDO_DISPONIBLE, C.VALUE_DATE");
            sql.AppendLine("  FROM ERS_LIMIT A INNER JOIN (SELECT LIMIT_ID, MAX(VALUE_DATE) AS VALUE_DATE FROM ERS_LIMIT_USAGE WHERE VERSION=0 GROUP BY LIMIT_ID) B ON ( A.LIMIT_ID = B.LIMIT_ID )");
            sql.AppendLine("                                     INNER JOIN ERS_LIMIT_USAGE C ON ( A.LIMIT_ID = C.LIMIT_ID AND B.VALUE_DATE = C.VALUE_DATE )");
            sql.AppendLine("  WHERE A.LIMIT_BUCKET = '0D-2D'");
            sql.AppendLine("    AND C.VERSION=0");
            sql.AppendLine("    AND A.MEASURE='Settlement' ");

            bool filtrarCdl = !string.IsNullOrEmpty(cdl);
            if (filtrarCdl)
            {
                sql.AppendLine("    AND A.ATTR1_VALUE LIKE :cdl");
            }

            DataTable resultado = new DataTable();
            using (DbConnection conexion = basedatosConexion.CrearConexion())
            using (DbCommand comando = basedatosConexion.CrearComando(sql.ToString(), conexion))
            {
                comando.CommandType = CommandType.Text;

                if (filtrarCdl)
                {
                    comando.Parameters.Add(basedatosConexion.CrearParametro(":cdl", "%" + cdl.PadLeft(8, '0')));
                }

                await conexion.OpenAsync().ConfigureAwait(false);
                using (DbDataReader lector = await comando.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    resultado.Load(lector);
                }
            }

            return resultado;
        }

        /// <summary>
        /// Define el nombre del TAG del nodo root para el XML serializado  
        /// </summary>
        /// <param name="tipoObjeto"></param>
        /// <param name="nombreNodoRoot"></param>
        /// <returns>string</returns>
        private string DevuelveTagName(Type tipoObjeto, string nombreNodoRoot)
        {
            // si viene definido el nombre raiz, usarlo
            if (!string.IsNullOrWhiteSpace(nombreNodoRoot))
                return nombreNodoRoot.Trim();

            if (tipoObjeto == null)
                return "Unknown";

            // si es un tipo generico
            if (tipoObjeto.IsGenericType && tipoObjeto.GetGenericTypeDefinition() == typeof(List<>))
            {
                Type tipoGenerico = tipoObjeto.GetGenericArguments()[0];
                return tipoGenerico.Name + "List";
            }

            // si es un arreglo
            if (tipoObjeto.IsArray)
            {
                return tipoObjeto.GetElementType()?.Name + "Array";
            }

            // caso normal
            return tipoObjeto.Name;
        }

        /// <summary>
        /// Serializar objecto en XML
        /// </summary>
        /// <param name="objeto"></param>
        /// <param name="tipoSerializacion"></param>
        /// <param name="RemoveDiacritics"></param>
        /// <param name="AddNodeTag"></param>
        /// <param name="encodingEnum"></param>
        /// <param name="rootNodeName"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="NotSupportedException"></exception>
        public string CalypsoSerializarClase(object objeto, TipoSerializacion tipoSerializacion,
            bool RemoveDiacritics, bool AddNodeTag, EncodingEnum encodingEnum = EncodingEnum.Encode_Default,
            string rootNodeName = ""
            )
        {
            if (objeto == null)
                throw new ArgumentNullException(nameof(objeto));

            Encoding encoding = ResolverEncoding(encodingEnum);

            XmlWriterSettings config = new XmlWriterSettings
            {
                Encoding = encoding,
                OmitXmlDeclaration = true,
                NamespaceHandling = NamespaceHandling.OmitDuplicates,
                Indent = true,
                IndentChars = "    "
            };

            string nombreTag = DevuelveTagName(objeto.GetType(), rootNodeName);
            StringBuilder sb = new StringBuilder();

            if (tipoSerializacion == TipoSerializacion.SerialXML)
            {
                SerializarXml(objeto, nombreTag, rootNodeName, config, sb);
            }
            else if (tipoSerializacion == TipoSerializacion.SerialDataContract)
            {
                SerializarDataContract(objeto, nombreTag, config, sb);
            }
            else
            {
                throw new NotSupportedException("Tipo de serializacion no soportado: " + tipoSerializacion);
            }

            string resultado = sb.ToString();
            if (RemoveDiacritics)
                resultado = RemueveDiacriticos(resultado);

            StringBuilder stringBuilder = new StringBuilder();
            if (AddNodeTag)
            {
                stringBuilder.AppendLine(string.Format(@"<?xml version=""1.0"" encoding=""UTF-8"" standalone=""yes""?>", encoding.WebName));
                stringBuilder.Append(@"<CalypsoUploadDocument UploadDate=""20100716"" ClientCode=""INDI"" Version=""1"">");
                stringBuilder.AppendLine(resultado);
                stringBuilder.AppendLine(@"</CalypsoUploadDocument>");

                resultado = sb.ToString();
            }
            return resultado;
        }

        /// <summary>
        /// Inicializa una nueva instancia de CalypsoDeserializarClase.
        /// </summary>
        public bool CalypsoDeserializarClase(string xml, ref object objeto, TipoSerializacion tipoSerializacion, bool removerDataTag, string nombreNodoRoot = "")
        {
            if (xml == null)
                throw new ArgumentException(nameof(xml));
            if (xml.Trim() == "")
                throw new ArgumentException("XML string cannot be empty");
            if (objeto == null)
                throw new ArgumentNullException(nameof(objeto));

            string objetoXml = xml;
            string nombreTag = DevuelveTagName(objeto.GetType(), nombreNodoRoot);
            XmlReaderSettings xmlReaderSettings = new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Prohibit,
                XmlResolver = null
            };

            try
            {
                // si requiere eliminar nodo <DATA>
                if (removerDataTag)
                {
                    using (StringReader sr = new StringReader(xml))
                    using (XmlReader lector = XmlReader.Create(sr, xmlReaderSettings))
                    {
                        while (lector.Read())
                        {
                            if (lector.NodeType == XmlNodeType.Element && lector.Name == "DATA")
                            {
                                objetoXml = lector.ReadInnerXml();
                                break;
                            }
                        }
                    }
                }

                // Deserializacion segun tipo
                if (tipoSerializacion == TipoSerializacion.SerialDataContract)
                {
                    DataContractSerializer serializar = new DataContractSerializer(objeto.GetType(), nombreTag, string.Empty);
                    using (StringReader sr = new StringReader(objetoXml))
                    {
                        using (XmlReader xr = XmlReader.Create(sr, xmlReaderSettings))
                        {
                            using (XmlDictionaryReader xdr = XmlDictionaryReader.CreateDictionaryReader(xr))
                            {
                                objeto = serializar.ReadObject(xdr, true);
                            }
                        }
                    }
                }
                else if (tipoSerializacion == TipoSerializacion.SerialXML)
                {
                    XmlSerializer serializer;
                    if (string.IsNullOrWhiteSpace(nombreNodoRoot))
                    {
                        serializer = new XmlSerializer(objeto.GetType());
                    }
                    else
                    {
                        serializer = new XmlSerializer(objeto.GetType(), new XmlRootAttribute(nombreTag));
                    }

                    using (var sr = new StringReader(objetoXml))
                    {
                        using (XmlReader xr = XmlReader.Create(sr, xmlReaderSettings))
                        {
                            objeto = serializer.Deserialize(xr);
                        }
                    }
                }
                else
                {
                    throw new NotSupportedException("Tipo de serializacion no soportado" + tipoSerializacion);
                }
                return objeto != null;
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Normalizacion de cadena
        /// </summary>
        /// <param name="texto"></param>
        /// <returns>string</returns>
        private static string RemueveDiacriticos(string texto)
        {
            if (string.IsNullOrWhiteSpace(texto))
                return texto;

            // Normaliza a FormD
            string normalizarCadena = texto.Normalize(NormalizationForm.FormD);
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < normalizarCadena.Length; i++)
            {
                char c = normalizarCadena[i];

                // Ignora marcas de acento
                if (CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark)
                {
                    sb.Append(c);
                }
            }

            // Vuelve a componer los caracteres
            return sb.ToString().Normalize(NormalizationForm.FormC);
        }

        /// <summary>
        /// Serializacion con DataContract
        /// </summary>
        /// <param name="objeto"></param>
        /// <param name="nombreTag"></param>
        /// <param name="nombreNodoRoot"></param>
        /// <param name="setting"></param>
        /// <param name="sb"></param>
        private static void SerializarDataContract(object objeto, string nombreTag, XmlWriterSettings setting, StringBuilder sb)
        {
            DataContractSerializer serializer = new DataContractSerializer(objeto.GetType(), nombreTag, string.Empty);

            using (XmlWriter escribir = XmlWriter.Create(sb, setting))
            {
                using (XmlDictionaryWriter dictionaryWriter = XmlDictionaryWriter.CreateDictionaryWriter(escribir))
                {
                    serializer.WriteObject(dictionaryWriter, objeto);
                }
            }
        }

        /// <summary>
        /// Serializacion con XmlSerializer
        /// </summary>
        /// <param name="objeto"></param>
        /// <param name="nombreTag"></param>
        /// <param name="nombreNodoRoot"></param>
        /// <param name="nombreNodoRoot"></param>
        /// <param name="setting"></param>
        /// <param name="sb"></param>
        private static void SerializarXml(object objeto, string nombreTag, string nombreNodoRoot, XmlWriterSettings setting, StringBuilder sb)
        {
            XmlSerializer serializer;
            if (string.IsNullOrWhiteSpace(nombreNodoRoot))
            {
                serializer = new XmlSerializer(objeto.GetType());
            }
            else
            {
                serializer = new XmlSerializer(objeto.GetType(), new XmlRootAttribute(nombreTag));
            }

            XmlSerializerNamespaces xmlnsns = new XmlSerializerNamespaces();
            xmlnsns.Add(string.Empty, string.Empty);

            using (XmlWriter escribir = XmlWriter.Create(sb, setting))
            {
                serializer.Serialize(escribir, objeto, xmlnsns);
            }
        }

        /// <summary>
        /// Resolver Encoding con enum
        /// </summary>
        /// <param name="encodingEnum"></param>
        /// <returns></returns>
        private static Encoding ResolverEncoding(EncodingEnum encodingEnum)
        {
            switch (encodingEnum)
            {
                case EncodingEnum.Encode_ASCII: return Encoding.ASCII;
                case EncodingEnum.Encode_BigEndianUnicode: return Encoding.BigEndianUnicode;
                case EncodingEnum.Encode_UTF8: return Encoding.UTF8;
                case EncodingEnum.Encode_UTF32: return Encoding.UTF32;
                case EncodingEnum.Encode_Unicode: return Encoding.Unicode;
                default: return Encoding.Default;
            }
        }

        #endregion

        #region Metodos Zeus_Extractor
        /// <summary>
        /// Inicializa una nueva instancia de EjecutarOracleRefCursorAsync.
        /// </summary>
        protected async Task<DbDataReader> EjecutarOracleRefCursorAsync(string storedProcedure, int fechaProceso, long idTrabajo)
        {
            DbConnection conexion = basedatosConexion.CrearConexion();

            if (!(conexion is OracleConnection oracleConnection))
            {
                throw new InvalidOperationException("La conexión configurada no es Oracle.");
            }

            OracleCommand comando = new OracleCommand(storedProcedure, oracleConnection);

            comando.CommandType = CommandType.StoredProcedure;

            comando.Parameters.Add(new OracleParameter("FECHAPROCESO", OracleDbType.Int32)
            {
                Direction = ParameterDirection.Input,
                Value = fechaProceso
            });

            comando.Parameters.Add(new OracleParameter("ID_TRABAJO", OracleDbType.Int32)
            {
                Direction = ParameterDirection.Input,
                Value = idTrabajo
            });

            comando.Parameters.Add(new OracleParameter("result_set", OracleDbType.RefCursor)
            {
                Direction = ParameterDirection.Output
            });

            comando.Parameters.Add(new OracleParameter("IDENTIFICADORERROR", OracleDbType.Varchar2)
            {
                Direction = ParameterDirection.InputOutput,
                Size = 4000,
                Value = DBNull.Value
            });

            await oracleConnection.OpenAsync();

            DbDataReader reader = await comando.ExecuteReaderAsync(CommandBehavior.CloseConnection);

            object error = comando.Parameters["IDENTIFICADORERROR"].Value;

            if (error != null && error != DBNull.Value)
            {
                throw new Exception(error.ToString());
            }

            return reader;
        }

        /// <summary>
        /// Inicializa una nueva instancia de EjecutarOracleQueryAsync.
        /// </summary>
        protected async Task<DbDataReader> EjecutarOracleQueryAsync(string query)
        {
            DbConnection conexion = basedatosConexion.CrearConexion();

            DbCommand comando = basedatosConexion.CrearComando(query, conexion);

            comando.CommandType = CommandType.Text;

            await conexion.OpenAsync();

            return await comando.ExecuteReaderAsync(CommandBehavior.CloseConnection);
        }


        /// <summary>
        /// Obtiene la estructura de un DataReader en formato DataTable.
        /// Equivalente al método getMetaData del código legacy.
        /// </summary>
        /// <param name="reader">Reader del proveedor de datos.</param>
        /// <returns>DataTable con definición de columnas.</returns>
        protected DataTable ObtenerMetadataReader(DbDataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            return CrearEsquemaDataTable(reader.GetSchemaTable());
        }

        #endregion

        /// <summary>
        /// Valida y obtiene el puerto disponible del servicio.
        /// Compatibilidad legacy.
        /// </summary>
        [System.Obsolete("Compatibilidad legacy.", false)]
        public async Task<PuertoServicioDTO> ObtenerPuerto(string servicio)
        {
            PuertoServicioDTO puertoServicioDTO = null;
            List<PuertoServicioDTO> resultado = (await EjecutarStoredProcedureQuery<PuertoServicioDTO>(
                EnumNombreSp.SP_OBTENER_PUERTO,
                cmd => cmd.Parameters.Add(basedatosConexion.CrearParametro("@servicio", servicio))
            )).Data;

            foreach (PuertoServicioDTO pServicio in resultado)
            {
                bool procesoActivo = EstaActivoProceso(
                    pServicio.Pid,
                    pServicio.ProcessName,
                    pServicio.Puerto
                );

                if (!procesoActivo)
                    throw new ArgumentException("La aplicacion ya esta en Ejecucion.");

                if (PuertoDisponible(pServicio.Puerto))
                {
                    puertoServicioDTO = pServicio;
                }
            }

            return puertoServicioDTO;
        }

        /// <summary>
        /// Valida si un puerto esta ocupado.
        /// Compatibilidad legacy.
        /// </summary>
        [System.Obsolete("Compatibilidad legacy.", false)]
        public bool PuertoDisponible(int port)
        {
            try
            {
                TcpListener listener = new TcpListener(IPAddress.Parse("127.0.0.1"), port);
                listener.Start();
                listener.Stop();
                return true;
            }
            catch (SocketException ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("Error TCPListener:" + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Valida si el proceso está activo.
        /// Compatibilidad legacy.
        /// </summary>
        [System.Obsolete("Compatibilidad legacy.", false)]
        public bool EstaActivoProceso(object pid, object nombreProceso, object puerto)
        {
            try
            {
                int _procesoId = Convert.ToInt32(pid);
                string _nombreProceso = Convert.ToString(nombreProceso);

                Process loProceso = Process.GetProcessById(_procesoId);
                if (_nombreProceso == loProceso.ProcessName || loProceso.Responding)
                {
                    return true;
                }

                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                    $"El proceso esperado no corresponde. ProcessName esperado={_nombreProceso}, ProcessName en ejecución={loProceso.ProcessName}");
                throw new Exception(
                    $"El proceso esperado no corresponde. ProcessName esperado={_nombreProceso}, ProcessName en ejecución={loProceso.ProcessName}");
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("Error: " + ex.Message);
                throw new Exception($"Error: {ex}");
            }
        }

        /// <summary>
        /// Proceso para establecerse en la escucha de un puerto.
        /// Compatibilidad legacy.
        /// </summary>
        [System.Obsolete("Compatibilidad legacy.", false)]
        public async Task EscuchaPuerto(int puerto, string servicio, int pId, string nombreProceso,
            string displayName, Delegate funcionProcesar, bool enviarRespuesta)
        {
            bool escucharPuerto = true;
            bool enProceso = false;
            TcpListener listener = null;
            try
            {
                listener = new TcpListener(IPAddress.Parse("127.0.0.1"), puerto)
                {
                    ExclusiveAddressUse = false
                };
                listener.Start();

                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"[INFO] Escuchando puerto {puerto}");

                await EjecutarStoredProceduredNonQuery(EnumNombreSp.SP_ESCUCHA_PUERTO, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@pId", pId));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreProceso", nombreProceso));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@displayName", displayName));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@puerto", puerto));
                    cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
                });

                while (escucharPuerto)
                {
                    try
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"[INFO] Esperando petición en puerto {puerto}");
                        using (TcpClient cliente = await listener.AcceptTcpClientAsync())
                        using (NetworkStream stream = cliente.GetStream())
                        {
                            if (enProceso)
                            {
                                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("[INFO] Servicio ocupando proceso anterior");
                                if (enviarRespuesta)
                                {
                                    byte[] bytesOcupados = Encoding.ASCII.GetBytes(
                                        global::MUFG_T24.Comunes.Helpers.CryptoHelper.Encriptar("0", null));
                                    stream.Write(bytesOcupados, 0, bytesOcupados.Length);
                                }
                                continue;
                            }

                            byte[] buffer = new byte[1024];
                            StringBuilder mensajeCompleto = new StringBuilder();
                            int byteLeidos;

                            do
                            {
                                byteLeidos = await stream.ReadAsync(buffer, 0, buffer.Length);
                                mensajeCompleto.Append(Encoding.ASCII.GetString(buffer, 0, byteLeidos));
                            }
                            while (stream.DataAvailable);

                            string datoLeido = global::MUFG_T24.Comunes.Helpers.CryptoHelper.Desencriptar(
                                mensajeCompleto.ToString(), null);

                            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"[INFO] Petición recibida. Folio {datoLeido}");

                            enProceso = true;
                            bool resultado = Convert.ToBoolean(funcionProcesar.DynamicInvoke(datoLeido));
                            enProceso = false;

                            if (enviarRespuesta)
                            {
                                byte[] bytesOk = Encoding.ASCII.GetBytes(
                                    global::MUFG_T24.Comunes.Helpers.CryptoHelper.Encriptar(resultado ? "1" : "0", null));
                                stream.Write(bytesOk, 0, bytesOk.Length);
                            }
                        }
                    }
                    catch (ThreadAbortException)
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("[AVISO] Proceso abortado");
                        escucharPuerto = false;
                    }
                    catch (ThreadInterruptedException)
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("[AVISO] Proceso interrumpido");
                        escucharPuerto = false;
                    }
                    catch (Exception exCliente)
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"[ERROR] Error con socket cliente: {exCliente.Message}");
                        escucharPuerto = false;
                    }
                }
            }
            catch (Exception exServer)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"[ERROR] Error en socket server: {exServer.Message}, servicio:{servicio}");
            }
            finally
            {
                listener?.Stop();
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("[INFO] Termina la escucha del puerto");
            }
        }
    }
}

using System;
using System.Collections.Generic;

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Resultado de ejecutar un Stored Procedure
    /// </summary>
    public class StoredProcedureResult
    {
        public int? UltimoIdRecuperado { get; set; }
        public int FilasAfectadas { get; set; }
        public string MensajeError { get; set; }
        public Exception Exception { get; set; }
        public bool Exito {  get; set; }
        public bool? Afecto { get; set; }
    }

    /// <summary>
    /// Representa StoredProcedureResult.
    /// </summary>
    public class StoredProcedureResult<T>:StoredProcedureResult
    {
        public List<T> Data { get; set; }

        /// <summary>
        /// Inicializa una nueva instancia de StoredProcedureResult.
        /// </summary>
        public StoredProcedureResult()
        {
            Data = new List<T>();
        }
    }
}


using MUFG_T24.Comunes;
using MUFG_T24.Core.DTOs.T24;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace MUFG_T24.Repositorios.T24
{
    internal sealed class T24MwOfsTrx
    {
        private readonly IBasedatosConexion _basedatosConexion;
        private readonly IBasedatosConexion _basedatosConexionVistasT24;
        private readonly Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<OperacionesEntidad>>> _ejecutarOperaciones;
        private readonly Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<MwT24TrxMaestro>>> _ejecutarMaestro;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult<T24MwVersionConfigDto>>> _ejecutarVersionConfig;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult<T24MwVersionDefaultDto>>> _ejecutarVersionDefault;
        private readonly Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult>> _ejecutarNonQuery;
        private readonly Func<string, Task<OperacionesEntidad>> _existeRegistro;
        private readonly Func<string, int, char, string, Task<StoredProcedureResult>> _insertaRegistro;
        private readonly Func<string, string, char> _obtenerEntradaSalidaDesdeFolio;

        public T24MwOfsTrx(
            IBasedatosConexion basedatosConexion,
            IBasedatosConexion basedatosConexionVistasT24,
            Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<OperacionesEntidad>>> ejecutarOperaciones,
            Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<MwT24TrxMaestro>>> ejecutarMaestro,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult<T24MwVersionConfigDto>>> ejecutarVersionConfig,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult<T24MwVersionDefaultDto>>> ejecutarVersionDefault,
            Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult>> ejecutarNonQuery,
            Func<string, Task<OperacionesEntidad>> existeRegistro,
            Func<string, int, char, string, Task<StoredProcedureResult>> insertaRegistro,
            Func<string, string, char> obtenerEntradaSalidaDesdeFolio)
        {
            _basedatosConexion = basedatosConexion;
            _basedatosConexionVistasT24 = basedatosConexionVistasT24;
            _ejecutarOperaciones = ejecutarOperaciones;
            _ejecutarMaestro = ejecutarMaestro;
            _ejecutarVersionConfig = ejecutarVersionConfig;
            _ejecutarVersionDefault = ejecutarVersionDefault;
            _ejecutarNonQuery = ejecutarNonQuery;
            _existeRegistro = existeRegistro;
            _insertaRegistro = insertaRegistro;
            _obtenerEntradaSalidaDesdeFolio = obtenerEntradaSalidaDesdeFolio;
        }

        public async Task<bool> RegistraTrx(string folioUnico, int fechaJulianaT24, string tabla, string tipoOperacion, string folio, string operacionId,
            MwT24TrxDetalle trxDetalle, MwT24TrxMaestro trxMaestro)
        {
            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.registra_TRX|Cuenta: '{trxDetalle.SDebit_acct_no}'.\n" +
                $"INFO|Comunes.registra_TRX|Cuenta BEN: '{trxDetalle.SCredit_acct_no}'.\n" +
                $"INFO|Comunes.registra_TRX|Importe: '{trxDetalle.SCredit_acct_no}'.\n" +
                "INFO|Comunes.registra_TRX|A registrar operación.");

            OperacionesEntidad registroEncontrado = await _existeRegistro(folioUnico);
            if (registroEncontrado != null && registroEncontrado.ExitoResultado)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.registra_TRX|La operación ya fue registra en Middleware-T24 con estatus=" +
                    $"'{registroEncontrado.Estatus}', T24_folio_asignado='{registroEncontrado.T24_folio_asignado}'.");

                if (registroEncontrado.Estatus != EstatusOperacionesIntercambioT24.PEND.ToString() &&
                    registroEncontrado.Estatus != EstatusOperacionesIntercambioT24.ERROR.ToString())
                {
                    return false;
                }
            }
            else
            {
                StoredProcedureResult resultadoInsercion = await _insertaRegistro(
                    folioUnico,
                    fechaJulianaT24,
                    _obtenerEntradaSalidaDesdeFolio(folioUnico, nameof(RegistraTrx)),
                    tipoOperacion);

                if (resultadoInsercion == null || !resultadoInsercion.Exito)
                {
                    string detalle = resultadoInsercion?.MensajeError ?? resultadoInsercion?.Exception?.Message ?? "Sin detalle";
                    throw new InvalidOperationException($"No fue posible registrar la operación en Middleware-T24. FolioUnico='{folioUnico}'. Detalle: {detalle}");
                }
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|Comunes.registra_TRX|A buscar comisiones.");

            trxMaestro = await BuscarEsquemaTrx(trxMaestro);

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.registra_TRX|Finalmente, el esquema de comisión será: Transaction Type='{trxMaestro.STrxTransactionType} '," +
                $" esquema='{trxMaestro.STrxInternalType}'.");

            registroEncontrado = await _existeRegistro(folioUnico);

            if (registroEncontrado == null || !registroEncontrado.ExitoResultado || string.IsNullOrWhiteSpace(registroEncontrado.T24_folio_propuesto))
            {
                throw new InvalidOperationException($"No se pudo recuperar un registro válido para OFS. FolioUnico='{folioUnico}'.");
            }

            trxDetalle.SFundsTransferID = registroEncontrado.T24_folio_propuesto;
            trxDetalle.STransaction_type = trxMaestro.STrxTransactionType;

            if (!string.IsNullOrEmpty(trxDetalle.SInstructions))
            {
                trxDetalle.SInstructions = trxDetalle.SInstructions.ToUpper().Replace("$FOLIO$", registroEncontrado.T24_folio_propuesto);
                trxDetalle.SInstructions = ConvierteTextoAMultivalor(trxDetalle.SInstructions.ToUpper(), 65);
            }

            if (!string.IsNullOrEmpty(trxDetalle.SBeneficiary))
                trxDetalle.SBeneficiary = ConvierteTextoAMultivalor(trxDetalle.SBeneficiary.ToUpper(), 65);

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|Comunes.registra_TRX|A generar el OFS.");

            trxDetalle.ID = trxDetalle.SFundsTransferID;
            string ofsEntrada = await GeneraOfs("INPUT", tabla, folio, trxDetalle, trxMaestro);

            if (string.IsNullOrEmpty(ofsEntrada))
                throw new InvalidOperationException($"Error al generar el OFS de la operación '{trxDetalle.SFundsTransferID}'");

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.registra_TRX|OFS='{ofsEntrada}'.");
            try
            {
                StoredProcedureResult resultadoSp = await _ejecutarNonQuery(
                    EnumNombreSp.SP_T24_ADD_TRX, cmd =>
                    {
                        cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@folio", trxDetalle.SFundsTransferID, 25));
                        cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@trans_source", tipoOperacion, 15));
                        cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@trans_id", folio, 25));
                        cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@folio_unico", folioUnico, 65));
                        cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@ofs", ofsEntrada));
                        cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@ofs_type", ofsEntrada, 20));
                    }, true);

                if (!resultadoSp.Exito)
                {
                    string mensajeSp = resultadoSp.MensajeError;

                    if (!string.IsNullOrWhiteSpace(mensajeSp) && mensajeSp.StartsWith("Error SQL:", StringComparison.OrdinalIgnoreCase))
                    {
                        string[] partes = mensajeSp.Split(new[] { ' ' }, 3, StringSplitOptions.RemoveEmptyEntries);
                        if (partes.Length == 3)
                            mensajeSp = partes[2];
                    }

                    if (string.IsNullOrWhiteSpace(mensajeSp))
                        mensajeSp = $"SP_T24_ADD_TRX devolvió resultado no exitoso para ID={trxDetalle.SFundsTransferID}.";

                    throw new InvalidOperationException(mensajeSp);
                }

                return true;
            }
            catch (Exception ex)
            {
                string mensajeOperacionPrevia = $"Operacion enviada previamente a T24, ID={trxDetalle.SFundsTransferID}.";

                if (string.Equals(ex.Message, mensajeOperacionPrevia, StringComparison.Ordinal))
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"AVISO|Comunes.registra_TRX|{ex.Message}");
                    return true;
                }

                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Comunes.registra_TRX|{ConstantesClaveAviso.Error}= {ex}");
                throw;
            }
        }

        public async Task<string> GeneraOfs(string tipoOfs, string tablaTrabajo, string folio, MwT24TrxDetalle trxDetalle, MwT24TrxMaestro trxMaestro)
        {
            bool respuesta = false;
            string ofs = string.Empty;

            try
            {
                if (trxMaestro == null)
                    throw new ArgumentException(nameof(trxMaestro));

                if (trxDetalle == null)
                    throw new ArgumentException(nameof(trxDetalle));

                string ofsHeader = string.Empty;
                string ofsBody = string.Empty;
                string ofsRecId = string.Empty;
                string referenciaCampo = string.Empty;

                StoredProcedureResult<T24MwVersionConfigDto> resultadoVersion =
                    await _ejecutarVersionConfig(
                        EnumNombreSp.SP_GET_VERSION_CONFIG,
                        cmd =>
                        {
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tran_system_id", trxMaestro.STrxSystemID, 10));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tran_operation_id", trxMaestro.STrxOperationID, 10));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tran_internal_type", trxMaestro.STrxInternalType, 30));
                        });

                T24MwVersionConfigDto versionConfig = resultadoVersion?.Data?.FirstOrDefault();

                if (versionConfig == null)
                {
                    throw new InvalidOperationException(
                        $"Sin parametrización en tabla de versiones: " +
                        $"system_id='{trxMaestro.STrxSystemID}', " +
                        $"operation_id='{trxMaestro.STrxOperationID}', " +
                        $"internal_type='{trxMaestro.STrxInternalType}'");
                }

                string aplicacion = (versionConfig.TRAN_T24_APPLICATION ?? string.Empty).Trim().ToUpper();
                string version = (versionConfig.TRAN_T24_VERSION ?? string.Empty).Trim().ToUpper();
                referenciaCampo = (versionConfig.TRAN_T24_REFERENCE_FLD ?? string.Empty).Trim().ToUpper();

                ofsHeader = aplicacion;

                if (!string.IsNullOrEmpty(version))
                {
                    ofsHeader += "," + version;
                }
                else
                {
                    ofsHeader += ",";
                }

                switch (tipoOfs)
                {
                    case "INPUT":
                        ofsHeader += "/I/PROCESS";
                        break;
                    case "AUTHOR":
                        ofsHeader += "/A";
                        break;
                }

                ofsHeader += ",";
                ofs = ofsHeader;

                DataTable tbDetalleFt = trxDetalle.ConvertirDetalleADataTable();
                if (tbDetalleFt == null || tbDetalleFt.Rows.Count == 0)
                    throw new InvalidOperationException("Error construyendo mensaje OFS. El detalle de transacción no contiene registros.");

                DataRow fila = tbDetalleFt.Rows[0];

                StoredProcedureResult<T24MwVersionDefaultDto> resultadoDefaults =
                    await _ejecutarVersionDefault(
                        EnumNombreSp.SP_GET_DEFAULTS_CONFIG,
                        cmd =>
                        {
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tran_system_id", trxMaestro.STrxSystemID, 10));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tran_operation_id", trxMaestro.STrxOperationID, 10));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tran_internal_type", trxMaestro.STrxInternalType, 30));
                        });

                if (resultadoDefaults == null || resultadoDefaults.Data == null || !resultadoDefaults.Data.Any())
                {
                    throw new InvalidOperationException(
                        $"Sin parametrización en tabla de version_default: " +
                        $"system_id='{trxMaestro.STrxSystemID}', " +
                        $"operation_id='{trxMaestro.STrxOperationID}', " +
                        $"internal_type='{trxMaestro.STrxInternalType}'");
                }

                int contador = 0;
                foreach (T24MwVersionDefaultDto item in resultadoDefaults.Data)
                {
                    contador++;

                    string campo = (item.TRAN_VERSION_FIELD ?? string.Empty).Trim().ToUpper();
                    string nombreClase = (item.TRAN_SYS_CLASS_PROPERTY ?? string.Empty).Trim().ToUpper();
                    string defaultTipo = (item.TRAN_IS_DEFAULT ?? string.Empty).Trim().ToUpper();
                    string defaultValor = item.TRAN_DEFAULT_VALUE ?? string.Empty;
                    string versionField = item.TRAN_VERSION_FIELD ?? string.Empty;

                    string valor;
                    try
                    {
                        if (defaultTipo == "X")
                            continue;

                        if (defaultTipo == "F")
                        {
                            valor = defaultValor;
                        }
                        else
                        {
                            string nombreColumnaDetalle = ObtenerNombreColumnaDataTable(tbDetalleFt, nombreClase);

                            if (string.IsNullOrEmpty(nombreColumnaDetalle))
                            {
                                string columnasDisponibles = string.Join(", ", tbDetalleFt.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
                                throw new FormatException($"Campo {nombreClase} no fue encontrado en DataTable. Columnas disponibles: {columnasDisponibles}");
                            }

                            object valorFila = fila[nombreColumnaDetalle];

                            if (defaultTipo == "R")
                            {
                                if (EsNuloOVacio(valorFila))
                                    throw new ArgumentException($"Campo requerido por T24 '{versionField}' no puede estar vacio. Columna {nombreClase}");

                                valor = ObtenerValorOfs(tbDetalleFt, fila, nombreColumnaDetalle);
                            }
                            else if (defaultTipo == "S")
                            {
                                if (EsNuloOVacio(valorFila))
                                    valor = defaultValor;
                                else
                                    valor = ObtenerValorOfs(tbDetalleFt, fila, nombreColumnaDetalle);
                            }
                            else
                            {
                                if (EsNuloOVacio(valorFila))
                                    continue;

                                valor = ObtenerValorOfs(tbDetalleFt, fila, nombreColumnaDetalle);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Error en detalle de transacción. Campo {nombreClase} no fue encontrado en DataTable. {tbDetalleFt.TableName}| DatableName Columns {tbDetalleFt.Columns.Count} | Error {ex.Message}");
                    }

                    if (campo.Equals("@ID"))
                    {
                        ofsRecId = OfsEspecialChar(valor);
                    }
                    else
                    {
                        bool esMultivalor = ObtenerBooleano(item.TRAN_MULTIVALUE);
                        if (esMultivalor)
                        {
                            string[] multiValores = valor.Split('|');
                            for (int i = 0; i < multiValores.Length; i++)
                            {
                                string campoMultiValor = campo + ":" + (i + 1) + ":1";
                                if (contador > 1 && ofsBody.Length > 0)
                                    campoMultiValor = "," + campoMultiValor;

                                ofsBody = ofsBody + campoMultiValor + "=" + OfsEspecialChar(multiValores[i]);
                            }
                        }
                        else
                        {
                            if (valor != "NO INCLUIR EN OFS")
                            {
                                if (contador > 1 && ofsBody.Length > 0)
                                    campo = "," + campo;

                                ofsBody = ofsBody + campo + "=" + OfsEspecialChar(valor);
                            }
                        }
                    }

                    if (nombreClase.Equals(referenciaCampo))
                    {
                        // valor capturado para mantener equivalencia funcional de flujo legacy
                    }
                }

                if (string.IsNullOrEmpty(ofsBody) && string.IsNullOrEmpty(ofsRecId))
                    throw new InvalidOperationException("Error construyendo mensaje OFS.");

                ofs += "," + ofsRecId + (ofsBody.Length > 0 ? "," + ofsBody : string.Empty);

                StoredProcedureResult resultadoInsercion =
                    await _ejecutarNonQuery(
                        EnumNombreSp.SP_OFS_TOMBIN_NF_ENTRADAS,
                        cmd =>
                        {
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tablaTrabajo", tablaTrabajo, 128));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tipoOfs", tipoOfs, 128));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@ofs", ofs));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio", folio, 50));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
                        }, false);

                respuesta = resultadoInsercion.Exito;
                return respuesta ? ofs : string.Empty;
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|T24_MW_Connection.generaOFS|Error: {ex}");
                throw;
            }
            finally
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|T24_MW_Connection.generaOFS|Se insertó el OFS {tipoOfs} en la tabla de trabajo. OFS = {respuesta}");
            }
        }

        public string OfsEspecialChar(string texto)
        {
            if (string.IsNullOrEmpty(texto))
                return string.Empty;

            return texto
                .Replace(",", "?")
                .Replace("\"", "|")
                .Replace("/", "^");
        }

        public string ConvierteTextoAMultivalor(string texto, int longitudSeparadorMultivalor)
        {
            if (string.IsNullOrWhiteSpace(texto))
                return string.Empty;

            texto = texto.Trim();
            while (texto.StartsWith("-"))
            {
                texto = texto.Substring(1).Trim();
            }

            int posicionActual = 0;
            int largoTotal = texto.Length;
            List<string> resultado = new List<string>();

            while (posicionActual < largoTotal)
            {
                int posicionFin = posicionActual + longitudSeparadorMultivalor;
                if (posicionFin >= largoTotal)
                {
                    resultado.Add(texto.Substring(posicionActual));
                    break;
                }

                int ajusteMaximo = Math.Min(10, largoTotal - posicionFin);
                for (int i = 0; i < ajusteMaximo; i++)
                {
                    char caracterSiguiente = texto[posicionFin];
                    char caracterAnterior = texto[posicionFin - 1];

                    if (caracterSiguiente == ' ' || caracterSiguiente == '-' || caracterAnterior == ' ')
                        posicionFin--;
                    else
                        break;
                }

                int longitudReal = posicionFin - posicionActual;
                if (longitudReal <= 0)
                    break;

                resultado.Add(texto.Substring(posicionActual, longitudReal));
                posicionActual = posicionFin;
            }

            return string.Join(EnumParametrosGenerales.SEPARADORMULTIVALOR, resultado);
        }

        public string FechaT24(DateTime fechaValor)
        {
            return fechaValor.ToString(EnumParametrosGenerales.FECHA_BD.ToString(), CultureInfo.InvariantCulture);
        }

        public async Task<MwT24TrxMaestro> BuscarEsquemaTrx(MwT24TrxMaestro trxMaestro)
        {
            try
            {
                string esquema = string.Empty;
                int totalRegistrosEsquema = 0;

                if (trxMaestro == null)
                    throw new ArgumentException(nameof(trxMaestro));

                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.busca_esquema_TRX|A buscar esquema de comisiones para: Origen='{trxMaestro.STrxSystemID}', Operacion='{trxMaestro.STrxOperationSpecificType}', Subtipo='{trxMaestro.STrxOperationSubType}'.");

                StoredProcedureResult<MwT24TrxMaestro> resultadoSp =
                    await _ejecutarMaestro(
                        EnumNombreSp.SP_ESQUEMA_TRX_T24,
                        cmd =>
                        {
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@trx_origen", trxMaestro.STrxSystemID, 30));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@trx_operacion", trxMaestro.STrxOperationSpecificType, 30));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@trx_sub_operacion", trxMaestro.STrxOperationSubType, 30));
                        }, false);

                MwT24TrxMaestro esquemaComisiones = resultadoSp.Data.FirstOrDefault();
                if (!resultadoSp.Exito || esquemaComisiones == null)
                    throw new InvalidOperationException($"No hay definición de esquemas para trx_origen='{trxMaestro.STrxSystemID}', trx_operacion='{trxMaestro.STrxOperationSpecificType}', trx_sub_operacion='{trxMaestro.STrxOperationSubType}'.");

                trxMaestro.STrxOperationID = esquemaComisiones.STrxOperationID;
                trxMaestro.STrxInternalType = esquemaComisiones.STrxInternalType;
                trxMaestro.STrxTransactionType = esquemaComisiones.STrxTransactionType;

                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.busca_esquema_TRX|TrxOperation_Specific_Type='{trxMaestro.STrxOperationSpecificType}', TrxOperation_SubType='{trxMaestro.STrxOperationSubType}'. Encontro, TrxOperation_ID='{trxMaestro.STrxOperationID}', TrxInternal_Type='{trxMaestro.STrxInternalType}', trx_type='{trxMaestro.STrxTransactionType}'.");

                if (trxMaestro.STrxInternalType == EnumParametrosGenerales.ESQUEMA)
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.busca_esquema_TRX|Inicia busqueda de esquema de comisión de cliente para la cuenta '{trxMaestro.STrxOperationAcct}'.");

                    string xmlNegocio = new XElement("DATA",
                        new XElement("COMSCHE",
                            new XElement("ACCOUNT_NUMBER", trxMaestro.STrxOperationAcct ?? string.Empty),
                            new XElement("TRANSACTION_TYPE", trxMaestro.STrxTransactionType ?? string.Empty)))
                        .ToString(SaveOptions.DisableFormatting);

                    try
                    {
                        StoredProcedureResult<OperacionesEntidad> resultadoEsquemaSp = await _ejecutarOperaciones(
                            EnumNombreSp.SP_NET_CUENTA_TRX_T24,
                            cmd =>
                            {
                                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@doc", xmlNegocio));
                                cmd.Parameters.Add(_basedatosConexion.CrearParametroSalida("@recordCount", DbType.Int32));
                                cmd.Parameters.Add(_basedatosConexion.CrearParametroSalida("@errorCode", DbType.Int32));
                                cmd.Parameters.Add(_basedatosConexion.CrearParametroSalida("@errorDesc", DbType.String, 4000));
                            }, true);

                        if (resultadoEsquemaSp.Data.Count > 0)
                        {
                            OperacionesEntidad esquemaCliente = resultadoEsquemaSp.Data.FirstOrDefault();
                            esquema = esquemaCliente?.CUST_SCHEME;

                            if (!string.IsNullOrWhiteSpace(esquema))
                                totalRegistrosEsquema = 1;
                            else
                                totalRegistrosEsquema = 0;
                        }
                        else
                        {
                            totalRegistrosEsquema = 0;
                        }
                    }
                    catch (Exception ex)
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Comunes.busca_esquema_TRX|Error en lectura de esquema de comisiones x cliente.");
                        throw new InvalidOperationException($"Error en lectura de esquema de comisiones x cliente - {ex}");
                    }

                    if (totalRegistrosEsquema == 0)
                    {
                        trxMaestro.STrxInternalType = EnumParametrosGenerales.SINESQUEMA;
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.busca_esquema_TRX|Cambio el esquema de comisiones por no existir parametrización del cliente. trxOperation_ID='{trxMaestro.STrxOperationID}', trxInternal_Type='{trxMaestro.STrxInternalType}'.");

                        StoredProcedureResult<MwT24TrxMaestro> resultadoSpCom =
                            await _ejecutarMaestro(
                                EnumNombreSp.SP_SIN_ESQUEMA_TRX_T24,
                                cmd =>
                                {
                                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@trx_origen", trxMaestro.STrxSystemID));
                                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@trx_operacion", trxMaestro.STrxOperationSpecificType));
                                }, false);

                        MwT24TrxMaestro resultadoSinEsquema = (resultadoSpCom?.Data?.FirstOrDefault())
                            ?? throw new InvalidOperationException($"No hay definición de esquemas para trx_origen='{trxMaestro.STrxSystemID}', trx_operacion='{trxMaestro.STrxOperationSpecificType}, trx_sub_operacion='{trxMaestro.STrxOperationSubType}'.");

                        trxMaestro.STrxInternalType = resultadoSinEsquema.STrxInternalType;
                        trxMaestro.STrxOperationID = resultadoSinEsquema.STrxOperationID;
                        trxMaestro.STrxTransactionType = resultadoSinEsquema.STrxTransactionType;
                    }

                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.busca_esquema_TRX|Esquema Encontrado = {esquema}.");
                }

                return trxMaestro;
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("Error busqueda esquema" + ex.ToString());
                throw;
            }
        }

        private static bool EsNuloOVacio(object valor)
        {
            return valor == null || valor == DBNull.Value || string.IsNullOrEmpty(valor.ToString());
        }

        private string ObtenerValorOfs(DataTable tabla, DataRow fila, string nombreColumna)
        {
            object valor = fila[nombreColumna];

            if (valor == null || valor == DBNull.Value)
            {
                return string.Empty;
            }

            if (tabla.Columns[nombreColumna].DataType == typeof(DateTime))
            {
                return FechaT24((DateTime)valor);
            }

            return valor.ToString();
        }

        private static string ObtenerNombreColumnaDataTable(DataTable tabla, string nombreColumna)
        {
            if (tabla == null || string.IsNullOrEmpty(nombreColumna))
                return null;

            string nombreBuscado = nombreColumna.Trim();

            DataColumn columnaExacta = tabla.Columns
                .Cast<DataColumn>()
                .FirstOrDefault(c => string.Equals(c.ColumnName, nombreBuscado, StringComparison.OrdinalIgnoreCase));

            if (columnaExacta != null)
                return columnaExacta.ColumnName;

            string nombreNormalizado = NormalizarNombreColumna(nombreBuscado);

            DataColumn columnaNormalizada = tabla.Columns
                .Cast<DataColumn>()
                .FirstOrDefault(c => NormalizarNombreColumna(c.ColumnName) == nombreNormalizado);

            if (columnaNormalizada != null)
                return columnaNormalizada.ColumnName;

            string nombreConPrefijoS = "S" + nombreNormalizado;

            DataColumn columnaConPrefijoS = tabla.Columns
                .Cast<DataColumn>()
                .FirstOrDefault(c => NormalizarNombreColumna(c.ColumnName) == nombreConPrefijoS);

            if (columnaConPrefijoS != null)
                return columnaConPrefijoS.ColumnName;

            string alias = ObtenerAliasColumnaDetalle(nombreBuscado);

            if (!string.IsNullOrEmpty(alias))
            {
                DataColumn columnaAlias = tabla.Columns
                    .Cast<DataColumn>()
                    .FirstOrDefault(c => string.Equals(c.ColumnName, alias, StringComparison.OrdinalIgnoreCase));

                if (columnaAlias != null)
                    return columnaAlias.ColumnName;

                string aliasNormalizado = NormalizarNombreColumna(alias);

                DataColumn columnaAliasNormalizada = tabla.Columns
                    .Cast<DataColumn>()
                    .FirstOrDefault(c => NormalizarNombreColumna(c.ColumnName) == aliasNormalizado);

                if (columnaAliasNormalizada != null)
                    return columnaAliasNormalizada.ColumnName;
            }

            return null;
        }

        private static bool ObtenerBooleano(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return false;

            if (valor is bool boolValue)
                return boolValue;

            string texto = valor.ToString().Trim();

            if (texto == "1")
                return true;

            if (texto == "0")
                return false;

            bool resultado;
            return bool.TryParse(texto, out resultado) && resultado;
        }

        private static string NormalizarNombreColumna(string nombre)
        {
            if (string.IsNullOrEmpty(nombre))
                return string.Empty;

            return nombre
                .Replace("_", string.Empty)
                .Replace(".", string.Empty)
                .Replace("-", string.Empty)
                .Replace(" ", string.Empty)
                .Trim()
                .ToUpper();
        }

        private static string ObtenerAliasColumnaDetalle(string nombreColumna)
        {
            if (string.IsNullOrEmpty(nombreColumna))
                return null;

            switch (nombreColumna.Trim().ToUpper())
            {
                case "ID":
                    return "ID";
                case "BENEFICIARY_NAME":
                case "BENEFICIARY.NAM":
                case "BENEFICIARY_NAM":
                case "BENEFICIARY NAM":
                    return "SBeneficiary";
                default:
                    return null;
            }
        }
    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace MUFG_T24.Repositorios.T24
{
    internal sealed class T24MwOperaciones
    {
        private readonly IBasedatosConexion _basedatosConexion;
        private readonly Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<OperacionesEntidad>>> _ejecutarOperaciones;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult<MW_Tipo_operacion>>> _ejecutarMwTipoOperacion;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult>> _ejecutarNonQuery;

        public T24MwOperaciones(
            IBasedatosConexion basedatosConexion,
            Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<OperacionesEntidad>>> ejecutarOperaciones,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult<MW_Tipo_operacion>>> ejecutarMwTipoOperacion,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult>> ejecutarNonQuery)
        {
            _basedatosConexion = basedatosConexion;
            _ejecutarOperaciones = ejecutarOperaciones;
            _ejecutarMwTipoOperacion = ejecutarMwTipoOperacion;
            _ejecutarNonQuery = ejecutarNonQuery;
        }

        public static string CambiarFechaAJulianaT24(string textoFecha)
        {
            if (string.IsNullOrWhiteSpace(textoFecha) || textoFecha.Length != 8)
                throw new ArgumentException($"No se puede convertir a una fecha juliana la fecha '{textoFecha}'");

            if (!DateTime.TryParseExact(
                    textoFecha,
                    "yyyyMMdd",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out DateTime fecha))
            {
                throw new ArgumentException($"Fecha invalida: {textoFecha}");
            }

            return fecha.ToString("yy") + fecha.DayOfYear.ToString("000");
        }

        public async Task<Dictionary<string, OperacionesEntidad>> ObtieneOperacionesMwT24()
        {
            List<OperacionesEntidad> resultado = (await _ejecutarOperaciones(EnumNombreSp.SP_T24_OPERACIONES, null, false)).Data;
            resultado.ForEach(x => x.Tabla_trabajo_maestro = BaseDatosHelper.ObtieneTexto(x.Tabla_trabajo_maestro));

            Dictionary<string, OperacionesEntidad> dicOperaciones = new Dictionary<string, OperacionesEntidad>();
            foreach (OperacionesEntidad operacion in resultado.Where(o => !string.IsNullOrEmpty(o.Tipo_operacion)))
            {
                if (dicOperaciones.ContainsKey(operacion.Tipo_operacion))
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"WARN|OnStart|La operación '{operacion.Tipo_operacion}' está duplicada en el catálogo de Operaciones MW-T24. Se ignora el registro repetido.");
                    continue;
                }

                dicOperaciones.Add(operacion.Tipo_operacion, operacion);
            }

            if (dicOperaciones.Count <= 0)
                throw new Exception("No existen registros en el catálogo de Operaciones MW-T24");

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|OnStart|Se cargaron {dicOperaciones.Count} registros del catálogo de Operaciones MW-T24 correctamente.");
            return dicOperaciones;
        }

        public async Task<List<OperacionesEntidad>> ObtieneDatosCuentasFad()
        {
            List<OperacionesEntidad> resultado = (await _ejecutarOperaciones(EnumNombreSp.SP_T24_CUENTAS_FAD, null, false)).Data;
            if (resultado.Count <= 0)
                throw new Exception("No existen registros en el catálogo de Cuentas FAD MW-T24 correctamente.");

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|OnStart|Se cargaron {resultado.Count} registros del catálogo de Operaciones MW-T24 correctamente.");
            return resultado;
        }

        public async Task<List<OperacionesEntidad>> ObtieneDatosCuentasNostro()
        {
            List<OperacionesEntidad> resultado = (await _ejecutarOperaciones(EnumNombreSp.SP_T24_CUENTAS_NOSTRO, null, false)).Data;
            if (resultado.Count <= 0)
                throw new Exception("No existen registros en el catálogo de Cuentas NOSTRO MW-T24");

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|OnStart|Se cargaron {resultado.Count} registros del catálogo de Cuentas NOSTRO MW-T24 correctamente.");
            return resultado;
        }

        public async Task<List<OperacionesEntidad>> ObtieneDatosSwfOutDestino()
        {
            List<OperacionesEntidad> resultado = (await _ejecutarOperaciones(EnumNombreSp.SP_T24_CUENTAS_SWF_SALIDA_DESTINO, null, false)).Data;
            if (resultado.Count <= 0)
                throw new Exception("No existen registros en el catálogo de Swift Out destinos MW-T24");

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|OnStart|Se cargaron {resultado.Count} registros del catálogo de Swift Out destinos MW-T24 correctamente.");
            return resultado;
        }

        public async Task<string> GeneraFolioUnico(string tipoOperacion, string fechaJuliana, string folio, int longitudFoli = 8)
        {
            Dictionary<string, OperacionesEntidad> operacionesT24 = await ObtieneOperacionesMwT24();
            if (!operacionesT24.TryGetValue(tipoOperacion, out OperacionesEntidad tipoOp))
                throw new KeyNotFoundException($"No existe el tipo de operacion: {tipoOperacion}");

            return $"{tipoOp.Prefijo_folio_unico}{fechaJuliana}{folio.PadLeft(longitudFoli, '0')}";
        }

        public async Task<string> GeneraFolioUnico(string origen, string tipo, string tipoOperacion, string fechaJuliana, string folio, int longitudFoli = 16)
        {
            Dictionary<string, OperacionesEntidad> operacionesT24 = await ObtieneOperacionesMwT24();
            if (!operacionesT24.TryGetValue(tipoOperacion, out OperacionesEntidad tipoOp))
                throw new KeyNotFoundException($"No existe el tipo de operacion: {tipoOperacion}");

            return $"{origen}{tipo}{tipoOp.Prefijo_folio_unico}{fechaJuliana}{folio.PadLeft(longitudFoli, '0')}";
        }

        public string CambiaJulianaAFecha(string fechaJuliana)
        {
            if (string.IsNullOrWhiteSpace(fechaJuliana))
                throw new ArgumentException("La fecha juliana esta vacia");

            if (fechaJuliana.Length == 5)
                fechaJuliana = EnumParametrosGenerales.SIGLO_20 + fechaJuliana;

            if (fechaJuliana.Length != 7)
                throw new ArgumentException($"Formato juliana invalido: {fechaJuliana}");

            if (!int.TryParse(fechaJuliana.Substring(0, 4), out int ano) || !int.TryParse(fechaJuliana.Substring(4, 3), out int diaAno))
                throw new ArgumentException($"Formato juliana invalido: {fechaJuliana}");

            DateTime fecha = new DateTime(ano, 1, 1).AddDays(diaAno - 1);
            return fecha.ToString(EnumParametrosGenerales.FECHA_BD);
        }

        public string ValidaMoneda(string moneda)
        {
            if (string.Equals(moneda, Moneda.MXP.ToString()) || string.Equals(moneda, Moneda.MXN.ToString()))
                return Moneda.MXN.ToString();
            return moneda;
        }

        public string DameCuentaMufg(string cuenta)
        {
            if (string.IsNullOrEmpty(cuenta))
                return cuenta;

            if (cuenta.Length == 18 && cuenta.StartsWith(EnumParametrosGenerales.PREFIJO_CUENTA_MUFG))
                return cuenta.Substring(10, 7);

            return cuenta;
        }

        public async Task<string> ObtenerFechaJuliana()
        {
            bool esConexionVistasT24 = true;
            return (await _ejecutarOperaciones(EnumNombreSp.SP_BUSCA_FECHA_T24, null, esConexionVistasT24))
                .Data
                .Select(x => x.Julian_date)
                .FirstOrDefault();
        }

        public async Task<string> ObtenerFolioAsignado(string folioUnico)
        {
            OperacionesEntidad resultadoOp = await ExisteRegistro(folioUnico);
            if (resultadoOp != null && resultadoOp.ExitoResultado)
                return resultadoOp.T24_folio_asignado;

            return null;
        }

        public async Task<OperacionesEntidad> ExisteRegistro(string folioUnico)
        {
            try
            {
                OperacionesEntidad registroEncontrado = (await _ejecutarOperaciones(
                    EnumNombreSp.SP_BUSCA_FOLIO_INTERCAMBIO_T24,
                    cmd =>
                    {
                        cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folioUnico", folioUnico, 65));
                    }, false)).Data.FirstOrDefault();

                if (registroEncontrado != null)
                {
                    registroEncontrado.ExitoResultado = true;
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.MUFG_intercambio_T24.existe_registro|La operación fue encontrada en la tabla de transacción entre Middleware - T24 con estatus='{registroEncontrado.Estatus}', folio_T24_propuesto='{registroEncontrado.T24_folio_propuesto}', folio_T24_asignado='{registroEncontrado.T24_folio_asignado}'.");
                }
                else
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|Comunes.MUFG_intercambio_T24.existe_registro|La operación no fue encontrada en la tabla de registro.");
                }

                return registroEncontrado;
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Comunes.MUFG_intercambio_T24.existe_registro|Ocurrió excepcion: {ex}");
                throw;
            }
        }

        public async Task<string> GeneraFolioPresupuesto(string folioUnico, int fechaJulianaT24, string tipoOperacion)
        {
            OperacionesEntidad registroEncontrado = await ExisteRegistro(folioUnico);
            if (registroEncontrado != null && registroEncontrado.ExitoResultado)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.genera_folioPropuesto|La operación ya fue registra en Middleware - T24 con estatus = '{registroEncontrado.Estatus}', T24_folio_asignado = '{registroEncontrado.T24_folio_asignado}'.");

                if (registroEncontrado.Estatus != EstatusOperacionesIntercambioT24.PEND.ToString()
                    && registroEncontrado.Estatus != EstatusOperacionesIntercambioT24.ERROR.ToString())
                    throw new Exception($"Se desea generar un nuevo folio para una operación previamente realizada, folio_unico='{registroEncontrado.Folio_unico}'");

                return registroEncontrado.T24_folio_propuesto;
            }

            return await InsertaRegistro(folioUnico, fechaJulianaT24, ObtenerEntradaSalidaDesdeFolio(folioUnico, nameof(GeneraFolioPresupuesto)), tipoOperacion);
        }

        public char ObtenerEntradaSalidaDesdeFolio(string folioUnico, string contexto)
        {
            if (string.IsNullOrEmpty(folioUnico))
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|{contexto}|No fue posible obtener entrada/salida porque folioUnico viene nulo o vacío.");
                return default(char);
            }

            return folioUnico[0];
        }

        public async Task<string> InsertaRegistro(string folioUnico, int fechaJulianaT24, char entradaSalida, string tipoOperacion)
        {
            try
            {
                OperacionesEntidad registroEncontrado = await ExisteRegistro(folioUnico);
                if (registroEncontrado != null && registroEncontrado.ExitoResultado)
                {
                    if (registroEncontrado.Estatus == EstatusOperacionesIntercambioT24.PEND.ToString() ||
                        registroEncontrado.Estatus == EstatusOperacionesIntercambioT24.ERROR.ToString())
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.inserta_registro|La operación ya existia y estaba en estatus de '{registroEncontrado.Estatus}'.");
                        return registroEncontrado.T24_folio_propuesto;
                    }

                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|Comunes.inserta_registro|La operación se encontró previamente y no puede volver a ser insertada.");
                }

                string prefijo = tipoOperacion == TipoOperacion.TMD.ToString()
                    ? TipoOperacion.LD.ToString()
                    : TipoOperacion.FT.ToString();

                var folioPresupuesto = _basedatosConexion.CrearParametroSalida("@folioPropuesto", DbType.String, 12);
                var exito = _basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean);

                StoredProcedureResult reg = await _ejecutarNonQuery(EnumNombreSp.SP_INSERTA_FOLI_OPERACION, cmd =>
                {
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folioUnico", folioUnico));
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@fechaJuliana", fechaJulianaT24));
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@entradaSalida", entradaSalida, 1));
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@prefijo", prefijo, 2));
                    cmd.Parameters.Add(folioPresupuesto);
                    cmd.Parameters.Add(exito);
                });

                if (reg.Exito)
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.inserta_registro|La operación se insertó correctamente en la tabla de registro, folio_unico='{folioUnico}', folio_propuesto_T24='{folioPresupuesto.Value?.ToString()}'.");

                return folioPresupuesto.Value?.ToString();
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Comunes.inserta_registro|Ocurrió excepcion: {ex}");
                throw;
            }
        }

        public async Task<StoredProcedureResult> InsertaRegistroConResultado(string folioUnico, int fechaJulianaT24, char entradaSalida, string tipoOperacion)
        {
            try
            {
                OperacionesEntidad registroEncontrado = await ExisteRegistro(folioUnico);
                if (registroEncontrado != null && registroEncontrado.ExitoResultado)
                {
                    if (registroEncontrado.Estatus == EstatusOperacionesIntercambioT24.PEND.ToString() ||
                        registroEncontrado.Estatus == EstatusOperacionesIntercambioT24.ERROR.ToString())
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.inserta_registro|La operación ya existia y estaba en estatus de '{registroEncontrado.Estatus}'.");
                        return new StoredProcedureResult { Exito = true };
                    }

                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|Comunes.inserta_registro|La operación se encontró previamente y no puede volver a ser insertada.");
                    return new StoredProcedureResult
                    {
                        Exito = false,
                        MensajeError = $"La operación '{folioUnico}' ya existe con estatus '{registroEncontrado.Estatus}'."
                    };
                }

                string prefijo = tipoOperacion == TipoOperacion.TMD.ToString()
                    ? TipoOperacion.LD.ToString()
                    : TipoOperacion.FT.ToString();

                var folioPresupuesto = _basedatosConexion.CrearParametroSalida("@folioPropuesto", DbType.String, 12);
                var exito = _basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean);

                StoredProcedureResult reg = await _ejecutarNonQuery(EnumNombreSp.SP_INSERTA_FOLI_OPERACION, cmd =>
                {
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folioUnico", folioUnico));
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@fechaJuliana", fechaJulianaT24));
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@entradaSalida", entradaSalida, 1));
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@prefijo", prefijo, 2));
                    cmd.Parameters.Add(folioPresupuesto);
                    cmd.Parameters.Add(exito);
                });

                if (reg.Exito)
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.inserta_registro|La operación se insertó correctamente en la tabla de registro, folio_unico='{folioUnico}', folio_propuesto_T24='{folioPresupuesto.Value?.ToString()}'.");

                return reg;
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Comunes.inserta_registro|Ocurrió excepcion: {ex}");
                throw;
            }
        }

        public async Task<MW_Tipo_operacion> ObtenerMWDatosOperacion(string tipoOperacion)
        {
            var resultado = await _ejecutarMwTipoOperacion(ConstantesProcedimientosMUFG.UspT24MWOperaciones, cmd =>
            {
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion));
            });

            return resultado.Data.Count > 0 ? resultado.Data[0] : null;
        }
    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.Repositorios.T24
{
    internal sealed class T24MwSwift
    {
        private readonly IBasedatosConexion _basedatosConexion;
        private readonly IBasedatosConexion _basedatosConexionVistasT24;
        private readonly Func<string, Task<StoredProcedureResult<object>>> _ejecutarQueryObject;
        private readonly Func<string, Task<StoredProcedureResult<MensajesSwfT24Entidad>>> _ejecutarQueryMensajes;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult<IntercambioEntidad>>> _ejecutarQueryIntercambio;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult>> _ejecutarNonQueryVistas;

        public T24MwSwift(
            IBasedatosConexion basedatosConexion,
            IBasedatosConexion basedatosConexionVistasT24,
            Func<string, Task<StoredProcedureResult<object>>> ejecutarQueryObject,
            Func<string, Task<StoredProcedureResult<MensajesSwfT24Entidad>>> ejecutarQueryMensajes,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult<IntercambioEntidad>>> ejecutarQueryIntercambio,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult>> ejecutarNonQueryVistas)
        {
            _basedatosConexion = basedatosConexion;
            _basedatosConexionVistasT24 = basedatosConexionVistasT24;
            _ejecutarQueryObject = ejecutarQueryObject;
            _ejecutarQueryMensajes = ejecutarQueryMensajes;
            _ejecutarQueryIntercambio = ejecutarQueryIntercambio;
            _ejecutarNonQueryVistas = ejecutarNonQueryVistas;
        }

        public DataTable PreparaTablaSwf()
        {
            DataTable dtRegreso = new DataTable();

            dtRegreso.Columns.Add("HEADER_ID", typeof(string));
            dtRegreso.Columns.Add("MSG_ID", typeof(string));
            dtRegreso.Columns.Add("MESSAGE_TYPE", typeof(string));
            dtRegreso.Columns.Add("TRANSACTION_REF", typeof(string));
            dtRegreso.Columns.Add("SWIFT_SENDER", typeof(string));
            dtRegreso.Columns.Add("SWIFT_RECEIVER", typeof(string));
            dtRegreso.Columns.Add("SWIFT_MSG", typeof(string));
            dtRegreso.Columns.Add("T24_MSG", typeof(string));
            return dtRegreso;
        }

        public async Task<DataTable> LeerMensajesSwfT24()
        {
            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|Comunes.Lee_mensajes_T24|Inicia la lectura de SWIFT.");

            DataTable dtRegreso = PreparaTablaSwf();

            StoredProcedureResult<MensajesSwfT24Entidad> resultadoSp =
                await _ejecutarQueryMensajes(EnumNombreSp.SP_VW_T24_SWF_MESSAGES);

            if (resultadoSp.Data == null || !resultadoSp.Data.Any())
            {
                return dtRegreso;
            }

            foreach (MensajesSwfT24Entidad swfT24 in resultadoSp.Data)
            {
                DataRow row = dtRegreso.NewRow();

                row["HEADER_ID"] = swfT24.HEADER_ID;
                row["MSG_ID"] = swfT24.MSG_ID;
                row["MESSAGE_TYPE"] = swfT24.MESSAGE_TYPE;
                row["TRANSACTION_REF"] = swfT24.TRANSACTION_REF;
                row["SWIFT_SENDER"] = swfT24.CIF_SENDER;
                row["SWIFT_RECEIVER"] = swfT24.BANCO_RECEPTOR;
                row["T24_MSG"] = swfT24.MT_MSG;

                string msj = swfT24.MESSAGE_TXT;
                if (msj.EndsWith("}"))
                    msj = msj.Substring(0, msj.Length - 1);

                row["SWIFT_MSG"] = msj;
                dtRegreso.Rows.Add(row);
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.lee_mensajes_swift_T24|Rows.Count= {resultadoSp.Data.Count}.");
            return dtRegreso;
        }

        public Task<bool> MarcaMensajesSwfT24(Dictionary<string, string> swfHeadersId)
        {
            return MarcaMensajesSwfT24(swfHeadersId, CancellationToken.None);
        }

        public async Task<bool> MarcaMensajesSwfT24(Dictionary<string, string> swfHeadersId, CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                if (swfHeadersId == null || swfHeadersId.Count == 0)
                    return false;

                object resultadoFecha = (await _ejecutarQueryObject(EnumNombreSp.SP_FECHA_SERVIDOR)).Data.FirstOrDefault();

                if (resultadoFecha == null)
                    return false;

                if (!DateTime.TryParse(resultadoFecha.ToString(), out DateTime fechaBase))
                    return false;

                string fechabase = fechaBase.ToString("yyyy-MM-dd HH:mm:ss.fff");
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.marca_mensajes_swift_T24|Inicio ciclo en fecha y hora de BD = '{fechabase}'.");

                foreach (KeyValuePair<string, string> headerId in swfHeadersId)
                {
                    ct.ThrowIfCancellationRequested();
                    try
                    {
                        string doh = "DOH";
                        string swiftHeaderId = headerId.Key;
                        string msjId = $"{doh}.{swiftHeaderId}";
                        string ofs = $"DE.O.HEADER,BTM.STATUS.UPD/I/PROCESS,,{swiftHeaderId},STP.STATUS:1:1=MQ";

                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.marca_mensajes_swift_T24|A intentar marcar el mensaje Swift Header ID '{swiftHeaderId}' como realizado.");

                        await _ejecutarNonQueryVistas(EnumNombreSp.SP_T24_ADD_TRX, cmd =>
                        {
                            cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@folio", msjId));
                            cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@trans_source", doh));
                            cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@trans_id", swiftHeaderId));
                            cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@folio_unico", string.Empty));
                            cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@ofs", ofs));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
                        });

                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|Comunes.marca_mensajes_swift_T24|Paso la declaración de parametros, inicia la ejecución del SP: uSP_T24_ADD_TRX.");
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|Comunes.marca_mensajes_swift_T24|Paso la ejecución del SP.");
                    }
                    catch (Exception ex)
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"AVISO|Comunes.marca_mensajes_swift_T24|ERROR={ex.Message}.");
                    }
                }

                const int maxIntentos = 10;
                for (int i = 0; i < maxIntentos; i++)
                {
                    await Task.Delay(1000, ct);
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.marca_mensajes_swift_T24|Inicia ciclo {i + 1} de busqueda de respuesta.");
                    StoredProcedureResult<IntercambioEntidad> resultadoSp =
                        await _ejecutarQueryIntercambio(
                            EnumNombreSp.SP_INTERCAMBIO, cmd =>
                            {
                                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@fechaProceso", fechabase));
                            });

                    if (resultadoSp.Data == null || !resultadoSp.Data.Any())
                        continue;

                    Dictionary<string, bool> respuesta = resultadoSp.Data
                        .Where(x => x != null && !string.IsNullOrEmpty(x.RecId))
                        .GroupBy(x => x.RecId)
                        .ToDictionary(
                            g => g.Key,
                            g => g.First().Cve_status == EstatusSwiftT24.Success);

                    bool todosProcesados = true;

                    foreach (string headerId in swfHeadersId.Keys)
                    {
                        string msjId = $"DOH.{headerId}";

                        if (!respuesta.TryGetValue(msjId, out bool estatusExitoso))
                        {
                            todosProcesados = false;
                            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|marca_mensajes_swift_T24|El Swift Headers ID '{headerId}' no ha sido marcado, a realizar siguiente ciclo de revisión.");
                            break;
                        }

                        if (!estatusExitoso)
                        {
                            todosProcesados = false;
                            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|marca_mensajes_swift_T24|El Swift Headers ID '{headerId}' fue encontrado pero con estatus no exitoso, a realizar siguiente ciclo de revisión.");
                            break;
                        }
                    }

                    if (todosProcesados)
                    {
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|marca_mensajes_swift_T24|Todos los Swift Headers ID fueron encontrados, se termina el ciclo.");
                        return true;
                    }
                }
                return false;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Comunes.marca_mensajes_swift_T24|{ConstantesClaveAviso.Error}={ex}.");
                return false;
            }
        }
    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace MUFG_T24.Repositorios.T24
{
    internal sealed class T24MwSpid
    {
        private readonly IBasedatosConexion _basedatosConexion;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult<SpidValidacionEntidad>>> _ejecutarQuerySpid;

        public T24MwSpid(
            IBasedatosConexion basedatosConexion,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult<SpidValidacionEntidad>>> ejecutarQuerySpid)
        {
            _basedatosConexion = basedatosConexion;
            _ejecutarQuerySpid = ejecutarQuerySpid;
        }

        /// <summary>
        /// Validación de la cuenta SPID x cdl (validación de FIEL y Accionistas)
        /// </summary>
        public async Task<EsClienteSpidValidoResultado> EsClienteSpidValido(string cdl)
        {
            EsClienteSpidValidoResultado esClienteSpidValido = new EsClienteSpidValidoResultado();

            StoredProcedureResult<SpidValidacionEntidad> resultadoSp = await _ejecutarQuerySpid(
                EnumNombreSp.SP_SPID_CLIENTES, cmd =>
                {
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@cdl", cdl));
                });

            if (resultadoSp == null || resultadoSp.Data == null || !resultadoSp.Data.Any())
            {
                esClienteSpidValido.EsClienteValido = false;
                esClienteSpidValido.Mensaje = "No se encontró información SPID del cliente asociado a la cuenta";
                return esClienteSpidValido;
            }

            SpidValidacionEntidad data = resultadoSp.Data.FirstOrDefault();

            bool fielValidos = data.Fiel_status == Estatus.PROCESADO.ToString() &&
                data.Ws_resp_code == "0" &&
                data.Estado_consulta == Estatus.Válido.ToString();

            bool accionistasProcesados = data.Accionistas_estatus == Estatus.PROCESADO.ToString();

            bool validadoHoy = false;

            if (!string.IsNullOrWhiteSpace(data.Fecha_consulta) &&
                DateTime.TryParse(data.Fecha_consulta, out DateTime fechaConsulta))
            {
                DateTime hoy = DateTime.Now;
                validadoHoy = fechaConsulta.Date == hoy.Date && fechaConsulta.Hour >= 6;
            }

            bool reporteT24 = data.Reporte_24 == "1";
            bool fechaConstitucionValida = !string.IsNullOrWhiteSpace(data.Fecha_constitucion);

            if (!fielValidos)
            {
                esClienteSpidValido.EsClienteValido = false;
                esClienteSpidValido.Mensaje = "El cliente asociado a la cuenta no tiene una FIEL válida";
                return esClienteSpidValido;
            }

            if (!accionistasProcesados)
            {
                esClienteSpidValido.EsClienteValido = false;
                esClienteSpidValido.Mensaje = "El cliente asociado a la cuenta no tiene una lista de accionistas";
                return esClienteSpidValido;
            }

            if (!validadoHoy)
            {
                esClienteSpidValido.EsClienteValido = false;
                esClienteSpidValido.Mensaje = "El cliente asociado a la cuenta no se ha validado el día de hoy";
                return esClienteSpidValido;
            }

            if (reporteT24)
            {
                esClienteSpidValido.EsClienteValido = false;
                esClienteSpidValido.Mensaje = "El cliente asociado a la cuenta tiene reporte 24 horas activado";
                return esClienteSpidValido;
            }

            if (!fechaConstitucionValida)
            {
                esClienteSpidValido.EsClienteValido = false;
                esClienteSpidValido.Mensaje = "El cliente asociado a la cuenta no tiene una fecha constitución establecida";
                return esClienteSpidValido;
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|comunes.esClienteSPID_Valido|A validar que todos los accionistas no esten bloqueados por HotScan.");
            StoredProcedureResult<SpidValidacionEntidad> hotScanRespuesta =
                await _ejecutarQuerySpid(
                    EnumNombreSp.SP_SPID_ACCIONISTAS_VALIDADOS, cmd =>
                    {
                        cmd.Parameters.Add(_basedatosConexion.CrearParametro("@cdl", cdl));
                    });

            if (hotScanRespuesta != null && hotScanRespuesta.Data.Any())
            {
                foreach (SpidValidacionEntidad estatus in hotScanRespuesta.Data)
                {
                    if (estatus.Nf_estatus == Estatus.RECC.ToString())
                    {
                        esClienteSpidValido.Mensaje = "El cliente asociado a la cuenta está rechazado en Hotscan";
                        esClienteSpidValido.EsClienteValido = false;
                        return esClienteSpidValido;
                    }
                    if (estatus.Nf_estatus != Estatus.CONP.ToString())
                    {
                        esClienteSpidValido.Mensaje = "El cliente asociado a la cuenta está en proceso de evaluación Hotscan";
                        esClienteSpidValido.EsClienteValido = false;
                        return esClienteSpidValido;
                    }
                }
            }

            esClienteSpidValido.Cdl = cdl;
            esClienteSpidValido.Mensaje = string.Empty;
            esClienteSpidValido.EsClienteValido = true;
            return esClienteSpidValido;
        }

        /// <summary>
        /// Validación de la cuenta SPID x cdl (validación de FIEL y Accionistas)
        /// </summary>
        public async Task<EsClienteSpidValidoResultado> EsCuentaSpidValida(string cuenta, string mensaje, string cdl = "")
        {
            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|comunes.esClienteSPID_Valido|A validar los estatus de FIEL y accionistas de la cuenta={cuenta}.");

            StoredProcedureResult<SpidValidacionEntidad> resultadoSp =
                await _ejecutarQuerySpid(
                    EnumNombreSp.SP_SPID_SOLICITUDES_CUENTAS_CLIENTES, cmd =>
                    {
                        cmd.Parameters.Add(_basedatosConexion.CrearParametro("@cuenta", cuenta));
                    });

            if (resultadoSp != null && resultadoSp.Data != null && resultadoSp.Data.Count > 0)
            {
                cdl = resultadoSp.Data.Select(x => x.Cdl).FirstOrDefault();

                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|comunes.esClienteSPID_Valido|Envio validar el CDL={cdl}.");
                return await EsClienteSpidValido(cdl);
            }

            EsClienteSpidValidoResultado clienteNoValido = new EsClienteSpidValidoResultado();
            mensaje = "La cuenta no tiene autorización para procesar pagos SPID";
            clienteNoValido.Mensaje = mensaje;
            clienteNoValido.EsClienteValido = false;
            return clienteNoValido;
        }
    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Data;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MUFG_T24.Repositorios.T24
{
    internal sealed class T24MwEnd2End
    {
        private readonly IBasedatosConexion _basedatosConexion;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult<OperacionesEntidad>>> _ejecutarQueryOperaciones;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult>> _ejecutarNonQuery;

        public T24MwEnd2End(
            IBasedatosConexion basedatosConexion,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult<OperacionesEntidad>>> ejecutarQueryOperaciones,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult>> ejecutarNonQuery)
        {
            _basedatosConexion = basedatosConexion;
            _ejecutarQueryOperaciones = ejecutarQueryOperaciones;
            _ejecutarNonQuery = ejecutarNonQuery;
        }

        public bool EsSwEnd2EndRefValida(string swEnd2EndRef)
        {
            Regex expresion = new Regex(
                @"^[0-9abcdef]{8}\-[0-9abcdef]{4}\-4[0-9abcdef]{3}\-[89ab][0-9abcdef]{3}-[0-9abcdef]{12}$"
            );

            if (string.IsNullOrWhiteSpace(swEnd2EndRef))
                return false;

            return expresion.IsMatch(swEnd2EndRef);
        }

        public async Task<string> GeneraSwEnd2EndRef(string slaUetr, string referenciaT24, string folioUnico,
            string tipoOperacion, string folioOperacion, string swiftInOut, string folioSwift, string claveMensaje)
        {
            string uetr = Guid.NewGuid().ToString();

            if (!EsSwEnd2EndRefValida(uetr))
                throw new Exception("No se pudo crear una referencia UETR");

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|genera_SW_END2END_REF|Se creo la UETR = '{uetr}'.");

            string exstente = await BuscaSwEnd2EndRef(
                uetr,
                string.Empty,
                string.Empty,
                string.Empty,
                string.Empty,
                string.Empty,
                string.Empty);

            if (!string.IsNullOrEmpty(exstente))
                throw new Exception($"La clave '{uetr}' ya existe");

            await InsertaSwEnd2EndRef(uetr, slaUetr, referenciaT24, folioUnico, tipoOperacion, folioOperacion, swiftInOut, folioSwift);

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|genera_SW_END2END_REF|Se genero la llave SW_END2END_REF='{uetr}', " +
                $"para los datos: referenciaT24='{referenciaT24}', folio_unico='{folioUnico}', tipo_operacion='{tipoOperacion}', " +
                $"folio_operacion='{folioOperacion}', swift_in_out='{swiftInOut}', folio_swift='{folioSwift}', clave_mensaje='{claveMensaje}'.");

            return uetr;
        }

        public async Task<string> BuscaSwEnd2EndRef(string swEnd2EndRef, string referenciaT24, string folioUnico,
            string tipoOperacion, string folioOperacion, string swiftInOut, string folioSwift)
        {
            IDbDataParameter swRefRet = _basedatosConexion.CrearParametroSalida("@sw_end2end_ref_ret", DbType.String, 36);

            await _ejecutarQueryOperaciones(EnumNombreSp.SP_SWF_BUSCA_SW_END2END_REF, cmd =>
            {
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@sw_end2end_ref", swEnd2EndRef, 36));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@referenciaT24", referenciaT24, 35));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_unico", folioUnico, 35));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion, 10));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_operacion", folioOperacion, 25));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@swift_in_out", swiftInOut, 10));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_swift", folioSwift, 25));
                cmd.Parameters.Add(swRefRet);
            });

            object valorSalida = swRefRet?.Value;
            if (valorSalida == null || valorSalida == DBNull.Value)
                return string.Empty;

            return BaseDatosHelper.ObtieneTexto(valorSalida.ToString());
        }

        public async Task<bool> InsertaSwEnd2EndRef(string swEnd2EndRef, string slaUetr, string referenciaT24, string folioUnico,
            string tipoOperacion, string folioOperacion, string swiftInOut, string folioSwift)
        {
            StoredProcedureResult resultadoSp = await _ejecutarNonQuery(EnumNombreSp.SP_SWF_INSERTA_SW_END2END_REF, cmd =>
            {
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@sw_end2end_ref", swEnd2EndRef, 36));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@sla_uetr", slaUetr, 3));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@referenciaT24", referenciaT24, 35));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_unico", folioUnico, 35));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion, 10));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_operacion", folioOperacion, 25));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@swift_in_out", swiftInOut, 10));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_swift", folioSwift, 25));
            });

            return resultadoSp.Exito;
        }

        public async Task<bool> ActualizaSwEnd2EndRef(string swEnd2EndRef, string referenciaT24, string folioUnico,
            string tipoOperacion, string folioOperacion, string swiftInOut, string folioSwift)
        {
            StoredProcedureResult resultadoSp = await _ejecutarNonQuery(EnumNombreSp.SP_SWF_ACTUALIZA_SW_END2END_REF, cmd =>
            {
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@sw_end2end_ref", swEnd2EndRef, 36));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@referenciaT24", referenciaT24, 35));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_unico", folioUnico, 35));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion, 10));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_operacion", folioOperacion, 25));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@swift_in_out", swiftInOut, 10));
                cmd.Parameters.Add(_basedatosConexion.CrearParametro("@folio_swift", folioSwift, 25));
            });

            if (resultadoSp.Exito)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|actualiza_SW_END2END_REF|Se actualizó la llave SW_END2END_REF=' {swEnd2EndRef}' " +
                    $"con los datos: referenciaT24='{referenciaT24}', folio_unico='{folioUnico}', tipo_operacion='{tipoOperacion}', " +
                    $"folio_operacion='{folioOperacion}', swift_in_out='{swiftInOut}', folio_swift='{folioSwift}'.");
            }
            else
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|actualiza_SW_END2END_REF|No se actualizó la llave SW_END2END_REF='{swEnd2EndRef}'. " +
                    $"Detalle: {resultadoSp.MensajeError ?? resultadoSp.Exception?.Message ?? "Sin detalle"}");
            }

            return resultadoSp.Exito;
        }
    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace MUFG_T24.Repositorios.T24
{
    internal sealed class T24MwForex
    {
        private readonly IBasedatosConexion _basedatosConexion;
        private readonly Func<string, Action<IDbCommand>, Task<StoredProcedureResult<SolicitudesForexEntidad>>> _ejecutarQueryForex;
        private readonly Func<string, string> _validaMoneda;

        public T24MwForex(
            IBasedatosConexion basedatosConexion,
            Func<string, Action<IDbCommand>, Task<StoredProcedureResult<SolicitudesForexEntidad>>> ejecutarQueryForex,
            Func<string, string> validaMoneda)
        {
            _basedatosConexion = basedatosConexion;
            _ejecutarQueryForex = ejecutarQueryForex;
            _validaMoneda = validaMoneda;
        }

        /// <summary>
        /// Valida estatus FOREX.
        /// Legacy: validaEstatus_Forex
        /// </summary>
        public async Task<ValidacionForexResultado> ValidaEstatusForex(string forexTipoYNuemro)
        {
            if (string.IsNullOrWhiteSpace(forexTipoYNuemro))
                return new ValidacionForexResultado { Encontrado = false, EsValido = false };

            string fuente = forexTipoYNuemro.Substring(0, 1);
            string numero = forexTipoYNuemro.Length > 1 ? forexTipoYNuemro.Substring(1) : string.Empty;

            string tabla = EnumTablas.SOLICITUDESFOREX;
            string tipo = null;

            string fuenteFinal = fuente;
            string numeroFinal = numero;

            switch (fuente)
            {
                case "C":
                    tipo = TipoOperacion.CMS.ToString();
                    break;

                case "F":
                    tabla += TipoOperacion.FAX.ToString();
                    tipo = TipoOperacion.FAX.ToString();
                    break;

                case "B":
                    tabla += TipoOperacion.BANCOS.ToString();
                    tipo = TipoOperacion.BANCOS.ToString();
                    break;

                case "E":
                    tabla += TipoOperacion.ET.ToString();
                    tipo = TipoOperacion.ET.ToString();
                    break;

                default:
                    if (int.TryParse(fuente, out _))
                    {
                        fuenteFinal = "C";
                        tipo = TipoOperacion.CMS.ToString();
                        numeroFinal = forexTipoYNuemro;
                    }
                    break;
            }

            StoredProcedureResult<SolicitudesForexEntidad> respuestaSp =
                await _ejecutarQueryForex(
                    EnumNombreSp.SP_CMM_SOLICITUDESFOREX,
                    cmd =>
                    {
                        cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tabla", tabla));
                        cmd.Parameters.Add(_basedatosConexion.CrearParametro("@autorizacionId", numeroFinal));
                    });

            if (respuestaSp?.Data == null || !respuestaSp.Data.Any())
            {
                return new ValidacionForexResultado
                {
                    Encontrado = false,
                    EsValido = false,

                    ForexFuente = fuenteFinal,
                    ForexTipo = tipo,
                    ForexTabla = tabla,
                    ForexNumero = numeroFinal
                };
            }

            SolicitudesForexEntidad dataAutorizada = respuestaSp.Data
                .FirstOrDefault(x =>
                    string.Equals(x.StatusOperacion, "AUTORIZADO", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(x.StatusOperacion, "PROCESADO", StringComparison.OrdinalIgnoreCase));

            if (dataAutorizada == null)
            {
                return new ValidacionForexResultado
                {
                    Encontrado = false,
                    EsValido = false,

                    ForexFuente = fuenteFinal,
                    ForexTipo = tipo,
                    ForexTabla = tabla,
                    ForexNumero = numeroFinal
                };
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.validaEstatus_Forex|Forex {tipo} con FX '{numeroFinal}' ya está autorizado.");

            return new ValidacionForexResultado
            {
                Encontrado = true,
                EsValido = true,

                ForexFuente = fuenteFinal,
                ForexTabla = tabla,
                ForexTipo = tipo,
                ForexNumero = numeroFinal,

                DivisaAdquiereCliente = dataAutorizada.DivisaAdquiereCliente,
                CuentaAdquiereCliente = dataAutorizada.CuentaAdquiereCliente
            };
        }

        /// <summary>
        /// Valida datos FOREX.
        /// Legacy: validaDatos_Forex
        /// </summary>
        public async Task<ValidacionForexResultado> ValidaDatosForex(string forexTipoYNuemro, string divisa, string cuenta, decimal monto, string sentido)
        {
            var resultado = new ValidacionForexResultado
            {
                Encontrado = false,
                EsValido = false
            };

            if (string.IsNullOrWhiteSpace(forexTipoYNuemro))
            {
                resultado.RazonRechazo = "El forex '' no existe.";
                return resultado;
            }

            string fuente = forexTipoYNuemro.Substring(0, 1);
            string numero = forexTipoYNuemro.Length > 1 ? forexTipoYNuemro.Substring(1) : string.Empty;

            string tabla = EnumTablas.SOLICITUDESFOREX;
            string tipo = null;

            if (int.TryParse(fuente, out _))
            {
                fuente = "C";
                tipo = TipoOperacion.CMS.ToString();
                numero = forexTipoYNuemro;
            }
            else
            {
                switch (fuente)
                {
                    case "C":
                        tipo = TipoOperacion.CMS.ToString();
                        break;

                    case "F":
                        tabla += TipoOperacion.FAX.ToString();
                        tipo = TipoOperacion.FAX.ToString();
                        break;

                    case "B":
                        tabla += TipoOperacion.BANCOS.ToString();
                        tipo = TipoOperacion.BANCOS.ToString();
                        break;

                    case "E":
                        tabla += TipoOperacion.ET.ToString();
                        tipo = TipoOperacion.ET.ToString();
                        break;

                    default:
                        resultado.RazonRechazo = $"El forex '{forexTipoYNuemro}' no existe.";
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.validaDatos_Forex|{resultado.RazonRechazo}");
                        return resultado;
                }
            }

            resultado.ForexFuente = fuente;
            resultado.ForexTipo = tipo;
            resultado.ForexTabla = tabla;
            resultado.ForexNumero = numero;

            var respuestaSp = await _ejecutarQueryForex(
                EnumNombreSp.SP_CMM_SOLICITUDESFOREX,
                cmd =>
                {
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@tabla", tabla));
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@autorizacionId", numero));
                });

            if (respuestaSp?.Data == null || !respuestaSp.Data.Any())
            {
                resultado.RazonRechazo = $"El forex '{forexTipoYNuemro}' no existe.";
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.validaDatos_Forex|{resultado.RazonRechazo}");
                return resultado;
            }

            var data = respuestaSp.Data.First();

            resultado.Encontrado = true;

            string estatus = (data.StatusOperacion ?? string.Empty).Trim();
            bool autorizado = estatus.Equals("AUTORIZADO", StringComparison.OrdinalIgnoreCase)
                           || estatus.Equals("PROCESADO", StringComparison.OrdinalIgnoreCase);

            if (!autorizado)
            {
                resultado.RazonRechazo = $"El forex '{forexTipoYNuemro}' no está AUTORIZADO.";
                return resultado;
            }

            string divisaBd;
            string cuentaBd;
            decimal montoBd;

            if (string.Equals(sentido, "E", StringComparison.OrdinalIgnoreCase))
            {
                divisaBd = _validaMoneda(data.DivisaEntregaCliente);
                cuentaBd = data.CuentaEntregaCliente;
                montoBd = data.MontoEntregaCliente;
            }
            else
            {
                divisaBd = _validaMoneda(data.DivisaAdquiereCliente);
                cuentaBd = data.CuentaAdquiereCliente;
                montoBd = data.MontoAdquiereCliente;
            }

            bool validacion = string.Equals(divisa, divisaBd, StringComparison.OrdinalIgnoreCase)
                           && string.Equals(cuenta, cuentaBd, StringComparison.OrdinalIgnoreCase)
                           && monto == montoBd;
            if (!validacion)
            {
                resultado.RazonRechazo =
                    $"Forex {tipo} con FX '{numero}' ya está autorizado para divisa='{divisaBd}', cuenta='{cuentaBd}', monto='{montoBd}'. " +
                    $"Los parámetros de validación son divisa='{divisa}', cuenta='{cuenta}', monto='{monto}'. Por lo que no corresponden.";
            }

            resultado.EsValido = validacion;

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                $"INFO|Comunes.validaDatos_Forex|Forex {tipo} con FX '{numero}' ya está autorizado para divisa='{divisaBd}', cuenta='{cuentaBd}', monto='{montoBd}'. " +
                $"Los parámetros de validación son divisa='{divisa}', cuenta='{cuenta}', monto='{monto}'. La validación es:{validacion}.");

            return resultado;
        }
    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace MUFG_T24.Repositorios.T24
{
    internal sealed class T24MwVistas
    {
        private readonly IBasedatosConexion _basedatosConexion;
        private readonly IBasedatosConexion _basedatosConexionVistasT24;
        private readonly Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<OperacionesEntidad>>> _ejecutarOperaciones;
        private readonly Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<CuentaClienteT24>>> _ejecutarCuentaCliente;
        private readonly Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<MwT24TrxDetalle>>> _ejecutarDetalle;

        public T24MwVistas(
            IBasedatosConexion basedatosConexion,
            IBasedatosConexion basedatosConexionVistasT24,
            Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<OperacionesEntidad>>> ejecutarOperaciones,
            Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<CuentaClienteT24>>> ejecutarCuentaCliente,
            Func<string, Action<IDbCommand>, bool, Task<StoredProcedureResult<MwT24TrxDetalle>>> ejecutarDetalle)
        {
            _basedatosConexion = basedatosConexion;
            _basedatosConexionVistasT24 = basedatosConexionVistasT24;
            _ejecutarOperaciones = ejecutarOperaciones;
            _ejecutarCuentaCliente = ejecutarCuentaCliente;
            _ejecutarDetalle = ejecutarDetalle;
        }

        public async Task<string> ObtenerFechaJuliana5digitos()
        {
            bool esConexionVistasT24 = true;
            string fechaJulianaRaw = (await _ejecutarOperaciones(
                EnumNombreSp.SP_BUSCA_FECHA_T24, null, esConexionVistasT24))
                .Data
                .Select(x => x.Julian_date)
                .FirstOrDefault();

            if (string.IsNullOrWhiteSpace(fechaJulianaRaw))
                throw new InvalidOperationException("No se obtuvo fecha juliana desde la vista T24.");

            fechaJulianaRaw = fechaJulianaRaw.Trim();

            if (fechaJulianaRaw.Length <= 2)
                throw new InvalidOperationException($"Formato de fecha juliana inválido: '{fechaJulianaRaw}'.");

            if (!fechaJulianaRaw.All(char.IsDigit))
                throw new InvalidOperationException($"Formato de fecha juliana no numérico: '{fechaJulianaRaw}'.");

            return fechaJulianaRaw.Substring(2);
        }

        public async Task<DataTable> DatosClienteCuentasVw(string cliente = null, string cuenta = null)
        {
            DataTable dt = new DataTable();
            StoredProcedureResult<CuentaClienteT24> resultadoSp = await _ejecutarCuentaCliente(
                EnumNombreSp.SP_T24_CUSTOMER_ACCOUNT, cmd =>
                {
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@cliente", cliente));
                }, true
            );

            if (resultadoSp.Data.Count == 0)
                return dt;

            return Comunes.Comunes.ConvertirListaADataTable(resultadoSp.Data);
        }

        public async Task<DataTable> DatosClienteVw(string codigoCliente)
        {
            DataTable dt = new DataTable();
            StoredProcedureResult<CuentaClienteT24> resultadoSp = await _ejecutarCuentaCliente(
                EnumNombreSp.SP_T24_DATOS_CUENTA_CLIENTE, cmd =>
                {
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@numeroCuenta", codigoCliente));
                }, true
            );

            if (resultadoSp.Data.Count == 0)
                return dt;

            return Comunes.Comunes.ConvertirListaADataTable(resultadoSp.Data);
        }

        public async Task<DataTable> DatosCuentaVw(string codigoCliente)
        {
            return await DatosClienteVw(codigoCliente);
        }

        public async Task<DataTable> DatosClienteVw2(string cliente)
        {
            DataTable dt = new DataTable();
            StoredProcedureResult<CuentaClienteT24> resultadoSp = await _ejecutarCuentaCliente(
                EnumNombreSp.SP_T24_ObtenerCustomerT24, cmd =>
                {
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@customer_code", cliente));
                }, true
            );

            if (resultadoSp.Data.Count == 0)
                return dt;

            return Comunes.Comunes.ConvertirListaADataTable(resultadoSp.Data);
        }

        public async Task<List<CuentaClienteT24>> ObtieneDireccionClienteVw(string cliente)
        {
            StoredProcedureResult<CuentaClienteT24> resultadoSp = await _ejecutarCuentaCliente(
                EnumNombreSp.SP_TOMI_VW_ADDRESS_CUSTOMER_ALL, cmd =>
                {
                    cmd.Parameters.Add(_basedatosConexion.CrearParametro("@codigoCliente", cliente));
                }, true
            );

            if (resultadoSp.Data.Count == 0)
                return new List<CuentaClienteT24>();

            return resultadoSp.Data;
        }

        public async Task<MwT24TrxDetalle> LecturaFtT24Vw(string folioT24, bool inau)
        {
            string strVistaNombre;
            try
            {
                if (inau)
                    strVistaNombre = EnumTablas.VISTA_FT_INAU;
                else
                    strVistaNombre = EnumTablas.VISTA_FT;

                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.existe_FT_T24_VW|Leyendo el folio '{folioT24}' de enquiry de T24. INAU: {inau}");
                StoredProcedureResult<MwT24TrxDetalle> resultadoSp =
                    await _ejecutarDetalle(
                        EnumNombreSp.SP_T24_FUNDS_TRANSFER,
                        cmd =>
                        {
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@curId", folioT24));
                            cmd.Parameters.Add(_basedatosConexion.CrearParametro("@vista", strVistaNombre));
                        }, true
                );
                if (resultadoSp.Exito)
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.lee_FT_T24_VW|Existe folio={folioT24}.");
                return resultadoSp.Data.FirstOrDefault();
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Comunes.lee_FT_T24_VW|Error: {ex}");
                return null;
            }
        }

        public async Task<MwT24TrxDetalle> ExisteT24Vw(string folioT24)
        {
            try
            {
                MwT24TrxDetalle trxDetalle = new MwT24TrxDetalle();
                bool datosEncontrados = false;
                if (string.IsNullOrEmpty(folioT24) || folioT24.Length <= 2)
                    return null;

                if (folioT24.Substring(0, 2) == TipoOperacion.FT.ToString())
                {
                    StoredProcedureResult<MwT24TrxDetalle> datosT24 = await _ejecutarDetalle(
                        EnumNombreSp.SP_T24_FUNDS_TRANSFER_ID,
                        cmd =>
                        {
                            cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@curId", folioT24));

                        }, true);
                    datosEncontrados = datosT24.Exito;
                    trxDetalle = datosT24.Data.FirstOrDefault() ?? new MwT24TrxDetalle();
                }
                else if (folioT24.Substring(0, 2) == TipoOperacion.LD.ToString())
                {
                    StoredProcedureResult<MwT24TrxDetalle> datosT24 =
                        await _ejecutarDetalle(
                            EnumNombreSp.SP_T24_LOANS_AND_DEPOSITS,
                            cmd =>
                            {
                                cmd.Parameters.Add(_basedatosConexionVistasT24.CrearParametro("@ldId", folioT24));

                            }, true);
                    datosEncontrados = datosT24.Exito;
                    trxDetalle = datosT24.Data.FirstOrDefault() ?? new MwT24TrxDetalle();
                }
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|Comunes.existe_T24_VW|Encontro valores={datosEncontrados}");
                return trxDetalle;
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Comunes.existe_T24_VW|Error: {ex}");
                throw;
            }
        }
    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Core.Configuracion;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Data.Common;

namespace MUFG_T24.Conexiones
{
    /// <summary>
    /// Crea la conexión apropiada según el proveedor
    /// </summary>
    public class ConexionBasedatosFactory
    {
        /// <summary>
        /// Inicializa una nueva instancia de CrearConexion.
        /// </summary>
        public static IBasedatosConexion CrearConexion(BasedatosProveedor? proveedor, NombreBasedatos? nombreBasedatos)
        {
            string cadenaConexion = ObtenerCadenaConexion(proveedor, nombreBasedatos);
            switch (proveedor)
            {
                case BasedatosProveedor.SqlServer:
                    return new ConexionSqlServer(cadenaConexion);
                case BasedatosProveedor.MySql:
                    return new ConexionMySql(cadenaConexion);
                case BasedatosProveedor.Oracle:
                    return new ConexionOracle(cadenaConexion);
                default:
                    throw new NotSupportedException($"El proveedor de base de datos {proveedor} no es soportado");
            }
        }

        /// <summary>
        /// Obtiene datos generales de la seccion AppSettings del archivo de configuración
        /// </summary>
        /// <param name="dato"></param>
        /// <returns>string</returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static string ObtenerDatosGenerales(string dato)
        {
            return LecturaConfguracionGeneral.ObtenerDatosGenerales(dato);
        }

        /// <summary>
        /// Método para obtener cadenas de conexión
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        /// <returns>string</returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static string ObtenerCadenaConexion(BasedatosProveedor? proveedor, NombreBasedatos? nombreBasedatos)
        {
            return LecturaConfguracionGeneral.ObtenerCadeaConexion(proveedor, nombreBasedatos);
        }

        /// <summary>
        /// Obtiene parametros de la cadena de conexion
        /// </summary>
        /// <param name="cadenaConexion"></param>
        /// <param name="parametro"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static string ObtenerParametroConexion(string cadenaConexion, string parametro)
        {
            DbConnectionStringBuilder constructor = new DbConnectionStringBuilder
            {
                ConnectionString = cadenaConexion
            };

            string valor = constructor.ContainsKey(parametro)
                ? constructor[parametro].ToString() 
                : null;

            if (string.IsNullOrEmpty(valor))
            {
                throw new InvalidOperationException(
                    $"El valor del parametro {parametro} no fue encontrado en el archivo de configuración.");
            }
            return valor;
        }

    }
}

using MUFG_T24.Interfaces_utilidades;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace MUFG_T24.Conexiones
{
    /// <summary>
    /// Implementación de SQLSERVER
    /// </summary>
    internal class ConexionSqlServer : IBasedatosConexion
    {
        private readonly string _cadenaConexion;

        /// <summary>
        /// Inicializa una nueva instancia de ConexionSqlServer.
        /// </summary>
        public ConexionSqlServer(string cadenaConexion)
        {
            _cadenaConexion = cadenaConexion;
        }

        /// <summary>
        /// Método crea comando SQL
        /// </summary>
        /// <param name="comando"></param>
        /// <param name="conexion"></param>
        /// <returns>IDbCommand</returns>
        public DbCommand CrearComando(string comando, DbConnection conexion)
        {
            return new SqlCommand(comando, (SqlConnection)conexion);
        }

        /// <summary>
        /// Método para crear la conexión a la BD
        /// </summary>
        /// <returns>IDbConnection</returns>
        public DbConnection CrearConexion()
        {
            return new SqlConnection(_cadenaConexion);
        }

        /// <summary>
        /// Método para crear parámetros
        /// </summary>
        /// <param name="nombre"></param>
        /// <param name="valor"></param>
        /// <returns>IDbDataParameter</returns>
        public IDbDataParameter CrearParametro(string nombre, object valor, int? tamano = null)
        {
            SqlParameter parametro = new SqlParameter(nombre, valor ?? DBNull.Value);

            if (tamano.HasValue)
            {
                parametro.Size = tamano.Value;
            }

            return parametro;
        }

        /// <summary>
        /// Método para crear parametros de salida
        /// </summary>
        /// <param name="nombre"></param>
        /// <param name="dbType"></param>
        /// <returns>IDbDataParameter</returns>
        public IDbDataParameter CrearParametroSalida(string nombre, DbType dbType, int tamano = 0)
        {
            SqlParameter parametro = new SqlParameter(nombre, ConvertirDbTypeASqlDbType(dbType))
            {
                Direction = ParameterDirection.Output
            };

            if (tamano > 0)
            {
                parametro.Size = tamano;
            }

            return parametro;
        }

        /// <summary>
        /// Conversion de tipos a SQL
        /// </summary>
        /// <param name="dbType"></param>
        /// <returns>SqlDbType</returns>
        private SqlDbType ConvertirDbTypeASqlDbType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.Int16: return SqlDbType.SmallInt;
                case DbType.Int32: return SqlDbType.Int;
                case DbType.Int64: return SqlDbType.BigInt;
                case DbType.String: return SqlDbType.NVarChar;
                case DbType.AnsiString: return SqlDbType.VarChar;
                case DbType.DateTime: return SqlDbType.DateTime;
                case DbType.Date: return SqlDbType.Date;
                case DbType.Decimal: return SqlDbType.Decimal;
                case DbType.Double: return SqlDbType.Float;
                case DbType.Boolean: return SqlDbType.Bit;
                default: return SqlDbType.NVarChar;
            }
        }
    }
}

using MUFG_T24.Interfaces_utilidades;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.Common;

namespace MUFG_T24.Conexiones
{
    /// <summary>
    /// Implementacion conexión a ORACLE
    /// </summary>
    internal class ConexionOracle:IBasedatosConexion
    {
        private readonly string _cadenaConexion;

        /// <summary>
        /// Inicializa una nueva instancia de ConexionOracle.
        /// </summary>
        public ConexionOracle(string cadenaConexion)
        {
            _cadenaConexion = cadenaConexion;
        }

        /// <summary>
        /// Método crea comando SQL
        /// </summary>
        /// <param name="comando"></param>
        /// <param name="conexion"></param>
        /// <returns>IDbCommand</returns>
        public DbCommand CrearComando(string comando, DbConnection conexion)
        {
            return new OracleCommand(comando, (OracleConnection)conexion);
        }

        /// <summary>
        /// Método para crear la conexión a la BD
        /// </summary>
        /// <returns>IDbConnection</returns>
        public DbConnection CrearConexion()
        {
            return new OracleConnection(_cadenaConexion);
        }

        /// <summary>
        /// Método para crear parámetros
        /// </summary>
        /// <param name="nombre"></param>
        /// <param name="valor"></param>
        /// <returns>IDbDataParameter</returns>
        public IDbDataParameter CrearParametro(string nombre, object valor, int? tamano = null)
        {
            OracleParameter parametro = new OracleParameter(nombre, valor ?? DBNull.Value);

            if (tamano.HasValue)
            {
                parametro.Size = tamano.Value;
            }
            return parametro;
        }

        /// <summary>
        /// Método para crear parametros de salida
        /// </summary>
        /// <param name="nombre"></param>
        /// <param name="dbType"></param>
        /// <returns>IDbDataParameter</returns>
        public IDbDataParameter CrearParametroSalida(string nombre, DbType dbType, int tamano = 0)
        {
            return new OracleParameter(nombre, ConvertirDbTypeASqlDbType(dbType))
            {
                Direction = ParameterDirection.Output,
                Size = tamano
            };
        }

        /// <summary>
        /// Conversion de tipos a SQL
        /// </summary>
        /// <param name="dbType"></param>
        /// <returns>SqlDbType</returns>
        private OracleDbType ConvertirDbTypeASqlDbType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.Int32: return OracleDbType.Int32;
                case DbType.String: return OracleDbType.Varchar2;
                case DbType.DateTime: return OracleDbType.Date;
                case DbType.Decimal: return OracleDbType.Decimal;
                default: return OracleDbType.Varchar2;
            }
        }
    }
}

using MUFG_T24.Interfaces_utilidades;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.Common;

namespace MUFG_T24.Conexiones
{
    /// <summary>
    /// Implementacion conexión MySQL
    /// </summary>
    public class ConexionMySql:IBasedatosConexion
    {
        private readonly string _cadenaConexion;

        /// <summary>
        /// Inicializa una nueva instancia de ConexionMySql.
        /// </summary>
        public ConexionMySql(string cadenaConexion)
        {
            _cadenaConexion = cadenaConexion;
        }

        /// <summary>
        /// Método crea comando SQL
        /// </summary>
        /// <param name="comando"></param>
        /// <param name="conexion"></param>
        /// <returns>IDbCommand</returns>
        public DbCommand CrearComando(string comando, DbConnection conexion)
        {
            return new MySqlCommand(comando, (MySqlConnection)conexion);
        }

        /// <summary>
        /// Método para crear la conexión a la BD
        /// </summary>
        /// <returns>IDbConnection</returns>
        public DbConnection CrearConexion()
        {
            return new MySqlConnection(_cadenaConexion);
        }

        /// <summary>
        /// Método para crear parámetros
        /// </summary>
        /// <param name="nombre"></param>
        /// <param name="valor"></param>
        /// <returns>IDbDataParameter</returns>
        public IDbDataParameter CrearParametro(string nombre, object valor, int? tamano)
        {
            MySqlParameter parametro = new MySqlParameter(nombre, valor ?? DBNull.Value);

            if (tamano.HasValue)
            {
                parametro.Size = tamano.Value;
            }
            return parametro;
        }

        /// <summary>
        /// Método para crear parametros de salida
        /// </summary>
        /// <param name="nombre"></param>
        /// <param name="dbType"></param>
        /// <returns>IDbDataParameter</returns>
        public IDbDataParameter CrearParametroSalida(string nombre, DbType dbType, int tamano=0)
        {
            return new MySqlParameter(nombre, ConvertirDbTypeASqlDbType(dbType))
            {
                Direction = ParameterDirection.Output,
                Size = tamano
            };
        }

        /// <summary>
        /// Conversion de tipos a SQL
        /// </summary>
        /// <param name="dbType"></param>
        /// <returns>SqlDbType</returns>
        private MySqlDbType ConvertirDbTypeASqlDbType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.Int32: return MySqlDbType.Int32;
                case DbType.String: return MySqlDbType.VarChar;
                case DbType.DateTime: return MySqlDbType.DateTime;
                case DbType.Decimal: return MySqlDbType.Decimal;
                default: return MySqlDbType.VarChar;
            }
        }
    }
}

using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Clase para envio de alertas en BD
    /// </summary>
    public class Alertas : RepositorioMaestro, IAlertas
    {
        /// <summary>
        /// Inicializa una nueva instancia de Alertas.
        /// </summary>
        public Alertas(
            BasedatosProveedor proveedor,
            NombreBasedatos nombreBasedatos,
            BasedatosProveedor? proveedorVistasT24,
            NombreBasedatos? nombreBasedatosVistasT24)
            : base(proveedor, nombreBasedatos, proveedorVistasT24, nombreBasedatosVistasT24)
        { }

        /// <summary>
        /// Inserta una nueva alerta en el sistema
        /// </summary>
        /// <param name="alerta"></param>
        /// <returns>true/false</returns>
        public Task<bool> InsertarAlerta(AlertaDTO alerta)
        {
            return InsertarAlerta(alerta, CancellationToken.None);
        }

        /// <summary>
        /// Inserta una nueva alerta en el sistema a partir de valores simples.
        /// </summary>
        public Task<bool> InsertarAlerta(
            string folio,
            string tipoOperacion,
            string servicio,
            string claveAviso,
            string descripcion,
            int? pId,
            string nombreProceso,
            Exception exception)
        {
            var alerta = new AlertaDTO
            {
                Folio = folio,
                TipoOPeracion = tipoOperacion,
                Servicio = servicio,
                ClaveAviso = claveAviso,
                Descipcion = descripcion,
                PId = pId,
                NombreProceso = nombreProceso,
                Exception = exception
            };

            return InsertarAlerta(alerta, CancellationToken.None);
        }

        /// <summary>
        /// Inserta una nueva alerta en el sistema a partir de valores simples.
        /// </summary>
        public Task<bool> InsertarAlerta(
            string folio,
            string tipoOperacion,
            string servicio,
            string claveAviso,
            string descripcion,
            int? pId,
            string nombreProceso,
            Exception exception,
            CancellationToken ct)
        {
            var alerta = new AlertaDTO
            {
                Folio = folio,
                TipoOPeracion = tipoOperacion,
                Servicio = servicio,
                ClaveAviso = claveAviso,
                Descipcion = descripcion,
                PId = pId,
                NombreProceso = nombreProceso,
                Exception = exception
            };

            return InsertarAlerta(alerta, ct);
        }

        /// <summary>
        /// Inicializa una nueva instancia de InsertarAlerta.
        /// </summary>
        public async Task<bool> InsertarAlerta(AlertaDTO alerta, CancellationToken ct)
        {
            try
            {
                int pidSeguro = alerta.PId.HasValue ? alerta.PId.Value : 0;

                ct.ThrowIfCancellationRequested();

                if (alerta == null)
                    alerta = new AlertaDTO();

                if (pidSeguro < 0)
                    pidSeguro = 0;

                string lsMensajeAlerta;
                string lsDescripcion;

                // Valida y limpia descripcion
                if (string.IsNullOrEmpty(alerta.Descipcion))
                {
                    alerta.Descipcion = "";
                }

                lsDescripcion = alerta.Descipcion.Trim();

                if (lsDescripcion.Length > 50)
                {
                    lsDescripcion = lsDescripcion.Substring(0, 50);
                }

                // Preparar mensaje de alerta
                if (alerta.Exception != null)
                {
                    lsMensajeAlerta = alerta.Exception.ToString()
                        .Replace(Convert.ToChar(10).ToString(), "")
                        .Replace(Convert.ToChar(13).ToString(), "");

                    // LOG
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Inserta_alerta|{lsMensajeAlerta}");
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(
                        $" {ConstantesClaveAviso.Error} | inserta_alerta | Error durante el proceso. Descripción: ' " +
                        $"{lsDescripcion}', excepción: {lsMensajeAlerta}", "Mufg");
                }

                // Ejecuta el INSERT usando el metodo en RepositorioMaestro
                StoredProcedureResult spResult = await EjecutarStoredProceduredNonQuery(EnumNombreSp.SP_INSERTA_ALERTA, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", alerta.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoOperacion", alerta.TipoOPeracion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@servicio", alerta.Servicio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cveError", alerta.ClaveAviso));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@descripcion", alerta.Descipcion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@pId", pidSeguro));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreProceso", alerta.NombreProceso));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@excepcion", alerta.Exception?.ToString() ?? string.Empty));

                    cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@exito", DbType.Boolean));
                }, ct);

                return spResult.Exito;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Inserta_alerta|{ex.Message}");

                if (alerta?.Exception != null)
                {
                    ExceptionDispatchInfo.Capture(alerta.Exception).Throw();
                }

                throw;
            }
        }

    }
}

using MUFG_T24.Modelos;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.Interfaces_utilidades
{
    /// <summary>
    /// Contrato para IAlertas.
    /// </summary>
    public interface IAlertas
    {
        Task<bool> InsertarAlerta(AlertaDTO alerta);
        Task<bool> InsertarAlerta(AlertaDTO alerta, CancellationToken ct);
        Task<bool> InsertarAlerta(
            string folio,
            string tipoOperacion,
            string servicio,
            string claveAviso,
            string descripcion,
            int? pId,
            string nombreProceso,
            Exception exception);
    }
}

using MUFG_T24.Core.Modelos.Logging;
using MUFG_T24.Enums_utilidades;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.Infraestructura.Logging
{
    /// <summary>
    /// Representa FileLogger.
    /// </summary>
    public sealed class FileLogger : IDisposable
    {
        private LoggerOptions _opt;
        private bool _initialized;

        private BlockingCollection<string> _queue;
        private CancellationTokenSource _writerCts;
        private Task _writerTask;
        private const string ValorEnmascarado = "***Enmascarado***";

        private static readonly string[] TokensSensibles = new[]
        {
            "PASSWORD", "PASS", "PWD", "SECRET", "TOKEN", "KEY", "CERT", "CERTIFICATE", "SSL",
            "USER", "USERNAME", "EMAIL", "MAIL", "HOST", "IP", "URI", "URL", "ENDPOINT",
            "SERVER", "SERVIDOR", "SFTP", "MQ", "HSM", "ACCOUNT", "CUENTA", "CONTRATO"
        };

        private static readonly HashSet<string> ClavesSensiblesExactas = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "IP_SERVIDOR_VALIDACION", "IP_SERVIDOR_TAS", "IP_SERVIDOR_VALIDACION_CONCI", "HSM_SERVIDOR", "HSM_SERVIDOR_UP",
            "HSM_SERVIDOR_ANT", "CONECTOR_CLIENTES", "SPEI_CEP_IP", "SPEI_IP", "MQ.SWF_IN.HOST", "MQ.SWF_OUT.HOST",
            "MQ.SWF_IN.HOST_MX", "MQ.SWF_OUT_GCMS.HOST", "REMOTE_HOST_NAME_GPH", "SSO_SERVIDOR", "CMS_TRASPASO_USER",
            "CMS_ASIGNATRASPASO_USER", "ADMON_PERMISOS_SITIO_BACK", "URI_BBVA_GENERATE_TSEC", "URI_BBVA_PAGO_CIE",
            "SWF_GETPACS008DATA", "SWF_GETPACS009DATA", "SWF_GET_MESSAGE_HEADERS_XAMH", "SWF_VALIDATE_MESSAGE_MX",
            "SPID_WEBSERVICE_URL", "RSA_WS_AUTH", "RSA_WS_SYNC", "RSA_WS_ADMIN", "FOREX_URL_MAIL", "TMD_URL_MAIL",
            "WST24_MW_ENDPOINT", "WS_Reversa_EndPoint", "ACCT_BALANCE_API_ENDPOINT", "API_REJECTION_TOKEN_URL", "API_REJECTION_API_URL",
            "COMM_API_URL", "RUTA_P12_BBVA", "DIGSIG_CERT_NUMSER", "SSH_PRIVATE_KEY_PATH_GPH", "MQ.SWF_OUT.SSL_KEYREPOSITORY",
            "MQ.SWF_IN.SSL_KEYREPOSITORY", "MQ.SWF_IN.SSL_KEYREPOSITORY_MX", "MQ.SWF_OUT_GCMS.SSL_KEYREPOSITORY", "RUTA_LLAVES_GPH",
            "MAIL_SOCKET_TO", "MAIL_SOCKET_CC", "MAIL_SOCKET_CEP_TO", "AVISO_BENEFICIARIOS_BCC", "AVISO_REGISTRO_BENEFICIARIOS_BCC",
            "MAIL_ERROR", "MAIL_ERROR_CMS", "TMD_CC_EMAIL", "TMD_TO_MAIL", "FOREX_TO_MAIL", "FOREX_CC_MAIL", "FOREX_FROM_MAIL",
            "TMD_FROM_EMAIL", "REPLICAS_EMAIL_TO", "REPLICAS_EMAIL_CC", "REPLICAS_EMAIL_FROM", "RPA_SOBREGIRO_mailTo",
            "RPA_SOBREGIRO_mailCc", "RPA_SOBREGIRO_mailBcc", "RPA_CASHPOOLING_mailTo", "RPA_CASHPOOLING_mailCc",
            "RPA_CASHPOOLING_mailBcc", "RPA_CTASFAD_mailTo", "RPA_CTASFAD_mailCc", "RPA_CTASFAD_mailBcc", "RPA_FIELES_mailTo",
            "RPA_FIELES_mailCc", "RPA_FIELES_mailBcc", "NotifOtro_mailTo", "NotifOtro_mailCc", "NotifOtro_mailBCc", "SALDOS_EMAIL_NOTIF",
            "SALDOS_EMAIL_SENDER", "SWF_OUT_ETL_MAIL_ERROR", "SWF_OUT_ETL_CC_MAIL_ERROR", "MAIL_ARCHIVOSSALIDA", "GPH_DESCARGA_EMAIL_ERROR_TO",
            "GPH_DESCARGA_EMAIL_ERROR_CC", "GPH_DESCARGA_EMAIL_ERROR_FROM", "GPH_ETL_EMAIL_ERROR_TO", "GPH_ETL_EMAIL_ERROR_CC",
            "GPH_ETL_EMAIL_FROM", "REPLICAS_EMAIL_ERROR_TO", "REPLICAS_EMAIL_ERROR_CC", "MAIL_PSG", "USUARIOS_EMAIL_FROM",
            "WST24_MW_USERNAME", "WST24_MW_USERNAME_AUT", "WST24_MW_USERNAME_REG", "WST24_MW_USERNAME_G_AUT", "WST24_MW_USERNAME_SWF_MX",
            "CALYPSO_MQ_USER", "FCM_MQ_USER", "API_REJECTION_USER", "USER_SFTP_GPH", "MQ.SWF_OUT.USER_ID", "MQ.SWF_IN.USER_ID",
            "MQ.SWF_IN.USER_ID_MX", "MQ.SWF_OUT_GCMS.USER_ID", "BDTT_WEBSERVICE_OPERADOR", "USUARIOS_USER_REGISTRA_BAJA_ALTA",
            "LUM_MINDS_FUNCIONARIO", "PASSWORD_P12_BBVA", "DIGSIG_CERT_PSW", "WST24_MW_PASSWORD", "WST24_MW_PASSWORD_AUT",
            "WST24_MW_PASSWORD_REG", "WST24_MW_PASSWORD_G_AUT", "WST24_MW_PASSWORD_SWF_MX", "CALYPSO_MQ_PSW", "FCM_MQ_PSW",
            "API_REJECTION_PASSWORD", "PASSWORD_SFTP_GPH", "SSH_PRIVATE_KEY_PASS_GPH", "MQ.SWF_OUT.PASSWORD", "MQ.SWF_IN.PASSWORD",
            "MQ.SWF_IN.PASSWORD_MX", "MQ.SWF_OUT_GCMS.PASSWORD", "RSA_CADENA", "CLAVE_HSM_PASSWORD_P12_BBVA", "CLAVE_HSM_LLAVE_DIGITAL_BBVA",
            "KCV_HSM_PASSWORD_P12_BBVA", "KCV_HSM_LLAVE_DIGITAL_BBVA", "HSM_LMK17", "HSM_CS_LMK17", "HSM_LMK17_ANT", "HSM_CS_LMK17_ANT",
            "LLAVE_DIGITAL_BBVA", "SSH_HOST_KEY_GPH", "MQ.SWF_OUT.CERTIFICATE_LABEL", "MQ.SWF_IN.CERTIFICATE_LABEL",
            "MQ.SWF_IN.CERTIFICATE_LABEL_MX", "MQ.SWF_OUT_GCMS.CERTIFICATE_LABEL", "DIGSIG_CERT_NUMSER_KAR", "NUMERO_CONTRATO_BBVA",
            "NUMERO_CUENTA_MUFG", "NUMERO_CUENTA_BBVA", "WST24_MW_CreditAccount_PR", "SWF_USERS_ASR"
        };

        /// <summary>
        /// Inicializa una nueva instancia de Inicializar.
        /// </summary>
        public void Inicializar(LoggerOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.Ruta))
                throw new ArgumentException("Ruta no puede ser vacío.", nameof(options.Ruta));

            var dir = options.Ruta.Trim();
            if (!dir.EndsWith(Path.DirectorySeparatorChar.ToString()))
                dir += Path.DirectorySeparatorChar;

            options.Ruta = dir;
            Directory.CreateDirectory(dir);

            _opt = options;

            _queue = new BlockingCollection<string>(boundedCapacity: options.MaxQueueItems);
            _writerCts = new CancellationTokenSource();
            _writerTask = Task.Factory.StartNew(
                () => WriterLoop(_writerCts.Token),
                _writerCts.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);

            _initialized = true;
        }

        /// <summary>
        /// Inicializa una nueva instancia de Enqueue.
        /// </summary>
        private void Enqueue(string level, string message, Exception ex, bool masked = false, string textoEnmascarar = null)
        {
            if (!_initialized) return;
            if (string.IsNullOrWhiteSpace(message) && ex == null) return;

            try
            {
                if (!string.IsNullOrWhiteSpace(textoEnmascarar))
                    message = EnmascararMensaje(message, textoEnmascarar);
                else if (masked)
                    message = EnmascararMensaje(message, null);

                message = EnmascararPorClavesSensibles(message);

                var line = FormatoLinea(level, message, ex);

                // Si la cola está llena, descartas el log (no bloqueas el servicio).
                // Alternativa: TryAdd con timeout pequeño.
                if (!_queue.TryAdd(line))
                {
                    // fallback síncrono (último recurso)
                    EscribirDirecto(line);
                }
            }
            catch
            {
                // swallow: logging nunca rompe el servicio
            }
        }

        private string EnmascararMensaje(string message, string textoEnmascarar)
        {
            if (string.IsNullOrWhiteSpace(message))
                return ValorEnmascarado;

            if (!string.IsNullOrWhiteSpace(textoEnmascarar))
                return message.Replace(textoEnmascarar, ValorEnmascarado);

            int indiceIgual = message.LastIndexOf("=", StringComparison.Ordinal);
            if (indiceIgual < 0)
                return ValorEnmascarado;

            return message.Substring(0, indiceIgual + 1).TrimEnd() + " " + ValorEnmascarado;
        }

        private string EnmascararPorClavesSensibles(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
                return message;

            // Patrón: Parámetro 'CLAVE' = 'VALOR'
            string texto = Regex.Replace(
                message,
                "(Parámetro\\s*'(?<key>[^']+)'\\s*=\\s*')(?<val>[^']*)(')",
                m => EsClaveSensible(m.Groups["key"].Value)
                    ? m.Groups[1].Value + ValorEnmascarado + m.Groups[3].Value
                    : m.Value,
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

            // Patrón: CLAVE=VALOR o CLAVE = 'VALOR'
            texto = Regex.Replace(
                texto,
                "(?<prefix>(^|[|,;\\s])(?<key>[A-Za-z0-9._-]+)\\s*=\\s*)(?<quote>['\"]?)(?<val>[^|,;\\r\\n'\"]*)(?<quote2>['\"]?)",
                m =>
                {
                    string clave = m.Groups["key"].Value;
                    if (!EsClaveSensible(clave))
                        return m.Value;

                    string quote = m.Groups["quote"].Value;
                    string quote2 = m.Groups["quote2"].Value;
                    if (string.IsNullOrEmpty(quote2)) quote2 = quote;

                    return m.Groups["prefix"].Value + quote + ValorEnmascarado + quote2;
                },
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

            return texto;
        }

        private static bool EsClaveSensible(string clave)
        {
            if (string.IsNullOrWhiteSpace(clave))
                return false;

            string normalizada = clave.Trim();
            if (ClavesSensiblesExactas.Contains(normalizada))
                return true;

            string[] partes = Regex.Split(normalizada.ToUpperInvariant(), "[^A-Z0-9]+")
                ?? new string[0];

            foreach (string parte in partes)
            {
                if (string.IsNullOrWhiteSpace(parte))
                    continue;

                for (int i = 0; i < TokensSensibles.Length; i++)
                {
                    if (string.Equals(parte, TokensSensibles[i], StringComparison.Ordinal))
                        return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Inicializa una nueva instancia de WriterLoop.
        /// </summary>
        private void WriterLoop(CancellationToken ct)
        {
            try
            {
                foreach (var line in _queue.GetConsumingEnumerable(ct))
                {
                    try
                    {
                        var baseName = FormatoNombre();
                        var filePath = RealizarRotacion(baseName);
                        string mutexName = $"Local\\MUFG_LOG_{filePath.GetHashCode()}";

                        bool mutexTomado = false;
                        using (var mutex = new Mutex(false, mutexName))
                        {
                            try
                            {
                                try
                                {
                                    mutexTomado = mutex.WaitOne(200);
                                }
                                catch (AbandonedMutexException)
                                {
                                    mutexTomado = true;
                                }

                                if (!mutexTomado)
                                    continue;

                                File.AppendAllText(filePath, line + Environment.NewLine, Encoding.UTF8);
                            }
                            catch
                            {
                                // swallow
                            }
                            finally
                            {
                                if (mutexTomado)
                                    mutex.ReleaseMutex();
                            }
                        }
                    }
                    catch
                    {
                        // swallow: writer no rompe proceso
                    }
                }
            }
            catch
            {
                // swallow global
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de FormatoLinea.
        /// </summary>
        private string FormatoLinea(string level, string message, Exception ex)
        {
            string ts = DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
            const string sep = "|";

            if (ex == null)
                return string.IsNullOrWhiteSpace(level) ? $"{ts}{sep}{message}" : $"{ts}{sep}{level}{sep}{message}";

            var exText = ex.ToString();
            if (string.IsNullOrWhiteSpace(message))
                return string.IsNullOrWhiteSpace(level) ? $"{ts}{sep}{exText}" : $"{ts}{sep}{level}{sep}{exText}";

            return string.IsNullOrWhiteSpace(level) ? $"{ts}{sep}{message}{sep}{exText}" : $"{ts}{sep}{level}{sep}{message}{sep}{exText}";
        }

        /// <summary>
        /// Inicializa una nueva instancia de FormatoNombre.
        /// </summary>
        private string FormatoNombre()
        {
            string prefix = string.IsNullOrWhiteSpace(_opt.NombreArchivo) ? "LOG" : _opt.NombreArchivo.Trim();
            string date = DateTime.Now.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            return Path.Combine(_opt.Ruta, $"{prefix}_{date}");
        }

        /// <summary>
        /// Inicializa una nueva instancia de RealizarRotacion.
        /// </summary>
        private string RealizarRotacion(string baseName)
        {
            long maxBytes = (long)_opt.TamanoMaximoKB * 1024;
            const string ext = ".txt";

            for (int i = 1; ; i++)
            {
                string candidate = $"{baseName}_{i}{ext}";

                if (!File.Exists(candidate))
                    return candidate;

                var info = new FileInfo(candidate);
                if (info.Length < maxBytes)
                    return candidate;
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de EscribirDirecto.
        /// </summary>
        private void EscribirDirecto(string line)
        {
            try
            {
                var baseName = FormatoNombre();
                var filePath = RealizarRotacion(baseName);

                string mutexName = $"Local\\MUFG_LOG_{filePath.GetHashCode()}";
                bool mutexTomado = false;

                using (var mutex = new Mutex(false, mutexName))
                {
                    try
                    {
                        try
                        {
                            mutexTomado = mutex.WaitOne(100);
                        }
                        catch (AbandonedMutexException)
                        {
                            mutexTomado = true;
                        }

                        if (!mutexTomado)
                            return;

                        File.AppendAllText(filePath, line + Environment.NewLine, Encoding.UTF8);
                    }
                    catch
                    {
                        // swallow
                    }
                    finally
                    {
                        if (mutexTomado)
                            mutex.ReleaseMutex();
                    }
                }
            }
            catch
            {
                // último nivel
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de Write.
        /// </summary>
        public void Write(string message) => Write(message, false);

        /// <summary>
        /// Inicializa una nueva instancia de Write.
        /// </summary>
        public void Write(string message, bool masked) => Enqueue("", message, null, masked);

        /// <summary>
        /// Inicializa una nueva instancia de Write.
        /// </summary>
        public void Write(string message, string textoEnmascarar) => Enqueue("", message, null, false, textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Info.
        /// </summary>
        public void Info(string message) => Info(message, false);

        /// <summary>
        /// Inicializa una nueva instancia de Info.
        /// </summary>
        public void Info(string message, bool masked) => Enqueue("INFO", message, null, masked);

        /// <summary>
        /// Inicializa una nueva instancia de Info.
        /// </summary>
        public void Info(string message, string textoEnmascarar) => Enqueue("INFO", message, null, false, textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Warn.
        /// </summary>
        public void Warn(string message) => Warn(message, false);

        /// <summary>
        /// Inicializa una nueva instancia de Warn.
        /// </summary>
        public void Warn(string message, bool masked) => Enqueue("WARN", message, null, masked);

        /// <summary>
        /// Inicializa una nueva instancia de Warn.
        /// </summary>
        public void Warn(string message, string textoEnmascarar) => Enqueue("WARN", message, null, false, textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public void Error(string message) => Error(message, false);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public void Error(string message, bool masked) => Enqueue(ConstantesClaveAviso.Error, message, null, masked);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public void Error(string message, string textoEnmascarar) => Enqueue(ConstantesClaveAviso.Error, message, null, false, textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public void Error(Exception ex, string context = null) => Error(ex, context, false);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public void Error(Exception ex, string context, bool masked) => Enqueue(ConstantesClaveAviso.Error, context, ex, masked);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public void Error(Exception ex, string context, string textoEnmascarar) => Enqueue(ConstantesClaveAviso.Error, context, ex, false, textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Dispose.
        /// </summary>
        public void Dispose()
        {
            try
            {
                if (!_initialized) return;

                _queue.CompleteAdding();
                _writerCts.Cancel();

                // No esperamos infinito: el servicio no debe colgarse al detener.
                _writerTask?.Wait(TimeSpan.FromSeconds(_opt.FlushSecondsOnStop));
            }
            catch
            {
                // swallow
            }
            finally
            {
                _writerCts?.Dispose();
                _queue?.Dispose();
                _initialized = false;
            }
        }

    }

}

using MUFG_T24.Core.Modelos.Logging;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;

namespace MUFG_T24.Infraestructura.Logging
{
    /// <summary>
    /// Representa Logging.
    /// </summary>
    public static class Logging
    {
        /// <summary>
        /// Inicializa una nueva instancia de FileLogger.
        /// </summary>
        private static readonly FileLogger _logger = new FileLogger();

        /// <summary>
        /// Ejecuta AsyncLocal.
        /// </summary>
        private static readonly AsyncLocal<LoggingContext> _context = new AsyncLocal<LoggingContext>();
        private static int _workerSequence;

        /// <summary>
        /// Inicializa una nueva instancia de Init.
        /// </summary>

        public static void Init(MUFG_T24.Comunes.RutasConfiguracion rutas, string filePrefix = "Mufg", int maxMb = 100)
        {
            const int kbPerMb = 1024;

            _logger.Inicializar(new LoggerOptions
            {
                Ruta = rutas.MUFG_Servicios_Path_Log,
                NombreArchivo = ObtenerNombreArchivo(rutas.MUFG_Servicios_Path_Log, filePrefix),
                TamanoMaximoKB = maxMb * kbPerMb
            });
        }

        /// <summary>
        /// Inicializa una nueva instancia de Init a partir de la ruta de log directamente.
        /// </summary>
        public static void Init(string pathLog, string filePrefix = "Mufg", int maxMb = 100)
        {
            Init(new MUFG_T24.Comunes.RutasConfiguracion { MUFG_Servicios_Path_Log = pathLog }, filePrefix, maxMb);
            Logging.Write("INFO|OnStart|Leyendo archivo de configuración.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerNombreArchivo.
        /// </summary>
        private static string ObtenerNombreArchivo(string rutaLog, string filePrefix)
        {
            string nombre = null;

            if (!string.IsNullOrWhiteSpace(filePrefix) && !string.Equals(filePrefix, "Mufg", StringComparison.OrdinalIgnoreCase))
            {
                nombre = filePrefix;
            }
            else if (!string.IsNullOrWhiteSpace(rutaLog))
            {
                string rutaSinSeparador = rutaLog.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                nombre = Path.GetFileName(rutaSinSeparador);
            }

            return string.IsNullOrWhiteSpace(nombre) ? "LOG" : nombre.Trim().ToUpperInvariant();
        }

        /// <summary>
        /// Inicializa una nueva instancia de BeginWorkerScope.
        /// </summary>
        public static IDisposable BeginWorkerScope()
        {
            var previous = _context.Value;
            _context.Value = new LoggingContext(CrearWorkerId(), null, null);

            return new ContextScope(previous);
        }

        /// <summary>
        /// Inicializa una nueva instancia de BeginOperacionScope.
        /// </summary>
        public static IDisposable BeginOperacionScope(string operacion, string folio)
        {
            var previous = _context.Value;
            string workerId = previous == null ? null : previous.WorkerId;

            _context.Value = new LoggingContext(workerId, LimpiarValor(operacion), LimpiarValor(folio));

            return new ContextScope(previous);
        }

        /// <summary>
        /// Inicializa una nueva instancia de Write.
        /// </summary>
        public static void Write(string msg) => Write(msg, false);

        /// <summary>
        /// Escribe un log legado detectando nivel por prefijo del mensaje.
        /// </summary>
        public static void WriteLegacy(string msg) => WriteLegacy(msg, false);

        /// <summary>
        /// Escribe un log legado detectando nivel por prefijo del mensaje.
        /// </summary>
        public static void WriteLegacy(string msg, bool masked)
        {
            if (string.IsNullOrWhiteSpace(msg))
            {
                Write(msg, masked);
                return;
            }

            string trimmed = msg.TrimStart();
            if (trimmed.StartsWith("INFO|", StringComparison.OrdinalIgnoreCase))
            {
                Info(msg, masked);
                return;
            }

            if (trimmed.StartsWith("WARN|", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("WARNING|", StringComparison.OrdinalIgnoreCase))
            {
                Warn(msg, masked);
                return;
            }

            if (trimmed.StartsWith("ERROR|", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("CRITICO|", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("CRITITICAL|", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("CRITICAL|", StringComparison.OrdinalIgnoreCase))
            {
                Error(msg, masked);
                return;
            }

            Write(msg, masked);
        }

        /// <summary>
        /// Escribe un log legado aplicando enmascaramiento por texto específico.
        /// </summary>
        public static void WriteLegacyMasked(string msg, string textoEnmascarar)
        {
            if (string.IsNullOrWhiteSpace(msg))
            {
                Write(msg, textoEnmascarar);
                return;
            }

            string trimmed = msg.TrimStart();
            if (trimmed.StartsWith("INFO|", StringComparison.OrdinalIgnoreCase))
            {
                Info(msg, textoEnmascarar);
                return;
            }

            if (trimmed.StartsWith("WARN|", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("WARNING|", StringComparison.OrdinalIgnoreCase))
            {
                Warn(msg, textoEnmascarar);
                return;
            }

            if (trimmed.StartsWith("ERROR|", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("CRITICO|", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("CRITITICAL|", StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith("CRITICAL|", StringComparison.OrdinalIgnoreCase))
            {
                Error(msg, textoEnmascarar);
                return;
            }

            Write(msg, textoEnmascarar);
        }

        /// <summary>
        /// Sobrecarga legado; el nombre de archivo ya no aplica con FileLogger.
        /// </summary>
        public static void WriteLegacy(string msg, string nombreArchivo) => WriteLegacy(msg, false);

        /// <summary>
        /// Sobrecarga legado; el nombre de archivo ya no aplica con FileLogger.
        /// </summary>
        public static void WriteLegacy(string msg, string nombreArchivo, bool masked) => WriteLegacy(msg, masked);

        /// <summary>
        /// Inicializa una nueva instancia de Write.
        /// </summary>
        public static void Write(string msg, bool masked) => _logger.Write(AgregarContexto(msg), masked);

        /// <summary>
        /// Inicializa una nueva instancia de Write.
        /// </summary>
        public static void Write(string msg, string textoEnmascarar) => _logger.Write(AgregarContexto(msg), textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Info.
        /// </summary>
        public static void Info(string msg) => Info(msg, false);

        /// <summary>
        /// Inicializa una nueva instancia de Info.
        /// </summary>
        public static void Info(string msg, bool masked) => _logger.Info(AgregarContexto(msg), masked);

        /// <summary>
        /// Inicializa una nueva instancia de Info.
        /// </summary>
        public static void Info(string msg, string textoEnmascarar) => _logger.Info(AgregarContexto(msg), textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Warn.
        /// </summary>
        public static void Warn(string msg) => Warn(msg, false);

        /// <summary>
        /// Inicializa una nueva instancia de Warn.
        /// </summary>
        public static void Warn(string msg, bool masked) => _logger.Warn(AgregarContexto(msg), masked);

        /// <summary>
        /// Inicializa una nueva instancia de Warn.
        /// </summary>
        public static void Warn(string msg, string textoEnmascarar) => _logger.Warn(AgregarContexto(msg), textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public static void Error(string msg) => Error(msg, false);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public static void Error(string msg, bool masked) => _logger.Error(AgregarContexto(msg), masked);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public static void Error(string msg, string textoEnmascarar) => _logger.Error(AgregarContexto(msg), textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public static void Error(System.Exception ex, string ctx = null) => Error(ex, ctx, false);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public static void Error(System.Exception ex, string ctx, bool masked) => _logger.Error(ex, AgregarContexto(ctx), masked);

        /// <summary>
        /// Inicializa una nueva instancia de Error.
        /// </summary>
        public static void Error(System.Exception ex, string ctx, string textoEnmascarar) => _logger.Error(ex, AgregarContexto(ctx), textoEnmascarar);

        /// <summary>
        /// Inicializa una nueva instancia de WriteConContexto.
        /// </summary>
        public static void WriteConContexto(string contexto, string msg)
        {
            if (string.IsNullOrWhiteSpace(contexto))
            {
                Write(msg);
                return;
            }

            if (string.IsNullOrWhiteSpace(msg))
            {
                Write(contexto);
                return;
            }

            Write(contexto + "| " + msg);
        }

        /// <summary>
        /// Inicializa una nueva instancia de Shutdown.
        /// </summary>
        public static void Shutdown() => _logger.Dispose();

        /// <summary>
        /// Inicializa una nueva instancia de AgregarContexto.
        /// </summary>
        private static string AgregarContexto(string msg)
        {
            string contexto = FormatearContexto(_context.Value);

            if (string.IsNullOrWhiteSpace(contexto))
                return msg;

            if (string.IsNullOrWhiteSpace(msg))
                return contexto;

            return contexto + "| " + msg;
        }

        /// <summary>
        /// Inicializa una nueva instancia de FormatearContexto.
        /// </summary>
        private static string FormatearContexto(LoggingContext context)
        {
            if (context == null)
                return null;

            var partes = new List<string>();

            if (!string.IsNullOrWhiteSpace(context.WorkerId))
                partes.Add("[W:" + context.WorkerId + "]");

            if (!string.IsNullOrWhiteSpace(context.Operacion))
                partes.Add("[OP:" + context.Operacion + "]");

            if (!string.IsNullOrWhiteSpace(context.Folio))
                partes.Add("[Folio:" + context.Folio + "]");

            return partes.Count == 0 ? null : string.Concat(partes);
        }

        /// <summary>
        /// Inicializa una nueva instancia de CrearWorkerId.
        /// </summary>
        private static string CrearWorkerId()
        {
            int sequence = Interlocked.Increment(ref _workerSequence);
            int processId;

            using (var process = Process.GetCurrentProcess())
            {
                processId = process.Id;
            }

            return string.Format(CultureInfo.InvariantCulture, "P{0}-W{1:0000}", processId, sequence);
        }

        /// <summary>
        /// Inicializa una nueva instancia de LimpiarValor.
        /// </summary>
        private static string LimpiarValor(string valor)
        {
            return string.IsNullOrWhiteSpace(valor) ? null : valor.Trim();
        }

        /// <summary>
        /// Representa ContextScope.
        /// </summary>
        private sealed class ContextScope : IDisposable
        {
            private readonly LoggingContext _previous;
            private bool _disposed;

            /// <summary>
            /// Inicializa una nueva instancia de ContextScope.
            /// </summary>
            public ContextScope(LoggingContext previous)
            {
                _previous = previous;
            }

            /// <summary>
            /// Inicializa una nueva instancia de Dispose.
            /// </summary>
            public void Dispose()
            {
                if (_disposed) return;

                _disposed = true;
                _context.Value = _previous;
            }
        }

        /// <summary>
        /// Representa LoggingContext.
        /// </summary>
        private sealed class LoggingContext
        {
            /// <summary>
            /// Inicializa una nueva instancia de LoggingContext.
            /// </summary>
            public LoggingContext(string workerId, string operacion, string folio)
            {
                WorkerId = workerId;
                Operacion = operacion;
                Folio = folio;
            }

            public string WorkerId { get; }
            public string Operacion { get; }
            public string Folio { get; }
        }

    }
}

using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Threading.Tasks;

namespace MUFG_T24.Comunes.Helpers
{
    /// <summary>
    /// Clase auxiliar para envio de correos electronicos
    /// </summary>
    public static class CorreoHelper
    {
        /// <summary>
        /// Envia correo electronico
        /// </summary>
        /// <param name="solicitud"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static Task<bool> EnviarCorreo(string[] de, string para, string con_copia, string copia_oculta, string sujeto, string mensaje, string[] archivosAdjuntos = null)
        {
            return EnviarCorreoAsync(new MensajeCorreo
            {
                CorreoOrigen = de != null && de.Length > 0 ? de[0] : null,
                NombreOrigen = null,
                Para = para,
                ConCopia = con_copia,
                CopiaOculta = copia_oculta,
                Asunto = sujeto,
                Mensaje = mensaje,
                ArchivosAdjuntos = archivosAdjuntos
            });
        }

        public static Task<bool> EnviarCorreo(MensajeCorreo solicitud)
        {
            return EnviarCorreoAsync(solicitud);
        }

        /// <summary>
        /// Inicializa una nueva instancia de EnviarCorreoAsync.
        /// </summary>
        public static async Task<bool> EnviarCorreoAsync(MensajeCorreo solicitud)
        {
            if (solicitud == null)
                throw new ArgumentException(nameof(solicitud));

            try
            {
                Parametrizacion parametrizacion = new Parametrizacion();
                Dictionary<string, string>  parametrosSistema = await parametrizacion.ObtenerParametrosSistemaAsync("'SERVIDOR_SMTP', 'PORT_SMTP'");

                using (MailMessage mensaje = new MailMessage())
                {
                    mensaje.From = new MailAddress(
                        solicitud.CorreoOrigen,
                        solicitud.NombreOrigen
                    );

                    AgregarDirecciones(mensaje.To, solicitud.Para);
                    AgregarDirecciones(mensaje.CC, solicitud.ConCopia);
                    AgregarDirecciones(mensaje.Bcc, solicitud.CopiaOculta);

                    mensaje.Subject = solicitud.Asunto;
                    mensaje.Body = solicitud.Mensaje;
                    mensaje.IsBodyHtml = EsHtml(solicitud.Mensaje);

                    AgregarAdjuntos(mensaje, solicitud.ArchivosAdjuntos);

                    System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                    using (SmtpClient cliente = new SmtpClient(parametrosSistema["SERVIDOR_SMTP"], Convert.ToInt32(parametrosSistema["PORT_SMTP"])))
                    {
                        cliente.Timeout = EnumRutasGenerales.Smtp_Tiempo_Espera;
                        cliente.UseDefaultCredentials = true;
                        cliente.EnableSsl = true;
                        cliente.DeliveryMethod = SmtpDeliveryMethod.Network;
                        await cliente.SendMailAsync(mensaje);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                RegistrarError(ex, solicitud);
                return false;
            }
        }

        #region Metodos auxiliares

        /// <summary>
        /// Agrega direcciones de correo a una coleccion
        /// </summary>
        /// <param name="coleccion"></param>
        /// <param name="direcciones"></param>
        private static void AgregarDirecciones(MailAddressCollection coleccion, string direcciones)
        {
            if (string.IsNullOrWhiteSpace(direcciones))
                return;

            char[] separadores = new[] { ',', ';', '|' };

            direcciones
                .Split(separadores, StringSplitOptions.RemoveEmptyEntries)
                .Select(d => d.Trim())
                .Where(d => d.Length > 0)
                .ToList()
                .ForEach(d => coleccion.Add(d));
        }

        /// <summary>
        /// Agregar archivo adjunto al correo
        /// </summary>
        /// <param name="mensaje"></param>
        /// <param name="archivosAdjuntos"></param>
        private static void AgregarAdjuntos(MailMessage mensaje, string[] archivosAdjuntos)
        {
            if (archivosAdjuntos == null || archivosAdjuntos.Length == 0)
            {
                return;
            }

            foreach (string archivo in archivosAdjuntos)
            {
                mensaje.Attachments.Add(new Attachment(archivo));
            }
        }

        /// <summary>
        /// Determina si el mensaje es HTML
        /// </summary>
        /// <returns>true/false</returns>
        private static bool EsHtml(string mensaje)
        {
            return !string.IsNullOrWhiteSpace(mensaje) &&
                mensaje.TrimStart().StartsWith(EnumRutasGenerales.Codigo_Etiqueta_Html, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Registra errores del envio de correo
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="solicitud"></param>
        private static void RegistrarError(Exception ex, MensajeCorreo solicitud)
        {
            //LOGS
            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|Envia Email|Ocurre excepcion: {ex}\n" +
                $"**********************************************************************\n" +
                $"Detalles del correo: \n" +
                $"to: {solicitud.Para} \n" +
                $"cc: {solicitud.ConCopia} \n" +
                $"bcc: {solicitud.CopiaOculta} \n" +
                $"Subject: {solicitud.Asunto} \n" +
                $"IsBodyHtml: {EsHtml(solicitud.Mensaje)}\n" +
                $"Body: {solicitud.Mensaje} \n" +
                $"**********************************************************************"
                , "EnviarCorreo"
            );
        }
        #endregion Metodos auxiliares

    }
}

using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Core.Configuracion;
using System;
using System.Security.Cryptography;
using System.Text;

namespace MUFG_T24.Encriptacion_utilidades
{
    /// <summary>
    /// Representa Encriptacion.
    /// </summary>
    public static class Encriptacion
    {
        private const int AesBlockSizeBytes = 16;

        /// <summary>
        /// Inicializa una nueva instancia de GenerarLlave.
        /// </summary>
        public static string GenerarLlave()
        {
            return LecturaConfguracionGeneral.ObtenerValorGeneral("LLAVE_ENCRIPTADODESPACHADOR");
        }

        /// <summary>
        /// Inicializa una nueva instancia de GenerarLlaveAleatoria.
        /// </summary>
        public static string GenerarLlaveAleatoria()
        {
            using (var aes = Aes.Create())
            {
                aes.KeySize = 256;
                aes.GenerateKey();

                return Convert.ToBase64String(aes.Key);
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de Encrypt.
        /// </summary>
        public static string Encrypt(string plainText, string base64Key)
        {
            byte[] key = ObtenerLlave(base64Key);
            byte[] iv = GenerarVectorInicializacion();
            byte[] inputBytes = Encoding.UTF8.GetBytes(plainText);

            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;

                using (var encryptor = aes.CreateEncryptor(key, iv))
                {
                    byte[] encrypted = encryptor.TransformFinalBlock(inputBytes, 0, inputBytes.Length);
                    byte[] result = new byte[iv.Length + encrypted.Length];

                    Buffer.BlockCopy(iv, 0, result, 0, iv.Length);
                    Buffer.BlockCopy(encrypted, 0, result, iv.Length, encrypted.Length);

                    return Convert.ToBase64String(result);
                }
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de Decrypt.
        /// </summary>
        public static string Decrypt(string cipherText, string base64Key)
        {
            byte[] key = ObtenerLlave(base64Key);
            byte[] cipherBytes = Convert.FromBase64String(cipherText);

            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;

                int ivLength = AesBlockSizeBytes;
                if (cipherBytes.Length <= ivLength)
                {
                    throw new ArgumentException("El texto cifrado no contiene un IV válido.", "cipherText");
                }

                byte[] iv = new byte[ivLength];
                byte[] encrypted = new byte[cipherBytes.Length - ivLength];

                Buffer.BlockCopy(cipherBytes, 0, iv, 0, iv.Length);
                Buffer.BlockCopy(cipherBytes, iv.Length, encrypted, 0, encrypted.Length);

                using (var decryptor = aes.CreateDecryptor(key, iv))
                {
                    byte[] decrypted = decryptor.TransformFinalBlock(encrypted, 0, encrypted.Length);

                    return Encoding.UTF8.GetString(decrypted);
                }
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerLlave.
        /// </summary>
        private static byte[] ObtenerLlave(string base64Key)
        {
            if (string.IsNullOrWhiteSpace(base64Key))
            {
                throw new ArgumentException("La llave de cifrado es requerida.", "base64Key");
            }

            byte[] key = Convert.FromBase64String(base64Key);
            if (key.Length != 16 && key.Length != 24 && key.Length != 32)
            {
                throw new ArgumentException("La llave AES debe tener 16, 24 o 32 bytes en Base64.", "base64Key");
            }

            return key;
        }

        /// <summary>
        /// Inicializa una nueva instancia de GenerarVectorInicializacion.
        /// </summary>
        private static byte[] GenerarVectorInicializacion()
        {
            byte[] iv = new byte[AesBlockSizeBytes];

            using (var random = RandomNumberGenerator.Create())
            {
                random.GetBytes(iv);
            }

            return iv;
        }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MUFG_T24.Interfaces_utilidades
{
    /// <summary>
    /// Contrato para IEncriptacion.
    /// </summary>
    public interface IEncriptacion
    {
        string GenerarLlave();
        string Encrypt(string plainText, string base64Key);
        string Decrypt(string cipherText, string base64Key);
    }
}

using System;

namespace MUFG_T24.Comunes.Helpers
{
    /// <summary>
    /// Clase auxiliar para HTML
    /// </summary>
    public static class HtmlHelper
    {
        #region Funciones Comunes
        /// <summary>
        /// Convierte texto a HTML seguro
        /// </summary>
        /// <param name="texto"></param>
        /// <returns></returns>
        public static string TextoHtml(object texto)
        {
            if (texto == null || texto == DBNull.Value)
                return EnumRutasGenerales.Codigo_Espacio_Html;

            string valor = texto.ToString();
            return string.IsNullOrWhiteSpace(valor) ? EnumRutasGenerales.Codigo_Espacio_Html : valor;
        }

        /// <summary>
        /// Convierte texto a formato moneda
        /// </summary>
        /// <param name="texto"></param>
        /// <returns></returns>
        public static string ImporteHtml(object texto)
        {
            if (texto == null || texto == DBNull.Value)
                return EnumRutasGenerales.Codigo_Espacio_Html;

            if (decimal.TryParse(texto.ToString(), out decimal importe))
                return importe.ToString("C");

            return EnumRutasGenerales.Codigo_Espacio_Html;
        }

        /// <summary>
        /// Convierte fecha a formato HTML
        /// </summary>
        /// <param name="fecha"></param>
        /// <returns></returns>
        public static string FechaHtml(DateTime? fecha)
        {
            return fecha.HasValue
                ? fecha.Value.ToString(EnumRutasGenerales.Formato_Fecha_Html)
                : EnumRutasGenerales.Codigo_Espacio_Html;
        }

        /// <summary>
        /// Convierte fechas string YYYYMMDD a DD/MM/YYYY
        /// </summary>
        /// <param name="fecha"></param>
        /// <returns></returns>
        public static string FechaFormatoFijoHtml(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha) || fecha.Length < 8)
                return EnumRutasGenerales.Codigo_Espacio_Html;

            return $"{fecha.Substring(0, 4)}/{fecha.Substring(4, 2)}/{fecha.Substring(6, 2)}";
        }
        #endregion Funciones Comunes

    }
}

using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using MUFG_T24.Infraestructura.Logging;

namespace MUFG_T24.Sellado_Utilidades
{
    /// <summary>
    /// Proporciona utilidades para cargar una llave privada RSA y generar sellos digitales.
    /// </summary>
    /// <remarks>
    /// Se recomienda registrar <see cref="Sellado"/> con ciclo de vida <c>Transient</c> o <c>Scoped</c>.
    /// No se recomienda <c>Singleton</c> porque la instancia no es thread-safe.
    /// </remarks>
    public sealed class Sellado : IDisposable
    {
        private const string PemHeader = "-----BEGIN RSA PRIVATE KEY-----";
        private const string PemFooter = "-----END RSA PRIVATE KEY-----";
        private const string Sha256Algorithm = "SHA256";

        private const string ErrorArchivoPemNoEncontrado = "No se encontró el archivo PEM especificado.";
        private const string ErrorArchivoCerNoEncontrado = "No se encontró el archivo CER especificado.";
        private const string ErrorPemInvalidoOVacio = "El archivo PEM no contiene una llave privada RSA válida o viene vacío.";
        private const string ErrorNoSePudoObtenerLlavePrivada = "No se pudo obtener la llave privada RSA del archivo PEM.";
        private const string ErrorPemBase64Invalido = "No se pudo decodificar el contenido Base64 del PEM.";
        private const string ErrorDecodeLlavePrivada = "No se pudo decodificar la llave privada RSA desde el PEM.";

        private RSACryptoServiceProvider _rsaCsp;

        /// <summary>
        /// Inicializa una nueva instancia de <see cref="Sellado"/> cargando la llave privada RSA desde un archivo PEM.
        /// </summary>
        /// <param name="archivoPEM">Ruta del archivo PEM que contiene la llave privada RSA.</param>
        public Sellado(string archivoPEM)
        {
            //Se rechaza si el archivo PEM es nulo, vacío o contiene solo espacios en blanco...
            if (string.IsNullOrWhiteSpace(archivoPEM))
                throw new ArgumentNullException(nameof(archivoPEM));

            //Se rechaza si el archivo PEM no existe...
            if (!File.Exists(archivoPEM))
                throw new FileNotFoundException(ErrorArchivoPemNoEncontrado, archivoPEM);

            //Se lee el contenido del archivo PEM y se obtiene la llave privada RSA...
            string pemInfo = File.ReadAllText(archivoPEM);
            byte[] certBuffer = GetBytesFromPEM(pemInfo);

            //Se rechaza si el archivo PEM no contiene una llave privada RSA válida o viene vacío...
            if (certBuffer == null || certBuffer.Length == 0)
                throw new InvalidOperationException(ErrorPemInvalidoOVacio);

            //Se decodifica la llave obtenida del archivo PEM...
            _rsaCsp = DecodeRSAPrivateKey(certBuffer);

            //Se rechaza si no se pudo obtener la llave privada RSA del archivo PEM...
            if (_rsaCsp == null)
                throw new InvalidOperationException(ErrorNoSePudoObtenerLlavePrivada);
        }

        /// <summary>
        /// Obtiene el número de serie de un certificado CER en formato hexadecimal.
        /// </summary>
        /// <param name="archivoCER">Ruta del archivo CER.</param>
        /// <returns>Número de serie en hexadecimal sin separadores.</returns>
        public static string ObtenerNumeroSerie(string archivoCER)
        {
            //Se rechaza si el archivo CER es nulo, vacío o contiene solo espacios en blanco...
            if (string.IsNullOrWhiteSpace(archivoCER))
                throw new ArgumentNullException(nameof(archivoCER));

            //Se rechaza si el archivo CER no existe...
            if (!File.Exists(archivoCER))
                throw new FileNotFoundException(ErrorArchivoCerNoEncontrado, archivoCER);

            //Se carga el certificado CER y se obtiene su número de serie en formato hexadecimal...
            using (var cert = new X509Certificate2(archivoCER))
            {
                byte[] serialNumberArray = cert.GetSerialNumber();
                Array.Reverse(serialNumberArray);
                return BitConverter.ToString(serialNumberArray).Replace("-", string.Empty);
            }
        }

        /// <summary>
        /// Genera el sello digital de la información proporcionada.
        /// </summary>
        /// <param name="informacion">Texto a firmar.</param>
        /// <returns>Firma digital codificada en Base64.</returns>
        public string GenerarSello(string informacion)
        {
            //Rechaza si la información a sellar es nula...
            if (informacion == null)
                throw new ArgumentNullException(nameof(informacion));

            //Rechaza si la instancia de Sellado ha sido cerrada o eliminada...
            if (_rsaCsp == null)
                throw new ObjectDisposedException(nameof(Sellado));

            //Se genera el sello con SHA256 (de la informacion en bytes usando UTF-8)...
            byte[] data = Encoding.UTF8.GetBytes(informacion);
            byte[] firma = _rsaCsp.SignData(data, Sha256Algorithm);
            return Convert.ToBase64String(firma);
        }

        /// <summary>
        /// Extrae y decodifica la sección Base64 de una llave privada RSA en formato PEM.
        /// </summary>
        /// <param name="pemString">Contenido del archivo PEM.</param>
        /// <returns>Arreglo de bytes de la llave privada, o <c>null</c> si el contenido no es válido.</returns>
        private static byte[] GetBytesFromPEM(string pemString)
        {
            //Se rechaza si el contenido del PEM es nulo, vacío o contiene solo espacios en blanco...
            if (string.IsNullOrWhiteSpace(pemString))
                return null;

            //Se rechaza si el contenido del PEM no contiene los encabezados y pies de página esperados...
            int headerIndex = pemString.IndexOf(PemHeader, StringComparison.Ordinal);
            if (headerIndex < 0)
                return null;

            //Se rechaza si el contenido del PEM no contiene los encabezados y pies de página esperados...
            int start = headerIndex + PemHeader.Length;
            int footerIndex = pemString.IndexOf(PemFooter, start, StringComparison.Ordinal);
            if (footerIndex < 0)
                return null;

            //Se rechaza si el contenido entre los encabezados y pies de página es inválido o vacío...
            int contentLength = footerIndex - start;
            if (contentLength <= 0)
                return null;
            
            //Se extrae el contenido entre los encabezados y pies de página...
            string raw = pemString.Substring(start, contentLength);
            var base64Builder = new StringBuilder(raw.Length);

            //Se eliminan los espacios en blanco y se construye la cadena Base64...
            for (int i = 0; i < raw.Length; i++)
            {
                char c = raw[i];
                if (!char.IsWhiteSpace(c))
                    base64Builder.Append(c);
            }

            //Se rechaza si la cadena Base64 resultante es vacía...
            if (base64Builder.Length == 0)
                return null;

            //Retorna el arreglo de bytes decodificado...
            try
            {
                return Convert.FromBase64String(base64Builder.ToString());
            }
            catch (FormatException)
            {
                Logging.Error(ErrorPemBase64Invalido);
                return null;
            }
        }

        /// <summary>
        /// Decodifica una llave privada RSA en formato PKCS#1 DER.
        /// </summary>
        /// <remarks>
        /// Este parseo es manual para compatibilidad con .NET Framework 4.8,
        /// donde no existe una API nativa equivalente a <c>ImportFromPem</c>.
        /// </remarks>
        /// <param name="privkey">Bytes binarios DER de la llave privada.</param>
        /// <returns>Instancia de <see cref="RSACryptoServiceProvider"/> inicializada, o <c>null</c> si la llave no es válida.</returns>
        private static RSACryptoServiceProvider DecodeRSAPrivateKey(byte[] privkey)
        {
            //Se rechaza si la llave privada es nula o vacía...
            if (privkey == null || privkey.Length == 0)
                return null;

            //Se realiza el parseo manual del formato ASN.1 DER de la llave privada RSA...
            byte[] modulus = null;
            byte[] exponent = null;
            byte[] d = null;
            byte[] p = null;
            byte[] q = null;
            byte[] dp = null;
            byte[] dq = null;
            byte[] inverseQ = null;

            using (var mem = new MemoryStream(privkey))
            using (var binr = new BinaryReader(mem))
            {
                byte bt;
                ushort twobytes;
                int elems;

                try
                {
                    twobytes = binr.ReadUInt16();
                    if (twobytes == 0x8130)
                        binr.ReadByte();
                    else if (twobytes == 0x8230)
                        binr.ReadInt16();
                    else
                        return null;

                    twobytes = binr.ReadUInt16();
                    if (twobytes != 0x0102)
                        return null;

                    bt = binr.ReadByte();
                    if (bt != 0x00)
                        return null;

                    elems = GetIntegerSize(binr);
                    if (elems <= 0) return null;
                    modulus = binr.ReadBytes(elems);
                    if (modulus.Length != elems) return null;

                    elems = GetIntegerSize(binr);
                    if (elems <= 0) return null;
                    exponent = binr.ReadBytes(elems);
                    if (exponent.Length != elems) return null;

                    elems = GetIntegerSize(binr);
                    if (elems <= 0) return null;
                    d = binr.ReadBytes(elems);
                    if (d.Length != elems) return null;

                    elems = GetIntegerSize(binr);
                    if (elems <= 0) return null;
                    p = binr.ReadBytes(elems);
                    if (p.Length != elems) return null;

                    elems = GetIntegerSize(binr);
                    if (elems <= 0) return null;
                    q = binr.ReadBytes(elems);
                    if (q.Length != elems) return null;

                    elems = GetIntegerSize(binr);
                    if (elems <= 0) return null;
                    dp = binr.ReadBytes(elems);
                    if (dp.Length != elems) return null;

                    elems = GetIntegerSize(binr);
                    if (elems <= 0) return null;
                    dq = binr.ReadBytes(elems);
                    if (dq.Length != elems) return null;

                    elems = GetIntegerSize(binr);
                    if (elems <= 0) return null;
                    inverseQ = binr.ReadBytes(elems);
                    if (inverseQ.Length != elems) return null;

                    var rsa = new RSACryptoServiceProvider();
                    rsa.PersistKeyInCsp = false;

                    var rsaParams = new RSAParameters
                    {
                        Modulus = modulus,
                        Exponent = exponent,
                        D = d,
                        P = p,
                        Q = q,
                        DP = dp,
                        DQ = dq,
                        InverseQ = inverseQ
                    };

                    try
                    {
                        rsa.ImportParameters(rsaParams);
                        return rsa;
                    }
                    catch
                    {
                        rsa.Dispose();
                        throw;
                    }
                }
                catch
                {
                    Logging.Error(ErrorDecodeLlavePrivada);
                    return null;
                }
                finally
                {
                    if (privkey != null) Array.Clear(privkey, 0, privkey.Length);
                    if (modulus != null) Array.Clear(modulus, 0, modulus.Length);
                    if (exponent != null) Array.Clear(exponent, 0, exponent.Length);
                    if (d != null) Array.Clear(d, 0, d.Length);
                    if (p != null) Array.Clear(p, 0, p.Length);
                    if (q != null) Array.Clear(q, 0, q.Length);
                    if (dp != null) Array.Clear(dp, 0, dp.Length);
                    if (dq != null) Array.Clear(dq, 0, dq.Length);
                    if (inverseQ != null) Array.Clear(inverseQ, 0, inverseQ.Length);
                }
            }
        }

        /// <summary>
        /// Obtiene el tamaño de un entero ASN.1 desde la posición actual del lector binario.
        /// </summary>
        /// <remarks>
        /// Método auxiliar de parseo manual para DER en .NET Framework 4.8.
        /// </remarks>
        /// <param name="binr">Lector binario sobre el flujo DER.</param>
        /// <returns>Tamaño del entero ASN.1, o <c>0</c> si no se puede determinar.</returns>
        private static int GetIntegerSize(BinaryReader binr)
        {
            byte bt;
            byte lowbyte;
            byte highbyte;
            int count;

            //Se realiza el parseo manual del tamaño del entero ASN.1, considerando posibles encabezados y terminadores...
            if (!TryReadByte(binr, out bt) || bt != 0x02)
                return 0;

            if (!TryReadByte(binr, out bt))
                return 0;

            if (bt == 0x81)
            {
                if (!TryReadByte(binr, out bt))
                    return 0;

                count = bt;
            }
            else if (bt == 0x82)
            {
                if (!TryReadByte(binr, out highbyte) || !TryReadByte(binr, out lowbyte))
                    return 0;

                byte[] modint = { lowbyte, highbyte, 0x00, 0x00 };
                count = BitConverter.ToInt32(modint, 0);
            }
            else
                count = bt;

            bool readNonZeroTerminator = false;

            while (TryReadByte(binr, out bt))
            {
                if (bt == 0x00)
                {
                    count -= 1;
                    continue;
                }

                readNonZeroTerminator = true;
                break;
            }

            if (readNonZeroTerminator)
                binr.BaseStream.Seek(-1, SeekOrigin.Current);

            return count;
        }

        /// <summary>
        /// Intenta leer un byte del flujo sin lanzar excepción al alcanzar el fin del stream.
        /// </summary>
        /// <remarks>
        /// Método auxiliar de parseo manual para DER en .NET Framework 4.8.
        /// </remarks>
        /// <param name="binr">Lector binario sobre el flujo DER.</param>
        /// <param name="value">Valor leído cuando el método retorna <c>true</c>.</param>
        /// <returns><c>true</c> si se leyó un byte; en caso contrario, <c>false</c>.</returns>
        private static bool TryReadByte(BinaryReader binr, out byte value)
        {
            if (binr.BaseStream.Position >= binr.BaseStream.Length)
            {
                value = 0;
                return false;
            }

            value = binr.ReadByte();
            return true;
        }

        /// <summary>
        /// Cierra la instancia y libera recursos criptográficos.
        /// </summary>
        public void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Libera los recursos administrados por la instancia.
        /// </summary>
        public void Dispose()
        {
            if (_rsaCsp != null)
            {
                _rsaCsp.Dispose();
                _rsaCsp = null;
            }
        }

    }
}

using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace MUFG_T24.Modelos
{
    //[GeneratedCode("xsd", "2.0.50727.3038")]
    //[Serializable]
    //[DebuggerStepThrough]
    //[DesignerCategory("code")]
    //[XmlType(AnonymousType =true)]
    //[XmlRoot(Namespace = "", IsNullable = false)]
    /// <summary>
    /// Representa CalypsoMwSolicitud.
    /// </summary>
    [XmlRoot("Calypso_MW_Request")]
    public class CalypsoMwSolicitud
    {
        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public partial class FX : CalypsoObject
        {
            private string primaryCurrencyField;
            private string secondaryCurrencyField;
            private double primaryAmountField;
            private double secondaryAmountField;

            private bool secondaryAmountFieldSpecified;

            private double spotRateField;
            private bool spotRateFieldSpecified;

            private double spotMarginField;
            private bool spotMarginFieldSpecified;

            private double forwardPointsField;
            private bool forwardPointsFieldSpecified;

            private double forwardMarginPointsField;
            private bool forwardMarginPointsFieldSpecified;

            private double forwardRateField;
            private bool forwardRateFieldSpecified;

            private double marginPointField;
            private bool marginPointFieldSpecified;

            private double finalRateField;
            private bool finalRateFieldSpecified;

            private double primaryRateField;
            private bool primaryRateFieldSpecified;

            private double secondaryRateField;
            private bool secondaryRateFieldSpecified;

            private string negotiableCurrencyField;
            private string fXResetRateField;
            private string fXSettlementCurrencyField;
            private DateTime farResetDateField;
            private bool farResetDateFieldSpecified;
            private string quantoCurrencyField;
            private bool quantoResetDateFieldSpecified;
            private bool quantoFixingSplitBField;
            private bool quantoFixingSplitBFieldSpecified;
            private string quantoFXResetRateField;
            private DateTime farQuantoResetDateField;
            private bool farQuantoResetDateFieldSpecified;
            private string preciousMetalLocationField;
            private string preciousMetalPhysicalFormField;
            private double preciousMetalLocationSpreadField;
            private bool preciousMetalLocationSpreadFieldSpecified;
            private double preciousMetalLocationSpotField;
            private bool preciousMetalLocationSpotFieldSpecified;
            private bool preciousMetalAllocatedBField;
            private bool preciousMetalAllocatedBFieldSpecified;
            private bool cLSFlagBField;
            private bool cLSFlagBFieldSpecified;
            private bool cLSFarFlagBField;
            private bool cLSFarFlagBFieldSpecified;
            private double totalMarginField;
            private bool totalMarginFieldSpecified;

            /// <comentarios/>
            public string PrimaryCurrency
            {
                get { return this.primaryCurrencyField; }
                set { this.primaryCurrencyField = value; }
            }

            /// <comentarios/>
            public string SecondaryCurrency
            {
                get { return this.secondaryCurrencyField; }
                set { this.secondaryCurrencyField = value; }
            }

            /// <comentarios/>
            public double PrimaryAmount
            {
                get { return this.primaryAmountField; }
                set { this.primaryAmountField = value; }
            }

            /// <comentarios/>
            public double? SecondaryAmount
            {
                get { return this.secondaryAmountField; }
                set
                {
                    this.secondaryAmountField = value.GetValueOrDefault();
                    this.secondaryAmountFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool SecondaryAmountSpecified
            {
                get { return this.secondaryAmountFieldSpecified; }
                set { this.secondaryAmountFieldSpecified = value; }
            }

            /// <comentarios/>
            public double? SpotRate
            {
                get { return this.spotRateField; }
                set
                {
                    this.spotRateField = value.GetValueOrDefault();
                    this.spotRateFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool SpotRateSpecified
            {
                get { return this.spotRateFieldSpecified; }
                set { this.spotRateFieldSpecified = value; }
            }

            /// <comentarios/>
            public double SpotMargin
            {
                get { return this.spotMarginField; }
                set
                {
                    this.spotMarginField = value;
                    // En VB el If: Not(Value = vbNull) Or Not(Value = 0) => prácticamente siempre True.
                    // Se conserva el comportamiento: marcar Specified al asignar.
                    this.spotMarginFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool SpotMarginSpecified
            {
                get { return this.spotMarginFieldSpecified; }
                set { this.spotMarginFieldSpecified = value; }
            }

            /// <comentarios/>
            public double ForwardPoints
            {
                get { return this.forwardPointsField; }
                set
                {
                    this.forwardPointsField = value;
                    this.forwardPointsFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool ForwardPointsSpecified
            {
                get { return this.forwardPointsFieldSpecified; }
                set { this.forwardPointsFieldSpecified = value; }
            }

            /// <comentarios/>
            public double ForwardMarginPoints
            {
                get { return this.forwardMarginPointsField; }
                set
                {
                    this.forwardMarginPointsField = value;
                    // En VB el If: Not(Value = vbNull) Or Not(Value = 0) => prácticamente siempre True.
                    // Se conserva el comportamiento: marcar Specified al asignar.
                    this.forwardMarginPointsFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool ForwardMarginPointsSpecified
            {
                get { return this.forwardMarginPointsFieldSpecified; }
                set { this.forwardMarginPointsFieldSpecified = value; }
            }

            /// <comentarios/>
            public double? ForwardRate
            {
                get { return this.forwardRateField; }
                set
                {
                    this.forwardRateField = value.GetValueOrDefault();
                    this.forwardRateFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool ForwardRateSpecified
            {
                get { return this.forwardRateFieldSpecified; }
                set { this.forwardRateFieldSpecified = value; }
            }

            /// <comentarios/>
            public double? MarginPoint
            {
                get { return this.marginPointField; }
                set
                {
                    this.marginPointField = value.GetValueOrDefault();
                    this.marginPointFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool MarginPointSpecified
            {
                get { return this.marginPointFieldSpecified; }
                set { this.marginPointFieldSpecified = value; }
            }

            /// <comentarios/>
            public double? FinalRate
            {
                get { return this.finalRateField; }
                set
                {
                    this.finalRateField = value.GetValueOrDefault();
                    this.finalRateFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool FinalRateSpecified
            {
                get { return this.finalRateFieldSpecified; }
                set { this.finalRateFieldSpecified = value; }
            }

            /// <comentarios/>
            public double? PrimaryRate
            {
                get { return this.primaryRateField; }
                set
                {
                    this.primaryRateField = value.GetValueOrDefault();
                    this.primaryRateFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool PrimaryRateSpecified
            {
                get { return this.primaryRateFieldSpecified; }
                set { this.primaryRateFieldSpecified = value; }
            }

            /// <comentarios/>
            public double? SecondaryRate
            {
                get { return this.secondaryRateField; }
                set
                {
                    this.secondaryRateField = value.GetValueOrDefault();
                    this.secondaryRateFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool SecondaryRateSpecified
            {
                get { return this.secondaryRateFieldSpecified; }
                set { this.secondaryRateFieldSpecified = value; }
            }

            /// <comentarios/>
            public string NegotiableCurrency
            {
                get { return this.negotiableCurrencyField; }
                set { this.negotiableCurrencyField = value; }
            }

            /// <comentarios/>
            public string FXResetRate
            {
                get { return this.fXResetRateField; }
                set { this.fXResetRateField = value; }
            }

            /// <comentarios/>
            public string FXSettlementCurrency
            {
                get { return this.fXSettlementCurrencyField; }
                set { this.fXSettlementCurrencyField = value; }
            }

            /// <comentarios/>
            [XmlElement(DataType = "date")]
            public DateTime FarResetDate
            {
                get { return this.farResetDateField; }
                set { this.farResetDateField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool FarResetDateSpecified
            {
                get { return this.farResetDateFieldSpecified; }
                set { this.farResetDateFieldSpecified = value; }
            }

            /// <comentarios/>
            public string QuantoCurrency
            {
                get { return this.quantoCurrencyField; }
                set { this.quantoCurrencyField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool QuantoResetDateSpecified
            {
                get { return this.quantoResetDateFieldSpecified; }
                set { this.quantoResetDateFieldSpecified = value; }
            }

            /// <comentarios/>
            public bool QuantoFixingSplitB
            {
                get { return this.quantoFixingSplitBField; }
                set { this.quantoFixingSplitBField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool QuantoFixingSplitBSpecified
            {
                get { return this.quantoFixingSplitBFieldSpecified; }
                set { this.quantoFixingSplitBFieldSpecified = value; }
            }

            /// <comentarios/>
            public string QuantoFXResetRate
            {
                get { return this.quantoFXResetRateField; }
                set { this.quantoFXResetRateField = value; }
            }

            /// <comentarios/>
            [XmlElement(DataType = "date")]
            public DateTime FarQuantoResetDate
            {
                get { return this.farQuantoResetDateField; }
                set { this.farQuantoResetDateField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool FarQuantoResetDateSpecified
            {
                get { return this.farQuantoResetDateFieldSpecified; }
                set { this.farQuantoResetDateFieldSpecified = value; }
            }

            /// <comentarios/>
            public string PreciousMetalLocation
            {
                get { return this.preciousMetalLocationField; }
                set { this.preciousMetalLocationField = value; }
            }

            /// <comentarios/>
            public string PreciousMetalPhysicalForm
            {
                get { return this.preciousMetalPhysicalFormField; }
                set { this.preciousMetalPhysicalFormField = value; }
            }

            /// <comentarios/>
            public double PreciousMetalLocationSpread
            {
                get { return this.preciousMetalLocationSpreadField; }
                set { this.preciousMetalLocationSpreadField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool PreciousMetalLocationSpreadSpecified
            {
                get { return this.preciousMetalLocationSpreadFieldSpecified; }
                set { this.preciousMetalLocationSpreadFieldSpecified = value; }
            }

            /// <comentarios/>
            public double PreciousMetalLocationSpot
            {
                get { return this.preciousMetalLocationSpotField; }
                set { this.preciousMetalLocationSpotField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool PreciousMetalLocationSpotSpecified
            {
                get { return this.preciousMetalLocationSpotFieldSpecified; }
                set { this.preciousMetalLocationSpotFieldSpecified = value; }
            }

            /// <comentarios/>
            public bool PreciousMetalAllocatedB
            {
                get { return this.preciousMetalAllocatedBField; }
                set { this.preciousMetalAllocatedBField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool PreciousMetalAllocatedBSpecified
            {
                get { return this.preciousMetalAllocatedBFieldSpecified; }
                set { this.preciousMetalAllocatedBFieldSpecified = value; }
            }

            /// <comentarios/>
            public bool CLSFlagB
            {
                get { return this.cLSFlagBField; }
                set { this.cLSFlagBField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool CLSFlagBSpecified
            {
                get { return this.cLSFlagBFieldSpecified; }
                set { this.cLSFlagBFieldSpecified = value; }
            }

            /// <comentarios/>
            public bool CLSFarFlagB
            {
                get { return this.cLSFarFlagBField; }
                set { this.cLSFarFlagBField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool CLSFarFlagBSpecified
            {
                get { return this.cLSFarFlagBFieldSpecified; }
                set { this.cLSFarFlagBFieldSpecified = value; }
            }

            /// <comentarios/>
            public double TotalMargin
            {
                get { return this.totalMarginField; }
                set { this.totalMarginField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool TotalMarginSpecified
            {
                get { return this.totalMarginFieldSpecified; }
                set { this.totalMarginFieldSpecified = value; }
            }
        }

        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        [XmlType(AnonymousType = true)]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public partial class Keyword
        {
            private string keywordNameField;

            private string keywordValueField;

            public string KeywordName
            {
                get { return this.keywordNameField; }
                set { this.keywordNameField = value; }
            }

            public string KeywordValue
            {
                get { return this.keywordValueField; }
                set { this.keywordValueField = value; }
            }
        }

        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        public abstract partial class CalypsoObject
        {
            private string objectStatusField;

            /// <comentarios/>
            public string ObjectStatus
            {
                get { return this.objectStatusField; }
                set { this.objectStatusField = value; }
            }
        }

        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        public partial class Product : CalypsoObject
        {
            private object itemField;

            /// <comentarios/>
            [XmlElement("FX", typeof(FX))]
            public object Item
            {
                get { return this.itemField; }
                set { this.itemField = value; }
            }
        }

        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        public enum BuySellType
        {
            Buy, Sell
        }
        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        [XmlType(AnonymousType = true)]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public partial class CalypsoTrade : CalypsoObject
        {
            private string externalReferenceField;
            private string internalReferenceField;
            private int tradeIdField;

            private bool tradeIdFieldSpecified;
            private string actionField;
            private bool allegeActionBField;
            private string tradeCounterPartyField;
            private string counterPartyRoleField;
            private string tradeBookField;
            private string tradeBundleTypeField;
            private string tradeBundleNameField;
            private bool tradeBundleOneMsgField;

            private bool tradeBundleOneMsgFieldSpecified;

            private bool tradeEventsInSameBundleField;

            private bool tradeEventsInSameBundleFieldSpecified;

            private string tradeCurrencyField;
            private string tradeSettlementCurrencyField;
            private string negotiatedCurrencyField;
            private double tradeNotionalField;
            private BuySellType buySellField;
            private DateTime tradeDateTimeField;
            private DateTime tradeSettleDateField;
            private DateTime startDateField;
            private bool startDateFieldSpecified;
            private DateTime maturityDateField;
            private bool maturityDateFieldSpecified;
            private string traderNameField;
            private string salesPersonField;
            private string commentField;
            private string templateNameField;
            private string[] holidayCodeField;
            private string productTypeField;
            private string productSubTypeField;
            private Product productField;
            private Keyword[] tradeKeywordsField;
            private string mirrorBookField;
            private string marketTypeField;
            private string marketPlaceField;
            private string marketDataItemOverrideField;
            private bool tradeTypeFieldSpecified;
/// <summary>
/// Inicializa una nueva instancia de CalypsoTrade.
/// </summary>

            public CalypsoTrade()
            {
                this.allegeActionBField = false;
                this.tradeBundleOneMsgField = false;
                this.tradeEventsInSameBundleField = false;
                this.tradeNotionalField = 0;
            }

            /// <comentarios/>
            public string ExternalReference
            {
                get { return this.externalReferenceField; }
                set { this.externalReferenceField = value; }
            }

            /// <comentarios/>
            public string InternalReference
            {
                get { return this.internalReferenceField; }
                set { this.internalReferenceField = value; }
            }

            /// <comentarios></comentarios>
            public int TradeId
            {
                get { return this.tradeIdField; }
                set
                {
                    this.tradeIdField = value;
                    this.tradeIdFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool TradeIdSpecified
            {
                get { return this.tradeIdFieldSpecified; }
                set { this.tradeIdFieldSpecified = value; }
            }

            /// <comentarios/>
            public string Action
            {
                get { return this.actionField; }
                set { this.actionField = value; }
            }

            /// <comentarios/>
            [DefaultValue(false)]
            public bool AllegeActionB
            {
                get { return this.allegeActionBField; }
                set { this.allegeActionBField = value; }
            }

            /// <comentarios/>
            public string TradeCounterParty
            {
                get { return this.tradeCounterPartyField; }
                set { this.tradeCounterPartyField = value; }
            }

            /// <comentarios/>
            public string CounterPartyRole
            {
                get { return this.counterPartyRoleField; }
                set { this.counterPartyRoleField = value; }
            }

            /// <comentarios/>
            public string TradeBook
            {
                get { return this.tradeBookField; }
                set { this.tradeBookField = value; }
            }

            /// <comentarios/>
            public string TradeBundleType
            {
                get { return this.tradeBundleTypeField; }
                set { this.tradeBundleTypeField = value; }
            }

            /// <comentarios/>
            public string TradeBundleName
            {
                get { return this.tradeBundleNameField; }
                set { this.tradeBundleNameField = value; }
            }

            /// <comentarios/>
            public bool TradeBundleOneMsg
            {
                get { return this.tradeBundleOneMsgField; }
                set { this.tradeBundleOneMsgField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool TradeBundleOneMsgSpecified
            {
                get { return this.tradeBundleOneMsgFieldSpecified; }
                set { this.tradeBundleOneMsgFieldSpecified = value; }
            }

            /// <comentarios/>
            public bool TradeEventsInSameBundle
            {
                get { return this.tradeEventsInSameBundleField; }
                set { this.tradeEventsInSameBundleField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool TradeEventsInSameBundleSpecified
            {
                get { return this.tradeEventsInSameBundleFieldSpecified; }
                set { this.tradeEventsInSameBundleFieldSpecified = value; }
            }

            /// <comentarios/>
            public string TradeCurrency
            {
                get { return this.tradeCurrencyField; }
                set { this.tradeCurrencyField = value; }
            }

            /// <comentarios/>
            public string TradeSettlementCurrency
            {
                get { return this.tradeSettlementCurrencyField; }
                set { this.tradeSettlementCurrencyField = value; }
            }

            /// <comentarios/>
            public string NegotiatedCurrency
            {
                get { return this.negotiatedCurrencyField; }
                set { this.negotiatedCurrencyField = value; }
            }

            /// <comentarios/>
            public double TradeNotional
            {
                get { return this.tradeNotionalField; }
                set { this.tradeNotionalField = value; }
            }

            /// <comentarios/>
            public BuySellType BuySell
            {
                get { return this.buySellField; }
                set { this.buySellField = value; }
            }

            /// <comentarios/>
            public DateTime TradeDateTime
            {
                get { return this.tradeDateTimeField; }
                set { this.tradeDateTimeField = value; }
            }

            /// <comentarios/>
            [XmlElement(DataType = "date")]
            public DateTime TradeSettleDate
            {
                get { return this.tradeSettleDateField; }
                set { this.tradeSettleDateField = value; }
            }

            /// <comentarios/>
            [XmlElement(DataType = "date")]
            public DateTime StartDate
            {
                get { return this.startDateField; }
                set
                {
                    this.startDateField = value;
                    this.startDateFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool StartDateSpecified
            {
                get { return this.startDateFieldSpecified; }
                set { this.startDateFieldSpecified = value; }
            }

            /// <comentarios/>
            [XmlElement(DataType = "date")]
            public DateTime MaturityDate
            {
                get { return this.maturityDateField; }
                set
                {
                    this.maturityDateField = value;
                    this.maturityDateFieldSpecified = true;
                }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool MaturityDateSpecified
            {
                get { return this.maturityDateFieldSpecified; }
                set { this.maturityDateFieldSpecified = value; }
            }

            /// <comentarios/>
            public string TraderName
            {
                get { return this.traderNameField; }
                set { this.traderNameField = value; }
            }

            /// <comentarios/>
            public string SalesPerson
            {
                get { return this.salesPersonField; }
                set { this.salesPersonField = value; }
            }

            /// <comentarios/>
            public string Comment
            {
                get { return this.commentField; }
                set { this.commentField = value; }
            }

            /// <comentarios/>
            public string TemplateName
            {
                get { return this.templateNameField; }
                set { this.templateNameField = value; }
            }

            /// <comentarios/>
            [XmlArrayItem("Holiday", IsNullable = false)]
            public string[] HolidayCode
            {
                get { return this.holidayCodeField; }
                set { this.holidayCodeField = value; }
            }

            /// <comentarios/>
            public string ProductType
            {
                get { return this.productTypeField; }
                set { this.productTypeField = value; }
            }

            /// <comentarios/>
            public string ProductSubType
            {
                get { return this.productSubTypeField; }
                set { this.productSubTypeField = value; }
            }

            /// <comentarios/>
            public Product Product
            {
                get { return this.productField; }
                set { this.productField = value; }
            }

            /// <comentarios/>
            [XmlArrayItem("Keyword", IsNullable = false)]
            public Keyword[] TradeKeywords
            {
                get { return this.tradeKeywordsField; }
                set { this.tradeKeywordsField = value; }
            }

            /// <comentarios/>
            public string MirrorBook
            {
                get { return this.mirrorBookField; }
                set { this.mirrorBookField = value; }
            }

            /// <comentarios/>
            public string MarketType
            {
                get { return this.marketTypeField; }
                set { this.marketTypeField = value; }
            }

            /// <comentarios/>
            public string MarketPlace
            {
                get { return this.marketPlaceField; }
                set { this.marketPlaceField = value; }
            }

            /// <comentarios/>
            public string MarketDataItemOverride
            {
                get { return this.marketDataItemOverrideField; }
                set { this.marketDataItemOverrideField = value; }
            }

            /// <comentarios/>
            [XmlIgnore]
            public bool tradeTypeSpecified
            {
                get { return this.tradeTypeFieldSpecified; }
                set { this.tradeTypeFieldSpecified = value; }
            }
        }
    }

}

using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Schema;
using System.Xml.Serialization;

namespace MUFG_T24.Modelos
{
    //[GeneratedCode("xsd", "2.0.50727.3038")]
    //[Serializable]
    //[DebuggerStepThrough]
    //[DesignerCategory("code")]
    //[XmlType(AnonymousType =true)]
    //[XmlRoot(Namespace = "", IsNullable = false)]
    /// <summary>
    /// Representa CalypsoMwRespuesta.
    /// </summary>
    [XmlRoot("Calypso_MW_Response")]
    public class CalypsoMwRespuesta
    {

        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        [XmlType(AnonymousType = true)]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public partial class Error
        {
            private string codeField;
            private string itemField;
            private string messageField;
            private string valueField;
            private ErrorType typeField;

            /// <comentarios/>
            [XmlElement(Form = XmlSchemaForm.Unqualified)]
            public string Code
            {
                get { return this.codeField; }
                set { this.codeField = value; }
            }

            /// <comentarios/>
            public string Item
            {
                get { return this.itemField; }
                set { this.itemField = value; }
            }

            /// <comentarios/>
            public string Message
            {
                get { return this.messageField; }
                set { this.messageField = value; }
            }

            /// <comentarios/>
            public string Value
            {
                get { return this.valueField; }
                set { this.valueField = value; }
            }

            /// <comentarios/>
            [XmlAttribute]
            public ErrorType Type
            {
                get { return this.typeField; }
                set { this.typeField = value; }
            }
        }

        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        [XmlType(AnonymousType = true)]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public partial class Keyword
        {
            private string nameField;
            private string valueField;

            /// <comentarios/>
            [XmlElement(Form = XmlSchemaForm.Unqualified)]
            public string Name
            {
                get { return this.nameField; }
                set { this.nameField = value; }
            }

            /// <comentarios/>
            [XmlElement(Form = XmlSchemaForm.Unqualified)]
            public string Value
            {
                get { return this.valueField; }
                set { this.valueField = value; }
            }
        }

        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        [XmlType(AnonymousType = true)]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public partial class CalypsoTrade
        {
            private string externalRefField;
            private string processingOrgField;
            private int calypsoTradeIdField;
            private Status statusField;
            private string actionField;
            private Error[] errorField;
            private Keyword[] tradeCustomKeywordsField;

            /// <comentarios/>
            public string ExternalRef
            {
                get { return this.externalRefField; }
                set { this.externalRefField = value; }
            }

            /// <comentarios/>
            public string ProcessingOrg
            {
                get { return this.processingOrgField; }
                set { this.processingOrgField = value; }
            }

            /// <comentarios/>
            public int CalypsoTradeId
            {
                get { return this.calypsoTradeIdField; }
                set { this.calypsoTradeIdField = value; }
            }

            /// <comentarios/>
            public Status Status
            {
                get { return this.statusField; }
                set { this.statusField = value; }
            }

            /// <comentarios/>
            public string Action
            {
                get { return this.actionField; }
                set { this.actionField = value; }
            }

            /// <comentarios/>
            [XmlElement("Error")]
            public Error[] Error
            {
                get { return this.errorField; }
                set { this.errorField = value; }
            }

            /// <comentarios/>
            [XmlArrayItem("Keyword", IsNullable = false)]
            public Keyword[] TradeCustomKeywords
            {
                get { return this.tradeCustomKeywordsField; }
                set { this.tradeCustomKeywordsField = value; }
            }
        }


        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [XmlType(AnonymousType = true)]
        public enum ErrorType
        {

            InvalidData,
            MissingData,
            MissingSecurity,
            StaticDataError,
            Rejection,
            HoldForMatching,
            Warning
        }

        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [XmlType(AnonymousType = true)]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public enum Status
        {
            Failed,
            Rejected,
            Success,
            Successful
        }
/// <summary>
/// Representa CalypsoTrades.
/// </summary>

        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        [XmlType(AnonymousType = true)]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public partial class CalypsoTrades //: CalypsoAcknowledgementObject
        {
            private CalypsoTrade[] calypsoTradeField;

            /// <comentarios/>
            [XmlElement("CalypsoTrade")]
            public CalypsoTrade[] CalypsoTrade
            {
                get { return this.calypsoTradeField; }
                set { this.calypsoTradeField = value; }
            }
        }

        /// <comentarios/>
        [GeneratedCode("xsd", "2.0.50727.3038")]
        [Serializable]
        [DebuggerStepThrough]
        [DesignerCategory("code")]
        [XmlType(AnonymousType = true)]
        [XmlRoot(Namespace = "", IsNullable = false)]
        public partial class CalypsoAcknowledgement
        {
            private CalypsoTrades calypsoTradesField;           

            private string uploadFileField;
            private string uploadDateField;
            private int uploadedField;
            private int rejectedField;
            private int receivedField;

            /// <comentarios/>
            public CalypsoTrades CalypsoTrades
            {
                get { return this.calypsoTradesField; }
                set { this.calypsoTradesField = value; }
            }

            /// <comentarios/>
            [XmlAttribute]
            public string UploadFile
            {
                get { return this.uploadFileField; }
                set { this.uploadFileField = value; }
            }

            /// <comentarios/>
            [XmlAttribute]
            public string UploadDate
            {
                get { return this.uploadDateField; }
                set { this.uploadDateField = value; }
            }

            /// <comentarios/>
            [XmlAttribute]
            public int Uploaded
            {
                get { return this.uploadedField; }
                set { this.uploadedField = value; }
            }

            /// <comentarios/>
            [XmlAttribute]
            public int Rejected
            {
                get { return this.rejectedField; }
                set { this.rejectedField = value; }
            }

            /// <comentarios/>
            [XmlAttribute]
            public int Received
            {
                get { return this.receivedField; }
                set { this.receivedField = value; }
            }
        }
    }
}

using MUFG_T24.Enums_utilidades;
using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace MUFG_T24.Comunes.Helpers
{
    /// <summary>
    /// Representa ConversionHelper.
    /// </summary>
    public static class ConversionHelper
    {
        /// <summary>
        /// Representa el orden de bytes.
        /// </summary>
        public enum ByteOrder
        {
            BIG_ENDIAN,
            LITTLE_ENDIAN
        }

        /// <summary>
        /// Inicializa una nueva instancia de intToByteArray.
        /// </summary>
        public static byte[] intToByteArray(int value, ByteOrder order)
        {
            try
            {
                byte[] bytes = BitConverter.GetBytes(value);
                ReverseIfNeeded(bytes, order);
                return bytes;
            }
            catch (Exception ErrorBytes)
            {
                throw new Exception($"intToByteArray|Se presenta exception:{ErrorBytes}");
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de ReverseIfNeeded.
        /// </summary>
        /// <param name="fecha"></param>
        /// <returns></returns>
        public static DateTime TextoAFechaT24(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                return DateTime.MinValue;

            DateTime fechaConvertida;
            if (DateTime.TryParseExact(fecha, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out fechaConvertida))
                return fechaConvertida;

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|TextoAFechaT24|No fue posible convertir la fecha '{fecha}' con formato yyyyMMdd.");
            return DateTime.MinValue;
        }


        /// <summary>
        /// Inicializa una nueva instancia de shortToByteArray.
        /// </summary>
        public static byte[] shortToByteArray(ushort value, ByteOrder order)
        {
            try
            {
                byte[] bytes = BitConverter.GetBytes(value);
                ReverseIfNeeded(bytes, order);
                return bytes;
            }
            catch (Exception ErrorBytes)
            {
                throw new Exception($"shortToByteArray|Se presenta exception:{ErrorBytes}");
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de dateToByteArray.
        /// </summary>
        public static byte[] dateToByteArray(int value, ByteOrder order)
        {
            try
            {
                int dia = value % 100;
                value -= dia;
                int mes = (value / 100) % 100;
                int anio = (value - mes) / 10000;

                byte[] bytes = new byte[4];
                bytes[0] = (byte)dia;
                bytes[1] = (byte)mes;

                byte[] anioBytes = BitConverter.GetBytes((ushort)anio);
                ReverseIfNeeded(anioBytes, order);

                bytes[2] = anioBytes[0];
                bytes[3] = anioBytes[1];

                return bytes;
            }
            catch (Exception ErrorBytes)
            {
                throw new Exception($"dateToByteArray|Se presenta exception:{ErrorBytes}");
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de stringToByteArray.
        /// </summary>
        public static byte[] stringToByteArray(string value, ByteOrder order)
        {
            byte[] bytes = null;

            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                System.Text.Encoding loEncoding;

                sb.Append(value);
                loEncoding = System.Text.Encoding.GetEncoding(850);

                bytes = loEncoding.GetBytes(sb.ToString());

                Array.Resize(ref bytes, bytes.Length + 1);
            }
            catch (Exception lexError)
            {
                throw new Exception("stringToByteArray|Se presenta exception: " + lexError.ToString());
            }

            return bytes;
        }

        /// <summary>
        /// Inicializa una nueva instancia de longToByteArray.
        /// </summary>
        public static byte[] longToByteArray(long value, ByteOrder order)
        {
            try
            {
                byte[] bytes = BitConverter.GetBytes(value);
                ReverseIfNeeded(bytes, order);
                return bytes;
            }
            catch (Exception ErrorBytes)
            {
                throw new Exception($"longToByteArray|Se presenta exception:{ErrorBytes}");
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de moneyToByteArray.
        /// </summary>
        public static byte[] moneyToByteArray(double value, ByteOrder order)
        {
            byte[] bytes2 = null;

            try
            {
                decimal decimalR = Math.Round(Convert.ToDecimal(value), 2);
                long[] lngMonto = DoubleToLong(decimalR);

                byte[] bytesP1;
                using (MemoryStream streamP1 = new MemoryStream())
                {
                    streamP1.SetLength(8);
                    using (BinaryWriter writerP1 = new BinaryWriter(streamP1))
                    {
                        writerP1.Write(lngMonto[0]);
                    }
                    bytesP1 = streamP1.ToArray();
                }

                byte[] bytesP2;
                using (MemoryStream streamP2 = new MemoryStream())
                {
                    streamP2.SetLength(8);
                    using (BinaryWriter writerP2 = new BinaryWriter(streamP2))
                    {
                        writerP2.Write(lngMonto[1]);
                    }
                    bytesP2 = streamP2.ToArray();
                }

                byte[] bytes;
                using (MemoryStream stream = new MemoryStream())
                {
                    stream.SetLength(16);
                    using (BinaryWriter writer = new BinaryWriter(stream))
                    {
                        if (ShouldReverse(order))
                        {
                            Array.Reverse(bytesP1, 0, bytesP1.Length);
                            Array.Reverse(bytesP2, 0, bytesP2.Length);
                        }

                        writer.Write(bytesP1);
                        writer.Write(bytesP2);
                    }
                    bytes = stream.ToArray();
                }

                byte[] array1;
                byte[] array2;

                if (order == ByteOrder.BIG_ENDIAN)
                {
                    array1 = GetData(bytes, 4, 8);
                    array2 = GetData(bytes, 12, 16);
                }
                else
                {
                    array1 = GetData(bytes, 0, 4);
                    array2 = GetData(bytes, 8, 12);
                }

                using (MemoryStream stream2 = new MemoryStream())
                {
                    stream2.SetLength(8);
                    using (BinaryWriter writer2 = new BinaryWriter(stream2))
                    {
                        writer2.Write(array1);
                        writer2.Write(array2);
                    }
                    bytes2 = stream2.ToArray();
                }
            }
            catch (Exception lexError)
            {
                throw new Exception("longToByteArray|Se presenta exception: " + lexError.ToString());
            }

            return bytes2;
        }

        /// <summary>
        /// Inicializa una nueva instancia de DoubleToLong.
        /// </summary>
        public static long[] DoubleToLong(decimal monto)
        {
            bool negativo = false;
            int digitos = 9;
            long[] retorno = new long[] { 0L, 0L };

            if (Convert.ToDouble(monto) < 0.0)
            {
                negativo = true;
                monto = decimal.Multiply(monto, -1);
            }

            string strMonto = string.Format("{0:0.00}", monto);

            strMonto = strMonto.Replace(".", "");
            strMonto = strMonto.Replace(",", "");

            int longitudCadena = strMonto.Length;

            if (longitudCadena >= (digitos + 1))
            {
                retorno[0] = Convert.ToInt64(Convert.ToDouble(strMonto.Substring(0, longitudCadena - digitos)));
                retorno[1] = Convert.ToInt64(Convert.ToDouble(strMonto.Substring(longitudCadena - digitos)));

                if (negativo)
                {
                    retorno[0] = retorno[0] * -1L;
                }
            }
            else
            {
                retorno[1] = Convert.ToInt64(Convert.ToDouble(strMonto));

                if (negativo)
                {
                    retorno[1] = retorno[1] * -1L;
                }
            }

            return retorno;
        }

        /// <summary>
        /// Inicializa una nueva instancia de GetData.
        /// </summary>
        public static byte[] GetData(byte[] trama, int initIndex, int finalIndex)
        {
            int highestIndex = finalIndex - initIndex;
            int totalElements = highestIndex - 1;

            byte[] array = new byte[totalElements + 1];
            int i = 0;

            try
            {
                for (int x = initIndex; x < finalIndex; x++)
                {
                    array[i] = trama[x];
                    i++;
                }
            }
            catch (Exception ex)
            {
                throw new IndexOutOfRangeException("El indice " + ex.Message + " excede el array de la trama");
            }

            return array;
        }

        /// <summary>
        /// Inicializa una nueva instancia de addZeroByte.
        /// </summary>
        public static void addZeroByte(ref byte[] bytes)
        {
            Array.Resize(ref bytes, bytes.Length + 1);
        }

        /// <summary>
        /// Inicializa una nueva instancia de Texto.
        /// </summary>
        public static string Texto(object valor)
        {
            return valor == null || valor == DBNull.Value
                ? string.Empty
                : Convert.ToString(valor, CultureInfo.InvariantCulture) ?? string.Empty;
        }

        /// <summary>
        /// Inicializa una nueva instancia de FormatearImporteLegacy.
        /// </summary>
        public static string FormatearImporteLegacy(string value)
        {
            if (string.IsNullOrWhiteSpace(value)) return string.Empty;
            if (decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal importe)) return importe.ToString("0.##", CultureInfo.InvariantCulture);
            if (decimal.TryParse(value, NumberStyles.Any, CultureInfo.CurrentCulture, out importe)) return importe.ToString("0.##", CultureInfo.InvariantCulture);
            return value;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirIntRequerido.
        /// </summary>
        public static int ConvertirIntRequerido(object valor, string campo)
        {
            if (valor == null)
                throw new Exception($"El valor de {campo} es requerido.");

            if (int.TryParse(Convert.ToString(valor), NumberStyles.Any, CultureInfo.InvariantCulture, out int resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo {campo} a int. Valor='{valor}'.");       
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableInt.
        /// </summary>
        public static int? ConvertirNullableInt(object valor, string campo = null)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            string texto = Convert.ToString(valor, CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(texto))
                return null;

            if (int.TryParse(texto, NumberStyles.Any, CultureInfo.InvariantCulture, out int resultado))
                return resultado;

            if (campo == null)
                return Convert.ToInt32(valor, CultureInfo.InvariantCulture);

            throw new Exception($"No fue posible convertir el campo {campo} a int. Valor='{valor}'.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirLongRequerido.
        /// </summary>
        public static long ConvertirLongRequerido(object valor, string campo)
        {
            if (valor == null)
                throw new Exception($"El valor de {campo} es requerido.");

            if (long.TryParse(Convert.ToString(valor), NumberStyles.Any, CultureInfo.InvariantCulture, out long resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo {campo} a long. Valor='{valor}'.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseLongRequerido.
        /// </summary>
        public static long ParseLongRequerido(string valor, string campo)
        {
            long resultado;
            if (!long.TryParse(valor, out resultado))
                throw new Exception($"El campo {campo} no tiene un valor numérico válido: {valor}");

            return resultado;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableLong.
        /// </summary>
        public static long? ConvertirNullableLong(object valor, string campo = null)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            string texto = Convert.ToString(valor, CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(texto))
                return null;

            if (long.TryParse(texto, NumberStyles.Any, CultureInfo.InvariantCulture, out long resultado))
                return resultado;

            if (campo == null)
                return Convert.ToInt64(valor, CultureInfo.InvariantCulture);

            throw new Exception($"No fue posible convertir el campo {campo} a long. Valor='{valor}'.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableLongDefaultCero.
        /// </summary>
        public static long? ConvertirNullableLongDefaultCero(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return 0;

            return Convert.ToInt64(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirLongDefaultCero.
        /// </summary>
        public static long ConvertirLongDefaultCero(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return 0;

            return Convert.ToInt64(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirDecimalRequerido.
        /// </summary>
        public static decimal ConvertirDecimalRequerido(object valor, string campo)
        {
            if (valor == null)
                throw new Exception($"El valor de {campo} es requerido.");

            if (decimal.TryParse(Convert.ToString(valor), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal resultado))
                return resultado;

            if (decimal.TryParse(Convert.ToString(valor), NumberStyles.Any, CultureInfo.CurrentCulture, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo {campo} a decimal. Valor='{valor}'.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirDecimalComparable.
        /// </summary>
        public static decimal ConvertirDecimalComparable(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return 0m;

            if (valor is decimal dec)
                return dec;

            if (valor is double dbl)
                return Convert.ToDecimal(dbl, CultureInfo.InvariantCulture);

            if (valor is float flt)
                return Convert.ToDecimal(flt, CultureInfo.InvariantCulture);

            string texto = Texto(valor).Trim();
            if (string.Equals(texto, string.Empty, StringComparison.Ordinal))
                return 0m;

            texto = texto.Replace("m", "").Replace("M", "").Replace(",", "");

            if (decimal.TryParse(texto, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal resultado))
                return resultado;

            if (decimal.TryParse(texto, NumberStyles.Any, CultureInfo.CurrentCulture, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el monto '{texto}' a decimal.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableDecimal.
        /// </summary>
        public static decimal? ConvertirNullableDecimal(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            string texto = Convert.ToString(valor);
            if (string.IsNullOrWhiteSpace(texto))
                return null;

            return Convert.ToDecimal(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseNullableDecimalInvariant.
        /// </summary>
        public static decimal? ParseNullableDecimalInvariant(object value)
        {
            if (value == null)
                return null;

            if (decimal.TryParse(Convert.ToString(value), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal result))
                return result;

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de TryParseNullableDecimal.
        /// </summary>
        public static decimal? TryParseNullableDecimal(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            string texto = Convert.ToString(valor);
            if (string.IsNullOrWhiteSpace(texto))
                return null;

            decimal resultado;
            if (decimal.TryParse(texto, out resultado))
                return resultado;

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirDecimalOrDefault.
        /// </summary>
        public static decimal ConvertirDecimalOrDefault(object valor)
        {
            return TryParseNullableDecimal(valor) ?? 0m;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerMontoEnviarMaestroDecimal.
        /// </summary>
        public static decimal ObtenerMontoEnviarMaestroDecimal(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return 0m;

            string texto = Convert.ToString(valor);
            if (string.IsNullOrWhiteSpace(texto))
                return 0m;

            decimal monto;
            return decimal.TryParse(texto, out monto) ? monto : 0m;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerMontoEnviarMaestroDouble.
        /// </summary>
        public static double ObtenerMontoEnviarMaestroDouble(object valor)
        {
            double resultado = 0;

            if (valor == null || valor == DBNull.Value)
                return resultado;

            if (Convert.ToString(valor) == string.Empty)
                return resultado;

            try
            {
                resultado = Convert.ToDouble(valor);
            }
            catch
            {
                resultado = 0;
            }

            return resultado;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableIntConDecimal.
        /// </summary>
        public static int? ConvertirNullableIntConDecimal(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            string texto = valor.ToString();
            if (string.IsNullOrWhiteSpace(texto))
                return null;

            int resultado;
            if (int.TryParse(texto, out resultado))
                return resultado;

            decimal decimalResultado;
            if (decimal.TryParse(texto, out decimalResultado))
                return Convert.ToInt32(decimalResultado);

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableLongConDecimal.
        /// </summary>
        public static long? ConvertirNullableLongConDecimal(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            string texto = valor.ToString();
            if (string.IsNullOrWhiteSpace(texto))
                return null;

            long resultado;
            if (long.TryParse(texto, out resultado))
                return resultado;

            decimal decimalResultado;
            if (decimal.TryParse(texto, out decimalResultado))
                return Convert.ToInt64(decimalResultado);

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableDateTime.
        /// </summary>
        public static DateTime? ConvertirNullableDateTime(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            string texto = Convert.ToString(valor, CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(texto))
                return null;

            if (DateTime.TryParse(texto, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime resultado))
                return resultado;

            if (DateTime.TryParse(texto, CultureInfo.CurrentCulture, DateTimeStyles.None, out resultado))
                return resultado;

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableDateTimeInvariant.
        /// </summary>
        public static DateTime? ConvertirNullableDateTimeInvariant(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            if (valor is DateTime fecha)
                return fecha;

            string texto = Texto(valor);
            if (string.Equals(texto, string.Empty, StringComparison.Ordinal))
                return null;

            return Convert.ToDateTime(texto, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableDateTimeDirect.
        /// </summary>
        public static DateTime? ConvertirNullableDateTimeDirect(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            return Convert.ToDateTime(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de TryParseNullableDateTime.
        /// </summary>
        public static DateTime? TryParseNullableDateTime(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            DateTime resultado;
            if (DateTime.TryParse(Convert.ToString(valor), out resultado))
                return resultado;

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de TextoToFechaNullable.
        /// </summary>
        public static DateTime? TextoToFechaNullable(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                return null;

            if (fecha.Length == 8 &&
                DateTime.TryParseExact(fecha, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime fechaYmd))
                return fechaYmd;

            if (DateTime.TryParse(fecha, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime fechaNormal))
                return fechaNormal;

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de TextoToFechaInvariantNullable.
        /// </summary>
        public static DateTime? TextoToFechaInvariantNullable(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                return null;

            if (fecha.Length == 8)
            {
                return new DateTime(
                    Convert.ToInt32(fecha.Substring(0, 4), CultureInfo.InvariantCulture),
                    Convert.ToInt32(fecha.Substring(4, 2), CultureInfo.InvariantCulture),
                    Convert.ToInt32(fecha.Substring(6, 2), CultureInfo.InvariantCulture));
            }

            return Convert.ToDateTime(fecha, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Inicializa una nueva instancia de TextoToFechaFlexible.
        /// </summary>
        public static DateTime? TextoToFechaFlexible(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                return null;

            fecha = fecha.Trim();

            if (fecha.Length == 8)
            {
                int anio = Convert.ToInt32(fecha.Substring(0, 4));
                int mes = Convert.ToInt32(fecha.Substring(4, 2));
                int dia = Convert.ToInt32(fecha.Substring(6, 2));

                return new DateTime(anio, mes, dia);
            }

            DateTime fechaConvertida;
            if (DateTime.TryParse(fecha, out fechaConvertida))
                return fechaConvertida;

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de TextoToFechaConvert.
        /// </summary>
        public static DateTime? TextoToFechaConvert(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                return null;

            fecha = fecha.Trim();
            if (fecha.Length == 8)
            {
                return new DateTime(
                    Convert.ToInt32(fecha.Substring(0, 4)),
                    Convert.ToInt32(fecha.Substring(4, 2)),
                    Convert.ToInt32(fecha.Substring(6, 2)));
            }

            return Convert.ToDateTime(fecha);
        }

        /// <summary>
        /// Inicializa una nueva instancia de TextoToFechaDesdeObject.
        /// </summary>
        public static DateTime? TextoToFechaDesdeObject(object fecha)
        {
            if (fecha == null || fecha == DBNull.Value)
                return null;

            string texto = Convert.ToString(fecha)?.Trim();
            if (string.IsNullOrWhiteSpace(texto))
                return null;

            if (texto.Length == 8)
            {
                int anio = Convert.ToInt32(texto.Substring(0, 4));
                int mes = Convert.ToInt32(texto.Substring(4, 2));
                int dia = Convert.ToInt32(texto.Substring(6, 2));

                return new DateTime(anio, mes, dia);
            }

            return Convert.ToDateTime(texto);
        }

        /// <summary>
        /// Inicializa una nueva instancia de TextoToFechaTryParsePartes.
        /// </summary>
        public static DateTime? TextoToFechaTryParsePartes(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                return null;

            if (fecha.Length == 8)
            {
                int anio;
                int mes;
                int dia;

                if (int.TryParse(fecha.Substring(0, 4), out anio) &&
                    int.TryParse(fecha.Substring(4, 2), out mes) &&
                    int.TryParse(fecha.Substring(6, 2), out dia))
                {
                    return new DateTime(anio, mes, dia);
                }
            }

            DateTime resultado;
            if (DateTime.TryParse(fecha, out resultado))
                return resultado;

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de TextoToFechaYmdOnly.
        /// </summary>
        public static DateTime? TextoToFechaYmdOnly(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                return null;

            if (fecha.Length == 8 &&
                int.TryParse(fecha.Substring(0, 4), out int y) &&
                int.TryParse(fecha.Substring(4, 2), out int m) &&
                int.TryParse(fecha.Substring(6, 2), out int d))
                return new DateTime(y, m, d);

            return null;
        }

        /// <summary>
        /// Inicializa una nueva instancia de EsNumerico.
        /// </summary>
        public static bool EsNumerico(string valor)
        {
            if (string.IsNullOrWhiteSpace(valor))
                return false;

            decimal numero;
            return decimal.TryParse(valor, out numero);
        }

        /// <summary>
        /// Inicializa una nueva instancia de EsNumericoInvariant.
        /// </summary>
        public static bool EsNumericoInvariant(string valor)
        {
            if (string.IsNullOrWhiteSpace(valor))
                return false;

            decimal numero;
            return decimal.TryParse(valor, NumberStyles.Any, CultureInfo.InvariantCulture, out numero);
        }

        /// <summary>
        /// Inicializa una nueva instancia de EsValorNuloOVacio.
        /// </summary>
        public static bool EsValorNuloOVacio(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return true;

            string texto = Convert.ToString(valor);
            return string.IsNullOrWhiteSpace(texto);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseIntOrDefault.
        /// </summary>
        public static int ParseIntOrDefault(string value)
        {
            int.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out int i);
            return i;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseLongOrDefault.
        /// </summary>
        public static long ParseLongOrDefault(string value)
        {
            long.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out long i);
            return i;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseDecimalOrDefault.
        /// </summary>
        public static decimal ParseDecimalOrDefault(string value)
        {
            decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal d);
            return d;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseDoubleOrDefault.
        /// </summary>
        public static double ParseDoubleOrDefault(string value)
        {
            double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double d);
            return d;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirIntDirecto.
        /// </summary>
        public static int ConvertirIntDirecto(object valor, string campo)
        {
            if (valor == null)
                throw new Exception($"No se pudo convertir {campo} a int.");

            return Convert.ToInt32(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirLongDirecto.
        /// </summary>
        public static long ConvertirLongDirecto(object valor, string campo)
        {
            if (valor == null)
                throw new Exception($"No se pudo convertir {campo} a long.");

            return Convert.ToInt64(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableIntDirecto.
        /// </summary>
        public static int? ConvertirNullableIntDirecto(object valor, string campo = null)
        {
            if (valor == null || string.Equals(Convert.ToString(valor), string.Empty, StringComparison.Ordinal))
                return null;

            return Convert.ToInt32(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableLongDirecto.
        /// </summary>
        public static long? ConvertirNullableLongDirecto(object valor, string campo = null)
        {
            if (valor == null || string.Equals(Convert.ToString(valor), string.Empty, StringComparison.Ordinal))
                return null;

            return Convert.ToInt64(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirDecimalDirecto.
        /// </summary>
        public static decimal ConvertirDecimalDirecto(object valor, string campo)
        {
            if (valor == null)
                throw new Exception($"No se pudo convertir {campo} a decimal.");

            return Convert.ToDecimal(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirDoubleDirecto.
        /// </summary>
        public static double ConvertirDoubleDirecto(object valor, string campo)
        {
            if (valor == null)
                throw new Exception($"No se pudo convertir {campo} a double.");

            return Convert.ToDouble(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirDateTimeDirecto.
        /// </summary>
        public static DateTime ConvertirDateTimeDirecto(object valor, string campo)
        {
            if (valor == null)
                throw new Exception($"No se pudo convertir {campo} a DateTime.");

            return Convert.ToDateTime(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirNullableDateTimeDirecto.
        /// </summary>
        public static DateTime? ConvertirNullableDateTimeDirecto(object valor, string campo = null)
        {
            if (valor == null || string.Equals(Convert.ToString(valor), string.Empty, StringComparison.Ordinal))
                return null;

            return Convert.ToDateTime(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de Mid.
        /// </summary>
        public static string Mid(string valor, int start, int length)
        {
            if (string.IsNullOrEmpty(valor))
                return string.Empty;

            int startIndex = Math.Max(start - 1, 0);

            if (startIndex >= valor.Length)
                return string.Empty;

            if (startIndex + length > valor.Length)
                return valor.Substring(startIndex);

            return valor.Substring(startIndex, length);
        }

        /// <summary>
        /// Inicializa una nueva instancia de Mid.
        /// </summary>
        public static string Mid(string valor, int start)
        {
            if (string.IsNullOrEmpty(valor))
                return string.Empty;

            int startIndex = Math.Max(start - 1, 0);

            if (startIndex >= valor.Length)
                return string.Empty;

            return valor.Substring(startIndex);
        }

        /// <summary>
        /// Inicializa una nueva instancia de Left.
        /// </summary>
        public static string Left(string valor, int length)
        {
            if (string.IsNullOrEmpty(valor))
                return string.Empty;

            return valor.Length <= length ? valor : valor.Substring(0, length);
        }

        /// <summary>
        /// Inicializa una nueva instancia de FormatearFolio8.
        /// </summary>
        public static string FormatearFolio8(string folio)
        {
            long valor;
            if (!long.TryParse(folio, NumberStyles.Integer, CultureInfo.InvariantCulture, out valor))
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|FormatearFolio8|No fue posible convertir el folio '{folio}' a número.");
                return string.Empty;
            }

            return valor.ToString("00000000", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Inicializa una nueva instancia de FormatearFechaT24.
        /// </summary>
        public static string FormatearFechaT24(DateTime fecha)
        {
            return fecha.Year.ToString(CultureInfo.InvariantCulture) + fecha.Month.ToString("00", CultureInfo.InvariantCulture) + fecha.Day.ToString("00", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirFechaYYYYMMDDaISO.
        /// </summary>
        public static string ConvertirFechaYYYYMMDDaISO(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha) || fecha.Length < 8)
                return fecha;

            return $"{fecha.Substring(0, 4)}-{fecha.Substring(4, 2)}-{fecha.Substring(6, 2)}";
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirDecimalInvariant.
        /// </summary>
        public static decimal ConvertirDecimalInvariant(string valor)
        {
            return Convert.ToDecimal(valor, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Inicializa una nueva instancia de RemoveDiacritics.
        /// </summary>
        public static string RemoveDiacritics(string texto)
        {
            if (string.IsNullOrEmpty(texto))
                return string.Empty;

            string textoNormalizado = texto.Normalize(NormalizationForm.FormD);
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < textoNormalizado.Length; i++)
            {
                UnicodeCategory categoria = CharUnicodeInfo.GetUnicodeCategory(textoNormalizado[i]);

                if (categoria != UnicodeCategory.NonSpacingMark)
                    sb.Append(textoNormalizado[i]);
            }

            return sb.ToString().Normalize(NormalizationForm.FormC);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseBoolFlag.
        /// </summary>
        public static bool ParseBoolFlag(string value, bool aceptarYes = false)
        {
            if (string.IsNullOrWhiteSpace(value))
                return false;

            value = value.Trim();
            return value.Equals("1") ||
                value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                value.Equals("x", StringComparison.OrdinalIgnoreCase) ||
                (aceptarYes && value.Equals("yes", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Inicializa una nueva instancia de ToDouble.
        /// </summary>
        public static double ToDouble(decimal? valor)
        {
            return Convert.ToDouble(valor ?? 0m);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseIntOrZero.
        /// </summary>
        public static int ParseIntOrZero(string valor, string nombreCampo)
        {
            if (string.IsNullOrWhiteSpace(valor))
                return 0;

            if (int.TryParse(valor, NumberStyles.Any, CultureInfo.InvariantCulture, out int resultado))
                return resultado;

            if (int.TryParse(valor, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{nombreCampo}' con valor '{valor}' a int.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertLongToNullableInt.
        /// </summary>
        public static int? ConvertLongToNullableInt(long valor)
        {
            return Convert.ToInt32(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertLongToInt.
        /// </summary>
        public static int ConvertLongToInt(long valor)
        {
            return Convert.ToInt32(valor);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseLongSeguro.
        /// </summary>
        public static long ParseLongSeguro(string valor, string nombreCampo)
        {
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{nombreCampo}' porque viene vacío.");

            if (long.TryParse(valor, NumberStyles.Any, CultureInfo.InvariantCulture, out long resultado))
                return resultado;

            if (long.TryParse(valor, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{nombreCampo}' con valor '{valor}' a long.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseDoubleOrZero.
        /// </summary>
        public static double ParseDoubleOrZero(string valor, string nombreCampo)
        {
            if (string.IsNullOrWhiteSpace(valor))
                return 0d;

            if (double.TryParse(valor, NumberStyles.Any, CultureInfo.InvariantCulture, out double resultado))
                return resultado;

            if (double.TryParse(valor, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{nombreCampo}' con valor '{valor}' a double.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ConvertirDecimalOrZeroDirecto.
        /// </summary>
        public static decimal ConvertirDecimalOrZeroDirecto(object valor)
        {
            if (valor == null || valor == DBNull.Value || string.IsNullOrWhiteSpace(Convert.ToString(valor)))
                return 0m;

            try
            {
                return Convert.ToDecimal(valor);
            }
            catch
            {
                return 0m;
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseDecimalSeguro.
        /// </summary>
        public static decimal ParseDecimalSeguro(string valor)
        {
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception("No fue posible convertir un valor vacío a decimal.");

            if (decimal.TryParse(valor, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal resultado))
                return resultado;

            if (decimal.TryParse(valor, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el valor '{valor}' a decimal.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseDecimalSeguroConDefaultCero.
        /// </summary>
        public static decimal ParseDecimalSeguroConDefaultCero(string valor)
        {
            if (string.IsNullOrWhiteSpace(valor))
                return 0m;

            if (decimal.TryParse(valor, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal resultado))
                return resultado;

            if (decimal.TryParse(valor, out resultado))
                return resultado;

            return 0m;
        }

        /// <summary>
        /// Inicializa una nueva instancia de EsDoubleInvariant.
        /// </summary>
        public static bool EsDoubleInvariant(string value)
        {
            double result;
            return double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseIntRequerido.
        /// </summary>
        public static int ParseIntRequerido(string valor, string nombre, string mensajeFormato)
        {
            if (int.TryParse(valor, out int result))
                return result;

            throw new Exception(string.Format(mensajeFormato, nombre, valor));
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseFechaYyyyMMdd.
        /// </summary>
        public static DateTime ParseFechaYyyyMMdd(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                throw new Exception("La fecha yyyyMMdd viene vacía.");

            if (DateTime.TryParseExact(fecha, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime resultado))
                return resultado;

            throw new Exception($"No fue posible convertir la fecha '{fecha}' con formato yyyyMMdd.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseFechaYyyyMMddExacta.
        /// </summary>
        public static DateTime ParseFechaYyyyMMddExacta(string fecha)
        {
            return DateTime.ParseExact(fecha, "yyyyMMdd", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseFechaEntera.
        /// </summary>
        public static int ParseFechaEntera(string fecha)
        {
            if (string.IsNullOrWhiteSpace(fecha))
                return 0;

            if (int.TryParse(fecha, out int fechaInt))
                return fechaInt;

            if (DateTime.TryParseExact(fecha, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime fechaParseada))
                return int.Parse(fechaParseada.ToString("yyyyMMdd", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

            throw new Exception($"No fue posible convertir la fecha '{fecha}' a entero yyyyMMdd.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseFechaSistema.
        /// </summary>
        public static DateTime ParseFechaSistema(string fecha)
        {
            if (DateTime.TryParseExact(fecha, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime fechaParseada))
                return fechaParseada;

            if (DateTime.TryParse(fecha, CultureInfo.InvariantCulture, DateTimeStyles.None, out fechaParseada))
                return fechaParseada;

            throw new Exception($"No fue posible convertir la fecha '{fecha}' a DateTime.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ParseTimeSpanRequerido.
        /// PENDIENTE: Hacer Obsolete
        /// </summary>
        public static TimeSpan ParseTimeSpanRequerido(string valor, string nombreCampo)
        {
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{nombreCampo}' porque viene vacío.");

            if (TimeSpan.TryParse(valor, CultureInfo.InvariantCulture, out TimeSpan resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{nombreCampo}' con valor '{valor}' a TimeSpan.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ReverseIfNeeded.
        /// </summary>
        private static void ReverseIfNeeded(byte[] bytes, ByteOrder order)
        {
            if (ShouldReverse(order))
            {
                Array.Reverse(bytes);
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de ShouldReverse.
        /// </summary>
        private static bool ShouldReverse(ByteOrder order)
        {
            return (BitConverter.IsLittleEndian && order == ByteOrder.BIG_ENDIAN) ||
                (!BitConverter.IsLittleEndian && order == ByteOrder.LITTLE_ENDIAN);
        }

    }
}

using System;
using System.Globalization;
using MUFG_T24.Infraestructura.Logging;

namespace MUFG_T24.Aplicacion.Conversiones
{
    /// <summary>
    /// Proporciona conversiones comunes de texto a tipos primitivos con soporte de valores requeridos y opcionales.
    /// </summary>
    public static class ConversionesComunes
    {
        private const string ContextoLog = "|ConversionesComunes|";
        private const string NombreCampoSinNombre = "(sin-nombre)";

        /// <summary>
        /// Valida un texto requerido y devuelve su valor normalizado.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Texto validado.</returns>
        public static string ParseTextoRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");

            return valor;
        }

        /// <summary>
        /// Convierte un valor requerido a <see cref="TimeSpan"/>.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Valor convertido a <see cref="TimeSpan"/>.</returns>
        public static TimeSpan ParseTimeSpanRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");

            //Se intenta convertir el valor a TimeSpan...
            if (TimeSpan.TryParse(valor, CultureInfo.InvariantCulture, out TimeSpan resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{campo}' con valor '{valor}' a TimeSpan.");
        }

        /// <summary>
        /// Convierte un valor opcional a <see cref="TimeSpan"/> o retorna un valor por defecto.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto cuando no se puede convertir.</param>
        /// <returns>Valor convertido o <paramref name="valorDefault"/>.</returns>
        public static TimeSpan ParseTimeSpanOpcional(string valor, string nombreCampo, TimeSpan valorDefault)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Warn($"{ContextoLog}El campo '{campo}' viene vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            //Se intenta convertir el valor a TimeSpan...
            TimeSpan resultado;
            if (TimeSpan.TryParse(valor, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            Logging.Warn($"{ContextoLog}No fue posible convertir el campo '{campo}' con valor '{valor}' a TimeSpan. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Convierte un valor requerido a <see cref="int"/>.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Valor convertido a <see cref="int"/>.</returns>
        public static int ParseEnteroRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");

            //Se intenta convertir el valor a Int32...
            int resultado;
            if (int.TryParse(valor, NumberStyles.Integer, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{campo}' con valor '{valor}' a Int32.");
        }

        /// <summary>
        /// Convierte un valor opcional a <see cref="int"/> o retorna un valor por defecto.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto cuando no se puede convertir.</param>
        /// <returns>Valor convertido o <paramref name="valorDefault"/>.</returns>
        public static int ParseEnteroOpcional(string valor, string nombreCampo, int valorDefault)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Warn($"{ContextoLog}El campo '{campo}' viene vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            //Se intenta convertir el valor a Int32...
            int resultado;
            if (int.TryParse(valor, NumberStyles.Integer, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            Logging.Warn($"{ContextoLog}No fue posible convertir el campo '{campo}' con valor '{valor}' a Int32. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Convierte un valor requerido a <see cref="long"/>.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Valor convertido a <see cref="long"/>.</returns>
        public static long ParseLongRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");

            //Se intenta convertir el valor a int64...
            long resultado;
            if (long.TryParse(valor, NumberStyles.Integer, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{campo}' con valor '{valor}' a Int64.");
        }

        /// <summary>
        /// Convierte un valor opcional a <see cref="long"/> o retorna un valor por defecto.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto cuando no se puede convertir.</param>
        /// <returns>Valor convertido o <paramref name="valorDefault"/>.</returns>
        public static long ParseLongOpcional(string valor, string nombreCampo, long valorDefault)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Warn($"{ContextoLog}El campo '{campo}' viene vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            //Se intenta convertir el valor a int64...
            long resultado;
            if (long.TryParse(valor, NumberStyles.Integer, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            Logging.Warn($"{ContextoLog}No fue posible convertir el campo '{campo}' con valor '{valor}' a Int64. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Convierte un valor requerido a <see cref="decimal"/>.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Valor convertido a <see cref="decimal"/>.</returns>
        public static decimal ParseDecimalRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");

            //Se intenta convertir el valor a decimal...
            decimal resultado;
            if (decimal.TryParse(valor, NumberStyles.Number, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{campo}' con valor '{valor}' a Decimal.");
        }

        /// <summary>
        /// Convierte un valor opcional a <see cref="decimal"/> o retorna un valor por defecto.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto cuando no se puede convertir.</param>
        /// <returns>Valor convertido o <paramref name="valorDefault"/>.</returns>
        public static decimal ParseDecimalOpcional(string valor, string nombreCampo, decimal valorDefault)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Warn($"{ContextoLog}El campo '{campo}' viene vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            //Se intenta convertir el valor a decimal...
            decimal resultado;
            if (decimal.TryParse(valor, NumberStyles.Number, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            Logging.Warn($"{ContextoLog}No fue posible convertir el campo '{campo}' con valor '{valor}' a Decimal. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Convierte un valor requerido a <see cref="double"/>.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Valor convertido a <see cref="double"/>.</returns>
        public static double ParseDoubleRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");

            //Se intenta convertir el valor a double...
            double resultado;
            if (double.TryParse(valor, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{campo}' con valor '{valor}' a Double.");
        }

        /// <summary>
        /// Convierte un valor opcional a <see cref="double"/> o retorna un valor por defecto.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto cuando no se puede convertir.</param>
        /// <returns>Valor convertido o <paramref name="valorDefault"/>.</returns>
        public static double ParseDoubleOpcional(string valor, string nombreCampo, double valorDefault)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Warn($"{ContextoLog}El campo '{campo}' viene vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            //Se intenta convertir el valor a double...
            double resultado;
            if (double.TryParse(valor, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            Logging.Warn($"{ContextoLog}No fue posible convertir el campo '{campo}' con valor '{valor}' a Double. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Convierte un valor requerido a <see cref="bool"/>.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Valor convertido a <see cref="bool"/>.</returns>
        public static bool ParseBooleanoRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");
            
            //Se intenta convertir el valor a Boolean...
            bool resultado;
            if (bool.TryParse(valor, out resultado))
                return resultado;

            int valorNumerico;
            if (int.TryParse(valor, NumberStyles.Integer, CultureInfo.InvariantCulture, out valorNumerico) && (valorNumerico == 0 || valorNumerico == 1))
                return valorNumerico == 1;

            throw new Exception($"No fue posible convertir el campo '{campo}' con valor '{valor}' a Boolean.");
        }

        /// <summary>
        /// Convierte un valor opcional a <see cref="bool"/> o retorna un valor por defecto.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto cuando no se puede convertir.</param>
        /// <returns>Valor convertido o <paramref name="valorDefault"/>.</returns>
        public static bool ParseBooleanoOpcional(string valor, string nombreCampo, bool valorDefault)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Warn($"{ContextoLog}El campo '{campo}' viene vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            //Se intenta convertir el valor a Boolean...
            bool resultado;
            if (bool.TryParse(valor, out resultado))
                return resultado;
            
            //Se intenta convertir el valor a Int32...
            int valorNumerico;
            if (int.TryParse(valor, NumberStyles.Integer, CultureInfo.InvariantCulture, out valorNumerico) && (valorNumerico == 0 || valorNumerico == 1))
                return valorNumerico == 1;

            Logging.Warn($"{ContextoLog}No fue posible convertir el campo '{campo}' con valor '{valor}' a Boolean. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Convierte un valor requerido a <see cref="DateTime"/>.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Valor convertido a <see cref="DateTime"/>.</returns>
        public static DateTime ParseFechaRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");

            //Se intenta convertir el valor a DateTime...
            DateTime resultado;
            if (DateTime.TryParse(valor, CultureInfo.InvariantCulture, DateTimeStyles.None, out resultado))
                return resultado;

            throw new Exception($"No fue posible convertir el campo '{campo}' con valor '{valor}' a DateTime.");
        }

        /// <summary>
        /// Convierte un valor opcional a <see cref="DateTime"/> o retorna un valor por defecto.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto cuando no se puede convertir.</param>
        /// <returns>Valor convertido o <paramref name="valorDefault"/>.</returns>
        public static DateTime ParseFechaOpcional(string valor, string nombreCampo, DateTime valorDefault)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Warn($"{ContextoLog}El campo '{campo}' viene vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            //Se intenta convertir el valor a DateTime...
            DateTime resultado;
            if (DateTime.TryParse(valor, CultureInfo.InvariantCulture, DateTimeStyles.None, out resultado))
                return resultado;

            Logging.Warn($"{ContextoLog}No fue posible convertir el campo '{campo}' con valor '{valor}' a DateTime. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Convierte un valor requerido a <see cref="Guid"/>.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <returns>Valor convertido a <see cref="Guid"/>.</returns>
        public static Guid ParseGuidRequerido(string valor, string nombreCampo)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
                throw new Exception($"No fue posible convertir el campo '{campo}' porque viene vacío.");

            //Se intenta convertir el valor a Guid...
            Guid resultado;
            if (Guid.TryParse(valor, out resultado))
                return resultado;

            throw new Exception($"No fue possible convertir el campo '{campo}' con valor '{valor}' a Guid.");
        }

        /// <summary>
        /// Convierte un valor opcional a <see cref="Guid"/> o retorna un valor por defecto.
        /// </summary>
        /// <param name="valor">Valor de entrada en texto.</param>
        /// <param name="nombreCampo">Nombre del campo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto cuando no se puede convertir.</param>
        /// <returns>Valor convertido o <paramref name="valorDefault"/>.</returns>
        public static Guid ParseGuidOpcional(string valor, string nombreCampo, Guid valorDefault)
        {
            //Normaliza el nombre del campo...
            string campo = NormalizarNombreCampo(nombreCampo);

            //Se rechaza el valor si viene vacío o nulo...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Warn($"{ContextoLog}El campo '{campo}' viene vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            //Se intenta convertir el valor a Guid...
            Guid resultado;
            if (Guid.TryParse(valor, out resultado))
                return resultado;

            Logging.Warn($"{ContextoLog}No fue posible convertir el campo '{campo}' con valor '{valor}' a Guid. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Normaliza el nombre del campo para los mensajes de error y advertencia.
        /// </summary>
        /// <param name="nombreCampo">El nombre del campo a normalizar.</param>
        /// <returns>El nombre del campo normalizado.</returns>
        private static string NormalizarNombreCampo(string nombreCampo)
        {
            return string.IsNullOrWhiteSpace(nombreCampo) ? NombreCampoSinNombre : nombreCampo;
        }

    }
}

using System;
using System.Globalization;
using System.Xml.Linq;
using MUFG_T24.Infraestructura.Logging;

namespace MUFG_T24.Aplicacion.Conversiones
{
    /// <summary>
    /// Proporciona conversiones seguras de valores de configuración.
    /// </summary>
    public static class ConversionConfiguracion
    {
        private const string ContextoLog = "|ConversionConfiguracion|";
        private const string NombreAtributoSinNombre = "(sin-nombre)";

        /// <summary>
        /// Obtiene un valor entero desde un atributo de un nodo XML.
        /// </summary>
        /// <param name="nodoServicio">Nodo XML del servicio.</param>
        /// <param name="nombreAtributo">Nombre del atributo a leer.</param>
        /// <param name="valorDefault">Valor por defecto.</param>
        /// <returns>Valor entero convertido o el valor por defecto.</returns>
        public static int ObtenerEnteroConfiguracion(XElement nodoServicio, string nombreAtributo, int valorDefault)
        {
            XAttribute atributo = nodoServicio == null ? null : nodoServicio.Attribute(nombreAtributo);
            return ObtenerEnteroConfiguracion(atributo, nombreAtributo, valorDefault);
        }

        /// <summary>
        /// Obtiene un valor entero desde un atributo XML.
        /// </summary>
        /// <param name="atributo">Atributo XML.</param>
        /// <param name="nombreAtributo">Nombre lógico del atributo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto.</param>
        /// <returns>Valor entero convertido o el valor por defecto.</returns>
        public static int ObtenerEnteroConfiguracion(XAttribute atributo, string nombreAtributo, int valorDefault)
        {
            string nombreNormalizado = NormalizarNombreAtributo(nombreAtributo, atributo == null ? null : atributo.Name.LocalName);
            string valor = atributo == null ? null : atributo.Value;

            //Se rechaza si el atributo esta vacio o es null...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Info($"{ContextoLog}El atributo '{nombreNormalizado}' no está definido o está vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            int resultado;
            if (int.TryParse(valor, NumberStyles.Integer, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            Logging.Error($"{ContextoLog}El atributo '{nombreNormalizado}' no pudo convertirse a entero. Valor='{valor}'. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Obtiene un valor entero desde texto de configuración.
        /// </summary>
        /// <param name="valor">Valor en texto.</param>
        /// <param name="nombreAtributo">Nombre lógico del atributo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto.</param>
        /// <returns>Valor entero convertido o el valor por defecto.</returns>
        public static int ObtenerEnteroConfiguracion(string valor, string nombreAtributo, int valorDefault)
        {
            string nombreNormalizado = NormalizarNombreAtributo(nombreAtributo, null);

            //Se rechaza si el atributo esta vacio o es null...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Info($"{ContextoLog}El atributo '{nombreNormalizado}' no está definido o está vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            int resultado;
            if (int.TryParse(valor, NumberStyles.Integer, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            Logging.Error($"{ContextoLog}El atributo '{nombreNormalizado}' no pudo convertirse a entero. Valor='{valor}'. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Obtiene un valor booleano desde un atributo de un nodo XML.
        /// </summary>
        /// <param name="nodoServicio">Nodo XML del servicio.</param>
        /// <param name="nombreAtributo">Nombre del atributo a leer.</param>
        /// <param name="valorDefault">Valor por defecto.</param>
        /// <returns>Valor booleano convertido o el valor por defecto.</returns>
        public static bool ObtenerBooleanoConfiguracion(XElement nodoServicio, string nombreAtributo, bool valorDefault)
        {
            XAttribute atributo = nodoServicio == null ? null : nodoServicio.Attribute(nombreAtributo);
            return ObtenerBooleanoConfiguracion(atributo, nombreAtributo, valorDefault);
        }

        /// <summary>
        /// Obtiene un valor booleano desde un atributo XML.
        /// </summary>
        /// <param name="atributo">Atributo XML.</param>
        /// <param name="nombreAtributo">Nombre lógico del atributo para trazabilidad.</param>
        /// <param name="valorDefault">Valor por defecto.</param>
        /// <returns>Valor booleano convertido o el valor por defecto.</returns>
        public static bool ObtenerBooleanoConfiguracion(XAttribute atributo, string nombreAtributo, bool valorDefault)
        {
            string nombreNormalizado = NormalizarNombreAtributo(nombreAtributo, atributo == null ? null : atributo.Name.LocalName);
            string valor = atributo == null ? null : atributo.Value;

            //Se rechaza si el atributo esta vacio o es null...
            if (string.IsNullOrWhiteSpace(valor))
            {
                Logging.Info($"{ContextoLog}El atributo '{nombreNormalizado}' no está definido o está vacío. Se usará '{valorDefault}'.");
                return valorDefault;
            }

            bool resultado;
            if (bool.TryParse(valor, out resultado))
                return resultado;

            Logging.Error($"{ContextoLog}El atributo '{nombreNormalizado}' no pudo convertirse a booleano. Valor='{valor}'. Se usará '{valorDefault}'.");
            return valorDefault;
        }

        /// <summary>
        /// Normaliza el nombre lógico de un atributo para trazabilidad y logging.
        /// </summary>
        /// <param name="nombreAtributo">Nombre lógico recibido por parámetro.</param>
        /// <param name="nombreDesdeAtributo">Nombre derivado del atributo XML.</param>
        /// <returns>Nombre normalizado del atributo.</returns>
        private static string NormalizarNombreAtributo(string nombreAtributo, string nombreDesdeAtributo)
        {
            //Se rechaza si el atributo esta vacio o es null...
            if (!string.IsNullOrWhiteSpace(nombreAtributo))
                return nombreAtributo;

            //Se rechaza si el nombre esta vacio o es null...
            if (!string.IsNullOrWhiteSpace(nombreDesdeAtributo))
                return nombreDesdeAtributo;

            return NombreAtributoSinNombre;
        }

    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MUFG_T24.Aplicacion.SSR2026
{
    /// <summary>
    /// Clase encargada de obtener y validar los datos del banco a partir del BIC/ABA, para luego ser utilizados en la generación del mensaje SWIFT.
    /// </summary>
    public class SwiftStandardReleaseSanitize : RepositorioMaestro
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public SwiftStandardReleaseSanitize()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Método encargado de obtener y validar los datos del banco a partir del BIC/ABA.
        /// </summary>
        public async Task<SwiftStandardReleaseDto> ObtenerBancoValidado(string BIC)
        {
            SwiftStandardReleaseDto swiftStandardRelease = new SwiftStandardReleaseDto();

            //1. Busca en catalogo el BIC / ABA.
            swiftStandardRelease = await ObtenerDatosBanco(BIC);

            //2. Valida que los datos obligatorios existan...
            if (swiftStandardRelease is null)
            {
                throw new Exception($"No se encontraron datos para el BIC/ABA: {BIC}");
            }
            if (string.IsNullOrEmpty(swiftStandardRelease.FinancialInstitutionCountryCode) || string.IsNullOrEmpty(swiftStandardRelease.FinancialInstitutionTownName) || string.IsNullOrEmpty(swiftStandardRelease.FinancialInstitutionName))
            {
                throw new Exception($"La información de la institución viene incompleta. Bank name: {swiftStandardRelease.FinancialInstitutionName}, Country code: {swiftStandardRelease.FinancialInstitutionCountryCode}, Town name: {swiftStandardRelease.FinancialInstitutionTownName}");
            }

            //3. Retorna los datos...
            return swiftStandardRelease;
        }

        /// <summary>
        /// Método encargado de obtener los datos del banco a partir del BIC/ABA, consultando el catálogo en la base de datos.
        /// </summary>
        /// <param name="BICoABA"></param>
        /// <returns></returns>
        public async Task<SwiftStandardReleaseDto> ObtenerDatosBanco(string BICoABA)
        {
            var r = await EjecutarStoredProcedureQuery<SwiftStandardReleaseDto>(
                "dbo.usp_MUFGSSR_ObtenerInstitucionPorBICoABA",
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@BICoABA", BICoABA));
                });

            return r.Data.FirstOrDefault();
        }

    }

    /// <summary>
    /// DTO que representa los datos del banco necesarios para la generación del mensaje SWIFT, obtenidos a partir del BIC/ABA.
    /// </summary>
    public class SwiftStandardReleaseDto
    {
        public string FinancialInstitutionName { get; set; }
        public string FinancialInstitutionCountryCode { get; set; }
        public string FinancialInstitutionTownName { get; set; }
    }

}

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Clase con metodos comunes para todos los servicios
    /// </summary>
    public class Comunes
    {
        [Obsolete("Legacy compatibility only.")]
        public static RutasConfiguracion Rutas { get; set; }

        /// <summary>
        /// Formatos para fechas
        /// </summary>
        /// <returns>string</returns>
        public static string[] ObtenerFormatoValido()
        {
            return new[]
            {
                "yyyyMMdd",
                "yyyy/MM/dd",
                "yyyy-MM-dd"
            };
        }

        /// <summary>
        /// Convierte texto a en formato de fecha valido
        /// </summary>
        /// <param name="texto"></param>
        /// <returns>DateTime</returns>
        public DateTime? ConvertirTextoAFecha(object texto)
        {
            if (texto == null || texto == DBNull.Value)
                return null;

            string valor = texto.ToString().Trim();

            if (string.IsNullOrEmpty(valor))
                return null;

            if (DateTime.TryParseExact(
                valor,
                ObtenerFormatoValido(),
                CultureInfo.InvariantCulture,
                DateTimeStyles.None, out DateTime fecha))
            {
                return fecha;
            }
            return null;
        }

        public static string GenerarCadenaArreglo(string[] arreglo)
        {
            string folios = "";
            foreach (string item in arreglo)
            {
                folios += item + ", ";
            }
            return folios;
        }

        /// <summary>
        /// Evalua si la fecha tiene un formato valido
        /// </summary>
        /// <param name="texto"></param>
        /// <returns>true/false</returns>
        public bool ValidarFecha(object texto)
        {
            return ConvertirTextoAFecha(texto).HasValue;
        }

        /// <summary>
        /// Calcula la CLABE completa con dígito verificado
        /// LEGACY: CuentaClabe
        /// </summary>
        /// <param name="cuenta"></param>
        /// <returns>string</returns>
        /// <exception cref="ArgumentException"></exception>
        public static string CuentaClabe(string cuenta)
        {
            if (string.IsNullOrWhiteSpace(cuenta))
                throw new ArgumentException("La cuenta no puede ser vacia.");

            if (!cuenta.All(char.IsDigit))
                throw new ArgumentException("La cuenta debe contener solo números.");

            string cuentaBase = EnumRutasGenerales.Prefijo_Banco + cuenta.PadLeft(11, '0');

            int suma = 0;
            for (int i = 0; i < 17; i++)
            {
                int digito = cuentaBase[i] - '0';
                int actor = EnumRutasGenerales.Multiplicadores[i] - '0';

                suma += (digito * actor) % 10;
            }

            int digitoVerificador = (10 - (suma % 10)) % 10;

            return cuentaBase + digitoVerificador;
        }

        #region Metodos Encoding

        /// <summary>
        /// Centralizado de caracteres inválidos
        /// </summary>
        public static readonly byte[] CaracterInvalido =
        {
            33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 45, 47, 58, 59, 60, 61,
            62, 63, 64, 91, 92, 93, 94, 95, 96, 123, 124, 125, 126, 127, 128, 129, 130,
            131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145,
            146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160,
            161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173
        };

        /// <summary>
        /// Normalizacion
        /// </summary>
        private static readonly (string oldChar, string newChar)[] RemplazoCaracter =
        {
            ("Á", "A"),
            ("É", "E"),
            ("Í", "I"),
            ("Ó", "O"),
            ("Ú", "U"),
            ("Ä", "A"),
            ("Ë", "E"),
            ("Ï", "I"),
            ("Ö", "O"),
            ("Ü", "U"),
            ("À", "A"),
            ("È", "E"),
            ("Ì", "I"),
            ("Ò", "O"),
            ("Ù", "U"),
            ("Ñ", "N"),
        };

        /// <summary>
        /// Normaliza y codifica texto con caracteres especiales a formato especifico
        /// </summary>
        /// <param name="texto"></param>
        /// <returns>string</returns>
        private static string NormalizarYCodificarTexto(object texto)
        {
            if (texto == null || string.IsNullOrWhiteSpace(texto.ToString()))
                return string.Empty;

            string textoStr = texto.ToString().ToUpper();

            // Normalizar caracteres especiales
            foreach (var (anteriorChar, nuevoChar) in RemplazoCaracter)
            {
                textoStr = textoStr.Replace(anteriorChar, nuevoChar);
            }

            // Codificar a 850
            Encoding encoding850 = Encoding.GetEncoding(850);
            byte[] textoBytes = encoding850.GetBytes(textoStr);

            StringBuilder sb = new StringBuilder(textoBytes.Length);

            foreach (byte b in textoBytes)
            {
                if (!CaracterInvalido.Contains(b))
                {
                    sb.Append(encoding850.GetString(new[] { b }));
                }
            }
            return sb.ToString().TrimEnd();
        }

        /// <summary>
        /// Convierte texto con caracteres especiales para formato valido
        /// </summary>
        /// <param name="texto"></param>
        /// <returns>string</returns>
        public static string TextoSPEI(object texto)
        {
            return NormalizarYCodificarTexto(texto);
        }

        /// <summary>
        /// Inicializa una nueva instancia de DameTexto.
        /// </summary>
        public static string DameTexto(object texto, uint largo = 0)
        {
            if (texto == null || texto == DBNull.Value)
                return string.Empty;

            string textoStr = Convert.ToString(texto);

            if (largo > 0 && textoStr.Length > largo)
                return textoStr.Substring(0, (int)largo);

            return textoStr;
        }

        /// <summary>
        /// Convierte texto con caracteres especiales para formato SPID
        /// </summary>
        /// <param name="texto"></param>
        /// <returns>string</returns>
        public static string TextoSPID(object texto)
        {
            return NormalizarYCodificarTexto(texto);
        }

        /// <summary>
        /// Convierte texto con caracteres especiales para formato TEF
        /// </summary>
        /// <param name="texto"></param>
        /// <returns>string</returns>
        public static string TextoTEF(object texto)
        {
            return NormalizarYCodificarTexto(texto);
        }
        
        /// <summary>
        /// Convierte texto con caracteres especiales para formato BESS
        /// </summary>
        /// <param name="texto"></param>
        /// <returns>string</returns>
        public static string TextoBESS(object texto)
        {
            return NormalizarYCodificarTexto(texto);
        }

        #endregion Metodos Encoding


        #region Metodos Validacion

        /// <summary>
        /// Valida formato de ABA
        /// </summary>
        /// <param name="aba"></param>
        /// <returns>true/false</returns>
        public static bool EsABA(string aba)
        {
            if (string.IsNullOrWhiteSpace(aba))
                return false;

            Regex expresion = new Regex(@"^(/\?[N])?[0-9]{9}?$");
            return expresion.IsMatch(aba);
        }

        /// <summary>
        /// Valida formato de codigo SWIFT
        /// </summary>
        /// <param name="swift"></param>
        /// <returns>true/false</returns>
        public static bool EsSWIFT(string swift)
        {
            if (string.IsNullOrWhiteSpace(swift))
                return false;

            Regex expresion = new Regex(@"^([a-zA-Z]){4}([a-zA-Z]){2}([0-9a-zA-Z]){2}([0-9a-zA-Z]{3})?$");
            return expresion.IsMatch(swift);
        }

        /// <summary>
        /// Valida formato de RFC
        /// </summary>
        /// <param name="rfc"></param>
        /// <returns>true/false</returns>
        public static bool EsRFC(string rfc)
        {
            if (string.IsNullOrWhiteSpace(rfc))
                return false;

            Regex expresion = new Regex(@"^[A-Z,Ñ,&]{3,4}[0-9]{2}[0-1][0-9][0-3][0-9][A-Z,0-9]?[A-Z,0-9]?[0-9,A-Z]?$");
            return expresion.IsMatch(rfc);
        }

        /// <summary>
        /// Valida formato de CURP
        /// </summary>
        /// <param name="curp"></param>
        /// <returns>true/false</returns>
        public static bool EsCURP(string curp)
        {
            if (string.IsNullOrWhiteSpace(curp))
                return false;

            Regex expresion = new Regex(@"^([A-Z,Ñ,&]{3,4}([0-9]{2})(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-9]|3[0-1])[A-Z|\d]{3})$");
            return expresion.IsMatch(curp);
        }

        /// <summary>
        /// Inicializa una nueva instancia de EnmascaraCuenta.
        /// </summary>
        public static string EnmascaraCuenta(string cuenta)
        {
            if (string.IsNullOrWhiteSpace(cuenta))
                return string.Empty;

            if (cuenta.Length == 18)
            {
                return $"{cuenta.Substring(0, 10)}****{cuenta.Substring(15, 3)}*";
            }
            else
            {
                int start = Math.Max(0, cuenta.Length - 3);
                int length = Math.Min(3, cuenta.Length - start);

                return $"****{cuenta.Substring(start, length)}";
            }
        }

        #endregion Metodos Validacion

        #region Metodo Desencripta

        /// <summary>
        /// Desencripta una clave
        /// </summary>
        /// <param name="claveEncriptada"></param>
        /// <returns>string</returns>
        public static string DesencriptaClave(string claveEncriptada)
        {
            if (string.IsNullOrWhiteSpace(claveEncriptada))
                return string.Empty;

            try
            {

                claveEncriptada = claveEncriptada.Trim();
                int longitud = claveEncriptada.Length;
                if(longitud % 3 != 0)
                    return string.Empty;

                StringBuilder sb = new StringBuilder(longitud);

                for(int i=0; i < longitud; i++)
                {
                    sb.Append(MapearCaracter(claveEncriptada[i]));
                }
                int posiciones = longitud / 3;

                string parte1 = sb.ToString().Substring(0, posiciones);
                string parte2 = sb.ToString().Substring(posiciones, posiciones);
                string parte3 = sb.ToString().Substring(posiciones*2, posiciones);

                StringBuilder resultado = new StringBuilder(longitud);
                for(int i=0; i<posiciones; i++)
                {
                    resultado.Append(parte3[i]);
                    resultado.Append(parte1[i]);
                    resultado.Append(parte2[i]);
                }

                StringBuilder claveDesencriptada = new StringBuilder(longitud);
                string res = resultado.ToString();

                for(int i=0; i < posiciones; i++)
                {
                    claveDesencriptada.Append(res.Substring(i * 3, 3));
                }

                return claveDesencriptada.ToString();
            }
            catch
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Ejecuta ConvertirListaADataTable.
        /// </summary>
        public static DataTable ConvertirListaADataTable<T>(List<T> lista)
        {
            DataTable dt = new DataTable();

            if (lista == null || lista.Count == 0)
                return dt;

            T primerElemento = lista.FirstOrDefault();

            if (primerElemento == null)
                return dt;

            // Caso 1: la lista viene como Dictionary<string, object> o similar
            if (primerElemento is IDictionary<string, object> diccionarioPrimerElemento)
            {
                foreach (var key in diccionarioPrimerElemento.Keys)
                {
                    Type tipoColumna = typeof(object);

                    object valor = diccionarioPrimerElemento[key];

                    if (valor != null)
                        tipoColumna = Nullable.GetUnderlyingType(valor.GetType()) ?? valor.GetType();

                    dt.Columns.Add(key, tipoColumna);
                }

                foreach (T item in lista)
                {
                    var diccionario = item as IDictionary<string, object>;

                    DataRow row = dt.NewRow();

                    foreach (DataColumn columna in dt.Columns)
                    {
                        object valor = null;

                        if (diccionario != null && diccionario.ContainsKey(columna.ColumnName))
                            valor = diccionario[columna.ColumnName];

                        row[columna.ColumnName] = valor ?? DBNull.Value;
                    }

                    dt.Rows.Add(row);
                }

                return dt;
            }

            // Caso 2: clase DTO normal
            PropertyInfo[] propiedades = primerElemento.GetType()
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead)
                .ToArray();

            foreach (PropertyInfo propiedad in propiedades)
            {
                Type tipoColumna = Nullable.GetUnderlyingType(propiedad.PropertyType) ?? propiedad.PropertyType;

                dt.Columns.Add(propiedad.Name, tipoColumna);
            }

            foreach (T item in lista)
            {
                DataRow row = dt.NewRow();

                foreach (PropertyInfo propiedad in propiedades)
                {
                    object valor = propiedad.GetValue(item, null);

                    row[propiedad.Name] = valor ?? DBNull.Value;
                }

                dt.Rows.Add(row);
            }

            return dt;
        }

        /// <summary>
        /// Mapea un caracter a su equivalente numerico
        /// </summary>
        /// <param name="c">caracter</param>
        /// <returns>string</returns>
        private static string MapearCaracter(char c)
        {
            switch (c)
            {
                case 'W': return "0";
                case 'f': return "1";
                case 'h': return "2";
                case 'T': return "3";
                case 'N': return "4";
                case 'x': return "5";
                case '3': return "6";
                case 'C': return "7";
                case 'I': return "8";
                case 's': return "9";
                default: return c.ToString();
            }
        }
        #endregion 

        /// <summary>
        /// Obtiene la version del ensamblado actual
        /// </summary>
        /// <returns>string</returns>
        public static string ObtenerVersion()
        {
            return Assembly.GetExecutingAssembly().GetName().Version.ToString();
        }

        /// <summary>
        /// Obtiene una subcadena de manera segura
        /// </summary>
        public static string SafeSubstring(string value, int startIndex, int length)
        {
            if (string.IsNullOrEmpty(value))
                return string.Empty;

            if (startIndex < 0)
                startIndex = 0;

            if (startIndex >= value.Length)
                return string.Empty;

            if (startIndex + length > value.Length)
                length = value.Length - startIndex;

            return value.Substring(startIndex, length);
        }

        /// <summary>
        /// Obtiene una subcadena desde un índice de manera segura
        /// </summary>
        public static string SafeSubstringFrom(string value, int startIndex)
        {
            if (string.IsNullOrEmpty(value))
                return string.Empty;

            if (startIndex < 0)
                startIndex = 0;

            if (startIndex >= value.Length)
                return string.Empty;

            return value.Substring(startIndex);
        }

        /// <summary>
        /// Inicializa una nueva instancia de Left.
        /// </summary>
        public static string Left(string s, int n)
        {
            if (string.IsNullOrEmpty(s)) return string.Empty;
            return s.Length <= n ? s : s.Substring(0, n);
        }

        /// <summary>
        /// Inicializa una nueva instancia de DameTextoComoCLng.
        /// </summary>
        public static string DameTextoComoCLng(object valor)
        {
            string s = DameTexto(valor);
            // Emula CLng: si no cabe en Int64, lanza (igual que VB).
            long n = long.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
            return n.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Inicializa una nueva instancia de Mid.
        /// </summary>
        public static string Mid(string value, int start1Based, int length)
        {
            if (string.IsNullOrEmpty(value)) return string.Empty;
            int startIndex = Math.Max(0, start1Based - 1);
            if (startIndex >= value.Length) return string.Empty;
            int maxLen = Math.Min(length, value.Length - startIndex);
            return maxLen <= 0 ? string.Empty : value.Substring(startIndex, maxLen);
        }

        /// <summary>
        /// Inicializa una nueva instancia de IniciaComunes.
        /// </summary>
        [Obsolete("Legacy compatibility only.")]
        public static void IniciaComunes(RutasConfiguracion rutas = null)
        {
            Rutas = rutas;
        }

        /// <summary>
        /// Obtiene el archivo a escribir para LOGS
        /// </summary>
        /// <returns>string</returns>
        [Obsolete("Legacy compatibility only.")]
        public string ObtenerRutaLog()
        {
            return Rutas?.MUFG_Servicios_Path_Log;
        }

        /// <summary>
        /// Establece el archivo de log y crea el directorio si no existe
        /// </summary>
        /// <param name="rutaArchivo"></param>
        [Obsolete("Legacy compatibility only.")]
        public static void CreaRutaArchivoSalida(string rutaArchivo)
        {
            if (string.IsNullOrWhiteSpace(rutaArchivo))
            {
                throw new ArgumentException("El directorio del log no puede ser vacío.");
            }
            try
            {
                string directorio = Path.GetDirectoryName(rutaArchivo);
                if (!Directory.Exists(directorio))
                {
                    Directory.CreateDirectory(directorio);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error al establecer el archivo de logs", ex);
            }
        }

        /// <summary>
        /// Obtiene ruta y nombre final del archivo de log con fecha
        /// </summary>
        [Obsolete("Legacy compatibility only.")]
        public static string ObtenerNombreArchivoLog(string nombreArchivo = null)
        {
            if (Rutas == null || string.IsNullOrWhiteSpace(Rutas.MUFG_Servicios_Path_Log))
            {
                throw new InvalidOperationException("El archivo de log no ha sido inicializado");
            }
            return $"{Rutas.MUFG_Servicios_Path_Log}{nombreArchivo}_{DateTime.Now:yyyyMMdd}.txt";
        }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MUFG_T24.Modelos
{
    /// <summary>
    /// Representa CampoGeneral.
    /// </summary>
    public class CampoGeneral
    {
        public long Folio_maestro { get; set; }
        public string Alias_proceso { get; set; }
        public int Num_regs { get; set; }
        public string T24_folio_asignado { get; set; }
        public string short_name { get; set; }
    }
}

namespace MUFG_T24.Comunes
{
    /// <summary>
    /// Enum para esquema
    /// </summary>
    public static class EsquemaColumnas
    {
        public const string NOMBRECOLUMNA = "ColumnName";
        public const string TIPODATO = "DataType";
        public const string PERMITIRDBNULL = "AllowDBNull";
        public const string ESUNICO = "IsUnique";
        public const string PRESICIONNUMERICA = "NumericPrecision";
        public const string ESCALANUMERICA = "NumericScale";
        public const string TAMAÑOCOLUMNAS = "ColumnSize";
        public const string TIPODATOPROVEEDOR = "ProviderSpecificDataType";

        public const string PRESICION = "precision";
        public const string ESCALA = "scale";
        public const string TAMAÑO = "size";

        #region PROPIEDADES
        public const string CONTADORCOLUMNAS = "ColumnName";
        public const string CAMPOS = "fields";
        public const string TIPOS = "types";
        public const string DATOS = "data";
        public const string TABLA = "TAS_Table";
        #endregion PROPIEDADES

        #region MENSAJES_BD
        public const string VERSION_DESCONOCIDA = "Version base de datos esconocida";
        public const string BASEDATOS_DESCONICIDA = "Nombre base de datos desconocido";
        public const string SERVIDOR_DESCONOCIDO = "Nombre servidor desconocido";
        #endregion MENSAJES_BD
    }
}
