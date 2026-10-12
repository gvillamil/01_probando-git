using Calypso_Forex.Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Calypso_Forex.Infraestructura.Repository
{
    public class CalypsoRepository : RepositorioMaestro, ICalypso
    {
        #region Constructor

        /// <summary>
        /// Constructor del servicio
        /// </summary>
        public CalypsoRepository() : base(BasedatosProveedor.Oracle, NombreBasedatos.CALYPSO, null, null)
        {
        }

        #endregion

        #region Métodos

        /// <summary>
        /// Método para obtener el nombre corto acorde al CDL
        /// </summary>
        /// <param name="cdl"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task<string> ObtenerNombreCorto(string cdl)
        {
            //Falla este método
            string respuesta = await CalypsoObtenerNombreCorto(cdl);

            if (respuesta == null)
                throw new Exception();

            return respuesta;
        }

        /// <summary>
        /// Deserializa un xml y lo asigna a un objeto
        /// </summary>
        /// <param name="mensaje"></param>
        /// <param name="ack"></param>
        /// <param name="tipoSerializacion"></param>
        /// <param name="tag"></param>
        /// <param name="nodo"></param>
        /// <returns></returns>
        public bool Deserializar(string mensaje, ref object ack,
                                        TipoSerializacion tipoSerializacion, bool tag, string nodo)
        {
            bool resultado = CalypsoDeserializarClase(mensaje, ref ack, tipoSerializacion, tag, nodo);
            return resultado;
        }

        /// <summary>
        /// Serializa un objeto a un string que contiene un XML
        /// </summary>
        /// <param name="objeto"></param>
        /// <param name="tipo"></param>
        /// <param name="RemoveDiacritics"></param>
        /// <param name="AddNode"></param>
        /// <param name="encodingEnum"></param>
        /// <param name="rootNodeName"></param>
        /// <returns></returns>
        public string Serialiar(
                    object objeto, TipoSerializacion tipo, bool RemoveDiacritics, bool AddNode,
                    EncodingEnum encodingEnum = EncodingEnum.Encode_Default, string rootNodeName = "")
        {
            return CalypsoSerializarClase(objeto, tipo, RemoveDiacritics, AddNode, encodingEnum, rootNodeName);
        }

        #endregion
    }
}
------------------------------------------------------------------------------------------------------------------------------------------------------
using Calypso_Forex.Aplicacion;
using Calypso_Forex.Aplicacion.Enums;
using Calypso_Forex.Aplicacion.Interfaces;
using Calypso_Forex.Aplicacion.Servicios;
using Calypso_Forex.Infraestructura.Repository;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.Diagnostics;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace Calypso_Forex
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : ServiceBase
    {
        #region Propiedades

        private Container _containerDI;
        private Configuracion _configuracionBase;

        private System.Timers.Timer _timerServicio;
        private CancellationTokenSource _cancellationTokenSource;

        private Task _startupTask;
        private Task _runningTask = Task.CompletedTask;
        private readonly SemaphoreSlim _tickLock = new SemaphoreSlim(1, 1);

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor 
        /// </summary>
        public ServiceHost() { }

        #endregion

        #region Ciclo vida servicio

#if DEBUG
        //private static bool _debugSingleShot = false;
        public void DebugRun(string[] args)
        {
            OnStart(args);
            Console.WriteLine("Servicio iniciado en modo DEBUG. Presiona ENTER para detener...");
            Console.ReadLine();
            OnStop();
        }
#endif

        /// <summary>
        /// OnStart de contrato, usado para correr la Inicializacion y ParametrizacionesAsync.
        /// Sin bloquear OnStart (no puede ser async por contrato).
        /// </summary>
        protected override void OnStart(string[] args)
        {
            try
            {
                //Ejecuta la inicializacion
                _cancellationTokenSource = new CancellationTokenSource();
                Inicializacion(args, _cancellationTokenSource.Token);
                _startupTask = ParametrizacionesAsync(_cancellationTokenSource.Token);

                _startupTask.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        var ex = t.Exception;
                        Config.EscribirLog(Constantes.LogMensaje.ERROR_ON_START);

                        this.ExitCode = 1;
                        this.Stop();
                        return;
                    }

                    if (_timerServicio.Enabled)
                        _timerServicio.Start();

                }, TaskScheduler.Default);
            }
            catch (Exception ex)
            {
                Console.WriteLine(Constantes.LogMensaje.ERROR_ARCHIVO_CONFIGURACION + ex.Message);

                try
                {
                    if (!EventLog.SourceExists(Constantes.General.NOMBRE_SERVICIO))
                        EventLog.CreateEventSource(Constantes.General.NOMBRE_SERVICIO, Constantes.General.APPLICATION);

                    EventLog.WriteEntry(
                        Constantes.General.NOMBRE_SERVICIO,
                        Constantes.LogMensaje.ERROR_ON_START + ex,
                        EventLogEntryType.Error);
                }
                catch
                {
                    Config.EscribirLog(Constantes.LogMensaje.INFO_ONSTART_ULTIMO_FALLBACK);
                }

                this.ExitCode = 1;
                this.Stop();
            }
        }

        /// <summary>
        /// Ejecuta el funcionamiento principal del servicio en cada tick.
        /// </summary>  
        private async Task OnTimerElapsedAsync()
        {
            //Condicional que bloquea otros ticks si un tick anterior sigue corriendo.
            if (_cancellationTokenSource?.IsCancellationRequested == true)
                return;

            //Condicional que bloquea otros ticks si un tick anterior sigue corriendo.
            if (!await _tickLock.WaitAsync(0))
                return;

            try
            {
                //Ejecuta el procedimiento principal del servicio.
                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    var operaciones = _containerDI.GetInstance<IOperacionServicio>();
                    _runningTask = operaciones.EjecutarProcedimientosAsync();
                    await _runningTask.ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog(Constantes.LogMensaje.ERROR_REALIZA_TIMER + ex);
            }
            finally
            {
                _tickLock.Release();
            }
        }

        /// <summary>
        /// OnStop de contrato, usado para correr TerminacionAsync en async.
        /// Sin bloquear OnStop (no puede ser async por contrato).
        /// </summary>
        protected override void OnStop()
        {
            try
            {
                //Detiene el timer y señaliza cancelación
                _timerServicio?.Stop();
                _cancellationTokenSource?.Cancel();

                //Ejecuta la terminación
                _cancellationTokenSource = new CancellationTokenSource();
                CancellationToken token = _cancellationTokenSource?.Token ?? CancellationToken.None;
                TerminacionAsync(_cancellationTokenSource.Token).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Config.EscribirLog(Constantes.LogMensaje.ERROR_ON_STOP + ex);
            }
            finally
            {
                _cancellationTokenSource?.Dispose();
            }
        }

        #endregion

        #region Procesos Complementarios

        /// <summary>
        /// Realiza la inyección y establece un timer
        /// que ejecutará su delegado por tick del intervalo.
        /// </summary>
        private void Inicializacion(string[] args, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            //Realiza la configuracion del _containerDI.
            _containerDI = new SimpleInjector.Container();
            _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
            _containerDI.Register<IAlertas>(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);
            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();

            //Almacena configuración necesaria para el funcionamiento del servicio
            _configuracionBase = new Configuracion();

            //Inicializa la obtencia de la configuración general y las rutas del log
            {
                ConfiguracionEncriptadaHelper.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion
                    = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Calypso_Forex.ToString());

                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.RutaArchivoConfiguracion = $@"{EnumRutasGenerales.Path_Log_CMS}\{NombreServicio.Calypso_Forex.ToString()}\";
                _configuracionBase.PathLog = servicioMainConfiguracion.Log + servicioMainConfiguracion.NombreServicio;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.configuracion = servicioMainConfiguracion.configuracion;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;
                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(Constantes.General.PARA);

                RutasConfiguracion rutas = new RutasConfiguracion()
                {
                    MUFG_Servicios_Path_Log = _configuracionBase.PathLog
                };

                Config.TamanoMaximoArchivoKB = Convert.ToInt32(_configuracionBase.TamanoMaximoArchivoLogKB);
                Config.AjustarRutas = true;
                Config.IniciarRutas(rutas);
                Config.CreaRutaArchivoSalida(Config.ObtenerNombreArchivoLog());
                Config.EscribirLog(Constantes.LogMensaje.INFO_LEYENDO_ARCHIVO_CONFIGURACION);
            }

            //Registra la configuracion como Scoped + Factory (lazy)
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);

            //Registra los componentes
            _containerDI.Register<ITomi, TomiRepository>();
            _containerDI.Register<ICalypso, CalypsoRepository>();
            _containerDI.Register<IOperacionServicio, OperacionServicio>();
            _containerDI.Register<IOperacionesForex, OperacionesForexService>();

            //Inicializa el timer y establece el delegado
            _timerServicio = new System.Timers.Timer
            {
                Enabled = _configuracionBase.TimerEnabled ?? false,
                AutoReset = true,
                Interval = _configuracionBase.TimerInterval ?? 10000
            };

            _timerServicio.Elapsed += async (sender, e) =>
            {
                if (_cancellationTokenSource?.IsCancellationRequested == true)
                    return;

                try
                {
                    await OnTimerElapsedAsync();
                }
                catch (Exception ex)
                {
                    Config.EscribirLog(Constantes.LogMensaje.ERROR_ELAPSED + ex);
                }
            };
        }

        /// <summary>
        /// Realiza la obtención inicial de parámetros (una sola vez).
        /// </summary>
        private async Task ParametrizacionesAsync(CancellationToken cancellationToken)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                //Obtiene el proceso actual.
                IParametrizacion _parametrizacion = _containerDI.GetInstance<IParametrizacion>();
                _configuracionBase = await _parametrizacion.ObtenerProcesoActualAsync(_configuracionBase);

                //Marca el servicio en uso.
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
                await _controlServicio.MarcarPuertoEnUsoAsync(
                    _configuracionBase.Pid.Value,
                    _configuracionBase.NombreProceso,
                    _configuracionBase.NombreDisplay,
                    _configuracionBase.NombreServicio
                );
            }
            catch (OperationCanceledException ex)
            {
                Config.EscribirLog(Constantes.LogMensaje.ERROR_PARAMETRIZACIONES + ex);
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(Constantes.LogMensaje.ERROR_PARAMETRIZACIONES + ex);
                throw;
            }
        }

        /// <summary>
        /// Realiza ciertas operaciones antes de finalizar el servicio.
        /// </summary>
        private async Task TerminacionAsync(CancellationToken cancellationToken)
        {
            Config.EscribirLog(Constantes.LogMensaje.INFO_ONSTOP_FINALIZANDO);

            try
            {
                //Señala que debe haber cancelación.
                _cancellationTokenSource?.Cancel();

                //Si hay un procesamiento activo, lo espera.
                if (_runningTask != null)
                {
                    await Task.WhenAny(
                        _runningTask,
                        Task.Delay(TimeSpan.FromSeconds(60), cancellationToken)
                    );
                }
                ;

                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    Configuracion _configuracion = _containerDI.GetInstance<Configuracion>();
                    IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();

                    //Marca el servicio en uso.
                    await _controlServicio
                        .MarcarPuertoEnUsoAsync(null, null, null, _configuracion.NombreServicio)
                        .ConfigureAwait(false);

                    //Marca el termino del servicio.
                    await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio);
                }
                Config.EscribirLog(Constantes.LogMensaje.INFO_ONSTOP_TERMINACION);
            }
            catch (OperationCanceledException ex)
            {
                Config.EscribirLog(Constantes.LogMensaje.ERROR_TERMINACION_OPERACION_ASYNC + ex);
            }
            catch (Exception ex)
            {
                Config.EscribirLog(Constantes.LogMensaje.ERROR_TERMINACION_ASYNC + ex);
            }
        }
        #endregion
    }
}
-------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------------
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
