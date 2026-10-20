using AplicacionSwfINMQMX.Enums;
using AplicacionSwfINMQMX.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Threading;
using System.Threading.Tasks;
using IOperacionServicio = AplicacionSwfINMQMX.Interfaces.IOperacionServicio;

namespace AplicacionSwfINMQMX
{
    /// <summary>
    /// Clase principal para ejecutar procedimientos.
    /// </summary>
    public class OperacionServicio : IOperacionServicio
    {
        private readonly Configuracion _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IAlertas _alertas;
        private readonly IParametrizacion _parametrizacion;
        private readonly IRepositorioTomi _repositorioTomi;

        /// <summary>
        /// Constructor para ejecución de operaciones.
        /// </summary>
        /// <param name="configuracion"></param>
        /// <param name="controlServicio"></param>
        /// <param name="alertas"></param>
        /// <param name="parametrizacion"></param>
        /// <param name="repositorioTomi"></param>
        public OperacionServicio(Configuracion configuracion, IControlServicio controlServicio, IAlertas alertas, IParametrizacion parametrizacion, IRepositorioTomi repositorioTomi)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _alertas = alertas;
            _parametrizacion = parametrizacion;
            _repositorioTomi = repositorioTomi;
        }

        /// <summary>
        /// Método principal de ejecución de operaciones.
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task<bool> EjecutarProcedimientosAsync(CancellationToken ct)
        {
            try
            {
                //Marca la ejecución del servicio...
                var resultadoEjecucion = await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct);

                if (resultadoEjecucion == null || !resultadoEjecucion.Exito)
                {
                    Config.EscribirLog($"ERROR|ActualizaEJecucion no devolvio resultado.|No se pudo actualizar el inicio de la ejecución correctamente.");
                    return false;
                }

                //Obtiene los parámetros del sistema...
                _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync(ct);
                _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil();
                _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesServicio.ParametrosSistema, ConstantesServicio.ParametroWsT24MwPassword, ct);

                int pidAlerta = _configuracion.Pid.HasValue ? _configuracion.Pid.Value : 0;
                //Valida que sea día hábil...
                if (!_configuracion.EsDiaHabil.Value)
                {
                    Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.MensajeEjecuta}|{ConstantesServicio.MensajeDiaInhabil}");
                    await _alertas.InsertarAlerta(new AlertaDTO()
                    {
                        Folio = null,
                        TipoOPeracion = null,
                        Servicio = _configuracion.NombreServicio,
                        ClaveAviso = ConstantesClaveAviso.Error,
                        Descipcion = ConstantesServicio.MensajeDiaInhabil,
                        PId = pidAlerta,
                        NombreProceso = _configuracion.NombreProceso,
                        Exception = new Exception(ConstantesServicio.MensajeDiaInhabil)
                    });
                    return false;
                }
                TimeSpan horaInicio = TimeSpan.Zero;
                TimeSpan horaFin = TimeSpan.Zero;
                DateTime fecha = new DateTime();


                if (!DateTime.TryParseExact(_configuracion.FechaSistema, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var result))
                {
                    Config.EscribirLog($"Fecha invalida: '{_configuracion.FechaSistema}'");
                    return false;
                }
                else { fecha = result; }

                switch (fecha.DayOfWeek)
                {
                    case DayOfWeek.Monday:
                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_LU_INI], out var Monday))
                            horaInicio = Monday;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{Monday}.");

                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_LU_FIN], out var MondayFin))
                            horaFin = MondayFin;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{MondayFin}.");
                        break;
                    case DayOfWeek.Tuesday:
                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_MA_INI], out var Tuesday))
                            horaInicio = Tuesday;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{Tuesday}.");

                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_MA_FIN], out var TuesdayFin))
                            horaFin = TuesdayFin;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{TuesdayFin}.");
                        break;
                    case DayOfWeek.Wednesday:
                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_MI_INI], out var Wednesday))
                            horaInicio = Wednesday;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{Wednesday}.");

                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_MI_FIN], out var WednesdayFin))
                            horaFin = WednesdayFin;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{WednesdayFin}.");
                        break;
                    case DayOfWeek.Thursday:
                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_JU_INI], out var Thursday))
                            horaInicio = Thursday;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{Thursday}.");

                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_JU_FIN], out var ThursdayFin))
                            horaFin = ThursdayFin;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{ThursdayFin}.");
                        break;
                    case DayOfWeek.Friday:
                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_VI_INI], out var Friday))
                            horaInicio = Friday;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{Friday}.");

                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_VI_FIN], out var FridayFin))
                            horaFin = FridayFin;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{FridayFin}.");
                        break;
                    case DayOfWeek.Saturday:
                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_SA_INI], out var Saturday))
                            horaInicio = Saturday;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{Saturday}.");

                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_SA_FIN], out var SaturdayFin))
                            horaFin = SaturdayFin;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{SaturdayFin}.");
                        break;
                    case DayOfWeek.Sunday:
                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_DO_INI], out var Sunday))
                            horaInicio = Sunday;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{Sunday}.");

                        if (TimeSpan.TryParse(_configuracion.ParametrosSistema[ConstantesServicio.HORA_OP_SWF_DO_FIN], out var SundayFin))
                            horaFin = SundayFin;
                        else
                            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.ErrorHora}:{SundayFin}.");
                        break;
                }

                if (horaInicio == TimeSpan.Zero && horaFin == TimeSpan.Zero)
                {
                    Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.MensajeEjecuta}|{ConstantesServicio.MensajeErrorFechaRecuperada}");
                    return false;
                }

                //Valida el horario de operación...
                TimeSpan timeHoraSistema = _parametrizacion.ObtenerHoraSistema();

                if (timeHoraSistema < horaInicio || timeHoraSistema > horaFin)
                {
                    Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.MensajeEjecuta}|{ConstantesServicio.MensajeFueraDeServicio}");
                    await _alertas.InsertarAlerta(new AlertaDTO()
                    {
                        Folio = null,
                        TipoOPeracion = null,
                        Servicio = _configuracion.NombreServicio,
                        ClaveAviso = ConstantesClaveAviso.Error,
                        Descipcion = ConstantesServicio.MensajeFueraDeServicio,
                        PId = pidAlerta,
                        NombreProceso = _configuracion.NombreProceso,
                        Exception = new Exception($"El servicio '{_configuracion.NombreServicio}' está fuera de horario.")
                    });
                    return false;
                }

                //Recupera los datos de procesamiento...
                var respuesta = await _repositorioTomi.ObtieneLecturaMensajesMQMX();
                if (respuesta)
                    Config.EscribirLog(ConstantesServicio.MensajeEjecutoCorrectamente);

                //Marca el termino del servicio...
                await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);

                return respuesta;

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.MensajeEjecuta}|Se presenta exception: {ex}");
                await _alertas.InsertarAlerta(new AlertaDTO()
                {
                    Folio = null,
                    TipoOPeracion = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = ConstantesClaveAviso.Critico,
                    Descipcion = null,
                    PId = _configuracion.Pid.Value,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = ex
                });
            }
            return false;
        }
    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Data;
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
            int pId,
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
        /// Inicializa una nueva instancia de InsertarAlerta.
        /// </summary>
        public async Task<bool> InsertarAlerta(AlertaDTO alerta, CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                if (alerta == null)
                    alerta = new AlertaDTO();

                if (alerta.PId < 0)
                    alerta.PId = 0;

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

                int pidSeguro = alerta.PId;

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
                return false;
            }
        }

    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    using AplicacionSwfINMQMX.Enums;
using AplicacionSwfINMQMX.Interfaces;
using InfraestructuraSwfINMQMX.Repositorio;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Logeo_utilidades;
using MUFG_T24.Modelos;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SwfINMQMX
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private Configuracion _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.Swf_IN_MQ_MX.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServiceHost() { }

        /// <summary>
        /// Realiza la inyección y establece un timer
        /// que ejecutará su delegado por tick del intervalo.
        /// </summary>
        protected override void Inicializacion(string[] args, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            //Realiza la configuracion del contenedor DI...
            _containerDI = new SimpleInjector.Container();
            _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
            _containerDI.Register<Alertas>(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);
            _containerDI.Register<IAlertas>(() => _containerDI.GetInstance<Alertas>(), Lifestyle.Singleton);
            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();

            //Objeto que almacena la configuración necesaria para el funcionamiento del servicio...
            _configuracionBase = new Configuracion();

            //Inicializa la obtención de la configuración general y el logging...
            {
                ConfiguracionEncriptadaHelper.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Swf_IN_MQ_MX.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.PathLog = servicioMainConfiguracion.Log;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;
                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AsuntoTest);
                _configuracionBase.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                RutasConfiguracion rutas = new RutasConfiguracion()
                {
                    MUFG_Servicios_Path_Log = _configuracionBase.PathLog
                };
                Logging.Init(rutas, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
                Logging.Write(ConstantesServicio.MensajeErrorConfiguracion);
            }

            //Registra la configuracion...
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);

            //Registra los componentes...
            _containerDI.Register<AplicacionSwfINMQMX.OperacionServicio>();
            _containerDI.Register<IRepositorioTomi, RepositorioTomi>();
            _containerDI.Verify();
        }

        /// <summary>
        /// Realiza la obtención inicial de parámetros (una sola vez).
        /// </summary>
        protected override async Task ParametrizacionesAsync(CancellationToken ct)
        {
            try
            {
                //Obtiene el proceso actual...
                IParametrizacion _parametrizacion = _containerDI.GetInstance<IParametrizacion>();
                _configuracionBase = await _parametrizacion.ObtenerProcesoActualAsync(_configuracionBase, ct);

                //Marca el servicio en uso...
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
                await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio, ct);
            }
            catch (OperationCanceledException exc)
            {
                Config.EscribirLog($"{ConstantesServicio.MensajeParametrizaciones}|{exc}");
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"{ConstantesServicio.MensajeParametrizaciones}|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Ejecuta el funcionamiento principal del servicio en cada tick.
        /// </summary>   
        protected override async Task OnTimerAsync(CancellationToken ct)
        {
            try
            {
                //Ejecuta el procedimiento principal del servicio...
                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    var operacionServicio = _containerDI.GetInstance<AplicacionSwfINMQMX.OperacionServicio>();
                    await operacionServicio.EjecutarProcedimientosAsync(ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"{ConstantesServicio.MensajeTimer}|{ex}");
            }
        }

        /// <summary>
        /// Realiza ciertas operaciones antes de finalizar el servicio.
        /// </summary>
        protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            Config.EscribirLog(ConstantesServicio.MensajeTerminacion);
            IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
            try
            {
                if (_containerDI == null || _configuracionBase == null)
                {
                    Config.EscribirLog(ConstantesServicio.MensajeOmitido);
                    return;
                }

                

                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    

                    await _controlServicio.EsperarTareaActivaAsync(startupTask, ConstantesSistema.ParametrizacionInicial, waitCt).ConfigureAwait(false);
                    await _controlServicio.EsperarTareaActivaAsync(runningTask, ConstantesSistema.ProcesamientoActivo, waitCt).ConfigureAwait(false);

                    //Marca el servicio en uso...
                    await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);

                    

                    Config.EscribirLog(ConstantesServicio.MensajeTerminacionExitosa);
                }
            }
            catch (OperationCanceledException exc)
            {
                Config.EscribirLog($"{ConstantesServicio.MensajeOperationCanceledException}|{exc}");
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"{ConstantesServicio.MensajeException}|{ex}");
            }
            finally
            {
                try
                {
                    //Marca el termino del servicio...
                    await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"{ConstantesServicio.MensajeException}|{ex}");
                }
                finally { _containerDI?.Dispose(); }               
                
            }
        }

    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
            CommandType tipoComando = CommandType.Text, bool esVistasT24 = false, int? commandTimeout = null) where T : new()
        {
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

                        await conexion.OpenAsync().ConfigureAwait(false);
                        using (DbDataReader lector = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            Dictionary<string, int> columnas = ObtenerColumnasReader(lector);
                            PropertyInfo[] propiedades = tipoDTO.GetProperties();
                            MethodInfo cloneMetodo = tipoDTO.GetMethod("Clone", Type.EmptyTypes);

                            while (await lector.ReadAsync().ConfigureAwait(false))
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
        public async Task<string> CalypsoObtenerNombreCorto(string cdl)
        {
            LegalEntity resultado = (await EjecutarReader<LegalEntity>(
                "SELECT short_name FROM LEGAL_ENTITY WHERE LPAD(external_ref,8,'0') = :cdl", cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro(":cdl", cdl.PadLeft(8, '0')));
                }
            )).Data.FirstOrDefault();

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
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    namespace AplicacionSwfINMQMX.Enums
{
    /// <summary>
    /// Clase que se utiliza para colocar constantes de manera parametrizada.
    /// </summary>
    public class ConstantesServicio
    {
        //Constantes de parametrización...
        public const string ParametrosSistema = "'HORA_OP_SWF_LU_INI','HORA_OP_SWF_LU_FIN','HORA_OP_SWF_MA_INI','HORA_OP_SWF_MA_FIN','HORA_OP_SWF_MI_INI','HORA_OP_SWF_MI_FIN','HORA_OP_SWF_JU_INI','HORA_OP_SWF_JU_FIN','HORA_OP_SWF_VI_INI','HORA_OP_SWF_VI_FIN','HORA_OP_SWF_SA_INI','HORA_OP_SWF_SA_FIN','HORA_OP_SWF_DO_INI','HORA_OP_SWF_DO_FIN','MQ.SWF_IN.MANAGER_NAME_MX','MQ.SWF_IN.HOST_MX','MQ.SWF_IN.PORT_MX','MQ.SWF_IN.CHANNEL_MX','MQ.SWF_IN.QUEUE_NAME_MX','MAIL_PSG','MQ.SWF_IN.USER_ID_MX'";          
        public const string ParametroWsT24MwPassword = "'MQ.SWF_IN.PASSWORD_MX','MQ.SWF_IN.SSL_CIPHERSPEC_MX','MQ.SWF_IN.SSL_KEYREPOSITORY_MX','MQ.SWF_IN.CERTIFICATE_LABEL_MX'";

        //Constantes de lógica...
        public const string TipoOperacion = "SWIFT_IN";
        public const string Servicio = "SWF_IN_MQ_MX";

        //Constantes de SQL...
        public const string uspInsertaMensajeMQMX = "usp_SWF_INSERT_MQ_MSG_MX";
        public const int MaxSpTimeout = 10;
        public const int WaitInterval = 5000;

        //Constantes de Emeal...
        public const string NombreOrigen = "CMS Servicios";
        public const string Para = "MAIL_TO";
        public const string Asunto = "Error en MQ SWIFT_IN";
        public const string AsuntoTest = "MAIL_SUBJECT";
        public const string Mensaje = "ERROR durante la lectura del MQ del Swift_IN para el mensaje= ";
        public const string SenderTest = "MAIL_SENDER_TEST";

        //Constantes de configuracion de MQ_MX...
        public const string MANAGER_NAME_MX = "MQ.SWF_IN.MANAGER_NAME_MX";
        public const string CHANNEL_MX = "MQ.SWF_IN.CHANNEL_MX";
        public const string QUEUE_NAME_MX = "MQ.SWF_IN.QUEUE_NAME_MX";
        public const string HOST_MX = "MQ.SWF_IN.HOST_MX";
        public const string PORT_MX = "MQ.SWF_IN.PORT_MX";
        public const string USER_ID_MX = "MQ.SWF_IN.USER_ID_MX";
        public const string PASSWORD_MX = "MQ.SWF_IN.PASSWORD_MX";
        public const string SSL_CIPHERSPEC_MX = "MQ.SWF_IN.SSL_CIPHERSPEC_MX";
        public const string SSL_KEYREPOSITORY_MX = "MQ.SWF_IN.SSL_KEYREPOSITORY_MX";
        public const string CERTIFICATE_LABEL_MX = "MQ.SWF_IN.CERTIFICATE_LABEL_MX";
        public const string CORREO_PARA_DEFAULT = "MAIL_PSG";

        //Constantes Horario
        public const string HORA_OP_SWF_LU_INI = "HORA_OP_SWF_LU_INI";
        public const string HORA_OP_SWF_LU_FIN = "HORA_OP_SWF_LU_FIN";
        public const string HORA_OP_SWF_MA_INI = "HORA_OP_SWF_MA_INI";
        public const string HORA_OP_SWF_MA_FIN = "HORA_OP_SWF_MA_FIN";
        public const string HORA_OP_SWF_MI_INI = "HORA_OP_SWF_MI_INI";
        public const string HORA_OP_SWF_MI_FIN = "HORA_OP_SWF_MI_FIN";
        public const string HORA_OP_SWF_JU_INI = "HORA_OP_SWF_JU_INI";
        public const string HORA_OP_SWF_JU_FIN = "HORA_OP_SWF_JU_FIN";
        public const string HORA_OP_SWF_VI_INI = "HORA_OP_SWF_VI_INI";
        public const string HORA_OP_SWF_VI_FIN = "HORA_OP_SWF_VI_FIN";
        public const string HORA_OP_SWF_SA_INI = "HORA_OP_SWF_SA_INI";
        public const string HORA_OP_SWF_SA_FIN = "HORA_OP_SWF_SA_FIN";
        public const string HORA_OP_SWF_DO_INI = "HORA_OP_SWF_DO_INI";
        public const string HORA_OP_SWF_DO_FIN = "HORA_OP_SWF_DO_FIN";

        //Constantes de mensajes de Error...
        public const string Application = "Application";
        public const string MensajeErrorGeneral = "INFO|OnStart|Último fallback: no hacer nada.";
        public const string MensajeErrorConfiguracion = "INFO|OnStart|Leyendo archivo de configuración.";
        public const string MensajeTerminacion = "INFO|OnStop|Finalizando Servicio.";
        public const string MensajeTerminacionExitosa = "INFO|OnStop|Terminación exitosa de bases de datos.";
        public const string MensajeOperationCanceledException = "ERROR|TerminacionAsync|OperationCanceledException|";
        public const string MensajeException = "ERROR|TerminacionAsync";
        public const string MensajeOmitido = "INFO|OnStop|Terminación omitida por inicialización incompleta.";

        public const string MensajeTimer = "ERROR|Realiza_timer|Se presenta exception: ";
        public const string MensajeParametrizaciones = "ERROR|ParametrizacionesAsync";
        public const string MensajeError = "ERROR";
        public const string MensajeOnStart = "OnStart";
        public const string MensajeElapsed = "ERROR|Elapsed";
        public const string ErrorHora = "Ocurrio un error al convertir hora";
        public const string MensajeDiaInhabil = "Hoy es un día inhábil.";
        public const string MensajeEjecuta = "Ejecuta_Procedimientos";
        public const string MensajeFueraDeServicio = "El servicio está fuera de horario";
        public const string MensajeEjecutoCorrectamente = "El servicio SIWFT_IN_MQ_MX se ejecuto correctamente.";

        //Catálogo de mensajes MQ.
        public const string Codigo2033 = "MQRC_NO_MSG_AVAILABLE: No hay mensajes disponibles en la cola.";
        public const string Codigo2035 = "MQRC_NOT_AUTHORIZED: El usuario no tiene permisos para esta operación.";
        public const string Codigo2009 = "MQRC_CONNECTION_BROKEN: La conexión al Queue Manager se perdió.";
        public const string Codigo2059 = "MQRC_Q_MGR_NOT_AVAILABLE: El Queue Manager no está disponible.";
        public const string Codigo2085 = "MQRC_UNKNOWN_OBJECT_NAME: El objeto (cola/canal) no existe.";
        public const string Codigo2063 = "MQRC_SECURITY_ERROR: Error de seguridad en la conexión.";
        public const string Codigo2381 = "MQRC_KEY_REPOSITORY_ERROR: Problema con el repositorio de claves (SSL/TLS).";
        public const string Codigo2393 = "MQRC_SSL_INITIALIZATION_ERROR: Error al inicializar SSL.";
        public const string Codigo2400 = "MQRC_UNSUPPORTED_CIPHER_SUITE: CipherSuite no soportado.";
        public const string Codigo2538 = "MQRC_HOST_NOT_AVAILABLE: Host remoto no accesible.";
        public const string Codigo2540 = "MQRC_CHANNEL_NOT_AVAILABLE: Canal no disponible.";
        public const string CodigoDefault = "Error MQ desconocido. Código:";

        public const string CERTICATELABEL = "CertificateLabel";
        public const string LEERMENSAJES = "INFO|Lee_mensajes_MQ_MX|Datos para establecer la conexion.";
        public const string MensajeConexionExitosa = "Conexión exitosa a IBM MQ";
        public const string MensajeConexiónFallida = "Conexión fallida (IsConnected=false)";
        public const string MensajeFormato = "INFO|Lee_mensajes_MQ_MX|Formato del mensaje:";
        public const string MensajeGET = "INFO|Lee_mensajes_MQ| GET: *";
        public const string MensajeNohayICN = "INFO|Lee_mensajes_MQ_MX|No hay ICN de regreso (mensaje o dato inválido en la cola).";
        public const string MensajeRecuperado = "INFO|Lee_mensajes_MQ_MX|ICN recuperado correctamente, ICN: ";
        public const string MensajeERRORMQException = "ERROR|Lee_mensajes_MQ_MX|MQ Exception:";
        public const string MensajeMQRC_NO_MSG_AVAILABLE = "Terminación normal|MQRC_NO_MSG_AVAILABLE";
        public const string MensajeMQQueue = "INFO|Lee_mensajes_MQ_MX|MQQueue::termino con * ";

        public const string MensajeERRORSWIFT_IN = "ERROR|Lee_mensajes_MQ_MX|Error en MQ SWIFT_IN";
        public const string MensajeLectura = "ERROR durante la lectura del MQ del Swift_IN para el mensaje= ";
        public const string MensajeSPName = "EXEC usp_SWF_INSERT_MQ_MSG_MX MQ 'SWIFT_IN', '";
        public const string MensajeErrorColaMQ = "Error al cerrar cola MQ";
        public const string MensajeErrorDescMQ = "Error al deesconectar MQ";
        public const string MensajeErrorIcn = "Error: no se recuperó valor Icn.";
        public const string MensajeInsertaMensajeMQ = "INFO|InsertaMensajeMQ|Se insertó correctamente el mensaje , con el ICN: ";
        public const string MensajeLecturaMensajesMQ = "INFO|ObtieneLecturaMensajesMQ|Clave de parametro de configuracion Faltante.|No se recupera de BD.";
        public const string MensajeErrorFechaRecuperada = "La fecha Inicio y/o Fin no fue recuperada correctamente.";
        public const string MensajeErrorEnvioCorreo = "Ocurrio un error en el envio de correo.";
    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
﻿using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace SwfINMQMX
{
    internal static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                NombreServicio.Swf_IN_MQ_MX.ToString(),
                () => new ServiceHost(),
                args
#if DEBUG
                , (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs)
#endif
                );
        }
    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using AplicacionSwfINMQMX.Enums;
using System;

public class MQErrorHandler
{
    /// <summary>
    /// Catálogo de mensajes detallados, relacionados con los codigos de error del servidor MQ.
    /// </summary>
    /// <param name="reasonCode"></param>
    /// <returns></returns>
    public static string GetMQErrorDescription(int reasonCode)
    {
        switch (reasonCode)
        {
            case 2033:
                return ConstantesServicio.Codigo2033;

            case 2035:
                return ConstantesServicio.Codigo2035;

            case 2009:
                return ConstantesServicio.Codigo2009;

            case 2059:
                return ConstantesServicio.Codigo2059;

            case 2085:
                return ConstantesServicio.Codigo2085;

            case 2063:
                return ConstantesServicio.Codigo2063;

            case 2381:
                return ConstantesServicio.Codigo2381;

            case 2393:
                return ConstantesServicio.Codigo2393;

            case 2400:
                return ConstantesServicio.Codigo2400;

            case 2538:
                return ConstantesServicio.Codigo2538;

            case 2540:
                return ConstantesServicio.Codigo2540;

            default:
                return $"{ConstantesServicio.CodigoDefault} {reasonCode}";
        }
    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using AplicacionSwfINMQMX.Enums;
using AplicacionSwfINMQMX.Interfaces;
using AplicacionSwfINMQMX.Modelos;
using IBM.WMQ;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;

namespace InfraestructuraSwfINMQMX.Repositorio
{
    /// <summary>
    /// Repositorio InterfacesT24 (TOMI) y VistasT24
    /// </summary>
    public class RepositorioTomi : RepositorioMaestro, IRepositorioTomi
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public RepositorioTomi(Configuracion configuracion) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _configuracion = configuracion;
        }


        /// <summary>
        /// Recupera lista de mensajes MQ
        /// </summary>
        /// <returns></returns>
        public async Task<bool> ObtieneLecturaMensajesMQMX()
        {
            MQQueue mqQueue = null;
            MQQueueManager queueManager = null;
            MQGetMessageOptions mqGetMessageOptions;
            ConfiguracionMQMX configMQ = new ConfiguracionMQMX();
            bool isContinue = true;
            string mensaje = string.Empty;
            bool respuesta = false;

            // Configuración MQ
            configMQ.QueueManagerName = ObtenerParametroMX(ConstantesServicio.MANAGER_NAME_MX);
            configMQ.ChannelName = ObtenerParametroMX(ConstantesServicio.CHANNEL_MX);
            configMQ.QueueName = ObtenerParametroMX(ConstantesServicio.QUEUE_NAME_MX);
            configMQ.Host = ObtenerParametroMX(ConstantesServicio.HOST_MX);
            configMQ.Port = ObtenerEnteroObligatorio(ConstantesServicio.PORT_MX);

            // Formato requerido por MQ
            configMQ.User = ObtenerParametroMX(ConstantesServicio.USER_ID_MX);
            configMQ.Password = ObtenerParametroMX(ConstantesServicio.PASSWORD_MX);
            configMQ.Ssl = ObtenerParametroMX(ConstantesServicio.SSL_CIPHERSPEC_MX);
            configMQ.SslPath = ObtenerParametroMX(ConstantesServicio.SSL_KEYREPOSITORY_MX);
            configMQ.Cert = ObtenerParametroMX(ConstantesServicio.CERTIFICATE_LABEL_MX);
            configMQ.MailTo = ObtenerParametroMX(ConstantesServicio.CORREO_PARA_DEFAULT);

            try
            {
                MQEnvironment.UserId = configMQ.User;
                MQEnvironment.Password = configMQ.Password;
                MQEnvironment.SSLCipherSpec = configMQ.Ssl;
                MQEnvironment.SSLKeyRepository = configMQ.SslPath;


                Hashtable connectionProperties = new Hashtable();
                connectionProperties.Add(MQC.HOST_NAME_PROPERTY, configMQ.Host);
                connectionProperties.Add(MQC.PORT_PROPERTY, configMQ.Port);
                connectionProperties.Add(MQC.CHANNEL_PROPERTY, configMQ.ChannelName);
                connectionProperties.Add(ConstantesServicio.CERTICATELABEL, configMQ.Cert);


                Config.EscribirLog(ConstantesServicio.LEERMENSAJES);

                queueManager = new MQQueueManager(configMQ.QueueManagerName, connectionProperties);

                mqQueue = queueManager.AccessQueue(configMQ.QueueName, MQC.MQOO_OUTPUT + MQC.MQOO_INPUT_SHARED + MQC.MQOO_INQUIRE);

                if (queueManager.IsConnected)
                {
                    Config.EscribirLog(ConstantesServicio.MensajeConexionExitosa);
                }
                else
                {
                    Config.EscribirLog(ConstantesServicio.MensajeConexiónFallida);
                }

                // Leer todos los mensajes disponibles en este ciclo
                while (isContinue)
                {
                    MQMessage message = new MQMessage();
                    mqGetMessageOptions = new MQGetMessageOptions();
                    mqGetMessageOptions.WaitInterval = ConstantesServicio.WaitInterval;
                    mqGetMessageOptions.Options += MQC.MQGMO_WAIT;
                    try
                    {
                        mqQueue.Get(message, mqGetMessageOptions);
                        Config.EscribirLog(ConstantesServicio.MensajeFormato + message.Format);

                        mensaje = message.ReadString(message.MessageLength);
                        Config.EscribirLog(ConstantesServicio.MensajeGET + mensaje + "*");

                        var IdICN = await InsertaMensajeMQMX(mensaje);
                        if (string.IsNullOrEmpty(IdICN))
                        {
                            Config.EscribirLog(ConstantesServicio.MensajeNohayICN);
                            respuesta = false;
                        }
                        else
                        {
                            Config.EscribirLog(ConstantesServicio.MensajeRecuperado + IdICN);
                            respuesta = true;
                        }

                    }
                    catch (MQException ex)
                    {
                        string errorMsg = MQErrorHandler.GetMQErrorDescription(ex.ReasonCode);
                        Config.EscribirLog(ConstantesServicio.MensajeERRORMQException + errorMsg);
                        if (ex.ReasonCode == MQC.MQRC_NO_MSG_AVAILABLE)
                        {
                            Config.EscribirLog(ConstantesServicio.MensajeMQRC_NO_MSG_AVAILABLE);
                            isContinue = false;
                        }
                        else
                        {
                            Config.EscribirLog(ConstantesServicio.MensajeMQQueue + ex.Message.ToString() + "*");
                            if (ex.Reason == MQC.MQRC_TRUNCATED_MSG_FAILED)
                            {
                                isContinue = false;
                            }
                        }
                        break;
                    }
                }

            }
            catch (MQException ex)
            {
                string errorMsg = MQErrorHandler.GetMQErrorDescription(ex.ReasonCode);
                Config.EscribirLog(ConstantesServicio.MensajeERRORMQException + errorMsg);
            }
            catch (Exception exc)
            {

                Config.EscribirLog(ConstantesServicio.MensajeERRORSWIFT_IN + ConstantesServicio.MensajeLectura + mensaje + "Error= " + exc.Message);
                Config.EscribirLog(ConstantesServicio.MensajeSPName + mensaje + "'");

                var destinatario = ObtenerValorConfig(_configuracion.CorreoParaDefault, configMQ.MailTo);
                var asunto = string.IsNullOrEmpty(_configuracion.SujetoDefault)
               ? ConstantesServicio.Asunto
               : $"{ConstantesServicio.Asunto}[{_configuracion.SujetoDefault}]";

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo()
                {
                    CorreoOrigen = _configuracion.CorreoSenderTest,
                    NombreOrigen = ConstantesServicio.NombreOrigen,
                    Para = destinatario,
                    ConCopia = string.Empty,
                    CopiaOculta = null,
                    Asunto = asunto,
                    Mensaje = ConstantesServicio.Mensaje + mensaje + " Error= " + exc.Message.ToString()
                });

                if(!resultado)
                {
                    Config.EscribirLog(ConstantesServicio.MensajeErrorEnvioCorreo);
                }
            }
            finally
            {
                try
                {
                    mqQueue?.Close();
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"{ConstantesServicio.MensajeErrorColaMQ}: {ex.Message}");
                }

                try
                {
                    queueManager?.Disconnect();
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"{ConstantesServicio.MensajeErrorDescMQ}: {ex.Message}");
                }
            }
            return respuesta;
        }

        /// <summary>
        /// Obtiene y valida un parametro de configuración obligatorio de tipo entero.
        /// </summary>
        /// <param name="clave"></param>
        /// <returns></returns>
        /// <exception cref="ConfigurationErrorsException"></exception>
        private int ObtenerEnteroObligatorio(string clave)
        {
            string valor = ObtenerParametroMX(clave);
            if (!int.TryParse(valor, out int resultado))
            {
                throw new ConfigurationErrorsException($"La configuración [{ConstantesServicio.PORT_MX}] es obligatoria y debe de ser númerica.");
            }
            return resultado;
        }
        public async Task<string> InsertaMensajeMQMX(string mensaje)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<IcnMQMX>(ConstantesServicio.uspInsertaMensajeMQMX, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", ConstantesServicio.TipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@mq_msg", mensaje));
                });
                var icn = resultado?.Data?.FirstOrDefault()?.Icn;
                if (string.IsNullOrWhiteSpace(icn.ToString()))
                {
                    var mensajeError = $"Error: no se recuperó valor Icn.|{NombreServicio.Swf_IN_MQ_MX.ToString()}";
                    Config.EscribirLog(mensajeError);
                    throw new InvalidOperationException(mensajeError);
                }
                else
                {
                    Config.EscribirLog($"{ConstantesServicio.MensajeInsertaMensajeMQ}" + icn.ToString());
                }

                return icn?.ToString() ?? string.Empty;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"{ConstantesClaveAviso.Error}|Ocurrió un error al Insertar Mensaje MQ: {ex}");
                throw;
            }
        }

        /// <summary>
        /// Metodo para obtener valor de parametro
        /// </summary>
        /// <param name="valorConfig"></param>
        /// <param name="valorDefault"></param>
        /// <returns></returns>
        string ObtenerValorConfig(string valorConfig, string valorDefault)
        {
            return string.IsNullOrEmpty(valorConfig) ? valorDefault : valorConfig;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private string ObtenerParametroMX(string key)
        {
            if (_configuracion.ParametrosSistema.TryGetValue(key, out var value))
                return value;

            Config.EscribirLog($"{ConstantesServicio.MensajeLecturaMensajesMQ}: {key}");
            throw new ConfigurationErrorsException($"{ConstantesServicio.MensajeLecturaMensajesMQ}: {key}");
        }
    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.ServiceModel;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace MUFG_T24.Inicializacion_utilidades
{
    /// <summary>
    /// Representa Parametrizacion.
    /// </summary>
    public class Parametrizacion : RepositorioMaestro, IParametrizacion
    {
        /// <summary>
        /// Inicializa una nueva instancia de Parametrizacion.
        /// </summary>
        public Parametrizacion()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerHoraSistema.
        /// </summary>
        public TimeSpan ObtenerHoraSistema()
        {
            return DateTime.Now.TimeOfDay;
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerProcesoActualAsync.
        /// </summary>
        public Task<Configuracion> ObtenerProcesoActualAsync(Configuracion configuracion)
        {
            return ObtenerProcesoActualAsync(configuracion, CancellationToken.None);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerProcesoActualAsync.
        /// </summary>
        public async Task<Configuracion> ObtenerProcesoActualAsync(Configuracion configuracion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            if (configuracion == null)
                throw new ArgumentNullException(nameof(configuracion));

            using (Process currentProcess = Process.GetCurrentProcess())
            {
                configuracion.Pid = currentProcess.Id;
                configuracion.NombreProceso = currentProcess.ProcessName;
            }

            if (!configuracion.Pid.HasValue)
                throw new InvalidOperationException("No fue posible obtener el identificador del proceso actual.");

            int pid = configuracion.Pid.Value;
#if DEBUG
            configuracion.NombreDisplay = await ObtenerProcesoActualCadena();
#else
            configuracion.NombreDisplay = await ObtieneDisplayName(pid);
#endif
            ct.ThrowIfCancellationRequested();


            //Inicializa el archivo log de salida...
            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy("INFO|OnStart|Inicializando Servicio.");
            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|OnStart|Proceso iniciado con las caracteristicas: PID={configuracion.Pid}, ProcessName='{configuracion.NombreProceso}', servicio='{configuracion.NombreServicio}', displayName='{configuracion.NombreDisplay}' buscando puerto donde escuchar.");
            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|OnStart|Versiones: Servicio={System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString()}, BTMUM.dll={Comunes.Comunes.ObtenerVersion()}, Framework={Environment.Version.ToString()}.");

            return configuracion;
        }

        /// <summary>
        /// Obtiene el proceso actual y valida la configuración crítica resultante.
        /// </summary>
        public async Task<Configuracion> ObtenerProcesoActualValidadoAsync(Configuracion configuracion, CancellationToken ct)
        {
            var configuracionActual = await ObtenerProcesoActualAsync(configuracion, ct);
            ValidarProcesoActual(configuracionActual);
            return configuracionActual;
        }

        /// <summary>
        /// Valida los datos críticos del proceso actual en la configuración del servicio.
        /// </summary>
        public void ValidarProcesoActual(Configuracion configuracion)
        {
            if (configuracion == null)
                throw new InvalidOperationException("No se obtuvo la configuración base del proceso actual.");

            if (!configuracion.Pid.HasValue)
                throw new InvalidOperationException("No se obtuvo el PID del proceso actual.");

            if (string.IsNullOrWhiteSpace(configuracion.NombreProceso))
                throw new InvalidOperationException("No se obtuvo el nombre del proceso actual.");
        }
        /// <summary>
        /// Inicializa una nueva instancia de ObtenerProcesoActualCadena.
        /// </summary>
        public Task<string> ObtenerProcesoActualCadena()
        {
            try
            {
                return Task.FromResult("PRUEBAS");
            }
            catch (Exception)
            {
                return Task.FromResult("PRUEBAS");
            }
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerFechaSistemaAsync.
        /// </summary>
        public Task<string> ObtenerFechaSistemaAsync()
        {
            return ObtenerFechaSistemaAsync(CancellationToken.None);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerFechaSistemaAsync.
        /// </summary>
        public async Task<string> ObtenerFechaSistemaAsync(CancellationToken ct)
        {
            var resultado = await EjecutarStoredProcedureQuery<FechaSistemaDto>(ConstantesProcedimientosMUFG.UspObtenerFechaSistema, null, ct);
            FechaSistemaDto fechaSistema = resultado.Data.Count > 0 ? resultado.Data[0] : null;
            if (fechaSistema == null)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|obtenerParametrosSistema|No se obtuvo la fecha del sistema desde '{ConstantesProcedimientosMUFG.UspObtenerFechaSistema}'.");
                throw new InvalidOperationException("No se obtuvo la fecha del sistema requerida para inicializar el servicio.");
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|obtenerParametrosSistema|Fecha del sistema '{fechaSistema.Fecha.ToString("yyyyMMdd")}'.");
            return fechaSistema.Fecha.ToString("yyyyMMdd");
        }

        /// <summary>
        /// Obtiene la fecha del sistema y valida el valor resultante.
        /// </summary>
        public async Task<string> ObtenerFechaSistemaValidadaAsync(CancellationToken ct)
        {
            var fechaSistema = await ObtenerFechaSistemaAsync(ct).ConfigureAwait(false);
            ValidarFechaSistema(fechaSistema);
            return fechaSistema;
        }

        /// <summary>
        /// Valida la fecha del sistema obtenida para el flujo del servicio.
        /// </summary>
        public void ValidarFechaSistema(string fechaSistema)
        {
            if (fechaSistema == null)
                throw new InvalidOperationException("No se obtuvo la FechaSistema.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerFechaSistemaJulianaAsync.
        /// </summary>
        public async Task<string> ObtenerFechaSistemaJulianaAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<FechaSistemaDto>(ConstantesProcedimientosMUFG.UspObtenerFechaSistemaJuliana);
            FechaSistemaDto fechaSistema = resultado.Data.Count > 0 ? resultado.Data[0] : null;
            if (fechaSistema == null)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|FechaSistemaJuliana|No se obtuvo la fecha juliana del sistema desde '{ConstantesProcedimientosMUFG.UspObtenerFechaSistemaJuliana}'.");
                throw new InvalidOperationException("No se obtuvo la fecha juliana del sistema requerida para inicializar el servicio.");
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|FechaSistemaJuliana='{fechaSistema.Fecha.ToString("yyyyMMdd")}'.");
            return fechaSistema.Fecha.ToString("yyyyMMdd");
        }

        /// <summary>
        /// /Obtiene los parámetros del sistema.
        /// </summary>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametros)
        {
            return ObtenerParametrosSistemaAsync(strParametros, CancellationToken.None);
        }

        /// <summary>
        /// Obtiene los parámetros del sistema.
        /// </summary>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(ParametroSistemaDefinicion[] parametros)
        {
            return ObtenerParametrosSistemaAsync(parametros, CancellationToken.None);
        }

        /// <summary>
        /// Obtiene los parámetros del sistema combinando parámetros por defecto con parámetros específicos.
        /// </summary>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, ParametroSistemaDefinicion[] parametrosEspecificos)
        {
            return ObtenerParametrosSistemaAsync(strParametrosDefault, parametrosEspecificos, CancellationToken.None);
        }

        /// <summary>
        /// Obtiene los parámetros del sistema combinando parámetros por defecto con parámetros específicos.
        /// </summary>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, ParametroSistemaDefinicion[] parametrosEspecificos, CancellationToken ct)
        {
            var porDefecto = ConvertirCadenaADefinicionesParametros(strParametrosDefault, requerido: true, sensible: false);
            var especificos = NormalizarDefinicionesParametros(parametrosEspecificos);
            var definiciones = CombinarDefinicionesParametros(porDefecto, especificos);
            return ObtenerParametrosSistemaAsync(definiciones, ct);
        }

        /// <summary>
        /// Obtiene los parámetros del sistema combinando parámetros por defecto con parámetros obligatorios y sensibles.
        /// </summary>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, string strParametrosObligatoriosSensibles)
        {
            return ObtenerParametrosSistemaAsync(strParametrosDefault, strParametrosObligatoriosSensibles, CancellationToken.None);
        }

        /// <summary>
        /// Obtiene los parámetros del sistema combinando parámetros por defecto con parámetros obligatorios y sensibles.
        /// </summary>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, string strParametrosObligatoriosSensibles, CancellationToken ct)
        {
            var porDefecto = ConvertirCadenaADefinicionesParametros(strParametrosDefault, requerido: true, sensible: false);
            var obligatoriosSensibles = ConvertirCadenaADefinicionesParametros(strParametrosObligatoriosSensibles, requerido: true, sensible: true);
            var definiciones = CombinarDefinicionesParametros(porDefecto, obligatoriosSensibles);
            return ObtenerParametrosSistemaAsync(definiciones, ct);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerParametrosSistemaAsync.
        /// </summary>
        public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametros, CancellationToken ct)
        {
            return ObtenerParametrosSistemaAsync(ConvertirCadenaADefinicionesParametros(strParametros), ct);
        }

        /// <summary>
        /// Inicializa una nueva instancia de ObtenerParametrosSistemaAsync.
        /// </summary>
        public async Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(ParametroSistemaDefinicion[] parametros, CancellationToken ct)
        {
            ParametroSistemaDefinicion[] definiciones = NormalizarDefinicionesParametros(parametros);
            string strParametros = CrearCadenaParametrosSistema(definiciones);

            var resultado = await EjecutarStoredProcedureQuery<ParametroDTO>(ConstantesProcedimientosMUFG.UspObtenerParametrosSistema, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strParametros", strParametros));
            }, ct);
            Dictionary<string, string> parametrosSistema = new Dictionary<string, string>();
            foreach (var parametro in resultado.Data)
            {
                ct.ThrowIfCancellationRequested();
                if (parametrosSistema.ContainsKey(parametro.Id_parametro))
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|obtenerParametrosSistema|El parámetro '{parametro.Id_parametro}' está duplicado en la base de datos.");
                    continue;
                }

                parametrosSistema.Add(parametro.Id_parametro, parametro.Valor);
            }

            foreach (ParametroSistemaDefinicion definicion in definiciones)
            {
                ct.ThrowIfCancellationRequested();
                if (!parametrosSistema.TryGetValue(definicion.Clave, out var valor) ||
                    string.IsNullOrWhiteSpace(valor))
                {
                    string nivel = definicion.Requerido ? ConstantesClaveAviso.Error : "INFO";
                    string obligatoriedad = definicion.Requerido ? "requerido" : "opcional";
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{nivel}|obtenerParametrosSistema|El parámetro {obligatoriedad} '{definicion.Clave}' no está definido o está vacío.");

                    if (definicion.Requerido)
                        throw new InvalidOperationException($"El parámetro requerido '{definicion.Clave}' no está definido o está vacío.");
                }
                else
                {
                    string mensajeParametro = $"INFO|obtenerParametrosSistema|Parámetro '{definicion.Clave}' = '{valor}'.";

                    if (definicion.Sensible)
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacyMasked(mensajeParametro, valor);
                    else
                        MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy(mensajeParametro);
                }
            }

            return parametrosSistema;


        }

        private static ParametroSistemaDefinicion[] ConvertirCadenaADefinicionesParametros(string strParametros)
        {
            return ConvertirCadenaADefinicionesParametros(strParametros, requerido: true, sensible: false);
        }

        private static ParametroSistemaDefinicion[] ConvertirCadenaADefinicionesParametros(string strParametros, bool requerido, bool sensible)
        {
            if (string.IsNullOrWhiteSpace(strParametros))
                return new ParametroSistemaDefinicion[0];

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

        private static ParametroSistemaDefinicion[] CombinarDefinicionesParametros(ParametroSistemaDefinicion[] parametrosDefault, ParametroSistemaDefinicion[] parametrosEspecificos)
        {
            var combinados = new Dictionary<string, ParametroSistemaDefinicion>(StringComparer.OrdinalIgnoreCase);

            foreach (var parametro in NormalizarDefinicionesParametros(parametrosDefault))
                combinados[parametro.Clave] = parametro;

            foreach (var parametro in NormalizarDefinicionesParametros(parametrosEspecificos))
                combinados[parametro.Clave] = parametro;

            return combinados.Values.ToArray();
        }

        private static ParametroSistemaDefinicion[] NormalizarDefinicionesParametros(ParametroSistemaDefinicion[] parametros)
        {
            if (parametros == null)
                return new ParametroSistemaDefinicion[0];

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

        private static string CrearCadenaParametrosSistema(ParametroSistemaDefinicion[] parametros)
        {
            return string.Join(",", parametros.Select(parametro => $"'{parametro.Clave.Replace("'", "''")}'"));
        }

        /// <summary>
        /// Obtiene parámetros adicionales del sistema.
        /// </summary>
        public async Task<Dictionary<string, string>> ObtenerParametrosSistemaAdicionalesAsync(Dictionary<string, string> parametrosSistema, string strParametros)
        {
            var resultado = await EjecutarStoredProcedureQuery<ParametroDTO>(ConstantesProcedimientosMUFG.UspObtenerParametrosSistema, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strParametros", strParametros));
            });
            foreach (var parametro in resultado.Data)
            {
                if (parametrosSistema.ContainsKey(parametro.Id_parametro))
                {
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|obtenerParametrosSistema|El parámetro '{parametro.Id_parametro}' está duplicado en la base de datos.");
                    continue;
                }
                parametrosSistema.Add(parametro.Id_parametro, parametro.Valor);
            }
            return parametrosSistema;
        }

        /// <summary>
        /// Obtiene parámetros adicionales del sistema.
        /// </summary>
        public async Task<string> ObtenerPuertos(string nombreServicio, int pid)
        {
            var resultado = await EjecutarStoredProcedureQuery<ObtenerPuertosDto>(ConstantesProcedimientosMUFG.UspObtenerPuerto, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombreServicio", nombreServicio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pid", pid));
            });
            ObtenerPuertosDto puertoServicio = resultado.Data.Count > 0 ? resultado.Data[0] : null;
            if (puertoServicio == null)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ObtenerPuertos|No se obtuvo puerto para el servicio '{nombreServicio}' y PID '{pid}'.");
                throw new InvalidOperationException($"No se obtuvo el puerto requerido para inicializar el servicio '{nombreServicio}'.");
            }

            await ValidarPuertoDisponibleAsync(puertoServicio.Puerto, nombreServicio, pid);

            return puertoServicio.Puerto.ToString();
        }

        private async Task ValidarPuertoDisponibleAsync(int puerto, string nombreServicio, int pid)
        {
            if (puerto < IPEndPoint.MinPort || puerto > IPEndPoint.MaxPort)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ObtenerPuertos|El puerto '{puerto}' configurado para el servicio '{nombreServicio}' y PID '{pid}' no es válido.");
                throw new InvalidOperationException($"El puerto configurado para el servicio '{nombreServicio}' no es válido.");
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|ObtenerPuertos|Validando disponibilidad del puerto {puerto} para el servicio '{nombreServicio}'.");

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
                    MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|ObtenerPuertos|Validando puerto intento {intento}, excepción: {ex}");
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
                    await Task.Delay(500);
                }
            }

            if (!puertoLibre)
            {
                MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"{ConstantesClaveAviso.Error}|ObtenerPuertos|El puerto {puerto} del servicio '{nombreServicio}' está en uso por otro proceso.");
                throw new InvalidOperationException($"El puerto {puerto} configurado para el servicio '{nombreServicio}' está en uso por otro proceso.");
            }

            MUFG_T24.Infraestructura.Logging.Logging.WriteLegacy($"INFO|ObtenerPuertos|Puerto disponible {puerto} para el servicio '{nombreServicio}'.");
        }

        /// <summary>
        /// Inicializa una nueva instancia de DetenerServicio.
        /// </summary>
        public void DetenerServicio(ServiceBase serviceHost)
        {
            serviceHost.ExitCode = 1;
            serviceHost.Stop();
        }

    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
﻿using System.Threading.Tasks;

namespace AplicacionSwfINMQMX.Interfaces
{
    /// <summary>
    /// Interfaz para implementar métodos de consulta a la BD InterfacesT24
    /// </summary>
    public interface IRepositorioTomi
    {
        /// <summary>
        /// Recupera mensajes de MQMX.
        /// </summary>
        /// <returns></returns>      
        Task<bool> ObtieneLecturaMensajesMQMX();

        /// <summary>
        /// Método para insertar mensajes en la tabla de trabajo MQ MX.
        /// </summary>
        /// <param name="mensaje"></param>
        /// <returns></returns>
        Task<string> InsertaMensajeMQMX(string mensaje);
    }

}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using AplicacionSwfINMQMX;
using AplicacionSwfINMQMX.Interfaces;
using InfraestructuraSwfINMQMX.Repositorio;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

[TestClass]
public class SwfINMQMXTest
{
    private Mock<IControlServicio> _controlServicio;
    private Mock<IAlertas> _alertas;
    private Mock<IParametrizacion> _parametrizacion;
    private Mock<IRepositorioTomi> _repositorioTomi;
    private OperacionServicio _servicio = null;

    [TestInitialize]
    private void Initialize()
    {
        _controlServicio = new Mock<IControlServicio>();
        _alertas = new Mock<IAlertas>();
        _parametrizacion = new Mock<IParametrizacion>();
        _repositorioTomi = new Mock<IRepositorioTomi>();
    }


    [TestInitialize]
    public void Setup()
    {
        var config = new Configuracion
        {
            NombreServicio = "TestServicio",
            NombreProceso = "TestProceso",
            NombreDisplay = "TestDisplay",
            Pid = 123,
            EsDiaHabil = true,
            ParametrosSistema = new Dictionary<string, string>
                {
                    { "HORA_OP_SWF_JU_INI", "06:05:00" },
                    { "HORA_OP_SWF_JU_FIN", "20:00:00" }
                }
        };
    }

    /// <summary>
    /// Test de método principal
    /// </summary>
    /// <param name="ct"></param>
    /// <returns></returns>
    [TestMethod]
    public async Task EjecutarProcedimientosAsync_ReturnsTrueTest(CancellationToken ct)
    {
        var result = await _servicio.EjecutarProcedimientosAsync(ct);

        Assert.IsTrue(result);

    }

    /// <summary>
    /// Test cuando no es día hábil
    /// </summary>
    /// <param name="ct"></param>
    /// <returns></returns>
    [TestMethod]
    public async Task EjecutarProcedimientosAsync_ReturnsFalse_WhenNoDiaHabil(CancellationToken ct)
    {
        var config = new Configuracion
        {
            NombreServicio = "Test",
            Pid = 1,
            EsDiaHabil = false,
            ParametrosSistema = new Dictionary<string, string>
                {
                    { "HORA_OP_SWF_JU_INI", "06:05:00" },
                    { "HORA_OP_SWF_JU_FIN", "23:59:59" }
                }
        };

        var resultado = await _servicio.EjecutarProcedimientosAsync(CancellationToken.None);

        Assert.IsTrue(resultado);
    }

    [TestMethod]
    public async Task EjecutarProcedimientosAsync_ValidacionProcesos()
    {

        var configuracion = new Configuracion
        {
            NombreServicio = "Test",
            Pid = 1,
            EsDiaHabil = false,
            ParametrosSistema = new Dictionary<string, string>
                {
                    { "HORA_OP_SWF_JU_INI", "06:05:00" },
                    { "HORA_OP_SWF_JU_FIN", "23:59:59" }
                }
        };

        _controlServicio.Setup(x => x.EsDiaHabil()).ReturnsAsync(true);

        _parametrizacion.Setup(x => x.ObtenerFechaSistemaAsync(It.IsAny<CancellationToken>())).ReturnsAsync("20260824");

        _parametrizacion.Setup(x => x.ObtenerParametrosSistemaAsync(It.IsAny<string>(), It.IsAny<string>())).ReturnsAsync(configuracion.ParametrosSistema);

        var resultado = await _servicio.EjecutarProcedimientosAsync(CancellationToken.None);

        Assert.IsFalse(resultado);
    }
}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    AplicacionSwfINMQMX.csproj
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    <?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{15A2B62B-C8CA-4C53-BF74-BE4944109002}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>AplicacionSwfINMQMX</RootNamespace>
    <AssemblyName>AplicacionSwfINMQMX</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <Deterministic>true</Deterministic>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <OutputPath>bin\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Core" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="Enums\ConstantesServicio.cs" />
    <Compile Include="Interfaces\IOperacionServicio.cs" />
    <Compile Include="Interfaces\IRepositorioTomi.cs" />
    <Compile Include="Modelos\ConfiguracionMQMX.cs" />
    <Compile Include="Modelos\IcnMQMX.cs" />
    <Compile Include="OperacionServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <ItemGroup />
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    InfraestructuraSwfINMQMX.csproj
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    <?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{32F50EB9-969A-4CB3-8FAC-24E55F165769}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>InfraestructuraSwfINMQMX</RootNamespace>
    <AssemblyName>InfraestructuraSwfINMQMX</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <Deterministic>true</Deterministic>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <OutputPath>bin\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="amqmdnet, Version=9.4.0.20, Culture=neutral, PublicKeyToken=dd3cb1c9aae9ec97, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\amqmdnet.dll</HintPath>
    </Reference>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="Messaging\MQErrorHandler.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
    <Compile Include="Repositorio\RepositorioTomi.cs" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\AplicacionSwfINMQMX\AplicacionSwfINMQMX.csproj">
      <Project>{15a2b62b-c8ca-4c53-bf74-be4944109002}</Project>
      <Name>AplicacionSwfINMQMX</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PruebasUnitarias.csproj 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" DefaultTargets="Build" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="..\packages\MSTest.TestAdapter.2.2.10\build\net46\MSTest.TestAdapter.props" Condition="Exists('..\packages\MSTest.TestAdapter.2.2.10\build\net46\MSTest.TestAdapter.props')" />
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{FC3B5402-8F76-4CBA-AF59-8C0D3BCC3B0E}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>PruebasUnitarias</RootNamespace>
    <AssemblyName>PruebasUnitarias</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <ProjectTypeGuids>{3AC096D0-A1C2-E12C-1390-A8335801FDAB};{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}</ProjectTypeGuids>
    <VisualStudioVersion Condition="'$(VisualStudioVersion)' == ''">15.0</VisualStudioVersion>
    <VSToolsPath Condition="'$(VSToolsPath)' == ''">$(MSBuildExtensionsPath32)\Microsoft\VisualStudio\v$(VisualStudioVersion)</VSToolsPath>
    <ReferencePath>$(ProgramFiles)\Common Files\microsoft shared\VSTT\$(VisualStudioVersion)\UITestExtensionPackages</ReferencePath>
    <IsCodedUITest>False</IsCodedUITest>
    <TestProjectType>UnitTest</TestProjectType>
    <NuGetPackageImportStamp>
    </NuGetPackageImportStamp>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <OutputPath>bin\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="Castle.Core, Version=5.0.0.0, Culture=neutral, PublicKeyToken=407dd0808d44fbdc, processorArchitecture=MSIL">
      <HintPath>..\packages\Castle.Core.5.1.1\lib\net462\Castle.Core.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.VisualStudio.TestPlatform.TestFramework, Version=14.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\MSTest.TestFramework.2.2.10\lib\net45\Microsoft.VisualStudio.TestPlatform.TestFramework.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.VisualStudio.TestPlatform.TestFramework.Extensions, Version=14.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\MSTest.TestFramework.2.2.10\lib\net45\Microsoft.VisualStudio.TestPlatform.TestFramework.Extensions.dll</HintPath>
    </Reference>
    <Reference Include="Moq, Version=4.20.72.0, Culture=neutral, PublicKeyToken=69f491c39445e920, processorArchitecture=MSIL">
      <HintPath>..\packages\Moq.4.20.72\lib\net462\Moq.dll</HintPath>
    </Reference>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.Runtime.CompilerServices.Unsafe, Version=4.0.4.1, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Runtime.CompilerServices.Unsafe.4.5.3\lib\net461\System.Runtime.CompilerServices.Unsafe.dll</HintPath>
    </Reference>
    <Reference Include="System.Threading.Tasks.Extensions, Version=4.2.0.1, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Threading.Tasks.Extensions.4.5.4\lib\net461\System.Threading.Tasks.Extensions.dll</HintPath>
    </Reference>
  </ItemGroup>
  <ItemGroup>
    <Compile Include="SwfINMQMXTest.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <ItemGroup>
    <None Include="packages.config" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\AplicacionSwfINMQMX\AplicacionSwfINMQMX.csproj">
      <Project>{15a2b62b-c8ca-4c53-bf74-be4944109002}</Project>
      <Name>AplicacionSwfINMQMX</Name>
    </ProjectReference>
    <ProjectReference Include="..\InfraestructuraSwfINMQMX\InfraestructuraSwfINMQMX.csproj">
      <Project>{32f50eb9-969a-4cb3-8fac-24e55f165769}</Project>
      <Name>InfraestructuraSwfINMQMX</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(VSToolsPath)\TeamTest\Microsoft.TestTools.targets" Condition="Exists('$(VSToolsPath)\TeamTest\Microsoft.TestTools.targets')" />
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
  <Target Name="EnsureNuGetPackageBuildImports" BeforeTargets="PrepareForBuild">
    <PropertyGroup>
      <ErrorText>This project references NuGet package(s) that are missing on this computer. Use NuGet Package Restore to download them.  For more information, see http://go.microsoft.com/fwlink/?LinkID=322105. The missing file is {0}.</ErrorText>
    </PropertyGroup>
    <Error Condition="!Exists('..\packages\MSTest.TestAdapter.2.2.10\build\net46\MSTest.TestAdapter.props')" Text="$([System.String]::Format('$(ErrorText)', '..\packages\MSTest.TestAdapter.2.2.10\build\net46\MSTest.TestAdapter.props'))" />
    <Error Condition="!Exists('..\packages\MSTest.TestAdapter.2.2.10\build\net46\MSTest.TestAdapter.targets')" Text="$([System.String]::Format('$(ErrorText)', '..\packages\MSTest.TestAdapter.2.2.10\build\net46\MSTest.TestAdapter.targets'))" />
  </Target>
  <Import Project="..\packages\MSTest.TestAdapter.2.2.10\build\net46\MSTest.TestAdapter.targets" Condition="Exists('..\packages\MSTest.TestAdapter.2.2.10\build\net46\MSTest.TestAdapter.targets')" />
</Project>
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    packages.config
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------    
    ﻿<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="Microsoft.Bcl.AsyncInterfaces" version="1.0.0" targetFramework="net48" />
  <package id="SimpleInjector" version="5.0.0" targetFramework="net48" />
  <package id="System.Runtime.CompilerServices.Unsafe" version="4.5.2" targetFramework="net48" />
  <package id="System.Threading.Tasks.Extensions" version="4.5.2" targetFramework="net48" />
</packages>

