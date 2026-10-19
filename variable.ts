using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// clase principal del servicio.
    /// </summary>
    public class OperacionServicio : Aplicacion.Interfaces.IOperacionServicio
    {
        // Sistema Interfaces.
        private readonly ConfiguracionServicio _configuracion;
       

        // Entradas Interfaces.
        private readonly IParametrosOperaciones _IparametrosOperaciones;
        private readonly IParametrizacion _iParametrizacion;
        private readonly IOperacionesEntradaTomi _IoperacionesEntradaTomi;
        private readonly IOperacionesEntradaSpei _IoperacionesEntradaSpei;
        private readonly IOperacionesEntradaSpei2 _IoperacionesEntradaSpei2;
        private readonly IOperacionesEntradaSpid _IoperacionesEntradaSpid;
        private readonly IOperacionesEntradaTef _IoperacionesEntradaTef;
        private readonly IOperacionesEntradaCms _IoperacionesEntradaCms;

        private readonly int CleanTokenTime = 10; //segundos

        /// <summary>
        /// Controlador principal.
        /// </summary>
        /// <param name="configuracion"></param>
        /// <param name="iParametrizacion"></param>
        /// <param name="iparametrosOperaciones"></param>
        /// <param name="ioperacionesEntradaTomi"></param>
        /// <param name="ioperacionesEntradaSpei"></param>
        /// <param name="ioperacionesEntradaSpei2"></param>
        /// <param name="ioperacionesEntradaSpid"></param>
        /// <param name="ioperacionesEntradaTef"></param>
        /// <param name="ioperacionesEntradaCms"></param>
        public OperacionServicio(ConfiguracionServicio configuracion, IParametrizacion iParametrizacion, IParametrosOperaciones iparametrosOperaciones, IOperacionesEntradaTomi ioperacionesEntradaTomi, IOperacionesEntradaSpei ioperacionesEntradaSpei, IOperacionesEntradaSpei2 ioperacionesEntradaSpei2, IOperacionesEntradaSpid ioperacionesEntradaSpid, IOperacionesEntradaTef ioperacionesEntradaTef, IOperacionesEntradaCms ioperacionesEntradaCms)
        {
            _configuracion = configuracion;
            _IparametrosOperaciones = iparametrosOperaciones;
            _IoperacionesEntradaTomi = ioperacionesEntradaTomi;
            _IoperacionesEntradaSpei = ioperacionesEntradaSpei;
            _IoperacionesEntradaSpei2 = ioperacionesEntradaSpei2;
            _IoperacionesEntradaSpid = ioperacionesEntradaSpid;
            _IoperacionesEntradaTef = ioperacionesEntradaTef;
            _IoperacionesEntradaCms = ioperacionesEntradaCms;
            _iParametrizacion = iParametrizacion;
        }



        /// <summary>
        /// Procedimiento principal para el procesamiento de las operaciones.
        /// </summary>
        public async Task EjecutaProcedimientosAsync(CancellationToken ct)
        {
            try
            {
                // Actualiza ejecucion
                await _IparametrosOperaciones.MarcarServicioEnEjecucionAsync(_configuracion.NombreServicio, ct);
                ct.ThrowIfCancellationRequested();

                // Obtiene los parametros del sistema del Servicio Entradas.
                var dicParametrosSistema = await _IparametrosOperaciones.ParametrosSistemaAsync(ct);
                ct.ThrowIfCancellationRequested();

                // Obtiene la fecha de la base de datos y validacion.
                _configuracion.fechaServicio = await _IparametrosOperaciones.ObtencionFechaSistemaAsync(ct);
                if (!DateTime.TryParseExact(_configuracion.fechaServicio, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out _)) throw new InvalidOperationException("Fecha de sistema inválida.");
                


                ct.ThrowIfCancellationRequested();
                Config.EscribirLog($"INFO|Fecha: {_configuracion.fechaServicio} para ejecucion de procesos SPID, SPEI, TEF");

                //Valida los horarios de operación...
                TimeSpan timeHoraSistema =_iParametrizacion.ObtenerHoraSistema();
                bool boHoraOperacionInicio = false;

                //Pregunta a la base de datos si el dia es habil segun el registro en base de datos.

                _configuracion.EsDiaHabil = await _IparametrosOperaciones.BEsDiaHabil(ct);
                ct.ThrowIfCancellationRequested();

                //Validacion de campos obligatorios.
                string[] faltantes = Enum.GetNames(typeof(ParametrosSistema)).Where(k => !dicParametrosSistema.ContainsKey(k)).ToArray();
                if (faltantes.Length > 0)
                {
                    throw new InvalidOperationException(
                        "Parametros de sistema requeridos ausentes: " + string.Join(", ", faltantes));
                }

                //Carga la configuración de los parámetros para el inicio de operaciones de la base de datos.

                foreach (var paramSistema in dicParametrosSistema)
                {
                    if(!Enum.TryParse(paramSistema.Key, out ParametrosSistema parametro)) continue; // Cambia el tipo string a un enum
                    switch (parametro)
                    {
                        case ParametrosSistema.HORA_LECTURA_INICIO:
                            _configuracion.StrHoraLecturaInicio = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_LECTURA_INICIO.ToString());
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de inicio de lectura '" + paramSistema.Value + "'.", NombreServicio.Entradas.ToString());
                            break;

                        case ParametrosSistema.HORA_LECTURA_FIN:
                            _configuracion.StrHoraLecturaFin = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_LECTURA_FIN.ToString());
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de finalización de lectura '" + paramSistema.Value + "'.", NombreServicio.Entradas.ToString());
                            break;

                        case ParametrosSistema.HORA_OP_INICIO:
                            _configuracion.StrHoraOperacionesInicio = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_OP_INICIO.ToString());
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de inicio de operaciones '" + paramSistema.Value + "'.", NombreServicio.Entradas.ToString());
                            break;

                        case ParametrosSistema.HORA_OP_FIN:
                            _configuracion.StrHoraOperacionesFin = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_OP_FIN.ToString());
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de finalización de operaciones '" + paramSistema.Value + "'.", NombreServicio.Entradas.ToString());
                            break;

                        case ParametrosSistema.SPEI_SERVIDOR_1:
                            _configuracion.StrNombreSPEI1 = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Servidor 1 de SPEI activo '" + paramSistema.Value + "'.", NombreServicio.Entradas.ToString());
                            break;

                        case ParametrosSistema.SPEI_SERVIDOR_2:
                            _configuracion.StrNombreSPEI2 = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Servidor 2 de SPEI activo '" + paramSistema.Value + "'.", NombreServicio.Entradas.ToString());
                            break;
                    }
                }

                ///Validamos el horario de las operaciones, indicara si la bandera es falso o verdadero. 
                boHoraOperacionInicio = (timeHoraSistema >= _configuracion.StrHoraOperacionesInicio && timeHoraSistema <= _configuracion.StrHoraOperacionesFin);
                Config.EscribirLog($"INFO|Ejecuta_Procedimientos|{(boHoraOperacionInicio ? "inicio" : "Termina")}horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.Entradas.ToString());

            
                _configuracion.StrFechaSistema = timeHoraSistema;


                if (_configuracion.StrFechaSistema >= _configuracion.StrHoraLecturaInicio && _configuracion.StrFechaSistema <= _configuracion.StrHoraLecturaFin) // Inicio Horario
                {
                    if (!_configuracion.BHorarioLectura)
                    {
                        _configuracion.BHorarioLectura = true;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Inicio horario de lectura, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.Entradas.ToString());
                    }
                }
                else // Termina Horario
                {
                    if (_configuracion.BHorarioLectura) 
                    {   
                        _configuracion.BHorarioLectura = false;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Fin horario de lectura, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.Entradas.ToString());
                    }
                }

                if (!_configuracion.BHorarioLectura) // Fuera de Horario
                {
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos| El Servicio esta fuera de Horario.", NombreServicio.Entradas.ToString());
 
                    await _IparametrosOperaciones.EnviaAlerta(new AlertaDTO
                    {
                        TipoOPeracion = null,
                        Folio = null,
                        Servicio = _configuracion.NombreServicio,
                        ClaveAviso = "CRITICO",
                        Descipcion = "Error en ejecuta_procedimientos",
                        PId = _configuracion.Pid ?? 0,
                        NombreProceso = _configuracion.NombreProceso,
                        Exception = new Exception("El servicio '" + _configuracion.NombreServicio + "' está fuera de horario.")
                    });

                    return;
                }

                ///Inicia los servicios asincronos segun el horario de operacion de cada uno.

            
                ct.ThrowIfCancellationRequested();
                await _IoperacionesEntradaCms.LeeOperacionesCMS(ct); // Lee todas las operaciones del CMS y crea su plan de trabajo (valida que no falten operaciones)

                if ((bool)_configuracion.EsDiaHabil)  // Valido que sea día hábil (bool)_configuracion.EsDiaHabil
                {

                    if (_configuracion.StrFechaSistema >= _configuracion.StrHoraOperacionesInicio && _configuracion.StrFechaSistema <= _configuracion.StrHoraOperacionesFin) 
                    {
                        _configuracion.BenHorario = true;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Inicio horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.Entradas.ToString());

                        // Lectura de operaciones dentro de horario de operaciones

                        var tareasHilo = new List<Task>
                        {
                            _IoperacionesEntradaTomi.LeeOperacionesLocales(ct), // Lee operaciones locales
                            _IoperacionesEntradaTomi.LeeOperacionesCOAS(ct), // Lee las operaciones del COAS
                            _IoperacionesEntradaSpid.LeeOperacionesSPID(ct), // Lee las operaciones del SPID
                            _IoperacionesEntradaTomi.LeeOperacionesConcentradoras(ct), // Lee las operaciones de entrada de las Concentradoras
                            _IoperacionesEntradaTomi.LeeOperacionesFOREX(ct), // Operaciones de confirmación de FOREX
                            _IoperacionesEntradaTef.LeeOperacionesTEF(ct), // Lee operaciones del sistema TEF
                            _IoperacionesEntradaTomi.LeeOperacionesGPH(ct), // Lee operaciones de archivos GPH
                            _IoperacionesEntradaTomi.LeeOperacionesMD(ct) // Lee operaciones de MD (Calypso)  
                        };

                        if (_configuracion.StrNombreSPEI1 == "1")
                        {
                            tareasHilo.Add(_IoperacionesEntradaSpei.LeeOperacionesSPEI1(ct)); //  Lee las operaciones del SPEI 1
                        }

                        if (_configuracion.StrNombreSPEI2 == "1")
                        {
                            tareasHilo.Add(_IoperacionesEntradaSpei2.LeeOperacionesSPEI2(ct)); //  Lee las operaciones del SPEI 2
                        }

                        await Task.WhenAll(tareasHilo);
                    }
                }
                else
                {
                    if ((bool)_configuracion.BenHorario) //Fuera de horario
                    {
                        _configuracion.BenHorario = false;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Termina horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.Entradas.ToString());
                    }
                }
            }
            catch (Exception ex) 
            {
                Config.EscribirLog("ERROR|Ejecuta_Procedimientos|Se presenta exception: " + ex.ToString());

                await _IparametrosOperaciones.EnviaAlerta(new AlertaDTO
                {
                    TipoOPeracion = null,
                    Folio = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = "CRITICO",
                    Descipcion = "Error en ejecuta_procedimientos",
                    PId = _configuracion.Pid ?? 0,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = ex
                });
                Config.EscribirLog("ERROR|Ejecuta_Procedimientos|Se forza la finalización del servicio.", NombreServicio.Entradas.ToString());
            }
            finally
            {
                try
                {
                    using (var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(CleanTokenTime)))
                    {
                        await _IparametrosOperaciones.MarcarServicioEnFinalizacion(
                            _configuracion.NombreServicio,
                            cleanup.Token);
                    }
                }
                catch(Exception exFinally)
                {
                    Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|no fue posible marcar el servicio en finalización: {exFinally}", NombreServicio.Entradas.ToString());
                }
            }
        }
    }
}
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 using MUFG_T24.Configuracion_utilidades;
using System;
namespace Aplicacion.Modelos
{
    /// <summary>
    /// Propiedades generales del servicio.
    /// </summary>   
    public class ConfiguracionServicio : Configuracion
    {
        public TimeSpan StrFechaSistema { get; set; }
        public TimeSpan StrHoraLecturaInicio { get; set; }
        public TimeSpan StrHoraLecturaFin { get; set; }
        public TimeSpan StrHoraOperacionesInicio { get; set; }
        public TimeSpan StrHoraOperacionesFin { get; set; }
        public bool BHorarioLectura { get; set; } = false;
        public string StrNombreSPEI1 { get; set; } = null;
        public string StrNombreSPEI2 { get; set; } = null;
        public bool? BenHorario { get; set; } = false; 

        public string fechaServicio { get; set;} = null;
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using Aplicacion;
using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase de parametros del sistema.
    /// </summary>
    public class ConsultaCamposBDTomi : RepositorioMaestro, IParametrosOperaciones
    {
        private readonly IAlertas _alerta;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public ConsultaCamposBDTomi(IAlertas alertas) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) 
        { 
            _alerta = alertas; 
        }

        /// <summary>
        /// Obtencion de fecha desde la base de datos.
        /// </summary>
        /// <returns>Date</returns>
        public async Task<string> ObtencionFechaSistemaAsync(CancellationToken ct)
        {
            var resultado = await EjecutarStoredProcedureQuery<SFecha>(ConstantesServicio.sp_Fecha_Sistema, null, ct);
            return resultado?.Data?
                .FirstOrDefault()?
                .fecha
                .ToString("yyyyMMdd") 
                ?? string.Empty;
        }

        /// <summary>
        /// Obtiene los parametros del sistema.
        /// </summary>
        /// <returns>Dictionary<string, string></returns>
        public async Task<Dictionary<string, string>> ParametrosSistemaAsync(CancellationToken ct)
        {
            var result = await EjecutarStoredProcedureQuery<ParametroModelo>(ConstantesServicio.sp_Parametros_Sistema, null, ct);

            if(result?.Data == null) return new Dictionary<string, string>();

            return result.Data
                .Where(x => x != null && !string.IsNullOrWhiteSpace(x.id_parametro))
                .GroupBy(x => x.id_parametro)
                .ToDictionary(
                    x => x.Key,
                    x => x.LastOrDefault()?.valor ?? string.Empty
                );
            
        }

        // <summary>
        /// Actualiza el estado del servicio en la base de datos, lo marca como en uso.
        /// </summary>
        /// <returns></returns>
        public async Task MarcarServicioEnEjecucionAsync(string strNombreServicio, CancellationToken ct) => await ActualizaEJecucion(strNombreServicio, ct);

        // <summary>
        /// Envia alertas al sistema.
        /// </summary>
        /// <returns></returns>
        public Task<bool> EnviaAlerta(AlertaDTO alertaDTO) => _alerta.InsertarAlerta(alertaDTO);   

        // <summary>
        /// Actualiza el estado del servicio en la base de datos, lo marca como en uso.
        /// </summary>
        /// <returns></returns>
        public async Task MarcarServicioEnFinalizacion(string strNombreServicio, CancellationToken ct) => await ActualizaTerminacion(strNombreServicio, ct);

        /// <summary>
        /// Pregunta si es dia habil para poder ejecutar el servicio.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> BEsDiaHabil(CancellationToken ct) => await EsDiaHabil(ct);
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Servicios;
using Infraestructura.Repositorio;
using MUFG_T24.Comunes;
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Entradas
{

    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private ConfiguracionServicio _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.Entradas.ToString();
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

            _configuracionBase = new ConfiguracionServicio();

            //Inicializa la obtención de la configuración general y el logging...
            {
                MUFG_T24.Core.Configuracion.LecturaConfguracionGeneral.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = MUFG_T24.Core.Configuracion.LecturaConfguracionGeneral.ObtenerServicio(NombreServicio.Entradas.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.RutaArchivoConfiguracion = $@"{EnumRutasGenerales.Path_Log_CMS}{NombreServicio.Entradas.ToString()}\";
                _configuracionBase.PathLog = servicioMainConfiguracion.Log + servicioMainConfiguracion.NombreServicio;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.configuracion = servicioMainConfiguracion.configuracion;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.OperacionesPermitidas =
                    (servicioMainConfiguracion.OperacionesPermitidas ?? "")
                    .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Trim())
                    .ToArray();

                RutasConfiguracion rutas = new RutasConfiguracion()
                {
                    MUFG_Servicios_Path_Log = _configuracionBase.PathLog
                };
                Logging.Init(rutas, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
                Logging.Write("INFO|OnStart|Leyendo archivo de configuración.");
            }

            //Registra la configuracion...
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);


            // ENTRADAS

            _containerDI.Register<ConfiguracionServicio>(() => (ConfiguracionServicio)_configuracionBase.Clone(), Lifestyle.Scoped);

            _containerDI.Register<IOperacionesGenerales, OperacionesGenerales>(Lifestyle.Scoped);
            _containerDI.Register<IParametrosOperaciones, ConsultaCamposBDTomi>(Lifestyle.Scoped);
            _containerDI.Register<IOperacionesEntradaTomi, OperacionRepositorioTomi>(Lifestyle.Scoped);
            _containerDI.Register<IOperacionesEntradaCms, OperacionRepositorioCms>(Lifestyle.Scoped);
            _containerDI.Register<IOperacionesEntradaSpei, OperacionRepositorioSpei>(Lifestyle.Scoped);
            _containerDI.Register<IOperacionesEntradaSpei2, OperacionRepositorioSpei2>(Lifestyle.Scoped);
            _containerDI.Register<IOperacionesEntradaSpid, OperacionRepositorioSpid>(Lifestyle.Scoped);
            _containerDI.Register<IOperacionesEntradaTef, OperacionRepositorioTef>(Lifestyle.Scoped);
            _containerDI.Register<Aplicacion.Interfaces.IOperacionServicio, OperacionServicio>(Lifestyle.Scoped);

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


                Configuracion configMufgT24 = new Configuracion();
                Configuracion nuevaConfig = await _parametrizacion.ObtenerProcesoActualAsync(configMufgT24);

                if (nuevaConfig == null)
                {
                    throw new InvalidOperationException("No fue posible obtener la configuración del proceso.");
                }

                if (!nuevaConfig.Pid.HasValue)
                {
                    throw new InvalidOperationException("El parámetro Pid no fue configurado para el servicio.");
                }

                if (string.IsNullOrWhiteSpace(nuevaConfig.NombreProceso))
                {
                    throw new InvalidOperationException("El parámetro NombreProceso no fue configurado para el servicio");
                }

                if (string.IsNullOrWhiteSpace(nuevaConfig.NombreDisplay))
                {
                    throw new InvalidOperationException("El parámetro NombreDisplay no fue configurado para el servicio.");
                }
                _configuracionBase.Pid = nuevaConfig.Pid;
                _configuracionBase.NombreProceso = nuevaConfig.NombreProceso;
                _configuracionBase.NombreDisplay = nuevaConfig.NombreDisplay;

                //Marca el servicio en uso...
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
                await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio, ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ParametrizacionesAsync|{ex}");
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
                    var operacionServicio = _containerDI.GetInstance<Aplicacion.Interfaces.IOperacionServicio>();
                    await operacionServicio.EjecutaProcedimientosAsync(ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Realiza_timer|Se presenta exception: {ex}");
            }
        }

        /// <summary>
        /// Realiza ciertas operaciones antes de finalizar el servicio.
        /// </summary>
        protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            Config.EscribirLog("INFO|OnStop|Finalizando Servicio.");

            try
            {
                if (_containerDI == null || _configuracionBase == null)
                {
                    Config.EscribirLog("INFO|OnStop|Terminación omitida por inicialización incompleta.");
                    return;
                }

                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();

                    await _controlServicio.EsperarTareaActivaAsync(startupTask, ConstantesSistema.ParametrizacionInicial, waitCt).ConfigureAwait(false);
                    await _controlServicio.EsperarTareaActivaAsync(runningTask, ConstantesSistema.ProcesamientoActivo, waitCt).ConfigureAwait(false);

                    //Marca el servicio en uso...
                    await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);

                    //Marca el termino del servicio...
                    await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);

                    Config.EscribirLog("INFO|OnStop|Terminación exitosa de bases de datos.");
                }
            }
            catch (OperationCanceledException)
            {
                //Apagado normal
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|TerminacionAsync|{ex}");
            }
            finally
            {
                _containerDI?.Dispose();
            }
        }

    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
﻿using MUFG_T24.Modelos;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Parametros operaciones.
    /// </summary>
    public interface IParametrosOperaciones
    {

        Task<string> ObtencionFechaSistemaAsync(CancellationToken ct);
        Task<Dictionary<string, string>> ParametrosSistemaAsync(CancellationToken ct);
        Task<bool> BEsDiaHabil(CancellationToken ct);
        Task MarcarServicioEnEjecucionAsync(string strNombreServicio, CancellationToken ct);
        Task MarcarServicioEnFinalizacion(string strNombreServicio, CancellationToken ct);
        Task<bool> EnviaAlerta(AlertaDTO alerta);
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
﻿using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Tomi.
    /// </summary>
    public interface IOperacionesEntradaTomi
    {
        Task<int> LeeOperacionesLocales(CancellationToken ct);

        Task<int> LeeOperacionesCOAS(CancellationToken ct);

        Task<int> LeeOperacionesConcentradoras(CancellationToken ct);

        Task<int> LeeOperacionesFOREX(CancellationToken ct);

        Task<int> LeeOperacionesGPH(CancellationToken ct);

        Task<int> LeeOperacionesMD(CancellationToken ct);
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using Aplicacion.Modelos;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Generales.
    /// </summary>
    public interface IOperacionesGenerales
    {
        Task<int> RegistraFolios(Dictionary<string, FolioOperacionDTO> dicFolios, string strOrigen, CancellationToken ct);
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Servicios;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.ServiceProcess;

namespace PruebasUnitarias
{
    /// <summary>
    /// Clase de pruebas unitarias.
    /// </summary>
    [TestClass]
    public class EntradasUT
    {

        private ParametrosOperacionesFake _parametrosOperaciones;
        private ParametrizacionFake _parametrizacion;
        private OperacionesEntradaTomiFake _repositorioTomi;
        private OperacionesEntradaCmsFake _repositorioCms;
        private OperacionesEntradaSpeiFake _repositorioSpei;
        private OperacionesEntradaSpei2Fake _repositorioSpei2;
        private OperacionesEntradaSpidFake _repositorioSpid;
        private OperacionesEntradaTefFake _repositorioTef;

        private ConfiguracionServicio _configuracion;
        private OperacionServicio _servicio;



        private readonly CancellationToken ct = new CancellationToken();


        [TestInitialize]
        public void Setup()
        {
            var rutasLogs = Path.Combine(Path.GetTempPath(), "EntradasPruebasUnitarias");
            Directory.CreateDirectory(rutasLogs);
            Config.Rutas = new RutasConfiguracion
            {
                MUFG_Servicios_Path_Log = rutasLogs + Path.DirectorySeparatorChar
            };

            
            _parametrosOperaciones = new ParametrosOperacionesFake();
            _parametrizacion = new ParametrizacionFake();

            _repositorioTomi = new OperacionesEntradaTomiFake();
            _repositorioCms = new OperacionesEntradaCmsFake();
            _repositorioSpei = new OperacionesEntradaSpeiFake();
            _repositorioSpei2 = new OperacionesEntradaSpei2Fake();
            _repositorioSpid = new OperacionesEntradaSpidFake();
            _repositorioTef = new OperacionesEntradaTefFake();

            _configuracion = new ConfiguracionServicio
            {
                NombreServicio = "EntradasPruebasUnitarias",
                EsDiaHabil = true,
                BenHorario = false,
                BHorarioLectura = true,
                StrHoraOperacionesInicio = TimeSpan.Parse("00:00"),
                StrHoraOperacionesFin = TimeSpan.Parse("23:59"),
                StrHoraLecturaInicio = TimeSpan.Parse("00:00"),
                StrHoraLecturaFin = TimeSpan.Parse("23:59")
            };

            _servicio = new OperacionServicio(
                _configuracion,
                _parametrizacion,
                _parametrosOperaciones,
                _repositorioTomi,
                _repositorioSpei,
                _repositorioSpei2,
                _repositorioSpid,
                _repositorioTef,
                _repositorioCms);
        }
        [TestMethod]
        public async Task EjecutaProcedimientos_CuandoTodoEsCorrecto_EjecutaTodosLosProcesos()
        {
            await _servicio.EjecutaProcedimientosAsync(ct);

            Assert.AreEqual(1, _parametrosOperaciones.MarcarServicioEnEjecucionCount);
            Assert.AreEqual(1, _parametrosOperaciones.MarcarServicioEnFinalizacionCount);

            Assert.AreEqual(1, _repositorioCms.LeeOperacionesCMSCount);

            Assert.AreEqual(1, _repositorioTomi.LeeOperacionesLocalesCount);
            Assert.AreEqual(1, _repositorioTomi.LeeOperacionesCOASCount);
            Assert.AreEqual(1, _repositorioTomi.LeeOperacionesConcentradorasCount);
            Assert.AreEqual(1, _repositorioTomi.LeeOperacionesFOREXCount);
            Assert.AreEqual(1, _repositorioTomi.LeeOperacionesGPHCount);
            Assert.AreEqual(1, _repositorioTomi.LeeOperacionesMDCount);

            Assert.AreEqual(1, _repositorioSpid.LeeOperacionesSPIDCount);
            Assert.AreEqual(1, _repositorioTef.LeeOperacionesTEFCount);

            Assert.AreEqual(1, _repositorioSpei.LeeOperacionesSPEI1Count);
            Assert.AreEqual(1, _repositorioSpei2.LeeOperacionesSPEI2Count);

            Assert.AreEqual(0, _parametrosOperaciones.EnviaAlertaCount);

            Assert.IsTrue(_configuracion.BHorarioLectura);
            Assert.IsTrue(_configuracion.BenHorario);
        }

        [TestMethod]
        public async Task EjecutaProcedimientos_CuandoNoEsDiaHabil_NoEjecutaProcesos()
        {
            _parametrosOperaciones.EsDiaHabilResultado = false;

            await _servicio.EjecutaProcedimientosAsync(ct);

            Assert.AreEqual(1, _repositorioCms.LeeOperacionesCMSCount);

            Assert.AreEqual(0, _repositorioTomi.LeeOperacionesLocalesCount);
            Assert.AreEqual(0, _repositorioSpid.LeeOperacionesSPIDCount);
            Assert.AreEqual(0, _repositorioTef.LeeOperacionesTEFCount);

            Assert.AreEqual(1, _parametrosOperaciones.MarcarServicioEnFinalizacionCount);
        }

        [TestMethod]
        public async Task EjecutaProcedimientos_FueraDeHorario_EnviaAlerta()
        {
            _parametrizacion.HoraSistema = TimeSpan.Parse("23:59");

            _parametrosOperaciones.ParametrosSistemaResultado =
                new Dictionary<string, string>
                {
                    ["HORA_LECTURA_INICIO"] = "08:00",
                    ["HORA_LECTURA_FIN"] = "18:00",
                    ["HORA_OP_INICIO"] = "08:00",
                    ["HORA_OP_FIN"] = "18:00",
                    ["SPEI_SERVIDOR_1"] = "1",
                    ["SPEI_SERVIDOR_2"] = "1"
                };

            await _servicio.EjecutaProcedimientosAsync(ct);

            Assert.AreEqual(1, _parametrosOperaciones.EnviaAlertaCount);

            Assert.AreEqual(0, _repositorioCms.LeeOperacionesCMSCount);

            Assert.AreEqual(1, _parametrosOperaciones.MarcarServicioEnFinalizacionCount);
        }

        #region Fakes

        private sealed class ParametrizacionFake : IParametrizacion
        {
            public TimeSpan HoraSistema { get; set; }
                = TimeSpan.Parse("12:00");

            public TimeSpan ObtenerHoraSistema()
            {
                return HoraSistema;
            }


            void IParametrizacion.DetenerServicio(ServiceBase serviceHost)
            {
            }

            public Task<string> ObtenerFechaSistemaAsync()
            {
                throw new NotImplementedException();
            }

            public Task<string> ObtenerFechaSistemaAsync(CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<string> ObtenerFechaSistemaJulianaAsync()
            {
                throw new NotImplementedException();
            }

            public Task<string> ObtenerFechaSistemaJulianaAsync(CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<string> ObtenerFechaSistemaValidadaAsync(CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAdicionalesAsync(Dictionary<string, string> parametrosSistema, string strParametros)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAdicionalesAsync(string strParametrosDefault, string strParametrosObligatoriosSensibles, CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(ParametroSistemaDefinicion[] parametros)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(ParametroSistemaDefinicion[] parametros, CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametros)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametros, CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, ParametroSistemaDefinicion[] parametrosEspecificos)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, ParametroSistemaDefinicion[] parametrosEspecificos, CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, string strParametrosObligatoriosSensibles)
            {
                throw new NotImplementedException();
            }

            public Task<Dictionary<string, string>> ObtenerParametrosSistemaAsync(string strParametrosDefault, string strParametrosObligatoriosSensibles, CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<Configuracion> ObtenerProcesoActualAsync(Configuracion configuracion)
            {
                throw new NotImplementedException();
            }

            public Task<Configuracion> ObtenerProcesoActualAsync(Configuracion configuracion, CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public Task<Configuracion> ObtenerProcesoActualValidadoAsync(Configuracion configuracion, CancellationToken ct)
            {
                throw new NotImplementedException();
            }

            public void ValidarProcesoActual(Configuracion configuracion)
            {
                throw new NotImplementedException();
            }

            public void ValidarFechaSistema(string fechaSistema)
            {
                throw new NotImplementedException();
            }

            public Task<string> ObtenerPuertos(string nombreServicio, int pid)
            {
                throw new NotImplementedException();
            }

            public Task<string> ObtenerPuertos(string nombreServicio, int pid, CancellationToken ct)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class ParametrosOperacionesFake : IParametrosOperaciones
        {
            public int MarcarServicioEnEjecucionCount { get; private set; }

            public int MarcarServicioEnFinalizacionCount { get; private set; }

            public int EnviaAlertaCount { get; private set; }

            public bool EsDiaHabilResultado { get; set; } = true;

            public string FechaSistemaResultado { get; set; }
            = "20261013";

            public Dictionary<string, string> ParametrosSistemaResultado { get; set; }
                = new Dictionary<string, string>
                {
                    ["HORA_LECTURA_INICIO"] = "00:00",
                    ["HORA_LECTURA_FIN"] = "23:59",
                    ["HORA_OP_INICIO"] = "00:00",
                    ["HORA_OP_FIN"] = "23:59",
                    ["SPEI_SERVIDOR_1"] = "1",
                    ["SPEI_SERVIDOR_2"] = "1"
                };

            public AlertaDTO UltimaAlerta { get; private set; }

            public Task<string> ObtencionFechaSistemaAsync(CancellationToken ct)
            {
                return Task.FromResult(FechaSistemaResultado);
            }

            public Task<Dictionary<string, string>> ParametrosSistemaAsync(CancellationToken ct)
            {
                return Task.FromResult(ParametrosSistemaResultado);
            }

            public Task<bool> BEsDiaHabil(CancellationToken ct)
            {
                return Task.FromResult(EsDiaHabilResultado);
            }

            public Task MarcarServicioEnEjecucionAsync(
                string strNombreServicio,
                CancellationToken ct)
            {
                MarcarServicioEnEjecucionCount++;
                return Task.CompletedTask;
            }

            public Task MarcarServicioEnFinalizacion(
                string strNombreServicio,
                CancellationToken ct)
            {
                MarcarServicioEnFinalizacionCount++;
                return Task.CompletedTask;
            }

            public Task<bool> EnviaAlerta(AlertaDTO alerta)
            {
                EnviaAlertaCount++;
                UltimaAlerta = alerta;
                return Task.FromResult(true);
            }
        }

        private sealed class OperacionesEntradaCmsFake : IOperacionesEntradaCms
        {
            public int LeeOperacionesCMSCount { get; private set; }

            public Task<int> LeeOperacionesCMS(CancellationToken ct)
            {
                LeeOperacionesCMSCount++;
                return Task.FromResult(1);
            }
        }

        private sealed class OperacionesEntradaSpeiFake : IOperacionesEntradaSpei
        {
            public int LeeOperacionesSPEI1Count { get; private set; }

            public Task<int> LeeOperacionesSPEI1(CancellationToken ct)
            {
                LeeOperacionesSPEI1Count++;
                return Task.FromResult(1);
            }
        }

        private sealed class OperacionesEntradaSpei2Fake : IOperacionesEntradaSpei2
        {
            public int LeeOperacionesSPEI2Count { get; private set; }

            public Task<int> LeeOperacionesSPEI2(CancellationToken ct)
            {
                LeeOperacionesSPEI2Count++;
                return Task.FromResult(1);
            }
        }

        private sealed class OperacionesEntradaSpidFake : IOperacionesEntradaSpid
        {
            public int LeeOperacionesSPIDCount { get; private set; }

            public Task<int> LeeOperacionesSPID(CancellationToken ct)
            {
                LeeOperacionesSPIDCount++;
                return Task.FromResult(1);
            }
        }

        private sealed class OperacionesEntradaTefFake : IOperacionesEntradaTef
        {
            public int LeeOperacionesTEFCount { get; private set; }

            public Task<int> LeeOperacionesTEF(CancellationToken ct)
            {
                LeeOperacionesTEFCount++;
                return Task.FromResult(1);
            }
        }

        private sealed class OperacionesEntradaTomiFake : IOperacionesEntradaTomi
        {
            public int LeeOperacionesLocalesCount { get; private set; }

            public int LeeOperacionesCOASCount { get; private set; }

            public int LeeOperacionesConcentradorasCount { get; private set; }

            public int LeeOperacionesFOREXCount { get; private set; }

            public int LeeOperacionesGPHCount { get; private set; }

            public int LeeOperacionesMDCount { get; private set; }

            public Task<int> LeeOperacionesLocales(CancellationToken ct)
            {
                LeeOperacionesLocalesCount++;
                return Task.FromResult(1);
            }

            public Task<int> LeeOperacionesCOAS(CancellationToken ct)
            {
                LeeOperacionesCOASCount++;
                return Task.FromResult(1);
            }

            public Task<int> LeeOperacionesConcentradoras(CancellationToken ct)
            {
                LeeOperacionesConcentradorasCount++;
                return Task.FromResult(1);
            }

            public Task<int> LeeOperacionesFOREX(CancellationToken ct)
            {
                LeeOperacionesFOREXCount++;
                return Task.FromResult(1);
            }

            public Task<int> LeeOperacionesGPH(CancellationToken ct)
            {
                LeeOperacionesGPHCount++;
                return Task.FromResult(1);
            }

            public Task<int> LeeOperacionesMD(CancellationToken ct)
            {
                LeeOperacionesMDCount++;
                return Task.FromResult(1);
            }
        }

        #endregion

        

    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase de CMS.
    /// </summary>
    public class OperacionRepositorioCms : RepositorioMaestro, IOperacionesEntradaCms
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IOperacionesGenerales registraFolios;

        private readonly FiltroFolios filtroFolios = new FiltroFolios();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioCms(ConfiguracionServicio configuracion, IOperacionesGenerales operacionesGenerales) : base(BasedatosProveedor.SqlServer, NombreBasedatos.CMS, null, null)
        {
            _configuracion = configuracion;
            registraFolios = operacionesGenerales;
        }

        /// <summary>
        /// Busca operaciones del CMS (de todos los tipos) con varios sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesCMS(CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>();

                //Operaciones FXD, FXI, FXT e Impuestos
                if (_configuracion.OperacionesPermitidas.Contains("FXD") || _configuracion.OperacionesPermitidas.Contains("FXI") || _configuracion.OperacionesPermitidas.Contains("FXT") ||  _configuracion.OperacionesPermitidas.Contains("SAT") || _configuracion.OperacionesPermitidas.Contains("MFX") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionFXDFXIFXT = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_FXD_FXI_FXT_Impuestos, null, ct);
                    if (spEjecucionFXDFXIFXT != null)
                    {

                        foreach (var respSPQuery in spEjecucionFXDFXIFXT?.Data ?? new List<Formato>())
                        {
                            
                            long intTipo = respSPQuery.tipoFormato;
                            long intOpClave = respSPQuery.id_formato;

                            switch (intTipo) // 12 MFX
                            {
                                case 3:
                                    if (_configuracion.OperacionesPermitidas.Contains("FXD") || _configuracion.OperacionesPermitidas.Length == 0)
                                        filtroFolios.AgregarFolio(
                                        dicFolios,
                                        "FXD-" + intOpClave,
                                        new FolioOperacionDTO
                                        {
                                            FolioMaestro = intOpClave.ToString(),
                                            FolioDetalle = intOpClave.ToString(),
                                            EsAgrupado = false,
                                            TipoOperacion = "FXD",
                                            DescripcionOperacion = "Domésticas"
                                        }); // Prefijo R para evitar error por llave duplicada
                                    break;
                                case 4:
                                case 11:
                                case 14:
                                    if (_configuracion.OperacionesPermitidas.Contains("FXI") || _configuracion.OperacionesPermitidas.Length == 0)
                                        filtroFolios.AgregarFolio(
                                        dicFolios,
                                        "FXI-" + intOpClave,
                                        new FolioOperacionDTO
                                        {
                                            FolioMaestro = intOpClave.ToString(),
                                            FolioDetalle = intOpClave.ToString(),
                                            EsAgrupado = false,
                                            TipoOperacion = "FXI",
                                            DescripcionOperacion = "Internacionales"
                                        }); // Prefijo R para evitar error por llave duplicada
                                    break;
                                case 7:
                                    if (_configuracion.OperacionesPermitidas.Contains("FXT") || _configuracion.OperacionesPermitidas.Length == 0)
                                        filtroFolios.AgregarFolio(
                                        dicFolios,
                                        "FXT-" + intOpClave,
                                        new FolioOperacionDTO
                                        {
                                            FolioMaestro = intOpClave.ToString(),
                                            FolioDetalle = intOpClave.ToString(),
                                            EsAgrupado = false,
                                            TipoOperacion = "FXT",
                                            DescripcionOperacion = "Traspasos BTMUM"
                                        }); // Prefijo R para evitar error por llave duplicada
                                    break;
                                case 5:
                                case 6:
                                case 9:
                                case 10:
                                case 13:
                                    if (_configuracion.OperacionesPermitidas.Contains("SAT") || _configuracion.OperacionesPermitidas.Length == 0)
                                        filtroFolios.AgregarFolio(
                                        dicFolios,
                                        "SAT-" + intOpClave,
                                        new FolioOperacionDTO
                                        {
                                            FolioMaestro = intOpClave.ToString(),
                                            FolioDetalle = intOpClave.ToString(),
                                            EsAgrupado = false,
                                            TipoOperacion = "SAT",
                                            DescripcionOperacion = "SAT - " + (intTipo == 13 ? "DREF" : "NEPE")
                                        }); // Prefijo R para evitar error por llave duplicada
                                    break;
                                case 12:
                                    if (_configuracion.OperacionesPermitidas.Contains("MFX") || _configuracion.OperacionesPermitidas.Length == 0)
                                    {
                                        ct.ThrowIfCancellationRequested();
                                        await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Update_MFX, cmd => cmd.Parameters.Add(basedatosConexion.CrearParametro("@idFormato", intOpClave)), ct);
                                        Config.EscribirLog("INFO | lee_operaciones_CMS | A marcar la operacion MFX como en proceso, folio: " + intOpClave + ".", NombreServicio.Entradas.ToString());
                                    }
                                    break;
                            }
                        }

                    }
                }

                // Operaciones SER y TAX
                if (_configuracion.OperacionesPermitidas.Contains("SER") || _configuracion.OperacionesPermitidas.Contains("TAX") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionSERTAX = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_SER_TAX, null, ct);
                    if (spEjecucionSERTAX != null)
                    {
                        foreach (var respSPQuery in spEjecucionSERTAX?.Data ?? new List<Formato>())
                        {
                            long intTipo = respSPQuery.tipoFormato;
                            long intOpClave = respSPQuery.id_formato;

                            switch (intTipo) // 30-SER | 31-TAX
                            {
                                case 30:
                                    if (_configuracion.OperacionesPermitidas.Contains("SER") || _configuracion.OperacionesPermitidas.Length == 0)
                                        filtroFolios.AgregarFolio(
                                        dicFolios,
                                        "SER-" + intOpClave,
                                        new FolioOperacionDTO
                                        {
                                            FolioMaestro = intOpClave.ToString(),
                                            FolioDetalle = intOpClave.ToString(),
                                            EsAgrupado = false,
                                            TipoOperacion = "SER",
                                            DescripcionOperacion = "Pago de Servicios"
                                        });
                                    break;
                                case 31:
                                    if (_configuracion.OperacionesPermitidas.Contains("TAX") || _configuracion.OperacionesPermitidas.Length == 0)
                                        filtroFolios.AgregarFolio(
                                        dicFolios,
                                        "TAX-" + intOpClave,
                                        new FolioOperacionDTO
                                        {
                                            FolioMaestro = intOpClave.ToString(),
                                            FolioDetalle = intOpClave.ToString(),
                                            EsAgrupado = false,
                                            TipoOperacion = "TAX",
                                            DescripcionOperacion = "Pago de Impuestos Locales"
                                        });
                                    break;

                            }
                        }

                    }
                }

                // Operaciones SPID - SPID
                if (_configuracion.OperacionesPermitidas.Contains("SPID") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionSPID = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_SPID, null, ct);
                    if (spEjecucionSPID != null)
                    {
                        foreach (var respSPQuery in spEjecucionSPID?.Data ?? new List<Formato>())
                        {
                            long intTipo = respSPQuery.tipoFormato;
                            long intOpClave = respSPQuery.id_formato;

                            switch (intTipo) // 24-SPID
                            {
                                case 24:
                                    filtroFolios.AgregarFolio(
                                        dicFolios,
                                        "SPID-" + intOpClave,
                                        new FolioOperacionDTO
                                        {
                                            FolioMaestro = intOpClave.ToString(),
                                            FolioDetalle = intOpClave.ToString(),
                                            EsAgrupado = false,
                                            TipoOperacion = "SPID",
                                            DescripcionOperacion = "SPID"
                                        });
                                    break;

                            }
                        }

                    }
                }

                // Operaciones TEF Domesticas
                if (_configuracion.OperacionesPermitidas.Contains("TEF") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionTEF = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_TEF, null, ct);
                    if (spEjecucionTEF != null)
                    {
                        foreach (var respSPQuery in spEjecucionTEF?.Data ?? new List<Formato>())
                        {
                            long intOpClave = respSPQuery.id_formato;

                            filtroFolios.AgregarFolio(
                            dicFolios,
                            "TEF-" + intOpClave,
                            new FolioOperacionDTO
                            {
                                FolioMaestro = intOpClave.ToString(),
                                FolioDetalle = intOpClave.ToString(),
                                EsAgrupado = false,
                                TipoOperacion = "TEF",
                                DescripcionOperacion = "TEF"
                            });
                        }
                    }
                }

                // Operaciones Time Deposits
                if (_configuracion.OperacionesPermitidas.Contains("TMD") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionTiempoDeposito = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_Tiempo_Deposito, null, ct);
                    if (spEjecucionTiempoDeposito != null)
                    {
                        foreach (var respSPQuery in spEjecucionTiempoDeposito?.Data ?? new List<Formato>())
                        {
                            long intOpClave = respSPQuery.id_formato;

                            filtroFolios.AgregarFolio(
                            dicFolios,
                            "TMD-" + intOpClave,
                            new FolioOperacionDTO
                            {
                                FolioMaestro = intOpClave.ToString(),
                                FolioDetalle = intOpClave.ToString(),
                                EsAgrupado = false,
                                TipoOperacion = "TMD",
                                DescripcionOperacion = "TMD"
                            });
                        }
                    }
                }

                // Operaciones de Nomina Maestro (solo FXD)
                if (_configuracion.OperacionesPermitidas.Contains("NOM_FXD") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionFXD = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_Nomina_FXD, null, ct);

                    if(spEjecucionFXD?.Data == null || !spEjecucionFXD.Data.Any()) Config.EscribirLog("AVISO|No hay detalle de dispersiones pendientes para NOM_FXD", NombreServicio.Entradas.ToString());

                    foreach (var respSPQuery in spEjecucionFXD?.Data ?? new List<Formato>())
                    {
                        long intIdGrupo = respSPQuery.idGrupo;
                        long intOpClave = respSPQuery.id_formato;

                        filtroFolios.AgregarFolio(
                        dicFolios,
                        "NOM_FXD-" + intOpClave,
                        new FolioOperacionDTO
                        {
                            FolioMaestro = intIdGrupo.ToString(),
                            FolioDetalle = intOpClave.ToString(),
                            EsAgrupado = true,
                            TipoOperacion = "NOM_FXD",
                            DescripcionOperacion = "Nómina"
                        });
                    }
                }

                //
                // Operaciones de PaP (Pago a Proveedores) Maestro
                if (_configuracion.OperacionesPermitidas.Contains("PaP_FXD") || _configuracion.OperacionesPermitidas.Contains("PaP_FXI") || _configuracion.OperacionesPermitidas.Contains("PaP_FXT") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionPAPMaestro = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_PAP_Maestro, null, ct);

                    if (spEjecucionPAPMaestro?.Data == null || !spEjecucionPAPMaestro.Data.Any()) Config.EscribirLog("AVISO|No hay detalle de dispersiones pendientes para PaP_FXD, PaP_FXI, PaP_FXT", NombreServicio.Entradas.ToString());

                    foreach (var respSPQuery in spEjecucionPAPMaestro?.Data ?? new List<Formato>())
                    {
                        long intIdGrupo = respSPQuery.idGrupo;
                        long intOpClave = respSPQuery.id_formato;
                        long intTipoFormatoDetalle = respSPQuery.tipoFormato;

                        switch (intTipoFormatoDetalle)
                        {
                            case 3: // PaP_FXD
                                if (_configuracion.OperacionesPermitidas.Contains("PaP_FXD") || _configuracion.OperacionesPermitidas.Length == 0)
                                    filtroFolios.AgregarFolio(
                                    dicFolios,
                                    "PaP_FXD-" + intOpClave,
                                    new FolioOperacionDTO
                                    {
                                        FolioMaestro = intIdGrupo.ToString(),
                                        FolioDetalle = intOpClave.ToString(),
                                        EsAgrupado = true,
                                        TipoOperacion = "PaP_FXD",
                                        DescripcionOperacion = "Pago a Proveedores - FXD"
                                    });
                                break;

                            case 4: // PaP_FXI
                                if (_configuracion.OperacionesPermitidas.Contains("PaP_FXI") || _configuracion.OperacionesPermitidas.Length == 0)
                                    filtroFolios.AgregarFolio(
                                    dicFolios,
                                    "PaP_FXI-" + intOpClave,
                                    new FolioOperacionDTO
                                    {
                                        FolioMaestro = intIdGrupo.ToString(),
                                        FolioDetalle = intOpClave.ToString(),
                                        EsAgrupado = true,
                                        TipoOperacion = "PaP_FXI",
                                        DescripcionOperacion = "Pago a Proveedores - FXI"
                                    });
                                break;

                            case 7: // PaP_FXT
                                if (_configuracion.OperacionesPermitidas.Contains("PaP_FXT") || _configuracion.OperacionesPermitidas.Length == 0)
                                    filtroFolios.AgregarFolio(
                                    dicFolios,
                                    "PaP_FXT-" + intOpClave,
                                    new FolioOperacionDTO
                                    {
                                        FolioMaestro = intIdGrupo.ToString(),
                                        FolioDetalle = intOpClave.ToString(),
                                        EsAgrupado = true,
                                        TipoOperacion = "PaP_FXT",
                                        DescripcionOperacion = "Pago a Proveedores - FXT"
                                    });
                                break;
                        }
                    }
                }

                // Operaciones de PaP_SPID (Pago a Proveedores)) Maestro
                if (_configuracion.OperacionesPermitidas.Contains("PaP_SPID") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionPAPSPIDMaestro = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_PAP_SPID_Maestro, null, ct);

                    if (spEjecucionPAPSPIDMaestro?.Data == null || !spEjecucionPAPSPIDMaestro.Data.Any()) Config.EscribirLog("AVISO|No hay detalle de dispersiones pendientes para PaP_SPID", NombreServicio.Entradas.ToString());

                    foreach (var respSPQuery in spEjecucionPAPSPIDMaestro?.Data ?? new List<Formato>())
                    {
                        long intIdGrupo = respSPQuery.idGrupo;
                        long intOpClave = respSPQuery.id_formato;
                        long intTipoFormatoDetalle = respSPQuery.tipoFormato;

                        switch (intTipoFormatoDetalle)
                        {
                            case 24: // PaP_SPID
                                filtroFolios.AgregarFolio(
                                dicFolios,
                                "PaP_SPID-" + intOpClave,
                                new FolioOperacionDTO
                                {
                                    FolioMaestro = intIdGrupo.ToString(),
                                    FolioDetalle = intOpClave.ToString(),
                                    EsAgrupado = true,
                                    TipoOperacion = "PaP_SPID",
                                    DescripcionOperacion = "Pago a Proveedores - SPID"
                                });
                                break;
                        }
                    }
                }

                // Operaciones de PaP_TEF (Pago a Proveedores)) Maestro
                if (_configuracion.OperacionesPermitidas.Contains("PaP_TEF") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucionPAPTEFMaestro = await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Obtener_PAP_TEF_Maestro, null, ct);

                    if (spEjecucionPAPTEFMaestro?.Data == null || !spEjecucionPAPTEFMaestro.Data.Any()) Config.EscribirLog("AVISO|No hay detalle de dispersiones pendientes para PaP_SPID", NombreServicio.Entradas.ToString());

                    foreach (var respSPQuery in spEjecucionPAPTEFMaestro?.Data ?? new List<Formato>())
                    {
                        long intIdGrupo = respSPQuery.idGrupo;
                        long intOpClave = respSPQuery.id_formato;
                        long intTipoFormatoDetalle = respSPQuery.tipoFormato;

                        switch (intTipoFormatoDetalle)
                        {
                            case 23: // PaP_TEF
                                filtroFolios.AgregarFolio(
                                dicFolios,
                                "PaP_TEF-" + intOpClave,
                                new FolioOperacionDTO
                                {
                                    FolioMaestro = intIdGrupo.ToString(),
                                    FolioDetalle = intOpClave.ToString(),
                                    EsAgrupado = true,
                                    TipoOperacion = "PaP_TEF",
                                    DescripcionOperacion = "Pago a Proveedores - TEF"
                                });
                                break;
                        }
                    }
                }

                return await registraFolios.RegistraFolios(dicFolios, "CMS", ct);

            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesCMS|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
        }
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 namespace Aplicacion.Enums
{
    /// <summary>
    /// Constantes servicios.
    /// </summary>
    public static class ConstantesServicio
    {
        //Store Procedures
        public const string sp_Fecha_Sistema = "usp_ObtenerFechaSistema";
        public const string sp_Parametros_Sistema = "usp_ObtenerParametrosSistemaENTRADAS";
        public const string sp_Es_Dia_Habil = "usp_EsDiaHabil";

        //sp Tomi
        public const string sp_RegistraFolios = "usp_RegistrarEntradasPlanTrabajoENTRADAS";
        public const string dboFolioOperacionType = "dbo.FolioOperacionTypeENTRADAS";

        public const string sp_IdAutorizacionSolicitudesForex = "usp_ObtenerSolicitudesForexPendientesENTRADAS";
        public const string sp_FolioConcentradoras = "usp_ObtenerConcentradorasAbiertasENTRADAS";
        public const string sp_OpClaveBmxOrd = "usp_ObtenerOrdenesBMXPendientesENTRADAS";
        public const string sp_OpFolCalyLiq = "usp_ObtenerLiquidacionesINIENTRADAS";
        public const string sp_NfFxdGph = "usp_ObtenerNFFxdGPHENTRADAS";
        public const string sp_NfFxiGph = "usp_ObtenerNFFxiGPHENTRADAS";
        public const string sp_NfFxtGph = "usp_ObtenerNFFxtGphENTRADAS";
        public const string sp_NfSpidGph = "usp_ObtenerNFSpidGPHENTRADAS";
        public const string sp_NfTefGph = "usp_ObtenerNFTEFGPHENTRADAS";

        public const string sp_NfFxdLocal = "usp_ObtenerNFFxdLocalENTRADAS";
        public const string sp_NfFxiLocal = "usp_ObtenerNFFxiLocalENTRADAS";
        public const string sp_NfFxtLocal = "usp_ObtenerNFFxtLocalENTRADAS";
        public const string sp_NfSpidLocal = "usp_ObtenerNFSpidLocalENTRADAS";

        
        //sp Cms
        public const string sp_Update_MFX = "MUFG_MX2_ActualizaEstatusFormatoENTRADAS";
        public const string sp_Obtener_Nomina_FXD = "MUFG_MX2_ObtenerFormatoNominaENTRADAS";
        public const string sp_Obtener_PAP_Maestro = "MUFG_MX2_ObtenerFormatoPAPENTRADAS";
        public const string sp_Obtener_FXD_FXI_FXT_Impuestos = "MUFG_MX2_ObtenerFormatosMFXENTRADAS";
        public const string sp_Obtener_SPID = "MUFG_MX2_ObtenerFormatoSPIDENTRADAS";
        public const string sp_Obtener_PAP_SPID_Maestro = "MUFG_MX2_ObtenerFormatoSPIDPAPENTRADAS";
        public const string sp_Obtener_SER_TAX = "MUFG_MX2_ObtenerFormatosSER_TAXENTRADAS";
        public const string sp_Obtener_TEF = "MUFG_MX2_ObtenerFormatoTEFENTRADAS";
        public const string sp_Obtener_PAP_TEF_Maestro = "MUFG_MX2_ObtenerFormatoTEFPAPENTRADAS";
        public const string sp_Obtener_Tiempo_Deposito = "MUFG_MX2_ObtenerFormatoTMDENTRADAS";

        //sp Spei - ENLACE
        public const string sp_InterfaceTOMI_SPEI_Estatus_Recibidas = "sp_InterfaceTOMI_SPEI_Estatus_Recibidas";

        //sp Spid
        public const string sp_InterfaceTOMI_SPID_Estatus_Recibidas = "sp_InterfaceTOMI_SPID_Estatus_Recibidas";

        //sp Tef
        public const string sp_InterfaceTOMI_TEF_Recibidas = "sp_InterfaceTOMI_TEF_Recibidas";

    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Aplicacion.Modelos;
using System.Threading;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase de Tomi.
    /// </summary>
    public class OperacionRepositorioTomi : RepositorioMaestro, IOperacionesEntradaTomi
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IOperacionesGenerales opGenerales;

        private readonly FiltroFolios filtroFolios = new FiltroFolios();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioTomi(ConfiguracionServicio configuracion, IOperacionesGenerales operacionesGenerales) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) 
        {
            _configuracion = configuracion;
            opGenerales = operacionesGenerales;
        }

        /// <summary>
        /// Busca operaciones de entrada por COAS con un sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesCOAS(CancellationToken ct)
        {
            try
            {
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación
                if (_configuracion.OperacionesPermitidas.Contains("ENT_COAS") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucion = await EjecutarStoredProcedureQuery<BmxOrd>(ConstantesServicio.sp_OpClaveBmxOrd, null, ct);
                    foreach (var respSPQuery in spEjecucion?.Data ?? new List<BmxOrd>())
                    {
                        long opClave =  Convert.ToInt64(respSPQuery.op_clave);
                        filtroFolios.AgregarFolio(dicFolios,"ENT_COAS-" + opClave, new FolioOperacionDTO { FolioMaestro = opClave.ToString(), FolioDetalle = opClave.ToString(), EsAgrupado = false, TipoOperacion = "ENT_COAS", DescripcionOperacion = "Entrada COAS" }); // Prefijo CS para evitar error por llave duplicada

                    }
                }

                return await opGenerales.RegistraFolios(dicFolios, "entrada por COAS", ct);
            }
            catch (Exception ex) 
            {
                Config.EscribirLog("ERROR|LeeOperacionesCOAS|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
        }

        /// <summary>
        /// Busca operaciones de entrada por concentradoras con un sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesConcentradoras(CancellationToken ct)
        {
            try 
            {
                // sp_FolioConcentradoras falta crear
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación
                if (_configuracion.OperacionesPermitidas.Contains("CNT") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucion = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_FolioConcentradoras, null, ct);
                    foreach (var respSPQuery in spEjecucion?.Data ?? new List<TombinNf>())
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"CNT-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "CNT", DescripcionOperacion = "Entrada Concentradora" }); // Prefijo C para evitar error por llave duplicada
                    }
                }

                return await opGenerales.RegistraFolios(dicFolios, "entrada por Concentradora", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesConcentradoras|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
            
        }

        /// <summary>
        /// Busca en Forex con un sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesFOREX(CancellationToken ct)
        {
            try 
            {
                // sp_IdAutorizacionSolicitudesForex falta crear
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación
                if (_configuracion.OperacionesPermitidas.Contains("MFX") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucion = await EjecutarStoredProcedureQuery<CmmSolicitudesForex>(ConstantesServicio.sp_IdAutorizacionSolicitudesForex, null, ct);
                    foreach (var respSPQuery in spEjecucion?.Data ?? new List<CmmSolicitudesForex>())
                    {
                        string strAutorizacion = respSPQuery.id_autorizacion;
                        filtroFolios.AgregarFolio(dicFolios,"MFX-" + strAutorizacion, new FolioOperacionDTO { FolioMaestro = strAutorizacion.ToString(), FolioDetalle = strAutorizacion.ToString(), EsAgrupado = false, TipoOperacion = "MFX", DescripcionOperacion = "Confirmación FOREX" }); // Prefijo FX para evitar error por llave duplicada
                    }
                }

                return await opGenerales.RegistraFolios(dicFolios, "envío para confirmación de FOREX", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesFOREX|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
            
        }

        /// <summary>
        /// Busca operaciones de entrada por GPH con varios sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesGPH(CancellationToken ct)
        {
            try 
            {
                //Inicio de contenedor de folio y su tipo de operación //Faltan crear los sp_

                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación

                // Operaciones de entrada local FXD
                if (_configuracion.OperacionesPermitidas.Contains("FXD_GPH") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNffxd = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfFxdGph, null, ct);
                    if ((spEjecucionNffxd?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNffxd.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"FXD_GPH-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "FXD_GPH", DescripcionOperacion = "GPH SPEI" }); // Prefijo LD para evitar error por llave duplicada
                    }
                }

                // Operaciones de entrada local FXI
                if (_configuracion.OperacionesPermitidas.Contains("FXI_GPH") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNffxi = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfFxiGph, null, ct);
                    if ((spEjecucionNffxi?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNffxi.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"FXI_GPH-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "FXI_GPH", DescripcionOperacion = "GPH Internacional" }); // Prefijo LI para evitar error por llave duplicada
                    }
                }

                // Operaciones de entrada local FXT
                if (_configuracion.OperacionesPermitidas.Contains("FXT_GPH") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNffxt = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfFxtGph, null, ct);
                    if ((spEjecucionNffxt?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNffxt.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"FXT_GPH-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "FXT_GPH", DescripcionOperacion = "GPH Traspasos" }); // Prefijo LI para evitar error por llave duplicada
                    }
                }

                // Operaciones de entrada local SPID
                if (_configuracion.OperacionesPermitidas.Contains("SPID_GPH") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNfSpid = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfSpidGph, null, ct);
                    if ((spEjecucionNfSpid?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNfSpid.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"SPID_GPH-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "SPID_GPH", DescripcionOperacion = "GPH SPID" }); // Prefijo LS para evitar error por llave duplicada
                    }
                }

                // Operaciones de entrada local TEF
                if (_configuracion.OperacionesPermitidas.Contains("TEF_GPH") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNfTef = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfTefGph, null, ct);
                    if ((spEjecucionNfTef?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNfTef.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"TEF_GPH-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "TEF_GPH", DescripcionOperacion = "GPH TEF" }); // Prefijo LS para evitar error por llave duplicada
                    }
                }

                return await opGenerales.RegistraFolios(dicFolios, "entradas GPH", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesGPH|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
            
        }

        /// <summary>
        /// Busca operaciones de entrada por registro local con varios sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesLocales(CancellationToken ct)
        {
            try
            {
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación

                // Operaciones de entrada local FXD
                if (_configuracion.OperacionesPermitidas.Contains("FXD_Local") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNffxd = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfFxdLocal, null, ct);
                    if ((spEjecucionNffxd?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNffxd.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"FXD_Local-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "FXD_Local", DescripcionOperacion = "Local Doméstica" }); // Prefijo LD para evitar error por llave duplicada
                    }
                }

                // Operaciones de entrada local FXI
                if (_configuracion.OperacionesPermitidas.Contains("FXI_Local") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNffxi = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfFxiLocal, null, ct);
                    if ((spEjecucionNffxi?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNffxi.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"FXI_Local-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "FXI_Local", DescripcionOperacion = "Local Internacional" }); // Prefijo LI para evitar error por llave duplicada
                    }
                }

                // Operaciones de entrada local FXT
                if (_configuracion.OperacionesPermitidas.Contains("FXT_Local") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNffxt = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfFxtLocal, null, ct);
                    if ((spEjecucionNffxt?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNffxt.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"FXT_Local-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "FXT_Local", DescripcionOperacion = "Local Terceros BTMUM" }); // Prefijo LI para evitar error por llave duplicada
                    }
                }

                // Operaciones de entrada local SPID
                if (_configuracion.OperacionesPermitidas.Contains("SPID_Local") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    StoredProcedureResult<TombinNf> spEjecucionNfSpid = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfSpidLocal, null, ct);
                    if ((spEjecucionNfSpid?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNfSpid.Data)
                    {
                        long lngNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,"SPID_Local-" + lngNfFolio, new FolioOperacionDTO { FolioMaestro = lngNfFolio.ToString(), FolioDetalle = lngNfFolio.ToString(), EsAgrupado = false, TipoOperacion = "SPID_Local", DescripcionOperacion = "Local SPID" }); // Prefijo LS para evitar error por llave duplicada
                    }
                }

                return await opGenerales.RegistraFolios(dicFolios, "entradas Locales", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesLocales|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
           
        }

        /// <summary>
        /// Busca operaciones de entrada por MD (Mercado de Dinero) con un sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesMD(CancellationToken ct)
        {
            try 
            {
                // sp_OpFolCalyLiq falta crear
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación
                if (_configuracion.OperacionesPermitidas.Any(x => x.StartsWith("MD_", StringComparison.OrdinalIgnoreCase)) || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucion = await EjecutarStoredProcedureQuery<CalypsoLiquidacion>(ConstantesServicio.sp_OpFolCalyLiq, null, ct);
                    if ((spEjecucion?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucion.Data)
                    {
                        string strTipoOperacion = respSPQuery.tipo_operacion;
                        long intNfFolio = respSPQuery.nf_folio;
                        filtroFolios.AgregarFolio(dicFolios,strTipoOperacion + "-" + intNfFolio, new FolioOperacionDTO
                        {
                            FolioMaestro = intNfFolio.ToString(),
                            FolioDetalle = intNfFolio.ToString(),
                            EsAgrupado = false,
                            TipoOperacion = strTipoOperacion,
                            DescripcionOperacion = "LIQ. CALYPSO " + strTipoOperacion
                        });
                    }
                }

                return await opGenerales.RegistraFolios(dicFolios, "Liquidaciones mercado de dinero", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesMD|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
            
        }
        
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase de SPEI.
    /// </summary>
    public class OperacionRepositorioSpei : RepositorioMaestro, IOperacionesEntradaSpei
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IOperacionesGenerales registraFolios;

        private readonly FiltroFolios filtroFolios = new FiltroFolios();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioSpei(ConfiguracionServicio configuracion, IOperacionesGenerales operacionesGenerales) : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI, null, null)
        {
            _configuracion = configuracion;
            registraFolios = operacionesGenerales;
        }

        /// <summary>
        /// Busca operaciones de entrada por SPEI1 con un sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesSPEI1(CancellationToken ct)
        {
            try
            {
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación
                if (_configuracion.OperacionesPermitidas.Contains("ENT_SPEI") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucion = await EjecutarStoredProcedureQuery<BmxOrd>(ConstantesServicio.sp_InterfaceTOMI_SPEI_Estatus_Recibidas, cmd => { cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", _configuracion.fechaServicio)); }, ct);
                    foreach (var respSPQuery in spEjecucion?.Data ?? new List<BmxOrd>())
                    {
                        long opClave = Convert.ToInt64(respSPQuery.op_clave);

                        filtroFolios.AgregarFolio(
                        dicFolios,
                        "ENT_SPEI-" + opClave,
                        new FolioOperacionDTO
                        {
                            FolioMaestro = opClave.ToString(),
                            FolioDetalle = opClave.ToString(),
                            EsAgrupado = false,
                            TipoOperacion = "ENT_SPEI",
                            DescripcionOperacion = "Entrada SPEI_1"
                        }); // Prefijo S para evitar error por llave duplicada
                    }
                }

                return await registraFolios.RegistraFolios(dicFolios, "entrada por SPEI_1", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesSPEI1|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
            
        }

    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase de SPEI 2
    /// </summary>
    public class OperacionRepositorioSpei2 : RepositorioMaestro, IOperacionesEntradaSpei2
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IOperacionesGenerales registraFolios;

        private readonly FiltroFolios filtroFolios = new FiltroFolios();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioSpei2(ConfiguracionServicio configuracion, IOperacionesGenerales operacionesGenerales) : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI2, null, null)
        {
            _configuracion = configuracion;
            registraFolios = operacionesGenerales;
        }

        /// <summary>
        /// Busca operaciones de entrada por SPEI2 con un sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesSPEI2(CancellationToken ct)
        {
            try
            {
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación
                if (_configuracion.OperacionesPermitidas.Contains("ENT_SPEI_2") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucion = await EjecutarStoredProcedureQuery<BmxOrd>(ConstantesServicio.sp_InterfaceTOMI_SPEI_Estatus_Recibidas, cmd => { cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", _configuracion.fechaServicio)); }, ct);
                    foreach (var respSPQuery in spEjecucion?.Data ?? new List<BmxOrd>())
                    {
                        long opClave = Convert.ToInt64(respSPQuery.op_clave);

                        filtroFolios.AgregarFolio(
                        dicFolios,
                        "ENT_SPEI_2-" + opClave,
                        new FolioOperacionDTO
                        {
                            FolioMaestro = opClave.ToString(),
                            FolioDetalle = opClave.ToString(),
                            EsAgrupado = false,
                            TipoOperacion = "ENT_SPEI_2",
                            DescripcionOperacion = "Entrada SPEI_2"
                        }); // Prefijo S para evitar error por llave duplicada
                    }
                }

                return await registraFolios.RegistraFolios(dicFolios, "entrada por SPEI_2", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesSPEI2|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
        }
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase SPID.
    /// </summary>
    public class OperacionRepositorioSpid : RepositorioMaestro, IOperacionesEntradaSpid
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IOperacionesGenerales registraFolios;

        private readonly FiltroFolios filtroFolios = new FiltroFolios();

        public OperacionRepositorioSpid(ConfiguracionServicio configuracion, IOperacionesGenerales operacionesGenerales) : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPID, null, null)
        {
            _configuracion = configuracion;
            registraFolios = operacionesGenerales;
        }

        /// <summary>
        /// Busca operaciones de entrada por SPID con un sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesSPID(CancellationToken ct)
        {
            try
            {
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación
                if (_configuracion.OperacionesPermitidas.Contains("ENT_SPID") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucion = await EjecutarStoredProcedureQuery<BmxOrd>(ConstantesServicio.sp_InterfaceTOMI_SPID_Estatus_Recibidas, cmd => { cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", _configuracion.fechaServicio)); }, ct);
                    foreach (var respSPQuery in spEjecucion?.Data ?? new List<BmxOrd>())
                    {
                        long opClave = Convert.ToInt64(respSPQuery.op_clave);

                        filtroFolios.AgregarFolio(
                        dicFolios,
                        "ENT_SPID-" + opClave,
                        new FolioOperacionDTO
                        {
                            FolioMaestro = opClave.ToString(),
                            FolioDetalle = opClave.ToString(),
                            EsAgrupado = false,
                            TipoOperacion = "ENT_SPID",
                            DescripcionOperacion = "Entrada SPID"
                        }); // Prefijo S para evitar error por llave duplicada
                    }
                }

                return await registraFolios.RegistraFolios(dicFolios, "entrada por SPID", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesSPID|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
        }
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase de TEF.
    /// </summary>
    public class OperacionRepositorioTef : RepositorioMaestro, IOperacionesEntradaTef
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IOperacionesGenerales registraFolios;

        private readonly FiltroFolios filtroFolios = new FiltroFolios();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioTef(ConfiguracionServicio configuracion, IOperacionesGenerales operacionesGenerales) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TEF, null, null)
        {
            _configuracion = configuracion;
            registraFolios = operacionesGenerales;
        }

        /// <summary>
        /// Busca operaciones de entrada por sistema TEF con un sp de consulta en la base de datos de tomi y los resultados los envia a la funcion general RegistraFolios().
        /// </summary>
        /// <returns>int</returns>
        public async Task<int> LeeOperacionesTEF(CancellationToken ct)
        {
            try
            {
                Dictionary<string, FolioOperacionDTO> dicFolios = new Dictionary<string, FolioOperacionDTO>(); // 0:FolioMaestro; 1:FolioDetalle; 2:Es agrupado; 3:Alias de operación; 4:Descripción de tipo de operación
                if (_configuracion.OperacionesPermitidas.Contains("ENT_TEF") || _configuracion.OperacionesPermitidas.Length == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    var spEjecucion = await EjecutarStoredProcedureQuery<TefRecibidas>(ConstantesServicio.sp_InterfaceTOMI_TEF_Recibidas, cmd => { cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", _configuracion.fechaServicio)); }, ct);
                    foreach (var respSPQuery in spEjecucion?.Data ?? new List<TefRecibidas>())
                    {
                        int intOpClave = checked((int)respSPQuery.IDOperacion);
                        if ((respSPQuery?.MotivoDevolucion ?? 0) > 0)
                        {
                            filtroFolios.AgregarFolio(dicFolios,"ENT_TEF-" + intOpClave, new FolioOperacionDTO
                            {
                                FolioMaestro = intOpClave.ToString(),
                                FolioDetalle = intOpClave.ToString(),
                                EsAgrupado = false,
                                TipoOperacion = "ENT_TEF",
                                DescripcionOperacion = "Devolucion TEF"
                            }); // Prefijo S para evitar error por llave duplicada
                        }
                        else
                        {
                            filtroFolios.AgregarFolio(dicFolios,"ENT_TEF-" + intOpClave, new FolioOperacionDTO
                            {
                                FolioMaestro = intOpClave.ToString(),
                                FolioDetalle = intOpClave.ToString(),
                                EsAgrupado = false,
                                TipoOperacion = "ENT_TEF",
                                DescripcionOperacion = "Entrada TEF"
                            }); // Prefijo S para evitar error por llave duplicada
                        }

                    }
                }

                return await registraFolios.RegistraFolios(dicFolios, "entrada por TEF", ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|LeeOperacionesTEF|Ocurre Excepcion:" + ex.ToString(), NombreServicio.Entradas.ToString());
                throw;
            }
        }
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Bmx order.
    /// </summary>
    public class BmxOrd
    {
        public decimal op_clave { get; set; }
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
﻿using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System.Collections.Generic;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Función para validar que no se repitan los folios.
    /// </summary>
    public class FiltroFolios
    {
        public void AgregarFolio(
            Dictionary<string, FolioOperacionDTO> dicFolios,
            string key,
            FolioOperacionDTO valor)
        {
            if (!dicFolios.ContainsKey(key))
            {
                dicFolios.Add(key, valor);
            }
            else
            {
                Config.EscribirLog($"AVISO|Llave duplicada ignorada: {key}");
            }
        }
    }
}
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.FXD;
using Aplicacion.Modelos;
using Aplicacion.Servicios.FirmaElectronica;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;


namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa FXDH2 H.
    /// </summary>
    public class FXDH2H
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IFXDH2HDao _dao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IComunFXD _comunFXD;

        private const string tipoOperacion = "FXD_H2H";
        private string folioUnico = string.Empty;
        private string folioPropuesto = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXDH2H.
        /// </summary>

        public FXDH2H(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IFXDH2HDao dao,
            ConexionT24Mw vwT24,
            IComunFXD comunFXD)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _dao = dao;
            _vwT24 = vwT24;
            _comunFXD = comunFXD;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ProcesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            bool procesado = false;

            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });

            if (string.IsNullOrEmpty(_configuracion.AliasProceso))
                return false;

            using (var firma = new MetodosFirma(_configuracion))
            {
                if (!firma.CertificadoEncontrado())
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de SPEI: No se encontro el certificado y/o llave para realizar la firma de la transacción");
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}");

            // =========================
            // DATOS_GRAL
            // =========================
            if (_configuracion.AliasProceso == "DATOS_GRAL")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando datos del plan maestro");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                // 1) Obtener datos
                var datos = await _dao.ObtenerDatosFxdH2hPorFolio(folio);

                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString() ?? string.Empty;
                }

                if (string.IsNullOrEmpty(folioUnico))
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("No hay folio_unico asignado para la operación");
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'");
                }

                folioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                    folioUnico,
                    int.Parse(ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion)),
                    tipoOperacion);

                if (string.IsNullOrWhiteSpace(folioPropuesto))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error al obtener el folio propuesto de T24");

                bool actualizado = await _dao.ActualizarPlanTrabajoMaestroDesdeFxdH2h(
                    folio,
                    tipoOperacion,
                    folioUnico,
                ConversionHelper.ParseIntRequerido(fechaEjecucion, "NF_FECHA_EJECUCION", "Error al convertir {0}: {1}"),
                    _vwT24.MonedaDefaultMN);

                if (!actualizado)
                    throw new Exception($"Error actualizando plan maestro {folio}");

                procesado = true;

                if (!await _comunFXD.GeneraFirmaDigital("tb_tombin_nf_fxd_h2h", folio, folioPropuesto))
                    throw new Exception($"Error al generar la firma digital del: {folio}");

                if (procesado)
                {
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    return true;
                }
                else
                {
                    throw new Exception($"Error en procesamiento DATOS_GRAL {folio}");
                }
            }

            return false;
        }

    }
}

 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  AplicacionEntradas.csproj
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 <?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{93A21735-F0FA-4178-9458-51BF9EBEE238}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Aplicacion</RootNamespace>
    <AssemblyName>AplicacionEntradas</AssemblyName>
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
    <PlatformTarget>x86</PlatformTarget>
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
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
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
    <Compile Include="DTOs\CalypsoLiquidacion.cs" />
    <Compile Include="DTOs\BmxOrd.cs" />
    <Compile Include="DTOs\CmmSolicitudesForex.cs" />
    <Compile Include="DTOs\SFecha.cs" />
    <Compile Include="DTOs\TefRecibidas.cs" />
    <Compile Include="DTOs\TombinNf.cs" />
    <Compile Include="DTOs\Formato.cs" />
    <Compile Include="DTOs\ParametroModelo.cs" />
    <Compile Include="Enums\ConstantesServicio.cs" />
    <Compile Include="Enums\ParametrosSistemaENUM.cs" />
    <Compile Include="Interfaces\IOperacionesEntradaCms.cs" />
    <Compile Include="Interfaces\IOperacionesEntradaSpei2.cs" />
    <Compile Include="Interfaces\IOperacionesEntradaSpei.cs" />
    <Compile Include="Interfaces\IOperacionesEntradaSpid.cs" />
    <Compile Include="Interfaces\IOperacionesEntradaTef.cs" />
    <Compile Include="Interfaces\IOperacionesEntradaTomi.cs" />
    <Compile Include="Interfaces\IOperacionesGenerales.cs" />
    <Compile Include="Interfaces\IOperacionServicio.cs" />
    <Compile Include="Interfaces\IParametrosOperaciones.cs" />
    <Compile Include="Modelos\ConfiguracionServicio.cs" />
    <Compile Include="Modelos\FolioOperacionDTO.cs" />
    <Compile Include="Modelos\RegistroFolioModelo.cs" />
    <Compile Include="OperacionServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <ItemGroup>
    <Folder Include="Servicios\" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  InfraestructuraEntradas.csproj
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  <?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{4E3C93B8-F795-461B-A31D-AAD2046E9BB9}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Infraestructura</RootNamespace>
    <AssemblyName>InfraestructuraEntradas</AssemblyName>
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
    <PlatformTarget>x86</PlatformTarget>
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
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
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
    <Compile Include="Properties\AssemblyInfo.cs" />
    <Compile Include="Repositorio\ConsultaCamposBDTomi.cs" />
    <Compile Include="Repositorio\FiltroFolios.cs" />
    <Compile Include="Repositorio\OperacionRepositorioCms.cs" />
    <Compile Include="Repositorio\OperacionRepositorioSpei2.cs" />
    <Compile Include="Repositorio\OperacionRepositorioSpei.cs" />
    <Compile Include="Repositorio\OperacionRepositorioSpid.cs" />
    <Compile Include="Repositorio\OperacionRepositorioTef.cs" />
    <Compile Include="Repositorio\OperacionRepositorioTomi.cs" />
    <Compile Include="Repositorio\OperacionesGenerales.cs" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\AplicacionEntradas.csproj">
      <Project>{93A21735-F0FA-4178-9458-51BF9EBEE238}</Project>
      <Name>AplicacionEntradas</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Entradas.csproj
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  <?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{9544BA40-7D87-4EE1-8A6B-CB0831C8C270}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>Entradas</RootNamespace>
    <AssemblyName>Entradas</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
    <Deterministic>true</Deterministic>
    <NuGetPackageImportStamp>
    </NuGetPackageImportStamp>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <PlatformTarget>x86</PlatformTarget>
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <OutputPath>bin\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <PlatformTarget>x86</PlatformTarget>
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup>
    <StartupObject />
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <HintPath>..\packages\SimpleInjector.5.6.0\lib\net461\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.Numerics" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.ServiceProcess" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="ServiceHost.cs">
      <SubType>Component</SubType>
    </Compile>
    <Compile Include="ServiceHost.Designer.cs">
      <DependentUpon>ServiceHost.cs</DependentUpon>
    </Compile>
    <Compile Include="Program.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <ItemGroup>
    <None Include="App.config" />
    <None Include="packages.config" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\AplicacionEntradas.csproj">
      <Project>{93a21735-f0fa-4178-9458-51bf9ebee238}</Project>
      <Name>AplicacionEntradas</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura\InfraestructuraEntradas.csproj">
      <Project>{4e3c93b8-f795-461b-a31d-aad2046e9bb9}</Project>
      <Name>InfraestructuraEntradas</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
