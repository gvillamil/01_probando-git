using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Servicios;
using Infraestructura.Repositorio;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Aplicacion.SSR2026;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Logeo_utilidades;
using MUFG_T24.Modelos;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ActualizaEstatus

{
        /// <summary>
        /// Clase principal del servicio Windows (presentación).
        /// </summary>
        public partial class ServiceHost : WindowsTcpPaqueteServiceHostBase
{
    private SimpleInjector.Container _containerDI;
    private Configuracion _configuracionBase;
    private ConfiguracionServicio _configuracionServicio;

    protected override string NombreServicioLog => NombreServicio.ActualizaEstatus.ToString();
    protected override bool InicializacionCompleta => _containerDI != null && _configuracionBase != null;

    /// <summary>
    /// Constructor.
    /// </summary>
    public ServiceHost() { }

    /// <summary>
    /// Realiza la obtención de la configuración del servicio 
    /// y la inyección de dependencias.
    /// </summary>
    protected override void Inicializacion(string[] args, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        //Objeto que almacena la configuración necesaria para el funcionamiento del servicio...
        _configuracionBase = new Configuracion();
        _configuracionServicio = new ConfiguracionServicio();

        //Inicializa la obtención de la configuración general y las rutas del log...
        {
            ConfiguracionEncriptadaHelper.Inicializar();
            ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.ActualizaEstatus.ToString());
            _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
            _configuracionBase.RutaArchivoConfiguracion = $@"{EnumRutasGenerales.Path_Log_CMS}\{NombreServicio.ActualizaEstatus.ToString()}\";
            _configuracionBase.PathLog = servicioMainConfiguracion.Log + servicioMainConfiguracion.NombreServicio;
            _configuracionBase.configuracion = servicioMainConfiguracion.configuracion;
            _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
            _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;
            _configuracionBase.LimiteConcurrencia = servicioMainConfiguracion.LimiteConcurrencia;

            Logging.Init(_configuracionBase.PathLog, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
        }

        //Realiza la configuracion del contenedor DI...
        _containerDI = new SimpleInjector.Container();
        _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
        _containerDI.Register<Alertas>(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);
        _containerDI.Register<IAlertas>(() => _containerDI.GetInstance<Alertas>(), Lifestyle.Singleton);
        _containerDI.Register<IControlServicio, ControlServicio>();
        _containerDI.Register<SwiftStandardReleaseSanitize>();
        _containerDI.Register<MUFG_T24.Interfaces_utilidades.IParametrizacion, MUFG_T24.Inicializacion_utilidades.Parametrizacion>();

        //Registra los componentes del servicio...
        _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
        _containerDI.Register<ConfiguracionServicio>(() => _configuracionServicio.Clone(), Lifestyle.Scoped);

        _containerDI.Register<IParametrosOperaciones, ConsultasBDTomi>(Lifestyle.Transient);
        _containerDI.Register<IOperacionesCms, ConsultasBDCms>(Lifestyle.Transient);
        _containerDI.Register<IOperacionesSpei, ConsultasBDSpei>(Lifestyle.Transient);
        _containerDI.Register<IOperacionesSpid, ConsultasBDSpid>(Lifestyle.Transient);
        _containerDI.Register<IOperacionesTef, ConsultasBDTef>(Lifestyle.Transient);
        _containerDI.Register<IOperacionesSpei2, ConsultasBDSpei2>(Lifestyle.Transient);
        _containerDI.Register<IFabricaServicioGeneral, FabricaServicioGeneral>(Lifestyle.Transient);
        _containerDI.Register<IFabricaServiciosOperacion>(() => new FabricaServiciosOperacion(ProcesarOperacionAsync), Lifestyle.Transient);
        _containerDI.Register<Aplicacion.Interfaces.IOperacionServicio, OperacionServicio>(Lifestyle.Transient);


        _containerDI.Verify();
    }

    /// <summary>
    /// Realiza la obtención inicial de parámetros comunes.
    /// </summary>
    protected override async Task ParametrizacionesAsync(CancellationToken ct)
    {
        IParametrizacion parametrizacion = _containerDI.GetInstance<IParametrizacion>();
        _configuracionBase = await parametrizacion.ObtenerProcesoActualAsync(_configuracionBase).ConfigureAwait(false);

        IControlServicio controlServicio = _containerDI.GetInstance<IControlServicio>();
        await controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio).ConfigureAwait(false);

        _configuracionBase.PuertoTCP = await parametrizacion.ObtenerPuertos(_configuracionBase.NombreServicio, _configuracionBase.Pid.Value).ConfigureAwait(false);

        {
            _configuracionServicio.NombreServicio = _configuracionBase.NombreServicio;
            _configuracionServicio.TimerInterval = _configuracionBase.TimerInterval;
            _configuracionServicio.Pid = _configuracionBase.Pid;
            _configuracionServicio.NombreDisplay = _configuracionBase.NombreDisplay;
            _configuracionServicio.FechaSistema = _configuracionBase.FechaSistema;
            _configuracionServicio.NombreProceso = _configuracionBase.NombreProceso;
            _configuracionServicio.AliasProceso = "";
            _configuracionBase.AliasProceso = "";
        }

        ConfigurarTcpListener(
            ConversionesComunes.ParseEnteroRequerido(_configuracionBase.PuertoTCP, nameof(_configuracionBase.PuertoTCP)),
            _configuracionBase.LimiteConcurrencia ?? 5);
    }

    /// <summary>
    /// Procesa la petición recibida.
    /// </summary>
    protected override async Task<RespuestaResumenDto> ProcesarPaqueteAsync(SolicitudPaqueteDto paquete, string[] operaciones, CancellationToken token)
    {
        using (AsyncScopedLifestyle.BeginScope(_containerDI))
        {
            string[] operacionesNormalizadas = operaciones ?? Array.Empty<string>();
            var _operacionServicio = _containerDI.GetInstance<Aplicacion.Interfaces.IOperacionServicio>();
            RespuestaResumenDto resumen = await _operacionServicio
                .EjecutaProcedimientosAsync(paquete.Tipo, operacionesNormalizadas);

            return resumen;
        }
    }

    /// <summary>
    /// Realiza ciertas operaciones antes de finalizar el servicio.
    /// </summary>
    protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
    {
        using (AsyncScopedLifestyle.BeginScope(_containerDI))
        {
            IControlServicio controlServicio = _containerDI.GetInstance<IControlServicio>();

            await controlServicio.EsperarTareaActivaAsync(startupTask, "ParametrizacionInicial", waitCt).ConfigureAwait(false);
            await controlServicio.EsperarTareaActivaAsync(runningTask, "ProcesamientoActivo", waitCt).ConfigureAwait(false);

            await EsperarClientesActivosAsync(cleanupCt).ConfigureAwait(false);

            Configuracion configuracionBase = _containerDI.GetInstance<Configuracion>();

            await controlServicio.MarcarPuertoEnUsoAsync(null, null, null, configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);
            await controlServicio.ActualizaTerminacion(configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Especifica los recursos a liberar al finalizar el servicio.
    /// </summary>
    protected override void LiberarRecursosTerminacion() => _containerDI?.Dispose();

}
}


using Aplicacion.DTOs;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Servicios;
using System;
using System.Threading.Tasks;
using ConstantesOperacion = Aplicacion.Enums.ConstantesServicios;

namespace ActualizaEstatus
{
    public partial class ServiceHost
    {
        private T Resolver<T>() where T : class
        {
            return _containerDI.GetInstance<T>();
        }

        private async Task<bool> ProcesarOperacionAsync(string tipoOperacion, OperacionDTO operacion)
        {
            switch (tipoOperacion)
            {
                case ConstantesOperacion.OperacionFXD:
                    return await new Servicio_FXD(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_FXD();
                case ConstantesOperacion.OperacionFXI:
                    return await new Servicio_FXI(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionFXI();
                case ConstantesOperacion.OperacionFXT:
                    return await new Servicio_FXT(operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>()).ProcesarOperacionFXT();
                case ConstantesOperacion.OperacionSAT:
                    return await new Servicio_SAT(operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>()).ProcesarOperacionSAT();
                case ConstantesOperacion.OperacionMFX:
                    return await new Servicio_MFX(operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>()).ProcesarOperacionMFX();
                case ConstantesOperacion.OperacionFXDLocal:
                    return await new Servicio_FXDLocal(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionServicio_FXDLocal();
                case ConstantesOperacion.OperacionFXILocal:
                    return await new Servicio_FXILocal(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionServicio_FXILocal();
                case ConstantesOperacion.OperacionFXTLocal:
                    return await new Servicio_FXTLocal(operacion, Resolver<IParametrosOperaciones>()).ProcesarOperacionServicio_FXTLocal();
                case ConstantesOperacion.OperacionENTSPEI:
                    return await new Servicio_ENT_SPEI(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesSpei>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionENT_SPEI();
                case ConstantesOperacion.OperacionENTSPEI2:
                    return await new Servicio_ENT_SPEI2(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesSpei2>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionENT_SPEI2();
                case ConstantesOperacion.OperacionENTCOAS:
                    return await new Servicio_ENT_COAS(operacion, Resolver<IParametrosOperaciones>()).ProcesarOperacionENT_COAS();
                case ConstantesOperacion.OperacionENTSPID:
                    return await new Servicio_ENT_SPID(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesSpid>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionENT_SPID();
                case ConstantesOperacion.OperacionSPIDLocal:
                    return await new Servicio_SPIDLocal(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionServicio_SPIDLocal();
                case ConstantesOperacion.OperacionSPID:
                    return await new Servicio_SPID(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionServicio_SPID();
                case ConstantesOperacion.OperacionCNT:
                    return await new Servicio_CNT(operacion, Resolver<IParametrosOperaciones>()).ProcesarOperacionServicio_CNT();
                case ConstantesOperacion.OperacionNOMFXD:
                    return await new Servicio_NOM_FXD(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_NOM_FXD();
                case ConstantesOperacion.OperacionPaPFXD:
                    return await new Servicio_PaP_FXD(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_PaP_FXD();
                case ConstantesOperacion.OperacionPaPFXI:
                    return await new Servicio_PaP_FXI(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_PaP_FXI();
                case ConstantesOperacion.OperacionPaPFXT:
                    return await new Servicio_PaP_FXT(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_PaP_FXT();
                case ConstantesOperacion.OperacionPaPSPID:
                    return await new Servicio_PaP_SPID(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_PaP_SPID();
                case ConstantesOperacion.OperacionTMD:
                    return await new Servicio_TMD(operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>()).ProcesarOperacionTMD();
                case ConstantesOperacion.OperacionTEF:
                    return await new Servicio_TEF(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>()).ProcesarOperacionTEF();
                case ConstantesOperacion.OperacionPaPTEF:
                    return await new Servicio_PaP_TEF(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesCms>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_PaP_TEF();
                case ConstantesOperacion.OperacionENTTEF:
                    return await new Servicio_ENT_TEF(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IOperacionesTef>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionENT_TEF();
                case ConstantesOperacion.OperacionFXDGPH:
                    return await new Servicio_FXD_GPH(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_FXD();
                case ConstantesOperacion.OperacionFXIGPH:
                    return await new Servicio_FXI_GPH(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_FXI_GPH();
                case ConstantesOperacion.OperacionFXTGPH:
                    return await new Servicio_FXT_GPH(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>()).ProcesarOperacion_FXT_GPH();
                case ConstantesOperacion.OperacionSPIDGPH:
                    return await new Servicio_SPID_GPH(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionServicio_SPID_GPH();
                case ConstantesOperacion.OperacionTEFGPH:
                    return await new Servicio_TEF_GPH(operacion, Resolver<IParametrosOperaciones>()).ProcesarOperacion_TEF_GPH();
                case ConstantesOperacion.OperacionFXDH2H:
                    return await new Servicio_FXD_H2H(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_FXD_H2H();
                case ConstantesOperacion.OperacionFXIH2H:
                    return await new Servicio_FXI_H2H(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacion_FXI_H2H();
                case ConstantesOperacion.OperacionFXTH2H:
                    return await new Servicio_FXT_H2H(operacion, Resolver<IParametrosOperaciones>()).ProcesarOperacion_FXT_H2H();
                case ConstantesOperacion.OperacionSPIDH2H:
                    return await new Servicio_SPID_H2H(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionServicio_SPID_H2H();
                case ConstantesOperacion.OperacionTEFH2H:
                    return await new Servicio_TEF_H2H(operacion, Resolver<IParametrosOperaciones>()).ProcesarOperacion_TEF_H2H();
                case ConstantesOperacion.OperacionSER:
                    return await new Servicio_SER(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionSER();
                case ConstantesOperacion.OperacionTAX:
                    return await new Servicio_TAX(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionTAX();
                case ConstantesOperacion.OperacionMDUNKNOW:
                case ConstantesOperacion.OperacionMDT24:
                case ConstantesOperacion.OperacionMDSPEIE:
                case ConstantesOperacion.OperacionMDSPEIS:
                case ConstantesOperacion.OperacionMDSPIDE:
                case ConstantesOperacion.OperacionMDSPIDS:
                case ConstantesOperacion.OperacionMDMT103S:
                case ConstantesOperacion.OperacionMDMT202S:
                case ConstantesOperacion.OperacionMDMT210S:
                case ConstantesOperacion.OperacionMDMT300S:
                    return await new Servicio_MercadoDinero(Resolver<ConfiguracionServicio>(), operacion, Resolver<IParametrosOperaciones>(), Resolver<IFabricaServicioGeneral>()).ProcesarOperacionMercadoDinero();
                default:
                    throw new Exception($"Tipo de operación desconocida: {tipoOperacion}");
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Logeo_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Clase principal de OperacionServicio.
    /// </summary>
    public class OperacionServicio : IOperacionServicio
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IParametrosOperaciones _IparametrosOperaciones;
        private readonly IFabricaServiciosOperacion _fabricaServiciosOperacion;

        /// <summary>
        /// Constructor de OperacionServicio.
        /// Inicializa las dependencias necesarias para la ejecución de operaciones,
        /// consulta de parámetros y procesamiento de los distintos tipos de transacciones.
        /// </summary>
        /// <param name="configuracion">Configuración general del servicio.</param>
        /// <param name="iparametrosoperaciones">Parámetros requeridos durante la ejecución.</param>
        /// <param name="fabricaServiciosOperacion">Fábrica para resolver y ejecutar servicios por tipo de operación.</param>
        public OperacionServicio(ConfiguracionServicio configuracion, IParametrosOperaciones iparametrosoperaciones, IFabricaServiciosOperacion fabricaServiciosOperacion)
        {
            _configuracion = configuracion;
            _IparametrosOperaciones = iparametrosoperaciones;
            _fabricaServiciosOperacion = fabricaServiciosOperacion;
        }

        /// <summary>
        /// Procedimiento para la ejecución de las operaciones
        /// </summary>
        /// <returns></returns>
        public async Task<RespuestaResumenDto> EjecutaProcedimientosAsync(string tipoOperacion = null, string[] folios = null)
        {
            RespuestaResumenDto resumen = new RespuestaResumenDto(folios != null? folios.Length: 0);

            string contextoTipoOperacion = tipoOperacion;
            string contextoFolio = null;

            using (Logging.BeginWorkerScope())
            using (Logging.BeginOperacionScope(contextoTipoOperacion, null))
            {

            //Actualiza el proceso que se está ejecutando

            await _IparametrosOperaciones.MarcarServicioEnEjecucion(_configuracion.NombreServicio);

            //Obtiene los parametros del Sistema

            Dictionary<string, string> dicParametrosSistema = await _IparametrosOperaciones.parametrosSistema();


            //Validar los horarios de operación
            TimeSpan timeHoraSistema = new TimeSpan(9, 0, 0);
            bool boHoraInicioOperacion = false;

            _configuracion.strFechayyyyMMdd = DateTime.Now.ToString("yyyyMMdd");

            // Carga los parametros de configuración para el inicio de las operaciones
            foreach (var paramSistema in dicParametrosSistema)
            {
                if (!Enum.TryParse(paramSistema.Key, out ParametrosSistema parametro)) continue;

                switch (parametro)
                {
                    case ParametrosSistema.HORA_LECTURA_INICIO:
                        _configuracion.StrHoraLecturaInicio = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_LECTURA_INICIO.ToString());
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Hora de inicio de lectura '{0}'. ", paramSistema.Value));
                        break;

                    case ParametrosSistema.HORA_LECTURA_FIN:
                        _configuracion.StrHoraLecturaFin = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_LECTURA_FIN.ToString());
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Hora de Finalización de lectura '{0}'. ", paramSistema.Value));
                        break;

                    case ParametrosSistema.SPEI_SERVIDOR_1:
                        _configuracion.StrNombreSPEI1 = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Servidor 1 de SPEI activo '{0}'. ", paramSistema.Value));
                        break;

                    case ParametrosSistema.SPEI_SERVIDOR_2:
                        _configuracion.StrNombreSPEI2 = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Servidor 2 de SPEI activo '{0}'. ", paramSistema.Value));
                        break;

                    case ParametrosSistema.WST24_MW_COMPANY:
                        _configuracion.StrCompanyName = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|WST24_MW_COMPANY '{0}'. ", paramSistema.Value));
                        break;
                    case ParametrosSistema.WST24_MW_USERNAME_REG:
                        _configuracion.StrUserNameReg = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|WST24_MW_USERNAME_REG '{0}'. ", paramSistema.Value));
                        break;
                    case ParametrosSistema.WST24_MW_PASS_REG:
                        _configuracion.StrPasswdReg = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|WST24_MW_PASS_REG ***********"));
                        break;
                    case ParametrosSistema.WS_REVERSA_ENDPOINT:
                        _configuracion.UriReversa = paramSistema.Value;
                        Config.EscribirLog($"INFO|UriReversa:{paramSistema.Value}");
                        break;

                    }
            }

            //Validar los parámetros y enviar excepciones si alguno no es válido
            if (_configuracion.StrHoraLecturaInicio == null)
                throw new Exception("No se especifica la hora de inicio de operaciones");

            if (_configuracion.StrHoraLecturaFin == null)
                throw new Exception("No se especifica la hora de finalización de operaciones");

            // Validar si está en Horario de Operación
            boHoraInicioOperacion = (timeHoraSistema >= _configuracion.StrHoraLecturaInicio && timeHoraSistema <= _configuracion.StrHoraLecturaFin);
            Config.EscribirLog(String.Format("INFO|Ejecuta_procedimientos|{0} horario de operaciones, HoraSistema= '{1}'.",(boHoraInicioOperacion ? "Inicio" : "Fin"),timeHoraSistema.ToString()));

            //Recuperar los Datos de las Operaciones a Procesar

            List<OperacionDTO> operaciones = new List<OperacionDTO>();

            foreach (string folio in folios ?? Array.Empty<string>())
            {
                operaciones.Add(new OperacionDTO
                {
                    StrFolio = folio,
                    StrTipoOperacion = tipoOperacion
                });
            }

                // Obtencion de la fecha del sistema
                var fechaActual = DateTime.Now;
                _configuracion.StrFechaSistema = new TimeSpan(fechaActual.Hour, fechaActual.Minute, 0);

            if (_configuracion.StrFechaSistema >= _configuracion.StrHoraLecturaInicio && _configuracion.StrFechaSistema <= _configuracion.StrHoraLecturaFin) // Inicio Horario
            {
                if (!_configuracion.BHorarioLectura)
                {
                    _configuracion.BHorarioLectura = true;
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Inicio horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.Entradas.ToString());
                }
            }
            else // Termina Horario
            {
                if (_configuracion.BHorarioLectura)
                {
                    _configuracion.BHorarioLectura = false;
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Termina horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.Entradas.ToString());
                }
            }

            if (!_configuracion.BHorarioLectura) // Fuera de Horario
            {
                Config.EscribirLog($"INFO|Ejecuta_Procedimientos| El Servicio esta fuera de Horario.", NombreServicio.Entradas.ToString());
                // Inserto la alarma
                _IparametrosOperaciones.EnviaAlerta(new AlertaDTO()
                {
                    TipoOPeracion = null,
                    Folio = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = ConstantesClaveAviso.Critico,
                    Descipcion = "Error en ejecuta_procedimientos",
                    PId = (int)_configuracion.Pid,
                    NombreProceso = _configuracion.NombreServicio,
                    Exception = new Exception("El servicio '" + _configuracion.NombreServicio + "' está fuera de horario.")
                });
            }

            // Ejecutar procedimientos de Acuerdo al Tipo de Operación

            bool bResultadoOperacion = false;

                try
                {
                    foreach (OperacionDTO operacion in operaciones)
                    {
                        
                        contextoTipoOperacion = operacion?.StrTipoOperacion;
                        contextoFolio = operacion?.StrFolio;

                        using (Logging.BeginOperacionScope(contextoTipoOperacion, contextoFolio))
                        {
                            try
                            {
                                resumen.Procesadas++;
                                operacion.PId = (int)_configuracion.Pid;
                                operacion.StrServicio = ConstantesServicios.NombreServicio;
                                bResultadoOperacion = await _fabricaServiciosOperacion.ProcesarAsync(contextoTipoOperacion, operacion);

                                if (bResultadoOperacion)
                                {
                                    resumen.Ok++;

                                    Config.EscribirLog(String.Format("INFO|Ejecuta_Procedimientos|Se termina procesamiento de folio: {0}", contextoFolio));
                                }
                                else
                                {
                                    resumen.Error++;
                                    Config.EscribirLog(String.Format("ERROR|Ejecuta_Procedimientos|Error en la ejecución del folio: {0}", contextoFolio));
                                }
                            }
                            catch (Exception ex)
                            {
                                resumen.Error++;
                                Config.EscribirLog(String.Format("ERROR|Ejecuta_Procedimientos|Se presenta excepcion para folio {0}: {1}", contextoFolio, ex));
                                continue;
                            }
                        }
                        
                    }


                }
                catch (Exception ex)
                {
                    using (Logging.BeginOperacionScope(contextoTipoOperacion, contextoFolio))
                    {
                        Config.EscribirLog(String.Format("ERROR|Ejecuta_Procedimientos|Se presenta excepcion: {0}", ex.ToString()));
                        _IparametrosOperaciones.EnviaAlerta(new AlertaDTO()
                        {
                            TipoOPeracion = null,
                            Folio = null,
                            Servicio = _configuracion.NombreServicio,
                            ClaveAviso = ConstantesClaveAviso.Critico,
                            Descipcion = "Error en ejecuta_procedimientos",
                            PId = (int)_configuracion.Pid,
                            NombreProceso = _configuracion.NombreServicio,
                            Exception = ex
                        });
                    }
                }
                finally 
                {
                    try
                    {
                        await _IparametrosOperaciones.MarcarServicioEnFinalizacion(_configuracion.NombreServicio);
                    }
                    catch (Exception ex) 
                    {
                        Config.EscribirLog($"ERROR|No fue posible marcar el servicio en finalización: {ex}");
                    }
                }

            return resumen;
            }
        }

        /// <summary>
        /// Obtiene el puerto configurado para el servicio.
        /// </summary>
        /// <returns>
        /// Puerto de escucha configurado.
        /// </returns>
        public Task<string> ObtenerPuertoEscucha() => _IparametrosOperaciones.obtenerPuertoEscucha();
    }

    /// <summary>
    /// Fábrica para crear y ejecutar servicios por tipo de operación.
    /// </summary>
    public class FabricaServiciosOperacion : IFabricaServiciosOperacion
    {
        private readonly ProcesadorOperacion _procesadorOperacion;

        public FabricaServiciosOperacion(ProcesadorOperacion procesadorOperacion)
        {
            _procesadorOperacion = procesadorOperacion;
        }

        public Task<bool> ProcesarAsync(string tipoOperacion, OperacionDTO operacion) =>
            _procesadorOperacion(tipoOperacion, operacion);
    }

}

using Aplicacion.DTOs;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using System.Collections.Generic;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio Base.
    /// </summary>
    public class ServicioBase
    {
        private readonly IFabricaServicioGeneral _fabricaServicioGeneral;

        protected ServicioBase(IFabricaServicioGeneral fabricaServicioGeneral = null)
        {
            _fabricaServicioGeneral = fabricaServicioGeneral;
        }

        protected ConfiguracionServicio _Configuracion;
        protected OperacionDTO _Operacion;
        protected IParametrosOperaciones _IParametrosOperaciones;
        protected Dictionary<string, object> dicParametros;

        protected ServicioGeneral CrearServicioGeneral() => _fabricaServicioGeneral.Crear();
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading.Tasks;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;

namespace Aplicacion.Servicios
{
    public class FabricaServicioGeneral : IFabricaServicioGeneral
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IParametrosOperaciones _operaciones;
        private readonly IOperacionesCms _operacionesCms;
        private readonly IOperacionesSpei _operacionesSpei;
        private readonly IOperacionesSpid _operacionesSpid;
        private readonly IOperacionesTef _operacionesTef;

        public FabricaServicioGeneral(
            ConfiguracionServicio configuracion,
            IParametrosOperaciones operaciones,
            IOperacionesCms operacionesCms,
            IOperacionesSpei operacionesSpei,
            IOperacionesSpid operacionesSpid,
            IOperacionesTef operacionesTef)
        {
            _configuracion = configuracion;
            _operaciones = operaciones;
            _operacionesCms = operacionesCms;
            _operacionesSpei = operacionesSpei;
            _operacionesSpid = operacionesSpid;
            _operacionesTef = operacionesTef;
        }

        public ServicioGeneral Crear() => new ServicioGeneral(
            _configuracion,
            _operaciones,
            _operacionesCms,
            _operacionesSpei,
            _operacionesSpid,
            _operacionesTef);
    }

    /// <summary>
    /// Clase principal de servicio general.
    /// </summary>
    public class ServicioGeneral 
    {
        private readonly IParametrosOperaciones _IOperaciones;
        private readonly IOperacionesCms _OperacionesCms;
        private readonly IOperacionesSpid _OperacionesSpid;
        private readonly IOperacionesSpei _OperacionesSpei;
        private readonly IOperacionesTef _OperacionesTEF;
        private readonly ConfiguracionServicio _Configuracion;
        private Dictionary<string, object> dicParametros;

        /// <summary>
        /// Constructor de ServicioGeneral.
        /// Inicializa las dependencias necesarias para la validación de estatus,
        /// actualización de operaciones y comunicación con los distintos sistemas.
        /// </summary>
        /// <param name="configuracion">Configuración general del servicio.</param>
        /// <param name="operaciones">Operaciones generales y parámetros del sistema.</param>
        /// <param name="ioperacionesCMS">Operaciones relacionadas con CMS.</param>
        /// <param name="ioperacionesSPEI">Operaciones relacionadas con SPEI.</param>
        /// <param name="ioperacionesSPID">Operaciones relacionadas con SPID.</param>
        /// <param name="operacionesTEF">Operaciones relacionadas con TEF.</param>
        public ServicioGeneral(ConfiguracionServicio configuracion, IParametrosOperaciones operaciones, IOperacionesCms ioperacionesCMS = null, IOperacionesSpei ioperacionesSPEI = null, IOperacionesSpid ioperacionesSPID = null, IOperacionesTef operacionesTEF = null)
        {
            _Configuracion = configuracion;
            _IOperaciones = operaciones;
            _OperacionesCms = ioperacionesCMS;
            _OperacionesSpei = ioperacionesSPEI;
            _OperacionesSpid = ioperacionesSPID;
            _OperacionesTEF = operacionesTEF;
        }

        /// <summary>
        /// Valida el estatus de una operación SPEI y actualiza los estatus
        /// correspondientes en TOMI de acuerdo con la respuesta obtenida.
        /// </summary>
        /// <param name="tablaTomi">Tabla TOMI donde se encuentra registrada la operación.</param>
        /// <param name="CampoTablaTomi">Campo utilizado para localizar la operación.</param>
        /// <param name="FolioTomi">Folio de la operación a validar.</param>
        /// <param name="TipoOperacion">Tipo de operación en proceso.</param>
        /// <returns>
        /// Resultado de la validación de la operación SPEI.
        /// </returns>
        public async Task<EstatusSPEIModel> ValidaEstatusSPEI(string tablaTomi, string CampoTablaTomi,string FolioTomi, string TipoOperacion)
        {
            EstatusSPEIModel resultado = new EstatusSPEIModel();

            try
            {
                Config.EscribirLog($"INFO|Config.ValidaEstatus_SPEI|A revisar el estatus en SPEI del Folio {FolioTomi} en la base : {tablaTomi}");

                DatoSPEIDTO datoSpei = await _IOperaciones.EjecutarSPValidaEstatusSPEI(tablaTomi, CampoTablaTomi, FolioTomi);

                if (datoSpei == null) throw new Exception("No se encontro el registro en la tabla de TOMI");

                switch (datoSpei.Nf_estatus)
                {
                    case ConstantesServicios.TomiEstatusRECC:
                        Config.EscribirLog($"INFO|Config.ValidaEstatus_SPEI|Operación CANCELADA, no buscará el status en SPEI");

                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaEstatusRechazadoSPEI, new Dictionary<string, object>()
                        {
                            {"tablatomi", tablaTomi},
                            {"campotomi", CampoTablaTomi},
                            {"foliotomi", FolioTomi},
                        });

                        resultado.BResultado = true;
                        return resultado;
                    case ConstantesServicios.TomiEstatusCONP:
                    case ConstantesServicios.TomiEstatusCONF:

                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaEstatusProcesadoSPEI, new Dictionary<string, object>()
                        {
                            {"tablatomi", tablaTomi},
                            {"campotomi", CampoTablaTomi},
                            {"foliotomi", FolioTomi},
                        });

                        break;

                }

                if (datoSpei.Nf_fecha_ejecucion.ToString() != _Configuracion.strFechayyyyMMdd)
                    throw new Exception("La fecha de operación es distinta a la del sistema ");

                SalidaEstatusSPEIDTO resultadoStatusSpei = await _OperacionesSpei.EjecutarSPSPEIStatus(datoSpei.Nf_opfechaoper_spei, datoSpei.T24_folio_asignado);

                if (resultadoStatusSpei == null)
                    throw new Exception($"No fue posible consultar el estatus SPEI para el folio {FolioTomi}");
                
                string StrCadenaRegreso = String.Format("{0}|{1}|{2}", resultadoStatusSpei.op_estado, resultadoStatusSpei.op_monto, resultadoStatusSpei.Clave?.ToString() ?? string.Empty);

                await _OperacionesSpei.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_InterfacesTOMI_SPEI_Status3, new Dictionary<string, object>()
                {
                    {"op_fecha_oper", datoSpei.Nf_opfechaoper_spei},
                    {"op_cve_rastreo", datoSpei.Nf_opclave_spei}
                });

                Config.EscribirLog($"INFO|Config.ValidaEstatus_SPEI|SQL de Actualización de tabla SPEI_e");

                if (string.IsNullOrEmpty(StrCadenaRegreso)) await _OperacionesSpei.EjecutarSPValidaEstatusAdicionalSPEI(datoSpei.Nf_opfechaoper_spei, datoSpei.T24_folio_asignado);

                
                string[] strResultado = StrCadenaRegreso.Split('|');
                switch (strResultado[0])
                {
                    case ConstantesServicios.TomiEstatusLQ:
                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaEstatusInterno_SPEI_LQ, new Dictionary<string, object>()
                {
                    {"tablatomi", tablaTomi},
                    {"campotomi", CampoTablaTomi},
                    {"foliotomi", FolioTomi},
                });
                        break;

                    case ConstantesServicios.TomiEstatusD:
                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaEstatusInterno_SPEI_D, new Dictionary<string, object>()
                {
                    {"tablatomi", tablaTomi},
                    {"campotomi", CampoTablaTomi},
                    {"foliotomi", FolioTomi},
                });
                        break;

                    case ConstantesServicios.TomiEstatusCL:
                    case ConstantesServicios.TomiEstatusRL:
                    case ConstantesServicios.TomiEstatusCN:
                    case ConstantesServicios.TomiEstatusRB:

                        
                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaEstatusInterno_SPEI, new Dictionary<string, object>()
                        {
                            {"tablatomi", tablaTomi},
                            {"campotomi", CampoTablaTomi},
                            {"foliotomi", FolioTomi},
                            {"estatus", resultadoStatusSpei.op_estado }
                        });

                        break;
                    default:
                        await _OperacionesSpei.EjecutarSPValidaEstatusAdicionalSPEI(datoSpei.Nf_opfechaoper_spei, datoSpei.T24_folio_asignado);
                        break;
                }
                resultado.BResultado = true;
                resultado.StrSPey = resultadoStatusSpei.op_estado;

                Config.EscribirLog($"INFO|Config.ValidaEstatus_SPEI|Se termina procesamiento de folio: {FolioTomi}");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Config.ValidaEstatus_SPEI|Se presenta exception: {ex.ToString()}. Trace: {ex.InnerException}");
                _IOperaciones.EnviaAlerta(new AlertaDTO()
                {
                    Folio = FolioTomi,
                    TipoOPeracion = TipoOperacion,
                    NombreProceso = TipoOperacion
                });
            }

            return resultado;
        }

        /// <summary>
        /// Actualiza la información de CMS y TOMI para operaciones SPEI
        /// de acuerdo con el estatus final de la operación.
        /// </summary>
        /// <param name="tablaTomi">Tabla TOMI donde se encuentra registrada la operación.</param>
        /// <param name="FolioTomi">Folio de la operación.</param>
        /// <param name="TipoOperacion">Tipo de operación en proceso.</param>
        /// <param name="BaseCMS">Base o tabla CMS a actualizar.</param>
        /// <param name="strFT">Identificador FT asociado a la operación.</param>
        /// <returns>
        /// True cuando la actualización finaliza correctamente; en caso contrario, false.
        /// </returns>
        public async Task<bool> Actualiza_CMS_origen_SPEI(string tablaTomi, string FolioTomi, string TipoOperacion, string BaseCMS)
        {
            
            try
            {
                string strSPNombre = string.Empty;

                List<CatRazonRechazoDTO> razonesRechazo = null;
                CatRazonRechazoDTO razonRechazo = null;

                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|A revisar el estatus final en SPEI del folio: {FolioTomi}");

                if (tablaTomi.Contains(ConstantesServicios.VarianteServicioGPH))
                    strSPNombre = ProcedimientoAlmacenado.sp_Consultar_OrigenSPEI_CMS_GPH;
                else
                    strSPNombre = ProcedimientoAlmacenado.sp_Consultar_OrigenSPEI_CMS_FXD;


                List<DatoTomiNFDTO> datosTomi = await _IOperaciones.EjecutarSP<DatoTomiNFDTO>(strSPNombre, new Dictionary<string, object>()
                {
                    { "tablatomi", tablaTomi},
                    { "folio", FolioTomi }
                });
                DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI| Se ejecuta el Procedimiento: {strSPNombre}");

                if (datoTomi == null)
                    throw new Exception("No se encontró el registro en TOMI");

                if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                {
                    switch (datoTomi.nf_estatus_interno)
                    {
                        case ConstantesServicios.TomiEstatusLQ:


                            await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Comunes_Actualizar_CmsFormato, new Dictionary<string, object>()
                            {
                                { "tablacms", BaseCMS },
                                { "tablatomi", tablaTomi },
                                { "horaautorizacion", datoTomi.Nf_fecha_autoriza.ToString("HH:mm:ss") },
                                { "foio", FolioTomi }
                            });


                            switch (BaseCMS)
                            {
                                case ConstantesServicios.BaseCMSFormato:
                                    await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Insertar_CmsFormato, new Dictionary<string, object> {
                                { "folio", FolioTomi },
                                { "ft", datoTomi.T24_transaction_id }
                            });
                                    break;
                                case ConstantesServicios.BaseCMSFormatoPaPDetalle:
                                    await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Insertar_CmsFormatoPaPDetalle, new Dictionary<string, object> {
                                { "folio", FolioTomi },
                                { "ft", datoTomi.T24_transaction_id }
                            });
                                    break;
                            }

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|Terminando actualizacion de aceptación de operación");
                            

                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                            {
                                {"folio", FolioTomi },
                                {"tipo_operacion", TipoOperacion },
                                {"opcion" , 4 }
                            });

                            break;

                        case ConstantesServicios.TomiEstatusD:
                            

                            razonesRechazo = await _IOperaciones.EjecutarSP<CatRazonRechazoDTO>(ProcedimientoAlmacenado.sp_Consultar_Cat_RazonRechazo, new Dictionary<string, object>
                            {
                                { "clave", datoTomi.cd_clave },
                                { "tipoPago", ConstantesServicios.EstatusSPEI }
                            });
                            razonRechazo = razonesRechazo?.FirstOrDefault();

                            if (razonRechazo == null)
                                throw new Exception($"No se encontró la razón de rechazo SPEI para la clave {datoTomi.cd_clave}");

                            if (ConstantesServicios.VarianteServicioFXD.Equals(TipoOperacion))
                            {
                                

                                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|CMS - codigo_mensaje_CMS {razonRechazo.Codigo }");

                                

                                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|Ejecuta el procedimiento {ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPEI_Rechazado} con los parametros: Folio {FolioTomi}; BaseCMS {BaseCMS}; CodigoMensaje {razonRechazo.Codigo}");
                                await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPEI_Rechazado, new Dictionary<string, object>()
                                {
                                    { "tablacms", BaseCMS },
                                    { "tablatomi", tablaTomi },
                                    { "codigomensaje", razonRechazo.Codigo },
                                    { "folio", FolioTomi }
                                });
                            }
                            else
                            {
                                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|Ejecuta el procedimiento {ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPEI_Rechazado} con los parametros: Folio {FolioTomi}; BaseCMS {BaseCMS}; ");
                                
                                await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPEI_Rechazado, dicParametros = new Dictionary<string, object>()
                                {
                                    { "tablacms", BaseCMS },
                                    { "tablatomi", tablaTomi },
                                    { "razonrechazo", razonRechazo.Descripcion },
                                    { "codigomensaje", razonRechazo.Codigo },
                                    { "folio", FolioTomi }
                                });
                            }


                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|TOMI");
                            

                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_TomiFormato_SPEI_Rechazado, new Dictionary<string, object>()
                            {
                                { "strfolio", FolioTomi },
                                { "strtablatomi", tablaTomi },
                            });

                            if (ConstantesServicios.VarianteServicioFXD.Equals(TipoOperacion))
                            {
                                

                                await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                                {
                                    {"folio", FolioTomi },
                                    {"tipo_operacion", TipoOperacion },
                                    {"opcion" , 5 },
                                    {"desc_mensaje", razonRechazo.Descripcion}
                                });
                            } 
                            else
                            {

                                await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                                {
                                    {"folio", FolioTomi },
                                    {"tipo_operacion", TipoOperacion },
                                    {"opcion" , 6 }
                                });
                            }

                            break;

                        case ConstantesServicios.TomiEstatusCN:
                        case ConstantesServicios.TomiEstatusCL:
                        case ConstantesServicios.TomiEstatusRB:

                            razonesRechazo = await _IOperaciones.EjecutarSP<CatRazonRechazoDTO>(ProcedimientoAlmacenado.sp_Consultar_Cat_RazonRechazo, new Dictionary<string, object>()
                            {
                                { "strclave", datoTomi.cd_clave },
                                { "strtipopago", ConstantesServicios.EstatusSPEI },
                                { "strkarpay", datoTomi.nf_estatus_interno }
                            });
                            razonRechazo = razonesRechazo?.FirstOrDefault();

                            if (razonRechazo == null)
                                throw new Exception($"No se encontró la razón de rechazo SPEI para la clave {datoTomi.cd_clave}");

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|CMS - codigo_mensaje_CMS: {razonRechazo.Codigo}");

                            if (ConstantesServicios.VarianteServicioFXD.Equals(TipoOperacion))
                            {
                                ReversaPagoRespuestaModel reversar = await ReversarPagoT24(datoTomi.T24_transaction_id);

                                if (reversar.bCorrecto)
                                    Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|Reverso exitoso: {datoTomi.T24_transaction_id} |Folio: {FolioTomi}");
                                else
                                {
                                    Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|Error al reversar el pago: {datoTomi.T24_transaction_id} | error: {reversar.StrMensajeError}");

                                    

                                    await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                                    {
                                        {"folio", FolioTomi },
                                        {"tipo_operacion", TipoOperacion },
                                        {"opcion" , 7 }
                                    });

                                    return false;
                                }
                            }

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|Ejecuta el procedimiento {ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPEI_Rechazado} con los parametros: Folio {FolioTomi}; BaseCMS {BaseCMS};");

                           

                            await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPEI_Rechazado, new Dictionary<string, object>()
                            {
                                { "TablaCms", BaseCMS },
                                { "TablaTomi", tablaTomi },
                                { "RazonRechazo", razonRechazo.Descripcion },
                                { "CodigoMensaje", razonRechazo.Codigo },
                                { "Folio", FolioTomi }
                            });

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|TOMI");

                            

                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_TomiFormato_SPEI_Rechazado, new Dictionary<string, object>()
                            {
                                { "strFolio", FolioTomi },
                                { "strTablaTomi", tablaTomi },
                            });

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI|Terminando actualización de devolución de operación");

                            

                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                            {
                                {"Folio", FolioTomi },
                                {"Tipo_operacion", TipoOperacion },
                                {"Opcion" , 8 },
                                {"Desc_mensaje", datoTomi.nf_estatus_interno }
                            });
                            
                            break;

                        default:
                            throw new Exception($"Estatus Interno de SPEI deconocido, nf_estatus_interno = {datoTomi.nf_estatus_interno}");
                    }
                }
                else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                {
                    

                    await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPEI_Rechazado, new Dictionary<string, object>()
                    {
                        { "TablaCms", BaseCMS },
                        { "TablaTomi", tablaTomi },
                        { "RazonRechazo", razonRechazo?.Descripcion },
                        { "CodigoMensaje", razonRechazo?.Codigo },
                        { "Folio", FolioTomi }
                    });

                    ;

                    await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                    {
                        {"Folio", FolioTomi },
                        {"Tipo_operacion", TipoOperacion },
                        {"Opcion" , 8 }
                    });

                }

                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPEI| Se termina Procesamiento de Folio {FolioTomi}");

                return true;
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|Actualiza_CMS_Origen_SPEI|Se presenta Exception: {ex.ToString()} Trace: {ex.InnerException}");
                _IOperaciones.EnviaAlerta(new AlertaDTO()
                {
                    Folio = FolioTomi,
                    TipoOPeracion = TipoOperacion,
                    NombreProceso = TipoOperacion
                });
            }

            return false;
        }

        /// <summary>
        /// Valida el estatus de una operación BBVA y actualiza el estatus
        /// interno correspondiente en TOMI de acuerdo con la información
        /// obtenida desde BBVA.
        /// </summary>
        /// <param name="tablaTomi">
        /// Tabla TOMI donde se encuentra registrada la operación.
        /// </param>
        /// <param name="CampoTomi">
        /// Campo utilizado para localizar la operación.
        /// </param>
        /// <param name="FolioTomi">
        /// Folio de la operación a validar.
        /// </param>
        /// <param name="TipoOperacion">
        /// Tipo de operación que se encuentra en proceso.
        /// </param>
        /// <returns>
        /// True cuando la validación y actualización del estatus
        /// concluyen correctamente; en caso contrario, false.
        /// </returns>
        public async Task<bool> ValidaEstatus_BBVA(string tablaTomi, string CampoTomi,string FolioTomi, string TipoOperacion)
        {
            try
            {

                Config.EscribirLog($"INFO|Config.ValidaEstatus_BBVA|A revisar el estatus en BBVA del Folio {FolioTomi}");

                DatoBBVADTO datoBBVA = await _IOperaciones.EjecutarSPValidaEstatusBBVA(tablaTomi, CampoTomi, FolioTomi);

                if (datoBBVA == null)
                    throw new Exception("No se encontro el registro en la tabla TOMI");

                switch (datoBBVA.nf_estatus)
                {
                    case ConstantesServicios.TomiEstatusRECC:
                        Config.EscribirLog($"INFO|ValidaEstatus_BBVA|Operación CANCELADA, no buscará estatus en BBVA");

                        
                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinStatusInterno_BBVA, new Dictionary<string, object>()
                        {
                            {"TablaTomi", tablaTomi },
                            {"Folio", FolioTomi }
                        });

                        return true;
                    case ConstantesServicios.TomiEstatusCONF:
                    case ConstantesServicios.TomiEstatusCONP:


                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinStatusInterno_BBVA, new Dictionary<string, object>()
                        {
                            {"TablaTomi", tablaTomi },
                            {"Folio", FolioTomi },
                            {"Estatus", ConstantesServicios.TomiEstatusPROC}
                        });

                        break;
                }

                if (datoBBVA.nf_fecha_ejecucion.ToString() != _Configuracion.strFechayyyyMMdd)
                {
                    throw new Exception("La fecha de la operación es distinta a ladel sistema");
                }

                DatoPagoDTO pago = await _IOperaciones.BuscarEstatusPago(FolioTomi);

                switch (pago.Stage)
                {
                    case ConstantesServicios.TomiEstatusSUCC:

                        
                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinStatusInternoLQD_BBVA, new Dictionary<string, object>()
                        {
                            {"TablaTomi", tablaTomi },
                            {"Folio", FolioTomi },
                            {"Estatus", ConstantesServicios.TomiEstatusLQ }
                        });

                        return true;
                    case ConstantesServicios.TomiEstatusDECL:

                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinStatusInternoLQD_BBVA, new Dictionary<string, object>()
                        {
                            {"TablaTomi", tablaTomi },
                            {"Folio", FolioTomi },
                            {"Estatus", ConstantesServicios.TomiEstatusD }
                        });
                        
                        return true;
                    default:
                        break;
                }

                Config.EscribirLog($"INFO|ValidaEstatus_BBVA|Se termina Procesamiento de Folio");
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaEstatus_BBVA|Se presenta Exception: {ex.ToString()} Trace: {ex.InnerException}");

                _IOperaciones.EnviaAlerta(new AlertaDTO()
                {
                    Folio = FolioTomi,
                    TipoOPeracion = TipoOperacion,
                    NombreProceso = TipoOperacion
                });
            }

            return false;
        }

        /// <summary>
        /// Actualiza la información de CMS y TOMI para operaciones BBVA
        /// de acuerdo con el estatus final obtenido durante el procesamiento
        /// de la operación.
        /// </summary>
        /// <param name="tablaTomi">
        /// Tabla TOMI donde se encuentra registrada la operación.
        /// </param>
        /// <param name="FolioTomi">
        /// Folio de la operación a procesar.
        /// </param>
        /// <param name="TipoOperacion">
        /// Tipo de operación en proceso.
        /// </param>
        /// <param name="BaseCMS">
        /// Base o tabla CMS que será actualizada.
        /// </param>
        /// <returns>
        /// True cuando la actualización finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> Actualiza_CMS_origen_BBVA(string tablaTomi, string FolioTomi, string TipoOperacion,string BaseCMS)
        {
            try
            {

                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_BBVA|A revisar el estatus final en BBVA del folio {FolioTomi}");

                ErrorBBVADTO errorBBVA = (ErrorBBVADTO)await _IOperaciones.ConsultaErroresBBVA();


                List<DatoBBVADTO> datosBBVA = await _IOperaciones.EjecutarSP<DatoBBVADTO>(ProcedimientoAlmacenado.sp_Consulta_EstatusTombin_BBVA, new Dictionary<string, object>()
                {
                    { "TablaTomi", tablaTomi },
                    { "Folio", FolioTomi }
                });
                DatoBBVADTO datoBBVA = datosBBVA?.FirstOrDefault();

                if (datoBBVA == null)
                    throw new Exception("No se encontró el registro en TOMI");

                if (ConstantesServicios.TomiEstatusPROC.Equals(datoBBVA.nf_estatus))
                {
                    switch (datoBBVA.nf_estatus_interno)
                    {
                        case ConstantesServicios.TomiEstatusLQ:
                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_BBVA|CMS");


                            await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualiza_OrigenLQCms_BBVA, new Dictionary<string, object>
                            {   { "TablaCms", BaseCMS },
                                { "Folio", FolioTomi },
                                { "FechaPago", datoBBVA.nf_fecha_pago_bbva.ToString("yyyy-MM-dd") },
                                { "CieGuideBBVA", datoBBVA.nf_cieguide_bbva }
                            });

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_BBVA|Terminando aceptación de operación");


                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                            {
                                {"Folio", FolioTomi },
                                {"Tipo_operacion", TipoOperacion },
                                {"Opcion", 1 }
                            });

                            break;

                        case ConstantesServicios.TomiEstatusD:
                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_BBVA|CMS");


                            await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualiza_OrigenRecCms_BBVA, new Dictionary<string, object>
                            {   { "TablaCms", BaseCMS },
                                { "Folio", FolioTomi },
                                { "RazonRechazo", errorBBVA?.ErrorMessageGeneric },
                                { "IdRazon", errorBBVA?.IdError }
                            });

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_BBVA|Terminando actualizacion de devolución de operación");


                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualiza_OrigenRecTomi_BBVA, new Dictionary<string, object>
                            {   { "tablatomi", tablaTomi },
                                { "folio", FolioTomi },
                                { "strrazonrechazo", errorBBVA?.ErrorMessageGeneric }
                            });


                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                            {
                                {"Folio", FolioTomi },
                                {"Tipo_operacion", TipoOperacion },
                                {"Opcion", 2 }
                            });


                            break;

                        default:
                            throw new Exception($"Estatus Interno de BBVA desconocido, nf_estatus_interno = {datoBBVA.nf_estatus_interno}");

                    }
                }
                else if (ConstantesServicios.TomiEstatusRECC.Equals(datoBBVA.nf_estatus))
                {
                    Config.EscribirLog($"INFO|Actualiza_CMS_Origen_BBVA|CMS");

                    await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualiza_OrigenRecCms_BBVA, new Dictionary<string, object>
                    {   { "TablaCms", BaseCMS },
                        { "Folio", FolioTomi },
                        { "RazonRechazo", errorBBVA?.ErrorMessageGeneric },
                        { "IdRazon", errorBBVA?.IdError }
                    });


                    await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                    {
                        {"Folio", FolioTomi },
                        {"Tipo_operacion", TipoOperacion },
                        {"Opcion", 3 }
                    });

                    Config.EscribirLog($"INFO|Actualiza_CMS_Origen_BBVA|Terminando actualizacion de devolución de operación");
                    
                }

                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_BBVA|Se Termina Procesamiento de Folio: {FolioTomi}");

                return true;
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|Actualiza_CMS_Origen_BBVA|Se Presenta Exception {ex.ToString()}. Trace {ex.InnerException}");
            }

            return false;
        }

        /// <summary>
        /// Valida el estatus de una operación SPID y actualiza los estatus
        /// correspondientes en TOMI de acuerdo con la respuesta obtenida.
        /// </summary>
        /// <param name="tablaTomi">
        /// Tabla TOMI donde se encuentra registrada la operación.
        /// </param>
        /// <param name="CampoTomi">
        /// Campo utilizado para localizar la operación.
        /// </param>
        /// <param name="FolioTomi">
        /// Folio de la operación a validar.
        /// </param>
        /// <param name="TipoOperacion">
        /// Tipo de operación que se encuentra en proceso.
        /// <returns>
        /// True cuando la validación finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ValidaEstatus_SPID(string tablaTomi,string CampoTomi, string FolioTomi, string TipoOperacion)
        {
            try
            {
                string StrCadenaRegreso = string.Empty;

                Config.EscribirLog($"INFO|ValidaEstatus_SPID|A revisar el estatus en SPID del folio {FolioTomi}");


                List<DatoSPEIDTO> datosTomi = await _IOperaciones.EjecutarSP<DatoSPEIDTO>(ProcedimientoAlmacenado.sp_Consultar_Estatus_TbTombin_SPID, new Dictionary<string, object>() {
                    { "Folio", FolioTomi },
                    { "TablaTomi", tablaTomi },
                    { "CampoTomi", CampoTomi }
                });
                DatoSPEIDTO datoTomi = datosTomi?.FirstOrDefault();

                if (datoTomi == null)
                    throw new Exception("No se encontro el registro en la tabla TOMI");

                if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                {
                    Config.EscribirLog($"INFO|ValidaEstatus_SPID|Operacion CANCELADA, no bucara el estatus en SPID");

                    await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_EstatusInterno_SPID, new Dictionary<string, object>() {
                    { "Folio", FolioTomi },
                    { "TablaTomi", tablaTomi },
                    { "CampoTomi", CampoTomi }
                });

                    return true;
                }

                if (datoTomi.Nf_fecha_ejecucion.ToString() != _Configuracion.strFechayyyyMMdd)
                    throw new Exception("La fecha de la operación es distinta a la del sistema");


                List<SalidaStatusSPIDDTO> salidasSPID =  await _OperacionesSpid.EjecutaSPTransaccion<SalidaStatusSPIDDTO>("sp_InterfaceTOMI_SPID_Status2", new Dictionary<string, object>()
                {
                    { "op_fecha_oper", datoTomi.Nf_opfechaoper_spei },
                    { "op_cve_rastreo", datoTomi.T24_folio_asignado }
                });
                SalidaStatusSPIDDTO salidaSPID = salidasSPID?.FirstOrDefault();

                if (salidaSPID == null)
                    throw new Exception($"SPID no devolvió estatus para el folio {FolioTomi}");
                
                if (salidaSPID.Clave == null)
                    StrCadenaRegreso = $"{salidaSPID.op_estado}|{salidaSPID.op_monto}|";
                else
                    StrCadenaRegreso = $"{salidaSPID.op_estado}|{salidaSPID.op_monto}|{salidaSPID.Clave}";

                Config.EscribirLog($"INFO|ValidaEstatus_SPID|Cadena de regreso {StrCadenaRegreso}");

                if (string.IsNullOrEmpty(StrCadenaRegreso))
                   await ValidaEstatusAdicional_SPID(datoTomi.Nf_opfechaoper_spei.ToString(), datoTomi.T24_folio_asignado);

                switch (salidaSPID.op_estado)
                {
                    case ConstantesServicios.TomiEstatusLQ:

                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_EstatusInterno_SPID, new Dictionary<string, object>()
                        {
                            { "Folio", FolioTomi },
                            { "TablaTomi", tablaTomi },
                            { "CampoTomi", CampoTomi },
                            { "EstatusInterno", salidaSPID.op_estado }
                        });
                        break;

                    case ConstantesServicios.TomiEstatusD:

                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_EstatusInterno_SPID, new Dictionary<string, object>()
                        {
                            { "Folio", FolioTomi },
                            { "TablaTomi", tablaTomi },
                            { "CampoTomi", CampoTomi },
                            { "EstatusInterno", salidaSPID.op_estado },
                            { "CdClave", salidaSPID.Clave.ToString()  }
                        });
                        break;

                    case ConstantesServicios.TomiEstatusCL:
                    case ConstantesServicios.TomiEstatusRL:
                    case ConstantesServicios.TomiEstatusCN:
                    case ConstantesServicios.TomiEstatusRB:
                        
                        Config.EscribirLog($"INFO|ValidaEstatus_SPID|Estatus SPID: '{salidaSPID.op_estado}', EstatusTomi: {salidaSPID.op_estado}");

                        await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_EstatusInterno_SPID, new Dictionary<string, object>()
                        {
                            { "strFolio", FolioTomi },
                            { "strTablaTomi", tablaTomi },
                            { "strCampoTomi", CampoTomi },
                            { "strEstatusInterno", salidaSPID.op_estado }
                        });
                        break;

                    default:
                        await ValidaEstatusAdicional_SPID(datoTomi.Nf_opfechaoper_spei.ToString(), datoTomi.T24_folio_asignado);
                        break;
                }

                Config.EscribirLog($"INFO|ValidaEstatus_SPID|Se termina procesamiento de folio {FolioTomi}");
                return true;
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaEstatus_SPID|Se presenta Exception: {ex.ToString()} Trace: {ex.InnerException}");

                _IOperaciones.EnviaAlerta(new AlertaDTO()
                {
                    Folio = FolioTomi,
                    TipoOPeracion = TipoOperacion,
                    NombreProceso = TipoOperacion
                });
            }

            return false;
        }

        /// <summary>
        /// Actualiza la información de CMS y TOMI para operaciones SPID
        /// de acuerdo con el estatus final obtenido durante el procesamiento
        /// de la operación.
        /// </summary>
        /// <param name="tablaTomi">
        /// Tabla TOMI donde se encuentra registrada la operación.
        /// </param>
        /// <param name="FolioTomi">
        /// Folio de la operación a procesar.
        /// </param>
        /// <param name="TipoOperacion">
        /// Tipo de operación que se encuentra en proceso.
        /// </param>
        /// <param name="BaseCMS">
        /// Base o tabla CMS que será actualizada.
        /// </param>
        /// <returns>
        /// True cuando la actualización finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> Actualiza_CMS_origen_SPID(string tablaTomi, string FolioTomi, string TipoOperacion, string BaseCMS)
        {
            try
            {
                string strSPNombre = string.Empty;

                List<CatRazonRechazoDTO> razonesRechazo = null;
                CatRazonRechazoDTO razonRechazo = null;

                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|A revisar el estatus final en SPID del folio {FolioTomi}");

                if (tablaTomi.Contains(ConstantesServicios.VarianteServicioGPH))
                    strSPNombre = ProcedimientoAlmacenado.sp_Consultar_OrigenSPID_CMS_GPH;
                else
                    strSPNombre = ProcedimientoAlmacenado.sp_Consultar_OrigenSPID_CMS;


                List<DatoTomiNFDTO> datosTomi = await _IOperaciones.EjecutarSP<DatoTomiNFDTO>(strSPNombre, new Dictionary<string, object>()
                {
                    { "TablaTomi", tablaTomi},
                    { "Folio", FolioTomi }
                });
                DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                if (datoTomi == null)
                    throw new Exception("No se encontró el registroen TOMI");

                if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                {
                    switch(datoTomi.nf_estatus_interno)
                    {
                        case ConstantesServicios.TomiEstatusLQ:

                            await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Comunes_Actualizar_CmsFormato, new Dictionary<string, object>()
                            {
                                { "TablaCms", BaseCMS },
                                { "TablaTomi", tablaTomi},
                                { "HoraAutorizacion", datoTomi.Nf_fecha_autoriza.ToString("HH:mm:ss") },
                                { "Folio", FolioTomi },
                            });


                            switch (BaseCMS)
                            {
                                case ConstantesServicios.BaseCMSFormato:
                                    await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Insertar_T24Formato_SPID, new Dictionary<string, object> {
                                { "strFolio", FolioTomi },
                                { "sFt", datoTomi.T24_transaction_id },
                            });
                                    break;
                                case ConstantesServicios.BaseCMSFormatoPaPDetalle:
                                    await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Insertar_T24Formato_SPID_PaP, new Dictionary<string, object> {
                                { "strFolio", FolioTomi },
                                { "sFt", datoTomi.T24_transaction_id },
                            });
                                    break;
                            }

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Terminando actualización de aceptación de operación {FolioTomi}");
                            

                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                            {
                                {"Folio", FolioTomi },
                                {"Tipo_operacion", TipoOperacion },
                                {"Opcion", 10 }
                            });


                            break;

                        case ConstantesServicios.TomiEstatusD:


                            razonesRechazo = await _IOperaciones.EjecutarSP<CatRazonRechazoDTO>(ProcedimientoAlmacenado.sp_Consultar_Cat_RazonRechazo, new Dictionary<string, object>
                            {
                                { "strClave", datoTomi.cd_clave },
                                { "strTipoPago", ConstantesServicios.EstatusSPEI }
                            });
                            razonRechazo = razonesRechazo?.FirstOrDefault();

                            if (razonRechazo == null)
                                throw new Exception($"No se encontró la razón de rechazo SPID para la clave {datoTomi.cd_clave}");

                            if (ConstantesServicios.OperacionSPID.Equals(TipoOperacion))
                            {
                                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|CMS - codigo_mensaje_CMS {razonRechazo.Codigo }");


                                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Ejecuta el procedimiento {ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado} con los parametros: Folio {FolioTomi}; BaseCMS {BaseCMS}; CodigoMensaje {razonRechazo.Codigo}");
                                await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado, new Dictionary<string, object>()
                                {
                                    { "TablaCms", BaseCMS },
                                    { "TablaTomi", tablaTomi },
                                    { "CodigoMensaje",razonRechazo.Codigo },
                                    { "Folio", FolioTomi },
                                });
                            }
                            else
                            {
                                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Ejecuta el procedimiento {ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado} con los parametros: Folio {FolioTomi}; BaseCMS {BaseCMS}; ");

                                
                                await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado, new Dictionary<string, object>()
                                {
                                    { "TablaCms", BaseCMS },
                                    { "TablaTomi", tablaTomi },
                                    { "Folio", FolioTomi },
                                });
                            }

                            Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|TOMI ");


                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_Formato_SPID_Rechazado_TOMI, new Dictionary<string, object>()
                            {
                                { "Folio", FolioTomi },
                                { "TablaTomi", tablaTomi },
                            });

                            if (ConstantesServicios.OperacionSPID.Equals(TipoOperacion))
                            {

                                //CAG
                                dicParametros = new Dictionary<string, object>()
                                {
                                    {"Folio", FolioTomi },
                                    {"Tipo_operacion", TipoOperacion },
                                    {"Opcion" , 11 },
                                    {"Desc_mensaje", razonRechazo.Descripcion }
                                };

                            }
                            else
                            {
                                dicParametros = new Dictionary<string, object>()
                                {
                                    {"Folio", FolioTomi },
                                    {"Tipo_operacion", TipoOperacion },
                                    {"Opcion", 12 }
                                };
                            }

                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, dicParametros);
                            break;

                        case ConstantesServicios.TomiEstatusCN:
                        case ConstantesServicios.TomiEstatusCL:
                        case ConstantesServicios.TomiEstatusRB:


                            razonesRechazo = await _IOperaciones.EjecutarSP<CatRazonRechazoDTO>(ProcedimientoAlmacenado.sp_Consultar_Cat_RazonRechazo, new Dictionary<string, object>()
                            {
                                { "Clave", datoTomi.cd_clave },
                                { "TipoPago", ConstantesServicios.EstatusSPEI },
                                { "KarPay", datoTomi.nf_estatus_interno }
                            });
                            razonRechazo = razonesRechazo?.FirstOrDefault();

                            Config.EscribirLog($"INFO||Actualiza_CMS_Origen_SPID|CMS - codigo_mensaje_CMS: {razonRechazo?.Codigo}");
                            Config.EscribirLog($"INFO||Actualiza_CMS_Origen_SPID|CMS ");

                            if (ConstantesServicios.OperacionSPID.Equals(TipoOperacion))
                            {
                                ReversaPagoRespuestaModel reversar = await ReversarPagoT24(datoTomi.T24_transaction_id);

                                if (reversar.bCorrecto)
                                    Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Error al reversar el pago: {datoTomi.T24_transaction_id} |Folio: {FolioTomi}");
                                else
                                {
                                    Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Error al reversar el pago: {datoTomi.T24_transaction_id} | error: {reversar.StrMensajeError}");
                                    

                                    await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                                    {
                                        {"Folio", FolioTomi },
                                        {"Tipo_operacion", TipoOperacion },
                                        {"Opcion", 13 }
                                    });

                                    return false;
                                }


                                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Ejecutando el Procedimiento: {ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado} ");
                                
                                await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado, new Dictionary<string, object>()
                                {
                                    { "strFolio", FolioTomi },
                                    { "strTablaCms", BaseCMS},
                                    { "strCodigoMensaje", razonRechazo?.Codigo }
                                });
                            }
                            else
                            {

                                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Ejecutando el Procedimiento: {ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado} ");

                                await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado, new Dictionary<string, object>()
                                {
                                    { "strFolio", FolioTomi },
                                    { "strTablaCms", BaseCMS}
                                });
                            }

                            await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_Formato_SPID_Rechazado_TOMI, new Dictionary<string, object>()
                            {
                                { "Folio", FolioTomi },
                                { "TablaTomi", tablaTomi}
                            });
                            break;

                        default:
                            throw new Exception($"Estatus de interno de SPID desconocido nf_estatus_interno={datoTomi.nf_estatus_interno}");
                    }
                }

                if(ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                {
                    Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Ejecutando el Procedimiento: {ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado} ");
                    

                    await _OperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado, new Dictionary<string, object>()
                    {
                        { "TablaCms", BaseCMS },
                        { "TablaTomi", tablaTomi },
                        { "RazonRechazo", razonRechazo?.Descripcion  },
                        { "CodigoMensaje", razonRechazo?.Codigo  },
                        { "Folio", FolioTomi },
                    });


                    await _IOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, new Dictionary<string, object>()
                    {
                        {"Folio", FolioTomi },
                        {"Tipo_operacion", TipoOperacion },
                        {"Opcion", 13 }
                    });
                }

                Config.EscribirLog($"INFO|Actualiza_CMS_Origen_SPID|Se termina Procesamiento de folio: {FolioTomi}");

                return true;
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|Actualiza_CMS_Origen_SPID|Se presenta Exception: {ex.ToString()} Trace: {ex.InnerException}");

                _IOperaciones.EnviaAlerta(new AlertaDTO()
                {
                    Folio = FolioTomi,
                    TipoOPeracion = TipoOperacion,
                    NombreProceso = TipoOperacion
                });
            }

            return false;
        }

        /// <summary>
        /// Obtiene el estatus SWIFT asociado a una operación mediante
        /// el folio T24 proporcionado.
        /// </summary>
        /// <param name="FolioT24">
        /// Folio T24 de la operación a consultar.
        /// </param>
        /// <returns>
        /// Estatus SWIFT de la operación; retorna una cadena vacía
        /// cuando no se encuentra información.
        /// </returns>
        public async Task<string> Dame_Estatus_Swift(string FolioT24)
        {
            try
            {
                dicParametros = new Dictionary<string, object>() { {"folioT24", FolioT24 } };
                List<SwiftDTO> _Swift = await _IOperaciones.EjecutarSP<SwiftDTO>(ProcedimientoAlmacenado.sp_Consultar_EstatusSWIFT, dicParametros);
                SwiftDTO Switf = _Swift?.FirstOrDefault();

                return Switf?.cve_estatus ?? string.Empty;
            } catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Dame_Estatus_Swift|{ex}");
                return string.Empty;
            }
        }

        /// <summary>
        /// Obtiene el folio maestro asociado a una operación de acuerdo con
        /// el folio y tipo de operación proporcionados.
        /// </summary>
        /// <param name="Folio">
        /// Folio de la operación a consultar.
        /// </param>
        /// <param name="TipoOperacion">
        /// Tipo de operación asociado al folio.
        /// </param>
        /// <returns>
        /// Folio maestro encontrado para la operación;
        /// en caso de no existir información retorna una cadena vacía.
        /// </returns>
        public async Task<string> DameFolioMaestro(string Folio, string TipoOperacion) {
            try
            {
                FolioMaerstroModelo _master = await _IOperaciones.ObtenerFolioMestro(Folio, TipoOperacion);

                return (_master != null && !string.IsNullOrEmpty(_master.folio_maestro))
                    ? _master.folio_maestro
                    : string.Empty;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|DameFolioMaestro|{ex}");
                return string.Empty;
            }
            
        }

        /// <summary>
        /// Envía la respuesta de una operación TEF de salida y valida
        /// el resultado obtenido a partir de la cadena de respuesta
        /// generada por el proceso.
        /// </summary>
        /// <param name="IdOperacion">
        /// Identificador de la operación.
        /// </param>
        /// <param name="FechaProceso">
        /// Fecha de proceso de la operación.
        /// </param>
        /// <param name="ClaveRastreo">
        /// Clave de rastreo asociada a la operación.
        /// </param>
        /// <param name="Monto">
        /// Monto de la operación.
        /// </param>
        /// <param name="ClaveOperacion">
        /// Estatus o clave de operación a registrar.
        /// </param>
        /// <param name="CausaDevolucion">
        /// Código de la causa de devolución.
        /// </param>
        /// <returns>
        /// True cuando la operación se procesa correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> Responde_Salida_TEF(int IdOperacion, int FechaProceso,string ClaveRastreo, decimal Monto, string ClaveOperacion, int CausaDevolucion)
        {
            try
            {
                SalidaTEFModel salidaTEF = new SalidaTEFModel()
                {
                    iOperacionId = IdOperacion,
                    iFechaProceso = FechaProceso,
                    sClaveRastreo = ClaveRastreo,
                    dMonto = Monto,
                    sCausaDevolucion = CausaDevolucion.ToString(),
                    sClaveOperacion = ClaveOperacion
                };

                Config.EscribirLog(string.Format("INFO|ENT_TEF.responde_entrada_TEF|Pasaron los parametros. opClave={0},opFechaOper={1},opMonto={2},cveRastreo={3},cd_clave={4},op_estatus={5} ", salidaTEF.iOperacionId, salidaTEF.iFechaProceso, salidaTEF.dMonto, salidaTEF.sClaveRastreo, salidaTEF.sCausaDevolucion, salidaTEF.sClaveOperacion));

                SalidaTefDTO respuesta = await _IOperaciones.Responde_Salida_Tef(salidaTEF);

                Config.EscribirLog(string.Format("INFO|ENT_TEF.responde_entrada_TEF|Cadena_Regreso{0}", respuesta?.StrCadenaRegreso));

                string[] valores = respuesta?.StrCadenaRegreso?.Split(ConstantesServicios.SeparadorPipe);
                if (valores == null || valores.Length == 0)
                    return false;

                int.TryParse(valores[0], out int resultado);
                return resultado == 0; //true or false
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Responde_Salida_TEF|{ex}");
                return false;
            }
        }
    
        /// <summary>
        /// Actualiza el estatus de una operación SPID recibida y valida
        /// el resultado obtenido del procedimiento correspondiente.
        /// </summary>
        /// <param name="ClaveOperacion">
        /// Clave o estatus de la operación a registrar.
        /// </param>
        /// <param name="CausaDevolucion">
        /// Código de la causa de devolución asociado a la operación.
        /// </param>
        /// <param name="Op_Clave">
        /// Clave de la operación SPID.
        /// </param>
        /// <param name="Op_fecha_oper">
        /// Fecha de operación.
        /// </param>
        /// <param name="Op_folio">
        /// Folio de la operación.
        /// </param>
        /// <param name="Op_Cve_Reastreo">
        /// Clave de rastreo de la operación.
        /// </param>
        /// <param name="Op_monto">
        /// Monto asociado a la operación.
        /// </param>
        /// <returns>
        /// True cuando la actualización del estatus es exitosa;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> Responde_Entrada_SPID(string ClaveOperacion,int? CausaDevolucion, long Op_Clave, int Op_fecha_oper,int Op_folio,string Op_Cve_Reastreo,decimal Op_monto)
        {
            try
            {
                dicParametros = new Dictionary<string, object>()
                {
                    {"op_clave", Op_Clave },
                    {"op_fecha_oper", Op_fecha_oper },
                    {"op_folio", Op_folio },
                    {"op_cve_rastreo", Op_Cve_Reastreo ?? string.Empty },
                    {"op_monto", Op_monto },
                    {"cd_clave", CausaDevolucion ?? 0 },
                    {"op_estatus", ClaveOperacion }
                };

                EntradaSPIDModel entradaSPID = await _OperacionesSpid.SpidEstatusRecibidas(ProcedimientoAlmacenado.sp_InterfaceTOMI_SPID_Cambia_Estatus_Recibidas, dicParametros);

                return (entradaSPID?.IntClave != null && entradaSPID.IntClave.Value == 0); //true or false
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Responde_Entrada_SPID|{ex}");
                return false;
            }
        }

        /// <summary>
        /// Actualiza el estatus de una operación SPEI recibida y valida
        /// el resultado obtenido del procedimiento correspondiente.
        /// </summary>
        /// <param name="ClaveOperacion">
        /// Clave o estatus de la operación a registrar.
        /// </param>
        /// <param name="CausaDevolucion">
        /// Código de la causa de devolución asociado a la operación.
        /// </param>
        /// <param name="Op_Clave">
        /// Clave de la operación SPEI.
        /// </param>
        /// <param name="Op_fecha_oper">
        /// Fecha de operación.
        /// </param>
        /// <param name="Op_folio">
        /// Folio de la operación.
        /// </param>
        /// <param name="Op_Cve_Reastreo">
        /// Clave de rastreo de la operación.
        /// </param>
        /// <param name="Op_monto">
        /// Monto asociado a la operación.
        /// </param>
        /// <returns>
        /// True cuando la actualización del estatus es exitosa;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> Responde_Entrada_SPEI(string ClaveOperacion, int? CausaDevolucion, int Op_Clave, int Op_fecha_oper, int Op_folio, string Op_Cve_Reastreo, decimal Op_monto)
        {
            try
            {

                dicParametros = new Dictionary<string, object>()
                {
                    {"op_clave", Op_Clave.ToString() },
                    {"op_fecha_oper", Op_fecha_oper.ToString() },
                    {"op_folio", Op_folio.ToString() },
                    {"op_cve_restreo", Op_Cve_Reastreo },
                    {"op_monto", Op_monto.ToString() },
                    {"cd_clave", CausaDevolucion?.ToString() ?? string.Empty },
                    {"op_estatus", ClaveOperacion }

                };

                List<EntradaSPIDModel> entradasSPID = await _IOperaciones.EjecutarSP<EntradaSPIDModel>(ProcedimientoAlmacenado.sp_InterfaceTOMI_SPEI_Cambia_Estatus_Recibidas, dicParametros);
                EntradaSPIDModel entradaSPID = entradasSPID?.FirstOrDefault();

                return (entradaSPID?.IntClave != null && entradaSPID.IntClave.Value == 0); //true or false
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Responde_Entrada_SPEI|{ex}");
                return false;
            }
        }

        /// <summary>
        /// Ejecuta el reverso de una operación T24 mediante el servicio
        /// correspondiente y devuelve el resultado del proceso.
        /// </summary>
        /// <param name="FT">
        /// Identificador FT de la operación a reversar.
        /// </param>
        /// <returns>
        /// Resultado de la ejecución del reverso, incluyendo el estatus
        /// de éxito y el mensaje de error cuando corresponda.
        /// </returns>
        public async Task<ReversaPagoRespuestaModel> ReversarPagoT24(string FT)
        {

            string endpointWebService = ObtenerEndpointWebService(); var wsReverseFTRef = new wsReverseFT.T24WebServicesImplClient(

            new BasicHttpBinding(),
            new EndpointAddress(endpointWebService));

            ReversaPagoRespuestaModel respuesta = new ReversaPagoRespuestaModel();

            wsReverseFT.APLICAREVERSOFTRequest req =
                new wsReverseFT.APLICAREVERSOFTRequest();

            wsReverseFT.webRequestCommon wrc =
                new wsReverseFT.webRequestCommon
                {
                    company = _Configuracion.StrCompanyName,
                    password = _Configuracion.StrPasswdReg,
                    userName = _Configuracion.StrUserNameReg
                };

            wsReverseFT.enquiryInputCollection eiC =
                new wsReverseFT.enquiryInputCollection
                {
                    columnName = "@ID",
                    criteriaValue = FT,
                    operand = "EQ"
                };

            wsReverseFT.enquiryInputCollection[] eiC_Array = new wsReverseFT.enquiryInputCollection[1] 
            { 
                eiC
            };


            req.WebRequestCommon = wrc;
            req.MUFGEFTREVRECORDType = eiC_Array;

            wsReverseFT.APLICAREVERSOFTResponse response;

            try
            {
                response = await wsReverseFTRef.APLICAREVERSOFTAsync(req);
                 
                string messageGeneralError = string.Empty;

                if (response?.Status?.messages != null &&
                    response.Status.messages.Length > 0)
                {
                    for (short cnt = 0; cnt < response.Status.messages.Length; cnt++)
                    {
                        messageGeneralError += response.Status.messages[cnt];

                        if (cnt != response.Status.messages.Length - 1)
                        {
                            messageGeneralError += "|";
                        }
                    }
                }

                respuesta.StrMensajeError = messageGeneralError;
                respuesta.bCorrecto = false;

                if (response?.Status?.successIndicator != wsReverseFT.successIndicator.Success || messageGeneralError.Contains("No records were found that matched the selection criteria"))
                {
                    Config.EscribirLog($"INFO|ReversarPagoT24|El registro del reverso de la operación {FT} marcó un error :  {messageGeneralError} El status regresado fue {response?.Status?.successIndicator.ToString()} ");
                    return respuesta;
                }

                Config.EscribirLog($"INFO|ReversarPagoT24|El registro del reverso de la operación :  {FT} fue exitoso.");
                respuesta.StrMensajeError = "";
                respuesta.bCorrecto = true;
                return respuesta;
                
                
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ReversarPagoT24|El registro del reverso de la operación :  {FT} marcó un error : {ex.ToString()}");

                respuesta.StrMensajeError = ex.ToString();
                respuesta.bCorrecto = false;
                return respuesta;
            }

        }

        /// <summary>
        /// Realiza una validación adicional del estatus de una operación SPID
        /// mediante la consulta de información complementaria en el sistema.
        /// </summary>
        /// <param name="FechaOperSPID">
        /// Fecha de operación de la transacción SPID.
        /// </param>
        /// <param name="FolioReferencia">
        /// Folio o clave de rastreo asociado a la operación.
        /// </param>
        /// <returns>
        /// Cadena de respuesta obtenida de la validación adicional;
        /// retorna una cadena vacía cuando ocurre un error o no existe información.
        /// </returns>
        public async Task<string> ValidaEstatusAdicional_SPID(string FechaOperSPID, string FolioReferencia)
        {
            try
            {
                string cadenaRegreso2 = string.Empty;

                dicParametros = new Dictionary<string, object>()
                {
                    {"op_fecha_oper", FechaOperSPID.ToString() },
                    {"op_cve_rastreo", FolioReferencia },

                };

                List<ValidaSPIDStatus3Modelo> validarSPID = await _OperacionesSpid.EjecutaSPTransaccion<ValidaSPIDStatus3Modelo>("sp_InterfaceTOMI_SPID_Status3", dicParametros);
                ValidaSPIDStatus3Modelo validaSPID = validarSPID?.FirstOrDefault();

                if (validaSPID != null)
                {
                    cadenaRegreso2 = $"{validaSPID.op_estado_proceso}|{validaSPID.op_descripcion_error}";
                }

                if (!string.IsNullOrEmpty(cadenaRegreso2))
                {
                    string[] resultado = cadenaRegreso2.Split('|');
                    if (resultado[0] != ConstantesServicios.ResultadoExitoso) throw new Exception($"Se presenta un error al registrar la operación: {resultado[1]}");
                }
                return cadenaRegreso2;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaEstatusAdicional_SPID|{ex}");
                return string.Empty;
            }
        }

        /// <summary>
        /// Envía la respuesta de una operación TEF de entrada y valida
        /// el resultado obtenido del proceso correspondiente.
        /// </summary>
        /// <param name="IdOperacion">
        /// Identificador de la operación.
        /// </param>
        /// <param name="FechaProceso">
        /// Fecha de proceso de la operación.
        /// </param>
        /// <param name="CausaDevolucion">
        /// Código de la causa de devolución.
        /// </param>
        /// <param name="ClaveOperacion">
        /// Clave o estatus de la operación.
        /// </param>
        /// <returns>
        /// True cuando la actualización del estatus se procesa correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> Responde_Entrada_TEF(int IdOperacion, int FechaProceso, int CausaDevolucion, string ClaveOperacion)
        {
            try
            {

                SalidaTefDTO respuesta = await _OperacionesTEF.Responde_Entrada_Tef(IdOperacion, FechaProceso, CausaDevolucion, ClaveOperacion);

                if (respuesta == null) return false;
                
                respuesta.StrCadenaRegreso = $"{respuesta.op_error}|{respuesta.id_master}";
                string[] valores = respuesta.StrCadenaRegreso.Split(ConstantesServicios.SeparadorPipe);
                return (valores.Length > 0 && int.TryParse(valores[0], out int resultado) && resultado == 0); //true or flase

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Responde_Entrada_TEF|{ex}");
                return false;
            }
        }

        /// <summary>
        /// se oobtiene la informacion de la uri retorno dev.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private string ObtenerEndpointWebService() => _Configuracion?.UriReversa != null ? _Configuracion.UriReversa : throw new Exception($"No se encontró el parámetro obligatorio '{ParametrosSistema.WS_REVERSA_ENDPOINT}' para el web service Actualiza Estatus.");
    }
}

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
        public new string NombreServicio { get; set; } = null;
        public string StrNombreSPEI1 { get; set; } = null;
        public string StrNombreSPEI2 { get; set; } = null;
        public new bool? TimerEnabled { get; set; } = null;
        public new double? TimerInterval { get; set; } = null;
        public bool? BenHorario { get; set; } = false;

        public string strFechayyyyMMdd { get; set; }

        public new object RutaArchivoConfiguracion { get; set; }

        public new string TamanoMaximoArchivoLogKB { get; set; } = null;
        public string StrCompanyName { get; set; } = null;
        public string StrPasswdReg { get; internal set; }
        public string StrUserNameReg { get; set; }
        public new string PathLog { get; set; }
        public string configracion { get; set; }

        public string UriReversa { get; set; }

        public new ConfiguracionServicio Clone()
        {
            return (ConfiguracionServicio)MemberwiseClone();
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Servicios;
using MUFG_T24.Modelos;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    public delegate Task<bool> ProcesadorOperacion(string tipoOperacion, OperacionDTO operacion);

    /// <summary>
    /// Interfaces Operacion Servicio.
    /// </summary>
    public interface IOperacionServicio
    {
        Task<RespuestaResumenDto> EjecutaProcedimientosAsync(string tipoOperacion = null, string[] folios = null);
        Task<string> ObtenerPuertoEscucha();
    }

    /// <summary>
    /// Contrato para crear y ejecutar servicios según el tipo de operación.
    /// </summary>
    public interface IFabricaServiciosOperacion
    {
        Task<bool> ProcesarAsync(string tipoOperacion, OperacionDTO operacion);
    }

    public interface IFabricaServicioGeneral
    {
        ServicioGeneral Crear();
    }
}

using System.Collections.Generic;
using System.Threading.Tasks;
using Aplicacion.DTOs;
using Aplicacion.Modelos;
using MUFG_T24.Modelos;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Parametros Operaciones.
    /// </summary>
    public interface IParametrosOperaciones
    {
        Task<string> metodoBDAsync();

        Task<Dictionary<string, string>> parametrosSistema();
        Task MarcarServicioEnEjecucion(string strNombreServicio);
        Task MarcarServicioEnFinalizacion(string strNombreServcicio);
        void EnviaAlerta(AlertaDTO alerta);

        Task<string> DameUltimoProceso(OperacionDTO operacion);

        Task IniciarProceso(ProcesoDTO operacion);

        Task TerminarProceso(ProcesoDTO terminaProceso);

        Task<DatoBBVADTO> EjecutarSPValidaEstatusBBVA(string tablaTomi, string CampoTablaTomi, string Folio);
        Task<DatoSPEIDTO> EjecutarSPValidaEstatusSPEI(string tablaTomi, string CampoTablaTomi, string Folio);
        Task<DatoSPEIDTO> EjecutarSPValidaEstatusAdicionalSPEI(int FechaOperacionSPEI, string FolioReferencia);

        Task<SalidaEstatusSPEIDTO> EjecutarSPSPEIStatus(int Op_Fecha_Oper, string Op_Cve_Rastreo);
        Task<SalidaTefDTO> Responde_Salida_Tef(SalidaTEFModel salidaTEF);
        Task<SalidaTefDTO> Responde_Entrada_Tef(int IdOperacion, int FechaProceso,int CausaDevolucion, string ClaveOperacion);

        Task<DatoPagoDTO> BuscarEstatusPago(string Folio);

        Task<datoENT_TEFDTO> EjecutaSPTefRecibidad(int Op_Fecha_Oper);

        Task<DatoSPEIDTO> EjecutaSPSPSPEI_Estatus_Recibidas(int OpFechaOperacion);

        Task<ErrorBBVADTO> ConsultaErroresBBVA();

        Task<List<T>> EjecutarSP<T>(string SPNombre, Dictionary<string, object> Parametros) where T : new();
        Task<bool> EjecutarSP(string SPNombre, Dictionary<string, object> Parametros);
        Task<FolioMaerstroModelo> ObtenerFolioMestro(string folio, string tipoOperacion);
        Task<string> obtenerPuertoEscucha();
    }
}

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Operaciones Cms.
    /// </summary>
    public interface IOperacionesCms
    {
        Task<object> EjecutaSPTransaccion(string SPNombre, Dictionary<string, object> Parametros);
    }
}

using Aplicacion.DTOs;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Operaciones Spei.
    /// </summary>
    public interface IOperacionesSpei
    {
        Task<object> EjecutaSPTransaccion(string SPNombre, Dictionary<string, object> Parametros);
        Task<SalidaEstatusSPEIDTO> EjecutarSPSPEIStatus(int Op_Fecha_Oper, string Op_Cve_Rastreo);
        Task<DatoSPEIDTO> EjecutarSPValidaEstatusAdicionalSPEI(int FechaOperacionSPEI, string FolioReferencia);
        Task<List<DatoSPEIDTO>> EjecutaSPSPSPEI_Estatus_Recibidas(int OpFechaOperacion);
    }
}

using Aplicacion.DTOs;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaces Operaciones Spei2.
    /// </summary>
    public interface IOperacionesSpei2
    {
        Task<List<DatoSPEIDTO>> EjecutaSPSPSPEI_Estatus_Recibidas(int OpFechaOperacion);
    }
}

using Aplicacion.DTOs;
using Aplicacion.Modelos;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaces Operaciones Spid.
    /// </summary>
    public interface IOperacionesSpid
    {
        Task<List<T>> EjecutaSPTransaccion<T>(string SPNombre, Dictionary<string, object> Parametros) where T : new();
        Task<List<DatoENT_SPIDDTO>> EjecutaSPSPID_Estatus_Recibidas(int OpFechaOperacion);
        Task<EntradaSPIDModel> SpidEstatusRecibidas(string SPNombre, Dictionary<string, object> Parametros);
    }
}

using Aplicacion.DTOs;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaces Operaciones Tef.
    /// </summary>
    public interface IOperacionesTef
    {
        Task<datoENT_TEFDTO> EjecutaSPTefRecibidad(int Op_Fecha_Oper);
        Task<SalidaTefDTO> Responde_Entrada_Tef(int IdOperacion, int FechaProceso, int CausaDevolucion, string ClaveOperacion);
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Conexiones;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase principal de ConsultaBDTomi.
    /// </summary>
    public class ConsultasBDTomi : RepositorioMaestro, IParametrosOperaciones
    {
        private readonly IAlertas alerta;

        /// <summary>
        /// Constructor 
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBaseDatos"></param>
        public ConsultasBDTomi(IAlertas alertaT)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            alerta = alertaT;
        }

        /// <summary>
        /// Obtención de la fecha desde la base de datos
        /// </summary>
        /// <returns></returns>
        public async Task<string> metodoBDAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<SistemaFechaDto>(ProcedimientoAlmacenado.sp_Fecha_Sistema);
            return resultado?.Data?.FirstOrDefault()?.Fecha?.ToString();

        }

        /// <summary>
        /// Envia alerta.
        /// </summary>
        /// <param name="alertaDto"></param>
        public void EnviaAlerta(AlertaDTO alertaDto) => alerta.InsertarAlerta(alertaDto);

        /// <summary>
        /// Marca el servicio en ejecucion.
        /// </summary>
        /// <param name="strNombreServicio"></param>
        /// <returns></returns>
        public async Task MarcarServicioEnEjecucion(string strNombreServicio) => await ActualizaEJecucion(strNombreServicio);
        
        /// <summary>
        /// Marva el servicio en finalizacion.
        /// </summary>
        /// <param name="strNombreServcicio"></param>
        /// <returns></returns>
        public async Task MarcarServicioEnFinalizacion(string strNombreServcicio) => await ActualizaTerminacion(strNombreServcicio);

        /// <summary>
        /// Obtiene los parametros del sistema.
        /// </summary>
        /// <returns></returns>
        public async Task<Dictionary<string, string>> parametrosSistema()
        {
            string parametrosSistema = $"'{ParametrosSistema.HORA_LECTURA_INICIO}','{ParametrosSistema.HORA_LECTURA_FIN}','{ParametrosSistema.SPEI_SERVIDOR_1}','{ParametrosSistema.SPEI_SERVIDOR_2}','{ParametrosSistema.WST24_MW_COMPANY}','{ParametrosSistema.WST24_MW_USERNAME_REG}','{ParametrosSistema.WST24_MW_PASS_REG}','{ParametrosSistema.WS_REVERSA_ENDPOINT}'";
            var result = await EjecutarStoredProcedureQuery<ParametrosModelo>(ProcedimientoAlmacenado.sp_Parametros_Sistema, cmd =>
            {
                IDbDataParameter param = cmd.CreateParameter();
                param.ParameterName = "@strParametros";
                param.Value = parametrosSistema;
                cmd.Parameters.Add(param);
            });

            if (result.Data == null) return new Dictionary<string, string>();

            return result.Data
            .Where(x => x != null && !string.IsNullOrWhiteSpace(x.Id_parametro))
            .GroupBy(x => x.Id_parametro)
            .ToDictionary(
                x => x.Key,
                x => x.LastOrDefault()?.Valor ?? string.Empty);
        }

        /// <summary>
        /// Obtiene el ultimo proceso de la operación
        /// </summary>
        /// <param name="operacion">Los datos de la operación a consultar</param>
        /// <returns></returns>
        public async Task<string> DameUltimoProceso(OperacionDTO operacion)
        {
            
            //SP a Ejecutar sp_UltimoProcesoFolio
            var result = await EjecutarStoredProcedureQuery<AliasProcesoModelo>(ProcedimientoAlmacenado.sp_UltimoProcesoFolio, cmd =>
            {
                IDbDataParameter param = cmd.CreateParameter();
                param.ParameterName = "@TipoOperacion";
                param.Value = operacion.StrTipoOperacion;
                cmd.Parameters.Add(param);

                param = cmd.CreateParameter();
                param.ParameterName = "@Folio";
                param.Value = operacion.StrFolio;
                cmd.Parameters.Add(param);

                param = cmd.CreateParameter();
                param.ParameterName = "@Servicio";
                param.Value = operacion.StrServicio;
                cmd.Parameters.Add(param);
            });

            return result?.Data?
            .Select(r => r?.Alias_proceso)
            .FirstOrDefault() ?? string.Empty;
        }

        /// <summary>
        /// Inicia el proceso.
        /// </summary>
        /// <param name="operacion"></param>
        /// <returns></returns>
        public async Task IniciarProceso(ProcesoDTO operacion) => await IniciaProceso(operacion);

        /// <summary>
        /// Termina el proceso.
        /// </summary>
        /// <param name="operacion"></param>
        /// <returns></returns>
        public async Task TerminarProceso(ProcesoDTO operacion) => await TerminaProceso(operacion);

        /// <summary>
        /// Valida el estatus de BBVA.
        /// </summary>
        /// <param name="tablaTomi"></param>
        /// <param name="CampoTablaTomi"></param>
        /// <param name="Folio"></param>
        /// <returns></returns>
        public async Task<DatoBBVADTO> EjecutarSPValidaEstatusBBVA(string tablaTomi, string CampoTablaTomi, string Folio)
        {
            //DatoBBVADTO
            //sp_Consulta_Estatus_BBVA
            try
            {
                //SP a Ejecutar sp_TerminaProceso
                var result = await EjecutarStoredProcedureQuery<DatoBBVADTO>(ProcedimientoAlmacenado.sp_Consulta_Estatus_BBVA, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@TablaTomi";
                    param.Value = tablaTomi;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@CampoTomi";
                    param.Value = CampoTablaTomi;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = Folio;
                    cmd.Parameters.Add(param);

                });

                return result?.Data?.FirstOrDefault() ?? new DatoBBVADTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSPValidaEstatusBBVA|{ex}");
                return new DatoBBVADTO();
            }
        }

        /// <summary>
        /// Valida Estatus SPEI.
        /// </summary>
        /// <param name="tablaTomi"></param>
        /// <param name="CampoTablaTomi"></param>
        /// <param name="Folio"></param>
        /// <returns></returns>
        public async Task<DatoSPEIDTO> EjecutarSPValidaEstatusSPEI(string tablaTomi, string CampoTablaTomi, string Folio)
        {
            //SP: sp_ConsultaEstatusSPEI
            try
            {
                var result = await EjecutarStoredProcedureQuery<DatoSPEIDTO>(ProcedimientoAlmacenado.sp_ConsultaEstatusSPEI, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@TablaTomi";
                    param.Value = tablaTomi;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@CampoTomi";
                    param.Value = CampoTablaTomi;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@FolioTomi";
                    param.Value = Folio;
                    cmd.Parameters.Add(param);
                });


                return result?.Data?.FirstOrDefault() ?? new DatoSPEIDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSPValidaEstatusSPEI|{ex}");
                return new DatoSPEIDTO();
            }
        }

        /// <summary>
        /// Valida el estatus adicional de SPEI.
        /// </summary>
        /// <param name="FechaOperacionSPEI"></param>
        /// <param name="FolioReferencia"></param>
        /// <returns></returns>
        public async Task<DatoSPEIDTO> EjecutarSPValidaEstatusAdicionalSPEI(int FechaOperacionSPEI, string FolioReferencia)
        {
            try
            {
                using (var ConexionSPEI = (IDisposable)ConexionBasedatosFactory.CrearConexion(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI))
                {
                    var db = (IBasedatosConexion)ConexionSPEI;
                    var result = await EjecutarStoredProcedureQuery<DatoSPEIDTO>(ProcedimientoAlmacenado.sp_InterfacesTOMI_SPEI_Status3, cmd =>
                    {
                        cmd.Parameters.Add(db.CrearParametro("@op_fecha_oper", FechaOperacionSPEI));
                        cmd.Parameters.Add(db.CrearParametro("@op_cve_rastreo", FolioReferencia));
                    });

                    return result?.Data?.FirstOrDefault() ?? new DatoSPEIDTO();
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSPValidaEstatusAdicionalSPEI|{ex}");
                return new DatoSPEIDTO();
            } 
        }

        /// <summary>
        /// Ejecuta el Spei estatus.
        /// </summary>
        /// <param name="Op_Fecha_Oper"></param>
        /// <param name="Op_Cve_Rastreo"></param>
        /// <returns></returns>
        public async Task<SalidaEstatusSPEIDTO> EjecutarSPSPEIStatus(int Op_Fecha_Oper, string Op_Cve_Rastreo)
        {
            //SP: sp_InterfaceTOMI_SPEI_Status2
            try
            {
                using (var ConexionSPEI = (IDisposable)ConexionBasedatosFactory.CrearConexion(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI))
                {
                    var db = (IBasedatosConexion)ConexionSPEI;
                    var result = await EjecutarStoredProcedureQuery<SalidaEstatusSPEIDTO>(ProcedimientoAlmacenado.sp_InterfaceTOMI_SPEI_Status2, cmd =>
                    {
                        cmd.Parameters.Add(db.CrearParametro("@op_fecha_oper", Op_Fecha_Oper));

                        cmd.Parameters.Add(db.CrearParametro("@op_cve_rastreo", Op_Cve_Rastreo));

                    });

                    return result?.Data?.FirstOrDefault() ?? new SalidaEstatusSPEIDTO();
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSPSPEIStatus|{ex}");
                return new SalidaEstatusSPEIDTO();
            }

        }

        /// <summary>
        /// Responde salida TEF.
        /// </summary>
        /// <param name="salidaTEF"></param>
        /// <returns></returns>
        public async Task<SalidaTefDTO> Responde_Salida_Tef(SalidaTEFModel salidaTEF)
        {
            try
            {
                var resp = await EjecutarStoredProcedureQuery<SalidaTefDTO>(ProcedimientoAlmacenado.sp_InterfaceTOMITEFCambiaEstatusEnviadas, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@id_master";
                    param.Value = salidaTEF.iOperacionId;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@op_fecha_oper";
                    param.Value = salidaTEF.iFechaProceso;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@op_cve_rastreo";
                    param.Value = salidaTEF.sClaveRastreo;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@op_monto";
                    param.Value = salidaTEF.dMonto;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@op_razon_rechazo";
                    param.Value = salidaTEF.sCausaDevolucion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@op_estatus";
                    param.Value = salidaTEF.sClaveOperacion;
                    cmd.Parameters.Add(param);

                });

                return resp?.Data?.FirstOrDefault() ?? new SalidaTefDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Responde_Salida_Tef|{ex}");
                return new SalidaTefDTO();
                
            }

        }

        /// <summary>
        /// Responde entrada TEF.
        /// </summary>
        /// <param name="IdOperacion"></param>
        /// <param name="FechaProceso"></param>
        /// <param name="CausaDevolucion"></param>
        /// <param name="ClaveOperacion"></param>
        /// <returns></returns>
        public async Task<SalidaTefDTO> Responde_Entrada_Tef(int IdOperacion, int FechaProceso, int CausaDevolucion, string ClaveOperacion)
        {
            try
            {
                var resp = await EjecutarStoredProcedureQuery<SalidaTefDTO>(ProcedimientoAlmacenado.sp_InterfaceTOMI_TEF_Recibidas, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@id_master";
                    param.Value = IdOperacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@op_fecha_oper";
                    param.Value = FechaProceso;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@op_razon_rechazo";
                    param.Value = CausaDevolucion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@op_estatus";
                    param.Value = ClaveOperacion;
                    cmd.Parameters.Add(param);

                });

                return resp?.Data?.FirstOrDefault() ?? new SalidaTefDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Responde_Entrada_Tef|{ex}");
                return new SalidaTefDTO();
            }

        }

        /// <summary>
        /// Busca Estatus pago.
        /// </summary>
        /// <param name="Folio"></param>
        /// <returns></returns>
        public async Task<DatoPagoDTO> BuscarEstatusPago(string Folio)
        {
            //sp_Consulta_EstatusPago_BBVA
            try
            {
                var resp = await EjecutarStoredProcedureQuery<DatoPagoDTO>(ProcedimientoAlmacenado.sp_Consulta_EstatusPago_BBVA, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = Folio;
                    cmd.Parameters.Add(param);

                });

                return resp?.Data?.FirstOrDefault() ?? new DatoPagoDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|BuscarEstatusPago|{ex}");
                return new DatoPagoDTO();
            }


        }

        /// <summary>
        /// Ejecuta TEF recibidas.
        /// </summary>
        /// <param name="Op_Fecha_Oper"></param>
        /// <returns></returns>
        public async Task<datoENT_TEFDTO> EjecutaSPTefRecibidad(int Op_Fecha_Oper)
        {
            //sp_InterfaceTOMI_TEF_Recibidas
            try
            {
                var resp = await EjecutarStoredProcedureQuery<datoENT_TEFDTO>(ProcedimientoAlmacenado.sp_InterfaceTOMI_TEF_Recibidas, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@op_fecha_oper";
                    param.Value = Op_Fecha_Oper;
                    cmd.Parameters.Add(param);

                });

                return resp?.Data?.FirstOrDefault() ?? new datoENT_TEFDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutaSPTefRecibidad|{ex}");
                return new datoENT_TEFDTO();
                
            }

        }

        /// <summary>
        /// Ejecuta SPEI estatus recibidas.
        /// </summary>
        /// <param name="OpFechaOperacion"></param>
        /// <returns></returns>
        public async Task<DatoSPEIDTO> EjecutaSPSPSPEI_Estatus_Recibidas(int OpFechaOperacion)
        {
            //ConstantesServicios.sp_InterfaceTOMI_SPEI_Status2
            try
            {
                using (var ConexionSPEI = (IDisposable)ConexionBasedatosFactory.CrearConexion(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI))
                {
                    var db = (IBasedatosConexion)ConexionSPEI;
                    var result = await EjecutarStoredProcedureQuery<DatoSPEIDTO>(ProcedimientoAlmacenado.sp_InterfaceTOMI_SPEI_Status2, cmd =>
                    {
                        cmd.Parameters.Add(db.CrearParametro("@op_fecha_oper", OpFechaOperacion));

                    });

                    return result?.Data?.FirstOrDefault() ?? new DatoSPEIDTO();
                }
            }
            catch (Exception ex)
            {

                Config.EscribirLog($"ERROR|EjecutaSPSPSPEI_Estatus_Recibidas|{ex}");
                return new DatoSPEIDTO();
                
            }

        }

        /// <summary>
        /// Consulta Errores BBVA.
        /// </summary>
        /// <returns></returns>
        public async Task<ErrorBBVADTO> ConsultaErroresBBVA()
        {
            //sp_Consulta_Errores_BBVA
            try
            {
                var resp = await EjecutarStoredProcedureQuery<ErrorBBVADTO>(ProcedimientoAlmacenado.sp_Consulta_Errores_BBVA);

                return resp?.Data?.FirstOrDefault() ?? new ErrorBBVADTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConsultaErroresBBVA|{ex}");
                return new ErrorBBVADTO();
            }

        }

        /// <summary>
        /// Ejecuta Store procedures dinamicos con su DTO dinamico.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="SPNombre"></param>
        /// <param name="Parametros"></param>
        /// <returns></returns>
        public async Task<List<T>> EjecutarSP<T>(string SPNombre, Dictionary<string, object> Parametros) where T : new()
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<T>(SPNombre, cmd =>
                {
                    if (Parametros != null)
                    {
                        foreach (var parametro in Parametros)
                        {
                            IDbDataParameter param = cmd.CreateParameter();
                            param.ParameterName = $"@{parametro.Key}";
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);
                        }
                    }
                });

                return result?.Data;
            } 
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSP<T>|{ex}");
                return new List<T>();
            }
        }

        /// <summary>
        /// Ejecuta Store Procedures dinamicos.
        /// </summary>
        /// <param name="SPNombre"></param>
        /// <param name="Parametros"></param>
        /// <returns></returns>
        public async Task<bool> EjecutarSP(string SPNombre, Dictionary<string, object> Parametros)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(SPNombre, cmd =>
                {
                    if (Parametros != null)
                    {
                        foreach (var parametro in Parametros)
                        {
                            IDbDataParameter param = cmd.CreateParameter();
                            param.ParameterName = $"@{parametro.Key}";
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);
                        }
                    }
                });
                return result.Exito;

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSP|{ex}");
                return false;
            }

        }

        /// <summary>
        /// Obtiene Folio maestro.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="tipoOperacion"></param>
        /// <returns></returns>
        public async Task<FolioMaerstroModelo> ObtenerFolioMestro(string folio, string tipoOperacion)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<FolioMaerstroModelo>(ProcedimientoAlmacenado.sp_ObtenerFolio_Mestro, cmd =>
                {

                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@tipooperacion";
                    param.Value = tipoOperacion;
                    cmd.Parameters.Add(param);
                });

                return result?.Data?.FirstOrDefault() ?? new FolioMaerstroModelo();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerFolioMestro|{ex}");
                return new FolioMaerstroModelo();
            }
        }

        /// <summary>
        /// Obtiene el puerto donde escucha.
        /// </summary>
        /// <returns></returns>
        public async Task<string> obtenerPuertoEscucha()
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<PuertoEscuchaServicioModelo>(ProcedimientoAlmacenado.sp_ObtenerPuertoEscuchaServicio, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@servicio";
                    param.Value = ConstantesServicios.NombreServicio;
                    cmd.Parameters.Add(param);
                });

                return result?.Data?
                .Select(a => a.Puerto)
                .FirstOrDefault()
                .ToString() ?? string.Empty;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerFolioMestro|{ex}");
                return string.Empty;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Repositorio para la ejecución de consultas y procedimientos almacenados
    /// relacionados con operaciones SPEI.
    /// </summary>
    public class ConsultasBDSpei : RepositorioMaestro, IOperacionesSpei
    {
        /// <summary>
        /// Constructor de ConsultasBDSpei.
        /// Inicializa la conexión hacia la base de datos SPEI.
        /// </summary>
        public ConsultasBDSpei()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI, null, null)
        {
        }


        /// <summary>
        /// Ejecuta un procedimiento almacenado utilizando los parámetros recibidos.
        /// </summary>
        /// <param name="SPNombre">Nombre del procedimiento almacenado.</param>
        /// <param name="Parametros">Parámetros requeridos para la ejecución.</param>
        /// <returns>Resultado obtenido del procedimiento almacenado.</returns>
        public async Task<object> EjecutaSPTransaccion(
            string SPNombre,
            Dictionary<string, object> Parametros)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<DatoSPEIDTO>(
                    SPNombre,
                    cmd =>
                    {
                        if (Parametros == null)
                        {
                            return;
                        }

                        foreach (var parametro in Parametros)
                        {
                            IDbDataParameter param = cmd.CreateParameter();
                            param.ParameterName = $"@{parametro.Key}";
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);
                        }
                    });

                if (result?.Data == null)
                {
                    return new object();
                }

                return result.Data;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|EjecutaSPTransaccion|Error al ejecutar el procedimiento '{SPNombre}': {ex}");

                return null;
            }
        }

        /// <summary>
        /// Ejecuta la consulta de estatus SPEI para la operación indicada.
        /// </summary>
        /// <param name="Op_Fecha_Oper">Fecha de operación SPEI.</param>
        /// <param name="Op_Cve_Rastreo">Clave de rastreo.</param>
        /// <returns>Información de estatus SPEI.</returns>
        public async Task<SalidaEstatusSPEIDTO> EjecutarSPSPEIStatus(
            int Op_Fecha_Oper,
            string Op_Cve_Rastreo)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<SalidaEstatusSPEIDTO>(
                    ProcedimientoAlmacenado.sp_InterfaceTOMI_SPEI_Status2,
                    cmd =>
                    {
                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro(
                                "@op_fecha_oper",
                                Op_Fecha_Oper));

                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro(
                                "@op_cve_rastreo",
                                Op_Cve_Rastreo));
                    });

                if (result?.Data == null)
                {
                    return new SalidaEstatusSPEIDTO();
                }

                return result.Data.FirstOrDefault()
                    ?? new SalidaEstatusSPEIDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|EjecutarSPSPEIStatus|{ex}");

                return null;
            }
        }


        /// <summary>
        /// Obtiene información adicional de estatus para una operación SPEI.
        /// </summary>
        /// <param name="FechaOperacionSPEI">Fecha de operación.</param>
        /// <param name="FolioReferencia">Folio o clave de referencia.</param>
        /// <returns>Información adicional de la operación.</returns>
        public async Task<DatoSPEIDTO> EjecutarSPValidaEstatusAdicionalSPEI(
            int FechaOperacionSPEI,
            string FolioReferencia)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<DatoSPEIDTO>(
                    ProcedimientoAlmacenado.sp_InterfacesTOMI_SPEI_Status3,
                    cmd =>
                    {
                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro(
                                "@op_fecha_oper",
                                FechaOperacionSPEI));

                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro(
                                "@op_cve_rastreo",
                                FolioReferencia));
                    });

                if (result?.Data == null)
                {
                    return new DatoSPEIDTO();
                }

                return result.Data.FirstOrDefault()
                    ?? new DatoSPEIDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|EjecutarSPValidaEstatusAdicionalSPEI|{ex}");

                return null;
            }
        }


        /// <summary>
        /// Actualiza y obtiene las operaciones SPEI recibidas que requieren cambio de estatus.
        /// </summary>
        /// <param name="OpFechaOperacion">Fecha de operación.</param>
        /// <returns>Listado de operaciones SPEI procesadas.</returns>
        public async Task<List<DatoSPEIDTO>> EjecutaSPSPSPEI_Estatus_Recibidas(
            int OpFechaOperacion)
        {
            try
            {
                var result =
                    await EjecutarStoredProcedureQuery<DatoSPEIDTO>(
                        ProcedimientoAlmacenado.sp_InterfaceTOMI_SPEI_CambiaEstatusRecibidas,
                        cmd =>
                        {
                            cmd.Parameters.Add(
                                basedatosConexion.CrearParametro(
                                    "@op_fecha_oper",
                                    OpFechaOperacion));
                        });

                return result?.Data;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|EjecutaSPSPSPEI_Estatus_Recibidas|{ex}");

                return null;
            }
        }
    }
}

using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Repositorio para la ejecución de consultas y procedimientos almacenados
    /// en la base de datos CMS.
    /// </summary>
    public class ConsultasBDCms : RepositorioMaestro, IOperacionesCms
    {
        /// <summary>
        /// Constructor de ConsultasBDCms.
        /// Inicializa la conexión hacia la base de datos CMS.
        /// </summary>
        public ConsultasBDCms()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.CMS, null, null)
        {
        }

        /// <summary>
        /// Ejecuta un procedimiento almacenado utilizando los parámetros recibidos.
        /// </summary>
        /// <param name="SPNombre">Nombre del procedimiento almacenado.</param>
        /// <param name="Parametros">Parámetros requeridos por el procedimiento.</param>
        /// <returns>
        /// Resultado obtenido de la ejecución del procedimiento almacenado.
        /// </returns>
        public async Task<object> EjecutaSPTransaccion(
            string SPNombre,
            Dictionary<string, object> Parametros)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(
                    SPNombre,
                    cmd =>
                    {
                        if (Parametros == null)
                        {
                            return;
                        }

                        foreach (var parametro in Parametros)
                        {
                            IDbDataParameter param = cmd.CreateParameter();
                            param.ParameterName = $"@{parametro.Key}";
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);
                        }
                    });

                if (result?.Data == null)
                {
                    return new object();
                }

                return result.Data;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|EjecutaSPTransaccion|Error al ejecutar el procedimiento almacenado '{SPNombre}': {ex}");
                return null;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Repositorio para la ejecución de consultas y procedimientos almacenados
    /// relacionados con operaciones SPID.
    /// </summary>
    public class ConsultasBDSpid : RepositorioMaestro, IOperacionesSpid
    {
        /// <summary>
        /// Constructor de ConsultasBDSpid.
        /// Inicializa la conexión hacia la base de datos SPID.
        /// </summary>
        public ConsultasBDSpid()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPID, null, null)
        {

        }


        /// <summary>
        /// Ejecuta un procedimiento almacenado utilizando los parámetros recibidos.
        /// </summary>
        /// <param name="SPNombre">Nombre del procedimiento almacenado.</param>
        /// <param name="Parametros">Parámetros requeridos para la ejecución.</param>
        /// <returns>
        /// Lista de registros obtenidos del procedimiento almacenado.
        /// </returns>
        public async Task<List<T>> EjecutaSPTransaccion<T>(string SPNombre, Dictionary<string, object> Parametros) where T : new()
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<T>(
                    SPNombre,
                    cmd =>
                    {
                        if (Parametros == null)
                        {
                            return;
                        }

                        foreach (var parametro in Parametros)
                        {
                            IDbDataParameter param = cmd.CreateParameter();
                            param.ParameterName = $"@{parametro.Key}";
                            param.Value = parametro.Value;
                            cmd.Parameters.Add(param);
                        }
                    });

                return result?.Data;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|EjecutaSPTransaccion|Error al ejecutar '{SPNombre}': {ex}");

                return null;
            }
        }

        /// <summary>
        /// Obtiene el estatus de operaciones SPID recibidas para la fecha indicada.
        /// </summary>
        /// <param name="OpFechaOperacion">Fecha de operación.</param>
        /// <returns>
        /// Conjunto de operaciones SPID recibidas para la fecha indicada.
        /// </returns>
        public async Task<List<DatoENT_SPIDDTO>> EjecutaSPSPID_Estatus_Recibidas(
            int OpFechaOperacion)
        {
            try
            {
                var resp =
                    await EjecutarStoredProcedureQuery<DatoENT_SPIDDTO>(
                        ProcedimientoAlmacenado.sp_InterfaceTOMI_SPID_Estatus_Recibidas,
                        cmd =>
                        {
                            IDbDataParameter param = cmd.CreateParameter();
                            param.ParameterName = "@op_fecha_oper";
                            param.Value = OpFechaOperacion;
                            cmd.Parameters.Add(param);
                        });

                

                return resp?.Data
                    ?? new List<DatoENT_SPIDDTO>();
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|EjecutaSPSPID_Estatus_Recibidas|{ex}");

                return new List<DatoENT_SPIDDTO>();
            }
        }


        /// <summary>
        /// Ejecuta la consulta de estatus SPID utilizando el procedimiento almacenado indicado.
        /// </summary>
        /// <param name="SPNombre">Nombre del procedimiento almacenado.</param>
        /// <param name="Parametros">Parámetros requeridos para la consulta.</param>
        /// <returns>
        /// Resultado de la consulta de estatus SPID.
        /// </returns>
        public async Task<EntradaSPIDModel> SpidEstatusRecibidas(
            string SPNombre,
            Dictionary<string, object> Parametros)
        {
            try
            {
                var resultado =
                    await EjecutarStoredProcedureQuery<EntradaSPIDModel>(
                        SPNombre,
                        cmd =>
                        {
                            if (Parametros == null)
                            {
                                return;
                            }

                            foreach (var parametro in Parametros)
                            {
                                IDbDataParameter param = cmd.CreateParameter();
                                param.ParameterName = $"@{parametro.Key}";
                                param.Value = parametro.Value;
                                cmd.Parameters.Add(param);
                            }
                        },
                        reader => new EntradaSPIDModel()
                        {
                            IntClave = ConversionesComunes.ParseEnteroRequerido(reader.GetValue(0)?.ToString(), "IntClave")
                        },
                        CancellationToken.None,
                        esVistasT24: false,
                        commandTimeout: null,
                        validarAfectacion: false);

                return resultado?.Data?.FirstOrDefault();
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|SpidEstatusRecibidas|{ex}");

                return null;
            }
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Repositorio para la ejecución de consultas y procedimientos almacenados
    /// relacionados con operaciones TEF.
    /// </summary>
    public class ConsultasBDTef : RepositorioMaestro, IOperacionesTef
    {
        /// <summary>
        /// Constructor de ConsultasBDTef.
        /// Inicializa la conexión hacia la base de datos TEF.
        /// </summary>
        public ConsultasBDTef()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TEF, null, null)
        {
        }

        /// <summary>
        /// Obtiene las operaciones TEF recibidas para la fecha indicada.
        /// </summary>
        /// <param name="Op_Fecha_Oper">Fecha de operación a consultar.</param>
        /// <returns>
        /// Información de la operación TEF recuperada.
        /// </returns>
        public async Task<datoENT_TEFDTO> EjecutaSPTefRecibidad(
            int Op_Fecha_Oper)
        {
            try
            {
                var resp =
                    await EjecutarStoredProcedureQuery<datoENT_TEFDTO>(
                        ProcedimientoAlmacenado.sp_InterfaceTOMI_TEF_Recibidas,
                        cmd =>
                        {
                            IDbDataParameter param = cmd.CreateParameter();
                            param.ParameterName = "@op_fecha_oper";
                            param.Value = Op_Fecha_Oper;
                            cmd.Parameters.Add(param);
                        });

                if (resp?.Data == null)
                {
                    return new datoENT_TEFDTO();
                }

                return resp.Data.FirstOrDefault()
                    ?? new datoENT_TEFDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|EjecutaSPTefRecibidad|{ex}");

                return new datoENT_TEFDTO();
            }
        }

        /// <summary>
        /// Actualiza el estatus de una operación TEF y registra la respuesta
        /// de devolución correspondiente.
        /// </summary>
        /// <param name="IdOperacion">Identificador de la operación.</param>
        /// <param name="FechaProceso">Fecha de procesamiento.</param>
        /// <param name="CausaDevolucion">Código de causa de devolución.</param>
        /// <param name="ClaveOperacion">Estatus de la operación.</param>
        /// <returns>
        /// Resultado de la actualización de la operación.
        /// </returns>
        public async Task<SalidaTefDTO> Responde_Entrada_Tef(
            int IdOperacion,
            int FechaProceso,
            int CausaDevolucion,
            string ClaveOperacion)
        {
            try
            {
                var resp =
                    await EjecutarStoredProcedureQuery<SalidaTefDTO>(
                        ProcedimientoAlmacenado.sp_InterfaceTOMITEFCambiaEstatusRecibidas,
                        cmd =>
                        {
                            IDbDataParameter param = cmd.CreateParameter();
                            param.ParameterName = "@id_master";
                            param.Value = IdOperacion;
                            cmd.Parameters.Add(param);

                            param = cmd.CreateParameter();
                            param.ParameterName = "@op_fecha_oper";
                            param.Value = FechaProceso;
                            cmd.Parameters.Add(param);

                            param = cmd.CreateParameter();
                            param.ParameterName = "@op_razon_rechazo";
                            param.Value = CausaDevolucion;
                            cmd.Parameters.Add(param);

                            param = cmd.CreateParameter();
                            param.ParameterName = "@op_estatus";
                            param.Value = ClaveOperacion;
                            cmd.Parameters.Add(param);
                        });

                if (resp?.Data == null)
                {
                    return new SalidaTefDTO();
                }

                return resp.Data.FirstOrDefault()
                    ?? new SalidaTefDTO();
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|Responde_Entrada_Tef|{ex}");

                return new SalidaTefDTO();
            }
        }
    }

}


namespace Aplicacion.Enums
{
    /// <summary>
    /// Procedimientos almacenados.
    /// </summary>
    public class ProcedimientoAlmacenado
    {
        #region Stored Procedures IterfacesT24 (Tomi)

        public const string sp_Fecha_Sistema = "usp_ObtenerFechaSistema";
        public const string sp_Parametros_Sistema = "dbo.usp_Comun_ObtenerParametrosSistema";
        public const string sp_UltimoProcesoFolio = "dbo.usp_ConsultaAliasUltimoProceso_MUFGLibreria";
        public const string sp_IniciaProceso = "dbo.usp_comun_InicioProceso";
        public const string sp_TerminaProceso = "dbo.usp_comun_TerminarProceso"; 
        public const string sp_ActualizaPlanTrabajo_Maestro_CNT = "dbo.usp_CNT_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_FXD_GPH = "dbo.usp_FXD_GPH_ActualizaPlanTrabajoMaestro";        
        public const string sp_ActualizaPlanTrabajo_Maestro_FXD_H2H = "dbo.usp_FXD_H2H_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_FXD_Local = "dbo.usp_FXD_Local_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_FXD = "dbo.usp_FXD_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_TMD = "dbo.usp_TMD_ConsultarTombinNF";
        public const string sp_ActualizaFormatoAceptadoCms_TMD = "dbo.usp_TMD_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaPlanTrabajo_Maestro_TMD = "dbo.usp_TMD_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_TAX = "dbo.usp_TAX_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_SPID_GPH = "dbo.usp_SPID_GPH_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_SPID_H2H = "dbo.usp_SPID_H2H_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_SPID_Local = "dbo.usp_SPID_Local_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_SPID = "dbo.usp_SPID_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_SER = "dbo.usp_SER_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_FXT_GPH = "dbo.ssp_FXT_GPH_ConsultarTombinNF";
        public const string sp_ActualizaPlanTrabajo_Maestro_FXT_GPH = "dbo.usp_FXT_GPH_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_FXT_H2H = "dbo.usp_FXT_H2H_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_FXT_H2H = "dbo.ssp_FXT_H2H_ConsultarTombinNF";
        public const string sp_ActualizaPlanTrabajo_Maestro_FXT_Local = "dbo.usp_FXT_Local_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_FXT_Local = "dbo.ssp_FXT_Local_ConsultarTombinNF";
        public const string sp_ConsultarTombinNF_FXT = "dbo.ssp_FXT_ConsultarTombinNF";
        public const string sp_ActualizaPlanTrabajo_Maestro_FXT = "dbo.usp_FXT_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_MercadoDinero = "dbo.usp_MercadoDinero_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_MXF = "dbo.usp_MXF_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_MFX = "dbo.ssp_MFX_ConsultarTombinNF";
        public const string sp_ConsultarTombinNFEstatus_MFX = "dbo.ssp_MFX_ConsultarTombinNFEstatus";
        public const string sp_ActualizaTombinNF_MXF = "dbo.usp_MXF_ActualizaTombinNF";
        public const string sp_ConsultarTombinNFEstatus_TEF = "dbo.ssp_TEF_ConsultarTombinNF";
        public const string sp_ActualizaPlanTrabajo_Maestro_TEF = "dbo.usp_TEF_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_TEF_H2H = "dbo.usp_TEF_H2H_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_TEF_H2H = "dbo.ssp_TEF_H2H_ConsultarTombinNF";
        public const string sp_ActualizaPlanTrabajo_Maestro_TEF_GPH = "dbo.usp_TEF_GPH_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_TEF_GPH = "dbo.ssp_TEF_GPH_ConsultarTombinNF"; 
        public const string sp_ActualizaPlanTrabajo_Maestro_SAT = "dbo.usp_SAT_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_SAT = "dbo.ssp_SAT_ConsultarTombinNF"; 
        public const string sp_ActualizaPlanTrabajo_Maestro_PaP_FXD = "dbo.usp_PaP_FXD_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_PaP_FXD = "dbo.ssp_PaP_FXD_ConsultarTombinNF";
        public const string sp_ActualizaFormatoTomi_PaP_FXD = "dbo.usp_PaP_FXD_ActualizaFormatoCms";
        public const string sp_ActualizaPlanTrabajo_Maestro_PaP_SPID = "dbo.usp_PaP_SPID_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_PaP_SPID = "dbo.ssp_PaP_SPID_ConsultarTombinNF";
        public const string sp_ActualizaFormatoTomi_PaP_SPID = "dbo.usp_PaP_SPID_ActualizaFormatoTomi";
        public const string sp_ActualizaPlanTrabajo_Maestro_NOM_FXD = "dbo.usp_NOM_FXD_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_NOM_FXD = "dbo.ssp_PaP_SPID_ConsultarTombinNF";
        public const string sp_ActualizaFormatoTomi_NOM_FXD = "dbo.usp_NOM_FXD_ActualizaFormatoTomi";
        public const string sp_ActualizaPlanTrabajo_Maestro_PaP_FXT = "dbo.usp_PaP_FXT_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNFEstatus_PaP_FXT = "dbo.ssp_PaP_FXT_ConsultarTombinNFEstatus";
        public const string sp_ActualizaTombinDetalle_PaP_FXT = "dbo.usp_PaP_FXD_ActualizaTombinDetalle";
        public const string sp_ConsultarTombinNF_PaP_FXT = "dbo.ssp_PaP_FXT_ConsultarTombinNF";
        public const string sp_ActualizaFormatoTomi_PaP_FXT = "dbo.usp_PaP_FXT_ActualizaFormatoTomi";
        public const string sp_ActualizaPlanTrabajo_Maestro_ENT_SPEI = "dbo.usp_ENT_SPEI_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinEstradasSpei = "dbo.ssp_ENT_SPEI_ConsultarTombinEntradasSPEI";
        public const string sp_ConsultarBmxPagDia_ENT_COAS = "dbo.ssp_ENT_COAS_ConsultarBmxPagDia";
        public const string sp_ActualizarBmxPagDia_ENT_COAS = "dbo.usp_ENT_COAS_ActualizaBmxPagDia";
        public const string sp_ActualizaPlanTrabajo_Maestro_ENT_COAS = "dbo.usp_ENT_COAS_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaPlanTrabajo_Maestro_ENT_SPEI2 = "dbo.usp_ENT_SPEI2_ActualizaPlanTrabajoMaestro";
        public const string sp_InterfaceTOMI_SPID_Estatus_Recibidas = "sp_InterfaceTOMI_SPID_Estatus_Recibidas";
        public const string sp_ActualizaPlanTrabajo_Maestro_ENT_SPID = "dbo.usp_ENT_SPID_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarSubTipo_ENT_SPID = "ssp_ENT_SPID_ConsultarSubTipoPlanMaestro";
        public const string sp_ConsultarOperacionDevuelta_ENT_SPID = "dbo.ssp_ENT_SPID_ConsultarOperacionDevuelta";
        public const string sp_InterfaceTOMI_TEF_Recibidas = "sp_InterfaceTOMI_TEF_Recibidas";
        public const string sp_ActualizaPlanTrabajo_Maestro_ENT_TEF = "dbo.usp_ENT_TEF_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_ENT_TEF = "dbo.ssp_ENT_TEF_ConsultarTombinEntradasTEF";
        public const string sp_ConsultarTombinNF_FXI_GPH = "dbo.ssp_FXI_GPH_ConsultarTombin";
        public const string sp_ActualizaTombinNF_FXI_GPH = "usp_FXI_GPH_ActualizaTombinNF";
        public const string sp_ActualizaPlanTrabajo_Maestro_FXI_GPH = "usp_FXI_GPH_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_FXI_H2H = "dbo.ssp_FXI_H2H_ConsultarTombin";
        public const string sp_ActualizaTombinNF_FXI_H2H = "dbo.usp_FXI_H2H_ActualizaTombinNF";
        public const string sp_ActualizarPlanTrabajo_Maestro_FXI_H2H = "dbo.usp_FXI_H2H_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_FXI_Local = "dbo.ssp_FXI_Local_ConsultarTombin";
        public const string sp_ActualizaTombinNF_FXI_Local = "dbo.usp_FXI_Local_ActualizaTombinNF";
        public const string sp_ActualizarPlanTrabajo_Maestro_FXI_Local = "dbo.usp_FXI_Local_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_FXI = "ssp_FXI_ConsultarTombin";
        public const string sp_ActualizarPlanTrabajo_Maestro_FXI = "dbo.usp_FXI_ActualizaPlanTrabajoMaestro";
        public const string sp_ActualizaTombinDetalle_FXI = "dbo.usp_FXI_ActualizaTombinDetalle";
        public const string sp_ConsultarTombinNF_PaP_FXI = "dbo.ssp_PaP_FXI_ConsultarTombinNF";
        public const string sp_ActualizarPlanTrabajo_Maestro_PaP_FXI = "dbo.usp_PaP_FXI_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_Detalle_PaP_FXI = "dbo.ssp_PaP_FXI_ConsultarTombinDetalle";
        public const string sp_ActualizaTombinDetalle_PaP_FXI = "dbo.usp_PaP_FXI_Local_ActualizaTombinNFDetalle";
        public const string sp_ConsultarTombinNF_DetalleMaestro_PaP_FXI = "dbo.ssp_PaP_FXI_ConsultarTombinNF_DetalleMaestro";
        public const string sp_ActualizarFormatoTomi_DetalleMaestro_PaP_FXI = "usp_PaP_FXI_ActualizaTombinNFDetalleMaestro";
        public const string sp_ActualizarPlanTrabajo_Maestro_PaP_TEF = "dbo.usp_PaP_TEF_ActualizaPlanTrabajoMaestro";
        public const string sp_ConsultarTombinNF_Detalle_PaP_TEF = "dbo.ssp_PaP_TEF_ConsultarTombinDetalle";
        public const string sp_ActualizarTombinNF_Detalle_PaP_TEF = "dbo.usp_PaP_TEF_ActualizaTombinDetalle";
        public const string sp_ConsultarTombinNF_DetalleMaestro_PaP_TEF = "dbo.ssp_PaP_TEF_ConsultarTombinMaestro";
        public const string sp_ActualizarFormatoCms_DetalleMaestro_PaP_TEF = "dbo.usp_PaP_TEF_ActualizaFormatoCms_Maestro";
        public const string sp_ConsultaEstatusSPEI = "dbo.ssp_ConsultaEstatusSpei";
        public const string sp_ActualizaEstatusRechazadoSPEI = "dbo.usp_ActualizaEstatusRechazadoSpei";
        public const string sp_ActualizaEstatusProcesadoSPEI = "dbo.usp_ActualizaEstatusProcesadoSpei";
        public const string sp_ActualizaEstatusInterno_SPEI_LQ = "dbo.usp_ActualizaEstatuInternoLQSpei";
        public const string sp_ActualizaEstatusInterno_SPEI_D = "dbo.usp_ActualizaEstatuInternoDSpei";
        public const string sp_ActualizaEstatusInterno_SPEI = "dbo.usp_ActualizaEstatuInternoSpei";
        public const string sp_InterfaceTOMITEFCambiaEstatusEnviadas = "sp_InterfaceTOMI_TEF_Cambia_Estatus_Enviadas";
        public const string sp_InterfaceTOMI_SPEI_Status2 = "sp_InterfaceTOMI_SPEI_Status2";
        public const string sp_InterfaceTOMI_SPEI_Status_Recibidas = "sp_InterfaceTOMI_SPEI_Status_Recibidas";
        public const string sp_InterfaceTOMI_SPID_Status2 = "sp_InterfaceTOMI_SPID_Status2";
        public const string sp_Consultar_OrigenSPEI_CMS_GPH = "dbo.ssp_Consultar_OrigenSPEI_CMS_GPH";
        public const string sp_Consultar_OrigenSPEI_CMS_FXD = "dbo.ssp_Consultar_OrigenSPEI_CMS_FXD";
        public const string sp_Comunes_Actualizar_PlanTrabajoMaestro = "dbo.usp_Comunes_ActualizaPlanTrabajoMaestro";
        public const string sp_Consultar_Cat_RazonRechazo = "dbo.ssp_Consultar_Cat_Razon_Rechazo";
        public const string sp_Actualizar_TomiFormato_SPEI_Rechazado = "dbo.usp_Comunes_ActualizaFormatoCms_SPEI_Rechazado_TOMI";
        public const string sp_Consulta_Estatus_BBVA = "dbo.ssp_Consultar_Estatus_BBVA";
        public const string sp_Consulta_EstatusPago_BBVA = "dbo.ssp_Consultar_EstatusPago_BBVA";
        public const string sp_Consulta_Errores_BBVA = "dbo.ssp_Consultar_Errores_BBVA";
        public const string sp_Consulta_EstatusTombin_BBVA = "dbo.ssp_Consultar_TombinStatus_BBVA";
        public const string sp_Actualiza_OrigenRecTomi_BBVA = "dbo.usp_Comunes_ActualizaFormatoRechazadoTomi_BBVA";
        public const string sp_Consultar_Estatus_TbTombin_SPID = "dbo.ssp_Consultar_TombinStatus_SPID";
        public const string sp_Actualizar_EstatusInterno_SPID = "dbo.usp_Comunes_ActualizaEstatusInterno_SPID";
        public const string sp_Actualizar_Formato_SPID_Rechazado_TOMI = "dbo.usp_Comunes_ActualizaFormatoTomi_SPID_Rechazado";
        public const string sp_Consultar_EstatusSWIFT = "dbo.ssp_Consultar_EstatusSWIFT";
        public const string sp_InterfaceTOMI_SPID_Cambia_Estatus_Recibidas = "sp_InterfaceTOMI_SPID_Cambia_Estatus_Recibidas";
        public const string sp_InterfaceTOMI_SPEI_Cambia_Estatus_Recibidas = "sp_InterfaceTOMI_SPEI_Cambia_Estatus_Recibidas";
        public const string sp_InterfacesTOMI_SPEI_Status3 = "sp_InterfaceTOMI_SPEI_Status3";
        public const string sp_ObtenerFolio_Mestro = "dbo.usp_ObtenerFolioMaestro";
        public const string sp_ObtenerPuertoEscuchaServicio = "dbo.usp_ObtenerPuertoEscuchaServicio";
        public const string sp_InterfaceTOMITEFCambiaEstatusRecibidas = "sp_InterfaceTOMI_TEF_Cambia_Estatus_Recibidas";
        public const string sp_InterfaceTOMI_SPEI_CambiaEstatusRecibidas = "sp_InterfaceTOMI_SPEI_Estatus_Recibidas";
        public const string sp_ActualizaTombinStatusInterno_BBVA = "dbo.usp_Comunes_TombinStatusInterno_BBVA";
        public const string sp_ActualizaTombinStatusInternoLQD_BBVA = "dbo.usp_Comunes_TombinStatusInternoLQD_BBVA";
        public const string sp_Consultar_OrigenSPID_CMS_GPH = "dbo.ssp_Consultar_CmsOrigen_SPID_GPH";
        public const string sp_Consultar_OrigenSPID_CMS = "dbo.ssp_Consultar_CmsOrigen_SPID";
        #endregion

        #region SP's Para CMS(BOT)
        public const string sp_ActualizaFormatoRechazadoCms_TMD = "dbo.MUFG_MX2_TMD_ActualizaFormatoRechazadoCms"; //OK
        public const string sp_ActualizaFormatoAceptadoCms_FXT = "dbo.MUFG_MX2_FXT_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaFormatoRechazadoCms_FXT = "dbo.MUFG_MX2_FXT_ActualizaFormatoRechazadoCms";
        public const string sp_ActualizaFormatoAceptadoCms_MXF = "dbo.MUFG_MX2_MXF_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaFormatoRechazadoCms_MXF = "dbo.MUFG_MX2_MFX_ActualizaFormatoRechazadoCms";
        public const string sp_ActualizaFormatoAceptadoCms_TEF = "dbo.MUFG_MX2_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaFormatoRechazadoCms_TEF = "dbo.MUFG_MX2_TEF_ActualizaFormatoRechazadoCms";
        public const string sp_ActualizaFormatoAceptadoCms_SAT = "dbo.MUFG_MX2_SAT_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaFormatoRechazadoCms_SAT = "dbo.MUFG_MX2_SAT_ActualizaFormatoRechazadoCms";
        public const string sp_ActualizaFormatoCms_PaP_FXD = "dbo.MUFG_MX2_PaP_FXD_ActualizaFormatoCms";
        public const string sp_ActualizaFormatoCms_PaP_SPID = "dbo.MUFG_MX2_PaP_SPID_ActualizaFormatoCms";
        public const string sp_ActualizaFormatoCms_NOM_FXD = "dbo.MUFG_MX2_NOM_FXD_ActualizaFormatoCms";
        public const string sp_InsertarFormatoCms_NOM_FXD = "dbo.MUFG_MX2_NOM_FXD_InsertarFormatoCms";
        public const string sp_ActualizaFormatoAceptadoCms_PaP_FXT = "dbo.MUFG_MX2_PaP_FXT_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaFormatoRechazadoCms_PaP_FXT = "dbo.MUFG_MX2_PaP_FXT_ActualizaFormatoRechazadoCms";
        public const string sp_ActualizaFormatoCms_PaP_FXT = "dbo.MUFG_MX2_PaP_FXT_ActualizaFormatoCms";
        public const string sp_ActualizaFormatoAceptadoCms_FXI = "dbo.MUFG_MX2_FXI_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaFormatoAceptadoCms_PaP_FXI = "dbo.MUFG_MX2_PaP_FXI_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaFormatoRechazadoCms_PaP_FXI = "dbo.MUFG_MX2_PaP_FXI_ActualizaFormatoRechazadoCms";
        public const string sp_ActualizarFormatoCms_DetalleMaestro_PaP_FXI = "dbo.MUFG_MX2_PaP_FXI_ActualizaFormatoMaestro";
        public const string sp_ActualizaFormatoAceptadoCms_PaP_TEF = "dbo.MUFG_MX2_PaP_TEF_ActualizaFormatoAceptadoCms";
        public const string sp_ActualizaFormatoRechazadoCms_PaP_TEF = "dbo.MUFG_MX2_PaP_TEF_ActualizaFormatoRechazadoCms";
        public const string sp_Comunes_Actualizar_CmsFormato = "dbo.MUFG_MX2_Comunes_ActualizaCmsFormato";
        public const string sp_Insertar_CmsFormato = "dbo.MUFG_MX2_Insertar_T24Formato";
        public const string sp_Insertar_CmsFormatoPaPDetalle = "dbo.MUFG_MX2_Insertar_T24Formato_PaP_detalle";
        public const string sp_Actualizar_CmsFormato_SPEI_Rechazado = "dbo.MUFG_MX2_Comunes_ActualizaFormatoCms_SPEI_Rechazado";
        public const string sp_Actualiza_OrigenLQCms_BBVA = "dbo.MUFG_MX2_Comunes_ActualizaFormatoLiquidadaCms_BBVA";
        public const string sp_Actualiza_OrigenRecCms_BBVA = "dbo.MUFG_MX2_Comunes_ActualizaFormatoRechazadoCms_BBVA";
        public const string sp_Insertar_T24Formato_SPID = "dbo.MUFG_MX2_Insertar_T24Formato_SPID";
        public const string sp_Insertar_T24Formato_SPID_PaP = "dbo.MUFG_MX2_Insertar_T24Formato_SPID_PaP";
        public const string sp_Actualizar_CmsFormato_SPID_Rechazado = "dbo.MUFG_MX2_Comunes_ActualizaFormatoCms_SPID_Rechazado";
        public const string sp_ActualizaFormatoRechazadoCms_FXI = "dbo.MUFG_MX2_FXI_ActualizaFormatoRechazadoCms";

        #endregion

        #region SP's para Enlace(SPEI)
        public const string sp_ActualizaBmx1_SPEI = "dbo.MUFG_MX2_ActualizaBmx1Spei";
        #endregion
    }
}


namespace Aplicacion.Enums
{
    /// <summary>
    /// Constantes del servicio.
    /// </summary>
    public class ConstantesServicios
    {
        #region Constantes de Tipos de Operación

        public const string NombreServicio = "ActualizaEstatus";
        public const string OperacionFXD = "FXD";
        public const string OperacionFXI = "FXI";
        public const string OperacionFXT = "FXT";
        public const string OperacionSAT = "SAT";
        public const string OperacionMFX = "MFX";
        public const string OperacionFXDLocal = "FXD_Local";
        public const string OperacionFXILocal = "FXI_Local";
        public const string OperacionFXTLocal = "FXT_Local";
        public const string OperacionENTSPEI = "ENT_SPEI";
        public const string OperacionENTSPEI2 = "ENT_SPEI_2";
        public const string OperacionENTCOAS = "ENT_COAS";
        public const string OperacionENTSPID = "ENT_SPID";
        public const string OperacionSPIDLocal = "SPID_Local";
        public const string OperacionSPID = "SPID";
        public const string OperacionCNT = "CNT";
        public const string OperacionNOMFXD = "NOM_FXD";
        public const string OperacionPaPFXD = "PaP_FXD";
        public const string OperacionPaPFXI = "PaP_FXI";
        public const string OperacionPaPFXT = "PaP_FXT";
        public const string OperacionPaPSPID = "PaP_SPID";
        public const string OperacionTMD = "TMD";
        public const string OperacionTEF = "TEF";
        public const string OperacionPaPTEF = "PaP_TEF";
        public const string OperacionENTTEF = "ENT_TEF";
        public const string OperacionFXDGPH = "FXD_GPH";
        public const string OperacionFXIGPH = "FXI_GPH";
        public const string OperacionFXTGPH = "FXT_GPH";
        public const string OperacionSPIDGPH = "SPID_GPH";
        public const string OperacionTEFGPH = "TEF_GPH";
        public const string OperacionFXDH2H = "FXD_H2H";
        public const string OperacionFXIH2H = "FXI_H2H";
        public const string OperacionFXTH2H = "FXT_H2H";
        public const string OperacionSPIDH2H = "SPID_H2H";
        public const string OperacionTEFH2H = "TEF_H2H";
        public const string OperacionSER = "SER";
        public const string OperacionTAX = "TAX";
        public const string OperacionMDUNKNOW = "MD_UNKNOW";
        public const string OperacionMDT24 = "MD_T24";
        public const string OperacionMDSPEIE = "MD_SPEI_E";
        public const string OperacionMDSPEIS = "MD_SPEI_S";
        public const string OperacionMDSPIDE = "MD_SPID_E";
        public const string OperacionMDSPIDS = "MD_SPID_S";
        public const string OperacionMDMT103S = "MD_MT103_S";
        public const string OperacionMDMT202S = "MD_MT202_S";
        public const string OperacionMDMT210S = "MD_MT210_S";
        public const string OperacionMDMT300S = "MD_MT300_S";

        #endregion

        #region Cadenas de Estatus de Proceso

        public const string EstatusTerminada = "ESTATUS_TERMINADA";
        public const string EstatusProcesada = "PROCESADA";
        public const string EstatusSPEI = "SPEI";
        public const string EstatusTerminado = "TERMINADO";
        public const string EstatusRechazada = "RECHAZADA";

        #endregion

        #region Alias de Procesos

        public const string AliasDatosGeneral = "DATOS_GRAL";
        public const string AliasSPEIEstatus = "SPEI_ESTATUS";
        public const string AliasEstatusTerminadaCMS = "ESTATUS_TERMINADA_CMS";
        public const string AliasValidaEstatusTOMI = "VALIDA_ESTATUS_TOMI";
        public const string AliasSwitfEstatus = "SWIFT_ESTATUS";
        public const string AliasSPIDEstatus = "SPID_ESTATUS";
        public const string AliastBBVAStatus = "BBVA_ESTATUS";
        public const string AliasCambiaEstatusTEF = "CAMBIA_ESTATUS_TEF";
        public const string AliasCambiaEstatusCOAS = "CAMBIA_ESTATUS_COAS";
        public const string AliasCambiaEstatusSPID = "CAMBIA_ESTATUS_SPID";
        public const string AliasCambiaEstatusSPEI = "CAMBIA_ESTATUS_SPEI";
        public const string AliasEstatusTerminada = "ESTATUS_TERMINADA";

        #endregion

        #region Otras Constantes de cadena
        public const string TomiEstatusPROC = "PROC";
        public const string TomiEstatusAUAB = "AUAB";
        public const string TomiEstatusRECC = "RECC";
        public const string TomiEstatusAPBT = "APBT";
        public const string TomiEstatusCONP = "CONP";
        public const string TomiEstatusCONF = "CONF";

        public const string TomiEstatusSUCC = "SUCC";
        public const string TomiEstatusDECL = "DECL";

        public const string TomiEstatusLQ = "LQ";
        public const string TomiEstatusD = "D";
        public const string TomiEstatusCL = "CL";
        public const string TomiEstatusRL = "RL";
        public const string TomiEstatusCN = "CN";
        public const string TomiEstatusRB = "RB";

        public const string FormatoMensajeAceptado = "Formato aceptado";
        public const string FormatoMensajeRechazado = "Formato rechazado";

        public const string SubTipoOperacionDevolucion = "DEVOLUCION";

        public const char SeparadorPipe = '|';

        public const string SPEICl = "CL";
        public const string SPEIRl = "RL";
        public const string ClaveOperacionR = "R";
        public const string ClaveOperacionA = "A";
        public const string CausaDevolucionUno = "1";
        public const string ResultadoExitoso = "0";

        public const string VarianteServicioGPH = "gph";
        public const string VarianteServicioFXD = "FXD";

        public const string BaseCMSFormato = "formato";
        public const string BaseCMSFormatoPaPDetalle = "formato_pap_detalle";

        public const string ClaveEstatusRechazadaSPEI = "RECHAZADA - SPEI";

        #endregion

        #region Bases y Tablas
        public const string TbTombinNFFxdGph = "tb_tombin_nf_fxd_gph";
        public const string TbTombinNFFxd = "tb_tombin_nf_fxd";
        public const string CampoNFFolio = "nf_folio";
        public const string TbTombinNFFxdH2H = "tb_tombin_nf_fxd_h2h";
        public const string TbTombinNFFxdLocal = "tb_tombin_nf_fxd_local";
        public const string TbTombinNFSerPay = "tb_tombin_nf_ser_pay";
        public const string TbFormatPtPs = "tbl_format_pt_ps";
        public const string TbTombinNFSpid = "tb_tombin_nf_spid";
        public const string TbSpidFormat = "tbl_spid_formato";
        public const string TbTombinNFSpidGph = "tb_tombin_nf_spid_gph";
        public const string TbTombinNFSpidH2H = "tb_tombin_nf_spid_h2h";
        public const string TbTombinNFSpidLocal = "tb_tombin_nf_spid_local";
        public const string TbCalypsoLiquidacion = "tbl_calypso_liquidacion";
        public const string TbTombinNFFxdPaPDetalle = "tb_tombin_nf_fxd_pap_detalle";
        public const string TbFormatoPapDetalle = "formato_pap_detalle";
        public const string TbTombinNFSpidPaPDetalle = "tb_tombin_nf_spid_pap_detalle";
        public const string TbTombinNFFXDNominaDetalle = "tb_tombin_nf_fxd_nomina_detalle";
        public const string TbFormatoNominaDetalle = "formato_nomina_detalle";
        public const string TbTombinNFTaxPay = "tb_tombin_nf_tax_pay"; 


        
        #endregion

    }
}


namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Dato Spei.
    /// </summary>
    public class DatoSPEIDTO
    {

        public string Nf_estatus { get; set; }
        public int Op_clave { get; set; }
        public int Op_folio { get; set; }
        public string Op_cve_rastreo { get; set; }
        public string Folio_referencia { get; set; }
        public int Nf_fecha_ejecucion { get; set; }

        public int Ins_clave { get; set; }
        public int Tp_clave { get; set; }

        public int Op_monto { get; set; }
        public string Nf_opclave_spei { get; set; }
        public int Nf_opfechaoper_spei { get; set; }
        public string T24_folio_asignado { get; set; }
    }
}


namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Dato Ent_spid.
    /// </summary>
    public class DatoENT_SPIDDTO
    {
        public long op_clave { get; set; }
        public int Op_fecha_oper { get; set; }

        public int Op_folio { get; set; }

        public string Op_cve_rastreo { get; set; }
        public decimal Op_monto { get; set; }
        public int tp_clave { get; set; }
        public int cd_clave { get; set; }

    }

    public class OperacionDevueltaSPIDDTO
    {
        public int nf_folio { get; set; }
        public string canal { get; set; }
    }
}


namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Salida estatus spei.
    /// </summary>
    public class SalidaEstatusSPEIDTO
    {
        public string op_estado { get; set; }
        public decimal op_monto { get; set; }
        public int? Clave { get; set; } 
    }
}

using System;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Dato Tomi.
    /// </summary>
    public class DatoTomiNFDTO
    {
        public string NF_tipo { get; set;  }
        public string Nf_estatus { get; set; }
        public string Nf_razon { get; set; }
        public DateTime Nf_fecha_autoriza { get; set; }
        public string StrComisionCuenta { get; set; }
        public string StrComisionMoneda { get; set; }
        public decimal DecComisionMonto { get; set; }
        public decimal DecIvaMonto { get; set; }
        public string T24_transaction_id { get; set; }
        public string NF_Folio { get; set; }

        public string nf_estatus_interno { get; set; }
        public string cd_clave { get; set; }

        public DateTime NFFechaEjecucion { get; set; }

    }
}

using System;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Dato Tomi t24.
    /// </summary>
    public class DatoTomiT24DTO : DatoTomiNFDTO
    {
        public int Nf_tef_id_master { get; set; }
        public DateTime Nf_fecha_ejecucion { get; set; }
        public decimal T24_debit_amount { get; set; } 
    }
}

using System;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Dato BBVA.
    /// </summary>
    public class DatoBBVADTO
    {
        public string nf_estatus { get; set; }
        public string nf_cieguide_bbva { get; set; }

        public int nf_fecha_ejecucion { get; set; }
        public string t24_folio_asignado { get; set; }

        public string nf_estatus_interno { get; set; }
        public DateTime nf_fecha_pago_bbva { get; set; }

    }
}


namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Dato tomi sat.
    /// </summary>
    public class DatoTomiSATDTO
    {
        public string NF_estatus { get; set; }
        public string T24_transaction_id { get; set; }
        public string Nf_observacion { get; set; }
        public string Nf_cadena_original { get; set; }
        public string Nf_sello_digital { get; set; }
        public string Nf_llave_pago { get; set; }
        public string Nf_fecha_ejecucion { get; set; }
        public string nf_tipo { get; set; }
    }
}

