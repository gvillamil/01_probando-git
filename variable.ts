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

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Clase principal de servicio cnt.
    /// </summary>
    public class Servicio_CNT : ServicioBase
    {
        /// <summary>
        /// Controlador servicio cnt.
        /// </summary>
        /// <param name="operacion"></param>
        /// <param name="parametros"></param>
        public Servicio_CNT(OperacionDTO operacion, IParametrosOperaciones parametros) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }
        /// <summary>
        /// Procesa la actualización de estatus para operaciones CNT,
        /// registrando el avance del proceso y actualizando la información
        /// correspondiente en TOMI y Plan de Trabajo Maestro.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionServicio_CNT()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                dicParametros = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {
                    Config.EscribirLog("INFO|CNT.procesaOperacion|Actualizando el estatus como operación terminada en TOMI. ");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_CNT, dicParametros); //


                    Config.EscribirLog(string.Format("INFO|CNT.procesaOperacion|Se termina procesamiento de folio: {0}", _Operacion.StrFolio));

                    
                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });

                    Config.EscribirLog("INFO|CNT.procesaOperacion|Operación finalizada.");

                    dicParametros["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_CNT, dicParametros);


                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionServicio_CNT|{ex}");
                return false;
            }
            
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones ENT_COAS,
    /// incluyendo la actualización de estatus y la ejecución de los
    /// procesos correspondientes en TOMI.
    /// </summary>
    public class Servicio_ENT_COAS : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio ENT_COAS.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución del servicio.
        /// </param>
        public Servicio_ENT_COAS(OperacionDTO operacion, IParametrosOperaciones parametros)
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación ENT_COAS ejecutando las actualizaciones
        /// de estatus y las acciones correspondientes de acuerdo con el
        /// último proceso pendiente.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionENT_COAS()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Leyendo los datos de la operación en base SPEI");

                dicParametros = new Dictionary<string, object>() { { "FechaOperacion", _Configuracion.strFechayyyyMMdd } };


                List<DatoSPEIDTO> datosSpei = await _IParametrosOperaciones.EjecutarSP<DatoSPEIDTO>(ProcedimientoAlmacenado.sp_ConsultarBmxPagDia_ENT_COAS, dicParametros);
                DatoSPEIDTO datoSpei = datosSpei?.FirstOrDefault();

                if (strAliasProceso.Equals(ConstantesServicios.AliasCambiaEstatusCOAS))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A actualizar estatus en SPEI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    if (datoSpei?.Op_clave.ToString() == _Operacion.StrFolio)
                    {
                        dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarBmxPagDia_ENT_COAS, dicParametros);
                    }


                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación termina en TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros = new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"tipo_operacion", _Operacion.StrTipoOperacion },
                        {"opcion", 1 }
                    };

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_COAS, dicParametros);

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina procesaiento de folio: {_Operacion.StrFolio}");

                    
                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación en espera para finalizar el servicio del SEP");
                }


                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionENT_COAS|{ex}");
                return false;
            }
            
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones ENT_SPEI,
    /// incluyendo la actualización de estatus y la ejecución de los
    /// procesos asociados en TOMI y SPEI.
    /// </summary>
    public class Servicio_ENT_SPEI : ServicioBase
    {
        private readonly IOperacionesSpei _IOPeracionesSpei;

        /// <summary>
        /// Inicializa una nueva instancia del servicio ENT_SPEI.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesSPEI">
        /// Operaciones especializadas para SPEI.
        /// </param>
        public Servicio_ENT_SPEI(ConfiguracionServicio configuracion, OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesSpei operacionesSPEI, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IOPeracionesSpei = operacionesSPEI;
            _IParametrosOperaciones = parametros;

        }

        /// <summary>
        /// Procesa una operación ENT_SPEI ejecutando las actualizaciones
        /// de estatus y los procesos correspondientes de acuerdo con el
        /// último proceso pendiente registrado.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionENT_SPEI()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Leyendo los datos de la operación en base SPEI. ");


                List<DatoSPEIDTO> datosSpei = await _IOPeracionesSpei.EjecutaSPSPSPEI_Estatus_Recibidas(
                    ConversionesComunes.ParseEnteroRequerido(_Configuracion.strFechayyyyMMdd, nameof(_Configuracion.strFechayyyyMMdd)));
                DatoSPEIDTO datoSPEI = datosSpei?.Where(a => a?.Op_clave.ToString() == _Operacion.StrFolio).FirstOrDefault();

                if (strAliasProceso.Equals(ConstantesServicios.AliasCambiaEstatusSPEI))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A actualizar estatus en SPEI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    if (datoSPEI?.Op_folio.ToString() == _Operacion.StrFolio)
                    {
                        servicio = servicio ?? CrearServicioGeneral();
                        bProcesado = await servicio.Responde_Entrada_SPEI(ConstantesServicios.ClaveOperacionA, null, datoSPEI.Op_clave, datoSPEI.Nf_opfechaoper_spei, datoSPEI.Op_folio, datoSPEI.Op_cve_rastreo, datoSPEI.Op_monto);
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                        throw new Exception($"Error en procesamiento de resgitro CAMBIA_ESTATUS_SPEI con folio: {_Operacion.StrFolio}");
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_SPEI, dicParametros);

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina el procesamiento del folio: {_Operacion.StrFolio}");

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };


                        datosSpei = await _IParametrosOperaciones.EjecutarSP<DatoSPEIDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinEstradasSpei, dicParametros);

                        datoSPEI = datosSpei?.FirstOrDefault();

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Entrada Devolucion - {datoSPEI?.Tp_clave}");

                        if (datoSPEI?.Tp_clave == 0)
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando la operación {_Operacion.StrFolio} tipo - {datoSPEI?.Tp_clave}");

                            await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                            {
                                Folio = _Operacion.StrFolio,
                                TipoOperacion = _Operacion.StrTipoOperacion,
                                NombreProceso = _Configuracion.NombreProceso,
                                AliasProceso = strAliasProceso,
                                Servicio = _Configuracion.NombreServicio,
                                PId = (int)_Configuracion.Pid,
                                Excepcion = new Exception("")
                            }); //Termina Proceso

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_SPEI, dicParametros);

                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación en espera para finalizar el servicio del CEP.");
                        }

                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionENT_SPEI|{ex}");
                return false;
            }
            
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones ENT_SPEI2,
    /// incluyendo la actualización de estatus y la ejecución de los
    /// procesos asociados en TOMI y SPEI.
    /// </summary>
    public class Servicio_ENT_SPEI2 : ServicioBase
    {

        private readonly IOperacionesSpei2 _IOPeracionesSpei2;

        /// <summary>
        /// Inicializa una nueva instancia del servicio ENT_SPEI2.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesSpei2">
        /// Operaciones especializadas para SPEI2.
        /// </param>
        public Servicio_ENT_SPEI2(ConfiguracionServicio configuracion, OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesSpei2 operacionesSpei2, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _Configuracion = configuracion;
            _IOPeracionesSpei2 = operacionesSpei2;
        }

        /// <summary>
        /// Procesa una operación ENT_SPEI2 ejecutando las actualizaciones
        /// de estatus y los procesos correspondientes de acuerdo con el
        /// último proceso pendiente registrado.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionENT_SPEI2()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Leyendo los datos de la operación en base SPEI. ");

                List<DatoSPEIDTO> datosSpei = await _IOPeracionesSpei2.EjecutaSPSPSPEI_Estatus_Recibidas(
                    ConversionesComunes.ParseEnteroRequerido(_Configuracion.strFechayyyyMMdd, nameof(_Configuracion.strFechayyyyMMdd)));
                DatoSPEIDTO datoSPEI = datosSpei?.Where(a => a?.Op_clave.ToString() == _Operacion.StrFolio).FirstOrDefault();

                dicParametros = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasCambiaEstatusSPEI))
                {
                    if (datoSPEI == null)
                        throw new Exception($"No se encontró el registro SPEI para el folio {_Operacion.StrFolio}");

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A actualizar estatus en SPEI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    if (datoSPEI.Op_clave.ToString() == _Operacion.StrFolio)
                    {
                        servicio = servicio ?? CrearServicioGeneral();
                        bProcesado = await servicio.Responde_Entrada_SPEI(ConstantesServicios.ClaveOperacionA, null, datoSPEI.Op_clave, datoSPEI.Nf_opfechaoper_spei, datoSPEI.Op_folio, datoSPEI.Op_cve_rastreo, datoSPEI.Op_monto);
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                        throw new Exception($"Error en procesamiento de resgitro CAMBIA_ESTATUS_SPEI con folio: {_Operacion.StrFolio}");

                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_SPEI2, dicParametros);

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina el procesamiento del folio: {_Operacion.StrFolio}");

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        var dicParametros2 = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                        datosSpei = await _IParametrosOperaciones.EjecutarSP<DatoSPEIDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinEstradasSpei, dicParametros2);

                        datoSPEI = datosSpei?.FirstOrDefault();

                        if (datoSPEI == null)
                            throw new Exception($"No se encontró la entrada de devolución SPEI para el folio {_Operacion.StrFolio}");

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Entrada Devolucion - {datoSPEI.Tp_clave}");

                        if (datoSPEI.Tp_clave == 0)
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando la operación {_Operacion.StrFolio} tipo - {datoSPEI.Tp_clave}");


                            dicParametros["opcion"] = 2;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_SPEI2, dicParametros);

                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación en espera para finalizar el servicio del CEP.");
                        }

                    }
                    else
                        throw new Exception($"Error en el proceamiento de registro de ESTATUS_TERMINADA con folio {_Operacion.StrFolio}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionENT_SPEI2|{ex}");
                return false;
            }
            
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones ENT_SPID,
    /// incluyendo la actualización de estatus y la ejecución de los
    /// procesos asociados en TOMI y SPID.
    /// </summary>
    public class Servicio_ENT_SPID : ServicioBase
    {
        private readonly IOperacionesSpid _ioperacionesSpid;

        /// <summary>
        /// Inicializa una nueva instancia del servicio ENT_SPID.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesSpid">
        /// Operaciones especializadas para SPID.
        /// </param>
        public Servicio_ENT_SPID(ConfiguracionServicio configuracion, OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesSpid operacionesSpid, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _ioperacionesSpid = operacionesSpid;
        }

        /// <summary>
        /// Procesa una operación ENT_SPID ejecutando las actualizaciones
        /// de estatus y los procesos correspondientes de acuerdo con el
        /// último proceso pendiente registrado.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionENT_SPID()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Leyendo los datos de la operación en base SPID");

                List<DatoENT_SPIDDTO> datosENTSpid = await _ioperacionesSpid.EjecutaSPSPID_Estatus_Recibidas(
                    ConversionesComunes.ParseEnteroRequerido(_Configuracion.strFechayyyyMMdd, nameof(_Configuracion.strFechayyyyMMdd)));
                DatoENT_SPIDDTO datoENTSpid = datosENTSpid?.FirstOrDefault(dato => dato != null && dato.op_clave.ToString() == _Operacion.StrFolio);

                var dicparametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasCambiaEstatusSPID))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizar estatus en SPID");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    if (datoENTSpid != null)
                    {
                        servicio = servicio ?? CrearServicioGeneral();
                        bProcesado = await servicio.Responde_Entrada_SPID(ConstantesServicios.ClaveOperacionA, null, datoENTSpid.op_clave, datoENTSpid.Op_fecha_oper, datoENTSpid.Op_folio, datoENTSpid.Op_cve_rastreo, datoENTSpid.Op_monto);
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                    }
                    else
                        throw new Exception($"Error en procesamiento de registro CAMBIA_ESTATUS_SPID con folio: {_Operacion.StrFolio}");

                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada TOMI.");
                    string strDev = string.Empty;

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_SPID, dicparametros2);

                    dicParametros = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                };

                    List<SubtipoOperacionModelo> subtipoOP = await _IParametrosOperaciones.EjecutarSP<SubtipoOperacionModelo>(ProcedimientoAlmacenado.sp_ConsultarSubTipo_ENT_SPID, dicParametros);
                    strDev = subtipoOP?.FirstOrDefault()?.subtipo_operacion ?? string.Empty;

                    if (strDev.Equals(ConstantesServicios.SubTipoOperacionDevolucion) || datoENTSpid?.tp_clave == 0)
                    {
                        bProcesado = await ProcesarDevolucionEntSpid(datoENTSpid);

                        if (!bProcesado)
                        {
                            dicparametros2["opcion"] = 2;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_SPID, dicparametros2);
                            bProcesado = true;
                        }
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operacion en espera para finalizar el servicio del CEP.");

                    }
                    else
                        throw new Exception($"Error en procesamiento de registro CAMBIA_ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionENT_SPID|{ex}");
                return false;
            }

               
        }

        private async Task<bool> ProcesarDevolucionEntSpid(DatoENT_SPIDDTO datoENTSpid)
        {
            if (datoENTSpid == null || string.IsNullOrEmpty(datoENTSpid.Op_cve_rastreo))
                return false;

            dicParametros = new Dictionary<string, object>()
            {
                {"op_cve_rastreo", datoENTSpid.Op_cve_rastreo},
                {"op_monto", datoENTSpid.Op_monto},
                {"op_fecha_ejecucion", datoENTSpid.Op_fecha_oper}
            };

            List<OperacionDevueltaSPIDDTO> operacionesDevueltas = await _IParametrosOperaciones.EjecutarSP<OperacionDevueltaSPIDDTO>(ProcedimientoAlmacenado.sp_ConsultarOperacionDevuelta_ENT_SPID, dicParametros);
            OperacionDevueltaSPIDDTO operacionDevuelta = operacionesDevueltas?.LastOrDefault();

            if (operacionDevuelta == null || operacionDevuelta.nf_folio <= 0 || string.IsNullOrEmpty(operacionDevuelta.canal))
                return false;

            string tablaTomi = ObtenerTablaTomi(operacionDevuelta.canal);

            if (string.IsNullOrEmpty(tablaTomi))
                return false;

            dicParametros = new Dictionary<string, object>()
            {
                { "Folio", operacionDevuelta.nf_folio },
                { "TablaTomi", tablaTomi },
                { "CampoTomi", ConstantesServicios.CampoNFFolio },
                { "EstatusInterno", ConstantesServicios.TomiEstatusD },
                { "CdClave", datoENTSpid.cd_clave.ToString() }
            };

            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_EstatusInterno_SPID, dicParametros);

            dicParametros = new Dictionary<string, object>()
            {
                { "TablaTomi", tablaTomi },
                { "Folio", operacionDevuelta.nf_folio.ToString() }
            };

            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_Formato_SPID_Rechazado_TOMI, dicParametros);

            dicParametros = new Dictionary<string, object>()
            {
                { "Clave", datoENTSpid.cd_clave },
                { "TipoPago", ConstantesServicios.OperacionSPID }
            };

            List<CatRazonRechazoDTO> razonesRechazo = await _IParametrosOperaciones.EjecutarSP<CatRazonRechazoDTO>(ProcedimientoAlmacenado.sp_Consultar_Cat_RazonRechazo, dicParametros);
            CatRazonRechazoDTO razonRechazo = razonesRechazo?.FirstOrDefault();

            string tablaCms = ObtenerTablaCms(operacionDevuelta.canal);
            if (!string.IsNullOrEmpty(tablaCms))
            {
                dicParametros = new Dictionary<string, object>()
                {
                    { "TablaCms", tablaCms },
                    { "TablaTomi", tablaTomi },
                    { "RazonRechazo", razonRechazo?.Descripcion ?? string.Empty },
                    { "CodigoMensaje", operacionDevuelta.canal.Equals(ConstantesServicios.OperacionSPID) ? razonRechazo?.Codigo ?? string.Empty : string.Empty },
                    { "Folio", operacionDevuelta.nf_folio.ToString() }
                };

                await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Actualizar_CmsFormato_SPID_Rechazado, dicParametros);
            }

            dicParametros = new Dictionary<string, object>()
            {
                { "Folio", operacionDevuelta.nf_folio },
                { "Tipo_operacion", operacionDevuelta.canal },
                { "Opcion", operacionDevuelta.canal.Equals(ConstantesServicios.OperacionSPID) ? 11 : 12 }
            };

            if (operacionDevuelta.canal.Equals(ConstantesServicios.OperacionSPID))
            {
                dicParametros.Add("Desc_mensaje", razonRechazo?.Descripcion ?? string.Empty);
            }

            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_Comunes_Actualizar_PlanTrabajoMaestro, dicParametros);

            return true;
        }

        private static string ObtenerTablaTomi(string tipoOperacion)
        {
            switch (tipoOperacion)
            {
                case ConstantesServicios.OperacionSPID:
                    return ConstantesServicios.TbTombinNFSpid;
                case ConstantesServicios.OperacionSPIDGPH:
                    return ConstantesServicios.TbTombinNFSpidGph;
                case ConstantesServicios.OperacionSPIDH2H:
                    return ConstantesServicios.TbTombinNFSpidH2H;
                case ConstantesServicios.OperacionSPIDLocal:
                    return ConstantesServicios.TbTombinNFSpidLocal;
                case ConstantesServicios.OperacionPaPSPID:
                    return ConstantesServicios.TbTombinNFSpidPaPDetalle;
                default:
                    return string.Empty;
            }
        }

        private static string ObtenerTablaCms(string tipoOperacion)
        {
            switch (tipoOperacion)
            {
                case ConstantesServicios.OperacionSPID:
                    return ConstantesServicios.TbSpidFormat;
                case ConstantesServicios.OperacionPaPSPID:
                    return "tbl_spid_formato_pap_detalle";
                default:
                    return string.Empty;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones ENT_TEF,
    /// incluyendo la actualización de estatus y la ejecución de los
    /// procesos asociados en TOMI y TEF.
    /// </summary>
    public class Servicio_ENT_TEF : ServicioBase
    {
        private readonly IOperacionesTef _IOperacionesTEF;

        /// <summary>
        /// Inicializa una nueva instancia del servicio ENT_TEF.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesTEF">
        /// Operaciones especializadas para TEF.
        /// </param>
        public Servicio_ENT_TEF(ConfiguracionServicio configuracion, OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesTef operacionesTEF, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IOperacionesTEF = operacionesTEF;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación ENT_TEF ejecutando las actualizaciones
        /// de estatus y los procesos correspondientes de acuerdo con el
        /// último proceso pendiente registrado.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionENT_TEF()
        {

            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Leyendo los datos de la operación en base TEF");

                datoENT_TEFDTO datoEntTef = await _IOperacionesTEF.EjecutaSPTefRecibidad(
                    ConversionesComunes.ParseEnteroRequerido(_Configuracion.strFechayyyyMMdd, nameof(_Configuracion.strFechayyyyMMdd)));

                var dicparametros2 = new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"tipo_operacion", _Operacion.StrTipoOperacion },
                        {"opcion", 1 }
                    };

                if (strAliasProceso.Equals(ConstantesServicios.AliasCambiaEstatusTEF))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizar estatus en TEF");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    if (datoEntTef?.IDOperacion.ToString() == _Operacion.StrFolio)
                    {
                        servicio = servicio ?? CrearServicioGeneral();
                        await servicio.Responde_Entrada_TEF(datoEntTef.IDOperacion, datoEntTef.FechaProceso, 0, ConstantesServicios.ClaveOperacionA);
                        bProcesado = true;
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                        throw new Exception($"Error en procesamiento de registros CAMBIA_ESTATUS_TEF con folio: {_Operacion.StrFolio}");

                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como Operación terminada en TOMI.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    bProcesado = true;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_TEF, dicparametros2);

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Termina Procesamiento de Folio {_Operacion.StrFolio}");

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        dicParametros = new Dictionary<string, object>() { { "Folio", _Operacion.StrFolio } };
                        List<datoENT_TEFDTO> datosEntTef = await _IParametrosOperaciones.EjecutarSP<datoENT_TEFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_ENT_TEF, dicParametros);

                        datoEntTef = datosEntTef?.FirstOrDefault();


                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Entrada Tercero a Tercero {datoEntTef?.TipoOperacion}");
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminado la operación :{_Operacion.StrFolio}, Tipo {datoEntTef?.TipoOperacion}");

                        dicparametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_ENT_TEF, dicparametros2);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación en espera de finalizar en el servicio del CEP");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registros ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionENT_TEF|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXD,
    /// incluyendo la validación de estatus en SPEI y la actualización
    /// de información en CMS y TOMI.
    /// </summary>
    public class Servicio_FXD : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXD.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXD(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXD ejecutando las validaciones y
        /// actualizaciones correspondientes de acuerdo con el proceso
        /// pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_FXD()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                _Operacion.AliasProceso = strAliasProceso;

                dicParametros = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {
                    Config.EscribirLog("INFO|FXD.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXD, dicParametros);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPEIEstatus))
                {
                    Config.EscribirLog("INFO|FXD.procesaOperacion|A verificar el estatus de la operación en SPEI.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    EstatusSPEIModel resultadoSpei = await servicio.ValidaEstatusSPEI(ConstantesServicios.TbTombinNFFxd, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    bool bProcesado = resultadoSpei?.BResultado ?? false;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                    }

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {
                    Config.EscribirLog("INFO|FXD.procesaOperacion|Actualizando el estatus como operación en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXD, dicParametros);

                    servicio = servicio ?? CrearServicioGeneral();
                    bool bProcesado = await servicio.Actualiza_CMS_origen_SPEI(ConstantesServicios.TbTombinNFFxd, _Operacion.StrFolio, _Operacion.StrTipoOperacion, null);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                        Config.EscribirLog("INFO|FDX.procesaOperacion|Operacion finalizada");
                    }
                    else
                    {
                        throw new Exception(string.Format("Error en procesamiento de registro {0} con folio {1}", ConstantesServicios.AliasEstatusTerminadaCMS, _Operacion.StrFolio));
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_FXD|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXD Local,
    /// incluyendo la validación de estatus en SPEI y la actualización
    /// de información en CMS y TOMI.
    /// </summary>
    public class Servicio_FXDLocal : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXD Local.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXDLocal(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXD Local ejecutando las validaciones y
        /// actualizaciones correspondientes de acuerdo con el proceso
        /// pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionServicio_FXDLocal()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                dicParametros = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {


                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXD_Local, dicParametros);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPEIEstatus))
                {


                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operaciónen SPEI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    EstatusSPEIModel resultadoSpei = await servicio.ValidaEstatusSPEI(ConstantesServicios.TbTombinNFFxdLocal, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    bool bProcesado = resultadoSpei?.BResultado ?? false;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                    }

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en base TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXD_Local, dicParametros);

                    servicio = servicio ?? CrearServicioGeneral();
                    bool bProcesado = await servicio.Actualiza_CMS_origen_SPEI(ConstantesServicios.TbTombinNFFxdLocal, _Operacion.StrFolio, _Operacion.StrTipoOperacion, null);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionServicio_FXDLocal|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXD_GPH,
    /// incluyendo la validación de estatus en SPEI y la actualización
    /// de información en CMS y TOMI.
    /// </summary>
    public class Servicio_FXD_GPH : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXD_GPH.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXD_GPH(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXD_GPH ejecutando las validaciones y
        /// actualizaciones correspondientes de acuerdo con el proceso
        /// pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_FXD()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);




                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                dicParametros = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });



                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXD_GPH, dicParametros);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPEIEstatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operaciónen SPID");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    EstatusSPEIModel resultadoSpei = await servicio.ValidaEstatusSPEI(ConstantesServicios.TbTombinNFFxdGph, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    bool bProcesado = resultadoSpei?.BResultado ?? false;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                    }

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en base TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXD_GPH, dicParametros);

                    servicio = servicio ?? CrearServicioGeneral();
                    bool bProcesado = await servicio.Actualiza_CMS_origen_SPEI(ConstantesServicios.TbTombinNFFxdGph, _Operacion.StrFolio, _Operacion.StrTipoOperacion, null);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_FXD|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXD_H2H,
    /// incluyendo la validación de estatus en SPEI y la actualización
    /// de información en CMS y TOMI.
    /// </summary>
    public class Servicio_FXD_H2H : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXD_H2H.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXD_H2H(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXD_H2H ejecutando las validaciones y
        /// actualizaciones correspondientes de acuerdo con el proceso
        /// pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_FXD_H2H()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);


                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                dicParametros = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXD_H2H, dicParametros);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPEIEstatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operaciónen SPEI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    EstatusSPEIModel resultadoSpei = await servicio.ValidaEstatusSPEI(ConstantesServicios.TbTombinNFFxdH2H, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    bool bProcesado = resultadoSpei?.BResultado ?? false;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                    }
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en base TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXD_H2H, dicParametros);

                    servicio = servicio ?? CrearServicioGeneral();
                    bool bProcesado = await servicio.Actualiza_CMS_origen_SPEI(ConstantesServicios.TbTombinNFFxdH2H, _Operacion.StrFolio, _Operacion.StrTipoOperacion, null);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_FXD_H2H|{ex}");
                return false;
            }

        }
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
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones TMD,
    /// incluyendo la actualización de estatus en CMS y la
    /// sincronización de información proveniente de TOMI.
    /// </summary>
    public class Servicio_TMD : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio TMD.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesCms">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_TMD(OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionesCms) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionesCms;
        }

        /// <summary>
        /// Procesa una operación TMD ejecutando las actualizaciones
        /// correspondientes en CMS y Plan de Trabajo Maestro de acuerdo
        /// con el estatus registrado en TOMI.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionTMD()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {

                    Config.EscribirLog("INFO|TMD.procesaOperacion|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_TMD, dicParametros);
                    DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi?.Nf_estatus))
                    {
                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_TMD, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"sFT", datoTomi?.T24_transaction_id }
                    });

                        //bProcesado = true;
                        Config.EscribirLog("INFO|TMD.procesaOperacion|Terminado actualización de acpetación de operación.");


                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TMD, dicParametros2);

                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi?.Nf_estatus))
                    {

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoRechazadoCms_TMD, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"razonrechazo", datoTomi?.Nf_razon }
                    });

                        Config.EscribirLog("INFO|TMD.procesaOperacion|Terminando actualización de rechazo de operación.");
                        //bProcesado = true;

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TMD, dicParametros2);

                    }

                    dicParametros2["opcion"] = 3;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TMD, dicParametros2);


                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso
                    Config.EscribirLog("INFO|TMD.procesaOperacion|Operación Finalizada.");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionTMD|{ex}");
                return false;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones TEF_H2H,
    /// incluyendo la actualización de estatus, el manejo de devoluciones
    /// TEF y la actualización del Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_TEF_H2H : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio TEF_H2H.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_TEF_H2H(OperacionDTO operacion, IParametrosOperaciones parametros) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación TEF_H2H ejecutando las actualizaciones
        /// correspondientes de acuerdo con el estatus registrado en TOMI
        /// y el flujo definido para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_TEF_H2H()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF_H2H, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiT24DTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiT24DTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_TEF_H2H, dicParametros);
                    DatoTomiT24DTO datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminado actualización de acpetación de operación.");

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF_H2H, dicParametros2);

                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {

                        if (datoTomi.Nf_tef_id_master > 0)
                        {
                            SalidaTEFModel parametros = new SalidaTEFModel
                            {
                                iOperacionId = datoTomi.Nf_tef_id_master,
                                iFechaProceso = ConversionesComunes.ParseEnteroRequerido(datoTomi.Nf_fecha_ejecucion.ToString("yyyyMMdd"), nameof(datoTomi.Nf_fecha_ejecucion)),
                                sClaveRastreo = datoTomi.T24_transaction_id,
                                dMonto = datoTomi.T24_debit_amount,
                                sClaveOperacion = ConstantesServicios.ClaveOperacionR,
                                sCausaDevolucion = ConstantesServicios.CausaDevolucionUno,
                            };

                            await _IParametrosOperaciones.Responde_Salida_Tef(parametros);
                        }

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando actualización de rechazo de operación.");

                        dicParametros2["opcion"] = 3;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF_H2H, dicParametros2);

                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina procesamiento del folio: {_Operacion.StrFolio}");

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_TEF_H2H|{ex}");
                return false;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones TEF_GPH,
    /// incluyendo la actualización de estatus, el manejo de devoluciones
    /// TEF y la actualización del Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_TEF_GPH : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio TEF_GPH.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_TEF_GPH(OperacionDTO operacion, IParametrosOperaciones parametros) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación TEF_GPH ejecutando las actualizaciones
        /// correspondientes de acuerdo con el estatus registrado en TOMI
        /// y el flujo definido para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_TEF_GPH()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF_GPH, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiT24DTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiT24DTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_TEF_GPH, dicParametros);
                    DatoTomiT24DTO datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminado actualización de acpetación de operación.");

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF_GPH, dicParametros2);

                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {

                        if (datoTomi.Nf_tef_id_master > 0)
                        {
                            SalidaTEFModel parametros = new SalidaTEFModel
                            {
                                iOperacionId = datoTomi.Nf_tef_id_master,
                                iFechaProceso = ConversionesComunes.ParseEnteroRequerido(datoTomi.Nf_fecha_ejecucion.ToString("yyyyMMdd"), nameof(datoTomi.Nf_fecha_ejecucion)),
                                sClaveRastreo = datoTomi.T24_transaction_id,
                                dMonto = datoTomi.T24_debit_amount,
                                sClaveOperacion = ConstantesServicios.ClaveOperacionR,
                                sCausaDevolucion = ConstantesServicios.CausaDevolucionUno,
                            };

                            await _IParametrosOperaciones.Responde_Salida_Tef(parametros);
                        }

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando actualización de rechazo de operación.");

                        dicParametros2["opcion"] = 3;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF_GPH, dicParametros2);

                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina procesamiento del folio: {_Operacion.StrFolio}");

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");


                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_TEF_GPH|{ex}");
                return false;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones TEF,
    /// incluyendo la actualización de estatus en CMS, el manejo de
    /// devoluciones TEF y la actualización del Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_TEF : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio TEF.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesCms">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_TEF(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionesCms)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionesCms;
        }

        /// <summary>
        /// Procesa una operación TEF ejecutando las actualizaciones en CMS,
        /// el manejo de devoluciones cuando corresponda y la actualización
        /// del Plan de Trabajo Maestro.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionTEF()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {

                    Config.EscribirLog("INFO|TEF.procesaOperacion|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiT24DTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiT24DTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNFEstatus_TEF, dicParametros);
                    DatoTomiT24DTO datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {

                        dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_TEF, dicParametros);

                        Config.EscribirLog("INFO|TEF.procesaOperacion|Terminado actualización de acpetación de operación.");

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF, dicParametros2);
                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {

                        dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_TEF, dicParametros);

                        if (datoTomi.Nf_tef_id_master > 0)
                        {
                            SalidaTEFModel parametros = new SalidaTEFModel
                            {
                                iOperacionId = datoTomi.Nf_tef_id_master,
                                iFechaProceso = ConversionesComunes.ParseEnteroRequerido(datoTomi.Nf_fecha_ejecucion.ToString("yyyyMMdd"), nameof(datoTomi.Nf_fecha_ejecucion)),
                                sClaveRastreo = datoTomi.T24_transaction_id,
                                dMonto = datoTomi.T24_debit_amount,
                                sClaveOperacion = ConstantesServicios.ClaveOperacionR,
                                sCausaDevolucion = ConstantesServicios.CausaDevolucionUno,
                            };

                            await _IParametrosOperaciones.Responde_Salida_Tef(parametros);
                        }

                        Config.EscribirLog("INFO|TEF.procesaOperacion|Terminando actualización de rechazo de operación.");

                        dicParametros2["opcion"] = 3;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TEF, dicParametros2);

                    }

                    Config.EscribirLog($"INFO|TEF.procesaOperacion|Se termina procesamiento del folio: {_Operacion.StrFolio}");

 
                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog("INFO|TEF.procesaOperacion|Operación Finalizada.");


                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionTEF|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones TAX,
    /// incluyendo la validación de estatus en BBVA y la actualización
    /// de información en CMS y Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_TAX : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio TAX.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_TAX(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación TAX ejecutando las validaciones de
        /// estatus y las actualizaciones correspondientes de acuerdo
        /// con el proceso pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionTAX()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso
 

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TAX, dicParametros2);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });
                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliastBBVAStatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operación en BBVA");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.ValidaEstatus_BBVA(ConstantesServicios.TbTombinNFTaxPay, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrEstatusOperacion);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_TAX, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.Actualiza_CMS_origen_BBVA(ConstantesServicios.TbTombinNFTaxPay, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbFormatPtPs);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio {_Operacion.StrFolio}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionTAX|{ex}");
                return false;
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
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones SPID_H2H,
    /// incluyendo la validación de estatus en SPID y la actualización
    /// de información en CMS y Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_SPID_H2H : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio SPID_H2H.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_SPID_H2H(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación SPID_H2H ejecutando las validaciones de
        /// estatus y las actualizaciones correspondientes de acuerdo con
        /// el proceso pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionServicio_SPID_H2H()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SPID_H2H, dicParametros2);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPIDEstatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operaciónen en SPID");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.ValidaEstatus_SPID(ConstantesServicios.TbTombinNFSpidH2H, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en base TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SPID_H2H, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.Actualiza_CMS_origen_SPID(ConstantesServicios.TbTombinNFSpidH2H, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbFormatPtPs);

                    if (bProcesado)
                    {

                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");
                }
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionServicio_SPID_H2H|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones SPID_GPH,
    /// incluyendo la validación de estatus en SPID y la actualización
    /// de información en CMS y Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_SPID_GPH : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio SPID_GPH.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_SPID_GPH(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación SPID_GPH ejecutando las validaciones de
        /// estatus y las actualizaciones correspondientes de acuerdo con
        /// el proceso pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionServicio_SPID_GPH()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SPID_GPH, dicParametros2);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPIDEstatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operaciónen en SPID");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.ValidaEstatus_SPID(ConstantesServicios.TbTombinNFSpidGph, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }

                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en base TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SPID_GPH, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.Actualiza_CMS_origen_SPID(ConstantesServicios.TbTombinNFSpidGph, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbFormatPtPs);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionServicio_SPID_GPH|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones SPID Local,
    /// incluyendo la validación de estatus en SPID y la actualización
    /// de información en CMS y Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_SPIDLocal : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio SPID Local.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_SPIDLocal(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación SPID Local ejecutando las validaciones de
        /// estatus y las actualizaciones correspondientes de acuerdo con
        /// el proceso pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionServicio_SPIDLocal()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SPID_Local, dicParametros2);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPIDEstatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operaciónen en SPID");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.ValidaEstatus_SPID(ConstantesServicios.TbTombinNFSpidLocal, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog("INFO|SPID.procesaOperacion|Operación finalizada.");

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SPID_Local, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.Actualiza_CMS_origen_SPID(ConstantesServicios.TbTombinNFSpidLocal, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbSpidFormat);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog("INFO|SPID.procesaOperacion|Operación finalizada.");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionServicio_SPIDLocal|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones SPID,
    /// incluyendo la validación de estatus en SPID y la actualización
    /// de información en CMS y Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_SPID : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio SPID.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_SPID(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación SPID ejecutando las validaciones de
        /// estatus y las actualizaciones correspondientes de acuerdo
        /// con el proceso pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionServicio_SPID()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog("INFO|SPID.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SPID, dicParametros2);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPIDEstatus))
                {

                    Config.EscribirLog("INFO|SPID.procesaOperacion|A verificar el estatus de la operaciónen en SPID");
                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.ValidaEstatus_SPID(ConstantesServicios.TbTombinNFSpid, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                    }

                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {

                    Config.EscribirLog("INFO|SPID.procesaOperacion|Actualizando el estatus como operación terminada en CMS.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SPID, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.Actualiza_CMS_origen_SPID(ConstantesServicios.TbTombinNFSpid, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbSpidFormat);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionServicio_SPID|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones SER,
    /// incluyendo la validación de estatus en BBVA y la actualización
    /// de información en CMS y Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_SER : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio SER.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_SER(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación SER ejecutando las validaciones de
        /// estatus y las actualizaciones correspondientes de acuerdo
        /// con el proceso pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionSER()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                bool bProcesado = true;

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                   

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SER, new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                });

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliastBBVAStatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operación en BBVA.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.ValidaEstatus_BBVA(ConstantesServicios.TbTombinNFSerPay, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                    }

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SER, new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 2 }
                });


                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.Actualiza_CMS_origen_BBVA(ConstantesServicios.TbTombinNFSerPay, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbFormatPtPs);


                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionSER|{ex}");
                return false;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones SAT,
    /// incluyendo la actualización de estatus en CMS y la sincronización
    /// de información proveniente de TOMI.
    /// </summary>
    public class Servicio_SAT : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio SAT.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionCMS">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_SAT(OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionCMS) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionCMS;
        }

        /// <summary>
        /// Procesa una operación SAT ejecutando las actualizaciones
        /// correspondientes en CMS y Plan de Trabajo Maestro de acuerdo
        /// con el estatus registrado en TOMI.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionSAT()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {

                    Config.EscribirLog("INFO|ProcesarOperacionSAT|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso



                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SAT, new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                });

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiSATDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiSATDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_SAT, dicParametros);
                    DatoTomiSATDTO datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.NF_estatus))
                    {


                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_SAT, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"llavepago", datoTomi.Nf_llave_pago },
                        {"fechaejecucion", datoTomi.Nf_fecha_ejecucion },
                        {"sellodigital", datoTomi.Nf_sello_digital },
                        {"cadenaoriginal", datoTomi.Nf_cadena_original },
                        {"sFT", datoTomi.T24_transaction_id },
                    });

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminado actualización de acpetación de operación.");

                        

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SAT, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"tipo_operacion", _Operacion.StrTipoOperacion },
                        {"opcion" , 2 }
                    });

                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.NF_estatus))
                    {
                        

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoRechazadoCms_SAT, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"razonrechazo", datoTomi.Nf_observacion },
                        {"fechaejecucion", datoTomi.Nf_fecha_ejecucion }
                    });

                        Config.EscribirLog("INFO|ProcesarOperacionSAT|Terminando actualización de rechazo de operación.");

                       
                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_SAT, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"tipo_operacion", _Operacion.StrTipoOperacion },
                        {"opcion" , 3 }
                    });
                    }

                    Config.EscribirLog($"INFO|ProcesarOperacionSAT|Se termina procesamiento del folio: {_Operacion.StrFolio}");
                }


                await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                {
                    Folio = _Operacion.StrFolio,
                    TipoOperacion = _Operacion.StrTipoOperacion,
                    NombreProceso = _Configuracion.NombreProceso,
                    AliasProceso = strAliasProceso,
                    Servicio = _Configuracion.NombreServicio,
                    PId = (int)_Configuracion.Pid,
                    Excepcion = new Exception("")
                }); //Termina Proceso

                Config.EscribirLog("INFO|ProcesarOperacionSAT|Operación Finalizada.");

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionSAT|{ex}");
                return false;
            } 
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones PaP TEF,
    /// incluyendo la actualización de estatus en CMS y la consolidación
    /// de resultados sobre el folio maestro.
    /// </summary>
    public class Servicio_PaP_TEF : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio PaP_TEF.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionCMS">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_PaP_TEF(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionCMS, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionCMS;
        }

        /// <summary>
        /// Procesa una operación PaP TEF ejecutando las actualizaciones
        /// correspondientes en CMS, TOMI y Plan de Trabajo Maestro,
        /// consolidando los resultados asociados al folio maestro.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_PaP_TEF()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {
                    ServicioGeneral servicio = CrearServicioGeneral();
                    string strFolioMaestro = await servicio.DameFolioMaestro(_Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_TEF, new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                });

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiT24DTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiT24DTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_Detalle_PaP_TEF, dicParametros);
                    DatoTomiT24DTO datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_PaP_TEF, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"sFT", datoTomi.T24_transaction_id }
                    });



                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarTombinNF_Detalle_PaP_TEF, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"opcion", 1 }
                    });

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando actualización de aceptación de operación.");

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_TEF, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"tipo_operacion", _Operacion.StrTipoOperacion },
                        {"opcion", 2 }
                    });

                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoRechazadoCms_PaP_TEF, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"sFT", datoTomi.T24_transaction_id }
                    });


                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarTombinNF_Detalle_PaP_TEF, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"opcion", 2 }
                    });


                        if (datoTomi.Nf_tef_id_master > 0)
                            await servicio.Responde_Salida_TEF(
                                datoTomi.Nf_tef_id_master,
                                ConversionesComunes.ParseEnteroRequerido(datoTomi.Nf_fecha_ejecucion.ToString("yyyyMMdd"), nameof(datoTomi.Nf_fecha_ejecucion)),
                                datoTomi.T24_transaction_id,
                                datoTomi.T24_debit_amount,
                                ConstantesServicios.ClaveOperacionR,
                                1);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando actualización de rechazo de operación.");

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_TEF, new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"tipo_operacion", _Operacion.StrTipoOperacion },
                        {"opcion", 3 }
                    });

                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina Procesamiento de Folio: {_Operacion.StrFolio}");

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Buscando actualizar el maestro: {strFolioMaestro}");

                        dicParametros = new Dictionary<string, object>() { { "folio", strFolioMaestro } };

                        List<DatoPaPDetalleDTO> datosPap = await _IParametrosOperaciones.EjecutarSP<DatoPaPDetalleDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_DetalleMaestro_PaP_TEF, dicParametros);
                        DatoPaPDetalleDTO datoPap = datosPap?.FirstOrDefault();

                        bool bAprobado = datoPap != null && (datoPap.Procesados + datoPap.Devueltos) == datoPap.Regs_totales;

                        var dicParametros2 = new Dictionary<string, object>()
                        {
                            {"folio", strFolioMaestro },
                            {"procesados", datoPap ?.Procesados },
                            {"devueltos", datoPap ?.Devueltos },
                            {"opcion", 1 }
                        };

                        if (bAprobado)
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizar maestro como aprobado, Folio Maestro: {strFolioMaestro}");

                            

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizarFormatoCms_DetalleMaestro_PaP_TEF, dicParametros2);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarFormatoCms_DetalleMaestro_PaP_TEF, dicParametros2);

                            

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_TEF, new Dictionary<string, object>()
                        {
                            {"folio", _Operacion.StrFolio },
                            {"tipo_operacion", _Operacion.StrTipoOperacion },
                            {"opcion", 4 }
                        });

                        }
                        else
                        {
                            dicParametros2["opcion"] = 2;

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizarFormatoCms_DetalleMaestro_PaP_TEF, dicParametros2);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarFormatoCms_DetalleMaestro_PaP_TEF, dicParametros2);
                        }

                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_PaP_TEF|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones PaP SPID,
    /// incluyendo la validación de estatus en SPID, la actualización
    /// de información en CMS y la consolidación de resultados sobre
    /// el folio maestro.
    /// </summary>
    public class Servicio_PaP_SPID : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio PaP_SPID.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesCms">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_PaP_SPID(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionesCms, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionesCms;
        }

        /// <summary>
        /// Procesa una operación PaP SPID ejecutando las validaciones de
        /// estatus, actualizaciones en CMS y consolidación de resultados
        /// asociados al folio maestro.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_PaP_SPID()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_SPID, dicParametros2);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPIDEstatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operación en SPID.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.ValidaEstatus_SPID(ConstantesServicios.TbTombinNFSpidPaPDetalle, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }

                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_SPID, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.Actualiza_CMS_origen_SPID(ConstantesServicios.TbTombinNFSpidPaPDetalle, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbTombinNFSpidPaPDetalle);

                    if (bProcesado)
                    {
                        string strFolioMaestro = await servicio.DameFolioMaestro(_Operacion.StrFolio, _Operacion.StrTipoOperacion);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Buscando ACtualizar el Maestro: {strFolioMaestro}");

                        dicParametros = new Dictionary<string, object>() { { "folio", strFolioMaestro } };

                        List<DatoPaPDetalleDTO> datosPap = await _IParametrosOperaciones.EjecutarSP<DatoPaPDetalleDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_PaP_SPID, dicParametros);
                        DatoPaPDetalleDTO datoPap = datosPap?.FirstOrDefault();

                        bool bAprobado = datoPap != null && (datoPap.Procesados + datoPap.Devueltos) == datoPap.Regs_totales;

                        var dicParametros3 = new Dictionary<string, object>()
                        {
                            {"folio", strFolioMaestro },
                            {"procesadas", datoPap ?    .Procesados },
                            {"devueltas", datoPap ?.Devueltos },
                            {"opcion", 1 }
                        };

                        if (bAprobado)
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizar maestro como aprobado, Folio Maestro: {strFolioMaestro}");
                            

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoCms_PaP_SPID, dicParametros3);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaFormatoTomi_PaP_SPID, dicParametros3);

                            dicParametros2["folio"] = strFolioMaestro;
                            dicParametros2["opcion"] = 2;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_SPID, dicParametros2);
                        }
                        else
                        {
                            dicParametros3["opcion"] = 2;

                             await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoCms_PaP_SPID, dicParametros3);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaFormatoTomi_PaP_SPID, dicParametros3);

                        }

                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");

                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio {_Operacion.StrFolio}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_PaP_SPID|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones PaP FXT,
    /// incluyendo la actualización de estatus en CMS y la consolidación
    /// de resultados en el folio maestro.
    /// </summary>
    public class Servicio_PaP_FXT : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio PaP_FXT.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionCMS">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_PaP_FXT(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionCMS, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionCMS;
        }

        /// <summary>
        /// Procesa una operación PaP FXT ejecutando las actualizaciones
        /// correspondientes en CMS, TOMI y Plan de Trabajo Maestro,
        /// consolidando los resultados en el folio maestro.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_PaP_FXT()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {
                    ServicioGeneral servicio = CrearServicioGeneral();
                    string strFolioMaestro = await servicio.DameFolioMaestro(_Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_FXT, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };


                    List<DatoTomiT24DTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiT24DTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNFEstatus_PaP_FXT, dicParametros);
                    DatoTomiT24DTO datoTomi = datosTomi?.FirstOrDefault();


                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    var dicParametros3 = new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"sFT", datoTomi.T24_transaction_id }
                    };

                    var dicParametros4 = new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"opcion", 1 }
                    };

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {
                        

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_PaP_FXT, dicParametros3);

                        

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinDetalle_PaP_FXT, dicParametros4);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando actualización de aceptación de operación.");

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_FXT, dicParametros2);
                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        dicParametros3["sFT"] = datoTomi.Nf_razon;

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoRechazadoCms_PaP_FXT, dicParametros3);

                        dicParametros4["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinDetalle_PaP_FXT, dicParametros4);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando actualización de rechazo de operación.");

                        dicParametros2["opcion"] = 3;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_FXT, dicParametros2);

                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina procesamiento de folio");

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Buscando ACtualizar el Maestro: {strFolioMaestro}");

                        dicParametros = new Dictionary<string, object>() { { "Folio", strFolioMaestro } };

                        List<DatoPaPDetalleDTO> datosPap = await _IParametrosOperaciones.EjecutarSP<DatoPaPDetalleDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_PaP_FXT, dicParametros);
                        DatoPaPDetalleDTO datoPap = datosPap?.FirstOrDefault();

                        bool bAprobado = datoPap != null && (datoPap.Procesados + datoPap.Devueltos) == datoPap.Regs_totales;

                        var dicParametros5 = new Dictionary<string, object>()
                        {
                            {"folio", strFolioMaestro },
                            {"procesadas", datoPap ?.Procesados },
                            {"devueltas", datoPap ?.Devueltos },
                            {"opcion", 1 }
                        };

                        if (bAprobado)
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizar maestro como aprobado, Folio Maestro: {strFolioMaestro}");

                            

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoCms_PaP_FXT, dicParametros5);
                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoTomi_PaP_FXT, dicParametros5);

                            dicParametros2["opcion"] = 4;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_FXT, dicParametros2);

                        }
                        else
                        {
                            dicParametros5["opcion"] = 2;

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoCms_PaP_FXT, dicParametros5);
                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoTomi_PaP_FXT, dicParametros5);
                        }

                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_PaP_FXT|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones PaP FXI,
    /// incluyendo la validación de estatus en TOMI y SWIFT, la actualización
    /// de información en CMS y la consolidación de resultados sobre el
    /// folio maestro.
    /// </summary>
    public class Servicio_PaP_FXI : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;
        /// <summary>
        /// Inicializa una nueva instancia del servicio PaP_FXI.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesCMS">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_PaP_FXI(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionesCMS, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionesCMS;
        }

        /// <summary>
        /// Procesa una operación PaP FXI ejecutando las validaciones de
        /// estatus, actualizaciones en CMS y consolidación de resultados
        /// asociados al folio maestro.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_PaP_FXI()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                bool bProcesado = true;


                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;


                dicParametros = new Dictionary<string, object>() { { "Folio", _Operacion.StrFolio } };
                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_PaP_FXI, dicParametros);
                DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                if (datoTomi == null)
                    throw new Exception("No se encontró el registro en TOMI");

                if (strAliasProceso.Equals(ConstantesServicios.AliasValidaEstatusTOMI))
                {

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Confirmando el estatus de la operación en TOMI, estatus={datoTomi.Nf_estatus}.");


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_FXI, dicParametros2);

                    switch (datoTomi.Nf_estatus)
                    {
                        case ConstantesServicios.TomiEstatusPROC:
                        case ConstantesServicios.TomiEstatusAUAB:
                        case ConstantesServicios.TomiEstatusRECC:
                            break;
                        default:
                            return true;

                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");




                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSwitfEstatus))
                {

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        bProcesado = true;
                    }
                    else
                    {
                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_FXI, dicParametros2);

                        bProcesado = false;

                        servicio = servicio ?? CrearServicioGeneral();
                        string strEstatusSwift = await servicio.Dame_Estatus_Swift(datoTomi.T24_transaction_id);

                        if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusPROC))
                            bProcesado = true;
                        else
                            throw new Exception($"Error Operación Viva en T24 y Swift con estatus cancelado; folio:{_Operacion.StrFolio}");

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Swift Estatus --, estatus={datoTomi.Nf_estatus}.");
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|En espera de liberación de mensaje.");
                        return false;
                    }

                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 3;

                    bProcesado = await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_FXI, dicParametros2);

                    if (!bProcesado)
                        throw new Exception($"No fue posible actualizar el plan de trabajo maestro para el folio {_Operacion.StrFolio}");

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiNFDTO> datosDetalleTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_Detalle_PaP_FXI, dicParametros);
                    datoTomi = datosDetalleTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    var dicParametros3 = new Dictionary<string, object>()
                        {
                            {"folio", _Operacion.StrFolio },
                            {"opcion", 1 }
                        };

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {
                        dicParametros = new Dictionary<string, object>()
                        {
                            {"folio", _Operacion.StrFolio },
                            {"sFT", datoTomi.T24_transaction_id }
                        };

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_PaP_FXI, dicParametros);

                        

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinDetalle_PaP_FXI, dicParametros3);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando Actualización de acpetación de operación.");

                        bProcesado = true;

                        dicParametros2["opcion"] = 4;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_FXI, dicParametros2);


                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        dicParametros = new Dictionary<string, object>()
                        {
                            {"folio", _Operacion.StrFolio },
                            {"razonrechazo", datoTomi.Nf_razon}
                        };

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoRechazadoCms_PaP_FXI, dicParametros);

                        dicParametros3["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinDetalle_PaP_FXI, dicParametros3);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando Actualización de rechazo de operación.");

                        bProcesado = true;

                        dicParametros2["opcion"] = 5;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_FXI, dicParametros2);

                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina Procesamiento de Folio: {_Operacion.StrFolio}");

                    if (bProcesado)
                    {
                        servicio = servicio ?? CrearServicioGeneral();
                        string strFolioMaestro = await servicio.DameFolioMaestro(_Operacion.StrFolio, _Operacion.StrTipoOperacion);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Buscando ACtualizar el Maestro: {strFolioMaestro}");

                        dicParametros = new Dictionary<string, object>() { { "Folio", strFolioMaestro } };

                        List<DatoPaPDetalleDTO> datosPap = await _IParametrosOperaciones.EjecutarSP<DatoPaPDetalleDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_DetalleMaestro_PaP_FXI, dicParametros);
                        DatoPaPDetalleDTO datoPap = datosPap?.FirstOrDefault();

                        bool bAprobado = datoPap != null && (datoPap.Procesados + datoPap.Devueltos) == datoPap.Regs_totales;

                        var dicParametros4 = new Dictionary<string, object>()
                            {
                                {"folio", strFolioMaestro },
                                {"procesados", datoPap?.Procesados },
                                {"devueltos", datoPap?.Devueltos },
                                {"opcion", 1 }
                            };

                        if (bAprobado)
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizar maestro como aprobado, Folio Maestro: {strFolioMaestro}");

                            

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizarFormatoCms_DetalleMaestro_PaP_FXI, dicParametros4);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarFormatoTomi_DetalleMaestro_PaP_FXI, dicParametros4);


                            dicParametros2["opcion"] = 6;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_PaP_FXI, dicParametros2);

                        }
                        else
                        {
                            dicParametros4["opcion"] = 2;

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizarFormatoCms_DetalleMaestro_PaP_FXI, dicParametros4);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarFormatoTomi_DetalleMaestro_PaP_FXI, dicParametros4);
                        }

                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");

                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio {_Operacion.StrFolio}");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_PaP_FXI|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones PaP FXD,
    /// incluyendo la validación de estatus en SPEI, actualización en CMS
    /// y consolidación de resultados en el folio maestro.
    /// </summary>
    public class Servicio_PaP_FXD : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio PaP_FXD.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionCMS">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_PaP_FXD(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionCMS, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionCMS;
        }

        /// <summary>
        /// Procesa una operación PaP FXD ejecutando las validaciones de
        /// estatus, actualizaciones en CMS y consolidación de resultados
        /// asociados al folio maestro.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_PaP_FXD()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                bool bProcesado = true;


                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"tipo_operacion", _Operacion.StrTipoOperacion },
                        {"opcion" , 1 }
                    };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro(estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso((new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }));  //Inicia proceso

                    

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_FXD, dicParametros2);
                    await _IParametrosOperaciones.TerminarProceso((new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }));
                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPEIEstatus))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operación en SPEI.");

                    await _IParametrosOperaciones.IniciarProceso((new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    })); //Inicia proceso

                    servicio = servicio ?? CrearServicioGeneral();
                    EstatusSPEIModel resultadoSpei = await servicio.ValidaEstatusSPEI(ConstantesServicios.TbTombinNFFxdPaPDetalle, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);
                    bProcesado = resultadoSpei?.BResultado ?? false;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso((new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        })); //Termina Proceso
                    }
                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS.");

                    await _IParametrosOperaciones.IniciarProceso((new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }));  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_FXD, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.Actualiza_CMS_origen_SPEI(ConstantesServicios.TbTombinNFFxdPaPDetalle, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbFormatoPapDetalle);


                    if (bProcesado)
                    {
                        string strFolioMaestro = await servicio.DameFolioMaestro(_Operacion.StrFolio, _Operacion.StrTipoOperacion);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Buscando ACtualizar el Maestro: {strFolioMaestro}");

                        dicParametros = new Dictionary<string, object>() { { "Folio", strFolioMaestro } };

                        List<DatoPaPDetalleDTO> datosPap = await _IParametrosOperaciones.EjecutarSP<DatoPaPDetalleDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_PaP_FXD, dicParametros);
                        DatoPaPDetalleDTO datoPap = datosPap?.FirstOrDefault();

                        bool bAprobado = datoPap != null && (datoPap.Procesados + datoPap.Devueltos) == datoPap.Regs_totales;

                        var dicParametros3 = new Dictionary<string, object>()
                            {
                                {"folio", strFolioMaestro },
                                {"procesadas", datoPap?.Procesados },
                                {"devueltas", datoPap?.Devueltos },
                                {"opcion", 1 }
                            };

                        if (bAprobado)
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizar maestro como aprobado, Folio Maestro: {strFolioMaestro}");

                            

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoCms_PaP_FXD, dicParametros3);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaFormatoTomi_PaP_FXD, dicParametros3);

                            dicParametros2["opcion"] = 3;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_PaP_FXD, dicParametros2);

                        }
                        else
                        {

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoCms_PaP_FXD, dicParametros3);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaFormatoTomi_PaP_FXD, dicParametros3);
                        }

                        await _IParametrosOperaciones.TerminarProceso((new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        })); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");

                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio {_Operacion.StrFolio}");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_PaP_FXD|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones de Nómina FXD,
    /// incluyendo la validación de estatus en SPEI, la actualización de
    /// información en CMS y la consolidación de resultados en el folio maestro.
    /// </summary>
    public class Servicio_NOM_FXD : ServicioBase
    {
        private readonly IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio NOM_FXD.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionCMS">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_NOM_FXD(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionCMS, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionCMS;
        }

        /// <summary>
        /// Procesa una operación de Nómina FXD ejecutando las validaciones
        /// de estatus, actualizaciones en CMS y consolidación de resultados
        /// sobre el folio maestro de la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_NOM_FXD()
        {
            try 
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc).");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso



                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_NOM_FXD, dicParametros2);

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPEIEstatus))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operación en SPEI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_NOM_FXD, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    EstatusSPEIModel resultadoSpei = await servicio.ValidaEstatusSPEI(ConstantesServicios.TbTombinNFFXDNominaDetalle, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    bool bProcesado = resultadoSpei?.BResultado ?? false;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }

                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en CMS.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 3;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_NOM_FXD, dicParametros2);

                    dicParametros2["opcion"] = 4;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_NOM_FXD, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bool bProcesado = await servicio.Actualiza_CMS_origen_SPEI(ConstantesServicios.TbTombinNFFXDNominaDetalle, _Operacion.StrFolio, _Operacion.StrTipoOperacion, ConstantesServicios.TbFormatoNominaDetalle);

                    if (bProcesado)
                    {
                        string strFolioMaestro = await servicio.DameFolioMaestro(_Operacion.StrFolio, _Operacion.StrTipoOperacion);

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Buscando ACtualizar el Maestro: {strFolioMaestro}");

                        dicParametros = new Dictionary<string, object>() { { "folio", strFolioMaestro } };

                        List<DatoPaPDetalleDTO> datosNomina = await _IParametrosOperaciones.EjecutarSP<DatoPaPDetalleDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_NOM_FXD, dicParametros);
                        DatoPaPDetalleDTO datoNomina = datosNomina?.FirstOrDefault();

                        bool bAprobado = datoNomina != null && (datoNomina.Procesados + datoNomina.Devueltos) == datoNomina.Regs_totales;

                        var dicParametros3 = new Dictionary<string, object>()
                        {
                            {"folio", strFolioMaestro },
                            {"procesadas", datoNomina ?.Procesados },
                            {"devueltas", datoNomina ?.Devueltos },
                            {"opcion", 1 }
                        };

                        if (bAprobado)
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizar maestro como aprobado, Folio Maestro: {strFolioMaestro}");

                            

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoCms_NOM_FXD, dicParametros3);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaFormatoTomi_NOM_FXD, dicParametros3);

                            dicParametros2["opcion"] = 5;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_NOM_FXD, dicParametros2);

                            dicParametros = new Dictionary<string, object>()
                        {
                            {"folio", strFolioMaestro },
                            {"@sFT", string.Empty }
                        };

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_InsertarFormatoCms_NOM_FXD, dicParametros);
                        }
                        else
                        {
                            dicParametros3["opcion"] = 2;

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoCms_NOM_FXD, dicParametros3);

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaFormatoTomi_NOM_FXD, dicParametros3);
                        }

                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio {_Operacion.StrFolio}");
                }

                return true;
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_NOM_FXD|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones de Mercado Dinero,
    /// incluyendo la validación de estatus en SPEI y SPID, así como la
    /// actualización de los procesos asociados en TOMI.
    /// </summary>
    public class Servicio_MercadoDinero : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio Mercado Dinero.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_MercadoDinero(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación de Mercado Dinero ejecutando las validaciones
        /// de estatus y las actualizaciones correspondientes de acuerdo con el
        /// proceso pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionMercadoDinero()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                string stsSPEI = string.Empty;

                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                if (strAliasProceso.Equals(ConstantesServicios.AliasDatosGeneral))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc)");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    bProcesado = false;

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPEIEstatus))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operación en SPEI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_MercadoDinero, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    EstatusSPEIModel resultadoSpei = await servicio.ValidaEstatusSPEI(ConstantesServicios.TbCalypsoLiquidacion, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    bProcesado = resultadoSpei?.BResultado ?? false;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        });  //Inicia proceso
                    }
                    else
                    {
                        if (ConstantesServicios.OperacionMDSPEIS.Equals(_Operacion.StrTipoOperacion) && (stsSPEI.Equals(ConstantesServicios.SPEICl) || stsSPEI.Equals(ConstantesServicios.SPEIRl)))
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando la operación por rechazo local - {stsSPEI}");

                            await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                            {
                                Folio = _Operacion.StrFolio,
                                TipoOperacion = _Operacion.StrTipoOperacion,
                                NombreProceso = _Configuracion.NombreProceso,
                                AliasProceso = strAliasProceso,
                                Servicio = _Configuracion.NombreServicio,
                                PId = (int)_Configuracion.Pid,
                                Excepcion = new Exception("")
                            });  //Inicia proceso
                        }
                    }

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSPIDEstatus))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|A verificar el estatus de la operación en SPID");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 2;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_MercadoDinero, dicParametros2);

                    servicio = servicio ?? CrearServicioGeneral();
                    bProcesado = await servicio.ValidaEstatus_SPID(ConstantesServicios.TbCalypsoLiquidacion, ConstantesServicios.CampoNFFolio, _Operacion.StrFolio, _Operacion.StrTipoOperacion);

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                    }

                    strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {
                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualiza el estatus como operación terminada.");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 3;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_MercadoDinero, dicParametros2);

                    bProcesado = true;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");
                    }
                    else
                        throw new Exception(string.Format("Error en procesamiento de registro ESTATUS_TERMINADA con folio: {0}.", _Operacion.StrFolio));
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionMercadoDinero|{ex}");
                return false;
            }

            
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones MFX,
    /// incluyendo la actualización de estatus en CMS y la sincronización
    /// de información entre TOMI y los sistemas relacionados.
    /// </summary>
    public class Servicio_MFX : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio MFX.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesCms">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_MFX(OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionesCms) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionesCms;
        }

        /// <summary>
        /// Procesa una operación MFX ejecutando las actualizaciones
        /// correspondientes en CMS, TOMI y Plan de Trabajo Maestro
        /// de acuerdo con el estatus de la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionMFX()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;


                string FolioCMS = string.Empty;

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {

                    Config.EscribirLog("INFO|ProcesarOperacionMFX|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_MXF, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "Folio", _Operacion.StrFolio } };

                    List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_MFX, dicParametros);
                    DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                    if (string.IsNullOrEmpty(datoTomi?.NF_Folio))
                        throw new Exception("No se encontró relación en TOMI para el folio");


                    Config.EscribirLog($"INFO|ProcesarOperacionMFX|folioCMS={datoTomi.NF_Folio}");

                    FolioCMS = datoTomi.NF_Folio;

                    dicParametros = new Dictionary<string, object>() { { "Folio", FolioCMS } };

                    datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNFEstatus_MFX, dicParametros);
                    datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    switch (datoTomi.Nf_estatus)
                    {
                        case ConstantesServicios.TomiEstatusCONP:
                        case ConstantesServicios.TomiEstatusCONF:
                        case ConstantesServicios.TomiEstatusPROC:

                            dicParametros = new Dictionary<string, object>() { { "Folio", FolioCMS } };

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_MXF, dicParametros);

                            dicParametros = new Dictionary<string, object>() { { "Folio", FolioCMS }, { "Opcion", 1 } };

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinNF_MXF, dicParametros);

                            Config.EscribirLog($"INFO|ProcesarOperacionMFX|Terminado actualización de operación");

                            dicParametros2["opcion"] = 2;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_MXF, dicParametros2);

                            break;
                        case ConstantesServicios.TomiEstatusRECC:

                            dicParametros = new Dictionary<string, object>() { { "Folio", FolioCMS } };

                            await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoRechazadoCms_MXF, dicParametros);

                            dicParametros = new Dictionary<string, object>() { { "Folio", FolioCMS }, { "Opcion", 2 } };

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinNF_MXF, dicParametros);

                            Config.EscribirLog($"INFO|ProcesarOperacionMFX|Terminado actualización de rechazo de operación");

                            dicParametros2["opcion"] = 3;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_MXF, dicParametros2);

                            break;
                        default:
                            throw new Exception($"No se encontró el estatus '{datoTomi.Nf_estatus}' ");
                    }

                    Config.EscribirLog($"INFO|ProcesarOperacionMFX|Se termina procesamiento del folio: {_Operacion.StrFolio}");

 
                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog("INFO|FXT.procesaOperacion|Operación Finalizada.");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionMFX|{ex}");
                return false;
            }

            
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXT_H2H,
    /// incluyendo la actualización de estatus y el cierre de la
    /// operación en el Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_FXT_H2H : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXT_H2H.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXT_H2H(OperacionDTO operacion, IParametrosOperaciones parametros) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXT_H2H ejecutando las actualizaciones
        /// correspondientes de acuerdo con el proceso pendiente registrado
        /// para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_FXT_H2H()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog("INFO|ProcesarOperacion_FXT_H2H|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_H2H, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_FXT_H2H, dicParametros);
                    DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();


                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando actualización de aceptación de operación");

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_H2H, dicParametros2);

                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando actualización de rechazo de operación");
                        dicParametros2["opcion"] = 3;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_H2H, dicParametros2);
                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando procesamiento de folio: {_Operacion.StrFolio}");

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");


                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_FXT_H2H|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXT_GPH,
    /// incluyendo la actualización de estatus y el cierre de la
    /// operación en el Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_FXT_GPH : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXT_GPH.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXT_GPH(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXT_GPH ejecutando las actualizaciones
        /// correspondientes de acuerdo con el proceso pendiente registrado
        /// para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_FXT_GPH()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog("INFO|ProcesarOperacion_FXT_GPH|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion" , 1 }
                };

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_GPH, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_FXT_GPH, dicParametros);
                    DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {
                        Config.EscribirLog("INFO|ProcesarOperacion_FXT_GPH|Terminando actualización de aceptación de operación");

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_GPH, dicParametros2);
                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        Config.EscribirLog("INFO|ProcesarOperacion_FXT_GPH|Terminando actualización de rechazo de operación");
                        dicParametros2["opcion"] = 3;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_GPH, dicParametros2);
                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando procesamiento de folio: {_Operacion.StrFolio}");

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog("INFO|ProcesarOperacion_FXT_GPH|Operación Finalizada.");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_FXT_GPH|{ex}");
                return false;
            }

            
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXT Local,
    /// incluyendo la actualización de estatus y el cierre de la
    /// operación en el Plan de Trabajo Maestro.
    /// </summary>
    public class Servicio_FXTLocal : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXT Local.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXTLocal(OperacionDTO operacion, IParametrosOperaciones parametros) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXT Local ejecutando las actualizaciones
        /// correspondientes de acuerdo con el proceso pendiente registrado
        /// para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionServicio_FXTLocal()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog("INFO|TMD.procesaOperacion|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_Local, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_FXT_Local, dicParametros);
                    DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro en TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {
                        Config.EscribirLog("INFO|TMD.procesaOperacion|Terminando actualización de aceptación de operación");

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_Local, dicParametros2);

                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        Config.EscribirLog("INFO|TMD.procesaOperacion|Terminando actualización de rechazo de operación");

                        dicParametros2["opcion"] = 3;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT_Local, dicParametros2);

                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminando procesamiento de folio: {_Operacion.StrFolio}");


                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog("INFO|TMD.procesaOperacion|Operación Finalizada.");


                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionServicio_FXTLocal|{ex}");
                return false;
            }

        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using ConstantesServicios = Aplicacion.Enums.ConstantesServicios;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXT,
    /// incluyendo la actualización de información en CMS y la
    /// finalización del flujo de la operación.
    /// </summary>
    public class Servicio_FXT : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio FXT.
        /// </summary>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesCMS">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_FXT(OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionesCMS) 
        {
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionesCMS;
        }

        /// <summary>
        /// Procesa una operación FXT ejecutando las actualizaciones
        /// correspondientes en CMS y Plan de Trabajo Maestro de acuerdo
        /// con el proceso pendiente registrado.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionFXT()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {
                    Config.EscribirLog("INFO|FXT.procesaOperacion|Actualizando el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    var dicParametros2 = new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"tipo_operacion", _Operacion.StrTipoOperacion },
                        {"opcion" , 1 }
                    };

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT, dicParametros2);

                    dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                    List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_FXT, dicParametros);
                    DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                    if (datoTomi == null)
                        throw new Exception("No se encontró el registro TOMI");

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus))
                    {
                        dicParametros = new Dictionary<string, object>()
                        {
                            {"folio", _Operacion.StrFolio },
                            {"sFT", datoTomi.T24_transaction_id }
                        };

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_FXT, dicParametros);

                        Config.EscribirLog("INFO|FXT.procesaOperacion|Terminado actualización de aceptación de operación");

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT, dicParametros2);

                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        dicParametros = new Dictionary<string, object>()
                        {
                            {"folio", _Operacion.StrFolio },
                            {"razonrechazo", datoTomi.Nf_razon }
                        };

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoRechazadoCms_FXT, dicParametros);

                        Config.EscribirLog("INFO|FXT.procesaOperacion|Terminado actualización de rechazo de operación");

                        dicParametros2["opcion"] = 3;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXT, dicParametros2);

                    }

                    Config.EscribirLog($"INFO|FXT.procesaOperacion|Se termina procesamiento de folio {_Operacion.StrFolio}");

                    await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    }); //Termina Proceso

                    Config.EscribirLog("INFO|FXT.procesaOperacion|Operación Finalizada.");
                    

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionFXT|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXI_H2H,
    /// incluyendo la validación de estatus en TOMI, SWIFT y la
    /// actualización de información en CMS.
    /// </summary>
    public class Servicio_FXI_H2H : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXI_H2H.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXI_H2H(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXI_H2H ejecutando las validaciones y
        /// actualizaciones correspondientes de acuerdo con el proceso
        /// pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_FXI_H2H()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);



                bool bProcesado = true;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };
                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_FXI_H2H, dicParametros);
                DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                if (datoTomi == null)
                    throw new Exception("No se encontró el registro en TOMI");

                if (strAliasProceso.Equals(ConstantesServicios.AliasValidaEstatusTOMI))
                {

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Confirmando el estatus de la operación en TOMI, estatus={datoTomi.Nf_estatus}.");


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_H2H, dicParametros2);

                    switch (datoTomi.Nf_estatus)
                    {
                        case ConstantesServicios.TomiEstatusPROC:
                        case ConstantesServicios.TomiEstatusAUAB:
                        case ConstantesServicios.TomiEstatusRECC:
                            return true;
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSwitfEstatus))
                {

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                        bProcesado = true;
                    else
                    {

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_H2H, dicParametros2);

                        bProcesado = false;

                        servicio = servicio ?? CrearServicioGeneral();
                        string strEstatusSwift = await servicio.Dame_Estatus_Swift(datoTomi.T24_transaction_id);

                        if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusPROC))
                        {
                            bProcesado = true;
                        }
                        else if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusRECC))
                        {
                            throw new Exception($"Error Operación viva en T24 y Swift con estatus cancelado, folio: {_Operacion.StrFolio}");
                        }

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|SWIFT Status ---{datoTomi.Nf_fecha_autoriza}.");
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|En espera de liberación de mensaje.");
                        return false;
                    }
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizandp el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 3;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_H2H, dicParametros2);


                    switch (datoTomi.Nf_estatus)
                    {
                        case ConstantesServicios.TomiEstatusPROC:
                        case ConstantesServicios.TomiEstatusAUAB:

                            dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinNF_FXI_H2H, dicParametros);

                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminado actualización de aceptación de operación.");

                            bProcesado = true;

                            dicParametros2["opcion"] = 4;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_H2H, dicParametros2);

                            break;
                        case ConstantesServicios.TomiEstatusRECC:
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminado actualización de rechazo de operación.");

                            bProcesado = true;

                            dicParametros2["opcion"] = 5;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_H2H, dicParametros2);

                            break;
                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina procesamiento de folio:{_Operacion.StrFolio}.");

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}.");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_FXI_H2H|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXI_GPH,
    /// incluyendo la validación de estatus en TOMI, SWIFT y la
    /// actualización de información en CMS.
    /// </summary>
    public class Servicio_FXI_GPH : ServicioBase
    {

        /// <summary>
        /// Inicializa una nueva instancia del servicio FXI_GPH.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXI_GPH(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXI_GPH ejecutando las validaciones y
        /// actualizaciones correspondientes de acuerdo con el proceso
        /// pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacion_FXI_GPH()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);

                string strEstatusSwift = string.Empty;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                bool bProcesado = true;

                dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_FXI_GPH, dicParametros);
                DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                if (datoTomi == null)
                    throw new Exception("No se encontró el registro en TOMI");


                if (strAliasProceso.Equals(ConstantesServicios.AliasValidaEstatusTOMI))
                {

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso
                     

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXI_GPH, dicParametros2);

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Confirmando el estatus de la operación en TOMI, estatus={datoTomi.Nf_estatus}.");

                    switch (datoTomi.Nf_estatus)
                    {
                        case ConstantesServicios.TomiEstatusPROC:
                        case ConstantesServicios.TomiEstatusAUAB:
                        case ConstantesServicios.TomiEstatusRECC:
                            return true;
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                        throw new Exception($"Eror en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSwitfEstatus))
                {

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        bProcesado = true;
                    }
                    else
                    {
                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXI_GPH, dicParametros2);

                        bProcesado = false;

                        servicio = servicio ?? CrearServicioGeneral();
                        strEstatusSwift = await servicio.Dame_Estatus_Swift(datoTomi.T24_transaction_id);

                        if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusPROC))
                        {
                            bProcesado = true;
                        }
                        else if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusRECC))
                        {
                            throw new Exception($"Error Operación viva en T24 y Swift con estatus cancelado, folio: {_Operacion.StrFolio}");
                        }

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|SWIFT Status ---{datoTomi.Nf_estatus}.");
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|En espera de liberación de mensaje.");
                        return false;
                    }
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizandp el estatus como operación terminada en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 3;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXI_GPH, dicParametros2);


                    switch (datoTomi.Nf_estatus)
                    {
                        case ConstantesServicios.TomiEstatusPROC:
                        case ConstantesServicios.TomiEstatusAUAB:

                            dicParametros = new Dictionary<string, object>() { { "Folio", _Operacion.StrFolio } };

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinNF_FXI_GPH, dicParametros);

                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminado actualización de aceptación de operación.");

                            bProcesado = true;

                            dicParametros2["opcion"] = 4;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXI_GPH, dicParametros2);

                            break;
                        case ConstantesServicios.TomiEstatusRECC:
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Terminado actualización de rechazo de operación.");

                            dicParametros2["opcion"] = 5;

                            await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo_Maestro_FXI_GPH, dicParametros2);

                            break;
                    }

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina procesamiento de folio:{_Operacion.StrFolio}.");

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso
                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada.");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}.");

                }


                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacion_FXI_GPH|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXI Local,
    /// incluyendo la validación de estatus en TOMI, SWIFT y la
    /// actualización de información correspondiente en TOMI.
    /// </summary>
    public class Servicio_FXILocal : ServicioBase
    {
        /// <summary>
        /// Inicializa una nueva instancia del servicio FXI Local.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        public Servicio_FXILocal(ConfiguracionServicio configuracion,OperacionDTO operacion, IParametrosOperaciones parametros, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
        }

        /// <summary>
        /// Procesa una operación FXI Local ejecutando las validaciones
        /// y actualizaciones correspondientes de acuerdo con el proceso
        /// pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionServicio_FXILocal()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                bool bProcesado = true;

                string strEstatusSwift = string.Empty;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };
                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_FXI_Local, dicParametros);
                DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                if (datoTomi == null)
                    throw new Exception("No se encontró el registro en TOMI");

                if (strAliasProceso.Equals(ConstantesServicios.AliasValidaEstatusTOMI))
                {

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Confirmando el estatus de la operación en TOMI, estatus={datoTomi.Nf_estatus}.");


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_Local, dicParametros2);

                    if (!(datoTomi.Nf_estatus == ConstantesServicios.TomiEstatusPROC || datoTomi.Nf_estatus == ConstantesServicios.TomiEstatusAUAB || datoTomi.Nf_estatus == ConstantesServicios.TomiEstatusRECC))
                        return true;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                        throw new Exception($"Eror en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSwitfEstatus))
                {

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso


                    if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                        bProcesado = true;
                    else
                    {

                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_Local, dicParametros2);

                        bProcesado = false;

                        servicio = servicio ?? CrearServicioGeneral();
                        strEstatusSwift = await servicio.Dame_Estatus_Swift(datoTomi.T24_transaction_id);

                        if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusPROC))
                        {
                            bProcesado = true;
                        }
                        else if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusRECC))
                        {
                            throw new Exception($"Error Operación viva en T24 y Swift con estatus cancelado, folio: {_Operacion.StrFolio}");
                        }

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|SWIFT Status ---{datoTomi.Nf_estatus}.");

                        if (bProcesado)
                        {
                            await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                            {
                                Folio = _Operacion.StrFolio,
                                TipoOperacion = _Operacion.StrTipoOperacion,
                                NombreProceso = _Configuracion.NombreProceso,
                                AliasProceso = strAliasProceso,
                                Servicio = _Configuracion.NombreServicio,
                                PId = (int)_Configuracion.Pid,
                                Excepcion = new Exception("")
                            }); //Termina Proceso

                            strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                        }
                        else
                        {
                            Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|En espera de liberación de mensaje.");
                            return false;
                        }

                    }
                }

                if (strAliasProceso.Equals(ConstantesServicios.EstatusTerminada))
                {

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Actualizando el estatus como operación terminada en TOMI");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 3;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_Local, dicParametros2);

                    Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Se termina Procesamiento de Folio {_Operacion.StrFolio}");

                    if (bProcesado)
                    {
                        switch (datoTomi.Nf_estatus)
                        {
                            case ConstantesServicios.TomiEstatusPROC:
                            case ConstantesServicios.TomiEstatusAUAB:
                                dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                                await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinNF_FXI_Local, dicParametros);

                                dicParametros2["opcion"] = 4;

                                await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_Local, dicParametros2);

                                break;
                            case ConstantesServicios.TomiEstatusRECC:


                                dicParametros2["opcion"] = 5;

                                await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI_Local, dicParametros2);

                                break;
                        }

                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog($"INFO|{_Operacion.StrTipoOperacion}.procesaOperacion|Operación Finalizada");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA con folio: {_Operacion.StrFolio}.");

                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionServicio_FXILocal|{ex}");
                return false;
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
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Servicio encargado del procesamiento de operaciones FXI,
    /// incluyendo la validación de estatus en TOMI, SWIFT y la
    /// actualización de información en CMS.
    /// </summary>
    public class Servicio_FXI : ServicioBase
    {
        IOperacionesCms _IOperacionesCms;

        /// <summary>
        /// Inicializa una nueva instancia del servicio FXI.
        /// </summary>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        /// <param name="operacion">
        /// Información de la operación que será procesada.
        /// </param>
        /// <param name="parametros">
        /// Operaciones y parámetros requeridos durante la ejecución.
        /// </param>
        /// <param name="operacionesCMS">
        /// Operaciones relacionadas con CMS.
        /// </param>
        public Servicio_FXI(ConfiguracionServicio configuracion, OperacionDTO operacion, IParametrosOperaciones parametros, IOperacionesCms operacionesCMS, IFabricaServicioGeneral fabricaServicioGeneral)
            : base(fabricaServicioGeneral)
        {
            _Configuracion = configuracion;
            _Operacion = operacion;
            _IParametrosOperaciones = parametros;
            _IOperacionesCms = operacionesCMS;
        }

        /// <summary>
        /// Procesa una operación FXI ejecutando las validaciones y
        /// actualizaciones correspondientes de acuerdo con el proceso
        /// pendiente registrado para la operación.
        /// </summary>
        /// <returns>
        /// True cuando el procesamiento finaliza correctamente;
        /// en caso contrario, false.
        /// </returns>
        public async Task<bool> ProcesarOperacionFXI()
        {
            try
            {
                string strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                bool bProcesado = true;
                string strEstatusSwift = string.Empty;

                if (string.IsNullOrEmpty(strAliasProceso))
                    return false;

                ServicioGeneral servicio = null;

                var dicParametros2 = new Dictionary<string, object>()
                {
                    {"folio", _Operacion.StrFolio },
                    {"tipo_operacion", _Operacion.StrTipoOperacion },
                    {"opcion", 1 }
                };

                dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                List<DatoTomiNFDTO> datosTomi = await _IParametrosOperaciones.EjecutarSP<DatoTomiNFDTO>(ProcedimientoAlmacenado.sp_ConsultarTombinNF_FXI, dicParametros);
                DatoTomiNFDTO datoTomi = datosTomi?.FirstOrDefault();

                if (datoTomi == null)
                    throw new Exception("No se encontró el registro TOMI");


                if (strAliasProceso.Equals(ConstantesServicios.AliasValidaEstatusTOMI))
                {
                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    Config.EscribirLog(string.Format("INFO|FXI.procesaOperacion|Confirmando el estatus de la operacion en TOMI, estatus={0} ", datoTomi.Nf_estatus));


                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI, dicParametros2);

                    if (!(ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus) || ConstantesServicios.TomiEstatusAUAB.Equals(datoTomi.Nf_estatus) || ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus)))
                        return true;

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TERMINADA_CMS con folio: {_Operacion.StrFolio}");
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasSwitfEstatus))
                {
                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        bProcesado = true;
                    }
                    else
                    {
                        dicParametros2["opcion"] = 2;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI, dicParametros2);


                        bProcesado = false;

                        servicio = servicio ?? CrearServicioGeneral();
                        strEstatusSwift = await servicio.Dame_Estatus_Swift(datoTomi.T24_transaction_id);

                        if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusPROC))
                            bProcesado = true;

                        if (strEstatusSwift.Equals(ConstantesServicios.TomiEstatusRECC))
                            throw new Exception($"Error Operación viva en T24 y Swift con estatus cancelado, folio: {_Operacion.StrFolio}");

                        Config.EscribirLog(string.Format("INFO|{0}.prcesaOperacion|Swift Estatus --- {0}", strEstatusSwift));
                    }

                    if (bProcesado)
                    {
                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        strAliasProceso = await _IParametrosOperaciones.DameUltimoProceso(_Operacion);
                    }
                    else
                    {
                        Config.EscribirLog("INFO|{0}.prcesaOperacion|En espera de liberación de mensaje.");
                        return false;
                    }
                }

                if (strAliasProceso.Equals(ConstantesServicios.AliasEstatusTerminadaCMS))
                {
                    Config.EscribirLog("INFO|FXI.procesaOperacion|Actualizando el estatus como operación en CMS");

                    await _IParametrosOperaciones.IniciarProceso(new ProcesoDTO()
                    {
                        Folio = _Operacion.StrFolio,
                        TipoOperacion = _Operacion.StrTipoOperacion,
                        NombreProceso = _Configuracion.NombreProceso,
                        AliasProceso = strAliasProceso,
                        Servicio = _Configuracion.NombreServicio,
                        PId = (int)_Configuracion.Pid,
                        Excepcion = new Exception("")
                    });  //Inicia proceso

                    dicParametros2["opcion"] = 3;

                    await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI, dicParametros2);

                    if (ConstantesServicios.TomiEstatusPROC.Equals(datoTomi.Nf_estatus) || ConstantesServicios.TomiEstatusAUAB.Equals(datoTomi.Nf_estatus))
                    {
                        dicParametros = new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"lsft", datoTomi.T24_transaction_id }
                    };

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoAceptadoCms_FXI, dicParametros);

                        dicParametros = new Dictionary<string, object>() { { "folio", _Operacion.StrFolio } };

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizaTombinDetalle_FXI, dicParametros);

                        Config.EscribirLog("INFO|FXI.procesaOperacion|Terminado Actualizacíón de aceptación de operación");

                        bProcesado = true;

                        dicParametros2["opcion"] = 4;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI, dicParametros2);


                    }
                    else if (ConstantesServicios.TomiEstatusRECC.Equals(datoTomi.Nf_estatus))
                    {
                        dicParametros = new Dictionary<string, object>()
                    {
                        {"folio", _Operacion.StrFolio },
                        {"razonRechazo", datoTomi.Nf_razon}
                    };

                        await _IOperacionesCms.EjecutaSPTransaccion(ProcedimientoAlmacenado.sp_ActualizaFormatoRechazadoCms_FXI, dicParametros);

                        Config.EscribirLog("INFO|FXI.procesaOperacion|Terminado Actualizacíón de recazo de operación");

                        bProcesado = true;

                        dicParametros2["opcion"] = 5;

                        await _IParametrosOperaciones.EjecutarSP(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajo_Maestro_FXI, dicParametros2);

                    }

                    Config.EscribirLog($"INFO|FXI.procesaOperacion|Se termina procesamiento de folio: {_Operacion.StrFolio}");


                    if (bProcesado)
                    {

                        await _IParametrosOperaciones.TerminarProceso(new ProcesoDTO()
                        {
                            Folio = _Operacion.StrFolio,
                            TipoOperacion = _Operacion.StrTipoOperacion,
                            NombreProceso = _Configuracion.NombreProceso,
                            AliasProceso = strAliasProceso,
                            Servicio = _Configuracion.NombreServicio,
                            PId = (int)_Configuracion.Pid,
                            Excepcion = new Exception("")
                        }); //Termina Proceso

                        Config.EscribirLog("INFO|FXI.procesaOperacion|Operación finalizada.");
                    }
                    else
                        throw new Exception($"Error en procesamiento de registro ESTATUS_TTERMINADA_CMS con folio: {_Operacion.StrFolio}");

                }


                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionFXI|{ex}");
                return false;
            }

            
        }
    }
}

