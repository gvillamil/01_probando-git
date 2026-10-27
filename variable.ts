using AplicacionSwfOUTMQGCMS.Enums;
using AplicacionSwfOUTMQGCMS.Interfaces;
using AplicacionSwfOUTMQGCMS.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using IOperacionServicio = AplicacionSwfOUTMQGCMS.Interfaces.IOperacionServicio;

namespace AplicacionSwfOUTMQGCMS
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
        private readonly IRepositorioTomi<OperacionSWF> _repositorioTomi;

        /// <summary>
        /// Constructor principal de Operaciones.
        /// </summary>
        /// <param name="configuracion"></param>
        /// <param name="controlServicio"></param>
        /// <param name="alertas"></param>
        /// <param name="parametrizacion"></param>
        /// <param name="repositorioTomi"></param>
        public OperacionServicio(Configuracion configuracion,
            IControlServicio controlServicio,
            IAlertas alertas,
            IParametrizacion parametrizacion,
            IRepositorioTomi<OperacionSWF> repositorioTomi)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _alertas = alertas;
            _parametrizacion = parametrizacion;
            _repositorioTomi = repositorioTomi;
        }

        /// <summary>
        /// Método principal de ejecución de procesos de servicio SwfIOUTMQGCMS.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> EjecutarProcedimientosAsync(CancellationToken ct)
        {
            try
            {
                await InicializarContextoAsync(ct).ConfigureAwait(false);

                if (!_configuracion.ParametrosSistema.TryGetValue(ConstantesServicio.SwfOutGetMtBatch, out string gbMensajesBatch))
                {
                    LogError(ConstantesServicio.MensajeBatch);
                    return false;
                }


                if (string.IsNullOrWhiteSpace(gbMensajesBatch))
                {
                    LogError(ConstantesServicio.MensajeBatch);
                    return false;
                }
                bool esActivo = gbMensajesBatch == ConstantesParametroSistema.ValorBatchActivo;

                var operaciones = await _repositorioTomi.RecuperaOperacionesAsync(ct).ConfigureAwait(false);
                var listaOperaciones = operaciones?.ToList();

                if (listaOperaciones == null || listaOperaciones.Count == 0)
                {
                    LogInfo(ConstantesServicio.MensajeOperaciones);
                    return false;
                }

                if (esActivo)
                {
                    return await ProcesarOperacionesAsync(operaciones, ct).ConfigureAwait(false);
                }
                else
                {
                    if (!await EsDiaHabilAsync().ConfigureAwait(false))
                        return false;

                    if (!await EstaEnHorarioOperacionAsync().ConfigureAwait(false))
                        return false;

                    return await ProcesarOperacionesAsync(operaciones, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                LogError($"{ConstantesClaveAviso.Error} en {ConstantesServicio.MensajeEjecuta}: {ex}");
                await RegistrarAlertaAsync(ConstantesClaveAviso.Critico, ex).ConfigureAwait(false);
                return false;
            }
            finally
            {
                try
                {
                    await _controlServicio
                        .ActualizaTerminacion(
                            _configuracion.NombreServicio,
                            ct)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogError(
                        $"Error en ActualizaTerminacion: {ex}");
                }
            }
        }

        #region Inicialización
        /// <summary>
        /// Método para inicializar variables y parametros de confirguración.
        /// </summary>
        /// <returns></returns>
        private async Task InicializarContextoAsync(CancellationToken ct)
        {
            await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct).ConfigureAwait(false);

            _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync(ct).ConfigureAwait(false);
            _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil().ConfigureAwait(false);
            _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesServicio.ParametrosSistema, ConstantesServicio.ParametroWsT24MwPassword, ct).ConfigureAwait(false);
        }

        #endregion

        #region Validaciones
        /// <summary>
        /// Método para validar si es una habil o inhabil.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> EsDiaHabilAsync()
        {
            if (_configuracion.EsDiaHabil == true) 
                return true;

            LogError(ConstantesServicio.MensajeDiaInhabil);
            await RegistrarAlertaAsync(ConstantesClaveAviso.Error, new Exception(ConstantesServicio.MensajeDiaInhabil), ConstantesServicio.MensajeDiaInhabil).ConfigureAwait(false);
            return false;
        }

        /// <summary>
        /// Validación de horario de Operación.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> EstaEnHorarioOperacionAsync()
        {
            try
            {
                var horaActual = _parametrizacion.ObtenerHoraSistema();
                if (!_configuracion.ParametrosSistema.TryGetValue(ConstantesGenerales.HoraOPInicio, out string inicio))
                {
                    LogError(ConstantesServicio.MensajeHoraInicio);
                    return false;
                }

                if (!_configuracion.ParametrosSistema.TryGetValue(ConstantesGenerales.HoraOPFin, out string fin))
                {
                    LogError(ConstantesServicio.MensajeHoraFin);
                    return false;
                }

                if (string.IsNullOrEmpty(inicio))
                {
                    var mensajeInicio = ConstantesServicio.MensajeHoraInicio;
                    LogError(mensajeInicio);
                    return false;
                }

                if (string.IsNullOrEmpty(fin))
                {
                    var mensajeFin = ConstantesServicio.MensajeHoraFin;
                    LogError(mensajeFin);
                    return false;
                }

                if (!TimeSpan.TryParse(inicio, out var inicioResult))
                {
                    var mensajeR = $"{ConstantesServicio.MensajeHoraInicioError}'{inicio}'.";
                    LogError(mensajeR);
                    return false;
                }

                if (!TimeSpan.TryParse(fin, out var finResult))
                {
                    var mensajeF = $"{ConstantesServicio.MensajeHoraFinError}'{fin}'.";
                    LogError(mensajeF);
                    return false;
                }

                if (inicioResult >= finResult)
                {
                    var mensajeFueraDeHorario = $"El horario configurado no es valido'.";
                    LogError(mensajeFueraDeHorario);
                    return false;
                }

                if (horaActual >= inicioResult && horaActual <= finResult)
                    return true;

                var mensaje = $"El servicio '{_configuracion.NombreServicio}' está fuera de horario";
                LogError(mensaje);
                return false;

            }
            catch (Exception ex)
            {
                var mensaje = $"{ConstantesClaveAviso.Error}|EstaEnHorarioOperacion:|{ex.Message}";
                LogError(mensaje);
                await RegistrarAlertaAsync(ConstantesClaveAviso.Error, new Exception(mensaje), "Validación de Horario").ConfigureAwait(false);
                return false;
            }
        }


        #endregion

        #region Procesamiento
        /// <summary>
        /// Método principal para procesar operaciónes.
        /// </summary>
        /// <param name="operaciones"></param>
        /// <returns></returns>
        private async Task<bool> ProcesarOperacionesAsync(IEnumerable<OperacionSWF> operaciones, CancellationToken ct)
        {
            try
            {
                return await _repositorioTomi.GuardaMensajesOutMQ(operaciones, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogError($"{ConstantesClaveAviso.Error} en GuardaMensajesOutMQGCMS: {ex}");
                return false;
            }
        }

        #endregion

        #region Alertas y Logging
        /// <summary>
        /// Método para Registrar Alertas.
        /// </summary>
        /// <param name="tipo"></param>
        /// <param name="ex"></param>
        /// <param name="descripcion"></param>
        /// <returns></returns>
        private async Task RegistrarAlertaAsync(string tipo, Exception ex, string descripcion = null)
        {
            await _alertas.InsertarAlerta(new AlertaDTO
            {
                Servicio = _configuracion.NombreServicio,
                ClaveAviso = tipo,
                Descipcion = descripcion,
                PId = _configuracion.Pid.Value,
                NombreProceso = _configuracion.NombreProceso,
                Exception = ex
            }).ConfigureAwait(false);
        }
        /// <summary>
        /// Método para el registro de Log Informativo.
        /// </summary>
        /// <param name="msg"></param>
        private void LogInfo(string msg) =>
            Config.EscribirLog($"{ConstantesServicio.MensajeInfo}|{ConstantesServicio.MensajeEjecuta}|{msg}");
        /// <summary>
        /// Método para el registro de Log de Errores.
        /// </summary>
        /// <param name="msg"></param>
        private void LogError(string msg) =>
            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.MensajeEjecuta}|{msg}");

        #endregion
    }
}


namespace AplicacionSwfOUTMQGCMS.Enums
{
    /// <summary>
    /// Clase que se utiliza para colocar constantes de manera parametrizada.
    /// </summary>
    public class ConstantesServicio
    {
        //Constantes de parametrización...
        public const string ParametrosSistema = "'HORA_OP_INICIO','HORA_OP_FIN','SWF_OUT_GET_MT_BATCH','MQ.SWF_OUT_GCMS.MANAGER_NAME','MQ.SWF_OUT_GCMS.HOST','MQ.SWF_OUT_GCMS.PORT','MQ.SWF_OUT_GCMS.CHANNEL','MQ.SWF_OUT_GCMS.QUEUE_NAME','MAIL_ERROR'";
        public const string ParametroWsT24MwPassword = "'MQ.SWF_OUT_GCMS.PASSWORD','MQ.SWF_OUT_GCMS.SSL_CIPHERSPEC','MQ.SWF_OUT_GCMS.SSL_KEYREPOSITORY','MQ.SWF_OUT_GCMS.CERTIFICATE_LABEL','MQ.SWF_OUT_GCMS.USER_ID'";

        //Constantes de lógica...
        public const string TipoOperacion = "SWIFT_OUT";
        public const string Servicio = "SWF_OUT_MQ_GCMS";

        //Constantes de SQL...
        public const string UspObtenerOperacionesSwfOutMq = "usp_ObtenerOperacionesSWFOutMq";
        public const string UspActualizaEstatusEnvioMensajeSwfOutMq = "usp_ActualizaEstatusEnvioMensajeSWFOutMq";
        public const string UspActualizaEstatusProcesoSwfOutMq = "usp_ActualizaEstatusProcesoSWFOutMq";
        public const int MaxSpTimeout = 10;
        public const string EstatusSndng = "SNDNG";
        public const string EstatusError = "ERROR";

        //Constantes de Emeal...
        public const string NombreOrigen = "CMS Servicios";
        public const string Para = "MAIL_TO";
        public const string Asunto = "Error en MQ SWIFT_OUT";
        public const string AsuntoTest = "MAIL_SUBJECT";
        public const string Mensaje = "ERROR durante la inserción del MQ del Swift_OUT_GCMS, error= ";
        public const string SenderTest = "MAIL_SENDER_TEST";

        //Constantes de configuracion de OUT MQ...
        public const string ManagerName = "MQ.SWF_OUT_GCMS.MANAGER_NAME";
        public const string Channel = "MQ.SWF_OUT_GCMS.CHANNEL";
        public const string QueueName = "MQ.SWF_OUT_GCMS.QUEUE_NAME";
        public const string Host = "MQ.SWF_OUT_GCMS.HOST";
        public const string Port = "MQ.SWF_OUT_GCMS.PORT";
        public const string UserId = "MQ.SWF_OUT_GCMS.USER_ID";
        public const string Password = "MQ.SWF_OUT_GCMS.PASSWORD";
        public const string SslCipherSpec = "MQ.SWF_OUT_GCMS.SSL_CIPHERSPEC";
        public const string SslKeyRepository = "MQ.SWF_OUT_GCMS.SSL_KEYREPOSITORY";
        public const string CertificateLabel = "MQ.SWF_OUT_GCMS.CERTIFICATE_LABEL";
        public const string EMailDefault = "MAIL_ERROR";
        public const string SwfOutGetMtBatch = "SWF_OUT_GET_MT_BATCH";

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

        public const string CerticateLabel = "CertificateLabel";
        public const string MensajeConexionExitosa = "Conexión exitosa a IBM MQ";
        public const string MensajeConexiónFallida = "Conexión fallida (IsConnected=false)";
        public const string MensajeConexionMQExitosa = "Conexión a MQ establecida correctamente.";
        public const string MensajeErrorColaMQ = "Error al cerrar cola MQ";
        public const string MensajeErrorDescMQ = "Error al deesconectar MQ";

        public const string MensajeHoraInicio = "La hora de inicio es obligatoria";
        public const string MensajeHoraFin = "La hora de fin es obligatoria";
        public const string MensajeHoraInicioError = "Error al convertir la hora de inicio:";
        public const string MensajeHoraFinError = "Error al convertir la hora de fin:";
        public const string MensajeInfo = "INFO";
        public const string MensajeOperaciones = "No hay operaciones para procesar.";
        public const string MensajeWARN = "WARN";
        public const string MensajeGuardar = "GuardaMensajesOutMQ";
        public const string MensajeEstabeciendoCnx = "Estableciendo conexión a MQ...";

        public const string MensajeConfiguracionBatch = "El parametro de configuración SWFOUTGETMTBATCH no tiene un valor valido.";
        public const string MensajeBatch = "No se encontro el parametro de configuración SWFOUTGETMTBATCH.";
        public const string MensajeErrorEnvioCorreo = "Ocurrio un error en el envio de correo.";

    }

    public class ConstantesParametroSistema
    {
        public const string ValorBatchActivo = "1";
    }
}


using AplicacionSwfOUTMQGCMS.Modelos;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;

namespace AplicacionSwfOUTMQGCMS.Interfaces
{
    /// <summary>
    /// Interfaz para implementar metodos de consulta a la BD InterfacesT24
    /// </summary>
    public interface IRepositorioTomi<Tipo>
    {
        /// <summary>
        /// Método para guardar mensajes en MQ.
        /// </summary>
        /// <param name="listaOperaciones"></param>
        /// <returns></returns>
        Task<bool> GuardaMensajesOutMQ(IEnumerable<OperacionSWF> listaOperaciones, CancellationToken ct);
        /// <summary>
        /// Método para recuperar operaciones.
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<Tipo>> RecuperaOperacionesAsync(CancellationToken ct);
        /// <summary>
        /// Método para actualizar estatus de los mensajes
        /// </summary>
        /// <param name="estatus"></param>
        /// <param name="Folio"></param>
        /// <returns></returns>
        Task<bool> ActualizaEstatusMensajeOutMQAsync(string Folio, string Estatus, CancellationToken ct);
        /// <summary>
        /// Método para actualizar proceso de mensaje.
        /// </summary>
        /// <param name="Folio"></param>
        /// <returns></returns>
        Task<bool> ActualizaEstatusProcesoSWFOutMqAsync(string Folio, CancellationToken ct);
    }

}



﻿namespace AplicacionSwfOUTMQGCMS.Modelos
{
    /// <summary>
    /// Clase para obtener los valores de cada operación.
    /// </summary>
    public class OperacionSWF
    {
        public string Folio { get; set; }
        public string Mensaje { get; set; }

    }
}


﻿namespace AplicacionSwfOUTMQGCMS.Modelos
{
    /// <summary>
    /// Clase para manipular la configuración de servidor MQ.
    /// </summary>
    public class ConfiguracionMQ
    {
        public string MailTo { get; set; } = string.Empty;
        public string Cert { get; set; } = string.Empty;
        public string SslPath { get; set; } = string.Empty;
        public string Ssl { get; set; } = string.Empty;
        public string Password { get; set; } = string.Empty;
        public string User { get; set; } = string.Empty;
        public int Port { get; set; }
        public string Host { get; set; } = string.Empty;
        public string QueueName { get; set; } = string.Empty;
        public string ChannelName { get; set; } = string.Empty;
        public string QueueManagerName { get; set; } = string.Empty;        
    }
}


﻿namespace AplicacionSwfOUTMQGCMS.Enums
{
    /// <summary>
    /// Resultado Operaciones MQ
    /// </summary>
    public enum ResultadoOperacionMq
    {
        Exitosa,
        Rechazada,
        Fallida
    }
}


using AplicacionSwfOUTMQGCMS.Enums;
using AplicacionSwfOUTMQGCMS.Interfaces;
using AplicacionSwfOUTMQGCMS.Modelos;
using InfraestructuraSwfOUTMQGCMS.Repositorio;
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

namespace SwfIOUTMQGCMS
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private Configuracion _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.Swf_OUT_MQ_GCMS.ToString();
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
                ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Swf_OUT_MQ_GCMS.ToString());
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
            _containerDI.Register<AplicacionSwfOUTMQGCMS.OperacionServicio>();
            _containerDI.Register<IRepositorioTomi<OperacionSWF>, RepositorioTomi>();
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

                if (!_configuracionBase.Pid.HasValue)
                {
                    throw new Exception("PID del proceso no fue configurado.");
                }

                if (string.IsNullOrWhiteSpace(_configuracionBase.NombreProceso))
                {
                    throw new Exception("NombreProceso no fue configurado.");
                }

                if (string.IsNullOrWhiteSpace(_configuracionBase.NombreDisplay))
                {
                    throw new Exception("NombreDisplay no fue configurado.");
                }

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
                    var operacionServicio = _containerDI.GetInstance<AplicacionSwfOUTMQGCMS.OperacionServicio>();
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

            try
            {
                if (_containerDI == null || _configuracionBase == null)
                {
                    Config.EscribirLog(ConstantesServicio.MensajeOmitido);
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
                _containerDI?.Dispose();
            }
        }

    }
}


﻿using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace SwfIOUTMQGCMS
{
    internal static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                 NombreServicio.Swf_OUT_MQ_GCMS.ToString(),
                 () => new ServiceHost(),
                 args
#if DEBUG
                 , (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs)
#endif
                 );
        }
    }
}


AplicacionSwfOUTMQGCMS.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{27DD1AB7-038B-44B2-9F06-0364022798B8}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>AplicacionSwfOUTMQGCMS</RootNamespace>
    <AssemblyName>AplicacionSwfOUTMQGCMS</AssemblyName>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
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
    <Compile Include="Enums\ConstantesServicio.cs" />
    <Compile Include="Enums\ResultadoOperacionMq.cs" />
    <Compile Include="Interfaces\IOperacionServicio.cs" />
    <Compile Include="Interfaces\IRepositorioTomi.cs" />
    <Compile Include="Modelos\ConfiguracionMQ.cs" />
    <Compile Include="Modelos\OperacionSWF.cs" />
    <Compile Include="OperacionServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


InfraestructuraSwfOUTMQGCMS.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{A5B920A9-F966-4697-9516-E42B5FFB2B88}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>InfraestructuraSwfOUTMQGCMS</RootNamespace>
    <AssemblyName>InfraestructuraSwfOUTMQGCMS</AssemblyName>
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
    <ProjectReference Include="..\AplicacionSwfOUTMQGCMS\AplicacionSwfOUTMQGCMS.csproj">
      <Project>{27dd1ab7-038b-44b2-9f06-0364022798b8}</Project>
      <Name>AplicacionSwfOUTMQGCMS</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


SwfOUTMQGCMS.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{6C05C5AE-E675-4138-8DD8-28F5F6287E16}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>SwfOUTMQGCMS</RootNamespace>
    <AssemblyName>SwfOUTMQGCMS</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
    <Deterministic>true</Deterministic>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <PlatformTarget>AnyCPU</PlatformTarget>
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
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
    <Prefer32Bit>true</Prefer32Bit>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
    <Prefer32Bit>true</Prefer32Bit>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="Microsoft.Bcl.AsyncInterfaces, Version=1.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Bcl.AsyncInterfaces.1.0.0\lib\net461\Microsoft.Bcl.AsyncInterfaces.dll</HintPath>
    </Reference>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <HintPath>..\packages\SimpleInjector.5.0.0\lib\net461\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Core" />
    <Reference Include="System.Runtime.CompilerServices.Unsafe, Version=4.0.4.1, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Runtime.CompilerServices.Unsafe.4.5.2\lib\netstandard2.0\System.Runtime.CompilerServices.Unsafe.dll</HintPath>
    </Reference>
    <Reference Include="System.Threading.Tasks.Extensions, Version=4.2.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Threading.Tasks.Extensions.4.5.2\lib\netstandard2.0\System.Threading.Tasks.Extensions.dll</HintPath>
    </Reference>
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
    <ProjectReference Include="..\AplicacionSwfOUTMQGCMS\AplicacionSwfOUTMQGCMS.csproj">
      <Project>{27dd1ab7-038b-44b2-9f06-0364022798b8}</Project>
      <Name>AplicacionSwfOUTMQGCMS</Name>
    </ProjectReference>
    <ProjectReference Include="..\InfraestructuraSwfOUTMQGCMS\InfraestructuraSwfOUTMQGCMS.csproj">
      <Project>{a5b920a9-f966-4697-9516-e42b5ffb2b88}</Project>
      <Name>InfraestructuraSwfOUTMQGCMS</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


SwfOUTMQGCMS.slnx
<Solution>
  <Configurations>
    <Platform Name="Any CPU" />
    <Platform Name="x86" />
  </Configurations>
  <Project Path="AplicacionSwfOUTMQGCMS/AplicacionSwfOUTMQGCMS.csproj" Id="27dd1ab7-038b-44b2-9f06-0364022798b8">
    <Platform Solution="*|x86" Project="x86" />
  </Project>
  <Project Path="InfraestructuraSwfOUTMQGCMS/InfraestructuraSwfOUTMQGCMS.csproj" Id="a5b920a9-f966-4697-9516-e42b5ffb2b88">
    <Platform Solution="*|x86" Project="x86" />
  </Project>
  <Project Path="PruebasUnitarias/PruebasUnitarias.csproj" Id="950ba7a5-8b5d-42d6-b353-14c174810b9c">
    <Platform Solution="*|x86" Project="x86" />
  </Project>
  <Project Path="SwfIOUTMQGCMS/SwfOUTMQGCMS.csproj" Id="6c05c5ae-e675-4138-8dd8-28f5f6287e16">
    <Platform Solution="*|x86" Project="x86" />
  </Project>
</Solution>


