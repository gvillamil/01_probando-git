using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using AplicacionEmail.Servicios;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;


namespace AplicacionEmail
{
    public class OperacionServicio
    {
        private readonly Configuracion                    _configuracion;
        private readonly IControlServicio                 _controlServicio;
        private readonly IAlertas                         _alertas;
        private readonly IParametrizacion                 _parametrizacion;
        private readonly IENT_SOL                         _entSol;
        private readonly IENT_BEN                         _entBen;
        private readonly IENT_ACUSESAT                    _entAcuseSat;
        private readonly IENT_H2H                         _entH2H;
        private readonly IENT_BENCMS                      _entBenCms;
        private readonly IENT_NOTIFOREX                   _entNotiForex;
        private readonly IENT_PRENOTIFOREX                _entPreNotiForex;
        private readonly IENT_AVISOPWD                    _entAvisoPwd;
        private readonly IENT_OPERACFCM                   _entOperacFcm;
        private readonly IENT_AVISOURL                    _entAvisoUrl;
        private readonly IENT_AVISOOLVPWD                 _entAvisoOlvPwd;
        private readonly IENT_TOKENEXECP                  _entTokenExcep;
        private readonly IENTOPEHOTS                      _entOpeHots;
        private readonly IENT_OPEHOTSPID                  _entOpeHotsSpid;
        private readonly IENT_ALERTACRIT                  _entAlertaCrit;
        private readonly IENT_NOTIFIOTROS                 _entNotifiEmailOtros;
        private readonly IENT_SOBGIRO                     _entSobregiro;
        private readonly IENT_CUENTCASHPOOL               _entCuentaCash;
        private readonly IENT_CUENTAFAD                   _entCuentaFAD;
        private readonly IENT_FIELSPID                    _entFielSPID;
        private readonly IENT_INIFIELES                   _entIniFielSPID;

        /// <summary>
        /// Constructor.
        /// </summary>
        public OperacionServicio(Configuracion configuracion, 
                                IControlServicio   controlServicio, 
                                IAlertas           alertas, 
                                IParametrizacion   parametrizacion, 
                                IENT_SOL           entSol, 
                                IENT_BEN           entBen, 
                                IENT_ACUSESAT      entAcuseSat, 
                                IENT_H2H           entH2H, 
                                IENT_BENCMS        entBenCms, 
                                IENT_NOTIFOREX     entNotiForex, 
                                IENT_PRENOTIFOREX  entPreNotiForex, 
                                IENT_AVISOPWD      entAvisoPwd, 
                                IENT_OPERACFCM     entOperacFcm, 
                                IENT_AVISOURL      entAvisoUrl, 
                                IENT_AVISOOLVPWD   entAvisoOlvPwd, 
                                IENT_TOKENEXECP    entTokenExcep, 
                                IENTOPEHOTS        entOpeHots, 
                                IENT_OPEHOTSPID    entOpeHotsSpid , 
                                IENT_ALERTACRIT    entAlertaCrit, 
                                IENT_NOTIFIOTROS   entNotifiEmailOtros, 
                                IENT_SOBGIRO       entSobregiro, 
                                IENT_CUENTCASHPOOL entCuentaCash,
                                IENT_CUENTAFAD     entCuentaFAD,
                                IENT_FIELSPID      entFielSPID,
                                IENT_INIFIELES     entIniFielSPID)
        {
            _configuracion     = configuracion;
            _controlServicio   = controlServicio;
            _alertas           = alertas;
            _parametrizacion   = parametrizacion;
            _entSol            = entSol;
            _entBen            = entBen;
            _entAcuseSat       = entAcuseSat;
            _entH2H            = entH2H;
            _entBenCms         = entBenCms;
            _entNotiForex      = entNotiForex;
            _entPreNotiForex   = entPreNotiForex;
            _entAvisoPwd       = entAvisoPwd;
            _entOperacFcm      = entOperacFcm;
            _entAvisoUrl       = entAvisoUrl;
            _entAvisoOlvPwd    = entAvisoOlvPwd;
            _entTokenExcep     = entTokenExcep;
            _entOpeHots        = entOpeHots;
            _entOpeHotsSpid      = entOpeHotsSpid;
            _entAlertaCrit       = entAlertaCrit;
            _entNotifiEmailOtros = entNotifiEmailOtros;
            _entSobregiro        = entSobregiro;
            _entCuentaCash       = entCuentaCash;
            _entCuentaFAD        = entCuentaFAD;
            _entFielSPID         = entFielSPID;
            _entIniFielSPID      = entIniFielSPID; 

        }

        /// <summary>
        /// Control del flujo principal del procesamiento.
        /// </summary>
        public async Task<bool> EjecutarProcedimientosAsync(CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                //Marca la ejecución del servicio...
                await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct);

                //Obtiene los parámetros del sistema...
                _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync(ct);
                _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil(ct);
                _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesServicio.ParametrosSistema, ct);
                _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAdicionalesAsync(_configuracion.ParametrosSistema, ConstantesServicio.ParametrosSistemaAdiconales);

                if (_configuracion.ParametrosSistema == null)
                    throw new InvalidOperationException("No se obtuvieron los parámetros del sistema.");

                _configuracion.ParametrosSistema.TryGetValue("MAIL_SOCKET_CEP_TO", out string mailSocketCepTo);
                if (!_configuracion.ParametrosSistema.TryGetValue(ConstantesGenerales.HoraLecInicio, out string horaLecturaInicio) || string.IsNullOrWhiteSpace(horaLecturaInicio))
                    throw new InvalidOperationException("No se especifica la hora de inicio de operaciones.");
                if (!_configuracion.ParametrosSistema.TryGetValue(ConstantesGenerales.HoraLecFin, out string horaLecturaFin) || string.IsNullOrWhiteSpace(horaLecturaFin))
                    throw new InvalidOperationException("No se especifica la hora de finalización de operaciones CEP.");
                if (!_configuracion.EsDiaHabil.HasValue)
                    throw new InvalidOperationException("No se obtuvo el indicador de día hábil.");
                if (!_configuracion.Pid.HasValue)
                    throw new InvalidOperationException("No se obtuvo el PID del servicio.");

                //Logeo...
                {
                    Config.EscribirLog($"INFO|obtenerParametrosSistema|Fecha del sistema '{_configuracion.FechaSistema}'.");
                    Config.EscribirLog($"INFO|obtenerParametrosSistema|Mails de CEP '{mailSocketCepTo}'.");
                    Config.EscribirLog($"INFO|obtenerParametrosSistema|Hora de inicio de operaciones '{horaLecturaInicio}'.");
                    Config.EscribirLog($"INFO|obtenerParametrosSistema|Hora de finalización de operaciones CEP '{horaLecturaFin}'.");
                }


                //Valida que sea día hábil...
                if (!_configuracion.EsDiaHabil.Value)
                {
                    Config.EscribirLog("ERROR|Ejecuta_Procedimientos|Hoy es un día inhábil.");
                    await _alertas.InsertarAlerta(new AlertaDTO()
                    {
                        Folio = null,
                        TipoOPeracion = null,
                        Servicio = _configuracion.NombreServicio,
                        ClaveAviso = ConstantesClaveAviso.Error,
                        Descipcion = "Hoy es un día inhábil",
                        PId = _configuracion.Pid.Value,
                        NombreProceso = _configuracion.NombreProceso,
                        Exception = new Exception($"Hoy es un día inhábil.")
                    });
                    return false;
                }

                //Valida el horario de operación...
                TimeSpan timeHoraSistema = _parametrizacion.ObtenerHoraSistema();
                string horaSistema = timeHoraSistema.ToString(@"hh\:mm");

                TimeSpan horaInicio = ConversionesComunes.ParseTimeSpanRequerido(horaLecturaInicio, ConstantesGenerales.HoraLecInicio);
                TimeSpan horaFin = ConversionesComunes.ParseTimeSpanRequerido(horaLecturaFin, ConstantesGenerales.HoraLecFin);
                bool esHorarioLectura = timeHoraSistema >= horaInicio || timeHoraSistema <= horaFin;
                _configuracion.EsHorarioLectura = esHorarioLectura;

                if (!esHorarioLectura)
                {
                    Config.EscribirLog("ERROR|Ejecuta_Procedimientos|El servicio está fuera de horario.");
                    await _alertas.InsertarAlerta(new AlertaDTO()
                    {
                        Folio = null,
                        TipoOPeracion = null,
                        Servicio = _configuracion.NombreServicio,
                        ClaveAviso = ConstantesClaveAviso.Error,
                        Descipcion = "El servicio está fuera de horario",
                        PId = _configuracion.Pid.Value,
                        NombreProceso = _configuracion.NombreProceso,
                        Exception = new Exception($"El servicio '{_configuracion.NombreServicio}' está fuera de horario.")
                    });
                    return false;
                }

                //Realizar el proceso para validación de URL activa
                await _entAvisoUrl.ProcesarValidacionURLAsync();

                //'RPA  Fieles  - log --> EmailFieles  -  120000 mseg  (2  min )
                //Realizar el proceso para las operaciones para la inicialización de fieles de tipo SPID
                await _entIniFielSPID.ProcesarInicioFielesSPIDAsync();
                //Realizar el proceso para las operaciones de fieles SPID
                await _entFielSPID.ProcesarFielesSPIDAsync();

                //Reportes RPA  '-  EmailRPA   -  120000  ( 60000ML  1 MIN)  mlsec  - log --> EmailRPA
                //Realizar el proceso para el encio de operaciones de cuentas de tipo FAD
                await _entCuentaFAD.ProcesarCuentasFADAsync();

                //Realizar el proceso para el encio de operaciones de cuentas de tipo cash pool
                await _entCuentaCash.ProcesarOperacionCuentaCashPoolAsync();

                //Realiza el proceso para las operaciones de sobregiro
                await _entSobregiro.ProcesarSobregirosAsync();

                //-- BLOQUE olvido password   -  EmailOlvidoPass   -  5000 mlsec  - log --> EmailOlvidoPass --- gsSiempreActivo = "SI"
                //Realizar el proceso para procesar las solicitudes de olcido de password
                await _entAvisoOlvPwd.ProcesarOperacionAvisoOlvidoPWD();

                //-- BLOQUE cambio password   -  EmailCmsCbioPass   -  5000 mlsec  - log --> EmailCmsCbioPass --- gsSiempreActivo = "SI"
                //Realiza el proceso principal para el envio de correos de notificaciones PWD
                await _entAvisoPwd.ProcesarOperacionAvisoPwdAsync();

                //-- BLOQUE flujo TOKEN Excepcion   -  EmailtokenException   -  5000 mlsec  - log --> EmailTOKEN
                //Realizar el proceso para procesar las solicitudes de token de excepcion
                await _entTokenExcep.ProcesarOperacionTokenExecpAsynx();

                //-- BLOQUE DE SOLIC_BENEFICIARIOS  -  EmailSoliBenef   -  10000 mlsec
                //Realiza el proceso principal para el envio de correos
                await _entSol.ProcesarOperacionSolicitudsync();
                //Realiza el proceso principal para el envio de notificaciones de beneficiarios CMS
                await _entBenCms.ProcesarOperacionBeneficiarioCmsAsync();
                //Realiza el proceso principal para el envio de correos de beneficiarios
                await _entBen.ProcesarCorreoBeneficiarios();
                //Realiza el proceso principal para el envio de correos de acuses del SAT
                await _entAcuseSat.ProcesarCorreoAcuseSAT();

                //FALTA PROCESA_REEMPLAZO_TOKEN

                //---  BLOQUE  DE H2H       -   EmailH2H    - 10000 mlsec
                //Realiza el proceso principal para el envio de notificaciones H2H
                await _entH2H.ProcesarCorreoAvisoH2H();

                //'---  BLOQUE  DE forex       -   EmaiForexFcm    - 10000 mlsec
                //Realiza el proceso principal para el envio de correos de pre-notificaciones FOREX
                await _entPreNotiForex.ProcesarOperacionPreNotificacionesForexAsync();
                //Realiza el proceso principal para el envio de correos de notificaciones FOREX
                await _entNotiForex.ProcesarOperacionNotificacionesForexAsync();
                //Realiza el proceso principal para el envio de operaciones FCM
                await _entOperacFcm.ProcesarOperacionFcmAsync();

                //'---  BLOQUE  DE H2H       -   EmailH2H    - 10000 mlsec
                //Realiza el proceso para las operaciones de Hotscan
                await _entOpeHots.ProcesarOperacionHotscanAsync();
                //Realiza el proceso para las operaciones de Hotscan Spid
                await _entOpeHotsSpid.ProcesarOperacionHostSpidAsync();

                //'---  BLOQUE  DE Akertas Criticas       -   EmailAlertaCritica    - 60000 mlsec  1 min
                //Realiza el proceso para las alertas criticas
                await _entAlertaCrit.ProcesarAlertasCriticasAsync();
                
                //---  BLOQUE  DE Notificaciones Generales       -   EmailNotificaOtros    - 300000 mlsec  5 min
                //Realiza el proceso para las notificaciones de otros emails
                await _entNotifiEmailOtros.ProcesarNotificacionesEmailOtrosAsync();


                Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Se termina procesamiento de operaciones");

                //Marca el termino del servicio...
                await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);

                return true;
            }
            catch (OperationCanceledException) {
                Config.EscribirLog("INFO|Ejecuta_Procedimientos|Operación cancelada.");
                return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Se presenta exception: {ex.ToString()}");
                if (_configuracion.Pid.HasValue)
                {
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
                    }, ct);
                }
            }
            return false;
        }

    }
}


using AplicacionEmail;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using AplicacionEmail.Servicios;
using InfraestructuraEmail.Repositorios;
using InfraestructuraEmail.Servicios;
using Microsoft.Extensions.Configuration;
using MUFG_T24.Aplicacion.Conversiones;
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
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;

namespace Email
{
    /// <summary>
    /// Clase Principal del servicio windows(presentación).
    /// </summary>>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container     _containerDI;
        private Configuracion _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.ControladorServicios.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

        /// <summary>
        /// Constructor
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
                ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Email.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.PathLog = servicioMainConfiguracion.Log;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;
                _configuracionBase.LimiteConcurrencia = servicioMainConfiguracion.LimiteConcurrencia;

                _configuracionBase.configuracion = servicioMainConfiguracion.configuracion;
                _configuracionBase.CorreoParaDefault  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault      = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest   = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);
                

                _configuracionBase.RutaArchivoConfiguracion = $@"{EnumRutasGenerales.Path_Log_CMS}{NombreServicio.Email.ToString()}\";

                //Establece las rutas de log...
                RutasConfiguracion rutas = new RutasConfiguracion()
                {
                    MUFG_Servicios_Path_Log = _configuracionBase.PathLog
                };
                
                Config.TamanoMaximoArchivoKB = ConversionesComunes.ParseEnteroRequerido(
                    _configuracionBase.TamanoMaximoArchivoLogKB,
                    nameof(_configuracionBase.TamanoMaximoArchivoLogKB));
                Config.AjustarRutas = true;
                Config.IniciarRutas(rutas);
                Config.CreaRutaArchivoSalida(Config.ObtenerNombreArchivoLog());
            }
            
            //Registra la configuracion como Transient + Factory (lazy)...
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);

            //Registra los componentes...
            _containerDI.Register<IDetallesTomser,   DetallesTomserDao>();
            _containerDI.Register<IDetallesTomi,     DetallesTomiDao>();
            _containerDI.Register<IDetallesBot,      DetallesBotDao>();
            _containerDI.Register<IDetallesBotLog,   DetallesBotLogDao>();
            _containerDI.Register<IDetallesEnlace,   DetallesEnlaceDao>();
            _containerDI.Register<IDetallesVistas,   DetallesVistasDao>();
            _containerDI.Register<IENT_SOL,          ENT_SOL>();
            _containerDI.Register<IENT_BEN,          ENT_BEN>();
            _containerDI.Register<IENT_BENFXD,       ENT_BENFXD>();
            _containerDI.Register<IENT_ACUSESAT,     ENT_ACUSESAT>();
            _containerDI.Register<IENT_ACUSESATNEPE, ENT_ACUSESATNEPE>();
            _containerDI.Register<IENT_ACUSESATREF,  ENT_ACUSESATREF>();
            _containerDI.Register<IENT_ACUSESATSHCP, ENT_ACUSESATSHCP>();
            _containerDI.Register<IENT_ENVIOACUSE,   ENT_ENVIOACUSE>();
            _containerDI.Register<IENT_AVISOH2H,     ENT_AVISOH2H>();
            _containerDI.Register<IENT_H2H,          ENT_H2H>();
            _containerDI.Register<IENT_H2HFXD,       ENT_H2HFXD>();
            _containerDI.Register<IENT_H2HFXI,       ENT_H2HFXI>();
            _containerDI.Register<IENT_H2HFXT,       ENT_H2HFXT>();
            _containerDI.Register<IENT_H2HSPID,      ENT_H2HSPID>();
            _containerDI.Register<IENT_H2HTEF,       ENT_H2HTEF>();
            _containerDI.Register<IENT_ENVIOBENCMS,  ENT_ENVIOBENCMS>();
            _containerDI.Register<IENT_BENCMS,       ENT_BENCMS>();
            _containerDI.Register<IENT_NOTIFOREX,    ENT_NOTIFOREX>();
            _containerDI.Register<IENT_PRENOTIFOREX, ENT_PRENOTIFOREX>();
            _containerDI.Register<IENT_AVISOPWD,     ENT_AVISOPWD>();
            _containerDI.Register<IENT_ENVAVISOPWD,  ENT_ENVAVISOPWD>();
            _containerDI.Register<IENT_OPERACFCM,   ENT_OPERACFCM>();
            _containerDI.Register<IENT_ENVFCM, ENT_ENVFCM>();
            _containerDI.Register<IENT_ENVIANOTIFOREX, ENT_ENVIANOTIFOREX>();
            _containerDI.Register<IENT_NOTIFICACION, ENT_NOTIFICACION>();
            _containerDI.Register<IENT_AVISO,        ENT_AVISO>();
            _containerDI.Register<IENT_AVISOURL,     ENT_AVISOURL>();
            _containerDI.Register<IENT_AVISOOLVPWD, ENT_AVISOOLVPWD>();
            _containerDI.Register<IENT_TOKENEXECP, ENT_TOKENEXECP>();
            _containerDI.Register<IENTOPEHOTS, ENTOPEHOTS>();
            _containerDI.Register<IENT_OPEHOTSPID, ENT_OPEHOTSPID>();
            _containerDI.Register<IENT_ALERTACRIT, ENT_ALERTACRIT>();
            _containerDI.Register<IENT_NOTIFIOTROS, ENT_NOTIFIOTROS>();
            _containerDI.Register<IENT_SOBGIRO, ENT_SOBGIRO>();
            _containerDI.Register<IENT_CUENTCASHPOOL, ENT_CUENTCASHPOOL>();
            _containerDI.Register<IENT_CUENTAFAD, ENT_CUENTAFAD>();
            _containerDI.Register<IENT_FIELSPID, ENT_FIELSPID>();
            _containerDI.Register<IENT_INIFIELES, ENT_INIFIELES>();
            _containerDI.Register<OperacionServicio>();
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
                if (!_configuracionBase.Pid.HasValue)
                    throw new InvalidOperationException("No se obtuvo el PID requerido para marcar el servicio en uso.");

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
                //Ejecuta el procedimiento principal del servicio
                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    var operacionServicio = _containerDI.GetInstance<AplicacionEmail.OperacionServicio>();
                    await operacionServicio.EjecutarProcedimientosAsync(ct).ConfigureAwait(false);
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

namespace Email
{
    partial class ServiceHost
    {
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            components = new System.ComponentModel.Container();
            this.ServiceName = "Service1";
        }

        #endregion
    }
}

using System;
using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;

namespace Email
{
    internal static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                NombreServicio.Email.ToString(),
                () => new ServiceHost(),
                args,
                (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs));
        }    
    }
}

emal/App.config
<?xml version="1.0" encoding="utf-8"?>
<configuration>
	<startup>
		<supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.8" />
	</startup>

	<appSettings>
		<add key="NombreServicio" value="EMAIL"></add>
		<add key="AliasProceso" value="GENERA_EMAIL"></add>
		<add key="AliasProcesoFin" value="ENVIA_EMAIL"></add>
		<add key="ServicioPlan" value="EMAIL"></add>

		<add key="TimerEnabled" value="true"></add>
		<add key="TimerInterval" value="5000"></add>
		<add key="MaxTimeout" value="10"></add>

		<add key="MonedaDefault" value="MXP"></add>
		<add key="ListaTipos" value="'ENT_SOL'"></add>

		<add key="PathLog" value="C:/LogsEmail/CMS/SPEI_CEP_GEN_CDA/"></add>
	</appSettings>
  <runtime>
    <assemblyBinding xmlns="urn:schemas-microsoft-com:asm.v1">
      <dependentAssembly>
        <assemblyIdentity name="Microsoft.Extensions.Configuration.Abstractions" publicKeyToken="adb9793829ddae60" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-10.0.0.10" newVersion="10.0.0.10" />
      </dependentAssembly>
      <dependentAssembly>
        <assemblyIdentity name="System.Memory" publicKeyToken="cc7b13ffcd2ddd51" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-4.0.5.0" newVersion="4.0.5.0" />
      </dependentAssembly>
      <dependentAssembly>
        <assemblyIdentity name="System.Buffers" publicKeyToken="cc7b13ffcd2ddd51" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-4.0.5.0" newVersion="4.0.5.0" />
      </dependentAssembly>
      <dependentAssembly>
        <assemblyIdentity name="System.Numerics.Vectors" publicKeyToken="b03f5f7f11d50a3a" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-4.1.6.0" newVersion="4.1.6.0" />
      </dependentAssembly>
      <dependentAssembly>
        <assemblyIdentity name="System.Runtime.CompilerServices.Unsafe" publicKeyToken="b03f5f7f11d50a3a" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-6.0.3.0" newVersion="6.0.3.0" />
      </dependentAssembly>
    </assemblyBinding>
  </runtime>
</configuration>

using System;
using System.Collections.Generic;
using System.Drawing;
using System.Text;

namespace AplicacionEmail.Enums
{
    /// <summary>
    /// Clase que contiene las constantes a modo de enumerador
    /// Por compatibilidad: NET Framework 4.8 no permite enumeradores strings.
    /// </summary>
    public static class ConstantesServicio
    {
        //Constantes de operadores html
        public const string Tab = "\t";
        public const string NewLine = "\r\n";

        //----------------------------------
        //Constantes de información correo solicitudes...
        public const string CorreoOrigenSol       = "mufgmexicocms@mx.mufg.jp";
        public const string NombreCorreoOrigenSol = "CMS Support";
        public const string ConCopiaSol           = "cmsmexicosupport@mx.mufg.jp";
        public const string Asunto                = "*** Asignacion de Token ***";
        
        public const string CorreoOrigenErrorSol       = "avisos_error@mx.mufg.jp";
        public const string CorreoOrigenHotscan        = "tomiscan_mx@mx.mufg.jp";
        public const string CorreoOrigenHotscanSpid    = "tomiscan_spid@mx.mufg.jp";
        public const string NombreCorreoOrigenErrorSol = "Avisos ERROR";
        public const string NombreCorreoOrigenHotscan  = "TOMI_Scan process";
        public const string NombreCorreoOrigenHotscanSpid = "TOMI_Scan SPID";
        public const string CorreoParaErrorSol         = "Avisos ERROR";
        public const string AsuntoError                = "Error en envío de email";
        public const string AsuntoHotscan              = "*** Operacion TOMI Scan ***";
        public const string AsuntoHotscanSpid          = "*** Operacion TOMI Scan SPID***";
        public const string AsuntoErrorPwd             = "Error en envío de email notificación CUB FXD";
        public const string MensajeError               = "Se detectó un error en el envío de correo de solicitud. Por favor consulta el log de emails para mas detalle.";
        public const string MensajeErrorHotscan        = "Se detectó un error en el envío de correo de hotscan. Por favor consulta el log de emails para mas detalle.";
        
        //----------------------------------
        //Constantes de información correo avisos CUB SET TAX...
        public const string CorreoOrigenAviso       = "avisos_cms@mx.mufg.jp";
        public const string NombreCorreoOrigenAviso = "Avisos MUFG Bank México";
        public const string ConCopiaAviso           = "cmsmexicosupport@mx.mufg.jp";
        public const string AsuntoAviso             = "*** Notificación MUFG Bank México ***";
        public const string AsuntoErroSERTAX        = "Error en envío de email notificación CUB SER_TAX";
        public const string MensajeErrorSERTAX      = "Error en envío de email notificación CUB SER_TAX Se detectó un error en el envío de correo de traspaso. Por favor consulta el log de emails para mas detalle.";
        public const string MensajeErrorFXD         = "Error en envío de email notificación CUB FXD. Se detectó un error en el envío de correo de traspaso. Por favor consulta el log de emails para mas detalle.";
        public const string AvisoNotificacion = "*** Notificación MUFG Bank México ***";
        public const string AvisoPreNotificacion = "*** Pre-Notificación MUFG Bank México ***";
        public const string AvisoNotificacionEmailOtros = "*** Notificación Monitoreo  S P E I - dia  ***";
        public const string AvisoSobregiros = "*** Notificación Reportes RPA -  SobreGiros***";
        public const string AvisoFielesSPID = "*** Notificación Reportes RPA  - Estatus Validacion FIELES ***";
        public const string AvisoFielesINISPID = "*** Notificación RPA  - Inicio de dia SPID ( Inicializa Estatus FIELES )  ***";

        public const string MAIL_SOCKET_TO                   = "rrohde1@mx.mufg.jp, gvillamil@mx.mufg.jp";
        public const string MAIL_SOCKET_CC                   = "rrohde1@mx.mufg.jp, gvillamil@mx.mufg.jp";
        public const string AVISO_BENEFICIARIOS_BCC          = "cmsmexicosupport@mx.mufg.jp;iperez@mx.mufg.jp;";
        public const string MAIL_SOCKET_CEP_TO               = "rrohde1@mx.mufg.jp, gvillamil@mx.mufg.jp";
        public const string MAIL_ERROR                       = "gvillamil@mx.mufg.jp,iperez@mx.mufg.jp";
        public const string AVISO_REGISTRO_BENEFICIARIOS_BCC = "asilva@mx.mufg.jp;jramirez@mx.mufg.jp;apacheco@mx.mufg.jp;iperez@mx.mufg.jp;cosorio@mx.mufg.jp;lgallardo@mx.mufg.jp;gnavarro@mx.mufg.jp;gtrejo@mx.mufg.jp";
        public const string MAIL_ERROR_CMS                   = "rrohde1@mx.mufg.jp, gvillamil@mx.mufg.jp";

        public const string CorreoErroHelpDesk = "Mexico_Helpdesk_CMS@mx.mufg.jp";

        public const string MAIL_CC_NOTIFOREX                 = "htorres@mx.mufg.jp";

        //Constantes de información correos SAT
        public const string CorreoOrigenAvisoSAT          = "avisos_sat_nepe@mx.mufg.jp";
        public const string NombreCorreoOrigenAvisoSAT    = "Avisos SAT-NEPE";
        public const string AsuntoAvisoEnvioSAT           = "Aviso de ";
        public const string AsuntoAvisoEnvioSAT2          = " de archivo SAT-NEPE";
        public const string AsuntoAvisoEnvioSATError      = "Error en envío de email";

        public const string CorreoOrigenEnvioFcm  = "FCM@mx.mufg.jp";
        public const string NombreCorreoOrigenFcm = "FCM process";
        public const string AsuntoAvisoEnvioFCM   = "*** Alerta FCM ***";

        public const string AsuntoAvisoEnvioBENCMS        = "Error en envío de email Notificación Beneficiarios";
        public const string MensajeAvisoEnvioSATError     = "Se detectó un error en el envío de correo de SAT-NEPE. Por favor consulta el log de emails para mas detalle.";
        public const string MensajeAvisoEnvioSATErrorHSCP = " Se detectó un error en el envío de correo de SAT-SHCP. Por favor consulta el log de emails para mas detalle.";
        public const string MensajeAvisoEnvioBenCMS = " Se detectó un error en el envío de correo de notificación de beneficiarios. Por favor consulta el log de emails para mas detalle.";
        public const string MensajeAvisoTokenExcep  = "Se detectó un error en el envío de correo del remplazo de token.Por favor consulta el log de emails para mas detalle.";


        public const string AsuntoAvisoEnvioNotiForex   = "Error en envío de email Notificación FX";
        public const string AsuntoAvisoEnvioPreNotiForex = "Error en envío de email Pre-Notificación FX";
        public const string MensajeAvisoEnvioNotifForez = "Se detectó un error en el envío de correo de traspaso. Por favor consulta el log de emails para mas detalle.";

        public const string CorreoOrigenAvisoSATREF        = "avisos_sat_ref@mx.mufg.jp";
        public const string CorreoOrigenAvisoSATHSCP       = "avisos_sat_shcp@mx.mufg.jp";
        public const string CorreoCCTokenExcepcion          = "AllStaffMUFGMexicoProductionSupport @us.mufg.jp";
        
        public const string NombreCorreoOrigenAvisoSATREF  = "Avisos SAT-REF";
        public const string NombreCorreoOrigenAvisoSATHSCP = "Avisos SAT-SHCP";
        public const string AsuntoAvisoEnvioSATREF         = " de archivo SAT-REFERENCIADO";
        public const string AsuntoAvisoEnvioSATHSCP        = " de archivo SAT-SHCP";
        public const string AsuntoTokenExcepcion           = "Información de usuário / User information /ご案内";
        
        //----------------------------------
        //Constantes de lógica...
        public const string EntSolCMS = "SOL_CMS";
        public const string EntBenSER = "BEN_SER";
        public const string EntBenTAX = "BEN_TAX";
        public const string EntBenFXD = "BEN_FXD";
        public const string EntBenPAP = "BEN_PAP";

        //--------------------------------
        //Constantes de parametrización...
        public const string ParametrosSistema = "'HORA_LECTURA_INICIO','HORA_LECTURA_FIN','MON_T24_DUPLICADO_mailTo','MON_T24_DUPLICADO_mailCc','MON_T24_DUPLICADO_mailBcc','NotifOtro_mailTo','NotifOtro_mailCc','NotifOtro_mailBCc','RPA_SOBREGIRO_Time','RPA_CASHPOOLING_HoraIni','RPA_CASHPOOLING_HoraFin','RPA_SOBREGIRO_mailTo','RPA_SOBREGIRO_mailCc','RPA_SOBREGIRO_mailBcc','RPA_CASHPOOLING_mailTo','RPA_CASHPOOLING_mailCc','RPA_CASHPOOLING_mailBcc','RPA_CTASFAD_mailTo','RPA_CTASFAD_mailCc','RPA_CTASFAD_mailBcc','RPA_FIELES_mailTo','RPA_FIELES_mailCc','RPA_FIELES_mailBcc'";
        public const string ParametrosSistemaAdiconales = "'MAIL_SOCKET_CEP_TO'";


        //Constantes de SQL...
        public const string uspObtenerRegistrosCda          = "usp_SpeiGenCda_ObtenerRegistrosCda";
        public const string uspObtenerFolioSolicitud        = "usp_Email_obtenerFolioSolicitud";
        public const string uspEmailObtenerFolioBenefSer    = "usp_Email_obtenerFolioBenefSer";
        public const string uspEmailactualizarEstatusSol    = "usp_Email_actualizarEstatusSol";
        public const string uspEmailobtenerContacto         = "usp_Email_obtenerContacto";
        public const string uspEmailobtenerCorreoOrdenante  = "usp_Email_obtenerCorreoOrdenante";
        public const string uspEmailobtenerFolioBenTAX      = "usp_Email_obtenerFolioBenTAX";
        public const string uspEmailactualizarEstatusBen    = "usp_Email_actualizarEstatusBen";
        public const string uspEmailactualizarEstatusTax    = "usp_Email_actualizarEstatusTax";
        public const string uspEmailobtenerFolioBenFXD      = "usp_Email_obtenerFolioBenFXD";
        public const string uspEmailactualizarEstatusFxd    = "usp_Email_actualizarEstatusFxd";
        public const string uspEmailobtenerFolioBenPAPFXD   = "usp_Email_obtenerFolioBenPAPFXD";
        public const string uspEmailactualizarEstatusPapFxd = "usp_Email_actualizarEstatusPapFxd";
        public const string uspEmailobtenerFolioBenFXT      = "usp_Email_obtenerFolioBenFXT";
        public const string uspEmailactualizarEstatusFxt    = "usp_Email_actualizarEstatusFxt";
        public const string uspEmailobtenerFolioBenPAPFXT   = "usp_Email_obtenerFolioBenPAPFXT";
        public const string uspEmailactualizarEstatusPapFxt = "usp_Email_actualizarEstatusPapFxt";
        public const string uspEmailobtenerFolioBenFXI      = "usp_Email_obtenerFolioBenFXI";
        public const string uspEmailobtenerFolioBenPapFXI   = "usp_Email_obtenerFolioBenPapFXI";
        public const string uspEmailactualizarEstatusPapFxi = "usp_Email_actualizarEstatusPapFxi";
        public const string uspEmailobtenerFolioBenNOMFXD   = "usp_Email_obtenerFolioBenNOMFXD";
        public const string uspEmailactualizarEstatusNomFxd = "usp_Email_actualizarEstatusNomFxd";
        public const string uspEmailobtenerFolioBenDRE      = "usp_Email_obtenerFolioBenDRE";
        public const string uspEmailactualizarEstatusNomDre = "usp_Email_actualizarEstatusNomDre";
        public const string uspEmailobtenerFolioBenSPID     = "usp_Email_obtenerFolioBenSPID";
        public const string uspEmailactualizarEstatusSpid   = "usp_Email_actualizarEstatusSpid";
        public const string uspEmailobtenerFolioBenTEF      = "usp_Email_obtenerFolioBenTEF";
        public const string uspEmailactualizarEstatusTef    = "usp_Email_actualizarEstatusTef";
        public const string uspEmailobtenerFolioBenPAPTEF   = "usp_Email_obtenerFolioBenPAPTEF";
        public const string uspEmailactualizarEstatusPapTef = "usp_Email_actualizarEstatusPapTef";
        public const string uspEmailobtenerFolioBenPAPSPID  = "usp_Email_obtenerFolioBenPAPSPID";
        public const string uspEmailactualizarEstatusPapSpid = "usp_Email_actualizarEstatusPapSpid";
        public const string uspEmailobtenerFolioSatNepe      = "usp_Email_obtenerFolioSatNepe";
        public const string uspEmailobtenerFolioSatRef       = "usp_Email_obtenerFolioSatRef";
        public const string uspEmailobtenerFolioSatShcp      = "usp_Email_obtenerFolioSatShcp";
        public const string uspEmailobtenerFolioH2HFXD       = "usp_Email_obtenerFolioH2HFXD";
        public const string uspEmailactualizarEstatusH2HFXD  = "usp_Email_actualizarEstatusH2HFXD";
        public const string uspEmailobtenerFolioH2HFXI       = "usp_Email_obtenerFolioH2HFXI";
        public const string uspEmailactualizarEstatusH2HFXI  = "usp_Email_actualizarEstatusH2HFXI";
        public const string uspEmailactualizarEstatusH2HFXT  = "usp_Email_actualizarEstatusH2HFXT";
        public const string uspEmailobtenerFolioH2HFXT       = "usp_Email_obtenerFolioH2HFXT";
        public const string uspEmailobtenerFolioH2HSPID      = "usp_Email_obtenerFolioH2HSPID";
        public const string uspEmailactualizarEstatusH2HSPID = "usp_Email_actualizarEstatusH2HSPID";
        public const string uspEmailobtenerFolioH2HTEF       = "usp_Email_obtenerFolioH2HTEF";
        public const string uspEmailactualizarEstatusH2HTEF  = "usp_Email_actualizarEstatusH2HTEF";
        public const string uspEmailobtenerFolioBenCms       = "usp_Email_obtenerFolioBenCms";
        public const string uspEmailactualizarEstatusBenCms  = "usp_Email_actualizarEstatusBenCms";
        public const string uspEmailobtenerFolioNotiForex    = "usp_Email_obtenerFolioNotiForex";
        public const string uspEmailactualizarEstatusNotiForex = "usp_Email_actualizarEstatusNotiForex";
        public const string uspEmailobtenerFolioPreNotiForex   = "usp_Email_obtenerFolioPreNotiForex";
        public const string uspEmailactualizarEstatusPreNotiForex = "usp_Email_actualizarEstatusPreNotiForex";
        public const string uspEmailobtenerFolioCambioPwd         = "usp_Email_obtenerFolioCambioPwd";
        public const string uspEmailobtenerIdCliente              = "usp_Email_obtenerIdCliente";
        public const string uspEmailobtenerContactoPwd            = "usp_Email_obtenerContactoPwd";
        public const string uspEmailactualizarEstatusPwd          = "usp_Email_actualizarEstatusPwd";
        public const string uspEmailobtenerFolioDetalleCMS        = "usp_Email_obtenerFolioDetalleCMS";
        public const string uspEmailobtenerFolioOperacionesCMS    = "usp_Email_obtenerFolioOperacionesCMS";
        public const string uspEmailactualizarEstatusFcm          = "usp_Email_actualizarEstatusFcm";
        public const string uspEmailactualizarEstatusSat          = "usp_Email_actualizarEstatusSat";
        public const string uspEmailactualizarEstatusSatRef       = "usp_Email_actualizarEstatusSatRef";
        public const string uspEmailactualizarEstatusSatShcp     = "usp_Email_actualizarEstatusSatShcp";
        public const string uspEmailactualizarUsuarioEmail       = "usp_Email_actualizarUsuarioEmail";
        public const string uspEmailobtenerUsuarioMail           = "usp_Email_obtenerUsuarioMail";
        public const string uspEmailactualizarEstatusUsuarioCms  = "usp_Email_actualizarEstatusUsuarioCms";
        public const string uspEmailactualizarEstatusToken       = "usp_Email_actualizarEstatusToken";
        public const string uspEmailobtenerTokenExcepcion        = "usp_Email_obtenerTokenExcepcion";
        public const string uspEmailobtenerOperacionesHotscan    = "usp_Email_obtenerOperacionesHotscan";
        public const string uspEmailobtenerOperacionesHotscanDet = "usp_Email_obtenerOperacionesHotscanDet";
        public const string uspEmailactualizarOperacionesHotscan = "usp_Email_actualizarOperacionesHotscan";
        public const string uspEmailobtenerOperacionesHotscanSpid = "usp_Email_obtenerOperacionesHotscanSpid";
        public const string uspEmailobtenerOperacionesHotscanSpidDet = "usp_Email_obtenerOperacionesHotscanSpidDet";
        public const string uspEmailactualizarOperacionesHotscanSpid = "usp_Email_actualizarOperacionesHotscanSpid";
        public const string uspEmailobtenerAlertasCriticas = "usp_Email_obtenerAlertasCriticas";
        public const string uspEmailobtenerNotificacionEmailOtros = "usp_Email_obtenerNotificacionEmailOtros";
        public const string uspEmailobtenerSobregiros = "usp_Email_obtenerSobregiros";
        public const string uspEmailobtenerCuentasCashPool = "usp_Email_obtenerCuentasCashPool";
        public const string uspEmailobtenerCuentasFAD = "usp_Email_obtenerCuentasFAD";
        public const string uspEmailactualizarCuentasFAD = "usp_Email_actualizarCuentasFAD";
        public const string uspEmailobtenerFielesSPID = "usp_Email_obtenerFielesSPID";
        public const string uspEmailobtenerFielesSPIDDet = "usp_Email_obtenerFielesSPIDDet";
        public const string uspEmailactualizarCuentasFielInva = "usp_Email_actualizarCuentasFielInva";
        public const string uspEmailactualizarCuentasFielValidos = "usp_Email_actualizarCuentasFielValidos";
        public const string uspEmailobtenerCustomer = "usp_Email_obtenerCustomer";
        public const string uspEmailobtenerInicioFieles = "usp_Email_obtenerInicioFieles";
        public const string uspEmailinsertarInicioFieles = "usp_Email_insertarInicioFieles";
        public const string uspEmailobtenerEstatusFieles = "usp_Email_obtenerEstatusFieles";
        public const string uspEmailactualizarValidoRechFiel = "usp_Email_actualizarValidoRechFiel";
        public const string uspEmailobtenerInicioFielesSPID = "usp_Email_obtenerInicioFielesSPID";

        


        public const string quePasoFieles = "<BR><BR><STRONG> Nota ::: *** NO puede haber Fieles Invalidas o Pendientes,  Verifique que el WS de Banxico sea Valido ***</STRONG>";
        


        //Constantes tipos de transferencia & Avisos
        public const string tipoSER    = "SER";
        public const string tipoTAX    = "TAX";
        public const string tipoFXD    = "FXD";
        public const string tipoFXT    = "FXT";
        public const string tipoFXI    = "FXI";
        public const string tipoSPID   = "SPID";
        public const string tipoTEF    = "TEF";
        public const string tipoDRE    = "DRE";
        public const string tipoDPA    = "DPA";
        public const string tipoSPD    = "SPD";
        public const string tipoNOMINA = "NOMINA";

        public const string estadoFielValido   = "Válido";
        public const string estadoFielRevocado = "Revocado";
        public const string estadoFielCaduco   = "Caduco";
        public const string estadoFielInvalido = "INVALIDO";
        public const string tipoPAPFXD = "PAPFXD";
        public const string tipoPAPFXT = "PAPFXT";
        public const string tipoNOMFXD = "NOMFXD";

        public const string nombreTranSER    = "PAGO DE SERVICIOS";
        public const string nombreTranTAX    = "PAGO DE IMPUESTOS LOCALES";
        public const string nombreTranFXD    = "SPEI";
        public const string nombreTranFXT    = "TERCEROS";
        public const string nombreTranFXI    = "INTERNACIONAL";
        public const string nombreTranSPID   = "SPID";
        public const string nombreTranTEF    = "TEF";
        public const string nombreTranPAPFXI = "TRANSFERENCIA INTERNACIONAL";
        public const string nombreTranFXDNOM = "TRANSFERENCIA - NOMINA";
        public const string nombreTranDRE    = "PAGO DE IMPUESTOS - DEPOSITO REFERENCIADO";
        public const string nombreTranDPA    = "PAGO DE IMPUESTOS - DPA";
        public const string nombreTranBen    = "REGISTRO DE BENEFICIARIO";
        public const string nombreTranCCU    = "CAMBIO DE CONTRASEÑA (PASSWORD CHANGE)";

        //Constantes tipos de notificaciones FXD
        public const string nombreNotifFXD    = "TRANSFERENCIA SPEI";
        public const string nombreNotiFXT     = "TRASPASO ENTRE CUENTAS";
        public const string nombreNotiSPID    = "TRANSFERENCIA SPID";
        public const string nombreNotiTEF     = "TRANSFERENCIA TEF";

        //Constantes con las claves de eventos por cada tipo de beneficiario
        public const string claveEvento1  = "001";
        public const string claveEvento2  = "002";
        public const string claveEvento3  = "003";
        public const string claveEvento4  = "004";
        public const string claveEvento5  = "005";
        public const string claveEvento6  = "006";
        public const string claveEvento8  = "008";
        public const string claveEvento11 = "011";
        public const string claveEvento12 = "012";
        public const string moneda        = "MXP";

        //Constantes con los estatus de los acuses deñ SAT
        public const string estatusAcept = "ACEPTADO";
        public const string estatusRechz = "RECHAZADO";
        public const string estatusDesco = "DESCONOCIDO";
        public const string estatusFalla = "FALLA EN ENVIO";
        public const string tipoEnvio    = "ENVIO";
        public const string tipoAcuse    = "ACUSE";
        public const string plaza        = "México D.F.";

        //Constantes con las bases para los tipos de operaciones FCM

        public const string tipoENT_SPEI =  "ENT_SPEI";
        public const string tipoENT_SPEI_2 = "ENT_SPEI_2";
        public const string tipoFXD_Local = "FXD_Local";
        public const string tipoFXD_GPH = "FXD_GPH";
        public const string tipoNOM_FXD = "NOM_FXD";
        public const string tipoPaP_FXD = "PaP_FXD";
        public const string tipoENT_SPID = "ENT_SPID";
        public const string tipoSPID_Local = "SPID_Local";
        public const string tipoSPID_GPH = "SPID_GPH";
        public const string tipoPaP_SPID = "PaP_SPID";
        public const string tipoENT_TEF = "ENT_TEF";
        public const string tipoTEF_GPH = "TEF_GPH";
        public const string tipoPaP_TEF = "PaP_TEF";

        //Constantes con las bases para las operaciones FCM
        public const string baseEntSPEI = "Entrada SPEI";
        public const string baseEntSPID = "ENTRADA SPID";
        public const string baseSPEI = "SPEI";
        public const string baseSPEIL = "SPEI LOCAL";
        public const string baseSPEIG = "SPEI GPH";
        public const string baseNOMINA = "NÓMINA";
        public const string basePAGOP = "PAGO A PROVEEDORES SPEI";
        public const string baseSPIDL = "SPID LOCAL";
        public const string baseSPIDG = "SPID GPH";
        public const string basePAGOPS = "PAGO A PROVEEDORES SPID";
        public const string baseENTTEF = "ENTRADA TEF";
        public const string baseTEF = "TEF";
        public const string baseTEGG = "TEF GPH";
        public const string basePAGOT = "PAGO A PROVEEDORES TEF";
        public const string basePAGOS = "PAGO DE SERVICIOS";
        public const string basePAGOIM = "PAGO DE IMPUESTOS LOCALES";

        //Constantes con las bases para obtener los valores definidos desde el archivo de configuración
        public const string Para       = "MAIL_TO";
        public const string AuntoTest  = "MAIL_SUBJECT";
        public const string SenderTest = "MAIL_SENDER_TEST";

        //URL de validacion 
        public const string urlValida = "https://cms.mx.bk.mufg.jp/";
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace InfraestructuraEmail.Servicios
{
    public class DetallesTomiDao : RepositorioMaestro, IDetallesTomi
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesTomiDao(Configuracion configuracion = null)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _configuracion = configuracion;
        }

        #region Procesos Beneficiarios SER
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenSer>> RecuperarDetallesBenefSerAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenSer>(ConstantesServicio.uspEmailObtenerFolioBenefSer, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strFecha", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }


        /// <summary>
        /// Actualiza el folio de los email como enviado
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiario(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusBen, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion

        #region Procesos Beneficiarios TAX
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenTaxDTO>> RecuperarDetallesBenefTaxAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenTaxDTO>(ConstantesServicio.uspEmailobtenerFolioBenTAX, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado TAX
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioTax(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusTax, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion

        #region Procesos Beneficiarios FXD
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenFxdDTO>> RecuperarDetallesBenefFxdAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenFxdDTO>(ConstantesServicio.uspEmailobtenerFolioBenFXD, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado FXD
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioFxd(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusFxd, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion

        #region Procesos Beneficiarios PAPFXD
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenPapFxdDTO>> RecuperarDetallesBenefPapFxdAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenPapFxdDTO>(ConstantesServicio.uspEmailobtenerFolioBenPAPFXD, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
                
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado PAPFXD
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioPapFxd(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusPapFxd, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Beneficiarios PAPFXD
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenFxtDTO>> RecuperarDetallesBenefFxtAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenFxtDTO>(ConstantesServicio.uspEmailobtenerFolioBenFXT, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado FXT
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioFxt(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusFxt, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion

        #region Procesos Beneficiarios PAPFXT
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenPapFxtDTO>> RecuperarDetallesBenefPapFxtAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenPapFxtDTO>(ConstantesServicio.uspEmailobtenerFolioBenPAPFXT, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
                
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado PAPFXT
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioPapFxt(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusPapFxt, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion

        #region Procesos Beneficiarios FXI
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenFxiDTO>> RecuperarDetallesBenefFxiAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenFxiDTO>(ConstantesServicio.uspEmailobtenerFolioBenFXI, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado FXI
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioFxi(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusPapFxt, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion

        #region Procesos Beneficiarios PAPFXI
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenPapFxiDTO>> RecuperarDetallesBenefPapFxiAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenPapFxiDTO>(ConstantesServicio.uspEmailobtenerFolioBenPapFXI, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado FXI
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioPapFxi(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusPapFxi, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Beneficiarios NOMFXD
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenNomFxdDTO>> RecuperarDetallesBenefNomFxdAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenNomFxdDTO>(ConstantesServicio.uspEmailobtenerFolioBenNOMFXD, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado NOMFXD
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioNomFxd(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusNomFxd, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Beneficiarios DRE
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenDreDTO>> RecuperarDetallesBenefDreAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenDreDTO>(ConstantesServicio.uspEmailobtenerFolioBenDRE, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado NOMFXD
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioDre(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusNomDre, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Beneficiarios SPID
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenSpidDTO>> RecuperarDetallesBenefSpidAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenSpidDTO>(ConstantesServicio.uspEmailobtenerFolioBenSPID, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado SPID
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioSpid(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusSpid, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Beneficiarios TEF
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenTefDTO>> RecuperarDetallesBenefTefAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenTefDTO>(ConstantesServicio.uspEmailobtenerFolioBenTEF, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado TEF
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioTef(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusTef, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Beneficiarios PAPTEF
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenPapTefDTO>> RecuperarDetallesBenefPapTefAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenPapTefDTO>(ConstantesServicio.uspEmailobtenerFolioBenPAPTEF, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado PAPTEF
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioPapTef(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusTef, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Beneficiarios PAPSPID
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenPapSpidDTO>> RecuperarDetallesBenefPapSpidAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenPapSpidDTO>(ConstantesServicio.uspEmailobtenerFolioBenPAPSPID, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado PAPSPID
        /// </summary>
        public async Task<bool> ActualizaFolioBeneficiarioPapSpid(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusPapSpid, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos H2H FXD
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoH2HFxdDTO>> RecuperarDetallesH2HFxdAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoH2HFxdDTO>(ConstantesServicio.uspEmailobtenerFolioH2HFXD, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado H2HFXD
        /// </summary>
        public async Task<bool> ActualizaFolioH2HFxd(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusH2HFXI, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos H2H FXI
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoH2HFxiDTO>> RecuperarDetallesH2HFxiAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoH2HFxiDTO>(ConstantesServicio.uspEmailobtenerFolioH2HFXI, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado H2HFXI
        /// </summary>
        public async Task<bool> ActualizaFolioH2HFxi(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusH2HFXI, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos H2H FXT
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoH2HFxtDTO>> RecuperarDetallesH2HFxtAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoH2HFxtDTO>(ConstantesServicio.uspEmailobtenerFolioH2HFXT, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado H2HFXT
        /// </summary>
        public async Task<bool> ActualizaFolioH2HFxt(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusH2HFXT, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos H2H SPID
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoH2HSpidDTO>> RecuperarDetallesH2HSpidAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoH2HSpidDTO>(ConstantesServicio.uspEmailobtenerFolioH2HSPID, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado H2HSPID
        /// </summary>
        public async Task<bool> ActualizaFolioH2HSpid(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusH2HSPID, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos H2H TEF
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoH2HTefDTO>> RecuperarDetallesH2HTefAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoH2HTefDTO>(ConstantesServicio.uspEmailobtenerFolioH2HTEF, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado H2HTEF
        /// </summary>
        public async Task<bool> ActualizaFolioH2HTef(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusH2HTEF, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos NOTIFICACIONES FOREX
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoNotiForexDTO>> RecuperarDetallesNotiForexAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoNotiForexDTO>(ConstantesServicio.uspEmailobtenerFolioNotiForex, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoNotiForexDTO>> RecuperarDetallesPreNotiForexAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoNotiForexDTO>(ConstantesServicio.uspEmailobtenerFolioPreNotiForex, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }


        /// <summary>
        /// Actualiza el folio de los email como enviado NOTI FOREX
        /// </summary>
        public async Task<bool> ActualizaFolioNotiForex(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusNotiForex, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado PRENOTI FOREX
        /// </summary>
        public async Task<bool> ActualizaFolioPreNotiForex(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusPreNotiForex, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Operaciones FCM
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoFcmDTO>> RecuperarDetallesOperacFcmAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoFcmDTO>(ConstantesServicio.uspEmailobtenerFolioDetalleCMS);

            return resultado.Data;
        }

        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoOperaFcmDTO>> RecuperarDetallesOperacAsync(long folio, string tipoOperac)
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoOperaFcmDTO>(ConstantesServicio.uspEmailobtenerFolioOperacionesCMS, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strTipo", tipoOperac));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strFolio", folio.ToString()));
            });

            return resultado.Data;
        }
        /// <summary>
        /// Actualiza el folio de los email como enviado de las operaciones FCM
        /// </summary>
        public async Task<bool> ActualizaFolioFcm(long idSolicitud, string strTipo)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusFcm, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strTipo", strTipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@bintFolio", idSolicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Operaciones Hotscan
        /// <summary>
        /// Obtiene los detalles de las operaciones de Hotscan de tipos SPID
        /// </summary>
        public async Task<IEnumerable<ComplementoOperHotsSpidDTO>> RecuperarDetallesOperHotscanSpid()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoOperHotsSpidDTO>(ConstantesServicio.uspEmailobtenerOperacionesHotscanSpid);

            return resultado.Data;
        }

        /// <summary>
        /// Obtiene los detalles de las operaciones de Hotscan de tipos SPID
        /// </summary>
        public async Task<IEnumerable<ComplementoOperacHotscanSpidDetDTO>> RecuperarDetallesOperHotscanSpidDet(long idAlerta)
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoOperacHotscanSpidDetDTO>(ConstantesServicio.uspEmailobtenerOperacionesHotscanSpidDet, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Idalerta", idAlerta));
            });
            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado de las operaciones hotscan de tipo SPID
        /// </summary>
        public async Task<bool> ActualizaFolioHotscanSpid(long idAlerta)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarOperacionesHotscanSpid, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idAlerta", idAlerta));
            });

            return resultado.Exito;
        }

        #endregion

        #region Notificaciones Otros Email
        /// <summary>
        /// Obtiene los detalles de las notificaciones de otros emails
        /// </summary>
        public async Task<IEnumerable<ComplementoEmailOtrosDTO>> RecuperarDetallesNotiEmailOtros()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoEmailOtrosDTO>(ConstantesServicio.uspEmailobtenerNotificacionEmailOtros);

            return resultado.Data;
        }
        #endregion

        #region Notificaciones Sobregiros
        /// <summary>
        /// Obtiene los detalles de las notificaciones de otros emails
        /// </summary>
        public async Task<IEnumerable<ComplementoSobregirosDTO>> RecuperarDetallesSobregiros()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoSobregirosDTO>(ConstantesServicio.uspEmailobtenerSobregiros);

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email con operaciones de sobregiros
        /// </summary>
        public async Task<bool> ActualizaFolioSobregiro()
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailobtenerSobregiros);

            return resultado.Exito;
        }
        #endregion

        #region Notificaciones Cuentas Cash Pool
        /// <summary>
        /// Obtiene los detalles de las notificaciones de otros emails
        /// </summary>
        public async Task<IEnumerable<ComplementoCuentaCashPoolDTO>> RecuperarDetallesCuentaCashPool()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoCuentaCashPoolDTO>(ConstantesServicio.uspEmailobtenerCuentasCashPool);

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email con operaciones de sobregiros
        /// </summary>
        /*public async Task<bool> ActualizaFolioSobregiro()
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailobtenerSobregiros);

            return resultado.Exito;
        }*/
        #endregion

        #region Notificaciones Cuentas FAD
        /// <summary>
        /// Obtiene los detalles de las notificaciones de las cuentas de tipo FAD
        /// </summary>
        public async Task<IEnumerable<ComplementoCuentasFADDTO>> RecuperarDetallesCuentasFAD()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoCuentasFADDTO>(ConstantesServicio.uspEmailobtenerCuentasFAD);

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email con operaciones de sobregiros
        /// </summary>
        public async Task<bool> ActualizaFolioCuentasFad()
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarCuentasFAD);

            return resultado.Exito;
        }
        #endregion

        #region Notificaciones FIELES SID
        /// <summary>
        /// Obtiene los detalles de las notificaciones de las cuentas de tipo FAD
        /// </summary>
        public async Task<IEnumerable<ComplementoFielValidoDTO>> RecuperarDetallesIniFielesSPID()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoFielValidoDTO>(ConstantesServicio.uspEmailobtenerInicioFielesSPID);

            return resultado.Data;
        }

        /// <summary>
        /// Obtiene los detalles de las notificaciones de las cuentas de tipo FAD
        /// </summary>
        public async Task<IEnumerable<ComplementoFielValidoDTO>> RecuperarDetallesFielesSPID()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoFielValidoDTO>(ConstantesServicio.uspEmailobtenerFielesSPID);

            return resultado.Data;
        }

        /// <summary>
        /// Obtiene los detalles de las notificaciones de los fieles de tipo SPID
        /// </summary>
        public async Task<IEnumerable<ComplementoFielDetDTO>> RecuperarDetallesFielesSPIDDet()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoFielDetDTO>(ConstantesServicio.uspEmailobtenerFielesSPIDDet);

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email con operaciones de fieles de tipo invalidas
        /// </summary>
        public async Task<bool> ActualizaFolioFielInvalidas()
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarCuentasFielInva);

            return resultado.Exito;
        }

        /// <summary>
        /// Actualiza el folio de los email con operaciones de fieles de tipo invalidas
        /// </summary>
        public async Task<bool> ActualizaFolioFielValidas(int validos, int rechazados)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarCuentasFielValidos);

            return resultado.Exito;
        }

        #endregion

        #region Notificaciones FIELES SID
        /// <summary>
        /// Obtiene los detalles de las notificaciones del inicio de los fieles de tipo SPID
        /// </summary>
        public async Task<IEnumerable<ComplementoInicioFielDTO>> RecuperarDetallesInicioFieles()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoInicioFielDTO>(ConstantesServicio.uspEmailobtenerInicioFieles);
            
            if (!resultado.Exito)
                throw new Exception(resultado.MensajeError);

            return resultado.Data;
        }

        /// <summary>
        /// Inserta registro con el total de cuentas fieles de tipo SPID
        /// </summary>
        public async Task<bool> InsertarFolioInicioFiel(int total)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailinsertarInicioFieles, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TotalReg", total));
            });

            return resultado.Exito;
        }

        /// <summary>
        /// Obtiene los detalles de las notificaciones de las cuentas de tipo FAD
        /// </summary>
        public async Task<IEnumerable<ComplementoValidasFielDTO>> RecuperarDetallesValidasFielesSPID()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoValidasFielDTO>(ConstantesServicio.uspEmailobtenerEstatusFieles);

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email con operaciones de fieles de tipo invalidas
        /// </summary>
        public async Task<bool> ActualizaFolioFielValidasRechazadas()
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarValidoRechFiel);

            return resultado.Exito;
        }

        #endregion
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace InfraestructuraEmail.Servicios
{
    public class DetallesTomserDao : RepositorioMaestro, IDetallesTomser
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesTomserDao(Configuracion configuracion = null)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMSER, null, null)
        {
            _configuracion = configuracion;
        }
        #region Procesos Solicitudes
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoSolDTO>> RecuperarDetallesSolicitudAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoSolDTO>(ConstantesServicio.uspObtenerFolioSolicitud);
            
            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el estatus de los emails de solicitudes
        /// </summary>
        public async Task<bool> ActualizarEstatusEmailSol(long id_solicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusSol, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", id_solicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Beneficiario SER
        /// <summary>
        /// Obtiene los detalles de los contactos
        /// </summary>
        public async Task<IEnumerable<ComplementoBenSerSec>> RecuperarDetallesContactosAsync(decimal customerCode, string claveEvento)
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenSerSec>(ConstantesServicio.uspEmailobtenerContacto, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@intIdCliente", customerCode));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@claveEvento" , claveEvento));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Obtiene los detalles de los contactos
        /// </summary>
        public async Task<IEnumerable<ComplementoAvisoBeneficiario>> RecuperarEmailOrdenante(string strTipo, string strCliente, long idplantilla )
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoAvisoBeneficiario>(ConstantesServicio.uspEmailobtenerCorreoOrdenante, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strTipo", strTipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strCliente", strCliente));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@intPlantilla", idplantilla));
            });

            return resultado.Data;
        }

        #endregion

        #region Procesos SAT NEPE
        /// <summary>
        /// Obtiene los detallesde la información de los acuses del SAT para el tipo NEPÉ
        /// </summary>
        public async Task<IEnumerable<ComplementoSatNepeDTO>> RecuperarDetallesSatNepe()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoSatNepeDTO>(ConstantesServicio.uspEmailobtenerFolioSatNepe);

            return resultado.Data;
        }
        #endregion

        #region Procesos SAT REF
        /// <summary>
        /// Obtiene los detallesde la información de los acuses del SAT para el tipo REF
        /// </summary>
        public async Task<IEnumerable<ComplementoSatRefDTO>> RecuperarDetallesSatRef()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoSatRefDTO>(ConstantesServicio.uspEmailobtenerFolioSatRef);

            return resultado.Data;
        }
        #endregion

        #region Procesos SAT SHCP
        /// <summary>
        /// Obtiene los detallesde la información de los acuses del SAT para el tipo SHCP
        /// </summary>
        public async Task<IEnumerable<ComplementoSatNepeDTO>> RecuperarDetallesSatShcp()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoSatNepeDTO>(ConstantesServicio.uspEmailobtenerFolioSatShcp);

            return resultado.Data;
        }
        #endregion

        #region Procesos BEN CMS
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoBenCmsDTO>> RecuperarDetallesBenCms()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoBenCmsDTO>(ConstantesServicio.uspEmailobtenerFolioBenCms);

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el estatus de los emails de las notificaciones de beneficiarios CMS
        /// </summary>
        public async Task<bool> ActualizarEstatusEmailBenCms(long id_solicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusBenCms, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", id_solicitud));
            });

            return resultado.Exito;
        }
        #endregion

        #region Procesos Avisos PWD
        /// <summary>
        /// Obtiene los paquetes recibidos SPEI para el detalle CDA.
        /// </summary>
        public async Task<IEnumerable<ComplementoContactoDTO>> RecuperarDetallesContactosPwdAsync(string usuario, long idCliente)
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoContactoDTO>(ConstantesServicio.uspEmailobtenerContactoPwd, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@intIdCliente", idCliente));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strUsuario", usuario));
            });
            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado de las operaciones SAT
        /// </summary>
        public async Task<bool> ActualizaFolioSAT(long idSolicitud, string strCampo)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusSat, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strCampo", strCampo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@bintFolio", idSolicitud));
            });

            return resultado.Exito;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado de las operaciones SAT
        /// </summary>
        public async Task<bool> ActualizaFolioSATREF(long idSolicitud, string strCampo)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusSatRef, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strCampo", strCampo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@bintFolio", idSolicitud));
            });

            return resultado.Exito;
        }

        /// <summary>
        /// Actualiza el folio de los email como enviado de las operaciones SAT
        /// </summary>
        public async Task<bool> ActualizaFolioSATSHCP(long idSolicitud, string strCampo)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusSatShcp, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strCampo", strCampo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@bintFolio", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion

        #region Procesos Avisos Olvido PWD
        /// <summary>
        /// Actualiza el folio de los email olvido de pwd
        /// </summary>
        public async Task<bool> ActualizaFolioOlvidoPwd(string newEmail, string strUsuario)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusUsuarioCms, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strEmail", newEmail));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strUsuario", strUsuario));
            });

            return resultado.Exito;
        }

        #endregion

        #region Operaciones Hotscan
        /// <summary>
        /// Obtiene los detalles de las operaciones de Hotscan
        /// </summary>
        public async Task<IEnumerable<ComplementoOperacHotscanDTO>> RecuperarDetallesOperHotscan()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoOperacHotscanDTO>(ConstantesServicio.uspEmailobtenerOperacionesHotscan);

            return resultado.Data;
        }

        public async Task<IEnumerable<ComplementoOperacHotscanDetDTO>> RecuperarDetallesOperHotscanDet(long idScan)
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoOperacHotscanDetDTO>(ConstantesServicio.uspEmailobtenerOperacionesHotscanDet, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idScan", idScan));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de la operacion de hotscan 
        /// </summary>
        public async Task<bool> ActualizaOperacionHotscan(long idScan)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarOperacionesHotscan, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idScan", idScan));
            });

            return resultado.Exito;
        }

        #endregion
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace InfraestructuraEmail.Repositorios
{
    public class DetallesBotDao : RepositorioMaestro, IDetallesBot
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesBotDao(Configuracion configuracion = null)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.CMS, null, null)
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Obtiene los id de clientes filtrando por el nombre de usuario
        /// </summary>
        public async Task<IEnumerable<ComplementoClienteDTO>> RecuperarDetallesClientesAsync(string usuario)
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoClienteDTO>(ConstantesServicio.uspEmailobtenerIdCliente, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strUsuario", usuario));
            });
            return resultado.Data;
        }

        /// <summary>
        /// Obtiene las solicitudes del olvido de pwd
        /// </summary>
        public async Task<IEnumerable<ComplementoUsuarioEmailDTO>> RecuperarDetallesOlvPwdAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoUsuarioEmailDTO>(ConstantesServicio.uspEmailobtenerUsuarioMail);

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza las solicitudes de email de olvido de pwd
        /// </summary>
        public async Task<bool> ActualizaFolioOlvidoPwdCms(string newEmail, string strUsuario)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusUsuarioCms, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strEmail", newEmail));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strUsuario", strUsuario));
            });

            return resultado.Exito;
        }

        #region Tokens con excepcion
        /// <summary>
        /// Obtiene la información de los tokens que contengan alguna excepcion
        /// </summary>
        public async Task<IEnumerable<ComplementoTokenExcepDTO>> RecuperarDetallesTokenExcepAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoTokenExcepDTO>(ConstantesServicio.uspEmailobtenerTokenExcepcion);
            
            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el estatus de las solicitudes con excepción de tokens
        /// </summary>
        public async Task<bool> ActualizaTokenExcepcion(int idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusToken, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace InfraestructuraEmail.Servicios
{
    public class DetallesBotLogDao :RepositorioMaestro, IDetallesBotLog
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesBotLogDao(Configuracion configuracion = null)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.BOTLOG, null, null)
        {
            _configuracion = configuracion;
        }

        #region Procesos Notificaciones Pwd
        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoAvisoPwdDTO>> RecuperarDetallesAvisoPwdAsync()
        {
            var resultado = await EjecutarStoredProcedureQuery<ComplementoAvisoPwdDTO>(ConstantesServicio.uspEmailobtenerFolioCambioPwd, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@dateFechaE", ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema))));
            });

            return resultado.Data;
        }

        /// <summary>
        /// Actualiza el folio de las solicitudes de actualizaci[on de PWD
        /// </summary>
        public async Task<bool> ActualizaFolioAvisoPwd(long idSolicitud)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarEstatusPwd, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdSolicitud", idSolicitud));
            });

            return resultado.Exito;
        }

        #endregion
    }
}

using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using AplicacionEmail.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AplicacionEmail.Interfaces;

namespace InfraestructuraEmail.Repositorios
{
    public class DetallesEnlaceDao : RepositorioMaestro, IDetallesEnlace
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesEnlaceDao(Configuracion configuracion = null)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI, null, null)
        {
            _configuracion = configuracion;
        }


        /// <summary>
        /// Actualiza el folio de los email como enviado de las operaciones FCM
        /// </summary>
        public async Task<bool> ActualizaFolioOlvidoPwd(string nuevoEmail, string usuario)
        {
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspEmailactualizarUsuarioEmail, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strEmail"  , nuevoEmail));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@strUsuario", usuario));
            });

            return resultado.Exito;
        }

    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Threading.Tasks;
using System.Globalization;
using System.Text;

namespace AplicacionEmail.Servicios
{
    public class ENT_ENVIANOTIFOREX : IENT_ENVIANOTIFOREX
    {
        private Configuracion _configuracionBase;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_ENVIANOTIFOREX(Configuracion configuracion) {
            _configuracionBase = configuracion;
        }

        public async  Task<bool> EnviarNotificacionesForex(string mailTo, string mailCC, ComplementoNotiForexDTO complementoNotiForex)
        {
            try
            {
                if (complementoNotiForex == null)
                    throw new ArgumentNullException(nameof(complementoNotiForex));
                string lsFechaOperacion   = null;
                string lsFechaLiquidacion = null;
                string lsMensaje = null;
                string lsNombreTransferencia = "FX" + complementoNotiForex.PRODUCTO;

                lsFechaOperacion = complementoNotiForex.FECHACAPTURA.ToString("yyyyMMdd");
                lsFechaLiquidacion = complementoNotiForex.FECHAVALOR.ToString("yyyyMMdd");


                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>NOTIFICACIONES MUFG Bank México (NOTIFICATIONS MUFG Bank Mexico)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Estimado Cliente (Dear Client):</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='800' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Le notificamos que la operación que en seguida se describe se ha ejecutado y registrado por MUFG Bank México, S.A., Institución de Banca Múltiple Filial:</td></tr>");
                sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Hereby we notice that operation described herein has been executed and registered by MUFG Bank Mexico, S.A., Institución de Banca Múltiple Filial:</td></tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Operación (Transaction):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(lsNombreTransferencia)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Tipo (Class): **</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.TIPOOPERACION)}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha Operación (Transaction Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFechaOperacion}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha de Liquidación (Value Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFechaLiquidacion}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Hora Operación (Transaction Time):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.HORACAPTURA)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Cliente (Customer):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.NOMBRE)}</td></tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Divisa a entregar (Currency to be delivered):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.DIVISAENTREGACLIENTE)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Divisa a recibir (Currency to be received):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.DIVISAADQUIERECLIENTE)}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Importe a entregar (Amount to be delivered):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.MONTOENTREGACLIENTE.ToString("C"))}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Importe a recibir (Amount to be received):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.MONTOADQUIERECLIENTE.ToString("C"))}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cuenta de Débito (Debit Account):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.CUENTAENTREGACLIENTE)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cuenta de Crédito (Credit Account):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.CUENTAADQUIERECLIENTE)}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Tipo de Cambio (Exchange Rate):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.ECCLIENTEREAL)}</td></tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;No. de Autorización (Deal Number):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.FOLIO_TAS)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Plaza (Location):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(ConstantesServicio.plaza)}</td>" +
                    $"</tr>");

                sb.AppendLine("<tr><td class='dato_tabla' width='100%' colspan='4' align='right'>&nbsp;** Esta operación se deberá leer en sentido del Banco.</td></tr>");
                sb.AppendLine("<tr><td class='dato_tabla' width='100%' colspan='4' align='right'>&nbsp;*** Esta operación no genera ninguna comisión.</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>AVISO LEGAL:</B> A través de éste correo electrónico se le notifica que MUFG Bank México, S.A., Institución de Banca Múltiple Filial (indistintamente, \"MUFG\" o el \"Banco\") ha ejecutado y registrado la operación antes señalada, debidamente instruida por su representante, con cargo a la cuenta y/o número de crédito indicados, según sea el caso. El contenido de este correo y su envío, se realiza en cumplimiento de las disposiciones normativas aplicables y no representa un comprobante fiscal de la operación.</p></span>");
                sb.AppendLine("<span class='Nota'><p>La fecha y hora de este mensaje puede variar respecto a la fecha y hora real de la ejecución de la operación debido, entre otras causas, a la zona horaria programada en su computadora o la configuración de sus servicios de correo. Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de la misma queda estrictamente prohibida.</p></span>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>LEGAL NOTICE:</B> Through communication you are noticed that MUFG Bank Mexico, S.A. Institución de Banca Múltiple Filial (indistinctively \"MUFG\" or the \"Bank\") has executed and registered the above mentioned transaction, duly instructed by your legal representative.</p></span>");
                sb.AppendLine("<span class='Nota'><p>Hour and date in this message may vary in connection to the real date and time of the operation execution due to, among other causes, to the time zone programed in your computer or your email service configuration.</p></span>");
                sb.AppendLine("</html>");

                lsMensaje = sb.ToString();



                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var asunto = string.IsNullOrEmpty(_configuracionBase.SujetoDefault)
                              ? ConstantesServicio.AvisoNotificacion
                              : $"{ConstantesServicio.AvisoNotificacion}[{_configuracionBase.SujetoDefault}]";

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? mailTo,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? mailCC,
                    CopiaOculta = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = asunto,
                    Mensaje = lsMensaje,
                    ArchivosAdjuntos = null
                });
                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoAvisoEnvioNotiForex : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeAvisoEnvioNotifForez,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }

        public async Task<bool> EnviarPreNotificacionesForex(string mailTo, string mailCC, ComplementoNotiForexDTO complementoNotiForex)
        {
            try
            {
                string lsFechaOperacion = null;
                string lsFechaLiquidacion = null;
                string lsMensaje = null;
                string lsNombreTransferencia = "FX" + complementoNotiForex.PRODUCTO;

                lsFechaOperacion = complementoNotiForex.FECHACAPTURA.ToString("yyyyMMdd");
                lsFechaLiquidacion = complementoNotiForex.FECHAVALOR.ToString("yyyyMMdd");
                if (complementoNotiForex == null)
                    throw new ArgumentNullException(nameof(complementoNotiForex));
                string lsFechaOperacion   = null;

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");

                sb.AppendLine("<center><span class='titulo_general'>PRE-NOTIFICACIONES MUFG Bank México (PRE-NOTIFICATIONS MUFG Bank Mexico)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Estimado Cliente (Dear Client):</span>");

                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='800' borderColor='#E0E0E0'>");

                sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Le notificamos que la operación que en seguida se describe se ha solicitado a MUFG Bank México, S.A., Institución de Banca Múltiple Filial:</td></tr>");

                sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Hereby we notice that operation described herein has been requested to MUFG Bank Mexico, S.A., Institución de Banca Múltiple Filial:</td></tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Operación (Transaction):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(lsNombreTransferencia)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Tipo (Class): **</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.TIPOOPERACION)}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha Operación (Transaction Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFechaOperacion}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha de Liquidación (Value Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFechaLiquidacion}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Hora Operación (Transaction Time):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.HORACAPTURA)}</td></tr>");

                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Cliente (Customer):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.NOMBRE)}</td></tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Divisa a entregar (Currency to be delivered):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.DIVISAENTREGACLIENTE)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Divisa a recibir (Currency to be received):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.DIVISAADQUIERECLIENTE)}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Importe a entregar (Amount to be delivered):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.MONTOENTREGACLIENTE.ToString())}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Importe a recibir (Amount to be received):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.MONTOADQUIERECLIENTE.ToString())}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cuenta de Débito (Debit Account):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.CUENTAENTREGACLIENTE)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cuenta de Crédito (Credit Account):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.CUENTAADQUIERECLIENTE)}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Tipo de Cambio (Exchange Rate):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.ECCLIENTEREAL)}</td></tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Folio de Recepción (Folio Number):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.ID_AUTORIZACION)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Plaza (Location):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(ConstantesServicio.plaza)}</td>" +
                    $"</tr>");

                sb.AppendLine("<tr><td class='dato_tabla' width='100%' colspan='4' align='right'>&nbsp;** Esta operación se deberá leer en sentido del Banco.</td></tr>");
                sb.AppendLine("<tr><td class='dato_tabla' width='100%' colspan='4' align='right'>&nbsp;*** Esta operación no genera ninguna comisión.</td></tr>");

                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");

                sb.AppendLine("<BR><BR><span class='Nota'><p><B>AVISO LEGAL.</B> A través de éste correo electrónico se le notifica que MUFG Bank México, S.A., Institución de Banca Múltiple Filial (indistintamente, \"MUFG\" o el \"Banco\") ha recibido una solicitud de su representante para realizar la transacción arriba descrita con cargo a la cuenta y/o número de crédito indicados, según sea el caso. La ejecución de la operación se encuentra sujeta a la capacidad administrativa de MUFG; en consecuencia ésta comunicación no constituye confirmación o aprobación por parte del Banco para su ejecución. Para la cancelación de la operación, favor de notificar por escrito al Banco, antes de las 15:30 horas (Ciudad de México) del día en curso; el silencio u omisión en la notificación señalada autoriza al Banco para ejecutar la operación sin responsabilidad alguna.</p></span>");

                sb.AppendLine("<span class='Nota'><p>Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de la misma queda estrictamente prohibida. Si usted no es el destinatario o no es una persona autorizada por éste para recibir la información contenida en él favor de notificar inmediatamente al remitente y destruir el mensaje absteniéndose de leer, reproducir, transmitir, almacenar, divulgar o usar en cualquier forma y por cualquier medio la información contenida en él.</p></span>");

                sb.AppendLine("<BR><BR><span class='Nota'><p><B>LEGAL NOTICE.</B> Through communication you are noticed that MUFG Bank Mexico, S.A. Institución de Banca Múltiple Filial has received a request from your legal representative to execute the above mentioned transaction.</p></span>");

                sb.AppendLine("<span class='Nota'><p>This email and any of the attached documents may contain confidential and privileged information. Unauthorized disclosure or distribution is strictly prohibited.</p></span>");

                sb.AppendLine("</html>");

                lsMensaje = sb.ToString();

                _configuracionBase.CorreoParaDefault = _configuracionBase.CorreoSenderTest ??ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var asunto = string.IsNullOrEmpty(_configuracionBase.SujetoDefault)
                              ? ConstantesServicio.AvisoPreNotificacion
                              : $"{ConstantesServicio.AvisoPreNotificacion}[{_configuracionBase.SujetoDefault}]";

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? mailTo,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? mailCC,
                    CopiaOculta = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = asunto,
                    Mensaje = lsMensaje,
                    ArchivosAdjuntos = null
                });
                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoAvisoEnvioPreNotiForex : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeAvisoEnvioNotifForez,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_NOTIFIOTROS : ProcesamientoEmailOtros, IProcesamientoEmailOtros, IENT_NOTIFIOTROS
    {
        private Configuracion _configuracion;
        IDetallesTomi         _detallesTomi;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_NOTIFIOTROS(Configuracion configuracion, IDetallesTomi detallesTomi)
            : base(configuracion, detallesTomi)
        {
            
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
        }

        public async Task ProcesarNotificacionesEmailOtrosAsync()
        {
            try
            {
                string email = null;
                string emailcc = null;
                string emailbcc = null;
                _configuracion.ParametrosSistema?.TryGetValue("NotifOtro_mailTo", out email);
                _configuracion.ParametrosSistema?.TryGetValue("NotifOtro_mailCc", out emailcc);
                _configuracion.ParametrosSistema?.TryGetValue("NotifOtro_mailBCc", out emailbcc);

                string lsMensaje = null;
                //Obtiene los atributos de los complementos de las notificaciones de otros tipos de email
                Config.EscribirLog("INFO|ProcesarNotificacionesEmailOtrosAsync|Procesando operaciones otras notificaciones de email ");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoEmailOtrosDTO> complementoEmaiLOtros = await this.ObtencionDatosEmailOtros();

                if (complementoEmaiLOtros.Count is 0)
                {
                    var sb = new StringBuilder();

                    sb.AppendLine("<html>");
                    sb.AppendLine("<head>");
                    sb.AppendLine("<style type='text/css'>");
                    sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:22px; background-color:#000000; color:#FFFFFF; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:16px; color:#A21403; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#F5F6F9; text-decoration:none; }");
                    sb.AppendLine(".header_white   { font-family: sans-serif; font-size:11px; background-color:#066A9D; color:#FFFFFF; text-decoration:none; }");
                    sb.AppendLine(".header_white1  { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#FFFFFF; text-decoration:none; }");
                    sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:09px; color:#1C1C1D; text-decoration:none; }");
                    sb.AppendLine(".dato_rojo      { font-family: sans-serif; font-size:09px; color:#F5240B; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color:#A0A0A0; text-decoration:none; }");
                    sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color:#FF0000; text-decoration:none; }");
                    sb.AppendLine("table { border: 1 #030303; }");
                    sb.AppendLine("</style>");
                    sb.AppendLine("</head>");
                    sb.AppendLine("<body>");

                    sb.AppendLine("<TABLE border='0' cellspacing='1' cellpadding='1' width='701' borderColor='#E0E0E0'>");

                    sb.AppendLine("<TR>");
                    sb.AppendLine("<TD class='titulo_general'><CENTER>NOTIFICACION CIERRE DE DIA S P E I</CENTER></TD>");
                    sb.AppendLine("</TR>");

                    sb.AppendLine("<TR>");
                    sb.AppendLine("<TD class='header_white1'><CENTER>&nbsp;</CENTER></TD>");
                    sb.AppendLine("</TR>");

                    sb.AppendLine(
                        $"<TR><TD class='dato_tabla' align='right'><STRONG>{DateTime.Now:dd/MM/yyyy HH:mm:ss}</STRONG></TD></TR>");

                    sb.AppendLine("<TR>");
                    sb.AppendLine("<TD>");
                    sb.AppendLine("<BR>");

                    sb.AppendLine("<span class='titulo_tabla' align='left'>&nbsp;</span>");

                    sb.AppendLine("<table border='0' cellspacing='1' cellpadding='1' width='60%' borderColor='#E0E0E0'>");

                    sb.AppendLine("<tr>");
                    sb.AppendLine("<td colspan='7' class='header_white1'>&nbsp;</td>");
                    sb.AppendLine("</tr>");

                    sb.AppendLine("<tr>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Hora</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Detalle</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Comentario</td>");
                    sb.AppendLine("</tr>");

                    //Realiza el recorrido de los datos
                    foreach (ComplementoEmailOtrosDTO NotifOtroEmail in complementoEmaiLOtros)
                    {
                        sb.AppendLine($"<tr>" +
                                      $"<td class='dato_tabla' align='CENTER'>{NotifOtroEmail.hora}</td>" +
                                      $"<td class='dato_tabla'>&nbsp;{NotifOtroEmail.detalle}</td>" +
                                      $"<td class='dato_tabla' align='CENTER'>{NotifOtroEmail.descripcion}</td>" +
                                      $"</tr>");
                    }

                    sb.AppendLine("</table>");
                    sb.AppendLine("<BR><BR>");
                    sb.AppendLine("<span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");

                    sb.AppendLine("</TR>");
                    sb.AppendLine("</TD>");
                    sb.AppendLine("</TABLE>");

                    sb.AppendLine("</body>");
                    sb.AppendLine("</html>");

                    lsMensaje = sb.ToString();

                    Config.EscribirLog($"INFO|ProcesarAlertasCriticasAsync | Enviando el Correo -->  :'{email}' ");

                   

                    _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                    _configuracion.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                    _configuracion.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                    var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                               ? ConstantesServicio.AvisoNotificacionEmailOtros
                               : _configuracion.SujetoDefault;

                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                        Para = _configuracion.CorreoParaDefault ?? email,
                        ConCopia = _configuracion.CorreoParaDefault ?? emailcc,
                        CopiaOculta = _configuracion.CorreoParaDefault ?? emailbcc,
                        Asunto = _configuracion.SujetoDefault ?? ConstantesServicio.AvisoNotificacionEmailOtros,
                        Mensaje = lsMensaje,
                        ArchivosAdjuntos = null
                    });
                }

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarNotificacionesEmailOtrosAsync|Se presenta exception: '{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using static MUFG_T24.Modelos.CalypsoMwSolicitud;


namespace AplicacionEmail.Servicios
{
    public class ENT_NOTIFICACION : IENT_NOTIFICACION
    {
        private Configuracion _configuracionBase;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_NOTIFICACION(Configuracion configuracion) {
            _configuracionBase = configuracion;
        }

        public async Task<bool> EnviarNotificacionesCUBSERTAX(string ptipo, string pMailCub, string pMailCC, ComplementoBenSer complementoBenSer)
        {
            try
            {
                if (complementoBenSer == null)
                    throw new ArgumentNullException(nameof(complementoBenSer));
                string lsNombreTransferencia;
                string lsHoraOperacion;
                string lsCuentaMascara;
                string lsCadenaMensajeHeader;

                //Establece el nombre por tipo de transferencia
                switch (ptipo)
                {
                    case ConstantesServicio.tipoSER:
                        lsNombreTransferencia = ConstantesServicio.nombreTranSER;
                        break;
                    case ConstantesServicio.tipoTAX:
                        lsNombreTransferencia = ConstantesServicio.nombreTranTAX;
                        break;
                    default:
                        lsNombreTransferencia = ConstantesServicio.nombreTranSER;
                        break;
                }

                //Establece la hora de operacion
                lsHoraOperacion = Convert.ToString(complementoBenSer.NF_FECHA_AUTORIZA);
                if (lsHoraOperacion.StartsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(1);
                }
                if (lsHoraOperacion.EndsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(0, lsHoraOperacion.Length - 1);
                }

                //Establece la mascara ordenante
                lsCuentaMascara = Convert.ToString(complementoBenSer.T24_ACCT_NO);
                if (lsCuentaMascara.Length > 5)
                {
                    lsCuentaMascara = new string('*', lsCuentaMascara.Length - 5) + lsCuentaMascara.PadRight(5).Substring(1, 4) + "*";
                }
                else
                {
                    lsCuentaMascara = "*" + lsCuentaMascara + "*";
                }


                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table           { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>NOTIFICACIONES MUFG Bank México, S.A. (NOTIFICATIONS MUFG Bank Mexico, S.A.)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Estimado Cliente (Dear Client):</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='header_tabla' width='30%' colspan='2'>Le notificamos que la siguiente operación ha sido registrada en la Banca Electrónica de MUFG:</td></tr>");
                sb.AppendLine("<tr><td class='header_tabla' width='30%' colspan='2'>We notify you that the next transaction has been registered in the Electronic Banking of MUFG:</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Operación (Transaction):</td><td class='dato_tabla' width='70%'>&nbsp;{lsNombreTransferencia}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha Operación (Transaction Date):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.FechaHtml(ConversionesComunes.ParseFechaRequerido(Convert.ToString(BaseDatosHelper.ConvierteAFecha(complementoBenSer.NF_FECHA_EJECUCION)), nameof(complementoBenSer.NF_FECHA_EJECUCION)))}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Hora Operación (Transaction Time):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsHoraOperacion)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Ordenante (Sender):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenSer.T24_SHORT_NAME)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Cuenta Origen (Origin Account):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsCuentaMascara)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Beneficiario (Beneficiary):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenSer.T24_BEN_CUST_NAME)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Número de Convenio (Agreement Number):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenSer.T24_NO_AGREEMENT)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Referencia (Reference):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenSer.T24_REFERENCE)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Concepto (Concept):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenSer.T24_CONCEPT)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Importe (Amount):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenSer.T24_AMOUNT)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Moneda (Currency):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenSer.T24_CURRENCY)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Clave de Rastreo (Code):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenSer.T24_TRANSACTION_ID)}</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("<span class='Nota'><p><b>NOTA:</b> Este correo electrónico es para notificarle la realización de la operación descrita en el cuadro anterior...</p></span>");
                sb.AppendLine("<span class='Nota'><p>Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada...</p></span>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("<span class='Nota'><p><b>NOTE:</b> The purpose of this e-mail is to notify you the transaction made and described in the chart above...</p></span>");
                sb.AppendLine("<span class='Nota'><p>This electronic mail message, and any of the accompanying documents, may contain confidential or privileged information...</p></span>");
                sb.AppendLine("</html>");

                lsCadenaMensajeHeader = sb.ToString();


                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var asuntoBase = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault)
                                ? ConstantesServicio.AvisoNotificacion
                                : _configuracionBase.SujetoDefault;

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para         = _configuracionBase.CorreoParaDefault ?? pMailCub,
                    ConCopia     = _configuracionBase.CorreoParaDefault ?? pMailCC,
                    CopiaOculta  = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto       = asuntoBase,
                    Mensaje      = lsCadenaMensajeHeader,
                    ArchivosAdjuntos = null,
                });

                if (resultado is true)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoErroSERTAX : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeErrorSERTAX,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Envia_Notificacion_CUBSERTAX_: '{ptipo + "|Ocurre exception: " + ex.ToString()}'");
                return true;
            }
        }

        public async Task<bool> EnviarNotificacionesCUBFXD(string strTipoT, string strMail, string strMailCC, ComplementoCUBFXDDTO complementoCUBFXD)
        {
            try
            {
                string lsCadenaMensaje;
                string lsNombreTransferencia;
                string lsHoraOperacion = null;
                string lsCuentaMascara;
                string lsReferenciaNumerica = null;
                string lsClasifOperacion = null;
                string lsFechaCargo = null;
                string lsFechaAplicacion = null;
                string lsCuentaMascaraBen;
                string lsRelComercial = null;

                
                //Asignacion de nombre de notificaciones por tipo de beneficiario
                if (complementoCUBFXD == null)
                    throw new ArgumentNullException(nameof(complementoCUBFXD));

                switch (strTipoT)
                {
                    case ConstantesServicio.tipoFXD:
                        lsNombreTransferencia = ConstantesServicio.nombreNotifFXD;
                        break;
                    case ConstantesServicio.tipoPAPFXD:
                        lsNombreTransferencia = ConstantesServicio.nombreNotifFXD;
                        break;
                    case ConstantesServicio.tipoFXT:
                        lsNombreTransferencia = ConstantesServicio.nombreNotiFXT;
                        break;
                    case ConstantesServicio.tipoPAPFXT:
                        lsNombreTransferencia = ConstantesServicio.nombreNotiFXT;
                        break;
                    case ConstantesServicio.tipoSPID:
                        lsNombreTransferencia = ConstantesServicio.nombreNotiSPID;
                        lsReferenciaNumerica = complementoCUBFXD.ToString();
                        lsClasifOperacion = complementoCUBFXD.NF_CLASIFICACION_OPERACION;
                        break;
                    case ConstantesServicio.tipoTEF:
                        lsNombreTransferencia = ConstantesServicio.nombreNotiTEF;
                        lsFechaCargo = Convert.ToString(complementoCUBFXD.NF_FECHA_CARGO);
                        lsFechaAplicacion = Convert.ToString(complementoCUBFXD.NF_FECHA_APLICACION);
                        break;
                    default:
                        lsNombreTransferencia = ConstantesServicio.nombreNotifFXD;
                        lsReferenciaNumerica = null;
                        lsClasifOperacion = null;
                        lsRelComercial = null;
                        break;
                };

                //Asigna hora de la operacion
                lsHoraOperacion = Convert.ToString(complementoCUBFXD.NF_FECHA_AUTORIZA);
                if (lsHoraOperacion.StartsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(1);
                }

                if (lsHoraOperacion.EndsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(0, lsHoraOperacion.Length - 1);
                }


                //Obtiene Mascara ordenante
                lsCuentaMascara = Convert.ToString(complementoCUBFXD.T24_BEN_ACCT_NO);
                if (lsCuentaMascara.Length > 5)
                {
                    lsCuentaMascara = new string('*', lsCuentaMascara.Length - 5) + lsCuentaMascara.PadRight(5).Substring(1, 4) + "*";
                }
                else
                {
                    lsCuentaMascara = "*" + lsCuentaMascara + "*";
                }

                //Obtiene Mascara beneficiarios
                lsCuentaMascaraBen = Convert.ToString(complementoCUBFXD.T24_DEBIT_ACCT_NO);
                if (lsCuentaMascaraBen.Length > 5)
                {
                    lsCuentaMascaraBen = new string('*', lsCuentaMascaraBen.Length - 5) + lsCuentaMascaraBen.PadRight(5).Substring(1, 4) + "*";
                }
                else
                {
                    lsCuentaMascaraBen = "*" + lsCuentaMascaraBen + "*";
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>NOTIFICACIONES MUFG Bank México, S.A. (NOTIFICATIONS MUFG Bank Mexico, S.A.)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Estimado Cliente (Dear Client):</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='header_tabla' width='30%' colspan='2'>Le notificamos que la siguiente operación ha sido registrada en la Banca Electrónica de MUFG:</td></tr>");
                sb.AppendLine("<tr><td class='header_tabla' width='30%' colspan='2'>We notify you that the next transaction has been registered in the Electronic Banking of MUFG:</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Operación (Transaction):</td><td class='dato_tabla' width='70%'>&nbsp;{lsNombreTransferencia}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha Operación (Transaction Date):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.FechaFormatoFijoHtml(Convert.ToString(complementoCUBFXD.NF_FECHA_EJECUCION))}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Hora Operación (Transaction Time):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsHoraOperacion)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Ordenante (Sender):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoCUBFXD.T24_SHORT_NAME)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Cuenta Origen (Origin Account):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsCuentaMascara)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Beneficiario (Beneficiary):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoCUBFXD.T24_BEN_CUST_NAME)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Cuenta Beneficiario (Beneficiary Account):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsCuentaMascaraBen)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Banco Destino (Destination Bank):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoCUBFXD.NF_BANCO_BENEF)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Concepto (Concept):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoCUBFXD.T24_DEBIT_THEIR_REF)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Importe (Amount):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.ImporteHtml(complementoCUBFXD.T24_DEBIT_AMOUNT)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Moneda (Currency):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoCUBFXD.T24_DEBIT_CURRENCY)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Clave de Rastreo (Code):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoCUBFXD.T24_TRANSACTION_ID)}</td></tr>");

                if (strTipoT == ConstantesServicio.tipoSPID)
                {
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Referencia Numérica (Numeric Reference):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsReferenciaNumerica)}</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Clasificación de la Operación (Classification):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsClasifOperacion)}</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Relacion Comercial (Commercial Relationship):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsRelComercial)}</td></tr>");
                }

                if (strTipoT == ConstantesServicio.tipoTEF)
                {
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha de Cargo (Debit Date):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsFechaCargo)}</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha de Aplicacion (Credit Date):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsFechaAplicacion)}</td></tr>");
                }

                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>NOTA:</B> Este correo electrónico es para notificarle la realización de la operación descrita en el cuadro anterior. El contenido de este correo y su envío, se realiza en cumplimiento de las disposiciones normativas aplicables. Este aviso constituye únicamente una referencia de los términos en los que se realizó la operación y no es un comprobante de la misma. La fecha y hora de este mensaje puede variar respecto a la fecha y hora real de la realización de la operación debido, (entre otros) a la zona horaria configurada en su computadora o la configuración de sus servicios de correo.</p></span>");
                sb.AppendLine("<span class='Nota'><p>Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de dicha información queda estrictamente prohibida. Si usted no es el destinatario o no es una persona autorizada por éste para recibir la información contenida en él favor de notificar inmediatamente al remitente y destruir el mensaje absteniéndose de leer, reproducir, transmitir, almacenar, divulgar o usar en cualquier forma y por cualquier medio la información contenida en él. Los mensajes enviados a través de medios electrónicos pueden ser sujetos de retrasos o alteraciones no autorizadas. Ni MUFG Bank, Ltd. ni ninguna sus filiales será responsable por dichas alteraciones o retrasos.</p></span>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>NOTE:</B> The purpose of this e-mail is to notify you the transaction made and described in the chart above. The contents of this e-mail and sending are made pursuant the applicable legal provisions. This notice is only a reference to the terms in which the transaction was made and it does not constitute a valid receipt of such transaction. The date and time of this message may be different from the real date and time in which the transaction was made due to (among others) the time zone configured in your computer or e-mail services configuration.</p></span>");
                sb.AppendLine("<span class='Nota'><p>This electronic mail message, and any of the accompanying documents, may contain confidential or privileged information. Any unauthorized disclosure or distribution of such information is strictly prohibited. If you are not the intended recipient of this message or if you are not a person authorized by the recipient thereof, please notify the sender immediately and destroy the message abstaining yourself from reading, reproducing, transmitting, storing, disclosing, revealing or using the information in any form and by any means. Messages sent through electronic media may be subject to delays or unauthorized alterations. Neither The MUFG Bank, Ltd. nor any of its affiliates is responsible for any such delay or alteration.</p></span>");
                sb.AppendLine("</html>");

                lsCadenaMensaje = sb.ToString();



                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                //var origen = ObtenerValor
                var asunto = string.IsNullOrEmpty(_configuracionBase.SujetoDefault)
                      ? ConstantesServicio.AvisoNotificacion
                      : $"{ConstantesServicio.AvisoNotificacion}[{_configuracionBase.SujetoDefault}]";

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? strMail,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? strMailCC,
                    CopiaOculta = ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = asunto,
                    Mensaje = lsCadenaMensaje,
                    ArchivosAdjuntos = null
                });

                if (resultado is true)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = string.Empty,
                        CopiaOculta = string.Empty,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeErrorFXD,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaNotificaciones_CUB_'{strTipoT + "| Se presenta exception:" + ex.ToString()}'");
                return true;
            }
        }

        public async Task<bool> EnviarNotificacionesCUBFXI(string strTipoT, string strMail, string strMailCC, ComplementoBenPapFxiDTO complementoBenPapFxi)
        {
            try
            {
                string lsCadenaMensaje;
                string lsHoraOperacion = null;
                string lsCuentaMascara;
                string lsCuentaMascaraBen;

                if (complementoBenPapFxi == null)
                    throw new ArgumentNullException(nameof(complementoBenPapFxi));

                //Asigna hora de la operacion
                lsHoraOperacion = Convert.ToString(complementoBenPapFxi.NF_FECHA_AUTORIZA);
                string lsNombreTransferencia;
                if (lsHoraOperacion.StartsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(1);
                }

                if (lsHoraOperacion.EndsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(0, lsHoraOperacion.Length - 1);
                }


                //Obtiene Mascara ordenante
                lsCuentaMascara = Convert.ToString(complementoBenPapFxi.T24_DEBIT_ACCT_NO);
                if (lsCuentaMascara.Length > 5)
                {
                    lsCuentaMascara = new string('*', lsCuentaMascara.Length - 5) + lsCuentaMascara.PadRight(5).Substring(1, 4) + '*';
                }
                else
                {
                    lsCuentaMascara = "*" + lsCuentaMascara + "*";
                }

                //Obtiene Mascara beneficiarios
                lsCuentaMascaraBen = Convert.ToString(complementoBenPapFxi.NF_CUENTA_CORRESPONSAL);
                if (lsCuentaMascaraBen.Length > 5)
                {
                    lsCuentaMascaraBen = new string('*', lsCuentaMascaraBen.Length - 5) + lsCuentaMascaraBen.PadRight(5).Substring(1, 4) + '*';
                }
                else
                {
                    lsCuentaMascaraBen = "*" + lsCuentaMascaraBen + "*";
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulogeneral { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulotabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".headertabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".datotabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota          { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir    { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulogeneral'>NOTIFICACIONES MUFG Bank México (NOTIFICATIONS MUFG Bank Mexico)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulotabla'>Estimado Cliente (Dear Client):</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='headertabla' width='30%' colspan='2'>Le notificamos que la siguiente operación ha sido registrada en la Banca Electrónica de MUFG:</td></tr>");
                sb.AppendLine("<tr><td class='headertabla' width='30%' colspan='2'>We notify you that the next transaction has been registered in the Electronic Banking of MUFG:</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Operación (Transaction):</td><td class='datotabla' width='70%'>&nbsp;{ConstantesServicio.nombreTranPAPFXI}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Fecha Operación (Transaction Date):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.FechaFormatoFijoHtml(Convert.ToString(complementoBenPapFxi.NF_FECHA_EJECUCION))}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Hora Operación (Transaction Time):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsHoraOperacion)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Ordenante (Sender):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.T24_SHORT_NAME)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Cuenta Origen (Origin Account):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsCuentaMascara)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;País (Country):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.NF_COUNTRY)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Cargo (Charge):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.NF_CHARGE)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Beneficiario (Beneficiary):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.T24_BEN_CUST_NAME)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Cuenta Beneficiario (Beneficiary Account):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsCuentaMascaraBen)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Banco Destino (Destination Bank):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.NF_BANCO_BENEF)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Concepto (Concept):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.T24_DEBIT_THEIR_REF)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Remittance message:</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.NF_MENSAJE)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Importe (Amount):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.ImporteHtml(complementoBenPapFxi.T24_DEBIT_AMOUNT)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Moneda (Currency):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.T24_DEBIT_CURRENCY)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Clave de Rastreo (Code):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenPapFxi.T24_TRANSACTION_ID)}</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>NOTA:</B> Este correo electrónico es para notificarle la realización de la operación descrita en el cuadro anterior. El contenido de este correo y su envío, se realiza en cumplimiento de las disposiciones normativas aplicables. Este aviso constituye únicamente una referencia de los términos en los que se realizó la operación y no es un comprobante de la misma. La fecha y hora de este mensaje puede variar respecto a la fecha y hora real de la realización de la operación debido, (entre otros) a la zona horaria configurada en su computadora o la configuración de sus servicios de correo.</p></span>");
                sb.AppendLine("<span class='Nota'><p>Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de dicha información queda estrictamente prohibida. Si usted no es el destinatario o no es una persona autorizada por éste para recibir la información contenida en él favor de notificar inmediatamente al remitente y destruir el mensaje absteniéndose de leer, reproducir, transmitir, almacenar, divulgar o usar en cualquier forma y por cualquier medio la información contenida en él. Los mensajes enviados a través de medios electrónicos pueden ser sujetos de retrasos o alteraciones no autorizadas. Ni The MUFG Bank, Ltd. ni ninguna sus filiales será responsable por dichas alteraciones o retrasos.</p></span>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>NOTE:</B> The purpose of this e-mail is to notify you the transaction made and described in the chart above. The contents of this e-mail and sending are made pursuant the applicable legal provisions. This notice is only a reference to the terms in which the transaction was made and it does not constitute a valid receipt of such transaction. The date and time of this message may be different from the real date and time in which the transaction was made due to (among others) the time zone configured in your computer or e-mail services configuration.</p></span>");
                sb.AppendLine("<span class='Nota'><p>This electronic mail message, and any of the accompanying documents, may contain confidential or privileged information. Any unauthorized disclosure or distribution of such information is strictly prohibited. If you are not the intended recipient of this message or if you are not a person authorized by the recipient thereof, please notify the sender immediately and destroy the message abstaining yourself from reading, reproducing, transmitting, storing, disclosing, revealing or using the information in any form and by any means. Messages sent through electronic media may be subject to delays or unauthorized alterations. Neither The MUFG Bank, Ltd. nor any of its affiliates is responsible for any such delay or alteration.</p></span>");
                sb.AppendLine("</html>");
                lsCadenaMensaje = sb.ToString();

                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

               

                //var origen = ObtenerValor
                var asunto = string.IsNullOrEmpty(_configuracionBase.SujetoDefault)
                      ? ConstantesServicio.AvisoNotificacion
                      : $"{ConstantesServicio.AvisoNotificacion}[{_configuracionBase.SujetoDefault}]";

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? strMail,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? strMailCC,
                    CopiaOculta = ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = asunto,
                    Mensaje = lsCadenaMensaje,
                    ArchivosAdjuntos = null
                });

                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeErrorFXD,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaNotificaciones_CUB_'{strTipoT + "| Se presenta exception:" + ex.ToString()}'");
                return true;
            }
        }

        public async Task<bool> EnviarNotificacionesCUBFXDNOM(string strTipoT, string strMail, string strMailCC, ComplementoBenNomFxdDTO complementoBenNomFxd)
        {
            try
            {
                string lsCadenaMensaje;
                string lsHoraOperacion = null;
                string lsCuentaMascara;
                string lsCuentaMascaraBen;

                if (complementoBenNomFxd == null)
                    throw new ArgumentNullException(nameof(complementoBenNomFxd));

                //Asigna hora de la operacion
                lsHoraOperacion = Convert.ToString(complementoBenNomFxd.NF_FECHA_AUTORIZA);
                string lsNombreTransferencia;
                if (lsHoraOperacion.StartsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(1);
                }

                if (lsHoraOperacion.EndsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(0, lsHoraOperacion.Length - 1);
                }


                //Obtiene Mascara ordenante
                lsCuentaMascara = Convert.ToString(complementoBenNomFxd.T24_CUSTOMER_CODE);
                if (lsCuentaMascara.Length > 5)
                {
                    lsCuentaMascara = new string('*', lsCuentaMascara.Length - 5) + lsCuentaMascara.PadRight(5).Substring(1, 4) + '*';

                }
                else
                {
                    lsCuentaMascara = "*" + lsCuentaMascara + "*";
                }

                //Obtiene Mascara beneficiarios
                lsCuentaMascaraBen = Convert.ToString(complementoBenNomFxd.NOPAGOS);
                if (lsCuentaMascaraBen.Length > 5)
                {
                    lsCuentaMascaraBen = new string('*', lsCuentaMascaraBen.Length - 5) + lsCuentaMascaraBen.PadRight(5).Substring(1, 4) + '*';
                }
                else
                {
                    lsCuentaMascaraBen = "*" + lsCuentaMascaraBen + "*";
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>NOTIFICACIONES MUFG Bank México, S.A. (NOTIFICATIONS MUFG Bank Mexico, S.A.)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Estimado Cliente (Dear Client):</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='header_tabla' width='30%' colspan='2'>Le notificamos que la siguiente operación ha sido registrada en la Banca Electrónica de MUFG:</td></tr>");
                sb.AppendLine("<tr><td class='header_tabla' width='30%' colspan='2'>We notify you that the next transaction has been registered in the Electronic Banking of MUFG:</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Operación (Transaction):</td><td class='dato_tabla' width='70%'>&nbsp;{ConstantesServicio.nombreTranFXDNOM}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha Operación (Transaction Date):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.FechaFormatoFijoHtml(Convert.ToString(complementoBenNomFxd.NF_FECHA_EJECUCION))}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Hora Operación (Transaction Time):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsHoraOperacion)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Ordenante (Sender):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenNomFxd.T24_SHORT_NAME)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Cuenta Origen (Origin Account):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsCuentaMascara)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Número de Pagos Solicitados (Total Requested Payment Number):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenNomFxd.NOPAGOS)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Pagos Procesados (Successful Payments):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenNomFxd.NOPAGOSPROCESADOS)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Pagos Devueltos (Rejected Payments):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenNomFxd.NOPAGOSDEVUELTOS)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Concepto (Concept):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenNomFxd.T24_DEBIT_THEIR_REF)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Importe (Amount):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.ImporteHtml(complementoBenNomFxd.T24_DEBIT_AMOUNT)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Moneda (Currency):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenNomFxd.T24_DEBIT_CURRENCY)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Clave de Rastreo (Code):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenNomFxd.T24_TRANSACTION_ID)}</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>NOTA:</B> Este correo electrónico es para notificarle la realización de la operación descrita en el cuadro anterior. El contenido de este correo y su envío, se realiza en cumplimiento de las disposiciones normativas aplicables. Este aviso constituye únicamente una referencia de los términos en los que se realizó la operación y no es un comprobante de la misma. La fecha y hora de este mensaje puede variar respecto a la fecha y hora real de la realización de la operación debido, (entre otros) a la zona horaria configurada en su computadora o la configuración de sus servicios de correo.</p></span>");
                sb.AppendLine("<span class='Nota'><p>Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de dicha información queda estrictamente prohibida. Si usted no es el destinatario o no es una persona autorizada por éste para recibir la información contenida en él favor de notificar inmediatamente al remitente y destruir el mensaje absteniéndose de leer, reproducir, transmitir, almacenar, divulgar o usar en cualquier forma y por cualquier medio la información contenida en él. Los mensajes enviados a través de medios electrónicos pueden ser sujetos de retrasos o alteraciones no autorizadas. Ni The MUFG Bank, Ltd. ni ninguna sus filiales será responsable por dichas alteraciones o retrasos.</p></span>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>NOTE:</B> The purpose of this e-mail is to notify you the transaction made and described in the chart above. The contents of this e-mail and sending are made pursuant the applicable legal provisions. This notice is only a reference to the terms in which the transaction was made and it does not constitute a valid receipt of such transaction. The date and time of this message may be different from the real date and time in which the transaction was made due to (among others) the time zone configured in your computer or e-mail services configuration.</p></span>");
                sb.AppendLine("<span class='Nota'><p>This electronic mail message, and any of the accompanying documents, may contain confidential or privileged information. Any unauthorized disclosure or distribution of such information is strictly prohibited. If you are not the intended recipient of this message or if you are not a person authorized by the recipient thereof, please notify the sender immediately and destroy the message abstaining yourself from reading, reproducing, transmitting, storing, disclosing, revealing or using the information in any form and by any means. Messages sent through electronic media may be subject to delays or unauthorized alterations. Neither The MUFG Bank, Ltd. nor any of its affiliates is responsible for any such delay or alteration.</p></span>");
                sb.AppendLine("</html>");

                lsCadenaMensaje = sb.ToString();

                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var asuntoBase = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault)
                                ? ConstantesServicio.AvisoNotificacion
                                : _configuracionBase.SujetoDefault;

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? strMail,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? strMailCC,
                    CopiaOculta = ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = asuntoBase,
                    Mensaje = lsCadenaMensaje,
                    ArchivosAdjuntos = null
                });


                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeErrorFXD,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaNotificaciones_CUB_'{strTipoT + "| Se presenta exception:" + ex.ToString()}'");
                return true;
            }

        }

        public async Task<bool> EnviarNotificacionesCUBDRE(string strTipoT, string strMail, string strMailCC, ComplementoBenDreDTO complementoBenDre)
        {
            try
            {
                string lsCadenaMensaje;
                string lsNombreTransferencia;
                string lsHoraOperacion = null;
                string lsCuentaMascara;
                string lsCuentaMascaraBen;

               

                if (complementoBenDre == null)
                    throw new ArgumentNullException(nameof(complementoBenDre));
                switch (strTipoT)
                {
                    case ConstantesServicio.tipoDRE:
                        lsNombreTransferencia = ConstantesServicio.nombreTranDRE;
                        break;
                    case ConstantesServicio.tipoDPA:
                        lsNombreTransferencia = ConstantesServicio.nombreTranDPA;
                        break;
                    default:
                        lsNombreTransferencia = ConstantesServicio.nombreNotifFXD;
                        break;
                }
                ;

                //Asigna hora de la operacion
                lsHoraOperacion = Convert.ToString(complementoBenDre.NF_FECHA_PROCESA);
                if (lsHoraOperacion.StartsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(1);
                }

                if (lsHoraOperacion.EndsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(0, lsHoraOperacion.Length - 1);
                }


                //Obtiene Mascara ordenante
                lsCuentaMascara = Convert.ToString(complementoBenDre.T24_DEBIT_ACCT_NO);
                if (lsCuentaMascara.Length > 5)
                {
                    lsCuentaMascara = new string('*', lsCuentaMascara.Length - 5) + lsCuentaMascara.PadRight(5).Substring(1, 4) + "*";
                }
                else
                {
                    lsCuentaMascara = "*" + lsCuentaMascara + "*";
                }

                //Obtiene Mascara beneficiarios
                lsCuentaMascaraBen = Convert.ToString(complementoBenDre.NF_TIPO);
                if (lsCuentaMascaraBen.Length > 5)
                {
                    lsCuentaMascaraBen = new string('*', lsCuentaMascaraBen.Length - 5) + lsCuentaMascaraBen.PadRight(5).Substring(1, 4) + "*";
                }
                else
                {
                    lsCuentaMascaraBen = "*" + lsCuentaMascaraBen + "*";
                }
                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulogeneral { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulotabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".headertabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".datotabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota          { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir    { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulogeneral'>NOTIFICACIONES MUFG Bank México, S.A. (NOTIFICATIONS MUFG Bank Mexico, S.A.)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulotabla'>Estimado Cliente (Dear Client):</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='headertabla' width='30%' colspan='2'>Le notificamos que la siguiente operación ha sido registrada en la Banca Electrónica de MUFG:</td></tr>");
                sb.AppendLine("<tr><td class='headertabla' width='30%' colspan='2'>We notify you that the next transaction has been registered in the Electronic Banking of MUFG:</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Operación (Transaction):</td><td class='datotabla' width='70%'>&nbsp;{lsNombreTransferencia}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Fecha Operación (Transaction Date):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.FechaFormatoFijoHtml(Convert.ToString(complementoBenDre.NF_FECHA_EJECUCION))}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Ordenante (Sender):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenDre.T24_COMPANY_NAME)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Cuenta Origen (Origin Account):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsCuentaMascara)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Concepto (Concept):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenDre.NF_DESCRIPCION)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Importe (Amount):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.ImporteHtml(complementoBenDre.T24_DEBIT_AMOUNT)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Moneda (Currency):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(ConstantesServicio.moneda)}</td></tr>");
                sb.AppendLine($"<tr><td class='headertabla' width='30%'>&nbsp;Clave de Rastreo (Code):</td><td class='datotabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenDre.T24_TRANSACTION_ID)}</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>NOTA:</B> Este correo electrónico es para notificarle la realización de la operación descrita en el cuadro anterior. El contenido de este correo y su envío, se realiza en cumplimiento de las disposiciones normativas aplicables. Este aviso constituye únicamente una referencia de los términos en los que se realizó la operación y no es un comprobante de la misma. La fecha y hora de este mensaje puede variar respecto a la fecha y hora real de la realización de la operación debido, (entre otros) a la zona horaria configurada en su computadora o la configuración de sus servicios de correo.</p></span>");
                sb.AppendLine("<span class='Nota'><p>Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de dicha información queda estrictamente prohibida. Si usted no es el destinatario o no es una persona autorizada por éste para recibir la información contenida en él favor de notificar inmediatamente al remitente y destruir el mensaje absteniéndose de leer, reproducir, transmitir, almacenar, divulgar o usar en cualquier forma y por cualquier medio la información contenida en él. Los mensajes enviados a través de medios electrónicos pueden ser sujetos de retrasos o alteraciones no autorizadas. Ni The MUFG Bank, Ltd. ni ninguna sus filiales será responsable por dichas alteraciones o retrasos.</p></span>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>NOTE:</B> The purpose of this e-mail is to notify you the transaction made and described in the chart above. The contents of this e-mail and sending are made pursuant the applicable legal provisions. This notice is only a reference to the terms in which the transaction was made and it does not constitute a valid receipt of such transaction. The date and time of this message may be different from the real date and time in which the transaction was made due to (among others) the time zone configured in your computer or e-mail services configuration.</p></span>");
                sb.AppendLine("<span class='Nota'><p>This electronic mail message, and any of the accompanying documents, may contain confidential or privileged information. Any unauthorized disclosure or distribution of such information is strictly prohibited. If you are not the intended recipient of this message or if you are not a person authorized by the recipient thereof, please notify the sender immediately and destroy the message abstaining yourself from reading, reproducing, transmitting, storing, disclosing, revealing or using the information in any form and by any means. Messages sent through electronic media may be subject to delays or unauthorized alterations. Neither The MUFG Bank, Ltd. nor any of its affiliates is responsible for any such delay or alteration.</p></span>");
                sb.AppendLine("</html>");

                lsCadenaMensaje = sb.ToString();

                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                //var origen = ObtenerValor
                var asunto = string.IsNullOrEmpty(_configuracionBase.SujetoDefault)
                      ? ConstantesServicio.AvisoNotificacion
                      : $"{ConstantesServicio.AvisoNotificacion}[{_configuracionBase.SujetoDefault}]";

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? strMail,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? strMailCC,
                    CopiaOculta = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = asunto,
                    Mensaje = lsCadenaMensaje,
                    ArchivosAdjuntos = null
                });

                

                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeErrorFXD,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaNotificaciones_CUB_'{strTipoT + "| Se presenta exception:" + ex.ToString()}'");
                return false;
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_AVISO : IENT_AVISO
    {
        Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_AVISO(Configuracion configuracion) {
            _configuracion = configuracion;
        }

        public async Task<bool> EnviaAvisos(string strTipoT, string strMail, string strMailCC, ComplementoAvisosDTO complementoAvisos)
        {
            try
            {
                if (complementoAvisos == null)
                    throw new ArgumentNullException(nameof(complementoAvisos));
                string lsCadenaMensaje;
                string lsNombreTransferencia;
                string lsHoraOperacion;
                string lsCuentaMascara;

                bool lbInternacional = false;
                bool lbPagoSER_TAX = false;

                try
                {
                    switch (strTipoT)
                    {

                        case ConstantesServicio.tipoFXD:
                            lsNombreTransferencia = ConstantesServicio.nombreTranFXD;
                            break;
                        case ConstantesServicio.tipoFXT:
                            lsNombreTransferencia = ConstantesServicio.nombreTranFXT;
                            break;
                        case ConstantesServicio.tipoFXI:
                            lsNombreTransferencia = ConstantesServicio.nombreTranFXI;
                            lbInternacional = true;
                            break;
                        case ConstantesServicio.tipoSPID:
                            lsNombreTransferencia = ConstantesServicio.nombreTranFXI;
                            break;
                        case ConstantesServicio.tipoTEF:
                            lsNombreTransferencia = ConstantesServicio.nombreTranTEF;
                            break;
                        case ConstantesServicio.tipoSER:
                            lsNombreTransferencia = ConstantesServicio.nombreTranSER;
                            lbPagoSER_TAX = true;
                            break;
                        case ConstantesServicio.tipoTAX:
                            lsNombreTransferencia = ConstantesServicio.nombreTranTAX;
                            lbPagoSER_TAX = true;
                            break;
                        default:
                            lsNombreTransferencia = ConstantesServicio.nombreTranSER;
                            lbPagoSER_TAX = false;
                            break;
                    }
                    ;
                }
                catch (Exception)
                {
                    lbPagoSER_TAX = false;
                    lsNombreTransferencia = null;
                }

                //Hora de operacion
                lsHoraOperacion = Convert.ToString(complementoAvisos.NF_FECHA_AUTORIZA);
                if (lsHoraOperacion.StartsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(1);
                }

                if (lsHoraOperacion.EndsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(0, lsHoraOperacion.Length - 1);
                }

                //Obtiene Mascara
                lsCuentaMascara = Convert.ToString(complementoAvisos.T24_SHORT_NAME);
                if (lsCuentaMascara.Length > 5)
                {
                    lsCuentaMascara = new string('*', lsCuentaMascara.Length - 5) + lsCuentaMascara.PadRight(5).Substring(1, 4) + "*";
                }
                else
                {
                    lsCuentaMascara = "*" + lsCuentaMascara + "*";
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");

                if (lbPagoSER_TAX)
                {
                    sb.AppendLine($"<center><span class='titulo_general'>{lsNombreTransferencia}</span>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<span class='titulo_tabla'>Datos de la Transacción:</span>");
                    sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%' colspan='2'>Esta notificación es enviada a petición de: {HtmlHelper.TextoHtml(complementoAvisos.T24_SHORT_NAME)}, quien solicitó un Pago a su referencia.</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%' colspan='2'>This notification is sent at the request of: {HtmlHelper.TextoHtml(complementoAvisos.T24_SHORT_NAME)}, who requested a Payment to your reference.</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Beneficiario (Beneficiary):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoAvisos.T24_BEN_CUST_NAME)}</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Número de Convenio (Agreement Number):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoAvisos.T24_NO_AGREEMENT)}</td></tr>");
                }
                else
                {
                    sb.AppendLine($"<center><span class='titulo_general'>TRANSFERENCIA DE FONDOS {lsNombreTransferencia}</span>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<span class='titulo_tabla'>Datos de la Transacción:</span>");
                    sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%' colspan='2'>Esta notificación es enviada a petición de: {HtmlHelper.TextoHtml(complementoAvisos.T24_SHORT_NAME)}, quien solicitó una Transferencia Interbancaria a su cuenta.</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%' colspan='2'>This notification is sent at the request of: {HtmlHelper.TextoHtml(complementoAvisos.T24_SHORT_NAME)}, who requested a transfer Interbank to your account.</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Beneficiario (Beneficiary):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoAvisos.T24_BEN_CUST_NAME)}</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Banco (Bank):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(Convert.ToString(complementoAvisos.NF_BANCO_BENEF))}</td></tr>");
                }
                //Pago Internacional
                if (lbInternacional)
                {
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;País (Country):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoAvisos.NF_COUNTRY)}</td></tr>");
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Cargo (Charge):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoAvisos.NF_CHARGE)}</td></tr>");
                }
                else
                {
                    sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Clave de Rastreo (Code):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoAvisos.T24_TRANSACTION_ID)}</td></tr>");
                }

                var moneda = (strTipoT == ConstantesServicio.tipoSER || strTipoT == ConstantesServicio.tipoTAX)
                            ? complementoAvisos.T24_CURRENCY
                            : complementoAvisos.T24_DEBIT_CURRENCY;

                var importe = (strTipoT == ConstantesServicio.tipoSER || strTipoT == ConstantesServicio.tipoTAX)
                            ? complementoAvisos.T24_AMOUNT
                            : complementoAvisos.T24_DEBIT_AMOUNT;

                var concepto = (strTipoT == ConstantesServicio.tipoSER || strTipoT == ConstantesServicio.tipoTAX)
                            ? complementoAvisos.T24_CONCEPT
                            : complementoAvisos.T24_DEBIT_THEIR_REF;

                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Moneda (Currency):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(moneda)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Importe (Amount):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.ImporteHtml(importe)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Concepto (Concept):</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(concepto)}</td></tr>");

                if (lsNombreTransferencia == ConstantesServicio.nombreTranSPID)
                {
                    sb.AppendLine(
                        $"<tr><td class='header_tabla' width='30%'>&nbsp;Referencia Numérica (Numeric Reference):</td>" +
                        $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(Convert.ToString(complementoAvisos.NF_NUMERIC_REFERENCE))}</td></tr>");
                }

                if (lsNombreTransferencia == ConstantesServicio.tipoTEF)
                {
                    sb.AppendLine(
                        $"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha de Cargo (Debit Date):</td>" +
                        $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(Convert.ToString(complementoAvisos.NF_FECHA_CARGO))}</td></tr>");

                    sb.AppendLine(
                        $"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha de Aplicación (Credit Date):</td>" +
                        $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(Convert.ToString(complementoAvisos.NF_FECHA_APLICACION))}</td></tr>");
                }

                if (lbInternacional)
                {
                    sb.AppendLine(
                        $"<tr><td class='header_tabla' width='30%'>&nbsp;Remittance message:</td>" +
                        $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoAvisos.NF_MENSAJE)}</td></tr>");
                }

                if (lbPagoSER_TAX)
                {
                    sb.AppendLine(
                        $"<tr><td class='header_tabla' width='30%'>&nbsp;Referencia (Reference):</td>" +
                        $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoAvisos.T24_REFERENCE)}</td></tr>");
                }
                else
                {
                    sb.AppendLine(
                        $"<tr><td class='header_tabla' width='30%'>&nbsp;Cuenta de Abono (Beneficiary Account):</td>" +
                        $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(lsCuentaMascara)}</td></tr>");
                }

                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");
                sb.AppendLine($"<BR><BR><span class='Nota'><p><B>NOTA:</B> El contenido del mensaje es responsabilidad exclusiva de: {HtmlHelper.TextoHtml(complementoAvisos.T24_SHORT_NAME)} y se lleva a cabo a solicitud de ésta. MUFG Bank México,S.A. no se hace responsable del mismo. El objeto de este mensaje es informar de la transferencia electrónica de fondos realizada a la cuenta proporcionada por el cliente. Este aviso constituye únicamente una referencia de los términos en que se realizó la operación. La fecha y hora de este mensaje puede ser diferente a la fecha y hora real de operación de la transacción.</p></span>");
                sb.AppendLine($"<BR><BR><span class='Nota'><p><B>NOTE:</B> The contents of the message is the sole responsibility of: {HtmlHelper.TextoHtml(complementoAvisos.T24_SHORT_NAME)} and performed at its request. MUFG Bank Mexico, S.A. is not responsible for the same. The purpose of this message is to inform the electronic transfer of funds made to the account provided by the client. This notice is only a reference to the terms in which they conducted the operation. The date and time of this message may be different from the actual date and time of operation of the transaction.</p></span>");
                sb.AppendLine("<BR><span class='Nota'><p>La información contenida en este mensaje de datos es confidencial, constituye un secreto industrial en términos de la legislación vigente y se encuentra dirigida exclusivamente al destinatario indicado en dicho mensaje. Si usted recibe esta información por error o si usted no es el destinatario del mensaje, favor de destruirlo inmediatamente, absteniéndose de leerlo, reproducirlo, transmitirlo, almacenarlo, divulgarlo, revelarlo, o usarlo de manera directa o indirecta en cualquier forma y por cualquier medio. Muchas gracias.</p></span>");
                sb.AppendLine("<span class='Nota'><p>The information contained in this electronic message is confidential, privileged and intended for its recipient only. If you receive this mail by mistake and/or if you are not the recipient thereof, please destroy it immediately, abstaining yourself from reading, reproducing, transmitting, storing, disclosing, revealing or using it, either directly or indirectly, in any manner and by any means. Thank you very much.</p></span>");
                sb.AppendLine("</html>");

                lsCadenaMensaje = sb.ToString();



                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracion.CorreoDeDefault ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracion.CorreoParaDefault ?? strMail,
                    ConCopia = _configuracion.CorreoParaDefault ?? strMailCC,
                    CopiaOculta = _configuracion.CorreoParaDefault ?? ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = _configuracion.CorreoSenderTest ?? ConstantesServicio.AvisoNotificacion,
                    Mensaje = lsCadenaMensaje,
                    ArchivosAdjuntos = null
                });

                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoDeDefault ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracion.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = ConstantesServicio.AsuntoError,
                        Mensaje = ConstantesServicio.MensajeErrorSERTAX,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }


            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaAvisos|Se presenta exception: {ex.ToString()}");
                return false;
            }

        }
    }
}
    
using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_AVISOH2H : IENT_AVISOH2H
    {
        private Configuracion _configuracionBase;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_AVISOH2H(Configuracion configuracion) {
            _configuracionBase = configuracion;
        }   

        public async Task<bool> EnviaAvisosH2H(string strTipoT, string strMail, string strMailCC, ComplementoAvisoH2HDTO complementoAvisoH2H)
        {
            try
            {
                if (complementoAvisoH2H == null)
                    throw new ArgumentNullException(nameof(complementoAvisoH2H));
                string lsCadenaMensaje;
                string lsNombreTransferencia;
                string lsHoraOperacion;
                string lsCuentaMascara;

                bool lbInternacional = false;
                bool lbPagoSER_TAX = false;

                try
                {
                    switch (strTipoT)
                    {

                        case ConstantesServicio.tipoFXD:
                            lsNombreTransferencia = ConstantesServicio.nombreTranFXD;
                            break;
                        case ConstantesServicio.tipoFXT:
                            lsNombreTransferencia = ConstantesServicio.nombreTranFXT;
                            break;
                        case ConstantesServicio.tipoFXI:
                            lsNombreTransferencia = ConstantesServicio.nombreTranFXI;
                            lbInternacional = true;
                            break;
                        case ConstantesServicio.tipoSPID:
                            lsNombreTransferencia = ConstantesServicio.tipoSPID;
                            break;
                        case ConstantesServicio.tipoTEF:
                            lsNombreTransferencia = ConstantesServicio.nombreTranTEF;
                            break;
                        default:
                            lsNombreTransferencia = ConstantesServicio.nombreTranSER;
                            lbPagoSER_TAX = false;
                            break;
                    };
                }
                catch (Exception)
                {
                    lsNombreTransferencia = null;
                }

                //Hora de operacion
                lsHoraOperacion = Convert.ToString(complementoAvisoH2H.NF_FECHA_AUTORIZA);
                if (lsHoraOperacion.StartsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(1);
                }

                if (lsHoraOperacion.EndsWith(":"))
                {
                    lsHoraOperacion = lsHoraOperacion.Substring(0, lsHoraOperacion.Length - 1);
                }

                //Obtiene Mascara
                lsCuentaMascara = (strTipoT == ConstantesServicio.tipoFXI)
                                ? complementoAvisoH2H.NF_CUENTA_CORRESPONSAL
                                : complementoAvisoH2H.T24_BEN_ACCT_NO;
                lsCuentaMascara = Convert.ToString(lsCuentaMascara);
                if (lsCuentaMascara.Length > 5)
                {
                    lsCuentaMascara = new string('*', lsCuentaMascara.Length - 5) + lsCuentaMascara.PadRight(5).Substring(1, 4) + "*";
                }
                else
                {
                    lsCuentaMascara = "*" + lsCuentaMascara + "*";
                }

                lsCadenaMensaje = "<html>" + ConstantesServicio.NewLine +
                                  "<head>" + ConstantesServicio.NewLine +
                                  "<style type='text/css' > " + ConstantesServicio.NewLine +
                                  ".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }" + ConstantesServicio.NewLine +
                                  ".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }" + ConstantesServicio.NewLine +
                                  ".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }" + ConstantesServicio.NewLine +
                                  ".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }" + ConstantesServicio.NewLine +
                                  ".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }" + ConstantesServicio.NewLine +
                                  ".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }" + ConstantesServicio.NewLine +
                                  "table           { border: thin solid #E0E0E0 }" + ConstantesServicio.NewLine +
                                  "</style>" + ConstantesServicio.NewLine +
                                  "</head>" + ConstantesServicio.NewLine +
                                  "<body>" + ConstantesServicio.NewLine;

                lsCadenaMensaje += "<center><span class='titulo_general'>TRANSFERENCIA DE FONDOS " + lsNombreTransferencia + "</span>" + ConstantesServicio.NewLine +
                                   "<BR>" + ConstantesServicio.NewLine +
                                   "<BR>" + ConstantesServicio.NewLine +
                                   "<span class='titulo_tabla'>Datos de la Transacci&oacute;n:</span>" + ConstantesServicio.NewLine +
                                   "<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>" + ConstantesServicio.NewLine +
                                   "<tr><td class='header_tabla' width='30%' colspan='2'> Esta notificación es enviada a petición de: " + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_COMPANY_NAME) + ", quien solicitó una Transferencia Interbancaria a su cuenta.</td></tr>" + ConstantesServicio.NewLine +
                                   "<tr><td class='header_tabla' width='30%' colspan='2'> This notification is sent at the request of: " + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_COMPANY_NAME) + ", who requested a transfer Interbank to your account.</td></tr>" + ConstantesServicio.NewLine +
                                   "<tr><td class='header_tabla' width='30%'>&nbsp;Beneficiario (Beneficiary):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_BEN_CUST_NAME) + "</td></tr>" + ConstantesServicio.NewLine +
                                   "<tr><td class='header_tabla' width='30%'>&nbsp;Banco (Bank):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.NF_BANCO_BENEF) + "</td></tr>" + ConstantesServicio.NewLine;

                if (lbInternacional)
                {
                    //Pago Internacional
                    lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Pa&iacute;s (Country):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.NF_COUNTRY) + "</td></tr>" + ConstantesServicio.NewLine +
                                 "<tr><td class='header_tabla' width='30%'>&nbsp;Cargo (Charge):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.NF_CHARGE) + "</td></tr>" + ConstantesServicio.NewLine;
                }
                else
                {
                    //Domestica o Terceros
                    lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Clave de Rastreo (Code):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_TRANSACTION_ID) + "</td></tr>" + ConstantesServicio.NewLine;
                }
                lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Fecha Operación (Transfer Date):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.FechaFormatoFijoHtml(Convert.ToString(complementoAvisoH2H.NF_FECHA_EJECUCION)) + "</td></tr>" + ConstantesServicio.NewLine +
                                   "<tr><td class='header_tabla' width='30%'>&nbsp;Hora Operación (Transfer Time):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(lsHoraOperacion) + "</td></tr>" + ConstantesServicio.NewLine +
                                   "<tr><td class='header_tabla' width='30%'>&nbsp;Moneda (Currency):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_DEBIT_CURRENCY) + "</td></tr>" + ConstantesServicio.NewLine +
                                   "<tr><td class='header_tabla' width='30%'>&nbsp;Importe (Amount):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.ImporteHtml(complementoAvisoH2H.T24_DEBIT_AMOUNT) + "</td></tr>" + ConstantesServicio.NewLine +
                                   "<tr><td class='header_tabla' width='30%'>&nbsp;Concepto (Concept):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_DEBIT_THEIR_REF) + "</td></tr>" + ConstantesServicio.NewLine;

                if (lsNombreTransferencia == ConstantesServicio.tipoSPID)
                {
                    lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Referencia Numérica (Numeric Reference):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_DEBIT_ACCT_NO) + "</td></tr>" + ConstantesServicio.NewLine;
                }

                if (lsNombreTransferencia == ConstantesServicio.tipoTEF)
                {
                    lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Fecha de Cargo: (Debit Date):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.NF_FECHA_CARGO) + "</td></tr>" + ConstantesServicio.NewLine;
                    lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Fecha de Aplicación: (Credit Date):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.NF_FECHA_APLICACION) + "</td></tr>" + ConstantesServicio.NewLine;
                }

                //Remittance message
                if (lbInternacional) { 
                    lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Remittance message:</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.NF_MENSAJE) + "</td></tr>" + ConstantesServicio.NewLine;
                }

                if (lbPagoSER_TAX) {
                    lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Referencia (Reference):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(complementoAvisoH2H.NF_BANCO_BENEF) + "</td></tr>" + ConstantesServicio.NewLine;
                }
                else {
                    lsCadenaMensaje += "<tr><td class='header_tabla' width='30%'>&nbsp;Cuenta de Abono (Beneficiary Account):</td><td class='dato_tabla' width='70%'>&nbsp;" + HtmlHelper.TextoHtml(lsCuentaMascara) + "</td></tr>" + ConstantesServicio.NewLine;
                }

                lsCadenaMensaje += "</table>" + ConstantesServicio.NewLine +
                                   "<BR><BR>" + ConstantesServicio.NewLine +
                                   "</center>" + ConstantesServicio.NewLine +
                                   "<BR><BR><span class='Nota'><p><B>NOTA:</B> El contenido del mensaje es responsabilidad exclusiva de: " + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_COMPANY_NAME) + " y se lleva a cabo a solicitud de ésta. MUFG Bank México,S.A. no se hace responsable del mismo. El objeto de este mensaje es informar de la transferencia electrónica de fondos realizada a la cuenta proporcionada por el cliente. Este aviso constituye únicamente una referencia de los términos en que se realizó la operación. La fecha y hora de este mensaje puede ser diferente a la fecha y hora real de operación de la transacción.</p></span>" + ConstantesServicio.NewLine +
                                   "<BR><BR><span class='Nota'><p><B>NOTE:</B> The contents of the message is the sole responsibility of: " + HtmlHelper.TextoHtml(complementoAvisoH2H.T24_COMPANY_NAME) + " and performed at its request. MUFG Bank Mexico, S.A. is not responsible for the same. The purpose of this message is to inform the electronic transfer of funds made to the account provided by the client. This notice is only a reference to the terms in which they conducted the operation. The date and time of this message may be different from the actual date and time of operation of the transaction.</p></span>" + ConstantesServicio.NewLine +
                                   "<BR><span class='Nota'><p> La información contenida en este mensaje de datos es confidencial, constituye un secreto industrial en términos de la legislación vigente y se encuentra dirigida exclusivamente al destinatario indicado en dicho mensaje. Si usted recibe esta información por error o si usted no es el destinatario del mensaje, favor de destruirlo inmediatamente, absteniéndose de leerlo, reproducirlo, transmitirlo, almacenarlo, divulgarlo, revelarlo, o usarlo de manera directa o indirecta en cualquier forma y por cualquier medio. Muchas gracias.</p></span>" + ConstantesServicio.NewLine +
                                   "<span class='Nota'><p> The information contained in this electronic message is confidential, privileged and intended for its recipient only. If you receive this mail by mistake and/or if you are not the recipient thereof, please destroy it inmediately, abstaining yourself from reading, reproducing, transmitting, storing, disclosing, revealing or using it, either directly or indirectly, in any manner and by any means. Thank you very much.</p></span>" + ConstantesServicio.NewLine +
                                   "</html>";

                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? strMail,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? strMailCC,
                    CopiaOculta = ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = _configuracionBase.SujetoDefault ?? ConstantesServicio.AvisoNotificacion,
                    Mensaje = lsCadenaMensaje,
                    ArchivosAdjuntos = null
                });

                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeErrorSERTAX,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }


            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaAvisos|Se presenta exception: {ex.ToString()}");
                return false;
            }

        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_ENVFCM : IENT_ENVFCM
    {
        private Configuracion _configuracionBase;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_ENVFCM(Configuracion configuracion) {
            _configuracionBase = configuracion;
        }

        public async  Task<bool> EnviaAvisosFcm(string strTipoT, string strMail, string strMailCC, ComplementoOperaFcmDTO complementoOperaFcm)
        {
            try
            {
                if (complementoOperaFcm == null)
                    throw new ArgumentNullException(nameof(complementoOperaFcm));
                string lsCadenaMensaje = null;
                string lsBase = null;

                switch (strTipoT)
                {
                    case ConstantesServicio.tipoENT_SPEI:
                        lsBase = ConstantesServicio.baseEntSPEI;
                        break;
                    case ConstantesServicio.tipoENT_SPEI_2:
                        lsBase = ConstantesServicio.baseEntSPEI;
                        break;
                    case ConstantesServicio.tipoFXD:
                        lsBase = ConstantesServicio.baseSPEI;
                        break;
                    case ConstantesServicio.tipoFXD_Local:
                        lsBase = ConstantesServicio.baseSPEIL;
                        break;
                    case ConstantesServicio.tipoFXD_GPH:
                        lsBase = ConstantesServicio.baseSPEIG;
                        break;
                    case ConstantesServicio.tipoNOM_FXD:
                        lsBase = ConstantesServicio.baseNOMINA;
                        break;
                    case ConstantesServicio.tipoPaP_FXD:
                        lsBase = ConstantesServicio.basePAGOP;
                        break;
                    case ConstantesServicio.tipoENT_SPID:
                        lsBase = ConstantesServicio.baseEntSPID;
                        break;
                    case ConstantesServicio.tipoSPD:
                        lsBase = ConstantesServicio.nombreTranSPID;
                        break;
                    case ConstantesServicio.tipoSPID:
                        lsBase = ConstantesServicio.nombreTranSPID;
                        break;
                    case ConstantesServicio.tipoSPID_Local:
                        lsBase = ConstantesServicio.baseSPIDL;
                        break;
                    case ConstantesServicio.tipoSPID_GPH:
                        lsBase = ConstantesServicio.baseSPIDG;
                        break;
                    case ConstantesServicio.tipoPaP_SPID:
                        lsBase = ConstantesServicio.basePAGOPS;
                        break;
                    case ConstantesServicio.tipoENT_TEF:
                        lsBase = ConstantesServicio.baseENTTEF;
                        break;
                    case ConstantesServicio.tipoTEF:
                        lsBase = ConstantesServicio.baseTEF;
                        break;
                    case ConstantesServicio.tipoTEF_GPH:
                        lsBase = ConstantesServicio.baseTEGG;
                        break;
                    case ConstantesServicio.tipoPaP_TEF:
                        lsBase = ConstantesServicio.basePAGOT;
                        break;
                    case ConstantesServicio.tipoSER:
                        lsBase = ConstantesServicio.basePAGOS;
                        break;
                    case ConstantesServicio.tipoTAX:
                        lsBase = ConstantesServicio.basePAGOIM;
                        break;
                    default:
                        lsBase = null;
                        break;
                };

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>AVISO DE ALERTA FCM</span>");
                sb.AppendLine($"<BR><span class='titulo_general'>{lsBase}</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Datos de la Transacción:</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Folio:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperaFcm.NF_FOLIO)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha valor:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperaFcm.NF_FECHA_EJECUCION)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Nombre del Cliente:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperaFcm.T24_COMPANY_NAME)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Beneficiario:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperaFcm.T24_BEN_CUST_NAME)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Moneda:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperaFcm.T24_DEBIT_CURRENCY)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Monto:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperaFcm.T24_DEBIT_AMOUNT)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Concepto:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperaFcm.T24_DEBIT_THEIR_REF)}</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("<span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");
                sb.AppendLine("</center>");
                sb.AppendLine("</body>");
                sb.AppendLine("</html>");

                lsCadenaMensaje = sb.ToString();

                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var asuntoBase = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault)
                                ? ConstantesServicio.AsuntoAvisoEnvioFCM
                                : _configuracionBase.SujetoDefault;

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenEnvioFcm,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenFcm,
                    Para = _configuracionBase.CorreoParaDefault ?? strMail,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? strMailCC,
                    CopiaOculta = null,
                    Asunto = asuntoBase,
                    Mensaje = lsCadenaMensaje,
                    ArchivosAdjuntos = null
                });
                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeErrorSERTAX,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaAvisosFcm|Se presenta exception: {ex.ToString()}");
                return false;
            }
        }
    }
}
    
using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_ENVIOBENCMS : IENT_ENVIOBENCMS
    {
        private Configuracion _configuracionBase;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_ENVIOBENCMS(Configuracion configuracion) {
            _configuracionBase = configuracion;
        }

        public async Task<bool> EnviarNotificacionesBenCms(string mailTo, string mailCC, ComplementoBenCmsDTO complementoBenCms)
        {
            try
            {
                if (complementoBenCms == null)
                    throw new ArgumentNullException(nameof(complementoBenCms));
                string lsFecha = null;
                string lsBanco = null;
                string lsMensaje = null;

                lsFecha = complementoBenCms.FECHA_SOL.ToString().Substring(0, 5) + complementoBenCms.FECHA_SOL.ToString().Substring(5, 3);
                lsBanco = complementoBenCms.BANCO == null ? complementoBenCms.BANCOBENEFICIARIO : complementoBenCms.CLAVEBANCO + "-" + complementoBenCms.BANCO;

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>NOTIFICACIONES MUFG Bank México (NOTIFICATIONS MUFG Bank Mexico)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");

                if (string.IsNullOrWhiteSpace(mailTo))
                {
                    sb.AppendLine("<span class='titulo_tabla'>Registro de Beneficiarios</span>");
                    sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='800' borderColor='#E0E0E0'>");
                    sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Le notificamos que el cliente ha registrado un beneficiario para operar, sin embargo no tiene correos registrados para notificaciones.</td></tr>");
                    sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>We notify the customer has registered a beneficiary to operate, but has no registered mails for notifications.</td></tr>");
                }
                else
                {
                    sb.AppendLine("<span class='titulo_tabla'>Estimado Cliente (Dear Client):</span>");
                    sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='800' borderColor='#E0E0E0'>");
                    sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Le notificamos que la siguiente cuenta de beneficiario ha sido registrada en la Banca Electrónica de MUFG Bank México.</td></tr>");
                    sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>We notify you that the next beneficiary account has been registered in the Electronic Banking of MUFG Bank Mexico.</td></tr>");
                }

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Operación (Transaction):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(ConstantesServicio.nombreTranBen)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Tipo (Class):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.FORMATO)}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha de Autorización (Authorization Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFecha}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Hora de Autorización (Authorization Hour):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{complementoBenCms.HORA_AUT}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cliente (Customer):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.ORDENATE)}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Alias:</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.ALIAS)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Beneficiario (Beneficiary):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.NOMBRE)}</td>" +
                    $"</tr>");

                if (complementoBenCms.FORMATO == ConstantesServicio.tipoSER || complementoBenCms.FORMATO == ConstantesServicio.tipoTAX)
                {
                    sb.AppendLine(
                                    $"<tr>" +
                                    $"<td class='header_tabla' width='20%'>&nbsp;Número de Convenio (Agreement Number):</td>" +
                                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.NOCUENTA)}</td>" +
                                    $"<td class='header_tabla' width='20%'>&nbsp;Moneda (Currency):</td>" +
                                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.MONEDA)}</td>" +
                                    $"</tr>");
                }
                else
                {
                    sb.AppendLine(
                                $"<tr>" +
                                $"<td class='header_tabla' width='20%'>&nbsp;Número de Cuenta (Account Number):</td>" +
                                $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.NOCUENTA)}</td>" +
                                $"<td class='header_tabla' width='20%'>&nbsp;Moneda (Currency):</td>" +
                                $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.MONEDA)}</td>" +
                                $"</tr>");

                    sb.AppendLine(
                                    $"<tr>" +
                                    $"<td class='header_tabla' width='20%'>&nbsp;Banco Beneficiario (Beneficiary Bank):</td>" +
                                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(lsBanco)}</td>" +
                                    $"</tr>");
                }

                if (complementoBenCms.FORMATO == ConstantesServicio.tipoFXI) {
                    sb.AppendLine(
                            $"<tr>" +
                            $"<td class='header_tabla' width='20%'>&nbsp;País (Country):</td>" +
                            $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.COUNTRY)}</td>" +
                            $"<td class='header_tabla' width='20%'>&nbsp;ABA:</td>" +
                            $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.ABA)}</td>" +
                            $"</tr>");

                    sb.AppendLine(
                        $"<tr>" +
                        $"<td class='header_tabla' width='20%'>&nbsp;Swift Beneficiario (Beneficiary Swift):</td>" +
                        $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.SWIFTBENEFICIARIO)}</td>" +
                        $"<td class='header_tabla' width='20%'>&nbsp;Cuenta Beneficiario (Beneficiary Account):</td>" +
                        $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.CUENTABENEFICIARIO)}</td>" +
                        $"</tr>");

                    sb.AppendLine(
                        $"<tr>" +
                        $"<td class='header_tabla' width='20%'>&nbsp;Swift Corresponsal (Correspondent Swift):</td>" +
                        $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.SWIFTCORRESPONSAL)}</td>" +
                        $"<td class='header_tabla' width='20%'>&nbsp;País Beneficiario (Beneficiary Country):</td>" +
                        $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.COUNTRY_BEN)}</td>" +
                        $"</tr>");

                    sb.AppendLine(
                        $"<tr>" +
                        $"<td class='header_tabla' width='20%'>&nbsp;Banco Corresponsal (Correspondent Bank):</td>" +
                        $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoBenCms.BANCOCORRESPONSAL)}</td>" +
                        $"</tr>");
                }


                sb.AppendLine(
                        "<tr><td class='dato_tabla' width='100%' colspan='4' align='center'>" +
                        "&nbsp;*** El beneficiario estará disponible para operación después de 30 minutos de haber sido autorizado." +
                        "</td></tr>");

                sb.AppendLine("</table>");

                sb.AppendLine("<BR><BR>");

                sb.AppendLine("</center>");

                sb.AppendLine(
                    "<BR><BR><span class='Nota'><p><B>NOTA:</B> Este correo electrónico es para notificarle la realización del registro de beneficiario descrita en el cuadro anterior. El contenido de este correo y su envío, se realiza en cumplimiento de las disposiciones normativas aplicables. Este aviso constituye únicamente una referencia de los términos en los que se realizó el registro y no es un comprobante del mismo. La fecha y hora de este mensaje puede variar respecto a la fecha y hora real de la realización del registro debido, (entre otros) a la zona horaria configurada en su computadora o la configuración de sus servicios de correo.</p></span>");
                sb.AppendLine(
                    "<span class='Nota'><p>Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de dicha información queda estrictamente prohibida. Si usted no es el destinatario o no es una persona autorizada por éste para recibir la información contenida en él favor de notificar inmediatamente al remitente y destruir el mensaje absteniéndose de leer, reproducir, transmitir, almacenar, divulgar o usar en cualquier forma y por cualquier medio la información contenida en él. Los mensajes enviados a través de medios electrónicos pueden ser sujetos de retrasos o alteraciones no autorizadas. Ni The MUFG Bank, Ltd. ni ninguna de sus filiales será responsable por dichas alteraciones o retrasos.</p></span>");
                sb.AppendLine(
                    "<BR><BR><span class='Nota'><p><B>NOTE:</B> The purpose of this e-mail is to notify you the beneficiary registration made and described in the chart above. The contents of this e-mail and sending are made pursuant the applicable legal provisions. This notice is only a reference to the terms in which the beneficiary registration was made and it does not constitute a valid receipt of such transaction. The date and time of this message may be different from the real date and time in which the beneficiary registration was made due to (among others) the time zone configured in your computer or e-mail services configuration.</p></span>");
                sb.AppendLine(
                    "<span class='Nota'><p>This electronic mail message, and any of the accompanying documents, may contain confidential or privileged information. Any unauthorized disclosure or distribution of such information is strictly prohibited. If you are not the intended recipient of this message or if you are not a person authorized by the recipient thereof, please notify the sender immediately and destroy the message abstaining yourself from reading, reproducing, transmitting, storing, disclosing, revealing or using the information in any form and by any means. Messages sent through electronic media may be subject to delays or unauthorized alterations. Neither The MUFG Bank, Ltd. nor any of its affiliates is responsible for any such delay or alteration.</p></span>");

                sb.AppendLine("</body>");
                sb.AppendLine("</html>");

                lsMensaje = sb.ToString();

                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);


                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? mailTo,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? mailCC,
                    CopiaOculta = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = ConstantesServicio.AvisoNotificacion,
                    Mensaje = lsMensaje,
                    ArchivosAdjuntos = null
                });
                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoAvisoEnvioBENCMS : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeAvisoEnvioBenCMS,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception)
            {
                return false;   
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Security;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static MUFG_T24.Modelos.CalypsoMwRespuesta;

namespace AplicacionEmail.Servicios
{
    public class ENT_SOL : ProcesamientoSol, IProcesamientoSol, IENT_SOL
    {
        private Configuracion _configuracionBase;
        Configuracion _configuracion;
        IDetallesTomser     _detallesSol;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_SOL(Configuracion configuracion, IDetallesTomser detallesSol)
            : base(configuracion, detallesSol)
        {
            _configuracionBase = configuracion;
            _configuracion   = configuracion;
            _detallesSol     = detallesSol;
        }

        public async Task ProcesarOperacionSolicitudsync()
        {
            try 
            { 

                Config.EscribirLog("INFO|procesaOperacionSolicitud|Buscando información de las solicitudes");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoSolDTO> complementoSol = await this.ObtencionDatosAsync();

                //Si no pudo obtener sus complementos...
                if (complementoSol.Count is 0)
                {
                    Config.EscribirLog($"ERROR|No se encontro el detalle de la operación");
                }

                //Obtiene las solicitudes y realiza el envio de correo...
                foreach (ComplementoSolDTO complementoSolDTO in complementoSol)
                {
                    Config.EscribirLog($"INFO|Procesa_Solicitudes_CMS|Usuarios|Procesando Solicitud de Usuarios, folio: '{complementoSolDTO.id_solicitud}'");
                    bool resultadoEnvio = await EnviarSolicitudes(complementoSolDTO);
                    Config.EscribirLog($"INFO|procesaOperacion_SOL|Se ejecutó procedimiento para realizar el envio de las solicitudes: '{complementoSolDTO.id_solicitud_documento}', '{complementoSolDTO.satm04ama}', '{complementoSolDTO.satm04cor}', '{complementoSolDTO.descripcion}'");
                    //Si por lo menos hay un detalle que no puede insertar, detiene el proceso...
                    if (resultadoEnvio is false)
                    {
                        Config.EscribirLog($"ERROR|Procesa_Solicitudes_CMS|Error en procesamiento de folio: '{complementoSolDTO.id_solicitud}'");
                    }
                    else
                    {
                        await _detallesSol.ActualizarEstatusEmailSol(complementoSolDTO.id_solicitud);
                        Config.EscribirLog($"INFO|Procesa_Solicitudes_CMS|Se termina procesamiento de folio: '{complementoSolDTO.id_solicitud}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionSolicitudsync|Se presenta exception:'{ex.ToString()}'");
            }
        }

        public async Task<bool> EnviarSolicitudes(ComplementoSolDTO complementoSolDTO)
        {
            if (complementoSolDTO == null)
                throw new ArgumentNullException(nameof(complementoSolDTO));

            string codigoActivacion = complementoSolDTO.satm04clave_activacion?.Trim();
            if (string.IsNullOrWhiteSpace(codigoActivacion))
                throw new InvalidOperationException("El código de activación es obligatorio para enviar la solicitud.");

            string cadenaMensaje       = null;
            bool   resultadoEnvioError = false;

            var sb = new StringBuilder();
            
            sb.AppendLine("Cash Management Services");
            sb.AppendLine("Asignación de Token / Token Assignation / TOKENの手続き完了のお知らせ");
            sb.AppendLine();
            sb.AppendLine($"Estimado(a) / Dear: {complementoSolDTO.satm04cor?.Trim()} {complementoSolDTO.satm04ama?.Trim()}");
            sb.AppendLine();
            sb.AppendLine();
            sb.AppendLine("MUFG Bank México, S.A. le ha asignado un nuevo Dispositivo de Seguridad denominado Token.");
            sb.AppendLine("MUFG Bank México, S.A. has assigned to you a security device called Token.");
            sb.AppendLine("いつもMUFGバンク（メキシコ）をご愛顧賜り、誠にありがとうございます。TOKENの手続きが完了致しましたので お知らせいたします。");
            sb.AppendLine();
            sb.AppendLine("Para activar y comenzar a utilizer el Token, será necesario seguir los siguientes pasos:");
            sb.AppendLine("To activate and start using your token, it is necessary to follow the steps below:");
            sb.AppendLine("TOKENをお使いいただく前に、次の手順でアクティベート（初期設定）手続きを行ってください。");
            sb.AppendLine();
            sb.AppendLine($"1.{ConstantesServicio.Tab}Recibir su usuario y Token asignado (*) los cuales se lo haremos llegar por mensajería.");
            sb.AppendLine($"{ConstantesServicio.Tab}Receive the device Token (*) and letter in which is written your user ID by mail service.");
            sb.AppendLine($"{ConstantesServicio.Tab}郵送にてTOKEN(*)とユーザーIDを記載したレターをお送りしておりますので、お受取ください。");
            sb.AppendLine();
            sb.AppendLine("(*) En caso de traspaso de Token, no enviamos un nuevo Token.");
            sb.AppendLine("In the case of transferring a Token to a new user, we will not send a new device.");
            sb.AppendLine("譲渡手続きの場合は、TOKEN本体はお送りいたしません。");
            sb.AppendLine();
            sb.AppendLine($"2.{ConstantesServicio.Tab}Ponerse en contacto con el CMS Technical Help Desk. / Call to CMS Technical Help Desk / CMS テクニカルヘルプデスクへお電話ください。");
            sb.AppendLine($"{ConstantesServicio.Tab}Tel: +52 (55) 1102-8304 Lunes a Viernes de 8:30 a 18:00 horas tiempo México / Monday to Friday from 8:30 to 18:00 / 月曜～金曜8:30～18:00");
            sb.AppendLine();
            sb.AppendLine($"3.{ConstantesServicio.Tab}Proporcione los siguientes datos:");
            sb.AppendLine($"{ConstantesServicio.Tab}Provide the following information:");
            sb.AppendLine($"{ConstantesServicio.Tab}次の情報をお伝えください。");
            sb.AppendLine();
            sb.AppendLine($"{ConstantesServicio.Tab} Folio/Folio/フォリオ番号: {complementoSolDTO.id_solicitud_documento}");
            sb.AppendLine($"{ConstantesServicio.Tab} Código de aprobación/Approval code/承認番号: {codigoActivacion}");
            sb.AppendLine();
            sb.AppendLine($"4.{ConstantesServicio.Tab}CMS Help Technical Desk le proporcionará una contraseña temporal. Favor de ingresar a nuestra banca electrónica para generar su propia contraseña.");
            sb.AppendLine($"{ConstantesServicio.Tab}CMS Help Technical Desk will provide you temporary password. Please log in to our internet banking to change it to your own password.");
            sb.AppendLine($"{ConstantesServicio.Tab}担当者より仮パスワードをお伝えいたします。当行インターネットバンキングにログインしていただき、ご自身のパスワードに変更してください。");
            sb.AppendLine();
            sb.AppendLine("Para mayor información contáctenos a nuestro CMS Help Desk.");
            sb.AppendLine("For more information, please contact to our CMS Help Desk.");
            sb.AppendLine("その他のご質問はCMSヘルプデスクまでお問い合わせください。");
            sb.AppendLine();
            sb.AppendLine("CMS Help Desk / CMSヘルプデスク   Tel: (55) 1102-8303");
            sb.AppendLine("Lunes a Viernes de 8:30 a 18:00 hrs. / Monday to Friday from 8:30 to 18:00 / 月曜日～金曜日 8:30～18:00");
            sb.AppendLine();

            cadenaMensaje = sb.ToString();

            _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
            _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
            _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

            var asuntoBase = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault)
                            ? ConstantesServicio.AsuntoError
                            : _configuracionBase.SujetoDefault;

            var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
            {
                CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenSol,
                NombreOrigen = ConstantesServicio.NombreCorreoOrigenSol,
                Para = _configuracionBase.CorreoParaDefault ?? complementoSolDTO.satm04numsertoken,
                ConCopia = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.ConCopiaSol,
                CopiaOculta = null,
                Asunto = asuntoBase,
                Mensaje = cadenaMensaje,
                ArchivosAdjuntos = null
            });

            if (resultado is false)
            {
                await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                    Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.CorreoParaErrorSol,
                    ConCopia = null,
                    CopiaOculta = null,
                    Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracionBase.SujetoDefault + " Error",
                    Mensaje = ConstantesServicio.MensajeError,
                    ArchivosAdjuntos = null
                });

                if (resultadoEnvioError is false)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
            else
            {
                return true;
            }
        }

    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_TOKENEXECP : ProcesamientoTokenExcepcion, IProcesamientoTokenExcepcion, IENT_TOKENEXECP
    {
        Configuracion    _configuracion;
        IDetallesBot     _detallesBot;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_TOKENEXECP(Configuracion configuracion, IDetallesBot detallesBot)
            : base(configuracion, detallesBot)
        {
            _configuracion   = configuracion;
            _detallesBot     = detallesBot;
        }

        public async Task ProcesarOperacionTokenExecpAsynx()
        {
            try
            {
                //Obtiene los atributos de los complementos de los tokens con excepciones
                Config.EscribirLog("INFO|ProcesarOperacionFcmAsync|Procesando operaciones de tipo FCM ");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoTokenExcepDTO> complementoTokenExecp = await this.ObtencionDatosTokenExcep();

                //Si no pudo obtener sus complementos...
                if (complementoTokenExecp.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionTokenExecpAsynx | No se encontro información");
                    return;
                }

                //Realiza el recorrido de los datos
                foreach (ComplementoTokenExcepDTO compTokenEx in complementoTokenExecp)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionTokenExecpAsynx|Preparando envio de email, Folio :'{compTokenEx.idConsecutivo + ", Usuario: " + compTokenEx.IdUsuario + ", Email: " + compTokenEx.email }'.");
                    bool resultado = await EnviaTokenExcepcion(compTokenEx);

                    if (resultado)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionTokenExecpAsynx|Usuarios|Query para actualizar el estado del envio del correo del remplazo de token: '{compTokenEx.idConsecutivo}'.");
                        await _detallesBot.ActualizaTokenExcepcion(compTokenEx.idConsecutivo);
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionTokenExecpAsynx|Error ENVIO EMAIL en procesamiento de solicitud: '{compTokenEx.idConsecutivo}'.");
                    }

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionTokenExecpAsynx|Se presenta exception:'{ex.ToString()}'");
            }
        }

        public Task<bool> EnviaTokenExcepcion(ComplementoTokenExcepDTO complementoToken)
        {
            try
            {
                if (complementoToken == null)
                    throw new ArgumentNullException(nameof(complementoToken));

                string idUsuario = complementoToken.IdUsuario?.Trim();
                string serieToken = complementoToken.serieToken?.Trim();
                if (string.IsNullOrWhiteSpace(idUsuario) || string.IsNullOrWhiteSpace(serieToken))
                    throw new InvalidOperationException("El usuario y la serie del token son obligatorios.");

                string lsMensaje;

                lsMensaje = "<html>" + ConstantesServicio.NewLine +
                            "<head>" + ConstantesServicio.NewLine +
                            "<style type='text/css' > " + ConstantesServicio.NewLine +
                            ".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold;  }" + ConstantesServicio.NewLine +
                            ".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }" + ConstantesServicio.NewLine +
                            ".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }" + ConstantesServicio.NewLine +
                            ".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }" + ConstantesServicio.NewLine +
                            ".Nota           { font-family: sans-serif; font-size:12px; color: #0E0402; text-decoration:none; }" + ConstantesServicio.NewLine +
                            ".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }" + ConstantesServicio.NewLine +
                            "table           { border: thin solid #FFFFFF }" + ConstantesServicio.NewLine +
                            "</style>" + ConstantesServicio.NewLine +
                            "</head>" + ConstantesServicio.NewLine +
                            "<body>" + ConstantesServicio.NewLine;

                lsMensaje = lsMensaje + "<img src='data:image/jpeg;base64,/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoHBwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/2wBDAQMEBAUEBQkFBQkUDQsNFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBT/wAARCABPAYIDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD9U6KTNB6UARsx9PyoUbhz+tcv4u+IXhn4e2JvfEOr2ekW/Z7mYKG+nNeZJ+2v8IZLz7OfE2z/AKbSWsyx/wDfWzFYzq0oaSkelh8rx2Lh7ShRlKPlFnu3HYc0jZ/u/rXCXvxv8A6b4ci16fxZpcejTcpefaV8tvxrhov21/hDNdeSPFGD/fa0mEf/AH1spSxFKn8UjSjlOYYlP2OHlK3aLPduKXp71h+GfF2keMtNjv8ARdStdTs5B8k1tMHQ/lW32roPLnCdOXJNWY+ikpaRIUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFAEHljrntivKP2h/jfYfAvwLJqcird6tcH7PYWeeZZv/iV6mvWWYJt9zX5dftjfEh/H3xu1S3WbzNM0H/iXW6fwb/8Als//AH3/AOi687HYj6tRvE+14PySOe5kqNX4I+9I8u8UeKPEXxW8VC91a4uNb1y9l8uGP0d/uJCn8FfcPwB/Yh8P+G9NtNY8b2663r0ibxZuM21r/s4/ib3NeYfsBfCePxF4s1Txtfw+ZbaSfs1n5ne4f77f8BTA/wCB194654g07wxpNxqWqXkNhYWqF5biZtqIo715uAwsJw+s1tz7rjTiKrh6/wDYuWe4oaS5f/STFuPhX4MudP8AsM3hjSWtD/y7myi2fltr5U/aK/Yjshptz4i+Hds9rcxAy3GiglklUf8APHP3G/2elem6f+3Z8LdQ1xNNbUby0hZ9iajNZutqf+BkV9BWd5BqFrFc28yTQSruV1OVZfWvTnHD4yHIfBYXG53wxiIYiSnD/F8Mj8gfhh8VfEPwg8RQav4fu2h+f/SLJ/8AU3Cf3GTsa/VD4R/FHS/i/wCCLDxHpTfuZ02ywN9+CX+JG9wa+Av21vhXF8O/i9JqNhD5Gk+IE+2IifcWb/lsn/s//A66X/gn/wDEqfw/8Sr/AMIzy50/XYnmijP8FxDwf++0/wDQK8bBVp4bE/Vpn6vxRluG4hyWGeYWHv8ALzP/AA/aj/26fonXnsPx++HM+tppEXjLR5NUa5+yrai6Tf5u7Zsx65r0Ovxj8cSPB4/8QzRP5c6arcukkf8AA/nPX1J/Ox+zlcp40+J3hX4d/Zf+El16x0M3W7yTezBN+3riua/Z3+KUfxd+E+i687INQKfZr9Bj5LlPv/8Axf8AwOvz0/aw+KX/AAtn4y6pc28/maRpZ/s2wz9zYn33/wCBvQB+l9j8T/CmpeD7rxTa+ILGfw7bB/O1JJh5KbPv/P7UzwX8VPCHxEmuYfDfiHT9amt1Vpkspg+yvkX4Xf8AKPLxn/vXn/oaVT/4Jq/8jR44/wCvO2/9DegD7pvr+DTbSe7upVgtoUaWWR/uoi9TXLeD/jF4J8fak9h4c8T6brN4kXnNBaTB32f3qv8AxK/5J34r/wCwTdf+iXr4H/4Jz/8AJaNV/wCwC/8A6OhoA+1dS/aH+Gui6lc2F7400e1vbaR4JoHuRvR06rUX/DS/wr/6H/Q//AxK/NL4jaBeeKv2gfFGiaZCk+o6l4hubW2jk+Te7zV2v/DD/wAXf+hftP8AwPSgD9EfCPxh8E+PtSbTfDvifTdZvUi85oLWYO4T+9XS6xrNh4f0+a/1K7isbKEfPPO+xFr44/ZB/Z18cfB34m6jrvirTbex0yTSng86O5ST5/MR/wD2SvA/2kvjlq/x28fPp+nPcS+G7a5+y6Vp8P8Ay8vkp5zp/Gz54oA+8P8Ahrf4Sf2h9i/4TSx3f89Mvs/77xXqGheINN8T6bDqGl30Go2U33J7Zw6NX5lf8MT/ABY/sH+0/wCwrfOzf/Z32lPtP/fPrXpP/BP3w34ytfH3iB1ludN8MWCtDqOnXKfJLdfwJs/gdKAPv+iiigArmfC3xA8NeNrnUYNA1uy1afTZPJvEtZg/kvzw/wD3zXTV+cHwl8bS/BjxZ4/8STsEsfEC65bW0f8A0+20/wC7T/ge+gD7nh+Mnge40TV9Yi8U6a+l6PN5F/dJN8ls/wDcetjUfG2haTeaLZ3eqWtvc60+zTYZHw1223f8n4c1+e+geG7rwj+zP+0DpF7J5l5Z31gkz/7f7t3/APQ6+jPi/wD8lL/Zm/6/5v8A0lSgD1aP9oL4bTayukJ410Z9Ue5+ypafal3+dv2bPrvrY8RfFfwb4R1q30jWvE+mabqlz/qrS7uVR3/CvkT9mfWPAMnirV9E1fwhLq3it/EmoXVnrH9m74Idkjun77+D7lVvh/4d+HniD4D+KviP8UU+16h4n1i6tpNVkheaa0+d0hCJ/sUAfceoalbabYTX11OlvZQxNLLM5+REHO4/hXLeE/jR4D8e6kdO8O+LdJ1m+2eZ9mtLlHfb9K4PwlfabqH7J9z/AGTr1x4o0638P3ltFqt3D5L3OyORMun4V8y/BG+0TWPEv7Pdnp+jyeGtU01Jvt+uXVt9mTUvk/1Eb/8ALbfQB+h1FfMVz+0h4v0X4maRYa3oOkad4d1TXRoUNj9s3atDufZHcvH/AHHrnNS/am+JljofivxFF4W0i68NeF/EE+n393JM6SPAk2z5E/v8igD6X0b4haLr3i7XvDFld+ZrWiLC1/b7G/decm9PzFWvFnjbQvAOj/2r4h1S20fTt6x/arp9ib26Cvkub4pXng79oH4nx+G4Ir/xX4mbQ7XRbKf7h3wb3kf/AGESu+/bg02XU/gRpmn3TKLq61zTbZ3VfkLvJs/rQB7b4g+I3hnwn4dt9e1jW7LTdFuNnk3txLsjff8Ac/OpfEXjjQvCfh0a7rGqW9hpJ2bbqR/kbefk/wC+q/PP4seJbzxb8BvDfg2WZ/t3geC/m1iP+49tOltCn/j9e5/Er4ySXmoad4NsNO8K3f8AwjOnafq9w/iq78hGufL3wpCn9+gD64jkEyI68q1Y6eK9IbxM3h4ajbnXVtvtjWPmfvfJ3bPM2+lfNfiD9r3VNJ0zw/4mi0WO78N+KtEdtISPLTrrKPs+zP8A7G/vWq3xUufB/wAWtXufF+h6THqmj+AU1bUtSskfz94k+e2R/wC5voA+m6K+a/Cvx6+IljHBrvjvwbb2Hg3UdLm1a3vtLleR7BETzES5/wB9P1pvgf8AaC8dnxD4NuPG3hnT9K8K+N5hDo89ncb5rd3TzIEm/wB9KAPpauR034r+DdV8UTeG7LxNpl1r0I+fTY7lDOP+AVta5a3F5ouoW1lL5F1NbukMw/gcr8pr4F+H9v4Y8B3Hgrwn8SvAOs+EPE+l6vG8PjCx/wBXc3W/5N7/ANx6AP0OrmvCXxA8N+PPt3/CO63Zaz9gl8i5+yTB/Kk/ut71S+LvixfAvww8V685x/Z+nTTx/wC/sOz/AMexXzF+ydoz/CX4v2vhuZs/8JV4Ps9af5s/6Un+u/8AQ6APs+iiigAooooAqX0wt7OeVvuqhb9K/F3W759U1rU76V981xeTTPJ/vvX7Q30YuLOeP+9Gy1+LesWL6brGo2cqeW9tczQvH/uPXzecbQP3bwt5ObF9/d/9uP0n/YZ0OLTf2e9HnT71/PcXDfXzCn/sleT/APBRrxtdW6+GPCdvKyWlz5l/don/AC1Cnaif+h/lXrf7DmsQ337O+h28X3rGa5t5P97z3b/2evHf+CjHg+6kuPCfiqFWe0hSTTrlh/CxO+M/+h10Vb/2f7h4WVeyfGkvrX88/wBeU+LK/RH9gDx1d+I/hXqGh3szT/2DeCC3kf8A54um9E/4D8wr87q/Qn/gnr4Lu9D+F+r65dI6Jrl/5tuW/ihRAm78X315GV8/1g/TfEX2H9hy9p8fNHk/r/CVP+CjGipdfDnw7qez99a6p5W70V0fP/oIr46+BGsPonxo8D3ifwatEn/AHfZ/7PX2R/wUX1YWvwz8P6cW/e3eqh9vqqo+f/QhXxp8D9LfXPjN4Hs1/j1a3f8A74ff/wCyVtjP98XIefwjf/VKr7b4PfP2DTlFPtX5BR+C7z4jfGnV/D2nukd7ealqHk+Z/G6ec+z/AMcr9fU4RR7V+X3wJ/5PI0v/ALD17/7Wr60/mo574PfHzV/g/wCFfHeg2scqPrVn5Nt/053X3Hf/AL43/wDfCVxsfgC8j+GL+M5f3enf2qmkW3/TZ9ju7/8AAK9q/a++BN54c+OlrHoFo8lt4ym32iInyJdO/wC+T/2evS/2xPh/ZfC39mvwF4ZsT8mn6lHG7f8APV/Jfe//AH3VCIfhf/yjz8Z/715/6GlU/wDgmv8A8jN44/687X/0N6v/AApheb/gnv41RV3u329h/wB9qaxP+CcOpQweOPGNg7jz7nToJo1P8So/z/q9AH218Sf+SeeKP+wVdf8Aol6+Bv8AgnP/AMlo1X/sAv8A+joa+6fjJq0Oh/CfxjfXDqkMWlXRJfp/q3r4Z/4J0wu3xi1d9mEh0Fw//f8ASpA861LxHZ+Dv2sb3XtQd49O03xa91c7E3vsSavtD/hvr4Uf8/Orf+C96+IfGXhWbxx+0trfh6K5W0n1XxJNZJO6fIm+b79e6f8ADtvxD/0O2n/+Ab0AfQHxK+NWm+LP2V/F3jXwxNP9llsZ7eB5k2Pv3+Sf1NfLX/BPvwLZ+Ivi1qGsXcKvH4fsd1sj/wDPZ/k3/wDfG+voHxF8Er74c/sW+J/BL3iare2tnc3ImhTy0f8Afef0rwf/AIJ4+MLTR/ilq+iTyLE+t2H+jbv43hffs/74d6APsL4vftE+DvgjqGm2viaa7jn1CKSaH7JbmX5U6/8AoVcX4T/bK+FfiDxJY6PpUl8moaxeJAgNiU3zP8nz1B+1F+y9qX7QOt+Hr6y1220dNMt5oHWeFn373Q/+yV8VeDfB83w9/ai0HwzPcpdzaV4khtnnRNiP89AH6yUUUUAFfJ+m/so6pq3hPTtO1w2P2mz8eTeIPkffvsHf50/36+sKp2N9balaQ3VrMk1tMN6SJ0egD59sf2c9R1uw+OGla7PbwWXjbUvtNhNbvveFNnyO/wDwOqPw5+CfxK1X4heFNb+JOsaVcaf4Nhkh0qDS0O65kdPL85/T5K+mHkEalm+7VWPVrOS6e3FxEZkVXKbv4WJ2n8aAPLf2cfhnq/wv8K+IdO1xrd5r7Xr3UIfIff8AuZnyma8tm/Z/+K3gN9d8M/D3W9D/AOEI1u5muVTWIN82m+d9/Z/fr6yqtfX1vptq9zdSrBDHy0jnhaAPO9C+EqeCvgTJ4C0WTz5IdInsoJ5/l82Z0f53+rvmvDPAv7PfxWvLf4b+HfFk2g6d4V8FXKXkM2myO9zcvH9z6V9XSapZR6pDp7zIL2aJpo4v9hTgt/48K0qAPi/Sf2WPHun65oPnx+G7yHSvFsetTa38/wDaN/CZ9773/wBj+5XX337Pviy5+A3xX8JJ9h/tfxNr15qFhmX5PJeZHTf/AN8V9Jaxq1pomm3V9ezCC1t03yyf3RV+gD5K8T/sw+LtS8ZeKfFumzWNp4hh/sm58OXXm/8ALa2h2TJJ/sPXqP7QHw68SfFb4a6Lp1hFawavDqthqFxHJLhE8mTe+x677WfHvh7QtWh0rUtUt7S/lVJEt5nwSrHZ/PNdNQB8s/EL9lzUdUsfjdLoX2T+0fGzWf2Hzn2Imx0ebf8A8DBpfGn7O/imz8ZyeJ/DWl+FvEU+q6da2V/Y+JYDIltNCmwTwv8A+yV9S0UAfKPjD9mLxZ480+w0fVNUs4LPw9orvpM+nJ9mSXWXff5/kp9xE+5+Na0PwI8WeNvGWqar41+xww634GTw5fvay75EvN/zun+x/HX0ZfX0GmWc11dzJDawo0ksj/dVRVO68RadZ6XDqk9/bx6dMI9lw7gI28jZz/tbloA+fvDPwT+J/iCxg8M+Ptf05PB2naVNpMVvo+8PqO+HyUef/cSqPhn4KfEY6h4Og8c6lpN94V+H7/atN/stH+1ai8UeyDf/AHNlfSd9r1jp119nuLmOObyHudh6+Un33oh1/TZrC1vUuYja3Qi8mTP39/3PzoA4L4Sr4n8Z/B63bxszW+r6zBNJIkaeTJbQzF/LTH99EK14za/s9/FjX7Dw74E8W+IdJ1PwBot/Be/2lh31C8SGTekb+/8At19Yz3CWsLzStsRF3NVfTdSt9WsLa+tH8y2uI0lik/vK/IoA8D+JHwv+J/xC+HvivwzqGq6ZdpqviNHtinyeTpIdH2f7/wAlZkf7JsPw6+KngTxT8P8Azo4LK5mj1dNUvnm/0V49nybz/wCO1754k8daF4Ra2TV9ShsWud/lLJ/Hs5fFJ/wm+g+XC/8AaEHlzfOjZ4/1fnf+gfNQBj/CXTfGWleFGh8c6jaalrpvJ38+zTYnk+Z+6TH+7XdVXkuEjt2mZv3Krv31DpmqW+safbX1o/mWtxGk0Un95W5FAF6iiigCI4bFfln+158PJvh/8cNZdYtlhrP/ABMbeT3bh0/77r9SxJuA47Zrxz9pb4E2vx08DtZQulprtj+/0+6f+F/7jf7DYwa83MMO8TRtE+44PzyOR5kqlX4J+7I+av8Agn38UotG8Qav4GvZQsWok31l5n/PVR86D324b/gJr7d8T+E9J8aaFd6PrdlFfaddoY5reYZDCvyC1HS/EXwv8YJDexT6B4i0uXem/wCV1dPuOn99K+/fgH+2X4a8faba6b4puYNA8SImxxNJst7nGPnRj93/AHTzXDl+KhyfVqp9pxrw7iJYn+3Mt9+E9ZcvT+8Q2X/BP34d2etx3ks2q3lmr7xp81zmP/d3/fx+NfSum6XbaPp8FpZQpa2sCeXHDGuFRfQVQn8YaDDa/aptZsVhH/LRrlQv55r5a/aQ/bW07R9NutA+H96uoavKpil1aM5htf8Ad/vv7V6T+r4OHOfB0aOd8T4iFCcpT/xfDE8W/bk+KUXj74tppFjN5+n+HIntgyfca4f/AFw/9AStH9gX4dy+IvirceKJYf8AQPD9u6I3/TzL8n/oG+vCPAPw+8Q/FXxRDo2hWst7fzPvmnk+5F/feZ6/VH4K/CXSvgz4Fs/Dum/vJE/e3N0fvTzN99z+NeHgqM8ZiPrUz9a4nzHDcN5HDI8LP35x5X/7dL/t49Dr8vvgP/yeVpf/AGHtQ/8Aa1fqFXl2i/s4/Djw74xj8Uaf4YtrXXoppLlL1GffvfO9uv8AtGvqj+dTutQ8P6ZrF5pt1eWcN1c6bP8AaLN3X/UuUKF0/wCAsRXzD/wUZ/5JT4b/AOwyv/omSvrWuR+IXwt8L/FTS7bT/FOlQ6tZ28wnihn6K9AHhn7FOh2/ib9lu60i65tb+5v7WUf7D/If0r4huI/GH7NPxWeG3mfSfEOjzOkLyJ8lzD/f/wBtHSv1a8B/D/QfhroKaL4c02HStLWR5hbxdN7HLGs/4ifCLwf8WLVIPE+hW2qmEZhmkT95H/uNQB+bfxc/au8b/GDw4mi6vLa6bpPyPcw2S7Tc/wC/X1F+wF8I73wh4R1Txdqtu1tda6UjtI5F2OLZB98/77c16Z4V/ZD+FPhTUo7+08MRXNzE2+M3zvPs/wCAvXs8aLGuxV2ItAH5b6X/AMnqwf8AY5/+16/UyvLI/wBm34bw+Ml8Vp4YtU15L37f9uDvv8/O7fXqdAFa6tIL63ltriNZIZV2OjfxLX5R/HL4R67+zt8Tdts9xaad9p+06JrEP9z+BN/99K/WWsbxT4R0Xxxo82la9ptvq2nTfft7pN6GgD88of2/fibDoMdh5GjSXu3b/aTwvv8A++PuVyP7O/hnxZ8YPj7pGvJDLqTWuqpqeq6q/wDq0+ff9/8Av/7FfbEf7EPwiTUPtP8Awjb7P+ff7S/k/wDfFex+GPCWjeC9LTTdC0230rT4/uW9qmxBQBs0UUUAMrwLw1Drt9oEUWn6/PpNnpfh+2vYYYET55t8/wB//Y/d/cr6ArmNK8B6Vo9rPbwJIY5rNNPfe/8AyxTfgf8Aj70AeVatr2r6uuoyX+p6hp0Wq6dMlnHboj2UyfZd21H/AIJt+/79VtQ0u+urDQtEfXdSjS3/ALDdbqN/35d3m3/PXpdr8KdEsrxJk+2Nboo/0B7lvs2/y/K83Z/f2cUsfwp0ldLlsjcX+/ZCiXX2l/OhELu8Ox/9jeaAMO48Uanb6KJvtTSOvimSz/34UmcbP/HKxr258SW3g6PxK/iXe+qWyXDWUyrsh3ujJ5X+4md/0rt7j4U6Jca6mqSPeFlu01BbT7S/kfaU/wCW2z+/UP8Awp/QQs6k3kltIjpDbSXL+Xaq77n8lf4KAOcj1TVVmi8Zf2hM8VxqTab/AGZtQItv57wp/wAD34eo/AN74ghn8F32pa5Pqo163mW4hdU8tJEj3o6f98V2i/DfSI/EDasDdf617n7H57fZhM/35tn981e0/wAGadp6aCsSSbdFR47Pc/QMmz/0GgDy/wCIg1XxJ4b8aalFrUljb6ZM9hHp4VDA6ps37/8Aafef0qbTvFviTVvGBure31t7b+13094XhRNPS2R9jyb9+/f/AB/+OV2XiD4S6D4qvL2a+F35V9s+12sNyyQzMn3HdP73Sro+HWmf8JB/a++7/wBd9q+xLcv9m8//AJ7bP79AHn/xGsdc1Dxh4pttH07T75JtDtkuHvX2Oq75/uVGfGV5JrWk6hpOp6tc6Y15aWfkrbobOOF0Tekzn5/O+f79d54i+G9j4k1aTUZtS1Ozea2S1uIbK7eGOZE343gf771E3wn0J9Wgule8gihuIb1NPhuWS2WZMbH2f8BoA8+svFniTw54f0rX7vWpNVk1KwvGe1mCeSjwxu8Lp6fc+eug1SHWPCmmyLd+Lri6/tFYUjWOFXvfOZ/n+zJ/tqT/ALnWuyh8CaRb2el2XlM9vpySpBHI27KOhRw/977xrJT4P6LHbvGbnU5ZvkFvdPdu01oifdWF/wCCgDCtdYu9a+Dfi9NS+0SSWkeoWZ+3qnnuiI/3wn8eyuI8QfvvAuneDJvnj8OTJNL/ANcYXh+zf+jk/wC+K9o0vwDpWk+G9R0KL7RJZ6g0z3LTTM8jvN9/56Lr4c6HePqczW7efqUNvDczI3zukP8Aq6AOI1zWrqTw/c3fnD7S+vXOmPN5fz/Zg837muf8H295o/2nUoNRu5PO/sCH7LI++FEdId+xK9IvPhToupavJqEz3mGuTefZUun8hZtmxpNn96pofhnpUF1BLH9qRI4bWEw+d8j/AGZ0eF2/2xsHNAHnmnan4gTwjo/iLUPEEmojVZp7C5sQqCHY5mVNn+2hRP8Ax+svSvFHiJ7O2TSINblOi2dhDbw2UKfYpswI7+c7v6P/AMAxXe+CfhDZ6Dplh9vkuHu4El/0RLp2toncvudE/vlHxWtN8KdEkez2vfQwQwwwy28NyyR3KQ/c85P46AMrxte6lZ/ETw5Jp2jQ61O2nXgeCW4SHYm+D59zpXN+OdZvIfEWpyI6o+mvcm2TYn7n/iWb/wDgfz167NoltNrdrqbK32u3hkhT5v4XKb//AEBPyrI1T4d6RrN3e3E6S+deb/O2S/34fJP/AI5QB5vceKNTPiK1lS/1S+sry5ewmT7OiaeieS52b/v+cjp9+vTPhn/yTzw3/wBg6D/0AVmRfCnRv7UivGlvSsE32qG0Fy4ghm2OjuiZ/iDvmup0XSbfQ9Js9Otdy21rCkMYb+6mAKANGiiigBKKWigDhPHnwj8JfFCxW18T6Ja6oE5SSRPnT/dYfMK8qj/YK+FEd15r2F/Mm7PkyXz7PyzX0Zt3cA4pGyBisJUKVT4onr4XOMxwdP2OHryjDykeW6h+zb8N9T8Kw+HZPC1mNKhO+KJAVZG/vb87s+9cLD+wV8KYbpZRY38iBs+TJfSFPyr6NGQORikK98k0SoUp7xKo51mWGvGjiJK/945nwT8OvD/w90saf4f0q10y0HSO3jwD7tXTN6EUrY2+goXG3g5FbxjyrQ8mrVnWnz1JXkPpaKKCAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKAP/9k=' />" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<BR><BR><hr color = red><br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<span class='Nota'>Estimado(a) / Dear / お客様各位:</span><span class='titulo_general'>" + ConstantesServicio.Tab + idUsuario + "</SPAN><br><br><br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<LI><span class='Nota'>Su Token se encuentra vencido y próximamente recibirá la reposición de su dispositivo. En esta ocasión, a manera de contingencia y para garantizar su servicio le estamos proporcionando un número de Token temporal y único para poder ingresar a su banca electrónica y realizar sus operaciones cotidianas. Considere que recibirá un nuevo número cada vez que requiera iniciar sesión.</span></LI> <br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<LI><span class='Nota'>Your Token is expired and soon you will receive your device replacement. On this occasion, as a contingency and to guarantee your service we are providing you with a temporary and unique token number to enable your access to your electronic banking and carry out your daily operations. Consider that you will receive a new number each time you need to log in. </span></LI> <br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<LI><span class='Nota'>今般、輸送途上の問題によりお客様へのトークンの到着が遅延する事象が発生しており、現在お手元にお持ちのトークンは所定の有効期限を超過しております。これを受けまして、今般弊行より代替となるトークンをお送りしております。" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "代替のトークンのお届けに合わせまして、CMSサービスへのアクセスが可能となる臨時のトークン番号を送付申し上げます。" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "なお、この臨時のトークン番号はお客様がCMSにログインするたびに更新され、新たな番号を送付させていただきます旨ご留意ください。</span></LI> <br><br><br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<br><br>" + ConstantesServicio.NewLine;

                lsMensaje = lsMensaje + "<table border= 0 wifth = 80% align= left>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<TR><td  width=10%></td><td width=60% ALIGN=RIGHT>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<span class='Nota'>Su número de token temporal es <br><br> Your temporal token number is  <br><br> お客様のトークン番号 </span> </td>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<td width=30% ALIGN = CENTER><span class='Nota'></span>    <span class='titulo_general'>" + ConstantesServicio.Tab + serieToken + "</span></td>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "</tr></table>  " + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<br><br><br><br><br><br><CENTER>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<span class='Nota'>*CMS Help Desk / CMSヘルプデスク   Tel: (55) 1102- 8303 / (55) 1102- 8308</span><br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<span class='Nota'><a href='mailto:Mexico_Helpdesk_CMS@mx.mufg.jp'> Mexico_Helpdesk_CMS@mx.mufg.jp</a></span><br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<span class='Nota'>Lunes a Viernes de 8:30 a 18:00 hrs. / Monday to Friday from 8:30 to 18:00 / 月曜～金曜 8:30～18:00</span><br><br><br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<TABLE WIDTH=100%><tr><td style='background-color:#FF1010; color:#FFFFFF; text-align:CENTER'>https://cms.mx.nk.mufg.jop/</td></tr></table></CENTER><BR><BR>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<span class='Nota'>*Atentamente / Sincerely / 敬具</span><br><br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "<span class='Nota'>MUFG Bank México S.A.</span><br>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "</body>" + ConstantesServicio.NewLine;
                lsMensaje = lsMensaje + "</html>" + ConstantesServicio.NewLine;

                MensajeCorreo mensajeCorreo = new MensajeCorreo();

                _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                mensajeCorreo.CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso;
                mensajeCorreo.NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso;
                mensajeCorreo.Para         = _configuracion.CorreoParaDefault ?? complementoToken.email;
                mensajeCorreo.ConCopia     = null;
                mensajeCorreo.CopiaOculta  = _configuracion.CorreoParaDefault ?? ConstantesServicio.CorreoCCTokenExcepcion;
                mensajeCorreo.Asunto       = _configuracion.SujetoDefault ?? ConstantesServicio.AsuntoTokenExcepcion;
                mensajeCorreo.Mensaje = lsMensaje;
                mensajeCorreo.ArchivosAdjuntos = null;

                if (CorreoHelper.EnviarCorreo(mensajeCorreo))
                {
                    return Task.FromResult(true);
                }
                else
                {
                    mensajeCorreo.CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol;
                    mensajeCorreo.NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol;
                    mensajeCorreo.Para         = _configuracion.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR;
                    mensajeCorreo.ConCopia     = _configuracion.CorreoParaDefault ?? ConstantesServicio.CorreoCCTokenExcepcion; ;
                    mensajeCorreo.CopiaOculta  = _configuracion.CorreoParaDefault ?? ConstantesServicio.CorreoErroHelpDesk;
                    mensajeCorreo.Asunto       = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracion.SujetoDefault + " Error";
                    mensajeCorreo.Mensaje      = ConstantesServicio.MensajeAvisoTokenExcep;
                    mensajeCorreo.ArchivosAdjuntos = null;

                    CorreoHelper.EnviarCorreo(mensajeCorreo);
                    return Task.FromResult(true);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaTokenExcepcion|Se presenta exception:'{ex.ToString()}'");
                return Task.FromResult(false);
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_AVISOPWD : ProcesamientoAvisoPwd, IProcesamientoAvisoPwd, IENT_AVISOPWD
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesTomi;
        IDetallesTomser   _detallesTomser;
        IDetallesBot      _detallesBot;
        IDetallesBotLog   _detallesBotLog;
        IENT_ENVAVISOPWD  _entEnvAvisoPwd;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_AVISOPWD(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IDetallesBot detallesBot, IDetallesBotLog detallesBotLog, IENT_ENVAVISOPWD entEnvAvisoPwd)
            : base(configuracion, detallesTomi, detallesTomser, detallesBot, detallesBotLog)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _detallesBot     = detallesBot;
            _detallesBotLog  = detallesBotLog;
            _entEnvAvisoPwd  = entEnvAvisoPwd;
        }

        public async Task ProcesarOperacionAvisoPwdAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los ben.
                Config.EscribirLog("INFO|ProcesarOperacionAvisoPwdAsync|Procesando notificaciones de envio de cambios de password ");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoAvisoPwdDTO> complementoAvisoPwd = await this.ObtencionDatosAvisoPwd();

                //Si no pudo obtener sus complementos...
                if (complementoAvisoPwd.Count is 0)
                {
                    Config.EscribirLog("No se encontro el detalle de la operación");
                    return;
                }

                //Realiza el recorrido de los datos
                foreach (ComplementoAvisoPwdDTO AvisoPwd in complementoAvisoPwd)
                {
                    var arregloValores = new object[7];

                    //Se asignan valores al arreglo
                    arregloValores[0] = AvisoPwd.IDENTIFICADOR;
                    arregloValores[1] = AvisoPwd.FECHA;
                    arregloValores[2] = AvisoPwd.HORA;
                    arregloValores[3] = AvisoPwd.USUARIO;
                    arregloValores[4] = AvisoPwd.DESCRIPCION;

                    Config.EscribirLog($"INFO|ProcesarOperacionAvisoPwdAsync|Busca identificador del cliente por el usuario: '{AvisoPwd.USUARIO}'");
                    List<ComplementoClienteDTO> complementoCliente = await this.ObtenerClientesAsync(AvisoPwd.USUARIO);
                    Config.EscribirLog($"INFO|ProcesarOperacionAvisoPwdAsync|Busca identificador del cliente por el usuario: '{AvisoPwd.USUARIO}'");

                    foreach (ComplementoClienteDTO cliente in complementoCliente)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionAvisoPwdAsync|Busca informaci[on de contacto: '{AvisoPwd.USUARIO + " - " + cliente.ID_CLIENTE}'");
                        List<ComplementoContactoDTO> complementoContacto = await this.ObtenerContactosAsync(AvisoPwd.USUARIO, cliente.ID_CLIENTE);

                        foreach (ComplementoContactoDTO contacto in complementoContacto)
                        {
                            arregloValores[5] = contacto.SATM04COR;
                            arregloValores[6] = contacto.SATM04EMA;


                            if (contacto.DF_CONTACTOSCUB_EMAIL != null && (contacto.SATM04COR != null || contacto.SATM04EMA != null))
                            {
                                bool resultado = await _entEnvAvisoPwd.EnviaAvisosPwd(ConstantesServicio.tipoFXD, contacto.SATM04EMA, contacto.DF_CONTACTOSCUB_EMAIL, arregloValores);
                                Config.EscribirLog($"INFO|ProcesarOperacionAvisoPwdAsync|Busca informaci[on de contacto: '{AvisoPwd.USUARIO + " - " + cliente.ID_CLIENTE}'");

                                if (resultado)
                                {
                                    Config.EscribirLog($"INFO|ProcesarOperacionAvisoPwdAsync|Se termina procesamiento de folio: '{AvisoPwd.USUARIO + " - " + contacto.DF_CONTACTOSCUB_EMAIL + " - " + contacto.SATM04COR}'");
                                }
                                else
                                {
                                    Config.EscribirLog($"INFO|ProcesarOperacionAvisoPwdAsync|Error en procesamiento de folio: '{AvisoPwd.USUARIO + " - " + contacto.DF_CONTACTOSCUB_EMAIL + " - " + contacto.SATM04COR}'");
                                }
                            }
                        }
                    }

                    //Realiza la actualización de estatus de la solicitud
                    await _detallesBotLog.ActualizaFolioAvisoPwd(AvisoPwd.IDENTIFICADOR);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionAvisoPwdAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }

    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_AVISOOLVPWD : ProcesamientoOlvidoPwd, IProcesamientoOlvidoPwd, IENT_AVISOOLVPWD
    {
        Configuracion    _configuracion;
        IDetallesBot     _detallesBot;
        IDetallesTomser  _detallesTomser;
        IDetallesEnlace _detallesEnlace;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_AVISOOLVPWD(Configuracion configuracion, IDetallesBot detallesBot, IDetallesTomser detallesTomser, IDetallesEnlace detallesEnlace)
            : base(configuracion, detallesBot, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesBot     = detallesBot;
            _detallesTomser  = detallesTomser;
            _detallesEnlace  = detallesEnlace;
        }

        public async Task ProcesarOperacionAvisoOlvidoPWD()
        {
            try
            {
                //Obtiene los atributos de la información de las solicitudes de olvido de PWD
                Config.EscribirLog("INFO|ProcesarOperacionAvisoOlvidoPWD|Procesando operaciones de olvido de PWD");

                //Obtiene los complementos de las solicitudes...
                List<ComplementoUsuarioEmailDTO> complementoUsuE = await this.ObtencionDatosSolicitudPwd();

                //Si no pudo obtener sus complementos...
                if (complementoUsuE.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionAvisoOlvidoPWD | No se encontro información");
                    return;
                }

                //Realiza el recorrido de los datos
                foreach (ComplementoUsuarioEmailDTO OperacUsuE in complementoUsuE)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionAvisoOlvidoPWD|Busca identificador del usuario con solicitude de olvido de pwd: '{OperacUsuE.usuario}'");

                    if (OperacUsuE.usuario != null) {
                        await _detallesEnlace.ActualizaFolioOlvidoPwd(OperacUsuE.new_mail, OperacUsuE.usuario);
                        await _detallesTomser.ActualizaFolioOlvidoPwd(OperacUsuE.new_mail, OperacUsuE.usuario);
                    }

                    //Se manda email de notificaciones
                    var sb = new StringBuilder();

                    sb.AppendLine("<html>");
                    sb.AppendLine("<head>");
                    sb.AppendLine("<style type='text/css'>");
                    sb.AppendLine(".titulogeneral { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                    sb.AppendLine(".titulotabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".headertabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                    sb.AppendLine(".datotabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                    sb.AppendLine(".Nota          { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                    sb.AppendLine(".NotaOmitir    { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                    sb.AppendLine("table { border: thin solid #E0E0E0; }");
                    sb.AppendLine("</style>");
                    sb.AppendLine("</head>");

                    sb.AppendLine("<body>");
                    sb.Append(OperacUsuE.mensaje);
                    sb.AppendLine("</body>");

                    sb.AppendLine("</html>");

                    string lsCadenaMensaje = sb.ToString();


                    _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                    _configuracion.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                    _configuracion.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);
                    
                    var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                               ? ConstantesServicio.AvisoNotificacion
                               : _configuracion.SujetoDefault;

                    var resultadoEmail = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                        Para = _configuracion.CorreoParaDefault ?? OperacUsuE.new_mail,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = asuntoBase,
                        Mensaje = lsCadenaMensaje,
                        ArchivosAdjuntos = null
                    });
                    if (resultadoEmail)
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionAvisoOlvidoPWD| Actualiza_Email| Se envio Correo a '{OperacUsuE.new_mail}'");
                        await _detallesBot.ActualizaFolioOlvidoPwdCms(OperacUsuE.new_mail, OperacUsuE.usuario);
                        continue;
                    }
                    else
                    {
                        await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                        {
                            CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                            NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                            Para = _configuracion.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                            ConCopia = null,
                            CopiaOculta = null,
                            Asunto = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault) ? ConstantesServicio.AsuntoErrorPwd : _configuracion.SujetoDefault + " Error",
                            Mensaje = ConstantesServicio.MensajeError,
                            ArchivosAdjuntos = null
                        });
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionAvisoOlvidoPWD| Se presenta exception:'{ex.ToString()}'");
                return;
            }
        }
    }
}

using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes;
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Policy;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_AVISOURL : IENT_AVISOURL
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_AVISOURL() { 
        }

        public Task ProcesarValidacionURLAsync()
        {
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(ConstantesServicio.urlValida);
               
                request.Method = "GET";

                using (HttpWebResponse response =

                (HttpWebResponse)request.GetResponse())
                {
                    Config.EscribirLog($"INFO|ProcesarValidacionURLAsync | Staus URL | '{ConstantesServicio.urlValida}' | responseString '{response.StatusDescription}'");
                }
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarValidacionURLAsync | DetectaURLArriva |Se presenta exception: '{ex.ToString()}'");
                return Task.CompletedTask;
            }
        
        }

    }
}

using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BEN : IENT_BEN
    {
        private readonly IENT_BENFXD _entBenFxd;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ENT_BEN(IENT_BENFXD entBenFxd)
        {
            _entBenFxd = entBenFxd;
        }

        public async Task ProcesarCorreoBeneficiarios()
        {
            Config.EscribirLog("INFO|ProcesarCorreoBeneficiarios|Realiza el envio de correos por tipo de beneficiario");
            await _entBenFxd.ProcesarOperacionBeneficiarioFXDAsync();
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENCMS : ProcesamientoBenCms, IProcesamientoBenCms, IENT_BENCMS
    {
        Configuracion     _configuracion;
        IDetallesTomser   _detallesTomser;
        IENT_ENVIOBENCMS _envioBenCms;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENCMS(Configuracion configuracion, IDetallesTomser detallesTomser, IENT_ENVIOBENCMS envioBenCms)
            : base(configuracion, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomser  = detallesTomser;
            _envioBenCms     = envioBenCms;
        }

        public async Task ProcesarOperacionBeneficiarioCmsAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios CMS...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioCmssync|Procesando beneficiarios CMS");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenCmsDTO> complementoBenCms = await this.ObtencionDatosBenCms();

                //Si no pudo obtener sus complementos...
                if (complementoBenCms.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioFXDAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoBenCmsDTO BenCms in complementoBenCms)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioCmssync|Preparando envio del folio: '{BenCms.ID_SOLICITUD}'");
                    Config.EscribirLog($"INFO|Procesa_Notificaciones_Beneficiarios_CMS|Correos = ' {BenCms.DF_CONTACTOSCUB_EMAIL}'");
                    //Realiza el envio de las notificaciones H2H de tipo FXD
                    bool resultado =  await _envioBenCms.EnviarNotificacionesBenCms(BenCms.DF_CONTACTOSCUB_EMAIL, null,  BenCms );

                    if (resultado)
                    {
                        await _detallesTomser.ActualizarEstatusEmailBenCms(BenCms.ID_SOLICITUD);
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioCmssync|Se termina procesamiento de folio:. '{BenCms.ID_SOLICITUD}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioCmssync|Error en procesamiento de folio: '{BenCms.ID_SOLICITUD}'");
                    }
                }

            }
            catch(Exception ex) 
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioCmssync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENDRE : ProcesamientoBenDre, IProcesamientoBenDre, IENT_BENDRE
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesTomi;
        IDetallesTomser   _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO        _entAviso;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENDRE(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso        = entAviso;
        }

        public async Task ProcesarOperacionBeneficiarioDREAsync()
        {
            try
            { 
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo DRE...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioDREAsync|Procesando Solicitud de envio operaciones PaP_FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenDreDTO> complementoBenDre = await this.ObtencionDatosBenDre();

                //Si no pudo obtener sus complementos...
                if (complementoBenDre.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioDREAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoBenDreDTO BenDre in complementoBenDre)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioDREAsync|Preparando envio de email, folio: '{BenDre.NF_FOLIO}'");
                    //Buscamos si el cliente tiene habilitado el evento de notificación '006 - NOMFXD'
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    bool resultado = false;

                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenDre.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento8);

                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioDREAsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBDRE(ConstantesServicio.tipoDRE, lsMailCub, null, BenDre );
                    }

                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioDre(BenDre.NF_FOLIO);
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioDREAsync|Se termina procesamiento de folio: '{BenDre.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioDREAsync|Error en procesamiento de folio: '{BenDre.NF_FOLIO}'");
                    }

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioDREAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENFXD : ProcesamientoBenFxd, IProcesamientoBenFxd, IENT_BENFXD
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesTomi;
        IDetallesTomser   _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO        _entAviso;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENFXD(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso        = entAviso;
        }

        public async Task ProcesarOperacionBeneficiarioFXDAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo FXD...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioFXDAsync|Procesando Solicitud de envio operaciones PaP_FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenFxdDTO> complementoBenFxd = await this.ObtencionDatosBenFxd();

                //Si no pudo obtener sus complementos...
                if (complementoBenFxd.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioFXDAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoBenFxdDTO BenFxd in complementoBenFxd)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXDAsync|Preparando envio de email, folio: '{BenFxd.NF_FOLIO}'");
                    //Buscamos si el cliente tiene habilitado el evento de notificación '004 - PAPFXD'
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    bool resultado = false;

                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenFxd.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento4);

                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        var DtoCubFxd = new ComplementoCUBFXDDTO
                        {
                        NF_FOLIO = BenFxd.NF_FOLIO,
                        T24_SHORT_NAME = BenFxd.T24_SHORT_NAME,
                        T24_BEN_ACCT_NO = BenFxd.T24_BEN_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenFxd.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenFxd.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenFxd.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenFxd.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenFxd.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenFxd.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenFxd.T24_DEBIT_CURRENCY,
                        SATM02COD = BenFxd.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenFxd.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenFxd.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO = BenFxd.T24_DEBIT_ACCT_NO,
                        T24_CUSTOMER_CODE = BenFxd.T24_DEBIT_ACCT_NO,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null
                        };

                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXDAsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBFXD(ConstantesServicio.tipoFXD, lsMailCub, null, DtoCubFxd);
                    }

                    if (resultado)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXDAsync|Se termina procesamiento de folio: '{BenFxd.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioFXDAsync|Error en procesamiento de folio: '{BenFxd.NF_FOLIO}'");
                    }

                    bool lbResultado = false;
                    string lsMailTo = null;
                    string lsMailCC = null;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoFXD, BenFxd.SATM02COD.ToString(), BenFxd.ID_FORMATO_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                        lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                    }

                    if (lsMailTo != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenFxd.NF_FOLIO,
                        T24_SHORT_NAME = BenFxd.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = BenFxd.T24_DEBIT_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenFxd.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenFxd.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenFxd.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenFxd.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenFxd.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenFxd.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenFxd.T24_DEBIT_CURRENCY,
                        SATM02COD = BenFxd.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenFxd.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenFxd.T24_DEBIT_THEIR_REF,
                        T24_BEN_ACCT_NO = BenFxd.T24_BEN_ACCT_NO,
                        T24_CUSTOMER_CODE = BenFxd.T24_CUSTOMER_CODE,
                        T24_CONCEPT = string.Empty,
                        T24_CURRENCY = string.Empty,
                        T24_REFERENCE = string.Empty,
                        T24_AMOUNT = null,
                        T24_NO_AGREEMENT = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoFXD, lsMailTo, lsMailCC, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioFXDAsync|Error en procesamiento de folio:");
                    }
                    else
                    {
                        lbResultado = true; 
                    }

                    if (lbResultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioFxd(BenFxd.NF_FOLIO);
                        Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioFXDAsync | Se termina procesamiento de folio: '{BenFxd.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioFXDAsync  | Error en procesamiento de folio: '{BenFxd.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioFXDAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENFXI : ProcesamientoBenFxi, IProcesamientoBenFxi, IENT_BENFXI
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO _entAviso;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENFXI(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesTomser = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso = entAviso;
        }
        public async Task ProcesarOperacionBeneficiarioFXIAsync()
        {
            //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo FXI...
            Config.EscribirLog("INFO|Procesa_Aviso_Beneficiarios_FXI|Procesando Solicitud de envio operaciones FXI");

            //Obtiene los CDA de los complementos de la operación...
            List<ComplementoBenFxiDTO> complementoBenFxi = await this.ObtencionDatosBenFxi();

            //Si no pudo obtener sus complementos...
            if (complementoBenFxi.Count is 0)
            {
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioFXIAsync | No se encontro información");
                return;
            }

            foreach (ComplementoBenFxiDTO BenFxi in complementoBenFxi)
            {
                Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXIAsync|Preparando envio de email, folio: '{BenFxi.NF_FOLIO}'");
                //Buscamos si el cliente tiene habilitado el evento de notificación '004 - PAPFXD'
                bool lbAvisoCub = false;
                string lsMailCub = "";
                bool resultado = false;

                List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenFxi.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento3);

                foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                {
                    lbAvisoCub = true;
                    if (lsMailCub is null)
                    {
                        lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }
                    else
                    {
                        lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }

                    var DtoCubFxi = new ComplementoBenPapFxiDTO
                    {
                        NF_FOLIO = BenFxi.NF_FOLIO,
                        T24_SHORT_NAME = BenFxi.T24_SHORT_NAME,
                        NF_CUENTA_CORRESPONSAL = BenFxi.NF_CUENTA_CORRESPONSAL,
                        T24_DEBIT_AMOUNT = BenFxi.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenFxi.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenFxi.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenFxi.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenFxi.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenFxi.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenFxi.T24_DEBIT_CURRENCY,
                        SATM02COD = BenFxi.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenFxi.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenFxi.T24_DEBIT_THEIR_REF,
                        NF_COUNTRY = BenFxi.NF_COUNTRY,
                        NF_CHARGE = BenFxi.NF_CHARGE,
                        NF_MENSAJE = BenFxi.NF_MENSAJE,
                        T24_DEBIT_ACCT_NO = BenFxi.T24_DEBIT_ACCT_NO,
                        T24_CUSTOMER_CODE = BenFxi.T24_CUSTOMER_CODE
                    };

                    if (lbAvisoCub)
                    {
                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXIAsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBFXI(ConstantesServicio.tipoFXI, lsMailCub, null, DtoCubFxi);
                    }

                    if (resultado)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXIAsync|Se termina procesamiento de folio: '{BenFxi.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioFXIAsync|Error en procesamiento de folio: '{BenFxi.NF_FOLIO}'");
                    }

                    bool lbResultado = false;
                    string lsMailTo = null;
                    string lsMailCC = null;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoFXI, BenFxi.SATM02COD.ToString(), BenFxi.ID_FORMATO_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                        lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                    }

                    if (lsMailTo != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenFxi.NF_FOLIO,
                        T24_SHORT_NAME = BenFxi.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = BenFxi.T24_DEBIT_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenFxi.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenFxi.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenFxi.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenFxi.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenFxi.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenFxi.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenFxi.T24_DEBIT_CURRENCY,
                        SATM02COD = BenFxi.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenFxi.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenFxi.T24_DEBIT_THEIR_REF,
                        T24_BEN_ACCT_NO = string.Empty,
                        T24_CUSTOMER_CODE = BenFxi.T24_CUSTOMER_CODE,
                        T24_CONCEPT = string.Empty,
                        T24_CURRENCY = string.Empty,
                        T24_REFERENCE = string.Empty,
                        T24_AMOUNT = null,
                        T24_NO_AGREEMENT = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                        NF_CUENTA_CORRESPONSAL = BenFxi.NF_CUENTA_CORRESPONSAL,
                        NF_COUNTRY = BenFxi.NF_COUNTRY,
                        NF_CHARGE = BenFxi.NF_CHARGE,
                        NF_MENSAJE = BenFxi.NF_MENSAJE
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoFXI, lsMailTo, lsMailCC, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioFXIAsync|Error en procesamiento de folio:");
                    }
                    else
                    {
                        lbResultado = true;
                    }

                    if (lbResultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioFxd(BenFxi.NF_FOLIO);
                        Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioFXIAsync | Se termina procesamiento de folio: '{BenFxi.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioFXIAsync  | Error en procesamiento de folio: '{BenFxi.NF_FOLIO}'");
                    }

                }
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENFXT : ProcesamientoBenFxt, IProcesamientoBenFxt, IENT_BENFXT
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesTomi;
        IDetallesTomser   _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO        _entAviso;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENFXT(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesTomser = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso = entAviso;
        }

        public async Task ProcesarOperacionBeneficiarioFXTAsync()
        {
            //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo FXD...
            Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioFXTAsync|Procesando Solicitud de envio operaciones PaP_FXD");

            //Obtiene los CDA de los complementos de la operación...
            List<ComplementoBenFxtDTO> complementoBenFxt = await this.ObtencionDatosBenFxt();

            //Si no pudo obtener sus complementos...
            if (complementoBenFxt.Count is 0)
            {
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioFXTAsync | No se encontro información");
                return;
            }

            foreach (ComplementoBenFxtDTO BenFxt in complementoBenFxt)
            {
                Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXTAsync|Preparando envio de email, folio: '{BenFxt.NF_FOLIO}'");
                //Buscamos si el cliente tiene habilitado el evento de notificación '004 - PAPFXD'
                bool   lbAvisoCub = false;
                string lsMailCub  = "";
                bool   resultado  = false;

                List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenFxt.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento2);

                foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                {
                    lbAvisoCub = true;
                    if (lsMailCub is null)
                    {
                        lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }
                    else
                    {
                        lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }
                }

                if (lbAvisoCub)
                {
                    var DtoCubFxd = new ComplementoCUBFXDDTO
                    {
                    NF_FOLIO = BenFxt.NF_FOLIO,
                    T24_SHORT_NAME = BenFxt.T24_SHORT_NAME,
                    T24_BEN_ACCT_NO = BenFxt.T24_BEN_ACCT_NO,
                    T24_DEBIT_AMOUNT = BenFxt.T24_DEBIT_AMOUNT,
                    NF_FECHA_EJECUCION = BenFxt.NF_FECHA_EJECUCION,
                    NF_FECHA_AUTORIZA = BenFxt.NF_FECHA_AUTORIZA,
                    NF_BANCO_BENEF = BenFxt.NF_BANCO_BENEF,
                    T24_TRANSACTION_ID = BenFxt.T24_TRANSACTION_ID,
                    T24_BEN_CUST_NAME = BenFxt.T24_BEN_CUST_NAME,
                    T24_DEBIT_CURRENCY = BenFxt.T24_DEBIT_CURRENCY,
                    SATM02COD = BenFxt.SATM02COD,
                    ID_FORMATO_PLANTILLA = BenFxt.ID_FORMATO_PLANTILLA,
                    T24_DEBIT_THEIR_REF = BenFxt.T24_DEBIT_THEIR_REF,
                    T24_DEBIT_ACCT_NO = BenFxt.T24_DEBIT_ACCT_NO,
                    T24_CUSTOMER_CODE = BenFxt.T24_DEBIT_ACCT_NO,
                    NF_NUMERIC_REFERENCE = string.Empty,
                    NF_CLASIFICACION_OPERACION = string.Empty,
                    NF_RELACION_COMERCIAL = string.Empty,
                    NF_FECHA_CARGO = null,
                    NF_FECHA_APLICACION = null
                    };

                    //Enviar configuración del correo
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioTAXAsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                    resultado = await _entNotificacion.EnviarNotificacionesCUBFXD(ConstantesServicio.tipoFXT, lsMailCub, null, DtoCubFxd);
                }

                if (resultado)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioTAXAsync|Se termina procesamiento de folio: '{BenFxt.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                }
                else
                {
                    Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioTAXAsync|Error en procesamiento de folio: '{BenFxt.NF_FOLIO}'");
                }

                bool lbResultado = false;
                string lsMailTo = null;
                string lsMailCC = null;

                List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoFXT, BenFxt.SATM02COD.ToString(), BenFxt.ID_FORMATO_PLANTILLA);

                foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                {
                    lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                    lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                }

                if (lsMailTo != null)
                {
                    var DtoAvisos = new ComplementoAvisosDTO
                    {
                    NF_FOLIO = BenFxt.NF_FOLIO,
                    T24_SHORT_NAME = BenFxt.T24_SHORT_NAME,
                    T24_DEBIT_ACCT_NO = BenFxt.T24_DEBIT_ACCT_NO,
                    T24_DEBIT_AMOUNT = BenFxt.T24_DEBIT_AMOUNT,
                    NF_FECHA_EJECUCION = BenFxt.NF_FECHA_EJECUCION,
                    NF_FECHA_AUTORIZA = BenFxt.NF_FECHA_AUTORIZA,
                    NF_BANCO_BENEF = BenFxt.NF_BANCO_BENEF,
                    T24_TRANSACTION_ID = BenFxt.T24_TRANSACTION_ID,
                    T24_BEN_CUST_NAME = BenFxt.T24_BEN_CUST_NAME,
                    T24_DEBIT_CURRENCY = BenFxt.T24_DEBIT_CURRENCY,
                    SATM02COD = BenFxt.SATM02COD,
                    ID_FORMATO_PLANTILLA = BenFxt.ID_FORMATO_PLANTILLA,
                    T24_DEBIT_THEIR_REF = BenFxt.T24_DEBIT_THEIR_REF,
                    T24_BEN_ACCT_NO = BenFxt.T24_BEN_ACCT_NO,
                    T24_CUSTOMER_CODE = BenFxt.T24_CUSTOMER_CODE,
                    T24_CONCEPT = string.Empty,
                    T24_CURRENCY = string.Empty,
                    T24_REFERENCE = string.Empty,
                    T24_AMOUNT = null,
                    T24_NO_AGREEMENT = string.Empty,
                    NF_NUMERIC_REFERENCE = string.Empty,
                    NF_CLASIFICACION_OPERACION = string.Empty,
                    NF_RELACION_COMERCIAL = string.Empty,
                    NF_FECHA_CARGO = null,
                    NF_FECHA_APLICACION = null,
                    NF_CUENTA_CORRESPONSAL = string.Empty,
                    NF_COUNTRY = string.Empty,
                    NF_CHARGE = string.Empty,
                    NF_MENSAJE = string.Empty
                    };

                    lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoFXT, lsMailTo, lsMailCC, DtoAvisos);
                    Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioFXTAsync|Error en procesamiento de folio:");
                }
                else
                {
                    lbResultado = true;
                }

                if (lbResultado)
                {
                    await _detallesTomi.ActualizaFolioBeneficiarioFxi(BenFxt.NF_FOLIO);
                    Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioFXTAsync | Se termina procesamiento de folio: '{BenFxt.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                }
                else
                {
                    Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioFXTAsync  | Error en procesamiento de folio: '{BenFxt.NF_FOLIO}'");
                }
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENNOMFXD : ProcesamientoBenNomFxd, IProcesamientoBenNomFxd, IENT_BENNOMFXD
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO _entAviso;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENNOMFXD(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesTomser = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso = entAviso;
        }
        public async Task ProcesarOperacionBeneficiarioNOMFXDAsync()
        {
            try
            {
            
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo NOMFXD...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioNOMFXDAsync|Procesando Solicitud de envio operaciones PaP_FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenNomFxdDTO> complementoBenNomFxd = await this.ObtencionDatosBenNomFxd();

                //Si no pudo obtener sus complementos...
                if (complementoBenNomFxd.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioNOMFXDAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoBenNomFxdDTO BenNomFxd in complementoBenNomFxd)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXDAsync|Preparando envio de email, folio: '{BenNomFxd.NF_FOLIO}'");
                    //Buscamos si el cliente tiene habilitado el evento de notificación '006 - NOMFXD'
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    bool resultado = false;

                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenNomFxd.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento6);


                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioNOMFXDAsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBFXDNOM(ConstantesServicio.tipoNOMFXD, lsMailCub, null, BenNomFxd);
                    }
                    else
                    {
                        resultado = true;
                    }

                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioNomFxd(BenNomFxd.NF_FOLIO);
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioNOMFXDAsync|Se termina procesamiento de folio: '{BenNomFxd.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioNOMFXDAsync|Error en procesamiento de folio: '{BenNomFxd.NF_FOLIO}'");
                    }
                }
            }catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioNOMFXDAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENPAPFXD : ProcesamientoBenPapFxd, IProcesamientoBenPapFxd, IENT_BENPAPFXD
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO _entAviso;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENPAPFXD(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso        = entAviso;
        }
        public async Task ProcesarOperacionBeneficiarioPAPFXDAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo FXD...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioPAPFXDAsync|Procesando Solicitud de envio operaciones PaP_FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenPapFxdDTO> complementoBenPapFxd = await this.ObtencionDatosBenPapFxd();

                //Si no pudo obtener sus complementos...
                if (complementoBenPapFxd.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioPAPFXDAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoBenPapFxdDTO BenPapFxd in complementoBenPapFxd)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPFXDAsync|Preparando envio de email, folio: '{BenPapFxd.NF_FOLIO}'");
                    //Buscamos si el cliente tiene habilitado el evento de notificación '004 - PAPFXD'
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    bool resultado = false;


                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenPapFxd.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento4);

                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        var DtoCubFxd = new ComplementoCUBFXDDTO
                        {
                        NF_FOLIO = BenPapFxd.NF_FOLIO,
                        T24_SHORT_NAME = BenPapFxd.T24_SHORT_NAME,
                        T24_BEN_ACCT_NO = BenPapFxd.T24_BEN_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenPapFxd.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapFxd.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapFxd.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapFxd.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapFxd.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapFxd.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapFxd.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapFxd.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapFxd.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapFxd.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO = BenPapFxd.T24_DEBIT_ACCT_NO,
                        T24_CUSTOMER_CODE = BenPapFxd.T24_DEBIT_ACCT_NO,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null
                        };

                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPFXDAsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBFXD(ConstantesServicio.tipoPAPFXD, lsMailCub, null, DtoCubFxd);
                    }

                    if (resultado)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPFXDAsync|Se termina procesamiento de folio: '{BenPapFxd.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPFXDAsync|Error en procesamiento de folio: '{BenPapFxd.NF_FOLIO}'");
                    }


                    bool lbResultado = false;
                    string lsMailTo = null;
                    string lsMailCC = null;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoFXD, BenPapFxd.SATM02COD.ToString(), BenPapFxd.ID_FORMATO_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                        lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                    }

                    if (lsMailTo != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenPapFxd.NF_FOLIO,
                        T24_SHORT_NAME = BenPapFxd.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = BenPapFxd.T24_DEBIT_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenPapFxd.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapFxd.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapFxd.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapFxd.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapFxd.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapFxd.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapFxd.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapFxd.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapFxd.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapFxd.T24_DEBIT_THEIR_REF,
                        T24_BEN_ACCT_NO = BenPapFxd.T24_BEN_ACCT_NO,
                        T24_CUSTOMER_CODE = BenPapFxd.T24_CUSTOMER_CODE,
                        T24_CONCEPT = string.Empty,
                        T24_CURRENCY = string.Empty,
                        T24_REFERENCE = string.Empty,
                        T24_AMOUNT = null,
                        T24_NO_AGREEMENT = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoFXD, lsMailTo, lsMailCC, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioPAPFXDAsync|Error en procesamiento de folio:");
                    }
                    else
                    {
                        lbResultado = true;
                    }

                    if (lbResultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioPapFxd(BenPapFxd.NF_FOLIO);
                        Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioPAPFXDAsync | Se termina procesamiento de folio: '{BenPapFxd.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPFXDAsync  | Error en procesamiento de folio: '{BenPapFxd.NF_FOLIO}'");
                    }

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPFXDAsync|Se presenta exception:'{ex.ToString()}'");
            }

        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENPAPFXI : ProcesamientoBenPapFxi, IProcesamientoBenPapFxi, IENT_BENPAPFXI
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesTomi;
        IDetallesTomser   _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO        _entAviso;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENPAPFXI(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso        = entAviso;
        }
        public async Task ProcesarOperacionBeneficiarioPAPFXIAsync()
        {
            //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo FXD...
            Config.EscribirLog("INFO|Procesa_Aviso_Beneficiarios_PaP_FXI|Procesando Solicitud de envio operaciones PaP_FXD");

            //Obtiene los CDA de los complementos de la operación...
            List<ComplementoBenPapFxiDTO> complementoBenPapFxi = await this.ObtencionDatosBenPapFxi();

            //Si no pudo obtener sus complementos...
            if (complementoBenPapFxi.Count is 0)
            { 
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioPAPFXIAsync | No se encontro información");
                return;
            }

            foreach (ComplementoBenPapFxiDTO BenPapFxi in complementoBenPapFxi)
            {
                Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPapFXIAsync|Preparando envio de email, folio: '{BenPapFxi.NF_FOLIO}'");
                //Buscamos si el cliente tiene habilitado el evento de notificación '006 - PAPFXI'
                bool lbAvisoCub = false;
                string lsMailCub = "";
                bool resultado = false;

                List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenPapFxi.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento6);

                foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                {
                    lbAvisoCub = true;
                    if (lsMailCub is null)
                    {
                        lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }
                    else
                    {
                        lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }

                    var DtoCubFxi = new ComplementoBenPapFxiDTO
                    {
                        NF_FOLIO = BenPapFxi.NF_FOLIO,
                        T24_SHORT_NAME = BenPapFxi.T24_SHORT_NAME,
                        NF_CUENTA_CORRESPONSAL = BenPapFxi.NF_CUENTA_CORRESPONSAL,
                        T24_DEBIT_AMOUNT = BenPapFxi.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapFxi.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapFxi.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapFxi.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapFxi.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapFxi.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapFxi.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapFxi.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapFxi.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapFxi.T24_DEBIT_THEIR_REF,
                        NF_COUNTRY = BenPapFxi.NF_COUNTRY,
                        NF_CHARGE = BenPapFxi.NF_CHARGE,
                        NF_MENSAJE = BenPapFxi.NF_MENSAJE,
                        T24_DEBIT_ACCT_NO = BenPapFxi.T24_DEBIT_ACCT_NO,
                        T24_CUSTOMER_CODE = BenPapFxi.T24_CUSTOMER_CODE
                    };

                    if (lbAvisoCub)
                    {
                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPFXIAsync|Enviando información de la operación PAPFXI al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBFXI(ConstantesServicio.tipoFXI, lsMailCub, null, DtoCubFxi);
                    }

                    if (resultado)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPFXIAsync|Se termina procesamiento de folio: '{BenPapFxi.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPFXIAsync|Error en procesamiento de folio: '{BenPapFxi.NF_FOLIO}'");
                    }

                    bool lbResultado = false;
                    string lsMailTo = null;
                    string lsMailCC = null;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoFXI, BenPapFxi.SATM02COD.ToString(), BenPapFxi.ID_FORMATO_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                        lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                    }

                    if (lsMailTo != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenPapFxi.NF_FOLIO,
                        T24_SHORT_NAME = BenPapFxi.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = BenPapFxi.T24_DEBIT_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenPapFxi.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapFxi.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapFxi.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapFxi.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapFxi.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapFxi.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapFxi.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapFxi.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapFxi.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapFxi.T24_DEBIT_THEIR_REF,
                        T24_BEN_ACCT_NO = string.Empty,
                        T24_CUSTOMER_CODE = BenPapFxi.T24_CUSTOMER_CODE,
                        T24_CONCEPT = string.Empty,
                        T24_CURRENCY = string.Empty,
                        T24_REFERENCE = string.Empty,
                        T24_AMOUNT = null,
                        T24_NO_AGREEMENT = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                        NF_CUENTA_CORRESPONSAL = BenPapFxi.NF_CUENTA_CORRESPONSAL,
                        NF_COUNTRY = BenPapFxi.NF_COUNTRY,
                        NF_CHARGE = BenPapFxi.NF_CHARGE,
                        NF_MENSAJE = BenPapFxi.NF_MENSAJE
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoFXI, lsMailTo, lsMailCC, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioPAPFXIAsync|Error en procesamiento de folio:");
                    }
                    else
                    {
                        lbResultado = true;
                    }


                    if (lbResultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioPapFxi(BenPapFxi.NF_FOLIO);
                        Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioPAPFXIAsync | Se termina procesamiento de folio: '{BenPapFxi.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPFXIAsync  | Error en procesamiento de folio: '{BenPapFxi.NF_FOLIO}'");
                    }

                }

            }

        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENPAPFXT : ProcesamientoBenPapFxt, IProcesamientoBenPapFxt, IENT_BENPAPFXT
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesTomi;
        IDetallesTomser   _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO        _entAviso;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENPAPFXT(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso        = entAviso;
        }

        public async Task ProcesarOperacionBeneficiarioPAPFXTAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo PAPFXT...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioPAPFXTAsync|Procesando Solicitud de envio operaciones PaP_FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenPapFxtDTO> complementoBenPapFxt = await this.ObtencionDatosBenPapFxt();

                //Si no pudo obtener sus complementos...
                if (complementoBenPapFxt.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioPAPFXTAsync | No se encontro información");
                    return;
                }


                foreach (ComplementoBenPapFxtDTO BenPapFxt in complementoBenPapFxt)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPFXTAsync|Preparando envio de email, folio: '{BenPapFxt.NF_FOLIO}'");
                    //Buscamos si el cliente tiene habilitado el evento de notificación '005 - PAPFXT'
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    bool resultado = false;

                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenPapFxt.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento5);

                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        var DtoCubFxd = new ComplementoCUBFXDDTO
                        {
                            NF_FOLIO = BenPapFxt.NF_FOLIO,
                            T24_SHORT_NAME = BenPapFxt.T24_SHORT_NAME,
                            T24_BEN_ACCT_NO = BenPapFxt.T24_BEN_ACCT_NO,
                            T24_DEBIT_AMOUNT = BenPapFxt.T24_DEBIT_AMOUNT,
                            NF_FECHA_EJECUCION = BenPapFxt.NF_FECHA_EJECUCION,
                            NF_FECHA_AUTORIZA = BenPapFxt.NF_FECHA_AUTORIZA,
                            NF_BANCO_BENEF = BenPapFxt.NF_BANCO_BENEF,
                            T24_TRANSACTION_ID = BenPapFxt.T24_TRANSACTION_ID,
                            T24_BEN_CUST_NAME = BenPapFxt.T24_BEN_CUST_NAME,
                            T24_DEBIT_CURRENCY = BenPapFxt.T24_DEBIT_CURRENCY,
                            SATM02COD = BenPapFxt.SATM02COD,
                            ID_FORMATO_PLANTILLA = BenPapFxt.ID_FORMATO_PLANTILLA,
                            T24_DEBIT_THEIR_REF = BenPapFxt.T24_DEBIT_THEIR_REF,
                            T24_DEBIT_ACCT_NO = BenPapFxt.T24_DEBIT_ACCT_NO,
                            T24_CUSTOMER_CODE = BenPapFxt.T24_DEBIT_ACCT_NO,
                            NF_NUMERIC_REFERENCE = string.Empty,
                            NF_CLASIFICACION_OPERACION = string.Empty,
                            NF_RELACION_COMERCIAL = string.Empty,
                            NF_FECHA_CARGO = null,
                            NF_FECHA_APLICACION = null
                        };

                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPFXTAsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBFXD(ConstantesServicio.tipoPAPFXT, lsMailCub, null, DtoCubFxd);
                    }

                    if (resultado)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPFXTAsync|Se termina procesamiento de folio: '{BenPapFxt.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPFXTAsync|Error en procesamiento de folio: '{BenPapFxt.NF_FOLIO}'");
                    }

                    bool lbResultado = false;
                    string lsMailTo = null;
                    string lsMailCC = null;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoFXT, BenPapFxt.SATM02COD.ToString(), BenPapFxt.ID_FORMATO_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                        lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                    }

                    if (lsMailTo != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenPapFxt.NF_FOLIO,
                        T24_SHORT_NAME = BenPapFxt.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = BenPapFxt.T24_DEBIT_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenPapFxt.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapFxt.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapFxt.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapFxt.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapFxt.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapFxt.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapFxt.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapFxt.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapFxt.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapFxt.T24_DEBIT_THEIR_REF,
                        T24_BEN_ACCT_NO = BenPapFxt.T24_BEN_ACCT_NO,
                        T24_CUSTOMER_CODE = BenPapFxt.T24_CUSTOMER_CODE,
                        T24_CONCEPT = string.Empty,
                        T24_CURRENCY = string.Empty,
                        T24_REFERENCE = string.Empty,
                        T24_AMOUNT = null,
                        T24_NO_AGREEMENT = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoFXT, lsMailTo, lsMailCC, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioPAPFXTAsync|Error en procesamiento de folio:");
                    }
                    else
                    {
                        lbResultado = true;
                    }

                    if (lbResultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioPapFxt(BenPapFxt.NF_FOLIO);
                        Config.EscribirLog($"INFO |ProcesarOperacionBeneficiarioPAPFXTAsync | Se termina procesamiento de folio: '{BenPapFxt.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPFXTAsync  | Error en procesamiento de folio: '{BenPapFxt.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPFXTAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENPAPSPID : ProcesamientoBenPapSpid, IProcesamientoBenPapSpid, IENT_BENPAPSPID
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO _entAviso;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENPAPSPID(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso        = entAviso;
        }
        public async Task ProcesarOperacionBeneficiarioPAPSPIDAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo SPID...
                Config.EscribirLog("INFO|Procesa_Aviso_Beneficiarios_PAPSPID|Procesando Solicitud de envio operaciones SPID");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenPapSpidDTO> complementoBenPapSpid = await this.ObtencionDatosBenPapSpid();

                //Si no pudo obtener sus complementos...
                if (complementoBenPapSpid.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioPAPSPIDAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoBenPapSpidDTO BenPapSpid in complementoBenPapSpid)
                {

                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPSPIDAsync|Preparando envio de email, folio: '{BenPapSpid.NF_FOLIO}'");
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    bool resultado = false;

                    //Buscamos si el cliente tiene habilitado el evento de notificación '011 - PAPSPID'
                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenPapSpid.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento11);

                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        var DtoCubFxd = new ComplementoCUBFXDDTO
                        {
                        NF_FOLIO = BenPapSpid.NF_FOLIO,
                        T24_SHORT_NAME = BenPapSpid.T24_SHORT_NAME,
                        T24_BEN_ACCT_NO = BenPapSpid.T24_BEN_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenPapSpid.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapSpid.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapSpid.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapSpid.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapSpid.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapSpid.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapSpid.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapSpid.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapSpid.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapSpid.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO = BenPapSpid.T24_DEBIT_ACCT_NO,
                        T24_CUSTOMER_CODE = BenPapSpid.T24_DEBIT_ACCT_NO,
                        NF_NUMERIC_REFERENCE = BenPapSpid.NF_NUMERIC_REFERENCE,
                        NF_CLASIFICACION_OPERACION = BenPapSpid.NF_CLASIFICACION_OPERACION,
                        NF_RELACION_COMERCIAL = BenPapSpid.NF_RELACION_COMERCIAL,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null
                        };

                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPSPIDAsync|Enviando información de la operación SPID al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBFXD(ConstantesServicio.tipoSPID, lsMailCub, null, DtoCubFxd);
                    }

                    if (resultado)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPSPIDAsync|Se termina procesamiento de folio: '{BenPapSpid.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPSPIDAsync|Error en procesamiento de folio: '{BenPapSpid.NF_FOLIO}'");
                    }

                    bool lbResultado = false;
                    string lsMailTo = null;
                    string lsMailCC = null;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoSPD, BenPapSpid.SATM02COD.ToString(), BenPapSpid.ID_FORMATO_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                        lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                    }


                    if (lsMailTo != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenPapSpid.NF_FOLIO,
                        T24_SHORT_NAME = BenPapSpid.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = BenPapSpid.T24_DEBIT_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenPapSpid.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapSpid.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapSpid.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapSpid.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapSpid.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapSpid.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapSpid.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapSpid.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapSpid.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapSpid.T24_DEBIT_THEIR_REF,
                        T24_BEN_ACCT_NO = BenPapSpid.T24_BEN_ACCT_NO,
                        T24_CUSTOMER_CODE = BenPapSpid.T24_CUSTOMER_CODE,
                        T24_CONCEPT = string.Empty,
                        T24_CURRENCY = string.Empty,
                        T24_REFERENCE = string.Empty,
                        T24_AMOUNT = null,
                        T24_NO_AGREEMENT = string.Empty,
                        NF_NUMERIC_REFERENCE = BenPapSpid.NF_NUMERIC_REFERENCE,
                        NF_CLASIFICACION_OPERACION = BenPapSpid.NF_CLASIFICACION_OPERACION,
                        NF_RELACION_COMERCIAL = BenPapSpid.NF_RELACION_COMERCIAL,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoSPID, lsMailTo, lsMailCC, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioPAPSPIDAsync|Error en procesamiento de folio:");
                    }
                    else
                    {
                        lbResultado = true;
                    }

                    if (lbResultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioPapSpid(BenPapSpid.NF_FOLIO);
                        Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioPAPSPIDAsync | Se termina procesamiento de folio: '{BenPapSpid.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPSPIDAsync  | Error en procesamiento de folio: '{BenPapSpid.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPSPIDAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENPAPTEF : ProcesamientoBenPapTef, IProcesamientoBenPapTef, IENT_BENPAPTEF
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO _entAviso;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENPAPTEF(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso = entAviso;
        }
        public async Task ProcesarOperacionBeneficiarioPAPTEFAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo PAPTEF...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioPAPTEFAsync|Procesando Solicitud de envio operaciones SPID");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenPapTefDTO> complementoBenPapTef = await this.ObtencionDatosBenPapTef();

                //Si no pudo obtener sus complementos...
                if (complementoBenPapTef.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioPAPTEFAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoBenPapTefDTO BenPapTef in complementoBenPapTef)
                {

                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPTEFAsync|Preparando envio de email, folio: '{BenPapTef.NF_FOLIO}'");
                    //Buscamos si el cliente tiene habilitado el evento de notificación '012 - PAPTEF'
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    bool resultado = false;

                    //Buscamos si el cliente tiene habilitado el evento de notificación '011 - FXD'
                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenPapTef.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento12);

                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        var DtoCubFxd = new ComplementoCUBFXDDTO
                        {
                        NF_FOLIO = BenPapTef.NF_FOLIO,
                        T24_SHORT_NAME = BenPapTef.T24_SHORT_NAME,
                        T24_BEN_ACCT_NO = BenPapTef.T24_BEN_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenPapTef.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapTef.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapTef.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapTef.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapTef.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapTef.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapTef.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapTef.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapTef.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapTef.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO = BenPapTef.T24_DEBIT_ACCT_NO,
                        T24_CUSTOMER_CODE = BenPapTef.T24_DEBIT_ACCT_NO,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = BenPapTef.NF_FECHA_CARGO,
                        NF_FECHA_APLICACION = BenPapTef.NF_FECHA_APLICACION
                        };

                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPTEFAsync|Enviando información de la operación SPID al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBFXD(ConstantesServicio.tipoTEF, lsMailCub, null, DtoCubFxd);
                    }

                    if (resultado)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioPAPTEFAsync|Se termina procesamiento de folio: '{BenPapTef.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPTEFAsync|Error en procesamiento de folio: '{BenPapTef.NF_FOLIO}'");
                    }

                    bool lbResultado = false;
                    string lsMailTo = null;
                    string lsMailCC = null;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoFXD, BenPapTef.SATM02COD.ToString(), BenPapTef.ID_FORMATO_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                        lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                    }

                    if (lsMailTo != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenPapTef.NF_FOLIO,
                        T24_SHORT_NAME = BenPapTef.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = BenPapTef.T24_DEBIT_ACCT_NO,
                        T24_DEBIT_AMOUNT = BenPapTef.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = BenPapTef.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenPapTef.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = BenPapTef.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = BenPapTef.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenPapTef.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = BenPapTef.T24_DEBIT_CURRENCY,
                        SATM02COD = BenPapTef.SATM02COD,
                        ID_FORMATO_PLANTILLA = BenPapTef.ID_FORMATO_PLANTILLA,
                        T24_DEBIT_THEIR_REF = BenPapTef.T24_DEBIT_THEIR_REF,
                        T24_BEN_ACCT_NO = BenPapTef.T24_BEN_ACCT_NO,
                        T24_CUSTOMER_CODE = BenPapTef.T24_CUSTOMER_CODE,
                        T24_CONCEPT = string.Empty,
                        T24_CURRENCY = string.Empty,
                        T24_REFERENCE = string.Empty,
                        T24_AMOUNT = null,
                        T24_NO_AGREEMENT = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = BenPapTef.NF_FECHA_CARGO,
                        NF_FECHA_APLICACION = BenPapTef.NF_FECHA_APLICACION,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty,
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoFXD, lsMailTo, lsMailCC, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioPAPTEFAsync|Error en procesamiento de folio:");
                    }
                    else
                    {
                        lbResultado = true;
                    }

                    if (lbResultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioPapTef(BenPapTef.NF_FOLIO);
                        Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioPAPTEFAsync | Se termina procesamiento de folio: '{BenPapTef.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR | ProcesarOperacionBeneficiarioPAPTEFAsync  | Error en procesamiento de folio: '{BenPapTef.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioPAPTEFAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net.Security;
using System.Threading.Tasks;


namespace AplicacionEmail.Servicios
{
    public class ENT_BENSER : ProcesamientoBenSer, IProcesamientoBenSer, IENT_BENSER
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesBenSer;
        IDetallesTomser   _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO        _entAviso;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENSER(Configuracion configuracion, IDetallesTomi detallesBenSer, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            :base(configuracion, detallesBenSer, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesBenSer  = detallesBenSer;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso        = entAviso;
        }

        public async Task ProcesarOperacionBeneficiarioSERAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo SER...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioSERAsync|Buscando información de los beneficiarios SER");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenSer> complementoBenSer = await this.ObtencionDatosBenSer();

                //Si no pudo obtener sus complementos...
                if (complementoBenSer.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioSERAsync | No se encontro información");
                    return;
                }


                foreach (ComplementoBenSer BenSer in complementoBenSer)
                {

                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioSERAsync|Preparando envio de email, folio: '{BenSer.nf_folio}'");
                    //Buscamos si el cliente tiene habilitado el evento de notificación '001 - SER (Lo mismo que FXD)
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    string lsMailOrde = null;
                    bool resultado = false;

                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenSer.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento1);

                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioSERAsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                        resultado = await _entNotificacion.EnviarNotificacionesCUBSERTAX(ConstantesServicio.nombreTranSER, lsMailCub, null, BenSer);
                    }

                    if (resultado && lsMailCub != "")
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioSERAsync|Se termina procesamiento de folio: '{BenSer.nf_folio + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioSERAsync|Error en procesamiento de folio: '{BenSer.nf_folio}'");
                    }

                    bool lbResultado = false;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoSER, BenSer.SATM02COD.ToString(), BenSer.ID_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailOrde = AvisoSer.ID_EMAIL_ORDENANTE;
                    }

                    if (lsMailOrde != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenSer.nf_folio,
                        T24_SHORT_NAME = BenSer.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = string.Empty,
                        T24_DEBIT_AMOUNT = null,
                        NF_FECHA_EJECUCION = BenSer.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenSer.NF_FECHA_AUTORIZA.ToString(),
                        NF_BANCO_BENEF = string.Empty,
                        T24_TRANSACTION_ID = BenSer.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenSer.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = string.Empty,
                        SATM02COD = BenSer.SATM02COD,
                        ID_FORMATO_PLANTILLA = null,
                        T24_DEBIT_THEIR_REF = string.Empty,
                        T24_BEN_ACCT_NO = string.Empty,
                        T24_CUSTOMER_CODE = BenSer.T24_CUSTOMER_CODE,
                        T24_CONCEPT = BenSer.T24_CONCEPT,
                        T24_CURRENCY = BenSer.T24_CURRENCY,
                        T24_REFERENCE = BenSer.T24_REFERENCE,
                        T24_AMOUNT = BenSer.T24_AMOUNT,
                        T24_NO_AGREEMENT = BenSer.T24_NO_AGREEMENT,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty,
                        T24_ACCT_NO = BenSer.T24_ACCT_NO,
                        ID_PLANTILLA = BenSer.ID_PLANTILLA
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoSER, lsMailOrde, null, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioSERAsync|Error en procesamiento de folio:");
                        lsMailOrde = null;
                    }
                    else
                    {
                        lbResultado = true;
                    }

                    if (lbResultado)
                    {
                        await _detallesBenSer.ActualizaFolioBeneficiario(BenSer.nf_folio);
                        Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioSERAsync | Se termina procesamiento de folio: '{BenSer.nf_folio + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioSERAsync  | Error en procesamiento de folio: '{BenSer.nf_folio}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioSERAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENSPID : ProcesamientoBenSpid, IProcesamientoBenSpid, IENT_BENSPID
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO _entAviso;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENSPID(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesTomser = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso = entAviso;
        }
        public async Task ProcesarOperacionBeneficiarioSPIDAsync()
        {
            //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo SPID...
            Config.EscribirLog("INFO|Procesa_Aviso_Beneficiarios_NOM_FXD|Procesando Solicitud de envio operaciones SPID");

            //Obtiene los CDA de los complementos de la operación...
            List<ComplementoBenSpidDTO> complementoBenSpid = await this.ObtencionDatosBenSpid();

            //Si no pudo obtener sus complementos...
            if (complementoBenSpid.Count is 0)
            {
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioSPIDAsync | No se encontro información");
                return;
            }

            foreach (ComplementoBenSpidDTO BenSpid in complementoBenSpid)
            {

                Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioFXDAsync|Preparando envio de email, folio: '{BenSpid.NF_FOLIO}'");
                //Buscamos si el cliente tiene habilitado el evento de notificación '006 - NOMFXD'
                bool lbAvisoCub = false;
                string lsMailCub = "";
                bool resultado = false;

                //Buscamos si el cliente tiene habilitado el evento de notificación '011 - SPID'
                List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenSpid.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento11);

                foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                {
                    lbAvisoCub = true;
                    if (lsMailCub is null)
                    {
                        lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }
                    else
                    {
                        lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }
                }

                if (lbAvisoCub)
                {
                    var DtoCubFxd = new ComplementoCUBFXDDTO
                    {
                    NF_FOLIO = BenSpid.NF_FOLIO,
                    T24_SHORT_NAME = BenSpid.T24_SHORT_NAME,
                    T24_BEN_ACCT_NO = BenSpid.T24_BEN_ACCT_NO,
                    T24_DEBIT_AMOUNT = BenSpid.T24_DEBIT_AMOUNT,
                    NF_FECHA_EJECUCION = BenSpid.NF_FECHA_EJECUCION,
                    NF_FECHA_AUTORIZA = BenSpid.NF_FECHA_AUTORIZA,
                    NF_BANCO_BENEF = BenSpid.NF_BANCO_BENEF,
                    T24_TRANSACTION_ID = BenSpid.T24_TRANSACTION_ID,
                    T24_BEN_CUST_NAME = BenSpid.T24_BEN_CUST_NAME,
                    T24_DEBIT_CURRENCY = BenSpid.T24_DEBIT_CURRENCY,
                    SATM02COD = BenSpid.SATM02COD,
                    ID_FORMATO_PLANTILLA = BenSpid.ID_FORMATO_PLANTILLA,
                    T24_DEBIT_THEIR_REF = BenSpid.T24_DEBIT_THEIR_REF,
                    T24_DEBIT_ACCT_NO = BenSpid.T24_DEBIT_ACCT_NO,
                    T24_CUSTOMER_CODE = BenSpid.T24_DEBIT_ACCT_NO,
                    NF_NUMERIC_REFERENCE = BenSpid.NF_NUMERIC_REFERENCE,
                    NF_CLASIFICACION_OPERACION = BenSpid.NF_CLASIFICACION_OPERACION,
                    NF_RELACION_COMERCIAL = BenSpid.NF_RELACION_COMERCIAL,
                    NF_FECHA_CARGO = null,
                    NF_FECHA_APLICACION = null
                    };

                    //Enviar configuración del correo
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioSPIDAsync|Enviando información de la operación SPID al cliente. '{lsMailCub}'");
                    resultado = await _entNotificacion.EnviarNotificacionesCUBFXD(ConstantesServicio.tipoSPID, lsMailCub, null, DtoCubFxd);
                }

                if (resultado)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioSPIDAsync|Se termina procesamiento de folio: '{BenSpid.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                }
                else
                {
                    Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioSPIDAsync|Error en procesamiento de folio: '{BenSpid.NF_FOLIO}'");
                }

                bool lbResultado = false;
                string lsMailTo = null;
                string lsMailCC = null;

                List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoSPD, BenSpid.SATM02COD.ToString(), BenSpid.ID_FORMATO_PLANTILLA);

                foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                {
                    lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                    lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                }

                if (lsMailTo != null)
                {
                    var DtoAvisos = new ComplementoAvisosDTO
                    {
                    NF_FOLIO = BenSpid.NF_FOLIO,
                    T24_SHORT_NAME = BenSpid.T24_SHORT_NAME,
                    T24_DEBIT_ACCT_NO = BenSpid.T24_DEBIT_ACCT_NO,
                    T24_DEBIT_AMOUNT = BenSpid.T24_DEBIT_AMOUNT,
                    NF_FECHA_EJECUCION = BenSpid.NF_FECHA_EJECUCION,
                    NF_FECHA_AUTORIZA = BenSpid.NF_FECHA_AUTORIZA,
                    NF_BANCO_BENEF = BenSpid.NF_BANCO_BENEF,
                    T24_TRANSACTION_ID = BenSpid.T24_TRANSACTION_ID,
                    T24_BEN_CUST_NAME = BenSpid.T24_BEN_CUST_NAME,
                    T24_DEBIT_CURRENCY = BenSpid.T24_DEBIT_CURRENCY,
                    SATM02COD = BenSpid.SATM02COD,
                    ID_FORMATO_PLANTILLA = BenSpid.ID_FORMATO_PLANTILLA,
                    T24_DEBIT_THEIR_REF = BenSpid.T24_DEBIT_THEIR_REF,
                    T24_BEN_ACCT_NO = BenSpid.T24_BEN_ACCT_NO,
                    T24_CUSTOMER_CODE = BenSpid.T24_CUSTOMER_CODE,
                    T24_CONCEPT = string.Empty,
                    T24_CURRENCY = string.Empty,
                    T24_REFERENCE = string.Empty,
                    T24_AMOUNT = null,
                    T24_NO_AGREEMENT = string.Empty,
                    NF_NUMERIC_REFERENCE = BenSpid.NF_NUMERIC_REFERENCE,
                    NF_CLASIFICACION_OPERACION = BenSpid.NF_CLASIFICACION_OPERACION,
                    NF_RELACION_COMERCIAL = BenSpid.NF_RELACION_COMERCIAL,
                    NF_FECHA_CARGO = null,
                    NF_FECHA_APLICACION = null,
                    NF_CUENTA_CORRESPONSAL = string.Empty,
                    NF_COUNTRY = string.Empty,
                    NF_CHARGE = string.Empty,
                    NF_MENSAJE = string.Empty
                    };

                    lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoSPID, lsMailTo, lsMailCC, DtoAvisos);
                    Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioSPIDAsync|Error en procesamiento de folio:");
                }
                else
                {
                    lbResultado = true;
                }


                if (lbResultado)
                {
                    await _detallesTomi.ActualizaFolioBeneficiarioSpid(BenSpid.NF_FOLIO);
                    Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioSPIDAsync | Se termina procesamiento de folio: '{BenSpid.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                }
                else
                {
                    Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioSPIDAsync  | Error en procesamiento de folio: '{BenSpid.NF_FOLIO}'");
                }


            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using static MUFG_T24.Modelos.CalypsoMwSolicitud;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENTAX : ProcesamientoBenTax, IProcesamientoBenTax, IENT_BENTAX
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesTomi;
        IDetallesTomser   _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO        _entAviso;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENTAX(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso        = entAviso;
        }

        public async Task ProcesarOperacionBeneficiarioTAXsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo TAX...
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioTAXsync|Buscando información de los beneficiarios TAX");
                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoBenTaxDTO> complementoBenTax = await this.ObtencionDatosBenTax();

                //Si no pudo obtener sus complementos...
                if (complementoBenTax.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioTAXsync | No se encontro información");
                    return;
                }

                foreach (ComplementoBenTaxDTO BenTax in complementoBenTax)
                {
                    Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioTAXsync | Preparando envio de email, folio: '{BenTax.nf_folio}");

                    //Buscamos si el cliente tiene habilitado el evento de notificación '001 - TAX(Lo mismo que FXD)'
                    bool lbAvisoCub = false;
                    string lsMailCub = "";
                    bool resultado = false;
                    string lsMailOrde = null;

                    //Se obtiene la información de los contactos filtrando por numero de cliente.
                    List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenTax.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento1);

                    foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                    {
                        lbAvisoCub = true;
                        if (lsMailCub is null)
                        {
                            lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                        else
                        {
                            lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                        }
                    }

                    if (lbAvisoCub)
                    {
                        //Enviar configuración del correo
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioTAXsync|Enviando información de la operación SER al cliente. '{lsMailCub}'");
                        //resultado = _entNotificacion.EnviarNotificacionesCUBSERTAX(ConstantesServicio.nombreTranSER, lsMailCub, null, BenTax);
                    }

                    if (resultado && lsMailCub != "")
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioTAXsync|Se termina procesamiento de folio: '{BenTax.nf_folio + ", conAvisoCUB=" + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioTAXsync|Error en procesamiento de folio: '{BenTax.nf_folio}'");
                    }

                    bool lbResultado = false;

                    List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoSER, BenTax.SATM02COD.ToString(), BenTax.ID_PLANTILLA);

                    foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                    {
                        lsMailOrde = AvisoSer.ID_EMAIL_ORDENANTE;
                    }

                    if (lsMailOrde != null)
                    {
                        var DtoAvisos = new ComplementoAvisosDTO
                        {
                        NF_FOLIO = BenTax.nf_folio,
                        T24_SHORT_NAME = BenTax.T24_SHORT_NAME,
                        T24_DEBIT_ACCT_NO = string.Empty,
                        T24_DEBIT_AMOUNT = null,
                        NF_FECHA_EJECUCION = BenTax.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = BenTax.NF_FECHA_AUTORIZA.ToString(),
                        NF_BANCO_BENEF = string.Empty,
                        T24_TRANSACTION_ID = BenTax.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = BenTax.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = string.Empty,
                        SATM02COD = BenTax.SATM02COD,
                        ID_FORMATO_PLANTILLA = null,
                        T24_DEBIT_THEIR_REF = string.Empty,
                        T24_BEN_ACCT_NO = string.Empty,
                        T24_CUSTOMER_CODE = BenTax.T24_CUSTOMER_CODE,
                        T24_CONCEPT = BenTax.T24_CONCEPT,
                        T24_CURRENCY = BenTax.T24_CURRENCY,
                        T24_REFERENCE = BenTax.T24_REFERENCE,
                        T24_AMOUNT = BenTax.T24_AMOUNT,
                        T24_NO_AGREEMENT = BenTax.T24_NO_AGREEMENT,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty,
                        T24_ACCT_NO = BenTax.T24_ACCT_NO,
                        ID_PLANTILLA = BenTax.ID_PLANTILLA
                        };

                        lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoTAX, lsMailOrde, null, DtoAvisos);
                        Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioTAXsync|Error en procesamiento de folio:");
                        lsMailOrde = null;
                    }
                    else
                    {
                        lbResultado = true;
                    }

                    if (lbResultado)
                    {
                        await _detallesTomi.ActualizaFolioBeneficiarioTax(BenTax.nf_folio);
                        Config.EscribirLog($"INFO |ProcesarOperacionBeneficiarioTAXsync| Se termina procesamiento de folio: '{BenTax.nf_folio + ", conAviso = " + lbAvisoCub}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioTAXsync| Error en procesamiento de folio: '{BenTax.nf_folio}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioTAXsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_BENTEF : ProcesamientoBenTef, IProcesamientoBenTef, IENT_BENTEF
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_NOTIFICACION _entNotificacion;
        IENT_AVISO _entAviso;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_BENTEF(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_NOTIFICACION entNotificacion, IENT_AVISO entAviso)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesTomser = detallesTomser;
            _entNotificacion = entNotificacion;
            _entAviso = entAviso;
        }
        public async Task ProcesarOperacionBeneficiarioTEFAsync()
        {
            //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo SPID...
            Config.EscribirLog("INFO|Procesa_Aviso_Beneficiarios_TEF|Procesando Solicitud de envio operaciones SPID");

            //Obtiene los CDA de los complementos de la operación...
            List<ComplementoBenTefDTO> complementoBenTef = await this.ObtencionDatosBenTef();

            //Si no pudo obtener sus complementos...
            if (complementoBenTef.Count is 0)
            {
                Config.EscribirLog("INFO|ProcesarOperacionBeneficiarioTEFAsync | No se encontro información");
                return;
            }

            foreach (ComplementoBenTefDTO BenTef in complementoBenTef)
            {

                Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioTEFAsync|Preparando envio de email, folio: '{BenTef.NF_FOLIO}'");
                //Buscamos si el cliente tiene habilitado el evento de notificación '006 - NOMFXD'
                bool lbAvisoCub = false;
                string lsMailCub = "";
                bool resultado = false;

                //Buscamos si el cliente tiene habilitado el evento de notificación '011 - FXD'
                List<ComplementoBenSerSec> complementoBenSerSec = await this.ObtencionDatosBenSerContacto((decimal)BaseDatosHelper.ConvierteADecimal(BenTef.T24_CUSTOMER_CODE), ConstantesServicio.claveEvento12);

                foreach (ComplementoBenSerSec BenSerCont in complementoBenSerSec)
                {
                    lbAvisoCub = true;
                    if (lsMailCub is null)
                    {
                        lsMailCub = BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }
                    else
                    {
                        lsMailCub = lsMailCub + (char)59 + BenSerCont.DF_CONTACTOSCUB_EMAIL;
                    }
                }

                if (lbAvisoCub)
                {
                    var DtoCubFxd = new ComplementoCUBFXDDTO
                    {
                    NF_FOLIO = BenTef.NF_FOLIO,
                    T24_SHORT_NAME = BenTef.T24_SHORT_NAME,
                    T24_BEN_ACCT_NO = BenTef.T24_BEN_ACCT_NO,
                    T24_DEBIT_AMOUNT = BenTef.T24_DEBIT_AMOUNT,
                    NF_FECHA_EJECUCION = BenTef.NF_FECHA_EJECUCION,
                    NF_FECHA_AUTORIZA = BenTef.NF_FECHA_AUTORIZA,
                    NF_BANCO_BENEF = BenTef.NF_BANCO_BENEF,
                    T24_TRANSACTION_ID = BenTef.T24_TRANSACTION_ID,
                    T24_BEN_CUST_NAME = BenTef.T24_BEN_CUST_NAME,
                    T24_DEBIT_CURRENCY = BenTef.T24_DEBIT_CURRENCY,
                    SATM02COD = BenTef.SATM02COD,
                    ID_FORMATO_PLANTILLA = BenTef.ID_FORMATO_PLANTILLA,
                    T24_DEBIT_THEIR_REF = BenTef.T24_DEBIT_THEIR_REF,
                    T24_DEBIT_ACCT_NO = BenTef.T24_DEBIT_ACCT_NO,
                    T24_CUSTOMER_CODE = BenTef.T24_DEBIT_ACCT_NO,
                    NF_NUMERIC_REFERENCE = string.Empty,
                    NF_CLASIFICACION_OPERACION = string.Empty,
                    NF_RELACION_COMERCIAL = string.Empty,
                    NF_FECHA_CARGO = BenTef.NF_FECHA_CARGO,
                    NF_FECHA_APLICACION = BenTef.NF_FECHA_APLICACION
                    };

                    //Enviar configuración del correo
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioTEFAsync|Enviando información de la operación SPID al cliente. '{lsMailCub}'");
                    resultado = await _entNotificacion.EnviarNotificacionesCUBFXD(ConstantesServicio.tipoTEF, lsMailCub, null, DtoCubFxd);
                }

                if (resultado)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionBeneficiarioTEFAsync|Se termina procesamiento de folio: '{BenTef.NF_FOLIO + ", conAvisoCUB=" + lbAvisoCub}'");
                }
                else
                {
                    Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioTEFAsync|Error en procesamiento de folio: '{BenTef.NF_FOLIO}'");
                }

                bool lbResultado = false;
                string lsMailTo = null;
                string lsMailCC = null;

                List<ComplementoAvisoBeneficiario> complementoAvisoBenef = await this.ObtencionDatosEmailOrdenante(ConstantesServicio.tipoFXD, BenTef.SATM02COD.ToString(), BenTef.ID_FORMATO_PLANTILLA);

                foreach (ComplementoAvisoBeneficiario AvisoSer in complementoAvisoBenef)
                {
                    lsMailTo = AvisoSer.ID_EMAIL_ORDENANTE;
                    lsMailCC = AvisoSer.ID_EMAIL_BENEFICIARIO;
                }

                if (lsMailTo != null)
                {
                    var DtoAvisos = new ComplementoAvisosDTO
                    {
                    NF_FOLIO = BenTef.NF_FOLIO,
                    T24_SHORT_NAME = BenTef.T24_SHORT_NAME,
                    T24_DEBIT_ACCT_NO = BenTef.T24_DEBIT_ACCT_NO,
                    T24_DEBIT_AMOUNT = BenTef.T24_DEBIT_AMOUNT,
                    NF_FECHA_EJECUCION = BenTef.NF_FECHA_EJECUCION,
                    NF_FECHA_AUTORIZA = BenTef.NF_FECHA_AUTORIZA,
                    NF_BANCO_BENEF = BenTef.NF_BANCO_BENEF,
                    T24_TRANSACTION_ID = BenTef.T24_TRANSACTION_ID,
                    T24_BEN_CUST_NAME = BenTef.T24_BEN_CUST_NAME,
                    T24_DEBIT_CURRENCY = BenTef.T24_DEBIT_CURRENCY,
                    SATM02COD = BenTef.SATM02COD,
                    ID_FORMATO_PLANTILLA = BenTef.ID_FORMATO_PLANTILLA,
                    T24_DEBIT_THEIR_REF = BenTef.T24_DEBIT_THEIR_REF,
                    T24_BEN_ACCT_NO = BenTef.T24_BEN_ACCT_NO,
                    T24_CUSTOMER_CODE = BenTef.T24_CUSTOMER_CODE,
                    T24_CONCEPT = string.Empty,
                    T24_CURRENCY = string.Empty,
                    T24_REFERENCE = string.Empty,
                    T24_AMOUNT = null,
                    T24_NO_AGREEMENT = string.Empty,
                    NF_NUMERIC_REFERENCE = string.Empty,
                    NF_CLASIFICACION_OPERACION = string.Empty,
                    NF_RELACION_COMERCIAL = string.Empty,
                    NF_FECHA_CARGO = BenTef.NF_FECHA_CARGO,
                    NF_FECHA_APLICACION = BenTef.NF_FECHA_APLICACION,
                    NF_CUENTA_CORRESPONSAL = string.Empty,
                    NF_COUNTRY = string.Empty,
                    NF_CHARGE = string.Empty,
                    NF_MENSAJE = string.Empty
                    };

                    lbResultado = await _entAviso.EnviaAvisos(ConstantesServicio.tipoTEF, lsMailTo, lsMailCC, DtoAvisos);
                    Config.EscribirLog("ERROR|ProcesarOperacionBeneficiarioTEFAsync|Error en procesamiento de folio:");
                }
                else
                {
                    lbResultado = true;
                }

                if (lbResultado)
                {
                    await _detallesTomi.ActualizaFolioBeneficiarioTef(BenTef.NF_FOLIO);
                    Config.EscribirLog($"INFO | ProcesarOperacionBeneficiarioTEFAsync | Se termina procesamiento de folio: '{BenTef.NF_FOLIO + ", conAviso = " + lbAvisoCub}'");
                }
                else
                {
                    Config.EscribirLog($"ERROR|ProcesarOperacionBeneficiarioTEFAsync  | Error en procesamiento de folio: '{BenTef.NF_FOLIO}'");
                }


            }
        }
    }
}

using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_H2H : IENT_H2H
    {
        Configuracion    _configuracion;
        IENT_H2HFXD      _entH2hFxd;
        IENT_H2HFXI      _entH2hFxi;
        IENT_H2HFXT      _entH2hFxt;
        IENT_H2HSPID     _entH2hSpid;
        IENT_H2HTEF     _entH2hTef;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_H2H(Configuracion configuracion, IENT_H2HFXD entH2hFxd, IENT_H2HFXI entH2hFxi, IENT_H2HFXT entH2hFxt, IENT_H2HSPID entH2hSpid, IENT_H2HTEF entH2hTef)
        {
            _configuracion   = configuracion;
            _entH2hFxd       = entH2hFxd;
            _entH2hFxi       = entH2hFxi;
            _entH2hFxt       = entH2hFxt;
            _entH2hSpid      = entH2hSpid;
            _entH2hTef       = entH2hTef;
        }

        public async Task ProcesarCorreoAvisoH2H()
        {
            Config.EscribirLog("INFO|ProcesarCorreoAvisoH2H|Realiza el envio de correos para los avisos de H2H");
            await _entH2hFxd.ProcesarOperacionH2HFxdAsync();

            await _entH2hFxi.ProcesarOperacionH2HFxiAsync();

            await _entH2hFxt.ProcesarOperacionH2HFxtAsync();

            await _entH2hSpid.ProcesarOperacionH2HSpidAsync();
            
            await _entH2hTef.ProcesarOperacionH2HTefAsync();
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_H2HFXD : ProcesamientoH2HFxd, IProcesamientoH2HFxd, IENT_H2HFXD
    {
        Configuracion     _configuracion;
        IDetallesTomi     _detallesTomi;
        IDetallesTomser   _detallesTomser;
        IENT_AVISOH2H _envioH2H;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_H2HFXD(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_AVISOH2H envioH2H)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _envioH2H        = envioH2H;
        }
        public async Task ProcesarOperacionH2HFxdAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de las notificaciones H2H tipo FXD...
                Config.EscribirLog("INFO|ProcesarOperacionH2HFxdAsync|Procesando notificaciones H2H tipo FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoH2HFxdDTO> complementoH2HFXD = await this.ObtencionDatosH2HFxd();

                //Si no pudo obtener sus complementos...
                if (complementoH2HFXD.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionH2HFxdAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoH2HFxdDTO H2HFxd in complementoH2HFXD)
                {
                    var DtoAvisosH2H = new ComplementoAvisoH2HDTO
                    {
                        T24_COMPANY_NAME =            H2HFxd.T24_COMPANY_NAME,
                        T24_BEN_ACCT_NO =             H2HFxd.T24_BEN_ACCT_NO,
                        T24_DEBIT_AMOUNT =            H2HFxd.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION =          H2HFxd.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA =           H2HFxd.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF =              H2HFxd.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID =          H2HFxd.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME =           H2HFxd.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY =          H2HFxd.T24_DEBIT_CURRENCY,
                        T24_CUSTOMER_CODE =           H2HFxd.T24_CUSTOMER_CODE,
                        NF_FOLIO =                    H2HFxd.NF_FOLIO,
                        T24_DEBIT_THEIR_REF =         H2HFxd.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO =           H2HFxd.T24_DEBIT_ACCT_NO,
                        CORREO =                      H2HFxd.CORREO,
                        NF_CUENTA_CORRESPONSAL =      string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null,
                    };


                    Config.EscribirLog($"INFO|ProcesarOperacionH2HFxdAsync|Preparando envio del folio: '{H2HFxd.NF_FOLIO}'");
                    //Realiza el envio de las notificaciones H2H de tipo FXD
                    bool resultado = await _envioH2H.EnviaAvisosH2H(ConstantesServicio.tipoFXD, H2HFxd.CORREO, null, DtoAvisosH2H);



                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioH2HFxd(H2HFxd.NF_FOLIO);
                        Config.EscribirLog($"INFO|ProcesarOperacionH2HFxdAsync|Se termina procesamiento de folio:. '{H2HFxd.NF_FOLIO}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionH2HFxdAsync|Error en procesamiento de folio: '{H2HFxd.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionH2HFxdAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_H2HFXI : ProcesamientoH2HFxi, IProcesamientoH2HFxi, IENT_H2HFXI
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_AVISOH2H _envioH2H;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_H2HFXI(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_AVISOH2H envioH2H)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _envioH2H        = envioH2H;
        }
        public async Task ProcesarOperacionH2HFxiAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de las notificaciones H2H tipo FXI...
                Config.EscribirLog("INFO|ProcesarOperacionH2HFxiAsync|Procesando notificaciones H2H tipo FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoH2HFxiDTO> complementoH2HFXI = await this.ObtencionDatosH2HFxi();

                //Si no pudo obtener sus complementos...
                if (complementoH2HFXI.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionH2HFxiAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoH2HFxiDTO H2HFxi in complementoH2HFXI)
                {
                    var DtoAvisosH2H = new ComplementoAvisoH2HDTO
                    {
                        T24_COMPANY_NAME =             H2HFxi.T24_COMPANY_NAME,
                        T24_BEN_ACCT_NO =              string.Empty,
                        T24_DEBIT_AMOUNT =             H2HFxi.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION =           H2HFxi.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA =            H2HFxi.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF =               H2HFxi.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID =           H2HFxi.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME =            H2HFxi.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY =           H2HFxi.T24_DEBIT_CURRENCY,
                        T24_CUSTOMER_CODE =            H2HFxi.T24_CUSTOMER_CODE,
                        NF_FOLIO =                     H2HFxi.NF_FOLIO,
                        T24_DEBIT_THEIR_REF =          H2HFxi.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO =            H2HFxi.T24_DEBIT_ACCT_NO,
                        CORREO =                       H2HFxi.CORREO,
                        NF_CUENTA_CORRESPONSAL =       H2HFxi.NF_CUENTA_CORRESPONSAL,
                        NF_COUNTRY =                   H2HFxi.NF_COUNTRY,
                        NF_CHARGE =                    H2HFxi.NF_CHARGE,
                        NF_MENSAJE =                   H2HFxi.NF_MENSAJE,
                        NF_NUMERIC_REFERENCE =         string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO =               null,
                        NF_FECHA_APLICACION =          null
                    };


                    Config.EscribirLog($"INFO|ProcesarOperacionH2HFxiAsync|Preparando envio del folio: '{H2HFxi.NF_FOLIO}'");
                    //Realiza el envio de las notificaciones H2H de tipo FXI
                    bool resultado = await _envioH2H.EnviaAvisosH2H(ConstantesServicio.tipoFXI, H2HFxi.CORREO, null, DtoAvisosH2H);

                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioH2HFxd(H2HFxi.NF_FOLIO);
                        Config.EscribirLog($"INFO|ProcesarOperacionH2HFxiAsync|Se termina procesamiento de folio:. '{H2HFxi.NF_FOLIO}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionH2HFxiAsync|Error en procesamiento de folio: '{H2HFxi.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionH2HFxiAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_H2HFXT : ProcesamientoH2HFxt, IProcesamientoH2HFxt, IENT_H2HFXT
    {
        Configuracion    _configuracion;
        IDetallesTomi    _detallesTomi;
        IDetallesTomser  _detallesTomser;
        IENT_AVISOH2H    _envioH2H;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_H2HFXT(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_AVISOH2H envioH2H)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _envioH2H        = envioH2H;
        }
        public async Task ProcesarOperacionH2HFxtAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de las notificaciones H2H tipo FXT...
                Config.EscribirLog("INFO|ProcesarOperacionH2HFxtAsync|Procesando notificaciones H2H tipo FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoH2HFxtDTO> complementoH2HFXT = await this.ObtencionDatosH2HFxt();

                //Si no pudo obtener sus complementos...
                if (complementoH2HFXT.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionH2HFxtAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoH2HFxtDTO H2HFxt in complementoH2HFXT)
                {

                    var DtoAvisosH2H = new ComplementoAvisoH2HDTO
                    {
                        T24_COMPANY_NAME = H2HFxt.T24_COMPANY_NAME,
                        T24_BEN_ACCT_NO = H2HFxt.T24_BEN_ACCT_NO,
                        T24_DEBIT_AMOUNT = H2HFxt.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = H2HFxt.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = H2HFxt.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = H2HFxt.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = H2HFxt.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = H2HFxt.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = H2HFxt.T24_DEBIT_CURRENCY,
                        T24_CUSTOMER_CODE = H2HFxt.T24_CUSTOMER_CODE,
                        NF_FOLIO = H2HFxt.NF_FOLIO,
                        T24_DEBIT_THEIR_REF = H2HFxt.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO = H2HFxt.T24_DEBIT_ACCT_NO,
                        CORREO = H2HFxt.CORREO,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null
                    };

                    Config.EscribirLog($"INFO|ProcesarOperacionH2HFxtAsync|Preparando envio del folio: '{H2HFxt.NF_FOLIO}'");
                    //Realiza el envio de las notificaciones H2H de tipo FXD
                    bool resultado = await _envioH2H.EnviaAvisosH2H(ConstantesServicio.tipoFXT, H2HFxt.CORREO, null, DtoAvisosH2H);

                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioH2HFxt(H2HFxt.NF_FOLIO);
                        Config.EscribirLog($"INFO|ProcesarOperacionH2HFxtAsync|Se termina procesamiento de folio:. '{H2HFxt.NF_FOLIO}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionH2HFxtAsync|Error en procesamiento de folio: '{H2HFxt.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionH2HFxtAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_H2HSPID : ProcesamientoH2HSpid, IProcesamientoH2HSpid, IENT_H2HSPID
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_AVISOH2H _envioH2H;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_H2HSPID(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_AVISOH2H envioH2H)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesTomser = detallesTomser;
            _envioH2H = envioH2H;
        }
        public async Task ProcesarOperacionH2HSpidAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de las notificaciones H2H tipo SPID...
                Config.EscribirLog("INFO|ProcesarOperacionH2HSpidAsync|Procesando notificaciones H2H tipo FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoH2HSpidDTO> complementoH2HSPID = await this.ObtencionDatosH2HSpid();

                //Si no pudo obtener sus complementos...
                if (complementoH2HSPID.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionH2HSpidAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoH2HSpidDTO H2HSpid in complementoH2HSPID)
                {
                    var DtoAvisosH2H = new ComplementoAvisoH2HDTO
                    {
                        T24_COMPANY_NAME = H2HSpid.T24_COMPANY_NAME,
                        T24_BEN_ACCT_NO = H2HSpid.T24_BEN_ACCT_NO,
                        T24_DEBIT_AMOUNT = H2HSpid.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = H2HSpid.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = H2HSpid.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = H2HSpid.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = H2HSpid.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = H2HSpid.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = H2HSpid.T24_DEBIT_CURRENCY,
                        T24_CUSTOMER_CODE = H2HSpid.T24_CUSTOMER_CODE,
                        NF_FOLIO = H2HSpid.NF_FOLIO,
                        T24_DEBIT_THEIR_REF = H2HSpid.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO = H2HSpid.T24_DEBIT_ACCT_NO,
                        CORREO = H2HSpid.CORREO,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty,
                        NF_NUMERIC_REFERENCE = H2HSpid.NF_NUMERIC_REFERENCE,
                        NF_CLASIFICACION_OPERACION = H2HSpid.NF_CLASIFICACION_OPERACION,
                        NF_RELACION_COMERCIAL = H2HSpid.NF_RELACION_COMERCIAL,
                        NF_FECHA_CARGO = null,
                        NF_FECHA_APLICACION = null
                    };

                    Config.EscribirLog($"INFO|ProcesarOperacionH2HSpidAsync|Preparando envio del folio: '{H2HSpid.NF_FOLIO}'");
                    //Realiza el envio de las notificaciones H2H de tipo FXD
                    bool resultado = await _envioH2H.EnviaAvisosH2H(ConstantesServicio.tipoSPID, H2HSpid.CORREO, null, DtoAvisosH2H);

                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioH2HSpid(H2HSpid.NF_FOLIO);
                        Config.EscribirLog($"INFO|ProcesarOperacionH2HSpidAsync|Se termina procesamiento de folio:. '{H2HSpid.NF_FOLIO}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionH2HSpidAsync|Error en procesamiento de folio: '{H2HSpid.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionH2HSpidAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_H2HTEF : ProcesamientoH2HTef, IProcesamientoH2HTef, IENT_H2HTEF
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_AVISOH2H _envioH2H;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_H2HTEF(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_AVISOH2H envioH2H)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesTomser = detallesTomser;
            _envioH2H = envioH2H;
        }
        public async Task ProcesarOperacionH2HTefAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de las notificaciones H2H tipo FXD...
                Config.EscribirLog("INFO|ProcesarOperacionH2HTefAsync|Procesando notificaciones H2H tipo FXD");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoH2HTefDTO> complementoH2HTEF = await this.ObtencionDatosH2HTef();

                //Si no pudo obtener sus complementos...
                if (complementoH2HTEF.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionH2HTefAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoH2HTefDTO H2HTef in complementoH2HTEF)
                {

                    var DtoAvisosH2H = new ComplementoAvisoH2HDTO
                    {
                        T24_COMPANY_NAME = H2HTef.T24_COMPANY_NAME,
                        T24_BEN_ACCT_NO = H2HTef.T24_BEN_ACCT_NO,
                        T24_DEBIT_AMOUNT = H2HTef.T24_DEBIT_AMOUNT,
                        NF_FECHA_EJECUCION = H2HTef.NF_FECHA_EJECUCION,
                        NF_FECHA_AUTORIZA = H2HTef.NF_FECHA_AUTORIZA,
                        NF_BANCO_BENEF = H2HTef.NF_BANCO_BENEF,
                        T24_TRANSACTION_ID = H2HTef.T24_TRANSACTION_ID,
                        T24_BEN_CUST_NAME = H2HTef.T24_BEN_CUST_NAME,
                        T24_DEBIT_CURRENCY = H2HTef.T24_DEBIT_CURRENCY,
                        T24_CUSTOMER_CODE = H2HTef.T24_CUSTOMER_CODE,
                        NF_FOLIO = H2HTef.NF_FOLIO,
                        T24_DEBIT_THEIR_REF = H2HTef.T24_DEBIT_THEIR_REF,
                        T24_DEBIT_ACCT_NO = H2HTef.T24_DEBIT_ACCT_NO,
                        CORREO = H2HTef.CORREO,
                        NF_CUENTA_CORRESPONSAL = string.Empty,
                        NF_COUNTRY = string.Empty,
                        NF_CHARGE = string.Empty,
                        NF_MENSAJE = string.Empty,
                        NF_NUMERIC_REFERENCE = string.Empty,
                        NF_CLASIFICACION_OPERACION = string.Empty,
                        NF_RELACION_COMERCIAL = string.Empty,
                        NF_FECHA_CARGO = H2HTef.NF_FECHA_CARGO,
                        NF_FECHA_APLICACION = H2HTef.NF_FECHA_APLICACION
                    };

                    Config.EscribirLog($"INFO|ProcesarOperacionH2HTefAsync|Preparando envio del folio: '{H2HTef.NF_FOLIO}'");
                    //Realiza el envio de las notificaciones H2H de tipo TEF
                    bool resultado = await _envioH2H.EnviaAvisosH2H(ConstantesServicio.tipoFXD, H2HTef.CORREO, null, DtoAvisosH2H);

                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioH2HTef(H2HTef.NF_FOLIO);
                        Config.EscribirLog($"INFO|ProcesarOperacionH2HTefAsync|Se termina procesamiento de folio:. '{H2HTef.NF_FOLIO}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionH2HTefAsync|Error en procesamiento de folio: '{H2HTef.NF_FOLIO}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionH2HTefAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.Interfaces;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_ACUSESAT : IENT_ACUSESAT
    {
        Configuracion     _configuracion;
        IENT_ACUSESATNEPE _entAcuseSatNepe;
        IENT_ACUSESATREF  _entAcuseSatRef;
        IENT_ACUSESATSHCP _entAcuseSatShcp;

        public ENT_ACUSESAT(Configuracion configuracion, IENT_ACUSESATNEPE entAcuseSatNepe, IENT_ACUSESATREF entAcuseSatRef, IENT_ACUSESATSHCP entAcuseSatShcp)
        {
            _configuracion   = configuracion;
            _entAcuseSatNepe = entAcuseSatNepe;
            _entAcuseSatRef  = entAcuseSatRef;
            _entAcuseSatShcp = entAcuseSatShcp;
        }

        public async Task ProcesarCorreoAcuseSAT()
        {
            //Se llama metodo para el envio de acuse de SAT de tipo NEPE
            await _entAcuseSatNepe.ProcesarOperacionSatNepeAsync();

            //Se llama metodo para el envio de acuse de SAT de tipo REF
            await _entAcuseSatRef.ProcesarOperacionSatRefAsync();

            //Se llama metodo para el envio de acuse de SAT de tipo REF
            await _entAcuseSatShcp.ProcesarOperacionSatShcpAsync();
        }

    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_ACUSESATNEPE : ProcesamientoAcuseSatNepe, IProcesamientoAcuseSatNepe, IENT_ACUSESATNEPE
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_ENVIOACUSE _entEnvioAcuse;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_ACUSESATNEPE(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_ENVIOACUSE entEnvioAcuse)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entEnvioAcuse   = entEnvioAcuse;
        }

        public async Task ProcesarOperacionSatNepeAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los beneficiarios tipo SPID...
                Config.EscribirLog("INFO|ProcesarOperacionSatNepeAsync|Procesando Solicitud de envio operaciones SPID");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoSatNepeDTO> complementoSatNepe = await this.ObtencionDatosSatNepe();

                //Si no pudo obtener sus complementos...
                if (complementoSatNepe.Count is 0)
                {
                    Config.EscribirLog("No se encontro el detalle de la operación");
                }

                foreach (ComplementoSatNepeDTO SatNepe in complementoSatNepe)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionSatNepeAsync|Preparando envio del folio: '{SatNepe.CONSECUTIVO}'");
                    //Realiza el envio de la información al acuse correspondiente del SAT
                    bool resultado = _entEnvioAcuse.EnviarAcuseSatNepe(ConstantesServicio.MAIL_SOCKET_TO, ConstantesServicio.MAIL_SOCKET_CC, SatNepe);

                    if (resultado)
                    {
                        await _detallesTomser.ActualizaFolioSAT(SatNepe.CONSECUTIVO, SatNepe.CAMPO);
                        Config.EscribirLog($"INFO|ProcesarOperacionSatNepeAsync|Se termina procesamiento de folio:. '{SatNepe.CONSECUTIVO}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionSatNepeAsync|Error en procesamiento de folio: '{SatNepe.CONSECUTIVO}'");
                    }

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionSatNepeAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_ACUSESATREF : ProcesamientoAcuseSatRef, IProcesamientoAcuseSatRef, IENT_ACUSESATREF
    {
        Configuracion    _configuracion;
        IDetallesTomi    _detallesTomi;
        IDetallesTomser  _detallesTomser;
        IENT_ENVIOACUSE  _entEnvioAcuse;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_ACUSESATREF(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_ENVIOACUSE entEnvioAcuse)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entEnvioAcuse   = entEnvioAcuse;
        }

        public async Task ProcesarOperacionSatRefAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los acuses del SAT REF...
                Config.EscribirLog("INFO|Procesa_Aviso_Beneficiarios_NOM_FXD|Procesando Solicitud de envio operaciones SAT REF");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoSatRefDTO> complementoSatRef = await this.ObtencionDatosSatRef();

                //Si no pudo obtener sus complementos...
                if (complementoSatRef.Count is 0)
                {
                    throw new Exception("No se encontro el detalle de la operación");
                }

                foreach (ComplementoSatRefDTO SatRef in complementoSatRef)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionSatRefAsync|Preparando envio del folio: '{SatRef.CONSECUTIVO}'");
                    //Realiza el envio de la información al acuse correspondiente del SAT
                    bool resultado = _entEnvioAcuse.EnviarAcuseSatRef(ConstantesServicio.MAIL_SOCKET_TO, ConstantesServicio.MAIL_SOCKET_CC, SatRef);

                    if (resultado)
                    {
                        await _detallesTomser.ActualizaFolioSATREF(SatRef.CONSECUTIVO, SatRef.CAMPO);
                        Config.EscribirLog($"INFO|ProcesarOperacionSatRefAsync|Se termina procesamiento de folio:. '{SatRef.CONSECUTIVO}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionSatRefAsync|Error en procesamiento de folio: '{SatRef.CONSECUTIVO}'");
                    }

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionSatRefAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }

    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_ACUSESATSHCP : ProcesamientoAcuseSatNepe, IProcesamientoAcuseSatNepe, IENT_ACUSESATSHCP
    {
        Configuracion    _configuracion;
        IDetallesTomser  _detallesTomser;
        IENT_ENVIOACUSE  _entEnvioAcuse;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_ACUSESATSHCP(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_ENVIOACUSE entEnvioAcuse)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomser  = detallesTomser;
            _entEnvioAcuse   = entEnvioAcuse;
        }
        public async Task ProcesarOperacionSatShcpAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de los acuse de SAT tipo HSCP...
                Config.EscribirLog("INFO|ProcesarOperacionSatShcpAsync|Procesando Solicitud de envio operaciones SPID");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoSatNepeDTO> complementoSatNepe = await this.ObtencionDatosSatShcp();

                //Si no pudo obtener sus complementos...
                if (complementoSatNepe.Count is 0)
                {
                    Config.EscribirLog("No se encontro el detalle de la operación");
                }

                foreach (ComplementoSatNepeDTO SatNepe in complementoSatNepe)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionSatShcpAsync|Preparando envio del folio: '{SatNepe.CONSECUTIVO}'");
                    //Realiza el envio de la información al acuse correspondiente del SAT
                    bool resultado = _entEnvioAcuse.EnviarAcuseSatHscp(ConstantesServicio.MAIL_SOCKET_TO, ConstantesServicio.MAIL_SOCKET_CC, SatNepe);

                    if (resultado)
                    {
                        await _detallesTomser.ActualizaFolioSATSHCP(SatNepe.CONSECUTIVO, SatNepe.CAMPO);
                        Config.EscribirLog($"INFO|ProcesarOperacionSatShcpAsync|Se termina procesamiento de folio:. '{SatNepe.CONSECUTIVO}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionSatShcpAsync|Error en procesamiento de folio: '{SatNepe.CONSECUTIVO}'");
                    }

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionSatShcpAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENTOPEHOTS : ProcesamientoOperacionHotscan, IProcesamientoOperacionHotscan, IENTOPEHOTS
    {
        Configuracion    _configuracion;
        IDetallesTomser  _detallesTomser;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENTOPEHOTS(Configuracion configuracion, IDetallesTomser detallesTomser)
            : base(configuracion, detallesTomser)
        {
            _configuracion = configuracion;
            _detallesTomser = detallesTomser;
        }

        public async Task ProcesarOperacionHotscanAsync()
        {
            try
            {
                string lsMensaje = null;
                //Obtiene los atributos de los complementos de la operaciónes de tipo Hotscan
                Config.EscribirLog("INFO|ProcesarOperacionHotscanAsync|Procesando operaciones Hotscan ");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoOperacHotscanDTO> complementoOpHots = await this.ObtencionDatosOperacHotscan();

                //Si no pudo obtener sus complementos...
                if (complementoOpHots.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionHotscanAsync | No se encontro información");
                    return;
                }

                //Realiza el recorrido de los datos
                foreach (ComplementoOperacHotscanDTO complementoOperHotscan in complementoOpHots)
                {
                    
                    //Obtiene los CDA de los complementos de la operación de histcan detalle...
                    List<ComplementoOperacHotscanDetDTO> complementoOpHotsDet = await this.ObtencionDatosOperacHotscanDet(complementoOperHotscan.id_scan);

                    Config.EscribirLog($"INFO|ProcesarOperacionHotscanAsync | Busca información de detalle de la operación Hotscan Folio: '{complementoOperHotscan.id_scan}'");
                    //Realiza el recorrido de los datos
                    foreach (ComplementoOperacHotscanDetDTO complementoOperHotscanDet in complementoOpHotsDet)
                    {

                         lsMensaje += "<tr><td class='dato_tabla' align='center'>" + HtmlHelper.TextoHtml(complementoOperHotscanDet.des_detector) + "</td>" +
                                      "<td class='dato_tabla' align='center'>" + HtmlHelper.TextoHtml(complementoOperHotscanDet.des_origen_lista) + "</td>" +
                                      "<td class='dato_tabla'>&nbsp;" + HtmlHelper.TextoHtml(complementoOperHotscanDet.des_nombre_lista) + "</td></tr>" + ConstantesServicio.NewLine;
                    }

                    Config.EscribirLog($"INFO|ProcesarOperacionHotscanAsync | Preparando envio del folio: '{complementoOperHotscan.id_scan}'");
                    bool error = await EnviaOperacHotscan(complementoOperHotscan.des_parametro, complementoOperHotscan.des_parametro2, complementoOperHotscan, lsMensaje);

                    if (error)
                    {
                        await _detallesTomser.ActualizaOperacionHotscan(complementoOperHotscan.id_scan);
                        Config.EscribirLog($"INFO|ProcesarOperacionHotscanAsync |Se termina procesamiento de folio:'{complementoOperHotscan.id_scan}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionHotscanAsync |Error en procesamiento de folio: '{complementoOperHotscan.id_scan}'");
                    }

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionHotscanAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }

        public Task<bool> EnviaOperacHotscan(string strMail, string strMailCC, ComplementoOperacHotscanDTO complementoOperHotscan, string strMensajeComp)
        {
            try
            {
                if (complementoOperHotscan == null)
                    throw new ArgumentNullException(nameof(complementoOperHotscan));

                string lsMensaje = null;
                bool   lbOmitirRecurrente = true;
                string lsTipoBase = "";
                string lsBase = null;

                if (complementoOperHotscan.flg_omitir_notificacion)
                {
                    lbOmitirRecurrente = true;
                }
                else {
                    lbOmitirRecurrente = false;
                }

                

                lsBase = complementoOperHotscan.base_origen?.ToUpperInvariant() ?? string.Empty;

                if (lsBase.IndexOf(ConstantesServicio.tipoNOMINA) >= 0)
                {
                    lsTipoBase = ConstantesServicio.tipoNOMINA;
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>AVISO DE TRANSFERENCIA DE FONDOS DOMÉSTICA</span>");
                if (!string.IsNullOrEmpty(lsTipoBase))
                {
                    sb.AppendLine($"<BR><span class='titulo_general'>{lsTipoBase}</span>");
                }
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Datos de la Transacción:</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Folio:</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotscan.nf_folio)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha detección:</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.FechaHtml(complementoOperHotscan.fec_deteccion)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Nombre del Cliente:</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotscan.des_nombre_cliente)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Beneficiario:</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotscan.des_beneficiario)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Cuenta CLABE:</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotscan.num_clabe)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Monto:</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotscan.cve_moneda)} {HtmlHelper.ImporteHtml(complementoOperHotscan.num_importe)}</td></tr>");
                sb.AppendLine($"<tr><td class='header_tabla' width='30%'>&nbsp;Descripción:</td><td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotscan.des_descripcion)}</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("<span class='titulo_tabla'>Datos de listas restringidas:</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='header_tabla' align='center'>Origen</td><td class='header_tabla' align='center'>Lista</td><td class='header_tabla' align='center'>Nombre</td></tr>");
                sb.Append(strMensajeComp);
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR><span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");
                sb.AppendLine("</center>");

                if (lbOmitirRecurrente)
                {
                    sb.AppendLine("<BR><span class='NotaOmitir'><B>Este mail es de carácter informativo, ya que la transacción fue revisada y autorizada previamente y no se encontraron elementos que coincidan con la lista de OFAC</B></span>");
                }
                sb.AppendLine("</body>");
                sb.AppendLine("</html>");

                lsMensaje = sb.ToString();


                MensajeCorreo mensajeCorreo = new MensajeCorreo();

                _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                mensajeCorreo.CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenHotscan;
                mensajeCorreo.NombreOrigen = ConstantesServicio.NombreCorreoOrigenHotscan;
                mensajeCorreo.Para = _configuracion.CorreoParaDefault ?? strMail;
                mensajeCorreo.ConCopia = _configuracion.CorreoParaDefault ?? strMailCC;
                mensajeCorreo.CopiaOculta = null;
                mensajeCorreo.Asunto = _configuracion.SujetoDefault ?? ConstantesServicio.AsuntoHotscan;
                mensajeCorreo.Mensaje = lsMensaje;
                mensajeCorreo.ArchivosAdjuntos = null;

                if (CorreoHelper.EnviarCorreo(mensajeCorreo))
                {
                    return Task.FromResult(true);
                }
                else
                {
                    mensajeCorreo.CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol;
                    mensajeCorreo.NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol;
                    mensajeCorreo.Para = _configuracion.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR;
                    mensajeCorreo.ConCopia = null;
                    mensajeCorreo.CopiaOculta = null;
                    mensajeCorreo.Asunto = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracion.SujetoDefault + " Error";
                    mensajeCorreo.Mensaje = ConstantesServicio.MensajeErrorHotscan;
                    mensajeCorreo.ArchivosAdjuntos = null;

                    CorreoHelper.EnviarCorreo(mensajeCorreo);
                    return Task.FromResult(true);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaOperacHotscan|Se presenta exception: {ex.ToString()}");
                return Task.FromResult(false);
            }
        }

    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_OPEHOTSPID : ProcesamientoOperacionHotsSpid, IProcesamientoOperacionHotsSpid, IENT_OPEHOTSPID
    {
        Configuracion    _configuracion;
        IDetallesTomi    _detallesTomi;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_OPEHOTSPID(Configuracion configuracion, IDetallesTomi detallesTomi)
            : base(configuracion, detallesTomi)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
        }

        public async Task ProcesarOperacionHostSpidAsync()
        {
            try
            {
                string lsMensaje = null;
                //Obtiene los atributos de los complementos de la operaciónes de hotscan de tipo SPID
                Config.EscribirLog("INFO|ProcesarOperacionHostSpidAsync|Procesando operaciones Hotscan de tipo SPID ");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoOperHotsSpidDTO> complementoOpHotSpid = await this.ObtencionDatosOperacHotSpid();

                //Si no pudo obtener sus complementos...
                if (complementoOpHotSpid.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionHostSpidAsync | No se encontro información");
                    return;
                }

                //Realiza el recorrido de los datos
                foreach (ComplementoOperHotsSpidDTO OperacHotsSSpid in complementoOpHotSpid)
                {

                    //Obtiene los CDA de los complementos de la operación de histcan detalle...
                    List<ComplementoOperacHotscanSpidDetDTO> complementoOpHotsSpídDet = await this.ObtencionDatosOperacHotSpidDet(OperacHotsSSpid.id_alerta);

                    //Realiza el recorrido de los datos
                    foreach (ComplementoOperacHotscanSpidDetDTO OperacHotsSSpidDet in complementoOpHotsSpídDet)
                    {

                        lsMensaje += "<tr><td class='dato_tabla' align='center'>" + HtmlHelper.TextoHtml(OperacHotsSSpidDet.des_detector) + " </td>" +
                                     "<td class='dato_tabla' align='center'>" + HtmlHelper.TextoHtml(OperacHotsSSpidDet.des_origen_lista) + " </td>" +
                                     "<td class='dato_tabla'>&nbsp;" + HtmlHelper.TextoHtml(OperacHotsSSpidDet.des_nombre_lista) + "</td></tr>" + ConstantesServicio.NewLine;
                    }

                    Config.EscribirLog($"INFO|ProcesarOperacionHostSpidAsync | Preparando envio del folio: '{OperacHotsSSpid.id_alerta}'");
                    bool error = await EnviaOperacHotscanSpid(OperacHotsSSpid.cve_valor, OperacHotsSSpid.cve_valor2, OperacHotsSSpid, lsMensaje);

                    if (error)
                    {
                        await _detallesTomi.ActualizaFolioHotscanSpid(OperacHotsSSpid.id_alerta);
                        Config.EscribirLog($"INFO|ProcesarOperacionHostSpidAsync |Se termina procesamiento de folio:'{OperacHotsSSpid.id_alerta}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionHostSpidAsync |Error en procesamiento de folio: '{OperacHotsSSpid.id_alerta}'");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionHostSpidAsync|Se presenta exception: '{ex.ToString()}'");
            }
        }

        public async Task<bool> EnviaOperacHotscanSpid(string strMail, string strMailCC, ComplementoOperHotsSpidDTO complementoOperHotsSpid, string strMensajeComp)
        {
            try
            {
                if (complementoOperHotsSpid == null)
                    throw new ArgumentNullException(nameof(complementoOperHotsSpid));
                string lsMensaje = null;
                bool lbOmitirRecurrente = true;
                string lsTipoBase = "";
                string lsBase = null;

                if (complementoOperHotsSpid.flg_recurrente)
                { 
                    lbOmitirRecurrente = true;
                }
                else
                {
                    lbOmitirRecurrente = false;
                }



                lsBase = complementoOperHotsSpid?.base_origen?.ToUpper() ?? string.Empty;

                if (lsBase.IndexOf(ConstantesServicio.tipoNOMINA) >= 0)
                {
                    lsTipoBase = ConstantesServicio.tipoNOMINA;
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>AVISO DE TRANSFERENCIA SPID</span>");
                if (!string.IsNullOrWhiteSpace(lsTipoBase))
                {
                    sb.AppendLine($"<BR><span class='titulo_general'>{lsTipoBase}</span>");
                }
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Datos de la Transacción:</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Folio:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotsSpid.nf_folio)}-{HtmlHelper.TextoHtml(complementoOperHotsSpid.base_origen)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Fecha detección:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.FechaHtml(complementoOperHotsSpid.fec_deteccion)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Ordenante:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotsSpid.des_ordenante)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Beneficiario:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotsSpid.des_beneficiario)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Cuenta:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotsSpid.num_cta_beneficiario)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Monto:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotsSpid.cve_moneda)} {HtmlHelper.ImporteHtml(complementoOperHotsSpid.num_importe)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='30%'>&nbsp;Concepto de pago:</td>" +
                    $"<td class='dato_tabla' width='70%'>&nbsp;{HtmlHelper.TextoHtml(complementoOperHotsSpid.des_descripcion)}</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("<span class='titulo_tabla'>Datos de listas restringidas:</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='400' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='header_tabla' align='center'>Origen</td><td class='header_tabla' align='center'>Lista</td><td class='header_tabla' align='center'>Nombre</td></tr>");
                sb.Append(strMensajeComp);
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR><span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");
                sb.AppendLine("</center>");
                sb.AppendLine("</body>");
                sb.AppendLine("</html>");

                lsMensaje = sb.ToString();
                if (lbOmitirRecurrente)
                {
                    lsMensaje += "<BR><span class='NotaOmitir'><B>Este mail es de car&aacute;cter informativo, ya que la transacci&oacute;n fue revisada y autorizada previamente y no se encontraron elementos que coincidan con la lista de OFAC</span>" + ConstantesServicio.NewLine;
                }

                lsMensaje += "</body>" + ConstantesServicio.NewLine +
                             "</html>";


                _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                                ? ConstantesServicio.AsuntoHotscanSpid
                                : _configuracion.SujetoDefault;

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenHotscanSpid,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenHotscanSpid,
                    Para = _configuracion.CorreoParaDefault ?? strMail,
                    ConCopia = _configuracion.CorreoParaDefault ?? (lbOmitirRecurrente ? "" : strMailCC),
                    CopiaOculta = null,
                    Asunto = asuntoBase,
                    Mensaje = lsMensaje,
                    ArchivosAdjuntos = null
                });

                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracion.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault) ? ConstantesServicio.AsuntoError : _configuracion.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeErrorHotscan,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaOperacHotscan|Se presenta exception: {ex.ToString()}");
                return false;
            }
        }

    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_ALERTACRIT : ProcesamientoAlertaCritica, IProcesamientoAlertaCritica, IENT_ALERTACRIT
    {
        Configuracion _configuracion;
        IDetallesVistas _detallesVistasT24;
        private readonly IParametrizacion _parametrizacion;

        public ENT_ALERTACRIT(Configuracion configuracion, IParametrizacion parametrizacion, IDetallesVistas detallesVistasT24)
            : base(configuracion, detallesVistasT24)
        {
            _configuracion = configuracion;
            _detallesVistasT24 = detallesVistasT24;
            _parametrizacion = parametrizacion;
        }

        public async Task ProcesarAlertasCriticasAsync()
        {
            try
            {
                string email = null;
                string emailcc = null;
                string emailbcc = null;
                _configuracion.ParametrosSistema?.TryGetValue("MON_T24_DUPLICADO_mailTo", out email);
                _configuracion.ParametrosSistema?.TryGetValue("MON_T24_DUPLICADO_mailCc", out emailcc);
                _configuracion.ParametrosSistema?.TryGetValue("MON_T24_DUPLICADO_mailBcc", out emailbcc);

                string lsMensaje = null;
                //Obtiene los atributos de los complementos de las alertas criticas
                Config.EscribirLog("INFO|ProcesarAlertasCriticasAsync|Procesando alertas criticas");

                //Obtiene los CDA de los complementos de las alertas criticas ...
                List<ComplementoAlertasCriticasDTO> complementoAlertaCrit = await this.ObtencionDatosAlertasCriticas();

                //Si no pudo obtener sus complementos...
                if (complementoAlertaCrit.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarAlertasCriticasAsync | No se encontro información");
                    return;
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:22px; background-color:#000000; color:#FFFFFF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:16px; color:#A21403; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#F5F6F9; text-decoration:none; }");
                sb.AppendLine(".header_white   { font-family: sans-serif; font-size:11px; background-color:#066A9D; color:#FFFFFF; text-decoration:none; }");
                sb.AppendLine(".header_white1  { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#FFFFFF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:09px; color:#1C1C1D; text-decoration:none; }");
                sb.AppendLine(".dato_rojo      { font-family: sans-serif; font-size:09px; color:#F5240B; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color:#A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color:#FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: 1 #030303; }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<TABLE border='0' cellspacing='1' cellpadding='1' width='701' borderColor='#E0E0E0'>");
                sb.AppendLine("<TR>");
                sb.AppendLine("<TD class='titulo_general'><CENTER>ALERTA DUPLICIDAD INTERNATIONAL WIRE</CENTER></TD>");
                sb.AppendLine("</TR>");
                sb.AppendLine("<TR>");
                sb.AppendLine("<TD class='header_white1'><CENTER>&nbsp;</CENTER></TD>");
                sb.AppendLine("</TR>");
                sb.AppendLine(
                    $"<TR><TD class='dato_tabla' align='right'><STRONG>{DateTime.Now:dd/MM/yyyy HH:mm:ss}</STRONG></TD></TR>");

                sb.AppendLine("<TR>");
                sb.AppendLine("<TD>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla' align='left'>&nbsp;</span>");
                sb.AppendLine("<table border='0' cellspacing='1' cellpadding='1' width='100%' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr>");
                sb.AppendLine("<td colspan='7' class='header_white1'>&nbsp;</td>");
                sb.AppendLine("</tr>");
                sb.AppendLine("<tr>");
                sb.AppendLine("<td class='header_white' align='CENTER'>Cuenta</td>");
                sb.AppendLine("<td class='header_white' align='CENTER'>Cliente</td>");
                sb.AppendLine("<td class='header_white' align='CENTER'>Moneda</td>");
                sb.AppendLine("<td class='header_white' align='CENTER'>Monto</td>");
                sb.AppendLine("<td class='header_white' align='CENTER'>Saldo Total</td>");
                sb.AppendLine("<td class='header_white' align='CENTER'>Registros</td>");

                sb.AppendLine("</tr>");
                //Realiza el recorrido de los datos
                foreach (ComplementoAlertasCriticasDTO OperacAlertCrit in complementoAlertaCrit)
                {
                    sb.AppendLine(
                         $"<tr>" +
                         $"<td class='dato_tabla' align='CENTER'>{OperacAlertCrit.CUENTA}</td>" +
                         $"<td class='dato_tabla'>&nbsp;{OperacAlertCrit.CLIENTE}</td>" +
                         $"<td class='dato_tabla' align='CENTER'>{OperacAlertCrit.CCY}</td>" +
                         $"<td class='dato_tabla' align='RIGHT'>{OperacAlertCrit.MONTO}</td>" +
                         $"<td class='dato_tabla' align='RIGHT'>{OperacAlertCrit.TOTAL}</td>" +
                         $"<td class='dato_tabla'>&nbsp;{OperacAlertCrit.NUMREG}</td>" +
                         $"</tr>");
                }

                sb.AppendLine("</table>");
                sb.AppendLine(
                    "<BR><BR><span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");
                sb.AppendLine("</TR>");
                sb.AppendLine("</TD>");
                sb.AppendLine("</TABLE>");
                sb.AppendLine("</body>");
                sb.AppendLine("</html>");

                lsMensaje = sb.ToString();

                if (complementoAlertaCrit.Count > 0)
                {

                    Config.EscribirLog($"INFO|ProcesarAlertasCriticasAsync | Enviando el Correo -->  :'{email}' ");


                    _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                    _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                    _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                    var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                                ? ConstantesServicio.AvisoNotificacion
                                : _configuracion.SujetoDefault;

                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                        Para = _configuracion.CorreoParaDefault ?? email,
                        ConCopia = _configuracion.CorreoParaDefault ?? emailcc,
                        CopiaOculta = _configuracion.CorreoParaDefault ?? emailbcc,
                        Asunto = asuntoBase,
                        Mensaje = lsMensaje,
                        ArchivosAdjuntos = null
                    });
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarAlertasCriticasAsync|Se presenta exception: '{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_SOBGIRO : ProcesamientoSobregiros, IProcesamientoSobregiros, IENT_SOBGIRO
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_SOBGIRO(Configuracion configuracion, IDetallesTomi detallesTomi)
            : base(configuracion, detallesTomi)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
        }

        public async Task ProcesarSobregirosAsync()
        {
            try
            {
                string email = null;
                string emailcc = null;
                string emailbcc = null;
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailTo", out email);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailCc", out emailcc);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailBcc", out emailbcc);

                string lsMensaje = null;
                string lsRed = null;
                //Obtiene los atributos de los complementos de la operaciones de sobregiros
                Config.EscribirLog("INFO|ProcesarSobregirosAsync|Procesando operaciones de sobregiros");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoSobregirosDTO> complementoSobregiro = await this.ObtencionDatosSobregiros();

                //Si no pudo obtener sus complementos...
                if (complementoSobregiro.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarSobregirosAsync | No se encontro información");
                    return;
                }
                else
                {
                    var sb = new StringBuilder();

                    sb.AppendLine("<html>");
                    sb.AppendLine("<head>");
                    sb.AppendLine("<style type='text/css'>");
                    sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:22px; background-color:#000000; color:#FFFFFF; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:16px; color:#A21403; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#F5F6F9; text-decoration:none; }");
                    sb.AppendLine(".header_white   { font-family: sans-serif; font-size:11px; background-color:#066A9D; color:#FFFFFF; text-decoration:none; }");
                    sb.AppendLine(".header_white1  { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#FFFFFF; text-decoration:none; }");
                    sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:09px; color:#1C1C1D; text-decoration:none; }");
                    sb.AppendLine(".dato_rojo      { font-family: sans-serif; font-size:09px; color:#F5240B; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color:#A0A0A0; text-decoration:none; }");
                    sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color:#FF0000; text-decoration:none; }");
                    sb.AppendLine("table { border: 1 #030303; }");
                    sb.AppendLine("</style>");
                    sb.AppendLine("</head>");
                    sb.AppendLine("<body>");
                    sb.AppendLine("<TABLE border='0' cellspacing='1' cellpadding='1' width='701' borderColor='#E0E0E0'>");
                    sb.AppendLine("<TR><TD class='titulo_general'><CENTER>RELACION DE SOBREGIROS</CENTER></TD></TR>");
                    sb.AppendLine("<TR><TD class='header_white1'><CENTER>&nbsp;</CENTER></TD></TR>");
                    sb.AppendLine($"<TR><TD class='dato_tabla' align='right'><STRONG>{DateTime.Now}</STRONG></TD></TR>");
                    sb.AppendLine("<TR><TD>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<span class='titulo_tabla' align='left'>&nbsp;</span>");
                    sb.AppendLine("<table border='1' cellspacing='1' cellpadding='1' width='100%' borderColor='#E0E0E0'>");
                    sb.AppendLine("<tr><td colspan='7' class='header_white1'>&nbsp;</td></tr>");
                    sb.AppendLine("<tr>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Cuenta</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Cliente</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Moneda</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Sobregiro</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Saldo Disponible</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Concepto</td>");
                    sb.AppendLine("</tr>");

                    //Realiza el recorrido de los datos
                    foreach (ComplementoSobregirosDTO OperacSobregiro in complementoSobregiro)
                    {
                        lsRed = OperacSobregiro.Monto_Debito < 0 ? "dato_rojo" : "dato_tabla";

                        sb.AppendLine("<tr>");
                        sb.AppendLine($"<td class='dato_tabla' align='CENTER'>{OperacSobregiro.Cuenta}</td>");
                        sb.AppendLine($"<td class='dato_tabla'>&nbsp;{OperacSobregiro.Nombre_Cliente}</td>");
                        sb.AppendLine($"<td class='dato_tabla' align='CENTER'>{OperacSobregiro.Currency}</td>");
                        sb.AppendLine($"<td class='{lsRed}' align='RIGHT'>{OperacSobregiro.Monto_Debito}</td>");
                        sb.AppendLine($"<td class='{lsRed}' align='RIGHT'>{OperacSobregiro.Saldo}</td>");
                        sb.AppendLine($"<td class='dato_tabla'>&nbsp;{OperacSobregiro.Concepto}</td>");
                        sb.AppendLine("</tr>");
                    }

                    lsMensaje = sb.ToString();

                    Config.EscribirLog($"INFO|ProcesarSobregirosAsync | Enviando el Correo -->  :'{email}' ");

                    _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                    _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                    _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                    var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                                ? ConstantesServicio.AvisoSobregiros
                                : _configuracion.SujetoDefault;

                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                        Para = _configuracion.CorreoParaDefault ?? email,
                        ConCopia = _configuracion.CorreoParaDefault ?? emailcc,
                        CopiaOculta = _configuracion.CorreoParaDefault ?? emailbcc,
                        Asunto = asuntoBase,
                        Mensaje = lsMensaje,
                        ArchivosAdjuntos = null
                    });

                    Config.EscribirLog("INFO|ProcesarSobregirosAsync | EmailRPA_SobreGiros|Actualizando / Borrando Pasado.. TB_T24_REPORTE_CASHPOOLING  ");

                    await _detallesTomi.ActualizaFolioSobregiro();
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarSobregirosAsync|Se presenta exception: '{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_SOBGIRO : ProcesamientoSobregiros, IProcesamientoSobregiros, IENT_SOBGIRO
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_SOBGIRO(Configuracion configuracion, IDetallesTomi detallesTomi)
            : base(configuracion, detallesTomi)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
        }

        public async Task ProcesarSobregirosAsync()
        {
            try
            {
                string email = null;
                string emailcc = null;
                string emailbcc = null;
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailTo", out email);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailCc", out emailcc);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailBcc", out emailbcc);

                string lsMensaje = null;
                string lsRed = null;
                //Obtiene los atributos de los complementos de la operaciones de sobregiros
                Config.EscribirLog("INFO|ProcesarSobregirosAsync|Procesando operaciones de sobregiros");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoSobregirosDTO> complementoSobregiro = await this.ObtencionDatosSobregiros();

                //Si no pudo obtener sus complementos...
                if (complementoSobregiro.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarSobregirosAsync | No se encontro información");
                    return;
                }
                else
                {
                    var sb = new StringBuilder();

                    sb.AppendLine("<html>");
                    sb.AppendLine("<head>");
                    sb.AppendLine("<style type='text/css'>");
                    sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:22px; background-color:#000000; color:#FFFFFF; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:16px; color:#A21403; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#F5F6F9; text-decoration:none; }");
                    sb.AppendLine(".header_white   { font-family: sans-serif; font-size:11px; background-color:#066A9D; color:#FFFFFF; text-decoration:none; }");
                    sb.AppendLine(".header_white1  { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#FFFFFF; text-decoration:none; }");
                    sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:09px; color:#1C1C1D; text-decoration:none; }");
                    sb.AppendLine(".dato_rojo      { font-family: sans-serif; font-size:09px; color:#F5240B; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color:#A0A0A0; text-decoration:none; }");
                    sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color:#FF0000; text-decoration:none; }");
                    sb.AppendLine("table { border: 1 #030303; }");
                    sb.AppendLine("</style>");
                    sb.AppendLine("</head>");
                    sb.AppendLine("<body>");
                    sb.AppendLine("<TABLE border='0' cellspacing='1' cellpadding='1' width='701' borderColor='#E0E0E0'>");
                    sb.AppendLine("<TR><TD class='titulo_general'><CENTER>RELACION DE SOBREGIROS</CENTER></TD></TR>");
                    sb.AppendLine("<TR><TD class='header_white1'><CENTER>&nbsp;</CENTER></TD></TR>");
                    sb.AppendLine($"<TR><TD class='dato_tabla' align='right'><STRONG>{DateTime.Now}</STRONG></TD></TR>");
                    sb.AppendLine("<TR><TD>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<span class='titulo_tabla' align='left'>&nbsp;</span>");
                    sb.AppendLine("<table border='1' cellspacing='1' cellpadding='1' width='100%' borderColor='#E0E0E0'>");
                    sb.AppendLine("<tr><td colspan='7' class='header_white1'>&nbsp;</td></tr>");
                    sb.AppendLine("<tr>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Cuenta</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Cliente</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Moneda</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Sobregiro</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Saldo Disponible</td>");
                    sb.AppendLine("<td class='header_white' align='CENTER'>Concepto</td>");
                    sb.AppendLine("</tr>");

                    //Realiza el recorrido de los datos
                    foreach (ComplementoSobregirosDTO OperacSobregiro in complementoSobregiro)
                    {
                        lsRed = OperacSobregiro.Monto_Debito < 0 ? "dato_rojo" : "dato_tabla";

                        sb.AppendLine("<tr>");
                        sb.AppendLine($"<td class='dato_tabla' align='CENTER'>{OperacSobregiro.Cuenta}</td>");
                        sb.AppendLine($"<td class='dato_tabla'>&nbsp;{OperacSobregiro.Nombre_Cliente}</td>");
                        sb.AppendLine($"<td class='dato_tabla' align='CENTER'>{OperacSobregiro.Currency}</td>");
                        sb.AppendLine($"<td class='{lsRed}' align='RIGHT'>{OperacSobregiro.Monto_Debito}</td>");
                        sb.AppendLine($"<td class='{lsRed}' align='RIGHT'>{OperacSobregiro.Saldo}</td>");
                        sb.AppendLine($"<td class='dato_tabla'>&nbsp;{OperacSobregiro.Concepto}</td>");
                        sb.AppendLine("</tr>");
                    }

                    lsMensaje = sb.ToString();

                    Config.EscribirLog($"INFO|ProcesarSobregirosAsync | Enviando el Correo -->  :'{email}' ");

                    _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                    _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                    _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                    var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                                ? ConstantesServicio.AvisoSobregiros
                                : _configuracion.SujetoDefault;

                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                        Para = _configuracion.CorreoParaDefault ?? email,
                        ConCopia = _configuracion.CorreoParaDefault ?? emailcc,
                        CopiaOculta = _configuracion.CorreoParaDefault ?? emailbcc,
                        Asunto = asuntoBase,
                        Mensaje = lsMensaje,
                        ArchivosAdjuntos = null
                    });

                    Config.EscribirLog("INFO|ProcesarSobregirosAsync | EmailRPA_SobreGiros|Actualizando / Borrando Pasado.. TB_T24_REPORTE_CASHPOOLING  ");

                    await _detallesTomi.ActualizaFolioSobregiro();
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarSobregirosAsync|Se presenta exception: '{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_CUENTCASHPOOL : ProcesamientoCuentaCashPool, IProcesamientoCuentaCashPool, IENT_CUENTCASHPOOL
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_CUENTCASHPOOL(Configuracion configuracion, IDetallesTomi detallesTomi)
            : base(configuracion, detallesTomi)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
        }

        public async Task ProcesarOperacionCuentaCashPoolAsync()
        {
            try
            {
                string lsMensaje  = null;
                string email = null;
                string emailcc = null;
                string emailbcc = null;
                _configuracion.ParametrosSistema?.TryGetValue("RPA_CASHPOOLING_mailTo", out email);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_CASHPOOLING_mailCc", out emailcc);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_CASHPOOLING_mailBcc", out emailbcc);
                string lsCurrency = "";
                string lsRed      = null;

                //Obtiene los atributos de los complementos de la operaciónes de cuentas cash pool
                Config.EscribirLog("INFO|ProcesarOperacionCuentaCashPoolAsync|Procesando operaciones cuentas cash pool");


                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoCuentaCashPoolDTO> complementoCuentaCash = await this.ObtencionDatosCuentaCashPool();

                //Si no pudo obtener sus complementos...
                if (complementoCuentaCash.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionCuentaCashPoolAsync | No se encontro información");
                    return;
                }
                else
                {
                    var sb = new StringBuilder();

                    sb.AppendLine("<html>");
                    sb.AppendLine("<head>");
                    sb.AppendLine("<style type='text/css'>");
                    sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:22px; background-color:#000000; color:#FFFFFF; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:16px; color:#A21403; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:11px; background-color:#A21403; color:#F5F6F9; text-decoration:none; }");
                    sb.AppendLine(".header_white   { font-family: sans-serif; font-size:11px; background-color:#FFFAFA; color:#FFFAFA; text-decoration:none; }");
                    sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:09px; color:#1C1C1D; text-decoration:none; }");
                    sb.AppendLine(".dato_rojo      { font-family: sans-serif; font-size:09px; color:#F5240B; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color:#A0A0A0; text-decoration:none; }");
                    sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color:#FF0000; text-decoration:none; }");
                    sb.AppendLine("table { border: 1 #030303; }");
                    sb.AppendLine("</style>");
                    sb.AppendLine("</head>");
                    sb.AppendLine("<body>");
                    sb.AppendLine("<TABLE border='1' cellspacing='0' cellpadding='0' width='501' borderColor='#E0E0E0'>");
                    sb.AppendLine("<TR>");
                    sb.AppendLine("<TD class='titulo_general'><CENTER>CASHPOOLING</CENTER></TD>");
                    sb.AppendLine("</TR>");
                    sb.AppendLine("<TR>");
                    sb.AppendLine("<TD class='header_white'><CENTER>&nbsp;</CENTER></TD>");
                    sb.AppendLine("</TR>");

                    sb.AppendLine(
                        $"<TR><TD class='dato_tabla' align='right'><STRONG>{DateTime.Now:dd/MM/yyyy HH:mm:ss}</STRONG></TD></TR>");

                    sb.AppendLine("<TR>");
                    sb.AppendLine("<TD>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<BR>");

                    //Realiza el recorrido de los datos
                    foreach (ComplementoCuentaCashPoolDTO OperacCashPool in complementoCuentaCash)
                    {
                        if (lsCurrency == "")
                        {
                            sb.AppendLine(
                                $"<span class='titulo_tabla' align='left'>{OperacCashPool.Currency}</span>");

                            sb.AppendLine(
                                $"<table border='0' cellspacing='0' cellpadding='0' width='500' borderColor='#E0E0E0'>" +
                                $"<tr><td colspan='7' class='header_tabla'>{OperacCashPool.Nombre_Cliente}</td></tr>" +
                                $"<tr>" +
                                $"<td class='header_white' width='50%'>&nbsp;</td>" +
                                $"<td class='header_white' width='5%'>&nbsp;</td>" +
                                $"<td class='header_white' width='10%'>&nbsp;</td>" +
                                $"<td class='header_white' width='5%'>&nbsp;</td>" +
                                $"<td class='header_white' width='10%'>&nbsp;</td>" +
                                $"<td class='header_white' width='5%'>&nbsp;</td>" +
                                $"<td class='header_white' width='15%'>&nbsp;</td>" +
                                $"</tr>");
                        } 
                        else if (OperacCashPool.Currency != lsCurrency) 
                        {
                            sb.AppendLine("</TABLE><BR><BR><BR>");

                            sb.AppendLine(
                                $"<span class='titulo_tabla' align='left'>{OperacCashPool.Currency}</span>");

                            sb.AppendLine(
                                $"<table border='0' cellspacing='0' cellpadding='0' width='500' borderColor='#E0E0E0'>" +
                                $"<tr><td colspan='7' class='header_tabla'>{OperacCashPool.Nombre_Cliente}</td></tr>" +
                                $"<tr>" +
                                $"<td class='header_white' width='50%'>&nbsp;</td>" +
                                $"<td class='header_white' width='5%'>&nbsp;</td>" +
                                $"<td class='header_white' width='10%'>&nbsp;</td>" +
                                $"<td class='header_white' width='5%'>&nbsp;</td>" +
                                $"<td class='header_white' width='10%'>&nbsp;</td>" +
                                $"<td class='header_white' width='5%'>&nbsp;</td>" +
                                $"<td class='header_white' width='15%'>&nbsp;</td>" +
                                $"</tr>");
                        }
                        lsRed = OperacCashPool.monto < 0 ? "dato_rojo" : "dato_tabla";

                        sb.AppendLine(
                                $"<tr>" +
                                $"<td class='dato_tabla' width='50%'>{OperacCashPool.Nombre_Cliente}</td>" +
                                $"<td class='dato_tabla' width='5%' align='CENTER'>&nbsp;</td>" +
                                $"<td class='dato_tabla' width='10%'>{OperacCashPool.Currency}</td>" +
                                $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                $"<td class='dato_tabla' width='10%' align='CENTER'>{OperacCashPool.Cuenta}</td>" +
                                $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                $"<td class='{lsRed}' width='15%' align='RIGHT'>{OperacCashPool.Saldo}</td>" +
                                $"</tr>");
                    }

                    sb.AppendLine("</table>");
                    sb.AppendLine("<BR><BR><span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");
                    sb.AppendLine("</TR></TD></TABLE>");
                    sb.AppendLine("</body>");
                    sb.AppendLine("</html>");

                    lsMensaje = sb.ToString();

                    Config.EscribirLog($"INFO|ProcesarOperacionCuentaCashPoolAsync | Enviando el Correo -->  :'{email}' ");

                    _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                    _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                    _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                    var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                                ? ConstantesServicio.AvisoSobregiros
                                : _configuracion.SujetoDefault;

                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                        Para = _configuracion.CorreoParaDefault ?? email,
                        ConCopia = _configuracion.CorreoParaDefault ?? emailcc,
                        CopiaOculta = _configuracion.CorreoParaDefault ?? emailbcc,
                        Asunto = asuntoBase,
                        Mensaje = lsMensaje,
                        ArchivosAdjuntos = null
                    });
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionCuentaCashPoolAsync|Se presenta exception: '{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_CUENTAFAD : ProcesamientoCuentasFAD, IProcesamientoCuentasFAD, IENT_CUENTAFAD
    {
        Configuracion    _configuracion;
        IDetallesTomi    _detallesTomi;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_CUENTAFAD(Configuracion configuracion, IDetallesTomi detallesTomi)
            : base(configuracion, detallesTomi)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
        }

        public async Task ProcesarCuentasFADAsync()
        {
            try
            {
                string email = null;
                string emailcc = null;
                string emailbcc = null;
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailTo", out email);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailCc", out emailcc);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_SOBREGIRO_mailBcc", out emailbcc);

                string lsMensaje = null;
                //Obtiene los atributos de los complementos de la operaciones de cuentas de tipo FAD
                Config.EscribirLog("INFO|ProcesarCuentasFADAsync|Procesando operaciones de sobregiros");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoCuentasFADDTO> complementoCuentaFad = await this.ObtencionDatosCuentasFAD();

                //Si no pudo obtener sus complementos...
                if (complementoCuentaFad.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarCuentasFADAsync | No se encontro información");
                    return;
                }
                else
                {
                    Config.EscribirLog($"INFO|ProcesarCuentasFADAsync|Se presenta exception: '{DateTime.Now.ToString()}'");

                    var sb = new StringBuilder();

                    sb.AppendLine("<html>");
                    sb.AppendLine("<head>");
                    sb.AppendLine("<style type='text/css'>");
                    sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:18px; color:#FF1010; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:10px; color:#0000FF; font-weight:bold; text-decoration:none; }");
                    sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:11px; background-color:#000000; color:#F5F6F9; text-decoration:none; }");
                    sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:09px; color:#1C1C1D; text-decoration:none; }");
                    sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color:#A0A0A0; text-decoration:none; }");
                    sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color:#FF0000; text-decoration:none; }");
                    sb.AppendLine("table { border: 1 #030303; }");
                    sb.AppendLine("</style>");
                    sb.AppendLine("</head>");
                    sb.AppendLine("<body>");
                    sb.AppendLine("<center><span class='titulo_general'>Cuentas FAD</span>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<BR>");
                    sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='700' borderColor='#E0E0E0'>");

                    sb.AppendLine(
                        $"<tr><td class='Nota' align='right' colspan='6'>{DateTime.Now:dd/MM/yyyy HH:mm:ss}</td></tr>");

                    sb.AppendLine("<tr>");
                    sb.AppendLine("<td class='header_tabla' width='30%'>Cliente</td>");
                    sb.AppendLine("<td class='header_tabla' width='10%'>Cuenta</td>");
                    sb.AppendLine("<td class='header_tabla' width='10%'>Producto</td>");
                    sb.AppendLine("<td class='header_tabla' width='10%'>Moneda</td>");
                    sb.AppendLine("<td class='header_tabla' width='10%'>Saldo Actual</td>");
                    sb.AppendLine("<td class='header_tabla' width='30%'>Concepto</td>");

                    sb.AppendLine("</tr>");

                    //Realiza el recorrido de los datos
                    foreach (ComplementoCuentasFADDTO OperacCuentaFad in complementoCuentaFad)
                    {
                        sb.AppendLine("<tr>");
                        sb.AppendLine(
                            $"<td class='dato_tabla' width='30%'>{OperacCuentaFad.Nombre_Cliente}</td>");

                        sb.AppendLine(
                            $"<td class='dato_tabla' width='10%' align='RIGHT'>{OperacCuentaFad.Cuenta}</td>");

                        sb.AppendLine(
                            $"<td class='dato_tabla' width='10%'>{OperacCuentaFad.Desc_Cuenta}</td>");

                        sb.AppendLine(
                            $"<td class='dato_tabla' width='10%' align='CENTER'>{OperacCuentaFad.Currency}</td>");

                        sb.AppendLine(
                            $"<td class='dato_tabla' width='10%' align='right'>{OperacCuentaFad.Saldo}</td>");

                        sb.AppendLine(
                            $"<td class='dato_tabla' width='30%'>&nbsp;&nbsp;{OperacCuentaFad.Last_Txr}</td>");
                        sb.AppendLine("</tr>");
                    }

                    sb.AppendLine("</table>");
                    sb.AppendLine("<BR><BR><span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");
                    sb.AppendLine("</center>");
                    sb.AppendLine("</body>");
                    sb.AppendLine("</html>");

                    lsMensaje = sb.ToString();

                    Config.EscribirLog($"INFO|ProcesarCuentasFADAsync | Enviando el Correo -->  :'{email}' ");

                    _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                    _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                    _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                    var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                               ? ConstantesServicio.AvisoSobregiros
                               : _configuracion.SujetoDefault;

                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                        Para = _configuracion.CorreoParaDefault ?? email,
                        ConCopia = _configuracion.CorreoParaDefault ?? emailcc,
                        CopiaOculta = _configuracion.CorreoParaDefault ?? emailbcc,
                        Asunto = asuntoBase,
                        Mensaje = lsMensaje,
                        ArchivosAdjuntos = null
                    });

                    Config.EscribirLog($"INFO| ProcesarCuentasFADAsync| Actualizando TB_T24_REPORTE_CTASFAD ");

                    await _detallesTomi.ActualizaFolioCuentasFad();

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ProcesarCuentasFADAsync en Ejecucion: '{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_FIELSPID : ProcesamientoFielSPID, IProcesamientoFielSPID, IENT_FIELSPID
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesVistas _detallesVistas;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_FIELSPID(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesVistas detallesVistas)
            : base(configuracion, detallesTomi)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesVistas = detallesVistas;
        }

        public async Task ProcesarFielesSPIDAsync()
        {
            try
            {
                string lsMensaje = null;
                int liValidos = 0;
                int liTotal = 0;
                int liRechazados = 0;
                int liPendientes = 0;
                int liInvalidos = 0;
                int licontador = 0;

                string email = null;
                string emailcc = null;
                string emailbcc = null;
                _configuracion.ParametrosSistema?.TryGetValue("RPA_FIELES_mailTo", out email);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_FIELES_mailCc", out emailcc);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_FIELES_mailBcc", out emailbcc);

                //Obtiene los atributos de los complementos de la operaciones de sobregiros
                Config.EscribirLog("INFO|ProcesarFielesSPIDAsync|Procesando operaciones de sobregiros");

                var listvt24 = await _detallesVistas.ObtenerDataVwT24Customer(ConstantesServicio.uspEmailobtenerCustomer);
                var listVal  = await this.ObtencionDatosFielSPID();

                var listado = from a in listvt24
                              join b in listVal
                                  on a.CUSTOMER_CODE equals b.CDL
                              group b by new
                              {
                                  b.Dia,
                                  b.Estado_Consulta
                              }
                              into g
                              select new ComplementoFielSPIDDTO()
                              {
                                  Dia = g.Key.Dia,
                                  Estado_Consulta = g.Key.Estado_Consulta,
                                  Total = g.Count()
                              };

                var complementoFielSpid = listado.ToList();
                if (complementoFielSpid.Count == 0)
                {
                    Config.EscribirLog("INFO|ProcesarFielesSPIDAsync | No se encontro información");
                    return;
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:22px; background-color:#FFFFFF; color:#000000; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:16px; color:#A21403; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:11px; background-color:#A21403; color:#F5F6F9; text-decoration:none; }");
                sb.AppendLine(".header_white   { font-family: sans-serif; font-size:11px; background-color:#06A4F3; color:#FFFAFA; text-decoration:none; }");
                sb.AppendLine(".header_white1  { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#FFFFFF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:10px; color:#1C1C1D; text-decoration:none; }");
                sb.AppendLine(".dato_rojo      { font-family: sans-serif; font-size:10px; color:#F5240B; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color:#A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color:#FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: 1 #030303; }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<TABLE border='0' cellspacing='0' cellpadding='0' width='701' borderColor='#E0E0E0'>");
                sb.AppendLine("<TR>");
                sb.AppendLine("<TD class='titulo_general'>");
                sb.AppendLine("<CENTER>Validacion&nbsp;&nbsp;Banxico&nbsp;-&nbsp;&nbsp;&nbsp;FIELES&nbsp;&nbsp;de&nbsp;Clientes&nbsp;&nbsp;SPID</CENTER>");
                sb.AppendLine("</TD>");
                sb.AppendLine("</TR>");
                sb.AppendLine("<TR>");
                sb.AppendLine("<TD class='header_white1'><CENTER>&nbsp;</CENTER></TD>");
                sb.AppendLine("</TR>");
                sb.AppendLine(
                    $"<TR><TD class='dato_tabla' align='right'><STRONG>{DateTime.Now:dd/MM/yyyy HH:mm:ss}</STRONG></TD></TR>");
                sb.AppendLine("<TR>");
                sb.AppendLine("<TD>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");

                if (complementoFielSpid.Count > 0)
                {
                    sb.AppendLine(
                                "<span class='titulo_tabla' align='left'>Detalle - Revision FIEL Banxico</span>");
                    sb.AppendLine(
                                "<table border='0' cellspacing='2' cellpadding='2' width='100%' borderColor='#E0E0E0'>" +
                                "<tr>" +
                                "<td class='header_white' width='50%'>Fecha de Validacion</td>" +
                                "<td class='header_white1' width='5%'>&nbsp;</td>" +
                                "<td class='header_white' width='20%' align='CENTER'>Estatus</td>" +
                                "<td class='header_white1' width='5%'>&nbsp;</td>" +
                                "<td class='header_white' width='15%' align='CENTER'>Total Registros</td>" +
                                "<td class='header_white1' width='5%'>&nbsp;</td>" +
                                "</tr>");

                    //-- inicializa Contadores
                    licontador   = 0;
                    liValidos    = 0;
                    liRechazados = 0;
                    liPendientes = 0;
                    liInvalidos  = 0;

                    //Realiza el recorrido de los datos
                    foreach (ComplementoFielSPIDDTO OperacFielSPD in complementoFielSpid)
                    {
                        if (OperacFielSPD.Estado_Consulta == ConstantesServicio.estadoFielValido)
                        {
                            liValidos += OperacFielSPD.Total;
                        }
                        else if (OperacFielSPD.Estado_Consulta == ConstantesServicio.estadoFielRevocado || OperacFielSPD.Estado_Consulta == ConstantesServicio.estadoFielCaduco)
                        {
                            liRechazados += OperacFielSPD.Total;
                        }
                        else if (OperacFielSPD.Estado_Consulta == ConstantesServicio.estadoFielInvalido)
                        {
                            liInvalidos += OperacFielSPD.Total;
                        } 
                        else
                        {
                            liPendientes += OperacFielSPD.Total;
                        }

                        licontador ++;

                        sb.AppendLine(
                                    $"<tr>" +
                                    $"<td class='dato_tabla' width='50%'>{OperacFielSPD.Dia}</td>" +
                                    $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                    $"<td class='dato_tabla' width='20%' align='CENTER'>{OperacFielSPD.Estado_Consulta}</td>" +
                                    $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                    $"<td class='dato_tabla' width='15%' align='right'>{OperacFielSPD.Total}</td>" +
                                    $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                    $"</tr>");
                    }

                    sb.AppendLine("</table>");

                    //-- Aplica el tipo de Correo
                    if (liTotal == 0)
                    {
                        if (licontador > 0)
                        {
                            
                            var listValDet = await this.ObtencionDatosFielSPIDDet();
                            var listvt24Det = await _detallesVistas.ObtenerDataVwT24Customer(ConstantesServicio.uspEmailobtenerCustomer);
                            var listadoDet = from a in listvt24Det
                                          join b in listValDet
                                              on a.CUSTOMER_CODE equals b.CDL
                                          select new ComplementoFielSPIDDetDTO()
                                          {
                                              CDL    = b.CDL,
                                              NOMBRE = a.NOMBRE,
                                              FIEL   = b.FIEL,
                                              ESTADO_CONSULTA = b.ESTADO_CONSULTA,
                                              FECHACONSULTA   = b.FECHACONSULTA,
                                              WS_RESP_CODE    = b.WS_RESP_CODE
                                          };

                            var complementoFielSpidDet = listadoDet.ToList();

                            //List<ComplementoFielSPIDDetDTO> complementoFielSpidDet = await this.ObtencionDatosFielSPIDDet();

                            if (complementoFielSpidDet.Count > 0)
                            {
                                Config.EscribirLog("INFO|ProcesarFielesSPIDAsync | No se encontro información de detalle");

                                sb.AppendLine("<br><br><br><span class='titulo_tabla' align='left'>Detalle Fieles Revocadas / Caducas</span>");
                                sb.AppendLine("<table border='0' cellspacing='1' cellpadding='1' width='100%' borderColor='#E0E0E0'>");
                                sb.AppendLine("<tr>");
                                sb.AppendLine("<td class='header_white' align='CENTER'>CDL</td>");
                                sb.AppendLine("<td class='header_white' align='CENTER'>Nombre</td>");
                                sb.AppendLine("<td class='header_white' align='CENTER'>Fiel</td>");
                                sb.AppendLine("<td class='header_white' align='CENTER'>Fecha</td>");
                                sb.AppendLine("<td class='header_white' align='CENTER'>Estatus Fiel</td>");
                                sb.AppendLine("<td class='header_white' align='CENTER'>WS Resp</td>");
                                sb.AppendLine("</tr>");


                                //Realiza el recorrido de los datos de detalle
                                foreach (ComplementoFielSPIDDetDTO OperacFielSPDDet in complementoFielSpidDet)
                                {
                                    sb.AppendLine(
                                                     $"<tr>" +
                                                     $"<td class='dato_tabla' align='CENTER'>{OperacFielSPDDet.CDL}</td>" +
                                                     $"<td class='dato_tabla' align='CENTER'>{OperacFielSPDDet.NOMBRE}</td>" +
                                                     $"<td class='dato_tabla' align='CENTER'>{OperacFielSPDDet.FIEL}</td>" +
                                                     $"<td class='dato_tabla' align='CENTER'>{OperacFielSPDDet.FECHACONSULTA}</td>" +
                                                     $"<td class='dato_tabla' align='CENTER'><STRONG>{OperacFielSPDDet.ESTADO_CONSULTA}</STRONG></td>" +
                                                     $"<td class='dato_tabla' align='CENTER'>{OperacFielSPDDet.WS_RESP_CODE}</td>" +
                                                     $"</tr>");


                                }
                                sb.AppendLine("</table>");
                            }

                            if (liPendientes > 0)
                            {
                                Config.EscribirLog($"INFO|ProcesarFielesSPIDAsync | No esta Completo la Revision de Fieles -> '{ConstantesServicio.quePasoFieles}'");
                                sb.AppendLine("<span class='titulo_tabla' align='left'>" + ConstantesServicio.quePasoFieles + "</span>");
                            }
                            else if (liInvalidos > 0)
                            {
                                Config.EscribirLog($"INFO|ProcesarFielesSPIDAsync | Registros INVALIDOS == > '{ConstantesServicio.quePasoFieles}'");
                                sb.AppendLine("<span class='titulo_tabla' align='left'>" + ConstantesServicio.quePasoFieles + "</span>");

                                Config.EscribirLog("INFO|ProcesarFielesSPIDAsync |Inicializa Registros Invalidos !! a Reintentar la Validacion Banxico");
                                await _detallesTomi.ActualizaFolioFielInvalidas();
                            }
                            else
                            {
                                Config.EscribirLog("INFO|ProcesarFielesSPIDAsync | Genera Marca de que ya se inicializo y NO vuelva a Procesarse !!");
                                await _detallesTomi.ActualizaFolioFielValidas(liValidos, liRechazados);
                            }
                        }
                        else
                        {
                            Config.EscribirLog($"INFO|ProcesarFielesSPIDAsync | No esta Completo la REvision de Fieles -> '{ConstantesServicio.quePasoFieles}'");
                            sb.AppendLine("<span class='titulo_tabla' align='left'>" + ConstantesServicio.quePasoFieles + "</span>");
                        }

                        sb.AppendLine("<BR><BR>");
                        sb.AppendLine("<span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");
                        sb.AppendLine("</TR>");
                        sb.AppendLine("</TD>");
                        sb.AppendLine("</TABLE>");
                        sb.AppendLine("</body>");
                        sb.AppendLine("</html>");
                        lsMensaje = sb.ToString();
                    }

                    if (licontador > 0)
                    {
                        Config.EscribirLog("INFO|ProcesarFielesSPIDAsync | Preparacion de Envio de la Notificacion de Correo");
                        _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                        _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                        _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                        var asuntoBase = string.IsNullOrWhiteSpace(_configuracion.SujetoDefault)
                                ? ConstantesServicio.AvisoFielesSPID
                                : _configuracion.SujetoDefault;

                        await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                        {
                            CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                            NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                            Para = _configuracion.CorreoParaDefault ?? email,
                            ConCopia = _configuracion.CorreoParaDefault ?? emailcc,
                            CopiaOculta = _configuracion.CorreoParaDefault ?? emailbcc,
                            Asunto = asuntoBase,
                            Mensaje = lsMensaje,
                            ArchivosAdjuntos = null
                        });
                        Config.EscribirLog("INFO|ProcesarFielesSPIDAsync | Envio de la Notificacion de Correo");
                    }

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarFielesSPIDAsync|Se presenta exception: '{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_INIFIELES : ProcesamientoInicioFieles, IProcesamientoInicioFieles, IENT_INIFIELES
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IDetallesVistas _detallesVistas;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_INIFIELES(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesVistas detallesVistas)
            : base(configuracion, detallesTomi)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _detallesVistas = detallesVistas;
        }

        public async Task ProcesarInicioFielesSPIDAsync()
        {
            try
            {
                string lsMensaje = null;
                   int licontador = 0;

                string email = null;
                string emailcc = null;
                string emailbcc = null;
                _configuracion.ParametrosSistema?.TryGetValue("RPA_FIELES_mailTo", out email);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_FIELES_mailCc", out emailcc);
                _configuracion.ParametrosSistema?.TryGetValue("RPA_FIELES_mailBcc", out emailbcc);

                var listvt24 = await _detallesVistas.ObtenerDataVwT24Customer(ConstantesServicio.uspEmailobtenerCustomer);
                var listVal = await this.ObtenerDataFielValidos();

                int Total = (from a in listvt24
                             join b in listVal
                                 on a.CUSTOMER_CODE equals b.CDL
                             select b).Count();

                await _detallesTomi.InsertarFolioInicioFiel(Total);

                var listValIni = await this.ObtencionDatosFielSPID();

                Config.EscribirLog("INFO|ProcesarInicioFielesSPIDAsync | Validando si el Proceso de inicializacion es VALIDO !!");

                var listado = from a in listvt24
                              join b in listValIni
                                  on a.CUSTOMER_CODE equals b.CDL
                              group b by new
                              {
                                  b.Dia,
                                  b.Estado_Consulta
                              }
                              into g
                              select new ComplementoFielSPIDDTO()
                              {
                                  Dia = g.Key.Dia,
                                  Estado_Consulta = g.Key.Estado_Consulta,
                                  Total = g.Count()
                              };

                var complementoIniFielSpid = listado.ToList();
                if (complementoIniFielSpid.Count == 0)
                {
                    Config.EscribirLog("INFO|ProcesarInicioFielesSPIDAsync | No se encontro información");
                    return;
                }

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:22px; background-color:#FFFFFF; color:#000000; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:16px; color:#A21403; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:11px; background-color:#A21403; color:#F5F6F9; text-decoration:none; }");
                sb.AppendLine(".header_white   { font-family: sans-serif; font-size:11px; background-color:#06A4F3; color:#FFFAFA; text-decoration:none; }");
                sb.AppendLine(".header_white1  { font-family: sans-serif; font-size:11px; background-color:#FFFFFF; color:#FFFFFF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:10px; color:#1C1C1D; text-decoration:none; }");
                sb.AppendLine(".dato_rojo      { font-family: sans-serif; font-size:10px; color:#F5240B; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color:#A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color:#FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: 1 #030303; }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");

                sb.AppendLine("<TABLE border='0' cellspacing='0' cellpadding='0' width='701' borderColor='#E0E0E0'>");

                sb.AppendLine("<TR>");
                sb.AppendLine("<TD class='titulo_general'><CENTER>Inicio de dia SPID - Inicializa Estatus de Fieles</CENTER></TD>");
                sb.AppendLine("</TR>");

                sb.AppendLine("<TR>");
                sb.AppendLine("<TD class='header_white1'><CENTER>&nbsp;</CENTER></TD>");
                sb.AppendLine("</TR>");

                sb.AppendLine(
                    $"<TR><TD class='dato_tabla' align='right'><STRONG>{DateTime.Now:dd/MM/yyyy HH:mm:ss}</STRONG></TD></TR>");

                sb.AppendLine("<TR>");
                sb.AppendLine("<TD>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");



                if (complementoIniFielSpid.Count > 0)
                {
                    sb.AppendLine(
                    "<span class='titulo_tabla' align='left'>Detalle - Revision FIEL Banxico</span>");

                    sb.AppendLine(
                        "<table border='0' cellspacing='2' cellpadding='2' width='100%' borderColor='#E0E0E0'>" +
                        "<tr>" +
                        "<td class='header_white' width='50%'>Fecha de Validacion</td>" +
                        "<td class='header_white1' width='5%'>&nbsp;</td>" +
                        "<td class='header_white' width='20%' align='CENTER'>Estatus</td>" +
                        "<td class='header_white1' width='5%'>&nbsp;</td>" +
                        "<td class='header_white' width='15%' align='CENTER'>Total Registros</td>" +
                        "<td class='header_white1' width='5%'>&nbsp;</td>" +
                        "</tr>");
                    
                    //-- inicializa Contadores
                    licontador   = 0;

                    //Realiza el recorrido de los datos
                    foreach (ComplementoFielSPIDDTO OperacFielSPD in complementoIniFielSpid)
                    {
                        licontador++;
                        sb.AppendLine(
                                     $"<tr>" +
                                     $"<td class='dato_tabla' width='50%'>{OperacFielSPD.Dia}</td>" +
                                     $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                     $"<td class='dato_tabla' width='20%' align='CENTER'>{OperacFielSPD.Estado_Consulta}</td>" +
                                     $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                     $"<td class='dato_tabla' width='15%' align='right'>{OperacFielSPD.Total}</td>" +
                                     $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                     $"</tr>");
                    }
                    sb.AppendLine("</table>");

                    if (complementoIniFielSpid.Count > 0)
                    {
                        var listValFiel = await this.ObtencionDatosValidasFielSPID();

                        var listadoVal = from a in listvt24
                                         join b in listValFiel
                                          on a.CUSTOMER_CODE equals b.CDL
                                      group b by new
                                      {
                                          b.Dia,
                                          b.FIEL_STATUS
                                      }
                                                    into g
                                      select new ComplementoIniFielSPIDDTO()
                                      {
                                          Dia             = g.Key.Dia,
                                          FIEL_STATUS     = g.Key.FIEL_STATUS,
                                          Total           = g.Count()
                                      };

                        var complementoFielSpid = listadoVal.ToList();

                        sb.AppendLine("<br><br><span class='titulo_tabla' align='left'>Detalle para Validar FIEL en Banxico</span>");
                        sb.AppendLine("<table border='1' cellspacing='1' cellpadding='1' width='100%' borderColor='#E0E0E0'>");
                        sb.AppendLine("<tr>");
                        sb.AppendLine("<td class='header_white' width='50%' align='CENTER'>Fecha Validacion</td>");
                        sb.AppendLine("<td class='header_white1' width='5%'>&nbsp;</td>");
                        sb.AppendLine("<td class='header_white' width='20%' align='CENTER'>Estatus Fiel</td>");
                        sb.AppendLine("<td class='header_white1' width='5%'>&nbsp;</td>");
                        sb.AppendLine("<td class='header_white' width='15%' align='CENTER'>Total Registros</td>");
                        sb.AppendLine("<td class='header_white1' width='5%'>&nbsp;</td>");
                        sb.AppendLine("</tr>");

                        //Realiza el recorrido de los datos de detalle
                        foreach (ComplementoIniFielSPIDDTO OperacFielSPDDet in complementoFielSpid)
                        {
                            sb.AppendLine(
                                    $"<tr>" +
                                    $"<td class='dato_tabla' width='50%'>{OperacFielSPDDet.Dia}</td>" +
                                    $"<td class='dato_tabla' width='5%' align='CENTER'>&nbsp;</td>" +
                                    $"<td class='dato_tabla' width='20%'>{OperacFielSPDDet.FIEL_STATUS}</td>" +
                                    $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                    $"<td class='dato_tabla' width='15%' align='right'>{OperacFielSPDDet.Total}</td>" +
                                    $"<td class='dato_tabla' width='5%'>&nbsp;</td>" +
                                    $"</tr>");
                        }

                        sb.AppendLine("</table>");

                        await _detallesTomi.ActualizaFolioFielValidasRechazadas();
                        
                        Config.EscribirLog("INFO|ProcesarInicioFielesSPIDAsync | Prepara Email de Envio y notificacion!!");
                        
                        sb.AppendLine();
                        sb.AppendLine(
                            "<BR><BR><span class='Nota'><B>NOTA:</B> Este mail es solo informativo. Favor de no contestar este mail.</span>");
                        sb.AppendLine("</TR></TD></TABLE>");
                        sb.AppendLine("</body>");
                        sb.AppendLine("</html>");

                        lsMensaje = sb.ToString();

                        _configuracion.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                        _configuracion.SujetoDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                        _configuracion.CorreoSenderTest = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);


                        await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                        {
                            CorreoOrigen = _configuracion.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                            NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                            Para = _configuracion.CorreoParaDefault ?? email,
                            ConCopia = _configuracion.CorreoParaDefault ?? emailcc,
                            CopiaOculta = _configuracion.CorreoParaDefault ?? emailbcc,
                            Asunto = _configuracion.SujetoDefault ?? ConstantesServicio.AvisoFielesINISPID,
                            Mensaje = lsMensaje,
                            ArchivosAdjuntos = null
                        });

                        Config.EscribirLog("INFO|ProcesarInicioFielesSPIDAsync | Envio de la Notificacion de Correo");
                    }

                }

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarInicioFielesSPIDAsync|Se presenta exception: '{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_OPERACFCM : ProcesamientoOperacFcm, IProcesamientoOperacFcm, IENT_OPERACFCM
    {
        Configuracion    _configuracion;
        IDetallesTomi    _detallesTomi;
        IDetallesTomser _detallesTomser;
        IENT_ENVFCM     _entEnvFcm;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_OPERACFCM(Configuracion configuracion, IDetallesTomi detallesTomi, IDetallesTomser detallesTomser, IENT_ENVFCM entEnvFcm)
            : base(configuracion, detallesTomi, detallesTomser)
        {
            _configuracion   = configuracion;
            _detallesTomi    = detallesTomi;
            _detallesTomser  = detallesTomser;
            _entEnvFcm       = entEnvFcm;
        }
        public async Task ProcesarOperacionFcmAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operaciónes de tipo FCM
                Config.EscribirLog("INFO|ProcesarOperacionFcmAsync|Procesando operaciones de tipo FCM ");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoFcmDTO> complementoFcm = await this.ObtencionDatosOperacFcm();

                //Si no pudo obtener sus complementos...
                if (complementoFcm.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionFcmAsync | No se encontro información");
                    return;
                }

                //Realiza el recorrido de los datos
                foreach (ComplementoFcmDTO OperacFcm in complementoFcm)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionFcmAsync|Busca identificador del cliente por el usuario: '{OperacFcm.NF_FOLIO}'");
                    List<ComplementoOperaFcmDTO> complementoOperaFcm = await this.ObtencionDatosOperacDetaFcm(OperacFcm.NF_FOLIO, OperacFcm.TIPO_OPERACION);
                    Config.EscribirLog($"INFO|ProcesarOperacionFcmAsync|Busca identificador del cliente por el usuario: '{OperacFcm.NF_FOLIO}'");

                    //Si no pudo obtener sus complementos de las operaciones FCM...
                    if (complementoOperaFcm.Count is 0)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionFcmAsync | No se encontro información de detalle con los datos: Folio: '{OperacFcm.NF_FOLIO + "Tipo Operacion: " + OperacFcm.TIPO_OPERACION}");
                        continue;
                    }

                    foreach (ComplementoOperaFcmDTO OperaFcm in complementoOperaFcm)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionFcmAsync|Busca informaci[on de contacto: '{OperacFcm.NF_FOLIO}'");
                        bool resultado = await _entEnvFcm.EnviaAvisosFcm(OperacFcm.TIPO_OPERACION, OperacFcm.DES_PARAMETRO, OperacFcm.DES_PARAMETRO2, OperaFcm);

                        if (resultado)
                        {
                            //Realiza la actualización de estatus de la solicitud
                            await _detallesTomi.ActualizaFolioFcm(OperacFcm.NF_FOLIO, OperacFcm.TIPO_OPERACION);
                            Config.EscribirLog($"INFO|ProcesarOperacionFcmAsync|Se termina procesamiento de folio: '{OperacFcm.NF_FOLIO}'");
                        }
                        else
                        {
                            Config.EscribirLog($"INFO|ProcesarOperacionFcmAsync|Error en procesamiento de folio: '{OperacFcm.NF_FOLIO}'");
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionFcmAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_NOTIFOREX : ProcesamientoNotiForex, IProcesamientoNotiForex, IENT_NOTIFOREX
    {
        Configuracion    _configuracion;
        IDetallesTomi    _detallesTomi;
        IENT_ENVIANOTIFOREX _entEnvioForex;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_NOTIFOREX(Configuracion configuracion, IDetallesTomi detallesTomi, IENT_ENVIANOTIFOREX entEnvioForex)
            : base(configuracion, detallesTomi)
        {
            _configuracion   = configuracion;
            _detallesTomi  = detallesTomi;
            _entEnvioForex = entEnvioForex;
        }
        public async Task ProcesarOperacionNotificacionesForexAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de las notificaciones FOREX...
                Config.EscribirLog("INFO|ProcesarOperacionNotificacionesForexAsync|Procesando notificaciones FOREX");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoNotiForexDTO> complementoNotiForexDTO = await this.ObtencionDatosNotiForex();

                //Si no pudo obtener sus complementos...
                if (complementoNotiForexDTO.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionNotificacionesForexAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoNotiForexDTO NotiForex in complementoNotiForexDTO)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionNotificacionesForexAsync|Preparando envio del folio: '{NotiForex.ID_AUTORIZACION}'");
                    Config.EscribirLog($"INFO|ProcesarOperacionNotificacionesForexAsync|Correos = ' {NotiForex.CORREO}'");
                    //Realiza el envio de las notificaciones FOREX
                    bool resultado = await _entEnvioForex.EnviarNotificacionesForex(NotiForex.CORREO, ConstantesServicio.MAIL_CC_NOTIFOREX, NotiForex);

                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioNotiForex(NotiForex.ID_AUTORIZACION);
                        Config.EscribirLog($"INFO|ProcesarOperacionNotificacionesForexAsync|Se termina procesamiento de folio:. '{NotiForex.ID_AUTORIZACION}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionNotificacionesForexAsync|Error en procesamiento de folio: '{NotiForex.ID_AUTORIZACION}'");
                    }
                }

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionNotificacionesForexAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.Servicios
{
    public class ENT_PRENOTIFOREX : ProcesamientoPreNotiForex, IProcesamientoPreNotiForex, IENT_PRENOTIFOREX
    {
        Configuracion _configuracion;
        IDetallesTomi _detallesTomi;
        IENT_ENVIANOTIFOREX _entEnvioForex;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_PRENOTIFOREX(Configuracion configuracion, IDetallesTomi detallesTomi, IENT_ENVIANOTIFOREX entEnvioForex)
            : base(configuracion, detallesTomi)
        {
            _configuracion = configuracion;
            _detallesTomi = detallesTomi;
            _entEnvioForex = entEnvioForex;
        }
        public async Task ProcesarOperacionPreNotificacionesForexAsync()
        {
            try
            {
                //Obtiene los atributos de los complementos de la operación de las prenotificaciones FOREX...
                Config.EscribirLog("INFO|ProcesarOperacionPreNotificacionesForexAsync|Procesando pre notificaciones FOREX");

                //Obtiene los CDA de los complementos de la operación...
                List<ComplementoNotiForexDTO> complementoNotiForexDTO = await this.ObtencionDatosPreNotiForex();

                //Si no pudo obtener sus complementos...
                if (complementoNotiForexDTO.Count is 0)
                {
                    Config.EscribirLog("INFO|ProcesarOperacionPreNotificacionesForexAsync | No se encontro información");
                    return;
                }

                foreach (ComplementoNotiForexDTO NotiForex in complementoNotiForexDTO)
                {
                    Config.EscribirLog($"INFO|ProcesarOperacionPreNotificacionesForexAsync|Preparando envio del folio: '{NotiForex.ID_AUTORIZACION}'");
                    Config.EscribirLog($"INFO|ProcesarOperacionPreNotificacionesForexAsync|Correos = ' {NotiForex.CORREO}'");
                    //Realiza el envio de las notificaciones FOREX
                    bool resultado = await _entEnvioForex.EnviarPreNotificacionesForex(NotiForex.CORREO, ConstantesServicio.MAIL_CC_NOTIFOREX, NotiForex);

                    if (resultado)
                    {
                        await _detallesTomi.ActualizaFolioPreNotiForex(NotiForex.ID_AUTORIZACION);
                        Config.EscribirLog($"INFO|ProcesarOperacionPreNotificacionesForexAsync|Se termina procesamiento de folio:. '{NotiForex.ID_AUTORIZACION}'");
                    }
                    else
                    {
                        Config.EscribirLog($"ERROR|ProcesarOperacionPreNotificacionesForexAsync|Error en procesamiento de folio: '{NotiForex.ID_AUTORIZACION}'");
                    }
                }

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionNotificacionesForexAsync|Se presenta exception:'{ex.ToString()}'");
            }
        }
    }
}

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using AplicacionEmail.Interfaces;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Threading.Tasks;
using System.Globalization;
using System.Text;

namespace AplicacionEmail.Servicios
{
    public class ENT_ENVIANOTIFOREX : IENT_ENVIANOTIFOREX
    {
        private Configuracion _configuracionBase;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ENT_ENVIANOTIFOREX(Configuracion configuracion) {
            _configuracionBase = configuracion;
        }

        public async  Task<bool> EnviarNotificacionesForex(string mailTo, string mailCC, ComplementoNotiForexDTO complementoNotiForex)
        {
            try
            {
                if (complementoNotiForex == null)
                    throw new ArgumentNullException(nameof(complementoNotiForex));
                string lsFechaOperacion   = null;
                string lsFechaLiquidacion = null;
                string lsMensaje = null;
                string lsNombreTransferencia = "FX" + complementoNotiForex.PRODUCTO;

                lsFechaOperacion = complementoNotiForex.FECHACAPTURA.ToString("yyyyMMdd");
                lsFechaLiquidacion = complementoNotiForex.FECHAVALOR.ToString("yyyyMMdd");


                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");
                sb.AppendLine("<center><span class='titulo_general'>NOTIFICACIONES MUFG Bank México (NOTIFICATIONS MUFG Bank Mexico)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Estimado Cliente (Dear Client):</span>");
                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='800' borderColor='#E0E0E0'>");
                sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Le notificamos que la operación que en seguida se describe se ha ejecutado y registrado por MUFG Bank México, S.A., Institución de Banca Múltiple Filial:</td></tr>");
                sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Hereby we notice that operation described herein has been executed and registered by MUFG Bank Mexico, S.A., Institución de Banca Múltiple Filial:</td></tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Operación (Transaction):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(lsNombreTransferencia)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Tipo (Class): **</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.TIPOOPERACION)}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha Operación (Transaction Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFechaOperacion}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha de Liquidación (Value Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFechaLiquidacion}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Hora Operación (Transaction Time):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.HORACAPTURA)}</td></tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Cliente (Customer):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.NOMBRE)}</td></tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Divisa a entregar (Currency to be delivered):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.DIVISAENTREGACLIENTE)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Divisa a recibir (Currency to be received):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.DIVISAADQUIERECLIENTE)}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Importe a entregar (Amount to be delivered):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.MONTOENTREGACLIENTE.ToString("C"))}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Importe a recibir (Amount to be received):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.MONTOADQUIERECLIENTE.ToString("C"))}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cuenta de Débito (Debit Account):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.CUENTAENTREGACLIENTE)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cuenta de Crédito (Credit Account):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.CUENTAADQUIERECLIENTE)}</td>" +
                    $"</tr>");
                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Tipo de Cambio (Exchange Rate):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.ECCLIENTEREAL)}</td></tr>");
                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;No. de Autorización (Deal Number):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.FOLIO_TAS)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Plaza (Location):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(ConstantesServicio.plaza)}</td>" +
                    $"</tr>");

                sb.AppendLine("<tr><td class='dato_tabla' width='100%' colspan='4' align='right'>&nbsp;** Esta operación se deberá leer en sentido del Banco.</td></tr>");
                sb.AppendLine("<tr><td class='dato_tabla' width='100%' colspan='4' align='right'>&nbsp;*** Esta operación no genera ninguna comisión.</td></tr>");
                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>AVISO LEGAL:</B> A través de éste correo electrónico se le notifica que MUFG Bank México, S.A., Institución de Banca Múltiple Filial (indistintamente, \"MUFG\" o el \"Banco\") ha ejecutado y registrado la operación antes señalada, debidamente instruida por su representante, con cargo a la cuenta y/o número de crédito indicados, según sea el caso. El contenido de este correo y su envío, se realiza en cumplimiento de las disposiciones normativas aplicables y no representa un comprobante fiscal de la operación.</p></span>");
                sb.AppendLine("<span class='Nota'><p>La fecha y hora de este mensaje puede variar respecto a la fecha y hora real de la ejecución de la operación debido, entre otras causas, a la zona horaria programada en su computadora o la configuración de sus servicios de correo. Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de la misma queda estrictamente prohibida.</p></span>");
                sb.AppendLine("<BR><BR><span class='Nota'><p><B>LEGAL NOTICE:</B> Through communication you are noticed that MUFG Bank Mexico, S.A. Institución de Banca Múltiple Filial (indistinctively \"MUFG\" or the \"Bank\") has executed and registered the above mentioned transaction, duly instructed by your legal representative.</p></span>");
                sb.AppendLine("<span class='Nota'><p>Hour and date in this message may vary in connection to the real date and time of the operation execution due to, among other causes, to the time zone programed in your computer or your email service configuration.</p></span>");
                sb.AppendLine("</html>");

                lsMensaje = sb.ToString();



                _configuracionBase.CorreoParaDefault = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var asunto = string.IsNullOrEmpty(_configuracionBase.SujetoDefault)
                              ? ConstantesServicio.AvisoNotificacion
                              : $"{ConstantesServicio.AvisoNotificacion}[{_configuracionBase.SujetoDefault}]";

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? mailTo,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? mailCC,
                    CopiaOculta = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = asunto,
                    Mensaje = lsMensaje,
                    ArchivosAdjuntos = null
                });
                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoAvisoEnvioNotiForex : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeAvisoEnvioNotifForez,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }

        public async Task<bool> EnviarPreNotificacionesForex(string mailTo, string mailCC, ComplementoNotiForexDTO complementoNotiForex)
        {
            try
            {
                string lsFechaOperacion = null;
                string lsFechaLiquidacion = null;
                string lsMensaje = null;
                string lsNombreTransferencia = "FX" + complementoNotiForex.PRODUCTO;

                lsFechaOperacion = complementoNotiForex.FECHACAPTURA.ToString("yyyyMMdd");
                lsFechaLiquidacion = complementoNotiForex.FECHAVALOR.ToString("yyyyMMdd");
                if (complementoNotiForex == null)
                    throw new ArgumentNullException(nameof(complementoNotiForex));
                string lsFechaOperacion   = null;

                var sb = new StringBuilder();

                sb.AppendLine("<html>");
                sb.AppendLine("<head>");
                sb.AppendLine("<style type='text/css'>");
                sb.AppendLine(".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }");
                sb.AppendLine(".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }");
                sb.AppendLine(".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }");
                sb.AppendLine(".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }");
                sb.AppendLine(".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }");
                sb.AppendLine(".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }");
                sb.AppendLine("table { border: thin solid #E0E0E0 }");
                sb.AppendLine("</style>");
                sb.AppendLine("</head>");
                sb.AppendLine("<body>");

                sb.AppendLine("<center><span class='titulo_general'>PRE-NOTIFICACIONES MUFG Bank México (PRE-NOTIFICATIONS MUFG Bank Mexico)</span>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<BR>");
                sb.AppendLine("<span class='titulo_tabla'>Estimado Cliente (Dear Client):</span>");

                sb.AppendLine("<table border='1' cellspacing='0' cellpadding='0' width='800' borderColor='#E0E0E0'>");

                sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Le notificamos que la operación que en seguida se describe se ha solicitado a MUFG Bank México, S.A., Institución de Banca Múltiple Filial:</td></tr>");

                sb.AppendLine("<tr><td class='header_tabla' width='100%' colspan='4'>Hereby we notice that operation described herein has been requested to MUFG Bank Mexico, S.A., Institución de Banca Múltiple Filial:</td></tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Operación (Transaction):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(lsNombreTransferencia)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Tipo (Class): **</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.TIPOOPERACION)}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha Operación (Transaction Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFechaOperacion}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Fecha de Liquidación (Value Date):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{lsFechaLiquidacion}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Hora Operación (Transaction Time):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.HORACAPTURA)}</td></tr>");

                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Cliente (Customer):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.NOMBRE)}</td></tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Divisa a entregar (Currency to be delivered):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.DIVISAENTREGACLIENTE)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Divisa a recibir (Currency to be received):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.DIVISAADQUIERECLIENTE)}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Importe a entregar (Amount to be delivered):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.MONTOENTREGACLIENTE.ToString())}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Importe a recibir (Amount to be received):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.MONTOADQUIERECLIENTE.ToString())}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cuenta de Débito (Debit Account):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.CUENTAENTREGACLIENTE)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Cuenta de Crédito (Credit Account):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.CUENTAADQUIERECLIENTE)}</td>" +
                    $"</tr>");

                sb.AppendLine(
                    $"<tr><td class='header_tabla' width='20%'>&nbsp;Tipo de Cambio (Exchange Rate):</td>" +
                    $"<td class='dato_tabla' width='80%' colspan='3'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.ECCLIENTEREAL)}</td></tr>");

                sb.AppendLine(
                    $"<tr>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Folio de Recepción (Folio Number):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(complementoNotiForex.ID_AUTORIZACION)}</td>" +
                    $"<td class='header_tabla' width='20%'>&nbsp;Plaza (Location):</td>" +
                    $"<td class='dato_tabla' width='30%'>&nbsp;{HtmlHelper.TextoHtml(ConstantesServicio.plaza)}</td>" +
                    $"</tr>");

                sb.AppendLine("<tr><td class='dato_tabla' width='100%' colspan='4' align='right'>&nbsp;** Esta operación se deberá leer en sentido del Banco.</td></tr>");
                sb.AppendLine("<tr><td class='dato_tabla' width='100%' colspan='4' align='right'>&nbsp;*** Esta operación no genera ninguna comisión.</td></tr>");

                sb.AppendLine("</table>");
                sb.AppendLine("<BR><BR>");
                sb.AppendLine("</center>");

                sb.AppendLine("<BR><BR><span class='Nota'><p><B>AVISO LEGAL.</B> A través de éste correo electrónico se le notifica que MUFG Bank México, S.A., Institución de Banca Múltiple Filial (indistintamente, \"MUFG\" o el \"Banco\") ha recibido una solicitud de su representante para realizar la transacción arriba descrita con cargo a la cuenta y/o número de crédito indicados, según sea el caso. La ejecución de la operación se encuentra sujeta a la capacidad administrativa de MUFG; en consecuencia ésta comunicación no constituye confirmación o aprobación por parte del Banco para su ejecución. Para la cancelación de la operación, favor de notificar por escrito al Banco, antes de las 15:30 horas (Ciudad de México) del día en curso; el silencio u omisión en la notificación señalada autoriza al Banco para ejecutar la operación sin responsabilidad alguna.</p></span>");

                sb.AppendLine("<span class='Nota'><p>Este correo electrónico y cualquiera de los documentos adjuntos, pueden contener información confidencial o privilegiada. Cualquier divulgación o distribución no autorizada de la misma queda estrictamente prohibida. Si usted no es el destinatario o no es una persona autorizada por éste para recibir la información contenida en él favor de notificar inmediatamente al remitente y destruir el mensaje absteniéndose de leer, reproducir, transmitir, almacenar, divulgar o usar en cualquier forma y por cualquier medio la información contenida en él.</p></span>");

                sb.AppendLine("<BR><BR><span class='Nota'><p><B>LEGAL NOTICE.</B> Through communication you are noticed that MUFG Bank Mexico, S.A. Institución de Banca Múltiple Filial has received a request from your legal representative to execute the above mentioned transaction.</p></span>");

                sb.AppendLine("<span class='Nota'><p>This email and any of the attached documents may contain confidential and privileged information. Unauthorized disclosure or distribution is strictly prohibited.</p></span>");

                sb.AppendLine("</html>");

                lsMensaje = sb.ToString();

                _configuracionBase.CorreoParaDefault = _configuracionBase.CorreoSenderTest ??ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault     = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.AuntoTest);
                _configuracionBase.CorreoSenderTest  = ConfiguracionEncriptadaHelper.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                var asunto = string.IsNullOrEmpty(_configuracionBase.SujetoDefault)
                              ? ConstantesServicio.AvisoPreNotificacion
                              : $"{ConstantesServicio.AvisoPreNotificacion}[{_configuracionBase.SujetoDefault}]";

                var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                {
                    CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenAviso,
                    NombreOrigen = ConstantesServicio.NombreCorreoOrigenAviso,
                    Para = _configuracionBase.CorreoParaDefault ?? mailTo,
                    ConCopia = _configuracionBase.CorreoParaDefault ?? mailCC,
                    CopiaOculta = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.AVISO_BENEFICIARIOS_BCC,
                    Asunto = asunto,
                    Mensaje = lsMensaje,
                    ArchivosAdjuntos = null
                });
                if (resultado)
                {
                    return true;
                }
                else
                {
                    await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                    {
                        CorreoOrigen = _configuracionBase.CorreoSenderTest ?? ConstantesServicio.CorreoOrigenErrorSol,
                        NombreOrigen = ConstantesServicio.NombreCorreoOrigenErrorSol,
                        Para = _configuracionBase.CorreoParaDefault ?? ConstantesServicio.MAIL_ERROR,
                        ConCopia = null,
                        CopiaOculta = null,
                        Asunto = string.IsNullOrWhiteSpace(_configuracionBase.SujetoDefault) ? ConstantesServicio.AsuntoAvisoEnvioPreNotiForex : _configuracionBase.SujetoDefault + " Error",
                        Mensaje = ConstantesServicio.MensajeAvisoEnvioNotifForez,
                        ArchivosAdjuntos = null
                    });
                    return true;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}

Email.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{BFE4DCCC-E657-4FD7-8A45-7A9FF5C2321A}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>Email</RootNamespace>
    <AssemblyName>Email</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
    <Deterministic>true</Deterministic>
    <NuGetPackageImportStamp>
    </NuGetPackageImportStamp>
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
    <PlatformTarget>AnyCPU</PlatformTarget>
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
    <Reference Include="AplicacionEmail, Version=1.0.0.0, Culture=neutral, processorArchitecture=x86">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>..\Aplicacion\bin\x86\Debug\net4.8\AplicacionEmail.dll</HintPath>
    </Reference>
    <Reference Include="InfraestructuraEmail, Version=1.0.0.0, Culture=neutral, processorArchitecture=x86">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>..\Infraestructura\bin\x86\Debug\net4.8\InfraestructuraEmail.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Bcl.AsyncInterfaces, Version=10.0.0.10, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Bcl.AsyncInterfaces.10.0.10\lib\net462\Microsoft.Bcl.AsyncInterfaces.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration, Version=10.0.0.10, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.10.0.10\lib\net462\Microsoft.Extensions.Configuration.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Abstractions, Version=10.0.0.10, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.Abstractions.10.0.10\lib\net462\Microsoft.Extensions.Configuration.Abstractions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Binder, Version=9.0.0.0, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>..\packages\Microsoft.Extensions.Configuration.Binder.10.0.3\lib\netstandard2.0\Microsoft.Extensions.Configuration.Binder.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.FileExtensions, Version=10.0.0.10, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.FileExtensions.10.0.10\lib\net462\Microsoft.Extensions.Configuration.FileExtensions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Json, Version=10.0.0.10, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.Json.10.0.10\lib\net462\Microsoft.Extensions.Configuration.Json.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileProviders.Abstractions, Version=10.0.0.10, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.FileProviders.Abstractions.10.0.10\lib\net462\Microsoft.Extensions.FileProviders.Abstractions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileProviders.Physical, Version=10.0.0.10, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.FileProviders.Physical.10.0.10\lib\net462\Microsoft.Extensions.FileProviders.Physical.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileSystemGlobbing, Version=10.0.0.10, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.FileSystemGlobbing.10.0.10\lib\net462\Microsoft.Extensions.FileSystemGlobbing.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Primitives, Version=10.0.0.10, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Primitives.10.0.10\lib\net462\Microsoft.Extensions.Primitives.dll</HintPath>
    </Reference>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=x86">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <HintPath>D:\ServiceCMS\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Buffers, Version=4.0.5.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.Buffers.4.6.1\lib\net462\System.Buffers.dll</HintPath>
    </Reference>
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.IO.Pipelines, Version=10.0.0.10, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.IO.Pipelines.10.0.10\lib\net462\System.IO.Pipelines.dll</HintPath>
    </Reference>
    <Reference Include="System.Memory, Version=4.0.5.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.Memory.4.6.3\lib\net462\System.Memory.dll</HintPath>
    </Reference>
    <Reference Include="System.Numerics" />
    <Reference Include="System.Numerics.Vectors, Version=4.1.6.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>packages\System.Numerics.Vectors.4.6.1\lib\net462\System.Numerics.Vectors.dll</HintPath>
    </Reference>
    <Reference Include="System.Runtime.CompilerServices.Unsafe, Version=6.0.3.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>packages\System.Runtime.CompilerServices.Unsafe.6.1.2\lib\net462\System.Runtime.CompilerServices.Unsafe.dll</HintPath>
    </Reference>
    <Reference Include="System.Text.Encodings.Web, Version=10.0.0.10, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.Text.Encodings.Web.10.0.10\lib\net462\System.Text.Encodings.Web.dll</HintPath>
    </Reference>
    <Reference Include="System.Text.Json, Version=10.0.0.10, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.Text.Json.10.0.10\lib\net462\System.Text.Json.dll</HintPath>
    </Reference>
    <Reference Include="System.Threading.Tasks.Extensions, Version=4.2.4.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.Threading.Tasks.Extensions.4.6.3\lib\net462\System.Threading.Tasks.Extensions.dll</HintPath>
    </Reference>
    <Reference Include="System.ValueTuple" />
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
    <WCFMetadata Include="Connected Services\" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
  <Import Project="packages\System.ValueTuple.4.6.2\build\net471\System.ValueTuple.targets" Condition="Exists('packages\System.ValueTuple.4.6.2\build\net471\System.ValueTuple.targets')" />
  <Target Name="EnsureNuGetPackageBuildImports" BeforeTargets="PrepareForBuild">
    <PropertyGroup>
      <ErrorText>This project references NuGet package(s) that are missing on this computer. Use NuGet Package Restore to download them.  For more information, see http://go.microsoft.com/fwlink/?LinkID=322105. The missing file is {0}.</ErrorText>
    </PropertyGroup>
    <Error Condition="!Exists('packages\System.ValueTuple.4.6.2\build\net471\System.ValueTuple.targets')" Text="$([System.String]::Format('$(ErrorText)', 'packages\System.ValueTuple.4.6.2\build\net471\System.ValueTuple.targets'))" />
  </Target>
</Project>

AplicacionEmail.csproj
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net4.8</TargetFramework>
    <OutputType>Library</OutputType>
    <Platforms>AnyCPU;x86</Platforms>
    <AssemblyName>AplicacionEmail</AssemblyName>
  </PropertyGroup>

  <ItemGroup>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System.Net.Http" />
  </ItemGroup>



</Project>

InfraestructuraEmail.csproj
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net4.8</TargetFramework>
    <Platforms>AnyCPU;x86</Platforms>
    <AssemblyName>InfraestructuraEmail</AssemblyName>
  </PropertyGroup>

  <ItemGroup>
    <Reference Include="AplicacionEmail">
      <HintPath>..\Aplicacion\bin\x86\Debug\net4.8\AplicacionEmail.dll</HintPath>
    </Reference>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
  </ItemGroup>


</Project>

packages.config
<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="Microsoft.Bcl.AsyncInterfaces" version="10.0.10" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration" version="10.0.10" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration.Abstractions" version="10.0.10" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration.FileExtensions" version="10.0.10" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration.Json" version="10.0.10" targetFramework="net48" />
  <package id="Microsoft.Extensions.FileProviders.Abstractions" version="10.0.10" targetFramework="net48" />
  <package id="Microsoft.Extensions.FileProviders.Physical" version="10.0.10" targetFramework="net48" />
  <package id="Microsoft.Extensions.FileSystemGlobbing" version="10.0.10" targetFramework="net48" />
  <package id="Microsoft.Extensions.Primitives" version="10.0.10" targetFramework="net48" />
  <package id="System.Buffers" version="4.6.1" targetFramework="net48" />
  <package id="System.IO.Pipelines" version="10.0.10" targetFramework="net48" />
  <package id="System.Memory" version="4.6.3" targetFramework="net48" />
  <package id="System.Numerics.Vectors" version="4.6.1" targetFramework="net48" />
  <package id="System.Runtime.CompilerServices.Unsafe" version="6.1.2" targetFramework="net48" />
  <package id="System.Text.Encodings.Web" version="10.0.10" targetFramework="net48" />
  <package id="System.Text.Json" version="10.0.10" targetFramework="net48" />
  <package id="System.Threading.Tasks.Extensions" version="4.6.3" targetFramework="net48" />
  <package id="System.ValueTuple" version="4.6.2" targetFramework="net48" />
</packages>

using AplicacionEmail.DTOs;
using AplicacionEmail.Enums;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace InfraestructuraEmail.Servicios
{
    public interface IDetallesTomi
    {
        #region Procesos de Beneficiario SER
        Task<IEnumerable<ComplementoBenSer>> RecuperarDetallesBenefSerAsync();
        Task<bool> ActualizaFolioBeneficiario(long idSolicitud);
        #endregion

        #region Procesos de Beneficiario TAX
        Task<IEnumerable<ComplementoBenTaxDTO>> RecuperarDetallesBenefTaxAsync();
        Task<bool> ActualizaFolioBeneficiarioTax(long idSolicitud);
        #endregion

        #region Procesos de Beneficiarios FXD
        Task<IEnumerable<ComplementoBenFxdDTO>> RecuperarDetallesBenefFxdAsync();
        Task<bool> ActualizaFolioBeneficiarioFxd(long idSolicitud);
        #endregion

        #region Procesos de Beneficiarios PAPFXD
        Task<IEnumerable<ComplementoBenPapFxdDTO>> RecuperarDetallesBenefPapFxdAsync();
        Task<bool> ActualizaFolioBeneficiarioPapFxd(long idSolicitud);
        #endregion

        #region Procesos de Beneficiarios FXT
        Task<IEnumerable<ComplementoBenFxtDTO>> RecuperarDetallesBenefFxtAsync();
        Task<bool> ActualizaFolioBeneficiarioFxt(long idSolicitud);
        #endregion

        #region Procesos de Beneficiarios PAPFXT
        Task<IEnumerable<ComplementoBenPapFxtDTO>> RecuperarDetallesBenefPapFxtAsync();
        Task<bool> ActualizaFolioBeneficiarioPapFxt(long idSolicitud);
        #endregion

        #region Procesos de beneficiarios FXI
        Task<IEnumerable<ComplementoBenFxiDTO>> RecuperarDetallesBenefFxiAsync();
        Task<bool> ActualizaFolioBeneficiarioFxi(long idSolicitud);
        #endregion

        #region Procesos de beneficiarios PAPFXI
        Task<IEnumerable<ComplementoBenPapFxiDTO>> RecuperarDetallesBenefPapFxiAsync();

        Task<bool> ActualizaFolioBeneficiarioPapFxi(long idSolicitud);
        #endregion

        #region Procesos de beneficiarios NOMFXD
        Task<IEnumerable<ComplementoBenNomFxdDTO>> RecuperarDetallesBenefNomFxdAsync();
        Task<bool> ActualizaFolioBeneficiarioNomFxd(long idSolicitud);
        #endregion

        #region Procesos de beneficiarios DRE
        Task<IEnumerable<ComplementoBenDreDTO>> RecuperarDetallesBenefDreAsync();
        Task<bool> ActualizaFolioBeneficiarioDre(long idSolicitud);
        #endregion

        #region Procesos de beneficiarios SPID
        Task<IEnumerable<ComplementoBenSpidDTO>> RecuperarDetallesBenefSpidAsync();
        Task<bool> ActualizaFolioBeneficiarioSpid(long idSolicitud);
        #endregion

        #region Procesos de beneficiarios TEF
        Task<IEnumerable<ComplementoBenTefDTO>> RecuperarDetallesBenefTefAsync();
        Task<bool> ActualizaFolioBeneficiarioTef(long idSolicitud);
        #endregion

        #region Procesos de beneficiarios PAPTEF
        Task<IEnumerable<ComplementoBenPapTefDTO>> RecuperarDetallesBenefPapTefAsync();
        Task<bool> ActualizaFolioBeneficiarioPapTef(long idSolicitud);
        #endregion

        #region Procesos de beneficiarios PAPSPID
        Task<IEnumerable<ComplementoBenPapSpidDTO>> RecuperarDetallesBenefPapSpidAsync();
        Task<bool> ActualizaFolioBeneficiarioPapSpid(long idSolicitud);
        #endregion

        #region Procesos de notificaciones H2H FXD
        Task<IEnumerable<ComplementoH2HFxdDTO>> RecuperarDetallesH2HFxdAsync();
        Task<bool> ActualizaFolioH2HFxd(long idSolicitud);
        #endregion

        #region Procesos de notificaciones H2H FXI
        Task<IEnumerable<ComplementoH2HFxiDTO>> RecuperarDetallesH2HFxiAsync();
        Task<bool> ActualizaFolioH2HFxi(long idSolicitud);
        #endregion

        #region Procesos de notificaciones H2H FXT
        Task<IEnumerable<ComplementoH2HFxtDTO>> RecuperarDetallesH2HFxtAsync();
        Task<bool> ActualizaFolioH2HFxt(long idSolicitud);
        #endregion

        #region Procesos de notificaciones H2H SPID
        Task<IEnumerable<ComplementoH2HSpidDTO>> RecuperarDetallesH2HSpidAsync();
        Task<bool> ActualizaFolioH2HSpid(long idSolicitud);
        #endregion

        #region Procesos de notificaciones H2H TEF
        Task<IEnumerable<ComplementoH2HTefDTO>> RecuperarDetallesH2HTefAsync();
        Task<bool> ActualizaFolioH2HTef(long idSolicitud);
        #endregion

        #region Procesos de notificaciones FOREX
        Task<IEnumerable<ComplementoNotiForexDTO>> RecuperarDetallesNotiForexAsync();
        Task<IEnumerable<ComplementoNotiForexDTO>> RecuperarDetallesPreNotiForexAsync();
        Task<bool> ActualizaFolioNotiForex(long idSolicitud);
        Task<bool> ActualizaFolioPreNotiForex(long idSolicitud);
        #endregion

        #region Procesos Operaciones FCM
        Task<IEnumerable<ComplementoFcmDTO>> RecuperarDetallesOperacFcmAsync();
        Task<IEnumerable<ComplementoOperaFcmDTO>> RecuperarDetallesOperacAsync(long folio, string tipoOperac);
        Task<bool> ActualizaFolioFcm(long idSolicitud, string strTipo);
        #endregion

        #region Procesos de operaciones hotscan de tipo SPID
        Task<IEnumerable<ComplementoOperHotsSpidDTO>> RecuperarDetallesOperHotscanSpid();

        Task<IEnumerable<ComplementoOperacHotscanSpidDetDTO>> RecuperarDetallesOperHotscanSpidDet(long idAlerta);

        Task<bool> ActualizaFolioHotscanSpid(long idAlerta);
        #endregion

        #region Notificaciones Otros Email
        Task<IEnumerable<ComplementoEmailOtrosDTO>> RecuperarDetallesNotiEmailOtros();
        #endregion

        #region Notificaciones Sobregiros
        Task<IEnumerable<ComplementoSobregirosDTO>> RecuperarDetallesSobregiros();

        Task<bool> ActualizaFolioSobregiro();
        #endregion

        #region Notificaciones Sobregiros
        Task<IEnumerable<ComplementoCuentaCashPoolDTO>> RecuperarDetallesCuentaCashPool();

        #endregion

        #region Notificaciones cuentas FAD
        Task<IEnumerable<ComplementoCuentasFADDTO>> RecuperarDetallesCuentasFAD();
        Task<bool> ActualizaFolioCuentasFad();
        #endregion

        #region Notificaciones FIEL SPID
        Task<IEnumerable<ComplementoFielValidoDTO>> RecuperarDetallesFielesSPID();
        Task<IEnumerable<ComplementoFielDetDTO>> RecuperarDetallesFielesSPIDDet();
        Task<bool> ActualizaFolioFielInvalidas();
        Task<bool> ActualizaFolioFielValidas(int validos, int rechazados);
        #endregion

        #region Notificaciones FIELES SID
        Task<IEnumerable<ComplementoInicioFielDTO>> RecuperarDetallesInicioFieles();
        Task<bool> InsertarFolioInicioFiel(int total);
        Task<IEnumerable<ComplementoValidasFielDTO>> RecuperarDetallesValidasFielesSPID();
        Task<bool> ActualizaFolioFielValidasRechazadas();
        Task<IEnumerable<ComplementoFielValidoDTO>> RecuperarDetallesIniFielesSPID();
        #endregion
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoAlertasCriticasDTO
    {
       public  string  CLIENTE { get; set; }
       public  string  CUENTA { get; set; }
       public  string  CCY { get; set; }
       public  string  MONTO { get; set; }
       public  int TOTAL { get; set; }
       public int NUMREG { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoAvisoBeneficiario
    {
        public string ID_EMAIL_ORDENANTE {  get; set; }
        public string ID_EMAIL_BENEFICIARIO { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoAvisoH2HDTO
    {
        public string T24_COMPANY_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string CORREO { get; set; }
        public string NF_CUENTA_CORRESPONSAL { get; set; }
        public string NF_COUNTRY { get; set; }
        public string NF_CHARGE { get; set; }
        public string NF_MENSAJE { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
        public int? NF_FECHA_CARGO { get; set; }
        public int? NF_FECHA_APLICACION { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoAvisoPwdDTO
    {
        public long IDENTIFICADOR { get; set; }
        public string FECHA { get; set; }
        public string HORA { get; set; }
        public string USUARIO { get; set; }
        public string DESCRIPCION { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public  class ComplementoAvisosDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public decimal? T24_DEBIT_AMOUNT {get;set;}
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long? ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_CONCEPT { get; set; }
        public string T24_CURRENCY { get; set; }
        public string T24_REFERENCE { get; set; }
        public decimal? T24_AMOUNT { get; set; }
        public string T24_NO_AGREEMENT { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
        public int? NF_FECHA_CARGO { get; set; }
        public int? NF_FECHA_APLICACION { get; set; }
        public string NF_CUENTA_CORRESPONSAL { get; set; }
        public string NF_COUNTRY { get; set; }
        public string NF_CHARGE { get; set; }
        public string NF_MENSAJE { get; set; }
        public long ID_PLANTILLA { get; set; }
        public string T24_ACCT_NO { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenCmsDTO
    {
        public int SATM02COD { get; set; }
        public int SATM02CDL { get; set; }
        public long ID_SOLICITUD { get; set; }
        public string ORDENATE { get; set; }
        public DateTime FECHA_SOL { get; set; }
        public string HORA_AUT { get; set; }
        public string FORMATO { get; set; }
        public string ALIAS { get; set; }
        public string NOMBRE { get; set; }
        public string TIPOCUENTA { get; set; }
        public string CLAVEBANCO { get; set; }
        public string BANCO { get; set; }
        public string NOCUENTA { get; set; }
        public string MONEDA { get; set; }
        public string ABA { get; set; }
        public string COUNTRY { get; set; }
        public string BANCOBENEFICIARIO { get; set; }
        public string SWIFTBENEFICIARIO { get; set; }
        public string COUNTRY_BEN { get; set; }
        public string CUENTABENEFICIARIO { get; set; }
        public string SWIFTCORRESPONSAL { get; set; }
        public string BANCOCORRESPONSAL { get; set; }
        public string DF_CONTACTOSCUB_EMAIL { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenDreDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_COMPANY_NAME { get; set; }
        public string NF_TIPO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_PROCESA { get; set; }
        public string NF_CADENA_ORIGINAL { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public int SATM02COD { get; set; }
        public string NF_DESCRIPCION { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenFxdDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenFxiDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_CUENTA_CORRESPONSAL { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string NF_COUNTRY { get; set; }
        public string NF_CHARGE { get; set; }
        public string NF_MENSAJE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenFxtDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenNomFxdDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public int NOPAGOS { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public int NOPAGOSPROCESADOS { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public int NOPAGOSDEVUELTOS { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenPapFxdDTO
    {
        public long    NF_FOLIO { get; set; }
        public string  T24_SHORT_NAME { get; set; }
        public string  T24_DEBIT_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenPapFxiDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_CUENTA_CORRESPONSAL { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string NF_COUNTRY { get; set; }
        public string NF_CHARGE { get; set; }
        public string NF_MENSAJE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenPapFxtDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenPapSpidDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenPapTefDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public int NF_FECHA_CARGO { get; set; }
        public int NF_FECHA_APLICACION { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenSer
    {
        public long nf_folio { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_NO_AGREEMENT { get; set; }
        public decimal T24_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public DateTime NF_FECHA_AUTORIZA { get; set; }
        public string T24_REFERENCE { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_PLANTILLA { get; set; }
        public string T24_CONCEPT { get; set; }
        public string T24_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenSerSec
    {
        public string DF_CONTACTOSCUB_EMAIL { get; set; }
        public decimal PK_CONTACTOSCUB { get; set; }
        public string DF_CONTACTOSCUB_APELLIDOPATERNO { get; set; }
        public string DF_CONTACTOSCUB_NOMBRE { get; set; }
        public string DF_CLIENTESCUB_NOCDL { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenSpidDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenTaxDTO
    {
        public long nf_folio { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_NO_AGREEMENT { get; set; }
        public decimal T24_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string T24_REFERENCE { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_PLANTILLA { get; set; }
        public string T24_CONCEPT { get; set; }
        public string T24_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoBenTefDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public int NF_FECHA_CARGO { get; set; }
        public int NF_FECHA_APLICACION { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoCUBFXDDTO
    {

        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
        public int? NF_FECHA_CARGO { get; set; }
        public int? NF_FECHA_APLICACION { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoClienteDTO
    {
        public long ID_CLIENTE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoContactoDTO
    {
        public string DF_CONTACTOSCUB_EMAIL { get; set; }
        public string SATM04COR { get; set; }
        public string SATM04EMA { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoCubFxiDTO
    {
        public long NF_FOLIO { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_CUENTA_CORRESPONSAL { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public int SATM02COD { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string NF_COUNTRY { get; set; }
        public string NF_CHARGE { get; set; }
        public string NF_MENSAJE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoCuentaCashPoolDTO
    {

          public string Nombre_Cliente { get; set; }
          public string  Currency { get; set; }
          public int    Cuenta { get; set; }
          public double Saldo { get; set; }
          public double monto { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoCuentasFADDTO
    {
        public int Id { get; set; }
        public string Nombre_Cliente { get; set; }
        public int Cuenta { get; set; }
        public string Desc_Cuenta { get; set; }
        public string Currency { get; set; }
        public double Saldo { get; set; }
        public string Last_Txr { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoCustomerDTO
    {
        public string CUSTOMER_CODE { get; set; }   
        public string NOMBRE {  get; set; }   
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoEmailOtrosDTO
    {
        public string salida { get; set; }
        public string hora { get; set; }
        public string detalle { get; set; }
        public string descripcion { get; set; }
    } 
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoFcmDTO
    {
        public long NF_FOLIO {  get; set; }
        public string  TIPO_OPERACION { get; set; }
        public string DES_PARAMETRO { get; set; }
        public string DES_PARAMETRO2 { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoFielDetDTO
    {
        public string CDL { get; set; }
        public string FIEL { get; set; }
        public string ESTADO_CONSULTA { get; set; }
        public DateTime FECHACONSULTA { get; set; }
        public string WS_RESP_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoFielSPIDDTO
    {

        public  string Dia { get; set; }
        public  string Estado_Consulta { get; set; }
        public int  Total { get; set; }
    }

    public class ComplementoIniFielSPIDDTO
    {

        public string Dia { get; set; }
        public string FIEL_STATUS { get; set; }
        public int Total { get; set; }
    }

    public class ComplementoValidasFielSPIDDTO
    {

        public string Dia { get; set; }
        public string Estado_Consulta { get; set; }
        public string CDL { get; set; }
    }

    public class ComplementoFielValidoDTO
    {

        public string Dia { get; set; }
        public string Estado_Consulta { get; set; }
        public string CDL { get; set; }
    }

}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoFielSPIDDetDTO
    {
       public  string CDL { get; set; }
       public  string NOMBRE { get; set; }
       public  string FIEL { get; set; }
	   public  string ESTADO_CONSULTA { get; set; }
       public  DateTime? FECHACONSULTA { get; set; }
       public string WS_RESP_CODE { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoH2HFxdDTO
    {
        public string T24_COMPANY_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string CORREO { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoH2HFxiDTO
    {
        public string T24_COMPANY_NAME { get; set; }
        public string NF_CUENTA_CORRESPONSAL { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string NF_COUNTRY { get; set; }
        public string NF_CHARGE { get; set; }
        public string NF_MENSAJE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string CORREO { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoH2HFxtDTO
    {
        public string T24_COMPANY_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string CORREO { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoH2HSpidDTO
    {
        public string T24_COMPANY_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string CORREO { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
        
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoH2HTefDTO
    {
        public string T24_COMPANY_NAME { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_FECHA_AUTORIZA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string CORREO { get; set; }
        public int NF_FECHA_CARGO { get; set; }
        public int NF_FECHA_APLICACION { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoInicioFielDTO
    {
        public string CDL { get; set; } 
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoNotiForexDTO
    {

        public long ID_AUTORIZACION { get; set; }
        public string NOMBRE { get; set; }
        public string TIPOOPERACION { get; set; }
        public string PRODUCTO { get; set; }
        public DateTime FECHACAPTURA { get; set; }
        public DateTime FECHAVALOR { get; set; }
        public string HORACAPTURA { get; set; }
        public string DIVISAENTREGACLIENTE { get; set; }
        public string DIVISAADQUIERECLIENTE { get; set; }
        public decimal MONTOENTREGACLIENTE { get; set; }
        public decimal MONTOADQUIERECLIENTE { get; set; }
        public string CUENTAADQUIERECLIENTE { get; set; }
        public string CUENTAENTREGACLIENTE { get; set; }
        public double ECCLIENTEREAL { get; set; }
        public double FOLIO_TAS { get; set; }
        public long ID_CLIENTE { get; set; }
        public string CDL { get; set; }
        public string CORREO { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoOperHotsSpidDTO
    {

       public string cve_valor { get; set; }
       public string cve_valor2 { get; set; }
       public long   id_alerta { get; set; }
       public int    nf_folio { get; set; }
       public string base_origen { get; set; }
       public DateTime fec_deteccion { get; set; }
       public string des_beneficiario { get; set; }
       public double num_importe { get; set; }
       public string des_ordenante { get; set; }
       public string des_descripcion { get; set; }
       public string num_cta_beneficiario { get; set; }
       public   bool flg_recurrente { get; set; }
       public string cve_moneda { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoOperaFcmDTO
    {
        public string NF_ESTATUS { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public string NF_TIPO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public long NF_FOLIO { get; set; }
        public string NF_FOLIO_UNICO { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string T24_COMPANY_NAME { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public int MOTIVODEVOLUCION { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoOperacHotscanDTO
    {
        public long id_scan { get; set; }
        public string des_parametro { get; set; }
        public string des_parametro2 { get; set; }
        public int nf_folio { get; set; }
        public string base_origen { get; set; }
        public DateTime fec_deteccion { get; set; }
        public string des_beneficiario { get; set; }
        public double num_importe { get; set; }
        public string des_nombre_cliente { get; set; }
        public string des_descripcion { get; set; }
        public string num_clabe { get; set; }
        public bool flg_omitir_notificacion { get; set; }
        public string cve_moneda { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoOperacHotscanDetDTO
    {
        public string des_detector { get; set; }
        public string des_origen_lista { get; set; }
        public string des_nombre_lista { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoOperacHotscanSpidDetDTO
    {
        public string des_detector { get; set; }
        public string des_origen_lista { get; set; }
        public string des_nombre_lista { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoSatNepeDTO
    {
        public int CONSECUTIVO {  get; set; }   
        public string ID_LOTE { get; set; }
        public int ESTATUS { get; set; }
        public int PAQUETES { get; set; }
        public int OPERACIONES { get; set; }
        public int REGISTROS { get; set; }
        public decimal IMPORTE { get; set; }
        public string RESULTADO { get; set; }
        public string DESCRIPCION { get; set; }
        public string CAMPO { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoSatRefDTO
    {
        public int CONSECUTIVO { get; set; }
        public string ID_LOTE { get; set; }
        public int ESTATUS { get; set; }
        public int PAQUETES { get; set; }
        public int OPERACIONES { get; set; }
        public int REGISTROS { get; set; }
        public decimal IMPORTE { get; set; }
        public string RESULTADO { get; set; }
        public string DESCRIPCION { get; set; }
        public string CAMPO { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoSobregirosDTO
    {
       public string Nombre_Cliente {get; set; }
       public string Currency {get; set; }
       public int Cuenta { get; set; }
       public double Saldo {get; set; }
       public double Monto_Debito {get; set; }
       public string Concepto { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace AplicacionEmail.DTOs
{
    public class ComplementoSolDTO
    {
        public long id_solicitud { get; set; } 
        public long id_solicitud_documento { get; set; }
        public long satm04cod { get; set; }
        public string satm04log { get; set; }
        public string satm04apa { get; set; }
        public string satm04ama { get; set; }
        public string satm04nom { get; set; }
        public string satm04cor { get; set; }
        public string satm04ema { get; set; }
        public string satm04numsertoken { get; set; }
        public string descripcion { get; set; }
        public DateTime fecha_sol { get; set; }
        public string hora_sol { get; set; }
        public string usuario_capt { get; set; }
        public DateTime fecha_aut { get; set; }
        public string hora_aut { get; set; }
        public string usuario_aut { get; set; }
        public DateTime fecha_canc { get; set; }
        public string hora_canc { get; set; }
        public string usuario_canc { get; set; }
        public string status { get; set; }
        public string operacion { get; set; }
        public DateTime fecha_conf { get; set; }
        public string hora_conf { get; set; }
        public string usuario_conf { get; set; }
        public string satm04clave_activacion { get; set; }
        public string motivo_canc { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoTokenExcepDTO
    {
       public int idConsecutivo {  get; set; }
       public string IdUsuario { get; set; }
       public string serieToken { get; set; }
       public string email { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoUsuarioEmailDTO
    {

        public string usuario { get; set; }
        public string new_mail { get; set; }
        public string mensaje { get; set; }
        public string asunto { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ComplementoValidasFielDTO
    {

        public  string Dia { get; set; }
        public  string FIEL_STATUS { get; set; }
        public string CDL { get; set; }
    }
}

namespace AplicacionEmail.DTOs
{
    public class OperacionDTO
    {
        public string servicio { get; set; } = null;
        public string tipo_operacion { get; set; } = null;
        public long folio { get; set; }
        public decimal fec_valor { get; set; }
    }
}

using AplicacionEmail.Interfaces;
using InfraestructuraEmail.Servicios;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionEmail.DTOs
{
    public class ProcesamientoSobregiros : IProcesamientoSobregiros
    {
        Configuracion   _configuracion;
        IDetallesTomi   _detallesTomi;
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        public ProcesamientoSobregiros(Configuracion configuracion, IDetallesTomi detallesTomi)
        {
            _configuracion = configuracion;
            _detallesTomi  = detallesTomi;
        }

        /// <summary>
        /// Metodo que obtiene la información de las operaciones Hotscan de tipo SPID
        /// </summary>
        /// <returns></returns>
        public async Task<List<ComplementoSobregirosDTO>> ObtencionDatosSobregiros()
        {
            //Obtiene los complementos de las operaciones FCM...
            List<ComplementoSobregirosDTO> complementoSobregiros = (List<ComplementoSobregirosDTO>)await _detallesTomi.RecuperarDetallesSobregiros();

            return complementoSobregiros;
        }
    }
}
