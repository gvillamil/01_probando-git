using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion
{
    /// <summary>
    /// Ejecuta el proceso para levantar y detener servicios
    /// </summary>
    public class OperacionServicio : IOperacionServicio
    {
        private readonly IRepositorioTomi _repositorioTomi;
        private readonly IControladorServicios _controlador;
        private readonly ConfiguracionServicio _configuracion;

        private readonly IRelojNegocio _relojNegocio;

        /// <summary>
        /// Constructor.
        /// </summary>
        public OperacionServicio(IRepositorioTomi repositorioTomi,
            ConfiguracionServicio configuracion, IControladorServicios controlador, IRelojNegocio relojNegocio)
        {
            _configuracion = configuracion;
            _repositorioTomi = repositorioTomi;
            _controlador = controlador;
            _relojNegocio = relojNegocio;
        }


        /// <summary>
        /// Parametros sistema, hora, url
        /// </summary>
        private async Task ObtenerParametrosSistema()
        {

            Dictionary<string, string> ltsParametrosSistema = await _repositorioTomi.ObtenerParametrosSistema() ?? new Dictionary<string, string>();
                
            foreach (KeyValuePair<string, string> paramSistema in ltsParametrosSistema)
            {
                if (!Enum.TryParse(paramSistema.Key, out Enums.ParametrosSistema parametro)) continue; // Cambia el tipo string a un enum
                switch (parametro)
                {
                    case Enums.ParametrosSistema.HORA_OP_INICIO:
                        _configuracion.StrHoraOperacionesInicio = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_OP_INICIO.ToString());
                        Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de inicio de operaciones '" + _configuracion.StrHoraOperacionesInicio + "'.",
                            _configuracion.NombreServicio);
                        break;

                    case Enums.ParametrosSistema.HORA_OP_FIN:
                        _configuracion.StrHoraOperacionesFin = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_OP_FIN.ToString());
                        Config.EscribirLog("INFO|ObtenerParametrosSistema|Hora de cancelacion SPEI '" + _configuracion.StrHoraOperacionesFin + "'.",
                            _configuracion.NombreServicio);
                        break;

                    case Enums.ParametrosSistema.SWF_OUT_GET_MT_BATCH:
                        _configuracion.SWF_OUT_GET_MT_BATCH = paramSistema.Value == "1";
                        Config.EscribirLog("INFO|obtenerParametrosSistema|Obtención de mensajes batch MT-9xx '" + _configuracion.SWF_OUT_GET_MT_BATCH + "'.",
                            _configuracion.NombreServicio);
                        break;
                }
            }

        }

        /// <summary>
        /// Obtiene los parametros del sistema necesario para la operacion del servicio
        /// </summary>
        private async Task ObtenerParametros()
        {


            // Valida si es dia habil
            _configuracion.EsDiaHabil = await _repositorioTomi.ValidaSiEsDiaHabil();

            await ObtenerParametrosSistema();

            // Obtencion de la fecha del sistema.
            var horaActual = _relojNegocio.Now;
            _configuracion.StrFechaSistemaServicio = new TimeSpan(horaActual.Hour, horaActual.Minute, 0);

            ValidarTimeSpan(_configuracion.StrHoraOperacionesInicio, "No se especifica la hora de inicio de operaciones");
            ValidarTimeSpan(_configuracion.StrHoraOperacionesFin, "No se especifica la hora de fin de operaciones");

        }

        /// <summary>
        /// Ejecuta procedimeinto
        /// </summary>
        /// <returns></returns>
        public async Task EjecutaProcedimiento(CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                await _repositorioTomi.ProcesoActualizaEjecucion(_configuracion.NombreServicio, _configuracion.Pid, _configuracion.NombreProceso);

                // Obtiene los parametros del sistema (variables, rutas, fechas, etc)
                ct.ThrowIfCancellationRequested();
                await ObtenerParametros();

                if (_configuracion.StrFechaSistemaServicio >= _configuracion.StrHoraOperacionesInicio &&
                        _configuracion.StrFechaSistemaServicio <= _configuracion.StrHoraOperacionesFin) // Inicio Horario
                    {
                        if (!_configuracion.EnHorarioOperaciones)
                        {
                            _configuracion.EnHorarioOperaciones = true;
                            Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Inicio horario de operaciones, HoraSistema='{_configuracion.StrFechaSistemaServicio:hh\\:mm}'.",
                                _configuracion.NombreServicio);
                        }
                    }
                    else
                    {
                        // Termina Horario
                        if (_configuracion.EnHorarioOperaciones)
                        {
                            _configuracion.EnHorarioOperaciones = false;
                            Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Termina horario de operaciones, HoraSistema='{_configuracion.StrFechaSistemaServicio:hh\\:mm}'.",
                                _configuracion.NombreServicio);
                        }
                    }

                    bool bolPuedeIniciar = _configuracion.SWF_OUT_GET_MT_BATCH || (_configuracion.EsDiaHabil == true && _configuracion.EnHorarioOperaciones);


                    ct.ThrowIfCancellationRequested();
                    Task<bool>[] tareas = new[]
                    {
                        bolPuedeIniciar
                        ? _controlador.IniciaServicioAsync(EnumComunes.SWF_OUT_READ, ct)
                        : _controlador.DetieneServicioAsync(EnumComunes.SWF_OUT_READ, ct),

                        bolPuedeIniciar
                        ? _controlador.IniciaServicioAsync(EnumComunes.SWF_OUT_ETL, ct)
                        : _controlador.DetieneServicioAsync(EnumComunes.SWF_OUT_ETL, ct),

                        bolPuedeIniciar
                        ? _controlador.IniciaServicioAsync(EnumComunes.SWF_OUT_PK_DB, ct)
                        : _controlador.DetieneServicioAsync(EnumComunes.SWF_OUT_PK_DB, ct),
                    };

                    ct.ThrowIfCancellationRequested();
                    bool[] resultados = await Task.WhenAll(tareas);

                    if (resultados.Any(x => !x))
                    {
                        Config.EscribirLog(
                            "ERROR|Ejecuta_Procedimientos|No todos los servicios pudieron procesarse.",
                            _configuracion.NombreServicio);
                    }


                
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|Ejecuta_Procedimientos|Se presenta exception: " + ex, _configuracion.NombreServicio);
                Config.EscribirLog("ERROR|Ejecuta_Procedimientos|Se forza la finalización del servicio.", _configuracion.NombreServicio);
            }
            finally
            {
                try
                {
                    // Fin de los procesos a realizar
                    await _repositorioTomi.ProcesoActualizaTerminacion(_configuracion.NombreServicio, (int)_configuracion.Pid, _configuracion.NombreProceso);
                }
                catch (Exception exFinally)
                {
                    Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|no fue posible maracar el servicio en finalización: {exFinally}");
                    throw;

                }
            }
        }

     
        /// <summary>
        /// Mensaje de error y validacion de los campos Time.
        /// </summary>
        /// <param name="valor"></param>
        /// <param name="mensaje"></param>
        /// <exception cref="Exception"></exception>
        private void ValidarTimeSpan(TimeSpan? valor, string mensaje)
        {
            if (valor.HasValue) return;

            Config.EscribirLog(mensaje, _configuracion.NombreServicio);
            throw new Exception(mensaje);
        }

    }
}


using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using System;
using System.ServiceProcess;
using System.Threading.Tasks;
using System.Threading;

namespace Aplicacion.Service
{
    /// <summary>
    /// Clase para levantar y detener servicios
    /// </summary>
    public class ControladorServicios : IControladorServicios
    {
        private readonly ConfiguracionServicio _configuracion;
        private readonly IRepositorioTomi _repositorioTomi;

        /// <summary>
        /// Controlador.
        /// </summary>
        /// <param name="configuracion"></param>
        /// <param name="repositorioTomi"></param>
        public ControladorServicios(ConfiguracionServicio configuracion, IRepositorioTomi repositorioTomi)
        {
            _configuracion = configuracion;
            _repositorioTomi = repositorioTomi;
        }

        /// <summary>
        /// Inicia un servico de Windows.
        /// </summary>
        /// <param name="NombreServicio"></param>
        public async Task<bool> IniciaServicioAsync(string nombreServicio, CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                using (ServiceController serviceController =
                    new ServiceController(nombreServicio, "."))
                {
                    serviceController.Refresh();

                    switch (serviceController.Status)
                    {
                        case ServiceControllerStatus.Running:
                            return true;

                        case ServiceControllerStatus.StartPending:
                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Running,
                                TimeSpan.FromSeconds(30));
                            return true;

                        case ServiceControllerStatus.ContinuePending:
                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Running,
                                TimeSpan.FromSeconds(30));
                            return true;

                        case ServiceControllerStatus.Paused:
                            serviceController.Continue();

                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Running,
                                TimeSpan.FromSeconds(30));

                            return true;

                        case ServiceControllerStatus.PausePending:
                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Paused,
                                TimeSpan.FromSeconds(30));

                            serviceController.Continue();

                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Running,
                                TimeSpan.FromSeconds(30));

                            return true;

                        case ServiceControllerStatus.StopPending:
                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Stopped,
                                TimeSpan.FromSeconds(30));
                            break;
                    }

                    Config.EscribirLog(
                        $"INFO|Ejecuta_Procedimientos|Inicialización del Servicio '{nombreServicio}'.");
                    ct.ThrowIfCancellationRequested();
                    await Task.Run(() =>
                    {
                        serviceController.Start();

                        serviceController.WaitForStatus(
                            ServiceControllerStatus.Running,
                            TimeSpan.FromSeconds(30));
                    });

                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|servicioArriba|Excepcion: {ex}");

                await _repositorioTomi.ProcesoInsertaAlerta(new AlertaDTO()
                {
                    Folio = null,
                    TipoOPeracion = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = EnumComunes.ALERTA_ESTATUS_ERROR,
                    Descipcion = $"Error al tratar de levantar el servicio '{nombreServicio}'",
                    PId = (int)_configuracion.Pid,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = ex
                });
            }

            return false;
        }

        /// <summary>
        /// Detiene un servicio de Windows.
        /// </summary>
        /// <param name="NombreServicio"></param>
        public async Task<bool> DetieneServicioAsync(string nombreServicio, CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                using (ServiceController serviceController =
                    new ServiceController(nombreServicio, "."))
                {
                    serviceController.Refresh();

                    switch (serviceController.Status)
                    {
                        case ServiceControllerStatus.Stopped:
                            return true;

                        case ServiceControllerStatus.StopPending:
                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Stopped,
                                TimeSpan.FromSeconds(30));

                            return true;

                        case ServiceControllerStatus.StartPending:
                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Running,
                                TimeSpan.FromSeconds(30));
                            break;

                        case ServiceControllerStatus.ContinuePending:
                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Running,
                                TimeSpan.FromSeconds(30));
                            break;

                        case ServiceControllerStatus.PausePending:
                            serviceController.WaitForStatus(
                                ServiceControllerStatus.Paused,
                                TimeSpan.FromSeconds(30));
                            break;

                        case ServiceControllerStatus.Paused:
                            break;
                    }

                    Config.EscribirLog(
                        $"INFO|Ejecuta_Procedimientos|Finalización del Servicio '{nombreServicio}'.");
                    ct.ThrowIfCancellationRequested();
                    await Task.Run(() =>
                    {
                        serviceController.Stop();

                        serviceController.WaitForStatus(
                            ServiceControllerStatus.Stopped,
                            TimeSpan.FromSeconds(30));
                    });

                    return true;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|servicioAbajo|Excepcion: {ex}");

                await _repositorioTomi.ProcesoInsertaAlerta(new AlertaDTO()
                {
                    Folio = null,
                    TipoOPeracion = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = EnumComunes.ALERTA_ESTATUS_ERROR,
                    Descipcion = $"Error al tratar de detener el servicio '{nombreServicio}'",
                    PId = (int)_configuracion.Pid,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = ex
                });
            }

            return false;
        }
    }
}


﻿using MUFG_T24.Modelos;
using Aplicacion.Modelos;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Aplicacion.Interfaces
{
    public interface IOperacionServicio
    {

        /// <summary>
        /// Ejecuta procedimiento
        /// </summary>
        /// <returns></returns>
        Task EjecutaProcedimiento(CancellationToken ct);
    }
}


﻿using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Implementa metodos para orquestar servicios
    /// </summary>
    public interface IControladorServicios
    {
        /// <summary>
        /// Proceso para levantar servicio
        /// </summary>
        /// <param name="NombreServicio"></param>
        /// <returns>true/false</returns>
        Task<bool> IniciaServicioAsync(string NombreServicio, CancellationToken ct);

        /// <summary>
        /// Proceso para bajar servicio
        /// </summary>
        /// <param name="NombreServicio"></param>
        /// <returns>true/false</returns>
        Task<bool> DetieneServicioAsync(string NombreServicio, CancellationToken ct);
    }
}


using MUFG_T24.Modelos;
using Aplicacion.Modelos;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaz que implementa consultas y no consultas a bd InterfacesT24
    /// </summary>
    public interface IRepositorioTomi
    {
        /// <summary>
        /// Marca puerto libre
        /// </summary>
        /// <param name="intpId"></param>
        /// <param name="NombreProceso"></param>
        /// <param name="NombreServicio"></param>
        /// <param name="strServicio"></param>
        /// <returns></returns>
        Task ActualizaPuertoLibre(string strServicio);

        /// <summary>
        /// Marca puerto en uso
        /// </summary>
        /// <param name="intpId"></param>
        /// <param name="NombreProceso"></param>
        /// <param name="NombreServicio"></param>
        /// <param name="strServicio"></param>
        /// <returns></returns>
        Task ActualizaPuertoEnUso(int intpId, string NombreProceso, string NombreServicio, string strServicio);

        
        /// <summary>
        /// Inserta alertas
        /// </summary>
        /// <param name="alerta"></param>
        /// <returns></returns>
        Task ProcesoInsertaAlerta(AlertaDTO alerta);

        /// <summary>
        /// Valida si puede operar el servicio
        /// </summary>
        /// <returns>true/false</returns>
        Task<bool> ValidaSiEsDiaHabil();

        /// <summary>
        /// Obtiene los parametros del sistema necesario para la operacion del servicio, incluye horarios de inicio y finalizacion de operaciones
        /// </summary>
        /// <returns>Dictionary<string, string></returns>
        Task<Dictionary<string, string>> ObtenerParametrosSistema();

        /// <summary>
        /// Actualiza ejecucion del servicio INICIO
        /// </summary>
        /// <param name="strFolio"></param>
        /// <returns></returns>
        Task ProcesoActualizaEjecucion(string NombreServicio, int? intgpId, string NombreProceso);

        /// <summary>
        /// Actualiza terminacion del proceso
        /// </summary>
        /// <param name="NombreServicio"></param>
        /// <param name="intgpId"></param>
        /// <param name="NombreProceso"></param>
        /// <returns></returns>
        Task ProcesoActualizaTerminacion(string NombreServicio, int intgpId, string NombreProceso);
    }
}


using MUFG_T24.Configuracion_utilidades;
using System;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Propiedades generales del servicio
    /// </summary>
    public class ConfiguracionServicio : Configuracion
    {
        public TimeSpan? StrFechaSistemaServicio { get; set; } = null;
        public TimeSpan? StrHoraOperacionesInicio { get; set; } = null;
        public TimeSpan? StrHoraOperacionesFin { get; set; } = null;
        public bool EnHorarioOperaciones { get; set; } = false;

        // Logs y rutas archivo config
        public string RutaArchivoLogs { get; set; }

        // Parametros sistema
        public bool SWF_OUT_GET_MT_BATCH { get; set; }
        public bool MensajesBatch { get; set; }
    }
}


using System;
using System.Collections.Generic;
using System.Text;

namespace Aplicacion.Enums
{
    /// <summary>
    /// Enum para datos generales
    /// </summary>
    public static class EnumComunes
    {
        public const string ALERTA_ESTATUS_ERROR = "ERROR";

        public const string SWF_OUT_READ = "CMS-Swf_OUT_Read";
        public const string SWF_OUT_ETL = "CMS-Swf_OUT_ETL";
        public const string SWF_OUT_PK_DB = "CMS-Swf_OUT_PK_DB";
    }

    /// <summary>
    /// Enum para Procedimientos Almacenados
    /// </summary>
    public static class EnumProcedimientosAlmacenados
    {
        // TOMI
        public const string usp_ConsultaFechaSistema = "usp_ObtenerFechaSistema";
        public const string usp_EsDiaHabil = "usp_EsDiaHabil";
        public const string usp_ParametrosSistema = "usp_ConsultaParametrosSistema_SwfOutCtrlSrv";
        public const string usp_MarcaPuertoEnEso = "usp_ActualizaServicioEnUso_SwfOutCtrlSrv";
        public const string usp_MarcaPuertoLibre = "usp_ActualizaServicioLibre_SwfOutCtrlSrv";
    }

    /// <summary>
    /// Enum para parametros del sistema
    /// </summary>
    public enum ParametrosSistema
    {       
        HORA_OP_INICIO,
        HORA_OP_FIN,
        SWF_OUT_GET_MT_BATCH

    }
}


using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Modelos.Tomi;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Servicio
{
    /// <summary>
    /// Clase para conectarse a BD InterfacesT24 
    /// </summary>
    public class RepositorioTomi : RepositorioMaestro, IRepositorioTomi
    {
        private readonly Alertas _alertas;
        private readonly IParametrizacion _parametrizacion;

        /// <summary>
        /// Inicializa una nueva instancia del repositorio TOMI.
        /// </summary>
        public RepositorioTomi(Alertas alertas, IParametrizacion parametrizacion) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _alertas = alertas;
            _parametrizacion = parametrizacion;
        }

        /// <summary>
        /// Marca puerto libre.
        /// </summary>
        /// <param name="strServicio"></param>
        /// <returns></returns>
        public async Task ActualizaPuertoLibre(string strServicio)
        {
            StoredProcedureResult resultadoSP =await EjecutarStoredProceduredNonQuery(
                EnumProcedimientosAlmacenados.usp_MarcaPuertoLibre, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Servicio", strServicio));
                });

            if (!resultadoSP.Exito) Config.EscribirLog($"Error: no se completo actualizacion del puerto: {resultadoSP.MensajeError}");

        }

        /// <summary>
        /// Marca puerto en uso
        /// </summary>
        /// <param name="intpId"></param>
        /// <param name="NombreProceso"></param>
        /// <param name="NombreServicio"></param>
        /// <param name="strServicio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task ActualizaPuertoEnUso(
            int intpId, string NombreProceso, string NombreServicio, string strServicio)
        {
            StoredProcedureResult resultadoSP =await EjecutarStoredProceduredNonQuery(
                EnumProcedimientosAlmacenados.usp_MarcaPuertoEnEso, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@PId", intpId));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreProceso", NombreProceso, 50));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreServicio", NombreServicio, 50));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Servicio", strServicio, 50));
                });

            if (!resultadoSP.Exito) Config.EscribirLog($"Error: no se completo actualizacion del puerto: {resultadoSP.MensajeError}");

        }

        /// <summary>
        /// Inserta alertas
        /// </summary>
        /// <param name="alerta"></param>
        /// <returns></returns>
        public async Task ProcesoInsertaAlerta(AlertaDTO alerta)
        {
            try
            {
                bool boAlertaInsertada = await _alertas.InsertarAlerta(alerta);
                if (!boAlertaInsertada) Config.EscribirLog($"Error: no se completo la insercion de la Alerta");
                
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesoInsertaAlerta{ex}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene los parametros del sistema necesario para la operacion del servicio, incluye horarios de inicio y finalizacion de operaciones
        /// </summary>
        /// <returns>Dictionary<string, string></returns>
        public async Task<bool> ValidaSiEsDiaHabil()
        {
            try
            {
                return await EsDiaHabil();
            }
            catch (Exception ex)
            {

                Config.EscribirLog($"Error: no se pudo recuperar dia habil. Excepcion: {ex}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene los parametros del sistema necesario para la operacion del servicio, incluye horarios de inicio y finalizacion de operaciones
        /// </summary>
        /// <returns>Dictionary<string, string></returns>
        public async Task<Dictionary<string, string>> ObtenerParametrosSistema() => await _parametrizacion.ObtenerParametrosSistemaAsync("'HORA_OP_INICIO','HORA_OP_FIN','SWF_OUT_GET_MT_BATCH'");


        /// <summary>
        /// Actualiza ejecucion del servicio INICIO
        /// </summary>
        /// <param name="strFolio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task ProcesoActualizaEjecucion(string NombreServicio, int? intgpId, string NombreProceso)
        {
            try
            {
                StoredProcedureResult resultActualizaEjec = await ActualizaEJecucion(NombreServicio);
                if (!resultActualizaEjec?.Exito ?? false) Config.EscribirLog($"Error al ejecutar consulta. gpId:{intgpId}, proceso:{NombreProceso}, Ex: {resultActualizaEjec?.Exception}: Mensaje: {resultActualizaEjec?.MensajeError}");

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"Error al ejecutar proceso: {ex}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza terminacion del proceso
        /// </summary>
        /// <param name="NombreServicio"></param>
        /// <param name="intgpId"></param>
        /// <param name="NombreProceso"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task ProcesoActualizaTerminacion(string NombreServicio, int intgpId, string NombreProceso)
        {
            try
            {
                StoredProcedureResult resultActualizaTerm = await ActualizaTerminacion(NombreServicio);
                if (!resultActualizaTerm?.Exito ?? false)
                {

                    Config.EscribirLog($"Error al ejecutar consulta. gpId:{intgpId}, proceso:{NombreProceso}, Ex: {resultActualizaTerm?.Exception}: Mensaje: {resultActualizaTerm?.MensajeError}");
                    
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesoActualizaTerminacion{ex}");
                throw;
            }
        }

        
    }
}


using Aplicacion;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Service;
using Infraestructura.Servicio;
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

namespace Swf_OUT_ControladorServicios
{
    /// <summary>
    /// Clase principal servicio.
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private ConfiguracionServicio _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.Swf_OUT_ControladorServicios.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServiceHost() { InitializeComponent(); }

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
                ServicioConfiguracionSettings servicioMainConfiguracion = MUFG_T24.Core.Configuracion.LecturaConfguracionGeneral.ObtenerServicio(NombreServicio.Swf_OUT_ControladorServicios.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.RutaArchivoConfiguracion = $@"{EnumRutasGenerales.Path_Log_CMS}{NombreServicio.Swf_OUT_ControladorServicios.ToString()}\";
                _configuracionBase.PathLog = servicioMainConfiguracion.Log + servicioMainConfiguracion.NombreServicio;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.configuracion = servicioMainConfiguracion.configuracion;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB;

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


            // Contenedores

            var parametrizacion = new Parametrizacion();

            _containerDI.Register<ConfiguracionServicio>(() => (ConfiguracionServicio)_configuracionBase.Clone(), Lifestyle.Scoped);

            _containerDI.Register<IRelojNegocio, RelojNegocio>(Lifestyle.Singleton);

            _containerDI.Register<IRepositorioTomi, RepositorioTomi>(Lifestyle.Scoped);
            _containerDI.Register<Aplicacion.Interfaces.IControladorServicios, Aplicacion.Service.ControladorServicios>(Lifestyle.Scoped);
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
                    await operacionServicio.EjecutaProcedimiento(ct).ConfigureAwait(false);
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


﻿using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;
using System.ServiceProcess;

namespace Swf_OUT_ControladorServicios
{
    /// <summary>
    /// Punto de entrada Main.
    /// </summary>
    internal static class Program
    {
        /// <summary>
        /// Punto de entrada Main.
        /// </summary>
        static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                NombreServicio.Swf_OUT_ControladorServicios.ToString(),
                () => new ServiceHost(),
                args,
                (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs));
        }

    }
}


using MUFG_T24.Comunes;

namespace Swf_OUT_ControladorServicios
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
            this.ServiceName = NombreServicio.Swf_OUT_ControladorServicios.ToString();
        }

        #endregion
    }
}


Aplicacion.csproj
﻿<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net48</TargetFramework>
    <AssemblyName>AplicacionSWF_OUT_CONTROLADORSERVICIOS</AssemblyName>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>

  <ItemGroup>
    <Reference Include="MUFG_T24">
		<HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System.ServiceProcess">
      <HintPath>C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.ServiceProcess.dll</HintPath>
    </Reference>
  </ItemGroup>

</Project>


Infraestructura.csproj
﻿<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net48</TargetFramework>
    <AssemblyName>InfraestructuraSWF_OUT_CONTROLADORSERVICIOS</AssemblyName>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\Aplicacion.csproj" />
  </ItemGroup>

  <ItemGroup>
    <Reference Include="MUFG_T24">
		<HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
  </ItemGroup>

</Project>


Swf_OUT_ControladorServicios.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{B24B9FBF-B39D-4509-ACEA-C8ECD4D3EF60}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>Swf_OUT_ControladorServicios</RootNamespace>
    <AssemblyName>Swf_OUT_ControladorServicios</AssemblyName>
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
    <Reference Include="Castle.Core, Version=5.0.0.0, Culture=neutral, PublicKeyToken=407dd0808d44fbdc, processorArchitecture=MSIL">
      <HintPath>..\packages\Castle.Core.5.2.1\lib\net462\Castle.Core.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Bcl.AsyncInterfaces, Version=10.0.0.3, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Bcl.AsyncInterfaces.10.0.3\lib\net462\Microsoft.Bcl.AsyncInterfaces.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.Configuration.10.0.3\lib\net462\Microsoft.Extensions.Configuration.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Abstractions, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.Configuration.Abstractions.10.0.3\lib\net462\Microsoft.Extensions.Configuration.Abstractions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Binder, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.Configuration.Binder.10.0.3\lib\net462\Microsoft.Extensions.Configuration.Binder.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.FileExtensions, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.Configuration.FileExtensions.10.0.3\lib\net462\Microsoft.Extensions.Configuration.FileExtensions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Json, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.Configuration.Json.10.0.3\lib\net462\Microsoft.Extensions.Configuration.Json.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileProviders.Abstractions, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.FileProviders.Abstractions.10.0.3\lib\net462\Microsoft.Extensions.FileProviders.Abstractions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileProviders.Physical, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.FileProviders.Physical.10.0.3\lib\net462\Microsoft.Extensions.FileProviders.Physical.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileSystemGlobbing, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.FileSystemGlobbing.10.0.3\lib\net462\Microsoft.Extensions.FileSystemGlobbing.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Primitives, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>..\packages\Microsoft.Extensions.Primitives.10.0.3\lib\net462\Microsoft.Extensions.Primitives.dll</HintPath>
    </Reference>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <HintPath>..\packages\SimpleInjector.5.5.0\lib\net461\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Buffers, Version=4.0.5.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Buffers.4.6.1\lib\net462\System.Buffers.dll</HintPath>
    </Reference>
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.IO.Pipelines, Version=10.0.0.3, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\System.IO.Pipelines.10.0.3\lib\net462\System.IO.Pipelines.dll</HintPath>
    </Reference>
    <Reference Include="System.Memory, Version=4.0.5.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Memory.4.6.3\lib\net462\System.Memory.dll</HintPath>
    </Reference>
    <Reference Include="System.Numerics" />
    <Reference Include="System.Numerics.Vectors, Version=4.1.6.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Numerics.Vectors.4.6.1\lib\net462\System.Numerics.Vectors.dll</HintPath>
    </Reference>
    <Reference Include="System.Runtime.CompilerServices.Unsafe, Version=6.0.3.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Runtime.CompilerServices.Unsafe.6.1.2\lib\net462\System.Runtime.CompilerServices.Unsafe.dll</HintPath>
    </Reference>
    <Reference Include="System.Text.Encodings.Web, Version=10.0.0.3, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Text.Encodings.Web.10.0.3\lib\net462\System.Text.Encodings.Web.dll</HintPath>
    </Reference>
    <Reference Include="System.Text.Json, Version=10.0.0.3, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Text.Json.10.0.3\lib\net462\System.Text.Json.dll</HintPath>
    </Reference>
    <Reference Include="System.Threading.Tasks.Extensions, Version=4.2.4.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Threading.Tasks.Extensions.4.6.3\lib\net462\System.Threading.Tasks.Extensions.dll</HintPath>
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
    <Compile Include="Service1.Designer.cs">
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
    <ProjectReference Include="..\Aplicacion\Aplicacion.csproj">
      <Project>{2e777ddf-cb2b-41be-aa74-19eefb782bb4}</Project>
      <Name>Aplicacion</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura\Infraestructura.csproj">
      <Project>{a5e6b4fb-6fbc-4eca-bbdc-cd500c0e8a7e}</Project>
      <Name>Infraestructura</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
  <Import Project="..\packages\System.ValueTuple.4.6.1\build\net471\System.ValueTuple.targets" Condition="Exists('..\packages\System.ValueTuple.4.6.1\build\net471\System.ValueTuple.targets')" />
  <Target Name="EnsureNuGetPackageBuildImports" BeforeTargets="PrepareForBuild">
    <PropertyGroup>
      <ErrorText>This project references NuGet package(s) that are missing on this computer. Use NuGet Package Restore to download them.  For more information, see http://go.microsoft.com/fwlink/?LinkID=322105. The missing file is {0}.</ErrorText>
    </PropertyGroup>
    <Error Condition="!Exists('..\packages\System.ValueTuple.4.6.1\build\net471\System.ValueTuple.targets')" Text="$([System.String]::Format('$(ErrorText)', '..\packages\System.ValueTuple.4.6.1\build\net471\System.ValueTuple.targets'))" />
  </Target>
</Project>


Swf_OUT_ControladorServicios.sln

Microsoft Visual Studio Solution File, Format Version 12.00
# Visual Studio Version 17
VisualStudioVersion = 17.14.37111.16
MinimumVisualStudioVersion = 10.0.40219.1
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Aplicacion", "Aplicacion\Aplicacion.csproj", "{2E777DDF-CB2B-41BE-AA74-19EEFB782BB4}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Infraestructura", "Infraestructura\Infraestructura.csproj", "{A5E6B4FB-6FBC-4ECA-BBDC-CD500C0E8A7E}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Swf_OUT_ControladorServicios", "Swf_OUT_ControladorServicios\Swf_OUT_ControladorServicios.csproj", "{B24B9FBF-B39D-4509-ACEA-C8ECD4D3EF60}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "PruebasUnitariasSwfCtrlSrv", "PruebasUnitariasSwfCtrlSrv\PruebasUnitariasSwfCtrlSrv.csproj", "{8901AD17-F90E-42B1-A5A5-D78AE18956FB}"
EndProject
Global
	GlobalSection(SolutionConfigurationPlatforms) = preSolution
		Debug|Any CPU = Debug|Any CPU
		Release|Any CPU = Release|Any CPU
	EndGlobalSection
	GlobalSection(ProjectConfigurationPlatforms) = postSolution
		{2E777DDF-CB2B-41BE-AA74-19EEFB782BB4}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{2E777DDF-CB2B-41BE-AA74-19EEFB782BB4}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{2E777DDF-CB2B-41BE-AA74-19EEFB782BB4}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{2E777DDF-CB2B-41BE-AA74-19EEFB782BB4}.Release|Any CPU.Build.0 = Release|Any CPU
		{A5E6B4FB-6FBC-4ECA-BBDC-CD500C0E8A7E}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{A5E6B4FB-6FBC-4ECA-BBDC-CD500C0E8A7E}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{A5E6B4FB-6FBC-4ECA-BBDC-CD500C0E8A7E}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{A5E6B4FB-6FBC-4ECA-BBDC-CD500C0E8A7E}.Release|Any CPU.Build.0 = Release|Any CPU
		{B24B9FBF-B39D-4509-ACEA-C8ECD4D3EF60}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{B24B9FBF-B39D-4509-ACEA-C8ECD4D3EF60}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{B24B9FBF-B39D-4509-ACEA-C8ECD4D3EF60}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{B24B9FBF-B39D-4509-ACEA-C8ECD4D3EF60}.Release|Any CPU.Build.0 = Release|Any CPU
		{8901AD17-F90E-42B1-A5A5-D78AE18956FB}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{8901AD17-F90E-42B1-A5A5-D78AE18956FB}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{8901AD17-F90E-42B1-A5A5-D78AE18956FB}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{8901AD17-F90E-42B1-A5A5-D78AE18956FB}.Release|Any CPU.Build.0 = Release|Any CPU
	EndGlobalSection
	GlobalSection(SolutionProperties) = preSolution
		HideSolutionNode = FALSE
	EndGlobalSection
	GlobalSection(ExtensibilityGlobals) = postSolution
		SolutionGuid = {C24E260D-CE0F-4EE7-9B3C-B02B12461025}
	EndGlobalSection
EndGlobal


packages.config
﻿<?xml version="1.0" encoding="utf-8"?>
<packages>
	<package id="SimpleInjector" version="5.5.0" targetFramework="net48" />
</packages>


App.config
﻿<?xml version="1.0" encoding="utf-8"?>
<configuration>
    <startup> 
        <supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.8" />
    </startup>
  <runtime>
    <assemblyBinding xmlns="urn:schemas-microsoft-com:asm.v1">
      <dependentAssembly>
        <assemblyIdentity name="System.Text.Json" publicKeyToken="cc7b13ffcd2ddd51" culture="neutral" />
        <bindingRedirect oldVersion="0.0.0.0-10.0.0.3" newVersion="10.0.0.3" />
      </dependentAssembly>
    </assemblyBinding>
  </runtime>
</configuration>


