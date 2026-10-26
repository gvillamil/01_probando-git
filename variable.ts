using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.ServiceProcess;
using System.Threading.Tasks;

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
        /// Constructor.
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
        /// <param name="strNombreServicio"></param>
        public async Task<bool> IniciaServicioAsync(string strNombreServicio)
        {
            try
            {
                using (ServiceController serviceController = new ServiceController(strNombreServicio, "."))
                {

                    serviceController.Refresh();

                    if (serviceController.Status == ServiceControllerStatus.Running)
                    {
                        Config.EscribirLog($"INFO|IniciaServicio|El servicio {strNombreServicio} ya se encuentra iniciado.");
                        return true;
                    }

                    Config.EscribirLog($"INFO|IniciaServicio|Inicialización del Servicio '{strNombreServicio}'.");

                    await Task.Run(() =>
                    {
                        serviceController.Start();

                        serviceController.WaitForStatus(
                            ServiceControllerStatus.Running, TimeSpan.FromSeconds(30));
                    });

                    serviceController.Refresh();

                    if (serviceController.Status == ServiceControllerStatus.Running)
                    {
                        Config.EscribirLog($"INFO|IniciaServicio|Servicio '{strNombreServicio}' iniciado correctamente.");
                        return true;
                    }
                    else
                    {
                        string msj = $"No fue posible iniciar el servicio {strNombreServicio}.";
                        Config.EscribirLog($"ERROR|IniciaServicio|{msj}");

                        try
                        {
                            await _repositorioTomi.ProcesoInsertaAlerta(
                            new AlertaDTO()
                            {
                                Folio = null,
                                TipoOPeracion = null,
                                Servicio = _configuracion.StrNombreServicio,
                                ClaveAviso = EnumComunes.ALERTA_ESTATUS_ERROR,
                                Descipcion = msj,
                                PId = (int)_configuracion.Pid,
                                NombreProceso = _configuracion.NombreProceso,
                                Exception = null
                            });
                        }
                        catch (Exception exAlerta)
                        {
                            Config.EscribirLog($"INFO|IniciaServicio|No fue posible generar la alerta: {exAlerta}.");
                        }
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|IniciaServicio|Excepcion: {ex}");

                try
                {
                    await _repositorioTomi.ProcesoInsertaAlerta(
                    new AlertaDTO()
                    {
                        Folio = null,
                        TipoOPeracion = null,
                        Servicio = _configuracion.StrNombreServicio,
                        ClaveAviso = EnumComunes.ALERTA_ESTATUS_ERROR,
                        Descipcion = $"Error al tratar de levantar el servicio '{strNombreServicio}'",
                        PId = (int)_configuracion.Pid,
                        NombreProceso = _configuracion.NombreProceso,
                        Exception = ex
                    });
                }
                catch (Exception exAlerta)
                {
                    Config.EscribirLog($"INFO|IniciaServicio|No fue posible generar la alerta: {exAlerta}.");
                }
            }

            return false;
        }

        /// <summary>
        /// Detiene un servicio de Windows.
        /// </summary>
        /// <param name="strNombreServicio"></param>
        public async Task<bool> DetieneServicioAsync(string strNombreServicio)
        {
            try
            {
                using (ServiceController serviceController = new ServiceController(strNombreServicio, "."))
                {

                    serviceController.Refresh();

                    if (serviceController.Status == ServiceControllerStatus.Stopped)
                    {
                        Config.EscribirLog($"INFO|DetieneServicio|El servicio '{strNombreServicio}' del Servicio");
                        return true;
                    }

                    Config.EscribirLog($"INFO|DetieneServicio|Finalización del servicio '{strNombreServicio}'");

                    await Task.Run(() =>
                    {
                        serviceController.Stop();

                        serviceController.WaitForStatus(
                            ServiceControllerStatus.Stopped, TimeSpan.FromSeconds(30));
                    });

                    serviceController.Refresh();

                    if (serviceController.Status == ServiceControllerStatus.Stopped)
                    {
                        Config.EscribirLog($"INFO|DetieneServicio|Finalización del Servicio");
                        return true;
                    }
                    else
                    {
                        string msj = $"No fue posible detener el servicio {strNombreServicio}.";

                        Config.EscribirLog($"ERROR|DetieneServicio|{msj}");

                        try
                        {
                            await _repositorioTomi.ProcesoInsertaAlerta(
                            new AlertaDTO()
                            {
                                Folio = null,
                                TipoOPeracion = null,
                                Servicio = _configuracion.StrNombreServicio,
                                ClaveAviso = EnumComunes.ALERTA_ESTATUS_ERROR,
                                Descipcion = msj,
                                PId = (int)_configuracion.Pid,
                                NombreProceso = _configuracion.NombreProceso,
                                Exception = null
                            });
                        }
                        catch (Exception exAlert)
                        {
                            Config.EscribirLog($"INFO|DetieneServicio|No fue posible generar la alerta: {exAlert}.");
                        }

                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|DetieneServicio|Excepcion: {ex}");

                try
                {
                    await _repositorioTomi.ProcesoInsertaAlerta(new AlertaDTO()
                    {
                        Folio = null,
                        TipoOPeracion = null,
                        Servicio = _configuracion.StrNombreServicio,
                        ClaveAviso = EnumComunes.ALERTA_ESTATUS_ERROR,
                        Descipcion = $"Error al tratar de detener el servicio '{strNombreServicio}'",
                        PId = (int)_configuracion.Pid,
                        NombreProceso = _configuracion.NombreProceso,
                        Exception = ex
                    });
                }
                catch (Exception exAlerta)
                {
                    Config.EscribirLog($"INFO|DetieneServicio|No fue posible generar la alerta: {exAlerta}.");
                }
            }
            return false;
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
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

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="repositorioTomi"></param>
        /// <param name="configuracion"></param>
        /// <param name="controlador"></param>
        public OperacionServicio(IRepositorioTomi repositorioTomi,
            ConfiguracionServicio configuracion, IControladorServicios controlador)
        {
            _configuracion = configuracion;
            _repositorioTomi = repositorioTomi;
            _controlador = controlador;
        }


        /// <summary>
        /// Parametros sistema, hora, url
        /// </summary>
        private async Task ObtenerParametrosSistema()
        {
            try
            {
                Dictionary<string, string> ltsParametrosSistema = await _repositorioTomi.ObtenerParametrosSistema();
                foreach (KeyValuePair<string, string> paramSistema in ltsParametrosSistema)
                {
                    if (!Enum.TryParse(paramSistema.Key, out Enums.ParametrosSistema parametro)) continue;
                    switch (parametro)
                    {
                        case Enums.ParametrosSistema.HORA_OP_INICIO:
                            _configuracion.HORA_OP_INICIO = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_OP_INICIO.ToString());
                            Config.EscribirLog("INFO|ObtenerParametrosSistema|Hora de inicio de operaciones '" + _configuracion.HORA_OP_INICIO + "'.",
                                _configuracion.StrNombreServicio);
                            break;

                        case Enums.ParametrosSistema.HORA_OP_FIN:
                            _configuracion.HORA_OP_FIN = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_OP_FIN.ToString());
                            Config.EscribirLog("INFO|ObtenerParametrosSistema|Hora de cancelacion SPEI '" + _configuracion.HORA_OP_FIN + "'.",
                                _configuracion.StrNombreServicio);
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerParametrosSistema|Se presenta exception: " + ex.Message, _configuracion.StrNombreServicio);
                throw;
            }
        }

        /// <summary>
        /// Obtiene los parametros del sistema necesario para la operacion del servicio
        /// </summary>
        private async Task ObtenerParametros()
        {
            try
            {

                await _repositorioTomi.ProcesoActualizaEjecucion(
                   _configuracion.StrNombreServicio, (int)_configuracion.Pid, _configuracion.NombreProceso);

                // Valida si es dia habil
                _configuracion.BoEsDiaHabil = await _repositorioTomi.ValidaSiEsDiaHabil();

                await ObtenerParametrosSistema();

                // Obtencion de la fecha del sistema.
                var horaActual = DateTime.Now;
                _configuracion.StrFechaSistemaServicio = new TimeSpan(horaActual.Hour, horaActual.Minute, 0);

                if (string.IsNullOrEmpty(_configuracion.HORA_OP_INICIO.ToString()) || string.IsNullOrEmpty(_configuracion.HORA_OP_FIN.ToString()))
                {
                    Config.EscribirLog($"ERROR|No se especifica la hora de inicio/fin de operaciones", _configuracion.StrNombreServicio);
                    throw new Exception($"Error de especificación de hora inicio/fin");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerParametros|Se presenta exception: " + ex, _configuracion.StrNombreServicio);
                throw;
            }
        }

        /// <summary>
        /// Ejecuta procedimeinto
        /// </summary>
        /// <returns></returns>
        public async Task EjecutaProcedimiento()
        {
            Exception excepcionPrincipal = null;

            try
            {
                await ObtenerParametros();

                _configuracion.EnHorarioOperaciones = (_configuracion.StrFechaSistemaServicio >= _configuracion.HORA_OP_INICIO && 
                    _configuracion.StrFechaSistemaServicio <= _configuracion.HORA_OP_FIN);

                Config.EscribirLog($"INFO|EjecutaProcedimiento|{(_configuracion.EnHorarioOperaciones ? "Inicio" : "Termina")} " +
                    $"horario de operaciones, HoraSistema='{_configuracion.StrFechaSistemaServicio:hh\\:mm}'.", NombreServicio.Swf_IN_ControladorServicios.ToString());

                bool bolPuedeIniciar = _configuracion.BoEsDiaHabil && _configuracion.EnHorarioOperaciones;

                Task<bool>[] tareas = new[]
                {
                        bolPuedeIniciar
                        ? _controlador.IniciaServicioAsync(EnumComunes.Swf_IN_ETL)
                        : _controlador.DetieneServicioAsync(EnumComunes.Swf_IN_ETL),

                        bolPuedeIniciar
                        ? _controlador.IniciaServicioAsync(EnumComunes.Swf_IN_FT)
                        : _controlador.DetieneServicioAsync(EnumComunes.Swf_IN_FT),
                };

                var result = await Task.WhenAll(tareas);

                if (result.Any(r => r == false))
                {
                    Config.EscribirLog($"ERROR|Uno o mas servicios no alcanzaron el estado deseado: " +
                        $"{_configuracion.StrNombreServicio}");
                }
                else
                {
                    Config.EscribirLog($"INFO|Todos los servicios alcanzaron el estado deseado: " +
                        $"{_configuracion.StrNombreServicio}");
                }
            }
            catch (Exception ex)
            {
                excepcionPrincipal = ex;
                Config.EscribirLog("ERROR|EjecutaProcedimiento|Se presenta exception: " + ex,
                    _configuracion.StrNombreServicio);

                throw;
            }
            finally
            {
                try
                {
                    await _repositorioTomi.ProcesoActualizaTerminacion(
                        _configuracion.StrNombreServicio, (int)_configuracion.Pid, _configuracion.NombreProceso);
                }
                catch (Exception exFinally)
                {
                    Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|No fue posible marcar el servicio en finalización: {exFinally}");

                    if (excepcionPrincipal == null)
                    {
                        throw;
                    }
                }
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface IControladorServicios, tiene la lista de los metodos que se van a utilizar para iniciar o detener los servicios.
    /// </summary>
    public interface IControladorServicios
    {
        /// <summary>
        /// Proceso para levantar servicio
        /// </summary>
        /// <param name="strNombreServicio"></param>
        /// <returns>true/false</returns>
        Task<bool> IniciaServicioAsync(string strNombreServicio);

        /// <summary>
        /// Proceso para bajar servicio
        /// </summary>
        /// <param name="strNombreServicio"></param>
        /// <returns>true/false</returns>
        Task<bool> DetieneServicioAsync(string strNombreServicio);
    }
}

using Aplicacion.Modelos;
using MUFG_T24.Modelos;
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
        /// <param name="strNombreProceso"></param>
        /// <param name="strNombreServicio"></param>
        /// <param name="strServicio"></param>
        /// <returns></returns>
        Task ActualizaPuertoLibre(string strServicio);

        /// <summary>
        /// Marca puerto en uso
        /// </summary>
        /// <param name="intpId"></param>
        /// <param name="strNombreProceso"></param>
        /// <param name="strNombreServicio"></param>
        /// <param name="strServicio"></param>
        /// <returns></returns>
        Task ActualizaPuertoEnUso(int intpId, string strNombreProceso, string strNombreServicio, string strServicio);

        /// <summary>
        /// Obtiene las fechas del sistema
        /// </summary>
        /// <returns>Parametros</returns>
        Task<string> ObtenerFechaSistema();

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
        Task ProcesoActualizaEjecucion(string strNombreServicio, int intgpId, string strNombreProceso);

        /// <summary>
        /// Actualiza terminacion del proceso
        /// </summary>
        /// <param name="strNombreServicio"></param>
        /// <param name="intgpId"></param>
        /// <param name="strNombreProceso"></param>
        /// <returns></returns>
        Task ProcesoActualizaTerminacion(string strNombreServicio, int intgpId, string strNombreProceso);
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

        public const string Swf_IN_ETL = "CMS-Swf_IN_ETL";
        public const string Swf_IN_FT = "CMS-Swf_IN_FT";
    }

    /// <summary>
    /// Enum para Procedimientos Almacenados
    /// </summary>
    public static class EnumProcedimientosAlmacenados
    {       
        public const string usp_ConsultaFechaSistema = "usp_ObtenerFechaSistema";
        public const string usp_EsDiaHabil = "usp_EsDiaHabil";
        public const string usp_ParametrosSistema = "usp_ConsultaParametrosSistema_SwfInCtrlSrv";
        public const string usp_MarcaPuertoEnEso = "usp_ActualizaServicioEnUso_SwfInCtrlSrv";
        public const string usp_MarcaPuertoLibre = "usp_ActualizaServicioLibre_SwfInCtrlSrv";
    }

    /// <summary>
    /// Enum para parametros del sistema
    /// </summary>
    public enum ParametrosSistema
    {       
        HORA_OP_INICIO,
        HORA_OP_FIN
    }
}

﻿using MUFG_T24.Configuracion_utilidades;
using System;


namespace Aplicacion.Modelos
{
    /// <summary>
    /// Propiedades generales del servicio
    /// </summary>
    public class ConfiguracionServicio : Configuracion
    {
        public string StrFechaSistemaTomi { get; set; } = null;
        public TimeSpan? StrFechaSistemaServicio { get; set; } = null;
        public TimeSpan? StrHoraOperacionesInicio { get; set; } = null;
        public TimeSpan? StrHoraOperacionesFin { get; set; } = null;
        public TimeSpan? HORA_OP_INICIO {  get; set; } = null;
        public TimeSpan? HORA_OP_FIN { get; set; } = null;
        public bool EnHorarioOperaciones { get; set; } = false;
        public string StrNombreServicio { get; set; } = null;
        public bool BoEsDiaHabil { get; set; } = false;

        // Logs y rutas archivo config
        public string RutaArchivoConfiguracionOriginal { get; set; }
        public string RutaArchivoConfiguracionFinal { get; set; }
        public string RutaArchivoLogs { get; set; }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Modelos.Tomi;
using MUFG_T24.Comunes;
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
        private readonly ConfiguracionServicio _configuracionServicio;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        /// <param name="proveedorVistasT24"></param>
        /// <param name="nombreBasedatosVistasT24"></param>
        public RepositorioTomi(Alertas alertas, ConfiguracionServicio configuracionServicio) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _alertas = alertas;
            _configuracionServicio = configuracionServicio;
        }

        /// <summary>
        /// Marca puerto libre
        /// </summary>
        /// <param name="intpId"></param>
        /// <param name="strNombreProceso"></param>
        /// <param name="strNombreServicio"></param>
        /// <param name="strServicio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task ActualizaPuertoLibre(string strServicio)
        {
            try
            {
                StoredProcedureResult resultadoSP = await EjecutarStoredProceduredNonQuery(
              EnumProcedimientosAlmacenados.usp_MarcaPuertoLibre, cmd =>
              {
                  cmd.Parameters.Add(basedatosConexion.CrearParametro("@Servicio", strServicio));
              });

                if (!resultadoSP.Exito)
                {
                    string strMsjError = $"Error|ActualizaPuertoLibre|No se completo actualizacion del puerto {resultadoSP.MensajeError}";
                    Config.EscribirLog(strMsjError, _configuracionServicio.NombreServicio);
                    throw new Exception(strMsjError);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ActualizaPuertoLibre: " + ex, _configuracionServicio.NombreServicio);
                throw;
            }
        }

        /// <summary>
        /// Marca puerto en uso
        /// </summary>
        /// <param name="intpId"></param>
        /// <param name="strNombreProceso"></param>
        /// <param name="strNombreServicio"></param>
        /// <param name="strServicio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task ActualizaPuertoEnUso(int intpId, string strNombreProceso, string strNombreServicio, string strServicio)
        {
            try
            {
                StoredProcedureResult resultadoSP = await EjecutarStoredProceduredNonQuery(
               EnumProcedimientosAlmacenados.usp_MarcaPuertoEnEso, cmd =>
               {
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@PId", intpId));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreProceso", strNombreProceso, 50));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreServicio", strNombreServicio, 50));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@Servicio", strServicio, 50));
               });

                if (!resultadoSP.Exito)
                {
                    string strMsjError = $"ERROR|ActualizaPuertoEnUso|No se completo actualizacion del puerto: {resultadoSP.MensajeError}";
                    Config.EscribirLog(strMsjError, _configuracionServicio.NombreServicio);
                    throw new Exception(strMsjError);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ActualizaPuertoEnUso: " + ex, _configuracionServicio.NombreServicio);
                throw;
            }
        }

        /// <summary>
        /// Obtiene las fechas del sistema.
        /// </summary>
        /// <returns>string</returns>
        public async Task<string> ObtenerFechaSistema()
        {
            try
            {
                StoredProcedureResult<MapearCamposGenerales> strFecha =
                await EjecutarStoredProcedureQuery<MapearCamposGenerales>(EnumProcedimientosAlmacenados
                .usp_ConsultaFechaSistema);

                if (strFecha.Data.Count == 0 || strFecha.MensajeError != null)
                {
                    string strMsjError = $"ERROR|ObtenerFechaSistema: {strFecha.MensajeError}";
                    Config.EscribirLog(strMsjError, _configuracionServicio.NombreServicio);
                    throw new Exception(strMsjError);
                }

                string strFechaFormateada = strFecha.Data.FirstOrDefault().Fecha.ToString(EnumParametrosGenerales.FECHA_BD);
                return strFechaFormateada;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerFechaSistema: " + ex, _configuracionServicio.NombreServicio);
                throw;
            }
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
                if (!boAlertaInsertada)
                {
                    string strMsjError = "ERROR|ProcesoInsertaAlerta|No se completo la insercion de la Alerta";
                    Config.EscribirLog(strMsjError, _configuracionServicio.NombreServicio);
                    throw new Exception(strMsjError);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ProcesoInsertaAlerta: " + ex, _configuracionServicio.NombreServicio);
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
                string strMsjError = $"ERROR|ValidaSiEsDiaHabil|No se pudo recuperar dia habil. Excepcion: {ex}";
                Config.EscribirLog(strMsjError, _configuracionServicio.NombreServicio);
                throw;
            }
        }

        /// <summary>
        /// Obtiene los parametros del sistema necesario para la operacion del servicio, incluye horarios de inicio y finalizacion de operaciones
        /// </summary>
        /// <returns>Dictionary<string, string></returns>
        public async Task<Dictionary<string, string>> ObtenerParametrosSistema()
        {
            try
            {
                StoredProcedureResult<MapearCamposGenerales> resultadoSp =
                await EjecutarStoredProcedureQuery<MapearCamposGenerales>(
                EnumProcedimientosAlmacenados.usp_ParametrosSistema);

                if (resultadoSp.Data.Count == 0 || resultadoSp.MensajeError != null)
                {
                    string strMsjError = "ERROR|ObtenerParametrosSistema|" + resultadoSp.MensajeError;
                    Config.EscribirLog(strMsjError, _configuracionServicio.NombreServicio);
                    throw new Exception(strMsjError);
                }

                Dictionary<string, string> diccParametros = new Dictionary<string, string>();
                foreach (MapearCamposGenerales parametro in resultadoSp.Data)
                {
                    diccParametros.Add(parametro.Id_parametro, parametro.Valor);
                }
                return diccParametros;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerParametrosSistema: " + ex, _configuracionServicio.NombreServicio);
                throw;
            }
        }

        /// <summary>
        /// Actualiza ejecucion del servicio INICIO
        /// </summary>
        /// <param name="strFolio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task ProcesoActualizaEjecucion(string strNombreServicio, int intgpId, string strNombreProceso)
        {
            try
            {
                StoredProcedureResult resultActualizaEjec = await ActualizaEJecucion(strNombreServicio);
                if (resultActualizaEjec.MensajeError != null &&
                    resultActualizaEjec.Exception != null &&
                    !resultActualizaEjec.Exito)
                {
                    string strMsjErro = $"ERROR|ProcesoActualizaEjecucion| " +
                        $"Error al ejecutar consulta. gpId:{intgpId}, proceso:{strNombreProceso}, Ex: {resultActualizaEjec.Exception}, Mensaje: {resultActualizaEjec.MensajeError}";

                    Config.EscribirLog(strMsjErro, _configuracionServicio.NombreServicio);
                    throw new Exception(strMsjErro);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ProcesoActualizaEjecucion: " + ex, _configuracionServicio.NombreServicio);
                throw;
            }
        }

        /// <summary>
        /// Actualiza terminacion del proceso
        /// </summary>
        /// <param name="strNombreServicio"></param>
        /// <param name="intgpId"></param>
        /// <param name="strNombreProceso"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task ProcesoActualizaTerminacion(string strNombreServicio, int intgpId, string strNombreProceso)
        {
            try
            {
                StoredProcedureResult resultActualizaTerm = await ActualizaTerminacion(strNombreServicio);
                if (resultActualizaTerm.MensajeError != null && resultActualizaTerm.Exception != null && !resultActualizaTerm.Exito)
                {
                    string strMsjErro = $"ERROR|ProcesoActualizaTerminacion| " +
                        $"Error al ejecutar consulta. gpId:{intgpId}, proceso:{strNombreProceso}, Ex: {resultActualizaTerm.Exception}, Mensaje: {resultActualizaTerm.MensajeError}";

                    Config.EscribirLog(strMsjErro, _configuracionServicio.NombreServicio);
                    throw new Exception(strMsjErro);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ProcesoActualizaTerminacion " + ex, _configuracionServicio.NombreServicio);
                throw;
            }
        }
    }
}

using Aplicacion;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Infraestructura.Servicio;
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
using System.Threading;
using System.Threading.Tasks;

namespace Swf_IN_ControladorServicios
{
    /// <summary>
    /// Clase principal serviceHost
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private ConfiguracionServicio _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.Swf_IN_ControladorServicios.ToString();
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
                ServicioConfiguracionSettings servicioMainConfiguracion = MUFG_T24.Core.Configuracion.LecturaConfguracionGeneral.ObtenerServicio(NombreServicio.Swf_IN_ControladorServicios.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.StrNombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.RutaArchivoConfiguracion = $@"{EnumRutasGenerales.Path_Log_CMS}{NombreServicio.Swf_IN_ControladorServicios.ToString()}\";
                _configuracionBase.PathLog = servicioMainConfiguracion.Log + servicioMainConfiguracion.NombreServicio;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.configuracion = servicioMainConfiguracion.configuracion;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();

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

            _containerDI.Register<ConfiguracionServicio>(() => (ConfiguracionServicio)_configuracionBase.Clone(), Lifestyle.Scoped);
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
                    await operacionServicio.EjecutaProcedimiento().ConfigureAwait(false);
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

namespace Swf_IN_ControladorServicios
{

    /// <summary>
    /// Clase de entrada.
    /// </summary>
    internal static class Program
    {
        /// <summary>
        /// Punto de entrada Main.
        /// </summary>
        static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                NombreServicio.Swf_IN_ControladorServicios.ToString(),
                () => new ServiceHost(),
                args,
                (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs));
        }

    }

}

App.config
﻿<?xml version="1.0" encoding="utf-8" ?>
<configuration>
    <startup> 
        <supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.8" />
    </startup>
</configuration>

Aplicacion.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{D2BA13B9-2941-402C-910E-CF902AB9F5DC}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Aplicacion</RootNamespace>
    <AssemblyName>AplicacionSWF_IN_CONTROLADORSERVICIOS</AssemblyName>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=x86">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Core" />
    <Reference Include="System.ServiceProcess" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="Enums\EnumComunes.cs" />
    <Compile Include="Interfaces\IControladorServicios.cs" />
    <Compile Include="Interfaces\IOperacionServicio.cs" />
    <Compile Include="Interfaces\IRepositorioTomi.cs" />
    <Compile Include="Modelos\ConfiguracionServicio.cs" />
    <Compile Include="Modelos\Tomi\MapearCamposGenerales.cs" />
    <Compile Include="OperacionServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
    <Compile Include="Service\ControladorServicios.cs" />
  </ItemGroup>
  <ItemGroup />
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


Infraestructura.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Infraestructura</RootNamespace>
    <AssemblyName>InfraestructuraSWF_IN_CONTROLADORSERVICIOS</AssemblyName>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=x86">
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
    <Compile Include="Servicio\RepositorioTomi.cs" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\Aplicacion.csproj">
      <Project>{D2BA13B9-2941-402C-910E-CF902AB9F5DC}</Project>
      <Name>Aplicacion</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


Swf_IN_ControladorServicios.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{D033944C-F2CA-433D-8C6E-2D12ACABD822}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>Swf_IN_ControladorServicios</RootNamespace>
    <AssemblyName>Swf_IN_ControladorServicios</AssemblyName>
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
    <Reference Include="Microsoft.Bcl.AsyncInterfaces, Version=10.0.0.3, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Bcl.AsyncInterfaces.10.0.3\lib\net462\Microsoft.Bcl.AsyncInterfaces.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.10.0.3\lib\net462\Microsoft.Extensions.Configuration.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Abstractions, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.Abstractions.10.0.3\lib\net462\Microsoft.Extensions.Configuration.Abstractions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Binder, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.Binder.10.0.3\lib\net462\Microsoft.Extensions.Configuration.Binder.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.FileExtensions, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.FileExtensions.10.0.3\lib\net462\Microsoft.Extensions.Configuration.FileExtensions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Configuration.Json, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Configuration.Json.10.0.3\lib\net462\Microsoft.Extensions.Configuration.Json.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileProviders.Abstractions, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.FileProviders.Abstractions.10.0.3\lib\net462\Microsoft.Extensions.FileProviders.Abstractions.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileProviders.Physical, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.FileProviders.Physical.10.0.3\lib\net462\Microsoft.Extensions.FileProviders.Physical.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.FileSystemGlobbing, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.FileSystemGlobbing.10.0.3\lib\net462\Microsoft.Extensions.FileSystemGlobbing.dll</HintPath>
    </Reference>
    <Reference Include="Microsoft.Extensions.Primitives, Version=10.0.0.3, Culture=neutral, PublicKeyToken=adb9793829ddae60, processorArchitecture=MSIL">
      <HintPath>packages\Microsoft.Extensions.Primitives.10.0.3\lib\net462\Microsoft.Extensions.Primitives.dll</HintPath>
    </Reference>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <HintPath>packages\SimpleInjector.5.5.0\lib\net461\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Buffers, Version=4.0.5.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.Buffers.4.6.1\lib\net462\System.Buffers.dll</HintPath>
    </Reference>
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.IO.Pipelines, Version=10.0.0.3, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.IO.Pipelines.10.0.3\lib\net462\System.IO.Pipelines.dll</HintPath>
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
    <Reference Include="System.Text.Encodings.Web, Version=10.0.0.3, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.Text.Encodings.Web.10.0.3\lib\net462\System.Text.Encodings.Web.dll</HintPath>
    </Reference>
    <Reference Include="System.Text.Json, Version=10.0.0.3, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51, processorArchitecture=MSIL">
      <HintPath>packages\System.Text.Json.10.0.3\lib\net462\System.Text.Json.dll</HintPath>
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
    <ProjectReference Include="..\Aplicacion\Aplicacion.csproj">
      <Project>{D2BA13B9-2941-402C-910E-CF902AB9F5DC}</Project>
      <Name>Aplicacion</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura\Infraestructura.csproj">
      <Project>{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}</Project>
      <Name>Infraestructura</Name>
    </ProjectReference>
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


Swf_IN_ControladorServicios.sln

Microsoft Visual Studio Solution File, Format Version 12.00
# Visual Studio Version 18
VisualStudioVersion = 18.5.11723.231 stable
MinimumVisualStudioVersion = 10.0.40219.1
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Swf_IN_ControladorServicios", "Swf_IN_ControladorServicios.csproj", "{D033944C-F2CA-433D-8C6E-2D12ACABD822}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Aplicacion", "..\Aplicacion\Aplicacion.csproj", "{D2BA13B9-2941-402C-910E-CF902AB9F5DC}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Infraestructura", "..\Infraestructura\Infraestructura.csproj", "{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "PruebasUnitarias_Swf_IN_ControladorServicios", "..\PruebasUnitarias_Swf_IN_ControladorServicios\PruebasUnitarias_Swf_IN_ControladorServicios.csproj", "{029DB447-0C86-4178-A56C-4F6C756BC678}"
EndProject
Global
	GlobalSection(SolutionConfigurationPlatforms) = preSolution
		Debug|Any CPU = Debug|Any CPU
		Debug|x86 = Debug|x86
		Release|Any CPU = Release|Any CPU
		Release|x86 = Release|x86
	EndGlobalSection
	GlobalSection(ProjectConfigurationPlatforms) = postSolution
		{D033944C-F2CA-433D-8C6E-2D12ACABD822}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{D033944C-F2CA-433D-8C6E-2D12ACABD822}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{D033944C-F2CA-433D-8C6E-2D12ACABD822}.Debug|x86.ActiveCfg = Debug|x86
		{D033944C-F2CA-433D-8C6E-2D12ACABD822}.Debug|x86.Build.0 = Debug|x86
		{D033944C-F2CA-433D-8C6E-2D12ACABD822}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{D033944C-F2CA-433D-8C6E-2D12ACABD822}.Release|Any CPU.Build.0 = Release|Any CPU
		{D033944C-F2CA-433D-8C6E-2D12ACABD822}.Release|x86.ActiveCfg = Release|x86
		{D033944C-F2CA-433D-8C6E-2D12ACABD822}.Release|x86.Build.0 = Release|x86
		{D2BA13B9-2941-402C-910E-CF902AB9F5DC}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{D2BA13B9-2941-402C-910E-CF902AB9F5DC}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{D2BA13B9-2941-402C-910E-CF902AB9F5DC}.Debug|x86.ActiveCfg = Debug|x86
		{D2BA13B9-2941-402C-910E-CF902AB9F5DC}.Debug|x86.Build.0 = Debug|x86
		{D2BA13B9-2941-402C-910E-CF902AB9F5DC}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{D2BA13B9-2941-402C-910E-CF902AB9F5DC}.Release|Any CPU.Build.0 = Release|Any CPU
		{D2BA13B9-2941-402C-910E-CF902AB9F5DC}.Release|x86.ActiveCfg = Release|x86
		{D2BA13B9-2941-402C-910E-CF902AB9F5DC}.Release|x86.Build.0 = Release|x86
		{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}.Debug|x86.ActiveCfg = Debug|x86
		{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}.Debug|x86.Build.0 = Debug|x86
		{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}.Release|Any CPU.Build.0 = Release|Any CPU
		{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}.Release|x86.ActiveCfg = Release|x86
		{FE64292F-3102-4ACA-A034-18FC4A6BBBE6}.Release|x86.Build.0 = Release|x86
		{029DB447-0C86-4178-A56C-4F6C756BC678}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{029DB447-0C86-4178-A56C-4F6C756BC678}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{029DB447-0C86-4178-A56C-4F6C756BC678}.Debug|x86.ActiveCfg = Debug|x86
		{029DB447-0C86-4178-A56C-4F6C756BC678}.Debug|x86.Build.0 = Debug|x86
		{029DB447-0C86-4178-A56C-4F6C756BC678}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{029DB447-0C86-4178-A56C-4F6C756BC678}.Release|Any CPU.Build.0 = Release|Any CPU
		{029DB447-0C86-4178-A56C-4F6C756BC678}.Release|x86.ActiveCfg = Release|x86
		{029DB447-0C86-4178-A56C-4F6C756BC678}.Release|x86.Build.0 = Release|x86
	EndGlobalSection
	GlobalSection(SolutionProperties) = preSolution
		HideSolutionNode = FALSE
	EndGlobalSection
	GlobalSection(ExtensibilityGlobals) = postSolution
		SolutionGuid = {9DDF43B9-1825-489F-81A6-D97678A16CA8}
	EndGlobalSection
EndGlobal


packages.config
﻿<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="Microsoft.Bcl.AsyncInterfaces" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration.Abstractions" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration.Binder" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration.FileExtensions" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.Configuration.Json" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.FileProviders.Abstractions" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.FileProviders.Physical" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.FileSystemGlobbing" version="10.0.3" targetFramework="net48" />
  <package id="Microsoft.Extensions.Primitives" version="10.0.3" targetFramework="net48" />
  <package id="SimpleInjector" version="5.5.0" targetFramework="net48" />
  <package id="System.Buffers" version="4.6.1" targetFramework="net48" />
  <package id="System.IO.Pipelines" version="10.0.3" targetFramework="net48" />
  <package id="System.Memory" version="4.6.3" targetFramework="net48" />
  <package id="System.Numerics.Vectors" version="4.6.1" targetFramework="net48" />
  <package id="System.Runtime.CompilerServices.Unsafe" version="6.1.2" targetFramework="net48" />
  <package id="System.Text.Encodings.Web" version="10.0.3" targetFramework="net48" />
  <package id="System.Text.Json" version="10.0.3" targetFramework="net48" />
  <package id="System.Threading.Tasks.Extensions" version="4.6.3" targetFramework="net48" />
  <package id="System.ValueTuple" version="4.6.2" targetFramework="net48" />
</packages>


.gitignore
## Ignore Visual Studio temporary files, build results, and
## files generated by popular Visual Studio add-ons.
##
## Get latest from `dotnet new gitignore`

# dotenv files
.env
/setup

# User-specific files
*.rsuser
*.suo
*.user
*.userosscache
*.sln.docstates

# User-specific files (MonoDevelop/Xamarin Studio)
*.userprefs

# Mono auto generated files
mono_crash.*

# Build results
[Dd]ebug/
[Dd]ebugPublic/
[Rr]elease/
[Rr]eleases/
x64/
x86/
[Ww][Ii][Nn]32/
[Aa][Rr][Mm]/
[Aa][Rr][Mm]64/
bld/
[Bb]in/
[Oo]bj/
[Ll]og/
[Ll]ogs/

# Visual Studio 2015/2017 cache/options directory
.vs/
# Uncomment if you have tasks that create the project's static files in wwwroot
#wwwroot/

# Visual Studio 2017 auto generated files
Generated\ Files/

# MSTest test Results
[Tt]est[Rr]esult*/
[Bb]uild[Ll]og.*

# NUnit
*.VisualState.xml
TestResult.xml
nunit-*.xml

# Build Results of an ATL Project
[Dd]ebugPS/
[Rr]eleasePS/
dlldata.c

# Benchmark Results
BenchmarkDotNet.Artifacts/

# .NET
project.lock.json
project.fragment.lock.json
artifacts/

# Tye
.tye/

# ASP.NET Scaffolding
ScaffoldingReadMe.txt

# StyleCop
StyleCopReport.xml

# Files built by Visual Studio
*_i.c
*_p.c
*_h.h
*.ilk
*.meta
*.obj
*.iobj
*.pch
*.pdb
*.ipdb
*.pgc
*.pgd
*.rsp
*.sbr
*.tlb
*.tli
*.tlh
*.tmp
*.tmp_proj
*_wpftmp.csproj
*.log
*.tlog
*.vspscc
*.vssscc
.builds
*.pidb
*.svclog
*.scc

# Chutzpah Test files
_Chutzpah*

# Visual C++ cache files
ipch/
*.aps
*.ncb
*.opendb
*.opensdf
*.sdf
*.cachefile
*.VC.db
*.VC.VC.opendb

# Visual Studio profiler
*.psess
*.vsp
*.vspx
*.sap

# Visual Studio Trace Files
*.e2e

# TFS 2012 Local Workspace
$tf/

# Guidance Automation Toolkit
*.gpState

# ReSharper is a .NET coding add-in
_ReSharper*/
*.[Rr]e[Ss]harper
*.DotSettings.user

# TeamCity is a build add-in
_TeamCity*

# DotCover is a Code Coverage Tool
*.dotCover

# AxoCover is a Code Coverage Tool
.axoCover/*
!.axoCover/settings.json

# Coverlet is a free, cross platform Code Coverage Tool
coverage*.json
coverage*.xml
coverage*.info

# Visual Studio code coverage results
*.coverage
*.coveragexml

# NCrunch
_NCrunch_*
.*crunch*.local.xml
nCrunchTemp_*

# MightyMoose
*.mm.*
AutoTest.Net/

# Web workbench (sass)
.sass-cache/

# Installshield output folder
[Ee]xpress/

# DocProject is a documentation generator add-in
DocProject/buildhelp/
DocProject/Help/*.HxT
DocProject/Help/*.HxC
DocProject/Help/*.hhc
DocProject/Help/*.hhk
DocProject/Help/*.hhp
DocProject/Help/Html2
DocProject/Help/html

# Click-Once directory
publish/

# Publish Web Output
*.[Pp]ublish.xml
*.azurePubxml
# Note: Comment the next line if you want to checkin your web deploy settings,
# but database connection strings (with potential passwords) will be unencrypted
*.pubxml
*.publishproj

# Microsoft Azure Web App publish settings. Comment the next line if you want to
# checkin your Azure Web App publish settings, but sensitive information contained
# in these scripts will be unencrypted
PublishScripts/

# NuGet Packages
*.nupkg
# NuGet Symbol Packages
*.snupkg
# The packages folder can be ignored because of Package Restore
**/[Pp]ackages/*
# except build/, which is used as an MSBuild target.
!**/[Pp]ackages/build/
# Uncomment if necessary however generally it will be regenerated when needed
#!**/[Pp]ackages/repositories.config
# NuGet v3's project.json files produces more ignorable files
*.nuget.props
*.nuget.targets

# Microsoft Azure Build Output
csx/
*.build.csdef

# Microsoft Azure Emulator
ecf/
rcf/

# Windows Store app package directories and files
AppPackages/
BundleArtifacts/
Package.StoreAssociation.xml
_pkginfo.txt
*.appx
*.appxbundle
*.appxupload

# Visual Studio cache files
# files ending in .cache can be ignored
*.[Cc]ache
# but keep track of directories ending in .cache
!?*.[Cc]ache/

# Others
ClientBin/
~$*
*~
*.dbmdl
*.dbproj.schemaview
*.jfm
*.pfx
*.publishsettings
orleans.codegen.cs

# Including strong name files can present a security risk
# (https://github.com/github/gitignore/pull/2483#issue-259490424)
#*.snk

# Since there are multiple workflows, uncomment next line to ignore bower_components
# (https://github.com/github/gitignore/pull/1529#issuecomment-104372622)
#bower_components/

# RIA/Silverlight projects
Generated_Code/

# Backup & report files from converting an old project file
# to a newer Visual Studio version. Backup files are not needed,
# because we have git ;-)
_UpgradeReport_Files/
Backup*/
UpgradeLog*.XML
UpgradeLog*.htm
ServiceFabricBackup/
*.rptproj.bak

# SQL Server files
*.mdf
*.ldf
*.ndf

# Business Intelligence projects
*.rdl.data
*.bim.layout
*.bim_*.settings
*.rptproj.rsuser
*- [Bb]ackup.rdl
*- [Bb]ackup ([0-9]).rdl
*- [Bb]ackup ([0-9][0-9]).rdl

# Microsoft Fakes
FakesAssemblies/

# GhostDoc plugin setting file
*.GhostDoc.xml

# Node.js Tools for Visual Studio
.ntvs_analysis.dat
node_modules/

# Visual Studio 6 build log
*.plg

# Visual Studio 6 workspace options file
*.opt

# Visual Studio 6 auto-generated workspace file (contains which files were open etc.)
*.vbw

# Visual Studio 6 auto-generated project file (contains which files were open etc.)
*.vbp

# Visual Studio 6 workspace and project file (working project files containing files to include in project)
*.dsw
*.dsp

# Visual Studio 6 technical files
*.ncb
*.aps

# Visual Studio LightSwitch build output
**/*.HTMLClient/GeneratedArtifacts
**/*.DesktopClient/GeneratedArtifacts
**/*.DesktopClient/ModelManifest.xml
**/*.Server/GeneratedArtifacts
**/*.Server/ModelManifest.xml
_Pvt_Extensions

# Paket dependency manager
.paket/paket.exe
paket-files/

# FAKE - F# Make
.fake/

# CodeRush personal settings
.cr/personal

# Python Tools for Visual Studio (PTVS)
__pycache__/
*.pyc

# Cake - Uncomment if you are using it
# tools/**
# !tools/packages.config

# Tabs Studio
*.tss

# Telerik's JustMock configuration file
*.jmconfig

# BizTalk build output
*.btp.cs
*.btm.cs
*.odx.cs
*.xsd.cs

# OpenCover UI analysis results
OpenCover/

# Azure Stream Analytics local run output
ASALocalRun/

# MSBuild Binary and Structured Log
*.binlog

# NVidia Nsight GPU debugger configuration file
*.nvuser

# MFractors (Xamarin productivity tool) working folder
.mfractor/

# Local History for Visual Studio
.localhistory/

# Visual Studio History (VSHistory) files
.vshistory/

# BeatPulse healthcheck temp database
healthchecksdb

# Backup folder for Package Reference Convert tool in Visual Studio 2017
MigrationBackup/

# Ionide (cross platform F# VS Code tools) working folder
.ionide/

# Fody - auto-generated XML schema
FodyWeavers.xsd

# VS Code files for those working on multiple tools
.vscode/*
!.vscode/settings.json
!.vscode/tasks.json
!.vscode/launch.json
!.vscode/extensions.json
*.code-workspace

# Local History for Visual Studio Code
.history/

# Windows Installer files from build outputs
*.cab
*.msi
*.msix
*.msm
*.msp

# JetBrains Rider
*.sln.iml
.idea/

##
## Visual studio for Mac
##


# globs
Makefile.in
*.userprefs
*.usertasks
config.make
config.status
aclocal.m4
install-sh
autom4te.cache/
*.tar.gz
tarballs/
test-results/

# Mac bundle stuff
*.dmg
*.app

# content below from: https://github.com/github/gitignore/blob/main/Global/macOS.gitignore
# General
.DS_Store
.AppleDouble
.LSOverride

# Icon must end with two \r
Icon


# Thumbnails
._*

# Files that might appear in the root of a volume
.DocumentRevisions-V100
.fseventsd
.Spotlight-V100
.TemporaryItems
.Trashes
.VolumeIcon.icns
.com.apple.timemachine.donotpresent

# Directories potentially created on remote AFP share
.AppleDB
.AppleDesktop
Network Trash Folder
Temporary Items
.apdisk

# content below from: https://github.com/github/gitignore/blob/main/Global/Windows.gitignore
# Windows thumbnail cache files
Thumbs.db
ehthumbs.db
ehthumbs_vista.db

# Dump file
*.stackdump

# Folder config file
[Dd]esktop.ini

# Recycle Bin used on file shares
$RECYCLE.BIN/

# Windows Installer files
*.cab
*.msi
*.msix
*.msm
*.msp

# Windows shortcuts
*.lnk

# Vim temporary swap files
*.swp


README.md
  # SWF_IN_ControladorServicios



## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin http://gitlab.mufg.mx/mufg-it/mufg-sda/middleware-nttdata/SWIFT/incoming/SWF_IN_ControladorServicios.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](http://gitlab.mufg.mx/mufg-it/mufg-sda/middleware-nttdata/SWIFT/incoming/SWF_IN_ControladorServicios/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Set auto-merge](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing (SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thanks to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README

Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage
Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest example of usage that you can demonstrate, while providing links to more sophisticated examples if they are too long to reasonably include in the README.

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.
