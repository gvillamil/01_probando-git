using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Operacion principal del servicio.
    /// </summary>
    public class OperacionServicio : Interfaces.IOperacionServicio
    {
        // Sistema Interfaces.
        private readonly ConfiguracionServicio _configuracion;
        private readonly IParametrizacion _iParametrizacion;


        // Interfaces.proyecto tuyo
        private readonly IParametrosOperaciones _IparametrosOperaciones;
        private readonly IOperacionesSpidAccionistas _IOperacionesSpidAccionistas;
        private IOperacionesMINDS _IOperacionesMINDS;

        /// <summary>
        /// Inicializa una nueva instancia del servicio principal de operaciones SPID Accionistas.
        /// </summary>
        /// <param name="configuracion">Configuración del servicio.</param>
        /// <param name="iParametrizacion">Componente de parametrización.</param>
        /// <param name="iparametrosOperaciones">Repositorio de parámetros y operaciones.</param>
        /// <param name="iOperacionesSpidAccionistas">Operaciones de SPID Accionistas.</param>
        public OperacionServicio(ConfiguracionServicio configuracion, IParametrizacion iParametrizacion, IParametrosOperaciones iparametrosOperaciones, IOperacionesSpidAccionistas iOperacionesSpidAccionistas, IOperacionesMINDS operacionesMIND)
        {
            _configuracion = configuracion;
            _IparametrosOperaciones = iparametrosOperaciones;
            _IOperacionesSpidAccionistas = iOperacionesSpidAccionistas;
            _iParametrizacion = iParametrizacion;
            _IOperacionesMINDS = operacionesMIND;
        }

        /// <summary>
        /// Ejecuta el flujo principal de procesamiento de operaciones,
        /// carga parámetros del sistema, valida horarios de operación
        /// y procesa entidades de accionistas.
        /// </summary>
        public async Task EjecutaProcedimientosAsync()
        {
            // Actualiza ejecucion
            await _IparametrosOperaciones.MarcarServicioEnEjecucion(_configuracion.NombreServicio);

            // Obtiene los parametros del sistema.
            var dicParametrosSistema = await _IparametrosOperaciones.parametrosSistema();


            // Obtiene los parametros de SpidAccionista.
            var dicParametrosAccionistas = await _IparametrosOperaciones.parametrosAccionistas();


            //Valida los horarios de operación...
            TimeSpan timeHoraSistema = _iParametrizacion.ObtenerHoraSistema();

            //Pregunta a la base de datos si el dia es habil segun el registro en base de datos.

            _configuracion.EsDiaHabil = await _IparametrosOperaciones.BEsDiaHabil();

            if (dicParametrosSistema == null || dicParametrosAccionistas == null) throw new Exception("No se encontraron los parametros del sistema y/o de los accionistas.");


            //Carga la configuración de los parámetros para el inicio de operaciones de la base de datos.
            foreach (var paramSistema in dicParametrosSistema)
            {
                if(!Enum.TryParse(paramSistema.Key, out ParametrosSistema parametro)) continue; // Cambia el tipo string a un enum
                switch (parametro)
                {
                    case ParametrosSistema.HORA_LECTURA_INICIO:
                        _configuracion.StrHoraLecturaInicio = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_LECTURA_INICIO.ToString());
                        Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de inicio de lectura '" + paramSistema.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosSistema.HORA_LECTURA_FIN:
                        _configuracion.StrHoraLecturaFin = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_LECTURA_FIN.ToString());
                        Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de finalización de lectura '" + paramSistema.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosSistema.HORA_OP_INICIO:
                        _configuracion.StrHoraOperacionesInicio = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_OP_INICIO.ToString());
                        Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de inicio de operaciones '" + paramSistema.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosSistema.HORA_OP_FIN:
                        _configuracion.StrHoraOperacionesFin = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, ParametrosSistema.HORA_OP_FIN.ToString());
                        Config.EscribirLog("INFO|obtenerParametrosSistema|Hora de finalización de operaciones '" + paramSistema.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;
                }
            }

            //Carga la configuracion de los parametros de Accionista para validacion.
            foreach (var paramAccionistas in dicParametrosAccionistas) 
            {
                var strParametroLimpio = paramAccionistas.Key.Contains("_%") ? paramAccionistas.Key.Replace("_%", "") : paramAccionistas.Key; //Quita _% para que funcione el Enum.

                if (!Enum.TryParse(strParametroLimpio, out ParametrosAccionistasE parametro)) continue; // Cambia el tipo string a un enum
                switch (parametro)
                {
                    case ParametrosAccionistasE.TOMI_activo:
                        _configuracion.BHotScanBTMUMActivo = paramAccionistas.Value == "1";
                        Config.EscribirLog("INFO|obtenerParametrosSistema|HotScan Activo'" + paramAccionistas.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosAccionistasE.FCM_activo:
                        _configuracion.BFCMBTMUMActivo = paramAccionistas.Value == "1";
                        Config.EscribirLog("INFO|obtenerParametrosSistema|HotScan Activo'" + paramAccionistas.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosAccionistasE.minds_activo:
                        _configuracion.BHotScanMindsActivo = paramAccionistas.Value == "1";
                        Config.EscribirLog("INFO|obtenerParametrosSistema|Minds Activo'" + paramAccionistas.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosAccionistasE.TOMI_bloquear:
                        _configuracion.BHotScanBTMUMBloquear = paramAccionistas.Value == "1";
                        Config.EscribirLog("INFO|obtenerParametrosSistema|HotScan bloquear'" + paramAccionistas.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosAccionistasE.minds_bloquear:
                        _configuracion.BHotScanMindsBloquear = paramAccionistas.Value == "1";
                        Config.EscribirLog("INFO|obtenerParametrosSistema|Minds bloquear'" + paramAccionistas.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosAccionistasE.TOMI_Scan:
                        _configuracion.StrHotScanBTMUMPorc = paramAccionistas.Value;
                        Config.EscribirLog("INFO|obtenerParametrosSistema|% de HotScan'" + paramAccionistas.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;

                    case ParametrosAccionistasE.minds_Scan:
                        _configuracion.StrHotScanMindsPorc = paramAccionistas.Value;
                        Config.EscribirLog("INFO|obtenerParametrosSistema|% de Minds'" + paramAccionistas.Value + "'.", NombreServicio.SPID_Accionistas.ToString());
                        break;
                }
                
            }

            ///Valida que los parametros obtenidos de la base dedatos de las horas de ejecución no vengan vacios, de lo contrario enviara una excepcion.

            if (_configuracion.StrHoraOperacionesInicio == null)
                throw new Exception("No se especifica la hora de inicio de operaciones");
            if (_configuracion.StrHoraOperacionesFin == null)
                throw new Exception("No se especifica la hora de finalización de operaciones");

            ///Validamos el horario de las operaciones, indicara si la bandera es falso o verdadero. 
            bool boHoraOperacionInicio = (timeHoraSistema >= _configuracion.StrHoraOperacionesInicio && timeHoraSistema <= _configuracion.StrHoraOperacionesFin);
            Config.EscribirLog($"INFO|Ejecuta_Procedimientos|{(boHoraOperacionInicio ? "inicio" : "Termina")}horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.SPID_Accionistas.ToString());


            // Obtencion de la fecha del sistema.
            var horaActual = DateTime.Now;
            _configuracion.StrFechaSistema = new TimeSpan(horaActual.Hour, horaActual.Minute, 0);


            if (_configuracion.StrFechaSistema >= _configuracion.StrHoraLecturaInicio && _configuracion.StrFechaSistema <= _configuracion.StrHoraLecturaFin) // Inicio Horario
            {
                if (!_configuracion.BHorarioLectura)
                {
                    _configuracion.BHorarioLectura = true;
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Inicio horario de operaciones fecha sistema, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.SPID_Accionistas.ToString());
                }
            }
            else // Termina Horario
            {
                if (_configuracion.BHorarioLectura) 
                {   
                    _configuracion.BHorarioLectura = false;
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Termina horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.SPID_Accionistas.ToString());
                }
            }

            var estaFueraHorarioLectura = !_configuracion.BHorarioLectura;

            if (estaFueraHorarioLectura) // Fuera de Horario
            {
                Config.EscribirLog($"ERROR|Ejecuta_Procedimientos| El Servicio esta fuera de Horario.", NombreServicio.SPID_Accionistas.ToString());
                await _IparametrosOperaciones.EnviaAlerta(new AlertaDTO
                {
                    TipoOPeracion = null,
                    Folio = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = "ERROR",
                    Descipcion = "El servicio está fuera de horario",
                    PId = (int)_configuracion.Pid,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = new Exception("El servicio '" + _configuracion.NombreServicio + "' está fuera de horario.")
                });
            }

            ///Inicia los servicios asincronos segun el horario de operacion de cada uno.

            try
            {
                if (estaFueraHorarioLectura) return;

                //logica del proyecto a migrar

              if ((bool)_configuracion.EsDiaHabil)  // Valido que sea día hábil (bool)_configuracion.BoEsDiaHabil
                {
                    if (_configuracion.StrFechaSistema >= _configuracion.StrHoraOperacionesInicio && _configuracion.StrFechaSistema <= _configuracion.StrHoraOperacionesFin)
                    {
                        _configuracion.BenHorario = true;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Inicio horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.SPID_Accionistas.ToString());

                        // Ejecucion de operaciones dentro de horario de operaciones
                        Dictionary<string, DAccionistas> respListaAccionistas = await _IOperacionesSpidAccionistas.ObtenerListaClientes();
                        if (respListaAccionistas?.Any() == true) await ValidaEntidades(respListaAccionistas);

                    }
                }
                else
                {
                    if ((bool)_configuracion.BenHorario) //Fuera de horario
                    {
                        _configuracion.BenHorario = false;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Termina horario de operaciones, HoraSistema='{timeHoraSistema:hh\\:mm}'.", NombreServicio.SPID_Accionistas.ToString());
                    }
                }

                
            }
            catch (Exception ex) 
            {
                Config.EscribirLog("ERROR|Ejecuta_Procedimientos|Se presenta exception: " + ex.Message, NombreServicio.SPID_Accionistas.ToString());
                await _IparametrosOperaciones.EnviaAlerta(new AlertaDTO
                {
                    TipoOPeracion = null,
                    Folio = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = "CRITICO",
                    Descipcion = "Error en ejecuta_procedimientos",
                    PId = (int)_configuracion.Pid,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = ex
                });
                Config.EscribirLog("ERROR|Ejecuta_Procedimientos|Se forza la finalización del servicio.", NombreServicio.SPID_Accionistas.ToString());
                throw new Exception("Error en EjecutaProcedimientosAsync",ex);
            }
            finally
            {
                try
                {
                    // Fin de los procesos a realizar
                    await _IparametrosOperaciones.MarcarServicioEnFinalizacion(_configuracion.NombreServicio);
                }
                catch (Exception exFinally)
                {
                    Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|no fue posible maracar el servicio en finalización: {exFinally}", NombreServicio.SPID_Accionistas.ToString());
                }
            }
        }

        /// <summary>
        /// Valida entidades.
        /// </summary>
        /// <returns></returns>
        public async Task ValidaEntidades(Dictionary<string, DAccionistas> listaAccionistas)
        {

            try
            {
                if ((listaAccionistas?.Count ?? 0) == 0) return;

                var listaCld = string.Join(",", listaAccionistas.Keys);
                var listaCldMinds = string.Join(",", listaAccionistas.Keys.Select(accionista => $"'{accionista}'"));


                //------------------------------------------
                //              CLIENTES                   -
                //------------------------------------------

                var strEstatusClientes = await _IOperacionesSpidAccionistas.ObtenerEstatusBatch(EnumEstatusComunes.ID_CLIENTES_BATCH.ToString());
                Config.EscribirLog($"INFO|ValidaEntidades|Estatus CLIENTES: {strEstatusClientes}", NombreServicio.SPID_Accionistas.ToString());
                if (strEstatusClientes == EnumEstatusComunes.PENDIENTE.ToString())
                {
                    bool insertC = false;
                    Config.EscribirLog($"INFO|ValidaEntidades|Comienza la validacion de los clientes, CDL ({listaCld})", NombreServicio.SPID_Accionistas.ToString());

                    await _IOperacionesSpidAccionistas.EliminarPorTipo(EnumEstatusComunes.CUSTOMER, listaCld);

                    foreach (var cliente in listaAccionistas.Values)
                    {
                        if (!insertC) Config.EscribirLog("INFO|ValidaEntidades|Se procede a realizar la inserción de todos los clientes del listado.");
                        await _IOperacionesSpidAccionistas.InsertarCliente(cliente);
                        insertC = true;

                    }

                    Config.EscribirLog($"INFO|Valida_Entidades|Marcar proceso {EnumEstatusComunes.ID_CLIENTES_BATCH} como terminado.");
                    await _IOperacionesSpidAccionistas.MarcarBatchProcesado(EnumEstatusComunes.ID_CLIENTES_BATCH.ToString());
                }

                //------------------------------------------
                //              ACCIONISTAS                -
                //------------------------------------------

                var strEstatusAccionistas = await _IOperacionesSpidAccionistas.ObtenerEstatusBatch(EnumEstatusComunes.ID_ACCIONISTAS_BATCH.ToString());
                Config.EscribirLog($"INFO|ValidaEntidades|Estatus ACCIONISTAS: {strEstatusAccionistas}", NombreServicio.SPID_Accionistas.ToString());
                if (strEstatusAccionistas == EnumEstatusComunes.PENDIENTE.ToString())
                {
                    bool insertA = false;
                    Config.EscribirLog($"INFO|ValidaEntidades|Comienza la actualizacion de las fechas de constitucion", NombreServicio.SPID_Accionistas.ToString());

                    await _IOperacionesSpidAccionistas.ActualizarFechasConstitucion(listaCldMinds);

                    Config.EscribirLog("INFO|Valida_Entidades|Comienza la validación de los accionistas, CDL (" + listaCld + ").");

                    await _IOperacionesSpidAccionistas.EliminarPorTipo(EnumEstatusComunes.SHAREHOLDER.ToString(), listaCld);

                    var accionistas = await _IOperacionesMINDS.ObtenerAccionistas(listaCldMinds);

                    var dClientesConAccionistas = new HashSet<ulong>();

                    foreach (var accionista in accionistas)
                    {

                        if (!insertA) Config.EscribirLog("INFO|Valida_Entidades|Se procede a realizar la inserción de todos los Accionistas del listado.");
                        await _IOperacionesSpidAccionistas.InsertarAccionista(accionista);
                        insertA = true;

                        ulong cdlAccionista;
                        if (ulong.TryParse(accionista.CDL, out cdlAccionista))
                        {
                            dClientesConAccionistas.Add(cdlAccionista);
                        }

                    }

                    foreach (var cliente in listaAccionistas.Values)
                    {
                        ulong cdlCliente;
                        var tieneAccionistas = ulong.TryParse(cliente.RECID, out cdlCliente) && dClientesConAccionistas.Contains(cdlCliente);
                        var estatus = tieneAccionistas ? EnumEstatusComunes.PROCESADO.ToString() : EnumEstatusComunes.SIN_ACCIONISTAS.ToString();
                        await _IOperacionesSpidAccionistas.ActualizarEstatusCliente(cliente.RECID, estatus);
                    }

                    Config.EscribirLog($"INFO|Valida_Entidades|Marcar proceso {EnumEstatusComunes.ID_ACCIONISTAS_BATCH} como terminado.");
                    await _IOperacionesSpidAccionistas.MarcarBatchProcesado(EnumEstatusComunes.ID_ACCIONISTAS_BATCH.ToString());

                }

                //------------------------------------------
                //             REALTED PARTIES             -
                //------------------------------------------
                var strEstatusPartesRelacionadas = await _IOperacionesSpidAccionistas.ObtenerEstatusBatch(EnumEstatusComunes.ID_REALTED_PARTIES_BATCH.ToString());
                Config.EscribirLog($"INFO|ValidaEntidades|Estatus REALTED PARTIES : {strEstatusPartesRelacionadas}", NombreServicio.SPID_Accionistas.ToString());
                if (strEstatusPartesRelacionadas == EnumEstatusComunes.PENDIENTE.ToString())
                {
                    bool insertt = false;
                    Config.EscribirLog($"INFO|ValidaEntidades|Comienza la validacion de REALTED PARTIES", NombreServicio.SPID_Accionistas.ToString());
                    await _IOperacionesSpidAccionistas.EliminarPorTipo("VENDORS", listaCld);
                    var lRelacion = await _IOperacionesMINDS.ObtenerPartesRelacionadas(listaCldMinds);

                    foreach (var relacion in lRelacion)
                    {
                        if (!insertt) Config.EscribirLog("INFO|Valida_Entidades|Se procede a realizar la inserción de todos los realted parties del listado.");
                        await _IOperacionesSpidAccionistas.InsertarRelacionadas(relacion);
                        insertt = true;
                    }
                    Config.EscribirLog($"INFO|Valida_Entidades|Marcar proceso {EnumEstatusComunes.ID_REALTED_PARTIES_BATCH} como terminado.");
                    await _IOperacionesSpidAccionistas.MarcarBatchProcesado(EnumEstatusComunes.ID_REALTED_PARTIES_BATCH.ToString());
                }

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaEntidades|Ocurre Excepcion:{ex}", NombreServicio.SPID_Accionistas.ToString());
            }
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Operacion servicio.
    /// </summary>
    public interface IOperacionServicio
    {
        Task EjecutaProcedimientosAsync();
    }
}

﻿using Aplicacion.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Operaciones minds
    /// </summary>
    public interface IOperacionesMINDS
    {
        Task<List<DFechaConstitucion>> ObtenerFechasConstitucion(string CDL_LISTA);
        Task<List<DOAccionista>> ObtenerAccionistas(string CDL_LISTA);
        Task<List<DOAccionista>> ObtenerPartesRelacionadas(string CDL_LISTA);
    }
}

﻿using Aplicacion.DTOs;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaces Operaciones spid accionistas.
    /// </summary>
    public interface IOperacionesSpidAccionistas
    {
        Task<Dictionary<string, DAccionistas>> ObtenerListaClientes();

        Task<string> ObtenerEstatusBatch(string idProceso);

        Task EliminarPorTipo(string tipo, string listaCdl);

        Task InsertarCliente(DAccionistas cliente);

        Task MarcarBatchProcesado(string idProceso);

        Task ActualizarFechasConstitucion(string listaCdlMinds);

        Task InsertarAccionista(DOAccionista accionista);

        Task ActualizarEstatusCliente(string cdl, string estatus);

        Task InsertarRelacionadas(DOAccionista parteRelacionadoValidado);

    }
}

﻿using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using Aplicacion.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Parametros operaciones.
    /// </summary>
    public interface IParametrosOperaciones
    {
        Task<string> metodoBDAsync();
        Task<Dictionary<string, string>> parametrosSistema();
        Task MarcarServicioEnEjecucion(string strNombreServicio);
        Task MarcarServicioEnFinalizacion(string strNombreServicio);
        Task EnviaAlerta(AlertaDTO alerta);

        Task<bool> BEsDiaHabil();

        Task<Dictionary<string, string>> parametrosAccionistas();
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Accionistas
    /// </summary>
    public class DAccionistas
    {
        public string COMPANY_NAME { get; set; }
        public string RECID { get; set; }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Estatus
    /// </summary>
    public class DEstatus
    {
        public string STATUS { get; set; }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Fecha constitucion.
    /// </summary>
    public class DFechaConstitucion
    {
        public string CDL { get; set; }
        public string FECHA_CONSTITUCION { get; set; }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Accionista
    /// </summary>
    public class DOAccionista
    {
        public string CUENTA { get; set; }
        public string CDL { get; set; }
        public string NOMBRE { get; set; }
        public string NOM_CLIENTE { get; set; }
        public string FECHA_CONSTITUCION { get; set; }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO Parametro accionista
    /// </summary>
    public class ParametroAccionista
    {
        public string cve_parametro { get; set; }
        public string cve_valor { get; set; }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion
{

    /// <summary>
    /// DTO Parametro modelo.
    /// </summary>
    public class ParametroModelo
    {
        public string id_parametro { get; set; }
        public string valor { get; set; }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO String fecha
    /// </summary>
    public class SFecha
    {
        public DateTime fecha { get; set; }
    }
}

using Aplicacion;
using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Consultas operaciones tomi.
    /// </summary>
    public class ConsultaCamposBDTomi : RepositorioMaestro, IParametrosOperaciones
    {
        private readonly Alertas alerta;

        /// <summary>
        /// Inicializa una nueva instancia de ConsultaCamposBDTomi.
        /// </summary>
        /// <param name="alertas">
        /// Componente encargado del registro de alertas.
        /// </param>
        public ConsultaCamposBDTomi(Alertas alertas) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) 
        { 
            alerta = alertas; 
        }

        /// <summary>
        /// Obtencion de fecha desde la base de datos.
        /// </summary>
        /// <returns>Date</returns>
        public async Task<string> metodoBDAsync()
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<SFecha>(ConstantesServicio.sp_Fecha_Sistema);
                return resultado?.Data?.FirstOrDefault()?.fecha.ToString();
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|metodoBDAsync|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Obtencion de los parametros del sistema.
        /// </summary>
        /// <returns>Dictionary<string, string></returns>
        public async Task<Dictionary<string, string>> parametrosSistema()
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<ParametroModelo>(ConstantesServicio.sp_Parametros_Sistema);

                if (result?.Data == null) return new Dictionary<string, string>();

                return result.Data
                    .Where(x => x != null && !string.IsNullOrWhiteSpace(x.id_parametro))
                    .GroupBy(x => x.id_parametro)
                    .ToDictionary(
                        x => x.Key,
                        x => x.LastOrDefault()?.valor ?? string.Empty
                    );
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|parametrosSistema|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.Message);
            }
        }

        // <summary>
        /// Actualiza el estado del servicio en la base de datos, lo marca como en uso.
        /// </summary>
        /// <returns></returns>
        public async Task MarcarServicioEnEjecucion(string strNombreServicio) => await ActualizaEJecucion(strNombreServicio);

        // <summary>
        /// Envia alertas al sistema.
        /// </summary>
        /// <returns></returns>
        public async Task EnviaAlerta(AlertaDTO alertaDTO) => await alerta.InsertarAlerta(alertaDTO);

        // <summary>
        /// Actualiza el estado del servicio en la base de datos, lo marca como en uso.
        /// </summary>
        /// <returns></returns>
        public async Task MarcarServicioEnFinalizacion(string strNombreServicio) => await ActualizaTerminacion(strNombreServicio);

        /// <summary>
        /// Obtiene los parametros de Accionistas.
        /// </summary>
        /// <returns>Dictionary<string, string></returns>
        public async Task<Dictionary<string, string>> parametrosAccionistas()
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<ParametroAccionista>(ConstantesServicio.sp_Parametros_SpidAccionistas);

                if (result?.Data == null) return new Dictionary<string, string>();

                return result.Data
                    .Where(x => x != null && !string.IsNullOrWhiteSpace(x.cve_parametro))
                    .GroupBy(x => x.cve_parametro)
                    .ToDictionary(
                        x => x.Key,
                        x => x.LastOrDefault()?.cve_valor ?? string.Empty
                    );
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|parametrosAccionistas|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Pregunta si es dia habil para ejecutar la operacion.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> BEsDiaHabil() => await EsDiaHabil();

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operaciones minds.
    /// </summary>
    public class OperacionesMINDS : RepositorioMaestro, IOperacionesMINDS

    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase OperacionesMINDS.
        /// </summary>
        public OperacionesMINDS() : base(BasedatosProveedor.SqlServer, NombreBasedatos.MINDS, null, null)
        {
        }

        /// <summary>
        /// Obtencion de accionistas.
        /// </summary>
        /// <param name="CDL_LISTA"></param>
        /// <returns></returns>
        public async Task<List<DOAccionista>> ObtenerAccionistas(string CDL_LISTA)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<DOAccionista>(
                     ConstantesServicio.sp_ObtenerAccionistas,
                     cmd => {
                         cmd.Parameters.Add(basedatosConexion.CrearParametro("@lista_cdl", CDL_LISTA));
                     }
                 );
                return resultado?.Data ?? new List<DOAccionista>();
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerAccionistas|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Obtencion de fecha constitucion.
        /// </summary>
        /// <param name="CDL_LISTA">
        /// Identificador de la lista CDL a consultar.
        /// </param>
        public async Task<List<DFechaConstitucion>> ObtenerFechasConstitucion(string CDL_LISTA)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<DFechaConstitucion>(
                   ConstantesServicio.sp_ObtenerFechasConstitucion,
                   cmd => {
                       cmd.Parameters.Add(basedatosConexion.CrearParametro("@lista_cdl", CDL_LISTA));
                   }
                );
                return resultado?.Data ?? new List<DFechaConstitucion>();
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerFechasConstitucion|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Obtencion de verdaderas fiestas.
        /// </summary>
        /// <param name="CDL_LISTA"></param>
        /// <returns></returns>
        public async Task<List<DOAccionista>> ObtenerPartesRelacionadas(string CDL_LISTA)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<DOAccionista>(
                    ConstantesServicio.sp_ObtenerPartesRelacionadas,
                    cmd => {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@lista_cdl", CDL_LISTA));
                    }
                );
                return resultado?.Data ?? new List<DOAccionista>();

            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerPartesRelacionadas|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operaciones repositorios.
    /// </summary>
    public class OperacionesRepositorio : RepositorioMaestro, IOperacionesSpidAccionistas
    {

        // Sistema Interfaces.
        private readonly ConfiguracionServicio _configuracion;

        // Alertas
        private readonly Alertas _alertas;

        private readonly IOperacionesMINDS _operacionesMINDS;

        /// <summary>
        /// Inicializa una nueva instancia de OperacionesRepositorio.
        /// </summary>
        /// <param name="alertas">
        /// Componente encargado del registro de alertas.
        /// </param>
        /// <param name="operacionesMIND">
        /// Operaciones relacionadas con MINDS.
        /// </param>
        /// <param name="configuracion">
        /// Configuración general del servicio.
        /// </param>
        public OperacionesRepositorio(Alertas alertas, IOperacionesMINDS operacionesMIND, ConfiguracionServicio configuracion) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW)
        {
            _configuracion = configuracion;
            _operacionesMINDS = operacionesMIND;
            _alertas = alertas;
        }

        /// <summary>
        /// Obtiene la informacion de la lista de clientes de Accionistas.
        /// </summary>
        /// <returns>List<DLAccionistas></returns>
        public async Task<Dictionary<string, DAccionistas>> ObtenerListaClientes()
        {

            try
            {
                if (_configuracion.BobtenerListaClientes) Config.EscribirLog("INFO|ObtenerListaClientes|Buscando lista de clientes.", NombreServicio.SPID_Accionistas.ToString());
                _configuracion.BobtenerListaClientes = false;

                var spEjecucion = await EjecutarStoredProcedureQuery<DAccionistas>(ConstantesServicio.sp_Obtener_Lista_Accionistas_T24_VW, esVistasT24: true);
                var spEjecucionData =  spEjecucion?.Data ?? new List<DAccionistas>(); //Obtiene la informacion de Data sino crea un array vacio.
                var resultado = ConvertirListaClientesADiccionario(spEjecucionData);
                
                Config.EscribirLog($"INFO|ObtenerListaAccionistas|Se obtuvieron {resultado.Count} clientes.", NombreServicio.SPID_Accionistas.ToString());
                return resultado;
             
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerListaClientes|Ocurre Excepcion:{ex}", NombreServicio.SPID_Accionistas.ToString());
                // Alerta
                
                await _alertas.InsertarAlerta(new AlertaDTO()
                {
                    TipoOPeracion = null,
                    Folio = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = "ERROR",
                    Descipcion = ex.Message,
                    PId = (int)_configuracion.Pid,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = new Exception(ex.ToString())
                });
                throw;
            }
        }

        // ------------------------------------------------------------------ |METODOS AUXILIARES| ------------------------------------------------------------------

        /// <summary>
        /// Convierte lista clientes a diccionario.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        internal Dictionary<string, DAccionistas> ConvertirListaClientesADiccionario(List<DAccionistas> data) 
        {
            try
            {
                var resultado = new Dictionary<string, DAccionistas>();
                foreach (var accionista in data ?? new List<DAccionistas>())
                {
                    resultado.Add(accionista.RECID, accionista);
                }
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ConvertirListaClientesADiccionario|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Ejecuta un sp para obtener el estatus mediante el id del proceso.
        /// </summary>
        /// <param name="idProceso"></param>
        /// <returns>string</returns>
        public async Task<string>ObtenerEstatusBatch(string idProceso) 
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<DEstatus>(
                    ConstantesServicio.sp_ObtenerEstatusBatch,
                    cmd => {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@id_proceso", idProceso));
                    }
                );
                return resultado?.Data?.FirstOrDefault()?.STATUS;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerEstatusBatch|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Elimina por tipo
        /// </summary>
        /// <param name="tipo"></param>
        /// <param name="listaCdl"></param>
        /// <returns></returns>
        public async Task EliminarPorTipo(string tipo, string listaCdl)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(
                    ConstantesServicio.sp_EliminarPorTipo,
                    cmd => {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo", tipo));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@lista_cdl", listaCdl));
                    }
                );
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|EliminarPorTipo|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }

        }

        /// <summary>
        /// Inserta cliente.
        /// </summary>
        /// <param name="cliente"></param>
        /// <returns></returns>
        public async Task InsertarCliente(DAccionistas cliente)
        {
            try
            {
               await EjecutarStoredProceduredNonQuery(
                    ConstantesServicio.sp_InsertarCliente,
                    cmd => {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cdl", cliente.RECID));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombre",cliente.COMPANY_NAME));
                    }
               );
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|InsertarCliente|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Marca batch procesado.
        /// </summary>
        /// <param name="idProceso"></param>
        /// <returns></returns>
        public async Task MarcarBatchProcesado(string idProceso)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(
                    ConstantesServicio.sp_MarcarBatchProcesado,
                    cmd => {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@id_proceso", idProceso));
                    }
                );
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MarcarBatchProcesado|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Actualiza fecha constitucion.
        /// </summary>
        /// <param name="listaCdlMinds"></param>
        /// <returns></returns>
        public async Task ActualizarFechasConstitucion(string listaCdlMinds)
        {
            try
            {
                var fechas = await _operacionesMINDS.ObtenerFechasConstitucion(listaCdlMinds);

                foreach(var fecha in fechas) 
                {
                    Config.EscribirLog("INFO|Valida_Entidades|Para el CDL:"+ fecha.CDL +", se actualiza la fecha de constitución:'" +fecha.FECHA_CONSTITUCION +"'.");
                    await EjecutarStoredProceduredNonQuery(
                        ConstantesServicio.sp_ActualizarFechasConstitucion,
                        cmd => {
                            cmd.Parameters.Add(basedatosConexion.CrearParametro("@cdl", fecha.CDL));
                             cmd.Parameters.Add(basedatosConexion.CrearParametro("@fecha_constitucion", fecha.FECHA_CONSTITUCION));
                        }
                    );
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ActualizarFechasConstitucion|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Inserta accionistas.
        /// </summary>
        /// <param name="accionista"></param>
        /// <returns></returns>
        public async Task InsertarAccionista(DOAccionista accionista)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(
                    ConstantesServicio.sp_InsertarAccionista,
                    cmd => {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cdl", accionista.CDL));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cliente", accionista.NOM_CLIENTE));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombre", accionista.NOMBRE));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cuenta", accionista.CUENTA));
                        
                    });
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|InsertarAccionista|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
            
        }

        /// <summary>
        /// Actualiza estatus cliente.
        /// </summary>
        /// <param name="cdl"></param>
        /// <param name="estatus"></param>
        /// <returns></returns>
        public async Task ActualizarEstatusCliente(string cdl, string estatus)
        {
            try
            {

                await EjecutarStoredProceduredNonQuery(
                    ConstantesServicio.sp_ActualizarEstatusCliente,
                    cmd => {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cdl", cdl));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@estatus", estatus));
                    }
                );
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ActualizarEstatusCliente|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>
        /// Inserta Fiestas verdaderas.
        /// </summary>
        /// <param name="parteRelacionadoValidado"></param>
        /// <returns></returns>
        public async Task InsertarRelacionadas(DOAccionista parteRelacionadoValidado)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(
                    ConstantesServicio.sp_InsertarRelacionadas,
                    cmd => {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cdl", parteRelacionadoValidado.CDL));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cliente", parteRelacionadoValidado.NOM_CLIENTE));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombre", parteRelacionadoValidado.NOMBRE));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cuenta", parteRelacionadoValidado.CUENTA));
                    }
                );
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|InsertarRelacionadas|Ocurre Excepcion:" + ex.ToString(), NombreServicio.SPID_Accionistas.ToString());
                throw new Exception(ex.ToString());
            }
        }

    }
}

using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Servicios;
using Infraestructura.Repositorio;
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
using System.Linq;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace SpidAccionistas
{

    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private ConfiguracionServicio _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.SPID_Accionistas.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServiceHost() {           
        }

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
                ServicioConfiguracionSettings servicioMainConfiguracion = MUFG_T24.Core.Configuracion.LecturaConfguracionGeneral.ObtenerServicio(NombreServicio.SPID_Accionistas.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.RutaArchivoConfiguracion = $@"{EnumRutasGenerales.Path_Log_CMS}{NombreServicio.SPID_Accionistas.ToString()}\";
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


            

            var parametrizacion = new Parametrizacion();

            _containerDI.Register<ConfiguracionServicio>(() => (ConfiguracionServicio)_configuracionBase.Clone(), Lifestyle.Singleton);

            #region SPID_ACCIONISTAS
                _containerDI.Register<IParametrosOperaciones, ConsultaCamposBDTomi>(Lifestyle.Scoped);

                _containerDI.Register<IOperacionesMINDS, OperacionesMINDS>(Lifestyle.Singleton);

                _containerDI.Register<IOperacionesSpidAccionistas, OperacionesRepositorio>(Lifestyle.Singleton);
            #endregion SPID_ACCIONISTAS

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
                    await operacionServicio.EjecutaProcedimientosAsync().ConfigureAwait(false);
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

*************
    Aplicacion.csproj
************
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
    <AssemblyName>AplicacionSPID_ACCIONISTAS</AssemblyName>
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
  <PropertyGroup Condition=" '$(Configuration)' == 'Release' ">
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <GenerateDocumentation>bin\Release\SpidAccionistas.xml</GenerateDocumentation>
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
    <Compile Include="DTOs\DFechaConstitucion.cs" />
    <Compile Include="DTOs\DOAccionista.cs" />
    <Compile Include="DTOs\DAccionistas.cs" />
    <Compile Include="DTOs\DEstatus.cs" />
    <Compile Include="DTOs\ParametroAccionista.cs" />
    <Compile Include="DTOs\ParametroModelo.cs" />
    <Compile Include="DTOs\SFecha.cs" />
    <Compile Include="Enums\ConstantesServicio.cs" />
    <Compile Include="Enums\ParametrosSistemaENUM.cs" />
    <Compile Include="Interfaces\IOperacionesMINDS.cs" />
    <Compile Include="Interfaces\IOperacionesSpidAccionistas.cs" />
    <Compile Include="Interfaces\IOperacionServicio.cs" />
    <Compile Include="Interfaces\IParametrosOperaciones.cs" />
    <Compile Include="Modelos\ConfiguracionServicio.cs" />
    <Compile Include="OperacionServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <ItemGroup />
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

**********
Infraestructura.csproj
**********
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
    <AssemblyName>InfraestructuraSPID_ACCIONISTAS</AssemblyName>
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
  <PropertyGroup Condition=" '$(Configuration)' == 'Release' ">
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <GenerateDocumentation>bin\Release\SpidAccionistas.xml</GenerateDocumentation>
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
    <Compile Include="Repositorio\OperacionesMINDS.cs" />
    <Compile Include="Repositorio\OperacionesRepositorio.cs" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\Aplicacion.csproj">
      <Project>{93a21735-f0fa-4178-9458-51bf9ebee238}</Project>
      <Name>Aplicacion</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

*************
SPID_Accionistas.csproj
*************
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{9544BA40-7D87-4EE1-8A6B-CB0831C8C270}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>SpidAccionistas</RootNamespace>
    <AssemblyName>SPID_Accionistas</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
    <Deterministic>true</Deterministic>
    <PublishUrl>publish\</PublishUrl>
    <Install>true</Install>
    <InstallFrom>Disk</InstallFrom>
    <UpdateEnabled>false</UpdateEnabled>
    <UpdateMode>Foreground</UpdateMode>
    <UpdateInterval>7</UpdateInterval>
    <UpdateIntervalUnits>Days</UpdateIntervalUnits>
    <UpdatePeriodically>false</UpdatePeriodically>
    <UpdateRequired>false</UpdateRequired>
    <MapFileExtensions>true</MapFileExtensions>
    <ApplicationRevision>0</ApplicationRevision>
    <ApplicationVersion>1.0.0.%2a</ApplicationVersion>
    <IsWebBootstrapper>false</IsWebBootstrapper>
    <UseApplicationTrust>false</UseApplicationTrust>
    <BootstrapperEnabled>true</BootstrapperEnabled>
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
  <PropertyGroup Condition=" '$(Configuration)' == 'Release' ">
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <GenerateDocumentation>bin\Release\SpidAccionistas.xml</GenerateDocumentation>
  </PropertyGroup>
  <PropertyGroup>
    <StartupObject />
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector">
      <HintPath>D:\ServiceCMS\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.Numerics" />
    <Reference Include="System.Web.Extensions" />
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
  </ItemGroup>
  <ItemGroup>
    <BootstrapperPackage Include=".NETFramework,Version=v4.8">
      <Visible>False</Visible>
      <ProductName>Microsoft .NET Framework 4.8 %28x86 and x64%29</ProductName>
      <Install>true</Install>
    </BootstrapperPackage>
    <BootstrapperPackage Include="Microsoft.Net.Framework.3.5.SP1">
      <Visible>False</Visible>
      <ProductName>.NET Framework 3.5 SP1</ProductName>
      <Install>false</Install>
    </BootstrapperPackage>
  </ItemGroup>
  <ItemGroup>
    <WCFMetadata Include="Connected Services\" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\Aplicacion.csproj">
      <Project>{93a21735-f0fa-4178-9458-51bf9ebee238}</Project>
      <Name>Aplicacion</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura\Infraestructura.csproj">
      <Project>{4e3c93b8-f795-461b-a31d-aad2046e9bb9}</Project>
      <Name>Infraestructura</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>
****************
README.md
****************
                # SPID_ACCIONISTAS



## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin http://gitlab.mufg.mx/mufg-it/mufg-sda/middleware-nttdata/spei_spid/SPID_ACCIONISTAS.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](http://gitlab.mufg.mx/mufg-it/mufg-sda/middleware-nttdata/spei_spid/SPID_ACCIONISTAS/-/settings/integrations)

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
