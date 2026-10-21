using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Servicios;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion
{
    /// <summary>
    /// Clase que contiene el flujo principal del procesamiento (equivalente a Definiciones).
    /// </summary>
    public class OperacionServicio
    {
        private readonly Configuracion _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IAlertas _alertas;      
        private readonly IParametrizacion _parametrizacion;
        private readonly IRecuperacionDatos<OperacionDTO> _recuperacionDatos;
        private readonly IDetallesCiseSpidDao _detallesCiseSpidDao;
        private readonly ENT_SPID _entSpid;
        private readonly ENT_SPID_PART _entSpidPart;
        private readonly EstadoHorarioOperacion _estadoHorarioOperacion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public OperacionServicio(Configuracion configuracion, IControlServicio controlServicio, IAlertas alertas, IParametrizacion parametrizacion, IRecuperacionDatos<OperacionDTO> recuperacionDatos, IDetallesCiseSpidDao detallesCiseSpidDao, ENT_SPID entSpid, ENT_SPID_PART entSpidPart, EstadoHorarioOperacion estadoHorarioOperacion)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _alertas = alertas;
            _parametrizacion = parametrizacion;
            _recuperacionDatos = recuperacionDatos;
            _detallesCiseSpidDao = detallesCiseSpidDao;
            _entSpid = entSpid;
            _entSpidPart = entSpidPart;
            _estadoHorarioOperacion = estadoHorarioOperacion;
        }

        /// <summary>
        /// Control del flujo principal del procesamiento.
        /// </summary>
        public async Task<bool> EjecutarProcedimientosAsync(CancellationToken ct)
        {
            bool ejecucionActualizada = false;
            try
            {
                ct.ThrowIfCancellationRequested();

                //Marca la ejecución del servicio...
                await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct);
                ejecucionActualizada = true;

                //Obtiene los parámetros del sistema...
                _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync(ct);
                _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil(ct);
                _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesServicio.ParametrosSistema, ct);

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
                    }, ct);
                    return false;
                }

                //Valida el horario de operación...
                TimeSpan timeHoraSistema = _parametrizacion.ObtenerHoraSistema();
                string horaSistema = timeHoraSistema.ToString(@"hh\:mm");
                TimeSpan horaInicioOperacion = ConversionesComunes.ParseTimeSpanRequerido(_configuracion.ParametrosSistema[ConstantesGenerales.HoraOPInicio], ConstantesGenerales.HoraOPInicio);
                TimeSpan horaFinOperacion = ConversionesComunes.ParseTimeSpanRequerido(_configuracion.ParametrosSistema[ConstantesServicio.HoraSpidCepFin], ConstantesServicio.HoraSpidCepFin);
                if (timeHoraSistema >= horaInicioOperacion
                    && timeHoraSistema <= horaFinOperacion)
                {
                    if (!_estadoHorarioOperacion.EnHorarioOperacion)
                    {
                        _estadoHorarioOperacion.EnHorarioOperacion = true;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Inicio horario de operaciones, HoraSistema='{horaSistema}'.");
                    }
                }
                else
                {
                    if (_estadoHorarioOperacion.EnHorarioOperacion)
                    {
                        _estadoHorarioOperacion.EnHorarioOperacion = false;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Termina horario de operaciones, HoraSistema='{horaSistema}'");
                    }
                }

                if (!_estadoHorarioOperacion.EnHorarioOperacion)
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
                    }, ct);
                    return false;
                }

                //Recupera los datos de procesamiento...
                Task<IEnumerable<OperacionDTO>> operacionesSpidTask = _recuperacionDatos.RecuperarDatosAsync(ct);
                Task<List<OperacionesParticipantesDto>> operacionesParticipantesTask = _detallesCiseSpidDao.RecuperarRecibidasParticipantesAsync(ct);
                await Task.WhenAll(operacionesSpidTask, operacionesParticipantesTask);

                List<Operacion> operaciones = new List<Operacion>();
                List<OperacionDTO> operacionesSpid = operacionesSpidTask.Result.ToList();
                foreach (OperacionDTO operacionSpid in operacionesSpid)
                {
                    operaciones.Add(new Operacion()
                    {
                        folio = operacionSpid.folio,
                        tipo_operacion = operacionSpid.tipo_operacion
                    });
                }

                // Obtiene los paquetes SPID recibidos Participante (tp_clave 2 y 3)...
                List<OperacionesParticipantesDto> operacionesParticipantes = operacionesParticipantesTask.Result;
                foreach (OperacionesParticipantesDto operacionParticipante in operacionesParticipantes)
                {
                    operaciones.Add(new Operacion()
                    {
                        folio = (long)operacionParticipante.op_clave,
                        tipo_operacion = ConstantesServicio.EntSpidPart
                    });
                }

                //Si hay más de un registro...
                if (operaciones.Count > 0)
                {
                    Config.EscribirLog($"INFO|procesaOperacion|{operaciones.Count} operaciones para generar CDA");
                }

                //Realiza un procesamiento diferente de acuerdo al tipo de operación...
                foreach (Operacion operacion in operaciones)
                {
                    try
                    {
                        ct.ThrowIfCancellationRequested();
                        bool resultado = false;

                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos|***** TipoOperacion:{operacion.tipo_operacion}, folio:{operacion.folio} *****");
                        switch (operacion.tipo_operacion)
                        {
                            case ConstantesServicio.EntSpid:
                                resultado = await _entSpid.ProcesarOperacionCISEAsync(operacion.folio.ToString(), ct);
                                break;
                            case ConstantesServicio.EntSpidPart:
                                resultado = await _entSpidPart.ProcesarOperacionCISEAsync(operacion.folio.ToString(), ct);
                                break;
                            default:
                                throw new Exception($"Tipo de operación desconocida:'{operacion.tipo_operacion}'");
                        }

                        if (resultado)
                            Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Se termina procesamiento de folio: {operacion.folio}");
                        else
                            Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Error durante el procesamiento del folio: {operacion.folio}");
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Error en la ejecución del folio {operacion.folio}, excepción: {ex}");
                        await _controlServicio.ActualizaErrorProceso(new ProcesoDTO()
                        {
                            Folio = operacion.folio.ToString(),
                            TipoOperacion = operacion.tipo_operacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            Excepcion = ex,
                            PId = _configuracion.Pid.Value
                        }, ct);
                        await _alertas.InsertarAlerta(new AlertaDTO()
                        {
                            Folio = operacion.folio.ToString(),
                            TipoOPeracion = operacion.tipo_operacion,
                            Servicio = _configuracion.NombreServicio,
                            ClaveAviso = ConstantesClaveAviso.Critico,
                            Descipcion = ex.Message,
                            PId = _configuracion.Pid.Value,
                            NombreProceso = _configuracion.NombreProceso,
                            Exception = ex
                        }, ct);
                    }
                }

                return true;
            }
            catch (OperationCanceledException)
            {
                Config.EscribirLog("INFO|Ejecuta_Procedimientos|Ejecución cancelada.");
            }
            catch (Exception ex) 
            {
                Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Se presenta exception: {ex}");
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
            finally
            {
                if (ejecucionActualizada)
                {
                    try
                    {
                        await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);
                    }
                    catch (Exception ex)
                    {
                        Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|No fue posible actualizar terminación del servicio: {ex}");
                    }
                }
            }
            return false;
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Clase que contiene el flujo principal del procesamiento ENT_SPID_PART.
    /// </summary>
    public class ENT_SPID_PART
    {
        private readonly Configuracion _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IDetallesCiseDao _detallesCiseDao;
        private readonly IDetallesCiseSpidDao _detallesCiseSpidDao;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ENT_SPID_PART(Configuracion configuracion, IControlServicio controlServicio, IDetallesCiseDao detallesCiseDao, IDetallesCiseSpidDao detallesCiseSpidDao)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _detallesCiseDao = detallesCiseDao;
            _detallesCiseSpidDao = detallesCiseSpidDao;
        }

        /// <summary>
        /// Flujo principal de ENT_SPID_PART: orquestador.
        /// </summary>
        public async Task<bool> ProcesarOperacionCISEAsync(string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            const string tipoOperacion = ConstantesServicio.EntSpidPart;

            //Inicia el monitoreo del proceso...
            await _controlServicio.IniciaProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.ServicioPlan,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value,
                NombreProceso = _configuracion.NombreProceso
            }, ct);

            bool ordenPagoInsertada = await InsertarOrdenPagoAsync(folio, ct);
            if (!ordenPagoInsertada)
                return false;

            //Termina el proceso...
            await _controlServicio.TerminaProceso(new ProcesoDTO()
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.ServicioPlan,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            }, ct);
            await _controlServicio.TerminaProceso(new ProcesoDTO()
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.ServicioPlan,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProcesoFin,
                PId = _configuracion.Pid.Value
            }, ct);
            await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);
            await _detallesCiseDao.MarcarOperacionProcesadaAsync(folio, tipoOperacion, ct);

            Config.EscribirLog($"INFO|ProcesaOperacion_CISE|Se termina el procesamiento procesaOperacion_CISE - ent_spid_part -  de la operación folio={folio}");

            return true;
        }

        /// <summary>
        /// Inserta la orden de pago.
        /// </summary>
        internal async Task<bool> InsertarOrdenPagoAsync(string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            Config.EscribirLog($"INFO|procesaOperacion|Preparando procesaOperacion_CISE - entspid_part  - envio a CEP-SPID del folio: {folio}.");

            //Almacena la orden de pago...
            OrdenPagoDto ordenPago = await _detallesCiseSpidDao.InsertarOrdenPagoAsync(folio, ct);

            Config.EscribirLog($"INFO|ProcesaOperacion_CISE|Se ejecutó procedimiento sp_InterfaceMW_SPID_CEP . '{folio}");

            //Si no se encontro información del folio...
            if (ordenPago is null)
                throw new Exception($"No encontró el CDA teoricamente recienprocesaOperacion_CISE  insertado folio={folio}");

            //Si el resultado de la inserción es diferente de 0, se considera un error...
            if (ordenPago.Resultado != 0)
            {
                Config.EscribirLog($"ERROR|ProcesaOperacion_CISE|Error EN sp_InterfaceMW_SPID_CEPo.  FOLIO ={folio}");
                return false;
            }

            return true;
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Clase que contiene el flujo principal del procesamiento ENT_SID.
    /// </summary>
    public class ENT_SPID
    {
        private readonly Configuracion _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IDetallesCiseDao _detallesCiseDao;
        private readonly IDetallesCiseSpidDao _detallesCiseSpidDao;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ENT_SPID(Configuracion configuracion, IControlServicio controlServicio, IDetallesCiseDao detallesCiseDao, IDetallesCiseSpidDao detallesCiseSpidDao)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _detallesCiseDao = detallesCiseDao;
            _detallesCiseSpidDao = detallesCiseSpidDao;
        }

        /// <summary>
        /// Flujo principal de ENT_SPID: orquestador.
        /// </summary>
        public async Task<bool> ProcesarOperacionCISEAsync(string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            const string tipoOperacion = ConstantesServicio.EntSpid;

            //Inicia el monitoreo del proceso...
            await _controlServicio.IniciaProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.ServicioPlan,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value,
                NombreProceso = _configuracion.NombreProceso
            }, ct);

            //Obtiene los datos T24 del folio...
            DatosFolioDto datosFolio = await ObtenerDatosFolioAsync(folio, ct);

            //Inserta la orden de pago...
            bool ordenPagoInsertada = await InsertarOrdenPagoAsync(folio, datosFolio, ct);
            if (!ordenPagoInsertada)
                return false;

            //Termina el proceso...
            await _controlServicio.TerminaProceso(new ProcesoDTO()
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.ServicioPlan,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            }, ct);
            await _controlServicio.TerminaProceso(new ProcesoDTO()
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.ServicioPlan,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProcesoFin,
                PId = _configuracion.Pid.Value
            }, ct);
            await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);
            await _detallesCiseDao.MarcarOperacionProcesadaAsync(folio, tipoOperacion, ct);

            Config.EscribirLog($"INFO|ProcesaOperacion_CISE|Se termina el procesamiento procesaOperacion_CISE - ent_spid -  de la operación folio={folio}");

            return true;
        }

        /// <summary>
        /// Obtiene los datos T24 del folio.
        /// </summary>
        internal async Task<DatosFolioDto> ObtenerDatosFolioAsync(string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            Config.EscribirLog($"INFO|procesaOperacion|Buscando información en SPEI del folio: {folio}.");

            DatosFolioDto datosFolio = await _detallesCiseDao.ObtieneDatosFolioAsync(folio, ct);

            bool tieneFechaHoraT24 = datosFolio != null
                && !string.IsNullOrWhiteSpace(datosFolio.fechaT24)
                && !string.IsNullOrWhiteSpace(datosFolio.horaT24);

            //Si no se recupera la fecha y hora de autorización T24...
            if (tieneFechaHoraT24)
                Config.EscribirLog($"INFO|ProcesaOperacion_CISE|DATOS T24 {datosFolio.fechaT24} --> {datosFolio.horaT24}");
            else
                Config.EscribirLog($"ERROR|ProcesaOperacion_CISE|No se recupera Fecha y Hora Autoriza T24 en SPEI del folio: {folio}.");
            if (!tieneFechaHoraT24)
                throw new Exception("No se encontro el detalle de la operación");

            return datosFolio;
        }

        /// <summary>
        /// Inserta la orden de pago en CEP-SPID.
        /// </summary>
        internal async Task<bool> InsertarOrdenPagoAsync(string folio, DatosFolioDto datosFolio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            Config.EscribirLog($"INFO|procesaOperacion|Preparando procesaOperacion_CISE - ent_spid - envio a CEP-SPID del folio: {folio}.");

            //Inserta la orden de pago...
            OrdenPagoDto ordenPago = await _detallesCiseSpidDao.InsertarOrdenPagoAsync(folio, ct, datosFolio.fechaT24, datosFolio.horaT24);

            Config.EscribirLog($"INFO|ProcesaOperacion_CISE|Se ejecutó procedimiento sp_InterfaceMW_SPID_CEP . '{folio}");

            //Si no se recupera el CDA teoricamente recien insertado...
            if (ordenPago is null)
                throw new Exception($"No encontró el CDA teoricamente recienprocesaOperacion_CISE  insertado folio={folio}");

            ///Si el resultado es diferente de 0, significa que hubo un error en la inserción de la orden de pago...
            if (ordenPago.Resultado != 0)
            {
                Config.EscribirLog($"ERROR|ProcesaOperacion_CISE|Error EN sp_InterfaceMW_SPID_CEPo.  FOLIO ={folio}");
                return false;
            }

            return true;
        }

    }
}

﻿namespace Aplicacion.Enums
{
    /// <summary>
    /// Clase que contiene las constantes a modo de enumerador.
    /// Por compatibilidad: NET Framework 4.8 no permite enumeradores strings.
    /// </summary>
    public static class ConstantesServicio
    {
        //Constantes de parametrización...
        public const string ParametrosSistema = "'HORA_OP_INICIO','HORA_SPID_CEP_FIN','MAIL_SOCKET_CEP_TO'";
        public const string HoraSpidCepFin = "HORA_SPID_CEP_FIN";

        //Constantes de lógica...
        public const string EntSpid = "ENT_SPID";
        public const string EntSpidPart = "ENT_SPID_PART";
        public const string ND = "ND";

        //Constantes de SQL...
        public const string uspObtenerRegistrosCda = "usp_SpidGenCda_ObtenerRegistrosCda";
        public const string uspObtenerParticipantes = "sp_InterfaceTOMI_SPID_Recibidas_Participantes";
        public const string spInterfaceMwSpidCep = "sp_InterfaceMW_SPID_CEP";
        public const string uspObtenerDatosFolio = "usp_SpidGenCda_ObtenerDatosFolio";
        public const string uspMarcarOperacionProcesada = "usp_SpidGenCda_MarcarOperacionProcesada";

    }
}

﻿using Aplicacion.DTOs;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaz de clase DAO.
    /// </summary>
    public interface IDetallesCiseDao
    {
        Task<DatosFolioDto> ObtieneDatosFolioAsync(string folio, CancellationToken ct);
        Task<bool> MarcarOperacionProcesadaAsync(string folio, string tipoOperacion, CancellationToken ct);
    }
}

using Aplicacion.DTOs;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaz de clase DAO.
    /// </summary>
    public interface IDetallesCiseSpidDao
    {
        Task<List<OperacionesParticipantesDto>> RecuperarRecibidasParticipantesAsync(CancellationToken ct);
        Task<OrdenPagoDto> InsertarOrdenPagoAsync(string folio, CancellationToken ct, string fechaT24 = null, string horaT24 = null);
    }
}

﻿using Aplicacion.DTOs;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaz de clase DAO.
    /// </summary>
    public interface IRecuperacionDatos<Tipo>
    {
        Task<IEnumerable<OperacionDTO>> RecuperarDatosAsync(CancellationToken ct);
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorios
{
    /// <summary>
    /// Clase de recuperación de datos detalle de InterfacesT24.
    /// </summary>
    public class DetallesCiseDao : RepositorioMaestro, IDetallesCiseDao
    {
        Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesCiseDao(Configuracion configuracion)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Obtiene los datos del folio de la operación.
        /// </summary>
        public async Task<DatosFolioDto> ObtieneDatosFolioAsync(string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<DatosFolioDto>(ConstantesServicio.uspObtenerDatosFolio, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", _configuracion.FechaSistema));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
            }, ct);
            return resultado.Data.FirstOrDefault();
        }

        /// <summary>
        /// Marca la operacion como PROCESADA en el plan de trabajo maestro.
        /// </summary>
        public async Task<bool> MarcarOperacionProcesadaAsync(string folio, string tipoOperacion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspMarcarOperacionProcesada, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            }, ct, validarAfectacion: false);
            return resultado.Afecto == true;
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace Infraestructura.Servicios
{
    /// <summary>
    /// Clase de recuperación de datos detalle de SPID.
    /// </summary>
    public class DetallesCiseSpidDao : RepositorioMaestro, IDetallesCiseSpidDao
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesCiseSpidDao(Configuracion configuracion)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPID, null, null)
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Obtiene los paquetes SPID recibidos Participante (tp_clave 2 y 3).
        /// </summary>
        public async Task<List<OperacionesParticipantesDto>> RecuperarRecibidasParticipantesAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<OperacionesParticipantesDto>(ConstantesServicio.uspObtenerParticipantes, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", Convert.ToInt32(_configuracion.FechaSistema)));
            }, ct, commandTimeout: _configuracion.MaxTimeout);
            return resultado.Data;
        }

        /// <summary>
        /// Inserta la orden de pago ejecutando el procedimiento legacy de SPID.
        /// </summary>
        public async Task<OrdenPagoDto> InsertarOrdenPagoAsync(string folio, CancellationToken ct, string fechaT24 = null, string horaT24 = null)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<OrdenPagoDto>(ConstantesServicio.spInterfaceMwSpidCep, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", Convert.ToInt32(folio)));

                if (!string.IsNullOrWhiteSpace(fechaT24))
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaT24", Convert.ToInt32(fechaT24)));

                if (!string.IsNullOrWhiteSpace(horaT24))
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@HoraT24", horaT24));
            }, lector => new OrdenPagoDto()
            {
                Resultado = lector.FieldCount > 0 && lector.GetValue(0) != DBNull.Value ? Convert.ToInt32(lector.GetValue(0)) : 0,
                ErrorMessage = lector.FieldCount > 1 && lector.GetValue(1) != DBNull.Value ? Convert.ToString(lector.GetValue(1)) : null,
                ErrorNumber = lector.FieldCount > 2 && lector.GetValue(2) != DBNull.Value ? Convert.ToInt32(lector.GetValue(2)) : (int?)null
            }, ct, commandTimeout: _configuracion.MaxTimeout);
            return resultado.Data.FirstOrDefault();
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorios
{
    /// <summary>
    /// Clase de recuperación de datos principales.
    /// </summary>
    public class SondeoDao : RepositorioMaestro, IRecuperacionDatos<OperacionDTO>
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public SondeoDao(Configuracion configuracion)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) 
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Obtiene los registros con los que realiza su operación.
        /// </summary>
        public async Task<IEnumerable<OperacionDTO>> RecuperarDatosAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<OperacionDTO>(ConstantesServicio.uspObtenerRegistrosCda, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@ListaTiposOperacion", _configuracion.ListaTipos));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@AliasProceso", _configuracion.AliasProceso));
            }, ct);
            return resultado.Data;
        }

    }
}

using Aplicacion;
using Aplicacion.DTOs;
using Aplicacion.Interfaces;
using Aplicacion.Servicios;
using Infraestructura.Repositorios;
using Infraestructura.Servicios;
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
using System.Diagnostics;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace SPID_CEP_GEN_CDA
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private const double DefaultTimerIntervalMs = 10000;

        private Container _containerDI;
        private Configuracion _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.SPEI_CEP_ENV_CDA.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? DefaultTimerIntervalMs;

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
                ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.SPEI_CEP_GEN_CDA.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.PathLog = servicioMainConfiguracion.Log;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;
                _configuracionBase.MaxTimeout = servicioMainConfiguracion.MaxTimeout;

                _configuracionBase.AliasProceso = servicioMainConfiguracion.AliasProceso;
                _configuracionBase.AliasProcesoFin = servicioMainConfiguracion.AliasProcesoFin;
                _configuracionBase.ServicioPlan = servicioMainConfiguracion.ServicioPlan;
                _configuracionBase.MonedaDefault = servicioMainConfiguracion.MonedaDefault;
                _configuracionBase.ListaTipos = servicioMainConfiguracion.ListaTipos;

                Logging.Init(_configuracionBase.PathLog, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
            }

            //Registra la configuracion...
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);

            //Registra los componentes...
            _containerDI.Register<IRecuperacionDatos<OperacionDTO>, SondeoDao>();
            _containerDI.Register<IDetallesCiseDao, DetallesCiseDao>();
            _containerDI.Register<IDetallesCiseSpidDao, DetallesCiseSpidDao>();
            _containerDI.Register<OperacionServicio>();
            _containerDI.Register<ENT_SPID>();
            _containerDI.Register<ENT_SPID_PART>();

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
                _configuracionBase = await _parametrizacion.ObtenerProcesoActualAsync(_configuracionBase, ct).ConfigureAwait(false);

                //Marca el servicio en uso...
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
                await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio, ct).ConfigureAwait(false);
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
                    var operacionServicio = _containerDI.GetInstance<OperacionServicio>();
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

﻿using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;
using System.ServiceProcess;

namespace SPID_CEP_GEN_CDA
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
                NombreServicio.SPEI_CEP_ENV_CDA.ToString(),
                () => new ServiceHost(),
                args
#if DEBUG
                , (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs)
#endif
                );
        }

    }
}

Aplicacion.csproj
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net48</TargetFramework>
	<PlatformTarget>x86</PlatformTarget>
    <AssemblyName>AplicacionSPIDCEPGEN</AssemblyName>
  </PropertyGroup>

  <ItemGroup>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
  </ItemGroup>

</Project>

Infraestructura.csproj
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net48</TargetFramework>
	<PlatformTarget>x86</PlatformTarget>
    <AssemblyName>InfraestructuraSPIDCEPGEN</AssemblyName>
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

SPID_CEP_GEN_CDA.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>SPID_CEP_GEN_CDA</RootNamespace>
    <AssemblyName>SPID_CEP_GEN_CDA</AssemblyName>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <HintPath>..\packages\SimpleInjector.5.5.0\lib\net461\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
    <Reference Include="System.Data" />
    <Reference Include="System.Numerics" />
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
      <Project>{54cc424c-8227-4e2a-a81f-9d6e0daa9726}</Project>
      <Name>Aplicacion</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura\Infraestructura.csproj">
      <Project>{29917ce7-77fb-4cd4-b196-5a6a3ba132e1}</Project>
      <Name>Infraestructura</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

README.md
# SPID_CEP_GEN_CDA



## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin http://gitlab.mufg.mx/mufg-it/mufg-sda/middleware-nttdata/spei_spid/SPID_CEP_GEN_CDA.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](http://gitlab.mufg.mx/mufg-it/mufg-sda/middleware-nttdata/spei_spid/SPID_CEP_GEN_CDA/-/settings/integrations)

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
