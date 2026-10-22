using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Clase que contiene el flujo principal del procesamiento ENT_SPEI.
    /// </summary>
    public class ENT_SPEI : ProcesamientoCISE
    {
        private readonly Configuracion _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IDetallesCise _detallesCise;
        private readonly IDetallesCiseSpei _detallesCiseSpei;
        private readonly IDetallesCiseT24 _detallesCiseT24;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ENT_SPEI(Configuracion configuracion, IControlServicio controlServicio, IDetallesCise detallesCise, IDetallesCiseSpei detallesCiseSpei, IDetallesCiseT24 detallesCiseT24)
            : base(detallesCise)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _detallesCise = detallesCise;
            _detallesCiseSpei = detallesCiseSpei;
            _detallesCiseT24 = detallesCiseT24;
        }

        /// <summary>
        /// Flujo principal de ENT_SPEI: orquestador.
        /// </summary>
        public async Task<bool> ProcesarOperacionCISEAsync(string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            const string tipoOperacion = ConstantesServicio.EntSpei;

            //Inicia el monitoreo del proceso...
            await _controlServicio.IniciaProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.ServicioPlan,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value,
                NombreProceso = _configuracion.NombreProceso
            }, ct).ConfigureAwait(false);

            Config.EscribirLog($"INFO|procesaOperacion|Buscando información en SPEI del folio: {folio}.");

            //Obtiene los CDA de los complementos de la operación...
            List<CDA> operacionesCDA = await this.ObtenerCdasAsync(folio, ct).ConfigureAwait(false);

            //Extrae el RFC...
            foreach (CDA cda in operacionesCDA)
            {
                ct.ThrowIfCancellationRequested();
                string rfc = await this.ObtenerRfcAsync(cda, folio, ct).ConfigureAwait(false);
                cda.RFCCURPBeneficiario = rfc is null ? ConstantesServicio.ND : rfc;
            }

            //Inserta la orden de pago de cada complemento y termina su proceso...
            foreach (CDA cda in operacionesCDA)
            {
                ct.ThrowIfCancellationRequested();

                bool ordenPagoInsertada = await this.InsertarOrdenPagoAsync(cda, folio, ct).ConfigureAwait(false);
                if (!ordenPagoInsertada)
                {
                    //El SP no regresó filas para este CDA; se replica el comportamiento legacy: continúa silenciosamente con el siguiente CDA...
                    continue;
                }

                await _controlServicio.TerminaProceso(new ProcesoDTO()
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.ServicioPlan,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                }, ct).ConfigureAwait(false);
                await _controlServicio.TerminaProceso(new ProcesoDTO()
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.ServicioPlan,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProcesoFin,
                    PId = _configuracion.Pid.Value
                }, ct).ConfigureAwait(false);
                await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct).ConfigureAwait(false);
                await _detallesCise.MarcarOperacionProcesadaAsync(folio, tipoOperacion, ct).ConfigureAwait(false);
                Config.EscribirLog($"INFO|ProcesaOperacion_CISE|Se termina el procesamiento de la operación op_clave={folio}");
            }

            return true;
        }

        /// <summary>
        /// Obtiene los CDA y termina en caso de no haber ninguno.
        /// </summary>
        internal async Task<List<CDA>> ObtenerCdasAsync(string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            List<CDA> operacionesCDA = await this.ObtencionDatosAsync(folio, ct).ConfigureAwait(false);
            bool encontrado = operacionesCDA.Count > 0;

            //Verifica si el CDA tiene fecha valida...
            foreach (CDA cda in operacionesCDA)
            {
                ct.ThrowIfCancellationRequested();

                if (cda.fechaT24 is null)
                {
                    encontrado = false;
                    break;
                }
            }

            //Si no pudo obtener sus complementos o no tiene fecha valida...
            if (operacionesCDA.Count is 0 || !encontrado)
                throw new Exception("No se encontro el detalle de la operación");

            return operacionesCDA;
        }

        /// <summary>
        /// Obtiene el RFC y lo asigna.
        /// </summary>
        internal async Task<string> ObtenerRfcAsync(CDA cda, string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            //Si no se encuentra el RFC, se asigna ND para que el proceso continúe...
            cda.RFCCURPBeneficiario = ConstantesServicio.ND;
            if (!string.Equals(cda.customerCode, ConstantesServicio.ND, StringComparison.Ordinal))
            {
                try
                {
                    Config.EscribirLog($"INFO|ProcesaOperacion_CISE|Extracción RFC {ConstantesServicio.uspObtenerRfcClienteT24}");
                    string rfc = await _detallesCiseT24.ObtenerRfcAsync(cda.customerCode, ct).ConfigureAwait(false);
                    Config.EscribirLog($"INFO|ProcesaOperacion_CISE|RFC extraído para el folio {folio}:{rfc}");
                    return rfc;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"ERROR|ProcesaOperacion_CISE|No se pudo el RFC del Beneficiario, por lo que se mandará con ND. Error: {ex}");
                }
            }
            return null;
        }

        /// <summary>
        /// Inserta la orden de pago y retorna su resultado.
        /// </summary>
        internal async Task<bool> InsertarOrdenPagoAsync(CDA cda, string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            //Inserta la orden de pago...
            Config.EscribirLog($"INFO|ProcesaOperacion_CISE|-> sp_InterfaceMW_SPEI_CEP {cda.idFolioOperacion} {cda.fechaT24} {cda.horaT24} {cda.queNodo} {cda.RFCCURPBeneficiario}");
            InsercionDto resultado = await _detallesCiseSpei.InsertarOrdenPagoAsync(cda, ct).ConfigureAwait(false);

            //Si el SP no regresó filas, se replica el comportamiento legacy: continúa sin error...
            if (resultado is null)
                return false;

            Config.EscribirLog($"INFO|ProcesaOperacion_CISE|Status Ejecucion --> {resultado.errorCode}");

            //Si por lo menos hay un detalle que no puede insertar, detiene el proceso...
            if (resultado.errorCode != 0)
            {
                Config.EscribirLog($"ERROR|ProcesaOperacion_CISE|Error al insertar los datos de la orden de pago.  op_clave={cda.idFolioOperacion},{cda.fechaT24},{cda.horaT24}");
                Config.EscribirLog($"ERROR|ProcesaOperacion_CISE|Carga a CEP_SPEI|{resultado.errorMessage}");
                throw new Exception($"No encontró el CDA teoricamente recien insertado op_clave={folio}");
            }

            return true;
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Servicios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
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
        private readonly ENT_SPEI _entSpei;
        private readonly EstadoHorarioOperacion _estadoHorarioOperacion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public OperacionServicio(Configuracion configuracion, IControlServicio controlServicio, IAlertas alertas, IParametrizacion parametrizacion, IRecuperacionDatos<OperacionDTO> recuperacionDatos, ENT_SPEI entSpei, EstadoHorarioOperacion estadoHorarioOperacion)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _alertas = alertas;
            _parametrizacion = parametrizacion;
            _recuperacionDatos = recuperacionDatos;
            _entSpei = entSpei;
            _estadoHorarioOperacion = estadoHorarioOperacion;
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
                await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct).ConfigureAwait(false);

                //Obtiene los parámetros del sistema...
                _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync(ct).ConfigureAwait(false);
                _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil(ct).ConfigureAwait(false);
                _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesServicio.ParametrosSistema, ct).ConfigureAwait(false);

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
                    }, ct).ConfigureAwait(false);
                    return false;
                }

                //Valida el horario de operación...
                TimeSpan timeHoraSistema = _parametrizacion.ObtenerHoraSistema();
                string horaSistema = timeHoraSistema.ToString(@"hh\:mm");
                TimeSpan horaOperacionInicio = ConversionHelper.ParseTimeSpanRequerido(_configuracion.ParametrosSistema[ConstantesGenerales.HoraOPInicio], ConstantesGenerales.HoraOPInicio);
                TimeSpan horaOperacionFin = ConversionHelper.ParseTimeSpanRequerido(_configuracion.ParametrosSistema[ConstantesGenerales.HoraOPCepFin], ConstantesGenerales.HoraOPCepFin);

                if (timeHoraSistema >= horaOperacionInicio
                    && timeHoraSistema <= horaOperacionFin)
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
                    }, ct).ConfigureAwait(false);
                    return false;
                }

                //Recupera los datos de procesamiento...
                IEnumerable<OperacionDTO> operaciones = await _recuperacionDatos.RecuperarDatosAsync(ct).ConfigureAwait(false);

                //Realiza un procesamiento diferente de acuerdo al tipo de operación...
                foreach (OperacionDTO operacion in operaciones)
                {
                    using (Logging.BeginOperacionScope(operacion.tipo_operacion, operacion.folio.ToString()))
                    {
                        try
                        {
                            ct.ThrowIfCancellationRequested();
                            bool resultado = false;

                            Config.EscribirLog($"INFO|Ejecuta_Procedimientos|***** TipoOperacion:{operacion.tipo_operacion}, folio:{operacion.folio} *****");
                            switch (operacion.tipo_operacion)
                            {
                                case ConstantesServicio.EntSpei:
                                    resultado = await _entSpei.ProcesarOperacionCISEAsync(operacion.folio.ToString(), ct).ConfigureAwait(false);
                                    break;
                                default:
                                    throw new Exception($"Tipo de operación desconocida:'{operacion.tipo_operacion}'");
                            }

                            if (resultado)
                                Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Se termina procesamiento de folio: {operacion.folio}");
                            else
                                Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Error Salida durante el procesamiento del folio: {operacion.folio}");
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
                            }, ct).ConfigureAwait(false);
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
                            }, ct).ConfigureAwait(false);
                        }
                    }
                }

                //Marca el termino del servicio...
                await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct).ConfigureAwait(false);

                return true;
            }
            catch (OperationCanceledException)
            {
                Config.EscribirLog("INFO|Ejecuta_Procedimientos|Operación cancelada.");
                return false;
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
                }, ct).ConfigureAwait(false);
            }
            return false;
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using MUFG_T24.Comunes;
using System;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Modelo que permite crear un CDA con los datos de un ComplementoCiseDTO.
    /// </summary>
    public class CDA
    {
        private string _rfcCurpBeneficiario = ConstantesServicio.ND;

        public long idFolioCDA { get; set; }
        public decimal idFolioOperacion { get; set; }
        public decimal fechaOperacion { get; set; }
        public string fechaT24 { get; set; }
        public string horaT24 { get; set; }
        public string queNodo { get; set; }
        public string customerCode { get; set; }
        public string RFCCURPBeneficiario
        {
            get
            {
                if (_rfcCurpBeneficiario.Length > 18)
                    return _rfcCurpBeneficiario.Substring(0, 17);

                return _rfcCurpBeneficiario;
            }
            set
            {
                if (!string.IsNullOrEmpty(value) && value.TrimStart().Length > 0)
                    _rfcCurpBeneficiario = value;
                else
                    _rfcCurpBeneficiario = ConstantesServicio.ND;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public CDA(string folio, ComplementoCiseDTO complementoCISE_DTO)
        {
            if (complementoCISE_DTO is null)
            {
                Config.EscribirLog($"ERROR|ProcesaOperacion_CISE|Recupera Fecha y Hora Autoriza T24 en SPEI del folio: {folio}.");
                throw new Exception("No se encontro el detalle de la operación");
            }

            this.idFolioCDA = 0;
            this.idFolioOperacion = complementoCISE_DTO.op_clave;
            this.fechaOperacion = complementoCISE_DTO.op_fecha_oper;

            if (complementoCISE_DTO.fechaT24 != null)
            {
                this.fechaT24 = complementoCISE_DTO.fechaT24;
                this.horaT24 = complementoCISE_DTO.horaT24;
                this.queNodo = complementoCISE_DTO.NODO.ToString();
                Config.EscribirLog($"INFO|ProcesaOperacion_CISE|DATOS T24 {complementoCISE_DTO.fechaT24} --> {complementoCISE_DTO.horaT24} --> {complementoCISE_DTO.NODO}");
            }
            else
            {
                Config.EscribirLog($"ERROR|ProcesaOperacion_CISE|Recupera Fecha y Hora Autoriza T24 en SPEI del folio: {folio}.");
            }

            if (complementoCISE_DTO.T24_CUSTOMER_CODE != null)
                this.customerCode = complementoCISE_DTO.T24_CUSTOMER_CODE;
            else
                this.customerCode = ConstantesServicio.ND;

            Config.EscribirLog($"INFO|ProcesaOperacion_CISE|DATOS T24 {this.customerCode}");
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorios
{
    /// <summary>
    /// Clase de recuperación de datos detalle de InterfacesT24.
    /// </summary>
    public class DetallesCiseDao : RepositorioMaestro, IDetallesCise
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesCiseDao(Configuracion configuracion)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) 
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Obtiene los detalles del folio proporcionado.
        /// </summary>
        public async Task<IEnumerable<ComplementoCiseDTO>> RecuperarDetallesAsync(string folio, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ComplementoCiseDTO>(ConstantesServicio.uspRecuperarDetalles, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", _configuracion.FechaSistema));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
            }, ct).ConfigureAwait(false);
            return resultado.Data;
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
            }, ct, validarAfectacion: false).ConfigureAwait(false);
            return resultado.Afecto == true;
        }       

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorios
{
    /// <summary>
    /// Clase de recuperación de datos detalle de Enlace.
    /// </summary>
    public class DetallesCiseSpeiDao : RepositorioMaestro, IDetallesCiseSpei
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesCiseSpeiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI1, null, null) { }

        /// <summary>
        /// Inserta una orden de pago.
        /// </summary>
        public async Task<InsercionDto> InsertarOrdenPagoAsync(CDA cda, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<InsercionDto>(ConstantesServicio.uspInsertarOrdenPago, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", Convert.ToInt64(cda.idFolioOperacion)));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaT24", Convert.ToInt64(cda.fechaT24)));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@HoraT24", cda.horaT24));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@QueNodo", Convert.ToInt64(cda.queNodo)));
            }, lector => new InsercionDto()
            {
                errorCode = Convert.ToInt32(lector.GetValue(0)),
                errorMessage = Convert.ToString(lector.GetValue(1))
            }, ct, validarAfectacion: false).ConfigureAwait(false);
            return resultado.Data.FirstOrDefault();
        }
       
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorios
{
    /// <summary>
    /// Clase de recuperación de datos detalle de VistasT24.
    /// </summary>
    public class DetallesCiseT24Dao : RepositorioMaestro, IDetallesCiseT24
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public DetallesCiseT24Dao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW, null, null) { }

        /// <summary>
        /// Obtiene el RFC correspondiente al codigo de cliente.
        /// </summary>
        public async Task<string> ObtenerRfcAsync(string customerCode, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<T24CustomerRfcDTO>(ConstantesServicio.uspObtenerRfcClienteT24, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@CUSTOMER_CODE", customerCode));
            }, ct).ConfigureAwait(false);
            return resultado.Data.FirstOrDefault()?.RFC;
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
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
            }, ct).ConfigureAwait(false);
            return resultado.Data;
        }

    }
}

using Aplicacion;
using Aplicacion.DTOs;
using Aplicacion.Interfaces;
using Aplicacion.Servicios;
using Infraestructura.Repositorios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SPEI_CEP_GEN_CDA
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
            _containerDI.Register<OperacionServicio>();
            _containerDI.Register<ENT_SPEI>();
            _containerDI.Register<IRecuperacionDatos<OperacionDTO>, SondeoDao>();
            _containerDI.Register<IDetallesCise, DetallesCiseDao>();
            _containerDI.Register<IDetallesCiseSpei, DetallesCiseSpeiDao>();
            _containerDI.Register<IDetallesCiseT24, DetallesCiseT24Dao>();

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

using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;

namespace SPEI_CEP_GEN_CDA
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

USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_SpeiGenCda_MarcarOperacionProcesada
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-08-03

DESCRIPCIÓN         :
Marca como procesada una operación en el plan de trabajo maestro.

ALCANCE / NOTAS     :
- Operación de actualización.
- Mantiene la lógica legacy.
- Devuelve filas afectadas para validación del DAO.

PARÁMETROS          :
@Folio         BIGINT
@TipoOperacion VARCHAR(10)

CONTROL DE CAMBIOS  :
2026-08-03 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE [dbo].[usp_SpeiGenCda_MarcarOperacionProcesada]
(
      @Folio          BIGINT
    , @TipoOperacion  VARCHAR(10)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @FilasAfectadas INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE dbo.tb_srv_plan_trabajo_maestro
           SET cve_estatus          = 'PROCESADA',
               flg_terminado        = 1,
               fec_fin              = GETDATE(),
               num_estatus_especial = 0
         WHERE folio          = @Folio
           AND tipo_operacion = @TipoOperacion;

        SET @FilasAfectadas = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT
              @FilasAfectadas AS FilasAfectadas
            , CASE WHEN @FilasAfectadas > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS Afecto;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        INSERT INTO Auditoria.ErrorLog
        (
              ErrorNumber
            , ErrorSeverity
            , ErrorState
            , ErrorProcedure
            , ErrorLine
            , ErrorMessage
        )
        VALUES
        (
              ERROR_NUMBER()
            , ERROR_SEVERITY()
            , ERROR_STATE()
            , ERROR_PROCEDURE()
            , ERROR_LINE()
            , ERROR_MESSAGE()
        );

        THROW;
    END CATCH;
END;
GO

USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_SpeiGenCda_ObtenerRegistrosCda
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-08-03

DESCRIPCIÓN         :
Obtiene los registros pendientes de generación de CDA para SPEI,
filtrando por lista de tipos de operación y alias de proceso.

ALCANCE / NOTAS     :
- Operación de consulta.
- Mantiene la lógica legacy.

PARÁMETROS          :
@ListaTiposOperacion NVARCHAR(MAX)
@AliasProceso        VARCHAR(50)

CONTROL DE CAMBIOS  :
2026-08-03 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_SpeiGenCda_ObtenerRegistrosCda
(
      @ListaTiposOperacion NVARCHAR(MAX)
    , @AliasProceso        VARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

        DECLARE @ListaTipos TABLE
        (
            tipo_operacion VARCHAR(10)
        );

        DECLARE @xml XML;

        SET @xml = CAST(
            '<x>' + REPLACE(REPLACE(@ListaTiposOperacion, '''', ''), ',', '</x><x>') + '</x>'
            AS XML
        );

        INSERT INTO @ListaTipos (tipo_operacion)
        SELECT T.c.value('.', 'VARCHAR(10)')
        FROM @xml.nodes('/x') T(c);

        SELECT
              Det_plan.servicio
            , Det_plan.tipo_operacion
            , Det_plan.folio
            , Det_resumen.fec_valor
        FROM
        (
            SELECT
                  Det.folio
                , Det.tipo_operacion
                , MIN(Det.orden_ejecucion) AS orden_ejecucion
                , Mto.fec_valor
            FROM dbo.tb_srv_plan_trabajo_maestro  Mto WITH (NOLOCK)
               , dbo.tb_srv_plan_trabajo_detalle  Det WITH (NOLOCK)
            WHERE Mto.flg_terminado = 0
              AND Mto.flg_cierre    = 0
              AND Mto.tipo_operacion IN
                  (
                      SELECT tipo_operacion
                      FROM @ListaTipos
                  )
              AND Mto.folio          = Det.folio
              AND Mto.tipo_operacion = Det.tipo_operacion
              AND Det.flg_terminado  = 0
            GROUP BY
                  Det.folio
                , Det.tipo_operacion
                , Mto.fec_valor
        ) Det_resumen
        , tb_srv_plan_trabajo_detalle Det_plan WITH (NOLOCK)
        WHERE Det_plan.folio           = Det_resumen.folio
          AND Det_plan.tipo_operacion  = Det_resumen.tipo_operacion
          AND Det_plan.orden_ejecucion = Det_resumen.orden_ejecucion
          AND Det_plan.alias_proceso   = @AliasProceso
        ORDER BY Det_plan.fec_inicio;

    END TRY
    BEGIN CATCH
        INSERT INTO auditoria.errorlog
        (
              errornumber
            , errorseverity
            , errorstate
            , errorprocedure
            , errorline
            , errormessage
        )
        VALUES
        (
              ERROR_NUMBER()
            , ERROR_SEVERITY()
            , ERROR_STATE()
            , ERROR_PROCEDURE()
            , ERROR_LINE()
            , ERROR_MESSAGE()
        );

        THROW;
    END CATCH;
END;
GO

USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_SpeiGenCda_RecuperarDetalles
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Dom�nguez
FECHA               :  2026-08-03

DESCRIPCI�N         :
Recupera el detalle SPEI requerido para generar CDA por folio y fecha
del sistema.

ALCANCE / NOTAS     :
- Operaci�n de consulta.
- Mantiene la l�gica legacy.

PAR�METROS          :
@FechaSistema CHAR(8)
@Folio        NUMERIC(18,0)

CONTROL DE CAMBIOS  :
2026-08-03 | Rafael Dom�nguez | Creaci�n SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_SpeiGenCda_RecuperarDetalles
(
      @FechaSistema  CHAR(8)
    , @Folio         NUMERIC(18,0)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

        SELECT
              1 AS NODO
            , REPLACE(A.NF_FECHAAUTORIZACION,'/','') AS fechaT24
            , REPLACE(A.NF_HORA_AUTORIZACION,':','') AS horaT24
            , A.op_clave
            , A.op_fecha_oper
            , A.T24_CUSTOMER_CODE
        FROM dbo.TB_TOMBIN_NF_ENTRADAS_SPEI A
        WHERE A.op_fecha_oper = @FechaSistema
          AND A.op_clave      = @Folio

        UNION ALL

        SELECT
              2 AS NODO
            , REPLACE(A.NF_FECHAAUTORIZACION,'/','') AS fechaT24
            , REPLACE(A.NF_HORA_AUTORIZACION,':','') AS horaT24
            , A.op_clave
            , A.op_fecha_oper
            , A.T24_CUSTOMER_CODE
        FROM dbo.TB_TOMBIN_NF_ENTRADAS_SPEI_2 A
        WHERE A.op_fecha_oper = @FechaSistema
          AND A.op_clave      = @Folio;

    END TRY
    BEGIN CATCH
        INSERT INTO auditoria.errorlog
        (
              errornumber
            , errorseverity
            , errorstate
            , errorprocedure
            , errorline
            , errormessage
        )
        VALUES
        (
              ERROR_NUMBER()
            , ERROR_SEVERITY()
            , ERROR_STATE()
            , ERROR_PROCEDURE()
            , ERROR_LINE()
            , ERROR_MESSAGE()
        );

        THROW;
    END CATCH;
END;
GO


Aplicacion.csproj

﻿<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <AssemblyName>AplicacionSPEICEPGEN</AssemblyName>
    <PlatformTarget>x86</PlatformTarget>
    <Platforms>AnyCPU;x64;x86</Platforms>
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
    <AssemblyName>InfraestructuraSPEICEPGEN</AssemblyName>
    <PlatformTarget>x86</PlatformTarget>
    <Platforms>AnyCPU;x86</Platforms>
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


SPEI_CEP_GEN_CDAN.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>SPEI_CEP_GEN_CDA</RootNamespace>
    <AssemblyName>SPEI_CEP_GEN_CDA</AssemblyName>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector">
      <HintPath>D:\ServiceCMS\SimpleInjector.dll</HintPath>
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


SPEI_CEP_GEN_CDA.sln

Microsoft Visual Studio Solution File, Format Version 12.00
# Visual Studio Version 18
VisualStudioVersion = 18.4.11612.150
MinimumVisualStudioVersion = 10.0.40219.1
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "SPEI_CEP_GEN_CDAN", "SPEI_CEP_GEN_CDA\SPEI_CEP_GEN_CDAN.csproj", "{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Aplicacion", "Aplicacion\Aplicacion.csproj", "{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Infraestructura", "Infraestructura\Infraestructura.csproj", "{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "PuebasUnitarias", "PuebasUnitarias\PuebasUnitarias.csproj", "{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}"
EndProject
Global
	GlobalSection(SolutionConfigurationPlatforms) = preSolution
		Debug|Any CPU = Debug|Any CPU
		Debug|x64 = Debug|x64
		Debug|x86 = Debug|x86
		Release|Any CPU = Release|Any CPU
		Release|x64 = Release|x64
		Release|x86 = Release|x86
	EndGlobalSection
	GlobalSection(ProjectConfigurationPlatforms) = postSolution
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Debug|x64.ActiveCfg = Debug|Any CPU
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Debug|x64.Build.0 = Debug|Any CPU
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Debug|x86.ActiveCfg = Debug|x86
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Debug|x86.Build.0 = Debug|x86
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Release|Any CPU.Build.0 = Release|Any CPU
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Release|x64.ActiveCfg = Release|Any CPU
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Release|x64.Build.0 = Release|Any CPU
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Release|x86.ActiveCfg = Release|x86
		{2635B66F-7B6E-4439-8584-CBCFBEDAC5AE}.Release|x86.Build.0 = Release|x86
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Debug|x64.ActiveCfg = Debug|x64
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Debug|x64.Build.0 = Debug|x64
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Debug|x86.ActiveCfg = Debug|x86
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Debug|x86.Build.0 = Debug|x86
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|Any CPU.ActiveCfg = Debug|Any CPU
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|Any CPU.Build.0 = Debug|Any CPU
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|x64.ActiveCfg = Release|x64
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|x64.Build.0 = Release|x64
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|x86.ActiveCfg = Release|x86
		{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|x86.Build.0 = Release|x86
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Debug|x64.ActiveCfg = Debug|Any CPU
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Debug|x64.Build.0 = Debug|Any CPU
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Debug|x86.ActiveCfg = Debug|x86
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Debug|x86.Build.0 = Debug|x86
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Release|Any CPU.Build.0 = Release|Any CPU
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Release|x64.ActiveCfg = Release|Any CPU
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Release|x64.Build.0 = Release|Any CPU
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Release|x86.ActiveCfg = Release|x86
		{29917CE7-77FB-4CD4-B196-5A6A3BA132E1}.Release|x86.Build.0 = Release|x86
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Debug|x64.ActiveCfg = Debug|Any CPU
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Debug|x64.Build.0 = Debug|Any CPU
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Debug|x86.ActiveCfg = Debug|x86
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Debug|x86.Build.0 = Debug|x86
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Release|Any CPU.Build.0 = Release|Any CPU
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Release|x64.ActiveCfg = Release|Any CPU
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Release|x64.Build.0 = Release|Any CPU
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Release|x86.ActiveCfg = Release|x86
		{5BAE121B-7C5A-42D5-A3DD-585CCD2F81E9}.Release|x86.Build.0 = Release|x86
	EndGlobalSection
	GlobalSection(SolutionProperties) = preSolution
		HideSolutionNode = FALSE
	EndGlobalSection
	GlobalSection(ExtensibilityGlobals) = postSolution
		SolutionGuid = {811BF4BF-7F6C-46FE-998A-446457D33B91}
	EndGlobalSection
EndGlobal


