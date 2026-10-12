using Calypso_Forex.Aplicacion.DTOs;
using Calypso_Forex.Aplicacion.Enums;
using Calypso_Forex.Aplicacion.Interfaces;
using Calypso_Forex.Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Modelos;
using MUFG_T24.Enums_utilidades;
using System.Xml;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Calypso_Forex.Aplicacion.Servicios
{
    /// <summary>
    /// Clase de implementacion del servicio
    /// </summary>
    public class OperacionesForexService : IOperacionesForex
    {
        #region Propiedades

        private readonly ITomi _tomi;
        private readonly ICalypso _calypso;

        #endregion

        #region Constructor

        /// <summary>
        /// Construccion e inyeccion de servicios
        /// </summary>
        /// <param name="ITOMI"></param>
        /// <param name="calypso"></param>
        public OperacionesForexService(ITomi ITOMI, ICalypso calypso)
        {
            _tomi = ITOMI;
            _calypso = calypso;
        }

        #endregion

        #region Métodos

        /// <summary>
        /// Flujo que valida las líneas forex
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public Task<bool> ValidaLineasForex(CancellationToken ct, string complemento = "")
        {
            return ValidaLineasForex(ct, complemento, DateTime.Today);
        }

        public async Task<bool> ValidaLineasForex(CancellationToken ct, string complemento, DateTime fechaSistema)
        {
            ct.ThrowIfCancellationRequested();

            string Tabla = Constantes.General.TABLA_FOREX + complemento;

            try
            {
                DateTime fechaNegocio = fechaSistema;

                //Logs principales
                Config.EscribirLog("INFO|Valida Líneas FOREX|*****************************************");
                Config.EscribirLog($"INFO|Valida Líneas FOREX| Buscando operaciones nuevas para validar sus líneas de crédito, '{complemento}'");

                //Genera los parametros
                List<Parametros> listaParametros = new List<Parametros>
                {
                    new Parametros(Constantes.General.PARAMETRO_COMPLEMENTO, complemento)
                };

                //Inicialización de variables
                ValidaLineaForexDTO solicitud;

                //Obtiene los datos de base de datos.
                var datosObtenidos = await _tomi.ObtenerDatosProcedimiento<SolicitudesForex>(
                                     Constantes.Procedimientos.VAL_LIN_FOREX_OBTENER_DATOS, listaParametros, ct);

                if (datosObtenidos.Count() == 0)
                    return true;

                //Paso los datos a la tabla de intercambio de calypso - MW
                foreach (var item in datosObtenidos)
                {
                    ct.ThrowIfCancellationRequested();

                    if (complemento != Constantes.General.DB_ET &&
                        complemento != Constantes.General.DB_BANCOS)
                    {

                        if (item.Plazo > 0)
                        {
                            string divisa = string.Empty;
                            decimal montoForex = 0;
                            bool blnDivisa = false;

                            blnDivisa = ProcesarMontoDivisa(item, ref montoForex, ref divisa);

                            if (!blnDivisa)
                            {
                                Config.EscribirLog($"INFO|Valida Líneas FOREX| Validando linea del forex: {item.IdAutorizacion} ");
                                ImporteDisponibleDTO resultado = await _tomi.ObtenerImporteDisponible(item.CDL, divisa, ct);

                                if (resultado == null)
                                {
                                    string mensaje = "Error al validar la linea deCredito. No se obtuvo importe disponible.";
                                    solicitud = new ValidaLineaForexDTO(true, mensaje);
                                }
                                else
                                {
                                    decimal saldoDisponible = resultado.ImporteDisponible;

                                    if (montoForex > saldoDisponible)
                                    {
                                        Config.EscribirLog($"INFO|Valida Líneas FOREX| Linea de credito para el forex {item.IdAutorizacion} es insuficiente." +
                                                        $"MontoLinea= {saldoDisponible} MontoForex = {montoForex}");

                                        string mensaje = $"Linea de credito insuficiente. MontoLinea= {saldoDisponible} MontoForex = {montoForex}.";
                                        solicitud = new ValidaLineaForexDTO(true, mensaje);
                                    }
                                    else
                                    {
                                        Config.EscribirLog($"INFO|Valida Líneas FOREX| Linea de credito disponible para el forex {item.IdAutorizacion}");
                                        solicitud = new ValidaLineaForexDTO(true, null);
                                    }
                                }
                            }
                            else
                            {
                                Config.EscribirLog("INFO|Valida Líneas FOREX| No se puede validar la linea de credito para el forex "
                                                    + item.IdAutorizacion + ", Divisa no es USD ni MXN.");

                                string mensaje = "Error al validar la linea de credito. Divisa no es USD ni MXN.";
                                solicitud = new ValidaLineaForexDTO(true, mensaje);
                            }                       }
                        else
                        {
                            solicitud = new ValidaLineaForexDTO(false, null);
                        }
                    }
                    else
                    {
                        if (complemento == Constantes.General.DB_ET)
                            solicitud = new ValidaLineaForexDTO(false, null, FormatearFechaNegocio(fechaNegocio), FormatearHoraServidorActual());
                        else
                            solicitud = new ValidaLineaForexDTO(false, null);
                    }

                    await _tomi.ActualizaValidaLineaForex(complemento, solicitud, item.IdAutorizacion, ct);
                    Config.EscribirLog("INFO|Valida Líneas FOREX| Se termina procesamiento del forex: " + item.IdAutorizacion);
                }
                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|Valida Líneas FOREX| Se presenta exception en base " + Tabla + ", descripcion: " + ex.ToString());
                return false;
            }
        }

        /// <summary>
        /// Flujo quue revisa operaciones autorizadas forex
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public Task<bool> RevisaOperacionesAutorizadasForex(CancellationToken ct, string complemento = "")
        {
            return RevisaOperacionesAutorizadasForex(ct, complemento, DateTime.Today);
        }

        public async Task<bool> RevisaOperacionesAutorizadasForex(CancellationToken ct, string complemento, DateTime fechaSistema)
        {
            ct.ThrowIfCancellationRequested();

            string tabla = Constantes.General.TABLA_FOREX + complemento;

            try
            {
                DateTime fechaNegocio = fechaSistema;

                //Logs principales
                Config.EscribirLog("INFO|Revisa Operaciones Autorizadas FOREX|*****************************************");
                Config.EscribirLog($"INFO|Revisa Operaciones Autorizadas FOREX| Buscando operaciones Autorizadas FOREX, '{complemento}' ");

                //Se genera la lista de parámetros
                List<Parametros> listaParametros = new List<Parametros>
                {
                    new Parametros(Constantes.General.PARAMETRO_COMPLEMENTO, complemento)
                };

                //Lista donde depositar los datos
                var datosObtenidos = await _tomi.ObtenerDatosProcedimiento<SolicitudesForex>
                                     (Constantes.Procedimientos.REV_OPE_AUT_FOREX_OBTENER_DATOS, listaParametros, ct);

                if (datosObtenidos.Count() == 0)
                    return true;

                //Procesa la información 
                foreach (var idAutorizacion in datosObtenidos.Select(x => x.IdAutorizacion))
                {
                    ct.ThrowIfCancellationRequested();

                    Config.EscribirLog($"INFO|Revisa Operaciones Autorizadas FOREX| Procesando conciliación de FOREX. FolioTomi: {idAutorizacion}");

                    //Actualizamos el dato como actualizado
                    if (complemento == Constantes.General.DB_ET)
                    {
                        await _tomi.ActualizaRevisaOperacionesAutorizadasForex
                            (complemento, idAutorizacion, FormatearFechaNegocio(fechaNegocio), FormatearHoraServidorActual(), ct);
                    }
                    else
                    {
                        await _tomi.ActualizaRevisaOperacionesAutorizadasForex(complemento, idAutorizacion, null, null, ct);
                    }

                    Config.EscribirLog($"INFO|Revisa Operaciones Autorizadas FOREX| Formato|Se termina procesamiento de FolioTomi: {idAutorizacion}");
                }
                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Revisa Operaciones Autorizadas FOREX| Se presenta exception en base {tabla}, descripción: {ex.ToString()}");
                return false;
            }
        }


        /// <summary>
        /// Flujo de Validacion lineas forex autorizados
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public async Task<bool> ValidaLineasForexAutorizado(CancellationToken ct, string complemento = "")
        {
            ct.ThrowIfCancellationRequested();

            string tabla = Constantes.General.TABLA_FOREX + complemento;

            try
            {
                //Logs principales
                Config.EscribirLog("INFO|Valida Líneas FOREX Autorizado|*****************************************");
                Config.EscribirLog($"INFO|Valida Líneas FOREX Autorizado| Buscando operaciones nuevas para validar sus Líneas de crédito, '{complemento}'");

                //Se genera la lista de parámetros
                List<Parametros> listaParametros = new List<Parametros>
                {
                    new Parametros(Constantes.General.PARAMETRO_COMPLEMENTO, complemento)
                };

                //Lista donde depositar los datos
                var datosObtenidos = await _tomi.ObtenerDatosProcedimiento<SolicitudesForex>
                                     (Constantes.Procedimientos.VAL_LIN_FOREX_AUT_OBTENER_DATOS, listaParametros, ct);

                if (datosObtenidos.Count() == 0)
                    return true;

                //Crea el objeto para actualizar.
                ValidaLineaForexDTO solicitud;

                //Procesa la información 
                foreach (var item in datosObtenidos)
                {
                    ct.ThrowIfCancellationRequested();

                    if (complemento != Constantes.General.DB_ET
                        && complemento != Constantes.General.DB_BANCOS)
                    {
                        if (item.Plazo > 0)
                        {
                            string divisa = string.Empty;
                            decimal montoForex = 0;
                            bool blnDivisa = false;
                            string mensaje = string.Empty;

                            blnDivisa = ProcesarMontoDivisa(item, ref montoForex, ref divisa);

                            if (!blnDivisa)
                            {
                                Config.EscribirLog($"INFO|Valida Líneas FOREX Autorizado| Validando linea del forex {item.IdAutorizacion}");

                                ImporteDisponibleDTO result = await _tomi.ObtenerImporteDisponible(item.CDL, divisa, ct);
                                if (result == null)
                                {
                                    mensaje = "Error al validar la linea de credito. No se obtuvo importe disponible.";
                                    solicitud = new ValidaLineaForexDTO(true, true, mensaje, null);
                                }
                                else
                                {
                                    var saldoDisponible = result.ImporteDisponible;

                                    if (montoForex > saldoDisponible)
                                    {
                                        Config.EscribirLog($"INFO|Valida Líneas FOREX Autorizado| Linea de credito para el forex {item.IdAutorizacion}"
                                                        + $" es insuficiente. MontoLinea = {saldoDisponible} MontoForex = {montoForex}");

                                        mensaje = $"Linea de credito insuficiente. MontoLinea={saldoDisponible},  MontoForex = {montoForex}";
                                        solicitud = new ValidaLineaForexDTO(true, true, mensaje, null);
                                    }
                                    else
                                    {
                                        Config.EscribirLog($"INFO|Valida Líneas FOREX Autorizado| Linea de credito disponible para el forex {item.IdAutorizacion}");
                                        solicitud = new ValidaLineaForexDTO(false, false, null, true);
                                    }
                                }
                            }
                            else
                            {
                                Config.EscribirLog($"INFO|Valida Líneas FOREX Autorizado| No se puede validar la linea de credito para el forex {item.IdAutorizacion}, Divisa no es USD ni MXN.");
                                mensaje = "Error al validar la linea de credito. Divisa no es USD ni MXN.";
                                solicitud = new ValidaLineaForexDTO(true, true, mensaje, null);

                            }
                        }
                        else
                            solicitud = new ValidaLineaForexDTO(false, false, null, true);
                    }
                    else
                        solicitud = new ValidaLineaForexDTO(false, false, null, true);

                    //Ejecuta la actualizacion del registro.
                    await _tomi.ActualizaValidaLineaForexAut(complemento, solicitud, item.IdAutorizacion, ct);
                    Config.EscribirLog($"INFO|Valida Líneas FOREX Autorizado| Se termina procesamiento del forex: {item.IdAutorizacion} ");
                }
                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Valida Líneas FOREX Autorizado| Se presenta exception en base '{tabla}', descripcion: {ex.ToString()}");
                return false;
            }
        }

        /// <summary>
        /// Flujo de registro de operaciones forex
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public async Task<bool> RegistraOperacionesForex(CancellationToken ct, string complemento = "")
        {
            ct.ThrowIfCancellationRequested();

            string tabla = Constantes.General.TABLA_FOREX + complemento;

            try
            {
                //Logs principales
                Config.EscribirLog("INFO|Registra Operaciones Forex|*****************************************");
                Config.EscribirLog($"INFO|Registra Operaciones Forex| Buscando operaciones Forex para registrar, '{complemento}' ");

                //Declaramos variables para funcionamiento
                var listaDatos = new List<CalypsoMwSolicitud.CalypsoTrade>();
                string formato = string.Empty;

                //Validamos si es interbancario
                var esInterbancario = (complemento == Constantes.General.DB_BANCOS) ? true : false;

                //Creamos la lista de parametros. 
                List<Parametros> listaParametros = new List<Parametros>
                {
                    new Parametros(Constantes.General.PARAMETRO_COMPLEMENTO, complemento),
                    new Parametros(Constantes.General.PARAMETRO_ES_INTERBANCARIO, esInterbancario)
                };

                //Obtenemos los datos del storedProcedure.
                var datosObtenidos = await _tomi.ObtenerDatosProcedimiento<SolicitudesNegociadores>
                    (Constantes.Procedimientos.REG_OP_FOREX_OBTENER_DATOS, listaParametros, ct);

                if (datosObtenidos.Count() == 0)
                    return true;

                //Iteracion principal 
                foreach (var item in datosObtenidos)
                {
                    ct.ThrowIfCancellationRequested();

                    decimal primary = 0, secondary = 0;

                    var trade = new CalypsoMwSolicitud.CalypsoTrade();
                    var fx = new CalypsoMwSolicitud.FX();
                    var product = new CalypsoMwSolicitud.Product();

                    string[] monedas;
                    string mensajeValidacionProducto;
                    if (!TryObtenerMonedasYMontos(item, out monedas, out primary, out secondary, out mensajeValidacionProducto))
                    {
                        string mensajeError = $"Error en producto/divisas para idAutorizacion={item.idAutorizacion}. {mensajeValidacionProducto}";
                        Config.EscribirLog($"ERROR|Registra Operaciones Forex| {mensajeError}");
                        await RegistrarErrorRegistroForex(complemento, item.idAutorizacion, mensajeError, ct);
                        continue;
                    }

                    #region Configuracion Trade

                    fx.PrimaryCurrency = monedas[0];
                    fx.SecondaryCurrency = monedas[1];

                    decimal final = secondary / primary;

                    fx.PrimaryAmount = (double)primary;
                    fx.SecondaryAmount = (double)secondary;
                    fx.SpotRate = (double)item.precioProm;
                    fx.MarginPoint = (double)Math.Abs(final - item.precioProm);
                    var tipoOperacionCalypso = Constantes.General.NEW;

                    // Nombre corto en Calypso
                    string nombreCorto = await _calypso.ObtenerNombreCorto(item.cdl, ct);

                    product.Item = fx;
                    trade.ExternalReference = ObtenerReferenciaExterna(complemento, item.idAutorizacion);

                    trade.Action = tipoOperacionCalypso;
                    trade.TradeCounterParty = nombreCorto;
                    trade.CounterPartyRole = Constantes.General.COUNTER_PARTY;
                    trade.TradeBook = Constantes.General.TRADING_FX;
                    trade.TradeCurrency = monedas[0];
                    trade.TradeNotional = (double)primary;
                    trade.BuySell = (item.tipoOperacion == Constantes.General.TIPO_OPERACION_COMPRA) ? CalypsoMwSolicitud.BuySellType.Buy : CalypsoMwSolicitud.BuySellType.Sell;
                    trade.TradeDateTime = item.fechaCaptura;
                    trade.TradeSettleDate = item.fecha;
                    trade.StartDate = trade.TradeDateTime;
                    trade.MaturityDate = trade.TradeSettleDate;
                    trade.SalesPerson = Constantes.General.NONE;

                    if (esInterbancario)
                    {
                        if (string.IsNullOrWhiteSpace(item.contacto))
                        {
                            string mensajeError = $"Trader faltante para el folio {trade.ExternalReference}. Operación no registrada en Calypso.";
                            Config.EscribirLog($"ERROR|Registra Operaciones Forex| {mensajeError}");
                            await RegistrarErrorRegistroForex(complemento, item.idAutorizacion, mensajeError, ct);
                            continue;
                        }

                        Config.EscribirLog($"AVISO|Registra Operaciones Forex| Buscando datos del trader: {item.contacto}");
                        trade.TraderName = item.contacto;
                    }
                    else
                    {
                        if (string.IsNullOrWhiteSpace(item.usuarioCaptura))
                        {
                            string mensajeError = $"Negociador faltante para el folio {trade.ExternalReference}. Operación no registrada en Calypso.";
                            Config.EscribirLog($"ERROR|Registra Operaciones Forex| {mensajeError}");
                            await RegistrarErrorRegistroForex(complemento, item.idAutorizacion, mensajeError, ct);
                            continue;
                        }

                        Config.EscribirLog($"AVISO|Registra Operaciones Forex| Trader encontrado: {item.usuarioCaptura}");
                        trade.TraderName = item.usuarioCaptura.ToLower();
                    }

                    trade.ProductType = Constantes.General.FX;
                    trade.ProductSubType = Constantes.General.FX_SPOT;
                    trade.Product = product;

                    trade.HolidayCode = new string[2];
                    bool divisaSinFestivo = false;
                    for (int i = 0; i < 2; i++)
                    {
                        string codigoFestivo;
                        if (!TryObtenerCodigoFestivo(monedas[i], out codigoFestivo))
                        {
                            string mensajeError = $"No se pudo determinar HolidayCode para la divisa '{monedas[i]}' en idAutorizacion={item.idAutorizacion}.";
                            Config.EscribirLog($"ERROR|Registra Operaciones Forex| {mensajeError}");
                            await RegistrarErrorRegistroForex(complemento, item.idAutorizacion, mensajeError, ct);
                            divisaSinFestivo = true;
                            break;
                        }

                        trade.HolidayCode[i] = codigoFestivo;
                    }

                    if (divisaSinFestivo)
                        continue;

                    //Primer elemento
                    trade.TradeKeywords = new CalypsoMwSolicitud.Keyword[4];
                    trade.TradeKeywords[0] = new CalypsoMwSolicitud.Keyword();
                    trade.TradeKeywords[0].KeywordName = Constantes.Keyword.TRADE_SOURCE;
                    trade.TradeKeywords[0].KeywordValue = (esInterbancario) ? Constantes.Keyword.MD_TOMINET : Constantes.Keyword.MD_CMS;

                    //Segundo elemento
                    trade.TradeKeywords[1] = new CalypsoMwSolicitud.Keyword();
                    trade.TradeKeywords[1].KeywordName = Constantes.Keyword.PAY_ACCT;
                    trade.TradeKeywords[1].KeywordValue = item.cuentaCredito ?? string.Empty;

                    //Tercer elemento
                    trade.TradeKeywords[2] = new CalypsoMwSolicitud.Keyword();
                    trade.TradeKeywords[2].KeywordName = Constantes.Keyword.REC_ACCT;
                    trade.TradeKeywords[2].KeywordValue = item.cuentaDebito ?? string.Empty;

                    //Cuarto elemento
                    if (esInterbancario)
                    {
                        trade.TradeKeywords[3] = new CalypsoMwSolicitud.Keyword();
                        trade.TradeKeywords[3].KeywordName = Constantes.Keyword.BROOKER;

                        if (item.broker == Constantes.Keyword.REMT)
                            trade.TradeKeywords[3].KeywordValue = Constantes.Keyword.REMATE;
                        else if (item.broker == Constantes.Keyword.ENLP)
                            trade.TradeKeywords[3].KeywordValue = Constantes.Keyword.ENLACE;
                        else
                            trade.TradeKeywords[3].KeywordValue = Constantes.Keyword.NONE;
                    }

                    #endregion

                    listaDatos.Add(trade);
                }

                // Agregamos los datos a la tabla de intercambio de Calypso MW
                foreach (var item in listaDatos)
                {
                    ct.ThrowIfCancellationRequested();

                    //Log principal
                    Config.EscribirLog("INFO|Registra Operaciones Forex|*****************************************************");

                    //Se declaran variables auxiliares
                    string strXML = string.Empty;
                    int idEnvio = 0;

                    strXML = _calypso.Serializar(item, TipoSerializacion.SerialXML, true, true, EncodingEnum.Encode_UTF8, "CalypsoTrade");
                    formato = item.ExternalReference;

                    Config.EscribirLog($"INFO|Registra Operaciones Forex| Base = '{complemento}', Procesando Formato: {formato}");
                    Config.EscribirLog("INFO|Registra Operaciones Forex| Informacion a operar: ");
                    Config.EscribirLog($"INFO| XML = {strXML}");

                    //Registramos y obtenemos el id
                    idEnvio = await _tomi.RegistrarOperacionForexAtomica(complemento, ConversionesComunes.ParseLongRequerido(formato.Substring(1), "idAutorizacionFormato"), formato, strXML, ct);

                    if (idEnvio == 0)
                        Config.EscribirLog($"INFO|Registra Operaciones Forex| No se tiene informacion del forex: {formato}");
                    else
                    {
                        Config.EscribirLog($"INFO|Registra Operaciones Forex| Operacion realizada exitosamente. Formato: {formato}, con idEnvio: {idEnvio}");
                    }

                    Config.EscribirLog($"INFO|Registra Operaciones Forex| Se termina procesamiento de Formato: {formato.Substring(1)}");
                }

                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Registra Operaciones Forex| Se presenta exception en base '{complemento}', descripcion: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Flujo de revisión de operaciones forex calypso
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public async Task<bool> RevisaOperacionesForexCalypso(CancellationToken ct, string complemento = "")
        {
            ct.ThrowIfCancellationRequested();

            string tabla = Constantes.General.TABLA_FOREX + complemento;

            try
            {
                //Logs principales
                Config.EscribirLog("INFO|Revisa Operaciones FOREX Calypso|*****************************************");
                Config.EscribirLog($"INFO|Revisa Operaciones FOREX Calypso| Buscando respuestas de registros, '{complemento}'");

                //Se genera la lista de parámetros
                List<Parametros> listaParametros = new List<Parametros>
                {
                    new Parametros(Constantes.General.PARAMETRO_COMPLEMENTO, complemento)
                };

                //Se obtienen registros
                var datosObtenidos = await _tomi.ObtenerDatosProcedimiento<SolicitudesForex>
                                 (Constantes.Procedimientos.REV_OP_FOREX_CALYPSO_OBTENER_DATOS, listaParametros, ct);

                if (datosObtenidos.Count() == 0)
                    return true;

                var resultados = new List<ResultadoProcesoAckItem>();

                Config.EscribirLog($"INFO|Revisa Operaciones FOREX Calypso| Contador de operaciones = {datosObtenidos.Count()}");
                //Iteracion y funcionamiento principal
                foreach (var idAutorizacion in datosObtenidos.Select(x => x.IdAutorizacion))
                {
                    ct.ThrowIfCancellationRequested();

                    //Agregamos parámetros
                    var nuevaListaParametros = new List<Parametros>
                    {
                        new Parametros(Constantes.General.PARAMETRO_COMPLEMENTO, complemento),
                        new Parametros(Constantes.General.PARAMETRO_ID_AUTORIZACION, idAutorizacion)
                    };

                    // Se obtiene la informacion acorde al item.
                    var subDatosObtenidos = await _tomi.ObtenerDatosProcedimiento<CalypsoIntercambioDTO>
                                (Constantes.Procedimientos.REV_OP_FOREX_CALYPSO_MENSAJE_SALIDA, nuevaListaParametros, ct);

                    if (subDatosObtenidos == null || !subDatosObtenidos.Any())
                    {
                        string mensajeError = "No se encontraron acuses de salida de Calypso para la operación.";
                        Config.EscribirLog($"ERROR|Revisa Operaciones FOREX Calypso| idAutorizacion: {idAutorizacion}, {mensajeError}");

                        try
                        {
                            await _tomi.ActualizaRevOpeForexCalypso(complemento, idAutorizacion, mensajeError, ct);
                        }
                        catch (Exception exPersistencia)
                        {
                            Config.EscribirLog($"ERROR|Revisa Operaciones FOREX Calypso| No se pudo persistir error para idAutorizacion: {idAutorizacion}. Exception: {exPersistencia}");
                        }

                        resultados.Add(new ResultadoProcesoAckItem
                        {
                            IdAutorizacion = idAutorizacion,
                            IdEnvio = 0,
                            Estado = EstadoProcesoAckItem.AckNoEncontrado,
                            Mensaje = mensajeError
                        });

                        continue;
                    }

                    //Se itera mediante un Try-catch para guardar errores por item
                    foreach (var subItem in subDatosObtenidos)
                    {
                        ct.ThrowIfCancellationRequested();
                        try
                        {
                            Int64 folio = subItem.IdEnvio;
                            string mensaje = subItem.MensajeOut;

                            if (string.IsNullOrWhiteSpace(mensaje))
                            {
                                string mensajeError = "No se tiene mensaje de salida de Calypso";
                                Config.EscribirLog($"ERROR|Revisa Operaciones FOREX Calypso| Para el id_envio: {folio}, {mensajeError}");
                                await _tomi.ActualizarAcuseForexAtomico(complemento, idAutorizacion, folio, ConstantesClaveAviso.Error, mensajeError, ct);

                                resultados.Add(new ResultadoProcesoAckItem
                                {
                                    IdAutorizacion = idAutorizacion,
                                    IdEnvio = folio,
                                    Estado = EstadoProcesoAckItem.MensajeSalidaVacio,
                                    Mensaje = mensajeError
                                });

                                continue;
                            }

                            Config.EscribirLog($"INFO|Revisa Operaciones FOREX Calypso| Para el id_envio: {folio}, se enviara el mensaje = *{mensaje}*.");

                            string referenciaExternaEsperada = ObtenerReferenciaExterna(complemento, idAutorizacion);
                            var resultado = InterpretarRespuesta(mensaje, referenciaExternaEsperada);

                            Config.EscribirLog($"INFO|Revisa Operaciones FOREX Calypso | Para el id_envio: {folio}, la operación se encuentra en estatus: {resultado.Estatus}");
                            if (!resultado.EsExitoso)
                            {
                                string mensajeTas = string.IsNullOrWhiteSpace(resultado.MensajeError)
                                    ? "Acuse de Calypso inválido o no procesable."
                                    : resultado.MensajeError;

                                await _tomi.ActualizarAcuseForexAtomico(complemento, idAutorizacion, folio, ConstantesClaveAviso.Error, mensajeTas, ct);

                                resultados.Add(new ResultadoProcesoAckItem
                                {
                                    IdAutorizacion = idAutorizacion,
                                    IdEnvio = folio,
                                    Estado = EstadoProcesoAckItem.AckInvalido,
                                    Mensaje = mensajeTas
                                });
                            }
                            else
                            {
                                await _tomi.ActualizarAcuseForexAtomico(complemento, idAutorizacion, folio, Constantes.General.ESTATUS_PROCESADO, null, ct);

                                resultados.Add(new ResultadoProcesoAckItem
                                {
                                    IdAutorizacion = idAutorizacion,
                                    IdEnvio = folio,
                                    Estado = EstadoProcesoAckItem.Procesado,
                                    Mensaje = string.Empty
                                });
                            }
                        }
                        catch (Exception ex)
                        {
                            Config.EscribirLog($"Se el item con el ID: {subItem.IdEnvio}, presentó la excepción: {ex.ToString()}");

                            string mensajeError = $"Excepción al procesar acuse: {ex.Message}";
                            EstadoProcesoAckItem estado = EstadoProcesoAckItem.ExcepcionProcesamiento;

                            try
                            {
                                await _tomi.ActualizarAcuseForexAtomico(complemento, idAutorizacion, subItem.IdEnvio, ConstantesClaveAviso.Error, mensajeError, ct);
                            }
                            catch (Exception exPersistencia)
                            {
                                estado = EstadoProcesoAckItem.ExcepcionPersistencia;
                                mensajeError = mensajeError + " | Error persistiendo: " + exPersistencia.Message;
                                Config.EscribirLog($"ERROR|Revisa Operaciones FOREX Calypso| No se pudo persistir error para id_envio: {subItem.IdEnvio}. Exception: {exPersistencia}");
                            }

                            resultados.Add(new ResultadoProcesoAckItem
                            {
                                IdAutorizacion = idAutorizacion,
                                IdEnvio = subItem.IdEnvio,
                                Estado = estado,
                                Mensaje = mensajeError
                            });
                        }
                    }
                }

                int total = resultados.Count;
                int totalNoResueltos = resultados.Count(x => x.Estado != EstadoProcesoAckItem.Procesado);

                Config.EscribirLog($"INFO|Revisa Operaciones FOREX Calypso| Resumen de procesamiento. TotalItems={total}, Exitosos={total - totalNoResueltos}, NoResueltos={totalNoResueltos}");

                if (totalNoResueltos > 0)
                {
                    string detalleNoResueltos = string.Join(" | ", resultados
                        .Where(x => x.Estado != EstadoProcesoAckItem.Procesado)
                        .Select(x => $"idAutorizacion={x.IdAutorizacion}, idEnvio={x.IdEnvio}, estado={x.Estado}, mensaje={x.Mensaje}"));

                    Config.EscribirLog($"ERROR|Revisa Operaciones FOREX Calypso| Existen acuses no resueltos: {detalleNoResueltos}");
                    return false;
                }

                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Revisa Operaciones FOREX Calypso| Se presenta exception: {ex.ToString()}");
                return false;
            }
        }

        #endregion

        #region Métodos Complementarios

        private static string FormatearFechaNegocio(DateTime fechaSistema)
        {
            return fechaSistema.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        private static string FormatearHoraServidorActual()
        {
            return DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Procesa las divisas y asigna nuevos valores
        /// </summary>
        /// <param name="item"></param>
        /// <param name="montoForex"></param>
        /// <param name="divisa"></param>
        /// <returns></returns>
        private bool ProcesarMontoDivisa(SolicitudesForex item, ref decimal montoForex, ref string divisa)
        {
            if (item.DivisaEntrega == ConstantesMonedas.USD)
            {
                divisa = ConstantesMonedas.USD;
                montoForex = item.MontoEntrega;
            }
            else if (item.DivisaEntregaCliente == ConstantesMonedas.USD)
            {
                divisa = ConstantesMonedas.USD;
                montoForex = item.MontoEntregaCliente;
            }
            else if (item.DivisaEntrega == ConstantesMonedas.MXN
                    || item.DivisaEntrega == ConstantesMonedas.MXP)
            {
                divisa = ConstantesMonedas.MXN;
                montoForex = item.MontoEntrega;
            }
            else if (item.DivisaEntregaCliente == ConstantesMonedas.MXN
                    || item.DivisaEntregaCliente == ConstantesMonedas.MXP)
            {
                divisa = ConstantesMonedas.MXN;
                montoForex = item.MontoEntregaCliente;
            }
            else
            {
                return true;
            }

            return false;
        }

        private bool TryObtenerMonedasYMontos(SolicitudesNegociadores item, out string[] monedas, out decimal primary, out decimal secondary, out string mensajeError)
        {
            monedas = null;
            primary = 0;
            secondary = 0;
            mensajeError = string.Empty;

            string producto = (item.producto ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(producto))
            {
                mensajeError = "El campo producto está vacío.";
                return false;
            }

            var tokens = producto.Split('/');
            if (tokens.Length != 2)
            {
                mensajeError = $"El producto '{producto}' no tiene exactamente 2 divisas separadas por '/'.";
                return false;
            }

            string moneda0;
            if (!TryNormalizarMoneda(tokens[0], out moneda0))
            {
                mensajeError = $"La primera divisa '{tokens[0]}' es inválida.";
                return false;
            }

            string moneda1;
            if (!TryNormalizarMoneda(tokens[1], out moneda1))
            {
                mensajeError = $"La segunda divisa '{tokens[1]}' es inválida.";
                return false;
            }

            if (!EsMonedaPermitida(moneda0) || !EsMonedaPermitida(moneda1))
            {
                mensajeError = $"Divisas no permitidas en producto '{producto}'.";
                return false;
            }

            string divisaEntrega;
            if (!TryNormalizarMoneda(item.divisaEntrega, out divisaEntrega))
            {
                mensajeError = $"La divisaEntrega '{item.divisaEntrega}' es inválida.";
                return false;
            }

            if (moneda0 == divisaEntrega)
            {
                primary = item.montoEntrega;
                secondary = item.montoAdquiere;
            }
            else if (moneda1 == divisaEntrega)
            {
                primary = item.montoAdquiere;
                secondary = item.montoEntrega;
            }
            else
            {
                mensajeError = $"La divisaEntrega '{divisaEntrega}' no coincide con el producto '{producto}'.";
                return false;
            }

            if (primary <= 0 || secondary <= 0)
            {
                mensajeError = $"Montos inválidos para cálculo. primary={primary}, secondary={secondary}.";
                return false;
            }

            monedas = new[] { moneda0, moneda1 };
            return true;
        }

        private bool TryNormalizarMoneda(string monedaEntrada, out string monedaNormalizada)
        {
            monedaNormalizada = (monedaEntrada ?? string.Empty).Trim().ToUpperInvariant();

            if (monedaNormalizada.Length != 3)
                return false;

            if (monedaNormalizada == ConstantesMonedas.MXP)
                monedaNormalizada = ConstantesMonedas.MXN;

            return true;
        }

        private bool EsMonedaPermitida(string moneda)
        {
            if (moneda == ConstantesMonedas.MXN)
                return true;

            if (moneda == ConstantesMonedas.USD)
                return true;

            if (moneda == ConstantesMonedas.JPY)
                return true;

            if (moneda == ConstantesMonedas.EUR)
                return true;

            return false;
        }

        private bool TryObtenerCodigoFestivo(string moneda, out string codigoFestivo)
        {
            codigoFestivo = string.Empty;

            if (moneda == ConstantesMonedas.MXN)
            {
                codigoFestivo = ConstantesMonedas.MXN;
                return true;
            }

            if (moneda == ConstantesMonedas.USD)
            {
                codigoFestivo = Constantes.General.CODIGO_FESTIVO_NYC;
                return true;
            }

            if (moneda == ConstantesMonedas.JPY)
            {
                codigoFestivo = Constantes.General.CODIGO_FESTIVO_TOK;
                return true;
            }

            if (moneda == ConstantesMonedas.EUR)
            {
                codigoFestivo = Constantes.General.CODIGO_FESTIVO_TARGET;
                return true;
            }

            return false;
        }

        private async Task RegistrarErrorRegistroForex(string complemento, long idAutorizacion, string mensaje, CancellationToken ct)
        {
            try
            {
                await _tomi.ActualizaRevOpeForexCalypso(complemento, idAutorizacion, mensaje, ct);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Registra Operaciones Forex| No se pudo persistir error para idAutorizacion={idAutorizacion}. Exception: {ex}");
            }
        }

        /// <summary>
        /// Interpreta y valida un acuse de Calypso.
        /// </summary>
        private ResultadoInterpretacionAck InterpretarRespuesta(string mensaje, string referenciaExternaEsperada)
        {
            var resultado = new ResultadoInterpretacionAck
            {
                EsExitoso = false,
                Estatus = string.Empty,
                MensajeError = string.Empty,
                ReferenciaExterna = string.Empty
            };

            try
            {
                var ack = new CalypsoMwRespuesta.CalypsoAcknowledgement();
                object calypsoTrade = ack;

                XmlDocument doc;
                string error;
                if (!XmlHelper.TryParseXmlSeguro(mensaje, out doc, out error))
                {
                    Config.EscribirLog("ERROR|XML|XML inválido: " + error);
                    resultado.MensajeError = string.IsNullOrWhiteSpace(error) ? "XML inválido en acuse Calypso." : error;
                    return resultado;
                }

                Config.EscribirLog("INFO|InterpretarRespuesta| Deserializando clase.");
                _calypso.Deserializar(mensaje, ref calypsoTrade, TipoSerializacion.SerialXML, false, Constantes.General.ACKNOW_LEDGEMENT);
                Config.EscribirLog("INFO|Interpretar Respuesta| Terminado.");

                ack = (CalypsoMwRespuesta.CalypsoAcknowledgement)calypsoTrade;
                if (ack == null)
                {
                    resultado.MensajeError = "No se pudo deserializar el acuse de Calypso.";
                    return resultado;
                }

                if (ack.CalypsoTrades == null || ack.CalypsoTrades.CalypsoTrade == null || ack.CalypsoTrades.CalypsoTrade.Length == 0)
                {
                    resultado.MensajeError = "El acuse de Calypso no contiene operaciones utilizables.";
                    return resultado;
                }

                var trade = ack.CalypsoTrades.CalypsoTrade.FirstOrDefault(x =>
                    string.Equals(x.ExternalRef ?? string.Empty, referenciaExternaEsperada ?? string.Empty, StringComparison.Ordinal));

                if (trade == null)
                {
                    var primeraReferencia = ack.CalypsoTrades.CalypsoTrade.FirstOrDefault()?.ExternalRef ?? string.Empty;
                    resultado.ReferenciaExterna = primeraReferencia;
                    resultado.MensajeError = $"La referencia externa del acuse no coincide. Esperada: {referenciaExternaEsperada}. Recibida: {primeraReferencia}.";
                    return resultado;
                }

                resultado.ReferenciaExterna = trade.ExternalRef ?? string.Empty;
                var status = Enum.GetName(trade.Status.GetType(), trade.Status);
                var estatusMayus = (status ?? string.Empty).ToUpperInvariant();
                resultado.Estatus = estatusMayus;

                if (string.IsNullOrWhiteSpace(estatusMayus))
                {
                    resultado.MensajeError = "El acuse de Calypso no contiene un estatus válido.";
                    return resultado;
                }

                if (estatusMayus == Constantes.General.ESTATUS_REJECTED || estatusMayus == Constantes.General.ESTATUS_FAILED)
                {
                    resultado.MensajeError = ConstruirMensajeErrorTrade(trade, $"Estatus de rechazo recibido: {estatusMayus}.");
                    return resultado;
                }

                if (!EsEstatusExitoso(estatusMayus))
                {
                    resultado.MensajeError = $"Estatus no permitido recibido: {estatusMayus}.";
                    return resultado;
                }

                resultado.EsExitoso = true;
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR| Exception = {ex.ToString()}");
                resultado.MensajeError = ex.ToString();
                resultado.Estatus = ConstantesClaveAviso.Error;
                return resultado;
            }
        }

        private bool EsEstatusExitoso(string estatusMayus)
        {
            if (estatusMayus == Constantes.General.ESTATUS_SUCCESS)
                return true;

            if (estatusMayus == Constantes.General.ESTATUS_SUCCESSFUL)
                return true;

            return false;
        }

        private string ConstruirMensajeErrorTrade(CalypsoMwRespuesta.CalypsoTrade trade, string mensajeDefault)
        {
            if (trade == null || trade.Error == null || trade.Error.Length == 0)
                return mensajeDefault;

            string mensaje = string.Empty;
            for (int i = 0; i < trade.Error.Length; i++)
            {
                string nuevoMensaje = trade.Error[i].Message;
                if (!string.IsNullOrWhiteSpace(nuevoMensaje))
                    mensaje += i + ") " + nuevoMensaje + " ";
            }

            if (string.IsNullOrWhiteSpace(mensaje))
                return mensajeDefault;

            return mensaje.Trim();
        }

        private string ObtenerReferenciaExterna(string complemento, long idAutorizacion)
        {
            switch ((complemento ?? string.Empty).ToUpperInvariant())
            {
                case "":
                    return Constantes.General.PREFIJO_FORMATO_C + idAutorizacion;
                case Constantes.General.DB_FAX:
                    return Constantes.General.PREFIJO_FORMATO_F + idAutorizacion;
                case Constantes.General.DB_ET:
                    return Constantes.General.PREFIJO_FORMATO_B + idAutorizacion;
                case Constantes.General.DB_BANCOS:
                    return Constantes.General.PREFIJO_FORMATO_E + idAutorizacion;
                default:
                    return Constantes.General.PREFIJO_FORMATO_C + idAutorizacion;
            }
        }

        private class ResultadoInterpretacionAck
        {
            public bool EsExitoso { get; set; }
            public string Estatus { get; set; }
            public string MensajeError { get; set; }
            public string ReferenciaExterna { get; set; }
        }

        private class ResultadoProcesoAckItem
        {
            public long IdAutorizacion { get; set; }
            public long IdEnvio { get; set; }
            public EstadoProcesoAckItem Estado { get; set; }
            public string Mensaje { get; set; }
        }

        private enum EstadoProcesoAckItem
        {
            Procesado = 0,
            AckNoEncontrado = 1,
            MensajeSalidaVacio = 2,
            AckInvalido = 3,
            ExcepcionProcesamiento = 4,
            ExcepcionPersistencia = 5
        }

        #endregion
    }
}
