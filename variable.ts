using Calypso_Forex.Aplicacion.DTOs;
using Calypso_Forex.Aplicacion.Enums;
using Calypso_Forex.Aplicacion.Interfaces;
using Calypso_Forex.Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
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
        private DateTime FechaSistema { get; set; }

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
        /// Realiza el conteo de registros  
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task<RegistrosSolicitudesForex> ObtenerConteoRegistros(DateTime fecha)
        {
            FechaSistema = fecha;
            var conteoRegistros = await _tomi.ObtenerRegistrosSolicitudesForex();
            if (conteoRegistros is null)
                throw new Exception(Constantes.LogMensaje.ERROR_NUMERO_REGISTRO);

            return conteoRegistros;
        }

        /// <summary>
        /// Flujo que valida las líneas forex
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public async Task ValidaLineasForex(string complemento = "")
        {
            string Tabla = Constantes.General.TABLA_FOREX + complemento;

            try
            {
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
                                     Constantes.Procedimientos.VAL_LIN_FOREX_OBTENER_DATOS, listaParametros);

                //Paso los datos a la tabla de intercambio de calypso - MW
                foreach (var item in datosObtenidos)
                {
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
                                ImporteDisponibleDTO resultado = await _tomi.ObtenerImporteDisponible(item.CDL, divisa);

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
                            else
                            {
                                Config.EscribirLog("INFO|Valida Líneas FOREX| No se puede validar la linea de credito para el forex "
                                                    + item.IdAutorizacion + ", Divisa no es USD ni MXN.");

                                string mensaje = "Error al validar la linea de credito. Divisa no es USD ni MXN.";
                                solicitud = new ValidaLineaForexDTO(true, mensaje);
                            }
                        }
                        else
                        {
                            solicitud = new ValidaLineaForexDTO(false, null);
                        }
                    }
                    else
                    {
                        if (complemento == Constantes.General.DB_ET)
                            solicitud = new ValidaLineaForexDTO(false, null, FechaSistema.ToString(), FechaSistema.ToString());
                        else
                            solicitud = new ValidaLineaForexDTO(false, null);
                    }

                    await _tomi.ActualizaValidaLineaForex(complemento, solicitud, item.IdAutorizacion);
                    Config.EscribirLog("INFO|Valida Líneas FOREX| Se termina procesamiento del forex: " + item.IdAutorizacion);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|Valida Líneas FOREX| Se presenta exception en base " + Tabla + ", descripcion: " + ex.ToString());
            }
        }

        /// <summary>
        /// Flujo quue revisa operaciones autorizadas forex
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public async Task RevisaOperacionesAutorizadasForex(string complemento = "")
        {
            string tabla = Constantes.General.TABLA_FOREX + complemento;

            try
            {
                //Logs principales
                Config.EscribirLog("INFO|Revisa Operaciones Autorizadas FOREX|*****************************************");
                Config.EscribirLog($"INFO|Revisa Operaciones Autorizadas FOREX| Buscando operaciones Autorizadas FOREX, '{complemento}' ");

                //Valida si es interbancario
                string claveBase = string.Empty;
                bool esInterbancario = (complemento == Constantes.General.DB_BANCOS) ? true : false;

                //Se obtiene clave de Base de datos
                switch (complemento)
                {
                    case "":
                        claveBase = string.Empty;
                        break;
                    case Constantes.General.DB_FAX:
                        claveBase = "F";
                        break;
                    case Constantes.General.DB_ET:
                        claveBase = "E";
                        break;
                    case Constantes.General.DB_BANCOS:
                        claveBase = "B";
                        break;
                }

                //Se genera la lista de parámetros
                List<Parametros> listaParametros = new List<Parametros>
                {
                    new Parametros(Constantes.General.PARAMETRO_COMPLEMENTO, complemento)
                };

                //Lista donde depositar los datos
                var datosObtenidos = await _tomi.ObtenerDatosProcedimiento<SolicitudesForex>
                                     (Constantes.Procedimientos.REV_OPE_AUT_FOREX_OBTENER_DATOS, listaParametros);

                //Procesa la información 
                foreach (var idAutorizacion in datosObtenidos.Select(x => x.IdAutorizacion))
                {
                    Config.EscribirLog($"INFO|Revisa Operaciones Autorizadas FOREX| Procesando conciliación de FOREX. FolioTomi: {idAutorizacion}");

                    //Actualizamos el dato como actualizado
                    if (complemento == Constantes.General.DB_ET)
                    {
                        await _tomi.ActualizaRevisaOperacionesAutorizadasForex
                            (complemento, idAutorizacion, FechaSistema.ToString("yyyy-MM-dd"), DateTime.Now.ToString("HH:mm:ss"));
                    }
                    else
                    {
                        await _tomi.ActualizaRevisaOperacionesAutorizadasForex(complemento, idAutorizacion);
                    }

                    Config.EscribirLog($"INFO|Revisa Operaciones Autorizadas FOREX| Formato|Se termina procesamiento de FolioTomi: {idAutorizacion}");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Revisa Operaciones Autorizadas FOREX| Se presenta exception en base {tabla}, descripción: {ex.ToString()}");
            }
        }


        /// <summary>
        /// Flujo de Validacion lineas forex autorizados
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public async Task ValidaLineasForexAutorizado(string complemento = "")
        {
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
                                     (Constantes.Procedimientos.VAL_LIN_FOREX_AUT_OBTENER_DATOS, listaParametros);

                //Crea el objeto para actualizar.
                ValidaLineaForexDTO solicitud;

                //Procesa la información 
                foreach (var item in datosObtenidos)
                {
                    if (complemento != Constantes.General.DB_ET
                        && complemento != Constantes.General.DB_BANCOS)
                    {
                        if (item.Plazo > 0)
                        {
                            string divisa = string.Empty;
                            decimal montoForex = 0;
                            bool blnDivisa = false;
                            string mensaje = string.Empty;

                            //Valida el monto y la divisa
                            blnDivisa = ProcesarMontoDivisa(item, ref montoForex, ref divisa);

                            if (!blnDivisa)
                            {
                                Config.EscribirLog($"INFO|Valida Líneas FOREX Autorizado| Validando linea del forex {item.IdAutorizacion}");

                                ImporteDisponibleDTO result = await _tomi.ObtenerImporteDisponible(item.CDL, divisa);
                                var saldoDisponible = result.ImporteDisponible;
                                var saldoAutorizado = result.ImporteAutorizado;

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
                    await _tomi.ActualizaValidaLineaForexAut(complemento, solicitud, item.IdAutorizacion);
                    Config.EscribirLog($"INFO|Valida Líneas FOREX Autorizado| Se termina procesamiento del forex: {item.IdAutorizacion} ");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Valida Líneas FOREX Autorizado| Se presenta exception en base '{tabla}', descripcion: {ex.ToString()}");
            }
        }

        /// <summary>
        /// Flujo de registro de operaciones forex
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public async Task RegistraOperacionesForex(string complemento = "")
        {
            string tabla = Constantes.General.TABLA_FOREX + complemento;

            try
            {
                //Logs principales
                Config.EscribirLog("INFO|Registra Operaciones Forex|*****************************************");
                Config.EscribirLog($"INFO|Registra Operaciones Forex| Buscando operaciones Forex para registrar, '{complemento}' ");

                //Declaramos variables para funcionamiento
                var listaDatos = new List<CalypsoMwSolicitud.CalypsoTrade>();
                string formato = string.Empty;
                bool estrategia;

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
                    (Constantes.Procedimientos.REG_OP_FOREX_OBTENER_DATOS, listaParametros);

                //Iteracion principal 
                foreach (var item in datosObtenidos)
                {
                    bool blnErrorNegociador = false;
                    double primary = 0, secondary = 0;

                    ////Inicia la creacion del XML
                    var trade = new CalypsoMwSolicitud.CalypsoTrade();
                    var fx = new CalypsoMwSolicitud.FX();
                    var product = new CalypsoMwSolicitud.Product();
                    var monedas = item.producto.Split('/');

                    #region Configuracion Trade
                    var operacion = item.opTas;
                    monedas[0] = monedas[0].Substring(0, 3);
                    monedas[1] = monedas[1].Substring(0, 3);

                    if (monedas[0].ToString() == item.divisaEntrega)
                    {
                        primary = (double)item.montoEntrega;
                        secondary = (double)item.montoAdquiere;
                    }
                    else if (monedas[1].ToString() == item.divisaEntrega)
                    {
                        primary = (double)item.montoAdquiere;
                        secondary = (double)item.montoEntrega;
                    }

                    if (monedas[0] == Constantes.Divisas.MXP)
                        monedas[0] = Constantes.Divisas.MXN;

                    if (monedas[1] == Constantes.Divisas.MXP)
                        monedas[1] = Constantes.Divisas.MXN;

                    fx.PrimaryCurrency = monedas[0];
                    fx.SecondaryCurrency = monedas[1];
                    fx.PrimaryAmount = primary;
                    fx.SecondaryAmount = secondary;
                    fx.SpotRate = item.precioProm;

                    double final = secondary / primary;

                    fx.MarginPoint = Math.Abs(final - item.precioProm);
                    var tipoOperacionCalypso = Constantes.General.NEW;

                    // Nombre corto en Calypso
                    string nombreCorto = await _calypso.ObtenerNombreCorto(item.cdl);

                    Config.EscribirLog($"INFO|Registra Operaciones Forex| PrimaryCurrency = {monedas[0]}");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| SecondaryCurrency = {monedas[1]}");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| PrimaryAmount = {primary}");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| SecondaryAmount = {secondary}");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| SpotRate = {item.precio}");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| ForwardPoints = {0} ");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| ForwardRate = {0}");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| MarginPoint = {0}");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| FinalRate = {item.precioProm}");
                    Config.EscribirLog($"INFO|Registra Operaciones Forex| CDL = '{item.cdl}', ShortName = '{nombreCorto}'");

                    product.Item = fx;

                    switch (complemento.ToUpper())
                    {
                        case "":
                            trade.ExternalReference = "C" + item.idAutorizacion;
                            break;
                        case Constantes.General.DB_FAX:
                            trade.ExternalReference = "F" + item.idAutorizacion;
                            break;
                        case Constantes.General.DB_ET:
                            trade.ExternalReference = "B" + item.idAutorizacion;
                            break;
                        case Constantes.General.DB_BANCOS:
                            trade.ExternalReference = "E" + item.idAutorizacion;
                            break;
                    }

                    trade.Action = tipoOperacionCalypso;
                    trade.TradeCounterParty = nombreCorto;
                    trade.CounterPartyRole = Constantes.General.COUNTER_PARTY;
                    trade.TradeBook = Constantes.General.TRADING_FX;
                    trade.TradeCurrency = monedas[0].ToString();
                    trade.TradeNotional = primary;
                    trade.BuySell = (item.tipoOperacion == "C") ? CalypsoMwSolicitud.BuySellType.Buy : CalypsoMwSolicitud.BuySellType.Sell;
                    trade.TradeDateTime = item.fechaCaptura;
                    trade.TradeSettleDate = item.fecha;
                    trade.StartDate = trade.TradeDateTime;
                    trade.MaturityDate = trade.TradeSettleDate;
                    trade.SalesPerson = Constantes.General.NONE;

                    if (esInterbancario)
                    {
                        if (item.contacto == string.Empty)
                        {
                            blnErrorNegociador = true;
                            Config.EscribirLog($"ERROR|Registra Operaciones Forex| El Trader del folio {trade.ExternalReference} no existe. Se asigna FXONLINE");
                            trade.TraderName = "RTNS";
                        }
                        else
                        {
                            blnErrorNegociador = false;
                            Config.EscribirLog($"AVISO|Registra Operaciones Forex| Buscando datos del trader: {item.contacto.ToString()}");
                            trade.TraderName = item.contacto.ToString();
                        }
                    }
                    else
                    {
                        if (item.usuarioCaptura == string.Empty)
                        {
                            blnErrorNegociador = true;
                            Config.EscribirLog($"ERROR|Registra Operaciones Forex| El negociador del folio {trade.ExternalReference} no existe.Se asigna FXONLINE");
                            trade.TraderName = "FXONLINE";
                        }
                        else
                        {
                            blnErrorNegociador = false;
                            Config.EscribirLog($"AVISO|Registra Operaciones Forex| Trader encontrado: {item.usuarioCaptura.ToString()}");
                            trade.TraderName = item.usuarioCaptura.ToString().ToLower();
                        }
                    }

                    trade.ProductType = Constantes.General.FX;
                    trade.ProductSubType = Constantes.General.FX_SPOT;
                    trade.Product = product;

                    trade.HolidayCode = new string[2];
                    for (int i = 0; i < monedas.Length; i++)
                    {
                        if (monedas[i] == Constantes.Divisas.MXN || monedas[i] == Constantes.Divisas.MXP)
                        {
                            trade.HolidayCode[i] = Constantes.Divisas.MXN;
                            continue;
                        }

                        if (monedas[i] == Constantes.Divisas.USD)
                        {
                            trade.HolidayCode[i] = Constantes.Divisas.NYC;
                            continue;
                        }

                        if (monedas[i] == Constantes.Divisas.JPY)
                        {
                            trade.HolidayCode[i] = Constantes.Divisas.TOK;
                            continue;
                        }

                        if (monedas[i] == Constantes.Divisas.EUR)
                        {
                            trade.HolidayCode[i] = Constantes.Divisas.TARGET;
                            continue;
                        }
                    }

                    //Primer elemento
                    trade.TradeKeywords = new CalypsoMwSolicitud.Keyword[4];
                    trade.TradeKeywords[0] = new CalypsoMwSolicitud.Keyword();
                    trade.TradeKeywords[0].KeywordName = Constantes.Keyword.TRADE_SOURCE;
                    trade.TradeKeywords[0].KeywordValue = (esInterbancario) ? Constantes.Keyword.MD_TOMINET : Constantes.Keyword.MD_CMS;

                    //Segundo elemento
                    trade.TradeKeywords[1] = new CalypsoMwSolicitud.Keyword();
                    trade.TradeKeywords[1].KeywordName = Constantes.Keyword.PAY_ACCT;
                    trade.TradeKeywords[1].KeywordValue = item.cuentaCredito.ToString();

                    //Tercer elemento
                    trade.TradeKeywords[2] = new CalypsoMwSolicitud.Keyword();
                    trade.TradeKeywords[2].KeywordName = Constantes.Keyword.REC_ACCT;
                    trade.TradeKeywords[2].KeywordValue = item.cuentaDebito.ToString();

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

                    estrategia = (item.estrategia == false) ? false : item.estrategia;

                    #endregion

                    if (!blnErrorNegociador)
                        listaDatos.Add(trade);
                }

                // Agregamos los datos a la tabla de intercambio de Calypso MW
                foreach (var item in listaDatos)
                {
                    //Log principal
                    Config.EscribirLog("INFO|Registra Operaciones Forex|*****************************************************");

                    //Se declaran variables auxiliares
                    string strXML = string.Empty;
                    int idEnvio = 0;

                    strXML = _calypso.Serialiar(item, TipoSerializacion.SerialXML, true, true, EncodingEnum.Encode_UTF8, "CalypsoTrade");
                    formato = item.ExternalReference;

                    Config.EscribirLog($"INFO|Registra Operaciones Forex| Base = '{complemento}', Procesando Formato: {formato}");
                    Config.EscribirLog("INFO|Registra Operaciones Forex| Informacion a operar: ");
                    Config.EscribirLog($"INFO| XML = {strXML}");

                    //Registramos y obtenemos el id
                    idEnvio = await _tomi.RegistrarCalypsoIntercambio(formato, strXML);

                    if (idEnvio == 0)
                        Config.EscribirLog($"INFO|Registra Operaciones Forex| No se tiene informacion del forex: {formato}");
                    else
                    {
                        Config.EscribirLog($"INFO|Registra Operaciones Forex| Operacion realizada exitosamente. Formato: {formato}, con idEnvio: {idEnvio}");

                        //Configura parámetos a enviar
                        string mensaje = $"{strXML.ToString()}";
                        long id = long.Parse(formato.Substring(1));

                        //Actualiza los registros y obtiene las lineas afectadas
                        int registrosAfectados = await _tomi
                            .ActualizaRegistraOperacionesForex(complemento, id, mensaje, idEnvio);

                        ////Valida que hizo el insert correctamente
                        if (registrosAfectados == 0)
                            Config.EscribirLog("INFO|Registra Operaciones Forex| Error en actualizacion de registro");
                    }

                    Config.EscribirLog($"INFO|Registra Operaciones Forex| Se termina procesamiento de Formato: {formato.Substring(1)}");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Registra Operaciones Forex| Se presenta exception en base '{complemento}', descripcion: {ex}");
            }
        }

        /// <summary>
        /// Flujo de revisión de operaciones forex calypso
        /// </summary>
        /// <param name="complemento"></param>
        /// <returns></returns>
        public async Task RevisaOperacionesForexCalypso(string complemento = "")
        {
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
                                 (Constantes.Procedimientos.REV_OP_FOREX_CALYPSO_OBTENER_DATOS, listaParametros);

                Config.EscribirLog($"INFO|Revisa Operaciones FOREX Calypso| Contador de operaciones = {datosObtenidos.Count()}");
                //Iteracion y funcionamiento principal
                foreach (var idAutorizacion in datosObtenidos.Select(x => x.IdAutorizacion))
                {
                    //Agregamos parámetros
                    var nuevaListaParametros = new List<Parametros>
                    {
                        new Parametros(Constantes.General.PARAMETRO_COMPLEMENTO, complemento),
                        new Parametros(Constantes.General.PARAMETRO_ID_AUTORIZACION, idAutorizacion)
                    };

                    // Se obtiene la informacion acorde al item.
                    var subDatosObtenidos = await _tomi.ObtenerDatosProcedimiento<CalypsoIntercambioDTO>
                                (Constantes.Procedimientos.REV_OP_FOREX_CALYPSO_MENSAJE_SALIDA, nuevaListaParametros);

                    foreach (var subItem in subDatosObtenidos)
                    {
                        Int64 folio = subItem.IdEnvio;
                        string mensaje = subItem.MensajeOut;

                        if (mensaje.Trim() == string.Empty)
                        {
                            Config.EscribirLog($"ERROR|Revisa Operaciones FOREX Calypso| Para el id_envio: {folio}, no se tiene mensaje de salida de Calypso");
                            continue;
                        }

                        Config.EscribirLog($"INFO|Revisa Operaciones FOREX Calypso| Para el id_envio: {folio}, se enviara el mensaje = *{mensaje}*.");
                        var estatus = InterpretarRespuesta(ref mensaje);

                        Config.EscribirLog($"INFO|Revisa Operaciones FOREX Calypso | Para el id_envio: {folio}, la operación se encuentra en estatus: {estatus.ToUpper()}");
                        if (estatus.ToUpper() == Constantes.General.ESTATUS_REJECTED)
                        {
                            string mensajeTas = $"No hay registro de la operación en Calypso. - {mensaje}";

                            await _tomi.ActualizaRevOpeForexCalypso(complemento, idAutorizacion, mensajeTas);
                            await _tomi.ActualizaIntercambioCalypso(folio, Constantes.Clave.ERROR);
                        }
                        else
                        {
                            await _tomi.ActualizaRevOpeForexCalypso(complemento, idAutorizacion, null);
                            await _tomi.ActualizaIntercambioCalypso(folio, Constantes.Clave.PROC);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Revisa Operaciones FOREX Calypso| Se presenta exception: {ex.StackTrace.ToString()}");
            }
        }
        #endregion

        #region Métodos Complementarios

        /// <summary>
        /// Procesa las divisas y asigna nuevos valores
        /// </summary>
        /// <param name="item"></param>
        /// <param name="montoForex"></param>
        /// <param name="divisa"></param>
        /// <returns></returns>
        private bool ProcesarMontoDivisa(SolicitudesForex item, ref decimal montoForex, ref string divisa)
        {
            if (item.DivisaEntrega == Constantes.Divisas.USD)
            {
                divisa = Constantes.Divisas.USD;
                montoForex = item.MontoEntrega;
            }
            else if (item.DivisaEntregaCliente == Constantes.Divisas.USD)
            {
                divisa = Constantes.Divisas.USD;
                montoForex = item.MontoEntregaCliente;
            }
            else if (item.DivisaEntrega == Constantes.Divisas.MXN
                    || item.DivisaEntrega == Constantes.Divisas.MXP)
            {
                divisa = Constantes.Divisas.MXN;
                montoForex = item.MontoEntrega;
            }
            else if (item.DivisaEntregaCliente == Constantes.Divisas.MXN
                    || item.DivisaEntregaCliente == Constantes.Divisas.MXP)
            {
                divisa = Constantes.Divisas.MXN;
                montoForex = item.MontoEntregaCliente;
            }
            else
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Recibe un mensaje en XML, le asigna un formato manejable
        /// </summary>
        /// <param name="mensaje"></param>
        /// <returns></returns>
        private string InterpretarRespuesta(ref string mensaje)
        {
            string respuesta = string.Empty;

            try
            {
                var ack = new CalypsoMwRespuesta.CalypsoAcknowledgement();
                object CalypsoTrade = ack;

                Config.EscribirLog("INFO|InterpretarRespuesta| Deserializando clase.");
                _calypso.Deserializar(mensaje, ref CalypsoTrade, TipoSerializacion.SerialXML, false, Constantes.General.ACKNOW_LEDGEMENT);
                Config.EscribirLog("INFO|Interpretar Respuesta| Terminado.");

                ack = (CalypsoMwRespuesta.CalypsoAcknowledgement)CalypsoTrade;
                mensaje = string.Empty;
                respuesta = string.Empty;

                var uploaded = (ack.Uploaded.ToString() != null) ? ack.Uploaded : 0;
                Config.EscribirLog($"INFO| Uploaded = {uploaded}.");

                var rejected = ack.Rejected.ToString();
                Config.EscribirLog($"INFO| Rejected = {rejected}.");

                var received = ack.Received.ToString();
                Config.EscribirLog($"INFO| Received = {received}.");

                var externalRefl = (ack.CalypsoTrades.CalypsoTrade[0].ExternalRef.ToString() != null
                                   ? ack.CalypsoTrades.CalypsoTrade[0].ExternalRef.ToString() : string.Empty);
                Config.EscribirLog($"INFO| ExternalRef = {externalRefl}.");

                var calypsoTradeID = ack.CalypsoTrades.CalypsoTrade[0].CalypsoTradeId;
                Config.EscribirLog($"INFO| CalypsoTradeId = {calypsoTradeID}.");

                var status = Enum.GetName(ack.CalypsoTrades.CalypsoTrade[0].Status.GetType(), ack.CalypsoTrades.CalypsoTrade[0].Status);
                Config.EscribirLog($"INFO| Status = {calypsoTradeID}.");

                var action = ack.CalypsoTrades.CalypsoTrade[0].Action;
                Config.EscribirLog($"INFO| Action = {action}.");

                var processingOrg = ack.CalypsoTrades.CalypsoTrade[0].ProcessingOrg;
                Config.EscribirLog($"INFO| ProcessingOrg = {processingOrg}.");

                try
                {
                    for (int i = 0; i < ack.CalypsoTrades.CalypsoTrade.Length; i++)
                    {
                        string nuevoMensaje = ack.CalypsoTrades.CalypsoTrade[0].Error[i].Message;
                        Config.EscribirLog($"INFO| Mensaje = {nuevoMensaje}");
                        mensaje += i + ") " + nuevoMensaje + " ";
                    }
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"ERROR| Se produjo una excepcion {ex.Message}");
                }

                respuesta = status;

                if (ack.CalypsoTrades.CalypsoTrade[0].Error != null)
                {
                    for (int i = 0; i < ack.CalypsoTrades.CalypsoTrade[0].Error.Length; i++)
                    {
                        string[] newRow =
                        {
                             Enum.GetName(
                                 ack.CalypsoTrades.CalypsoTrade[0].Error[i].Type.GetType(),
                                 ack.CalypsoTrades.CalypsoTrade[0].Error[i].Type
                             ),
                             ack.CalypsoTrades.CalypsoTrade[0].Error[i].Code,
                             ack.CalypsoTrades.CalypsoTrade[0].Error[i].Item,
                             ack.CalypsoTrades.CalypsoTrade[0].Error[i].Message,
                             ack.CalypsoTrades.CalypsoTrade[0].Error[i].Value
                        };

                        //Config.EscribirLog($"INFO| newRow = {newRow}");
                        Config.EscribirLog($"INFO| newRow = {string.Join("|", newRow)}");
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR| Exception = {ex.ToString()}");
                mensaje = ex.ToString();
                respuesta = Constantes.Clave.ERROR;
            }

            return respuesta;
        }
        #endregion
    }
}
