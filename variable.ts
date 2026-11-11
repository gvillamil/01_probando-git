using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Contexto;
using Aplicacion_IN_ETL.DTO.Operaciones;
using Aplicacion_IN_ETL.DTO.SintaxisMT;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Enums;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Interfaces.Externos;
using Aplicacion_IN_ETL.Modelos;
using Aplicacion_IN_ETL.Servicios;
using Aplicacion_IN_ETL.Tag57D;
using Aplicacion_IN_ETL.TipoOperacion;
using Aplicacion_IN_ETL.Utilidades;
using Aplicacion_IN_ETL.Utilidades.Bic;
using Aplicacion_IN_ETL.Utilidades.Entidades;
using Aplicacion_IN_ETL.Utilidades.Flg;
using Aplicacion_IN_ETL.Utilidades.LimpiarValores;
using Aplicacion_IN_ETL.Utilidades.Rechazo;
using Aplicacion_IN_ETL.Utilidades.Tags;
using Aplicacion_IN_ETL.Utilidades.Xml;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using MUFG_T24.SwiftStandardRelease;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.ServiceModel.Channels;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using static Aplicacion_IN_ETL.Modelos.SwiftISO20022;
using static Aplicacion_IN_ETL.Utilidades.Parametros.ParametrosHelper;
using static Aplicacion_IN_ETL.Utilidades.Xml.XmlUtils;
using ConstantesParametrosSistema = Aplicacion_IN_ETL.Constantes.ConstantesParametrosSistema;
namespace Aplicacion_IN_ETL
{
    public class OperacionServicio
    {

        /// <summary>
        /// Campos privados.
        /// </summary>
        #region "Campos Privados"

        private readonly Configuracion _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IAlertas _alertas;
        private readonly IParametrizacion _parametrizacion;
        private readonly IOperaciones _operaciones;
        private readonly IApiComission _apiComission;
        private readonly IT24CommissionService _t24CommissionService;
        private readonly ConexionT24Mw _conexionT24Mw;
        private readonly ICuentasNostro _cuentasNostro;
        private readonly IServicioSpid _servicioSpid;
        private readonly DatosCuenta _datosCuenta;
        private readonly IAlerta _alerta;
        private readonly ILegalEntities _legalEntities;
        private readonly ICounterParty _counterParty;
        private readonly ICuenta _cuenta;
        private readonly ISwift _swift;
        private readonly IReglas _reglas;
        private readonly IActualizacion _actualizacion;
        private readonly IIntegracion _integracion;
        private readonly ISintaxisMT _sintaxis;
        private readonly ISpei _spei;
        private readonly IVostro _vostro;
        private readonly ISamhService _samhService;
        private readonly IPacsService _pacsService;
        private readonly IPacs008 _pacs008;
        private readonly IPacs009 _pacs009;
        private readonly ProcesaCampo _procesaCampo;
        private readonly IContextoXml _contextoXml;
        private readonly ILeerHeader _leerHeader;
        private readonly HttpClient _httpClient;
        private readonly SwiftStandardReleaseSanitize _sanitize;
        private readonly EstadoHorarioOperacion _estadoHorarioOperacion;

        #endregion

        /// <summary>
        /// Constructor.
        /// </summary>
        #region"Constructor"

        public OperacionServicio(Configuracion configuracion, IControlServicio controlServicio, IAlertas alertas, IParametrizacion parametrizacion, IOperaciones operaciones, IApiComission apiComission,
         IT24CommissionService t24CommissionService, ConexionT24Mw conexionT24Mw, ICuentasNostro cuentasNostro, IServicioSpid servicioSpid, DatosCuenta datosCuenta, IAlerta alerta, ILegalEntities legalEntities,
             ICounterParty counterParty, ICuenta cuenta, ISwift swift, IReglas reglas, IActualizacion actualizacion, IIntegracion integracion,
            ISintaxisMT sintaxisMT, ISpei spei, IVostro vostro, ISamhService samhService, IPacsService pacsService, IPacs008 pacs008, IPacs009 pacs009, ProcesaCampo procesaCampo, IContextoXml contextoXml, ILeerHeader leerHeader,
            HttpClient httpClient, SwiftStandardReleaseSanitize sanitize, EstadoHorarioOperacion estadoHorarioOperacion)
        {
            _configuracion = configuracion; _controlServicio = controlServicio; _alertas = alertas; _parametrizacion = parametrizacion; _operaciones = operaciones; _apiComission = apiComission;
            _t24CommissionService = t24CommissionService; _conexionT24Mw = conexionT24Mw; _cuentasNostro = cuentasNostro; _servicioSpid = servicioSpid; _datosCuenta = datosCuenta; _alerta = alerta; _legalEntities = legalEntities;
            _counterParty = counterParty; _cuenta = cuenta; _swift = swift; _reglas = reglas; _actualizacion = actualizacion; _integracion = integracion;
            _sintaxis = sintaxisMT; _spei = spei; _vostro = vostro; _samhService = samhService; _pacsService = pacsService; _pacs008 = pacs008; _pacs009 = pacs009; _procesaCampo = procesaCampo; _contextoXml = contextoXml; _leerHeader = leerHeader;
            _httpClient = httpClient;
            _sanitize = sanitize;
            _estadoHorarioOperacion = estadoHorarioOperacion;
        }

        #endregion

        /// <summary>
        /// Control del flujo principal del procesamiento.
        /// </summary>
        public async Task<bool> EjecutarProcedimientosAsync(CancellationToken ct)
        {
            try
            {
                await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct);
                var validacion = await ValidarEjecucionAsync(ct);

                if (!validacion)
                    return false;

                await ObtieneOperacionesAsync();
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarProcedimientos|Se presenta exception: {ex}");
                await RegistrarAlertaAsync(ConstantesAux.ClaveAvisoCritico, null, ex);
                return false;
            }
            finally
            {
                await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);
            }
        }

        /// <summary>
        /// Validacion del sistema.
        /// </summary>
        private async Task<bool> ValidarEjecucionAsync(CancellationToken ct)
        {
            _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil(ct);

            // Validar día hábil...
            if (_configuracion.EsDiaHabil != true)
            {
                Config.EscribirLog($"INFO|ValidarEjecucion|Hoy es un día inhábil.");
                return false;
            }

            _configuracion.FechaSistema =
                await _parametrizacion.ObtenerFechaSistemaAsync(ct);


            _configuracion.ParametrosSistema
                = await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesParametrosSistema.ParametrosSistema, ct);

            //Validar horario...
            TimeSpan horaSistema =
                _parametrizacion.ObtenerHoraSistema();

            TimeSpan horaInicio =
                ConversionesComunes.ParseTimeSpanRequerido(
                _configuracion.ParametrosSistema[ConstantesGenerales.HoraOPInicio],
                ConstantesGenerales.HoraOPInicio);

            TimeSpan horaFin =
                ConversionesComunes.ParseTimeSpanRequerido(
                _configuracion.ParametrosSistema[ConstantesGenerales.HoraOPFin],
                ConstantesGenerales.HoraOPFin);

            bool dentroHorario =
                horaSistema >= horaInicio &&
                horaSistema <= horaFin;

            if (dentroHorario)
            {
                if (!_estadoHorarioOperacion.EnHorarioOperacion)
                {
                    _estadoHorarioOperacion.EnHorarioOperacion = true;
                    Config.EscribirLog($"INFO|Inicio de horario operación. HoraSistema |{horaSistema}");

                }
                return true;
            }

            if (_estadoHorarioOperacion.EnHorarioOperacion)
            {
                _estadoHorarioOperacion.EnHorarioOperacion = false;
                Config.EscribirLog($"INFO|Fin de horario operación. HoraSistema |{horaSistema}");


                int pidSeguro = _configuracion.Pid ?? 0;
                await _alertas.InsertarAlerta(new AlertaDTO()
                {
                    Folio = null,
                    TipoOPeracion = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = ConstantesClaveAviso.Error,
                    Descipcion = "El servicio está fuera de horario",
                    PId = pidSeguro,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = new Exception($"El servicio '{_configuracion.NombreServicio}' está fuera de horario.")
                });

            }

            Config.EscribirLog(
                $"INFO|Servicio fuera de horario operativo");

            return false;
        }

        /// <summary>
        /// Metodo que registra alerta si ocurre un error.<<
        /// </summary>
        private async Task RegistrarAlertaAsync(string claveAviso, string descripcion, Exception ex)
        {
            await _alertas.InsertarAlerta(new AlertaDTO()
            {
                Folio = null,
                TipoOPeracion = null,
                Servicio = _configuracion.NombreServicio,
                ClaveAviso = claveAviso,
                Descipcion = descripcion,
                PId = _configuracion.Pid.Value,
                NombreProceso = _configuracion.NombreProceso,
                Exception = ex
            });
        }

        /// <summary>
        /// Metodo que invoca a ObtieneOperaciones/ObtieneMensajesMX.
        /// </summary>
        private async Task ObtieneOperacionesAsync()
        {
            try
            {
                await CargaMensajesAsync();
                await CargaMensajesMXAsync();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtieneOperaciones|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Este metodo obtiene las operaciones para procesar los NF_MQ_MESAJE, 
        /// los filtros usados en la consulta son TipoOperación = 'SWIFT_IN' y CVE_ESTATUS = 'READ',
        /// ademas de que flg_cierre = 0
        /// </summary>
        private async Task CargaMensajesAsync()
        {
            try
            {
                Config.EscribirLog("INFO|CargaMensajes|Buscando operaciones...");
                var operaciones = await _operaciones.ObtieneOperacionesAsync(ConstantesServicio.TipoOperacion);

                if (!operaciones.Any())
                {
                    Config.EscribirLog("INFO|ObtieneOperaciones|No hay operaciones a procesar");
                    return;
                }

                foreach (var op in operaciones)
                {
                    await InterpretaMensajesAsync(op);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|CargaMensajes|Error al obtener operaciones|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Este metodo inicia el proceso de los mensajes. 
        /// </summary>
        private async Task InterpretaMensajesAsync(OperacionesDTO op)
        {
            try
            {
                Config.EscribirLog($"INFO|InterpretaMensajes|Inicio analisis de ICN: {op.NF_FOLIO}");
                await IniciaProcesoAsync(op.NF_FOLIO, op.NF_MQ_MSG);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|InterpretaMensajes|Folio:{op.NF_FOLIO}|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Este método crea un contexto XML y genera el folio unico de la operación. 
        /// </summary>
        private async Task IniciaProcesoAsync(string folio, string msg)
        {
            try
            {
                ContextoXmlDTO variablesXml;

                Config.EscribirLog($"INFO|IniciaProceso|ICN = {folio}");
                variablesXml = await _contextoXml.CrearContextoXmlAsync(folio, msg);

                await ProcesaMensajeAsync(variablesXml);

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|IniciaProceso|ICN:{folio}|{ex}");
                throw;
            }
        }

        #region "Procesa Mensajes"

        /// <summary>
        /// Procesa un mensaje SWIFT (MT) a partir de su ICN y contenido.
        /// </summary>
        /// <param name="variablesXml">Contiene los valores que usaremos.</param>
        /// <exception cref="Exception">
        /// 
        public async Task ProcesaMensajeAsync(ContextoXmlDTO variablesXml)
        {
            try
            {
                variablesXml.XmlDoc = XDocument.Parse(IniciaXml(variablesXml.ICN).ToString());
                variablesXml.InterpretacionInicial = await _leerHeader.LeerHeaderAsync(variablesXml);

                if (!variablesXml.InterpretacionInicial)
                {
                    Config.EscribirLog($"WARN|ProcesaMensaje|ICN:{variablesXml.ICN}|No se pudo interpretar el mensaje: {variablesXml.RazonRechazo}");
                }
                else
                {
                    var tagsList = Tags.BuscarTags(variablesXml.Mensaje);
                    var tag = string.Join(",", tagsList);
                    Config.EscribirLog($"INFO|ProcesaMensaje|Para el ICN {variablesXml.ICN} se detectó: TipoMensaje = {variablesXml.TipoMensaje}, Moneda = {variablesXml.Moneda}, Importe = {variablesXml.Importe}, TAG's = {tag}");

                    var criterioCompleto = ConfiguracionHelper.GetBool(_configuracion.ParametrosSistema, ConfiguracionSistema.SWF_CRITERIO_COMPLETO);
                    var sintaxisMT = await _sintaxis.ObtenerSintaxisMtAsync(variablesXml.TipoMensaje, tag, criterioCompleto);

                    if (sintaxisMT.Count > 0 && variablesXml.Clave != ConstantesCategoriaMT.MT950)
                    {
                        await ProcesaTagsAsync(tagsList, sintaxisMT, variablesXml);
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|ProcesaMensaje|Para el ICN {variablesXml.ICN}, el tipo de mensaje debe procesarse manualmente.");
                        variablesXml.RazonRechazo = "MT-950 se procesa manualmente";
                        variablesXml.PuedeInterpretarse = false;
                    }
                }

                if (variablesXml.PuedeInterpretarse)
                {
                    variablesXml.Folio = await _integracion.GeneraFolioUnicoAsync(ConstantesServicio.OperacionFolioUnico, string.Empty, variablesXml.ICN, 13);
                    Config.EscribirLog($"INFO|ProcesaMensaje|FolioUnico = {variablesXml.Folio}");
                }

                variablesXml.VariablesXml = variablesXml.XmlDoc?.ToString() ?? string.Empty;
                await ProcesarResultadoAsync(variablesXml);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesaMensaje|{ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Metodo que procesa los tags del mensaje.
        /// </summary>
        private async Task ProcesaTagsAsync(List<string> tags, Dictionary<string, SintaxisMtDTO> sintaxisMT, ContextoXmlDTO variablesXml)
        {
            try
            {
                variablesXml.TagsValidos = true;

                foreach (var tag in tags)
                {
                    Config.EscribirLog($"INFO|ProcesaTags|Para el ICN {variablesXml.ICN}, analizando el TAG = {tag}");

                    if (sintaxisMT.ContainsKey(tag))
                    {
                        string sintaxis = RegexHelper.ObtenerSintaxis(tag, sintaxisMT);

                        if (string.IsNullOrWhiteSpace(sintaxis))
                        {
                            variablesXml.TagsValidos = false;
                            Config.EscribirLog($"WARN|ObtenerSintaxis|TAG = {tag}, sin sintaxis definida");
                            continue;
                        }

                        var parteMensaje = ObtenerParteMensaje(tag, variablesXml.Mensaje);

                        if (parteMensaje == null)
                        {
                            variablesXml.TagsValidos = false;
                            Config.EscribirLog($"WARN|ObtenerPartesMensaje|TAG = {tag}, no tiene mensaje.");
                            continue;
                        }

                        Config.EscribirLog($"INFO|ProcesaTags|TAG = {tag}.Conversión de sintaxis = {sintaxis}");

                        var expresion = new Regex(sintaxis, RegexOptions.Multiline);

                        foreach (var parteOriginal in parteMensaje)
                        {
                            var valorMsgClean = parteOriginal;
                            var match = expresion.Match(valorMsgClean);

                            if (!match.Success)
                            {
                                Config.EscribirLog($"INFO|ProcesaTags|No se pudo interpretar TAG {tag}");
                                await _alerta.AgregaAlertaAsync(variablesXml.ICN, $"No se pudo interpretar la información con la sintaxis obtenida para el TAG {tag}");
                                continue;
                            }

                            if (tag == ConstantesTags.TAG57D)
                            {
                                Config.EscribirLog($"INFO|ProcesaTags|Procesando TAG {tag}");

                                var procesaTag57 = await ProcesaTag57D.ConstruirDireccionTag57DAsync(variablesXml, tag, valorMsgClean, _sanitize);

                                if (!procesaTag57)
                                {
                                    await _alerta.AgregaAlertaAsync(variablesXml.ICN, $"Error en procesamiento de TAG {tag}: {variablesXml.RazonRechazo}");
                                }

                                continue;
                            }

                            foreach (var groupName in expresion.GetGroupNames())
                            {
                                if (int.TryParse(groupName, out _))
                                    continue;

                                var group = match.Groups[groupName];

                                if (!group.Success)
                                    continue;

                                var rawValue = group.Value;

                                if (string.IsNullOrWhiteSpace(rawValue))
                                    continue;

                                if (group.Captures.Count > 1)
                                {
                                    var listaValores = group.Captures
                                       .Cast<Capture>()
                                       .Select(c => LimpiaValores.LimpiarValor(c.Value))
                                       .Where(v => !string.IsNullOrWhiteSpace(v))
                                       .ToList();

                                    if (listaValores.Count == 0)
                                        continue;

                                    Config.EscribirLog($"INFO|ProcesaTags|[{groupName}] multivalor ({listaValores.Count})");
                                    XmlHelper.AgregarElemento(variablesXml.XmlDoc, groupName, null, listaValores);
                                }
                                else
                                {
                                    var valor = LimpiaValores.LimpiarValor(group.Value);

                                    if (string.IsNullOrWhiteSpace(valor))
                                        continue;

                                    Config.EscribirLog($"INFO|ProcesaTags|{groupName} = {valor}");
                                    XmlHelper.AgregarElemento(variablesXml.XmlDoc, groupName, valor);
                                }
                            }
                        }
                    }
                }

                if (!variablesXml.TagsValidos)
                {
                    variablesXml.PuedeInterpretarse = false;
                    variablesXml.RazonRechazo = "No se pudo establecer el tipo de mensaje";
                    return;
                }

                await TagsValidosAsync(variablesXml);

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesaTags|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Metodo que para la validación core de los datos.
        /// </summary>
        private async Task TagsValidosAsync(ContextoXmlDTO variablesXml)
        {
            var bancoReceptor = ValidarBancoReceptor(variablesXml);

            if (bancoReceptor)
            {
                if (variablesXml.TipoMensaje == ConstantesCategoriaMT.MT191 &&
                    ConfiguracionHelper.GetBool(_configuracion.ParametrosSistema, ConfiguracionSistema.COMM_MT191OUT_FEATURE_FLAG))
                {
                    if (!await ProcesarComisionAsync(variablesXml))
                        return;

                    variablesXml.PuedeInterpretarse = true;
                }
                else
                {
                    if (!await ProcesarValidacionCoreAsync(variablesXml))
                        return;
                }
            }

        }

        /// <summary>
        /// Metodo que valida el banco receptor.
        /// </summary>
        private bool ValidarBancoReceptor(ContextoXmlDTO xml)
        {
            var bancoReceptor = XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.bancoReceptor);

            if (!string.IsNullOrWhiteSpace(bancoReceptor) &&
                (bancoReceptor.StartsWith(ConstantesAux.BANCO_BOTKMXMX) || bancoReceptor == ConstantesAux.BANCO_37046019))
                return true;

            xml.PuedeInterpretarse = false;
            xml.RazonRechazo = "Banco receptor invalido";
            return false;

        }

        /// <summary>
        /// Metodo que procesa la comsión por referencia en t24.
        /// </summary>
        private async Task<bool> ProcesarComisionAsync(ContextoXmlDTO xml)
        {
            try
            {
                if (xml?.XmlDoc?.Root == null)
                    return RazoRechazo.Rechazar(xml, "No se encontró el XML de variables para procesar la comisión.");

                var moneda = XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.mensajeMoneda);

                if (moneda == ConstantesAux.MonedaMXN)
                    return RazoRechazo.Rechazar(xml, "No se procesa cobro de comisión para moneda nacional.");

                var ourRef = XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.ourRelatedRef);
                var theirRef = XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.theirRef);
                var enProceso = await _operaciones.ExisteComisionEnProcesoAsync(ourRef, theirRef);

                if (enProceso)
                    return RazoRechazo.Rechazar(xml, $"Referencia {ourRef} ya está en proceso");

                var resultOperacion = await _operaciones.ObtenerOperacionPorReferenciaT24Async(ourRef);

                if (resultOperacion?.IsSuccess != true || resultOperacion.Data == null)
                    return RazoRechazo.Rechazar(xml, $"No se encontró la operación {ourRef}");

                var op = resultOperacion.Data;

                XmlHelper.AgregarElemento(xml.XmlDoc, ConstantesXml.customerCdl, op.CustomerCdl);
                XmlHelper.AgregarElemento(xml.XmlDoc, ConstantesXml.customerName, op.CustomerName);
                XmlHelper.AgregarElemento(xml.XmlDoc, ConstantesXml.originalAccount, op.OriginalAccount);
                XmlHelper.AgregarElemento(xml.XmlDoc, ConstantesXml.originalDate, op.OperationDate);

                xml.CamposActualizar[ConstantesXml.nombreCliente] = op.CustomerName;
                xml.CamposActualizar[ConstantesXml.numCdl] = op.CustomerCdl;
                xml.CamposActualizar[ConstantesXml.numCliente] = op.CustomerCdl;
                xml.CamposActualizar[ConstantesXml.cveMoneda] = moneda;
                xml.CamposActualizar[ConstantesXml.desImporte] = XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.mensajeImporte);

                var chargeAccount = await _t24CommissionService.GetCommissionAccountAsync(op.CustomerCdl);

                if (chargeAccount?.IsSuccess != true || chargeAccount.Data == null)
                    return RazoRechazo.Rechazar(xml, chargeAccount?.Reason ?? "T24 no devolvió una cuenta de comisión válida.");

                XmlHelper.AgregarElemento(xml.XmlDoc, ConstantesXml.chargeAccount, chargeAccount.Data.Account);
                XmlHelper.AgregarElemento(xml.XmlDoc, ConstantesXml.chargeAccountCurrency, chargeAccount.Data.Currency);

                xml.CamposActualizar[ConstantesXml.numCuenta] = chargeAccount.Data.Account;
                xml.CamposActualizar[ConstantesXml.fecValor] = ConstantesAux.FechaValorNoInformada;

                var apiUrl = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.COMM_API_URL);
                var enqueue = await _apiComission.EnqueueIncomingCommissionChargeRequestAsync(xml.ICN, xml.XmlDoc, apiUrl);

                if (enqueue?.IsSuccess != true)
                    return RazoRechazo.Rechazar(xml, enqueue?.Reason ?? "No fue posible encolar el cobro de comisión.");

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarComision|{ex.Message}");
                xml.RazonRechazo = "Error al procesar la comisión.";
                xml.PuedeInterpretarse = false;
                return false;
            }
        }

        /// <summary>
        /// Metodo para validar el mensaje y obtener ciertos valores en base a la reglas.
        /// </summary>
        private async Task<bool> ProcesarValidacionCoreAsync(ContextoXmlDTO xml)
        {
            Config.EscribirLog("INFO|ProcesarValidacionCore|***** Iniciando validación core *****");

            var result = await ValidaDatosCoreAsync(xml.ICN, xml.TipoMensaje, xml.XmlDoc, xml.CamposActualizar, xml);

            if (!result)
            {
                xml.PuedeInterpretarse = false;

                if (string.IsNullOrWhiteSpace(xml.RazonRechazo))
                    xml.RazonRechazo = "Error en validación datos core";

                return false;
            }

            xml.PuedeInterpretarse = true;

            if (!await EjecutaValidacionCoreAsync(xml))
                return false;

            await SetMTCategoryAsync(xml.XmlDoc, xml.TipoMensaje, xml.ICN);

            return true;
        }

        /// <summary>
        /// Método que aplica reglas y banderas del mensaje.
        /// </summary>
        public async Task<bool> ValidaDatosCoreAsync(string icn, string claveMensaje, XDocument variablesXml, Dictionary<string, string> camposActualizar, ContextoXmlDTO contexto = null)
        {
            var dtc = new ValidaDatosCoreContext();
            try
            {
                Config.EscribirLog($"INFO|ValidaDatosCore|Validando datos ICN = {icn}");

                var reglas = await _swift.ObtenerMapeoCamposAsync(claveMensaje);

                if (reglas.Any())
                {
                    foreach (var regla in reglas)
                    {
                        var cveCampoBd = Comunes.DameTexto(regla.cve_campo_bd);
                        var cveFunConv = (regla.cve_fun_conv ?? "").Split(new[] { "|" }, StringSplitOptions.RemoveEmptyEntries);
                        var cveSwfVar = Comunes.DameTexto(regla.cve_swf_var);
                        var cveEstatus = Comunes.DameTexto(regla.cve_estatus);
                        var valor = XmlHelper.ObtenerValor(variablesXml, cveSwfVar);

                        valor = Conversiones.AplicaConversiones(valor, cveFunConv, dtc);

                        Flags.AplicarFlags(cveFunConv, dtc);
                        await _procesaCampo.ProcesarCampoAsync(cveCampoBd, valor, cveEstatus, camposActualizar, dtc);
                    }
                }

                if (dtc.MensajeErroneo)
                    return false;

                if (dtc.CuentaMandatoria && string.IsNullOrWhiteSpace(dtc.CuentaSwift))
                {
                    dtc.RazonRechazo = "La cuenta no pudo ser leída desde el mensaje Swift.";
                    return false;
                }

                if (dtc.MonedaMandatoria && string.IsNullOrWhiteSpace(dtc.MonedaSwift))
                {
                    dtc.RazonRechazo = "La moneda no pudo ser leída desde el mensaje Swift.";
                    return false;
                }

                if (dtc.ImporteMandatoria && string.IsNullOrWhiteSpace(dtc.ImporteSwift))
                {
                    dtc.RazonRechazo = "El importe no pudo ser leído desde el mensaje Swift.";
                    return false;
                }

                if (dtc.FechaMandatoria && string.IsNullOrWhiteSpace(dtc.FechaSwift))
                    dtc.FechaSwift = DateTime.Now.ToString("yyyyMMdd");

                if (dtc.ValidaCuentaBTMUM)
                {
                    bool validaCuenta = await ValidaCuentaBTMUMAsync(icn, claveMensaje, dtc, variablesXml, camposActualizar);

                    if (!validaCuenta)
                    {
                        return false;
                    }
                }

                if (dtc.ValidaCIFEmisor)
                {
                    bool validaCif = await ValidaCIFEmisorAsync(dtc, variablesXml, camposActualizar);

                    if (!validaCif)
                    {
                        return false;
                    }
                }

                if (dtc.ValidaCIFSpei)
                {
                    bool validaSpei = await ValidaCIFSpeiAsync(dtc, claveMensaje, variablesXml);

                    if (!validaSpei)
                    {
                        return false;
                    }
                }

                if (dtc.ValidaCIFSpei2 && !dtc.CIFCuentaValida)
                {
                    bool validaCifSpei2 = await ValidaCIFSpei2Async(dtc, claveMensaje, variablesXml);

                    if (!validaCifSpei2)
                    {
                        return false;
                    }
                }

                if (dtc.ValidaCIFVostro)
                {
                    bool validaVostro = await ValidaCIFVostroAsync(dtc, variablesXml, camposActualizar);

                    if (!validaVostro)
                    {
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaDatosCore|{ex.Message}");
                dtc.RazonRechazo = "Error en validación datos core";
                return false;
            }
            finally
            {
                if (contexto != null && !string.IsNullOrWhiteSpace(dtc.RazonRechazo))
                    contexto.RazonRechazo = dtc.RazonRechazo;
            }
        }

        /// <summary>
        /// Metodo que valida la cuenta BTMUM.
        /// </summary>
        public async Task<bool> ValidaCuentaBTMUMAsync(string icn, string claveMensaje, ValidaDatosCoreContext dtc, XDocument variablesXml, Dictionary<string, string> camposActualizar)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(dtc.CuentaSwift))
                {
                    Config.EscribirLog("ALERTA|ValidaDatosCore|No existe el dato de la cuenta a validar.");
                    dtc.RazonRechazo = "No existe el dato de la cuenta a validar en el sistema T24.";
                    return false;
                }

                Config.EscribirLog($"INFO|ValidaDatosCore|Validando los datos en T24: Cuenta = {dtc.CuentaSwift}, FechaValor = {dtc.FechaSwift}, Moneda = {dtc.MonedaSwift}, Importe = {dtc.ImporteSwift}");

                try
                {
                    var cuentaVT24 = await _cuenta.ObtenerCuentaVT24Async(dtc.CuentaSwift);

                    if (cuentaVT24 != null)
                    {
                        Config.EscribirLog($"INFO|ObtenerCuentaVT24|Se encontró el dato en T24: CodigoCliente = {cuentaVT24.CUSTOMER}, NombreCuenta = {cuentaVT24.ACCOUNT_TITLE_1}, Cuenta = {cuentaVT24.ACCOUNT_NUMBER}, Moneda = {cuentaVT24.CURRENCY}");

                        dtc.T24Encontrado = true;
                        dtc.T24CodigoCliente = cuentaVT24.CUSTOMER;
                        dtc.T24NombreCuenta = cuentaVT24.ACCOUNT_TITLE_1;
                        dtc.T24CodigoMoneda = cuentaVT24.CURRENCY;
                        dtc.CuentaInterpretada = cuentaVT24.ACCOUNT_NUMBER;
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|ObtenerCuentaVT24|Validar cuenta|NombreCliente: {dtc.T24NombreCliente}, Moneda = {dtc.MonedaSwift}");

                        dtc.CuentaOriginal = dtc.CuentaSwift;
                        var cuentaBusqueda = XmlHelper.ObtenerValor(variablesXml, ConstantesXml.beneficiaryAccount);

                        var resultCuenta = await _cuenta.ValidaDatosCuentaVT24Async(cuentaBusqueda, dtc.MonedaSwift);

                        if (resultCuenta?.IsSuccess == true && resultCuenta.Data != null)
                        {
                            var cuentaResult = resultCuenta.Data;
                            dtc.T24Encontrado = true;
                            dtc.T24CodigoCliente = cuentaResult.CUSTOMER;
                            dtc.T24NombreCuenta = cuentaResult.ACCOUNT_TITLE_1;
                            dtc.T24CodigoMoneda = cuentaResult.CURRENCY;
                            dtc.CuentaInterpretada = cuentaResult.ACCOUNT_NUMBER;

                            Config.EscribirLog($"INFO|ValidaDatosCuentaVT24|Se encontró el dato en T24 por busqueda de cuenta con algoritmo: CodigoCliente = {dtc.T24CodigoCliente},NombreCuenta = {dtc.T24NombreCuenta}, Moneda = {dtc.T24CodigoMoneda}");
                        }

                        Config.EscribirLog($"INFO|ValidaDatosCuentaVT24|CuentaOriginal = {dtc.CuentaOriginal}");
                        Config.EscribirLog($"INFO|ValidaDatosCuentaVT24|Cuenta Interpretada: {dtc.CuentaInterpretada}");

                        if (dtc.CuentaOriginal != dtc.CuentaInterpretada)
                        {
                            QuitaValorXml(variablesXml, ConstantesXml.beneficiaryAccount, out _);

                            if (dtc.CTAInterMx)
                            {
                                await _cuenta.ActualizaFlagCuentaAsync(icn, claveMensaje, "Cuenta barrida de caracteres especiales y letras");
                            }
                            else
                            {
                                await _cuenta.ActualizaFlagCuentaAsync(icn, claveMensaje, "Cuenta identificada de forma extraordinaria");
                            }

                            XmlHelper.AgregarElemento(variablesXml, ConstantesXml.beneficiaryAccount, dtc.CuentaInterpretada);
                            camposActualizar.Remove(ConstantesXml.numCuenta);
                            camposActualizar.Add(ConstantesXml.numCuenta, dtc.CuentaInterpretada);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"ERROR|ValidaDatosCuentaVT24|Error al invocar la consulta de Middleware: {ex.Message}");
                    return false;
                }
                try
                {
                    if (dtc.T24Encontrado)
                    {
                        var resultDatosCliente = await _cuenta.ObtieneDatosClienteAsync(dtc.T24CodigoCliente);

                        if (resultDatosCliente != null)
                        {
                            dtc.T24NombreCliente = resultDatosCliente.NAME_1 ?? string.Empty;
                            dtc.T24CodigoCDL = resultDatosCliente.CUSTOMER_CODE;
                            Config.EscribirLog($"INFO|ObtieneDatosCliente|Se encontró el dato en T24: Nombre Cliente = {dtc.T24NombreCliente}, CDL = {dtc.T24CodigoCDL}");
                        }

                        if (dtc.T24CodigoMoneda == dtc.MonedaSwift)
                        {
                            dtc.T24CuentaValida = true;
                        }
                        else
                        {
                            Config.EscribirLog($"ALERTA|ValidaDatosCore|Las monedas son diferentes.La moneda de la cuenta es {dtc.T24CodigoMoneda} y la moneda del mensaje es {dtc.MonedaSwift}, CuentaMandatoria = {dtc.CuentaMandatoria}.");
                        }

                        var mandatoryCheck = ConfiguracionHelper.GetBool(_configuracion.ParametrosSistema, ConfiguracionSistema.MANDATORY_T24_SERVICE_ACCT_RESTRICT_CHECK);

                        if (mandatoryCheck)
                        {

                            var debitRestriction = ConfiguracionHelper.GetString(
                                _configuracion.ParametrosSistema,
                                ConfiguracionSistema.SWIFT_RESTRICCION_DEBIT_LIST);

                            var accountRestriction = new AccountRestrictionDTO
                            {
                                debitRestrict = string.IsNullOrWhiteSpace(debitRestriction)
                                ? new List<string>()
                                : debitRestriction
                                .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                                .Select(x => x.Trim())
                                .ToList()
                            };

                            dtc.T24AcctRestricted = await _t24CommissionService.ValidateAccountHasCreditRestrictedAsync(
                                dtc.CuentaInterpretada, accountRestriction);
                        }
                    }
                    else
                    {
                        var cuentaBusqueda = XmlHelper.ObtenerValor(variablesXml, ConstantesXml.beneficiaryAccount);
                        dtc.T24AcctClosed = await _cuenta.ExisteCuentaCerradaAsync(cuentaBusqueda, dtc.MonedaSwift);
                    }
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"ERROR|ValidaDatosCore|Error al invocar la consulta de Middleware: {ex}");
                    return false;
                }

                var cuenta = dtc.CuentaInterpretada;

                bool esOperacion = !string.IsNullOrEmpty(cuenta) && cuenta.Length > 7 && !string.IsNullOrEmpty(_conexionT24Mw.Clave_CASFIM) && _conexionT24Mw.Clave_CASFIM.Length >= 5 && string.Equals(
                        dtc.MonedaSwift, _conexionT24Mw.MonedaDefaultMN, StringComparison.OrdinalIgnoreCase) && cuenta.Substring(0, 3) != _conexionT24Mw.Clave_CASFIM.Substring(2, 3);

                dtc.CuentaMandatoria &= !esOperacion;

                if (dtc.CuentaMandatoria && !dtc.T24Encontrado)
                {
                    if (dtc.T24AcctClosed)
                    {
                        dtc.RazonRechazo = $"La cuenta {dtc.CuentaSwift} se encuentra cerrada en el sistema T24";
                    }
                    else
                    {
                        Config.EscribirLog("ALERTA|ValidaDatosCore|No existe la cuenta en el sistema T24");
                        dtc.RazonRechazo = $"No existe la cuenta {dtc.CuentaSwift} en el sistema T24";
                    }
                    return false;
                }

                if (dtc.CuentaMandatoria && !dtc.T24CuentaValida)
                {
                    Config.EscribirLog($"ALERTA|ValidaDatosCore|Diferencia de monedas. La cuenta seleccionada en el sistema T24 está relacionada a {dtc.T24CodigoMoneda} y el mensaje Swift está relacionado a {dtc.MonedaSwift}");
                    dtc.RazonRechazo = $"Diferencia de monedas.La cuenta seleccionada en el sistema T24 está relacionada a {dtc.T24CodigoMoneda} y el mensaje Swift está relacionado a {dtc.MonedaSwift}";
                    return false;
                }

                if (dtc.CuentaMandatoria && dtc.T24AcctRestricted)
                {
                    var mandatoryCheckT24AcctRestricted = ConfiguracionHelper.GetBool(_configuracion.ParametrosSistema, ConfiguracionSistema.MANDATORY_T24_ACCT_RESTRICT_CHECK);

                    if (mandatoryCheckT24AcctRestricted)
                    {
                        Config.EscribirLog("ALERTA|ValidaDatosCore|Cuenta restringida. La cuenta seleccionada en el sistema T24 está marcada como restringida.");
                        dtc.RazonRechazo = ($"La cuenta encontrada en el sistema T24 {dtc.CuentaInterpretada} tiene una restricción para acreditar fondos en T24");
                        return false;
                    }

                    Config.EscribirLog("ALERTA|ValidaDatosCore|Cuenta restringida.La cuenta está marcada como restringida pero no es mandatoria la validación");
                    await _alerta.AgregaAlertaAsync(icn, "La cuenta puede tener una restricción que no permitá acreditar los fondos pero se deja pasar la operación por bandera de configuración en Middleware");
                }

                if (dtc.T24Encontrado && dtc.T24CuentaValida)
                {
                    Config.EscribirLog($"INFO|ValidaDatosCore|La cuenta {dtc.CuentaSwift} existe en T24 y corresponde a la moneda del mensaje {dtc.T24CodigoMoneda}");

                    if (!string.IsNullOrEmpty(dtc.T24NombreCliente) && dtc.T24NombreCliente.Length > 100)
                    {
                        dtc.T24NombreCliente = dtc.T24NombreCliente.Substring(0, 100);
                    }

                    camposActualizar[ConstantesXml.numCliente] = dtc.T24CodigoCliente;
                    camposActualizar[ConstantesXml.nombreCliente] = dtc.T24NombreCliente;
                    camposActualizar[ConstantesXml.numCdl] = dtc.T24CodigoCDL;

                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNumCliente, dtc.T24CodigoCliente);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNombreCliente, dtc.T24NombreCliente);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNumCdl, dtc.T24CodigoCDL);

                }
                else if (dtc.T24Encontrado && (claveMensaje == ConstantesCategoriaMT.MT103 || claveMensaje == ConstantesPACS.PACS008) && !dtc.T24CuentaValida)
                {
                    Config.EscribirLog("ALERTA|ValidaDatosCore|Cuenta inválida.");
                    dtc.RazonRechazo = " La cuuenta inválida.";
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaCuentaBTMUMAsync|Se presenta excepción: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Metodo que valida el CIF Emisor.
        /// </summary>
        public async Task<bool> ValidaCIFEmisorAsync(ValidaDatosCoreContext dtc, XDocument variablesXml, Dictionary<string, string> camposActualizar)
        {
            try
            {
                Config.EscribirLog($"INFO|ValidaDatosCore|Buscando datos del CIF emisor. CIF o SWF = {dtc.CIFSender}, Es Mandatorio = {dtc.CIFEmisorMandatorio}");

                dtc.CIFCuentaValida = false;
                var cuentaVostro = await _vostro.ObtenerCuentaVostroAsync(dtc.CIFSender);

                if (cuentaVostro != null)
                {
                    dtc.CIFCuentaValida = true;

                    camposActualizar[ConstantesXml.cveSwiftEmisor] = cuentaVostro.cve_swift;
                    camposActualizar[ConstantesXml.ctaVostroEmisor] = cuentaVostro.cta_vostro;
                    camposActualizar[ConstantesXml.nombreEmisor] = cuentaVostro.des_nombre_remitente;

                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumCveSwiftEmisor, cuentaVostro.cve_swift);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumCtaVostroEmisor, cuentaVostro.cta_vostro);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNombreEmisor, cuentaVostro.des_nombre_remitente);
                }

                Config.EscribirLog($"ALERTA|ValidaDatosCore|Cuenta CIF para emisor encontrada = {dtc.CIFCuentaValida}");

                if (dtc.CIFEmisorMandatorio && !dtc.CIFCuentaValida)
                {
                    Config.EscribirLog($"ALERTA|ValidaDatosCore|Cuenta CIF para emisor no encontrada.CIF = {dtc.CIFSender}");
                    dtc.RazonRechazo = $"Cuenta CIF de emisor no encontrada. {dtc.CIFSender}";
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaCIFEmisorAsync|Se presenta excepción: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Metodo que valida el CIF Spei.
        /// </summary>
        public async Task<bool> ValidaCIFSpeiAsync(ValidaDatosCoreContext dtc, string claveMensaje, XDocument variablesXml)
        {
            try
            {
                Config.EscribirLog($"INFO|ValidaDatosCore|Buscando datos del CIF spei. CIF o SWF = {dtc.CIFSpei}.Es Mandatorio = {dtc.CIFEmisorMandatorio}.");

                dtc.CIFCuentaValida = false;
                var cifSpei = await _spei.ObtenerCifSpeiAsync(dtc.CIFSpei, claveMensaje);

                if (cifSpei != null)
                {
                    dtc.CIFCuentaValida = true;
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNumCifSpei, cifSpei.num_cif);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumCveSwiftSpei, cifSpei.cve_swift);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumCveCasfimSpei, cifSpei.cve_casfim);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNombreBancoSpei, cifSpei.des_nombre_banco);
                }

                Config.EscribirLog($"ALERTA|ValidaDatosCore|Cuenta CIF para SPEI encontrada = {dtc.CIFCuentaValida}");

                if (dtc.CIFSpeiMandatorio && !dtc.CIFCuentaValida)
                {
                    Config.EscribirLog($"ALERTA|ValidaDatosCore|Cuenta CIF para SPEI no encontrada. CIF = {dtc.CIFSpei}");
                    dtc.RazonRechazo = $"Cuenta CIF de SPEI no encontrada. CIF = {dtc.CIFSpei}";
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaCIFSpeiAsync|Se presenta excepción: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Metodo que valida el CIF Spei2.
        /// </summary>
        public async Task<bool> ValidaCIFSpei2Async(ValidaDatosCoreContext dtc, string claveMensaje, XDocument variablesXml)
        {
            try
            {
                Config.EscribirLog("INFO|ValidaDatosCore|No se encontró el banco a enviar el SPEI en el tag 57A/CdtrAgt, a buscar en intermediarios");
                Config.EscribirLog($"INFO|ValidaDatosCore|Buscando datos del CIF spei. CIF o SWF = {dtc.CIFSpei2}.Es Mandatorio = {dtc.CIFSpeiMandatorio}");

                dtc.CIFCuentaValida = false;
                var cifSpei = await _spei.ObtenerCifSpeiAsync2(dtc.CIFSpei);

                if (cifSpei != null)
                {
                    dtc.CIFCuentaValida = true;
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNumCifSpei, cifSpei.num_cif);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumCveSwiftSpei, cifSpei.cve_swift);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumCveCasfimSpei, cifSpei.cve_casfim);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNombreBancoSpei, cifSpei.des_nombre_banco);
                }

                Config.EscribirLog($"ALERTA|ValidaDatosCore|Cuenta CIF para SPEI encontrada = {dtc.CIFCuentaValida}");

                if (dtc.CIFSpeiMandatorio && !dtc.CIFCuentaValida)
                {
                    Config.EscribirLog($"ALERTA|ValidaDatosCore|Cuenta CIF para SPEI no encontrada. CIF = {dtc.CIFSpei}");
                    dtc.RazonRechazo = $"Cuenta CIF de SPEI no encontrada. CIF = {dtc.CIFSpei}";
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaCIFSpei2Async|Se presenta excepción: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Metodo que valida el CIF Vostro.
        /// </summary>
        public async Task<bool> ValidaCIFVostroAsync(ValidaDatosCoreContext dtc, XDocument variablesXml, Dictionary<string, string> camposActualizar)
        {
            try
            {
                Config.EscribirLog($"INFO|ValidaDatosCore|Buscando datos del CIF vostro. CIF o SWF = {dtc.CIFVostro}.Es Mandatorio = {dtc.CIFVostroMandatorio}");

                dtc.CIFCuentaValida = false;
                var cuentaVostro = await _vostro.ObtenerCuentaCIFVostroAsync(dtc.CIFVostro);

                if (cuentaVostro != null)
                {
                    dtc.CIFCuentaValida = true;
                    camposActualizar[ConstantesXml.nombreCliente] = cuentaVostro.des_nombre_remitente;
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumCveSwiftVostro, cuentaVostro.cve_swift);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumCtaVostroVostro, cuentaVostro.cta_vostro);
                    XmlHelper.AgregarElemento(variablesXml, ConstantesXml.btmumNombreVostro, cuentaVostro.des_nombre_remitente);
                }

                Config.EscribirLog($"ALERTA|ValidaDatosCore|Cuenta CIF para vostro encontrada = {dtc.CIFCuentaValida}");

                if (dtc.CIFVostroMandatorio && !dtc.CIFCuentaValida)
                {
                    Config.EscribirLog($"ALERTA|ValidaDatosCore|Cuenta CIF para vostro no encontrada.CIF = {dtc.CIFVostro}");
                    dtc.RazonRechazo = $"Cuenta CIF de vostro no encontrada. CIF = {dtc.CIFVostro}";
                    return false;
                }
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaCIFVostroAsync|Se presenta excepción: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Metodo para ejecutar las validaciones de lo obtenido en valida datos core.
        /// </summary>
        public async Task<bool> EjecutaValidacionCoreAsync(ContextoXmlDTO variablesXml)
        {
            try
            {
                if (!variablesXml.PuedeInterpretarse)
                    return false;

                if (variablesXml.TipoMensaje != ConstantesCategoriaMT.MT103)
                    return true;

               ConstantesAux.validateSpidLikePaymenst = await ValidateSpidLikePaymentsAsync(
                    variablesXml.TipoMensaje, variablesXml.XmlDoc, variablesXml.RazonRechazo, null);

                var resultCuenta = await ObtenerCuentaAsync(variablesXml.ICN, variablesXml.Moneda, variablesXml.XmlDoc);

                if (resultCuenta?.IsSuccess != true)
                {
                    variablesXml.PuedeInterpretarse = false;
                    variablesXml.RazonRechazo = resultCuenta?.Reason ?? "No fue posible obtener la cuenta de débito.";
                    return false;
                }

                var account = resultCuenta.Data;

                if (!string.IsNullOrWhiteSpace(account))
                {
                    XmlHelper.AgregarElemento(variablesXml.XmlDoc, ConstantesXml.debitAccount, account);
                }

                if (string.IsNullOrWhiteSpace(account) && variablesXml.Moneda != ConstantesAux.MonedaMXN)
                {
                    variablesXml.RazonRechazo = "No se encontro una cuenta a debitar.";
                    variablesXml.PuedeInterpretarse = false;
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutaValidacionCore|Ocurrio un problema con la ejecución|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Metodo para registrar la operación y crear el plan de trabajo del mensaje.
        /// </summary>
        public async Task ProcesarResultadoAsync(ContextoXmlDTO xml)
        {
            bool gbSpidLikePayments = ConstantesAux.gbSpidLikePayments;
            bool rechazoRegistrado = false;
            bool operacionAceptada = false;
            try
            {
                if (xml.PuedeInterpretarse && !gbSpidLikePayments)
                {
                    await _integracion.AceptaOperacionSwtAsync(xml.ICN, xml.Folio, xml.TipoMensaje, xml.VariablesXml, xml.CamposActualizar);
                    operacionAceptada = true;
                    await _integracion.InsertLaikaQueueAsync(xml.ICN);

                    if (xml.TipoMensaje == ConstantesCategoriaMT.MT103 &&
                        XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.detailCharges) == ConstantesAux.OUR &&
                        ConfiguracionHelper.GetBool(_configuracion.ParametrosSistema, ConfiguracionSistema.COMM_MT191IN_FEATURE_FLAG))
                    {
                        Config.EscribirLog($"INFO|Comision|OUR detectado");
                        var apiUrl = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.COMM_API_URL);
                        await EnqueueCommissionChargeRequestAsync(xml.ICN, xml, apiUrl);
                    }

                    await AplicarReglasAsync(xml.ICN, xml.TipoMensaje);
                }
                else if (!ConstantesAux.validateSpidLikePaymenst && gbSpidLikePayments)
                {
                    var rechazo = await RechazarOperacionAsync(xml.ICN, xml);
                    rechazoRegistrado = rechazo?.IsSuccess == true;
                    await MarcarOperacionErrorAsync(xml.ICN, xml.RazonRechazo);
                }
                else
                {
                    if (xml.CamposActualizar.ContainsKey(ConstantesXml.fecValor))
                    {
                        xml.FechaValor = xml.CamposActualizar[ConstantesXml.fecValor];
                    }

                    var rechazo = await RechazarOperacionAsync(xml.ICN, xml);
                    rechazoRegistrado = rechazo?.IsSuccess == true;
                    await ActualizarCamposAsync(xml.ICN, xml);

                    if (gbSpidLikePayments)
                    {
                        await ActualizarEstatusSpidLikePaymentsAsync(xml.ICN);
                    }
                }

                if (gbSpidLikePayments)
                {
                    Config.EscribirLog($"INFO|ProcesarResultado|ICN {xml.ICN} interpretado como SPID");
                }
                else
                {
                    Config.EscribirLog($"INFO|ProcesarResultado|ICN {xml.ICN}.Interpretado: {(xml.PuedeInterpretarse ? "SI" : "NO")}");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarResultado|ICN:{xml.ICN} {ex.Message}");
                await MarcarOperacionErrorAsync(xml.ICN, ex.ToString());
                xml.RazonRechazo = ex.Message;

                if (!rechazoRegistrado && !operacionAceptada)
                    await RechazarOperacionAsync(xml.ICN, xml);
            }
        }

        /// <summary>
        /// Metodo para obtener cuentaNostro.
        /// </summary>
        private async Task<Result<string>> ObtenerCuentaAsync(string icn, string moneda, XDocument xmlDoc)
        {
            if (moneda == ConstantesAux.MonedaUSD)
            {
                return await ObtieneCuentaDebitoAsync(icn, moneda, xmlDoc);
            }

            try
            {
                var account = await _cuentasNostro.ObtenerCuentaNostroAsync(moneda);

                if (string.IsNullOrWhiteSpace(account))
                {
                    return Result<string>.Failed($"No se encontró cuenta NOSTRO para moneda {moneda}");
                }

                return Result<string>.Success(account);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerCuenta|{ex.Message}");
                return Result<string>.Failed($"Error al obtener cuenta NOSTRO para moneda {moneda}");
            }
        }

        /// <summary>
        /// Metodo para obtener la cuenta de debito.
        /// </summary>
        private async Task<Result<string>> ObtieneCuentaDebitoAsync(string icn, string moneda, XDocument variablesXml)
        {
            var bic = Bic.ObtenerBic(variablesXml);

            // CORRESPONSAL
            var result = await _integracion.GetNostroAsync(bic, moneda);

            if (result?.IsSuccess == true && !string.IsNullOrWhiteSpace(result.Data))
                return result;

            // EMISOR
            var bicEmisor = XmlHelper.ObtenerValor(variablesXml, ConstantesXml.bancoEmisor);
            var alert = $"No se encontró cuenta NOSTRO para el corresponsal, se busca cuenta del emisor: {bicEmisor}";

            var fallback = await _integracion.GetNostroAsync(bicEmisor, moneda);

            if (fallback?.IsSuccess == true && !string.IsNullOrWhiteSpace(fallback.Data))
            {
                await _alerta.AgregaAlertaAsync(icn, alert);
                return fallback;
            }

            alert = "No se encontró cuenta NOSTRO para corresponsal ni emisor, se utilizara la cuenta predeterminada en caso de autorizacion automatica (JP Morgan)";
            await _alerta.AgregaAlertaAsync(icn, alert);

            try
            {
                var defaultAccount = await _cuentasNostro.ObtenerCuentaNostroAsync(moneda);
                if (string.IsNullOrWhiteSpace(defaultAccount))
                {
                    return Result<string>.Failed("No se pudo obtener cuenta default");
                }

                return Result<string>.Success(defaultAccount);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerCuentaNostro|{ex.Message}");
                return Result<string>.Failed("Error obteniendo cuenta de débito.");
            }
        }

        /// <summary>
        /// Metodo que guarda la categoria del mensaje.
        /// </summary>
        private async Task SetMTCategoryAsync(XDocument xDocument, string clave, string icn)
        {
            try
            {
                var mensajeMoneda = XmlHelper.ObtenerValor(xDocument, ConstantesXml.mensajeMoneda);
                Config.EscribirLog($"INFO|ObtenerMonedaSwift|Se busca la moneda del mensaje SWIFT: {mensajeMoneda}");
                string lsCurrencyInSwfMessage = _conexionT24Mw.ValidaMoneda(mensajeMoneda);

                string beneficiaryAccount = Comunes.DameTexto(XmlHelper.ObtenerValor(xDocument, ConstantesXml.beneficiaryAccount)) ?? string.Empty;
                Config.EscribirLog($"INFO|ObtenerCuentaBeneficiario|Cuenta beneficiario: {beneficiaryAccount}");

                if (beneficiaryAccount.Length == 18)
                {
                    Config.EscribirLog($"INFO|SetMTCategory|La cuenta {beneficiaryAccount} es una CLABE, se extraera la cuenta interna.");
                    if (beneficiaryAccount.StartsWith(_conexionT24Mw.Clave_CASFIM.Substring(2)))
                    {
                        beneficiaryAccount = beneficiaryAccount.Substring(10, 7);
                    }
                }

                var lsT24AccountData = await ObtenerMonedaT24Async(beneficiaryAccount, mensajeMoneda);
                var categoria = DeterminarCategoria(clave, mensajeMoneda, lsT24AccountData, beneficiaryAccount, xDocument);

                Config.EscribirLog($"INFO|SetMTCategory|ICN:{icn} Categoria:{categoria}");
                await GuardarCategoriaAsync(icn, categoria);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|SetMTCategory|ICN:{icn} {ex.Message}");
            }
        }

        /// <summary>
        /// Metodo que consulta un servicio externo para obtener la comisión.
        /// </summary>
        private async Task<Result<bool>> EnqueueCommissionChargeRequestAsync(string folio, ContextoXmlDTO xml, string apiUrl)
        {
            try
            {
                var receiverAddress = !string.IsNullOrWhiteSpace(XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.orderiInstBic))
                        ? XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.orderiInstBic)
                        : XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.btmumCveSwiftEmisor);

                var currency = XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.settledCcy);
                var rate = ObtenerTasaComision(currency);

                if (rate <= 0)
                {
                    Config.EscribirLog($"WARN|EnqueueCommissionChargeRequest|La comision MT191 en {currency} no se encuentra parametrizada (o es menor a 0), por tanto no se envia el mensaje");
                    return Result<bool>.Success(true);
                }

                //VALIDACIÓN ENVÍO
                var shouldSend = await OutgoingCommissionShouldBeGeneratedAsync(receiverAddress, currency);

                if (!shouldSend)
                {
                    Config.EscribirLog($"WARN|OutgoingCommissionShouldBeGenerated|La comision MT191 en {currency} para {receiverAddress} se encuentra parametrizada para no ser enviada.");
                    return Result<bool>.Success(true);
                }

                //RFK
                var rfk = new string(folio.Where(char.IsDigit).ToArray());

                if (rfk.Length > 16)
                    rfk = rfk.Substring(0, 16);

                //CONSTRUCCIÓN DTO
                var job = new ChargesPaymentRequestDto
                {
                    Rfk = rfk,
                    MessageDirection = MessageDirectionEnum.Outgoing,
                    UnderlyingTransactionUetr = XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.UETR),
                    CounterPartyIdentifier = receiverAddress,
                    RelatedReference = XmlHelper.ObtenerValor(xml.XmlDoc, ConstantesXml.senderReference),
                    Currency = currency,
                    Amount = rate,
                    OrderingInstitutionBICFI = ConstantesAux.BANCO_BOTKMXMX,
                    ChargesBreakdownCurrency = currency,
                    ChargesBreakdownAmount = rate,
                    SenderToReceiverInformationCode = ConstantesAux.OURC,
                    SenderToReceiverInformationInstructionInfo = string.Empty
                };

                Config.EscribirLog("INFO|EnqueueComissionChargeRequest|A generar peticion para cobro de comision OUR");

                var json = JsonConvert.SerializeObject(job, new DecimalFormatConverter());

                Config.EscribirLog($"INFO|EnqueueCommissionChargeRequestAsync|Payload: {json}");

                var content = new StringContent(json, Encoding.UTF8, "application/json");

                Config.EscribirLog($"INFO|EnqueueComissionChargeRequest|To enqueue commission request for MT103 with ref {folio}");

                var response = await _httpClient.PostAsync(apiUrl, content);

                Config.EscribirLog($"INFO|EnqueueCommissionChargeRequest|Respuesta: {response.StatusCode}");

                return Result<bool>.Success(true);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnqueueCommissionChargeRequest|{ex.Message}");
                return Result<bool>.Failed("Error al enviar comisión.");
            }
        }

        private decimal ObtenerTasaComision(string moneda)
        {
            ConfiguracionSistema parametro;

            switch (moneda)
            {
                case ConstantesAux.MonedaUSD:
                    parametro = ConfiguracionSistema.COMMISION_RATE_USD;
                    break;

                case ConstantesAux.MonedaMXN:
                    parametro = ConfiguracionSistema.COMMISION_RATE_MXN;
                    break;

                case ConstantesAux.MonedaEUR:
                    parametro = ConfiguracionSistema.COMMISION_RATE_EUR;
                    break;

                case ConstantesAux.MonedaJPY:
                    parametro = ConfiguracionSistema.COMMISION_RATE_JPY;
                    break;

                default:
                    return 0;
            }

            return ConfiguracionHelper.GetDecimal(
                _configuracion.ParametrosSistema, parametro);
        }

        /// <summary>
        /// Obtiene la moneda del mensaje SWIFT a partir del XML proporcionado.
        /// </summary>
        private string ObtenerMonedaSwift(XDocument xml)
        {
            var moneda = XmlHelper.ObtenerValor(xml, ConstantesXml.mensajeMoneda);
            Config.EscribirLog($"INFO|ObtenerMonedaSwift|Se busca la moneda del mensaje SWIFT: {moneda}");
            return (moneda);
        }

        /// <summary>
        /// Obtiene la cuenta del beneficiario desde el XML del mensaje.
        /// </summary>
        private string ObtenerCuentaBeneficiario(XDocument xml)
        {
            var cuenta = Comunes.DameTexto(XmlHelper.ObtenerValor(xml, ConstantesXml.beneficiaryAccount)) ?? string.Empty;
            Config.EscribirLog($"INFO|ObtenerCuentaBeneficiario|Cuenta beneficiario: {cuenta}");
            return cuenta.Trim();
        }

        /// <summary>
        /// Normaliza la cuenta bancaria.
        /// </summary>
        private string NormalizarCuenta(string cuenta)
        {
            if (cuenta.Length == 18)
            {
                if (cuenta.StartsWith(ConstantesAux.Clave_CASFIM.Substring(2)))
                {
                    cuenta = cuenta.Substring(10, 7);
                }
            }
            return cuenta;
        }

        /// <summary>
        /// Obtiene la moneda de una cuenta en T24.
        /// </summary>
        private async Task<string> ObtenerMonedaT24Async(string cuenta, string monedaSwift)
        {
            if (string.IsNullOrWhiteSpace(cuenta))
                return string.Empty;

            try
            {
                var data = await _datosCuenta.DatosCuentaVW(cuenta);

                if (data != null && data.Rows.Count > 0)
                {
                    return _conexionT24Mw.ValidaMoneda(
                        data.Rows[0]["CURRENCY"]?.ToString());
                }

                var dataAlt = await _cuenta.ValidaDatosCuentaVT24Async(cuenta, monedaSwift);

                if (dataAlt != null && dataAlt.IsSuccess && dataAlt.Data != null)
                {
                    return _conexionT24Mw.ValidaMoneda(dataAlt.Data.CURRENCY);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerMonedaT24|Consulta cuenta T24 {ex.Message}");
            }

            return string.Empty;
        }

        /// <summary>
        /// Determina la categoría del mensaje en función de su tipo, moneda y datos asociados.
        /// </summary>
        private MessageCategory DeterminarCategoria(string clave, string monedaSwift, string monedaT24, string cuenta, XDocument xDocument)
        {
            try
            {
                switch (clave)
                {
                    case ConstantesCategoriaMT.MT103:
                    case ConstantesCategoriaMT.PACS008:

                        if (string.IsNullOrWhiteSpace(cuenta))
                            return MessageCategory.NO_CAT;

                        if (monedaSwift == ConstantesAux.MonedaMXN &&
                            cuenta.Length > 7 &&
                            !cuenta.StartsWith(ConstantesAux.Clave_CASFIM.Substring(2, 3)))
                        {
                            return MessageCategory.EXT_SPEI;
                        }

                        return monedaSwift != monedaT24
                            ? MessageCategory.INT_DIFF_CURRENCY
                            : MessageCategory.INT_EQ_CURRENCY;

                    case ConstantesCategoriaMT.MT191:
                    //Tenemos que obtener la cuenta de control del cliente puesta en T24, despues de esto comprobar que tanto 
                    //la cuenta a debitar (que es la cuenta que el cliente pide se tomen las comisiones) y la moneda a cobrar la 
                    //comision coincidan, de igual manera si la comision es pedida en MXN puede salir como SPEI. Por el momento 
                    //los mensajes se dejan en NO_CAT ya que aun no se reciben.
                    case ConstantesCategoriaMT.MT202:

                        var vostro = Comunes.DameTexto(XmlHelper.ObtenerValor(xDocument, ConstantesXml.btmumCtaVostroVostro));

                        if (string.IsNullOrWhiteSpace(vostro))
                            return MessageCategory.EXT_SPEI;

                        return monedaSwift != monedaT24
                            ? MessageCategory.INT_DIFF_CURRENCY
                            : MessageCategory.INT_EQ_CURRENCY;

                    case ConstantesCategoriaMT.MT940:
                    case ConstantesCategoriaMT.MT950:
                    case ConstantesCategoriaMT.MT998:
                        return MessageCategory.INF;

                    default:
                        return MessageCategory.NO_CAT;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|DeterminarCategoria|Categoria {ex.Message}");
                return MessageCategory.NO_CAT;
            }
        }

        /// <summary>
        /// Metodo para guardar la categoria de la operacion.
        /// </summary>
        private async Task GuardarCategoriaAsync(string icn, MessageCategory categoria)
        {
            try
            {
                await _actualizacion.ActualizarCategoriaMensajeAsync(icn, (int)categoria);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|GuardarCategoria|{ex.Message}");
            }
        }

        /// <summary>
        /// Evalúa si se debe generar una comisión para una transacción saliente,
        /// en función del BIC de la contraparte y la moneda.
        /// </summary>
        private async Task<bool> OutgoingCommissionShouldBeGeneratedAsync(string bic, string currency)
        {
            var receiverId = await GetCounterPartyIdForBicAsync(bic);

            if (receiverId <= 0)
                return true;

            return await _integracion.OutgoingCommissionRequestShouldBeGeneratedAsync(receiverId, currency);
        }

        /// <summary>
        /// Metodo para Obtener CounterPartyId.
        /// </summary>
        private async Task<long> GetCounterPartyIdForBicAsync(string bic)
        {
            try
            {
                var sanitizedBic = SanitizeBic11.SanitizeBic11Helper(bic);
                Config.EscribirLog($"INFO|GetCounterPartyIdForBic|Buscando ID para BIC: {sanitizedBic}");

                if (string.IsNullOrWhiteSpace(sanitizedBic))
                {
                    Config.EscribirLog("WARN|GetCounterPartyIdForBic|BIC inválido tras sanitización");
                    return -1;
                }

                return await _counterParty.ObtenerCounterPartyIdAsync(sanitizedBic);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|GetCounterPartyIdForBic|{ex.Message}");
                return -1;
            }
        }

        /// <summary>
        /// Metodo para aplicar reglas a la operación.
        /// </summary>
        private async Task<Result<bool>> AplicarReglasAsync(string icn, string claveMensaje)
        {
            try
            {
                Config.EscribirLog($"INFO|AplicarReglas|Aplicando reglas para el ICN: {icn}");

                var result = await _reglas.AplicarReglasAsync(icn, ConstantesServicio.TipoOperacion, claveMensaje);

                if (result?.Exito != true)
                {
                    Config.EscribirLog($"ERROR|AplicarReglas|{result?.MensajeError ?? "Resultado nulo"}");
                    return Result<bool>.Failed("Error al aplicar reglas");
                }

                return Result<bool>.Success(true);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|AplicarReglas|{ex.Message}");
                return Result<bool>.Failed("Excepción al aplicar reglas");
            }
        }

        /// <summary>
        /// Metodo que actualizara la operación como rechazo.
        /// </summary>
        private async Task<Result<bool>> RechazarOperacionAsync(string icn, ContextoXmlDTO xml)
        {
            try
            {
                Config.EscribirLog($"INFO|RechazarOperacion|ICN:{icn} Motivo:{xml.RazonRechazo}");

                int? fechaValor = null;

                if (!string.IsNullOrWhiteSpace(xml.FechaValor))
                {
                    try
                    {
                        fechaValor = ConversionesComunes.ParseEnteroRequerido(
                            xml.FechaValor,
                            ConstantesXml.fecValor);
                    }
                    catch
                    {
                        fechaValor = null;
                    }
                }

                var tipoMensaje = !string.IsNullOrWhiteSpace(xml.TipoMensaje) ? xml.TipoMensaje : xml.Clave;
                var result = await _reglas.RechazarOperacionAsync(icn, ConstantesServicio.TipoOperacion, xml.RazonRechazo, tipoMensaje, xml.Moneda, xml.Importe, fechaValor);

                if (result?.Exito != true)
                {
                    Config.EscribirLog($"ERROR|RechazarOperacion|{result?.MensajeError ?? "Resultado nulo"}");
                    return Result<bool>.Failed("Error al rechazar operación");
                }

                return Result<bool>.Success(true);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|RechazarOperacion|{ex.Message}");
                return Result<bool>.Failed("Excepción al rechazar");
            }
        }

        /// <summary>
        /// Metodo que marcara una operación si esta fue erronea.
        /// </summary>
        private async Task<Result<bool>> MarcarOperacionErrorAsync(string icn, string mensajeError)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(icn) || string.IsNullOrWhiteSpace(mensajeError))
                {
                    return Result<bool>.Failed("ICN o mensaje error inválido");
                }

                Config.EscribirLog($"INFO|MarcarOperacionError|Marcando error en ICN:{icn}");
                var result = await _reglas.MarcarOperacionErrorAsync(icn, ConstantesServicio.TipoOperacion, mensajeError);

                if (result?.Exito != true)
                {
                    Config.EscribirLog($"ERROR|MarcarOperacionError|{result?.MensajeError ?? "Resultado nulo"}");
                    return Result<bool>.Failed("Error al marcar operación");
                }

                return Result<bool>.Success(true);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|MarcarOperacionError|{ex.Message}");
                return Result<bool>.Failed("Excepción al marcar error");
            }
        }

        /// <summary>
        /// Metodo que actualizara los campos el la tabla de trabajo maestro.
        /// </summary>
        private async Task ActualizarCamposAsync(string icn, ContextoXmlDTO xml)
        {
            try
            {
                Config.EscribirLog($"INFO|ActualizarCampos|ICN:{icn}");
                await _actualizacion.ActualizarCamposAsync(icn, ConstantesServicio.TipoOperacion, xml.Clave, null, xml.CamposActualizar);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizarCampos|{ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Metodo que actualizara el estatus Spid.
        /// </summary>
        private async Task ActualizarEstatusSpidLikePaymentsAsync(string icn)
        {
            try
            {
                Config.EscribirLog($"INFO|ActualizarEstatusSpidLikePayments|Actualizando estatus SPID ICN:{icn}");
                await _actualizacion.ActualizarEstatusSpidAsync(icn, ConstantesServicio.TipoOperacion);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizarEstatusSpidLikePayments|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Obtenemos la parte del mensaje.
        /// </summary>
        private List<string> ObtenerParteMensaje(string tag, string mensaje)
        {
            try
            {
                var resultados = new List<string>();
                var tagBusqueda = $":{tag}:";
                var patronTags = @"\:[0-9]{2}?[A-Z]?\:";
                var matches = Regex.Matches(mensaje, patronTags);
                var patronFooter = @"\-(?<footer>[0-9A-Za-z\s'\(\)\+\,\-\.\/\:\?]{0,50}?)$";
                var matchFooter = Regex.Match(mensaje, patronFooter);

                int limiteFinal = matchFooter.Success
                    ? matchFooter.Index
                    : mensaje.Length;

                foreach (Match match in matches.Cast<Match>())
                {
                    if (match.Value == tagBusqueda)
                    {
                        int inicio = match.Index;
                        var siguiente = match.NextMatch();

                        int fin = siguiente.Success ? siguiente.Index : limiteFinal;

                        string bloque = mensaje.Substring(inicio, fin - inicio);
                        resultados.Add(bloque);
                    }
                }

                return resultados;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerParteMensaje|{ex}");
                throw;
            }
        }

        private async Task<bool> ValidateSpidLikePaymentsAsync(string clave_mensaje, XDocument xmlDocument, string razonRechazo,
           ResponsePACS objPACS008)
        {
            try
            {
                ConstantesAux.gbSpidLikePayments = false;
                string errorApi = "";
                bool respApi = false;
                var catalogo = await _legalEntities.ObtieneLegalEntitiesAsync();
                var datos = ObtenerDatosSpid(clave_mensaje, xmlDocument, objPACS008);

                Config.EscribirLog($"INFO|MensajesMX|SPID_Like_Payment|Currency = {datos.lsCurrency ?? ""}");
                Config.EscribirLog($"INFO|MensajesMX|SPID_Like_Payment|Banco = {datos.lsBancoEmisor ?? ""}");

                if (clave_mensaje == ConstantesCategoriaMT.MT103)
                {
                    Config.EscribirLog($"INFO|MensajesMX|SPID_Like_Payment|Nombre1 = {datos.lsNombreOrd_1 ?? ""}");
                    Config.EscribirLog($"INFO|MensajesMX|SPID|Nombre2 = {datos.lsNombreOrd_2 ?? ""}");
                }
                else
                {
                    Config.EscribirLog($"INFO|MensajesMX|SPID_Like_Payment|Nombre1 = {datos.lsNombreOrd_1 ?? ""}");
                }

                if (datos.lsCurrency == ConstantesAux.MonedaUSD && datos.lsBancoEmisor.Substring(4, 2) == "MX")
                {
                    if (clave_mensaje == ConstantesCategoriaMT.MT103)
                    {
                        if (Entidad.ValidarEntidad(datos, catalogo, clave_mensaje))
                        {
                            ConstantesAux.gbSpidLikePayments = true;
                            Config.EscribirLog("INFO|MensajesMX|SPID_Like_Payment|El pago debe ser procesado como tipo de pago SPID");
                            respApi = await _servicioSpid.ProcesarRechazoSpidAsync(datos, xmlDocument, clave_mensaje, razonRechazo, errorApi);

                            if (respApi)
                            {
                                razonRechazo = "El pago debe ser procesado como tipo de pago SPI";
                            }
                            else
                            {
                                razonRechazo = errorApi;
                            }

                            return respApi;
                        }
                    }
                    else
                    {
                        // Procesa Validar entidad de manera diferente si no es MT-103
                        if (Entidad.ValidarEntidad(datos, catalogo, clave_mensaje))
                        {
                            Config.EscribirLog("INFO|MensajesMX|SPID_Like_Payment|El pago debe ser procesado como tipo de pago SPID");
                            ConstantesAux.gbSpidLikePayments = true;
                            respApi = await _servicioSpid.ProcesarRechazoSpidAsync(datos, xmlDocument, clave_mensaje, razonRechazo, errorApi);

                            if (respApi)
                            {
                                razonRechazo = "El pago debe ser procesado como tipo de pago SPID";
                            }
                            else
                            {
                                razonRechazo = errorApi;
                            }
                            return respApi;
                        }
                    }
                }

                return respApi;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidateSpidLikePayments|{ex.Message}");
                throw;
            }
        }

        #endregion

        #region"Procesa Mensajes MX"

        /// <summary>
        /// Metodo que obtiene las operaciones MX para procesar.
        /// </summary>
        private async Task CargaMensajesMXAsync()
        {
            try
            {
                Config.EscribirLog("INFO|ProcesarMensajesMX|Buscando operaciones MX...");

                var opMX = await _operaciones.ObtieneOperacionesMXAsync(ConstantesServicio.TipoOperacion);

                if (!opMX.Any())
                {
                    Config.EscribirLog("INFO|ObtieneOperacionesMX|No hay operaciones a procesar...");
                    return;
                }

                foreach (var op in opMX)
                {
                    await ProcesarOperacionMXAsync(op);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|CargaMensajesMX|Error al obtener operaciones|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Metodo principal para procesar las operaciones MX.
        /// </summary>
        /// 
        private async Task ProcesarOperacionMXAsync(OperacionesDTO op)
        {
            ConstantesAux.gbSpidLikePayments = false;
            bool operacionAceptada = false;
            ContextoXmlDTO ctx;
            ctx = await _contextoXml.CrearContextoXmlAsync(op.NF_FOLIO, op.NF_MQ_MSG);

            try
            {
                Config.EscribirLog($"INFO|ProcesarOperacionMX|Inicio procesamiento para ICN: {ctx.ICN}");
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|*** Inicio ***");
                await ObtenerHeaderSAMHAsync(ctx);
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|*** Fin ***");

                if (ctx.PuedeInterpretarse)
                {
                    await ValidaMensajeMXAsync(ctx);

                    if (ctx.PuedeInterpretarse)
                    {
                        var uniqueKeyMX = ctx.Samh.AmhHeader.msgUniqueKey;
                        Config.EscribirLog($"INFO|Inicio análisis ICN = {uniqueKeyMX}");
                        ctx.XmlDoc = XDocument.Parse(IniciaXml(uniqueKeyMX).ToString());
                        await InterpretarSegunTipoAsync(ctx);
                    }

                    if (ctx.PuedeInterpretarse)
                    {
                        var xdocument = XDocument.Parse(ctx.VariablesXml);
                        ctx.XmlDoc = xdocument;
                        var bancoReceptor = XmlHelper.ObtenerValor(xdocument, ConstantesXml.bancoReceptor);

                        if (!string.IsNullOrWhiteSpace(bancoReceptor) &&
                            (bancoReceptor.StartsWith(ConstantesAux.BANCO_BOTKMXMX) || bancoReceptor == ConstantesAux.BANCO_37046019))
                        {
                            if (ctx.Clave == ConstantesCAMT.CAMT106 && ConfiguracionHelper.GetBool(_configuracion.ParametrosSistema, 
                                ConfiguracionSistema.COMM_MT191OUT_FEATURE_FLAG))
                            {
                                var result = await ProcesarComision(ctx);

                                if (!result)
                                {
                                    Config.EscribirLog($"INFO|ProcesarComision|RazonRechazo = {ctx.RazonRechazo}");
                                    ctx.PuedeInterpretarse = false;
                                }
                            }
                            else
                            {
                                Config.EscribirLog("INFO|ValidaDatosCore iniciado...");
                                ctx.XmlDoc = XDocument.Parse(ctx.VariablesXml);
                                var result = await ValidaDatosCoreAsync(ctx.ICN, ctx.Clave, ctx.XmlDoc, ctx.CamposActualizar, ctx);

                                if (!result)
                                {
                                    ctx.PuedeInterpretarse = false;

                                    if (string.IsNullOrWhiteSpace(ctx.RazonRechazo))
                                        ctx.RazonRechazo = "Error en validación datos core";
                                }
                                else
                                {
                                    await SetMTCategoryAsync(ctx.XmlDoc, ctx.Clave, ctx.ICN);
                                }

                                if (ctx.PuedeInterpretarse && ctx.Clave == ConstantesPACS.PACS008)
                                {
                                  ConstantesAux.validateSpidLikePaymenst = await ValidateSpidLikePaymentsAsync(ctx.Clave, ctx.XmlDoc,
                                        ctx.RazonRechazo, ctx.responsePACS);
                                }
                            }
                        }
                        else
                        {
                            ctx.RazonRechazo = "Banco receptor invalido";
                            ctx.PuedeInterpretarse = false;
                        }
                    }
                    else
                    {
                        ctx.PuedeInterpretarse = false;
                        if (ctx.RazonRechazo == "")
                        {
                            ctx.RazonRechazo = "No se pudo establecer el tipo de mensaje";
                        }
                    }

                    if (ctx.PuedeInterpretarse && !ConstantesAux.gbSpidLikePayments)
                    {
                        ctx.Folio = await _integracion.GeneraFolioUnicoAsync(ConstantesServicio.OperacionFolioUnico, string.Empty, ctx.Samh.AmhHeader.msgUniqueKey, 13);
                        Config.EscribirLog($"INFO|ProcesarOperacionMX|FolioUnico = {ctx.Folio}");

                        await _integracion.AceptaOperacionSwtAsync(ctx.ICN, ctx.Folio, ctx.Clave, ctx.XmlDoc.ToString(), ctx.CamposActualizar);
                        operacionAceptada = true;
                        await _integracion.InsertLaikaQueueAsync(ctx.ICN);

                        if (ctx.Mensaje?.Contains(ConstantesPACS.PACS00800108) == true &&
                            XmlHelper.ObtenerValor(ctx.XmlDoc, ConstantesXml.detailCharges) == ConstantesAux.OUR &&
                            ConfiguracionHelper.GetBool(_configuracion.ParametrosSistema, ConfiguracionSistema.COMM_MT191IN_FEATURE_FLAG))
                        {
                            var apiUrl = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.COMM_API_URL);
                            await EnqueueCommissionChargeRequestAsync(ctx.ICN, ctx, apiUrl);
                        }

                        await AplicarReglasAsync(ctx.ICN, ctx.Clave);
                    }
                    else if (!ConstantesAux.validateSpidLikePaymenst && !ConstantesAux.gbSpidLikePayments)
                    {
                        await RechazarOperacionAsync(ctx.ICN, ctx);
                        await MarcarOperacionErrorAsync(ctx.ICN, ctx.RazonRechazo);
                    }
                    else
                    {
                        if (ctx.CamposActualizar.ContainsKey(ConstantesXml.fecValor))
                        {
                            ctx.FechaValor = ctx.CamposActualizar[ConstantesXml.fecValor];
                        }

                        await RechazarOperacionAsync(ctx.ICN, ctx);

                        if (!ConstantesAux.gbSpidLikePayments)
                        {
                            await ActualizarEstatusSpidLikePaymentsAsync(ctx.ICN);
                        }
                    }
                    if (!ConstantesAux.gbSpidLikePayments)
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionMX|Se termina ICN {ctx.ICN}, interpretado como SPID.");
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|ProcesarOperacionMX|Se termina ICN {ctx.ICN}," +
                            $" {(ctx.PuedeInterpretarse ? "SI" : "NO")} pudo interpretarse");
                    }

                }
                else
                {
                    if (string.IsNullOrWhiteSpace(ctx.RazonRechazo))
                        ctx.RazonRechazo = "No se pudo leer el encabezado del mensaje MX";

                    await RechazarOperacionAsync(ctx.ICN, ctx);
                    Config.EscribirLog($"INFO|ProcesarOperacionMX|Se termina ICN {ctx.ICN}, NO pudo interpretarse");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarOperacionMX|{ex}");
                ctx.RazonRechazo = ex.Message;
                await MarcarOperacionErrorAsync(ctx.ICN, ex.Message);

                if (!operacionAceptada)
                    await RechazarOperacionAsync(ctx.ICN, ctx);
            }
        }

        /// <summary>
        /// Metodo que obtiene el header mediante un servicio externo.
        /// </summary>
        private async Task<bool> ObtenerHeaderSAMHAsync(ContextoXmlDTO ctx)
        {
            try
            {
                var gsUrlGetMessageHeaders_XAMH = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.SWF_GET_MESSAGE_HEADERS_XAMH);

                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|Url= {gsUrlGetMessageHeaders_XAMH}");
                var result = await _samhService.ObtenerHeaderAsync(ctx.Mensaje, gsUrlGetMessageHeaders_XAMH);

                if (result?.IsSuccess != true || result.Data?.AmhHeader == null || !result.Data.valid)
                {
                    Config.EscribirLog($"WARN|ObtenerHeaderSAMH|No se pudo obtener el header SAMH: {ctx.Folio}");
                    ctx.RazonRechazo = "No se pudo leer el header SAMH";
                    ctx.PuedeInterpretarse = false;
                    return false;
                }

                ctx.Samh = result.Data;
                ctx.PuedeInterpretarse = true;
                var header = ctx.Samh.AmhHeader;
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|msgUniqueKey:{header.msgUniqueKey ?? "NULL"}");
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|originationSysId:{header.originationSysId ?? "NULL"}");
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|destinationSysId:{header.destinationSysId ?? "NULL"}");
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|receivingService:{header.receivingService ?? "NULL"}");
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|mxTranslationFlag:{header.mxTranslationFlag ?? "NULL"}");
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|enrichFlag:{header.enrichFlag ?? "NULL"}");
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|internalRemarks:{header.internalRemarks ?? "NULL"}");
                Config.EscribirLog($"INFO|ObtenerHeaderSAMH|reservedArea:{header.reservedArea ?? "NULL"}");

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerHeaderSAMH|{ex.Message}");
                ctx.RazonRechazo = "No se pudo leer el mensaje para establecer la estructura del header SAMH.";
                ctx.PuedeInterpretarse = false;
                return false;
            }
        }

        /// <summary>
        /// Metodo que consulta un servicio externo para validar el mensaje.
        /// </summary>
        private async Task<bool> ValidaMensajeMXAsync(ContextoXmlDTO ctx)
        {
            try
            {
                Config.EscribirLog($"INFO|ValidarMensajeMX|Iniciando validación ICN: {ctx.ICN}");
                var UrlValidateMessageMX = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.SWF_VALIDATE_MESSAGE_MX);

                var result = await _samhService.ValidarMensajeAsync(ctx.Mensaje, UrlValidateMessageMX);

                if (result?.IsSuccess != true || result.Data == null)
                {
                    Config.EscribirLog($"ERROR|ValidarMensajeMX|ICN:{ctx.ICN} RazonRechazo:{ctx.RazonRechazo}");
                    ctx.RazonRechazo = result?.Reason ?? "Error validando mensaje MX";
                    ctx.PuedeInterpretarse = false;
                    return false;
                }

                if (!result.Data.valid)
                {
                    Config.EscribirLog($"WARN|ValidarMensajeMX|Header inválido ICN: {ctx.Folio}");
                    ctx.RazonRechazo = "No se pudo validar el mensaje";
                    ctx.PuedeInterpretarse = false;
                    return false;
                }

                ctx.Response_Validator_OK = result.Data;
                Config.EscribirLog($"INFO|ValidarMensajeMX|ICN:{ctx.ICN} Tipo:{ctx.Mensaje}");
                ctx.PuedeInterpretarse = true;
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidarMensajeMX|{ex}");
                ctx.RazonRechazo = "Error en validación de mensaje MX";
                ctx.PuedeInterpretarse = false;
                return false;
            }
        }

        /// <summary>
        /// Metdo que realizara la interpretación segun el tipo del mensaje.
        /// </summary>
        private async Task InterpretarSegunTipoAsync(ContextoXmlDTO ctx)
        {
            try
            {
                var tipo = ctx.Response_Validator_OK?.messageType;

                if (string.IsNullOrWhiteSpace(tipo))
                {
                    ctx.RazonRechazo = "No se obtuvo el tipo de mensaje MX.";
                    ctx.PuedeInterpretarse = false;
                    return;
                }

                Config.EscribirLog($"INFO|Tipo de mensaje a interpretar: {tipo}");

                // PACS008
                if (tipo.Contains(ConstantesPACS.PACS00800108))
                {
                    var pacs = await RequestDataPacs008(ctx);

                    if (pacs == null)
                    {
                        ctx.PuedeInterpretarse = false;
                        return;
                    }

                    ctx.PuedeInterpretarse = await _pacs008.InterpretarPacs008Async(ctx, pacs);

                    if (!ctx.PuedeInterpretarse || ctx.XmlDoc == null)
                        return;

                    ctx.Clave = ConstantesPACS.PACS008;
                    var xmlString = LimpiaCaracteresMx.CleanCharactersFromMX(ctx.XmlDoc.ToString());
                    ctx.VariablesXml = xmlString;
                    Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML = {xmlString}");
                }

                // PACS009
                else if (tipo.Contains(ConstantesPACS.PACS00900108) &&
                    !tipo.Contains(ConstantesPACS.PACS00900108COV))
                {
                    var xmlDoc = new RemoveNameSpace().RemoveNamespace(ctx.Mensaje);
                    var isCover = CoverMessage.IsCoverMessage(xmlDoc);

                    if (!isCover)
                    {
                        var pacs = await RequestDataPacs009(ctx);

                        if (pacs == null)
                        {
                            ctx.PuedeInterpretarse = false;
                            return;
                        }

                        ctx.PuedeInterpretarse = await _pacs009.InterpretarPacs009Async(ctx, pacs);

                        if (!ctx.PuedeInterpretarse || ctx.XmlDoc == null)
                            return;

                        ctx.Clave = ConstantesPACS.PACS009;
                        var xmlString = LimpiaCaracteresMx.CleanCharactersFromMX(ctx.XmlDoc.ToString());
                        ctx.VariablesXml = xmlString;
                        Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML = {xmlString}");
                    }
                    else
                    {
                        ctx.PuedeInterpretarse = false;
                        ctx.Clave = ConstantesPACS.PACS009COV;
                        var xmlString = LimpiaCaracteresMx.CleanCharactersFromMX(ctx.XmlDoc.ToString());
                        ctx.VariablesXml = xmlString;
                        ctx.RazonRechazo = "Este tipo de mensaje debe procesarse manualmente";
                        Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML PACS009COV = {xmlString}");
                    }
                }

                // PACS009COV
                else if (tipo.Contains(ConstantesPACS.PACS00900108COV))
                {
                    ctx.PuedeInterpretarse = false;
                    ctx.Clave = ConstantesPACS.PACS009COV;
                    var xmlString = LimpiaCaracteresMx.CleanCharactersFromMX(ctx.Mensaje.ToString());
                    ctx.VariablesXml = xmlString;
                    ctx.RazonRechazo = "Este tipo de mensaje debe procesarse manualmente";
                    Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML PACS009COV = {xmlString}");
                }

                // CAMT057
                else if (tipo.Contains(ConstantesCAMT.CAMT05700106))
                {
                    ctx.PuedeInterpretarse = false;
                    ctx.Clave = ConstantesCAMT.CAMT057;
                    ctx.RazonRechazo = "Este tipo de mensaje debe procesarse manualmente";
                    Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXMLcamt057 = {ctx.XmlDoc}");
                }

                // CAMT106
                else if (tipo.Contains(ConstantesCAMT.CAMT106001))
                {
                    ctx.Clave = ConstantesCAMT.CAMT106;

                    try
                    {
                        var camt = new camt_106(ctx.Mensaje, ctx.ICN);
                        Config.EscribirLog("INFO|InterpretarMensajes|Se extrajo el AppHdr y el Document correctamente, a extraer la informacion");
                        ctx.VariablesXml = LimpiaCaracteresMx.CleanCharactersFromMX(camt.GetVariablesXml(ctx));
                        ctx.PuedeInterpretarse = true;
                    }
                    catch (Exception ex)
                    {
                        ctx.PuedeInterpretarse = false;
                        ctx.RazonRechazo = ex.Message;
                    }

                    Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXMLcamt106 = {ctx.XmlDoc}");
                }

                // CAMT052
                else if (tipo.Contains(ConstantesCAMT.CAMT052001))
                {
                    ctx.Clave = ConstantesCAMT.CAMT052;

                    try
                    {
                        Config.EscribirLog("INFO|InterpretarMensajes.camt.052.001|A eliminar el Namespace del mensaje");
                        var xmlDoc = new RemoveNameSpace().RemoveNamespace(ctx.Mensaje);
                        Config.EscribirLog("INFO|InterpretarMensajes.camt.052.001|Namespace eliminado del mensaje.");
                        var camt = new camt_052(xmlDoc, ctx.ICN);
                        Config.EscribirLog("INFO|InterpretarMensajes|Se extrajo el AppHdr y el Document correctamente, a extraer la informacion.");
                        var resultadoXml = camt.ProcessMessage(ctx.VariablesXml);
                        ctx.VariablesXml = resultadoXml;
                        Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML = {resultadoXml}");
                        ctx.PuedeInterpretarse = true;
                    }
                    catch (Exception ex)
                    {
                        ctx.PuedeInterpretarse = false;
                        ctx.RazonRechazo = ex.Message;
                        Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML camt.052 = {ctx.VariablesXml}");
                    }

                    Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML camt.052 = {ctx.VariablesXml}");
                }

                // CAMT053
                else if (tipo.Contains(ConstantesCAMT.CAMT053001))
                {
                    ctx.Clave = ConstantesCAMT.CAMT053;

                    try
                    {
                        Config.EscribirLog("INFO|InterpretarMensajes.camt.053.001|A eliminar el Namespace del mensaje.");
                        var xmlDoc = new RemoveNameSpace().RemoveNamespace(ctx.Mensaje);
                        Config.EscribirLog("INFO|InterpretarMensajes.camt.053.001|Namespace eliminado del mensaje.");
                        var camt = new camt_053(xmlDoc, ctx.ICN);
                        Config.EscribirLog("INFO|InterpretarMensajes|Se extrajo el AppHdr y el Document correctamente, a extraer la informacion.");
                        ctx.VariablesXml = ctx.XmlDoc.ToString();
                        var resultadoXml = camt.ProcessMessage(ctx.VariablesXml);
                        ctx.VariablesXml = resultadoXml;
                        Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML = {resultadoXml}");
                        ctx.PuedeInterpretarse = true;
                    }
                    catch (Exception ex)
                    {
                        ctx.PuedeInterpretarse = false;
                        ctx.RazonRechazo = ex.Message;
                        Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML camt.053 = {ctx.VariablesXml}");
                    }

                    Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML camt.053 = {ctx.VariablesXml}");
                }
                else
                {
                    ctx.PuedeInterpretarse = false;
                    ctx.Clave = ConstantesCAMT.UNKNOWN;
                    ctx.RazonRechazo = "No se pudo establecer el tipo de mensaje";
                    Config.EscribirLog($"INFO|InterpretarMensajes|ICN:{ctx.ICN} debe procesarse manualmente.");
                }
            }
            catch (Exception ex)
            {
                ctx.PuedeInterpretarse = false;
                ctx.RazonRechazo = ex.Message;
                Config.EscribirLog($"ERROR|InterpretarMensajes|{ex.Message}");
            }
        }

        /// <summary>
        /// Metodo para procesar la comisión a partir de una referencia.
        /// </summary>
        private async Task<bool> ProcesarComision(ContextoXmlDTO variablesXml)
        {
            if (variablesXml?.XmlDoc?.Root == null)
            {
                if (variablesXml != null)
                {
                    variablesXml.PuedeInterpretarse = false;
                    variablesXml.RazonRechazo = "No se encontró el XML de variables para procesar la comisión.";
                }

                return false;
            }

            var moneda = XmlHelper.ObtenerValor(variablesXml.XmlDoc, ConstantesXml.mensajeMoneda);

            if (moneda == ConstantesAux.MonedaMXN)
            {
                variablesXml.PuedeInterpretarse = false;
                variablesXml.RazonRechazo = "No se procesa cobro de comisión para moneda nacional.";
                return false;
            }

            var ourRef = XmlHelper.ObtenerValor(variablesXml.XmlDoc, ConstantesXml.ourRelatedRef);
            var theirRef = XmlHelper.ObtenerValor(variablesXml.XmlDoc, ConstantesXml.theirRef);
            var enProceso = await _operaciones.ExisteComisionEnProcesoAsync(ourRef, theirRef);

            if (enProceso)
            {
                variablesXml.PuedeInterpretarse = false;
                variablesXml.RazonRechazo = $"Referencia {ourRef} ya está en proceso";
                return false;
            }

            var relatedOperationResult = await _operaciones.ObtenerOperacionPorReferenciaT24Async(ourRef);

            if (relatedOperationResult?.IsSuccess != true || relatedOperationResult.Data == null)
            {
                variablesXml.PuedeInterpretarse = false;
                variablesXml.RazonRechazo = $"No se encontró la operación relacionada {ourRef} para el cobro de comisión.";
                return false;
            }

            var op = relatedOperationResult.Data;
            var xdocument = variablesXml.XmlDoc;

            XmlHelper.AgregarElemento(xdocument, ConstantesXml.customerCdl, op.CustomerCdl);
            XmlHelper.AgregarElemento(xdocument, ConstantesXml.customerName, op.CustomerName);
            XmlHelper.AgregarElemento(xdocument, ConstantesXml.originalAccount, op.OriginalAccount);
            XmlHelper.AgregarElemento(xdocument, ConstantesXml.originalDate, op.OperationDate);

            variablesXml.CamposActualizar[ConstantesXml.nombreCliente] = op.CustomerName;
            variablesXml.CamposActualizar[ConstantesXml.numCdl] = op.CustomerCdl;
            variablesXml.CamposActualizar[ConstantesXml.numCliente] = op.CustomerCdl;
            variablesXml.CamposActualizar[ConstantesXml.cveMoneda] = XmlHelper.ObtenerValor(variablesXml.XmlDoc, ConstantesXml.mensajeMoneda);
            variablesXml.CamposActualizar[ConstantesXml.desImporte] = XmlHelper.ObtenerValor(variablesXml.XmlDoc, ConstantesXml.mensajeImporte);

            var charge = await _t24CommissionService.GetCommissionAccountAsync(op.CustomerCdl);

            if (charge?.IsSuccess != true || charge.Data == null)
            {
                variablesXml.RazonRechazo = charge?.Reason ?? "T24 no devolvió una cuenta de comisión válida.";
                return false;
            }

            XmlHelper.AgregarElemento(xdocument, ConstantesXml.chargeAccount, charge.Data.Account);
            XmlHelper.AgregarElemento(xdocument, ConstantesXml.chargeAccountCurrency, charge.Data.Currency);

            variablesXml.XmlDoc = xdocument;
            variablesXml.VariablesXml = xdocument.ToString();

            variablesXml.CamposActualizar[ConstantesXml.numCuenta] = charge.Data.Account;
            variablesXml.CamposActualizar[ConstantesXml.fecValor] = ConstantesAux.FechaValorNoInformada;

            var apiUrl =
                ConfiguracionHelper.GetString(
                    _configuracion.ParametrosSistema,
                    ConfiguracionSistema.COMM_API_URL
                );

            var enqueueResult = await _apiComission.EnqueueIncomingCommissionChargeRequestAsync(variablesXml.ICN, variablesXml.XmlDoc, apiUrl);

            if (enqueueResult?.IsSuccess != true)
            {
                variablesXml.PuedeInterpretarse = false;
                variablesXml.RazonRechazo = enqueueResult?.Reason ?? "No fue posible encolar el cobro de comisión.";
                return false;
            }

            return true;
        }

        /// <summary>
        /// Consume el servicio configurado para obtener los datos de un mensaje PACS008
        /// a partir del contenido XML proporcionado en el contexto.
        /// </summary>
        private async Task<ResponsePACS> RequestDataPacs008(ContextoXmlDTO ctx)
        {
            try
            {
                var apiUrl = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.SWF_GETPACS008DATA);

                Config.EscribirLog($"INFO|URL servicio que obtiene datos del PACS008|{apiUrl}");

                var result = await _pacsService.GetPacs008DataAsync(ctx.Mensaje, apiUrl);

                if (result?.IsSuccess != true || result.Data == null)
                {
                    ctx.RazonRechazo = result?.Reason ?? "Error al obtener PACS008";
                    Config.EscribirLog($"WARN|RequestDataPacs008|ICN:{ctx.ICN}|{ctx.RazonRechazo}");
                    return null;
                }

                return result.Data;
            }
            catch (Exception ex)
            {
                ctx.RazonRechazo = "Error interno PACS008";
                Config.EscribirLog($"ERROR|RequestDataPacs008|{ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Consume el servicio configurado para obtener los datos de un mensaje PACS009
        /// a partir del contenido XML proporcionado en el contexto.
        /// </summary>
        private async Task<ResponsePACS> RequestDataPacs009(ContextoXmlDTO ctx)
        {
            try
            {
                var apiUrl = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.SWF_GETPACS009DATA);

                Config.EscribirLog($"INFO|URL servicio que obtiene datos del PACS009|{apiUrl}");

                var result = await _pacsService.GetPacs009DataAsync(ctx.Mensaje, apiUrl);

                if (result?.IsSuccess != true || result.Data == null)
                {
                    ctx.RazonRechazo = result?.Reason ?? "Error al obtener PACS009";
                    Config.EscribirLog($"WARN|RequestDataPacs009|ICN:{ctx.ICN}|{ctx.RazonRechazo}");
                    return null;
                }

                return result.Data;
            }
            catch (Exception ex)
            {
                ctx.RazonRechazo = "Error interno PACS009";
                Config.EscribirLog($"ERROR|RequestDataPacs009|ICN:{ctx.ICN}|{ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Metodo que obtiene los datos SPID.
        /// </summary>
        private SpidDTO ObtenerDatosSpid(string clave_mensaje, XDocument xDocument, ResponsePACS objPACS008)
        {
            var datos = new SpidDTO { lsCurrency = XmlHelper.ObtenerValor(xDocument, ConstantesXml.mensajeMoneda) };

            if (clave_mensaje == ConstantesCategoriaMT.MT103)
            {
                datos.lsBancoEmisor = XmlHelper.ObtenerValor(xDocument, ConstantesXml.CIFSender);
                datos.lsNombreOrd_1 = XmlHelper.ObtenerValor(xDocument, ConstantesXml.orderingNameAddr1);
                datos.lsNombreOrd_2 = XmlHelper.ObtenerValor(xDocument, ConstantesXml.orderingNameAddr2);
            }
            else
            {
                datos.lsBancoEmisor = objPACS008?.appHdr?.frBICFI;
                datos.lsNombreOrd_1 = objPACS008?.dbtr?.nm;
            }

            return datos;
        }

        #endregion
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using static Aplicacion_IN_ETL.Utilidades.Xml.XmlUtils;

namespace Aplicacion_IN_ETL.Servicios
{
    /// <summary>
    /// Servicio encargado de gestionar la comunicacion con los servicios SPID
    /// requeridos durante el procesamiento de la operacion.
    /// </summary>
    public class ServicioSpid : IServicioSpid
    {

        private readonly IRequestApiRejection _requestApiRejection;

        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="ServicioSpid"/>
        /// </summary>
        /// <param name="requestApiRejection">Componente responsable de la invocacion de los servicios de rechazo de operaciones SPID</param>
        public ServicioSpid(IRequestApiRejection requestApiRejection)
        {
            _requestApiRejection = requestApiRejection;
        }

        /// <summary>
        /// Procesa el rechazo de una operación que debe ser gestionada como SPID.
        /// Obtiene información relevante desde el XML (ICN, referencia e importe)
        /// y realiza la llamada a un API externo para registrar el rechazo.
        ///
        /// En caso de éxito, retorna un resultado exitoso con valor <c>true</c>.
        /// En caso de fallo, retorna un resultado fallido con el motivo correspondiente.
        /// </summary>
        /// <param name="datos">Objeto que contiene información del banco emisor y moneda.</param>
        /// <param name="xDocument">Documento XML que contiene los datos de la operación.</param>
        /// <param name="claveMensaje">Clave del mensaje asociada a la operación.</param>
        /// <param name="razonRechazo">Motivo del rechazo de la operación.</param>
        /// <returns>
        public async Task<bool> ProcesarRechazoSpidAsync(SpidDTO datos, XDocument xDocument, string claveMensaje, string razonRechazo, string errorApi)
        {
            try
            {
                Config.EscribirLog("INFO|MX|SPID_Like_Payment|El pago debe procesarse como SPID");

                var icn = XmlHelper.ObtenerValor(xDocument, "ICN");
                var reference = XmlHelper.ObtenerValor(xDocument, "sender_reference");
                var importe = XmlHelper.ObtenerValor(xDocument, "mensaje_importe");

                var respApi = await _requestApiRejection.RequestApiRejectionAsync
                   (
                    icn,
                    reference,
                    datos.lsBancoEmisor,
                    claveMensaje,
                    datos.lsCurrency,
                    importe,
                    "THE PAYMENT IS FROM A MEXICAN BANK AND MUST BE PROCESSED THROUGH SPID",
                    errorApi
                  );

                if (!respApi)
                {
                    return false;
                }

                return true;

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarRechazoSpid|{ex}");
                throw;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Constantes
{
    /// <summary>
    /// Constantes auxiliares
    /// </summary>
    public class ConstantesAux
    {
        public const string Clave_CASFIM = "40108";
        public const string MonedaUSD = "USD";
        public const string MonedaMXN = "MXN";
        public const string MonedaEUR = "EUR";
        public const string MonedaJPY = "JPY";
        public const string BANCO_BOTKMXMX = "BOTKMXMX";
        public const string BANCO_37046019 = "37046019";
        public const string OURC = "OURC";
        public const string BEN = "BEN";
        public const string OUR = "OUR";
        public const string SHA = "SHA";
        public const string CRED = "CRED";
        public const string DEBT = "DEBT";
        public const string SHAR = "SHAR";
        public const string ClaveAvisoCritico = "CRITICO";
        public const string ClaveAvisoError = "ERROR";
        public const string IndicadorSi = "yes";
        public const string CodigoSinBloqueo = "0";
        public const string FechaValorNoInformada = "0";
        public static bool gbSpidLikePayments = false;
        public static bool validateSpidLikePaymenst;
    }
}

using Aplicacion_IN_ETL.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Aplicacion_IN_ETL.DTO.Contexto
{
    /// <summary>
    /// Recuperación de datos
    /// </summary>
    public class ContextoXmlDTO
    {
        public string ICN { get; set; }
        public string Mensaje { get; set; }
        public string TipoMensaje { get; set; }
        public string Folio { get; set; }
        public string VariablesXml { get; set; }
        public string Clave { get; set; }
        public string Moneda { get; set; }
        public decimal Importe { get; set; }
        public string FechaValor { get; set; }
        public bool PuedeInterpretarse { get; set; }
        public string RazonRechazo { get; set; }
        public bool TagsValidos { get; set; }
        public bool InterpretacionInicial { get; set; }
        public Dictionary<string, string> CamposActualizar { get; set; }
        public XDocument XmlDoc { get; set; }
        public SwiftISO20022.SAMH Samh { get; set; }
        public SwiftISO20022.Response_Validator_OK Response_Validator_OK { get; set; }
        public SwiftISO20022.ResponsePACS responsePACS { get; set; }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Contexto;
using Aplicacion_IN_ETL.Enums;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Utilidades
{
    /// <summary>
    /// Contiene metodos para poder asignar los valores dependiendo del case.
    /// </summary>
    public class ProcesaCampo
    {
        private readonly ConexionT24Mw _conexionT24Mw;

        /// <summary>
        /// Inicializa una nueva instancia para <see cref="ProcesaCampo"/>
        /// </summary>
        /// <param name="conexionT24Mw"></param>
        public ProcesaCampo (ConexionT24Mw conexionT24Mw)
        {
            _conexionT24Mw = conexionT24Mw;
        }

        /// <summary>
        /// Procesamos los campos para actualizar con un valor mandatorio u opcional.
        /// </summary>
        public async Task ProcesarCampoAsync(
            string campo, string valor, string estatus, Dictionary<string, string> camposActualizar, ValidaDatosCoreContext ctx)
        {
            switch (campo)
            {
                case "num_cuenta":
                    ctx.CuentaSwift = valor;
                    if (!string.IsNullOrEmpty(ctx.CuentaSwift))
                    {
                        if (ctx.CuentaSwift.Length == 18)
                        {
                            var prefijo =
                                !string.IsNullOrEmpty(ConstantesAux.Clave_CASFIM) && ConstantesAux.Clave_CASFIM.Length >= 3
                                ? ConstantesAux.Clave_CASFIM.Substring(2)
                                : ConstantesAux.Clave_CASFIM;

                            if (!string.IsNullOrEmpty(prefijo) && ctx.CuentaSwift.StartsWith(prefijo))
                            {
                                ctx.CuentaSwift = ctx.CuentaSwift.Substring(10, 7);
                                valor = ctx.CuentaSwift;
                            }
                        }
                    }

                    ctx.CuentaMandatoria = estatus == ConstantesEstatusCampo.Mandatorio ? true :
                                           estatus == ConstantesEstatusCampo.Opcional ? false :
                                           ctx.CuentaMandatoria;
                    break;

                case "fec_valor":
                    ctx.FechaSwift = valor;
                    ctx.FechaMandatoria = estatus == ConstantesEstatusCampo.Mandatorio ? true :
                                          estatus == ConstantesEstatusCampo.Opcional ? false :
                                          ctx.FechaMandatoria;
                    break;

                case "cve_moneda":
                    ctx.MonedaSwift = _conexionT24Mw?.ValidaMoneda(valor)?? valor;
                    ctx.MonedaMandatoria = estatus == ConstantesEstatusCampo.Mandatorio ? true :
                                           estatus == ConstantesEstatusCampo.Opcional ? false :
                                           ctx.MonedaMandatoria;
                    break;

                case "des_importe":
                    ctx.ImporteSwift = valor;
                    ctx.ImporteMandatoria = estatus == ConstantesEstatusCampo.Mandatorio ? true :
                                            estatus == ConstantesEstatusCampo.Opcional ? false :
                                            ctx.ImporteMandatoria;
                    break;

                case "num_cif_emisor":
                    ctx.CIFSender = valor;
                    ctx.CIFEmisorMandatorio = estatus == ConstantesEstatusCampo.Mandatorio ? true :
                                              estatus == ConstantesEstatusCampo.Opcional ? false :
                                              ctx.CIFEmisorMandatorio;
                    break;

                case "num_cif_spei":
                    ctx.CIFSpei = valor;
                    ctx.CIFSpeiMandatorio = estatus == ConstantesEstatusCampo.Mandatorio ? true :
                                            estatus == ConstantesEstatusCampo.Opcional ? false :
                                            ctx.CIFSpeiMandatorio;
                    break;

                case "num_cif_spei_2":
                    ctx.CIFSpei2 = valor;
                    ctx.CIFSpeiMandatorio = estatus == ConstantesEstatusCampo.Mandatorio ? true :
                                            estatus == ConstantesEstatusCampo.Opcional ? false :
                                            ctx.CIFSpeiMandatorio;
                    break;

                case "num_cif_vostro":
                    ctx.CIFVostro = valor;
                    ctx.CIFVostroMandatorio = estatus == ConstantesEstatusCampo.Mandatorio ? true :
                                              estatus == ConstantesEstatusCampo.Opcional ? false :
                                              ctx.CIFVostroMandatorio;
                    break;

            }

            if (!string.IsNullOrWhiteSpace(valor))
            {
                camposActualizar[campo] = valor;
            }
        }
    }
}

using Aplicacion_IN_ETL.DTO.Contexto;
using MUFG_T24.Comunes;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Aplicacion_IN_ETL.Utilidades
{
    /// <summary>
    /// Proporciona metodos para aplicar conversiones de formato a la fecha o importe
    /// </summary>
    public class Conversiones
    {
        /// <summary>
        /// Aplicamos conversiones para fechas e importe.
        /// </summary>
        public static string AplicaConversiones(string valor, IEnumerable<string> funciones, ValidaDatosCoreContext dtc)
        {
            try
            {
                if (funciones == null)
                    return valor;

                if (funciones.Contains("convertir_fecha") && !string.IsNullOrEmpty(valor))
                {
                    valor = "20" + valor;

                    if (!DateTime.TryParseExact(
                            valor,
                            "yyyyMMdd",
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.None,
                            out _))
                    {
                        dtc.MensajeErroneo = true;
                        dtc.RazonRechazo = $"Fecha inválida '{valor}' (no es yyyyMMdd válida)";
                    }
                }

                if (funciones.Contains("convertir_importe"))
                {
                    valor = valor.Replace(",", CultureInfo.InvariantCulture.NumberFormat.NumberDecimalSeparator);
                }

                return valor;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaDatosCore|AplicaConversiones {ex}");
                throw;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Constantes
{
    /// <summary>
    /// Constantes Parametros Sistemaa
    /// </summary>
    public static class ConstantesParametrosSistema
    {
        public const string ParametrosSistema =

            "'HORA_OP_INICIO', " +
            "'HORA_OP_FIN', " +
            "'SWF_CRITERIO_COMPLETO', " +
            "'SWF_GET_MESSAGE_HEADERS_XAMH', " +
            "'SWF_VALIDATE_MESSAGE_MX'," +
            "'SWF_GETPACS008DATA', " +
            "'SWF_GETPACS009DATA', " +
            "'SWIFT_RESTRICCION_DEBIT_LIST', " +
            "'WST24_MW_COMPANY', " +
            "'WST24_MW_USERNAME', " +
            "'MANDATORY_T24_ACCT_RESTRICT_CHECK', " +
            "'MANDATORY_T24_SERVICE_ACCT_RESTRICT_CHECK', " +
            "'API_REJECTION_TOKEN_URL', " +
            "'API_REJECTION_API_URL', " +
            "'API_REJECTION_USER', " +
            "'COMMISION_RATE_USD', " +
            "'COMMISION_RATE_EUR', " +
            "'COMMISION_RATE_JPY', " +
            "'COMMISION_RATE_MXN', " +
            "'COMM_API_URL', " +
            "'COMM_MT191IN_FEATURE_FLAG', " +
            "'COMM_MT191OUT_FEATURE_FLAG'," +
            "'WST24_MW_PASSWORD'," +
            "'API_REJECTION_PASSWORD'";

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Enums
{
    /// <summary>
    /// Enums configuración sistema
    /// </summary>
    public enum ConfiguracionSistema
    {
        HORA_OP_INICIO,
        HORA_OP_FIN,
        SWF_CRITERIO_COMPLETO,
        SWF_GET_MESSAGE_HEADERS_XAMH,
        SWF_VALIDATE_MESSAGE_MX,
        SWF_GETPACS008DATA,
        SWF_GETPACS009DATA,
        SWIFT_RESTRICCION_DEBIT_LIST,
        WST24_MW_COMPANY,
        WST24_MW_USERNAME,
        WST24_MW_PASSWORD,
        MANDATORY_T24_ACCT_RESTRICT_CHECK,
        MANDATORY_T24_SERVICE_ACCT_RESTRICT_CHECK,
        API_REJECTION_TOKEN_URL,
        API_REJECTION_API_URL,
        API_REJECTION_USER,
        API_REJECTION_PASSWORD,
        COMMISION_RATE_USD,
        COMMISION_RATE_EUR,
        COMMISION_RATE_JPY,
        COMMISION_RATE_MXN,
        COMM_API_URL,
        COMM_MT191IN_FEATURE_FLAG,
        COMM_MT191OUT_FEATURE_FLAG
    }
}

using Aplicacion_IN_ETL.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Utilidades.Parametros
{
    /// <summary>
    /// Proporciona utilidades para obtener y convertir parametros de configuracion utilizados por la aplicacion
    /// </summary>
    public static class ParametrosHelper
    {
        /// <summary>
        /// Contiene los metodos para recuperar valores de configuracion del sistema en direccion de tipo de datos.
        /// </summary>
        public static class ConfiguracionHelper
        {
            /// <summary>
            /// Obtiene el valor nde un aparametro de configuracion como cadena.
            /// </summary>
            /// <param name="parametros">Coleccion de los parametros</param>
            /// <param name="key">Clave del parametro</param>
            /// <param name="defaultValue">Valor default.</param>
            /// <returns></returns>
            public static string GetString(
                Dictionary<string, string> parametros,
                ConfiguracionSistema key,
                string defaultValue = "")
            {
                return parametros.TryGetValue(key.ToString(), out var value)
                    ? value
                    : defaultValue;
            }

            /// <summary>
            /// Obtiene el valor de un parametro de configuracion y lo convierte a un valor booleano.
            /// </summary>
            /// <param name="parametros">Coleccion de parametros</param>
            /// <param name="key">Clave del parametro a consultar</param>
            /// <param name="defaultValue">Valor default</param>
            /// <returns></returns>
            public static bool GetBool(Dictionary<string, string> parametros, ConfiguracionSistema key, bool defaultValue = false)
            {
                if (!parametros.TryGetValue(key.ToString(), out var value))
                    return defaultValue;

                if (string.IsNullOrWhiteSpace(value))
                    return defaultValue;

                value = value.Trim();
                if (value == "1") return true;
                if (value == "0") return false;

                if (bool.TryParse(value, out var result))
                    return result;

                return defaultValue;
            }

            public static decimal GetDecimal(Dictionary<string, string> parametros, ConfiguracionSistema key, decimal defaultValue = 0)
            {
                if (!parametros.TryGetValue(key.ToString(), out var value))
                    return defaultValue;
                if (string.IsNullOrWhiteSpace(value))
                    return defaultValue;

                value = value.Trim();

                return decimal.TryParse(value, out var result)
                    ? result
                    : defaultValue;
            }
        }
    }
}

using System;
using System.Collections.Generic;

namespace Aplicacion_IN_ETL.Modelos
{
    /// <summary>
    /// Representa el resultado estandar de una operacion,
    /// indicando si fue exitosa, los datos obtenidos o el motivo del error.
    /// </summary>
    public class Result<T>
    {
        public bool IsSuccess { get; set; }

        public string Reason { get; set; } = string.Empty;

        public T Data { get; set; }

        public static Result<T> Success(T data)
        {
            return new Result<T>
            {
                IsSuccess = true,
                Data = data
            };
        }

        /// <summary>
        /// Crea una respuesta fallida con el error del mensaje especificado.
        /// </summary>
        public static Result<T> Failed(string message)
        {
            return new Result<T>
            {
                IsSuccess = false,
                Reason = message
            };
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Contexto;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Utilidades.Fecha;
using Aplicacion_IN_ETL.Utilidades.Gets;
using Aplicacion_IN_ETL.Utilidades.Texto;
using Aplicacion_IN_ETL.Utilidades.Xml;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using static Aplicacion_IN_ETL.Modelos.SwiftISO20022;


namespace Aplicacion_IN_ETL.Servicios.Interpretacion
{
    /// <summary>
    /// Interpreta el mensaje y construye el XML
    /// </summary>
    public class PACS008MX : IPacs008
    {
        private readonly ICuentasNostro _cuentasNostro;
        private readonly IAccountDebit _accountDebit;
        private readonly IAlerta _alerta;
        private readonly ConexionT24Mw _conexionT24Mw;

        /// <summary>
        /// Inicia la nueva instancia de la clase <see cref="PACS008MX"/>
        /// </summary>
        public PACS008MX(ICuentasNostro nostroCache, IAccountDebit accountDebit, IAlerta alerta, ConexionT24Mw conexionT24Mw)
        {
            _cuentasNostro = nostroCache;
            _accountDebit = accountDebit;
            _alerta = alerta;
            _conexionT24Mw = conexionT24Mw;
        }

        /// <summary>
        /// Interpreta un mensaje PACS.008 y mapea su información hacia un objeto <see cref="ContextoXmlDTO"/>,
        /// construyendo un XML interno con los datos relevantes de la transacción.
        /// Incluye el procesamiento de encabezado, montos, cuentas, corresponsales, beneficiario,
        /// instrucciones de operación, cargos y resolución de cuenta NOSTRO.
        /// </summary>
        /// <param name="ctx">Contexto que contiene el mensaje original y donde se almacenará el XML generado y los datos procesados.</param>
        /// <param name="pacs">Objeto deserializado del mensaje PACS.008 con la información de la transacción.</param>
        /// <returns>
        /// <c>true</c> si la interpretación y el mapeo se realizaron correctamente; 
        /// <c>false</c> si ocurre algún error o si faltan datos obligatorios.
        /// </returns>
        public async Task<bool> InterpretarPacs008Async(ContextoXmlDTO ctx, ResponsePACS pacs)
        {
            try
            {
                var limpio = XmlUtils.NormalizarXml(ctx.Mensaje);
                var xmlInput = XmlUtils.RemoveAllNamespaces(limpio);
                int totalLineasDir = 0;
                string lsDireccionConcat = string.Empty;
                string nodoXml;

                ctx.XmlDoc = new XDocument(
                    new XElement("row",
                        new XAttribute("id", ctx.ICN),
                        new XAttribute(XNamespace.Xml + "space", "preserve")
                    )
                );


                string currency = "";
                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.ICN, ctx.ICN);

                //Banco Emisor
                if (!string.IsNullOrWhiteSpace(pacs.appHdr.frBICFI))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.CIFSender, "NSWF" + pacs.appHdr.frBICFI);

                //Banco Receptor
                if (!string.IsNullOrWhiteSpace(pacs.instgAgtBICFI))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.bancoEmisor, pacs.instgAgtBICFI);

                //Banco Receptor
                if (!string.IsNullOrWhiteSpace(pacs.instdAgtBICFI))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.bancoReceptor, pacs.instdAgtBICFI);

                //UETR
                if (!string.IsNullOrWhiteSpace(pacs.pmtId.uetr))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.UETR, pacs.pmtId.uetr);

                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.mensajeTipo, ConstantesPACS.PACS008);

                //Sender's Reference
                if (!string.IsNullOrWhiteSpace(pacs.pmtId.instrId))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.senderReference, pacs.pmtId.instrId);

                //Bank Operation Code
                if (!string.IsNullOrWhiteSpace(pacs.pmtTpInfPrtry))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.bankOpCode, pacs.pmtTpInfPrtry);

                //Instruction Code
                if (!string.IsNullOrWhiteSpace(pacs.instrForCdtrAgtCd))
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.instructionCode, pacs.instrForCdtrAgtCd);
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.instructionAddInfo, pacs.instrForCdtrAgtCd);
                }

                //Transaction Type Code
                if (!string.IsNullOrWhiteSpace(pacs.purpPrtry))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.transTypeCode, pacs.purpPrtry);

                if (!string.IsNullOrWhiteSpace(pacs.intrBkSttlmDt))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.settledDate, ConvertirFecha.ConvierteFecha(pacs.intrBkSttlmDt));

                //Moneda 
                if (!string.IsNullOrWhiteSpace(pacs.intrBkSttlmAmtCcy))
                {
                    currency = pacs.intrBkSttlmAmtCcy;
                    ctx.Moneda = currency;

                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.mensajeMoneda, currency);
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.settledCcy, currency);
                }

                //Importe (Mandatory)
                if (string.IsNullOrWhiteSpace(pacs.intrBkSttlmAmt))
                {
                    ctx.RazonRechazo = "No se pudo leer el mensaje para establecer el importe.";
                    return false;
                }

                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.mensajeImporte, pacs.intrBkSttlmAmt);
                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.settledAmount, pacs.intrBkSttlmAmt);

                //Moneda Ordenado  
                if (!string.IsNullOrWhiteSpace(pacs.instdAmtCcy))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.instructedCcy, pacs.instdAmtCcy);

                //Importe Ordenado
                if (!string.IsNullOrWhiteSpace(pacs.instdAmt))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.instructedAmount, pacs.instdAmt);

                //Tipo de Cambio
                if (!string.IsNullOrWhiteSpace(pacs.xchgRate))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.exRate, pacs.xchgRate);

                //Debtor Account
                if (!string.IsNullOrWhiteSpace(pacs.dbtr.acctIdOthrId))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderingAccount, pacs.dbtr.acctIdOthrId);

                //Settlement 
                string settlementMethod = GetValuePath.GetValueByXPath(xmlInput, ConstantesXmlPath.SttlmMtdPacs08);
                if (!string.IsNullOrWhiteSpace(settlementMethod))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.settlementMethod, settlementMethod);

                //Debtor Name
                List<string> laMultivalorMX = new List<string>();
                List<string> laMultivalorMXAux = new List<string>();

                if (!string.IsNullOrWhiteSpace(pacs.dbtr.nm))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderingName, pacs.dbtr.nm);

                int totalLineasNameOrd = 0;
                laMultivalorMX = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(pacs.dbtr.nm, 35, ref totalLineasNameOrd);

                Config.EscribirLog($"INFO|InterpretaMensajes|totalLineasNameOrd = {totalLineasNameOrd}");
                Config.EscribirLog($"INFO|InterpretaMensajes|laMultivalorMX.Count = {laMultivalorMX.Count}");


                if (totalLineasNameOrd > 2)
                {
                    laMultivalorMX = CortarTexto.RegresaNLineasDeMultivalor(laMultivalorMX, 2, ref laMultivalorMXAux);

                    if (laMultivalorMXAux.Count > 2)
                    {
                        XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderingCustNmExt, null,
                            CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(string.Join("", laMultivalorMXAux), 65, ref totalLineasNameOrd));
                    }
                }

                //Debtor address
                if (pacs.dbtr.pstlAdr != null)
                {
                    totalLineasNameOrd = laMultivalorMX.Count;

                    if (pacs.dbtr.pstlAdr.adrLine.Count > 0)
                    {
                        nodoXml = string.Join(" ", pacs.dbtr.pstlAdr.adrLine);
                        Config.EscribirLog($"INFO|interpreta_mensajes|Direccion Completa = {nodoXml}");

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.ctry))
                        {
                            nodoXml = string.Join(" ", pacs.dbtr.pstlAdr.ctry);
                        }

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.twnNm))
                        {
                            nodoXml = string.Join(" ", pacs.dbtr.pstlAdr.twnNm);
                        }

                        laMultivalorMXAux = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(nodoXml, 35, ref totalLineasDir);
                        Config.EscribirLog($"INFO|interpreta_mensajes|Lineas de la direccion del ordenate = {laMultivalorMXAux.Count}");
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.strtNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.strtNm);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.bldgNb))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.bldgNb);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.bldgNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.bldgNm);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.flr))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.flr);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.pstBx))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.pstBx);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.room))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.room);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.dept))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.dept);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.subDept))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.subDept);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.ctry))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.ctry);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.twnNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.twnNm);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.pstCd))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.pstCd);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.ctrySubDvsn))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.ctrySubDvsn);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.twnLctnNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.twnLctnNm);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.dstrctNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.dstrctNm);

                        if (!string.IsNullOrWhiteSpace(lsDireccionConcat))
                        {
                            laMultivalorMXAux = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(lsDireccionConcat, 35, ref totalLineasDir);
                        }
                    }

                    if (laMultivalorMX.Count == 2)
                    {
                        Config.EscribirLog($"INFO|interpreta_mensajes|Array principal con 2 o mas lineas = {laMultivalorMX.Count}");
                        laMultivalorMX.AddRange(laMultivalorMXAux.GetRange(0, 1));
                    }
                    else if (laMultivalorMXAux.Count == 1)
                    {
                        Config.EscribirLog($"INFO|interpreta_mensajes|Array principal con 1 o menos lineas =  {laMultivalorMX.Count}");
                        laMultivalorMX.AddRange(laMultivalorMXAux.GetRange(0, 1));
                    }
                    else
                    {
                        laMultivalorMX.AddRange(laMultivalorMXAux.GetRange(0, 2));
                    }
                }

                if (totalLineasDir > (3 - totalLineasNameOrd))
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderingCustAdrExt, null,
                        CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(string.Join(" ", laMultivalorMXAux.GetRange((3 - totalLineasNameOrd), totalLineasDir - (3 - totalLineasNameOrd))), 65, ref totalLineasNameOrd));
                }

                if (laMultivalorMX.Count > 0)
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderingNameAddr, null, laMultivalorMX);
                }

                if (!string.IsNullOrWhiteSpace(pacs.dbtrAgt.nm))
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderInstName, pacs.dbtrAgt.nm);
                    laMultivalorMX.Add(pacs.dbtrAgt.nm);
                }

                if (pacs.dbtrAgt.pstlAdr != null)
                {
                    if (pacs.dbtrAgt.pstlAdr.adrLine.Count > 0)
                    {
                        laMultivalorMX.AddRange(pacs.dbtrAgt.pstlAdr.adrLine);
                    }
                }

                if (laMultivalorMX.Count > 0)
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderInstNameAddr, null, laMultivalorMX);
                }

                //Ordering Instruction
                if (!string.IsNullOrWhiteSpace(pacs.dbtrAgt.nm))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderInstParty, pacs.dbtrAgt.nm);

                //Ordering Institution BIC
                string dbtrAgentBic = GetValuePath.GetValueByXPath(xmlInput, ConstantesXmlPath.FIToBICFI);
                if (!string.IsNullOrWhiteSpace(dbtrAgentBic))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.orderiInstBic, dbtrAgentBic);

                //Creditor Account 
                if (!string.IsNullOrWhiteSpace(pacs.cdtr.acctIdOthrId))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.beneficiaryAccount, pacs.cdtr.acctIdOthrId);

                //Our Correspondent 
                XmlUtils.AgregaElemento(ctx, ConstantesXml.recCorrBic, XmlUtils.GetBicFromNode(xmlInput, ConstantesXml.InstdRmbrsmntAgt));
                XmlUtils.AgregaElemento(ctx, ConstantesXml.sendCorrBic, XmlUtils.GetBicFromNode(xmlInput, ConstantesXml.InstgRmbrsmntAgt));

                string intermediaryAgent = GetValuePath.GetValueByXPath(xmlInput, ConstantesXmlPath.IntermediaryAgent);
                if (!string.IsNullOrWhiteSpace(intermediaryAgent))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.intCif, intermediaryAgent);

                //Creditor Name
                laMultivalorMX = new List<string>();
                laMultivalorMXAux = new List<string>();
                List<string> laMultivalorMXBen = new List<string>();
                totalLineasDir = 0;
                int totalLineasNameBen = 0;

                if (!string.IsNullOrWhiteSpace(pacs.cdtr.nm))
                {
                    laMultivalorMX = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(pacs.cdtr.nm, 35, ref totalLineasNameBen);

                    if (totalLineasNameBen > 2)
                    {
                        laMultivalorMX = CortarTexto.RegresaNLineasDeMultivalor(laMultivalorMX, 2, ref laMultivalorMXAux);
                        if (laMultivalorMXAux.Count > 0)
                        {
                            Config.EscribirLog($"INFO|interpreta_mensajes|B|laMultivalorMXAux.Count = {laMultivalorMXAux.Count}");
                            laMultivalorMXBen = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(string.Join(" ", laMultivalorMXAux), 65, ref totalLineasNameBen);
                        }
                    }
                }

                //Creditor address
                if (pacs.cdtr.pstlAdr != null)
                {
                    totalLineasNameBen = laMultivalorMX.Count;
                    Config.EscribirLog($"INFO|interpreta_mensajes|totalLineasNameBen = {totalLineasNameBen}");

                    if (pacs.cdtr.pstlAdr.adrLine.Count > 0)
                    {
                        nodoXml = string.Join(" ", pacs.cdtr.pstlAdr.adrLine);

                        //Hybrid
                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.pstlAdr.ctry))
                            nodoXml = string.Concat(nodoXml, " ", pacs.cdtr.pstlAdr.ctry);

                        //TownName debe ser requerido
                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.pstlAdr.twnNm))
                            nodoXml = string.Concat(nodoXml, " ", pacs.cdtr.pstlAdr.twnNm);

                        laMultivalorMXAux = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(nodoXml, 35, ref totalLineasDir);
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.strtNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.strtNm);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.bldgNb))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.bldgNb);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.bldgNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.bldgNm);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.flr))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.flr);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.pstBx))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.pstBx);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.room))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.room);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.dept))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.dept);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.subDept))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.subDept);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.ctry))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.ctry);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.twnNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.twnNm);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.pstCd))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.pstCd);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.ctrySubDvsn))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.ctrySubDvsn);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.twnLctnNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.twnLctnNm);

                        if (!string.IsNullOrWhiteSpace(pacs.dbtr.pstlAdr.dstrctNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.dbtr.pstlAdr.dstrctNm);
                    }
                    if (!string.IsNullOrWhiteSpace(lsDireccionConcat))
                        laMultivalorMXAux = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(lsDireccionConcat, 35, ref totalLineasDir);
                }

                if (laMultivalorMX.Count == 2)
                {
                    Config.EscribirLog($"INFO|interpreta_mensajes|Array principal con 2 o mas lineas = {laMultivalorMX.Count}");
                    laMultivalorMX.AddRange(laMultivalorMXAux.GetRange(0, 1));
                }
                else if (laMultivalorMX.Count == 1)
                {
                    if (laMultivalorMXAux.Count > 0)
                    {
                        laMultivalorMX.AddRange(laMultivalorMXAux.GetRange(0, laMultivalorMXAux.Count));
                    }
                }

                if (totalLineasDir > (3 - totalLineasNameBen))
                {
                    laMultivalorMXBen.AddRange(CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(string.Join(" ", laMultivalorMXAux.GetRange((3 - totalLineasNameBen), totalLineasDir - (3 - totalLineasNameBen))), 65, ref totalLineasNameBen));
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.beneficiaryNameExt, null, laMultivalorMXBen);
                }
                else if (laMultivalorMXBen.Count > 0)
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.beneficiaryNameExt, null, laMultivalorMXBen);
                }

                if (laMultivalorMX.Count > 0)
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.beneficiaryNameExt, null, laMultivalorMX);
                }

                if (!string.IsNullOrWhiteSpace(pacs.rmtInfUstrd))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.remmitanceInformation, pacs.rmtInfUstrd);

                if (!string.IsNullOrWhiteSpace(pacs.chrgBr))
                {
                    switch (pacs.chrgBr)
                    {
                        case ConstantesAux.CRED: XmlUtils.AgregaElemento(ctx, ConstantesXml.detailCharges, ConstantesAux.BEN); break;
                        case ConstantesAux.DEBT: XmlUtils.AgregaElemento(ctx, ConstantesXml.detailCharges, ConstantesAux.OUR); break;
                        case ConstantesAux.SHAR: XmlUtils.AgregaElemento(ctx, ConstantesXml.detailCharges, ConstantesAux.SHA); break;
                    }
                }

                //Sender's Charges Amount
                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.senderAmount, pacs.chrgsInfAmt);

                if (!await ResolverCuentaNostro(ctx, pacs))
                    return false;

                return true;

            }
            catch (Exception ex)
            {
                ctx.RazonRechazo = ex.Message;
                return false;
            }
        }

        private async Task<bool> ResolverCuentaNostro(ContextoXmlDTO ctx, ResponsePACS pacs)
        {
            var currency = ctx.Moneda;
            string account = string.Empty;

            try
            {
                if (currency == ConstantesAux.MonedaUSD)
                {
                    string bic = string.Empty;

                    var settlementMethod = XmlHelper.ObtenerValor(ctx.XmlDoc, ConstantesXml.settlementMethod);
                    var instdRmbrsmntBic = XmlHelper.ObtenerValor(ctx.XmlDoc, ConstantesXml.recCorrBic);
                    var instgRmbrsmntBic = XmlHelper.ObtenerValor(ctx.XmlDoc, ConstantesXml.sendCorrBic);

                    if (settlementMethod == ConstantesPACS.MetodoLiquidacionCobertura && !string.IsNullOrWhiteSpace(instdRmbrsmntBic))
                    {
                        bic = instdRmbrsmntBic;
                        Config.EscribirLog($"INFO|NostroAccount|A usar el BIC corresponsal (TAG 54a): {bic}");
                    }
                    else if (settlementMethod == ConstantesPACS.MetodoLiquidacionCobertura && !string.IsNullOrWhiteSpace(instgRmbrsmntBic))
                    {
                        bic = instgRmbrsmntBic;
                        Config.EscribirLog($"INFO|NostroAccount|A usar el BIC corresponsal (TAG 53a): {bic}");
                    }
                    else
                    {
                        bic = pacs.appHdr.frBICFI;
                        Config.EscribirLog($"INFO|NostroAccount|A usar el BIC emisor: {bic}");
                    }

                    var debitAccountResult = await _accountDebit.GetDebitAccountAsync(bic, currency);

                    if (debitAccountResult?.IsSuccess == true &&
                        !string.IsNullOrWhiteSpace(debitAccountResult.Data))
                    {
                        account = debitAccountResult.Data;
                    }
                    else
                    {
                        string alert = $"No se encontró cuenta NOSTRO para corresponsal, se intenta con emisor: {pacs.appHdr.frBICFI}";

                        // BIC EMISOR
                        var fallbackResult = await _accountDebit.GetDebitAccountAsync(pacs.appHdr.frBICFI, currency);

                        if (fallbackResult?.IsSuccess == true &&
                            !string.IsNullOrWhiteSpace(fallbackResult.Data))
                        {
                            account = fallbackResult.Data;
                        }
                        else
                        {
                            // CUENTA JP MORGAN
                            alert = "No se encontró cuenta NOSTRO para corresponsal ni emisor; se usará cuenta predeterminada (JP Morgan)";
                            account = await _cuentasNostro.ObtenerCuentaNostroAsync(currency);
                        }
                        await _alerta.AgregaAlertaAsync(ctx.ICN, alert);
                    }
                }
                // OTRAS MONEDAS
                else
                {
                    try
                    {
                        account = await _cuentasNostro.ObtenerCuentaNostroAsync(currency);
                    }
                    catch
                    {
                        account = string.Empty;
                    }
                }
                if (!string.IsNullOrWhiteSpace(account))
                {
                    XmlUtils.AgregaElemento(ctx, ConstantesXml.debitAccount, account);
                }
                if (string.IsNullOrWhiteSpace(account) && currency != ConstantesAux.MonedaMXN)
                {
                    ctx.RazonRechazo = "No se encontró una cuenta a debitar.";
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                ctx.RazonRechazo = $"Error en cuenta Nostro: {ex.Message}";
                return false;
            }
        }
    }
}


using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Contexto;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Utilidades.Fecha;
using Aplicacion_IN_ETL.Utilidades.Gets;
using Aplicacion_IN_ETL.Utilidades.Texto;
using Aplicacion_IN_ETL.Utilidades.Xml;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.XPath;
using static Aplicacion_IN_ETL.Modelos.SwiftISO20022;

namespace Aplicacion_IN_ETL.Servicios.Interpretadores
{
    /// <summary>
    /// Interpreta un mensaje PACS.009 y mapea su información hacia un objeto <see cref="ContextoXmlDTO"/>,
    /// construyendo un XML interno con los datos relevantes de la transacción.
    /// Incluye el procesamiento de encabezado, montos, cuentas, corresponsales, beneficiario, etc.
    /// </summary>
    public class PACS009MX : IPacs009
    {
        public async Task<bool> InterpretarPacs009Async(ContextoXmlDTO ctx, ResponsePACS pacs)
        {
            try
            {
                var xmlInput = new RemoveNameSpace().RemoveNamespace(ctx.Mensaje);
                int totalLineasNameBen = 0;
                int totalLineasDir = 0;
                string lsDireccionConcat = string.Empty;

                if (xmlInput?.Root == null)
                {
                    ctx.RazonRechazo = "No fue posible procesar el XML de entrada.";
                    return false;
                }

                ctx.XmlDoc = new XDocument(
                    new XElement("row",
                        new XAttribute("id", ctx.ICN),
                        new XAttribute(XNamespace.Xml + "space", "preserve")
                    )
                );

                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.ICN, ctx.ICN);

                // CIF Sender
                if (!string.IsNullOrWhiteSpace(pacs.appHdr.frBICFI))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.CIFSender, $"NSWF{pacs.appHdr.frBICFI}");

                // Banco emisor
                if (!string.IsNullOrWhiteSpace(pacs.instgAgtBICFI))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.bancoEmisor, pacs.instgAgtBICFI);

                // Banco receptor
                if (!string.IsNullOrWhiteSpace(pacs.instdAgtBICFI))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.bancoReceptor, pacs.instdAgtBICFI);

                //SLA_UETR
                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.SLA_UETR, "");

                //UETR
                if (!string.IsNullOrWhiteSpace(pacs.pmtId.uetr))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.UETR, pacs.pmtId.uetr);

                //Mensaje tipo
                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.mensajeTipo, ConstantesPACS.PACS009);

                // Moneda
                if (!string.IsNullOrWhiteSpace(pacs.intrBkSttlmAmtCcy))
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.mensajeMoneda, pacs.intrBkSttlmAmtCcy);
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.currency, pacs.intrBkSttlmAmtCcy);
                }

                // Importe
                if (string.IsNullOrWhiteSpace(pacs.intrBkSttlmAmt))
                {
                    ctx.RazonRechazo = "No se pudo leer el mensaje para establecer el importe.";
                    return false;
                }

                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.mensajeImporte, pacs.intrBkSttlmAmt);
                XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.amount, pacs.intrBkSttlmAmt);

                //'Sender's Reference
                if (!string.IsNullOrWhiteSpace(pacs.pmtId.endToEndId))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.relatedReference, pacs.pmtId.endToEndId);

                //'Referencia / Referencia Original
                if (!string.IsNullOrWhiteSpace(pacs.pmtId.instrId))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.senderReference, pacs.pmtId.instrId);

                //Fecha valor
                if (!string.IsNullOrWhiteSpace(pacs.intrBkSttlmDt))
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.valueDate, ConvertirFecha.ConvierteFecha(pacs.intrBkSttlmDt));
                }

                //Moneda
                if (!string.IsNullOrWhiteSpace(pacs.instdAmtCcy))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.instructedCurrency, pacs.instdAmtCcy);

                //Importe
                if (!string.IsNullOrWhiteSpace(pacs.instdAmt))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.instructedAmount, pacs.instdAmt);

                //Creditor Agent Account
                if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.acctIdOthrId))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.awCifAccount, pacs.cdtrAgt.acctIdOthrId);

                // Settlement Method
                var settlementMethod = GetValuePath.GetValueByXPath(xmlInput, ConstantesXmlPath.SttlmMtd);
                if (!string.IsNullOrWhiteSpace(settlementMethod))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.settlementMethod, settlementMethod);

                // Creditor Agent Name
                List<string> laMultivalorMX = new List<string>();
                List<string> laMultivalorMXAux = new List<string>();
                string nodoXML = string.Empty;

                if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.nm))
                    laMultivalorMX = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(pacs.cdtrAgt.finInstnId.nm, 35, ref totalLineasNameBen);

                if (pacs.cdtrAgt.finInstnId.pstlAdr != null)
                {
                    totalLineasNameBen = laMultivalorMX.Count;

                    if (pacs.cdtrAgt.finInstnId.pstlAdr.adrLine.Count > 0)
                    {
                        nodoXML = string.Join(" ", pacs.cdtrAgt.finInstnId.pstlAdr.adrLine);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.ctry))
                        {
                            nodoXML = string.Join(" ", pacs.cdtrAgt.finInstnId.pstlAdr.ctry);
                        }

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.twnNm))
                        {
                            nodoXML = string.Join(" ", pacs.cdtrAgt.finInstnId.pstlAdr.twnNm);
                        }

                        laMultivalorMXAux = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(nodoXML, 35, ref totalLineasDir);
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.strtNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.strtNm);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.bldgNb))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.bldgNb);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.bldgNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.bldgNm);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.flr))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.flr);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.pstBx))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.pstBx);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.room))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.room);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.dept))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.dept);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.subDept))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.subDept);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.ctry))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.ctry);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.twnNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.twnNm);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.pstCd))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.pstCd);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.ctrySubDvsn))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.ctrySubDvsn);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.twnLctnNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.twnLctnNm);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtrAgt.finInstnId.pstlAdr.dstrctNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtrAgt.finInstnId.pstlAdr.dstrctNm);

                        if (!string.IsNullOrWhiteSpace(lsDireccionConcat))
                        {
                            laMultivalorMXAux = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(lsDireccionConcat, 35, ref totalLineasDir);
                        }
                    }
                }

                laMultivalorMX.Concat(laMultivalorMXAux);

                if (laMultivalorMX.Count > 0)
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.awNameAddr, null, laMultivalorMX);
                }

                // Creditor Agent BIC 
                var cdtrAgtBic = GetValuePath.GetValueByXPath(xmlInput, ConstantesXmlPath.CdtrAgtBic);
                if (!string.IsNullOrWhiteSpace(cdtrAgtBic))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.awCif, pacs.cdtrAgt.finInstnId.bicfi);

                // Our Correspondent 
                var instdRmbrsmntBic = GetValuePath.GetValueByXPath(xmlInput, ConstantesXmlPath.BICFI);
                if (!string.IsNullOrWhiteSpace(instdRmbrsmntBic))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.recCorrBic, instdRmbrsmntBic);

                //'Creditor Account 
                if (!string.IsNullOrWhiteSpace(pacs.cdtr.acctIdOthrId))
                    XmlUtils.AgregaElemento(ctx, ConstantesXml.benIntAccount, pacs.cdtr.acctIdOthrId);

                var intermediaryAgent = GetValuePath.GetValueByXPath(xmlInput, ConstantesXmlPath.IntermediaryAgentPacs09);
                if (!string.IsNullOrWhiteSpace(intermediaryAgent))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.intCif, intermediaryAgent);


                laMultivalorMX = new List<string>();
                laMultivalorMXAux = new List<string>();
                totalLineasDir = 0;

                if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.nm))
                    laMultivalorMX = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(pacs.cdtr.finInstnId.nm, 35, ref totalLineasNameBen);

                if (pacs.cdtr.finInstnId.pstlAdr != null)
                {
                    totalLineasNameBen = laMultivalorMX.Count;

                    if (pacs.cdtr.finInstnId.pstlAdr.adrLine.Count > 0)
                    {
                        nodoXML = string.Join(" ", pacs.cdtr.finInstnId.pstlAdr.adrLine);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.ctry))
                        {
                            nodoXML = string.Join(" ", pacs.cdtr.finInstnId.pstlAdr.ctry);
                        }

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.twnNm))
                        {
                            nodoXML = string.Join(" ", pacs.cdtr.finInstnId.pstlAdr.twnNm);
                        }

                        laMultivalorMXAux = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(nodoXML, 35, ref totalLineasDir);
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.strtNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.strtNm);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.bldgNb))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.bldgNb);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.bldgNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.bldgNm);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.flr))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.flr);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.pstBx))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.pstBx);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.room))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.room);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.dept))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.dept);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.subDept))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.subDept);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.ctry))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.ctry);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.twnNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.twnNm);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.pstCd))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.pstCd);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.ctrySubDvsn))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.ctrySubDvsn);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.twnLctnNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.twnLctnNm);

                        if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.pstlAdr.dstrctNm))
                            lsDireccionConcat = string.Concat(lsDireccionConcat, " ", pacs.cdtr.finInstnId.pstlAdr.dstrctNm);

                        if (!string.IsNullOrWhiteSpace(lsDireccionConcat))
                        {
                            laMultivalorMXAux = CortarTexto.ConvierteTextoAMultivalorSinCortarLocal(lsDireccionConcat, 35, ref totalLineasDir);
                        }
                    }
                }

                laMultivalorMX.Concat(laMultivalorMXAux);

                if (laMultivalorMX.Count > 0)
                {
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.benIntNameAddr, null, laMultivalorMX);
                }

                if (!string.IsNullOrWhiteSpace(pacs.cdtr.finInstnId.bicfi))
                    XmlHelper.AgregarElemento(ctx.XmlDoc, ConstantesXml.benIntCif, pacs.cdtr.finInstnId.bicfi);

                return true;

            }
            catch (Exception ex)
            {
                ctx.RazonRechazo = ex.Message;
                return false;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Modelos.camt._052;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Xml.Linq;
using System.Xml.XPath;

namespace Aplicacion_IN_ETL.TipoOperacion
{
    /// <summary>
    /// Obtiene los valores del tipo mensaje CAMT 052 para construir el XML
    /// </summary>
    public sealed class camt_052
    {
        private string Rfk { get; set; }
        private XElement AppHdr { get; set; }
        private XElement Document { get; set; }
        private AccountReport25 AccountReport { get; set; }

        public camt_052(XDocument message, string _rfk)
        {
            try
            {
                Rfk = _rfk;
                ParseAsXDocument(message);
                AccountReport = new AccountReport25(message);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|camt_052.ctor|Error al construir la clase: {ex.Message}");
                throw;
            }
        }


        public string ProcessMessage(string originalVariables)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(originalVariables))
                {
                    throw new Exception("originalVariables es nulo o vacío.");
                }

                XDocument xmlDoc = XDocument.Parse(originalVariables);

                // CounterParty BIC
                Config.EscribirLog("INFO|ProcessMessage|A extraer el elemento: /Fr/FIId/FinInstnId/BICFI");

                var counterPartyBic = AppHdr?.XPathSelectElement("./Fr/FIId/FinInstnId/BICFI");

                if (counterPartyBic != null && !string.IsNullOrWhiteSpace(counterPartyBic.Value))
                {
                    Config.EscribirLog("INFO|ProcessMessage|Se extrajo correctamente el elemento /Fr/FIId/FinInstnId/BICFI");
                    XmlHelper.AgregarElemento(xmlDoc, ConstantesXml.CIFSender, counterPartyBic.Value);
                }
                else
                {
                    throw new Exception("No se encontró el dato requerido counterPartyBic.");
                }

                // Receiver BIC
                Config.EscribirLog("INFO|ProcessMessage|A extraer el elemento: /To/FIId/FinInstnId/BICFI");
                var receiverPartyBic = AppHdr?.XPathSelectElement("./To/FIId/FinInstnId/BICFI");

                if (receiverPartyBic != null && !string.IsNullOrWhiteSpace(receiverPartyBic.Value))
                {
                    Config.EscribirLog("INFO|ProcessMessage|Se extrajo correctamente el elemento /To/FIId/FinInstnId/BICFI");
                    XmlHelper.AgregarElemento(xmlDoc, ConstantesXml.bancoReceptor, receiverPartyBic.Value);
                }
                else
                {
                    throw new Exception("No se encontró el dato requerido receiverPartyBic.");
                }

                return xmlDoc.ToString();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcessMessage|Error procesando mensaje: {ex.Message}");
                throw;
            }
        }

        private void ParseAsXDocument(XDocument doc)
        {
            try
            {
                if (doc == null)
                {
                    throw new ArgumentNullException(nameof(doc), "El documento XML es null.");
                }

                // AppHdr
                Config.EscribirLog("INFO|ParseAsXDocument|A extraer el elemento: /root/AppHdr");
                var appHdr = doc.XPathSelectElement("/root/AppHdr");

                if (appHdr == null)
                {
                    throw new InvalidOperationException("No se pudo leer el AppHdr del camt.052");
                }

                Config.EscribirLog("INFO|ParseAsXDocument|Se extrajo correctamente el elemento: /root/AppHdr");
                AppHdr = appHdr;

                // Document
                Config.EscribirLog("INFO|ParseAsXDocument|A extraer el elemento: /root/Document");
                var document = doc.XPathSelectElement("/root/Document");

                if (document == null)
                {
                    throw new InvalidOperationException("No se pudo leer el Document del camt.052");
                }

                Config.EscribirLog("INFO|ParseAsXDocument|Se extrajo correctamente el elemento: /root/Document");
                Document = document;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ParseAsXDocument|Error al parsear XML: {ex.Message}");
                throw;
            }
        }
    }
}


using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Modelos.camt._053;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Xml.Linq;
using System.Xml.XPath;

namespace Aplicacion_IN_ETL.TipoOperacion
{    /// <summary>
     /// Obtiene los valores del tipo mensaje CAMT 053 
     /// </summary>
    public sealed class camt_053
    {
        private string Rfk { get; set; }
        private XElement AppHdr { get; set; }
        private XElement Document { get; set; }
        private AccountStatement9 Statement { get; set; }

        public camt_053(XDocument _message, string _rfk)
        {
            try
            {
                Rfk = _rfk;
                ParseAsXDocument(_message);
                Statement = new AccountStatement9(Document);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|camt_053.ctor|Error al construir la clase: {ex.Message}");
                throw;
            }
        }

        public string ProcessMessage(string originalVariables)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(originalVariables))
                {
                    throw new Exception("originalVariables es nulo o vacío.");
                }

                // Parse una sola vez
                XDocument xmlDoc = XDocument.Parse(originalVariables);

                // CounterParty BIC
                Config.EscribirLog("INFO|ProcessMessage|A extraer el elemento: /Fr/FIId/FinInstnId/BICFI");
                var counterPartyBic = AppHdr.XPathSelectElement("./Fr/FIId/FinInstnId/BICFI");

                if (counterPartyBic != null && !string.IsNullOrWhiteSpace(counterPartyBic.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /Fr/FIId/FinInstnId/BICFI");
                    XmlHelper.AgregarElemento(xmlDoc, ConstantesXml.CIFSender, counterPartyBic.Value);
                }
                else
                {
                    throw new Exception("No se encontró el dato requerido counterPartyBic.");
                }

                // Receiver BIC
                Config.EscribirLog("INFO|ProcessMessage|A extraer el elemento: /To/FIId/FinInstnId/BICFI");
                var receiverPartyBic = AppHdr.XPathSelectElement("./To/FIId/FinInstnId/BICFI");

                if(receiverPartyBic != null && !string.IsNullOrWhiteSpace(receiverPartyBic.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /To/FIId/FinInstnId/BICFI");
                    XmlHelper.AgregarElemento(xmlDoc, ConstantesXml.bancoReceptor, receiverPartyBic.Value);
                }
                else
                {
                    throw new Exception("No se encontro el dato requerido receiverPartyBic.");
                }

                // Convertir a string una sola vez
                return xmlDoc.ToString();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcessMessage|Error procesando mensaje: {ex.Message}");
                throw;
            }
        }

        private void ParseAsXDocument(XDocument doc)
        {
            try
            {
                if (doc == null)
                {
                    throw new ArgumentNullException(nameof(doc), "El documento XML es null.");
                }

                // AppHdr
                Config.EscribirLog("INFO|ParseAsXDocument|A extraer el elemento: /root/AppHdr");
                var _appHdr = doc.XPathSelectElement("/root/AppHdr");

                if (_appHdr == null)
                {
                    throw new InvalidOperationException("No se pudo leer el AppHdr del camt.053");
                }

                Config.EscribirLog("INFO|ParseAsXDocument|Se extrajo correctamente el elemento: /root/AppHdr");
                AppHdr = _appHdr;

                //Document
                Config.EscribirLog("INFO|ParseAsXDocument|A extraer el elemento: /root/Document");
                var document = doc.XPathSelectElement("/root/Document");

                if (document == null)
                {
                    throw new InvalidOperationException("No se pudo leer el Document del camt.052");
                }

                Config.EscribirLog("INFO|ParseAsXDocument|Se extrajo correctamente el elemento: /root/Document");
                Document = document;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ParseAsXDocument|Error al parsear XML: {ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Contexto;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Linq;
using System.Xml.Linq;
using System.Xml.XPath;
using static Aplicacion_IN_ETL.Modelos.SwiftISO20022;

namespace Aplicacion_IN_ETL.TipoOperacion
{
    /// <summary>
    /// Obtiene los valores del tipo mensaje CAMT 106 para construir el XML
    /// </summary>
    public sealed class camt_106
    {
        private string Rfk { get; set; }
        private XElement AppHdr { get; set; }
        private XElement Document { get; set; }

        public camt_106(string _message, string _rfk)
        {
            try
            {
                Rfk = _rfk;
                ParseAsXDocument(_message);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|camt_106.ctor|Error al inicializar: {ex.Message}");
                throw;
            }
        }

        public string GetVariablesXml(ContextoXmlDTO ctx)
        {
            try
            {
                var xdocument = XDocument.Parse(ctx.VariablesXml);

                Config.EscribirLog("INFO|GetVariablesXml|A extraer el elemento: /Fr/FIId/FinInstnId/BICFI");
                var counterPartyBic = AppHdr.XPathSelectElement("./Fr/FIId/FinInstnId/BICFI");
                if (counterPartyBic != null && !string.IsNullOrWhiteSpace(counterPartyBic.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /Fr/FIId/FinInstnId/BICFI");
                    XmlHelper.AgregarElemento(xdocument, "CIF_sender", counterPartyBic.Value);
                }
                else
                {
                    throw new Exception($"No se encontró el dato requerido counterPartyBic.");
                }

                Config.EscribirLog("INFO|GetVariablesXml|A extraer el elemento: /To/FIId/FinInstnId/BICFI");
                var receiverPartyBic = AppHdr.XPathSelectElement("./To/FIId/FinInstnId/BICFI");
                if (receiverPartyBic != null && !string.IsNullOrWhiteSpace(receiverPartyBic.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /To/FIId/FinInstnId/BICFI");
                    XmlHelper.AgregarElemento(xdocument, "banco_receptor", receiverPartyBic.Value);
                }
                else
                {
                    throw new Exception($"No se encontró el dato requerido receiverPartyBic.");
                }

                Config.EscribirLog("INFO|GetVariablesXml|A extraer el elemento: /ChrgsPmtReq/Chrgs/PerTx/ChrgsId");
                var theirRef = Document.XPathSelectElement("./ChrgsPmtReq/Chrgs/PerTx/ChrgsId");
                if (theirRef != null && !string.IsNullOrWhiteSpace(theirRef.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /ChrgsPmtReq/Chrgs/PerTx/ChrgsId");
                    XmlHelper.AgregarElemento(xdocument, "their_ref", theirRef.Value);
                }
                else
                {
                    throw new Exception($"No se encontró el dato requerido theirRef.");
                }

                Config.EscribirLog("INFO|GetVariablesXml|A extraer el elemento: /ChrgsPmtReq/Chrgs/PerTx/Rcrd/UndrlygTx/InstrId");
                var ourRelatedRef = Document.XPathSelectElement("./ChrgsPmtReq/Chrgs/PerTx/Rcrd/UndrlygTx/InstrId");
                if (ourRelatedRef != null && !string.IsNullOrWhiteSpace(ourRelatedRef.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /ChrgsPmtReq/Chrgs/PerTx/Rcrd/UndrlygTx/InstrId");
                    XmlHelper.AgregarElemento(xdocument, "our_related_ref", ourRelatedRef.Value);
                }
                else
                {
                    throw new Exception($"No se encontró el dato requerido ourRelatedRef.");
                }

                Config.EscribirLog("INFO|GetVariablesXml|A extraer el elemento: /ChrgsPmtReq/Chrgs/PerTx/Rcrd/TtlChrgsPerRcrd/TtlChrgsAmt");
                var ttlChrgsAmt = Document?.XPathSelectElement("./ChrgsPmtReq/Chrgs/PerTx/Rcrd/TtlChrgsPerRcrd/TtlChrgsAmt");

                if (ttlChrgsAmt == null)
                    throw new Exception("No se encontro el dato requerido TtlChrgsAmt.");

                XmlHelper.AgregarElemento(xdocument, "mensaje_moneda", ttlChrgsAmt.Attribute("Ccy")?.Value);
                XmlHelper.AgregarElemento(xdocument, "mensaje_importe", ttlChrgsAmt.Value);

                Config.EscribirLog("INFO|GetVariablesXml|A extraer el elemento: ./ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgt/FinInstnId/BICFI");
                var chrgsAcctAgtBic = Document.XPathSelectElement("./ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgt/FinInstnId/BICFI");

                if (chrgsAcctAgtBic != null && !string.IsNullOrWhiteSpace(chrgsAcctAgtBic.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgt/FinInstnId/BICFI");
                    XmlHelper.AgregarElemento(xdocument, "cust_identifier_code", chrgsAcctAgtBic.Value);
                }

                Config.EscribirLog("INFO|GetVariablesXml|A extraer el elemento: /ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgtAcct/Id/Othr/Id");
                var chrgsAcctAgtAcct = Document.XPathSelectElement("./ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgtAcct/Id/Othr/Id");

                if (chrgsAcctAgtAcct == null || string.IsNullOrWhiteSpace(chrgsAcctAgtAcct.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgtAcct/Id/IBAN");
                    chrgsAcctAgtAcct = Document.XPathSelectElement("./ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgtACct/Id/IBAN");
                }

                if (chrgsAcctAgtAcct != null && !string.IsNullOrWhiteSpace(chrgsAcctAgtAcct.Value))
                {
                    Config.EscribirLog("INFO|Se extrajo correctamente el elemento /ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgtAcct/Id/Othr/Id");
                    XmlHelper.AgregarElemento(xdocument, "cust_identifier_code", chrgsAcctAgtAcct.Value);
                }

                return xdocument.ToString();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|GetVariablesXml|Error procesando mensaje: {ex.Message}");
                throw;
            }
        }

        private void ParseAsXDocument(string rawMessage)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(rawMessage))
                    throw new Exception("El mensaje XML está vacío.");

                var doc = XDocument.Parse("<root>" + rawMessage + "</root>");

                // Limpieza de namespaces
                foreach (var e in doc.Descendants())
                {
                    e.Name = e.Name.LocalName;
                    e.Attributes()
                        .Where(a => a.IsNamespaceDeclaration).Remove();
                }

                // AppHdr
                Config.EscribirLog("INFO|ParseAsXDocument|A extraer el elemento: /root/AppHdr");
                AppHdr = doc.XPathSelectElement("/root/AppHdr")
                     ?? throw new InvalidOperationException("No se pudo leer el AppHdr del camt.106");

                //Document
                Config.EscribirLog("INFO|ParseAsXDocument|A extraer el elemento: /root/Document");
                Document = doc.XPathSelectElement("/root/Document")
                     ?? throw new InvalidOperationException("No se pudo leer el Document del camt.106");

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ParseAsXDocument|Error al parsear XML: {ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Modelos.camt._053;
using MUFG_T24.Comunes;
using System;
using System.Collections.Generic;
using System.Xml.Linq;
using System.Xml.XPath;

namespace Aplicacion_IN_ETL.Modelos.camt._052
{
    /// <summary>
    /// Extrae los elementos del XML
    /// </summary>
    public class AccountReport25
    {
        public List<CashBalance8> Balances { get; set; }
        public List<ReportEntry10> Entries { get; set; }
        public CashAccount39 Account { get; set; }
        public Pagination1 Pagination { get; set; }
        public string Identification { get; set; }

        public AccountReport25(XNode document)
        {
            Config.EscribirLog("INFO|AccountReport25.ctor|A extraer el nodo ./BkToCstmrAcctRpt/Rpt");

            var statement = document.XPathSelectElement(ConstantesXmlPath.Report);

            if (statement == null)
            {
                Config.EscribirLog("WARN|AccountReport25.ctor|No pudo ser extraido el nodo Rpt");
                throw new Exception("No pudo ser extraido el nodo Rpt");
            }
                
            ManejoErrores(() =>
            {
                Identification = statement.XPathSelectElement(ConstantesXmlPath.Id)?.Value;
            }, "ERROR|No pudo ser extraido el nodo Id");

            ManejoErrores(() => ExtractAccount(statement), "ERROR|No pudo ser extraido el nodo Acct");
            ManejoErrores(() => ExtraerPaginacion(statement), "ERROR|No pudo ser extraido el nodo RptPgntn");
            ManejoErrores(() => ExtractBalances(statement), "ERROR|No pudo ser extraido el nodo Bal");
            ManejoErrores(() => ExtractEntries(statement), "ERROR|No pudo ser extraido el nodo Bal");
        }


        private void ExtractAccount(XNode statement)
        {
            Config.EscribirLog("INFO|AccountReport25.ctor.ExtractAccount|A extraer el elemento: ./Acct");
            var acctNode = statement.XPathSelectElement(ConstantesXmlPath.Acct);
            Account = new CashAccount39(acctNode);
            Config.EscribirLog("INFO|AccountReport25.ctor.ExtractAccount|Se extrajo correctamente el nodo: ./Acct");
        }

        private void ExtraerPaginacion(XNode statement)
        {
            Config.EscribirLog("INFO|AccountReport25.ctor.ExtractPagination | A extraer el elemento: ./RptPgntn");
            var acctNode = statement.XPathSelectElement(ConstantesXmlPath.RptPgntn);
            Pagination = new Pagination1(acctNode);
            Config.EscribirLog("INFO|AccountReport25.ctor.ExtractPagination|Se extrajo correctamente el nodo: ./RptPgntn");
        }

        private void ExtractBalances(XNode statement)
        {
            Config.EscribirLog("INFO|AccountReport25.ctor.ExtractBalances|A extraer el elemento: ./Bal");
            var balNodes = statement.XPathSelectElements(ConstantesXmlPath.Bal);
            var balList = new List<CashBalance8>();

            foreach (XNode node in balNodes)
            {
                try
                {
                    var bal = new CashBalance8(node);
                    balList.Add(bal);
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"WARN|AccountReport25.ctor.ExtractBalances|No se pudo extraer un nodo de Bal - {ex.Message}");
                }
            }

            Balances = balList;

            if (Balances.Count < 1)
            {
                throw new Exception("La lista de balances debe contener cuando menos un balance.");
            }
        }

        private void ExtractEntries(XNode statement)
        {
            Config.EscribirLog("INFO|AccountReport25.ctor.ExtractEntries|A extraer el elemento: ./Ntry");
            var ntryNodes = statement.XPathSelectElements(ConstantesXmlPath.Ntry);
            var ntryList = new List<ReportEntry10>();

            foreach (XNode node in ntryNodes)
            {
                try
                {
                    var ntry = new ReportEntry10(node);
                    ntryList.Add(ntry);
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"WARN|AccountReport25.ctor.ExtractEntries|No se pudo extraer un nodo de Ntry - {ex.Message}");
                }
            }

            Entries = ntryList;
        }

        /// <summary>
        /// Manejo de errores
        /// </summary>
        public void ManejoErrores(Action action, string errorMessage, bool throwError = true)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"{errorMessage}: {ex.Message}");
                if (throwError) throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using MUFG_T24.Comunes;
using System;
using System.Collections.Generic;
using System.Xml.Linq;
using System.Xml.XPath;

namespace Aplicacion_IN_ETL.Modelos.camt._053
{
    /// <summary>
    /// Extrae los elementos del XML
    /// </summary>
    public sealed class AccountStatement9
    {
        public List<CashBalance8> Balances { get; set; }
        public List<ReportEntry10> Entries { get; set; }
        public CashAccount39 Account { get; set; }
        public Pagination1 Pagination { get; set; }
        public string Identification { get; set; }


        public AccountStatement9(XNode document)
        {
            Config.EscribirLog("INFO|AccountStatement9.ctor|A extraer el nodo ./BkToCstmrStmt/Stmt");
            var statement = document.XPathSelectElement(ConstantesXmlPath.Stmt);

            if (statement == null)
            {
                Config.EscribirLog("WARN|AccountStatement9.ctor|No pudo ser extraido el nodo Stmt");
                throw new Exception("No pudo ser extraido el nodo Stmt");
            }

            ManejoExcepciones(() =>
            {
                var identificationNode = statement.XPathSelectElement(ConstantesXmlPath.Id)
                    ?? throw new InvalidOperationException("No pudo ser extraído el nodo Id.");
                Identification = identificationNode.Value;
            },  "ERROR|AccountStatement9.ctor|No pudo ser extraido el nodo Id");

            ManejoExcepciones(() => ExtractAccount(statement), "ERROR|AccountStatement9.ctor|No pudo ser extraido el nodo Acct");
            ManejoExcepciones(() => ExtractPagination(statement), "ERROR|AccountStatement9.ctor|No pudo ser extraido el nodo StmtPgntn");
            ManejoExcepciones(() => ExtractBalances(statement), "ERROR | AccountStatement9.ctor | No pudo ser extraido el nodo Bal");
            ManejoExcepciones(() => ExtractEntries(statement), "WARN|AccountStatement9.ctor|No pudo ser extraido el nodo Ntry");
        }

        private void ExtractAccount(XNode statement)
        {
            Config.EscribirLog("INFO|AccountStatement9.ctor.ExtractAccount|A extraer el elemento: ./Acct");
            var acctNode = statement.XPathSelectElement(ConstantesXmlPath.Acct);
            Account = new CashAccount39(acctNode);
            Config.EscribirLog("INFO|AccountStatement9.ctor.ExtractAccount|Se extrajo correctamente el nodo: ./Acct");
        }

        private void ExtractPagination(XNode statement)
        {
            Config.EscribirLog("INFO|AccountStatement9.ctor.ExtractPagination|A extraer el elemento: ./StmtPgntn");
            var acctNode = statement.XPathSelectElement(ConstantesXmlPath.StmtPgntn);
            Pagination = new Pagination1(acctNode);
            Config.EscribirLog("INFO|AccountStatement9.ctor.ExtractPagination|Se extrajo correctamente el nodo: ./StmtPgntn");
        }

        private void ExtractBalances(XNode statement)
        {
            Config.EscribirLog("INFO|AccountStatement9.ctor.ExtractBalances|A extraer el elemento: ./Bal");
            var balNodes = statement.XPathSelectElements(ConstantesXmlPath.Bal);

            var balList = new List<CashBalance8>();

            foreach (XNode node in balNodes)
            {
                try
                {
                    var bal = new CashBalance8(node);
                    balList.Add(bal);
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"WARN|AccountStatement9.ctor.ExtractBalances|No se pudo extraer un nodo de Bal - {ex.Message}");
                }
            }

            Balances = balList;

            if (Balances.Count < 1)
            {
                throw new Exception("La lista de balances debe contener cuando menos un balance.");
            }
        }

        private void ExtractEntries(XNode statement)
        {
            Config.EscribirLog("INFO|AccountStatement9.ctor.ExtractEntries|A extraer el elemento: ./Ntry");
            var ntryNodes = statement.XPathSelectElements(ConstantesXmlPath.Ntry);
            var ntryList = new List<ReportEntry10>();

            foreach (XNode node in ntryNodes)
            {
                try
                {
                    var ntry = new ReportEntry10(node);
                    ntryList.Add(ntry);
                }
                catch (Exception ex)
                {
                    Config.EscribirLog($"WARN|AccountStatement9.ctor.ExtractEntries|No se pudo extraer un nodo de Ntry - {ex.Message}");
                }
            }

            Entries = ntryList;
        }


        public void ManejoExcepciones(Action action, string errorMessage, bool throwError = true)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"{errorMessage}: {ex.Message}");
                if (throwError) throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Utilidades.Xml;
using MUFG_T24.Comunes;
using System;
using System.Xml.Linq;
using System.Xml.XPath;

namespace Aplicacion_IN_ETL.Modelos.camt._053
{
    /// <summary>
    /// Extrae los elementos del XML
    /// </summary>
    public class ReportEntry10
    {
        public string EntryRef { get; set; }
        public string AccountServicerRef { get; set; }
        public ActiveOrHistoricCurrencyAndAmount Amount { get; set; }
        public CreditDebitCode CreditDebitIndicator { get; set; }
        public EntryStatus1Choice Status { get; set; }
        public DateAndDateTime2Choice ValueDate { get; set; }
        public DateAndDateTime2Choice BookingDate { get; set; }
        public BankTransactionCodeStructure4 BankTransactionCode { get; set; }
        public EntryDetails9 EntryDetails { get; set; }

        public ReportEntry10(XNode document)
        {
            if (document == null)
            {
                Config.EscribirLog("ERROR|ReportEntry10.ctor|No pudo ser extraido el nodo Ntry.");
                throw new Exception("No pudo ser extraido el nodo Ntry");
            }

            EntryRef = XmlUtils.TryExtractNodeValue(document, ConstantesXmlPath.NtryRef);
            AccountServicerRef = XmlUtils.TryExtractNodeValue(document, ConstantesXmlPath.AcctSvcrRef);
            var amtNode = document.XPathSelectElement(ConstantesXmlPath.Amt);

            if (amtNode != null)
            {
                Amount = new ActiveOrHistoricCurrencyAndAmount(amtNode);
            }

            var indicador = XmlUtils.TryExtractNodeValue(document, ConstantesXmlPath.CdtDbtInd);

            switch (indicador)
            {
                case ConstantesCAMT.IndicadorDebito:
                    CreditDebitIndicator = CreditDebitCode.DBIT;
                    break;

                case ConstantesCAMT.IndicadorCredito:
                    CreditDebitIndicator = CreditDebitCode.CRDT;
                    break;

                default:
                    throw new Exception("Invalid CreditDebitCode");
            }

            var dtNode = document.XPathSelectElement(ConstantesXmlPath.ValDt);

            if (dtNode != null)
            {
                ValueDate = new DateAndDateTime2Choice(dtNode);
            }

            var bkDtNode = document.XPathSelectElement(ConstantesXmlPath.BookgDt);

            if (bkDtNode != null)
            {
                BookingDate = new DateAndDateTime2Choice(bkDtNode);
            }

            Config.EscribirLog("INFO|BalanceType13.ctor|A extraer el nodo Sts.");
            var node = document.XPathSelectElement(ConstantesXmlPath.Sts);

            if (node == null)
            {
                throw new Exception("No fue posible extraer el nodo Sts");
            }

            Status = new EntryStatus1Choice(node);
            Config.EscribirLog("INFO|BalanceType13.ctor|Se extrajo correctamente el nodo Sts.");

            var bkTxCdNode = document.XPathSelectElement(ConstantesXmlPath.BkTxCd);

            if (bkTxCdNode != null)
            {
                BankTransactionCode = new BankTransactionCodeStructure4(bkTxCdNode);
            }
            else
            {
                throw new Exception("Se presenta un estado invalido en el nodo BkTxCd - No fue posible extraer el nodo");
            }

            var ntryDtlsNode = document.XPathSelectElement(ConstantesXmlPath.NtryDtls);

            if (ntryDtlsNode != null)
            {
                EntryDetails = new EntryDetails9(ntryDtlsNode);
            }
            else
            {
                throw new Exception("Se presenta un estado invalido en el nodo tryDtls - No fue posible extraer el nodo");
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using MUFG_T24.Comunes;
using System;
using System.Xml.Linq;
using System.Xml.XPath;

namespace Aplicacion_IN_ETL.Modelos.camt._053
{
    /// <summary>
    /// Extrae los elementos del XML
    /// </summary>
    public class BalanceType13
    {
        public BalanceType10Choice CodeOrProprietary { get; set; }
        public BalanceType10Choice SubType { get; set; }

        public BalanceType13(XNode document)
        {
            if (document == null)
            {
                Config.EscribirLog("ERROR|BalanceType13.ctor|No pudo ser extraido el nodo Tp.");
                throw new Exception("No pudo ser extraido el nodo Tp");
            }

            Config.EscribirLog("INFO|BalanceType13.ctor|A extraer el nodo CdOrPrtry.");

            //XElement node = document.XPathSelectElement("./CdOrPrtry");
            XElement node = document.XPathSelectElement(ConstantesXmlPath.CdOrPrtry);

            if (node == null)
            {
                throw new Exception("No fue posible extraer el nodo CdOrPrtry");
            }

            CodeOrProprietary = new BalanceType10Choice(node);

            Config.EscribirLog("INFO|BalanceType13.ctor|Se extrajo correctamente el nodo CdOrPrtry.");

            try
            {
                Config.EscribirLog("INFO|BalanceType13.ctor|A extraer el nodo SubTp.");

                //XElement node = document.XPathSelectElement("./SubTp");
                XElement _node = document.XPathSelectElement(ConstantesXmlPath.SubTp);

                if (node != null)
                {
                    SubType = new BalanceType10Choice(_node);

                    Config.EscribirLog("INFO|BalanceType13.ctor|Se extrajo correctamente el nodo SubTp.");
                }
                else
                {
                    Config.EscribirLog("INFO|BalanceType13.ctor|No existe el nodo SubTp.");
                }
            }
            catch (Exception)
            {
                Config.EscribirLog("WARN|BalanceType13.ctor|No pudo ser extraido el nodo SubTp.");
            }
        }

    }
}

using MUFG_T24.Aplicacion.Conversiones;
using Aplicacion_IN_ETL.DTO.SintaxisMT;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Aplicacion_IN_ETL.Utilidades
{
    public static class RegexHelper
    {
        /// <summary>
        /// Obtenemos sintaxis.
        /// </summary>
        public static string ObtenerSintaxis(string tag, Dictionary<string, SintaxisMtDTO> patrones)
        {
            if (!patrones.ContainsKey(tag))
            {
                throw new Exception($"No existe definición del TAG {tag} para el mensaje en proceso");
            }

            var dato = patrones[tag];
            string regexContenido;

            if (!string.IsNullOrWhiteSpace(dato.des_regexp))
            {
                regexContenido = dato.des_regexp;
            }
            else
            {
                regexContenido = ConvertirMtARegex(dato.des_content ?? string.Empty);
            }

            string patronTag = $@"(:{dato.cve_tag}:{regexContenido})[ \t]*(?:\r?\n|$)";
            return patronTag;
        }

        /// Convertimos a Regex.
        /// </summary>
        private static string ConvertirMtARegex(string contenidoMT)
        {
            if (string.IsNullOrWhiteSpace(contenidoMT))
                return @"[0-9A-Za-z\s'\(\)\+\,\-\.\/:\?\n\r]{0,200}?";

            contenidoMT = Regex.Replace(contenidoMT, @"\[(?<inner>[^\[\]]+)\]", m => $"({m.Groups["inner"].Value}){{0,1}}?");
            contenidoMT = Regex.Replace(contenidoMT, @"\/\(\?\<(?<nombre>[A-Za-z_]+)\>34x\)", m => $@"/(?<{m.Groups["nombre"].Value}>[^\r\n]+)");

            //TIPOS BASE
            string declaracionSimple = @"(?<repeticion>[0-9]{1,4}!?)" + @"(?<tipo>[nacxdz])";
            string declaracionVariable = $@"\(\?\<(?<nombre>[A-Za-z_]+)\>({declaracionSimple}|[0-9]{{1,2}}\*[0-9]{{1,2}}[nacxdz])+\)";
            var regexVariables = new Regex(declaracionVariable);
            string patron = contenidoMT;
            var matches = regexVariables.Matches(contenidoMT);

            foreach (Match match in matches)
            {
                string nombre = match.Groups["nombre"].Value;
                string expresionInterna = match.Value;

                // LIMPIAR NOMBRES INTERNOS TIPO <CAMPO>
                expresionInterna = Regex.Replace(expresionInterna, @"\<[A-Za-z_]+\>", "");

                // DETECTA MULTILÍNEA
                var multiMatch = Regex.Match(
                    expresionInterna,
                    @"(?<lineas>[0-9]{1,2})\*(?<largo>[0-9]{1,2})(?<tipo>[nacxdz])"
                );

                // MULTILÍNEA
                if (multiMatch.Success && !expresionInterna.Contains("/"))
                {
                    int maxLineas = ConversionesComunes.ParseEnteroRequerido(
                        multiMatch.Groups["lineas"].Value,
                        "lineas");
                    int maxLength = ConversionesComunes.ParseEnteroRequerido(
                        multiMatch.Groups["largo"].Value,
                        "largo");
                    string tipo = multiMatch.Groups["tipo"].Value;
                    string tipoRegex = ConstruirRegexPorTipo(tipo);
                    string linea = $@"(?<{nombre}>{tipoRegex}{{1,{maxLength}}})";

                    // PATRÓN ESTABLE
                    string multilinea = $@"({linea}\r?\n){{0,{maxLineas - 1}}}{linea}";
                    patron = patron.Replace(match.Value, multilinea);
                }
                else
                {
                    // PROCESAMIENTO SIMPLE
                    var simples = Regex.Matches(expresionInterna, declaracionSimple);
                    var sb = new StringBuilder();
                    int lastIndex = 0;

                    foreach (Match simple in simples)
                    {
                        var segmento = expresionInterna.Substring(lastIndex, simple.Index - lastIndex);
                        segmento = segmento.Replace("?", "");
                        segmento = Regex.Replace(segmento, @"\{.*?\}", "");
                        sb.Append(segmento);
                        string tipo = simple.Groups["tipo"].Value;
                        string repeticion = simple.Groups["repeticion"].Value;
                        string tipoRegex = ConstruirRegexPorTipo(tipo);

                        if (match.Value.Contains("/"))
                        {
                            sb.Append(@"[^\r\n]+");
                        }
                        else
                        {
                            string repRegex = ConstruirRepeticion(repeticion);
                            sb.Append(tipoRegex + repRegex);
                        }

                        lastIndex = simple.Index + simple.Length;
                    }

                    var restante = expresionInterna.Substring(lastIndex);
                    restante = restante.Replace("?", "");
                    restante = Regex.Replace(restante, @"\{.*?\}", "");
                    sb.Append(restante);
                    string resultadoInterno = sb.ToString();
                    string resultado = $@"(?<{nombre}>{resultadoInterno})";
                    patron = patron.Replace(match.Value, resultado);
                }
            }
            patron = Regex.Replace(patron, @"\{0,1\}\{1,", "{1,");

            return patron;
        }

        /// Construimos a Regex por tipo.
        /// </summary>
        private static string ConstruirRegexPorTipo(string tipo)
        {
            switch (tipo)
            {
                case "n":
                    return "[0-9]";

                case "a":
                    return "[A-Za-z]";
                case "c":
                    return "[0-9A-Za-z]";

                case "x":
                    return @"[0-9A-Za-z\s'\(\)\+\,\-\.\/:\?]";

                case "d":
                    return "[0-9,]";

                case "z":
                    return @"[0-9A-Za-z\s'\(\)\+\,\-\.\/:\?\n\r]";

                default:
                    throw new Exception("Tipo '" + tipo + "' desconocido.");
            }
        }

        /// <summary>
        /// Construimos la repeticion.
        /// </summary>
        private static string ConstruirRepeticion(string repeticion)
        {
            if (string.IsNullOrWhiteSpace(repeticion))
                return "";

            if (repeticion.EndsWith("!"))
            {
                var values = repeticion.Replace("!", "");
                return $"{{{values}}}";
            }

            return $"{{1,{repeticion}}}";
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Utilidades.Tags
{
    /// <summary>
    /// Proporciona metodos para realizar la busqueda de TAGS de un mensaje MQ
    /// </summary>
    public class Tags
    {
        /// <summary>
        /// Busca los TAGS en el mensaje MQ
        /// </summary>
        public static List<string> BuscarTags(string mensaje)
        {
            if (string.IsNullOrWhiteSpace(mensaje))
                return new List<string>();

            var pattern = @"(?<=\r\n|^):\d{2}[A-Z]?:";

            return Regex.Matches(mensaje, pattern)
                .Cast<Match>()
                .Select(m => m.Value.Replace(":", ""))
                .Distinct()
                .ToList();
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Contexto;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.SwiftStandardRelease;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Tag57D
{
    /// <summary>
    /// Método que construye la dirección mediante el tag57 del mensaje MQ y lo agrega al XML
    /// </summary>
    public static class ProcesaTag57D
    {
        public static async Task<bool> ConstruirDireccionTag57DAsync(ContextoXmlDTO xml, string tag, string contenido, SwiftStandardReleaseSanitize sanitize)
        {
            try
            {
                Config.EscribirLog($"INFO|TAG57|Procesando TAG {tag}");


                var lineas = contenido
                    .Replace($":{tag}:", "")
                    .Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Trim().ToUpper())
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .ToList();

                string id = null;
                string nombreBanco = null;
                string ciudad = null;
                string pais = null;

                // DETECTAR FORMATO
                if (tag.Equals(ConstantesTags.TAG57A, StringComparison.OrdinalIgnoreCase))
                {
                    // BIC
                    id = lineas.FirstOrDefault();
                }
                else if (tag.Equals(ConstantesTags.TAG57D, StringComparison.OrdinalIgnoreCase))
                {
                    if (lineas.Count == 4)
                    {
                        nombreBanco = lineas[0];
                        id = lineas[1];
                        ciudad = lineas[2];
                        pais = lineas[3];
                    }
                    else
                    {
                        // IDENTIFICADOR
                        id = lineas.LastOrDefault();
                    }
                }

                if (string.IsNullOrWhiteSpace(id))
                {
                    xml.RazonRechazo = $"SSR2026: TAG {tag} sin identificador";
                    return false;
                }

                // NORMALIZAR BIC / ABA
                bool esBic = Regex.IsMatch(id, @"^[A-Z0-9]{8}([A-Z0-9]{3})?$");

                if (esBic && id.Length == 8)
                {
                    id = id + "XXX"; // CONVERTIR A BIC11
                }

                // ENRIQUECIMIENTO
                SwiftStandardReleaseDto infoBank = null;

                try
                {
                    infoBank = await sanitize.ObtenerBancoValidado(id);
                }

                catch (Exception ex)
                {
                    xml.RazonRechazo = $"SSR2026: {ex.Message}";
                    return false;
                }

                if (infoBank == null)
                {
                    xml.RazonRechazo = "SSR2026: No se obtuvo información del banco.";
                    return false;
                }

                nombreBanco = infoBank.FinancialInstitutionName;
                ciudad = infoBank.FinancialInstitutionTownName;
                pais = infoBank.FinancialInstitutionCountryCode;

                if (string.IsNullOrWhiteSpace(nombreBanco) || string.IsNullOrWhiteSpace(ciudad) || string.IsNullOrWhiteSpace(pais))
                {
                    xml.RazonRechazo = $"SSR2026: Datos incompletos del banco";
                    return false;
                }

                if (!Regex.IsMatch(pais, "^[A-Z]{2}$"))
                {
                    xml.RazonRechazo = $"SSR2026: País inválido";
                    return false;
                }

                // CONSTRUCCIÓN XML HYBRID
                var listaValores = new List<string> { nombreBanco, id, ciudad, pais };

                XmlHelper.AgregarElemento(xml.XmlDoc, "account_with_institution", null, listaValores);
                Config.EscribirLog($"INFO|ConstruirDireccionTag57D|Correcta: {id} {ciudad} {pais}");

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConstruirDireccionTag57D|{ex}");
                throw;
            }
        }
    }
}


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Utilidades.Texto
{
    /// <summary>
    /// Construcción de texto multilinea para la dirección de los PACS
    /// </summary>
    public class TextoMultilinea
    {
        public static List<string> ConvertirTextoMultilinea(string texto, int maxLength, out int totalLineas)
        {
            var resultado = new List<string>();
            if (string.IsNullOrWhiteSpace(texto))
            {
                totalLineas = 0;
                return resultado;
            }
            texto = texto.Trim().TrimStart('-');
            var palabras = texto.Split(' ');
            var linea = "";
            foreach (var palabra in palabras)
            {
                if ((linea + " " + palabra).Trim().Length <= maxLength)
                {
                    linea = string.IsNullOrEmpty(linea)
                        ? palabra
                        : linea + " " + palabra;
                }
                else
                {
                    resultado.Add(linea);
                    linea = palabra;
                }
            }
            if (!string.IsNullOrEmpty(linea))
                resultado.Add(linea);
            totalLineas = resultado.Count;
            return resultado;
        }
    }
}

using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using Newtonsoft.Json;
using System;
using System.Globalization;

namespace Aplicacion_IN_ETL.Utilidades
{
    /// <summary>
    /// Proporciona metodos para dar formato a un objeto JSON.
    /// </summary>
    public class DecimalFormatConverter : JsonConverter
    {
        /// <summary>
        ///Determina si el convertidor puede procesar el tipo especificado.
        /// </summary>
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(decimal) || objectType == typeof(decimal?);
        }

        /// <summary>
        /// Deserializa unn valor JSON a un tipo decimal o decimal?.
        /// Si el valor del JSON  es nuelo, devuelve 0 para decimal y null para decimal?
        /// </summary>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            try
            {
                if (reader.Value == null)
                    return objectType == typeof(decimal?) ? (decimal?)null : 0m;

                return ConversionesComunes.ParseDecimalRequerido(
                    Convert.ToString(reader.Value, CultureInfo.InvariantCulture),
                    reader.Path);
              
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|DecimalFormatConverter.ReadJson|{ex.Message}");
                throw new JsonSerializationException("Error al deserializar decimal.", ex);
            }
        }

        /// <summary>
        /// Serializa un valor decimal utilizando el formato numerico definido por la aplicacion.
        /// </summary>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            try
            {
                if (value == null)
                {
                    writer.WriteNull();
                    return;
                }

                decimal decimalValue = (decimal)value;
                writer.WriteValue(decimalValue.ToString("0.00", CultureInfo.InvariantCulture));
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|DecimalFormatConverter.WriteJson|{ex.Message}");
                throw new JsonSerializationException("Error al serializar decimal.", ex);
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Utilidades.Fecha
{
    /// <summary>
    ///Propociona metodos para poder formatear la fecha utilizadas en el procesamiento de operaciones
    /// </summary>
    public class ConvertirFecha
    {
        /// <summary>
        /// Convierte una fecha al formato esperado por el flujo del procesamiento.
        /// </summary>
        /// <param name="texto">Valor que contiene la fecha a formatear</param>
        /// <returns></returns>
        public static string ConvierteFecha(object texto)
        {
            if (texto == null)
                return null;

            var lsFecha = texto.ToString();

            if (string.IsNullOrWhiteSpace(lsFecha))
                return null;
            //formato yyyyMMdd
            if (lsFecha.Length == 8 && long.TryParse(lsFecha, out _))
            {
                // 20250525 
                return lsFecha;
            }
            //formato yyyy-MM-dd
            if (lsFecha.Length == 10 && lsFecha[4] == '-' && lsFecha[7] == '-')
            {
                // 250525
                return lsFecha.Substring(2, 2) +
                       lsFecha.Substring(5, 2) +
                       lsFecha.Substring(8, 2);
            }
            return null;
        }
    }
}

using MUFG_T24.Comunes;
using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Utilidades.Texto
{
    /// <summary>
    /// Proporciona metodos para formatear texto de los mensajes SWIFT.
    /// </summary>
    public class CortarTexto
    {
        /// <summary>
        /// Utilidad para formatear campos de textode mensajes SWIFT, dividiendo el contenido en multiples lineas con una longitud 
        /// maxima determinada y preservando la integridad de la informacion.
        /// </summary>
        public static List<string> ConvierteTextoAMultivalorSinCortarLocal(string texto, int MW_SeparadorMultivalorLength, ref int totalLineas, bool Prefix = false)
        {
            string strCadena = string.Empty;
            List<string> retorno = new List<string>();
            string contenedor;
            string subContenedor;

            texto = texto.Trim();

            while (texto.StartsWith("-"))
            {
                texto = texto.Substring(1); 
                texto = texto.Trim();
            }

            if (texto.Length == 0)
            {
                return retorno;
            }
            else if (texto.Length <= MW_SeparadorMultivalorLength)
            {
                if (Prefix)
                {
                    strCadena = "//" + texto;
                }
                else
                {
                    retorno.Add(texto);
                }

                totalLineas = 1;
                return retorno;
            }

            contenedor = "";

            bool blnAux = false;
            int intCont = 0;
            string[] arreglo = texto.Split(' ');

            foreach (var _ in arreglo)
            {
                string palabraTemp = string.Empty;

                if (!blnAux)
                {
                    if (arreglo.Length > (intCont + 1))
                    {
                        if (arreglo[intCont + 1].StartsWith("-"))
                        {
                            palabraTemp = arreglo[intCont];
                            blnAux = false;
                        }
                        else
                        {
                            palabraTemp = arreglo[intCont];
                        }
                    }
                    else
                    {
                        palabraTemp = arreglo[intCont];
                    }

                    if (palabraTemp.Length >= MW_SeparadorMultivalorLength)
                    {
                        if (contenedor.Length > 0)
                        {
                            retorno.Add(contenedor);
                            contenedor = "";
                        }

                        while (palabraTemp.Length >= MW_SeparadorMultivalorLength)
                        {
                            string trozo = palabraTemp.Substring(0, MW_SeparadorMultivalorLength);
                            retorno.Add(trozo);
                            palabraTemp = palabraTemp.Remove(0, MW_SeparadorMultivalorLength);
                        }
                    }

                    if (palabraTemp.Length > 0)
                    {
                        if (contenedor.Length + palabraTemp.Length + 1 > MW_SeparadorMultivalorLength)
                        {
                            if (Regex.IsMatch(contenedor, "(?<pattern>,.+:)"))
                            {
                                subContenedor = contenedor.Substring(0, (contenedor.LastIndexOf(":") - 1));
                                retorno.Add(subContenedor);
                                palabraTemp = contenedor.Substring(contenedor.LastIndexOf(":")) + " " + palabraTemp;
                                contenedor = palabraTemp;
                            }
                            else
                            {
                                retorno.Add(contenedor);
                                contenedor = palabraTemp;
                            }
                        }
                        else
                        {
                            if (contenedor == "")
                            {
                                contenedor = palabraTemp;
                            }
                            else
                            {
                                contenedor = contenedor + " " + palabraTemp;
                            }
                        }
                    }
                }
                else
                {
                    blnAux = false;
                }

                intCont += 1;
            }

            if (contenedor.Length > 0)
                retorno.Add(contenedor);

            foreach (string Elemento in retorno)
            {
                if (Prefix)
                {
                    strCadena += "//" + Elemento + "|";
                }
                else
                {
                    strCadena += Elemento + "|";
                }
            }

            if (strCadena.Length > 0)
                strCadena = strCadena.Substring(0, strCadena.Length - 1);

            totalLineas = retorno.Count;

            return retorno;
        }

        /// <summary>
        /// Obtiene una cantidad especifica de lienas de una coleccion multivalor. 
        /// Si el numero de lineas excede e limite indicado, las lineas adicionales se devuelven mediante 
        /// el parametro <paramref name="lineasSobrantes"/>
        /// </summary>
        public static List<string> RegresaNLineasDeMultivalor(List<string> texto, int lineasRegreso, ref List<string> lineasSobrantes)
        {
            int numeroLineas = 0;
            List<string> retorno = new List<string>();
            numeroLineas = texto.Count;

            Config.EscribirLog($"INFO|regresaNLineasDeMultivalor|numLineas =  {numeroLineas}");
            Config.EscribirLog($"INFO|regresaNLineasDeMultivalor|LineasDeRegreso = {lineasRegreso}");

            if (numeroLineas > lineasRegreso)
            {
                retorno = texto;
                retorno = texto.GetRange(0, lineasRegreso);
                Config.EscribirLog($"INFO|regresaNLineasDeMultivalor|retorno = {retorno.Count}");

                lineasSobrantes = texto.GetRange(lineasRegreso, numeroLineas - lineasRegreso);
            }
            else
            {
                retorno = texto;
                lineasSobrantes = null;
            }

            return retorno;
        }
    }
}

using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using Aplicacion_IN_ETL.Utilidades;
using MUFG_T24.Comunes;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion_IN_ETL.Servicios
{
    /// <summary>
    /// Gestiona la obtencion y consulta de cuentas NOSTRO asociadas a una moneda, utilizando 
    /// un mecanismo de cache para optimizar el acceso a la informacion.
    /// </summary>
    public class CuentasNostro : ICuentasNostro
    {
        private readonly IRepositorioNostro _repo;
        private Dictionary<string, string> _cache;

        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="CuentasNostro"/>
        /// </summary>
        /// <param name="repo">Responsable de proporcionar la informacion de cuentas NOSTRO</param>
        public CuentasNostro(IRepositorioNostro repo)
        {
            _repo = repo;
        }

        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Obtiene la cuenta asociada a la moneda indicada.
        /// </summary>
        /// <param name="moneda">Codigo de la moneda</param>
        /// <returns></returns>
        public async Task<string> ObtenerCuentaNostroAsync(string moneda)
        {
            if (_cache != null)
                return GetCuenta(moneda);

            await _lock.WaitAsync();

            try
            {
                if (_cache == null)
                {
                    var lista = await _repo.ObtieneDatosCuentasNostro();

                    _cache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    foreach (var cuentaNostro in lista.Where(x => !string.IsNullOrWhiteSpace(x.Moneda)))
                    {
                         moneda = cuentaNostro.Moneda.Trim().ToUpperInvariant();

                        if (_cache.ContainsKey(moneda))
                        {
                            Config.EscribirLog($"ERROR|ObtenerCuentaNostroAsync|La moneda '{moneda}' está duplicada en la base de datos.");
                            continue;
                        }

                        _cache.Add(moneda, cuentaNostro.Cuenta);
                    }
                }
            }
            finally
            {
                _lock.Release();
            }

            return GetCuenta(moneda);
        }

        /// <summary>
        /// Obtiene la cuenta por medio de la moneda
        /// </summary>
        private string GetCuenta(string moneda)
        {
            if (string.IsNullOrWhiteSpace(moneda))
                return string.Empty;

            return _cache.TryGetValue(moneda.Trim().ToUpper(), out var cuenta)
                ? cuenta
                : string.Empty;
        }
    }
}

using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using static Aplicacion_IN_ETL.Modelos.SwiftISO20022;

namespace Aplicacion_IN_ETL.Servicios
{
    /// <summary>
    /// Servicio responsable de la comunicacion con los servicios SAMH requerido durante el procesamiento de las operaciones.
    /// </summary>
    public class SamhService : ISamhService
    {
        private readonly HttpClient _httpClient;

        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="SamhService"/>
        /// </summary>
        /// <param name="httpClient">Cliente HTTP utilizado para realizar las llamadas a los servicios</param>
        public SamhService(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        /// <summary>
        /// Envía un mensaje en formato XML a un servicio externo para obtener la información
        /// del encabezado (SAMH) y la deserializa a un objeto de tipo <see cref="SAMH"/>.
        /// </summary>
        /// <param name="mensaje">Contenido del mensaje en formato XML que será procesado por el servicio.</param>
        /// <param name="gsUrlGetMessageHeaders_XAMH">URL del endpoint que procesa la extracción del header SAMH.</param>
        /// <returns>
        /// Un objeto <see cref="Result{SAMH}"/> que contiene:
        /// - Éxito: el objeto SAMH con la información del encabezado.
        /// - Falla: un mensaje de error si la respuesta HTTP falla, es inválida o ocurre una excepción.
        /// </returns>
        /// 
        public async Task<Result<SAMH>> ObtenerHeaderAsync(string mensaje, string gsUrlGetMessageHeaders_XAMH)
        {
            try
            {
                var content = new StringContent(mensaje, Encoding.UTF8, "application/xml");

                var response = await _httpClient.PostAsync(gsUrlGetMessageHeaders_XAMH, content);

                if (!response.IsSuccessStatusCode)
                {
                    return Result<SAMH>.Failed($"Error HTTP SAMH: {response.StatusCode}");
                }

                var json = await response.Content.ReadAsStringAsync();
                var obj = JsonConvert.DeserializeObject<SAMH>(json);

                if (obj == null)
                {
                    return Result<SAMH>.Failed("Respuesta SAMH inválida");
                }

                return Result<SAMH>.Success(obj);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerHeader_XAMH|{ex}");
                return Result<SAMH>.Failed("No se pudo leer el mensaje para establecer la estructura del header SAMH.");
            }
        }

        /// <summary>
        /// Valida un mensaje XML contra un servicio externo de validación (MX).
        /// Envía el contenido al endpoint especificado y procesa la respuesta.
        /// </summary>
        /// <param name="mensaje">Mensaje XML a validar.</param>
        /// <param name="gsUrlValidateMessage_MX">URL del servicio de validación.</param>
        /// <returns>
        /// Un objeto <see cref="Result{Response_Validator_OK}"/> que contiene:
        /// Éxito con la respuesta validada si la operación es correcta.
        /// Error con el mensaje correspondiente si falla la validación o la llamada HTTP.
        /// </returns>
        public async Task<Result<Response_Validator_OK>> ValidarMensajeAsync(string mensaje, string gsUrlValidateMessage_MX)
        {
            try
            {
                var content = new StringContent(mensaje, Encoding.UTF8, "application/xml");
                var response = await _httpClient.PostAsync(gsUrlValidateMessage_MX, content);
                var json = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                {
                    var error = JsonConvert.DeserializeObject<ResponseValidator>(json);

                    return Result<Response_Validator_OK>.Failed(
                        error?.message ?? "ValidarMensaje|MX");
                }

                var data = JsonConvert.DeserializeObject<Response_Validator_OK>(json);
                return Result<Response_Validator_OK>.Success(data);
            }
            catch (Exception ex)
            {
                return Result<Response_Validator_OK>.Failed($"ValidarMensaje|MX|{ex}");
            }
        }
    }
}

using Aplicacion_IN_ETL.Interfaces.Externos;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using static Aplicacion_IN_ETL.Modelos.SwiftISO20022;
using static Aplicacion_IN_ETL.Utilidades.Parametros.ParametrosHelper;

namespace Aplicacion_IN_ETL.Servicios
{
    /// <summary>
    /// Servicio responsable de la comunicacion con los servicios externos utilizados para el procesamiento de mensajes PACS.
    /// </summary>
    public class PacsService : IPacsService
    {
        private readonly HttpClient _httpClient;


        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="PacsService"/>
        /// </summary>
        /// <param name="httpClient">Cliente HTTP utilizado para realizar las llamadas a los servicios</param>
        public PacsService(HttpClient httpClient)
        {
            _httpClient = httpClient;

        }

        /// <summary>
        /// Envía un mensaje (PACS008) a un servicio externo para su validación y procesamiento.
        /// Realiza una petición HTTP POST con el mensaje en formato XML y procesa la respuesta.
        /// </summary>
        /// <param name="mensaje">Contenido del mensaje PACS008 en formato XML.</param>
        /// <param name="apiUrl">URL del servicio API que procesará el mensaje.</param>
        /// <returns>
        /// Un objeto <see cref="Result{ResponsePACS}"/> que contiene:
        /// - Éxito con los datos deserializados del PACS008b si la respuesta es válida.
        /// <remarks>
        /// En caso de error HTTP, intenta deserializar el mensaje de error del servicio.
        /// Si ocurre una excepción interna, se registra en el log.
        /// </remarks>
        public async Task<Result<ResponsePACS>> GetPacs008DataAsync(string mensaje, string apiUrl)
        {
            try
            {
                var content = new StringContent(
                    mensaje,
                    Encoding.UTF8,
                    "application/xml"
                );

                var response = await _httpClient.PostAsync(apiUrl, content);
                var json = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                {
                    try
                    {
                        var error = JsonConvert.DeserializeObject<ResponseValidator>(json);
                        return Result<ResponsePACS>.Failed(error?.message ?? "Error PACS008");
                    }
                    catch
                    {
                        return Result<ResponsePACS>.Failed($"Error HTTP PACS008: {response.StatusCode}"
                        );
                    }
                }

                var data = JsonConvert.DeserializeObject<ResponsePACS>(json);

                if (data == null)
                {
                    return Result<ResponsePACS>.Failed("Respuesta PACS008 inválida");
                }

                return Result<ResponsePACS>.Success(data);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|PACS008|{ex}");

                return Result<ResponsePACS>.Failed("Error obteniendo PACS008");
            }
        }
        /// <summary>
        /// Envía un mensaje (PACS009) a un servicio externo para su validación y procesamiento.
        /// Realiza una petición HTTP POST con el mensaje en formato XML y procesa la respuesta.
        /// </summary>
        /// <param name="mensaje">Contenido del mensaje PACS009 en formato XML.</param>
        /// <param name="apiUrl">URL del servicio API que procesará el mensaje.</param>
        /// <returns>
        /// Un objeto <see cref="Result{ResponsePACS}"/> que contiene:
        /// - Éxito con los datos deserializados del PACS009 si la respuesta es válida.
        /// <remarks>
        /// En caso de error HTTP, intenta deserializar el mensaje de error del servicio.
        /// Si ocurre una excepción interna, se registra en el log.
        /// </remarks>
        public async Task<Result<ResponsePACS>> GetPacs009DataAsync(string mensaje, string apiUrl)
        {
            try
            {
                var content = new StringContent(
                    mensaje,
                    Encoding.UTF8,
                    "application/xml"
                );

                var response = await _httpClient.PostAsync(apiUrl, content);
                var json = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                {
                    try
                    {
                        var error = JsonConvert.DeserializeObject<ResponseValidator>(json);

                        return Result<ResponsePACS>.Failed(
                            error?.message ?? "Error PACS009"
                        );
                    }
                    catch
                    {
                        return Result<ResponsePACS>.Failed(
                            $"Error HTTP PACS009: {response.StatusCode}"
                        );
                    }
                }

                var data = JsonConvert.DeserializeObject<ResponsePACS>(json);

                return data != null
                    ? Result<ResponsePACS>.Success(data)
                    : Result<ResponsePACS>.Failed("Respuesta PACS009 inválida");
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|PACS009|{ex}");

                return Result<ResponsePACS>.Failed(
                    "Error obteniendo PACS009");
            }
        }
    }
}

using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Proporciona los metodos para poder obtener la informacion de una cuenta de debito a traves de un bic y una moneda
    /// </summary>
    public class AccountDebitRepo : IAccountDebit
    {
        private readonly IIntegracion _integracion;

        /// <summary>
        /// Inicializa una nueva instancia para <see cref="AccountDebitRepo"/>
        /// </summary>
        public AccountDebitRepo(IIntegracion integracion)
        {
            _integracion = integracion;
        }

        /// <summary>
        /// Obtiene la cuenta de débito asociada a un BIC y una moneda específica.
        /// </summary>
        /// <param name="bic">Código BIC (Bank Identifier Code) de la institución.</param>
        /// <param name="currency">Código de moneda (por ejemplo, USD, EUR, MXN).</param>
        /// <returns>
        /// Un objeto <see cref="Result{T}{"/> que contiene la cuenta de débito si la operación es exitosa,
        /// o un mensaje de error en caso de que no se encuentre la cuenta.
        /// </returns>
        public async Task<Result<string>> GetDebitAccountAsync(string bic, string currency)
        {
            try
            {
                var result = await _integracion.GetNostroAsync(bic, currency);

                if (result?.IsSuccess == true && !string.IsNullOrWhiteSpace(result.Data))
                {
                    return result;
                }

                return Result<string>.Failed(
                    $"No se encontró cuenta para BIC:{bic} moneda:{currency}");
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|GetDebitAccount|{ex.Message}");
                throw;
            }
        }
    }
}



using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using Aplicacion_IN_ETL.Utilidades.Valores;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Reepositorio responsable de la actualizacion de la operacion.
    /// configuradas.
    /// </summary>
    public class ActualizacionRepo : RepositorioMaestro, IActualizacion
    {
        /// <summary>
        /// Inicializa una nueva instancia <see cref="ActualizacionRepo"/>
        /// </summary>
        public ActualizacionRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Actualiza el estatus SPID de un folio especifico.
        /// </summary>
        /// <param name="folio">Folio de la operación a actualizar</param>
        /// <param name="tipoOperacion">SWIFT_IN.</param>
        /// <returns>
        public async Task ActualizarEstatusSpidAsync(string folio, string tipoOperacion)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(
                ConstantesSql.USP_SWT_IN_ETL_UPDATE_ESTATUS_SPID,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion));
                });
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizarEstatusSpid|Folio:{folio}|TipoOperación:{tipoOperacion}|{ex}");
                throw;
            }

        }

        /// <summary>
        /// Actualiza el estatus SPID de un folio especifico.
        /// </summary>
        /// <param name="folio">Folio de la operación a actualizar</param>
        /// <param name="categoria">Categoria MY</param>
        /// <returns>
        public async Task ActualizarCategoriaMensajeAsync(string folio, int categoria)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(
                 ConstantesSql.USP_SWT_IN_ETL_ACTUALIZA_CATEGORIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@categoria", categoria));
                });
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizarCategoriaMensaje|Folio:{folio}|Categoria:{categoria}|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza los campos que se van guardando en el diccionario
        /// </summary>
        /// <param name="icn">Folio de la operación que se va a actualizar.</param>
        /// <param name="tipoOperacion">SWIFT_IN.</param>
        /// <param name="claveMensaje">Categoria del mensaje MT.</param>
        /// <param name="variablesXml">Contiene el xml generado a partir de la de la sintaxis de los Tags.</param>
        /// <param name="campos">Diccionario que contiene los campos que se actualizaran.</param> 
        public async Task ActualizarCamposAsync(string icn, string tipoOperacion, string claveMensaje, string variablesXml, Dictionary<string, string> campos)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(ConstantesSql.USP_SWF_UPDATE_DB_PLAN_TRABAJO_VALUES,
                cmd =>
               {
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@icn", icn));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_mensaje", claveMensaje));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@variables_xml", variablesXml));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cliente", GetValor.DameValor(campos, "num_cliente")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombre_cliente", GetValor.DameValor(campos, "nombre_cliente")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cdl", GetValor.DameValor(campos, "num_cdl")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cuenta", GetValor.DameValor(campos, "num_cuenta")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_moneda", GetValor.DameValor(campos, "cve_moneda")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@des_importe", GetValor.DameValor(campos, "des_importe")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@fec_valor", GetValor.DameValor(campos, "fec_valor")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cif_emisor", GetValor.DameValor(campos, "num_cif_emisor")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cta_vostro_emisor", GetValor.DameValor(campos, "cta_vostro_emisor")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_swift_emisor", GetValor.DameValor(campos, "cve_swift_emisor")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombre_emisor", GetValor.DameValor(campos, "nombre_emisor")));
               });
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizarCampos|ICN:{icn}|Tipo:{tipoOperacion}|Mensaje:{claveMensaje}|{ex}");
                throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Proporciona los metodos para agregar una alerta asociadaa a un folio especifico.
    /// </summary>
    public class AlertaRepo : RepositorioMaestro, IAlerta
    {
        /// <summary>
        /// Inicializa una nueva instancia <see cref="AlertaRepo"/>
        /// </summary>
        public AlertaRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Registra una alerta asociada a un identificador ICN en la base de datos mediante un procedimiento almacenado.
        /// </summary>
        /// <param name="icn">Identificador del caso o registro al que se asociará la alerta.</param>
        /// <param name="mensajeAlerta">Descripción o mensaje de la alerta que se desea registrar.</param>
        /// <returns>
        /// Retorna <c>true</c> si la alerta se registra exitosamente; en caso contrario, retorna <c>false</c>.
        /// </returns>
        /// 
        public async Task<bool> AgregaAlertaAsync(string icn, string mensajeAlerta)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(icn))
                    return false;

                if (string.IsNullOrWhiteSpace(mensajeAlerta))
                    return false;

                Config.EscribirLog($"INFO|AgregarAlerta|Intento registrar alerta, icn='{icn}'.");

                var result = await EjecutarStoredProceduredNonQuery(
                    ConstantesSql.USP_SWF_IN_ETL_MARCA_OP_ALERTA,
                    cmd =>
                    {
                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro("@icn", icn));
                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro("@tipo_operacion", ConstantesServicio.TipoOperacion));
                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro("@des_alerta", mensajeAlerta));
                    });

                return result.Exito;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|AgregarAlerta|No se pudo marcar el error: {ex}");
                return false;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Aplicacion.Conversiones;
using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Encargado de consumir la API de comisiones mediante solicitudes HTTP.
    /// </summary>
    public class ApiCommissionClientRepo : IApiComission
    {

        private readonly HttpClient _httpClient;

        /// <summary>
        /// Inicializa una nueva instancia <see cref="ApiCommissionClientRepo"/>
        /// </summary>
        public ApiCommissionClientRepo(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        /// <summary>
        /// Construye y envía una solicitud HTTP para encolar un cargo de comisión entrante
        /// a partir de la información contenida en un documento XML (mensaje MT-191).
        /// </summary>
        /// <param name="folio">Identificador único de la operación (RFK).</param>
        /// <param name="xmlDoc">Documento XML del cual se extraen los datos necesarios para la solicitud.</param>
        /// <param name="apiUrl">URL del endpoint de la API donde se enviará la solicitud.</param>
        /// <returns>
        /// Un objeto <see cref="Result{bool}"/> que indica si la operación fue exitosa.
        /// Devuelve éxito cuando la API responde con un código HTTP satisfactorio; en caso contrario,
        /// contiene el mensaje de error correspondiente.
        /// </returns>
        public async Task<Result<bool>> EnqueueIncomingCommissionChargeRequestAsync(string folio, XDocument xmlDoc, string apiUrl)
        {
            try
            {
                Config.EscribirLog("INFO|EnqueueCommission|A generar peticion para pago de comision MT-191");

                var job = new ChargesPaymentRequestDto
                {
                    Rfk = folio,
                    MessageDirection = MessageDirectionEnum.Incoming,
                    CounterPartyIdentifier = XmlHelper.ObtenerValor(xmlDoc, ConstantesXml.CIFSender),
                    RelatedReference = XmlHelper.ObtenerValor(xmlDoc, ConstantesXml.ourRelatedRef),
                    TheirTransactionReference = XmlHelper.ObtenerValor(xmlDoc, ConstantesXml.theirRef),
                    AccountWithInstitution = XmlHelper.ObtenerValor(xmlDoc, ConstantesXml.custIdentifier),
                    AccountWithInstitutionIdentifier = XmlHelper.ObtenerValor(xmlDoc, ConstantesXml.custIdentifierCode),
                    Currency = XmlHelper.ObtenerValor(xmlDoc, ConstantesXml.mensajeMoneda),

                    Amount = ConversionesComunes.ParseDecimalRequerido(
                        XmlHelper.ObtenerValor(xmlDoc, ConstantesXml.mensajeImporte),
                        ConstantesXml.mensajeImporte)
                };

                var json = JsonConvert.SerializeObject(job);
                Config.EscribirLog($"INFO|EnqueueCommission|Payload: {json}");

                var content = new StringContent(json, Encoding.UTF8, "application/json");
                Config.EscribirLog($"INFO|EnqueueIncomingComissionChargeRequest|To enqueue commission request for MT103 with ref {folio}");

                var response = await _httpClient.PostAsync(apiUrl, content);
                Config.EscribirLog($"INFO|EnqueueCommission|Respuesta del servidor: {response.ReasonPhrase}");

                if (response.IsSuccessStatusCode)
                {
                    return Result<bool>.Success(true);
                }

                var reason = await response.Content.ReadAsStringAsync();
                return Result<bool>.Failed(reason);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnqueueCommission|Error al encolar: {ex.Message}");
                return Result<bool>.Failed(ex.Message);
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Encargado de gestionar las operaciones de consulta relacionadas con la informacion de bancos
    /// </summary>
    public class BancoRepo : RepositorioMaestro, IBanco
    {
        /// <summary>
        /// Inicializa una nueva instancia de <see cref="BancoRepo"/>
        /// </summary>
        public BancoRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Consulta el nombre del banco a partir de su código SWIFT (CIF sender).
        /// Ejecuta un stored procedure para obtener la información del banco.
        /// Registra advertencias si no se encuentra ningún banco o si hay múltiples coincidencias.
        /// </summary>
        /// <param name="cifSender">Código SWIFT del banco a consultar.</param>
        /// <returns>
        /// Un objeto <see cref="CodSwtDTO"/> con la información del banco encontrado,
        /// o null si no existe coincidencia.
        /// </returns>
        public async Task<CodSwtDTO> ConsultaNombreBancoAsync(string cifSender)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<CodSwtDTO>(
                    ConstantesSql.USP_SWF_IN_ETL_OBTENER_NOMBRE_BANCO,
                    cmd =>
                    {
                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro("@cve_swift", cifSender)
                        );
                    });

                var bancosList = result?.Data?.ToList();
                var banco = bancosList?.LastOrDefault();

                if (banco == null)
                {
                    Config.EscribirLog($"WARN|ConsultaNombreBanco|No se encontró banco para: {cifSender}");
                    return null;
                }

                Config.EscribirLog($"INFO|ConsultaNombreBanco|Nombre del Banco: {banco?.des_nombre}");
                return banco;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConsultaNombreBanco|Error al consultar banco para: {cifSender}: {ex}");
                return null;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Encargado de consultar informacion de contrapartes
    /// </summary>
    public class CounterParty : RepositorioMaestro, ICounterParty
    {
        /// <summary>
        /// Inicializa una nueva instancia para <see cref="CounterParty"/>
        /// </summary>
        public CounterParty() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene el identificador de la contraparte (CounterPartyId) a partir de un BIC proporcionado.
        /// </summary>
        /// <param name="bicSanitizado">Código BIC de la institución financiera.</param>
        /// <returns>
        /// Retorna el identificador de la institución financiera si se encuentra;
        /// en caso contrario, devuelve -1.
        /// </returns>
        /// 
        public async Task<long> ObtenerCounterPartyIdAsync(string bicSanitizado)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<FinancialInstitutionIdentifiersDTO>(
                    ConstantesSql.USP_SWT_IN_ETL_GET_COUNTERPARTY_ID_BY_BIC,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@FinancialInstitutionIdentifier", bicSanitizado));
                    });

                if (result?.Data == null || result.Data.Count == 0)
                {
                    Config.EscribirLog($"INFO|CounterParty|No se encontró ID para BIC {bicSanitizado}");
                    return -1;
                }

                return result.Data[0].FinancialInstitutionId;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|CounterParty|Error al obtener ID para BIC {bicSanitizado}: {ex}");
                return -1;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    public class CuentaRepo : RepositorioMaestro, ICuenta
    {
        private readonly ConexionT24Mw _conexionT24Mw;

        public CuentaRepo(ConexionT24Mw conexionT24Mw)
           : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW)
        {
            _conexionT24Mw = conexionT24Mw;
        }

        /// <summary>
        /// Valida los datos de una cuenta proveniente de un mensaje SWIFT,
        /// extrayendo los valores numéricos, generando posibles combinaciones de cuentas
        /// y consultando su existencia en T24.
        /// </summary>
        /// <param name="cuenta">Cuenta en formato SWIFT que puede contener caracteres no numéricos.</param>
        /// <param name="moneda">Moneda asociada a la cuenta (ej. MXN).</param>
        /// <returns>
        /// Un objeto <see cref="Result{CustomerAccountDTO}"/>
        /// </returns>

        public async Task<Result<CustomerAccountDTO>> ValidaDatosCuentaVT24Async(string cuenta, string moneda)
        {
            try
            {
                var matches = Regex.Matches(cuenta ?? string.Empty, @"\d+");

                string cuentaNumerica = string.Concat(
                    matches.Cast<Match>()
                           .Select(m => m.Value));

                if (string.IsNullOrWhiteSpace(cuentaNumerica))
                {
                    return Result<CustomerAccountDTO>.Failed("Cuenta inválida");
                }

                var listaCuentas = new List<string>();
                bool ctaInterMXN;

                if (!string.Equals(
                        moneda,
                        ConstantesAux.MonedaMXN,
                        StringComparison.OrdinalIgnoreCase))
                {
                    if (cuentaNumerica.Length >= 7)
                    {
                        for (int i = 0; i <= cuentaNumerica.Length - 7; i++)
                        {
                            listaCuentas.Add(
                                cuentaNumerica.Substring(i, 7));
                        }
                    }
                    else
                    {
                        listaCuentas.Add(cuentaNumerica);
                    }

                    ctaInterMXN = false;
                }
                else
                {
                    listaCuentas.Add(cuentaNumerica);
                    ctaInterMXN = true;
                }

                string cuentasParam = string.Join(
                    ",",
                    listaCuentas
                        .Where(x => !string.IsNullOrWhiteSpace(x))
                        .Select(x => "'" + x + "'"));

                Config.EscribirLog("INFO|ValidaDatosCuentaVT24|Cuentas=" + cuentasParam);

                var resultado =
                    await EjecutarStoredProcedureQuery<CustomerAccountDTO>(
                        ConstantesSql.MUFG_MX2_VALIDA_DATOS_CUENTAS,
                        cmd =>
                        {
                            cmd.Parameters.Add(
                                basedatosConexion.CrearParametro(
                                    "@cuentas",
                                    cuentasParam));
                        },
                        esVistasT24: true);

                if (resultado == null || resultado.Data == null || !resultado.Data.Any())
                {
                    return Result<CustomerAccountDTO>.Failed("No se encontraron cuentas");
                }

                var row = resultado.Data.First();

                var dto = new CustomerAccountDTO
                {
                    ACCOUNT_NUMBER = row.ACCOUNT_NUMBER,
                    CUSTOMER = row.CUSTOMER,
                    ACCOUNT_TITLE_1 = row.ACCOUNT_TITLE_1,
                    CURRENCY = _conexionT24Mw.ValidaMoneda(row.CURRENCY),
                    EsCuentaIntermediaMXN = ctaInterMXN
                };

                return Result<CustomerAccountDTO>.Success(dto);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ValidaDatosCuentaVT24|" + ex);
                return Result<CustomerAccountDTO>.Failed("Error en validación de cuenta");
            }
        }

        /// <summary>
        /// Obtiene la información de una cuenta de cliente desde la vista VT24.
        /// </summary>
        /// <param name="cuenta">Número de cuenta a consultar.</param>
        /// <returns>
        /// Un objeto <see cref="CustomerAccountDTO"/> con los datos de la cuenta,
        /// </returns>
        /// 
        public async Task<CustomerAccountDTO> ObtenerCuentaVT24Async(string cuenta)
        {
            try
            {
                var dt = await _conexionT24Mw.DatosClienteVw(cuenta);

                if (dt == null || dt.Rows.Count == 0)
                    return null;

                var row = dt.Rows[0];

                return new CustomerAccountDTO
                {
                    ACCOUNT_NUMBER = row["ACCOUNT_NUMBER"]?.ToString(),
                    CUSTOMER = row["CUSTOMER"]?.ToString(),
                    ACCOUNT_TITLE_1 = row["ACCOUNT_TITLE_1"]?.ToString(),
                    CURRENCY = _conexionT24Mw.ValidaMoneda(row["CURRENCY"]?.ToString())
                };
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                   $"ERROR|DatosClienteCuentasVw|{ex}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza el flag de una cuenta en el sistema SWIFT mediante la ejecución de un procedimiento almacenado.
        /// </summary>
        /// <param name="icn">Folio  de la transacción.</param>
        /// <param name="claveMensaje">Clave del mensaje asociado a la operación.</param>
        /// <param name="mensajeError">Mensaje de error.</param>
        /// <returns>
        /// Devuelve <c>true</c> si la actualización se realizó correctamente; de lo contrario, <c>false</c>.
        /// </returns>
        public async Task<bool> ActualizaFlagCuentaAsync(string icn, string claveMensaje, string mensajeError)
        {
            try
            {
                Config.EscribirLog($"INFO|ActualizaFlagCuenta|Actualizar cuenta con ICN = {icn},TipoOperacion = {ConstantesServicio.TipoOperacion}, CveMensaje = {claveMensaje}");

                var resultado = await EjecutarStoredProceduredNonQuery(ConstantesSql.usp_SWF_UPDATE_FLAG_CTA,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@icn", icn));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", ConstantesServicio.TipoOperacion));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@err", mensajeError));
                    });

                return resultado.Exito;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizaFlagCuenta|{ex}");
                return false;
            }
        }

        /// <summary>
        /// Valida si una cuenta proveniente de Swift corresponde a una cuenta cerrada en T24.
        /// </summary>
        /// <param name="lsCuenta_Swift">Cuenta en formato Swift, puede contener caracteres alfanuméricos.</param>
        /// <param name="lsMoneda_Swift">Moneda asociada a la cuenta (por ejemplo: MXN, USD).</param>
        /// <returns>
        public async Task<bool> ExisteCuentaCerradaAsync(string lsCuenta_Swift, string lsMoneda_Swift)
        {
            try
            {
                var matches = Regex.Matches(lsCuenta_Swift ?? "", @"\d+");

                var lsCuentaNumerica = string.Concat(
                    matches.Cast<Match>().Select(m => m.Value)
                );

                if (string.IsNullOrWhiteSpace(lsCuentaNumerica))
                    return false;

                List<string> listaCuentas = new List<string>();

                if (!string.Equals(lsMoneda_Swift, ConstantesAux.MonedaMXN, StringComparison.OrdinalIgnoreCase))
                {
                    if (lsCuentaNumerica.Length >= 7)
                    {
                        for (int i = 0; i <= lsCuentaNumerica.Length - 7; i++)
                        {
                            listaCuentas.Add(lsCuentaNumerica.Substring(i, 7));
                        }
                    }
                    else
                    {
                        listaCuentas.Add(lsCuentaNumerica);
                    }
                }
                else
                {
                    listaCuentas.Add(lsCuentaNumerica);
                }

                var cuentasParam = string.Join(",", listaCuentas);

                Config.EscribirLog($"INFO|ExisteCuentaCerrada|Cuentas = {cuentasParam}");

                var resultado = await EjecutarStoredProcedureQuery<AccountClosedDTO>(
                          ConstantesSql.MUFG_MX2_CUENTA_CERRADA,
                          cmd =>
                          {
                              cmd.Parameters.Add(
                                  basedatosConexionVistasT24.CrearParametro("@cuentas", cuentasParam));
                          },
                          esVistasT24: true
                      );

                if (resultado?.Data?.Any() == true)
                {
                    var cuenta = resultado.Data.FirstOrDefault();

                    if (cuenta != null)
                    {
                        Config.EscribirLog($"INFO|ExisteCuentaCerrada|Se encontró la cuenta cerrada: {cuenta.ACCOUNT_NO}");
                        return true;
                    }
                }

                return false;

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ExisteCuentaCerrada|{ex}");
                return false;
            }
        }

        /// <summary>
        /// Obtiene los datos de un cliente a partir de su código.
        /// </summary>
        /// <param name="codigoCliente">Código único del cliente a consultar.</param>
        /// <returns>
        /// Un objeto <see cref="T24_CustomerDTO"/> con la información del cliente,
        /// </returns>
        /// <exception cref="ApplicationException">
        /// Se lanza cuando ocurre un error durante la ejecución del procedimiento almacenado.
        /// </exception
        public async Task<T24_CustomerDTO> ObtieneDatosClienteAsync(string codigoCliente)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<T24_CustomerDTO>(
                    ConstantesSql.MUFG_MX2_DATOS_CLIENTE,
                    cmd =>
                    {
                        cmd.Parameters.Add(
                            basedatosConexionVistasT24.CrearParametro("@customer", codigoCliente)
                        );
                    },
                    esVistasT24: true
                );

                return resultado?.Data?.FirstOrDefault();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtieneDatosCliente|{codigoCliente}|{ex}");
                throw new ApplicationException($"Error al obtener datos del cliente: {codigoCliente}", ex);
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Cuentas;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using Aplicacion_IN_ETL.Utilidades.Bic;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using Aplicacion_IN_ETL.Utilidades.Valores;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    public class IntegracionRepo : RepositorioMaestro, IIntegracion
    {
        private readonly ConexionT24Mw _conexionT24Mw;
        public IntegracionRepo(ConexionT24Mw conexionT24Mw) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _conexionT24Mw = conexionT24Mw;
        }

        /// <summary>
        /// Genera un folio único a partir del tipo de operación, fecha, folio base y longitud especificada.
        /// </summary>
        /// <param name="tipoOperacion">Tipo de operación que identifica el proceso.</param>
        /// <param name="fecha">Fecha utilizada como parte de la generación del folio.</param>
        /// <param name="icn">Folio base sobre el cual se construirá el folio único.</param>
        /// <param name="longitud">Longitud total deseada del folio generado.</param>
        /// <returns>Cadena que representa el folio único generado.</returns>
        /// <exception cref="Exception">Se lanza cuando ocurre un error durante la generación del folio.</exception>
        public async Task<string> GeneraFolioUnicoAsync(string tipoOperacion, string fecha, string folio, int longitud)
        {
            try
            {
                return await _conexionT24Mw.GeneraFolioUnico(tipoOperacion, fecha, folio, longitud);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|GeneraFolioUnico|{ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Acepta la operación SWIFT, registrando los datos asociados del cliente, cuenta y detalles de la transacción.
        /// </summary>
        /// <param name="icn">Identificador de la transacción (ICN).</param>
        /// <param name="folio">Folio único de la operación.</param>
        /// <param name="clave">Clave del mensaje SWIFT.</param>
        /// <param name="variablesXml">Información adicional (xml).</param>
        /// <param name="camposActualizar">Diccionario con los campos dinámicos a actualizar.</param>
        public async Task AceptaOperacionSwtAsync(string icn, string folio, string clave, string variablesXml, Dictionary<string, string> camposActualizar)
        {
            try
            {
                var result = await EjecutarStoredProceduredNonQuery(ConstantesSql.USP_SWF_QUALIFY_AUTO,
                cmd =>
               {
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@icn", icn));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", ConstantesServicio.TipoOperacion));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio_unico", folio));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_mensaje", clave));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@variables_xml", variablesXml));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cliente", GetValor.DameValor(camposActualizar, "num_cliente")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombre_cliente", GetValor.DameValor(camposActualizar, "nombre_cliente")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cdl", GetValor.DameValor(camposActualizar, "num_cdl")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cuenta", GetValor.DameValor(camposActualizar, "num_cuenta")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_moneda", GetValor.DameValor(camposActualizar, "cve_moneda")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@des_importe", GetValor.DameValor(camposActualizar, "des_importe")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@fec_valor", GetValor.DameValor(camposActualizar, "fec_valor")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cif_emisor", GetValor.DameValor(camposActualizar, "num_cif_emisor")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cta_vostro_emisor", GetValor.DameValor(camposActualizar, "cta_vostro_emisor")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_swift_emisor", GetValor.DameValor(camposActualizar, "cve_swift_emisor")));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@nombre_emisor", GetValor.DameValor(camposActualizar, "nombre_emisor")));
               });

                if (result?.Exito != true)
                {
                    throw new InvalidOperationException($"Error al aceptar operacion: {result?.MensajeError ?? "Resultado nulo"}");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|AceptaOperacionSwt|ICN:{icn}|Folio:{folio}|Clave:{clave}|{ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Inserta un registro en la cola Laika utilizando el ICN proporcionado.
        /// </summary>
        /// <param name="icn">Identificador único de la operación (ICN) que será enviado a la cola.</param>
        /// <returns>
        /// Un objeto <see cref="StoredProcedureResult"/> que contiene el resultado de la ejecución.
        public async Task InsertLaikaQueueAsync(string icn)
        {
            try
            {
                var result = await EjecutarStoredProceduredNonQuery(
                    ConstantesSql.USP_ENQUEUELAIKAHEAP,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@payload", icn));
                    });

                if (result?.Exito != true)
                {
                    Config.EscribirLog($"ERROR|InsertLaikaQueue|ICN:{icn}|{result?.MensajeError ?? "Resultado nulo"}");

                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|InsertLaikaQueue|ICN:{icn}|{ex}");
            }
        }

        /// <summary>
        /// Determina si se debe generar una solicitud de comisión saliente 
        /// en función del identificador del receptor y la moneda.
        /// </summary>
        /// <param name="receiverId">Identificador del receptor del mensaje.</param>
        /// <param name="currency">Código de la moneda asociada a la operación.</param>
        /// <exception cref="InvalidOperationException">
        /// Se lanza cuando no se puede recuperar la configuración de envío de comisión.
        /// </exception>
        public async Task<bool> OutgoingCommissionRequestShouldBeGeneratedAsync(long receiverId, string currency)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<OutgoingMessagesParametrizationDTO>(
                   ConstantesSql.USP_SWT_IN_ETL_OBTENER_SHOULD_SEND_COMMISSION,
                   cmd =>
                   {
                       cmd.Parameters.Add(
                           basedatosConexion.CrearParametro("@ReceiverId", receiverId));

                       cmd.Parameters.Add(
                           basedatosConexion.CrearParametro("@Currency", currency));
                   });

                var shouldSend = result?.Data?.FirstOrDefault();

                if (shouldSend == null)
                {
                    var mensajeError = "No se recuperó configuración ShouldSend.";
                    Config.EscribirLog($"WARN|OutgoingCommissionRequestShouldBeGenerated|{mensajeError}");
                    throw new InvalidOperationException(mensajeError);
                }
                else
                {
                    Config.EscribirLog(
                        $"INFO|OutgoingCommissionRequestShouldBeGenerated|ShouldSend recuperado: {shouldSend.ShouldSend}");
                }

                return shouldSend.ShouldSend;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OutgoingCommissionRequestShouldBeGenerated|ReceiverId:{receiverId}|{ex.Message}");
                throw;
            }
        }


        /// <summary>
        /// Obtiene el número de cuenta NOSTRO asociado a un BIC y moneda dados.
        /// </summary>
        /// <param name="bic">Código BIC de la institución financiera.</param>
        /// <param name="currency">Código de moneda (por ejemplo, USD, MXN).</param>
        /// <returns>
        /// Un objeto <see cref="Result{string}"/> que contiene:
        /// - El número de cuenta NOSTRO si existe.
        /// - Cadena vacía si no se encuentra información.
        /// - Un error si la operación falla.
        /// </returns>
        /// <exception cref="Exception">
        /// No lanza excepciones directamente; en caso de error, se registra en log y se
        /// retorna un resultado fallido.
        /// </exception>
        public async Task<Result<string>> GetNostroAsync(string bic, string currency)
        {
            if (string.IsNullOrWhiteSpace(bic))
            {
                return Result<string>.Failed("No se suplió un BIC Beneficiario, posible error en registro del beneficiario.");
            }

            Config.EscribirLog($"INFO|GetNostro|A sanitizar el BIC: {bic}");
            var bicSanitizado = SanitizeBic11.SanitizeBic11Helper(bic);
            Config.EscribirLog($"INFO|GetNostro|BIC sanitizado: {bicSanitizado}");
            Config.EscribirLog($"INFO|GetNostro|A buscar la cuenta de crédito para {bicSanitizado} en la moneda: {currency}");
          
            try
            {
                var result = await EjecutarStoredProcedureQuery<NostroAccountsDTO>(
                    ConstantesSql.USP_SWT_IN_ETL_NOSTRO_ACCOUNTS,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@bic", bicSanitizado));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@currency", currency));
                    });

                if (result?.Exito != true)
                {
                    return Result<string>.Failed("No fue posible hacer la consulta al cátalogo de cuentas NOSTRO.");
                }

                var dto = result.Data?.FirstOrDefault();
                if (dto == null || string.IsNullOrWhiteSpace(dto.NostroAccountNumber))
                {
                    return Result<string>.Success(string.Empty);
                }

                return Result<string>.Success(dto.NostroAccountNumber);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|GetNostro|{ex.Message}");
                return Result<string>.Failed("Error consultando cuentas NOSTRO.");
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Contexto;
using Aplicacion_IN_ETL.DTO.Swf;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Utilidades.Gets;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Repositorio encargado de procesar la informacion del encabezado de un mensaje, realizando su validacion, enriquecimiento y mapeo.
    /// </summary>
    public class LeerHeaderRepo : RepositorioMaestro, ILeerHeader
    {
        private readonly IBanco _consultaNombreBanco;
        private readonly ISwift _swEnd2End;

        /// <summary>
        /// Inicializa una nueva instancia de <see cref="LeerHeaderRepo"/>
        /// </summary>
        /// <param name="consultaNombreBanco">Servicio utilizado para obtener el banco</param>
        /// <param name="swEnd2End">Gestion y validacion de referencias .EndToEnd</param>
        public LeerHeaderRepo(IBanco consultaNombreBanco, ISwift swEnd2End) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _consultaNombreBanco = consultaNombreBanco;
            _swEnd2End = swEnd2End;
        }

        /// <summary>
        /// Lee y procesa la informacion del encabezado del mensaje
        /// </summary>
        /// <param name="xml">Contexto para el documento XML</param>
        public async Task<bool> LeerHeaderAsync(ContextoXmlDTO xml)
        {
            try
            {
                var mensaje = xml.Mensaje;
                var header = ParseHeader(mensaje);

                if (!ValidarHeader(header, xml))
                    return false;

                await EnriquecerHeaderAsync(header);

                if (!await ValidarUetrAsync(header, xml))
                    return false;

                MapearAContexto(header, xml);
                ConstruirXml(header, xml);

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|LeerHeader|{ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Analiza el contenido del mensaje y obtiene la informacion principal del encabezado.
        /// </summary>
        /// <param name="mensaje">Mensaje que recibira el tratamiento</param>
        /// <returns></returns>
        private HeaderDTO ParseHeader(string mensaje)
        {
            var header = new HeaderDTO();
            header.ICN = GetSubstring.GetSafeSubstring(mensaje, 0, 17);

            int idx2 = mensaje.IndexOf("{2:", StringComparison.Ordinal);
            header.Tipo = idx2 >= 0
                ? GetSubstring.GetSafeSubstring(mensaje, idx2 + 4, 3)
                : string.Empty;

            bool esMT191 = header.Tipo == ConstantesCategoriaMT.Header191;
            string tag = esMT191 ? ":32B:" : ":32A:";

            int currencyIndex = mensaje.IndexOf(tag, StringComparison.Ordinal);

            if (currencyIndex >= 0)
            {
                int start = currencyIndex + tag.Length;
                int end = mensaje.IndexOf("\r\n", start, StringComparison.Ordinal);

                if (end > start)
                {
                    string raw = mensaje.Substring(start, end - start);

                    if (!esMT191 && raw.Length >= 11)
                    {
                        header.Moneda = raw.Substring(6, 3);
                        header.Importe = ParseDecimal(raw.Substring(9));
                    }
                    else if (esMT191 && raw.Length >= 3)
                    {
                        header.Moneda = raw.Substring(0, 3);
                        header.Importe = ParseDecimal(raw.Substring(3));
                    }
                }
            }

            int uetrIndex = mensaje.IndexOf("{121:", StringComparison.Ordinal);
            if (uetrIndex >= 0)
                header.Uetr = GetSubstring.GetSafeSubstring(mensaje, uetrIndex + 5, 36);

            header.CifSender = idx2 >= 0
                ? GetSubstring.GetSafeSubstring(mensaje, idx2 + 17, 8)
                : string.Empty;

            int idx1 = mensaje.IndexOf("{1:F01", StringComparison.Ordinal);
            header.BancoReceptor = idx1 >= 0
                ? GetSubstring.GetSafeSubstring(mensaje, idx1 + 6, 8)
                : string.Empty;

            if (header.Tipo == ConstantesCategoriaMT.Header202 && mensaje.Contains("{119:COV"))
                header.Tipo = ConstantesCategoriaMT.Header202COV;

            return header;
        }

        /// <summary>
        /// Completa la informacion del encabezado obteniendo el nombre del banco emisor asociado al CIF del remitente.
        /// </summary>
        private async Task EnriquecerHeaderAsync(HeaderDTO header)
        {
            var banco = await _consultaNombreBanco.ConsultaNombreBancoAsync(header.CifSender);
            header.BancoEmisor = banco != null
                ? banco.des_nombre : string.Empty;
        }


        /// <summary>
        /// Valida que la informacion obligatoria del encabezado  se encuentre presente y sea consistente.
        /// </summary>
        private bool ValidarHeader(HeaderDTO header, ContextoXmlDTO xml)
        {
            if (string.IsNullOrEmpty(header.Tipo))
            {
                xml.RazonRechazo = "No se pudo leer el tipo.";
                return false;
            }

            if (header.Moneda == null)
            {
                xml.RazonRechazo = "No se pudo leer la moneda.";
                return false;
            }

            if (!header.Importe.HasValue)
            {
                xml.RazonRechazo = "No se pudo leer el importe.";
                return false;
            }

            return true;
        }

        /// <summary>
        /// Valida que el UETR del mensaje no se encuentre previamente registrado para evitar el procesamiento duplicado de operaciones.
        /// </summary>
        private async Task<bool> ValidarUetrAsync(HeaderDTO header, ContextoXmlDTO xml)
        {
            if (header.Tipo != ConstantesCategoriaMT.Header103 &&
                header.Tipo != ConstantesCategoriaMT.Header202 &&
                header.Tipo != ConstantesCategoriaMT.Header202COV)
                return true;

            if (string.IsNullOrEmpty(header.Uetr))
                return true;

            var existing = await _swEnd2End.BuscaSwEnd2EndRef(new SwEnd2EndDTO
            {
                Uetr = header.Uetr,
                TipoOperacion = ConstantesServicio.TipoOperacion,
                FolioSwift = header.ICN
            });

            if (!string.IsNullOrEmpty(existing))
            {
                xml.RazonRechazo = "UETR duplicado";
                return false;
            }

            await _swEnd2End.InsertaSwEnd2EndRef(new SwEnd2EndDTO
            {
                Uetr = header.Uetr,
                TipoOperacion = ConstantesServicio.TipoOperacion,
                FolioSwift = header.ICN
            });

            return true;
        }

        /// <summary>
        /// Transfiere la informacion relevante del encabezado del mensaje al contexto de procesamiento
        /// </summary>

        private void MapearAContexto(HeaderDTO header, ContextoXmlDTO xml)
        {
            xml.TipoMensaje = ConstantesCategoriaMT.PrefijoMT + header.Tipo;
            xml.Moneda = header.Tipo == ConstantesCategoriaMT.Header940 ? string.Empty : header.Moneda;

            if (header.Importe.HasValue)
                xml.Importe = header.Importe.Value;
        }

        /// <summary>
        /// Agrega los elementos obtenidos al documento XML
        /// </summary>
        private void ConstruirXml(HeaderDTO header, ContextoXmlDTO xml)
        {
            var doc = xml.XmlDoc;
            XmlHelper.AgregarElemento(doc, ConstantesXml.ICN, header.ICN);
            XmlHelper.AgregarElemento(doc, ConstantesXml.UETR, header.Uetr);
            XmlHelper.AgregarElemento(doc, ConstantesXml.bancoReceptor, header.BancoReceptor);
            XmlHelper.AgregarElemento(doc, ConstantesXml.bancoEmisor, header.BancoEmisor);
            XmlHelper.AgregarElemento(doc, ConstantesXml.CIFSender, header.CifSender);
            XmlHelper.AgregarElemento(doc, ConstantesXml.mensajeTipo, xml.TipoMensaje);
            XmlHelper.AgregarElemento(doc, ConstantesXml.mensajeMoneda, xml.Moneda);

            XmlHelper.AgregarElemento(doc, ConstantesXml.mensajeImporte, 
                xml.Importe.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Convierte una representacion textual del importe a una valor decimal.
        /// </summary>
        private decimal? ParseDecimal(string input)
        {
            if (string.IsNullOrEmpty(input))
                return null;

            input = input.Replace(",", ".");

            try
            {
                return ConversionesComunes.ParseDecimalRequerido(input, ConstantesXml.mensajeImporte);
            }
            catch
            {
                return null;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    public class LegalEntitiesRepo : RepositorioMaestro, ILegalEntities
    {
        public LegalEntitiesRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene el catálogo de entidades legales.
        /// </summary>
        /// <returns>
        /// Una colección de objetos <see cref="LegalEntitiesDto"/> que representa las entidades legales disponibles.
        ///
        public async Task<IEnumerable<LegalEntitiesDto>> ObtieneLegalEntitiesAsync()
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<LegalEntitiesDto>(ConstantesSql.USP_SWT_IN_ETL_LEGAL_ENTITIES, cmd => { });
                return resultado?.Data;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtieneLegalEntities|{ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Interfaz de Repositorio encargado de obtener la informacion de las cuentas NOSTRO
    /// </summary>
    public class NostroRepo : IRepositorioNostro
    {
        private readonly ConexionT24Mw _conexion;

        /// <summary>
        /// Inicializa una nueva instancia de <see cref="NostroRepo"/>
        /// </summary>
        /// <param name="conexion">Conexion a Vistat24</param>
        public NostroRepo(ConexionT24Mw conexion)
        {
            _conexion = conexion;
        }

        /// <summary>
        /// Obtiene la informacion de las cuentas NOSTRO disponibles
        /// </summary>
        /// <returns></returns>
        public async Task<List<OperacionesEntidad>> ObtieneDatosCuentasNostro()
        {
            return await _conexion.ObtieneDatosCuentasNostro();
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Operaciones;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    public class Operaciones : RepositorioMaestro, IOperaciones
    {
        public Operaciones() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene las operaciones que se van a procesar (NF_FOLIO y NF_MQ_MSG).
        /// </summary>
        /// /// <param name="operacion">Operación: SWIFT_IN.</param>
        /// <returns>
        /// Una colección de objetos <see cref="OperacionesDTO"/> que representa las operaciones.
        /// 
        public async Task<IEnumerable<OperacionesDTO>> ObtieneOperacionesAsync(string operacion)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<OperacionesDTO>(
                     ConstantesSql.USP_SWF_IN_ETL_OPERACIONES,
                 cmd =>
                 {
                     cmd.Parameters.Add(
                         basedatosConexion.CrearParametro("@tipo_operacion", operacion)
                     );
                 });

                return resultado?.Data ?? Enumerable.Empty<OperacionesDTO>();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtieneOperaciones|Operacion:{operacion}|{ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene las operaciones que se van a procesar (NF_FOLIO y NF_MQ_MSG).
        /// </summary>
        /// /// <param name="operacion">Operación: SWIFT_IN.</param>
        /// <returns>
        /// Una colección de objetos <see cref="OperacionesDTO"/> que representa las operaciones.
        /// 
        public async Task<IEnumerable<OperacionesDTO>> ObtieneOperacionesMXAsync(string operacion)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<OperacionesDTO>(
              ConstantesSql.USP_SWF_ETL_OPS_MX,
              cmd =>
              {
                  cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", operacion)
                  );
              });

                return resultado?.Data ?? Enumerable.Empty<OperacionesDTO>();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtieneOperacionesMX|Operacion:{operacion}|{ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene la información de una operación en T24 a partir de su referencia.
        /// </summary>
        /// <param name="reference">Referencia de la operación en T24.</param>
        /// <returns>
        /// Un objeto <see cref="Result{OperacionInfoDTO}"/> que contiene la informacíon de la operación.
        /// 
        public async Task<Result<OperacionInfoDTO>> ObtenerOperacionPorReferenciaT24Async(string reference)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<OperacionInfoDTO>(
                     ConstantesSql.USP_SWT_IN_ETL_OBTENER_OPERACION_POR_REFERENCIA_T24,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@reference", reference));
                    });

                var data = resultado?.Data?.FirstOrDefault();

                if (data == null)
                {
                    return Result<OperacionInfoDTO>.Failed(
                        $"No se encontró una operación con la referencia {reference}");
                }

                return Result<OperacionInfoDTO>.Success(data);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerOperacion|{ex.Message}");
                return Result<OperacionInfoDTO>.Failed("Error al obtener la operación.");
            }
        }

        /// <summary>
        /// Verifica si existe una comisión en proceso para la combinación de referencias proporcionadas.
        /// </summary>
        /// <param name="ourRef">Referencia interna de la operación (Our Reference).</param>
        /// <param name="theirRef">Referencia externa de la operación (Their Reference).</param>
        /// <exception cref="Exception">
        /// Se lanza cuando ocurre un error durante la ejecución del procedimiento almacenado.
        /// </exception
        /// 
        public async Task<bool> ExisteComisionEnProcesoAsync(string ourRef, string theirRef)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<object>(
                    ConstantesSql.USP_SWF_IN_ETL_EXISTE_COMISION_EN_PROCESO,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@our_ref", ourRef));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@their_ref", theirRef));
                    });

                return resultado?.Data?.Any() ?? false;
            }

            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerOperacion|{ex.Message}");
                return false;
            }
        }
    }
}

using Aplicacion_IN_ETL.DTO;
using Aplicacion_IN_ETL.Enums;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static Aplicacion_IN_ETL.Utilidades.Parametros.ParametrosHelper;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Este metodo devolvera las credenciales a partir de los parametros de sistema obtenidos./>,
    /// </summary>
    public class ProveedorCredenciales : IProveedorCredenciales
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Inicializa una nueva instancia de <see cref="ProveedorCredenciales"/>
        /// </summary>
        public ProveedorCredenciales(Configuracion configuracion)
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Obtiene un objeto con los valores que se obtienen de Configuracion sistema
        /// </summary>
        /// <returns></returns>
        public CredencialesDTO Obtener()
        {
            return new CredencialesDTO
            {
                WST24Company = ConfiguracionHelper.GetString(
                    _configuracion.ParametrosSistema,
                    ConfiguracionSistema.WST24_MW_COMPANY),

                WST24Username = ConfiguracionHelper.GetString(
                    _configuracion.ParametrosSistema,
                    ConfiguracionSistema.WST24_MW_USERNAME),

                WST24Psw = ConfiguracionHelper.GetString(
                    _configuracion.ParametrosSistema,
                    ConfiguracionSistema.WST24_MW_PASSWORD)
            };
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    public class ReglasRepo : RepositorioMaestro, IReglas
    {
        public ReglasRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Ejecuta el procedimiento almacenado encargado de aplicar las reglas de negocio
        /// para una operación específica, utilizando el identificador ICN, tipo de operación
        /// y clave de mensaje.
        /// </summary>
        /// <param name="icn">Identificador único de la operación.</param>
        /// <param name="tipoOperacion">Tipo de operación que se desea procesar.</param>
        /// <param name="claveMensaje">Clave del mensaje asociada a la operación.</param>
        /// <returns>
        /// Un objeto <see cref="StoredProcedureResult"/> que contiene el resultado de la ejecución,
        /// incluyendo el número de filas afectadas, mensaje de error en caso de existir y la excepción.
        /// </returns>
        public async Task<StoredProcedureResult> AplicarReglasAsync(string icn, string tipoOperacion, string claveMensaje)
        {
            try
            {
                var result = await EjecutarStoredProceduredNonQuery(
                    ConstantesSql.SP_APLICA_REGLAS,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@icn", icn));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_mensaje", claveMensaje));
                    });

                return result;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|AplicarReglas|ICN:{icn}|{ex.Message}");

                return new StoredProcedureResult
                {
                    FilasAfectadas = 0,
                    MensajeError = ex.Message,
                    Exception = ex
                };
            }
        }

        /// <summary>
        /// Ejecuta el proceso de rechazo de una operación en el sistema SWIFT mediante la ejecución
        /// de un stored procedure, registrando la razón del rechazo, tipo de operación, tipo de mensaje,
        /// moneda, importe y fecha valor.
        /// </summary>
        /// <param name="icn">Folio de la operación (ICN).</param>
        /// <param name="tipoOperacion">Tipo de operación asociada a la transacción.</param>
        /// <param name="razonRechazo">Descripción o motivo del rechazo de la operación.</param>
        /// <param name="tipoMensaje">Clave o tipo de mensaje SWIFT asociado a la operación.</param>
        /// <param name="moneda">Código de la moneda de la operación.</param>
        /// <param name="importe">Importe de la operación.</param>
        /// <param name="fechaValor">Fecha valor de la operación (opcional).</param>
        /// <returns>
        /// Un objeto <see cref="StoredProcedureResult"/> que contiene el resultado de la ejecución del
        /// procedimiento almacenado, incluyendo número de filas afectadas, mensaje de error en caso de fallo
        /// y la excepción capturada.
        /// </returns>
        public async Task<StoredProcedureResult> RechazarOperacionAsync(string icn, string tipoOperacion, string razonRechazo, string tipoMensaje, string moneda, decimal importe, int? fechaValor)
        {
            try
            {
                var result = await EjecutarStoredProceduredNonQuery(ConstantesSql.SP_SWF_QUALIFY_MANUAL,
                    cmd =>
               {
                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@icn", icn));
                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion));
                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@razon_rechazo", razonRechazo));
                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@cve_mensaje", tipoMensaje ?? string.Empty));
                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@cve_moneda", moneda ?? string.Empty));
                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@des_importe", importe));
                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@fec_valor", (object)fechaValor ?? DBNull.Value));
               });

                return result;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                   $"ERROR|RechazarOperacion|ICN:{icn}|{ex.Message}");

                return new StoredProcedureResult
                {
                    FilasAfectadas = 0,
                    MensajeError = ex.Message,
                    Exception = ex
                };
            }
        }

        /// <summary>
        /// Marca una operación como errónea en el sistema, registrando el mensaje de error asociado.
        /// </summary>
        /// <param name="icn">Folio de la operación.</param>
        /// <param name="tipoOperacion">Tipo de operación que se está procesando(SWIFT_IN).</param>
        /// <param name="mensajeError">Descripción del error ocurrido durante el procesamiento.</param>
        /// <returns>
        /// Un objeto <see cref="StoredProcedureResult"/> que contiene el resultado de la ejecución,
        /// incluyendo el número de filas afectadas y cualquier mensaje de error.
        /// </returns>
        public async Task<StoredProcedureResult> MarcarOperacionErrorAsync(string icn, string tipoOperacion, string mensajeError)
        {
            try
            {
                var result = await EjecutarStoredProceduredNonQuery(
                   ConstantesSql.USP_SWF_MARCA_OP_ERROR,
                   cmd =>
                   {
                       cmd.Parameters.Add(
                           basedatosConexion.CrearParametro("@icn", icn));

                       cmd.Parameters.Add(
                           basedatosConexion.CrearParametro("@tipo_operacion", tipoOperacion));

                       cmd.Parameters.Add(
                           basedatosConexion.CrearParametro("@des_error", mensajeError));
                   });

                return result;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                  $"ERROR|MarcarOperacionError|ICN:{icn}|{ex.Message}");

                return new StoredProcedureResult
                {
                    FilasAfectadas = 0,
                    MensajeError = ex.Message,
                    Exception = ex
                };
            }
        }
    }
}

using Aplicacion_IN_ETL.Enums;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static Aplicacion_IN_ETL.Utilidades.Parametros.ParametrosHelper;

namespace Infraestructura_IN_ETL.Repositorios
{
    public class RequestApiRejection : IRequestApiRejection
    {
        private readonly Configuracion _configuracion;
        private readonly ILlamadaAPI _llamadaAPI;

        public RequestApiRejection(Configuracion configuracion, ILlamadaAPI llamadaAPI)
        {
            _configuracion = configuracion;
            _llamadaAPI = llamadaAPI;
        }

        public async Task<bool> RequestApiRejectionAsync(string samhRfk, string senderReference, string receivingBank, string messageType, string currency, string amount, string reason, string errorApi)
        {
            try
            {

                var tokenUrl = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.API_REJECTION_TOKEN_URL);
                var apiUrl = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.API_REJECTION_API_URL);

                var credenciales = new
                {
                    userLogin = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.API_REJECTION_USER),
                    userPass = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.API_REJECTION_PASSWORD)
                };

                Config.EscribirLog($"INFO|RequestApiRejection|Request Token URL: {tokenUrl}");
                Config.EscribirLog($"INFO|RequestApiRejection|Request Api URL: {apiUrl}");
                Config.EscribirLog($"INFO|RequestApiRejection|Request Api User: {credenciales.userLogin}");
                Config.EscribirLog($"INFO|RequestApiRejection|samhRfk: {samhRfk}");
                Config.EscribirLog($"INFO|RequestApiRejection|senderReference: {senderReference}");
                Config.EscribirLog($"INFO|RequestApiRejection|receivingBank: {receivingBank}");
                Config.EscribirLog($"INFO|RequestApiRejection|valueDate: {messageType}");
                Config.EscribirLog($"INFO|RequestApiRejection|messageType: {DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.fffK")}");
                Config.EscribirLog($"INFO|RequestApiRejection|currency: {currency}");
                Config.EscribirLog($"INFO|RequestApiRejection|amount: {amount}");
                Config.EscribirLog($"INFO|RequestApiRejection|reason: {reason}");

                var json = CreaJson(samhRfk, senderReference, receivingBank, messageType, currency, amount, reason);

                var token = await ObtenerTokenAsync(tokenUrl, credenciales);

                if (string.IsNullOrEmpty(token))
                {
                    Config.EscribirLog("ERROR|RequestApiRejection|No se pudo obtener el token.");
                    return false;
                }

                Config.EscribirLog($"INFO|RequestApiRejection|Token obtenido: {token}");
                string responseTask = await _llamadaAPI.CallApiPostMethod(apiUrl, token, JsonConvert.SerializeObject(json));
                Config.EscribirLog($"INFO|RequestApiRejection|Respuesta exitosa: {responseTask}");
                return true;

            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|RequestApiRejection|No pudo ser completada la solicitud al API REJECTION.");
                Config.EscribirLog($"ERROR|RequestApiRejection|{ex.Message}");
                return false;
            }
        }

      
        private async Task<string> ObtenerTokenAsync(string tokenUrl, object credenciales)
        {
            try
            {
                var json = JsonConvert.SerializeObject(credenciales);
                return await _llamadaAPI.GetTokenAsync(tokenUrl, json);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerToken|{ex.Message}");
                throw;
            }
        }


        private object CreaJson(string samhRfk, string senderReference, string receivingBank, string messageType, string currency, string amount, string reason)
        {
            return new
            {
                samhRfk,
                senderReference,
                receivingBank,
                messageType,
                valueDate = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                currency,
                amount,
                reason
            };
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.SintaxisMT;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Repositorio encargado de consultar y proporcionar las definiciones de sintaxis utilizadas durante el procesamiento 
    /// y validacion de los mensajes MT.
    /// </summary>
    public class SintaxisRepo : RepositorioMaestro, ISintaxisMT
    {
        /// <summary>
        /// Inicializa una nueva instancia de <see cref="SintaxisRepo"/>
        /// </summary>
        public SintaxisRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene la configuración de sintaxis MT desde base de datos a partir de la clave de mensaje,
        /// lista de tags y el criterio de búsqueda indicado.
        /// Ejecuta un stored procedure que devuelve una colección de definiciones de sintaxis,
        /// las cuales son agrupadas por tag y transformadas en un diccionario para facilitar su acceso.
        /// </summary>
        /// <param name="claveMensaje">Clave del tipo de mensaje (MT) a consultar.</param>
        /// <param name="tags">Lista de tags que se desean evaluar dentro de la sintaxis.</param>
        /// <param name="criterioCompleto">Indica si la búsqueda debe considerar criterios completos.</param>
        /// <returns>
        public async Task<Dictionary<string, SintaxisMtDTO>> ObtenerSintaxisMtAsync(string claveMensaje, string tags, bool criterioCompleto)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<SintaxisMtDTO>(
               ConstantesSql.USP_SWF_IN_ETL_FIND_TYPE,
               cmd =>
               {
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", ConstantesServicio.TipoOperacion));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_mensaje", claveMensaje));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@tags", tags));
                   cmd.Parameters.Add(basedatosConexion.CrearParametro("@completo", criterioCompleto));
               });

                var lista = result?.Data?.ToList() ?? new List<SintaxisMtDTO>();

                var diccionario = lista
                    .Where(x => !string.IsNullOrWhiteSpace(x.cve_tag))
                    .GroupBy(x => x.cve_tag)
                    .ToDictionary(g => g.Key, g => g.First());

                return diccionario;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerSintaxisMt|{ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    public class SpeiRepo : RepositorioMaestro, ISpei
    {
        public SpeiRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene la información de un CIF SPEI a partir de un identificador y el tipo de mensaje.
        /// Dependiendo de la clave de mensaje (PACS009 o MT202), consulta por clave SWIFT o número de CIF.
        /// Ejecuta un procedimiento almacenado para recuperar los datos y retorna el primer resultado encontrado.
        /// </summary>
        public async Task<CatCifSpeiDTO> ObtenerCifSpeiAsync(string lsCIF_SPEI, string claveMensaje)
        {
            try
            {
                object numCif;
                object cveSwift;

                if (claveMensaje == ConstantesPACS.PACS009 || claveMensaje == ConstantesCategoriaMT.MT202)
                {
                    numCif = DBNull.Value;
                    cveSwift = lsCIF_SPEI;
                }
                else
                {
                    numCif = lsCIF_SPEI;
                    cveSwift = DBNull.Value;
                }

                var resultado = await EjecutarStoredProcedureQuery<CatCifSpeiDTO>(
                    ConstantesSql.USP_SWF_GET_CIF_SPEI,
                    cmd =>
                    {
                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro("@num_cif", numCif));

                        cmd.Parameters.Add(
                            basedatosConexion.CrearParametro("@cve_swift", cveSwift));
                    });

                return resultado.Data?.FirstOrDefault();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerCifSpei|{ex}");
                throw;
            }
        }

        public async Task<CatCifSpeiDTO> ObtenerCifSpeiAsync2(string lsCIF_SPEI)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<CatCifSpeiDTO>(
                 ConstantesSql.USP_SWF_GET_CIF_SPEI,
                 cmd =>
                 {
                     cmd.Parameters.Add(
                         basedatosConexion.CrearParametro("@num_cif", DBNull.Value));
                
                     cmd.Parameters.Add(
                         basedatosConexion.CrearParametro("@cve_swift", lsCIF_SPEI));
                 });

                return resultado.Data?.FirstOrDefault();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerCifSpei|{ex}");
                throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTO.Swf;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    public class SwiftRepo : RepositorioMaestro, ISwift
    {

        public SwiftRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Consulta el identificador SW End-to-End (sw_end2end_ref) a partir de distintos criterios de búsqueda
        /// como UETR, referencia T24, folio único y otros datos operativos.
        /// Ejecuta un procedimiento almacenado que retorna el valor mediante un parámetro de salida.
        /// </summary>
        public async Task<string> BuscaSwEnd2EndRef(SwEnd2EndDTO query)
        {
            if (string.IsNullOrWhiteSpace(query.Uetr))
                throw new ArgumentException("UETR es requerido");
            try
            {
                IDbDataParameter outputParam = basedatosConexion.CrearParametroSalida("@sw_end2end_ref_ret", DbType.String, 36);

                await EjecutarStoredProcedureQuery<OperacionesEntidad>(
                    EnumNombreSp.SP_SWF_BUSCA_SW_END2END_REF,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@sw_end2end_ref", query.Uetr, 36));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@referenciaT24", query.ReferenciaT24, 35));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio_unico", query.FolioUnico, 35));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", query.TipoOperacion, 10));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio_operacion", query.FolioOperacion, 25));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@swift_in_out", query.SwiftInOut, 10));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio_swift", query.FolioSwift, 25));
                        cmd.Parameters.Add(outputParam);

                    });

                var result = outputParam?.Value?.ToString();

                return string.IsNullOrWhiteSpace(result) ? null : result;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|BuscaSwEnd2EndRef|{ex.Message}");
                throw;
            }

        }

        /// <summary>
        /// Inserta un registro de relación SW End-to-End en base de datos a partir de la información proporcionada.
        /// Valida que el identificador UETR sea obligatorio antes de ejecutar el procedimiento almacenado.
        public async Task<bool> InsertaSwEnd2EndRef(SwEnd2EndDTO query)
        {
            if (string.IsNullOrWhiteSpace(query.Uetr))
                throw new ArgumentException("UETR es requerido");

            try
            {
                await EjecutarStoredProcedureQuery<SwEnd2EndDTO>(
                    EnumNombreSp.SP_SWF_INSERTA_SW_END2END_REF,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@sw_end2end_ref", query.Uetr, 36));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@sla_uetr", query.SlaUetr ?? string.Empty, 3));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@referenciaT24", query.ReferenciaT24 ?? string.Empty, 35));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio_unico", query.FolioUnico ?? string.Empty, 35));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipo_operacion", query.TipoOperacion ?? string.Empty, 10));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio_operacion", query.FolioOperacion ?? string.Empty, 25));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@swift_in_out", query.SwiftInOut ?? string.Empty, 10));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio_swift", query.FolioSwift ?? string.Empty, 25));
                    });

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|InsertaSwEnd2EndRef|{ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Obtiene el mapeo de campos SWIFT para un tipo de mensaje específico,
        /// consultando la configuración almacenada en base de datos.
        /// </summary>
        /// <param name="claveMensaje">
        /// Clave del mensaje SWIFT o PACS para el cual se requiere el mapeo de campos.
        /// </param>
        /// <returns>
        public async Task<IEnumerable<SwiftFieldMappingDTO>> ObtenerMapeoCamposAsync(string claveMensaje)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<SwiftFieldMappingDTO>(
               ConstantesSql.usp_swf_in_etl_map_fields,
               cmd =>
               {
                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@tipo_operacion", ConstantesServicio.TipoOperacion));

                   cmd.Parameters.Add(
                       basedatosConexion.CrearParametro("@cve_mensaje", claveMensaje));
               });

                return resultado?.Data ?? Enumerable.Empty<SwiftFieldMappingDTO>();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerMapeoCampos|{ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion_IN_ETL.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using T24CommissionAcct;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Cliente encargado de consultar informacion de comisiones por cliente a traves de los servicios expuestos por t24
    /// </summary>
    public class T24Client : IT24Client
    {
        /// <summary>
        /// Consulta las comisiones asociadas a un cliente mediante el servicio de T24.
        /// </summary>
        /// <param name="request">Datos necesarios para realizar la consulta.</param>
        /// <returns></returns>
        public async Task<ConsultaComisionesporClienteResponse> ConsultaComisionesporClienteAsync(
            ConsultaComisionesporClienteRequest request)
        {
            var client = new T24CommissionAcct.T24WebServicesImplClient();
            return await client.ConsultaComisionesporClienteAsync(request);
        }
    }
}

using AccountRestrictionValidator;
using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Linq;
using System.Threading.Tasks;
using T24CommissionAcct;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Servicio responsable de consultar en t24 la cuenta de cargo de comisiones asociada a un cliente para su utilizacion
    /// duante el procesamiento de operaciones.
    /// </summary>
    public class T24CommissionServiceRepo : IT24CommissionService
    {
        private readonly IProveedorCredenciales _proveedorCredenciales;
        private readonly IT24Client _t24Client;

        /// <summary>
        /// Inicializa una nueva instancia de <see cref="T24CommissionServiceRepo"/>
        /// </summary>
        public T24CommissionServiceRepo(IProveedorCredenciales proveedorCredenciales, IT24Client t24Client)
        {
            _proveedorCredenciales = proveedorCredenciales;
            _t24Client = t24Client;
        }

        /// <summary>
        /// Obtiene la cuenta de cargo de comisiones asociada a un cliente en T24
        /// a partir de su identificador CDL.
        /// </summary>
        /// <param name="cdl">Identificador único del cliente (CDL).</param>
        /// <returns>
        /// Un objeto <see cref="Result{ChargeAccountInfo}"/> que contiene la información
        /// de la cuenta de cargo si la operación es exitosa.
        /// </returns>
        /// 
        public async Task<Result<ChargeAccountInfo>> GetCommissionAccountAsync(string cdl)
        {
            try
            {
                Config.EscribirLog("INFO|GetCommissionAccount|Se inicia petición al WS T24 para obtener la cuenta de cobro.");

                var creds = _proveedorCredenciales.Obtener();

                if (creds == null)
                {
                    return Result<ChargeAccountInfo>.Failed(
                        "No fue posible obtener las credenciales para consultar T24.");
                }

                var request = new ConsultaComisionesporClienteRequest
                {
                    WebRequestCommon = new T24CommissionAcct.webRequestCommon
                    {
                        company = creds.WST24Company,
                        userName = creds.WST24Username,
                        password = creds.WST24Psw
                    },

                    MXMBBTMHCUCOMSCHELISTType = new[]
                    {
                    new T24CommissionAcct.enquiryInputCollection
                      {
                        columnName = "@ID",
                        criteriaValue = cdl,
                        operand = "EQ"
                      }
                    }
                };

                var response = await _t24Client.ConsultaComisionesporClienteAsync(request);

                if (response?.Status == null)
                {
                    return Result<ChargeAccountInfo>.Failed(
                        "T24 devolvió una respuesta inválida al consultar la cuenta cargo.");
                }

                var status = response.Status;

                if (status.successIndicator != T24CommissionAcct.successIndicator.Success)
                {
                    Config.EscribirLog($"ERROR|GetCommissionAccount|Error en WS T24: {status.successIndicator}");

                    return Result<ChargeAccountInfo>.Failed(
                        "No fue posible establecer la conexión con T24 para consultar la cuenta cargo.");
                }

                var lista = response.MXMBBTMHCUCOMSCHELISTType;

                if (lista == null || lista.Length == 0)
                {
                    return Result<ChargeAccountInfo>.Failed(
                        "No se encontró información de cuenta cargo para el cliente.");
                }

                var detalles = lista[0]?.gMXMBBTMHCUCOMSCHELISTDetailType?.mMXMBBTMHCUCOMSCHELISTDetailType;
                var detalle = detalles?.FirstOrDefault();

                if (detalle == null)
                {
                    return Result<ChargeAccountInfo>.Failed(
                        "No se encontró detalle de cuenta cargo.");
                }

                var acctInfo = new ChargeAccountInfo
                {
                    CustomerCdl = detalle.NodeCliente?.Trim(),
                    CustomerName = detalle.NombredeCliente?.Trim(),
                    Account = detalle.Cuenta?.Trim(),
                    Currency = detalle.Moneda?.Trim()
                };

                if (string.IsNullOrWhiteSpace(acctInfo.Account))
                {
                    return Result<ChargeAccountInfo>.Failed(
                        "No se encontró una cuenta cargo para el cliente.");
                }

                Config.EscribirLog("INFO|GetCommissionAccount|Respuesta obtenida....");
                Config.EscribirLog($"INFO|GetCommissionAccount|CDL: {acctInfo.CustomerCdl}");
                Config.EscribirLog($"INFO|GetCommissionAccount|Moneda: {acctInfo.Currency}");
                Config.EscribirLog($"INFO|GetCommissionAccount|Cuenta: {acctInfo.Account}");

                return Result<ChargeAccountInfo>.Success(acctInfo);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|GetCommissionAccount|No pudo completarse la solicitud al WS T24.");

                Config.EscribirLog($"ERROR|GetCommissionAccount|Excepción = {ex.Message}");

                return Result<ChargeAccountInfo>.Failed(
                    "Hubo un error al intentar establecer la conexión con T24.");
            }
        }

        /// <summary>
        /// Valida si una cuenta en T24 tiene restricciones de crédito.
        /// </summary>
        /// <param name="t24Account">Número de cuenta en T24 a validar.</param>
        /// <param name="accountRestrictionDTO">Objeto que contiene las restricciones de débito permitidas.</param>
        /// <returns>
        /// true si la cuenta presenta una restricción de crédito no permitida o ocurre un error;
        /// false si no tiene bloqueo o el bloqueo es permitido.
        /// </returns>
        public async Task<bool> ValidateAccountHasCreditRestrictedAsync(string t24Account, AccountRestrictionDTO accountRestrictionDTO)
        {
            Config.EscribirLog("INFO|ValidateAccountHasCreditRestricted|Inicio WS T24");
            Config.EscribirLog($"INFO|ValidateAccountHasCreditRestricted|Cuenta = {t24Account ?? "null"}");

            try
            {
                var client = new AccountRestrictionValidator.T24WebServicesImplClient();

                var enquiry = new AccountRestrictionValidator.enquiryInputCollection
                {
                    columnName = "ACCOUNT",
                    criteriaValue = t24Account,
                    operand = "EQ"
                };

                var request = new ACBloqueodeCuentaRequest
                {
                    WebRequestCommon = BuildWebRequestCommon(),
                    EMUFGACGETBLOCKACCType = new[] { enquiry }
                };

                var response = await client.ACBloqueodeCuentaAsync(request);

                if (response?.Status == null || response.Status.successIndicator != AccountRestrictionValidator.successIndicator.Success)
                {
                    Config.EscribirLog($"ERROR|ValidateAccountCanReceiveFunds.WST24|Se presento un error en la consulta al WebService de T24: {response?.Status?.successIndicator.ToString()}");
                    return true;
                }

                var detail = response?
                    .EMUFGACGETBLOCKACCType?.FirstOrDefault()?
                    .gEMUFGACGETBLOCKACCDetailType?
                    .mEMUFGACGETBLOCKACCDetailType?
                    .FirstOrDefault();

                if (detail == null)
                {
                    Config.EscribirLog("WARN|ValidateAccountHasCreditRestricted|Respuesta sin detalles");
                    return true;
                }

                if (detail.Bloqueo == ConstantesAux.CodigoSinBloqueo)
                {
                    Config.EscribirLog("INFO|ValidateAccountHasCreditRestricted|No se presenta bloqueo en la cuenta.");
                    return false;
                }

                Config.EscribirLog($"INFO|ValidateAccountHasCreditRestricted|Se encontró bloqueo: {detail.Bloqueo ?? "null"} - {detail.DescripcionBloqueo ?? "null"}");

                return !accountRestrictionDTO?.debitRestrict?.Contains(detail.Bloqueo) ?? false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidateAccountHasCreditRestricted|Excepción: {ex.Message}");
                return true;
            }
        }

        /// <summary>
        /// Construye el objeto de credenciales.
        /// </summary>
        private AccountRestrictionValidator.webRequestCommon BuildWebRequestCommon()
        {
            var credenciales = _proveedorCredenciales.Obtener();

            return new AccountRestrictionValidator.webRequestCommon
            {
                company = credenciales?.WST24Company,
                userName = credenciales?.WST24Username,
                password = credenciales?.WST24Psw
            };
        }
    }
}

using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.DTOs;
using Aplicacion_IN_ETL.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infraestructura_IN_ETL.Repositorios
{
    /// <summary>
    /// Repositorio encargado de consultar informacion de cuentas VOSTRO utilizadas durante 
    /// el procesamiento de operaciones financieras.
    /// </summary>
    public class VostroRepo : RepositorioMaestro, IVostro
    {
        /// <summary>
        /// Inicializa una nueva instancia de <see cref="VostroRepo"/>
        /// </summary>
        public VostroRepo() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene la cuenta Vostro a partir del identificador proporcionado,
        /// ya sea extrayendo el código SWIFT o utilizándolo directamente.
        /// </summary>
        /// <param name="lsCIF_Sender">
        /// Identificador del remitente que puede contener un prefijo "NSWF"
        /// seguido del código SWIFT o el SWIFT directamente.
        /// </param>
        /// <returns>
        /// Un objeto <see cref="CatVostroDTO"/> con la información de la cuenta Vostro.
        /// </returns>
        public async Task<CatVostroDTO> ObtenerCuentaVostroAsync(string lsCIF_Sender)
        {
            try
            {
                string numCif = string.Empty;
                string cveSwift = string.Empty;

                if (!string.IsNullOrEmpty(lsCIF_Sender) && lsCIF_Sender.StartsWith("NSWF")
                    && lsCIF_Sender.Length >= 12)
                {
                    cveSwift = lsCIF_Sender.Substring(4, 8);
                }
                else
                {
                    cveSwift = lsCIF_Sender;
                }

                var resultado = await EjecutarStoredProcedureQuery<CatVostroDTO>(
                   ConstantesSql.USP_SWF_IN_ETL_GET_CUENTA_VOSTRO,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cif",
                            string.IsNullOrEmpty(numCif) ?
                            (object)DBNull.Value : numCif));

                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_swift",
                           string.IsNullOrEmpty(cveSwift) ?
                            (object)DBNull.Value : cveSwift));
                    });

                var row = resultado?.Data?.FirstOrDefault();
                return row;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerCuentaVostro|{ex.Message}");
                return null;
            }
        }

        public async Task<CatVostroDTO> ObtenerCuentaCIFVostroAsync(string lpNum_CIF)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<CatVostroDTO>(
                   ConstantesSql.USP_SWF_IN_ETL_GET_CUENTA_VOSTRO,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_cif", DBNull.Value));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_swift", lpNum_CIF));

                    });

                var row = resultado.Data.FirstOrDefault();

                return row;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerCuentaVostro|{ex.Message}");
                return null;
            }
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


using Aplicacion_IN_ETL;
using Aplicacion_IN_ETL.Constantes;
using Aplicacion_IN_ETL.Enums;
using Aplicacion_IN_ETL.Interfaces;
using Aplicacion_IN_ETL.Interfaces.Externos;
using Aplicacion_IN_ETL.Servicios;
using Aplicacion_IN_ETL.Servicios.Interpretacion;
using Aplicacion_IN_ETL.Servicios.Interpretadores;
using Aplicacion_IN_ETL.Utilidades;
using Infraestructura_IN_ETL.API;
using Infraestructura_IN_ETL.Repositorios;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Logeo_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using MUFG_T24.SwiftStandardRelease;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.Diagnostics;
using System.Net.Http;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace SWIFT_IN_ETL
{
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private Configuracion _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.Swf_IN_ETL.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

        public ServiceHost() { }

        protected override void Inicializacion(string[] args, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            //Realiza la configuracion del _containerDI
            _containerDI = new SimpleInjector.Container();
            _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();

            _containerDI.Register<IAlertas>(() => new Alertas(BasedatosProveedor.SqlServer, 
                NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);

            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();

            //Objeto que almacena la configuración necesaria para el funcionamiento del servicio.
            _configuracionBase = new Configuracion();

            {
                ConfiguracionEncriptadaHelper.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion =
                    ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Swf_IN_ETL.ToString());

                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.PathLog = servicioMainConfiguracion.Log;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;

                RutasConfiguracion rutas = new RutasConfiguracion()
                {
                    MUFG_Servicios_Path_Log = _configuracionBase.PathLog
                };

                Logging.Init(rutas, servicioMainConfiguracion.NombreServicio, 
                    servicioMainConfiguracion.TamanoMaximoArchivoLogKB);

                Logging.Write("INFO|Inicializacion|Leyendo archivo de configuración.");
            }

            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.RegisterInstance(new HttpClient());

            //Registro de los componentes...
            _containerDI.Register(() =>
                new ConexionT24Mw(
                    BasedatosProveedor.SqlServer,
                    NombreBasedatos.TOMI,
                    BasedatosProveedor.SqlServer,
                    NombreBasedatos.T24_VW
                ),
                Lifestyle.Singleton);

            _containerDI.Register<OperacionServicio>();
            _containerDI.Register<IRepositorioNostro, NostroRepo>(Lifestyle.Singleton);
            _containerDI.Register<ICuentasNostro, CuentasNostro>(Lifestyle.Singleton);
            _containerDI.Register<IOperaciones, Operaciones>();
            _containerDI.Register<IApiComission, ApiCommissionClientRepo>();
            _containerDI.Register<ILlamadaAPI, ApiClient>();
            _containerDI.Register<IT24CommissionService, T24CommissionServiceRepo>();
            _containerDI.Register<IServicioSpid, ServicioSpid>();
            _containerDI.Register<DatosCuenta>();
            _containerDI.Register<IAlerta, AlertaRepo>();
            _containerDI.Register<ILegalEntities, LegalEntitiesRepo>();
            _containerDI.Register<ICounterParty, CounterParty>();
            _containerDI.Register<ICuenta, CuentaRepo>();
            _containerDI.Register<IBanco, BancoRepo>();
            _containerDI.Register<ISwift, SwiftRepo>();
            _containerDI.Register<IProveedorCredenciales, ProveedorCredenciales>();
            _containerDI.Register<IReglas, ReglasRepo>();
            _containerDI.Register<IActualizacion, ActualizacionRepo>();
            _containerDI.Register<IIntegracion, IntegracionRepo>();
            _containerDI.Register<IRequestApiRejection, RequestApiRejection>();
            _containerDI.Register<ISintaxisMT, SintaxisRepo>();
            _containerDI.Register<ISpei, SpeiRepo>();
            _containerDI.Register<IVostro, VostroRepo>();
            _containerDI.Register<IAccountDebit, AccountDebitRepo>();
            _containerDI.Register<IPacs008, PACS008MX>();
            _containerDI.Register<IPacs009, PACS009MX>();
            _containerDI.Register<ISamhService, SamhService>();
            _containerDI.Register<IPacsService, PacsService>();
            _containerDI.Register<IT24Client, T24Client>();
            _containerDI.Register<ProcesaCampo>();
            _containerDI.Register<IContextoXml, ContextoXml>();
            _containerDI.Register<ILeerHeader, LeerHeaderRepo>();
            _containerDI.Register<SwiftStandardReleaseSanitize>();
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);

            _containerDI.Verify();
        }

        /// <summary>
        /// Realiza la obtención inicial de parámetros (una sola vez).
        /// </summary>
        /// 
        protected override async Task ParametrizacionesAsync(CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                //Obtiene el proceso actual...
                IParametrizacion _parametrizacion = 
                    _containerDI.GetInstance<IParametrizacion>();

                _configuracionBase = 
                    await _parametrizacion.ObtenerProcesoActualAsync(_configuracionBase, ct);

                //Marca el servicio en uso...
                IControlServicio _controlServicio = 
                    _containerDI.GetInstance<IControlServicio>();

                await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso, 
                    _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio, ct);
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
                    var operacionServicio =
                        _containerDI.GetInstance<OperacionServicio>();

                    await operacionServicio.EjecutarProcedimientosAsync(ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OnTimerAsync|Se presenta exception: {ex}");
            }
        }

        /// <summary>
        /// Realiza ciertas operaciones antes de finalizar el servicio.
        /// </summary>
        protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            Config.EscribirLog("INFO|TerminacionAsync|Finalizando Servicio.");
            try
            {
                if (_containerDI == null || _configuracionBase == null)
                {
                    Config.EscribirLog("INFO|TerminacionAsync|Terminación omitida por inicialización incompleta.");
                    return;
                }

                using (AsyncScopedLifestyle.BeginScope(_containerDI))

                {
                    IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();

                    await _controlServicio.EsperarTareaActivaAsync(
                        startupTask, ConstantesSistema.ParametrizacionInicial, waitCt).ConfigureAwait(false);

                    await _controlServicio.EsperarTareaActivaAsync(
                        runningTask, ConstantesSistema.ProcesamientoActivo, waitCt).ConfigureAwait(false);

                    //Marca el servicio en uso...
                    await _controlServicio.MarcarPuertoEnUsoAsync(
                        null, null, null, _configuracionBase.NombreServicio).ConfigureAwait(false);

                    //Marca el termino del servicio...
                    await _controlServicio.ActualizaTerminacion(
                        _configuracionBase.NombreServicio);
                }

                Config.EscribirLog("INFO|TerminacionAsync|Terminación exitosa de bases de datos.");
            }
            catch (OperationCanceledException exc)
            {
                Config.EscribirLog($"ERROR|TerminacionAsync|OperationCanceledException|{exc}");
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|TerminacionAsync|{ex}");
            }
        }
    }
}

namespace SWIFT_IN_ETL
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
    }
}

using Aplicacion_IN_ETL.Constantes;
using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;

namespace SWIFT_IN_ETL
{
    internal static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                NombreServicio.Swf_IN_ETL.ToString(),
                () => new ServiceHost(),
                args,
                (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs));
        }
    }
}

SWIFT_IN_ETL.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{9650462E-BA0E-40CB-860F-75894F6B056F}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>SWIFT_IN_ETL</RootNamespace>
    <AssemblyName>SWIFT_IN_ETL</AssemblyName>
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
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Core" />
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
    <ProjectReference Include="..\Aplicacion_IN_ETL\Aplicacion_IN_ETL.csproj">
      <Project>{f993e68c-2fe0-4ace-81a6-406a185e9f40}</Project>
      <Name>Aplicacion_IN_ETL</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura_IN_ETL\Infraestructura_IN_ETL.csproj">
      <Project>{b336730e-914f-e4aa-5c4b-e677c8c857b3}</Project>
      <Name>Infraestructura_IN_ETL</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

Aplicacion_IN_ETL.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net48</TargetFramework>
    <Platforms>AnyCPU;x86</Platforms>
  </PropertyGroup>
  <ItemGroup>
    <Compile Remove="Modelos\camt\057\**" />
    <EmbeddedResource Remove="Modelos\camt\057\**" />
    <None Remove="Modelos\camt\057\**" />
  </ItemGroup>
  <ItemGroup>
    <Compile Remove="TipoOperacion\camt_057.cs" />
  </ItemGroup>
  <ItemGroup>
    <EditorConfigFiles Remove="C:\Users\n371682\Desktop\SWIFT_IN_ETL\SWIFT_IN_ETL\Aplicacion_IN_ETL\.editorconfig" />
  </ItemGroup>
  <ItemGroup>
    <None Include="C:\Users\n371682\Desktop\SWIFT_IN_ETL\SWIFT_IN_ETL\Aplicacion_IN_ETL\.editorconfig" />
  </ItemGroup>
  <ItemGroup>
    <PackageReference Include="Newtonsoft.Json" Version="13.0.4" />
  </ItemGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System.Net.Http" />
    <Reference Include="System.ServiceModel" />
  </ItemGroup>
</Project>

Infraestructura_IN_ETL.csproj
﻿<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net48</TargetFramework>
    <Platforms>AnyCPU;x86</Platforms>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Newtonsoft.Json" Version="13.0.4" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\Aplicacion_IN_ETL\Aplicacion_IN_ETL.csproj" />
  </ItemGroup>

  <ItemGroup>
    <Reference Include="MUFG_T24">
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System.Net.Http" />
    <Reference Include="System.ServiceModel" />
  </ItemGroup>

</Project>

packages.config
﻿<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="SimpleInjector" version="5.5.2" targetFramework="net48" />
</packages>
