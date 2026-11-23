using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.Helper;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Proceso;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Xsl;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using static AplicacionGPH_ETL.Modelos.Pagos;

namespace AplicacionGPH_ETL.Servicios
{
    /// <summary>
    /// Clase para manejar el proceso de lectura de archivo
    /// </summary>
    public class ProcesoServicio : IProcesoServicio
    {
        private readonly Configuracion _config;
        private readonly ParametrosConfiguracion _params;
        private readonly ILoggerService _log;
        private readonly IRepositorioTomi _repTomi;
        private readonly IEnumerable<IProcesosOperacion> _procesosOperacion;


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config"></param>
        /// <param name="parametrosConfiguracion"></param>
        /// <param name="log"></param>
        /// <param name="repTomi"></param>
        /// <param name="procesosOperacion"></param>
        public ProcesoServicio(
            Configuracion config
            , ParametrosConfiguracion parametrosConfiguracion
            , ILoggerService log
            , IRepositorioTomi repTomi
            , IEnumerable<IProcesosOperacion> procesosOperacion            
        ) {
            _config = config;
            _params = parametrosConfiguracion;
            _log = log;
            _repTomi = repTomi;
            _procesosOperacion = procesosOperacion;            
        }

        /// <summary>
        /// Metodo principal que obtiene las operacipones/archivos, genera/valida los archivos xml y
        /// se procesa de acuerdo al tipo de archivo
        /// </summary>
        /// <returns></returns>
        public async Task<bool> RealizaOperaciones()
        {
            var serializado = false;           
            try
            {
                _log.Info(MsgLog.Procesos.Inicia);

                //verifica si existen las rutas de los archivos
                if (!Directory.Exists(_params.RUTA_ARCHIVOS_GPH_XSD))
                {                    
                    Directory.CreateDirectory(_params.RUTA_ARCHIVOS_GPH_XSD);
                }

                //verifica que ya exista el archivo "output.xslt"
                string archivoOutPutXML = Path.Combine(_params.RUTA_ARCHIVOS_GPH_XSD, ConstGenerales.ARCHIVO_XSLT);
                if (!File.Exists(archivoOutPutXML)) 
                {                    
                    _log.Error(string.Format(MsgLog.Procesos.Serializar.SinArchivoMaestro, _params.RUTA_ARCHIVOS_GPH_XSD));
                    return false;
                }
                
                _log.Info(MsgLog.Procesos.Serializar.BuscaArchivos);
                var FechaSistema = Conversion.StringToDate(_config.FechaSistema);



                var laListaArchivos = await _repTomi.ObtenerListaOperaciones(FechaSistema);                
                if(laListaArchivos.Count == 0)
                {
                    _log.Info(MsgLog.Procesos.Serializar.SinArchivos);                    
                    return true;
                }
                _log.Info(string.Format(MsgLog.Procesos.Serializar.TotalArchivos, laListaArchivos.Count));
                
                _log.Info(MsgLog.Procesos.Serializar.Inicio);
                foreach (var item in laListaArchivos)
                {
                    var strEx = "";
                    var TotalPagos = 0;
                    GroupHeader GroupHeaderFile = null;
                    
                    _log.Info($"{MsgLog.Procesos.Serializar.ProcesandoArchivo}: {item.Nombre_Archivo}{ConstGenerales.EXTENSION_XML}" );                    
                    string archivoXML = Path.Combine(_params.RUTA_ARCHIVOS_GPH_XML, $"{item.Nombre_Archivo}{ConstGenerales.EXTENSION_XML}");                    
                    if (File.Exists(archivoXML))
                    {
                        try
                        {
                            #region ConvierteArchivoXml
                            var rutaXsd = Path.Combine(_params.RUTA_ARCHIVOS_GPH_XSD, ConstGenerales.ARCHIVO_XSLT);
                            string xslContent = File.ReadAllText(rutaXsd, Encoding.UTF8);
                            if (!xslContent.Contains("<xsl:output"))
                            {
                                int index = xslContent.IndexOf(">", xslContent.IndexOf("<xsl:stylesheet")) + 1;
                                string outputTag = "\n<xsl:output method=\"xml\" indent=\"yes\" encoding=\"UTF-8\" omit-xml-declaration=\"yes\"/>\n";
                                xslContent = xslContent.Insert(index, outputTag);
                            }

                            //método para ajustar las rutas del archivo xslt
                            string ConvertirRutaPath(string ruta)
                            {
                                if (ruta.Contains("(") || ruta.Contains("|"))
                                {
                                    return ruta;
                                }
                                string[] partes = ruta.Split('/');
                                var partesConvertidas = new List<string>();
                                foreach (var parte in partes)
                                {
                                    string partelimpia = parte.Trim();
                                    if (string.IsNullOrEmpty(partelimpia)) continue;
                                    if (partelimpia.StartsWith("@"))
                                    {
                                        string NombreAtributo = parte.Substring(1);
                                        if (NombreAtributo == "*" || NombreAtributo.Contains("local-name"))
                                        {
                                            partesConvertidas.Add(partelimpia);
                                        }
                                        else
                                        {
                                            partesConvertidas.Add($"@*[local-name()='{NombreAtributo}']");
                                        }
                                        continue;
                                    }

                                    if (partelimpia == "*")
                                    {
                                        partesConvertidas.Add("*");
                                        continue;
                                    }

                                    if (partelimpia == "." || partelimpia == "..")
                                    {
                                        System.Diagnostics.Debug.WriteLine($" -> CASO PUNTO:´{partelimpia}´ ");
                                        partesConvertidas.Add(partelimpia);
                                        continue;
                                    }
                                    if (partelimpia == "local-name")
                                    {
                                        partesConvertidas.Add(partelimpia);
                                        continue;
                                    }

                                    if (partelimpia.Contains("["))
                                    {
                                        int indx = partelimpia.IndexOf("[");
                                        string name = partelimpia.Substring(0, indx);
                                        string predicado = partelimpia.Substring(indx);
                                        partesConvertidas.Add($"*[local-name()='{name}']{predicado}");
                                        continue;
                                    }

                                    partesConvertidas.Add($"*[local-name()='{partelimpia}']");
                                }
                                string resultado = string.Join("/", partesConvertidas);
                                return resultado;
                            }

                            //aplica patrones para sustituir rutas para las de tipo "match"
                            var regexMatch = new Regex(@"match\s*=\s*""([^""]+)""");
                            xslContent = regexMatch.Replace(xslContent, m =>
                            {
                                string valor = m.Groups[1].Value.Trim();
                                string convertido = ConvertirRutaPath(valor);
                                return $@"match=""{convertido}""";
                            });

                            //aplica patrones para sustituir rutas para las de tipo "select"
                            var regexSelect = new Regex(@"select\s*=\s*""([^""]+)""");
                            xslContent = regexSelect.Replace(xslContent, m =>
                            {
                                string valor = m.Groups[1].Value.Trim();
                                if (valor.Contains("("))
                                {
                                    return m.Value;
                                }
                                if (valor.Contains("|"))
                                {
                                    var partes = valor.Split('|').Select(p => ConvertirRutaPath(p.Trim()));
                                    return $@"select=""{string.Join(" | ", partes)}""";
                                }
                                string convertido = ConvertirRutaPath(valor);
                                return $@"select=""{convertido}""";
                            });


                            string plantillaIgnorar = @"
                            <!-- Ignorar declaraciones XML y processing instructions para evitar duplicados -->
                            <xsl:template match=""processing-instruction()"" priority=""100""/>
                            <xsl:template match=""comment()"" priority=""0""/>
                             ";

                            string plantillaCcy = @"
                                    <xsl:template match=""*[local-name()='InstdAmt'] | *[local-name()='DuePyblAmt'] | *[local-name()='DscntApldAmt']
                                    | *[local-name()='CdtNoteAmt'] | *[local-name()='TaxAmt'] | *[local-name()='RmtdAmt'] | 
                                    *[local-name()='Amt']"" priority=""1000""> 
                                        <xsl:copy>
                                            <xsl:copy-of select=""@*""/>
                                            <xsl:apply-templates select=""node()""/>
                                        </xsl:copy>
                                    </xsl:template>
                                    ";

                            xslContent = xslContent.Replace("</xsl:stylesheet>", plantillaCcy + "\n" + plantillaIgnorar + "\n</xsl:stylesheet>");
                            File.WriteAllText(Path.Combine(_params.RUTA_ARCHIVOS_GPH_XSD, "Output_After.xslt"), xslContent);

                            var xslt = new XslCompiledTransform();
                            using (var stringReader = new StringReader(xslContent))
                            {
                                using (var xmlReadXsl = XmlReader.Create(stringReader))
                                {
                                    xslt.Load(xmlReadXsl);
                                }
                            }

                            _log.Info(string.Format(MsgLog.Procesos.Serializar.GenerandoArchivoAfter, item.Nombre_Archivo));
                            var rutaSalida = Path.Combine(_params.RUTA_ARCHIVOS_GPH_XML, $"{item.Nombre_Archivo}_After.xml");
                            var readerSettingsF = new XmlReaderSettings
                            {
                                IgnoreWhitespace = true,
                                ConformanceLevel = ConformanceLevel.Document,
                                DtdProcessing = DtdProcessing.Ignore,
                            };

                            using (var xmlReadInput = XmlReader.Create(archivoXML, readerSettingsF))
                            {
                                using (var fsOutput = File.Create(rutaSalida))
                                {
                                    using (var xmlOutput = XmlWriter.Create(fsOutput, new XmlWriterSettings
                                    {
                                        Encoding = new UTF8Encoding(false),
                                        Indent = true,
                                        OmitXmlDeclaration = false,
                                        ConformanceLevel = ConformanceLevel.Document
                                    }))
                                    {
                                        xslt.Transform(xmlReadInput, null, xmlOutput);
                                    }
                                }
                            }
                            #endregion

                            #region ValidaEstructuraXml
                            _log.Info(string.Format(MsgLog.Procesos.Serializar.SerializandoArchivo, item.Nombre_Archivo));
                            var rutaArchivoAfter = Path.Combine(_params.RUTA_ARCHIVOS_GPH_XML, $"{item.Nombre_Archivo}_After.xml");
                            string xmlEntrada = File.ReadAllText(rutaArchivoAfter, Encoding.UTF8);                            
                            string codigoError_MaximoNopagos = string.Empty;
                            IEnumerable<XElement> pagosXml = null;
                            XElement gphHdrXml = null;
                            if (Conversion.StringToXmlSeguro(xmlEntrada, out XDocument docXml, out string errorxml))
                            {
                                var cstmr = docXml.Descendants().FirstOrDefault(e => e.Name.LocalName == "CstmrCdtTrfInitn");
                                _log.Info(MsgLog.Procesos.Serializar.ObteniedoListaPagos);
                                pagosXml = cstmr?.Elements().Where(e => e.Name.LocalName == "PmtInf");
                                gphHdrXml = cstmr?.Elements().FirstOrDefault(e => e.Name.LocalName == "GrpHdr");
                                var msgErrorEstructura = string.Empty;
                                //Validaciones
                                if (cstmr == null)
                                    msgErrorEstructura = "[Falta nodo 'CstmrCdtTrfInitn'] ";
                                if (pagosXml == null || !pagosXml.Any())
                                    msgErrorEstructura += ((string.IsNullOrEmpty(msgErrorEstructura)) ? "" : ",") + "[Falta nodo 'PmtInf']";

                                if (gphHdrXml == null)
                                    msgErrorEstructura += ((string.IsNullOrEmpty(msgErrorEstructura)) ? "" : ",") + "[Falta nodo 'GrpHdr']";

                                var msgId = gphHdrXml?.Elements().FirstOrDefault(e => e.Name.LocalName == "MsgId");
                                if (msgId == null || string.IsNullOrWhiteSpace(msgId.Value))
                                    msgErrorEstructura += ((string.IsNullOrEmpty(msgErrorEstructura)) ? "" : ",") + "[GrpHdr no contiene MsgId o esta vacío]";

                                var PagosInValidos = pagosXml.ToList().Count > _params.TOTAL_PAGOS_GPH_ACEPTADOS;
                                if (PagosInValidos)
                                {
                                    codigoError_MaximoNopagos = Codigo.Cod_1Z35201;
                                    msgErrorEstructura += ((string.IsNullOrEmpty(msgErrorEstructura)) ? "" : ",") + $"[No total de pagos invalidos, permitidos {_params.TOTAL_PAGOS_GPH_ACEPTADOS}, actuales {pagosXml.ToList().Count}]";
                                }
                                    

                                if (!string.IsNullOrEmpty(msgErrorEstructura))
                                    strEx = $"Estructura XML inválida: {msgErrorEstructura}";
                            }
                            else
                            {
                                strEx = $"XML inválido: {errorxml}";
                            } 
                            #endregion

                            #region ValidacionAccesoCliente
                            _log.Info(MsgLog.Procesos.Serializar.ValidaAccesoCliente);
                            string[] datos = item.Nombre_Archivo.Split('_');
                            int conteo = 0;
                            long ClienteId;

                            if (datos.Length > 1)
                            {
                                ClienteId = ConversionesComunes.ParseLongRequerido(datos[1], "ClienteId");
                                conteo = await _repTomi.ValidarAccesoServicioAsync(ClienteId);
                            }
                            var CodigoError_EstatusAccesoServicio = "";
                            if (conteo <= 0)
                            {
                                CodigoError_EstatusAccesoServicio = Codigo.Cod_1T10070;
                                _log.Error(MsgLog.Procesos.Serializar.ClienteSinAcceso);
                            }

                            #endregion

                            if (string.IsNullOrEmpty(strEx))
                            {
                                //pasar el gphHdrXml a objeto serializado...aqui ya viene validado
                                GroupHeader groupHeader = ValidaXML.Deserealizar<GroupHeader>(gphHdrXml);


                                //la siguiente validaciones si no se cumplen, permite el procveso de tipo de documento [FXD, FXI,etc]
                                string ipAddress = groupHeader.Authstn?.Prtry;
                                string ipAddress2 = gphHdrXml.Elements().FirstOrDefault(e => e.Name.LocalName == "Authstn")?
                                    .Elements().FirstOrDefault(e => e.Name.LocalName == "Prtry")?.Value;

                                //actualiza MsgId
                                var msgIdValue = gphHdrXml?.Elements().FirstOrDefault(e => e.Name.LocalName == "MsgId").Value;
                                _log.Info(MsgLog.Procesos.Serializar.InsertaRegistroPago);
                                var resultUpdate = await _repTomi.MaestroActualizaMsgId(item.Folio, ConstGenerales.GSTIPO_OPERACION, msgIdValue);
                                if (resultUpdate)
                                    _log.Info(string.Format(MsgLog.Procesos.Serializar.SeActualizoRegistro, item.Folio, msgIdValue));


                                //procesa los pagos que contiene el archiv xml
                                foreach (var pago in pagosXml.ToList())
                                {                                    
                                    var msgErrorEstructuraPago = string.Empty; 
                                    var codErrorEstructuraPago= string.Empty;
                                    var errorResultado = string.Empty;
                                    TotalPagos += 1;
                                    //valida estructura de pago
                                    //deserealiza nodo de pago
                                    PaymentInstruction pagoItem= ValidaXML.Deserealizar<PaymentInstruction>(pago);

                                    var cdtNode = pago.Elements().Where(e => e.Name.LocalName == "CdtTrfTxInf").ToList();
                                    if (!cdtNode.Any() && cdtNode.Count() > 1)
                                    {
                                        codErrorEstructuraPago = Codigo.Cod_1T10072;
                                        msgErrorEstructuraPago = $"[{string.Format(MsgLog.Procesos.Serializar.MarcaRegistroRechazado, cdtNode.Count())}]";
                                        _log.Error(string.Format(MsgLog.Procesos.Serializar.MarcaRegistroRechazado, cdtNode.Count()));                                     
                                    }

                                    //obtener loa nombres de los nodos contenidos en [CdtTrfTxInf]
                                    var nombres = cdtNode.Elements().Select(e => e.Name.LocalName.Trim()).Distinct().ToList();
                                    var presentes = CdtTrfTxInf_NodosRequeridos.Where(n => nombres.Contains(n,StringComparer.OrdinalIgnoreCase)).ToList();
                                                                       
                                    var nodosFaltantes = CdtTrfTxInf_NodosRequeridos.Except(presentes).ToList();
                                    if (nodosFaltantes.Any())
                                    {
                                        codErrorEstructuraPago = Codigo.Cod_1T10072;
                                        _log.Error($"Estructura inválida del nodo [CdtTrfTxInf], le faltan lo elementos [{string.Join(",", nodosFaltantes )}]");
                                    }

                                    //nodos ok, 
                                    //obtiene el tipo de proceso(pago) [FXD,FXI, etc]
                                    var tipoProceso = pago.Elements().FirstOrDefault(e => e.Name.LocalName == "PmtTpInf")?
                                        .Elements().FirstOrDefault(e => e.Name.LocalName == "LclInstrm")?
                                        .Elements().FirstOrDefault(e => e.Name.LocalName == "Cd")?.Value;

                                    IProcesosOperacion proceso =  null;
                                    if (string.IsNullOrWhiteSpace(tipoProceso))
                                    {
                                        codErrorEstructuraPago += ((string.IsNullOrEmpty(codErrorEstructuraPago)) ? "" : ",") + Codigo.Cod_1T10062;
                                        _log.Error(string.Format(Codigo.GetDescription(nameof(Codigo.Cod_1T10062)), tipoProceso, Codigo.Cod_1T10062));                                        
                                    }
                                    else
                                    {
                                        //buscamos el proceso implementado
                                        proceso = _procesosOperacion.FirstOrDefault(s => s.TipoProceso.Contains(tipoProceso, StringComparer.OrdinalIgnoreCase)) ;
                                    }
                                    if (proceso == null)
                                        codErrorEstructuraPago += ((string.IsNullOrEmpty(codErrorEstructuraPago)) ? "" : ",") + Codigo.Cod_1T10062;


                                    if (string.IsNullOrEmpty(codErrorEstructuraPago))
                                        errorResultado = await proceso.ProcesaOperacionAsync(item, pagoItem, ipAddress, codigoError_MaximoNopagos, CodigoError_EstatusAccesoServicio, groupHeader);
                                    else
                                        errorResultado = codErrorEstructuraPago;

                                    if (!string.IsNullOrEmpty(errorResultado))
                                    {
                                        //registra detalle de archivo con error
                                        var resultInsert = await _repTomi.InsertaDetalleArchivo(item.Folio, pagoItem.PmtInfId, ConstGenerales.OPERACION_RECHAZADA, errorResultado, pagoItem.CdtTrfTxInf.PmtId.EndToEndId, gphHdrXml.ToString());                                        
                                        if (resultInsert > 0)
                                            _log.Error($"Se creó correctamente registro de operación '{ConstGenerales.OPERACION_RECHAZADA}' detalles de archivo: Folio: {item.Folio}");
                                    }

                                }
                            }                        
                        }
                        catch (Exception ex) {
                            _log.Error($"{MsgLog.Procesos.Serializar.ExceptionSerializacion} : {ex.Message}" );
                            strEx = ex.Message;                            
                        }
                        finally {
                            if (File.Exists($"{_params.RUTA_ARCHIVOS_GPH_XML}{item.Nombre_Archivo}_After.xml"))
                            {
                                _log.Info(string.Format(MsgLog.Procesos.Serializar.EliminandoArchivoAfter, item.Nombre_Archivo));                                                                
                                File.Delete($"{_params.RUTA_ARCHIVOS_GPH_XML}{item.Nombre_Archivo}_After.xml");
                            }
                        }
                    } else
                    {                        
                        strEx = $"El archivo [{item.Nombre_Archivo}{ConstGenerales.EXTENSION_XML}] no existe en la ruta [{_params.RUTA_ARCHIVOS_GPH_XML} , no se puede procesar]";                        
                    }
               
                    if (!string.IsNullOrEmpty(strEx))
                    {
                        _log.Error($"{MsgLog.Procesos.Serializar.ErroresAcumulados}:{strEx}");
                        await _repTomi.MarcarRegistroConError(item.Folio, strEx);
                    }
                    else
                    {                       
                        _log.Info(MsgLog.Procesos.Serializar.MarcaRegistroSerializado + item.Folio);
                        string cmlHeaderstring = Conversion.XmlToString(GroupHeaderFile);
                        cmlHeaderstring = cmlHeaderstring.Replace("utf-16", "utf-8");
                        await _repTomi.MarcarRegistrosSerializados(item.Folio, TotalPagos, cmlHeaderstring);
                        serializado = true;
                    }
                    
                    _log.Info(MsgLog.Procesos.Serializar.FinProcesandoArchivo);
                }
                
                _log.Info(MsgLog.Procesos.Serializar.Fin);
                return serializado;
            }
            catch (Exception ex) { 
                _log.Error($"{MsgLog.Procesos.Exception}: ex:{ex}");                
            }
            finally
            {                
                _log.Info(MsgLog.Procesos.Fin);
            }
            return false;
        }


    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Helper;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Modelos;
using AplicacionGPH_ETL.Modelos.Procesos;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Aplicacion.Conversiones;
using System;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using AplicacionGPH_ETL.Interfaces.Servicios;

namespace AplicacionGPH_ETL.Servicios.ProcesosOperacion
{
    
    /// <summary>
    /// Clase que implementa metodos comunes a todos los procesos
    /// </summary>
    public class ServiciosBase
    {
        protected readonly ILoggerService _log;
        protected readonly IRepositorioTomi _repTomi;
        protected readonly IRepositorioVT24 _repT24;
        protected readonly IRepositorioTEF _repTEF;
        protected readonly IConexionT24Service _conexionT24Service;
        protected readonly ParametrosConfiguracion _paramConfig;
        protected readonly Configuracion _config;
        protected DateTime FechaSistema;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="log"></param>
        /// <param name="repTomi"></param>
        /// <param name="repT24"></param>
        /// <param name="repTEF"></param>
        /// <param name="paramConfig"></param>
        /// <param name="config"></param>
        public ServiciosBase(
            ILoggerService log
            , IRepositorioTomi repTomi
            , IRepositorioVT24 repT24
            , IRepositorioTEF repTEF
            , IConexionT24Service conexionT24Service
            , ParametrosConfiguracion paramConfig
            , Configuracion config
        )
        {
            _log = log;
            _repTomi = repTomi;
            _repT24 = repT24;
            _repTEF = repTEF;
            _conexionT24Service = conexionT24Service;
            _paramConfig = paramConfig;
            _config = config;                    
        }

        /// <summary>
        /// Mpetodo que agrega codigo de error y regista log
        /// </summary>
        /// <param name="tipoOperacion"></param>
        /// <param name="codigoError"></param>        
        /// <returns></returns>
        protected string AgregarCodigoError(string tipoOperacion, string codigoError)
        {
            if (string.IsNullOrWhiteSpace(tipoOperacion))
                throw new ArgumentNullException("El valor de [tipoOperacion] no puede esta vacio]");

            if (codigoError != string.Empty)
            {
                _log.Error($"ProcesaOperacion|{tipoOperacion}|Codigo: {codigoError}|Descripcion: {Codigo.GetDescription(codigoError)}");
                return $"{codigoError}|";
            }
            return string.Empty;
        }



        /// <summary>
        /// Método que realiza diferentes tipos de validaciones comunes entre todos los tipos de procesos
        /// </summary>
        /// <param name="tipoOperacion"></param>
        /// <param name="Oper"></param>
        /// <param name="IP_Address"></param>
        /// <param name="EstatusMaxPayments"></param>
        /// <param name="EstatusAccesServcies"></param>
        /// <param name="laValoresFormato"></param>
        /// <returns></returns>
        protected async Task<string> ValidacionesComunes(
            string tipoOperacion
            , Pagos.PaymentInstruction Oper
            , string IP_Address
            , string EstatusMaxPayments
            , string EstatusAccesServcies
            , ProcesosBaseModel laValoresFormato
         )
        {
            string ErrorCodes = string.Empty;            
            
            ErrorCodes += AgregarCodigoError(tipoOperacion, EstatusMaxPayments);
            ErrorCodes += AgregarCodigoError(tipoOperacion, EstatusAccesServcies);
            ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaIpAddress(IP_Address));
            ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.EsTRF(Oper));
            ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaFormatoFechaEjecucion(Oper));
            var esDiaHabil = await _repTomi.EsFechaEjecucionEnDiaHabil(Oper.ReqdExctnDt);
            if (!esDiaHabil)
            {
                ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1Z30203);
            }
            ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaLongitudCuenta(Oper));
            ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaBTMUMCODE(Oper));

            int longcta = 0;
            switch (tipoOperacion)
            {
                case OperacionTipos.TEF:
                case OperacionTipos.FXD:               
                case OperacionTipos.FXT:
                    longcta = 7;
                    break;
                case OperacionTipos.FXI:
                case OperacionTipos.SPID:
                    longcta = 20;
                    break;
            }
            
            var endToEndId = Oper.CdtTrfTxInf?.PmtId?.EndToEndId?.Trim() ?? string.Empty;
            ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaCustomerReference(endToEndId, longcta));
            ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaDebitAmount(Oper));


            switch (tipoOperacion)
            {
                case OperacionTipos.TEF:
                case OperacionTipos.FXD:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.NumericCustomerReference(Oper));
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaMonedaMXN(Oper));
                    break;
                case OperacionTipos.SPID:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaMonedaUSD(Oper));
                    break;
                case OperacionTipos.FXI:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaMonedaFXI(Oper));
                    break;
                case OperacionTipos.FXT:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaDebitCurrencyFXT(Oper));
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaBENEFBANK_CODE(Oper));
                    break;
            }


            //Valida Tipo Persona - F o M
            switch (tipoOperacion)
            {
                case OperacionTipos.TEF:
                case OperacionTipos.FXD:
                case OperacionTipos.FXT:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaTipoPersona(Oper));   //de ser m o f
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaPatronPersonaFisica(Oper));
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaPatronPersonaMoral(Oper));

                    break;
                case OperacionTipos.SPID:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaTipoPersonaMoral(Oper));
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaPatronPersonaMoral(Oper));

                    //CLASIFICACION_OPERACION
                    ErrorCodes += AgregarCodigoError(tipoOperacion,ValidaXML.ValidaClasificacion(Oper));

                    // RegulatoryReporting NNK NI
                    ErrorCodes += AgregarCodigoError(tipoOperacion,ValidaXML.EsRegulatorio(Oper));

                    //BUSSINESS_RELATIONSHIP 
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaRelacionNegocio(Oper));

                    break;
                case OperacionTipos.FXI:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaTipoPersona(Oper));
                    // RegulatoryReporting NNK NI
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.EsRegulatorio(Oper));

                    //'2.43 <Oper.CdtTrfTxInf.RmtInf.Strd.AddtlRmtInf> - Additional Remittance Information       Max length: 40, Min length: 1
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaInfoAdicionalRemitente(Oper));

                    break;

            }
            
            switch (tipoOperacion)
            {
                case OperacionTipos.FXI:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaNombreBancoBeneficiarioFXI(Oper));
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaCuentaBeneficiarioFXI(Oper));

                    break;
                case OperacionTipos.SPID:
                case OperacionTipos.FXD:
                case OperacionTipos.TEF:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaReferenciaPago(Oper));
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaCodigoBancoBeneficiario(Oper));
                    break;
                case OperacionTipos.FXT:
                    ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaReferenciaPago(Oper));
                    break;
            }

            
            ErrorCodes += AgregarCodigoError(tipoOperacion, ValidaXML.ValidaNombreBeneficiario(Oper));

            var cuentaOrdenante = Oper.DbtrAcct?.Id?.Othr?.Id ?? string.Empty;
            _log.Info($"procesaOperacion|Buscando que la cuenta sea una cuenta BTMUM valida: {cuentaOrdenante}");
            //'Valida Cuenta
            var cta = await _repT24.ObtenerDatosCuenta(cuentaOrdenante);
            if (cta != null)
            {
                laValoresFormato.T24_CUSTOMER_CODE = cta.Customer;           //[21]'CDL
                laValoresFormato.NAME_1 = cta.Name_1;                        //[30]'COMPANY NAME
                laValoresFormato.RFC = cta.Rfc;                              //[31]'RFC
                laValoresFormato.CURRENCY = cta.Currency;                    //[32]'CURRENCY
                laValoresFormato.ACCOUNT_TITLE_1 = cta.Account_Title_1;      //[33]'ACCOUNT NAME
                laValoresFormato.T24_DEBIT_ACCT_NO = cta.Account_Number;     //[2]'DEBIT ACCOUNT                        
            }

            if (string.IsNullOrEmpty(laValoresFormato.T24_CUSTOMER_CODE))
            {
                ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10019);
            }
            else
            {
                //Valida permiso de ejecucion
                var tienePermiso = await _repTomi.ValidaPermisoEjecucion(laValoresFormato.T24_CUSTOMER_CODE, tipoOperacion);
                if (!tienePermiso)
                {
                    switch (tipoOperacion)
                    {
                        case OperacionTipos.TEF:
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10069);     //--TEF
                            break;
                        case OperacionTipos.FXD:
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10066);    //--FXD
                            break;
                        case OperacionTipos.SPID:
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10068);     //--SPID
                            break;
                        case OperacionTipos.FXI:
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10065);    //--FXI
                            break;
                        case OperacionTipos.FXT:
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10067);   //--FXT
                            break;
                    }
                }
            }

            //'Valida Moneda
            switch (tipoOperacion)
            {
                case OperacionTipos.TEF:
                case OperacionTipos.FXD:                    
                    if (laValoresFormato.CURRENCY != ConstGenerales.MONEDAMX)
                    {
                        ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10071);
                    }
                    break;
                case OperacionTipos.SPID:                    
                    if (laValoresFormato.CURRENCY != ConstGenerales.MONEDAUSD)
                    {
                        ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10056);
                    }
                    break;
            }



           // Valida Banco Beneficiario
           var mmbId = Oper.CdtTrfTxInf?.CdtrAgt?.FinInstnId?.ClrSysMmbId?.MmbId ?? string.Empty;                        
           var insClave = ConversionesComunes.ParseDecimalRequerido(mmbId, "InsClave");
            BancoBeneficiarioDTO dataBanco = null;
            switch (tipoOperacion)
            {
                case OperacionTipos.FXD:
                    dataBanco = await _repTomi.ObtenerBancoBeneficiarioInstituciones(insClave);
                    break;
                case OperacionTipos.SPID:
                    dataBanco = await _repTomi.ObtenerBancoBeneficiarioInstitucionesSPID(insClave);
                    break;

                case OperacionTipos.TEF:
                    dataBanco = await _repTEF.ObtenerBancoBeneficiario(insClave);
                    break;
            }

            switch (tipoOperacion)
            {
                case OperacionTipos.FXD:
                case OperacionTipos.SPID:
                case OperacionTipos.TEF:
                    if (dataBanco == null || string.IsNullOrEmpty(dataBanco.Ins_nombre))
                    {
                        ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10021);
                    }
                    else
                    {
                        laValoresFormato.LsBanco = dataBanco.Ins_clave.ToString();
                        laValoresFormato.NF_BANCO_BENEF = $"{dataBanco.Ins_clave} - {dataBanco.Ins_nombre}";
                    }


                    break;
            }
            return ErrorCodes;

        }


        /// <summary>
        /// Metodo para vallidar la fecha de ejecución de un proceso
        /// </summary>
        /// <param name="tipoOperacion"></param>
        /// <param name="Oper"></param>
        /// <param name="laValoresFormato"></param>
        /// <param name="HrInicio_XTipXOperacion"></param>
        /// <param name="HrFin_XTipoOperacion"></param>
        /// <returns></returns>
        protected async Task<string> ValidaFechaEjecucion(
            string tipoOperacion
            , Pagos.PaymentInstruction Oper
            , ProcesosBaseModel laValoresFormato
            , TimeSpan HrInicio_XTipXOperacion
            , TimeSpan HrFin_XTipoOperacion          
         )
        {
            DateTime LSFe;
            string ErrorCodes = string.Empty;
            if (!string.IsNullOrEmpty(laValoresFormato.NF_FECHA_EJECUCION))
            {
                laValoresFormato.NF_FECHA_APLICACION = (Oper?.ReqdExctnDt ?? string.Empty).Replace("-", "");
                LSFe = Conversion.StringToDate(laValoresFormato.NF_FECHA_EJECUCION);
                if (LSFe <  FechaSistema)
                {
                    ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1Z30204);
                }
                else
                {
                    //fecha futura?
                    var lbFechaFutura = LSFe > FechaSistema;
                    // Mismo Dia? Verificar horario de operación
                    if (!lbFechaFutura)
                    {
                        var lhi = ConversionesComunes.ParseTimeSpanOpcional(DateTime.Now.ToString("HH:mm"), "HoraSistema", TimeSpan.Zero);
                        bool estaEnRago = lhi >= HrInicio_XTipXOperacion && lhi <= HrFin_XTipoOperacion;
                        if (!estaEnRago)
                        {
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10063);
                        }
                    }
                    else
                    {
                        //' Valida Fecha ejecucion no mayor a 15 dias
                        DateTime FechaMaximaEjecucion = FechaSistema.AddDays(15);
                        if (FechaMaximaEjecucion < LSFe)
                        {
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10058);
                        }
                    }
                    //' Valida que la Fecha de ejecucion no sea Sabado o Domingo
                    var fEjecucionEnFinSemanana = await _repTomi.EsFechaEjecucionEnFinSemana(laValoresFormato.NF_FECHA_EJECUCION);
                    if (fEjecucionEnFinSemanana)
                    {
                        ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1Z30205);
                    }

                }
            }
            return ErrorCodes;
        }



        /// <summary>
        /// Método para validar tipo de cuenta
        /// </summary>
        /// <param name="tipoOperacion"></param>
        /// <param name="Oper"></param>
        /// <param name="laValoresFormato"></param>
        /// <returns></returns>
        protected string ValidaTipoCuenta(string tipoOperacion, Pagos.PaymentInstruction Oper, ProcesosBaseModel laValoresFormato)
        {
            string ErrorCodes = string.Empty;
            var cuentaBeneficiario = Oper.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id?.Trim() ?? string.Empty;

            switch (tipoOperacion)
            {
                case OperacionTipos.TEF:
                case OperacionTipos.FXD:
                    switch (laValoresFormato.NF_TIPO_CUENTA)
                    {
                        case "40":
                            if (tipoOperacion == OperacionTipos.TEF)
                            {
                                if (cuentaBeneficiario.Length != 18)
                                {
                                    ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10022);
                                }
                            }
                            else
                            {
                                int blnCLABE = ValidacionesGenerales.ObtenerCuentaCLABE(cuentaBeneficiario, laValoresFormato.LsBanco);
                                switch (blnCLABE)
                                {
                                    case 2:
                                        ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10022);                                        
                                        break;
                                    case 3:
                                        ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10023);                                        
                                        break;
                                    case 4:
                                        ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10024);                                        
                                        break;
                                }
                            }
                            laValoresFormato.NF_TIPO_CUENTA = "CUENTA DE CHEQUES(CLABE)";
                            break;
                        case "03":
                            if (cuentaBeneficiario.Length != 16)
                            {
                                ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10046);
                            }
                            laValoresFormato.NF_TIPO_CUENTA = "TARJETA DE DEBITO";
                            break;
                        default:
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10016);
                            break;
                    }
                    break;
                case OperacionTipos.SPID:
                case OperacionTipos.FXT:
                    switch (laValoresFormato.NF_TIPO_CUENTA)
                    {
                        case "40":
                            int blnCLABE = ValidacionesGenerales.ObtenerCuentaCLABE(cuentaBeneficiario, "40108");
                            switch (blnCLABE)
                            {
                                case 2:
                                    ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10022);                                    
                                    break;
                                case 3:
                                    ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10023);
                                    break;
                                case 4:
                                    ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10024);                                    
                                    break;
                            }
                            laValoresFormato.NF_TIPO_CUENTA = "CUENTA DE CHEQUES(CLABE)";
                            break;
                        default:
                            ErrorCodes += AgregarCodigoError(tipoOperacion, Codigo.Cod_1T10045);
                            break;
                    }
                    break;

            }

            return ErrorCodes;
        }


        /// <summary>
        /// Metodo para registrar en bd la finalizacion de un proceso
        /// </summary>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="FechaEjecucion"></param>
        /// <param name="ErrorCodes"></param>
        /// <param name="TipoOperacion"></param>
        /// <param name="Folio"></param>
        /// <returns></returns>
        protected async Task<bool> FinalizaOperacion(ArchivosGphDTO Archivo, Pagos.PaymentInstruction Oper
            ,string FechaEjecucion, string ErrorCodes, string TipoOperacion, long Folio) {
            string EstatusFinal = ConstGenerales.OPERACION_RECHAZADA;
            if (string.IsNullOrEmpty(ErrorCodes))
            {
                var lsf1 = Conversion.StringToDate(FechaEjecucion);
                var nfEstatus = "";
                if (lsf1 == FechaSistema)
                {
                    switch (TipoOperacion) 
                    {
                        case OperacionTipos.FXD:
                        case OperacionTipos.TEF:
                        case OperacionTipos.SPID:
                            nfEstatus = EstatusOpe.SCNP;
                            break;
                        case OperacionTipos.FXI:
                        case OperacionTipos.FXT:
                            nfEstatus = EstatusOpe.CONP;
                            break;
                    }
                    
                }
                else if (lsf1 > FechaSistema)
                {
                    switch (TipoOperacion)
                    {
                        case OperacionTipos.FXD:
                        case OperacionTipos.TEF:
                        case OperacionTipos.SPID:
                            nfEstatus = EstatusOpe.SCNF;
                            break;
                        case OperacionTipos.FXI:
                        case OperacionTipos.FXT:
                            nfEstatus = EstatusOpe.CONF;
                            break;
                    }                   
                }

                bool resultUpdate = false;
                if (!string.IsNullOrEmpty(nfEstatus))
                {
                    _log.Info($"Procesos|{TipoOperacion}|{string.Format(MsgLog.Procesos.AsignarEstatus, nfEstatus, Folio)}"  );                    
                    switch (TipoOperacion)
                    {
                        case OperacionTipos.FXD:                            
                            resultUpdate = await _repTomi.ActualizaEstatusRegistroFXD(Folio, nfEstatus);
                            break;
                        case OperacionTipos.FXI:
                            resultUpdate = await _repTomi.ActualizaEstatusRegistroFXI(Folio, nfEstatus);
                            break;
                        case OperacionTipos.FXT:
                            resultUpdate = await _repTomi.ActualizaEstatusRegistroFXT(Folio, nfEstatus);
                            break;
                        case OperacionTipos.TEF:
                            resultUpdate = await _repTomi.ActualizaEstatusRegistroTEF(Folio, nfEstatus);
                            break;
                        case OperacionTipos.SPID:
                            resultUpdate = await _repTomi.ActualizaEstatusRegistroSPID(Folio, nfEstatus);
                            break;
                    }
                }
                if (resultUpdate)
                {
                    _log.Info($"Procesos|{TipoOperacion}|{MsgLog.Procesos.RegistroActualizado}");
                    ErrorCodes = "Operacion Aceptada.";
                    EstatusFinal = ConstGenerales.OPERACION_ACEPTADA;
                    _log.Info($"Procesos|{TipoOperacion}|{string.Format(MsgLog.Procesos.InsertaOperacionAceptada, Folio)}");
                }
                else
                {
                    ErrorCodes = $"Ocurrió un error al intentar actulizar el estatus del registro con Folio:{Folio}, TipoOperacion: {TipoOperacion}";
                } 
            }

            if (EstatusFinal == ConstGenerales.OPERACION_RECHAZADA)
                _log.Info($"Procesos|{TipoOperacion}|{string.Format(MsgLog.Procesos.InsertaOperacionRechazada, Folio)}");

            var insert = await _repTomi.FinalizaOperacionArchivo(Archivo, Oper, ErrorCodes, Folio, EstatusFinal, TipoOperacion);
            if (insert)
                _log.Info($"Procesos|{TipoOperacion}|Se finaliza correctamente el archivo con Folio: {Folio}, con estatus final: {EstatusFinal}");

            return insert;
            
        }
    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Helper;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Proceso;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Interfaces.Servicios;
using AplicacionGPH_ETL.Modelos;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using msgs = AplicacionGPH_ETL.Enums.ConstantesServicio.MsgLog.Procesos.TEF;

namespace AplicacionGPH_ETL.Servicios.ProcesosOperacion
{
    /// <summary>
    /// Clase que implementa el metodo para procesar una operacion de tipo TEF
    /// </summary>
    public class ServicioTEF : ServiciosBase, IProcesosOperacion
    {        
        public string TipoOperacion => OperacionTipos.TEF;        
        List<string> IProcesosOperacion.TipoProceso => new List<string> {TiposCd.TEF01, TiposCd.TEF03, TiposCd.TEF06 };

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="log"></param>
        /// <param name="repTomi"></param>
        /// <param name="repT24"></param>
        /// <param name="repTEF"></param>
        /// <param name="paramConfig"></param>
        /// <param name="config"></param>
        public ServicioTEF(
            ILoggerService log
            , IRepositorioTomi repTomi
            , IRepositorioVT24 repT24
            , IRepositorioTEF repTEF
            , ParametrosConfiguracion paramConfig
            , Configuracion config
            , IConexionT24Service conexionT24Service
         ) : base(log, repTomi, repT24, repTEF, conexionT24Service, paramConfig, config)
        {            
        }


        /// <summary>
        /// Metodo parea procesar los archivos de tipo TEF
        /// </summary>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="IP_Address"></param>
        /// <param name="EstatusMaxPayments"></param>
        /// <param name="EstatusAccesServcies"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<string> ProcesaOperacionAsync(ArchivosGphDTO Archivo, Pagos.PaymentInstruction Oper, string IP_Address, string EstatusMaxPayments, string EstatusAccesServcies, Pagos.GroupHeader GroupHeaderFile)
        {
            TB_TOMBIN_NF_TEF_GPH laValoresFormato = new TB_TOMBIN_NF_TEF_GPH();
            long lbNF_Folio = 0;
            FechaSistema = Conversion.StringToDate(_config.FechaSistema);
            try
            {
                _log.Info(msgs.Inicia);

                //Validaciones
                string ErrorCodes = await base.ValidacionesComunes(TipoOperacion, Oper, IP_Address, EstatusMaxPayments, EstatusAccesServcies, laValoresFormato);

                //Valida pago nuevo
                var pagoNuevo = await _repTomi.ValidarPagoNuevoTEF(FechaSistema, Oper.PmtInfId, Archivo.Nombre_Archivo);
                if (pagoNuevo)
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10018);
                }
                laValoresFormato.NF_PMT_INF_ID = Oper.PmtInfId;

                var lsFechaCalculada = await Calcula_Fecha(Oper.ReqdExctnDt);
                if (string.IsNullOrEmpty(lsFechaCalculada))
                {
                    _log.Error(msgs.ErrorFechaCalculada);
                }

                laValoresFormato.NF_TIPO = ValoresOperativos.TipoOperacionTef;
                laValoresFormato.NF_DIRECCION_IP = IP_Address;
                laValoresFormato.T24_SHORT_NAME = Oper.Dbtr?.Nm ?? string.Empty;
                laValoresFormato.T24_DEBIT_CURRENCY = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Ccy ?? string.Empty;
                laValoresFormato.T24_BEN_ACCT_NO = Oper.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id ?? string.Empty;
                laValoresFormato.NF_TIPO_CUENTA = Oper.CdtTrfTxInf?.CdtrAcct?.Tp?.Prtry ?? string.Empty;
                laValoresFormato.T24_BEN_CUST_NAME = Oper.CdtTrfTxInf?.Cdtr?.Nm ?? string.Empty;
                laValoresFormato.NF_RFC_BENEFICIARIO = Oper.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.SchmeNm?.Prtry ?? string.Empty;
                laValoresFormato.NF_TASA_RETENCION = ValoresOperativos.TasaRetencionCero;
                laValoresFormato.NF_FECHA_EJECUCION = lsFechaCalculada.Replace("-", "");
                laValoresFormato.T24_DEBIT_THEIR_REF = Oper.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf ?? string.Empty;
                laValoresFormato.NF_MONTO_ENVIAR = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;
                laValoresFormato.NF_CODIGO_OPERACION = ValoresOperativos.CodigoOperacionTef;
                laValoresFormato.NF_FECHA_CAPTURA = FechaSistema.ToString("yyyy-MM-dd");
                laValoresFormato.NF_USUARIO_CAPTURA = ValoresOperativos.UsuarioSistemaGph;
                laValoresFormato.NF_FECHA_AUTORIZA = FechaSistema.ToString("yyyy-MM-dd");
                laValoresFormato.NF_USUARIO_AUTORIZA = ValoresOperativos.UsuarioSistemaGph;
                laValoresFormato.T24_DEBIT_AMOUNT = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;
                laValoresFormato.NF_NOMBRE_ARCHIVO_GPH = Archivo.Nombre_Archivo.ToString();
                laValoresFormato.NF_FECHA_CARGO = lsFechaCalculada.Replace("-", "");
                laValoresFormato.NF_TIPO_OPERACION = (Oper.PmtTpInf?.LclInstrm?.Cd ?? string.Empty).Trim().Replace(ValoresOperativos.TipoOperacionTef, "");         // 'TIPO OPERACION

                //Valida Tipo de Cuenta
                ErrorCodes += ValidaTipoCuenta(TipoOperacion, Oper, laValoresFormato);
                //Valida  Fecha ejecucion
                ErrorCodes += await ValidaFechaEjecucion(TipoOperacion, Oper, laValoresFormato, _paramConfig.HORA_GPH_INICIO_TEF, _paramConfig.HORA_GPH_TEF_FIN_MD);
                
                // Lectura del CMS e inserción a TOMI                
                _log.Info(msgs.InsertaArchivo);                
                lbNF_Folio = await _repTomi.InsertaFormatoTEF(laValoresFormato,Archivo,Oper,  string.IsNullOrEmpty(ErrorCodes)? ValoresOperativos.EstatusPendiente : ValoresOperativos.EstatusRechazoConfig, GroupHeaderFile);
                if (lbNF_Folio > 0)
                {
                    //generar folio en  T24                                    
                    _log.Info(msgs.GenerarFolioUnico);                    
                    var folioUnico = await _conexionT24Service.GeneraFolioUnico(ConstGenerales.GSTIPO_OPERACION, "01", TipoOperacion, FechaSistema, lbNF_Folio.ToString());
                    if (!string.IsNullOrEmpty(folioUnico))
                    {
                        var resultUpdate = await _repTomi.ActualizaFolioTEF(folioUnico, lbNF_Folio);
                        if (resultUpdate)
                            _log.Info(string.Format(msgs.AsignarFolioUnico, folioUnico, lbNF_Folio));
                        else
                            ErrorCodes = $"Ocurrió un error al asignar el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                    else
                    {
                        ErrorCodes = $"Ocurrió un error al genera el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                }
                else
                {
                    ErrorCodes = $"Ocurrió un error al insertar el registro. TipoOperacion: {TipoOperacion}";
                }
              


                //valida errores
                if (string.IsNullOrEmpty(ErrorCodes) && lbNF_Folio > 0)
                {
                    _log.Info(msgs.InsertaEntradaSalida);
                    var itemResutado = await _repTomi.ConsultaRegistroTEF(lbNF_Folio);
                    if(itemResutado != null)
                    {
                        var itemInsert = new TbTombinEntradasSalidasDto()
                        {
                            Ent_Referencia = itemResutado.NF_FOLIO_UNICO,
                            Ent_Cliente = itemResutado.T24_CUSTOMER_CODE,
                            Ent_CDL = itemResutado.T24_CUSTOMER_CODE,
                            Ent_Ordenante= itemResutado.T24_COMPANY_NAME,
                            Ent_Operacion = ValoresOperativos.TipoOperacionSalidaTts,
                            Ent_Operacion_Desc= ValoresOperativos.TipoOperacionSalidaTtsDesc,
                            Ent_Tipo = ValoresOperativos.TipoSalidaDomestica,
                            Ent_Tipo_Desc = ValoresOperativos.TipoSalidaTefDomesticaDesc,
                            Ent_Cuenta = itemResutado.T24_DEBIT_ACCT_NO,
                            Ent_NombreCuenta = itemResutado.NF_NOMBRE_CUENTA,
                            Ent_Moneda_Cuenta = ValidacionesGenerales.ValidaMoneda(itemResutado.NF_MONEDA_CUENTA),
                            Ent_ViaPago = ValoresOperativos.ViaPagoTEF,
                            Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescTEF,
                            Ent_Importe = itemResutado.T24_DEBIT_AMOUNT,
                            Ent_Saldo = ValoresOperativos.SaldoCero,
                            Ent_Comentarios =itemResutado.T24_DEBIT_THEIR_REF,
                            Ent_FechaCaptura = FechaSistema.ToString("yyyyMMdd"),
                            Ent_FechaValor = itemResutado.NF_FECHA_EJECUCION,
                            Ent_Hora = DateTime.Now.ToLongTimeString(),
                            Ent_Captura= ValoresOperativos.CapturaSistema,
                            Ent_Fad = ValoresOperativos.SaldoCero,
                            Ent_Referencia_Control = ValoresOperativos.ReferenciaControl,
                            Ent_Referencia_Control_Cont = ValoresOperativos.ReferenciaControlContador
                        };

                        _log.Info(msgs.InsertaEntradaSalida);
                        var insertSalida = await _repTomi.InsertaEntradaSalida(itemInsert);
                        if (insertSalida)
                        {                            
                            _log.Info(msgs.InsertaEntradaSalidaOK);
                        }

                    }
                }

                var result = await FinalizaOperacion(Archivo, Oper, laValoresFormato.NF_FECHA_EJECUCION, ErrorCodes, TipoOperacion, lbNF_Folio);
                if (!result)
                    throw new Exception("Ocurrió un error al finalizar la operación");
                return "";
            }
            catch (ApplicationException ex)
            {
                _log.Info($"Procesos|{TipoOperacion}|{string.Format(MsgLog.Procesos.InsertaOperacionRechazada, lbNF_Folio)}");
                var insert = await _repTomi.FinalizaOperacionArchivo(Archivo, Oper, ex.Message, lbNF_Folio, ConstGenerales.OPERACION_RECHAZADA, TipoOperacion);
                if (insert)
                    _log.Info($"Procesos|{TipoOperacion}|{MsgLog.Procesos.InsertaOperacionOk}");
                return "";
            }
            catch (Exception ex) {                
                _log.Error($"{msgs.Exception}:{ex}");
                return ex.Message;
            }
            finally
            {                
                _log.Info(msgs.Fin);
            }
            
        }


        private async Task<string> Calcula_Fecha(string Fecha)
        {
            return await _repTomi.ConsultaFechaCalculada(Fecha);
        }

    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Helper;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Proceso;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Interfaces.Servicios;
using AplicacionGPH_ETL.Modelos;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using msgs = AplicacionGPH_ETL.Enums.ConstantesServicio.MsgLog.Procesos.SPID;

namespace AplicacionGPH_ETL.Servicios.ProcesosOperacion
{
    /// <summary>
    /// Clase que implementa el metodo para procesar una operacion de tipo SPID
    /// </summary>
    public class ServicioSPID : ServiciosBase, IProcesosOperacion
    {
        public List<string> TipoProceso => new List<string> { TiposCd.SPID };        
        public string TipoOperacion => OperacionTipos.SPID;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="log"></param>
        /// <param name="repTomi"></param>
        /// <param name="repT24"></param>
        /// <param name="repTEF"></param>
        /// <param name="paramConfig"></param>
        /// <param name="config"></param>
        public ServicioSPID(
            ILoggerService log
            , IRepositorioTomi repTomi
            , IRepositorioVT24 repT24
            , IRepositorioTEF repTEF
            , ParametrosConfiguracion paramConfig
            , Configuracion config
            , IConexionT24Service conexionT24Service
        ) : base(log, repTomi, repT24, repTEF, conexionT24Service, paramConfig, config)
        {
            
        }

        /// <summary>
        /// Metodo parea procesar los archivos de tipo SPID
        /// </summary>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="IP_Address"></param>
        /// <param name="EstatusMaxPayments"></param>
        /// <param name="EstatusAccesServcies"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<string> ProcesaOperacionAsync(ArchivosGphDTO Archivo, Pagos.PaymentInstruction Oper, string IP_Address, string EstatusMaxPayments, string EstatusAccesServcies, Pagos.GroupHeader GroupHeaderFile)
        {
            TB_TOMBIN_NF_TEF_GPH laValoresFormato = new TB_TOMBIN_NF_TEF_GPH();
            long lbNF_Folio;
            FechaSistema = Conversion.StringToDate(_config.FechaSistema);
            try
            {                
                _log.Info(msgs.Inicia);
                //Validaciones comunes
                string ErrorCodes = await base.ValidacionesComunes(TipoOperacion, Oper, IP_Address, EstatusMaxPayments, EstatusAccesServcies, laValoresFormato);
                
                //Valida que el pago no se haya procesado previamente                
                var pagoNuevo = await _repTomi.ValidarPagoNuevoSPID(FechaSistema, Oper.PmtInfId, Archivo.Nombre_Archivo);
                if (pagoNuevo)
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10018);

                laValoresFormato.NF_TIPO = ValoresOperativos.TipoOperacionSpd;           //19                
                laValoresFormato.NF_DIRECCION_IP = IP_Address;                                                  //'DIRECCION IP
                laValoresFormato.T24_SHORT_NAME = Oper.Dbtr?.Nm ?? string.Empty;                                                 //'CUSTOMER NAME
                laValoresFormato.T24_DEBIT_CURRENCY = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Ccy ?? string.Empty;                        //'CURRENCY                
                laValoresFormato.T24_BEN_ACCT_NO = Oper.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id?.Trim() ?? string.Empty;                 //'CUENTA BENEFICIARIO
                laValoresFormato.NF_TIPO_CUENTA = Oper.CdtTrfTxInf?.CdtrAcct?.Tp?.Prtry?.Trim() ?? string.Empty;                    //[8]'TIPO CUENTA BENEFICIARIO
                laValoresFormato.T24_BEN_CUST_NAME = Oper.CdtTrfTxInf?.Cdtr?.Nm ?? string.Empty;                                  //'BENEFICIARIO
                laValoresFormato.NF_RFC_BENEFICIARIO = Oper.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.SchmeNm?.Prtry ?? string.Empty;       //[8]'RFC BENEFICIARIO
                laValoresFormato.NF_TASA_RETENCION = ValoresOperativos.TasaRetencionCero;
                laValoresFormato.NF_FECHA_EJECUCION = (Oper.ReqdExctnDt ?? string.Empty).Replace("-", "");                        //'NF_FECHA_EJECUCION
                laValoresFormato.T24_DEBIT_THEIR_REF = Oper.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf ?? string.Empty;                //'DEBIT THEIR REF
                laValoresFormato.NF_MONTO_ENVIAR = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;                         //'DEBIT AMOUNT
                laValoresFormato.NF_FECHA_CAPTURA = FechaSistema.ToString("yyyy-MM-dd");                        //'FECHA CAPTURA
                laValoresFormato.NF_NOMBRE_ARCHIVO_GPH = Archivo.Nombre_Archivo;                                //'NOMBRE ARCHIVO
                laValoresFormato.NF_PMT_INF_ID =  Oper.PmtInfId;                                                //'ID ARCHIVO
                laValoresFormato.OP_REF_NUMERICA = Oper.CdtTrfTxInf?.PmtId?.EndToEndId ?? string.Empty;                           //'NUMERIC REFERENCE
                laValoresFormato.NF_CLASIFICACION_OPERACION = Oper.CdtTrfTxInf?.Purp?.Cd ?? string.Empty;                       //'CLASIFICACION OPERACION
                laValoresFormato.NF_RELACION_COMERCIAL = Oper.CdtTrfTxInf?.RmtInf?.Ustrd ?? string.Empty;                         //'BUSSINESS RELATIONSHIP
                laValoresFormato.NF_USUARIO_CAPTURA = ValoresOperativos.UsuarioSistemaGph;                                                    //[24] USUARIO CAPTURA
                laValoresFormato.NF_USUARIO_AUTORIZA = ValoresOperativos.UsuarioSistemaGph;                                                   //[26] USUARIO AUTORIZA
                laValoresFormato.NF_FECHA_AUTORIZA = FechaSistema.ToString("yyyy-MM-dd");                                                //[25] FECHA AUTORIZA
                laValoresFormato.T24_DEBIT_AMOUNT = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;                        //[27]'DEBIT AMOUNT

                //Valida Tipo de Cuenta
                ErrorCodes += ValidaTipoCuenta(TipoOperacion, Oper, laValoresFormato);
                //Valida  Fecha ejecucion
                ErrorCodes += await ValidaFechaEjecucion(TipoOperacion, Oper, laValoresFormato, _paramConfig.HORA_GPH_INICIO_SPID, _paramConfig.HORA_GPH_SPID_FIN_MD);

                
                if (string.IsNullOrWhiteSpace(laValoresFormato.T24_CUSTOMER_CODE)) 
                {                    
                    throw new ApplicationException("El banco del beneficiario es invalido.");
                }
                else
                {
                    _log.Info(string.Format(msgs.ObtenerDireccionCliente, laValoresFormato.T24_CUSTOMER_CODE));
                    var dataCta = await _repT24.ObtenerCustomerEdoCta(laValoresFormato.T24_CUSTOMER_CODE);                                       
                    if (dataCta != null)
                    {
                        _log.Info(msgs.DireccionClienteOk);
                        laValoresFormato.P1_CALLE = dataCta.P1_CALLE;
                        laValoresFormato.P1_CODIGO_POSTAL = dataCta.P1_CODIGO_POSTAL;
                    }
                    else
                    {                        
                        throw new ApplicationException($"No se encontraron datos del banco del beneficiario: {laValoresFormato.T24_CUSTOMER_CODE}");
                    }
                }

                //Inserta Archivo spid
                //Archivo_TOMI
                _log.Info(msgs.InsertaArchivo);
                lbNF_Folio = await _repTomi.InsertaFormatoSPID(laValoresFormato, Archivo, Oper, string.IsNullOrEmpty(ErrorCodes) ? ValoresOperativos.EstatusPendiente : ValoresOperativos.EstatusRechazoConfig, GroupHeaderFile);
                if (lbNF_Folio > 0)
                {                    
                    _log.Info(msgs.GenerarFolioUnico);                    
                    var folioUnico = await _conexionT24Service.GeneraFolioUnico(ConstGenerales.GSTIPO_OPERACION, "01", TipoOperacion, FechaSistema, lbNF_Folio.ToString());
                    if (!string.IsNullOrEmpty(folioUnico))
                    {
                        var resultUpdate = await _repTomi.ActualizaFolioSPID(folioUnico, lbNF_Folio);
                        if (resultUpdate)
                            _log.Info(string.Format(msgs.AsignarFolioUnico, folioUnico, lbNF_Folio));
                        else
                            ErrorCodes = $"Ocurrió un error al asignar el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                    else
                    {
                        ErrorCodes = $"Ocurrió un error al genera el Folio Unico. TipoOperacion: {TipoOperacion}";                        
                    }
                }
                else
                {
                    ErrorCodes = $"Ocurrió un error al  insertar el registro. TipoOperacion: {TipoOperacion}";
                }


                //Entrada/Sallida
                if (string.IsNullOrEmpty(ErrorCodes) && lbNF_Folio > 0)
                {                                
                    _log.Info(string.Format(msgs.ConsultaFolio, lbNF_Folio));
                    var itemResutado = await _repTomi.ConsultaRegistroSPID(lbNF_Folio);
                    if (itemResutado != null)
                    {
                        _log.Info(msgs.ValidaCliente);
                        if (!await _repTomi.ValidarClienteSPID(itemResutado.T24_CUSTOMER_CODE))
                            throw new ApplicationException("El cliente no esta autorizado para operar SPID");

                        var itemInsert = new TbTombinEntradasSalidasDto()
                        {
                            Ent_Referencia = itemResutado.NF_FOLIO_UNICO,
                            Ent_Cliente = itemResutado.T24_CUSTOMER_CODE,
                            Ent_CDL = itemResutado.T24_CUSTOMER_CODE,
                            Ent_Ordenante = itemResutado.T24_COMPANY_NAME,
                            Ent_Operacion = ValoresOperativos.TipoOperacionSalidaTts,
                            Ent_Operacion_Desc = ValoresOperativos.TipoOperacionSalidaTtsDesc,
                            Ent_Tipo = ValoresOperativos.TipoSalidaDomestica,
                            Ent_Tipo_Desc = ValoresOperativos.TipoSalidaDomesticaDesc,
                            Ent_Cuenta = itemResutado.T24_DEBIT_ACCT_NO,
                            Ent_NombreCuenta = itemResutado.NF_NOMBRE_CUENTA,
                            Ent_Moneda_Cuenta = ValidacionesGenerales.ValidaMoneda(itemResutado.NF_MONEDA_CUENTA),
                            Ent_ViaPago = ValoresOperativos.ViaPagoSPEI,
                            Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescSPID,
                            Ent_Importe = itemResutado.T24_DEBIT_AMOUNT,
                            Ent_Saldo = ValoresOperativos.SaldoCero,
                            Ent_Comentarios = itemResutado.T24_DEBIT_THEIR_REF,
                            Ent_FechaCaptura = FechaSistema.ToString("yyyyMMdd"),
                            Ent_FechaValor = itemResutado.NF_FECHA_EJECUCION,
                            Ent_Hora = DateTime.Now.ToLongTimeString(),
                            Ent_Captura = ValoresOperativos.CapturaSistema,
                            Ent_Fad = ValoresOperativos.SaldoCero,
                            Ent_Referencia_Control = ValoresOperativos.ReferenciaControl,
                            Ent_Referencia_Control_Cont = ValoresOperativos.ReferenciaControlContador
                        };

                        _log.Info(msgs.InsertaEntradaSalida);
                        var insertSalida = await _repTomi.InsertaEntradaSalida(itemInsert);
                        if (insertSalida)
                            _log.Info(msgs.InsertaEntradaSalidaOK);
                    }
                }
                
                var result = await FinalizaOperacion(Archivo, Oper, laValoresFormato.NF_FECHA_EJECUCION, ErrorCodes, TipoOperacion, lbNF_Folio);
                if (!result)
                    throw new ApplicationException($"Ocurrió un error al finalizar la operación con Folio: {Archivo.Folio}");
                return "";
            }
            catch(ApplicationException ex)
            {
                _log.Error($"Procesos|{TipoOperacion}|OperacionRechazada, ArchivoFolio:{Archivo.Folio}, error: {ex.Message}");                
                var insert = await _repTomi.FinalizaOperacionArchivo(Archivo, Oper, ex.Message, 0, ConstGenerales.OPERACION_RECHAZADA, TipoOperacion);
                if (insert)
                    _log.Error($"Procesos|{TipoOperacion}|{MsgLog.Procesos.InsertaOperacionOk}");
                return "";
            }
            catch (Exception ex)
            {                
                _log.Error($"{msgs.Exception}:{ex}");
                return ex.Message;
            }
            finally
            {                
                _log.Info(msgs.Fin);
            }
            
        }

    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Helper;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Proceso;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Interfaces.Servicios;
using AplicacionGPH_ETL.Modelos;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using msgs = AplicacionGPH_ETL.Enums.ConstantesServicio.MsgLog.Procesos.FXT;

namespace AplicacionGPH_ETL.Servicios.ProcesosOperacion
{
    /// <summary>
    /// Clase que implementa el metodo para procesar una operacion de tipo FXT
    /// </summary>
    public class ServicioFXT : ServiciosBase, IProcesosOperacion
    {       
        public List<string> TipoProceso => new List<string> { TiposCd.FT };        
        public string TipoOperacion => OperacionTipos.FXT;

        public ServicioFXT(
            ILoggerService log
            , IRepositorioTomi repTomi
            , IRepositorioVT24 repT24
            , IRepositorioTEF repTEF
            , ParametrosConfiguracion paramConfig
            , Configuracion config
            , IConexionT24Service conexionT24Service
         ) : base(log, repTomi, repT24, repTEF, conexionT24Service, paramConfig, config)
        {            
        }

        /// <summary>
        /// Metodo parea procesar los archivos de tipo FXT
        /// </summary>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="IP_Address"></param>
        /// <param name="EstatusMaxPayments"></param>
        /// <param name="EstatusAccesServcies"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<string> ProcesaOperacionAsync(ArchivosGphDTO Archivo, Pagos.PaymentInstruction Oper, string IP_Address, string EstatusMaxPayments, string EstatusAccesServcies, Pagos.GroupHeader GroupHeaderFile)
        {
            var laValoresFormato = new TB_TOMBIN_NF_FXI_GPH();
            long lbNF_Folio=0;
            FechaSistema = Conversion.StringToDate(_config.FechaSistema);
            try
            {
                _log.Info(msgs.Inicia);
                //Validaciones comunes
                string ErrorCodes = await base.ValidacionesComunes(TipoOperacion, Oper, IP_Address, EstatusMaxPayments, EstatusAccesServcies, laValoresFormato);

                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaXML.ValidaCurrencyAcct(Oper));
                
                //Valida que el pago no se haya procesado previamente                
                var pagoNuevo = await _repTomi.ValidarPagoNuevoFXT(FechaSistema, Oper.PmtInfId, Archivo.Nombre_Archivo);
                if (pagoNuevo)
                {                    
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10018);
                }

                //Valida Cuenta Beneficiario
                var valCta = await _repT24.ObtenerDatosCuenta(Oper.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id ?? string.Empty);
                if (valCta == null)
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10050);
                }

                laValoresFormato.NF_TIPO = ValoresOperativos.TipoOperacionFxt;                                                               //[19]
                laValoresFormato.NF_DIRECCION_IP = IP_Address;                                                  //[0]'DIRECCION IP
                laValoresFormato.T24_SHORT_NAME = Oper.Dbtr?.Nm ?? string.Empty;                                                 //[1]'CUSTOMER NAME
                laValoresFormato.T24_DEBIT_CURRENCY = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Ccy ?? string.Empty;                        //[4]'CURRENCY A
                laValoresFormato.NF_BANCO_BENEF = Oper.CdtTrfTxInf?.CdtrAgt?.FinInstnId?.ClrSysMmbId?.MmbId ?? string.Empty;        //[6]'BANCO BENEFICIARIO
                laValoresFormato.T24_BEN_ACCT_NO = Oper.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id?.Trim() ?? string.Empty;                 //[7]'CUENTA BENEFICIARIO
                laValoresFormato.NF_TIPO_CUENTA = Oper.CdtTrfTxInf?.CdtrAcct?.Tp?.Prtry?.Trim() ?? string.Empty;                    //[8]' TIPO CUENTA BENEFICIARIO
                laValoresFormato.T24_BEN_CUST_NAME = Oper.CdtTrfTxInf?.Cdtr?.Nm ?? string.Empty;                                  //[10]'BENEFICIARIO                
                laValoresFormato.NF_RFC_BENEFICIARIO = Oper.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.SchmeNm?.Prtry ?? string.Empty;       //[11]'RFC BENEFICIARIO
                laValoresFormato.NF_TASA_RETENCION = ValoresOperativos.TasaRetencionCero;                                                       //[12]
                laValoresFormato.NF_FECHA_EJECUCION = (Oper.ReqdExctnDt ?? string.Empty).Replace("-", "");                        //'NF_FECHA_EJECUCION
                laValoresFormato.T24_DEBIT_THEIR_REF = Oper.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf ?? string.Empty;                //[15]'DEBIT THEIR REF
                laValoresFormato.NF_MONTO_ENVIAR = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;                         //[18]'DEBIT AMOUNT
                laValoresFormato.NF_FECHA_CAPTURA = FechaSistema.ToString("yyyy-MM-dd");                        //[23]'FECHA CAPTURA
                laValoresFormato.NF_FECHA_AUTORIZA = FechaSistema.ToString("yyyy-MM-dd");                       //[25]'FECHA AUTORIZA 
                laValoresFormato.NF_NOMBRE_ARCHIVO_GPH = Archivo.Nombre_Archivo;                                //[28]'NOMBRE ARCHIVO
                laValoresFormato.NF_PMT_INF_ID =  Oper.PmtInfId;                                                //[29]'ID ARCHIVO
                laValoresFormato.OP_REF_NUMERICA = Oper.CdtTrfTxInf?.PmtId?.EndToEndId ?? string.Empty;                           //[20]'NUMERIC REFERENCE
                laValoresFormato.NF_USUARIO_AUTORIZA = ValoresOperativos.UsuarioSistemaGph;                                                   //[26]
                laValoresFormato.NF_USUARIO_CAPTURA = ValoresOperativos.UsuarioSistemaGph;                                                    //[24]
                laValoresFormato.T24_DEBIT_AMOUNT = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;                      //[27]   'DEBIT AMOUNT
                

                //Monedas
                string CurrencyA = laValoresFormato.CURRENCY;  //[32]
                string CurrencyB = laValoresFormato.T24_DEBIT_CURRENCY; //[4];
                if (CurrencyA != CurrencyB || CurrencyA != (Oper.CdtTrfTxInf?.CdtrAcct?.Ccy ?? string.Empty))
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10044);
                }

                //Valida Fecha Ejecución
                ErrorCodes += await ValidaFechaEjecucion(TipoOperacion, Oper, laValoresFormato, _paramConfig.HORA_GPH_INICIO_FXT, _paramConfig.HORA_GPH_FXT_FIN_MD);
                //Valida Tipo de Cuenta
                ErrorCodes += ValidaTipoCuenta(TipoOperacion, Oper, laValoresFormato);

                //' Lectura del CMS e inserción a TOMI
                _log.Info(msgs.InsertaArchivo);
                lbNF_Folio = await _repTomi.InsertaFormatoFXT(laValoresFormato, Archivo, Oper, string.IsNullOrEmpty(ErrorCodes) ? MUFG_T24.Comunes.ConstantesEstatusPlan.INI : MUFG_T24.Comunes.ConstantesEstatusPlan.RECC, GroupHeaderFile);
                if (lbNF_Folio > 0)
                {                    
                    _log.Info(msgs.GenerarFolioUnico);
                    var folioUnico = await _conexionT24Service.GeneraFolioUnico(ConstGenerales.GSTIPO_OPERACION, "01", TipoOperacion, FechaSistema, lbNF_Folio.ToString());
                    if (!string.IsNullOrEmpty(folioUnico))
                    {
                        //actualiza folio en tabla                        
                        var resultUpdate = await _repTomi.ActualizaFolioFXT(folioUnico, lbNF_Folio);
                        if (resultUpdate)
                            _log.Info(string.Format(msgs.AsignarFolioUnico, folioUnico, lbNF_Folio));
                        else
                            ErrorCodes = $"Ocurrió un error al asignar el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                    else
                    {
                        ErrorCodes = $"Ocurrió un error al genera el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                }
                else
                {
                    ErrorCodes = $"Ocurrió un error al insertar el registro. TipoOperacion: {TipoOperacion}";
                }

                var result= await FinalizaOperacion(Archivo, Oper, laValoresFormato.NF_FECHA_EJECUCION, ErrorCodes, TipoOperacion, lbNF_Folio);
                if (!result)
                    throw new Exception("Ocurrió un error al finalizar la operación");
                return "";
            }
            catch (ApplicationException ex)
            {
                _log.Info($"Procesos|{TipoOperacion}|{string.Format(MsgLog.Procesos.InsertaOperacionRechazada, lbNF_Folio)}");
                var insert = await _repTomi.FinalizaOperacionArchivo(Archivo, Oper, ex.Message, lbNF_Folio, ConstGenerales.OPERACION_RECHAZADA, TipoOperacion);
                if (insert)
                    _log.Info($"Procesos|{TipoOperacion}|Se finalizó correctamente el archivo con Folio: {lbNF_Folio}");
                return "";
            }
            catch (Exception ex)
            {
                _log.Error($"{msgs.Exception}:{ex}");
                return ex.Message;
            }
            finally
            {
                _log.Info(msgs.Fin);
            }
         
        }
    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Helper;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Proceso;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Interfaces.Servicios;
using AplicacionGPH_ETL.Modelos;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using msgs = AplicacionGPH_ETL.Enums.ConstantesServicio.MsgLog.Procesos.FXI;

namespace AplicacionGPH_ETL.Servicios.ProcesosOperacion
{
    /// <summary>
    /// Clase que implementa el metodo para procesar una operacion de tipo FXI
    /// </summary>
    public class ServicioFXI : ServiciosBase, IProcesosOperacion
    {        
        public List<string> TipoProceso => new List<string> { TiposCd.SWIFT };        
        public string TipoOperacion => OperacionTipos.FXI;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="log"></param>
        /// <param name="repTomi"></param>
        /// <param name="repT24"></param>
        /// <param name="repTEF"></param>
        /// <param name="paramConfig"></param>
        /// <param name="config"></param>
        public ServicioFXI(
            ILoggerService log
            , IRepositorioTomi repTomi
            , IRepositorioVT24 repT24
            , IRepositorioTEF repTEF
            , ParametrosConfiguracion paramConfig
            , Configuracion config
            , IConexionT24Service conexionT24Service
          ) : base(log, repTomi, repT24, repTEF, conexionT24Service, paramConfig, config)
          {
            
          }


        /// <summary>
        /// Metodo parea procesar los archivos de tipo FXI
        /// </summary>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="IP_Address"></param>
        /// <param name="EstatusMaxPayments"></param>
        /// <param name="EstatusAccesServcies"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<string> ProcesaOperacionAsync(ArchivosGphDTO Archivo, Pagos.PaymentInstruction Oper, string IP_Address
            , string EstatusMaxPayments, string EstatusAccesServcies, Pagos.GroupHeader GroupHeaderFile)
        {
            var laValoresFormato = new TB_TOMBIN_NF_FXI_GPH();
            long lbNF_Folio=0;
            FechaSistema = Conversion.StringToDate(_config.FechaSistema);
            try
            {
                _log.Info(msgs.Inicia);
                _log.Info(msgs.Validaciones);
                string ErrorCodes = await base.ValidacionesComunes(TipoOperacion, Oper, IP_Address, EstatusMaxPayments
                    , EstatusAccesServcies, laValoresFormato);

                var pagoNuevo = await _repTomi.ValidarPagoNuevoFXI(FechaSistema, Oper.PmtInfId, Archivo.Nombre_Archivo);
                if (pagoNuevo)
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10018);
                }
                
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf.RmtInf.Strd.AddtlRmtInf
                    , Codigo.Cod_1T10073));
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf.ChrgBr
                    , Codigo.Cod_1T10074));
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf.Cdtr.Nm
                    , Codigo.Cod_1T10075));
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf
                    , Codigo.Cod_1T10073));
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf?.ChrgBr
                    , Codigo.Cod_1T10074));
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf?.Cdtr?.Nm
                    , Codigo.Cod_1T10075));


                laValoresFormato.NF_TIPO = OperacionTipos.FXI;
                laValoresFormato.NF_DIRECCION_IP = string.IsNullOrEmpty(IP_Address) ? "Nothing" : IP_Address;   //'DIRECCION IP --0
                laValoresFormato.T24_SHORT_NAME = Oper.Dbtr?.Nm ?? string.Empty;                                                 //'CUSTOMER NAME
                laValoresFormato.T24_DEBIT_CURRENCY = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Ccy ?? string.Empty;                        //'CURRENCY A
                                                                                                                //laValoresFormato(4) = Oper.CdtTrfTxInf(0).CdtrAcct.Ccy                                'CURRENCY B
                laValoresFormato.T24_DEBIT_AMOUNT = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;                        //[5]laValoresFormato(5) = Oper.CdtTrfTxInf(0).Amt.InstdAmt.Value                          'DEBIT AMOUNT
                
                laValoresFormato.NF_MONTO_ENVIAR = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;                         //'DEBIT AMOUNT
                laValoresFormato.NF_FECHA_CAPTURA = FechaSistema.ToString("yyyy-MM-dd");                        //[33]'FECHA CAPTURA
                laValoresFormato.NF_FECHA_AUTORIZA = FechaSistema.ToString("yyyy-MM-dd");                        //[35]'FECHA AUTORIZA
                laValoresFormato.NF_NOMBRE_ARCHIVO_GPH = Archivo.Nombre_Archivo;                               //[38]->NOMBRE ARCHIVO --38
                laValoresFormato.NF_PMT_INF_ID = Oper.PmtInfId;                                                //[39]->ID ARCHIVO
                laValoresFormato.NF_TIPO_CUENTA = ValoresOperativos.TipoCuentaTelegraphicTransfer;
                laValoresFormato.NF_PAYMENT = ValoresOperativos.PaymentAdviseAndPay;
                laValoresFormato.NF_USUARIO_AUTORIZA = ValoresOperativos.UsuarioSistemaGph;  //[36]
                laValoresFormato.NF_USUARIO_CAPTURA = ValoresOperativos.UsuarioSistemaGph;    //[34]

                switch ((Oper.CdtTrfTxInf?.ChrgBr ?? string.Empty).Trim())
                {
                    case ValoresOperativos.ChargeDebt:
                        laValoresFormato.NF_CHARGE = ValoresOperativos.ChargeSender;
                        break;
                    case ValoresOperativos.ChargeSharedCode:
                        laValoresFormato.NF_CHARGE = ValoresOperativos.ChargeShared;
                        break;
                    case ValoresOperativos.ChargeCred:
                        laValoresFormato.NF_CHARGE = ValoresOperativos.ChargeBeneficiary;
                        break;
                    default:
                        ErrorCodes += AgregarCodigoError(TipoOperacion,Codigo.Cod_1T10039);
                        break;
                }
                laValoresFormato.NF_FECHA_EJECUCION = (Oper.ReqdExctnDt ?? string.Empty).Replace("-", "");                        //'NF_FECHA_EJECUCION
                laValoresFormato.T24_BEN_CUST_NAME = ValidaTextBESS.TextoBESSLocal(Oper.CdtTrfTxInf?.Cdtr?.Nm);                                  //'BENEFICIARIO
                
                var countryCode = Oper.CdtTrfTxInf?.Cdtr?.PstlAdr?.Ctry?.Trim();
                string NombrePais = string.IsNullOrEmpty(countryCode) ? string.Empty : await _repTomi.ObtenerNombrePais(countryCode);
                if (!string.IsNullOrEmpty(NombrePais))
                    laValoresFormato.COUNTRY_BEN = NombrePais;
                else
                    laValoresFormato.COUNTRY_BEN = string.Empty ;
                
                var lsDetailsPayments = string.Join(" ", new[] {
                        laValoresFormato.COUNTRY_BEN
                        , string.IsNullOrWhiteSpace(Oper.CdtTrfTxInf?.RmtInf?.Ustrd) ? string.Empty : Oper.CdtTrfTxInf?.RmtInf?.Ustrd
                        , string.IsNullOrWhiteSpace(Oper.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf)? string.Empty : Oper.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf
                    }.Where(a => !string.IsNullOrEmpty(a)));

                if (lsDetailsPayments.Length > 140)
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10086);
                }

                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaXML.ValidaNombreCalle(Oper));

                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaXML.ValidaCountryBen(laValoresFormato.COUNTRY_BEN));

                if (Oper.CdtTrfTxInf?.Cdtr?.PstlAdr?.AdrLine != null)
                {
                    laValoresFormato.NF_FXD_COMMENT = ValidaTextBESS.TextoBESSLocal(string.Join(" ", Oper.CdtTrfTxInf?.Cdtr?.PstlAdr?.AdrLine?.Where(s => !string.IsNullOrEmpty(s)) ?? Enumerable.Empty<string>()));
                }

                var countryForRules = Oper.CdtTrfTxInf?.Cdtr?.PstlAdr?.Ctry?.Trim() ?? string.Empty;
                var (_, addressRequired) = await _repTomi.ValidarCityAddressLineRequired(countryForRules);

                if (addressRequired)
                {
                    if (string.IsNullOrEmpty(laValoresFormato.NF_FXD_COMMENT))
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10099);
                    }
                }

                var ciudad = Oper.CdtTrfTxInf?.Cdtr?.PstlAdr?.TwnNm?.Trim();
                if (string.IsNullOrEmpty(ciudad))
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10098);
                }
                else
                {
                    if (ciudad.Length > 32 || ciudad.Length < 1)
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10096);
                    }

                    ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoBESSLocal(ciudad, Codigo.Cod_1T10097));
                    laValoresFormato.CITY_BEN = ciudad.ToUpper(); //[44]

                }

                if (!string.IsNullOrEmpty(laValoresFormato.NF_FXD_COMMENT))
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoBESSLocal(laValoresFormato.NF_FXD_COMMENT, Codigo.Cod_1T10076));
                }


                if (!string.IsNullOrEmpty(laValoresFormato.NF_FXD_COMMENT) && laValoresFormato.NF_FXD_COMMENT.Length > 100)
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10059);
                }

                var prtry = Oper.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.SchmeNm?.Prtry;
                if (!string.IsNullOrEmpty(prtry))
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(prtry, Codigo.Cod_1T10077));
                }


                laValoresFormato.NF_RFC_BENEFICIARIO = ValidaTextBESS.TextoBESSLocal(prtry);   //'RFC BENEFICIARIO
                laValoresFormato.NF_TASA_RETENCION = ValoresOperativos.TasaRetencionCero;                                   //'TASA RETENCION  
                laValoresFormato.NF_BANCO_BENEF = ValidaTextBESS.TextoBESSLocal(Oper.CdtTrfTxInf?.CdtrAgt?.FinInstnId?.Nm);   //'BEN BANK
                laValoresFormato.NF_SWIFT_BENEFICIARIO = "";
                                 
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf?.CdtrAgt?.FinInstnId?.Nm, Codigo.Cod_1T10078));

                var Bic = Oper.CdtTrfTxInf?.CdtrAgt?.FinInstnId?.BIC?.Trim() ?? string.Empty;
                var Ccy = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Ccy?.Trim() ?? string.Empty;
                var bicCorresponsal = Oper.CdtTrfTxInf?.IntrmyAgt1?.FinInstnId?.BIC?.Trim() ?? string.Empty;
                var nombreCorresponsal = Oper.CdtTrfTxInf?.IntrmyAgt1?.FinInstnId?.Nm?.Trim() ?? string.Empty;
                if (Bic.Length > 11 || Bic.Length < 8)
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10030);
                }
                else if (Bic.Substring(4, 2) == "US" && Ccy == "USD")
                {
                    if (bicCorresponsal != "")
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10090);
                    }
                    if (nombreCorresponsal != "")
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10091);
                    }
                }
                else if (Bic.Substring(4, 2) == "JP" && Ccy == "JPY")
                {
                    if (bicCorresponsal != "")
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10092);
                    }
                    if (nombreCorresponsal != "")
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10093);
                    }
                }
                else if ((Bic.Substring(4, 2) == "GB" && Ccy == "EUR") || (Bic.Substring(4, 2) == "DE" && Ccy == "EUR"))
                {
                    if (bicCorresponsal != "")
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10094);
                    }
                    if (nombreCorresponsal != "")
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10095);
                    }
                }
                else if (Bic.Substring(4, 2) == "MX" && Ccy == "USD")
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10084);
                }
                else if (Bic.Substring(4, 2) == "MX" && Ccy == "MXN")
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10085);
                }
                else
                {
                    if (bicCorresponsal.Length > 11 || bicCorresponsal.Length < 8)
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10028);
                    }
                    if (nombreCorresponsal.Length > 34 || nombreCorresponsal.Length < 1)
                    {
                        ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10029);
                    }
                }

                laValoresFormato.NF_SWIFT_BENEFICIARIO = Bic;                     //'BEN SWIFT



                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf?.IntrmyAgt1?.FinInstnId?.Nm, Codigo.Cod_1T10079));
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id, Codigo.Cod_1T10080));
                ErrorCodes += AgregarCodigoError(TipoOperacion, ValidaTextBESS.ValidaTextoError(Oper.CdtTrfTxInf?.RmtInf?.Ustrd, Codigo.Cod_1T10081));

                laValoresFormato.T24_BEN_ACCT_NO = "";  //21
                laValoresFormato.NF_BANCO_CORRESPONSAL = ValidaTextBESS.TextoBESSLocal(Oper.CdtTrfTxInf?.IntrmyAgt1?.FinInstnId?.Nm); //'BANCO CORRESPONSAL 23
                laValoresFormato.NF_SWIFT_CORRESPONSAL = Oper.CdtTrfTxInf?.IntrmyAgt1?.FinInstnId?.BIC;                //'SWIFT CORRESPONSAL 24
                laValoresFormato.NF_CUENTA_CORRESPONSAL = ValidaTextBESS.TextoBESSLocal(Oper.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id);     //'CUENTA BENEF 25                
                var nfMensaje = string.Join(" ", new[] { Oper.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf?.Trim(), Oper.CdtTrfTxInf?.RmtInf?.Ustrd?.Trim() }.Where(a => !string.IsNullOrEmpty(a)));
                laValoresFormato.NF_MENSAJE = nfMensaje;                            //[9],[27]
                laValoresFormato.T24_DEBIT_THEIR_REF = nfMensaje;                  //[27][9]'DEBIT THEIR REF
                laValoresFormato.NF_COUNTRY = "";                
                laValoresFormato.NF_ID_ARCHIVO = Archivo.Folio.ToString();      //

                string CurrencyA = laValoresFormato.T24_DEBIT_CURRENCY;     //3
                string[] CurrencysA = new[] { "JPY", "EUR", "USD" };
                if (CurrencysA.Where(a=>a.Equals(CurrencyA)).ToList().Count == 0)
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10043);
                }

                //Monedas
                if (laValoresFormato.CURRENCY != CurrencyA || laValoresFormato.CURRENCY != (Oper.CdtTrfTxInf?.CdtrAcct?.Ccy ?? string.Empty))
                {
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10044);
                }

                //Valida Fecha Ejecución
                ErrorCodes += await ValidaFechaEjecucion(TipoOperacion, Oper, laValoresFormato, _paramConfig.HORA_GPH_INICIO_FXI
                    , _paramConfig.HORA_GPH_FXI_FIN_MD);

                
                _log.Info(msgs.InsertaArchivo);
                lbNF_Folio = await _repTomi.InsertaFormatoFXI(laValoresFormato, Archivo, Oper, string.IsNullOrEmpty(ErrorCodes) ? ValoresOperativos.EstatusPendiente : ValoresOperativos.EstatusRechazoConfig, GroupHeaderFile);
                if (lbNF_Folio > 0)
                {
                    //generar folio en  T24                    
                    _log.Info(msgs.GenerarFolioUnico);
                    var folioUnico = await _conexionT24Service.GeneraFolioUnico(ConstGenerales.GSTIPO_OPERACION, "01", TipoOperacion, FechaSistema, lbNF_Folio.ToString());
                    if (!string.IsNullOrEmpty(folioUnico))
                    {
                        //actualiza folio en tabla                        
                        var resultUpdate = await _repTomi.ActualizaFolioFXI(folioUnico, lbNF_Folio);
                        if (resultUpdate)
                        {
                            _log.Info(string.Format(msgs.AsignarFolioUnico, folioUnico, lbNF_Folio));
                        }
                        else
                            ErrorCodes = $"Ocurrió un error al asignar el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                    else
                    {
                        ErrorCodes = $"Ocurrió un error al genera el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                }
                else
                {
                    ErrorCodes = $"Ocurrió un error al  insertar el registro. TipoOperacion: {TipoOperacion}";
                }


                //Registro de la Entrada/Salida
                if (string.IsNullOrEmpty(ErrorCodes) && lbNF_Folio > 0)
                {                    
                    _log.Info(string.Format(msgs.ConsultaFolio, lbNF_Folio));
                    var itemResutado = await _repTomi.ConsultaRegistroFXI(lbNF_Folio);
                    if (itemResutado != null)
                    {
                        var itemInsert = new TbTombinEntradasSalidasDto()
                        {
                            Ent_Referencia = itemResutado.NF_FOLIO_UNICO,
                            Ent_Cliente = itemResutado.T24_CUSTOMER_CODE,
                            Ent_CDL = itemResutado.T24_CUSTOMER_CODE,
                            Ent_Ordenante = itemResutado.T24_COMPANY_NAME,
                            Ent_Operacion = ValoresOperativos.TipoOperacionSalidaTts,
                            Ent_Operacion_Desc = ValoresOperativos.TipoOperacionSalidaTtsDesc,
                            Ent_Tipo = ValoresOperativos.TipoSalidaInternacional,
                            Ent_Tipo_Desc = ValoresOperativos.TipoSalidaInternacionalDesc,

                            Ent_FechaCaptura = FechaSistema.ToString("yyyyMMdd"),
                            Ent_FechaValor = itemResutado.NF_FECHA_EJECUCION,
                            Ent_Hora = DateTime.Now.ToLongTimeString(),
                            Ent_Captura = ValoresOperativos.CapturaSistema,
                            Ent_Fad = ValoresOperativos.SaldoCero,
                            Ent_Referencia_Control = ValoresOperativos.ReferenciaControl,
                            Ent_Referencia_Control_Cont = ValoresOperativos.ReferenciaControlContador,
                        };

                        if(itemResutado.T24_DEBIT_CURRENCY != ConstGenerales.MONEDAMX)
                        {
                            itemInsert.Ent_Cuenta = itemResutado.T24_DEBIT_ACCT_NO;
                            itemInsert.Ent_NombreCuenta = itemResutado.NF_NOMBRE_CUENTA;
                            itemInsert.Ent_Moneda_Cuenta = itemResutado.NF_MONEDA_CUENTA;
                            itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoSPEI;
                            itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescUsdBtmny;
                            itemInsert.Ent_Importe = itemResutado.T24_DEBIT_AMOUNT;
                            itemInsert.Ent_Saldo = ValoresOperativos.SaldoCero;
                            itemInsert.Ent_Comentarios = itemResutado.NF_MENSAJE;
                        }
                        else
                        {
                            if(itemResutado.NF_FOREX_RELACIONADO == "1")
                            {
                                itemInsert.Ent_Cuenta = itemResutado.T24_DEBIT_ACCT_NO;
                                itemInsert.Ent_NombreCuenta = itemResutado.NF_NOMBRE_CUENTA;
                                itemInsert.Ent_Moneda_Cuenta = itemResutado.NF_MONEDA_CUENTA;
                                itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoSPEI;
                                itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescUsdBtmny;
                            }
                            else
                            {
                                if(itemResutado.T24_DEBIT_CURRENCY == "USD")
                                {
                                    itemInsert.Ent_Cuenta = "3300064";
                                    itemInsert.Ent_NombreCuenta = "BTMM USD FAD";
                                    itemInsert.Ent_Moneda_Cuenta = "USD";
                                    itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoUsd;
                                    itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescUsdBtmny;
                                }
                                else if (itemResutado.T24_DEBIT_CURRENCY == "EUR")
                                {
                                    itemInsert.Ent_Cuenta = "3399940";
                                    itemInsert.Ent_NombreCuenta = "BTMM EUR FAD";
                                    itemInsert.Ent_Moneda_Cuenta = "EUR";
                                    itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoEur;
                                    itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescEur;
                                }
                                else if (itemResutado.T24_DEBIT_CURRENCY == "JPY")
                                {
                                    itemInsert.Ent_Cuenta = "3399902";
                                    itemInsert.Ent_NombreCuenta = "BTMM JPY FAD";
                                    itemInsert.Ent_Moneda_Cuenta = "JPY";
                                    itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoJpy;
                                    itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescJpy;
                                }
                                else if (itemResutado.T24_DEBIT_CURRENCY == "HKD")
                                {
                                    itemInsert.Ent_Cuenta = "3300420";
                                    itemInsert.Ent_NombreCuenta = "BTMM HKD FAD";
                                    itemInsert.Ent_Moneda_Cuenta = "HKD";
                                    itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoUsd;
                                    itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescUsdBtmny;
                                }
                                else if (itemResutado.T24_DEBIT_CURRENCY == "CAD")
                                {
                                    itemInsert.Ent_Cuenta = "3399903";
                                    itemInsert.Ent_NombreCuenta = "BTMM CAD FAD";
                                    itemInsert.Ent_Moneda_Cuenta = "CAD";
                                    itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoUsd;
                                    itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescUsdBtmny;
                                }
                                else if (itemResutado.T24_DEBIT_CURRENCY == "SGD")
                                {
                                    itemInsert.Ent_Cuenta = "3300153";
                                    itemInsert.Ent_NombreCuenta = "BTMM SGD FAD";
                                    itemInsert.Ent_Moneda_Cuenta = "SGD";
                                    itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoUsd;
                                    itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescUsdBtmny;
                                }
                                else if (itemResutado.T24_DEBIT_CURRENCY == "GBP")
                                {
                                    itemInsert.Ent_Cuenta = "3300404";
                                    itemInsert.Ent_NombreCuenta = "BTMM GBP FAD";
                                    itemInsert.Ent_Moneda_Cuenta = "GBP";
                                    itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoUsd;
                                    itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescUsdBtmny;
                                }
                                else if (itemResutado.T24_DEBIT_CURRENCY == "AUD")
                                {
                                    itemInsert.Ent_Cuenta = "3300129";
                                    itemInsert.Ent_NombreCuenta = "BTMM AUD FAD";
                                    itemInsert.Ent_Moneda_Cuenta = "AUD";
                                    itemInsert.Ent_ViaPago = ValoresOperativos.ViaPagoUsd;
                                    itemInsert.Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescUsdBtmny;
                                }
                            }
                            itemInsert.Ent_Importe = itemResutado.T24_DEBIT_AMOUNT;
                            itemInsert.Ent_Saldo = ValoresOperativos.SaldoCero;
                            itemInsert.Ent_Comentarios = itemResutado.NF_MENSAJE;
                        }

                        _log.Info(msgs.InsertaEntradaSalida);
                        var insertSalida = await _repTomi.InsertaEntradaSalida(itemInsert);
                        if (insertSalida)
                            _log.Info(msgs.InsertaEntradaSalidaOK);

                    }
                }


                var result = await FinalizaOperacion(Archivo, Oper, laValoresFormato.NF_FECHA_EJECUCION, ErrorCodes, TipoOperacion, lbNF_Folio);
                if (!result)
                    throw new ApplicationException($"Ocurrió un error al finalizar la operación con Folio: {Archivo.Folio}");
                return "";
            }
            catch (ApplicationException ex)
            {
                _log.Info($"Procesos|{TipoOperacion}|{string.Format(MsgLog.Procesos.InsertaOperacionRechazada, lbNF_Folio)}");
                var insert = await _repTomi.FinalizaOperacionArchivo(Archivo, Oper, ex.Message, lbNF_Folio, ConstGenerales.OPERACION_RECHAZADA, TipoOperacion);
                if (insert)
                    _log.Info($"Procesos|{TipoOperacion}|{MsgLog.Procesos.InsertaOperacionOk}");
                return "";
            }
            catch (Exception ex)
            {                
                _log.Error($"{msgs.Exception}:{ex}");
                return ex.Message;
            }
            finally
            {                
                _log.Info(msgs.Fin);
            }
            
        }


    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Helper;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Proceso;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Interfaces.Servicios;
using AplicacionGPH_ETL.Modelos;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using msgs = AplicacionGPH_ETL.Enums.ConstantesServicio.MsgLog.Procesos.FXD;


namespace AplicacionGPH_ETL.Servicios.ProcesosOperacion
{
    /// <summary>
    /// Clase que implementa el metodo para procesar una operacion de tipo FXD
    /// </summary>
    public class ServicioFXD : ServiciosBase, IProcesosOperacion
    {        
        public List<string> TipoProceso => new List<string> { TiposCd.SPEI01 };        
        public string TipoOperacion => OperacionTipos.FXD;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="log"></param>
        /// <param name="repTomi"></param>
        /// <param name="repT24"></param>
        /// <param name="repTEF"></param>
        /// <param name="paramConfig"></param>
        /// <param name="config"></param>
        public ServicioFXD(
            ILoggerService log
            , IRepositorioTomi repTomi
            , IRepositorioVT24 repT24
            , IRepositorioTEF repTEF
            , ParametrosConfiguracion paramConfig
            , Configuracion config
            , IConexionT24Service conexionT24Service
         ) : base(log, repTomi, repT24, repTEF, conexionT24Service,paramConfig, config)
        {
            
        }

        /// <summary>
        /// Metodo parea procesar los archivos de tipo FXD
        /// </summary>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="IP_Address"></param>
        /// <param name="EstatusMaxPayments"></param>
        /// <param name="EstatusAccesServcies"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<string> ProcesaOperacionAsync(ArchivosGphDTO Archivo, Pagos.PaymentInstruction Oper, string IP_Address, string EstatusMaxPayments, string EstatusAccesServcies, Pagos.GroupHeader GroupHeaderFile)
        {
            TB_TOMBIN_NF_TEF_GPH laValoresFormato = new TB_TOMBIN_NF_TEF_GPH();
            long lbNF_Folio = 0;
            FechaSistema = Conversion.StringToDate(_config.FechaSistema);
            try
            {
                _log.Info(msgs.Inicia);
                string ErrorCodes = await base.ValidacionesComunes(TipoOperacion, Oper, IP_Address, EstatusMaxPayments, EstatusAccesServcies, laValoresFormato);
           
                var pagoNuevo = await _repTomi.ValidarPagoNuevoFXD(FechaSistema, Oper.PmtInfId, Archivo.Nombre_Archivo);
                if (pagoNuevo)
                {                    
                    ErrorCodes += AgregarCodigoError(TipoOperacion, Codigo.Cod_1T10018);
                }
                
                laValoresFormato.NF_PMT_INF_ID = Oper.PmtInfId;

                laValoresFormato.NF_TIPO = ValoresOperativos.TipoOperacionFxd;
                laValoresFormato.NF_DIRECCION_IP = IP_Address;                                                  //'DIRECCION IP
                laValoresFormato.T24_SHORT_NAME = Oper.Dbtr?.Nm ?? string.Empty;                                                 //'CUSTOMER NAME
                laValoresFormato.T24_DEBIT_CURRENCY = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Ccy ?? string.Empty;                        //'CURRENCY                
                laValoresFormato.T24_BEN_ACCT_NO = Oper.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id?.Trim() ?? string.Empty;                        //'CUENTA BENEFICIARIO
                laValoresFormato.NF_TIPO_CUENTA = Oper.CdtTrfTxInf?.CdtrAcct?.Tp?.Prtry?.Trim() ?? string.Empty;                    //' TIPO CUENTA BENEFICIARIO
                laValoresFormato.T24_BEN_CUST_NAME = Oper.CdtTrfTxInf?.Cdtr?.Nm ?? string.Empty;                                  //'BENEFICIARIO
                laValoresFormato.NF_RFC_BENEFICIARIO = Oper.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.SchmeNm?.Prtry ?? string.Empty;       //'RFC BENEFICIARIO
                laValoresFormato.NF_TASA_RETENCION = ValoresOperativos.TasaRetencionCero;
                laValoresFormato.NF_FECHA_EJECUCION = (Oper.ReqdExctnDt ?? string.Empty).Replace("-", "");                        //'NF_FECHA_EJECUCION
                laValoresFormato.T24_DEBIT_THEIR_REF = Oper.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf ?? string.Empty;                //'DEBIT THEIR REF
                laValoresFormato.NF_MONTO_ENVIAR = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;                         //'DEBIT AMOUNT
                laValoresFormato.NF_FECHA_CAPTURA = FechaSistema.ToString("yyyy-MM-dd");                        //'FECHA CAPTURA
                laValoresFormato.NF_NOMBRE_ARCHIVO_GPH = Archivo.Nombre_Archivo;                                //'NOMBRE ARCHIVO                
                laValoresFormato.OP_REF_NUMERICA = Oper.CdtTrfTxInf?.PmtId?.EndToEndId ?? string.Empty;                           //'NUMERIC REFERENCE
                laValoresFormato.NF_USUARIO_CAPTURA = ValoresOperativos.UsuarioSistemaGph;                    //[24]
                laValoresFormato.NF_USUARIO_AUTORIZA = ValoresOperativos.UsuarioSistemaGph;                   //[26]
                laValoresFormato.NF_FECHA_AUTORIZA = FechaSistema.ToString("yyyy-MM-dd");                      //[25]
                laValoresFormato.T24_DEBIT_AMOUNT = Oper.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty;                        //[27]
                                
                ErrorCodes += ValidaTipoCuenta(TipoOperacion, Oper, laValoresFormato);

                //Valida  Fecha ejecucion
                ErrorCodes += await ValidaFechaEjecucion(TipoOperacion, Oper, laValoresFormato, _paramConfig.HORA_GPH_INICIO_FXD, _paramConfig.HORA_GPH_FXD_FIN_MD);
                                
                _log.Info(msgs.InsertaArchivo);
                lbNF_Folio = await _repTomi.InsertaFormatoFXD(laValoresFormato, Archivo, Oper, string.IsNullOrEmpty(ErrorCodes) ? ValoresOperativos.EstatusPendiente : ValoresOperativos.EstatusRechazoConfig, GroupHeaderFile);
                if (lbNF_Folio > 0)
                {
                    _log.Info(msgs.GenerarFolioUnico);                    
                    var folioUnico = await _conexionT24Service.GeneraFolioUnico(ConstGenerales.GSTIPO_OPERACION, "01", TipoOperacion, FechaSistema, lbNF_Folio.ToString());
                    if (!string.IsNullOrEmpty(folioUnico))
                    {
                        var resultUpdate = await _repTomi.ActualizaFolioFXD(folioUnico, lbNF_Folio);
                        if (resultUpdate)
                            _log.Info(string.Format(msgs.AsignarFolioUnico, folioUnico, lbNF_Folio));
                        else
                            ErrorCodes = $"Ocurrió un error al asignar el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                    else
                    {
                        ErrorCodes = $"Ocurrió un error al genera el Folio Unico. TipoOperacion: {TipoOperacion}";
                    }
                }
                else
                {
                    ErrorCodes = $"Ocurrió un error al insertar el registro. TipoOperacion: {TipoOperacion}";
                }



                //Registro de la Entrada/Salida                
                if (string.IsNullOrEmpty(ErrorCodes) && lbNF_Folio > 0)
                {
                    _log.Info(string.Format(msgs.ConsultaFolio, lbNF_Folio));
                    var itemResutado = await _repTomi.ConsultaRegistroFXD(lbNF_Folio);
                    if(itemResutado != null)
                    {
                        var itemInsert = new TbTombinEntradasSalidasDto()
                        {
                            Ent_Referencia = itemResutado.NF_FOLIO_UNICO,
                            Ent_Cliente = itemResutado.T24_CUSTOMER_CODE,
                            Ent_CDL = itemResutado.T24_CUSTOMER_CODE,
                            Ent_Ordenante = itemResutado.T24_COMPANY_NAME,
                            Ent_Operacion = ValoresOperativos.TipoOperacionSalidaTts,
                            Ent_Operacion_Desc = ValoresOperativos.TipoOperacionSalidaTtsDesc,
                            Ent_Tipo = ValoresOperativos.TipoSalidaDomestica,
                            Ent_Tipo_Desc = ValoresOperativos.TipoSalidaDomesticaDesc,
                            Ent_Cuenta = itemResutado.T24_DEBIT_ACCT_NO,
                            Ent_NombreCuenta = itemResutado.NF_NOMBRE_CUENTA,
                            Ent_Moneda_Cuenta = ValidacionesGenerales.ValidaMoneda(itemResutado.NF_MONEDA_CUENTA),
                            Ent_ViaPago = ValoresOperativos.ViaPagoSPEI,
                            Ent_ViaPago_Desc = ValoresOperativos.ViaPagoDescSPEI,
                            Ent_Importe = itemResutado.T24_DEBIT_AMOUNT,
                            Ent_Saldo = ValoresOperativos.SaldoCero,
                            Ent_Comentarios = itemResutado.T24_DEBIT_THEIR_REF,
                            Ent_FechaCaptura = FechaSistema.ToString("yyyyMMdd"),
                            Ent_FechaValor = itemResutado.NF_FECHA_EJECUCION,
                            Ent_Hora = DateTime.Now.ToLongTimeString(),
                            Ent_Captura = ValoresOperativos.CapturaSistema,
                            Ent_Fad = ValoresOperativos.SaldoCero,
                            Ent_Referencia_Control = ValoresOperativos.ReferenciaControl,
                            Ent_Referencia_Control_Cont = ValoresOperativos.ReferenciaControlContador
                        };

                        _log.Info(msgs.InsertaEntradaSalida);
                        var insertSalida = await _repTomi.InsertaEntradaSalida(itemInsert);
                        if (insertSalida)
                            _log.Info(msgs.InsertaEntradaSalidaOK);
                    }
                }

                var result= await FinalizaOperacion(Archivo, Oper, laValoresFormato.NF_FECHA_EJECUCION, ErrorCodes, TipoOperacion, lbNF_Folio);
                if (!result)
                    throw new ApplicationException($"Ocurrió un error al finalizar la operación con Folio: {Archivo.Folio}");
                return "";
            }
            catch (ApplicationException ex)
            {
                _log.Info($"Procesos|{TipoOperacion}|{string.Format(MsgLog.Procesos.InsertaOperacionRechazada, lbNF_Folio)}");
                var insert = await _repTomi.FinalizaOperacionArchivo(Archivo, Oper, ex.Message, lbNF_Folio, ConstGenerales.OPERACION_RECHAZADA, TipoOperacion);
                if (insert)
                    _log.Info($"Procesos|{TipoOperacion}|{MsgLog.Procesos.InsertaOperacionOk}");
                return "";
            }
            catch (Exception ex) {
                _log.Error($"{msgs.Exception}:{ex}");
                return ex.Message;
            }
            finally
            {
                _log.Info(msgs.Fin);
            }
            
        }
    }
}
using AplicacionGPH_ETL.Comunes;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using System.Xml.Serialization;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using static AplicacionGPH_ETL.Modelos.Pagos;


namespace AplicacionGPH_ETL.Helper
{
    /// <summary>
    /// Validaciones generales para el contenido del archivo xml y regresos de códigos de errores si no se  cumple la validación correspondiente
    /// </summary>
    public static class ValidaXML
    {
        private static readonly string strOK = string.Empty;


        /// <summary>
        /// Método para validar dirección Ip
        /// </summary>
        /// <param name="ipAddress"></param>
        /// <returns></returns>
        public static string ValidaIpAddress(string ipAddress)
        {
            string ipPattern = @"^([01]?\d\d?|2[0-4]\d|25[0-5])\.([01]?\d\d?|2[0-4]\d|25[0-5])\.([01]?\d\d?|2[0-4]\d|25[0-5])\.([01]?\d\d?|2[0-4]\d|25[0-5])$";
            var valorIp = (ipAddress ?? string.Empty).Trim();
            return (!Regex.IsMatch(valorIp, ipPattern) ? Codigo.Cod_1T10057 : strOK);
        }

        /// <summary>
        /// Método para validar tipo TRF
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string EsTRF(PaymentInstruction oper)
        {
            var pmtMtd = (oper?.PmtMtd ?? string.Empty).Trim().ToUpper();
            return (pmtMtd != "TRF" ? Codigo.Cod_1T10001 : strOK);
        }

        /// <summary>
        /// Metodo para validar formato de fecha de ejecución
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaFormatoFechaEjecucion(PaymentInstruction oper)
        {
            string datePattern = @"^[0-9]{4}-(((0[13578]|(10|12))-(0[1-9]|[1-2][0-9]|3[0-1]))|(02-(0[1-9]|[1-2][0-9]))|((0[469]|11)-(0[1-9]|[1-2][0-9]|30)))$";
            var reqdExctnDt = oper?.ReqdExctnDt ?? string.Empty;
            return (!Regex.IsMatch(reqdExctnDt, datePattern) ? Codigo.Cod_1T10003 : strOK);
        }

        /// <summary>
        /// Método para validar longitud de cuenta
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaLongitudCuenta(PaymentInstruction oper)
        {
            var cuenta = oper?.DbtrAcct?.Id?.Othr?.Id ?? string.Empty;
            return (cuenta.Length != 18 ? Codigo.Cod_1T10006 : strOK);
        }

        /// <summary>
        /// ´Valida codigo BTMUM
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaBTMUMCODE(PaymentInstruction oper)
        {
            var mmbId = (oper?.DbtrAgt?.FinInstnId?.ClrSysMmbId?.MmbId ?? string.Empty).Trim();
            return (mmbId != "40108" ? Codigo.Cod_1T10007 : strOK);
        }
        /// <summary>
        /// Válida custom reference
        /// </summary>
        /// <param name="EndToEndId"></param>
        /// <param name="longitud"></param>
        /// <returns></returns>
        public static string ValidaCustomerReference(string EndToEndId, int longitud)
        {
            var endToEndId = EndToEndId ?? string.Empty;
            return ((endToEndId.Length > longitud || endToEndId.Length < 1) ? Codigo.Cod_1T10064 : strOK);
        }

        /// <summary>
        /// Válida numeric Customer reference
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string NumericCustomerReference(PaymentInstruction oper)
        {
            string validPattern = @"^\d+$";
            var endToEndId = (oper?.CdtTrfTxInf?.PmtId?.EndToEndId ?? string.Empty).Trim();
            return (!Regex.IsMatch(endToEndId, validPattern) ? Codigo.Cod_1T10100 : strOK);
        }

        /// <summary>
        /// Válida longitud de cuenta de debito
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaDebitAmount(PaymentInstruction oper)
        {
            var amount = (oper?.CdtTrfTxInf?.Amt?.InstdAmt?.Value ?? string.Empty).Trim();
            return ((amount.Length > 15 || amount.Length < 1) ? Codigo.Cod_1T10009 : strOK);
        }

        /// <summary>
        /// Válida si es tipo de moneda USD
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaMonedaUSD(PaymentInstruction oper)
        {
            var ccy = (oper?.CdtTrfTxInf?.Amt?.InstdAmt?.Ccy ?? string.Empty).Trim();
            return (ccy != "USD" ? Codigo.Cod_1T10010 : strOK);
        }

        /// <summary>
        /// Válida si es tipo de moneda MXN
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaMonedaMXN(PaymentInstruction oper)
        {
            var ccy = (oper?.CdtTrfTxInf?.Amt?.InstdAmt?.Ccy ?? string.Empty).Trim();
            return (ccy != "MXN" ? Codigo.Cod_1T10035 : strOK);
        }

        /// <summary>
        /// Válida longitud de banco beneficiario
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaCodigoBancoBeneficiario(PaymentInstruction oper)
        {
            var mmbId = (oper?.CdtTrfTxInf?.CdtrAgt?.FinInstnId?.ClrSysMmbId?.MmbId ?? string.Empty).Trim();
            return (mmbId.Length != 5 ? Codigo.Cod_1T10011 : strOK);
        }

        /// <summary>
        /// Válida longitud de nombre de beneficiario
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaNombreBeneficiario(PaymentInstruction oper)
        {
            var nombreBeneficiario = (oper?.CdtTrfTxInf?.Cdtr?.Nm ?? string.Empty).Trim();
            return ((nombreBeneficiario.Length > 100) || nombreBeneficiario.Length < 1 ? Codigo.Cod_1T10012 : strOK);
        }

        /// <summary>
        /// Válida longitud de nombre de banco beneficairio
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaNombreBancoBeneficiarioFXI(PaymentInstruction oper)
        {
            var nombreBanco = (oper?.CdtTrfTxInf?.CdtrAgt?.FinInstnId?.Nm ?? string.Empty).Trim();
            return ((nombreBanco.Length > 34) || nombreBanco.Length < 1 ? Codigo.Cod_1T10032 : strOK);
        }

        /// <summary>
        /// Válida longitud de cuenta de beneficiario
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaCuentaBeneficiarioFXI(PaymentInstruction oper)
        {
            var cuentaBeneficiario = (oper?.CdtTrfTxInf?.CdtrAcct?.Id?.Othr?.Id ?? string.Empty).Trim();
            return ((cuentaBeneficiario.Length > 34) || cuentaBeneficiario.Length < 1 ? Codigo.Cod_1T10049 : strOK);
        }


        /// <summary>
        /// Válida caracter de persona moral
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaTipoPersonaMoral(PaymentInstruction oper)
        {
            var tipoPersona = (oper?.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.Id ?? string.Empty).Trim().ToUpper();
            return tipoPersona != "M" ? Codigo.Cod_1T10040 : strOK;
        }

        
        /// <summary>
        /// Válida persona fisica
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaPatronPersonaFisica(PaymentInstruction oper)
        {
            string taxIdPatternF = @"^[A-Za-z]{4}[ \-]{0,1}[0-9]{6}[ |\-]{0,1}[0-9A-Za-z]{3}$";
            string valor = (oper?.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.SchmeNm?.Prtry ?? string.Empty).Trim().ToUpper();
            var tipoPersona = (oper?.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.Id ?? string.Empty).Trim().ToUpper();

            if (tipoPersona == "F")
            {
                return (!Regex.IsMatch(valor, taxIdPatternF) ? Codigo.Cod_1T10013 : strOK);
            }
            return strOK;
        }


        /// <summary>
        /// v{alida longitud de persona moral
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaPatronPersonaMoral(PaymentInstruction oper)
        {
            string taxIdPatternM = @"^[A-Za-z]{3}[ \-]{0,1}[0-9]{6}[ |\-]{0,1}[0-9A-Za-z]{3}$";
            string valor = (oper?.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.SchmeNm?.Prtry ?? string.Empty).Trim().ToUpper();
            var tipoPersona = (oper?.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.Id ?? string.Empty).Trim().ToUpper();

            if (tipoPersona == "M")
            {
                return (!Regex.IsMatch(valor, taxIdPatternM)? Codigo.Cod_1T10005 : strOK);
            }
            return strOK;
        }



        /// <summary>
        /// Válida tipo de persona fisica
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaTipoPersona(PaymentInstruction oper)
        {
            string taxTypePattern = @"^(M|F)$";
            var tipoPersona = (oper?.CdtTrfTxInf?.Cdtr?.Id?.OrgId?.Othr?.Id ?? string.Empty).Trim().ToUpper();
            return (!Regex.IsMatch(tipoPersona, taxTypePattern) ? Codigo.Cod_1T10014 : strOK);
        }



        /// <summary>
        /// Valida clasificacion
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaClasificacion(PaymentInstruction oper)
        {
            string validPattern = @"^(01|02|03|04|05|06|07)$";
            var clasificacion = (oper?.CdtTrfTxInf?.Purp?.Cd ?? string.Empty).Trim();
            return (!Regex.IsMatch(clasificacion, validPattern) ? Codigo.Cod_1T10054 : strOK);
        }

        /// <summary>
        /// Valida dato regularorio
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string EsRegulatorio(PaymentInstruction oper)
        {
            var regulatorio = (oper?.CdtTrfTxInf?.RgltryRptg?.Dtls?.Inf ?? string.Empty).Trim().ToUpper();
            return (regulatorio != "NNK NI" ? Codigo.Cod_1T10053 : strOK);
        }

        /// <summary>
        /// valida relacion de negocio
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaRelacionNegocio(PaymentInstruction oper)
        {
            // Recomendación: utilizar `@"^0[1-7]$"` (o `^(01|02|03|04|05|06|07)$`)
            string validPattern = @"^(01|02|03|04|05|06|07)$";
            var relacionNegocio = (oper?.CdtTrfTxInf?.RmtInf?.Ustrd ?? string.Empty).Trim();
            return (!Regex.IsMatch(relacionNegocio, validPattern) ? Codigo.Cod_1T10055 : strOK);
        }

        /// <summary>
        /// Vallida referencia de pago
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaReferenciaPago(PaymentInstruction oper)
        {
            var referenciaPago = (oper?.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf ?? string.Empty).Trim();
            return ((referenciaPago.Length > 40 || referenciaPago.Length < 1) ? Codigo.Cod_1T10017 : strOK);
        }


        /// <summary>
        /// Valida currency
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaCurrencyAcct(PaymentInstruction oper)
        {
            string validPattern = @"^(MXN|USD|JPY|EUR)$";
            var currency = (oper?.CdtTrfTxInf?.CdtrAcct?.Ccy ?? string.Empty).Trim().ToUpper();
            return (!Regex.IsMatch(currency, validPattern) ? Codigo.Cod_1T10052 : strOK);
        }

        /// <summary>
        /// valida tipo de moneda
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaMonedaFXI(PaymentInstruction oper)
        {
            string validPattern = @"^(USD|JPY|EUR)$";
            var moneda = (oper?.CdtTrfTxInf?.CdtrAcct?.Ccy ?? string.Empty).Trim().ToUpper();
            return (!Regex.IsMatch(moneda, validPattern) ? Codigo.Cod_1T10036 : strOK);
        }

        /// <summary>
        /// valida tipo de moneda
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaDebitCurrencyFXT(PaymentInstruction oper)
        {
            string validPattern = @"^(MXN|USD|JPY|EUR)$";
            var moneda = (oper?.CdtTrfTxInf?.CdtrAcct?.Ccy ?? string.Empty).Trim().ToUpper();
            return (!Regex.IsMatch(moneda, validPattern) ? Codigo.Cod_1T10034 : strOK);
        }

        /// <summary>
        /// valida codigo de BENEFBANK
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaBENEFBANK_CODE(PaymentInstruction oper)
        {
            var mmbId = (oper?.CdtTrfTxInf?.CdtrAgt?.FinInstnId?.ClrSysMmbId?.MmbId ?? string.Empty).Trim();
            return (mmbId != "40108" ? Codigo.Cod_1T10083 : strOK);
        }


        /// <summary>
        /// Valida longitud de nombre de calle
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaNombreCalle(PaymentInstruction oper)
        {
            var nombreCalle = oper?.CdtTrfTxInf?.Cdtr?.PstlAdr?.StrtNm;
            return (!string.IsNullOrEmpty((nombreCalle ?? string.Empty).Trim()) ? Codigo.Cod_1T10089 : strOK);
        }

        /// <summary>
        /// Valida longitud de pais de beneficairio
        /// </summary>
        /// <param name="countryBen"></param>
        /// <returns></returns>
        public static string ValidaCountryBen(string countryBen)
        {            
            return (string.IsNullOrEmpty(countryBen) ? Codigo.Cod_1T10060 : strOK);
        }


        /// <summary>
        /// Metodo para validar informacion del remitente y regresar código de error en caso de no cumplir la condición
        /// </summary>
        /// <param name="oper"></param>
        /// <returns></returns>
        public static string ValidaInfoAdicionalRemitente(PaymentInstruction oper)
        {
            var infoAdicional = (oper?.CdtTrfTxInf?.RmtInf?.Strd?.AddtlRmtInf ?? string.Empty).Trim();
            return ((infoAdicional.Length > 40 || infoAdicional.Length < 1) ? Codigo.Cod_1T10082 : strOK);
        }



        /// <summary>
        /// Metodo para serializar un objeto
        /// </summary>
        /// <param name="Tipo"></param>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string SerializaObjetoXML(string Tipo, object obj)
        {
            StringWriter stringWriter = new StringWriter();
            XmlSerializer xmlSerializer;
            if (Tipo == ConstGenerales.TIPO_GROUP_HEADER)
            {
                xmlSerializer = new XmlSerializer(typeof(GroupHeader));
            }
            else
            {
                xmlSerializer = new XmlSerializer(typeof(PaymentInstruction));
            }
            xmlSerializer.Serialize(stringWriter, obj);

            return stringWriter.ToString();
        }

        public static T Deserealizar<T>(XElement nodo) where T: class
        {
            if (nodo == null)
                throw new ArgumentNullException(nameof(nodo));
            var serializar = ObtenerSerializer(typeof(T), nodo.Name);
            using (var reader = nodo.CreateReader())
            {
                return (T)serializar .Deserialize(reader);
            }
        }

        private static readonly ConcurrentDictionary<string, XmlSerializer> _serializers = new ConcurrentDictionary<string, XmlSerializer>();
        private static XmlSerializer ObtenerSerializer(Type tipo, XName nombre)
        {
            string key = $"{tipo.FullName}|{nombre.Namespace}|{nombre.LocalName}";
            return _serializers.GetOrAdd(key, _ =>
                new XmlSerializer(
                    tipo,
                    null,
                    null,
                    new XmlRootAttribute
                    {
                        ElementName = nombre.LocalName,
                        Namespace = nombre.NamespaceName
                    },
                    nombre.NamespaceName
                    )
            );
        }


        /// <summary>
        /// Obtiene el numero de nodos CdtTrfTxInf dentro del xml por pagos
        /// </summary>
        /// <param name="rutaXml"></param>
        /// <returns></returns>
        public static bool ValidaNoNodosCdtTrfTxInf(XDocument doc, out string error) 
        {
            try
            {
                error = null;
                var cstmr = doc.Descendants().Where(e => e.Name.LocalName == "CstmrCdtTrfInitn");
                if (cstmr == null)
                {
                    error = "No existe el nodo [CstmrCdtTrfInitn]";
                    return false;
                }

                //debe tener uno omas nodos de tipo [PmtInf]
                var pmtInfNodes = cstmr.Elements().Where(e => e.Name.LocalName == "PmtInf").ToList();
                if(pmtInfNodes.Count == 0)
                {
                    error = "[CstmrCdtTrfInitn] debe contener al menos un un nod [PmtInf]";
                    return false;
                }

                //Cada [PmtInf] no debe contener mas de un nodo [CdtTrfTxInf]
                var invalidos = pmtInfNodes.Select((pmt, index) => new
                    {
                        posicion = index + 1,
                        Total = pmtInfNodes.Elements().Count(e => e.Name.LocalName == "CdtTrfTxInf")
                    })
                    .Where(x => x.Total > 1)
                    .ToList();

                if (invalidos.Any())
                {
                    error = "Los siguientes nodos [PmtInf] tienen mas de un [CdtTrfTxInf]: " +  string.Join(", ", invalidos.Select(x=>$"#{x.posicion}  ({x.Total})")) ;
                    return false;
                }

                return true;
            }
            catch (Exception ex) {
                throw new Exception(ex.Message);
            }

        }

    }



    public static class ValidacionesGenerales
    {
        /// <summary>
        /// Método para validar caracteres de moneda
        /// </summary>
        /// <param name="moneda"></param>
        /// <returns></returns>
        public static string ValidaMoneda(string moneda)
        {
            if (string.Equals(moneda, Moneda.MXP.ToString()) || string.Equals(moneda, Moneda.MXN.ToString()))
                return Moneda.MXN.ToString();
            return moneda;
        }

        /// <summary>
        /// Metodo para validar cuenta CLABE
        /// </summary>
        /// <param name="NumCuenta"></param>
        /// <param name="Banco"></param>
        /// <returns></returns>
        public static int ObtenerCuentaCLABE(string NumCuenta , string Banco )
        {
            try
            {
                int intValores, intSuma, intCaracter, intMultiplo, intBancoAuxDV;
                if (string.IsNullOrEmpty(NumCuenta) || string.IsNullOrEmpty(Banco) || Banco.Length < 5)
                {
                    return 2;
                }
                if (NumCuenta.Length == 18)
                {
                    var strBancoAux = Banco.Substring(2, 3);
                    var strBancoAuxCuenta = NumCuenta.Substring(0, 3);
                    if (strBancoAux == strBancoAuxCuenta)
                    {                        
                        var strBancoAuxDV = NumCuenta.Substring(17, 1);
                        var strBancoAuxCuentaClabe = NumCuenta.Substring(0, 17);
                        intValores = 0;
                        var strMultiplicador = "37137137137137137";
                        for (int i = 0; i < 17; i++)
                        {
                            intCaracter = ConversionesComunes.ParseEnteroRequerido(strBancoAuxCuentaClabe.Substring(i, 1), "CLABE_Digito");
                            intMultiplo = ConversionesComunes.ParseEnteroRequerido(strMultiplicador.Substring(i, 1), "CLABE_Multiplicador");

                            intSuma = (intCaracter * intMultiplo) % 10;
                            intValores += intSuma;
                        }
                        intValores %= 10;
                        intValores = 10 - intValores;
                        if (intValores == 10)
                            intValores = 0;
                        intBancoAuxDV = Conversion.StringToInt32(strBancoAuxDV);
                        if (intValores == intBancoAuxDV)
                        {
                            return 1;  //Clabe válida
                        }
                        else
                        {
                            return 3;
                        }
                    }
                    else
                    {
                        return 4;
                    }
                }
                return 2;
            }
            catch (Exception ) {                
                return 2;
            }
        }

    }



}
namespace AplicacionGPH_ETL.Modelos
{
    /// <summary>
    /// Clase que contiene las propiedades/nodos que conforman los archivos xml
    /// y se utiliza para serializar el contenido de los archivos 
    /// </summary>
    public class Pagos
    {
       
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlRootAttribute(Namespace = "", IsNullable = false)]
        public partial class Document
        {

            public CustomerCreditTransferInitiation CstmrCdtTrfInitn;
        }


        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class CustomerCreditTransferInitiation
        {

            public GroupHeader GrpHdr;

            [System.Xml.Serialization.XmlElementAttribute("PmtInf")]
            public PaymentInstruction[] PmtInf;
        }

        /// <remarks/>        
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class GroupHeader
        {

            public string MsgId;

            public string CreDtTm;

            public Authorisation1Choice Authstn;

            public string NbOfTxs;

            public object InitgPty;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class Authorisation1Choice
        {

            [System.Xml.Serialization.XmlElementAttribute("Prtry")]
            public string Prtry;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class PaymentTypeInformation
        {

            public LocalInstrumentChoice LclInstrm;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class LocalInstrumentChoice
        {

            [System.Xml.Serialization.XmlElementAttribute("Cd")]
            public string Cd;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class PaymentInstruction
        {

            public string PmtInfId;

            public string PmtMtd;

            public PaymentTypeInformation PmtTpInf;

            public string ReqdExctnDt;

            public PartyIdentification43 Dbtr;

            public CashAccount24 DbtrAcct;

            public BranchAndFinancialInstitutionIdentification5 DbtrAgt;

            public CreditTransferTransaction26 CdtTrfTxInf;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class PartyIdentification43
        {

            public string Nm;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public PostalAddress6 PstlAdr;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public Party11Choice Id;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class PostalAddress6
        {

            public string StrtNm;

            public string TwnNm;

            public string Ctry;

            [System.Xml.Serialization.XmlElementAttribute("AdrLine", IsNullable = true)]
            public string[] AdrLine;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class Party11Choice
        {

            [System.Xml.Serialization.XmlElementAttribute("OrgId")]
            public OrganisationIdentification8 OrgId;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class OrganisationIdentification8
        {

            public GenericOrganisationIdentification1 Othr;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class GenericOrganisationIdentification1
        {

            public string Id;

            public OrganisationIdentificationSchemeName1Choice SchmeNm;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class OrganisationIdentificationSchemeName1Choice
        {

            public string Cd;

            public string Prtry;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class CashAccount24
        {

            public AccountIdentification4Choice Id;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public CashAccountType2Choice Tp;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public string Ccy;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class AccountIdentification4Choice
        {

            [System.Xml.Serialization.XmlElementAttribute("Othr")]
            public GenericAccountIdentification1 Othr;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class GenericAccountIdentification1
        {

            public string Id;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class CashAccountType2Choice
        {

            [System.Xml.Serialization.XmlElementAttribute("Prtry")]
            public string Prtry;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class BranchAndFinancialInstitutionIdentification5
        {

            public FinancialInstitutionIdentification8 FinInstnId;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class FinancialInstitutionIdentification8
        {

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public string BIC;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public ClearingSystemMemberIdentification2 ClrSysMmbId;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public string Nm;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public PostalAddress6 PstlAdr;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class ClearingSystemMemberIdentification2
        {

            public string MmbId;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class CreditTransferTransaction26
        {

            public PaymentIdentification1 PmtId;

            public AmountType4Choice Amt;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public string ChrgBr;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public BranchAndFinancialInstitutionIdentification5 IntrmyAgt1;

            public BranchAndFinancialInstitutionIdentification5 CdtrAgt;

            public PartyIdentification43 Cdtr;

            public CashAccount24 CdtrAcct;

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public Purpose2Choice Purp;

            public RegulatoryReporting3 RgltryRptg;

            public RemittanceInformation11 RmtInf;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class PaymentIdentification1
        {

            public string EndToEndId;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class AmountType4Choice
        {

            [System.Xml.Serialization.XmlElementAttribute("InstdAmt")]
            public ActiveOrHistoricCurrencyAndAmount InstdAmt;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class ActiveOrHistoricCurrencyAndAmount
        {

            [System.Xml.Serialization.XmlAttributeAttribute()]
            public string Ccy;

            [System.Xml.Serialization.XmlTextAttribute()]
            public string Value;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class Purpose2Choice
        {

            [System.Xml.Serialization.XmlElementAttribute("Cd")]
            public string Cd;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class RegulatoryReporting3
        {

            public StructuredRegulatoryReporting3 Dtls;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class StructuredRegulatoryReporting3
        {

            public string Inf;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class RemittanceInformation11
        {

            [System.Xml.Serialization.XmlElementAttribute(IsNullable = true)]
            public string Ustrd;

            public StructuredRemittanceInformation13 Strd;
        }

        /// <remarks/>
        [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
        [System.SerializableAttribute()]
        [System.Diagnostics.DebuggerStepThroughAttribute()]
        [System.ComponentModel.DesignerCategoryAttribute("code")]
        [System.Xml.Serialization.XmlTypeAttribute(Namespace = "")]
        public partial class StructuredRemittanceInformation13
        {

            public string AddtlRmtInf;
        }
    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Interfaces.Alertas;
using AplicacionGPH_ETL.Interfaces.ControlError;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Conexiones;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using static AplicacionGPH_ETL.Modelos.Pagos;
using Sp = AplicacionGPH_ETL.Enums.ConstantesServicio.Sp.Tomi;


namespace Infraestructura.GPH_ETL.Repositorios.Tomi
{
    /// <summary>
    /// Metodos de enlace a la base de datos tomi
    /// </summary>
    public class RepositorioTomi : RepositorioMaestro,  IRepositorioTomi
    {
        private readonly ILoggerService _log;
        private readonly IControlErrorService _controlErrorService;
        private readonly IExtensionAlerta _extAlerta;

        public RepositorioTomi(
            ILoggerService log
            , IControlErrorService controlErrorService
            , IExtensionAlerta extAlerta
        ) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) 
        {
            _log = log;
            _controlErrorService = controlErrorService;
            _extAlerta = extAlerta;
        }

        /// <summary>
        /// Metodo para validar conexión con base de datos
        /// </summary>
        /// <returns></returns>
        public async Task<bool> ValidaAccesoBdTomiAsync()
        {
            try
            {
                _log.Info(MsgLog.Bd.Tomi.ValidaAccesoInicio);
                var cnn = ConexionBasedatosFactory.CrearConexion(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI);
                using (var cn = cnn.CrearConexion())
                {
                    await cn.OpenAsync();
                    
                    if (cn.State == ConnectionState.Open)
                    {
                        _log.Info(MsgLog.Bd.Tomi.ConexionOk);
                        _controlErrorService.LimpiaControlError(1);
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error($"{MsgLog.Bd.Tomi.Exception} : {ex}");
                await _controlErrorService.ControlErrorAsync(1, ex);
                //Registra alerta                
                await _extAlerta.RegistraCriticoAsync(MsgLog.Bd.Tomi.msgAcceso, ex);
            }
            finally
            {
                _log.Info(MsgLog.Bd.Tomi.ValidaAccesoFin);
            }
            return false;
        }

        /// <summary>
        /// Método para obtener fecha habil desde base de Datos
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task<bool> EsDiaHabilAsync()
        {

            var resultadoSp = await EjecutarStoredProcedureQuery<CampoGeneral>(
                EnumNombreSp.SP_ES_DIA_HABIL,
                cmd => cmd.Parameters.Add(
                    basedatosConexion.CrearParametro("@fecha", DateTime.Now.Date.ToString(EnumParametrosGenerales.FECHA_HORA0_BD)
                    )
                )              
            );

            if (!resultadoSp.Exito || !string.IsNullOrEmpty(resultadoSp.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ProcesoEsDiaHabil}{MsgLog.Bd.ErrorEjecucionGral}{resultadoSp.MensajeError}");
                throw new Exception(resultadoSp.MensajeError);
            }

            if (resultadoSp.Data.FirstOrDefault()?.Num_regs > 0)
                return false;

            return true;
        }




        /// <summary>
        /// Metodo que actualiza la clave del msg id del archivo maestro a traves del Folio
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="TipoOperacion"></param>
        /// <param name="MsgId"></param>
        /// <returns></returns>
        public async Task<bool> MaestroActualizaMsgId(long Folio, string TipoOperacion, string MsgId)
        {
            var result =await EjecutarStoredProceduredNonQuery( Sp.uspMaestroActualizaMsgId, cmd =>
                {                    
                    cmd.AddInt64Param("@Folio", Folio);
                    cmd.AddStringParam("@TipoOperacion", TipoOperacion);
                    cmd.AddStringParam("@MsgId", MsgId);
                });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }
            return result.Exito;
        }


        /// <summary>
        /// Consulta para obtener la lista de operaciones/Archivos a procesar
        /// </summary>
        /// <param name="FechaSistema"></param>
        /// <returns></returns>
        public async Task<List<ArchivosGphDTO>> ObtenerListaOperaciones(DateTime FechaSistema)
        {
            var result = await EjecutarStoredProcedureQuery<ArchivosGphDTO>( Sp.uspObtenerListaOperaciones, cmd =>
            {                
                cmd.AddDateTimeParam("FechaSistema", FechaSistema);
            });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {                
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Data;
        }

        /// <summary>
        /// Consulta para vallidar si un cliente tiene acceo de operación al servicio
        /// </summary>
        /// <param name="CustomerId"></param>
        /// <returns></returns>
        public async Task<int> ValidarAccesoServicioAsync(long CustomerId)
        {
            // Creamos parámetros de salida.
            SqlParameter outConteo = null;            
            var result = await EjecutarStoredProceduredNonQuery( Sp.uspCuentaServiciosCliente, cmd =>
            {             
                cmd.AddInt64Param("CustomerId", CustomerId);
                outConteo = cmd.AddOutPutInt("@Conteo");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outConteo.GetValue<int>(); 
        }

        /// <summary>
        /// COnsulta para obtener folio de operacion
        /// </summary>
        /// <param name="FechaEjecucion"></param>
        /// <param name="NfPmtInfId"></param>
        /// <param name="NfNombreArchivoGph"></param>
        /// <param name="TipoOperacion"></param>
        /// <returns></returns>
        public async Task<long> ObtenerFolioOperacion(DateTime FechaEjecucion, string NfPmtInfId, string NfNombreArchivoGph
            , string TipoOperacion)
        {
            SqlParameter outFolioOperacion = null;
            var result =  await EjecutarStoredProceduredNonQuery(Sp.uspObtenerFolioOperacion, cmd =>
                {
                    cmd.CommandTimeout = 60;
                    cmd.AddDateTimeParam("@FechaEjecucion", FechaEjecucion);
                    cmd.AddStringParam("@NfPmtInfId", NfPmtInfId);
                    cmd.AddStringParam("@NfNombreArchivoGph", NfNombreArchivoGph);
                    cmd.AddStringParam("@TipoOperacion", TipoOperacion);
                    outFolioOperacion = cmd.AddOutPutInt64("@Folio");
                });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outFolioOperacion.GetValue<long>();
        }


        /// <summary>
        /// Operacion para insertar registro de operacion en bd
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="TipoOperacion"></param>
        /// <param name="PmtInfId"></param>
        /// <param name="Estatus"></param>
        /// <param name="ErrorCodes"></param>
        /// <param name="EndToEndId"></param>
        /// <param name="FolioOperacion"></param>
        /// <returns></returns>
        public async Task<bool> InsertaOperacionTOMI(long Folio, string TipoOperacion, string PmtInfId, string Estatus
            , string ErrorCodes, string EndToEndId, long FolioOperacion)
        {
            var result =  await EjecutarStoredProceduredNonQuery(Sp.uspInsertaOperacion, cmd =>
                {
                    cmd.AddInt64Param("@Folio", Folio);
                    cmd.AddStringParam("@TipoOperacion", TipoOperacion);
                    cmd.AddStringParam("@PmtInfId", PmtInfId);
                    cmd.AddStringParam("@Estatus", Estatus);
                    cmd.AddStringParam("@Error", ErrorCodes);
                    cmd.AddStringParam("@EndToEndId", EndToEndId);
                    cmd.AddInt64Param("@FolioOperacion", FolioOperacion);
                });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Exito;
        }



        /// <summary>
        /// Operacion para insertar los detalles del un archivo [TB_SRV_GPH_DETALLE_ARCHIVO]
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="TipoOperacion"></param>
        /// <param name="PmtInfID"></param>
        /// <param name="Estatus"></param>
        /// <param name="ErrorCodes"></param>
        /// <param name="EndToEndId"></param>
        /// <param name="NfArchivoXML"></param>
        /// <returns></returns>
        public async Task<int> InsertaDetalleArchivo(long Folio, string PmtInfID, string Estatus, string ErrorCodes, string EndToEndId
            , string NfArchivoXML)
        {
            SqlParameter outSalida = null;         
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspTombinInsertDetalleArchivo, cmd =>
                {
                    cmd.AddInt64Param("@FOLIO", Folio);
                    cmd.AddStringParam("@TIPO_OPERACION", ConstGenerales.GSTIPO_OPERACION);
                    cmd.AddStringParam("@PMT_INF_ID", PmtInfID);
                    cmd.AddStringParam("@ESTATUS", Estatus);
                    cmd.AddStringParam("@ERROR", ErrorCodes);
                    cmd.AddStringParam("@END_TO_END_ID", EndToEndId);
                    cmd.AddStringParam("@NF_ARCHIVO_XML", NfArchivoXML);
                    outSalida = cmd.AgregarParametroSalida("@Salida", SqlDbType.Int);
                });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outSalida.GetValueOrDefault<int>();            
        }

        /// <summary>
        /// Operacion para marcar registro con error, es decir que no se pudo procesar
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="descripcionError"></param>
        /// <returns></returns>
        public async Task<bool> MarcarRegistroConError(long Folio, string descripcionError)
        {
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspActualizarSrvGphArchivosSerializadosConError, cmd =>
            {
                cmd.AddInt64Param("@Folio", Folio);
                cmd.AddStringParam("@TipoOperacion", ConstGenerales.GSTIPO_OPERACION);
                cmd.AddStringParam("@AliasProceso", ConstGenerales.GSAliasProceso);
                cmd.AddStringParam("@Error", descripcionError);                
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Exito;
        }

        /// <summary>
        /// Operación para marcar un registro como procesado/Finalizado
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="TotalOperaciones"></param>
        /// <param name="headerxml"></param>
        /// <returns></returns>
        public async Task<bool> MarcarRegistrosSerializados(long Folio, int TotalOperaciones, string headerxml)
        {
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspActualizarSrvGphArchivosSerializados, cmd =>
            {
                cmd.AddInt64Param("@Folio", Folio);
                cmd.AddStringParam("@TipoOperacion", ConstGenerales.GSTIPO_OPERACION);
                cmd.AddStringParam("@AliasProceso", ConstGenerales.GSAliasProceso);
                cmd.AddIntParam("@TotalOperaciones", TotalOperaciones);
                cmd.AddStringParam("@HeaderArchivoXML", headerxml);                
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Exito;
        }

        /// <summary>
        /// Operación para validar si una fecha cae en día hábil
        /// </summary>
        /// <param name="FechaEjecucion"></param>
        /// <returns></returns>
        public async Task<bool> EsFechaEjecucionEnDiaHabil(string FechaEjecucion)
        {            
            SqlParameter outEsDiaHabil = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspValidarFechaEjecucionValida, cmd =>
            {
                cmd.AddDateTimeParam("@FechaEjecucion",  Conversion.StringToDate(FechaEjecucion) );                
                outEsDiaHabil = cmd.AddOutPutBool("@EsDiaHabil");                
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outEsDiaHabil.GetValueBool();
        }


        /// <summary>
        /// Operación para validar si un cliente tiene permiso de operación [TB_SRV_GPH_SERVICIOS_CLIENTES]
        /// </summary>
        /// <param name="customer_cdl"></param>
        /// <param name="Servicio"></param>
        /// <returns></returns>
        public async Task<bool> ValidaPermisoEjecucion(string customer_cdl, string Servicio)
        {
            SqlParameter outTienePermiso = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspValidarPermisoCliente, cmd =>
            {
                cmd.AddStringParam("@Customer_cdl", customer_cdl);
                cmd.AddStringParam("@Operacion", Servicio);
                //cmd.Parameters.Add(outTienePermiso);
                outTienePermiso = cmd.AddOutPutBool("@TienePermiso");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outTienePermiso.GetValue<bool>();
        }

        /// <summary>
        /// Operación para consultar fecha
        /// </summary>
        /// <param name="fecha"></param>
        /// <returns></returns>
        public async Task<string> ConsultaFechaCalculada(string fecha)
        {            
            SqlParameter outFechaCalculada = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspConsultaDiaAnteriorHabil, cmd =>
            {
                cmd.AddDateTimeParam("@Fecha", fecha);                
                outFechaCalculada = cmd.AddOutPutString("@FechaCalculada",10);
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outFechaCalculada.GetValueOrDefault<string>();
        }

        /// <summary>
        /// Consulta para validar su una fecha cae en fin de semana
        /// </summary>
        /// <param name="FechaEjecucion"></param>
        /// <returns></returns>
        public async Task<bool> EsFechaEjecucionEnFinSemana(string FechaEjecucion)
        {            
            SqlParameter outEsFinSemana = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspFechaEjecucionEsFinDeSemana, cmd =>
            {
                cmd.AddDateTimeParam("@FechaEjecucion", FechaEjecucion);                
                outEsFinSemana = cmd.AddOutPutBool("@EsFinSemana");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outEsFinSemana.GetValue<bool>();
        }

        /// <summary>
        /// Operación para validar pago nuevo de tipo TEF
        /// </summary>
        /// <param name="fechaSistema"></param>
        /// <param name="PmtInfId"></param>
        /// <param name="nombreaArchivo"></param>
        /// <returns></returns>
        public async Task<bool> ValidarPagoNuevoTEF(DateTime fechaSistema, string PmtInfId, string nombreaArchivo)
        {
            SqlParameter outPagoExiste = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspValidarPagoNuevoTEF, cmd =>
            {
                cmd.AddDateTimeParam("@FechaCaptura", fechaSistema);
                cmd.AddStringParam("@PmtInfId", PmtInfId);
                cmd.AddStringParam("@NombreArchivo", nombreaArchivo);                
                outPagoExiste = cmd.AddOutPutBool("@PagoExiste");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outPagoExiste.GetValue<bool>();
            
        }


        /// <summary>
        /// Metodo que registra archivo de tipo TEF
        /// </summary>
        /// <param name="valoresFormato"></param>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="Estatus"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<long> InsertaFormatoTEF(TB_TOMBIN_NF_TEF_GPH valoresFormato, ArchivosGphDTO Archivo, PaymentInstruction Oper
            , string Estatus, GroupHeader GroupHeaderFile)
        {            
            SqlParameter outNf_Folio = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspInsertaRegistroTEF, cmd =>
            {
                cmd.AddStringParam("@NF_FOLIO_UNICO", "");
                cmd.AddStringParam("@NF_TIPO", valoresFormato.NF_TIPO);
                cmd.AddStringParam("@T24_CUSTOMER_CODE", valoresFormato.T24_CUSTOMER_CODE);
                cmd.AddStringParam("@T24_DEBIT_ACCT_NO", valoresFormato.T24_DEBIT_ACCT_NO);
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddStringParam("@NF_USUARIO_CAPTURA", valoresFormato.NF_USUARIO_CAPTURA);
                cmd.AddStringParam("@NF_USUARIO_AUTORIZA", valoresFormato.NF_USUARIO_AUTORIZA);
                cmd.AddDateTimeParam("@NF_FECHA_CAPTURA", valoresFormato.NF_FECHA_CAPTURA);
                cmd.AddDateTimeParam("@NF_FECHA_AUTORIZA", valoresFormato.NF_FECHA_AUTORIZA);
                cmd.AddDecimalParam("@T24_DEBIT_AMOUNT", valoresFormato.T24_DEBIT_AMOUNT);
                cmd.AddStringParam("@T24_SHORT_NAME", valoresFormato.T24_SHORT_NAME);
                cmd.AddStringParam("@T24_DEBIT_CURRENCY", valoresFormato.T24_DEBIT_CURRENCY);
                cmd.AddStringParam("@NF_BANCO_BENEF", valoresFormato.NF_BANCO_BENEF);
                cmd.AddStringParam("@T24_BEN_ACCT_NO", valoresFormato.T24_BEN_ACCT_NO);
                cmd.AddStringParam("@NF_TIPO_CUENTA", valoresFormato.NF_TIPO_CUENTA);
                cmd.AddStringParam("@T24_BEN_CUST_NAME", valoresFormato.T24_BEN_CUST_NAME);
                cmd.AddStringParam("@NF_RFC_BENEFICIARIO", valoresFormato.NF_RFC_BENEFICIARIO);
                cmd.AddStringParam("@NF_TASA_RETENCION", valoresFormato.NF_TASA_RETENCION);
                cmd.AddIntParam("@NF_FECHA_EJECUCION", valoresFormato.NF_FECHA_EJECUCION);
                cmd.AddStringParam("@T24_DEBIT_THEIR_REF", valoresFormato.T24_DEBIT_THEIR_REF);
                cmd.AddDecimalParam("@NF_MONTO_ENVIAR", valoresFormato.NF_MONTO_ENVIAR);
                cmd.AddStringParam("@NF_CODIGO_OPERACION", valoresFormato.NF_CODIGO_OPERACION);
                cmd.AddStringParam("@NF_TIPO_OPERACION", valoresFormato.NF_TIPO_OPERACION);
                cmd.AddIntParam("@NF_FECHA_APLICACION", valoresFormato.NF_FECHA_APLICACION);
                cmd.AddIntParam("@NF_FECHA_CARGO", valoresFormato.NF_FECHA_CARGO);
                cmd.AddStringParam("@NF_DIRECCION_IP", valoresFormato.NF_DIRECCION_IP);
                cmd.AddStringParam("@NF_NOMBRE_ARCHIVO_GPH", valoresFormato.NF_NOMBRE_ARCHIVO_GPH);
                cmd.AddStringParam("@NF_PMT_INF_ID", valoresFormato.NF_PMT_INF_ID);
                cmd.AddStringParam("@NAME_1", valoresFormato.NAME_1);
                cmd.AddStringParam("@RFC", valoresFormato.RFC);
                cmd.AddStringParam("@CURRENCY", valoresFormato.CURRENCY);
                cmd.AddStringParam("@ACCOUNT_TITLE_1", valoresFormato.ACCOUNT_TITLE_1);
                cmd.AddInt64Param("@NF_ID_ARCHIVO", Archivo.Folio);                                
                cmd.AddStringMaxParam("@NF_ARCHIVO_XML", Conversion.XmlToString<PaymentInstruction>(Oper)); 
                cmd.AddStringMaxParam("@NF_HEADER_ARCHIVO_XML", Conversion.XmlToString<GroupHeader>(GroupHeaderFile));
                outNf_Folio = cmd.AddOutPutInt64("@NoFolio");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new ApplicationException(result.MensajeError);
            }

            return outNf_Folio.GetValue<long>();
        }

        /// <summary>
        /// Operación para asignar folio unico a registro TEF
        /// </summary>
        /// <param name="FolioUnico"></param>
        /// <param name="FolioOperacion"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaFolioTEF(string FolioUnico, long FolioOperacion)
        {
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.upsActualizaFolioUnicoTEFGPH, cmd =>
            {
                cmd.AddStringParam("@NF_FOLIO", FolioOperacion);
                cmd.AddStringParam("@NF_FOLIO_UNICO", FolioUnico);                
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Exito;
        }

        /// <summary>
        /// Operación para consultar registro TEF
        /// </summary>
        /// <param name="NF_FOLIO"></param>
        /// <returns></returns>
        public async Task<TB_TOMBIN_NF_TEF_GPH> ConsultaRegistroTEF(long NF_FOLIO)
        {
            var result = await EjecutarStoredProcedureQuery<TB_TOMBIN_NF_TEF_GPH>(Sp.uspGphEtlObtenerRegistroEntradaSalidaTEF, cmd =>
            {
                cmd.AddInt64Param("@NF_FOLIO", NF_FOLIO);
            });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Operación para actualizar el estatus de un registro TEF
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="Estatus"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaEstatusRegistroTEF(long Folio, string Estatus)
        {            
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspActualizaEstatusRegistroTEF, cmd =>
            {
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddInt64Param("@FolioOperacion", Folio);         
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");                
            }

            return result.Exito;
        }



        /// <summary>
        /// Operación para registrar archivo de entradaSalida [TB_TOMBIN_ENTRADAS_SALIDAS]
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public async Task<bool> InsertaEntradaSalida(TbTombinEntradasSalidasDto item)
        {            
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspInsertaRegistroEntradaSalidas, cmd =>
            {
                cmd.AddStringParam("@Referencia", item.Ent_Referencia);
                cmd.AddDecimalParam("@NumCliente", item.Ent_Cliente);
                cmd.AddDecimalParam("@CDL", item.Ent_CDL); 
                cmd.AddStringParam("@Ordenante", item.Ent_Ordenante);
                cmd.AddStringParam("@Operacion", item.Ent_Operacion);
                cmd.AddStringParam("@OperacionDesc", item.Ent_Operacion_Desc);
                cmd.AddStringParam("@Tipo", item.Ent_Tipo);
                cmd.AddStringParam("@TipoDesc", item.Ent_Tipo_Desc);
                cmd.AddStringParam("@Cuenta", item.Ent_Cuenta);
                cmd.AddStringParam("@Nombre", item.Ent_NombreCuenta); 
                cmd.AddStringParam("@CuentaMoneda", item.Ent_Moneda_Cuenta);
                cmd.AddDecimalParam("@ViaPago", item.Ent_ViaPago);
                cmd.AddStringParam("@ViaPagoDesc", item.Ent_ViaPago_Desc);
                cmd.AddDecimalParam("@Importe", item.Ent_Importe);
                cmd.AddDecimalParam("@Saldo", item.Ent_Saldo);
                cmd.AddStringParam("@Comentarios", item.Ent_Comentarios);
                cmd.AddSmallDateTimeParam("@FechaCaptura", item.Ent_FechaCaptura);
                cmd.AddSmallDateTimeParam("@FechaValor", item.Ent_FechaValor); 
                cmd.AddStringParam("@Hora", item.Ent_Hora);
                cmd.AddStringParam("@Captura", item.Ent_Captura);
                cmd.AddDecimalParam("@Fad", item.Ent_Fad);
                cmd.AddStringParam("@ReferenciaControl", item.Ent_Referencia_Control);
                cmd.AddIntParam("@ReferenciaControlCont", item.Ent_Referencia_Control_Cont);
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new ApplicationException(result.MensajeError);
            }            
            return result.Exito;
        }

        /// <summary>
        /// Operación para finalizar archivo
        /// </summary>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="strError"></param>
        /// <param name="FolioOp"></param>
        /// <param name="Estatus"></param>
        /// <param name="TipoOperacion"></param>
        /// <returns></returns>
        public async Task<bool> FinalizaOperacionArchivo(ArchivosGphDTO Archivo, PaymentInstruction Oper, string strError
            , long FolioOp, string Estatus, string TipoOperacion)
        {
            //var outlpSalida = basedatosConexion.CrearParametroSalida("@FilasAfectadas", DbType.Int32);
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspFinalizaOperacionArchivo, cmd =>
            {
                cmd.AddInt64Param("@Folio", Archivo.Folio);
                cmd.AddStringParam("@TipoOperacion", TipoOperacion);
                cmd.AddStringParam("@PmtInfId",Oper.PmtInfId);
                cmd.AddStringParam("@Estatus", Estatus);
                cmd.AddStringParam("@Error", strError);
                cmd.AddStringParam("@EndToEndId", Oper?.CdtTrfTxInf?.PmtId?.EndToEndId ?? string.Empty);
                cmd.AddInt64Param("@FolioOperacion", FolioOp);                
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");

            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"Mensaje: {result.MensajeError}");
                throw new Exception(result.MensajeError);
            }

            return result.Exito;
        }


        /// <summary>
        /// Operación para obtener los datos de un banco beneficairio
        /// </summary>
        /// <param name="InsClave"></param>
        /// <returns></returns>
        public async Task<BancoBeneficiarioDTO> ObtenerBancoBeneficiarioInstituciones(decimal InsClave)
        {

            var result = await EjecutarStoredProcedureQuery<BancoBeneficiarioDTO>(Sp.uspObtenerBancoBeneficiarioInstituciones, cmd =>
            {
                cmd.AddDecimalParam("@InsClave", InsClave);
            });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new Exception(result.MensajeError);
            }
            return result.Data?.FirstOrDefault();            
        }



        /// <summary>
        /// Operación para validar si un pago es nuevo o existente
        /// </summary>
        /// <param name="fechaSistema"></param>
        /// <param name="PmtInfId"></param>
        /// <param name="nombreaArchivo"></param>
        /// <returns></returns>
        public async Task<bool> ValidarPagoNuevoFXD(DateTime fechaSistema, string PmtInfId, string nombreaArchivo)
        {
            SqlParameter outPagoExiste = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspValidarPagoNuevoFXD, cmd =>
            {
                cmd.AddDateTimeParam("@FechaCaptura", fechaSistema);
                cmd.AddStringParam("@PmtInfId", PmtInfId);
                cmd.AddStringParam("@NombreArchivo", nombreaArchivo);
                //cmd.Parameters.Add(outPagoExiste);
                outPagoExiste = cmd.AddOutPutBool("@PagoExiste");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outPagoExiste.GetValue<bool>();
        }
        
        /// <summary>
        /// Operacion para registrar archivo de tipo FXD
        /// </summary>
        /// <param name="valoresFormato"></param>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="Estatus"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<long> InsertaFormatoFXD(TB_TOMBIN_NF_TEF_GPH valoresFormato, ArchivosGphDTO Archivo, PaymentInstruction Oper
            , string Estatus, GroupHeader GroupHeaderFile)
        {            

            SqlParameter outNf_Folio = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.usplInsertaRegistroFXD, cmd =>
            {
                cmd.AddStringParam("@NF_FOLIO_UNICO", "");
                cmd.AddStringParam("@NF_TIPO", valoresFormato.NF_TIPO);
                cmd.AddStringParam("@T24_CUSTOMER_CODE", valoresFormato.T24_CUSTOMER_CODE);
                cmd.AddStringParam("@T24_DEBIT_ACCT_NO", valoresFormato.T24_DEBIT_ACCT_NO);
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddStringParam("@NF_USUARIO_CAPTURA", valoresFormato.NF_USUARIO_CAPTURA);
                cmd.AddStringParam("@NF_USUARIO_AUTORIZA", valoresFormato.NF_USUARIO_AUTORIZA);
                cmd.AddDateTimeParam("@NF_FECHA_CAPTURA", valoresFormato.NF_FECHA_CAPTURA);
                cmd.AddDateTimeParam("@NF_FECHA_AUTORIZA", valoresFormato.NF_FECHA_AUTORIZA);
                cmd.AddDecimalParam("@T24_DEBIT_AMOUNT", valoresFormato.T24_DEBIT_AMOUNT);
                cmd.AddStringParam("@T24_SHORT_NAME", valoresFormato.T24_SHORT_NAME);
                cmd.AddStringParam("@T24_DEBIT_CURRENCY", valoresFormato.T24_DEBIT_CURRENCY);
                cmd.AddStringParam("@NF_BANCO_BENEF", valoresFormato.NF_BANCO_BENEF);
                cmd.AddStringParam("@T24_BEN_ACCT_NO", valoresFormato.T24_BEN_ACCT_NO);
                cmd.AddStringParam("@NF_TIPO_CUENTA", valoresFormato.NF_TIPO_CUENTA);
                cmd.AddStringParam("@T24_BEN_CUST_NAME", valoresFormato.T24_BEN_CUST_NAME);
                cmd.AddStringParam("@NF_RFC_BENEFICIARIO", valoresFormato.NF_RFC_BENEFICIARIO);
                cmd.AddStringParam("@NF_TASA_RETENCION", valoresFormato.NF_TASA_RETENCION);
                cmd.AddIntParam("@NF_FECHA_EJECUCION", valoresFormato.NF_FECHA_EJECUCION);
                cmd.AddStringParam("@T24_DEBIT_THEIR_REF", valoresFormato.T24_DEBIT_THEIR_REF);
                cmd.AddDecimalParam("@NF_MONTO_ENVIAR", valoresFormato.NF_MONTO_ENVIAR);
                cmd.AddStringParam("@NF_DIRECCION_IP", valoresFormato.NF_DIRECCION_IP);
                cmd.AddStringParam("@NF_NOMBRE_ARCHIVO_GPH", valoresFormato.NF_NOMBRE_ARCHIVO_GPH);
                cmd.AddStringParam("@NF_PMT_INF_ID", valoresFormato.NF_PMT_INF_ID);
                cmd.AddStringParam("@NAME_1", valoresFormato.NAME_1);
                cmd.AddStringParam("@RFC", valoresFormato.RFC);
                cmd.AddStringParam("@CURRENCY", valoresFormato.CURRENCY);
                cmd.AddStringParam("@ACCOUNT_TITLE_1", valoresFormato.ACCOUNT_TITLE_1);
                cmd.AddInt64Param("@NF_ID_ARCHIVO", Archivo.Folio);                                
                cmd.AddStringMaxParam("@NF_ARCHIVO_XML", Conversion.XmlToString<PaymentInstruction>(Oper));
                cmd.AddStringMaxParam("@NF_HEADER_ARCHIVO_XML", Conversion.XmlToString<GroupHeader>(GroupHeaderFile));
                cmd.AddStringParam("@OP_REF_NUMERICA", valoresFormato.OP_REF_NUMERICA);
                outNf_Folio = cmd.AddOutPutInt64("@NoFolio");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new ApplicationException(result.MensajeError);
            }

            return outNf_Folio.GetValue<long>(); 
        }
        
        /// <summary>
        /// Operación para actualizar el folio de un refistro de tipo FXD
        /// </summary>
        /// <param name="FolioUnico"></param>
        /// <param name="FolioOperacion"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaFolioFXD(string FolioUnico, long FolioOperacion)
        {
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.upsActualizaFolioUnicoFXDGPH, cmd =>
            {
                cmd.AddInt64Param("@NF_FOLIO", FolioOperacion);
                cmd.AddStringParam("@NF_FOLIO_UNICO", FolioUnico);                
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Exito;
        }

        /// <summary>
        /// Operación para consultar los datos de un registro de tipo FXD
        /// </summary>
        /// <param name="NF_FOLIO"></param>
        /// <returns></returns>
        public async Task<TB_TOMBIN_NF_TEF_GPH> ConsultaRegistroFXD(long NF_FOLIO)
        {
            var result = await EjecutarStoredProcedureQuery<TB_TOMBIN_NF_TEF_GPH>(Sp.uspGphEtlObtenerRegistroEntradaSalidaFXD, cmd =>
            {
                cmd.AddInt64Param("@NF_FOLIO", NF_FOLIO);
            });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Data?.FirstOrDefault();
        }

        /// <summary>
        /// operación para actualizar estatus de un registro de tipo FXD
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="Estatus"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaEstatusRegistroFXD(long Folio, string Estatus)
        {            
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspActualizaEstatusRegistroFXD, cmd =>
            {
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddInt64Param("@FolioOperacion", Folio);         
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");                
            }

            return result.Exito;
        }

        


        /// <summary>
        /// operación para validar existencia de pago  de tipo SPID
        /// </summary>
        /// <param name="fechaSistema"></param>
        /// <param name="PmtInfId"></param>
        /// <param name="nombreaArchivo"></param>
        /// <returns></returns>
        public async Task<bool> ValidarPagoNuevoSPID(DateTime fechaSistema, string PmtInfId, string nombreaArchivo)
        {
            SqlParameter outPagoExiste = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspValidarPagoNuevoSPID, cmd =>
            {
                cmd.AddDateTimeParam("@FechaCaptura", fechaSistema);
                cmd.AddStringParam("@PmtInfId", PmtInfId);
                cmd.AddStringParam("@NombreArchivo", nombreaArchivo);
                //cmd.Parameters.Add(outPagoExiste);
                outPagoExiste = cmd.AddOutPutBool("@PagoExiste");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outPagoExiste.GetValue<bool>();
        }

        /// <summary>
        /// Opreación para obtener los datos de los bancos de tipo SPID
        /// </summary>
        /// <param name="InsClave"></param>
        /// <returns></returns>
        public async Task<BancoBeneficiarioDTO> ObtenerBancoBeneficiarioInstitucionesSPID(decimal InsClave)
        {
            var result = await EjecutarStoredProcedureQuery<BancoBeneficiarioDTO>(Sp.uspObtenerBancoBeneficiarioInstitucionesSPID, cmd =>
            {
                cmd.AddDecimalParam("@InsClave", InsClave);
            });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new Exception(result.MensajeError);
            }
            return result.Data?.FirstOrDefault();
        }


        /// <summary>
        /// Opreación para el registro de archivos de tipo SPID tabla [TB_TOMBIN_NF_SPID_GPH]
        /// </summary>
        /// <param name="valoresFormato"></param>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="Estatus"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<long> InsertaFormatoSPID(TB_TOMBIN_NF_TEF_GPH valoresFormato, ArchivosGphDTO Archivo
            , PaymentInstruction Oper, string Estatus, GroupHeader GroupHeaderFile            
            )
        {

            SqlParameter outNf_Folio = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.usplInsertaRegistroSPID, cmd =>
            {
                cmd.AddStringParam("@NF_FOLIO_UNICO", valoresFormato.NF_FOLIO_UNICO);
                cmd.AddStringParam("@NF_TIPO", valoresFormato.NF_TIPO);
                cmd.AddStringParam("@T24_CUSTOMER_CODE", valoresFormato.T24_CUSTOMER_CODE);
                cmd.AddStringParam("@T24_DEBIT_ACCT_NO", valoresFormato.T24_DEBIT_ACCT_NO);
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddStringParam("@NF_USUARIO_CAPTURA", valoresFormato.NF_USUARIO_CAPTURA);
                cmd.AddStringParam("@NF_USUARIO_AUTORIZA", valoresFormato.NF_USUARIO_AUTORIZA);
                cmd.AddDateTimeParam("@NF_FECHA_CAPTURA", valoresFormato.NF_FECHA_CAPTURA);
                cmd.AddDateTimeParam("@NF_FECHA_AUTORIZA", valoresFormato.NF_FECHA_AUTORIZA);
                cmd.AddDecimalParam("@T24_DEBIT_AMOUNT", valoresFormato.T24_DEBIT_AMOUNT);
                cmd.AddStringParam("@T24_SHORT_NAME", valoresFormato.T24_SHORT_NAME);
                cmd.AddStringParam("@T24_DEBIT_CURRENCY", valoresFormato.T24_DEBIT_CURRENCY);
                cmd.AddStringParam("@NF_BANCO_BENEF", valoresFormato.NF_BANCO_BENEF);
                cmd.AddStringParam("@T24_BEN_ACCT_NO", valoresFormato.T24_BEN_ACCT_NO);
                cmd.AddStringParam("@NF_TIPO_CUENTA", valoresFormato.NF_TIPO_CUENTA);
                cmd.AddStringParam("@T24_BEN_CUST_NAME", valoresFormato.T24_BEN_CUST_NAME);
                cmd.AddStringParam("@NF_RFC_BENEFICIARIO", valoresFormato.NF_RFC_BENEFICIARIO);
                cmd.AddStringParam("@NF_TASA_RETENCION", valoresFormato.NF_TASA_RETENCION);
                cmd.AddIntParam("@NF_FECHA_EJECUCION", valoresFormato.NF_FECHA_EJECUCION);
                cmd.AddStringParam("@T24_DEBIT_THEIR_REF", valoresFormato.T24_DEBIT_THEIR_REF);
                cmd.AddDecimalParam("@NF_MONTO_ENVIAR", valoresFormato.NF_MONTO_ENVIAR);
                cmd.AddStringParam("@NF_ADDRESS", valoresFormato.P1_CALLE);
                cmd.AddStringParam("@NF_ZIP_CODE", valoresFormato.P1_CODIGO_POSTAL);
                cmd.AddStringParam("@NF_CLASIFICACION_OPERACION", valoresFormato.NF_CLASIFICACION_OPERACION);
                cmd.AddStringParam("@NF_RELACION_COMERCIAL", valoresFormato.NF_RELACION_COMERCIAL);
                cmd.AddStringParam("@NF_NUMERIC_REFERENCE", valoresFormato.OP_REF_NUMERICA);
                cmd.AddStringParam("@NF_DIRECCION_IP", valoresFormato.NF_DIRECCION_IP);
                cmd.AddStringParam("@NF_NOMBRE_ARCHIVO_GPH", valoresFormato.NF_NOMBRE_ARCHIVO_GPH);
                cmd.AddStringParam("@NF_PMT_INF_ID", valoresFormato.NF_PMT_INF_ID);
                cmd.AddStringParam("@NAME_1", valoresFormato.NAME_1);
                cmd.AddStringParam("@RFC", valoresFormato.RFC);
                cmd.AddStringParam("@CURRENCY", valoresFormato.CURRENCY);
                cmd.AddStringParam("@ACCOUNT_TITLE_1", valoresFormato.ACCOUNT_TITLE_1);
                cmd.AddInt64Param("@NF_ID_ARCHIVO", Archivo.Folio);
                cmd.AddStringMaxParam("@NF_ARCHIVO_XML", Conversion.XmlToString<PaymentInstruction>(Oper));
                cmd.AddStringMaxParam("@NF_HEADER_ARCHIVO_XML", Conversion.XmlToString<GroupHeader>(GroupHeaderFile));
                outNf_Folio = cmd.AddOutPutInt64("@NoFolio");

            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new ApplicationException(result.MensajeError);
            }

            return outNf_Folio.GetValue<long>(); 
        }

        /// <summary>
        /// Operación para la actualización de folio en un registro de tipo SPID
        /// </summary>
        /// <param name="FolioUnico"></param>
        /// <param name="FolioOperacion"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaFolioSPID(string FolioUnico, long FolioOperacion)
        {
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.upsActualizaFolioUnicoSPID, cmd =>
            {
                cmd.AddStringParam("@NF_FOLIO", FolioOperacion);
                cmd.AddStringParam("@NF_FOLIO_UNICO", FolioUnico);
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Exito;
        }

        /// <summary>
        /// Operación para la consulta de datos de un registro de tipo SPID [TB_TOMBIN_NF_SPID_GPH]
        /// </summary>
        /// <param name="NF_FOLIO"></param>
        /// <returns></returns>
        public async Task<TB_TOMBIN_NF_TEF_GPH> ConsultaRegistroSPID(long NF_FOLIO)
        {
            var result = await EjecutarStoredProcedureQuery<TB_TOMBIN_NF_TEF_GPH>(Sp.uspObtenerRegistroEntradaSalidaSPID, cmd =>
            {
                cmd.AddInt64Param("@NF_FOLIO", NF_FOLIO);
            });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Operación para validar cliente de tipo SPID
        /// </summary>
        /// <param name="cdl"></param>
        /// <returns></returns>
        public async Task<bool> ValidarClienteSPID(string cdl)
        {
            SqlParameter outExiste = null;

            var result = await EjecutarStoredProceduredNonQuery(Sp.usp_ValidarExistenciaClienteSPID, cmd =>
            {
                cmd.AddStringParam("@Cdl", cdl);                
                outExiste = cmd.AddOutPutBool("@PagoExiste");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outExiste.GetValue<bool>();
        }

        /// <summary>
        /// Operación para actualizar estatus de un registro de tipo SPID
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="Estatus"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaEstatusRegistroSPID(long Folio, string Estatus)
        {            
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspActualizaEstatusRegistroSPID, cmd =>
            {
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddInt64Param("@FolioOperacion", Folio);                
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");                
            }

            return result.Exito;
        }



        /// <summary>
        /// Operación para validar pago nuevo de tipo FXI
        /// </summary>
        /// <param name="fechaSistema"></param>
        /// <param name="PmtInfId"></param>
        /// <param name="nombreaArchivo"></param>
        /// <returns></returns>
        public async Task<bool> ValidarPagoNuevoFXI(DateTime fechaSistema, string PmtInfId, string nombreaArchivo)
        {
            SqlParameter outPagoExiste = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspValidarPagoNuevoFXI, cmd =>
            {
                cmd.AddDateTimeParam("@FechaCaptura", fechaSistema);
                cmd.AddStringParam("@PmtInfId", PmtInfId);
                cmd.AddStringParam("@NombreArchivo", nombreaArchivo);         
                outPagoExiste = cmd.AddOutPutBool("@PagoExiste");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return outPagoExiste.GetValue<bool>();
        }

        /// <summary>
        /// Operación para obtener el nombre de un Pais
        /// </summary>
        /// <param name="cve_country"></param>
        /// <returns></returns>
        public async Task<string> ObtenerNombrePais(string cve_country)
        {                        
            SqlParameter outNombrePais = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspObtenerNombrePais, cmd =>
            {
                cmd.AddStringParam("@cve_country2", cve_country);                
                outNombrePais = cmd.AddOutPutString("@NombrePais", 35);
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new Exception(result.MensajeError);
            }
            return outNombrePais.GetValueOrDefault<string>();
        }

        /// <summary>
        /// Operación para validar/consultar la dirección 
        /// </summary>
        /// <param name="cve_country"></param>
        /// <returns></returns>
        public async Task<(bool cityRequired, bool addressRequired)> ValidarCityAddressLineRequired(string cve_country)
        {
            SqlParameter outCityRequired = null;
            SqlParameter outAddressRequired = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspValidarCityAddressRequired, cmd =>
            {
                cmd.AddStringParam("@cve_country2", cve_country);
                outCityRequired = cmd.AddOutPutInt("@CityRequired");
                outAddressRequired = cmd.AddOutPutInt("@AddressLineRequired");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            bool cityRequired = outCityRequired.GetValueBool();
            bool AddressRequired= outAddressRequired.GetValueBool();
            return (cityRequired, AddressRequired);

        }


        /// <summary>
        /// Operación para el registro de un archivo de tipo FXI
        /// </summary>
        /// <param name="valoresFormato"></param>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="Estatus"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<long> InsertaFormatoFXI(TB_TOMBIN_NF_FXI_GPH valoresFormato, ArchivosGphDTO Archivo, PaymentInstruction Oper
            , string Estatus, GroupHeader GroupHeaderFile)
        {            
            SqlParameter outNf_Folio = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.usplInsertaRegistroFXI, cmd =>
            {
                cmd.AddStringParam("@NF_FOLIO_UNICO", valoresFormato.NF_FOLIO_UNICO??"");
                cmd.AddStringParam("@NF_TIPO", valoresFormato.NF_TIPO);
                cmd.AddStringParam("@T24_CUSTOMER_CODE", valoresFormato.T24_CUSTOMER_CODE);
                cmd.AddStringParam("@T24_DEBIT_ACCT_NO", valoresFormato.T24_DEBIT_ACCT_NO);
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddStringParam("@NF_USUARIO_CAPTURA", valoresFormato.NF_USUARIO_CAPTURA);
                cmd.AddStringParam("@NF_USUARIO_AUTORIZA", valoresFormato.NF_USUARIO_AUTORIZA);
                cmd.AddDateTimeParam("@NF_FECHA_CAPTURA", valoresFormato.NF_FECHA_CAPTURA);
                cmd.AddDateTimeParam("@NF_FECHA_AUTORIZA", valoresFormato.NF_FECHA_AUTORIZA);
                cmd.AddDecimalParam("@T24_DEBIT_AMOUNT", valoresFormato.T24_DEBIT_AMOUNT);
                cmd.AddStringParam("@T24_SHORT_NAME", valoresFormato.T24_SHORT_NAME);
                cmd.AddStringParam("@T24_DEBIT_CURRENCY", valoresFormato.T24_DEBIT_CURRENCY);
                cmd.AddDecimalParam("@NF_MONTO_ENVIAR", valoresFormato.NF_MONTO_ENVIAR);
                cmd.AddStringParam("@T24_DEBIT_THEIR_REF", valoresFormato.T24_DEBIT_THEIR_REF);
                cmd.AddStringParam("@NF_TIPO_CUENTA", valoresFormato.NF_TIPO_CUENTA);
                cmd.AddStringParam("@NF_PAYMENT", valoresFormato.NF_PAYMENT);
                cmd.AddStringParam("@NF_CHARGE", valoresFormato.NF_CHARGE);
                cmd.AddInt64Param("@NF_FECHA_EJECUCION", valoresFormato.NF_FECHA_EJECUCION);
                cmd.AddStringParam("@T24_BEN_CUST_NAME", valoresFormato.T24_BEN_CUST_NAME);
                cmd.AddStringParam("@NF_FXD_COMMENT", valoresFormato.NF_FXD_COMMENT);
                cmd.AddStringParam("@NF_RFC_BENEFICIARIO", valoresFormato.NF_RFC_BENEFICIARIO);
                cmd.AddStringParam("@NF_TASA_RETENCION", valoresFormato.NF_TASA_RETENCION);
                cmd.AddStringParam("@NF_BANCO_BENEF", valoresFormato.NF_BANCO_BENEF);
                cmd.AddStringParam("@NF_SWIFT_BENEFICIARIO", valoresFormato.NF_SWIFT_BENEFICIARIO);
                cmd.AddStringParam("@T24_BEN_ACCT_NO", valoresFormato.T24_BEN_ACCT_NO);
                cmd.AddStringParam("@NF_BANCO_CORRESPONSAL", valoresFormato.NF_BANCO_CORRESPONSAL);
                cmd.AddStringParam("@NF_SWIFT_CORRESPONSAL", valoresFormato.NF_SWIFT_CORRESPONSAL);
                cmd.AddStringParam("@NF_CUENTA_CORRESPONSAL", valoresFormato.NF_CUENTA_CORRESPONSAL);
                cmd.AddStringParam("@NF_MENSAJE", valoresFormato.NF_MENSAJE);
                cmd.AddStringParam("@NF_COUNTRY", valoresFormato.NF_COUNTRY);
                cmd.AddStringParam("@COUNTRY_BEN", valoresFormato.COUNTRY_BEN);
                cmd.AddStringParam("@NF_DIRECCION_IP", valoresFormato.NF_DIRECCION_IP);
                cmd.AddStringParam("@NF_NOMBRE_ARCHIVO_GPH", valoresFormato.NF_NOMBRE_ARCHIVO_GPH);
                cmd.AddStringParam("@NF_PMT_INF_ID", valoresFormato.NF_PMT_INF_ID);
                cmd.AddStringParam("@NAME_1", valoresFormato.NAME_1);
                cmd.AddStringParam("@RFC", valoresFormato.RFC);
                cmd.AddStringParam("@CURRENCY", valoresFormato.CURRENCY);
                cmd.AddStringParam("@ACCOUNT_TITLE_1", valoresFormato.ACCOUNT_TITLE_1);
                cmd.AddInt64Param("@NF_ID_ARCHIVO", Archivo.Folio);                                
                cmd.AddStringMaxParam("@NF_ARCHIVO_XML", Conversion.XmlToString<PaymentInstruction>(Oper));
                cmd.AddStringMaxParam("@NF_HEADER_ARCHIVO_XML",   Conversion.XmlToString<GroupHeader>(GroupHeaderFile));
                cmd.AddStringParam("@CITY_BEN", valoresFormato.CITY_BEN);
                outNf_Folio = cmd.AddOutPutInt64("@NoFolio");
            });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new ApplicationException(result.MensajeError);
            }

            return outNf_Folio.GetValue<long>();
        }

        /// <summary>
        /// Operación para actualizar folio a un registro de tipo FXI
        /// </summary>
        /// <param name="FolioUnico"></param>
        /// <param name="FolioOperacion"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaFolioFXI(string FolioUnico, long FolioOperacion)
        {
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.upsActualizaFolioUnicoFXI, cmd =>
            {
                cmd.AddInt64Param("@NF_FOLIO", FolioOperacion);
                cmd.AddStringParam("@NF_FOLIO_UNICO", FolioUnico);
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Exito;
        }

        /// <summary>
        /// Operación para consultar los datos de un archivo de tipo FXI
        /// </summary>
        /// <param name="NF_FOLIO"></param>
        /// <returns></returns>
        public async Task<TB_TOMBIN_NF_FXI_GPH> ConsultaRegistroFXI(long NF_FOLIO)
        {
            var result = await EjecutarStoredProcedureQuery<TB_TOMBIN_NF_FXI_GPH>(Sp.uspObtenerRegistroEntradaSalidaFXI, cmd =>
            {
                cmd.AddInt64Param("@NF_FOLIO", NF_FOLIO);
            });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Data?.FirstOrDefault();
            
        }

        /// <summary>
        /// Operación para actualizar el Estatus de un archivo de tipo FXI
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="Estatus"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaEstatusRegistroFXI(long Folio, string Estatus)
        {            
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspActualizaEstatusRegistroFXI, cmd =>
            {
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddInt64Param("@FolioOperacion", Folio);                
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");                
            }
            return result.Exito;            
        }



        /// <summary>
        /// Operación para la validación de existencia de Pago nuevo de tipo FXT
        /// </summary>
        /// <param name="fechaSistema"></param>
        /// <param name="PmtInfId"></param>
        /// <param name="nombreaArchivo"></param>
        /// <returns></returns>
        public async Task<bool> ValidarPagoNuevoFXT(DateTime fechaSistema, string PmtInfId, string nombreaArchivo)
        {
            SqlParameter outPagoExiste = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspValidarPagoNuevoFXT, cmd =>
            {
                cmd.AddDateTimeParam("@FechaCaptura", fechaSistema);
                cmd.AddStringParam("@PmtInfId", PmtInfId);
                cmd.AddStringParam("@NombreArchivo", nombreaArchivo);                
                outPagoExiste = cmd.AddOutPutBool("@PagoExiste");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }
            return outPagoExiste.GetValueBool();            
        }

        /// <summary>
        /// Opreación para el registro de un archivo de tipo FXT
        /// </summary>
        /// <param name="valoresFormato"></param>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="Estatus"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        public async Task<long> InsertaFormatoFXT(TB_TOMBIN_NF_FXI_GPH valoresFormato, ArchivosGphDTO Archivo
            , PaymentInstruction Oper, string Estatus, GroupHeader GroupHeaderFile)
        {            
            SqlParameter outNf_Folio = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.usplInsertaRegistroFXT, cmd =>
            {
                cmd.AddStringParam("@NF_FOLIO_UNICO", valoresFormato.NF_FOLIO_UNICO?? "");
                cmd.AddStringParam("@NF_TIPO", valoresFormato.NF_TIPO);
                cmd.AddStringParam("@T24_CUSTOMER_CODE", valoresFormato.T24_CUSTOMER_CODE);
                cmd.AddStringParam("@T24_DEBIT_ACCT_NO", valoresFormato.T24_DEBIT_ACCT_NO);
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddStringParam("@NF_USUARIO_CAPTURA", valoresFormato.NF_USUARIO_CAPTURA);
                cmd.AddStringParam("@NF_USUARIO_AUTORIZA", valoresFormato.NF_USUARIO_AUTORIZA);
                cmd.AddDateTimeParam("@NF_FECHA_CAPTURA", valoresFormato.NF_FECHA_CAPTURA);
                cmd.AddDateTimeParam("@NF_FECHA_AUTORIZA", valoresFormato.NF_FECHA_AUTORIZA);
                cmd.AddDecimalParam("@T24_DEBIT_AMOUNT", valoresFormato.T24_DEBIT_AMOUNT);
                cmd.AddStringParam("@T24_SHORT_NAME", valoresFormato.T24_SHORT_NAME);
                cmd.AddStringParam("@T24_DEBIT_CURRENCY", valoresFormato.T24_DEBIT_CURRENCY);
                cmd.AddStringParam("@NF_BANCO_BENEF", valoresFormato.NF_BANCO_BENEF);
                cmd.AddStringParam("@T24_BEN_ACCT_NO", valoresFormato.T24_BEN_ACCT_NO);
                cmd.AddStringParam("@NF_TIPO_CUENTA", valoresFormato.NF_TIPO_CUENTA);
                cmd.AddStringParam("@T24_BEN_CUST_NAME", valoresFormato.T24_BEN_CUST_NAME);
                cmd.AddStringParam("@NF_RFC_BENEFICIARIO", valoresFormato.NF_RFC_BENEFICIARIO);
                cmd.AddStringParam("@NF_TASA_RETENCION", valoresFormato.NF_TASA_RETENCION);
                cmd.AddStringParam("@NF_FECHA_EJECUCION", valoresFormato.NF_FECHA_EJECUCION);
                cmd.AddStringParam("@T24_DEBIT_THEIR_REF", valoresFormato.T24_DEBIT_THEIR_REF);
                cmd.AddDecimalParam("@NF_MONTO_ENVIAR", valoresFormato.NF_MONTO_ENVIAR);
                cmd.AddStringParam("@NF_DIRECCION_IP", valoresFormato.NF_DIRECCION_IP);
                cmd.AddStringParam("@NF_NOMBRE_ARCHIVO_GPH", valoresFormato.NF_NOMBRE_ARCHIVO_GPH);
                cmd.AddStringParam("@NF_PMT_INF_ID", valoresFormato.NF_PMT_INF_ID);
                cmd.AddStringParam("@NAME_1", valoresFormato.NAME_1);
                cmd.AddStringParam("@RFC", valoresFormato.RFC);
                cmd.AddStringParam("@CURRENCY", valoresFormato.CURRENCY);
                cmd.AddStringParam("@ACCOUNT_TITLE_1", valoresFormato.ACCOUNT_TITLE_1);
                cmd.AddInt64Param("@NF_ID_ARCHIVO", Archivo.Folio);                                
                cmd.AddStringMaxParam("@NF_ARCHIVO_XML", Conversion.XmlToString<PaymentInstruction>(Oper));
                cmd.AddStringMaxParam("@NF_HEADER_ARCHIVO_XML", Conversion.XmlToString<GroupHeader>(GroupHeaderFile));
                outNf_Folio = cmd.AddOutPutInt64("@NoFolio");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
                throw new ApplicationException(result.MensajeError);
            }

            return outNf_Folio.GetValue<long>();
        }

        /// <summary>
        /// Operacion para la asignación/actualzacion de folio a un registro de tipo FXT
        /// </summary>
        /// <param name="FolioUnico"></param>
        /// <param name="FolioOperacion"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaFolioFXT(string FolioUnico, long FolioOperacion)
        {
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.upsActualizaFolioUnicoFXT, cmd =>
            {
                cmd.AddStringParam("@NF_FOLIO", FolioOperacion);
                cmd.AddStringParam("@NF_FOLIO_UNICO", FolioUnico);
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");
            }

            return result.Exito;
        }

        /// <summary>
        /// Operación para la actualización de un registro de tipo FXT
        /// </summary>
        /// <param name="Folio"></param>
        /// <param name="Estatus"></param>
        /// <returns></returns>
        public async Task<bool> ActualizaEstatusRegistroFXT(long Folio, string Estatus)
        {            
            SqlParameter outSalida = null;
            var result = await EjecutarStoredProceduredNonQuery(Sp.uspActualizaEstatusRegistroFXT, cmd =>
            {
                cmd.AddStringParam("@NF_ESTATUS", Estatus);
                cmd.AddInt64Param("@FolioOperacion", Folio);
                outSalida = cmd.AddOutPutInt("@FilasAfectadas");
            });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"{MsgLog.Bd.Tomi.ErrorEjecucion}:|{result.MensajeError}");                
            }

            return result.Exito;            
        }
    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Enums;
using AplicacionGPH_ETL.Interfaces.Alertas;
using AplicacionGPH_ETL.Interfaces.ControlError;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using MUFG_T24.Comunes;
using MUFG_T24.Conexiones;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;

namespace Infraestructura.GPH_ETL.Repositorios.TEF
{
    /// <summary>
    /// Metodos de enlace a la base de datos TEF
    /// </summary>
    public class RepositorioTEF : RepositorioMaestro, IRepositorioTEF
    {
        private readonly ILoggerService _log;
        private readonly IControlErrorService _controlErrorService;
        private readonly IExtensionAlerta _extAlerta;
        public RepositorioTEF(
            ILoggerService log
            , IControlErrorService controlErrorService
            , IExtensionAlerta extAlerta
         ) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TEF, null, null)
        {
            _log = log;
            _controlErrorService = controlErrorService;
            _extAlerta = extAlerta;
        }


        /// <summary>
        /// Metodo para validar conexión con base de datos
        /// </summary>
        /// <returns></returns>
        public async Task<bool> ValidaAccesoBdTEFAsync()
        {
            try
            {
                _log.Info(MsgLog.Bd.Tef.ValidaAccesoInicio);

                var cnn = ConexionBasedatosFactory.CrearConexion(BasedatosProveedor.SqlServer, NombreBasedatos.TEF);

                using (var cn = cnn.CrearConexion())
                {
                    await cn.OpenAsync();
                    _log.Info(MsgLog.Bd.Tef.ConexionOk);
                    if (cn.State == ConnectionState.Open)
                    {
                        // Base ok
                        _controlErrorService.LimpiaControlError(1);
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error($"{MsgLog.Bd.Tef.Exception} : {ex.Message}");
                await _controlErrorService.ControlErrorAsync(1, ex);
                //Registra alerta                
                await _extAlerta.RegistraCriticoAsync(MsgLog.Bd.Tef.msgAcceso, ex);
            }
            finally
            {
                _log.Info(MsgLog.Bd.Tef.ValidaAccesoFin);
            }
            return false;
        }



        /// <summary>
        /// Metodo para consultar y almacenar datos del Bancobeneficiario
        /// </summary>
        /// <param name="InsClave"></param>
        /// <returns></returns>
        public async Task<BancoBeneficiarioDTO> ObtenerBancoBeneficiario(decimal InsClave)
        {
            var result =await EjecutarStoredProcedureQuery<BancoBeneficiarioDTO>(ConstantesServicio.Sp.TEF.uspObtenerBancoBeneficiarioTEF, cmd =>
                    {
                        cmd.AddStringParam("@InsClave", InsClave);
                    });

            if (!string.IsNullOrEmpty(result.MensajeError))
            {
                _log.Error($"Mensaje: {result.MensajeError}");
                throw new Exception(result.MensajeError);
            }
            return result.Data?.FirstOrDefault();
        }
    }
}
using AplicacionGPH_ETL.Comunes;
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Interfaces.Alertas;
using AplicacionGPH_ETL.Interfaces.ControlError;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using MUFG_T24.Comunes;
using MUFG_T24.Conexiones;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using Sp = AplicacionGPH_ETL.Enums.ConstantesServicio.Sp.VT24;


namespace Infraestructura.GPH_ETL.Repositorios.VT24
{
    /// <summary>
    /// Metodos de enlace a la base de datos vistas T24
    /// </summary>
    public class RepositorioVT24 : RepositorioMaestro, IRepositorioVT24
    {
        private readonly ILoggerService _log;
        private readonly IControlErrorService _controlErrorService;
        private readonly IExtensionAlerta _extAlerta;

        public RepositorioVT24(
            ILoggerService log
            , IControlErrorService controlErrorService
            , IExtensionAlerta extAlerta

        ) : base(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW, null, null) 
        {
            _log = log;
            _controlErrorService = controlErrorService;
            _extAlerta = extAlerta;
        }


        /// <summary>
        /// Metodo para validar conexión con base de datos
        /// </summary>
        /// <returns></returns>
        public async Task<bool> ValidaAccesoBdvt24Async()
        {
            try
            {
                _log.Info(MsgLog.Bd.VT24.ValidaAccesoInicio);

                var cnn = ConexionBasedatosFactory.CrearConexion(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW);

                using (var cn = cnn.CrearConexion())
                {
                    await cn.OpenAsync();
                    _log.Info(MsgLog.Bd.VT24.ConexionOk);
                    if (cn.State == ConnectionState.Open)
                    {
                        // Base ok
                        _controlErrorService.LimpiaControlError(1);
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error($"{MsgLog.Bd.VT24.Exception} : {ex.Message}");
                await _controlErrorService.ControlErrorAsync(1, ex);
                //Registra alerta                
                await _extAlerta.RegistraCriticoAsync(MsgLog.Bd.VT24.msgAcceso, ex);
            }
            finally
            {
                _log.Info(MsgLog.Bd.VT24.ValidaAccesoFin);
            }
            return false;
        }


        /// <summary>
        /// Operación para la consulta de estado de cuenta de un cliente
        /// </summary>
        /// <param name="customerCode"></param>
        /// <returns></returns>
        public async Task<PaPSpidInsightCustomerEdoCtaDto> ObtenerCustomerEdoCta(string customerCode)
        {
            var r = await EjecutarStoredProcedureQuery<PaPSpidInsightCustomerEdoCtaDto>(
                Sp.uspListCustomerEdoCtaT24, cmd =>
                {
                    cmd.AddStringParam("@CustomerCode", customerCode);
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Operación para obtener los datos de una cuenta
        /// </summary>
        /// <param name="Clabe"></param>
        /// <returns></returns>
        public async Task<CuentaBtnumDto> ObtenerDatosCuenta(string Clabe)
        {
            var result = await EjecutarStoredProcedureQuery<CuentaBtnumDto>( Sp.uspObtenerCuentaT24, cmd =>
                {
                    cmd.AddStringParam("@Clabe", Clabe);
                });
            if (!string.IsNullOrEmpty(result.MensajeError))
            {                
                _log.Error($"{MsgLog.Bd.VT24.ErrorEjecucion}:|{result.MensajeError}");
            }
            return result.Data?.FirstOrDefault();
        }



    }
}
using AplicacionGPH_ETL.Interfaces.Alertas;
using AplicacionGPH_ETL.Interfaces.ControlError;
using AplicacionGPH_ETL.Interfaces.Email;
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Proceso;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Interfaces.Servicios;
using AplicacionGPH_ETL.Modelos;
using AplicacionGPH_ETL.Servicios;
using AplicacionGPH_ETL.Servicios.Alertas;
using AplicacionGPH_ETL.Servicios.ControlError;
using AplicacionGPH_ETL.Servicios.Email;
using AplicacionGPH_ETL.Servicios.ProcesosOperacion;
using Infraestructura.GPH_ETL.Repositorios.TEF;
using Infraestructura.GPH_ETL.Repositorios.Tomi;
using Infraestructura.GPH_ETL.Repositorios.VT24;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Core.Configuracion;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;
using static AplicacionGPH_ETL.Enums.ConstantesServicio.MsgLog;
using _log = MUFG_T24.Infraestructura.Logging.Logging;

namespace GPH_ETL
{
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private Configuracion _configuracionBase;
        private ILoggerService _logger;

        protected override string NombreServicioLog => NombreServicio.GPH_ETL.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 300000;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServiceHost() {
            this.ServiceName = NombreServicio.GPH_ETL.ToString();
        }

        /// <summary>
        /// Realiza la inyección y establece un timer
        /// que ejecutará su delegado por tick del intervalo.
        /// </summary>
        protected override void Inicializacion(string[] args, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            //Inicializa la obtención de la configuración general y las rutas del log...   
            _configuracionBase = new Configuracion();
            LecturaConfguracionGeneral.Inicializar();
            ServicioConfiguracionSettings servicioMainConfiguracion = LecturaConfguracionGeneral.ObtenerServicio(NombreServicio.GPH_ETL.ToString());
            _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
            _configuracionBase.RutaArchivoConfiguracion = Path.Combine(EnumRutasGenerales.Path_Log_CMS, NombreServicio.GPH_ETL.ToString());
            _configuracionBase.PathLog = servicioMainConfiguracion.Log;
            _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
            _configuracionBase.configuracion = servicioMainConfiguracion.configuracion;
            _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
            _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
            _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;

            RutasConfiguracion rutas = new RutasConfiguracion()
            {
                MUFG_Servicios_Path_Log = _configuracionBase.PathLog
            };
            _log.Init(rutas, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
            _log.Write("INFO|OnStart|Leyendo archivo de configuración.");


            //Realiza la configuracion del _containerDI.
            _containerDI = new Container();
            _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
            _containerDI.Register<IAlertas>(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);
            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();

            //Registra la configuracion como Transient + Factory (lazy)
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);

            //Registra configuraciones/parametros propias del servicio
            _containerDI.Register<ParametrosConfiguracion>(Lifestyle.Singleton);

            //Registra los componentes
            _containerDI.Register<IOperacionServicio, OperacionServicio>();
            _containerDI.Register<IProcesoServicio, ProcesoServicio>();
            _containerDI.Register(() => new ConexionT24Mw(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW), Lifestyle.Scoped);
            _containerDI.Register<IConexionT24Service, ConexionT24Service>(Lifestyle.Scoped);

            //para registro de log con interface
            _containerDI.Register<ILoggerService, LoggerService>(Lifestyle.Singleton);


            //Repositorios
            _containerDI.Register<IRepositorioTEF, RepositorioTEF>(Lifestyle.Scoped);
            _containerDI.Register<IRepositorioTomi, RepositorioTomi>(Lifestyle.Scoped);
            _containerDI.Register<IRepositorioVT24, RepositorioVT24>(Lifestyle.Scoped);

            //
            _containerDI.Register<IEmailService, EmailService>(Lifestyle.Scoped);
            //Servicio para el manejo de errores
            _containerDI.Register<ControlErrorStore>(Lifestyle.Singleton);
            _containerDI.Register<IControlErrorService, ControlErrorService>(Lifestyle.Scoped);
            //Servicio 
            _containerDI.Register<IExtensionAlerta, ExtensionAlerta>(Lifestyle.Scoped);


            //tipos de procesos-operaciones
            _containerDI.Collection.Register<IProcesosOperacion>(
                new[]
                {
                    typeof(ServicioFXD),
                    typeof(ServicioTEF),
                    typeof(ServicioFXI),
                    typeof(ServicioFXT),
                    typeof(ServicioSPID)
                },
                Lifestyle.Scoped
            );



            _containerDI.Verify();

            
            using (AsyncScopedLifestyle.BeginScope(_containerDI))
            {
                _logger = _containerDI.GetInstance<ILoggerService>();
            }
        }


        /// <summary>
        /// Realiza la obtención inicial de parámetros (una sola vez).
        /// </summary>
        protected override async Task ParametrizacionesAsync(CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                //Obtiene el proceso actual...
                IParametrizacion _parametrizacion = _containerDI.GetInstance<IParametrizacion>();
                _configuracionBase = await _parametrizacion.ObtenerProcesoActualAsync(_configuracionBase);

                //Marca el servicio en uso...
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
                await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio);
            }
            catch (Exception ex)
            {
                _log.Error($"{MsgLog.ErrorParametrizacion}{ex}");
                throw;
            }
        }

        /// <summary>
        /// Ejecuta el funcionamiento principal del servicio en cada tick.
        /// </summary>  
        protected override async Task OnTimerAsync(CancellationToken ct)
        {
            using (AsyncScopedLifestyle.BeginScope(_containerDI))
            {
                try
                {
                    _logger.Info(SEPARADOR);
                    _logger.Info(LogsTimer.TIMER_INICIO);
                   
                    //validacion de acceso y conexion a bases de datos                    
                    var repTomi = _containerDI.GetInstance<IRepositorioTomi>();
                    var repVT24 = _containerDI.GetInstance<IRepositorioVT24>();
                    var repTef = _containerDI.GetInstance<IRepositorioTEF>();
                    bool _conexionBd = repTomi.ValidaAccesoBdTomiAsync().GetAwaiter().GetResult();
                    if (!_conexionBd)
                        throw new Exception(MsgLog.Bd.Tomi.ConexionError);
                    bool _conexionBdvT24 = repVT24.ValidaAccesoBdvt24Async().GetAwaiter().GetResult();
                    if (!_conexionBdvT24)
                        throw new Exception(MsgLog.Bd.VT24.ConexionError);
                    bool _conexionBdTef = repTef.ValidaAccesoBdTEFAsync().GetAwaiter().GetResult();
                    if (!_conexionBdTef)
                        throw new Exception(MsgLog.Bd.Tef.ConexionError);

                    //Ejecuta el procedimiento principal del servicio...
                    var operacionServicio = _containerDI.GetInstance<IOperacionServicio>();
                    await operacionServicio.EjecutarProcedimientosAsync().ConfigureAwait(false);

                }
                catch (Exception ex)
                {
                    _logger.Error($"{LogsTimer.TIMER_EXCEPTION} {ex}");
                }
                finally
                {
                    _logger.Info(LogsTimer.TIMER_FIN);
                    _logger.Info(SEPARADOR);
                }
            }
        }


        /// <summary>
        /// Realiza ciertas operaciones antes de finalizar el servicio.
        /// </summary>
        protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            _logger.Info(LogsTerminacion.INFO_ONSTOP_FINALIZANDO_INICIO);

            try
            {

                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();

                    await _controlServicio.EsperarTareaActivaAsync(startupTask, ConstantesSistema.ParametrizacionInicial, waitCt).ConfigureAwait(false);
                    await _controlServicio.EsperarTareaActivaAsync(runningTask, ConstantesSistema.ProcesamientoActivo, waitCt).ConfigureAwait(false);

                    //Marca el servicio en uso...
                    await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);

                    //Marca el termino del servicio...
                    await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);
                    _logger.Info(LogsTerminacion.TERMINACION_EXITOSA_BD);
                }
            }
            catch (ApplicationException exp)
            {                
                _logger.Info($"{LogsTerminacion.ERROR_APPLICATION_EXCEPTION}|{exp.Message}");
            }
            catch (OperationCanceledException ex)
            {
                _log.Info($"{LogsTerminacion.ERROR_CANCEL_EXCEPTION}|{ex}");
            }
            catch (Exception ex)
            {
                _logger.Error($"{LogsTerminacion.ERROR_TERMINACION}{ex}");
            }
            finally
            {
                _logger.Info(LogsTerminacion.INFO_ONSTOP_FINALIZANDO_FIN);
                _containerDI?.Dispose();
            }
        }


    }
}
using AplicacionGPH_ETL.Interfaces.Alertas;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Threading.Tasks;

namespace AplicacionGPH_ETL.Servicios.Alertas
{
    public class ExtensionAlerta : IExtensionAlerta
    {
        private readonly IAlertas _alertas;
        private readonly Configuracion _configuracion;
        public ExtensionAlerta(
            Configuracion configuracion
            , IAlertas alertas
        )
        {
            _configuracion = configuracion;
            _alertas = alertas;
        }

        /// <summary>
        /// Metodo para registrar alerta de tipo error
        /// </summary>
        /// <param name="ErrDesc"></param>
        /// <returns></returns>
        public async Task RegistraErrorAsync(string ErrDesc)
        {
            await Registra(ConstantesClaveAviso.Error, ErrDesc, null);
        }

        /// <summary>
        /// Método para registrar alerta de tipo error critico con descripcion extra
        /// </summary>
        /// <param name="descripcion"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        public async Task RegistraCriticoAsync(string descripcion, Exception ex)
        {
            await Registra(ConstantesClaveAviso.Critico, descripcion, ex);
        }
        /// <summary>
        /// Metodo para el registro de alerta solo con Exception
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public async Task RegistraCriticoAsync(Exception ex)
        {
            await Registra(ConstantesClaveAviso.Critico, "", ex);
        }


        /// <summary>
        /// Método que registra la alerta
        /// </summary>
        /// <param name="clave">Tipo de alerta</param>
        /// <param name="descripcion">Descripción del error</param>
        /// <param name="ex">Exception</param>
        /// <returns></returns>
        private async Task Registra(string clave, string descripcion, Exception ex)
        {
            await _alertas.InsertarAlerta(new AlertaDTO()
            {
                Folio = null,
                TipoOPeracion = null,
                Servicio = _configuracion.NombreServicio,
                ClaveAviso = clave,
                Descipcion = descripcion,
                PId = _configuracion.Pid.Value,
                NombreProceso = _configuracion.NombreProceso,
                Exception = ex
            });
        }

    }

}
using AplicacionGPH_ETL.Interfaces.Logger;
using AplicacionGPH_ETL.Interfaces.Repositorios;
using AplicacionGPH_ETL.Modelos;
using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Enums.ConstantesServicio;

namespace AplicacionGPH_ETL.Comunes
{
    /// <summary>
    /// Clase estatica para asignar y validar parametros
    /// </summary>
    public static class AsignaValidaParametros
    {
        /// <summary>
        /// Metodo para asignar los valores del dictionario de parametros de configuración a una clase
        /// además de validar los valores de algunos parametros necesarios para el correcto funcionamiento del servicio
        /// </summary>
        /// <param name="config"></param>
        /// <param name="HrSistema"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static async Task<bool> ParametrosAsync(
            Configuracion config,  
            Dictionary<string, string> paramsDb, 
            ParametrosConfiguracion _params, 
            IRepositorioTomi _repTomi,  
            ILoggerService _log)
        {            
            string msgErrorParametros = string.Empty;
            var fechaSis = DateTime.Now;
            bool HorariorValido = true;

            try
            {
                if(_params.HORA_GPH_INICIO == TimeSpan.Zero)
                {
                    if (paramsDb.GetValueTimeSpan(nameof(_params.HORA_GPH_INICIO)) == TimeSpan.Zero)
                        msgErrorParametros += MsgLog.Parametros.FaltaHoraInicio;

                    if (paramsDb.GetValueTimeSpan(nameof(_params.HORA_GPH_FIN)) == TimeSpan.Zero)
                        msgErrorParametros += ((string.IsNullOrEmpty(msgErrorParametros)) ? "" : ",") + MsgLog.Parametros.FaltaHoraFin;

                    if (!string.IsNullOrEmpty(msgErrorParametros))
                        throw new ConfigurationErrorsException($"{MsgLog.Parametros.ErrorParametros}{msgErrorParametros}");

                    //carga parametros
                    _params.HORA_GPH_INICIO = paramsDb.GetValueTimeSpan("HORA_GPH_INICIO");
                    _params.HORA_GPH_FIN = paramsDb.GetValueTimeSpan("HORA_GPH_FIN");
                    _params.HORA_GPH_INICIO_FXD = paramsDb.GetValueTimeSpan("HORA-GPH-INICIO-FXD");
                    _params.HORA_GPH_INICIO_SPID = paramsDb.GetValueTimeSpan("HORA-GPH-INICIO-SPID");
                    _params.HORA_GPH_INICIO_FXI = paramsDb.GetValueTimeSpan("HORA-GPH-INICIO-FXI");
                    _params.HORA_GPH_INICIO_FXT = paramsDb.GetValueTimeSpan("HORA-GPH-INICIO-FXT");
                    _params.HORA_GPH_INICIO_TEF = paramsDb.GetValueTimeSpan("HORA-GPH-INICIO-TEF");
                    _params.HORA_GPH_FIN_MD = paramsDb.GetValueTimeSpan("HORA_GPH_FIN_MD");
                    _params.HORA_GPH_FXD_FIN_MD = paramsDb.GetValueTimeSpan("HORA_GPH_FXD_FIN_MD");
                    _params.HORA_GPH_SPID_FIN_MD = paramsDb.GetValueTimeSpan("HORA_GPH_SPID_FIN_MD");
                    _params.HORA_GPH_FXI_FIN_MD = paramsDb.GetValueTimeSpan("HORA_GPH_FXI_FIN_MD");
                    _params.HORA_GPH_TEF_FIN_MD = paramsDb.GetValueTimeSpan("HORA_GPH_TEF_FIN_MD");
                    _params.HORA_GPH_FXT_FIN_MD = paramsDb.GetValueTimeSpan("HORA_GPH_FXT_FIN_MD");
                    _params.TOTAL_PAGOS_GPH_ACEPTADOS = paramsDb.GetValueIntOrDefault("TOTAL_PAGOS_GPH_ACEPTADOS", 400);
                    _params.RUTA_ARCHIVOS_GPH_XML = paramsDb.GetValueString("RUTA_ARCHIVOS_GPH_XML");
                    _params.RUTA_ARCHIVOS_GPH_XSD = paramsDb.GetValueString("RUTA_ARCHIVOS_GPH_XSD");
                    _params.GPH_ETL_EMAIL_ERROR_TO = paramsDb.GetValueStringOrDefault("GPH_ETL_EMAIL_ERROR_TO", "");
                    _params.GPH_ETL_EMAIL_ERROR_CC = paramsDb.GetValueStringOrDefault("GPH_ETL_EMAIL_ERROR_CC", "");
                    _params.GPH_ETL_EMAIL_FROM = paramsDb.GetValueStringOrDefault("GPH_ETL_EMAIL_FROM", "");

                    config.ParametrosSistema = paramsDb;
                }

                //Valida Horario operacion
                TimeSpan HrSistema = config.HoraSistema ?? DateTime.Now.TimeOfDay;
                config.HoraSistema = HrSistema;

                if (HrSistema < _params.HORA_GPH_INICIO || HrSistema > _params.HORA_GPH_FIN)
                    HorariorValido = false;

                if (!HorariorValido)
                {
                    //Si es un nuevo dia y esta en horario invalido...                
                    if (_params.UltFechaEjecucion.Date != fechaSis.Date || !_params.LogMessagesStandBy)
                        throw new InvalidOperationException(string.Format(MsgLog.EjecutaServicio.FueraHorario, DateTime.Now.ToString(ConstGenerales.FORMATO_FECHA), DateTime.Now.ToString(ConstGenerales.FORMATO_HORA), _params.HORA_GPH_INICIO, _params.HORA_GPH_FIN));

                    //continua con el proceso normal aunque no lo ejecuta
                    return false;
                }

                //validamos si es día habil
                config.EsDiaHabil = await _repTomi.EsDiaHabilAsync();
                //validamos tambien el  horario de operacion
                if (!config.EsDiaHabil.Value)
                {
                    //el registro entro en dia in habil 
                    //se valida si ya se realizo el registro de log de dia inhabil                
                    if (!_params.LogMessagesStandBy)
                        throw new InvalidOperationException(string.Format(MsgLog.EjecutaServicio.DiaInhabil, DateTime.Now.ToString(ConstGenerales.FORMATO_FECHA)));

                    //continua con el proceso normal aunque no lo ejecuta
                    //no se maneja como registro de alerta por que es una situación esperada
                    return false;
                }


                if (_params.LogMessagesStandBy)
                {
                    _params.LogMessagesStandBy = false;
                    //Escribirmos mensaje de restablecimiento de logs
                    _log.Info(string.Format(MsgLog.EjecutaServicio.RestablecerLogs, DateTime.Now.ToString(ConstGenerales.FORMATO_FECHA), DateTime.Now.ToString(ConstGenerales.FORMATO_HORA)));
                }

                //continua con el proceso normal
                return true;

            }
            catch (InvalidOperationException invex)
            {
                //Es una excepcion esperada, no propaga la excepcion, simplemente no ejecuta las operaciones del servicio por estar fuera de horario ó dian inhabil
                _log.Info(invex.Message);
                _log.Info(string.Format(MsgLog.EjecutaServicio.PausaLogs, DateTime.Now.ToString(ConstGenerales.FORMATO_FECHA), DateTime.Now.ToString(ConstGenerales.FORMATO_HORA)));
                _log.Info($"EjecutaServicio|..........");
                _params.LogMessagesStandBy = true;
                return false;
            }
            catch (Exception) {
                throw;
            }
            finally
            {
                _params.UltFechaEjecucion = DateTime.Now;
            }

        }

    }
}
using MUFG_T24.Comunes;
using System.ComponentModel;
using System.Linq;
using System.Reflection;

namespace AplicacionGPH_ETL.Enums
{
    /// <summary>
    /// Constantes generales del servicio
    /// </summary>
    public static class ConstantesServicio
    {


        /// <summary>
        /// Constantes generales
        /// </summary>
        public struct ConstGenerales
        {                        
            public const string FORMATO_FECHA = "yyyy/MM/dd";
            public const string FORMATO_HORA = "HH:mm";
            public const string FORMATO_FECHA_SQL = "yyyyMMdd";
            public const string GSTIPO_OPERACION = "GPH";            
            public const string TIPO_GROUP_HEADER = "GroupHeader";
            public const string TIPO_PAYMENT_INSTRUCTION = "PaymentInstruction";
            public const string EXTENSION_XML = ".xml";
            public const string MONEDAMX = "MXN";
            public const string MONEDAUSD = "USD";
            public const string ARCHIVO_XSLT = "output.xslt";            
            public const string GSAliasProceso = "ETL_ARCHIVO_ENTRADA";

            public const string OPERACION_ACEPTADA = "ACEPTADA";
            public const string OPERACION_RECHAZADA = "RECHAZADA";
            
        }

        /// <summary>
        /// Constantes de tipos de procesos
        /// </summary>
        public struct TiposCd
        {
            public const string SPEI01 = "SPEI01";
            public const string SPID = "SPID";
            public const string SWIFT = "SWIFT";
            public const string FT = "FT";            
            public const string TEF01 = "TEF01";
            public const string TEF03 = "TEF03";
            public const string TEF06 = "TEF06";
        }

        /// <summary>
        /// Constantes de tipos de operacion
        /// </summary>
        public struct OperacionTipos
        {
            public const string FXD = "FXD_GPH";
            public const string FXI = "FXI_GPH";
            public const string FXT = "FXT_GPH";
            public const string SPID = "SPID_GPH";
            public const string TEF = "TEF_GPH";
        }

        /// <summary>
        /// Constantes de tipos de operaciones GPH
        /// </summary>
        public enum TiposOperacionesGph {
            FXD_GPH,
            FXI_GPH,
            FXT_GPH,
            SPID_GPH,
            TEF_GPH
        }


        /// <summary>
        /// Constantes de tipos de Estatus
        /// </summary>
        public struct EstatusOpe
        {
            public const string SCNP = "SCNP";
            public const string SCNF = "SCNF";
            public const string CONP = "CONP";
            public const string CONF = "CONF";            
        }

        /// <summary>
        /// Constantes de valores Operativos
        /// </summary>
        public struct ValoresOperativos
        {
            public const string EstatusPendiente = ConstantesEstatusPlan.INI;
            public const string EstatusRechazoConfig = ConstantesEstatusPlan.RECC;
            public const string TipoOperacionSalidaTts = "218";
            public const string TipoOperacionSalidaTtsDesc = "Salida (TTS)";
            public const string ReferenciaControl = "A";
            public const string ReferenciaControlContador = "0";
            public const string CapturaSistema = "JEAI";
            public const string SaldoCero = "0";
            public const string TasaRetencionCero = "0";
            public const string UsuarioSistemaGph = ConstGenerales.GSTIPO_OPERACION;
            public const string CodigoOperacionTef = "60";
            public const string TipoOperacionFxd = "FXD";
            public const string TipoOperacionSpd = "SPD";
            public const string TipoOperacionFxt = "FXT";
            public const string TipoOperacionTef = "TEF";
            public const string TipoSalidaDomestica = "D";
            public const string TipoSalidaInternacional = "I";
            public const string TipoSalidaDomesticaDesc = "Doméstica";
            public const string TipoSalidaTefDomesticaDesc = "TEF Doméstica";

            public const string ViaPagoSPEI = "800";
            public const string ViaPagoTEF = "60";
            public const string ViaPagoUsd = "810";
            public const string ViaPagoJpy = "820";
            public const string ViaPagoEur = "830";

            public const string ViaPagoDescSPEI = "TRF ENVIADA MXN SPEI";
            public const string ViaPagoDescTEF = "TRF ENVIADA MXN TEF";
            public const string ViaPagoDescSPID = "TRF ENVIADA USD SPID";
            public const string ViaPagoDescUsdBtmny = "TRF ENVIADA USD BTMNY";
            public const string ViaPagoDescEur = "TRF ENVIADA EUR";
            public const string ViaPagoDescJpy = "TRF ENVIADA JPY";
            public const string TipoSalidaInternacionalDesc = "Internacional";

            public const string TipoCuentaTelegraphicTransfer = "TELEGRAPHIC TRANSFER";
            public const string PaymentAdviseAndPay = "ADVISE AND PAY";
            public const string ChargeDebt = "DEBT";
            public const string ChargeSharedCode = "SHAR";
            public const string ChargeCred = "CRED";
            public const string ChargeSender = "SENDER";
            public const string ChargeShared = "SHARED";
            public const string ChargeBeneficiary = "BENEFICIARY";
        }

        /// <summary>
        /// Constantes de tipos de parametros utilizados en el sistema
        /// </summary>
        private static readonly string[] ParametrosSistema =
        {
            "HORA_GPH_INICIO",
            "HORA_GPH_FIN",
            "RUTA_ARCHIVOS_GPH_XML",
            "RUTA_ARCHIVOS_GPH_XSD",
            "HORA_GPH_FIN_MD",
            "HORA-GPH-INICIO-FXD",
            "HORA-GPH-INICIO-SPID",
            "HORA-GPH-INICIO-FXI",
            "HORA-GPH-INICIO-FXT",
            "HORA-GPH-INICIO-TEF",
            "HORA_GPH_FXD_FIN_MD",
            "HORA_GPH_SPID_FIN_MD",
            "HORA_GPH_FXI_FIN_MD",
            "HORA_GPH_FXT_FIN_MD",
            "HORA_GPH_TEF_FIN_MD",
            "TOTAL_PAGOS_GPH_ACEPTADOS",
            "GPH_ETL_EMAIL_ERROR_TO",
            "GPH_ETL_EMAIL_ERROR_CC",
            "GPH_ETL_EMAIL_FROM"
        };
        /// <summary>
        /// Constante con la cadena de parametros para consulta en bd, cada parametro va envuelto con['] ( comillas simples) y separado por [,] (comas)
        /// </summary>
        public static readonly string stringParametros = string.Join(",", ParametrosSistema.Select(p => "'" + p + "'"));



        /// <summary>
        /// Constantes de intervalo de envió de mensajes en control de errores
        /// </summary>
        public static class ControlErrorIntervalos
        {
            public const int MinutoPrimero = 10;
            public const int MinutoRepetir = 5;
        }

        /// <summary>
        /// Constrantes para el envío de correos
        /// </summary>
        public static class EnvioCorreo
        {
            public const string EMISOR_NOMBRE = "Notificador Servicios";            
            public const string ASUNTO_ERROR_SERVICIO = "Error en Servicio de GPH_ETL";

        }

        /// <summary>
        /// Nodos que debe de contener el nodo de Pagos [CdtTrfTxInf] dentro de los archivos XML
        /// </summary>
        public static readonly string[] CdtTrfTxInf_NodosRequeridos = { "Pmtid", "Amt", "CdtrAgt", "Cdtr" };
        


        /// <summary>
        /// Constantes para los registros de logs
        /// </summary>
        public struct MsgLog
        {
            public const string LeyendoArchivoConfig = "INFO|OnStart|Leyendo archivo de configuración.";
            public const string ErrorParametrizacion = "ERROR|ParametrizacionesAsync|";        
            public const string SEPARADOR = "====================================================";
            public const string MsgGralException = "Error|Se presenta Exception";


            /// <summary>
            /// Logs para el metodo de terminación del servicio
            /// </summary>
            public static class LogsTerminacion
            {
                public const string INFO_ONSTOP_FINALIZANDO_INICIO = "OnStop|Terminacion|Finalizando Servicio|Inicio.";
                public const string INFO_ONSTOP_FINALIZANDO_FIN = "OnStop|Terminacion|Finalizando Servicio|Fin.";
                public const string TERMINACION_EXITOSA_BD = "OnStop|Terminacion|Terminación exitosa de bases de datos";
                public const string ERROR_TERMINACION = "OnStop|Terminacion|Exception:";
                public const string ERROR_APPLICATION_EXCEPTION = "OnStop|Terminacion|AplicationException:";
                public const string ERROR_CANCEL_EXCEPTION = "OnStop|Terminacion|CancelException:";
            }



            /// <summary>
            /// Mensajes de logs en el metodo de obtencion y validaicón de parametros
            /// </summary>
            public static class Parametros
            {
                public const string DictionaryNull = "Los parametros del servicio no se han podido inicializar, el valor de [{0}] es null";
                public const string FaltaKey = "No existe la clave [{0}] en parametros del sistema";
                public const string KeyNull = "El valor de la clave [{0}] es null.";
                public const string ValorBooleanInvalido = "El valor [{0}] de la clave [{1}]  no es un booleano válido. Valores permitidos:1,0,true, false.";
                public const string ValorTimeSpanInvalido = "El valor [{0}] de la clave [{1}] no es un TimeSpan válido";

                public const string ErrorParametros = "ERROR EN LA CONFIGURACIÓN DE PARAMETROS:";
                public const string FaltaHoraInicio = "[No se ha especificado la hora de inicio de GPH_ETL]";
                public const string FaltaHoraFin = "[No se ha especificado la hora de finalización de GPH_ETL]";
            }

            /// <summary>
            /// logs para el metodo de ejecución del Timer
            /// </summary>
            public static class LogsTimer
            {
                public const string TIMER_INICIO = "Timer|Inicio de ciclo";
                public const string TIMER_FIN = "Timer|Fin de ciclo";
                public const string TIMER_EXCEPTION = "Timer|Se presenta Exception|";                
            }



            /// <summary>
            /// Logs para el metodo principal de ejecución del servicio
            /// </summary>
            public static class EjecutaServicio
            {
                public const string Nombre = "EjecutaServicio|";
                public const string Inicio = Nombre + "Inicio";
                public const string Fin = Nombre + "Fin";
                public const string Exception = Nombre + MsgGralException;
                public const string DiaInhabil = Nombre + "EL SERVICIO ENTRÓ EN UN DÍA INHABIL, Fecha: [{0}]";
                public const string FueraHorario = Nombre + "EL SERVICIO ENTRÓ EN UN HORARIO NO PERMITIDO: Fecha:[{0}], Hora: [{1}]. HORARIO PERMITIDO: [{2}] a [{3}]";
                public const string PausaLogs = Nombre + "SE PAUSA EL REGISTRO DE LOGS: Fecha:[{0}], Hora: [{1}]";
                public const string RestablecerLogs = Nombre + "SE RESTABLECE EL REGISTRO DE LOGS: Fecha:[{0}], Hora: [{1}]";
                public const string ErrorServicio = "Se presentó un error en la ejecución del servicio";
                public const string ErrorEjecucionServicio = Nombre + ErrorServicio;
                public const string ERROR_ACTUALIZA_TERMINACION = Nombre + "Ocurrio un error al actualizar la terminación del servicio";
            }


            /// <summary>
            /// Mensajes de logs en em metodo de procedimieentos de la clase OperacionServicio
            /// </summary>
            public static class Procedimientos
            {
                public const string Nombre = "EjecutaProcedimientos|";
                public const string Inicio = Nombre + "Inicio";
                public const string Fin = Nombre + "Fin";
                public const string Exception = Nombre + MsgGralException;
                public const string DiaInhabil = "Hoy es un día inhábil";                
                public const string FueraHorario = "El servicio está fuera de horario";
                public const string ErrorFueraHorario = Nombre + FueraHorario;
                public const string ErrorServicio = "Se presentó un error en la ejecución del servicio";
                public const string ErrorEjecucionServicio = Nombre + ErrorServicio;
                public const string BuscaValoresParametros = Nombre + "Busca Valores de Parametros en bd";
            }

            public static class Bd
            {
                public const string ErrorEjecucionGral = "Error de ejecución en base de datos";                
                public static class Tomi
                {
                    public const string Repo = "RepositorioTomi|";
                    public const string ValidaAccesoInicio = Repo + "ValidaAccesoBd|Inicio";
                    public const string ValidaAccesoFin = Repo + "ValidaAccesoBd|Fin";

                    public const string ConexionOk = Repo + "Base de datos iniciada correctamente";
                    public const string msgAcceso = Repo + "Error en la apertura de la base TOMI";
                    public const string ConexionError = Repo + msgAcceso;
                    public const string Exception = Repo + MsgGralException;

                    public const string ErrorEjecucion = Repo + ErrorEjecucionGral;
                    public const string ProcesoEsDiaHabil = Repo + "EsDiaHabil|";
                }

                public static class VT24
                {
                    public const string Repo = "RepositorioVT24|";
                    public const string ValidaAccesoInicio = Repo + "ValidaAccesoBd|Inicio";
                    public const string ValidaAccesoFin = Repo + "ValidaAccesoBd|Fin";
                    public const string ConexionOk = Repo + "Base de datos iniciada correctamente";
                    public const string msgAcceso = Repo + "Error en la apertura de la base VT24";
                    public const string ConexionError = Repo + msgAcceso;
                    public const string Exception = Repo + MsgGralException;

                    public const string ErrorEjecucion = Repo + ErrorEjecucionGral;
                }

                public static class Tef
                {
                    public const string Repo = "RepositorioTEF|";
                    public const string ValidaAccesoInicio = Repo + "ValidaAccesoBd|Inicio";
                    public const string ValidaAccesoFin = Repo + "ValidaAccesoBd|Fin";
                    public const string ConexionOk = Repo + "Base de datos iniciada correctamente";
                    public const string msgAcceso = Repo + "Error en la apertura de la base TEF";
                    public const string ConexionError = Repo + msgAcceso;
                    public const string Exception = Repo + MsgGralException;

                    public const string ErrorEjecucion = Repo + ErrorEjecucionGral;
                }

            }


            public static class Procesos
            {
                private const string Nombre = "Procesos|";
                public const string Inicia = Nombre + "Inicio";
                public const string Fin = Nombre + "Fin";
                public const string Exception = Nombre + MsgGralException;

                public const string ErrorEjecucionProceso = "Ocurrio un error en la ejecución del proceso";

                public static class Serializar
                {
                    public const string SerializarXML = Nombre + "SerializarXML|";
                    public const string Inicio = SerializarXML + "Inicio";
                    public const string Fin = SerializarXML + "Fin";
                    public const string Exception = SerializarXML + MsgGralException;

                    public const string SinArchivoMaestro = SerializarXML + "No se encuentra el archivo maestro [output.xslt], en la ruta [{0}]";


                    public const string BuscaArchivos = SerializarXML + "Busca archivos xml para serializar";
                    public const string TotalArchivos = SerializarXML + "Se encontraron [{0}] archivos xml para serializar.";
                    public const string SinArchivos = SerializarXML + "No se encontraron archivos xml para serializar.";
                    public const string ExceptionSerializacion = SerializarXML + "Se presenta error al serializar el archivo:";
                    public const string IniciaSerializacionArchivos = SerializarXML + "Comienza la serializacion de archivos";                    
                    public const string ProcesandoArchivo = SerializarXML + "Inicio de Procesar archivo";
                    public const string FinProcesandoArchivo = SerializarXML + "Fin de Procesar archivo";
                    public const string ErroresAcumulados = SerializarXML + "Errores acumulados:";
                    public const string GenerandoArchivoAfter = SerializarXML + "Generando archivo {0}_After.xml";
                    public const string EliminandoArchivoAfter = SerializarXML + "Eliminando archivo {0}_After.xml";
                    public const string SerializandoArchivo = SerializarXML + "Serializando archivo {0}_After.xml";
                    public const string ObteniedoListaPagos = SerializarXML + "Obtener lista de pagos";
                    public const string ActualizaArchivoMaestro = SerializarXML + "Se actualiza Mensaje del registro Maestro con Folio {0}";
                    public const string CargaArregloPagos = SerializarXML + "Carga el arreglo de pagos en el archivo {0}.xml";
                    public const string InsertaRegistroPago = SerializarXML + "Objeto generado, insertando registros en BD";
                    public const string SeActualizoRegistro = SerializarXML + "El registro maestro con con Folio:{0} , se actualizó con msgId: {1}";
                    public const string ValidaAccesoCliente = SerializarXML + "Validar acceso del cliente al servicio GPH";                    
                    public const string ClienteSinAcceso = "The customer does not have permission to process payments in GPH";
                    public const string MarcaRegistroSerializado = SerializarXML + "MarcaRegistroSerializado Folio:";
                    public const string MarcaRegistroRechazado = SerializarXML + "El Tag <CdtTrfTxInf> contiene: {0} nodos. La estructura es invalida.|1T10072";
                    public const string InsertaOK = SerializarXML + "Se insertó correctamente el registro";
                    public const string ProcesadoOK = SerializarXML + "El archivo se proceso correctamente";
                }



                private const string FechaCalculada = "No se logro obtener la fecha calculada";

                private const string InsertarArchivo = "Insertar datos de archivo";
                private const string FolioUnico = "Generar Folio Unico";
                private const string AsignarFolio = "Se asignó correctamente el FolioUnico :[{0}] al registro con folio: {1}";
                private const string ConsultaRegistro = "Consulta registro con Folio: [{0}]";
                public const string AsignarEstatus = "Se Asigna Estatus [{0}] al registro con Folio [{1}]";
                public const string RegistroActualizado = "Se Actualizó correctamente el registro";
                public const string InsertaOperacionAceptada = "Se crea registro de operacion Aceptada del folio:[{0}]";
                public const string InsertaOperacionRechazada = "Se crea registro de operacion Rechazada del folio:[{0}]";
                public const string InsertaOperacionOk = "Se creó correctamente el registro";

                public static class FXI
                {
                    private const string ProcesoFXI = Nombre+"FXI|";
                    public const string Inicia = ProcesoFXI + "Inicio";
                    public const string Fin = ProcesoFXI + "Fin";
                    public const string Exception = ProcesoFXI + MsgGralException;
                    public const string Validaciones = ProcesoFXI + "Validaciones generales";
                    public const string InsertaArchivo = ProcesoFXI + InsertarArchivo;
                    public const string GenerarFolioUnico = ProcesoFXI + FolioUnico;
                    public const string AsignarFolioUnico = ProcesoFXI + AsignarFolio;
                    public const string ConsultaFolio = ProcesoFXI + ConsultaRegistro;
                    public const string InsertaEntradaSalida = ProcesoFXI + "Insertar datos de Entrada/Salida en TOMI.";
                    public const string InsertaEntradaSalidaOK = ProcesoFXI + "Se insertó correctamente el folio en base de Entrada/Salida.";


                }

                public static class FXD
                {
                    private const string ProcesoFXD = Nombre + "FXD|";
                    public const string Inicia = ProcesoFXD + "Inicio";
                    public const string Fin = ProcesoFXD + "Fin";
                    public const string Exception = ProcesoFXD + MsgGralException;
                    public const string InsertaArchivo = ProcesoFXD + InsertarArchivo;
                    public const string GenerarFolioUnico = ProcesoFXD + FolioUnico;
                    public const string AsignarFolioUnico = ProcesoFXD + AsignarFolio;
                    public const string ConsultaFolio = ProcesoFXD + ConsultaRegistro;
                    public const string InsertaEntradaSalida = ProcesoFXD + "Insertar datos de Entrada/Salida en TOMI.";
                    public const string InsertaEntradaSalidaOK = ProcesoFXD + "Se insertó correctamente el folio en base de Entrada/Salida.";



                }

                public static class TEF
                {
                    private const string ProcesoFXT = Nombre + "FXT|";
                    public const string Inicia = ProcesoFXT + "Inicio";
                    public const string Fin = ProcesoFXT + "Fin";
                    public const string Exception = ProcesoFXT + MsgGralException;
                    public const string InsertaArchivo = ProcesoFXT + InsertarArchivo;
                    public const string GenerarFolioUnico = ProcesoFXT + FolioUnico;
                    public const string AsignarFolioUnico = ProcesoFXT + AsignarFolio;

                    public const string ErrorFechaCalculada= ProcesoFXT + FechaCalculada;
                    public const string InsertaEntradaSalida = ProcesoFXT + "Insertar datos de Entrada/Salida en TOMI.";
                    public const string InsertaEntradaSalidaOK = ProcesoFXT + "Se insertó correctamente el folio en base de Entrada/Salida.";

                }

                public static class SPID
                {
                    private const string ProcesoSPID = Nombre + "SPID|";
                    public const string Inicia = ProcesoSPID + "Inicio";
                    public const string Fin = ProcesoSPID + "Fin";
                    public const string Exception = ProcesoSPID + MsgGralException;
                    public const string InsertaArchivo = ProcesoSPID + InsertarArchivo;
                    public const string GenerarFolioUnico = ProcesoSPID + FolioUnico;
                    public const string AsignarFolioUnico = ProcesoSPID + AsignarFolio;
                    public const string ConsultaFolio = ProcesoSPID + ConsultaRegistro;
                    public const string ObtenerDireccionCliente = ProcesoSPID + "Obtener Dirección del Cliente: [{0}]";
                    public const string DireccionClienteOk = ProcesoSPID + "Se encontró la dirección";
                    public const string DireccionClienteNo = ProcesoSPID + "Sin datos en la dirección";
                    public const string ValidaCliente = ProcesoSPID + "Valida Cliente";
                    public const string ClienteNoAutorizado = ProcesoSPID + "El cliente no está autorizado para operar";
                    public const string InsertaEntradaSalida = ProcesoSPID + "Insertar datos de Entrada/Salida en TOMI.";
                    public const string InsertaEntradaSalidaOK = ProcesoSPID + "Se insertó correctamente el folio en base de Entrada/Salida.";


                }

                public static class FXT
                {
                    private const string ProcesoFXT = Nombre + "FXT|";
                    public const string Inicia = ProcesoFXT + "Inicio";
                    public const string Fin = ProcesoFXT + "Fin";
                    public const string Exception = ProcesoFXT + MsgGralException;
                    public const string Validaciones = ProcesoFXT + "Validaciones generales";
                    public const string InsertaArchivo = ProcesoFXT + InsertarArchivo;
                    public const string GenerarFolioUnico = ProcesoFXT + FolioUnico;
                    public const string AsignarFolioUnico = ProcesoFXT + AsignarFolio;
                }

            }


        }

        /// <summary>
        /// Constantes de tipos de codigos de error y sus definiciones
        /// </summary>
        public static class Codigo
        {
            [Description("Exceed maximum number of transaction. Please count the number of transaction appropriately.")]
            public const string Cod_1Z35201 = "1Z35201";    //EstatusMaxPayments           
            [Description("Customer does not have permission to process payments in GPH.")]
            public const string Cod_1T10070 = "1T10070";    //EstatusAccessService
            [Description("Invalid IPV4 Address.|1T10057")]
            public const string Cod_1T10057 = "1T10057";
            [Description("Incorrect Code. Only TRF is allowed.|1T10001")]
            public const string Cod_1T10001 = "1T10001";
            [Description("Execution date is not in YYYY-MM-DD format.|1T10003")]
            public const string Cod_1T10003 = "1T10003";
            [Description("La fecha de ejecución cae en dia inhábil. Se rechaza el pago.|1Z30203")]
            public const string Cod_1Z30203 = "1Z30203";
            [Description("Invalid Account length. Must be 18 digits.|1T10006")]
            public const string Cod_1T10006 = "1T10006";
            [Description("Invalid BTMUM Code. Must be 40108.|1T10007")]
            public const string Cod_1T10007 = "1T10007";
            [Description("La referencia del cliente no tiene la longitud correcta.|1T10064")]
            public const string Cod_1T10064 = "1T10064";
            [Description("El monto no tiene la longitud correcta.|1T10009")]
            public const string Cod_1T10009 = "1T10009";
            [Description("La referencia del cliente debe ser numérica.|1T10100")]
            public const string Cod_1T10100 = "1T10100";
            [Description("Currency must be MXN.|1T10035")]
            public const string Cod_1T10035 = "1T10035";
            [Description("There is an error in the currency code in the tag 2.43 InstructedAmount <InstdAmt>. Only 'USD' is available.|1T10010")]
            public const string Cod_1T10010 = "1T10010";
            [Description("There is an error in the currency code in the tag 2.80.1.1.10 Proprietary <Prtry>. Only 'USD' or 'JPY' or 'EUR' is available.|1T10036")]
            public const string Cod_1T10036 = "1T10036";
            [Description("There is an error in the currency code in the tag 2.43 InstructedAmount <InstdAmt>. Only 'MXN' or 'USD' or 'JPY' or 'EUR' is available.|1T10034")]
            public const string Cod_1T10034 = "1T10034";
            [Description("El código del Banco Beneficiario es incorrecto. Deben ser 5 digitos.|1T10083")]
            public const string Cod_1T10083 = "1T10083";
            [Description("El código de 'PERSON_TYPE' es incorrecto. Sólo está disponible 'M' o 'F'.|1T10014")]
            public const string Cod_1T10014 = "1T10014";
            [Description("El código RFC o la clave CURP es incorrecto. Para RFC las longitudes admisibles son 12 o 13 dígitos. Para la clave CURP la longitud permitida es 18 dígitos.|1T10013")]
            public const string Cod_1T10013 = "1T10013";
            [Description("Código RFC incorrecto. Longitud debe ser 12 digitos|1T10005")]
            public const string Cod_1T10005 = "1T10005";
            [Description("El código de 'PERSON_TYPE' es incorrecto. Sólo está disponible  el código 'M'|1T10040")]
            public const string Cod_1T10040 = "1T10040";
            [Description("La clasificación de la operación no es correcta. Se rechaza el pago.|1T10054")]
            public const string Cod_1T10054 = "1T10054";
            [Description("There is an error in the code in the tag 11.1.10 Information <Inf>. Only 'NNK NI' is available|1T10053")]
            public const string Cod_1T10053 = "1T10053";
            [Description("There is an error in the Business Relationship code. Only '01', '02', '03', '04', '05', '06', '07'.|1T10055")]
            public const string Cod_1T10055 = "1T10055";
            [Description("Additional Remittance Information is required.|1T10082")]
            public const string Cod_1T10082 = "1T10082";
            [Description("El nombre del banco beneficiario no tiene la longitud correcta. Se rechaza el pago.|1T10032")]
            public const string Cod_1T10032 = "1T10032";
            [Description("La cuenta del Beneficiario es incorrecta. La longitud máxima permitida son 34 dígitos.|1T10049")]
            public const string Cod_1T10049 = "1T10049";
            [Description("Invalid Payment Reference.|1T10017")]
            public const string Cod_1T10017 = "1T10017";
            [Description("Invalid Destination Bank Code. Must be 5 digits.|1T10011")]
            public const string Cod_1T10011 = "1T10011";
            [Description("Nombre de beneficiario no tiene la longitud correcta. Se rechaza el pago.|1T10012")]
            public const string Cod_1T10012 = "1T10012";
            [Description("Cuenta del Ordenante invalida. La cuenta no pertece a un cliente de BTMUM|1T10019")]
            public const string Cod_1T10019 = "1T10019";
            [Description("El cliente no tiene permiso de ejecutar el tipo de pago TEF|1T10069")]
            public const string Cod_1T10069 = "1T10069";
            [Description("El cliente no tiene permiso de ejecutar el tipo de pago  FXD|1T10066")]
            public const string Cod_1T10066 = "1T10066";
            [Description("El cliente no tiene permiso de ejecutar el tipo de pago  SPID|1T10068")]
            public const string Cod_1T10068 = "1T10068";
            [Description("El cliente no tiene permiso de ejecutar el tipo de pago FXI|1T10065")]
            public const string Cod_1T10065 = "1T10065";
            [Description("El cliente no tiene permiso de ejecutar el tipo de pago FXT|1T10067")]
            public const string Cod_1T10067 = "1T10067";
            [Description("Cuenta del Ordenante invalida, no corresponde con la moneda 'MXN'.|1T10071")]
            public const string Cod_1T10071 = "1T10071";
            [Description("Cuenta del Ordenante invalida, no corresponde con la moneda 'USD'.|1T10056")]
            public const string Cod_1T10056 = "1T10056";
            [Description("El banco del beneficiario es invalido.|1T10021")]
            public const string Cod_1T10021 = "1T10021";            
            [Description("El Tag <PmtTpInf><LclInstrm><Cd> contiene un valor invalido:[{0}]. Tipo de pago invalido.|1T10062")]
            public const string Cod_1T10062 = "1T10062";
            [Description("El Tag <CdtTrfTxInf> contiene una estructura invalida de nodos")]
            public const string Cod_1T10072 = "1T10072";
            [Description("Duplicidad de pagos. El identificador de cada transaccion debe ser único.|1T10018")]            
            public const string Cod_1T10018 = "1T10018";
            [Description("Invalid characters found at node RmtInf.Strd.AddtlRmtInf")]
            public const string Cod_1T10073 = "1T10073";
            [Description("Invalid characters found at node ChrgBr")]
            public const string Cod_1T10074 = "1T10074";
            [Description("Invalid characters found at node Cdtr.Nm")]
            public const string Cod_1T10075 = "1T10075";
            [Description("La fecha de ejecución no puede ser menor a la fecha del día. Se rechaza el pago.|1Z30204")]
            public const string Cod_1Z30204 = "1Z30204";
            [Description("Operación fuera de horario.|1T10063")]
            public const string Cod_1T10063 = "1T10063";
            [Description("La fecha de ejecución no puede ser mayor a 15 días naturales. Se rechaza el pago.|1T10058")]
            public const string Cod_1T10058 = "1T10058";
            [Description("La fecha de ejecución cae en fin de semana. Se rechaza el pago.|1Z30205")]
            public const string Cod_1Z30205 = "1Z30205";
            [Description("Correlacion erronea  'ACCT_TYPE' es '40' pero la cuenta no es una 'Cuenta CLABE'. La longitud permitida es 18 dígitos para 'Cuenta CLABE'.|1T10022")]
            public const string Cod_1T10022 = "1T10022";
            [Description("La cuenta CLABE no corresponde con el dígito de control. Se rechaza el pago.|1T10023")]
            public const string Cod_1T10023 = "1T10023";
            [Description("La cuenta CLABE no corresponde al banco del beneficiario. Se rechaza el pago.|1T10024")]
            public const string Cod_1T10024 = "1T10024";
            [Description("Correlacion erronea  'ACCT_TYPE' es '03' pero la cuenta no es una 'Tarjeta de Crédito'. La longitud permitida es 16 dígitos para 'Tarjeta de Crédito'.|1T10046")]
            public const string Cod_1T10046 = "1T10046";
            [Description("Tipo de cuenta invalido, los valores permitidos son '40' y '0'|1T10016")]
            public const string Cod_1T10016 = "1T10016";
            [Description("Tipo de cuenta invalido, el valor permitido es de '40'|1T10045")]
            public const string Cod_1T10045 = "1T10045";
            [Description("Cuenta del beneficiario invalida. La cuenta no perdece a un cliente de BTMUM|1T10050")]
            public const string Cod_1T10050 = "1T10050";
            [Description("Difference between Account Currency AND Currency Field.|1T10044")]
            public const string Cod_1T10044 = "1T10044";
            [Description("There is an error in the currency code in the tag 2.43 InstructedAmount <InstdAmt>. Only 'USD' or 'JPY' or 'EUR' is available.|1T10043")]
            public const string Cod_1T10043 = "1T10043";
            [Description("There is an error in ChargeBearer. Only 'DEBT' or 'SHAR' or 'CRED' is available.|1T10039")]
            public const string Cod_1T10039 = "1T10039";
            [Description("La concatenacion de los nodos <Country>, <Ustrd> y <AddtlRmtInf> no debe sobrepasar los 140 caracteres|1T10086")]
            public const string Cod_1T10086 = "1T10086";
            [Description("The field <StrtNm>  is not allowed to be part of the message|1T10089")]
            public const string Cod_1T10089 = "1T10089";
            [Description("COUNTRY_BEN field is missing. Se rechaza el pago.|1T10060")]
            public const string Cod_1T10060 = "1T10060";
            [Description("El campo AddressLine es requerido. Se rechaza el pago.|1T10099")]
            public const string Cod_1T10099 = "1T10099";
            [Description("La ciudad del beneficiario no tiene la longitud correcta. Se rechaza el pago.|1T10096")]
            public const string Cod_1T10096 = "1T10096";
            [Description("La ciudad del Beneficiacio es requerida. Se rechaza el pago.|1T10098")]
            public const string Cod_1T10098 = "1T10098";
            [Description("La ciudad del beneficiario contiene caracteres invalidos|1T10097")]
            public const string Cod_1T10097 = "1T10097";
            [Description("La direccion del beneficiario contiene caracteres invalidos|1T10076")]
            public const string Cod_1T10076 = "1T10076";
            [Description("El nodo <Prtry> contiene caracteres invalidos|1T10077")]
            public const string Cod_1T10077 = "1T10077";
            [Description("El campo AddressLine no tiene la longitud correcta. Se rechaza el pago.|1T10059")]
            public const string Cod_1T10059 = "1T10059";
            [Description("El nodo <Nm> banco beneficiario contiene caracteres invalidos|1T10078")]
            public const string Cod_1T10078 = "1T10078";
            [Description("La estructura del código SWIFT del campo 2.77.6.1.1 BIC <BIC> es incorrecto.|1T10030")]
            public const string Cod_1T10030 = "1T10030";
            [Description("Intermediary bank  SWIFT BIC is not allowed to use for USD payment |1T10090")]
            public const string Cod_1T10090 = "1T10090";
            [Description("Intermediary bank  name is not allowed to use for USD payment |1T10091")]
            public const string Cod_1T10091 = "1T10091";
            [Description("Intermediary bank  SWIFT BIC is not allowed to use for JPY payment |1T10092")]
            public const string Cod_1T10092 = "1T10092";
            [Description("Intermediary bank  name is not allowed to use for JPY payment |1T10093")]
            public const string Cod_1T10093 = "1T10093";
            [Description("Intermediary bank  SWIFT BIC is not allowed to use for EUR payment |1T10094")]
            public const string Cod_1T10094 = "1T10094";
            [Description("Intermediary bank  name is not allowed to use for EUR payment |1T10095")]
            public const string Cod_1T10095 = "1T10095";
            [Description("By official disposition it is no longer possible to pay USD to Mexico, please use payments by SPID|1T10084")]
            public const string Cod_1T10084 = "1T10084";
            [Description("By official disposition it is no longer possible to pay MXN to Mexico, please use payments by SPEI|1T10085")]
            public const string Cod_1T10085 = "1T10085";
            [Description("La estructura del código SWIFT del campo 2.71.6.1.1 BIC <BIC> es incorrecto.|1T10028")]
            public const string Cod_1T10028 = "1T10028";
            [Description("El nombre del banco no tiene la longitud correcta. Se rechaza el pago.|1T10029")]
            public const string Cod_1T10029 = "1T10029";
            [Description("El nodo <IntrmyAgt1><FinInstnId><Nm> banco corresponsal contiene caracteres invalidos|1T10079")]
            public const string Cod_1T10079 = "1T10079";
            [Description("El nodo <CdtrAcct><Id><Othr><Id> cuenta beneficiario contiene caracteres invalidos|1T10080")]
            public const string Cod_1T10080 = "1T10080";
            [Description("El nodo <RmtInf><Ustrd> contiene caracteres invalidos|1T10081")]
            public const string Cod_1T10081 = "1T10081";

            [Description("There is an error in the currency code in the tag 2.80.1.1.10 Proprietary <Prtry>. Only 'MXN' or 'USD' or 'JPY' or 'EUR' is available.|1T10052")]
            public const string Cod_1T10052 = "1T10052";

            public static string GetDescription(string FieldName)
            {
                var field = typeof(Codigo).GetField($"Cod_{FieldName}");
                var attribute = field?.GetCustomAttribute<DescriptionAttribute>();
                return attribute?.Description ?? string.Empty;
            }
        }

        
        /// <summary>
        /// Constantes para procedimientos almacenados
        /// </summary>
        public static class Sp {             
            public static class VT24
            {
                public const string uspListCustomerEdoCtaT24 = "usp_List_CUSTOMER_EdoCta_T24";
                public const string uspObtenerCuentaT24 = "MUFG_MX2_GphEtlObtenerCuentaT24";
            }

            public static class TEF
            {
                public const string uspObtenerBancoBeneficiarioTEF = "usp_GphEtlObtenerBancoBeneficiarioTEF";
            }

            public static class Tomi
            {
                public const string uspObtenerListaOperaciones = "usp_GphEtlObtenerListaOperacionesTOMI";
                public const string uspMaestroActualizaMsgId = "usp_GphEtlMaestroActualizaMsgIdTOMI";
                public const string uspCuentaServiciosCliente = "usp_GphEtlCuentaServiciosClienteTOMI";
                public const string uspObtenerFolioOperacion = "usp_GphEtlObtenerFolioOperacionTOMI";                
                public const string uspInsertaOperacion = "usp_GphEtlInsertaOperacionTOMI";                
                public const string uspActualizarSrvGphArchivosSerializados = "usp_GphEtlActualizarSrvGphArchivosSerializados";
                public const string uspActualizarSrvGphArchivosSerializadosConError = "usp_GphEtlActualizarSrvGphArchivosSerializadosConError";                
                public const string uspValidarFechaEjecucionValida = "usp_GphEtlValidarFechaEjecucionValida";
                public const string uspValidarPermisoCliente = "usp_GphEtlValidarPermisoCliente";
                public const string uspValidarPagoNuevoTEF = "usp_GphEtlValidarPagoNuevoTEF";
                public const string uspConsultaDiaAnteriorHabil = "usp_GphEtlConsultarDiaAnteriorHabil";
                public const string uspFechaEjecucionEsFinDeSemana = "usp_GphEtlValidarFechaEjecucionFinDeSemana";                
                public const string uspInsertaRegistroTEF = "usp_GphEtlInsertaRegistroTEF";
                public const string uspGphEtlObtenerRegistroEntradaSalidaTEF = "usp_GphEtlObtenerRegistroEntradaSalidaTEF";
                public const string uspInsertaRegistroEntradaSalidas = "usp_GphEtlInsertaRegistroEntradaSalidas";
                public const string upsActualizaFolioUnicoTEFGPH = "usp_GphEtlActualizaFolioUnicoTEF";
                public const string uspFinalizaOperacionArchivo = "usp_GphEtlFinalizaOperacionArchivo";
                public const string uspActualizaEstatusRegistroTEF = "usp_GphEtlActualizaEstatusRegistroTEF";
                public const string uspObtenerBancoBeneficiarioInstituciones = "usp_GphEtlObtenerBancoBeneficiarioInstituciones";
                public const string uspValidarPagoNuevoFXD = "usp_GphEtlValidarPagoNuevoFXD";
                public const string usplInsertaRegistroFXD = "usp_GphEtlInsertaRegistroFXD";
                public const string upsActualizaFolioUnicoFXDGPH = "usp_GphEtlActualizaFolioUnicoFXD";
                public const string uspGphEtlObtenerRegistroEntradaSalidaFXD = "usp_GphEtlObtenerRegistroEntradaSalidaFXD";
                public const string uspActualizaEstatusRegistroFXD = "usp_GphEtlActualizaEstatusRegistroFXD";
                public const string uspValidarPagoNuevoSPID = "usp_GphEtlValidarPagoNuevoSPID";
                public const string uspObtenerBancoBeneficiarioInstitucionesSPID = "usp_GphEtlObtenerBancoBeneficiarioInstitucionesSPID";
                public const string usplInsertaRegistroSPID = "usp_GphEtlInsertaRegistroSPID";
                public const string upsActualizaFolioUnicoSPID = "usp_GphEtlActualizaFolioUnicoSPID";
                public const string uspObtenerRegistroEntradaSalidaSPID = "usp_GphEtlObtenerRegistroEntradaSalidaSPID";
                public const string usp_ValidarExistenciaClienteSPID = "usp_GphEtlValidarExistenciaClienteSPID";
                public const string uspActualizaEstatusRegistroSPID = "usp_GphEtlActualizaEstatusRegistroSPID";
                public const string uspValidarPagoNuevoFXI = "usp_GphEtlValidarPagoNuevoFXI";
                public const string uspObtenerNombrePais = "usp_GphEtlObtenerNombrePais";
                public const string uspValidarCityAddressRequired = "usp_GphEtlValidarCityAddressLineRequired";
                public const string usplInsertaRegistroFXI = "usp_GphEtlInsertaRegistroFXI";
                public const string upsActualizaFolioUnicoFXI = "usp_GphEtlActualizaFolioUnicoFXI";
                public const string uspObtenerRegistroEntradaSalidaFXI = "usp_GphEtlObtenerRegistroEntradaSalidaFXI";
                public const string uspActualizaEstatusRegistroFXI = "usp_GphEtlActualizaEstatusRegistroFXI";
                public const string uspValidarPagoNuevoFXT = "usp_GphEtlValidarPagoNuevoFXT";
                public const string usplInsertaRegistroFXT = "usp_GphEtlInsertaRegistroFXT";
                public const string upsActualizaFolioUnicoFXT = "usp_GphEtlActualizaFolioUnicoFXT";
                public const string uspActualizaEstatusRegistroFXT = "usp_GphEtlActualizaEstatusRegistroFXT";
                public const string uspTombinInsertDetalleArchivo = "usp_TOMBIN_INSERT_DETALLE_ARCHIVO_GPH";

            }
        }
        

    }
}
using AplicacionGPH_ETL.DTOs;
using AplicacionGPH_ETL.Modelos;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static AplicacionGPH_ETL.Modelos.Pagos;

namespace AplicacionGPH_ETL.Interfaces.Repositorios
{
    /// <summary>
    /// Interfaz para el acceso con la base de datos Tomi
    /// </summary>
    public interface IRepositorioTomi
    {
        Task<bool> ValidaAccesoBdTomiAsync();

        /// <summary>
        /// Declaración del método para validar si la fecha actual es un día inhabil
        /// </summary>
        /// <returns></returns>
        Task<bool> EsDiaHabilAsync();


        Task<List<ArchivosGphDTO>> ObtenerListaOperaciones(DateTime FechaSistema);
        /// <summary>
        /// Finaliza Archivo, inserta operacion Aceptada o Rechazada [TB_SRV_GPH_DETALLE_ARCHIVO]
        /// </summary>
        /// <param name="Archivo">Datos del archivo procesado</param>
        /// <param name="Oper">Tipo de Operacion [FXD,FXI,FXT,SPID,TEF]</param>
        /// <param name="strError">Operacion Aceptada ó Claves de Error  </param>
        /// <param name="FolioOp"></param>
        /// <param name="Estatus">ACEPTADA/RECHAZADA</param>
        /// <param name="TipoOperacion">TEF_GPH,SPID_GPH,FXT_GPH,FXI_GPH,FXD_GPH</param>
        /// <returns></returns>
        Task<bool> FinalizaOperacionArchivo(ArchivosGphDTO Archivo, PaymentInstruction Oper, string strError, long FolioOp
            , string Estatus, string TipoOperacion);

        Task<bool> MaestroActualizaMsgId(long Folio, string TipoOperacion, string MsgId);
        Task<int> ValidarAccesoServicioAsync(long CustomerId);
        Task<long> ObtenerFolioOperacion(DateTime FechaEjecucion, string NfPmtInfId, string NfNombreArchivoGph, string TipoOperacion);
        Task<int> InsertaDetalleArchivo(long Folio, string PmtInfID, string Estatus, string ErrorCodes, string EndToEndId, string NfArchivoXML);
        
        Task<bool> InsertaOperacionTOMI(long Folio, string TipoOperacion, string PmtInfId, string Estatus , string ErrorCodes
            , string EndToEndId, long FolioOperacion);

        /// <summary>
        /// Operación para registrar archivo de entradaSalida [TB_TOMBIN_ENTRADAS_SALIDAS]
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        Task<bool> InsertaEntradaSalida(TbTombinEntradasSalidasDto item);


        Task<bool> MarcarRegistroConError(long Folio, string descripcionError);
        Task<bool> MarcarRegistrosSerializados(long Folio, int TotalOperaciones, string headerxml);


        Task<bool> EsFechaEjecucionEnDiaHabil(string FechaEjecucion);
        Task<bool> EsFechaEjecucionEnFinSemana(string FechaEjecucion);
        
        Task<bool> ValidaPermisoEjecucion(string customer_cdl, string Servicio);


        Task<string> ConsultaFechaCalculada(string fecha);



        Task<bool> ValidarPagoNuevoTEF(DateTime fechaSistema, string PmtInfId, string nombreaArchivo);
        Task<long> InsertaFormatoTEF(TB_TOMBIN_NF_TEF_GPH valoresFormato, ArchivosGphDTO Archivo, PaymentInstruction Oper, string Estatus
            , GroupHeader GroupHeaderFile);
        Task<bool> ActualizaFolioTEF(string FolioUnico, long FolioOperacion);
        Task<TB_TOMBIN_NF_TEF_GPH> ConsultaRegistroTEF(long NF_FOLIO);
        Task<bool> ActualizaEstatusRegistroTEF(long Folio, string Estatus);



        Task<bool> ValidarPagoNuevoFXD(DateTime fechaSistema, string PmtInfId, string nombreaArchivo);
        Task<long> InsertaFormatoFXD(TB_TOMBIN_NF_TEF_GPH valoresFormato, ArchivosGphDTO Archivo, PaymentInstruction Oper
            , string Estatus, GroupHeader GroupHeaderFile);
        Task<bool> ActualizaFolioFXD(string FolioUnico, long FolioOperacion);
        Task<TB_TOMBIN_NF_TEF_GPH> ConsultaRegistroFXD(long NF_FOLIO);
        Task<bool> ActualizaEstatusRegistroFXD(long Folio, string Estatus);

        Task<BancoBeneficiarioDTO> ObtenerBancoBeneficiarioInstituciones(decimal InsClave);


        Task<bool> ValidarPagoNuevoSPID(DateTime fechaSistema, string PmtInfId, string nombreaArchivo);
        Task<BancoBeneficiarioDTO> ObtenerBancoBeneficiarioInstitucionesSPID(decimal InsClave);
        /// <summary>
        /// Inserta registro en la tabla [TB_TOMBIN_NF_SPID_GPH]
        /// </summary>
        /// <param name="valoresFormato"></param>
        /// <param name="Archivo"></param>
        /// <param name="Oper"></param>
        /// <param name="Estatus"></param>
        /// <param name="GroupHeaderFile"></param>
        /// <returns></returns>
        Task<long> InsertaFormatoSPID(TB_TOMBIN_NF_TEF_GPH valoresFormato, ArchivosGphDTO Archivo, PaymentInstruction Oper
            , string Estatus, GroupHeader GroupHeaderFile);
        Task<bool> ActualizaFolioSPID(string FolioUnico, long FolioOperacion);
        /// <summary>
        /// Operación para la consulta de datos de un registro de tipo SPID [TB_TOMBIN_NF_SPID_GPH]
        /// </summary>
        /// <param name="NF_FOLIO"></param>
        /// <returns></returns>
        Task<TB_TOMBIN_NF_TEF_GPH> ConsultaRegistroSPID(long NF_FOLIO);
        Task<bool> ValidarClienteSPID(string cdl);
        Task<bool> ActualizaEstatusRegistroSPID(long Folio, string Estatus);


        Task<bool> ValidarPagoNuevoFXI(DateTime fechaSistema, string PmtInfId, string nombreaArchivo);
        Task<string> ObtenerNombrePais(string cve_country);
        Task<(bool cityRequired, bool addressRequired)> ValidarCityAddressLineRequired(string cve_country);
        Task<long> InsertaFormatoFXI(TB_TOMBIN_NF_FXI_GPH valoresFormato, ArchivosGphDTO Archivo, PaymentInstruction Oper
            , string Estatus, GroupHeader GroupHeaderFile);
        Task<bool> ActualizaFolioFXI(string FolioUnico, long FolioOperacion);
        Task<TB_TOMBIN_NF_FXI_GPH> ConsultaRegistroFXI(long NF_FOLIO);
        Task<bool> ActualizaEstatusRegistroFXI(long Folio, string Estatus);


        Task<bool> ValidarPagoNuevoFXT(DateTime fechaSistema, string PmtInfId, string nombreaArchivo);
        Task<long> InsertaFormatoFXT(TB_TOMBIN_NF_FXI_GPH valoresFormato, ArchivosGphDTO Archivo, PaymentInstruction Oper
            , string Estatus, GroupHeader GroupHeaderFile);
        Task<bool> ActualizaFolioFXT(string FolioUnico, long FolioOperacion);
        Task<bool> ActualizaEstatusRegistroFXT(long Folio, string Estatus);
    }


}
using AplicacionGPH_ETL.Interfaces.Servicios;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace AplicacionGPH_ETL.Servicios
{
    public class ConexionT24Service : IConexionT24Service
    {
        private readonly ConexionT24Mw _conexionT24;

        public ConexionT24Service(ConexionT24Mw conexionT24)
        {
            _conexionT24 = conexionT24;
        }

        public string MonedaDefaultMN => _conexionT24.MonedaDefaultMN;

        public Task<string> GeneraFolioUnico(string origen, string tipo, string tipoOperacion, DateTime Fecha, string folioOperacion)
        {
            string fechaJuliana =  Fecha.ToString("yy") + Fecha.DayOfYear.ToString("D3");
            return _conexionT24.GeneraFolioUnico(origen, tipo, tipoOperacion, fechaJuliana, folioOperacion);
        }
    }
}
using System;
using System.Threading.Tasks;

namespace AplicacionGPH_ETL.Interfaces.Servicios
{
    public interface IConexionT24Service
    {
        string MonedaDefaultMN { get; }

        Task<string> GeneraFolioUnico(string origen, string tipo, string tipoOperacion, DateTime Fecha, string folioOperacion);
    }
}
