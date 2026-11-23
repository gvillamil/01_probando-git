using AplicacionGPH_ArchivosSalida.Comunes;
using AplicacionGPH_ArchivosSalida.DTOs;
using AplicacionGPH_ArchivosSalida.Enums;
using AplicacionGPH_ArchivosSalida.Interfaces;
using AplicacionGPH_ArchivosSalida.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Core.Configuracion;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using PgpCore;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using System.Xml.XPath;
using WinSCP;
using IOperacionServicio = AplicacionGPH_ArchivosSalida.Interfaces.IOperacionServicio;


namespace AplicacionGPH_ArchivosSalida
{
    /// <summary>
    /// Clase principal de entrada para la ejecución del servicio
    /// </summary>
    public class OperacionServicio : IOperacionServicio
    {
        private readonly Configuracion _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IAlertas _alertas;
        private readonly IParametrizacion _parametrizacion;
        private readonly IRepositorioTomi _repositorioTomi;
        private readonly IRealizaControlError _realizaControlError;
        private ControlErrorEstructura[] loControlError = new ControlErrorEstructura[2];
        private int giCountErrorGral;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="configuracion"></param>
        /// <param name="controlServicio"></param>
        /// <param name="alertas"></param>
        /// <param name="parametrizacion"></param>
        /// <param name="repositorioTomi"></param>
        /// <param name="realizaControlError"></param>
        public OperacionServicio(Configuracion configuracion,
           IControlServicio controlServicio,
           IAlertas alertas,
           IParametrizacion parametrizacion,
           IRepositorioTomi repositorioTomi,
           IRealizaControlError realizaControlError
           )
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _alertas = alertas;
            _parametrizacion = parametrizacion;
            _repositorioTomi = repositorioTomi;
            _realizaControlError = realizaControlError;
        }

        /// <summary>
        /// Método principal para Ejecucion de procedimientos.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> EjecutarProcedimientosAsync(CancellationToken ct)
        {
            try
            {
                await InicializarContextoAsync(ct);

                if (!await EsDiaHabilAsync())
                    return false;

                if (!await EstaEnHorarioOperacion())
                    return false;

                await ProcesarOperacionesAsync();
                return true;
            }
            catch (Exception ex)
            {
                LogError($"Error en EjecutarProcedimientos: {ex}");
                await RegistrarAlertaAsync(ConstantesServicio.ClaveAvisoCritico, ex);
                await _realizaControlError.RealizaControlErrorExceptionAsync(0, ex, loControlError, _configuracion);
                return false;
            }
            finally
            {
                await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);                
            }
        }
        #region Procesamiento
        /// <summary>
        /// Método principal para procesar operaciones.
        /// </summary>
        /// <returns></returns>
        private async Task ProcesarOperacionesAsync()
        {
                LogInfo("INFO|realiza_operaciones|***** Nuevo ciclo de GPH Archivo Salida *****");                
                await GeneraArchivosLevel1();                
                await GeneraArchivosLevel3();                
                await EncriptarArchivos();                
                await EnviarArchivosSFTP();               
                LogInfo("INFO|realiza_operaciones|Fin de ciclo de GPH Archivo Salida");
        }

        #endregion

        #region Inicialización
        /// <summary>
        /// Método para inicializar servicio y parametros del sistema.
        /// </summary>
        /// <returns></returns>
        private async Task InicializarContextoAsync(CancellationToken ct)
        {
            await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct);

            _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync(ct);
            _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil();
            _configuracion.ParametrosSistema =
                await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesServicio.stringParametros, ConstantesServicio.stringParametrosSensibles,ct);

        }

        #endregion

        #region Validaciones
        /// <summary>
        /// Método para validar si es un día inhabil.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> EsDiaHabilAsync()
        {
            if (_configuracion.EsDiaHabil == true) return true;
            LogError($"ERROR|{ConstantesServicio.MensajeDiaInhabil}");
            await RegistrarAlertaAsync(ConstantesServicio.ClaveAvisoError, new Exception(ConstantesServicio.MensajeDiaInhabil));
            return false;
        }

        /// <summary>
        /// Método para validar si el servicio está en horario de Operación.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> EstaEnHorarioOperacion()
        {
            var horaActual = _parametrizacion.ObtenerHoraSistema();
            var inicio = ConversionesComunes.ParseTimeSpanRequerido(_configuracion.ParametrosSistema[ConstantesServicio.HoraGPH_Inicio], ConstantesServicio.HoraGPH_Inicio);
            var fin = ConversionesComunes.ParseTimeSpanRequerido(_configuracion.ParametrosSistema[ConstantesServicio.HoraGPH_Fin], ConstantesServicio.HoraGPH_Fin);
            if (horaActual >= inicio && horaActual <= fin)
                return true;

            var mensaje = $"El servicio '{_configuracion.NombreServicio}' está fuera de horario";

            LogError(mensaje);
            await RegistrarAlertaAsync(ConstantesServicio.ClaveAvisoError, new Exception(mensaje), "Servicio fuera de horario");
            return false;
        }

        #endregion

        #region Alertas y Logging
        /// <summary>
        /// Método para registrar alertas.
        /// </summary>
        /// <param name="tipo"></param>
        /// <param name="ex"></param>
        /// <param name="descripcion"></param>
        /// <returns></returns>
        private async Task RegistrarAlertaAsync(string tipo, Exception ex, string descripcion = null)
        {
            await _alertas.InsertarAlerta(new AlertaDTO
            {
                Servicio = _configuracion.NombreServicio,
                ClaveAviso = tipo,
                Descipcion = descripcion,
                PId = _configuracion.Pid.Value,
                NombreProceso = _configuracion.NombreProceso,
                Exception = ex
            });
        }

        /// <summary>
        /// Método para registrar Log informativo.
        /// </summary>
        /// <param name="msg"></param>
        private void LogInfo(string msg) =>
            Logging.Info($"|Ejecuta_Procedimientos|{msg}");

        /// <summary>
        /// Método para registrar log de errores.
        /// </summary>
        /// <param name="msg"></param>
        private void LogError(string msg) =>
            Logging.Error($"|Ejecuta_Procedimientos|{msg}");

        #endregion

        #region L1
        /// <summary>
        /// Método principal para generar archivos Level1.
        /// </summary>
        /// <returns></returns>
        private async Task GeneraArchivosLevel1()
        {
            try
            {                
                LogInfo("INFO|GeneraArchivosLevel1|Obteniendo lista de archivos con estatus RECHAZADO.");                
                List<OperacionL1DTO> operaciones = await _repositorioTomi.RecuperarOperacionLevel1Async();
                if (!operaciones.Any())
                {
                    LogInfo("INFO|GeneraArchivosLevel1|No se encontraron archivos");                    
                }
                else {
                    foreach (OperacionL1DTO element in operaciones)
                    {
                        try
                        {
                            string nombreArchivo = Convert.ToString(element?.Nombre_archivo) ?? string.Empty;
                            string[] Nombres = nombreArchivo.Split('_');
                            string lsFolioFile = Convert.ToString(element?.FOLIO_FILE) ?? string.Empty;
                            string CustId = "";
                            string archivoNew;

                            if (Nombres.Length > 1)
                            {
                                archivoNew = Nombres[0] + "_" + Nombres[1] + "_ERROR_" + lsFolioFile;
                                CustId = Nombres[1];
                            }
                            else
                            {
                                archivoNew = nombreArchivo + "_ERROR_" + lsFolioFile;
                                CustId = "NA";
                            }

                            string gsRutaFilesTransmite = "";
                            var L1 = new ActualizaL1
                            {
                                Tipo = 0,
                                Folio = element.Folio,
                                ArchivoNew = archivoNew

                            };

                            LogInfo("INFO|GeneraArchivosLevel1|Genera archivo nivel 1 para: " + archivoNew);

                            if (!File.Exists(archivoNew))
                            {
                                gsRutaFilesTransmite = _configuracion.ParametrosSistema[ConstantesServicio.RUTA_ARCHIVOS_GPH_TRANSMITE];
                                using (StreamWriter file = new StreamWriter(Path.Combine(gsRutaFilesTransmite, archivoNew, ".txt")))
                                {
                                    file.WriteLine("GPH Customer ID".PadRight(19, ' ') + ": " + CustId);
                                    file.WriteLine("Received Date Time".PadRight(19, ' ') + ": " + element.Fecha_inicio);
                                    file.WriteLine("Received File ID".PadRight(19, ' ') + ": " + nombreArchivo);
                                    file.WriteLine("BTMU Reference".PadRight(19, ' ') + ": NA");
                                    file.WriteLine("Sender".PadRight(19, ' ') + ": NA");
                                    file.WriteLine("Status Code".PadRight(19, ' ') + ": 10-9999");
                                    file.WriteLine("Error".PadRight(19, ' ') + ": " + (element?.Err_descripcion ?? string.Empty));
                                }

                                LogInfo("INFO|GeneraArchivosLevel1|Actualizando el nombre del archivo en maestro. Archivo: " + archivoNew);
                                L1.Tipo = 1;
                                await _repositorioTomi.MarcarOperacionProcesadaLevel1Async(L1);
                            }
                            else
                            {
                                LogInfo("INFO|GeneraArchivosLevel1|Error el archivo ya fue generado. Archivo: " + archivoNew);
                                L1.Tipo = 2;
                                await _repositorioTomi.MarcarOperacionProcesadaLevel1Async(L1);
                            }
                        }
                        catch (Exception ex)
                        {
                            LogError("ERROR|GeneraArchivosLevel1|Se presenta exception: " + ex.ToString());
                        }
                    }

                }

            }
            catch (Exception ex) 
            {                
                LogError("ERROR|GeneraArchivosLevel1|Se presenta exception: " + ex.ToString());                
            }
            
        }

        #endregion

        #region L3
        /// <summary>
        /// Método principal para generar archivos Level3.
        /// </summary>
        /// <returns></returns>
        private async Task GeneraArchivosLevel3()
        {
            var L3 = new ActualizaL3Archivo
            {
                Tipo = 1,
                Folio = 0,
                ArchivoNew = "",
                Num_operaciones_aceptadas = 0,
                Num_operaciones_rechazadas = 0

            };
            string gsRutaFilesTransmite = string.Empty;            
            string ErrorGral = string.Empty;                        
            try
            {
                LogInfo("GeneraArchivosLevel3|Obteniendo lista de archivos MAESTRO con estatus XML SERIALIZADO.");                
                List<OperacionL3DTO> operaciones = await _repositorioTomi.RecuperarOperacionLevel3Async(L3);
                if (!operaciones.Any())
                {
                    LogInfo("GeneraArchivosLevel3|No se encontraron archivos para generar.");
                }
                else
                {
                    LogInfo($"EncriptarArchivos|Comienza la generacion de archivos de salida, se detectaron {operaciones.Count.ToString()} archivos para generar.");

                    foreach (OperacionL3DTO Archivo in operaciones)
                    {

                        try
                        {
                            string archivoNew = string.Empty;
                            giCountErrorGral = 0;
                            var L3d = new ActualizaL3Archivo
                            {
                                Tipo = 2,
                                Folio = Archivo.Folio
                            };
                            ErrorGral = "";

                            L3.Folio = Archivo.Folio;

                            LogInfo("INFO|GeneraArchivosLevel3|Obteniendo Detalles del Archivo " + Archivo.Nombre + "|Estatus:" + Archivo.estatus);

                            //' 2.- De cada archivo Maestro, obtenemos los pagos aceptados y rechazados de la tabla de Detalles.
                            LogInfo("INFO|GeneraArchivosLevel3|Obtener pagos aceptados, Folio = " + Archivo.Folio.ToString());
                            List<OperacionL3DetalleDTO> operacionesDet = await _repositorioTomi.RecuperarOperacionLevel3DetalleAsync(L3d);

                            LogInfo("INFO|GeneraArchivosLevel3|Obteniendo totales de operaciones del archivo " + Archivo.Nombre);
                            foreach (OperacionL3DetalleDTO DetArchGPH in operacionesDet)
                            {
                                var estatusOperacion = DetArchGPH?.Estatus ?? string.Empty;
                                var errorOperacion = DetArchGPH?.Error ?? string.Empty;

                                if (string.Equals(estatusOperacion, ConstantesServicio.EstadoRechazada, StringComparison.OrdinalIgnoreCase))
                                {
                                    Archivo.Num_operaciones_rechazadas += 1;

                                    if (string.IsNullOrWhiteSpace(ErrorGral) && (errorOperacion.Contains(ConstantesServicio.CodigoErrorGeneralA) ||
                                        errorOperacion.Contains(ConstantesServicio.CodigoErrorGeneralB) ||
                                        errorOperacion.Contains(ConstantesServicio.CodigoErrorGeneralC)))
                                    {
                                        ErrorGral = errorOperacion;
                                    }

                                }
                                else
                                    if (string.Equals(estatusOperacion, ConstantesServicio.EstadoAceptada, StringComparison.OrdinalIgnoreCase) ||
                                        string.Equals(estatusOperacion, ConstantesServicio.EstadoProcesada, StringComparison.OrdinalIgnoreCase))
                                    {
                                        Archivo.Num_operaciones_aceptadas += 1;
                                    }

                            }

                            //'Generamos nombre acorde a las especificaciones XXXX_XXXX_BTMM_ERROR.txt
                            string nombreArchivoL3 = Archivo?.Nombre ?? string.Empty;
                            string[] NombreArc = nombreArchivoL3.Split('_');
                            if (NombreArc.Length > 1)
                            {
                                archivoNew = NombreArc[0] + "_" + NombreArc[1] + "_BTMM_" + Archivo.FOLIO_FILE;
                            }
                            else
                            {
                                archivoNew = nombreArchivoL3 + "_BTMM_" + Archivo.FOLIO_FILE;
                            }

                            var settings = new XmlWriterSettings
                            {
                                Indent = true,
                                IndentChars = " ",
                                Encoding = new UTF8Encoding(false),
                                OmitXmlDeclaration = false
                            };

                            gsRutaFilesTransmite = _configuracion.ParametrosSistema[ConstantesServicio.RUTA_ARCHIVOS_GPH_TRANSMITE];
                            using (XmlWriter writer = XmlWriter.Create(Path.Combine(gsRutaFilesTransmite, archivoNew, ".xml"), settings))
                            {
                                writer.WriteStartElement("Document", "urn:iso:std:iso:20022:tech:xsd:pain.002.001.03");
                                writer.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");

                                writer.WriteStartElement("CstmrPmtStsRpt");
                                var FechaSistema = Conversion.StringToDate(_configuracion.FechaSistema);
                                writer.WriteStartElement("GrpHdr"); // BEGIN - GrpHdr
                                writer.WriteElementString("MsgId", $"{FechaSistema.ToString("yyyyMMdd")}" + Archivo.FOLIO_FILE);
                                writer.WriteElementString("CreDtTm", $"{FechaSistema.ToString("yyyy-MM-dd")}T{DateTime.Now.ToString("HH:mm:ss")}");
                                writer.WriteEndElement();          // ' END - GrpHdr
                                writer.WriteStartElement("OrgnlGrpInfAndSts"); //' Begin - OrgnlGrpInfAndSts
                                writer.WriteElementString("OrgnlMsgId", Archivo.Msg_id);
                                writer.WriteElementString("OrgnlMsgNmId", "pain.001.001.03");

                                //--
                                //Parsear el XML que viene en el string GROUP_HEADER
                                string creDtTmValue = null;
                                string nbOfTxsValue = null;
                                string ctrlSumValue = null;
                                string InitgPtyNmValue = null;

                                //Nodos opcionesles
                                if (!string.IsNullOrEmpty(Archivo.GROUP_HEADER))
                                {
                                    try
                                    {
                                        XmlDocument docGroupHeader = new XmlDocument();
                                        docGroupHeader.LoadXml(Archivo.GROUP_HEADER);
                                        var navGroupHeader = docGroupHeader.CreateNavigator();
                                        creDtTmValue = navGroupHeader.SelectSingleNode("//GroupHeader/CreDtTm")?.Value;
                                        nbOfTxsValue = navGroupHeader.SelectSingleNode("//GroupHeader/NbOfTxs")?.Value;
                                        ctrlSumValue = navGroupHeader.SelectSingleNode("//GroupHeader/CtrlSum")?.Value;
                                        InitgPtyNmValue = navGroupHeader.SelectSingleNode("//GroupHeader/InitgPty/Nm")?.Value;
                                    }
                                    catch (Exception ex)
                                    {
                                        LogError($"|GeneraArchivosLevel3|No se pudo parsear GROUP_HEADER: {ex.Message}");
                                    }
                                }

                                if (!string.IsNullOrEmpty(creDtTmValue))
                                {
                                    writer.WriteElementString("OrgnlCreDtTm", creDtTmValue);
                                }
                                if (!string.IsNullOrEmpty(nbOfTxsValue))
                                {
                                    writer.WriteElementString("OrgnlNbOfTxs", nbOfTxsValue);
                                }
                                if (!string.IsNullOrEmpty(ctrlSumValue))
                                {
                                    writer.WriteElementString("OrgnlCtrlSum", ctrlSumValue);
                                }

                                bool tieneErrorEspecifico = ErrorGral.Contains("1Z35201") ||
                                                            ErrorGral.Contains("1T10070") ||
                                                            ErrorGral.Contains("1T10057");

                                string grpSts = tieneErrorEspecifico ? "RJCT" :
                                    (Archivo.Total_operaciones == Archivo.Num_operaciones_aceptadas) ? "ACCP" :
                                    (Archivo.Total_operaciones == Archivo.Num_operaciones_rechazadas) ? "RJCT" : "PART";

                                writer.WriteElementString("GrpSts", grpSts);

                                LogInfo("INFO|StsRsnInf|Begin|InitgPty");
                                writer.WriteStartElement("StsRsnInf");


                                //----
                                string groupHeader = Archivo?.GROUP_HEADER;
                                if (string.IsNullOrWhiteSpace(groupHeader))
                                {
                                    throw new Exception("GROUP_HEADER vacío o nulo para generar XML de salida.");
                                }

                                var nav = new XPathDocument(new StringReader(groupHeader)).CreateNavigator();
                                string xmlValor = string.Empty;

                                xmlValor = ProcesarNodo(nav, writer, "//GroupHeader/CreDtTm", "OrgnlCreDtTm");
                                xmlValor = ProcesarNodo(nav, writer, "//GroupHeader/NbOfTxs", "OrgnlNbOfTxs");
                                xmlValor = ProcesarNodo(nav, writer, "//GroupHeader/CtrlSum", "OrgnlCtrlSum");

                                if (ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralA) ||
                                    ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralB) ||
                                    ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralC))
                                {
                                    writer.WriteElementString("GrpSts", ConstantesServicio.EstadoGrupoRjct);
                                }
                                else
                                {
                                    writer.WriteElementString(
                                        "GrpSts",
                                        (Archivo.Total_operaciones == Archivo.Num_operaciones_aceptadas)
                                            ? ConstantesServicio.EstadoGrupoAccp
                                            : (Archivo.Total_operaciones == Archivo.Num_operaciones_rechazadas)
                                                ? ConstantesServicio.EstadoGrupoRjct
                                                : ConstantesServicio.EstadoGrupoPart
                                    );
                                }

                                writer.WriteStartElement("StsRsnInf");

                                LogInfo("INFO|StsRsnInf|Begin|InitgPty");

                                xmlValor = ProcesarNodo(nav, writer, "//GroupHeader/InitgPty/Nm", "");
                                if (xmlValor.Length > 0)
                                {
                                    writer.WriteStartElement("Orgtr");  // ' Begin - Orgtr
                                    writer.WriteElementString("Nm", xmlValor);
                                    writer.WriteEndElement();  //' End - Orgtr
                                }
                                LogInfo("INFO|StsRsnInf|End|InitgPty");


                                writer.WriteStartElement("Rsn");

                                if (ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralA) ||
                                    ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralB) ||
                                    ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralC))
                                {
                                    writer.WriteElementString("Prtry", ConstantesServicio.CodigoPrtryError);
                                }
                                else
                                {
                                    writer.WriteElementString(
                                        "Prtry",
                                        (Archivo.Total_operaciones == Archivo.Num_operaciones_aceptadas)
                                            ? ConstantesServicio.CodigoPrtryOk
                                            : (Archivo.Total_operaciones == Archivo.Num_operaciones_rechazadas)
                                                ? ConstantesServicio.CodigoPrtryError
                                                : ConstantesServicio.CodigoPrtryParcial
                                    );
                                }


                                writer.WriteEndElement();           //' END - Rsn
                                writer.WriteEndElement();           //' END - StsRsnInf
                                writer.WriteEndElement();           //' END - OrgnlGrpInfAndSts                        

                                //' /*********** Crear por cada Transaccion **************/   
                                LogInfo("INFO|GeneraArchivosLevel3|Obtener detalle por operacion");
                                foreach (OperacionL3DetalleDTO DetArchGPHXml in operacionesDet)
                                {
                                    if (ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralALegacy) ||
                                        ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralBLegacy) ||
                                        ErrorGral.Contains(ConstantesServicio.CodigoErrorGeneralCLegacy))
                                    {
                                        giCountErrorGral = giCountErrorGral + 1;
                                    }

                                    await CreateDetailFileStatus(writer, DetArchGPHXml, giCountErrorGral);
                                }
                                writer.WriteEndDocument();
                                writer.Close();
                            }

                            var patchFileNew = Path.Combine(gsRutaFilesTransmite, archivoNew, ".xml");
    
                            var doc = XDocument.Load(Path.Combine(patchFileNew));

                            doc.Descendants()
                                .Where(e =>
                                    string.IsNullOrWhiteSpace(e.Value) &&
                                    !e.HasElements &&
                                    !e.HasAttributes)
                                .Remove();

                            doc.Save(patchFileNew);

                            LogInfo("INFO|GeneraArchivosLevel3|Se termina la generacion de archivo XML de salida: " + archivoNew + ".xml");

                            L3.Tipo = 1;
                            L3.ArchivoNew = archivoNew;
                            L3.Folio = Archivo.Folio;
                            L3.Num_operaciones_aceptadas = Archivo.Num_operaciones_aceptadas;
                            L3.Num_operaciones_rechazadas = Archivo.Num_operaciones_rechazadas;
                            await _repositorioTomi.MarcarOperacionProcesadaLevel3Async(L3);

                        }
                        catch (Exception ex)
                        {
                            LogError("ERROR|GeneraArchivosLevel3|Error en la generacion de los archivos de salida. - " + ex.ToString());
                            L3.Tipo = 1;
                            L3.ArchivoNew = Archivo.Nombre;
                            L3.Folio = Archivo.Folio;
                            L3.Num_operaciones_aceptadas = Archivo.Num_operaciones_aceptadas;
                            L3.Num_operaciones_rechazadas = Archivo.Num_operaciones_rechazadas;
                            await _repositorioTomi.MarcarOperacionProcesadaLevel3Async(L3);
                        }
                    }

                }


            }
            catch (Exception ex)
            {
                LogError("ERROR|GeneraArchivosLevel1|Se presenta exception: " + ex.ToString());                
                L3.Tipo = 2;
                await _repositorioTomi.MarcarOperacionProcesadaLevel3Async(L3);                
            }
        }
        #endregion

        #region Encriptacion
        /// <summary>
        /// Método principal para encriptar archivos.
        /// </summary>
        /// <returns></returns>
        private async Task EncriptarArchivos()
        {
            try
            {
                string gsRutaFilesTransmite = string.Empty;
                string gsRutaKeys = string.Empty;
                string gsLlavePublica = string.Empty;
                string gsRutaFilesEncriptados = string.Empty;
                string gsRutaFilesProcesados = string.Empty;                
                LogInfo("INFO|EncriptarArchivos|Obteniendo lista de archivos pendientes de encriptar.");                
                List<OperacionEncryptaDTO> operaciones = await _repositorioTomi.RecuperarOperacionEncryptaAsync();
                if (!operaciones.Any())
                {
                    LogInfo("INFO|EncriptarArchivos|No se encontraron archivos pgp para encriptar.");
                    
                }
                else
                {
                    LogInfo("INFO|EncriptarArchivos|Comienza la encripcion, se detectaron " + operaciones.Count + " archivos de salida.");

                    try
                    {
                        gsRutaFilesTransmite = _configuracion.ParametrosSistema[ConstantesServicio.RUTA_ARCHIVOS_GPH_TRANSMITE];
                        gsRutaKeys = _configuracion.ParametrosSistema[ConstantesServicio.RUTA_LLAVES_GPH];

                        //SERVICIO.CONFIG
                        gsLlavePublica = LecturaConfguracionGeneral.ObtenerValorGeneral("LLAVE_PUBLICA_PGP");

                        gsRutaFilesEncriptados = _configuracion.ParametrosSistema[ConstantesServicio.RUTA_ARCHIVOS_GPH_ENCRIPTADOS];
                        gsRutaFilesProcesados = _configuracion.ParametrosSistema[ConstantesServicio.RUTA_ARCHIVOS_GPH_PROCESADOS];

                        foreach (OperacionEncryptaDTO elementEncry in operaciones)
                        {
                            string strEx = string.Empty;
                            try
                            {

                                string inputFileLocation = Path.Combine(gsRutaFilesTransmite, elementEncry.Nombre_archivo_salida);
                                string publicKeyLocation = Path.Combine(gsRutaKeys, gsLlavePublica);
                                string outputFile = Path.Combine(gsRutaFilesEncriptados, elementEncry.Nombre_archivo_salida.Replace(".xml", "").Replace(".txt", ""));

                                LogInfo("INFO|EncriptarArchivos|Encriptando ArchivoEntrada=" + inputFileLocation + " - ArchivoSalida=" + outputFile);

                                await EncryptFile(inputFileLocation, publicKeyLocation, outputFile);
                                LogInfo("INFO|EncriptarArchivos|Inicio - moviendo archivo a: " + gsRutaFilesTransmite + elementEncry.Nombre_archivo_salida);

                                FileInfo fInfo = new FileInfo(inputFileLocation);

                                MoveFile(fInfo, Path.Combine(gsRutaFilesProcesados, elementEncry.Nombre_archivo_salida));
                                LogInfo("INFO|EncriptarArchivos|Fin - moviendo archivo a: " + gsRutaFilesTransmite + elementEncry.Nombre_archivo_salida);

                            }
                            catch (Exception ex)
                            {
                                strEx = ex.Message;
                            }

                            var Encry = new ActualizaEncrypta
                            {
                                Tipo = 0,
                                Folio = elementEncry.Folio,
                                ErroDesc = strEx
                            };

                            if (strEx != "")
                            {
                                LogInfo("INFO|EncriptarArchivos|Error en la encripcion del archivo pgp " + elementEncry.Nombre_archivo_salida + " - " + strEx);

                                Encry.Tipo = 1;
                                Encry.ErroDesc = strEx;
                                await _repositorioTomi.MarcarOperacionProcesadaEncryptaAsync(Encry);
                            }
                            else
                            {
                                LogInfo("INFO|EncriptarArchivos|Se termina la encripcion deL archivo pgp " + elementEncry.Nombre_archivo_salida);

                                Encry.Tipo = 2;
                                await _repositorioTomi.MarcarOperacionProcesadaEncryptaAsync(Encry);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError($"ERROR|EncriptarArchivos|Error en la encripcion de los archivos pgp. - {ex}");
                    }
                }

          
            }
            catch (Exception ex)
            {             
                LogError($"ERROR|EncriptarArchivos|Se presenta exception: {ex}");
            }            
        }

        #endregion

        #region Envio SFTP
        /// <summary>
        /// Método principal para envio de archivos SFTP.
        /// </summary>
        /// <returns></returns>
        private async Task EnviarArchivosSFTP()
        {
            try
            {

                string gsRemoteHostNameSFTP = string.Empty;
                string gsRemoteUsrSFTP = string.Empty;

                Int32 gsSshPortNumberHostSFTP = 0;
                string gsSshHostKeySFTP = string.Empty;
                string gsSshPrivateKeyPathSFTP = string.Empty;

                string gsRutaFilesEncriptados = string.Empty;
                string gsRutaFilesRemoteUpload = string.Empty;
                string gsRutaFilesProcesados = string.Empty;
                

                LogInfo("INFO|SendFiles|Obteniendo lista de archivos encriptados para su envio.");                
                List<OperacionEnviaDTO> operaciones = await _repositorioTomi.RecuperarOperacionEnviarAsync();

                if (!operaciones.Any())
                {
                    LogInfo("INFO|EncriptarArchivos|No se encontraron archivos pgp para encriptar.");
                    
                }
                else
                {

                    gsRutaFilesEncriptados = _configuracion.ParametrosSistema[ConstantesServicio.RUTA_ARCHIVOS_GPH_ENCRIPTADOS];
                    gsRutaFilesRemoteUpload = _configuracion.ParametrosSistema[ConstantesServicio.REMOTE_PATH_SFTP_GPH_UPLOAD];

                    gsRemoteHostNameSFTP = _configuracion.ParametrosSistema[ConstantesServicio.REMOTE_HOST_NAME_GPH];
                    gsRemoteUsrSFTP = _configuracion.ParametrosSistema[ConstantesServicio.USER_SFTP_GPH];

                    gsSshPortNumberHostSFTP = ConversionesComunes.ParseEnteroRequerido(_configuracion.ParametrosSistema[ConstantesServicio.SSH_HOST_PORT_NUMBER_GPH], ConstantesServicio.SSH_HOST_PORT_NUMBER_GPH);
                    gsSshHostKeySFTP = _configuracion.ParametrosSistema[ConstantesServicio.SSH_HOST_KEY_GPH];


                    gsSshPrivateKeyPathSFTP = _configuracion.ParametrosSistema[ConstantesServicio.SSH_PRIVATE_KEY_PATH_GPH];
                    gsRutaFilesProcesados = _configuracion.ParametrosSistema[ConstantesServicio.RUTA_ARCHIVOS_GPH_PROCESADOS];

                    LogInfo("INFO|SendFiles|Lista de archivos con estatus ENCRIPTADO");

                    SessionOptions sessionOptions = new SessionOptions
                    {
                        Protocol = Protocol.Sftp,
                        HostName = gsRemoteHostNameSFTP,
                        UserName = gsRemoteUsrSFTP,
                        PortNumber = gsSshPortNumberHostSFTP,
                        SshHostKeyFingerprint = gsSshHostKeySFTP,
                        SshPrivateKeyPath = gsSshPrivateKeyPathSFTP
                    };

                    LogInfo("INFO|SendFiles|Setup session options, ready");

                    using (Session session = new Session())
                    {
                        // Connect
                        session.Open(sessionOptions);
                        LogInfo("INFO|SendFiles|Connection, ready");

                        // Upload files
                        TransferOptions transferOptions = new TransferOptions
                        {
                            TransferMode = TransferMode.Automatic
                        };
                        transferOptions.AddRawSettings("IgnorePermErrors", "1");

                        LogInfo("INFO|SendFiles|Upload begin, ready");
                        LogInfo("INFO|SendFiles|gsRutaFilesTransmite: " + gsRutaFilesEncriptados + "*.*");
                        LogInfo("INFO|SendFiles|gsRutaFilesRemoteUpload: " + gsRutaFilesRemoteUpload);



                        TransferOperationResult transferResult = session.PutFiles(
                            gsRutaFilesEncriptados + "*.*",
                            gsRutaFilesRemoteUpload,
                            false,
                            transferOptions);

                        LogInfo("INFO|SendFiles|Upload end, ready, Numero de Transfer:" +
                                  transferResult.Transfers.Count.ToString());

                        transferResult.Check();
                        LogInfo("INFO|SendFiles|Check end");
                        foreach (WinSCP.TransferEventArgs transfer in transferResult.Transfers)
                        {
                            if (transfer.Error != null)
                            {
                                LogError($"ERROR|SendFiles|Upload of {transfer.FileName}, error: {transfer.Error} ");
                                continue;
                            }
                            LogInfo("INFO|SendFiles|Upload of " + transfer.FileName + ", succeeded");


                            OperacionEnviaDTO selectedValue = operaciones.Find(
                                p => p.Nombre_archivo_salida.Replace(".xml", "").Replace(".txt", "") ==
                                     transfer.FileName.Replace(gsRutaFilesEncriptados, "")
                            );

                            LogInfo("INFO|SendFiles|find element ");

                            if (selectedValue != null)
                            {
                                LogInfo("INFO|SendFiles|Upload of " + selectedValue.Nombre_archivo_salida + ", succeeded");

                                FileInfo fInfo = new FileInfo(transfer.FileName);

                                MoveFile(fInfo,
                                    gsRutaFilesProcesados +
                                    transfer.FileName.Replace(gsRutaFilesEncriptados, "")
                                );

                                LogInfo("INFO|SendFiles|Move " + selectedValue.Nombre_archivo_salida + ", succeeded");
                                await _repositorioTomi.MarcarOperacionProcesadaEnviarAsync(selectedValue.Folio);
                            }
                            else
                            {
                                LogInfo("INFO|SendFiles|No se encontro el archivo: " + transfer.FileName);
                            }
                        }
                    }

                    LogInfo("INFO|SendFiles|* * * * * *  Envio de Archivos PGP por SFTP Completa   * * * * * * * * * * *");
                    LogInfo("INFO|SendFiles|=========================================================================");

                }
            }
            catch (Exception ex)
            {                
                LogError($"ERROR|EncriptarArchivos|Se presenta exception: {ex}");
            }            
        }
        #endregion


        #region Generales
        /// <summary>
        /// Método para procesamiento de nodo XML.
        /// </summary>
        /// <param name="nav"></param>
        /// <param name="writer"></param>
        /// <param name="nodoObtener"></param>
        /// <param name="nodoPonerlo"></param>
        /// <returns></returns>
        private string ProcesarNodo(XPathNavigator nav, XmlWriter writer, string nodoObtener, string nodoPonerlo)
        {            
            var valor = nav.SelectSingleNode(nodoObtener)?.Value;
            if (!string.IsNullOrWhiteSpace(valor) && !string.IsNullOrWhiteSpace(nodoPonerlo)) { writer.WriteElementString(nodoPonerlo, valor); }
            return valor;
        }

        /// <summary>
        /// Método para modificar el detalle de estatus de cada archivo.
        /// </summary>
        /// <param name="FileXMLStatus"></param>
        /// <param name="DetailFileGPH"></param>
        /// <returns></returns>
        private async Task<bool> CreateDetailFileStatus(XmlWriter FileXMLStatus, OperacionL3DetalleDTO DetailFileGPH, int giCountErrorGral)
        {            
            bool PagoAceptado;
            var L3FileSatatus = new DetailFileStatus
            {
                Folio = DetailFileGPH.Folio,
                FolioOperacion = DetailFileGPH.Folio_operacion,
                Pmt_Inf_Id = DetailFileGPH.Pmt_Inf_Id,
                EndToEndId = DetailFileGPH.EndToEndId,
                WorkTable = "",
            };

            string lsErrorCodeGral= string.Empty;
            string estatusDetalle = DetailFileGPH?.Estatus ?? string.Empty;
            string errorDetalle = DetailFileGPH?.Error ?? string.Empty;

            if (string.Equals(estatusDetalle, ConstantesServicio.EstadoProcesada, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(estatusDetalle, ConstantesServicio.EstadoAceptada, StringComparison.OrdinalIgnoreCase))
            {
                PagoAceptado = true;
            }
            else
            {
                PagoAceptado = false;
            }

            switch (DetailFileGPH.Tipo_operacion)
            {
                case ConstantesServicio.TipoOperacionFxdGph:
                    L3FileSatatus.WorkTable = ConstantesServicio.TablaFxdGph;
                    break;

                case ConstantesServicio.TipoOperacionFxtGph:
                    L3FileSatatus.WorkTable = ConstantesServicio.TablaFxtGph;
                    break;

                case ConstantesServicio.TipoOperacionTefGph:
                    L3FileSatatus.WorkTable = ConstantesServicio.TablaTefGph;
                    break;

                case ConstantesServicio.TipoOperacionSpidGph:
                    L3FileSatatus.WorkTable = ConstantesServicio.TablaSpidGph;
                    break;

                case ConstantesServicio.TipoOperacionFxiGph:
                    L3FileSatatus.WorkTable = ConstantesServicio.TablaFxiGph;
                    break;


                default:
                    L3FileSatatus.FolioOperacion = 0;
                    L3FileSatatus.EndToEndId = "";
                    L3FileSatatus.WorkTable = ConstantesServicio.TablaNoAplica;

                    break;

            }

            if (L3FileSatatus.WorkTable != ConstantesServicio.TablaNoAplica)
            {
                LogInfo($"INFO|Obtener info de BD de la tabla: '{L3FileSatatus.WorkTable}'");
            }
            else
            {
                LogInfo($"INFO|Obtener info de BD de la tabla: '{ConstantesServicio.TablaDetalleArchivo}'");
            }

            List<OperacionDetailFileStatusDTO> operacionesFileStatus = await _repositorioTomi.RecuperarOperacionDetailFileStatusAsync(L3FileSatatus);
            LogInfo("INFO|Obtener info de BD Salio BIEN");

            if (!operacionesFileStatus.Any())
            {
                LogInfo("INFO|CrearXmlDetalleFXD|No se encontro información para el Pago: " + L3FileSatatus.Pmt_Inf_Id + ", con numero de folio de archivo: " + L3FileSatatus.Folio);
                return true;
            }


            foreach (OperacionDetailFileStatusDTO lrsDatos in operacionesFileStatus)
            {
                string xmlValor = string.Empty;
                string xmlValorT = xmlValor;

                XmlDocument doc;
                string error;

                if (!XmlHelper.TryParseXmlSeguro(lrsDatos?.NF_ARCHIVO_XML, out doc, out error))
                {
                    LogError("ERROR|XML|XML inválido: " + error);
                    return false;
                }

                var nav = doc.CreateNavigator();


                //'---------------------------------    OrgnlPmtInfAndSts  ------------------------------
                LogInfo("INFO|Begin|OrgnlPmtInfAndSts");
                FileXMLStatus.WriteStartElement("OrgnlPmtInfAndSts");//  ' Begin - OrgnlPmtInfAndSts

                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/PmtInfId", "OrgnlPmtInfId");
                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/NbOfTxs", "OrgnlNbOfTxs");
                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/CtrlSum", "OrgnlCtrlSum");

                LogInfo("INFO|End|OrgnlPmtInfAndSts");

                //'----------------------------------    TxInfAndSts  ------------------------------
                LogInfo("INFO|Begin|TxInfAndSts");
                FileXMLStatus.WriteStartElement("TxInfAndSts");//  ' Begin - TxInfAndSts
                FileXMLStatus.WriteElementString("StsId", lrsDatos?.NF_FOLIO_UNICO ?? string.Empty);


                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/CdtTrfTxInf[1]/PmtId/InstrId", "OrgnlInstrId");
                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/CdtTrfTxInf[1]/PmtId/EndToEndId", "OrgnlEndToEndId");

                FileXMLStatus.WriteElementString("TxSts", PagoAceptado ? ConstantesServicio.EstadoGrupoAccp : ConstantesServicio.EstadoGrupoRjct);
                LogInfo("INFO|End|TxInfAndSts");
                //'----------------------------------------------------------    StsRsnInf  ------------------------------

                LogInfo("INFO|Begin|StsRsnInf");

                FileXMLStatus.WriteStartElement("StsRsnInf");   // ' Begin - StsRsnInf
                FileXMLStatus.WriteStartElement("Rsn"); //// ' Begin - Rsn

                if (errorDetalle.Contains(ConstantesServicio.CodigoErrorGeneralA) ||
                    errorDetalle.Contains(ConstantesServicio.CodigoErrorGeneralB) ||
                    errorDetalle.Contains(ConstantesServicio.CodigoErrorGeneralC))
                {
                    if (errorDetalle.Contains(ConstantesServicio.CodigoErrorGeneralA))
                    {
                        lsErrorCodeGral += ConstantesServicio.CodigoErrorGeneralA + "|";
                    }

                    if (errorDetalle.Contains(ConstantesServicio.CodigoErrorGeneralB))
                    {
                        lsErrorCodeGral += ConstantesServicio.CodigoErrorGeneralB + "|";
                    }

                    if (errorDetalle.Contains(ConstantesServicio.CodigoErrorGeneralC))
                    {
                        lsErrorCodeGral += ConstantesServicio.CodigoErrorGeneralC + "|";
                    }

                    // equivalente de IIf en C# (operador ternario)
                    FileXMLStatus.WriteElementString(
                        "Prtry",
                        giCountErrorGral > 1
                            ? ConstantesServicio.CodigoTxErrorMultiple
                            : lsErrorCodeGral.Substring(0, lsErrorCodeGral.Length - 1)
                    );
                }
                else
                {
                    FileXMLStatus.WriteElementString(
                        "Prtry",
                        PagoAceptado
                            ? ConstantesServicio.CodigoTxOk
                            : errorDetalle
                    );
                }

                FileXMLStatus.WriteEndElement();        // ' End - Rsn
                FileXMLStatus.WriteEndElement();        // ' End - StsRsnInf  
                LogInfo("INFO|End|StsRsnInf");

                //'----------------------------------------------------------    OrgnlTxRef  ------------------------------
                FileXMLStatus.WriteStartElement("OrgnlTxRef");//// ' Begin - OrgnlTxRef

                //'----------------------------------------    Amt  ------------------------------
                LogInfo("INFO|Begin|Amt");
                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/CdtTrfTxInf[1]/Amt/InstdAmt", "");
                if (xmlValor.Length > 0)
                {

                    FileXMLStatus.WriteStartElement("Amt"); // ' Begin - Amt
                    FileXMLStatus.WriteStartElement("InstdAmt");
                    xmlValorT = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/CdtTrfTxInf[1]/Amt/InstdAmt/@Ccy", "");
                    FileXMLStatus.WriteAttributeString("Ccy", xmlValorT); //'T24_DEBIT_CURRENCY // payment.CdtTrfTxInf(0).Amt.InstdAmt.Ccy 
                    FileXMLStatus.WriteString(xmlValor);    //'NF_MONTO_ENVIAR //payment.CdtTrfTxInf(0).Amt.InstdAmt.Value
                    FileXMLStatus.WriteEndElement();        // ' End - InstdAmt                            
                    FileXMLStatus.WriteEndElement();        // ' End - Amt  
                }

                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/ReqdExctnDt", "ReqdExctnDt");
                LogInfo("INFO|End|Amt");
                //'----------------------------------------    PmtTpInf  ------------------------------
                LogInfo("INFO|Begin|PmtTpInf");

                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/PmtTpInf/LclInstrm", "");
                xmlValorT = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/CdtTrfTxInf[1]/PmtTpInf/CtgyPurp", "");

                if (!string.IsNullOrWhiteSpace(xmlValor) || !string.IsNullOrWhiteSpace(xmlValorT))
                {
                    FileXMLStatus.WriteStartElement("PmtTpInf"); //// ' Begin - PmtTpInf

                    if (!string.IsNullOrWhiteSpace(xmlValor))
                    {
                        FileXMLStatus.WriteStartElement("LclInstrm"); //// ' Begin - LclInstrm

                        xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/PmtTpInf/LclInstrm/Cd", "Cd");
                        xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/PmtTpInf/LclInstrm/Prtry", "Prtry");

                        FileXMLStatus.WriteEndElement();  // ' End - LclInstrm


                        var nodo22 = nav.SelectSingleNode("//PaymentInstruction/PmtTpInf/LclInstrm");
                        if (nodo22 != null)
                        {
                            nodo22.DeleteSelf();
                        }

                    }
                    
                    if (!string.IsNullOrWhiteSpace(xmlValorT))
                    {
                        FileXMLStatus.WriteStartElement("CtgyPurp"); //// ' Begin - CtgyPurp

                        xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/CdtTrfTxInf[1]/PmtTpInf/CtgyPurp/Cd", "Cd");
                        xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/CdtTrfTxInf[1]/PmtTpInf/CtgyPurp/Prtry", "Prtry");

                        FileXMLStatus.WriteEndElement();  // ' End - CtgyPurp

                        var nodo2 = nav.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/PmtTpInf/CtgyPurp");
                        if (nodo2 != null)
                        {
                            nodo2.DeleteSelf();
                        }
                    }

                    FileXMLStatus.WriteEndElement();               // ' End - PmtTpInf

                }
                LogInfo("INFO|End|PmtTpInf");

                xmlValor = string.Empty;
                xmlValorT = string.Empty;

                // '----------------------------------------    PmtMtd  ------------------------------
                xmlValor = ProcesarNodo(nav, FileXMLStatus, "//PaymentInstruction/PmtMtd", "PmtMtd");//'Oper/PmtMtd

                // '----------------------------------------    RmtInf  ------------------------------
                LogInfo("INFO|Begin|RmtInf");
                //rmcb se trae bloqueo                  
                var nodo = nav.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/RmtInf");
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/RmtInf");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|RmtInf");

                //'---------------------------------------- -------------------------   UltmtDbtr  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|UltmtDbtr");
                //rmcb se trae bloque 
                nodo = nav.SelectSingleNode("//PaymentInstruction/UltmtDbtr");// //PaymentInstruction/UltmtDbtr/isNotNullPropertiesThen
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/UltmtDbtr");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|UltmtDbtr");

                //'---------------------------------------- -------------------------   Dbtr  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|Dbtr");
                //rmcb se trae bloque 
                nodo = nav.SelectSingleNode("//PaymentInstruction/Dbtr");//PaymentInstruction/Dbtr/isNotNullPropertiesThen
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/Dbtr");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|Dbtr");
                ///'---------------------------------------- -------------------------   DbtrAcct  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|DbtrAcct");
                nodo = nav.SelectSingleNode("//PaymentInstruction/DbtrAcct");//PaymentInstruction/DbtrAcct/isNotNullPropertiesThen
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/DbtrAcct");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|DbtrAcct");
                //'---------------------------------------- -------------------------   DbtrAgt  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|DbtrAgt");
                nodo = nav.SelectSingleNode("//PaymentInstruction/DbtrAgt");//PaymentInstruction/DbtrAgt/isNotNullPropertiesThen
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/DbtrAgt");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|DbtrAgt");
                //'---------------------------------------- -------------------------   DbtrAgtAcct  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|DbtrAgtAcct");
                nodo = nav.SelectSingleNode("//PaymentInstruction/DbtrAgtAcct");//PaymentInstruction/DbtrAgtAcct/isNotNullPropertiesThen
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/DbtrAgtAcct");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|DbtrAgtAcct");
                //'---------------------------------------- -------------------------   CdtrAgt  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|CdtrAgt");
                nodo = nav.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/CdtrAgt");//PaymentInstruction/CdtTrfTxInf[1]/CdtrAgt/isNotNullPropertiesThen
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/CdtrAgt");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|CdtrAgt");
                //'---------------------------------------- -------------------------   CdtrAgtAcct  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|CdtrAgtAcct");
                nodo = nav.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/CdtrAgtAcct");//PaymentInstruction/CdtTrfTxInf[1]/CdtrAgtAcct/isNotNullPropertiesThen

                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/CdtrAgtAcct");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|CdtrAgtAcct");
                //'---------------------------------------- -------------------------   Cdtr  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|Cdtr");
                nodo = nav.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/Cdtr");//PaymentInstruction/CdtTrfTxInf[1]/Cdtr/isNotNullPropertiesThen
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/Cdtr");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }

                LogInfo("INFO|End|Cdtr");
                //'---------------------------------------- -------------------------   CdtrAcct  -------------------------------------------------------------------------
                LogInfo("INFO|Begin|CdtrAcct");
                nodo = nav.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/CdtrAcct");//PaymentInstruction/CdtTrfTxInf[1]/CdtrAcct/isNotNullPropertiesThen
                if (nodo != null)
                {
                    XmlNode nodo2 = doc.SelectSingleNode("//PaymentInstruction/CdtTrfTxInf[1]/CdtrAcct");
                    // limpiar nodos vacíos
                    RemoveEmptyNodes(nodo2);
                    FileXMLStatus.WriteRaw(nodo2.OuterXml);
                }
                LogInfo("INFO|End|CdtrAcct");


                FileXMLStatus.WriteEndElement();    // ' End - OrgnlTxRef
                FileXMLStatus.WriteEndElement();      // ' End - TxInfAndSts						
                FileXMLStatus.WriteEndElement();   // ' End - OrgnlPmtInfAndSts						

            }
            return true;
        }

        /// <summary>
        /// Método para remover nodos de el archivo XML.
        /// </summary>
        /// <param name="node"></param>
        void RemoveEmptyNodes(XmlNode node)
        {
            if (node == null) return;

            // 1. Limpiar atributos vacíos
            if (node.Attributes != null)
            {
                for (int i = node.Attributes.Count - 1; i >= 0; i--)
                {
                    var attr = node.Attributes[i];

                    if (string.IsNullOrWhiteSpace(attr.Value))
                    {
                        node.Attributes.Remove(attr);
                    }
                }
            }


            // recorrer hijos de atrás hacia adelante (importante para eliminar)
            for (int i = node.ChildNodes.Count - 1; i >= 0; i--)
            {
                XmlNode child = node.ChildNodes[i];

                // llamada recursiva
                RemoveEmptyNodes(child);

                // condiciones para eliminar nodo vacío
                bool isEmptyElement =
                    child.NodeType == XmlNodeType.Element &&
                    string.IsNullOrWhiteSpace(child.InnerText) &&
                    child.ChildNodes.Count == 0 &&
                    (child.Attributes == null || child.Attributes.Count == 0);

                if (isEmptyElement)
                {
                    node.RemoveChild(child);
                }
            }
        }

        /// <summary>
        /// Método para encriptar el archivo.
        /// </summary>
        /// <param name="inputFile"></param>
        /// <param name="publicKeyFile"></param>
        /// <param name="outputFile"></param>
        /// <returns></returns>
        private async Task EncryptFile(string inputFile, string publicKeyFile, string outputFile)
        {
            using (var publicKeyStream = File.OpenRead(publicKeyFile))
            {
                using (var input = File.OpenRead(inputFile))
                using (var output = File.Create(outputFile))
                {
                    var keys1 = new EncryptionKeys(publicKeyStream);
                    var pgp = new PGP(keys1);
                    await pgp.EncryptStreamAsync(input, output);
                }                
            }
        }

        /// <summary>
        /// Método para mover el archivo.
        /// </summary>
        /// <param name="fInfo"></param>
        /// <param name="destPath"></param>
        private void MoveFile(FileInfo fInfo, string destPath)
        {
            // Crear FileInfo del destino
            FileInfo fInfoDest = new FileInfo(destPath);
            string dirPath = fInfoDest.Directory.FullName;

            //write_Log("INFO|MoveFile|Verifica si el directorio existe, si no existe lo crea: " + dirPath);
            LogInfo("INFO|MoveFile|Verifica si el directorio existe, si no existe lo crea: " + dirPath);

            // Si el directorio no existe, crearlo
            if (!Directory.Exists(dirPath))
            {
                Directory.CreateDirectory(dirPath);
            }

            //write_Log("INFO|MoveFile|Verifica si el archivo existe en directorio, si existe lo elimina: " + destPath);
            LogInfo("INFO|MoveFile|Verifica si el archivo existe en directorio, si existe lo elimina: " + destPath);

            // Si el archivo existe, eliminarlo
            if (File.Exists(destPath))
            {
                File.Delete(destPath);
            }

            // Mover archivo
            fInfo.MoveTo(destPath);
        }
        #endregion

    }
}
using AplicacionGPH_ArchivosSalida.Enums;
using AplicacionGPH_ArchivosSalida.Interfaces;
using AplicacionGPH_ArchivosSalida.Servicios;
using InfraestructuraGPH_ArchivosSalida.Repositorio;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Core.Configuracion;
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

namespace GPH_ArchivosSalida
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// Hereda de WindowsServiceHostBase, quien contiene toda robustez de la gestión del ciclo de vida del servicio.
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private Configuracion _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.GPH_ArchivosSalida.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

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
                LecturaConfguracionGeneral.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = LecturaConfguracionGeneral.ObtenerServicio(NombreServicio.GPH_ArchivosSalida.ToString());
                ValidarServicioConfiguracionLocal(servicioMainConfiguracion);

                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.PathLog = servicioMainConfiguracion.Log;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;
                _configuracionBase.CorreoParaDefault = LecturaConfguracionGeneral.ObtenerValorGeneral(ConstantesServicio.Para);
                _configuracionBase.SujetoDefault = LecturaConfguracionGeneral.ObtenerValorGeneral(ConstantesServicio.AsuntoTest);
                _configuracionBase.CorreoSenderTest = LecturaConfguracionGeneral.ObtenerValorGeneral(ConstantesServicio.SenderTest);

                Logging.Init(_configuracionBase.PathLog, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
            }

            //Registra la configuracion...
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);

            //Registra los componentes...
            _containerDI.Register<AplicacionGPH_ArchivosSalida.OperacionServicio>();
            _containerDI.Register<IRepositorioTomi, RepositorioTomi>();
            _containerDI.Register<IRealizaControlError, RealizaControlError>(Lifestyle.Scoped);
            _containerDI.Verify();
        }

        /// <summary>
        /// Realiza la obtención inicial de parámetros (una sola vez).
        /// </summary>
        protected override async Task ParametrizacionesAsync(CancellationToken ct)
        {

            //Obtiene el proceso actual...
            IParametrizacion _parametrizacion = _containerDI.GetInstance<IParametrizacion>();
            _configuracionBase = await _parametrizacion.ObtenerProcesoActualValidadoAsync(_configuracionBase, ct).ConfigureAwait(false);

            //Validación explícita posterior para revisión de contrato...
            _parametrizacion.ValidarProcesoActual(_configuracionBase);

            //Marca el servicio en uso...
            IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
            await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Ejecuta el funcionamiento principal del servicio en cada tick.
        /// </summary>
        protected override async Task OnTimerAsync(CancellationToken ct)
        {
            //Ejecuta el procedimiento principal del servicio...
            using (AsyncScopedLifestyle.BeginScope(_containerDI))
            {
                var operacionServicio = _containerDI.GetInstance<AplicacionGPH_ArchivosSalida.OperacionServicio>();
                await operacionServicio.EjecutarProcedimientosAsync(ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Realiza ciertas operaciones antes de finalizar el servicio.
        /// </summary>
        protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            //Si no se inicializó correctamente el servicio, no se realiza la terminación...
            if (_containerDI == null || _configuracionBase == null)
            {
                Config.EscribirLog(ConstantesServicio.MensajeOmitido);
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
            }
        }

        /// <summary>
        /// Especifica los recursos a liberar al finalizar el servicio.
        /// </summary>
        protected override void LiberarRecursosTerminacion()
            => _containerDI?.Dispose();

        /// <summary>
        /// Valida la configuración del servicio obtenida desde el archivo de configuración.
        /// </summary>
        private static void ValidarServicioConfiguracionLocal(ServicioConfiguracionSettings configuracion)
        {
            if (configuracion == null)
                throw new InvalidOperationException("No se obtuvo configuración del servicio.");

            if (string.IsNullOrWhiteSpace(configuracion.Log))
                throw new InvalidOperationException("La configuración obligatoria 'Log' no está definida para el servicio.");

            if (configuracion.TamanoMaximoArchivoLogKB <= 0)
                throw new InvalidOperationException("La configuración obligatoria 'TamanoMaximoArchivoLogKB' debe ser mayor a cero para el servicio.");

            if (configuracion.TimerEnable && configuracion.Timer <= 0)
                throw new InvalidOperationException("La configuración obligatoria 'Timer' debe ser mayor a cero cuando 'TimerEnable' es true en el servicio.");
        }

    }
}
using AplicacionGPH_ArchivosSalida.Enums;
using AplicacionGPH_ArchivosSalida.Interfaces;
using AplicacionGPH_ArchivosSalida.Modelos;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Threading.Tasks;
using static AplicacionGPH_ArchivosSalida.Enums.ConstantesServicio;



namespace AplicacionGPH_ArchivosSalida.Servicios
{
    public class RealizaControlError : IRealizaControlError
    {

        public async Task<string> RealizaControlErrorExceptionAsync(int index, Exception e, ControlErrorEstructura[] loControlError, Configuracion _configuracion)
        {
            bool lbEnviaCorreo = false;
            string mensaje = string.Empty;

            string correoDeDefault = string.Empty;
            string correoParaDefault = string.Empty;
            string sujetoDefault = string.Empty;
            
            if (loControlError[index].LbConErrorPrevio)
            {                
                // Valido si es el mismo error
                if (loControlError[index].LsMensaje == e.Message)
                {
                    // Mismo error
                    if (loControlError[index].LdFechaInicio != null &&
                        ((DateTime)loControlError[index].LdFechaInicio).AddMinutes(ControlErrorIntervalos.MinutoPrimero) <= DateTime.Now)
                    {
                        // 10 minutos o más con el mismo error
                        if (loControlError[index].LdFechaMail == null)
                        {
                            lbEnviaCorreo = true;
                            mensaje += ("INFO|ControlError|Primer mail de error, index=" + index) + Environment.NewLine;
                        }
                        else
                        {
                            if (((DateTime)loControlError[index].LdFechaMail).AddMinutes(ControlErrorIntervalos.MinutoRepetir) <= DateTime.Now)
                            {
                                lbEnviaCorreo = true;
                                mensaje += ("INFO|ControlError|Error recursivo, index=" + index) + Environment.NewLine;
                            }
                        }
                    }
                }
                else
                {
                    // Nuevo error
                    loControlError[index].LdFechaInicio = DateTime.Now;
                    loControlError[index].LdFechaMail = null;
                    loControlError[index].LsMensaje = e.Message;
                    loControlError[index].LoExcepcion = e;
                }
            }
            else
            {
                mensaje += ("INFO|ControlError|Es inicio de control de error, index=" + index) + Environment.NewLine;
                loControlError[index].LbConErrorPrevio = true;
                loControlError[index].LdFechaInicio = DateTime.Now;
                loControlError[index].LsMensaje = e.Message;
                loControlError[index].LdFechaMail = null;
                loControlError[index].LoExcepcion = e;
            }

            if (lbEnviaCorreo)
            {
                mensaje += ("INFO|ControlError|Tratando de enviar correo de error, index=" + index) + Environment.NewLine;

                //revisar  en el llamado cargan config y tienen la logica de tomar de  _TEST o del aDB
                if (string.IsNullOrEmpty(_configuracion.CorreoParaDefault) || string.IsNullOrEmpty(_configuracion.CorreoDeDefault))
                {
                    mensaje += ("INFO|ControlError|EnviarCorreo No se identifican datos para enviar correo, index=" + index) + Environment.NewLine;

                }
                else
                {
                    correoDeDefault = _configuracion.CorreoDeDefault;
                    correoParaDefault = _configuracion.CorreoParaDefault;
                    sujetoDefault = _configuracion.SujetoDefault;
                }

                if (!string.IsNullOrEmpty(correoParaDefault) || !string.IsNullOrEmpty(correoDeDefault))
                {
                    try
                    {
                        mensaje += ("INFO|ControlError|EnviarCorreo Inicia Proceso enviar Correo, index=" + index) + Environment.NewLine;
                        var respuesta = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
                        {
                            CorreoOrigen = correoDeDefault,
                            NombreOrigen = ConstantesServicio.NombreOrigen,
                            Para = correoParaDefault,
                            Asunto = sujetoDefault,
                            Mensaje = ConstantesServicio.MensajeDB + loControlError[index].LoExcepcion
                        });

                        if (respuesta)
                        {
                            loControlError[index].LdFechaMail = DateTime.Now;
                            mensaje += ("INFO|ControlError|EnviarCorreo Correo enviado, index=" + index) + Environment.NewLine;
                        }
                        else
                        {
                            mensaje += ("INFO|ControlError|Error al enviar Correo , index=" + index) + Environment.NewLine;

                        }
                    }
                    catch (Exception exMail)
                    {                        
                        mensaje = ("INFO|ControlError|Error|Ocurre exception: " + exMail.Message) + Environment.NewLine;
                        return mensaje;
                    }
                }
                else
                {
                    mensaje += ("INFO|ControlError|Correo sin parametros para enviar, index=" + index) + Environment.NewLine;
                }
                return mensaje;
            }
            return mensaje;
        }
    }
}
using AplicacionGPH_ArchivosSalida.Comunes;
using AplicacionGPH_ArchivosSalida.DTOs;
using AplicacionGPH_ArchivosSalida.Enums;
using AplicacionGPH_ArchivosSalida.Interfaces;
using AplicacionGPH_ArchivosSalida.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace InfraestructuraGPH_ArchivosSalida.Repositorio
{
      

    /// <summary>
    /// Repositorio InterfacesT24 (TOMI) y VistasT24
    /// </summary>
    public class RepositorioTomi : RepositorioMaestro,               
        IRepositorioTomi
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public RepositorioTomi(Configuracion configuracion)
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _configuracion = configuracion;
        }

        #region l1

        /// <summary>
        /// Metodo para recupera información de nivel 1
        /// </summary>
        /// <param name="fecha"></param>
        /// <returns></returns>
        public async Task<List<OperacionL1DTO>> RecuperarOperacionLevel1Async()
        {
            var FechaSistema = Conversion.StringToDate(_configuracion.FechaSistema);
            var resultado = await EjecutarStoredProcedureQuery<OperacionL1DTO>(ConstantesServicio.usp_ObtenerOperacionesGPH_AS_Level1, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", FechaSistema)); 
            });
            return resultado.Data;
        }


        /// <summary>
        /// Metodo para marcar operacion de nivel 1
        /// </summary>
        /// <param name="L1"></param>
        /// <returns></returns>
        public async Task<bool> MarcarOperacionProcesadaLevel1Async(ActualizaL1 L1)
        {
            var resultado = await EjecutarStoredProceduredNonQuery(ConstantesServicio.usp_ActualizarOperacionesGPH_AS_Level1, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", L1.Tipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", L1.Folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@ArchivoNew", L1.ArchivoNew)); 
            });
            return resultado.Exito;
        }

        #endregion

        #region L3
        /// <summary>
        /// Método para recupear operaciones nivel 3
        /// </summary>
        /// <param name="L3"></param>
        /// <returns></returns>
        public async Task<List<OperacionL3DTO>> RecuperarOperacionLevel3Async(ActualizaL3Archivo L3)        
        {
            var FechaSistema = Conversion.StringToDate(_configuracion.FechaSistema);
            var resultado = await EjecutarStoredProcedureQuery<OperacionL3DTO>(ConstantesServicio.usp_ObtenerOperacionesGPH_AS_Level3, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", L3.Tipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio",L3.Folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", FechaSistema)); 
            });
            return resultado.Data;
        }

        /// <summary>
        /// Méetodo para marcar operación de nivel 3
        /// </summary>
        /// <param name="L3"></param>
        /// <returns></returns>
        public async Task<bool> MarcarOperacionProcesadaLevel3Async(ActualizaL3Archivo L3)
        {
            var resultado = await EjecutarStoredProceduredNonQuery(ConstantesServicio.usp_ActualizarOperacionesGPH_AS_Level3, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", L3.Tipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", L3.Folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@ArchivoNew", L3.ArchivoNew));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_operaciones_aceptadas", L3.Num_operaciones_aceptadas));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_operaciones_rechazadas", L3.Num_operaciones_rechazadas));

            });
            return resultado.Exito;
        }

        #endregion

        #region DetFileStsatus


        /// <summary>
        /// Método para recuperar los detalles de operacion nivel 3
        /// </summary>
        /// <param name="L3"></param>
        /// <returns></returns>
        public async Task<List<OperacionL3DetalleDTO>> RecuperarOperacionLevel3DetalleAsync(ActualizaL3Archivo L3)
        {
            var FechaSistema = Conversion.StringToDate(_configuracion.FechaSistema);
            var resultado = await EjecutarStoredProcedureQuery<OperacionL3DetalleDTO>(ConstantesServicio.usp_ObtenerOperacionesGPH_AS_Level3, cmd =>
            {

                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", L3.Tipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", L3.Folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", FechaSistema));


            });
            return resultado.Data;
        }


        /// <summary>
        /// Método para recuperar los detalles de archivo
        /// </summary>
        /// <param name="DetFileStatus"></param>
        /// <returns></returns>
        public async Task<List<OperacionDetailFileStatusDTO>> RecuperarOperacionDetailFileStatusAsync(DetailFileStatus DetFileStatus)

        {
            var resultado = await EjecutarStoredProcedureQuery<OperacionDetailFileStatusDTO>(ConstantesServicio.usp_ObtenerDetailFileStatus, cmd =>
            {

                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", DetFileStatus.Folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioOperacion", DetFileStatus.FolioOperacion));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Pmt_Inf_Id", DetFileStatus.Pmt_Inf_Id));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@EndToEndId", DetFileStatus.EndToEndId));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@WorkTable", DetFileStatus.WorkTable));


            });
            return resultado.Data;
        }
        #endregion

        #region Encrypta


        /// <summary>
        /// Método para recuperar operaciones encriptadas
        /// </summary>
        /// <param name="FechaSistema"></param>
        /// <returns></returns>
        public async Task<List<OperacionEncryptaDTO>> RecuperarOperacionEncryptaAsync()
        {

            var FechaSistema = Conversion.StringToDate(_configuracion.FechaSistema);
            var resultado = await EjecutarStoredProcedureQuery<OperacionEncryptaDTO>(ConstantesServicio.usp_ObtenerOperacionesGPH_AS_Encrypta, cmd =>
            { 
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", FechaSistema));
            });
            return resultado.Data;
        }

        /// <summary>
        /// Método para marcar operación procesada
        /// </summary>
        /// <param name="Encry"></param>
        /// <returns></returns>
        public async Task<bool> MarcarOperacionProcesadaEncryptaAsync(ActualizaEncrypta Encry)
        {
            var resultado = await EjecutarStoredProceduredNonQuery(ConstantesServicio.usp_ActualizarOperacionesGPH_AS_Encrypta, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", Encry.Tipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", Encry.Folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@ErroDesc", Encry.ErroDesc));
            });
            return resultado.Exito;    

        }

        #endregion

        #region EnviaSFTP


        /// <summary>
        /// Método para recuperar operación a enviar
        /// </summary>
        /// <param name="FechaSistema"></param>
        /// <returns></returns>
        public async Task<List<OperacionEnviaDTO>> RecuperarOperacionEnviarAsync()
        {
            var FechaSistema = Conversion.StringToDate(_configuracion.FechaSistema);
            var resultado = await EjecutarStoredProcedureQuery<OperacionEnviaDTO>(ConstantesServicio.usp_ObtenerOperacionesGPH_AS_Envia, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", FechaSistema));
            });
            return resultado.Data;
        }

        /// <summary>
        /// Método para marcar operación enviada
        /// </summary>
        /// <param name="Folio"></param>
        /// <returns></returns>
        public async Task<bool> MarcarOperacionProcesadaEnviarAsync(Int64 Folio)
        {
            var resultado = await EjecutarStoredProceduredNonQuery(ConstantesServicio.usp_ActualizarOperacionesGPH_AS_Envia, cmd =>
            { 
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", Folio));
            });
            return resultado.Exito;

             
        }

         
        #endregion


    }

}
namespace AplicacionGPH_ArchivosSalida.DTOs
{
    /// <summary>
    /// Propiedades para la operacion L3
    /// </summary>
    public class OperacionL3DTO
    {
        public long Folio { get; set; }
        public string Nombre { get; set; }
        public string estatus { get; set; }

        public int Total_operaciones { get; set; }
        public int Num_operaciones_aceptadas { get; set; }
        public int Num_operaciones_rechazadas { get; set; }

        public string Msg_id { get; set; }        

        public string FOLIO_FILE { get; set; }

         public string GROUP_HEADER { get; set; }
 

    }


   
}
