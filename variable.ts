using Aplicacion.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Helpers
{
    /// <summary>
    /// Carga los campos comunes requeridos por todos los tipos de operacion.
    /// </summary
    public class CargaDatosComunes
    {
        /// <summary>
        /// Carga y prepara los datos comunes de una orden de pago para la construccion de la trama de la firma 
        /// </summary>
        /// <param name="conversiones">Objeto que almacena los datos en formato byte</param>
        /// <param name="dataValue">Informacion de la operacion a procesar</param>
        /// <param name="opcion">Identificador utilizado para determianr la estructura final de la trama</param>
        public static void CargaDatos(OrdenPagoByteArray conversiones, OrdenPagoValueModelo dataValue, int opcion)
        {
            byte[] arrayZero = new byte[0];

            conversiones.OpFechaOper = MetodosConversionBytes.DateToByteArray(
                Convert.ToInt32(dataValue.OpFechaOper),
                ByteOrder.BIG_ENDIAN);

            conversiones.InsClaveOrd = MetodosConversionBytes.IntToByteArray(
                Convert.ToInt32(dataValue.InsClaveOrd),
                ByteOrder.BIG_ENDIAN);

            conversiones.InsClaveBen = MetodosConversionBytes.IntToByteArray(
                Convert.ToInt32(dataValue.InsClaveBen),
                ByteOrder.BIG_ENDIAN);

            conversiones.OpCveRastreo = MetodosConversionBytes.StringToByteArray(
                dataValue.OpCveRastreo,
                ByteOrder.BIG_ENDIAN);

            conversiones.OpMonto = MetodosConversionBytes.MoneyToByteArray(
                Convert.ToDouble(dataValue.OpCveRastreo),
                ByteOrder.BIG_ENDIAN);


            if (opcion == 0)
            {
                conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).
                     Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).Concat(conversiones.OpMonto).ToArray();
            }
            else if (opcion == 1)
            {
                conversiones.OpCuentaOrd = MetodosConversionBytes.StringToByteArray(dataValue.OpCuentaOrd, ByteOrder.BIG_ENDIAN);
                conversiones.OpCuentaBen = MetodosConversionBytes.StringToByteArray(dataValue.OpCuentaBen, ByteOrder.BIG_ENDIAN);

                conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).
                  Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).Concat(conversiones.OpMonto).
                  Concat(conversiones.OpCuentaOrd).Concat(conversiones.OpCuentaBen).ToArray();
            }
            else if (opcion == 5)
            {
                conversiones.OpCuentaBen = MetodosConversionBytes.StringToByteArray(dataValue.OpCuentaBen, ByteOrder.BIG_ENDIAN);

                conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).
                     Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).
                     Concat(conversiones.OpMonto).Concat(conversiones.OpCuentaBen).ToArray();
            }
            else if (opcion == 6)
            {
                conversiones.OpCuentaBen = MetodosConversionBytes.StringToByteArray(dataValue.OpCuentaBen, ByteOrder.BIG_ENDIAN);

                MetodosConversionBytes.AddZeroByte(ref arrayZero);
                conversiones.OpCuentaBen2 = arrayZero;

                conversiones.TramaCompleta = conversiones.OpFechaOper
                    .Concat(conversiones.InsClaveOrd)
                    .Concat(conversiones.InsClaveBen)
                    .Concat(conversiones.OpCveRastreo)
                    .Concat(conversiones.OpMonto)
                    .Concat(conversiones.OpCuentaBen)
                    .Concat(conversiones.OpCuentaBen2).ToArray();
            }
            else if (opcion == 7)
            {
                conversiones.TramaCompleta = conversiones.OpFechaOper
                   .Concat(conversiones.InsClaveOrd)
                   .Concat(conversiones.InsClaveBen)
                   .Concat(conversiones.OpCveRastreo)
                   .Concat(conversiones.OpMonto).ToArray();
            }
            else if(opcion == 3)
            {
                conversiones.OpCuentaOrd = MetodosConversionBytes.StringToByteArray(dataValue.OpCuentaOrd, ByteOrder.BIG_ENDIAN);
                conversiones.OpCuentaBen = MetodosConversionBytes.StringToByteArray(dataValue.OpCuentaBen, ByteOrder.BIG_ENDIAN);

                int valorNegativo = -1;

                if (Convert.ToInt32(dataValue.OpCuentaBen2) == valorNegativo)
                {
                    arrayZero = new byte[0];
                    MetodosConversionBytes.AddZeroByte(ref arrayZero);
                    conversiones.OpCuentaBen2 = arrayZero;
                }
                else
                {
                    conversiones.OpCuentaBen2 = MetodosConversionBytes.StringToByteArray(dataValue.OpCuentaBen2, ByteOrder.BIG_ENDIAN);
                }

                conversiones.TramaCompleta = conversiones.TramaCompleta = conversiones.OpFechaOper
                   .Concat(conversiones.InsClaveOrd)
                   .Concat(conversiones.InsClaveBen)
                   .Concat(conversiones.OpCveRastreo)
                   .Concat(conversiones.OpMonto)
                   .Concat(conversiones.OpCuentaOrd)
                   .Concat(conversiones.OpCuentaBen)
                   .Concat(conversiones.OpCuentaBen2).ToArray();
            }
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Aplicacion.FirmaElectronica;
using MUFG_T24.Comunes;
using Aplicacion.Helpers;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Proporciona funcionalidades comunes utilizadas durante el procesamiento de operaciones FX
    /// </summary>
    public class ComunesFXD
    {
        IOperaciones _OperacionesTomi;
        ConfiguracionModelo _Config;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="ComunesFXD"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public ComunesFXD(IOperaciones operacionesTomi, ConfiguracionModelo config)
        {
            _OperacionesTomi = operacionesTomi;
            _Config = config;
        }

        /// <summary>
        /// Construye la informacion necesaria para el registro SPEI y genera la firma digital requerida para el procesamiento
        /// de la operacion.
        /// </summary>
        public async Task<bool> GeneraFirmaDigital(OperacionesSwiftModelo datos, int tipoPago, string tabla, string folio)
        {
            try
            {
                OperacionesGenerales _comunes = new OperacionesGenerales(_Config, _OperacionesTomi);
                Config.EscribirLog("INFO|ComunFXD.GeneraFirmaDigital|Inicia proceso para generación de firma");

                SwfPlanTrabajoMestroModelo objDatosFec = await _OperacionesTomi.ConsultarFechaValor(folio);
                string fecOperacion = string.Empty;

                if (objDatosFec != null)
                {
                    fecOperacion = objDatosFec.fec_valor.ToString();
                }

                string strIntermediaryIdentifier = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.IntCif);
                string strSpeireceiverIdentifier = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftSpei);

                OrdenPagoValueModelo dataValue = new OrdenPagoValueModelo()
                {
                    InsClaveOrd = ConstantesServicios.CASFIM_MUFG,
                    OpFechaOper = fecOperacion,
                    InsClaveBen = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml),
                    OpMonto = datos.Des_importe.ToString(),
                    OpCveRastreo = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SenderReferenceXml)
                };

                if (tipoPago == 3)
                {
                    string rawAccount = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                    string OrderingAccount = rawAccount.Replace("-", "");
                    dataValue.OpCuentaOrd = OrderingAccount;
                }

                if (strIntermediaryIdentifier.Contains(strSpeireceiverIdentifier))
                {
                    Config.EscribirLog($"INFO|registraSPEI|El banco intermediario es a quien se enviara el SPEI");
                    dataValue.OpCuentaBen = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.AwCifAccount);
                    dataValue.OpCuentaBen2 = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntAccountXml);
                }
                else
                {
                    Config.EscribirLog($"INFO|registraSPEI|El creditorAgent es a quien se enviara el SPEI");
                    dataValue.OpCuentaBen = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntAccountXml);
                    dataValue.OpCuentaBen2 = "-1";
                }

                dataValue.TpPago = tipoPago;
                OrdenPagoByteArray conversiones = await ObtieneTrama(dataValue);

                //Generacion de dato firmado
                Config.EscribirLog($"INFO|registraSPEI|Se realiza el llamado para la firma");


                string certPath = _Config.StrDigSigCertNumSer;
                string certPass = _Config.StrDigSigCertPsw;

                string strFirmaDigital = MetodosFirma.GenerateSign(conversiones.TramaCompleta, certPath, certPass);

                if (string.IsNullOrWhiteSpace(strFirmaDigital))
                {
                    Config.EscribirLog("INFO|ComunFXD.GeneraFirmaDigital|No fue posible generear la firma digital.");
                    return false;
                }

                await _OperacionesTomi.ActualizarFirmaDigital(tabla, folio, strFirmaDigital);
                Config.EscribirLog("INFO|ComunFXD.GeneraFirmaDigital|Se actualizó el campo para la firma exitosamente");
                return true;

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ComunFXD.GeneraFirmaDigital|Se presenta exception: {ex.Message}");
            }

            return false;
        }

        /// <summary>
        /// Convierte el texto recibido a un formato compatible con SPEI, removiendo caracteres invalidos, sustituyendo caracteres 
        /// acentuados y normalizando la codificacion
        /// </summary>
        /// <param name="texto"></param>
        /// <returns></returns>
        public async Task<string> texto_SPEI(object texto)
        {
            // Caracteres inválidos (según SPEI / MT-950)
            byte[] laInvalidos = new byte[]
            {
                33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 45, 47, 58, 59, 60,
                61, 62, 63, 64, 91, 92, 93, 94, 95, 96, 123, 124, 125, 126,
                127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138,
                139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150,
                151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162,
                163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173
            };

            // Validación
            if (texto == null || string.IsNullOrWhiteSpace(texto.ToString()))
            {
                return string.Empty;
            }

            // Texto inicial
            string lsTexto = texto.ToString().ToUpper();

            // Reemplazos comunes
            lsTexto = lsTexto.Replace("Á", "A")
                             .Replace("É", "E")
                             .Replace("Í", "I")
                             .Replace("Ó", "O")
                             .Replace("Ú", "U");

            // Codificaciones
            Encoding loUnicode = Encoding.Unicode;
            Encoding loEncoding = Encoding.GetEncoding(850);

            // Texto a bytes
            byte[] laBytes = loUnicode.GetBytes(lsTexto);

            // Conversión de Unicode a CP850
            byte[] laNewBytes = Encoding.Convert(loUnicode, loEncoding, laBytes);

            string lsRegreso = string.Empty;
            int liCount = 0;

            // Análisis por carácter
            for (int liIdx = 0; liIdx < laNewBytes.Length; liIdx++)
            {
                byte liCodigo = laNewBytes[liIdx];
                bool lbEsInvalido = false;

                if (liCodigo < 32 || liCodigo > 173)
                {
                    lbEsInvalido = true;
                }
                else if (Array.IndexOf(laInvalidos, liCodigo) >= 0)
                {
                    lbEsInvalido = true;
                }

                if (!lbEsInvalido)
                {
                    lsRegreso += loEncoding.GetString(laNewBytes, liIdx, 1);
                    liCount++;
                }
            }

            return lsRegreso.Trim();
        }
        
        /// <summary>
        /// Construye la trama SPEI correspondiente a la operacion, utilizando la información contenida en el modelo recibido.
        /// </summary>
        /// <param name="dataValue"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<OrdenPagoByteArray> ObtieneTrama(OrdenPagoValueModelo dataValue)
        {
            OrdenPagoByteArray conversiones = new OrdenPagoByteArray();
            Int16 tipoPago = Convert.ToInt16(dataValue.TpPago);
            int opcion = 0;

            var propiedades = dataValue.GetType().GetProperties();

            foreach (var prop in propiedades)
            {
                var valor = prop.GetValue(dataValue);
                Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|{prop.Name} = {valor}");
            }

            try
            {
                switch (tipoPago)
                {
                    case 0:
                        opcion = 0;
                        Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|Se inicia proceso de conversión de bytes tipo Devolucion");
                        CargaDatosComunes.CargaDatos(conversiones, dataValue, opcion);
                        break;

                    case 1:
                        opcion = 1;
                        Config.EscribirLog($"INFO|ComunFXD.ObtieneTramal|Se inicia proceso de conversión de bytes tipo tercero tercero");
                        CargaDatosComunes.CargaDatos(conversiones, dataValue, opcion);
                        break;

                    case 5:
                        opcion = 5;
                        Config.EscribirLog($"INFO|ComunFXD.ObtieneTramal|Se inicia proceso de conversión de bytes tipo Participante a tercero");
                        CargaDatosComunes.CargaDatos(conversiones, dataValue, opcion);
                        break;

                    case 6:
                        opcion = 6;
                        Config.EscribirLog($"INFO|ComunFXD.ObtieneTramal|Se inicia proceso de conversión de bytes tipo Participante a Vostro");
                        CargaDatosComunes.CargaDatos(conversiones, dataValue, opcion);
                        break;

                    case 7:

                        opcion = 7;
                        Config.EscribirLog($"INFO|ComunFXD.ObtieneTramal|Se inicia proceso de conversión de bytes tipo Participante a Participante");
                        CargaDatosComunes.CargaDatos(conversiones, dataValue, opcion);
                        break;

                    case 3:
                        opcion = 3;
                        Config.EscribirLog($"INFO|ComunFXD.ObtieneTramal|Se inicia proceso de conversión de bytes tipo Tercero a Tercero Vostro");
                        CargaDatosComunes.CargaDatos(conversiones, dataValue, opcion);
                        break;

                    default:
                        throw new Exception($"No existe definición para pago SPEI del tipo de pago {tipoPago} ");
                }

                await LogueoDatosTrama(conversiones);

            }
            catch (Exception ex)
            {
                throw new Exception($"ERROR|ComunFXD.ObtieneTrama|Se presenta exception: {ex.Message}");
            }

            return conversiones;
        }

        /// <summary>
        /// Escribe los logs en base a la trama
        /// </summary>
        /// <param name="datos"></param>
        /// <returns></returns>
        private async Task LogueoDatosTrama(OrdenPagoByteArray datos)
        {
            Config.EscribirLog("INFO|ComunFXD.GeneraFirmaDigital|Se obtienen bytes de cada tipo de dato ");

            var propiedades = datos.GetType().GetProperties();
            foreach (var prop in propiedades)
            {
                var valor = prop.GetValue(datos);
                Config.EscribirLog($"INFO|ComunFXD.ObtieneTramal|{prop.Name} = {valor.ToString()}");
            }
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using static MUFG_T24.Modelos.CalypsoMwRespuesta;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Proporciona funcionalidades comunes utilizadas durante el flujo operativo para el registro, seguimiento y actualizacion del estado 
    /// de las operaciones procesadas por el sistema.
    /// </summary>
    public class OperacionesGenerales : OperacionBase
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="OperacionesGenerales"/>
        /// </summary>
        /// <param name="config">Configuracion utilizada durante el flujo operativo</param>
        /// <param name="operacionesTomi">BD Tomi</param>
        /// <param name="conexionT24Mw">BD VistasT24</param>
        public OperacionesGenerales(ConfiguracionModelo config, IOperaciones operacionesTomi, ConexionT24Mw conexionT24Mw = null)
        {
            _OperacionesTomi = operacionesTomi;
            _ConexionT24Mw = conexionT24Mw;
            _Config = config;
        }

        /// <summary>
        /// Regisra la trazabilidad de entredas y salidas generadas durante el procesamiento  de una operacion y marca el  avance de la etapa 
        /// correspondiente dentro del flujo operativo.
        /// </summary>
        public async Task<bool> Registra_E_S(string folio, string tipoOperacion, string aliasProceso, TombinEntradasSalidasModelo registro)
        {
            bool bRegistrar = false;
            int iSalida = 0;

            int NumRegistros = await _OperacionesTomi.ObtenerNumRegistrosPlantrabajoDetalle(folio, tipoOperacion, aliasProceso);
            
            if (NumRegistros > 0)
                bRegistrar = true;

            Config.EscribirLog($"INFO|Comunes.Registra_E_S|Por_registrar={bRegistrar}");

            if (bRegistrar)
            {
                iSalida = await _OperacionesTomi.InsertarTombinEntradasSalidas(registro);

                Config.EscribirLog($"INFO|Comunes.Registra_E_S|Paso la ejecución. Salida=={iSalida}");

                if (iSalida == 0)
                {
                    await _OperacionesTomi.ActualizaPlanTrabajoDetalle(folio, tipoOperacion, aliasProceso, 1);
                    return true;
                }
                else
                    return false;
            }
            else
                return false;
        }
        
        /// <summary>
        /// Construye y registra una transaccion en T24 utilizando la informacion de la operacion procesada, obteniendo lo datos requeridos del ordenante, beneficiario y la 
        /// configuracion operativa necesaria para el registro.
        /// </summary>
        /// <param name="icn">Folio de la operacion</param>
        /// <param name="datos">Informacion de la operacion</param>
        public async Task<SalidaT24RegistroModelo> T24_Registro(string icn, OperacionesSwiftModelo datos)
        {
            SalidaT24RegistroModelo resp = new SalidaT24RegistroModelo();

            try
            {
                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.ProcesaOperacion|Inicia registro en T24.");

                string _Moneda = (datos.Cve_moneda.Equals("MXP") || datos.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : datos.Cve_moneda;
                string _SwiftInstOrdenante = string.Empty;
                string _NombreInstOrdenante = string.Empty;
                string _DireccionInstOrdenante = string.Empty;
                string _Descripcion = string.Empty;
                string _DetailCharges = string.Empty;
                string _T24_Internal_Type = string.Empty;
                string _OrderingBank = string.Empty;
                string CIF_Inst_Ordenante = string.Empty;

                _NombreInstOrdenante = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.NombreInstOrdenanteXml);

                if (!string.IsNullOrEmpty(_NombreInstOrdenante))
                {
                    _DireccionInstOrdenante = string.Format("{0} {1}", await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DireccionInstOrdenanteXml), await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DireccionInstOrdenanteXml2));
                    _SwiftInstOrdenante = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.InstOrdenanteXml);
                    //Truncar campos
                    _NombreInstOrdenante = await Quita_Linea(_NombreInstOrdenante, 50);
                    _DireccionInstOrdenante = await Quita_Linea(_DireccionInstOrdenante, 80);

                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Se encontró la información del ordenante por institución ordenante");
                }
                else
                {
                    //No hay banco ordenante, busco el CIF ordenante
                    CIF_Inst_Ordenante = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.CIFOrdenanteXml);
                    if (!string.IsNullOrEmpty(CIF_Inst_Ordenante))
                    {
                        CIFSwiftModelo _cif = await _OperacionesTomi.Busca_Cif_Datos(CIF_Inst_Ordenante, _SwiftInstOrdenante);
                        if (_cif != null)
                        {
                            _SwiftInstOrdenante = _cif.Cve_swift;
                            _NombreInstOrdenante = await Quita_Linea(_cif.Des_nombre, 50);
                            _DireccionInstOrdenante = await Quita_Linea(_cif.Des_calle, 80);

                            Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Se encontró: Swift= {_cif.Cve_swift}, nombre={_NombreInstOrdenante}, calle = {_DireccionInstOrdenante}");
                        }
                    }
                    else
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|No se encontró el CIF de la institución ordenante en el catálogo.");
                }

                //Armando la descripción
                _Descripcion = ConstantesServicios.DescripcionTransIntMt103;

                if (!string.IsNullOrEmpty(_NombreInstOrdenante))
                    _Descripcion += $" {ConstantesServicios.DescBancoOrdenante} {_NombreInstOrdenante}";
                else
                {
                    if (!string.IsNullOrEmpty(await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)))
                        _Descripcion += $", {ConstantesServicios.DescBancoOrigen} {await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}";
                }

                if (!string.IsNullOrEmpty(await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrdenanteXml)))
                    _Descripcion += $", {ConstantesServicios.DescOrdenante} {await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrdenanteXml)}";

                if (!string.IsNullOrEmpty(await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DetalleCargosXml)))
                    _Descripcion += $", {ConstantesServicios.DescCargos} {await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DetalleCargosXml)}";

                if (!string.IsNullOrEmpty(await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.ConceptoXml)))
                    _Descripcion += $", {ConstantesServicios.DescConcepto} {await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.ConceptoXml)}";

                // Es OUR?
                _DetailCharges = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DetalleCargosXml);

                if (_DetailCharges.Equals(ConstantesServicios.DetalleCargoOUR))
                {
                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Es registro con DetailCharges = 'OUR' (sin comisiones).");
                    _T24_Internal_Type = ConstantesServicios.TipoMensajeMT103.Replace("-", "");
                    _OrderingBank = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftEmisor);

                }
                else
                {
                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Inicia Busqueda de comisiones.");
                    _T24_Internal_Type = ConstantesServicios.T24TipoINternoMT103Com;
                    _OrderingBank = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftEmisor);
                }

                //Preparamos los datos para el Registro del FT
                MUFG_T24.Modelos.MwT24TrxMaestro oTrxMaster = new MwT24TrxMaestro()
                {
                    STrxOperationSpecificType = ConstantesServicios.TipoOperacion,
                    STrxOperationSubType = _T24_Internal_Type,
                    STrxOperationFolio = icn,
                    STrxOperationAcct = datos.Num_cuenta
                };

                string _moneda = (datos.Cve_moneda.Equals("MXP") || datos.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : datos.Cve_moneda;
                string _CuentaDeb = string.Empty;
                string _CuentaCred = string.Empty;

                if (_moneda.Equals(ConstantesServicios.ModenaDefaultMN))
                {
                    _CuentaDeb = (datos.Nf_vostro_emisor.Length >= 7) ? datos.Nf_vostro_emisor.Substring(datos.Nf_vostro_emisor.Length - 7) : datos.Nf_vostro_emisor;
                    _CuentaCred = datos.Num_cuenta;

                    if (_CuentaCred.Length == 18)
                        _CuentaCred = _CuentaCred.Substring(11, 7);
                    else
                        throw new Exception("La cuenta a acreditar no es válida");
                }
                else
                {
                    _CuentaDeb = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DebitAccount);
                    _CuentaCred = datos.Num_cuenta;
                }

                string _Beneficiario = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BeneficiarioXml);

                string _OrderingCust = "AC" +
                                        (string.IsNullOrWhiteSpace(await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrderingAccount)) ? "" : (await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrderingAccount)).TrimEnd().TrimStart()) +
                                        (string.IsNullOrWhiteSpace(await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrderingNameAddr1)) ? "" : ConstantesServicios.Separador + (await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrderingNameAddr1)).TrimStart().TrimEnd()) +
                                        (string.IsNullOrWhiteSpace(await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrderingNameAddr2)) ? "" : ConstantesServicios.Separador + (await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrderingNameAddr2)).TrimStart().TrimEnd()) +
                                        (string.IsNullOrWhiteSpace(await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrderingNameAddr3)) ? "" : ConstantesServicios.Separador + (await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.OrderingNameAddr3)).TrimStart().TrimEnd());

                if (string.IsNullOrEmpty(_Beneficiario))
                    _Beneficiario = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.Beneficiario2Xml);

                CIF_Inst_Ordenante = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.CIFOrdenanteXml);

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Buscando datos de la institución ordenante en el catálogo cif={CIF_Inst_Ordenante}.");

                string _SWEnd2EndRef = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.UETR);
                string _SlaUetr = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SLA_UETR);

                MwT24TrxDetalle oTrxDetailFF = new MwT24TrxDetalle()
                {
                    SDebit_acct_no = _CuentaDeb,
                    SDebit_currency = _moneda,
                    SDebit_amount = string.Format("{0:0.##}", datos.Des_importe),
                    SDebit_their_ref = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_their_ref = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_acct_no = _CuentaCred,
                    SCredit_currency = _moneda,
                    SBen_our_charges = _DetailCharges,
                    SOrdering_bank = _OrderingBank,
                    SBeneficiary = _Beneficiario,
                    SOrdering_cust = (_OrderingCust.Substring(_OrderingCust.Length - 1, 1) == ConstantesServicios.Separador) ? _OrderingCust.Substring(0, _OrderingCust.Length - 1) : _OrderingCust,
                    SCusRef1 = _NombreInstOrdenante,
                    SCusRef2 = _SwiftInstOrdenante,
                    SCusRef3 = CIF_Inst_Ordenante,
                    SRelated_ref = await Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SInstructions = _Descripcion,
                    SSatelSysRef = datos.Folio_unico,
                    SFolio_tomi = datos.Folio_unico,
                    SSW_END2END_REF = _SWEnd2EndRef.Trim(),
                };

                resp.BCorrecto = await _ConexionT24Mw.RegistraTrx(datos.Folio_unico, Convert.ToInt32(_Config.FechaJuliana), ConstantesServicios.TablaTrabajo,
                    ConstantesServicios.TipoOperacion, icn, ConstantesServicios.TipoOperacion, oTrxDetailFF , oTrxMaster);

                resp.StrMensajeError = "";
               
                return resp;
            }
            catch (Exception ex)
            {
                resp.StrMensajeError = ex.Message;
                resp.BCorrecto = false;
            }

            return resp;
        }

        /// <summary>
        /// Consulta la existencia de una operacion previamente procesada para evitar registros duplicados.
        /// </summary>
        /// <param name="pFolio_Unico">Folio de la operacion a consultar</param>
        public async Task<string> Existe_Registro(string pFolio_Unico)
        {
            string strFolioAsignado = string.Empty;
            try
            {
                Config.EscribirLog($"INFO|Comunes.Existe_Registro|A validar si la operacion ya fue registrada en Middleware - T24, folio_unico={pFolio_Unico}");

                T24MwControlModelo datoT24 = await _OperacionesTomi.ExisteRegistro(pFolio_Unico);

                if (datoT24 != null)
                    strFolioAsignado = datoT24.T24_folio_asignado;

                if(!string.IsNullOrEmpty(strFolioAsignado))
                    Config.EscribirLog($"INFO|Comunes.Existe_Registro|La operación fue encontrada en la tabla de transacción entre Middleware - T24 con estatus={datoT24.Estatus} folio_t24_propuesto = {datoT24.T24_folio_propuesto}, folio_t24_asignado = {datoT24.T24_folio_asignado}");
                else
                    Config.EscribirLog($"INFO|Comunes.Existe_Registro|La operación no fue encontrada en la tabla deregistro.");

                return strFolioAsignado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|Comunes.Existe_Registro|Ocurrió la excepción: {ex.Message}");
            }

            return strFolioAsignado;
        }

        /// <summary>
        /// Actualiza el estado de una operacion indicando que se ha concluido.
        /// </summary>
        /// <param name="icn">Folio de la operacion</param>
        /// <param name="estatus">Estado final de la operacion</param>
        /// <param name="usuarioAutorizacion">Responsable de la autorizacion</param>
        /// <param name="detalleRechazo">Motivo de rechazo cuando aplique</param>
        /// <returns></returns>
        public async Task<int> Marca_Op_Terminada(string icn, string estatus, string usuarioAutorizacion, string detalleRechazo = "" )
        {
            if(string.IsNullOrEmpty(icn))
                return 0;
            try
            {
                Config.EscribirLog($"INFO|marca_op_terminada|Intento registrar la operación como terminada icn={icn}");
                await _OperacionesTomi.Marca_Op_Terminada(icn, estatus, usuarioAutorizacion, detalleRechazo );

            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|marca_op_terminadan|No se pudo marcar la operación, error: {ex.Message}");
            }

            return 0;
        }

        /// <summary>
        /// Determina si una operacion pueder ser reprocesada, considerando las reglas de reintentos configurados para las operaciones.
        /// </summary>
        /// <param name="cve_mensaje">Clave del mensaje</param>
        /// <param name="folio">Folio unico de la operacion</param>
        /// <returns></returns>
        public async Task<bool> ReintentosPermitidosSWF_MX(string cve_mensaje, string folio)
        {
            try
            {
                DateTime? fecUlt = new DateTime();

                Config.EscribirLog($"INFO|ReintentoPermitido|Consulta para verificar si la operación {cve_mensaje} con folio {folio} se habia registrado previamente sin fondos");

                T24AdmonFoliosModelo t24datos = await _OperacionesTomi.ConsultarReintentosPermitidosSwt(folio, cve_mensaje);

                if (t24datos != null)
                    fecUlt = t24datos.Fecha_registro;
                else
                    fecUlt = null;

                if(fecUlt.HasValue)
                {
                    Config.EscribirLog($"INFO|ReintentoPermitido|Consulta para verificar si la operación {cve_mensaje} con folio {folio} se encuentra registrada previamente sin fondos");
                    
                    DateTime t1 = DateTime.ParseExact(DateTime.Now.ToString("HH:mm"), "HH:mm", null);
                    DateTime t2 = DateTime.ParseExact(_Config.StrSinFondosHoraValida, "HH:mm", null);

                    if(t1 > t2)
                    {
                        await _OperacionesTomi.EliminarRegistroAdmonFolios(folio, cve_mensaje);
                        Config.EscribirLog($"INFO|ReintentoPermitido|Se permite el reintento de la operación {cve_mensaje} con folio {folio} pues la hora es válida");
                        return true;
                    }

                    if(DateTime.Now > fecUlt.Value.AddMinutes(Convert.ToDouble(_Config.StrSinFondosHoraValida)))
                    {
                        await _OperacionesTomi.EliminarRegistroAdmonFolios(folio, cve_mensaje);
                        Config.EscribirLog($"INFO|ReintentoPermitido|Se permite el reintento de la operación {cve_mensaje} con folio {folio} pues la hora es válida");
                        return true;
                    }

                    Config.EscribirLog($"INFO|ReintentoPermitido|No se permite el reintento de la operación {cve_mensaje} con folio {folio}, por no cumplir condiciones válidas ");
                    return false;
                }

                Config.EscribirLog($"INFO|ReintentoPermitido|Se permite el reintento de la operación {cve_mensaje} con folio {folio}. No hay histórico de falta de fondos");
                return true;
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|ReintentoPermitido|{cve_mensaje}|Se presenta error en el folio {folio} - {ex.Message} ");
                return false;
            }
        }

        /// <summary>
        /// Consulta y recupera el valor de un elemento dentro de una estructura XML
        /// </summary>
        public async Task<string> Buscar_Valor_Xml(string strXml, string strCampo)
        {
            string valor = string.Empty;

            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(strXml);

            var nodo = xmlDoc.DocumentElement;

            string _campo = nodo[strCampo]?.InnerText;

            valor = _campo;

            return valor;
        }

        /// <summary>
        /// Normaliza el contenido de una linea eliminando prefijos de numeracion y limitando la longitud del texto cuando sea necesario.
        /// </summary>
        public async Task<string> Quita_Linea(string texto, int largo = 0)
        {
            string strRegreso = string.Empty;

            if (string.IsNullOrEmpty(texto))
                return "";

            if(texto.Length > 2)
            {
                if (char.IsDigit(texto.Substring(0, 1)[0]) && texto.Substring(1,1) == "/") 
                {
                    strRegreso = texto.Substring(2);

                    if (largo == 0 || strRegreso.Length <= largo)
                        return strRegreso;
                    else
                        return strRegreso.Substring(0,largo);
                }
                else
                {
                    if(largo == 0 || texto.Length <= largo)
                        return texto;
                    else
                        return texto.Substring(0,largo);
                }
            }

            return texto;
        }

      /// <summary>
      /// Valida si una cadena corresponde a un identificador SWIFT.
      /// </summary>
        public async Task<bool> EsSwift(string swift)
        {
            string strSWIFT_ExReg = "^([a-zA-Z]){4}([a-zA-Z]){2}([0-9a-zA-Z]){2}([0-9a-zA-Z]{3})?$";
            Regex oExpReg = new Regex(strSWIFT_ExReg);

            return oExpReg.IsMatch(swift);
        }

        /// <summary>
        /// Determina si una cuenta CLABE es valida aplicando las reglas de validacion establecidas para el calculo del digito verificador.
        /// </summary>
        /// <param name="cuenta">Cuenta a validar</param>
        public async Task<bool> EsCuentaCLABE(string cuenta)
        {
            string multiplicador;
            int suma;
            int valores;

            if (cuenta == null) return false;
            if (cuenta.Length != 18) return false;

            valores = 0;
            multiplicador = "37137137137137137";

            for (int i = 0; i < 17; i++)
            {
                int caracter = int.Parse(cuenta.Substring(i, 1));
                int multiplo = int.Parse(multiplicador.Substring(i, 1));
                suma = (caracter * multiplo) % 10;
                valores += suma;
            }

            valores = valores % 10;
            valores = 10 - valores;
            if (valores == 10) valores = 0;

            return cuenta.Substring(17, 1) == valores.ToString();
        }

        /// <summary>
        /// Gestiona el ciclo completo de registro de una transaccion financiera, incluyendo la determinacion de comisiones, invocacion de servicios T24
        /// y actualizacion del estado de la operacion.
        /// </summary>
        public async Task<bool> Registra_TRX_WS(string folio_unico, string tablaTrabajo, string tipoOperacion, string folio, string cve_mensaje,
            MwT24TrxMaestro oTrxMaster, MwT24TrxDetalle oTrxDetailFF,string des_variable_xml)
        {

            WSResponseModelo resultado = new WSResponseModelo();

            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Cuenta: {oTrxDetailFF.SIn_debit_acct_no}");
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Cuenta BEN: {oTrxDetailFF.SCredit_acct_no}");
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Importe : {oTrxDetailFF.SDebit_amount}");
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|A registrar operación.");
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|A buscar comisiones.");

            try
            {
                await _ConexionT24Mw.BuscarEsquemaTrx(oTrxMaster);
                Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Finalmente, el esquema de comisión será: Transaction Type='{oTrxMaster.STrxTransactionType}, esquema = {oTrxMaster.STrxInternalType} '");
                oTrxDetailFF.STransaction_type = oTrxMaster.STrxTransactionType;

                Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Inicia invocación del WS");

                await ImprimirMasterDetail(oTrxMaster,oTrxDetailFF);

                oTrxDetailFF = await LimpiarCamposDetail(oTrxDetailFF);
                des_variable_xml = await LimpiarCamposXMLVariables(des_variable_xml);

                if(oTrxMaster.STrxInternalType == ConstantesServicios.SinEsquema)
                {
                    Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Se ejecutará la versión SIN_ESQUEMA");
                    resultado = await registraT24_WOSH(oTrxMaster, oTrxDetailFF, des_variable_xml);
                } else if( oTrxMaster.STrxInternalType == ConstantesServicios.ConEsquema)
                {
                    Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Se ejecutará la versión CON_ESQUEMA");
                    resultado = await registraT24_WSH(oTrxMaster, oTrxDetailFF, des_variable_xml);
                    
                }

                if (resultado.Result == false)
                {
                    if (resultado.Error.Contains("OVERRIDE"))
                        await ProcesaCuentaSinFondosSWF_MX(cve_mensaje, folio);
                    else
                        await ProcesaRegistroConError(folio, cve_mensaje, resultado.Error);
                }
                else
                    await ProcesaRegistroExitoso(resultado.TransactionID, folio, cve_mensaje);
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|{cve_mensaje}|Se presenta error en el folio {folio} - {ex.Message}");
                return false;
            }

            return true;
        }


        /// <summary>
        /// Procesa una transaccion mediante los servicios de T24 y devuelve un resultado estructurado que indica si la operacion fue exitosa o la causa especifica del
        /// rechazo o error presentado.
        /// </summary>
        public async Task<Result<string>> registra_TRX_WS_Result(string folio, string cve_mensaje, MwT24TrxMaestro clsTrxMaster, MwT24TrxDetalle clsTrxDetail, 
            string des_variables_xml)
        {
            WSResponseModelo resultado = new WSResponseModelo();
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Cuenta: '{clsTrxDetail.SDebit_acct_no}'.");
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Cuenta BEN: '{clsTrxDetail.SCredit_acct_no}'.");
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Importe: '{clsTrxDetail.SDebit_amount}'.");
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|A registrar operación.");
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|A buscar comisiones.");

            try
            {
                //Calculo de esquema de comision o sin esquema de comisiones (la función actualiza TrxOperation_ID, TrxInternal_Type y TrxTransaction_Type)
                await _ConexionT24Mw.BuscarEsquemaTrx(clsTrxMaster);
                
                Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Finalmente, el esquema de comisión será: Transaction Type='{clsTrxMaster.STrxTransactionType}, esquema = {clsTrxMaster.STrxInternalType} '");
                clsTrxDetail.STransaction_type = clsTrxMaster.STrxTransactionType;

                Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Inicia invocación del WS");

                //Se invoca al WS
                await ImprimirMasterDetail(clsTrxMaster,clsTrxDetail);

                clsTrxDetail = await LimpiarCamposDetail(clsTrxDetail);
                des_variables_xml = await LimpiarCamposXMLVariables(des_variables_xml);

                if(clsTrxMaster.STrxInternalType == ConstantesServicios.SinEsquema)
                {
                    Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Se ejecutará la versión SIN_ESQUEMA");
                    resultado = await registraT24_WOSH(clsTrxMaster, clsTrxDetail, des_variables_xml);
                }
                else if(clsTrxMaster.STrxInternalType == ConstantesServicios.ConEsquema)
                {
                    Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Se ejecutará la versión CON_ESQUEMA");
                    resultado = await registraT24_WSH(clsTrxMaster,clsTrxDetail,des_variables_xml);
                }

                if (resultado.Result == false)
                {
                    if (resultado.Error.Contains("OVERRIDE"))
                    {
                        await ProcesaCuentaSinFondosSWF_MX(cve_mensaje, folio);
                        return Result<string>.Failed($"Error: La cuenta no tiene los fondos suficientes para cubrir el monto de la operacion con folio {folio}");
                    }
                    else if (resultado.Error.Contains("SWIFT CHAR"))
                        return Result<string>.Failed($"Error: El mensaje contiene caracteres invalidos para la operacion con folio {folio}");
                    else
                    {
                        await ProcesaRegistroConError(folio, cve_mensaje, resultado.Error);
                        return Result<string>.Failed($"Error: {resultado.Error}");
                    }
                }
                else
                    await ProcesaRegistroExitoso(resultado.TransactionID, folio, cve_mensaje);
            }
            catch(TimeoutException toEx)
            {
                Config.EscribirLog($"ERROR|registra_TRX_WS|{cve_mensaje}|Se presenta error en el folio {folio} - {toEx.ToString()}");
                return Result<string>.Failed($"Error: El servicio no regreso una respuesta, varificar operacion en T24. {ErrorCodeEnum.T24TimeoutError}");
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|registra_TRX_WS|{cve_mensaje}|Se presenta error en el folio {folio} - {ex.ToString()}");
                return Result<string>.Failed($"Error: Se generó la siguiente Excepción: {ex.Message}");
            }

            return Result<string>.Success(resultado.TransactionID);
        }

       
        /// <summary>
        /// Ejecuta el proceso de registro de una operacion en T24, mediante el servicio WOSH. realizando el mapeo de informacion financiera, participantes, importes
        /// y referencias necesarias para la transaccion.
        /// </summary>
        /// <param name="clsTrxMaster">Datos generales de la operacion</param>
        /// <param name="clsTrxDetail">Datos financieros y operativos de la transaccion</param>
        /// <param name="des_variable_xml">Informacion XML</param>
        private async Task<WSResponseModelo> registraT24_WOSH(MwT24TrxMaestro clsTrxMaster, MwT24TrxDetalle clsTrxDetail, string des_variable_xml)
        {
            WSResponseModelo resp = new WSResponseModelo();

            string company = _Config.StrWsT24MwCompany;
            string password = _Config.StrWst24MwPasswordSwfMx;
            string userName = _Config.Usuario;

            wsT24SwiftIn.T24WebServicesImplClient wsT24SwiftInRef = new wsT24SwiftIn.T24WebServicesImplClient();
            wsT24SwiftIn.SWIFTINWITHOUTSCHEMARequest req = new wsT24SwiftIn.SWIFTINWITHOUTSCHEMARequest();

            wsT24SwiftIn.webRequestCommon wrc = new wsT24SwiftIn.webRequestCommon()
            {
                company = company,
                password = password,
                userName = userName,
            };

            wsT24SwiftIn.ofsFunction ofsFunction = new wsT24SwiftIn.ofsFunction();
            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMAType ftSwift = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMAType();

            Config.EscribirLog("INFO|registraT24_WSH|  ----DATOS ENVIADOS ----  |");

            ftSwift.TRANSACTIONTYPE = clsTrxDetail.STransaction_type;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.TRANSACTIONTYPE " + ftSwift.TRANSACTIONTYPE);

            ftSwift.DEBITACCTNO = clsTrxDetail.SDebit_acct_no;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.DEBITACCTNO " + ftSwift.DEBITACCTNO);

            ftSwift.DEBITCURRENCY = clsTrxDetail.SDebit_currency;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.DEBITCURRENCY " + ftSwift.DEBITCURRENCY);

            ftSwift.DEBITAMOUNT = clsTrxDetail.SDebit_amount;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.DEBITAMOUNT " + ftSwift.DEBITAMOUNT);

            ftSwift.DEBITTHEIRREF = clsTrxDetail.SDebit_their_ref;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.DEBITTHEIRREF " + ftSwift.DEBITTHEIRREF);

            ftSwift.CREDITTHEIRREF = clsTrxDetail.SCredit_their_ref;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CREDITTHEIRREF " + ftSwift.CREDITTHEIRREF);

            ftSwift.CREDITACCTNO = clsTrxDetail.SCredit_acct_no;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CREDITACCTNO " + ftSwift.CREDITACCTNO);

            ftSwift.CREDITCURRENCY = clsTrxDetail.SCredit_currency;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CREDITCURRENCY " + ftSwift.CREDITCURRENCY);

            ftSwift.CREDITAMOUNT = await Buscar_Valor_Xml(des_variable_xml, "mensaje_importe");

            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERINGCUST gOrdCus = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERINGCUST();

            string mensajeTipo = await Buscar_Valor_Xml(des_variable_xml, "mensaje_tipo");

            if(mensajeTipo.Equals(ConstantesServicios.TipoMensajePACS008))
            {
                List<string> orderinCustNomAddr = await extraeNodosXML(des_variable_xml, "ordering_name_addr");
                List<string> orderingCustFinal = new List<string>();

                string strOrderingAccount = await Buscar_Valor_Xml(des_variable_xml, "ordering_account");

                orderingCustFinal.Add("AC" + strOrderingAccount);
                orderingCustFinal = orderingCustFinal.Concat(orderinCustNomAddr).ToList();

                gOrdCus.ORDERINGCUST = orderingCustFinal.ToArray();
            }
            else
                gOrdCus.ORDERINGCUST = new string[] { clsTrxDetail.SOrdering_cust };

            gOrdCus.g = "1";

            ftSwift.gORDERINGCUST = gOrdCus;


            for (int i = 0; i < gOrdCus.ORDERINGCUST.Length; i++)
            {
                gOrdCus.ORDERINGCUST[i] = gOrdCus.ORDERINGCUST[i].Replace(":", " ");
                Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.ORDERINGCUST " + gOrdCus.ORDERINGCUST[i]);
            }

            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERINGBANK gOrdBank = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERINGBANK
            {
                ORDERINGBANK = new string[] { clsTrxDetail.SOrdering_bank },
                g = "1"
            };

            ftSwift.gORDERINGBANK = gOrdBank;

            foreach(var x in gOrdBank.ORDERINGBANK)
            {
                Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.gORDERINGBANK " + x);
            }

            ftSwift.BENOURCHARGES = clsTrxDetail.SBen_our_charges;
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.BENOURCHARGES " + ftSwift.BENOURCHARGES);

            ftSwift.RELATEDREF = clsTrxDetail.SRelated_ref;
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.RELATEDREF " + ftSwift.RELATEDREF);

            List<string> instructions = await convierteTexto_a_Multivalor_SinCortar_Local(clsTrxDetail.SInstructions, 65);

            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGINSTRUCTIONS gInstructions = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGINSTRUCTIONS
            {
                g = "1",
                INSTRUCTIONS = instructions.ToArray()
            };

            ftSwift.gINSTRUCTIONS = gInstructions;

            foreach (var x in gInstructions.INSTRUCTIONS)
            {
                Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.INSTRUCTIONS " + x);
            }

            List<string> beneficiaryNameAddr = await extraeNodosXML(des_variable_xml, "beneficiary_name_addr");
            ftSwift.BENEFICIARYNAM = string.Join(",",beneficiaryNameAddr);
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.BENEFICIARYNAM " + ftSwift.BENEFICIARYNAM);

            ftSwift.SATELSYSREF = clsTrxDetail.SSatelSysRef;
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.SATELSYSREF " + ftSwift.SATELSYSREF);

            ftSwift.CUSTREF1 = clsTrxDetail.Cust_Ref1;
            ftSwift.CUSTREF2 = clsTrxDetail.Cust_Ref2;
            ftSwift.CUSTREF3 = clsTrxDetail.Cust_Ref3;
            ftSwift.CUSTREF4 = clsTrxDetail.Cust_Ref4;
            ftSwift.CUSTREF5 = clsTrxDetail.Cust_Ref5;

            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.CUSTREF1 " + ftSwift.CUSTREF1);
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.CUSTREF2 " + ftSwift.CUSTREF2);
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.CUSTREF3 " + ftSwift.CUSTREF3);
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.CUSTREF4 " + ftSwift.CUSTREF4);
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.CUSTREF5 " + ftSwift.CUSTREF5);

            ftSwift.SWEND2ENDREF = clsTrxDetail.SSW_END2END_REF;
            Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.CUSTREF5 " + ftSwift.SWEND2ENDREF);

            List<string> ordering_cust_nm_ext = await extraeNodosXML(des_variable_xml, "ordering_cust_nm_ext");
            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERCUNMEXT gOrdCuNmExt = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERCUNMEXT();

            if (ordering_cust_nm_ext.Count > 0)
                gOrdCuNmExt.ORDERCUNMEXT = ordering_cust_nm_ext.ToArray();
            else
                gOrdCuNmExt.ORDERCUNMEXT = new string[] { };

            gOrdCuNmExt.g = "1";

            ftSwift.gORDERCUNMEXT = gOrdCuNmExt;

            foreach (var x in gOrdCuNmExt.ORDERCUNMEXT)
            {
                Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.gORDERCUNMEXT " + x);
            }

            List<string> ordering_cust_adr_ext = await extraeNodosXML(des_variable_xml, "ordering_cust_adr_ext");
            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERCUADEXT gOrdCuAdExt = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERCUADEXT();

            if (ordering_cust_adr_ext.Count > 0)
                gOrdCuAdExt.ORDERCUADEXT = ordering_cust_adr_ext.ToArray();
            else
                gOrdCuAdExt.ORDERCUADEXT = new string[] { };

            gOrdCuAdExt.g = "1";

            ftSwift.gORDERCUADEXT = gOrdCuAdExt;

            foreach (var x in gOrdCuAdExt.ORDERCUADEXT)
            {
                Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.ORDERCUADEXT " + x);
            }

            List<string> beneficiary_name_ext = await extraeNodosXML(des_variable_xml, "beneficiary_name_ext");
            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGBENEFNAMEEXT gBenefNameExt = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGBENEFNAMEEXT();

            if (beneficiary_name_ext.Count > 0)
                gBenefNameExt.BENEFNAMEEXT = beneficiary_name_ext.ToArray();
            else
                gBenefNameExt.BENEFNAMEEXT = new string[] { };

            gBenefNameExt.g = "1";
            ftSwift.gBENEFNAMEEXT = gBenefNameExt;

            for (int i = 0; i < gBenefNameExt.BENEFNAMEEXT.Length; i++)
            {
                gBenefNameExt.BENEFNAMEEXT[i] = gBenefNameExt.BENEFNAMEEXT[i].Replace(":", " ");
                Config.EscribirLog("INFO|registraT24_WOSH|ftSwift.BENEFNAMEEXT " + gBenefNameExt.BENEFNAMEEXT[i]);
            }

            req.WebRequestCommon = wrc;
            req.OfsFunction = ofsFunction;
            req.FUNDSTRANSFERSWIFTINWOSCHEMAType = ftSwift;

            wsT24SwiftIn.SWIFTINWITHOUTSCHEMAResponse response = new wsT24SwiftIn.SWIFTINWITHOUTSCHEMAResponse();

            response = await ((wsT24SwiftIn.T24WebServicesImpl)wsT24SwiftInRef).SWIFTINWITHOUTSCHEMAAsync(req);

            Config.EscribirLog($"INFO|registraT24_WOSH | Response {response.Status.successIndicator.ToString()}");

            if(response.Status.successIndicator == wsT24SwiftIn.successIndicator.Success && !string.IsNullOrEmpty(response.Status.transactionId))
            {
                resp.TransactionID = response.Status.transactionId;
                resp.Result = true;
                resp.Error = string.Empty;
                Config.EscribirLog($"INFO|registraT24_WOSH | Respuesta exitosa. Se obtuvo el FT {resp.TransactionID}");
            }
            else
            {
                resp.TransactionID = string.Empty;
                resp.Result = false;
                resp.Error = await obtieneMensajeError(response.Status.messages);
                Config.EscribirLog($"ERROR|registraT24_WOSH|El servicio regresó respuesta con error. Se actualizan tablas de error. {resp.Error}");
            }
            return resp;
        }

        /// <summary>
        /// Ejecuta el registro de una operacion en T24 mediante el servicio WSH, realizando el mapeo de cuentas, participantes, importes,
        /// divisas, referencias y datos obtenidos del XML de la operacion.
        /// </summary>
        /// <param name="clsTrxMaster">Datos generales de la operacion</param>
        /// <param name="clsTrxDetail">Datos financieros y operativos de la transaccion</param>
        /// <param name="des_variable_xml">Informacion XML</param>
        private async Task<WSResponseModelo> registraT24_WSH(MwT24TrxMaestro clsTrxMaster, MwT24TrxDetalle clsTrxDetail, string des_variable_xml) 
        {
            WSResponseModelo resp = new WSResponseModelo();

            string company = _Config.StrWsT24MwCompany;
            string password = _Config.StrWst24MwPasswordSwfMx;
            string userName = _Config.Usuario;

            wsT24SwiftIn.T24WebServicesImplClient wsT24SwiftInRef = new wsT24SwiftIn.T24WebServicesImplClient();
            wsT24SwiftIn.SWIFTINWITHSCHEMARequest req = new wsT24SwiftIn.SWIFTINWITHSCHEMARequest();

            wsT24SwiftIn.webRequestCommon wrc = new wsT24SwiftIn.webRequestCommon()
            {
                company = company,
                password = password,
                userName = userName,
            };

            wsT24SwiftIn.ofsFunction ofsFunction = new wsT24SwiftIn.ofsFunction();
            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMAType ftSwift = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMAType();

            Config.EscribirLog("INFO|registraT24_WSH|  ----DATOS ENVIADOS ----  |");

            ftSwift.TRANSACTIONTYPE = clsTrxDetail.STransaction_type;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.TRANSACTIONTYPE " + ftSwift.TRANSACTIONTYPE);

            ftSwift.DEBITACCTNO = clsTrxDetail.SDebit_acct_no;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.DEBITACCTNO " + ftSwift.DEBITACCTNO);

            ftSwift.DEBITCURRENCY = clsTrxDetail.SDebit_currency;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.DEBITCURRENCY " + ftSwift.DEBITCURRENCY);

            ftSwift.DEBITAMOUNT = clsTrxDetail.SDebit_amount;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.DEBITAMOUNT " + ftSwift.DEBITAMOUNT);


            ftSwift.DEBITTHEIRREF = clsTrxDetail.SDebit_their_ref;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.DEBITTHEIRREF " + ftSwift.DEBITTHEIRREF);

            ftSwift.CREDITTHEIRREF = clsTrxDetail.SCredit_their_ref;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CREDITTHEIRREF " + ftSwift.CREDITTHEIRREF);

            ftSwift.CREDITACCTNO = clsTrxDetail.SCredit_acct_no;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CREDITACCTNO " + ftSwift.CREDITACCTNO);

            ftSwift.CREDITCURRENCY = clsTrxDetail.SCredit_currency;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CREDITCURRENCY " + ftSwift.CREDITCURRENCY);

            ftSwift.CREDITAMOUNT = await Buscar_Valor_Xml(des_variable_xml, "mensaje_importe");

            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERINGCUST gOrdCus = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERINGCUST();

            string mensajeTipo = await Buscar_Valor_Xml(des_variable_xml, "mensaje_tipo");

            if (mensajeTipo.Equals(ConstantesServicios.TipoMensajePACS008))
            {
                List<string> orderinCustNomAddr = await extraeNodosXML(des_variable_xml, "ordering_name_addr");
                List<string> orderingCustFinal = new List<string>();

                string strOrderingAccount = await Buscar_Valor_Xml(des_variable_xml, "ordering_account");

                orderingCustFinal.Add("AC" + strOrderingAccount);
                orderingCustFinal = orderingCustFinal.Concat(orderinCustNomAddr).ToList();

                gOrdCus.ORDERINGCUST = orderingCustFinal.ToArray();
            }
            else
                gOrdCus.ORDERINGCUST = new string[] { clsTrxDetail.SOrdering_cust };

            gOrdCus.g = "1";

            ftSwift.gORDERINGCUST = gOrdCus;


            for (int i = 0; i < gOrdCus.ORDERINGCUST.Length; i++)
            {
                gOrdCus.ORDERINGCUST[i] = gOrdCus.ORDERINGCUST[i].Replace(":", " ");
                Config.EscribirLog("INFO|registraT24_WSH|ftSwift.ORDERINGCUST " + gOrdCus.ORDERINGCUST[i]);
            }

            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERINGBANK gOrdBank = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERINGBANK
            {
                ORDERINGBANK = new string[] { clsTrxDetail.SOrdering_bank },
                g = "1"
            };

            ftSwift.gORDERINGBANK = gOrdBank;

            foreach (var x in gOrdBank.ORDERINGBANK)
            {
                Config.EscribirLog("INFO|registraT24_WSH|ftSwift.gORDERINGBANK " + x);
            }

            ftSwift.BENOURCHARGES = clsTrxDetail.SBen_our_charges;
            Config.EscribirLog("INFO|registraT24_WOH|ftSwift.BENOURCHARGES " + ftSwift.BENOURCHARGES);

            ftSwift.RELATEDREF = clsTrxDetail.SRelated_ref;
            Config.EscribirLog("INFO|registraT24_WOH|ftSwift.RELATEDREF " + ftSwift.RELATEDREF);

            List<string> instructions = await convierteTexto_a_Multivalor_SinCortar_Local(clsTrxDetail.SInstructions, 65);

            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGINSTRUCTIONS gInstructions = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGINSTRUCTIONS
            {
                g = "1",
                INSTRUCTIONS = instructions.ToArray()
            };

            ftSwift.gINSTRUCTIONS = gInstructions;

            foreach (var x in gInstructions.INSTRUCTIONS)
            {
                Config.EscribirLog("INFO|registraT24_WSH|ftSwift.INSTRUCTIONS " + x);
            }

            List<string> beneficiaryNameAddr = await extraeNodosXML(des_variable_xml, "beneficiary_name_addr");
            ftSwift.BENEFICIARYNAM = string.Join(",", beneficiaryNameAddr);
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.BENEFICIARYNAM " + ftSwift.BENEFICIARYNAM);

            ftSwift.SATELSYSREF = clsTrxDetail.SSatelSysRef;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.SATELSYSREF " + ftSwift.SATELSYSREF);

            ftSwift.CUSTREF1 = clsTrxDetail.Cust_Ref1;
            ftSwift.CUSTREF2 = clsTrxDetail.Cust_Ref2;
            ftSwift.CUSTREF3 = clsTrxDetail.Cust_Ref3;
            ftSwift.CUSTREF4 = clsTrxDetail.Cust_Ref4;
            ftSwift.CUSTREF5 = clsTrxDetail.Cust_Ref5;

            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CUSTREF1 " + ftSwift.CUSTREF1);
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CUSTREF2 " + ftSwift.CUSTREF2);
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CUSTREF3 " + ftSwift.CUSTREF3);
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CUSTREF4 " + ftSwift.CUSTREF4);
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CUSTREF5 " + ftSwift.CUSTREF5);

            ftSwift.SWEND2ENDREF = clsTrxDetail.SSW_END2END_REF;
            Config.EscribirLog("INFO|registraT24_WSH|ftSwift.CUSTREF5 " + ftSwift.SWEND2ENDREF);

            List<string> ordering_cust_nm_ext = await extraeNodosXML(des_variable_xml, "ordering_cust_nm_ext");
            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERCUNMEXT gOrdCuNmExt = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERCUNMEXT();

            if (ordering_cust_nm_ext.Count > 0)
                gOrdCuNmExt.ORDERCUNMEXT = ordering_cust_nm_ext.ToArray();
            else
                gOrdCuNmExt.ORDERCUNMEXT = new string[] { };

            gOrdCuNmExt.g = "1";

            ftSwift.gORDERCUNMEXT = gOrdCuNmExt;

            foreach (var x in gOrdCuNmExt.ORDERCUNMEXT)
            {
                Config.EscribirLog("INFO|registraT24_WSH|ftSwift.gORDERCUNMEXT " + x);
            }

            List<string> ordering_cust_adr_ext = await extraeNodosXML(des_variable_xml, "ordering_cust_adr_ext");
            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERCUADEXT gOrdCuAdExt = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERCUADEXT();

            if (ordering_cust_adr_ext.Count > 0)
                gOrdCuAdExt.ORDERCUADEXT = ordering_cust_adr_ext.ToArray();
            else
                gOrdCuAdExt.ORDERCUADEXT = new string[] { };

            gOrdCuAdExt.g = "1";

            ftSwift.gORDERCUADEXT = gOrdCuAdExt;

            foreach (var x in gOrdCuAdExt.ORDERCUADEXT)
            {
                Config.EscribirLog("INFO|registraT24_WSH|ftSwift.ORDERCUADEXT " + x);
            }

            List<string> beneficiary_name_ext = await extraeNodosXML(des_variable_xml, "beneficiary_name_ext");
            wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGBENEFNAMEEXT gBenefNameExt = new wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGBENEFNAMEEXT();

            if (beneficiary_name_ext.Count > 0)
                gBenefNameExt.BENEFNAMEEXT = beneficiary_name_ext.ToArray();
            else
                gBenefNameExt.BENEFNAMEEXT = new string[] { };

            gBenefNameExt.g = "1";
            ftSwift.gBENEFNAMEEXT = gBenefNameExt;

            for (int i = 0; i < gBenefNameExt.BENEFNAMEEXT.Length; i++)
            {
                gBenefNameExt.BENEFNAMEEXT[i] = gBenefNameExt.BENEFNAMEEXT[i].Replace(":", " ");
                Config.EscribirLog("INFO|registraT24_WSH|ftSwift.BENEFNAMEEXT " + gBenefNameExt.BENEFNAMEEXT[i]);
            }

            req.WebRequestCommon = wrc;
            req.OfsFunction = ofsFunction;
            req.FUNDSTRANSFERSWIFTINWSCHEMAType = ftSwift;

            wsT24SwiftIn.SWIFTINWITHSCHEMAResponse response = new wsT24SwiftIn.SWIFTINWITHSCHEMAResponse();

            response = await ((wsT24SwiftIn.T24WebServicesImpl)wsT24SwiftInRef).SWIFTINWITHSCHEMAAsync(req);

            Config.EscribirLog($"INFO|registraT24_WSH | Response {response.Status.successIndicator.ToString()}");

            if (response.Status.successIndicator == wsT24SwiftIn.successIndicator.Success && !string.IsNullOrEmpty(response.Status.transactionId))
            {
                resp.TransactionID = response.Status.transactionId;
                resp.Result = true;
                resp.Error = string.Empty;
                Config.EscribirLog($"INFO|registraT24_WSH | Respuesta exitosa. Se obtuvo el FT {resp.TransactionID}");
            }
            else
            {
                resp.TransactionID = string.Empty;
                resp.Result = false;
                resp.Error = await obtieneMensajeError(response.Status.messages);
                Config.EscribirLog($"ERROR|registraT24_WSH|El servicio regresó respuesta con error. Se actualizan tablas de error. {resp.Error}");
            }
            return resp;
        }

        /// <summary>
        /// Gestiona el procesamiento de operaciones que presentan la condicion de cuenta sin fondos, registrando o actualizando la informacion
        /// correspondiente para su posterior seguimiento.
        /// </summary>
        /// <param name="cve_mensaje">Clave del mensaje de la operacion</param>
        /// <param name="lsFolio">Folio unico de la operacion</param>
        public async Task ProcesaCuentaSinFondosSWF_MX(string cve_mensaje, string lsFolio)
        {
            try
            {
                int num = 0;
                Config.EscribirLog($"INFO|ProcesaCuentaSinFondos| Inicia el procedimiento para tratar la operacion {cve_mensaje}");
                await _OperacionesTomi.ActualizarSwfPlanTrabajoMaestroCuentaSinFondos(lsFolio,cve_mensaje);

                AdmonFoliosModelo rsDatos = await _OperacionesTomi.ConsultaAdmonFoliosCuentaSinFondos(lsFolio, cve_mensaje);

                if (rsDatos != null)
                    num = rsDatos.valor;

                if(num == 0)
                {
                    Config.EscribirLog($"INFO|ProcesaCuentaSinFondos| Inserción de la operacion {cve_mensaje} con folio {lsFolio} en la tabla TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX");
                }
                else
                {
                    Config.EscribirLog($"INFO|ProcesaCuentaSinFondos| INFO|ProcesaCuentaSinFondos| Actualización de la operacion {cve_mensaje} con folio {lsFolio} en la tabla TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX");
                }

                await _OperacionesTomi.ActualizarAdmonFoliosCuentasSinFndos(lsFolio, cve_mensaje, num);
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"INFO|ProcesaCuentaSinFondos|{cve_mensaje} |Se presenta error en el folio {lsFolio} - {ex.Message}");
                throw new Exception("Error al Procesar la Cuenta Sin Fondos");
            }
        }

        /// <summary>
        /// Actualiza el estado de unaa operacion fallida y almacena el detalle de error para fines de seguimiento y auditoria.
        /// </summary>
        public async Task ProcesaRegistroConError(string folio, string cve_mensaje, string des_response)
        {
            try
            {
                await _OperacionesTomi.ProcesarCuentasConError(folio,des_response);
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesaRegistroConError|{cve_mensaje}| Se presenta error en el folio  {folio}  -  {ex.Message}");
                throw new Exception("Error al registrar error en la transacción");
            }
        }


        /// <summary>
        /// Actualiza el estado de unaa operacion procesada correctamente, registrando la referencia de la transaccion generada y 
        /// la informacion necesaria para su seguimiento.
        /// </summary>
        public async Task ProcesaRegistroExitoso(string transactionId, string folio, string cve_mensaje)
        {
            try
            {
                await _OperacionesTomi.ProcesarCuentasRegistroExitoso(folio, cve_mensaje, transactionId);
                Config.EscribirLog("INFO|ProcesaRegistroExitoso|Se actualizaron los planes de trabajo");
            }
            catch(Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesaRegistroExitoso|{cve_mensaje}| Se presenta error en el folio  {folio}  -  {ex.Message}");
                throw new Exception("Error al registrar éxito en la transacción");
            }
        }

        /// <summary>
        /// Registra en el log la informacion contenida en los objetos de cabecera y detalle de la transaccion para fines se seguimientos durante el proceso.
        /// </summary>
        private async Task ImprimirMasterDetail(MwT24TrxMaestro clsTrxMaster, MwT24TrxDetalle clsTrxDetail)
        {
            Config.EscribirLog($"INFO|Comunes.registra_TRX_WS|Inicia invocación del WS");
            Config.EscribirLog("T24_MW_TrxMaster");
            Config.EscribirLog("T24_MW_TrxMaster.TrxCompany_ID " + clsTrxMaster.STrxCompanyID);
            Config.EscribirLog("T24_MW_TrxMaster.TrxOperation_Acct " + clsTrxMaster.STrxOperationAcct);
            Config.EscribirLog("T24_MW_TrxMaster.TrxOperation_Folio " + clsTrxMaster.STrxOperationFolio);
            Config.EscribirLog("T24_MW_TrxMaster.TrxOperation_ID " + clsTrxMaster.STrxOperationID);
            Config.EscribirLog("T24_MW_TrxMaster.TrxOperation_Specific_Type " + clsTrxMaster.STrxOperationSpecificType);
            Config.EscribirLog("T24_MW_TrxMaster.TrxOperation_SubType " + clsTrxMaster.STrxOperationSubType);
            Config.EscribirLog("T24_MW_TrxMaster.TrxSystem_ID " + clsTrxMaster.STrxSystemID);
            Config.EscribirLog("T24_MW_TrxMaster.TrxTransaction_Type " + clsTrxMaster.STrxTransactionType);
            Config.EscribirLog("T24_MW_TrxMaster.TrxUser " + clsTrxMaster.STrxUser);

            Config.EscribirLog("T24_MW_TrxDetail");
            Config.EscribirLog("T24_MW_TrxDetail.ben_our_charges " + clsTrxDetail.SBen_our_charges);
            Config.EscribirLog("T24_MW_TrxDetail.Beneficiary_Name " + clsTrxDetail.SBeneficiary);
            Config.EscribirLog("T24_MW_TrxDetail.commission_type " + clsTrxDetail.SCommission_type);
            Config.EscribirLog("T24_MW_TrxDetail.credit_acct_no " + clsTrxDetail.SCredit_acct_no);
            Config.EscribirLog("T24_MW_TrxDetail.credit_currency " + clsTrxDetail.SCredit_currency);
            Config.EscribirLog("T24_MW_TrxDetail.credit_their_ref " + clsTrxDetail.SCredit_their_ref);
            Config.EscribirLog("T24_MW_TrxDetail.Cust_Ref1 " + clsTrxDetail.Cust_Ref1);
            Config.EscribirLog("T24_MW_TrxDetail.Cust_Ref2 " + clsTrxDetail.Cust_Ref2);
            Config.EscribirLog("T24_MW_TrxDetail.Cust_Ref3 " + clsTrxDetail.Cust_Ref3);
            Config.EscribirLog("T24_MW_TrxDetail.Cust_Ref4 " + clsTrxDetail.Cust_Ref4);
            Config.EscribirLog("T24_MW_TrxDetail.Cust_Ref5 " + clsTrxDetail.Cust_Ref5);
            Config.EscribirLog("T24_MW_TrxDetail.debit_acct_no " + clsTrxDetail.SDebit_acct_no);
            Config.EscribirLog("T24_MW_TrxDetail.debit_amount " + clsTrxDetail.SDebit_amount);
            Config.EscribirLog("T24_MW_TrxDetail.debit_currency " + clsTrxDetail.SDebit_currency);
            Config.EscribirLog("T24_MW_TrxDetail.debit_their_ref " + clsTrxDetail.SDebit_their_ref);
            Config.EscribirLog("T24_MW_TrxDetail.Instructions " + clsTrxDetail.SInstructions);
            Config.EscribirLog("T24_MW_TrxDetail.ordering_bank " + clsTrxDetail.SOrdering_bank);
            Config.EscribirLog("T24_MW_TrxDetail.ordering_cust " + clsTrxDetail.SOrdering_cust);
            Config.EscribirLog("T24_MW_TrxDetail.related_ref " + clsTrxDetail.SRelated_ref);
            Config.EscribirLog("T24_MW_TrxDetail.Satel_Sys_Ref " + clsTrxDetail.SSatelSysRef);
            Config.EscribirLog("T24_MW_TrxDetail.SW_END2END_REF " + clsTrxDetail.SSW_END2END_REF);
            Config.EscribirLog("T24_MW_TrxDetail.transaction_type " + clsTrxDetail.STransaction_type);
        }

        /// <summary>
        /// Recorre las propiedades de texto del detalle de la transaccion y elimina caracteres especiales que puedan afectar 
        /// la integracion con sistemas externos.
        /// </summary>
        private async Task<MwT24TrxDetalle> LimpiarCamposDetail(MwT24TrxDetalle clsTrxDetail)
        {

            Type type = clsTrxDetail.GetType();
            System.Reflection.PropertyInfo[] propertiesList = type.GetProperties();

            foreach (System.Reflection.PropertyInfo prop in propertiesList)
            {
                if (prop.PropertyType == typeof(string))
                {
                    var value = prop.GetValue(clsTrxDetail, null) as string;

                    if (value != null)
                    {
                        string newValue = value
                            .Replace(",", "?")
                            .Replace("\"", "|")
                            .Replace("/", "^");

                        prop.SetValue(clsTrxDetail, newValue, null);
                    }
                }
            }

            if (clsTrxDetail.SInstructions != null)
            {
                clsTrxDetail.SInstructions = clsTrxDetail.SInstructions.Replace(":", " ");
            }

            return clsTrxDetail;
        }

        /// <summary>
        /// Normaliza los valores contenidos en un documento XML eliminando caracteres especiales que pueden afectar
        /// su procesamiento posterior.
        /// </summary>
        private async Task<string> LimpiarCamposXMLVariables(string des_variables_xml)
        {

            XmlDocument doc = new XmlDocument();
            doc.LoadXml(des_variables_xml);

            foreach (XmlElement el in doc.SelectNodes("//*[not(*)]"))
            {
                el.InnerText = el.InnerText
                    .Replace(",", "?")
                    .Replace("\"", "|")
                    .Replace("/", "^");
            }

            des_variables_xml = doc.OuterXml;

            return des_variables_xml;
        }

        /// <summary>
        /// Recupera el contenido de los nodos pertenecientes a un elemento XML especifico para facilitar
        /// su procesamiento dentro del flujo de negocio.
        /// </summary>
        /// <param name="variablesXML">Estructura XML de origen</param>
        /// <param name="nombre">Nombre del elemento a buscar</param>
        public async Task<List<string>> extraeNodosXML(string variablesXML, string nombre)
        {
            try
            {
                List<string> result = new List<string>();
                System.Xml.XmlDocument loXML_Doc = new System.Xml.XmlDocument();
                System.Xml.XmlNodeList loXML_Nodes;

                loXML_Doc.LoadXml(variablesXML);

                if (!string.IsNullOrEmpty(loXML_Doc.InnerXml))
                {
                    loXML_Nodes = loXML_Doc.GetElementsByTagName(nombre);

                    if (loXML_Nodes.Count > 0)
                    {
                        foreach (System.Xml.XmlNode loXML_Node in loXML_Nodes[0].ChildNodes)
                        {
                            result.Add(loXML_Node.InnerText);
                        }
                    }
                }

                return result;
            }
            catch (Exception lexErrorXML)
            {
                throw new Exception("Error al obtener nodos del XML: " + lexErrorXML.Message);
            }
        }

        /// <summary>
        /// Utilidad para formatear campos de texto de mensajes SWIFT, dividiendo el contenido en multiples lineas con una longitud 
        /// maxima determinada y preservando la integridad de la informacion.
        /// </summary>
        public async Task<List<string>> convierteTexto_a_Multivalor_SinCortar_Local(string texto, int MW_SeparadorMultivalorLength,int TotalLineas = 0,bool Prefix = false)
        {
            string strCadena = "";
            List<string> retorno = new List<string>(); 
            string palabra = string.Empty;
            string contenedor;

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

                TotalLineas = 1;
                return retorno;
            }

            contenedor = "";

            bool blnAux = false;
            int intCont = 0;
            string[] Arreglo = texto.Split(' ');

            foreach (var _ in Arreglo)
            {
                string PalabraTemp = "";

                if (!blnAux)
                {
                    if (Arreglo.Length > (intCont + 1))
                    {
                        if (Arreglo[intCont + 1].StartsWith("-"))
                        {
                            PalabraTemp = Arreglo[intCont];
                            blnAux = false;
                        }
                        else
                        {
                            PalabraTemp = Arreglo[intCont];
                        }
                    }
                    else
                    {
                        PalabraTemp = Arreglo[intCont];
                    }

                    // Trocear palabra si excede longitud
                    if (PalabraTemp.Length >= MW_SeparadorMultivalorLength)
                    {
                        if (contenedor.Length > 0)
                        {
                            retorno.Add(contenedor);
                            contenedor = "";
                        }

                        while (PalabraTemp.Length >= MW_SeparadorMultivalorLength)
                        {
                            string trozo = PalabraTemp.Substring(0, MW_SeparadorMultivalorLength);
                            retorno.Add(trozo);
                            PalabraTemp = PalabraTemp.Remove(0, MW_SeparadorMultivalorLength);
                        }
                    }

                    if (PalabraTemp.Length > 0)
                    {
                        if (contenedor.Length + PalabraTemp.Length + 1 > MW_SeparadorMultivalorLength)
                        {
                            retorno.Add(contenedor);
                            contenedor = PalabraTemp;
                        }
                        else
                        {
                            if (contenedor == "")
                            {
                                contenedor = PalabraTemp;
                            }
                            else
                            {
                                contenedor = contenedor + " " + PalabraTemp;
                            }
                        }
                    }
                }
                else
                {
                    blnAux = false;
                }

                intCont++;
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

            TotalLineas = retorno.Count;

            return retorno;
        }

        /// <summary>
        /// Construye un mensaje unico a apartir de una coleccion de errores, concatenando cada elemento mediante el caracter seperador '|'
        /// </summary>
        /// <param name="arrayError">Coleccion de mensaje que seran agrupados</param>
        private async Task<string> obtieneMensajeError(string[] arrayError)
        {
            string messageGeneralError = string.Empty;

            if (arrayError.Length > 0)
            {
                for (short cnt = 0; cnt <= arrayError.Length - 1; cnt++)
                {
                    messageGeneralError += arrayError[cnt];

                    if (cnt != arrayError.Length - 1)
                    {
                        messageGeneralError += "|";
                    }
                }
            }

            return messageGeneralError;
        }
    }
}

using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Clase base que proporciona acceso a loser servicios, repositorios y configuraciones compartidas requeridas durante 
    /// la ejecucion de las operaciones.
    /// </summary>
    public class OperacionBase
    {
        protected OperacionesSwiftModelo _SwfOperacion;
        protected IOperaciones _OperacionesTomi;
        protected IOperacionesT24 _OperacionesT24;
        protected IOperacionesSPEI _OperacionesSpei;
        protected ConfiguracionModelo _Config;
        protected OperacionesGenerales _comunes;
        protected ConexionT24Mw _ConexionT24Mw;

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Operaciones;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion
{
    public class OperacionServicio
    {
        private readonly IOperaciones _Operaciones;
        private readonly IOperacionesT24 _OperacionesT24;
        private readonly ConfiguracionModelo _Configuracion;
        private readonly ConexionT24Mw _ConexionT24Mw;
        private readonly IOperacionesSPEI _OperacionesSPEI;
        public OperacionServicio(ConfiguracionModelo configuracion, IOperaciones operaciones, IOperacionesT24 operacionesT24, ConexionT24Mw conexionT24Mw, IOperacionesSPEI operacionesSPEI)
        {
            _Configuracion = configuracion;
            _Operaciones = operaciones;
            _OperacionesT24 = operacionesT24;
            _ConexionT24Mw = conexionT24Mw;
            _OperacionesSPEI = operacionesSPEI;
        }

        public async Task EjecutaProcedimientosAsync(CancellationToken ct)
        {
            //Obtener la Fecha de la base de datos, incluida la fecha en formato juliana
            FechaSistemaDto fechaSistema = await _Operaciones.ObtenerFechaJuliana();
            string fechaJulianaT24 = await _OperacionesT24.ObtenerFecha_T24();
            fechaJulianaT24 = (!string.IsNullOrEmpty(fechaJulianaT24)) ? fechaJulianaT24.Substring(2, fechaJulianaT24.Length - 2) : string.Empty;

            Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Fecha del sistema {0}", fechaSistema.Fecha.ToString("yyyyMMdd")));
            Config.EscribirLog(string.Format("INFO|FechaSistemaJuliana={0}", fechaSistema.Fecha_jul));
            Config.EscribirLog(string.Format("INFO|FechaSistema_T24_Juliana={0}", fechaJulianaT24));

            if (!fechaSistema.Fecha_jul.Equals(fechaJulianaT24))
            {
                Config.EscribirLog(string.Format("AVISO|Los Sistemas tienen diferentes fechas. TOMI={0} T24={1}", fechaSistema.Fecha_jul, fechaJulianaT24));
            }

            _Configuracion.IntFechaSistema = Convert.ToInt32(fechaSistema.Fecha.ToString("yyyyMMdd"));
            _Configuracion.FechaJuliana = fechaSistema.Fecha_jul;
            _Configuracion.BoEsDiaHabil = await _Operaciones.EsDiaHabil();

            Dictionary<string, string> dicParametrosSistema = await _Operaciones.parametrosSistema();

            foreach (var paramSistema in dicParametrosSistema)
            {
                if (!Enum.TryParse(paramSistema.Key, out ParametrosSistema parametro)) continue;

                switch (parametro)
                {
                    case ParametrosSistema.HORA_OP_INICIO:
                        _Configuracion.StrHoraOpInicio = TimeSpan.Parse(paramSistema.Value);
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Hora de inicio de operaciones '{0}'. ", paramSistema.Value));
                        break;

                    case ParametrosSistema.HORA_OP_FIN:
                        _Configuracion.StrHoraOpFin = TimeSpan.Parse(paramSistema.Value);
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Hora de Finalización de operaciones '{0}'. ", paramSistema.Value));
                        break;

                    case ParametrosSistema.SWF_CRITERIO_COMPLETO:
                        _Configuracion.StrSwfCriterioCompleto = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Criterio completo '{0}'. ", paramSistema.Value));
                        break;

                    case ParametrosSistema.DIGSIG_CERT_NUMSER:
                        _Configuracion.StrDigSigCertNumSer = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Número de serie de certificado para firma digital '{0}'. ", paramSistema.Value));
                        break;

                    case ParametrosSistema.DIGSIG_CERT_PSW:
                        _Configuracion.StrDigSigCertPsw = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|Password de certificado para firma digital '************'. "));
                        break;

                    case ParametrosSistema.SINFONDOS_HORA_VALIDA:
                        _Configuracion.StrSinFondosHoraValida = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|{0} '{1}'. ", ParametrosSistema.SINFONDOS_HORA_VALIDA.ToString(), paramSistema.Value));
                        break;

                    case ParametrosSistema.SINFONDOS_TIEMPO_ESPERA:
                        _Configuracion.StrSinFondosTiempoEspera = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|{0} '{1}'. ", ParametrosSistema.SINFONDOS_TIEMPO_ESPERA.ToString(), paramSistema.Value));
                        break;

                    case ParametrosSistema.WST24_MW_USERNAME_SWF_MX:
                        _Configuracion.StrNombreSPEI2 = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|{0} '{1}'. ", ParametrosSistema.WST24_MW_USERNAME_SWF_MX.ToString(), paramSistema.Value));
                        break;

                    case ParametrosSistema.WST24_MW_PASSWORD_SWF_MX:
                        _Configuracion.StrWst24MwPasswordSwfMx = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|{0} '************'. ", ParametrosSistema.WST24_MW_PASSWORD_SWF_MX.ToString()));
                        break;

                    case ParametrosSistema.WST24_MW_COMPANY:
                        _Configuracion.StrWsT24MwCompany = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema|{0} '************'. ", ParametrosSistema.WST24_MW_COMPANY.ToString()));
                        break;

                    case ParametrosSistema.COMM_API_URL:
                        _Configuracion.StrCommApiUrl = paramSistema.Value;
                        Config.EscribirLog(string.Format("INFO|obtenerParametrosSistema| Url de la suite de APIS MDW: '{0}'. ", paramSistema.Value));
                        break;
                }
            }

            if (_Configuracion.StrHoraOpInicio == null)
                throw new Exception("No se especifica la hora de inicio de operaciones");

            if (_Configuracion.StrHoraOpFin == null)
                throw new Exception("No se especifica la hora de finalización de operaciones");

            int iPDI = Process.GetCurrentProcess().Id;
            string strProcesoNombre = Process.GetCurrentProcess().ProcessName;

            AlertaDTO alertaSistema = new AlertaDTO()
            {
                TipoOPeracion = null,
                Folio = null,
                Servicio = _Configuracion.NombreServicio,
                ClaveAviso = "CRITICO",
                Descipcion = "Error en ejecuta_procedimientos",
                PId = iPDI,
                NombreProceso = strProcesoNombre,
                Exception = new Exception()
            };

            _Configuracion.StrFechaSistema = TimeSpan.Parse(DateTime.Now.ToString("HH:mm"));
            string lsHoraSistema = _Configuracion.StrFechaSistema.ToString(@"hh\:mm");

            if (_Configuracion.StrFechaSistema >= _Configuracion.StrHoraOpInicio && _Configuracion.StrFechaSistema <= _Configuracion.StrHoraOpFin)
            {
                if (!_Configuracion.BHorarioLectura)
                {
                    _Configuracion.BHorarioLectura = true;
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Inicio horario de operaciones, HoraSistema='{lsHoraSistema}'.", NombreServicio.Swf_IN_FT.ToString());
                }
            }
            else
            {
                if (_Configuracion.BHorarioLectura)
                {
                    _Configuracion.BHorarioLectura = false;
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos| Termina horario de operaciones, HoraSistema='{lsHoraSistema}'.", NombreServicio.Swf_IN_FT.ToString());
                }
            }

            if (!_Configuracion.BHorarioLectura)
            {
                Config.EscribirLog($"INFO|Ejecuta_Procedimientos| El Servicio esta fuera de Horario.", NombreServicio.Swf_IN_FT.ToString());
                alertaSistema.ClaveAviso = "CRITICO";
                alertaSistema.Descipcion = "Error en ejecuta_procedimientos";
                alertaSistema.Exception = new Exception("El servicio '" + alertaSistema.Servicio + "' está fuera de horario.");
                _Operaciones.EnviaAlerta(alertaSistema);
            }

            bool bFechaFutura = false;
            bool bResultado = false;
            try
            {
                if (_Configuracion.BoEsDiaHabil)
                {
                    Config.EscribirLog("INFO|CerrarOperacionesAnteriores|Cerrando operaciones de fechas anteriores");
                    await _Operaciones.CerrarOperacionesAnteriores(Convert.ToInt32(DateTime.Now.ToString("yyyyMMdd")));

                    Config.EscribirLog("INFO|ActualizaPlanTrabajo|Actualizando posibles operaciones autorizadas pero con plan de trabajo inconcluso");
                    await _Operaciones.ActualizaPlanTrabajo();

                    List<OperacionesSwiftModelo> lstOperaciones = await _Operaciones.Busca_Operaciones_FT();

                    foreach (OperacionesSwiftModelo SwiftItem in lstOperaciones)
                    {
                        try
                        {
                            Config.EscribirLog($"INFO|SWF_IN.registra_FT|===================== Inicia análisis de operación ICN={SwiftItem.Folio} =====================");

                            if (SwiftItem.Fec_valor < _Configuracion.IntFechaSistema && (!SwiftItem.Cve_mensaje.Equals(ConstantesServicios.TipoMensajeMT191) && !SwiftItem.Cve_mensaje.Equals(ConstantesServicios.TipoMensajeCAMT106)))
                            {
                                throw new Exception($"Se desea realizar una operación al {SwiftItem.Fec_valor} cuando la fecha del sistema es: {_Configuracion.IntFechaSistema}");
                            }

                            bFechaFutura = (SwiftItem.Fec_valor > _Configuracion.IntFechaSistema);

                            if (bFechaFutura)
                            {
                                Config.EscribirLog($"INFO|SWF_IN.registra_FT|Operación fecha valor futuro.");
                            }
                            else
                            {
                                switch (SwiftItem.Cve_mensaje)
                                {
                                    case ConstantesServicios.TipoMensajeMT103:

                                        if (SwiftItem.Cve_moneda.Equals(ConstantesServicios.ModenaDefaultMN) && SwiftItem.Num_cuenta.Length > 7 && (SwiftItem.Num_cuenta.Substring(0, 3) != ConstantesServicios.ClaveCASFIM.Substring(2, 3)))
                                        {
                                            MT_103_MXN _mt103Mxn = new MT_103_MXN(_Configuracion, _Operaciones, _OperacionesT24, SwiftItem, ConstantesServicios.TipoOperacion);
                                            bResultado = await _mt103Mxn.ProcesarOperacion();
                                        }
                                        else
                                        {
                                            MT_103 _mt103 = new MT_103(_Configuracion, _Operaciones, _OperacionesT24, _ConexionT24Mw, SwiftItem, ConstantesServicios.TipoOperacion);
                                            bResultado = await _mt103.ProcesarOperacion();
                                        }
                                        break;

                                    case ConstantesServicios.TipoMensajeMT191:
                                        MT_191 _mt191 = new MT_191(_Configuracion, _Operaciones, _OperacionesT24, _ConexionT24Mw, SwiftItem, ConstantesServicios.TipoOperacion);
                                        bResultado = await _mt191.ProcesarOperacion();
                                        break;

                                    case ConstantesServicios.TipoMensajeMT202:
                                        MT_202 _mt202 = new MT_202(_Configuracion, _Operaciones, _OperacionesSPEI, SwiftItem, ConstantesServicios.TipoOperacion);
                                        bResultado = await _mt202.ProcesarOperacion();
                                        break;

                                    case ConstantesServicios.TipoMensajeMT210:
                                        MT_210 _mt210 = new MT_210(_Configuracion, _Operaciones, SwiftItem, ConstantesServicios.TipoOperacion);
                                        bResultado = await _mt210.ProcesarOperacion();
                                        break;

                                    case ConstantesServicios.TipoMensajeMT950:
                                        MT_950 _mt950 = new MT_950(_Configuracion, _Operaciones, SwiftItem, ConstantesServicios.TipoOperacion);
                                        bResultado = await _mt950.ProcesarOperacion();
                                        break;

                                    case ConstantesServicios.TipoMensajeCAMT106:
                                        CAMT106 _camt106 = new CAMT106(_Configuracion, _Operaciones, _OperacionesT24, SwiftItem, ConstantesServicios.TipoOperacion);
                                        bResultado = await _camt106.ProcesarOperacion();
                                        break;

                                    case ConstantesServicios.TipoMensajePACS008:
                                        if (SwiftItem.Cve_moneda.Equals(ConstantesServicios.ModenaDefaultMN) && SwiftItem.Num_cuenta.Length > 7 && (SwiftItem.Num_cuenta.Substring(0, 3) != ConstantesServicios.ClaveCASFIM.Substring(2, 3)))
                                        {
                                            PACS008_MXN _pACS008mx = new PACS008_MXN(_Configuracion, _Operaciones, SwiftItem, ConstantesServicios.TipoOperacion);
                                            bResultado = await _pACS008mx.ProcesarOperacion();
                                        }
                                        else
                                        {
                                            PACS008 _pacs008 = new PACS008(_Configuracion, _Operaciones, _OperacionesT24, _ConexionT24Mw, SwiftItem, ConstantesServicios.TipoOperacion);
                                            bResultado = await _pacs008.ProcesarOperacion();
                                        }
                                        break;

                                    case ConstantesServicios.TipoMensajePACS009:
                                        PACS009 _pacs009 = new PACS009(_Configuracion, _Operaciones, _OperacionesSPEI, SwiftItem, ConstantesServicios.TipoOperacion);
                                        bResultado = await _pacs009.ProcesarOperacion();
                                        break;

                                    default:
                                        throw new Exception($"Mensaje ' {SwiftItem.Cve_mensaje} ' no tiene logica de procesamiento");
                                }
                            }

                            Config.EscribirLog($"INFO|SWF_IN.registra_FT|===================== Se termina el procesamiento del ICN: {SwiftItem.Folio}");
                            
                        }
                        catch (Exception ex)
                        {
                            Config.EscribirLog($"ERROR|SWF_IN.registra_FT| {ex.ToString()}");
                            await _Operaciones.Marca_Op_Con_Error(SwiftItem.Folio, ex.Message);
                        }
                    }
                }
                else
                {
                    Config.EscribirLog("ERROR|Ejecuta_Procedimientos|Hoy es un día inhábil.");
                }
            }
            catch (Exception e)
            {
                Config.EscribirLog(String.Format("ERROR|Ejecuta_Procedimientos|Se presenta excepcion: {0}", e.Message));

                alertaSistema.ClaveAviso = "CRITICO";
                alertaSistema.Descipcion = "Error en ejecuta_procedimientos";
                alertaSistema.Exception = e;
                _Operaciones.EnviaAlerta(alertaSistema);
            }
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo MT103 ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class MT_103 : OperacionBase
    {
        string strTipoOperacion = string.Empty;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="MT_103"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public MT_103(ConfiguracionModelo configuracion, IOperaciones operacionesTomi, IOperacionesT24 operacionesT24, ConexionT24Mw conexionT24Mw, OperacionesSwiftModelo swiftOperacion, string TipoOperacion)
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _OperacionesT24 = operacionesT24;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
            _ConexionT24Mw = conexionT24Mw;
        }

        /// <summary>
        /// Implementa la logica de procesamiento correspondiente al tipo de operacion definido, coordinando la ejecucion de validaciones,
        /// consultas e integraciones requeridas por el flujo de negocio.
        /// </summary>
        public async Task<bool> ProcesarOperacion()
        {
            OperacionesGenerales _comunes = new OperacionesGenerales(_Config, _OperacionesTomi, _ConexionT24Mw);

            bool bRegistro = false;
            bool bProcesado = false;

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación, folio_unico = {_SwfOperacion.Folio_unico}.");

            string strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            if (strAliasProceso == ConstantesServicios.AliasTomiEtradasSalidas)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A realizar el registro de E/S");
                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                TombinEntradasSalidasModelo DatosE_S = new TombinEntradasSalidasModelo()
                {
                    Referencia = _SwfOperacion.Folio_unico,
                    Num_Cliente = _SwfOperacion.Num_cliente,
                    Ordenante = _SwfOperacion.Nombre_cliente,
                    Operacion = Convert.ToDecimal(ConstantesServicios.Operacion218),
                    OperacionDesc = ConstantesServicios.OperacionDescEntradaTTS,
                    Tipo = ConstantesServicios.OpTipoInternacional,
                    TipoDesc = ConstantesServicios.OpTipoInternacionalDesc,
                    Cuenta = Convert.ToDecimal(_SwfOperacion.Num_cuenta),
                    Nombre = _SwfOperacion.Nombre_cliente,
                    CuentaMoneda = (_SwfOperacion.Cve_moneda.Equals("MXP") || _SwfOperacion.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : _SwfOperacion.Cve_moneda,
                    Via = Convert.ToDecimal(ConstantesServicios.ViaSubAplicacion810),
                    ViaDesc = ConstantesServicios.ViaSubAplicacion810Desc,
                    Importe = _SwfOperacion.Des_importe,
                    Saldo = 0,
                    Comentarios = string.Empty,
                    Fecha_Captura = DateTime.Now.Date,
                    FechaValor = DateTime.ParseExact(_Config.IntFechaSistema.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                    Hora = DateTime.Now.ToString("hh:mm:ss AMPM"),
                    Capturista = ConstantesServicios.CapturistaJeai,
                    Fad = 0,
                    Referencia_Control = string.Empty,
                    Referencia_Control_Cont = 0
                };

                bRegistro = await _comunes.Registra_E_S(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso, DatosE_S);

                if (bRegistro == false)
                    throw new Exception("No se pudo registrar la operación como entrada/salida");

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Se realizó el registro de E/S existosamente.");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Se termina el registro E/S e inicia siguiente tarea.");

                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);
            }

            if (strAliasProceso == ConstantesServicios.AliasT24Registro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar operación en T24.");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                SalidaT24RegistroModelo respuestaT24 = await _comunes.T24_Registro(_SwfOperacion.Folio, _SwfOperacion);
                bProcesado = respuestaT24.BCorrecto;

                if (bProcesado == false && !string.IsNullOrEmpty(respuestaT24.StrMensajeError))
                    throw new Exception(respuestaT24.StrMensajeError);

                if (bProcesado == false)
                    throw new Exception($"Error en procesamiento de resgitro MT-103 T24_registro con folio {_SwfOperacion.Folio}");
            }

            if (strAliasProceso == ConstantesServicios.AliasConciliar)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A conciliar operaciones (por estatus) entre TOMI y T24");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                //Se busca la operación en T24
                string strFolioT24 = await _comunes.Existe_Registro(_SwfOperacion.Folio_unico);

                bool EsIgualEnT24 = false;

                if (string.IsNullOrEmpty(strFolioT24))
                    throw new Exception($"No existe folio asignado para el folio_unico={_SwfOperacion.Folio_unico}");

                //Valida contra los datos de Tomi - T24
                string _moneda = (_SwfOperacion.Cve_moneda.Equals("MXP") || _SwfOperacion.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : _SwfOperacion.Cve_moneda;
                string _CuentaDeb = string.Empty;
                string _CuentaCred = string.Empty;

                if (_moneda.Equals(ConstantesServicios.ModenaDefaultMN))
                {
                    _CuentaDeb = (_SwfOperacion.Nf_vostro_emisor.Length >= 7) ? _SwfOperacion.Nf_vostro_emisor.Substring(_SwfOperacion.Nf_vostro_emisor.Length - 7) : _SwfOperacion.Nf_vostro_emisor;
                    _CuentaCred = _SwfOperacion.Num_cuenta;

                    if (_CuentaCred.Length == 18)
                        _CuentaCred = _CuentaCred.Substring(11, 7);
                    else
                        throw new Exception("La cuenta a acreditar no es válida");
                }
                else
                {
                    _CuentaDeb = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.DebitAccount);
                    _CuentaCred = _SwfOperacion.Num_cuenta;
                }

                T24FundsTransferModelo _objT24 = await _OperacionesT24.ConsultarFoliSwiftT24(strFolioT24);

                if (_objT24 != null)
                {
                    if (strFolioT24.Equals(_objT24.Cur_Id)
                        && _SwfOperacion.Folio_unico.Equals(_objT24.Satel_sys_ref)
                        && _SwfOperacion.Des_importe.ToString().Equals(_objT24.Debit_Amount)
                        && _moneda.Equals(_objT24.Debit_Currency)
                        && _CuentaDeb.Equals(_objT24.Debit_acct_no)
                        && _CuentaCred.Equals(_objT24.Credit_acct_no))
                    {
                        EsIgualEnT24 = true;
                    }
                }

                if (EsIgualEnT24 == false)
                    throw new Exception("Los datos de conciliacion entre TOMI y T24 son diferentes");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);
                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
            }

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Fin del Registro de la operación");
            return true;
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo MT103 MXN, ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class MT_103_MXN : OperacionBase
    {
        string strTipoOperacion = string.Empty;
        string strError = string.Empty;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="MT_103_MXN"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public MT_103_MXN(ConfiguracionModelo configuracion, IOperaciones operacionesTomi, IOperacionesT24 operacionesT24, OperacionesSwiftModelo swiftOperacion, string TipoOperacion)
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _OperacionesT24 = operacionesT24;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
        }

        /// <summary>
        /// Implementa la logica de procesamiento correspondiente al tipo de operacion definido, coordinando la ejecucion de validaciones,
        /// consultas e integraciones requeridas por el flujo de negocio.
        /// </summary>
        public async Task<bool> ProcesarOperacion()
        {
            _comunes = new OperacionesGenerales(_Config, _OperacionesTomi);
            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación, folio_unico = {_SwfOperacion.Folio_unico}.");

            bool bProcesado = false;
            string strAliasProceso = string.Empty;
            strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            if (strAliasProceso == ConstantesServicios.AliasTomiEtradasSalidas || strAliasProceso == ConstantesServicios.AliasT24Registro || strAliasProceso == ConstantesServicios.AliasConciliar)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Registrar Operacion en TOMI.");
                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                if (strAliasProceso == ConstantesServicios.AliasTomiEtradasSalidas)
                {
                    long llFolioMW = 0;
                    bProcesado = await TOMI_REGISTRO(_SwfOperacion.Folio, _SwfOperacion, llFolioMW);

                    if (bProcesado == false && !string.IsNullOrEmpty(strError))
                        throw new Exception(strError);

                    if (!bProcesado)
                        throw new Exception($"Error en procesamiento de registro TOMI_REGISTRO con folio {_SwfOperacion.Folio}");

                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Fin del registro de la operación.");
                    await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);
                }

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, ConstantesServicios.AliasT24Registro);
                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, ConstantesServicios.AliasT24Registro);

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, ConstantesServicios.AliasConciliar);
                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, ConstantesServicios.AliasConciliar);

                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
                strAliasProceso= string.Empty;
            }

            return true;
        }


        /// <summary>
        /// Ejecuta el proceso de registro de una operacion, obteniendo la informacion necesaria desde el mensaje XML, validando cuentas involucradas 
        /// y determinando reglas aplicables al tratamiento de comisiones y cargos.
        /// </summary>
        /// <param name="icn">Folio identificador</param>
        /// <param name="datos">Datos de la operacion</param>
        /// <param name="llFolioMW">Folio de seguimiento</param>
        public async Task<bool> TOMI_REGISTRO(string icn, OperacionesSwiftModelo datos, long llFolioMW)
        {
            try
            {
                string strCuentaCompletaVostro;
                string[] strCuentaSeparadaVostro;
                string strCuentaPrincipal;
                string strCuentaComisiones;
                string strDescripcion;
                string strDetailCharges;
                string strNumeroCliente = string.Empty;
                string strNombreCliente = string.Empty;
                string strNombreCuenta = string.Empty;
                bool bCuentaT24 = false;
                int iTipoAppAnterior = 0;
                string strTipoDesc;
                string strNombreBancoBene;
                bool bActiveAutomation = false;

                string strBeneficiario = (await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BeneficiarioXml)).Trim();

                if (string.IsNullOrEmpty(strBeneficiario))
                    strBeneficiario = (await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.Beneficiario2Xml)).Trim();

                if (strBeneficiario.Length > 100)
                    strBeneficiario = strBeneficiario.Substring(0, 100);

                strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                strCuentaPrincipal = strCuentaSeparadaVostro[2];
                strCuentaComisiones = strCuentaPrincipal;

                strDescripcion = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.ConceptoXml);
                strDetailCharges = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DetalleCargosXml);

                //Datos de lacuenta en T24
                T24CustomerAccountModelo cuentaT24 = await _OperacionesT24.Datos_Cuenta_VW(strCuentaPrincipal);

                if (cuentaT24 == null)
                {
                    throw new Exception($"No se encontró o está inactiva la cuenta en T24 {strCuentaPrincipal}");
                }

                strNumeroCliente = cuentaT24.Customer;
                strNombreCliente = cuentaT24.Name_1;
                strNombreCuenta = cuentaT24.Account_title_1;

                //Valido las comisiones o tipo de operación
                if ((await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftEmisor)).Equals(ConstantesServicios.OrdenanteBotKus33))
                {
                    //es NY

                    if (strDetailCharges.Equals(ConstantesServicios.DetalleCargoOUR))
                    {
                        Config.EscribirLog($"INFO|{strTipoOperacion}.TOMI_REGISTRO|Es registro de NY, swift='BOTKUS33' y DetailCharges='OUR'");
                        iTipoAppAnterior = int.Parse(ConstantesServicios.ViaSubAplicacion804);
                        strTipoDesc = ConstantesServicios.ViaSubAplicacion804Desc;
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|BTMU NY, comisión diferente a OUR");
                        iTipoAppAnterior = int.Parse(ConstantesServicios.ViaSubAplicacion800);
                        strTipoDesc = ConstantesServicios.ViaSubAplicacion800Desc;
                    }
                }
                else
                {
                    //No es NY
                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Subaplicación 800");
                    if (strDetailCharges.Equals(ConstantesServicios.DetalleCargoOUR))
                    {
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|No es NY, comisión OUR.");
                        iTipoAppAnterior = int.Parse(ConstantesServicios.ViaSubAplicacion800);
                        strTipoDesc = ConstantesServicios.ViaSubAplicacion800Desc;
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|No es NY, comisión diferente a our");
                        iTipoAppAnterior = int.Parse(ConstantesServicios.ViaSubAplicacion800);
                        strTipoDesc = ConstantesServicios.ViaSubAplicacion800Desc;
                    }
                }

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Inicia registro en base TOMI con llamado 'usp_TOMBIN_INSERT_FORMATO_FXD_LOCAL'");
                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|T24_Customer_Code={strNumeroCliente}");
                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|A buscar el nombre del banco para la cuenta CLABE {datos.Num_cuenta}");

                strNombreBancoBene = await _OperacionesTomi.Dame_Banco(datos.Num_cuenta);

                if (string.IsNullOrEmpty(strNombreBancoBene))
                    throw new Exception($"El banco asociado a la cuenta: {datos.Num_cuenta} no existe");

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Banco={strNombreBancoBene}");

                bActiveAutomation = await _OperacionesTomi.AutomationIsActive();

                string status = (bActiveAutomation == true) ?
                                ((Convert.ToDecimal(DateTime.Now.Date.ToString("yyyyMMdd")) <= datos.Fec_valor) ? ConstantesServicios.EstatusScnp : ConstantesServicios.EstatusConf)
                                : ConstantesServicios.EstatusT24Pend;

                FormatoFxdLocalModelo formatoFxdLocal = new FormatoFxdLocalModelo()
                {
                    NF_FOLIO_UNICO = datos.Folio_unico,
                    NF_TIPO = ConstantesServicios.FXD,
                    NF_FOLIO = null,
                    T24_CUSTOMER_CODE = strNumeroCliente,
                    T24_DEBIT_ACCT_NO = strCuentaPrincipal,
                    NF_ESTATUS = status,
                    NF_USUARIO_CAPTURA = ConstantesServicios.CapturistaJeai,
                    NF_USUARIO_AUTORIZA = (bActiveAutomation == true) ? ConstantesServicios.UsuarioTomi : null,
                    NF_FECHA_CAPTURA = DateTime.Now,
                    NF_FECHA_AUTORIZA = bActiveAutomation ? (DateTime?)DateTime.Now : null,
                    T24_DEBIT_AMOUNT = datos.Des_importe,
                    T24_SHORT_NAME = strNombreCliente,
                    NF_FOREX_RELACIONADO = "",
                    T24_DEBIT_CURRENCY = Moneda.MXN.ToString(),
                    NF_BANCO_BENEF = strNombreBancoBene,
                    T24_BEN_ACCT_NO = datos.Num_cuenta,
                    NF_TIPO_CUENTA = ConstantesServicios.TipoCuentaCLABE,
                    T24_BEN_CUST_NAME = strBeneficiario,
                    NF_RFC_BENEFICIARIO = null,
                    NF_TASA_RETENCION = null,
                    NF_FECHA_EJECUCION = Convert.ToInt32(datos.Fec_valor),
                    T24_DEBIT_THEIR_REF = icn,
                    NF_MONTO_ENVIAR = datos.Des_importe,
                    ID_PLANTILLA = null,
                    ID_FORMATO_PLANTILLA = null,
                    Salida = 0
                };

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|usp_INSERT_FORMATO_FXD_LOCAL pasaron parametros, inicia ejecución");

                SalidaFXDLocalModelo salida = await _OperacionesTomi.Insert_Formato_FXD_local(formatoFxdLocal);

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Paso la ejecución, Salida={salida.Salida}, folioRegreso={salida.Folio}");

                if (salida.Salida != 0 || salida.Folio.Equals(0))
                    throw new Exception($"No se pudo registrar la Operacion como FXD_Local");

                await _OperacionesTomi.ActualizaDatosOperacion(icn, Convert.ToInt32(salida.Folio), strDetailCharges);

                TombinEntradasSalidasModelo datosE_S = new TombinEntradasSalidasModelo()
                {
                    Referencia = datos.Folio_unico,
                    Num_Cliente = Convert.ToInt32(strNumeroCliente),
                    Ordenante = strNombreCliente,
                    Operacion = Convert.ToDecimal(ConstantesServicios.Operacion218),
                    OperacionDesc = ConstantesServicios.OperacionDescSalidaTTS,
                    Tipo = ConstantesServicios.TipoDomestica,
                    TipoDesc = ConstantesServicios.TipoDomesticaDesc,
                    Cuenta = Convert.ToDecimal(strCuentaPrincipal),
                    Nombre = strNombreCuenta,
                    CuentaMoneda = Moneda.MXN.ToString(),
                    Via = iTipoAppAnterior,
                    ViaDesc = strTipoDesc,
                    Importe = datos.Des_importe,
                    Saldo = 0,
                    Comentarios = "",
                    Fecha_Captura = DateTime.Now.Date,
                    FechaValor = Convert.ToDateTime(_Config.StrFechaSistema),
                    Hora = DateTime.Now.ToString("hh:mm:ss AMPM"),
                    Capturista = ConstantesServicios.CapturistaJeai,
                    Fad = 0,
                    Referencia_Control = "",
                    Referencia_Control_Cont = 0
                };

                bool bRegistrado = await _comunes.Registra_E_S(icn, ConstantesServicios.TipoOperacion, "", datosE_S);

                if (bRegistrado == false)
                    throw new Exception($"No se pudo registrar la operación como entrada/salida");

                llFolioMW = salida.Folio;

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Se presenta excepción: {ex.Message}");
            }

            return false;
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Microsoft.SqlServer.Server;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo MT191, ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class MT_191 : OperacionBase
    {
        string strTipoOperacion = string.Empty;
        string strError = string.Empty;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="MT_191"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public MT_191(ConfiguracionModelo configuracion, IOperaciones operacionesTomi, IOperacionesT24 operacionesT24, ConexionT24Mw conexionT24Mw, OperacionesSwiftModelo swiftOperacion, string TipoOperacion)
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _OperacionesT24 = operacionesT24;
            _ConexionT24Mw = conexionT24Mw;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
        }


        /// <summary>
        /// Implementa la logica de procesamiento correspondiente al tipo de operacion definido, coordinando la ejecucion de validaciones,
        /// consultas e integraciones requeridas por el flujo de negocio.
        /// </summary>
        public async Task<bool> ProcesarOperacion()
        {
            _comunes = new OperacionesGenerales(_Config, _OperacionesTomi);

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación, folio_unico = {_SwfOperacion.Folio_unico}.");

            string strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A procesar el paso {strAliasProceso}.");

            if (strAliasProceso == ConstantesServicios.AliasT24Registro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Registrar Operación en T24");
                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                Result<string> result = Result<string>.Failed(string.Empty);
                Result<string> alreadyInsertedInT24 = await this.GetFT(_SwfOperacion.Folio);

                if (alreadyInsertedInT24.IsSuccess)
                {
                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|La búsqueda de FT fue exitosa");

                    if (string.IsNullOrWhiteSpace(alreadyInsertedInT24.Data))
                    {
                        Result<string> resultT24Insert = await T24_Registro(_SwfOperacion.Folio, _SwfOperacion);

                        if (!resultT24Insert.IsSuccess)
                        {
                            if (resultT24Insert.ErrorCode != ErrorCodeEnum.NoCode)
                            {
                                await _OperacionesTomi.Marca_Op_Con_Error(_SwfOperacion.Folio, result.Reason);
                                throw new Exception($"Error en procesamiento de registro MT-191 T24_REGISTRO con folio: {_SwfOperacion.Folio}");
                            }
                            else
                            {
                                throw new Exception($"Error en procesamiento de registro MT-191 T24_REGISTRO con folio: {_SwfOperacion.Folio}");
                            }
                        }

                        Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|No se encontró ningun FT, se procede a hacer el registro en T24");
                        result = resultT24Insert;
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Se encontró un FT asociado al Mensaje: {alreadyInsertedInT24}");
                        result = alreadyInsertedInT24;
                    }
                }

                Result<string> updatedCommission = await ActualizaComisionContext(_SwfOperacion.Folio, result.Data);
                if (!updatedCommission.IsSuccess)
                {
                    throw new Exception($"No se pudo enviar el FT asociado al cobro de esta comision: {_SwfOperacion.Folio}  " +
                        $"Razon: {updatedCommission.Reason}");
                }

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);
                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);
            }

            if (strAliasProceso == ConstantesServicios.AliasSPEIRegistro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar operación en SPEI");
                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                bool procesadoSpei = await this.Registra_SPEI(_SwfOperacion.Folio, _SwfOperacion);

                if (procesadoSpei == false && !string.IsNullOrEmpty(strError))
                    throw new Exception(strError);

                if (procesadoSpei == false)
                    throw new Exception($"No se pudo registrar la operación en SPEI");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, ConstantesServicios.AliasSPEIRegistro);
                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);
            }

            if (strAliasProceso == ConstantesServicios.AliasSwiftRegistro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A generar el mensaje MT-202");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                Result<string> dispatChResult = await DispatchMT202(_SwfOperacion.Folio);

                if (!dispatChResult.IsSuccess)
                {
                    Config.EscribirLog($"ERROR|{strTipoOperacion}.ProcesaOperacion|{dispatChResult}");
                    await _OperacionesTomi.Marca_Op_Con_Error(_SwfOperacion.Folio, "No se pudo enviar el MT202 asociado al cobro de esta comision");
                    throw new Exception($"No se pudo enviar el MT202 asociado al cobro de esta comision: {_SwfOperacion.Folio}");
                }

                return false;
            }

            strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            if (strAliasProceso == ConstantesServicios.AliasConciliar)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A conciliar operaciones (por estatus) Entre TOMI y T24");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);
                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);
                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
            }

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Fin del Registro de la Operación");
            return true;
        }

        /// <summary>
        /// Construye y registra una transaccion de comision en T24 a partir de la informacion de la operacion procesada,
        /// generando las estucturas requeridas para el registro contable y operativo.
        /// </summary>
        /// <param name="folio">Folio de la operacion</param>
        /// <param name="swfOperacion">Datos de la operacion necesarios para la generacion de la transaccion</param>
        private async Task<Result<string>> T24_Registro(string folio, OperacionesSwiftModelo swfOperacion)
        {
            RegistroT24Modelo resp = new RegistroT24Modelo();
            string xmlData = swfOperacion.Nf_variables_xml;
            Config.EscribirLog($"INFO|{strTipoOperacion}.T24_REGISTRO|Inicia registro en T24.");
            InternationalCommission.InternationalCommission commission = InternationalCommission.InternationalCommission.Map(xmlData);

            MwT24TrxMaestro loTrxMaster = new MwT24TrxMaestro()
            {
                STrxOperationSpecificType = ConstantesServicios.TipoOperacion,
                STrxOperationSubType = ConstantesServicios.TipoMensajeMT191.Replace("-", ""),
                STrxOperationFolio = folio,
                STrxOperationAcct = commission.DebitAccountNumber
            };

            StringBuilder instructions = new StringBuilder($"COMISION BANCO BENEFICIARIO{Environment.NewLine}");
            instructions.Append($"{commission.OriginalReference}\t");
            instructions.Append($"{commission.OriginalDate}\t");
            instructions.Append($"COMM:\t{commission.CreditAmount.ToString("C", CultureInfo.CreateSpecificCulture("en-US"))}");
            instructions.Append($"\t{commission.CreditCurrency}");

            List<OperacionesEntidad> cuentas = await _ConexionT24Mw.ObtieneDatosCuentasNostro();
            OperacionesEntidad cuenta = cuentas.Where(c => c.Moneda.ToLower().Equals(commission.CreditCurrency.ToLower())).FirstOrDefault();

            MwT24TrxDetalle loTrxDetailFT = new MwT24TrxDetalle()
            {
                SCredit_acct_no = (cuenta != null) ? cuenta.Cuenta : string.Empty,
                SCredit_currency = commission.CreditCurrency,
                SCredit_amount = commission.CreditAmount.ToString(),
                SDebit_acct_no = commission.DebitAccountNumber,
                SOrdering_cust = commission.OrderingCustomer,
                SInstructions = instructions.ToString(),
                SSatelSysRef = swfOperacion.Folio_unico,
                SFolio_tomi = swfOperacion.Folio_unico
            };

            return await _comunes.registra_TRX_WS_Result(folio, strTipoOperacion, loTrxMaster, loTrxDetailFT, swfOperacion.Nf_variables_xml);

        }


        /// <summary>
        /// Actualiza el contexto de cobro de comisiones asociando la referencia de T24 con la referencia original de la operacion.
        /// </summary>
        /// <param name="originalReference">Referencia original de la operacion</param>
        /// <param name="ourReferencia">Referencia generada en T24</param>
        public async Task<Result<string>> ActualizaComisionContext(string originalReference, string ourReferencia)
        {
            if (string.IsNullOrEmpty(ourReferencia))
            {
                Config.EscribirLog("ERROR|MT191.ActualizaComisionContext|No T24 reference passed to update commission context.");
                return Result<string>.Failed("Error: No T24 reference passed to update commission context.");
            }

            if (string.IsNullOrEmpty(originalReference))
            {
                Config.EscribirLog("ERROR|MT191.ActualizaComisionContext|No SAMH reference passed to update commission context");
                return Result<string>.Failed("Error: No SAMH reference passed to update commission context.");
            }

            try
            {
                Config.EscribirLog("INFO|MT_191.ActualizaComisionContext|To update the Commission Context with the T24 reference");

                ChargesPaymentUpdateDto creditConfirmation = new ChargesPaymentUpdateDto()
                {
                    OriginalReference = originalReference,
                    T24Reference = ourReferencia
                };

                string json = Newtonsoft.Json.JsonConvert.SerializeObject(creditConfirmation);

                Config.EscribirLog($"INFO|MT_191.ActualizaComisionContext|Payload: \n {json}");

                using (var _client = new HttpClient())
                {
                    StringContent _content = new StringContent(json, Encoding.UTF8, "application/json");

                    Config.EscribirLog($"INFO|MT_191.ActualizaComisionContext|To update Commission Context with context ref {creditConfirmation.OriginalReference}");

                    HttpResponseMessage response = _client.PutAsync(_Config.StrCommApiUrl + "/confirmation", _content).Result;

                    Config.EscribirLog($"INFO|MT_191.ActualizaComisionContext|Respuesta del servidor:  {response.ReasonPhrase}");

                    if (response.StatusCode == System.Net.HttpStatusCode.Accepted)
                        return Result<string>.Success(response.ReasonPhrase);
                    else
                        return Result<string>.Failed($"Error: {response.ReasonPhrase}");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("INFO|MT191.DispatchMT202|An error occurred: " + ex.Message);
                return Result<string>.Failed($"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Solicita la generacion del mensaje MT202 requerido para concluir el proceso de cobor de comisión
        /// </summary>
        public async Task<Result<string>> DispatchMT202(string folio)
        {
            try
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.DispatchMT202|A enviar MT202 para terminar el cobro de la comision.");
                FiToFiCommissionCreditConfirmationDto creditConfirmation = new FiToFiCommissionCreditConfirmationDto()
                {
                    TheirReference = folio
                };

                string json = JsonConvert.SerializeObject(creditConfirmation);

                Config.EscribirLog($"INFO|{strTipoOperacion}.DispatchMT202|PayLoad: \n {json}");

                using (var _client = new HttpClient())
                {
                    StringContent _content = new StringContent(json, Encoding.UTF8, "application/json");

                    Config.EscribirLog($"INFO|MT_191.DispatchMT202|To enqueue dispatching of MT202 with context ref {creditConfirmation.TheirReference}");

                    HttpResponseMessage response = _client.PutAsync(_Config.StrCommApiUrl + "/confirmation", _content).Result;

                    Config.EscribirLog($"INFO|MT_191.DispatchMT202|Respuesta del servidor:  {response.ReasonPhrase}");

                    if (response.StatusCode == System.Net.HttpStatusCode.Accepted)
                        return Result<string>.Success(response.ReasonPhrase);
                    else
                        return Result<string>.Failed(response.ReasonPhrase);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|MT_191.DispatchMT202|An error occurred: {ex.Message}");
                return Result<string>.Failed(ex.Message);
            }
        }


        /// <summary>
        /// Ejecuta el proceso de registro de una operacion SPEI, validando el estado de la transaccion, determinando el tipo de paogo correspondiente, generando la informacion
        /// requerdia para el elnvio y almacenado de la operacion en el sitema SPEDI.
        /// </summary>
        /// <param name="icn">Folio indentificador</param>
        /// <param name="swfOperacion">Datos de la operacion</param>
        public async Task<bool> Registra_SPEI(string icn, OperacionesSwiftModelo swfOperacion)
        {
            ComunesFXD _fx = new ComunesFXD(_OperacionesTomi, _Config);
            Config.EscribirLog("INFO|registra_SPEI|A registrar pago SPEI.");
            string registroSPEI = await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.BtmumRegistroSPEI);

            if (registroSPEI.Equals(ConstantesServicios.EstatusPROC))
            {
                Config.EscribirLog("INFO|registra_SPEI|El pago ya fue registrado en SPEI.");
                return true;
            }

            int iTipoPago = 3;
            int iTipoCuenta = 4;
            Config.EscribirLog($"INFO|registra_SPEI|A registrar en SPEI para el tipo de pago= {iTipoPago}");

            switch (iTipoPago)
            {
                case 3:
                    Config.EscribirLog($"INFO|registra_SPEI|Tercero a tercero vostro.");
                    break;
                case 5:
                    Config.EscribirLog($"INFO|registra_SPEI|Participante a tercero.");
                    break;
                case 6:
                    Config.EscribirLog($"INFO|registra_SPEI|Participante a vostro.");
                    break;
                case 7:
                    Config.EscribirLog($"INFO|registra_SPEI|Participante a participante.");
                    break;
                default:
                    throw new Exception($"No existe definición para pago SPEI del tipo de pago {iTipoPago}");
            }

            string ReferenciaNumerica = ConstantesServicios.CASFIM_MUFG;

            if (await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml) != ""
                  && double.TryParse(await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml), out _))
            {
                string valorTmp = await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml);
                if (!string.IsNullOrEmpty(valorTmp) && decimal.TryParse(valorTmp, out _))
                {
                    if (valorTmp.Length > 7)
                    {
                        valorTmp = valorTmp.Substring(Math.Max(0, valorTmp.Length - 7));
                    }

                    ReferenciaNumerica = valorTmp;
                }
            }

            string sConceoto = "FROM" + await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml);

            if (await _fx.GeneraFirmaDigital(swfOperacion, iTipoPago, ConstantesServicios.TablaTrabajoMx, icn) == false)
                throw new Exception($"Se presentó un error al generar la firma digital del folio: {icn}");

            string InsClave = await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml);
            InterfaceSPEIModelo insertSpei = new InterfaceSPEIModelo
            {
                ins_clave = Convert.ToInt64(InsClave),
                op_monto = swfOperacion.Des_importe,
                op_cve_rastreo = await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml)
            };

            string Nombrebeneficiario = await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml);
            string benINtCif = await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.BenIntCif);

            if (iTipoPago != 7)
            {
                if (string.IsNullOrEmpty(benINtCif))
                {
                    CIFSwiftModelo rsDatos = await _OperacionesTomi.ConsultarNombreCIF(benINtCif, 2);

                    if (rsDatos != null)
                    {
                        Nombrebeneficiario = rsDatos.Des_nombre;
                    }
                }

                insertSpei.op_nom_ben = await _fx.texto_SPEI(Nombrebeneficiario);
                insertSpei.tc_clave_ben = iTipoCuenta;
                insertSpei.op_cuenta_ben = await _comunes.Buscar_Valor_Xml(swfOperacion.Nf_variables_xml, ConstantesServicios.BenIntAccountXml);
                insertSpei.op_rfc_curp_ben = ConstantesServicios.ND;
            }

            insertSpei.op_concepto_pago = await _fx.texto_SPEI(sConceoto);
            insertSpei.op_ref_numerica = Convert.ToInt32(ReferenciaNumerica);
            insertSpei.usu_clave_int = 2;

            string strFirmaDigital = string.Empty;
            FirmaElectronicaDto rsDatosFirma = await _OperacionesTomi.ConsultarFirmaElectronica(icn);
            if (rsDatosFirma != null)
            {
                if (!string.IsNullOrEmpty(rsDatosFirma.Op_fimra_dig))
                    strFirmaDigital = rsDatosFirma.Op_fimra_dig;
            }

            if (string.IsNullOrEmpty(strFirmaDigital))
                throw new Exception("No fue posible obtener el campo de firma digital");

            insertSpei.op_firma_dig = strFirmaDigital;

            //Termina Definición de Parametros y hace llamado
            Config.EscribirLog("INFO|registra_SPEI|Pasaron los parametros.");

            string NombreSP = $"{ProcedimientoAlmacenado.sp_InterfaceTomiSpeiDinamico}{iTipoPago}";
            string strCadenaRegreso = string.Empty;

            SalidaSPEIModelo salidaSPEI = await _OperacionesSpei.EjecutarSP_InterfaceTOMI_SPEI(NombreSP, insertSpei);

            Config.EscribirLog("INFO|registra_SPEI|Se procesó procedimientos si errores.");

            if (salidaSPEI != null)
            {
                strCadenaRegreso = $"{salidaSPEI.op_error}|{salidaSPEI.op_clave}|{salidaSPEI.op_fecha_oper}|{salidaSPEI.op_folio}|{salidaSPEI.op_descripcion}";
            }

            if (strCadenaRegreso.StartsWith("0|") || strCadenaRegreso.Contains("Ya existe una operación"))
            {
                XDocument xml = XDocument.Parse(swfOperacion.Nf_variables_xml);
                XmlHelper.AgregarElemento(xml, ConstantesServicios.BtmumRegistroSPEI, ConstantesServicios.EstatusPROC, null);
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Obtiene la referencia de la transaccion generada en T24 para la operacion indicada, permitiendo validar si el 
        /// registrado ya fue procesado.
        /// </summary>
        public async Task<Result<string>> GetFT(string rfk)
        {
            try
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.GetFT|A buscar FT para la comisión.");
                SwfPlanTrabajoMestroModelo resultado = await _OperacionesTomi.ConsultarExisteFT(rfk);
                Config.EscribirLog($"INFO|{strTipoOperacion}.GetFT|Se ejecutó el Query");

                if (string.IsNullOrEmpty(resultado.T24_Ref))
                    return Result<string>.Success(string.Empty);

                return Result<string>.Success(resultado.T24_Ref);

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.GetFT|Se Presenta la Excepcion {ex.Message}");
                return Result<string>.Failed(ex.Message);
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.FirmaElectronica;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Modelos;
using System;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo MT202, ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class MT_202 : OperacionBase
    {
        string strTipoOperacion = string.Empty;
        string strError = string.Empty;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="MT_202"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public MT_202(ConfiguracionModelo configuracion, IOperaciones operacionesTomi, IOperacionesSPEI operacionesSPei, OperacionesSwiftModelo swiftOperacion, string TipoOperacion)
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _OperacionesSpei = operacionesSPei;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
        }

        /// <summary>
        /// Implementa la logica de procesamiento correspondiente al tipo de operacion definido, coordinando la ejecucion de validaciones,
        /// consultas e integraciones requeridas por el flujo de negocio.
        /// </summary>
        public async Task<bool> ProcesarOperacion()
        {
            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Confirmando que el certificado se encuentre");

            string strCuentaVostroBene = string.Empty;
            bool bRegistrado = false;

            if (MetodosFirma.CertificadoEncontrado(_Config.StrDigSigCertNumSer, _Config.StrDigSigCertPsw) == false)
                throw new Exception("Error en procesamiento de SPEI: No se encontro el certificado y/o llave para realizar la firma de la transacción");

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación, folio_unico= {_SwfOperacion.Folio_unico}");

            string strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            if (strAliasProceso == ConstantesServicios.AliasSPEIRegistro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar operación en SPEI (si aplica).");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                strCuentaVostroBene = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCtaVostroVostro);

                if (string.IsNullOrEmpty(strCuentaVostroBene))
                {
                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar operación en SPEI");

                    bool procesadoSpei = await this.Registra_SPEI(_SwfOperacion.Folio, _SwfOperacion);

                    if (procesadoSpei == false && !string.IsNullOrEmpty(strError))
                        throw new Exception(strError);

                    if (procesadoSpei == false)
                        throw new Exception($"No se pudo registrar la operación en SPEI");
                }
                else
                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|No aplica el registrar operación en SPEI");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, ConstantesServicios.AliasSPEIRegistro);
                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            }

            if (strAliasProceso == ConstantesServicios.AliasT24Registro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.procesaOperacion|A registrar operación en T24.");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                strCuentaVostroBene = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCtaVostroVostro);

                if (string.IsNullOrEmpty(strCuentaVostroBene))
                {
                    Config.EscribirLog($"INFO|{strTipoOperacion}.procesaOperacion|A registrar operación en T24 con salida SPEI.");

                    SalidaT24RegistroModelo _resp = await T24_Registro(_SwfOperacion.Folio, _SwfOperacion);
                    bRegistrado = _resp.BCorrecto;

                    if (bRegistrado == false && !string.IsNullOrEmpty(_resp.StrMensajeError))
                        throw new Exception(strError);

                    if (bRegistrado == false)
                        throw new Exception($"Error en el procesamiento de registro MT-202 T24_Registro con folio {_SwfOperacion.Folio}");

                }
                else
                {
                    Config.EscribirLog($"INFO|{strTipoOperacion}.procesaOperacion|A registrar operación en T24 como traspaso entre VOSTROS.");

                    SalidaT24RegistroModelo _resp = await T24_Registr_Vostro(_SwfOperacion.Folio, _SwfOperacion);
                    bRegistrado = _resp.BCorrecto;

                    if (bRegistrado == false && !string.IsNullOrEmpty(_resp.StrMensajeError))
                        throw new Exception(strError);

                    if (bRegistrado == false)
                        throw new Exception($"Error en el procesamiento de registro MT-202 T24_Registro_Vistros con folio {_SwfOperacion.Folio}");
                }
            }

            if (strAliasProceso == ConstantesServicios.AliasConciliar)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A conciliar operaciones (por estatus) Entre TOMI y T24");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                //Busco la operacion en T24
                string strFolioT24 = await _comunes.Existe_Registro(_SwfOperacion.Folio_unico);

                if (string.IsNullOrEmpty(strFolioT24))
                    throw new Exception($"No existe folio asignado para el folio_unico={_SwfOperacion.Folio_unico}");

                Config.EscribirLog($"INFO|Folio T24 {strFolioT24}");

                //Valida contra los datos de Tomi - T24
                bool EsIgualEnT24 = false;
                string _moneda = (_SwfOperacion.Cve_moneda.Equals("MXP") || _SwfOperacion.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : _SwfOperacion.Cve_moneda;

                //Ordenante 
                string strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                string[] strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                string strCuentaDeb = strCuentaSeparadaVostro[2];
                string strBen_SubAplicacion = string.Empty;

                string strBen_Cuenta = string.Empty;
                //Beneficiario
                strCuentaVostroBene = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCtaVostroVostro);

                if (string.IsNullOrEmpty(strCuentaVostroBene))
                    strBen_Cuenta = ConstantesServicios.T24_BanxicoAcct;
                else
                {
                    strCuentaSeparadaVostro = strCuentaVostroBene.Split('-');
                    strBen_SubAplicacion = strCuentaSeparadaVostro[1];
                    strBen_Cuenta = strCuentaSeparadaVostro[2];
                }

                T24FundsTransferModelo _objT24 = await _OperacionesT24.ConsultarFoliSwiftT24(strFolioT24);

                if (_objT24 != null)
                {
                    if (strFolioT24.Equals(_objT24.Cur_Id)
                        && _SwfOperacion.Folio_unico.Equals(_objT24.Satel_sys_ref)
                        && _SwfOperacion.Des_importe.ToString().Equals(_objT24.Debit_Amount)
                        && _moneda.Equals(_objT24.Debit_Currency)
                        && strCuentaDeb.Equals(_objT24.Debit_acct_no)
                        && strBen_Cuenta.Equals(_objT24.Credit_acct_no))
                    {
                        EsIgualEnT24 = true;
                    }
                }

                if (EsIgualEnT24 == false)
                    throw new Exception("Los datos de conciliacion entre TOMI y T24 son diferentes");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                //Marca la operación como finalizada total
                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
            }
            return true;
        }

        /// <summary>
        /// Construye y registra una transaccion de comision en T24 a partir de la informacion de la operacion procesada,
        /// generando las estucturas requeridas para el registro contable y operativo.
        /// </summary>
        /// <param name="icn">Folio de la operacion</param>
        /// <param name="datos">Datos de la operacion necesarios para la generacion de la transaccion</param>
        private async Task<SalidaT24RegistroModelo> T24_Registro(string icn, OperacionesSwiftModelo datos)
        {
            //Base De Datos
            SalidaT24RegistroModelo resp = new SalidaT24RegistroModelo();

            try
            {
                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.ProcesaOperacion|Inicia registro en T24.");


                string strMoneda = string.Empty;
                string strDescripcion = string.Empty;
                string strBanco = string.Empty;
                string strNOmbre_Beneficiario = string.Empty;
                string strNumero_Cuenta_Beneficiario = string.Empty;
                string strCuentaCompletaVostro = string.Empty;
                string[] strCuentaSeparadaVostro = new string[] { };
                string strCuentaDeb = string.Empty;
                string strSW_END2END_REF = string.Empty;
                string strSLA_UETR = string.Empty;

                //Inicio de Variables
                strMoneda = (datos.Cve_moneda.Equals("MXP") || datos.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : datos.Cve_moneda;
                strDescripcion = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml);
                strBanco = $"{await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)}-{await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml)} ";
                strNOmbre_Beneficiario = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml);
                strNumero_Cuenta_Beneficiario = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml);
                strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                strCuentaDeb = strCuentaSeparadaVostro[2];

                //Armando la descripción
                strDescripcion = ConstantesServicios.DescripcionTransIntMt202;

                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)))
                    strDescripcion += $", {ConstantesServicios.DescBancoOrigen} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}";

                //Vostro
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro)))
                    strDescripcion += $", BANCO DESTINO: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro)}";

                //Spei
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)))
                    strDescripcion += $", PAGO SPEI A: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)}";

                //Referencia
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)))
                    strDescripcion += $", REL. REFERENCE: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)}";

                //Validación de Modena
                if (!strMoneda.Equals(ConstantesServicios.ModenaDefaultMN))
                    throw new Exception($"No hay referencia del mensaje MT-202 para la moneda {strMoneda}.");

                //Preparamos los datos para el registro del FT

                MwT24TrxMaestro oTrxMaster = new MwT24TrxMaestro()
                {
                    STrxOperationSpecificType = ConstantesServicios.TipoOperacion,
                    STrxOperationSubType = ConstantesServicios.OperacionSubTipoMt202,
                    STrxOperationFolio = icn,
                    STrxOperationAcct = strCuentaDeb
                };

                MwT24TrxDetalle oTrxDetailFF = new MwT24TrxDetalle()
                {
                    SDebit_acct_no = strCuentaDeb,
                    SDebit_currency = strMoneda,
                    SDebit_amount = string.Format("{0:0.##}", _SwfOperacion.Des_importe),
                    SDebit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_acct_no = ConstantesServicios.T24_BanxicoAcct,
                    SCredit_currency = strMoneda,
                    SOrdering_cust = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml),
                    SRelated_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SInstructions = strDescripcion,
                    SSatelSysRef = datos.Folio_unico,
                    SFolio_tomi = datos.Folio_unico,
                    SBeneficiary = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml),
                    SSW_END2END_REF = (await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.UETR)).Trim()

                };

                resp.BCorrecto = await _ConexionT24Mw.RegistraTrx(_SwfOperacion.Folio_unico, Convert.ToInt32(_Config.FechaJuliana), ConstantesServicios.TablaTrabajo,
                    ConstantesServicios.TipoOperacion, icn, ConstantesServicios.TipoOperacion, oTrxDetailFF, oTrxMaster);

                resp.StrMensajeError = "";


                return resp;
            }
            catch (Exception ex)
            {
                resp.StrMensajeError = ex.Message;
                resp.BCorrecto = false;
            }

            return resp;
        }

        /// <summary>
        /// Generala informacion requerida para una transferencia VOSTRO y ejecuta el registro correspondiente en t24 
        /// considerando los datos del ordenante, beneficiario, modena y referencias de la operacion.
        /// </summary>
        /// <param name="icn">Folio identificador</param>
        /// <param name="datos">Datos de la operacion</param>
        private async Task<SalidaT24RegistroModelo> T24_Registr_Vostro(string icn, OperacionesSwiftModelo datos)
        {
            //Base de Datos
            SalidaT24RegistroModelo resp = new SalidaT24RegistroModelo();

            try
            {
                string strMoneda = string.Empty;
                string strDescripcion = string.Empty;
                string strCuentaCompletaVostro = string.Empty;
                string[] strCuentaSeparadaVostro = new string[] { };
                string strOrd_SubAplicacion = string.Empty;
                string strOrd_Cuenta = string.Empty;
                string strOrd_Nombre = string.Empty;
                string strBen_SubAplicacion = string.Empty;
                string strBen_Cuenta = string.Empty;
                string strBen_Nombre = string.Empty;
                string strSW_END2END_REF = string.Empty;
                string strSLA_UETR = string.Empty;

                strMoneda = (datos.Cve_moneda.Equals("MXP") || datos.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : datos.Cve_moneda;

                //Ordenante
                strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                strOrd_SubAplicacion = strCuentaSeparadaVostro[1];
                strOrd_Cuenta = strCuentaSeparadaVostro[2];
                strOrd_Nombre = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml);

                //Beneficiario
                strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCtaVostroVostro);
                strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                strBen_SubAplicacion = strCuentaSeparadaVostro[1];
                strBen_Cuenta = strCuentaSeparadaVostro[2];
                strBen_Nombre = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro);

                //Armado de la descripción
                strDescripcion = ConstantesServicios.DescripcionTransIntMt202Vostro;

                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)))
                    strDescripcion += $", {ConstantesServicios.DescBancoOrigen} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}";

                //Vostro
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro)))
                    strDescripcion += $", BANCO DESTINO: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro)}";

                //Spei
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)))
                    strDescripcion += $", PAGO SPEI A: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)}";

                //Referencia
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)))
                    strDescripcion += $", REL. REFERENCE: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)}";

                //Preparamos los datos para el registro del FT

                MwT24TrxMaestro oTrxMaster = new MwT24TrxMaestro()
                {
                    STrxOperationSpecificType = ConstantesServicios.TipoOperacion,
                    STrxOperationSubType = ConstantesServicios.OperacionSubTipoMt202Vostros,
                    STrxOperationFolio = icn,
                    STrxOperationAcct = strOrd_Cuenta
                };

                MwT24TrxDetalle oTrxDetailFF = new MwT24TrxDetalle()
                {
                    SDebit_acct_no = strOrd_Cuenta,
                    SDebit_currency = strMoneda,
                    SDebit_amount = string.Format("{0:0.##}", _SwfOperacion.Des_importe),
                    SDebit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_acct_no = strBen_Nombre,
                    SCredit_currency = strMoneda,
                    SRelated_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SInstructions = strDescripcion,
                    SSatelSysRef = datos.Folio_unico,
                    SFolio_tomi = datos.Folio_unico,
                    SBeneficiary = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml),
                    SSW_END2END_REF = (await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.UETR)).Trim()
                };

                resp.BCorrecto = await _ConexionT24Mw.RegistraTrx(_SwfOperacion.Folio_unico, Convert.ToInt32(_Config.FechaJuliana), ConstantesServicios.TablaTrabajo, 
                    ConstantesServicios.TipoOperacion, icn, ConstantesServicios.TipoOperacion, oTrxDetailFF, oTrxMaster);

                resp.StrMensajeError = "";


                return resp;
            }
            catch (Exception ex)
            {
                resp.StrMensajeError = ex.Message;
                resp.BCorrecto = false;
            }
            return resp;
        }

        /// <summary>
        /// Ejecuta el proceso de registro de una operacion SPEI, validando el estado de la transaccion, determinando el tipo de paogo correspondiente, generando la informacion
        /// requerdia para el elnvio y almacenado de la operacion en el sitema SPEI.
        /// </summary>
        /// <param name="icn">Folio indentificador</param>
        /// <param name="datos">Datos de la operacion</param>
        private async Task<bool> Registra_SPEI(string icn, OperacionesSwiftModelo datos)
        {
            //Base de Datos
            Config.EscribirLog("INFO|registra_SPEI|A registrar pago SPEI.");

            if ((await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumRegistroSPEI)).Equals(ConstantesServicios.EstatusPROC))
            {
                Config.EscribirLog("INFO|registra_SPEI|El pago ya fue registrado en SPEI");
                return true;
            }

            int iTipoPago = ConstantesServicios.SpeiTipoPago;
            int iTipoCuenta = 4;

            Config.EscribirLog($"INFO|registra_SPEI|A registrar en SPEI para el tipo de pago = {iTipoPago}");

            switch (iTipoPago)
            {
                case 3:
                    Config.EscribirLog("INFO|registra_SPEI|Tercero a tercero vostro.");
                    break;
                case 5:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a tercero.");
                    break;
                case 6:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a vostro.");
                    break;
                case 7:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a participante.");
                    break;
                default:
                    throw new Exception($"No existe definición para pago SPEI del tipo de pago: {iTipoPago}");
            }

            //Valores TAS-SPEI
            string strReferenciaNumerica = ConstantesServicios.ClaveCASFIM;

            if (string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)))
            {
                string ValorTmp = string.Empty;

                ValorTmp = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml);


                if (int.TryParse(ValorTmp, out _))
                {
                    if (ValorTmp.Length > 7)
                        ValorTmp = ValorTmp.Substring(ValorTmp.Length - 7);

                    strReferenciaNumerica = ValorTmp;
                }
            }

            string strConcepto = $"FROM {await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}";

            //Proceso para generar la firma Digital
            ComunesFXD _comunesFXD = new ComunesFXD(_OperacionesTomi, _Config);

            if ((await _comunesFXD.GeneraFirmaDigital(datos, iTipoPago, ConstantesServicios.TablaTrabajo, icn)) == false)
            {
                throw new Exception($"Se presentó un error al generar la firma digital del folio: {icn}");
            }

            //Definicion de Procedimiento Almacenado

            InterfaceSPEIModelo datSpei = new InterfaceSPEIModelo()
            {
                ins_clave = Convert.ToInt64(await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)),
                op_monto = datos.Des_importe,
                op_cve_rastreo = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SenderReferenceXml)
            };

            string strNomBeneficiario = string.Empty;
            int Opcion = 0; //Esta Opcion solo es para el Query que se va a ejecutar 
            if (iTipoPago != 7)
            {
                strNomBeneficiario = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml);
                string beneficiaryIdentifier = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SenderReferenceXml);

                if (string.IsNullOrEmpty(beneficiaryIdentifier))
                {
                    if (await _comunes.EsSwift(beneficiaryIdentifier))
                    {
                        if (beneficiaryIdentifier.Length == 8)
                            Opcion = 1;
                        else
                            Opcion = 2;
                    }
                    else
                        Opcion = 3;

                    CIFSwiftModelo rsDatosCif = await _OperacionesTomi.ConsultarNombreCIF(beneficiaryIdentifier, Opcion);

                    if (rsDatosCif != null)
                        strNomBeneficiario = rsDatosCif.Des_nombre;
                }

                string strOrderingCustomerName = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml);
                string strRawAccount = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                strRawAccount = strRawAccount.Replace("-", "");
                string strCuentaBen = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntAccountXml);

                datSpei.op_nom_ord = await _comunesFXD.texto_SPEI(strOrderingCustomerName);
                datSpei.op_cuenta_ord = await _comunesFXD.texto_SPEI(strRawAccount);
                datSpei.op_nom_ben = await _comunesFXD.texto_SPEI(strNomBeneficiario);
                datSpei.tc_clave_ben = iTipoCuenta;
                datSpei.op_cuenta_ben = strCuentaBen;
                datSpei.op_rfc_curp_ben = ConstantesServicios.ND;
            }

            datSpei.op_concepto_pago = await _comunesFXD.texto_SPEI(strConcepto);
            datSpei.op_ref_numerica = Convert.ToInt32(strReferenciaNumerica);
            datSpei.usu_clave_int = 2;

            string strFirmaDigital = string.Empty;

            FirmaElectronicaDto rsDatosFirma = await _OperacionesTomi.ConsultarFirmaElectronica(icn);

            if (rsDatosFirma != null)
            {
                if (!string.IsNullOrEmpty(rsDatosFirma.Op_fimra_dig))
                    strFirmaDigital = rsDatosFirma.Op_fimra_dig;
            }

            if (string.IsNullOrEmpty(strFirmaDigital))
                throw new Exception("No fue posible obtener el campo de firma digital");

            datSpei.op_firma_dig = strFirmaDigital;

            //Termina Definición de Parametros y hace llamado
            Config.EscribirLog("INFO|registra_SPEI|Pasaron los parametros.");

            string NombreSP = $"{ProcedimientoAlmacenado.sp_InterfaceTomiSpeiDinamico}{iTipoPago}";
            string strCadenaRegreso = string.Empty;

            SalidaSPEIModelo salidaSPEI = await _OperacionesSpei.EjecutarSP_InterfaceTOMI_SPEI(NombreSP, datSpei);

            Config.EscribirLog("INFO|registra_SPEI|Se procesó procedimientos si errores.");

            if (salidaSPEI != null)
            {
                strCadenaRegreso = $"{salidaSPEI.op_error}|{salidaSPEI.op_clave}|{salidaSPEI.op_fecha_oper}|{salidaSPEI.op_folio}|{salidaSPEI.op_descripcion}";
            }

            if (strCadenaRegreso.StartsWith("0|") || strCadenaRegreso.Contains("Ya existe una operación"))
            {
                XDocument xml = XDocument.Parse(datos.Nf_variables_xml);
                XmlHelper.AgregarElemento(xml, ConstantesServicios.BtmumRegistroSPEI, ConstantesServicios.EstatusPROC, null);
                return true;
            }
            else
                return false;
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo MT210, ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class MT_210 : OperacionBase
    {
        string strTipoOperacion = string.Empty;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="MT_210"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public MT_210(ConfiguracionModelo configuracion, IOperaciones operacionesTomi, OperacionesSwiftModelo swiftOperacion, string TipoOperacion)
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
        }

        /// <summary>
        /// Implementa la logica de procesamiento correspondiente al tipo de operacion definido, coordinando la ejecucion de validaciones,
        /// consultas e integraciones requeridas por el flujo de negocio.
        /// </summary>
        public async Task<bool> ProcesarOperacion()
        {
            try
            {
                OperacionesGenerales _comunes = new OperacionesGenerales(_Config, _OperacionesTomi);

                bool bOrdenanteMUFG = false;

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación");

                string _SwiftEmisor = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftEmisor);

                if (string.IsNullOrEmpty(_SwiftEmisor))
                {
                    Config.EscribirLog("No existe la información del Swift emisor");
                    return false;
                }

                if (_SwiftEmisor.Length > 8)
                    _SwiftEmisor = _SwiftEmisor.Substring(0, 8);

                string _SwiftOrdenante = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftVostroXml);

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Swift ordenante {_SwiftOrdenante}");

                if (_SwiftOrdenante.StartsWith(ConstantesServicios.OrdenanteBotk) || _SwiftOrdenante.StartsWith(ConstantesServicios.OrdenanteBofCus33))
                {
                    bOrdenanteMUFG = true;
                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|El Wwift Ordenante es referencia interna MUFG");
                }

                string _Referencia = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderreferenceXml);
                _Referencia = _Referencia + (bOrdenanteMUFG == true ? " MUFG" : "");

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|usp_swf_btmum_210_add_ref. Pasaron parametros, inicia ejecución");

                int iSalida = await _OperacionesTomi.AgregaReferenciaBtmum210(_SwfOperacion.Folio, _SwfOperacion.Fec_valor.ToString(), _SwiftEmisor, _Referencia, _SwfOperacion.Des_importe);

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Pasó la ejecución");

                if (iSalida != 0)
                {
                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|CON ERROR");
                    Config.EscribirLog("No se pudo insertar el mensaje");
                    return false;
                }

                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Proceso Terminado");
                return true;
            }

            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Error = {ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo PACS008, ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class PACS008 : OperacionBase
    {
        string strTipoOperacion = string.Empty;
        string strError = string.Empty;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="PACS008"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public PACS008(ConfiguracionModelo configuracion, IOperaciones operacionesTomi,IOperacionesT24 operacionesT24, ConexionT24Mw conexionMt24Mw, OperacionesSwiftModelo swiftOperacion, string TipoOperacion) 
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _OperacionesT24 = operacionesT24;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
            _ConexionT24Mw = conexionMt24Mw;
        }

        /// <summary>
        /// Implementa la logica de procesamiento correspondiente al tipo de operacion definido, coordinando la ejecucion de validaciones,
        /// consultas e integraciones requeridas por el flujo de negocio.
        /// </summary>
        public async Task<bool> ProcesarOperacion()
        {
            string strAliasProceso = string.Empty;
            bool bProcesado = false;

            _comunes = new OperacionesGenerales(_Config, _OperacionesTomi,_ConexionT24Mw);

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación, folio_unico = {_SwfOperacion.Folio_unico}.");

            strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            if (strAliasProceso == ConstantesServicios.AliasTomiEtradasSalidas)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A realizar el registro E/S");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                DateTime fecValor = DateTime.MinValue;
                DateTime.TryParseExact(
                    _Config.IntFechaSistema.ToString(),
                    "yyyyMMdd",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out fecValor);

                TombinEntradasSalidasModelo datosE_S = new TombinEntradasSalidasModelo()
                {
                    Referencia = _SwfOperacion.Folio_unico,
                    Num_Cliente = _SwfOperacion.Num_cliente,
                    Ordenante = _SwfOperacion.Nombre_cliente,
                    Operacion = Convert.ToDecimal(ConstantesServicios.Operacion218),
                    OperacionDesc = ConstantesServicios.OperacionDescEntradaTTS,
                    Tipo = ConstantesServicios.OpTipoInternacional,
                    TipoDesc = ConstantesServicios.OpTipoInternacionalDesc,
                    Cuenta = _SwfOperacion.Num_cliente,
                    Nombre = _SwfOperacion.Nombre_cliente,
                    CuentaMoneda = (_SwfOperacion.Cve_moneda.Equals("MXP") || _SwfOperacion.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : _SwfOperacion.Cve_moneda,
                    Via = Convert.ToDecimal(ConstantesServicios.ViaSubAplicacion810),
                    ViaDesc = ConstantesServicios.ViaSubAplicacion810Desc,
                    Importe = _SwfOperacion.Des_importe,
                    Saldo = 0,
                    Comentarios = "",
                    Fecha_Captura = DateTime.Now.Date,
                    FechaValor = fecValor,
                    Hora = DateTime.Now.ToString("hh:mm:ss AMPM"),
                    Capturista = ConstantesServicios.CapturistaJeai,
                    Fad = 0,
                    Referencia_Control = "",
                    Referencia_Control_Cont = 0
                };

                bProcesado = await _comunes.Registra_E_S(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso, datosE_S);

                if(!bProcesado)
                    throw new Exception($"No se pudo registrar la operación como entrada/salida");

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Se realizó el registro de E/S exitosamente");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Se termina el registro E/S e inicia siguiente tarea");

                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            }

            if (strAliasProceso == ConstantesServicios.AliasT24Registro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar Operación en T24");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                if((await _comunes.ReintentosPermitidosSWF_MX(strTipoOperacion, _SwfOperacion.Folio)) == true)
                {
                    var salida = await T24_Registro(_SwfOperacion.Folio, _SwfOperacion);
                    bProcesado = salida.BCorrecto;
                    strError = salida.StrMensajeError;

                    if(bProcesado == false && !string.IsNullOrEmpty(strError))
                        throw new Exception(strError);

                    if (bProcesado == false)
                        throw new Exception($"Error en procesamiento de registro PACS008 T24_REGISTRO con folio: {_SwfOperacion.Folio} ");
                }

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);
                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);
            }

            if (strAliasProceso == ConstantesServicios.AliasConciliar)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A conciliar Operaciones (por estatus) entre TOMI y T24");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                string strFolioT24 = await _comunes.Existe_Registro(_SwfOperacion.Folio_unico);

                bool EsIgualEnT24 = false;

                if (string.IsNullOrEmpty(strFolioT24))
                    throw new Exception($"No existe folio asignado para el folio_unico={_SwfOperacion.Folio_unico}");

                //Valida contra los datos de Tomi - T24
                string _moneda = (_SwfOperacion.Cve_moneda.Equals("MXP") || _SwfOperacion.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : _SwfOperacion.Cve_moneda;
                string _CuentaDeb = string.Empty;
                string _CuentaCred = string.Empty;

                if (_moneda.Equals(ConstantesServicios.ModenaDefaultMN))
                {
                    _CuentaDeb = (_SwfOperacion.Nf_vostro_emisor.Length >= 7) ? _SwfOperacion.Nf_vostro_emisor.Substring(_SwfOperacion.Nf_vostro_emisor.Length - 7) : _SwfOperacion.Nf_vostro_emisor;
                    _CuentaCred = _SwfOperacion.Num_cuenta;

                    if (_CuentaCred.Length == 18)
                        _CuentaCred = _CuentaCred.Substring(11, 7);
                    else
                        throw new Exception("La cuenta a acreditar no es válida");
                }
                else
                {
                    _CuentaDeb = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.DebitAccount);
                    _CuentaCred = _SwfOperacion.Num_cuenta;
                }

                T24FundsTransferModelo _objT24 = await _OperacionesT24.ConsultarFoliSwiftT24(strFolioT24);

                if (_objT24 != null)
                {
                    if (strFolioT24.Equals(_objT24.Cur_Id)
                        && _SwfOperacion.Folio_unico.Equals(_objT24.Satel_sys_ref)
                        && _SwfOperacion.Des_importe.ToString("0.00").Equals(_objT24.Debit_Amount)
                        && _moneda.Equals(_objT24.Debit_Currency)
                        && _CuentaDeb.Equals(_objT24.Debit_acct_no)
                        && _CuentaCred.Equals(_objT24.Credit_acct_no))
                    {
                        EsIgualEnT24 = true;
                    }
                }

                if (EsIgualEnT24 == false)
                    throw new Exception("Los datos de conciliacion entre TOMI y T24 son diferentes");

                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Conciliación exitosa");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                //Marca la operación como finalizada total
                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
            }

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Fin del registro de la operación");

            return true;
        }

        /// <summary>
        /// Construye y registra una transaccion de comision en T24 a partir de la informacion de la operacion procesada,
        /// generando las estucturas requeridas para el registro contable y operativo.
        /// </summary>
        /// <param name="icn">Folio de la operacion</param>
        /// <param name="datos">Datos de la operacion necesarios para la generacion de la transaccion</param>
        public async Task<SalidaT24RegistroModelo> T24_Registro(string icn, OperacionesSwiftModelo datos)
        {
            SalidaT24RegistroModelo resp = new SalidaT24RegistroModelo();

            try
            {
                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.ProcesaOperacion|Inicia registro en T24.");

                string _Moneda = (datos.Cve_moneda.Equals("MXP") || datos.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : datos.Cve_moneda;
                string _SwiftInstOrdenante = string.Empty;
                string _NombreInstOrdenante = string.Empty;
                string _DireccionInstOrdenante = string.Empty;
                string _Descripcion = string.Empty;
                string _DetailCharges = string.Empty;
                string _T24_Internal_Type = string.Empty;
                string _OrderingBank = string.Empty;
                string CIF_Inst_Ordenante = string.Empty;

                _NombreInstOrdenante = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.NombreInstOrdenanteXml);

                if (!string.IsNullOrEmpty(_NombreInstOrdenante))
                {
                    _DireccionInstOrdenante = string.Format("{0} {1}", await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DireccionInstOrdenanteXml), 
                        await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DireccionInstOrdenanteXml2));

                    _SwiftInstOrdenante = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.InstOrdenanteXml);
                    //Truncar campos
                    _NombreInstOrdenante = await _comunes.Quita_Linea(_NombreInstOrdenante, 50);
                    _DireccionInstOrdenante = await _comunes.Quita_Linea(_DireccionInstOrdenante, 80);

                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Se encontró la información del ordenante por institución ordenante");
                }
                else
                {
                    //No hay banco ordenante, busco el CIF ordenante
                    CIF_Inst_Ordenante = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.CIFOrdenanteXml);
                    if (!string.IsNullOrEmpty(CIF_Inst_Ordenante))
                    {
                        CIFSwiftModelo _cif = await _OperacionesTomi.Busca_Cif_Datos(CIF_Inst_Ordenante, _SwiftInstOrdenante);
                        if (_cif != null)
                        {
                            _SwiftInstOrdenante = _cif.Cve_swift;
                            _NombreInstOrdenante = await _comunes.Quita_Linea(_cif.Des_nombre, 50);
                            _DireccionInstOrdenante = await _comunes.Quita_Linea(_cif.Des_calle, 80);

                            Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Se encontró: Swift= {_cif.Cve_swift}, nombre={_NombreInstOrdenante}, calle = {_DireccionInstOrdenante}");
                        }
                    }
                    else
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|No se encontró el CIF de la institución ordenante en el catálogo.");
                }

                //Armando la descripción
                _Descripcion = ConstantesServicios.DescripcionTransIntPacs008;

                if (!string.IsNullOrEmpty(_NombreInstOrdenante))
                    _Descripcion += $" {ConstantesServicios.DescBancoOrdenante} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderInstNameXml)}";
                else
                {
                    if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)))
                        _Descripcion += $", {ConstantesServicios.DescBancoOrigen} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}";
                }

                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrdenanteXml)))
                    _Descripcion += $", {ConstantesServicios.DescOrdenante} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingNameXml)}";

                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.DetalleCargosXml)))
                    _Descripcion += $", {ConstantesServicios.DescCargos} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.DetalleCargosXml)}";

                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.ConceptoXml)))
                    _Descripcion += $", {ConstantesServicios.DescConcepto} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.ConceptoXml)}";

                // Es OUR?
                _DetailCharges = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.DetalleCargosXml);

                if (_DetailCharges.Equals(ConstantesServicios.DetalleCargoOUR))
                {
                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Es registro con DetailCharges = 'OUR' (sin comisiones).");
                    _T24_Internal_Type = ConstantesServicios.TipoMensajePACS008;
                    _OrderingBank = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftEmisor);

                }
                else
                {
                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Inicia Busqueda de comisiones.");
                    _T24_Internal_Type = ConstantesServicios.T24TipoINternoPacs008;
                    _OrderingBank = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftEmisor);
                }

                //Preparamos los datos para el Registro del FT
                MwT24TrxMaestro oTrxMaster = new MwT24TrxMaestro()
                {
                    STrxOperationSpecificType = ConstantesServicios.TipoOperacionEsquemas,
                    STrxInternalType = ConstantesServicios.TipoOperacionEsquemas,
                    STrxOperationSubType = _T24_Internal_Type,
                    STrxTransactionType = _T24_Internal_Type,
                    STrxOperationFolio = icn,
                    STrxOperationAcct = _SwfOperacion.Num_cuenta
                };

                string _moneda = (_SwfOperacion.Cve_moneda.Equals("MXP") || _SwfOperacion.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : _SwfOperacion.Cve_moneda;
                string _CuentaDeb = string.Empty;
                string _CuentaCred = string.Empty;

                if (_moneda.Equals(ConstantesServicios.ModenaDefaultMN))
                {
                    _CuentaDeb = (_SwfOperacion.Nf_vostro_emisor.Length >= 7) ? _SwfOperacion.Nf_vostro_emisor.Substring(_SwfOperacion.Nf_vostro_emisor.Length - 7) : _SwfOperacion.Nf_vostro_emisor;
                    _CuentaCred = _SwfOperacion.Num_cuenta;

                    if (_CuentaCred.Length == 18)
                        _CuentaCred = _CuentaCred.Substring(11, 7);
                    else
                        throw new Exception("La cuenta a acreditar no es válida");
                }
                else
                {
                    _CuentaDeb = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.DebitAccount);
                    _CuentaCred = _SwfOperacion.Num_cuenta;
                }

                string _Beneficiario = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BeneficiarioXml);

                if (string.IsNullOrEmpty(_Beneficiario))
                    _Beneficiario = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.Beneficiario2Xml);


                string _OrderingCust = "AC" +
                                        (string.IsNullOrWhiteSpace(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingAccount)) ? "" : (await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingAccount)).TrimEnd().TrimStart()) +
                                        (string.IsNullOrWhiteSpace(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingNameAddr1)) ? "" : ConstantesServicios.Separador + (await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingNameAddr1)).TrimStart().TrimEnd()) +
                                        (string.IsNullOrWhiteSpace(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingNameAddr2)) ? "" : ConstantesServicios.Separador + (await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingNameAddr2)).TrimStart().TrimEnd()) +
                                        (string.IsNullOrWhiteSpace(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingNameAddr3)) ? "" : ConstantesServicios.Separador + (await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.OrderingNameAddr3)).TrimStart().TrimEnd());

                CIF_Inst_Ordenante = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.CIFOrdenanteXml);

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Buscando datos de la institución ordenante en el catálogo cif={CIF_Inst_Ordenante}.");

                if(string.IsNullOrEmpty(_NombreInstOrdenante))
                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|No se encontró el CIF de la institución ordenante en el catálogo.");
                else
                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.T24_Registro|Encontró Swift={_SwiftInstOrdenante}, Nombre = {_NombreInstOrdenante} ");


                string _SWEnd2EndRef = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.UETR);
                string _SlaUetr = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SLA_UETR);

                MwT24TrxDetalle oTrxDetailFF = new MwT24TrxDetalle()
                {
                    SDebit_acct_no = _CuentaDeb,
                    SDebit_currency = _moneda,
                    SDebit_amount = string.Format("{0:0.##}", _SwfOperacion.Des_importe),
                    SDebit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_acct_no = _CuentaCred,
                    SCredit_currency = _moneda,
                    SBen_our_charges = _DetailCharges,
                    SOrdering_bank = _OrderingBank,
                    SBeneficiary = _Beneficiario,
                    SOrdering_cust = (_OrderingCust.Substring(_OrderingCust.Length - 1, 1) == ConstantesServicios.Separador) ? _OrderingCust.Substring(0, _OrderingCust.Length - 1) : _OrderingCust,
                    SCusRef1 = _NombreInstOrdenante,
                    SCusRef2 = _SwiftInstOrdenante,
                    SCusRef3 = CIF_Inst_Ordenante,
                    SRelated_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SInstructions = _Descripcion,
                    SSatelSysRef = datos.Folio_unico,
                    SFolio_tomi = datos.Folio_unico,
                    SSW_END2END_REF = _SWEnd2EndRef.Trim(),
                };

                resp.BCorrecto = await _ConexionT24Mw.RegistraTrx(_SwfOperacion.Folio_unico, Convert.ToInt32(_Config.FechaJuliana), ConstantesServicios.TablaTrabajo, 
                    ConstantesServicios.TipoOperacion, icn, ConstantesServicios.TipoOperacion, oTrxDetailFF, oTrxMaster);

                resp.StrMensajeError = "";


                return resp;
            }
            catch (Exception ex)
            {
                resp.StrMensajeError = ex.Message;
                resp.BCorrecto = false;
            }

            return resp;
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo PACS008 MXN, ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class PACS008_MXN : OperacionBase
    {
        string strTipoOperacion = string.Empty;
        public PACS008_MXN(ConfiguracionModelo configuracion, IOperaciones operacionesTomi, OperacionesSwiftModelo swiftOperacion, string TipoOperacion)
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
        }

        public async Task<bool> ProcesarOperacion()
        {
            string strError = string.Empty;

            _comunes = new OperacionesGenerales(_Config, _OperacionesTomi);

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación, folio_unico = {_SwfOperacion.Folio_unico}.");

            string strAliasProceso = string.Empty;
            bool bProcesado = false;

            strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (strAliasProceso.Equals(ConstantesServicios.AliasTomiEtradasSalidas) || strAliasProceso.Equals(ConstantesServicios.AliasT24Registro) || strAliasProceso.Equals(ConstantesServicios.AliasConciliar))
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar Operacion en TOMI");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                if (strAliasProceso.Equals(ConstantesServicios.AliasTomiEtradasSalidas))
                {
                    int FolioMW = 0;
                    bProcesado = await this.TOMI_REGISTRO(_SwfOperacion.Folio, _SwfOperacion, FolioMW, strAliasProceso);

                    if (bProcesado == false && string.IsNullOrEmpty(strError))
                        throw new Exception(strError);

                    if (bProcesado == false)
                        throw new Exception($"Error en el procesamiento del registro TOMI_REGISTRO con folio: {_SwfOperacion.Folio}");

                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Fin del registro de OperaciónI");

                    await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);
                }

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, ConstantesServicios.AliasT24Registro);
                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, ConstantesServicios.AliasT24Registro);

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, ConstantesServicios.AliasConciliar);
                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, ConstantesServicios.AliasConciliar);

                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
            }


            return true;
        }

        /// <summary>
        /// Ejecuta el proceso de registro de una operacion, obteniendo la informacion necesaria desde el mensaje XML, validando cuentas involucradas 
        /// y determinando reglas aplicables al tratamiento de comisiones y cargos.
        /// </summary>
        /// <param name="icn">Folio identificador</param>
        /// <param name="datos">Datos de la operacion</param>
        public async Task<bool> TOMI_REGISTRO(string icn, OperacionesSwiftModelo datos, int folioMW, string aliasProceso)
        {
            try
            {
                string strBeneficiario = (await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BeneficiarioXml)).Trim();
                string strCuentaCompletaVostro;
                string[] strCuentaSeparadaVostro;
                string strCuentaPrincipal;
                string strCuentaComisiones;
                string strDescripcion;
                string strDetailCharges;
                string strNumeroCliente = string.Empty;
                string strNombreCliente = string.Empty;
                string strNombreCuenta = string.Empty;
                bool bCuentaT24 = false;
                int iTipoAppAnterior = 0;
                string strTipoDesc;
                string strNombreBancoBene;
                bool bActiveAutomation = false;

                if (string.IsNullOrEmpty(strBeneficiario))
                    strBeneficiario = (await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.Beneficiario2Xml)).Trim();

                if (strBeneficiario.Length > 100)
                    strBeneficiario = strBeneficiario.Substring(0, 100);

                strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                strCuentaPrincipal = strCuentaSeparadaVostro[2];
                strCuentaComisiones = strCuentaPrincipal;


                strDescripcion = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.ConceptoXml);
                strDetailCharges = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.DetalleCargosXml);

                //Datos de lacuenta en T24
                T24CustomerAccountModelo cuentaT24 = await _OperacionesT24.Datos_Cuenta_VW(strCuentaPrincipal);

                if (cuentaT24 == null)
                {
                    throw new Exception($"No se encontró o está inactiva la cuenta en T24 {strCuentaPrincipal}");
                }

                strNumeroCliente = cuentaT24.Customer;
                strNombreCliente = cuentaT24.Name_1;
                strNombreCuenta = cuentaT24.Account_title_1;

                //Valido las comisiones o tipo de operación
                if ((await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftEmisor)).Equals(ConstantesServicios.OrdenanteBotKus33))
                {
                    //es NY

                    if (strDetailCharges.Equals(ConstantesServicios.DetalleCargoOUR))
                    {
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Es registro de NY, swift='BOTKUS33' y DetailCharges='OUR'");
                        iTipoAppAnterior = int.Parse(ConstantesServicios.ViaSubAplicacion804);
                        strTipoDesc = ConstantesServicios.ViaSubAplicacion804Desc;
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|BTMU NY, comisión diferente a OUR");
                        iTipoAppAnterior = int.Parse(ConstantesServicios.ViaSubAplicacion800);
                        strTipoDesc = ConstantesServicios.ViaSubAplicacion800Desc;
                    }
                }
                else
                {
                    //No es NY
                    Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Subaplicación 800");
                    if (strDetailCharges.Equals(ConstantesServicios.DetalleCargoOUR))
                    {
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|No es NY, comisión OUR.");
                        iTipoAppAnterior = int.Parse(ConstantesServicios.ViaSubAplicacion800);
                        strTipoDesc = ConstantesServicios.ViaSubAplicacion800Desc;
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|No es Ny, comisión diferente a our");
                        iTipoAppAnterior = int.Parse(ConstantesServicios.ViaSubAplicacion800);
                        strTipoDesc = ConstantesServicios.ViaSubAplicacion800Desc;
                    }
                }

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Inicia registro en base TOMI con llamado 'usp_TOMBIN_INSERT_FORMATO_FXD_LOCAL'");
                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|T24_Customer_Code={strNumeroCliente}");
                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|A buscar el nombre del banco para la cuenta CLABE {datos.Num_cuenta}");

                strNombreBancoBene = await _OperacionesTomi.Dame_Banco(datos.Num_cuenta);

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Banco={strNombreBancoBene}");

                if (string.IsNullOrEmpty(strNombreBancoBene))
                    throw new Exception($"El banco asociado a la cuenta: {datos.Num_cuenta} no existe");

                bActiveAutomation = await _OperacionesTomi.AutomationIsActive();

                string status = (bActiveAutomation == true) ?
                                ((Convert.ToDecimal(DateTime.Now.Date.ToString("yyyyMMdd")) <= datos.Fec_valor) ? ConstantesServicios.EstatusScnp : ConstantesServicios.EstatusConf)
                                : ConstantesServicios.EstatusT24Pend;

                FormatoFxdLocalModelo formatoFxdLocal = new FormatoFxdLocalModelo()
                {
                    NF_FOLIO_UNICO = datos.Folio_unico,
                    NF_TIPO = ConstantesServicios.FXD,
                    NF_FOLIO = null,
                    T24_CUSTOMER_CODE = strNumeroCliente,
                    T24_DEBIT_ACCT_NO = strCuentaPrincipal,
                    NF_ESTATUS = status,
                    NF_USUARIO_CAPTURA = ConstantesServicios.CapturistaJeai,
                    NF_USUARIO_AUTORIZA = (bActiveAutomation == true) ? ConstantesServicios.UsuarioTomi : null,
                    NF_FECHA_CAPTURA = DateTime.Now,
                    NF_FECHA_AUTORIZA = bActiveAutomation ? (DateTime?)DateTime.Now : null,
                    T24_DEBIT_AMOUNT = datos.Des_importe,
                    T24_SHORT_NAME = strNombreCliente,
                    NF_FOREX_RELACIONADO = "",
                    T24_DEBIT_CURRENCY = Moneda.MXN.ToString(),
                    NF_BANCO_BENEF = strNombreBancoBene,
                    T24_BEN_ACCT_NO = datos.Num_cuenta,
                    NF_TIPO_CUENTA = ConstantesServicios.TipoCuentaCLABE,
                    T24_BEN_CUST_NAME = strBeneficiario,
                    NF_RFC_BENEFICIARIO = null,
                    NF_TASA_RETENCION = null,
                    NF_FECHA_EJECUCION = Convert.ToInt32(datos.Fec_valor),
                    T24_DEBIT_THEIR_REF = icn,
                    NF_MONTO_ENVIAR = datos.Des_importe,
                    ID_PLANTILLA = null,
                    ID_FORMATO_PLANTILLA = null,
                    Salida = 0
                };

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|usp_INSERT_FORMATO_FXD_LOCAL pasaron parametros, inicia ejecución");

                SalidaFXDLocalModelo salida = await _OperacionesTomi.Insert_Formato_FXD_local(formatoFxdLocal);

                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Paso la ejecución, Salida={salida.Salida}, folioRegreso={salida.Folio}");

                if (salida.Salida != 0 || salida.Folio.Equals(0))
                    throw new Exception($"No se pudo registrar la Operacion como FXD_Local");

                await _OperacionesTomi.ActualizaDatosOperacion(icn, Convert.ToInt32(salida.Folio), strDetailCharges);

                TombinEntradasSalidasModelo datosE_S = new TombinEntradasSalidasModelo()
                {
                    Referencia = datos.Folio_unico,
                    Num_Cliente = Convert.ToInt32(strNumeroCliente),
                    Ordenante = strNombreCliente,
                    Operacion = Convert.ToDecimal(ConstantesServicios.Operacion218),
                    OperacionDesc = ConstantesServicios.OperacionDescEntradaTTS,
                    Tipo = ConstantesServicios.TipoDomestica,
                    TipoDesc = ConstantesServicios.TipoDomesticaDesc,
                    Cuenta = Convert.ToDecimal(strCuentaPrincipal),
                    Nombre = strNombreCuenta,
                    CuentaMoneda = Moneda.MXN.ToString(),
                    Via = iTipoAppAnterior,
                    ViaDesc = strTipoDesc,
                    Importe = datos.Des_importe,
                    Saldo = 0,
                    Comentarios = "",
                    Fecha_Captura = DateTime.Now.Date,
                    FechaValor = Convert.ToDateTime(_Config.StrFechaSistema),
                    Hora = DateTime.Now.ToString("hh:mm:ss AMPM"),
                    Capturista = ConstantesServicios.CapturistaJeai,
                    Fad = 0,
                    Referencia_Control = "",
                    Referencia_Control_Cont = 0
                };

                bool bRegistrado = await _comunes.Registra_E_S(icn, ConstantesServicios.TipoOperacion, aliasProceso, datosE_S);

                if (bRegistrado == false)
                    throw new Exception($"No se pudo registrar la operación como entrada/salida");

                folioMW = Convert.ToInt32(salida.Folio);
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{ConstantesServicios.TipoOperacion}.TOMI_REGISTRO|Se presenta excepción: {ex.Message}");
            }

            return false;
        }

    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.FirmaElectronica;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Modelos;
using System;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo PACS009, ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class PACS009 : OperacionBase
    {
        string strTipoOperacion = string.Empty;
        string strError = string.Empty;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="PACS009"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public PACS009(ConfiguracionModelo configuracion, IOperaciones operacionesTomi, IOperacionesSPEI operacionesSPei, OperacionesSwiftModelo swiftOperacion, string TipoOperacion)
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _OperacionesSpei = operacionesSPei;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
        }

        /// <summary>
        /// Implementa la logica de procesamiento correspondiente al tipo de operacion definido, coordinando la ejecucion de validaciones,
        /// consultas e integraciones requeridas por el flujo de negocio.
        /// </summary>
        public async Task<bool> ProcesarOperacion()
        {
            string strCuentaVostroBene = string.Empty;
            bool bRegistrado = false;

            //Confirmo que se encuentre el certificado
            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Confirmando que el certificado se encuentre");

            if (MetodosFirma.CertificadoEncontrado(_Config.StrDigSigCertNumSer, _Config.StrDigSigCertPsw) == false)
                throw new Exception("Error en procesamiento de SPEI: No se encontro el certificado y/o llave para realizar la firma de la transacción");

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación, folio_unico= {_SwfOperacion.Folio_unico}");

            string strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            if (strAliasProceso == ConstantesServicios.AliasSPEIRegistro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar operación en SPEI (si aplica).");
                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                strCuentaVostroBene = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCtaVostroVostro);

                if (string.IsNullOrEmpty(strCuentaVostroBene))
                {
                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar operación en SPEI");

                    bool procesadoSpei = await this.Registra_SPEI(_SwfOperacion.Folio, _SwfOperacion);

                    if (procesadoSpei == false && !string.IsNullOrEmpty(strError))
                        throw new Exception(strError);

                    if (procesadoSpei == false)
                        throw new Exception($"No se pudo registrar la operación en SPEI");
                }
                else
                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|No aplica el registrar operación en SPEI");

                //Termino del Proceso
                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, ConstantesServicios.AliasSPEIRegistro);

                // Nuevo alias del proceso
                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);
            }

            // Registro de la operación en T24
            if (strAliasProceso == ConstantesServicios.AliasT24Registro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.procesaOperacion|A registrar operación en T24.");

                //Inicio el proceso
                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                // Valido el tipo de operacion PACS009 (hay caso para salida SPEI y caso de traspaso entre cuentas vostro internas)
                strCuentaVostroBene = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCtaVostroVostro);

                if (string.IsNullOrEmpty(strCuentaVostroBene))
                {
                    //Valido la operacion en T24
                    Config.EscribirLog($"INFO|{strTipoOperacion}.procesaOperacion|A registrar operación en T24 con salida SPEI.");

                    SalidaT24RegistroModelo _resp = await this.T24_Registro(_SwfOperacion.Folio, _SwfOperacion);
                    bRegistrado = _resp.BCorrecto;

                    if (bRegistrado == false && !string.IsNullOrEmpty(_resp.StrMensajeError))
                        throw new Exception(strError);

                    if (bRegistrado == false)
                        throw new Exception($"Error en el procesamiento de registro PACS009 T24_Registro con folio {_SwfOperacion.Folio}");

                }
                else
                {
                    Config.EscribirLog($"INFO|{strTipoOperacion}.procesaOperacion|A registrar operación en T24 como traspaso entre VOSTROS.");

                    SalidaT24RegistroModelo _resp = await T24_Registro_Vostro(_SwfOperacion.Folio, _SwfOperacion);
                    bRegistrado = _resp.BCorrecto;

                    if (bRegistrado == false && !string.IsNullOrEmpty(_resp.StrMensajeError))
                        throw new Exception(strError);

                    if (bRegistrado == false)
                        throw new Exception($"Error en el procesamiento de registro PACS009 T24_Registro_Vistros con folio {_SwfOperacion.Folio}");
                }
            }

            //Conciliación
            if (strAliasProceso == ConstantesServicios.AliasConciliar)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A conciliar operaciones (por estatus) Entre TOMI y T24");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                Config.EscribirLog($"INFO|Folio Unico {_SwfOperacion.Folio_unico}");

                //Busco la operacion en T24
                string strFolioT24 = await _comunes.Existe_Registro(_SwfOperacion.Folio_unico);

                Config.EscribirLog($"INFO|Folio T24 {strFolioT24}");

                if (string.IsNullOrEmpty(strFolioT24))
                    throw new Exception($"No existe folio asignado para el folio_unico={_SwfOperacion.Folio_unico}");

                //Valida contra los datos de Tomi - T24
                bool EsIgualEnT24 = false;
                string _moneda = (_SwfOperacion.Cve_moneda.Equals("MXP") || _SwfOperacion.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : _SwfOperacion.Cve_moneda;

                //Ordenante 
                string strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                string[] strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                string strCuentaDeb = strCuentaSeparadaVostro[2];
                string strBen_SubAplicacion = string.Empty;

                string strBen_Cuenta = string.Empty;
                //Beneficiario
                strCuentaVostroBene = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCtaVostroVostro);

                if (string.IsNullOrEmpty(strCuentaVostroBene))
                    strBen_Cuenta = ConstantesServicios.T24_BanxicoAcct;
                else
                {
                    strCuentaSeparadaVostro = strCuentaVostroBene.Split('-');
                    strBen_SubAplicacion = strCuentaSeparadaVostro[1];
                    strBen_Cuenta = strCuentaSeparadaVostro[2];
                }

                T24FundsTransferModelo _objT24 = await _OperacionesT24.ConsultarFoliSwiftT24(strFolioT24);

                if (_objT24 != null)
                {
                    if (strFolioT24.Equals(_objT24.Cur_Id)
                        && _SwfOperacion.Folio_unico.Equals(_objT24.Satel_sys_ref)
                        && _SwfOperacion.Des_importe.ToString().Equals(_objT24.Debit_Amount)
                        && _moneda.Equals(_objT24.Debit_Currency)
                        && strCuentaDeb.Equals(_objT24.Debit_acct_no)
                        && strBen_Cuenta.Equals(_objT24.Credit_acct_no))
                    {
                        EsIgualEnT24 = true;
                    }
                }

                if (EsIgualEnT24 == false)
                    throw new Exception("Los datos de conciliacion entre TOMI y T24 son diferentes");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, strTipoOperacion, strAliasProceso);

                //Marca la operación como finalizada total
                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
            }

            // Termina el proceso del folio
            return true;
        }

        /// <summary>
        /// Construye y registra una transaccion de comision en T24 a partir de la informacion de la operacion procesada,
        /// generando las estucturas requeridas para el registro contable y operativo.
        /// </summary>
        /// <param name="icn">Folio de la operacion</param>
        /// <param name="datos">Datos de la operacion necesarios para la generacion de la transaccion</param>
        private async Task<SalidaT24RegistroModelo> T24_Registro(string icn, OperacionesSwiftModelo datos)
        {
            //Base De Datos
            SalidaT24RegistroModelo resp = new SalidaT24RegistroModelo();

            try
            {
                Config.EscribirLog($"INFO|{ConstantesServicios.TipoOperacion}.ProcesaOperacion|Inicia registro en T24.");


                string strMoneda = string.Empty;
                string strDescripcion = string.Empty;
                string strBanco = string.Empty;
                string strNOmbre_Beneficiario = string.Empty;
                string strNumero_Cuenta_Beneficiario = string.Empty;
                string strCuentaCompletaVostro = string.Empty;
                string[] strCuentaSeparadaVostro = new string[] { };
                string strCuentaDeb = string.Empty;
                string strSW_END2END_REF = string.Empty;
                string strSLA_UETR = string.Empty;

                //Inicio de Variables
                strMoneda = (datos.Cve_moneda.Equals("MXP") || datos.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : datos.Cve_moneda;
                strDescripcion = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml);

                strBanco = $"{await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)} - " +
                    $"{await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml)} ";

                strNOmbre_Beneficiario = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml);
                strNumero_Cuenta_Beneficiario = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml);
                strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                strCuentaDeb = strCuentaSeparadaVostro[2];

                //Armando la descripción
                strDescripcion = ConstantesServicios.DescripcionTransIntPacs009;

                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)))
                    strDescripcion += $", {ConstantesServicios.DescBancoOrigen} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}";

                //Vostro
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro)))
                    strDescripcion += $", BANCO DESTINO: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro)}";

                //Spei
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)))
                    strDescripcion += $", PAGO SPEI A: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)}";

                //Referencia
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)))
                    strDescripcion += $", REL. REFERENCE: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)}";

                //Validación de Modena
                if (!strMoneda.Equals(ConstantesServicios.ModenaDefaultMN))
                    throw new Exception($"No hay referencia del mensaje MT-202 para la moneda {strMoneda}.");

                //Preparamos los datos para el registro del FT

                MwT24TrxMaestro oTrxMaster = new MwT24TrxMaestro()
                {
                    STrxOperationSpecificType = ConstantesServicios.TipoOperacion,
                    STrxOperationSubType = ConstantesServicios.OperacionSubTipoPacs009,
                    STrxOperationFolio = icn,
                    STrxOperationAcct = strCuentaDeb
                };

                MwT24TrxDetalle oTrxDetailFF = new MwT24TrxDetalle()
                {
                    SDebit_acct_no = strCuentaDeb,
                    SDebit_currency = strMoneda,
                    SDebit_amount = string.Format("{0:0.##}", _SwfOperacion.Des_importe),
                    SDebit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_acct_no = ConstantesServicios.T24_BanxicoAcct,
                    SCredit_currency = strMoneda,
                    SOrdering_cust = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml),
                    SRelated_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SInstructions = strDescripcion,
                    SSatelSysRef = datos.Folio_unico,
                    SFolio_tomi = datos.Folio_unico,
                    SBeneficiary = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml),
                    SSW_END2END_REF = (await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.UETR)).Trim()

                };

                resp.BCorrecto = await _ConexionT24Mw.RegistraTrx(_SwfOperacion.Folio_unico, Convert.ToInt32(_Config.FechaJuliana), ConstantesServicios.TablaTrabajo, ConstantesServicios.TipoOperacion, icn, ConstantesServicios.TipoOperacion, oTrxDetailFF, oTrxMaster);

                resp.StrMensajeError = "";


                return resp;
            }
            catch (Exception ex)
            {
                resp.StrMensajeError = ex.Message;
                resp.BCorrecto = false;
            }

            return resp;
        }


        /// <summary>
        /// Generala informacion requerida para una transferencia VOSTRO y ejecuta el registro correspondiente en t24 
        /// considerando los datos del ordenante, beneficiario, modena y referencias de la operacion.
        /// </summary>
        /// <param name="icn">Folio identificador</param>
        /// <param name="datos">Datos de la operacion</param>
        private async Task<SalidaT24RegistroModelo> T24_Registro_Vostro(string icn, OperacionesSwiftModelo datos)
        {
            SalidaT24RegistroModelo resp = new SalidaT24RegistroModelo();

            try
            {
                string strMoneda = string.Empty;
                string strDescripcion = string.Empty;
                string strCuentaCompletaVostro = string.Empty;
                string[] strCuentaSeparadaVostro = new string[] { };
                string strOrd_SubAplicacion = string.Empty;
                string strOrd_Cuenta = string.Empty;
                string strOrd_Nombre = string.Empty;
                string strBen_SubAplicacion = string.Empty;
                string strBen_Cuenta = string.Empty;
                string strBen_Nombre = string.Empty;
                string strSW_END2END_REF = string.Empty;
                string strSLA_UETR = string.Empty;

                strMoneda = (datos.Cve_moneda.Equals("MXP") || datos.Cve_moneda.Equals("MXN")) ? ConstantesServicios.ModenaDefaultMN : datos.Cve_moneda;

                //Ordenante
                strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                strOrd_SubAplicacion = strCuentaSeparadaVostro[1];
                strOrd_Cuenta = strCuentaSeparadaVostro[2];
                strOrd_Nombre = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml);

                //Beneficiario
                strCuentaCompletaVostro = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCtaVostroVostro);
                strCuentaSeparadaVostro = strCuentaCompletaVostro.Split('-');
                strBen_SubAplicacion = strCuentaSeparadaVostro[1];
                strBen_Cuenta = strCuentaSeparadaVostro[2];
                strBen_Nombre = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro);

                //Armado de la descripción
                strDescripcion = ConstantesServicios.DescripcionTransIntPacs009Vostro;

                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)))
                    strDescripcion += $", {ConstantesServicios.DescBancoOrigen} {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}";

                //Vostro
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro)))
                    strDescripcion += $", BANCO DESTINO: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro)}";

                //Spei
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)))
                    strDescripcion += $", PAGO SPEI A: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)}";

                //Referencia
                if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)))
                    strDescripcion += $", REL. REFERENCE: {await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)}";

                //Preparamos los datos para el registro del FT

                MwT24TrxMaestro oTrxMaster = new MwT24TrxMaestro()
                {
                    STrxOperationSpecificType = ConstantesServicios.TipoOperacion,
                    STrxOperationSubType = ConstantesServicios.OperacionSubTipoPacs009Vostros,
                    STrxOperationFolio = icn,
                    STrxOperationAcct = strOrd_Cuenta
                };

                MwT24TrxDetalle oTrxDetailFF = new MwT24TrxDetalle()
                {
                    SDebit_acct_no = strOrd_Cuenta,
                    SDebit_currency = strMoneda,
                    SDebit_amount = string.Format("{0:0.##}", _SwfOperacion.Des_importe),
                    SDebit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_their_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SCredit_acct_no = strBen_Nombre,
                    SCredit_currency = strMoneda,
                    SRelated_ref = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.SenderReferenceXml),
                    SInstructions = strDescripcion,
                    SSatelSysRef = datos.Folio_unico,
                    SFolio_tomi = datos.Folio_unico,
                    SBeneficiary = await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.BtmumNombreVostro),
                    SSW_END2END_REF = (await _comunes.Buscar_Valor_Xml(_SwfOperacion.Nf_variables_xml, ConstantesServicios.UETR)).Trim()
                };

                resp.BCorrecto = await _comunes.Registra_TRX_WS(_SwfOperacion.Folio_unico, ConstantesServicios.TablaTrabajo, ConstantesServicios.TipoOperacion,
                    icn, strTipoOperacion, oTrxMaster, oTrxDetailFF, datos.Nf_variables_xml);

                resp.StrMensajeError = "";


                return resp;
            }
            catch (Exception ex)
            {
                resp.StrMensajeError = ex.Message;
                resp.BCorrecto = false;
            }
            return resp;
        }

        /// <summary>
        /// Ejecuta el proceso de registro de una operacion SPEI, validando el estado de la transaccion, determinando el tipo de paogo correspondiente, generando la informacion
        /// requerdia para el elnvio y almacenado de la operacion en el sitema SPEI.
        /// </summary>
        /// <param name="icn">Folio indentificador</param>
        /// <param name="datos">Datos de la operacion</param>
        private async Task<bool> Registra_SPEI(string icn, OperacionesSwiftModelo datos)
        {
            Config.EscribirLog("INFO|registra_SPEI|A registrar pago SPEI.");

            if ((await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumRegistroSPEI)).Equals(ConstantesServicios.EstatusPROC))
            {
                Config.EscribirLog("INFO|registra_SPEI|El pago ya fue registrado en SPEI");
                return true;
            }

            int iTipoPago = ConstantesServicios.SpeiTipoPago;
            int iTipoCuenta = 4;

            Config.EscribirLog($"INFO|registra_SPEI|A registrar en SPEI para el tipo de pago = {iTipoPago}");

            switch (iTipoPago)
            {
                case 3:
                    Config.EscribirLog("INFO|registra_SPEI|Tercero a tercero vostro.");
                    break;
                case 5:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a tercero.");
                    break;
                case 6:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a vostro.");
                    break;
                case 7:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a participante.");
                    break;
                default:
                    throw new Exception($"No existe definición para pago SPEI del tipo de pago: {iTipoPago}");
            }

            //Valores TAS-SPEI

            string strReceiverIndentifier = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveSwiftSpei);
            string strCreditorIdentifier = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntCif);
            string strCreditorAgentIdentifier = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.AwCif);
            string strIntermediaryIdentifier = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.IntCif);

            bool bSpeiInstitutionIsIntermediary = string.IsNullOrWhiteSpace(strIntermediaryIdentifier) && strIntermediaryIdentifier.Contains(strReceiverIndentifier);

            string strReferenciaNumerica = ConstantesServicios.ClaveCASFIM;

            if (!string.IsNullOrEmpty(await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml)))
            {
                string ValorTmp = string.Empty;

                ValorTmp = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml);


                if (int.TryParse(ValorTmp, out _))
                {
                    if (ValorTmp.Length > 7)
                        ValorTmp = ValorTmp.Substring(ValorTmp.Length - 7);

                    strReferenciaNumerica = ValorTmp;
                }
            }

            string strConcepto = (bSpeiInstitutionIsIntermediary == false) ?
                $"FROM {await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}"
                : $"FFC ACCT {await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntAccountXml)} {strCreditorIdentifier}";

            ComunesFXD _comunesFXD = new ComunesFXD(_OperacionesTomi, _Config);
            //Proceso para Generar la firma digital

            if ((await _comunesFXD.GeneraFirmaDigital(datos, iTipoPago, ConstantesServicios.TablaTrabajoMx, icn) == false))
                throw new Exception($"Se presentó un error al generar la firma digital del folio: {icn}");

            //Definicion de Procedimiento Almacenado

            InterfaceSPEIPacs009Modelo datoSpei = new InterfaceSPEIPacs009Modelo()
            {
                ins_clave = Convert.ToInt64(await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)),
                op_monto = datos.Des_importe,
                op_cve_rastreo = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SenderReferenceXml)
            };

            if (iTipoPago != 7)
            {
                string strOrderingCustomerName = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml);
                string strRawAccount = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCuentaVostroEmisor);
                strRawAccount = strRawAccount.Replace("-", "");

                datoSpei.op_nom_ord = await _comunesFXD.texto_SPEI(strOrderingCustomerName);
                datoSpei.op_cuenta_ord = await _comunesFXD.texto_SPEI(strRawAccount);

                if (bSpeiInstitutionIsIntermediary == false)
                {
                    datoSpei.op_nom_ben = await _comunesFXD.texto_SPEI(strCreditorIdentifier);

                    string strAccount = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntAccountXml);
                    int accountType = (await _comunes.EsCuentaCLABE(strAccount)) ? 40 : iTipoCuenta;

                    datoSpei.tc_clave_ben = accountType;
                    datoSpei.op_cuenta_ben = strAccount;
                    datoSpei.op_rfc_curp_ben = ConstantesServicios.ND;
                }
                else
                {
                    Config.EscribirLog("INFO|registra_SPEI|El banco intermediario es a quien se enviara el SPEI");

                    datoSpei.op_nom_ben = await _comunesFXD.texto_SPEI(strCreditorAgentIdentifier);
                    datoSpei.tc_clave_ben = iTipoCuenta;
                    datoSpei.op_cuenta_ben = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.AwCifAccount);
                    datoSpei.op_rfc_curp_ben = ConstantesServicios.ND;
                    datoSpei.op_nom_ben2 = await _comunesFXD.texto_SPEI(strCreditorIdentifier);
                    datoSpei.tc_clave_ben2 = iTipoCuenta;
                    datoSpei.op_cuenta_ben2 = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntAccountXml);
                    datoSpei.op_rfc_curp_ben2 = ConstantesServicios.ND;
                }

                datoSpei.op_concepto_pago = await _comunesFXD.texto_SPEI(strConcepto);
                datoSpei.op_ref_numerica = Convert.ToInt32(strReferenciaNumerica);
                datoSpei.usu_clave_int = 2;

                string strFirmaDigital = string.Empty;

                FirmaElectronicaDto rsDatosFirma = await _OperacionesTomi.ConsultarFirmaElectronicaMx(icn);

                if (rsDatosFirma != null)
                {
                    if (!string.IsNullOrEmpty(rsDatosFirma.Op_fimra_dig))
                        strFirmaDigital = rsDatosFirma.Op_fimra_dig;
                }

                if (string.IsNullOrEmpty(strFirmaDigital))
                    throw new Exception("No fue posible obtener el campo de firma digital");

                datoSpei.op_firma_dig = strFirmaDigital;

                //Termina Definición de Parametros y hace llamado
                Config.EscribirLog("INFO|registra_SPEI|Pasaron los parametros.");

                string NombreSP = $"{ProcedimientoAlmacenado.sp_InterfaceTomiSpeiDinamico}{iTipoPago}";
                string strCadenaRegreso = string.Empty;

                SalidaSPEIModelo salidaSPEI = await _OperacionesSpei.EjecutarSP_InterfaceTOMI_SPEI_PACS009(NombreSP, datoSpei);

                Config.EscribirLog("INFO|registra_SPEI|Se procesó procedimientos si errores.");

                if (salidaSPEI != null)
                {
                    strCadenaRegreso = $"{salidaSPEI.op_error}|{salidaSPEI.op_clave}|{salidaSPEI.op_fecha_oper}|{salidaSPEI.op_folio}|{salidaSPEI.op_descripcion}";
                }

                if (strCadenaRegreso.StartsWith("0|") || strCadenaRegreso.Contains("Ya existe una operación"))
                {
                    XDocument xml = XDocument.Parse(datos.Nf_variables_xml);
                    XmlHelper.AgregarElemento(xml, ConstantesServicios.BtmumRegistroSPEI, ConstantesServicios.EstatusPROC, null);
                    return true;
                }
                else
                    return false;
            }

            return true;
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Modelos;
using Newtonsoft.Json;
using System;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Aplicacion.Operaciones
{
    /// <summary>
    /// Gestiona el procesamiento de operaciones del tipo CAMT106, ejecutando validaciones, consultas e integraciones requeridas para su registro y 
    /// seguimiento
    /// </summary>
    public class CAMT106 : OperacionBase
    {
        string strTipoOperacion = string.Empty;
        string strError = string.Empty;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="CAMT106"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public CAMT106(ConfiguracionModelo configuracion, IOperaciones operacionesTomi, IOperacionesT24 operacionesT24, OperacionesSwiftModelo swiftOperacion, string TipoOperacion)
        {
            _Config = configuracion;
            _OperacionesTomi = operacionesTomi;
            _OperacionesT24 = operacionesT24;
            _SwfOperacion = swiftOperacion;
            strTipoOperacion = TipoOperacion;
        }

        /// <summary>
        /// Implementa la logica de procesamiento correspondiente al tipo de operacion definido, coordinando la ejecucion de validaciones,
        /// consultas e integraciones requeridas por el flujo de negocio.
        /// </summary>
        public async Task<bool> ProcesarOperacion()
        {
            _comunes = new OperacionesGenerales(_Config, _OperacionesTomi);
            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Iniciando el registro de la operación, folio_unico = {_SwfOperacion.Folio_unico}.");

            string strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);

            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A procesar el paso {strAliasProceso}.");

            if (strAliasProceso == ConstantesServicios.AliasT24Registro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Registrar Operación en T24");
                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                Result<string> result = Result<string>.Failed(string.Empty);
                Result<string> alreadyInsertedInT24 = await this.GetFT(_SwfOperacion.Folio);

                if (alreadyInsertedInT24.IsSuccess)
                {
                    Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|La búsqueda de FT fue exitosa");

                    if (string.IsNullOrWhiteSpace(alreadyInsertedInT24.Data))
                    {
                        Result<string> resultT24Insert = await T24_Registro(_SwfOperacion.Folio, _SwfOperacion);

                        if (!resultT24Insert.IsSuccess)
                        {
                            if (resultT24Insert.ErrorCode != ErrorCodeEnum.NoCode)
                            {
                                await _OperacionesTomi.Marca_Op_Con_Error(_SwfOperacion.Folio, resultT24Insert.Reason);
                                throw new Exception($"Error en procesamiento de registro CAMT106 T24_REGISTRO con folio: {_SwfOperacion.Folio}");
                            }
                            else
                            {
                                throw new Exception($"Error en procesamiento de registro CAMT106 T24_REGISTRO con folio: {_SwfOperacion.Folio}");
                            }
                        }

                        Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|No se encontró ningun FT, se procede a hacer el registro en T24");
                        result = resultT24Insert;
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Se encontro un FT asociado al mensaje: {alreadyInsertedInT24}");
                        result = alreadyInsertedInT24;
                    }
                }

                Result<string> updatedCommission = await ActualizaComisionContext(_SwfOperacion.Folio, result.Data);

                if (!updatedCommission.IsSuccess)
                {
                    throw new Exception($"No se pudo enviar el FT asociado al cobro de esta comision: {_SwfOperacion.Folio}  " +
                        $"Razon: {updatedCommission.Reason}");
                }

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);
                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);
            }

            if (strAliasProceso == ConstantesServicios.AliasSPEIRegistro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A registrar operación en SPEI");
                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                bool procesadoSpei = await Registra_SPEI(_SwfOperacion.Folio, _SwfOperacion);

                if (procesadoSpei == false && !string.IsNullOrEmpty(strError))
                    throw new Exception(strError);

                if (!procesadoSpei)
                    throw new Exception($"No se pudo registrar la operación en SPEI");

                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);
                strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);
            }

            if (strAliasProceso == ConstantesServicios.AliasSwiftRegistro)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A generar el mensaje MT-202");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);

                Result<string> dispatChResult = await DispatchMT202(_SwfOperacion.Folio);

                if (!dispatChResult.IsSuccess)
                {
                    Config.EscribirLog($"ERROR|{strTipoOperacion}.ProcesaOperacion|{dispatChResult}");
                    await _OperacionesTomi.Marca_Op_Con_Error(_SwfOperacion.Folio, "No se pudo enviar el MT202 asociado al cobro de esta comision");
                    throw new Exception($"No se pudo enviar el MT202 asociado al cobro de esta comision: {_SwfOperacion.Folio}");
                }

                return false;
            }

            strAliasProceso = await _OperacionesTomi.ObtenerUltimoProceso_SWF(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion);
           
            if (string.IsNullOrEmpty(strAliasProceso))
            {
                Config.EscribirLog($"INFO|Ocurrio un problema al obtener el ultimo proceso de la operación: {strTipoOperacion}");
                return false;
            }

            if (strAliasProceso == ConstantesServicios.AliasConciliar)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|A conciliar operaciones (por estatus) Entre TOMI y T24");

                await _OperacionesTomi.IniciarProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);
                await _OperacionesTomi.TerminaProceso_SFW(_SwfOperacion.Folio, ConstantesServicios.TipoOperacion, strAliasProceso);
                await _comunes.Marca_Op_Terminada(_SwfOperacion.Folio, ConstantesServicios.EstatusPROC, ConstantesServicios.CapturistaJeai);
            }

            Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Fin del Registro de la Operación");
            return true;
        }

        /// <summary>
        /// Construye y registra una transaccion de comision en T24 a partir de la informacion de la operacion procesada,
        /// generando las estucturas requeridas para el registro contable y operativo.
        /// </summary>
        /// <param name="icn">Folio de la operacion</param>
        /// <param name="datos">Datos de la operacion necesarios para la generacion de la transaccion</param>
        public async Task<Result<string>> T24_Registro(string icn, OperacionesSwiftModelo datos)
        {
            RegistroT24Modelo resultado = new RegistroT24Modelo();

            string t24InternalType = ConstantesServicios.TipoMensajeCAMT106;
            string xmlData = datos.Nf_variables_xml;

            try
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.ProcesaOperacion|Inicia registro en T24.");

                InternationalCommission.InternationalCommission commission = InternationalCommission.InternationalCommission.Map(xmlData);

                MwT24TrxMaestro loTrxMaster = new MwT24TrxMaestro()
                {
                    STrxOperationSpecificType = ConstantesServicios.TipoOperacion,
                    STrxOperationSubType = t24InternalType,
                    STrxOperationFolio = icn,
                    STrxOperationAcct = commission.DebitAccountNumber
                };

                StringBuilder instructions = new StringBuilder("COMISION BANCO BENEFICIARIO" + Environment.NewLine);
                instructions.Append($"{commission.OriginalReference}\t");
                instructions.Append($"{commission.OriginalDate}\t");
                instructions.Append($"COMM:\t{commission.CreditAmount.ToString("C", CultureInfo.CreateSpecificCulture("en-US"))}");
                instructions.Append($"\t{commission.CreditCurrency}");

                MwT24TrxDetalle loTrxDetailFT = new MwT24TrxDetalle()
                {
                    SCredit_acct_no = T24_NOSTRO_Account(commission.CreditCurrency),
                    SCredit_currency = commission.CreditCurrency,
                    SCredit_amount = commission.CreditAmount.ToString(),
                    SDebit_acct_no = commission.DebitAccountNumber,
                    SOrdering_cust = commission.OrderingCustomer,
                    SInstructions = instructions.ToString(),
                    SSatelSysRef = datos.Folio_unico,
                    SFolio_tomi = datos.Folio_unico

                };

                return await _comunes.registra_TRX_WS_Result(icn, strTipoOperacion, loTrxMaster, loTrxDetailFT, datos.Nf_variables_xml);

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{strTipoOperacion}.T24_REGISTRO|Se presenta exception: {ex.ToString()}");
            }
            return Result<string>.Failed("Hubo un error al intentar registrar la operacion");
        }

        /// <summary>
        /// Actualiza el contexto de cobro de comisiones asociando la referencia de T24 con la referencia original de la operacion.
        /// </summary>
        /// <param name="originalReference">Referencia original de la operacion</param>
        /// <param name="ourReferencia">Referencia generada en T24</param>
        public virtual async Task<Result<string>> ActualizaComisionContext(string originalReference, string ourReferencia)
        {
            if (string.IsNullOrWhiteSpace(ourReferencia))
            {
                Config.EscribirLog($"ERROR|CAMT106.UpdateCommissionContext|No T24 reference passed to update commission context.");
                return Result<string>.Failed("Error: No T24 reference passed to update commission context.");
            }

            if (string.IsNullOrWhiteSpace(ourReferencia))
            {
                Config.EscribirLog($"ERROR|CAMT106.UpdateCommissionContext|No SAMH reference passed to update commission context");
                return Result<string>.Failed("Error: No SAMH reference passed to update commission context.");
            }

            try
            {
                Config.EscribirLog($"INFO|CAMT106.UpdateCommissionContext|To update the Commission Context with the T24 reference");

                ChargesPaymentUpdateDto creditConfirmation = new ChargesPaymentUpdateDto()
                {
                    OriginalReference = originalReference,
                    T24Reference = ourReferencia
                };

                string json = JsonConvert.SerializeObject(creditConfirmation);
                Config.EscribirLog($"INFO|CAMT106.UpdateCommissionContext|Payload:\n{json}");

                using (var _client = new HttpClient())
                {
                    StringContent _content = new StringContent(json, Encoding.UTF8, "application/json");
                    Config.EscribirLog($"INFO|CAMT106.UpdateCommissionContext|To update Commission Context with context ref {creditConfirmation.OriginalReference} ");

                    HttpResponseMessage response = _client.PutAsync(_Config.StrCommApiUrl, _content).Result;
                    Config.EscribirLog($"INFO|CAMT106.UpdateCommissionContext|Server's answer: {response.ReasonPhrase}");

                    if (response.StatusCode == System.Net.HttpStatusCode.Accepted)
                        return Result<string>.Success(response.ReasonPhrase);
                    else
                        return Result<string>.Failed($"Error: {response.ReasonPhrase}");
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|CAMT106.UpdateCommissionContext|An error occurred: {ex.Message}");
                return Result<string>.Failed($"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Solicita la generacion del mensaje MT202 requerido para concluir el proceso de cobor de comisión
        /// </summary>
        public async Task<Result<string>> DispatchMT202(string folio)
        {
            try
            {
                Config.EscribirLog($"ERROR|CAMT106.DispatchMT202|A enviar MT202 para terminar el cobro de la comision.");
                FiToFiCommissionCreditConfirmationDto creditConfirmation = new FiToFiCommissionCreditConfirmationDto()
                {
                    TheirReference = folio
                };

                string json = JsonConvert.SerializeObject(creditConfirmation);

                Config.EscribirLog($"ERROR|CAMT106.DispatchMT202|Payload:\n {json}");

                using (var _client = new HttpClient())
                {
                    StringContent _content = new StringContent(json, Encoding.UTF8, "application/json");
                    Config.EscribirLog($"INFO|CAMT106.UpdateCommissionContext|To enqueue dispatching of MT202 with context ref {creditConfirmation.TheirReference} ");

                    HttpResponseMessage response = _client.PutAsync(_Config.StrCommApiUrl + "/confirmation", _content).Result;
                    Config.EscribirLog($"INFO|CAMT106.UpdateCommissionContext|Respuesta del servidor: {response.ReasonPhrase}");

                    if (response.StatusCode == System.Net.HttpStatusCode.Accepted)
                        return Result<string>.Success(response.ReasonPhrase);

                    return Result<string>.Failed(response.ReasonPhrase);
                }

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|CAMT106.DispatchMT202|An error occurred: {ex.Message}");
                return Result<string>.Failed(ex.Message);
            }
        }

        /// <summary>
        /// Ejecuta el proceso de registro de una operacion SPEI, validando el estado de la transaccion, determinando el tipo de paogo correspondiente, generando la informacion
        /// requerdia para el elnvio y almacenado de la operacion en el sitema SPEI.
        /// </summary>
        /// <param name="icn">Folio indentificador</param>
        /// <param name="datos">Datos de la operacion</param>
        public async Task<bool> Registra_SPEI(string icn, OperacionesSwiftModelo datos)
        {
            string CadenaRegreso = string.Empty;
            string lsNomBeneficiario = string.Empty;

            Config.EscribirLog("INFO|registra_SPEI|A registrar pago SPEI.");

            if ((await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumRegistroSPEI)).Equals(ConstantesServicios.EstatusPROC))
            {
                Config.EscribirLog("INFO|registra_SPEI|El pago ya fue registrado en SPEI.");
                return true;
            }

            //Tipo de pago SPEI
            int liTipoPago = 3;
            int iTipoCuenta = 4;

            Config.EscribirLog($"INFO|registra_SPEI|A registrar en SPEI para el tipo de pago={liTipoPago}.");

            switch (liTipoPago)
            {
                case 3:
                    Config.EscribirLog("INFO|registra_SPEI|Tercero a tercero vostro.");
                    break;
                case 5:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a tercero.");
                    break;
                case 6:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a vostro.");
                    break;
                case 7:
                    Config.EscribirLog("INFO|registra_SPEI|Participante a participante.");
                    break;
                default:
                    throw new Exception($"No existe definición para pago SPEI del tipo de pago {liTipoPago}");
            }

            //Valores TAS - SPEI
            int liRefNumerica = Convert.ToInt32(ConstantesServicios.ClaveCASFIM);

            if (!(await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml) != ""
                && double.TryParse(await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml), out _)))
            {
                string ValorTmp = string.Empty;

                ValorTmp = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml);

                if (double.TryParse(await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.RelatedReferenceXml), out _))
                {
                    if (ValorTmp.Length > 7)
                        ValorTmp = ValorTmp.Substring(ValorTmp.Length - 7);

                    liRefNumerica = int.Parse(ValorTmp);
                }
            }

            string concepto = $"FROM {await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreEmisorXml)}";

            ComunesFXD _comunesFXD = new ComunesFXD(_OperacionesTomi, _Config);

            if ((await _comunesFXD.GeneraFirmaDigital(datos, liTipoPago, ConstantesServicios.TablaTrabajoMX, icn)) == false)
            {
                throw new Exception($"Se presentó un error al generar la firma digital del folio: {icn}");
            }

            //Definicion de procedimiento Almacenado
            InterfaceSPEIModelo insertSpei = new InterfaceSPEIModelo()
            {
                ins_clave = Convert.ToInt64(await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumCveCasfimSPeiXml)),
                op_monto = datos.Des_importe,
                op_cve_rastreo = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.SenderReferenceXml)
            };

            // De acuerdo al tipo de pago son los siguientes parametros
            if (liTipoPago != 7) // No enviar los parametros para Participante - Participante
            {
                // Nombre del beneficiario
                lsNomBeneficiario = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BtmumNombreBancoSpeiXml); // Nombre del banco beneficiario (por default)
                if (await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntCif) != "")
                {
                    string identificador = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntCif);
                    CIFSwiftModelo rsDatos = await _OperacionesTomi.ConsultarNombreCIF(identificador, 2);

                    if (rsDatos != null)
                    {
                        lsNomBeneficiario = rsDatos.Des_nombre;
                    }
                }

                insertSpei.op_nom_ben = await _comunesFXD.texto_SPEI(lsNomBeneficiario);
                insertSpei.tc_clave_ben = iTipoCuenta;
                insertSpei.op_cuenta_ben = await _comunes.Buscar_Valor_Xml(datos.Nf_variables_xml, ConstantesServicios.BenIntAccountXml);
                insertSpei.op_rfc_curp_ben = ConstantesServicios.ND;
            }

            insertSpei.op_concepto_pago = await _comunesFXD.texto_SPEI(concepto);
            insertSpei.op_ref_numerica = liRefNumerica;
            insertSpei.usu_clave_int = 2;

            string strFirmaDigital = string.Empty;

            FirmaElectronicaDto rsDatosFirma = await _OperacionesTomi.ConsultarFirmaElectronica(icn);
            if (rsDatosFirma != null)
            {
                if (!string.IsNullOrEmpty(rsDatosFirma.Op_fimra_dig))
                    strFirmaDigital = rsDatosFirma.Op_fimra_dig;
            }

            if (string.IsNullOrEmpty(strFirmaDigital))
                throw new Exception("No fue posible obtener el campo de firma digital");

            insertSpei.op_firma_dig = strFirmaDigital;

            //Termina Definición de Parametros y hace llamado
            Config.EscribirLog("INFO|registra_SPEI|Pasaron los parametros.");

            string NombreSP = $"{ProcedimientoAlmacenado.sp_InterfaceTomiSpeiDinamico}{liTipoPago}";
            string strCadenaRegreso = string.Empty;

            SalidaSPEIModelo salidaSPEI = await _OperacionesSpei.EjecutarSP_InterfaceTOMI_SPEI(NombreSP, insertSpei);

            Config.EscribirLog("INFO|registra_SPEI|Se procesó procedimientos si errores.");

            if (salidaSPEI != null)
            {
                strCadenaRegreso = $"{salidaSPEI.op_error}|{salidaSPEI.op_clave}|{salidaSPEI.op_fecha_oper}|{salidaSPEI.op_folio}|{salidaSPEI.op_descripcion}";
            }

            if (strCadenaRegreso.StartsWith("0|") || strCadenaRegreso.Contains("Ya existe una operación"))
            {
                XDocument xml = XDocument.Parse(datos.Nf_variables_xml);
                XmlHelper.AgregarElemento(xml, ConstantesServicios.BtmumRegistroSPEI, ConstantesServicios.EstatusPROC, null);
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Obtiene la referencia de la transaccion generada en T24 para la operacion indicada, permitiendo validar si el 
        /// registrado ya fue procesado.
        /// </summary>
        public async Task<Result<string>> GetFT(string rfk)
        {
            try
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.GetFT|A buscar FT para la comisión.");
                SwfPlanTrabajoMestroModelo resultado = await _OperacionesTomi.ConsultarExisteFT(rfk);
                Config.EscribirLog($"INFO|{strTipoOperacion}.GetFT|Se ejecutó el Query");

                if (string.IsNullOrEmpty(resultado.T24_Ref))
                    return Result<string>.Success(string.Empty);

                return Result<string>.Success(resultado.T24_Ref);

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|{strTipoOperacion}.GetFT|Se Presenta la Excepcion {ex.Message}");
                return Result<string>.Failed(ex.Message);
            }
        }

        /// <summary>
        /// Resuelve la cuenta nostro que debera utilizarse en T24 de acuerdo con la moneda de la operacion
        /// </summary>
        private string T24_NOSTRO_Account(string strCurrency)
        {
            string valor = string.Empty;

            switch (strCurrency)
            {
                case ConstantesServicios.USD:
                    valor = ConstantesServicios.ValorUSD;
                    break;
                case ConstantesServicios.EUR:
                    valor = ConstantesServicios.EUR;
                    break;
                case ConstantesServicios.JPY:
                    valor = ConstantesServicios.ValorJPY;
                    break;
            }

            return valor;
        }
    }
}

using Aplicacion.Enums;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Aplicacion.FirmaElectronica
{
    /// <summary>
    /// Proporciona metodos para generar y validar firmas digitales 
    /// utilizando certificados X.509 y algoritmos criptograficos RSA.
    /// </summary>
    public static class MetodosFirma
    {
        /// <summary>
        /// Genera una firma digital de la trama proporcionada utilizando la llave privada del certificado configurado y valida el resultado
        /// mediante la llave publica asociada.
        /// </summary>
        /// <param name="tramaCompleta">Contenido de la trama en formato de arreglo de bytes que sera firmado.</param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static string GenerateSign(byte[] tramaCompleta, string certPath, string certPass)
        {
            string resultBase64 = string.Empty;
            try
            {
                Config.EscribirLog("INFO|MetodosFirma.GenerateSign|Nombre del archivo p12 : " + certPath);

                X509Certificate2Collection collection = new X509Certificate2Collection();
                collection.Import(certPath, certPass, X509KeyStorageFlags.Exportable);

                X509Certificate2 cert = collection[0];

                RSACryptoServiceProvider publicKey =
                    (RSACryptoServiceProvider)cert.PublicKey.Key;

                RSACryptoServiceProvider privateKey = new RSACryptoServiceProvider();

                Config.EscribirLog("INFO|MetodosFirma.GenerateSign|Se obtienen las llaves pública y privada");

                privateKey.FromXmlString(cert.PrivateKey.ToXmlString(true));

                byte[] data = tramaCompleta;

                byte[] signature = privateKey.SignData(
                    data,
                    CryptoConfig.MapNameToOID("SHA256")
                );

                bool isValid = publicKey.VerifyData(
                    data,
                    CryptoConfig.MapNameToOID("SHA256"),
                    signature
                );

                if (isValid)
                {
                    Config.EscribirLog("INFO|MetodosFirma.GenerateSign|Trama firmada exitosamente");
                    Config.EscribirLog("INFO|MetodosFirma.GenerateSign|Conversion Base 64");

                    resultBase64 = Convert.ToBase64String(signature, 0, signature.Length);

                    Config.EscribirLog($"INFO|MetodosFirma.GenerateSign|Resultado conversión Base 64 : {resultBase64} ");
                }
            }
            catch (Exception ex)
            {
                throw new Exception("GenerateSign|Se presenta exception: " + ex);
            }

            return resultBase64;
        }

        /// <summary>
        /// Verifica que el certificado configurado en la aplicacion exista y cuente con llave privada asociada para realizar 
        /// operaciones de firma digital.
        /// </summary>
        /// <exception cref="Exception">Se produce cuando ocurre un error durante la carga o validacion del certificados.</exception>
        public static bool CertificadoEncontrado(string certPath, string certPass)
        {
            try
            {
                X509Certificate2Collection collection = new X509Certificate2Collection();
                collection.Import(certPath, certPass, X509KeyStorageFlags.Exportable);
                X509Certificate2 cert = collection[0];
                return cert.HasPrivateKey;
            }
            catch (Exception ex)
            {
                throw new Exception("CertificadoEncontrado|Se presenta exception: " + ex);
            }
        }
    }
}

using System;
using System.IO;
using System.Text;

/// <summary>
/// Proporciona metodos para convertir distintos tipos de datos a arreglos de bytes, considerando  el orden de los bytes requerido para la integracion 
/// y transmision de informacion.
/// </summary>
public static class MetodosConversionBytes
{
    /// <summary>
    /// Convierte un valor entero a un arreglo de bytes utilizando el orden de bytes especificado. 
    /// </summary>
    /// <param name="value">valor enteror a covertir</param>
    /// <param name="order">Orden de bytes que se aplicara deurante la conversion</param>
    /// <exception cref="Exception">Se produce cuando ocurre un error durante la conversion</exception>
    public static byte[] IntToByteArray(int value, ByteOrder order)
    {
        try
        {
            byte[] bytes;
            using (MemoryStream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                writer.Write(value);
                bytes = stream.ToArray();
            }

            if ((BitConverter.IsLittleEndian && order == ByteOrder.BIG_ENDIAN) ||
                (!BitConverter.IsLittleEndian && order == ByteOrder.LITTLE_ENDIAN))
            {
                Array.Reverse(bytes);
            }

            return bytes;
        }
        catch (Exception ex)
        {
            throw new Exception("intToByteArray|Se presenta exception: " + ex);
        }
    }


    /// <summary>
    /// Convierte una fecha representada en formato numerico a un arreglo de bytes adecuado para su procesamiento o transmision.
    /// </summary>
    /// <param name="fecha">fecha en formato numerico</param>
    /// <param name="order">Orden de bytes que se aplicara en la conversion</param>
    /// <exception cref="Exception">Se produce cuando ocurre un error durante la conversion</exception>
    public static byte[] DateToByteArray(int fecha, ByteOrder order)
    {
        try
        {
            byte[] bytes = new byte[4];

            int dia = fecha % 100;
            fecha -= dia;
            int mes = (fecha / 100) % 100;
            int anio = (fecha - mes) / 10000;

            bytes[0] = (byte)dia;
            bytes[1] = (byte)mes;

            byte[] bytesAnio = BitConverter.GetBytes((ushort)anio);

            if ((BitConverter.IsLittleEndian && order == ByteOrder.BIG_ENDIAN) ||
                (!BitConverter.IsLittleEndian && order == ByteOrder.LITTLE_ENDIAN))
            {
                Array.Reverse(bytesAnio);
            }

            bytes[2] = bytesAnio[0];
            bytes[3] = bytesAnio[1];

            return bytes;
        }
        catch (Exception ex)
        {
            throw new Exception("dateToByteArray|Se presenta exception: " + ex);
        }
    }

    /// <summary>
    /// Obtiene la representacion binaria de un valor de texto utilizando la codificacion requerida por los procesos de integracion.
    /// </summary>
    /// <param name="value">Valor de texto a convertir</param>
    /// <param name="order">Orden de bytes utilizado para la conversion</param>
    /// <exception cref="Exception">Se produce cuando ocurre un error durante la conversion</exception>
    public static byte[] StringToByteArray(string value, ByteOrder order)
    {
        try
        {
            Encoding encoding = Encoding.GetEncoding(850);
            byte[] bytes = encoding.GetBytes(value ?? string.Empty);
            Array.Resize(ref bytes, bytes.Length + 1);
            return bytes;
        }
        catch (Exception ex)
        {
            throw new Exception("stringToByteArray|Se presenta exception: " + ex);
        }
    }

    /// <summary>
    /// Convierte  un importe monetario a su representacion binaria, aplicando el formato requerido y el orden de bytes especificado
    /// </summary>
    /// <param name="value">Importe monetaria a convertir</param>
    /// <param name="order">Orden de bytes que se utilizara durante la conversion</param>
    /// <returns></returns>
    /// <exception cref="Exception">Se produce cuando ocurre un error durante la conversion</exception>
    public static byte[] MoneyToByteArray(double value, ByteOrder order)
    {
        try
        {
            decimal decimalR = Math.Round((decimal)value, 2);
            long[] lngMonto = DoubleToLong(decimalR);

            byte[] p1 = BitConverter.GetBytes(lngMonto[0]);
            byte[] p2 = BitConverter.GetBytes(lngMonto[1]);

            if ((BitConverter.IsLittleEndian && order == ByteOrder.BIG_ENDIAN) ||
                (!BitConverter.IsLittleEndian && order == ByteOrder.LITTLE_ENDIAN))
            {
                Array.Reverse(p1);
                Array.Reverse(p2);
            }

            byte[] bytes = new byte[16];
            Buffer.BlockCopy(p1, 0, bytes, 0, 8);
            Buffer.BlockCopy(p2, 0, bytes, 8, 8);

            byte[] array1, array2;

            if (order == ByteOrder.BIG_ENDIAN)
            {
                array1 = GetData(bytes, 4, 8);
                array2 = GetData(bytes, 12, 16);
            }
            else
            {
                array1 = GetData(bytes, 0, 4);
                array2 = GetData(bytes, 8, 12);
            }

            byte[] result = new byte[8];
            Buffer.BlockCopy(array1, 0, result, 0, 4);
            Buffer.BlockCopy(array2, 0, result, 4, 4);

            return result;
        }
        catch (Exception ex)
        {
            throw new Exception("moneyToByteArray|Se presenta exception: " + ex);
        }
    }

    /// <summary>
    /// Descompone un importe monetario en sus componentes numericos para facilitar su representacion binaria.
    /// </summary>
    /// <param name="monto">Importe monetario</param>
    public static long[] DoubleToLong(decimal monto)
    {
        bool negativo = monto < 0;
        if (negativo) monto = Math.Abs(monto);

        string strMonto = monto.ToString("0.00").Replace(".", "").Replace(",", "");
        int digitos = 9;

        long[] retorno = new long[2];

        if (strMonto.Length >= digitos + 1)
        {
            retorno[0] = long.Parse(strMonto.Substring(0, strMonto.Length - digitos));
            retorno[1] = long.Parse(strMonto.Substring(strMonto.Length - digitos));
            if (negativo) retorno[0] *= -1;
        }
        else
        {
            retorno[1] = long.Parse(strMonto);
            if (negativo) retorno[1] *= -1;
        }

        return retorno;
    }

    /// <summary>
    /// Obtiene un segmnento de bytes a partir de las posiciones especificadas dentro de una trama.
    /// </summary>
    /// <param name="trama">Arreglo de bytes de origen</param>
    /// <param name="initIndex">Posicion inicial desde donde comenzara la extraccion</param>
    /// <param name="finalIndex">Posicion final utilizada para calcular la longitud del segmento</param>
    /// <returns></returns>
    /// <exception cref="IndexOutOfRangeException"></exception>
    public static byte[] GetData(byte[] trama, int initIndex, int finalIndex)
    {
        try
        {
            int length = finalIndex - initIndex;
            byte[] result = new byte[length];
            Array.Copy(trama, initIndex, result, 0, length);
            return result;
        }
        catch (Exception ex)
        {
            throw new IndexOutOfRangeException("El indice excede el array de la trama: " + ex.Message);
        }
    }

    /// <summary>
    /// Agrega un byte adicional al final del arreglo recibido.
    /// </summary>
    /// <param name="bytes">Arreglo de bytes que sera redimensionado</param>
    public static void AddZeroByte(ref byte[] bytes)
    {
        Array.Resize(ref bytes, bytes.Length + 1);
    }
}

/// <summary>
/// Define el orden de almacenamiento de los bytes utilizado durante los procesos de conversion.
/// </summary>
public enum ByteOrder
{
    BIG_ENDIAN,
    LITTLE_ENDIAN
}

using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Aplicacion.InternationalCommission
{
    /// <summary>
    /// Representa la informacion requerida para el calculo y procesamiento de comisiones internacionales, incluyendo 
    /// los datos de las cuentas, monedas, importe y referencias de la operacion.
    /// </summary>
    public class InternationalCommission
    {
        public string TransactionType { get; set; }
        public string CreditCurrency { get; set; }
        public double CreditAmount { get; set; }
        public double CalculatedDebitAmount { get; set; }
        public string DebitAccountCurrency { get; set; }
        public string DebitAccountNumber { get; set; }
        public string OrderingCustomer { get; set; }
        public string CreditAccountNumber { get; set; }
        public string OriginalReference { get; set; }
        public string OriginalDate { get; set; }


        /// <summary>
        /// Construye una instancia de <see cref="InternationalCommission"/>
        /// a partir de informacion contenida en un documento XML, validando la existencia de los datos obligatorios
        /// </summary>
        /// <param name="xml">Contenido XML que contiene la informacion de la operacion</param>
        /// <exception cref="Exception">Se produce cuando alguno de los datos obligatorios no se encuentra presente en la informacion recibida</exception>
        public static InternationalCommission Map(string xml)
        {
            var dto = new InternationalCommission
            {
                TransactionType = "AC22",
                DebitAccountCurrency = busca_valor_xml(xml, "chargeAccountCurrency"),
                DebitAccountNumber = busca_valor_xml(xml, "chargeAccount"),
                CreditCurrency = busca_valor_xml(xml, "mensaje_moneda"),
                CreditAmount = Convert.ToDouble(busca_valor_xml(xml, "mensaje_importe")),
                OrderingCustomer = busca_valor_xml(xml, "identifier_code"),
                OriginalReference = busca_valor_xml(xml, "our_related_ref"),
                OriginalDate = busca_valor_xml(xml, "original_date")
            };

            if (string.IsNullOrWhiteSpace(dto.CreditCurrency))
                throw new Exception("Sin datos necesarios para completar la operacion.");

            if (dto.CreditAmount == 0)
                throw new Exception("Sin datos necesarios para completar la operacion.");

            if (string.IsNullOrWhiteSpace(dto.DebitAccountCurrency))
                throw new Exception("Sin datos necesarios para completar la operacion.");

            if (string.IsNullOrWhiteSpace(dto.DebitAccountNumber))
                throw new Exception("Sin datos necesarios para completar la operacion.");

            if (string.IsNullOrWhiteSpace(dto.OriginalReference))
                throw new Exception("Sin datos necesarios para completar la operacion.");

            return dto;
        }

        /// <summary>
        /// Extrae y valida la informacion necesaria para el procesamiento de comisiones internacionales a partir de los datos contenidos
        /// en el XML de la operacion
        /// </summary>
        private static string busca_valor_xml(string strXml, string strCampo)
        {
            string valor = string.Empty;

            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(strXml);

            var nodo = xmlDoc.DocumentElement;

            string _campo = nodo[strCampo]?.InnerText;

            valor = _campo;

            return valor;
        }

    }
}

using MUFG_T24.Configuracion_utilidades;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa una entidad de datos utilizada para el intercambio y almacenamiento de informacion
    /// dentro de la apliacion.
    /// </summary>
    public class ConfiguracionModelo : Configuracion
    {
        public bool BoEsDiaHabil { get; set; }
        public TimeSpan StrHoraOpInicio { get; set; }
        public TimeSpan StrHoraOpFin { get; set; }
        public string StrDigSigCertNumSer { get; set; }
        public string StrDigSigCertPsw { get; set; }
        public string StrSinFondosHoraValida { get; set; }
        public string StrSinFondosTiempoEspera { get; set; }
        public string StrNombreSPEI2 { get; set; }
        public string StrWst24MwPasswordSwfMx { get; set; }
        public string StrWsT24MwCompany { get; set; }
        public string StrCommApiUrl { get; set; }
        public TimeSpan StrFechaSistema { get; set; }
        public bool BHorarioLectura { get; set; }

        public string StrSwfCriterioCompleto { get; set; }
        public int IntFechaSistema { get; set; }
        public string FechaJuliana { get; internal set; }

    }
}

using System.Globalization;
using System.Runtime.Remoting.Metadata.W3cXsd2001;

namespace Aplicacion.Enums
{
    /// <summary>
    /// Constantes que contienen valores para el flujo operativo.
    /// </summary>
    public class ConstantesServicios
    {
        public const string NombreServicio = "Swf_FT";
        public const string TipoOperacion = "SWIFT_IN";
        public const string TipoOperacionEsquemas = "SWF_IN";
        public const string ModenaDefaultMN = "MXN";
        public const string ClaveCASFIM = "40108";
        public const string TablaTrabajo = "TB_SRV_SWF_TRABAJO";
        public const string T24_BanxicoAcct = "5990060";
        public const string CASFIM_MUFG = "40108";
        public const string TablaTrabajoMx = "TB_SRV_SWF_TRABAJO_MX";


        #region Alias de Proceso

        public const string AliasTomiEtradasSalidas = "TOMI_ENTRADAS/SALIDAS";
        public const string AliasT24Registro = "T24_REGISTRO";
        public const string AliasConciliar = "CONCILIAR";
        public const string AliasSPEIRegistro = "SPEI_REGISTRO";
        public const string AliasSwiftRegistro = "SWIFT_REGISTRO";

        #endregion

        #region Mensajes Swift

        public const string TipoMensajeMT103 = "MT-103";
        public const string TipoMensajeMT191 = "MT-191";
        public const string TipoMensajeMT202 = "MT-202";
        public const string TipoMensajeMT210 = "MT-210";
        public const string TipoMensajeMT950 = "MT-950";
        public const string TipoMensajeCAMT106 = "CAMT106";
        public const string TipoMensajePACS008 = "PACS008";
        public const string TipoMensajePACS009 = "PACS009";

        #endregion

        public const string Operacion218 = "218";
        public const string OperacionDescEntradaTTS = "Entrada (TTS)";
        public const string OperacionDescSalidaTTS = "Salida (TTS)";
        public const string OpTipoInternacional = "I";
        public const string OpTipoInternacionalDesc = "Internacional";
        public const string ViaSubAplicacion810 = "810";
        public const string ViaSubAplicacion804 = "804";
        public const string ViaSubAplicacion800 = "800";
        public const string ViaSubAplicacion810Desc = "TRF RECIBIDA";
        public const string ViaSubAplicacion804Desc = "TRF ENVIADA MXN SPEI (COM USD)";
        public const string ViaSubAplicacion800Desc = "TRF ENVIADA MXN SPEI";
        public const string CapturistaJeai = "JEAI";

        public const string EstatusPROC = "PROC";
        public const string DetalleCargoOUR = "OUR";
        public const string T24TipoINternoMT103Com = "MT103_COM";
        public const string T24TipoINternoPacs008 = "PACS008_COM";

        public const string Separador = "|";

        public const string EstatusT24Pend = "PEND";
        public const string EstatusT24Error = "ERROR";
        public const string EstatusScnp = "SCNP";
        public const string EstatusConf = "CONF";

        public const string OrdenanteBotk = "BOTK";
        public const string OrdenanteBofCus33 = "BOFCUS33";
        public const string OrdenanteBotKus33 = "BOTKUS33";
        public const string TipoDomestica = "D";
        public const string TipoDomesticaDesc = "Doméstica";

        public const string OperacionSubTipoMt202 = "MT202_SPEI";
        public const string OperacionSubTipoMt202Vostros = "MT202_VOSTROS";
        public const string OperacionSubTipoPacs009 = "PACS009_SPEI";
        public const string OperacionSubTipoPacs009Vostros = "PACS009_VOSTROS";
        public const int SpeiTipoPago = 3;
        public const int SpeiTipoCuenta = 4;

        #region CamposXml

        public const string InstOrdenanteXml = "order_inst_party";
        public const string NombreInstOrdenanteXml = "order_inst_name_addr_1";
        public const string DireccionInstOrdenanteXml = "order_inst_name_addr_2";
        public const string DireccionInstOrdenanteXml2 = "order_inst_name_addr_3";
        public const string CIFOrdenanteXml = "order_inst_cif";
        public const string BtmumNombreEmisorXml = "btmum_nombre_emisor";
        public const string OrdenanteXml = "ordering_name_addr_1";
        public const string DetalleCargosXml = "detail_charges";
        public const string ConceptoXml = "remmitance_information";
        public const string BtmumCveSwiftEmisor = "btmum_cve_swift_emisor";
        public const string SenderReferenceXml = "sender_reference";
        public const string BeneficiarioXml = "beneficiary_name_addr";
        public const string Beneficiario2Xml = "beneficiary_name_addr_1";
        public const string BtmumCveSwiftVostroXml = "btmum_cve_swift_vostro";
        public const string SenderreferenceXml = "sender_reference";
        public const string BtmumCuentaVostroEmisor = "btmum_cta_vostro_emisor";
        public const string OrderInstNameXml = "order_inst_name";
        public const string OrderingNameXml = "ordering_name";
        public const string BtmumCtaVostroVostro = "btmum_cta_vostro_vostro";
        public const string RelatedReferenceXml = "related_reference";
        public const string BtmumCveCasfimSPeiXml = "btmum_cve_casfim_spei";
        public const string BtmumNombreBancoSpeiXml = "btmum_nombre_banco_spei";
        public const string BtmumNombreVostro = "btmum_nombre_vostro";
        public const string BtmumRegistroSPEI = "btmum_registro_SPEI";
        public const string BenIntAccountXml = "ben_int_account";
        public const string BtmumCveSwiftSpei = "btmum_cve_swift_spei";
        public const string BenIntCif = "ben_int_cif";
        public const string AwCif = "aw_cif";
        public const string IntCif = "int_cif";
        public const string AwCifAccount = "aw_cif_account";
        public const string DebitAccount = "debit_account";
        public const string OrderingAccount = "ordering_account";
        public const string OrderingNameAddr1 = "ordering_name_addr_1";
        public const string OrderingNameAddr2 = "ordering_name_addr_2";
        public const string OrderingNameAddr3 = "ordering_name_addr_3";
        public const string SLA_UETR = "SLA_UETR";
        public const string UETR = "UETR";


        #endregion

        public const string DescripcionTransIntMt103 = "RECEPCION TRANSFERENCIA INTERNACIONAL MT-103";
        public const string DescripcionTransIntPacs008 = "RECEPCION TRANSFERENCIA INTERNACIONAL PACS008.";
        public const string DescripcionTransIntMt202 = "RECEPCION TRANSFERENCIA INTERNACIONAL MT-202 SALIDA SPEI.";
        public const string DescripcionTransIntMt202Vostro = "RECEPCION TRANSFERENCIA INTERNACIONAL MT-202 VOSTROS.";
        public const string DescripcionTransIntPacs009 = "RECEPCION TRANSFERENCIA INTERNACIONAL PACS009 SALIDA SPEI.";
        public const string DescripcionTransIntPacs009Vostro = "RECEPCION TRANSFERENCIA INTERNACIONAL PACS009 VOSTROS.";
        public const string DescBancoOrdenante = "BANCO ORDENANTE: ";
        public const string DescBancoOrigen = "BANCO ORIGEN: ";
        public const string DescOrdenante = "ORDENANTE: ";
        public const string DescCargos = "DET. CHARGES: ";
        public const string DescConcepto = "POR CONCEPTO DE: ";

        public const string ND = "ND";
        public const string TablaTrabajoMX = "TB_SRV_SWF_TRABAJO_MX";

        public const string USD = "USD";
        public const string ValorUSD = "5990160";
        public const string EUR = "EUR";
        public const string ValorEUR = "5990140";
        public const string JPY = "JPY";
        public const string ValorJPY = "5990150";


        public const string FXD = "FXD";
        public const string UsuarioTomi = "TOMI";
        public const string TipoCuentaCLABE = "CUENTA DE CHEQUES(CLABE)";
        public const string MUFG = "MUFG";
        public const string SinEsquema = "SIN_ESQUEMA";
        public const string ConEsquema = "CON_ESQUEMA";


    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Enums
{
    /// <summary>
    /// Constantes que contienen lo objetos de base de datos (Interfacest24, SPEI).
    /// </summary>
    public class ProcedimientoAlmacenado
    {
        public const string sp_ObtenerFechaSistema = "dbo.usp_Comun_ObtenerFechaSistema";
        public const string sp_Fecha_Sistema = "usp_ObtenerFechaSistema";
        public const string sp_EsDiahabil = "dbo.usp_EsDiaHabil";
        public const string sp_ObtenerFechaSistemaT24 = "dbo.usp_ConsultarFechaJulianaT24";
        public const string sp_Parametros_Sistema = "dbo.usp_Comun_ObtenerParametrosSistema";
        public const string sp_CerrarOperacionesAnteriores = "dbo.usp_CerrarOperacionesAnteriores_Swift_In_Ft";
        public const string sp_ActualizaPlanTrabajo = "dbo.usp_ActualizaPlanTrabajo_Swift_In_Ft";
        public const string sp_Swift_FT_Operaciones = "dbo.usp_swf_ft_ops";
        public const string sp_UltimoProcesoSwift = "dbo.usp_ConsultaUltimoProcesoSwift_MUFGLibreria";
        public const string sp_IniciaProcesoSwift = "dbo.usp_IniciaProceso_Swift_In_Ft";
        public const string sp_ConsultarNumRegsPlanTrabajoDetalleES = "dbo.usp_ConsultarNumRegistrosPlanTrabajoDetalleE_S";
        public const string sp_InsertarTombinEntradas_Salidas = "dbo.usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
        public const string sp_TerminaProcesoSwift = "dbo.usp_TerminaProceso_Swift_In_Ft";
        public const string sp_ConsultarExisteFolioT24 = "dbo.usp_ConsultarExisteFolioT24MW_Swift_In";
        public const string sp_ConsultarFolioSwiftT24 = "dbo.usp_ConsultarFolioT24_Swift_In_FT";
        public const string sp_MarcaOpTerminada = "usp_swf_marca_op_terminada";
        public const string sp_ConsultaCIFDatos = "usp_swf_get_cif_datos";
        public const string sp_EjecutaTranBtmum210 = "dbo.usp_EjecutaTranBtmum210Ref_Swift_In";
        public const string sp_DatosCuentaT24 = "dbo.usp_ConsultarDatosCuentaT24_Swift_In_Ft";
        public const string sp_SpeiListaBancos = "usp_spei_listaBancos";
        public const string sp_ConsultaAotmationActive = "usp_ConsultarAutomationActivo_Swift";
        public const string sp_Insert_Formato_FXDLocal = "usp_TOMBIN_INSERT_FORMATO_FXD_LOCAL";
        public const string sp_ConsultarReintentosPermitidos = "dbo.usp_ConsultarReintentosPermitidos_Swift_MX";
        public const string sp_EliminarRegistroAdmonFoliosT24 = "dbo.usp_EliminarRegistroAdmonFolios_Swift";
        public const string sp_ConsultarExisteFt = "dbo.usp_ConsultarExisteFT_Swift_In";
        public const string sp_MarcaOperacionConError = "usp_swf_marca_op_error";
        public const string sp_ConsultaTablaTrabajoTomi = "dbo.usp_SwiftTrabajoTomi";
        public const string sp_ActualizarFirmaDigital = "dbo.usp_ActualizaFirmaDigital_Swift";
        public const string sp_ConsultarNombreCIF = "dbo.usp_ConsultarNombreCIFDatos_Swift";
        public const string sp_ConsultarFirmaDigital = "dbo.usp_ConsultarFirmaElectronica_Swift";
        public const string sp_InterfaceTomiSpeiDinamico = "sp_InterfaceTOMI_SPEI_";
        public const string sp_ConsultarFirmaDigitalMx = "dbo.usp_ConsultarFirmaElectronicaMx_Swift";
        public const string sp_ActualizaDatosOperacion = "dbo.ActualizaDatosOperacion_Swift_In_Ft";
        public const string sp_ConsultarFechaValor = "dbo.usp_ConsultarExisteFechaValor_Swift_In";
        public const string sp_ActualizarPlanTrabajoMaestro_CtaSinFondos = "dbo.usp_ActualizaPlanTrabajoMaestro_CtaSinFondos_SwiftFT";
        public const string sp_ActualizaAdmonFolios_CtaSinfondos = "dbo.usp_ActualizaAdmonFolios_CtaSinFondos_SwiftFT";
        public const string sp_ProcesarCuentasConErrorSwift = "dbo.usp_ActualizaPlanTrabajoMtoDet_ConError";
        public const string sp_ProcesarCuentasRegistroExitosoSwift = "dbo.usp_ActualizaTransaccionRegistroExitoso_Swift_Ft";
        public const string sp_ActualizaPlanTrabajoDetalleSwift = "dbo.usp_ActualizaPlanTrabajoDetalle_Swift_In";

    }
}

using Aplicacion.DTOs;
using Aplicacion.Modelos;
using MUFG_T24.Modelos;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Define las operaciones que participan en el flujo de procesamiento, proporcionando los contratos que deben implementar los
    /// componentes encargados de ejecutar las validaciones y transformaciones requeridas.
    /// </summary>
    public interface IOperaciones
    {
        Task<Dictionary<string, string>> parametrosSistema();
        Task<FechaSistemaDto> ObtenerFechaJuliana();
        Task<bool> EsDiaHabil();
        void EnviaAlerta(AlertaDTO alertaDto);
        Task<bool> CerrarOperacionesAnteriores(int fechaSistema);
        Task<bool> ActualizaPlanTrabajo();
        Task<List<OperacionesSwiftModelo>> Busca_Operaciones_FT();
        Task<string> ObtenerUltimoProceso_SWF(string folio, string tipoOperacion);
        Task<bool> IniciarProceso_SFW(string folio, string tipoOperacion, string aliasProceso);
        Task<int> ObtenerNumRegistrosPlantrabajoDetalle(string folio, string tipoOperacion, string aliasProceso);
        Task<int> InsertarTombinEntradasSalidas(TombinEntradasSalidasModelo objTombinEntradaSalida);
        Task<bool> ActualizaPlanTrabajoDetalle(string folio, string tipoOperacion, string aliasProceso, int opcion);
        Task<bool> TerminaProceso_SFW(string folio, string tipoOperacion, string aliasProceso);
        Task<T24MwControlModelo> ExisteRegistro(string folio_unico);
        Task<bool> Marca_Op_Terminada(string icn, string estatus, string usuarioAutorizacion, string detalleRechazo);
        Task<CIFSwiftModelo> Busca_Cif_Datos(string cif, string swift);
        Task<int> AgregaReferenciaBtmum210(string icn, string fec_valor, string cve_swift, string des_referencia, decimal num_importe);
        Task<string> Dame_Banco(string cuenta_clabe);
        Task<bool> AutomationIsActive();
        Task<SalidaFXDLocalModelo> Insert_Formato_FXD_local(FormatoFxdLocalModelo fxdLocal);
        Task<int> ActualizaDatosOperacion(string icn, int folioRegreso, string detailCharges);
        Task<T24AdmonFoliosModelo> ConsultarReintentosPermitidosSwt(string folio, string cve_mensaje);
        Task<bool> EliminarRegistroAdmonFolios(string folio, string cve_mensaje);
        Task<SwfPlanTrabajoMestroModelo> ConsultarExisteFT(string folio);
        Task<bool> Marca_Op_Con_Error(string icn, string mensaje_error);
        Task<FXDFirmaDigitalModelo> ConsultaTablaTrabajoTomi(string Tabla, string folio);
        Task<bool> ActualizarFirmaDigital(string Tabla, string folio, string firmaDigital);
        Task<CIFSwiftModelo> ConsultarNombreCIF(string identificador, int Opcion);
        Task<FirmaElectronicaDto> ConsultarFirmaElectronica(string Folio);
        Task<FirmaElectronicaDto> ConsultarFirmaElectronicaMx(string Folio);
        Task<SwfPlanTrabajoMestroModelo> ConsultarFechaValor(string folio);
        Task<bool> ActualizarSwfPlanTrabajoMaestroCuentaSinFondos(string folio, string cve_mensaje);
        Task<AdmonFoliosModelo> ConsultaAdmonFoliosCuentaSinFondos(string folio, string cve_mensaje);
        Task<bool> ActualizarAdmonFoliosCuentasSinFndos(string folio, string cve_mensaje, int opcion);
        Task<bool> ProcesarCuentasConError(string folio, string des_response);
        Task<bool> ProcesarCuentasRegistroExitoso(string folio, string cve_mensaje, string transaccionId);
        Task<string> ObtenerFechaDelSistema();
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorios
{
    /// <summary>
    /// Repositorio encargado de ejecutar consultas e interfaces SPEI utilizadas durante el procesamiento de las operaciones.
    /// </summary>
    public class ConsultasBDTomi : RepositorioMaestro, IOperaciones
    {
        private readonly IAlertas alerta;
        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="ConsultasBDTomi"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public ConsultasBDTomi(IAlertas alertaT) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            alerta = alertaT;
        }

        /// <summary>
        /// Obtiene los parametros de configuracion del sistema requeridos para la ejecucion del flujo operativo.
        /// </summary>
        /// <returns></returns>
        public async Task<Dictionary<string, string>> parametrosSistema()
        {
            try
            {
                string parametrosSistema = string.Join(",", Enum.GetNames(typeof(ParametrosSistema)).Select(p => $"'{p}'"));
                var result = await EjecutarStoredProcedureQuery<ParametrosModelo>(ProcedimientoAlmacenado.sp_Parametros_Sistema, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@strParametros";
                    param.Value = parametrosSistema;
                    cmd.Parameters.Add(param);
                });

                if (result.Data == null) return new Dictionary<string, string>();

                return result.Data.ToDictionary(r => r.Id_parametro, r => r.Valor);
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|parametrosSistema|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene la fecha de sistema configurada para la operacion desde la fuente de datos.
        /// </summary>
        public async Task<FechaSistemaDto> ObtenerFechaJuliana()
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<FechaSistemaDto>(ProcedimientoAlmacenado.sp_ObtenerFechaSistema);

                if (resultado.Data != null)
                    return resultado.Data.FirstOrDefault();

                return null;

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ObtenerFechaJuliana|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Registra una alerta utilizando la informacion proporcionada durante el procesamiento de la operacion
        /// </summary>
        /// <param name="alertaDto">Datos de la alerta</param>
        public void EnviaAlerta(AlertaDTO alertaDto)
        {
            alerta.InsertarAlerta(alertaDto);
        }

        /// <summary>
        /// Ejecuta el cierre de las operaciones pendientes de días anteriores tomando como referencia la fecha de sistema
        /// proporcionada.
        /// </summary>
        /// <param name="fechaSistema">Fecha de sistema</param>
        /// <returns></returns>
        public async Task<bool> CerrarOperacionesAnteriores(int fechaSistema)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_CerrarOperacionesAnteriores, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@FechaSistema";
                    param.Value = fechaSistema;
                    cmd.Parameters.Add(param);
                });

                if (resultado.Exito)
                {
                    Config.EscribirLog($"INFO|CerrarOperacionesAnteriores|Se cerraron {resultado.FilasAfectadas} Operaciones ");
                    return true;
                }
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|CerrarOperacionesAnteriores|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza el plan de trabajo utilizado por el proceso operativo para reflejar el avance de las actividades programadas.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> ActualizaPlanTrabajo()
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajo, cmd => { });

                if (resultado.Exito)
                {
                    Config.EscribirLog($"INFO|ActualizaPlanTrabajo|Se actualizaron {resultado.FilasAfectadas} Operaciones ");
                    return true;
                }
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ActualizaPlanTrabajo|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene las operaciones financieras de tipo FT pendientes de procesamiento de acuerdo con el tipo de operacion configurado en el sistema.
        /// </summary>
        public async Task<List<OperacionesSwiftModelo>> Busca_Operaciones_FT()
        {
            List<OperacionesSwiftModelo> operacionesSwiftIn = new List<OperacionesSwiftModelo>();
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<OperacionesSwiftModelo>(ProcedimientoAlmacenado.sp_Swift_FT_Operaciones, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@tipo_operacion";
                    param.Value = ConstantesServicios.TipoOperacion;
                    cmd.Parameters.Add(param);
                });

                if (resultado.Data != null)
                    operacionesSwiftIn = resultado.Data;

                return operacionesSwiftIn;

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|Busca_Operaciones_FT|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene el alias del ultimo proceso ejecutando para una operacion SWIFT utilizando el folio y tipo de operacion proporcionados.
        /// </summary>
        /// <param name="folio">Identificador la operacion a consultar</param>
        /// <param name="tipoOperacion">Tipo de operacion asociado</param>
        /// <returns></returns>
        public async Task<string> ObtenerUltimoProceso_SWF(string folio, string tipoOperacion)
        {
            try
            {
                string resultadoAlias = string.Empty;
                var result = await EjecutarStoredProcedureQuery<AliasProcesoModelo>(ProcedimientoAlmacenado.sp_UltimoProcesoSwift, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@TipoOperacion";
                    param.Value = tipoOperacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@PId";
                    param.Value = 0;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    resultadoAlias = result.Data.Select(r => r.Alias_proceso).FirstOrDefault();

                return resultadoAlias;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ObtenerUltimoProceso_SWF|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Inicia una etapa del flujo de procesamiento de una operacion SWIFT, registrando el alias del proceso asociado al folio y tipo de operacion
        /// </summary>
        /// <param name="folio">Identificador unico</param>
        /// <param name="tipoOperacion">Tipo de operacion en ejecucion</param>
        /// <param name="aliasProceso">Alias del proceso</param>
        public async Task<bool> IniciarProceso_SFW(string folio, string tipoOperacion, string aliasProceso)
        {
            try
            {
                await EjecutarStoredProcedureQuery<AliasProcesoModelo>(ProcedimientoAlmacenado.sp_IniciaProcesoSwift, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@TipoOperacion";
                    param.Value = tipoOperacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@AliasProceso";
                    param.Value = aliasProceso;
                    cmd.Parameters.Add(param);
                });

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|IniciarProceso_SFW|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene la cantidad de registros asociados a una etapa especifica del plan de trabajo de una operacion
        /// </summary>
        /// <param name="folio">Identificador unico</param>
        /// <param name="tipoOperacion">Tipo de operacion en ejecucion</param>
        /// <param name="aliasProceso">Alias del proceso</param>
        public async Task<int> ObtenerNumRegistrosPlantrabajoDetalle(string folio, string tipoOperacion, string aliasProceso)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<PlanTrabajoDetalleModel>(ProcedimientoAlmacenado.sp_ConsultarNumRegsPlanTrabajoDetalleES, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@TipoOperacion";
                    param.Value = tipoOperacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@AliasProceso";
                    param.Value = aliasProceso;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return result.Data.Select(a => a.Num_regs).SingleOrDefault();
                else
                    return 0;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ObtenerNumRegistrosPlantrabajoDetalle|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Registra la informacion de entradas y salidas TOMBIN asociada a una oepracion en la base de datos.
        /// </summary>
        /// <param name="objTombinEntradaSalida">Informacion de entradas y salida que sera almacenada</param>
        /// <returns></returns>
        public async Task<int> InsertarTombinEntradasSalidas(TombinEntradasSalidasModelo objTombinEntradaSalida)
        {
            try
            {
                int iSalida = -1;
                var result = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_InsertarTombinEntradas_Salidas, cmd =>
                {
                    foreach (var prop in objTombinEntradaSalida.GetType().GetProperties())
                    {
                        if (prop.Name.Equals("Salida"))
                            iSalida = cmd.Parameters.Add(basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32, 1));
                        else
                            cmd.Parameters.Add(basedatosConexion.CrearParametro($"@{prop.Name}", prop.GetValue(objTombinEntradaSalida)));


                    }
                });

                if (result.Exito)
                    return 0;
                else
                    return 1;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|InsertarTombinEntradasSalidas|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza el detalle del plan de trabajo de una operacion registrando el avance del proceso ejecutado.
        /// </summary>
        /// <param name="folio">Identificador unico</param>
        /// <param name="tipoOperacion">Tipo de operacion en ejecucion</param>
        /// <param name="aliasProceso">Alias del proceso</param>
        public async Task<bool> ActualizaPlanTrabajoDetalle(string folio, string tipoOperacion, string aliasProceso, int opcion)
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_ActualizaPlanTrabajoDetalleSwift, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@TipoOperacion";
                    param.Value = tipoOperacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@AliasProceso";
                    param.Value = aliasProceso;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Opcion";
                    param.Value = opcion;
                    cmd.Parameters.Add(param);
                });

                if (resultado.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ActualizaPlanTrabajoDetalle|Se presenta exception: {ex.ToString()}");
                return false;
            }
        }

        /// <summary>
        /// Marca como finalizado el proceso asociado a una operacion SWIFT, registrando la ultima etapa ejecutada dentro del flujo operativo.
        /// </summary>
        /// <param name="folio">Identificador unico</param>
        /// <param name="tipoOperacion">Tipo de operacion en ejecucion</param>
        /// <param name="aliasProceso">Alias del proceso</param>
        public async Task<bool> TerminaProceso_SFW(string folio, string tipoOperacion, string aliasProceso)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<AliasProcesoModelo>(ProcedimientoAlmacenado.sp_TerminaProcesoSwift, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@TipoOperacion";
                    param.Value = tipoOperacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@AliasProceso";
                    param.Value = aliasProceso;
                    cmd.Parameters.Add(param);
                });

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|TerminaProceso_SFW|Se presenta exception: {ex.ToString()}");
                return false;
            }
        }

        /// <summary>
        /// Verifica la existencia de un registro previamente almacenado utilizando el folio unico de la operacion.
        /// </summary>
        /// <param name="folio_unico">Folio identificador unico</param>
        /// <returns></returns>
        public async Task<T24MwControlModelo> ExisteRegistro(string folio_unico)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<T24MwControlModelo>(ProcedimientoAlmacenado.sp_ConsultarExisteFolioT24, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio_unico";
                    param.Value = folio_unico;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ExisteRegistro|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Registra el resultado final de la operacion dentro del flujo operativo, incluyendo estatus, usuario responsable 
        /// y detalle de rechazo cuando corresponda.
        /// </summary>
        /// <param name="icn"></param>
        /// <param name="estatus"></param>
        /// <param name="usuarioAutorizacion"></param>
        /// <param name="detalleRechazo"></param>
        /// <returns></returns>
        public async Task<bool> Marca_Op_Terminada(string icn, string estatus, string usuarioAutorizacion, string detalleRechazo)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_MarcaOpTerminada, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@icn";
                    param.Value = icn;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@tipo_operacion";
                    param.Value = ConstantesServicios.TipoOperacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@cve_estatus";
                    param.Value = estatus;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@cve_usuario";
                    param.Value = usuarioAutorizacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@des_rechazo";
                    param.Value = detalleRechazo;
                    cmd.Parameters.Add(param);
                });

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|Marca_Op_Terminada|Se presenta exception: {ex.ToString()}");
                return false;
            }
        }

        /// <summary>
        /// Obtiene la informacion CIF asociada a una institucion financiera utilizandp el numero CIF y/o SWIF proporcionados.
        /// </summary>
        public async Task<CIFSwiftModelo> Busca_Cif_Datos(string cif, string swift)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<CIFSwiftModelo>(ProcedimientoAlmacenado.sp_ConsultaCIFDatos, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@num_cif";
                    param.Value = cif;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@cve_swift";
                    param.Value = swift;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|Busca_Cif_Datos|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }


        /// <summary>
        /// Registra una referencia BTNUM210 asociada a una operacion incluyendo informacion de identificcion, referencia, SWIFT e importe.
        /// </summary> 
        public async Task<int> AgregaReferenciaBtmum210(string icn, string fec_valor, string cve_swift, string des_referencia, decimal num_importe)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_EjecutaTranBtmum210, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@icn";
                    param.Value = icn;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@fec_valor";
                    param.Value = fec_valor;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@cve_swift";
                    param.Value = cve_swift;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@des_referencia";
                    param.Value = des_referencia;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@num_importe";
                    param.Value = num_importe;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return 0;
                else
                    return -1;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|AgregaReferenciaBtmum210|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene el nombre de la institucion financiera asociada a una cuenta CLABE
        /// </summary>
        public async Task<string> Dame_Banco(string cuenta_clabe)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<SpeiListaBancosModelo>(ProcedimientoAlmacenado.sp_SpeiListaBancos);
                Dictionary<string, string> dicRegreso = new Dictionary<string, string>();

                if (result.Data != null)
                {
                    foreach (SpeiListaBancosModelo item in result.Data)
                    {
                        dicRegreso.Add(item.Ins_clave.ToString().PadLeft(5, '0'), item.Ins_nombre);
                    }

                    string inicioCuenta = cuenta_clabe.Substring(0, 3);
                    return dicRegreso.Where(a => a.Key.Substring(2, 3).Equals(inicioCuenta)).Select(a => string.Format("{0}-{1}", a.Key, a.Value)).FirstOrDefault();
                }

                return "";
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|Dame_Banco|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Determina si la automatizacion de procesos se encuentra habilitada de acuerdo con la configuracion vigente del sistema.
        /// </summary>
        public async Task<bool> AutomationIsActive()
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<SwiftAutomationControlDto>(ProcedimientoAlmacenado.sp_ConsultaAotmationActive);

                if (result.Data != null)
                {
                    int opAutomatica = result.Data.Select(a => a.Op_automatica).FirstOrDefault();

                    if (opAutomatica == 1)
                        return true;
                    else
                        return false;
                }

                return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|AutomationIsActive|Se presenta exception: {ex.ToString()}");
                return false;
            }
        }

        /// <summary>
        /// Almacena la informacion del formato FXD local en la fuenta de datos correspondiente para su utilizacion en procesos posteriores.
        /// </summary>
        public async Task<SalidaFXDLocalModelo> Insert_Formato_FXD_local(FormatoFxdLocalModelo fxdLocal)
        {
            try
            {
                SalidaFXDLocalModelo salida = new SalidaFXDLocalModelo();
                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_Insert_Formato_FXDLocal, cmd =>
                {
                    foreach (var prop in fxdLocal.GetType().GetProperties())
                    {
                        IDbDataParameter param = cmd.CreateParameter();
                        param.ParameterName = $"@{prop.Name}";
                        param.Value = prop.GetValue(fxdLocal);

                        if (prop.Name.Equals("Salida"))
                            param.Direction = ParameterDirection.Output;

                        cmd.Parameters.Add(param);
                    }
                });

                if (result.Data != null)
                {
                    salida.Salida = result.Data[0];
                    salida.Folio = result.Data[1];
                    return salida;
                }

                return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|Insert_Formato_FXD_local|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza la informacion asociada a una operacion previamente registrada, incluyendo el folio de ingreso 
        /// y los cargos detallados relacionados.
        /// </summary>
        public async Task<int> ActualizaDatosOperacion(string icn, int folioRegreso, string detailCharges)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_ActualizaDatosOperacion, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@icn";
                    param.Value = icn;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Folioregreso";
                    param.Value = folioRegreso;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@DetailCharges";
                    param.Value = detailCharges;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return 0;
                else
                    return -1;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|Insert_Formato_FXD_local|Se presenta exception: {ex.ToString()}");
                return -1;
            }
        }

        /// <summary>
        /// Obtiene la configuracion de reintentos permitidos para una operacion y tipo de mensaje determinados.
        /// </summary>
        public async Task<T24AdmonFoliosModelo> ConsultarReintentosPermitidosSwt(string folio, string cve_mensaje)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<T24AdmonFoliosModelo>(ProcedimientoAlmacenado.sp_ConsultarReintentosPermitidos, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Cve_mensaje";
                    param.Value = cve_mensaje;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null && result.Data.Count > 0)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ConsultarReintentosPermitidosSwt|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Elimina el registro de administracion de folios asociado a una operacion y tipo de mensaje.
        /// </summary>
        public async Task<bool> EliminarRegistroAdmonFolios(string folio, string cve_mensaje)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_EliminarRegistroAdmonFoliosT24, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Cve_mensaje";
                    param.Value = cve_mensaje;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|EliminarRegistroAdmonFolios|Se presenta exception: {ex.ToString()}");
                return false;
            }
        }


        /// <summary>
        /// Consulta la informacion de una transaccion previamente registrada utilizando el folio de la operacion como criterio de busqueda.
        /// </summary>
        /// <param name="folio">folio de la operacion a consultar</param>
        public async Task<SwfPlanTrabajoMestroModelo> ConsultarExisteFT(string folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_ConsultarExisteFt, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ConsultarExisteFT|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza el estado de una operacion para indicar que ocurrio un error durante su ejecion, registrando ademas el detalle correspondiente.
        /// </summary>
        public async Task<bool> Marca_Op_Con_Error(string icn, string mensaje_error)
        {
            try
            {
                if (string.IsNullOrEmpty(icn))
                    return false;

                if (string.IsNullOrEmpty(mensaje_error))
                    return false;

                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_MarcaOperacionConError, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@icn";
                    param.Value = icn;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@tipo_operacion";
                    param.Value = ConstantesServicios.TipoOperacion;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@des_error";
                    param.Value = mensaje_error;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|Marca_Op_Con_Error|Se presenta exception: {ex.ToString()}");
                return false;
            }
        }

        /// <summary>
        /// Consultar la informacion de una operacion almacenada en una tabla de trabajo
        /// </summary>
        public async Task<FXDFirmaDigitalModelo> ConsultaTablaTrabajoTomi(string Tabla, string folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<FXDFirmaDigitalModelo>(ProcedimientoAlmacenado.sp_ConsultaTablaTrabajoTomi, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Tabla";
                    param.Value = Tabla;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"INFO|ConsultaTablaTrabajoTomi|Se presenta exception: {ex.ToString()}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza la firma digital generada para una operacion.
        /// </summary>
        /// <param name="Tabla">Tabla donde se encuentra almacenada la operacion</param>
        /// <param name="folio">Folio de la operacion</param>
        /// <param name="firmaDigital">Firma digital correspondiente a la operacion</param>
        public async Task<bool> ActualizarFirmaDigital(string Tabla, string folio, string firmaDigital)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_ActualizarFirmaDigital, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Tabla";
                    param.Value = Tabla;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Firma_Digital";
                    param.Value = firmaDigital;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizarFirmaDigital|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }
        

        /// <summary>
        /// Obtiene la informacion asociada a una institucion financiera a partir de su identificador CIF.
        /// </summary>
        public async Task<CIFSwiftModelo> ConsultarNombreCIF(string identificador, int Opcion)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<CIFSwiftModelo>(ProcedimientoAlmacenado.sp_ConsultarNombreCIF, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Identificador";
                    param.Value = identificador;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@Opcion";
                    param.Value = Opcion;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConsultarNombreCIF|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Consulta la firma electronica generada durante el procesamiento de una operacion financiera.
        /// </summary>
        public async Task<FirmaElectronicaDto> ConsultarFirmaElectronica(string Folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<FirmaElectronicaDto>(ProcedimientoAlmacenado.sp_ConsultarFirmaDigital, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = Folio;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConsultarFirmaElectronica|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Consulta la firma electronica correspondiente a operaciones MX, utilizada para validar y dar seguimiento al procesamiento de la transaccion
        /// </summary>
        public async Task<FirmaElectronicaDto> ConsultarFirmaElectronicaMx(string Folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<FirmaElectronicaDto>(ProcedimientoAlmacenado.sp_ConsultarFirmaDigitalMx, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = Folio;
                    cmd.Parameters.Add(param);
                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConsultarFirmaElectronicaMx|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene la informacion de la fecha valor asociada a una operacion a partir del folio proporcionado.
        /// </summary>
        public async Task<SwfPlanTrabajoMestroModelo> ConsultarFechaValor(string folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<dynamic>(ProcedimientoAlmacenado.sp_ConsultarFechaValor, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConsultarFechaValor|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }


        /// <summary>
        /// Actualiza el plan de trabajo maestro de una operacion y el mensaje asociado.
        /// </summary>
        public async Task<bool> ActualizarSwfPlanTrabajoMaestroCuentaSinFondos(string folio, string cve_mensaje)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajoMaestro_CtaSinFondos, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@cve_mensaje";
                    param.Value = cve_mensaje;
                    cmd.Parameters.Add(param);

                });

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizarSwfPlanTrabajoMaestroCuentaSinFondos|Se presenta el siguiente error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Recupera la informacion de seguimiento y control de folios para operaciones que presentan la condicion de cuenta sin fondos.
        /// </summary>
        public async Task<AdmonFoliosModelo> ConsultaAdmonFoliosCuentaSinFondos(string folio, string cve_mensaje)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<AdmonFoliosModelo>(ProcedimientoAlmacenado.sp_ActualizarPlanTrabajoMaestro_CtaSinFondos, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@cve_mensaje";
                    param.Value = cve_mensaje;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConsultaAdmonFoliosCuentaSinFondos|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Actualiza la informacion de administracion de folios relacionada con operaciones rechazadas o pendientes por insuficiencia de fondos.
        /// </summary>
        public async Task<bool> ActualizarAdmonFoliosCuentasSinFndos(string folio, string cve_mensaje, int opcion)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ActualizaAdmonFolios_CtaSinfondos, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@cve_mensaje";
                    param.Value = cve_mensaje;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@opcion";
                    param.Value = opcion;
                    cmd.Parameters.Add(param);

                });

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ActualizarAdmonFoliosCuentasSinFndos|Se presenta el siguiente error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Registra y procesa una operacion que presento errores durante su ejecucion, almacenando el detalle de la respuesta obtenida.
        /// </summary></param>
        public async Task<bool> ProcesarCuentasConError(string folio, string des_response)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ProcesarCuentasConErrorSwift, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@des_response";
                    param.Value = des_response;
                    cmd.Parameters.Add(param);

                });

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarCuentasConError|Se presenta el siguiente error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Registra el resultado exitoso del procesamiento de cuentas, almacenando la informacion de la operacion y la transaccion asociada.
        /// </summary>
        public async Task<bool> ProcesarCuentasRegistroExitoso(string folio, string cve_mensaje, string transaccionId)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ProcesarCuentasRegistroExitosoSwift, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@cve_mensaje";
                    param.Value = cve_mensaje;
                    cmd.Parameters.Add(param);

                    param = cmd.CreateParameter();
                    param.ParameterName = "@transactionId";
                    param.Value = transaccionId;
                    cmd.Parameters.Add(param);

                });

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesarCuentasRegistroExitoso|Se presenta el siguiente error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Obtiene la fecha del sistema configurada y utilizada por los procesos operativos de la aplicacion
        /// </summary>
        public async Task<string> ObtenerFechaDelSistema()
        {
            try
            {
                var resultado = await EjecutarStoredProcedureQuery<SistemaFechaDto>(ProcedimientoAlmacenado.sp_Fecha_Sistema);
                return resultado.Data.FirstOrDefault()?.Fecha.ToString();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ObtenerFechaDelSistema|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Repositorios;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorios
{
    /// <summary>
    /// Repositorio encargado de ejecutar consultas e interfaces SPEI utilizadas durante el procesamiento de las operaciones.
    /// </summary>
    public class ConsultasBDSpei : RepositorioMaestro, IOperacionesSPEI
    {
        private readonly IAlertas alerta;

        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref="ConsultasBDSpei"/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public ConsultasBDSpei(IAlertas alertaT) : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI, null, null)
        {
            alerta = alertaT;
        }

        /// <summary>
        /// Ejecuta la interfaz SPEI para registrar procesar una operacion utilizando la informacion proporcionada.
        /// </summary>
        /// <param name="spNombre">Nombre del procedimiento almacenado</param>
        /// <param name="datos">Datos de la operacion</param>
        public async Task<SalidaSPEIModelo> EjecutarSP_InterfaceTOMI_SPEI(string spNombre, InterfaceSPEIModelo datos)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<SalidaSPEIModelo>(spNombre, cmd =>
                {
                    foreach (var prop in datos.GetType().GetProperties())
                    {
                        IDbDataParameter param = cmd.CreateParameter();
                        param.ParameterName = $"@{prop.Name}";
                        param.Value = prop.GetValue(datos);
                        cmd.Parameters.Add(param);
                    }
                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (System.Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSP_InterfaceTOMI_SPEI|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Ejecuta una interfaz SPEI eviando datos de la operacion al procedimiento configurado y devuelve el resultado obtenido.
        /// </summary>
        /// <param name="spNombre">Nombre del procedimiento almacenado</param>
        /// <param name="datos">Datos de la operacion</param>
        public async Task<SalidaSPEIModelo> EjecutarSP_InterfaceTOMI_SPEI_PACS009(string spNombre, InterfaceSPEIPacs009Modelo datos)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<SalidaSPEIModelo>(spNombre, cmd =>
                {
                    foreach (var prop in datos.GetType().GetProperties())
                    {
                        IDbDataParameter param = cmd.CreateParameter();
                        param.ParameterName = $"@{prop.Name}";
                        param.Value = prop.GetValue(datos);
                        cmd.Parameters.Add(param);
                    }
                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (System.Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSP_InterfaceTOMI_SPEI_PACS009|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorios
{
    /// <summary>
    /// Repositorio encargado de ejecutar consultas e interfaces SPEI utilizadas durante el procesamiento de las operaciones.
    /// </summary>
    public class ConsultasBDVistasT24 : RepositorioMaestro, IOperacionesT24
    {
        private readonly IAlertas alerta;
        /// <summary>
        ///  Inicializa una nueva instancia de la clase <see cref=ConsultasBDVistasT24""/>
        ///  con las dependencias necesarias para el procesamiento de la operación.
        /// </summary>
        public ConsultasBDVistasT24(IAlertas alertaT) : base(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW, BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW)
        {
            alerta = alertaT;
        }

        /// <summary>
        /// Consulta la fecha operativa vigente en T24 para su utilizacion en los procesos de negocio e integracion.
        /// </summary>
        /// <returns></returns>
        public async Task<string> ObtenerFecha_T24()
        {
            string fechaT24 = string.Empty;
            try
            {

                var result = await EjecutarStoredProcedureQuery<FechaT24>(ProcedimientoAlmacenado.sp_ObtenerFechaSistemaT24);
                fechaT24 = result.Data.Select(x => x.Julian_Date).FirstOrDefault();
                return fechaT24;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutarSP_InterfaceTOMI_SPEI|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Consulta en T24 los datos de una operacion financiera utilizando el folio SWIFT como criterio de busqueda.
        /// </summary>
        /// <param name="folio">Folio de la operacion</param>
        public async Task<T24FundsTransferModelo> ConsultarFoliSwiftT24(string folio)
        {
            try
            {

                var result = await EjecutarStoredProcedureQuery<T24FundsTransferModelo>(ProcedimientoAlmacenado.sp_ConsultarFolioSwiftT24, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Folio";
                    param.Value = folio;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ConsultarFoliSwiftT24|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene la informacion de una cuenta de cliente requeridos para el procesamiento de operaciones
        /// </summary>
        /// <param name="cuenta">Identificador de la cuenta a consultar</param>
        /// <returns></returns>
        public async Task<T24CustomerAccountModelo> Datos_Cuenta_VW(string cuenta)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<T24CustomerAccountModelo>(ProcedimientoAlmacenado.sp_DatosCuentaT24, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@Cuenta";
                    param.Value = cuenta;
                    cmd.Parameters.Add(param);

                });

                if (result.Data != null)
                    return result.Data.FirstOrDefault();
                else
                    return null;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Registra_Trx|Se presenta el siguiente error: {ex.Message}");
                throw;
            }
        }
    }
}

using Aplicacion;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Infraestructura.Repositorios;
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
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.Net;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace Swft_In_Ft
{
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private Configuracion _configuracionBase;
        private ConfiguracionModelo _configuracionModelo;

        protected override string NombreServicioLog => NombreServicio.Swf_IN_ETL.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

        public ServiceHost() { }

        protected override void Inicializacion(string[] args, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            _containerDI = new SimpleInjector.Container();
            _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
            _containerDI.Register<IAlertas>(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null));
            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();

            _configuracionBase = new Configuracion();
            _configuracionModelo = new ConfiguracionModelo();

            {
                ConfiguracionEncriptadaHelper.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Swf_IN_FT.ToString());
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

                Logging.Init(rutas, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
                Logging.Write("INFO|Inicializacion|Leyendo archivo de configuración.");
            }

            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<ConfiguracionModelo>(() => _configuracionModelo, Lifestyle.Scoped);

            //Componentes...
            _containerDI.Register(() =>
                new ConexionT24Mw(
                    BasedatosProveedor.SqlServer,
                    NombreBasedatos.TOMI,
                    BasedatosProveedor.SqlServer,
                    NombreBasedatos.T24_VW
                ),
                Lifestyle.Singleton);

            _containerDI.Register<IOperaciones, ConsultasBDTomi>();
            _containerDI.Register<IOperacionesT24, ConsultasBDVistasT24>();
            _containerDI.Register<IOperacionesSPEI, ConsultasBDSpei>();
            _containerDI.Register<OperacionServicio>();

            _containerDI.Verify();
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
                _configuracionBase = await _parametrizacion.ObtenerProcesoActualAsync(_configuracionBase, ct);

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
                    var operacionServicio = _containerDI.GetInstance<OperacionServicio>();
                    await operacionServicio.EjecutaProcedimientosAsync(ct).ConfigureAwait(false);
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

                    await _controlServicio.EsperarTareaActivaAsync(startupTask, ConstantesSistema.ParametrizacionInicial, waitCt).ConfigureAwait(false);
                    await _controlServicio.EsperarTareaActivaAsync(runningTask, ConstantesSistema.ProcesamientoActivo, waitCt).ConfigureAwait(false);

                    //Marca el servicio en uso...
                    await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio).ConfigureAwait(false);

                    //Marca el termino del servicio...
                    await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio);
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

using Aplicacion;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Infraestructura.Repositorios;
using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;
using SimpleInjector;
using System;
using System.ServiceProcess;
using System.Timers;

namespace Swft_In_Ft
{
    internal static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                NombreServicio.Swf_IN_FT.ToString(),
                () => new ServiceHost(),
                args,
                (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs));
        }
    }
}


Swft_In_Ft/App.config
﻿<?xml version="1.0" encoding="utf-8" ?>
<configuration>
    <startup> 
        <supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.8" />
    </startup>
</configuration>

Aplicacion/App.config
﻿<?xml version="1.0" encoding="utf-8" ?>
<configuration>
    <system.serviceModel>
        <bindings>
            <basicHttpBinding>
                <binding name="T24WebServicesImplPortBinding" />
            </basicHttpBinding>
        </bindings>
        <client>
            <endpoint address="http://10.97.38.163:8085/SWIFT.IN/services"
                binding="basicHttpBinding" bindingConfiguration="T24WebServicesImplPortBinding"
                contract="wsT24SwiftIn.T24WebServicesImpl" name="T24WebServicesImplPort" />
        </client>
    </system.serviceModel>
</configuration>


T24WebServicesImplService.wsdl
<?xml version="1.0" encoding="utf-8"?>
<definitions xmlns:wsam="http://www.w3.org/2007/05/addressing/metadata" xmlns:wsp="http://www.w3.org/ns/ws-policy" xmlns:wsp1_2="http://schemas.xmlsoap.org/ws/2004/09/policy" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://temenos.com/SWIFT.IN" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" name="T24WebServicesImplService" targetNamespace="http://temenos.com/SWIFT.IN" xmlns="http://schemas.xmlsoap.org/wsdl/">
  <types>
    <xsd:schema>
      <xsd:import schemaLocation="http://10.97.38.163:8085/SWIFT.IN/services?xsd=1" namespace="http://temenos.com/SWIFT.IN" />
    </xsd:schema>
    <xsd:schema>
      <xsd:import schemaLocation="http://10.97.38.163:8085/SWIFT.IN/services?xsd=2" namespace="http://jaxb.dev.java.net/array" />
    </xsd:schema>
  </types>
  <message name="SWIFTINWITHSCHEMA_Validate">
    <part name="parameters" element="tns:SWIFTINWITHSCHEMA_Validate" />
  </message>
  <message name="SWIFTINWITHSCHEMA_ValidateResponse">
    <part name="parameters" element="tns:SWIFTINWITHSCHEMA_ValidateResponse" />
  </message>
  <message name="SWIFTINWITHOUTSCHEMA_Validate">
    <part name="parameters" element="tns:SWIFTINWITHOUTSCHEMA_Validate" />
  </message>
  <message name="SWIFTINWITHOUTSCHEMA_ValidateResponse">
    <part name="parameters" element="tns:SWIFTINWITHOUTSCHEMA_ValidateResponse" />
  </message>
  <message name="SWIFTINWITHOUTSCHEMA">
    <part name="parameters" element="tns:SWIFTINWITHOUTSCHEMA" />
  </message>
  <message name="SWIFTINWITHOUTSCHEMAResponse">
    <part name="parameters" element="tns:SWIFTINWITHOUTSCHEMAResponse" />
  </message>
  <message name="SWIFTINWITHSCHEMA">
    <part name="parameters" element="tns:SWIFTINWITHSCHEMA" />
  </message>
  <message name="SWIFTINWITHSCHEMAResponse">
    <part name="parameters" element="tns:SWIFTINWITHSCHEMAResponse" />
  </message>
  <portType name="T24WebServicesImpl">
    <operation name="SWIFTINWITHSCHEMA_Validate">
      <input wsam:Action="http://temenos.com/SWIFT.IN/T24WebServicesImpl/SWIFTINWITHSCHEMA_ValidateRequest" message="tns:SWIFTINWITHSCHEMA_Validate" />
      <output wsam:Action="http://temenos.com/SWIFT.IN/T24WebServicesImpl/SWIFTINWITHSCHEMA_ValidateResponse" message="tns:SWIFTINWITHSCHEMA_ValidateResponse" />
    </operation>
    <operation name="SWIFTINWITHOUTSCHEMA_Validate">
      <input wsam:Action="http://temenos.com/SWIFT.IN/T24WebServicesImpl/SWIFTINWITHOUTSCHEMA_ValidateRequest" message="tns:SWIFTINWITHOUTSCHEMA_Validate" />
      <output wsam:Action="http://temenos.com/SWIFT.IN/T24WebServicesImpl/SWIFTINWITHOUTSCHEMA_ValidateResponse" message="tns:SWIFTINWITHOUTSCHEMA_ValidateResponse" />
    </operation>
    <operation name="SWIFTINWITHOUTSCHEMA">
      <input wsam:Action="http://temenos.com/SWIFT.IN/T24WebServicesImpl/SWIFTINWITHOUTSCHEMARequest" message="tns:SWIFTINWITHOUTSCHEMA" />
      <output wsam:Action="http://temenos.com/SWIFT.IN/T24WebServicesImpl/SWIFTINWITHOUTSCHEMAResponse" message="tns:SWIFTINWITHOUTSCHEMAResponse" />
    </operation>
    <operation name="SWIFTINWITHSCHEMA">
      <input wsam:Action="http://temenos.com/SWIFT.IN/T24WebServicesImpl/SWIFTINWITHSCHEMARequest" message="tns:SWIFTINWITHSCHEMA" />
      <output wsam:Action="http://temenos.com/SWIFT.IN/T24WebServicesImpl/SWIFTINWITHSCHEMAResponse" message="tns:SWIFTINWITHSCHEMAResponse" />
    </operation>
  </portType>
  <binding name="T24WebServicesImplPortBinding" type="tns:T24WebServicesImpl">
    <soap:binding transport="http://schemas.xmlsoap.org/soap/http" />
    <operation name="SWIFTINWITHSCHEMA_Validate">
      <soap:operation soapAction="" />
      <input>
        <soap:body use="literal" />
      </input>
      <output>
        <soap:body use="literal" />
      </output>
    </operation>
    <operation name="SWIFTINWITHOUTSCHEMA_Validate">
      <soap:operation soapAction="" />
      <input>
        <soap:body use="literal" />
      </input>
      <output>
        <soap:body use="literal" />
      </output>
    </operation>
    <operation name="SWIFTINWITHOUTSCHEMA">
      <soap:operation soapAction="" />
      <input>
        <soap:body use="literal" />
      </input>
      <output>
        <soap:body use="literal" />
      </output>
    </operation>
    <operation name="SWIFTINWITHSCHEMA">
      <soap:operation soapAction="" />
      <input>
        <soap:body use="literal" />
      </input>
      <output>
        <soap:body use="literal" />
      </output>
    </operation>
  </binding>
  <service name="T24WebServicesImplService">
    <port name="T24WebServicesImplPort" binding="tns:T24WebServicesImplPortBinding">
      <soap:address location="http://10.97.38.163:8085/SWIFT.IN/services" />
    </port>
  </service>
</definitions>


Reference.svcmap
<?xml version="1.0" encoding="utf-8"?>
<ReferenceGroup xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ID="57790818-0e0d-4f8c-a069-adeb50b8235a" xmlns="urn:schemas-microsoft-com:xml-wcfservicemap">
  <ClientOptions>
    <GenerateAsynchronousMethods>false</GenerateAsynchronousMethods>
    <GenerateTaskBasedAsynchronousMethod>true</GenerateTaskBasedAsynchronousMethod>
    <EnableDataBinding>true</EnableDataBinding>
    <ExcludedTypes />
    <ImportXmlTypes>false</ImportXmlTypes>
    <GenerateInternalTypes>false</GenerateInternalTypes>
    <GenerateMessageContracts>false</GenerateMessageContracts>
    <NamespaceMappings />
    <CollectionMappings />
    <GenerateSerializableTypes>true</GenerateSerializableTypes>
    <Serializer>Auto</Serializer>
    <UseSerializerForFaults>true</UseSerializerForFaults>
    <ReferenceAllAssemblies>true</ReferenceAllAssemblies>
    <ReferencedAssemblies />
    <ReferencedDataContractTypes />
    <ServiceContractMappings />
  </ClientOptions>
  <MetadataSources>
    <MetadataSource Address="http://10.97.38.163:8085/SWIFT.IN/services?wsdl" Protocol="http" SourceId="1" />
  </MetadataSources>
  <Metadata>
    <MetadataFile FileName="services.xsd" MetadataType="Schema" ID="4d2a6fe1-601a-4fd1-abb2-56b5f2571200" SourceId="1" SourceUrl="http://10.97.38.163:8085/SWIFT.IN/services?xsd=1" />
    <MetadataFile FileName="T24WebServicesImplService.wsdl" MetadataType="Wsdl" ID="6697c7d8-93bb-4ac0-817d-1abf619c728c" SourceId="1" SourceUrl="http://10.97.38.163:8085/SWIFT.IN/services?wsdl" />
    <MetadataFile FileName="services1.xsd" MetadataType="Schema" ID="f05f3090-51a2-45ba-893f-ed8a78892c10" SourceId="1" SourceUrl="http://10.97.38.163:8085/SWIFT.IN/services?xsd=2" />
  </Metadata>
  <Extensions>
    <ExtensionFile FileName="configuration91.svcinfo" Name="configuration91.svcinfo" />
    <ExtensionFile FileName="configuration.svcinfo" Name="configuration.svcinfo" />
  </Extensions>
</ReferenceGroup>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa una entidad de datos utilizada para el intercambio y almacenamiento de informacion
    /// dentro de la apliacion.
    /// </summary>
    public class OrdenPagoValueModelo
    {
        public string OpFechaOper { get; set; }
        public string InsClaveOrd { get; set; }
        public string InsClaveBen { get; set; }
        public string OpCveRastreo { get; set; }
        public string OpMonto { get; set; }
        public string OpCuentaOrd { get; set; }
        public string OpCuentaBen { get; set; }
        public string OpCuentaBen2 { get; set; }
        public int TpPago { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa una entidad de datos utilizada para el intercambio y almacenamiento de informacion
    /// dentro de la apliacion.
    /// </summary>
    public class TombinEntradasSalidasModelo
    {
        public string Referencia { get; set; }
        public int Num_Cliente { get; set; }
        public decimal CDL { get; set; }
        public string Ordenante { get; set; }
        public decimal Operacion { get; set; }
        public string OperacionDesc { get; set; }
        public string Tipo { get; set; }
        public string TipoDesc { get; set; }
        public decimal Cuenta { get; set; }
        public string Nombre { get; set; }
        public string CuentaMoneda { get; set; }
        public decimal Via { get; set; }
        public string ViaDesc { get; set; }
        public decimal Importe { get; set; }
        public decimal Saldo { get; set; }
        public string Comentarios { get; set; }
        public DateTime Fecha_Captura { get; set; }
        public DateTime FechaValor { get; set; }
        public string Hora { get; set; }
        public string Capturista { get; set; }
        public decimal Fad { get; set; }
        public string Referencia_Control { get; set; }
        public int Referencia_Control_Cont { get; set; }
        public int Salida { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa una entidad de datos utilizada para el intercambio y almacenamiento de informacion
    /// dentro de la apliacion.
    /// </summary>
    public class FormatoFxdLocalModelo
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_TIPO { get; set; }
        public int? NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime NF_FECHA_CAPTURA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string NF_BANCO_BENEF {  get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public decimal NF_MONTO_ENVIAR { get; set; }
        public string ID_PLANTILLA { get; set; }
        public string ID_FORMATO_PLANTILLA { get; set; }
        public int Salida { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa una entidad de datos utilizada para el intercambio y almacenamiento de informacion
    /// dentro de la apliacion.
    /// </summary>
    public class InterfaceSPEIModelo
    {
        public long ins_clave { get; set; }
        public decimal op_monto { get; set; }
        public string op_cve_rastreo { get; set; }
        public string op_nom_ord {  get; set; }
        public string op_cuenta_ord { get; set; }
        public string op_nom_ben {  get; set; }
        public int tc_clave_ben { get; set; }
        public string op_cuenta_ben { get; set; }
        public string op_rfc_curp_ben { get; set; }
        public string op_concepto_pago { get; set; }
        public int op_ref_numerica { get; set; }
        public int usu_clave_int { get; set; }
        public string op_firma_dig { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa una entidad de datos utilizada para el intercambio y almacenamiento de informacion
    /// dentro de la apliacion.
    /// </summary>
    public class InterfaceSPEIPacs009Modelo
    {
        public long ins_clave { get; set; }
        public decimal op_monto { get; set; }
        public string op_cve_rastreo { get; set; }
        public string op_nom_ord { get; set; }
        public string op_cuenta_ord { get; set; }
        public string op_nom_ben { get; set; }
        public int tc_clave_ben { get; set; }
        public string op_cuenta_ben { get; set; }
        public string op_rfc_curp_ben { get; set; }
        public string op_concepto_pago { get; set; }
        public int op_ref_numerica { get; set; }
        public int usu_clave_int { get; set; }
        public string op_firma_dig { get; set; }
        public string op_nom_ben2 { get; internal set; }
        public int tc_clave_ben2 { get; internal set; }
        public string op_cuenta_ben2 { get; internal set; }
        public string op_rfc_curp_ben2 { get; internal set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa una entidad de datos utilizada para el intercambio y almacenamiento de informacion
    /// dentro de la apliacion.
    /// </summary>
    public class SwfPlanTrabajoMestroModelo
    {
        public string T24_Ref { get; set; }
        public decimal fec_valor { get; set; }
    }
}

using Aplicacion.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa el resultado de una operacion incluyendo la informacion sobre su exito 
    ///  o fracaso, el dato obtenido y los detalles de error cuando corresponda.
    /// </summary>
    /// <typeparam name="T">Tipo de dato obtenido en la respuesta</typeparam>
    public class Result<T>
    {
        public bool IsSuccess { get; set; }
        public string Reason { get; set; }
        public ErrorCodeEnum ErrorCode { get; set; }
        public T Data { get; set; }


        /// <summary>
        /// Crea una respuesta exitosa con los datos proporcionados.
        /// </summary>
        /// <param name="data">Información resultante de la operación</param>
        /// <returns></returns>
        public static Result<T> Success(T data)
        {
            return new Result<T>
            {
                IsSuccess = true,
                Data = data
            };
        }

        /// <summary>
        /// Crea una respuesta fallida, con el mensaje de error especificado.
        /// </summary>
        /// <param name="message">Descripcion de la causa del error</param>
        /// <returns></returns>
        public static Result<T> Failed(string message)
        {
            return new Result<T>
            {
                IsSuccess = false,
                Reason = message,
                ErrorCode = ErrorCodeEnum.NoCode
            };
        }

        /// <summary>
        /// Crea una respuesta fallida con un mensaje y código de error específico.
        /// </summary>
        /// <param name="message">Descripción del error</param>
        /// <param name="errorCode">Código de error asociado a la operación</param>
        /// <returns></returns>
        public static Result<T> Failed(string message, ErrorCodeEnum errorCode)
        {
            return new Result<T>
            {
                IsSuccess = false,
                Reason = message,
                ErrorCode = errorCode
            };
        }
    }
}
