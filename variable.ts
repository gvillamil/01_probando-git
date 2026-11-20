using Aplicacion.DTOs.ConfiguracionServicio;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using static Aplicacion.Enums.Enum;

namespace Aplicacion
{
    /// <summary>
    /// Clase principal homologa a "Definiciones" en LEGACY
    /// </summary>
    public class OperacionServicio
    {
        private readonly ConfiguracionModelo _Configuracionmodelo;
        private readonly IControlServicio _controlServicio;
        private readonly IAlertas _alertas;
        private readonly IParametrizacion _parametrizacion;
        private readonly IGeneralDao _generalDao;
        private readonly Aplicacion.Servicios.Operaciones.FXD _FXD;
        private readonly Aplicacion.Servicios.Operaciones.FXI _FXI;
        private readonly Aplicacion.Servicios.Operaciones.FXT _FXT;
        private readonly Aplicacion.Servicios.Operaciones.FXD_Local _FXD_Local;
        private readonly Aplicacion.Servicios.Operaciones.FXILocal _FXI_Local;
        private readonly Aplicacion.Servicios.Operaciones.FXTLocal _FXT_Local;
        private readonly Aplicacion.Servicios.Operaciones.MFX _MFX;
        private readonly Aplicacion.Servicios.Operaciones.SAT _SAT;
        private readonly Aplicacion.Servicios.Operaciones.CNT _CNT;
        private readonly Aplicacion.Servicios.Operaciones.ENTSPEI _ENT_SPEI;
        private readonly Aplicacion.Servicios.Operaciones.ENT_COAS _ENT_COAS;
        private readonly Aplicacion.Servicios.Operaciones.ENTSPID _ENT_SPID;
        private readonly Aplicacion.Servicios.Operaciones.SPIDLocal _SPID_Local;
        private readonly Aplicacion.Servicios.Operaciones.SPID _SPID;
        private readonly Aplicacion.Servicios.Operaciones.NOMFXD _NOM_FXD;
        private readonly Aplicacion.Servicios.Operaciones.PaP_FXD _PaP_FXD;
        private readonly Aplicacion.Servicios.Operaciones.PaPFXI _PaP_FXI;
        private readonly Aplicacion.Servicios.Operaciones.PaP_FXT _PaP_FXT;
        private readonly Aplicacion.Servicios.Operaciones.PaP_SPID _PaP_SPID;
        private readonly Aplicacion.Servicios.Operaciones.PaP_TEF _PaP_TEF;
        private readonly Aplicacion.Servicios.Operaciones.TEF _TEF;
        private readonly Aplicacion.Servicios.Operaciones.TEFLocal _TEF_Local;
        private readonly Aplicacion.Servicios.Operaciones.ENT_TEF _ENT_TEF;
        private readonly Aplicacion.Servicios.Operaciones.TMD _TMD;
        private readonly Aplicacion.Servicios.Operaciones.FXDGPH _FXD_GPH;
        private readonly Aplicacion.Servicios.Operaciones.FXTGPH _FXT_GPH;
        private readonly Aplicacion.Servicios.Operaciones.FXIGPH _FXI_GPH;
        private readonly Aplicacion.Servicios.Operaciones.SPIDGPH _SPID_GPH;
        private readonly Aplicacion.Servicios.Operaciones.TEFGPH _TEF_GPH;
        private readonly Aplicacion.Servicios.Operaciones.FXDH2H _FXD_H2H;
        private readonly Aplicacion.Servicios.Operaciones.FXI_H2H _FXI_H2H;
        private readonly Aplicacion.Servicios.Operaciones.FXTH2H _FXT_H2H;
        private readonly Aplicacion.Servicios.Operaciones.SPID_H2H _SPID_H2H;
        private readonly Aplicacion.Servicios.Operaciones.TEF_H2H _TEF_H2H;
        private readonly Aplicacion.Servicios.Operaciones.SER _SER;
        private readonly Aplicacion.Servicios.Operaciones.TAX _TAX;
        private readonly Aplicacion.Servicios.Operaciones.MercadoDinero _MercadoDinero;
/// <summary>
/// Inicializa una nueva instancia de la clase OperacionServicio.
/// </summary>


        public OperacionServicio(
            ConfiguracionModelo configuracionmodelo,
            IControlServicio controlServicio,
            IAlertas alertas,
            MUFG_T24.Interfaces_utilidades.IParametrizacion parametrizacion,
            IGeneralDao generalDao,
            Aplicacion.Servicios.Operaciones.FXD fxd,
            Aplicacion.Servicios.Operaciones.FXI fxi,
            Aplicacion.Servicios.Operaciones.FXT fxt,
            Aplicacion.Servicios.Operaciones.FXD_Local fxdLocal,
            Aplicacion.Servicios.Operaciones.FXILocal fxiLocal,
            Aplicacion.Servicios.Operaciones.FXTLocal fxtLocal,
            Aplicacion.Servicios.Operaciones.MFX mfx,
            Aplicacion.Servicios.Operaciones.SAT sat,
            Aplicacion.Servicios.Operaciones.CNT cnt,
            Aplicacion.Servicios.Operaciones.ENTSPEI entSpei,
            Aplicacion.Servicios.Operaciones.ENT_COAS entCoas,
            Aplicacion.Servicios.Operaciones.ENTSPID entSpid,
            Aplicacion.Servicios.Operaciones.SPID spid,
            Aplicacion.Servicios.Operaciones.SPIDLocal spidLocal,
            Aplicacion.Servicios.Operaciones.NOMFXD nomFxd,
            Aplicacion.Servicios.Operaciones.PaP_FXD papFxd,
            Aplicacion.Servicios.Operaciones.PaPFXI papFxi,
            Aplicacion.Servicios.Operaciones.PaP_FXT papFxt,
            Aplicacion.Servicios.Operaciones.PaP_SPID papSpid,
            Aplicacion.Servicios.Operaciones.PaP_TEF papTef,
            Aplicacion.Servicios.Operaciones.TEF tef,
            Aplicacion.Servicios.Operaciones.TEFLocal tefLocal,
            Aplicacion.Servicios.Operaciones.ENT_TEF entTef,
            Aplicacion.Servicios.Operaciones.TMD tmd,
            Aplicacion.Servicios.Operaciones.FXDGPH fxdGph,
            Aplicacion.Servicios.Operaciones.FXTGPH fxtGph,
            Aplicacion.Servicios.Operaciones.FXIGPH fxiGph,
            Aplicacion.Servicios.Operaciones.SPIDGPH spidGph,
            Aplicacion.Servicios.Operaciones.TEFGPH tefGph,
            Aplicacion.Servicios.Operaciones.FXDH2H fxdH2h,
            Aplicacion.Servicios.Operaciones.FXI_H2H fxiH2h,
            Aplicacion.Servicios.Operaciones.FXTH2H fxtH2h,
            Aplicacion.Servicios.Operaciones.SPID_H2H spidH2h,
            Aplicacion.Servicios.Operaciones.TEF_H2H tefH2h,
            Aplicacion.Servicios.Operaciones.SER ser,
            Aplicacion.Servicios.Operaciones.TAX tax,
            Aplicacion.Servicios.Operaciones.MercadoDinero mercadoDinero
        )
        {
            _Configuracionmodelo = configuracionmodelo;
            _controlServicio = controlServicio;
            _alertas = alertas;
            _parametrizacion = parametrizacion;
            _generalDao = generalDao;
            _FXD = fxd;
            _FXI = fxi;
            _FXT = fxt;
            _FXD_Local = fxdLocal;
            _FXI_Local = fxiLocal;
            _FXT_Local = fxtLocal;
            _MFX = mfx;
            _SAT = sat;
            _CNT = cnt;
            _ENT_SPEI = entSpei;
            _ENT_COAS = entCoas;
            _ENT_SPID = entSpid;
            _SPID = spid;
            _SPID_Local = spidLocal;
            _NOM_FXD = nomFxd;
            _PaP_FXD = papFxd;
            _PaP_FXI = papFxi;
            _PaP_FXT = papFxt;
            _PaP_SPID = papSpid;
            _PaP_TEF = papTef;
            _TEF = tef;
            _TEF_Local = tefLocal;
            _ENT_TEF = entTef;
            _TMD = tmd;
            _FXD_GPH = fxdGph;
            _FXT_GPH = fxtGph;
            _FXI_GPH = fxiGph;
            _SPID_GPH = spidGph;
            _TEF_GPH = tefGph;
            _FXD_H2H = fxdH2h;
            _FXI_H2H = fxiH2h;
            _FXT_H2H = fxtH2h;
            _SPID_H2H = spidH2h;
            _TEF_H2H = tefH2h;
            _SER = ser;
            _TAX = tax;
            _MercadoDinero = mercadoDinero;
        }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>


        public async Task<MUFG_T24.Modelos.RespuestaResumenDto> EjecutaProcedimientosAsync(string tipoOperacion, string[] folios, CancellationToken ct = default(CancellationToken))
        {
            ct.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(tipoOperacion))
                throw new ArgumentException("El parámetro tipoOperacion es obligatorio.", nameof(tipoOperacion));

            if (folios == null || folios.Length == 0)
                throw new ArgumentException("El parámetro folios es obligatorio y debe contener al menos un elemento.", nameof(folios));

            if (!_Configuracionmodelo.Pid.HasValue)
                throw new InvalidOperationException("No se obtuvo el PID del proceso actual.");

            int pidProceso = _Configuracionmodelo.Pid.Value;

            MUFG_T24.Modelos.RespuestaResumenDto resumen = new MUFG_T24.Modelos.RespuestaResumenDto(folios.Length);
            try 
            {
                using (Logging.BeginWorkerScope())
                using (Logging.BeginOperacionScope(tipoOperacion, null))
                {
                //Marca la ejecución del servicio...
                await _controlServicio.ActualizaEJecucion(_Configuracionmodelo.StrNombreServicio);

                //Obtiene los parámetros del sistema...
                _Configuracionmodelo.BoEsDiaHabil = await _controlServicio.EsDiaHabil();
                _Configuracionmodelo.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync($"'{ParametrosSistema.HORA_LECTURA_INICIO}','{ParametrosSistema.HORA_LECTURA_FIN}','{ParametrosSistema.WST24_MW_COMPANY}','{ParametrosSistema.WST24_MW_USERNAME_REG}','{ParametrosSistema.WST24_MW_COLUMN}','{ParametrosSistema.WST24_MW_OPERATION}','{ParametrosSistema.WST24_MW_ENDPOINT}','{ParametrosSistema.IP_SERVIDOR_VALIDACION}','{ParametrosSistema.PUERTO_SERVIDOR}','{ParametrosSistema.SAT_VALIDADOR_RUTA_LINEA}','{ParametrosSistema.SAT_IGNORA_VALIDADOR}','{ParametrosSistema.REQUIRED_TOWN_NAME_FOR_PAP_FXI}','{ParametrosSistema.DIGSIG_CERT_NUMSER}'", $"'{ParametrosSistema.WST24_MW_PASSWORD_REG}','{ParametrosSistema.DIGSIG_CERT_PSW}','{ParametrosSistema.DIGSIG_CERT_NUMSER_KAR}'");

                    _Configuracionmodelo.StrFechaSistema = await _parametrizacion.ObtenerFechaSistemaValidadaAsync(ct).ConfigureAwait(false);

                    //Validación explícita posterior para revisión de contrato...
                    _parametrizacion.ValidarFechaSistema(_Configuracionmodelo.StrFechaSistema);

                    //Carga la configuración de los parámetros para el inicio de operaciones de la base de datos.
                    foreach (var paramSistema in _Configuracionmodelo.ParametrosSistema)
                {
                    string claveParametro = (paramSistema.Key ?? string.Empty).Trim();
                    string valorParametro = (paramSistema.Value ?? string.Empty).Trim();

                    if (!System.Enum.TryParse(claveParametro, true, out ParametrosSistema parametro)) continue; // Cambia el tipo string a un enum (ignore case)
                    switch (parametro)
                    {
                        case ParametrosSistema.HORA_LECTURA_INICIO:
                            _Configuracionmodelo.StrHoraLecturaInicio = TimeSpan.Parse(paramSistema.Value);
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro HORA_LECTURA_INICIO cargado.");
                            break;

                        case ParametrosSistema.HORA_LECTURA_FIN:
                            _Configuracionmodelo.StrHoraLecturaFin = TimeSpan.Parse(paramSistema.Value);
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro HORA_LECTURA_FIN cargado.");
                            break;
                        case ParametrosSistema.WST24_MW_COMPANY:
                            _Configuracionmodelo.StrWSCompany = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Codigo de compania T24 (WST24_MW_COMPANY) cargado.");
                            break;
                        case ParametrosSistema.WST24_MW_USERNAME_REG:
                            _Configuracionmodelo.StrWSRegisterUserName = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro WST24_MW_USERNAME_REG cargado.");
                            break;
                        case ParametrosSistema.WST24_MW_PASSWORD_REG:
                            _Configuracionmodelo.StrWSRegisterPassword = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro WST24_MW_PASSWORD_REG cargado.");
                            break;
                        case ParametrosSistema.WST24_MW_COLUMN:
                            _Configuracionmodelo.StrWSColumnName = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro WST24_MW_COLUMN cargado.");
                            break;
                        case ParametrosSistema.WST24_MW_OPERATION:
                            _Configuracionmodelo.StrWSOperationID = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro WST24_MW_OPERATION cargado.");
                            break;
                        case ParametrosSistema.IP_SERVIDOR_VALIDACION:
                            _Configuracionmodelo.StrValidadorSAT_Servidor = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro IP_SERVIDOR_VALIDACION cargado.");
                            break;
                        case ParametrosSistema.PUERTO_SERVIDOR:
                            _Configuracionmodelo.StrValidadorSAT_Puerto = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro PUERTO_SERVIDOR cargado.");
                            break;
                        case ParametrosSistema.SAT_VALIDADOR_RUTA_LINEA:
                            _Configuracionmodelo.StrValidadorSAT_Ruta = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro SAT_VALIDADOR_RUTA_LINEA cargado.");
                            break;
                        case ParametrosSistema.SAT_IGNORA_VALIDADOR:
                            _Configuracionmodelo.StrValidadorSAT_IgnoraValidador = paramSistema.Value;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro SAT_IGNORA_VALIDADOR cargado.");
                            break;
                        case ParametrosSistema.DIGSIG_CERT_NUMSER:
                            _Configuracionmodelo.StrValidadorSAT_CertPathName = valorParametro;
                            _Configuracionmodelo.RutaPFX = valorParametro;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro DIGSIG_CERT_NUMSER cargado.");
                            break;
                        case ParametrosSistema.DIGSIG_CERT_PSW:
                            _Configuracionmodelo.StrValidadorSAT_CertPsw = valorParametro;
                            _Configuracionmodelo.SerialPassword = valorParametro;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro DIGSIG_CERT_PSW cargado.");
                            break;
                        case ParametrosSistema.DIGSIG_CERT_NUMSER_KAR:
                            _Configuracionmodelo.StrValidadorSAT_NoSerieCertKarPay = valorParametro;
                            _Configuracionmodelo.SerialKarpay = valorParametro;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro DIGSIG_CERT_NUMSER_KAR cargado.");
                            break;
                        case ParametrosSistema.REQUIRED_TOWN_NAME_FOR_PAP_FXI:
                            _Configuracionmodelo.BoRequiredTownNameForPaP = (paramSistema.Value == "1") ? true : false;
                            Config.EscribirLog("INFO|obtenerParametrosSistema|Parametro REQUIRED_TOWN_NAME_FOR_PAP_FXI cargado.");
                            break;

                    }
                }

                if (string.IsNullOrWhiteSpace(_Configuracionmodelo.RutaPFX))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("No se encontró valor para DIGSIG_CERT_NUMSER (RutaPFX). Verifica parámetro de sistema y que apunte al archivo .p12/.pfx.");

                List<OperacionDTO> operaciones = new List<OperacionDTO>();
                const int umbralLoteMasivo = 100;
                bool esLoteMasivo = folios.Length >= umbralLoteMasivo;

                foreach (string folio in folios)
                {
                    OperacionDTO _oper = new OperacionDTO()
                    {
                        StrFolio = folio,
                        StrTipoOperacion = tipoOperacion
                    };

                    if (!esLoteMasivo)
                    {
                        Config.EscribirLog($"INFO|Lista Operaciones|Folio: {_oper.StrFolio}|TipoOperacion: {_oper.StrTipoOperacion}");
                    }

                    operaciones.Add(_oper);
                }

                if (esLoteMasivo)
                {
                    Config.EscribirLog($"INFO|Lista Operaciones|Lote masivo detectado. TipoOperacion: {tipoOperacion}|TotalFolios: {folios.Length}|Se omiten trazas informativas por folio.");
                }

                int iPDI = Process.GetCurrentProcess().Id;
                Config.EscribirLog($"INFO|Obtención de Operaciones|iPDI: {iPDI}");
                string strProcesoNombre = Process.GetCurrentProcess().ProcessName;
                Config.EscribirLog($"INFO|Obtención de Operaciones|ProcessName: {strProcesoNombre}");
                Config.EscribirLog($"INFO|Obtención de Operaciones|Nombre Servicio: {_Configuracionmodelo.NombreServicio}");



                MUFG_T24.Modelos.AlertaDTO alertaSistema = new MUFG_T24.Modelos.AlertaDTO()
                {
                    TipoOPeracion = null,
                    Folio = null,
                    Servicio = _Configuracionmodelo.NombreServicio,
                    ClaveAviso = "CRITICO",
                    Descipcion = "Error en ejecuta_procedimientos",
                    PId = iPDI,
                    NombreProceso = strProcesoNombre,
                    Exception = new Exception()
                };

                Config.EscribirLog($"INFO|Obtención de Operaciones|Entra al Lopp para Tipo de Operaciones.");
                    foreach (OperacionDTO operacion in operaciones)
                    {
                        using (Logging.BeginOperacionScope(operacion.StrTipoOperacion, operacion.StrFolio))
                        {
                            try
                            {
                                ct.ThrowIfCancellationRequested();
                                resumen.Procesadas++;
                                operacion.PId = iPDI;
                                operacion.StrServicio = _Configuracionmodelo.NombreServicio;

                                bool boResultado = false;

                                switch (operacion.StrTipoOperacion)
                                {
                                    case TipoOperacion.FXD:
                                        boResultado = await _FXD.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXI:
                                        boResultado = await _FXI.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXT:
                                        boResultado = await _FXT.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXD_Local:
                                        boResultado = await _FXD_Local.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXI_Local:
                                        boResultado = await _FXI_Local.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXT_Local:
                                        boResultado = await _FXT_Local.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.MFX:
                                        boResultado = await _MFX.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.SAT:
                                        boResultado = await _SAT.ProcesOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.CNT:
                                        boResultado = await _CNT.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.ENT_SPEI:
                                        boResultado = await _ENT_SPEI.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.ENT_COAS:
                                        boResultado = await _ENT_COAS.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.ENT_SPID:
                                        boResultado = await _ENT_SPID.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.SPID_Local:
                                        boResultado = await _SPID_Local.ProcesOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.NOM_FXD:
                                        boResultado = await _NOM_FXD.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.PaP_FXD:
                                        boResultado = await _PaP_FXD.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.PaP_FXI:
                                        boResultado = await _PaP_FXI.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.PaP_FXT:
                                        boResultado = await _PaP_FXT.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.PaP_SPID:
                                        boResultado = await _PaP_SPID.ProcesaOperacion(operacion.StrFolio, ct);
                                        break;
                                    case TipoOperacion.PaP_TEF:
                                        boResultado = await _PaP_TEF.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.TEF:
                                        boResultado = await _TEF.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.TEF_Local:
                                        boResultado = await _TEF_Local.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.ENT_TEF:
                                        boResultado = await _ENT_TEF.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXD_GPH:
                                        boResultado = await _FXD_GPH.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXI_GPH:
                                        boResultado = await _FXI_GPH.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXT_GPH:
                                        boResultado = await _FXT_GPH.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.SPID_GPH:
                                        boResultado = await _SPID_GPH.ProcesOperacion(operacion.StrFolio, ct);
                                        break;
                                    case TipoOperacion.TEF_GPH:
                                        boResultado = await _TEF_GPH.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXD_H2H:
                                        boResultado = await _FXD_H2H.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXI_H2H:
                                        boResultado = await _FXI_H2H.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.FXT_H2H:
                                        boResultado = await _FXT_H2H.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.SPID_H2H:
                                        boResultado = await _SPID_H2H.procesaOperacion(operacion.StrFolio, ct);
                                        break;
                                    case TipoOperacion.TEF_H2H:
                                        boResultado = await _TEF_H2H.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.SPID:
                                        boResultado = await _SPID.ProcesaOperacion(operacion.StrFolio, ct);
                                        break;
                                    case TipoOperacion.SER:
                                        boResultado = await _SER.procesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.TAX:
                                        boResultado = await _TAX.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case TipoOperacion.TMD:
                                        boResultado = await _TMD.ProcesaOperacion(operacion.StrFolio);
                                        break;
                                    case "MD_UNKNOW":
                                    case "MD_T24":
                                    case "MD_SPEI_E":
                                    case "MD_SPEI_S":
                                    case "MD_SPID_E":
                                    case "MD_SPID_S":
                                    case "MD_MT103_S":
                                    case "MD_MT202_S":
                                    case "MD_MT210_S":
                                    case "MD_MT300_S":
                                        boResultado = await _MercadoDinero.ProcesaOperacion(operacion.StrTipoOperacion, operacion.StrFolio);
                                        break;
                                    default:
                                        throw new Exception($"Tipo de Operacion desconocida:{operacion.StrTipoOperacion}");

                                }
                                if (boResultado)
                                {
                                    resumen.Ok++;
                                    if (!esLoteMasivo)
                                    {
                                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Se termina procesamiento de folio:{operacion.StrFolio}");
                                    }
                                }
                                else
                                {
                                    resumen.Error++;
                                    Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Error durante procesamiento de folio:{operacion.StrFolio}");
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                throw;
                            }
                            catch (Exception ex)
                            {
                                await _generalDao.ActualizarFallo(operacion.StrFolio, operacion.StrTipoOperacion, _Configuracionmodelo.StrNombreServicio, _Configuracionmodelo.AliasProceso, ex.Message);
                                Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Se presenta el siguiente error {ex.Message} Detalle: \n {ex.StackTrace}");
                                await _controlServicio.ActualizaErrorProceso(new ProcesoDTO()
                                {
                                    Folio = operacion.StrFolio.ToString(),
                                    TipoOperacion = operacion.StrTipoOperacion,
                                    Servicio = _Configuracionmodelo.NombreServicio,
                                    NombreProceso = _Configuracionmodelo.NombreProceso,
                                    AliasProceso = _Configuracionmodelo.AliasProceso,
                                    Excepcion = ex,
                                    PId = pidProceso
                                });
                                await _alertas.InsertarAlerta(new AlertaDTO()
                                {
                                    Folio = null,
                                    TipoOPeracion = null,
                                    Servicio = _Configuracionmodelo.StrNombreServicio,
                                    ClaveAviso = "CRITICO",
                                    Descipcion = null,
                                    PId = pidProceso,
                                    NombreProceso = _Configuracionmodelo.NombreProceso,
                                    Exception = ex
                                });
                            }
                        }
                    }

                return resumen;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch(Exception ext)
            {
                const string claveFallaGlobal = "FALLA_GLOBAL";
                string mensajeFallaGlobal = $"{claveFallaGlobal}|Servicio:{_Configuracionmodelo.StrNombreServicio}|TipoOperacion:{tipoOperacion}|Pid:{pidProceso}|Error:{ext.Message}";

                resumen.FallaCritica = true;
                resumen.MensajeFallaCritica = mensajeFallaGlobal;
                if (resumen.Error == 0)
                    resumen.Error = 1;

                Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|{mensajeFallaGlobal}|Detalle:{ext}");

                if (_alertas != null)
                {
                    try
                    {
                        await _alertas.InsertarAlerta(new AlertaDTO()
                        {
                            Folio = null,
                            TipoOPeracion = tipoOperacion,
                            Servicio = _Configuracionmodelo.StrNombreServicio,
                            ClaveAviso = "CRITICO",
                            Descipcion = mensajeFallaGlobal,
                            PId = pidProceso,
                            NombreProceso = _Configuracionmodelo.NombreProceso,
                            Exception = ext
                        });
                    }
                    catch (Exception exAlerta)
                    {
                        Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|No fue posible registrar la alerta de falla global. Detalle: {exAlerta}");
                    }
                }
            }
            finally
            {
                await _controlServicio.ActualizaTerminacion(_Configuracionmodelo.StrNombreServicio);
            }
            return resumen;
        }
      
    }
}


using MUFG_T24.Configuracion_utilidades;
using System;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa Configuracion Modelo.
    /// </summary>
    public class ConfiguracionModelo : Configuracion
    {

        /// <summary>
        /// Propiedades generales del servicio.
        /// </summary>    
        public string StrDespachador_Puerto { get; set; } = null;
        public int StrPID { get; set; } = 0;
        public string StrNombreServicio { get; set; } = null;
        public string StrDisplayName { get; set; } = null;
        public string StrFechaSistema { get; set; } = null;
        public TimeSpan? StrFechaSistemaJuliana { get; set; } = null;
        public TimeSpan? StrHoraCierreSAT { get; set; } = null;
        public TimeSpan? StrHoraLecturaInicio { get; set; } = null;
        public TimeSpan? StrHoraLecturaFin { get; set; } = null;
        /// <summary>
        /// Datos para el uso del WEB SERVICE
        /// </summary>
        public TimeSpan? StrFechaSistema_T24_Juliana { get; set; } = null;
        public string StrWSCompany { get; set; } = null;
        public string StrWSRegisterUserName { get; set; } = null;
        public string StrWSRegisterPassword { get; set; } = null;
        public string StrWSColumnName { get; set; } = null;
        public string StrWSOperationID { get; set; } = null;
        /// <summary>
        /// Datos Servidor SAT
        /// </summary>
        public string StrValidadorSAT_Servidor { get; set; } = null;
        public string StrValidadorSAT_Puerto { get; set; } = null;
        public string StrValidadorSAT_Ruta { get; set; } = null;
        public string StrValidadorSAT_IgnoraValidador { get; set; } = null;
        public string StrValidadorSAT_CertPathName { get; set; } = null;
        public string StrValidadorSAT_CertPsw { get; set; } = null;
        public string StrValidadorSAT_NoSerieCertKarPay { get; set; } = null;
        public bool? BoRequiredTownNameForPaP { get; set; } = false;


        public bool BHorarioLectura { get; set; } = false;
        public bool? BTimerEnabled { get; set; } = null;
        public double? DTimerInterval { get; set; } = null;
        public bool? BoEsDiaHabil { get; set; } = false;
        public bool? BenHorario { get; set; } = false;
        public object StrRutaArchivoConfiguracion { get; set; }


        public string RutaPFX { get; set; } = null;
        public string SerialKarpay { get; set; } = null;
        public string SerialPassword { get; set; } = null;
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public new ConfiguracionModelo Clone()
        {
            /// <summary>
            /// Ejecuta la operaciÃ³n return.
            /// </summary>
            return (ConfiguracionModelo)MemberwiseClone();
        }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Enums
{
    /// <summary>
    /// Representa Enum.
    /// </summary>
    public static class Enum
    {
        /// <summary>
        /// Define los valores de Parametros Sistema.
        /// </summary>
        public enum ParametrosSistema
        {
            IP_SERVIDOR_VALIDACION,
            PUERTO_SERVIDOR,
            HORA_LECTURA_INICIO,
            HORA_LECTURA_FIN,
            HORA_SAT_FIN,
            SAT_VALIDADOR_RUTA_LINEA,
            SAT_IGNORA_VALIDADOR,
            DIGSIG_CERT_NUMSER,
            DIGSIG_CERT_NUMSER_KAR,
            WST24_MW_COMPANY,
            DIGSIG_CERT_PSW,
            WST24_MW_USERNAME_REG,
            REQUIRED_TOWN_NAME_FOR_PAP_FXI,
            WST24_MW_COLUMN,
            WST24_MW_PASSWORD_REG,
            WST24_MW_OPERATION,
            WST24_MW_ENDPOINT
        }
/// <summary>
/// Define los valores de Alias Proceso.
/// </summary>

        public enum AliasProceso
        {
            DATOS_GRAL,
            CMS_A_TOMI,
            TOMI_A_CMS,
            CAMBIA_ESTATUS,
            CONCILIAR,
            TESORERIA_AUTORIZACION,
            ESPERANDO_CONFIRMACION,
            USER_ESTATUS,
            VALIDAR_FLUJO,
            SWF_OUT,
            COAS_A_TOMI,
            SPID_A_TOMI,
            CAMBIO_ESTATUS
        }
/// <summary>
/// Define los valores de Byte Order.
/// </summary>

        public enum ByteOrder
        {
            BIG_ENDIAN,
            LITTLE_ENDIAN
        }
    }
/// <summary>
/// Representa Procedimiento Almacenado.
/// </summary>

    public static class ProcedimientoAlmacenado
    {

        #region Configuracion Servicio
        /// <summary>
        /// Procedimientos almacenados de configuracion para clase principal y metodo EjecutaProcedimientos
        /// </summary>
        public static string SP_FECHA_SISTEMA = "dbo.usp_ObtenerFechaSistema";
        public const string SP_PARAMETROS_SISTEMA = "dbo.usp_Comun_ObtenerParametrosSistema";
        public static string SP_ES_DIA_HABIL = "SP_ES_DIA_HABIL";

        #endregion

        #region SER
        public static string SP_GET_FORMATO_SER = "SP_GET_FORMATO_SER";
        public static string USP_TOMBIN_INSERT_FORMATO_SER_PAY = "usp_TOMBIN_INSERT_FORMATO_SER_PAY";
        public static string SP_GET_TOMBIN_NF_PAY_SER = "SP_GET_TOMBIN_NF_PAY_SER";
        public static string SP_GET_TOMBIN_NF_PAY_ES_SER = "SP_GET_TOMBIN_NF_PAY_ES_SER";
        public static string SP_UPD_FORMATO_PT_PS_SER = "SP_UPD_FORMATO_PT_PS_SER";
        public static string SP_GET_TOMBIN_ES_SER = "SP_GET_TOMBIN_ES_SER";
        public static string SP_UPD_TOMBIN_NF_PAY_SER = "SP_UPD_TOMBIN_NF_PAY_SER";
        #endregion

        #region TAX
        public static string SP_GET_FORMATO_TAX = "SP_GET_FORMATO_TAX";
        public static string USP_TOMBIN_INSERT_FORMATO_TAX_PAY = "usp_TOMBIN_INSERT_FORMATO_TAX_PAY";
        public static string SP_GET_TOMBIN_NF_PAY_ES_TAX = "SP_GET_TOMBIN_NF_PAY_ES_TAX";
        public static string SP_UPD_FORMATO_PT_PS_TAX = "SP_UPD_FORMATO_PT_PS_TAX";
        public static string SP_UPD_TOMBIN_NF_PAY_TAX = "SP_UPD_TOMBIN_NF_PAY_TAX";
        public static string SP_GET_TOMBIN_ES_TAX = "SP_GET_TOMBIN_ES_TAX";
        public static string SP_GET_TOMBIN_NF_PAY_TAX = "SP_GET_TOMBIN_NF_PAY_TAX";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_TAX = "SP_UPD_PLAN_TRABAJO_MAESTRO_TAX";
        #endregion

        #region TMD

        public static string SP_GET_FORMATO_TMD = "SP_GET_FORMATO_TMD";
        public static string SP_GET_SOLICITUDES_TMD = "SP_GET_SOLICITUDES_TMD";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_TMD = "SP_UPD_PLAN_TRABAJO_MAESTRO_TMD";
        public static string USP_TOMBIN_INSERT_FORMATO_TMD = "usp_TOMBIN_INSERT_FORMATO_TMD";
        public static string SP_UPD_SOLICITUDES_TMD_ESPECIAL = "SP_UPD_SOLICITUDES_TMD_ESPECIAL";
        public static string SP_UPD_TMD_FORMATO = "SP_UPD_TMD_FORMATO";
        public static string SP_UPD_TOMBIN_NF_TMD = "SP_UPD_TOMBIN_NF_TMD";
        public static string SP_GET_TOMBIN_TMD = "SP_GET_TOMBIN_TMD";
        public static string SP_GET_TOMBIN_CON_TMD = "SP_GET_TOMBIN_CON_TMD";
        #endregion

        #region CNT
        public static string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
        public static string SP_GET_CONCENTRADORAS_CNT_UTILERIAS = "SP_GET_CONCENTRADORAS_CNT_UTILERIAS";
        public static string SP_GET_TOMBIN_CONCENTRADORAS_CNT = "SP_GET_TOMBIN_CONCENTRADORAS_CNT";
        public static string SP_UPD_TOMBIN_CONCENTRADORAS_CNT = "SP_UPD_TOMBIN_CONCENTRADORAS_CNT";
        public static string SP_GET_COUNT_TOMBIN_ENTRADA_SALIDAS = "SP_GET_COUNT_TOMBIN_ENTRADA_SALIDAS";
        public static string SP_GET_TOMBIN_ENTRADAS_SALIDAS_CNT = "SP_GET_TOMBIN_ENTRADAS_SALIDAS_CNT";

        public static string SP_TOMBIN_SELECT_RESUMEN_PAP = "usp_TOMBIN_SELECT_RESUMEN_PAP";
        #endregion

        #region SAT
        //Servicio
        public static string SP_GET_FORMATO_SAT = "SP_GET_FORMATO_SAT";
        public static string SP_GET_CLIENTE_SAT = "SP_GET_CLIENTE_SAT";
        //Utilerias SAT
        public static string SP_GET_SALIDAAPPLET_LECTURA_INICIAL_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_LECTURA_INICIAL_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_SECUENCIA_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_SECUENCIA_UTILERIAS_SAT";
        public static string SP_GET_DIAINHABIL_UTILERIAS_SAT = "usp_GET_DIAINHABIL_UTILERIAS_SAT";
        public static string SP_UPD_PLANTRABAJOMAESTRO_SAT = "usp_Upd_Lecturas_PlanTrabajoMaestro_SAT";
        public static string SP_GET_TOMBIN_NUMREGS_SAT = "dbo.usp_Get_TombinCmsSat_NoRegs_SAT";
        public static string SP_UPD_TOMBIN_FORMATORECHAZADO_TOMI_SAT = "dbo.usp_UPD_TOMBIN_FormatoRechazadoTOMI_SAT";
        public static string SP_UPD_TOMBIN_FORMATORECHAZADO_CMS_SAT = "dbo.usp_UPD_TOMBIN_FormatoRechazadoCMS_SAT";
        public static string SP_UPD_PLANTRABAJOMAESTRO_FRECHAZADO_SAT = "usp_Upd_Lecturas_PlanTrabajoMaestroFRechazado_SAT";
        public static string SP_UPD_TOMBIN_FORMATOACEPTADO_CMS_SAT = "dbo.usp_UPD_TOMBIN_FormatoAceptadoCMS_SAT";
        public static string SP_UPD_TOMBIN_FORMATOACEPTADO_TOMI_SAT = "dbo.usp_UPD_TOMBIN_FormatoAceptadoTOMI_SAT";
        public static string SP_UPD_PLANESTRABAJO_MSTDET_SAT = "usp_UPD_PlanesTrabajoMstDet_SAT";
        public static string SP_UPD_TOMBIN_FORMATOACEPTADO_PENDDBT_CMS_SAT = "dbo.usp_UPD_TOMBIN_FormatoAceptadoPendienteDbtCMS_SAT";
        public static string SP_UPD_TOMBIN_FORMATOACEPTADO_PENDDBT_TOMI_SAT = "dbo.usp_UPD_TOMBIN_FormatoAceptadoPendienteDbtTOMI_SAT";
        public static string SP_UPD_TOMBIN_FORMATOACEPTADO_REPE_CMS_SAT = "dbo.usp_UPD_TOMBIN_FormatoAceptadoRepe_SAT";
        public static string SP_UPD_TOMBIN_FORMATOACEPTADO_REPE_TOMI_SAT = "dbo.usp_UPD_TOMBIN_FormatoAceptadoPendienteDbtTOMI_SAT";
        public static string SP_GET_TOMBIN_DATOS_SAT = "dbo.usp_Get_TombinCmsSat_Datos_SAT";
        public static string SP_UPD_PLANTRABAJOMAESTRO_FECEJEC_SAT = "dbo.usp_UPD_PlanTrabajoMaestro_FechaEjecucion_SAT";

        public static string SP_GET_SALIDAAPPLET_FOLIOS_R_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_FOLIOS_R_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_FOLIOS_A_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_FOLIOS_A_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_CONTEO_R23_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_CONTEO_R23_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_R23_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_R23_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_CLAVES_14704_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_CLAVES_14704_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_R47_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_R47_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_COLUMNAS_C_ALL_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_COLUMNAS_C_ALL_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_FINALES_1_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_FINALES_1_UTILERIAS_SAT";
        public static string SP_GET_SALIDAAPPLET_A16_UTILERIAS_SAT = "usp_GET_SALIDAAPPLET_A16_UTILERIAS_SAT";

        #endregion

        #region FXD
        /// <summary>
        /// Procedimientos almacenados para las operaciones FXD_GPH
        /// </summary>
        public static string SP_GET_TOMBIN_FXD_GPH = "SP_GET_TOMBIN_FXD_GPH";
        public static string SP_UPD_TOMBIN_FXD_GPH = "SP_UPD_TOMBIN_FXD_GPH";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXD_GPH = "SP_UPD_PLAN_TRABAJO_MAESTRO_FXD_GPH";

        /// <summary>
        /// Procedimientos almacenados para las operaciones FXD_H2H
        /// </summary>
        public static string SP_GET_TOMBIN_FXD_H2H = "SP_GET_TOMBIN_FXD_H2H";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXD_H2H = "SP_UPD_PLAN_TRABAJO_MAESTRO_FXD_H2H";
        public static string SP_GET_VALIDA_PACS008_FXD_Local = "dbo.usp_Get_Lecturas_Valida_PACS008_FXD_Local";
        public static string SP_GET_TOMBINNF_FXD_Local = "dbo.usp_Get_Lecturas_TombinNf_FXD_Local";
        public static string SP_UPD_FOLIOUNICO_FXD_Local = "dbo.usp_Upd_Lecturas_FolioUnico_FXD_Local";
        public static string SP_UPD_PLANMAESTROGRAL_FXD_Local = "dbo.usp_Upd_PlanMaestrogeneral_FXD_Local";
        public static string SP_GET_DATOSCOMISION_FXD_Local = "dbo.usp_Get_Lecturas_DatosComision_FXD_Local";
        public static string SP_GET_DATOSCUSTOMER_FXD_Local = "dbo.sp_GET_CUSTOMERTYPE_FXD_Local";
        public static string SP_GET_TIPOCAMBIO_FXD_Local = "dbo.sp_GET_TipoCambio_FXD_Local";
        public static string SP_UPD_TOMBINNF_SPEIENVIO_FXD_Local = "dbo.usp_Upd_TombinNf_SpeiEnvio_FXD_Local";
        public static string SP_GET_FORMATO_FXD = "dbo.usp_GET_Lecturas_FORMATO_FXD";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXD = "dbo.usp_Upd_Srv_PlanTrabajoMaestro_DtGRal_FXD";


        #endregion

        #region TEF

        /// <summary>
        /// Procedimientos almacenados TEF_GPH
        /// </summary>
        public static string SP_GET_TOMBIN_TEF_GPH = "SP_GET_TOMBIN_TEF_GPH";
        public static string SP_UPD_TOMBIN_TEF_GPH = "SP_UPD_TOMBIN_TEF_GPH";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_TEF_GPH = "SP_UPD_PLAN_TRABAJO_MAESTRO_TEF_GPH";

        /// <summary>
        /// Procedimientos almacenados TEF_H2H
        /// </summary>
        public static string SP_GET_TOMBIN_TEF_H2H = "SP_GET_TOMBIN_TEF_H2H";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_TEF_H2H = "SP_UPD_PLAN_TRABAJO_MAESTRO_TEF_H2H";

        /// <summary>
        /// Procedimientos almacenados TEF_Local
        /// </summary>
        public static string SP_GET_TOMBIN_TEF_Local = "SP_GET_TOMBIN_TEF_Local";
        public static string SP_UPD_TOMBIN_TEF_Local = "SP_UPD_TOMBIN_TEF_Local";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_TEF_Local = "SP_UPD_PLAN_TRABAJO_MAESTRO_TEF_Local";

        public static string usp_GET_TOMBIN_TEF_H2H = "dbo.usp_GET_TOMBIN_TEF_H2H";
        public static string usp_UPD_PLAN_TRABAJO_MAESTRO_TEF_H2H = "dbo.usp_UPD_PLAN_TRABAJO_MAESTRO_TEF_H2H";


        public static string usp_GET_FORMATO_TEF = "dbo.usp_GET_FORMATO_TEF";
        public static string usp_UPD_PLAN_TRABAJO_MAESTRO_TEF = "dbo.usp_UPD_PLAN_TRABAJO_MAESTRO_TEF";
        public static string usp_GET_TOMBIN_NF_TEF = "dbo.usp_GET_TOMBIN_NF_TEF";
        public static string usp_GET_TOMBIN_ES_TEF = "dbo.usp_GET_TOMBIN_ES_TEF";
        public static string usp_GET_TOMBIN_ES_TEF_CONCILIA = "dbo.usp_GET_TOMBIN_ES_TEF_CONCILIA";
        public static string usp_UPD_FORMATO_PT_PS_TEF = "dbo.usp_UPD_FORMATO_PT_PS_TEF";
        public static string usp_UPD_TOMBIN_NF_TEF = "dbo.usp_UPD_TOMBIN_NF_TEF";
        public static string usp_GET_TOMBIN_NF_ES_TEF = "dbo.usp_GET_TOMBIN_NF_ES_TEF";
        public static string usp_GET_TOMBIN_NF_ES_TEF_DATOS = "dbo.usp_GET_TOMBIN_NF_ES_TEF_Datos";

        public static string usp_TOMBIN_INSERT_FORMATO_TEF = "dbo.usp_TOMBIN_INSERT_FORMATO_TEF";
        public static string usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "dbo.usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";

        public static string usp_GET_TOMBIN_TEF_GPH = "dbo.usp_GET_TOMBIN_TEF_GPH";
        public static string usp_UPD_TOMBIN_TEF_GPH = "dbo.usp_UPD_TOMBIN_TEF_GPH";
        public static string usp_UPD_PLAN_TRABAJO_MAESTRO_TEF_GPH = "dbo.usp_UPD_PLAN_TRABAJO_MAESTRO_TEF_GPH";

        public static string usp_GET_TOMBIN_TEF_Local = "dbo.usp_GET_TOMBIN_TEF_Local";
        public static string usp_UPD_TOMBIN_TEF_Local = "dbo.usp_UPD_TOMBIN_TEF_Local";
        public static string usp_UPD_PLAN_TRABAJO_MAESTRO_TEF_Local = "dbo.usp_UPD_PLAN_TRABAJO_MAESTRO_TEF_Local";

        public static string sp_InterfaceTOMI_TEF_Participantes = "sp_InterfaceTOMI_TEF_Participantes";
        public static string sp_InterfaceTOMI_TEF_recibidas = "sp_InterfaceTOMI_TEF_Recibidas";
        public static string SP_UPD_DEVOLUCIONES_PLAN_TRABAJOMAESTRO_ENT_TEF = "usp_Upd_Devoluciones_Plan_TrabajoMestro_ENT_TEF";
        public static string SP_UPD_PLANTRABAJOMAESTRO_DTGRAL_ENT_TEF = "dbo.usp_Upd_Plan_TrabajoMestroDtGral_ENT_TEF";
        public static string SP_GET_Tombin_NF_ES_TEF = "dbo.usp_Get_TOMBIN_NF_ES_TEF";
        public static string SP_GET_Tombin_NF_ES_TEF_NOREGS = "usp_SP_GET_Tombin_NF_ES_TEF_NOREGS";

        #endregion

        #region FXI

        public static string SP_GET_FORMATO_FXI = "dbo.usp_GET_FORMATO_FXI";
        public static string SP_GET_SOLICITUD_FOREX_BANCOS_FAX_FXI = "dbo.usp_Get_Solicitud_Forex_Bancos_FAX_FXI";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXI = "dbo.usp_Upd_Plan_TrabajoMaestro_Fxi";
        public static string SP_UPD_FORMATO_RECC_CMS_FXI = "dbo.usp_Upd_Formato_RECC_CMS_FXI";
        public static string SP_UPD_FORMATO_RECC_TOMI_FXI = "dbo.usp_Upd_Formato_RECC_CMS_FXI";
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_FXI = "dbo.usp_Upd_Plan_TrabajoMaestro_CambioEstatus_FXI";
        public static string SP_UPD_FORMATO_ACEPTADO_CMS_FXI = "dbo.usp_Upd_Formato_ACEPTADO_CMS_FXI";
        public static string SP_UPD_FORMATO_ACEPTADO_TOMI_FXI = "dbo.usp_Upd_Formato_Aceptado_TOMI_FXI";
        public static string SP_UPD_FORMATO_ACEPTADOREPE_CMS_FXI = "dbo.usp_Upd_Formato_AceptadoRepe_CMS_FXI";
        public static string SP_UPD_FORMATO_ACEPTADOREPE_TOMI_FXI = "dbo.usp_Upd_Formato_AceptadoRepe_TOMI_FXI";
        public static string USP_TOMBIN_INSERT_FORMATO_FXI = "usp_TOMBIN_INSERT_FORMATO_FXI";
        public static string USP_TOMBIN_INSERT_FORMATO_FXE = "usp_TOMBIN_INSERT_FORMATO_FXE";

        /// <summary>
        /// Procedimientos almacenados FXI_Local
        /// </summary>
        public static string SP_GET_TOMBIN_FXI_Local = "SP_GET_TOMBIN_FXI_Local";
        public static string SP_UPD_TOMBIN_FXI_Local = "SP_UPD_TOMBIN_FXI_Local";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXI_Local = "SP_UPD_PLAN_TRABAJO_MAESTRO_FXI_Local";

        /// <summary>
        /// Procedimientos almacenados FXI_H2H
        /// </summary>
        public static string SP_GET_TOMBIN_FXI_H2H = "SP_GET_TOMBIN_FXI_H2H";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXI_H2H = "SP_UPD_PLAN_TRABAJO_MAESTRO_FXI_H2H";

        /// <summary>
        /// Procedimientos almacenados FXI_GPH
        /// </summary>
        public static string SP_GET_TOMBIN_FXI_GPH = "SP_GET_TOMBIN_FXI_GPH";
        public static string SP_UPD_TOMBIN_FXI_GPH = "SP_UPD_TOMBIN_FXI_GPH";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXI_GPH = "SP_UPD_PLAN_TRABAJO_MAESTRO_FXI_GPH";
        public static string SP_UPD_GPH_DETALLE_FXI_GPH = "SP_UPD_GPH_DETALLE_FXI_GPH";


        #endregion

        #region FXT
        /// <summary>
        /// Procedimientos almacenados FXT
        /// </summary>
        public static string SP_GET_FORMATO_FXT = "SP_GET_FORMATO_FXT";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXT = "SP_UPD_PLAN_TRABAJO_MAESTRO_FXT";
        public static string USP_TOMBIN_INSERT_FORMATO_FXT = "usp_TOMBIN_INSERT_FORMATO_FXT";
        public static string SP_GET_REGS_NF_FXT = "SP_GET_REGS_NF_FXT";
        public static string SP_UPD_FORMATO_RECHAZO_FXT = "SP_UPD_FORMATO_RECHAZO_FXT";
        public static string SP_UPD_TOMBIN_NF_FXT = "SP_UPD_TOMBIN_NF_FXT";
        public static string SP_UPD_PLAN_TRABAJO_RECHAZO_FOREX_FXT = "SP_UPD_PLAN_TRABAJO_RECHAZO_FOREX_FXT";
        public static string SP_UPD_FORMATO_ACEPTADO_FXT = "SP_UPD_FORMATO_ACEPTADO_FXT";
        public static string SP_GET_TOMBIN_NF_FXT = "SP_GET_TOMBIN_NF_FXT";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_CONCILAR_FXT = "SP_UPD_PLAN_TRABAJO_MAESTRO_CONCILAR_FXT";

        /// <summary>
        /// Procedimientos almacenados FXT_Local
        /// </summary>
        public static string SP_GET_TOMBIN_FXT_Local = "SP_GET_TOMBIN_FXT_Local";
        public static string SP_UPD_TOMBIN_FXT_Local = "usp_SP_UPD_TOMBIN_FXT_Local";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXT_Local = "usp_SP_UPD_PLAN_TRABAJO_MAESTRO_FXT_Local";

        /// <summary>
        /// Procedimientos almacenados FXT_H2H
        /// </summary>
        public static string SP_GET_TOMBIN_FXT_H2H = "SP_GET_TOMBIN_FXT_H2H";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXT_H2H = "SP_UPD_PLAN_TRABAJO_MAESTRO_FXT_H2H";

        /// <summary>
        /// Procedimientos almacenados FXT_GPH
        /// </summary>
        public static string SP_GET_TOMBIN_FXT_GPH = "SP_GET_TOMBIN_FXT_GPH";
        public static string SP_UPD_TOMBIN_FXT_GPH = "usp_SP_UPD_TOMBIN_FXT_GPH";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXT_GPH = "usp_SP_UPD_PLAN_TRABAJO_MAESTRO_FXT_GPH";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_FXT_GPH_FECFUT = "usp_SP_UPD_PLAN_TRABAJO_MAESTRO_FXT_GPH_FECFUT";
        public static string SP_DELETE_DETALLE_ARCHIVO_FXT_GPH = "SP_DELETE_DETALLE_ARCHIVO_FXT_GPH";
        public static string SP_INSERT_DETALLE_ARCHIVO_FXT_GPH = "SP_INSERT_DETALLE_ARCHIVO_FXT_GPH";

        #endregion

        #region SPID

        public static string SP_GET_TOMBIN_SPID_Local = "SP_GET_TOMBIN_SPID_Local";
        public static string SP_UPD_TOMBIN_SPID_Local = "usp_SP_UPD_TOMBIN_SPID_Local";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_SPID_Local = "usp_SP_UPD_PLAN_TRABAJO_MAESTRO_SPID_Local";

        public static string SP_GET_TOMBIN_SPID_GPH = "SP_GET_TOMBIN_SPID_GPH";
        public static string SP_UPD_TOMBIN_SPID_GPH = "SP_UPD_TOMBIN_SPID_GPH";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_SPID_GPH = "SP_UPD_PLAN_TRABAJO_MAESTRO_SPID_GPH";

        public static string SP_GET_FORMATO_SPID = "usp_GET_FORMATO_SPID"; 
        public static string SP_GET_SOLICITUD_FOREX_BANCOS_FAX_SPID = "usp_GET_SOLICITUD_FOREX_BANCOS_FAX_SPID";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_SPID = "usp_UPD_PLAN_TRABAJO_MAESTRO_SPID";
        public static string SP_GET_TOMBIN_NF_SPID = "usp_GET_TOMBIN_NF_SPID";
        public static string USP_TOMBIN_INSERT_FORMATO_SPD = "usp_TOMBIN_INSERT_FORMATO_SPD";
        public static string SP_GET_TOMBIN_ES_SPID = "usp_GET_TOMBIN_ES_SPID";
        public static string SP_GET_TOMBIN_NF_ES_SPID = "usp_GET_TOMBIN_NF_ES_SPID";
        public static string SP_UPD_FORMATO_CMS_SPID = "usp_UPD_FORMATO_CMS_SPID"; 
        public static string SP_UPD_FORMATO_TOMI_SPID = "usp_UPD_FORMATO_TOMI_SPID"; 
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_SPID = "usp_UPD_SRV_PLAN_TRABAJO_MAESTRO_SPID"; 
        public static string SP_GET_TOMBIN_NF_CONCILIAR_SPID = "usp_GET_TOMBIN_NF_CONCILIAR_SPID"; 
        public static string SP_GET_TOMBIN_ES_CONCILIAR_SPID = "usp_GET_TOMBIN_ES_CONCILIAR_SPID";
        public static string SP_GET_DIAS_NO_HABILES_SPID = "usp_GET_DIAS_NO_HABILES_SPID"; 
        public static string SP_GET_TOBIN_NF_SPID_H2H = "dbo.usp_Get_Lecturas_TombinNF_SPID_H2H";
        public static string SP_UPD_PLANTRABAJOGENERAL_SPID_H2H = "dbo.usp_Upd_PlanTrabajoGeneral_SPID_H2H";
        public static string SP_UPD_PLANTTRABAJOMAESTRO_MSGERROR_SPID_H2H = "dbo.usp_Upd_PlanTrabajoMestro_MsgError_SPID_H2H";
        public static string SP_GET_LIST_CUSTOMER_EDOCTA_INSIGHT = "dbo.sp_GET_LIST_CUSTOMER_EdoCta_Insight";
        public static string SP_UPD_DIRECCION_TBTOMBINNF_SPID_H2H = "dbo.usp_TombinNF_Direccion_SPID_H2H";
        #endregion

        #region MFX
        public static string SP_GET_FORMATO_MFX = "SP_GET_FORMATO_MFX";
        public static string SP_GET_SOLICITUDESFOREX_MFX = "SP_GET_SOLICITUDESFOREX_MFX";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_MFX = "SP_UPD_PLAN_TRABAJO_MAESTRO_MFX";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_CANCELAR_OP_MFX = "SP_UPD_PLAN_TRABAJO_MAESTRO_CANCELAROP_MFX";
        public static string SP_INS_AUTORIZACIONFX_MFX = "SP_INS_AUTORIZACIONFX_MFX";
        public static string SP_UPD_AUTORIZACIONFX_MFX = "SP_UPD_AUTORIZACIONFX_MFX";
        public static string SP_UPD_SOLICITUDESFOREX_MFX = "SP_UPD_SOLICITUDESFOREX_MFX";
        public static string SP_GET_VISTA_ACCOUNT_CUSTOMER_FXT = "SP_GET_VISTA_ACCOUNT_CUSTOMER_FXT";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_CONFIRMACION_MFX = "SP_UPD_PLAN_TRABAJO_MAESTRO_CONFIRMACION_MFX";
        public static string SP_UPD_PLAN_TRABAJO_ESPERANDO_CONFIRMACION_MFX = "SP_UPD_PLAN_TRABAJO_ESPERANDO_CONFIRMACION_MFX";
        public static string SP_INS_FORMATO_MFX = "SP_INS_FORMATO_MFX";
        public static string SP_GET_TOMBIN_NF_MFX = "SP_GET_TOMBIN_NF_MFX";
        public static string SP_UPD_SOLICITUDES_FOREX_CMS_A_TOMI_MFX = "SP_UPD_SOLICITUDES_FOREX_CMS_A_TOMI_MFX";
        public static string SP_UPD_PLAN_TRABAJO_TESORERIA_MFX = "SP_UPD_PLAN_TRABAJO_TESORERIA_MFX";
        public static string SP_UPD_FORMATO_RECHAZO_MFX = "SP_UPD_FORMATO_RECHAZO_MFX";
        public static string SP_UPD_TOMBIN_NF_MFX = "SP_UPD_TOMBIN_NF_MFX";
        public static string SP_GET_FORMATO_CONCILIACION_MFX = "SP_GET_FORMATO_CONCILIACION_MFX";
        public static string SP_GET_TOMBIN_NF_CONCILIACION_MFX = "SP_GET_TOMBIN_NF_CONCILIACION_MFX";

        #endregion

        #region PaP
        /// <summary>
        /// Procedimientos almacenados de la operacion PAP_FXT
        /// </summary>
        public static string USP_TOMBIN_INSERT_FORMATO_FXT_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_FXT_PAP_DETALLE";
        public static string USP_TOMBIN_INSERT_FORMATO_FXT_PAP_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_FXT_PAP_MAESTRO";
        public static string SP_GET_FORMATO_PaP_FXT = "SP_GET_FORMATO_PaP_FXT";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_PAP_FXT = "SP_UPD_PLAN_TRABAJO_MAESTRO_PAP_FXT";
        public static string SP_GET_FOREX_BANCOS_FAX_PAP_FXT = "SP_GET_FOREX_BANCOS_FAX_PAP_FXT";
        public static string SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_AMOUNT = "usp_SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_AMOUNT";
        public static string SP_GET_TOMBIN_NF_FXT_PAP_DETALLE = "usp_SP_GET_TOMBIN_NF_FXT_PAP_DETALLE";
        public static string SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_NOREGS = "usp_SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_NOREGS";
        public static string SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_CAMBIO_ESTATUS = "SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_CAMBIO_ESTATUS";
        public static string SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_CAMBIO_ESTATUS_NOPEND = "usp_SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_CAMBIO_ESTATUS_NOPEND";
        public static string SP_GET_TOMBIN_NF_FXT_PAP_CAMBIO_ESTATUS = "SP_GET_TOMBIN_NF_FXT_PAP_CAMBIO_ESTATUS";
        public static string SP_GET_TOMBIN_NF_FXT_PAP_CAMBIO_ESTATUS_FMTO = "usp_SP_GET_TOMBIN_NF_FXT_PAP_DETALLE_CAMBIO_ESTATUS_FMTO";
        public static string SP_UPD_FORMATO_CMS_PAP_FXT = "SP_UPD_FORMATO_CMS_PAP_FXT";
        public static string SP_UPD_FORMATO_TOMI_PAP_FXT = "SP_UPD_FORMATO_TOMI_PAP_FXT";
        public static string SP_UPD_FORMATO_TOMI_PAP_FXT_ACEPTADO = "dbo.usp_SP_UPD_FORMATO_TOMI_PAP_FXT_ACEPTADO";
        public static string SP_UPD_FORMATO_CMS_PAP_FXT_ACEPTADO = "usp_SP_UPD_FORMATO_CMS_PAP_FXT_ACEPTADO";
        public static string SP_UPD_PLAN_TRABAJO_CAMBIO_ESTATUS_PAP_FXT = "SP_UPD_PLAN_TRABAJO_CAMBIO_ESTATUS_PAP_FXT";
        public static string SP_UPD_PLAN_TRABAJO_CAMBIO_ESTATUS_FOLIO_MAESTRO_PAP_FXT = "SP_UPD_PLAN_TRABAJO_CAMBIO_ESTATUS_FOLIO_MAESTRO_PAP_FXT";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_CONCILIAR_PAP_FXT = "SP_UPD_PLAN_TRABAJO_MAESTRO_CONCILIAR_PAP_FXT";

        /// <summary>
        /// Procedimientos almacenados de la operacion PAP_TEF
        /// </summary>
        public static string SP_GET_FORMATO_PAP_TEF = "SP_GET_FORMATO_PAP_TEF";
        public static string SP_GET_FORMATO_DETALLE_PAP_TEF = "SP_GET_FORMATO_DETALLE_PAP_TEF";
        public static string SP_GET_SOLICITUD_FOREX_BANCOS_FAX_PAP_TEF = "SP_GET_SOLICITUD_FOREX_BANCOS_FAX_PAP_TEF";
        public static string SP_GET_TOMBIN_NF_FXT_TEF_DETALLE = "SP_GET_TOMBIN_NF_TEF_DETALLE";
        public static string SP_GET_TOMBIN_NF_ES_PAP_TEF = "SP_GET_TOMBIN_NF_ES_PAP_TEF";
        public static string SP_GET_TOMBIN_NF_TEF_DETALLE = "SP_GET_TOMBIN_NF_TEF_DETALLE";
        public static string SP_GET_TOMBIN_NF_TEF = "SP_GET_TOMBIN_NF_TEF";
        public static string SP_GET_TOMBIN_NF_TEF_CONCILIA = "SP_GET_TOMBIN_NF_TEF_CONCILIA";

        public static string SP_GET_TOMBIN_NF_TEF_NOREGS = "SP_GET_TOMBIN_NF_TEF_NOREGS";
        public static string SP_UPD_FORMATO_CMS_PAP_TEF = "SP_UPD_FORMATO_CMS_PAP_TEF";
        public static string SP_UPD_FORMATO_TOMI_PAP_TEF = "SP_UPD_FORMATO_TOMI_PAP_TEF";
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_PAP_TEF = "SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_PAP_TEF";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_PAP_TEF = "SP_UPD_PLAN_TRABAJO_MAESTRO_PAP_TEF";
        public static string USP_TOMBIN_INSERT_FORMATO_TEF_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_TEF_PAP_DETALLE";
        public static string USP_TOMBIN_INSERT_FORMATO_TEF_PAP_MAESTRO = "USP_TOMBIN_INSERT_FORMATO_TEF_PAP_MAESTRO";

        /// <summary>
        /// Procedimientos almacenados de la operacion PAP_FXI
        /// </summary>
        public static string SP_GET_FORMATO_PAP_FXI = "SP_GET_FORMATO_PAP_FXI";
        public static string SP_GET_FORMATO_DETALLE_PAP_FXI = "SP_GET_FORMATO_DETALLE_PAP_FXI";
        public static string SP_GET_TOMBIN_NF_FXI = "SP_GET_TOMBIN_NF_FXI";
        public static string SP_GET_TOMBIN_NF_ES_PAP_FXI = "SP_GET_TOMBIN_NF_ES_PAP_FXI";
        public static string SP_GET_TOMBIN_NF_FXI_DETALLE = "SP_GET_TOMBIN_NF_FXI_DETALLE";
        public static string SP_UPD_FORMATO_CMS_PAP_FXI = "SP_UPD_FORMATO_CMS_PAP_FXI";
        public static string SP_UPD_FORMATO_TOMI_PAP_FXI = "SP_UPD_FORMATO_TOMI_PAP_FXI";
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_PAP_FXI = "SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_PAP_FXI";
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CONCILIAR_PAP_FXI = "SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CONCILIAR_PAP_FXI";
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CONCILIAR_FM_PAP_FXI = "SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CONCILIAR_FM_PAP_FXI";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_PAP_FXI = "SP_UPD_PLAN_TRABAJO_MAESTRO_PAP_FXI";
        public static string USP_TOMBIN_INSERT_FORMATO_FXI_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_FXI_PAP_DETALLE";
        public static string SP_GET_SOLICITUD_FOREX_BANCOS_FAX_PAP_FXI = "usp_GET_SOLICITUD_FOREX_BANCOS_FAX_PAP_FXI";
        public static string SP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
        public static string SP_GET_TOMBIN_NF_FXI_STATUS = "SP_GET_TOMBIN_NF_FXI_STATUS"; 

        //PAP_FXD
        public static string SP_GET_FORMATO_PAP_FXD = "usp_GET_FORMATO_PAP_FXD";
        public static string SP_GET_FORMATO_DETALLE_PAP_FXD = "usp_GET_FORMATO_DETALLE_PAP_FXD";
        public static string SP_GET_SOLICITUD_FOREX_BANCOS_FAX_PAP_FXD = "usp_GET_SOLICITUD_FOREX_BANCOS_FAX_PAP_FXD";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_PAP_FXD = "usp_UPD_PLAN_TRABAJO_MAESTRO_PAP_FXD";
        public static string SP_GET_TOMBIN_NF_FXD = "usp_GET_TOMBIN_NF_FXD";
        public static string SP_GET_TOMBIN_NF_ES_PAP_FXD = "usp_GET_TOMBIN_NF_ES_PAP_FXD";
        public static string SP_GET_TOMBIN_NF_FXD_DETALLE = "usp_GET_TOMBIN_NF_FXD_DETALLE";
        public static string SP_GET_TOMBIN_NF_PAP = "usp_GET_TOMBIN_NF_PAP"; 

        public static string SP_GET_TOMBIN_NF_FXD_MASTER = "usp_GET_TOMBIN_NF_FXD_MASTER"; 

        public static string SP_UPD_FORMATO_CMS_PAP_FXD = "usp_UPD_FORMATO_CMS_PAP_FXD";
        public static string SP_UPD_FORMATO_TOMI_PAP_FXD = "usp_UPD_FORMATO_TOMI_PAP_FXD";
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_PAP_FXD = "usp_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_PAP_FXD";
        public static string SP_UPD_FORMATO_CMS_PAP_FXD_APFT = "usp_UPD_FORMATO_CMS_PAP_FXD_APFT";
        public static string SP_UPD_FORMATO_TOMI_PAP_FXD_SCNP = "usp_UPD_FORMATO_TOMI_PAP_FXD_SCNP"; 

        public static string SP_UPD_FORMATO_TOMI_PAP_FXD_REPE = "usp_UPD_FORMATO_TOMI_PAP_FXD_REPE"; 

        public static string SP_UPD_FORMATO_TOMI_PAP_FXD_SCNF = "usp_UPD_FORMATO_TOMI_PAP_FXD_SCNF"; 



        public static string SP_GET_SUM_TOMBIN_NF_FXD_DETALLE_TOMI = "usp_GET_SUM_TOMBIN_NF_FXD_DETALLE_TOMI"; 

        public static string SP_GET_TOMBIN_ENTSAL_FXD = "usp_GET_TOMBIN_ENTSAL_FXD"; 

        public static string USP_TOMBIN_INSERT_FORMATO_FXD_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_FXD_PAP_DETALLE";
        public static string USP_TOMBIN_INSERT_FORMATO_FXD_PAP_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_FXD_PAP_MAESTRO";
        public static string SP_GET_TOMBIN_NF_FXD_PAP_DETALLE = "usp_GET_TOMBIN_NF_FXD_PAP_DETALLE";

        public static string SP_GET_FORMATO_PAP_SPID = "usp_GET_FORMATO_PAP_SPID";
        public static string SP_GET_FORMATO_DETALLE_PAP_SPID = "usp_GET_FORMATO_DETALLE_PAP_SPID";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_PAP_SPID = "usp_UPD_PLAN_TRABAJO_MAESTRO_PAP_SPID";
        public static string SP_GET_TOMBIN_NF_SPID_PAP = "usp_GET_TOMBIN_NF_SPID_PAP";
        public static string SP_GET_TOMBIN_NF_DETALLE_PAP_SPID = "usp_GET_TOMBIN_NF_DETALLE_PAP_SPID";
        public static string SP_GET_TOMBIN_NF_DETALLE_PAP_SPID_NREGS = "dbo.usp_GET_TOMBIN_NF_DETALLE_PAP_SPID_NoRegsF";
        public static string SP_GET_TOMBIN_NF_DETALLE_PAP_MasRegs = "usp_SP_GET_TOMBIN_NF_DETALLE_PAP_MasRegs";
        public static string SP_GET_TOMBIN_NF_DETALLE_PAP_SPID_Amount = "usp_GET_TOMBIN_NF_DETALLE_PAP_SPID_Amount";
        public static string SP_GET_TOMBIN_NF_DETALLE_PAP_SPID_NREGS_NOPEND = "usp_GET_TOMBIN_NF_DETALLE_PAP_SPID_NREGNOPEND";
        public static string SP_GET_TOMBIN_ES_PAP_SPID = "usp_GET_TOMBIN_ES_PAP_SPID";
        public static string USP_TOMBIN_INSERT_FORMATO_SPID_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_SPID_PAP_DETALLE";
        public static string SP_GET_TOMBIN_NF_PAP_SPID_DETALLE = "usp_GET_TOMBIN_NF_PAP_SPID_DETALLE";

        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CONCILIAR_PAP_SPID = "dbo.usp_Upd_Srv_PlanTrabajoMaestro_Conciliar_PaP_SPID";
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CONCILIAR_FM_PAP_SPID = "usp_Upd_Srv_PlanTrabajoMaestro_ConciliarFM_PaP_SPID";
        public static string USP_TOMBIN_INSERT_FORMATO_SPID_PAP_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_SPID_PAP_MAESTRO";

        public static string SP_GET_FORMATO_PAP_FXT = "usp_SP_GET_FORMATO_PAP_FXT";
        public static string SP_GET_FORMATO_PAP_FXT_DETALLE = "usp_SP_GET_FORMATO_PAP_FXT_DETALLE";
        #endregion

        #region ENT
        public static string Union_Bank = "4500180";
        public static string New_York = "4500202";
        public static string Head_Office = "4500164";
        public static string Londres = "4500253";
        public static string BTMU_BRASIL = "5007436";
        public static string BTMU_HOLLAND_AMSTERDAM = "5004817";
        public static string BTMU_LOS_ANGELES = "5003870";
        public static string BTMU_SINGAPORE = "5003985";
        public static string BTMU_ESPANA = "5006377";
        public static string SP_GET_PARTICIPANTES_SPEI_ENT_COAS = "sp_InterfaceTOMI_SPEI_Participantes";
        public static string SP_GET_BMX_ORD_PAGDIAN_R_ENT_COAS = "dbo.usp_GetBmxOrd_PagDian_R_ENT_COAS";
        public static string SP_UPD_BMX_ORD_PAGDIA_E_ENT_COAS = "dbo.usp_Upd_BmxOrdPagDia_E_ENT_COAS";
        public static string SP_GET_BMX_ORD_PAGDIA_E_ENT_COAS = "dbo.usp_GetBmxOrd_PagDia_E_ENT_COAS";
        public static string SP_UPD_DEVOLUCIONES_PLAN_TRABAJO_MAESTRO_ENT_COAS = "dbo.usp_Upd_Devoluciones_Plan_TrabajoMestro_ENT_COAS";
        public static string SP_UPD_DEVOLUCIONES_BMX_ORD_PAGDIA_R_ENT_COAS = "dbo.usp_Upd_Devoluciones_BmxOrdPagdia_R_ENT_COAS";
        public static string SP_UPD_DEVOLUCIONES_BMX_ORD_PAGDIA_E_ENT_COAS = "dbo.usp_Upd_Devoluciones_BmxOrdPagdia_E_ENT_COAS";
        public static string SP_UPD_DATOS_GRAL_PLAN_TRABAJO_MAESTRO_ENT_COAS = "dbo.usp_Upd_DatosGral_PlanTrabajoMaestro_ENT_COAS";
        public static string SP_GET_TOMBIN_NF_ENTRADAS_SPEI_ENT_COAS = "dbo.usp_Get_TombinNfEntradas_SPEI_ENT_COAS";
        public static string SP_UPD_BMX_ORD_PAGDIA_R_COAS_TOMI_ENT_COAS = "dbo.usp_Upd_OrdPagDia_R_COAS_TOMI_ENT_COAS";
        public static string SP_UPD_TOMBIN_NF_ENTRADAS_COAS_A_TOMI_ENT_COAS = "dbo.usp_Upd_TombinNFEntradas_COAS_TOMI_ENT_COAS";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_COAS_A_TOMI_ENT_COAS = "dbo.usp_Upd_PlanTrabajoMestro_COAS_A_TOMI_ENT_COAS";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_DETALLE_COAS_A_TOMI_ENT_COAS = "dbo.usp_Upd_PlanTrabajoDetalle_COAS_A_TOMI_ENT_COAS";
        public static string SP_GET_TOMBIN_ES_SPEI_ENT_COAS = "dbo.usp_Get_TombinES_SPEI_ENT_COAS";
        public static string SP_UPD_BMX_ORD_PAGDIA_R_CAMBIO_ESTATUS_ENT_COAS = "dbo.usp_Upd_MbxOrd_PagDia_R_CambioEstatus_ENT_COAS";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_ENT_COAS = "dbo.usp_Upd_PlanTrabajoMaestro_CambioEstatus_ENT_COAS";
        public static string SP_UPD_TOMBIN_NF_ENTRADAS_SPEI_CAMBIO_ESTATUS_RECC_ENT_COAS = "dbo.usp_Upd_TombinEs_SPEI_CambioEstatus_Recc_ENT_COAS";
        public static string SP_UPD_TOMBIN_NF_ENTRADAS_SPEI_CAMBIO_ESTATUS_CONP_ENT_COAS = "dbo.usp_Upd_TombinEs_SPEI_CambioEstatus_Conp_ENT_COAS";
        public static string SP_GET_TOMBIN_NF_ENTRADAS_ENT_COAS = "dbo.usp_Get_TombinNf_Entradas_ENT_COAS";
        public static string SP_GET_TOMBIN_NF_ENTRADAS_REF_ENT_COAS = "dbo.usp_Get_TombinNf_EntradasRef_ENT_COAS";
        public static string USP_TOMBIN_INSERT_FORMATO_SPEI = "usp_TOMBIN_INSERT_FORMATO_SPEI";
        public static string sp_InterfaceTOMI_SPID_Estatus_Recibidas = "sp_InterfaceTOMI_SPID_Estatus_Recibidas";
        public static string sp_InterfaceTOMI_SPID_Participantes = "sp_InterfaceTOMI_SPID_Participantes";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_ENT_SPID = "dbo.usp_Upd_PlanTrabajoMestro_ENT_SPID";
        public static string sp_InterfaceTOMI_SPEI_Estatus_Recibidas = "sp_InterfaceTOMI_SPEI_Estatus_Recibidas";
        public static string SP_UPD_TB_TRABAJO_MESTRO_ENT_SPEI = "dbo.usp_UPD_PLAN_TRABAJO_MAESTRO_ENT_SPEI";
        public static string SP_GET_TB_TOMBINNF_ENTRADAS_ENT_SPEI = "usp_GET_TB_TOMBINNF_ENTRADAS_ENT_SPEI";
        public static string SP_UPD_TOMBINNF_ENTRADAS_ENT_SPEI = "dbo.usp_Upd_TB_TOMBINNF_ENTRADAS_ENT_SPEI";
        public static string SP_UPD_PLANTRABAJOMAESTRO_RECIBIDA_ENT_SPEI = "dbo.usp_Upd_PlanTrabajoMaestro_Recibida_ENT_SPEI";
        public static string SP_UPD_PLANTRABAJODETALLE_RECIBIDA_ENT_SPEI = "dbo.usp_Upd_PlanTrabajoDetalle_Recibida_ENT_SPEI";
        public static string SP_GET_TOMBINNF_ENTRADASSALIDAS_ENT_SPEI = "dbo.usp_GET_TB_TOMBINNF_ENTRADASSALIDAS_ENT_SPEI";
        public static string SP_UPD_PLANTRABAJOMAESTRO_RECHAZADA_ENT_SPEI = "dbo.usp_Upd_Lectura_PlanTrabajoMestro_Rechazada_ENT_SPEI";
        public static string SP_UPD_TB_TOMBINNF_ENTRADAS_CONPPEND_ENT_SPEI = "dbo.usp_Upd_TB_TOMBINNF_ENTRADAS_CONPPEND_ENT_SPEI";
        public static string SP_InterfaceTOMI_SPEI_Cambia_Estatus_Recibidas = "sp_InterfaceTOMI_SPEI_Cambia_Estatus_Recibidas";
        public static string SP_InterfaceTOMI_get_DataDev = "sp_InterfaceTOMI_get_dataDev";
        public static string SP_InterfaceTOMI_SPEI_0 = "sp_InterfaceTOMI_SPEI_0";
        public static string SP_GET_CAT_AUTODEVS = "dbo.usp_Get_CatAutoDevs_ENT_SPEI";
        public static string SP_GET_BUSCACTA_VOSTRO = "dbo.usp_Get_BuscaCtaVostro_ENT_SPEI";
        public static string SP_UPD_PLANTRABAJOMAESTRO_DEVOLUCION_ENT_SPEI = "dbo.usp_Upd_PlanTrabajoMaestro_Devolucion_ENT_SPEI";
        public static string SP_GET_TOMBIN_NF_ENTRADAS_ENT_SPID = "dbo.usp_Get_TombinNfEntradas_SPID";
        public static string SP_UPD_TOMBIN_ENTRADAS_ENT_SPID = "dbo.usp_UPD_TombinNfEntradas_PROC_SPID";
        public static string SP_GET_TOMBIN_ES_ENT_SPID = "dbo.usp_Get_TOMBIN_ES_ENT_SPID";

        public static string SP_UPD_PLANTRABAJOMAESTRO_RECIBIDANOABON_ENT_TEF = "dbo.usp_Upd_Plan_TrabajoMestroRecibidaNoabon_ENT_TEF";
        public static string SP_UPD_PLANTRABAJOMAESTRO_CANCELDEV_ENTE_TEF = "dbo.usp_Upd_Plan_TrabajoMestroCancelDev_ENT_TEF";
        public static string SP_UPD_TOMBINENTRADASTEF_PEND_ENT_TEF = "dbo.usp_Upd_TombienEntradasTef_Pend_ENT_TEF";
        public static string SP_SET_TOMBINNF_ES_ENT_TEF = "usp_Get_TOMBIN_NF_ES_ENT_TEF";
        public static string SP_USP_TOMBIN_INSERT_FORMATO_ENTRADA_TEF = "usp_TOMBIN_INSERT_FORMATO_ENTRADA_TEF";
        public static string sp_InterfaceTOMI_TEF_Cambia_Estatus_Recibidas = "sp_InterfaceTOMI_TEF_Cambia_Estatus_Recibidas";

        #endregion

        #region NOM
        public static string SP_GET_FORMATO_NOM_FXD = "usp_GET_FORMATO_NOM_FXD";
        public static string SP_GET_FORMATO_DETALLE_NOM_FXD = "usp_GET_FORMATO_DETALLE_NOM_FXD";
        public static string SP_GET_SOLICITUD_FOREX_BANCOS_FAX_NOM_FXD = "usp_GET_SOLICITUD_FOREX_BANCOS_FAX_NOM_FXD";
		public static string SP_UPD_PLAN_TRABAJO_MAESTRO_NOM_FXD = "usp_UPD_PLAN_TRABAJO_MAESTRO_NOM_FXD";
        public static string SP_GET_TOMBIN_NF_NON_FXD = "usp_GET_TOMBIN_NF_NON_FXD";
        public static string SP_GET_TOMBIN_NF_FXD_ND_RESUMEN = "usp_GET_TOMBIN_NF_FXD_NOMINA_DETALLE_RESUMEN"; 
        public static string SP_GET_NF_FXD_NOMINA_DETALLE_COUNT = "usp_GET_NF_FXD_NOMINA_DETALLE_COUNT";
        public static string SP_GET_NF_FXD_NOMINA_COUNT = "usp_GET_NF_FXD_NOMINA_COUNT"; 
        public static string SP_UPD_FORMATO_CMS_NOM_FXD = "usp_UPD_FORMATO_CMS_NOM_FXD";
        public static string SP_UPD_FORMATO_TOMI_NOM_FXD = "usp_UPD_FORMATO_TOMI_NOM_FXD";
        public static string SP_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_NOM_FXD = "usp_UPD_SRV_PLAN_TRABAJO_MAESTRO_CAMBIO_ESTATUS_NOM_FXD";
        public static string SP_UPD_ESTADO_FORMATO_NOMINA_APFT = "usp_UPD_ESTADO_NOMINA_APFT"; 
        public static string SP_UPD_NF_FXD_NOMINA_SCNP_CONP = "usp_UPD_NF_FXD_NOMINA_SCNP_CONP"; 
        public static string SP_UPD_ESTADO_NOMINA_REPE = "usp_UPD_ESTADO_NOMINA_REPE"; 
        public static string SP_UPD_TOMI_NF_FXD_NOMINA_SCNF_CONF = "usp_UPD_TOMI_NF_FXD_NOMINA_SCNF_CONF"; 
        public static string SP_UPD_SRV_PLAN_TRABAJO_DETALLE_MAESTRO_CAMBIO_ESTATUS_NOM_FXD = "usp_UPD_SRV_PLAN_TRABAJO_DETALLE_MAESTRO_CAMBIO_ESTATUS_NOM_FXD"; 
        public static string SP_GET_TOMBIN_NF_ES_FXI = "usp_GET_TOMBIN_NF_ES_FXI";
        public static string SP_GET_NF_FXD_NOMINA_DETALLE = "usp_GET_NF_FXD_NOMINA_DETALLE"; 
        public static string SP_GET_NF_FXD_NOMINA_DETALLE_RESUMEN_POR_FORMATO = "usp_GET_NF_FXD_NOMINA_DETALLE_RESUMEN_POR_FORMATO"; 
        public static string SP_GET_TB_TOMBIN_ENTRADAS_SALIDAS_BY_REFERENCIA = "usp_GET_TB_TOMBIN_ENTRADAS_SALIDAS_BY_REFERENCIA"; 
                                                                                                                                   
        public static string SP_TOMBIN_INSERT_FORMATO_FXD_NOMINA_DETALLE = "usp_TOMBIN_INSERT_FORMATO_FXD_NOMINA_DETALLE"; 
        public static string SP_TOMBIN_INSERT_FORMATO_FXD_NOMINA_MAESTRO = "USP_TOMBIN_INSERT_FORMATO_FXD_NOMINA_MAESTRO"; 
        public static string SP_GET_TOMBIN_NF_NOM_FXD = "usp_GET_TOMBIN_NF_NOM_FXD";
        public static string SP_GET_TOMBIN_ES_NOM_FXD = "dbo.usp_Get_TOMBIN_ES_NOM_FXD";
        public static string SP_UPD_FORMATO_PT_PS_FXD = "usp_UPD_FORMATO_PT_PS_FXD";
        public static string SP_UPD_TOMBIN_NF_FXD = "usp_Upd_Tombin_NF_FXD";
        public static string SP_GET_TOMBIN_ES_FXD = "dbo.usp_Get_TOMBIN_ES_FXD";
        public static string SP_GET_TOMBIN_NF_ES_FXD = "dbo.usp_Get_TOMBIN_NF_ES_FXD";

        #endregion

        #region Mercado Dinero

        public static string SP_GET_CALYPSO_LIQUIDACION_MD = "usp_GET_CALYPSO_LIQUIDACION_MD";
        public static string SP_GET_CALYPSO_LIQUIDACION_COMPLEMENTO_MD = "usp_GET_CALYPSO_LIQUIDACION_COMPLEMENTO_MD";
        public static string SP_UPD_COMPLEMENTO_MD = "usp_UPD_COMPLEMENTO_MD";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_DATOS_GRAL_MD = "usp_UPD_PLAN_TRABAJO_MAESTRO_DATOS_GRAL_MD";
        public static string SP_UPD_CALYPSO_LIQUIDACION_MD = "usp_UPD_CALYPSO_LIQUIDACION_MD";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_USER_ESTATUS_MD = "usp_UPD_PLAN_TRABAJO_MAESTRO_USER_ESTATUS_MD";
        public static string SP_UPD_PLAN_TRABAJO_MAESTRO_VALIDAR_FLUJO_MD = "usp_UPD_PLAN_TRABAJO_MAESTRO_VALIDAR_FLUJO_MD";
        public static string SP_GET_CALYPSO_LIQUIDACION_VF = "usp_GET_CALYPSO_LIQUIDACION_VF";

        #endregion
    }
}

namespace Aplicacion.Enums
{
    /// <summary>
    /// Representa Constantes Servicio.
    /// </summary>
    public static class ConstantesServicio
    {
        /// <summary>
        /// Representa Procedimientos CNT.
        /// </summary>
        public static class ProcedimientosCNT
        {
            public const string USP_LECTURA_CNT_OBTENER_CONCENTRADORA_POR_FOLIO = "usp_LecturaCNT_ObtenerConcentradoraPorFolio";
            public const string USP_LECTURA_CNT_ACTUALIZAR_FOLIO_UNICO_CONCENTRADORA = "usp_LecturaCNT_ActualizarFolioUnicoConcentradora";
            public const string USP_LECTURA_CNT_ACTUALIZAR_DATOS_PLAN_TRABAJO_MAESTRO_DESDE_CONCENTRADORA = "usp_LecturaCNT_ActualizarDatosPlanTrabajoMaestroDesdeConcentradora";
            public const string USP_LECTURA_CNT_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA = "usp_LecturaCNT_ContarEntradasSalidasPorReferencia";
            public const string USP_LECTURA_CNT_OBTENER_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaCNT_ObtenerEntradaSalidaPorReferencia";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
        }
/// <summary>
/// Representa Procedimientos ENTCOAS.
/// </summary>

        public static class ProcedimientosENTCOAS
        {
            public const string USP_LECTURA_ENT_COAS_OBTENER_ORDEN_PAGO_RECIBIDA_POR_FOLIO = "usp_LecturaENTCOAS_ObtenerOrdenPagoRecibidaPorFolio";
            public const string USP_LECTURA_ENT_COAS_MARCAR_ORDEN_EMITIDA_COMO_DEVUELTA = "usp_LecturaENTCOAS_MarcarOrdenEmitidaComoDevuelta";
            public const string USP_LECTURA_ENT_COAS_OBTENER_ORDEN_EMITIDA_DEVUELTA_PARA_VALIDACION = "usp_LecturaENTCOAS_ObtenerOrdenEmitidaDevueltaParaValidacion";
            public const string USP_LECTURA_ENT_COAS_CANCELAR_PLAN_TRABAJO_MAESTRO_DEVUELTO = "usp_LecturaENTCOAS_CancelarPlanTrabajoMaestroDevuelto";
            public const string USP_LECTURA_ENT_COAS_MARCAR_ORDEN_RECIBIDA_DEVUELTA_POR_FOLIO = "usp_LecturaENTCOAS_MarcarOrdenRecibidaDevueltaPorFolio";
            public const string USP_LECTURA_ENT_COAS_INSERTAR_DEVOLUCION_COAS_DESDE_ORDEN_RECIBIDA = "usp_LecturaENTCOAS_InsertarDevolucionCoasDesdeOrdenRecibida";
            public const string USP_LECTURA_ENT_COAS_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_DATOS_GENERALES = "usp_LecturaENTCOAS_ActualizarPlanTrabajoMaestroDatosGenerales";
            public const string USP_LECTURA_ENT_COAS_CONTAR_ENTRADAS_SPEI_POR_FOLIO = "usp_LecturaENTCOAS_ContarEntradasSpeiPorFolio";
            public const string USP_LECTURA_ENT_COAS_MARCAR_ORDEN_RECIBIDA_ACEPTADA = "usp_LecturaENTCOAS_MarcarOrdenRecibidaAceptada";
            public const string USP_LECTURA_ENT_COAS_MARCAR_ENTRADA_SPEI_PROCESADA = "usp_LecturaENTCOAS_MarcarEntradaSpeiProcesada";
            public const string USP_LECTURA_ENT_COAS_FINALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA_BBC = "usp_LecturaENTCOAS_FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBbc";
            public const string USP_LECTURA_ENT_COAS_FINALIZAR_DETALLE_PLAN_TRABAJO_EXCEPTO_CEP = "usp_LecturaENTCOAS_FinalizarDetallePlanTrabajoExceptoCep";
            public const string USP_LECTURA_ENT_COAS_FINALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA = "usp_LecturaENTCOAS_FinalizarPlanTrabajoMaestroRecibidaNoAbonada";
            public const string USP_LECTURA_ENT_COAS_CONTAR_ENTRADAS_SALIDAS_POR_FOLIO_UNICO = "usp_LecturaENTCOAS_ContarEntradasSalidasPorFolioUnico";
            public const string USP_LECTURA_ENT_COAS_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA = "usp_LecturaENTCOAS_ActualizarPlanTrabajoMaestroRecibidaNoAbonada";
            public const string USP_LECTURA_ENT_COAS_MARCAR_ORDEN_RECIBIDA_RECHAZADA = "usp_LecturaENTCOAS_MarcarOrdenRecibidaRechazada";
            public const string USP_LECTURA_ENT_COAS_MARCAR_ENTRADA_SPEI_RECHAZADA = "usp_LecturaENTCOAS_MarcarEntradaSpeiRechazada";
            public const string USP_LECTURA_ENT_COAS_MARCAR_ENTRADA_SPEI_CONFIRMADA_PENDIENTE = "usp_LecturaENTCOAS_MarcarEntradaSpeiConfirmadaPendiente";
            public const string USP_LECTURA_ENT_COAS_OBTENER_CONCILIACION_TOMI_POR_FOLIO = "usp_LecturaENTCOAS_ObtenerConciliacionTomiPorFolio";
            public const string USP_LECTURA_ENT_COAS_OBTENER_CONCILIACION_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaENTCOAS_ObtenerConciliacionEntradaSalidaPorReferencia";
            public const string USP_LECTURA_ENT_COAS_OBTENER_ENTRADA_SPEI_TOMI_POR_FOLIO = "usp_LecturaENTCOAS_ObtenerEntradaSpeiTomiPorFolio";
            public const string USP_TOMBIN_INSERT_FORMATO_SPEI = "usp_TOMBIN_INSERT_FORMATO_SPEI";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string SP_INTERFACE_TOMI_SPEI_PARTICIPANTES = "sp_InterfaceTOMI_SPEI_Participantes";
            public const string SP_INTERFACE_TOMI_SPEI_CAMBIA_ESTATUS_RECIBIDAS = "sp_InterfaceTOMI_SPEI_Cambia_Estatus_Recibidas";
        }
/// <summary>
/// Representa Procedimientos ENTTEF.
/// </summary>

        public static class ProcedimientosENTTEF
        {
            public const string SP_INTERFACE_TOMI_TEF_PARTICIPANTES = "sp_InterfaceTOMI_TEF_Participantes";
            public const string SP_INTERFACE_TOMI_TEF_RECIBIDAS = "sp_InterfaceTOMI_TEF_Recibidas";
            public const string SP_INTERFACE_TOMI_TEF_CAMBIA_ESTATUS_RECIBIDAS = "sp_InterfaceTOMI_TEF_Cambia_Estatus_Recibidas";
            public const string USP_LECTURA_ENT_TEF_ACTUALIZAR_DATOS_GENERALES_MAESTRO_CON_CLIENTE = "usp_LecturaENTTEF_ActualizarDatosGeneralesMaestroConCliente";
            public const string USP_LECTURA_ENT_TEF_ACTUALIZAR_DATOS_GENERALES_MAESTRO_SIN_CLIENTE = "usp_LecturaENTTEF_ActualizarDatosGeneralesMaestroSinCliente";
            public const string USP_TOMBIN_INSERT_FORMATO_ENTRADA_TEF = "usp_TOMBIN_INSERT_FORMATO_ENTRADA_TEF";
            public const string USP_LECTURA_ENT_TEF_EXISTE_FORMATO_ENTRADA_TEF = "usp_LecturaENTTEF_ExisteFormatoEntradaTef";
            public const string USP_LECTURA_ENT_TEF_OBTENER_FORMATO_ENTRADA_TEF = "usp_LecturaENTTEF_ObtenerFormatoEntradaTef";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string USP_LECTURA_ENT_TEF_EXISTE_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaENTTEF_ExisteEntradaSalidaPorReferencia";
            public const string USP_LECTURA_ENT_TEF_ACTUALIZAR_ESTATUS_FORMATO_ENTRADA_TEF_A_SCNP = "usp_LecturaENTTEF_ActualizarEstatusFormatoEntradaTefAScnp";
            public const string USP_LECTURA_ENT_TEF_CANCELAR_MAESTRO_POR_DEVOLUCION = "usp_LecturaENTTEF_CancelarMaestroPorDevolucion";
            public const string USP_LECTURA_ENT_TEF_MARCAR_MAESTRO_RECIBIDA_NO_ABONADA = "usp_LecturaENTTEF_MarcarMaestroRecibidaNoAbonada";
            public const string USP_LECTURA_ENT_TEF_OBTENER_CONCILIACION_FORMATO_ENTRADA_TEF = "usp_LecturaENTTEF_ObtenerConciliacionFormatoEntradaTef";
            public const string USP_LECTURA_ENT_TEF_OBTENER_CONCILIACION_ENTRADA_SALIDA = "usp_LecturaENTTEF_ObtenerConciliacionEntradaSalida";
        }
/// <summary>
/// Representa Procedimientos ENTSPEI.
/// </summary>

        public static class ProcedimientosENTSPEI
        {
            public const string USP_LECTURA_ENT_SPEI_OBTENER_NF_ENTRADA_SPEI_POR_FOLIO = "usp_LecturaENTSPEI_ObtenerNfEntradaSpeiPorFolio";
            public const string USP_LECTURA_ENT_SPEI_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_DATOS_GRAL = "usp_LecturaENTSPEI_ActualizarPlanTrabajoMaestroDatosGral";
            public const string USP_TOMBIN_INSERT_FORMATO_SPEI = "usp_TOMBIN_INSERT_FORMATO_SPEI";
            public const string USP_LECTURA_ENT_SPEI_CONTAR_NF_ENTRADAS_SPEI_POR_FOLIO = "usp_LecturaENTSPEI_ContarNfEntradasSpeiPorFolio";
            public const string USP_LECTURA_ENT_SPEI_MARCAR_NF_ENTRADA_SPEI_COMO_PROCESADA = "usp_LecturaENTSPEI_MarcarNfEntradaSpeiComoProcesada";
            public const string USP_LECTURA_ENT_SPEI_ACTUALIZAR_PLAN_TRABAJO_RECIBIDA_NO_ABONADA = "usp_LecturaENTSPEI_ActualizarPlanTrabajoRecibidaNoAbonada";
            public const string USP_LECTURA_ENT_SPEI_CERRAR_PLAN_TRABAJO_DETALLE_EXCEPTO_CEP = "usp_LecturaENTSPEI_CerrarPlanTrabajoDetalleExceptoCep";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string USP_LECTURA_ENT_SPEI_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA = "usp_LecturaENTSPEI_ContarEntradasSalidasPorReferencia";
            public const string USP_LECTURA_ENT_SPEI_ACTUALIZAR_PLAN_TRABAJO_RECHAZO_SIN_ABONO = "usp_LecturaENTSPEI_ActualizarPlanTrabajoRechazoSinAbono";
            public const string USP_LECTURA_ENT_SPEI_MARCAR_NF_ENTRADA_SPEI_COMO_CONP = "usp_LecturaENTSPEI_MarcarNfEntradaSpeiComoConp";
            public const string USP_LECTURA_ENT_SPEI_OBTENER_CONCILIACION_TOMI_POR_FOLIO = "usp_LecturaENTSPEI_ObtenerConciliacionTomiPorFolio";
            public const string USP_LECTURA_ENT_SPEI_OBTENER_CONCILIACION_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaENTSPEI_ObtenerConciliacionEntradaSalidaPorReferencia";
            public const string USP_LECTURA_ENT_SPEI_OBTENER_CATALOGO_DEVOLUCIONES_AUTOMATICAS = "usp_LecturaENTSPEI_ObtenerCatalogoDevolucionesAutomaticas";
            public const string USP_LECTURA_ENT_SPEI_OBTENER_CUENTA_VOSTRO = "usp_LecturaENTSPEI_ObtenerCuentaVostro";
            public const string USP_LECTURA_ENT_SPEI_ACTUALIZAR_PLAN_DEVOLUCION = "usp_LecturaENTSPEI_ActualizarPlanDevolucion";
            public const string SP_INTERFACE_TOMI_SPEI_PARTICIPANTES = "sp_InterfaceTOMI_SPEI_Participantes";
            public const string SP_INTERFACE_TOMI_SPEI_ESTATUS_RECIBIDAS = "sp_InterfaceTOMI_SPEI_Estatus_Recibidas";
            public const string SP_INTERFACE_TOMI_SPEI_CAMBIA_ESTATUS_RECIBIDAS = "sp_InterfaceTOMI_SPEI_Cambia_Estatus_Recibidas";
            public const string SP_INTERFACE_TOMI_GET_DATA_DEV = "sp_InterfaceTOMI_get_dataDev";
            public const string SP_INTERFACE_TOMI_SPEI_0 = "sp_InterfaceTOMI_SPEI_0";
        }
/// <summary>
/// Representa Procedimientos ENTSPID.
/// </summary>

        public static class ProcedimientosENTSPID
        {
            public const string USP_LECTURA_ENT_SPID_ACTUALIZAR_DATOS_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaENTSPID_ActualizarDatosPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_ENT_SPID_CONTAR_ENTRADAS_SPID_POR_FOLIO = "usp_LecturaENTSPID_ContarEntradasSpidPorFolio";
            public const string USP_LECTURA_ENT_SPID_MARCAR_ENTRADA_SPID_PROCESADA = "usp_LecturaENTSPID_MarcarEntradaSpidProcesada";
            public const string USP_LECTURA_ENT_SPID_FINALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA_BTMUM = "usp_LecturaENTSPID_FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBTMUM";
            public const string USP_LECTURA_ENT_SPID_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA = "usp_LecturaENTSPID_ContarEntradasSalidasPorReferencia";
            public const string USP_LECTURA_ENT_SPID_FINALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA = "usp_LecturaENTSPID_FinalizarPlanTrabajoMaestroRecibidaNoAbonada";
            public const string USP_LECTURA_ENT_SPID_CANCELAR_PLAN_TRABAJO_MAESTRO_POR_DEVOLUCION = "usp_LecturaENTSPID_CancelarPlanTrabajoMaestroPorDevolucion";
            public const string USP_LECTURA_ENT_SPID_MARCAR_ENTRADA_SPID_CONFIRMADA_PENDIENTE = "usp_LecturaENTSPID_MarcarEntradaSpidConfirmadaPendiente";
            public const string USP_LECTURA_ENT_SPID_OBTENER_CONCILIACION_ENTRADA_SPID_POR_FOLIO = "usp_LecturaENTSPID_ObtenerConciliacionEntradaSpidPorFolio";
            public const string USP_LECTURA_ENT_SPID_OBTENER_CONCILIACION_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaENTSPID_ObtenerConciliacionEntradaSalidaPorReferencia";
            public const string USP_LECTURA_ENT_SPID_OBTENER_ENTRADA_SPID_PARA_ENTRADA_SALIDA = "usp_LecturaENTSPID_ObtenerEntradaSpidParaEntradaSalida";
            public const string USP_TOMBIN_INSERT_FORMATO_SPID = "usp_TOMBIN_INSERT_FORMATO_SPID";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string SP_INTERFACE_TOMI_SPID_PARTICIPANTES = "sp_InterfaceTOMI_SPID_Participantes";
            public const string SP_INTERFACE_TOMI_SPID_ESTATUS_RECIBIDAS = "sp_InterfaceTOMI_SPID_Estatus_Recibidas";
            public const string SP_INTERFACE_TOMI_SPID_CAMBIA_ESTATUS_RECIBIDAS = "sp_InterfaceTOMI_SPID_Cambia_Estatus_Recibidas";
            public const string SP_INTERFACE_TOMI_GET_DATA_DEV = "sp_InterfaceTOMI_get_dataDev";
            public const string SP_INTERFACE_TOMI_SPID_0 = "sp_InterfaceTOMI_SPID_0";
        }
/// <summary>
/// Representa Procedimientos FXD.
/// </summary>

        public static class ProcedimientosFXD
        {
            public const string USP_LECTURA_FXD_OBTENER_FOREX_STATUS_POR_EXTERNAL_REF = "usp_LecturaFXD_ObtenerForexStatusPorExternalRef";
            public const string USP_LECTURA_FXD_ACTUALIZAR_DATOS_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaFXD_ActualizarDatosPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_FXD_CONTAR_REGISTROS_TOMBIN_FXD_POR_FOLIO = "usp_LecturaFXD_ContarRegistrosTombinFxdPorFolio";
            public const string USP_LECTURA_FXD_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA = "usp_LecturaFXD_ContarEntradasSalidasPorReferencia";
            public const string USP_LECTURA_FXD_ACTUALIZAR_TOMBIN_FXD_RECHAZADO_POR_FOREX_CANCELADO = "usp_LecturaFXD_ActualizarTombinFxdRechazadoPorForexCancelado";
            public const string USP_LECTURA_FXD_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_CANCELADO_POR_FOREX = "usp_LecturaFXD_ActualizarPlanTrabajoMaestroCanceladoPorForex";
            public const string USP_LECTURA_FXD_ACTUALIZAR_TOMBIN_FXD_CONFIRMADO_HOY = "usp_LecturaFXD_ActualizarTombinFxdConfirmadoHoy";
            public const string USP_LECTURA_FXD_ACTUALIZAR_TOMBIN_FXD_PENDIENTE_FECHA_FUTURA = "usp_LecturaFXD_ActualizarTombinFxdPendienteFechaFutura";
            public const string USP_LECTURA_FXD_OBTENER_TOMBIN_FXD_PARA_CONCILIACION = "usp_LecturaFXD_ObtenerTombinFxdParaConciliacion";
            public const string USP_LECTURA_FXD_OBTENER_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaFXD_ObtenerEntradaSalidaPorReferencia";
            public const string USP_LECTURA_FXD_OBTENER_TOMBIN_FXD_POR_FOLIO = "usp_LecturaFXD_ObtenerTombinFxdPorFolio";
            public const string USP_TOMBIN_INSERT_FORMATO_FXD = "usp_TOMBIN_INSERT_FORMATO_FXD";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string MUFG_MX2_LECTURA_FXD_OBTENER_FORMATO_FXD_CMS_POR_FOLIO = "MUFG_MX2_LecturaFXDObtenerFormatoFxdCmsPorFolio";
            public const string MUFG_MX2_LECTURA_FXD_ACTUALIZAR_FORMATO_CMS_RECHAZADO_POR_FOREX_CANCELADO = "MUFG_MX2_LecturaFXDActualizarFormatoCmsRechazadoPorForexCancelado";
            public const string MUFG_MX2_LECTURA_FXD_ACTUALIZAR_FORMATO_CMS_ACEPTADO_HOY = "MUFG_MX2_LecturaFXDActualizarFormatoCmsAceptadoHoy";
            public const string MUFG_MX2_LECTURA_FXD_ACTUALIZAR_FORMATO_CMS_PENDIENTE_EJECUCION = "MUFG_MX2_LecturaFXDActualizarFormatoCmsPendienteEjecucion";
        }
/// <summary>
/// Representa Procedimientos FXDGPH.
/// </summary>

        public static class ProcedimientosFXDGPH
        {
            public const string USP_LECTURA_FXD_GPH_OBTENER_DATOS_POR_FOLIO = "usp_LecturaFXDGPH_ObtenerDatosPorFolio";
            public const string USP_LECTURA_FXD_GPH_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaFXDGPH_ActualizarFolioUnico";
            public const string USP_LECTURA_FXD_GPH_ACTUALIZAR_PLAN_TRABAJO_MAESTRO = "usp_LecturaFXDGPH_ActualizarPlanTrabajoMaestro";
        }
/// <summary>
/// Representa Procedimientos FXDH2 H.
/// </summary>

        public static class ProcedimientosFXDH2H
        {
            public const string USP_LECTURA_FXD_H2H_OBTENER_DATOS_POR_FOLIO = "usp_LecturaFXDH2H_ObtenerDatosPorFolio";
            public const string USP_LECTURA_FXD_H2H_ACTUALIZAR_PLAN_TRABAJO_MAESTRO = "usp_LecturaFXDH2H_ActualizarPlanTrabajoMaestro";
            public const string GFD_CONSULTAR_DATOS_FIRMA = "GFD_CONSULTAR_DATOS_FIRMA";
            public const string GFD_ACTUALIZAR_FIRMA_DIGITAL = "GFD_ACTUALIZAR_FIRMA_DIGITAL";
        }
/// <summary>
/// Representa Procedimientos FXDLocal.
/// </summary>

        public static class ProcedimientosFXDLocal
        {
            public const string USP_LECTURA_FXD_LOCAL_ES_PACS008_POR_FOLIO = "usp_LecturaFXDLocal_EsPacs008PorFolio";
            public const string USP_LECTURA_FXD_LOCAL_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaFXDLocal_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_FXD_LOCAL_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaFXDLocal_ActualizarFolioUnico";
            public const string USP_LECTURA_FXD_LOCAL_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaFXDLocal_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_FXD_LOCAL_OBTENER_DATOS_COMISION_POR_FOLIO = "usp_LecturaFXDLocal_ObtenerDatosComisionPorFolio";
            public const string USP_LECTURA_FXD_LOCAL_ACTUALIZAR_DATOS_COMISION = "usp_LecturaFXDLocal_ActualizarDatosComision";
            public const string MUFG_MX2_LECTURA_FXD_LOCAL_ES_BANCO_POR_CUSTOMER_CODE = "MUFG_MX2_LecturaFXDLocalEsBancoPorCustomerCode";
            public const string MUFG_MX2_LECTURA_FXD_LOCAL_OBTENER_COMMISSION_TYPE_T24 = "MUFG_MX2_LecturaFXDLocalObtenerCommissionTypeT24";
        }
/// <summary>
/// Representa Procedimientos FXI.
/// </summary>

        public static class ProcedimientosFXI
        {
            public const string USP_LECTURA_FXI_OBTENER_ESTATUS_FOREX = "usp_LecturaFXI_ObtenerEstatusForex";
            public const string USP_LECTURA_FXI_OBTENER_NOSTRO = "usp_LecturaFXI_ObtenerNostro";
            public const string USP_LECTURA_FXI_ACTUALIZAR_MAESTRO_DATOS_GENERALES = "usp_LecturaFXI_ActualizarMaestroDatosGenerales";
            public const string USP_LECTURA_FXI_EXISTE_FORMATO_FXI = "usp_LecturaFXI_ExisteFormatoFxi";
            public const string USP_LECTURA_FXI_OBTENER_FORMATO_FXI = "usp_LecturaFXI_ObtenerFormatoFxi";
            public const string USP_LECTURA_FXI_EXISTE_ENTRADA_SALIDA = "usp_LecturaFXI_ExisteEntradaSalida";
            public const string USP_LECTURA_FXI_ACTUALIZAR_FORMATO_FXI_RECHAZADO_FOREX = "usp_LecturaFXI_ActualizarFormatoFxiRechazadoForex";
            public const string USP_LECTURA_FXI_CANCELAR_MAESTRO_FOREX = "usp_LecturaFXI_CancelarMaestroForex";
            public const string USP_LECTURA_FXI_ACTUALIZAR_ESTATUS_FORMATO_FXI = "usp_LecturaFXI_ActualizarEstatusFormatoFxi";
            public const string USP_LECTURA_FXI_OBTENER_DATOS_CONCILIACION_FORMATO_FXI = "usp_LecturaFXI_ObtenerDatosConciliacionFormatoFxi";
            public const string USP_LECTURA_FXI_OBTENER_DATOS_CONCILIACION_ENTRADA_SALIDA = "usp_LecturaFXI_ObtenerDatosConciliacionEntradaSalida";
            public const string USP_LECTURA_FXI_ACTUALIZAR_MAESTRO_FECHA_FUTURA = "usp_LecturaFXI_ActualizarMaestroFechaFutura";
            public const string USP_TOMBIN_INSERT_FORMATO_FXI = "usp_TOMBIN_INSERT_FORMATO_FXI";
            public const string USP_TOMBIN_INSERT_FORMATO_FXE = "usp_TOMBIN_INSERT_FORMATO_FXE";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string USP_INSERT_SPEI_REGISTRATION_FOR_COVER = "usp_InsertSpeiRegistrationForCover";
            public const string MUFG_MX2_LECTURA_FXI_OBTENER_FORMATO_POR_FOLIO = "MUFG_MX2_LecturaFXIObtenerFormatoPorFolio";
            public const string MUFG_MX2_LECTURA_FXI_OBTENER_RFC_ORDENANTE = "MUFG_MX2_LecturaFXIObtenerRfcOrdenante";
            public const string MUFG_MX2_LECTURA_FXI_ACTUALIZAR_ESTATUS_FORMATO_RECHAZADO_FOREX = "MUFG_MX2_LecturaFXIActualizarEstatusFormatoRechazadoForex";
            public const string MUFG_MX2_LECTURA_FXI_ACTUALIZAR_ESTATUS_FORMATO_CMS = "MUFG_MX2_LecturaFXIActualizarEstatusFormatoCms";
        }
/// <summary>
/// Representa Procedimientos FXIGPH.
/// </summary>

        public static class ProcedimientosFXIGPH
        {
            public const string USP_LECTURA_FXI_GPH_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaFXIGph_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_FXI_GPH_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaFXIGph_ActualizarFolioUnico";
            public const string USP_LECTURA_FXI_GPH_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaFXIGph_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_FXI_GPH_MARCAR_FECHA_FUTURA = "usp_LecturaFXIGph_MarcarFechaFutura";
            public const string USP_LECTURA_FXI_GPH_INSERTAR_DETALLE_OPERACION_GPH = "usp_LecturaFXIGph_InsertarDetalleOperacionGph";
        }
/// <summary>
/// Representa Procedimientos FXIH2 H.
/// </summary>

        public static class ProcedimientosFXIH2H
        {
            public const string USP_LECTURA_FXI_H2H_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaFXIH2H_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_FXI_H2H_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaFXIH2H_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_FXI_H2H_MARCAR_FECHA_FUTURA = "usp_LecturaFXIH2H_MarcarFechaFutura";
        }
/// <summary>
/// Representa Procedimientos FXILocal.
/// </summary>

        public static class ProcedimientosFXILocal
        {
            public const string USP_LECTURA_FXI_LOCAL_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaFXILocal_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_FXI_LOCAL_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaFXILocal_ActualizarFolioUnico";
            public const string USP_LECTURA_FXI_LOCAL_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaFXILocal_ActualizarPlanTrabajoMaestroLectura";
        }
/// <summary>
/// Representa Procedimientos FXT.
/// </summary>

        public static class ProcedimientosFXT
        {
            public const string USP_LECTURA_FXT_OBTENER_STATUS_FOREX_POR_EXTERNAL_REF = "usp_LecturaFXT_ObtenerStatusForexPorExternalRef";
            public const string USP_LECTURA_FXT_ACTUALIZAR_DATOS_GENERALES_PLAN_TRABAJO = "usp_LecturaFXT_ActualizarDatosGeneralesPlanTrabajo";
            public const string USP_TOMBIN_INSERT_FORMATO_FXT = "usp_TOMBIN_INSERT_FORMATO_FXT";
            public const string USP_LECTURA_FXT_EXISTE_FORMATO_FXT_POR_FOLIO = "usp_LecturaFXT_ExisteFormatoFxtPorFolio";
            public const string USP_LECTURA_FXT_ACTUALIZAR_ESTATUS_FXT_RECHAZADO_FOREX = "usp_LecturaFXT_ActualizarEstatusFxtRechazadoForex";
            public const string USP_LECTURA_FXT_ACTUALIZAR_ESTATUS_FXT_CONFIRMACION = "usp_LecturaFXT_ActualizarEstatusFxtConfirmacion";
            public const string USP_LECTURA_FXT_CANCELAR_PLAN_TRABAJO_POR_FOREX = "usp_LecturaFXT_CancelarPlanTrabajoPorForex";
            public const string USP_LECTURA_FXT_OBTENER_CONCILIACION_FXT_POR_FOLIO = "usp_LecturaFXT_ObtenerConciliacionFxtPorFolio";
            public const string USP_LECTURA_FXT_MARCAR_PLAN_TRABAJO_FECHA_FUTURA = "usp_LecturaFXT_MarcarPlanTrabajoFechaFutura";
            public const string MUFG_MX2_LECTURA_FXT_OBTENER_FORMATO_POR_FOLIO = "MUFG_MX2_LecturaFXTObtenerFormatoPorFolio";
            public const string MUFG_MX2_LECTURA_FXT_ACTUALIZAR_ESTATUS_FORMATO_RECHAZADO = "MUFG_MX2_LecturaFXTActualizarEstatusFormatoRechazado";
            public const string MUFG_MX2_LECTURA_FXT_ACTUALIZAR_ESTATUS_FORMATO_SIMPLE = "MUFG_MX2_LecturaFXTActualizarEstatusFormatoSimple";
        }
/// <summary>
/// Representa Procedimientos FXTGPH.
/// </summary>

        public static class ProcedimientosFXTGPH
        {
            public const string USP_LECTURA_FXT_GPH_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaFXTGph_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_FXT_GPH_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaFXTGph_ActualizarFolioUnico";
            public const string USP_LECTURA_FXT_GPH_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaFXTGph_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_FXT_GPH_MARCAR_FECHA_FUTURA = "usp_LecturaFXTGph_MarcarFechaFutura";
            public const string USP_LECTURA_FXT_GPH_INSERTAR_DETALLE_OPERACION_GPH = "usp_LecturaFXTGph_InsertarDetalleOperacionGph";
        }
/// <summary>
/// Representa Procedimientos FXTH2 H.
/// </summary>

        public static class ProcedimientosFXTH2H
        {
            public const string USP_LECTURA_FXT_H2H_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaFXTH2H_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_FXT_H2H_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaFXTH2H_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_FXT_H2H_MARCAR_FECHA_FUTURA = "usp_LecturaFXTH2H_MarcarFechaFutura";
        }
/// <summary>
/// Representa Procedimientos FXTLocal.
/// </summary>

        public static class ProcedimientosFXTLocal
        {
            public const string USP_LECTURA_FXT_LOCAL_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaFXTLocal_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_FXT_LOCAL_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaFXTLocal_ActualizarFolioUnico";
            public const string USP_LECTURA_FXT_LOCAL_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaFXTLocal_ActualizarPlanTrabajoMaestroLectura";
        }
/// <summary>
/// Representa Procedimientos Mercado Dinero.
/// </summary>

        public static class ProcedimientosMercadoDinero
        {
            public const string USP_LECTURA_MD_OBTENER_LIQUIDACION_POR_FOLIO = "usp_LecturaMD_ObtenerLiquidacionPorFolio";
            public const string USP_LECTURA_MD_OBTENER_COMPLEMENTO_PAGO_POR_TIPO_LIQUIDACION_Y_CDL = "usp_LecturaMD_ObtenerComplementoPagoPorTipoLiquidacionYCdl";
            public const string USP_LECTURA_MD_OBTENER_DUPLICIDADES_LIQUIDACION_POR_FOLIO = "usp_LecturaMD_ObtenerDuplicidadesLiquidacionPorFolio";
            public const string USP_LECTURA_MD_OBTENER_ESTATUS_PAR_POR_FOLIO_SALIDA = "usp_LecturaMD_ObtenerEstatusParPorFolioSalida";
            public const string USP_LECTURA_MD_ACTUALIZAR_DATOS_COMPLEMENTARIOS_EN_TABLA_TRABAJO = "usp_LecturaMD_ActualizarDatosComplementariosEnTablaTrabajo";
            public const string USP_LECTURA_MD_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_Y_FOLIO_UNICO = "usp_LecturaMD_ActualizarPlanTrabajoMaestroYFolioUnico";
            public const string USP_LECTURA_MD_ACTUALIZAR_ESTATUS_CALYPSO_EN_PROCESO = "usp_LecturaMD_ActualizarEstatusCalypsoEnProceso";
            public const string USP_LECTURA_MD_MARCAR_PLAN_TRABAJO_ESPERANDO_AUTORIZACION_USUARIO = "usp_LecturaMD_MarcarPlanTrabajoEsperandoAutorizacionUsuario";
            public const string USP_LECTURA_MD_MARCAR_PLAN_TRABAJO_VALIDANDO_INFO = "usp_LecturaMD_MarcarPlanTrabajoValidandoInfo";
            public const string USP_LECTURA_MD_ACTUALIZAR_ESTATUS_CALYPSO_CONP = "usp_LecturaMD_ActualizarEstatusCalypsoConp";
            public const string USP_LECTURA_MD_INSERTAR_SWIFT_OUT = "usp_LecturaMD_InsertarSwiftOut";
        }
/// <summary>
/// Representa Procedimientos MFX.
/// </summary>

        public static class ProcedimientosMFX
        {
            public const string USP_LECTURA_MFX_OBTENER_SOLICITUD_FOREX_POR_FOLIO = "usp_LecturaMFX_ObtenerSolicitudForexPorFolio";
            public const string USP_LECTURA_MFX_ACTUALIZAR_MAESTRO_DATOS_GRAL = "usp_LecturaMFX_ActualizarMaestroDatosGral";
            public const string USP_LECTURA_MFX_CANCELAR_MAESTRO_POR_FOREX_CANCELADO_BTMUM_DESDE_LECTURA = "usp_LecturaMFX_CancelarMaestroPorForexCanceladoBTMUMDesdeLectura";
            public const string USP_LECTURA_MFX_MARCAR_MAESTRO_ENVIO_CMS = "usp_LecturaMFX_MarcarMaestroEnvioCms";
            public const string USP_LECTURA_MFX_MARCAR_MAESTRO_ESPERANDO_CONFIRMACION = "usp_LecturaMFX_MarcarMaestroEsperandoConfirmacion";
            public const string USP_LECTURA_MFX_CANCELAR_MAESTRO_POR_FOREX_CANCELADO_BTMUM_DESDE_ESPERANDO_CONFIRMACION = "usp_LecturaMFX_CancelarMaestroPorForexCanceladoBTMUMDesdeEsperandoConfirmacion";
            public const string USP_LECTURA_MFX_MARCAR_MAESTRO_LECTURA_CON_FOLIO_CMS = "usp_LecturaMFX_MarcarMaestroLecturaConFolioCms";
            public const string USP_LECTURA_MFX_CONTAR_TOMBIN_MFX_POR_FOLIO = "usp_LecturaMFX_ContarTombinMfxPorFolio";
            public const string USP_LECTURA_MFX_ACTUALIZAR_SOLICITUD_FOREX_VALIDACION_SISTEMA = "usp_LecturaMFX_ActualizarSolicitudForexValidacionSistema";
            public const string USP_LECTURA_MFX_MARCAR_MAESTRO_ESPERANDO_AUTORIZACION_TESORERIA = "usp_LecturaMFX_MarcarMaestroEsperandoAutorizacionTesoreria";
            public const string USP_LECTURA_MFX_CANCELAR_MAESTRO_POR_FOREX_CANCELADO_BTMUM_DESDE_TESORERIA = "usp_LecturaMFX_CancelarMaestroPorForexCanceladoBTMUMDesdeTesoreria";
            public const string USP_LECTURA_MFX_ACTUALIZAR_TOMBIN_MFX_RECHAZADO = "usp_LecturaMFX_ActualizarTombinMfxRechazado";
            public const string USP_LECTURA_MFX_CANCELAR_MAESTRO_POR_FOREX_CANCELADO_CMS = "usp_LecturaMFX_CancelarMaestroPorForexCanceladoCMS";
            public const string USP_LECTURA_MFX_ACTUALIZAR_TOMBIN_MFX_CONFIRMADO = "usp_LecturaMFX_ActualizarTombinMfxConfirmado";
            public const string USP_LECTURA_MFX_OBTENER_DATOS_FOREX_PARA_CONCILIACION = "usp_LecturaMFX_ObtenerDatosForexParaConciliacion";
            public const string USP_LECTURA_MFX_OBTENER_DATOS_TOMI_PARA_CONCILIACION = "usp_LecturaMFX_ObtenerDatosTomiParaConciliacion";
            public const string USP_LECTURA_MFX_MARCAR_SOLICITUD_FOREX_PROCESADA = "usp_LecturaMFX_MarcarSolicitudForexProcesada";
            public const string USP_TOMBIN_INSERT_FORMATO_MFX = "usp_TOMBIN_INSERT_FORMATO_MFX";
            public const string MUFG_MX2_LECTURA_MFX_OBTENER_FORMATOS_FOREX_CONFIRMADOS = "MUFG_MX2_LecturaMFXObtenerFormatosForexConfirmados";
            public const string MUFG_MX2_LECTURA_MFX_RECHAZAR_FORMATO_FOREX = "MUFG_MX2_LecturaMFXRechazarFormatoForex";
            public const string MUFG_MX2_LECTURA_MFX_ACEPTAR_FORMATO_FOREX = "MUFG_MX2_LecturaMFXAceptarFormatoForex";
            public const string MUFG_MX2_LECTURA_MFX_INSERTAR_AUTORIZACION_FOREX_DESDE_TOMI = "MUFG_MX2_LecturaMFXInsertarAutorizacionForexDesdeTomi";
            public const string MUFG_MX2_LECTURA_MFX_CANCELAR_AUTORIZACION_FOREX = "MUFG_MX2_LecturaMFXCancelarAutorizacionForex";
            public const string MUFG_MX2_LECTURA_MFX_OBTENER_CUENTA_CUSTOMER = "MUFG_MX2_LecturaMFXObtenerCuentaCustomer";
        }
/// <summary>
/// Representa Procedimientos NOMFXD.
/// </summary>

        public static class ProcedimientosNOMFXD
        {
            public const string USP_LECTURA_NOM_FXD_OBTENER_STATUS_FOREX_POR_EXTERNAL_REF = "usp_LecturaNOMFXD_ObtenerStatusForexPorExternalRef";
            public const string USP_LECTURA_NOM_FXD_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaNOMFXD_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_TOMBIN_INSERT_FORMATO_FXD_NOMINA_DETALLE = "usp_TOMBIN_INSERT_FORMATO_FXD_NOMINA_DETALLE";
            public const string USP_LECTURA_NOM_FXD_CONTAR_DETALLE_NOMINA_POR_FOLIO = "usp_LecturaNOMFXD_ContarDetalleNominaPorFolio";
            public const string USP_LECTURA_NOM_FXD_OBTENER_SUMARIZADO_DETALLE_POR_MAESTRO = "usp_LecturaNOMFXD_ObtenerSumarizadoDetallePorMaestro";
            public const string USP_LECTURA_NOM_FXD_CONTAR_DETALLE_NOMINA_PROCESADO_POR_FOLIO = "usp_LecturaNOMFXD_ContarDetalleNominaProcesadoPorFolio";
            public const string USP_LECTURA_NOM_FXD_CONTAR_MAESTRO_NOMINA_POR_FOLIO = "usp_LecturaNOMFXD_ContarMaestroNominaPorFolio";
            public const string USP_LECTURA_NOM_FXD_MARCAR_NOMINA_TOMI_RECHAZADA_POR_FOREX_CANCELADO = "usp_LecturaNOMFXD_MarcarNominaTomiRechazadaPorForexCancelado";
            public const string USP_LECTURA_NOM_FXD_CANCELAR_PLAN_TRABAJO_POR_FOREX_CANCELADO_DETALLE = "usp_LecturaNOMFXD_CancelarPlanTrabajoPorForexCanceladoDetalle";
            public const string USP_LECTURA_NOM_FXD_CANCELAR_PLAN_TRABAJO_POR_FOREX_CANCELADO_MAESTRO = "usp_LecturaNOMFXD_CancelarPlanTrabajoPorForexCanceladoMaestro";
            public const string USP_LECTURA_NOM_FXD_MARCAR_NOMINA_TOMI_ACEPTADA_HOY = "usp_LecturaNOMFXD_MarcarNominaTomiAceptadaHoy";
            public const string USP_LECTURA_NOM_FXD_MARCAR_NOMINA_TOMI_FECHA_FUTURA = "usp_LecturaNOMFXD_MarcarNominaTomiFechaFutura";
            public const string USP_LECTURA_NOM_FXD_MARCAR_PROCESOS_DETALLE_TERMINADOS = "usp_LecturaNOMFXD_MarcarProcesosDetalleTerminados";
            public const string USP_LECTURA_NOM_FXD_CONTAR_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaNOMFXD_ContarEntradaSalidaPorReferencia";
            public const string USP_LECTURA_NOM_FXD_OBTENER_DETALLE_CONCILIACION_POR_FOLIO = "usp_LecturaNOMFXD_ObtenerDetalleConciliacionPorFolio";
            public const string USP_LECTURA_NOM_FXD_OBTENER_SUMARIZADO_DETALLE_PROCESADO_POR_MAESTRO = "usp_LecturaNOMFXD_ObtenerSumarizadoDetalleProcesadoPorMaestro";
            public const string USP_LECTURA_NOM_FXD_OBTENER_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaNOMFXD_ObtenerEntradaSalidaPorReferencia";
            public const string USP_TOMBIN_INSERT_FORMATO_FXD_NOMINA_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_FXD_NOMINA_MAESTRO";
            public const string USP_LECTURA_NOM_FXD_OBTENER_NOMINA_MAESTRO_PARA_ENTRADA_SALIDA = "usp_LecturaNOMFXD_ObtenerNominaMaestroParaEntradaSalida";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string MUFG_MX2_LECTURA_NOM_FXD_OBTENER_MAESTRO_NOMINA_POR_FOLIO = "MUFG_MX2_LecturaNOMFXDObtenerMaestroNominaPorFolio";
            public const string MUFG_MX2_LECTURA_NOM_FXD_OBTENER_DETALLE_NOMINA_POR_FOLIO = "MUFG_MX2_LecturaNOMFXDObtenerDetalleNominaPorFolio";
            public const string MUFG_MX2_LECTURA_NOM_FXD_MARCAR_NOMINA_RECHAZADA_POR_FOREX_CANCELADO = "MUFG_MX2_LecturaNOMFXDMarcarNominaRechazadaPorForexCancelado";
            public const string MUFG_MX2_LECTURA_NOM_FXD_MARCAR_NOMINA_ACEPTADA = "MUFG_MX2_LecturaNOMFXDMarcarNominaAceptada";
            public const string MUFG_MX2_LECTURA_NOM_FXD_MARCAR_NOMINA_RECIBIDA_PENDIENTE_EJECUCION = "MUFG_MX2_LecturaNOMFXDMarcarNominaRecibidaPendienteEjecucion";
        }
/// <summary>
/// Representa Procedimientos Pa PFXD.
/// </summary>

        public static class ProcedimientosPaPFXD
        {
            public const string USP_LECTURA_PAP_FXD_OBTENER_FOREX_STATUS_POR_EXTERNAL_REF = "usp_LecturaPaPFXD_ObtenerForexStatusPorExternalRef";
            public const string USP_LECTURA_PAP_FXD_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaPaPFXD_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_PAP_FXD_CONTAR_REGISTROS_DETALLE_POR_FOLIO = "usp_LecturaPaPFXD_ContarRegistrosDetallePorFolio";
            public const string USP_LECTURA_PAP_FXD_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA = "usp_LecturaPaPFXD_ContarEntradasSalidasPorReferencia";
            public const string USP_LECTURA_PAP_FXD_OBTENER_SUMATORIA_DETALLE = "usp_LecturaPaPFXD_ObtenerSumatoriaDetalle";
            public const string USP_LECTURA_PAP_FXD_CONTAR_DETALLES_CON_ESTATUS_DISTINTO_PEND = "usp_LecturaPaPFXD_ContarDetallesConEstatusDistintoPend";
            public const string USP_LECTURA_PAP_FXD_CONTAR_REGISTROS_MAESTRO_POR_FOLIO = "usp_LecturaPaPFXD_ContarRegistrosMaestroPorFolio";
            public const string USP_LECTURA_PAP_FXD_ACTUALIZAR_TOMI_RECHAZADO_POR_FOREX_CANCELADO = "usp_LecturaPaPFXD_ActualizarTomiRechazadoPorForexCancelado";
            public const string USP_LECTURA_PAP_FXD_ACTUALIZAR_PLAN_TRABAJO_DETALLE_CANCELADO_POR_FOREX = "usp_LecturaPaPFXD_ActualizarPlanTrabajoDetalleCanceladoPorForex";
            public const string USP_LECTURA_PAP_FXD_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_CANCELADO_POR_FOREX = "usp_LecturaPaPFXD_ActualizarPlanTrabajoMaestroCanceladoPorForex";
            public const string USP_LECTURA_PAP_FXD_ACTUALIZAR_TOMI_ACEPTADO_HOY = "usp_LecturaPaPFXD_ActualizarTomiAceptadoHoy";
            public const string USP_LECTURA_PAP_FXD_ACTUALIZAR_TOMI_PENDIENTE_EJECUCION = "usp_LecturaPaPFXD_ActualizarTomiPendienteEjecucion";
            public const string USP_LECTURA_PAP_FXD_OBTENER_DETALLE_PROCESADO_POR_FOLIO = "usp_LecturaPaPFXD_ObtenerDetalleProcesadoPorFolio";
            public const string USP_LECTURA_PAP_FXD_OBTENER_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaPaPFXD_ObtenerEntradaSalidaPorReferencia";
            public const string USP_LECTURA_PAP_FXD_OBTENER_DETALLE_TOMI_POR_FOLIO = "usp_LecturaPaPFXD_ObtenerDetalleTomiPorFolio";
            public const string USP_TOMBIN_INSERT_FORMATO_FXD_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_FXD_PAP_DETALLE";
            public const string USP_TOMBIN_INSERT_FORMATO_FXD_PAP_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_FXD_PAP_MAESTRO";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string MUFG_MX2_LECTURA_PAP_FXD_OBTENER_MAESTRO_POR_DETALLE = "MUFG_MX2_LecturaPaPFXDObtenerMaestroPorDetalle";
            public const string MUFG_MX2_LECTURA_PAP_FXD_OBTENER_DETALLE_POR_FOLIO = "MUFG_MX2_LecturaPaPFXDObtenerDetallePorFolio";
            public const string MUFG_MX2_LECTURA_PAP_FXD_ACTUALIZAR_CMS_RECHAZADO_POR_FOREX_CANCELADO = "MUFG_MX2_LecturaPaPFXDActualizarCmsRechazadoPorForexCancelado";
            public const string MUFG_MX2_LECTURA_PAP_FXD_ACTUALIZAR_CMS_ACEPTADO_HOY = "MUFG_MX2_LecturaPaPFXDActualizarCmsAceptadoHoy";
            public const string MUFG_MX2_LECTURA_PAP_FXD_ACTUALIZAR_CMS_PENDIENTE_EJECUCION = "MUFG_MX2_LecturaPaPFXDActualizarCmsPendienteEjecucion";
        }
/// <summary>
/// Representa Procedimientos Pa PFXI.
/// </summary>

        public static class ProcedimientosPaPFXI
        {
            public const string USP_LECTURA_PAP_FXI_OBTENER_STATUS_FOREX_POR_EXTERNAL_REF = "usp_LecturaPaPFXI_ObtenerStatusForexPorExternalRef";
            public const string USP_LECTURA_PAP_FXI_ACTUALIZAR_PLAN_TRABAJO_DATOS_GRAL = "usp_LecturaPaPFXI_ActualizarPlanTrabajoDatosGral";
            public const string USP_LECTURA_PAP_FXI_CONTAR_DETALLE_PAP_TOMI_POR_FOLIO = "usp_LecturaPaPFXI_ContarDetallePapTomiPorFolio";
            public const string USP_LECTURA_PAP_FXI_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA = "usp_LecturaPaPFXI_ContarEntradasSalidasPorReferencia";
            public const string USP_LECTURA_PAP_FXI_OBTENER_SUMARIZADO_DETALLE_POR_MAESTRO = "usp_LecturaPaPFXI_ObtenerSumarizadoDetallePorMaestro";
            public const string USP_LECTURA_PAP_FXI_CONTAR_DETALLE_PAP_TOMI_NO_PENDIENTE_POR_FOLIO = "usp_LecturaPaPFXI_ContarDetallePapTomiNoPendientePorFolio";
            public const string USP_LECTURA_PAP_FXI_CONTAR_MAESTRO_PAP_TOMI_POR_FOLIO = "usp_LecturaPaPFXI_ContarMaestroPapTomiPorFolio";
            public const string USP_LECTURA_PAP_FXI_MARCAR_RECHAZO_FOREX_CANCELADO_TOMI = "usp_LecturaPaPFXI_MarcarRechazoForexCanceladoTomi";
            public const string USP_LECTURA_PAP_FXI_CANCELAR_PLAN_TRABAJO_DETALLE_POR_FOREX = "usp_LecturaPaPFXI_CancelarPlanTrabajoDetallePorForex";
            public const string USP_LECTURA_PAP_FXI_CANCELAR_PLAN_TRABAJO_MAESTRO_POR_FOREX = "usp_LecturaPaPFXI_CancelarPlanTrabajoMaestroPorForex";
            public const string USP_LECTURA_PAP_FXI_MARCAR_ACEPTADO_HOY_TOMI = "usp_LecturaPaPFXI_MarcarAceptadoHoyTomi";
            public const string USP_LECTURA_PAP_FXI_MARCAR_FECHA_FUTURA_TOMI = "usp_LecturaPaPFXI_MarcarFechaFuturaTomi";
            public const string USP_LECTURA_PAP_FXI_OBTENER_DETALLE_TOMI_PARA_CONCILIACION = "usp_LecturaPaPFXI_ObtenerDetalleTomiParaConciliacion";
            public const string USP_LECTURA_PAP_FXI_OBTENER_SUMARIZADO_DETALLE_NO_PENDIENTE_POR_MAESTRO = "usp_LecturaPaPFXI_ObtenerSumarizadoDetalleNoPendientePorMaestro";
            public const string USP_LECTURA_PAP_FXI_OBTENER_ENTRADA_SALIDA_PARA_CONCILIACION = "usp_LecturaPaPFXI_ObtenerEntradaSalidaParaConciliacion";
            public const string USP_LECTURA_PAP_FXI_MARCAR_PLAN_TRABAJO_DETALLE_FECHA_FUTURA = "usp_LecturaPaPFXI_MarcarPlanTrabajoDetalleFechaFutura";
            public const string USP_LECTURA_PAP_FXI_MARCAR_PLAN_TRABAJO_MAESTRO_FECHA_FUTURA = "usp_LecturaPaPFXI_MarcarPlanTrabajoMaestroFechaFutura";
            public const string USP_TOMBIN_INSERT_FORMATO_FXI_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_FXI_PAP_DETALLE";
            public const string USP_TOMBIN_INSERT_FORMATO_FXI_PAP_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_FXI_PAP_MAESTRO";
            public const string USP_LECTURA_PAP_FXI_OBTENER_DETALLE_TOMI_PARA_ENTRADA_SALIDA = "usp_LecturaPaPFXI_ObtenerDetalleTomiParaEntradaSalida";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string MUFG_MX2_LECTURA_PAP_FXI_OBTENER_MAESTRO_CMS_POR_FOLIO_DETALLE = "MUFG_MX2_LecturaPaPFXIObtenerMaestroCmsPorFolioDetalle";
            public const string MUFG_MX2_LECTURA_PAP_FXI_OBTENER_DETALLE_CMS_POR_FOLIO = "MUFG_MX2_LecturaPaPFXIObtenerDetalleCmsPorFolio";
            public const string MUFG_MX2_LECTURA_PAP_FXI_MARCAR_RECHAZO_FOREX_CANCELADO_CMS = "MUFG_MX2_LecturaPaPFXIMarcarRechazoForexCanceladoCms";
            public const string MUFG_MX2_LECTURA_PAP_FXI_MARCAR_ACEPTADO_HOY_CMS = "MUFG_MX2_LecturaPaPFXIMarcarAceptadoHoyCms";
            public const string MUFG_MX2_LECTURA_PAP_FXI_MARCAR_FECHA_FUTURA_CMS = "MUFG_MX2_LecturaPaPFXIMarcarFechaFuturaCms";
        }
/// <summary>
/// Representa Procedimientos Pa PFXT.
/// </summary>

        public static class ProcedimientosPaPFXT
        {
            public const string USP_LECTURA_PAP_FXT_OBTENER_ESTATUS_FOREX_POR_REFERENCIA = "usp_LecturaPaPFXT_ObtenerEstatusForexPorReferencia";
            public const string USP_LECTURA_PAP_FXT_ACTUALIZAR_DATOS_GENERALES_PLAN_TRABAJO = "usp_LecturaPaPFXT_ActualizarDatosGeneralesPlanTrabajo";
            public const string USP_LECTURA_PAP_FXT_EXISTE_FORMATO_PAP_DETALLE_POR_FOLIO = "usp_LecturaPaPFXT_ExisteFormatoPapDetallePorFolio";
            public const string USP_LECTURA_PAP_FXT_OBTENER_SUMARIZADO_DETALLE_POR_ID_FORMATO_PAP = "usp_LecturaPaPFXT_ObtenerSumarizadoDetallePorIdFormatoPap";
            public const string USP_LECTURA_PAP_FXT_EXISTE_FORMATO_PAP_DETALLE_PROCESADO_POR_FOLIO = "usp_LecturaPaPFXT_ExisteFormatoPapDetalleProcesadoPorFolio";
            public const string USP_LECTURA_PAP_FXT_EXISTE_FORMATO_PAP_MAESTRO_POR_FOLIO = "usp_LecturaPaPFXT_ExisteFormatoPapMaestroPorFolio";
            public const string USP_LECTURA_PAP_FXT_ACTUALIZAR_ESTATUS_FORMATO_PAP_TOMI = "usp_LecturaPaPFXT_ActualizarEstatusFormatoPapTomi";
            public const string USP_LECTURA_PAP_FXT_CANCELAR_PLAN_TRABAJO_POR_FOREX = "usp_LecturaPaPFXT_CancelarPlanTrabajoPorForex";
            public const string USP_LECTURA_PAP_FXT_OBTENER_IMPORTE_DETALLE_CONCILIACION_POR_FOLIO = "usp_LecturaPaPFXT_ObtenerImporteDetalleConciliacionPorFolio";
            public const string USP_LECTURA_PAP_FXT_OBTENER_SUMARIZADO_DETALLE_PROCESADO_POR_ID_FORMATO_PAP = "usp_LecturaPaPFXT_ObtenerSumarizadoDetalleProcesadoPorIdFormatoPap";
            public const string USP_LECTURA_PAP_FXT_MARCAR_PLAN_TRABAJO_FECHA_FUTURA = "usp_LecturaPaPFXT_MarcarPlanTrabajoFechaFutura";
            public const string USP_TOMBIN_INSERT_FORMATO_FXT_PAP_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_FXT_PAP_MAESTRO";
            public const string USP_TOMBIN_INSERT_FORMATO_FXT_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_FXT_PAP_DETALLE";
            public const string MUFG_MX2_LECTURA_PAP_FXT_OBTENER_FORMATO_PAP_MAESTRO_POR_FOLIO = "MUFG_MX2_LecturaPaPFXTObtenerFormatoPapMaestroPorFolio";
            public const string MUFG_MX2_LECTURA_PAP_FXT_OBTENER_FORMATO_PAP_DETALLE_POR_FOLIO = "MUFG_MX2_LecturaPaPFXTObtenerFormatoPapDetallePorFolio";
            public const string MUFG_MX2_LECTURA_PAP_FXT_ACTUALIZAR_ESTATUS_FORMATO_PAP = "MUFG_MX2_LecturaPaPFXTActualizarEstatusFormatoPap";
            public const string MUFG_MX2_LECTURA_PAP_FXT_ACTUALIZAR_ESTATUS_FORMATO_PAP_SIN_RAZON = "MUFG_MX2_LecturaPaPFXTActualizarEstatusFormatoPapSinRazon";
        }
/// <summary>
/// Representa Procedimientos Pa PSPID.
/// </summary>

        public static class ProcedimientosPaPSPID
        {
            public const string USP_LECTURA_PAP_SPID_OBTENER_ESTATUS_FOREX = "usp_LecturaPaP_SPID_ObtenerEstatusForex";
            public const string USP_LECTURA_PAP_SPID_ACTUALIZAR_DATOS_GENERALES_PLAN_TRABAJO = "usp_LecturaPaP_SPID_ActualizarDatosGeneralesPlanTrabajo";
            public const string USP_LECTURA_PAP_SPID_CONTAR_DETALLE_POR_FOLIO = "usp_LecturaPaP_SPID_ContarDetallePorFolio";
            public const string USP_LECTURA_PAP_SPID_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA = "usp_LecturaPaP_SPID_ContarEntradasSalidasPorReferencia";
            public const string USP_TOMBIN_INSERT_FORMATO_SPID_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_SPID_PAP_DETALLE";
            public const string USP_LECTURA_PAP_SPID_OBTENER_SUMARIZADO_DETALLE = "usp_LecturaPaP_SPID_ObtenerSumarizadoDetalle";
            public const string USP_LECTURA_PAP_SPID_CONTAR_DETALLE_NO_PENDIENTE_POR_FOLIO = "usp_LecturaPaP_SPID_ContarDetalleNoPendientePorFolio";
            public const string USP_TOMBIN_INSERT_FORMATO_SPID_PAP_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_SPID_PAP_MAESTRO";
            public const string USP_LECTURA_PAP_SPID_CONTAR_MAESTRO_POR_FOLIO = "usp_LecturaPaP_SPID_ContarMaestroPorFolio";
            public const string USP_LECTURA_PAP_SPID_ACTUALIZAR_ESTATUS_TOMI_RECHAZADO_FOREX = "usp_LecturaPaP_SPID_ActualizarEstatusTomiRechazadoForex";
            public const string USP_LECTURA_PAP_SPID_CANCELAR_PLAN_TRABAJO_POR_FOREX = "usp_LecturaPaP_SPID_CancelarPlanTrabajoPorForex";
            public const string USP_LECTURA_PAP_SPID_ACTUALIZAR_ESTATUS_ACEPTADO_HOY = "usp_LecturaPaP_SPID_ActualizarEstatusAceptadoHoy";
            public const string USP_LECTURA_PAP_SPID_ACTUALIZAR_ESTATUS_FECHA_FUTURA = "usp_LecturaPaP_SPID_ActualizarEstatusFechaFutura";
            public const string USP_LECTURA_PAP_SPID_OBTENER_DETALLE_CONCILIACION = "usp_LecturaPaP_SPID_ObtenerDetalleConciliacion";
            public const string USP_LECTURA_PAP_SPID_OBTENER_SUMARIZADO_DETALLE_NO_PENDIENTE = "usp_LecturaPaP_SPID_ObtenerSumarizadoDetalleNoPendiente";
            public const string USP_LECTURA_PAP_SPID_OBTENER_ENTRADA_SALIDA_CONCILIACION = "usp_LecturaPaP_SPID_ObtenerEntradaSalidaConciliacion";
            public const string USP_LECTURA_PAP_SPID_ACTUALIZAR_PLAN_TRABAJO_FECHA_FUTURA = "usp_LecturaPaP_SPID_ActualizarPlanTrabajoFechaFutura";
            public const string USP_LECTURA_PAP_SPID_OBTENER_DETALLES_PARA_ENTRADA_SALIDA = "usp_LecturaPaP_SPID_ObtenerDetallesParaEntradaSalida";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string MUFG_MX2_LECTURA_PAP_SPID_OBTENER_MAESTRO_POR_FOLIO_DETALLE = "MUFG_MX2_LecturaPaPSPIDObtenerMaestroPorFolioDetalle";
            public const string MUFG_MX2_LECTURA_PAP_SPID_OBTENER_DETALLE_POR_FOLIO = "MUFG_MX2_LecturaPaPSPIDObtenerDetallePorFolio";
            public const string MUFG_MX2_LECTURA_PAP_SPID_ACTUALIZAR_ESTATUS_CMS_RECHAZADO_FOREX = "MUFG_MX2_LecturaPaPSPIDActualizarEstatusCmsRechazadoForex";
            public const string MUFG_MX2_LECTURA_PAP_SPID_ACTUALIZAR_ESTATUS_CMS_ACEPTADO = "MUFG_MX2_LecturaPaPSPIDActualizarEstatusCmsAceptado";
            public const string MUFG_MX2_LECTURA_PAP_SPID_ACTUALIZAR_ESTATUS_CMS_PENDIENTE_EJECUCION = "MUFG_MX2_LecturaPaPSPIDActualizarEstatusCmsPendienteEjecucion";
            public const string MUFG_MX2_LIST_CUSTOMER_EDO_CTA_T24 = "MUFG_MX2_ListCUSTOMEREdoCtaT24";
        }
/// <summary>
/// Representa Procedimientos Pa PTEF.
/// </summary>

        public static class ProcedimientosPaPTEF
        {
            public const string USP_LECTURA_PAP_TEF_OBTENER_ESTATUS_FOREX = "usp_LecturaPaPTEF_ObtenerEstatusForex";
            public const string USP_LECTURA_PAP_TEF_ACTUALIZAR_DATOS_GENERALES_PLAN_TRABAJO = "usp_LecturaPaPTEF_ActualizarDatosGeneralesPlanTrabajo";
            public const string USP_LECTURA_PAP_TEF_CONTAR_DETALLE_PAP_TOMI_POR_FOLIO = "usp_LecturaPaPTEF_ContarDetallePapTomiPorFolio";
            public const string USP_LECTURA_PAP_TEF_CONTAR_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaPaPTEF_ContarEntradaSalidaPorReferencia";
            public const string USP_LECTURA_PAP_TEF_OBTENER_SUMARIZADO_DETALLE_POR_MAESTRO = "usp_LecturaPaPTEF_ObtenerSumarizadoDetallePorMaestro";
            public const string USP_LECTURA_PAP_TEF_CONTAR_DETALLE_PAP_TOMI_PROCESADO_POR_FOLIO = "usp_LecturaPaPTEF_ContarDetallePapTomiProcesadoPorFolio";
            public const string USP_LECTURA_PAP_TEF_CONTAR_MAESTRO_PAP_TOMI_POR_FOLIO = "usp_LecturaPaPTEF_ContarMaestroPapTomiPorFolio";
            public const string USP_LECTURA_PAP_TEF_ACTUALIZAR_TOMI_RECHAZADO_POR_FOREX_CANCELADO = "usp_LecturaPaPTEF_ActualizarTomiRechazadoPorForexCancelado";
            public const string USP_LECTURA_PAP_TEF_CANCELAR_PLAN_TRABAJO_DETALLE_POR_FOREX = "usp_LecturaPaPTEF_CancelarPlanTrabajoDetallePorForex";
            public const string USP_LECTURA_PAP_TEF_CANCELAR_PLAN_TRABAJO_MAESTRO_POR_FOREX = "usp_LecturaPaPTEF_CancelarPlanTrabajoMaestroPorForex";
            public const string USP_LECTURA_PAP_TEF_ACTUALIZAR_TOMI_ACEPTADO_HOY = "usp_LecturaPaPTEF_ActualizarTomiAceptadoHoy";
            public const string USP_LECTURA_PAP_TEF_ACTUALIZAR_TOMI_FECHA_FUTURA = "usp_LecturaPaPTEF_ActualizarTomiFechaFutura";
            public const string USP_LECTURA_PAP_TEF_OBTENER_DETALLE_TOMI_PARA_CONCILIACION = "usp_LecturaPaPTEF_ObtenerDetalleTomiParaConciliacion";
            public const string USP_LECTURA_PAP_TEF_OBTENER_SUMARIZADO_DETALLE_PROCESADO_POR_MAESTRO = "usp_LecturaPaPTEF_ObtenerSumarizadoDetalleProcesadoPorMaestro";
            public const string USP_LECTURA_PAP_TEF_OBTENER_ENTRADA_SALIDA_PARA_CONCILIACION = "usp_LecturaPaPTEF_ObtenerEntradaSalidaParaConciliacion";
            public const string USP_TOMBIN_INSERT_FORMATO_TEF_PAP_DETALLE = "usp_TOMBIN_INSERT_FORMATO_TEF_PAP_DETALLE";
            public const string USP_TOMBIN_INSERT_FORMATO_TEF_PAP_MAESTRO = "usp_TOMBIN_INSERT_FORMATO_TEF_PAP_MAESTRO";
            public const string USP_LECTURA_PAP_TEF_OBTENER_DETALLE_TOMI_PARA_ENTRADA_SALIDA = "usp_LecturaPaPTEF_ObtenerDetalleTomiParaEntradaSalida";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string MUFG_MX2_LECTURA_PAP_TEF_OBTENER_MAESTRO_POR_DETALLE = "MUFG_MX2_LecturaPaPTEFObtenerMaestroPorDetalle";
            public const string MUFG_MX2_LECTURA_PAP_TEF_OBTENER_DETALLE_POR_FOLIO = "MUFG_MX2_LecturaPaPTEFObtenerDetallePorFolio";
            public const string MUFG_MX2_LECTURA_PAP_TEF_ACTUALIZAR_CMS_RECHAZADO_POR_FOREX_CANCELADO = "MUFG_MX2_LecturaPaPTEFActualizarCmsRechazadoPorForexCancelado";
            public const string MUFG_MX2_LECTURA_PAP_TEF_ACTUALIZAR_CMS_ACEPTADO_HOY = "MUFG_MX2_LecturaPaPTEFActualizarCmsAceptadoHoy";
            public const string MUFG_MX2_LECTURA_PAP_TEF_ACTUALIZAR_CMS_PENDIENTE_EJECUCION = "MUFG_MX2_LecturaPaPTEFActualizarCmsPendienteEjecucion";
        }
/// <summary>
/// Representa Procedimientos SER.
/// </summary>

        public static class ProcedimientosSER
        {
            public const string USP_LECTURA_SER_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaSER_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_TOMBIN_INSERT_FORMATO_SER_PAY = "usp_TOMBIN_INSERT_FORMATO_SER_PAY";
            public const string USP_LECTURA_SER_CONTAR_SER_PAY_POR_FOLIO = "usp_LecturaSER_ContarSerPayPorFolio";
            public const string USP_LECTURA_SER_OBTENER_SER_PAY_PARA_ENTRADA_SALIDA = "usp_LecturaSER_ObtenerSerPayParaEntradaSalida";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string USP_LECTURA_SER_CONTAR_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaSER_ContarEntradaSalidaPorReferencia";
            public const string USP_LECTURA_SER_ACTUALIZAR_SER_PAY_ACEPTADO = "usp_LecturaSER_ActualizarSerPayAceptado";
            public const string USP_LECTURA_SER_ACTUALIZAR_SER_PAY_FECHA_FUTURA = "usp_LecturaSER_ActualizarSerPayFechaFutura";
            public const string USP_LECTURA_SER_OBTENER_SER_PAY_CONCILIACION_POR_FOLIO = "usp_LecturaSER_ObtenerSerPayConciliacionPorFolio";
            public const string USP_LECTURA_SER_OBTENER_ENTRADA_SALIDA_CONCILIACION_POR_REFERENCIA = "usp_LecturaSER_ObtenerEntradaSalidaConciliacionPorReferencia";
            public const string MUFG_MX2_LECTURA_SER_OBTENER_FORMATO_CMS_POR_FOLIO = "MUFG_MX2_LecturaSERObtenerFormatoCmsPorFolio";
            public const string MUFG_MX2_LECTURA_SER_ACTUALIZAR_FORMATO_CMS_ACEPTADO = "MUFG_MX2_LecturaSERActualizarFormatoCmsAceptado";
            public const string MUFG_MX2_LECTURA_SER_ACTUALIZAR_FORMATO_CMS_PENDIENTE_EJECUCION = "MUFG_MX2_LecturaSERActualizarFormatoCmsPendienteEjecucion";
        }
/// <summary>
/// Representa Procedimientos SPID.
/// </summary>

        public static class ProcedimientosSPID
        {
            public const string USP_LECTURA_SPID_OBTENER_STATUS_FOREX_POR_EXTERNAL_REF = "usp_LecturaSPID_ObtenerStatusForexPorExternalRef";
            public const string USP_LECTURA_SPID_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaSPID_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_TOMBIN_INSERT_FORMATO_SPD = "usp_TOMBIN_INSERT_FORMATO_SPD";
            public const string USP_LECTURA_SPID_CONTAR_TOMBIN_NF_POR_FOLIO = "usp_LecturaSPID_ContarTombinNfPorFolio";
            public const string USP_LECTURA_SPID_OBTENER_TOMBIN_NF_POR_FOLIO = "usp_LecturaSPID_ObtenerTombinNfPorFolio";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string USP_LECTURA_SPID_CONTAR_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaSPID_ContarEntradaSalidaPorReferencia";
            public const string USP_LECTURA_SPID_ACTUALIZAR_TOMBIN_NF_ESTATUS = "usp_LecturaSPID_ActualizarTombinNfEstatus";
            public const string USP_LECTURA_SPID_CANCELAR_PLAN_TRABAJO_POR_FOREX = "usp_LecturaSPID_CancelarPlanTrabajoPorForex";
            public const string USP_LECTURA_SPID_OBTENER_CONCILIACION_NF = "usp_LecturaSPID_ObtenerConciliacionNf";
            public const string USP_LECTURA_SPID_OBTENER_CONCILIACION_ES = "usp_LecturaSPID_ObtenerConciliacionEs";
            public const string USP_LECTURA_SPID_EXISTE_DIA_NO_HABIL = "usp_LecturaSPID_ExisteDiaNoHabil";
            public const string MUFG_MX2_LECTURA_SPID_OBTENER_FORMATO_POR_FOLIO = "MUFG_MX2_LecturaSPIDObtenerFormatoPorFolio";
            public const string MUFG_MX2_LECTURA_SPID_ACTUALIZAR_FORMATO = "MUFG_MX2_LecturaSPIDActualizarFormato";
            public const string MUFG_MX2_LIST_CUSTOMER_EDO_CTA_T24 = "MUFG_MX2_ListCUSTOMEREdoCtaT24";
        }
/// <summary>
/// Representa Procedimientos SPIDGPH.
/// </summary>

        public static class ProcedimientosSPIDGPH
        {
            public const string USP_LECTURA_SPID_GPH_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaSPIDGph_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_SPID_GPH_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaSPIDGph_ActualizarFolioUnico";
            public const string USP_LECTURA_SPID_GPH_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaSPIDGph_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_SPID_GPH_ACTUALIZAR_DIRECCION = "usp_LecturaSPIDGph_ActualizarDireccion";
            public const string USP_LECTURA_SPID_GPH_EXISTE_DIA_NO_HABIL = "usp_LecturaSPIDGph_ExisteDiaNoHabil";
            public const string USP_LECTURA_SPID_GPH_MARCAR_ERROR_CUENTA_INVALIDA = "usp_LecturaSPIDGph_MarcarErrorCuentaInvalida";
        }
/// <summary>
/// Representa Procedimientos SPIDH2 H.
/// </summary>

        public static class ProcedimientosSPIDH2H
        {
            public const string USP_LECTURA_SPID_H2H_OBTENER_DATOS_OPERACION = "usp_LecturaSPID_H2H_ObtenerDatosOperacion";
            public const string USP_LECTURA_SPID_H2H_ACTUALIZAR_PLAN_TRABAJO_MAESTRO = "usp_LecturaSPID_H2H_ActualizarPlanTrabajoMaestro";
            public const string USP_LECTURA_SPID_H2H_ACTUALIZAR_DIRECCION = "usp_LecturaSPID_H2H_ActualizarDireccion";
            public const string USP_LECTURA_SPID_H2H_MARCAR_PLAN_TRABAJO_CON_ERROR = "usp_LecturaSPID_H2H_MarcarPlanTrabajoConError";
            public const string USP_LECTURA_SPID_H2H_OBTENER_DIA_NO_HABIL = "usp_LecturaSPID_H2H_ObtenerDiaNoHabil";
            public const string MUFG_MX2_LIST_CUSTOMER_EDO_CTA_T24 = "MUFG_MX2_ListCUSTOMEREdoCtaT24";
        }
/// <summary>
/// Representa Procedimientos SPIDLocal.
/// </summary>

        public static class ProcedimientosSPIDLocal
        {
            public const string USP_LECTURA_SPID_LOCAL_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaSPIDLocal_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_SPID_LOCAL_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaSPIDLocal_ActualizarFolioUnico";
            public const string USP_LECTURA_SPID_LOCAL_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaSPIDLocal_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_LECTURA_SPID_LOCAL_MARCAR_ERROR_CUENTA_INVALIDA = "usp_LecturaSPIDLocal_MarcarErrorCuentaInvalida";
        }
/// <summary>
/// Representa Procedimientos TAX.
/// </summary>

        public static class ProcedimientosTAX
        {
            public const string USP_LECTURA_TAX_ACTUALIZAR_DATOS_GENERALES_MAESTRO = "usp_LecturaTAX_ActualizarDatosGeneralesMaestro";
            public const string USP_TOMBIN_INSERT_FORMATO_TAX_PAY = "usp_TOMBIN_INSERT_FORMATO_TAX_PAY";
            public const string USP_LECTURA_TAX_CONTAR_TAX_PAY_POR_FOLIO = "usp_LecturaTAX_ContarTaxPayPorFolio";
            public const string USP_LECTURA_TAX_OBTENER_TAX_PAY_PARA_ENTRADA_SALIDA = "usp_LecturaTAX_ObtenerTaxPayParaEntradaSalida";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string USP_LECTURA_TAX_CONTAR_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaTAX_ContarEntradaSalidaPorReferencia";
            public const string USP_LECTURA_TAX_ACTUALIZAR_TAX_PAY_ACEPTADO = "usp_LecturaTAX_ActualizarTaxPayAceptado";
            public const string USP_LECTURA_TAX_ACTUALIZAR_TAX_PAY_FECHA_FUTURA = "usp_LecturaTAX_ActualizarTaxPayFechaFutura";
            public const string USP_LECTURA_TAX_OBTENER_TAX_PAY_PARA_CONCILIACION = "usp_LecturaTAX_ObtenerTaxPayParaConciliacion";
            public const string USP_LECTURA_TAX_OBTENER_ENTRADA_SALIDA_PARA_CONCILIACION = "usp_LecturaTAX_ObtenerEntradaSalidaParaConciliacion";
            public const string MUFG_MX2_LECTURA_TAX_OBTENER_FORMATO_TAX_POR_FOLIO = "MUFG_MX2_LecturaTAXObtenerFormatoTaxPorFolio";
            public const string MUFG_MX2_LECTURA_TAX_ACTUALIZAR_FORMATO_ACEPTADO = "MUFG_MX2_LecturaTAXActualizarFormatoAceptado";
            public const string MUFG_MX2_LECTURA_TAX_ACTUALIZAR_FORMATO_RECIBIDO_PENDIENTE_EJECUCION = "MUFG_MX2_LecturaTAXActualizarFormatoRecibidoPendienteEjecucion";
        }
/// <summary>
/// Representa Procedimientos TEF.
/// </summary>

        public static class ProcedimientosTEF
        {
            public const string USP_LECTURA_TEF_OBTENER_STATUS_FOREX_POR_EXTERNAL_REF = "usp_LecturaTEF_ObtenerStatusForexPorExternalRef";
            public const string USP_LECTURA_TEF_ACTUALIZAR_DATOS_GENERALES_PLAN_TRABAJO = "usp_LecturaTEF_ActualizarDatosGeneralesPlanTrabajo";
            public const string USP_LECTURA_TEF_EXISTE_FORMATO_TEF_POR_FOLIO = "usp_LecturaTEF_ExisteFormatoTefPorFolio";
            public const string USP_TOMBIN_INSERT_FORMATO_TEF = "usp_TOMBIN_INSERT_FORMATO_TEF";
            public const string USP_LECTURA_TEF_OBTENER_ENTRADA_SALIDA_ORIGEN_POR_FOLIO = "usp_LecturaTEF_ObtenerEntradaSalidaOrigenPorFolio";
            public const string USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS = "usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS";
            public const string USP_LECTURA_TEF_EXISTE_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaTEF_ExisteEntradaSalidaPorReferencia";
            public const string USP_LECTURA_TEF_ACTUALIZAR_ESTATUS_TEF = "usp_LecturaTEF_ActualizarEstatusTef";
            public const string USP_LECTURA_TEF_CANCELAR_PLAN_TRABAJO_POR_FOREX = "usp_LecturaTEF_CancelarPlanTrabajoPorForex";
            public const string USP_LECTURA_TEF_OBTENER_CONCILIACION_TEF_POR_FOLIO = "usp_LecturaTEF_ObtenerConciliacionTefPorFolio";
            public const string USP_LECTURA_TEF_OBTENER_CONCILIACION_ENTRADA_SALIDA_POR_REFERENCIA = "usp_LecturaTEF_ObtenerConciliacionEntradaSalidaPorReferencia";
            public const string MUFG_MX2_LECTURA_TEF_OBTENER_FORMATO_POR_FOLIO = "MUFG_MX2_LecturaTEFObtenerFormatoPorFolio";
            public const string MUFG_MX2_LECTURA_TEF_ACTUALIZAR_ESTATUS_FORMATO_CMS = "MUFG_MX2_LecturaTEFActualizarEstatusFormatoCms";
        }
/// <summary>
/// Representa Procedimientos TEFGPH.
/// </summary>

        public static class ProcedimientosTEFGPH
        {
            public const string USP_LECTURA_TEF_GPH_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaTEFGph_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_TEF_GPH_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaTEFGph_ActualizarFolioUnico";
            public const string USP_LECTURA_TEF_GPH_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaTEFGph_ActualizarPlanTrabajoMaestroLectura";
        }
/// <summary>
/// Representa Procedimientos TEFH2 H.
/// </summary>

        public static class ProcedimientosTEFH2H
        {
            public const string USP_LECTURA_TEF_H2H_OBTENER_DATOS_OPERACION = "usp_LecturaTEF_H2H_ObtenerDatosOperacion";
            public const string USP_LECTURA_TEF_H2H_ACTUALIZAR_PLAN_TRABAJO_MAESTRO = "usp_LecturaTEF_H2H_ActualizarPlanTrabajoMaestro";
        }
/// <summary>
/// Representa Procedimientos TEFLocal.
/// </summary>

        public static class ProcedimientosTEFLocal
        {
            public const string USP_LECTURA_TEF_LOCAL_OBTENER_DATOS_BASICOS_POR_FOLIO = "usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio";
            public const string USP_LECTURA_TEF_LOCAL_ACTUALIZAR_FOLIO_UNICO = "usp_LecturaTEFLocal_ActualizarFolioUnico";
            public const string USP_LECTURA_TEF_LOCAL_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaTEFLocal_ActualizarPlanTrabajoMaestroLectura";
        }
/// <summary>
/// Representa Procedimientos TMD.
/// </summary>

        public static class ProcedimientosTMD
        {
            public const string USP_LECTURA_TMD_OBTENER_SOLICITUD_ESPECIAL_POR_ID = "usp_LecturaTMD_ObtenerSolicitudEspecialPorId";
            public const string USP_LECTURA_TMD_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA = "usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura";
            public const string USP_TOMBIN_INSERT_FORMATO_TMD = "usp_TOMBIN_INSERT_FORMATO_TMD";
            public const string USP_LECTURA_TMD_CONTAR_TOMBIN_NF_POR_FOLIO = "usp_LecturaTMD_ContarTombinNfPorFolio";
            public const string USP_LECTURA_TMD_MARCAR_SOLICITUD_ESPECIAL_PROCESADA = "usp_LecturaTMD_MarcarSolicitudEspecialProcesada";
            public const string USP_LECTURA_TMD_ACTUALIZAR_NF_ESTATUS = "usp_LecturaTMD_ActualizarNfEstatus";
            public const string USP_LECTURA_TMD_OBTENER_DATOS_CONCILIACION = "usp_LecturaTMD_ObtenerDatosConciliacion";
            public const string MUFG_MX2_LECTURA_TMD_OBTENER_FORMATO_POR_FOLIO = "MUFG_MX2_LecturaTMDObtenerFormatoPorFolio";
            public const string MUFG_MX2_LECTURA_TMD_ACTUALIZAR_FORMATO_ESTADO = "MUFG_MX2_LecturaTMDActualizarFormatoEstado";
        }
    }
}

using Aplicacion.DTOs.CNT;
using Aplicacion.Interfaces.CNT;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa CNT.
    /// </summary>
    public class CNT
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ICNTDao _CNTDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "CNT";
        private string folioUnico = "";

        /// <summary>
        /// Constructor.
        /// </summary>
        public CNT(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ICNTDao cNTDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _CNTDao = cNTDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación CNT del servicio Lectura.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio)
        {
            CntConcentradoraDto datosConcentradora;
            string fechaEjecucionTexto = string.Empty;
            string monedaTomi = string.Empty;
            decimal importeTomi = 0m;
            bool procesado;
            bool conEntradaSalida = false;

            //Verificamos la última operación no realizada...
            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });

            if (string.Equals(_configuracion.AliasProceso, "", StringComparison.Ordinal))
                return false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            //Datos de la operación...
            datosConcentradora = await _CNTDao.ObtenerConcentradoraPorFolio(folio);
            if (datosConcentradora is null)
                throw new Exception($"No se encontraron datos de operación CNT para folio={folio}.");

            if (datosConcentradora != null)
            {
                folioUnico = DameTexto(datosConcentradora.NfFolioUnico);
                fechaEjecucionTexto = datosConcentradora.NfFechaEjecucion?.ToString() ?? string.Empty;
                importeTomi = datosConcentradora.T24DebitAmount ?? 0m;
                monedaTomi = _vwT24.ValidaMoneda(datosConcentradora.T24DebitCurrency);

                if (string.Equals(monedaTomi, "MXP", StringComparison.Ordinal) ||
                    string.Equals(monedaTomi, "MXN", StringComparison.Ordinal))
                {
                    conEntradaSalida = true;
                }
            }

            //Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc).");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                procesado = false;

                if (string.Equals(folioUnico, "", StringComparison.Ordinal))
                {
                    folioUnico = await _vwT24.GeneraFolioUnico(tipoOperacion, ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucionTexto), folio);
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Creamos folio_unico='{folioUnico}'.");

                    // Actualizamos en concentradora.
                    bool actualizoFolioUnico = await _CNTDao.ActualizarFolioUnicoConcentradora(folio, folioUnico);
                    if (!actualizoFolioUnico)
                        throw new Exception($"No fue posible actualizar el folio_unico en tb_tombin_nf_concentradoras para folio={folio}.");
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                int fechaValor = datosConcentradora?.NfFechaEjecucion ?? 0;
                procesado = await _CNTDao.ActualizarDatosPlanTrabajoMaestroDesdeConcentradora(
                    folio,
                    tipoOperacion,
                    "LECTURA",
                    folioUnico,
                    fechaValor,
                    _vwT24.MonedaDefaultMN);

                if (procesado)
                {
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    //Verificamos la última operación no realizada.
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
                }
            }

            //Registro de la Entrada/Salida.
            if (string.Equals(_configuracion.AliasProceso, "TOMI_ENTRADAS/SALIDAS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|CNT.procesaOperacion|A insertar datos de Entrada/Salida en TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (conEntradaSalida)
                {
                    procesado = await TOMI_ENTRADA_SALIDA(folio);

                    if (procesado)
                    {
                        Config.EscribirLog("INFO|CNT.procesaOperacion|Se insertó correctamente el folio en base de Entrada/Salida.");
                    }
                    else
                    {
                        int numRegs = await _CNTDao.ContarEntradasSalidasPorReferencia(folioUnico);
                        if (numRegs > 0)
                        {
                            procesado = true;
                            Config.EscribirLog("INFO|CNT.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                        }
                    }
                }
                else
                {
                    procesado = true;
                    Config.EscribirLog("INFO|CNT.procesaOperacion|La operación no debe generar Entrada/Salida en TOMI (la operación no es en pesos).");
                }

                if (procesado)
                {
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    //Verificamos la última operación no realizada.
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");
                }
            }

            //Conciliación.
            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                if (conEntradaSalida)
                {
                    int regsEntSal = 0;
                    string fechaEjecucionEntSal = string.Empty;
                    string monedaEntSal = string.Empty;
                    decimal importeEntSal = 0m;

                    Config.EscribirLog("INFO|CNT.procesaOperacion|Inicia la Conciliacion");

                    CntEntradaSalidaDto entradaSalida = await _CNTDao.ObtenerEntradaSalidaPorReferencia(folioUnico);
                    if (entradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = entradaSalida.EntFechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                        monedaEntSal = _vwT24.ValidaMoneda(entradaSalida.EntMonedaCuenta);
                        importeEntSal = entradaSalida.EntImporte ?? 0m;
                    }

                    procesado = false;

                    Config.EscribirLog($"{regsEntSal}-{fechaEjecucionEntSal}-{_configuracion.StrFechaSistema}-{monedaEntSal}-{monedaTomi}-{importeEntSal}-{importeTomi}");

                    if (regsEntSal == 1 &&
                        string.Equals(fechaEjecucionEntSal, _configuracion.StrFechaSistema, StringComparison.Ordinal) &&
                        string.Equals(monedaEntSal, monedaTomi, StringComparison.Ordinal) &&
                        importeEntSal == importeTomi)
                    {
                        procesado = true;
                    }

                    if (procesado)
                    {
                        Config.EscribirLog("INFO|CNT.procesaOperacion|La operación está correctamente conciliada.");

                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });

                        _configuracion.AliasProceso = "";
                    }
                    else
                    {
                        throw new Exception($"Error en procesamiento de registro CONCILIAR con folio: {folio}");
                    }
                }
                else
                {
                    procesado = true;

                    if (procesado)
                    {
                        Config.EscribirLog("INFO|CNT.procesaOperacion|La operación está correctamente conciliada.");

                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });

                        _configuracion.AliasProceso = "";
                    }
                    else
                    {
                        throw new Exception($"Error en procesamiento de registro CONCILIAR con folio: {folio}");
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Inserta registro de Entradas/Salidas en TOMI.
        /// </summary>
        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            object[] valoresEntSal = new object[24];

            try
            {
                CntConcentradoraDto datosConcentradora = await _CNTDao.ObtenerConcentradoraPorFolio(folio);
                if (datosConcentradora is null)
                    throw new Exception($"No se encontraron datos de operación CNT para folio={folio}.");

                if (datosConcentradora != null)
                {
                    valoresEntSal[1] = folioUnico;
                    valoresEntSal[2] = ConversionHelper.ParseIntOrZero(datosConcentradora.T24CustomerCode, "t24_customer_code");
                    valoresEntSal[3] = ConversionHelper.ParseDoubleOrZero(datosConcentradora.T24CustomerCode, "t24_customer_code");
                    valoresEntSal[4] = datosConcentradora.T24CompanyName;
                    valoresEntSal[5] = 198d;
                    valoresEntSal[6] = "Entrada  (TTP)";
                    valoresEntSal[7] = "D";
                    valoresEntSal[8] = "Concentradora";
                    valoresEntSal[9] = ConversionHelper.ParseDoubleOrZero(datosConcentradora.T24BenAcctNo, "t24_ben_acct_no");
                    valoresEntSal[10] = datosConcentradora.T24CompanyName;
                    valoresEntSal[11] = _vwT24.ValidaMoneda(datosConcentradora.T24DebitCurrency);
                    valoresEntSal[12] = 0d; // SubAplicacion
                    valoresEntSal[13] = "TRF RECIBIDA CONCENTRADORA";
                    valoresEntSal[14] = datosConcentradora.T24DebitAmount ?? 0m; // Monto
                    valoresEntSal[15] = 0m;
                    valoresEntSal[16] = datosConcentradora.NfReferencia?.ToString(); // Comentario
                    valoresEntSal[17] = ConversionHelper.ParseFechaSistema(_configuracion.StrFechaSistema); // Fecha de proceso
                    valoresEntSal[18] = ConversionHelper.ParseFechaSistema(_configuracion.StrFechaSistema); // Fecha valor
                    valoresEntSal[19] = DateTime.Now.ToLongTimeString();
                    valoresEntSal[20] = "JEAI";
                    valoresEntSal[21] = 0d;
                    valoresEntSal[22] = "A";
                    valoresEntSal[23] = 0d;
                }

                for (int intcont = 0; intcont <= 23; intcont++)
                {
                    Config.EscribirLog($"VALOR {intcont} | {valoresEntSal[intcont]}");
                }

                bool resultado = await _CNTDao.InsertarEntradaSalidaSrv(
                    referencia: Convert.ToString(valoresEntSal[1]),
                    numCliente: Convert.ToInt32(valoresEntSal[2]),
                    cdl: Convert.ToDouble(valoresEntSal[3]),
                    ordenante: Convert.ToString(valoresEntSal[4]),
                    operacion: Convert.ToDouble(valoresEntSal[5]),
                    operacionDesc: Convert.ToString(valoresEntSal[6]),
                    tipo: Convert.ToString(valoresEntSal[7]),
                    tipoDesc: Convert.ToString(valoresEntSal[8]),
                    cuenta: Convert.ToDouble(valoresEntSal[9]),
                    nombre: Convert.ToString(valoresEntSal[10]),
                    cuentaMoneda: Convert.ToString(valoresEntSal[11]),
                    via: Convert.ToDouble(valoresEntSal[12]),
                    viaDesc: Convert.ToString(valoresEntSal[13]),
                    importe: Convert.ToDecimal(valoresEntSal[14]),
                    saldo: Convert.ToDecimal(valoresEntSal[15]),
                    comentarios: Convert.ToString(valoresEntSal[16]),
                    fechaCaptura: Convert.ToDateTime(valoresEntSal[17]),
                    fechaValor: Convert.ToDateTime(valoresEntSal[18]),
                    hora: Convert.ToString(valoresEntSal[19]),
                    capturista: Convert.ToString(valoresEntSal[20]),
                    fad: Convert.ToDouble(valoresEntSal[21]),
                    referenciaControl: Convert.ToString(valoresEntSal[22]),
                    referenciaControlCont: Convert.ToDouble(valoresEntSal[23]));

                Config.EscribirLog($"INFO|CNT.TOMI_ENTRADA_SALIDA|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|CNT.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}.Trace:{ex}");
            }

            return false;
        }


    }
}

using System;
using System.Collections.Generic;

namespace Aplicacion.Servicios.Operaciones
{
    internal sealed class ContenidoFormatoParseResult
    {
        public ContenidoFormatoParseResult(string[] tokens, Dictionary<string, string> valuesByKey)
        {
            Tokens = tokens;
            ValuesByKey = valuesByKey;
        }

        public string[] Tokens { get; }
        public Dictionary<string, string> ValuesByKey { get; }
    }

    internal static class ContenidoFormatoParser
    {
        public static ContenidoFormatoParseResult Parse(string contenidoFormato, string folio)
        {
            string[] tokens = (contenidoFormato ?? string.Empty)
                .Split(new[] { "|" }, StringSplitOptions.None);

            var valuesByKey = new Dictionary<string, string>(StringComparer.Ordinal);

            for (int i = 0; i < tokens.Length; i++)
            {
                string token = tokens[i];

                if (string.IsNullOrEmpty(token))
                    continue;

                string[] kv = token.Split(new[] { "=" }, 2, StringSplitOptions.None);
                if (kv.Length != 2 || string.IsNullOrWhiteSpace(kv[0]))
                    throw new Exception($"El contenidoFormato del folio {folio} contiene un dato malformado en la posición {i}: '{token}'.");

                valuesByKey[kv[0]] = kv[1];
            }

            return new ContenidoFormatoParseResult(tokens, valuesByKey);
        }

        public static void ValidarMinimoIndice(ContenidoFormatoParseResult parseado, string folio, int indiceRequerido)
        {
            if (parseado.Tokens.Length - 1 < indiceRequerido)
                throw new Exception($"El contenidoFormato del folio {folio} es diferente al esperado, debe ser {indiceRequerido} datos y se tienen {parseado.Tokens.Length - 1}.");
        }

        public static string ObtenerValorPosicional(ContenidoFormatoParseResult parseado, string folio, int indice)
        {
            if (indice < 0 || indice >= parseado.Tokens.Length)
                throw new Exception($"El contenidoFormato del folio {folio} no contiene la posición {indice} requerida.");

            string token = parseado.Tokens[indice];
            if (string.IsNullOrEmpty(token))
                throw new Exception($"El contenidoFormato del folio {folio} contiene un dato vacío en la posición {indice}.");

            string[] kv = token.Split(new[] { "=" }, 2, StringSplitOptions.None);
            if (kv.Length != 2 || string.IsNullOrWhiteSpace(kv[0]))
                throw new Exception($"El contenidoFormato del folio {folio} contiene un dato malformado en la posición {indice}: '{token}'.");

            return kv[1];
        }

        public static string ObtenerValorPorClave(ContenidoFormatoParseResult parseado, string clave)
        {
            if (parseado.ValuesByKey.TryGetValue(clave, out string valor))
                return valor;

            return string.Empty;
        }
    }
}

using Aplicacion.DTOs.ENT_SPEI;
using Aplicacion.Interfaces;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.SPEI;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa ENTSPEI.
    /// </summary>
    public class ENTSPEI
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IEntSpeiDao _tomiDao;
        private readonly IEntSpeiSpeiDao _speiDao;
        private readonly IComunFXD _comunFXD;
        private readonly IT24CuentaWebService _t24CuentaWebService;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "ENT_SPEI";
        private const string CASFIM_MUFG = "40108";
        private static readonly List<int> SPEI_Devs_NumCel = new List<int> { 1, 3, 5, 30, 36 };
        private static readonly string[] BTMUM_CLABES = new[]
        {
            "00012180004471781901",
            "00002180019710381753",
            "00002180087000493707"
        };

        private string folioUnico = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase ENTSPEI.
        /// </summary>

        public ENTSPEI(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IEntSpeiDao tomiDao,
            IEntSpeiSpeiDao speiDao,
            IComunFXD comunFXD,
            IT24CuentaWebService t24CuentaWebService,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tomiDao = tomiDao;
            _speiDao = speiDao;
            _comunFXD = comunFXD;
            _t24CuentaWebService = t24CuentaWebService;
            _vwT24 = vwT24;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            string moneda = string.Empty;
            decimal importe = 0m;
            string subtipoOperacion = string.Empty;
            string razonRechazo = string.Empty;
            string comentarios = string.Empty;
            string cuentaBeneficiario = string.Empty;
            string cuentaBeneficiario2 = string.Empty;
            string cuentaBTMUM = "0";
            bool devolucion = false;
            bool esInterbancarioBTMUM = false;
            bool conEntradaSalida = true;
            bool t24Encontrado = false;
            bool t24CuentaValida = false;
            bool t24CuentaNoExiste = false;
            bool t24CuentaBloqueada = false;
            bool t24CuentaCerrada = false;
            bool rfcCurpValido = true;
            bool caracterValido = true;
            string statusCuenta = string.Empty;
            string participanteNombre = string.Empty;
            int tipoOperacionSpei;
            int tipoClaveBen;
            int tipoClaveBen2;
            bool firmaValida = false;
            DataTable datosCuentaT24 = null;
            string t24CustomerCode = string.Empty;
            string t24CompanyName = string.Empty;
            string t24AccountTitle1 = string.Empty;
            string t24Currency = string.Empty;

            bool procesoCompletado;
            bool resultado;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Iniciando");

            _configuracion.AliasProceso =
                await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

            if (string.IsNullOrEmpty(_configuracion.AliasProceso))
                return false;

            var participantes = await _speiDao.ObtenerParticipantes();

            var operaciones = await _speiDao
                .ObtenerOperacionesRecibidasPorFecha(Convert.ToDecimal(_configuracion.StrFechaSistema));

            var operacion = operaciones.FirstOrDefault(x => x.op_clave.ToString() == folio);

            if (operacion == null)
                throw new Exception($"No se encontró información SPEI para folio {folio}");

            fechaEjecucion = operacion.op_fecha_oper.ToString();
            importe = Convert.ToDecimal(operacion.op_monto);
            if (!string.IsNullOrEmpty(operacion.op_concepto_pago))
            {
                comentarios += operacion.op_concepto_pago;
            }

            if (!string.IsNullOrEmpty(operacion.op_concepto_pag2))
            {
                comentarios += operacion.op_concepto_pag2;
            }

            moneda = _vwT24.MonedaDefaultMN;

            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(_configuracion.StrFechaSistema),
                folio);

            var participante = participantes.FirstOrDefault(x => x.ins_clave == Convert.ToInt32(operacion.ins_clave));
            if (participante == null)
                throw new Exception($"No existe participante {operacion.ins_clave}");
            participanteNombre = participante.ins_nombre;

            tipoOperacionSpei = Convert.ToInt32(operacion.tp_clave ?? 0);
            tipoClaveBen = (tipoOperacionSpei == 30 || tipoOperacionSpei == 31 || tipoOperacionSpei == 36)
                ? 0
                : Convert.ToInt32(operacion.tc_clave_ben ?? 0);
            tipoClaveBen2 = Convert.ToInt32(operacion.tc_clave_ben2 ?? 0);

            if ((tipoOperacionSpei == 1 || tipoOperacionSpei == 5 || tipoOperacionSpei == 30 || tipoOperacionSpei == 31 || tipoOperacionSpei == 36) && tipoClaveBen != 10)
            {
                if (tipoOperacionSpei != 31)
                {
                    cuentaBeneficiario = operacion.op_cuenta_ben ?? string.Empty;
                    cuentaBTMUM = ObtenerCuentaBtmum(cuentaBeneficiario);
                }
                else
                {
                    cuentaBeneficiario = "124950001";
                    cuentaBTMUM = "124950001";
                }

                string cuentaOrdenante = operacion.op_cuenta_ord ?? string.Empty;
                esInterbancarioBTMUM = BTMUM_CLABES.Any(x => x == cuentaOrdenante);
                if (esInterbancarioBTMUM)
                {
                    subtipoOperacion = "BBC";
                }
            }
            else if ((tipoOperacionSpei == 6 || tipoOperacionSpei == 3) && (tipoClaveBen != 10 && tipoClaveBen2 != 10))
            {
                subtipoOperacion = "VOSTRO";
                string swift = operacion.op_nom_ben ?? string.Empty;
                string swiftShort = swift.Length >= 8 ? swift.Substring(0, 8) : swift;
                cuentaBeneficiario = operacion.op_cuenta_ben ?? string.Empty;
                cuentaBTMUM = await ResolverCuentaVostroLegacy(swiftShort, cuentaBeneficiario);
            }
            else if (tipoOperacionSpei == 0)
            {
                subtipoOperacion = "DEVOLUCION";
                devolucion = true;
                cuentaBeneficiario = operacion.op_cuenta_ord ?? string.Empty;
                if (string.IsNullOrEmpty(cuentaBeneficiario))
                {
                    cuentaBTMUM = "0";
                    esInterbancarioBTMUM = true;
                    subtipoOperacion = "Part.-Vostro";
                }
                else
                {
                    cuentaBTMUM = ObtenerCuentaBtmum(cuentaBeneficiario);
                }
            }
            else
            {
                var autoDev = (await _tomiDao.ObtenerCatalogoDevolucionesAutomaticas())
                    .FirstOrDefault(x => x.Clave == tipoOperacionSpei);

                if (autoDev != null)
                {
                    razonRechazo = $"Pago no aceptado por ser tipo {tipoOperacionSpei}";
                    string firmaDev = await ObtenerFirmaDevolucion(folio);
                    await _speiDao.EjecutarDevolucion(folio, autoDev.CodigoDevolucion, firmaDev);
                    await _tomiDao.ActualizarPlanDevolucion(folio, tipoOperacion, razonRechazo, int.Parse(fechaEjecucion, CultureInfo.InvariantCulture), moneda, importe);
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    return true;
                }

                if (SPEI_Devs_NumCel.Contains(tipoOperacionSpei) && (tipoClaveBen == 10 || tipoClaveBen2 == 10))
                {
                    cuentaBeneficiario = operacion.op_cuenta_ben ?? string.Empty;
                    cuentaBeneficiario2 = operacion.op_cuenta_ben2 ?? string.Empty;
                    if (string.IsNullOrEmpty(cuentaBeneficiario) && string.IsNullOrEmpty(cuentaBeneficiario2))
                    {
                        throw new Exception($"Error al obtener la cuenta del beneficiario para el folio: {folio}");
                    }

                    razonRechazo = "Pago no aceptado por tener cuenta beneficiario con número celular";
                    string firmaDev = await ObtenerFirmaDevolucion(folio);
                    await _speiDao.EjecutarDevolucion(folio, 22, firmaDev);
                    await _tomiDao.ActualizarPlanDevolucion(folio, tipoOperacion, razonRechazo, int.Parse(fechaEjecucion, CultureInfo.InvariantCulture), moneda, importe);
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    return true;
                }

                throw new Exception($"Tipo de operación de entrada SPEI desconocido, tipo={tipoOperacionSpei}");
            }

            if (tipoOperacionSpei != 31)
            {
                if (long.TryParse(cuentaBTMUM, out long cuentaSinCeros))
                {
                    cuentaBTMUM = cuentaSinCeros.ToString(CultureInfo.InvariantCulture);
                }
            }

            if (esInterbancarioBTMUM)
            {
                conEntradaSalida = false;
            }
            else
            {
                if (tipoOperacionSpei != 31)
                {
                    caracterValido = long.TryParse(cuentaBTMUM, out _);
                    if (!caracterValido)
                    {
                        razonRechazo = "La cuenta contiene caracteres no válidos";
                    }

                    if (caracterValido)
                    {
                        var infoCuenta = await _t24CuentaWebService.ObtenerInformacionCuenta(cuentaBTMUM);
                        if (infoCuenta == null)
                            throw new InvalidOperationException($"El servicio de información de cuenta T24 regresó respuesta nula para la cuenta '{cuentaBTMUM}'.");

                        statusCuenta = infoCuenta.Status ?? string.Empty;
                        switch (statusCuenta)
                        {
                            case "CTA ACTIVA":
                                datosCuentaT24 = await _vwT24.DatosCuentaVw(cuentaBTMUM);
                                if (datosCuentaT24 != null && datosCuentaT24.Rows.Count > 0)
                                {
                                    DataRow fila = datosCuentaT24.Rows[0];
                                    t24CustomerCode = Convert.ToString(fila["CUSTOMER"]) ?? string.Empty;
                                    t24CompanyName = Convert.ToString(fila["NAME_1"]) ?? string.Empty;
                                    t24AccountTitle1 = Convert.ToString(fila["ACCOUNT_TITLE_1"]) ?? string.Empty;
                                    t24Currency = Convert.ToString(fila["CURRENCY"]) ?? string.Empty;

                                    if (!string.IsNullOrWhiteSpace(t24CustomerCode))
                                    {
                                        t24Encontrado = true;
                                        t24CuentaValida = string.Equals(t24Currency, _vwT24.MonedaDefaultMN, StringComparison.OrdinalIgnoreCase);
                                    }
                                }

                                if (!t24CuentaValida)
                                {
                                    razonRechazo = "La cuenta seleccionada en el sistema T24 esta relacionada a una moneda no valida";
                                }
                                break;
                            case "CTA CERRADA":
                                t24CuentaCerrada = true;
                                razonRechazo = "La cuenta en el sistema T24 está cerrada";
                                break;
                            case "CTA BLOQUEADA":
                                t24CuentaBloqueada = true;
                                razonRechazo = "La cuenta en el sistema T24 está bloqueada";
                                break;
                            default:
                                t24CuentaNoExiste = true;
                                razonRechazo = "No existe la cuenta en el sistema T24";
                                break;
                        }
                    }
                }
                else
                {
                    t24Encontrado = true;
                    t24CuentaValida = true;
                }

                if (tipoOperacionSpei == 1 || tipoOperacionSpei == 30 || tipoOperacionSpei == 31 || tipoOperacionSpei == 36)
                {
                    rfcCurpValido = EsValidoRfcCurpSpei(operacion.op_rfc_curp_ord);
                    if (!rfcCurpValido)
                    {
                        razonRechazo = "El RFC del ordenante no es válido";
                    }

                    if (tipoOperacionSpei != 31 && !(operacion.op_cuenta_ben ?? string.Empty).StartsWith("1081800000", StringComparison.Ordinal))
                    {
                        t24CuentaValida = false;
                    }
                }

                conEntradaSalida = t24Encontrado && t24CuentaValida && rfcCurpValido && caracterValido;
            }

            if (!esInterbancarioBTMUM)
            {
                var ordenPago = new OrdenPagoValue
                {
                    OpFechaOper = fechaEjecucion,
                    InsClaveOrd = (operacion.ins_clave ?? 0).ToString(CultureInfo.InvariantCulture),
                    InsClaveBen = CASFIM_MUFG,
                    OpCveRastreo = operacion.op_cve_rastreo ?? string.Empty,
                    OpCuentaOrd = operacion.op_cuenta_ord ?? string.Empty,
                    OpCuentaBen = operacion.op_cuenta_ben ?? string.Empty,
                    OpCuentaBen2 = operacion.op_cuenta_ben2 ?? string.Empty,
                    TpPago = tipoOperacionSpei,
                    OpMonto = importe.ToString(CultureInfo.InvariantCulture),
                    opCuentaPartOrd = operacion.OP_CUENTA_PART_ORD ?? string.Empty,
                    opCuentaEmiRem = operacion.OP_CUENTA_EMI_REM ?? string.Empty
                };

                if (string.IsNullOrWhiteSpace(operacion.OP_FIRMA_DIG))
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("No se encontró información de la firma digital");
                }

                firmaValida = _comunFXD.ValidaSPEIEntrada(ordenPago, operacion.OP_FIRMA_DIG);
            }

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(fechaEjecucion, CultureInfo.InvariantCulture);
            if (fechaEjecucion != _configuracion.StrFechaSistema)
            {
                if (intFechaEjecucion < fechaSistema)
                    await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));

                throw new Exception(
                    $"Se desea realizar una operación al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // DATOS_GRAL
            if (_configuracion.AliasProceso == "DATOS_GRAL")
            {
                Config.EscribirLog("INFO|ENT_SPEI|Actualizando DATOS_GRAL");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                await _tomiDao
                    .ActualizarPlanTrabajoMaestroDatosGral(
                        folio,
                        tipoOperacion,
                        folioUnico,
                        Convert.ToInt32(fechaEjecucion),
                        moneda,
                        importe,
                        subtipoOperacion,
                        t24Encontrado && t24CuentaValida && rfcCurpValido && caracterValido && tipoOperacionSpei != 31,
                        t24CustomerCode,
                        t24CompanyName,
                        tipoOperacionSpei == 31 ? null : cuentaBTMUM);
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // SPEI_A_TOMI
            if (_configuracion.AliasProceso == "SPEI_A_TOMI")
            {
                Config.EscribirLog("INFO|ENT_SPEI|Insertando SPEI en TOMI");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });
                procesoCompletado = false;

                if (!t24CuentaCerrada && !t24CuentaNoExiste && !t24CuentaBloqueada)
                {
                    resultado = await SPEI_A_TOMI(
                        folio,
                        operacion,
                        t24Encontrado && t24CuentaValida ? "PEND" : "RECC",
                        cuentaBTMUM,
                        participanteNombre,
                        cuentaBTMUM,
                        tipoOperacionSpei == 31 ? string.Empty : t24CustomerCode,
                        tipoOperacionSpei == 31 ? string.Empty : t24CompanyName,
                        razonRechazo);

                    if (resultado)
                    {
                        procesoCompletado = true;
                    }
                    else
                    {
                        int count = await _tomiDao.ContarNfEntradasSpeiPorFolio(folio);
                        procesoCompletado = count > 0;
                    }

                    if (!procesoCompletado)
                        throw new Exception($"Error SPEI_A_TOMI folio {folio}");

                    if (esInterbancarioBTMUM)
                    {
                        var respuestaInterbancaria = await _speiDao.CambiarEstatusRecibida(
                            operacion.op_clave,
                            operacion.op_fecha_oper,
                            operacion.op_folio.Value,
                            operacion.op_cve_rastreo,
                            operacion.op_monto.Value,
                            operacion.cd_clave,
                            "A");

                        if (respuestaInterbancaria?.op_error != 0)
                        {
                            throw new Exception($"Error en CambiaEstatus para cuenta BTMUM: {respuestaInterbancaria?.op_descripcion}");
                        }

                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });
                        await _tomiDao.MarcarNfEntradaSpeiComoProcesada(folio);

                        bool cerrarDetalleExceptoCep = string.Equals(subtipoOperacion, "BBC", StringComparison.Ordinal);
                        await _tomiDao.ActualizarPlanTrabajoRecibidaNoAbonada(
                            folio,
                            tipoOperacion,
                            subtipoOperacion,
                            "Operación de Bancos BTMUM",
                            int.Parse(fechaEjecucion, CultureInfo.InvariantCulture),
                            moneda,
                            importe,
                            !cerrarDetalleExceptoCep);

                        if (cerrarDetalleExceptoCep)
                        {
                            await _tomiDao.CerrarPlanTrabajoDetalleExceptoCep(folio, tipoOperacion);
                        }

                        return true;
                    }
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso =
                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });
                }
                else
                {
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso =
                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });
                }
            }

            // TOMI_ENTRADAS/SALIDAS
            if (_configuracion.AliasProceso == "TOMI_ENTRADAS/SALIDAS")
            {
                Config.EscribirLog("INFO|ENT_SPEI|Insertando EntradaSalida");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (conEntradaSalida)
                {
                    resultado = await TOMI_ENTRADA_SALIDA(
                        folio,
                        tipoOperacionSpei == 31,
                        t24CustomerCode,
                        t24AccountTitle1,
                        t24Currency,
                        cuentaBTMUM,
                        comentarios);

                    if (!resultado)
                    {
                        int count = await _tomiDao.ContarEntradasSalidasPorReferencia(folioUnico);
                        resultado = count > 0;
                    }
                }
                else
                {
                    resultado = true;
                }

                if (!resultado)
                    throw new Exception($"Error TOMI_ENTRADAS_SALIDAS {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // CAMBIA_ESTATUS
            if (_configuracion.AliasProceso == "CAMBIA_ESTATUS")
            {
                Config.EscribirLog("INFO|ENT_SPEI|Cambiando estatus");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                bool rechazada = false;
                bool procesado = true;
                int causaDevolucion = 0;

                if (devolucion)
                {
                    var respuestaAceptada = await _speiDao.CambiarEstatusRecibida(
                        operacion.op_clave,
                        operacion.op_fecha_oper,
                        operacion.op_folio.Value,
                        operacion.op_cve_rastreo,
                        operacion.op_monto.Value,
                        operacion.cd_clave,
                        "A"
                    );

                    if (respuestaAceptada?.op_error != 0)
                    {
                        throw new Exception($"Error en CambiaEstatus: {respuestaAceptada?.op_descripcion}");
                    }

                    if (t24Encontrado)
                    {
                        if (t24CuentaValida)
                        {
                            if (!rfcCurpValido || !caracterValido)
                            {
                                rechazada = true;
                            }
                        }
                        else
                        {
                            rechazada = true;
                        }
                    }
                    else
                    {
                        rechazada = true;
                    }

                    if (rechazada)
                    {
                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });
                        await _tomiDao.ActualizarPlanTrabajoRechazoSinAbono(folio, tipoOperacion);
                        return true;
                    }
                }
                else
                {
                    if (!caracterValido)
                    {
                        rechazada = true;
                        causaDevolucion = 19;
                        razonRechazo = "La cuenta contiene caracteres no válidos";
                    }
                    else if (!t24Encontrado)
                    {
                        rechazada = true;
                        if (t24CuentaNoExiste)
                        {
                            causaDevolucion = 1;
                        }
                        else if (t24CuentaBloqueada)
                        {
                            causaDevolucion = 2;
                        }
                        else if (t24CuentaCerrada)
                        {
                            causaDevolucion = 3;
                        }
                    }
                    else if (!t24CuentaValida)
                    {
                        rechazada = true;
                        causaDevolucion = 5;
                    }
                    else if (!rfcCurpValido)
                    {
                        rechazada = true;
                        causaDevolucion = 14;
                    }

                    if (rechazada)
                    {
                        string firmaDev = await ObtenerFirmaDevolucion(folio);
                        procesado = await _speiDao.EjecutarDevolucion(folio, causaDevolucion, firmaDev);

                        if (!procesado)
                        {
                            throw new Exception($"Error en respuesta de devolución SPEI para folio {folio}");
                        }
                    }
                }

                if (!firmaValida)
                {
                    throw new Exception($"No se pudo confirmar la autenticación de los datos con la firma eléctrónica. Folio {folio}");
                }

                if (rechazada)
                {
                    await _tomiDao.ActualizarPlanDevolucion(
                        folio,
                        tipoOperacion,
                        razonRechazo,
                        int.Parse(fechaEjecucion, CultureInfo.InvariantCulture),
                        moneda,
                        importe);
                }
                else
                {
                    await _tomiDao.MarcarNfEntradaSpeiComoConp(folio);
                }
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // CONCILIAR
            if (_configuracion.AliasProceso == "CONCILIAR")
            {
                Config.EscribirLog("INFO|ENT_SPEI|Conciliación");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (t24CuentaCerrada || t24CuentaNoExiste || t24CuentaBloqueada)
                {
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    _configuracion.AliasProceso = string.Empty;
                    return true;
                }

                var datosTomi =
                    await _tomiDao.ObtenerConciliacionTomiPorFolio(folio);

                if (datosTomi == null)
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("No existe registro TOMI");

                var datosEntSal =
                    await _tomiDao.ObtenerConciliacionEntradaSalidaPorReferencia(folioUnico);

                string error = string.Empty;

                if (datosTomi.OpFechaOper?.ToString() != fechaEjecucion)
                    error += " Diferencia fecha";

                if (!datosTomi.OpMonto.HasValue || Convert.ToDecimal(datosTomi.OpMonto.Value) != importe)
                    error += " Diferencia importe";

                if (!string.Equals(moneda, _vwT24.MonedaDefaultMN, StringComparison.OrdinalIgnoreCase))
                    error += " Diferencia moneda";

                if (datosEntSal == null && conEntradaSalida)
                {
                    error += " No se ha registrado la operación en Entrada/Salida";
                }

                if (datosEntSal != null)
                {
                    string fechaEntSal = datosEntSal.EntFechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                    if (fechaEntSal != fechaEjecucion)
                        error += " Diferencia fecha entrada/salida";

                    string monedaEntSal = _vwT24.ValidaMoneda(datosEntSal.EntMonedaCuenta);
                    if (!string.Equals(monedaEntSal, moneda, StringComparison.OrdinalIgnoreCase))
                        error += " Diferencia moneda entrada/salida";

                    if (!datosEntSal.EntImporte.HasValue || Convert.ToDecimal(datosEntSal.EntImporte.Value) != importe)
                        error += " Diferencia entrada/salida";
                }

                if (!string.IsNullOrEmpty(error))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(error);
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                _configuracion.AliasProceso = "";
            }

            return true;
        }

        private async Task<bool> SPEI_A_TOMI(
            string folio,
            EntSpeiOperacionRecibidaDto dto,
            string nfEstatus,
            string cuentaBTMUM,
            string bancoBeneficiario,
            string t24BenAcctNo,
            string t24CustomerCode,
            string t24CompanyName,
            string razonRechazo)
        {
            return await _tomiDao.InsertarFormatoSpei(
                long.Parse(dto.op_clave.ToString()),
                int.Parse(dto.op_fecha_oper.ToString()),
                Convert.ToInt32(dto.me_clave),
                Convert.ToInt32(dto.op_folio),
                Convert.ToInt64(dto.ins_clave),
                Convert.ToDecimal(dto.op_monto),
                Convert.ToInt32(dto.tp_clave),
                dto.op_cve_rastreo,
                dto.op_nom_ord,
                Convert.ToInt32(dto.tc_clave_ord),
                dto.op_cuenta_ord,
                dto.op_rfc_curp_ord,
                dto.op_nom_ben,
                Convert.ToInt32(dto.tc_clave_ben),
                dto.op_cuenta_ben,
                dto.op_rfc_curp_ben,
                dto.op_nom_ben2,
                Convert.ToInt32(dto.tc_clave_ben2),
                dto.op_cuenta_ben2,
                dto.op_rfc_curp_ben2,
                dto.op_concepto_pago,
                dto.op_concepto_pag2,
                dto.op_iva,
                Convert.ToInt32(dto.op_ref_numerica),
                dto.op_ref_cobranza,
                dto.op_clave_pago,
                Convert.ToInt32(dto.to_clave),
                Convert.ToInt32(dto.cd_clave),
                dto.op_estado,
                dto.op_cde,
                Convert.ToInt32(dto.op_fecha_cap),
                Convert.ToInt32(dto.op_hora_cap),
                dto.op_tipo_orden,
                Convert.ToInt32(dto.op_prioridad),
                dto.usu_clave,
                Convert.ToInt32(dto.op_folio_servidor),
                string.IsNullOrWhiteSpace(razonRechazo) ? dto.op_razon_rechazo : razonRechazo,
                dto.op_usu_autoriza,
                Convert.ToInt32(dto.op_hora_liq_bm),
                Convert.ToInt64(dto.op_hora_liq_sist),
                nfEstatus,
                folioUnico,
                t24CustomerCode,
                t24CompanyName,
                t24BenAcctNo,
                bancoBeneficiario,
                Convert.ToInt32(_configuracion.StrFechaSistema)
            );
        }

        private async Task<bool> TOMI_ENTRADA_SALIDA(
            string folio,
            bool esTipo31,
            string t24CustomerCode,
            string t24AccountTitle1,
            string t24Currency,
            string cuentaBTMUM,
            string comentarios)
        {
            var data = await _tomiDao.ObtenerNfEntradaSpeiPorFolio(folio);

            if (data == null)
                return false;

            int numCliente = 24771;
            double cdl = 24771;
            string ordenante = string.Empty;
            string nombreCuenta = "MUFG";
            string cuentaMoneda = "MXN";

            if (!esTipo31)
            {
                int.TryParse(t24CustomerCode ?? "0", out numCliente);
                cdl = numCliente;
                ordenante = t24AccountTitle1 ?? string.Empty;
                nombreCuenta = ordenante;
                cuentaMoneda = t24Currency ?? "MXN";
            }

            double cuentaNumerica = 0;
            double.TryParse(cuentaBTMUM, out cuentaNumerica);

            return await _tomiDao.InsertarEntradaSalidaSrv(
                folioUnico,
                numCliente,
                cdl,
                ordenante,
                198,
                "Entrada  (TTP)",
                "D",
                "Doméstica",
                cuentaNumerica,
                nombreCuenta,
                cuentaMoneda,
                850,
                "TRF RECIBIDA MXN SPEUA",
                data.OpMonto,
                0m,
                comentarios,
                DateTime.ParseExact(_configuracion.StrFechaSistema, "yyyyMMdd", CultureInfo.InvariantCulture),
                DateTime.ParseExact(_configuracion.StrFechaSistema, "yyyyMMdd", CultureInfo.InvariantCulture),
                DateTime.Now.ToLongTimeString(),
                "JEAI",
                0,
                "A",
                0
            );
        }

        private async Task<string> ObtenerFirmaDevolucion(string folio)
        {
            var dataDev = await _speiDao.ObtenerDataDevolucionPorFolio(folio);
            if (dataDev == null)
            {
                return string.Empty;
            }

            var opv = new OrdenPagoValue
            {
                OpFechaOper = (dataDev.OpFechaOper ?? 0).ToString(CultureInfo.InvariantCulture),
                OpCveRastreo = dataDev.OpCveRastreo ?? string.Empty,
                InsClaveOrd = CASFIM_MUFG,
                InsClaveBen = (dataDev.InsClave ?? 0).ToString(CultureInfo.InvariantCulture),
                OpMonto = (dataDev.OpMonto ?? 0).ToString(CultureInfo.InvariantCulture),
                TpPago = 0,
                OpCuentaOrd = string.Empty,
                OpCuentaBen = string.Empty,
                OpCuentaBen2 = string.Empty,
                opCuentaPartOrd = string.Empty,
                opCuentaEmiRem = string.Empty
            };

            return await _comunFXD.ObtieneCampoFirma(opv);
        }

        private static string ObtenerCuentaBtmum(string cuenta)
        {
            if (string.IsNullOrEmpty(cuenta))
            {
                return "0";
            }

            if (cuenta.Length == 18)
            {
                return cuenta.Substring(6, 11);
            }

            if (cuenta.Length == 20)
            {
                return cuenta.Substring(8, 11);
            }

            return cuenta;
        }

        private async Task<string> ResolverCuentaVostroLegacy(string swiftShort, string cuentaBeneficiario)
        {
            string cuentaCatalogo = await _tomiDao.ObtenerCuentaVostro(swiftShort, cuentaBeneficiario);
            if (!string.IsNullOrWhiteSpace(cuentaCatalogo) && !string.Equals(cuentaCatalogo, "0", StringComparison.Ordinal))
                return cuentaCatalogo;

            switch ((swiftShort ?? string.Empty).ToUpperInvariant())
            {
                case "BOFCUS33": return "4500180";
                case "BOTKUS33": return "4500202";
                case "BOTKJPJT": return "4500164";
                case "BOTKGB2L": return "4500253";
                case "MTBCUS33": return "5005256";
                case "BOTKBRSX": return "5007436";
                case "BOTKNL2X": return "5004817";
                case "BOTKUS6L": return "5003870";
                case "BOTKSGSX": return "5003985";
                case "BOTKESMX": return "5006377";
            }

            string cuenta = cuentaBeneficiario ?? string.Empty;
            if (cuenta.IndexOf("4500180", StringComparison.Ordinal) >= 0) return "4500180";
            if (cuenta.IndexOf("4500202", StringComparison.Ordinal) >= 0) return "4500202";
            if (cuenta.IndexOf("4500164", StringComparison.Ordinal) >= 0) return "4500164";
            if (cuenta.IndexOf("4500253", StringComparison.Ordinal) >= 0) return "4500253";
            if (cuenta.IndexOf("4500261", StringComparison.Ordinal) >= 0) return "4500261";

            return "0";
        }

        private static bool EsValidoRfcCurpSpei(string texto)
        {
            string valor = (texto ?? string.Empty).Trim();

            if (valor == "ND")
            {
                return true;
            }

            if (Regex.IsMatch(valor, "^[A-Z&Ñ]{3,4}[0-9]{6}[A-Z0-9]{3}$", RegexOptions.CultureInvariant))
            {
                return true;
            }

            if (Regex.IsMatch(valor, "^[A-Z][AEIOUX][A-Z]{2}[0-9]{6}[HM][A-Z]{5}[A-Z0-9][0-9]$", RegexOptions.CultureInvariant))
            {
                return true;
            }

            return false;
        }
    }
}

using Aplicacion.DTOs.ENT_SPID;
using Aplicacion.Interfaces;
using Aplicacion.Interfaces.ENT_SPID;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Modelos;
using Aplicacion.Servicios.FirmaElectronica;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Security.Principal;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;


namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa ENTSPID.
    /// </summary>
    public class ENTSPID
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IENTSPIDTomiDao _tomiDao;
        private readonly IENTSPIDSpidDao _spidDao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IT24CuentaWebService _t24CuentaWebService;
        private readonly ComunSPID _comunSPID;
        private readonly IComunFXD _comunFXD;

        private const string tipoOperacion = "ENT_SPID";
        private const string MonedaSPID = "USD";
        private const string CASFIM_MUFG = "40108";

        private string folioUnico = string.Empty;

        private Dictionary<decimal, string> participantesSpid = new Dictionary<decimal, string>();

        private string cuentaBtmum = "0";
        private string cuentaBeneficiario = string.Empty;
        private string cuentaOrdenante = string.Empty;
        private string comentarios = string.Empty;
        private string razonRechazo = string.Empty;

        private decimal spidOpClave;
        private decimal spidFechaOper;
        private decimal spidOpFolio;
        private string spidCveRastreo = string.Empty;
        private decimal spidInsClave;
        private string spidInsNombre = string.Empty;
        private string spidFirCuentaOrd = string.Empty;
        private string spidFirCuentaBen = string.Empty;
        private string spidFirCuentaBen2 = string.Empty;
        private string spidFirDigital = string.Empty;
        private decimal spidImporte;

        private decimal tipoOperacionSpid;
        private decimal? tipoClaveBen;

        private bool t24Encontrado;
        private bool t24CuentaValida;
        private bool rfcCurpValido;
        private bool t24MonedaValida;
        private bool t24CuentaNoExiste;
        private bool t24CuentaCerrada;
        private bool t24CuentaBloqueada;
        private bool fielValido;
        private bool firmaValida;

        private DataTable t24DatosCliente;
        /// <summary>
        /// Inicializa una nueva instancia de la clase ENTSPID.
        /// </summary>

        public ENTSPID(ConfiguracionModelo configuracion, IControlServicio controlServicio, IENTSPIDTomiDao tomiDao, IENTSPIDSpidDao spidDao, ConexionT24Mw vwT24, IT24CuentaWebService t24CuentaWebService, ComunSPID comunSPID, IComunFXD comunFXD)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tomiDao = tomiDao;
            _spidDao = spidDao;
            _vwT24 = vwT24;
            _t24CuentaWebService = t24CuentaWebService;
            _comunSPID = comunSPID;
            _comunFXD = comunFXD;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            string moneda = string.Empty;
            string subtipo = string.Empty;

            bool conDatoSpid = false;
            bool conEntradaSalida = true;
            bool procesado;
            bool resultado;
            bool esInterbancarioBtmum = false;
            bool devolucion = false;
            bool tipoVostro = false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Iniciando");

            _configuracion.AliasProceso =
                await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

            if (string.IsNullOrEmpty(_configuracion.AliasProceso))
                return false;
            /// <summary>
            /// Ejecuta la operaciÃ³n InicializarVariables.
            /// </summary>

            InicializarVariables();

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo lista de participantes de SPID.");

            var participantes = await _spidDao.ObtenerParticipantesSpid();

            participantesSpid = participantes
                .GroupBy(x => x.InsClave)
                .ToDictionary(x => x.Key, x => x.First().InsNombre);

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo los datos de la operación en base SPID.");

            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(_configuracion.StrFechaSistema),
                folio);

            var operaciones = await _spidDao
                .ObtenerOperacionesRecibidasPorFechaOperacion(Convert.ToDecimal(_configuracion.StrFechaSistema));

            var operacion = operaciones
                .FirstOrDefault(x => x.OpClave.ToString() == folio);

            if (operacion == null)
                throw new Exception($"No se encontró información del folio (op_clave) {folio} en los datos de entrada del SPID");

            conDatoSpid = true;

            fechaEjecucion = operacion.OpFechaOper.ToString();
            moneda = MonedaSPID;
            spidImporte = operacion.OpMonto;

            comentarios = string.Empty;

            if (!string.IsNullOrEmpty(operacion.OpConceptoPago))
                comentarios += operacion.OpConceptoPago;

            if (!string.IsNullOrEmpty(operacion.OpConceptoPag2))
                comentarios += operacion.OpConceptoPag2;

            tipoOperacionSpid = operacion.TpClave;
            tipoClaveBen = operacion.TcClaveBen;

            if ((tipoOperacionSpid == 1 || tipoOperacionSpid == 2) && tipoClaveBen != 10)
            {
                subtipo = string.Empty;

                cuentaBeneficiario = operacion.OpCuentaBen ?? string.Empty;
                cuentaBtmum = ObtenerCuentaBtmumDesdeCuenta(cuentaBeneficiario);

                cuentaOrdenante = DameTexto(operacion.OpCuentaOrd);

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Validando si la cuenta del ordenante es BTMUM de bancos. CuentaOrdenante='{cuentaOrdenante}'.");

                esInterbancarioBtmum = false;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Es una cuenta de bancos de BTMUM={esInterbancarioBtmum}.");
            }
            else if (tipoOperacionSpid == 0 || tipoClaveBen == 10)
            {
                subtipo = "DEVOLUCION";
                devolucion = true;

                if (string.IsNullOrEmpty(operacion.OpCuentaOrd))
                {
                    cuentaBeneficiario = string.Empty;
                    cuentaBtmum = "0";
                    esInterbancarioBtmum = true;
                    subtipo = "Part.-Vostro";

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Devolucion, SIN CUENTA.");
                }
                else
                {
                    cuentaBeneficiario = operacion.OpCuentaOrd;
                    cuentaBtmum = ObtenerCuentaBtmumDesdeCuenta(cuentaBeneficiario);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Devolucion, cuenta='{cuentaBeneficiario}',CuentaBTMUM={cuentaBtmum}.");
                }
            }
            else
            {
                throw new Exception($"Tipo de operación de entrada SPID desconocido, tipo={tipoOperacionSpid}");
            }

            cuentaBtmum = Convert.ToInt64(cuentaBtmum).ToString();

            spidOpClave = operacion.OpClave;
            spidFechaOper = operacion.OpFechaOper;
            spidOpFolio = operacion.OpFolio;
            spidCveRastreo = operacion.OpCveRastreo;
            spidInsClave = operacion.InsClave;

            spidFirCuentaOrd = operacion.OpCuentaOrd ?? string.Empty;
            spidFirCuentaBen = operacion.OpCuentaBen ?? string.Empty;
            spidFirCuentaBen2 = operacion.OpCuentaBen2 ?? string.Empty;
            spidFirDigital = operacion.OpFirmaDig ?? string.Empty;

            if (!conDatoSpid)
                throw new Exception($"No se encontró información del folio (op_clave) {folio} en los datos de entrada del SPID");

            if (!participantesSpid.ContainsKey(spidInsClave))
                throw new Exception($"No se tiene relación con la institución {spidInsClave}");

            spidInsNombre = participantesSpid[spidInsClave];

            OrdenPagoValue ordPagVal = new OrdenPagoValue
            {
                OpFechaOper = fechaEjecucion,
                InsClaveOrd = spidInsClave.ToString(),
                InsClaveBen = CASFIM_MUFG,
                OpCveRastreo = spidCveRastreo,
                OpCuentaOrd = spidFirCuentaOrd,
                OpCuentaBen = spidFirCuentaBen,
                OpCuentaBen2 = spidFirCuentaBen2,
                TpPago = Convert.ToInt32(tipoOperacionSpid),
                OpMonto = spidImporte.ToString()
            };

            if (string.IsNullOrEmpty(spidFirDigital))
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("No se encontró información de la firma digital");

            if (esInterbancarioBtmum)
            {
                conEntradaSalida = false;
            }
            else
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Validando en T24 los datos de la cuenta '{cuentaBtmum}'.");

                string status = string.Empty;
                string currency = string.Empty;
                string category = string.Empty;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Invocando al servicio para obtención de información de la cuenta: {cuentaBtmum}");

                try
                {
                    T24InformacionCuentaDto informacionCuentaT24 =
                        await _t24CuentaWebService.ObtenerInformacionCuenta(cuentaBtmum);

                    status = informacionCuentaT24.Status;
                    currency = informacionCuentaT24.Currency;
                    category = informacionCuentaT24.Category;
                }
                catch (Exception ex)
                {
                    throw new Exception($"Error al realizar la consulta de la cuenta: {ex.Message}", ex);
                }

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Datos obtenidos exitosamente");

                t24DatosCliente = await _vwT24.DatosClienteCuentasVw(cuentaBtmum);

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El servicio regresó un status {status}");
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El servicio regresó moneda {currency}");
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El servicio regresó categoría {category}");

                if (status == "CTA ACTIVA")
                {
                    t24Encontrado = true;

                    if (currency == MonedaSPID)
                    {
                        t24MonedaValida = true;

                        if (category != "1013" && category != "1012")
                        {
                            t24CuentaValida = true;
                        }
                        else
                        {
                            Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|La cuenta seleccionada en el sistema T24 está relacionada a una cuenta interna.");
                            razonRechazo = "La cuenta seleccionada en el sistema T24 está relacionada a una cuenta interna";
                        }
                    }
                    else
                    {
                        Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|La cuenta seleccionada en el sistema T24 está relacionada a una moneda no válida.");
                        razonRechazo = "La cuenta seleccionada en el sistema T24 está relacionada a una moneda no válida";
                    }
                }
                else
                {
                    if (status == "CTA NO EXISTE")
                    {
                        Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|No Existe la cuenta en el sistema T24.");
                        razonRechazo = "No existe la cuenta en el sistema T24";
                        t24CuentaNoExiste = true;
                    }
                    else if (status == "CTA CERRADA")
                    {
                        Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|La cuenta en el sistema T24 esta cerrada.");
                        razonRechazo = "La cuenta en el sistema T24 está cerrada";
                        t24CuentaCerrada = true;
                    }
                    else if (status == "CTA BLOQUEADA")
                    {
                        Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|La cuenta en el sistema T24 esta bloqueada");
                        razonRechazo = "La cuenta en el sistema T24 está bloqueada";
                        t24CuentaBloqueada = true;
                    }
                }

                rfcCurpValido = true;

                if (tipoOperacionSpid == 1)
                {
                    if (!EsValidoRFC_CURP_SPID(operacion.OpRfcCurpOrd))
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|RFC ordenante invalido='{DameTexto(operacion.OpRfcCurpOrd)}'.");
                        rfcCurpValido = false;
                        razonRechazo = "El RFC del ordenante no es válido";
                    }

                    if (!DameTexto(operacion.OpCuentaBen).StartsWith("1081800000"))
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Cuenta CLABE Inválida='{DameTexto(operacion.OpCuentaBen)}'.");
                        t24CuentaValida = false;
                        razonRechazo = "La cuenta CLABE no es válida";
                    }
                }

                if (t24Encontrado && t24CuentaValida)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La cuenta '{cuentaBtmum}' existe en T24 y es en moneda valida SPID.");
                }

                if (t24Encontrado && t24CuentaValida && rfcCurpValido)
                {
                    string errorDetencion = string.Empty;

                    EsClienteSpidValidoResultado result = await _vwT24.EsCuentaSpidValida(cuentaBtmum, errorDetencion);
                    fielValido = result.EsClienteValido;
                    errorDetencion = result.Mensaje;
                    if (!fielValido)
                    {
                        razonRechazo = errorDetencion;
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion| {errorDetencion}");
                    }
                }

                conEntradaSalida = t24Encontrado && t24CuentaValida && rfcCurpValido;
            }

            _configuracion.AliasProceso =
                await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(fechaEjecucion, CultureInfo.InvariantCulture);
            if (fechaEjecucion != _configuracion.StrFechaSistema)
            {
                if (intFechaEjecucion < fechaSistema)
                    await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));

                throw new Exception(
                    $"Se desea realizar una operación al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // DATOS_GRAL
            if (_configuracion.AliasProceso == "DATOS_GRAL")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando DATOS_GRAL");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                bool actualizarDatosCliente =
                    t24Encontrado &&
                    t24CuentaValida &&
                    rfcCurpValido &&
                    fielValido;

                int? numCliente = null;
                string nombreCliente = null;
                string numCuenta = null;

                if (actualizarDatosCliente)
                {
                    DataRow filaClienteT24 = ObtenerFilaT24Requerida(folio, "DATOS_GRAL");
                    numCliente = Convert.ToInt32(filaClienteT24["CUSTOMER"]);
                    nombreCliente = filaClienteT24["NAME_1"].ToString();
                    numCuenta = cuentaBtmum;
                }

                bool actualizado = await _tomiDao.ActualizarDatosPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    Convert.ToDecimal(fechaEjecucion),
                    moneda,
                    spidImporte,
                    subtipo,
                    actualizarDatosCliente,
                    numCliente,
                    nombreCliente,
                    numCuenta);

                if (!actualizado)
                    throw new Exception($"Error en DATOS_GRAL folio {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // SPID_A_TOMI
            if (_configuracion.AliasProceso == "SPID_A_TOMI")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Insertando SPID en TOMI");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (!t24CuentaCerrada && !t24CuentaNoExiste)
                {
                    procesado = false;

                    resultado = await SPID_A_TOMI(folio, operacion);

                    if (resultado)
                    {
                        procesado = true;
                    }
                    else
                    {
                        int count = await _tomiDao.ContarEntradasSpidPorFolio(folio);
                        procesado = count > 0;
                    }

                    if (!procesado)
                        throw new Exception($"Error en procesamiento de registro SPID_A_TOMI con folio: {folio}");

                    if (esInterbancarioBtmum)
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se marca la operación como Aceptada.");

                        procesado = await ResponderEntradaSpid("A", 0);

                        if (!procesado)
                            throw new Exception($"Error en respuesta de la entrada SPID y cuenta CLABE de BTMUM para el folio: {folio}");

                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });

                        await _tomiDao.MarcarEntradaSpidProcesada(folio);

                        await _tomiDao.FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBTMUM(
                            folio,
                            tipoOperacion,
                            subtipo,
                            Convert.ToDecimal(fechaEjecucion),
                            moneda,
                            spidImporte,
                            "Operación de Bancos BTMUM");

                        return true;
                    }
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso =
                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación no continúa con inserción normal en TOMI.");
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso =
                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });
                }
            }

            // TOMI_ENTRADAS/SALIDAS
            if (_configuracion.AliasProceso == "TOMI_ENTRADAS/SALIDAS")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Insertando Entrada/Salida");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (conEntradaSalida)
                {
                    resultado = await TOMI_ENTRADA_SALIDA(folio);

                    if (!resultado)
                    {
                        int count = await _tomiDao.ContarEntradasSalidasPorReferencia(folioUnico);
                        resultado = count > 0;
                    }

                    procesado = resultado;
                }
                else
                {
                    procesado = true;
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación no debe generar Entrada/Salida en TOMI.");
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // CAMBIA_ESTATUS
            if (_configuracion.AliasProceso == "CAMBIA_ESTATUS")
            {
                bool rechazada = false;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando estatus en SPID y TOMI");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                procesado = true;

                if (devolucion || tipoVostro)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación es una DEVOLUCION o VOSTRO y envío su aceptación.");

                    procesado = await ResponderEntradaSpid("A", 0);

                    if (!procesado)
                        throw new Exception($"Error en respuesta de la entrada SPID para el folio: {folio}");

                    if (t24Encontrado)
                    {
                        if (t24CuentaValida)
                        {
                            if (!rfcCurpValido)
                                rechazada = true;
                        }
                        else
                        {
                            rechazada = true;
                        }
                    }
                    else
                    {
                        rechazada = true;
                    }

                    if (rechazada)
                    {
                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });

                        await _tomiDao.FinalizarPlanTrabajoMaestroRecibidaNoAbonada(
                            folio,
                            tipoOperacion);

                        return true;
                    }
                }
                else
                {
                    if (t24Encontrado)
                    {
                        if (t24MonedaValida)
                        {
                            if (t24CuentaValida)
                            {
                                if (rfcCurpValido)
                                {
                                    if (!fielValido)
                                    {
                                        rechazada = true;

                                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se realiza devolución por FIEL invalida.");

                                        procesado = await DevolverEntradaSpid(folio, 11);
                                    }
                                }
                                else
                                {
                                    rechazada = true;

                                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se realiza devolución por RFC invalido.");

                                    procesado = await DevolverEntradaSpid(folio, 12);
                                }
                            }
                            else
                            {
                                rechazada = true;

                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se realiza devolución por cuenta no valida.");

                                procesado = await DevolverEntradaSpid(folio, 1);
                            }
                        }
                        else
                        {
                            rechazada = true;

                            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se realiza devolución por cuenta en otra divisa.");

                            procesado = await DevolverEntradaSpid(folio, 4);
                        }
                    }
                    else
                    {
                        rechazada = true;

                        if (t24CuentaNoExiste)
                        {
                            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se realiza devolución por cuenta inexistente.");
                            procesado = await DevolverEntradaSpid(folio, 1);
                        }
                        else if (t24CuentaBloqueada)
                        {
                            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se realiza devolución por Cuenta bloqueada.");
                            procesado = await DevolverEntradaSpid(folio, 2);
                        }
                        else if (t24CuentaCerrada)
                        {
                            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se realiza devolución por Cuenta cancelada.");
                            procesado = await DevolverEntradaSpid(folio, 3);
                        }
                    }

                    if (!procesado)
                        throw new Exception($"Error en respuesta de la entrada SPID para el folio: {folio}");
                }

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Iniciando validación de firma digital");

                firmaValida = await _comunSPID.ValidaSPIDEntrada(ordPagVal, spidFirDigital);
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La validación de la firma generó como resultado 'FirmaValida' = {firmaValida}");

                if (!firmaValida)
                    throw new Exception($"No se pudo confirmar la autenticación de los datos con la firma eléctronica. Folio {folio}");

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se valida la firma digital de la operación. Se continúa con el proceso");

                if (procesado)
                {
                    if (rechazada)
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A cancelar la operación por devolución.");

                        await _tomiDao.CancelarPlanTrabajoMaestroPorDevolucion(
                            folio,
                            tipoOperacion,
                            razonRechazo,
                            Convert.ToDecimal(fechaEjecucion),
                            moneda,
                            spidImporte);
                    }
                    else
                    {
                        await _tomiDao.MarcarEntradaSpidConfirmadaPendiente(folio);
                    }
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro CAMBIO_ESTATUS con folio: {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // CONCILIAR
            if (_configuracion.AliasProceso == "CONCILIAR")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Conciliación");

                if (!t24CuentaCerrada && !t24CuentaNoExiste)
                {
                    await _controlServicio.IniciaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    int registrosSpid = 1;
                    int registrosTomi = 0;
                    int registrosEntSal = 0;

                    string error = string.Empty;

                    string fechaTomi = string.Empty;
                    string monedaTomi = string.Empty;
                    decimal importeTomi = 0;

                    string fechaEntSal = string.Empty;
                    string monedaEntSal = string.Empty;
                    decimal importeEntSal = 0;

                    var datosTomi = await _tomiDao.ObtenerConciliacionEntradaSpidPorFolio(folio);

                    if (datosTomi != null)
                    {
                        registrosTomi = 1;
                        fechaTomi = datosTomi.OpFechaOper.ToString();
                        monedaTomi = MonedaSPID;
                        importeTomi = datosTomi.OpMonto;
                    }

                    if (conEntradaSalida)
                    {
                        var datosEntSal = await _tomiDao.ObtenerConciliacionEntradaSalidaPorReferencia(folioUnico);

                        if (datosEntSal != null)
                        {
                            registrosEntSal = 1;
                            fechaEntSal = datosEntSal.EntFechaValor.HasValue
                                ? datosEntSal.EntFechaValor.Value.ToString("yyyyMMdd")
                                : string.Empty;

                            monedaEntSal = _vwT24.ValidaMoneda(datosEntSal.EntMonedaCuenta);
                            importeEntSal = datosEntSal.EntImporte;
                        }
                    }

                    if (registrosSpid == 1 &&
                        registrosTomi == 1 &&
                        ((conEntradaSalida && registrosEntSal == 1) || !conEntradaSalida))
                    {
                        if (fechaEjecucion != fechaTomi)
                            error += Environment.NewLine + $" - Diferencia en la fecha de ejecución. SPID='{fechaEjecucion}', TOMI='{fechaTomi}'.";

                        if (moneda != monedaTomi)
                            error += Environment.NewLine + $" - Diferencia en la monedas. SPID='{moneda}', TOMI='{monedaTomi}'.";

                        if (spidImporte != importeTomi)
                            error += Environment.NewLine + $" - Diferencia en el importe. SPID='{spidImporte}', TOMI='{importeTomi}'.";

                        if (conEntradaSalida)
                        {
                            if (fechaEjecucion != fechaEntSal)
                                error += Environment.NewLine + $" - Diferencia en la fecha de ejecución de Entrada/Salida. SPID='{fechaEjecucion}', TOMI='{fechaEntSal}'.";

                            if (moneda != monedaEntSal)
                                error += Environment.NewLine + $" - Diferencia en la monedas de Entrada/Salida. SPID='{moneda}', TOMI='{monedaEntSal}'.";

                            if (spidImporte != importeEntSal)
                                error += Environment.NewLine + $" - Diferencia en el importe de Entrada/Salida. SPID='{spidImporte}', Entrada/Salida='{importeEntSal}'.";
                        }

                        procesado = true;
                    }
                    else
                    {
                        procesado = false;
                        error += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                    }

                    if (!string.IsNullOrEmpty(error))
                        /// <summary>
                        /// Ejecuta la operaciÃ³n Exception.
                        /// </summary>
                        throw new Exception(error);

                    if (!procesado)
                        throw new Exception($"Error en procesamiento de registro CONCILIAR con folio: {folio}");

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación está correctamente conciliada.");
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso =
                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });

                    _configuracion.AliasProceso = string.Empty;
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación no debe ser conciliada.");

                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso = string.Empty;
                }
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Finalizado");

            return true;
        }

        private async Task<bool> SPID_A_TOMI(
            string folio,
            EntSpidOperacionRecibidaDto operacion)
        {
            try
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.SPID_A_TOMI|Iniciando inserción de SPID a TOMI. Folio={folio}");

                DataRow filaClienteT24 = ObtenerFilaT24Requerida(folio, "SPID_A_TOMI");

                var dto = new EntSpidInsertarFormatoTomiDto
                {
                    OpClave = Convert.ToInt64(operacion.OpClave),
                    OpFechaOper = Convert.ToInt32(operacion.OpFechaOper),
                    MeClave = Convert.ToInt32(operacion.MeClave),
                    OpFolio = Convert.ToInt32(operacion.OpFolio),
                    InsClave = Convert.ToInt64(operacion.InsClave),
                    OpMonto = operacion.OpMonto,
                    TpClave = Convert.ToInt32(operacion.TpClave),

                    OpCveRastreo = operacion.OpCveRastreo,

                    OpNomOrd = operacion.OpNomOrd,
                    TcClaveOrd = operacion.TcClaveOrd.HasValue
                        ? Convert.ToInt32(operacion.TcClaveOrd.Value)
                        : (int?)null,
                    OpCuentaOrd = operacion.OpCuentaOrd,
                    OpRfcCurpOrd = operacion.OpRfcCurpOrd,

                    OpNomBen = operacion.OpNomBen,
                    TcClaveBen = operacion.TcClaveBen.HasValue
                        ? Convert.ToInt32(operacion.TcClaveBen.Value)
                        : (int?)null,
                    OpCuentaBen = operacion.OpCuentaBen,
                    OpRfcCurpBen = operacion.OpRfcCurpBen,

                    OpNomBen2 = operacion.OpNomBen2,
                    TcClaveBen2 = operacion.TcClaveBen2.HasValue
                        ? Convert.ToInt32(operacion.TcClaveBen2.Value)
                        : (int?)null,
                    OpCuentaBen2 = operacion.OpCuentaBen2,
                    OpRfcCurpBen2 = operacion.OpRfcCurpBen2,

                    OpConceptoPago = operacion.OpConceptoPago,
                    OpConceptoPag2 = operacion.OpConceptoPag2,
                    OpIva = operacion.OpIva,

                    OpRefNumerica = string.IsNullOrEmpty(operacion.OpRefNumerica)
                        ? string.Empty
                        : operacion.OpRefNumerica.Substring(
                            0,
                            Math.Min(operacion.OpRefNumerica.Length, 20)),

                    OpRefCobranza = operacion.OpRefCobranza,
                    OpClavePago = operacion.OpClavePago,

                    ToClave = operacion.ToClave.HasValue
                        ? Convert.ToInt32(operacion.ToClave.Value)
                        : (int?)null,
                    CdClave = operacion.CdClave.HasValue
                        ? Convert.ToInt32(operacion.CdClave.Value)
                        : (int?)null,

                    OpEstado = operacion.OpEstado,
                    OpCde = operacion.OpCde,

                    OpFechaCap = Convert.ToInt32(operacion.OpFechaCap),
                    OpHoraCap = Convert.ToInt32(operacion.OpHoraCap),

                    OpTipoOrden = operacion.OpTipoOrden,
                    OpPrioridad = Convert.ToInt32(operacion.OpPrioridad),

                    UsuClave = operacion.UsuClave,
                    OpFolioServidor = operacion.OpFolioServidor.HasValue
                        ? Convert.ToInt64(operacion.OpFolioServidor.Value)
                        : (long?)null,

                    OpRazonRechazo = razonRechazo,

                    OpUsuAutoriza = operacion.OpUsuAutoriza,

                    OpHoraLiqBm = operacion.OpHoraLiqBm.HasValue
                        ? Convert.ToInt32(operacion.OpHoraLiqBm.Value)
                        : (int?)null,

                    OpHoraLiqSist = operacion.OpHoraLiqSist.HasValue
                        ? Convert.ToInt64(operacion.OpHoraLiqSist.Value)
                        : (long?)null,

                    NfEstatus = t24Encontrado && t24CuentaValida
                        ? "PEND"
                        : "RECC",

                    NfFolioUnico = folioUnico,

                    T24CustomerCode = filaClienteT24["CUSTOMER"].ToString(),
                    T24CompanyName = filaClienteT24["NAME_1"].ToString(),
                    T24BenAcctNo = cuentaBtmum,

                    NfBancoBenef = spidInsNombre,

                    NfFechaEjecucion = Convert.ToInt32(_configuracion.StrFechaSistema)
                };

                bool resultado = await _tomiDao.InsertarFormatoSpidEnTomi(dto);

                Config.EscribirLog($"INFO|{tipoOperacion}.SPID_A_TOMI|Resultado={resultado}. Folio={folio}");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{tipoOperacion}.SPID_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
                return false;
            }
        }

        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            try
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.TOMI_ENTRADA_SALIDA|Iniciando inserción de Entrada/Salida. Folio={folio}");

                var entradaSpid = await _tomiDao.ObtenerEntradaSpidParaEntradaSalida(folio);

                if (entradaSpid == null)
                    throw new Exception($"No se encontró información en TB_TOMBIN_NF_ENTRADAS_SPID para el folio {folio}");

                DataRow filaClienteT24 = ObtenerFilaT24Requerida(folio, "TOMI_ENTRADA_SALIDA");

                var dto = new EntSpidInsertarEntradaSalidaDto
                {
                    Referencia = folioUnico,

                    NumCliente = Convert.ToInt64(filaClienteT24["CUSTOMER"]),
                    Cdl = Convert.ToInt64(filaClienteT24["CUSTOMER"]),

                    Ordenante = filaClienteT24["ACCOUNT_TITLE_1"].ToString(),

                    Operacion = 198,
                    OperacionDesc = "Entrada  (TTP)",

                    Tipo = "D",
                    TipoDesc = "Doméstica",

                    Cuenta = Convert.ToInt64(cuentaBtmum),
                    Nombre = filaClienteT24["ACCOUNT_TITLE_1"].ToString(),
                    CuentaMoneda = filaClienteT24["CURRENCY"].ToString(),

                    Via = 850,
                    ViaDesc = "TRF RECIBIDA MXN SPEUA",

                    Importe = entradaSpid.OpMonto,
                    Saldo = 0,

                    Comentarios = comentarios,

                    FechaCaptura = ConversionHelper.TextoAFechaT24(_configuracion.StrFechaSistema),
                    FechaValor = ConversionHelper.TextoAFechaT24(_configuracion.StrFechaSistema),

                    Hora = DateTime.Now.ToLongTimeString(),
                    Capturista = "JEAI",

                    Fad = 0,
                    ReferenciaControl = "A",
                    ReferenciaControlCont = 0
                };

                bool resultado = await _tomiDao.InsertarEntradaSalida(dto);

                Config.EscribirLog($"INFO|{tipoOperacion}.TOMI_ENTRADA_SALIDA|Resultado={resultado}. Folio={folio}");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{tipoOperacion}.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}. Trace: {ex}");
                return false;
            }
        }

        private DataRow ObtenerFilaT24Requerida(string folio, string etapa)
        {
            if (t24DatosCliente == null)
                throw new Exception($"Error técnico al consultar datos T24 para folio {folio} en etapa {etapa}.");

            if (t24DatosCliente.Rows.Count == 0)
                throw new Exception($"No se encontraron datos T24 para folio {folio} en etapa {etapa}.");

            return t24DatosCliente.Rows[0];
        }

        private async Task<bool> ResponderEntradaSpid(string claveOperacion, int causaDevolucion)
        {
            try
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.ResponderEntradaSpid|Pasaron los parametros. " +
                    $"opClave={spidOpClave},opFechaOper={spidFechaOper},opFolio={spidOpFolio}," +
                    $"cveRastreo='{spidCveRastreo}',opMonto={spidImporte}," +
                    $"cd_clave={causaDevolucion},op_estatus='{claveOperacion}'.");

                var dto = new EntSpidCambiarEstatusRecibidaDto
                {
                    OpClave = Convert.ToInt64(spidOpClave),
                    OpFechaOper = Convert.ToInt32(spidFechaOper),
                    OpFolio = Convert.ToInt32(spidOpFolio),
                    OpCveRastreo = spidCveRastreo,
                    OpMonto = spidImporte,
                    CausaDevolucion = causaDevolucion,
                    OpEstatus = claveOperacion
                };

                bool resultado = await _spidDao.CambiarEstatusRecibidaSpid(dto);

                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.ResponderEntradaSpid|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|{tipoOperacion}.ResponderEntradaSpid|Se presenta exception: {ex.Message}. Trace: {ex}");

                return false;
            }
        }

        private async Task<bool> DevolverEntradaSpid(string claveOperacion, int causaDevolucion)
        {
            try
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.DevolverEntradaSpid|Obteniendo datos del registro para devolución. " +
                    $"op_clave={claveOperacion}, cd_clave={causaDevolucion}.");

                var datosDevolucion = await _spidDao.ObtenerDatosDevolucionSpid(claveOperacion);

                if (datosDevolucion == null)
                    throw new Exception($"No se obtuvieron datos para la devolución SPID del folio {claveOperacion}");

                var ordenPago = new OrdenPagoValue
                {
                    OpFechaOper = datosDevolucion.OpFechaOper.ToString(),
                    OpCveRastreo = datosDevolucion.OpCveRastreo,
                    InsClaveBen = datosDevolucion.InsClave.ToString(),
                    OpMonto = datosDevolucion.OpMonto.ToString(),
                    InsClaveOrd = CASFIM_MUFG,
                    TpPago = 0
                };

                string firmaDigital = await _comunFXD.ObtieneCampoFirma(ordenPago);

                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.DevolverEntradaSpid|Se obtuvieron los datos del registro para la devolución.");

                var dto = new EntSpidDevolverEntradaDto
                {
                    OpClave = Convert.ToInt64(claveOperacion),
                    CausaDevolucion = causaDevolucion,
                    OpFirmaDigital = firmaDigital
                };

                bool resultado = await _spidDao.DevolverEntradaSpid(dto);

                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.DevolverEntradaSpid|StoreProcedure ejecutado correctamente, " +
                    $"entrada SPID devuelta. op_clave={claveOperacion}, cd_clave={causaDevolucion}, resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|{tipoOperacion}.DevolverEntradaSpid|Se presenta exception: {ex.Message}. Trace: {ex}");

                return false;
            }
        }

        private bool EsValidoRFC_CURP_SPID(object texto)
        {
            string valor = DameTexto(texto).Trim();

            if (valor == "ND")
                return true;

            if (EsRFC_Local(valor))
                return true;

            return false;
        }

        private bool EsRFC_Local(string rfc)
        {
            const string expresionRegular =
                "^[A-Z,Ñ,&]{3,4}([0-9]{2})(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-9]|3[0-1])[A-Z,0-9]?[A-Z,0-9]?[0-9,A-Z]?$";

            var regex = new System.Text.RegularExpressions.Regex(expresionRegular);

            return regex.IsMatch(rfc);
        }



        private void InicializarVariables()
        {
            cuentaBtmum = "0";
            cuentaBeneficiario = string.Empty;
            cuentaOrdenante = string.Empty;
            comentarios = string.Empty;
            razonRechazo = string.Empty;

            spidImporte = 0;
            spidFirCuentaOrd = string.Empty;
            spidFirCuentaBen = string.Empty;
            spidFirCuentaBen2 = string.Empty;
            spidFirDigital = string.Empty;

            t24Encontrado = false;
            t24CuentaValida = false;
            rfcCurpValido = false;
            t24MonedaValida = false;
            t24CuentaNoExiste = false;
            t24CuentaCerrada = false;
            t24CuentaBloqueada = false;
            fielValido = false;
            firmaValida = false;
        }

        private string ObtenerCuentaBtmumDesdeCuenta(string cuenta)
        {
            if (string.IsNullOrEmpty(cuenta))
                return "0";

            if (cuenta.Length == 18)
                return cuenta.Substring(6, 11);

            if (cuenta.Length == 20)
                return cuenta.Substring(8, 11);

            return "0";
        }

    }
}

using Aplicacion.DTOs.ENT_COAS;
using Aplicacion.Interfaces.ENT_COAS;
using Aplicacion.Modelos;
using Infraestructura.Repositorio.ENT_COAS;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    public class ENT_COAS
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IENTCOASTomiDao _tomiDao;
        private readonly IENTCOASSpeiDao _speiDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "ENT_COAS";

        private string folioUnico = string.Empty;

        private decimal cuentaBtmum = 0;
        private string cuentaBeneficiario = string.Empty;
        private string cuentaOrdenante = string.Empty;
        private string codigoSwift = string.Empty;
        private string codigoSwiftShort = string.Empty;
        private string comentarios = string.Empty;
        private string razonRechazo = string.Empty;

        private decimal speiImporte = 0;
        private decimal speiOpClave = 0;
        private decimal speiFechaOper = 0;
        private decimal speiOpFolio = 0;
        private decimal speiInsClave = 0;
        private string speiCveRastreo = string.Empty;
        private string speiInsNombre = string.Empty;
        private decimal causaDevolucionSpei = 0;

        private decimal tipoOperacionSpei = 0;
        private decimal tipoClaveBen = 0;

        private bool t24Encontrado = false;
        private bool t24CuentaValida = false;
        private bool rfcCurpValido = false;

        private DataTable datosClienteT24;

        private readonly List<string> btmumClabes = new List<string>() { "00012180004471781901", "00002180019710381753", "00002180087000493707" };

        public ENT_COAS(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IENTCOASTomiDao tomiDao,
            IENTCOASSpeiDao speiDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tomiDao = tomiDao;
            _speiDao = speiDao;
            _vwT24 = vwT24;
        }

        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            string moneda = string.Empty;
            string subtipo = string.Empty;

            bool conDatoSpei = false;
            bool conEntradaSalida = true;
            bool procesado;
            bool resultado;
            bool esInterbancarioBtmum = false;
            bool devolucion = false;
            bool tipoVostro = false;

            decimal causaDev = 0;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Iniciando");

            InicializarVariablesOperacion();

            _configuracion.AliasProceso =
                await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

            if (string.IsNullOrEmpty(_configuracion.AliasProceso))
                return false;

            var participantes = await _speiDao.ObtenerParticipantesSpei();

            var participantesDic = participantes
                .GroupBy(x => x.InsClave)
                .ToDictionary(x => x.Key, x => x.First().InsNombre);

            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(_configuracion.StrFechaSistema),
                folio);

            var orden = await _tomiDao.ObtenerOrdenPagoRecibidaPorFolio(
                folio,
                Convert.ToInt32(_configuracion.StrFechaSistema));

            if (orden != null && orden.OpClave == Convert.ToDecimal(folio))
            {
                conDatoSpei = true;

                fechaEjecucion = orden.OpFechaOper?.ToString("0") ?? string.Empty;
                moneda = _vwT24.MonedaDefaultMN;

                speiImporte = orden.OpMonto ?? 0;
                comentarios = $"{DameTexto(orden.OpConceptoPago)}{DameTexto(orden.OpConceptoPag2)}";

                tipoOperacionSpei = orden.TpClave ?? 0;
                tipoClaveBen = orden.TcClaveBen ?? 0;

                if ((tipoOperacionSpei == 1 || tipoOperacionSpei == 5) && tipoClaveBen != 10)
                {
                    subtipo = string.Empty;

                    cuentaBeneficiario = DameTexto(orden.OpCuentaBen);

                    if (cuentaBeneficiario.Length == 18)
                        cuentaBtmum = Convert.ToDecimal(cuentaBeneficiario.Substring(6, 11));
                    else if (cuentaBeneficiario.Length == 20)
                        cuentaBtmum = Convert.ToDecimal(cuentaBeneficiario.Substring(8, 11));

                    cuentaOrdenante = DameTexto(orden.OpCuentaOrd);

                    Config.EscribirLog(
                        $"INFO|{tipoOperacion}.procesaOperacion|Validando si la cuenta del ordenante es BTMUM de bancos. CuentaOrdenante='{cuentaOrdenante}'.");

                    esInterbancarioBtmum = false;

                    foreach (string clabe in btmumClabes)
                    {
                        if (clabe == cuentaOrdenante)
                        {
                            esInterbancarioBtmum = true;
                            subtipo = "BBC";
                            break;
                        }
                    }

                    Config.EscribirLog(
                        $"INFO|{tipoOperacion}.procesaOperacion|Es una cuenta de bancos de BTMUM={esInterbancarioBtmum}.");
                }
                else if (tipoOperacionSpei == 6 || tipoOperacionSpei == 3)
                {
                    subtipo = "VOSTRO";
                    tipoVostro = true;

                    codigoSwift = DameTexto(orden.OpNomBen);
                    cuentaBtmum = 0;

                    if (codigoSwift.Length >= 8)
                    {
                        codigoSwiftShort = codigoSwift.Substring(0, 8);

                        if (codigoSwiftShort == "BOFCUS33")
                            cuentaBtmum = 4500180;
                        else if (codigoSwiftShort == "BOTKUS33")
                            cuentaBtmum = 4500202;
                        else if (codigoSwiftShort == "BOTKJPJT")
                            cuentaBtmum = 4500164;
                        else if (codigoSwiftShort == "BOTKGB2L")
                            cuentaBtmum = 4500253;
                        else if (codigoSwiftShort == "MTBCUS33")
                            cuentaBtmum = 5005256;
                        else if (codigoSwiftShort == "BOTKBRSX")
                            cuentaBtmum = 5007436;
                        else if (codigoSwiftShort == "BOTKNL2X")
                            cuentaBtmum = 5004817;
                        else if (codigoSwiftShort == "BOTKUS6L")
                            cuentaBtmum = 5003870;
                        else if (codigoSwiftShort == "BOTKSGSX")
                            cuentaBtmum = 5003985;
                        else if (codigoSwiftShort == "BOTKESMX")
                            cuentaBtmum = 5006377;
                        else
                            cuentaBtmum = 0;
                    }

                    if (cuentaBtmum == 0)
                    {
                        cuentaBeneficiario = DameTexto(orden.OpCuentaBen);

                        if (cuentaBeneficiario.IndexOf("4500180", StringComparison.OrdinalIgnoreCase) >= 0)
                            cuentaBtmum = 4500180;
                        else if (cuentaBeneficiario.IndexOf("4500202", StringComparison.OrdinalIgnoreCase) >= 0)
                            cuentaBtmum = 4500202;
                        else if (cuentaBeneficiario.IndexOf("4500164", StringComparison.OrdinalIgnoreCase) >= 0)
                            cuentaBtmum = 4500164;
                        else if (cuentaBeneficiario.IndexOf("4500253", StringComparison.OrdinalIgnoreCase) >= 0)
                            cuentaBtmum = 4500253;
                        else if (cuentaBeneficiario.IndexOf("4500261", StringComparison.OrdinalIgnoreCase) >= 0)
                            cuentaBtmum = 4500261;
                    }
                }
                else if (tipoOperacionSpei == 0 || tipoClaveBen == 10)
                {
                    causaDevolucionSpei = orden.CdClave ?? 0;
                }
                else
                {
                    throw new Exception($"Tipo de operación de entrada SPEI desconocido, tipo={tipoOperacionSpei}");
                }

                cuentaBtmum = Convert.ToDecimal(Convert.ToInt64(cuentaBtmum));

                speiOpClave = orden.OpClave ?? 0;
                speiFechaOper = orden.OpFechaOper ?? 0;
                speiOpFolio = orden.OpFolio ?? 0;
                speiCveRastreo = orden.OpCveRastreo;
                speiInsClave = orden.InsClave ?? 0;
            }

            if (tipoOperacionSpei == 0 || tipoClaveBen == 10)
            {
                subtipo = "DEVOLUCION";
                devolucion = true;

                await _tomiDao.MarcarOrdenEmitidaComoDevuelta(
                    Convert.ToInt32(speiFechaOper),
                    Convert.ToInt32(speiInsClave),
                    speiImporte,
                    speiCveRastreo,
                    Convert.ToInt32(causaDevolucionSpei));

                var ordenDevuelta =
                    await _tomiDao.ObtenerOrdenEmitidaDevueltaParaValidacion(
                        Convert.ToInt32(speiFechaOper),
                        Convert.ToInt32(speiInsClave),
                        speiImporte,
                        speiCveRastreo);

                if (ordenDevuelta != null)
                {
                    conDatoSpei = true;

                    orden = AplicarCamposDevolucionSobreOrdenOriginal(orden, ordenDevuelta);

                    if (string.IsNullOrEmpty(ordenDevuelta.OpCuentaOrd))
                    {
                        cuentaBeneficiario = string.Empty;
                        cuentaBtmum = 0;
                        esInterbancarioBtmum = true;
                        subtipo = "Part.-Vostro";

                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Devolucion, SIN CUENTA.");
                    }
                    else
                    {
                        cuentaBeneficiario = ordenDevuelta.OpCuentaOrd;

                        if (cuentaBeneficiario.Length == 18)
                            cuentaBtmum = Convert.ToDecimal(cuentaBeneficiario.Substring(6, 11));
                        else if (cuentaBeneficiario.Length == 20)
                            cuentaBtmum = Convert.ToDecimal(cuentaBeneficiario.Substring(8, 11));

                        Config.EscribirLog(
                            $"INFO|{tipoOperacion}.procesaOperacion|Devolucion, cuenta='{cuentaBeneficiario}',CuentaBTMUM={cuentaBtmum}.");
                    }
                }
                else
                {
                    conDatoSpei = false;
                }

                cuentaBtmum = Convert.ToDecimal(Convert.ToInt64(cuentaBtmum));
            }

            if (!conDatoSpei)
                throw new Exception($"No se encontró información de la folio (op_clave) {folio} en los datos de entrada del SPEI");

            if (!participantesDic.ContainsKey(speiInsClave))
                throw new Exception($"No se tiene relación con la institución {speiInsClave}");

            speiInsNombre = participantesDic[speiInsClave];

            if (esInterbancarioBtmum)
            {
                conEntradaSalida = false;
                t24Encontrado = false;
                t24CuentaValida = false;
                rfcCurpValido = false;
            }
            else
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Validando en T24 los datos de la cuenta '{cuentaBtmum}'.");

                t24Encontrado = false;
                t24CuentaValida = false;
                rfcCurpValido = false;

                datosClienteT24 = await _vwT24.DatosClienteCuentasVw(cuentaBtmum.ToString());

                DataRow filaClienteT24 = ObtenerFilaT24(folio, "VALIDACION_T24", false);

                if (filaClienteT24 != null)
                {
                    if (filaClienteT24["CUSTOMER"].ToString() != string.Empty)
                    {
                        t24Encontrado = true;

                        if (filaClienteT24["CURRENCY"].ToString() == _vwT24.MonedaDefaultMN)
                        {
                            t24CuentaValida = true;
                        }
                        else
                        {
                            Config.EscribirLog(
                                $"ALERTA|{tipoOperacion}.procesaOperacion|La cuenta seleccionada en el sistema T24 está relacionada a una moneda no válida.");

                            razonRechazo = "La cuenta seleccionada en el sistema T24 esta relacionada a una moneda no valida";
                        }
                    }
                    else
                    {
                        Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|No Existe la cuenta en el sistema T24.");
                        razonRechazo = "No existe la cuenta en el sistema T24";
                    }
                }
                else
                {
                    t24Encontrado = false;
                    t24CuentaValida = false;
                    razonRechazo = "No existe la cuenta en el sistema T24";

                    Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|No Existe el cliente en el sistema T24.");
                }

                rfcCurpValido = true;

                if (tipoOperacionSpei == 1)
                {
                    if (!EsValidoRFC_CURP_SPEI(orden.OpRfcCurpOrd))
                    {
                        Config.EscribirLog(
                            $"INFO|{tipoOperacion}.procesaOperacion|RFC ordenante invalido='{DameTexto(orden.OpRfcCurpOrd)}'.");

                        rfcCurpValido = false;
                    }
                }

                if (t24Encontrado && t24CuentaValida)
                {
                    Config.EscribirLog(
                        $"INFO|{tipoOperacion}.procesaOperacion|La cuenta '{cuentaBtmum}' existe en T24 y es en moneda nacional.");
                }
                else
                {
                    causaDev = 1;
                    procesado = true;

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A cancelar la operación por cuenta o cliente inexistente.");

                    await _tomiDao.CancelarPlanTrabajoMaestroDevuelto(
                        folio,
                        tipoOperacion,
                        razonRechazo,
                        fechaEjecucion,
                        moneda,
                        speiImporte);

                    await _tomiDao.MarcarOrdenRecibidaDevueltaPorFolio(folio, Convert.ToInt32(causaDev));

                    await _tomiDao.InsertarDevolucionCoasDesdeOrdenRecibida(
                        folio,
                        Convert.ToInt32(_configuracion.StrFechaSistema),
                        Convert.ToInt32(causaDev));

                    if (!procesado)
                        throw new Exception($"Error en procesamiento de registro con folio: {folio}");

                    await _controlServicio.TerminaProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });


                    _configuracion.AliasProceso =

                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO

                        {

                            Folio = folio,

                            TipoOperacion = tipoOperacion,

                            Servicio = _configuracion.NombreServicio,

                            NombreProceso = _configuracion.NombreProceso,

                            AliasProceso = _configuracion.AliasProceso,

                            PId = _configuracion.Pid.Value

                        });
                }

                conEntradaSalida = t24Encontrado && t24CuentaValida && rfcCurpValido;
            }

            _configuracion.AliasProceso =
                await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(fechaEjecucion, CultureInfo.InvariantCulture);
            if (fechaEjecucion != _configuracion.StrFechaSistema)
            {
                if (intFechaEjecucion < fechaSistema)
                {
                    await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                }

                throw new Exception(
                    $"Se desea realizar una operación al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // DATOS_GRAL
            if (_configuracion.AliasProceso == "DATOS_GRAL")
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Actualizando DATOS_GRAL. T24_Encontrado={t24Encontrado},T24_CuentaValida={t24CuentaValida},RFC_CURP_Valido={rfcCurpValido},FechaEjecucion={fechaEjecucion},esInterbancario={esInterbancarioBtmum},cve_moneda='{moneda}',importe={speiImporte}.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                int? numCliente = null;
                string nombreCliente = null;
                string numCuenta = null;

                if (t24Encontrado && t24CuentaValida && rfcCurpValido)
                {
                    DataRow filaClienteT24 = ObtenerFilaT24(folio, "DATOS_GRAL", true);
                    numCliente = Convert.ToInt32(filaClienteT24["CUSTOMER"].ToString());
                    nombreCliente = filaClienteT24["NAME_1"].ToString();
                    numCuenta = cuentaBtmum.ToString("0");
                }

                bool actualizado =
                    await _tomiDao.ActualizarPlanTrabajoMaestroDatosGenerales(
                        folio,
                        tipoOperacion,
                        folioUnico,
                       fechaEjecucion,
                        moneda,
                        speiImporte,
                        subtipo,
                        numCliente,
                        nombreCliente,
                        numCuenta);

                if (!actualizado)
                    throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");

                await _controlServicio.TerminaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });


                _configuracion.AliasProceso =

                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });
            }

            // COAS_A_TOMI
            if (_configuracion.AliasProceso == "COAS_A_TOMI")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A insertar datos de COAS a TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                resultado = await SPEI_A_TOMI(folio, orden);

                if (resultado)
                {
                    procesado = true;
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                }
                else
                {
                    int count = await _tomiDao.ContarEntradasSpeiPorFolio(folio);

                    procesado = count > 0;

                    if (procesado)
                    {
                        Config.EscribirLog(
                            $"INFO|{tipoOperacion}.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro COAS_A_TOMI con folio: {folio}");

                if (esInterbancarioBtmum)
                {
                    try
                    {
                        await _tomiDao.MarcarOrdenRecibidaAceptada(folio);
                        procesado = true;
                    }
                    catch
                    {
                        throw new Exception($"Error en respuesta de la entrada SPEI y cuenta CLABE de BTMUM para el folio: {folio}");
                    }

                    await _controlServicio.TerminaProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });


                    _configuracion.AliasProceso =

                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO

                        {

                            Folio = folio,

                            TipoOperacion = tipoOperacion,

                            Servicio = _configuracion.NombreServicio,

                            NombreProceso = _configuracion.NombreProceso,

                            AliasProceso = _configuracion.AliasProceso,

                            PId = _configuracion.Pid.Value

                        });

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A terminar la operacion por ser cuenta BTMUM pero de Bancos.");

                    await _tomiDao.MarcarEntradaSpeiProcesada(folio);

                    if (subtipo == "BBC")
                    {
                        await _tomiDao.FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBbc(
                            folio,
                            tipoOperacion,
                            subtipo,
                            fechaEjecucion,
                            moneda,
                            speiImporte,
                            "Operación de Bancos BTMUM");

                        await _tomiDao.FinalizarDetallePlanTrabajoExceptoCep(folio, tipoOperacion);
                    }
                    else
                    {
                        await _tomiDao.FinalizarPlanTrabajoMaestroRecibidaNoAbonada(
                            folio,
                            tipoOperacion,
                            subtipo,
                            fechaEjecucion,
                            moneda,
                            speiImporte,
                            "Operación de Bancos BTMUM");
                    }

                    return true;
                }

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A marcar el proceso como terminado.");

                await _controlServicio.TerminaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });


                _configuracion.AliasProceso =

                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });
            }

            // TOMI_ENTRADAS/SALIDAS
            if (_configuracion.AliasProceso == "TOMI_ENTRADAS/SALIDAS")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A insertar datos de Entrada/Salida en TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (conEntradaSalida)
                {
                    resultado = await TOMI_ENTRADA_SALIDA(folio);

                    if (!resultado)
                    {
                        int count = await _tomiDao.ContarEntradasSalidasPorFolioUnico(folioUnico);
                        resultado = count > 0;

                        if (resultado)
                        {
                            Config.EscribirLog(
                                $"INFO|{tipoOperacion}.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                        }
                    }

                    if (!resultado)
                        throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio en base de Entrada/Salida.");
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación no debe generar Entrada/Salida en TOMI.");
                }

                await _controlServicio.TerminaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });


                _configuracion.AliasProceso =

                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });
            }

            // CAMBIA_ESTATUS
            if (_configuracion.AliasProceso == "CAMBIA_ESTATUS")
            {
                bool rechazada = false;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A actualizar estatus en SPEI y TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                procesado = true;

                if (devolucion || tipoVostro)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación es una DEVOLUCION o VOSTRO y envio su aceptación.");

                    await _tomiDao.MarcarOrdenRecibidaAceptada(folio);

                    if (t24Encontrado)
                    {
                        if (t24CuentaValida)
                        {
                            if (!rfcCurpValido)
                                rechazada = true;
                        }
                        else
                        {
                            rechazada = true;
                        }
                    }
                    else
                    {
                        rechazada = true;
                    }

                    if (rechazada)
                    {
                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });

                        _configuracion.AliasProceso =
                            await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                            {
                                Folio = folio,
                                TipoOperacion = tipoOperacion,
                                Servicio = _configuracion.NombreServicio,
                                NombreProceso = _configuracion.NombreProceso,
                                AliasProceso = _configuracion.AliasProceso,
                                PId = _configuracion.Pid.Value
                            });

                        await _tomiDao.ActualizarPlanTrabajoMaestroRecibidaNoAbonada(
                            folio,
                            tipoOperacion);

                        return true;
                    }
                }
                else
                {
                    if (t24Encontrado)
                    {
                        if (t24CuentaValida)
                        {
                            if (rfcCurpValido)
                            {
                                await _tomiDao.MarcarOrdenRecibidaAceptada(folio);
                            }
                            else
                            {
                                rechazada = true;
                                causaDev = 14;

                                await _tomiDao.MarcarOrdenRecibidaRechazada(folio, Convert.ToInt32(causaDev));
                            }
                        }
                        else
                        {
                            rechazada = true;
                            causaDev = 5;

                            await _tomiDao.MarcarOrdenRecibidaRechazada(folio, Convert.ToInt32(causaDev));
                        }
                    }
                    else
                    {
                        rechazada = true;
                        causaDev = 1;

                        await _tomiDao.MarcarOrdenRecibidaRechazada(folio, Convert.ToInt32(causaDev));
                    }
                }

                if (procesado)
                {
                    if (rechazada)
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A cancelar la operación por devolución.");

                        await _tomiDao.CancelarPlanTrabajoMaestroDevuelto(
                            folio,
                            tipoOperacion,
                            razonRechazo,
                            fechaEjecucion,
                            moneda,
                            speiImporte);

                        await _tomiDao.InsertarDevolucionCoasDesdeOrdenRecibida(
                            folio,
                            Convert.ToInt32(_configuracion.StrFechaSistema),
                            Convert.ToInt32(causaDev));

                        await _tomiDao.MarcarEntradaSpeiRechazada(folio, Convert.ToInt32(causaDev));
                    }
                    else
                    {
                        await _tomiDao.MarcarEntradaSpeiConfirmadaPendiente(folio);
                    }
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro CAMBIO_ESTATUS con folio: {folio}");

                await _controlServicio.TerminaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });


                _configuracion.AliasProceso =

                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });
            }

            // CONCILIAR
            if (_configuracion.AliasProceso == "CONCILIAR")
            {
                string error = string.Empty;

                string fechaEjecucionTomi = string.Empty;
                string monedaTomi = string.Empty;
                decimal importeTomi = 0;

                string fechaEjecucionEntSal = string.Empty;
                string monedaEntSal = string.Empty;
                decimal importeEntSal = 0;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A conciliar operaciones entre SPEI y TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var datosTomi = await _tomiDao.ObtenerConciliacionTomiPorFolio(folio);

                if (datosTomi == null)
                    error += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                else
                {
                    fechaEjecucionTomi = datosTomi.OpFechaOper?.ToString("0") ?? string.Empty;
                    monedaTomi = _vwT24.MonedaDefaultMN;
                    importeTomi = datosTomi.OpMonto ?? 0;

                    if (fechaEjecucion != fechaEjecucionTomi)
                        error += Environment.NewLine + $" - Diferencia en la fecha de ejecución. SPEI='{fechaEjecucion}', TOMI='{fechaEjecucionTomi}'.";

                    if (moneda != monedaTomi)
                        error += Environment.NewLine + $" - Diferencia en la monedas. SPEI='{moneda}', TOMI='{monedaTomi}'.";

                    if (speiImporte != importeTomi)
                        error += Environment.NewLine + $" - Diferencia en el importe. SPEI='{speiImporte}', TOMI='{importeTomi}'.";
                }

                if (conEntradaSalida)
                {
                    var datosEntSal =
                        await _tomiDao.ObtenerConciliacionEntradaSalidaPorReferencia(folioUnico);

                    if (datosEntSal == null)
                        error += Environment.NewLine + " - No se ha registrado la operación en Entrada/Salida.";
                    else
                    {
                        fechaEjecucionEntSal =
                            datosEntSal.EntFechaValor.HasValue
                                ? datosEntSal.EntFechaValor.Value.ToString("yyyyMMdd")
                                : string.Empty;

                        monedaEntSal = _vwT24.ValidaMoneda(datosEntSal.EntMonedaCuenta);
                        importeEntSal = datosEntSal.EntImporte ?? 0;

                        if (fechaEjecucion != fechaEjecucionEntSal)
                            error += Environment.NewLine + $" - Diferencia en la fecha de ejecución de Entrada/Salida. SPEI='{fechaEjecucion}', TOMI='{fechaEjecucionEntSal}'.";

                        if (moneda != monedaEntSal)
                            error += Environment.NewLine + $" - Diferencia en la monedas de Entrada/Salida. SPEI='{moneda}', TOMI='{monedaEntSal}'.";

                        if (speiImporte != importeEntSal)
                            error += Environment.NewLine + $" - Diferencia en el importe de Entrada/Salida. SPEI='{speiImporte}', Entrada/Salida='{importeEntSal}'.";
                    }
                }

                if (!string.IsNullOrEmpty(error))
                    throw new Exception(error);

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación está correctamente conciliada.");

                await _controlServicio.TerminaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });


                _configuracion.AliasProceso =

                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });

                _configuracion.AliasProceso = string.Empty;
            }

            return true;
        }

        private async Task<bool> SPEI_A_TOMI(string folio, EntCoasOrdenPagoDto orden)
        {
            try
            {
                if (orden == null)
                    throw new Exception($"No existe información COAS para insertar en TOMI. Folio {folio}");

                string nfEstatus =
                    t24Encontrado && t24CuentaValida
                        ? "PEND"
                        : "RECC";

                var dto = new EntCoasInsertarFormatoSpeiDto
                {
                    OpClave = orden.OpClave,
                    OpFechaOper = orden.OpFechaOper,
                    MeClave = orden.MeClave,
                    OpFolio = orden.OpFolio,
                    InsClave = orden.InsClave,
                    OpMonto = orden.OpMonto,
                    TpClave = orden.TpClave,
                    OpCveRastreo = orden.OpCveRastreo,
                    OpNomOrd = orden.OpNomOrd,
                    TcClaveOrd = orden.TcClaveOrd,
                    OpCuentaOrd = orden.OpCuentaOrd,
                    OpRfcCurpOrd = orden.OpRfcCurpOrd,
                    OpNomBen = orden.OpNomBen,
                    TcClaveBen = orden.TcClaveBen,
                    OpCuentaBen = orden.OpCuentaBen,
                    OpRfcCurpBen = orden.OpRfcCurpBen,
                    OpNomBen2 = orden.OpNomBen2,
                    TcClaveBen2 = orden.TcClaveBen2,
                    OpCuentaBen2 = orden.OpCuentaBen2,
                    OpRfcCurpBen2 = orden.OpRfcCurpBen2,
                    OpConceptoPago = orden.OpConceptoPago,
                    OpConceptoPag2 = orden.OpConceptoPag2,
                    OpIva = orden.OpIva,
                    OpRefNumerica = orden.OpRefNumerica,
                    OpRefCobranza = orden.OpRefCobranza,
                    OpClavePago = orden.OpClavePago,
                    ToClave = orden.ToClave,
                    CdClave = orden.CdClave,
                    OpEstado = orden.OpEstado,
                    OpCde = orden.OpCde,
                    OpFechaCap = orden.OpFechaCap,
                    OpHoraCap = orden.OpHoraCap,
                    OpTipoOrden = orden.OpTipoOrden,
                    OpPrioridad = orden.OpPrioridad,
                    UsuClave = orden.UsuClave,
                    OpFolioServidor = orden.OpFolioServidor,
                    OpRazonRechazo = razonRechazo,
                    OpUsuAutoriza = orden.OpUsuAutoriza,
                    OpHoraLiqBm = orden.OpHoraLiqBm,
                    OpHoraLiqSist = orden.OpHoraLiqSist,

                    NfEstatus = nfEstatus,
                    NfFolioUnico = folioUnico,
                    T24CustomerCode = ObtenerValorT24("CUSTOMER"),
                    T24CompanyName = ObtenerValorT24("NAME_1"),
                    T24BenAcctNo = cuentaBtmum.ToString("0"),
                    NfBancoBenef = speiInsNombre,
                    NfFechaEjecucion = Convert.ToInt32(_configuracion.StrFechaSistema)
                };

                bool insertado = await _tomiDao.InsertarFormatoSpei(dto);

                Config.EscribirLog($"INFO|{tipoOperacion}.SPEI_A_TOMI|Resultado={insertado}");

                return insertado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|{tipoOperacion}.SPEI_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");

                return false;
            }
        }
        
        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            try
            {
                var entradaSpei = await _tomiDao.ObtenerEntradaSpeiTomiPorFolio(folio);

                if (entradaSpei == null)
                    return false;

                var dto = new EntCoasInsertarEntradaSalidaDto
                {
                    Referencia = folioUnico,
                    NumCliente = ConversionHelper.TryParseNullableDecimal(ObtenerValorT24("CUSTOMER")),
                    Cdl = ConversionHelper.TryParseNullableDecimal(ObtenerValorT24("CUSTOMER")),
                    Ordenante = ObtenerValorT24("ACCOUNT_TITLE_1"),
                    Operacion = 198,
                    OperacionDesc = "Entrada  (TTP)",
                    Tipo = "D",
                    TipoDesc = "Doméstica",
                    Cuenta = cuentaBtmum,
                    Nombre = ObtenerValorT24("ACCOUNT_TITLE_1"),
                    CuentaMoneda = ObtenerValorT24("CURRENCY"),
                    Via = 850,
                    ViaDesc = "TRF RECIBIDA MXN SPEUA",
                    Importe = Convert.ToDecimal(entradaSpei.OpMonto.Value),
                    Saldo = 0,
                    Comentarios = comentarios,
                    FechaCaptura = ConversionHelper.TextoAFechaT24(_configuracion.StrFechaSistema),
                    FechaValor = ConversionHelper.TextoAFechaT24(_configuracion.StrFechaSistema),
                    Hora = DateTime.Now.ToLongTimeString(),
                    Capturista = "JEAI",
                    Fad = 0,
                    ReferenciaControl = "A",
                    ReferenciaControlCont = 0
                };

                bool insertado = await _tomiDao.InsertarEntradaSalida(dto);

                Config.EscribirLog($"INFO|{tipoOperacion}.TOMI_ENTRADA_SALIDA|Resultado={insertado}");

                return insertado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|{tipoOperacion}.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}. Trace: {ex}");

                return false;
            }
        }

        private bool EsValidoRFC_CURP_SPEI(object texto)
        {
            string textoValidar = DameTexto(texto).Trim();

            if (textoValidar == "ND")
                return true;

            if (EsRFC(textoValidar))
                return true;

            if (EsCURP(textoValidar))
                return true;

            return false;
        }

        private void InicializarVariablesOperacion()
        {
            cuentaBtmum = 0;
            cuentaBeneficiario = string.Empty;
            cuentaOrdenante = string.Empty;
            codigoSwift = string.Empty;
            codigoSwiftShort = string.Empty;
            comentarios = string.Empty;
            razonRechazo = string.Empty;

            speiImporte = 0;
            speiOpClave = 0;
            speiFechaOper = 0;
            speiOpFolio = 0;
            speiInsClave = 0;
            speiCveRastreo = string.Empty;
            speiInsNombre = string.Empty;
            causaDevolucionSpei = 0;

            tipoOperacionSpei = 0;
            tipoClaveBen = 0;

            t24Encontrado = false;
            t24CuentaValida = false;
            rfcCurpValido = false;

            datosClienteT24 = null;
        }

        private EntCoasOrdenPagoDto AplicarCamposDevolucionSobreOrdenOriginal(
            EntCoasOrdenPagoDto original,
            EntCoasOrdenPagoDto devolucion)
        {
            if (original == null)
                return devolucion;

            original.OpCveRastreo = devolucion.OpCveRastreo;
            original.OpNomOrd = devolucion.OpNomOrd;
            original.TcClaveOrd = devolucion.TcClaveOrd;
            original.OpCuentaOrd = devolucion.OpCuentaOrd;
            original.OpRfcCurpOrd = devolucion.OpRfcCurpOrd;
            original.OpNomBen = devolucion.OpNomBen;
            original.TcClaveBen = devolucion.TcClaveBen;
            original.OpCuentaBen = devolucion.OpCuentaBen;
            original.OpRfcCurpBen = devolucion.OpRfcCurpBen;
            original.OpNomBen2 = devolucion.OpNomBen2;
            original.TcClaveBen2 = devolucion.TcClaveBen2;
            original.OpCuentaBen2 = devolucion.OpCuentaBen2;
            original.OpRfcCurpBen2 = devolucion.OpRfcCurpBen2;
            original.OpConceptoPago = devolucion.OpConceptoPago;
            original.OpConceptoPag2 = devolucion.OpConceptoPag2;
            original.OpIva = devolucion.OpIva;
            original.OpRefNumerica = devolucion.OpRefNumerica;
            original.OpRefCobranza = devolucion.OpRefCobranza;
            original.OpClavePago = devolucion.OpClavePago;
            original.ToClave = devolucion.ToClave;
            original.CdClave = devolucion.CdClave;
            original.OpEstado = devolucion.OpEstado;
            original.OpCde = devolucion.OpCde;
            original.OpFechaCap = devolucion.OpFechaCap;
            original.OpHoraCap = devolucion.OpHoraCap;
            original.OpTipoOrden = devolucion.OpTipoOrden;
            original.OpPrioridad = devolucion.OpPrioridad;
            original.UsuClave = devolucion.UsuClave;
            original.OpFolioServidor = devolucion.OpFolioServidor;
            original.OpRazonRechazo = devolucion.OpRazonRechazo;
            original.OpUsuAutoriza = devolucion.OpUsuAutoriza;
            original.OpTopologia = devolucion.OpTopologia;
            original.ErrClave = devolucion.ErrClave;
            original.OpExporta = devolucion.OpExporta;
            original.OpCargoAbono = devolucion.OpCargoAbono;
            original.OpAfectaBan = devolucion.OpAfectaBan;
            original.OpDuplicado = devolucion.OpDuplicado;
            original.OpHoraLiqBm = devolucion.OpHoraLiqBm;
            original.OpHoraLiqSist = devolucion.OpHoraLiqSist;

            return original;
        }
  
        private DataRow ObtenerFilaT24(string folio, string etapa, bool requerida)
        {
            if (datosClienteT24 == null)
                throw new Exception($"Error técnico al consultar datos T24 para folio {folio} en etapa {etapa}.");

            if (datosClienteT24.Rows.Count == 0)
            {
                if (requerida)
                    throw new Exception($"No se encontraron datos T24 para folio {folio} en etapa {etapa}.");

                return null;
            }

            return datosClienteT24.Rows[0];
        }

        private string ObtenerValorT24(string columna)
        {
            DataRow fila = ObtenerFilaT24(string.Empty, "LECTURA_COLUMNA", false);
            if (fila == null)
                return string.Empty;

            if (!datosClienteT24.Columns.Contains(columna))
                return string.Empty;

            object valor = fila[columna];

            return DameTexto(valor);
        }

     }
}

using Aplicacion.DTOs.ENT_TEF;
using Aplicacion.Interfaces.ENT_TEF;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa ENT TEF.
    /// </summary>
    public class ENT_TEF
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IENTTEFTefDao _entTefTefDao;
        private readonly IENTTEFTomiDao _entTefTomiDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "ENT_TEF";

        /// <summary>
        /// Constructor.
        /// </summary>
        public ENT_TEF(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IENTTEFTefDao entTefTefDao,
            IENTTEFTomiDao entTefTomiDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _entTefTefDao = entTefTefDao;
            _entTefTomiDao = entTefTomiDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación ENT_TEF del servicio Lectura.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio)
        {
            EntTefRecibidaDto datosTef;
            string folioUnico = string.Empty;
            string cuentaOrdenante = string.Empty;
            string cuentaBeneficiario = string.Empty;
            string cuentaBTMUM = "0";
            string comentarios = string.Empty;
            string razonRechazo = string.Empty;
            string fechaEjecucionTexto = string.Empty;
            string moneda = string.Empty;
            string subtipo = string.Empty;
            bool conDatoTef = false;
            bool conEntradaSalida = true;
            bool procesado;
            bool resultado;
            bool devolucion = false;
            int tipoOperacionTEF = 0;
            int tipoClaveBen = 0;
            bool t24Encontrado = false;
            bool t24CuentaValida = false;
            bool rfcCurpValido = false;
            long tefIdOperacion = 0;
            int tefFechaProceso = 0;
            string tefClaveRastreo = string.Empty;
            decimal tefImporte = 0m;
            int tefBancoPresentador = 0;
            string tefInsNombre = string.Empty;
            int tefMotivoDevolucion = 0;
            Dictionary<int, string> tefParticipantes = new Dictionary<int, string>();
            DataTable t24DatosCliente = null;

            //Verificamos la última operación no realizada.
            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });

            if (string.Equals(_configuracion.AliasProceso, string.Empty, StringComparison.Ordinal))
                return false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo lista de participantes de TEF.");

            List<EntTefParticipanteDto> participantes = await _entTefTefDao.ObtenerParticipantes();

            foreach (EntTefParticipanteDto participante in participantes)
            {
                if (!tefParticipantes.ContainsKey(participante.ins_clave))
                    tefParticipantes.Add(participante.ins_clave, participante.ins_nombre);
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo los datos de la operación en base TEF.");

            //Genero el folio único.
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(_configuracion.StrFechaSistema),
                folio);

            datosTef = await _entTefTefDao.ObtenerRecibidaPorFolio(
                Convert.ToInt32(_configuracion.StrFechaSistema),
                Convert.ToInt64(folio));

            if (datosTef != null)
            {
                conDatoTef = true;

                //Fecha ejecución.
                fechaEjecucionTexto = datosTef.FechaAplicacion.ToString();

                //Moneda.
                moneda = _vwT24.MonedaDefaultMN;

                //Importe.
                tefImporte = datosTef.Importe;

                //Comentarios.
                comentarios = DameTexto(datosTef.RefLeyendaOrdenante);

                tefMotivoDevolucion = datosTef.MotivoDevolucion;

                tipoOperacionTEF = Convert.ToInt32(DameTexto(datosTef.TipoOperacion));
                tipoClaveBen = Convert.ToInt32(DameTexto(datosTef.TipoCuentaOrdenante));

                //Identificamos el tipo de operación.
                if ((tipoOperacionTEF == 1) &&
                    (tipoClaveBen == 3 || tipoClaveBen == 40) &&
                    tefMotivoDevolucion == 0)
                {
                    subtipo = string.Empty;
                    cuentaBeneficiario = DameTexto(datosTef.CuentaReceptor);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Validando la cuenta del beneficiario. CuentaBeneficiario='{cuentaBeneficiario}'.");

                    if (cuentaBeneficiario.Length == 18)
                    {
                        cuentaBTMUM = cuentaBeneficiario.Substring(6, 11);
                    }
                    else if (cuentaBeneficiario.Length == 20)
                    {
                        cuentaBTMUM = cuentaBeneficiario.Substring(8, 11);
                        cuentaBeneficiario = cuentaBeneficiario.Substring(2, 18);
                    }

                    cuentaOrdenante = DameTexto(datosTef.CuentaOrdenante);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Validando la cuenta del ordenante. CuentaOrdenante='{cuentaOrdenante}'.");
                }
                else if (tefMotivoDevolucion > 0)
                {
                    subtipo = "DEVOLUCION";
                    devolucion = true;

                    cuentaBeneficiario = DameTexto(datosTef.CuentaOrdenante);

                    if (cuentaBeneficiario.Length == 18)
                    {
                        cuentaBTMUM = cuentaBeneficiario.Substring(6, 11);
                    }
                    else if (cuentaBeneficiario.Length == 20)
                    {
                        cuentaBTMUM = cuentaBeneficiario.Substring(8, 11);
                        cuentaBeneficiario = cuentaBeneficiario.Substring(2, 18);
                    }

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Devolucion, cuenta='{cuentaBeneficiario}',CuentaBTMUM={cuentaBTMUM}.");
                }
                else
                {
                    throw new Exception($"Tipo de operación de entrada TEF desconocido, tipo={tipoOperacionTEF}");
                }

                //Quita los 0's de la izquierda.
                cuentaBTMUM = Convert.ToInt64(cuentaBTMUM).ToString();

                //Datos del TEF.
                tefIdOperacion = datosTef.IDOperacion;
                tefFechaProceso = datosTef.FechaProceso;
                tefClaveRastreo = datosTef.ClaveRastreo;
                tefBancoPresentador = Convert.ToInt32(DameTexto(datosTef.BancoPresentador));
            }

            if (!conDatoTef)
                throw new Exception($"No se encontró información de la folio (IDOperacion) {folio} en los datos de entrada del TEF");

            //Valido el nombre de la institución que solicita.
            if (!tefParticipantes.ContainsKey(tefBancoPresentador))
                throw new Exception($"No se tiene relación con la institución {tefBancoPresentador}");

            tefInsNombre = tefParticipantes[tefBancoPresentador];

            //Validación de la cuenta.
            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Validando en T24 los datos de la cuenta '{cuentaBTMUM}'.");

            t24DatosCliente = await _vwT24.DatosClienteCuentasVw(cuentaBTMUM);
            DataRow filaClienteT24 = ObtenerFilaT24(t24DatosCliente, folio, "VALIDACION_T24", false);

            if (filaClienteT24 != null)
            {
                if (!string.Equals(DameTexto(filaClienteT24["CUSTOMER"]), string.Empty, StringComparison.Ordinal))
                {
                    t24Encontrado = true;

                    if (string.Equals(DameTexto(filaClienteT24["CURRENCY"]), _vwT24.MonedaDefaultMN, StringComparison.Ordinal))
                    {
                        t24CuentaValida = true;
                    }
                    else
                    {
                        Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|La cuenta seleccionada en el sistema T24 está relacionada a una moneda no válida.");
                        razonRechazo = "La cuenta seleccionada en el sistema T24 esta relacionada a una moneda no valida";
                    }
                }
                else
                {
                    Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|No Existe la cuenta en el sistema T24.");
                    razonRechazo = "No existe la cuenta en el sistema T24";
                }
            }
            else
            {
                Config.EscribirLog($"ALERTA|{tipoOperacion}.procesaOperacion|No Existe la cuenta en el sistema T24.");
                razonRechazo = "No existe la cuenta en el sistema T24";

                procesado = await RespondeEntradaTef("R", 1, tefIdOperacion, tefFechaProceso, tefClaveRastreo, tefImporte);

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A cancelar la operación por devolución.");

                await _entTefTomiDao.CancelarMaestroPorDevolucion(
                    Convert.ToInt64(folio),
                    tipoOperacion,
                    razonRechazo,
                    Convert.ToInt32(fechaEjecucionTexto),
                    moneda,
                    tefImporte);

                return true;
            }

            //Validación del RFC para pagos tercero a tercero.
            rfcCurpValido = true;

            if (tipoOperacionTEF == 1)
            {
                //“Se rechaza el hallazgo por diseño funcional vigente.
                //En la ruta ENT_TEF, la validación RFC/CURP es informativa (log)
                //y no bloqueante, según contrato legacy
                //documentado (QUEDA PENDIENTE LA VALIDACIÓN DE RFC). El criterio
                //de aceptación/rechazo operativo se determina por validaciones
                //TEF/T24 (cuenta, moneda, conciliación y estatus), no por RFC/CURP
                //en esta etapa. Cambiar la bandera a inválida modificaría comportamiento
                //productivo y rompería compatibilidad funcional 1:1 con legacy.”
                if (!EsValidoRFC_CURP_TEF(datosTef.RFCOrdenante))
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|RFC ordenante invalido='{DameTexto(datosTef.RFCOrdenante)}'.");

                    rfcCurpValido = true;
                }

                if (!cuentaBeneficiario.StartsWith("1081800000", StringComparison.Ordinal))
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Cuenta CLABE Inválida='{DameTexto(datosTef.CuentaReceptor)}'.");
                    t24CuentaValida = false;
                }
            }

            if (t24Encontrado && t24CuentaValida)
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La cuenta '{cuentaBTMUM}' existe en T24 y es en moneda nacional.");
            }

            conEntradaSalida = t24Encontrado && t24CuentaValida && rfcCurpValido;

            //Verificamos la última operación no realizada.
            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(fechaEjecucionTexto, CultureInfo.InvariantCulture);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception(
                    $"Se desea realizar una operación al {fechaEjecucionTexto} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            //Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc). " +
                    $"T24_Encontrado={t24Encontrado},T24_CuentaValida={t24CuentaValida},RFC_CURP_Valido={rfcCurpValido}," +
                    $"FechaEjecucion={fechaEjecucionTexto},cve_moneda='{moneda}',importe={tefImporte}.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                procesado = false;

                if (t24Encontrado && t24CuentaValida && rfcCurpValido)
                {
                    DataRow filaClienteT24Requerida = ObtenerFilaT24(t24DatosCliente, folio, "DATOS_GRAL", true);

                    procesado = await _entTefTomiDao.ActualizarDatosGeneralesMaestroConCliente(
                        Convert.ToInt64(folio),
                        tipoOperacion,
                        folioUnico,
                        Convert.ToInt32(fechaEjecucionTexto),
                        moneda,
                        tefImporte,
                        subtipo,
                        Convert.ToInt32(DameTexto(filaClienteT24Requerida["CUSTOMER"])),
                        DameTexto(filaClienteT24Requerida["NAME_1"]),
                        cuentaBTMUM);
                }
                else
                {
                    procesado = await _entTefTomiDao.ActualizarDatosGeneralesMaestroSinCliente(
                        Convert.ToInt64(folio),
                        tipoOperacion,
                        folioUnico,
                        Convert.ToInt32(fechaEjecucionTexto),
                        moneda,
                        tefImporte,
                        subtipo);
                }

                if (procesado)
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
                }
            }

            //Lectura del TEF e inserción a TOMI.
            if (string.Equals(_configuracion.AliasProceso, "TEF_A_TOMI", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A insertar datos de TEF a TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                procesado = false;
                resultado = await TEF_A_TOMI(folio, datosTef, tefMotivoDevolucion, t24Encontrado, t24CuentaValida, t24DatosCliente, folioUnico, tefInsNombre);

                if (resultado)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                    procesado = true;
                }
                else
                {
                    bool existeFormato = await _entTefTomiDao.ExisteFormatoEntradaTef(Convert.ToInt64(folio));

                    if (existeFormato)
                    {
                        procesado = true;
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (procesado)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A marcar el proceso como terminado.");
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>

                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro TEF_A_TOMI con folio: {folio}");
                }
            }

            //Registro de la Entrada/Salida.
            if (string.Equals(_configuracion.AliasProceso, "TOMI_ENTRADAS/SALIDAS", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A insertar datos de Entrada/Salida en TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (conEntradaSalida)
                {
                    procesado = false;
                    resultado = await TOMI_ENTRADA_SALIDA(folio, folioUnico, t24DatosCliente, cuentaBTMUM, comentarios);

                    if (resultado)
                    {
                        procesado = true;
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio en base de Entrada/Salida.");
                    }
                    else
                    {
                        bool existeEntradaSalida = await _entTefTomiDao.ExisteEntradaSalidaPorReferencia(folioUnico);

                        if (existeEntradaSalida)
                        {
                            procesado = true;
                            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                        }
                    }
                }
                else
                {
                    procesado = true;
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación no debe generar Entrada/Salida en TOMI.");
                }

                if (procesado)
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");
                }
            }

            //Actualización del estatus del TEF.
            if (string.Equals(_configuracion.AliasProceso, "CAMBIA_ESTATUS", StringComparison.Ordinal))
            {
                bool rechazada = false;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A actualizar estatus en TEF y TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                procesado = true;

                if (devolucion)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación es una DEVOLUCION o VOSTRO y envio su aceptación.");

                    procesado = await RespondeEntradaTef("A", null, tefIdOperacion, tefFechaProceso, tefClaveRastreo, tefImporte);

                    if (!procesado)
                        throw new Exception($"Error en respuesta de la entrada TEF para el folio: {folio}");

                    if (t24Encontrado)
                    {
                        if (t24CuentaValida)
                        {
                            if (!rfcCurpValido)
                                rechazada = true;
                        }
                        else
                        {
                            rechazada = true;
                        }
                    }
                    else
                    {
                        rechazada = true;
                    }

                    if (rechazada)
                    {
                        /// <summary>
                        /// Ejecuta la operaciÃ³n TerminaProceso.
                        /// </summary>
                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });

                        await _entTefTomiDao.MarcarMaestroRecibidaNoAbonada(
                            Convert.ToInt64(folio),
                            tipoOperacion);

                        return true;
                    }
                }
                else
                {
                    if (t24Encontrado)
                    {
                        if (t24CuentaValida)
                        {
                            if (!rfcCurpValido)
                            {
                                rechazada = true;
                                procesado = await RespondeEntradaTef("R", 1, tefIdOperacion, tefFechaProceso, tefClaveRastreo, tefImporte);

                                if (!procesado)
                                    throw new Exception($"Error en respuesta de la entrada TEF para el folio: {folio}");
                            }
                        }
                        else
                        {
                            rechazada = true;
                            procesado = await RespondeEntradaTef("R", 5, tefIdOperacion, tefFechaProceso, tefClaveRastreo, tefImporte);

                            if (!procesado)
                                throw new Exception($"Error en respuesta de la entrada TEF para el folio: {folio}");
                        }
                    }
                    else
                    {
                        rechazada = true;
                        procesado = await RespondeEntradaTef("R", 1, tefIdOperacion, tefFechaProceso, tefClaveRastreo, tefImporte);

                        if (!procesado)
                            throw new Exception($"Error en respuesta de la entrada TEF para el folio: {folio}");
                    }
                }

                if (procesado)
                {
                    if (rechazada)
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A cancelar la operación por devolución.");

                        await _entTefTomiDao.CancelarMaestroPorDevolucion(
                            Convert.ToInt64(folio),
                            tipoOperacion,
                            razonRechazo,
                            Convert.ToInt32(fechaEjecucionTexto),
                            moneda,
                            tefImporte);

                        return true;
                    }
                    else
                    {
                        await _entTefTomiDao.ActualizarEstatusFormatoEntradaTefAScnp(Convert.ToInt64(folio));
                    }
                }

                if (procesado)
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CAMBIO_ESTATUS con folio: {folio}");
                }
            }

            //Actualización del estatus del TEF, TOMI y ENTRADA/SALIDA.
            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                int regsTef = 1;
                int regsTomi = 0;
                int regsEntSal = 0;

                string mensajeError = string.Empty;

                string fechaEjecucionTomi = string.Empty;
                string monedaTomi = string.Empty;
                decimal importeTomi = 0m;

                string fechaEjecucionEntSal = string.Empty;
                string monedaEntSal = string.Empty;
                decimal importeEntSal = 0m;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A conciliar operaciones entre TEF y TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                EntTefConciliacionTomiDto conciliacionTomi =
                    await _entTefTomiDao.ObtenerConciliacionFormatoEntradaTef(Convert.ToInt64(folio));

                if (conciliacionTomi != null)
                {
                    regsTomi = 1;
                    fechaEjecucionTomi = conciliacionTomi.FechaAplicacion.ToString();
                    monedaTomi = _vwT24.MonedaDefaultMN;
                    importeTomi = conciliacionTomi.Importe;
                }

                if (conEntradaSalida)
                {
                    EntTefConciliacionEntradaSalidaDto conciliacionEntradaSalida =
                        await _entTefTomiDao.ObtenerConciliacionEntradaSalida(folioUnico);

                    if (conciliacionEntradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = conciliacionEntradaSalida.Ent_FechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                        monedaEntSal = _vwT24.ValidaMoneda(conciliacionEntradaSalida.Ent_Moneda_Cuenta);
                        importeEntSal = conciliacionEntradaSalida.Ent_Importe;
                    }
                }

                mensajeError = string.Empty;

                if (regsTef == 1 &&
                    regsTomi == 1 &&
                    ((conEntradaSalida && regsEntSal == 1) || !conEntradaSalida))
                {
                    if (!string.Equals(fechaEjecucionTexto, fechaEjecucionTomi, StringComparison.Ordinal))
                    {
                        mensajeError += Environment.NewLine +
                                        $" - Diferencia en la fecha de ejecución. TEF='{fechaEjecucionTexto}', TOMI='{fechaEjecucionTomi}'.";
                    }

                    if (!string.Equals(moneda, monedaTomi, StringComparison.Ordinal))
                    {
                        mensajeError += Environment.NewLine +
                                        $" - Diferencia en la monedas. TEF='{moneda}', TOMI='{monedaTomi}'.";
                    }

                    if (tefImporte != importeTomi)
                    {
                        mensajeError += Environment.NewLine +
                                        $" - Diferencia en el importe. TEF='{tefImporte}', TOMI='{importeTomi}'.";
                    }

                    if (conEntradaSalida)
                    {
                        if (!string.Equals(fechaEjecucionTexto, fechaEjecucionEntSal, StringComparison.Ordinal))
                        {
                            mensajeError += Environment.NewLine +
                                            $" - Diferencia en la fecha de ejecución de Entrada/Salida. TEF='{fechaEjecucionTexto}', TOMI='{fechaEjecucionEntSal}'.";
                        }

                        if (!string.Equals(moneda, monedaEntSal, StringComparison.Ordinal))
                        {
                            mensajeError += Environment.NewLine +
                                            $" - Diferencia en la monedas de Entrada/Salida. TEF='{moneda}', TOMI='{monedaEntSal}'.";
                        }

                        if (tefImporte != importeEntSal)
                        {
                            mensajeError += Environment.NewLine +
                                            $" - Diferencia en el importe de Entrada/Salida. TEF='{tefImporte}', Entrada/Salida='{importeEntSal}'.";
                        }
                    }

                    procesado = true;
                }
                else
                {
                    procesado = false;
                    mensajeError += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                }

                if (!string.Equals(mensajeError, string.Empty, StringComparison.Ordinal))
                {
                    procesado = false;
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(mensajeError);
                }

                if (procesado)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación está correctamente conciliada.");
