using AplicacionSolicitudes.DTO;
using AplicacionSolicitudes.Enums;
using AplicacionSolicitudes.Interfaces;
using AplicacionSolicitudes.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AplicacionSolicitudes.Servicios
{
    /// <summary>
    /// Servicio donde implementan las funcionalidades del servicio
    /// </summary>
    public class ProcesosServicio : IProcesos
    {
        #region Propiedades

        private List<long> _rechazados;
        private readonly ITomi _tomi;
        private readonly ICMS _cms;

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor servicio
        /// </summary>
        /// <param name="configuraicon"></param>
        /// <param name="operacionesCMS"></param>
        /// <param name="operacionesTomi"></param>
        public ProcesosServicio(ICMS operacionesCMS, ITomi operacionesTomi)
        {
            _cms = operacionesCMS;
            _tomi = operacionesTomi;
        }

        #endregion

        #region Beneficiarios CMS

        /// <summary>
        /// Método que registra los beneficiariosCMS
        /// </summary>
        /// <returns></returns>
        public async Task<bool> InsertaBeneficiariosCMS()
        {
            try
            {
                var listaAlertaBeneficiario = new List<AlertasBeneficiarioModelo>();
                var palabrasRestringidas = await _tomi.ObtenerEntidadesRestringidas();
                var palabrasPermitidas = await _tomi.ObtenerEntidadesPermitidas();
                var listaSolicitudes = await _cms.ObtenerSolicitudes();

                if (listaSolicitudes != null)
                {
                    foreach (SolicitudesBeneficiariosCmsModelo solictud in listaSolicitudes)
                    {
                        Config.EscribirLog($"INFO|InsertaBeneficiariosCMS|Insertando a TOMI la solicitud: {solictud.ID_SOLICITUD_CMS}");


                        if (solictud.Formato == Constantes.FormatoSPD && !string.Equals(solictud.OPERACION, Constantes.TipoOperacionBaja, StringComparison.OrdinalIgnoreCase))
                            solictud.STATUS = Constantes.EstatusMesaControl;
                        else
                            solictud.STATUS = Constantes.EstatusConfirmado;

                        if (solictud.FECHA_SOL != null)
                            solictud.Hora_Sol = solictud.FECHA_SOL.Value.ToString("HH:mm");
                        else
                            solictud.Hora_Sol = string.Empty;

                        if (solictud.FECHA_AUT != null)
                            solictud.Hora_Aut = solictud.FECHA_AUT.Value.ToString("HH:mm");
                        else
                            solictud.Hora_Aut = string.Empty;

                        if (solictud.FECHA_CANC != null)
                            solictud.Hora_Canc = solictud.FECHA_CANC.Value.ToString("HH:mm");
                        else
                            solictud.Hora_Canc = string.Empty;

                        if (solictud.FECHA_CONF != null)
                            solictud.Hora_Conf = solictud.FECHA_CONF.Value.ToString("HH:mm");
                        else
                            solictud.Hora_Conf = string.Empty;


                        if (solictud.STATUS == Constantes.EstatusMesaControl)
                        {
                            AlertasBeneficiarioModelo alerta = GeneraAlertasBeneficiario(solictud, palabrasRestringidas, palabrasPermitidas);
                            listaAlertaBeneficiario.Add(alerta);
                        }
                    }

                    string datosXML = string.Empty;
                    datosXML = Utilidades.Serializar<SolicitudesBeneficiariosCmsModelo>(listaSolicitudes);

                    if (await _tomi.TombinInsertSolicitudesBeneficiarios(datosXML))
                    {
                        if (listaAlertaBeneficiario.Count > 0)
                        {
                            datosXML = Utilidades.Serializar<AlertasBeneficiarioModelo>(listaAlertaBeneficiario);
                            await _tomi.InsertarAlertasBeneficiarios(datosXML);
                        }

                        var foliosSolicitud = listaSolicitudes.Select(a => new SolicitudesBeneficiarioDto() { IdSolicitud = a.ID_SOLICITUD_CMS }).ToList();

                        datosXML = Utilidades.Serializar<SolicitudesBeneficiarioDto>(foliosSolicitud);
                        await _cms.ActualizarSolicitudesBeneficiarios(datosXML);

                        Config.EscribirLog("INFO|InsertaBeneficiariosCMS|Se termina el registro en TOMI de la solicitudes");
                    }
                    else
                        Config.EscribirLog("ERROR|InsertaBeneficiariosCMS|Error en el registro en TOMI");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|InsertaBeneficiariosCMS|Ocurre exception: {ex.ToString()}");
                return false;
            }
        }

        /// <summary>
        /// Función para enviar alertas (Registra la alerta en base de datos)
        /// </summary>
        private AlertasBeneficiarioModelo GeneraAlertasBeneficiario(
            SolicitudesBeneficiariosCmsModelo solictud,
            List<SPIDEntidadesRestringidasModelo> laListaPalabrasRestringidas,
            List<SPIDEntidadesPermitidasModelo> laListaPalabrasPermitidas)
        {
            AlertasBeneficiarioModelo alerta = new AlertasBeneficiarioModelo();

            string[] valores = {
                TextoBESS(solictud.Alias),
                TextoBESS(solictud.Nombre),
                TextoBESS(solictud.Apellido_P),
                TextoBESS(solictud.Apellido_M),
             };

            string alerta1 = string.Empty;
            string alerta2 = string.Empty;
            string alertaFin = string.Empty;

            foreach (string palabraBuscar in valores)
            {
                if (string.IsNullOrWhiteSpace(palabraBuscar) || string.IsNullOrEmpty(palabraBuscar))
                    continue;

                var Coincidencias = laListaPalabrasRestringidas
                    .Where(a => a.Palabra.ToUpper().Contains(palabraBuscar.ToUpper()))
                    .ToList();

                if (Coincidencias != null && Coincidencias.Count > 0)
                {
                    Config.EscribirLog($"INFO|GeneraAlertasBeneficiarios|Se encontró coincidencia con: {palabraBuscar}");
                    alerta1 += $"{palabraBuscar}|";
                }
            }

            valores = new string[]
            {
                TextoBESS(solictud.Nombre)
            };

            foreach (string palabraBuscar in valores)
            {
                if (string.IsNullOrEmpty(palabraBuscar))
                    continue;

                var Coincidencias = laListaPalabrasPermitidas
                    .Where(a => a.Palabra.ToUpper().Contains(palabraBuscar.ToUpper()))
                    .ToList();

                if (Coincidencias == null || Coincidencias.Count == 0)
                    alerta2 = "Beneficiario no es una empresa, posible persona fisica.|";
                else
                {
                    Config.EscribirLog($"INFO|GeneraAlertasBeneficiarios|Se encontró coincidencia con {Coincidencias[0].Palabra}" +
                                        $" por lo que el nombre del beneficiario {palabraBuscar} sí corresponde a una empresa.");
                    alerta2 = "";
                }
            }

            if (!string.IsNullOrEmpty(alerta2))
            {
                Config.EscribirLog(alerta2);
                alertaFin += alerta2;
            }

            if (!string.IsNullOrEmpty(alerta1))
            {
                Config.EscribirLog(alerta1);
                alertaFin += "Coincidencia en lista restringida por: |" + alerta1;
            }

            alerta.ID_SOLICITUD_DOCUMENTO = solictud.ID_SOLICITUD_DOCUMENTO;
            alerta.Alerta = (string.IsNullOrEmpty(alertaFin)) ? "Sin Alerta" : alertaFin;

            return alerta;
        }


        /// <summary>
        /// Función para limpiar una cadena de Texto
        /// </summary>
        private string TextoBESS(string texto)
        {
            Encoding loUnicode;
            Encoding loEncoding;
            byte[] laBytes;
            byte[] laNewBytes;
            byte liCodigo;

            string lsTexto;
            string lsRegreso = "";

            int liIdx;
            int liCount = 0;
            bool lbEsInvalido;

            // Validación
            if (texto == null)
                return "";

            if (string.IsNullOrWhiteSpace(texto.ToString()))
                return "";

            // Texto inicial
            lsTexto = texto.ToString().ToUpper();

            // Reemplazos comunes
            lsTexto = lsTexto.Replace("Á", "A")
                             .Replace("É", "E")
                             .Replace("Í", "I")
                             .Replace("Ó", "O")
                             .Replace("Ú", "U");

            // CodePage 850
            loUnicode = Encoding.Unicode;
            loEncoding = Encoding.GetEncoding(850);

            // Texto a bytes
            laBytes = loUnicode.GetBytes(lsTexto);

            // Conversión
            laNewBytes = Encoding.Convert(loUnicode, loEncoding, laBytes);

            // Análisis por carácter
            for (liIdx = 0; liIdx < laNewBytes.Length; liIdx++)
            {
                liCodigo = laNewBytes[liIdx];
                lbEsInvalido = false;

                if (liCodigo < 32 || liCodigo > 173)
                {
                    lbEsInvalido = true;
                }
                else if (Array.IndexOf(Constantes.CaracteresNoValidos, liCodigo) >= 0)
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

        #endregion

        #region Beneficiarios

        /// <summary>
        /// Función para ejecutar las solicitudes de Beneficiarios 
        /// </summary>
        public async Task<bool> ProcesaBeneficiarios()
        {
            try
            {
                var solicitudes = await _tomi.ObtenerSolicitudesBeneficiariosTomiTomser();
                if (solicitudes.Count > 0)
                {
                    Config.EscribirLog($"INFO|ProcesaBeneficiarios|Procesando {solicitudes.Count} Plantillas ");

                    await ValidaBeneficiarios(solicitudes);

                    var ids = new HashSet<long>(_rechazados);
                    var correctas = solicitudes.Where(s => !ids.Contains(s.ID_SOLICITUD)).ToList();

                    if (correctas.Count > 0)
                    {
                        Config.EscribirLog("INFO|ProcesaBeneficiarios|Beneficiario sin duplicados en CMS ");
                        ids = new HashSet<long>(_rechazados);

                        var procesadas = solicitudes.Where(s => !ids.Contains(s.ID_SOLICITUD)).ToList();
                        if (procesadas.Count > 0)
                        {
                            string datosXML = string.Empty;
                            int resultado = await ProcesaPlantilla(procesadas);

                            if (resultado > 0)
                            {
                                var solicitudesAltas = procesadas.Where(x => string.Equals(x.OPERACION, Constantes.ALTA, StringComparison.OrdinalIgnoreCase)).ToList();
                                if (solicitudesAltas.Count > 0)
                                {
                                    datosXML = Utilidades.Serializar<SolicitudesBeneficiarioTomserModelo>(solicitudesAltas);
                                    await _tomi.ActualizarSolicitudesAltasTomser(datosXML);
                                }

                                var solicitudesModificacion = procesadas.Where(x => string.Equals(x.OPERACION, Constantes.ALTA, StringComparison.OrdinalIgnoreCase)).ToList();
                                if (solicitudesModificacion.Count > 0)
                                {
                                    datosXML = Utilidades.Serializar<SolicitudesBeneficiarioTomserModelo>(solicitudesModificacion);
                                    await _tomi.ActualizarSolicitudesModificacionTomser(datosXML);
                                }

                                Config.EscribirLog($"INFO|ProcesaBeneficiarios|Se termina procesamiento de Formatos: {string.Join(",", procesadas.Select(p => p.ID_SOLICITUD))}");
                            }
                        }
                        else
                            Config.EscribirLog($"ERROR|ProcesaBeneficiarios|Error en procesamiento de los siguientes Formatos {string.Join(",", _rechazados)}");
                    }
                    else
                        Config.EscribirLog($"ERROR|ProcesaBeneficiarios|Error en procesamiento de los Formatos.");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesaBeneficiarios|Se presenta una excepción: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Función para validar Duplicados de Beneficiarios
        /// </summary>
        /// <param name="listaSolicitudes"></param>
        /// <returns></returns>
        private async Task<bool> ValidaBeneficiarios(List<SolicitudesBeneficiarioTomserModelo> listaSolicitudes)
        {
            try
            {
                if (listaSolicitudes.Count == 0)
                    return true;

                _rechazados = new List<long>();

                Config.EscribirLog("INFO|ValidaBeneficiarios|Validando posibles duplicados en CMS");
                List<ValidaBeneficiariosDto> listaValidacionBenef = new List<ValidaBeneficiariosDto>();

                foreach (var solicitud in listaSolicitudes)
                {
                    ValidaBeneficiariosDto item = new ValidaBeneficiariosDto()
                    {
                        Id_Solicitud = solicitud.ID_SOLICITUD_DOCUMENTO,
                        Formato = solicitud.Formato,
                        Estatus = solicitud.STATUS,
                        Id_Cliente = solicitud.Id_Cliente,
                        TipoCuenta = solicitud.TipoCuenta,
                        NoCuenta = solicitud.NoCuenta,
                        Moneda = solicitud.Moneda,
                        ABA = solicitud.ABA,
                        SwiftBeneficiario = solicitud.SwiftBeneficiario,
                        SwiftCorresponsal = solicitud.SwiftCorresponsal
                    };

                    listaValidacionBenef.Add(item);
                }

                string datosXML = Utilidades.Serializar<ValidaBeneficiariosDto>(listaValidacionBenef);
                List<ValidacionBeneficiariosModel> listaValidacionBeneficiarios = await _cms.ValidaBeneficiarios(datosXML);
                foreach (var item in listaValidacionBeneficiarios)
                {
                    var _sol = listaSolicitudes.Where(s => s.ID_SOLICITUD_DOCUMENTO == item.Id_SolicitudOriginal).FirstOrDefault();

                    if (_sol != null)
                    {
                        item.Id_Solicitud = _sol.ID_SOLICITUD;
                        item.Origen = _sol.origen;
                    }
                }

                var rechazados = listaValidacionBeneficiarios
                                .Where(x => string.Equals(x.Estatus, Constantes.RECHAZADO, StringComparison.OrdinalIgnoreCase))
                                .ToList();

                if (rechazados.Count > 0)
                    _rechazados = rechazados.Select(a => a.Id_Solicitud).ToList();

                datosXML = Utilidades.Serializar<ValidacionBeneficiariosModel>(listaValidacionBeneficiarios);
                await _tomi.ActualizaSolicitudesTomiTomser(datosXML);

                var encontrados = listaValidacionBeneficiarios
                    .Where(a => a.Cont > 0 && !string.Equals(a.Estatus, Constantes.RECHAZADO, StringComparison.OrdinalIgnoreCase)
                                            && string.Equals(a.Origen, Constantes.CMS, StringComparison.OrdinalIgnoreCase)).ToList();

                var recOrigenCMS = rechazados
                    .Where(a => string.Equals(a.Origen, Constantes.CMS, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                var conRegistro = listaValidacionBeneficiarios
                    .Where(a => a.Cont > 0 && !string.Equals(a.Estatus, Constantes.RECHAZADO, StringComparison.OrdinalIgnoreCase))
                    .Select(a => a.Id_Solicitud).ToList();

                _rechazados.AddRange(conRegistro);

                if (recOrigenCMS.Count > 0)
                {
                    datosXML = string.Empty;
                    datosXML = Utilidades.Serializar<ValidacionBeneficiariosModel>(recOrigenCMS);

                    await _cms.ActualizarSolicitudesBeneficiariosRechazados(datosXML);
                }

                if (encontrados.Count > 0)
                {
                    datosXML = string.Empty;
                    datosXML = Utilidades.Serializar<ValidacionBeneficiariosModel>(encontrados);

                    await _cms.ActualizarSolicitudesBeneficiariosConregistro(datosXML);
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ValidaBeneficiarios|Se presenta una excepción: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Función para procesar las plantillas de solicitudes de los beneficiarios
        /// </summary>
        private async Task<int> ProcesaPlantilla(List<SolicitudesBeneficiarioTomserModelo> listaSolicitudes)
        {
            try
            {
                string datosXML = Utilidades.Serializar<SolicitudesBeneficiarioTomserModelo>(listaSolicitudes);

                int iResultadoCMS = await _cms.ProcesaPlantilla_CMS(datosXML);
                int iResultadoTomi = await _tomi.ProcesaPlantillaTOMI(datosXML);

                Config.EscribirLog("INFO|ProcesaPlantilla|FIN DEL LLAMADO");
                return iResultadoCMS + iResultadoTomi;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR|ProcesaPlantilla|Se presenta exception: {ex.ToString()}");
                return 0;
            }
        }

        #endregion

        #region Parametros

        /// <summary>
        /// Función Principal para procesar las solicitudes de Parametros
        /// </summary>
        public async Task<bool> ProcesaParametros()
        {
            try
            {
                List<SolicitudParametrosModelo> listaParametros = await _tomi.ObtenerListaParametrosAProcesar();
                if (listaParametros.Count > 0)
                {
                    var folios = listaParametros.Select(a => a.Id_Solicitud).Distinct().ToList();
                    string datosXML = Utilidades.Serializar<SolicitudParametrosModelo>(listaParametros);

                    await _tomi.InsertarParametrosClienteTomi(datosXML);
                    await _cms.InsertarParametrosCliente_CMS(datosXML);

                    Config.EscribirLog($"INFO|ProcesaParametros|Se termina procesamiento de Parametros Generales de los siguientes Folios {string.Join(",", folios)}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesaParametros|Ocurre exception: {ex.Message}");
                return false;
            }
        }

        #endregion

        #region Bancos

        /// <summary>
        /// Función para poder procesar las colicitudes correspondientes a bancos
        /// </summary>
        public async Task<bool> ProcesaBancos()
        {
            try
            {
                List<SolicitudBancoModelo> listaBancos = await _tomi.ObtenerSolicitudesBancos();
                if (listaBancos.Count > 0)
                {
                    List<long> ids = listaBancos.Select(x => x.Id_Solicitud).Distinct().ToList();
                    Config.EscribirLog($"INFO|ProcesaBancos|Procesando solicitudes de Bancos:  {string.Join(", ", ids)}");

                    int boResultado = await ProcesaAutorizacionBancos(listaBancos);
                    if (boResultado > 0)
                    {
                        string datosXML = Utilidades.Serializar<SolicitudBancoModelo>(listaBancos);
                        await _tomi.ActualizarSolicitudesBancos(datosXML);

                        Config.EscribirLog($"INFO|ProcesaBancos|Se termina procesamiento de solicitudes de Bancos: {string.Join(",", ids)}");
                    }
                    else
                        Config.EscribirLog($"ERROR|ProcesaBancos|Error en procesamiento de solicitudes de Bancos: {string.Join(", ", ids)}");
                }

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesaBancos|Ocurre exception: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Procesar las solicitudes de autorización de bancos
        /// </summary>
        private async Task<int> ProcesaAutorizacionBancos(List<SolicitudBancoModelo> solicitudBancos)
        {
            try
            {
                ProcesaAutorizacionBancosModelo salida = new ProcesaAutorizacionBancosModelo();
                string datosXML = string.Empty;
                int total = 0;

                string[] procedimientosCMS = new string[]
                {
                    ProcedimientoAlmacenado.sp_InsertarAbausaCMS,
                    ProcedimientoAlmacenado.sp_InsertarSwiftMex_CMS,
                    ProcedimientoAlmacenado.sp_InsertarSwiftOthBenUsd_CMS,
                    ProcedimientoAlmacenado.sp_InsertarOtherSwift_CMS
                };

                string[] procedimientosTomi = new string[]
                {
                    ProcedimientoAlmacenado.sp_InsertarAbausaTOMI,
                    ProcedimientoAlmacenado.sp_InsertarSwiftMex_TOMI,
                    ProcedimientoAlmacenado.sp_InsertarSwiftOthBenUsd_TOMI,
                    ProcedimientoAlmacenado.sp_InsertarOtherSwift_TOMI
                };

                for(int i = 0; i < 4; i++)
                {
                    int tipo = i + 1;
                    var datosFiltrados = solicitudBancos.Where(x => x.Tipo == tipo && string.Equals(x.Operacion, Constantes.ALTA, StringComparison.OrdinalIgnoreCase)).ToList();
                    if(datosFiltrados.Count > 0)
                    {
                        datosXML = Utilidades.Serializar<SolicitudBancoModelo>(datosFiltrados);
                        salida = await _cms.InsertarDatosCMS(datosXML, procedimientosCMS[i]);
                        total += salida.Insertados;

                        if(salida.Insertados > 0)
                        {
                            salida = await _tomi.InsertarDatosTOMI(datosXML, procedimientosTomi[i]);
                            total += salida.Insertados;
                        }
                    }
                }

                return total;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Procesa_Autorizacion_Bancos|Se presenta exception: {ex.Message}");
                return 0;
            }
        }

        #endregion
    }
}

using AplicacionSolicitudes.Enums;
using AplicacionSolicitudes.Interfaces;
using AplicacionSolicitudes.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SolicitudesAplicacion
{
    /// <summary>
    /// Método principal donde se ejecutan las operaciones
    /// </summary>
    public class OperacionesServicio
    {
        #region Propiedades

        private readonly ConfiguracionServicio _configuracion;
        private readonly IParametrizacion _parametrizacion;
        private readonly IControlServicio _controlServicio;
        private readonly IProcesos _procesos;

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor de Operaciones Servicio
        /// </summary>
        public OperacionesServicio(
            ConfiguracionServicio configuracion,
            IParametrizacion parametrizacion,
            IControlServicio controlServicio,
            IProcesos procesos)
        {
            _configuracion = configuracion;
            _parametrizacion = parametrizacion;
            _controlServicio = controlServicio;
            _procesos = procesos;
        }

        #endregion

        #region Métodos

        /// <summary>
        /// Procedimiento Principal que ejecuta las operaciones
        /// </summary>
        public async Task<bool> EjecutaProcedimientosAsync(CancellationToken ct)
        {
            try
            {
                #region Validaciones Principales

                ct.ThrowIfCancellationRequested();

                //Marca la ejecucion del servicio
                await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct);

                //Obtiene los parámetros del sistema
                _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync(ct);
                _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil(ct);
                _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(Constantes.PARAMETROS_SISTEMA, ct);
                TimeSpan horaSistema = _parametrizacion.ObtenerHoraSistema();
                bool BoEnHorario = false;
                bool tieneHoraOperacionesInicio = false;
                bool tieneHoraOperacionesFin = false;

                if (_configuracion.ParametrosSistema == null || _configuracion.ParametrosSistema.Count == 0)
                    throw new Exception("No se obtuvieron parámetros del sistema");

                foreach (var paramSistema in _configuracion.ParametrosSistema)
                {
                    if (!Enum.TryParse(paramSistema.Key, out ParametrosSistema parametro)) continue;

                    switch (parametro)
                    {
                        case ParametrosSistema.HORA_LECTURA_INICIO:
                            TimeSpan.TryParse(paramSistema.Value, out TimeSpan horaLecturaInicio);
                            _configuracion.tHoraLecturaInicio = horaLecturaInicio;
                            Config.EscribirLog($"INFO|obtenerParametrosSistema|Hora de inicio de lectura '{paramSistema.Value}'. ");
                            break;

                        case ParametrosSistema.HORA_LECTURA_FIN:
                            TimeSpan.TryParse(paramSistema.Value, out TimeSpan horaLecturaFin);
                            _configuracion.tHoraLecturaFin = horaLecturaFin;
                            Config.EscribirLog($"INFO|obtenerParametrosSistema|Hora de Finalización de lectura '{paramSistema.Value}'. ");
                            break;

                        case ParametrosSistema.HORA_OP_INICIO:
                            if (!TimeSpan.TryParse(paramSistema.Value, out TimeSpan horaOperacionesInicio))
                                throw new Exception($"La hora de inicio de operaciones tiene formato inválido: '{paramSistema.Value}'");

                            _configuracion.tHoraOperacionesInicio = horaOperacionesInicio;
                            tieneHoraOperacionesInicio = true;
                            Config.EscribirLog($"INFO|obtenerParametrosSistema|Hora de inicio de operaciones '{paramSistema.Value}'. ");
                            break;

                        case ParametrosSistema.HORA_OP_FIN:
                            if (!TimeSpan.TryParse(paramSistema.Value, out TimeSpan horaOperacionesFin))
                                throw new Exception($"La hora de finalización de operaciones tiene formato inválido: '{paramSistema.Value}'");

                            _configuracion.tHoraOperacionesFin = horaOperacionesFin;
                            tieneHoraOperacionesFin = true;
                            Config.EscribirLog($"INFO|obtenerParametrosSistema|Hora de finalización de operaciones '{paramSistema.Value}'. ");
                            break;
                    }
                }

                if (!tieneHoraOperacionesInicio)
                    throw new Exception("No se especifica la hora de inicio de operaciones");

                if (!tieneHoraOperacionesFin)
                    throw new Exception("No se especifica la hora de finalización de operaciones");

                if (horaSistema >= _configuracion.tHoraOperacionesInicio && horaSistema <= _configuracion.tHoraOperacionesFin)
                {
                    if (BoEnHorario == false)
                    {
                        BoEnHorario = true;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Inicio horario de operaciones, HoraSistema='{horaSistema}'.");
                    }
                }
                else
                {
                    if (BoEnHorario)
                    {
                        BoEnHorario = false;
                        Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Termina horario de operaciones, HoraSistema='{horaSistema}'.");
                    }
                }

                if (!BoEnHorario)
                {
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos| El servicio está fuera de horario.");
                    return false;
                }

                #endregion

                if (!_configuracion.EsDiaHabil.HasValue)
                    throw new Exception("No se pudo determinar si el día es hábil");

                if (_configuracion.EsDiaHabil.Value)
                {
                    if (!await _procesos.InsertaBeneficiariosCMS())
                        return false;

                    if (!await _procesos.ProcesaBeneficiarios())
                        return false;

                    if (!await _procesos.ProcesaParametros())
                        return false;

                    if (!await _procesos.ProcesaBancos())
                        return false;
                }

                await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EjecutaProcedimientosAsync|Se presenta una excepción: {ex}");
                return false;
            }
        }

        #endregion
    }
}

namespace AplicacionSolicitudes.Enums
{
    /// <summary>
    /// Constantes del servicio
    /// </summary>
    public class Constantes
    {
        public const string FormatoSPD = "SPD";
        public const string TipoOperacionBaja = "BAJA";
        public const string EstatusMesaControl = "MESA_CTRL";
        public const string EstatusConfirmado = "CONFIRMADO";
        public const string PARAMETROS_SISTEMA = "'HORA_LECTURA_INICIO','HORA_LECTURA_FIN','HORA_OP_INICIO','HORA_OP_FIN'";
        public const string ALTA = "ALTA";
        public const string RECHAZADO = "RECHAZADO";
        public const string CMS = "CMS";

        public static readonly byte[] CaracteresNoValidos = {
                33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 45, 47, 58, 59, 60, 61, 62,
                63, 64, 91, 92, 93, 94, 95, 96, 123, 124, 125, 126, 127, 128, 129, 130,
                131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144,
                145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158,
                159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173
        };
    }
}

namespace AplicacionSolicitudes.Enums
{
    /// <summary>
    /// Constantes con nombres de los Procedimientos Almacenados
    /// </summary>
    public class ProcedimientoAlmacenado
    {
        #region Stored Procedures

        public const string sp_ObtenerFechaSistema = "usp_ObtenerFechaSistema";
        public const string sp_ParametrosSistema = "dbo.usp_Comun_ObtenerParametrosSistema";
        public const string sp_TOMBIN_INSERT_SOLICITUDES_BENEFICIARIOS = "dbo.usp_SP_TOMBIN_INSERT_SOLICITUDES_BENEFICIARIOS_Masivo";
        public const string sp_ObtenerEntidadesRestringidas = "dbo.usp_Solicitudes_ObtenerEntidadesRestringidas";
        public const string sp_ObtenerEntidadesPermitidas = "dbo.usp_Solicitudes_ObtenerEntidadesPermitidas";
        public const string sp_InsertarAlertasBeneficiario = "dbo.usp_Solicitudes_InsertarAlertas_Beneficiarios";
        public const string sp_ObtenerSolicitudesBeneficarioTomiTomser = "dbo.usp_ObtenerSolicitudesBen_TomiTomser";
        public const string sp_ActualizarSolicitudesbeneficiariosTomiTomser = "dbo.usp_Solicitudes_ActualizaSolicitudesBen_TomiTomser";
        public const string sp_ProcesaPlantillaTOMI = "dbo.usp_Solicitudes_ProcesarPlantillaTOMI";
        public const string sp_ActualizarSolicitudesAltasTomser = "dbo.usp_Solicitudes_ActualizarbeneficiariosTomserAltas";
        public const string sp_ActualizarSolicitudesModificacionesTomser = "dbo.usp_Solicitudes_ActualizarbeneficiariosTomserModificaciones";
        public const string sp_ObtenerParametrosAProcesar = "dbo.usp_Solicitudes_ObtenerParametrosProcesar";
        public const string sp_InsertarParametrosClientesTomi = "dbo.usp_Solicitudes_ActualizarParametrosTOMI";
        public const string sp_ObtenerSolicitudesBancos = "dbo.usp_Solicitudes_ConsultarBancosSolicitudes";
        public const string sp_InsertarAbausaTOMI = "dbo.usp_Solicitudes_InsertarAbausa_TOMI";
        public const string sp_InsertarSwiftMex_TOMI = "dbo.usp_Solicitudes_InsertarSwiftMex_TOMI";
        public const string sp_InsertarSwiftOthBenUsd_TOMI = "dbo.usp_Solicitudes_InsertarSwifOthBenUsd_TOMI";
        public const string sp_InsertarOtherSwift_TOMI = "dbo.usp_Solicitudes_InsertarOthersSwift_TOMI";
        public const string sp_ActualizarSolicitudesBancos = "dbo.usp_Solicitudes_ActualizarBancosSol";

        #endregion

        #region Stored Procedures CMS (Bot)

        public const string sp_ActualizarSolicitudesBeneficiarios = "dbo.MUFG_MX2_Actualizar_Solicitudes_Beneficiarios";
        public const string sp_ActualizarSolicitudesBeneficiariosConRegistroCMS = "dbo.MUFG_MX2_ActualizarSolicitudes_ConRegistroCMS";
        public const string sp_ActualizarSolicitudesBeneficiariosRechazadosCMS = "dbo.MUFG_MX2_ActualizarSolicitudes_RechazadasCMS";
        public const string sp_InsertarParametrosClientesCMS = "dbo.MUFG_MX2_Solicitudes_ActualizarParametrosCMS";
        public const string sp_InsertarAbausaCMS = "dbo.MUFG_MX2_Solicitudes_InsertarAbausa_CMS";
        public const string sp_InsertarOtherSwift_CMS = "dbo.MUFG_MX2_Solicitudes_InsertarOthersSwift_CMS";
        public const string sp_InsertarSwiftOthBenUsd_CMS = "dbo.MUFG_MX2_Solicitudes_InsertarSwifOthBenUsd_CMS";
        public const string sp_InsertarSwiftMex_CMS = "dbo.MUFG_MX2_Solicitudes_InsertarSwiftMex_CMS";
        public const string sp_ObtenerListaSolicitudesBeneficiarios = "dbo.MUFG_MX2_Solicitudes_ObtenerListaSolicitudesBen";
        public const string sp_ProcesaPlantillaCMS = "dbo.MUFG_MX2_Solicitudes_ProcesarPlantillaCMS";
        public const string sp_ValidarBeneficiariosCMS = "dbo.MUFG_MX2_Solicitudes_ValidarBeneficiarios";

        #endregion
    }
}

using System;
using System.Xml.Serialization;

namespace AplicacionSolicitudes.Modelos
{
    /// <summary>
    /// Modelo de Solicitudes Beneficiarios
    /// </summary>
    public class SolicitudesBeneficiariosCmsModelo
    {
        public long ID_SOLICITUD_CMS { get; set; }
        public long ID_SOLICITUD_DOCUMENTO { get; set; }
        public int TIPO { get; set; }
        public long id_Formato { get; set; }
        [XmlElement(IsNullable = true)]
        public string Formato { get; set; }
        public long Id_Cliente { get; set; }
        [XmlElement(IsNullable = true)]
        public string Ordenate { get; set; }
        [XmlElement(IsNullable = true)]
        public string Alias { get; set; }
        [XmlElement(IsNullable = true)]
        public string Nombre { get; set; }
        [XmlElement(IsNullable = true)]
        public string Apellido_P { get; set; }
        [XmlElement(IsNullable = true)]
        public string Apellido_M { get; set; }
        [XmlElement(IsNullable = true)]
        public string TipoBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string ClaveBanco { get; set; }
        [XmlElement(IsNullable = true)]
        public string Banco { get; set; }
        [XmlElement(IsNullable = true)]
        public string TipoCuenta { get; set; }
        [XmlElement(IsNullable = true)]
        public string NoCuenta { get; set; }
        public int Firma { get; set; }
        public decimal MontoOperacion { get; set; }
        [XmlElement(IsNullable = true)]
        public string Moneda { get; set; }
        [XmlElement(IsNullable = true)]
        public string UsuariosCaptura { get; set; }
        [XmlElement(IsNullable = true)]
        public string UsuariosAutoriza { get; set; }
        public int Indicador_Plantilla { get; set; }
        [XmlElement(IsNullable = true)]
        public string Country { get; set; }
        [XmlElement(IsNullable = true)]
        public string ABA { get; set; }
        [XmlElement(IsNullable = true)]
        public string BancoBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string SwiftBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string CuentaBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string SwiftCorresponsal { get; set; }
        [XmlElement(IsNullable = true)]
        public string BancoCorresponsal { get; set; }
        public DateTime? FECHA_SOL { get; set; }
        public string Hora_Sol { get; set; }
        [XmlElement(IsNullable = true)]
        public string USUARIO_CAPT { get; set; }
        public DateTime? FECHA_AUT { get; set; }
        [XmlElement(IsNullable = true)]
        public string USUARIO_AUT { get; set; }
        public string Hora_Aut { get; set; }
        public DateTime? FECHA_CANC { get; set; }
        public string Hora_Canc { get; set; }
        [XmlElement(IsNullable = true)]
        public string USUARIO_CANC { get; set; }
        [XmlElement(IsNullable = true)]
        public string STATUS { get; set; }
        [XmlElement(IsNullable = true)]
        public string OPERACION { get; set; }
        [XmlElement(IsNullable = true)]
        public string COMMENT { get; set; }
        public DateTime? FECHA_CONF { get; set; }
        public string Hora_Conf { get; set; }
        public string USUARIO_CONF { get; set; }
        [XmlElement(IsNullable = true)]
        public string NOTA_CANC { get; set; }
        [XmlElement(IsNullable = true)]
        public string emailBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string emailOrdenante { get; set; }
        public int flgMail { get; set; }
        [XmlElement(IsNullable = true)]
        public string residencia { get; set; }
        [XmlElement(IsNullable = true)]
        public string rfc { get; set; }
        [XmlElement(IsNullable = true)]
        public string origen { get; set; }
        [XmlElement(IsNullable = true)]
        public string country_ben { get; set; }
        public long Id_Agreement { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_res_ben { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_add_ben { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_num_ben { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_col_ben { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_zip_ben { get; set; }
        public int InsertarAvisoMail { get; set; }
    }
}

using System;
using System.Xml.Serialization;

namespace AplicacionSolicitudes.Modelos
{
    /// <summary>
    /// Modelo de Solucitudes Beneficiario 
    /// </summary>
    public class SolicitudesBeneficiarioTomserModelo
    {
        public long ID_SOLICITUD { get; set; }
        public long ID_SOLICITUD_DOCUMENTO { get; set; }
        public int TIPO { get; set; }
        public long id_Formato { get; set; }
        [XmlElement(IsNullable = true)]
        public string Formato { get; set; }
        public long Id_Cliente { get; set; }
        [XmlElement(IsNullable = true)]
        public string Ordenate { get; set; }
        [XmlElement(IsNullable = true)]
        public string Alias { get; set; }
        [XmlElement(IsNullable = true)]
        public string Nombre { get; set; }
        [XmlElement(IsNullable = true)]
        public string Apellido_P { get; set; }
        [XmlElement(IsNullable = true)]
        public string Apellido_M { get; set; }
        [XmlElement(IsNullable = true)]
        public string TipoBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string ClaveBanco { get; set; }
        [XmlElement(IsNullable = true)]
        public string Banco { get; set; }
        [XmlElement(IsNullable = true)]
        public string TipoCuenta { get; set; }
        [XmlElement(IsNullable = true)]
        public string NoCuenta { get; set; }
        public int Firma { get; set; }
        public decimal MontoOperacion { get; set; }
        [XmlElement(IsNullable = true)]
        public string Moneda { get; set; }
        [XmlElement(IsNullable = true)]
        public string UsuariosCaptura { get; set; }
        [XmlElement(IsNullable = true)]
        public string UsuariosAutoriza { get; set; }
        public int Indicador_Plantilla { get; set; }
        [XmlElement(IsNullable = true)]
        public string Country { get; set; }
        [XmlElement(IsNullable = true)]
        public string ABA { get; set; }
        [XmlElement(IsNullable = true)]
        public string BancoBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string SwiftBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string CuentaBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string SwiftCorresponsal { get; set; }
        [XmlElement(IsNullable = true)]
        public string BancoCorresponsal { get; set; }
        public DateTime FECHA_SOL { get; set; }
        public string Hora_Sol { get; set; }
        [XmlElement(IsNullable = true)]
        public string USUARIO_CAPT { get; set; }
        public DateTime FECHA_AUT { get; set; }
        [XmlElement(IsNullable = true)]
        public string USUARIO_AUT { get; set; }
        public string Hora_Aut { get; set; }
        public DateTime? FECHA_CANC { get; set; }
        public string Hora_Canc { get; set; }
        [XmlElement(IsNullable = true)]
        public string USUARIO_CANC { get; set; }
        [XmlElement(IsNullable = true)]
        public string STATUS { get; set; }
        [XmlElement(IsNullable = true)]
        public string OPERACION { get; set; }
        [XmlElement(IsNullable = true)]
        public string COMMENT { get; set; }
        public DateTime? FECHA_CONF { get; set; }
        public string Hora_Conf { get; set; }
        public string USUARIO_CONF { get; set; }
        [XmlElement(IsNullable = true)]
        public string NOTA_CANC { get; set; }
        [XmlElement(IsNullable = true)]
        public string emailBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string emailOrdenante { get; set; }
        public int flgMail { get; set; }
        [XmlElement(IsNullable = true)]
        public string residencia { get; set; }
        [XmlElement(IsNullable = true)]
        public string rfc { get; set; }
        [XmlElement(IsNullable = true)]
        public string origen { get; set; }
        [XmlElement(IsNullable = true)]
        public string country_ben { get; set; }
        public long Id_Agreement { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_res_ben { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_add_ben { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_num_ben { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_col_ben { get; set; }
        [XmlElement(IsNullable = true)]
        public string city_zip_ben { get; set; }

        public string strOrigen { get; set; }
        public string strOperacion { get; set; }
    }
}

namespace AplicacionSolicitudes.Modelos
{
    /// <summary>
    /// Modelo de solicitud banco
    /// </summary>
    public class SolicitudBancoModelo
    {
        public long Id_Solicitud { get; set; }
        public int Tipo { get; set; }
        public string ABA { get; set; }
        public string Cus_Name { get; set; }
        public string State { get; set; }
        public string City { get; set; }
        public string Currency { get; set; }
        public long Ban_Id { get; set; }
        public string Swift_Ben { get; set; }
        public string Cus_Name_Ben { get; set; }
        public string Cus_Name_Acc { get; set; }
        public long Ban_Id_Int { get; set; }
        public string Swift_Int { get; set; }
        public string Cus_Name_Int { get; set; }
        public string Swift_Oth { get; set; }
        public string Cus_Name_Oth { get; set; }
        public string Currency_Oth { get; set; }
        public string Operacion { get; set; }
    }
}

using System.Xml.Serialization;

namespace AplicacionSolicitudes.DTO
{
    /// <summary>
    /// DTO de Valida Beneficiarios
    /// </summary>
    public class ValidaBeneficiariosDto
    {
        public long Id_Solicitud { get; set; }
        [XmlElement(IsNullable = true)]
        public string Formato { get; set; }
        [XmlElement(IsNullable = true)]
        public string Estatus { get; set; }
        public long Id_Cliente { get; set; }
        [XmlElement(IsNullable = true)]
        public string TipoCuenta { get; set; }
        [XmlElement(IsNullable = true)]
        public string NoCuenta { get; set; }
        [XmlElement(IsNullable = true)]
        public string Moneda { get; set; }
        [XmlElement(IsNullable = true)]
        public string ABA { get; set; }
        [XmlElement(IsNullable = true)]
        public string SwiftBeneficiario { get; set; }
        [XmlElement(IsNullable = true)]
        public string SwiftCorresponsal { get; set; }
    }
}using MUFG_T24.Configuracion_utilidades;
using System;

namespace AplicacionSolicitudes.Modelos
{
    /// <summary>
    /// Modelo para configuracion de servicio
    /// </summary>
    public class ConfiguracionServicio : Configuracion
    {
        public TimeSpan tHoraLecturaInicio { get; set; }
        public TimeSpan tHoraLecturaFin { get; set; }
        public TimeSpan tHoraOperacionesInicio { get; set; }
        public TimeSpan tHoraOperacionesFin { get; set; }
        public new ConfiguracionServicio Clone()
        {
            return (ConfiguracionServicio)MemberwiseClone();
        }
    }
}

using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;

public static class Utilidades
{
    /// <summary>
    /// Método para serializar lista de datos.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="listaDatos"></param>
    /// <returns></returns>
    public static string Serializar<T>(List<T> listaDatos)
    {
        var serializer = new XmlSerializer(typeof(List<T>));
        string resultadoXML = string.Empty;

        using (var stringWritter = new StringWriter())
        {
            serializer.Serialize(stringWritter, listaDatos);
            resultadoXML = stringWritter.ToString();
        }

        return resultadoXML;
    }
}

using AplicacionSolicitudes.Enums;
using AplicacionSolicitudes.Interfaces;
using AplicacionSolicitudes.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace InfraestructuraSolicitudes.Repositorios
{
    /// <summary>
    /// Clase donde se implementan las operaciones CMS
    /// </summary>
    public class CMSRepositorio : RepositorioMaestro, ICMS
    {
        #region Constructor

        /// <summary>
        /// Constructor para la conexión a base de datos CMS (Bot)
        /// </summary>
        public CMSRepositorio()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.CMS, null, null) { }

        #endregion

        #region Métodos

        /// <summary>
        /// Función para obtener de base de datos las Solicitudes a procesar
        /// </summary>
        public async Task<List<SolicitudesBeneficiariosCmsModelo>> ObtenerSolicitudes()
        {
            var result = await EjecutarStoredProcedureQuery<SolicitudesBeneficiariosCmsModelo>
                (ProcedimientoAlmacenado.sp_ObtenerListaSolicitudesBeneficiarios);

            if (result.Exito)
                return result.Data;
            else
                throw new ArgumentException($"Ocurrió un errro al ejecutar el Procedimiento Almacenado, error: {result.MensajeError}");
        }

        /// <summary>
        /// Función para Actualizar las Solicitudes de Beneficiarios
        /// </summary>
        public async Task<bool> ActualizarSolicitudesBeneficiarios(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ActualizarSolicitudesBeneficiarios, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrio un error al Actualizar masivamente Las Solicitudes error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para Validar los datos de los beneficiarios
        /// </summary>
        public async Task<List<ValidacionBeneficiariosModel>> ValidaBeneficiarios(string datosXml)
        {

            var result = await EjecutarStoredProcedureQuery<ValidacionBeneficiariosModel>(ProcedimientoAlmacenado.sp_ValidarBeneficiariosCMS, cmd =>
            {
                IDbDataParameter param = cmd.CreateParameter();
                param.ParameterName = "@datosXml";
                param.Value = datosXml;
                param.DbType = DbType.Xml;
                cmd.Parameters.Add(param);
            });

            if (result.Exito)
                return result.Data;
            else
                throw new ArgumentException($"Ocurrió un errro al ejecutar el Procedimiento Almacenado, error: {result.MensajeError}");
        }

        /// <summary>
        /// Función para actualizar la lista de beneficiarios rechazados
        /// </summary>
        public async Task<bool> ActualizarSolicitudesBeneficiariosRechazados(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ActualizarSolicitudesBeneficiariosRechazadosCMS, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrio un error al Actualizar masivamente Las Solicitudes error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para Actualizar la lista de Beneficiarios con un registro previo
        /// </summary>
        public async Task<bool> ActualizarSolicitudesBeneficiariosConregistro(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ActualizarSolicitudesBeneficiariosConRegistroCMS, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrio un error al Actualizar masivamente Las Solicitudes error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para ejecutar el procesamiento de las Solicitudes
        /// </summary>
        public async Task<int> ProcesaPlantilla_CMS(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ProcesaPlantillaCMS, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return 1;
                else
                    return 0;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrio un error al Actualizar masivamente Las Solicitudes error: {ex.Message}");
                return 0;
            }
        }

        /// <summary>
        /// Función para Insertar los parametros de un cliente
        /// </summary>
        public async Task<bool> InsertarParametrosCliente_CMS(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_InsertarParametrosClientesCMS, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocirrio un error an insertar masivamente Los parametros de cliente: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Insertar datos en en CMS acorde al procedimiento almacenado
        /// </summary>
        /// <param name="datosXml"></param>
        /// <param name="procedimientoAlmacenado"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public async Task<ProcesaAutorizacionBancosModelo> InsertarDatosCMS(string datosXml, string procedimientoAlmacenado)
        {
            var result = await EjecutarStoredProcedureQuery<ProcesaAutorizacionBancosModelo>(procedimientoAlmacenado, cmd =>
            {
                IDbDataParameter param = cmd.CreateParameter();
                param.ParameterName = "@datosXml";
                param.Value = datosXml;
                param.DbType = DbType.Xml;
                cmd.Parameters.Add(param);
            });

            if (result.Exito)
                return result.Data.FirstOrDefault();
            else
                throw new ArgumentException($"Ocurrio un error al ejecutar el procedimiento almacenado: '{procedimientoAlmacenado}', error: {result.MensajeError}");
        }

        #endregion
    }
}

using AplicacionSolicitudes.Enums;
using AplicacionSolicitudes.Interfaces;
using AplicacionSolicitudes.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace InfraestructuraSolicitudes.Repositorios
{
    /// <summary>
    /// Clase donde se implementan las operaciones Tomi
    /// </summary>
    public class TomiRepositorio : RepositorioMaestro, ITomi
    {
        #region Constructor

        /// <summary>
        /// Constructor para poder conectar a la base de InterfacesT24
        /// </summary>
        public TomiRepositorio() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        #endregion

        #region Métodos

        /// <summary>
        /// Función para insertar los Beneficiarios en tablas Tombin
        /// </summary>
        public async Task<bool> TombinInsertSolicitudesBeneficiarios(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_TOMBIN_INSERT_SOLICITUDES_BENEFICIARIOS, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocirrio un error an insertar masivamente en Solicitudes Beneficiarios: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para obtener la lista de entidades registradas
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public async Task<List<SPIDEntidadesRestringidasModelo>> ObtenerEntidadesRestringidas()
        {
            var result = await EjecutarStoredProcedureQuery<SPIDEntidadesRestringidasModelo>
                (ProcedimientoAlmacenado.sp_ObtenerEntidadesRestringidas);

            if (result.Exito)
                return result.Data;
            else
                throw new ArgumentException($"Ocurrió un errro al ejecutar el Procedimiento Almacenado, error: {result.MensajeError}");
        }

        /// <summary>
        /// Función para  obtener la lista de entidades permitidas para operar
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public async Task<List<SPIDEntidadesPermitidasModelo>> ObtenerEntidadesPermitidas()
        {
            var result = await EjecutarStoredProcedureQuery<SPIDEntidadesPermitidasModelo>
                (ProcedimientoAlmacenado.sp_ObtenerEntidadesPermitidas);

            if (result.Exito)
                return result.Data;
            else
                throw new ArgumentException($"Ocurrió un errro al ejecutar el Procedimiento Almacenado, error: {result.MensajeError}");
        }

        /// <summary>
        /// Función para insertar alerta de beneficiario
        /// </summary>
        /// <param name="datosXml"></param>
        /// <returns></returns>
        public async Task<bool> InsertarAlertasBeneficiarios(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery
                    (ProcedimientoAlmacenado.sp_InsertarAlertasBeneficiario, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocirrio un error an insertar masivamente Las alertas de Beneficiarios: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para Obtener la lista de solicitudes de beneficiarios
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public async Task<List<SolicitudesBeneficiarioTomserModelo>> ObtenerSolicitudesBeneficiariosTomiTomser()
        {
            var result = await EjecutarStoredProcedureQuery<SolicitudesBeneficiarioTomserModelo>
                (ProcedimientoAlmacenado.sp_ObtenerSolicitudesBeneficarioTomiTomser);

            if (result.Exito)
                return result.Data;
            else
                throw new ArgumentException($"Ocurrió un errro al ejecutar el Procedimiento Almacenado, error: {result.MensajeError}");
        }

        /// <summary>
        /// Función para Actualizas la lista de solicitudes de beneficiarios
        /// </summary>
        public async Task<bool> ActualizaSolicitudesTomiTomser(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ActualizarSolicitudesbeneficiariosTomiTomser, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrió un error an Actualizar masivamente Los Beneficiarios: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para Procesar Plantilla en Base InterfacesT24
        /// </summary>
        public async Task<int> ProcesaPlantillaTOMI(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ProcesaPlantillaTOMI, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return 1;
                else
                    return 0;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrio un error al Actualizar masivamente Las Solicitudes error: {ex.Message}");
                return 0;
            }
        }

        /// <summary>
        /// Función para actualizar las solicitudes de alta
        /// </summary>
        public async Task<bool> ActualizarSolicitudesAltasTomser(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ActualizarSolicitudesAltasTomser, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrio un error al Actualizar masivamente Las Solicitudes error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para Actualizar las solicitudes de Modificación de datos
        /// </summary>
        public async Task<bool> ActualizarSolicitudesModificacionTomser(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_ActualizarSolicitudesModificacionesTomser, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrio un error al Actualizar masivamente Las Solicitudes error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para Obtener la lista de parametros que se van a procesar
        /// </summary>
        public async Task<List<SolicitudParametrosModelo>> ObtenerListaParametrosAProcesar()
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<SolicitudParametrosModelo>(ProcedimientoAlmacenado.sp_ObtenerParametrosAProcesar);

                if (result.Exito)
                    return result.Data;
                else
                    throw new ArgumentException($"Ocurrió un errro al ejecutar el Procedimiento Almacenado, error: {result.MensajeError}");
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocurrio un error al Actualizar masivamente Las Solicitudes error: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Función para Insertar los parámetros del cliente
        /// </summary>
        public async Task<bool> InsertarParametrosClienteTomi(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_InsertarParametrosClientesTomi, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocirrio un error an insertar masivamente Los parametros de cliente: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Función para Obtener la lista de solicitudes de Bancos
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public async Task<List<SolicitudBancoModelo>> ObtenerSolicitudesBancos()
        {
            var result = await EjecutarStoredProcedureQuery<SolicitudBancoModelo>(ProcedimientoAlmacenado.sp_ObtenerSolicitudesBancos);

            if (result.Exito)
                return result.Data;
            else
                throw new ArgumentException($"Ocurrió un errro al ejecutar el Procedimiento Almacenado, error: {result.MensajeError}");
        }

        /// <summary>
        /// Función para actualizar las Solicitudes de Bancos
        /// </summary>
        public async Task<ProcesaAutorizacionBancosModelo> ActualizarSolicitudesBancos(string datosXml)
        {
            try
            {
                var resul = await EjecutarStoredProcedureQuery<ProcesaAutorizacionBancosModelo>(ProcedimientoAlmacenado.sp_ActualizarSolicitudesBancos, cmd =>
                {
                    IDbDataParameter param = cmd.CreateParameter();
                    param.ParameterName = "@datosXml";
                    param.Value = datosXml;
                    param.DbType = DbType.Xml;
                    cmd.Parameters.Add(param);
                });

                if (resul.Exito && resul.Data != null)
                    return resul.Data?.FirstOrDefault();
                else
                    return new ProcesaAutorizacionBancosModelo();
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|OperacionesTOMI|Ocirrio un error an insertar masivamente Los parametros de cliente: {ex.Message}");
                return new ProcesaAutorizacionBancosModelo();
            }
        }

        /// <summary>
        /// Insertar datos en en TOMI acorde al procedimiento almacenado
        /// </summary>
        /// <param name="datosXml"></param>
        /// <param name="procedimientoAlmacenado"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public async Task<ProcesaAutorizacionBancosModelo> InsertarDatosTOMI(string datosXml, string procedimientoAlmacenado)
        {
            var result = await EjecutarStoredProcedureQuery<ProcesaAutorizacionBancosModelo>(procedimientoAlmacenado, cmd =>
            {
                IDbDataParameter param = cmd.CreateParameter();
                param.ParameterName = "@datosXml";
                param.Value = datosXml;
                param.DbType = DbType.Xml;
                cmd.Parameters.Add(param);
            });

            if (result.Exito)
                return result.Data.FirstOrDefault();
            else
                throw new ArgumentException($"Ocurrio un error al ejecutar el procedimiento almacenado: '{procedimientoAlmacenado}', error: {result.MensajeError}");
        }
        #endregion
    }
}

using System.Threading.Tasks;

namespace AplicacionSolicitudes.Interfaces
{
    /// <summary>
    /// Interfaz principal de control de procesos
    /// </summary>
    public interface IProcesos
    {
        /// <summary>
        /// Proceso de benficiarios
        /// </summary>
        /// <returns></returns>
        Task<bool> ProcesaBeneficiarios();

        /// <summary>
        /// Inserta Beneficiarios en CMS
        /// </summary>
        /// <returns></returns>
        Task<bool> InsertaBeneficiariosCMS();

        /// <summary>
        /// Procesa Parámetros
        /// </summary>
        /// <returns></returns>
        Task<bool> ProcesaParametros();

        /// <summary>
        /// Procesa Bancos
        /// </summary>
        /// <returns></returns>
        Task<bool> ProcesaBancos();
    }
}

using AplicacionSolicitudes.Modelos;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AplicacionSolicitudes.Interfaces
{
    /// <summary>
    /// Interfaz de base datos Tomi
    /// </summary>
    public interface ITomi
    {
        /// <summary>
        /// Función para insertar los Beneficiarios en tablas Tombin
        /// </summary>
        Task<bool> TombinInsertSolicitudesBeneficiarios(string datosXml);

        /// <summary>
        /// Función para obtener la lista de entidades registradas
        /// </summary>
        Task<List<SPIDEntidadesRestringidasModelo>> ObtenerEntidadesRestringidas();

        /// <summary>
        /// Función para  obtener la lista de entidades permitidas para operar
        /// </summary>
        Task<List<SPIDEntidadesPermitidasModelo>> ObtenerEntidadesPermitidas();

        /// <summary>
        /// Función para insertar alerta de beneficiario
        /// </summary>
        Task<bool> InsertarAlertasBeneficiarios(string datosXml);

        /// <summary>
        /// Función para Obtener la lista de solicitudes de beneficiarios
        /// </summary>
        Task<List<SolicitudesBeneficiarioTomserModelo>> ObtenerSolicitudesBeneficiariosTomiTomser();

        /// <summary>
        /// Función para Actualizas la lista de solicitudes de beneficiarios
        /// </summary>
        Task<bool> ActualizaSolicitudesTomiTomser(string datosXml);

        /// <summary>
        /// Función para Procesar Plantilla en Base InterfacesT24
        /// </summary>
        Task<int> ProcesaPlantillaTOMI(string datosXml);

        /// <summary>
        /// Función para actualizar las solicitudes de alta
        /// </summary>
        Task<bool> ActualizarSolicitudesAltasTomser(string datosXml);

        /// <summary>
        /// Función para Actualizar las solicitudes de Modificación de datos
        /// </summary>
        Task<bool> ActualizarSolicitudesModificacionTomser(string datosXml);

        /// <summary>
        /// Función para Obtener la lista de parametros que se van a procesar
        /// </summary>
        Task<List<SolicitudParametrosModelo>> ObtenerListaParametrosAProcesar();

        /// <summary>
        /// Función para Insertar los parámetros del cliente
        /// </summary>
        Task<bool> InsertarParametrosClienteTomi(string datosXml);

        /// <summary>
        /// Función para Obtener la lista de solicitudes de Bancos
        /// </summary>
        Task<List<SolicitudBancoModelo>> ObtenerSolicitudesBancos();

        /// <summary>
        /// Función para actualizar las Solicitudes de Bancos
        /// </summary>
        Task<ProcesaAutorizacionBancosModelo> ActualizarSolicitudesBancos(string datosXml);

        /// <summary>
        /// Insertar la lista de datos en CMS
        /// </summary>
        /// <param name="datosXml"></param>
        /// <param name="procedimientoAlmacenado"></param>
        /// <returns></returns>
        Task<ProcesaAutorizacionBancosModelo> InsertarDatosTOMI(string datosXml, string procedimientoAlmacenado);
    }
}

using AplicacionSolicitudes.Modelos;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AplicacionSolicitudes.Interfaces
{
    /// <summary>
    /// Interfaz de llamados a base de datos CMS
    /// </summary>
    public interface ICMS
    {
        /// <summary>
        /// Función para obtener de base de datos las Solicitudes a procesar
        /// </summary>
        Task<List<SolicitudesBeneficiariosCmsModelo>> ObtenerSolicitudes();

        /// <summary>
        /// Función para Actualizar las Solicitudes de Beneficiarios
        /// </summary>
        Task<bool> ActualizarSolicitudesBeneficiarios(string datosXml);

        /// <summary>
        /// Función para Validar los datos de los beneficiarios
        /// </summary>
        Task<List<ValidacionBeneficiariosModel>> ValidaBeneficiarios(string datosXml);

        /// <summary>
        /// Función para actualizar la lista de beneficiarios rechazados
        /// </summary>
        Task<bool> ActualizarSolicitudesBeneficiariosRechazados(string datosXml);

        /// <summary>
        /// Función para Actualizar la lista de Beneficiarios con un registro previo
        /// </summary>
        Task<bool> ActualizarSolicitudesBeneficiariosConregistro(string datosXml);

        /// <summary>
        /// Función para ejecutar el procesamiento de las Solicitudes
        /// </summary>
        Task<int> ProcesaPlantilla_CMS(string datosXml);

        /// <summary>
        /// Función para Insertar los parametros de un cliente
        /// </summary>
        Task<bool> InsertarParametrosCliente_CMS(string datosXml);

        /// <summary>
        /// Inserta informacion en CMS acorde al procedimiento almacenado
        /// </summary>
        /// <param name="datosXml"></param>
        /// <param name="procedimientoAlmacenado"></param>
        /// <returns></returns>
        Task<ProcesaAutorizacionBancosModelo> InsertarDatosCMS(string datosXml, string procedimientoAlmacenado);
    }
}

USE Bot;
GO

/***********************************************************************
NOMBRE              :  dbo.usp_Solicitudes_ObtenerListaSolicitudesBen
SISTEMA/BD          :  BOT
AUTOR               :  Carlos Aguilar
FECHA               :  2026-06-10
 
DESCRIPCIÓN         : 
Obtiene la lista de Solicitudes de Beneficiarios para ser procesadas
 
ALCANCE / NOTAS     :
- Operación DML.
- Se espera retornar filas.
 
PARÁMETROS          :
-
 
CONTROL DE CAMBIOS  : 
2026-06-10 | Carlos Aguilar | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ObtenerListaSolicitudesBen
AS
BEGIN
	SELECT ID_SOLICITUD_CMS,ID_SOLICITUD_DOCUMENTO,TIPO,id_Formato,Formato,Id_Cliente,Ordenate,Alias,Nombre,Apellido_P,Apellido_M,TipoBeneficiario, 
		   ClaveBanco,Banco,TipoCuenta,NoCuenta,Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,Country,ABA,
		   BancoBeneficiario,SwiftBeneficiario,CuentaBeneficiario,SwiftCorresponsal,BancoCorresponsal,FECHA_SOL,USUARIO_CAPT,FECHA_AUT,USUARIO_AUT,FECHA_CANC, 
		   USUARIO_CANC,STATUS,OPERACION,COMMENT,FECHA_CONF,USUARIO_CONF,NOTA_CANC,emailBeneficiario,emailOrdenante,flgMail,residencia,rfc,origen,country_ben,Id_Agreement, 
		   city_res_ben,city_add_ben,city_num_ben,city_col_ben,city_zip_ben 
	FROM tbl_solicitudes_beneficiarios 
	WHERE id_solicitud_cms IN (SELECT id_solicitud_cms FROM tbl_solicitudes_beneficiarios WHERE status = 'AUTORIZADO')
END
GO

/********************************************************************/
/***********************************************************************    
NOMBRE              :  dbo.usp_Actualizar_Solicitudes_Beneficiarios    
SISTEMA/BD          :  InterfacesT24    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Actualiza el estatus de las solicitudes Procesadas Masivamente  
     
ALCANCE / NOTAS     :    
- Operación DML.    
- No se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    

CREATE OR ALTER PROCEDURE dbo.usp_Actualizar_Solicitudes_Beneficiarios  
(  
    @datosXml XML  
)   
AS    
BEGIN    
  
    SET NOCOUNT ON;  
    SET XACT_ABORT ON;  
  
    BEGIN TRY  
        
        BEGIN TRANSACTION;  
   
        -- 1. Variable tabla
        DECLARE @tmpSolicitudes TABLE
        (
            ID_SOLICITUD_DOCUMENTO BIGINT
        );

        -- 2. Insertar desde XML
        INSERT INTO @tmpSolicitudes (ID_SOLICITUD_DOCUMENTO)
        SELECT 
            T.N.value('(IdSolicitud)[1]','BIGINT')
        FROM @datosXml.nodes('ArrayOfSolicitudesBeneficiarioDto/SolicitudesBeneficiarioDto') AS T(N);    

        -- 3. Update 1
        UPDATE sol   
        SET STATUS = 'ENBT'  
        FROM tbl_solicitudes_beneficiarios sol  
        INNER JOIN @tmpSolicitudes tm 
            ON tm.ID_SOLICITUD_DOCUMENTO = sol.ID_SOLICITUD_CMS;  

        -- 4. Update 2
        UPDATE req   
        SET Status = 'ENBT'  
        FROM tbl_requests_pt_ps req  
        INNER JOIN @tmpSolicitudes tm 
            ON tm.ID_SOLICITUD_DOCUMENTO = req.Id_Request_CMS_Detail;  

        IF @@ROWCOUNT = 0  
            THROW 50001, 'No se actualizaron registros', 1;  

        COMMIT TRANSACTION;  
          
    END TRY  
  
    BEGIN CATCH  
  
        IF XACT_STATE() <> 0  
            ROLLBACK TRANSACTION;  
   
        INSERT INTO Auditoria.ErrorLog  
        (  
            ErrorNumber, ErrorSeverity, ErrorState,  
            ErrorProcedure, ErrorLine, ErrorMessage  
        )  
        VALUES  
        (  
            ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(),  
            ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()  
        );  
   
        THROW;  
    END CATCH  
END
GO

/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_ValidarBeneficiarios  
SISTEMA/BD          :  BOT  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-10  
   
DESCRIPCIÓN         :   
Valida las concidencias de beneficiarios con Id_Cliente, NoCuenta, Moneda  
iguales  
   
ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-10 | Carlos Aguilar | Creación SP  
***********************************************************************/  
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ValidarBeneficiarios    
(    
    @datosXml XML    
)    
AS    
BEGIN    

    SET NOCOUNT ON;  
    SET XACT_ABORT ON;



        ;WITH Solicitudes AS
        (
            SELECT  
                T.N.value('(Id_Solicitud)[1]','BIGINT') AS Id_SolicitudOriginal,
                T.N.value('(Formato)[1]','VARCHAR(3)') AS Formato,
                T.N.value('(Estatus)[1]','VARCHAR(25)') AS Estatus,
                T.N.value('(Id_Cliente)[1]','BIGINT') AS Id_Cliente,
                T.N.value('(TipoCuenta)[1]','VARCHAR(100)') AS TipoCuenta,
                T.N.value('(NoCuenta)[1]','VARCHAR(45)') AS NoCuenta,
                T.N.value('(Moneda)[1]','VARCHAR(3)') AS Moneda,
                T.N.value('(ABA)[1]','VARCHAR(9)') AS ABA,
                T.N.value('(SwiftBeneficiario)[1]','VARCHAR(11)') AS SwiftBeneficiario,
                T.N.value('(SwiftCorresponsal)[1]','VARCHAR(11)') AS SwiftCorresponsal
            FROM @datosXml.nodes('ArrayOfValidaBeneficiariosDto/ValidaBeneficiariosDto') AS T(N)
        )

        -- 🔹 RESULTADO FINAL (UNION DE AMBOS CASOS)
        SELECT  
            ISNULL(b.Id_Formato,0) AS Id_Solicitud,
            s.Id_SolicitudOriginal,
            s.Formato,
            s.Estatus,
            s.Id_Cliente,
            s.TipoCuenta,
            s.NoCuenta,
            s.Moneda,
            s.ABA,
            s.SwiftBeneficiario,
            s.SwiftCorresponsal,
            COUNT(b.Id_Formato) AS Cont
        FROM Solicitudes s
        LEFT JOIN beneficiarios b 
            ON b.Formato = s.Formato 
           AND b.Id_Cliente = s.Id_Cliente 
           AND b.TipoCuenta = s.TipoCuenta 
           AND b.NoCuenta = s.NoCuenta  
        WHERE s.Formato IN ('FXD','SPD','FXT','SER','TAX')
        GROUP BY 
            b.Id_Formato,
            s.Id_SolicitudOriginal,
            s.Formato,
            s.Estatus,
            s.Id_Cliente,
            s.TipoCuenta,
            s.NoCuenta,
            s.Moneda,
            s.ABA,
            s.SwiftBeneficiario,
            s.SwiftCorresponsal

        UNION ALL

        SELECT  
            ISNULL(b.Id_Formato,0) AS Id_Solicitud,
            s.Id_SolicitudOriginal,
            s.Formato,
            s.Estatus,
            s.Id_Cliente,
            s.TipoCuenta,
            s.NoCuenta,
            s.Moneda,
            s.ABA,
            s.SwiftBeneficiario,
            s.SwiftCorresponsal,
            COUNT(b.Id_Formato) AS Cont
        FROM Solicitudes s
        LEFT JOIN beneficiarios b 
            ON b.Formato = s.Formato 
           AND b.Id_Cliente = s.Id_Cliente 
           AND b.NoCuenta = s.NoCuenta  
           AND b.Moneda = s.Moneda 
           AND b.ABA = s.ABA 
           AND b.SwiftBeneficiario = s.SwiftBeneficiario 
           AND b.SwiftCorresponsal = s.SwiftCorresponsal  
        WHERE s.Formato IN ('FXI','FXE')
        GROUP BY 
            b.Id_Formato,
            s.Id_SolicitudOriginal,
            s.Formato,
            s.Estatus,
            s.Id_Cliente,
            s.TipoCuenta,
            s.NoCuenta,
            s.Moneda,
            s.ABA,
            s.SwiftBeneficiario,
            s.SwiftCorresponsal;
END
GO


/***********************************************************************  
NOMBRE              :  dbo.usp_ActualizarSolicitudes_RechazadasCMS  
SISTEMA/BD          :  BOT  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-10  
   
DESCRIPCIÓN         :   
Actualiza la tabla tbl_solicitudes_beneficiarios, los campos:  
status, FECHA_CANC, USUARIO_CANC, NOTA_CANC  
   
ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-10 | Carlos Aguilar | Creación SP  
***********************************************************************/  

CREATE OR ALTER PROCEDURE dbo.usp_ActualizarSolicitudes_RechazadasCMS  
(    
    @datosXml XML    
)    
AS    
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON; 
    
    BEGIN TRY
          
        BEGIN TRANSACTION;  

        UPDATE t  
        SET STATUS = 'CANCELADO',  
            FECHA_CANC = GETDATE(),  
            USUARIO_CANC = 'CMS',  
            NOTA_CANC = 'Beneficiario rechazado por Mesa de Control'  
        FROM tbl_solicitudes_beneficiarios t  
        INNER JOIN (
            SELECT 
                T.N.value('(Id_SolicitudOriginal)[1]','BIGINT') AS Id_Solicitud_Documento  
            FROM @datosXml.nodes('ArrayOfValidaBeneficiariosDto/ValidaBeneficiariosDto') AS T(N)
        ) x
            ON x.Id_Solicitud_Documento = t.ID_SOLICITUD_CMS;  
    
        COMMIT TRANSACTION;  
    
    END TRY

    BEGIN CATCH

        IF XACT_STATE() <> 0  
            ROLLBACK TRANSACTION;  
   
        INSERT INTO Auditoria.ErrorLog  
        (  
            ErrorNumber,  
            ErrorSeverity,  
            ErrorState,  
            ErrorProcedure,  
            ErrorLine,  
            ErrorMessage  
        )  
        VALUES  
        (  
            ERROR_NUMBER(),  
            ERROR_SEVERITY(),  
            ERROR_STATE(),  
            ERROR_PROCEDURE(),  
            ERROR_LINE(),  
            ERROR_MESSAGE()  
        );  
   
        THROW;  

    END CATCH

END
GO

/***********************************************************************      
NOMBRE              :  dbo.usp_Solicitudes_ProcesarPlantillaCMS      
SISTEMA/BD          :  BOT      
AUTOR               :  Carlos Aguilar      
FECHA               :  2026-06-10      
       
DESCRIPCIÓN         :       
Actualiza la tabla tbl_solicitudes_beneficiarios, los campos:      
status, FECHA_CANC, USUARIO_CANC, NOTA_CANC      
       
ALCANCE / NOTAS     :      
- Operación DML.      
- Se espera retornar filas.      
       
PARÁMETROS          :      
-      
       
CONTROL DE CAMBIOS  :       
2026-06-10 | Carlos Aguilar | Creación SP      
***********************************************************************/      
CREATE OR ALTER  PROCEDURE dbo.usp_Solicitudes_ProcesarPlantillaCMS      
(        
 @datosXml XML        
)        
AS        
BEGIN        
 SET NOCOUNT ON;  
    SET XACT_ABORT ON;  
   
 BEGIN TRY  
  
    BEGIN TRANSACTION;    
  
     
   DECLARE @tmpSolicitudes TABLE  
   (  
    Id_Solicitud_Documento BIGINT,  
    Id_Solicitud BIGINT,  
    Id_Formato BIGINT,  
    Formato VARCHAR(3),  
    Operacion VARCHAR(25),  
    Id_Cliente BIGINT,  
    Alias VARCHAR(100),  
    Nombre VARCHAR(100),  
    ClaveBanco VARCHAR(5),  
    Banco VARCHAR(100),  
    TipoCuenta VARCHAR(100),  
    NoCuenta VARCHAR(45),  
    Firma INT,  
    MontoOperacion DECIMAL(18,2),  
    Moneda CHAR(3),  
    UsuariosCaptura VARCHAR(250),  
    UsuariosAutoriza VARCHAR(250),  
    Indicador_Plantilla INT,  
    Country VARCHAR(6),  
    ABA VARCHAR(9),  
    BancoBeneficiario VARCHAR(100),  
    SwiftBeneficiario VARCHAR(11),  
    CuentaBeneficiario VARCHAR(25),  
    SwiftCorresponsal VARCHAR(11),  
    BancoCorresponsal VARCHAR(100),  
    residencia VARCHAR(100),  
    rfc VARCHAR(20),  
    Origen VARCHAR(5),  
    country_ben VARCHAR(5),  
    Comment VARCHAR(100),  
    EmailBeneficiario VARCHAR(250),  
    EmailOrdenante VARCHAR(250),  
    TipoBeneficiario VARCHAR(1),  
    Apellido_P VARCHAR(40),  
    Apellido_M VARCHAR(40),  
    Id_Agreement BIGINT,  
    city_res_ben VARCHAR(32),  
    city_add_ben VARCHAR(45),  
    city_num_ben VARCHAR(15),  
    city_col_ben VARCHAR(30),  
    city_zip_ben VARCHAR(10),  
    InsertarAvisoMail INT  
   );  
  
   INSERT INTO @tmpSolicitudes  
   SELECT   
    T.N.value('(ID_SOLICITUD_DOCUMENTO)[1]','BIGINT') ID_SOLICITUD_DOCUMENTO,  
    T.N.value('(ID_SOLICITUD)[1]','BIGINT') Id_Solicitud,  
    T.N.value('(id_Formato)[1]','BIGINT') Id_Formato,  
    T.N.value('(Formato)[1]','VARCHAR(3)') Formato,  
    T.N.value('(OPERACION)[1]','VARCHAR(25)') Operacion,  
    T.N.value('(Id_Cliente)[1]','BIGINT') Id_Cliente,  
    T.N.value('(Alias)[1]','VARCHAR(100)') Alias,  
    T.N.value('(Nombre)[1]','VARCHAR(100)') Nombre,  
    T.N.value('(ClaveBanco)[1]','VARCHAR(5)') ClaveBanco,  
    T.N.value('(Banco)[1]','VARCHAR(100)') Banco,  
    T.N.value('(TipoCuenta)[1]','VARCHAR(100)') TipoCuenta,  
    T.N.value('(NoCuenta)[1]','VARCHAR(45)') NoCuenta,  
    T.N.value('(Firma)[1]','INT') Firma,  
    T.N.value('(MontoOperacion)[1]','DECIMAL(10,0)') MontoOperacion,  
    T.N.value('(Moneda)[1]','CHAR(3)') Moneda,  
    T.N.value('(UsuariosCaptura)[1]','VARCHAR(250)') UsuariosCaptura,  
    T.N.value('(USUARIO_AUT)[1]','VARCHAR(250)') USUARIO_AUT,  
    T.N.value('(Indicador_Plantilla)[1]','INT') Indicador_Plantilla,  
    T.N.value('(Country)[1]','VARCHAR(6)') Country,  
    T.N.value('(ABA)[1]','VARCHAR(9)') ABA,  
    T.N.value('(BancoBeneficiario)[1]','VARCHAR(100)') BancoBeneficiario,  
    T.N.value('(SwiftBeneficiario)[1]','VARCHAR(11)') SwiftBeneficiario,  
    T.N.value('(CuentaBeneficiario)[1]','VARCHAR(25)') CuentaBeneficiario,  
    T.N.value('(SwiftCorresponsal)[1]','VARCHAR(11)') SwiftCorresponsal,  
    T.N.value('(BancoCorresponsal)[1]','VARCHAR(100)') BancoCorresponsal,  
    T.N.value('(residencia)[1]','VARCHAR(100)') residencia,  
    T.N.value('(rfc)[1]','VARCHAR(20)') rfc,  
    T.N.value('(origen)[1]','VARCHAR(5)') origen,  
    T.N.value('(country_ben)[1]','VARCHAR(5)') country_ben,  
    T.N.value('(COMMENT)[1]','VARCHAR(100)') COMMENT,  
    T.N.value('(emailBeneficiario)[1]','VARCHAR(250)') emailBeneficiario,  
    T.N.value('(emailOrdenante)[1]','VARCHAR(250)') emailOrdenante,  
    T.N.value('(TipoBeneficiario)[1]','VARCHAR(1)') TipoBeneficiario,  
    T.N.value('(Apellido_P)[1]','VARCHAR(40)') Apellido_P,  
    T.N.value('(Apellido_M)[1]','VARCHAR(40)') Apellido_M,  
    T.N.value('(Id_Agreement)[1]','BIGINT') Id_Agreement,  
    T.N.value('(city_res_ben)[1]','VARCHAR(32)') city_res_ben,  
    T.N.value('(city_add_ben)[1]','VARCHAR(45)') city_add_ben,  
    T.N.value('(city_num_ben)[1]','VARCHAR(15)') city_num_ben,  
    T.N.value('(city_col_ben)[1]','VARCHAR(30)') city_col_ben,  
    T.N.value('(city_zip_ben)[1]','VARCHAR(10)') city_zip_ben,  
    T.N.value('(InsertarAvisoMail)[1]','INT') InsertarAvisoMail   
   FROM @datosXml.nodes('ArrayOfSolicitudesBeneficiarioTomserModelo/SolicitudesBeneficiarioTomserModelo') as T(N);  
  
     
   /*      
    *  ALtas Para FXD, FXT, SPD, SER, TAX      
   */      
  
   DECLARE @tmpAltaOp1 TABLE  
   (  
    Id_Solicitud BIGINT,  
    Cont INT  
   );  
  
   INSERT INTO @tmpAltaOp1  
   SELECT s.Id_Solicitud, COUNT(b.Id_Formato)  
   FROM @tmpSolicitudes s  
   LEFT JOIN beneficiarios b on b.Id_Formato = s.Id_Solicitud  
   WHERE s.Formato IN ('FXD', 'FXT', 'SPD', 'SER', 'TAX')  
   AND UPPER(s.Operacion) = 'ALTA'  
   GROUP BY s.Id_Solicitud;  
  
   -- Insertamos las Operaciones donde Cont = 0      
   INSERT INTO beneficiarios (id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,Firma,MontoOperacion,  
   Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement)  
   SELECT s.Id_Solicitud, s.Formato, Id_Cliente, Alias, Nombre, ClaveBanco, Banco, TipoCuenta, NoCuenta, Firma, MontoOperacion,  
   Moneda, UsuariosCaptura, UsuariosAutoriza, Indicador_Plantilla, rfc, Comment, TipoBeneficiario, Apellido_P, Apellido_M, Id_Agreement  
   FROM @tmpSolicitudes s  
   INNER JOIN @tmpAltaOp1 a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE a.Cont = 0;  
  
   -- Actualizamos las Operaciones en tbl_solicitudes_beneficiarios donde Cont > 0 y de Origen CMS      
   UPDATE t  
   SET status='PROCESADO', id_Formato = s.Id_Solicitud  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpSolicitudes s on s.Id_Solicitud_Documento = t.ID_SOLICITUD_CMS  
   INNER JOIN @tmpAltaOp1 a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE a.Cont > 0;  
  
   -- Actualizamos las Operaciones donde Cont > 0 y tipo SER o TAX      
   UPDATE t  
   SET status='PROCESADO', id_Formato = s.Id_Solicitud  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpSolicitudes s on s.Id_Solicitud_Documento = t.ID_SOLICITUD_CMS  
   INNER JOIN @tmpAltaOp1 a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE a.Cont > 0  
   AND s.Operacion IN ('SER','TAX');  
  
   -- Avisos      
   INSERT INTO nf_beneficiarios_aviso (nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,id_email_beneficiario,  
   id_email_ordenante,id_email_ordenante_error)  
   SELECT Formato, Id_Cliente, Alias, s.Id_Solicitud,1, EmailBeneficiario, EmailOrdenante, EmailOrdenante  
   FROM @tmpSolicitudes s  
   INNER JOIN @tmpAltaOp1 a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE s.InsertarAvisoMail = 1;  
  
     
   /*      
    *  Modificación Para FXD, FXT, SPD, SER, TAX      
   */      
  
   DECLARE @tmpModificacionOp1 TABLE  
   (  
    Id_Solicitud_Documento BIGINT,  
    Id_Solicitud BIGINT,  
    Id_Formato BIGINT,  
    Formato VARCHAR(3),  
    Operacion VARCHAR(25),  
    Id_Cliente BIGINT,  
    Alias VARCHAR(100),  
    Nombre VARCHAR(100),  
    ClaveBanco VARCHAR(5),  
    Banco VARCHAR(100),  
    TipoCuenta VARCHAR(100),  
    NoCuenta VARCHAR(45),  
    Firma INT,  
    MontoOperacion DECIMAL(18,2),  
    Moneda CHAR(3),  
    UsuariosCaptura VARCHAR(250),  
    UsuariosAutoriza VARCHAR(250),  
    Indicador_Plantilla INT,  
    Country VARCHAR(6),  
    ABA VARCHAR(9),  
    BancoBeneficiario VARCHAR(100),  
    SwiftBeneficiario VARCHAR(11),  
    CuentaBeneficiario VARCHAR(25),  
    SwiftCorresponsal VARCHAR(11),  
    BancoCorresponsal VARCHAR(100),  
    residencia VARCHAR(100),  
    rfc VARCHAR(20),  
    Origen VARCHAR(5),  
    country_ben VARCHAR(5),  
    Comment VARCHAR(100),  
    EmailBeneficiario VARCHAR(250),  
    EmailOrdenante VARCHAR(250),  
    TipoBeneficiario VARCHAR(1),  
    Apellido_P VARCHAR(40),  
    Apellido_M VARCHAR(40),  
    Id_Agreement BIGINT,  
    city_res_ben VARCHAR(32),  
    city_add_ben VARCHAR(45),  
    city_num_ben VARCHAR(15),  
    city_col_ben VARCHAR(30),  
    city_zip_ben VARCHAR(10),  
    InsertarAvisoMail INT  
   );  
  
   INSERT INTO @tmpModificacionOp1  
   SELECT *  
   FROM @tmpSolicitudes s  
   WHERE s.Formato IN ('FXD', 'FXT', 'SPD', 'SER', 'TAX')  
   AND UPPER(s.Operacion) = 'MODIFICACION';  
  
   -- Actualizamos beneficiarios con los Movimientos de Modificación      
   UPDATE b  
   SET Firma = s.Firma,  
    MontoOperacion = s.MontoOperacion,  
    UsuariosCaptura = s.UsuariosCaptura,  
    UsuariosAutoriza = s.UsuariosAutoriza,  
    TipoBeneficiario = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.TipoBeneficiario   
     ELSE b.TipoBeneficiario   
    END,  
    Nombre = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Nombre   
     ELSE b.Nombre   
    END,  
    Apellido_P = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Apellido_P   
     ELSE b.Apellido_P   
    END,  
    Apellido_M = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Apellido_M   
     ELSE b.Apellido_M   
    END  
   FROM beneficiarios b  
   INNER JOIN @tmpModificacionOp1 s on s.Id_Formato = b.Id_Formato;  
  
   -- Actualizamos las Operaciones Con Origen CMS      
   UPDATE t  
   SET status='PROCESADO',  
    id_Formato = s.Id_Formato  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpModificacionOp1 s on s.Id_Solicitud_Documento = t.id_solicitud_cms  
   WHERE UPPER(s.Origen) = 'CMS';  
  
   UPDATE t  
   SET status='PROCESADO',  
    Id_Template = s.Id_Formato  
   FROM tbl_requests_pt_ps t  
   INNER JOIN @tmpModificacionOp1 s on s.Id_Solicitud_Documento = t.Id_Request_CMS_Detail  
   WHERE UPPER(s.Origen) = 'CMS';  
  
   -- Avisos      
   DELETE FROM nf_beneficiarios_aviso   
   WHERE id_formato_plantilla IN   
   (  
    SELECT Id_Formato   
    FROM @tmpModificacionOp1   
    WHERE InsertarAvisoMail = 1  
   );  
  
   INSERT INTO nf_beneficiarios_aviso   
   (  
    nf_tipo,  
    nf_cliente,  
    id_plantilla,  
    id_formato_plantilla,  
    id_envio,  
    id_email_beneficiario,  
    id_email_ordenante,  
    id_email_ordenante_error  
   )  
   SELECT   
    Formato,  
    Id_Cliente,  
    Alias,  
    s.Id_Solicitud,  
    1,  
    EmailBeneficiario,  
    EmailOrdenante,  
    EmailOrdenante  
   FROM @tmpModificacionOp1 s  
   WHERE s.InsertarAvisoMail = 1;  
  
     
   /*      
    *  Bajas Para FXD, FXT, SPD, SER, TAX      
   */      
  
   DECLARE @tmpBajaOp1 TABLE  
   (  
    Id_Solicitud_Documento BIGINT,  
    Id_Solicitud BIGINT,  
    Id_Formato BIGINT,  
    Formato VARCHAR(3),  
    Operacion VARCHAR(25),  
    Id_Cliente BIGINT,  
    Alias VARCHAR(100),  
    Nombre VARCHAR(100),  
    ClaveBanco VARCHAR(5),  
    Banco VARCHAR(100),  
    TipoCuenta VARCHAR(100),  
    NoCuenta VARCHAR(45),  
    Firma INT,  
    MontoOperacion DECIMAL(18,2),  
    Moneda CHAR(3),  
    UsuariosCaptura VARCHAR(250),  
    UsuariosAutoriza VARCHAR(250),  
    Indicador_Plantilla INT,  
    Country VARCHAR(6),  
    ABA VARCHAR(9),  
    BancoBeneficiario VARCHAR(100),  
    SwiftBeneficiario VARCHAR(11),  
    CuentaBeneficiario VARCHAR(25),  
    SwiftCorresponsal VARCHAR(11),  
    BancoCorresponsal VARCHAR(100),  
    residencia VARCHAR(100),  
    rfc VARCHAR(20),  
    Origen VARCHAR(5),  
    country_ben VARCHAR(5),  
    Comment VARCHAR(100),  
    EmailBeneficiario VARCHAR(250),  
    EmailOrdenante VARCHAR(250),  
    TipoBeneficiario VARCHAR(1),  
    Apellido_P VARCHAR(40),  
    Apellido_M VARCHAR(40),  
    Id_Agreement BIGINT,  
    city_res_ben VARCHAR(32),  
    city_add_ben VARCHAR(45),  
    city_num_ben VARCHAR(15),  
    city_col_ben VARCHAR(30),  
    city_zip_ben VARCHAR(10),  
    InsertarAvisoMail INT  
   );  
  
   INSERT INTO @tmpBajaOp1  
   SELECT *  
   FROM @tmpSolicitudes s  
   WHERE s.Formato IN ('FXD', 'FXT', 'SPD', 'SER', 'TAX')  
   AND UPPER(s.Operacion) = 'BAJA';  
  
   -- Actualizamos Beneficiarios con Movimientos de Baja    
   UPDATE b  
   SET Indicador_Plantilla = 3  
   FROM beneficiarios b  
   INNER JOIN @tmpBajaOp1 s on s.Id_Formato = b.Id_Formato;  
  
   -- Actualizamos las Operaciones Con Origen CMS      
   UPDATE t  
   SET status='PROCESADO',  
    id_Formato = s.Id_Formato  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpBajaOp1 s on s.Id_Solicitud_Documento = t.id_solicitud_cms  
   WHERE UPPER(s.Origen) = 'CMS';  
  
   UPDATE t  
   SET status='PROCESADO',  
    Id_Template = s.Id_Formato  
   FROM tbl_requests_pt_ps t  
   INNER JOIN @tmpBajaOp1 s on s.Id_Solicitud_Documento = t.Id_Request_CMS_Detail  
   WHERE UPPER(s.Origen) = 'CMS';  
  
   -- Avisos    
   DELETE FROM nf_beneficiarios_aviso   
   WHERE id_formato_plantilla IN  
   (  
    SELECT Id_Formato  
    FROM @tmpBajaOp1  
    WHERE InsertarAvisoMail = 1  
   );  
  
   INSERT INTO nf_beneficiarios_aviso   
   (  
    nf_tipo,  
    nf_cliente,  
    id_plantilla,  
    id_formato_plantilla,  
    id_envio,  
    id_email_beneficiario,  
    id_email_ordenante,  
    id_email_ordenante_error  
   )  
   SELECT   
    Formato,  
    Id_Cliente,  
    Alias,  
    s.Id_Solicitud,  
    1,  
    EmailBeneficiario,  
    EmailOrdenante,  
    EmailOrdenante  
   FROM @tmpBajaOp1 s  
   WHERE s.InsertarAvisoMail = 1;  
  
     
   /*      
    *  ALTAS Para FXI      
   */    
  
   DECLARE @tmpAltaFXI TABLE  
   (  
    Id_Solicitud BIGINT,  
    Cont INT  
   );  
  
   INSERT INTO @tmpAltaFXI  
   SELECT s.Id_Solicitud, COUNT(b.Id_Formato)  
   FROM @tmpSolicitudes s  
   LEFT JOIN beneficiarios b on b.Id_Formato = s.Id_Solicitud  
   WHERE s.Formato IN ('FXI')  
   AND UPPER(s.Operacion) = 'ALTA'  
   GROUP BY s.Id_Solicitud;  

   SELECT * FROM @tmpAltaFXI; -- TEST
  
   -- Insertamos las Operaciones donde Cont = 0      
   INSERT INTO beneficiarios  
   (  
    id_formato,  
    formato,  
    id_cliente,  
    Alias,  
    Country,  
    ABA,  
    BancoBeneficiario,  
    SwiftBeneficiario,  
    CuentaBeneficiario,  
    Firma,  
    MontoOperacion,  
    Moneda,  
    UsuariosCaptura,  
    UsuariosAutoriza,  
    Indicador_Plantilla,  
    SwiftCorresponsal,  
    BancoCorresponsal,  
    Nombre,  
    NoCuenta,  
    Comment,  
    residencia,  
    rfc,  
    country_ben,  
    TipoBeneficiario,  
    Apellido_P,  
    Apellido_M,  
    city_res_ben,  
    city_add_ben,  
    city_num_ben,  
    city_col_ben,  
    city_zip_ben  
   )  
   SELECT  
    s.Id_Solicitud,  
    formato,  
    id_cliente,  
    Alias,  
    Country,  
    ABA,  
    BancoBeneficiario,  
    SwiftBeneficiario,  
    CuentaBeneficiario,  
    Firma,  
    MontoOperacion,  
    Moneda,  
    UsuariosCaptura,  
    UsuariosAutoriza,  
    Indicador_Plantilla,  
    SwiftCorresponsal,  
    BancoCorresponsal,  
    Nombre,  
    NoCuenta,  
    Comment,  
    residencia,  
    rfc,  
    country_ben,  
    TipoBeneficiario,  
    Apellido_P,  
    Apellido_M,  
    city_res_ben,  
    city_add_ben,  
    city_num_ben,  
    city_col_ben,  
    city_zip_ben  
   FROM @tmpSolicitudes s  
   INNER JOIN @tmpAltaFXI a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE a.Cont = 0;  
  
   -- Actualizamos las Operaciones donde Cont > 0      
   UPDATE t  
   SET status='PROCESADO',  
    id_Formato = s.Id_Solicitud  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpSolicitudes s on s.Id_Solicitud_Documento = t.ID_SOLICITUD_CMS  
   INNER JOIN @tmpAltaFXI a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE a.Cont > 0;  
  
   -- Avisos      
   INSERT INTO nf_beneficiarios_aviso  
   (  
    nf_tipo,  
    nf_cliente,  
    id_plantilla,  
    id_formato_plantilla,  
    id_envio,  
    id_email_beneficiario,  
    id_email_ordenante,  
    id_email_ordenante_error  
   )  
   SELECT   
    Formato,  
    Id_Cliente,  
    Alias,  
    s.Id_Solicitud,  
    1,  
    EmailBeneficiario,  
    EmailOrdenante,  
    EmailOrdenante  
   FROM @tmpSolicitudes s  
   INNER JOIN @tmpAltaFXI a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE s.InsertarAvisoMail = 1;  
    
   /*      
    *  Modificación Para FXI    
   */      
  
   DECLARE @tmpModificacionFXI TABLE  
   (  
    Id_Solicitud_Documento BIGINT,  
    Id_Solicitud BIGINT,  
    Id_Formato BIGINT,  
    Formato VARCHAR(3),  
    Operacion VARCHAR(25),  
    Id_Cliente BIGINT,  
    Alias VARCHAR(100),  
    Nombre VARCHAR(100),  
    ClaveBanco VARCHAR(5),  
    Banco VARCHAR(100),  
    TipoCuenta VARCHAR(100),  
    NoCuenta VARCHAR(45),  
    Firma INT,  
    MontoOperacion DECIMAL(18,2),  
    Moneda CHAR(3),  
    UsuariosCaptura VARCHAR(250),  
    UsuariosAutoriza VARCHAR(250),  
    Indicador_Plantilla INT,  
    Country VARCHAR(6),  
    ABA VARCHAR(9),  
    BancoBeneficiario VARCHAR(100),  
    SwiftBeneficiario VARCHAR(11),  
    CuentaBeneficiario VARCHAR(25),  
    SwiftCorresponsal VARCHAR(11),  
    BancoCorresponsal VARCHAR(100),  
    residencia VARCHAR(100),  
    rfc VARCHAR(20),  
    Origen VARCHAR(5),  
    country_ben VARCHAR(5),  
    Comment VARCHAR(100),  
    EmailBeneficiario VARCHAR(250),  
    EmailOrdenante VARCHAR(250),  
    TipoBeneficiario VARCHAR(1),  
    Apellido_P VARCHAR(40),  
    Apellido_M VARCHAR(40),  
    Id_Agreement BIGINT,  
    city_res_ben VARCHAR(32),  
    city_add_ben VARCHAR(45),  
    city_num_ben VARCHAR(15),  
    city_col_ben VARCHAR(30),  
    city_zip_ben VARCHAR(10),  
    InsertarAvisoMail INT  
   );  
  
   INSERT INTO @tmpModificacionFXI  
   SELECT *  
   FROM @tmpSolicitudes s  
   WHERE s.Formato IN ('FXI')  
   AND UPPER(s.Operacion) = 'MODIFICACION';  
  
   -- Actualizamos beneficiarios con los Movimientos de Modificación      
   UPDATE b  
   SET Firma = s.Firma,  
    MontoOperacion = s.MontoOperacion,  
    UsuariosCaptura = s.UsuariosCaptura,  
    UsuariosAutoriza = s.UsuariosAutoriza,  
    TipoBeneficiario = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.TipoBeneficiario   
     ELSE b.TipoBeneficiario   
    END,  
    Nombre = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Nombre   
     ELSE b.Nombre   
    END,  
    Apellido_P = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Apellido_P   
     ELSE b.Apellido_P   
    END,  
    Apellido_M = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Apellido_M   
     ELSE b.Apellido_M   
    END,  
    country_ben = CASE   
     WHEN s.country_ben IS NULL OR TRIM(s.country_ben) <> ''   
     THEN s.country_ben   
     ELSE b.country_ben   
    END,  
    city_res_ben = s.city_res_ben,  
    city_add_ben = s.city_add_ben,  
    city_num_ben = s.city_num_ben,  
    city_col_ben = s.city_col_ben,  
    city_zip_ben = s.city_zip_ben  
   FROM beneficiarios b  
   INNER JOIN @tmpModificacionFXI s on s.Id_Formato = b.Id_Formato;  
  
   -- Actualizamos las Operaciones Con Origen CMS      
   UPDATE t  
   SET status='PROCESADO',  
    id_Formato = s.Id_Formato  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpModificacionFXI s on s.Id_Solicitud_Documento = t.id_solicitud_cms  
   WHERE UPPER(s.Origen) = 'CMS';  
  
   -- Avisos      
   DELETE FROM nf_beneficiarios_aviso  
   WHERE id_formato_plantilla IN  
   (  
    SELECT Id_Formato  
    FROM @tmpModificacionFXI  
    WHERE InsertarAvisoMail = 1  
   );  
  
   INSERT INTO nf_beneficiarios_aviso  
   (  
    nf_tipo,  
    nf_cliente,  
    id_plantilla,  
    id_formato_plantilla,  
    id_envio,  
    id_email_beneficiario,  
    id_email_ordenante,  
    id_email_ordenante_error  
   )  
   SELECT   
    Formato,  
    Id_Cliente,  
    Alias,  
    s.Id_Solicitud,  
    1,  
    EmailBeneficiario,  
    EmailOrdenante,  
    EmailOrdenante  
   FROM @tmpModificacionFXI s  
   WHERE s.InsertarAvisoMail = 1;  
  
     
   /*      
    *  Bajas Para FXI    
   */      
  
   DECLARE @tmpBajaFXI TABLE  
   (  
    Id_Solicitud_Documento BIGINT,  
    Id_Solicitud BIGINT,  
    Id_Formato BIGINT,  
    Formato VARCHAR(3),  
    Operacion VARCHAR(25),  
    Id_Cliente BIGINT,  
    Alias VARCHAR(100),  
    Nombre VARCHAR(100),  
    ClaveBanco VARCHAR(5),  
    Banco VARCHAR(100),  
    TipoCuenta VARCHAR(100),  
    NoCuenta VARCHAR(45),  
    Firma INT,  
    MontoOperacion DECIMAL(18,2),  
    Moneda CHAR(3),  
    UsuariosCaptura VARCHAR(250),  
    UsuariosAutoriza VARCHAR(250),  
    Indicador_Plantilla INT,  
    Country VARCHAR(6),  
    ABA VARCHAR(9),  
    BancoBeneficiario VARCHAR(100),  
    SwiftBeneficiario VARCHAR(11),  
    CuentaBeneficiario VARCHAR(25),  
    SwiftCorresponsal VARCHAR(11),  
    BancoCorresponsal VARCHAR(100),  
    residencia VARCHAR(100),  
    rfc VARCHAR(20),  
    Origen VARCHAR(5),  
    country_ben VARCHAR(5),  
    Comment VARCHAR(100),  
    EmailBeneficiario VARCHAR(250),  
    EmailOrdenante VARCHAR(250),  
    TipoBeneficiario VARCHAR(1),  
    Apellido_P VARCHAR(40),  
    Apellido_M VARCHAR(40),  
    Id_Agreement BIGINT,  
    city_res_ben VARCHAR(32),  
    city_add_ben VARCHAR(45),  
    city_num_ben VARCHAR(15),  
    city_col_ben VARCHAR(30),  
    city_zip_ben VARCHAR(10),  
    InsertarAvisoMail INT  
   );  
  
   INSERT INTO @tmpBajaFXI  
   SELECT *  
   FROM @tmpSolicitudes s  
   WHERE s.Formato IN ('FXI')  
   AND UPPER(s.Operacion) = 'BAJA';  
  
   -- Actualizamos Beneficiarios con Movimientos de Baja    
   UPDATE b  
   SET Indicador_Plantilla = 3  
   FROM beneficiarios b  
   INNER JOIN @tmpBajaFXI s on s.Id_Formato = b.Id_Formato;  
  
   -- Actualizamos las Operaciones Con Origen CMS      
   UPDATE t  
   SET status='PROCESADO',  
    id_Formato = s.Id_Formato  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpBajaFXI s on s.Id_Solicitud_Documento = t.id_solicitud_cms  
   WHERE UPPER(s.Origen) = 'CMS';  
  
   -- Eliminamos los avisos      
   DELETE FROM nf_beneficiarios_aviso  
   WHERE id_formato_plantilla IN  
   (  
    SELECT Id_Formato  
    FROM @tmpBajaFXI  
    WHERE InsertarAvisoMail = 1  
   );  
  
     
   /*      
    *  Altas Para FXE    
   */      
  
   DECLARE @tmpAltaFXE TABLE  
   (  
    Id_Solicitud BIGINT,  
    Cont INT  
   );  
  
   INSERT INTO @tmpAltaFXE  
   SELECT s.Id_Solicitud, COUNT(b.Id_Formato)  
   FROM @tmpSolicitudes s  
   LEFT JOIN beneficiarios b on b.Id_Formato = s.Id_Solicitud  
   WHERE s.Formato IN ('FXE')  
   AND UPPER(s.Operacion) = 'ALTA'  
   GROUP BY s.Id_Solicitud;  
  
   -- Insertamos las Operaciones donde Cont = 0      
   INSERT INTO beneficiarios  
   (  
    id_formato,  
    formato,  
    id_cliente,  
    Alias,  
    Country,  
    ABA,  
    BancoBeneficiario,  
    SwiftBeneficiario,  
    CuentaBeneficiario,  
    Firma,  
    MontoOperacion,  
    Moneda,  
    UsuariosCaptura,  
    UsuariosAutoriza,  
    Indicador_Plantilla,  
    SwiftCorresponsal,  
    BancoCorresponsal,  
    Nombre,  
    NoCuenta,  
    Comment,  
    residencia,  
    rfc,  
    country_ben,  
    TipoBeneficiario,  
    Apellido_P,  
    Apellido_M,  
    city_res_ben,  
    city_add_ben,  
    city_num_ben,  
    city_col_ben,  
    city_zip_ben  
   )  
   SELECT  
    id_formato,  
    formato,  
    id_cliente,  
    Alias,  
    Country,  
    ABA,  
    BancoBeneficiario,  
    SwiftBeneficiario,  
    CuentaBeneficiario,  
    Firma,  
    MontoOperacion,  
    Moneda,  
    UsuariosCaptura,  
    UsuariosAutoriza,  
    Indicador_Plantilla,  
    SwiftCorresponsal,  
    BancoCorresponsal,  
    Nombre,  
    NoCuenta,  
    Comment,  
    residencia,  
    rfc,  
    country_ben,  
    TipoBeneficiario,  
    Apellido_P,  
    Apellido_M,  
    city_res_ben,  
    city_add_ben,  
    city_num_ben,  
    city_col_ben,  
    city_zip_ben  
   FROM @tmpSolicitudes s  
   INNER JOIN @tmpAltaFXE a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE a.Cont = 0;  
  
   -- Actualizamos las Operaciones donde Cont > 0 y de Origen CMS      
   UPDATE t  
   SET status='PROCESADO',  
    id_Formato = s.Id_Solicitud  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpSolicitudes s on s.Id_Solicitud_Documento = t.ID_SOLICITUD_CMS  
   INNER JOIN @tmpAltaFXE a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE a.Cont > 0;  
  
   -- Avisos      
   INSERT INTO nf_beneficiarios_aviso  
   (  
    nf_tipo,  
    nf_cliente,  
    id_plantilla,  
    id_formato_plantilla,  
    id_envio,  
    id_email_beneficiario,  
    id_email_ordenante,  
    id_email_ordenante_error  
   )  
   SELECT   
    Formato,  
    Id_Cliente,  
    Alias,  
    s.Id_Solicitud,  
    1,  
    EmailBeneficiario,  
    EmailOrdenante,  
    EmailOrdenante  
   FROM @tmpSolicitudes s  
   INNER JOIN @tmpAltaFXE a on a.Id_Solicitud = s.Id_Solicitud  
   WHERE s.InsertarAvisoMail = 1;  
  
     
   /*      
    *  Modificación Para FXE    
   */      
  
   DECLARE @tmpModificacionFXE TABLE  
   (  
    Id_Solicitud_Documento BIGINT,  
    Id_Solicitud BIGINT,  
    Id_Formato BIGINT,  
    Formato VARCHAR(3),  
    Operacion VARCHAR(25),  
    Id_Cliente BIGINT,  
    Alias VARCHAR(100),  
    Nombre VARCHAR(100),  
    ClaveBanco VARCHAR(5),  
    Banco VARCHAR(100),  
    TipoCuenta VARCHAR(100),  
    NoCuenta VARCHAR(45),  
    Firma INT,  
    MontoOperacion DECIMAL(18,2),  
    Moneda CHAR(3),  
    UsuariosCaptura VARCHAR(250),  
    UsuariosAutoriza VARCHAR(250),  
    Indicador_Plantilla INT,  
    Country VARCHAR(6),  
    ABA VARCHAR(9),  
    BancoBeneficiario VARCHAR(100),  
    SwiftBeneficiario VARCHAR(11),  
    CuentaBeneficiario VARCHAR(25),  
    SwiftCorresponsal VARCHAR(11),  
    BancoCorresponsal VARCHAR(100),  
    residencia VARCHAR(100),  
    rfc VARCHAR(20),  
    Origen VARCHAR(5),  
    country_ben VARCHAR(5),  
    Comment VARCHAR(100),  
    EmailBeneficiario VARCHAR(250),  
    EmailOrdenante VARCHAR(250),  
    TipoBeneficiario VARCHAR(1),  
    Apellido_P VARCHAR(40),  
    Apellido_M VARCHAR(40),  
    Id_Agreement BIGINT,  
    city_res_ben VARCHAR(32),  
    city_add_ben VARCHAR(45),  
    city_num_ben VARCHAR(15),  
    city_col_ben VARCHAR(30),  
    city_zip_ben VARCHAR(10),  
    InsertarAvisoMail INT  
   );  
  
   INSERT INTO @tmpModificacionFXE  
   SELECT *  
   FROM @tmpSolicitudes s  
   WHERE s.Formato IN ('FXE')  
   AND UPPER(s.Operacion) = 'MODIFICACION';  
  
   -- Actualizamos beneficiarios con los Movimientos de Modificación      
   UPDATE b  
   SET Firma = s.Firma,  
    MontoOperacion = s.MontoOperacion,  
    UsuariosCaptura = s.UsuariosCaptura,  
    UsuariosAutoriza = s.UsuariosAutoriza,  
    TipoBeneficiario = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.TipoBeneficiario   
     ELSE b.TipoBeneficiario   
    END,  
    Nombre = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Nombre   
     ELSE b.Nombre   
    END,  
    Apellido_P = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Apellido_P   
     ELSE b.Apellido_P   
    END,  
    Apellido_M = CASE   
     WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> ''   
     THEN s.Apellido_M   
     ELSE b.Apellido_M   
    END,  
    country_ben = CASE   
     WHEN s.country_ben IS NULL OR TRIM(s.country_ben) <> ''   
     THEN s.country_ben   
     ELSE b.country_ben   
    END,  
    city_res_ben = s.city_res_ben,  
    city_add_ben = s.city_add_ben,  
    city_num_ben = s.city_num_ben,  
    city_col_ben = s.city_col_ben,  
    city_zip_ben = s.city_zip_ben,  
    BancoBeneficiario = s.BancoBeneficiario,  
    SwiftBeneficiario = s.SwiftBeneficiario,  
    CuentaBeneficiario = s.CuentaBeneficiario,  
    SwiftCorresponsal = s.SwiftCorresponsal,  
    BancoCorresponsal = s.BancoCorresponsal  
   FROM beneficiarios b  
   INNER JOIN @tmpModificacionFXE s on s.Id_Formato = b.Id_Formato;  
  
   -- Actualizamos las Operaciones Con Origen CMS      
   UPDATE t  
   SET status='PROCESADO',  
    id_Formato = s.Id_Formato  
   FROM tbl_solicitudes_beneficiarios t  
   INNER JOIN @tmpModificacionFXE s on s.Id_Solicitud_Documento = t.id_solicitud_cms  
   WHERE UPPER(s.Origen) = 'CMS';  
  
   -- Avisos      
   DELETE FROM nf_beneficiarios_aviso   
   WHERE id_formato_plantilla IN  
   (  
    SELECT Id_Formato  
    FROM @tmpModificacionFXE  
    WHERE InsertarAvisoMail = 1  
   );  
  
   INSERT INTO nf_beneficiarios_aviso  
   (  
    nf_tipo,  
    nf_cliente,  
    id_plantilla,  
    id_formato_plantilla,  
    id_envio,  
    id_email_beneficiario,  
    id_email_ordenante,  
    id_email_ordenante_error  
   )  
   SELECT   
    Formato,  
    Id_Cliente,  
    Alias,  
    s.Id_Solicitud,  
    1,  
    EmailBeneficiario,  
    EmailOrdenante,  
    EmailOrdenante  
   FROM @tmpModificacionFXE s  
   WHERE s.InsertarAvisoMail = 1;  
  
   COMMIT TRANSACTION;    
  
 END TRY  
  
 BEGIN CATCH  
  
    IF XACT_STATE() <> 0    
        ROLLBACK TRANSACTION;    
  
            INSERT INTO Auditoria.ErrorLog    
        (    
            ErrorNumber,    
            ErrorSeverity,    
            ErrorState,    
            ErrorProcedure,    
            ErrorLine,    
            ErrorMessage    
        )    
        VALUES    
        (    
            ERROR_NUMBER(),    
            ERROR_SEVERITY(),    
            ERROR_STATE(),    
            ERROR_PROCEDURE(),    
            ERROR_LINE(),    
            ERROR_MESSAGE()    
        );    
     
        THROW;    
  
 END CATCH  

END  
GO

/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_ActualizarParametrosCMS    
SISTEMA/BD          :  Bot   
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-10    
     
DESCRIPCIÓN         :     
Borra los parametros de las aolicitudes y actualiza la tabla  
parametros_clientee con los nuevos valores  
     
ALCANCE / NOTAS     :    
- Operación DML.    
- No Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-10 | Carlos Aguilar | Creación SP    
***********************************************************************/    

CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ActualizarParametrosCMS    
(      
 @datosXml XML      
)      
AS      
BEGIN    
  
    SET NOCOUNT ON;  
    SET XACT_ABORT ON;  
  
BEGIN TRY  
        
    BEGIN TRANSACTION;  
  
    DECLARE @tmpParametrosSol TABLE
    (
        Descripcion VARCHAR(100),
        Id_Solicitud BIGINT,
        Id_Formato BIGINT,
        Formato VARCHAR(3),
        Id_Cliente BIGINT,
        TipoFormato INT,
        MontoOperacion DECIMAL(18,2),
        Moneda VARCHAR(3),
        Firma INT
    );

    INSERT INTO @tmpParametrosSol
    SELECT 
        T.N.value('(Descripcion)[1]','VARCHAR(100)'),
        T.N.value('(Id_Solicitud)[1]','BIGINT'),
        T.N.value('(Id_Formato)[1]','BIGINT'),
        T.N.value('(Formato)[1]','VARCHAR(3)'),
        T.N.value('(Id_Cliente)[1]','BIGINT'),
        T.N.value('(TipoFormato)[1]','INT'),
        T.N.value('(MontoOperacion)[1]','DECIMAL(18,2)'),
        T.N.value('(Moneda)[1]','VARCHAR(3)'),
        T.N.value('(Firma)[1]','INT')
    FROM @datosXml.nodes('ArrayOfSolicitudParametrosModelo/SolicitudParametrosModelo') as T(N);    
  
    DELETE c  
    FROM parametros_cliente c  
    INNER JOIN @tmpParametrosSol p 
        ON p.Id_Formato = c.Id_Formato 
       AND p.Id_Cliente = c.Id_Cliente;  
  
    INSERT INTO parametros_cliente
        (Descripcion, Id_Solicitud, Id_Formato, Formato, Id_Cliente, TipoFormato, MontoOperacion, Moneda, Firma)  
    SELECT 
        Descripcion, Id_Solicitud, Id_Formato, Formato, Id_Cliente, TipoFormato, MontoOperacion, Moneda, Firma  
    FROM @tmpParametrosSol;  
  
    COMMIT TRANSACTION;  
      
END TRY  
  
BEGIN CATCH  
  
    IF XACT_STATE() <> 0  
        ROLLBACK TRANSACTION;  
   
    INSERT INTO Auditoria.ErrorLog  
    (  
        ErrorNumber,  
        ErrorSeverity,  
        ErrorState,  
        ErrorProcedure,  
        ErrorLine,  
        ErrorMessage  
    )  
    VALUES  
    (  
        ERROR_NUMBER(),  
        ERROR_SEVERITY(),  
        ERROR_STATE(),  
        ERROR_PROCEDURE(),  
        ERROR_LINE(),  
        ERROR_MESSAGE()  
    );  
   
    THROW;  

END CATCH  

END
GO

/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_InsertarAbausa_CMS  
SISTEMA/BD          :  Bot  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-11  
   
DESCRIPCIÓN         :   
Inserta de forma masiva en la tabla: abausa

ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-11 | Carlos Aguilar | Creación SP  
***********************************************************************/  
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_InsertarAbausa_CMS
(
	@datosXml	XML
)
AS
BEGIN

	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @regsInicio int = 0;
	DECLARE @regsFinal int = 0;

	BEGIN TRY

		BEGIN TRANSACTION;

		SELECT @regsInicio = count(*) FROM abausa;

		
		DECLARE @tmpBancosSol TABLE
				(
					ABA VARCHAR(9),
					Cus_Name VARCHAR(36),
					State VARCHAR(2),
					City VARCHAR(25),
					Currency VARCHAR(3)
				);

	    INSERT INTO @tmpBancosSol (ABA, Cus_Name, State, City, Currency)
		SELECT T.N.value('(ABA)[1]','VARCHAR(9)')  as ABA,
			   T.N.value('(Cus_Name)[1]','VARCHAR(36)')  as Cus_Name,
			   T.N.value('(State)[1]','VARCHAR(2)')  as State,
			   T.N.value('(City)[1]','VARCHAR(25)')  as City,
			   T.N.value('(Currency)[1]','VARCHAR(3)')  as Currency
		FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);  

		INSERT INTO abausa(ABA,CUS_NAME,STATE,CITY,CURRENCY)
		SELECT ABA, Cus_Name, State, City, Currency FROM @tmpBancosSol;

		SELECT @regsFinal = count(*) FROM abausa;

		SELECT (@regsFinal - @regsInicio) AS Insertados
		

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
		IF XACT_STATE() <> 0
			ROLLBACK TRANSACTION;
 
		INSERT INTO Auditoria.ErrorLog
		(
			ErrorNumber,
			ErrorSeverity,
			ErrorState,
			ErrorProcedure,
			ErrorLine,
			ErrorMessage
		)
		VALUES
		(
			ERROR_NUMBER(),
			ERROR_SEVERITY(),
			ERROR_STATE(),
			ERROR_PROCEDURE(),
			ERROR_LINE(),
			ERROR_MESSAGE()
		);
 
		THROW;
	END CATCH;

END
GO

/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_InsertarSwiftMex_CMS  
SISTEMA/BD          :  Bot  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-11  
   
DESCRIPCIÓN         :   
Inserta de forma masiva en la tabla: swift_mex
y swift_mex_int

ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-11 | Carlos Aguilar | Creación SP  
***********************************************************************/  
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_InsertarSwiftMex_CMS 
(
	@datosXml	XML
)
AS
BEGIN

	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @regsInicio int = 0;
	DECLARE @regsFinal int = 0;

	BEGIN TRY

		BEGIN TRANSACTION;

		SELECT @regsInicio = count(*) FROM swift_mex;

		
		DECLARE @tmpBancosSol TABLE
				(
					Ban_Id BIGINT,
					Swift_Ben VARCHAR(11),
					Cus_Name_Ben VARCHAR(100),
					Cus_Name_Acc VARCHAR(25),
					Swift_Int VARCHAR(11),
					Cus_Name_Int VARCHAR(100)
				);

	    INSERT INTO @tmpBancosSol (Ban_Id, Swift_Ben, Cus_Name_Ben, Cus_Name_Acc,Swift_Int,Cus_Name_Int)
		SELECT T.N.value('(Ban_Id)[1]','BIGINT')  as Ban_Id,
			   T.N.value('(Swift_Ben)[1]','VARCHAR(11)')  as Swift_Ben,
			   T.N.value('(Cus_Name_Ben)[1]','VARCHAR(100)')  as Cus_Name_Ben,
			   T.N.value('(Cus_Name_Acc)[1]','VARCHAR(25)')  as Cus_Name_Acc,
			   T.N.value('(Swift_Int)[1]','VARCHAR(11)')  as Swift_Int,
			   T.N.value('(Cus_Name_Int)[1]','VARCHAR(100)')  as Cus_Name_Int
		FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);  

		INSERT INTO swift_mex(BAN_ID,SWIFT_BEN,CUS_NAME_BEN,CUS_NAME_ACC)
		SELECT Ban_Id, Swift_Ben, Cus_Name_Ben, Cus_Name_Acc FROM @tmpBancosSol;

		INSERT INTO swift_mex_int(BAN_ID_MEX,BAN_ID_INT,SWIFT_INT,CUS_NAME_INT,CUS_NAME_ACC)
		SELECT Ban_Id,1, Swift_Int, Cus_Name_Int, '' as Cus_Name_Acc FROM @tmpBancosSol;

		SELECT @regsFinal = count(*) FROM swift_mex;

		SELECT (@regsFinal - @regsInicio) AS Insertados
		

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
		IF XACT_STATE() <> 0
			ROLLBACK TRANSACTION;
 
		INSERT INTO Auditoria.ErrorLog
		(
			ErrorNumber,
			ErrorSeverity,
			ErrorState,
			ErrorProcedure,
			ErrorLine,
			ErrorMessage
		)
		VALUES
		(
			ERROR_NUMBER(),
			ERROR_SEVERITY(),
			ERROR_STATE(),
			ERROR_PROCEDURE(),
			ERROR_LINE(),
			ERROR_MESSAGE()
		);
 
		THROW;
	END CATCH;

END
GO

/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_InsertarSwifOthBenUsd_CMS    
SISTEMA/BD          :  Bot    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Inserta de forma masiva en la tabla: swift_othben_usd  
y swift_othint_usd  
  
ALCANCE / NOTAS     :    
- Operación DML.    
- Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE OR ALTER  PROCEDURE dbo.usp_Solicitudes_InsertarSwifOthBenUsd_CMS   
(  
 @datosXml XML  
)  
AS  
BEGIN  
  
 SET NOCOUNT ON;  
 SET XACT_ABORT ON;  
  
 DECLARE @regsInicio int = 0;  
 DECLARE @regsFinal int = 0;  
  
 BEGIN TRY  
  
  BEGIN TRANSACTION;  
  
	  SELECT @regsInicio = count(*) FROM swift_othben_usd;  
  
    
	  DECLARE @tmpBancosSol TABLE  
		(  
		 Ban_Id BIGINT,  
		 Swift_Ben VARCHAR(11),  
		 Cus_Name_Ben VARCHAR(100),  
		 Ban_id_int BIGINT,  
		 Swift_Int VARCHAR(11),  
		 Cus_Name_Int VARCHAR(100)  
		);  
  
      INSERT INTO @tmpBancosSol (Ban_Id, Swift_Ben, Cus_Name_Ben, Ban_id_int,Swift_Int,Cus_Name_Int)  
	  SELECT T.N.value('(Ban_Id)[1]','BIGINT')  as Ban_Id,  
		  T.N.value('(Swift_Ben)[1]','VARCHAR(11)')  as Swift_Ben,  
		  T.N.value('(Cus_Name_Ben)[1]','VARCHAR(100)')  as Cus_Name_Ben,  
		  T.N.value('(ban_id_int)[1]','BIGINT')  as Ban_id_int,  
		  T.N.value('(Swift_Int)[1]','VARCHAR(11)')  as Swift_Int,  
		  T.N.value('(Cus_Name_Int)[1]','VARCHAR(100)')  as Cus_Name_Int  
	  FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);    
  
	  INSERT INTO swift_othben_usd(BAN_ID,SWIFT_BEN,CUS_NAME_BEN)  
	  SELECT Ban_Id, Swift_Ben, Cus_Name_Ben FROM @tmpBancosSol;  
  
	  INSERT INTO swift_othint_usd(BAN_ID,SWIFT_BEN,CUS_NAME_BEN)  
	  SELECT Ban_id_int, Swift_Int, Cus_Name_Int FROM @tmpBancosSol;  
  
	  SELECT @regsFinal = count(*) FROM swift_othben_usd;  
  
	  SELECT (@regsFinal - @regsInicio) AS Insertados  
    
  
  COMMIT TRANSACTION;  
  
 END TRY  
  
 BEGIN CATCH  
  IF XACT_STATE() <> 0  
   ROLLBACK TRANSACTION;  
   
  INSERT INTO Auditoria.ErrorLog  
  (  
   ErrorNumber,  
   ErrorSeverity,  
   ErrorState,  
   ErrorProcedure,  
   ErrorLine,  
   ErrorMessage  
  )  
  VALUES  
  (  
   ERROR_NUMBER(),  
   ERROR_SEVERITY(),  
   ERROR_STATE(),  
   ERROR_PROCEDURE(),  
   ERROR_LINE(),  
   ERROR_MESSAGE()  
  );  
   
  THROW;  
 END CATCH;  
  
END  
GO

/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_InsertarOthersSwift_CMS    
SISTEMA/BD          :  Bot    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Inserta de forma masiva en la tabla: others_swift
  
ALCANCE / NOTAS     :    
- Operación DML.    
- Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE OR ALTER  PROCEDURE dbo.usp_Solicitudes_InsertarOthersSwift_CMS   
(  
 @datosXml XML  
)  
AS  
BEGIN  
  
 SET NOCOUNT ON;  
 SET XACT_ABORT ON;  
  
 DECLARE @regsInicio int = 0;  
 DECLARE @regsFinal int = 0;  
  
 BEGIN TRY  
  
  BEGIN TRANSACTION;  
  
	  SELECT @regsInicio = count(*) FROM others_swift;  
  
    
	  DECLARE @tmpBancosSol TABLE  
		(  
		 ABA VARCHAR(15),   
		 Cus_Name VARCHAR(100),  
		 Currency VARCHAR(3)  
		);  
  
      INSERT INTO @tmpBancosSol (ABA, Cus_Name, Currency)  
	  SELECT T.N.value('(swift_oth)[1]','VARCHAR(15)')  as ABA,  
		  T.N.value('(Cus_Name_Oth)[1]','VARCHAR(100)')  as Cus_Name,  
		  T.N.value('(Currency_Oth)[1]','VARCHAR(3)')  as Currency
	  FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);    
  
	  INSERT INTO others_swift (ABA,CUS_NAME,CURRENCY)  
	  SELECT ABA, Cus_Name, Currency FROM @tmpBancosSol;  
  
  
	  SELECT @regsFinal = count(*) FROM others_swift;  
  
	  SELECT (@regsFinal - @regsInicio) AS Insertados  
    
  
  COMMIT TRANSACTION;  
  
 END TRY  
  
 BEGIN CATCH  
  IF XACT_STATE() <> 0  
   ROLLBACK TRANSACTION;  
   
  INSERT INTO Auditoria.ErrorLog  
  (  
   ErrorNumber,  
   ErrorSeverity,  
   ErrorState,  
   ErrorProcedure,  
   ErrorLine,  
   ErrorMessage  
  )  
  VALUES  
  (  
   ERROR_NUMBER(),  
   ERROR_SEVERITY(),  
   ERROR_STATE(),  
   ERROR_PROCEDURE(),  
   ERROR_LINE(),  
   ERROR_MESSAGE()  
  );  
   
  THROW;  
 END CATCH;  
  
END  
GO


USE InterfacesT24;
/***********************************************************************    
NOMBRE              :  dbo.usp_SP_TOMBIN_INSERT_SOLICITUDES_BENEFICIARIOS_Masivo    
SISTEMA/BD          :  InterfacesT24    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Inserta en las tablas Tomser.dbo.SOLICITUDES_BENEFICIARIOS y    
TB_BILUN_SOLICITUDES_BENEFICIARIOS De forma masiva a traves    
de un objeto Xml    
     
ALCANCE / NOTAS     :    
- Operación DML.    
- No se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE OR ALTER PROCEDURE dbo.usp_SP_TOMBIN_INSERT_SOLICITUDES_BENEFICIARIOS_Masivo
(    
 @datosXml XML    
)    
AS    
BEGIN    
  
 SET NOCOUNT ON;  
    SET XACT_ABORT ON;  
  
 BEGIN TRY  
    
  BEGIN TRANSACTION;  
    
		DECLARE @tmpSolicitudes TABLE
		(
			ID_SOLICITUD_DOCUMENTO BIGINT,
			TIPO INT,
			ID_FORMATO BIGINT,
			FORMATO CHAR(3),
			ID_CLIENTE BIGINT,
			ORDENATE VARCHAR(250),
			ALIAS VARCHAR(100),
			NOMBRE VARCHAR(100),
			CLAVEBANCO VARCHAR(5),
			BANCO VARCHAR(100),
			TIPOCUENTA VARCHAR(100),
			NOCUENTA VARCHAR(45),
			FIRMA INT,
			MONTOOPERACION DECIMAL(18,2),
			MONEDA CHAR(3),
			USUARIOSCAPTURA VARCHAR(250),
			USUARIOSAUTORIZA VARCHAR(250),
			INDICADOR_PLANTILLA INT,
			COUNTRY VARCHAR(6),
			ABA VARCHAR(9),
			BANCOBENEFICIARIO VARCHAR(100),
			SWIFTBENEFICIARIO VARCHAR(11),
			CUENTABENEFICIARIO VARCHAR(25),
			SWIFTCORRESPONSAL VARCHAR(11),
			BANCOCORRESPONSAL VARCHAR(100),
			FECHA_SOL DATETIME,
			HORA_SOL VARCHAR(5),
			USUARIO_CAPT VARCHAR(15),
			FECHA_AUT DATETIME,
			HORA_AUT VARCHAR(5),
			USUARIO_AUT VARCHAR(15),
			FECHA_CANC DATETIME,
			HORA_CANC VARCHAR(5),
			USUARIO_CANC VARCHAR(15),
			STATUS VARCHAR(25),
			OPERACION VARCHAR(25),
			COMMENT VARCHAR(100),
			FECHA_CONF DATETIME,
			HORA_CONF VARCHAR(5),
			USUARIO_CONF VARCHAR(15),
			NOTA_CANC VARCHAR(250),
			EMAILBENEFICIARIO VARCHAR(250),
			EMAILORDENANTE VARCHAR(250),
			FLGMAIL BIT,
			RESIDENCIA VARCHAR(100),
			RFC VARCHAR(20),
			ORIGEN VARCHAR(5),
			COUNTRY_BEN VARCHAR(5),
			TIPOBENEFICIARIO VARCHAR(1),
			APELLIDO_P VARCHAR(40),
			APELLIDO_M VARCHAR(40),
			ID_AGREEMENT BIGINT,
			city_res_ben VARCHAR(32),
			city_add_ben VARCHAR(45),
			city_num_ben VARCHAR(15),
			city_col_ben VARCHAR(30),
			city_zip_ben VARCHAR(10)
		);

    WITH XMLNAMESPACES ('http://www.w3.org/2001/XMLSchema-instance' AS xsi)
	INSERT INTO @tmpSolicitudes
    SELECT T.N.value('(ID_SOLICITUD_CMS)[1]','BIGINT')  as ID_SOLICITUD_DOCUMENTO,    
     T.N.value('(TIPO)[1]','INT')  as TIPO,    
     T.N.value('(id_Formato)[1]','BIGINT')  as ID_FORMATO,    
     T.N.value('(Formato)[1]','CHAR(3)')  as FORMATO,    
     T.N.value('(Id_Cliente)[1]','BIGINT')  as ID_CLIENTE,    
     T.N.value('(Ordenate)[1]','VARCHAR(250)')  as ORDENATE,    
     T.N.value('(Alias)[1]','VARCHAR(100)')  as ALIAS,  
     T.N.value('(Nombre)[1]','VARCHAR(100)')  as NOMBRE,  
     T.N.value('(ClaveBanco)[1]','VARCHAR(5)')  as CLAVEBANCO,    
     T.N.value('(Banco)[1]','VARCHAR(100)')  as BANCO,    
     T.N.value('(TipoCuenta)[1]','VARCHAR(100)')  as TIPOCUENTA,    
     T.N.value('(NoCuenta)[1]','VARCHAR(45)')  as NOCUENTA,    
     T.N.value('(Firma)[1]','INT')  as FIRMA,    
     T.N.value('(MontoOperacion)[1]','DECIMAL(10,0)')  as MONTOOPERACION,    
     T.N.value('(Moneda)[1]','CHAR(3)')  as MONEDA,    
     T.N.value('(UsuariosCaptura)[1]','VARCHAR(250)')  as USUARIOSCAPTURA,    
     T.N.value('(USUARIO_AUT)[1]','VARCHAR(250)')  as USUARIOSAUTORIZA,    
     T.N.value('(Indicador_Plantilla)[1]','INT')  as INDICADOR_PLANTILLA,    
     T.N.value('(Country)[1]','VARCHAR(6)')  as COUNTRY,    
     T.N.value('(ABA)[1]','VARCHAR(9)')  as ABA,    
     T.N.value('(BancoBeneficiario)[1]','VARCHAR(100)')  as BANCOBENEFICIARIO,    
     T.N.value('(SwiftBeneficiario)[1]','VARCHAR(11)')  as SWIFTBENEFICIARIO,    
     T.N.value('(CuentaBeneficiario)[1]','VARCHAR(25)')  as CUENTABENEFICIARIO,    
     T.N.value('(SwiftCorresponsal)[1]','VARCHAR(11)')  as SWIFTCORRESPONSAL,    
     T.N.value('(BancoCorresponsal)[1]','VARCHAR(100)')  as BANCOCORRESPONSAL,    
     T.N.value('(FECHA_SOL)[1]','DATETIME')  as FECHA_SOL,    
     T.N.value('(Hora_Sol)[1]','VARCHAR(5)')  as HORA_SOL,  
     T.N.value('(USUARIO_CAPT)[1]','VARCHAR(15)')  as USUARIO_CAPT,  
     T.N.value('(FECHA_AUT)[1]','DATETIME')  as FECHA_AUT,  
     T.N.value('(Hora_Aut)[1]','VARCHAR(5)')  as HORA_AUT,  
     T.N.value('(USUARIO_AUT)[1]','VARCHAR(15)')  as USUARIO_AUT,  
     (CASE WHEN T.N.value('(FECHA_CANC/@xsi:nil)[1]', 'VARCHAR(5)') = 'true' THEN NULL ELSE T.N.value('(FECHA_CANC)[1]', 'DATETIME') END) as FECHA_CANC,  
     T.N.value('(Hora_Canc)[1]','VARCHAR(5)')  as HORA_CANC,  
     T.N.value('(USUARIO_CANC)[1]','VARCHAR(15)')  as USUARIO_CANC,  
     T.N.value('(STATUS)[1]','VARCHAR(25)')  as 'STATUS',  
     T.N.value('(OPERACION)[1]','VARCHAR(25)')  as OPERACION,  
     T.N.value('(COMMENT)[1]','VARCHAR(100)')  as COMMENT,  
     (CASE WHEN T.N.value('(FECHA_CONF/@xsi:nil)[1]','VARCHAR(5)') = 'true' THEN NULL ELSE T.N.value('(FECHA_CONF)[1]','DATETIME') END ) as FECHA_CONF,  
     T.N.value('(Hora_Conf)[1]','VARCHAR(5)')  as HORA_CONF,  
     T.N.value('(USUARIO_CONF)[1]','VARCHAR(15)')  as USUARIO_CONF,  
     T.N.value('(NOTA_CANC)[1]','VARCHAR(250)')  as NOTA_CANC,  
     T.N.value('(emailBeneficiario)[1]','VARCHAR(250)')  as EMAILBENEFICIARIO,  
     T.N.value('(emailOrdenante)[1]','VARCHAR(250)')  as EMAILORDENANTE,  
     T.N.value('(flgMail)[1]','BIT')  as FLGMAIL,  
     T.N.value('(residencia)[1]','VARCHAR(100)')  as RESIDENCIA,  
     T.N.value('(rfc)[1]','VARCHAR(20)')  as RFC,  
     T.N.value('(origen)[1]','VARCHAR(5)')  as ORIGEN,  
     T.N.value('(country_ben)[1]','VARCHAR(5)')  as COUNTRY_BEN,  
     T.N.value('(TipoBeneficiario)[1]','VARCHAR(1)')  as TIPOBENEFICIARIO,  
     T.N.value('(Apellido_P)[1]','VARCHAR(40)')  as APELLIDO_P,  
     T.N.value('(Apellido_M)[1]','VARCHAR(40)')  as APELLIDO_M,  
     T.N.value('(Id_Agreement)[1]','BIGINT') as ID_AGREEMENT,  
     T.N.value('(city_res_ben)[1]','VARCHAR(32)') as city_res_ben,  
     T.N.value('(city_add_ben)[1]','VARCHAR(45)') as city_add_ben,  
     T.N.value('(city_num_ben)[1]','VARCHAR(15)') as city_num_ben,  
     T.N.value('(city_col_ben)[1]','VARCHAR(30)') as city_col_ben,  
     T.N.value('(city_zip_ben)[1]','VARCHAR(10)') as city_zip_ben  
    FROM @datosXml.nodes('ArrayOfSolicitudesBeneficiariosCmsModelo/SolicitudesBeneficiariosCmsModelo') as T(N);    
  
    INSERT INTO Tomser.dbo.SOLICITUDES_BENEFICIARIOS  
    (  
	   ID_SOLICITUD_DOCUMENTO,TIPO,ID_FORMATO,FORMATO,ID_CLIENTE,ORDENATE,ALIAS,NOMBRE,CLAVEBANCO,BANCO,TIPOCUENTA,NOCUENTA,FIRMA,MONTOOPERACION,MONEDA,  
	   USUARIOSCAPTURA,USUARIOSAUTORIZA,INDICADOR_PLANTILLA,COUNTRY,ABA,BANCOBENEFICIARIO,SWIFTBENEFICIARIO,CUENTABENEFICIARIO,SWIFTCORRESPONSAL,  
	   BANCOCORRESPONSAL,FECHA_SOL,HORA_SOL,USUARIO_CAPT,FECHA_AUT,HORA_AUT,USUARIO_AUT,FECHA_CANC,HORA_CANC,USUARIO_CANC,STATUS,OPERACION,COMMENT,  
	   FECHA_CONF,HORA_CONF,USUARIO_CONF,NOTA_CANC,EMAILBENEFICIARIO,EMAILORDENANTE,FLGMAIL,RESIDENCIA,RFC,ORIGEN,COUNTRY_BEN,TIPOBENEFICIARIO,  
	   APELLIDO_P,APELLIDO_M,ID_AGREEMENT,city_res_ben,city_add_ben,city_num_ben,city_col_ben,city_zip_ben  
    )  
    SELECT    
	   ID_SOLICITUD_DOCUMENTO,TIPO,ID_FORMATO,FORMATO,ID_CLIENTE,ORDENATE,ALIAS,NOMBRE,CLAVEBANCO,BANCO,TIPOCUENTA,NOCUENTA,FIRMA,MONTOOPERACION,MONEDA,  
	   USUARIOSCAPTURA,USUARIOSAUTORIZA,INDICADOR_PLANTILLA,COUNTRY,ABA,BANCOBENEFICIARIO,SWIFTBENEFICIARIO,CUENTABENEFICIARIO,SWIFTCORRESPONSAL,  
	   BANCOCORRESPONSAL,FECHA_SOL,HORA_SOL,USUARIO_CAPT,FECHA_AUT,HORA_AUT,USUARIO_AUT,FECHA_CANC,HORA_CANC,USUARIO_CANC,STATUS,OPERACION,COMMENT,  
	   FECHA_CONF,HORA_CONF,USUARIO_CONF,NOTA_CANC,EMAILBENEFICIARIO,EMAILORDENANTE,FLGMAIL,RESIDENCIA,RFC,ORIGEN,COUNTRY_BEN,TIPOBENEFICIARIO,  
	   APELLIDO_P,APELLIDO_M,ID_AGREEMENT,city_res_ben,city_add_ben,city_num_ben,city_col_ben,city_zip_ben  
    FROM @tmpSolicitudes;  
  
    IF @@ROWCOUNT = 0  
		THROW 50001, 'No se insertaron registros', 1;  
      
  
  
    INSERT INTO TB_BILUN_SOLICITUDES_BENEFICIARIOS   
    (    
	   ID_SOLICITUD_DOCUMENTO,TIPO,ID_FORMATO,FORMATO,ID_CLIENTE,ORDENATE,ALIAS,NOMBRE,CLAVEBANCO,BANCO,TIPOCUENTA,NOCUENTA,FIRMA,MONTOOPERACION,MONEDA,  
	   USUARIOSCAPTURA,USUARIOSAUTORIZA,INDICADOR_PLANTILLA,COUNTRY,ABA,BANCOBENEFICIARIO,SWIFTBENEFICIARIO,CUENTABENEFICIARIO,SWIFTCORRESPONSAL,  
	   BANCOCORRESPONSAL,FECHA_SOL,HORA_SOL,USUARIO_CAPT,FECHA_AUT,HORA_AUT,USUARIO_AUT,FECHA_CANC,HORA_CANC,USUARIO_CANC,STATUS,OPERACION,COMMENT,  
	   FECHA_CONF,HORA_CONF,USUARIO_CONF,NOTA_CANC,EMAILBENEFICIARIO,EMAILORDENANTE,FLGMAIL,RESIDENCIA,RFC,ORIGEN,COUNTRY_BEN,TIPOBENEFICIARIO,  
	   APELLIDO_P,APELLIDO_M,ID_AGREEMENT,city_res_ben,city_add_ben,city_num_ben,city_col_ben,city_zip_ben  
    )  
    SELECT    
	   ID_SOLICITUD_DOCUMENTO,TIPO,ID_FORMATO,FORMATO,ID_CLIENTE,ORDENATE,ALIAS,NOMBRE,CLAVEBANCO,BANCO,TIPOCUENTA,NOCUENTA,FIRMA,MONTOOPERACION,MONEDA,  
	   USUARIOSCAPTURA,USUARIOSAUTORIZA,INDICADOR_PLANTILLA,COUNTRY,ABA,BANCOBENEFICIARIO,SWIFTBENEFICIARIO,CUENTABENEFICIARIO,SWIFTCORRESPONSAL,  
	   BANCOCORRESPONSAL,FECHA_SOL,HORA_SOL,USUARIO_CAPT,FECHA_AUT,HORA_AUT,USUARIO_AUT,FECHA_CANC,HORA_CANC,USUARIO_CANC,STATUS,OPERACION,COMMENT,  
	   FECHA_CONF,HORA_CONF,USUARIO_CONF,NOTA_CANC,EMAILBENEFICIARIO,EMAILORDENANTE,FLGMAIL,RESIDENCIA,RFC,ORIGEN,COUNTRY_BEN,TIPOBENEFICIARIO,  
	   APELLIDO_P,APELLIDO_M,ID_AGREEMENT,city_res_ben,city_add_ben,city_num_ben,city_col_ben,city_zip_ben  
    FROM @tmpSolicitudes;  
  
      
    IF @@ROWCOUNT = 0  
		THROW 50002, 'No se insertaron registros en BILUN', 1;  
  
  
    COMMIT TRANSACTION;  
      
 END TRY  
  
 BEGIN CATCH  
  
  IF XACT_STATE() <> 0  
            ROLLBACK TRANSACTION;  
   
        INSERT INTO Auditoria.ErrorLog  
        (  
            ErrorNumber,  
            ErrorSeverity,  
            ErrorState,  
            ErrorProcedure,  
            ErrorLine,  
            ErrorMessage  
        )  
        VALUES  
        (  
            ERROR_NUMBER(),  
            ERROR_SEVERITY(),  
            ERROR_STATE(),  
            ERROR_PROCEDURE(),  
            ERROR_LINE(),  
            ERROR_MESSAGE()  
        );  
   
        THROW;  
 END CATCH  
END
GO


/***********************************************************************      
NOMBRE              :  dbo.usp_Solicitudes_InsertarAlertas_Beneficiarios      
SISTEMA/BD          :  InterfacesT24      
AUTOR               :  Carlos Aguilar      
FECHA               :  2026-06-11      
       
DESCRIPCIÓN         :       
Inserta valores en la tabla tbl_beneficiarios_alertas    
de forma masiva    
       
ALCANCE / NOTAS     :      
- Operación DML.      
- Se espera retornar filas.      
       
PARÁMETROS          :      
-      
       
CONTROL DE CAMBIOS  :       
2026-06-11 | Carlos Aguilar | Creación SP      
***********************************************************************/      
  
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_InsertarAlertas_Beneficiarios    
(    
    @datosXml XML    
)     
AS      
BEGIN      
    
    SET NOCOUNT ON;    
    SET XACT_ABORT ON;    
    
    BEGIN TRY    
          
        BEGIN TRANSACTION;    
    
        -- 1. Declarar variable tipo tabla  
        DECLARE @tmpAlertas TABLE  
        (  
            ID_SOLICITUD_DOCUMENTO BIGINT,  
            Alerta VARCHAR(MAX)  
        );  
    
        -- 2. Insertar desde XML  
        INSERT INTO @tmpAlertas (ID_SOLICITUD_DOCUMENTO, Alerta)  
        SELECT    
            T.N.value('(ID_SOLICITUD_DOCUMENTO)[1]','BIGINT'),  
            T.N.value('(Alerta)[1]','VARCHAR(MAX)')  
        FROM @datosXml.nodes('ArrayOfAlertasBeneficiarioModelo/AlertasBeneficiarioModelo') AS T(N);      
    
        -- 3. Insert final  
        INSERT INTO tbl_beneficiarios_alertas   
        (ID_SOLICITUD_DOCUMENTO, ALERTA, FECHA_ALERTA)    
        SELECT    
            ID_SOLICITUD_DOCUMENTO,   
            Alerta,   
            GETDATE()  
        FROM @tmpAlertas;    
    
        IF @@ROWCOUNT = 0    
            THROW 50001, 'No se insertaron registros', 1;    
    
        COMMIT TRANSACTION;    
            
    END TRY    
    
    BEGIN CATCH    
    
        IF XACT_STATE() <> 0    
            ROLLBACK TRANSACTION;    
     
        INSERT INTO Auditoria.ErrorLog    
        (    
            ErrorNumber,    
            ErrorSeverity,    
            ErrorState,    
            ErrorProcedure,    
            ErrorLine,    
            ErrorMessage    
        )    
        VALUES    
        (    
            ERROR_NUMBER(),    
            ERROR_SEVERITY(),    
            ERROR_STATE(),    
            ERROR_PROCEDURE(),    
            ERROR_LINE(),    
            ERROR_MESSAGE()    
        );    
     
        THROW;    
    END CATCH    
END  
GO

/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_ObtenerEntidadesRestringidas    
SISTEMA/BD          :  InterfacesT24    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Obtiene el catalogo de Entidades Restringidas  
     
ALCANCE / NOTAS     :    
- Operación DML.    
- Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE    PROCEDURE dbo.usp_Solicitudes_ObtenerEntidadesRestringidas  
   
AS    
BEGIN    
  
 SELECT Palabra FROM tb_spid_entidades_restringidas;  
  
END
GO

/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_ObtenerEntidadesPermitidas    
SISTEMA/BD          :  InterfacesT24    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Obtiene el catalogo de Entidades Permitidas  
     
ALCANCE / NOTAS     :    
- Operación DML.    
- Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE    PROCEDURE dbo.usp_Solicitudes_ObtenerEntidadesPermitidas  
   
AS    
BEGIN    
  
 SELECT Palabra FROM tb_spid_entidades_permitidas  
  
END
GO

/***********************************************************************  
NOMBRE              :  dbo.usp_ObtenerSolicitudesBen_TomiTomser  
SISTEMA/BD          :  InterfacesT24  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-11  
   
DESCRIPCIÓN         :   
Revisa en la tabla Solicitudes_Beneficiarios la lista de solicitudes
comparando con los parametros de InterfacesT24 la fecha de autorización
   
ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-11 | Carlos Aguilar | Creación SP  
***********************************************************************/  

CREATE OR ALTER PROCEDURE dbo.usp_ObtenerSolicitudesBen_TomiTomser
AS
BEGIN
    SET NOCOUNT ON;

    -- Variable tabla
    DECLARE @tmpSolicitudesBen TABLE
    (
        id_solicitud BIGINT
    );

    INSERT INTO @tmpSolicitudesBen (id_solicitud)
    SELECT id_solicitud 
    FROM tomser.dbo.Solicitudes_Beneficiarios 
    WHERE status IN ('CONFIRMADO','RECHAZADO') 
      AND FLG_NOTIFICADO = 1  
      AND 1 = CASE 
            WHEN ISNULL(origen,'') = 'CMS' AND OPERACION = 'ALTA'  
                THEN CASE 
                        WHEN DATEDIFF(MINUTE, fecha_aut, GETDATE()) > 
                             (SELECT VALOR 
                              FROM TB_SRV_PARAMETROS_SISTEMA 
                              WHERE ID_PARAMETRO = 'GralAutorizacion') 
                        THEN 1 ELSE 0 
                     END 
            ELSE 1 
        END;

    -- Resultado final
    SELECT *
    FROM tomser.dbo.Solicitudes_Beneficiarios 
    WHERE id_solicitud IN (SELECT id_solicitud FROM @tmpSolicitudesBen);

END
GO


/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_ActualizaSolicitudesBen_TomiTomser  
SISTEMA/BD          :  InterfacesT24  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-10  
   
DESCRIPCIÓN         :   
Actualiza las tablas: TB_BILUN_Solicitudes_Beneficiarios y   
tomser.dbo.Solicitudes_Beneficiarios sb  
   
ALCANCE / NOTAS     :  
- Operación DML.  
- No Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-10 | Carlos Aguilar | Creación SP  
***********************************************************************/  

CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ActualizaSolicitudesBen_TomiTomser  
(  
    @datosXml XML  
)  
AS  
BEGIN  

    SET NOCOUNT ON;  
    SET XACT_ABORT ON;    
    
    BEGIN TRY

        BEGIN TRANSACTION;

        -- 1. Tabla principal
        DECLARE @tmpSolicitudes TABLE
        (
            Id_Solicitud BIGINT,
            Id_Solicitud_Documento BIGINT,
            Formato VARCHAR(3),
            Estatus VARCHAR(25),
            Id_Cliente BIGINT,
            TipoCuenta VARCHAR(100),
            NoCuenta VARCHAR(45),
            Moneda VARCHAR(3),
            ABA VARCHAR(9),
            SwiftBeneficiario VARCHAR(11),
            SwiftCorresponsal VARCHAR(11),
            Cont INT
        );

        INSERT INTO @tmpSolicitudes
        SELECT  
            T.N.value('(Id_Solicitud)[1]','BIGINT'),
            T.N.value('(Id_SolicitudOriginal)[1]','BIGINT'),
            T.N.value('(Formato)[1]','VARCHAR(3)'),
            T.N.value('(Estatus)[1]','VARCHAR(25)'),
            T.N.value('(Id_Cliente)[1]','BIGINT'),
            T.N.value('(TipoCuenta)[1]','VARCHAR(100)'),
            T.N.value('(NoCuenta)[1]','VARCHAR(45)'),
            T.N.value('(Moneda)[1]','VARCHAR(3)'),
            T.N.value('(ABA)[1]','VARCHAR(9)'),
            T.N.value('(SwiftBeneficiario)[1]','VARCHAR(11)'),
            T.N.value('(SwiftCorresponsal)[1]','VARCHAR(11)'),
            T.N.value('(Cont)[1]','INT')
        FROM @datosXml.nodes('ArrayOfValidacionBeneficiariosModel/ValidacionBeneficiariosModel') AS T(N);

        -- 2. Rechazados
        DECLARE @tmpRechazados TABLE
        (
            Id_Solicitud BIGINT,
            Id_Solicitud_Documento BIGINT,
            Formato VARCHAR(3),
            Estatus VARCHAR(25),
            Id_Cliente BIGINT,
            TipoCuenta VARCHAR(100),
            NoCuenta VARCHAR(45),
            Moneda VARCHAR(3),
            ABA VARCHAR(9),
            SwiftBeneficiario VARCHAR(11),
            SwiftCorresponsal VARCHAR(11),
            Cont INT
        );

        INSERT INTO @tmpRechazados
        SELECT * 
        FROM @tmpSolicitudes 
        WHERE Estatus = 'RECHAZADO';

        DECLARE @NoRegistros INT;

        SELECT @NoRegistros = COUNT(*) FROM @tmpRechazados;

        IF(@NoRegistros > 0) 
        BEGIN  

            -- 3. Mensajes
            DECLARE @tmpMensajesSolicitud TABLE
            (
                Id_Solicitud BIGINT,
                DESCRIPCION VARCHAR(MAX)
            );

            INSERT INTO @tmpMensajesSolicitud
            SELECT  
                s.Id_Solicitud,
                CASE 
                    WHEN a.DESCRIPCION IS NULL OR LTRIM(RTRIM(CAST(a.DESCRIPCION AS VARCHAR(MAX)))) = '' 
                    THEN 'Beneficiario rechazado por Mesa de Control' 
                    ELSE a.DESCRIPCION 
                END
            FROM @tmpRechazados s  
            LEFT JOIN TBL_BENEFICIARIOS_ALERTAS a 
                ON a.ID_SOLICITUD_DOCUMENTO = s.Id_Solicitud_Documento;

            -- Updates
            UPDATE sb  
            SET FECHA_CANC = GETDATE(),   
                USUARIO_CANC = 'TOMI',  
                status = 'CANCELADO',  
                NOTA_CANC = m.DESCRIPCION  
            FROM tomser.dbo.Solicitudes_Beneficiarios sb  
            INNER JOIN @tmpMensajesSolicitud m 
                ON m.Id_Solicitud = sb.ID_SOLICITUD;

            UPDATE sb  
            SET FECHA_CANC = GETDATE(),  
                USUARIO_CANC = 'TOMI',  
                status = 'CANCELADO',  
                NOTA_CANC = m.DESCRIPCION  
            FROM TB_BILUN_Solicitudes_Beneficiarios sb  
            INNER JOIN @tmpMensajesSolicitud m 
                ON m.Id_Solicitud = sb.ID_SOLICITUD;  

        END  

        -- 4. Duplicados (Cont > 0)
        DECLARE @tmpSolicitudesConRegistro TABLE
        (
            Id_Solicitud BIGINT
        );

        INSERT INTO @tmpSolicitudesConRegistro
        SELECT Id_Solicitud
        FROM @tmpSolicitudes 
        WHERE Cont > 0;

        SELECT @NoRegistros = COUNT(*) FROM @tmpSolicitudesConRegistro;

        IF(@NoRegistros > 0) 
        BEGIN  

            UPDATE sb  
            SET FECHA_CANC = GETDATE(),  
                USUARIO_CANC = 'TOMI',  
                status = 'CANCELADO',  
                NOTA_CANC = 'Beneficiario Duplicado en CMS'  
            FROM tomser.dbo.Solicitudes_Beneficiarios sb  
            INNER JOIN @tmpSolicitudesConRegistro r 
                ON r.Id_Solicitud = sb.ID_SOLICITUD;  

            UPDATE sb  
            SET FECHA_CANC = GETDATE(),  
                USUARIO_CANC = 'TOMI',  
                status = 'CANCELADO',  
                NOTA_CANC = 'Beneficiario Duplicado en CMS'  
            FROM TB_BILUN_Solicitudes_Beneficiarios sb  
            INNER JOIN @tmpSolicitudesConRegistro r 
                ON r.Id_Solicitud = sb.ID_SOLICITUD;  

        END  

        COMMIT TRANSACTION;

    END TRY

    BEGIN CATCH 

        IF XACT_STATE() <> 0  
            ROLLBACK TRANSACTION;  
   
        INSERT INTO Auditoria.ErrorLog  
        (  
            ErrorNumber,  
            ErrorSeverity,  
            ErrorState,  
            ErrorProcedure,  
            ErrorLine,  
            ErrorMessage  
        )  
        VALUES  
        (  
            ERROR_NUMBER(),  
            ERROR_SEVERITY(),  
            ERROR_STATE(),  
            ERROR_PROCEDURE(),  
            ERROR_LINE(),  
            ERROR_MESSAGE()  
        );  
   
        THROW;

    END CATCH;
  
END
GO


/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_ActualizarbeneficiariosTomserModificaciones
SISTEMA/BD          :  InterfacesT24  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-10  
   
DESCRIPCIÓN         :   
Actualiza la tabla tomser.dbo.Solicitudes_Beneficiarios, los campos:  
status
   
ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-10 | Carlos Aguilar | Creación SP  
***********************************************************************/  
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ActualizarbeneficiariosTomserModificaciones  
(    
    @datosXml XML    
)    
AS    
BEGIN  

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        
        BEGIN TRANSACTION;

        -- 1. Declarar variable tabla
        DECLARE @tmpSolicitudesModif TABLE
        (
            Id_Solictud BIGINT
        );

        -- 2. Insertar desde XML
        INSERT INTO @tmpSolicitudesModif (Id_Solictud)
        SELECT 
            T.N.value('(ID_SOLICITUD)[1]','BIGINT')
        FROM @datosXml.nodes('ArrayOfSolicitudesBeneficiarioTomserModelo/SolicitudesBeneficiarioTomserModelo') AS T(N);

        -- 3. Update
        UPDATE t 
        SET status = 'PROCESADO'
        FROM tomser.dbo.Solicitudes_Beneficiarios t
        INNER JOIN @tmpSolicitudesModif a 
            ON a.Id_Solictud = t.ID_SOLICITUD;

        COMMIT TRANSACTION;
          
    END TRY

    BEGIN CATCH

        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH
END
GO


/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_ActualizarbeneficiariosTomserAltas    
SISTEMA/BD          :  InterfacesT24    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-10    
     
DESCRIPCIÓN         :     
Actualiza la tabla tomser.dbo.Solicitudes_Beneficiarios, los campos:    
status, id_formato   
     
ALCANCE / NOTAS     :    
- Operación DML.    
- Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-10 | Carlos Aguilar | Creación SP    
***********************************************************************/    

CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ActualizarbeneficiariosTomserAltas    
(      
    @datosXml XML      
)      
AS      
BEGIN    
  
    SET NOCOUNT ON;  
    SET XACT_ABORT ON;  
  
    BEGIN TRY  
        
        BEGIN TRANSACTION;  
   
        -- 1. Variable tabla
        DECLARE @tmpSolicitudesAltas TABLE
        (
            Id_Solictud BIGINT
        );

        -- 2. Insertar desde XML
        INSERT INTO @tmpSolicitudesAltas (Id_Solictud)
        SELECT 
            T.N.value('(ID_SOLICITUD)[1]','BIGINT')
        FROM @datosXml.nodes('ArrayOfSolicitudesBeneficiarioTomserModelo/SolicitudesBeneficiarioTomserModelo') AS T(N);

        -- 3. Update
        UPDATE t   
        SET status = 'PROCESADO',
            id_Formato = a.Id_Solictud  
        FROM tomser.dbo.Solicitudes_Beneficiarios t  
        INNER JOIN @tmpSolicitudesAltas a 
            ON a.Id_Solictud = t.ID_SOLICITUD;  

        COMMIT TRANSACTION;  
          
    END TRY  
  
    BEGIN CATCH  
  
        IF XACT_STATE() <> 0  
            ROLLBACK TRANSACTION;  
   
        INSERT INTO Auditoria.ErrorLog  
        (  
            ErrorNumber,  
            ErrorSeverity,  
            ErrorState,  
            ErrorProcedure,  
            ErrorLine,  
            ErrorMessage  
        )  
        VALUES  
        (  
            ERROR_NUMBER(),  
            ERROR_SEVERITY(),  
            ERROR_STATE(),  
            ERROR_PROCEDURE(),  
            ERROR_LINE(),  
            ERROR_MESSAGE()  
        );  
   
        THROW;  
    END CATCH  
END
GO


/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_ObtenerParametrosProcesar  
SISTEMA/BD          :  InterfacesT24  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-10  
   
DESCRIPCIÓN         :   
Obtiene la lista de parametros de Solicitudes a Procesar
   
ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-10 | Carlos Aguilar | Creación SP  
***********************************************************************/  
CREATE OR ALTER   PROCEDURE dbo.usp_Solicitudes_ObtenerParametrosProcesar     
AS    
BEGIN  

	SET NOCOUNT ON;

	SELECT B.Descripcion,B.Id_Solicitud,B.Id_Formato,B.Formato,A.Id_Cliente,B.TipoFormato,B.MontoOperacion,B.Moneda,B.Firma 
	FROM tomser.dbo.SOLICITUDES_PARAMETROS A 
	INNER JOIN tomser.dbo.SOLICITUDES_PARAMETROS_DETALLE B ON A.ID_SOLICITUD = B.Id_Solicitud 
	WHERE (A.ID_SOLICITUD IN(SELECT id_solicitud FROM tomser.dbo.solicitudes_parametros WHERE status='CONFIRMADO')) 
	AND (B.ID_FORMATO BETWEEN 1 AND 23) 
	ORDER BY B.Id_Formato;

END
GO


/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_ActualizarParametrosTOMI    
SISTEMA/BD          :  InterfacesT24    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-10    
     
DESCRIPCIÓN         :     
Borra los parametros de las aolicitudes y actualiza la tabla  
tomser.dbo.parametros_cliente con los nuevos valores  
     
ALCANCE / NOTAS     :    
- Operación DML.    
- No Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-10 | Carlos Aguilar | Creación SP    
***********************************************************************/    

CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ActualizarParametrosTOMI    
(      
    @datosXml XML      
)      
AS      
BEGIN    
  
    SET NOCOUNT ON;  
    SET XACT_ABORT ON;  
  
    BEGIN TRY  
        
        BEGIN TRANSACTION;  
   
        -- 1. Variable tabla
        DECLARE @tmpParametrosSol TABLE
        (
            Descripcion VARCHAR(100),
            Id_Solicitud BIGINT,
            Id_Formato BIGINT,
            Formato VARCHAR(3),
            Id_Cliente BIGINT,
            TipoFormato INT,
            MontoOperacion DECIMAL(18,2), 
            Moneda VARCHAR(3),
            Firma INT
        );

        -- 2. Insertar desde XML
        INSERT INTO @tmpParametrosSol
        (
            Descripcion, Id_Solicitud, Id_Formato, Formato,
            Id_Cliente, TipoFormato, MontoOperacion, Moneda, Firma
        )
        SELECT  
            T.N.value('(Descripcion)[1]','VARCHAR(100)'),
            T.N.value('(Id_Solicitud)[1]','BIGINT'),
            T.N.value('(Id_Formato)[1]','BIGINT'),
            T.N.value('(Formato)[1]','VARCHAR(3)'),
            T.N.value('(Id_Cliente)[1]','BIGINT'),
            T.N.value('(TipoFormato)[1]','INT'),
            T.N.value('(MontoOperacion)[1]','DECIMAL(18,2)'),
            T.N.value('(Moneda)[1]','VARCHAR(3)'),
            T.N.value('(Firma)[1]','INT')
        FROM @datosXml.nodes('ArrayOfSolicitudParametrosModelo/SolicitudParametrosModelo') AS T(N);    

        -- 3. Delete
        DELETE c  
        FROM tomser.dbo.parametros_cliente c  
        INNER JOIN @tmpParametrosSol p 
            ON p.Id_Formato = c.Id_Formato 
           AND p.Id_Cliente = c.Id_Cliente;  

        -- 4. Insert
        INSERT INTO tomser.dbo.parametros_cliente
        (
            Descripcion, Id_Solicitud, Id_Formato, Formato,
            Id_Cliente, TipoFormato, MontoOperacion, Moneda, Firma
        )  
        SELECT  
            Descripcion, Id_Solicitud, Id_Formato, Formato,
            Id_Cliente, TipoFormato, MontoOperacion, Moneda, Firma  
        FROM @tmpParametrosSol;  

        COMMIT TRANSACTION;  
          
    END TRY  
  
    BEGIN CATCH  
  
        IF XACT_STATE() <> 0  
            ROLLBACK TRANSACTION;  
   
        INSERT INTO Auditoria.ErrorLog  
        (  
            ErrorNumber, ErrorSeverity, ErrorState,
            ErrorProcedure, ErrorLine, ErrorMessage  
        )  
        VALUES  
        (  
            ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(),
            ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()  
        );  
   
        THROW;  
    END CATCH  
END
GO

/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_ConsultarBancosSolicitudes  
SISTEMA/BD          :  InterfacesT24  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-11  
   
DESCRIPCIÓN         :   
Consulta las solicitudes de Bancos en la tabla:
dbo.solicitudes_bancos

ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-11 | Carlos Aguilar | Creación SP  
***********************************************************************/  
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ConsultarBancosSolicitudes
AS
BEGIN
	SELECT * 
	FROM tomser.dbo.solicitudes_bancos 
	WHERE id_solicitud IN (SELECT id_solicitud FROM tomser.dbo.solicitudes_bancos WHERE status IN ('AUTORIZADO'));
END
GO

/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_InsertarAbausa_TOMI
SISTEMA/BD          :  InterfacesT24  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-11  
   
DESCRIPCIÓN         :   
Inserta de forma masiva en la tabla: abausa

ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-11 | Carlos Aguilar | Creación SP  
***********************************************************************/  
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_InsertarAbausa_TOMI
(
	@datosXml	XML
)
AS
BEGIN

	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @regsInicio int = 0;
	DECLARE @regsFinal int = 0;

	BEGIN TRY

		BEGIN TRANSACTION;

		SELECT @regsInicio = count(*) FROM tomser.dbo.abausa;

		
		DECLARE @tmpBancosSol TABLE
				(
					ABA VARCHAR(9),
					Cus_Name VARCHAR(36),
					State VARCHAR(2),
					City VARCHAR(25),
					Currency VARCHAR(3)
				);

	    INSERT INTO @tmpBancosSol (ABA, Cus_Name, State, City, Currency)
		SELECT T.N.value('(ABA)[1]','VARCHAR(9)')  as ABA,
			   T.N.value('(Cus_Name)[1]','VARCHAR(36)')  as Cus_Name,
			   T.N.value('(State)[1]','VARCHAR(2)')  as State,
			   T.N.value('(City)[1]','VARCHAR(25)')  as City,
			   T.N.value('(Currency)[1]','VARCHAR(3)')  as Currency
		FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);  

		INSERT INTO tomser.dbo.abausa(ABA,CUS_NAME,STATE,CITY,CURRENCY)
		SELECT ABA, Cus_Name, State, City, Currency FROM @tmpBancosSol;

		INSERT INTO tb_bilun_abausa (ABA,CUS_NAME,STATE,CITY,CURRENCY)
		SELECT ABA, Cus_Name, State, City, Currency FROM @tmpBancosSol;

		SELECT @regsFinal = count(*) FROM tomser.dbo.abausa;

		SELECT (@regsFinal - @regsInicio) AS Insertados
		

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
		IF XACT_STATE() <> 0
			ROLLBACK TRANSACTION;
 
		INSERT INTO Auditoria.ErrorLog
		(
			ErrorNumber,
			ErrorSeverity,
			ErrorState,
			ErrorProcedure,
			ErrorLine,
			ErrorMessage
		)
		VALUES
		(
			ERROR_NUMBER(),
			ERROR_SEVERITY(),
			ERROR_STATE(),
			ERROR_PROCEDURE(),
			ERROR_LINE(),
			ERROR_MESSAGE()
		);
 
		THROW;
	END CATCH;

END
GO

/***********************************************************************  
NOMBRE              :  dbo.usp_Solicitudes_InsertarSwiftMex_TOMI  
SISTEMA/BD          :  InterfacesT24  
AUTOR               :  Carlos Aguilar  
FECHA               :  2026-06-11  
   
DESCRIPCIÓN         :   
Inserta de forma masiva en la tabla: tomser.dbo.swift_mex
tb_bilun_swift_mex,tomser.dbo.swift_mex_int, tb_bilun_swift_mex_int

ALCANCE / NOTAS     :  
- Operación DML.  
- Se espera retornar filas.  
   
PARÁMETROS          :  
-  
   
CONTROL DE CAMBIOS  :   
2026-06-11 | Carlos Aguilar | Creación SP  
***********************************************************************/  
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_InsertarSwiftMex_TOMI 
(
	@datosXml	XML
)
AS
BEGIN

	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @regsInicio int = 0;
	DECLARE @regsFinal int = 0;

	BEGIN TRY

		BEGIN TRANSACTION;

		SELECT @regsInicio = count(*) FROM tomser.dbo.swift_mex;

		
		DECLARE @tmpBancosSol TABLE
				(
					Ban_Id BIGINT,
					Swift_Ben VARCHAR(11),
					Cus_Name_Ben VARCHAR(100),
					Cus_Name_Acc VARCHAR(25),
					Swift_Int VARCHAR(11),
					Cus_Name_Int VARCHAR(100)
				);

	    INSERT INTO @tmpBancosSol (Ban_Id, Swift_Ben, Cus_Name_Ben, Cus_Name_Acc,Swift_Int,Cus_Name_Int)
		SELECT T.N.value('(Ban_Id)[1]','BIGINT')  as Ban_Id,
			   T.N.value('(Swift_Ben)[1]','VARCHAR(11)')  as Swift_Ben,
			   T.N.value('(Cus_Name_Ben)[1]','VARCHAR(100)')  as Cus_Name_Ben,
			   T.N.value('(Cus_Name_Acc)[1]','VARCHAR(25)')  as Cus_Name_Acc,
			   T.N.value('(Swift_Int)[1]','VARCHAR(11)')  as Swift_Int,
			   T.N.value('(Cus_Name_Int)[1]','VARCHAR(100)')  as Cus_Name_Int
		FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);  

		INSERT INTO tomser.dbo.swift_mex(BAN_ID,SWIFT_BEN,CUS_NAME_BEN,CUS_NAME_ACC)
		SELECT Ban_Id, Swift_Ben, Cus_Name_Ben, Cus_Name_Acc FROM @tmpBancosSol;

		INSERT INTO tb_bilun_swift_mex(BAN_ID,SWIFT_BEN,CUS_NAME_BEN,CUS_NAME_ACC)
		SELECT Ban_Id, Swift_Ben, Cus_Name_Ben, Cus_Name_Acc FROM @tmpBancosSol;

		INSERT INTO tomser.dbo.swift_mex_int (BAN_ID_MEX,BAN_ID_INT,SWIFT_INT,CUS_NAME_INT,CUS_NAME_ACC)
		SELECT Ban_Id,1, Swift_Int, Cus_Name_Int, '' as Cus_Name_Acc FROM @tmpBancosSol;

		INSERT INTO tb_bilun_swift_mex_int (BAN_ID_MEX,BAN_ID_INT,SWIFT_INT,CUS_NAME_INT,CUS_NAME_ACC)
		SELECT Ban_Id,1, Swift_Int, Cus_Name_Int, '' as Cus_Name_Acc FROM @tmpBancosSol;

		SELECT @regsFinal = count(*) FROM tomser.dbo.swift_mex;

		SELECT (@regsFinal - @regsInicio) AS Insertados
		

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
		IF XACT_STATE() <> 0
			ROLLBACK TRANSACTION;
 
		INSERT INTO Auditoria.ErrorLog
		(
			ErrorNumber,
			ErrorSeverity,
			ErrorState,
			ErrorProcedure,
			ErrorLine,
			ErrorMessage
		)
		VALUES
		(
			ERROR_NUMBER(),
			ERROR_SEVERITY(),
			ERROR_STATE(),
			ERROR_PROCEDURE(),
			ERROR_LINE(),
			ERROR_MESSAGE()
		);
 
		THROW;
	END CATCH;

END
GO

/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_InsertarSwifOthBenUsd_TOMI    
SISTEMA/BD          :  InterfacesT24    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Inserta de forma masiva en la tabla: tomser.dbo.swift_othben_usd,  
tb_bilun_swift_othben_usd, tomser.dbo.swift_othint_usd,
tb_bilun_swift_othint_usd 
  
ALCANCE / NOTAS     :    
- Operación DML.    
- Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE OR ALTER  PROCEDURE dbo.usp_Solicitudes_InsertarSwifOthBenUsd_TOMI   
(  
 @datosXml XML  
)  
AS  
BEGIN  
  
 SET NOCOUNT ON;  
 SET XACT_ABORT ON;  
  
 DECLARE @regsInicio int = 0;  
 DECLARE @regsFinal int = 0;  
  
 BEGIN TRY  
  
  BEGIN TRANSACTION;  
  
	  SELECT @regsInicio = count(*) FROM tomser.dbo.swift_othben_usd;  
  
    
	  DECLARE @tmpBancosSol TABLE  
		(  
		 Ban_Id BIGINT,  
		 Swift_Ben VARCHAR(11),  
		 Cus_Name_Ben VARCHAR(100),  
		 Ban_id_int BIGINT,  
		 Swift_Int VARCHAR(11),  
		 Cus_Name_Int VARCHAR(100)  
		);  
  
      INSERT INTO @tmpBancosSol (Ban_Id, Swift_Ben, Cus_Name_Ben, Ban_id_int,Swift_Int,Cus_Name_Int)  
	  SELECT T.N.value('(Ban_Id)[1]','BIGINT')  as Ban_Id,  
		  T.N.value('(Swift_Ben)[1]','VARCHAR(11)')  as Swift_Ben,  
		  T.N.value('(Cus_Name_Ben)[1]','VARCHAR(100)')  as Cus_Name_Ben,  
		  T.N.value('(ban_id_int)[1]','BIGINT')  as Ban_id_int,  
		  T.N.value('(Swift_Int)[1]','VARCHAR(11)')  as Swift_Int,  
		  T.N.value('(Cus_Name_Int)[1]','VARCHAR(100)')  as Cus_Name_Int  
	  FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);    
  
	  INSERT INTO tomser.dbo.swift_othben_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) 
	  SELECT Ban_Id, Swift_Ben, Cus_Name_Ben FROM @tmpBancosSol;
	  
	  INSERT INTO tb_bilun_swift_othben_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) 
	  SELECT Ban_Id, Swift_Ben, Cus_Name_Ben FROM @tmpBancosSol;  


  
	  INSERT INTO tomser.dbo.swift_othint_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN)  
	  SELECT Ban_id_int, Swift_Int, Cus_Name_Int FROM @tmpBancosSol;
	  
	  INSERT INTO tb_bilun_swift_othint_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) 
	  SELECT Ban_id_int, Swift_Int, Cus_Name_Int FROM @tmpBancosSol;  
  
	  SELECT @regsFinal = count(*) FROM tomser.dbo.swift_othben_usd;  
  
	  SELECT (@regsFinal - @regsInicio) AS Insertados  
    
  
  COMMIT TRANSACTION;  
  
 END TRY  
  
 BEGIN CATCH  
  IF XACT_STATE() <> 0  
   ROLLBACK TRANSACTION;  
   
  INSERT INTO Auditoria.ErrorLog  
  (  
   ErrorNumber,  
   ErrorSeverity,  
   ErrorState,  
   ErrorProcedure,  
   ErrorLine,  
   ErrorMessage  
  )  
  VALUES  
  (  
   ERROR_NUMBER(),  
   ERROR_SEVERITY(),  
   ERROR_STATE(),  
   ERROR_PROCEDURE(),  
   ERROR_LINE(),  
   ERROR_MESSAGE()  
  );  
   
  THROW;  
 END CATCH;  
  
END
GO

/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_InsertarOthersSwift_TOMI    
SISTEMA/BD          :  Bot    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Inserta de forma masiva en la tabla: tomser.dbo.others_swift,
tb_bilun_others_swift
  
ALCANCE / NOTAS     :    
- Operación DML.    
- Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE OR ALTER  PROCEDURE dbo.usp_Solicitudes_InsertarOthersSwift_TOMI   
(  
 @datosXml XML  
)  
AS  
BEGIN  
  
 SET NOCOUNT ON;  
 SET XACT_ABORT ON;  
  
 DECLARE @regsInicio int = 0;  
 DECLARE @regsFinal int = 0;  
  
 BEGIN TRY  
  
  BEGIN TRANSACTION;  
  
	  SELECT @regsInicio = count(*) FROM tomser.dbo.others_swift;  
  
    
	  DECLARE @tmpBancosSol TABLE  
		(  
		 ABA VARCHAR(15),   
		 Cus_Name VARCHAR(100),  
		 Currency VARCHAR(3)  
		);  
  
      INSERT INTO @tmpBancosSol (ABA, Cus_Name, Currency)  
	  SELECT T.N.value('(swift_oth)[1]','VARCHAR(15)')  as ABA,  
		  T.N.value('(Cus_Name_Oth)[1]','VARCHAR(100)')  as Cus_Name,  
		  T.N.value('(Currency_Oth)[1]','VARCHAR(3)')  as Currency
	  FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);    
  
	  INSERT INTO tomser.dbo.others_swift (ABA,CUS_NAME,CURRENCY)  
	  SELECT ABA, Cus_Name, Currency FROM @tmpBancosSol;  

	  INSERT INTO tb_bilun_others_swift (ABA,CUS_NAME,CURRENCY) 
	  SELECT ABA, Cus_Name, Currency FROM @tmpBancosSol; 
  
  
	  SELECT @regsFinal = count(*) FROM tomser.dbo.others_swift;  
  
	  SELECT (@regsFinal - @regsInicio) AS Insertados  
    
  
  COMMIT TRANSACTION;  
  
 END TRY  
  
 BEGIN CATCH  
  IF XACT_STATE() <> 0  
   ROLLBACK TRANSACTION;  
   
  INSERT INTO Auditoria.ErrorLog  
  (  
   ErrorNumber,  
   ErrorSeverity,  
   ErrorState,  
   ErrorProcedure,  
   ErrorLine,  
   ErrorMessage  
  )  
  VALUES  
  (  
   ERROR_NUMBER(),  
   ERROR_SEVERITY(),  
   ERROR_STATE(),  
   ERROR_PROCEDURE(),  
   ERROR_LINE(),  
   ERROR_MESSAGE()  
  );  
   
  THROW;  
 END CATCH;  
  
END  
GO
/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_ActualizarBancosSol   
SISTEMA/BD          :  Bot    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-11    
     
DESCRIPCIÓN         :     
Inserta de forma masiva en la tabla: tomser.dbo.solicitudes_bancos
  
ALCANCE / NOTAS     :    
- Operación DML.    
- No se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-11 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE OR ALTER  PROCEDURE dbo.usp_Solicitudes_ActualizarBancosSol   
(  
 @datosXml XML  
)  
AS  
BEGIN  
  
 SET NOCOUNT ON;  
 SET XACT_ABORT ON;  
  
 BEGIN TRY  
  
  BEGIN TRANSACTION;  
      
	  DECLARE @tmpBancosSol TABLE  
		(  
		 Id_Solicitud BIGINT 
		);  
  
      INSERT INTO @tmpBancosSol (Id_Solicitud)  
	  SELECT T.N.value('(Id_Solicitud)[1]','BIGINT')  as Id_Solicitud
	  FROM @datosXml.nodes('ArrayOfSolicitudBancoModelo/SolicitudBancoModelo') as T(N);    
  
	  UPDATE sb
			 SET status='PROCESADO'
	  FROM tomser.dbo.solicitudes_bancos sb
	  INNER JOIN @tmpBancosSol s on s.Id_Solicitud = sb.ID_SOLICITUD;
    
  
  COMMIT TRANSACTION;  
  
 END TRY  
  
 BEGIN CATCH  
  IF XACT_STATE() <> 0  
   ROLLBACK TRANSACTION;  
   
  INSERT INTO Auditoria.ErrorLog  
  (  
   ErrorNumber,  
   ErrorSeverity,  
   ErrorState,  
   ErrorProcedure,  
   ErrorLine,  
   ErrorMessage  
  )  
  VALUES  
  (  
   ERROR_NUMBER(),  
   ERROR_SEVERITY(),  
   ERROR_STATE(),  
   ERROR_PROCEDURE(),  
   ERROR_LINE(),  
   ERROR_MESSAGE()  
  );  
   
  THROW;  
 END CATCH;  
  
END  
GO

/***********************************************************************    
NOMBRE              :  dbo.usp_Solicitudes_ProcesarPlantillaTOMI    
SISTEMA/BD          :  IntefcaesT24    
AUTOR               :  Carlos Aguilar    
FECHA               :  2026-06-10    
     
DESCRIPCIÓN         :     
Actualiza la tabla tbl_solicitudes_beneficiarios, los campos:    
status, FECHA_CANC, USUARIO_CANC, NOTA_CANC    
     
ALCANCE / NOTAS     :    
- Operación DML.    
- Se espera retornar filas.    
     
PARÁMETROS          :    
-    
     
CONTROL DE CAMBIOS  :     
2026-06-10 | Carlos Aguilar | Creación SP    
***********************************************************************/    
CREATE OR ALTER PROCEDURE dbo.usp_Solicitudes_ProcesarPlantillaTOMI    
(      
 @datosXml XML      
)      
AS      
BEGIN      

	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	BEGIN TRY
	      
		BEGIN TRANSACTION;
		
		DECLARE @tmpSolicitudes TABLE
		(
			ID_SOLICITUD_DOCUMENTO BIGINT,
			Id_Solicitud BIGINT,
			Id_Formato BIGINT,
			Formato VARCHAR(3),
			Operacion VARCHAR(25),
			Id_Cliente BIGINT,
			Alias VARCHAR(100),
			Nombre VARCHAR(100),
			ClaveBanco VARCHAR(5),
			Banco VARCHAR(100),
			TipoCuenta VARCHAR(100),
			NoCuenta VARCHAR(45),
			Firma INT,
			MontoOperacion DECIMAL(18,2),
			Moneda CHAR(3),
			UsuariosCaptura VARCHAR(250),
			UsuariosAutoriza VARCHAR(250),
			Indicador_Plantilla INT,
			Country VARCHAR(6),
			ABA VARCHAR(9),
			BancoBeneficiario VARCHAR(100),
			SwiftBeneficiario VARCHAR(11),
			CuentaBeneficiario VARCHAR(25),
			SwiftCorresponsal VARCHAR(11),
			BancoCorresponsal VARCHAR(100),
			residencia VARCHAR(100),
			rfc VARCHAR(20),
			origen VARCHAR(5),
			country_ben VARCHAR(5),
			COMMENT VARCHAR(100),
			emailBeneficiario VARCHAR(250),
			emailOrdenante VARCHAR(250),
			TipoBeneficiario VARCHAR(1),
			Apellido_P VARCHAR(40),
			Apellido_M VARCHAR(40),
			Id_Agreement BIGINT,
			city_res_ben VARCHAR(32),
			city_add_ben VARCHAR(45),
			city_num_ben VARCHAR(15),
			city_col_ben VARCHAR(30),
			city_zip_ben VARCHAR(10),
			InsertarAvisoMail INT
		);

		INSERT INTO @tmpSolicitudes
		SELECT  
			T.N.value('(ID_SOLICITUD_DOCUMENTO)[1]','BIGINT'),
			T.N.value('(ID_SOLICITUD)[1]','BIGINT'),
			T.N.value('(id_Formato)[1]','BIGINT'),
			T.N.value('(Formato)[1]','VARCHAR(3)'),
			T.N.value('(OPERACION)[1]','VARCHAR(25)'),
			T.N.value('(Id_Cliente)[1]','BIGINT'),
			T.N.value('(Alias)[1]','VARCHAR(100)'),
			T.N.value('(Nombre)[1]','VARCHAR(100)'),
			T.N.value('(ClaveBanco)[1]','VARCHAR(5)'),
			T.N.value('(Banco)[1]','VARCHAR(100)'),
			T.N.value('(TipoCuenta)[1]','VARCHAR(100)'),
			T.N.value('(NoCuenta)[1]','VARCHAR(45)'),
			T.N.value('(Firma)[1]','INT'),
			T.N.value('(MontoOperacion)[1]','DECIMAL(10,0)'),
			T.N.value('(Moneda)[1]','CHAR(3)'),
			T.N.value('(UsuariosCaptura)[1]','VARCHAR(250)'),
			T.N.value('(USUARIO_AUT)[1]','VARCHAR(250)'),
			T.N.value('(Indicador_Plantilla)[1]','INT'),
			T.N.value('(Country)[1]','VARCHAR(6)'),
			T.N.value('(ABA)[1]','VARCHAR(9)'),
			T.N.value('(BancoBeneficiario)[1]','VARCHAR(100)'),
			T.N.value('(SwiftBeneficiario)[1]','VARCHAR(11)'),
			T.N.value('(CuentaBeneficiario)[1]','VARCHAR(25)'),
			T.N.value('(SwiftCorresponsal)[1]','VARCHAR(11)'),
			T.N.value('(BancoCorresponsal)[1]','VARCHAR(100)'),
			T.N.value('(residencia)[1]','VARCHAR(100)'),
			T.N.value('(rfc)[1]','VARCHAR(20)'),
			T.N.value('(origen)[1]','VARCHAR(5)'),
			T.N.value('(country_ben)[1]','VARCHAR(5)'),
			T.N.value('(COMMENT)[1]','VARCHAR(100)'),
			T.N.value('(emailBeneficiario)[1]','VARCHAR(250)'),
			T.N.value('(emailOrdenante)[1]','VARCHAR(250)'),
			T.N.value('(TipoBeneficiario)[1]','VARCHAR(1)'),
			T.N.value('(Apellido_P)[1]','VARCHAR(40)'),
			T.N.value('(Apellido_M)[1]','VARCHAR(40)'),
			T.N.value('(Id_Agreement)[1]','BIGINT'),
			T.N.value('(city_res_ben)[1]','VARCHAR(32)'),
			T.N.value('(city_add_ben)[1]','VARCHAR(45)'),
			T.N.value('(city_num_ben)[1]','VARCHAR(15)'),
			T.N.value('(city_col_ben)[1]','VARCHAR(30)'),
			T.N.value('(city_zip_ben)[1]','VARCHAR(10)'),
			T.N.value('(InsertarAvisoMail)[1]','INT')
		FROM @datosXml.nodes('ArrayOfSolicitudesBeneficiarioTomserModelo/SolicitudesBeneficiarioTomserModelo') as T(N);
     
        
		   /*    
			*  ALtas Para FXD, FXT, SPD, SER, TAX    
		   */    
		   
			DECLARE @tmpAltaOp1 TABLE
			(
				Id_Solicitud BIGINT,
				Cont INT
			);

			INSERT INTO @tmpAltaOp1
			SELECT s.Id_Solicitud, COUNT(b.Id_Formato)
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXD', 'FXT', 'SPD', 'SER', 'TAX')
			AND UPPER(s.Operacion) = 'ALTA'
			GROUP BY s.Id_Solicitud;
    
  
		   -- Insertar en tomser.dbo.beneficiarios los registros donde Cont = 0  
			INSERT INTO tomser.dbo.beneficiarios
			(
				id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			)
			SELECT 
				s.id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			FROM @tmpAltaOp1 a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE a.Cont = 0;
		   
			DECLARE @tmpAltaBilunOp1 TABLE
			(
				Id_Solicitud BIGINT,
				Cont INT
			);

			INSERT INTO @tmpAltaBilunOp1
			SELECT s.Id_Solicitud, COUNT(b.Id_Formato)
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXD', 'FXT', 'SPD', 'SER', 'TAX')
			AND UPPER(s.Operacion) = 'ALTA'
			GROUP BY s.Id_Solicitud;
		   
			-- Insert tb_bilun_beneficiarios
			INSERT INTO tb_bilun_beneficiarios
			(
				id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			)
			SELECT 
				s.id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			FROM @tmpAltaBilunOp1 a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE a.Cont = 0;
 
  
		   --Insertar para aviso email del beneficiario  
		   
			INSERT INTO tomser.dbo.nf_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpAltaOp1 a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

  
		   /*    
			*  Modificación Para FXD, FXT, SPD, SER, TAX    
		   */   
		   
			DECLARE @tmpModificacionOp1 TABLE
			(
				ID_SOLICITUD_DOCUMENTO BIGINT,
				Id_Solicitud BIGINT,
				Id_Formato BIGINT,
				Formato VARCHAR(3),
				Operacion VARCHAR(25),
				Id_Cliente BIGINT,
				Alias VARCHAR(100),
				Nombre VARCHAR(100),
				ClaveBanco VARCHAR(5),
				Banco VARCHAR(100),
				TipoCuenta VARCHAR(100),
				NoCuenta VARCHAR(45),
				Firma INT,
				MontoOperacion DECIMAL(18,2),
				Moneda CHAR(3),
				UsuariosCaptura VARCHAR(250),
				UsuariosAutoriza VARCHAR(250),
				Indicador_Plantilla INT,
				Country VARCHAR(6),
				ABA VARCHAR(9),
				BancoBeneficiario VARCHAR(100),
				SwiftBeneficiario VARCHAR(11),
				CuentaBeneficiario VARCHAR(25),
				SwiftCorresponsal VARCHAR(11),
				BancoCorresponsal VARCHAR(100),
				residencia VARCHAR(100),
				rfc VARCHAR(20),
				origen VARCHAR(5),
				country_ben VARCHAR(5),
				COMMENT VARCHAR(100),
				emailBeneficiario VARCHAR(250),
				emailOrdenante VARCHAR(250),
				TipoBeneficiario VARCHAR(1),
				Apellido_P VARCHAR(40),
				Apellido_M VARCHAR(40),
				Id_Agreement BIGINT,
				city_res_ben VARCHAR(32),
				city_add_ben VARCHAR(45),
				city_num_ben VARCHAR(15),
				city_col_ben VARCHAR(30),
				city_zip_ben VARCHAR(10),
				InsertarAvisoMail INT
			);

			INSERT INTO @tmpModificacionOp1
			SELECT s.*
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXD', 'FXT', 'SPD', 'SER', 'TAX')
			AND UPPER(s.Operacion) = 'MODIFICACION';
		   
			UPDATE b
			SET Firma = s.Firma,
				MontoOperacion = s.MontoOperacion,
				UsuariosCaptura = s.UsuariosCaptura,
				UsuariosAutoriza = s.UsuariosAutoriza,
				TipoBeneficiario = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.TipoBeneficiario ELSE b.TipoBeneficiario END,
				Nombre = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Nombre ELSE b.Nombre END,
				Apellido_P = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_P ELSE b.Apellido_P END,
				Apellido_M = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_M ELSE b.Apellido_M END
			FROM tomser.dbo.beneficiarios b
			INNER JOIN @tmpModificacionOp1 s on s.Id_Formato = b.Id_Formato;
 
     
		   
			UPDATE b
			SET Firma = s.Firma,
				MontoOperacion = s.MontoOperacion,
				UsuariosCaptura = s.UsuariosCaptura,
				UsuariosAutoriza = s.UsuariosAutoriza,
				TipoBeneficiario = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.TipoBeneficiario ELSE b.TipoBeneficiario END,
				Nombre = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Nombre ELSE b.Nombre END,
				Apellido_P = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_P ELSE b.Apellido_P END,
				Apellido_M = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_M ELSE b.Apellido_M END
			FROM tb_bilun_beneficiarios b
			INNER JOIN @tmpModificacionOp1 s on s.Id_Formato = b.Id_Formato;
  
  
		   --Insertar para aviso email del beneficiario  
  
		   
			DELETE FROM tomser.dbo.nf_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpModificacionOp1
				WHERE InsertarAvisoMail = 1
			);

			DELETE FROM tb_bilun_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpModificacionOp1
				WHERE InsertarAvisoMail = 1
			);

			INSERT INTO tomser.dbo.nf_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpModificacionOp1 a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

			INSERT INTO tb_bilun_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpModificacionOp1 a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;
 
  
		   /*    
			*  Bajas Para FXD, FXT, SPD, SER, TAX    
		   */   
		   
			DECLARE @tmpBajaOp1 TABLE
			(
				ID_SOLICITUD_DOCUMENTO BIGINT,
				Id_Solicitud BIGINT,
				Id_Formato BIGINT,
				Formato VARCHAR(3),
				Operacion VARCHAR(25),
				Id_Cliente BIGINT,
				Alias VARCHAR(100),
				Nombre VARCHAR(100),
				ClaveBanco VARCHAR(5),
				Banco VARCHAR(100),
				TipoCuenta VARCHAR(100),
				NoCuenta VARCHAR(45),
				Firma INT,
				MontoOperacion DECIMAL(18,2),
				Moneda CHAR(3),
				UsuariosCaptura VARCHAR(250),
				UsuariosAutoriza VARCHAR(250),
				Indicador_Plantilla INT,
				Country VARCHAR(6),
				ABA VARCHAR(9),
				BancoBeneficiario VARCHAR(100),
				SwiftBeneficiario VARCHAR(11),
				CuentaBeneficiario VARCHAR(25),
				SwiftCorresponsal VARCHAR(11),
				BancoCorresponsal VARCHAR(100),
				residencia VARCHAR(100),
				rfc VARCHAR(20),
				origen VARCHAR(5),
				country_ben VARCHAR(5),
				COMMENT VARCHAR(100),
				emailBeneficiario VARCHAR(250),
				emailOrdenante VARCHAR(250),
				TipoBeneficiario VARCHAR(1),
				Apellido_P VARCHAR(40),
				Apellido_M VARCHAR(40),
				Id_Agreement BIGINT,
				city_res_ben VARCHAR(32),
				city_add_ben VARCHAR(45),
				city_num_ben VARCHAR(15),
				city_col_ben VARCHAR(30),
				city_zip_ben VARCHAR(10),
				InsertarAvisoMail INT
			);

			INSERT INTO @tmpBajaOp1
			SELECT s.*
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXD', 'FXT', 'SPD', 'SER', 'TAX')
			AND UPPER(s.Operacion) = 'BAJA';

			UPDATE tomser.dbo.beneficiarios
			SET Indicador_Plantilla = 3
			WHERE id_formato IN
			(
				SELECT Id_Formato
				FROM @tmpBajaOp1
			);

			UPDATE tb_bilun_beneficiarios
			SET Indicador_Plantilla = 3
			WHERE id_formato IN
			(
				SELECT Id_Formato
				FROM @tmpBajaOp1
			);

  
		   --Se eliminan Avisos.  
		   
			DELETE FROM tomser.dbo.nf_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpBajaOp1
			);

			DELETE FROM tb_bilun_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpBajaOp1
			);
  
  
		   /*    
			*  Altas Para FXI  
		   */  
			DECLARE @tmpAltaFXI TABLE
			(
				Id_Solicitud BIGINT,
				Cont INT
			);

			INSERT INTO @tmpAltaFXI
			SELECT s.Id_Solicitud, COUNT(b.Id_Formato)
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXI')
			AND UPPER(s.Operacion) = 'ALTA'
			GROUP BY s.Id_Solicitud;
 
     
		   -- Insertar en tomser.dbo.beneficiarios los registros donde Cont = 0  
		   
			INSERT INTO tomser.dbo.beneficiarios
			(
				id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			)
			SELECT 
				s.Id_Solicitud,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			FROM @tmpAltaFXI a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE a.Cont = 0;
 
  
		   
			DECLARE @tmpAltaBilunFXI TABLE
			(
				Id_Solicitud BIGINT,
				Cont INT
			);

			INSERT INTO @tmpAltaBilunFXI
			SELECT s.Id_Solicitud, COUNT(b.Id_Formato)
			FROM @tmpSolicitudes s
			LEFT JOIN tb_bilun_beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXI')
			AND UPPER(s.Operacion) = 'ALTA'
			GROUP BY s.Id_Solicitud;
		   
			INSERT INTO tb_bilun_beneficiarios
			(
				id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			)
			SELECT 
				s.Id_Solicitud,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			FROM @tmpAltaBilunFXI a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE a.Cont = 0;
 
  
		   --Insertar para aviso email del beneficiario  
		   
			INSERT INTO tomser.dbo.nf_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpAltaFXI a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

			INSERT INTO tb_bilun_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpAltaBilunFXI a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

		   /*    
			*  Modificaciones Para FXI  
		   */  
		   
			DECLARE @tmpModificacionFXI TABLE
			(
				ID_SOLICITUD_DOCUMENTO BIGINT,
				Id_Solicitud BIGINT,
				Id_Formato BIGINT,
				Formato VARCHAR(3),
				Operacion VARCHAR(25),
				Id_Cliente BIGINT,
				Alias VARCHAR(100),
				Nombre VARCHAR(100),
				ClaveBanco VARCHAR(5),
				Banco VARCHAR(100),
				TipoCuenta VARCHAR(100),
				NoCuenta VARCHAR(45),
				Firma INT,
				MontoOperacion DECIMAL(18,2),
				Moneda CHAR(3),
				UsuariosCaptura VARCHAR(250),
				UsuariosAutoriza VARCHAR(250),
				Indicador_Plantilla INT,
				Country VARCHAR(6),
				ABA VARCHAR(9),
				BancoBeneficiario VARCHAR(100),
				SwiftBeneficiario VARCHAR(11),
				CuentaBeneficiario VARCHAR(25),
				SwiftCorresponsal VARCHAR(11),
				BancoCorresponsal VARCHAR(100),
				residencia VARCHAR(100),
				rfc VARCHAR(20),
				origen VARCHAR(5),
				country_ben VARCHAR(5),
				COMMENT VARCHAR(100),
				emailBeneficiario VARCHAR(250),
				emailOrdenante VARCHAR(250),
				TipoBeneficiario VARCHAR(1),
				Apellido_P VARCHAR(40),
				Apellido_M VARCHAR(40),
				Id_Agreement BIGINT,
				city_res_ben VARCHAR(32),
				city_add_ben VARCHAR(45),
				city_num_ben VARCHAR(15),
				city_col_ben VARCHAR(30),
				city_zip_ben VARCHAR(10),
				InsertarAvisoMail INT
			);

			INSERT INTO @tmpModificacionFXI
			SELECT s.*
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXI')
			AND UPPER(s.Operacion) = 'MODIFICACION';

			UPDATE b
			SET Firma = s.Firma,
				MontoOperacion = s.MontoOperacion,
				UsuariosCaptura = s.UsuariosCaptura,
				UsuariosAutoriza = s.UsuariosAutoriza,
				TipoBeneficiario = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.TipoBeneficiario ELSE b.TipoBeneficiario END,
				Nombre = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Nombre ELSE b.Nombre END,
				Apellido_P = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_P ELSE b.Apellido_P END,
				Apellido_M = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_M ELSE b.Apellido_M END,
				country_ben = CASE 
					WHEN s.country_ben IS NULL OR TRIM(s.country_ben) <> '' 
					THEN s.country_ben ELSE b.country_ben END,
				city_res_ben = s.city_res_ben,
				city_add_ben = s.city_add_ben,
				city_num_ben = s.city_num_ben,
				city_col_ben = s.city_col_ben,
				city_zip_ben = s.city_zip_ben
			FROM tomser.dbo.beneficiarios b
			INNER JOIN @tmpModificacionFXI s on s.Id_Formato = b.Id_Formato;

			UPDATE b
			SET Firma = s.Firma,
				MontoOperacion = s.MontoOperacion,
				UsuariosCaptura = s.UsuariosCaptura,
				UsuariosAutoriza = s.UsuariosAutoriza,
				TipoBeneficiario = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.TipoBeneficiario ELSE b.TipoBeneficiario END,
				Nombre = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Nombre ELSE b.Nombre END,
				Apellido_P = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_P ELSE b.Apellido_P END,
				Apellido_M = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_M ELSE b.Apellido_M END,
				country_ben = CASE 
					WHEN s.country_ben IS NULL OR TRIM(s.country_ben) <> '' 
					THEN s.country_ben ELSE b.country_ben END,
				city_res_ben = s.city_res_ben,
				city_add_ben = s.city_add_ben,
				city_num_ben = s.city_num_ben,
				city_col_ben = s.city_col_ben,
				city_zip_ben = s.city_zip_ben
			FROM tb_bilun_beneficiarios b
			INNER JOIN @tmpModificacionFXI s on s.Id_Formato = b.Id_Formato;
   
  
		   --Insertar para aviso email del beneficiario  
			DELETE FROM tomser.dbo.nf_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpModificacionFXI
				WHERE InsertarAvisoMail = 1
			);

			DELETE FROM tb_bilun_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpModificacionFXI
				WHERE InsertarAvisoMail = 1
			);
		   
			INSERT INTO tomser.dbo.nf_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpModificacionFXI a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

			INSERT INTO tb_bilun_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpModificacionFXI a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;
 
  
		   /*    
			*  Bajas Para FXI    
		   */   

			DECLARE @tmpBajaFXI TABLE
			(
				ID_SOLICITUD_DOCUMENTO BIGINT,
				Id_Solicitud BIGINT,
				Id_Formato BIGINT,
				Formato VARCHAR(3),
				Operacion VARCHAR(25),
				Id_Cliente BIGINT,
				Alias VARCHAR(100),
				Nombre VARCHAR(100),
				ClaveBanco VARCHAR(5),
				Banco VARCHAR(100),
				TipoCuenta VARCHAR(100),
				NoCuenta VARCHAR(45),
				Firma INT,
				MontoOperacion DECIMAL(18,2),
				Moneda CHAR(3),
				UsuariosCaptura VARCHAR(250),
				UsuariosAutoriza VARCHAR(250),
				Indicador_Plantilla INT,
				Country VARCHAR(6),
				ABA VARCHAR(9),
				BancoBeneficiario VARCHAR(100),
				SwiftBeneficiario VARCHAR(11),
				CuentaBeneficiario VARCHAR(25),
				SwiftCorresponsal VARCHAR(11),
				BancoCorresponsal VARCHAR(100),
				residencia VARCHAR(100),
				rfc VARCHAR(20),
				origen VARCHAR(5),
				country_ben VARCHAR(5),
				COMMENT VARCHAR(100),
				emailBeneficiario VARCHAR(250),
				emailOrdenante VARCHAR(250),
				TipoBeneficiario VARCHAR(1),
				Apellido_P VARCHAR(40),
				Apellido_M VARCHAR(40),
				Id_Agreement BIGINT,
				city_res_ben VARCHAR(32),
				city_add_ben VARCHAR(45),
				city_num_ben VARCHAR(15),
				city_col_ben VARCHAR(30),
				city_zip_ben VARCHAR(10),
				InsertarAvisoMail INT
			);

			
			INSERT INTO @tmpBajaFXI
			SELECT s.*
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXI')
			AND UPPER(s.Operacion) = 'BAJA';
		   
			UPDATE tomser.dbo.beneficiarios
			SET Indicador_Plantilla = 3
			WHERE id_formato IN
			(
				SELECT Id_Formato
				FROM @tmpBajaFXI
			);

			UPDATE tb_bilun_beneficiarios
			SET Indicador_Plantilla = 3
			WHERE id_formato IN
			(
				SELECT Id_Formato
				FROM @tmpBajaFXI
			);
 
  
		   --Se eliminan Avisos.  
		   
			DELETE FROM tomser.dbo.nf_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpBajaFXI
			);

			DELETE FROM tb_bilun_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpBajaFXI
			);

  
		   /*    
			*  Altas Para FXE  
		   */  
  
		   
			DECLARE @tmpAltaFXE TABLE
			(
				Id_Solicitud BIGINT,
				Cont INT
			);

			INSERT INTO @tmpAltaFXE
			SELECT s.Id_Solicitud, COUNT(b.Id_Formato)
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXE')
			AND UPPER(s.Operacion) = 'ALTA'
			GROUP BY s.Id_Solicitud;
		   
			INSERT INTO tomser.dbo.beneficiarios
			(
				id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			)
			SELECT 
				s.id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			FROM @tmpAltaFXE a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud;
		   
			INSERT INTO tb_bilun_beneficiarios
			(
				id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			)
			SELECT 
				s.id_formato,formato,id_cliente,Alias,Nombre,ClaveBanco,Banco,TipoCuenta,NoCuenta,
				Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,rfc,COMMENT,
				TipoBeneficiario,Apellido_P,Apellido_M,Id_Agreement
			FROM @tmpAltaFXE a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud;
  
  
		   --Insertar para aviso email del beneficiario  

			INSERT INTO tomser.dbo.nf_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpAltaFXE a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

			INSERT INTO tb_bilun_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpAltaFXE a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

  
		   /*    
			*  Modificaciones Para FXE  
		   */  
		   
			DECLARE @tmpModificacionFXE TABLE
			(
				ID_SOLICITUD_DOCUMENTO BIGINT,
				Id_Solicitud BIGINT,
				Id_Formato BIGINT,
				Formato VARCHAR(3),
				Operacion VARCHAR(25),
				Id_Cliente BIGINT,
				Alias VARCHAR(100),
				Nombre VARCHAR(100),
				ClaveBanco VARCHAR(5),
				Banco VARCHAR(100),
				TipoCuenta VARCHAR(100),
				NoCuenta VARCHAR(45),
				Firma INT,
				MontoOperacion DECIMAL(18,2),
				Moneda CHAR(3),
				UsuariosCaptura VARCHAR(250),
				UsuariosAutoriza VARCHAR(250),
				Indicador_Plantilla INT,
				Country VARCHAR(6),
				ABA VARCHAR(9),
				BancoBeneficiario VARCHAR(100),
				SwiftBeneficiario VARCHAR(11),
				CuentaBeneficiario VARCHAR(25),
				SwiftCorresponsal VARCHAR(11),
				BancoCorresponsal VARCHAR(100),
				residencia VARCHAR(100),
				rfc VARCHAR(20),
				origen VARCHAR(5),
				country_ben VARCHAR(5),
				COMMENT VARCHAR(100),
				emailBeneficiario VARCHAR(250),
				emailOrdenante VARCHAR(250),
				TipoBeneficiario VARCHAR(1),
				Apellido_P VARCHAR(40),
				Apellido_M VARCHAR(40),
				Id_Agreement BIGINT,
				city_res_ben VARCHAR(32),
				city_add_ben VARCHAR(45),
				city_num_ben VARCHAR(15),
				city_col_ben VARCHAR(30),
				city_zip_ben VARCHAR(10),
				InsertarAvisoMail INT
			);

			INSERT INTO @tmpModificacionFXE
			SELECT s.*
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXE')
			AND UPPER(s.Operacion) = 'MODIFICACION';
			
			UPDATE b
			SET Firma = s.Firma,
				MontoOperacion = s.MontoOperacion,
				UsuariosCaptura = s.UsuariosCaptura,
				UsuariosAutoriza = s.UsuariosAutoriza,
				TipoBeneficiario = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.TipoBeneficiario ELSE b.TipoBeneficiario END,
				Nombre = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Nombre ELSE b.Nombre END,
				Apellido_P = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_P ELSE b.Apellido_P END,
				Apellido_M = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_M ELSE b.Apellido_M END,
				country_ben = CASE 
					WHEN s.country_ben IS NULL OR TRIM(s.country_ben) <> '' 
					THEN s.country_ben ELSE b.country_ben END,
				city_res_ben = s.city_res_ben,
				city_add_ben = s.city_add_ben,
				city_num_ben = s.city_num_ben,
				city_col_ben = s.city_col_ben,
				city_zip_ben = s.city_zip_ben
			FROM tomser.dbo.beneficiarios b
			INNER JOIN @tmpModificacionFXE s on s.Id_Formato = b.Id_Formato;
		   
			UPDATE b
			SET Firma = s.Firma,
				MontoOperacion = s.MontoOperacion,
				UsuariosCaptura = s.UsuariosCaptura,
				UsuariosAutoriza = s.UsuariosAutoriza,
				TipoBeneficiario = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.TipoBeneficiario ELSE b.TipoBeneficiario END,
				Nombre = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Nombre ELSE b.Nombre END,
				Apellido_P = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_P ELSE b.Apellido_P END,
				Apellido_M = CASE 
					WHEN s.TipoBeneficiario IS NULL OR TRIM(s.TipoBeneficiario) <> '' 
					THEN s.Apellido_M ELSE b.Apellido_M END,
				country_ben = CASE 
					WHEN s.country_ben IS NULL OR TRIM(s.country_ben) <> '' 
					THEN s.country_ben ELSE b.country_ben END,
				city_res_ben = s.city_res_ben,
				city_add_ben = s.city_add_ben,
				city_num_ben = s.city_num_ben,
				city_col_ben = s.city_col_ben,
				city_zip_ben = s.city_zip_ben
			FROM tb_bilun_beneficiarios b
			INNER JOIN @tmpModificacionFXE s on s.Id_Formato = b.Id_Formato;
  
  
		   --Insertar para aviso email del beneficiario  
			DELETE FROM tomser.dbo.nf_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpModificacionFXE
				WHERE InsertarAvisoMail = 1
			);

			DELETE FROM tb_bilun_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpModificacionFXE
				WHERE InsertarAvisoMail = 1
			);

			INSERT INTO tomser.dbo.nf_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpModificacionFXE a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

			INSERT INTO tb_bilun_beneficiarios_aviso
			(
				nf_tipo,nf_cliente,id_plantilla,id_formato_plantilla,id_envio,
				id_email_beneficiario,id_email_ordenante,id_email_ordenante_error
			)
			SELECT 
				s.Formato, s.Id_Cliente, s.Alias, s.Id_Solicitud,1,
				s.EmailBeneficiario,s.EmailOrdenante,s.EmailOrdenante
			FROM @tmpModificacionFXE a
			INNER JOIN @tmpSolicitudes s on s.Id_Solicitud = a.Id_Solicitud
			WHERE s.InsertarAvisoMail = 1;

  
		   /*    
			*  Bajas Para FXE  
		   */  
		   
			DECLARE @tmpBajaFXE TABLE
			(
				ID_SOLICITUD_DOCUMENTO BIGINT,
				Id_Solicitud BIGINT,
				Id_Formato BIGINT,
				Formato VARCHAR(3),
				Operacion VARCHAR(25),
				Id_Cliente BIGINT,
				Alias VARCHAR(100),
				Nombre VARCHAR(100),
				ClaveBanco VARCHAR(5),
				Banco VARCHAR(100),
				TipoCuenta VARCHAR(100),
				NoCuenta VARCHAR(45),
				Firma INT,
				MontoOperacion DECIMAL(18,2),
				Moneda CHAR(3),
				UsuariosCaptura VARCHAR(250),
				UsuariosAutoriza VARCHAR(250),
				Indicador_Plantilla INT,
				Country VARCHAR(6),
				ABA VARCHAR(9),
				BancoBeneficiario VARCHAR(100),
				SwiftBeneficiario VARCHAR(11),
				CuentaBeneficiario VARCHAR(25),
				SwiftCorresponsal VARCHAR(11),
				BancoCorresponsal VARCHAR(100),
				residencia VARCHAR(100),
				rfc VARCHAR(20),
				origen VARCHAR(5),
				country_ben VARCHAR(5),
				COMMENT VARCHAR(100),
				emailBeneficiario VARCHAR(250),
				emailOrdenante VARCHAR(250),
				TipoBeneficiario VARCHAR(1),
				Apellido_P VARCHAR(40),
				Apellido_M VARCHAR(40),
				Id_Agreement BIGINT,
				city_res_ben VARCHAR(32),
				city_add_ben VARCHAR(45),
				city_num_ben VARCHAR(15),
				city_col_ben VARCHAR(30),
				city_zip_ben VARCHAR(10),
				InsertarAvisoMail INT
			);

			INSERT INTO @tmpBajaFXE
			SELECT s.*
			FROM @tmpSolicitudes s
			LEFT JOIN tomser.dbo.beneficiarios b on b.Id_Formato = s.Id_Solicitud
			WHERE s.Formato IN ('FXE')
			AND UPPER(s.Operacion) = 'BAJA';

			UPDATE tomser.dbo.beneficiarios
			SET Indicador_Plantilla = 3
			WHERE id_formato IN
			(
				SELECT Id_Formato
				FROM @tmpBajaFXE
			);

			UPDATE tb_bilun_beneficiarios
			SET Indicador_Plantilla = 3
			WHERE id_formato IN
			(
				SELECT Id_Formato
				FROM @tmpBajaFXE
			);

  
		   --Se eliminan Avisos.  
		   
			DELETE FROM tomser.dbo.nf_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpBajaFXE
			);

			DELETE FROM tb_bilun_beneficiarios_aviso
			WHERE id_formato_plantilla IN
			(
				SELECT Id_Formato
				FROM @tmpBajaFXE
			);

		   
		   COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH

		IF XACT_STATE() <> 0    
			ROLLBACK TRANSACTION;    
  
        INSERT INTO Auditoria.ErrorLog    
		(    
			ErrorNumber,    
			ErrorSeverity,    
			ErrorState,    
			ErrorProcedure,    
			ErrorLine,    
			ErrorMessage    
		)    
		VALUES    
		(    
			ERROR_NUMBER(),    
			ERROR_SEVERITY(),    
			ERROR_STATE(),    
			ERROR_PROCEDURE(),    
			ERROR_LINE(),    
			ERROR_MESSAGE()    
		);    
 
		THROW;

	END CATCH
END
GO


using AplicacionSolicitudes.Interfaces;
using AplicacionSolicitudes.Modelos;
using AplicacionSolicitudes.Servicios;
using InfraestructuraSolicitudes.Repositorios;
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
using SolicitudesAplicacion;
using AplicacionSolicitudes.Servicios;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Solicitudes
{
    public partial class ServiceHost : WindowsServiceHostBase
    {
        #region Propiedades

        private Container _containerDI;
        private Configuracion _configuracionBase;
        private ConfiguracionServicio _configuracionServicio;

        protected override string NombreServicioLog => NombreServicio.Solicitudes.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor 
        /// </summary>|
        public ServiceHost() { }

        #endregion

        protected override void Inicializacion(string[] args, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            //Realiza la configuracion del _containerDI...
            _containerDI = new SimpleInjector.Container();
            _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
            _containerDI.Register<Alertas>(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);
            _containerDI.Register<IAlertas>(() => _containerDI.GetInstance<Alertas>(), Lifestyle.Singleton);
            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();

            //Almacena configuracion necesaria para el funcionamiento del servicio
            _configuracionBase = new Configuracion();
            _configuracionServicio = new ConfiguracionServicio();

            //Inicializa la obtención de la configuracion general y el logging...
            {
                ConfiguracionEncriptadaHelper.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Solicitudes.ToString());
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
                Logging.Write("INFO|OnStart|Leyendo archivo de configuración.");
            }

            //Registra la configuración como Transient + Factory
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<ConfiguracionServicio>(() => _configuracionServicio.Clone(), Lifestyle.Scoped);

            //Registro de Componentes
            _containerDI.Register<ITomi, TomiRepositorio>(Lifestyle.Transient);
            _containerDI.Register<ICMS, CMSRepositorio>(Lifestyle.Transient);
            _containerDI.Register<IProcesos, ProcesosServicio>(Lifestyle.Transient);
            _containerDI.Register<OperacionesServicio>(Lifestyle.Transient);

            _containerDI.Verify();
        }

        /// <summary>
        /// Realiza la obtención inicial de parámetros (una sola vez).
        /// </summary>
        protected override async Task ParametrizacionesAsync(CancellationToken cancellationToken)
        {
            try
            {
                //Obtiene el proceso actual.
                IParametrizacion _parametrizacion = _containerDI.GetInstance<IParametrizacion>();
                _configuracionBase = await _parametrizacion.ObtenerProcesoActualAsync(_configuracionBase, cancellationToken);

                //Marca el servicio en uso.
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
                await _controlServicio.MarcarPuertoEnUsoAsync(
                    _configuracionBase.Pid.Value, _configuracionBase.NombreProceso,
                    _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio,
                    cancellationToken);

                //Parametrizacion de la configuracion local
                {
                    _configuracionServicio.NombreServicio = _configuracionBase.NombreServicio;
                    _configuracionServicio.TimerInterval = _configuracionBase.TimerInterval;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ParametrizacionesAsync|{ex}");
                throw;
            }
        }

        /// Ejecuta el funcionamiento principal del servicio en cada tick.
        /// </summary>   
        protected override async Task OnTimerAsync(CancellationToken ct)
        {
            try
            {
                //Ejecuta el procedimiento principal del servicio...
                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    var operacionServicio = _containerDI.GetInstance<OperacionesServicio>();
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

namespace Solicitudes
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
                NombreServicio.Solicitudes.ToString(),
                    () => new ServiceHost(), args
#if DEBUG
                        , (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs)
#endif
            );
        }
    }
}


App.config
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
    <startup> 
        <supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.8" />
    </startup>
</configuration>


Solicitudes.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{7E0CDD20-4271-4946-B9C5-175FD5736363}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>Solicitudes</RootNamespace>
    <AssemblyName>Solicitudes</AssemblyName>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector">
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
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\AplicacionSolicitudes\AplicacionSolicitudes.csproj">
      <Project>{3F800036-139E-473D-84D0-4598DCC524AE}</Project>
      <Name>AplicacionSolicitudes</Name>
    </ProjectReference>
    <ProjectReference Include="..\InfraestructuraSolicitudes\InfraestructuraSolicitudes.csproj">
      <Project>{291D1F59-3922-463C-A15D-4CCEC55576F9}</Project>
      <Name>InfraestructuraSolicitudes</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


InfraestructuraSolicitudes.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{291D1F59-3922-463C-A15D-4CCEC55576F9}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>InfraestructuraSolicitudes</RootNamespace>
    <AssemblyName>InfraestructuraSolicitudes</AssemblyName>
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
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
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
    <Compile Include="Repositorios\CMSRepositorio.cs" />
    <Compile Include="Repositorios\TomiRepositorio.cs" />
    <Content Include="Scripts\Scripts_BDTomi.sql" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\AplicacionSolicitudes\AplicacionSolicitudes.csproj">
      <Project>{3f800036-139e-473d-84d0-4598dcc524ae}</Project>
      <Name>AplicacionSolicitudes</Name>
    </ProjectReference>
  </ItemGroup>
  <ItemGroup>
    <Content Include="Scripts\Scripts_BDCms.sql" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


AplicacionSolicitudes.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{3F800036-139E-473D-84D0-4598DCC524AE}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>AplicacionSolicitudes</RootNamespace>
    <AssemblyName>AplicacionSolicitudes</AssemblyName>
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
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
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
    <Compile Include="DTO\SolicitudesBeneficiarioDto.cs" />
    <Compile Include="DTO\ValidaBeneficiariosDto.cs" />
    <Compile Include="Enums\Constantes.cs" />
    <Compile Include="Enums\ParametrosSistemaENUM.cs" />
    <Compile Include="Enums\ProcedimientoAlmacenado.cs" />
    <Compile Include="Utilidades\Utilidades.cs" />
    <Compile Include="Interfaces\IProcesos.cs" />
    <Compile Include="Interfaces\ICMS.cs" />
    <Compile Include="Interfaces\ITomi.cs" />
    <Compile Include="Modelos\AlertasBeneficiarioModelo.cs" />
    <Compile Include="Modelos\ConfiguracionServicio.cs" />
    <Compile Include="Modelos\ProcesaAutorizacionBancosModelo.cs" />
    <Compile Include="Modelos\SolicitudBancoModelo.cs" />
    <Compile Include="Modelos\SolicitudesBeneficiariosCmsModelo.cs" />
    <Compile Include="Modelos\SolicitudesBeneficiarioTomserModelo.cs" />
    <Compile Include="Modelos\SolicitudParametrosModelo.cs" />
    <Compile Include="Modelos\SPIDEntidadesPermitidasModelo.cs" />
    <Compile Include="Modelos\SPIDEntidadesRestringidasModelo.cs" />
    <Compile Include="Modelos\ValidacionBeneficiariosModel.cs" />
    <Compile Include="OperacionesServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
    <Compile Include="Servicios\ProcesosServicio.cs" />
    <Compile Include="Servicios\ServiciosBase.cs" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>



