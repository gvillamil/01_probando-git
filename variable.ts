using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelo;
using CrystalDecisions.CrystalReports.Engine;
using CrystalDecisions.Shared;
using MUFG_T24.Comunes;
using MUFG_T24.Conexiones;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Logeo_utilidades;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Clase encargada de generar e imprimir reportes utilizando Crystal Reports.
    /// </summary>
    public class CrystalReporterWorker : ICrystalReporterWorker
    {
        private readonly Configuracion _configuracion;
        private readonly IRecuperacionDatosDao _recuperacionDatosDao;
        private string _ultimoCodigoError;
        private string _ultimoMensajeError;
        private Exception _ultimaExcepcion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public CrystalReporterWorker(Configuracion configuracion, IRecuperacionDatosDao recuperacionDatosDao)
        {
            _configuracion = configuracion;
            _recuperacionDatosDao = recuperacionDatosDao;
        }

        /// <summary>
        /// Imprime un folio empaquetado, iterando sobre cada reporte correspondiente al folio.
        /// </summary>
        public async Task<bool> ImprimirAsync(ImpresionFolioJob folioEmpaquetado, CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                foreach (ImpresionReporteJob reporte in folioEmpaquetado.Reportes)
                {
                    ct.ThrowIfCancellationRequested();

                    if (!GenerarReporte(reporte, ct))
                        return false;
                }

                bool folioMarcado = await _recuperacionDatosDao.MarcarImpreso(folioEmpaquetado.Proceso, folioEmpaquetado.Folio, ct);
                if (!folioMarcado)
                {
                    Logging.Write("ERROR|Formato|No se pudo marcar como impreso el folio: " + folioEmpaquetado.Folio);
                    return false;
                }

                Logging.Write("INFO|Formato|Se termina procesamiento de Formato: " + folioEmpaquetado.Folio);
                return true;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logging.Write("ERROR|Imprime_Operaciones|Se presenta exception: " + ex);
                return false;
            }
        }

        public async Task<ResultadoOperacionImpresion> ImprimirDetalleAsync(ImpresionFolioJob folioEmpaquetado, CancellationToken ct)
        {
            string correlationId = Guid.NewGuid().ToString("N");

            try
            {
                ct.ThrowIfCancellationRequested();

                foreach (ImpresionReporteJob reporte in folioEmpaquetado.Reportes)
                {
                    ct.ThrowIfCancellationRequested();

                    if (!GenerarReporte(reporte, ct))
                    {
                        return new ResultadoOperacionImpresion
                        {
                            Exito = false,
                            CorrelationId = correlationId,
                            Proceso = folioEmpaquetado.Proceso,
                            Folio = folioEmpaquetado.Folio,
                            Operacion = "GenerarReporte",
                            CodigoError = _ultimoCodigoError ?? "ERROR_GENERAR_REPORTE",
                            Mensaje = _ultimoMensajeError ?? "Fallo al generar reporte.",
                            EsReintentable = EsReintentable(_ultimaExcepcion),
                            Exception = _ultimaExcepcion
                        };
                    }
                }

                ResultadoOperacionImpresion marcado = await _recuperacionDatosDao.MarcarImpresoDetalleAsync(folioEmpaquetado.Proceso, folioEmpaquetado.Folio, ct);
                if (!marcado.Exito)
                {
                    Logging.Write("ERROR|Formato|CorrelationId=" + correlationId + "|No se pudo marcar como impreso el folio: " + folioEmpaquetado.Folio + ". Codigo=" + marcado.CodigoError + ". Mensaje=" + marcado.Mensaje);
                    return new ResultadoOperacionImpresion
                    {
                        Exito = false,
                        CorrelationId = correlationId,
                        Proceso = folioEmpaquetado.Proceso,
                        Folio = folioEmpaquetado.Folio,
                        Operacion = "MarcarImpreso",
                        CodigoError = marcado.CodigoError,
                        Mensaje = marcado.Mensaje,
                        EsReintentable = marcado.EsReintentable,
                        Exception = marcado.Exception
                    };
                }

                return new ResultadoOperacionImpresion
                {
                    Exito = true,
                    CorrelationId = correlationId,
                    Proceso = folioEmpaquetado.Proceso,
                    Folio = folioEmpaquetado.Folio,
                    Operacion = "Imprimir"
                };
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return new ResultadoOperacionImpresion
                {
                    Exito = false,
                    CorrelationId = correlationId,
                    Proceso = folioEmpaquetado.Proceso,
                    Folio = folioEmpaquetado.Folio,
                    Operacion = "Imprimir",
                    CodigoError = "WORKER_EXCEPTION",
                    Mensaje = ex.Message,
                    EsReintentable = EsReintentable(ex),
                    Exception = ex
                };
            }
        }

        /// <summary>
        /// Carga el reporte y lo manda a CrystalReports para imprimir.
        /// </summary>
        protected virtual bool GenerarReporte(ImpresionReporteJob reporte, CancellationToken ct)
        {
            ReportDocument documentoReporte = null;
            _ultimoCodigoError = null;
            _ultimoMensajeError = null;
            _ultimaExcepcion = null;

            try
            {
                Logging.Write("INFO|Imprime_Reporte|Iniciando la impresión del reporte:" + reporte.NombreReporte + ", formulas:" + reporte.Formulas + ".");

                if (string.IsNullOrWhiteSpace(reporte.Impresora))
                {
                    throw new InvalidOperationException("No se encontró impresora para el reporte: " + reporte.NombreReporte);
                }

                documentoReporte = new ReportDocument();

                Logging.Write("INFO|Imprime_Reporte|Imprimiendo Reporte='" + reporte.NombreReporte + "',impresora='" + reporte.Impresora + "',formulas='" + reporte.Formulas + "'.");

                string rutaReporte = _configuracion.ParametrosSistema[ConstantesServicio.ReportesDirectorio] + reporte.NombreReporte;
                Logging.Write("INFO|Imprime_Reporte|A cargar reporte '" + rutaReporte + "'.");
                documentoReporte.Load(rutaReporte);

                Logging.Write("INFO|Imprime_Reporte|Reporte cargado, a establecer origen de datos del reporte.");
                for (int i = 0; i < documentoReporte.DataSourceConnections.Count; i++)
                {
                    documentoReporte.DataSourceConnections[i].SetConnection(
                        ObtenerParametro(ConstantesServicio.Servidor),
                        ObtenerParametro(ConstantesServicio.Base),
                        ObtenerParametro(ConstantesServicio.Usuario),
                        ObtenerParametro(ConstantesServicio.Clave));
                }

                documentoReporte.Refresh();

                Logging.Write("INFO|Imprime_Reporte|A establecer en reporte datos de formulas '" + reporte.Formulas + "'.");
                AplicarFormulas(documentoReporte, reporte);

                Logging.Write("INFO|Imprime_Reporte|A imprimir reporte en '" + reporte.Impresora + "'.");
                documentoReporte.PrintOptions.PrinterName = reporte.Impresora;

                Logging.Write("INFO|Imprime_Reporte|Opciones de impresión establecidas, a imprimir reporte.");
                ct.ThrowIfCancellationRequested();
                string directorioReportes = _configuracion.ParametrosSistema[ConstantesServicio.ReportesDirectorio];
                if (string.IsNullOrWhiteSpace(directorioReportes))
                {
                    throw new InvalidOperationException("No se encontró configuración de directorio de reportes.");
                }

                string nombreReporteSinExtension = Path.GetFileNameWithoutExtension(reporte.NombreReporte);
                string timestamp = DateTime.Now.ToString("yyyyMMddHHmmssfff");
                string nombreArchivoPdf = nombreReporteSinExtension + "_" + timestamp + ".pdf";
                string rutaArchivoPdf = Path.Combine(directorioReportes, nombreArchivoPdf);

                documentoReporte.ExportToDisk(ExportFormatType.PortableDocFormat, rutaArchivoPdf);
                Logging.Write("INFO|Imprime_Reporte|PDF generado en '" + rutaArchivoPdf + "'.");                
                return true;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _ultimoCodigoError = ObtenerCodigoError(ex);
                _ultimoMensajeError = ex.Message;
                _ultimaExcepcion = ex;
                Logging.Write("ERROR|Imprime_Reporte|" + ex);
                return false;
            }
            finally
            {
                if (documentoReporte != null)
                {
                    documentoReporte.Close();
                    documentoReporte.Dispose();
                }
            }
        }

        private static void AplicarFormulas(ReportDocument documentoReporte, ImpresionReporteJob reporte)
        {
            if (!string.IsNullOrWhiteSpace(reporte.Formulas))
            {
                string[] formulasReporte = reporte.Formulas.Split('|');
                for (int i = 0; i < formulasReporte.Length; i++)
                {
                    string formulaRaw = formulasReporte[i];
                    string[] valores = formulaRaw.Split(new[] { '=' }, 2);

                    if (valores.Length != 2 || string.IsNullOrWhiteSpace(valores[0]) || string.IsNullOrWhiteSpace(valores[1]))
                    {
                        string mensaje = "Fórmula inválida para reporte '" + reporte.NombreReporte + "': '" + formulaRaw + "'. Formato esperado: Nombre=Valor.";
                        Logging.Write("ERROR|Imprime_Reporte|" + mensaje);
                        throw new FormatException(mensaje);
                    }

                    string nombreFormula = valores[0].Trim();
                    string valorFormula = valores[1].Trim();
                    bool formulaExiste = false;

                    for (int j = 0; j < documentoReporte.DataDefinition.FormulaFields.Count; j++)
                    {
                        if (string.Equals(documentoReporte.DataDefinition.FormulaFields[j].Name, nombreFormula, StringComparison.OrdinalIgnoreCase))
                        {
                            formulaExiste = true;
                            break;
                        }
                    }

                    if (!formulaExiste)
                    {
                        string mensaje = "La fórmula '" + nombreFormula + "' no existe en el reporte '" + reporte.NombreReporte + "'.";
                        Logging.Write("ERROR|Imprime_Reporte|" + mensaje);
                        throw new InvalidOperationException(mensaje);
                    }

                    documentoReporte.DataDefinition.FormulaFields[nombreFormula].Text = valorFormula;
                }
            }
        }

        private static bool EsReintentable(Exception ex)
        {
            if (ex == null)
                return true;

            if (ex is FormatException)
                return false;

            if (ex is InvalidOperationException)
                return false;

            return true;
        }

        private static string ObtenerCodigoError(Exception ex)
        {
            if (ex is FormatException)
                return "FORMULA_INVALIDA";

            if (ex is InvalidOperationException)
                return "OPERACION_INVALIDA";

            if (ex is IOException)
                return "IO_ERROR";

            return "ERROR_IMPRESION";
        }

        /// <summary>
        /// Obtiene el valor de un parámetro de conexión a la base de datos.
        /// </summary>
        private static string ObtenerParametro(string parametro)
        {
            string cadenaConexion = ConexionBasedatosFactory.ObtenerCadenaConexion(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI);
            SqlConnectionStringBuilder parametrosConexion = new SqlConnectionStringBuilder(cadenaConexion);

            switch (parametro)
            {
                case ConstantesServicio.Servidor:
                    return parametrosConexion.DataSource;
                case ConstantesServicio.Base:
                    return parametrosConexion.InitialCatalog;
                case ConstantesServicio.Usuario:
                    return parametrosConexion.UserID;
                case ConstantesServicio.Clave:
                    return parametrosConexion.Password;
                default:
                    return string.Empty;
            }
        }

    }
}
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelo;
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
        private readonly IRecuperacionDatosDao _recuperacionDatosDao;
        private readonly ICrystalReporterWorker _crystalReportWorker;
        private readonly EstadoHorarioOperacion _estadoHorarioOperacion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public OperacionServicio(Configuracion configuracion, 
            IControlServicio controlServicio,
            IAlertas alertas, 
            IParametrizacion parametrizacion,
            IRecuperacionDatosDao recuperacionDatosDao,
            ICrystalReporterWorker crystalReportWorker,
            EstadoHorarioOperacion estadoHorarioOperacion)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _alertas = alertas;
            _parametrizacion = parametrizacion;
            _recuperacionDatosDao = recuperacionDatosDao;
            _crystalReportWorker = crystalReportWorker;
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
                await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio);

                //Obtiene los parámetros del sistema...
                _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync();
                _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesServicio.ParametrosSistema);

                //Valida el horario de operación...
                TimeSpan timeHoraSistema = _parametrizacion.ObtenerHoraSistema();
                string horaSistema = timeHoraSistema.ToString(@"hh\:mm");
                TimeSpan horaOperacionInicio = ConversionesComunes.ParseTimeSpanRequerido(
                    _configuracion.ParametrosSistema[ConstantesGenerales.HoraOPInicio],
                    ConstantesGenerales.HoraOPInicio);
                TimeSpan horaOperacionFin = ConversionesComunes.ParseTimeSpanRequerido(
                    _configuracion.ParametrosSistema[ConstantesGenerales.HoraOPFin],
                    ConstantesGenerales.HoraOPFin);

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
                    return false;
                }

                ct.ThrowIfCancellationRequested();

                //Obtiene las operaciones y las agrupa por folio...
                IEnumerable<ImpresionPendienteDto> operaciones = await _recuperacionDatosDao.ObtieneFoliosOperaciones(ct);
                List<ImpresionFolioJob> foliosEmpaquetados = operaciones
                    .GroupBy(x => new { x.Proceso, x.Folio })
                    .OrderBy(g => g.Min(x => x.OrdenProceso))
                    .ThenBy(g => g.Key.Folio)
                    .Select(g => new ImpresionFolioJob
                    {
                        Proceso = g.Key.Proceso,
                        Folio = g.Key.Folio,
                        FolioEsString = g.First().FolioEsString,
                        Reportes = g
                            .OrderBy(x => x.OrdenReporte)
                            .Select(x => new ImpresionReporteJob
                            {
                                NombreReporte = x.NombreReporte,
                                Formulas = x.Formulas,
                                Impresora = x.Impresora,
                                OrdenReporte = x.OrdenReporte
                            })
                            .ToList()
                    })
                    .ToList();

                //Los manda a imprimir en un worker...
                foreach (ImpresionFolioJob folioEmpaquetado in foliosEmpaquetados)
                {
                    ct.ThrowIfCancellationRequested();
                    bool impresionExitosa = await _crystalReportWorker.ImprimirAsync(folioEmpaquetado, ct);
                    if (!impresionExitosa)
                    {
                        Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Fallo impresión en Proceso='{folioEmpaquetado.Proceso}', Folio='{folioEmpaquetado.Folio}'. Se continúa con el siguiente folio.");
                    }
                }
                
                ct.ThrowIfCancellationRequested();

                //Marca el termino del servicio...
                await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio);

                return true;
            }
            catch (OperationCanceledException)
            {
                Config.EscribirLog("INFO|Ejecuta_Procedimientos|Ejecución cancelada.");
            }
            catch (Exception ex) {
                Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Se presenta exception: {ex.ToString()}");
                int pidSeguro = _configuracion.Pid ?? 0;
                await _alertas.InsertarAlerta(new AlertaDTO()
                {
                    Folio = null,
                    TipoOPeracion = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = ConstantesClaveAviso.Critico,
                    Descipcion = "Error en ejecuta_procedimientos",
                    PId = pidSeguro,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = ex
                });
            }
            return false;
        }

    }
}
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    using Aplicacion;
using Aplicacion.Interfaces;
using Aplicacion.Servicios;
using Infraestructura.Repositorio;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Core.Configuracion;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System.Threading;
using System.Threading.Tasks;

namespace Impresion
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private Container _containerDI;
        private Configuracion _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.Impresion.ToString();
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
            _containerDI = new Container();
            _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
            _containerDI.Register(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);
            _containerDI.Register<IAlertas>(() => _containerDI.GetInstance<Alertas>(), Lifestyle.Singleton);
            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();

            //Objeto que almacena la configuración necesaria para el funcionamiento del servicio...
            _configuracionBase = new Configuracion();

            //Inicializa la obtención de la configuración general y el logging...
            {
                ObtencionConfguracionGeneral.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = ObtencionConfguracionGeneral.ObtenerServicio(NombreServicio.Impresion.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.PathLog = servicioMainConfiguracion.Log;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;

                Logging.Init(_configuracionBase.PathLog, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
            }

            //Registra la configuracion...
            _containerDI.Register(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);

            //Registra los componentes...
            _containerDI.Register<OperacionServicio>();
            _containerDI.Register<ICrystalReporterWorker, CrystalReporterWorker>();
            _containerDI.Register<IRecuperacionDatosDao, RecuperacionDatosDao>();

            _containerDI.Verify();
        }

        /// <summary>
        /// Realiza la obtención inicial de parámetros (una sola vez).
        /// </summary>
        protected override async Task ParametrizacionesAsync(CancellationToken ct)
        {
            //Obtiene el proceso actual...
            IParametrizacion _parametrizacion = _containerDI.GetInstance<IParametrizacion>();
            _configuracionBase = await _parametrizacion.ObtenerProcesoActualAsync(_configuracionBase, ct);

            //Marca el servicio en uso...
            IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
            _ = await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio, ct);
        }

        /// <summary>
        /// Ejecuta el funcionamiento principal del servicio en cada tick.
        /// </summary>   
        protected override async Task OnTimerAsync(CancellationToken ct)
        {
            //Ejecuta el procedimiento principal del servicio...
            using (AsyncScopedLifestyle.BeginScope(_containerDI))
            {
                OperacionServicio operacionServicio = _containerDI.GetInstance<OperacionServicio>();
                _ = await operacionServicio.EjecutarProcedimientosAsync(ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Realiza ciertas operaciones antes de finalizar el servicio.
        /// </summary>
        protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            if (_containerDI == null || _configuracionBase == null)
            {
                Logging.Info("OnStop|Terminación omitida por inicialización incompleta.");
                return;
            }

            using (AsyncScopedLifestyle.BeginScope(_containerDI))
            {
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();

                await _controlServicio.EsperarTareaActivaAsync(startupTask, ConstantesSistema.ParametrizacionInicial, waitCt).ConfigureAwait(false);
                await _controlServicio.EsperarTareaActivaAsync(runningTask, ConstantesSistema.ProcesamientoActivo, waitCt).ConfigureAwait(false);

                //Marca el servicio en uso...
                _ = await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);

                //Marca el termino del servicio...
                _ = await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Especifica los recursos a liberar al finalizar el servicio.
        /// </summary>
        protected override void LiberarRecursosTerminacion()
            => _containerDI?.Dispose();

    }
}
-------------------------------------------------------------------------------------------------------------------------------------------------------------
    using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelo;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase de recuperacion de datos.
    /// </summary>
    public class RecuperacionDatosDao : RepositorioMaestro, IRecuperacionDatosDao
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public RecuperacionDatosDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los folios de operaciones pendientes de impresión.
        /// </summary>
        public async Task<IEnumerable<ImpresionPendienteDto>> ObtieneFoliosOperaciones(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ImpresionPendienteDto>(ConstantesServicio.uspObtenerPendientes);
            ct.ThrowIfCancellationRequested();
            return resultado.Data;
        }

        /// <summary>
        /// Marca un folio como impreso.
        /// </summary>
        public async Task<bool> MarcarImpreso(string proceso, string folio, CancellationToken ct)
        {
            ResultadoOperacionImpresion detalle = await MarcarImpresoDetalleAsync(proceso, folio, ct);
            return detalle.Exito;
        }

        public async Task<ResultadoOperacionImpresion> MarcarImpresoDetalleAsync(string proceso, string folio, CancellationToken ct)
        {
            string correlationId = Guid.NewGuid().ToString("N");

            try
            {
                ct.ThrowIfCancellationRequested();
                var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.uspMarcarImpreso, cmd =>
                {
                    _ = cmd.Parameters.Add(basedatosConexion.CrearParametro("@Proceso", proceso));
                    _ = cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });
                ct.ThrowIfCancellationRequested();

                if (resultado.Exito)
                {
                    return new ResultadoOperacionImpresion
                    {
                        Exito = true,
                        CorrelationId = correlationId,
                        Proceso = proceso,
                        Folio = folio,
                        Operacion = "MarcarImpreso"
                    };
                }

                return new ResultadoOperacionImpresion
                {
                    Exito = false,
                    CorrelationId = correlationId,
                    Proceso = proceso,
                    Folio = folio,
                    Operacion = "MarcarImpreso",
                    CodigoError = "DB_MARK_FAILED",
                    Mensaje = "El procedimiento de marcado no confirmó éxito.",
                    EsReintentable = true
                };
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return new ResultadoOperacionImpresion
                {
                    Exito = false,
                    CorrelationId = correlationId,
                    Proceso = proceso,
                    Folio = folio,
                    Operacion = "MarcarImpreso",
                    CodigoError = "DB_EXCEPTION",
                    Mensaje = ex.Message,
                    EsReintentable = true,
                    Exception = ex
                };
            }
        }

    }
}
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  usp_impresion_marcarImpreso
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-08-06

DESCRIPCIÓN         :
Marca como impreso el folio indicado para el proceso de impresión
correspondiente.

ALCANCE / NOTAS     :
- Operación de actualización.
- Mantiene la lógica legacy.

PARÁMETROS          :
@Proceso NVARCHAR(100)
@Folio   NVARCHAR(100)

CONTROL DE CAMBIOS  :
2026-08-06 | Rafael Domínguez | Creacion SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_impresion_marcarImpreso
    @Proceso nvarchar(100),
    @Folio nvarchar(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF @Proceso = 'FXD_Local'
        BEGIN
            UPDATE tb_tombin_nf_fxd_local
            SET nf_impresion_voucher = 1
            WHERE nf_folio = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        IF @Proceso = 'FXI_Local'
        BEGIN
            UPDATE tb_tombin_nf_fxi_local
            SET nf_impresion_voucher = 1
            WHERE nf_folio = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        IF @Proceso = 'Solicitud_Alta_Usuario'
        BEGIN
            UPDATE tomser.dbo.solicitudes_usuarios_cms
            SET impresion_voucher = 1
            WHERE id_solicitud = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        IF @Proceso = 'Solicitud_Traspaso_Token'
        BEGIN
            UPDATE tomser.dbo.solicitudes_usuarios_cms
            SET impresion_voucher = 1
            WHERE id_solicitud = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        IF @Proceso = 'Solicitud_Reposicion_Token'
        BEGIN
            UPDATE tomser.dbo.solicitudes_usuarios_cms
            SET impresion_voucher = 1
            WHERE id_solicitud = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        IF @Proceso = 'HotScan'
        BEGIN
            UPDATE vw_nf_scan
            SET flg_impresion = 1
            WHERE id_scan = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        IF @Proceso = 'HotScan_v2'
        BEGIN
            UPDATE tbl_hotscan_alertas
            SET flg_impresion = 1
            WHERE id_alerta = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        IF @Proceso = 'SWIFT_IN'
        BEGIN
            UPDATE tb_srv_swf_trabajo
            SET nf_impresion = 1
            WHERE nf_tipo = 'SWIFT_IN'
              AND nf_folio = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        IF @Proceso = 'FXI_Operaciones_Especiales'
        BEGIN
            UPDATE tb_tombin_nf_fxi
            SET nf_impresion_voucher = 1
            WHERE nf_folio = @Folio;
            COMMIT TRANSACTION;
            RETURN;
        END

        RAISERROR('Proceso de impresion no soportado.', 16, 1);
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

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
---------------------------------------------------------------------------------------------------------------------------------------------------------
    USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  usp_impresion_obtenerPendientes
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-08-06

DESCRIPCIÓN         :
Obtiene los registros pendientes de impresión y la impresora asociada
al reporte correspondiente.

ALCANCE / NOTAS     :
- Operación de consulta.
- Mantiene la lógica legacy.

PARÁMETROS          :
N/A

CONTROL DE CAMBIOS  :
2026-08-06 | Rafael Domínguez | Creacion SP
***********************************************************************/
--EXEC usp_impresion_obtenerPendientes
CREATE OR ALTER PROCEDURE dbo.usp_impresion_obtenerPendientes
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        SELECT
            Pendientes.Proceso,
            Pendientes.Folio,
            Pendientes.FolioEsString,
            Pendientes.NombreReporte,
            Pendientes.Formulas,
            Pendientes.OrdenProceso,
            Pendientes.OrdenReporte,
            Impresora.des_impresora AS Impresora
        FROM (
            --SELECT
            --    'FXD_Local' AS Proceso,
            --    CONVERT(nvarchar(100), nf_folio) AS Folio,
            --    CAST(0 AS bit) AS FolioEsString,
            --    Reportes.NombreReporte,
            --    'Folios=' + CONVERT(nvarchar(100), nf_folio) + '|FoliosF=' + CONVERT(nvarchar(100), nf_folio) AS Formulas,
            --    10 AS OrdenProceso,
            --    Reportes.OrdenReporte
            --FROM tb_tombin_nf_fxd_local
            --CROSS APPLY (VALUES
            --    (1, 'new_fxd_local.rpt')
            --) AS Reportes(OrdenReporte, NombreReporte)
            --WHERE nf_estatus IN ('PEND','CONF')
            --  AND nf_impresion_voucher = 0

            --UNION ALL

            --SELECT
            --    'FXI_Local' AS Proceso,
            --    CONVERT(nvarchar(100), nf_folio) AS Folio,
            --    CAST(0 AS bit) AS FolioEsString,
            --    Reportes.NombreReporte,
            --    'Folios=' + CONVERT(nvarchar(100), nf_folio) + '|FoliosF=' + CONVERT(nvarchar(100), nf_folio) AS Formulas,
            --    20 AS OrdenProceso,
            --    Reportes.OrdenReporte
            --FROM tb_tombin_nf_fxi_local
            --CROSS APPLY (VALUES
            --    (1, 'new_fxi_local.rpt')
            --) AS Reportes(OrdenReporte, NombreReporte)
            --WHERE nf_estatus IN ('PEND','CONF')
            --  AND nf_impresion_voucher = 0

            --UNION ALL

            --SELECT
            --    'Solicitud_Alta_Usuario' AS Proceso,
            --    CONVERT(nvarchar(100), id_solicitud) AS Folio,
            --    CAST(0 AS bit) AS FolioEsString,
            --    Reportes.NombreReporte,
            --    'Folios=' + CONVERT(nvarchar(100), id_solicitud) AS Formulas,
            --    30 AS OrdenProceso,
            --    Reportes.OrdenReporte
            --FROM vw_solicitudes_usuarios_cms
            --CROSS APPLY (VALUES
            --    (1, 'SolicitudToken.rpt'),
            --    (2, 'SolicitudTokenOS.rpt'),
            --    (3, 'SolicitudTokenCarta.rpt')
            --) AS Reportes(OrdenReporte, NombreReporte)
            --WHERE status = 'AUTORIZADO'
            --  AND descripcion IN ('ALTA DE USUARIO')
            --  AND impresion_voucher = 0

            --UNION ALL

            --SELECT
            --    'Solicitud_Traspaso_Token' AS Proceso,
            --    CONVERT(nvarchar(100), id_solicitud) AS Folio,
            --    CAST(0 AS bit) AS FolioEsString,
            --    Reportes.NombreReporte,
            --    'Folios=' + CONVERT(nvarchar(100), id_solicitud) AS Formulas,
            --    40 AS OrdenProceso,
            --    Reportes.OrdenReporte
            --FROM vw_solicitudes_usuarios_cms
            --CROSS APPLY (VALUES
            --    (1, 'SolicitudTokenTraspaso.rpt'),
            --    (2, 'SolicitudTokenCarta.rpt')
            --) AS Reportes(OrdenReporte, NombreReporte)
            --WHERE status = 'AUTORIZADO'
            --  AND descripcion IN ('TRASPASO DE TOKEN')
            --  AND impresion_voucher = 0

            --UNION ALL

            --SELECT
            --    'Solicitud_Reposicion_Token' AS Proceso,
            --    CONVERT(nvarchar(100), id_solicitud) AS Folio,
            --    CAST(0 AS bit) AS FolioEsString,
            --    Reportes.NombreReporte,
            --    'Folios=' + CONVERT(nvarchar(100), id_solicitud) AS Formulas,
            --    50 AS OrdenProceso,
            --    Reportes.OrdenReporte
            --FROM vw_solicitudes_usuarios_cms
            --CROSS APPLY (VALUES
            --    (1, 'SolicitudTokenReposicion.rpt'),
            --    (2, 'SolicitudTokenReposicionOS.rpt'),
            --    (3, 'SolicitudTokenCarta.rpt')
            --) AS Reportes(OrdenReporte, NombreReporte)
            --WHERE status = 'AUTORIZADO'
            --  AND descripcion IN ('REPOSICION DE TOKEN')
            --  AND impresion_voucher = 0

            --UNION ALL

            --SELECT
            --    'HotScan' AS Proceso,
            --    CONVERT(nvarchar(100), id_scan) AS Folio,
            --    CAST(0 AS bit) AS FolioEsString,
            --    Reportes.NombreReporte,
            --    'Folio=' + CONVERT(nvarchar(100), id_scan) AS Formulas,
            --    60 AS OrdenProceso,
            --    Reportes.OrdenReporte
            --FROM vw_nf_scan
            --CROSS APPLY (VALUES
            --    (1, 'new_HotScan.rpt')
            --) AS Reportes(OrdenReporte, NombreReporte)
            --WHERE cve_estatus_inter IN ('HOTP','HOTF')
            --  AND (flg_impresion IS NULL OR flg_impresion = 0)

            --UNION ALL

            --SELECT
            --    'HotScan_v2' AS Proceso,
            --    CONVERT(nvarchar(100), id_alerta) AS Folio,
            --    CAST(0 AS bit) AS FolioEsString,
            --    Reportes.NombreReporte,
            --    'Folio=' + CONVERT(nvarchar(100), id_alerta) AS Formulas,
            --    70 AS OrdenProceso,
            --    Reportes.OrdenReporte
            --FROM tbl_hotscan_alertas
            --CROSS APPLY (VALUES
            --    (1, 'HotScan_v2.rpt')
            --) AS Reportes(OrdenReporte, NombreReporte)
            --WHERE cve_estatus_inter IN ('HOTP','HOTF')
            --  AND (flg_impresion IS NULL OR flg_impresion = 0)

            --UNION ALL

            --SELECT
            --    'SWIFT_IN' AS Proceso,
            --    CONVERT(nvarchar(100), Mto.folio) AS Folio,
            --    CAST(1 AS bit) AS FolioEsString,
            --    Reportes.NombreReporte,
            --    'Folio="' + CONVERT(nvarchar(100), Mto.folio) + '"' AS Formulas,
            --    80 AS OrdenProceso,
            --    Reportes.OrdenReporte
            --FROM tb_srv_swf_plan_trabajo_maestro Mto, tb_srv_swf_trabajo Trb
            --CROSS APPLY (VALUES
            --    (1, 'new_SPEI_IN.rpt')
            --) AS Reportes(OrdenReporte, NombreReporte)
            --WHERE cve_estatus IN ('EPAB', 'RECC','PROC')
            --  AND Mto.folio = Trb.nf_folio
            --  AND Trb.nf_tipo = 'SWIFT_IN'
            --  AND Trb.nf_impresion = 0
            --  AND Mto.cve_mensaje NOT IN ('MT-210')

            --UNION ALL

            SELECT
                'FXI_Operaciones_Especiales' AS Proceso,
                CONVERT(nvarchar(100), nf_folio) AS Folio,
                CAST(0 AS bit) AS FolioEsString,
                Reportes.NombreReporte,
                'Folios=' + CONVERT(nvarchar(100), nf_folio) + '|FoliosF=' + CONVERT(nvarchar(100), nf_folio) AS Formulas,
                90 AS OrdenProceso,
                Reportes.OrdenReporte
            FROM tb_tombin_nf_fxi
            CROSS APPLY (VALUES
                (1, 'new_fxi.rpt')
            ) AS Reportes(OrdenReporte, NombreReporte)
            WHERE nf_impresion_voucher = 0
        ) AS Pendientes
        OUTER APPLY (
            SELECT TOP (1)
                Impresoras.des_impresora
            FROM tb_srv_reporte_impresora Impresoras
            WHERE UPPER(Impresoras.des_reporte) = UPPER(Pendientes.NombreReporte)
        ) AS Impresora
        ORDER BY
            Pendientes.OrdenProceso,
            Pendientes.Folio,
            Pendientes.OrdenReporte;
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
------------------------------------------------------------------------------------------------------------------------------------
    namespace Aplicacion.Enums
{
    /// <summary>
    /// Clase que contiene las constantes a modo de enumerador.
    /// Por compatibilidad: NET Framework 4.8 no permite enumeradores strings.
    /// </summary>
    public static class ConstantesServicio
    {
        //Constantes de parametrización...
        public const string ReportesDirectorio = "REPORTES_DIRECTORIO";
        public const string ParametrosSistema = "'" + ReportesDirectorio + 
            "','HORA_LECTURA_INICIO','HORA_LECTURA_FIN','HORA_OP_INICIO','HORA_OP_FIN'";

        //Constantes de procedimientos del servicio...
        public const string uspObtenerPendientes = "usp_impresion_obtenerPendientes";
        public const string uspMarcarImpreso = "usp_impresion_marcarImpreso";

        //Constantes de parámetros de conexión...
        public const string Servidor = "SERVIDOR";
        public const string Base = "BASE";
        public const string Usuario = "USUARIO";
        public const string Clave = "CLAVE";
    }
}

------------------------------------------------------------------------------------------------------------------------------------
Impresion.csproj
------------------------------------------------------------------------------------------------------------------------------------
    <?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{57FD4377-DC64-499C-9E30-D7EB4DAF0F3B}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>Impresion</RootNamespace>
    <AssemblyName>Impresion</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
    <Deterministic>true</Deterministic>
    <NuGetPackageImportStamp>
    </NuGetPackageImportStamp>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <PlatformTarget>x64</PlatformTarget>
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
    <StartupObject>Impresion.Program</StartupObject>
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
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x64'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x64\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x64</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
    <Prefer32Bit>true</Prefer32Bit>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x64'">
    <OutputPath>bin\x64\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x64</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
    <Prefer32Bit>true</Prefer32Bit>
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
    <Reference Include="System.Core" />
    <Reference Include="System.Numerics" />
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
    <ProjectReference Include="..\AplicacionImpresion\AplicacionImpresion.csproj">
      <Project>{1FA1DB62-EBA9-426D-8B97-0E9BC1144250}</Project>
      <Name>AplicacionImpresion</Name>
    </ProjectReference>
    <ProjectReference Include="..\InfraestructuraImpresion.csproj\InfraestructuraImpresion.csproj">
      <Project>{65447CC8-24C4-4A50-AF01-482F91BBC466}</Project>
      <Name>InfraestructuraImpresion</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>
------------------------------------------------------------------------------------------------------------------------------------
                InfraestructuraImpresion.csproj
------------------------------------------------------------------------------------------------------------------------------------
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{65447CC8-24C4-4A50-AF01-482F91BBC466}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>InfraestructuraImpresion</RootNamespace>
    <AssemblyName>InfraestructuraImpresion</AssemblyName>
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
    <PlatformTarget>x64</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x64'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x64\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x64</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x64'">
    <OutputPath>bin\x64\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x64</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
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
    <Compile Include="Repositorio\RecuperacionDatosDao.cs" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\AplicacionImpresion\AplicacionImpresion.csproj">
      <Project>{1FA1DB62-EBA9-426D-8B97-0E9BC1144250}</Project>
      <Name>AplicacionImpresion</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
