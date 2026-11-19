using AplicacionZeus_Files.Enums;
using AplicacionZeus_Files.Interfaces;
using AplicacionZeus_Files.Modelos;
using AplicacionZeus_Files.Operaciones;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AplicacionZeus_Files
{
    /// <summary>
    /// Servicio que maneja la operacion principal
    /// </summary>
    public class OperacionServicio
    {
        #region Propiedades

        private readonly ConfiguracionModelo _configuracion;
        private readonly IOperacionesDWH _operacionDWH;
        private readonly Productos _productos;
        private readonly Archivos _archivos;

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor de clase para ejecutar las operaciones
        /// </summary>
        /// <param name="configuracion">Inyección de la configuración</param>
        /// <param name="operacionesDWH">Inyección para la base de datos DWH</param>
        public OperacionServicio(ConfiguracionModelo configuracion,
                                IOperacionesDWH operacionesDWH,
                                Productos productos,
                                Archivos archivos)
        {
            _configuracion = configuracion;
            _operacionDWH = operacionesDWH;
            _productos = productos;
            _archivos = archivos;
        }

        #endregion

        #region Métodos

        /// <summary>
        /// Función Principal para ejecutar las operaciones del Servicio.
        /// </summary>
        /// <returns></returns>
        public async Task EjecutarProcedimientosAsync(CancellationToken ct)
        {
            try
            {
                #region Validaciones Parámetros

                ct.ThrowIfCancellationRequested();

                // Carga los parametros de configuración para el inicio de las operaciones
                Dictionary<string, string> dicParametrosSistema = await _operacionDWH.ParametrosSistema();

                foreach (var paramSistema in dicParametrosSistema)
                {
                    if (!Enum.TryParse(paramSistema.Key, out ParametrosSistema parametro)) continue;

                    switch (parametro)
                    {
                        case ParametrosSistema.RUTA_ZEUS_ENVIO:
                            _configuracion.StrRutaZeusEnvio = paramSistema.Value;
                            Config.EscribirLog($"INFO|obtenerParametrosSistema|Ruta de envio de archivo local '{paramSistema.Value}'");
                            break;

                        case ParametrosSistema.RUTA_ZEUS_RESP:
                            _configuracion.StrRutaZeusResp = paramSistema.Value;
                            Config.EscribirLog($"INFO|obtenerParametrosSistema|Ruta de respaldo de archivo en red '{paramSistema.Value}'");
                            break;

                        case ParametrosSistema.MAIL_ZEUS_TO:
                            _configuracion.StrMailZeusTo = paramSistema.Value;
                            Config.EscribirLog($"INFO|obtenerParametrosSistema|Mails zeus to '{paramSistema.Value}'");
                            break;

                        case ParametrosSistema.MAIL_ZEUS_CC:
                            _configuracion.StrMailZeusCc = paramSistema.Value;
                            Config.EscribirLog($"INFO|obtenerParametrosSistema|Mails zeus to '{paramSistema.Value}'");
                            break;
                    }
                }

                #endregion

                int numeroProductos = await _productos.BuscaProductos(ct);

                if (numeroProductos > 0)
                {
                    Config.EscribirLog($"INFO|Ejecuta_Procedimientos|Se encontraron {numeroProductos} productos a generar (Zeus->Files).");
                    await _archivos.ProcesaArchivos(ct);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Se presenta exception: {ex}");
                throw;
            }
        }
        #endregion
    }
}

using AplicacionZeus_Files.Enums;
using AplicacionZeus_Files.Interfaces;
using AplicacionZeus_Files.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AplicacionZeus_Files.Operaciones
{
    /// <summary>
    /// Clase donde implementan métodos archivos
    /// </summary>
    public class Archivos
    {
        #region Propiedades

        private readonly ConfiguracionModelo _configuracion;
        private readonly IOperacionesDWH _operacionesDWH;

        #endregion

        #region Constructor

        /// <summary>
        /// Contructor de la case archivos
        /// </summary>
        /// <param name="configuracion">Inyección de la configuracíon</param>
        /// <param name="operacionesDwh">Inyección del acceso a base de datos</param>
        public Archivos(ConfiguracionModelo configuracion, IOperacionesDWH operacionesDwh)
        {
            _configuracion = configuracion;
            _operacionesDWH = operacionesDwh;
        }

        #endregion

        #region Métodos

        /// <summary>
        /// Función para Procesar los archivos de Zeus
        /// </summary>
        /// <returns></returns>
        public async Task<bool> ProcesaArchivos(CancellationToken ct)
        {
            try
            {
                RegistrosModelo registros;
                FileInfo oFile = null;
                string strBuffer = string.Empty;
                string strFechaArchivo = string.Empty;
                string strZeus_file_name = string.Empty;
                string strZeus_file = string.Empty;
                string strZeus_fileResp = string.Empty;
                string strZeus_file_noExt = string.Empty;
                string fechaProceso = string.Empty;
                long iFileSize = 0;
                int iRegs = 0;

                Config.EscribirLog("INFO|ProcesaArchivos|Inicio de procesa_Archivos.");

                //Lista de Procesos a Realizar
                List<ProcesosEjecutarModelo> listaProcesos = await _operacionesDWH.ListarProcesosEjecutar(Constantes.CLAVE_PROYECTO);

                //Bsucams que no tenga repetido la misma tabla Zeus y el mismo archivo
                List<ProcesosEjecutarModelo> listaProcesar = listaProcesos
                                                            .GroupBy(x => new { x.des_proyecto_tabla, x.des_proyecto_files })
                                                            .Select(g => g.First())
                                                            .ToList();

                //Lista de tareas de Queries de ajuste de datos solicitados por NY
                List<TareasAjusteDtosModelo> listaTareasAjuste = await _operacionesDWH.ListarTareasAjusteDatos();


                foreach (var item in listaTareasAjuste)
                    item.des_query = item.des_query.Replace("$id_trabajo$", item.id_trabajo.ToString());

                //Hay queries a realizar al INICIO antes de la realización de los procesos?
                List<TareasAjusteDtosModelo> lstTareasInicio = listaTareasAjuste.Where(x => string.Equals(x.cve_momento,
                                                                                                         Constantes.INI,
                                                                                                         StringComparison.OrdinalIgnoreCase)).ToList();

                if (string.IsNullOrWhiteSpace(_configuracion.StrRutaZeusEnvio))
                    throw new InvalidOperationException("ERROR|ProcesaArchivos|No se encontro la ruta de StrRutaZeusEnvio.");

                if (string.IsNullOrWhiteSpace(_configuracion.StrRutaZeusResp))
                    throw new InvalidOperationException("ERROR|ProcesaArchivos|No se encontro la ruta de StrRutaZeusResp.");

                if (string.IsNullOrWhiteSpace(_configuracion.StrMailZeusTo))
                    throw new InvalidOperationException("ERROR|ProcesaArchivos|No se encontro la ruta de StrMailZeusTo.");

                foreach (var queryAjuste in lstTareasInicio)
                {
                    try
                    {
                        //Marco el inicio del proceso de control
                        registros = await _operacionesDWH.MarcarInicioFinProcesoControl(queryAjuste.id_trabajo, queryAjuste.num_orden);

                        Config.EscribirLog($"INFO|ProcesaArchivos|QueryAjuste|Procesando: IdTrabajo: {queryAjuste.id_trabajo}, " +
                            $"NumeroOrden: {queryAjuste.num_orden}, Producto: '{queryAjuste.cve_producto}', Tipo: '{queryAjuste.cve_tipo}', " +
                            $"Tabla: '{queryAjuste.des_tabla}', Descripcion: '{queryAjuste.des_tabla}', SQL: *{queryAjuste.des_query}*.");

                        Config.EscribirLog($"INFO|ProcesaArchivos|QueryAjuste|Número de registros procesados:  {registros.num_regs}");
                    }
                    catch (Exception ex)
                    {
                        Config.EscribirLog($"ERROR|ProcesaArchivos|QueryAjuste|Se presenta exception: Procedimiento: '{ProcedimientoAlmacenado.sp_MarcarInicioFinProcesoControl}', Error:{ex}");
                        await _operacionesDWH.MarcarErrorProcesoControl(queryAjuste.id_trabajo, queryAjuste.num_orden, ex.Message);
                    }
                }

                //Valida si hay PreQueries pendientes de procesar
                registros = await _operacionesDWH.ObtenerPreQueriesPendientes();
                if (registros.num_regs > 0)
                {
                    Config.EscribirLog($"ERROR|ProcesaArchivos|QueryAjuste|Hay {registros.num_regs} reglas de ajustes de datos iniciales pendientes de realizar.");
                    return false;
                }

                //Barrido de los Procesos a realizar
                foreach (var tarea in listaProcesar)
                {
                    ct.ThrowIfCancellationRequested();

                    try
                    {
                        int registrosPorTerminar = 0;
                        string listaProcPorTerminar = string.Empty;

                        Config.EscribirLog($"INFO|ProcesaArchivos|Procesando: IdTrabajo: {tarea.id_trabajo}, Producto: '{tarea.cve_producto}', Tipo: '{tarea.cve_tipo}', " +
                                            $"TipoLayout: '{(tarea.cve_producto == Constantes.GENERAL ? Constantes.GENERAL : Constantes.SALDO)}', Sistema: '{tarea.cve_sistema}'," +
                                            $" FechaProceso: '{tarea.fecha_proceso}', TablaProyecto: '{tarea.des_proyecto_tabla}', Archivo:'{tarea.des_proyecto_files}'");

                        //Busca por trabajos compuestos (ejemplo DEPOSITS y DEPOSITS_CH son un solo archivo y se debe esperar a que terminen ambos para escribir el archivo)
                        var listaProcesosTerminar = await _operacionesDWH
                                                            .BuscarTrabajosCompuestos(tarea.id_trabajo, tarea.cve_producto, tarea.des_proyecto_tabla, tarea.des_proyecto_files);

                        if (listaProcesosTerminar.Count > 0)
                        {
                            registrosPorTerminar = listaProcesosTerminar.Count;
                            listaProcPorTerminar = string.Join(Environment.NewLine, listaProcesosTerminar.Select(x => $" - Producto: {x.cve_producto}, Tipo: {x.cve_tipo}, Sistema: {x.cve_sistema}"));
                        }

                        if (registrosPorTerminar > 0)
                            Config.EscribirLog($"INFO|ProcesaArchivos|Es necesario esperar la terminación de los siguientes procesos para escribir en archivo: {listaProcPorTerminar}");
                        else
                        {
                            // Marca proceso(s) como terminado para aquellos que no tienen proyecto y se no escriben archivo pero complementarios la información de productos
                            // (ej. FX_TRADE + FX_POSTING + FX_PRICER = FX, pero solo FX escribe archivo, con esto todos los productos terminaron la fase FILE)
                            RegistrosModelo registrosModelo = await _operacionesDWH.MarcarTerminadosSinProyecto(tarea.id_trabajo, Constantes.CLAVE_PROYECTO);
                            registrosPorTerminar = registrosModelo.num_regs;

                            Config.EscribirLog($"INFO|ProcesaArchivos|Se marcaron {registrosPorTerminar} registros como termino de proceso para trabajos generadores de productos.");

                            // Marca proceso(s) como iniciado
                            registrosModelo = await _operacionesDWH.MarcarProcesosInicio(tarea.id_trabajo, tarea.cve_producto, tarea.des_proyecto_tabla, tarea.des_proyecto_files);

                            Config.EscribirLog($"INFO|ProcesaArchivos|Se marcaron {registrosPorTerminar} registros como inicio de proceso.");

                            //Validaciones Especiales o Complementarias
                            //Proceso Estandard
                            Config.EscribirLog($"INFO|ProcesaArchivos|Obteniendo la estructura en Zeus la tabla '{tarea.des_proyecto_tabla}'.");

                            //Proceso creacion archivo
                            fechaProceso = tarea.fecha_proceso.ToString();
                            strFechaArchivo = ObtenerFechaFormato(fechaProceso, "MMddyyyy");
                            strZeus_file_name = tarea.des_proyecto_files.Replace("$fecha_presentacion$", strFechaArchivo);
                            strZeus_file_noExt = strZeus_file_name.Replace(".TXT", "");

                            try
                            {
                                if (!Directory.Exists($"{_configuracion.StrRutaZeusEnvio}{fechaProceso}\\"))
                                    Directory.CreateDirectory($"{_configuracion.StrRutaZeusEnvio}{fechaProceso}\\");

                                if (!Directory.Exists($"{_configuracion.StrRutaZeusResp}{fechaProceso}\\"))
                                    Directory.CreateDirectory($"{_configuracion.StrRutaZeusResp}{fechaProceso}\\");
                            }
                            catch (Exception ex)
                            {
                                Config.EscribirLog($"ERROR|ProcesaArchivos|Error: {ex}");
                            }

                            strZeus_fileResp = $"{_configuracion.StrRutaZeusResp}{fechaProceso}\\{strZeus_file_name}";
                            strZeus_file = $"{_configuracion.StrRutaZeusEnvio}{fechaProceso}\\{strZeus_file_name}";

                            string strZeus_fileTmp = $"{strZeus_file}.tmp";
                            string strZeus_fileRespTmp = $"{strZeus_fileResp}.tmp";

                            if (File.Exists(strZeus_fileTmp))
                                File.Delete(strZeus_fileTmp);

                            if (File.Exists(strZeus_fileRespTmp))
                                File.Delete(strZeus_fileRespTmp);

                            //Se obtiene la estructura de una tabla zeus
                            List<EstructuraTablaZeusModel> tabla = await _operacionesDWH.ObtenerEstructuraTablaZeus(
                                tarea.cve_producto,
                                tarea.cve_tipo,
                                tarea.cve_sistema,
                                tarea.des_proyecto_tabla);

                            if (tabla.Count == 0)
                                throw new Exception($"No fue posible obtener la estructura en Zeus de la tabla '{tarea.des_proyecto_tabla}" +
                                    $"', posiblemente la tabla no existe o no hay los permisos necesarios para obtener la información.");

                            List<DatosArchivoModelo> datosZeus = await _operacionesDWH.ProcesoDatosArchivoModelo(
                                tarea.cve_producto,
                                tarea.cve_tipo,
                                tarea.cve_sistema,
                                tarea.des_proyecto_tabla,
                                tarea.id_trabajo);

                            iRegs = 0;
                            using (StreamWriter writterPrincipal = new StreamWriter(strZeus_fileTmp, true, Encoding.ASCII))
                            using (StreamWriter writterRespaldo = new StreamWriter(strZeus_fileRespTmp, true, Encoding.ASCII))
                            {
                                foreach (var datosZ in datosZeus)
                                {
                                    strBuffer = "";
                                    strBuffer = datosZ.datos_archivo;

                                    await EscribeArchivo(writterPrincipal, strBuffer);
                                    await EscribeArchivo(writterRespaldo, strBuffer);

                                    iRegs++;
                                }
                            }

                            if (File.Exists(strZeus_file))
                                File.Replace(
                                    strZeus_fileTmp,
                                    strZeus_file,
                                    null);
                            else
                                File.Move(strZeus_fileTmp, strZeus_file);

                            if (File.Exists(strZeus_fileResp))
                                File.Replace(
                                    strZeus_fileRespTmp,
                                    strZeus_fileResp,
                                    null);
                            else
                                File.Move(strZeus_fileRespTmp, strZeus_fileResp);

                            Config.EscribirLog($"INFO|ProcesaArchivos|Número de registros procesados: {iRegs}");
                            iFileSize = 0;
                            if (iRegs > 0)
                            {
                                try
                                {
                                    oFile = new FileInfo(strZeus_file);
                                    iFileSize = oFile.Length;
                                }
                                catch (Exception ex)
                                {
                                    Config.EscribirLog($"AVISO|ProcesaArchivos|No se pudo obtener información del tamaño del archivo {strZeus_file}, error: {ex}");
                                }
                            }

                            //Marco Procesos como Terminados
                            registrosModelo = await _operacionesDWH.MarcarTerminados(tarea.id_trabajo, Constantes.CLAVE_PROYECTO, iRegs, strZeus_file_noExt, iFileSize, tarea.des_proyecto_tabla, tarea.des_proyecto_files);

                            Config.EscribirLog($"INFO|ProcesaArchivos|Se marcaron {registrosModelo.num_regs} registros como termino de proceso.");
                            //parte agregada para el TrailerRecord 
                            Config.EscribirLog($"INFO|ProcesaArchivos|Escribiendo Detalle a el Archivo {strZeus_file_name} con: {iRegs} al final del Archivo");

                            List<TrailerRecordModelo> oTrailerRecordFinal = await _operacionesDWH.ObtenerDatosTrailerRecord(tarea.id_trabajo, strZeus_file_noExt);

                            if (oTrailerRecordFinal != null && oTrailerRecordFinal.Count > 0)
                            {
                                string strTrailerRecordFinal = string.Empty;
                                foreach (var _record in oTrailerRecordFinal)
                                {
                                    strTrailerRecordFinal = _record.Datos.Replace(":", "");
                                }

                                using (StreamWriter writterPrincipal = new StreamWriter(strZeus_file, true, Encoding.ASCII))
                                using (StreamWriter writterRespaldo = new StreamWriter(strZeus_fileResp, true, Encoding.ASCII))
                                {

                                    await EscribeArchivo(writterPrincipal, strTrailerRecordFinal);
                                    await EscribeArchivo(writterRespaldo, strTrailerRecordFinal);
                                }
                            }
                        }

                        Config.EscribirLog("INFO|ProcesaArchivos|Verificando si ya se Pueden emitir Archivos por SFTP");

                        int iArchivosFaltantes = 0;
                        registros = await _operacionesDWH.ComprobarArchivosFaltantes(tarea.id_trabajo);

                        if (registros != null)
                            iArchivosFaltantes = registros.Faltantes;


                        if (iArchivosFaltantes == 0)
                        {
                            Config.EscribirLog("INFO|ProcesaArchivos|* * * * * * CREACION DE ARCHIVOS COMPLETA !!!!!!");
                            Config.EscribirLog($"INFO|ProcesaArchivos|Generando una lista de archivos de la ruta '{_configuracion.StrRutaZeusEnvio}'.");

                            // Realizo la extracción de una visualización del mail
                            string listadoArchivos = ConstruirListadoArchivos(_configuracion.StrRutaZeusEnvio, fechaProceso);

                            Config.EscribirLog($"INFO|envia_Archivos|Proceso Ejecutado 'dir' {_configuracion.StrRutaZeusEnvio}, salida: \n {listadoArchivos}");
                            int iResultado = 0;

                            iResultado = await EnviarAviso(
                                            tarea.id_trabajo,
                                            tarea.fecha_proceso,
                                            listadoArchivos);

                            Config.EscribirLog($"INFO|ProcesaArchivos|Envío email de archivos creados: {iResultado}");
                        }
                        else
                            Config.EscribirLog($"INFO|ProcesaArchivos|Faltan: {iArchivosFaltantes} Registros de la Tabla de Control para Enviarse los Archivos");

                    }
                    catch (Exception ex)
                    {
                        Config.EscribirLog($"ERROR|ProcesaArchivos|Procesando idTrabajo: {tarea.id_trabajo}, claveProducto: {tarea.cve_producto}, claveTipo: {tarea.cve_tipo}," +
                            $" ClaveSistema: {tarea.cve_sistema}. Se presenta exception: {ex} ");

                        // Marco el Proceso como con error
                        try
                        {
                            await _operacionesDWH.MarcarProcesoConError(tarea.id_trabajo, ex.Message, tarea.cve_producto, tarea.cve_tipo, tarea.cve_sistema);
                        }
                        catch (Exception ex2)
                        {
                            Config.EscribirLog($"ERROR|ProcesaArchivos|***** NO SE PUDO MARCAR ERROR EN id_trabajo={tarea.id_trabajo}, exception: {ex2.Message}");
                        }
                    }
                }

                // Verificar Si termino la Carga
                iRegs = 0;
                registros = await _operacionesDWH.ValidarFinCarga();

                if (registros != null)
                    iRegs = registros.num_regs;

                if (iRegs == 0)
                {
                    foreach (var ajuste in listaTareasAjuste)
                    {
                        try
                        {
                            //Marco el inicio del proceso de control
                            await _operacionesDWH.MarcarInicioControl(ajuste.id_trabajo, ajuste.num_orden, ajuste.des_query);

                            Config.EscribirLog($"INFO|ProcesaArchivos|queryAjuste_FIN|" + $"IdTrabajo={ajuste.id_trabajo}, " + $"NumOrden={ajuste.num_orden}");
                            Config.EscribirLog($"INFO|ProcesaArchivos|queryAjuste_FIN|Número de registros procesados:{iRegs}");
                        }
                        catch (Exception ex)
                        {
                            Config.EscribirLog($"ERROR|ProcesaArchivos|queryAjuste_FIN|" + $"IdTrabajo={ajuste.id_trabajo}, " + $"NumOrden={ajuste.num_orden}, Error={ex.Message}");
                            await _operacionesDWH.MarcarAjusteConError(ajuste.id_trabajo, ajuste.num_orden, ex.Message);
                        }
                    }
                }

                // Verificar si hay Pre-queries pendientes de procesar
                iRegs = 0;
                registros = await _operacionesDWH.ConsultarRegistrosSP(ProcedimientoAlmacenado.sp_ConsultarRegistrosCtlPrcZeusReglasDatD_Pendientes);

                if (registros != null)
                    iRegs = registros.num_regs;

                if (iRegs > 0)
                {
                    Config.EscribirLog($"ERROR|ProcesaArchivos|queryAjuste|Hay {iRegs} reglas de ajustes de datos pendientes de realizar al final del proceso.");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ProcesaArchivos|Se presenta exception:{ex.Message}");
                throw;
            }

            return true;
        }

        /// <summary>
        /// Función para Envíar el aviso de archivos por correo
        /// </summary>
        /// <param name="strMailZeusTo">Dirección de Correo a donde se va a enviar</param>
        /// <param name="strMailZeusCc">Dirección de correo con copia</param>
        /// <param name="id_trabajo">Id del trabajo</param>
        /// <param name="fecha_proceso">Fecha del Proceso</param>
        /// <param name="listaArchivos">Listado de los archivos generados</param>
        /// <returns></returns>
        private async Task<int> EnviarAviso(long id_trabajo, int fecha_proceso, string listaArchivos)
        {
            try
            {
                string rutaPlantilla = Path.Combine(
                     Path.GetDirectoryName(typeof(Archivos).Assembly.Location),
                    "Plantillas", "AvisoEmail.html");

                if (!File.Exists(rutaPlantilla))
                    throw new FileNotFoundException($"No se encontró la plantilla de correo {rutaPlantilla}");

                string html = File.ReadAllText(rutaPlantilla, Encoding.UTF8);
                string fechaProceso = WebUtility.HtmlEncode(fecha_proceso.ToString());
                string asunto = $"{Constantes.SUBJECT} {fecha_proceso}";
                string idTrabajo = WebUtility.HtmlEncode(id_trabajo.ToString());
                string rutaZeusResp = WebUtility.HtmlEncode(_configuracion.StrRutaZeusResp);
                string rutaZeusEnv = WebUtility.HtmlEncode(_configuracion.StrRutaZeusEnvio);

                string listaArchivos_ = WebUtility.HtmlEncode(listaArchivos)
                .Replace(" ", "&nbsp;")
                .Replace(Environment.NewLine, "<br>");

                html = html
                    .Replace("{{FECHA_PROCESO}}", fechaProceso)
                    .Replace("{{ID_TRABAJO}}", idTrabajo)
                    .Replace("{{RUTA_ZEUS_RESP}}", rutaZeusResp)
                    .Replace("{{RUTA_ZEUS_ENVIO}}", rutaZeusEnv)
                    .Replace("{{LISTA_ARCHIVOS}}", listaArchivos_);

                bool respuestaCorreo = await CorreoHelper.EnviarCorreo(
                    new string[] { Constantes.MAIL_FROM },
                    _configuracion.StrMailZeusTo,
                    _configuracion.StrMailZeusCc,
                    string.Empty,
                    asunto,
                    html);

                if (respuestaCorreo)
                    return 1;
                else
                    return 0;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|EnviaAviso|Ocurre exception: {ex}");
            }

            return 0;
        }

        /// <summary>
        /// Función para escribir el archivo en la carpeta de Zeus
        /// </summary>
        /// <param name="archivoNombre">Nombre del archivo</param>
        /// <param name="texto">Contenido del archivo</param>
        /// <returns></returns>
        private Task EscribeArchivo(StreamWriter writter, string texto) =>  writter.WriteLineAsync(texto);

        /// <summary>
        /// Hace un parse a una fecha en un formato especifico
        /// </summary>
        /// <param name="fechaProceso"></param>
        /// <param name="formato"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        private string ObtenerFechaFormato(string fechaProceso, string formato)
        {
            if (DateTime.TryParseExact(
                fechaProceso, "yyyyMMdd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var fecha))
            {
                return fecha.ToString(formato);
            }
            else
            {
                throw new ArgumentException("La fecha no posee un formato válido");
            }
        }

        /// <summary>
        /// Construye el listado de archivos que se incorpora en el cuerpo del correo
        /// </summary>
        /// <param name="rutaBase"></param>
        /// <param name="fechaProceso"></param>
        /// <returns></returns>
        private string ConstruirListadoArchivos(string rutaBase, string fechaProceso)
        {
            string rutaDirectorio = Path.Combine(rutaBase, fechaProceso);

            if (!Directory.Exists(rutaDirectorio))
                return $"No se encontró el directorio {rutaDirectorio}";

            var archivos = Directory
                .EnumerateFiles(rutaDirectorio, "*", SearchOption.TopDirectoryOnly)
                .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase);

            var salida = new StringBuilder();
            foreach (string archivo in archivos)
            {
                salida.AppendLine(Path.GetFileName(archivo));
            }

            return salida.Length > 0
                ? salida.ToString()
                : "No se encontraron archivos";
        }

        #endregion
    }
}

using AplicacionZeus_Files.Enums;
using AplicacionZeus_Files.Interfaces;
using AplicacionZeus_Files.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace AplicacionZeus_Files.Operaciones
{
    /// <summary>
    /// Clase que implementa los métodos de productos
    /// </summary>
    public class Productos
    {
        #region Propiedades

        private readonly IOperacionesDWH _OperacionesDWH;

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor de la clase
        /// </summary>
        /// <param name="operacionesDwh"></param>
        public Productos(IOperacionesDWH operacionesDwh)
        {
            _OperacionesDWH = operacionesDwh;
        }

        #endregion

        #region Métodos

        /// <summary>
        /// Método que implementa la busqueda de productos
        /// </summary>
        /// <returns></returns>
        public async Task<int> BuscaProductos(CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                int pendientes = 0;
                int iRegs = 0;

                RegistrosModelo regsCtlProceso = await _OperacionesDWH
                    .ConsultarRegistrosSP(ProcedimientoAlmacenado.sp_ConsultarRegistrosCtlProcesoD);

                RegistrosModelo regsZeusreglas = await _OperacionesDWH
                    .ConsultarRegistrosSP(ProcedimientoAlmacenado.sp_ConsultarRegistrosCtlPrcZeusReglasDatD);

                if (regsCtlProceso != null)
                    pendientes = regsCtlProceso.num_regs;

                if (regsZeusreglas != null)
                    pendientes += regsZeusreglas.num_regs;

                if (pendientes == 0)
                {
                    RegistrosModelo regsCtlProcesoPend = await _OperacionesDWH
                        .ConsultarRegistrosSP(ProcedimientoAlmacenado.sp_ConsultarRegistrosCtlProcesoD_Pendientes);

                    RegistrosModelo regsZeusreglasPend = await _OperacionesDWH
                        .ConsultarRegistrosSP(ProcedimientoAlmacenado.sp_ConsultarRegistrosCtlPrcZeusReglasDatD_Pendientes);

                    if (regsCtlProcesoPend != null)
                        iRegs = regsCtlProcesoPend.num_regs;

                    if (regsZeusreglasPend != null)
                        iRegs += regsZeusreglasPend.num_regs;
                }

                return iRegs;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|BuscaProductos|Se presentó una excepción {ex}");
                throw;
            }
        }

        #endregion
    }
}

using AplicacionZeus_Files.Modelos;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AplicacionZeus_Files.Interfaces
{
    /// <summary>
    /// Interfaz de operaciones DWH
    /// </summary>
    public interface IOperacionesDWH
    {
        Task<Dictionary<string, string>> ParametrosSistema();
        Task<RegistrosModelo> ConsultarRegistrosSP(string strSPName);
        Task<List<ProcesosEjecutarModelo>> ListarProcesosEjecutar(string Cve_proyecto);
        Task<List<TareasAjusteDtosModelo>> ListarTareasAjusteDatos();
        Task<RegistrosModelo> MarcarInicioFinProcesoControl(long id_trabajo, int num_orden);
        Task<RegistrosModelo> ObtenerPreQueriesPendientes();
        Task<List<TrabajosCompuestosModelo>> BuscarTrabajosCompuestos(long IdTrabajo, string CveProyecto, string DesProyectotabla, string DesProyectoFiles);
        Task<RegistrosModelo> MarcarTerminadosSinProyecto(long IdTrabajo, string CveProyecto);
        Task<RegistrosModelo> MarcarProcesosInicio(long IdTrabajo, string CveProyecto, string DesProyectotabla, string DesProyectoFiles);
        Task<List<EstructuraTablaZeusModel>> ObtenerEstructuraTablaZeus(string CveProducto, string CveTipo, string CveSistema, string TablaZeus);
        Task<RegistrosModelo> MarcarTerminados(long IdTrabajo, string CveProyecto, int NumReg, string ZeusFileName, long FileSize, string TablaZeus, string ArchivoOriginal);
        Task<List<TrailerRecordModelo>> ObtenerDatosTrailerRecord(long IdTrabajo, string ZeusFileName);
        Task<RegistrosModelo> ComprobarArchivosFaltantes(long IdTrabajo);
        Task<RegistrosModelo> ValidarFinCarga();
        Task<bool> MarcarInicioControl(long IdTrabajo, int NumOrden, string SqlQuery);
        Task MarcarAjusteConError(long IdTrabajo, int NumOrden, string MensajeError);
        Task MarcarErrorProcesoControl(long id_trabajo, int num_orden, string mensaje_error);
        Task MarcarProcesoConError(long IdTrabajo, string MensajeError, string CveProducto, string CveTipo, string CveSistema);
        Task<List<DatosArchivoModelo>> ProcesoDatosArchivoModelo(string claveProducto, string claveTipo, string claveSistema, string tablaZeus, long idTrabajo);
    }
}

namespace AplicacionZeus_Files.Enums
{
    /// <summary>
    /// Clase donde se almacenan las constantes del servicio
    /// </summary>
    public class Constantes
    {
        public const string CLAVE_PROYECTO = "ZEUS";
        public const string MAIL_FROM = "avisos_zeus@mx.mufg.jp";
        public const string SUBJECT = "Generacion de archivos Zeus, fechaProceso:";
        public const string GENERAL = "GRAL";
        public const string SALDO = "SALDO";
        public const string ID_TRABAJO = "ID_TRABAJO";
        public const string INI = "INI";
    }
}

namespace AplicacionZeus_Files.Enums
{
    /// <summary>
    /// Clases donde almacenan los procedimientos almacenados en constantes
    /// </summary>
    public class ProcedimientoAlmacenado
    {
        public const string sp_Parametros_Sistema = "dbo.MUFG_MX2_Comun_ObtenerParametrosSistema";
        public const string sp_ConsultarRegistrosCtlProcesoD = "dbo.MUFG_MX2_ZeusFiles_ConsultarRegsCtlProcesos_d";
        public const string sp_ConsultarRegistrosCtlPrcZeusReglasDatD = "dbo.MUFG_MX2_ZeusFiles_ConsultarRegsZeusReglasDat_d";
        public const string sp_ConsultarRegistrosCtlProcesoD_Pendientes = "dbo.MUFG_MX2_ZeusFiles_ConsultarRegsCtlProcesos_d_Pendientes";
        public const string sp_ConsultarRegistrosCtlPrcZeusReglasDatD_Pendientes = "dbo.MUFG_MX2_ZeusFiles_ConsultarRegsZeusReglasDat_d_Pendientes";
        public const string sp_ConsultarProcesosEjecutar = "dbo.MUFG_MX2_ZeuzFiles_ListarprocesosEjecutar";
        public const string sp_ConsultarTareasAjusteDatos = "dbo.MUFG_MX2_ZeuzFiles_ListarTareasAjusteDatos";
        public const string sp_MarcarInicioFinProcesoControl = "dbo.MUFG_MX2_ZeuzFiles_MarcarInicioFinProcesoControl";
        public const string sp_MarcarErrorControl = "dbo.MUFG_MX2_ZeuzFiles_MarcarErrorProcesoControl";
        public const string sp_ConsultarPreQueriesPendientes = "dbo.MUFG_MX2_ZeuzFiles_ObtenerPreQuerisPendientesProc";
        public const string sp_ConsultarTrabajosCompuestos = "dbo.MUFG_MX2_ZeusFiles_BuscarTrabajosCompuestos";
        public const string sp_MarcarTerminadosSinProyecto = "dbo.MUFG_MX2_ZeusFiles_MarcarTerminadosSinProyecto";
        public const string sp_MarcarInicioProcesos = "dbo.MUFG_MX2_ZeusFiles_MarcarProcesosInicio";
        public const string sp_ObtenerEstructuraTablaZeus = "dbo.MUFG_MX2_ZeusFiles_ObtenerEstructuraTablaZeus";
        public const string sp_ConsultaDatosArchivo = "dbo.MUFG_MX2_ZeusFiles_DatosArchivoConsulta";
        public const string sp_MarcarTerminados = "dbo.MUFG_MX2_ZeusFiles_MarcarTerminados";
        public const string sp_ObtenerDatosTrailerRecord = "dbo.MUFG_MX2_ZeusFiles_ObtenerDatosTrailerRecord";
        public const string sp_ComprobarArchivosFaltantes = "dbo.MUFG_MX2_ZeusFiles_ComprobarArchivosFaltantes";
        public const string sp_MarcarProcesoConError = "dbo.MUFG_MX2_ZeusFiles_MarcarProcesoConError";
        public const string sp_ValidarFinalCarga = "dbo.MUFG_MX2_ZeusFiles_ValidarFinalCarga";
        public const string sp_MarcarInicioControl = "dbo.MUFG_MX2_ZeusFiles_MarcarInicioControl";
        public const string sp_MarcarAjusteConError = "dbo.MUFG_MX2_ZeusFiles_MarcarAjusteConError";
        public const string sp_ProcesoDatosArchivoModelo = "dbo.MUFG_MX2_ZeusFiles_ProcesoDatosArchivoModelo";
    }
}

using MUFG_T24.Configuracion_utilidades;

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Clase configuracion Modelo
    /// </summary>
    public class ConfiguracionModelo : Configuracion
    {
        public string StrRutaZeusEnvio { get; set; }
        public string StrRutaZeusResp { get; set; }
        public string StrMailZeusTo { get; set; }
        public string StrMailZeusCc { get; set; }
        public new ConfiguracionModelo Clone()
        {
            return (ConfiguracionModelo)MemberwiseClone();
        }
    }
}

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Clase datos modelo archivo
    /// </summary>
    public class DatosArchivoModelo
    {
        public string datos_archivo { get; set; }
    }
}

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Estructura Tabla Zeus Model
    /// </summary>
    public class EstructuraTablaZeusModel
    {
        public string column_name { get; set; }
        public int? character_maximum_length { get; set; }
        public string des_alineacion { get; set; }
        public int num_posicion { get; set; }
    }
}

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Clase de modelo ParametrosModelo
    /// </summary>
    public class ParametrosModelo
    {
        public string Id_parametro { get; set; }
        public string des_valor { get; set; }
    }
}

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Clase de modelo ProcesosEjecutarModelo
    /// </summary>
    public class ProcesosEjecutarModelo
    {
        public long id_trabajo { get; set; }
        public string cve_producto { get; set; }
        public string cve_tipo { get; set; }
        public string cve_sistema { get; set; }
        public int fecha_proceso { get; set; }
        public string des_proyecto_tabla { get; set; }
        public string des_proyecto_files { get; set; }
    }
}

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Clase de modelo RegistrosModelo
    /// </summary>
    public class RegistrosModelo
    {
        public int num_regs { get; set; }
        public int Faltantes { get; set; }
    }
}

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Clase modelo TareasAjusteDtosModelo
    /// </summary>
    public class TareasAjusteDtosModelo
    {
        public long id_trabajo { get; set; }
        public string cve_momento { get; set; }
        public string cve_producto { get; set; }
        public string cve_tipo { get; set; }
        public string cve_sistema { get; set; }
        public int num_orden { get; set; }
        public string des_tabla { get; set; }
        public string des_descripcion { get; set; }
        public string des_query { get; set; }
    }
}

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Clase modelo TrabajosCompuestosModelo
    /// </summary>
    public class TrabajosCompuestosModelo
    {
        public string cve_producto { get; set; }
        public string cve_tipo { get; set; }
        public string cve_sistema { get; set; }
    }
}

namespace AplicacionZeus_Files.Modelos
{
    /// <summary>
    /// Clase modelo TrailerRecordModelo
    /// </summary>
    public class TrailerRecordModelo
    {
        public string Datos { get; set; }
    }
}

using System.Reflection;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("InfraestructuraZeus_Files")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("InfraestructuraZeus_Files")]
[assembly: AssemblyCopyright("Copyright ©  2026")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible
// to COM components.  If you need to access a type in this assembly from
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("a2d06809-77b4-45be-95cc-6b29ee61a766")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version
//      Build Number
//      Revision
//
[assembly: AssemblyVersion("1.0.0.0")]
[assembly: AssemblyFileVersion("1.0.0.0")]

using AplicacionZeus_Files.Enums;
using AplicacionZeus_Files.Interfaces;
using AplicacionZeus_Files.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace InfraestructuraZeus_Files.Repositorios
{
    /// <summary>
    /// Clase donde se implementan llamadas a BD
    /// </summary>
    public class DWHRepository : RepositorioMaestro, IOperacionesDWH
    {
        #region Constructor

        /// <summary>
        /// Constructor del repositorio para acceso a Base de datos
        /// </summary>
        /// <param name="alertaT"></param>
        public DWHRepository() :
            base(BasedatosProveedor.SqlServer, NombreBasedatos.DWH, null, null)
        { }

        #endregion

        #region Métodos

        /// <summary>
        /// Función para obtener los parametros del Sistema
        /// </summary>
        /// <returns></returns>
        public async Task<Dictionary<string, string>> ParametrosSistema()
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

            Dictionary<string, string> parametros = new Dictionary<string, string>();
            foreach (var parametro in result.Data)
            {
                if (parametros.ContainsKey(parametro.Id_parametro))
                {
                    Config.EscribirLog($"ERROR|obtenerParametrosSistema|El parámetro '{parametro.Id_parametro}' está duplicado en la base de datos.");
                    continue;
                }

                parametros.Add(parametro.Id_parametro, parametro.des_valor);
            }

            return parametros;
        }

        /// <summary>
        /// Función para consultar los Registros Pendientes
        /// </summary>
        /// <param name="strSPName"></param>
        /// <returns></returns>
        public async Task<RegistrosModelo> ConsultarRegistrosSP(string strSPName)
        {
            var result = await EjecutarStoredProcedureQuery<RegistrosModelo>(strSPName);

            if (result.Exito)
                return result?.Data?.FirstOrDefault() ?? new RegistrosModelo();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}"); ;
        }

        /// <summary>
        /// Función para obtener los Procesos que se ejecutarán
        /// </summary>
        /// <param name="claveProyecto">Clave del proyecto a consultar</param>
        /// <returns></returns>
        public async Task<List<ProcesosEjecutarModelo>> ListarProcesosEjecutar(string claveProyecto)
        {
            var result = await EjecutarStoredProcedureQuery<ProcesosEjecutarModelo>(ProcedimientoAlmacenado.sp_ConsultarProcesosEjecutar, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_proyecto", claveProyecto));
            });

            if (result.Exito)
                return result.Data ?? new List<ProcesosEjecutarModelo>();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}");
        }

        /// <summary>
        /// Obtener la lista de Tareas a Ejecutar
        /// </summary>
        /// <returns></returns>
        public async Task<List<TareasAjusteDtosModelo>> ListarTareasAjusteDatos()
        {
            var result = await EjecutarStoredProcedureQuery<TareasAjusteDtosModelo>(ProcedimientoAlmacenado.sp_ConsultarTareasAjusteDatos);

            if (result.Exito)
                return result.Data ?? new List<TareasAjusteDtosModelo>();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}");
        }

        /// <summary>
        /// Actualiza el estado del Proceso
        /// </summary>
        /// <param name="idTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="numeroOrden">Número de Orden de la tarea a ejecutar</param>
        /// <returns></returns>
        public async Task<RegistrosModelo> MarcarInicioFinProcesoControl(long idTrabajo, int numeroOrden)
        {
            var result = await EjecutarStoredProcedureQuery<RegistrosModelo>(ProcedimientoAlmacenado.sp_MarcarInicioFinProcesoControl, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@id_trabajo", idTrabajo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_orden", numeroOrden));
            });

            if (result.Exito)
                return result?.Data?.FirstOrDefault() ?? new RegistrosModelo();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}"); ;
        }

        /// <summary>
        /// Función para obtener las sentencias pendientes de ejecutar 
        /// </summary>
        /// <returns></returns>
        public async Task<RegistrosModelo> ObtenerPreQueriesPendientes()
        {
            var result = await EjecutarStoredProcedureQuery<RegistrosModelo>(ProcedimientoAlmacenado.sp_ConsultarPreQueriesPendientes);

            if (result.Exito)
                return result?.Data?.FirstOrDefault() ?? new RegistrosModelo();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}"); ;
        }

        /// <summary>
        /// Función para obtener trabajos adicionales Pendientes
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="claveProyecto">Clave del proyecto a consultar</param>
        /// <param name="DesProyectotabla">Tabla de donde se obtiene la información</param>
        /// <param name="DesProyectoFiles">Nombre del archivo</param>
        /// <returns></returns>
        public async Task<List<TrabajosCompuestosModelo>> BuscarTrabajosCompuestos(long IdTrabajo, string claveProyecto, string DesProyectotabla, string DesProyectoFiles)
        {
            var result = await EjecutarStoredProcedureQuery<TrabajosCompuestosModelo>(ProcedimientoAlmacenado.sp_ConsultarTrabajosCompuestos, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@id_trabajo", IdTrabajo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_proyecto", claveProyecto));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@des_proyecto_tabla", DesProyectotabla));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@des_proyecto_files", DesProyectoFiles));
            });

            if (result.Exito)
                return result.Data ?? new List<TrabajosCompuestosModelo>();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}");

        }

        /// <summary>
        /// Función para actualizar el estado de los procesos que no tienen asignado nombre de proyecto
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="claveProyecto">Clave del proyecto a consultar</param>
        /// <returns></returns>
        public async Task<RegistrosModelo> MarcarTerminadosSinProyecto(long IdTrabajo, string claveProyecto)
        {
            var result = await EjecutarStoredProcedureQuery<RegistrosModelo>(ProcedimientoAlmacenado.sp_MarcarTerminadosSinProyecto, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@id_trabajo", IdTrabajo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_proyecto", claveProyecto));
            });

            if (result.Exito)
                return result?.Data?.FirstOrDefault() ?? new RegistrosModelo();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}"); ;
        }

        /// <summary>
        /// Función para actualizar el estado del proyecto cuando el proceso inicia
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="CveProyecto">Clave del proyecto a consultar</param>
        /// <param name="DesProyectotabla">Tabla a consultar</param>
        /// <param name="DesProyectoFiles">Nombre del archivo</param>
        /// <returns></returns>

        public async Task<RegistrosModelo> MarcarProcesosInicio(long IdTrabajo, string CveProyecto, string DesProyectotabla, string DesProyectoFiles)
        {
            var result = await EjecutarStoredProcedureQuery<RegistrosModelo>(ProcedimientoAlmacenado.sp_MarcarInicioProcesos, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@id_trabajo", IdTrabajo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_proyecto", CveProyecto));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@des_proyecto_tabla", DesProyectotabla));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@des_proyecto_files", DesProyectoFiles));
            });

            if (result.Exito)
                return result?.Data?.FirstOrDefault() ?? new RegistrosModelo();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}"); ;
        }

        /// <summary>
        /// Función para obtener las columnas y el formato que deben llevar los datos
        /// </summary>
        /// <param name="CveProducto">Código del producto que generará el archivo</param>
        /// <param name="CveTipo">Tipo de Movimiento</param>
        /// <param name="CveSistema">Sistema</param>
        /// <param name="TablaZeus">Tabla a consultar</param>
        /// <returns></returns>
        public async Task<List<EstructuraTablaZeusModel>> ObtenerEstructuraTablaZeus(string CveProducto, string CveTipo, string CveSistema, string TablaZeus)
        {
            var result = await EjecutarStoredProcedureQuery<EstructuraTablaZeusModel>(ProcedimientoAlmacenado.sp_ObtenerEstructuraTablaZeus, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_producto", CveProducto));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_tipo", CveTipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cve_sistema", CveSistema));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@tabla_zeus", TablaZeus));
            });

            if (result.Exito)
                return result.Data ?? new List<EstructuraTablaZeusModel>();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}");
        }

        /// <summary>
        /// Función para actualizar el estado a terminado del proceso
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="CveProyecto">Clave del proyecto a consultar</param>
        /// <param name="NumReg">Numero de registros procesados</param>
        /// <param name="ZeusFileName">Nombre del Archivo</param>
        /// <param name="FileSize">Tamaño del Archivo</param>
        /// <param name="TablaZeus">Tabla con la que se trabajó</param>
        /// <param name="ArchivoOriginal">Nombre original del archivo</param>
        /// <returns></returns>
        public async Task<RegistrosModelo> MarcarTerminados(long IdTrabajo, string CveProyecto, int NumReg, string ZeusFileName, long FileSize, string TablaZeus, string ArchivoOriginal)
        {
            var result = await EjecutarStoredProcedureQuery<RegistrosModelo>(ProcedimientoAlmacenado.sp_MarcarTerminados, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idtrabajo", IdTrabajo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@numregs", NumReg));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@zeusfilename", ZeusFileName));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@filesize", FileSize));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cveproyecto", CveProyecto));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@tablazeus", TablaZeus));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@fileori", ArchivoOriginal));
            });

            if (result.Exito)
                return result?.Data?.FirstOrDefault() ?? new RegistrosModelo();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}"); ;
        }

        /// <summary>
        /// Función para obtener el Total de registros del archivo
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="ZeusFileName">Nombre del archivo</param>
        /// <returns></returns>
        public async Task<List<TrailerRecordModelo>> ObtenerDatosTrailerRecord(long IdTrabajo, string ZeusFileName)
        {
            var result = await EjecutarStoredProcedureQuery<TrailerRecordModelo>(ProcedimientoAlmacenado.sp_ObtenerDatosTrailerRecord, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idtrabajo", IdTrabajo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@zeusfilename", ZeusFileName));
            });

            if (result.Exito)
                return result.Data ?? new List<TrailerRecordModelo>();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}");
        }

        /// <summary>
        /// Función para verificar si existen procesos por ejecutar
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <returns></returns>
        public async Task<RegistrosModelo> ComprobarArchivosFaltantes(long IdTrabajo)
        {
            var result = await EjecutarStoredProcedureQuery<RegistrosModelo>(ProcedimientoAlmacenado.sp_ComprobarArchivosFaltantes, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idtrabajo", IdTrabajo));
            });

            if (result.Exito)
                return result?.Data?.FirstOrDefault() ?? new RegistrosModelo();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}"); ;
        }

        /// <summary>
        /// Función para actualizar si algún proceso tuvo errores
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="MensajeError">Mensaje de error que se guardará</param>
        /// <param name="CveProducto">Código del producto que generará el archivo</param>
        /// <param name="CveTipo">Tipo</param>
        /// <param name="CveSistema">Sistema</param>
        /// <returns></returns>
        public async Task MarcarProcesoConError(long IdTrabajo, string MensajeError, string CveProducto, string CveTipo, string CveSistema)
        {
            await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_MarcarProcesoConError, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idtrabajo", IdTrabajo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@mensajeerror", MensajeError));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cveproducto", CveProducto));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cvetipo", CveTipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@cvesistema", CveSistema));
            });
        }

        /// <summary>
        /// Función para comprobar que no existen mas archivos por procesar
        /// </summary>
        /// <returns></returns>
        public async Task<RegistrosModelo> ValidarFinCarga()
        {
            var result = await EjecutarStoredProcedureQuery<RegistrosModelo>(ProcedimientoAlmacenado.sp_ValidarFinalCarga);

            if (result.Exito)
                return result?.Data?.FirstOrDefault() ?? new RegistrosModelo();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}"); ;
        }

        /// <summary>
        /// Función para marcar el inicio de un proceso
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="NumOrden">Número de Orden de la tarea a ejecutar</param>
        /// <param name="SqlQuery">Consulta a ejecutar</param>
        /// <returns></returns>
        public async Task<bool> MarcarInicioControl(long IdTrabajo, int NumOrden, string SqlQuery)
        {
            var result = await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_MarcarInicioControl, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@idtrabajo", IdTrabajo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@numorden", NumOrden));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@sqlquery", SqlQuery));
            });

            return result.Exito ? true : throw new Exception(result.MensajeError);//Error de des_query si es que hay.
        }

        /// <summary>
        ///  Función para Actualizar el estado del Proceso si existe algún error.
        /// </summary>
        /// <param name="idTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="numeroOrden">Número de Orden de la tarea a ejecutar</param>
        /// <param name="mensajeError">El mensaje con el que se actualizará el trabajo</param>
        /// <returns></returns>
        public async Task MarcarErrorProcesoControl(long idTrabajo, int numeroOrden, string mensajeError)
        {
            try
            {
                await EjecutarStoredProcedureQuery<RegistrosModelo>(ProcedimientoAlmacenado.sp_MarcarErrorControl, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@id_trabajo", idTrabajo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@num_orden", numeroOrden));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@error_descripcion", mensajeError));
                });

            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Se presentó una excepcion al guardar el error. {ex}");
            }
        }

        /// <summary>
        /// Función para marcar las tareas de sentencias que no se ejecutaron y tienen error
        /// </summary>
        /// <param name="IdTrabajo">Identificador del trabajo ejecutado</param>
        /// <param name="NumOrden">Número de Orden de la tarea a ejecutar</param>
        /// <param name="MensajeError">Mensaje de error</param>
        /// <returns></returns>
        public async Task MarcarAjusteConError(long IdTrabajo, int NumOrden, string MensajeError)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(ProcedimientoAlmacenado.sp_MarcarAjusteConError, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@idtrabajo", IdTrabajo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@numorden", NumOrden));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@mensajeerror", MensajeError));
                });
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Se presentó una excepción en al Marcar Ajuste con Error, ex: {ex}");
            }
        }

        /// <summary>
        /// Procesa y configura un script, ademas de ejecutarlo
        /// Devuelve un objeto de tipo DatosArchivoModelo
        /// </summary>
        /// <param name="claveProducto"></param>
        /// <param name="claveTipo"></param>
        /// <param name="claveSistema"></param>
        /// <param name="tablaZeus"></param>
        /// <param name="idTrabajo"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public async Task<List<DatosArchivoModelo>> ProcesoDatosArchivoModelo(string claveProducto, string claveTipo, string claveSistema, string tablaZeus, long idTrabajo)
        {
            var result = await EjecutarStoredProcedureQuery<DatosArchivoModelo>(ProcedimientoAlmacenado.sp_ProcesoDatosArchivoModelo, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pClaveProducto", claveProducto));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pCLaveTipo", claveTipo));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pClaveSistema", claveSistema));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pTablaZeus", tablaZeus));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@pIdTrabajo", idTrabajo));
            });

            if (result.Exito)
                return result.Data ?? new List<DatosArchivoModelo>();
            else
                throw new InvalidOperationException($"El procedimiento almacenado presentó el siguiente problema: {result.MensajeError}");
        }

        #endregion
    }
}

using AplicacionZeus_Files;
using AplicacionZeus_Files.Interfaces;
using AplicacionZeus_Files.Modelos;
using AplicacionZeus_Files.Operaciones;
using InfraestructuraZeus_Files.Repositorios;
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
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Zeus_Files
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        #region Propiedades

        private Container _containerDI;
        private Configuracion _configuracionBase;
        private ConfiguracionModelo _configuracionModelo;

        protected override string NombreServicioLog => NombreServicio.Zeus_Files.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 5000;

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor 
        /// </summary>|
        public ServiceHost() { }

        #endregion

        #region Procesos

        /// <summary>
        /// Realiza la inyección y establece un timer
        /// que ejecutará su delegado por tick del intervalo.
        /// </summary>
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
            _configuracionModelo = new ConfiguracionModelo();

            //Inicializa la obtención de la configuracion general y el logging...
            {
                ConfiguracionEncriptadaHelper.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Zeus_Files.ToString());
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

            //Registra la configuración...
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<ConfiguracionModelo>(() => _configuracionModelo.Clone(), Lifestyle.Scoped);
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);

            //Registra los componentes
            _containerDI.Register<IOperacionesDWH, DWHRepository>();
            _containerDI.Register<Productos>();
            _containerDI.Register<Archivos>();
            _containerDI.Register<OperacionServicio>();

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

                if (!_configuracionBase.Pid.HasValue)
                {
                    throw new InvalidOperationException(
                        "No fue posible obtener el PID del proceso.");
                }

                //Marca el servicio en uso.
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
                await _controlServicio.MarcarPuertoEnUsoAsync(
                    _configuracionBase.Pid.Value, _configuracionBase.NombreProceso,
                    _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio,
                    cancellationToken);

                //Parametrizacion de la configuracion local
                {
                    _configuracionModelo.NombreServicio = _configuracionBase.NombreServicio;
                    _configuracionModelo.TimerInterval = _configuracionBase.TimerInterval;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|Parametrizaciones|Error: {ex}");
                throw;
            }
        }

        /// <summary>
        /// Ejecuta el funcionamiento principal del servicio en cada tick.
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
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

        #endregion
    }
}

namespace Zeus_Files
{
    partial class ServiceHost {}
}

using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;

namespace Zeus_Files
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
                NombreServicio.Zeus_Files.ToString(),
                    () => new ServiceHost(), args
#if DEBUG
                        , (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs)
#endif
            );
        }
    }
}

Zeus_Files/App.config
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
    <startup> 
        <supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.8" />
    </startup>
</configuration>

