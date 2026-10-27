using AplicacionSwfOUTMQGCMS.Enums;
using AplicacionSwfOUTMQGCMS.Interfaces;
using AplicacionSwfOUTMQGCMS.Modelos;
using IBM.WMQ;
using InfraestructuraSwfOUTMQGCMS.Messaging;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using System.Threading;

namespace InfraestructuraSwfOUTMQGCMS.Repositorio
{
    /// <summary>
    /// Repositorio InterfacesT24 (TOMI) y VistasT24
    /// </summary>
    public class RepositorioTomi : RepositorioMaestro, IRepositorioTomi<OperacionSWF>
    {
        private readonly Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public RepositorioTomi(Configuracion configuracion) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _configuracion = configuracion;
        }


        /// <summary>
        /// Envía una colección de operaciones al sistema IBM MQ, registrando cada mensaje en la cola configurada.
        /// </summary>
        /// <param name="operaciones"></param>
        /// <returns></returns>
        public async Task<bool> GuardaMensajesOutMQ(IEnumerable<OperacionSWF> operaciones, CancellationToken ct)
        {
            if (operaciones == null || !operaciones.Any())
            {
                LogWarning(ConstantesServicio.MensajeOperaciones);
                return false;
            }

            ConfiguracionMQ config = null;
            MQQueueManager queueManager = null;
            MQQueue queue = null;
            try
            {
                config = ObtenerConfiguracionMQ();
                ValidarConfiguracionMq(config);

                int contador = 0;
                var listaOperaciones = operaciones.ToList();
                int total = listaOperaciones.Count;
                var connectionProperties = CrearPropiedadesConexion(config);

                LogInfo(ConstantesServicio.MensajeEstabeciendoCnx);
                ct.ThrowIfCancellationRequested();
                queueManager = new MQQueueManager(config.QueueManagerName, connectionProperties);

                if (!queueManager.IsConnected)
                {
                    LogError(ConstantesServicio.MensajeConexiónFallida);
                    return false;
                }

                queue = queueManager.AccessQueue(config.QueueName, MQC.MQOO_OUTPUT);

                LogInfo(ConstantesServicio.MensajeConexionMQExitosa);

                int exitosas = 0;
                int rechazadas = 0;
                int fallidas = 0;
                ct.ThrowIfCancellationRequested();

                foreach (var operacion in listaOperaciones)
                {
                    ct.ThrowIfCancellationRequested();
                    contador++;

                    var resultado =
                        await ProcesarOperacionAsync(operacion, queue, ct);

                    switch (resultado)
                    {
                        case ResultadoOperacionMq.Exitosa:
                            exitosas++;

                            LogInfo($"Procesado {contador} de {total} - Folio: {operacion.Folio}");

                            break;

                        case ResultadoOperacionMq.Rechazada:
                            rechazadas++;
                            break;

                        case ResultadoOperacionMq.Fallida:
                            fallidas++;
                            break;
                    }
                }

                LogInfo($"Resumen procesamiento | Exitosas={exitosas} | Rechazadas={rechazadas} | Fallidas={fallidas}");
                return fallidas == 0 && rechazadas == 0; //Antes regresaba true directo.
            }
            catch (MQException ex)
            {
                string errorMsg = MQErrorHandler.GetMQErrorDescription(ex.ReasonCode);
                LogError($"MQException: Reason={ex.Reason}, Message={ex.Message}, Error MQ={errorMsg}");
                return false;
            }
            catch (Exception ex)
            {
                LogError($"Error general: {ex.Message} | HResult={ex.HResult}");
                if (config != null)
                {
                    await NotificarErrorAsync(config, ex).ConfigureAwait(false);
                }
                return false;
            }
            finally
            {
                try
                {
                    queue?.Close();
                }
                catch (Exception ex)
                {
                    LogError($"Error al cerrar cola MQ: {ex.Message}");
                }

                try
                {
                    queueManager?.Disconnect();
                }
                catch (Exception ex)
                {
                    LogError($"Error al deesconectar MQ: {ex.Message}");
                }
            }
        }

        #region Configuración
        /// <summary>
        /// Obtiene configuracion de conexión hacia MQ
        /// </summary>
        /// <returns></returns>
        private ConfiguracionMQ ObtenerConfiguracionMQ()
        {
            return new ConfiguracionMQ
            {
                QueueManagerName = ObtenerValorObligatorio(ConstantesServicio.ManagerName),
                QueueName = ObtenerValorObligatorio(ConstantesServicio.QueueName),
                User = ObtenerValorObligatorio(ConstantesServicio.UserId),
                Password = ObtenerValorObligatorio(ConstantesServicio.Password),
                Ssl = ObtenerValorObligatorio(ConstantesServicio.SslCipherSpec),
                SslPath = ObtenerValorObligatorio(ConstantesServicio.SslKeyRepository),
                Host = ObtenerValorObligatorio(ConstantesServicio.Host),
                Port = ObtenerEnteroObligatorio(ConstantesServicio.Port),
                ChannelName = ObtenerValorObligatorio(ConstantesServicio.Channel),
                Cert = ObtenerValorObligatorio(ConstantesServicio.CertificateLabel),
                MailTo = ObtenerValorObligatorio(ConstantesServicio.EMailDefault)
            };
        }

        /// <summary>
        /// Obtiene y valida un parametro de configuración obligatorio de tipo entero.
        /// </summary>
        /// <param name="clave"></param>
        /// <returns></returns>
        /// <exception cref="ConfigurationErrorsException"></exception>
        private int ObtenerEnteroObligatorio(string clave)
        {
            string valor = ObtenerValor(clave, x => x);
            if (!int.TryParse(valor, out int resultado))
            {
                throw new ConfigurationErrorsException($"La configuración [{ConstantesServicio.Port}] es obligatoria y debe de ser númerica.");
            }
            return resultado;
        }

        /// <summary>
        /// Crea las propiedades conexion hacia MQ
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        private Hashtable CrearPropiedadesConexion(ConfiguracionMQ config)
        {
            return new Hashtable
            {
                { MQC.HOST_NAME_PROPERTY, config.Host },
                { MQC.PORT_PROPERTY, config.Port },
                { MQC.CHANNEL_PROPERTY, config.ChannelName },

                { MQC.USER_ID_PROPERTY,  config.User},
                { MQC.PASSWORD_PROPERTY, config.Password},

                { MQC.SSL_CIPHER_SPEC_PROPERTY, config.Ssl},
                { MQC.SSL_CERT_STORE_PROPERTY, config.SslPath},

                { ConstantesServicio.CerticateLabel, config.Cert }
            };
        }

        #endregion

        #region Procesamiento

        /// <summary>
        /// Metodo para procesar cada una de las operaciones
        /// </summary>
        /// <param name="operacion"></param>
        /// <param name="queue"></param>
        /// <returns></returns>
        private async Task<ResultadoOperacionMq> ProcesarOperacionAsync(OperacionSWF operacion, MQQueue queue, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            if (operacion == null)
            {
                LogError("La operación recibida es nula.");
                return ResultadoOperacionMq.Rechazada;
            }

            if (queue == null)
            {
                LogError("La cola MQ no fue inicializada.");
                return ResultadoOperacionMq.Fallida;
            }

            var folio = operacion.Folio;
            var mensaje = operacion.Mensaje;

            if (string.IsNullOrEmpty(folio))
            {
                LogError("La operación no contiene un folio valido.");
                return ResultadoOperacionMq.Rechazada;
            }

            if (string.IsNullOrEmpty(mensaje))
            {
                LogError($"La operación con folio[{folio}] no contiene mensaje.");
                return ResultadoOperacionMq.Rechazada;
            }

            try
            {
                LogInfo($"Procesando folio: {folio}");

                var mqMessage = CrearMensaje(mensaje);

                LogInfo($"MessageLength={mqMessage.MessageLength}, StringLength={mensaje.Length}");

                var actualizado = await ActualizaEstatusMensajeOutMQAsync(folio, ConstantesServicio.EstatusSndng, ct).ConfigureAwait(false);

                if (!actualizado)
                {
                    LogError($"No se pudo actualizar estatus previo. Folio={folio}");
                    return ResultadoOperacionMq.Fallida;
                }

                LogInfo("Enviando mensaje a MQ...");
                ct.ThrowIfCancellationRequested();

                queue.Put(mqMessage, new MQPutMessageOptions());

                var procesado = await ActualizaEstatusProcesoSWFOutMqAsync(folio, ct).ConfigureAwait(false);

                if (!procesado)
                {

                    LogError($"Mensaje enviado a MQ pero no se actualizo en BD. Folio={folio}|Favor de actualizar manualmente.");

                    throw new Exception($"Mensaje enviado a MQ pero no se actualizo en BD. Folio={folio}|Favor de actualizar manualmente.");

                }

                return ResultadoOperacionMq.Exitosa;
            }
            catch (MQException ex)
            {
                LogError($"Error MQ en PUT | Folio={folio} | Reason={ex.Reason} | {ex.Message}");

                await ActualizaEstatusMensajeOutMQAsync(folio, ConstantesServicio.EstatusError, ct).ConfigureAwait(false);

                return ResultadoOperacionMq.Fallida;
            }
            catch (Exception ex)
            {
                LogError($"Error general | Folio={folio} | {ex.Message}");
                return ResultadoOperacionMq.Fallida;
            }
        }
        /// <summary>
        /// Metodo para crear Mensaje MQ.
        /// </summary>
        /// <param name="mensaje"></param>
        /// <returns></returns>
        private MQMessage CrearMensaje(string mensaje)
        {
            var mqMessage = new MQMessage();
            mqMessage.Format = MQC.MQFMT_STRING;
            mqMessage.CharacterSet = 1208; // UTF-8
            mqMessage.WriteString(mensaje);
            return mqMessage;
        }

        #endregion

        #region Notificación
        /// <summary>
        /// Metodo para crear Notificaciones.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        private async Task NotificarErrorAsync(ConfiguracionMQ config, Exception ex)
        {
            var destinatario = ObtenerValorConfig(_configuracion.CorreoParaDefault, config.MailTo);
            var asunto = string.IsNullOrEmpty(_configuracion.SujetoDefault)
                ? ConstantesServicio.Asunto
                : $"{ConstantesServicio.Asunto}[{_configuracion.SujetoDefault}]";


            var resultado = await CorreoHelper.EnviarCorreoAsync(new MensajeCorreo
            {
                CorreoOrigen = _configuracion.CorreoSenderTest,
                NombreOrigen = ConstantesServicio.NombreOrigen,
                Para = destinatario,
                Asunto = asunto,
                Mensaje = $"{ConstantesServicio.Mensaje} Error={ex.Message}"
            }).ConfigureAwait(false);
            if (!resultado)
            {
                Config.EscribirLog(ConstantesServicio.MensajeErrorEnvioCorreo);
            }
        }

        #endregion

        #region Logging
        /// <summary>
        /// Método para registro de Log de tipo INFO.
        /// </summary>
        /// <param name="msg"></param>
        private void LogInfo(string msg) =>
            Config.EscribirLog($"{ConstantesServicio.MensajeInfo}|{ConstantesServicio.MensajeGuardar}|{msg}");
        /// <summary>
        /// Método para registro de Log de tipo WARN.
        /// </summary>
        /// <param name="msg"></param>
        private void LogWarning(string msg) =>
            Config.EscribirLog($"{ConstantesServicio.MensajeWARN}|{ConstantesServicio.MensajeGuardar}|{msg}");
        /// <summary>
        /// Método para registro de Log de tipo ERROR.
        /// </summary>
        /// <param name="msg"></param>
        private void LogError(string msg) =>
            Config.EscribirLog($"{ConstantesClaveAviso.Error}|{ConstantesServicio.MensajeGuardar}|{msg}");

        #endregion

        /// <summary>
        /// Recupera los registros de operaciones SWF desde la base de datos
        /// mediante la ejecución del procedimiento almacenado correspondiente.
        /// </summary>
        /// <remarks>
        /// Este método consulta las operaciones disponibles para su procesamiento
        /// dentro del flujo SWIFT OUT MQ. No recibe parámetros de entrada y
        /// retorna una colección de solo lectura.
        /// </remarks>
        /// <returns>
        /// Colección de tipo <see cref="OperacionSWF"/> con los registros obtenidos.
        /// </returns>
        public async Task<IEnumerable<OperacionSWF>> RecuperaOperacionesAsync(CancellationToken ct)
        {

            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarReader<OperacionSWF>(ConstantesServicio.UspObtenerOperacionesSwfOutMq).ConfigureAwait(false);
            return resultado.Data;

        }


        /// <summary>
        /// Actualiza el estatus de envío de un mensaje SWIFT OUT en el sistema.
        /// </summary>
        /// <remarks>
        /// Este método ejecuta un procedimiento almacenado que actualiza el estatus
        /// de la operación en las tablas de control correspondientes dentro del flujo SWF OUT MQ.
        /// </remarks>
        public async Task<bool> ActualizaEstatusMensajeOutMQAsync(string Folio, string Estatus, CancellationToken ct)
        {
            
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProceduredNonQuery(
            ConstantesServicio.UspActualizaEstatusEnvioMensajeSwfOutMq,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", Folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", ConstantesServicio.TipoOperacion));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@ClaveEstatus", Estatus));
            }).ConfigureAwait(false);
            if (!resultado.Exito)
            {
                throw new InvalidOperationException($"Error al Actualizar Estatus Mensaje Out MQ: {resultado.MensajeError}|Folio={Folio}|Estatus={Estatus}");
            }
            return resultado.Exito;
            
        }


        /// <summary>
        /// Actualiza el estatus del proceso SWF OUT en base de datos.
        /// </summary>
        /// <returns>True si la ejecución es exitosa.</returns>

        public async Task<bool> ActualizaEstatusProcesoSWFOutMqAsync(string Folio, CancellationToken ct)
        {
            
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProceduredNonQuery(
            ConstantesServicio.UspActualizaEstatusProcesoSwfOutMq,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", Folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", ConstantesServicio.TipoOperacion));
            }).ConfigureAwait(false);
            if (!resultado.Exito)
            {
                throw new InvalidOperationException($"Error al Actualizar Estatus Proceso Out MQ: {resultado.MensajeError}|Folio={Folio}");
            }
            return resultado.Exito;
            
        }

        /// <summary>
        /// Metodo para obtener valor de parametros de configuracion de MQ.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="clave"></param>
        /// <param name="parser"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        T ObtenerValor<T>(string clave, Func<string, T> parser)
        {
            if (_configuracion.ParametrosSistema.TryGetValue(clave, out var valor))
                return parser(valor);
            var mensaje = $"{ConstantesServicio.MensajeInfo}|{ConstantesServicio.MensajeGuardar}|Clave de parametro de configuración Faltante.|No se recupera de BD: {clave}";
            Config.EscribirLog(mensaje);
            throw new ConfigurationErrorsException($"{mensaje}: {clave}");

        }

        /// <summary>
        /// Metodo para obtener valor de parametro
        /// </summary>
        /// <param name="valorConfig"></param>
        /// <param name="valorDefault"></param>
        /// <returns></returns>
        string ObtenerValorConfig(string valorConfig, string valorDefault)
        {
            return string.IsNullOrEmpty(valorConfig) ? valorDefault : valorConfig;
        }

        /// <summary>
        /// Obtiene y valida un parámetro obligatorio.
        /// </summary>
        /// <param name="clave"></param>
        /// <returns></returns>
        /// <exception cref="ConfigurationErrorsException"></exception>
        private string ObtenerValorObligatorio(string clave)
        {
            string valor = ObtenerValor(clave, x => x);

            if (string.IsNullOrWhiteSpace(valor))
            {
                throw new ConfigurationErrorsException($"La configuración [{clave}] es obligatoria.");
            }

            return valor;
        }

        /// <summary>
        /// Validacion de Configuracion MQ.
        /// </summary>
        /// <param name="config"></param>
        /// <exception cref="ConfigurationErrorsException"></exception>
        private void ValidarConfiguracionMq(ConfiguracionMQ config)
        {
            if (config == null)
            {
                throw new ConfigurationErrorsException(
                    "La configuración MQ no fue inicializada.");
            }

            if (string.IsNullOrWhiteSpace(config.Ssl))
            {
                throw new ConfigurationErrorsException(
                    "SSL_CIPHERSPEC es obligatorio.");
            }

            if (string.IsNullOrWhiteSpace(config.SslPath))
            {
                throw new ConfigurationErrorsException(
                    "SSL_KEYREPOSITORY es obligatorio.");
            }

            if (string.IsNullOrWhiteSpace(config.Cert))
            {
                throw new ConfigurationErrorsException(
                    "CERTIFICATE_LABEL es obligatorio.");
            }

            if (!Directory.Exists(config.SslPath))
            {
                throw new ConfigurationErrorsException(
                    $"El repositorio SSL no existe: {config.SslPath}");
            }
        }

    }
}


using AplicacionSwfOUTMQGCMS.Enums;

namespace InfraestructuraSwfOUTMQGCMS.Messaging
{
    public class MQErrorHandler
    {
        /// <summary>
        /// Catálogo de mensajes detallados, relacionados con los codigos de error del servidor MQ.
        /// </summary>
        /// <param name="reasonCode"></param>
        /// <returns></returns>
        public static string GetMQErrorDescription(int reasonCode)
        {
            switch (reasonCode)
            {
                case 2033:
                    return ConstantesServicio.Codigo2033;

                case 2035:
                    return ConstantesServicio.Codigo2035;

                case 2009:
                    return ConstantesServicio.Codigo2009;

                case 2059:
                    return ConstantesServicio.Codigo2059;

                case 2085:
                    return ConstantesServicio.Codigo2085;

                case 2063:
                    return ConstantesServicio.Codigo2063;

                case 2381:
                    return ConstantesServicio.Codigo2381;

                case 2393:
                    return ConstantesServicio.Codigo2393;

                case 2400:
                    return ConstantesServicio.Codigo2400;

                case 2538:
                    return ConstantesServicio.Codigo2538;

                case 2540:
                    return ConstantesServicio.Codigo2540;

                default:
                    return $"{ConstantesServicio.CodigoDefault} {reasonCode}";
            }
        }
    }
}
