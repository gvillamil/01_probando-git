using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Operacion principal del sistema.
    /// </summary>
    public class OperacionServicio : IOperacionServicio
    {
        // Sistema Interfaces.
        private readonly ConfiguracionServicio _configuracion;


        // Laika Interfaces.
        private readonly IParametrosOperaciones _IparametrosOperaciones;
        private readonly IOperacionRepositorioLaikaQueue _IoperacionRepositorioLaikaQueue;
        private readonly ISwiftAutomatizacionFactory _swiftAutomatizacionFactory;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="configuracion"></param>
        /// <param name="iparametrosOperaciones"></param>
        /// <param name="ioperacionRepositorioLaikaQueue"></param>
        /// <param name="swiftAutomatizacionFactory"></param>
        public OperacionServicio(ConfiguracionServicio configuracion, IParametrosOperaciones iparametrosOperaciones, IOperacionRepositorioLaikaQueue ioperacionRepositorioLaikaQueue, ISwiftAutomatizacionFactory swiftAutomatizacionFactory)
        {
            _configuracion = configuracion;
            _IparametrosOperaciones = iparametrosOperaciones;
            _IoperacionRepositorioLaikaQueue = ioperacionRepositorioLaikaQueue;
            _swiftAutomatizacionFactory = swiftAutomatizacionFactory;
        }

        /// <summary>
        /// Procedimiento principal para el procesamiento de las operaciones.
        /// </summary>
        public async Task EjecutaProcedimientosAsync()
        {

            try
            {
                //Ejecucion Proceso Laika
                var foliosPendientes = await _IoperacionRepositorioLaikaQueue.QueueTodosLosFolio() ?? Enumerable.Empty<string>(); //Obtiene y elimina los queue pendientes.

                foreach (var folio in foliosPendientes)
                {
                    try
                    {
                        Config.EscribirLog($"INFO|EjecutaProcedimientosAsync| Inicia procesamiento del folio: '{folio}'", NombreServicio.LAIKA.ToString());

                        //Crear y procesar
                        var resultado = await _swiftAutomatizacionFactory.CrearYProcesar(folio);

                        if (resultado == null)
                        {
                            Config.EscribirLog(
                                $"ERROR|EjecutaProcedimientosAsync|Sin resultado para el '{folio}", NombreServicio.LAIKA.ToString());
                            continue;
                        }

                        if (!resultado.Procesado)
                        {
                            Config.EscribirLog(
                                $"INFO|EjecutaProcedimientosAsync| El Folio: '{folio}' no se proceso correctamente. Detalle: {resultado.Mensaje}", NombreServicio.LAIKA.ToString());
                            continue;
                        }

                        Config.EscribirLog($"INFO|EjecutaProcedimientosAsync| El Folio: '{folio}' se proceso correctamente.", NombreServicio.LAIKA.ToString());

                    }
                    catch (Exception ex)
                    {
                        Config.EscribirLog($"ERROR|EjecutaProcedimientosAsync| Error procesando el folio: '{folio}': {ex}", NombreServicio.LAIKA.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|EjecutaProcedimientosAsync|Se presenta exception: " + ex.Message, NombreServicio.LAIKA.ToString());

                await _IparametrosOperaciones.EnviaAlerta(new AlertaDTO
                {
                    TipoOPeracion = null,
                    Folio = null,
                    Servicio = _configuracion.NombreServicio,
                    ClaveAviso = "CRITICO",
                    Descipcion = "Error en ejecuta_procedimientos",
                    PId = (int)_configuracion.Pid,
                    NombreProceso = _configuracion.NombreProceso,
                    Exception = ex
                });

                Config.EscribirLog("ERROR|EjecutaProcedimientosAsync|Se reinicia el servicio.", NombreServicio.LAIKA.ToString());
            }
        }
    }
}


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Operacion servicio.
    /// </summary>
    public interface IOperacionServicio
    {
        Task EjecutaProcedimientosAsync();
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Operacion repositorio laika.
    /// </summary>
    public interface IOperacionRepositorioLaikaQueue
    {
        Task<string> QueueUnFolio();
        Task<List<string>> QueueTodosLosFolio();
    }
}


sing Aplicacion.DTOs;
using Aplicacion.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interface Automatizacion factory.
    /// </summary>
    public interface ISwiftAutomatizacionFactory
    {
        Task<DMensajeLlave> ObtenerMensajeLlave(string icn);
        IOperacionRepositorioSwift ProcesadorDesdeLlave(DMensajeLlave mensajeLlave);
        Task<ResultadoProceso> CrearYProcesar(string icn);
    }
}


﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Resultado Proceso.
    /// </summary>
    public class ResultadoProceso
    {
        public bool Procesado {  get; set; }
        public bool AutorizadoAutomaticamente {  get; set; }
        public string Mensaje {  get; set; }
        public string Folio {  get; set; }
        public string Flujo {  get; set; }
    }
}


using Aplicacion.DTOs;
using System;

namespace Aplicacion.Enums
{
    /// <summary>
    /// ENUM Asignacion del tipo de mensaje.
    /// </summary>
    public static class SwiftMensajeResolvedor
    {
        public static ETipoMensajeSwift ParseTipoMensaje(DMensajeLlave llave)
        {
            if(llave == null || string.IsNullOrWhiteSpace(llave.MessageType))
            {
                return ETipoMensajeSwift.UNKNOWN;
            }

            return llave.MessageType.StartsWith("PACS", 
                System.StringComparison.OrdinalIgnoreCase) 
                ? ETipoMensajeSwift.MX 
                : ETipoMensajeSwift.MT;
        }

        public static TipoOperacionSwift ParseTipoOperacion(string valor)
        {
            if (string.IsNullOrWhiteSpace(valor)) 
                return TipoOperacionSwift.UNKNOWN;

            if (valor.Contains("OUT")) 
                return TipoOperacionSwift.OUT;

            if(valor.Contains("IN")) 
                return TipoOperacionSwift.IN;

            return TipoOperacionSwift.UNKNOWN;
            
        }

    }
}


using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Aplicacion.Helper;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Swift automatizacion factory.
    /// </summary>
    public class SwiftAutomatizacionFactory : RepositorioMaestro, ISwiftAutomatizacionFactory
    {

        private readonly IOperacionRepositorioMt103In _mt103InProceso;
        private readonly IOperacionRepositorioMt103Out _mt103OutProceso;
        private readonly IOperacionRepositorioMt202In _mt202InProceso;
        private readonly IOperacionRepositorioMt202CovIn _mt202CovInProceso;
        private readonly IOperacionRepositorioPacs008In _pacs008InProceso;
        private readonly IOperacionRepositorioSwiftNoSoportado _procesoNoSoportado;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public SwiftAutomatizacionFactory(
            IOperacionRepositorioMt103In mt103InProceso,
            IOperacionRepositorioMt103Out mt103OutProceso,
            IOperacionRepositorioMt202In mt202InProceso,
            IOperacionRepositorioMt202CovIn mt202CovInProceso,
            IOperacionRepositorioPacs008In pacs008InProceso,
            IOperacionRepositorioSwiftNoSoportado procesoNoSoportado) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _mt103InProceso = mt103InProceso;
            _mt103OutProceso = mt103OutProceso;
            _mt202InProceso = mt202InProceso;
            _mt202CovInProceso = mt202CovInProceso;
            _pacs008InProceso = pacs008InProceso;
            _procesoNoSoportado = procesoNoSoportado;
        }


        /// <summary>
        /// obtencion de mensaje llave.
        /// </summary>
        /// <param name="icn"></param>
        /// <returns></returns>
        public async Task<DMensajeLlave> ObtenerMensajeLlave(string icn)
        {
            try
            {
                var resultdato = await EjecutarStoredProcedureQuery<DMensajeLlave>(ConstantesServicio.usp_ObtenerSwiftMessageKeyPorIcn, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@icn", icn));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@buscarPorTransaccionRef", icn != null && icn.StartsWith("FT", StringComparison.OrdinalIgnoreCase)));

                });

                return resultdato?.Data?.FirstOrDefault();
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ObtenerMensajeLlave));
                throw;
            }
        }

        /// <summary>
        ///  Se mandan a llamar las operaciones Repositorios.
        /// </summary>
        /// <param name="llave"></param>
        /// <returns></returns>
        public IOperacionRepositorioSwift ProcesadorDesdeLlave(DMensajeLlave llave)
        {
            try
            {
                var tipoMensaje = SwiftMensajeResolvedor.ParseTipoMensaje(llave);
                var tipoOperacion = SwiftMensajeResolvedor.ParseTipoOperacion(llave?.OperationType);
                var flujo = SwiftFlujoResolvedor.Resolvedor(tipoMensaje, tipoOperacion, llave.MessageType); // regresa el tipo y la funcion del flujo a procesar

                //Aqui se ejecutan las funciones de los flujos.
                switch (flujo)
                {
                    case EFlujoTipoSwift.Mt103In:
                        return _mt103InProceso;

                    case EFlujoTipoSwift.Mt103Out:
                        return _mt103OutProceso;

                    case EFlujoTipoSwift.Mt202In:
                        return _mt202InProceso;

                    case EFlujoTipoSwift.Mt202CovIn:
                        return _mt202CovInProceso;

                    case EFlujoTipoSwift.Pacs008In:
                        return _pacs008InProceso;

                    default:
                        return _procesoNoSoportado;
                }
                ;
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesadorDesdeLlave));
                throw;
            }
        }

        /// <summary>
        /// Obtiene la información necesaria para saber si es proceso soportado o no.
        /// </summary>
        /// <param name="icn"></param>
        /// <returns></returns>
        public async Task<ResultadoProceso> CrearYProcesar(string icn)
        {
            try
            {
                var mensajeLlave = await ObtenerMensajeLlave(icn);

                if (mensajeLlave == null)
                {
                    return new ResultadoProceso
                    {
                        Procesado = false,
                        Mensaje = "No existe configuración de llave para el mensaje"
                    };
                }

                var proceso = ProcesadorDesdeLlave(mensajeLlave);
                return await proceso.Procesar(icn);
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(CrearYProcesar));
                throw;
            }
        }
    }
}


using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operacion Repositorio laika queue.
    /// </summary>
    public class OperacionRepositorioLaikaQueue : RepositorioMaestro, IOperacionRepositorioLaikaQueue
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioLaikaQueue() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Regresa todos los Queue pendientes de la base de datos.
        /// </summary>
        /// <returns>List<string></returns>
        public async Task<List<string>> QueueTodosLosFolio()
        {
            try
            {
                var strFolios = new List<string>();

                do // se ejecuta si sigue encontrando folios pendientes.
                {
                    var strFolio = await QueueUnFolio();

                    if (string.IsNullOrWhiteSpace(strFolio))
                        break;

                    strFolios.Add(strFolio);
                } while (true);

                return strFolios;
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex, nameof(QueueTodosLosFolio));
                throw;
            }
        }

        /// <summary>
        /// Regresa un solo Queue pendiente de la base de datos.
        /// </summary>
        /// <returns>string</returns>
        public async Task<string> QueueUnFolio()
        {
            try
            {
                var strQueueTesult =
                    await EjecutarStoredProcedureQuery<SQueue>(ConstantesServicio.usp_dequeueLaikaHeap);

                return strQueueTesult?.Data?.FirstOrDefault()?.icn;
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex, nameof(QueueUnFolio));
                throw;
            }
        }
    }
}


using Aplicacion.Interfaces;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// state.
    /// </summary>
    public abstract class State
    {
        protected readonly bool Automatico;
        protected ISwiftContext Context;
        protected IPlanTrabajoMaestro PlanTrabajoMaestro;
        protected IPlanTrabajoMaestroDetalle PlanTrabajoMaestroDetalle;

        /// <summary>
        /// verifica que tipo de plan es.
        /// </summary>
        /// <param name="automatic"></param>
        /// <param name="planTrabajoMaestro"></param>
        /// <param name="planTrabajoMaestroDetalle"></param>
        protected State(
            bool automatic,
            IPlanTrabajoMaestro planTrabajoMaestro = null,
            IPlanTrabajoMaestroDetalle planTrabajoMaestroDetalle = null
        )
        {
            Automatico = automatic;
            PlanTrabajoMaestro = planTrabajoMaestro ?? new PlanTrabajoMaestro();
            PlanTrabajoMaestroDetalle = planTrabajoMaestroDetalle ?? new PlanTrabajoMaestroDetalle();
        }

        /// <summary>
        /// Pasa el contexto al siguiente paso.
        /// </summary>
        /// <param name="context"></param>
        public void DarContexto(ISwiftContext context )
        {
            Context = context;
        }

        /// <summary>
        /// Asignacion del siguiente paso.
        /// </summary>
        /// <returns></returns>
        public abstract Task<bool> Handle();

        /// <summary>
        /// verifica el estado.
        /// </summary>
        /// <returns></returns>
        protected abstract Task<bool> VamosYValidamosElEstado();
    }
}


using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operaciones Generales laika.
    /// </summary>
    public class OperacionesGeneralesLaika : RepositorioMaestro, IOperacionesGeneralesLaika
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public OperacionesGeneralesLaika() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Indica la ejecucion de pasos mediante AssignacionGeneral.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="automatico"></param>
        /// <returns>bool</returns>
        public async Task<bool> EjecutarPipelinesEstados(string folio, bool automatico)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(folio)) return false;

                var contexto = new SwiftContext(
                    folio,
                    new AsignacionEstadoGeneral(automatico));

                return await contexto.RequestActualizaPLan();
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex, nameof(EjecutarPipelinesEstados));
                throw;
            }
        }
    }
}


using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Modelos;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// clase de Asignacion estado general.
    /// </summary>
    public class AsignacionEstadoGeneral : State
    {

        private string _folio = string.Empty;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="automatic"></param>
        /// <param name="planTrabajoMaestro"></param>
        /// <param name="planTrabajoMaestroDetalle"></param>
        public AsignacionEstadoGeneral(
            bool automatic,
            IPlanTrabajoMaestro planTrabajoMaestro = null,
            IPlanTrabajoMaestroDetalle planTrabajoMaestroDetalle = null) : base(automatic, planTrabajoMaestro, planTrabajoMaestroDetalle)
        {

        }

        /// <summary>
        /// Asignacion del siguiente paso.
        /// </summary>
        /// <returns></returns>
        public override async Task<bool> Handle()
        {
            try
            {
                _folio = Context.ObtenerFolioActual();

                var validarState = await VamosYValidamosElEstado();
                if (!validarState)
                {
                    return false;
                }

                if (!Automatico)
                {
                    return true;
                }

                await PlanTrabajoMaestro.ActualizamosElEstatusParaOperacion(
                    _folio,
                    EPlanTrabajoMaestroEstatus.X_ASR,
                    EPlanTrabajoMaestroEstatus.X_AUT,
                    "TOMI");

                var siguienteDetalleState = DetallePlan.ObtenerSiguienteEstadoDelPlan(EDetallePlan.ESPERANDO_ASIGNACION);

                await PlanTrabajoMaestroDetalle.ActualizarEstatusOperacion(
                    _folio,
                    (int)EDetallePlan.ESPERANDO_ASIGNACION,
                    (int)siguienteDetalleState);


                Context.TransitionTo(new AutorizacionEstadoGeneral(Automatico, PlanTrabajoMaestro, PlanTrabajoMaestroDetalle));
                return true;
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex, nameof(Handle));
                throw;
            }
        }

        /// <summary>
        /// Validamos el estado en que se encuentra el folio de la Queue.
        /// </summary>
        /// <returns></returns>
        protected override async Task<bool> VamosYValidamosElEstado()
        {
            try
            {
                var actualPlanTrabajoMaestroState = await PlanTrabajoMaestro.ObtenerEstatusActualParaOperacion(_folio);
                var actualPlanTrabajoMaestroDetalleState = await PlanTrabajoMaestroDetalle.ObtenerEstadoActualDeLaOperacion(_folio);
                var detalleActualState = (EDetallePlan)actualPlanTrabajoMaestroDetalleState;

                return actualPlanTrabajoMaestroState == EPlanTrabajoMaestroEstatus.X_ASR && detalleActualState == EDetallePlan.ESPERANDO_ASIGNACION;
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex, nameof(VamosYValidamosElEstado));
                throw;
            }

        }
    }
}


using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase Autorizacion estado general.
    /// </summary>
    internal class AutorizacionEstadoGeneral : State
    {

        private string _folio = string.Empty;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="automatic"></param>
        /// <param name="planTrabajoMaestro"></param>
        /// <param name="planTrabajoMaestroDetalle"></param>
        public AutorizacionEstadoGeneral(
            bool automatic,
            IPlanTrabajoMaestro planTrabajoMaestro = null,
            IPlanTrabajoMaestroDetalle planTrabajoMaestroDetalle = null) : base(automatic, planTrabajoMaestro, planTrabajoMaestroDetalle)
        {

        }

        /// <summary>
        /// Asignacion del siguiente paso.
        /// </summary>
        /// <returns></returns>
        public override async Task<bool> Handle()
        {
            try
            {
                _folio = Context.ObtenerFolioActual();

                var validarState = await VamosYValidamosElEstado();
                if (!validarState)
                {
                    return false;
                }

                if (Automatico)
                {
                    await PlanTrabajoMaestro.ActualizamosElEstatusParaOperacion(
                        _folio,
                        EPlanTrabajoMaestroEstatus.X_AUT,
                        EPlanTrabajoMaestroEstatus.AUT,
                        "TOMI");

                    var siguienteDetalleState = DetallePlan.ObtenerSiguienteEstadoDelPlan(EDetallePlan.ESPERANDO_AUTORIZACION);

                    await PlanTrabajoMaestroDetalle.ActualizarEstatusOperacion(
                        _folio,
                        (int)EDetallePlan.ESPERANDO_AUTORIZACION,
                        (int)siguienteDetalleState);
                }

                return true;
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex, nameof(Handle));
                throw;
            }
        }

        /// <summary>
        /// Validamos el estado en que se encuentra el folio de la Queue.
        /// </summary>
        /// <returns></returns>
        protected override async Task<bool> VamosYValidamosElEstado()
        {
            try
            {
                var actualPlanTrabajoMaestroState = await PlanTrabajoMaestro.ObtenerEstatusActualParaOperacion(_folio);
                var actualPlanTrabajoMaestroDetalleState = await PlanTrabajoMaestroDetalle.ObtenerEstadoActualDeLaOperacion(_folio);
                var detalleActualState = (EDetallePlan)actualPlanTrabajoMaestroDetalleState;

                return actualPlanTrabajoMaestroState == EPlanTrabajoMaestroEstatus.X_AUT && detalleActualState == EDetallePlan.ESPERANDO_AUTORIZACION;
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex, nameof(VamosYValidamosElEstado));
                throw;
            }
        }
    }
}


using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Plan trabajo maestro.
    /// </summary>
    public class PlanTrabajoMaestro : RepositorioMaestro, IPlanTrabajoMaestro
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        /// <param name="proveedorVistasT24"></param>
        /// <param name="nombreBasedatosVistasT24"></param>
        public PlanTrabajoMaestro() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene el plan de trabajo maestro po ICN.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        public virtual async Task<DMasterWorkPlan> ObtenerPlanDeTrabajoMaestroPorICN(string folio)
        {
            var result = await EjecutarStoredProcedureQuery<DMasterWorkPlan>(ConstantesServicio.usp_ObtenerMasterWorkPlanPorIcn, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
            });

            if (!result.Exito)
            {
                throw new InvalidOperationException(
                    result.MensajeError,
                    result.Exception);
            }

            return result.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene el plan de trabajo maestro por folio t24.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        public virtual async Task<DMasterWorkPlan> ObtenerPlanDeTrabajoMaestroPorFolioT24(string folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<DMasterWorkPlan>(ConstantesServicio.usp_ObtenerMasterWorkPlanPorT24Folio, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

                return result.Data?.FirstOrDefault();
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ObtenerPlanDeTrabajoMaestroPorFolioT24));
                throw;
            }
        }

        /// <summary>
        /// Obtiene el estatus acual de la operacion.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        public virtual async Task<EPlanTrabajoMaestroEstatus> ObtenerEstatusActualParaOperacion(string folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<DStringGeneral>(
                    ConstantesServicio.usp_ObtenerEstadoActualMasterWorkPlan, 
                    cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

                var estado = result.Data?.FirstOrDefault()?.Estado;

                return Enum.TryParse<EPlanTrabajoMaestroEstatus>(
                    estado, out var estatus)
                ? estatus
                : EPlanTrabajoMaestroEstatus.X_ASR;
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex,nameof(ObtenerEstatusActualParaOperacion));
                throw;
            }
        }

        /// <summary>
        /// Actualiza el estatus para la operacion.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="current"></param>
        /// <param name="next"></param>
        /// <param name="authorizerAlias"></param>
        /// <returns></returns>
        public virtual async Task ActualizamosElEstatusParaOperacion(string folio, EPlanTrabajoMaestroEstatus current, EPlanTrabajoMaestroEstatus next, string authorizerAlias)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(ConstantesServicio.usp_ActualizarEstadoMasterWorkPlan, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@estadoActual", current.ToString()));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@estadoNuevo", next.ToString()));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@usuario", authorizerAlias));
                });
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ActualizamosElEstatusParaOperacion));
                throw;
            }
        }
    }
}


using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Plan trabajo maestro detalle.
    /// </summary>
    public class PlanTrabajoMaestroDetalle : RepositorioMaestro, IPlanTrabajoMaestroDetalle
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        /// <param name="proveedorVistasT24"></param>
        /// <param name="nombreBasedatosVistasT24"></param>
        public PlanTrabajoMaestroDetalle() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
        }

        /// <summary>
        /// Actualiza el estatus de la operacion.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="current"></param>
        /// <param name="next"></param>
        /// <returns></returns>
        public virtual async Task ActualizarEstatusOperacion(string folio, int current, int next)
        {
            try
            {
                await EjecutarStoredProceduredNonQuery(ConstantesServicio.usp_ActualizarEstadoDetailWorkPlan, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@estadoActual", current));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@estadoNuevo", next));
                });

            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ActualizarEstatusOperacion));
                throw;
            }
        }

        /// <summary>
        /// Obtiene el estado actual de la operacion.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        public virtual async Task<int> ObtenerEstadoActualDeLaOperacion(string folio)
        {
            try
            {
                var result =
                 await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerEstadoActualDetailWorkPlan, cmd =>
                 {
                     cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                 });

                return result.Data?.FirstOrDefault()?.orden_ejecucion ?? 0;
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ObtenerEstadoActualDeLaOperacion));
                throw;
            }

        }
    }
}


using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// percondicion check.
    /// </summary>
    public class PrecondicionCheck : RepositorioMaestro, IPrecondicionCheck
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public PrecondicionCheck() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
        }

        /// <summary>
        /// Verifica si el mensaje tiene cobertura especial.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        public async Task<bool> MensajeTieneCoberturaEspecial(string folio)
        {
            try
            {
                var intermediaryBank = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerIntermediarioSpeiPorFolio, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                }).
                ContinueWith(t => t.Result?.Data?.FirstOrDefault());

                var intermediary = intermediaryBank?.info;

                if (string.IsNullOrWhiteSpace(intermediary) || intermediary.Length < 6)
                {
                    Config.EscribirLog(
                        $"INFO| No hay banco intermediario para la operación con folio {folio}.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                if (intermediary.Contains("BOTKMXMX"))
                {
                    Config.EscribirLog(
                        $"INFO| Mufg MX es el banco para la operación con folio {folio}.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog(
                    $"INFO| Se encontró el banco intermediario {intermediary} para la operación con folio {folio}.", NombreServicio.LAIKA.ToString());
                return intermediary.Substring(4, 2) == "MX";
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex, nameof(MensajeTieneCoberturaEspecial));
                throw;
            }
        }
    }
}


using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Informacion operacion MT.
    /// </summary>
    public class InformacionOperacionMt : RepositorioMaestro, IInformacionOperacionMt
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        /// <param name="proveedorVistasT24"></param>
        /// <param name="nombreBasedatosVistasT24"></param>
        public InformacionOperacionMt() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtencion de la informacion de la operacion actual MT.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        public async Task<DOperacionEntrante> ObtenerInformacionDeLaOperacionActual(string folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<DOperacionEntrante>(ConstantesServicio.usp_ObtenerInformacionOperacionMtEntrada, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

                return result.Data?.FirstOrDefault() ?? new DOperacionEntrante();
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ObtenerInformacionDeLaOperacionActual));
                throw;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Informacion de la operacion MX.
    /// </summary>
    public class InformacionOperacionMx : RepositorioMaestro, IInformacionOperacionMx
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        /// <param name="proveedorVistasT24"></param>
        /// <param name="nombreBasedatosVistasT24"></param>
        public InformacionOperacionMx() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
        }

        /// <summary>
        /// Obtencion de la informacion de la operacion actual MX.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        public async Task<DOperacionEntrante> ObtenerInformacionDeLaOperacionActual(string folio)
        {
            try
            {
                var result = await EjecutarStoredProcedureQuery<DOperacionEntrante>(ConstantesServicio.usp_ObtenerInformacionOperacionMxEntrada, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

                return result.Data?.FirstOrDefault() ?? new DOperacionEntrante();
            }
            catch (System.Exception ex)
            {
                LogHelper.Log(ex,nameof(ObtenerInformacionDeLaOperacionActual));
                throw;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operacion repositorio mt202in.
    /// </summary>
    public class OperacionRepositorioMt202In : OperacionRepositorioBase, IOperacionRepositorioMt202In
    {

        private readonly IContorlServiciosAutomatizado _contorlServiciosAutomatizado;
        private readonly IPlanTrabajoMaestro _planTrabajoMaestro;
        private readonly IRechazo _rechazo;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioMt202In(
            IOperacionesGeneralesLaika operacionesGeneralesLaika,
            IContorlServiciosAutomatizado contorlServiciosAutomatizado,
            IPlanTrabajoMaestro planTrabajoMaestro,
            IRechazo rechazo
        ) : base(operacionesGeneralesLaika)
        {
            _contorlServiciosAutomatizado = contorlServiciosAutomatizado;
            _planTrabajoMaestro = planTrabajoMaestro;
            _rechazo = rechazo;
        }

        /// <summary>
        /// Regresa la informacion a ProcesarInternos del resultado y con eso indica si el proceso es Automatico o Manual
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        public override Task<ResultadoProceso> Procesar(string folio) => ProcesarInternos(folio);

        /// <summary>
        /// Regresa un booleano indicando si es un proceso manual o automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>bool</returns>
        protected override async Task<bool> MensajePuedeSerAutomaticoAutorizado(string folio)
        {
            try
            {
                Config.EscribirLog($"INFO|MT202_IN|Se inicia procesamiento de reglas para autorizar automáticamente la operación con folio {folio}", NombreServicio.LAIKA.ToString());

                var plan = await _planTrabajoMaestro.ObtenerPlanDeTrabajoMaestroPorICN(folio);

                if (plan == null || plan.flg_procesado_manual)
                {
                    Config.EscribirLog($"INFO|MT202_IN|El mensaje ya viene marcado para procesamiento manual.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                if (!string.Equals(plan.cve_moneda, "MXN", StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(plan.cve_moneda, "MXP", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
                  
                Config.EscribirLog($"INFO|MT202_IN|Se inicia procesamiento de reglas para autorizar automáticamente la operación con folio {folio}", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202_IN|Comienza la búsqueda del objeto de control de autorización para la operación con folio: {folio}", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202_IN|Se busca por tipo de mensaje {plan.cve_mensaje} y categoria {plan.message_category_id}", NombreServicio.LAIKA.ToString());

                var controlAutomatizado = await _contorlServiciosAutomatizado.ObtenerControlAutomatizadoPorIdCategoria(plan.cve_mensaje, "SWIFT_IN", plan.message_category_id);
                if (controlAutomatizado == null)
                {
                    Config.EscribirLog($"INFO|MT202_IN|No se encontró el objeto para control de autorización para el mensaje {plan.cve_mensaje} con categoría {plan.message_category_id}", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog($"INFO|MT202_IN|Se encontró el objeto para control de autorización", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202_IN|Se comprobará si el mensaje puede ser autorizado automáticamente. Para el control de autorización {controlAutomatizado.message_category_name}, estado actual: {(controlAutomatizado.op_automatica ? "Automática" : "Manual")}", NombreServicio.LAIKA.ToString());


                return await MensajePuedeSerAutomaticoAutorizadoMX(folio, plan) && controlAutomatizado.op_automatica;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizado|Ocurre Excepcion:" + ex.ToString(), NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Verifica si puede ser automatico autorizado.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="plan"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<bool> MensajePuedeSerAutomaticoAutorizadoMX(string folio, DMasterWorkPlan plan)
        {
            try
            {
                Config.EscribirLog($"INFO|MT202_IN|Se comienza búsqueda del BIC emisor del mensaje con folio {folio}", NombreServicio.LAIKA.ToString());
                var bicEnviado = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerBicEmisorMtPorFolio, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

                string strBicEnviado = bicEnviado?.Data?.FirstOrDefault()?.info;
                if (string.IsNullOrWhiteSpace(strBicEnviado))
                {
                    Config.EscribirLog($"INFO|MT202_IN|No se encontro un BIC emisor para el mensaje con folio  {folio}", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "MXN/MXP sin BIC emisor con cuenta vostro a debitar.");
                    return false;
                }

                Config.EscribirLog($"INFO|MT202_IN|Se extrajo el BIC emisor {strBicEnviado} al mensaje folio {folio}.", NombreServicio.LAIKA.ToString());

                Config.EscribirLog($"INFO|MT202_IN|Se comienza busqueda de cuentas VOSTRO en el sistema.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202_IN|Se busca la cuenta VOSTRO en MXN para el BIC {strBicEnviado}.", NombreServicio.LAIKA.ToString());

                var vostro = await EjecutarStoredProcedureQuery<DStringGenerico>(ConstantesServicio.usp_ObtenerCuentaVostroPorBicEnMoneda, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@bic", strBicEnviado));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@moneda", plan.cve_moneda));
                });

                if (vostro == null || vostro?.Data?.Count == 0)
                {
                    Config.EscribirLog($"INFO|MT202_IN|No se encontró la cuenta VOSTRO del BIC emisor {strBicEnviado} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "Mensaje sin cuenta VOSTRO en T24 en MXN para debitar", true);
                    return false;
                }

                if (plan.num_cliente != 0)
                {
                    Config.EscribirLog($"INFO|MT202_IN|La operación es una transferencia interna, se operará automáticamente.", NombreServicio.LAIKA.ToString());
                    Config.EscribirLog($"INFO|MT202_IN|Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}.", NombreServicio.LAIKA.ToString());
                    return true;
                }

                Config.EscribirLog($"INFO|MT202_IN|Se encontró la cuenta VOSTRO del BIC emisor {strBicEnviado} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202_IN|Se comienza búsqueda del CASFIM receptor del mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());

                var casFimReceptor = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerCasFimReceptorMt202PorFolio, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

                string strCasFimReceptor = casFimReceptor?.Data?.FirstOrDefault()?.info;

                if (string.IsNullOrWhiteSpace(strCasFimReceptor))
                {

                    var receptorBic = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerBicReceptorMt202PorFolio, cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                    });

                    string strReceptorBic = receptorBic?.Data?.FirstOrDefault()?.info;
                    await _rechazo.RechazarMensajeConRazon(folio, $"No se encontró un CASFIM asociado a {strReceptorBic}.", true);

                    Config.EscribirLog($"INFO|MT202_IN|No se encontró el CASFIM asociado a {strReceptorBic} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog($"INFO|MT202_IN|Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}.", NombreServicio.LAIKA.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizadoMX|Ocurre Excepcion:" + ex.ToString(), NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es Automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        protected override async Task<ResultadoProceso> ProcesoAutomatico(string folio)
        {
            try
            {
                bool respuestaOperacionGeneral = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, true);
                return Resultado(
                    folio,
                    "MT202_IN",
                    respuestaOperacionGeneral,
                    true,
                    respuestaOperacionGeneral
                    ? "Procesado automáticamente"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoAutomatico));
                throw;
            }
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es manual.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        protected override async Task<ResultadoProceso> ProcesoManual(string folio)
        {
            try
            {
                bool respuestaOperacionGeneral = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, false);
                return Resultado(
                    folio,
                    "MT202_IN",
                    respuestaOperacionGeneral,
                    false,
                    respuestaOperacionGeneral
                    ? "Enrutado a flujo manual"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoManual));
                throw;
            }
        }
    }
}

using System;
using System.Text.RegularExpressions;


namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Swift utilidades.
    /// </summary>
    public class SwiftUtilidades
    {
        /// <summary>
        /// Extrae el valor de un tag SWIFT (ej: 71A, 20, 32A)
        /// </summary>
        /// <param name="mensaje"></param>
        /// <param name="tag"></param>
        /// <returns></returns>
        public static string ObtenerTagPorMt(string mensaje, string tag)
        {
            if (string.IsNullOrWhiteSpace(mensaje)) return string.Empty;
            var limits = ObtenerLimiteDelContenidoTag(mensaje, $":{tag}:");

            if (limits.Item1 == -1 || limits.Item2 == -1 || limits.Item2 <= limits.Item1)
                return string.Empty;

            var result = mensaje.Substring(limits.Item1, limits.Item2 - limits.Item1);
            return result?.Trim() ?? string.Empty;
        }

        /// <summary>
        /// Extrae el inicio y fin del contenido de un tag SWIFT
        /// </summary>
        /// <param name="mensaje"></param>
        /// <param name="tag"></param>
        /// <returns></returns>
        public static Tuple<int, int> ObtenerLimiteDelContenidoTag(string mensaje, string tag)
        {
            const string pattern = @"(?::\d{1,2}[a-zA-Z]?:|}{5:)";
            var regex = new Regex(pattern, RegexOptions.Multiline);

            var startIndex = mensaje.IndexOf(tag, StringComparison.OrdinalIgnoreCase);

            if (startIndex < 0)
                return new Tuple<int, int>(-1, -1);

            var contentStart = startIndex + tag.Length;

            var match = regex.Match(mensaje, contentStart);
            int contentEnd;

            if (match.Success)
            {
                contentEnd = match.Index - 1;
            }
            else
            {
                contentEnd = mensaje.Length;
            }

            return new Tuple<int, int>(contentStart, contentEnd);
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Swift in mt comun.
    /// </summary>
    public class SwiftInMtComun : RepositorioMaestro, ISwiftInMtComun
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        /// <param name="proveedorVistasT24"></param>
        /// <param name="nombreBasedatosVistasT24"></param>
        public SwiftInMtComun() : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
        }

        /// <summary>
        /// Obtencion de tipo comision para mensaje por folio.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task<ESwiftComision> ObtenerTipoComisionParaMensajePorFolio(string folio)
        {
            try
            {
                var mensaje = await ObtenerMensajePorFolio(folio);
                if (string.IsNullOrWhiteSpace(mensaje)) return ESwiftComision.UNKNOWN;

                var contenidoSwift = SwiftUtilidades.ObtenerTagPorMt(mensaje, "71A");

                if (string.IsNullOrWhiteSpace(contenidoSwift))
                {
                    return ESwiftComision.UNKNOWN;
                }

                return Enum.TryParse(contenidoSwift, out ESwiftComision tipoComision)
                    ? tipoComision
                    : ESwiftComision.UNKNOWN;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    "ERROR|ObtenerTipoComisionParaMensajePorFolio|Ocurre Excepcion:" + ex, NombreServicio.LAIKA.ToString());
                throw;
            }
        }

        /// <summary>
        /// obtencion de mensaje por folio.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        public async Task<string> ObtenerMensajePorFolio(string folio)
        {
            try
            {
                return await EjecutarStoredProcedureQuery<DNFolioMq>(ConstantesServicio.usp_ObtenerTipoComisionMtPorFolio, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                }).ContinueWith(t => t.Result?.Data?.FirstOrDefault()?.NF_MQ_MSG ?? "");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ObtenerMensajePorFolio));
                throw;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operacion repositorio Pacs008in.
    /// </summary>
    public class OperacionRepositorioPacs008In : OperacionRepositorioBase, IOperacionRepositorioPacs008In
    {
        private readonly IContorlServiciosAutomatizado _contorlServiciosAutomatizado;
        private readonly IPlanTrabajoMaestro _planTrabajoMaestro;
        private readonly IInformacionOperacionMx _informacionOperacionMx;
        private readonly ISwiftInMxComun _siftInMxCommun;
        private readonly IRechazo _rechazo;

        private readonly string[] USDCorrespndiente = { "CHASUS33", "BOTKUS33" };
        private readonly string[] JPYCorrespndiente = { "CHASJPJT" };
        private readonly string[] EURCorrespndiente = { "CHASGB2L" };
        private readonly string[] GBPCorrespndiente = { "CHASGB2L" };
        private readonly string[] CADCorrespndiente = { "CHASCATT" };

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioPacs008In(
            IOperacionesGeneralesLaika operacionesGeneralesLaika,
            IContorlServiciosAutomatizado contorlServiciosAutomatizado,
            IPlanTrabajoMaestro planTrabajoMaestro,
            IInformacionOperacionMx informacionOperacionMx,
            ISwiftInMxComun swiftServicioComun,
            IRechazo rechazo
        ) : base(operacionesGeneralesLaika)
        {
            _contorlServiciosAutomatizado = contorlServiciosAutomatizado;
            _planTrabajoMaestro = planTrabajoMaestro;
            _informacionOperacionMx = informacionOperacionMx;
            _siftInMxCommun = swiftServicioComun;
            _rechazo = rechazo;
        }

        /// <summary>
        /// Regresa la informacion a ProcesarInternos del resultado y con eso indica si el proceso es Automatico o Manual
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        public override Task<ResultadoProceso> Procesar(string folio) => ProcesarInternos(folio);

        /// <summary>
        /// Regresa un booleano indicando si es un proceso manual o automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>bool</returns>
        protected override async Task<bool> MensajePuedeSerAutomaticoAutorizado(string folio)
        {
            try
            {
                Config.EscribirLog(
                    $"INFO|PACS008_IN|Inicia evaluación. Folio: '{folio}'", NombreServicio.LAIKA.ToString());

                Config.EscribirLog(
                    $"INFO|PACS008_IN|Se inicia procesamiento de reglas para autorizar automáticamente la operación con folio {folio}", NombreServicio.LAIKA.ToString());

                var plan = await _planTrabajoMaestro.ObtenerPlanDeTrabajoMaestroPorICN(folio);

                if (plan == null || plan.flg_procesado_manual)
                {
                    Config.EscribirLog($"INFO|PACS008_IN|El mensaje ya viene marcado para procesamiento manual.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog(
                    $"INFO|PACS008_IN|Comienza la búsqueda del objeto de control de autorización para la operación con folio: {folio}.", NombreServicio.LAIKA.ToString());

                Config.EscribirLog(
                    $"INFO|PACS008_IN|Se busca por tipo de mensaje {plan.cve_mensaje} y categoria {plan.message_category_id}", NombreServicio.LAIKA.ToString());
            
                var controlAutomatizado = await _contorlServiciosAutomatizado.ObtenerControlAutomatizadoPorIdCategoria(plan.cve_mensaje, "SWIFT_IN", plan.message_category_id);
            
                if (controlAutomatizado == null || !controlAutomatizado.op_automatica)
                {
                    Config.EscribirLog($"INFO|PACS008_IN| No se encontró el objeto para control de autorización para el mensaje {plan.cve_mensaje} con categoría {plan.message_category_id}.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog($"INFO|PACS008_IN|Se encontró el objeto para control de autorización.", NombreServicio.LAIKA.ToString());

                Config.EscribirLog($"INFO|PACS008_IN|Se comprobará si el mensaje puede ser autorizado automáticamente." +
                    $" Para el control de autorización {controlAutomatizado.message_category_name}, estado actual: {(controlAutomatizado.op_automatica ? "Automática" : "Manual")}", NombreServicio.LAIKA.ToString());

                return plan.cve_moneda == "MXN" || plan.cve_moneda == "MXP"
                       ? await MxnPuedeSerAutomatizacionAutorizada(folio, plan) && controlAutomatizado.op_automatica
                       : await MonedaExtranjeraPuedeSerAutomatizacionAutorizada(folio, plan) && controlAutomatizado.op_automatica;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizado|Ocurre Excepcion:" + ex.ToString(), NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es Automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        protected override async Task<ResultadoProceso> ProcesoAutomatico(string folio)
        {
            try
            {
                var operacionInfo = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, true);

                return Resultado(
                    folio,
                    "PACS008_IN",
                    operacionInfo,
                    true, operacionInfo
                    ? "Procesado automáticamente"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoAutomatico));
                throw;
            }
            
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es Manual.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        protected override async Task<ResultadoProceso> ProcesoManual(string folio)
        {
            try
            {
                var operacionInfo = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, false);

                return Resultado(
                    folio,
                    "PACS008_IN",
                    operacionInfo,
                    false,
                    operacionInfo
                    ? "Estado a flujo manual"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoManual));
                throw;
            }
        }


        // Funciones Generales del PACS008In

        /// <summary>
        /// Verifica que la funcion pueda ser Automatica MX.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="planActualTrabajo"></param>
        /// <returns></returns>
        private async Task<bool> MxnPuedeSerAutomatizacionAutorizada(string folio, DMasterWorkPlan planActualTrabajo)
        {
            try
            {
                Config.EscribirLog(
                    $"INFO|PACS008_IN|Se comienza búsqueda del BIC emisor del mensaje con folio {folio}", NombreServicio.LAIKA.ToString());

                var enviarBic = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerBicEmisorMxPorFolio, cmd =>     
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                }).ContinueWith(t => t.Result?.Data?.FirstOrDefault() ?? new DStringGeneral());

                var bic = enviarBic?.info;

                if (string.IsNullOrWhiteSpace(bic))
                {
                    Config.EscribirLog($"INFO|PACS008_IN|No se encontro un BIC emisor para el mensaje con folio {folio}", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "MXN/MXP sin BIC emisor con cuenta vostro a debitar.");
                    return false;
                }

                Config.EscribirLog($"INFO|PACS008_IN|Se extrajo el BIC emisor {bic} al mensaje folio {folio}.", NombreServicio.LAIKA.ToString());

                var vostro = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerCuentaNostroPacsYMt103, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@bic", bic));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@moneda", planActualTrabajo.cve_moneda));
                });

                var hayVostro = vostro?.Data?.Any() == true;

                if (!hayVostro)
                {
                    Config.EscribirLog(
                        $"INFO|PACS008_IN|No se encontró la cuenta VOSTRO del BIC emisor {bic} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());

                    await _rechazo.RechazarMensajeConRazon(
                        folio, "Mensaje sin cuenta VOSTRO en T24 en MXN para debitar.", true);

                    return false;
                }

                if (planActualTrabajo.num_cliente != 0)
                {
                    Config.EscribirLog(
                        $"INFO|PACS008_IN|La operación es una transferencia interna, se operará automáticamente.", NombreServicio.LAIKA.ToString());

                    Config.EscribirLog(
                        $"INFO|PACS008_IN|Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}.", NombreServicio.LAIKA.ToString());

                    return true;
                }

                Config.EscribirLog(
                    $"INFO|PACS008_IN|Se encontró la cuenta VOSTRO del BIC emisor {bic} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());

                Config.EscribirLog(
                    $"INFO|PACS008_IN|Se procederá a buscar el esquema de comisión para la cuenta de {bic}.", NombreServicio.LAIKA.ToString());

                Config.EscribirLog(
                    $"INFO|PACS008_IN|Se va a buscar la cuenta VOSTRO para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());

                var informacionOperacionEntrada = await _informacionOperacionMx.ObtenerInformacionDeLaOperacionActual(folio);

                if(string.IsNullOrWhiteSpace(informacionOperacionEntrada?.VostroBank))
                {
                    Config.EscribirLog($"ERROR|PACS008_IN|No se encontró la cuenta VOSTRO.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "No se encontró un código de cliente para la operación.", true);
                    return false;
                }

                Config.EscribirLog(
                    $"INFO|PACS008_IN|Se encontró la cuenta VOSTRO {informacionOperacionEntrada.VostroBank}.", NombreServicio.LAIKA.ToString());

                var cuentaPaso = informacionOperacionEntrada.VostroBank.Split('-');
                var cuentaComision = cuentaPaso[2];

                var codigoCliente = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerCodigoClientePorCuenta, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cuenta", cuentaComision));
                }, esVistasT24: true);

                int clienteActual = codigoCliente?.Data?.FirstOrDefault()?.CUSTOMER ?? 0;

                Config.EscribirLog($"INFO|PACS008_IN|Se encontró el código de cliente {clienteActual}.", NombreServicio.LAIKA.ToString());

                if(clienteActual < 1)
                {
                    Config.EscribirLog($"INFO|PACS008_IN|No se encontró un código de cliente para el BIC {bic}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "No se encontró un código de cliente pora la operación.", true);
                    return false;
                }

                Config.EscribirLog(
                    $"INFO|PACS008_IN| Se procederá a buscar el tipo de comisión en el mensaje.", NombreServicio.LAIKA.ToString());

                var tipoComision = await _siftInMxCommun.ObtenerTipoComisionParaMensajePorFolio(folio);

                Config.EscribirLog(
                    $"INFO|PACS008_IN| Se encontró el tipo de comisión {tipoComision.ToString()} en el mensaje.", NombreServicio.LAIKA.ToString());

                var comisionEsquema = await EjecutarStoredProcedureQuery<DComisionEsquemaAfiliado>(ConstantesServicio.usp_ObtenerEsquemaComisionPorCliente, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@customerCode", clienteActual));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@commissionType", tipoComision.ToString()));
                });

                if(comisionEsquema?.Data?.FirstOrDefault() == null)
                {
                    Config.EscribirLog($"INFO|PACS008_IN|No se encontró el esquema de comisión para la cuenta de: {bic}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "No se encontró un esquema de comisión para la operación.", true);
                    return false;
                }

                Config.EscribirLog($"INFO|PACS008_IN|Se tiene el tipo de comisión: {comisionEsquema?.Data?.FirstOrDefault().Type}, se cobrará un total de {comisionEsquema?.Data?.FirstOrDefault().Total} {comisionEsquema?.Data?.FirstOrDefault().Currency} de comisión.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|PACS008_IN|Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}.", NombreServicio.LAIKA.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MxnPuedeSerAutomatizacionAutorizada|Ocurre Excepcion:" + ex.ToString(), NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        } 


        /// <summary>
        /// Verifica que la función pueda ser automatica Extranjera.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="planActualTrabajo"></param>
        /// <returns>bool</returns>
        private async Task<bool> MonedaExtranjeraPuedeSerAutomatizacionAutorizada(string folio, DMasterWorkPlan plan)
        {
            try
            {

                var operacionInfo = await _informacionOperacionMx.ObtenerInformacionDeLaOperacionActual(folio);

                if (string.IsNullOrWhiteSpace(operacionInfo.IssuingBank)) 
                    await _rechazo.RechazarMensajeConRazon(folio, "No fue posible determinar el banco emisor.");

                Config.EscribirLog($"INFO|PACS008_IN| Se procede a validar el banco emisor del mensaje.", NombreServicio.LAIKA.ToString());

                switch (plan.cve_moneda)
                {
                    case "USD":
                        if (!USDCorrespndiente.Any(n => operacionInfo.IssuingBank.Contains(n)))
                        {
                            Config.EscribirLog($"ERROR|PACS008_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.", 
                                NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "PACS008 USD no emitido por JP MORGAN o MUFG, NY");
                            return false;
                        }
                        break;
                    case "EUR":
                        if (!operacionInfo.IssuingBank.Contains(EURCorrespndiente[0]))
                        {
                            Config.EscribirLog($"ERROR|PACS008_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.",
                                NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "PACS008 EUR no emitido por JP MORGAN, LONDRES");
                            return false;
                        }
                        break;
                    case "JPY":
                        if (!operacionInfo.IssuingBank.Contains(JPYCorrespndiente[0]))
                        {
                            Config.EscribirLog($"ERROR|PACS008_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.",
                                NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "PACS008 JPY no emitido por JP MORGAN, TOKYO");
                            return false;
                        }
                        break;
                    case "GBP":
                        if (!operacionInfo.IssuingBank.Contains(GBPCorrespndiente[0]))
                        {
                            Config.EscribirLog($"ERROR|PACS008_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.", 
                                NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "PACS008 GBP no emitido por JP MORGAN, LONDRES");
                            return false;
                        }
                        break;
                    case "CAD":
                        if (!operacionInfo.IssuingBank.Contains(CADCorrespndiente[0]))
                        {
                            Config.EscribirLog($"ERROR|PACS008_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.", 
                                NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "PACS008 CAD no emitido por JP MORGAN, CANADA");
                            return false;
                        }
                        break;
                }

                Config.EscribirLog($"INFO|PACS008_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es válido.", 
                    NombreServicio.LAIKA.ToString());

                Config.EscribirLog($"INFO|PACS008_IN| Se validará si se logró extraer una cuenta existe en el sistema.", 
                    NombreServicio.LAIKA.ToString());

                if (operacionInfo.CustomerAccount == -1)
                {
                    Config.EscribirLog($"ERROR|PACS008_IN| No se logró localizar una cuenta en el sistema BTMUM.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "Cuenta Inválida o inexistente en BTMUM", true);
                    return false;
                }

                Config.EscribirLog($"INFO|PACS008_IN| Se procesaron exitosamente las reglas de ejecucion.", 
                    NombreServicio.LAIKA.ToString());

                Config.EscribirLog($"INFO|PACS008_IN| Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}.", 
                    NombreServicio.LAIKA.ToString());

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MonedaExtranjeraPuedeSerAutomatizacionAutorizada|Ocurre Excepcion:" + ex.ToString(), NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operacion repositorio mt202covin.
    /// </summary>
    public class OperacionRepositorioMt202CovIn : OperacionRepositorioBase, IOperacionRepositorioMt202CovIn
    {

        private readonly IContorlServiciosAutomatizado _contorlServiciosAutomatizado;
        private readonly IPlanTrabajoMaestro _planTrabajoMaestro;
        private readonly IRechazo _rechazo;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioMt202CovIn(
            IOperacionesGeneralesLaika operacionesGeneralesLaika, 
            IContorlServiciosAutomatizado contorlServiciosAutomatizado,
            IPlanTrabajoMaestro planTrabajoMaestro,
            IRechazo rechazo
        ) : base(operacionesGeneralesLaika)
        {
            _contorlServiciosAutomatizado = contorlServiciosAutomatizado;
            _planTrabajoMaestro = planTrabajoMaestro;
            _rechazo = rechazo;
        }

        /// <summary>
        /// Regresa la informacion a ProcesarInternos del resultado y con eso indica si el proceso es Automatico o Manual
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        public override Task<ResultadoProceso> Procesar(string folio)
            => ProcesarInternos(folio);

        /// <summary>
        /// Regresa un booleano indicando si es un proceso manual o automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>bool</returns>
        protected override async Task<bool> MensajePuedeSerAutomaticoAutorizado(string folio)
        {
            try
            {
                Config.EscribirLog($"INFO|MT202COV_IN|Se inicia procesamiento.", NombreServicio.LAIKA.ToString());

                var plan = await _planTrabajoMaestro.ObtenerPlanDeTrabajoMaestroPorICN(folio);

                if (plan == null || plan.flg_procesado_manual)
                {
                    Config.EscribirLog($"INFO|MT202COV_IN|El mensaje ya viene marcado para procesamiento manual.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                if (!string.Equals(plan.cve_moneda, "MXN", StringComparison.OrdinalIgnoreCase) &&
                      !string.Equals(plan.cve_moneda, "MXP", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }

                Config.EscribirLog($"INFO|MT202COV_IN|Se inicia procesamiento de reglas para autorizar automáticamente la operación con folio {folio}", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202COV_IN|Comienza la búsqueda del objeto de control de autorización para la operación con folio: {folio}", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202COV_IN|Se busca por tipo de mensaje {plan.cve_mensaje} y categoria {plan.message_category_id}", NombreServicio.LAIKA.ToString());

                var controlAutomatizado = await _contorlServiciosAutomatizado.ObtenerControlAutomatizadoPorIdCategoria(plan.cve_mensaje, "SWIFT_IN", plan.message_category_id);
                if (controlAutomatizado == null || !controlAutomatizado.op_automatica)
                {
                    Config.EscribirLog($"ERROR|MT202COV_IN|No se encontró el objeto para control de autorización para el mensaje {plan.cve_mensaje} con categoría {plan.message_category_id}", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog($"INFO|MT202COV_IN|Se encontró el objeto para control de autorización.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202COV_IN|Se comprobará si el mensaje puede ser autorizado automáticamente. Para el control de autorización {controlAutomatizado.message_category_name}, estado actual: {(controlAutomatizado.op_automatica ? "Automática" : "Manual")}", NombreServicio.LAIKA.ToString());
                return await MensajePuedeSerAutomaticoAutorizadoMX(folio, plan) && controlAutomatizado.op_automatica;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizado|Ocurre Excepcion:" + ex.ToString(), NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es Automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        protected override async Task<ResultadoProceso> ProcesoAutomatico(string folio)
        {
            try
            {
                bool respuestaOperacionGeneral = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, true);
                return Resultado(
                    folio,
                    "MT202COV_IN",
                    respuestaOperacionGeneral,
                    true,
                    respuestaOperacionGeneral
                    ? "Procesado automáticamente"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex,nameof(ProcesoAutomatico));
                throw;
            }
           
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es Manual.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        protected override async Task<ResultadoProceso> ProcesoManual(string folio)
        {
            try
            {
                bool respuestaOperacionGeneral = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, false);

                return Resultado(
                    folio,
                    "MT202COV_IN",
                    respuestaOperacionGeneral,
                    false,
                    respuestaOperacionGeneral
                    ? "Enrutado a flujo manual"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoManual));
                throw;
            }
        }

        /// <summary>
        /// Verifica si mensaje puede ser automatico autorizado.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="plan"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<bool> MensajePuedeSerAutomaticoAutorizadoMX(string folio, DMasterWorkPlan plan)
        {
            try
            {
                Config.EscribirLog($"INFO|MT202COV_IN|Se comienza búsqueda del BIC emisor del mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());

                var bicEnviado = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerBicEmisorMtPorFolio, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

                string strBicEnviado = bicEnviado?.Data?.FirstOrDefault()?.info;
                if (string.IsNullOrWhiteSpace(strBicEnviado))
                {
                    Config.EscribirLog($"ERROR|MT202COV_IN|No se encontro un BIC emisor para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "MXN/MXP sin BIC emisor con cuenta vostro a debitar.");
                    return false;
                }

                Config.EscribirLog($"INFO|MT202COV_IN|Se extrajo el BIC emisor {strBicEnviado} al mensaje folio {folio}.", NombreServicio.LAIKA.ToString());

                Config.EscribirLog($"INFO|MT202COV_IN|Se comienza busqueda de cuentas VOSTRO en el sistema.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202COV_IN|Se busca la cuenta VOSTRO en MXN para el BIC {strBicEnviado}", NombreServicio.LAIKA.ToString());
                var vostro = await EjecutarStoredProcedureQuery<DStringGenerico>(ConstantesServicio.usp_ObtenerCuentaVostroPorBicEnMoneda, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@bic", strBicEnviado));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@moneda", plan.cve_moneda));
                });

                if (vostro == null || vostro?.Data?.Count == 0)
                {
                    Config.EscribirLog($"INFO|MT202COV_IN|No se encontró la cuenta VOSTRO del BIC emisor {strBicEnviado} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "Mensaje sin cuenta VOSTRO en T24 en MXN para debitar", true);
                    return false;
                }

                if(plan.num_cliente != 0)
                {
                    Config.EscribirLog($"INFO|MT202COV_IN|La operación es una transferencia interna, se operará automáticamente.", NombreServicio.LAIKA.ToString());
                    Config.EscribirLog($"INFO|MT202COV_IN|Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}", NombreServicio.LAIKA.ToString());
                    
                    return true;
                }

                Config.EscribirLog($"INFO|MT202COV_IN|Se encontró la cuenta VOSTRO del BIC emisor {strBicEnviado} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT202COV_IN|Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}", NombreServicio.LAIKA.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizadoMX|Ocurre Excepcion:" + ex.ToString(), NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operacion repositorio mt103out.
    /// </summary>
    public class OperacionRepositorioMt103Out : OperacionRepositorioBase, IOperacionRepositorioMt103Out
    {

        private readonly IContorlServiciosAutomatizado _contorlServiciosAutomatizado;
        private readonly IPlanTrabajoMaestro _planTrabajoMaestro;
        private readonly IPrecondicionCheck _precondicionCheck;
        private readonly IPrecondicionCheckSpei _precondicionCheckSpei;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proveedor"></param>
        /// <param name="nombreBasedatos"></param>
        public OperacionRepositorioMt103Out(
            IOperacionesGeneralesLaika operacionesGeneralesLaika,
            IContorlServiciosAutomatizado contorlServiciosAutomatizado,
            IPlanTrabajoMaestro planTrabajoMaestro,
            IPrecondicionCheck precondicionCheck,
            IPrecondicionCheckSpei precondicionCheckSpei
        ) : base(operacionesGeneralesLaika)
        {
            _contorlServiciosAutomatizado = contorlServiciosAutomatizado;
            _planTrabajoMaestro = planTrabajoMaestro;
            _precondicionCheck = precondicionCheck;
            _precondicionCheckSpei = precondicionCheckSpei;

        }

        /// <summary>
        /// Regresa la informacion a ProcesarInternos del resultado y con eso indica si el proceso es Automatico o Manual
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        public override Task<ResultadoProceso> Procesar(string folio) => ProcesarInternos(folio);

        /// <summary>
        /// Regresa un booleano indicando si es un proceso manual o automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>bool</returns>
        protected override async Task<bool> MensajePuedeSerAutomaticoAutorizado(string folio)
        {
            try
            {
                Config.EscribirLog($"INFO|MT103_OUT|Se inicia procesamiento de reglas para autorizar automáticamente la operación con folio {folio}", NombreServicio.LAIKA.ToString());

                if (string.IsNullOrWhiteSpace(folio)) return false;
                DMasterWorkPlan planTrabajoActual = folio.StartsWith("FT")
                    ? await _planTrabajoMaestro.ObtenerPlanDeTrabajoMaestroPorFolioT24(folio)
                    : await _planTrabajoMaestro.ObtenerPlanDeTrabajoMaestroPorICN(folio);

                Config.EscribirLog($"INFO|MT103_OUT|Se hace la busqueda por {(folio.StartsWith("FT") ? "FT" : "Folio")}", NombreServicio.LAIKA.ToString());


                if (planTrabajoActual == null || planTrabajoActual.flg_procesado_manual)
                {
                    Config.EscribirLog($"INFO|MT103_OUT|El mensaje ya viene marcado para procesamiento manual.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog($"INFO|MT103_OUT|Comienza la búsqueda del objeto de control de autorización para la operación con folio: {folio}.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT103_OUT|Se busca por tipo de mensaje {planTrabajoActual.cve_mensaje} y categoria {planTrabajoActual.message_category_id}", NombreServicio.LAIKA.ToString());

                var controlAutomatizado = await _contorlServiciosAutomatizado.ObtenerControlAutomatizadoPorIdCategoria(planTrabajoActual.cve_mensaje, "SWIFT_OUT", planTrabajoActual.message_category_id);

                if (controlAutomatizado == null)
                {
                    Config.EscribirLog($"INFO|MT103_OUT|No se encontró el objeto para control de autorización para el mensaje {planTrabajoActual.cve_mensaje} con categoría {planTrabajoActual.message_category_id}", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog($"INFO|MT103_OUT|Se comprobará si el mensaje puede ser autorizado automáticamente. " +
                    $"Para el control de autorización {controlAutomatizado.message_category_name}, estado actual: {(controlAutomatizado.op_automatica ? "Automática" : "Manual")}", NombreServicio.LAIKA.ToString());

                Config.EscribirLog($"INFO|MT103_OUT|Se comprobará si el mensaje es en MXN y por ende debe cersiorarse que la liquidación de la operación se haya dado.", NombreServicio.LAIKA.ToString());

                if (planTrabajoActual.cve_moneda != "MXN")
                    return controlAutomatizado.op_automatica;

                Config.EscribirLog($"INFO|MT103_OUT|Se comprobará si el mensaje se liquida por el SPEI.", NombreServicio.LAIKA.ToString());

                if (!await _precondicionCheck.MensajeTieneCoberturaEspecial(planTrabajoActual.folio))
                    return controlAutomatizado.op_automatica;

                Config.EscribirLog($"INFO|MT103_OUT|Se comprobará si el mensaje ya fue liquidado en el SPEI.", NombreServicio.LAIKA.ToString());

                if (await _precondicionCheckSpei.CoberturaEspecialResuelta(planTrabajoActual.transaction_ref))
                    return controlAutomatizado.op_automatica;

                Config.EscribirLog($"INFO|MT103_OUT|El mensaje debe esperar a su liquidación en SPEI para poder ser enviado por SWIFT.", NombreServicio.LAIKA.ToString());
                return false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizado|Ocurre Excepcion:" + ex.ToString(), NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es Automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        protected override async Task<ResultadoProceso> ProcesoAutomatico(string folio)
        {
            try
            {
                bool respuestaOperacionGeneral = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, true);
                return Resultado(
                    folio,
                    "MT103_OUT",
                    respuestaOperacionGeneral,
                    true,
                    respuestaOperacionGeneral
                    ? "Procesado automáticamente"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoAutomatico));
                throw;
            }
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es manual.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        protected override async Task<ResultadoProceso> ProcesoManual(string folio)
        {
            try
            {
                bool respuestaOperacionGeneral =
               await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, false);

                return Resultado(
                    folio,
                    "MT103_OUT",
                    respuestaOperacionGeneral,
                    false,
                    respuestaOperacionGeneral
                    ? "Enrutado a flujo manual"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoManual));
                throw;
            }
        }
    }
}

using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Helper;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Operacion Rpositorio mt103in.
    /// </summary>
    public class OperacionRepositorioMt103In : OperacionRepositorioBase, IOperacionRepositorioMt103In
    {
        private readonly IContorlServiciosAutomatizado _contorlServiciosAutomatizado;
        private readonly IPlanTrabajoMaestro _planTrabajoMaestro;
        private readonly IInformacionOperacionMt _informacionOperacionMt;
        private readonly ISwiftInMtComun _siftInMtCommun;
        private readonly IRechazo _rechazo;

        private readonly string[] USDCorrespndiente = { "CHASUS33", "BOTKUS33" };
        private readonly string[] JPYCorrespndiente = { "CHASJPJT" };
        private readonly string[] EURCorrespndiente = { "CHASGB2L" };
        private readonly string[] GBPCorrespndiente = { "CHASGB2L" };
        private readonly string[] CADCorrespndiente = { "CHASCATT" };

        /// <summary>
        /// Constructor.
        /// </summary>
        public OperacionRepositorioMt103In(
            IOperacionesGeneralesLaika operacionesGeneralesLaika,
            IContorlServiciosAutomatizado contorlServiciosAutomatizado,
            IPlanTrabajoMaestro planTrabajoMaestro,
            IInformacionOperacionMt informacionOperacionMt,
            ISwiftInMtComun swiftInMtCommun,
            IRechazo rechazo)
        : base(operacionesGeneralesLaika)
        {
            _contorlServiciosAutomatizado = contorlServiciosAutomatizado;
            _planTrabajoMaestro = planTrabajoMaestro;
            _informacionOperacionMt = informacionOperacionMt;
            _siftInMtCommun = swiftInMtCommun;
            _rechazo = rechazo;
        }

        /// <summary>
        /// Regresa la informacion a ProcesarInternos del resultado y con eso indica si el proceso es Automatico o Manual
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>ResultadoProceso</returns>
        public override Task<ResultadoProceso> Procesar(string folio) => ProcesarInternos(folio);

        /// <summary>
        /// Regresa un booleano indicando si es un proceso manual o automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        protected override async Task<bool> MensajePuedeSerAutomaticoAutorizado(string folio)
        {
            try
            {

                Config.EscribirLog($"INFO|MT103_IN| Inicia evaluación. Folio: '{folio}'", NombreServicio.LAIKA.ToString());

                var plan = await _planTrabajoMaestro.ObtenerPlanDeTrabajoMaestroPorICN(folio);

                if (plan == null || plan.flg_procesado_manual)
                {
                    Config.EscribirLog($"INFO|MT103_IN| El mensaje ya viene marcado para procesamiento manual.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog($"INFO|MT103_IN| Comienza la búsqueda del objeto de control de autorización para la operación con folio: {folio}.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT103_IN| Se busca por tipo de mensaje {plan.cve_mensaje} y categoria {plan.message_category_id}.", NombreServicio.LAIKA.ToString());

                var controlAutomatizado = await _contorlServiciosAutomatizado.ObtenerControlAutomatizadoPorIdCategoria(plan.cve_mensaje, "SWIFT_IN", plan.message_category_id);
                if (controlAutomatizado == null || !controlAutomatizado.op_automatica)
                {
                    Config.EscribirLog($"ERROR|MT103_IN| No se encontró el objeto para control de autorización para el mensaje {plan.cve_mensaje} con categoría {plan.message_category_id}.", NombreServicio.LAIKA.ToString());
                    return false;
                }

                Config.EscribirLog($"INFO|MT103_IN| Se encontró el objeto para control de autorización.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT103_IN| Se comprobará si el mensaje puede ser autorizado automáticamente. Para el control de autorización {controlAutomatizado.message_category_name}, estado actual: {(controlAutomatizado.op_automatica ? "Automática" : "Manual")}", NombreServicio.LAIKA.ToString());

                return plan.cve_moneda == "MXN" || plan.cve_moneda == "MXP"
                       ? await MxnPuedeSerAutomatizacionAutorizada(folio, plan) && controlAutomatizado.op_automatica
                       : await MonedaExtranjeraPuedeSerAutomatizacionAutorizada(folio, plan) && controlAutomatizado.op_automatica;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizado|Ocurre Excepcion:" + ex, NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es Automatico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns>bool</returns>
        protected override async Task<ResultadoProceso> ProcesoAutomatico(string folio)
        {
            try
            {
                var operacionInfo = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, true);

                return Resultado(
                    folio,
                    "MT103_IN",
                    operacionInfo,
                    true,
                    operacionInfo
                    ? "Procesado automáticamente"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoAutomatico));
                throw;
            }
        }

        /// <summary>
        /// Indica si la ejecucion del proceso es Manual.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        protected override async Task<ResultadoProceso> ProcesoManual(string folio)
        {
            try
            {
                var operacionInfo = await _ioperacionesGeneralesLaika.EjecutarPipelinesEstados(folio, false);

                return Resultado(
                    folio,
                    "MT103_IN",
                    operacionInfo,
                    false,
                    operacionInfo
                    ? "Estado a flujo manual"
                    : "Error actualizando estados");
            }
            catch (Exception ex)
            {
                LogHelper.Log(ex, nameof(ProcesoManual));
                throw;
            }
        }

        //Funciones Generales del MT103IN

        /// <summary>
        /// Verifca que puede ser automatizacion autorizada.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="planActualTrabajo"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<bool> MxnPuedeSerAutomatizacionAutorizada(string folio, DMasterWorkPlan planActualTrabajo)
        {
            try
            {
                Config.EscribirLog($"INFO|MT103_IN| Se comienza búsqueda del BIC emisor del mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());

                var enviarBic = await ObtenerBicEmisorMt(folio);

                if (string.IsNullOrWhiteSpace(enviarBic))
                {
                    Config.EscribirLog($"ERROR|MT103_IN| No se encontro un BIC emisor para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, $"MXN/MXP sin BIC emisor con cuenta vostro a debitar.");
                    return false;
                }

                Config.EscribirLog($"INFO|MT103_IN| Se extrajo el BIC emisor {enviarBic} al mensaje folio {folio}.", NombreServicio.LAIKA.ToString());

                var cuentaVostro = await CuentaBicAsociada(enviarBic);

                if (!cuentaVostro)
                {
                    Config.EscribirLog($"ERROR|MT103_IN| No se encontró la cuenta VOSTRO del BIC emisor {enviarBic} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, $"Mensaje sin cuenta VOSTRO en T24 en MXN para debitar.", true);
                    return false;
                }

                if (planActualTrabajo.num_cliente != 0)
                {
                    Config.EscribirLog($"INFO|MT103_IN| La operación es una transferencia interna, se operará automáticamente.", NombreServicio.LAIKA.ToString());
                    Config.EscribirLog($"INFO|MT103_IN| Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}.", NombreServicio.LAIKA.ToString());
                    return true;
                }

                Config.EscribirLog($"INFO|MT103_IN| Se encontró la cuenta VOSTRO del BIC emisor {enviarBic} para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT103_IN| Se procederá a buscar el esquema de comisión para la cuenta de {enviarBic}.", NombreServicio.LAIKA.ToString());

                var codigoCliente = await ObtenerClienteMensajeCodigo(folio);

                if (codigoCliente < 1)
                {
                    Config.EscribirLog($"ERROR|MT103_IN| No se encontró un código de cliente para el BIC {enviarBic}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, $"no se encontró un código cliente para la operación.", true);
                    return false;
                }

                Config.EscribirLog($"INFO|MT103_IN| Se procederá a buscar el tipo de comisión en el mensaje.", NombreServicio.LAIKA.ToString());
                var tipoComision = await _siftInMtCommun.ObtenerTipoComisionParaMensajePorFolio(folio);
                Config.EscribirLog($"INFO|MT103_IN| Se encontró el tipo de comisión {tipoComision.ToString()} en el mensaje.", NombreServicio.LAIKA.ToString());

                var comisionInfoEsquema = await ObtenerEsquemaComisionParaEstatusCodigoCliente(codigoCliente, tipoComision);

                if (comisionInfoEsquema == null)
                {
                    Config.EscribirLog($"ERROR|MT103_IN| No se encontró el esquema de comisión para la cuenta de: {enviarBic}.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, $"no se encontró un esquema de comisión para la operación.", true);
                    return false;
                }

                Config.EscribirLog($"INFO|MT103_IN| Se tiene el tipo de comisión: {comisionInfoEsquema.Type}, se cobrará un total de {comisionInfoEsquema.Total} {comisionInfoEsquema.Currency} de comisión.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT103_IN| Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}", NombreServicio.LAIKA.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MxnPuedeSerAutomatizacionAutorizada|Ocurre Excepcion:" + ex, NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }


        /// <summary>
        /// verifica si es moneda extranjera.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="planActualTrabajo"></param>
        /// <returns>bool</returns>
        private async Task<bool> MonedaExtranjeraPuedeSerAutomatizacionAutorizada(string folio, DMasterWorkPlan plan)
        {
            try
            {
                var operacionInfo = await _informacionOperacionMt.ObtenerInformacionDeLaOperacionActual(folio);

                if (string.IsNullOrWhiteSpace(operacionInfo.IssuingBank))
                    await _rechazo.RechazarMensajeConRazon(folio, "No fue posible determinar el banco emisor.");

                Config.EscribirLog($"INFO|MT103_IN| Se procede a validar el banco emisor del mensaje.", NombreServicio.LAIKA.ToString());

                switch (plan.cve_moneda)
                {
                    case "USD":
                        if (!USDCorrespndiente.Any(n => operacionInfo.IssuingBank.Contains(n)))
                        {
                            Config.EscribirLog($"ERROR|MT103_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.", NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "MT-103 USD no emitido por JP MORGAN o MUFG, NY");
                            return false;
                        }
                        break;
                    case "EUR":
                        if (!operacionInfo.IssuingBank.Contains(EURCorrespndiente[0]))
                        {
                            Config.EscribirLog($"ERROR|MT103_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.", NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "MT-103 JPY no emitido por JP MORGAN, LONDRES");
                            return false;
                        }
                        break;
                    case "JPY":
                        if (!operacionInfo.IssuingBank.Contains(JPYCorrespndiente[0]))
                        {
                            Config.EscribirLog($"ERROR|MT103_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.", NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "MT-103 JPY no emitido por JP MORGAN, TOKYO");
                            return false;
                        }
                        break;
                    case "GBP":
                        if (!operacionInfo.IssuingBank.Contains(GBPCorrespndiente[0]))
                        {
                            Config.EscribirLog($"ERROR|MT103_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.", NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "MT-103 JPY no emitido por JP MORGAN, LONDRES");
                            return false;
                        }
                        break;
                    case "CAD":
                        if (!operacionInfo.IssuingBank.Contains(CADCorrespndiente[0]))
                        {
                            Config.EscribirLog($"ERROR|MT103_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es un banco inválido.", NombreServicio.LAIKA.ToString());
                            await _rechazo.RechazarMensajeConRazon(folio, "MT-103 CAD no emitido por JP MORGAN, CANADA");
                            return false;
                        }
                        break;
                }

                Config.EscribirLog($"INFO|MT103_IN| El mensaje en {plan.cve_mensaje} proviene de {operacionInfo.IssuingBank} el cual es válido.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT103_IN| Se validará si se logró extraer una cuenta existe en el sistema.", NombreServicio.LAIKA.ToString());

                if (operacionInfo.CustomerAccount == -1)
                {
                    Config.EscribirLog($"ERROR|MT103_IN| No se logró localizar una cuenta en el sistema BTMUM.", NombreServicio.LAIKA.ToString());
                    await _rechazo.RechazarMensajeConRazon(folio, "Cuenta Inválida o inexistente en BTMUM", true);
                    return false;
                }

                Config.EscribirLog($"INFO|MT103_IN| Se procesaron exitosamente las reglas de ejecucion.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT103_IN| Se termina el procesamiento de reglas para autorizar automáticamente la operación con folio {folio}.", NombreServicio.LAIKA.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|MonedaExtranjeraPuedeSerAutomatizacionAutorizada|Ocurre Excepcion:" + ex, NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }

        }

        /// <summary>
        /// Obtencion de bic emisor MT.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<string> ObtenerBicEmisorMt(string folio)
        {
            try
            {
                return await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerPuedeSerAutomatizacionMt103, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                }).ContinueWith(t => t.Result?.Data?.FirstOrDefault()?.info);
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerBicEmisorMt|Ocurre Excepcion:" + ex, NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Obtencion de cuenta bic asociada.
        /// </summary>
        /// <param name="bic"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<bool> CuentaBicAsociada(string bic)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(bic)) return false;

                Config.EscribirLog($"INFO|MT103_IN| Se comienza busqueda de cuentas VOSTRO en el sistema.", NombreServicio.LAIKA.ToString());
                Config.EscribirLog($"INFO|MT103_IN| Se busca la cuenta VOSTRO en MXN para el BIC {bic}", NombreServicio.LAIKA.ToString());


                var resultado = await EjecutarStoredProcedureQuery<DStringGenerico>(ConstantesServicio.usp_ObtenerCuentaNostroPacsYMt103, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@bic", bic));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@moneda", "MXN"));
                });

                return resultado?.Data?.Any() == true ? true : false;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|CuentaBicAsociada|Ocurre Excepcion:" + ex, NombreServicio.LAIKA.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Obtencion de cliente mensaje codigo.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<int> ObtenerClienteMensajeCodigo(string folio)
        {
            try
            {
                Config.EscribirLog($"INFO|MT103_IN| Se va a buscar la cuenta VOSTRO para el mensaje con folio {folio}.", NombreServicio.LAIKA.ToString());

                var informacionOperacion = await _informacionOperacionMt.ObtenerInformacionDeLaOperacionActual(folio);

                if (string.IsNullOrEmpty(informacionOperacion.VostroBank))
                {
                    Config.EscribirLog($"ERROR|MT103_IN| No se encontró la cuenta VOSTRO.", NombreServicio.LAIKA.ToString());
                    return -1;
                }

                Config.EscribirLog(
                    $"INFO|MT103_IN| Se encontró la cuenta VOSTRO {informacionOperacion.VostroBank}.", NombreServicio.LAIKA.ToString());

                var cuentaSep =
                    informacionOperacion.VostroBank.Split('-');

                if (cuentaSep.Length < 3)
                    return -1;

                var cuentaComision = cuentaSep[2];

                Config.EscribirLog(
                    $"INFO|MT103_IN| Se va a buscar el código de cliente para la cuenta {cuentaComision}.", NombreServicio.LAIKA.ToString());

                var result = await EjecutarStoredProcedureQuery<DStringGeneral>(ConstantesServicio.usp_ObtenerCodigoClientePorCuenta,
                    cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cuenta", cuentaComision));
                    },
                    esVistasT24: true);

                if (!result.Exito)
                {
                    throw new InvalidOperationException(
                        result.MensajeError,
                        result.Exception);
                }

                var codigoCliente =
                    result.Data?.FirstOrDefault()?.CUSTOMER ?? -1;

                Config.EscribirLog(
                    $"INFO|MT103_IN| Se encontró el código de cliente {codigoCliente}.", NombreServicio.LAIKA.ToString());
                return codigoCliente;
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerClienteMensajeCodigo|Ocurre Excepcion:" + ex, NombreServicio.LAIKA.ToString());
                throw;
            }
        }

        /// <summary>
        /// Obtencion de esquema comision para estatus codigo cliente.
        /// </summary>
        /// <param name="codigoCliente"></param>
        /// <param name="tipoComision"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<DComisionEsquemaAfiliado> ObtenerEsquemaComisionParaEstatusCodigoCliente(int codigoCliente, ESwiftComision tipoComision)
        {
            try
            {
                return await EjecutarStoredProcedureQuery<DComisionEsquemaAfiliado>(ConstantesServicio.usp_ObtenerEsquemaComisionPorCliente, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@customerCode", codigoCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@commissionType", tipoComision.ToString()));
                }).ContinueWith(t => t.Result?.Data?.FirstOrDefault());
            }
            catch (Exception ex)
            {
                Config.EscribirLog("ERROR|ObtenerEsquemaComisionParaEstatusCodigoCliente|Ocurre Excepcion:" + ex, NombreServicio.LAIKA.ToString());
                throw;
            }
        }
    }
}

using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using Aplicacion.Servicios;
using Infraestructura.Repositorio;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace LaikaAutomator
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceHostBase
    {
        private SimpleInjector.Container _containerDI;
        private ConfiguracionServicio _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.LAIKA.ToString();
        protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
        protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServiceHost()
        {
        }

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

            _configuracionBase = new ConfiguracionServicio();

            //Inicializa la obtención de la configuración general y el logging...
            {
                MUFG_T24.Core.Configuracion.LecturaConfguracionGeneral.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = MUFG_T24.Core.Configuracion.LecturaConfguracionGeneral.ObtenerServicio(NombreServicio.LAIKA.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.RutaArchivoConfiguracion = $@"{EnumRutasGenerales.Path_Log_CMS}{NombreServicio.LAIKA.ToString()}\";
                _configuracionBase.PathLog = servicioMainConfiguracion.Log + servicioMainConfiguracion.NombreServicio;
                _configuracionBase.TimerEnabled = servicioMainConfiguracion.TimerEnable;
                _configuracionBase.configuracion = servicioMainConfiguracion.configuracion;
                _configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();

                RutasConfiguracion rutas = new RutasConfiguracion()
                {
                    MUFG_Servicios_Path_Log = _configuracionBase.PathLog
                };

                Logging.Init(rutas, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
                Logging.Write("INFO|OnStart|Leyendo archivo de configuración.");
            }

            //Registra la configuracion...
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);
            _containerDI.Register<ConfiguracionServicio>(() => (ConfiguracionServicio)_configuracionBase.Clone(), Lifestyle.Singleton);

            #region LAIKA

                _containerDI.Register<IParametrosOperaciones, ConsultaCamposBDTomi>(Lifestyle.Singleton);
                _containerDI.Register<IContorlServiciosAutomatizado, ContorlServiciosAutomatizado>(Lifestyle.Singleton);
                _containerDI.Register<IInformacionOperacionMt, InformacionOperacionMt>(Lifestyle.Singleton);
                _containerDI.Register<IInformacionOperacionMx, InformacionOperacionMx>(Lifestyle.Singleton);
                _containerDI.Register<IRechazo, Rechazo>(Lifestyle.Singleton);
                _containerDI.Register<IPrecondicionCheck, PrecondicionCheck>(Lifestyle.Singleton);
                _containerDI.Register<IPrecondicionCheckSpei, PrecondicionCheckSpei>(Lifestyle.Singleton);
                _containerDI.Register<IOperacionRepositorioLaikaQueue, OperacionRepositorioLaikaQueue>(Lifestyle.Singleton);
                _containerDI.Register<IOperacionesGeneralesLaika, OperacionesGeneralesLaika>(Lifestyle.Singleton);
                _containerDI.Register<ISwiftInMtComun, SwiftInMtComun>(Lifestyle.Singleton);
                _containerDI.Register<ISwiftInMxComun, SwiftInMxComun>(Lifestyle.Singleton);
                _containerDI.Register<IPlanTrabajoMaestro, PlanTrabajoMaestro>(Lifestyle.Singleton);
                _containerDI.Register<IPlanTrabajoMaestroDetalle, PlanTrabajoMaestroDetalle>(Lifestyle.Singleton);
                _containerDI.Register<IOperacionRepositorioMt103In, OperacionRepositorioMt103In>(Lifestyle.Singleton);
                _containerDI.Register<IOperacionRepositorioMt103Out, OperacionRepositorioMt103Out>(Lifestyle.Singleton);
                _containerDI.Register<IOperacionRepositorioMt202In, OperacionRepositorioMt202In>(Lifestyle.Singleton);
                _containerDI.Register<IOperacionRepositorioMt202CovIn, OperacionRepositorioMt202CovIn>(Lifestyle.Singleton);
                _containerDI.Register<IOperacionRepositorioPacs008In, OperacionRepositorioPacs008In>(Lifestyle.Singleton);
                _containerDI.Register<IOperacionRepositorioSwiftNoSoportado, OperacionRepositorioSwiftNoSoportado>(Lifestyle.Singleton);
                _containerDI.Register<ISwiftAutomatizacionFactory, SwiftAutomatizacionFactory>(Lifestyle.Singleton);

            #endregion LAIKA

            _containerDI.Register<Aplicacion.Interfaces.IOperacionServicio, OperacionServicio>(Lifestyle.Scoped);

            _containerDI.Verify();

        }

        /// <summary>
        /// Realiza la obtención inicial de parámetros (una sola vez).
        /// </summary>
        protected override async Task ParametrizacionesAsync(CancellationToken ct)
        {
            try
            {
                //Obtiene el proceso actual...
                IParametrizacion _parametrizacion = _containerDI.GetInstance<IParametrizacion>();

                Configuracion configMufgT24 = new Configuracion();
                Configuracion nuevaConfig = await _parametrizacion.ObtenerProcesoActualAsync(configMufgT24);

                _configuracionBase.Pid = nuevaConfig.Pid;
                _configuracionBase.NombreProceso = nuevaConfig.NombreProceso;
                _configuracionBase.NombreDisplay = nuevaConfig.NombreDisplay;


                //Marca el servicio en uso...
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
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
                    var operacionServicio = _containerDI.GetInstance<Aplicacion.Interfaces.IOperacionServicio>();
                    await operacionServicio.EjecutaProcedimientosAsync().ConfigureAwait(false);
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

﻿using MUFG_T24.Configuracion_utilidades;


namespace Aplicacion.Modelos
{
    /// <summary>
    /// Configuracion servicio.
    /// </summary>
    public class ConfiguracionServicio : Configuracion {}
}

﻿using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;
using System.ServiceProcess;
namespace LaikaAutomator
{
    /// <summary>
    /// Punto de entrada Main.
    /// </summary>
    internal static class Program
    {
        /// <summary>
        /// Punto de entrada Main.
        /// </summary>
        static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                NombreServicio.LAIKA.ToString(),
                () => new ServiceHost(),
                args,
                (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs));
        }

    }

}

LaikaAutomator.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{2E75D71C-FBB3-4CDF-ADD4-B5C3A43499C4}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>LaikaAutomator</RootNamespace>
    <AssemblyName>LaikaAutomator</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
    <Deterministic>true</Deterministic>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <PlatformTarget>x86</PlatformTarget>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.Numerics" />
    <Reference Include="System.Web.Extensions" />
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
    <ProjectReference Include="..\Aplicacion\Aplicacion.csproj">
      <Project>{93a21735-f0fa-4178-9458-51bf9ebee238}</Project>
      <Name>Aplicacion</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura\Infraestructura.csproj">
      <Project>{4e3c93b8-f795-461b-a31d-aad2046e9bb9}</Project>
      <Name>Infraestructura</Name>
    </ProjectReference>
  </ItemGroup>
  <ItemGroup>
    <BootstrapperPackage Include=".NETFramework,Version=v4.8">
      <Visible>False</Visible>
      <ProductName>Microsoft .NET Framework 4.8 %28x86 and x64%29</ProductName>
      <Install>true</Install>
    </BootstrapperPackage>
    <BootstrapperPackage Include="Microsoft.Net.Framework.3.5.SP1">
      <Visible>False</Visible>
      <ProductName>.NET Framework 3.5 SP1</ProductName>
      <Install>false</Install>
    </BootstrapperPackage>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

Aplicacion.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{93A21735-F0FA-4178-9458-51BF9EBEE238}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Aplicacion</RootNamespace>
    <AssemblyName>AplicacionLAIKA</AssemblyName>
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
    <PlatformTarget>x86</PlatformTarget>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=x86">
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
    <Compile Include="DTOs\DComisionEsquemaAfiliado.cs" />
    <Compile Include="DTOs\DControlAutomatizacion.cs" />
    <Compile Include="DTOs\DMasterWorkPlan.cs" />
    <Compile Include="DTOs\DMensajeLlave.cs" />
    <Compile Include="DTOs\DNFolioMq.cs" />
    <Compile Include="DTOs\DOperacionEntrante.cs" />
    <Compile Include="DTOs\DStringGeneral.cs" />
    <Compile Include="DTOs\DStringGenerico.cs" />
    <Compile Include="DTOs\SQueue.cs" />
    <Compile Include="Enums\ConstantesServicio.cs" />
    <Compile Include="Enums\DetallePlan.cs" />
    <Compile Include="Enums\EDetallePlan.cs" />
    <Compile Include="Enums\EFlujoTipoSwift.cs" />
    <Compile Include="Enums\EPlanTrabajoMaestroEstatus.cs" />
    <Compile Include="Enums\ESwiftComision.cs" />
    <Compile Include="Enums\SwiftFlujoResolvedor.cs" />
    <Compile Include="Enums\SwiftMensajeResolvedor.cs" />
    <Compile Include="Enums\ETipoMensajeSwift.cs" />
    <Compile Include="Enums\TipoOperacionSwift.cs" />
    <Compile Include="Helper\LogHelper.cs" />
    <Compile Include="Interfaces\IContorlServiciosAutomatizado.cs" />
    <Compile Include="Interfaces\IInformacionOperacionMt.cs" />
    <Compile Include="Interfaces\IInformacionOperacionMx.cs" />
    <Compile Include="Interfaces\IOperacionesGeneralesLaika.cs" />
    <Compile Include="Interfaces\IOperacionRepositorioLaikaQueue.cs" />
    <Compile Include="Interfaces\IOperacionRepositorioMt103In.cs" />
    <Compile Include="Interfaces\IOperacionRepositorioMt103Out.cs" />
    <Compile Include="Interfaces\IOperacionRepositorioMt202CovIn.cs" />
    <Compile Include="Interfaces\IOperacionRepositorioMt202In.cs" />
    <Compile Include="Interfaces\IOperacionRepositorioPacs008In.cs" />
    <Compile Include="Interfaces\IOperacionRepositorioSwift.cs" />
    <Compile Include="Interfaces\IOperacionRepositorioSwiftNoSoportado.cs" />
    <Compile Include="Interfaces\IOperacionServicio.cs" />
    <Compile Include="Interfaces\IParametrosOperaciones.cs" />
    <Compile Include="Interfaces\IPlanTrabajoMaestro.cs" />
    <Compile Include="Interfaces\IPlanTrabajoMaestroDetalle.cs" />
    <Compile Include="Interfaces\IPrecondicionCheck.cs" />
    <Compile Include="Interfaces\IPrecondicionCheckSpei.cs" />
    <Compile Include="Interfaces\IRechazo.cs" />
    <Compile Include="Interfaces\ISwiftAutomatizacionFactory.cs" />
    <Compile Include="Interfaces\ISwiftInMtComun.cs" />
    <Compile Include="Interfaces\ISwiftInMxComun.cs" />
    <Compile Include="Modelos\ConfiguracionServicio.cs" />
    <Compile Include="Modelos\ResultadoProceso.cs" />
    <Compile Include="OperacionServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <ItemGroup>
    <Folder Include="Servicios\" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

Infraestructura.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{4E3C93B8-F795-461B-A31D-AAD2046E9BB9}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Infraestructura</RootNamespace>
    <AssemblyName>InfraestructuraLAIKA</AssemblyName>
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
    <PlatformTarget>x86</PlatformTarget>
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
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
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
    <Compile Include="Repositorio\AsignacionEstadoGeneral.cs" />
    <Compile Include="Repositorio\AutorizacionEstadoGeneral.cs" />
    <Compile Include="Repositorio\ConsultaCamposBDTomi.cs" />
    <Compile Include="Repositorio\ContorlServiciosAutomatizado.cs" />
    <Compile Include="Repositorio\InformacionOperacionMt.cs" />
    <Compile Include="Repositorio\InformacionOperacionMx.cs" />
    <Compile Include="Repositorio\ISwiftContext.cs" />
    <Compile Include="Repositorio\OperacionesGeneralesLaika.cs" />
    <Compile Include="Repositorio\OperacionRepositorioBase.cs" />
    <Compile Include="Repositorio\OperacionRepositorioLaikaQueue.cs" />
    <Compile Include="Repositorio\OperacionRepositorioMt103In.cs" />
    <Compile Include="Repositorio\OperacionRepositorioMt103Out.cs" />
    <Compile Include="Repositorio\OperacionRepositorioMt202CovIn.cs" />
    <Compile Include="Repositorio\OperacionRepositorioMt202In.cs" />
    <Compile Include="Repositorio\OperacionRepositorioPacs008In.cs" />
    <Compile Include="Repositorio\OperacionRepositorioSwiftNoSoportado.cs" />
    <Compile Include="Repositorio\PlanTrabajoMaestro.cs" />
    <Compile Include="Repositorio\PlanTrabajoMaestroDetalle.cs" />
    <Compile Include="Repositorio\PrecondicionCheck.cs" />
    <Compile Include="Repositorio\PrecondicionCheckSpei.cs" />
    <Compile Include="Repositorio\Rechazo.cs" />
    <Compile Include="Repositorio\State.cs" />
    <Compile Include="Repositorio\SwiftAutomatizacionFactory.cs" />
    <Compile Include="Repositorio\SwiftContext.cs" />
    <Compile Include="Repositorio\SwiftInMtComun.cs" />
    <Compile Include="Repositorio\SwiftInMxComun.cs" />
    <Compile Include="Repositorio\SwiftUtilidades.cs" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\Aplicacion.csproj">
      <Project>{93A21735-F0FA-4178-9458-51BF9EBEE238}</Project>
      <Name>Aplicacion</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

packages.config
﻿<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="Castle.Core" version="5.2.1" targetFramework="net48" />
  <package id="Moq" version="4.20.72" targetFramework="net48" />
  <package id="MSTest.TestAdapter" version="2.2.10" targetFramework="net48" />
  <package id="MSTest.TestFramework" version="2.2.10" targetFramework="net48" />
  <package id="System.Runtime.CompilerServices.Unsafe" version="4.5.3" targetFramework="net48" />
  <package id="System.Threading.Tasks.Extensions" version="4.5.4" targetFramework="net48" />
</packages>


App.config
<?xml version="1.0" encoding="utf-8"?>
<configuration>
	<startup>
		<supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.8" />
	</startup>
	<appSettings>
		<add key="ClientSettingsProvider.ServiceUri" value="" />
	</appSettings>
	<system.web>
		<membership defaultProvider="ClientAuthenticationMembershipProvider">
			<providers>
				<add name="ClientAuthenticationMembershipProvider" type="System.Web.ClientServices.Providers.ClientFormsAuthenticationMembershipProvider, System.Web.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35" serviceUri="" />
			</providers>
		</membership>
		<roleManager defaultProvider="ClientRoleProvider" enabled="true">
			<providers>
				<add name="ClientRoleProvider" type="System.Web.ClientServices.Providers.ClientRoleProvider, System.Web.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35" serviceUri="" cacheTimeout="86400" />
			</providers>
		</roleManager>
	</system.web>
</configuration>


