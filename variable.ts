using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelo;
using Aplicacion.Servicios;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Infraestructura.Logging;
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
        private readonly IAlertas _alertas;
        private readonly IControlServicio _controlServicio;
        private readonly IParametrizacion _parametrizacion;
        private readonly IParametrizacionControlDao _recuperacionControlDao;
        private readonly DespachadorWorker _despachadorWorker;
        
        //Control del flujo
        private SemaphoreSlim _control = new SemaphoreSlim(0);

        //Control de workers
        private readonly object _estadoLock = new object();
        int cantidadActualWorkers = 0; //Cantidad actual de workers.
        int cantidadMaximaGlobal; //Cantidad máxima de workers.
        Dictionary<string, Aplicacion.Modelo.ControlServicio> estadoPorServicio
            = new Dictionary<string, Aplicacion.Modelo.ControlServicio>(); //Control de servicios.
        Dictionary<(string Servicio, string Tipo), ControlTipo> estadoPorTipo
            = new Dictionary<(string Servicio, string Tipo), ControlTipo>(); //Control de paquetes.

        //Control de paquetes
        private readonly SemaphoreSlim _fetchLock = new SemaphoreSlim(1, 1);
        List<Paquete> _paquetesPendientes = new List<Paquete>();
        List<Paquete> _paquetesEnProceso = new List<Paquete>();

        /// <summary>
        /// Constructor.
        /// </summary>
        public OperacionServicio(Configuracion configuracion, IAlertas alertas, IControlServicio controlServicio, IParametrizacion parametrizacion, IParametrizacionControlDao recuperacionControlDao, DespachadorWorker despachadorWorker)
        {
            _configuracion = configuracion;
            _alertas = alertas;
            _controlServicio = controlServicio;
            _parametrizacion = parametrizacion;
            _recuperacionControlDao = recuperacionControlDao;
            _despachadorWorker = despachadorWorker;
            cantidadMaximaGlobal = (_configuracion.LimiteConcurrencia.HasValue && _configuracion.LimiteConcurrencia.Value > 0)
                ? _configuracion.LimiteConcurrencia.Value
                : 10;
        }

        /// <summary>
        /// Obtiene los parametros iniciales de control y llama al ciclo de inicio.
        /// </summary>
        public async Task<bool> IniciarDespachadorAsync(CancellationToken ct)
        {
            try
            {
                //Marca la ejecución del servicio...
                await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct);

                //Obtiene los parámetros del sistema...
                await RefrescarParametrosOperacionAsync(ct);

                //Refresco inicial de control dinámico
                await RefrescarControlDespachadorAsync(ct);

                //Hace sondeo profundo (inicial)...
                await ObtenerPendientesAsync(ct);

                //Wake-up inicial...
                Despertar();

                //Timer de seguridad (safety) y busqueda de actualizaciones...
                Logging.Write("INFO|Despachador|Iniciando loop principal");
                _ = Task.Run(async () =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        try
                        {
                             await Task.Delay(TimeSpan.FromSeconds(30), ct);

                            await RefrescarParametrosOperacionAsync(ct);

                            bool enHorario = await ValidarHorarioLecturaAsync(ct);
                            if (!enHorario) continue;

                            Logging.Write("DEBUG|Despachador|Refrescando control del despachador...");
                            await RefrescarControlDespachadorAsync(ct);

                            Logging.Write("DEBUG|Despachador|Obteniendo pendientes del Safety timer...");
                            await ObtenerPendientesAsync(ct);

                            if (ExistenPendientesEnMemoria())
                            {
                                Logging.Write("DEBUG|Despachador|Hay pendientes tras refresh, despertando...");
                                Despertar();
                            }
                        }
                        catch (Exception ex) when (!(ex is OperationCanceledException))
                        {
                            Logging.Write($"ERROR|SafetyLoop|{ex}");
                        }
                    }
                }, ct);

                // Loop principal...
                _ = Task.Run(async () => await CicloDespachadorAsync(ct), ct);

                return true;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logging.Write($"ERROR|Ejecuta_Procedimientos|Se presenta exception: {ex}");
                await _alertas.InsertarAlerta(
                    null,
                    null,
                    _configuracion.NombreServicio,
                    ConstantesClaveAviso.Critico,
                    null,
                    _configuracion.Pid.Value,
                    _configuracion.NombreProceso,
                    ex);
            }
            return false;
        }

        /// <summary>
        /// Refresca los datos de control del despachador con nuevos datos recuperados.
        /// </summary>
        private async Task RefrescarControlDespachadorAsync(CancellationToken ct)
        {
            List<ControlServicioDto> serviciosControl =
                (await _recuperacionControlDao.ObtenerControlServicioAsync(ct))?.ToList()
                ?? new List<ControlServicioDto>();

            List<ControlTipoDto> tiposControl =
                (await _recuperacionControlDao.ObtenerControlTipoAsync(ct))?.ToList()
                ?? new List<ControlTipoDto>();

            lock (_estadoLock)
            {
                var pendientesPorTipo = _paquetesPendientes
                    .GroupBy(p => (p.Servicio, p.Tipo))
                    .ToDictionary(g => g.Key, g => g.Count());

                var serviciosVigentes = new HashSet<string>(
                    serviciosControl.Select(x => x.Servicio),
                    StringComparer.OrdinalIgnoreCase);

                foreach (var dto in serviciosControl)
                {
                    if (estadoPorServicio.TryGetValue(dto.Servicio, out var existente))
                    {
                        int activos = existente.Activos;

                        estadoPorServicio[dto.Servicio] = new Aplicacion.Modelo.ControlServicio(dto)
                        {
                            Activos = activos,
                            Habilitado = true
                        };
                    }
                    else
                    {
                        estadoPorServicio[dto.Servicio] = new Aplicacion.Modelo.ControlServicio(dto)
                        {
                            Habilitado = true
                        };
                    }
                }

                //Servicios que ya no existen en control
                foreach (var servicioExistente in estadoPorServicio.Keys.ToList())
                {
                    if (!serviciosVigentes.Contains(servicioExistente))
                    {
                        var servicio = estadoPorServicio[servicioExistente];

                        // Si aún tiene workers activos, NO lo borres todavía
                        if (servicio.Activos > 0)
                        {
                            servicio.Habilitado = false;
                        }
                        else
                        {
                            estadoPorServicio.Remove(servicioExistente);
                        }
                    }
                }

                var tiposVigentes = new HashSet<(string Servicio, string Tipo)>(
                    tiposControl.Select(x => (x.Servicio, x.Tipo)));

                foreach (var dto in tiposControl)
                {
                    var key = (dto.Servicio, dto.Tipo);

                    int activos = 0;
                    DateTime ultimaEjecucion = DateTime.MinValue;

                    if (estadoPorTipo.TryGetValue(key, out var existente))
                    {
                        activos = existente.Activos;
                        ultimaEjecucion = existente.UltimaEjecucion;
                    }

                    estadoPorTipo[key] = new ControlTipo
                    {
                        Servicio = dto.Servicio,
                        Tipo = dto.Tipo,
                        Maximos = dto.CantidadMaxima,
                        Prioridad = dto.Prioridad,
                        Activos = activos,
                        UltimaEjecucion = ultimaEjecucion,
                        Pendientes = pendientesPorTipo.TryGetValue(key, out int pendientes) ? pendientes : 0,
                        Habilitado = true
                    };
                }

                //Tipos que ya no existen en control
                foreach (var key in estadoPorTipo.Keys.ToList())
                {
                    if (!tiposVigentes.Contains(key))
                    {
                        var tipo = estadoPorTipo[key];

                        //Si aún tiene activos o paquetes pendientes, se deshabilita...
                        if (tipo.Activos > 0 || tipo.Pendientes > 0)
                        {
                            tipo.Habilitado = false;
                            tipo.Maximos = 0;
                        }
                        else
                        {
                            estadoPorTipo.Remove(key);
                        }
                    }
                }

                _paquetesPendientes.RemoveAll(p =>
                {
                    var key = (p.Servicio, p.Tipo);

                    bool servicioValido =
                        estadoPorServicio.TryGetValue(p.Servicio, out var srv)
                        && srv.Habilitado;

                    bool tipoValido =
                        estadoPorTipo.TryGetValue(key, out var tipo)
                        && tipo.Habilitado;

                    return !servicioValido || !tipoValido;
                });

                var pendientesActualizados = _paquetesPendientes
                    .GroupBy(p => (p.Servicio, p.Tipo))
                    .ToDictionary(g => g.Key, g => g.Count());

                foreach (var key in estadoPorTipo.Keys.ToList())
                {
                    estadoPorTipo[key].Pendientes =
                        pendientesActualizados.TryGetValue(key, out int pendientes) ? pendientes : 0;
                }
            }
        }

        /// <summary>
        /// Obtiene las operaciones nuevas y las filtra para crear los paquetes lógicos.
        /// </summary>
        private async Task ObtenerPendientesAsync(CancellationToken ct)
        {
            await _fetchLock.WaitAsync(ct);

            try
            {
                var nuevasOperacionesPendientes =
                    (await _recuperacionControlDao.ObtenerOperacionesPendientesAsync(cantidadMaximaGlobal, ct))?.ToList()
                    ?? new List<OperacionDto>();

                Logging.Write($"DEBUG|Despachador|Se encontraron {nuevasOperacionesPendientes.Count} operaciones pendientes...");

                if (nuevasOperacionesPendientes.Count == 0)
                    return;

                var nuevasOperaciones = nuevasOperacionesPendientes
                    .Where(o => !FiltraTomadas(o))
                    .ToList();

                Logging.Write($"DEBUG|Despachador|Solo {nuevasOperaciones.Count} son operaciones nuevas (en memoria)...");

                if (nuevasOperaciones.Count == 0)
                    return;

                var paquetesLogicos = nuevasOperaciones
                    .GroupBy(o => (o.Servicio, o.Tipo_Operacion))
                    .SelectMany(g =>
                        g.ChunkBy(_configuracion.CantidadPaquete.Value)
                         .Select(chunk => new Paquete
                         {
                             Servicio = g.Key.Servicio,
                             Tipo = g.Key.Tipo_Operacion,
                             Operaciones = chunk
                         }))
                    .ToList();

                await AgregarNuevosPaquetes(paquetesLogicos);
            }
            finally
            {
                _fetchLock.Release();
            }

        }

        /// <summary>
        /// Agrega los paquetes logicos a los pendientes en memoria...
        /// </summary>
        private Task AgregarNuevosPaquetes(List<Paquete> paquetesLogicos)
        {
            Logging.Write($"DEBUG|Despachador|Agregando paquete a memoria...");
            bool hayNuevosPendientes = false;

            lock (_estadoLock)
            {
                if (_paquetesPendientes.Count >= _configuracion.LimitePaquetesPendientes.Value)
                {
                    Logging.Write("AVISO|Se están omitiendo paquetes por exceso en memoria. Primero debe despacharse lo que está en la cola.");
                    return Task.CompletedTask;
                }

                foreach (var paquete in paquetesLogicos)
                {
                    var key = (paquete.Servicio, paquete.Tipo);

                    if (!estadoPorServicio.TryGetValue(paquete.Servicio, out var servicio) || !servicio.Habilitado)
                    {
                        Logging.Write($"AVISO|Despachador|Paquete ignorado: servicio no controlado o deshabilitado {paquete.Servicio}");
                        continue;
                    }

                    if (!estadoPorTipo.TryGetValue(key, out var controlTipo) || !controlTipo.Habilitado)
                    {
                        Logging.Write($"AVISO|Despachador|Paquete ignorado: no existe control para {key.Servicio}/{key.Tipo}");
                        continue;
                    }

                    _paquetesPendientes.Add(paquete);
                    controlTipo.Pendientes++;
                    hayNuevosPendientes = true;
                }
            }

            if (hayNuevosPendientes)
                Despertar();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Realiza y mantiene el ciclo activo del despachador maestro.
        /// </summary>       
        private async Task CicloDespachadorAsync(CancellationToken ct)
        {
            Logging.Write("DEBUG|CicloDespachador|Loop iniciado...");
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    //Se queda esperando indefinidamente hasta recibir una señal de despertar...
                    await _control.WaitAsync(ct);

                    bool enHorario = await ValidarHorarioLecturaAsync(ct);
                    if (!enHorario) continue;

                    //Si no hay workers disponibles...
                    Logging.Write($"DEBUG|CicloDespachador|Revisando si hay Workers disponibles...");
                    if (WorkersDisponiblesGlobal() <= 0)
                        continue;

                    //Revisa si hay pendientes en memoria...
                    Logging.Write($"DEBUG|CicloDespachador|Revisando si hay pendientes en memoria...");
                    if (!ExistenPendientesEnMemoria())
                        continue;

                    //Se despachará un worker de acuerdo a prioridad...
                    Logging.Write($"DEBUG|CicloDespachador|Hay pendientes, se continua a despachar en un workers separado...");
                    LogicaWorker(ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex) when (!(ex is OperationCanceledException))
                {
                    Logging.Write($"ERROR|CicloDespachador|{ex}");
                }
            }

            //Marca el termino del servicio...
            Logging.Write($"DEBUG|CicloDespachador|Se esta terminando el servicio...");
            await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio);
        }

        /// <summary>
        /// Logica que determina que servicio-tipo toca despachar.
        /// </summary>
        private void LogicaWorker(CancellationToken ct)
        {
            Logging.Write($"DEBUG|Despachador|Hay {WorkersDisponiblesGlobal()} Workers disponibles...");
            Logging.Write($"DEBUG|Despachador|Se elegira un candidato de la memoria...");
            while (WorkersDisponiblesGlobal() > 0)
            {
                ControlTipo candidato;

                lock (_estadoLock)
                {
                    candidato = estadoPorTipo.Values
                        .Where(e => e.Habilitado &&
                                    e.Pendientes > 0 &&
                                    e.Activos < e.Maximos &&
                                    estadoPorServicio.TryGetValue(e.Servicio, out var srv) &&
                                    srv.TieneCapacidad)
                        .OrderBy(e => estadoPorServicio[e.Servicio].Prioridad)
                        .ThenBy(e => e.Prioridad)
                        .ThenBy(e => e.Ocupacion)
                        .ThenByDescending(e => (DateTime.UtcNow - e.UltimaEjecucion).TotalSeconds)
                        .FirstOrDefault();
                }

                if (candidato == null)
                    break;

                if (!IntentarReservarPaquete(candidato, out var paquete, out var servicioSnapshot))
                    break;

                LanzarWorker(paquete, candidato, servicioSnapshot, ct);
            }
        }

        /// <summary>
        /// Lanzador de worker a despachar (worker: obtencion, recuperacion y tcp client).
        /// </summary>
        /// <param name="candidato"></param>
        private void LanzarWorker(Paquete paquete, ControlTipo candidato, Aplicacion.Modelo.ControlServicio servicioSnapshot, CancellationToken ct)
        {
            Logging.Write($"[{candidato.Servicio}]Se lanzará un worker de tipo {candidato.Tipo} | {cantidadActualWorkers}/{cantidadMaximaGlobal}");
            _ = Task.Run(async () =>
            {
                try
                {
                    await _despachadorWorker.DespacharPaqueteAsync(
                        paquete,
                        servicioSnapshot.Ip,
                        servicioSnapshot.Puerto,
                        ct);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    Logging.Write($"ERROR|Worker|{ex}");
                }
                finally
                {
                    bool hayPendientesEnMemoria;

                    lock (_estadoLock)
                    {

                        var keysPaquete = GetKeys(paquete);
                        _paquetesEnProceso.RemoveAll(p =>
                            p.Operaciones.Any(o => keysPaquete.Contains(GetKey(o))));
                        _paquetesPendientes.RemoveAll(p =>
                            p.Operaciones.Any(o => keysPaquete.Contains(GetKey(o))));

                        var key = (candidato.Servicio, candidato.Tipo);

                        if (estadoPorTipo.TryGetValue(key, out var tipo))
                        {
                            tipo.Activos = Math.Max(0, tipo.Activos - 1);

                            //Recalcula pendientes reales de ese tipo desde memoria.
                            tipo.Pendientes = _paquetesPendientes.Count(p =>
                                p.Servicio == candidato.Servicio &&
                                p.Tipo == candidato.Tipo);
                        }

                        if (estadoPorServicio.TryGetValue(candidato.Servicio, out var srv))
                            srv.Activos = Math.Max(0, srv.Activos - 1);

                        cantidadActualWorkers = Math.Max(0, cantidadActualWorkers - 1);
                        hayPendientesEnMemoria = _paquetesPendientes.Count > 0;
                    }

                    if (hayPendientesEnMemoria && !ct.IsCancellationRequested)
                    {
                        int delayMs = _configuracion.DelayEntreDespachoMs ?? 300;
                        if (delayMs > 0)
                            await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct);

                        Despertar();
                    }
                }
            }, ct);
        }



        /// -------------
        /// Utilidades
        /// -------------

        /// <summary>
        /// Despierta el control (mientras que no haya cola acumulada).
        /// </summary>
        public void Despertar()
        {
            Logging.Write($"DEBUG|Despertar|CurrentCount antes: {_control.CurrentCount}");
            if (_control.CurrentCount == 0)
                _control.Release();
            Logging.Write("DEBUG|Despertar|Release ejecutado");
        }

        /// <summary>
        /// Obtiene la cantidad disponible (calculo simple).
        /// </summary>
        private int WorkersDisponiblesGlobal()
        {
            Logging.Write($"DEBUG|WorkersDisponiblesGlobal|{cantidadMaximaGlobal}-{cantidadActualWorkers}={cantidadMaximaGlobal - cantidadActualWorkers}.");
            if (!System.Threading.Monitor.TryEnter(_estadoLock, TimeSpan.FromSeconds(2)))
            {
                Logging.Write("WARN|WorkersDisponiblesGlobal|Timeout al adquirir _estadoLock (2s). Se retorna 0 por contención de lock.");
                return 0;
            }
            try
            {
                return cantidadMaximaGlobal - cantidadActualWorkers;
            }
            finally
            {
                System.Threading.Monitor.Exit(_estadoLock);
            }
        }

        /// <summary>
        /// Verifica si hay algun pendiente en memoria.
        /// </summary>
        private bool ExistenPendientesEnMemoria()
        {
            lock (_estadoLock)
            {
                return _paquetesPendientes.Count > 0;
            }
        }

        /// <summary>
        /// Filtra una operación de la lista de pendientes y en proceso.
        /// </summary>
        bool FiltraTomadas(OperacionDto op)
        {
            var key = GetKey(op);

            lock (_estadoLock)
            {
                return _paquetesPendientes.Any(p =>
                           p.Operaciones.Any(o => GetKey(o) == key))
                    || _paquetesEnProceso.Any(p =>
                           p.Operaciones.Any(o => GetKey(o) == key));
            }
        }

        /// <summary>
        /// Obtiene la clave unica de la operación.
        /// </summary>
        private string GetKey(OperacionDto o)
        {
            return $"{o.Servicio}|{o.Tipo_Operacion}|{o.Folio}";
        }
        private HashSet<string> GetKeys(Paquete paquete)
        {
            return new HashSet<string>(
                paquete.Operaciones.Select(GetKey),
                StringComparer.Ordinal);
        }

        /// <summary>
        /// Intenta reservar un paquete para despachar, actualizando el estado de control.
        /// </summary>
        private bool IntentarReservarPaquete(ControlTipo candidato, out Paquete paquete, out Aplicacion.Modelo.ControlServicio servicioSnapshot)
        {
            paquete = null;
            servicioSnapshot = null;

            var key = (candidato.Servicio, candidato.Tipo);

            lock (_estadoLock)
            {
                if (cantidadActualWorkers >= cantidadMaximaGlobal)
                    return false;

                if (!estadoPorServicio.TryGetValue(candidato.Servicio, out var servicio) || !servicio.Habilitado || !servicio.TieneCapacidad)
                    return false;

                if (!estadoPorTipo.TryGetValue(key, out var tipo) || !tipo.Habilitado || tipo.Pendientes <= 0 || tipo.Activos >= tipo.Maximos)
                    return false;

                paquete = _paquetesPendientes
                    .FirstOrDefault(p => p.Servicio == candidato.Servicio && p.Tipo == candidato.Tipo);

                if (paquete == null)
                    return false;

                _paquetesPendientes.Remove(paquete);
                _paquetesEnProceso.Add(paquete);

                tipo.Activos++;
                tipo.UltimaEjecucion = DateTime.UtcNow;
                tipo.Pendientes = Math.Max(0, tipo.Pendientes - 1);

                servicio.Activos++;
                cantidadActualWorkers++;

                servicioSnapshot = servicio;
                return true;
            }
        }

        /// <summary>
        /// Refresca los parametros de operación del servicio.
        /// </summary>
        private async Task RefrescarParametrosOperacionAsync(CancellationToken ct)
        {
            _configuracion.FechaSistema = await _parametrizacion.ObtenerFechaSistemaAsync(ct);
            _configuracion.EsDiaHabil = await _controlServicio.EsDiaHabil(ct);

            MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion[] parametrosSistema = new[]
            {
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesParametrosSistema.HoraLecInicio, Requerido = true, Sensible = false },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesParametrosSistema.HoraLecFin, Requerido = true, Sensible = false },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesParametrosSistema.HoraOPInicio, Requerido = true, Sensible = false },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesParametrosSistema.HoraOPFin, Requerido = true, Sensible = false },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesServicio.ParametroWsT24MwCompany, Requerido = true, Sensible = false },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesServicio.ParametroWsT24MwUsername, Requerido = true, Sensible = false },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesServicio.ParametroWsT24MwPassword, Requerido = true, Sensible = true },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesServicio.wsT24Endpoint, Requerido = true, Sensible = false },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesServicio.ParametroSinFondosHoraValida, Requerido = true, Sensible = false },
                new MUFG_T24.Inicializacion_dtos.ParametroSistemaDefinicion { Clave = ConstantesServicio.ParametroSinFondosTiempoEspera, Requerido = true, Sensible = false },
            };

            _configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(parametrosSistema, ct);
        }

        /// <summary>
        /// Valida el horario de operación.
        /// </summary>
        private async Task<bool> ValidarHorarioLecturaAsync(CancellationToken ct)
        {
            TimeSpan timeHoraSistema = _parametrizacion.ObtenerHoraSistema();
            TimeSpan horaLecturaInicio = ConversionesComunes.ParseTimeSpanRequerido(
                _configuracion.ParametrosSistema[ConstantesGenerales.HoraLecInicio],
                ConstantesGenerales.HoraLecInicio);
            TimeSpan horaLecturaFin = ConversionesComunes.ParseTimeSpanRequerido(
                _configuracion.ParametrosSistema[ConstantesGenerales.HoraLecFin],
                ConstantesGenerales.HoraLecFin);
            if (timeHoraSistema < horaLecturaInicio
                || timeHoraSistema > horaLecturaFin)
            {
                if (_configuracion.EsHorarioLectura is null || _configuracion.EsHorarioLectura.Value)
                {
                    Logging.Write("ERROR|Ejecuta_Procedimientos|El servicio está fuera de horario.");
                    await _alertas.InsertarAlerta(
                        null,
                        null,
                        _configuracion.NombreServicio,
                        ConstantesClaveAviso.Error,
                        "El servicio está fuera de horario",
                        _configuracion.Pid.Value,
                        _configuracion.NombreProceso,
                        new Exception($"El servicio '{_configuracion.NombreServicio}' está fuera de horario."));
                }
                _configuracion.EsHorarioLectura = false;
                return false;
            }
            _configuracion.EsHorarioLectura = true;
            return true;
        }

    }
}


using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Aplicacion.Modelo;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Enums_utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios
{
    /// <summary>
    /// Clase que contiene el flujo de procesamiento de despacho (Despachador Worker).
    /// </summary>
    public class DespachadorWorker
    {
        private static int _workerSequence;
        private static int _paqueteSequence;

        private sealed class AnalisisOperaciones
        {
            public HashSet<long> OperacionesNegadas { get; } = new HashSet<long>();
            public HashSet<long> OperacionesTMD { get; set; }
            public Dictionary<long, OperacionDto> OperacionesTmdIndex { get; set; }
            public List<long> OperacionesFuturas { get; } = new List<long>();
        }

        Configuracion _configuracion;
        IAlertas _alertas;
        private IRecuperacionDatos _sondeoDao;
        private IParametrizacion _parametrizacion;
        private IEnvioDatos _envioDatos;
        private IwsT24 _wsT24;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="sondeoDao"></param>
        public DespachadorWorker(Configuracion Configuracion, IAlertas alertas, IParametrizacion parametrizacion, IRecuperacionDatos sondeoDao, IEnvioDatos envioDatos, IwsT24 wsT24)
        {
            _configuracion = Configuracion;
            _alertas = alertas;
            _parametrizacion = parametrizacion;
            _sondeoDao = sondeoDao;
            _envioDatos = envioDatos;
            _wsT24 = wsT24;
        }

        /// <summary>
        /// Despacha el paquete asignado al servicio correspondiente.
        /// Al finalizar, retorna metricas.
        /// </summary>
        public async Task<WorkerResultadoDto> DespacharPaqueteAsync(Paquete paquete, string ip, int puerto, CancellationToken ct)
        {
            string contextoDespacho = CrearContextoDespacho(paquete);

            Logging.WriteConContexto(contextoDespacho, $"INFO|Despachador|Un worker de tipo {paquete.Servicio}-{paquete.Tipo} a ip {ip}, puerto {puerto}");
            Logging.WriteConContexto(contextoDespacho, $"INFO|Envia_Folios|El servicio '{paquete.Servicio}' será despachado por el puerto {puerto}, folios [{Comunes.GenerarCadenaArreglo(paquete.Operaciones.Select(x => x.Folio.ToString()).ToArray())}].");

            var resultado = new WorkerResultadoDto { Servicio = paquete.Servicio, Tipo = paquete.Tipo, Claimed = true };

            try
            {
                if (!await ValidarVentanaT24Async(paquete, resultado, ct, contextoDespacho))
                    return resultado;

                int fechaSistema = ConversionesComunes.ParseEnteroRequerido(_configuracion.FechaSistema, nameof(_configuracion.FechaSistema));
                var analisis = ClasificarOperaciones(paquete, fechaSistema, contextoDespacho);

                await ProcesarOperacionesTmdAsync(paquete, analisis, ct, contextoDespacho);
                await ProcesarOperacionesFuturasAsync(paquete, analisis, ct, contextoDespacho);

                var operacionesFiltradas = paquete.Operaciones
                    .Where(o => !analisis.OperacionesNegadas.Contains(o.Folio))
                    .ToList();

                if (operacionesFiltradas.Count == 0)
                    return resultado;

                var detalles = operacionesFiltradas
                    .Select(o => new SolicitudOperacionDto
                    {
                        Folio = o.Folio,
                        TipoOperacion = o.Tipo_Operacion
                    })
                    .ToList();

                Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|Construyendo el paquete de envio...");
                var peticion = new SolicitudPaqueteDto
                {
                    Servicio = paquete.Servicio,
                    Tipo = paquete.Tipo,
                    PID = _configuracion.Pid.Value,
                    EnviadoEn = DateTime.UtcNow,
                    Operaciones = detalles,
                    Version = 1,
                };
                var json = JsonConvert.SerializeObject(peticion);

                Logging.WriteConContexto(contextoDespacho, $"INFO|Despachador|Enviando la petición a {ip}:{puerto}...");
                int timeoutMs = _configuracion.TimeoutDespachadoMs ?? 30000;
                RespuestaPaqueteDto resp = await _envioDatos.EnviarPeticionAsync(ip, puerto, json, TimeSpan.FromMilliseconds(timeoutMs), ct, contextoDespacho);

                if (resp is null)
                {
                    resultado.Claimed = false;
                    resultado.ReturnedToPending = true;
                    resultado.Error = "No se recibió respuesta del servicio despachado";
                    return resultado;
                }

                if (resp.Resultado == (int)ConstantesRespuestaResultadoDespachador.OK)
                {
                    resultado.Claimed = true;
                }
                else if (resp.Resultado == (int)ConstantesRespuestaResultadoDespachador.PARCIAL)
                {
                    resultado.Claimed = false;
                    resultado.ReturnedToPending = true;
                    resultado.Error = "Resultado PARCIAL en despacho";
                }
                else
                {
                    resultado.Claimed = false;
                    resultado.ReturnedToPending = true;
                    resultado.Error = "Resultado ERROR en despacho";
                }

                if (resp.Errores != null && resp.Errores.Count > 0)
                {
                    string errorDetalle = string.Join(" | ", resp.Errores
                        .Select(e => $"Folio:{e.Folio};Codigo:{e.Codigo};Mensaje:{e.Mensaje}"));
                    resultado.Error = string.IsNullOrWhiteSpace(resultado.Error)
                        ? errorDetalle
                        : $"{resultado.Error} | {errorDetalle}";
                }

                return resultado;
            }
            catch (OperationCanceledException)
            {
                resultado.Cancelled = true;
                return resultado;
            }
            catch (Exception ex)
            {
                resultado.Error = ex.Message;
                throw;
            }
        }

        private async Task<bool> ValidarVentanaT24Async(Paquete paquete, WorkerResultadoDto resultado, CancellationToken ct, string contextoDespacho)
        {
            if (!string.Equals(paquete.Servicio, ConstantesServicios.T24, StringComparison.Ordinal))
                return true;

            bool esDiaHabil = _configuracion.EsDiaHabil == true;

            Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|Validando día habil...");
            if (!esDiaHabil)
            {
                if (_configuracion.EnDiaHabil is null || _configuracion.EnDiaHabil.Value != esDiaHabil)
                {
                    Logging.WriteConContexto(contextoDespacho, "ERROR|Ejecuta_Procedimientos|Hoy es un día inhábil.");
                    await _alertas.InsertarAlerta(
                        null,
                        null,
                        _configuracion.NombreServicio,
                        ConstantesClaveAviso.Error,
                        "Hoy es un día inhábil",
                        _configuracion.Pid.Value,
                        _configuracion.NombreProceso,
                        new Exception("Hoy es un día inhábil."));
                }
                resultado.Error = "Día inhábil";
                resultado.Claimed = false;
                _configuracion.EnDiaHabil = false;
                return false;
            }
            _configuracion.EnDiaHabil = true;

            Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|Validando el horario de operación..");
            TimeSpan timeHoraSistema = _parametrizacion.ObtenerHoraSistema();
            TimeSpan horaOperacionInicio = ConversionesComunes.ParseTimeSpanRequerido(
                _configuracion.ParametrosSistema[ConstantesParametrosSistema.HoraOPInicio],
                ConstantesParametrosSistema.HoraOPInicio);
            TimeSpan horaOperacionFin = ConversionesComunes.ParseTimeSpanRequerido(
                _configuracion.ParametrosSistema[ConstantesParametrosSistema.HoraOPFin],
                ConstantesParametrosSistema.HoraOPFin);
            if ((timeHoraSistema < horaOperacionInicio
                || timeHoraSistema > horaOperacionFin) || !esDiaHabil)
            {
                if (_configuracion.EsHorarioOperacion is null || _configuracion.EsHorarioOperacion.Value)
                {
                    Logging.WriteConContexto(contextoDespacho, $"ERROR|Envia_Folios|El servicio {paquete.Servicio} está fuera de horario");
                    await _alertas.InsertarAlerta(
                        null,
                        null,
                        _configuracion.NombreServicio,
                        ConstantesClaveAviso.Error,
                        $"ERROR|Envia_Folios|El servicio {paquete.Servicio} está fuera de horario",
                        _configuracion.Pid.Value,
                        _configuracion.NombreProceso,
                        new Exception($"El servicio {paquete.Servicio} está fuera de horario"));
                }
                resultado.Error = "Fuera de horario";
                resultado.Claimed = false;
                _configuracion.EsHorarioOperacion = false;
                return false;
            }
            _configuracion.EsHorarioOperacion = true;
            return true;
        }

        private AnalisisOperaciones ClasificarOperaciones(Paquete paquete, int fechaSistema, string contextoDespacho)
        {
            var analisis = new AnalisisOperaciones();

            Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|Revisando el tipo de servicio...");
            if (!string.Equals(paquete.Servicio, ConstantesServicios.Lectura, StringComparison.Ordinal)
                && !string.Equals(paquete.Servicio, ConstantesServicios.HotScan, StringComparison.Ordinal))
            {
                Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|No es Lectura ni HotScan, asi que se haran validaciones sobre la operación...");

                foreach (var operacion in paquete.Operaciones)
                {
                    if (operacion.Fec_Valor is null)
                    {
                        Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|La operación no tiene fecha valor...");
                        operacion.Fec_Valor = fechaSistema;
                    }

                    if (string.Equals(paquete.Servicio, ConstantesServicios.T24, StringComparison.Ordinal)
                        && string.Equals(operacion.Tipo_Operacion, TipoOperacion.TMD, StringComparison.Ordinal))
                    {
                        Logging.WriteConContexto(contextoDespacho, "INFO|procesa_operaciones_planTrabajo|Se encontraron folios TMD. Servicio T24");

                        if (analisis.OperacionesTMD == null)
                        {
                            analisis.OperacionesTMD = new HashSet<long>();
                            analisis.OperacionesTmdIndex = new Dictionary<long, OperacionDto>();
                        }

                        if (analisis.OperacionesTMD.Add(operacion.Folio))
                            analisis.OperacionesTmdIndex[operacion.Folio] = operacion;
                    }
                    else if (operacion.Fec_Valor.Value < fechaSistema)
                    {
                        Logging.WriteConContexto(contextoDespacho, $"ERROR|procesa_operaciones_planTrabajo|Se desea procesar una operación al {operacion.Fec_Valor.Value}.");
                        analisis.OperacionesNegadas.Add(operacion.Folio);
                        continue;
                    }
                    else if (operacion.Fec_Valor.Value > fechaSistema)
                    {
                        Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|Es fecha futura, se guarda para revisión RECC...");
                        analisis.OperacionesFuturas.Add(operacion.Folio);
                        continue;
                    }
                }
            }

            return analisis;
        }

        private async Task ProcesarOperacionesTmdAsync(Paquete paquete, AnalisisOperaciones analisis, CancellationToken ct, string contextoDespacho)
        {
            if (analisis.OperacionesTMD == null
                || analisis.OperacionesTMD.Count == 0
                || !string.Equals(paquete.Servicio, ConstantesServicios.T24, StringComparison.Ordinal))
                return;

            Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|Revisando las operaciones TMD...");
            var operacionesT24Registro = await _sondeoDao.ObtenerUltimoAliasProcesoAsync(analisis.OperacionesTMD, TipoOperacion.TMD, ct)
                ?? new HashSet<long>();

            foreach (var folioTmd in operacionesT24Registro)
            {
                if (!await ReintentoPermitidoAsync(TipoOperacion.TMD, folioTmd, ct, contextoDespacho))
                {
                    analisis.OperacionesNegadas.Add(folioTmd);
                    continue;
                }

                OperacionDto op = null;
                if (analisis.OperacionesTmdIndex == null || !analisis.OperacionesTmdIndex.TryGetValue(folioTmd, out op))
                {
                    Logging.WriteConContexto(contextoDespacho, $"ERROR|TMD|No se encontró operación para folio {folioTmd}");
                    continue;
                }

                decimal importe = op.Importe ?? 0m;
                if (!await RevisarFondosAsync(op.Num_Cuenta, importe, ct, contextoDespacho))
                {
                    await ProcesarCuentaSinFondosAsync(TipoOperacion.TMD, folioTmd, ct, contextoDespacho);
                    analisis.OperacionesNegadas.Add(folioTmd);
                }
            }
        }

        private async Task ProcesarOperacionesFuturasAsync(Paquete paquete, AnalisisOperaciones analisis, CancellationToken ct, string contextoDespacho)
        {
            if (analisis.OperacionesFuturas.Count == 0)
                return;

            Logging.WriteConContexto(contextoDespacho, "INFO|Despachador|Revisando las operaciones futuras para ver si son RECC...");
            var operacionesRECC = await _sondeoDao.RevisarRECCAsync(analisis.OperacionesFuturas, paquete.Tipo, ct)
                ?? new HashSet<long>();

            foreach (var folio in analisis.OperacionesFuturas)
            {
                if (!operacionesRECC.Contains(folio))
                {
                    Logging.WriteConContexto(contextoDespacho, $"INFO|procesa_operaciones_planTrabajo|Operación futura SIN estatus RECC detectada; se descarta para despacho... TipoOperacion='{paquete.Tipo}', folio={folio}.");
                    analisis.OperacionesNegadas.Add(folio);
                    continue;
                }

                Logging.WriteConContexto(contextoDespacho, $"INFO|procesa_operaciones_planTrabajo|Operación futura CON estatus RECC detectada; se incluye para despacho de cancelación... TipoOperacion='{paquete.Tipo}', folio={folio}.");
            }
        }

        /// <summary>
        /// Consulta si la operación puede volver a intentarse.
        /// </summary>
        internal async Task<bool> ReintentoPermitidoAsync(string tipoOperacion, long folio, CancellationToken ct, string contextoDespacho = null)
        {
            try
            {
                Logging.WriteConContexto(contextoDespacho, $"INFO|ReintentoPermitido|Consulta para verificar si la operación {tipoOperacion} con folio {folio} se había registrado previamente sin fondos");
                DateTime? fecUltStr = await _sondeoDao.VerificarSinFondosAsync(folio, tipoOperacion, ct);
                if (fecUltStr is null)
                {
                    Logging.WriteConContexto(contextoDespacho, $"INFO|ReintentoPermitido|Se permite el intento de ingreso de la operación {tipoOperacion} con folio {folio}. No hay histórico de falta de fondos");
                    return true;
                }
                DateTime fecUlt = fecUltStr.Value;

                Logging.WriteConContexto(contextoDespacho, $"INFO|ReintentoPermitido|La operación {tipoOperacion} con folio {folio} se encuentra registrada previamente sin fondos");

                var horaActual = _parametrizacion.ObtenerHoraSistema();
                var horaLimite = ConversionesComunes.ParseTimeSpanRequerido(
                    _configuracion.ParametrosSistema[ConstantesServicio.ParametroSinFondosHoraValida],
                    ConstantesServicio.ParametroSinFondosHoraValida);

                bool permitirPorHora = horaActual > horaLimite;
                bool permitirPorMinutos = DateTime.Now > fecUlt.AddMinutes(ConversionesComunes.ParseDoubleRequerido(
                    _configuracion.ParametrosSistema[ConstantesServicio.ParametroSinFondosTiempoEspera],
                    ConstantesServicio.ParametroSinFondosTiempoEspera));

                if (permitirPorHora || permitirPorMinutos)
                {
                    bool eliminado = await _sondeoDao.EliminarSinFondosAsync(folio, tipoOperacion, ct);
                    if (!eliminado)
                    {
                        Logging.WriteConContexto(contextoDespacho, $"ERROR|ReintentoPermitido|No fue posible eliminar la marca sin fondos para la operación {tipoOperacion} con folio {folio}");
                        return false;
                    }

                    if (permitirPorHora)
                    {
                        Logging.WriteConContexto(contextoDespacho, $"INFO|ReintentoPermitido| Se permite el re intento de la operación {tipoOperacion} con folio {folio} pues la hora es válida");
                    }
                    else
                    {
                        Logging.WriteConContexto(contextoDespacho, $"INFO|ReintentoPermitido| Se permite el re intento de la operación {tipoOperacion} con folio {folio} pues los minutos son válidos");
                    }

                    return true;
                }

                Logging.WriteConContexto(contextoDespacho, $"INFO|ReintentoPermitido| No se permitió el re intento de la operación {tipoOperacion} con folio {folio} por no cumplir condiciones válidas");
                return false;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logging.WriteConContexto(contextoDespacho, $"ERROR|ReintentoPermitido|{tipoOperacion}|Se presenta error en el folio {folio} - {ex}");
                return false;
            }
        }

        /// <summary>
        /// Almacena la operación para señalizar su falta de fondos.
        /// </summary>
        internal async Task<bool> ProcesarCuentaSinFondosAsync(string tipoOperacion, long folio, CancellationToken ct, string contextoDespacho = null)
        {
            try
            {
                Logging.WriteConContexto(contextoDespacho, $"INFO|ProcesaCuentaSinFondos| Inicia el procedimiento para tratar la operacion {tipoOperacion} con folio {folio} por falta de fondos");
                await _sondeoDao.ActualizaCuentaSinFondosAsync(folio, tipoOperacion, ct);
                DateTime? fecUltStr = await _sondeoDao.VerificarSinFondosAsync(folio, tipoOperacion, ct);

                if (fecUltStr is null)
                {
                    await _sondeoDao.InsertarFechaProcesadoSinFondosAsync(folio, tipoOperacion, ct);
                    Logging.WriteConContexto(contextoDespacho, $"INFO|ProcesaCuentaSinFondos| Inserción de la operacion {tipoOperacion} con folio {folio} en la tabla TB_SRV_T24_MW_ADMON_FOLIOS");
                }
                else
                {
                    await _sondeoDao.ActualizaFechaProcesadoSinFondosAsync(folio, tipoOperacion, ct);
                    Logging.WriteConContexto(contextoDespacho, $"INFO|ProcesaCuentaSinFondos| Actualización de la operacion {tipoOperacion} con folio {folio} en la tabla TB_SRV_T24_MW_ADMON_FOLIOS");
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logging.WriteConContexto(contextoDespacho, $"ERROR|ProcesaCuentaSinFondos|{tipoOperacion}|Se presenta error en el folio {folio} - {ex}");
                return false;
            }
            return true;
        }

        /// <summary>
        /// Consulta los fondos de la cuenta.
        /// </summary>
        internal async Task<bool> RevisarFondosAsync(string numCuenta, decimal importe, CancellationToken ct, string contextoDespacho = null)
        {
            try
            {
                Logging.WriteConContexto(contextoDespacho, $"INFO|FondosSuficientes|Consultando saldo de la cuenta {numCuenta}. Valida importe mínimo de {importe}");
                decimal? saldo = await _wsT24.ObtenerSaldoAsync(numCuenta, ct, contextoDespacho);
                if (saldo is null) return false;
                if (saldo >= importe) return true;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logging.WriteConContexto(contextoDespacho, $"ERROR|FondosSuficientes|Excepción:{ex}");
            }
            return false;
        }

        /// <summary>
        /// Crea un contexto de despacho único para el worker y el paquete (Logging especial de despachador).
        /// </summary>
        /// <param name="paquete"></param>
        private static string CrearContextoDespacho(Paquete paquete)
        {
            int workerId = Interlocked.Increment(ref _workerSequence);
            int paqueteId = Interlocked.Increment(ref _paqueteSequence);
            string servicio = paquete == null || string.IsNullOrWhiteSpace(paquete.Servicio) ? "N/A" : paquete.Servicio.Trim();
            string tipo = paquete == null || string.IsNullOrWhiteSpace(paquete.Tipo) ? "N/A" : paquete.Tipo.Trim();

            return string.Format(
                CultureInfo.InvariantCulture,
                "[W:DSP-W{0:0000}][Paquete:DSP-P{1:0000}][Servicio:{2}][Tipo:{3}]",
                workerId,
                paqueteId,
                servicio,
                tipo);
        }

    }
}


﻿namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO necesario para recuperar datos de DespacharPaquete (retorna resultado).
    /// </summary>
    public class WorkerResultadoDto
    {
        public string Servicio { get; set; }
        public string Tipo { get; set; }

        public bool Claimed { get; set; }          //¿tomo el paquete?
        public bool ReturnedToPending { get; set; } //¿deberia permitir volverlo a pendiente?
        public bool Paused { get; set; }           //¿lo dejo pausado?
        public bool Cancelled { get; set; }
        public string Error { get; set; }
    }
}


using Aplicacion.DTOs;
using System;

namespace Aplicacion.Modelo
{
    /// <summary>
    /// Modelo para el control por servicio.
    /// </summary>
    public class ControlServicio
    {
        public string Servicio { get; private set; }
        public int Maximos { get; private set; } 
        public int Activos { get; set; }
        public int Prioridad { get; set; }

        public int ErroresRecientes { get; set; }
        public TimeSpan LatenciaPromedio { get; set; }

        public string Ip { get; set; }
        public int Puerto { get; set; }

        public bool Habilitado { get; set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        public ControlServicio(ControlServicioDto servicio)
        {
            Servicio = servicio.Servicio;
            Maximos = servicio.Maximos;
            Activos = 0;
            Prioridad = servicio.Prioridad;
            Ip = servicio.IP;
            Puerto = servicio.Puerto;

            //Si no hay ip o puerto, se rechaza...
            if (string.IsNullOrEmpty(Ip) || Puerto == 0)
            {
                throw new Exception("No se encontro Ip o Puerto válidos");
            }
        }

        /// <summary>
        /// Determina si el servicio aun tiene workers permitidos.
        /// Retorna true si la cantidad actual activa es menor a los máximos permitidos;
        /// en caso contrario, retorna false.
        /// </summary>
        public bool TieneCapacidad =>
            Activos < Maximos;

    }
}


using System;

namespace Aplicacion.Modelo
{
    /// <summary>
    /// Modelo para el control por tipo.
    /// </summary>
    public class ControlTipo
    {
        public string Servicio { get; set; }
        public string Tipo { get; set; }
        public int Pendientes { get; set; }
        public int Activos { get; set; }
        public int Maximos { get; set; }
        public int Prioridad { get; set; }
        public DateTime UltimaEjecucion { get; set; } = DateTime.MinValue;

        public bool Habilitado { get; set; }

        /// <summary>
        /// Obtiene una calificacion de ocupación (de acuerdo a activos y máximos).
        /// </summary>
        public double Ocupacion =>
                Maximos == 0 ? 1.0 : (double)Activos / Maximos;

    }
}

using MUFG_T24.Enums_utilidades;

namespace Aplicacion.Enums
{
    /// <summary>
    /// Clase que contiene las constantes a modo de enumerador.
    /// Por compatibilidad: NET Framework 4.8 no permite enumeradores strings.
    /// </summary>
    public class ConstantesServicio
    {
        //Constantes de parámetros del sistema...
        public const string ParametroSinFondosHoraValida = "SINFONDOS_HORA_VALIDA";
        public const string ParametroSinFondosTiempoEspera = "SINFONDOS_TIEMPO_ESPERA";
        public const string ParametroWsT24MwCompany = "WST24_MW_COMPANY";
        public const string ParametroWsT24MwUsername = "WST24_MW_USERNAME";
        public const string ParametroWsT24MwPassword = "WST24_MW_PASSWORD";
        public const string wsT24Endpoint = "ACCT_BALANCE_API_ENDPOINT";

        //Constantes de procedimientos...
        public const string UspObtenerControlServicio = "usp_despachador_ObtenerControlServicio";
        public const string UspObtenerControlTipo = "usp_despachador_ObtenerControlTipo";
        public const string UspObtenerOperacionesPendientes = "usp_despachador_ObtenerOperacionesPendientes";
        public const string UspObtenerT24RegistroOperaciones = "usp_despachador_ObtenerT24RegistroOperaciones";
        public const string UspRevisarRECCBatch = "usp_despachador_RevisarRECCBatch";
        public const string UspVerificarSinFondos = "usp_despachador_VerificarSinFondos";
        public const string UspEliminarSinFondos = "usp_despachador_EliminarSinFondos";
        public const string UspActualizaCuentaSinFondos = "usp_despachador_ActualizaCuentaSinFondos";
        public const string UspActualizaFechaProcesadoSinFondos = "usp_despachador_ActualizaFechaProcesadoSinFondos";
        public const string UspInsertarFechaProcesadoSinFondos = "usp_despachador_InsertarFechaProcesadoSinFondos";

        public const string TbFolioConcatenado = "tb_despachador_folioConcatenado";

        //Constantes de contrato wsT24...
        public const string WsT24CampoNumeroCuenta = "ACCOUNT.NUMBER";
        public const string WsT24OperadorIgual = "EQ";

    }
}


using System;
using System.Collections.Generic;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Clase "Abstracta" para extender IEnumerable con métodos de utilidad.
    /// </summary>
    public static class IEnumerableExtensions
    {
        public static IEnumerable<List<T>> ChunkBy<T>(this IEnumerable<T> source, int chunkSize)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (chunkSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(chunkSize));

            List<T> chunk = new List<T>(chunkSize);

            foreach (var item in source)
            {
                chunk.Add(item);

                if (chunk.Count == chunkSize)
                {
                    yield return chunk;
                    chunk = new List<T>(chunkSize);
                }
            }

            if (chunk.Count > 0)
                yield return chunk;
        }

    }
}


﻿using Aplicacion.DTOs;
using System.Collections.Generic;

namespace Aplicacion.Modelo
{
    /// <summary>
    /// Modelo para el control de paquetes.
    /// </summary>
    public class Paquete
    {
        public string Servicio { get; set; }
        public string Tipo { get; set; }
        public IReadOnlyList<OperacionDto> Operaciones { get; set; }
        public int Cantidad => Operaciones?.Count ?? 0;
    }
}


using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase DAO de funciones de control para el Despachador Orquestador.
    /// </summary>
    public class ParametrizacionControlDao : RepositorioMaestro, IParametrizacionControlDao
    {
        Configuracion _configuracion;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ParametrizacionControlDao(Configuracion configuracion) 
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) 
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Obtiene el control del servicio.
        /// </summary>
        public async Task<IEnumerable<ControlServicioDto>> ObtenerControlServicioAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ControlServicioDto>(ConstantesServicio.UspObtenerControlServicio, null, ct);
            return resultado.Data;
        }

        /// <summary>
        /// Obtiene el control del tipo (operaciones por servicio).
        /// </summary>
        public async Task<IEnumerable<ControlTipoDto>> ObtenerControlTipoAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ControlTipoDto>(ConstantesServicio.UspObtenerControlTipo, null, ct);
            return resultado.Data;
        }

        /// <summary>
        /// Obtiene los paquetes de trabajo.
        /// </summary>
        public async Task<IEnumerable<OperacionDto>> ObtenerOperacionesPendientesAsync(int cantidadMaximaWorkers, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<OperacionDto>(ConstantesServicio.UspObtenerOperacionesPendientes, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TopN", _configuracion.CantidadPaquete * cantidadMaximaWorkers));
            }, ct);
            return resultado.Data;
        }

    }
}


using Aplicacion.DTOs;
using Aplicacion.Enums;
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio
{
    /// <summary>
    /// Clase DAO de funciones de de consulta para el Despachador Worker.
    /// </summary>
    public class RecuperacionDatos : RepositorioMaestro, IRecuperacionDatos
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public RecuperacionDatos() 
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene las operaciones que tienen ultima alias T24_Registro.
        /// </summary>
        public async Task<HashSet<long>> ObtenerUltimoAliasProcesoAsync(IEnumerable<long> folios, string tipoOperacion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            var dt = new DataTable();
            dt.Columns.Add("Folio", typeof(long));

            foreach (var f in folios.Distinct())
                dt.Rows.Add(f);

            var tvp = new SqlParameter("@Folios", dt)
            {
                SqlDbType = SqlDbType.Structured,
                TypeName = ConstantesServicio.TbFolioConcatenado
            };

            var resultado = await EjecutarStoredProcedureQuery<T24RegitroOperacionDto>(ConstantesServicio.UspObtenerT24RegistroOperaciones, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                cmd.Parameters.Add(tvp);
            }, ct);

            return new HashSet<long>(resultado.Data.Select(x => x.Folio));
        }

        /// <summary>
        /// Obtiene las operaciones que tienen estatus RECC.
        /// </summary>
        public async Task<HashSet<long>> RevisarRECCAsync(IEnumerable<long> folios, string tipoOperacion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            var dt = new DataTable();
            dt.Columns.Add("Folio", typeof(long));

            foreach (var f in folios.Distinct())
                dt.Rows.Add(f);

            var tvp = new SqlParameter("@Folios", dt)
            {
                SqlDbType = SqlDbType.Structured,
                TypeName = ConstantesServicio.TbFolioConcatenado
            };

            var resultado = await EjecutarStoredProcedureQuery<RevisarReccDto>(ConstantesServicio.UspRevisarRECCBatch, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                cmd.Parameters.Add(tvp);
            }, ct);

            return new HashSet<long>(resultado.Data.Select(x => x.Folio));
        }

        /// <summary>
        /// Verifica si una operacion está marcada sin fondos.
        /// </summary>
        public async Task<DateTime?> VerificarSinFondosAsync(long folio, string tipoOperacion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<VerificacionSinFondosDto>(ConstantesServicio.UspVerificarSinFondos, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            }, ct);
            return resultado.Data.Count > 0 ? resultado.Data[0].FEC_PROCESS : (DateTime?)null;
        }

        /// <summary>
        /// Elimina la marca sin fondos de una operación.
        /// </summary>
        public async Task<bool> EliminarSinFondosAsync(long folio, string tipoOperacion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.UspEliminarSinFondos, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            }, ct, validarAfectacion: true);
            return resultado.Afecto == true;
        }

        /// <summary>
        /// Actualiza la información de una operación marcada sin fondos.
        /// </summary>
        public async Task<bool> ActualizaCuentaSinFondosAsync(long folio, string tipoOperacion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.UspActualizaCuentaSinFondos, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            }, ct, validarAfectacion: true);
            return resultado.Afecto == true;
        }

        /// <summary>
        /// Actualiza la fecha de procesado de una operación marcada sin fondos.
        /// </summary>
        public async Task<bool> ActualizaFechaProcesadoSinFondosAsync(long folio, string tipoOperacion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.UspActualizaFechaProcesadoSinFondos, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            }, ct, validarAfectacion: true);
            return resultado.Afecto == true;
        }

        /// <summary>
        /// Inserta una operacion para marcarla como sin fondos.
        /// </summary>
        public async Task<bool> InsertarFechaProcesadoSinFondosAsync(long folio, string tipoOperacion, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(ConstantesServicio.UspInsertarFechaProcesadoSinFondos, cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            }, ct, validarAfectacion: true);
            return resultado.Afecto == true;
        }

    }
}


using Aplicacion.Enums;
using Aplicacion.Interfaces;
using Infraestructura.wsT24;
using MUFG_T24.Aplicacion.Conversiones;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.Infraestructura.Logging;
using System;
using System.Globalization;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.SOAP
{
    /// <summary>
    /// Clase Web Service de funciones de T24 para el consumo de Saldo.
    /// </summary>
    public class wsT24 : IwsT24
    {
        Configuracion _configuracion;

        public wsT24(Configuracion configuracion)
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Consume un servicio web para obtener el balance actual de la cuenta y saber si sus fondos cubren el importe.
        /// </summary>
        public async Task<decimal?> ObtenerSaldoAsync(string numCuenta, CancellationToken ct, string contextoDespacho = null)
        {
            string endpointWebService = ObtenerEndpointWebService();
            var client = new T24WebServicesImplClient(
                new BasicHttpBinding(),
                new EndpointAddress(endpointWebService));

            try
            {
                ct.ThrowIfCancellationRequested();

                var wrc = new webRequestCommon
                {
                    company = _configuracion.ParametrosSistema[ConstantesServicio.ParametroWsT24MwCompany],
                    userName = _configuracion.ParametrosSistema[ConstantesServicio.ParametroWsT24MwUsername],
                    password = _configuracion.ParametrosSistema[ConstantesServicio.ParametroWsT24MwPassword]
                };
                var filtro = new enquiryInputCollection
                {
                    columnName = ConstantesServicio.WsT24CampoNumeroCuenta,
                    criteriaValue = numCuenta,
                    operand = ConstantesServicio.WsT24OperadorIgual
                };
                var request = new CONSULTASALDOCUENTARequest
                {
                    WebRequestCommon = wrc,
                    ACCTBALWSType = new[] { filtro }
                };
                CONSULTASALDOCUENTAResponse response;
                using (ct.Register(() => client.Abort()))
                {
                    response = await client.CONSULTASALDOCUENTAAsync(request);
                }

                Logging.WriteConContexto(contextoDespacho, $"INFO|FondosSuficientes|Resultado {response.Status.successIndicator}");

                if (response.Status.successIndicator != successIndicator.Success)
                {
                    string messageGeneralError = string.Empty;
                    if (response.Status.messages != null && response.Status.messages.Length > 0)
                    {
                        messageGeneralError = string.Join("|", response.Status.messages);
                    }

                    Logging.WriteConContexto(contextoDespacho, $"INFO|FondosSuficientes|Error al consultar saldo {messageGeneralError}");
                    throw new Exception("Error al realizar la consulta de saldo");
                }

                //Obtiene el working balance...
                var workingBalRaw =
                    response.ACCTBALWSType?.FirstOrDefault()?
                        .gACCTBALWSDetailType?
                        .mACCTBALWSDetailType?.FirstOrDefault()?
                        .WORKINGBAL;

                string workingBalTexto = Convert.ToString(workingBalRaw, CultureInfo.InvariantCulture);
                decimal workingBal = ConversionesComunes.ParseDecimalOpcional(
                    workingBalTexto,
                    "WORKINGBAL",
                    decimal.MinValue);

                if (workingBal != decimal.MinValue)
                {
                    Logging.WriteConContexto(contextoDespacho, $"INFO|FondosSuficientes|Consulta de saldo cuenta {numCuenta}: {workingBal}");
                    return workingBal;
                }
            }
            catch (Exception) when (ct.IsCancellationRequested)
            {
                throw new OperationCanceledException(ct);
            }
            catch (Exception ex)
            {
                Logging.WriteConContexto(contextoDespacho, $"ERROR|ObtenerSaldoAsync|Excepción:{ex}");
            }
            return null;
        }

        private string ObtenerEndpointWebService()
        {
            if (_configuracion?.ParametrosSistema == null)
                throw new Exception($"No existe la colección de ParametrosSistema para obtener '{ConstantesServicio.wsT24Endpoint}'.");

            string endpoint = _configuracion.ParametrosSistema[ConstantesServicio.wsT24Endpoint];

            if (string.IsNullOrWhiteSpace(endpoint))
                throw new Exception($"No se encontró el parámetro obligatorio '{ConstantesServicio.wsT24Endpoint}' para el web service T24.");

            return endpoint.Trim();
        }

    }
}


using Aplicacion.Interfaces;
using MUFG_T24.Encriptacion_utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Modelos;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.TCP
{

    /// <summary>
    /// Clase TCP para el despacho de operaciones.
    /// </summary>
    public class PeticionTCP : IEnvioDatos
    {
        /// <summary>
        /// Envía la petición TCP.
        /// Mecanismo de cifrado: la llave no se transmite en el payload; emisor y receptor la obtienen
        /// desde la configuración general compartida (parámetro LLAVE_ENCRIPTADODESPACHADOR).
        /// El VECTOR_IV se genera dinámicamente por mensaje y viaja junto con el texto cifrado
        /// conforme a la implementación de MUFG_T24.Encriptacion_utilidades.
        /// </summary>
        public async Task<RespuestaPaqueteDto> EnviarPeticionAsync(string destino, int puerto, string peticion, TimeSpan timeout, CancellationToken ct, string contextoDespacho = null)
        {
            timeout = NormalizarTimeout(timeout);

            try
            {
                ct.ThrowIfCancellationRequested();

                using (var client = new TcpClient())
                {
                    var connectTask = client.ConnectAsync(destino, puerto);

                    //Si el tiempo de tiemout se agota...
                    if (await Task.WhenAny(connectTask, Task.Delay(timeout, ct)) != connectTask)
                    {
                        ct.ThrowIfCancellationRequested();
                        Logging.WriteConContexto(contextoDespacho, $"WARN|PeticionTCP|Timeout de conexión TCP hacia {destino}:{puerto} tras {timeout.TotalMilliseconds}ms.");
                        return new RespuestaPaqueteDto();
                    }

                    await connectTask;

                    using (var stream = client.GetStream())
                    {
                        string llave = Encriptacion.GenerarLlave();
                        string cifrado = Encriptacion.Encrypt(peticion, llave);

                        byte[] payloadBytes = Encoding.UTF8.GetBytes(cifrado);
                        byte[] lengthPrefix = BitConverter.GetBytes(payloadBytes.Length);

                        await stream.WriteAsync(lengthPrefix, 0, lengthPrefix.Length, ct);
                        await stream.WriteAsync(payloadBytes, 0, payloadBytes.Length, ct);
                        await stream.FlushAsync(ct);

                        Logging.WriteConContexto(contextoDespacho, $"[{destino}:{puerto}]La petición ha sido enviada.");

                        using (var timeoutCts = new CancellationTokenSource(timeout))
                        using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token))
                        {

                            byte[] respLengthBuffer = await ReadExactAsync(stream, 4, linkedCts.Token);
                            int respLength = BitConverter.ToInt32(respLengthBuffer, 0);

                            byte[] respBuffer = await ReadExactAsync(stream, respLength, linkedCts.Token);
                            string respuesta = Encoding.UTF8.GetString(respBuffer);

                            RespuestaPaqueteDto dto =
                                JsonConvert.DeserializeObject<RespuestaPaqueteDto>(respuesta);

                            return dto;
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logging.WriteConContexto(contextoDespacho, $"ERROR|PeticionTCP|Fallo en envío/recepción TCP hacia {destino}:{puerto}. Excepción:{ex}");
                return new RespuestaPaqueteDto
                {
                    Resultado = (int)ConstantesRespuestaResultadoDespachador.ERROR,
                };
            }
        }

        /// <summary>
        /// Interpreta la respuesta.
        /// </summary>
        internal static async Task<byte[]> ReadExactAsync(NetworkStream stream, int length, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            byte[] buffer = new byte[length];
            int offset = 0;

            while (offset < length)
            {
                int read = await stream.ReadAsync(
                    buffer, offset, length - offset, ct);

                if (read == 0)
                    throw new IOException("Conexión cerrada por el host remoto");

                offset += read;
            }

            return buffer;
        }

        /// <summary>
        /// Normaliza el timeout para evitar un timeout de 0.
        /// </summary>
        private static TimeSpan NormalizarTimeout(TimeSpan timeout)
        {
            if (timeout <= TimeSpan.Zero)
                return TimeSpan.FromSeconds(30);

            return timeout;
        }

    }
}


//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace Infraestructura.wsT24 {
    
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    [System.ServiceModel.ServiceContractAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE", ConfigurationName="wsT24.T24WebServicesImpl")]
    public interface T24WebServicesImpl {
        
        [System.ServiceModel.OperationContractAttribute(Action="http://temenos.com/WS.ACCT.BALANCE/T24WebServicesImpl/CONSULTASALDOCUENTARequest", ReplyAction="http://temenos.com/WS.ACCT.BALANCE/T24WebServicesImpl/CONSULTASALDOCUENTAResponse" +
            "")]
        [System.ServiceModel.XmlSerializerFormatAttribute(SupportFaults=true)]
        Infraestructura.wsT24.CONSULTASALDOCUENTAResponse CONSULTASALDOCUENTA(Infraestructura.wsT24.CONSULTASALDOCUENTARequest request);
        
        // CODEGEN: Generating message contract since the operation has multiple return values.
        [System.ServiceModel.OperationContractAttribute(Action="http://temenos.com/WS.ACCT.BALANCE/T24WebServicesImpl/CONSULTASALDOCUENTARequest", ReplyAction="http://temenos.com/WS.ACCT.BALANCE/T24WebServicesImpl/CONSULTASALDOCUENTAResponse" +
            "")]
        System.Threading.Tasks.Task<Infraestructura.wsT24.CONSULTASALDOCUENTAResponse> CONSULTASALDOCUENTAAsync(Infraestructura.wsT24.CONSULTASALDOCUENTARequest request);
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.8.9032.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE")]
    public partial class webRequestCommon : object, System.ComponentModel.INotifyPropertyChanged {
        
        private string companyField;
        
        private string passwordField;
        
        private string userNameField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=0)]
        public string company {
            get {
                return this.companyField;
            }
            set {
                this.companyField = value;
                this.RaisePropertyChanged("company");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=1)]
        public string password {
            get {
                return this.passwordField;
            }
            set {
                this.passwordField = value;
                this.RaisePropertyChanged("password");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=2)]
        public string userName {
            get {
                return this.userNameField;
            }
            set {
                this.userNameField = value;
                this.RaisePropertyChanged("userName");
            }
        }
        
        public event System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
        
        protected void RaisePropertyChanged(string propertyName) {
            System.ComponentModel.PropertyChangedEventHandler propertyChanged = this.PropertyChanged;
            if ((propertyChanged != null)) {
                propertyChanged(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.8.9032.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE")]
    public partial class ACCTBALWSType : object, System.ComponentModel.INotifyPropertyChanged {
        
        private string zERORECORDSField;
        
        private ACCTBALWSTypeGACCTBALWSDetailType gACCTBALWSDetailTypeField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=0)]
        public string ZERORECORDS {
            get {
                return this.zERORECORDSField;
            }
            set {
                this.zERORECORDSField = value;
                this.RaisePropertyChanged("ZERORECORDS");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=1)]
        public ACCTBALWSTypeGACCTBALWSDetailType gACCTBALWSDetailType {
            get {
                return this.gACCTBALWSDetailTypeField;
            }
            set {
                this.gACCTBALWSDetailTypeField = value;
                this.RaisePropertyChanged("gACCTBALWSDetailType");
            }
        }
        
        public event System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
        
        protected void RaisePropertyChanged(string propertyName) {
            System.ComponentModel.PropertyChangedEventHandler propertyChanged = this.PropertyChanged;
            if ((propertyChanged != null)) {
                propertyChanged(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.8.9032.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true, Namespace="http://temenos.com/WS.ACCT.BALANCE")]
    public partial class ACCTBALWSTypeGACCTBALWSDetailType : object, System.ComponentModel.INotifyPropertyChanged {
        
        private ACCTBALWSTypeGACCTBALWSDetailTypeMACCTBALWSDetailType[] mACCTBALWSDetailTypeField;
        
        private string gField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("mACCTBALWSDetailType", Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=0)]
        public ACCTBALWSTypeGACCTBALWSDetailTypeMACCTBALWSDetailType[] mACCTBALWSDetailType {
            get {
                return this.mACCTBALWSDetailTypeField;
            }
            set {
                this.mACCTBALWSDetailTypeField = value;
                this.RaisePropertyChanged("mACCTBALWSDetailType");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="positiveInteger")]
        public string g {
            get {
                return this.gField;
            }
            set {
                this.gField = value;
                this.RaisePropertyChanged("g");
            }
        }
        
        public event System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
        
        protected void RaisePropertyChanged(string propertyName) {
            System.ComponentModel.PropertyChangedEventHandler propertyChanged = this.PropertyChanged;
            if ((propertyChanged != null)) {
                propertyChanged(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.8.9032.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true, Namespace="http://temenos.com/WS.ACCT.BALANCE")]
    public partial class ACCTBALWSTypeGACCTBALWSDetailTypeMACCTBALWSDetailType : object, System.ComponentModel.INotifyPropertyChanged {
        
        private string accountNoField;
        
        private string customerField;
        
        private string aCCField;
        
        private string cUSTOMERNAMEField;
        
        private string tYPEField;
        
        private string cCYField;
        
        private string wORKINGBALField;
        
        private string lEDGERBALField;
        
        private string cLEAREDBALField;
        
        private string lOCKAMTField;
        
        private string mField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=0)]
        public string AccountNo {
            get {
                return this.accountNoField;
            }
            set {
                this.accountNoField = value;
                this.RaisePropertyChanged("AccountNo");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=1)]
        public string Customer {
            get {
                return this.customerField;
            }
            set {
                this.customerField = value;
                this.RaisePropertyChanged("Customer");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=2)]
        public string ACC {
            get {
                return this.aCCField;
            }
            set {
                this.aCCField = value;
                this.RaisePropertyChanged("ACC");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=3)]
        public string CUSTOMERNAME {
            get {
                return this.cUSTOMERNAMEField;
            }
            set {
                this.cUSTOMERNAMEField = value;
                this.RaisePropertyChanged("CUSTOMERNAME");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=4)]
        public string TYPE {
            get {
                return this.tYPEField;
            }
            set {
                this.tYPEField = value;
                this.RaisePropertyChanged("TYPE");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=5)]
        public string CCY {
            get {
                return this.cCYField;
            }
            set {
                this.cCYField = value;
                this.RaisePropertyChanged("CCY");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=6)]
        public string WORKINGBAL {
            get {
                return this.wORKINGBALField;
            }
            set {
                this.wORKINGBALField = value;
                this.RaisePropertyChanged("WORKINGBAL");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=7)]
        public string LEDGERBAL {
            get {
                return this.lEDGERBALField;
            }
            set {
                this.lEDGERBALField = value;
                this.RaisePropertyChanged("LEDGERBAL");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=8)]
        public string CLEAREDBAL {
            get {
                return this.cLEAREDBALField;
            }
            set {
                this.cLEAREDBALField = value;
                this.RaisePropertyChanged("CLEAREDBAL");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=9)]
        public string LOCKAMT {
            get {
                return this.lOCKAMTField;
            }
            set {
                this.lOCKAMTField = value;
                this.RaisePropertyChanged("LOCKAMT");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="positiveInteger")]
        public string m {
            get {
                return this.mField;
            }
            set {
                this.mField = value;
                this.RaisePropertyChanged("m");
            }
        }
        
        public event System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
        
        protected void RaisePropertyChanged(string propertyName) {
            System.ComponentModel.PropertyChangedEventHandler propertyChanged = this.PropertyChanged;
            if ((propertyChanged != null)) {
                propertyChanged(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.8.9032.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE")]
    public partial class Status : object, System.ComponentModel.INotifyPropertyChanged {
        
        private string transactionIdField;
        
        private string messageIdField;
        
        private successIndicator successIndicatorField;
        
        private bool successIndicatorFieldSpecified;
        
        private string applicationField;
        
        private string[] messagesField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=0)]
        public string transactionId {
            get {
                return this.transactionIdField;
            }
            set {
                this.transactionIdField = value;
                this.RaisePropertyChanged("transactionId");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=1)]
        public string messageId {
            get {
                return this.messageIdField;
            }
            set {
                this.messageIdField = value;
                this.RaisePropertyChanged("messageId");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=2)]
        public successIndicator successIndicator {
            get {
                return this.successIndicatorField;
            }
            set {
                this.successIndicatorField = value;
                this.RaisePropertyChanged("successIndicator");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlIgnoreAttribute()]
        public bool successIndicatorSpecified {
            get {
                return this.successIndicatorFieldSpecified;
            }
            set {
                this.successIndicatorFieldSpecified = value;
                this.RaisePropertyChanged("successIndicatorSpecified");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=3)]
        public string application {
            get {
                return this.applicationField;
            }
            set {
                this.applicationField = value;
                this.RaisePropertyChanged("application");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("messages", Form=System.Xml.Schema.XmlSchemaForm.Unqualified, IsNullable=true, Order=4)]
        public string[] messages {
            get {
                return this.messagesField;
            }
            set {
                this.messagesField = value;
                this.RaisePropertyChanged("messages");
            }
        }
        
        public event System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
        
        protected void RaisePropertyChanged(string propertyName) {
            System.ComponentModel.PropertyChangedEventHandler propertyChanged = this.PropertyChanged;
            if ((propertyChanged != null)) {
                propertyChanged(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.8.9032.0")]
    [System.SerializableAttribute()]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE")]
    public enum successIndicator {
        
        /// <remarks/>
        Success,
        
        /// <remarks/>
        TWSError,
        
        /// <remarks/>
        T24Error,
        
        /// <remarks/>
        T24Override,
        
        /// <remarks/>
        T24Offline,
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.8.9032.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE")]
    public partial class enquiryInputCollection : object, System.ComponentModel.INotifyPropertyChanged {
        
        private string columnNameField;
        
        private string criteriaValueField;
        
        private string operandField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=0)]
        public string columnName {
            get {
                return this.columnNameField;
            }
            set {
                this.columnNameField = value;
                this.RaisePropertyChanged("columnName");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=1)]
        public string criteriaValue {
            get {
                return this.criteriaValueField;
            }
            set {
                this.criteriaValueField = value;
                this.RaisePropertyChanged("criteriaValue");
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified, Order=2)]
        public string operand {
            get {
                return this.operandField;
            }
            set {
                this.operandField = value;
                this.RaisePropertyChanged("operand");
            }
        }
        
        public event System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
        
        protected void RaisePropertyChanged(string propertyName) {
            System.ComponentModel.PropertyChangedEventHandler propertyChanged = this.PropertyChanged;
            if ((propertyChanged != null)) {
                propertyChanged(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));
            }
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    [System.ServiceModel.MessageContractAttribute(WrapperName="CONSULTASALDOCUENTA", WrapperNamespace="http://temenos.com/WS.ACCT.BALANCE", IsWrapped=true)]
    public partial class CONSULTASALDOCUENTARequest {
        
        [System.ServiceModel.MessageBodyMemberAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE", Order=0)]
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified)]
        public Infraestructura.wsT24.webRequestCommon WebRequestCommon;
        
        [System.ServiceModel.MessageBodyMemberAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE", Order=1)]
        [System.Xml.Serialization.XmlArrayAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified)]
        [System.Xml.Serialization.XmlArrayItemAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified)]
        public Infraestructura.wsT24.enquiryInputCollection[] ACCTBALWSType;
        
        public CONSULTASALDOCUENTARequest() {
        }
        
        public CONSULTASALDOCUENTARequest(Infraestructura.wsT24.webRequestCommon WebRequestCommon, Infraestructura.wsT24.enquiryInputCollection[] ACCTBALWSType) {
            this.WebRequestCommon = WebRequestCommon;
            this.ACCTBALWSType = ACCTBALWSType;
        }
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    [System.ServiceModel.MessageContractAttribute(WrapperName="CONSULTASALDOCUENTAResponse", WrapperNamespace="http://temenos.com/WS.ACCT.BALANCE", IsWrapped=true)]
    public partial class CONSULTASALDOCUENTAResponse {
        
        [System.ServiceModel.MessageBodyMemberAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE", Order=0)]
        [System.Xml.Serialization.XmlElementAttribute(Form=System.Xml.Schema.XmlSchemaForm.Unqualified)]
        public Infraestructura.wsT24.Status Status;
        
        [System.ServiceModel.MessageBodyMemberAttribute(Namespace="http://temenos.com/WS.ACCT.BALANCE", Order=1)]
        [System.Xml.Serialization.XmlElementAttribute("ACCTBALWSType", Form=System.Xml.Schema.XmlSchemaForm.Unqualified)]
        public Infraestructura.wsT24.ACCTBALWSType[] ACCTBALWSType;
        
        public CONSULTASALDOCUENTAResponse() {
        }
        
        public CONSULTASALDOCUENTAResponse(Infraestructura.wsT24.Status Status, Infraestructura.wsT24.ACCTBALWSType[] ACCTBALWSType) {
            this.Status = Status;
            this.ACCTBALWSType = ACCTBALWSType;
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    public interface T24WebServicesImplChannel : Infraestructura.wsT24.T24WebServicesImpl, System.ServiceModel.IClientChannel {
    }
    
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    public partial class T24WebServicesImplClient : System.ServiceModel.ClientBase<Infraestructura.wsT24.T24WebServicesImpl>, Infraestructura.wsT24.T24WebServicesImpl {
        
        public T24WebServicesImplClient() {
        }
        
        public T24WebServicesImplClient(string endpointConfigurationName) : 
                base(endpointConfigurationName) {
        }
        
        public T24WebServicesImplClient(string endpointConfigurationName, string remoteAddress) : 
                base(endpointConfigurationName, remoteAddress) {
        }
        
        public T24WebServicesImplClient(string endpointConfigurationName, System.ServiceModel.EndpointAddress remoteAddress) : 
                base(endpointConfigurationName, remoteAddress) {
        }
        
        public T24WebServicesImplClient(System.ServiceModel.Channels.Binding binding, System.ServiceModel.EndpointAddress remoteAddress) : 
                base(binding, remoteAddress) {
        }
        
        [System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)]
        Infraestructura.wsT24.CONSULTASALDOCUENTAResponse Infraestructura.wsT24.T24WebServicesImpl.CONSULTASALDOCUENTA(Infraestructura.wsT24.CONSULTASALDOCUENTARequest request) {
            return base.Channel.CONSULTASALDOCUENTA(request);
        }
        
        public Infraestructura.wsT24.Status CONSULTASALDOCUENTA(Infraestructura.wsT24.webRequestCommon WebRequestCommon, Infraestructura.wsT24.enquiryInputCollection[] ACCTBALWSType, out Infraestructura.wsT24.ACCTBALWSType[] ACCTBALWSType1) {
            Infraestructura.wsT24.CONSULTASALDOCUENTARequest inValue = new Infraestructura.wsT24.CONSULTASALDOCUENTARequest();
            inValue.WebRequestCommon = WebRequestCommon;
            inValue.ACCTBALWSType = ACCTBALWSType;
            Infraestructura.wsT24.CONSULTASALDOCUENTAResponse retVal = ((Infraestructura.wsT24.T24WebServicesImpl)(this)).CONSULTASALDOCUENTA(inValue);
            ACCTBALWSType1 = retVal.ACCTBALWSType;
            return retVal.Status;
        }
        
        public System.Threading.Tasks.Task<Infraestructura.wsT24.CONSULTASALDOCUENTAResponse> CONSULTASALDOCUENTAAsync(Infraestructura.wsT24.CONSULTASALDOCUENTARequest request) {
            return base.Channel.CONSULTASALDOCUENTAAsync(request);
        }
    }
}


services.xsd
<?xml version="1.0" encoding="utf-8"?>
<xs:schema xmlns:tns="http://temenos.com/WS.ACCT.BALANCE" targetNamespace="http://temenos.com/WS.ACCT.BALANCE" version="1.0" xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="CONSULTASALDOCUENTA" type="tns:CONSULTASALDOCUENTA" />
  <xs:element name="CONSULTASALDOCUENTAResponse" type="tns:CONSULTASALDOCUENTAResponse" />
  <xs:complexType name="ACCTBALWSType">
    <xs:sequence>
      <xs:element minOccurs="0" name="ZERORECORDS" type="xs:string" />
      <xs:element minOccurs="0" name="gACCTBALWSDetailType">
        <xs:complexType>
          <xs:sequence>
            <xs:element minOccurs="0" maxOccurs="unbounded" name="mACCTBALWSDetailType">
              <xs:complexType>
                <xs:sequence>
                  <xs:element minOccurs="0" name="AccountNo" type="xs:string" />
                  <xs:element minOccurs="0" name="Customer" type="xs:string" />
                  <xs:element minOccurs="0" name="ACC" type="xs:string" />
                  <xs:element minOccurs="0" name="CUSTOMERNAME" type="xs:string" />
                  <xs:element minOccurs="0" name="TYPE" type="xs:string" />
                  <xs:element minOccurs="0" name="CCY" type="xs:string" />
                  <xs:element minOccurs="0" name="WORKINGBAL" type="xs:string" />
                  <xs:element minOccurs="0" name="LEDGERBAL" type="xs:string" />
                  <xs:element minOccurs="0" name="CLEAREDBAL" type="xs:string" />
                  <xs:element minOccurs="0" name="LOCKAMT" type="xs:string" />
                </xs:sequence>
                <xs:attribute name="m" type="xs:positiveInteger" />
              </xs:complexType>
            </xs:element>
          </xs:sequence>
          <xs:attribute name="g" type="xs:positiveInteger" />
        </xs:complexType>
      </xs:element>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="CONSULTASALDOCUENTA">
    <xs:sequence>
      <xs:element minOccurs="0" name="WebRequestCommon" type="tns:webRequestCommon" />
      <xs:element minOccurs="0" name="ACCTBALWSType" type="tns:enquiryInput" />
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="webRequestCommon">
    <xs:sequence>
      <xs:element minOccurs="0" name="company" type="xs:string" />
      <xs:element name="password" type="xs:string" />
      <xs:element name="userName" type="xs:string" />
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="enquiryInput">
    <xs:sequence>
      <xs:element minOccurs="0" maxOccurs="unbounded" name="enquiryInputCollection" nillable="true" type="tns:enquiryInputCollection" />
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="enquiryInputCollection">
    <xs:sequence>
      <xs:element minOccurs="0" name="columnName" type="xs:string" />
      <xs:element minOccurs="0" name="criteriaValue" type="xs:string" />
      <xs:element minOccurs="0" name="operand" type="xs:string" />
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="CONSULTASALDOCUENTAResponse">
    <xs:sequence>
      <xs:element minOccurs="0" name="Status" type="tns:Status" />
      <xs:element minOccurs="0" maxOccurs="unbounded" name="ACCTBALWSType" type="tns:ACCTBALWSType" />
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="Status">
    <xs:sequence>
      <xs:element minOccurs="0" name="transactionId" type="xs:string" />
      <xs:element minOccurs="0" name="messageId" type="xs:string" />
      <xs:element minOccurs="0" name="successIndicator" type="tns:successIndicator" />
      <xs:element minOccurs="0" name="application" type="xs:string" />
      <xs:element minOccurs="0" maxOccurs="unbounded" name="messages" nillable="true" type="xs:string" />
    </xs:sequence>
  </xs:complexType>
  <xs:simpleType name="successIndicator">
    <xs:restriction base="xs:string">
      <xs:enumeration value="Success" />
      <xs:enumeration value="TWSError" />
      <xs:enumeration value="T24Error" />
      <xs:enumeration value="T24Override" />
      <xs:enumeration value="T24Offline" />
    </xs:restriction>
  </xs:simpleType>
</xs:schema>


T24WebServicesImplService.wsdl
<?xml version="1.0" encoding="utf-8"?>
<definitions xmlns:wsam="http://www.w3.org/2007/05/addressing/metadata" xmlns:wsp="http://www.w3.org/ns/ws-policy" xmlns:wsp1_2="http://schemas.xmlsoap.org/ws/2004/09/policy" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://temenos.com/WS.ACCT.BALANCE" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" name="T24WebServicesImplService" targetNamespace="http://temenos.com/WS.ACCT.BALANCE" xmlns="http://schemas.xmlsoap.org/wsdl/">
  <types>
    <xsd:schema>
      <xsd:import schemaLocation="http://10.97.38.163:8085/WS.ACCT.BALANCE/services?xsd=1" namespace="http://temenos.com/WS.ACCT.BALANCE" />
    </xsd:schema>
    <xsd:schema>
      <xsd:import schemaLocation="http://10.97.38.163:8085/WS.ACCT.BALANCE/services?xsd=2" namespace="http://jaxb.dev.java.net/array" />
    </xsd:schema>
  </types>
  <message name="CONSULTASALDOCUENTA">
    <part name="parameters" element="tns:CONSULTASALDOCUENTA" />
  </message>
  <message name="CONSULTASALDOCUENTAResponse">
    <part name="parameters" element="tns:CONSULTASALDOCUENTAResponse" />
  </message>
  <portType name="T24WebServicesImpl">
    <operation name="CONSULTASALDOCUENTA">
      <input wsam:Action="http://temenos.com/WS.ACCT.BALANCE/T24WebServicesImpl/CONSULTASALDOCUENTARequest" message="tns:CONSULTASALDOCUENTA" />
      <output wsam:Action="http://temenos.com/WS.ACCT.BALANCE/T24WebServicesImpl/CONSULTASALDOCUENTAResponse" message="tns:CONSULTASALDOCUENTAResponse" />
    </operation>
  </portType>
  <binding name="T24WebServicesImplPortBinding" type="tns:T24WebServicesImpl">
    <soap:binding transport="http://schemas.xmlsoap.org/soap/http" />
    <operation name="CONSULTASALDOCUENTA">
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
      <soap:address location="http://10.97.38.163:8085/WS.ACCT.BALANCE/services" />
    </port>
  </service>
</definitions>


Reference.svcmap
<?xml version="1.0" encoding="utf-8"?>
<ReferenceGroup xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ID="12e591da-8f1d-45cc-8e1b-81719b0922a5" xmlns="urn:schemas-microsoft-com:xml-wcfservicemap">
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
    <MetadataSource Address="http://10.97.38.163:8085/WS.ACCT.BALANCE/services?wsdl" Protocol="http" SourceId="1" />
  </MetadataSources>
  <Metadata>
    <MetadataFile FileName="T24WebServicesImplService.wsdl" MetadataType="Wsdl" ID="5ec6c161-4402-4950-8127-a395b8c3b88a" SourceId="1" SourceUrl="http://10.97.38.163:8085/WS.ACCT.BALANCE/services?wsdl" />
    <MetadataFile FileName="services.xsd" MetadataType="Schema" ID="c9175c8f-90a8-488b-a177-4462f470eea8" SourceId="1" SourceUrl="http://10.97.38.163:8085/WS.ACCT.BALANCE/services?xsd=1" />
    <MetadataFile FileName="services1.xsd" MetadataType="Schema" ID="db0e23e2-1c18-4408-b385-cc30d820e400" SourceId="1" SourceUrl="http://10.97.38.163:8085/WS.ACCT.BALANCE/services?xsd=2" />
  </Metadata>
  <Extensions>
    <ExtensionFile FileName="configuration91.svcinfo" Name="configuration91.svcinfo" />
    <ExtensionFile FileName="configuration.svcinfo" Name="configuration.svcinfo" />
  </Extensions>
</ReferenceGroup>

using Aplicacion;
using Aplicacion.Interfaces;
using Aplicacion.Servicios;
using Infraestructura.Repositorio;
using Infraestructura.SOAP;
using Infraestructura.TCP;
using MUFG_T24.Comunes;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Core.Configuracion;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System.Threading;
using System.Threading.Tasks;

namespace Despachador
{
    /// <summary>
    /// Clase principal del servicio Windows (presentación).
    /// </summary>
    public partial class ServiceHost : WindowsServiceDespachador
    {
        private SimpleInjector.Container _containerDI;
        private Configuracion _configuracionBase;

        protected override string NombreServicioLog => NombreServicio.Despachador.ToString();
        protected override bool InicializacionCompleta => _containerDI != null && _configuracionBase != null && !string.IsNullOrWhiteSpace(_configuracionBase.NombreServicio);

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
            _containerDI.Register<Alertas>(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);
            _containerDI.Register<IAlertas>(() => _containerDI.GetInstance<Alertas>(), Lifestyle.Singleton);
            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();

            //Objeto que almacena la configuración necesaria para el funcionamiento del servicio...
            _configuracionBase = new Configuracion();

            //Inicializa la obtención de la configuración general y el logging...
            {
                ObtencionConfguracionGeneral.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = ObtencionConfguracionGeneral.ObtenerServicio(NombreServicio.Despachador.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.PathLog = servicioMainConfiguracion.Log;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;
                _configuracionBase.CantidadPaquete = servicioMainConfiguracion.CantidadPaquete;
                _configuracionBase.LimitePaquetesPendientes = servicioMainConfiguracion.LimitePaquetesPendientes;
                _configuracionBase.LimiteConcurrencia = servicioMainConfiguracion.LimiteConcurrencia;

                Logging.Init(_configuracionBase.PathLog, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
            }

            //Registra la configuracion...
            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);

            //Registra los componentes...
            _containerDI.Register<IwsT24, wsT24>(Lifestyle.Transient);
            _containerDI.Register<IParametrizacionControlDao, ParametrizacionControlDao>(Lifestyle.Transient);
            _containerDI.Register<IRecuperacionDatos, RecuperacionDatos>(Lifestyle.Transient);
            _containerDI.Register<IEnvioDatos, PeticionTCP>(Lifestyle.Singleton);
            _containerDI.Register<OperacionServicio>(Lifestyle.Transient);
            _containerDI.Register<DespachadorWorker>(Lifestyle.Transient);

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
            await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio, ct);

            //Ejecuta el procedimiento principal del servicio...
            using (AsyncScopedLifestyle.BeginScope(_containerDI))
            {
                var operacionServicio = _containerDI.GetInstance<OperacionServicio>();
                bool resultado = await operacionServicio.IniciarDespachadorAsync(ct);
                if (!resultado)
                {
                    _parametrizacion.DetenerServicio(this);
                }
            }
        }

        /// <summary>
        /// Realiza ciertas operaciones antes de finalizar el servicio.
        /// </summary>
        protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            if (_containerDI == null || _configuracionBase == null)
                return;

            using (AsyncScopedLifestyle.BeginScope(_containerDI))
            {
                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();

                //Espera a que la tarea de inicio se complete...
                await _controlServicio.EsperarTareaActivaAsync(startupTask, "parametrización inicial", waitCt).ConfigureAwait(false);

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

    }
}

﻿using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;

namespace Despachador
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
                NombreServicio.Despachador.ToString(),
                () => new ServiceHost(),
                args,
                (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs));
        }

    }
}


ServiceHost.Designer.cs
﻿namespace Despachador
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


AplicacionDespachador.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{F6FEFE84-AC9F-4F39-898A-82391CA288FD}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Aplicacion</RootNamespace>
    <AssemblyName>AplicacionDespachador</AssemblyName>
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
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="Newtonsoft.Json, Version=13.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed, processorArchitecture=MSIL">
      <HintPath>..\packages\Newtonsoft.Json.13.0.4\lib\net45\Newtonsoft.Json.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Core" />
    <Reference Include="System.IdentityModel" />
    <Reference Include="System.Runtime.Serialization" />
    <Reference Include="System.Runtime.Serialization.Primitives, Version=4.1.2.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.Runtime.Serialization.Primitives.4.3.0\lib\net46\System.Runtime.Serialization.Primitives.dll</HintPath>
      <Private>True</Private>
      <Private>True</Private>
    </Reference>
    <Reference Include="System.ServiceModel" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="AssemblyInfo.cs" />
    <Compile Include="DTOs\ControlServicioDto.cs" />
    <Compile Include="DTOs\ControlTipoDto.cs" />
    <Compile Include="DTOs\OperacionDto.cs" />
    <Compile Include="DTOs\RevisarReccDto.cs" />
    <Compile Include="DTOs\T24RegitroOperacionDto.cs" />
    <Compile Include="Modelo\Paquete.cs" />
    <Compile Include="DTOs\VerificacionSinFondosDto.cs" />
    <Compile Include="DTOs\WorkerResultadoDto.cs" />
    <Compile Include="Enums\ConstantesServicio.cs" />
    <Compile Include="Interfaces\IEnumerableExtensions.cs" />
    <Compile Include="Interfaces\IEnvioDatos.cs" />
    <Compile Include="Interfaces\IParametrizacionControlDao.cs" />
    <Compile Include="Interfaces\IRecuperacionDatos.cs" />
    <Compile Include="Interfaces\IwsT24.cs" />
    <Compile Include="Modelo\ControlServicio.cs" />
    <Compile Include="Modelo\ControlTipo.cs" />
    <Compile Include="OperacionServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
    <Compile Include="Servicios\DespachadorWorker.cs" />
  </ItemGroup>
  <ItemGroup>
    <None Include="app.config" />
    <None Include="packages.config" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


InfraestructuraDespachador.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{F1F018E7-2859-4225-8B6B-25884411D167}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Infraestructura</RootNamespace>
    <AssemblyName>InfraestructuraDespachador</AssemblyName>
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
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="Newtonsoft.Json, Version=13.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed, processorArchitecture=MSIL">
      <HintPath>..\packages\Newtonsoft.Json.13.0.4\lib\net45\Newtonsoft.Json.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Core" />
    <Reference Include="System.IdentityModel" />
    <Reference Include="System.Runtime.Serialization" />
    <Reference Include="System.ServiceModel" />
    <Reference Include="System.ServiceModel.Duplex, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.ServiceModel.Primitives.10.0.652802\lib\net462\System.ServiceModel.Duplex.dll</HintPath>
    </Reference>
    <Reference Include="System.ServiceModel.Http, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.ServiceModel.Http.10.0.652802\lib\net462\System.ServiceModel.Http.dll</HintPath>
    </Reference>
    <Reference Include="System.ServiceModel.Primitives, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.ServiceModel.Primitives.10.0.652802\lib\net462\System.ServiceModel.Primitives.dll</HintPath>
    </Reference>
    <Reference Include="System.ServiceModel.Security, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL">
      <HintPath>..\packages\System.ServiceModel.Primitives.10.0.652802\lib\net462\System.ServiceModel.Security.dll</HintPath>
    </Reference>
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="Connected Services\wsT24\Reference.cs">
      <AutoGen>True</AutoGen>
      <DesignTime>True</DesignTime>
      <DependentUpon>Reference.svcmap</DependentUpon>
    </Compile>
    <Compile Include="Properties\AssemblyInfo.cs" />
    <Compile Include="Repositorio\ParametrizacionControlDao.cs" />
    <Compile Include="Repositorio\RecuperacionDatos.cs" />
    <Compile Include="SOAP\wsT24.cs" />
    <Compile Include="TCP\PeticionTCP.cs" />
  </ItemGroup>
  <ItemGroup>
    <None Include="app.config" />
    <None Include="Connected Services\wsT24\Infraestructura.wsT24.ACCTBALWSType.datasource">
      <DependentUpon>Reference.svcmap</DependentUpon>
    </None>
    <None Include="Connected Services\wsT24\Infraestructura.wsT24.CONSULTASALDOCUENTAResponse.datasource">
      <DependentUpon>Reference.svcmap</DependentUpon>
    </None>
    <None Include="Connected Services\wsT24\Infraestructura.wsT24.Status.datasource">
      <DependentUpon>Reference.svcmap</DependentUpon>
    </None>
    <None Include="Connected Services\wsT24\services.xsd">
      <SubType>Designer</SubType>
    </None>
    <None Include="Connected Services\wsT24\services1.xsd">
      <SubType>Designer</SubType>
    </None>
    <None Include="Connected Services\wsT24\T24WebServicesImplService.wsdl" />
    <None Include="packages.config" />
  </ItemGroup>
  <ItemGroup>
    <WCFMetadata Include="Connected Services\" />
  </ItemGroup>
  <ItemGroup>
    <WCFMetadataStorage Include="Connected Services\wsT24\" />
  </ItemGroup>
  <ItemGroup>
    <None Include="Connected Services\wsT24\configuration91.svcinfo" />
  </ItemGroup>
  <ItemGroup>
    <None Include="Connected Services\wsT24\configuration.svcinfo" />
  </ItemGroup>
  <ItemGroup>
    <None Include="Connected Services\wsT24\Reference.svcmap">
      <Generator>WCF Proxy Generator</Generator>
      <LastGenOutput>Reference.cs</LastGenOutput>
    </None>
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\AplicacionDespachador.csproj">
      <Project>{F6FEFE84-AC9F-4F39-898A-82391CA288FD}</Project>
      <Name>AplicacionDespachador</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


Despachador.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{D9C51065-C04C-4662-9EC4-D0262F94960E}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>Despachador</RootNamespace>
    <AssemblyName>Despachador</AssemblyName>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
    <Deterministic>true</Deterministic>
    <NuGetPackageImportStamp>
    </NuGetPackageImportStamp>
    <RunSettingsFilePath>
    </RunSettingsFilePath>
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
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=MSIL">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <HintPath>..\packages\SimpleInjector.5.5.0\lib\net461\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
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
    <ProjectReference Include="..\Aplicacion\AplicacionDespachador.csproj">
      <Project>{f6fefe84-ac9f-4f39-898a-82391ca288fd}</Project>
      <Name>AplicacionDespachador</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura\InfraestructuraDespachador.csproj">
      <Project>{f1f018e7-2859-4225-8b6b-25884411d167}</Project>
      <Name>InfraestructuraDespachador</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


Despachador.sln

Microsoft Visual Studio Solution File, Format Version 12.00
# Visual Studio Version 18
VisualStudioVersion = 18.4.11612.150
MinimumVisualStudioVersion = 10.0.40219.1
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "Despachador", "Despachador\Despachador.csproj", "{D9C51065-C04C-4662-9EC4-D0262F94960E}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "AplicacionDespachador", "Aplicacion\AplicacionDespachador.csproj", "{F6FEFE84-AC9F-4F39-898A-82391CA288FD}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "InfraestructuraDespachador", "Infraestructura\InfraestructuraDespachador.csproj", "{F1F018E7-2859-4225-8B6B-25884411D167}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "PruebasUnitarias", "PruebasUnitarias\PruebasUnitarias.csproj", "{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}"
EndProject
Global
	GlobalSection(SolutionConfigurationPlatforms) = preSolution
		Debug|Any CPU = Debug|Any CPU
		Debug|x64 = Debug|x64
		Debug|x86 = Debug|x86
		Release|Any CPU = Release|Any CPU
		Release|x64 = Release|x64
		Release|x86 = Release|x86
	EndGlobalSection
	GlobalSection(ProjectConfigurationPlatforms) = postSolution
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Debug|x64.ActiveCfg = Debug|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Debug|x64.Build.0 = Debug|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Debug|x86.ActiveCfg = Debug|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Debug|x86.Build.0 = Debug|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Release|Any CPU.Build.0 = Release|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Release|x64.ActiveCfg = Release|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Release|x64.Build.0 = Release|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Release|x86.ActiveCfg = Release|Any CPU
		{D9C51065-C04C-4662-9EC4-D0262F94960E}.Release|x86.Build.0 = Release|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Debug|x64.ActiveCfg = Debug|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Debug|x64.Build.0 = Debug|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Debug|x86.ActiveCfg = Debug|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Debug|x86.Build.0 = Debug|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Release|Any CPU.Build.0 = Release|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Release|x64.ActiveCfg = Release|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Release|x64.Build.0 = Release|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Release|x86.ActiveCfg = Release|Any CPU
		{F6FEFE84-AC9F-4F39-898A-82391CA288FD}.Release|x86.Build.0 = Release|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Debug|x64.ActiveCfg = Debug|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Debug|x64.Build.0 = Debug|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Debug|x86.ActiveCfg = Debug|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Debug|x86.Build.0 = Debug|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Release|Any CPU.Build.0 = Release|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Release|x64.ActiveCfg = Release|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Release|x64.Build.0 = Release|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Release|x86.ActiveCfg = Release|Any CPU
		{F1F018E7-2859-4225-8B6B-25884411D167}.Release|x86.Build.0 = Release|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Debug|x64.ActiveCfg = Debug|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Debug|x64.Build.0 = Debug|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Debug|x86.ActiveCfg = Debug|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Debug|x86.Build.0 = Debug|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Release|Any CPU.Build.0 = Release|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Release|x64.ActiveCfg = Release|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Release|x64.Build.0 = Release|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Release|x86.ActiveCfg = Release|Any CPU
		{51EFD50B-5ECD-4514-8C9F-DB256974CAE3}.Release|x86.Build.0 = Release|Any CPU
	EndGlobalSection
	GlobalSection(SolutionProperties) = preSolution
		HideSolutionNode = FALSE
	EndGlobalSection
	GlobalSection(ExtensibilityGlobals) = postSolution
		SolutionGuid = {51742635-70C4-494A-AEEE-CAB16E6A0F37}
	EndGlobalSection
EndGlobal


app.config
﻿<?xml version="1.0" encoding="utf-8" ?>
<configuration>
    <system.serviceModel>
        <bindings />
        <client />
    </system.serviceModel>
</configuration>


packages.config
﻿<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="Newtonsoft.Json" version="13.0.4" targetFramework="net48" />
  <package id="System.Runtime.Serialization.Primitives" version="4.3.0" targetFramework="net48" />
</packages>



.editorconfig
root = true

[*.cs]

# Habilitar warnings importantes
dotnet_diagnostic.CS1998.severity = warning   # async sin await
dotnet_diagnostic.CS4014.severity = silent   # tarea no esperada
dotnet_diagnostic.CA2007.severity = silent   # ConfigureAwait

# Nivel general
dotnet_analyzer_diagnostic.severity = silent



--------- Tabla para el control de estado por servicio:
-- Es decir, cuantos workers puede tener m�ximo un servicio y su prioridad con respecto al resto de servicios.
CREATE TABLE dbo.tb_srv_controlServicio
(
      servicio              VARCHAR(30) NOT NULL
	, ip_servicio			VARCHAR(50) NOT NULL DEFAULT '127.0.0.1'
	, pid_servicio			INT NULL
    , max_workers           INT NOT NULL
    , prioridad             INT NOT NULL DEFAULT (1)
    , activo                BIT NOT NULL DEFAULT (1)
    , ultima_actualizacion  DATETIME2 NOT NULL DEFAULT SYSDATETIME()

    , CONSTRAINT PK_tb_srv_controlServicio
        PRIMARY KEY (servicio)
);
INSERT INTO tb_srv_controlServicio (servicio, max_workers, prioridad)
VALUES
('T24', 7, 1),
('Lectura', 7, 2),
('HotScan', 7, 1),
('ActualizaEstatus', 7, 2);

--------- Tabla para el control de estado por tipo de operacion
-- Es decir, cuantos workers puede tener m�ximo una operaci�n dentro de un mismo servicio.
CREATE TABLE dbo.tb_srv_controlTipo
(
      servicio              VARCHAR(30) NOT NULL
    , tipo_operacion         VARCHAR(10) NOT NULL

    , max_workers            INT NOT NULL
    , prioridad              INT NOT NULL

    , activo                 BIT NOT NULL DEFAULT (1)
    , ultima_actualizacion   DATETIME2 NOT NULL DEFAULT SYSDATETIME()

    , CONSTRAINT PK_tb_srv_controlTipo
        PRIMARY KEY (servicio, tipo_operacion)

    , CONSTRAINT FK_tb_srv_controlTipo_servicio
        FOREIGN KEY (servicio)
        REFERENCES dbo.tb_srv_controlServicio(servicio)
);

--------------- TVP
CREATE TYPE dbo.tb_despachador_folioConcatenado AS TABLE
(
    Folio BIGINT NOT NULL
);
GO


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_InsertarFechaProcesadoSinFondos
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Dom�nguez
FECHA               :  2026-04-24

DESCRIPCI�N         : 
Inserta el registro de control para una operaci�n marcada como SIN FONDOS (SF), almacenando la fecha/hora de procesamiento.

ALCANCE / NOTAS     :
- Operaci�n DML (INSERT).
- Se espera insertar una fila por (folio, operacion, status).
- Este procedimiento es invocado cuando no existe registro previo
  en TB_SRV_T24_MW_ADMON_FOLIOS para el folio, operaci�n y
  status SF.

PAR�METROS          :
@Folio         NUMERIC(18,0)
@TipoOperacion VARCHAR(50)

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Dom�nguez | Creaci�n SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_InsertarFechaProcesadoSinFondos
(
      @Folio         NUMERIC(18,0)
    , @TipoOperacion VARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @FilasAfectadas INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        INSERT INTO dbo.TB_SRV_T24_MW_ADMON_FOLIOS
        (
              FOLIO
            , OPERACION
            , FEC_PROCESS
            , STATUS
        )
        VALUES
        (
              @Folio
            , @TipoOperacion
            , GETDATE()
            , 'SF'
        );

        SET @FilasAfectadas = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT
              @FilasAfectadas AS FilasAfectadas
            , CAST(CASE WHEN @FilasAfectadas > 0 THEN 1 ELSE 0 END AS BIT) AS Afecto;
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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_obtenerT24RegistroOperaciones
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Dom�nguez
FECHA               :  2026-04-24

DESCRIPCI�N         : 
Obtiene los registros cuyo alias_proceso sea 'T24_REGISTRO'  (solo se usa con registros para T24).

ALCANCE / NOTAS     :
- Operaci�n de lectura (SELECT).
- Se espera retornar filas por (alias_proceso).
- Uso de CTE para obtener el paso actual por folio.

PAR�METROS          :
@TipoOperacion VARCHAR(20),
@Folios dbo.tb_despachador_folioConcatenado READONLY

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Dom�nguez | Creaci�n SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_obtenerT24RegistroOperaciones
(
      @TipoOperacion VARCHAR(20),
      @Folios dbo.tb_despachador_folioConcatenado READONLY
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

        ;WITH PasoActual AS
        (
            SELECT
                  detalle.folio
                , detalle.alias_proceso
                , rn = ROW_NUMBER() OVER
                (
                    PARTITION BY detalle.folio
                    ORDER BY detalle.orden_ejecucion ASC
                )
            FROM dbo.TB_SRV_PLAN_TRABAJO_DETALLE detalle
            INNER JOIN @Folios folios
                ON folios.Folio = detalle.folio
            WHERE
                    detalle.tipo_operacion = @TipoOperacion
                AND detalle.flg_terminado = 0
        )
        SELECT
            folio AS Folio
        FROM PasoActual
        WHERE
                rn = 1
            AND alias_proceso = 'T24_REGISTRO';

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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_RevisarRECCBatch
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-04-24

DESCRIPCIÓN         :
Obtiene el folio de los registros que existen en la tabla correspondiente al tipo de operación y cumplen la condición RECC.

ALCANCE / NOTAS     :
- Operación de lectura (SELECT).
- Se espera retornar filas por (nf_estatus).

PARÁMETROS          :
@TipoOperacion VARCHAR(20),
@Folios dbo.tb_despachador_folioConcatenado READONLY

CONTROL DE CAMBIOS  :
2026-04-24 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_RevisarRECCBatch
(
      @TipoOperacion VARCHAR(20)
    , @Folios dbo.tb_despachador_folioConcatenado READONLY
)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        IF @TipoOperacion = 'FXD'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxd tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXI'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxi tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXT'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxt tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'NOM_FXD'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxd_nomina_detalle tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'PaP_FXD'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxd_pap_detalle tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'PaP_FXI'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxi_pap_detalle tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'PaP_FXT'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxt_pap_detalle tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXD_Local'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxd_local tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXI_Local'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxi_local tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXT_Local'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxt_local tablaCorrespondiente
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'SAT'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_cms_sat tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'MFX'
        BEGIN
            SELECT
                tablaCorrespondiente.id_autorizacion AS Folio
            FROM dbo.tb_cmm_solicitudesforex tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.id_autorizacion;
        END
        ELSE IF @TipoOperacion = 'SPID'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_spid tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'SPID_Local'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_spid_local tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'PaP_SPID'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_spid_pap_detalle tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'TEF'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_tef tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'Pap_TEF'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_tef_pap_detalle tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'TMD'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_tmd tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXD_GPH'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxd_gph tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXI_GPH'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxi_gph tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXT_GPH'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxt_gph tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'SPID_GPH'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_spid_gph tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'TEF_GPH'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_tef_gph tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXD_H2H'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxd_h2h tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXI_H2H'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxi_h2h tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'FXT_H2H'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_fxt_h2h tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'SPID_H2H'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_spid_h2h tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'TEF_H2H'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_tef_h2h tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'SER'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_ser_pay tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE IF @TipoOperacion = 'TAX'
        BEGIN
            SELECT
                tablaCorrespondiente.nf_folio AS Folio
            FROM dbo.tb_tombin_nf_tax_pay tablaCorrespondiente WITH (NOLOCK)
            INNER JOIN @Folios folios
                ON folios.Folio = tablaCorrespondiente.nf_folio
            WHERE tablaCorrespondiente.nf_estatus = 'RECC';
        END
        ELSE
        BEGIN
            -- Tipo desconocido: retorna 0 filas (comportamiento seguro)
            SELECT
                CAST(NULL AS BIGINT) AS Folio
            WHERE 1 = 0;
        END

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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_verificarSinFondos
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-04-24

DESCRIPCIÓN         : 
Obtiene el valor fec_process de los registros cuyo estatus sea SIN FONDOS (SF).

ALCANCE / NOTAS     :
- Operación de lectura (SELECT).
- Se espera retornar filas por (folio, operacion, status).

PARÁMETROS          :
@Folio         NUMERIC(18,0)
@TipoOperacion VARCHAR(50)

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_verificarSinFondos
(
      @Folio         NUMERIC(18,0)
    , @TipoOperacion VARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        SELECT
            fec_process
        FROM dbo.tb_srv_t24_mw_admon_folios
        WHERE
                folio     = @Folio
            AND operacion = @TipoOperacion
            AND status    = 'SF';

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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_actualizaCuentaSinFondos
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-04-24

DESCRIPCIÓN         : 
Actualiza el plan de trabajo maestro haciendo explícito el error debido a la falta de fondos de la cuenta.

ALCANCE / NOTAS     :
- Operación DML.
- Se espera actualizar filas por (tipo_operacion, folio).

PARÁMETROS          :
@Folio         NUMERIC(18,0)
@TipoOperacion VARCHAR(50)

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_actualizaCuentaSinFondos
(
      @Folio         NUMERIC(18,0)
    , @TipoOperacion VARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @FilasAfectadas INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE dbo.tb_srv_plan_trabajo_maestro
        SET
              flg_con_error        = 0
            , num_estatus_especial = 2
            , err_descripcion      = 'La cuenta no tiene los fondos para cubrir la transacción.'
        WHERE
                tipo_operacion = @TipoOperacion
            AND folio = CAST(@Folio AS BIGINT);

        SET @FilasAfectadas = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT
              @FilasAfectadas AS FilasAfectadas
            , CAST(CASE WHEN @FilasAfectadas > 0 THEN 1 ELSE 0 END AS BIT) AS Afecto;
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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_actualizaFechaProcesadoSinFondos
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-04-24

DESCRIPCIÓN         : 
Actualiza la fecha en la tabla de intentos mientras la operación sea folio, operación y status.

ALCANCE / NOTAS     :
- Operación DML.
- Se espera actualizar filas por (folio, operacion, status).

PARÁMETROS          :
@Folio         NUMERIC(18,0)
@TipoOperacion VARCHAR(50)

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_actualizaFechaProcesadoSinFondos
(
      @Folio         NUMERIC(18,0)
    , @TipoOperacion VARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @FilasAfectadas INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE dbo.TB_SRV_T24_MW_ADMON_FOLIOS
        SET
            fec_process = GETDATE()
        WHERE
                folio     = @Folio
            AND operacion = @TipoOperacion
            AND status    = 'SF';

        SET @FilasAfectadas = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT
              @FilasAfectadas AS FilasAfectadas
            , CAST(CASE WHEN @FilasAfectadas > 0 THEN 1 ELSE 0 END AS BIT) AS Afecto;
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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_eliminarSinFondos
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-04-24

DESCRIPCIÓN         : 
Elimina el registro SIN FONDOS (historial)filtrando por folio, tipo de operación y status.

ALCANCE / NOTAS     :
- Operación DML.
- Se eliminan filas por (folio, operacion, status).

PARÁMETROS          :
@Folio         NUMERIC(18,0)
@TipoOperacion VARCHAR(50)

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_eliminarSinFondos
(
      @Folio         NUMERIC(18,0)
    , @TipoOperacion VARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @FilasAfectadas INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        DELETE
        FROM dbo.TB_SRV_T24_MW_ADMON_FOLIOS
        WHERE
                folio     = @Folio
            AND operacion = @TipoOperacion
            AND status    = 'SF';

        SET @FilasAfectadas = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT
              @FilasAfectadas AS FilasAfectadas
            , CAST(CASE WHEN @FilasAfectadas > 0 THEN 1 ELSE 0 END AS BIT) AS Afecto;
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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_obtenerControlServicio
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-04-24

DESCRIPCIÓN         : 
Obtiene los servicios despachables por el servicio Despachador, junto con sus prioridades, máximos, IP y puerto.

ALCANCE / NOTAS     :
- Operación de lectura (SELECT).
- Se espera retornar filas por (activo, operando).

PARÁMETROS          :
-

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_obtenerControlServicio
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        SELECT
              control.servicio    AS Servicio
            , control.max_workers AS Maximos
            , control.prioridad   AS Prioridad
            , control.ip_servicio AS Ip
            , servicio.puerto     AS Puerto
        FROM dbo.tb_srv_controlServicio AS control
        LEFT JOIN dbo.tb_srv_puerto_x_servicio AS servicio
            ON control.pid_servicio = servicio.pid
        WHERE
                control.activo   = 1
            AND servicio.operando = 1;

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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_obtenerControlTipo
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-04-24

DESCRIPCIÓN         : 
Obtiene los tipos despachables (respectivos al servicio) por el servicio Despachador, junto con sus prioridades y máximos.

ALCANCE / NOTAS     :
- Operación de lectura (SELECT).
- Se espera retornar filas por (activo).

PARÁMETROS          :
-

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_obtenerControlTipo
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        SELECT
              servicio       AS Servicio
            , tipo_operacion AS Tipo
            , max_workers    AS CantidadMaxima
            , prioridad      AS Prioridad
        FROM dbo.tb_srv_controlTipo
        WHERE activo = 1;

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


USE [InterfacesT24]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************
NOMBRE              :  dbo.usp_despachador_ObtenerOperacionesPendientes
SISTEMA/BD          :  InterfacesT24
AUTOR               :  Rafael Domínguez
FECHA               :  2026-04-24

DESCRIPCIÓN         : 
Obtiene todos los registros que el despachador debe "ver" como pendientes por despachar.

ALCANCE / NOTAS     :
- Operación de lectura (SELECT).
- Se espera retornar una fila por operación.
- Se ignoran operaciones marcadas con error.

PARÁMETROS          :
@@TopN INT

CONTROL DE CAMBIOS  : 
2026-04-24 | Rafael Domínguez | Creación SP
***********************************************************************/
CREATE OR ALTER PROCEDURE dbo.usp_despachador_ObtenerOperacionesPendientes
(
      @TopN INT
)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        ;WITH det_actual AS
        (
            SELECT
                  detalle.servicio
                , detalle.tipo_operacion
                , detalle.folio
                , detalle.orden_ejecucion
                , detalle.fec_inicio
                , rnPaso = ROW_NUMBER() OVER
                (
                    PARTITION BY detalle.folio, detalle.tipo_operacion
                    ORDER BY detalle.orden_ejecucion ASC
                )
            FROM dbo.tb_srv_plan_trabajo_detalle detalle
            WHERE
                    detalle.flg_terminado = 0
                AND detalle.servicio IN ('Lectura', 'T24', 'ActualizaEstatus', 'HotScan')
        ),
        elegibles AS
        (
            SELECT
                  det_actual.servicio         AS Servicio
                , det_actual.tipo_operacion   AS Tipo_Operacion
                , det_actual.folio            AS Folio
                , maestro.num_cuenta          AS Num_Cuenta
                , CAST(maestro.importe AS DECIMAL(18,2)) AS Importe
                , CAST(maestro.fec_valor AS INT)         AS Fec_Valor
                , det_actual.orden_ejecucion  AS Orden_Ejecucion
                , det_actual.fec_inicio       AS Fec_Inicio_Detalle
                , controltipo.prioridad       AS PrioridadTipo
                , rnTipo = ROW_NUMBER() OVER
                (
                    PARTITION BY det_actual.servicio, det_actual.tipo_operacion
                    ORDER BY maestro.fec_valor, det_actual.fec_inicio, det_actual.folio
                )
            FROM det_actual
            INNER JOIN dbo.tb_srv_plan_trabajo_maestro maestro
                ON  maestro.folio          = det_actual.folio
                AND maestro.tipo_operacion = det_actual.tipo_operacion
            INNER JOIN dbo.tb_srv_controltipo controltipo
                ON  controltipo.servicio       = det_actual.servicio
                AND controltipo.tipo_operacion = det_actual.tipo_operacion
                AND controltipo.activo         = 1
            WHERE
                    det_actual.rnPaso = 1
                AND maestro.flg_terminado = 0
                AND maestro.flg_cierre    = 0
                AND
                (
                       ISNULL(maestro.flg_con_error, 0) = 0
                    OR maestro.num_estatus_especial = 2
                )
        )
        SELECT TOP (@TopN)
              Servicio
            , Tipo_Operacion
            , Folio
            , Num_Cuenta
            , Importe
            , Fec_Valor
            , Orden_Ejecucion
        FROM elegibles
        ORDER BY
              PrioridadTipo
            , rnTipo
            , Fec_Valor
            , Fec_Inicio_Detalle;

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


Product.wxs
<Wix xmlns="http://wixtoolset.org/schemas/v4/wxs">
  <Package
      Name="Despachador"
      Manufacturer="MUFG Bank México"
      Version="1.0.0.0"
      UpgradeCode="2f1309ae-1d16-4f37-b0bb-17afc96b6d5e"
      Scope="perMachine">
    <MajorUpgrade DowngradeErrorMessage="Ya existe una versión instalada." />
    <MediaTemplate EmbedCab="yes" />
    <StandardDirectory Id="ProgramFiles64Folder">
      <Directory Id="INSTALLFOLDER" Name="Despachador" />
    </StandardDirectory>
    <Feature Id="MainFeature">
  	    <ComponentRef Id="ServiceExe" />
	      <ComponentRef Id="Bin_Config" />
  	    <ComponentRef Id="Bin_AppSettings" />
  	    <ComponentRef Id="Bin_AplicacionDll" />
  	    <ComponentRef Id="Bin_InfraestructuraDll" />
    </Feature>
  </Package>
  <Fragment>
      <Component Id="ServiceExe" Guid="*" Directory="INSTALLFOLDER" Bitness="always64">
        <File Source="$(var.Despachador.TargetDir)Despachador.exe" KeyPath="yes"/>
        <ServiceInstall
              Name="Despachador"
              DisplayName="Despachador"
              Description="Servicio Despachador"
              Type="ownProcess"
              Start="auto"
              ErrorControl="normal"
              Account="NT AUTHORITY\LocalService" />
              <ServiceControl
            Name="Despachador"
                  Start="install"
                  Stop="both"
                  Remove="uninstall"
                  Wait="yes" />
      </Component>
      <Component Id="Bin_Config"
           Guid="*"
           Directory="INSTALLFOLDER"
           Bitness="always64">
        <File Source="$(var.Despachador.TargetDir)Despachador.exe.config"
              KeyPath="yes"/>
      </Component>

      <Component Id="Bin_AppSettings"
                Guid="*"
                Directory="INSTALLFOLDER"
                Bitness="always64">
        <File Source="$(var.Despachador.TargetDir)appsettings.json"
              KeyPath="yes"/>
      </Component>

      <Component Id="Bin_AplicacionDll"
                Guid="*"
                Directory="INSTALLFOLDER"
                Bitness="always64">
        <File Source="$(var.Despachador.TargetDir)Aplicacion.dll"
              KeyPath="yes"/>
      </Component>

      <Component Id="Bin_InfraestructuraDll"
                Guid="*"
                Directory="INSTALLFOLDER"
                Bitness="always64">
        <File Source="$(var.Despachador.TargetDir)Infraestructura.dll"
              KeyPath="yes"/>
      </Component>        
  </Fragment>
</Wix>


Despachador.wixproj
<Project Sdk="WixToolset.Sdk/7.0.0">
  <PropertyGroup>
    <OutputType>Package</OutputType>
    <Platform>x64</Platform>
    <InstallerPlatform>x64</InstallerPlatform>
    <PackageName>Despachador</PackageName>
    <EnableDefaultItems>true</EnableDefaultItems>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\Despachador\Despachador.csproj" />
  </ItemGroup>
</Project>


ArquitecturaCifradoTCP.md
# Arquitectura de cifrado TCP (`PeticionTCP`)

## Objetivo
Documentar cómo emisor y receptor cifran/descifran el payload TCP para el despacho de operaciones.

## Mecanismo
1. El emisor (`Infraestructura/TCP/PeticionTCP.cs`) genera la llave de cifrado mediante `Encriptacion.GenerarLlave()`.
2. La llave **no viaja en el stream TCP**.
3. Emisor y receptores obtienen la misma llave desde configuración general compartida: `LLAVE_ENCRIPTADODESPACHADOR`.
4. El `VECTOR_IV` es dinámico por mensaje y se serializa junto con el mensaje cifrado por la librería común `MUFG_T24.Encriptacion_utilidades`.
5. El receptor usa la misma llave configurada para descifrar el payload.

## Parámetros requeridos
- `LLAVE_ENCRIPTADODESPACHADOR` (obligatorio en todos los nodos emisores/receptores).

## Consideraciones operativas
- No registrar en logs la llave ni material criptográfico.
- Al rotar `LLAVE_ENCRIPTADODESPACHADOR`, aplicar la rotación de forma coordinada en emisor y receptores.
- Validar conectividad/cifrado con prueba de extremo a extremo después de cualquier cambio de llave.




