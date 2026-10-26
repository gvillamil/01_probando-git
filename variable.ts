Swf IN ControladorServicios Recommendations
Swf_IN_ControladorServicios – Architecture & Code Quality Recommendations
Solution: Swf_IN_ControladorServicios.sln (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/Swf_IN_ControladorServicios.sln). Reviewed: 2026-10-15, branch CodeReview, commit 93404d5d35872d6d0c5a4d0195a2346847cef1f9 (UT CodeReview). Compared against: LoansApp coding-practices baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR review/severity checklist (../BaseLine_Docs/code_review.md). See also: _ORCHESTRATION.md (./_ORCHESTRATION.md) (methodology, template, standing findings GEN-#). Phase 0-1 context: application understanding §4.7.1 (../Application_understanding_technical_document_master.md), dependency ladder (../excecutive_docs/Base_Project_Dependency_Ladder.md). Target: .NET Framework 4.8 Windows Service; focus: all categories; severity threshold: all. Audit only: no application fixes were applied. JIRA prefix: SIC-FR-#.
1. Solution Overview (low/no-dependency -> high dependency)
Tier	Project	Kind	Path
0	Aplicacion	Class library (net48, x86) – orchestration logic, interfaces, models, Windows-service control	Aplicacion (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion)
1	Infraestructura	Class library (net48, x86) – RepositorioTomi stored-procedure access (references Aplicacion)	Infraestructura (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura)
2	Swf_IN_ControladorServicios	Windows Service host (Exe, net48, x86) – SimpleInjector composition root, timer tick	Swf_IN_ControladorServicios (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios)
2	PruebasUnitarias_Swf_IN_ControladorServicios	MSTest 2.2.10 project (9 [TestMethod], hand-written fakes)	PruebasUnitarias_Swf_IN_ControladorServicios (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios)

External dependency of all four projects: the shared MUFG_T24.dll via a raw D:\ServiceCMS\MUFG_T24.dll HintPath (source read for contract confirmation at General/MUFGT24_BTMUM_64 (../../General/MUFGT24_BTMUM_64)).
What this solution actually does: it is the *incoming-SWIFT supervisor*. It is a timer-driven Windows Service (interval and enable flag come from the shared central configuration; fallback 10 s, ServiceHost.cs:33-34 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs)). At start-up it registers its own process (PID, process name, display name) in the database and marks its port/service row as in use. On every tick it reads the TOMI system date, asks the shared RepositorioMaestro whether today is a business day (weekend + tb_srv_diasnohabiles for MXN/MXP), reads HORA_OP_INICIO/HORA_OP_FIN from tb_srv_parametros_sistema, and decides whether the *business day is open*. If it is, it starts the two Windows services CMS-Swf_IN_ETL and CMS-Swf_IN_FT (the incoming-SWIFT ETL and file-transfer workers); otherwise it stops both (OperacionServicio.cs:108-131 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)). It then stamps status=0 on its own tb_srv_controladoradeservicios row and, on stop, releases the port row. It processes no SWIFT messages itself; its correctness determines whether the two workers that do are running during operating hours.
2. Scope & Methodology Note
• Full read, no sampling. All 14 authored C# files (1,145 physical lines), 4 AssemblyInfo.cs, 4 .csproj, the .sln,
2 packages.config, App.config, README.md (GitLab boilerplate, no content) and .gitignore were read in full, line by line (28 authored files; git internals, restored packages/ and .vs/ excluded from line review but counted for hygiene). The solution is below the large-project threshold (150 files / 5,000 lines), so no progress-tracker file was needed; it was completed in a single pass.
• Cross-reads of dependencies (to avoid false positives on shared behavior): WindowsServiceHostBase (tick lock,
start-up contract), Config.EscribirLog/Logging.WriteLegacy, ConversionHelper.ParseTimeSpanRequerido, Configuracion.Clone, RepositorioMaestro.ActualizaEJecucion/ActualizaTerminacion/EsDiaHabil, MUFG_T24.ControladorServicios_Utilidades.ControladorServicios, and the SQL project's usp_*_SwfInCtrlSrv, usp_EsDiaHabil, usp_ObtenerFechaSistema, usp_ActualizaEjecucion_MUFGLibreria, usp_ActualizaTerminacion_HotScan scripts.
• Quantified cross-checks (supplement to the full read): Config.EscribirLog( 40 calls (15 in RepositorioTomi, 0 with a level
prefix); 17 catch (Exception; 13 throw;; 0 throw ex;; 0 .Result/.Wait(); 9 new Exception(; 5 ProcesoInsertaAlerta(; 3 (int) casts of Pid; 9 [TestMethod]. Five structurally identical copies of ControladorServicios.cs exist in the portfolio (see SIC-FR-3).
• Not verified: nothing was built or run. D:\ServiceCMS\MUFG_T24.dll does not exist on this machine and neither msbuild nor
dotnet is on PATH. The deployed MUFG_T24.dll version is unknown (findings rely on the source in General\MUFGT24_BTMUM_64). Windows ServiceController.Start/Stop behavior on already-running/stopped services is stated from documented .NET behavior, not from a run on the target server. Timer interval, service account rights and the real content of tb_srv_* tables are deployment facts not visible in the repository.
• Snapshot note: the working tree had 44 tracked files under Swf_IN_ControladorServicios/packages/ deleted (uncommitted). Findings
are on committed source; no authored .cs file differed from HEAD.
• The checklist's work-item/changed-lines rules are PR-specific; this is an as-is audit and does not assert acceptance against a story.
3. What's Already Good
• Clean layering and constructor injection: Aplicacion (rules + interfaces) -> Infraestructura (data) -> host
(composition root); OperacionServicio, ControladorServicios and RepositorioTomi take dependencies by constructor and the business flow depends on IRepositorioTomi/IControladorServicios abstractions (OperacionServicio.cs:19-35 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs), IRepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Interfaces/IRepositorioTomi.cs)).
• SimpleInjector with `Verify()` and scoped lifetimes: one AsyncScopedLifestyle scope per tick, _containerDI.Verify() at start-up,
container disposed in a finally (ServiceHost.cs:50-97, 137-141, 188-191 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs)).
• Shared host base used correctly: overlapping ticks are prevented by the base's SemaphoreSlim tick lock; the timer starts only
after the PID/process-name/display-name contract has been validated; stop has timeouts and cleanup tokens.
• No sync-over-async, no `async void`, no `throw ex;`: scans return 0 for .Result, .Wait(), GetResult(), throw ex;; the 13 rethrows all use throw;.
ConfigureAwait(false) is used in the host's async calls.
• Blocking service-control calls are off the async context: ServiceController.Start/Stop/WaitForStatus run inside Task.Run and the
ServiceController is wrapped in using (ControladorServicios.cs:39-47, 90-98 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs)).
• Parameterized stored procedures only: no dynamic SQL or string-concatenated queries in this solution; the SQL project's
procedures use BEGIN TRY/ROLLBACK, audit logging, and bounded VARCHAR(50) parameters.
• Fail-fast configuration parsing: ParseTimeSpanRequerido rejects empty/invalid hours and ObtenerParametros throws when either bound is
missing (OperacionServicio.cs:52, 58, 91-95 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)); tests exercise both missing-bound cases (UnitTest1.cs:139-169 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/UnitTest1.cs)).
• A real test project exists and is wired into the solution: MSTest, isolated by interface fakes, no database or Windows-service dependency,
deterministic for its nine scenarios (but see SIC-FR-5).
• Security hygiene: no secrets, credentials, cryptography, deserialization or plaintext-secret logging in any file; child service names are
compile-time constants (no injection into ServiceController).
• Graceful shutdown: TerminacionAsync waits for the start-up and running tasks, honors cancellation, and releases the DB state
(ServiceHost.cs:152-192 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs)).
4. Findings by Project (low/no-dependency -> high dependency)
Severity legend (from code_review.md): Blocker must fix; Major fix unless waived; Minor fix or track; Info suggestion. Finding IDs SIC-FR-1 ... SIC-FR-21 are final and match the backlog in section 6.
4.1 Aplicacion (Tier 0)
MustChange / Rewrite
SIC-FR-1 – Start/stop success checks are inverted, so a successful operation is reported as a failure (Blocker)
• File:Line: ControladorServicios.cs:51-60 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs) (start) and
ControladorServicios.cs:102-111 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs) (stop).
• Current code:
```csharp // IniciaServicioAsync, after Start() + WaitForStatus(Running, 30 s) + Refresh() if (serviceController.Status != ServiceControllerStatus.Running) { Config.EscribirLog($"INFO|IniciaServicio|Servicio {strNombreServicio} iniciado correctamente."); return true; } else { Config.EscribirLog($"INFO|IniciaServicio|Ocurrio un problema al iniciar el servicio {strNombreServicio}"); return false; }
// DetieneServicioAsync, after Stop() + WaitForStatus(Stopped, 30 s) + Refresh() if (serviceController.Status != ServiceControllerStatus.Stopped && serviceController.Status != ServiceControllerStatus.StopPending) { ... "Finalización del Servicio" ... return true; } else { ... "Ocurrio un problema al detener el servicio." ... return false; } ```
• Issue: both predicates are negated. When the service really reaches Running (or Stopped) the code takes the else branch,
logs "problema" and returns false; the true branch is only reachable when the wait *did not* produce the target state. OperacionServicio then logs ERROR|Uno o mas servicios no alcanzaron el estado deseado (OperacionServicio.cs:133-136 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)) for the normal outcome, and the success INFO is effectively unreachable. The unit tests cannot detect this because ControladorServiciosFake always returns true (UnitTest1.cs:252-272 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/UnitTest1.cs)). The same pattern exists in 4 other solutions (GEN-15).
• Recommended code: compare to the expected state, and treat StopPending as *not yet* stopped:
``csharp serviceController.Refresh(); bool ok = serviceController.Status == ServiceControllerStatus.Running;          // start // bool ok = serviceController.Status == ServiceControllerStatus.Stopped;       // stop Config.EscribirLog(ok ? $"INFO|IniciaServicio|Servicio {strNombreServicio} iniciado correctamente." : $"ERROR|IniciaServicio|Servicio {strNombreServicio} no alcanzó el estado Running."); return ok; `` Preferably delete the local class and use the shared implementation (see SIC-FR-3).
• Benefit: the controller's result and its summary log become truthful; monitoring on ERROR|Uno o mas servicios... becomes meaningful again.
• Suggested JIRA: SIC-FR-1 (Blocker)
SIC-FR-2 – Start()/Stop() are called unconditionally every tick, so steady state raises an ERROR log and an alert row per child service per tick (Blocker)
• File:Line: ControladorServicios.cs:39-47, 63-78, 90-98, 114-128 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs);
caller OperacionServicio.cs:120-131 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs).
• Current code:
``csharp using (ServiceController serviceController = new ServiceController(strNombreServicio, ".")) { await Task.Run(() => { serviceController.Start();          // never checks serviceController.Status first serviceController.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromSeconds(30)); }); ... catch (Exception ex) { Config.EscribirLog($"ERROR|IniciaServicio|Excepcion: {ex}"); await _repositorioTomi.ProcesoInsertaAlerta(new AlertaDTO() { ... ClaveAviso = "ERROR", ... Exception = ex }); } return false; ``
• Issue: ServiceController.Start() on a running service and Stop() on a stopped service throw InvalidOperationException
(inner Win32 error 1056 / 1062). The loop runs every timer tick (config-driven; 10000 ms fallback). In the normal steady state (inside the window the workers are already running; outside it they are already stopped) every tick therefore logs a full exception ERROR per child and inserts a new alert row per child: two alerts per tick, 720 alert rows per hour at a 10-second interval, with usp_InsertaAlerta_MUFGLibreria performing a plain insert (see GEN-10). Genuine failures are indistinguishable from this noise. Pending states (StartPending, StopPending, Paused) are not handled either. (Behavior stated from documented ServiceController semantics; not run on the target server.)
• Recommended code: make the operation idempotent and state-driven; only alert on a failed *transition*:
``csharp serviceController.Refresh(); if (serviceController.Status == ServiceControllerStatus.Running) return true;      // already in desired state if (serviceController.Status == ServiceControllerStatus.StartPending) { /* wait only */ } else serviceController.Start(); serviceController.WaitForStatus(ServiceControllerStatus.Running, timeout); ` The shared MUFG_T24.ControladorServicios_Utilidades.ControladorServicios.IniciaServicioConResultadoAsync/DetieneServicioConResultadoAsync already implements exactly this (already-running short-circuit, pending-state handling, typed catch for TimeoutException, InvalidOperationException, Win32Exception`, cancellation token and timeout parameters).
• Benefit: removes the alert/log storm, makes each real alert actionable, and removes two avoidable exceptions per tick from the host.
• Suggested JIRA: SIC-FR-2 (Blocker)
Rewrite Recommended
SIC-FR-3 – The same inverted, non-idempotent ControladorServicios is copy-pasted in five solutions while a correct shared implementation already exists (Major)
• File:Line: ControladorServicios.cs:1-132 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs).
• Evidence (count): a portfolio scan for new ServiceController( finds this class in SWF_IN_ControladorServicios, SWF_IN_ControladorServiciosMQ,
SWF_OUT_ControladorServicios, SWF_OUT_ControladorServiciosMQ and CEP_ControladorServicios; the scan for the inverted predicates (Status != ServiceControllerStatus.Running / Stopped && ... StopPending) matches all five (10 hits), plus the shared library's own General/MUFGT24_BTMUM_64/.../ControladorServicios.cs which does not contain them. The log text also shows the origin: OperacionServicio.cs:59 still says "Hora de cancelacion SPEI" (see SIC-FR-11). Tracked as GEN-15; the SWIFT pairs also relate to GEN-4.
• Current code: five near-identical copies (85-130 lines each, line numbers differ), each constructing ServiceController, waiting 30 s, and holding its own alert-insertion catch.
• Recommended code: delete the local class; register the shared one behind the existing IControladorServicios seam (adapter if the
Task<bool> signature is wanted): ``csharp // Aplicacion/Service/ControladorServiciosAdapter.cs public sealed class ControladorServiciosAdapter : Aplicacion.Interfaces.IControladorServicios { private readonly MUFG_T24.ControladorServicios_Utilidades.ControladorServicios _inner = new MUFG_T24.ControladorServicios_Utilidades.ControladorServicios(); public Task<bool> IniciaServicioAsync(string nombre) => _inner.IniciaServicioConResultadoAsync(nombre); public Task<bool> DetieneServicioAsync(string nombre) => _inner.DetieneServicioConResultadoAsync(nombre); } ` Alerting on a *failed transition* then belongs in OperacionServicio` (see SIC-FR-7), once, not in a per-copy catch.
• Benefit: one tested implementation instead of five; fixes SIC-FR-1/SIC-FR-2 in every consumer at once; honors cancellation and timeouts.
• Suggested JIRA: SIC-FR-3 (Major)
SIC-FR-4 – finally performs I/O and its throw; replaces the primary exception (Major)
• File:Line: OperacionServicio.cs:145-164 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs).
• Current code:
``csharp catch (Exception ex) { ...log...; throw; } finally { try { await _repositorioTomi.ProcesoActualizaTerminacion(_configuracion.StrNombreServicio, (int)_configuracion.Pid, _configuracion.NombreProceso); } catch (Exception exFinally) { Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|no fue posible maracar el servicio en finalización: {exFinally}"); throw; } } ``
• Issue: if the tick fails because the database is unreachable, the termination call fails for the same reason, and the throw; inside finally
replaces the original exception; the caller (ServiceHost.cs:143-146 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs)) logs only the secondary one. finally also does I/O (checklist §5: "finally used only to release resources"). (int)_configuracion.Pid throws InvalidOperationException if Pid were null (see SIC-FR-8).
• Recommended code: record the outcome and finalize outside the exception path, preserving the first failure:
``csharp Exception primary = null; try { await EjecutarCicloAsync(); } catch (Exception ex) { primary = ex; throw; } finally { await MarcarTerminacionSeguroAsync(primary); }   // logs its own failure, never throws over primary ``
• Benefit: the root cause is never hidden by a cleanup error; the lifecycle write is isolated and testable.
• Suggested JIRA: SIC-FR-4 (Major)
SIC-FR-5 – Tick lifecycle is unpaired: ActualizaTerminacion runs every tick, ActualizaEjecucion is never called (Major)
• File:Line: OperacionServicio.cs:156 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs); unused
IRepositorioTomi.cs:63 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Interfaces/IRepositorioTomi.cs) / RepositorioTomi.cs:217-237 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs).
• Issue: usp_ActualizaEjecucion_MUFGLibreria sets tb_srv_controladoradeservicios.status = 1; usp_ActualizaTerminacion_HotScan sets status = 0
(plus date/time). ProcesoActualizaEjecucion exists, is implemented and is in the interface, but nothing calls it. The row is therefore written status=0 after every tick *including while the service is running* and is never set to 1 — the opposite of the status other controllers publish (compare GEN-7). Whether a monitor or another service reads this flag for this controller is not visible in the repository. The shared terminate procedure is also named ..._HotScan, a naming leak (see GEN-13).
• Recommended code: decide the contract explicitly: either call ProcesoActualizaEjecucion at tick start and finalize with the outcome in finally
(SIC-FR-4), or remove both per-tick calls and keep the start-up/stop updates only. Document which table columns are the heartbeat.
• Benefit: persisted state reflects reality; removes a DB round-trip per tick if the flag is not used.
• Suggested JIRA: SIC-FR-5 (Major)
SIC-FR-6 – Operating-window evaluation is clock-coupled, unvalidated and cannot cross midnight (Major)
• File:Line: OperacionServicio.cs:88-89, 115-118 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs).
• Current code:
``csharp var horaActual = DateTime.Now; _configuracion.StrFechaSistemaServicio = new TimeSpan(horaActual.Hour, horaActual.Minute, 0); ... _configuracion.EnHorarioOperaciones = (StrFechaSistemaServicio >= HORA_OP_INICIO && StrFechaSistemaServicio <= HORA_OP_FIN); ``
• Issue: (a) DateTime.Now is read directly, so the "outside window" branch cannot be unit-tested (the existing tests pin 00:00–23:59 to stay
always inside); (b) no validation that HORA_OP_INICIO <= HORA_OP_FIN, and a window crossing midnight (e.g. 22:00–06:00) is never "in hours"; (c) the end bound is inclusive at minute granularity, so services are stopped at the first minute *after* HORA_OP_FIN, which may or may not be intended; (d) the business-day decision comes from the shared repository using the server's local calendar date while the window uses the local clock — two clock sources, and the TOMI system date fetched in line 80 is not used (SIC-FR-10); (e) TimeSpan? lifted comparisons make a null bound silently false rather than a failure (guarded today only by the earlier throw).
• Recommended code: inject Func<DateTime> _ahora (or ISystemClock), validate inicio < fin (or support an overnight window explicitly), and
extract static bool EstaEnHorario(TimeSpan ahora, TimeSpan inicio, TimeSpan fin) with table-driven tests for before/at/after each bound.
• Benefit: the decision that starts/stops two payment-adjacent workers becomes deterministic and testable; configuration mistakes fail fast.
• Suggested JIRA: SIC-FR-6 (Major)
SIC-FR-7 – Alerts fire for the expected state but not for real faults (Major)
• File:Line: alerts: ControladorServicios.cs:67-77, 117-127 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs);
no alert: OperacionServicio.cs:145-149 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs), ServiceHost.cs:143-146 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs), summary OperacionServicio.cs:133-140 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs).
• Issue: only the ServiceController catch inserts an alert (ProcesoInsertaAlerta has exactly two call sites in production code, both in ControladorServicios). A database failure, invalid/missing HORA_OP_*, or a false result from a child service ("one or more services did not reach the desired state")
only reaches the log; the Task.WhenAll result is never turned into an alert. Combined with SIC-FR-2 the alert table is full of expected-state noise while the failures that matter are silent.
• Recommended code: define one policy — one alert per failed transition (false result or exception) with the target service, desired state, and de-duplication;
alert on repeated tick-level failures (e.g. DB unavailable N times); never alert for "already in desired state".
• Benefit: operators receive few, actionable alerts. Related: GEN-10, GEN-11.
• Suggested JIRA: SIC-FR-7 (Major)
SIC-FR-8 – Nullable Pid is unwrapped without a HasValue check (Major)
• File:Line: ServiceHost.cs:119 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs) (_configuracionBase.Pid.Value);
OperacionServicio.cs:156 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs), ControladorServicios.cs:74, 124 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs) ((int)_configuracion.Pid).
• Issue: Configuracion.Pid is int?. At runtime the three tick-time casts are mitigated: the shared host validates Pid > 0 before the timer starts
(ValidarContratoInicializacionProceso) and the scoped config is a clone of _configuracionBase. Line 119 is not mitigated: it runs inside ParametrizacionesAsync, *before* that validation, so a null PID from ObtenerProcesoActualAsync raises InvalidOperationException (Nullable object must have a value) instead of the explicit contract error. In the alert catch (ControladorServicios.cs:74, 124 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs)) a secondary exception would also suppress the alert (GEN-8).
• Recommended code: if (!nuevaConfig.Pid.HasValue) throw new InvalidOperationException("ObtenerProcesoActualAsync no devolvió PID."); and pass Pid ?? 0 (or a validated
int) into the alert/termination helpers.
• Benefit: precise start-up diagnostics and no masking exception in error paths.
• Suggested JIRA: SIC-FR-8 (Major)
SIC-FR-9 – Child service names are hard-coded and the start/stop pairs are duplicated (Minor)
• File:Line: EnumComunes.cs:14-15 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Enums/EnumComunes.cs);
OperacionServicio.cs:120-129 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs).
• Current code: public const string Swf_IN_ETL = "CMS-Swf_IN_ETL"; and two parallel ternaries
bolPuedeIniciar ? Inicia(X) : Detiene(X) for ETL and FT.
• Issue: environment-specific Windows service names are compiled in (checklist §11 "no hard-coded environment values"); adding a third worker means editing code in
two places. The helper Aplicacion.Enums.EnumComunes is also a static class of constants named like an enum.
• Recommended code: read the managed service list from configuration and loop:
``csharp var tareas = _configuracion.ServiciosAdministrados .Select(s => puedeIniciar ? _controlador.IniciaServicioAsync(s) : _controlador.DetieneServicioAsync(s)).ToArray(); ``
• Benefit: no code change for renamed/new workers; test fakes can assert per-service outcomes.
• Suggested JIRA: SIC-FR-9 (Minor)
SIC-FR-10 – A database call whose result is never used runs every tick (Minor)
• File:Line: OperacionServicio.cs:80 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs);
RepositorioTomi.cs:111-134 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs).
• Current code: _configuracion.StrFechaSistemaTomi = await _repositorioTomi.ObtenerFechaSistema();
• Issue: scans show StrFechaSistemaTomi is assigned once and never read, so usp_ObtenerFechaSistema is executed (and can throw on an empty
tb_srv_fecha_sistema row set) on every tick purely for its side effects. A failure here aborts the whole tick and therefore the start/stop decision.
• Recommended code: remove the call and property, or use the TOMI date as the single calendar source for the business-day decision (SIC-FR-6).
• Benefit: one fewer SP per tick and one fewer unrelated failure mode in a safety-relevant decision.
• Suggested JIRA: SIC-FR-10 (Minor)
Nice to Rewrite if Time Permits
SIC-FR-11 – Copy-paste log text, typos and mismatched XML docs (Minor)
• File:Line: OperacionServicio.cs:59 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs) ("Hora de cancelacion SPEI" logged for HORA_OP_FIN;
tag obtenerEnums.ObtenerParametrosSistema), :116 ({...}horario de operaciones — missing space, logs "Iniciohorario"/"Terminahorario" on every tick and says "Inicio" when merely *inside* the window), :160 ("maracar", tag Ejecuta_Procedimientos), :93 (no level prefix); EnumComunes.cs:26 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Enums/EnumComunes.cs) (usp_MarcaPuertoEnEso); IRepositorioTomi.cs:17-20, 25-30, 53-55, 61-63 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Interfaces/IRepositorioTomi.cs) (doc comments list parameters that do not exist, and ValidaSiEsDiaHabil is documented as returning Dictionary<string,string>); ConfiguracionServicio.cs:13-17 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Modelos/ConfiguracionServicio.cs) (Str* prefix on TimeSpan? properties).
• Recommended: correct the text, log only on window *transitions* at INFO (see GEN-10), rename StrFechaSistemaServicio to HoraActual, fix XML docs.
• Benefit: logs that match behavior; fewer false leads for the next maintainer.
• Suggested JIRA: SIC-FR-11 (Minor)
SIC-FR-12 – Dead and duplicate members (Minor)
• Inventory: Aplicacion/Modelos/Proceso.cs (class never used); ConfiguracionServicio.StrHoraOperacionesInicio/Fin (never read);
IRepositorioTomi.ActualizaPuertoLibre/ActualizaPuertoEnUso + RepositorioTomi.cs:47-105 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs) + EnumProcedimientosAlmacenados.usp_MarcaPuertoEnEso/usp_MarcaPuertoLibre/usp_EsDiaHabil (the host uses the shared ControlServicio instead; EsDiaHabil comes from the base repository); ConfiguracionServicio.EnHorarioOperaciones/BoEsDiaHabil are written then only used locally; unused using directives in most files (System.Linq, System.Text, System.Runtime.InteropServices); MapearCamposGenerales serves three result shapes with mixed fields (Fecha, Num_regs, Id_parametro, Valor).
• Recommended: delete or wire the dead members; split the multi-purpose DTO (FechaSistema, Parametro).
• Benefit: smaller interface to fake in tests, less confusion about which SQL objects are live.
• Suggested JIRA: SIC-FR-12 (Minor)
4.2 Infraestructura (Tier 1)
MustChange / Rewrite
None identified. The repository's lifecycle/alert helpers were traced into the shared RepositorioMaestro and behave correctly for the failure shapes that exist today (see SIC-FR-13 for the fragile guard).
Rewrite Recommended
SIC-FR-13 – The failed-result guard in ProcesoActualizaEjecucion/ProcesoActualizaTerminacion AND-s three conditions (Minor)
• File:Line: RepositorioTomi.cs:222-224 and 252 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs).
• Current code:
``csharp if (resultActualizaTerm.MensajeError != null && resultActualizaTerm.Exception != null && !resultActualizaTerm.Exito) { ...throw new Exception(strMsjErro); } ``
• Issue: the sibling methods in the same class test only !resultadoSP.Exito (RepositorioTomi.cs:57, 93 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs)). Tracing the shared code,
EjecutarStoredProceduredNonQuery today always sets Exception together with MensajeError when it fails (SqlException and general catches; a non-empty @mensajeError is turned into a ResultadoStoredProcedureException and captured the same way), so the three-way guard is currently equivalent to !Exito and no silent failure was proven. It is nevertheless fragile: any future shared-layer path that returns Exito=false with only one of the fields set would be ignored without a log line here, and new Exception(...) discards the inner exception. Checklist §5/§8; related to GEN-11.
• Recommended code:
``csharp if (!resultActualizaTerm.Exito) throw new InvalidOperationException($"No se actualizó la terminación de '{strNombreServicio}' (pid {intgpId}): {resultActualizaTerm.MensajeError}", resultActualizaTerm.Exception); `` Apply to both methods and cover with a fake repository result in a unit test.
• Benefit: one consistent contract across the repository; inner exception preserved.
• Suggested JIRA: SIC-FR-13 (Minor)
SIC-FR-14 – Repository logs carry no severity, are logged-and-rethrown at every layer, and name a file the logger ignores (Major)
• File:Line: every EscribirLog call in RepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs)
(lines 60, 66, 96, 102, 122, 131, 149, 155, 173, 193, 206, 228, 234, 256, 262); OperacionServicio.cs:93 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs).
• Current code: Config.EscribirLog("Error: no se completo actualizacion del puerto:" + resultadoSP.MensajeError, _configuracionServicio.NombreServicio);
• Issue: Logging.WriteLegacy derives the level from a INFO|/WARN|/ERROR| prefix; messages without one go to the neutral Write level. 15 of 15 repository log calls (and 1 in OperacionServicio)
therefore have no ERROR level even though they describe failures, so severity-based filtering/alerting will miss them. Each failure is also logged in the repository, again in OperacionServicio (catch ... throw;), and again in the host (3+ entries per fault). WriteLegacy(msg, nombreArchivo, ...) ignores the second argument (shared source: "el nombre de archivo ya no aplica"), so passing NombreServicio conveys nothing. Config.EscribirLog is a static legacy facade (marked obsolete in its Logging sibling), so logging is not injectable (baseline §4.7/§5).
• Recommended code: inject an ILogger-style abstraction (or use Logging.Error(...) directly), log once at the boundary that handles the exception, wrap with context instead of log-and-throw
(throw new InvalidOperationException("ObtenerFechaSistema falló", ex)).
• Benefit: accurate severities, one log entry per fault, testable logging.
• Suggested JIRA: SIC-FR-14 (Major)
Nice to Rewrite if Time Permits
SIC-FR-15 – Small robustness gaps in parameter/date reads (Minor)
• File:Line: RepositorioTomi.cs:197-201 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs) (diccParametros.Add(...) throws ArgumentException on a duplicate id_parametro;
null keys throw too); RepositorioTomi.cs:119-124, 190-195 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs) (when Data.Count == 0 and MensajeError == null the exception message is just "Error: "); RepositorioTomi.cs:126 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs) (FirstOrDefault().Fecha is guarded by the preceding Count == 0 check — verified safe).
• Recommended: diccParametros[parametro.Id_parametro] = parametro.Valor; and an explicit "sin filas" message naming the SP.
• Benefit: clearer diagnostics. usp_ConsultaParametrosSistema_SwfInCtrlSrv selects from a configuration table by IN (...); duplicates are unlikely, hence Minor.
• Suggested JIRA: SIC-FR-15 (Minor)
4.3 Swf_IN_ControladorServicios host (Tier 2)
Rewrite Recommended
SIC-FR-16 – The Boolean result of MarcarPuertoEnUsoAsync is discarded and the shutdown call is mislabeled (Major)
• File:Line: ServiceHost.cs:119 and 172 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs).
• Current code:
``csharp //Marca el servicio en uso... await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio, cleanupCt).ConfigureAwait(false); ``
• Issue: the shared method returns resultado.Exito (GEN-9); a provider failure returns false without throwing and the host then logs "Terminación exitosa de bases de datos"
(ServiceHost.cs:177 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs)). The shutdown call is commented as "en uso" but passes null PID/process names, i.e. it *frees* the row — and this solution already carries an unused ActualizaPuertoLibre (RepositorioTomi.cs:47-69 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs)). Also note usp_ActualizaServicioEnUso/Libre_SwfInCtrlSrv return @Exito = 1 even when @@ROWCOUNT = 0, so a missing tb_srv_puerto_x_servicio row for this service also looks successful. TerminacionAsync stops this controller only; it does not stop CMS-Swf_IN_ETL/CMS-Swf_IN_FT — presumably intentional (workers keep running without a supervisor), but undocumented.
• Recommended code: if (!await ...MarcarPuertoEnUsoAsync(...)) throw/log ERROR; rename the shutdown call/comment ("libera puerto") or call an explicit LiberarPuertoAsync; return/check FilasAfectadas.
• Benefit: start-up/shutdown state is trustworthy; the shutdown contract is documented.
• Suggested JIRA: SIC-FR-16 (Major)
SIC-FR-17 – DI and composition-root clean-ups (Minor)
• File:Line: RepositorioTomi.cs:22, 32 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs) (depends on concrete Alertas) vs
ServiceHost.cs:52-53 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs) (IAlertas registered); ServiceHost.cs:89 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs) (var parametrizacion = new Parametrizacion(); never used); ServiceHost.cs:83 vs 91 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs) (the scoped Configuracion and ConfiguracionServicio registrations are two separate clones, and the (ConfiguracionServicio) cast relies on MemberwiseClone preserving the runtime type); ConfiguracionServicio is a mutable bag written by the application layer on every tick.
• Recommended: take IAlertas in RepositorioTomi; delete the unused local; register Configuracion and ConfiguracionServicio against one instance; consider immutable per-tick results instead of mutating shared config.
• Benefit: honest dependencies, fake alerting in repository tests, fewer hidden state paths.
• Suggested JIRA: SIC-FR-17 (Minor)
4.4 PruebasUnitarias_Swf_IN_ControladorServicios (Tier 2)
MustChange / Rewrite
SIC-FR-18 – The test project covers 1 of 3 behavior-bearing classes and cannot see the defects above (Major)
• File:Line: UnitTest1.cs:15-170 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/UnitTest1.cs) (9 tests), fakes at :172-273.
• Evidence: 9 [TestMethod], all against OperacionServicio; 0 tests for ControladorServicios (SIC-FR-1/2) or RepositorioTomi (SIC-FR-13/15), and none for the host (ServiceHost). ControladorServiciosFake returns true
unconditionally, so the "one or more services failed" path, per-service outcomes, and the outside-window branch (UnitTest1.cs:56-85 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/UnitTest1.cs) assert counts only) are never exercised. RepositorioTomiFake.ProcesoInsertaAlerta/ProcesoActualizaEjecucion throw NotImplementedException, so any change that alerts would fail the suite. No assertion checks the arguments passed to IniciaServicioAsync (which service, which state). The exception assertions use exact-type ThrowsExceptionAsync<Exception> and do not check messages. Do not read this as a measured coverage percentage; no coverage run was executed.
• Recommended code: add (1) ControladorServicios tests behind a thin IWindowsServiceClient seam (already-running, StartPending, timeout, access denied) — or test the shared implementation instead; (2) a window-boundary theory
(before/at/after start and end; overnight) once SIC-FR-6 exists; (3) fake Task.WhenAll outcomes (one false, one throws) and assert alert/log policy; (4) a RepositorioTomi test with a fake repository result for Exito=false without Exception; (5) a regression test that fails on the inverted predicates.
• Benefit: the defects that currently reach production (SIC-FR-1/2) become test failures first (checklist §13: bug fixes need a failing-without-fix regression test).
• Suggested JIRA: SIC-FR-18 (Major)
Nice to Rewrite if Time Permits
SIC-FR-19 – Test organization and tooling (Minor)
• File:Line: UnitTest1.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/UnitTest1.cs) (generic file/class name UnitTest1, fakes in the same file);
PruebasUnitarias...csproj:62-67, 3, 97-100 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios.csproj).
• Issue: the class name does not describe the scenarios (the nine method names do); hand-written fakes instead of the portfolio's usual mocking library (the baseline cites MSTest + Moq); each test writes a real log folder under %TEMP%
through the static Config.Rutas (mutable global state shared across tests); MSTest 2.2.10 is old; package paths point at the *host* project's ..\Swf_IN_ControladorServicios\packages\, so the test project builds only after restoring that sibling.
• Recommended: rename to OperacionServicioTests, move fakes to Fakes/, use a logging seam instead of the static, upgrade MSTest, and give the test project its own restore.
• Benefit: clearer suite, fewer inter-test dependencies.
• Suggested JIRA: SIC-FR-19 (Minor)
4.5 Solution-wide (all projects)
MustChange / Rewrite
SIC-FR-20 – MUFG_T24.dll is referenced through a hard-coded absolute D:\ServiceCMS\ path in all four projects (Blocker)
• File:Line: Aplicacion.csproj:54-57 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Aplicacion.csproj),
Infraestructura.csproj:54-57 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Infraestructura.csproj), Swf_IN_ControladorServicios.csproj:91-94 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/Swf_IN_ControladorServicios.csproj), PruebasUnitarias...csproj:68-70 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios.csproj).
• Current code: <Reference Include="MUFG_T24, ..."><SpecificVersion>False</SpecificVersion><HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath></Reference> (SpecificVersion=False in three, none in the test project).
• Issue: confirmed on this machine: D:\ServiceCMS\MUFG_T24.dll does not exist, so the solution cannot be built here; there is no nuget.config or package for it; whichever DLL happens to be at that path at build time is compiled in
with SpecificVersion=False — no version trace between source and binary (checklist §11: "no direct DLL references"). Systemic (GEN-1); the fix is one portfolio-level change, not per project. Note the contract matters here: this solution depends on the shared WindowsServiceHostBase start-up contract, ControlServicio, Alertas and RepositorioMaestro behavior.
• Recommended code: publish MUFG_T24 as a versioned internal NuGet package; replace the four HintPath references with a single PackageReference per project; pin the version.
• Benefit: reproducible builds and an auditable dependency version.
• Suggested JIRA: SIC-FR-20 (Blocker)
Rewrite Recommended
SIC-FR-21 – 710 packages/ files (340 binaries) and .vs caches are committed despite .gitignore (Major)
• File:Line: git ls-files at commit 93404d5: 741 tracked files, 710 under Swf_IN_ControladorServicios/packages/, 340 *.dll/*.exe/*.pdb/*.nupkg, 3 .vs/*.dtbcache.json;
.gitignore:34-35, 207 (../../SWIFT/Incoming/SWF_IN_ControladorServicios/.gitignore) already lists [Bb]in/, [Oo]bj/, **/[Pp]ackages/*.
• Issue: the ignore rules were added after the files were committed, so they do not apply; 44 of the tracked package files are currently deleted in the working tree (uncommitted). Committed binaries bloat history, hide
upgrades, and .vs caches are per-developer state (checklist §11/§15: "no committed binaries", repo hygiene).
• Recommended code: git rm -r --cached Swf_IN_ControladorServicios/packages Aplicacion/.vs Infraestructura/.vs PruebasUnitarias_Swf_IN_ControladorServicios/.vs, commit, and rely on package restore (SIC-FR-22).
• Benefit: a repository of 28 source files instead of 741 tracked files; clean diffs.
• Suggested JIRA: SIC-FR-21 (Major)
Nice to Rewrite if Time Permits
SIC-FR-22 – Build/packaging modernization and docs (Info)
• File:Line: packages.config (host (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/packages.config), tests (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/packages.config));
the four AssemblyInfo.cs (empty AssemblyCompany, fixed 1.0.0.0); README.md (../../SWIFT/Incoming/SWF_IN_ControladorServicios/README.md) (GitLab template, no purpose/runbook).
• Issue: old-style csproj + packages.config with 19 packages other than SimpleInjector (Microsoft.Extensions.*, System.Text.Json 10.x, BCL polyfills) in the host that no code in this solution uses directly (presumably required transitively by MUFG_T24; the dependency is implicit,
not declared). Debug/Release AnyCPU configurations force PlatformTarget=x86 while LangVersion 7.3 applies only to the explicit x86 configurations. No runbook documents the managed services, schedule parameters or the table rows needed (tb_srv_puerto_x_servicio, tb_srv_controladoradeservicios, HORA_OP_INICIO/FIN).
• Recommended: when the package feed exists (SIC-FR-20) migrate to SDK-style/PackageReference, declare only direct dependencies, stamp versions in CI, replace the README with a one-page runbook.
• Benefit: maintainability; deferred until the real defects above are fixed.
• Suggested JIRA: SIC-FR-22 (Info)
5. Architecture-Fit Note
This solution is a scheduled, stateless supervisor loop, not a continuous listener or a multi-step saga: each tick reads three small facts (business day, operating hours, current service states), decides "running" or "stopped" for two Windows services, and writes a heartbeat. Applying the portfolio's standing architecture question (./_ORCHESTRATION.md#6-standing-architecture-question-is-the-current-hostingarchitecture-model-right) to that shape: modernize in place is the only option the evidence supports. The Windows Service host is a natural fit (it manages other Windows services and needs local ServiceController rights), the DI/layering is sound, and no requirement in the code needs message-driven or workflow semantics. A serverless function or a Kubernetes-style scheduler would not be able to start/stop CMS-Swf_IN_ETL/CMS-Swf_IN_FT without an agent on the same host, and a microservice rewrite has no scaling or ownership driver. Unknowns that would change the answer and are not visible in the repository: the production timer interval, whether the two workers are expected to survive a supervisor stop, the SLA for how quickly workers must start at HORA_OP_INICIO, and the Windows account/rights model. Sequence: fix SIC-FR-1/2 (+ SIC-FR-3 by adopting the shared implementation) -> add the missing tests (SIC-FR-18) -> resolve the MUFG_T24 packaging (SIC-FR-20) -> only then consider runtime/hosting modernization (net48 -> .NET 8 Worker Service, which also needs the shared library to move).
Standing cross-cutting findings touched by this solution (status recorded in the control document): GEN-1 (SIC-FR-20), GEN-2 (SIC-FR-18), GEN-4/GEN-15 (SIC-FR-3), GEN-7 (SIC-FR-4/5), GEN-8 (SIC-FR-8), GEN-9 (SIC-FR-16), GEN-10 (SIC-FR-2/7), GEN-11 (SIC-FR-13), GEN-14 (inherited: ServiceHost derives directly from WindowsServiceHostBase and does not override OnStart; no solution-level change possible).
6. JIRA-Ready Backlog
22 open findings: 4 MustChange/rewrite, 13 rewrite_recommended, 5 NiceToRewrite_ifTimePermits; priorities 3 Blocker, 10 Major, 8 Minor, 1 Info.
ID	Title	Category	Project	File(s)	Priority
SIC-FR-1	Fix inverted success predicates in service start/stop	MustChange/rewrite	Aplicacion	ControladorServicios.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs)	Blocker
SIC-FR-2	Make start/stop idempotent; stop per-tick ERROR + alert storm	MustChange/rewrite	Aplicacion	ControladorServicios.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs), OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)	Blocker
SIC-FR-3	Replace the 5 copy-pasted ControladorServicios with the shared implementation	rewrite_recommended	Aplicacion	ControladorServicios.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs)	Major
SIC-FR-4	Stop finally I/O from masking the primary exception	rewrite_recommended	Aplicacion	OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)	Major
SIC-FR-5	Pair ActualizaEjecucion/ActualizaTerminacion or remove both per-tick calls	rewrite_recommended	Aplicacion	OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs), IRepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Interfaces/IRepositorioTomi.cs)	Major
SIC-FR-6	Inject clock, validate and test the operating-window logic (overnight, bounds)	rewrite_recommended	Aplicacion	OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)	Major
SIC-FR-7	Define alert policy: alert failed transitions, not expected state	rewrite_recommended	Aplicacion / Host	ControladorServicios.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Service/ControladorServicios.cs), OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs), ServiceHost.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs)	Major
SIC-FR-8	Guard nullable Pid (.Value before validation; (int) casts)	rewrite_recommended	Host / Aplicacion	ServiceHost.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs), OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)	Major
SIC-FR-9	Move child service names to configuration; loop instead of duplicated ternaries	rewrite_recommended	Aplicacion	EnumComunes.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Enums/EnumComunes.cs), OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)	Minor
SIC-FR-10	Remove the unused per-tick ObtenerFechaSistema call (or use it)	rewrite_recommended	Aplicacion	OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)	Minor
SIC-FR-11	Correct copy-paste log text, typos and XML docs	NiceToRewrite_ifTimePermits	Aplicacion	OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs), IRepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Interfaces/IRepositorioTomi.cs)	Minor
SIC-FR-12	Delete dead members (Proceso, ActualizaPuerto*, unused constants/properties)	NiceToRewrite_ifTimePermits	Aplicacion / Infraestructura	Proceso.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Modelos/Proceso.cs), ConfiguracionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Modelos/ConfiguracionServicio.cs), RepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs)	Minor
SIC-FR-13	Replace the three-way failed-result guard with !Exito	rewrite_recommended	Infraestructura	RepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs)	Minor
SIC-FR-14	Log at the right severity once per fault; inject logging	rewrite_recommended	Infraestructura / Aplicacion	RepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs), OperacionServicio.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/OperacionServicio.cs)	Major
SIC-FR-15	Harden parameter dictionary build and empty-result messages	NiceToRewrite_ifTimePermits	Infraestructura	RepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs)	Minor
SIC-FR-16	Check MarcarPuertoEnUsoAsync result; fix mislabeled shutdown call	rewrite_recommended	Host	ServiceHost.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs)	Major
SIC-FR-17	DI clean-ups (IAlertas, unused local, duplicate config registrations)	rewrite_recommended	Host / Infraestructura	ServiceHost.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/ServiceHost.cs), RepositorioTomi.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs)	Minor
SIC-FR-18	Add tests for ControladorServicios, RepositorioTomi, window bounds and failure paths	MustChange/rewrite	Tests	UnitTest1.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/UnitTest1.cs)	Major
SIC-FR-19	Rename/organize tests, remove static log dependency, upgrade MSTest	NiceToRewrite_ifTimePermits	Tests	UnitTest1.cs (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/UnitTest1.cs)	Minor
SIC-FR-20	Publish MUFG_T24 as a versioned package; remove D:\ServiceCMS HintPaths	MustChange/rewrite	All 4 projects	Aplicacion.csproj (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Aplicacion/Aplicacion.csproj), Infraestructura.csproj (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Infraestructura/Infraestructura.csproj), Swf_IN_ControladorServicios.csproj (../../SWIFT/Incoming/SWF_IN_ControladorServicios/Swf_IN_ControladorServicios/Swf_IN_ControladorServicios.csproj), PruebasUnitarias...csproj (../../SWIFT/Incoming/SWF_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios/PruebasUnitarias_Swf_IN_ControladorServicios.csproj)	Blocker
SIC-FR-21	Untrack 710 committed packages/ files and .vs caches	rewrite_recommended	Repository	.gitignore (../../SWIFT/Incoming/SWF_IN_ControladorServicios/.gitignore)	Major
SIC-FR-22	Modernize packaging; replace boilerplate README with runbook	NiceToRewrite_ifTimePermits	All	README.md (../../SWIFT/Incoming/SWF_IN_ControladorServicios/README.md)	Info

