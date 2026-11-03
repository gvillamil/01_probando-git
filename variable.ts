Swf OUT ControladorServicios Recommendations
Swf_OUT_ControladorServicios - Architecture & Code Quality Recommendations
Solution: Swf_OUT_ControladorServicios.sln (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios.sln). Reviewed: 2026-10-15, branch CodeReview, commit 81590439657ca7a9260e1307bc6f1a1f08e39700, clean working tree. Compared against: LoansApp coding-practices baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR review/severity checklist (../BaseLine_Docs/code_review.md). See also: orchestration/control (./_ORCHESTRATION.md#swf_out_controladorservicios-scoped-audit-checkpoint---2026-10-15). Target: .NET Framework 4.8 / x86 Windows Service; focus: all categories; severity threshold: all. Audit only: no application fixes applied.
1. Solution Overview (low/no-dependency -> high dependency)
Tier	Project	Kind	Path
0	Aplicacion	Class library (schedule decision + SCM control)	Aplicacion.csproj (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Aplicacion.csproj)
1	Infraestructura	Class library (TOMI repository)	Infraestructura.csproj (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Infraestructura.csproj)
2	PruebasUnitariasSwfCtrlSrv	MSTest library (3 methods / 8 data rows)	PruebasUnitariasSwfCtrlSrv.csproj (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/PruebasUnitariasSwfCtrlSrv.csproj)
2	Swf_OUT_ControladorServicios	Windows Service executable (composition root)	Swf_OUT_ControladorServicios.csproj (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/Swf_OUT_ControladorServicios.csproj)

What this solution actually does: it is a local supervisor Windows Service for the three SWIFT *outgoing* workers. On every timer tick (interval from the external service configuration, 10 s fallback) it records the start of the tick in TOMI, reads the TOMI system date (the value is not used), asks the shared library whether today is a business day (the shared check also returns *false* on Saturday/Sunday unless FORZA_EJECUCION_SERVICIOS=1), reads three parameters (HORA_OP_INICIO, HORA_OP_FIN, SWF_OUT_GET_MT_BATCH) and compares the machine's local time (truncated to the minute) with the window. It then, in parallel, starts the Windows services CMS-Swf_OUT_Read, CMS-Swf_OUT_ETL and CMS-Swf_OUT_PK_DB when it is a business day inside the window, or stops them otherwise; when SWF_OUT_GET_MT_BATCH = 1 it only starts them, ignoring day and window. Each start/stop uses ServiceController on the local machine and waits up to 30 s; failures are logged and written as TOMI alert rows. At the end of every tick it marks the controller's row as terminated in TOMI. It does not read, parse or route SWIFT messages and does not move funds; it is an availability dependency for the outgoing pipeline.
1.1 Issue Summary
Recommended priority	Count	Simple meaning
Blocker	1	Close before approving a release; the solution cannot be built reproducibly
Major	8	Correct or explicitly accept before relying on the controller; planned reliability work
Minor	17	Maintainability/cleanup after correctness work
Total distinct findings	26	One category per finding; no duplicate tickets

Skill category	Count
MustChange/rewrite	6
rewrite_recommended	17
NiceToRewrite_ifTimePermits	3

Priorities are source-review release recommendations, not CVSS or proof of a production incident. No credential, cryptography, SQL-injection, sync-over-async or throw ex; defect was found in the authored code (see 2.1).
2. Scope & Methodology Note
Area	Evidence / limitation
Source scope	25/25 tracked files, 2,294/2,294 physical lines fully read, not sampled: 15 C# files (1,358 lines; 11 first-party logic/interface/host files + 4 AssemblyInfo/Designer/Program files), four project files, one solution, two packages.config, App.config, README.md, .gitignore (read as a rule list)
Coverage record	SHA-256 / physical-line manifest (./_Swf_OUT_ControladorServicios_FileManifest.csv); every file has Reviewed range 1-N
Context also read	Five SQL objects for this solution under StoredProcedures\MWD_DB\DBMDW\SWIFT\Outgoing\SWF_OUT_ControladorServicios, and the relevant parts of shared MUFG_T24 source (WindowsServiceHostBase, ControlServicio.MarcarPuertoEnUsoConResultadoAsync, RepositorioMaestro.EsDiaHabil/ActualizaEJecucion/ActualizaTerminacion, Parametrizacion.ObtenerProcesoActualAsync, Configuracion, ConversionHelper.ParseTimeSpanRequerido, Config/Logging.WriteLegacy, ObtencionConfguracionGeneral.ObtenerServicio, the shared ControladorServicios). Shared-library defects are out of scope; they are used only to confirm or rule out local findings
Exclusions	Git internals; no packages, build output or IDE caches are tracked (.gitignore excludes packages/, bin/, obj/, .vs/), none on disk
Review basis	Full current-source audit, not a changed-lines PR verdict or acceptance review for an unspecified work item
Standards limitation	Baseline has blank inventory/evidence fields; its principles are criteria, not measured facts
Build	Not built. D:\ServiceCMS does not exist on this machine, so MUFG_T24.dll / SimpleInjector.dll are unavailable; msbuild/dotnet/nuget are not on PATH and no packages are restored
Tests	Three real MSTest methods (8 data rows, 0 Assert calls, 12 Mock.Verify calls). Reviewed, not executed; no coverage was measured
Safe execution	Isolated PowerShell/.NET probes (see 2.2) characterize TimeSpan parsing, the window comparison and the test-suite contradiction. They are not production-source unit tests
External evidence not used	No configuration file with the real service names/intervals, no live TOMI database, no SCM access, no deployed DLL versions, no secrets accessed
Fix examples	Illustrative contracts, not compiled patches. New helper types need implementation, DI registration and regression tests

Full-read batches in this audit (single session, 2026-10-15):
Batch	Contents	Files	Physical lines	Status
A	Aplicacion: OperacionServicio, ControladorServicios, 3 interfaces, enums, 2 models, csproj	9	552	Full read complete
B	Infraestructura: RepositorioTomi, csproj	2	238	Full read complete
C	Host: ServiceHost, Service1.Designer, Program, App.config, csproj, packages.config, AssemblyInfo	7	470	Full read complete
D	Tests: UnitTest1.cs, csproj, packages.config, AssemblyInfo	4	414	Full read complete
E	Root: .sln, README.md, .gitignore	3	620	Full read complete
Total	Entire tracked scope	25	2,294	No pending range

2.1 Quantified Cross-Checks and Duplication
Check	Count	Meaning
new ServiceController(	2	One per start/stop method (ControladorServicios.cs:39, 86); status checks cover Running / Stopped / StopPending only (FR-3)
new AlertaDTO construction blocks	2	Identical except text (ControladorServicios.cs:63-74, 109-120)
MensajeThrow( call sites / catch ( blocks in RepositorioTomi	12 / 6	Log + generic Exception thrown, 4 of them inside the same try whose catch logs and wraps again (FR-16)
Config.EscribirLog calls	21	Log text copy-pasted from the SPEI sibling (FR-13)
DateTime.Now in OperacionServicio	1	Overwrites the value a test (or any caller) sets (FR-7, FR-24)
Line overlap with twin Swf_OUT_ControladorServiciosMQ	87 % / 98 % / 78 % / 93 %	Non-blank, non-comment line overlap of OperacionServicio / ControladorServicios / RepositorioTomi / host ServiceHost (GEN-4)
Line overlap with SWF_IN_ControladorServicios, SWF_IN_ControladorServiciosMQ, CEP_ControladorServicios	62-73 % / 81-85 % / 70-81 % / 94-96 %	Same four files (GEN-4/GEN-15)
.Result, .Wait(, GetAwaiter().GetResult()	0	No sync-over-async in authored code
throw ex;, DES/3DES/ECB/MD5/SHA1, System.Random, secrets in logs	0	Not present
[TestMethod] / [DataRow] / Assert. / Verify(	3 / 8 / 0 / 12	Real framework, no assertions (FR-24)
First-party use of Microsoft.Extensions.*, System.Text.Json, Castle	0	Yet 20 NuGet packages (13 at 10.0.3 on net48) are restored in the host (FR-22)

Scans supplement, not replace, the full read.
Sibling comparison (same day): Swf_IN_ControladorServiciosMQ_Recommendations.md (./Swf_IN_ControladorServiciosMQ_Recommendations.md) (SICM-*) and Swf_IN_ControladorServicios_Recommendations.md (./Swf_IN_ControladorServicios_Recommendations.md) (SIC-FR-*) cover the near-identical incoming supervisors. This copy checks the SCM status *before* acting and returns true for an already-running/already-stopped service, so it does not have the ETL/FT copy's inverted predicates and per-tick exception storm (SIC-FR-1/2), nor the MQ copy's "no-op is a failure" ERROR every tick (SICM-A1) - GEN-15 is not reproduced here. The shared-family items are reconfirmed: pending/paused states (SICM-A2 -> FR-3), presence-only schedule validation (SICM-A3 -> FR-4), unused per-tick date call (SIC-FR-10 / SICM-A6 -> FR-15), discarded MarcarPuertoEnUsoAsync (SIC-FR-16 / SICM-H1 -> FR-20) and the HintPath defect (GEN-1 -> FR-1).
Twin cross-check (same day): the outgoing twin was reviewed independently in Swf_OUT_ControladorServiciosMQ_Recommendations.md (./Swf_OUT_ControladorServiciosMQ_Recommendations.md) (SOCM-*). After both reviews were complete, every twin finding was re-tested against this copy: SOCM-R1 -> FR-1, SOCM-A1 -> FR-4, SOCM-A2 -> FR-3, SOCM-A3 -> FR-10, SOCM-A4 -> FR-5, SOCM-A5 -> FR-3/FR-8, SOCM-A6 -> FR-13/FR-16, SOCM-A7 -> FR-7, SOCM-A8 -> FR-9, SOCM-I1 -> FR-15/FR-17, SOCM-I3 -> FR-15, SOCM-H1 -> FR-20, SOCM-H2 -> FR-19/FR-21, SOCM-H3 -> FR-12/FR-21, SOCM-H5 -> FR-23, SOCM-T1 -> FR-24, SOCM-N1/N3 -> FR-14/FR-2/FR-22. One twin finding was missing from the first pass here and is now FR-26 (SOCM-H4, KB passed as MB). Items not raised in the twin's report (several probably apply there too and should be checked): FR-6 (30 s wait vs 95 s worker shutdown budget), FR-18 (dead repository methods/SPs and six competing usp_EsDiaHabil definitions), the missing dbo.usp_Comun_ActualizarEnUso evidence in FR-20, and the three-way parsing of SWF_OUT_GET_MT_BATCH across the outgoing workers (FR-5).
2.2 Safe Probe Results
Probe	Observed
TimeSpan.TryParse(v, InvariantCulture) (what ParseTimeSpanRequerido uses)	'06:10', '23:59' ok; '8' -> 8.00:00:00 (8 days); '7' -> 7 days; '1.07:00' -> 1 day 7 h; '24:00' rejected
Window test now >= INI && now <= FIN	Overnight 22:00-06:00 is false at 23:30 and at 02:00 (never matches)
Test-suite contradiction	The window is evaluated against the wall clock (test value overwritten at OperacionServicio.cs:85). Over all 1,440 minutes of a day, the number of minutes at which both the "must start" rows (UnitTest1.cs:25-28, 214) and the "must not start" rows (124-126) can pass is 0
Weekend gate	Shared EsDiaHabil returns false on Saturday/Sunday unless FORZA_EJECUCION_SERVICIOS=1 (RepositorioMaestro.cs:1253-1262); the window can never start the services on a weekend
Logging API	Config.EscribirLog(msg, nombreArchivo) forwards to Logging.WriteLegacy(msg, nombreArchivo, masked) which ignores the file name (Logging.cs:184-189)

3. What's Already Good
• Layering and DI: IOperacionServicio, IControladorServicios, IRepositorioTomi interfaces with constructor injection
(OperacionServicio.cs:24-30 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L24), ControladorServicios.cs:25-29 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L25)); SimpleInjector composition root with Verify() (ServiceHost.cs:97 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L97)).
• Per-tick configuration isolation: ConfiguracionServicio is MemberwiseCloned into every async scope
(ServiceHost.cs:90 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L90)); schedule parameters are re-read from TOMI every tick, so operators can change hours without restarting the service. .Pid.Value at ServiceHost.cs:119 and the (int)_configuracion.Pid casts (ControladorServicios.cs:70, 116; OperacionServicio.cs:177) were ruled out as unguarded nullables: shared Parametrizacion.ObtenerProcesoActualAsync always assigns the PID (Parametrizacion.cs:59-69) and the base host rejects a non-positive PID before starting the timer (WindowsServiceHostBase.cs:282-304).
• Idempotent start/stop for the common case: Running (start) and Stopped/StopPending (stop) return true without touching the SCM
(ControladorServicios.cs:43-46, 89-92 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L43)); no per-tick exception storm in steady state.
• Overlap protection: the shared host serializes ticks with SemaphoreSlim(1,1) (WindowsServiceHostBase.cs:21,372), so two ticks cannot start/stop services concurrently.
• Resource handling: ServiceController is wrapped in using, blocking SCM waits run off the caller via Task.Run, and each wait is bounded (30 s).
• Termination bookkeeping: finally always attempts ProcesoActualizaTerminacion (OperacionServicio.cs:172-185 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L172)) and ProcesoActualizaEjecucion has a caller (line 100); rethrows use throw; - no throw ex; anywhere (GEN-7 not present).
• Fail-closed schedule presence check: both required parameters must exist or the tick aborts with a named message (OperacionServicio.cs:87-88, 195-201 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L195)).
• Data access: repository calls stored procedures with typed parameters; no string-built SQL. The five SQL objects use SET NOCOUNT ON; the two writers use TRY/CATCH, explicit transactions, XACT_ABORT ON and an error-log insert (even though no code calls them, FR-18).
• No secrets, crypto or cleartext credentials appear in code, config or logs; App.config only pins the runtime. No packages or binaries are committed (.gitignore excludes packages/).
• Tests exist (three MSTest methods with Moq and a temp-folder log path) - a starting point that most comparable supervisors lack (GEN-2), although they need repair (FR-24).
4. Findings by Project (low/no-dependency -> high dependency)
Every finding was verified against the current source. Locations use file:line from the manifest snapshot. Each ID is stable for JIRA. Prefix: SOCS-FR-#.
4.1 Solution-wide (build, documentation)
MustChange / Rewrite
SOCS-FR-1 - The solution cannot be built reproducibly: six absolute-path references in all four projects
Severity: Blocker (P0). Category: MustChange/rewrite. Checklist: section 11 (no direct DLL references; build has zero errors) - Blocker. Locations: Aplicacion.csproj:11,14 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Aplicacion.csproj#L11), Infraestructura.csproj:15 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Infraestructura.csproj#L15), host csproj:76,79 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/Swf_OUT_ControladorServicios.csproj#L76), tests csproj:57 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/PruebasUnitariasSwfCtrlSrv.csproj#L57).
Code block (xml)
<Reference Include="MUFG_T24"><HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath></Reference>          <!-- all 4 projects -->
<Reference Include="SimpleInjector"><HintPath>D:\ServiceCMS\SimpleInjector.dll</HintPath></Reference> <!-- host:79 -->
<Reference Include="System.ServiceProcess"><HintPath>C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.ServiceProcess.dll</HintPath></Reference> <!-- Aplicacion:14 -->
Five D:\ServiceCMS references plus one Program Files (x86) reference-assembly path. The D: folder does not exist on the audit machine, no nuget.config or MUFG_T24 package exists in the portfolio, and nothing records which build of MUFG_T24.dll this controller was last built or tested against; a change to the shared DLL silently changes the next rebuild. The framework assembly does not need a HintPath at all (in an SDK-style net48 project <Reference Include="System.ServiceProcess" /> resolves from the targeting pack). This is the portfolio-wide GEN-1.
Code block (xml)
<PackageReference Include="MUFG_T24" Version="x.y.z" />
<PackageReference Include="SimpleInjector" Version="5.x" />
<Reference Include="System.ServiceProcess" />
Fix/benefit: publish MUFG_T24 (and SimpleInjector) to the internal NuGet feed and reference them by version; the solution builds on any machine and the shared-library version in production becomes auditable. Fix once for the portfolio, not per project. Suggested JIRA: SOCS-FR-1 (P0).
Nice to Rewrite if Time Permits
SOCS-FR-2 - README is the untouched GitLab template and no operational facts are written down
Severity: Minor (P3). Category: NiceToRewrite_ifTimePermits. Location: README.md:1-93 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/README.md#L1). The README contains only the generic "Getting started / Suggestions for a good README" text. Nothing records the controlled service names (CMS-Swf_OUT_Read, CMS-Swf_OUT_ETL, CMS-Swf_OUT_PK_DB), the three TOMI parameters, the weekend rule inherited from the shared library, the account rights needed for ServiceController.Start/Stop, or how to debug. The portfolio master document (section 4.7.8) also lists a host appsettings.json that does not exist in the repository. Fix/benefit: a one-page README (purpose, parameters, semantics of the batch flag, deployment account, how to run the tests) lets the next maintainer verify FR-4/FR-5/FR-6 against real values. Suggested JIRA: SOCS-FR-2 (P3).
4.2 Aplicacion - Tier 0
MustChange / Rewrite
SOCS-FR-3 - Pending and paused SCM states are not handled
Severity: Major (P1). Category: MustChange/rewrite. Checklist: section 3 (boundary/default branches), section 5 (error handling). Locations: ControladorServicios.cs:43-56 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L43), ControladorServicios.cs:89-103 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L89).
Code block (csharp)
if (serviceController.Status == ServiceControllerStatus.Running) return true;                 // start path, :43
...serviceController.Start(); serviceController.WaitForStatus(Running, TimeSpan.FromSeconds(30));
if (Status == Stopped || Status == StopPending) return true;                                   // stop path, :89
...serviceController.Stop(); serviceController.WaitForStatus(Stopped, TimeSpan.FromSeconds(30));
Only three of the seven ServiceControllerStatus values are tested. Calling Start() while the service is StartPending, ContinuePending, PausePending, Paused or StopPending, or Stop() while it is StartPending, ContinuePending, PausePending or Paused, is expected to be rejected by the SCM with an InvalidOperationException; the catch at lines 59/106 then writes an ERROR line and a TOMI alert row on every tick for as long as the service sits in that state (a transition that outlasts the 30 s wait, FR-6, leaves the service pending on the next tick). The shared library's own ControladorServicios already handles all of these states (ControladorServicios.cs:47-75, 151-178 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Aplicacion/ControladorServicios/ControladorServicios.cs#L47)); the local copy predates or ignores it.
Code block (csharp)
switch (status)
{
    case Running:        return Result.NoChange;
    case StartPending:
    case ContinuePending: return await WaitUntil(Running, budget);   // wait / re-check next tick, no alert yet
    case Paused:         controller.Continue(); ...
    case StopPending:
    case PausePending:   await WaitUntil(Stopped|Paused, budget); goto case Stopped;
    case Stopped:        controller.Start(); ...
}
Fix/benefit: a status-driven policy (or delegating to the shared class, FR-9) makes a slow transition a *state*, not an alert storm. Validate each status with a scripted fake SCM before changing behavior. Suggested JIRA: SOCS-FR-3 (P1).
SOCS-FR-4 - Schedule values are validated for presence only; a stored 8 means eight days and an overnight window never matches
Severity: Major (P1). Category: MustChange/rewrite. Checklist: section 3 (boundary conditions, TryParse on external data) - Blocker/Major; section 14 (business-day rules). Locations: OperacionServicio.cs:48,54 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L48), OperacionServicio.cs:60 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L60), OperacionServicio.cs:85-88 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L85), OperacionServicio.cs:117-118 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L117).
Code block (csharp)
_configuracion.StrHoraOperacionesInicio = ConversionHelper.ParseTimeSpanRequerido(paramSistema.Value, "HORA_OP_INICIO");
...
_configuracion.SWF_OUT_GET_MT_BATCH = paramSistema.Value == "1";
...
if (StrFechaSistemaServicio >= StrHoraOperacionesInicio && StrFechaSistemaServicio <= StrHoraOperacionesFin) ...
ParseTimeSpanRequerido is TimeSpan.TryParse(value, InvariantCulture). Probes: '06:10' and '23:59' are accepted, but `'8'` becomes 8.00:00:00 (eight days), '1.07:00' is accepted as one day and seven hours, and '24:00' is rejected. A value of eight days is never reached by a time of day, so the window is permanently closed and the three services are stopped (non-batch branch) without any error. The comparison also has no overnight support (22:00-06:00 is false at 23:30 and at 02:00), and an inverted or equal pair (INI >= FIN) silently never starts anything. The batch flag accepts only the exact text "1": "true", " 1" or "01" silently mean *false*.
Code block (csharp)
static TimeSpan LeerHora(string valor, string campo)
{
    if (!TimeSpan.TryParseExact(valor?.Trim(), @"hh\:mm", CultureInfo.InvariantCulture, out var t) || t >= TimeSpan.FromDays(1))
        throw new ConfiguracionInvalidaException($"{campo}='{valor}' no es HH:mm");
    return t;
}
// reject INI >= FIN (or model overnight explicitly: inWindow = ini < fin ? now>=ini && now<=fin : now>=ini || now<=fin)
Fix/benefit: invalid schedule data fails with a named message (and one alert per change, not per tick) instead of silently stopping the outgoing pipeline. Owners must confirm whether overnight windows and weekend operation are ever needed. Suggested JIRA: SOCS-FR-4 (P1).
Rewrite Recommended
SOCS-FR-5 - The batch branch is a divergent copy of the start path and its flag is mirrored by a dead property
Severity: Minor (P2). Category: rewrite_recommended. Locations: OperacionServicio.cs:106-113 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L106), ConfiguracionServicio.cs:23-24 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Modelos/ConfiguracionServicio.cs#L23), UnitTest1.cs:270 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/UnitTest1.cs#L270).
Code block (csharp)
if (_configuracion.SWF_OUT_GET_MT_BATCH)
    await Task.WhenAll(IniciaServicioAsync(READ), IniciaServicioAsync(ETL), IniciaServicioAsync(PK_DB));   // bool[] dropped
else { ... bool[] resultados = await Task.WhenAll(tareas); if (resultados.Any(x => !x)) log ERROR ... }
When SWF_OUT_GET_MT_BATCH = 1 the code starts the three services regardless of business day and window and discards the result array, so the aggregate "No todos los servicios pudieron procesarse" line (:157-161) is missing in that mode (the individual failures are still logged and alerted inside the controller). EnHorarioOperaciones and its transition logs are not updated in the branch. A second boolean, MensajesBatch (ConfiguracionServicio.cs:24), is never read by production code - only the test sets it (UnitTest1.cs:270) while the production flag is SWF_OUT_GET_MT_BATCH. The intended semantics (should batch mode ever stop the services? does it override weekends and the window?) are not written down. The flag is also parsed three different ways across the outgoing family: this controller accepts only the exact text "1" (OperacionServicio.cs:60), SWF_OUT_READ and SWF_OUT_ETL accept "1" or "true" (case-insensitive, ParseBool), and SWF_OUT_PK_DB uses int.TryParse(...) == 1. A stored TRUE therefore puts READ and ETL into batch mode while this controller still sees *false* and stops them outside the window. Fix/benefit: one decision function returning the desired state per service (Start/Stop) with the batch override as an explicit input, one execution/logging path, and MensajesBatch removed. Suggested JIRA: SOCS-FR-5 (P2).
SOCS-FR-6 - The controller waits 30 s for a stop that the workers are allowed 95 s to complete
Severity: Major (P1). Category: rewrite_recommended. Locations: ControladorServicios.cs:97-101 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L97), ControladorServicios.cs:50-54 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L50); shared host WindowsServiceHostBase.cs:27-28,122-123 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Core/Estructura/WindowsServiceHostBase.cs#L27).
Code block (csharp)
serviceController.Stop();
serviceController.WaitForStatus(ServiceControllerStatus.Stopped, TimeSpan.FromSeconds(30));   // throws System.ServiceProcess.TimeoutException
The shared base host used by the workers waits up to 60 s for the running tick and 30 s for cleanup in OnStop and asks the SCM for waitTimeout + cleanupTimeout + 5 s = 95 s (defaults; a worker may override them). Stopping at the end of the window is a *daily, expected* event: if a worker needs more than 30 s, WaitForStatus throws, the catch logs an ERROR, inserts an alert row and returns false, and the tick logs "No todos los servicios pudieron procesarse" - a false failure for a healthy shutdown - while the tick is blocked for the full 30 s. The next tick sees StopPending and reports success. The real worker shutdown time is not in the repository.
Fix/benefit: make the budget configurable per service (default at least the workers' budget), treat "still StopPending/StartPending after the wait" as *pending* and re-check next tick, and alert only when the state is not reached after the agreed budget. Suggested JIRA: SOCS-FR-6 (P1).
SOCS-FR-7 - The business clock is hard-wired, so the schedule decision cannot be tested
Severity: Major (P1). Category: rewrite_recommended. Checklist: section 13 (tests deterministic: no clock) - Major; section 14 (DateTime.UtcNow/time-zone rules). Location: OperacionServicio.cs:84-85 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L84).
Code block (csharp)
var horaActual = DateTime.Now;
_configuracion.StrFechaSistemaServicio = new TimeSpan(horaActual.Hour, horaActual.Minute, 0);   // overwrites whatever a caller/test set
StrFechaSistemaServicio is a settable property on the injected configuration, but ObtenerParametros() always overwrites it with the server's local clock. Any test that sets it (UnitTest1.cs:80, 179, 269) is ignored, which is the root of FR-24. The window is evaluated against the *machine's* local time while the TOMI system date is read and discarded (FR-15) and the business-day rule uses another DateTime.Now inside the shared library (RepositorioMaestro.cs:1260,1271); the three clocks are never reconciled. The Str prefix on TimeSpan? members is also misleading (FR-14).
Code block (csharp)
public interface IReloj { DateTime Ahora { get; } }                      // production: DateTime.Now (or TOMI date + local time of day)
public static class Horario { public static bool EnVentana(TimeSpan ahora, TimeSpan ini, TimeSpan fin) => ...; }  // pure, table-tested
Fix/benefit: inject the clock (and keep the decision in one pure function) so boundaries (06:09/06:10, 23:59/00:00, Saturday, holiday, batch) become fast deterministic tests; document which clock is authoritative. Suggested JIRA: SOCS-FR-7 (P1).
SOCS-FR-8 - A failed alert insert escapes the start/stop methods, and the alert block is duplicated
Severity: Minor (P2). Category: rewrite_recommended. Locations: ControladorServicios.cs:59-75 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L59), ControladorServicios.cs:106-121 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L106).
Code block (csharp)
catch (Exception ex)
{
    Config.EscribirLog($"ERROR|servicioArriba|Excepcion: {ex}");
    await _repositorioTomi.ProcesoInsertaAlerta(new AlertaDTO { ... PId = (int)_configuracion.Pid, Exception = ex });   // can throw (RepositorioTomi.cs:111,116)
}
return false;
ProcesoInsertaAlerta throws when the alert is not inserted (MensajeThrow, RepositorioTomi.cs:111/116). Because the call sits in the catch, the exception leaves IniciaServicioAsync/DetieneServicioAsync instead of returning the documented false; Task.WhenAll then throws, the per-service outcomes are lost, and EjecutaProcedimiento logs "Se forza la finalización del servicio" (FR-13). A TOMI outage therefore turns every failed start/stop into a generic tick failure. The two new AlertaDTO blocks differ only in the text. Fix/benefit: wrap the alert call in its own try/catch that logs and continues, and build the DTO in one helper AlertarFalloServicio(accion, nombre, ex). Suggested JIRA: SOCS-FR-8 (P2).
SOCS-FR-9 - A local copy of the library's service controller sits in the application layer
Severity: Minor (P2). Category: rewrite_recommended. Checklist: section 12 (no duplicated logic; layer boundaries). Locations: ControladorServicios.cs:1-124 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L1), ServiceHost.cs:93-94 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L93), Aplicacion.csproj:13-15 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Aplicacion.csproj#L13).
Aplicacion.Service.ControladorServicios wraps the Windows SCM (System.ServiceProcess) inside the *application* project, so the business class cannot be unit-tested for the SCM path without a real service, and Aplicacion carries a Windows-only reference with a hard-coded path (FR-1). The shared library already ships MUFG_T24.ControladorServicios_Utilidades.ControladorServicios with IniciaServicioConResultadoAsync/DetieneServicioConResultadoAsync, a CancellationToken, a per-call timeout and handling of the pending/paused states (FR-3, FR-6, FR-12). The name collision is visible in the host, which must fully qualify both types (Aplicacion.Interfaces.IControladorServicios, Aplicacion.Service.ControladorServicios, lines 93-94). Measured non-blank line overlap of this file with its siblings: 98 % (twin Swf_OUT_ControladorServiciosMQ), 81-85 % (IN/IN_MQ/CEP) - GEN-4/GEN-15. Fix/benefit: delete the per-solution copy and call the shared class (confirming its alerting contract), or move the thin wrapper to Infraestructura behind an IServiceControl seam that tests can fake. Suggested JIRA: SOCS-FR-9 (P2).
SOCS-FR-10 - "Entered the window" is tracked on a per-tick clone, so it is logged every tick and "left the window" is never logged
Severity: Minor (P2). Category: rewrite_recommended. Locations: OperacionServicio.cs:114-136 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L114), ServiceHost.cs:83,90 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L90).
Code block (csharp)
if (!_configuracion.EnHorarioOperaciones) { _configuracion.EnHorarioOperaciones = true; Config.EscribirLog("INFO|... Inicio horario de operaciones ..."); }  // :120-124
...
if (_configuracion.EnHorarioOperaciones)  { _configuracion.EnHorarioOperaciones = false; ... "Termina horario de operaciones" ... }               // :130-134
ConfiguracionServicio is MemberwiseCloned from the base object into every scope (ServiceHost.cs:90) and nobody writes EnHorarioOperaciones back to the base, so it starts every tick as false: the "Inicio horario" line is written on every in-window tick, and the "Termina horario" line can never fire. The three parameter-echo lines (:49-50, 55-56, 61-62) add three more INFO lines per tick, so an in-window tick logs at least four lines (1,440 per hour at the 10 s fallback; the real interval is not in the repository). The host already registers a singleton EstadoHorarioOperacion (ServiceHost.cs:83; EnHorarioOperacion flag in the shared library) that nothing consumes - it is exactly the missing state holder. Fix/benefit: keep the previous state in that singleton, log only transitions, and log parameters only when they change. Suggested JIRA: SOCS-FR-10 (P2).
SOCS-FR-11 - The three workers are started and stopped in parallel with no ordering
Severity: Minor (P3). Category: rewrite_recommended (needs owner confirmation). Location: OperacionServicio.cs:140-155 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L140). READ, ETL and PK_DB form a pipeline by name (read -> transform -> package to database), yet all three Stop() calls are issued at the same instant and all three Start() calls likewise. If a downstream worker is stopped while the upstream one is still producing, or an upstream worker starts before its consumer, messages may wait or be reprocessed depending on the workers' own recovery (unverified - the workers are separate solutions). Fix/benefit: confirm the dependency with the worker owners; if it matters, stop upstream-first and start downstream-first with a status wait between stages, as a data-driven order list. Suggested JIRA: SOCS-FR-11 (P3).
SOCS-FR-12 - No cancellation reaches the tick, so a stopping controller can still start or stop workers
Severity: Minor (P3). Category: rewrite_recommended. Checklist: section 6 (CancellationToken accepted and propagated) - Major. Locations: IOperacionServicio.cs:17 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Interfaces/IOperacionServicio.cs#L17), ServiceHost.cs:132-140 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L132), IControladorServicios.cs:15,22 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Interfaces/IControladorServicios.cs#L15). OnTimerAsync(CancellationToken ct) receives the shutdown token but calls EjecutaProcedimiento() without it; none of the repository calls, IniciaServicioAsync, DetieneServicioAsync or the 30 s WaitForStatus can be cancelled. When the controller itself is stopped during a tick, TerminacionAsync waits for the running task (up to the base class's 60 s) while the tick may still issue Start/Stop commands against the workers. Fix/benefit: add CancellationToken to the three interfaces and pass it through to ct.ThrowIfCancellationRequested() checks and the wait loop (FR-3). Suggested JIRA: SOCS-FR-12 (P3).
SOCS-FR-13 - Copy-pasted and untrue log text misleads operators
Severity: Minor (P3). Category: rewrite_recommended. Locations: OperacionServicio.cs:55 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L55), OperacionServicio.cs:170 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L170), OperacionServicio.cs:181 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L181), ControladorServicios.cs:48,94 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Service/ControladorServicios.cs#L48).
Line	Text	Problem
OperacionServicio.cs:55	"... Hora de cancelacion SPEI '" + HoraOperacionesFin	Logs the *end of operations* of an outgoing SWIFT supervisor as the "SPEI cancellation time" (copied from the SPEI sibling)
OperacionServicio.cs:170	"ERROR|Ejecuta_Procedimientos|Se forza la finalización del servicio."	Nothing is forced: the method returns, the timer keeps running and the next tick retries
OperacionServicio.cs:49,61 vs 55	obtenerParametrosSistema vs ObtenerParametrosSistema	Inconsistent labels split one source across two search keys
OperacionServicio.cs:181	"no fue posible maracar el servicio en finalización"	Typo ("marcar")
ControladorServicios.cs:48,94	"INFO|Ejecuta_Procedimientos|Inicialización del Servicio ..."	Label belongs to OperacionServicio; the start and stop lines use the same label
OperacionServicio.cs:49-170	Config.EscribirLog(msg, _configuracion.NombreServicio)	The second argument is ignored by the legacy logger (Logging.cs:184-189); the controller omits it
OperacionServicio.cs:169	"... exception: " + ex.Message	Drops type and stack trace (the host's catch at ServiceHost.cs:145 logs {ex})

Fix/benefit: correct the labels, log {ex} once at the boundary, and drop the dead file-name argument; operators searching for Hora de cancelacion SPEI or Se forza la finalización stop chasing events that never happened. Suggested JIRA: SOCS-FR-13 (P3).
Nice to Rewrite if Time Permits
SOCS-FR-14 - Naming, dead members and unused usings in the application project
Severity: Minor (P3). Category: NiceToRewrite_ifTimePermits. Checklist: section 1 (no Hungarian notation, meaningful names), section 12 (dead code, unused usings). Locations: ConfiguracionServicio.cs:11-24 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Modelos/ConfiguracionServicio.cs#L11), EnumComunes.cs:1-3,10,22,28 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Enums/EnumComunes.cs#L10), IOperacionServicio.cs:1-5 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Interfaces/IOperacionServicio.cs#L1), MapearCamposGenerales.cs:1-3 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Modelos/Tomi/MapearCamposGenerales.cs#L1), IRepositorioTomi.cs:13-20 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Interfaces/IRepositorioTomi.cs#L13).
• StrFechaSistemaServicio, StrHoraOperacionesInicio, StrHoraOperacionesFin are TimeSpan? (Hungarian Str prefix on non-strings); StrFechaSistemaServicio holds a time of day, not a date.
• EnumComunes and EnumProcedimientosAlmacenados are static classes of const string (not enums); usp_MarcaPuertoEnEso is a typo for "EnUso"; usp_EsDiaHabil (EnumComunes.cs:26) is never used (the shared library owns that call).
• Unused members: StrFechaSistemaTomi (written at OperacionServicio.cs:76, never read), RutaArchivoConfiguracionOriginal, RutaArchivoConfiguracionFinal, RutaArchivoLogs (only set in tests), MensajesBatch (FR-5).
• Unused using lines (System.Collections.Generic/System.Text in three files); XML comments copied from siblings: RepositorioTomi.ValidaSiEsDiaHabil returns bool but its remarks describe a Dictionary<string, string> (RepositorioTomi.cs:120-123), and IRepositorioTomi.ActualizaPuertoLibre documents three parameters it does not have (:13-20).
Fix/benefit: rename (HoraActual, HoraInicioOperaciones, HoraFinOperaciones), delete the dead members and fix the doc comments in the same change as FR-5/FR-7. Suggested JIRA: SOCS-FR-14 (P3).
4.3 Infraestructura - Tier 1
MustChange / Rewrite
SOCS-FR-15 - A TOMI date is read on every tick, never used, and can abort the tick before any start/stop decision
Severity: Major (P1). Category: MustChange/rewrite. Checklist: section 4 (null checks before dereference) - Blocker; section 3 (correctness). Locations: OperacionServicio.cs:76 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/OperacionServicio.cs#L76), RepositorioTomi.cs:79-98 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L79), SQL 005 usp_ObtenerFechaSistema.sql:27-34 (../../StoredProcedures/MWD_DB/DBMDW/SWIFT/Outgoing/SWF_OUT_ControladorServicios/IT24/Procedures/005%20SP%20usp_ObtenerFechaSistema.sql#L27).
Code block (csharp)
_configuracion.StrFechaSistemaTomi = await _repositorioTomi.ObtenerFechaSistema();            // OperacionServicio.cs:76 - never read afterwards
 
if (strFecha?.Data == null || strFecha.Data.Count == 0 || strFecha.MensajeError != null)
    MensajeThrow($"Error: {strFecha.MensajeError}");                                          // :88 - NullReferenceException when strFecha is null
string strFechaFormateada = strFecha.Data.FirstOrDefault().Fecha.ToString(EnumParametrosGenerales.FECHA_BD);   // :90
StrFechaSistemaTomi is written once and never read anywhere in the solution, yet the call runs on every tick before the business-day check, the parameter read and the start/stop decision. Three defects follow: (a) when strFecha itself is null the guard's own message expression dereferences it (the catch at :93 wraps the NullReferenceException); (b) the procedure selects WHERE cerrado = 0 AND nextday = 1 AND openday = 1 without ORDER BY, so zero matching rows (closed day, end-of-day rollover - table contents are not in the repository) throw and abort the whole tick, and several rows return an arbitrary first one; (c) an irrelevant failure therefore prevents the supervisor from stopping or starting anything for that tick, and is reported only as a generic ERROR (no alert).
Fix/benefit: delete the call, the repository method, the SP wrapper and StrFechaSistemaTomi (or, if the TOMI business date is meant to drive the business-day rule, use it deliberately and handle "no open day" as a named state). The tick then depends only on data it actually uses. Suggested JIRA: SOCS-FR-15 (P1).
Rewrite Recommended
SOCS-FR-16 - MensajeThrow logs and throws a bare Exception inside the try whose catch logs and wraps again
Severity: Major (P1). Category: rewrite_recommended. Checklist: section 5 (wrap with inner exception; catch (Exception) only at boundaries; no custom/generic types where a framework type fits). Locations: RepositorioTomi.cs:88/95 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L88), :111/116 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L111), :176/181 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L176), :198/203 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L198), helper :213-217 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L213).
Code block (csharp)
try
{
    bool boAlertaInsertada = await _alertas.InsertarAlerta(alerta);
    if (!boAlertaInsertada) MensajeThrow($"Error: no se completo la insercion de la Alerta");   // logs, throws Exception
}
catch (Exception ex)
{
    MensajeThrow($"ERROR|ProcesoInsertaAlerta{ex}");                                            // catches that exception, logs again, throws a new Exception
}
private void MensajeThrow(string mensaje) { Config.EscribirLog(mensaje, ...); throw new Exception(mensaje); }
There are 12 MensajeThrow sites (two in the port-marking methods, ten in six try/catch methods). In four of them the throw is inside the try, so the same failure is logged twice here, then again by EjecutaProcedimiento (:169) and the host (ServiceHost.cs:145). Every wrapper flattens the original exception ({ex} is concatenated into text; there is no InnerException), so callers cannot tell a SqlException or a cancellation from a logic error, and the log line "ERROR|ObtenerFechaSistema{ex}" has no | between the label and the exception text, breaking the pipe-delimited convention used by every other line. The unreachable return string.Empty/false/new Dictionary after each catch exist only to satisfy the compiler.
Code block (csharp)
// repository: no catch-all; let the typed exception (or a RepositorioTomiException(msg, inner)) propagate
var resultado = await EjecutarStoredProcedureQuery<MapearCamposGenerales>(...);
if (!resultado.Exito) throw new RepositorioTomiException($"{nameof(ObtenerFechaSistema)} falló: {resultado.MensajeError}", resultado.Exception);
// boundary (OperacionServicio / host): log once with {ex}
Fix/benefit: one log line per failure at the boundary, the original type and stack preserved, and callers able to react selectively. Suggested JIRA: SOCS-FR-16 (P1).
SOCS-FR-17 - Provider failures are indistinguishable from "no data" in the parameter read
Severity: Minor (P2). Category: rewrite_recommended. Locations: RepositorioTomi.cs:146-156 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L146), :176, 198 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L176), SQL 003 usp_ConsultaParametrosSistema_SwfOutCtrlSrv.sql:32-37 (../../StoredProcedures/MWD_DB/DBMDW/SWIFT/Outgoing/SWF_OUT_ControladorServicios/IT24/Procedures/003%20SP%20usp_ConsultaParametrosSistema_SwfOutCtrlSrv.sql#L32).
Code block (csharp)
if (resultadoSp?.Data == null) return new Dictionary<string, string>();          // :148 - Exito/MensajeError never read
...GroupBy(x => x.Id_parametro).ToDictionary(x => x.Key, x => x.LastOrDefault()?.Valor ?? string.Empty);
if (!resultActualizaEjec?.Exito ?? false) MensajeThrow(...)                      // :176, :198 - a null result is treated as success
EjecutarStoredProcedureQuery reports a SqlException as Exito = false with an empty list (portfolio GEN-11). A failing parameter query therefore becomes an empty dictionary and surfaces later as "No se especifica la hora de inicio de operaciones" (OperacionServicio.cs:87), which blames the configuration instead of the database. LastOrDefault() is arbitrary when a parameter id appears twice because the procedure has no ORDER BY and the table's uniqueness on id_parametro is not visible. The !x?.Exito ?? false idiom is correct only by operator precedence and treats a null result as success (unreachable today because the shared ActualizaEJecucion throws on failure). Fix/benefit: branch on Exito first and throw a typed exception; enforce one row per parameter id (unique key or ORDER BY); replace the null-coalescing idiom with an explicit resultado == null || !resultado.Exito. Suggested JIRA: SOCS-FR-17 (P2).
SOCS-FR-18 - Dead repository methods, constants and stored procedures; the in-use SP and usp_EsDiaHabil have competing definitions
Severity: Minor (P2). Category: rewrite_recommended. Locations: RepositorioTomi.cs:38-47, 59-72 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L38), IRepositorioTomi.cs:21,31 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Interfaces/IRepositorioTomi.cs#L21), EnumComunes.cs:26,28-29 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Aplicacion/Enums/EnumComunes.cs#L26), SQL 001, 002, 004.
ActualizaPuertoLibre and ActualizaPuertoEnUso (plus their interface members, the constants usp_MarcaPuertoLibre/usp_MarcaPuertoEnEso and the stored procedures usp_ActualizaServicioLibre_SwfOutCtrlSrv / usp_ActualizaServicioEnUso_SwfOutCtrlSrv) have no caller in this solution: the host marks the process through the shared ControlServicio.MarcarPuertoEnUsoAsync, which calls a different procedure with a different signature (dbo.usp_Comun_ActualizarEnUso, FR-20). One of the two mechanisms is dead, and a reader cannot tell which. The own usp_EsDiaHabil (SQL 004, no DROP PROCEDURE IF EXISTS, so it cannot be re-run once the object exists) is one of six CREATE PROCEDURE dbo.usp_EsDiaHabil files found in the SQL project (the shared-library definition plus five supervisor folders), so whichever script is deployed last wins (portfolio GEN-13). Fix/benefit: delete the unused C# members and procedures (or wire them and remove the shared call), and keep exactly one usp_EsDiaHabil definition. Suggested JIRA: SOCS-FR-18 (P2).
SOCS-FR-19 - The repository depends on the concrete Alertas class and on a configuration it only uses for a dead log argument
Severity: Minor (P3). Category: rewrite_recommended. Locations: RepositorioTomi.cs:21-31 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Infraestructura/Servicio/RepositorioTomi.cs#L21), ServiceHost.cs:52-53 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L52). RepositorioTomi(Alertas alertas, ConfiguracionServicio configuracion) takes the concrete class although the host registers IAlertas (the interface the shared ControlServicio consumes), so the repository cannot be tested without constructing a real Alertas (which opens its own database connection factory). _configuracion is used only as the second argument of Config.EscribirLog, which the legacy logger ignores (Logging.cs:184-189). The class comment says "BD InterfacesT24" while the base constructor selects NombreBasedatos.TOMI. Fix/benefit: inject IAlertas, drop the configuration parameter, and align the comment. Suggested JIRA: SOCS-FR-19 (P3).
4.4 Swf_OUT_ControladorServicios (host) - Tier 2
MustChange / Rewrite
SOCS-FR-20 - Startup and shutdown database markers are ignored, and the shutdown path logs success regardless
Severity: Major (P1). Category: MustChange/rewrite. Checklist: section 5 (every result handled), section 3 (correctness). Portfolio GEN-9, with a GEN-13 candidate. Locations: ServiceHost.cs:119 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L119), ServiceHost.cs:171-177 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L171).
Code block (csharp)
await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, ..., ct);              // :119 - bool result discarded
//Marca el servicio en uso...
await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio, cleanupCt);   // :171-172 - frees the row; comment says "en uso"
await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio, cleanupCt);          // :175 - StoredProcedureResult discarded
Config.EscribirLog("INFO|OnStop|Terminación exitosa de bases de datos.");                           // :177 - always logged
The shared MarcarPuertoEnUsoAsync returns false (it does not throw) on cancellation or OperacionNoExitosaException (ControlServicio.cs:54-64), and ActualizaTerminacion reports failure through Exito = false (RepositorioMaestro.cs:1209-1214, 1220-1228). The host never inspects either result, so a service can run without ever recording its PID, and a failed termination update is followed by "Terminación exitosa". Evidence that this can really happen: MarcarPuertoEnUsoConResultadoAsync executes dbo.usp_Comun_ActualizarEnUso (ConstantesProcedimientosMUFG.cs:11), and a search of every .sql, .sqlproj and .cs file in the portfolio (excluding docs) finds no `CREATE PROCEDURE` for it - only the constant. Against a database built from this repository the call fails with "could not find stored procedure"; the shared adapter turns that into Exito = false (GEN-11) and the caller drops it. The deployed database may hold a hand-created copy; confirm with the DBA. The solution's own in-use/free procedures (FR-18) were evidently meant for this but are not called.
Code block (csharp)
if (!await _controlServicio.MarcarPuertoEnUsoAsync(pid, proceso, display, nombre, ct)) throw new InvalidOperationException("No se pudo registrar el servicio en uso en TOMI.");
var term = await _controlServicio.ActualizaTerminacion(nombre, cleanupCt);
if (!term.Exito) Config.EscribirLog($"ERROR|OnStop|Terminación no registrada: {term.MensajeError}"); else Config.EscribirLog("INFO|OnStop|Terminación exitosa de bases de datos.");
Fix/benefit: truthful startup/shutdown bookkeeping, and the monitoring that reads those rows stops trusting stale PIDs. Fix the comment on :171 and use an explicit "free" operation instead of passing null to the in-use call. Suggested JIRA: SOCS-FR-20 (P1).
Rewrite Recommended
SOCS-FR-21 - Composition-root clean-up: dead locals/registrations, obsolete logging API, unused scope
Severity: Minor (P3). Category: rewrite_recommended. Checklist: section 11 (zero new warnings), section 12 (dead code, obsolete APIs removed). Locations: ServiceHost.cs:7,82-83,88 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L82), :77-78 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L77), :164 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L164).
• var parametrizacion = new Parametrizacion(); (:88) is never used (and IParametrizacion is registered at :55).
• Register<Configuracion>(...) (:82) and Register<EstadoHorarioOperacion>(...) (:83) have no first-party consumer (the latter is the missing state holder of FR-10).
• Logging.Init/Logging.Write (:77-78) bind to MUFG_T24.Logeo_utilidades.Logging, which the library marks [Obsolete("Legacy compatibility only...")] (Config.cs:145-146); the solution also uses the Config.EscribirLog legacy façade in 21 places. All three paths end in the same file logger, but the obsolete type produces compiler warnings (CS0618).
• RutaArchivoConfiguracion (:66) and configuracion (:69) are assigned and never consumed by first-party code; TamanoMaximoArchivoLogKB is converted to a string only to be stored.
• using (AsyncScopedLifestyle.BeginScope(...)) at :164 wraps a transient ControlServicio that needs no scope; using MUFG_T24.Comunes.Encriptacion; (:7) is unused.
• TerminacionAsync (:152-192) re-implements hooks the base host already provides: it tests _containerDI == null itself (:158; the base offers InicializacionCompleta), swallows OperationCanceledException as the base does, disposes the container in its own finally (:190; the base offers LiberarRecursosTerminacion), and writes the same "Finalizando Servicio" / "Terminación exitosa" lines the base OnStop already logs (WindowsServiceHostBase.cs:117, 146), so each appears twice. If the base wait budget expires while a tick is still running, the container is disposed under it.
Fix/benefit: move to MUFG_T24.Infraestructura.Logging.Logging, delete the dead lines, and let the build run warning-free. Suggested JIRA: SOCS-FR-21 (P3).
SOCS-FR-22 - The host restores 20 packages that no first-party code uses (including a mocking library)
Severity: Minor (P3). Category: rewrite_recommended. Locations: packages.config:3-22 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/packages.config#L3), host csproj:41-108 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/Swf_OUT_ControladorServicios.csproj#L41), tests packages.config:3-8 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/packages.config#L3). The host lists 20 packages.config entries: ten Microsoft.*/Microsoft.Extensions.* packages and System.IO.Pipelines, System.Text.Encodings.Web, System.Text.Json (13 at version 10.0.3, built for net462 and loaded on net48), plus System.Buffers/Memory/Numerics.Vectors/..., System.ValueTuple and `Castle.Core` 5.2.1 (a Moq dependency). A scan of all first-party .cs files finds zero references to Microsoft.Extensions, System.Text.Json or Castle. They may be runtime requirements of MUFG_T24.dll (then they belong in that library's package, FR-1) or leftovers; either way each is an unreviewed production dependency (security patching, binding redirects). The test project pins Castle.Core 5.1.1 while the host pins 5.2.1, and MSTest 2.2.10 is several major versions old. The designer file is still named Service1.Designer.cs (class ServiceHost, DependentUpon ServiceHost.cs). The solution maps only Any CPU while every project forces PlatformTarget x86, and no CI file or analyzer settings are tracked. Fix/benefit: after FR-1, restore through the feed, let MUFG_T24 declare its own transitive packages, and remove everything the host does not use; rename the designer file. Suggested JIRA: SOCS-FR-22 (P3).
Nice to Rewrite if Time Permits
SOCS-FR-23 - Timer defaults are dead or silent
Severity: Minor (P3). Category: NiceToRewrite_ifTimePermits. Locations: ServiceHost.cs:33-34,68,70 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L33).
Code block (csharp)
protected override bool TimerEnabled => _configuracionBase?.TimerEnabled == true;
protected override double TimerIntervalMs => _configuracionBase?.TimerInterval ?? 10000;
_configuracionBase.TimerInterval = servicioMainConfiguracion.Timer;     // int, defaults to 0 when the attribute is absent (ObtencionConfguracionGeneral.cs:100)
After Inicializacion the 10 s fallback can never apply: TimerInterval is always the integer from configuration, which is 0 when the Timer attribute is missing, and a zero System.Timers.Timer.Interval is expected to be rejected at startup (a fail-fast with an unhelpful message). Likewise TimerEnable defaults to false (ObtencionConfguracionGeneral.cs:102), so a missing attribute starts a service that never ticks and logs nothing about it. Fix/benefit: validate both values in Inicializacion with a named message (and log "timer disabled"), or implement the documented default. Suggested JIRA: SOCS-FR-23 (P3).
SOCS-FR-26 - A log size configured in kilobytes is passed to a parameter measured in megabytes
Severity: Minor (P3). Category: rewrite_recommended. *(Added after cross-checking the twin's report, which found the same line; numbered last to keep the other IDs stable.)* Locations: ServiceHost.cs:71,77 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/Swf_OUT_ControladorServicios/ServiceHost.cs#L77); shared Logging.cs:32-41 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Logging/Logging.cs#L32).
Code block (csharp)
Logging.Init(rutas, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);   // ServiceHost.cs:77
// shared: Init(RutasConfiguracion rutas, string filePrefix = "Mufg", int maxMb = 100) { ... TamanoMaximoKB = maxMb * 1024 }
The setting is named ...KB but the third parameter of Init is maxMb and is multiplied by 1,024, so a configured 5120 would allow roughly 5 GB per log file instead of 5 MB. Which unit the central configuration really holds is not visible in the repository (confirm with the owners; the same call exists in the sibling hosts). Fix/benefit: convert explicitly once the unit is confirmed (maxMb: Math.Max(1, kb / 1024)), ideally inside the shared library, so log files stay at the intended size. Suggested JIRA: SOCS-FR-26 (P3).
4.5 PruebasUnitariasSwfCtrlSrv (tests) - Tier 2
MustChange / Rewrite
SOCS-FR-24 - The suite cannot be green at any time of day, has no assertions, and hides exceptions
Severity: Major (P1). Category: MustChange/rewrite. Checklist: section 13 (deterministic; assert behaviour and exceptions; full suite green) - Major/Blocker. Portfolio GEN-2. Locations: UnitTest1.cs:24-118 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/UnitTest1.cs#L24), :123-208 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/UnitTest1.cs#L123), :213-291 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/UnitTest1.cs#L213).
Code block (csharp)
[DataRow("06:10", "Entra en el bloque de READ")] ... [DataRow("06:15", "Ejecuta los 4 permanentes")]   // :25-28 expect Inicia...Times.Once
[DataRow("06:07", "No debe entrar READ")] [DataRow("06:08", "No debe entrar ETL")] ...                  // :124-126 expect Inicia...Times.Never
_configuracion.StrFechaSistemaServicio = horaSistema;                                                    // :80, 179, 269 - overwritten by DateTime.Now at OperacionServicio.cs:85
try { await _operacionServicio.EjecutaProcedimiento(); } catch (Exception ex) { excepcionCapturada = ex; }  // :82-89 - never asserted
1. Contradictory by construction. The scenario hour is overwritten with the wall-clock time (FR-7), so the "must start" rows pass only inside 06:10-23:59 and the "must not start" rows pass only outside it. Probe over all 1,440 minutes: 0 minutes at which both groups pass. The suite is red at every time of day,
and which tests fail depends on when someone presses *Run*. No continuous-integration gate can use it.
2. No `Assert` call at all (0 Assert., 12 Mock.Verify(...)) and the Act swallows every exception into an unused local, so an early failure (a config or log-path problem) makes the "must not start" rows pass for the wrong reason.
3. The batch test never exercises batch mode: it sets the dead MensajesBatch = true (:270) while the stub returns SWF_OUT_GET_MT_BATCH = "0" (:227), and it asserts the same in-window start as test 1.
4. No coverage of the stop path or the non-business-day path: DetieneServicioAsync is never verified; EsDiaHabil is stubbed true in all three tests although the comments (:121, :211) promise a non-business-day scenario; nothing tests ControladorServicios, RepositorioTomi or the host (the test project references only Aplicacion).
5. The mocked IControladorServicios returns the Moq default (completed false), so the "No todos los servicios..." branch executes in every test and nobody notices.
Fix/benefit: after FR-7, rewrite as table-driven tests that fix the clock, assert the exact Inicia/Detiene calls for each of READ/ETL/PK_DB (business day, Saturday, holiday, before/at/after both boundaries, batch on/off, invalid HORA_OP_*, repository failure), assert the logged outcome, and fail on an unexpected exception; add Infraestructura tests with a fake RepositorioMaestro result and a scripted SCM fake for FR-3/FR-6. Suggested JIRA: SOCS-FR-24 (P1).
Rewrite Recommended
SOCS-FR-25 - Test hygiene: tripled arrange blocks, scenario-string branching, global state, misleading comments
Severity: Minor (P3). Category: rewrite_recommended. Locations: UnitTest1.cs:36-80, 134-179, 222-269 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/UnitTest1.cs#L36), :94-117, 192-207 (../../SWIFT/Outgoing/SWF_OUT_ControladorServicios/PruebasUnitariasSwfCtrlSrv/UnitTest1.cs#L94).
• The same ~45-line arrange block (parameters, configuration, mocks, log folder, stubs) is pasted three times; the unused var _operacion = new Mock<IOperacionServicio>() is created in all three (:53, 151, 241).
• Assertions are selected by if (escenario.Contains("READ") && escenario.Contains("Entra")) chains over free-text scenario names (:94-117, 192-207) instead of passing the expected calls as data.
• Each test mutates the static Config.Rutas and creates folders under %TEMP% (:61-66, 159-164, 249-254); the third uses "ControladorServiciosMQ" as the folder name (copied from the MQ twin). Logging should be a fake, not global state.
• Doc comments contradict the tests: :121 and :211 say "no entre aunque este en horario debido a ser dia no habil" (neither test sets a non-business day); scenario labels say "4 permanentes" (:28) and "3 permanentes" (:214) for three services; "Escenario 1/3/5/7" numbering is unrelated to the data rows.
Fix/benefit: one fixture builder, data-driven (clock, params, businessDay) -> expected calls, an injected logger/clock, and meaningful method names; a fraction of the lines with real coverage. Suggested JIRA: SOCS-FR-25 (P3).
5. Architecture-Fit Note
This solution is a scheduled reconciliation loop, not a listener or a multi-step saga: every tick it computes a desired state (workers running or stopped) from a small amount of TOMI data and the local clock, then drives three Windows services toward it through the SCM. Per the standing architecture question in _ORCHESTRATION.md section 6 (./_ORCHESTRATION.md#6-standing-architecture-question-is-the-current-hostingarchitecture-model-right), the right next step is to modernize in place: keep the Windows Service, make the decision a pure, clock-injected function (FR-5, FR-7), make the SCM transition policy explicit and testable (FR-3, FR-6, FR-9), and only then ask whether the supervisor should exist as a bespoke service at all. The obvious alternatives - Windows service *recovery options*/scheduled tasks for the start/stop windows, or a shared library supervisor used by all five copies (GEN-4, GEN-15) - are worth evaluating after the defects are fixed; a message-driven or serverless re-platform is not indicated by anything in this code. Unknown and needed before any hosting decision: the real worker shutdown time (FR-6), the production tick interval, whether outgoing SWIFT may run on weekends or overnight (FR-4), whether batch mode must ever stop the workers (FR-5), the service account's SCM rights, and the SLA for the outgoing pipeline. A wholesale rewrite is not recommended from this code-quality review.
6. JIRA-Ready Backlog
ID	Title	Category	Project	File(s)	Priority
SOCS-FR-1	Publish shared libraries as a package; remove 6 absolute-path references	MustChange/rewrite	All four projects	4 csproj files	P0 Blocker
SOCS-FR-2	Replace the template README with operational documentation	NiceToRewrite_ifTimePermits	Solution	README.md	P3 Minor
SOCS-FR-3	Handle StartPending/ContinuePending/PausePending/Paused/StopPending	MustChange/rewrite	Aplicacion	Service/ControladorServicios.cs	P1 Major
SOCS-FR-4	Validate schedule values (HH:mm, range, INI < FIN or explicit overnight) and the batch flag	MustChange/rewrite	Aplicacion	OperacionServicio.cs	P1 Major
SOCS-FR-5	Unify the batch branch with the window branch; remove MensajesBatch	rewrite_recommended	Aplicacion	OperacionServicio.cs, ConfiguracionServicio.cs	P2 Minor
SOCS-FR-6	Align the stop/start wait with the workers' shutdown budget; treat timeout as pending	rewrite_recommended	Aplicacion	Service/ControladorServicios.cs	P1 Major
SOCS-FR-7	Inject the business clock; extract a pure window/decision function	rewrite_recommended	Aplicacion	OperacionServicio.cs	P1 Major
SOCS-FR-8	Isolate alert-insert failures from the start/stop result; share the alert builder	rewrite_recommended	Aplicacion	Service/ControladorServicios.cs	P2 Minor
SOCS-FR-9	Replace the local controller with the shared class or an IServiceControl seam in Infraestructura	rewrite_recommended	Aplicacion	Service/ControladorServicios.cs, ServiceHost.cs	P2 Minor
SOCS-FR-10	Persist window state in the singleton; log transitions only	rewrite_recommended	Aplicacion / host	OperacionServicio.cs, ServiceHost.cs	P2 Minor
SOCS-FR-11	Confirm and, if needed, order stop/start across READ -> ETL -> PK_DB	rewrite_recommended	Aplicacion	OperacionServicio.cs	P3 Minor
SOCS-FR-12	Propagate CancellationToken through the tick and the SCM wait	rewrite_recommended	Aplicacion	Interfaces, OperacionServicio.cs, ServiceHost.cs	P3 Minor
SOCS-FR-13	Correct copy-pasted and untrue log messages	rewrite_recommended	Aplicacion	OperacionServicio.cs, Service/ControladorServicios.cs	P3 Minor
SOCS-FR-14	Rename Str* members, remove dead members/usings, fix doc comments	NiceToRewrite_ifTimePermits	Aplicacion	ConfiguracionServicio.cs, EnumComunes.cs, interfaces	P3 Minor
SOCS-FR-15	Remove the unused per-tick TOMI date read (null dereference, abort risk)	MustChange/rewrite	Infraestructura / Aplicacion	RepositorioTomi.cs, OperacionServicio.cs	P1 Major
SOCS-FR-16	Replace MensajeThrow log-and-throw with typed exceptions logged once at the boundary	rewrite_recommended	Infraestructura	RepositorioTomi.cs	P1 Major
SOCS-FR-17	Check Exito, enforce one row per parameter, explicit null checks	rewrite_recommended	Infraestructura	RepositorioTomi.cs, SQL 003	P2 Minor
SOCS-FR-18	Remove dead repository members/SPs; keep one usp_EsDiaHabil definition	rewrite_recommended	Infraestructura / SQL	RepositorioTomi.cs, IRepositorioTomi.cs, SQL 001/002/004/005	P2 Minor
SOCS-FR-19	Inject IAlertas; drop the unused configuration dependency	rewrite_recommended	Infraestructura	RepositorioTomi.cs, ServiceHost.cs	P3 Minor
SOCS-FR-20	Check startup/shutdown marker results; confirm/define usp_Comun_ActualizarEnUso	MustChange/rewrite	Host	ServiceHost.cs	P1 Major
SOCS-FR-21	Clean up the composition root and the obsolete logging API	rewrite_recommended	Host	ServiceHost.cs	P3 Minor
SOCS-FR-22	Remove unused packages (20 restored, 0 used); rename the designer file	rewrite_recommended	Host	packages.config, csproj, Service1.Designer.cs	P3 Minor
SOCS-FR-23	Validate and log timer settings instead of silent/dead defaults	NiceToRewrite_ifTimePermits	Host	ServiceHost.cs	P3 Minor
SOCS-FR-24	Rebuild the unit tests: deterministic clock, real assertions, stop/non-business-day/failure paths	MustChange/rewrite	Tests	UnitTest1.cs	P1 Major
SOCS-FR-25	Remove arrange duplication, scenario-string branching and global state from the tests	rewrite_recommended	Tests	UnitTest1.cs	P3 Minor
SOCS-FR-26	Convert the log-size setting (KB) to the unit Logging.Init expects (MB)	rewrite_recommended	Host	ServiceHost.cs	P3 Minor

6.1 Suggested order of work
1. FR-1 (build) and the shared-library packaging - everything else needs a green build. Agree the weekend/overnight/batch rules and obtain the workers' real shutdown time (inputs to FR-4/5/6).
2. FR-7 then FR-24/FR-25 - clock seam and a trustworthy test suite *before* behavior changes, so each later fix has a failing-then-passing regression test.
3. FR-3, FR-4, FR-6, FR-15, FR-20 - correctness: pending states, schedule validation, timeout policy, the unused date read, truthful bookkeeping.
4. FR-16, FR-17, FR-8, FR-5, FR-10, FR-13 - exception/logging hygiene and branch unification.
5. FR-9, FR-18, FR-19, FR-21, FR-22, FR-26, FR-2, FR-11, FR-12, FR-14, FR-23 - structure, dead code and housekeeping; then evaluate the shared-supervisor idea for all five copies (GEN-4/GEN-15).
