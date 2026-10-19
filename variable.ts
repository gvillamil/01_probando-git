Entradas Recommendations
Entradas - Architecture & Code Quality Recommendations
Solution: General/Entradas/Entradas.sln (../../General/Entradas/Entradas.sln) Reviewed source: branch CodeReview2, commit 23ce60095802442f42e1784db9b2d9c457453ed5 Compared against: LoansApp architecture and coding baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR review/severity checklist (../BaseLine_Docs/code_review.md) See also: _ORCHESTRATION.md (./_ORCHESTRATION.md) for the portfolio methodology, template, and standing findings.
1. Solution Overview (low/no-dependency -> high dependency)
Tier	Project	Kind	Path
0	AplicacionEntradas	.NET Framework 4.8 class library	AplicacionEntradas.csproj (../../General/Entradas/Aplicacion/AplicacionEntradas.csproj)
1	InfraestructuraEntradas	.NET Framework 4.8 class library	InfraestructuraEntradas.csproj (../../General/Entradas/Infraestructura/InfraestructuraEntradas.csproj)
2	Entradas	.NET Framework 4.8 Windows Service host	Entradas.csproj (../../General/Entradas/Entradas/Entradas.csproj)
2	PruebasUnitarias	MSTest test project	PruebasUnitarias.csproj (../../General/Entradas/PruebasUnitarias/PruebasUnitarias.csproj)

What this solution actually does: Entradas is a timer-driven Windows Service that discovers newly arrived payment and treasury operations and registers each one in the TOMI work-plan tables so that downstream services can process it. On every tick it marks itself as running, loads six schedule/SPEI parameters and the business date from TOMI, asks the database whether today is a business day, and compares the server time with the reading and operations windows. Outside the reading window it inserts a CRITICO alert and ends the tick. Inside the reading window it always reads pending CMS/Bot formats (FXD, FXI, FXT, SAT, MFX, SER, TAX, SPID, TEF, TMD, NOM_FXD and four PaP families). On business days inside the operations window it also reads, concurrently, local, GPH, COAS, concentradora, FOREX-confirmation, Calypso money-market, SPID, TEF and, when enabled by parameter, SPEI-1 and SPEI-2 sources. Every reader builds a dictionary of TYPE-folio entries filtered by the host's OperacionesPermitidas allow-list and submits it through one table-valued-parameter procedure, usp_RegistrarEntradasPlanTrabajoENTRADAS, which creates the master and detail work-plan rows. The host composes everything with SimpleInjector, opens one async scope per tick, and inherits timer, overlap-lock, and shutdown behavior from the shared WindowsServiceHostBase.
2. Scope & Methodology Note
This is a fresh review after the authoritative portfolio reset dated 2026-10-07. Historical Entradas reports and finding IDs were not used as evidence; every finding below was derived from the current source.
Scope item	Result
Repository state	Clean CodeReview2 branch at commit 23ce60095802442f42e1784db9b2d9c457453ed5; git status reported no changes
Git-tracked scope	50/50 physical files reconciled exactly; no untracked, ignored, or generated source files. Playground/ is an empty local directory and not a solution member
Text scope	50/50 files, 3,891/3,891 lines, read in full with no sampling
Authored C# scope	40/40 files, 2,846/2,846 lines: AplicacionEntradas 24 files / 672 lines, InfraestructuraEntradas 10 / 1,373, Entradas host 4 / 323, PruebasUnitarias 2 / 478
Other tracked files	10 files / 1,045 lines: four .csproj, App.config, packages.config, Entradas.sln, .gitignore, .gitlab-ci.yml, README.md
Large-review threshold	Not reached (fewer than 150 files and fewer than 5,000 lines), so no persistent progress tracker was required
Shared-library context	Only directly controlling MUFG_T24 source was read to verify effective behavior: RepositorioMaestro (query, non-query, lifecycle, and business-day methods), Configuracion, WindowsServiceHostBase, Parametrizacion, Alertas, ControlServicio.MarcarPuertoEnUsoAsync, IParametrizacion, IAlertas, ConversionHelper.ParseTimeSpanRequerido, and ObtencionConfguracionGeneral.ObtenerServicio. None of it is counted as solution-owned and no finding is located inside it
SQL context	Fifteen definitions in the portfolio SQL project were read to verify what the C# contracts actually call: the registration procedure and its table type, the parameter/date/FOREX/liquidation/BMX readers, five Bot procedures (MFX read, MFX update, TMD read, NOM_FXD, and PaP), one H2H reader with its GPH counterpart, and the SPEI reader; the result columns of the remaining Entradas readers were checked against the DTO property names (all matched), and every parameter name that could be checked was compared with the C# call (one mismatch, ENT-FR-11). The existing Entradas SP audit (../../StoredProcedures/docs_v3/recommendation/Entradas-SP-Architecture-Audit.md) independently corroborates the registration-procedure defect cited in ENT-FR-7. These are dependency context, not solution files
Requirements basis	No JIRA/ADO story or acceptance criteria were supplied; this is a baseline architecture and code-quality audit, not a change-specific PR review
Executable validation	Build and tests could not run: dotnet, MSBuild, VSTest, NuGet, and VSWhere are unavailable, the .NET Framework 4.8 reference pack is absent, D:\ServiceCMS\MUFG_T24.dll does not exist, and the repository packages directory is not restored. Source remained clean

The full read was supplemented by quantified scans over the authored C#: 29 EjecutarStoredProcedureQuery call sites plus one EjecutarStoredProceduredNonQueryConResultado call, with zero reads of Exito, MensajeError, or Exception on any result; 36 OperacionesPermitidas allow-list expressions, 33 new FolioOperacionDTO constructions, and 21 stale Prefijo ... llave duplicada comments across six repository files; 41 Config.EscribirLog calls; 13 throw; and no throw ex;; zero CancellationToken parameters on the 11 reader contracts and zero token forwards to shared repository calls; zero string-concatenated SQL, raw SqlConnection/SqlCommand, sync-over-async, async void, weak-crypto, or credential literals; and 3 MSTest methods with 24 assertions beside 22 NotImplementedException stubs.
False-positive checks were resolved as follows:
• Concurrent use of one repository instance by Task.WhenAll is safe: the shared base creates and disposes a new SqlConnection per call (RepositorioMaestro.cs:248-410 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L248-L410)), and FiltroFolios and OperacionesGenerales hold no mutable state.
• (bool)_configuracion.EsDiaHabil (OperacionServicio.cs:209 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L209)) is assigned from a non-nullable bool on the same instance at line 86, and (bool)_configuracion.BenHorario (line 246) is a bool? initialised to false that is never set to null.
• OperacionesPermitidas is never null on the production path: the host assigns a trimmed non-null array (ServiceHost.cs:73-77 (../../General/Entradas/Entradas/ServiceHost.cs#L73-L77)) and ObtenerServicio throws instead of returning null; the 36 dereferences remain unguarded, so any construction path that bypasses the host would fail with a NullReferenceException (addressed in ENT-FR-12).
• The designer's generic "ServiceHost" name is overwritten by the shared base (ServiceName = NombreServicioLog, WindowsServiceHostBase.cs:99 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Core/Estructura/WindowsServiceHostBase.cs#L99)).
• _configuracionBase.Pid.Value (ServiceHost.cs:151 (../../General/Entradas/Entradas/ServiceHost.cs#L151)) is guarded by the HasValue check at line 131 and by the shared startup contract.
• The switch (intTipo) blocks without default match the closed tipoFormato IN (...) list of the MFX procedure today; the drift risk is recorded under ENT-FR-12 rather than as a current defect.
• MUFG_MX2_ObtenerFormatoTMDENTRADAS matches its SQL definition; only the script's file name carries a different prefix.
• ActualizaEJecucion throws when its update is unsuccessful, so a failure of the first call in a tick is not silent; failures of ActualizaTerminacion are logged by the shared method.
Dependency notes (context only; these belong to the MUFG_T24 audit and are not Entradas findings):
• RepositorioMaestro converts every SqlException into Exito = false with no log entry and no exception (RepositorioMaestro.cs:385-390 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L385-L390), RepositorioMaestro.cs:988-993 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L988-L993)); ENT-FR-7 is the service-side misuse of that contract.
• EsDiaHabil(ct) leaves its result true when the holiday query fails, because FirstOrDefault()?.Num_regs > 0 is then false (RepositorioMaestro.cs:1245-1290 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L1245-L1290)).
• Alertas.InsertarAlerta rethrows the alert's original exception, not the insert failure, when the insert fails (Alertas.cs:164-173 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Alertas/Alertas.cs#L164-L173)).
Finding Summary
Category	Blocker	Major	Minor	Total
MustChange/rewrite	2	9	0	11
rewrite_recommended	0	4	1	5
NiceToRewrite_ifTimePermits	0	0	2	2
Total	2	13	3	18

Fresh Cross-Cutting Checks
Candidate	Result	Current evidence
GEN-1 absolute DLL references	Yes	Five D:\ServiceCMS references: MUFG_T24.dll in all four projects and SimpleInjector.dll in the host. See ENT-FR-15.
GEN-2 zero automated tests	No	A real MSTest project exists (3 methods, 24 assertions); its critical gaps are local finding ENT-FR-16.
GEN-5 sensitive logging/data exposure	No	Logs contain schedule values, internal folio/operation-type identifiers, and exceptions; no credential, account number, amount, or customer payload was found.
GEN-6 undisposed signing helper	No	No signing, certificate, or cryptographic helper exists in the solution.
GEN-7 finalization only on success	Yes (partial)	The main try finalizes in finally, but the pre-try preamble can fail after the service was already marked running. See ENT-FR-2.
GEN-8 nullable PID dereference	No	Pid.Value is guarded by HasValue and the shared startup contract; the application layer never dereferences it.
GEN-9 discarded service-state Boolean	Yes	Startup and shutdown discard MarcarPuertoEnUsoAsync and ActualizaTerminacion outcomes. See ENT-FR-14.
GEN-10 out-of-hours alert on every tick	Yes (alert only)	Every tick outside the reading window logs an INFO line and inserts a CRITICO alert row; the non-business-day branch does not alert. See ENT-FR-5.
GEN-11 repository failure indistinguishable from empty/successful result	Yes	All 29 query sites and the registration call read only Data, never Exito; the registration procedure's THROW is commented out. See ENT-FR-7, ENT-FR-8, and ENT-FR-11.

3. What's Already Good
Good area	Evidence and value
Parameterized data access	All 30 database calls name fixed procedures from ConstantesServicio and pass values as parameters. The registration call uses a typed table-valued parameter whose five columns and lengths match dbo.FolioOperacionTypeENTRADAS (OperacionesGenerales.cs:42-55 (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L42-L55), OperacionesGenerales.cs:85-104 (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L85-L104), table type (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Tables/001%20SP%20FolioOperacionTypeENTRADAS.sql#L25-L31)). Scans found no concatenated SQL and no raw ADO.NET.
Constructor injection, verified container, per-tick scope	All behavior is injected through interfaces, lifetimes are scoped, the container is verified at startup, and each tick resolves its own async scope (OperacionServicio.cs:47-60 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L47-L60), ServiceHost.cs:44-108 (../../General/Entradas/Entradas/ServiceHost.cs#L44-L108), ServiceHost.cs:163-181 (../../General/Entradas/Entradas/ServiceHost.cs#L163-L181)).
Startup identity validation	Process metadata is validated before .Pid.Value is read (ServiceHost.cs:124-151 (../../General/Entradas/Entradas/ServiceHost.cs#L124-L151)).
Correct rethrow discipline	Every reader and the registrar logs the full exception and rethrows with throw;; there is no throw ex; and no message-only rethrow (OperacionRepositorioCms.cs:423-427 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L423-L427), OperacionRepositorioTomi.cs:58-62 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L58-L62), OperacionesGenerales.cs:176-180 (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L176-L180)).
Safe allow-list normalization	OperacionesPermitidas becomes a trimmed, non-null array at startup, so the 36 Contains/Length expressions are safe on the production path (ServiceHost.cs:73-77 (../../General/Entradas/Entradas/ServiceHost.cs#L73-L77)).
Idempotent polling	Readers re-select pending rows on every tick, and the registration procedure checks for existing master and detail rows before inserting, so re-reading the same pending operation does not create duplicates (procedure checks (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/020%20SP%20usp_RegistrarEntradasPlanTrabajoENTRADAS.sql#L163-L200)).
Testable boundaries	Time, parameters, run state, alerting, and every reader are behind interfaces; three deterministic tests use hand-written fakes with no database, network, or sleep (EntradasUT.cs:87-155 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L87-L155)). The IParametrizacion fake implements every member of the current shared interface.
Repository hygiene	The standard .gitignore excludes bin, obj, packages, .vs, .env, and *.pfx; no build output, credential, or connection string is tracked, and App.config contains only runtime metadata (App.config:1-8 (../../General/Entradas/Entradas/App.config#L1-L8)).
No high-risk APIs	No weak cryptography, TLS bypass, async void, .Result/.Wait(), BinaryFormatter, string-built SQL, or DateTime.Now appears in authored production code.

4. Findings by Project (low/no-dependency -> high dependency)
4.1 AplicacionEntradas (Tier 0)
MustChange / Rewrite
ENT-FR-1 - Validate the required schedule and SPEI parameters; the current null checks can never fire (Major)
Category: MustChange/rewrite Priority: Major
Evidence: OperacionServicio.cs:92-153 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L92-L153), ConfiguracionServicio.cs:11-18 (../../General/Entradas/Aplicacion/Modelos/ConfiguracionServicio.cs#L11-L18), ConsultaCamposBDTomi.cs:49-62 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L49-L62), usp_ObtenerParametrosSistemaENTRADAS (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/018%20SP%20usp_ObtenerParametrosSistemaENTRADAS.sql#L27-L39) (dependency context)
Code block (csharp)
public TimeSpan StrHoraLecturaInicio { get; set; }      // ConfiguracionServicio.cs:11 (non-nullable)
 
if (_configuracion.StrHoraLecturaInicio == null)        // OperacionServicio.cs:130
{
    throw new Exception(
        "No se especifica la hora de inicio de lectura");
}
All four Str* schedule fields are non-nullable TimeSpan, so comparing them with null is always false (compiler warning CS0472) and the four "No se especifica ..." exceptions at lines 130-152 are unreachable. The procedure returns only the rows that exist, and ParametrosSistemaAsync turns them into a dictionary without checking for the six expected keys. Enum.TryParse at line 94 is case-sensitive and silently skips any key it does not recognise, and the SPEI flags are compared with == "1" without trimming or validation (lines 231 and 236). A missing, renamed, differently-cased, or padded row therefore leaves 00:00:00 or null. With no reading window, every tick is "fuera de horario" and inserts a CRITICO alert (lines 184-200). With only the operations window missing, the CMS read still runs but every business-day source (lines 211-241) is skipped with no error or alert (the only trace is the per-tick information line at line 156). A missing or malformed SPEI flag silently disables that SPEI reader. The shared IParametrizacion that is already injected into this class provides required-key validation (Parametrizacion.cs:229-248 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Aplicacion/Control/Parametrizacion.cs#L229-L248)) but is used here only for ObtenerHoraSistema().
Recommended change: validate the key set once per tick (or load through the shared required-key loader) and reject missing, empty, or malformed values with a specific exception that names the keys.
Code block (csharp)
string[] faltantes = Enum.GetNames(typeof(ParametrosSistema))
    .Where(k => !dicParametrosSistema.ContainsKey(k)).ToArray();
if (faltantes.Length > 0)
{
    throw new InvalidOperationException(
        "Parametros de sistema requeridos ausentes: " + string.Join(", ", faltantes));
}
// Enum.TryParse(paramSistema.Key, true, out ParametrosSistema parametro) and paramSistema.Value.Trim() == "1"
Benefit: a configuration omission fails loudly and is alerted once with its cause, instead of silently disabling readers or producing an unexplained alert on every tick; four unreachable branches and a compiler warning disappear.
Suggested JIRA: ENT-FR-1 (Major, MustChange/rewrite).
ENT-FR-2 - Bring the tick preamble inside the alert and finalization boundary (Major)
Category: MustChange/rewrite Priority: Major
Evidence: OperacionServicio.cs:68-160 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L68-L160) (preamble), OperacionServicio.cs:162-281 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L162-L281) (guarded region), ServiceHost.cs:163-181 (../../General/Entradas/Entradas/ServiceHost.cs#L163-L181), RepositorioMaestro.cs:1158-1176 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L1158-L1176) (dependency context)
Code block (csharp)
await _IparametrosOperaciones.MarcarServicioEnEjecucionAsync(_configuracion.NombreServicio, ct);   // L68: marks "running"
var dicParametrosSistema = await _IparametrosOperaciones.ParametrosSistemaAsync(ct);               // L72
_configuracion.EsDiaHabil = await _IparametrosOperaciones.BEsDiaHabil(ct);                         // L86
// ... ParseTimeSpanRequerido and four validation throws (L92-152) ...
try { /* L162 */ }
catch (Exception ex) { /* log + EnviaAlerta */ }
finally { /* MarcarServicioEnFinalizacion */ }
Everything that can fail before line 162 escapes to the host's OnTimerAsync catch, which only writes a log line: the shared ActualizaEJecucion (it throws when its update is unsuccessful), the parameter, date, and business-day queries, ParseTimeSpanRequerido (a bare Exception for an empty or malformed value), and the validation throws. No CRITICO alert is inserted and MarcarServicioEnFinalizacion never runs, although line 68 has already marked the service as running. A persistent error such as an invalid time string or a parameter-source outage therefore repeats on every tick with only a log entry, and the persisted state stays "en ejecución". This is the pre-try scope of standing finding GEN-7.
Recommended change: move the whole tick into one guarded region so a single catch, alert, and finally cover context loading and processing.
Code block (csharp)
try
{
    await _IparametrosOperaciones.MarcarServicioEnEjecucionAsync(_configuracion.NombreServicio, ct);
    ContextoTick contexto = await CargarContextoAsync(ct);   // parameters, business date, business day, validation
    await ProcesarAsync(contexto, ct);
}
catch (Exception ex) when (!(ex is OperationCanceledException))
{
    await NotificarFalloAsync(ex);
}
finally
{
    await FinalizarEjecucionAsync();
}
Benefit: every failure class raises the same alert and ends in a deterministic persisted state, and the preamble becomes unit-testable on its own.
Suggested JIRA: ENT-FR-2 (Major, MustChange/rewrite; confirms the pre-try scope of GEN-7).
ENT-FR-3 - Await alert insertion and depend on the alert abstraction (Major)
Category: MustChange/rewrite Priority: Major
Evidence: IParametrosOperaciones.cs:19 (../../General/Entradas/Aplicacion/Interfaces/IParametrosOperaciones.cs#L19), ConsultaCamposBDTomi.cs:20-28 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L20-L28), ConsultaCamposBDTomi.cs:75 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L75), OperacionServicio.cs:188-200 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L188-L200), OperacionServicio.cs:257-267 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L257-L267), ServiceHost.cs:51-52 (../../General/Entradas/Entradas/ServiceHost.cs#L51-L52), Alertas.cs:99-175 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Alertas/Alertas.cs#L99-L175) (dependency context)
Code block (csharp)
void EnviaAlerta(AlertaDTO alerta);                                                     // IParametrosOperaciones.cs:19
public void EnviaAlerta(AlertaDTO alertaDTO) => _alerta.InsertarAlerta(alertaDTO);      // ConsultaCamposBDTomi.cs:75
Alertas.InsertarAlerta is an async Task<bool>. The void contract discards that task, so the database insert is still in flight when the caller continues, its Boolean outcome (spResult.Exito) is lost, and when the insert fails the shared method rethrows the alert's original exception into a task that nobody observes. The alert is the only operator signal for both the out-of-hours and the tick-failure paths, so its own failure (for example during database trouble, exactly when alerts matter) is invisible beyond one shared-library log line. The class also receives the concrete Alertas although IAlertas is registered for exactly this purpose, and the interface mixes parameter loading, business-day lookup, run-state marking, and alerting in one contract.
Recommended change: make the contract asynchronous, inject IAlertas, and observe the result without letting alert failure mask the original error.
Code block (csharp)
Task<bool> EnviaAlertaAsync(AlertaDTO alerta, CancellationToken ct);
 
if (!await _alertas.InsertarAlerta(alerta, CancellationToken.None).ConfigureAwait(false))
{
    Config.EscribirLog("ERROR|Ejecuta_Procedimientos|La alerta CRITICO no pudo registrarse.", NombreServicio.Entradas.ToString());
}
Benefit: alert outcomes become observable and ordered with the rest of the tick, and the consumer depends on the abstraction already registered by the host.
Suggested JIRA: ENT-FR-3 (Major, MustChange/rewrite).
Rewrite Recommended
ENT-FR-4 - Propagate cancellation through the readers and use an independent token for finalization (Major)
Category: rewrite_recommended Priority: Major
Evidence: IOperacionesEntradaTomi.cs:10-20 (../../General/Entradas/Aplicacion/Interfaces/IOperacionesEntradaTomi.cs#L10-L20), IOperacionesEntradaCms.cs:10 (../../General/Entradas/Aplicacion/Interfaces/IOperacionesEntradaCms.cs#L10), IOperacionesGenerales.cs:12 (../../General/Entradas/Aplicacion/Interfaces/IOperacionesGenerales.cs#L12), OperacionServicio.cs:206-241 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L206-L241), OperacionServicio.cs:270-281 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L270-L281), ConsultaCamposBDTomi.cs:35-62 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L35-L62), ConsultaCamposBDTomi.cs:87 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L87), WindowsServiceHostBase.cs:27-28 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Core/Estructura/WindowsServiceHostBase.cs#L27-L28) (dependency context)
Code block (csharp)
Task<int> LeeOperacionesCMS();                                   // eleven Lee* contracts and RegistraFolios: no token
public async Task<bool> BEsDiaHabil(CancellationToken ct) => await EsDiaHabil();   // ConsultaCamposBDTomi.cs:87: token dropped
await _IparametrosOperaciones.MarcarServicioEnFinalizacion(_configuracion.NombreServicio, ct);   // OperacionServicio.cs:275
Twelve data-access contracts (eleven Lee* methods and RegistraFolios) have no CancellationToken, and OperacionServicio calls them without one. ConsultaCamposBDTomi accepts ct but does not forward it in ObtencionFechaSistemaAsync, ParametrosSistemaAsync, or BEsDiaHabil, although the shared base provides token overloads for EjecutarStoredProcedureQuery (RepositorioMaestro.cs:248-251 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L248-L251)) and EsDiaHabil (RepositorioMaestro.cs:1245 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L1245)); only EjecutarStoredProceduredNonQueryConResultado lacks one. A stop requested during a tick therefore cannot interrupt the read phase: the host waits at most 60 seconds, logs a timeout warning, and continues shutdown (including disposing the container) while the remaining queries of the cycle (up to 29, each with the default 30-second command timeout) may still be running. The per-tick finalization at line 275 also reuses the same ct; when that token is already cancelled, ActualizaTerminacion throws at its first statement (RepositorioMaestro.cs:1197 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L1197)), the handler at lines 277-280 logs a misleading error, and the persisted state is repaired only later by the host's shutdown path.
Recommended change: add CancellationToken ct to every reader contract and forward it to EjecutarStoredProcedureQuery, EsDiaHabil, and the other token-aware shared calls; request a token overload of EjecutarStoredProceduredNonQueryConResultado in MUFG_T24; finalize with a short independent token.
Code block (csharp)
Task<int> LeeOperacionesCMS(CancellationToken ct);
 
using (var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
{
    await _IparametrosOperaciones.MarcarServicioEnFinalizacion(_configuracion.NombreServicio, cleanup.Token);
}
Benefit: shutdown stops new reads promptly, the stop path no longer races live queries, and finalization is no longer logged as a failure on every stop that lands mid-tick.
Suggested JIRA: ENT-FR-4 (Major, rewrite_recommended).
ENT-FR-5 - Stop using the per-tick configuration clone as cross-tick state (Minor)
Category: rewrite_recommended Priority: Minor
Evidence: ServiceHost.cs:88-96 (../../General/Entradas/Entradas/ServiceHost.cs#L88-L96), Configuracion.cs:113 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Core/Modelos/Configuracion/Configuracion.cs#L113) (dependency context), ConfiguracionServicio.cs:15-18 (../../General/Entradas/Aplicacion/Modelos/ConfiguracionServicio.cs#L15-L18), OperacionServicio.cs:166-200 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L166-L200), OperacionServicio.cs:211-252 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L211-L252), EntradasUT.cs:64-70 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L64-L70), EntradasUT.cs:112-113 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L112-L113)
Code block (csharp)
_containerDI.Register<ConfiguracionServicio>(() => (ConfiguracionServicio)_configuracionBase.Clone(), Lifestyle.Scoped);   // ServiceHost.cs:96
_containerDI.Register<EstadoHorarioOperacion>(Lifestyle.Singleton);                                                       // ServiceHost.cs:89, never consumed
ConfiguracionServicio is registered as a scoped MemberwiseClone of the base object, so every tick starts from the base defaults (BHorarioLectura = false, BenHorario = false) and discards its changes. The window-transition code assumes those flags survive between ticks: "Inicio horario" is logged on every in-range tick, and the "Termina horario" branches at lines 177-182 and 246-251 are unreachable. A tick in the fully active state writes up to ten INFO lines (date, six parameter lines, and three schedule lines at lines 78, 99-124, 156, 172, and 215), plus four AVISO|No hay detalle ... lines when nothing is pending (OperacionRepositorioCms.cs:271 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L271), 298 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L298), 361 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L361), 393 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L393)); at the host's 10-second fallback interval (the real value comes from configuration outside the repository) that is on the order of 10^5 lines per day. The out-of-hours alert likewise fires on every tick. EstadoHorarioOperacion, the shared cross-tick state holder, is registered but never used. The unit tests build one ConfiguracionServicio and assert on its flags, which hides the difference between test and production lifetimes.
Recommended change: keep window state in a singleton (the registered EstadoHorarioOperacion, or a small injected state object), log and alert only on transitions, and load parameters at Debug level unless they change.
Code block (csharp)
if (inReadingWindow != _estado.EnHorarioOperacionLectura)
{
    _estado.EnHorarioOperacionLectura = inReadingWindow;
    Config.EscribirLog("INFO|Ejecuta_Procedimientos|Cambio de horario de lectura: " + (inReadingWindow ? "inicia" : "termina"), NombreServicio.Entradas.ToString());
}
Benefit: logs and alerts describe real transitions, per-tick noise drops by an order of magnitude, and tests exercise the same lifetime as production.
Suggested JIRA: ENT-FR-5 (Minor, rewrite_recommended; confirms GEN-10 for the alert-on-every-tick behavior).
Nice to Rewrite if Time Permits
ENT-FR-6 - Remove misleading names, log text, dead code, and stale documentation (Minor)
Category: NiceToRewrite_ifTimePermits Priority: Minor
Evidence: OperacionServicio.cs:156 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L156), OperacionServicio.cs:159-160 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L159-L160), OperacionServicio.cs:194 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L194), OperacionServicio.cs:279 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L279), ConfiguracionServicio.cs:10-20 (../../General/Entradas/Aplicacion/Modelos/ConfiguracionServicio.cs#L10-L20), ConstantesServicio.cs:11 (../../General/Entradas/Aplicacion/Enums/ConstantesServicio.cs#L11), ServiceHost.cs:94 (../../General/Entradas/Entradas/ServiceHost.cs#L94), OperacionRepositorioTomi.cs:43 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L43), ConsultaCamposBDTomi.cs:65-81 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L65-L81), OperacionRepositorioCms.cs:36 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L36), OperacionesGenerales.cs:143 (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L143)
None of these is a runtime failure on its own, but each misleads the next maintainer or operator:
• Log text: line 156 renders iniciohorario and Terminahorario (missing space); the same "Inicio/Termina horario de operaciones" text is used for the reading window (lines 172 and 180) and the operations window (lines 215 and 249); line 279 says maracar; the out-of-hours alert (lines 193-196) is described as "Error en ejecuta_procedimientos"; the PaP_TEF idle message names PaP_SPID (OperacionRepositorioCms.cs:393 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L393)); and the unknown-return-type message prints the description instead of the type (OperacionesGenerales.cs:143 (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L143)).
• Names: StrFechaSistema holds a time of day, four Str* fields are TimeSpan, StrNombreSPEI1/2 hold enable flags, BenHorario is bool? but never null, fechaServicio is a public camelCase string, ParametroModelo sits in namespace Aplicacion instead of Aplicacion.DTOs, and OperacionServicio.cs lives at the project root in Aplicacion.Servicios beside an empty Servicios\ folder entry (AplicacionEntradas.csproj:76 (../../General/Entradas/Aplicacion/AplicacionEntradas.csproj#L76)).
• Dead code: the unused constant sp_Es_Dia_Habil, the unused var parametrizacion = new Parametrizacion();, the unconsumed EstadoHorarioOperacion registration, two separate scoped clones (Configuracion and ConfiguracionServicio) in one scope (ServiceHost.cs:88 (../../General/Entradas/Entradas/ServiceHost.cs#L88), ServiceHost.cs:96 (../../General/Entradas/Entradas/ServiceHost.cs#L96)), and two Process.GetCurrentProcess() calls per tick that are never disposed although Pid and NombreProceso are validated at startup.
• Stale comments and documentation: 21 Prefijo X para evitar error por llave duplicada comments although the keys use TYPE- prefixes; five "falta crear" comments for procedures that exist (OperacionRepositorioTomi.cs:43 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L43), 73 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L73), 103 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L103), 133 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L133), 274 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L274)); // <summary> with two slashes and a finalization summary that says "lo marca como en uso" (ConsultaCamposBDTomi.cs:65-81 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L65-L81)); constructor documentation for non-existent proveedor and nombreBasedatos parameters in six repositories; and a CMS reader documented as using the TOMI database although it connects to CMS (OperacionRepositorioCms.cs:29-36 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L29-L36)).
Recommended change: correct the text and names in one cleanup pass, delete the dead members, and take Pid/NombreProceso from the validated configuration instead of re-querying the process.
Code block (csharp)
Config.EscribirLog($"INFO|Ejecuta_Procedimientos|{(enHorario ? "Inicio" : "Termina")} horario de operaciones, HoraSistema='{hora:hh\\:mm}'.", ...);
int pid = _configuracion.Pid.GetValueOrDefault();
string nombreProceso = _configuracion.NombreProceso;
Benefit: operators read accurate messages, maintainers stop chasing dead members, and the per-tick process-information lookup disappears.
Suggested JIRA: ENT-FR-6 (Minor, NiceToRewrite_ifTimePermits).
4.2 InfraestructuraEntradas (Tier 1)
MustChange / Rewrite
ENT-FR-7 - Detect failed queries and failed registration instead of treating them as an empty result (Blocker)
Category: MustChange/rewrite Priority: Blocker
Evidence: OperacionesGenerales.cs:85-172 (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L85-L172), ConsultaCamposBDTomi.cs:35-62 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L35-L62), OperacionRepositorioCms.cs:46-420 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L46-L420), OperacionRepositorioTomi.cs:47-294 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L47-L294), OperacionRepositorioSpei.cs:46 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei.cs#L46), OperacionRepositorioSpei2.cs:46 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei2.cs#L46), OperacionRepositorioSpid.cs:41 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpid.cs#L41), OperacionRepositorioTef.cs:46 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTef.cs#L46), RepositorioMaestro.cs:385-390 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L385-L390) and RepositorioMaestro.cs:988-993 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L988-L993) (dependency context), registration procedure CATCH (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/020%20SP%20usp_RegistrarEntradasPlanTrabajoENTRADAS.sql#L367-L398) (dependency context)
Code block (csharp)
var spEjecucion = await EjecutarStoredProcedureQuery<BmxOrd>(ConstantesServicio.sp_OpClaveBmxOrd);   // OperacionRepositorioTomi.cs:47
foreach (var respSPQuery in spEjecucion?.Data ?? new List<BmxOrd>())                                    // only .Data is read, never .Exito
 
return registrarFoliosRespuesta?.Data?.Count ?? 0;                                                      // OperacionesGenerales.cs:172
Code block (sql)
        --THROW;     -- usp_RegistrarEntradasPlanTrabajoENTRADAS, CATCH block, line 397
The shared repository turns every SqlException (timeout, deadlock victim, permission error, missing object, login failure) into Exito = false with an empty Data list, without logging and without throwing. The registration call additionally swallows every other exception type, whereas errors that a procedure reports through a MensajeError column or output parameter, and non-SQL exceptions raised by the query overload, do propagate. Entradas reads only .Data at all 29 query sites and at the registration call, and no code path reads Exito, MensajeError, or Exception (scan: zero). A failed reader is therefore indistinguishable from "nothing pending", and RegistraFolios returns 0 both when there is nothing to register and when registration failed. The registration procedure compounds this: its CATCH rolls back, writes to Auditoria.ErrorLog, and leaves the re-raise commented out, so the caller receives no result set at all. The entire inbound pipeline (SPEI, SPID, TEF, FOREX, GPH, local, Calypso, and CMS formats) can stop producing work-plan entries with no log line and no alert; the only trace is a row in Auditoria.ErrorLog. The existing SP audit proposes only uncommenting THROW (DB-1); that alone is insufficient, because the shared layer would still convert the re-raised SqlException into Exito = false. Both sides must change. The only failure visible today is that of the first call of a tick (ActualizaEJecucion throws and the host logs it, ENT-FR-2).
Recommended change: check the result of every call and fail loudly with the procedure name and message, then re-enable the procedure's THROW.
Code block (csharp)
private static List<T> LeerDatos<T>(StoredProcedureResult<T> resultado, string procedimiento)
{
    if (!resultado.Exito)
    {
        throw new InvalidOperationException("Falló " + procedimiento + ": " + resultado.MensajeError, resultado.Exception);
    }
    return resultado.Data;
}
 
if (!registrarFoliosRespuesta.Exito)
{
    throw new InvalidOperationException("Falló el registro del plan de trabajo: " + registrarFoliosRespuesta.MensajeError, registrarFoliosRespuesta.Exception);
}
Benefit: database failures reach the existing log-and-alert path instead of silently stopping registration, and the service can distinguish "no work" from "cannot work".
Suggested JIRA: ENT-FR-7 (Blocker, MustChange/rewrite; confirms GEN-11; pair with the SQL-project change that restores THROW;).
ENT-FR-8 - Reject an empty business date instead of sending it to the SPEI, SPID, and TEF queries (Major)
Category: MustChange/rewrite Priority: Major
Evidence: ConsultaCamposBDTomi.cs:35-43 (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L35-L43), OperacionServicio.cs:76-78 (../../General/Entradas/Aplicacion/OperacionServicio.cs#L76-L78), OperacionRepositorioSpei.cs:46 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei.cs#L46), OperacionRepositorioSpei2.cs:46 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei2.cs#L46), OperacionRepositorioSpid.cs:41 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpid.cs#L41), OperacionRepositorioTef.cs:46 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTef.cs#L46), usp_ObtenerFechaSistema (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/002%20SP%20usp_ObtenerFechaSistema.sql#L27-L34), sp_InterfaceTOMI_SPEI_Estatus_Recibidas (../../StoredProcedures/MWD_DB/DBMDW/General/Lectura/Procedures/Operaciones/ENT_SPEI.sql#L310-L341), Parametrizacion.cs:131-146 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Aplicacion/Control/Parametrizacion.cs#L131-L146) (dependency context)
Code block (csharp)
return resultado?.Data?
    .FirstOrDefault()?
    .fecha
    .ToString("yyyyMMdd")
    ?? string.Empty;                                                                        // ConsultaCamposBDTomi.cs:38-42
 
cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", _configuracion.fechaServicio));   // SPEI-1, SPEI-2, SPID, TEF readers
usp_ObtenerFechaSistema returns zero rows whenever no day matches cerrado = 0 AND nextday = 1 AND openday = 1 (a plausible state around day rollover), and the method also returns "" after any failed query (ENT-FR-7). The only visible effect is the log line INFO|Fecha:  para ejecucion .... The four payment-rail readers then pass "" as @op_fecha_oper, which is NUMERIC(8,0) in the SPEI procedure, where an empty string converts to 0 and matches no row: inbound SPEI payments are silently not registered for the whole window, and the SPID and TEF procedures (whose definitions are not in the SQL project) receive the same empty value. A non-numeric value would raise a SqlException that ENT-FR-7 shows is swallowed. FirstOrDefault() without ordering also picks an arbitrary row if more than one day is open, and ToString("yyyyMMdd") is not culture-explicit. The injected IParametrizacion already offers ObtenerFechaSistemaValidadaAsync(ct), which throws when no row exists, but it is used only for ObtenerHoraSistema().
Recommended change: obtain the date through the shared validated method (or throw locally), validate the yyyyMMdd shape, and delete the silent local fallback.
Code block (csharp)
_configuracion.fechaServicio = await _iParametrizacion.ObtenerFechaSistemaValidadaAsync(ct);
if (!DateTime.TryParseExact(_configuracion.fechaServicio, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
{
    throw new InvalidOperationException("Fecha de sistema invalida: '" + _configuracion.fechaServicio + "'.");
}
Benefit: a closed or missing business date is reported as an error, not as an empty payment queue, and ENT-FR-2 ensures it is alerted.
Suggested JIRA: ENT-FR-8 (Major, MustChange/rewrite).
ENT-FR-9 - Stop one malformed source row from aborting the whole batch (Major)
Category: MustChange/rewrite Priority: Major
Evidence: OperacionRepositorioTomi.cs:110-111 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L110-L111), OperacionRepositorioTef.cs:49-50 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTef.cs#L49-L50), OperacionRepositorioTomi.cs:276-289 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L276-L289), OperacionesGenerales.cs:42-81 (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L42-L81), OperacionesGenerales.cs:165-170 (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L165-L170), BmxOrd.cs:9 (../../General/Entradas/Aplicacion/DTOs/BmxOrd.cs#L9), registration procedure row validation (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/020%20SP%20usp_RegistrarEntradasPlanTrabajoENTRADAS.sql#L85-L130) (dependency context)
Code block (csharp)
string strAutorizacion = respSPQuery.id_autorizacion;                                              // Tomi.cs:110 (database string, may be null)
... FolioMaestro = strAutorizacion.ToString(), FolioDetalle = strAutorizacion.ToString() ...      // Tomi.cs:111
 
int intOpClave = checked((int)respSPQuery.IDOperacion);                                            // Tef.cs:49 (Int64 narrowed to int)
 
if (string.IsNullOrEmpty(tipoOperacion)) throw new InvalidProgramException("Entrada sin tipo operacion");   // OperacionesGenerales.cs:69
Each reader builds one dictionary per source and RegistraFolios submits it as one unit, so a single bad row makes the whole source fail on every tick until the data is repaired:
• FOREX: id_autorizacion is a database string; a NULL throws NullReferenceException at strAutorizacion.ToString(), and an empty string reaches RegistraFolios, which throws for the whole dictionary.
• TEF: checked((int)IDOperacion) narrows an Int64 that every other reader and the procedure treat as long/BIGINT; once an identifier exceeds 2,147,483,647 the OverflowException aborts all TEF registrations.
• Money market: tipo_operacion is taken from the data and used unvalidated as key, TipoOperacion, and description. A null or empty value throws InvalidProgramException in RegistraFolios (line 69); more than ten characters violates the DataTable MaxLength set at OperacionesGenerales.cs line 52 when the row is added; a type missing from the catalog makes the procedure return an ERROR row, after which RegistraFolios throws (lines 165-170) on every tick. The allow-list gate at OperacionRepositorioTomi.cs line 276 only checks that some entry starts with MD_, so the returned type is never checked against the list.
• Registrar: RegistraFolios pre-validates and throws at the first bad entry with a message that names neither the key nor the folio, although the procedure already validates folio and catalog per row and returns per-row ERROR results. InvalidProgramException is a CLR-integrity exception, not a data-validation exception.
• Identifiers: BmxOrd.op_clave is a decimal converted with Convert.ToInt64, which rounds and can overflow, in the COAS, SPEI-1, SPEI-2, and SPID readers.
Recommended change: validate each row where it is read, skip and log it with source, key, and reason, register the valid rows, and let RegistraFolios throw only for contract violations that affect the whole batch. Keep identifiers as long.
Code block (csharp)
long idOperacion = respSPQuery.IDOperacion;
if (string.IsNullOrWhiteSpace(strAutorizacion))
{
    rechazados.Add("MFX: id_autorizacion vacio");   // logged with source and reason; the batch continues
    continue;
}
Benefit: a single bad row degrades one operation instead of blocking a whole payment source, and each rejection is attributable.
Suggested JIRA: ENT-FR-9 (Major, MustChange/rewrite).
ENT-FR-10 - Gate the SPEI-2 reader on ENT_SPEI_2, not ENT_SPEI (Major)
Category: MustChange/rewrite Priority: Major
Evidence: OperacionRepositorioSpei2.cs:44 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei2.cs#L44), OperacionRepositorioSpei2.cs:53-59 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei2.cs#L53-L59), OperacionRepositorioSpei.cs:44 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei.cs#L44)
Code block (csharp)
if (_configuracion.OperacionesPermitidas.Contains("ENT_SPEI") || _configuracion.OperacionesPermitidas.Length == 0)   // Spei2.cs:44
...
"ENT_SPEI_2-" + opClave, ... TipoOperacion = "ENT_SPEI_2",                                                          // Spei2.cs:53-59
With a non-empty allow-list, the SPEI-2 reader is gated by the SPEI-1 key. A node configured with ENT_SPEI_2 but not ENT_SPEI never reads SPEI-2, and a node configured with ENT_SPEI only reads and registers ENT_SPEI_2 operations that are not on its allow-list. Every other reader gates on the exact type it registers (ENT_SPID, ENT_TEF, CNT, and so on). The two SPEI files are near copies that differ only in database, key, and text, so this reads as a copy-paste miss. Confirm the intended node partitioning with the service owners; if both SPEI servers are meant to share one switch, document it and stop registering a separate ENT_SPEI_2 type.
Recommended change: gate on the registered type.
Code block (csharp)
if (_configuracion.OperacionesPermitidas.Contains("ENT_SPEI_2") || _configuracion.OperacionesPermitidas.Length == 0)
Benefit: the allow-list partitions SPEI-1 and SPEI-2 as configured, so no node silently skips or over-reads a SPEI server.
Suggested JIRA: ENT-FR-10 (Major, MustChange/rewrite).
ENT-FR-11 - Call the MFX status procedure with its declared parameter and verify the outcome (Major)
Category: MustChange/rewrite Priority: Major
Evidence: OperacionRepositorioCms.cs:122-128 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L122-L128), MUFG_MX2_ActualizaEstatusFormatoENTRADAS (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/Bot/Procedures/001%20SP%20MUFG_MX2_ActualizaEstatusFormatoENTRADAS.sql#L29-L48) (dependency context), RepositorioMaestro.cs:864 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L864) (dependency context)
Code block (csharp)
await EjecutarStoredProcedureQuery<Formato>(ConstantesServicio.sp_Update_MFX, cmd => cmd.Parameters.Add(basedatosConexion.CrearParametro("@intOpClave", intOpClave)));
Config.EscribirLog("INFO | lee_operaciones_CMS | A marcar la operacion MFX como en proceso, folio: " + intOpClave + ".", ...);
Code block (sql)
CREATE PROCEDURE dbo.MUFG_MX2_ActualizaEstatusFormatoENTRADAS
(
@idFormato BIGINT,
@mensaje NVARCHAR(250) = N'Formato en proceso',
@estatus CHAR(4) = 'APFT'
The C# passes @intOpClave, a parameter name that no procedure in the SQL project declares (a search of that project finds no declaration), while the current Entradas procedure declares the required @idFormato with no default. SQL Server rejects such a call with a parameter error; the shared layer converts that SqlException into Exito = false without logging (ENT-FR-7), and the loop then writes "A marcar la operacion MFX como en proceso" regardless. Unless the deployed procedure differs from the SQL project, MFX formats (tipoFormato 12) never leave ENBT, are re-selected on every tick, and fail again silently each time. Of the parameter names that could be checked against SQL definitions (@op_fecha_oper in the SPEI procedure, @origen and @folios in the registration procedure), this is the only mismatch. Secondary points: the update is issued once per MFX row inside the read loop (N+1), it is the only write among the 29 query sites yet uses the query helper with a Formato result type, and the present-tense log text cannot distinguish an attempt from a success.
Recommended change: pass the declared parameter, use the non-query helper with a token, check Exito, and report failures after the other families have been registered. Add a contract test that fails when a C# parameter name has no counterpart in the SQL definition.
Code block (csharp)
StoredProcedureResult actualizado = await EjecutarStoredProceduredNonQuery(ConstantesServicio.sp_Update_MFX,
    cmd => cmd.Parameters.Add(basedatosConexion.CrearParametro("@idFormato", intOpClave)), ct);
if (!actualizado.Exito)
{
    fallosMfx.Add(intOpClave + ": " + actualizado.MensajeError);   // raised after registration of the other families
}
Benefit: MFX formats advance as designed, a failed status change is reported with its cause, and the log states what actually happened.
Suggested JIRA: ENT-FR-11 (Major, MustChange/rewrite; confirm the deployed procedure signature before the change).
Rewrite Recommended
ENT-FR-12 - Replace the copy-pasted reader blocks with one table-driven registry (Major)
Category: rewrite_recommended Priority: Major
Evidence: OperacionRepositorioCms.cs:39-427 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L39-L427), OperacionRepositorioTomi.cs:39-300 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L39-L300), OperacionRepositorioSpei.cs:38-72 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei.cs#L38-L72), OperacionRepositorioSpei2.cs:38-72 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei2.cs#L38-L72), OperacionRepositorioSpid.cs:33-67 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpid.cs#L33-L67), OperacionRepositorioTef.cs:38-83 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTef.cs#L38-L83), FiltroFolios.cs:13-25 (../../General/Entradas/Infraestructura/Repositorio/FiltroFolios.cs#L13-L25)
Code block (csharp)
if (_configuracion.OperacionesPermitidas.Contains("FXD_Local") || _configuracion.OperacionesPermitidas.Length == 0)
{
    StoredProcedureResult<TombinNf> spEjecucionNffxd = await EjecutarStoredProcedureQuery<TombinNf>(ConstantesServicio.sp_NfFxdLocal);
    if ((spEjecucionNffxd?.Data?.Count ?? 0) > 0) foreach (var respSPQuery in spEjecucionNffxd.Data)
    {
        long lngNfFolio = respSPQuery.nf_folio;
        filtroFolios.AgregarFolio(dicFolios,"FXD_Local-" + lngNfFolio, new FolioOperacionDTO { /* five fields */ });
    }
}
The six repositories repeat one pattern: gate by allow-list, run a parameterless (or date-parameterised) procedure, loop, build a FolioOperacionDTO, and register. The scan counted 36 allow-list expressions, 33 DTO constructions, 29 query calls, 11 identical log-and-rethrow catch blocks, 15 distinct magic tipoFormato numbers (3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 23, 24, 30, and 31), and 21 stale Prefijo comments; the CMS and TOMI repositories (735 lines) are almost entirely this pattern. The duplication multiplies the defects above: the missing Exito check (ENT-FR-7), the dropped token (ENT-FR-4), and the unvalidated rows (ENT-FR-9) each recur at about thirty sites, and the wrong gate in one copy (ENT-FR-10) is exactly the drift that copy-paste invites. Two further design risks sit in the same code: an empty OperacionesPermitidas silently enables every family on that node (a missing or mistyped host attribute activates all readers, and the effective list is never logged), and the six switch (tipoFormato) blocks have no default, so a new format type returned by a changed procedure would be ignored without a message (today the MFX procedure's IN list matches the cases exactly). FiltroFolios.AgregarFolio also drops a duplicate key with only an AVISO line, and for the grouped families (NOM_FXD and the PaP types) the key TYPE-id_formato ignores the master/detail discriminator Grupo that the procedures return, so confirm that a master identifier and a detail identifier can never coincide.
Recommended change: describe each source as data and run one shared loop that centralizes the allow-list, the Exito check, token forwarding, row validation, de-duplication, and logging; expose a single null-safe IsAllowed(type) on the configuration model and log the effective allow-list at startup.
Code block (csharp)
private static readonly FuenteEntrada[] Fuentes =
{
    new FuenteEntrada("FXD_Local", ConstantesServicio.sp_NfFxdLocal, "Local Doméstica"),
    new FuenteEntrada("FXD_GPH",   ConstantesServicio.sp_NfFxdGph,   "GPH SPEI"),
    // ...
};
 
foreach (FuenteEntrada fuente in Fuentes.Where(f => _configuracion.EstaPermitida(f.TipoOperacion)))
{
    foreach (long folio in LeerFolios(await EjecutarStoredProcedureQuery<TombinNf>(fuente.Procedimiento, null, ct), fuente))
    {
        dicFolios.Agregar(fuente, folio);
    }
}
Benefit: about 1,000 lines of repository code, most of it duplicated, become a data table plus one loop, adding a family becomes a data change, and the fixes for ENT-FR-4, 7, 9, and 10 are made once.
Suggested JIRA: ENT-FR-12 (Major, rewrite_recommended).
ENT-FR-13 - Confirm whether the four prepared H2H entry procedures are intentionally unused (Major, needs owner confirmation)
Category: rewrite_recommended Priority: Major
Evidence: usp_ObtenerNFFxdH2HENTRADAS (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/005%20SP%20usp_ObtenerNFFxdH2HENTRADAS.sql#L27-L34), usp_ObtenerNFFxiH2HENTRADAS (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/008%20SP%20usp_ObtenerNFFxiH2HENTRADAS.sql#L27-L34), usp_ObtenerNFSpidH2HENTRADAS (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/013%20SP%20usp_ObtenerNFSpidH2HENTRADAS.sql#L27-L34), usp_ObtenerNFTEFH2HENTRADAS (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/016%20SP%20usp_ObtenerNFTEFH2HENTRADAS.sql#L27-L34), ConstantesServicio.cs:9-52 (../../General/Entradas/Aplicacion/Enums/ConstantesServicio.cs#L9-L52), OperacionRepositorioTomi.cs:129-192 (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L129-L192), FXDH2H.cs:27 (../../General/Lectura/Aplicacion/Servicios/Operaciones/FXDH2H.cs#L27)
The SQL project defines one H2H registration reader per family beside the GPH and Local readers that Entradas does consume (usp_ObtenerNFFxdH2HENTRADAS, ...FxiH2H..., ...SpidH2H..., and ...TEFH2H..., each selecting nf_folio from tb_tombin_nf_*_h2h for pending statuses). Entradas has no constant, reader, or allow-list entry for any of them, and the only procedure in the SQL project that inserts into the work-plan master table is the Entradas registration procedure. The downstream Lectura solution has processors for FXD_H2H, FXI_H2H, SPID_H2H, and TEF_H2H (and for FXT_H2H and TEF_Local, for which no Entradas procedure or reader exists either). If H2H operations are expected to be registered by this service, they would never receive a work plan and this becomes a functional defect. If another component registers them, the four procedures are dead objects.
Recommended change: confirm ownership of H2H work-plan registration with the service owners. If Entradas owns it, add the four sources to the registry from ENT-FR-12 (and decide on FXT_H2H and TEF_Local); otherwise remove or annotate the unused procedures.
Benefit: removes an unverified coverage gap before cut-over, or removes dead database objects.
Suggested JIRA: ENT-FR-13 (Major, rewrite_recommended; becomes MustChange if H2H registration is required here).
4.3 Entradas host (Tier 2)
MustChange / Rewrite
ENT-FR-14 - Check the startup and shutdown state-persistence results before logging success (Major)
Category: MustChange/rewrite Priority: Major
Evidence: ServiceHost.cs:151 (../../General/Entradas/Entradas/ServiceHost.cs#L151), ServiceHost.cs:184-224 (../../General/Entradas/Entradas/ServiceHost.cs#L184-L224), ControlServicio.cs:50-62 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Aplicacion/Control/ControlServicio.cs#L50-L62) (dependency context), RepositorioMaestro.cs:1193-1228 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L1193-L1228) (dependency context)
Code block (csharp)
await _controlServicio.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, ...);                                       // ServiceHost.cs:151
await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio, cleanupCt);         // :204
await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio, cleanupCt);                             // :207
Config.EscribirLog("INFO|OnStop|Terminación exitosa de bases de datos.");                                              // :209
MarcarPuertoEnUsoAsync returns false, without throwing, when the stored-procedure result is unsuccessful or the call is cancelled (unexpected exceptions still propagate), and ActualizaTerminacion returns an unsuccessful StoredProcedureResult without throwing. Startup and shutdown discard both outcomes; shutdown then logs "Terminación exitosa de bases de datos" unconditionally, and the catch (OperationCanceledException) at lines 212-215 swallows an expired cleanup token without any message. The service can therefore keep running with its PID and port unregistered, or stop with its persisted port and execution state still marked active while the log reports success. Monitoring and overlap control rely on that persisted state (standing finding GEN-9).
Recommended change: inspect each result, fail startup when registration fails, and log the real shutdown outcome (including an expired cleanup token).
Code block (csharp)
if (!await _controlServicio.MarcarPuertoEnUsoAsync(pid, nombreProceso, nombreDisplay, nombreServicio, ct))
{
    throw new InvalidOperationException("No fue posible registrar el PID y puerto del servicio.");
}
 
bool puertoLiberado = await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, nombreServicio, cleanupCt);
StoredProcedureResult terminacion = await _controlServicio.ActualizaTerminacion(nombreServicio, cleanupCt);
Config.EscribirLog(puertoLiberado && terminacion.Exito
    ? "INFO|OnStop|Terminación exitosa de bases de datos."
    : "ERROR|OnStop|La terminación no se persistió: " + terminacion.MensajeError);
Benefit: persisted run state is trustworthy, and a failed start or stop is visible instead of being reported as clean.
Suggested JIRA: ENT-FR-14 (Major, MustChange/rewrite; confirms GEN-9).
ENT-FR-15 - Replace the machine-specific D:\ServiceCMS references with versioned packages (Blocker)
Category: MustChange/rewrite Priority: Blocker
Evidence: AplicacionEntradas.csproj:36-39 (../../General/Entradas/Aplicacion/AplicacionEntradas.csproj#L36-L39), InfraestructuraEntradas.csproj:36-39 (../../General/Entradas/Infraestructura/InfraestructuraEntradas.csproj#L36-L39), Entradas.csproj:41-48 (../../General/Entradas/Entradas/Entradas.csproj#L41-L48), PruebasUnitarias.csproj:50-53 (../../General/Entradas/PruebasUnitarias/PruebasUnitarias.csproj#L50-L53)
Code block (xml)
<Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
  <SpecificVersion>False</SpecificVersion>
  <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
</Reference>
All four projects bind MUFG_T24.dll by an absolute path on drive D: with SpecificVersion disabled, and the host binds SimpleInjector.dll the same way; the solution has no package declaration or feed configuration for either. The path is absent on this machine, as is the packages directory, so a clean agent cannot build the solution, and with SpecificVersion=False any file at that path binds silently, so nothing records which shared-library build was compiled, tested, or deployed. MUFG_T24.dll is the dependency whose behavior ENT-FR-7, 8, 14, and the lifecycle findings hinge on. This confirms standing finding GEN-1.
Recommended change: publish MUFG_T24 as a versioned package on the approved internal feed, reference the same pinned version from all four projects, consume SimpleInjector from the public feed at a pinned version, and until then fail the build explicitly when the file is missing.
Code block (xml)
<PackageReference Include="MUFG.T24" Version="x.y.z" />
<PackageReference Include="SimpleInjector" Version="5.x.y" />
Benefit: deterministic builds on any agent, traceability of the shared-library version, and one reviewed upgrade path for production and tests.
Suggested JIRA: ENT-FR-15 (Blocker, MustChange/rewrite; confirms GEN-1).
4.4 PruebasUnitarias (Tier 2)
MustChange / Rewrite
ENT-FR-16 - Test failure, validation, cancellation, and repository behavior, not only three happy-path scenarios (Major)
Category: MustChange/rewrite Priority: Major
Evidence: EntradasUT.cs:40 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L40), EntradasUT.cs:43-85 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L43-L85), EntradasUT.cs:87-155 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L87-L155), EntradasUT.cs:283-345 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L283-L345)
Code block (csharp)
private readonly CancellationToken ct = new CancellationToken();                       // EntradasUT.cs:40, never cancelled
await _servicio.EjecutaProcedimientosAsync(ct);                                         // return value and failures never asserted
Assert.IsTrue(_configuracion.BHorarioLectura);                                          // :112 asserts on one shared config instance
The suite is a single file with three methods and 24 assertions, all on OperacionServicio through hand-written fakes that return Task.FromResult(1) for every reader. It never supplies a missing, empty, or malformed parameter (the dead guards of ENT-FR-1 are therefore never exercised; a test expecting the documented exception would fail), a reader that throws or faults, a failing alert or finalization, a cancelled token, a disabled SPEI flag, or an allow-list. FechaSistemaResultado is the time "12:00" rather than a yyyyMMdd date (EntradasUT.cs:293-294 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L293-L294)), so the date contract is never exercised either. None of the ten infrastructure classes is tested: the registrar, the 29 query sites, the allow-list gates, the key and DTO construction, and the Exito handling are exactly the logic that decides whether an operation is registered at all. Neither is ServiceHost. None of ENT-FR-1, 2, 3, 7, 8, 9, 10, or 11 would be detected by the current suite.
Recommended change: add focused tests for each contract and give the repositories a seam so they can be tested without a database (for example an injectable procedure executor, or an overridable protected method in a test subclass).
Code block (csharp)
[TestMethod]
public async Task EjecutaProcedimientos_CuandoFaltaParametroRequerido_AlertaYFinaliza()
{
    _parametrosOperaciones.ParametrosSistemaResultado.Remove("HORA_LECTURA_INICIO");
 
    bool resultado = await _servicio.EjecutaProcedimientosAsync(ct);
 
    Assert.IsFalse(resultado);
    Assert.AreEqual(1, _parametrosOperaciones.EnviaAlertaCount);
    Assert.AreEqual(1, _parametrosOperaciones.MarcarServicioEnFinalizacionCount);
}
Cover at minimum: missing and malformed parameters; one reader faulting while the others still run; cancellation; disabled SPEI flags; allow-list gating including SPEI-2; registrar results with INFO, ERROR, empty, and Exito = false; the empty-date case; and a contract test that compares every C# procedure and parameter name with the SQL project (it would have caught @intOpClave).
Benefit: the failure, validation, and registration behavior that matters operationally is protected against regression, and the fixes above can be made safely.
Suggested JIRA: ENT-FR-16 (Major, MustChange/rewrite).
Nice to Rewrite if Time Permits
ENT-FR-17 - Replace the 22-stub IParametrizacion fake and correct the date fixture (Minor)
Category: NiceToRewrite_ifTimePermits Priority: Minor
Evidence: EntradasUT.cs:157-281 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L157-L281), EntradasUT.cs:293-294 (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L293-L294), IParametrizacion.cs:14-31 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Core/Interfaces/Parametrizacion/IParametrizacion.cs#L14-L31) (dependency context)
ParametrizacionFake implements the shared interface with 22 NotImplementedException stubs, of which eight are not members of the current interface (for example the parameterless ObtenerFechaSistemaAsync() and ObtenerFechaSistemaJulianaAsync(), ObtenerParametrosSistemaAsync(string), and the token-less ObtenerProcesoActualAsync and ObtenerPuertos overloads), apparently left over from earlier interface revisions. Every member added to the shared interface will break compilation of this test project, and the stale stubs hide how little of the interface the test needs: only ObtenerHoraSistema().
Recommended change: use a mocking library or a small shared test double that implements only what the class under test calls, and use a realistic yyyyMMdd business date.
Code block (csharp)
var parametrizacion = new Mock<IParametrizacion>();
parametrizacion.Setup(p => p.ObtenerHoraSistema()).Returns(TimeSpan.Parse("12:00"));
Benefit: the fixture shrinks to the behavior under test and stops breaking on unrelated shared-interface changes.
Suggested JIRA: ENT-FR-17 (Minor, NiceToRewrite_ifTimePermits).
4.5 Repository delivery pipeline (all projects)
Rewrite Recommended
ENT-FR-18 - Replace the echo-only CI pipeline and the template README (Major)
Category: rewrite_recommended Priority: Major
Evidence: .gitlab-ci.yml:19-49 (../../General/Entradas/.gitlab-ci.yml#L19-L49), README.md:1-93 (../../General/Entradas/README.md#L1-L93)
Code block (yaml)
build-job:
  stage: build
  script:
    - echo "Compiling the code..."
    - echo "Compile complete."
...
deploy-job:
  stage: deploy
  environment: stage
  script:
    - echo "Deploying application..."
    - echo "Application successfully deployed."
The pipeline is the unmodified GitLab "Getting-Started" template: the build job only echoes, the test stage is commented out, and the deploy job only echoes. Every push reports a successful build and deployment without compiling, testing, scanning, or deploying anything, so the review checklist's "pipeline green" gate cannot detect a broken change. The README is also the unmodified GitLab template; it does not describe the service, its parameters, the meaning of OperacionesPermitidas (including that an empty list enables every family), its dependencies, or how to build, test, and run it, and its clone URL differs from the configured origin. Because the projects need the absent D:\ServiceCMS dependency, a real build job depends on ENT-FR-15.
Recommended change: replace the template with real restore, build, test, and (optionally) scan jobs on a Windows runner, make deployment a protected manual job, and document the service in the README.
Code block (yaml)
build-job:
  stage: build
  tags: [windows]
  script:
    - nuget restore Entradas.sln
    - msbuild Entradas.sln /p:Configuration=Release /warnaserror
test-job:
  stage: test
  script:
    - vstest.console.exe PruebasUnitarias\bin\Release\PruebasUnitarias.dll
Benefit: a green pipeline means the solution built and its tests passed, and new maintainers can run and operate the service from the README.
Suggested JIRA: ENT-FR-18 (Major, rewrite_recommended).
5. Architecture-Fit Note
Entradas is a short-interval polling batch: a timer tick (interval from host configuration, with a 10-second fallback) reads about thirty procedures across six databases, builds a keyed set of pending operations, and registers the new ones in the TOMI work plan through one idempotent procedure. All state between ticks lives in the database, and the shared host guarantees non-overlapping ticks. Applying the standing architecture discussion in _ORCHESTRATION.md (./_ORCHESTRATION.md) section 6, the best near-term fit is modernize in place: first make failures visible (ENT-FR-7, 11, 2, 3, and 14), then correct configuration, date, and gating behavior (ENT-FR-1, 8, and 10), then replace the duplication with the registry (ENT-FR-12) and add the missing tests (ENT-FR-16) before any change of hosting. A change- or message-driven registration design (for example queue-triggered or Service Broker-based instead of repeated polling) could remove polling latency and repeated re-reads, but it would move the idempotency, ordering, and OperacionesPermitidas partitioning guarantees into new infrastructure, and nothing in this review justifies a serverless or microservice split. A full rewrite is not a byproduct of this audit. These decisions require facts that are not visible here:
• the actual timer interval and the expected inbound volumes and latency objectives for SPEI, SPID, and TEF registration;
• how many Entradas instances run and how OperacionesPermitidas partitions the families among them (ENT-FR-10, ENT-FR-12);
• whether another component registers the H2H families, FXT_H2H, and TEF_Local (ENT-FR-13);
• whether the deployed MUFG_MX2_ActualizaEstatusFormatoENTRADAS signature matches the SQL project (ENT-FR-11);
• whether master and detail identifiers of the grouped families can coincide (ENT-FR-12); and
• the alerting and recovery-time expectations for a stalled registration pipeline.
6. JIRA-Ready Backlog
ID	Title	Category	Project	File(s)	Priority
ENT-FR-1	Validate required schedule and SPEI parameters; remove the dead TimeSpan == null guards	MustChange/rewrite	AplicacionEntradas	OperacionServicio.cs (../../General/Entradas/Aplicacion/OperacionServicio.cs#L92-L153), ConfiguracionServicio.cs (../../General/Entradas/Aplicacion/Modelos/ConfiguracionServicio.cs#L11-L18)	Major
ENT-FR-2	Bring the tick preamble inside the alert and finalization boundary	MustChange/rewrite	AplicacionEntradas / Entradas	OperacionServicio.cs (../../General/Entradas/Aplicacion/OperacionServicio.cs#L68-L281), ServiceHost.cs (../../General/Entradas/Entradas/ServiceHost.cs#L163-L181)	Major
ENT-FR-3	Await alert insertion and inject IAlertas	MustChange/rewrite	AplicacionEntradas / InfraestructuraEntradas	IParametrosOperaciones.cs (../../General/Entradas/Aplicacion/Interfaces/IParametrosOperaciones.cs#L19), ConsultaCamposBDTomi.cs (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L20-L75), OperacionServicio.cs (../../General/Entradas/Aplicacion/OperacionServicio.cs#L188-L267)	Major
ENT-FR-4	Propagate cancellation through readers and use an independent token for finalization	rewrite_recommended	AplicacionEntradas / InfraestructuraEntradas	OperacionServicio.cs (../../General/Entradas/Aplicacion/OperacionServicio.cs#L206-L281), ConsultaCamposBDTomi.cs (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L35-L87), IOperacionesEntradaTomi.cs (../../General/Entradas/Aplicacion/Interfaces/IOperacionesEntradaTomi.cs#L10-L20)	Major
ENT-FR-5	Keep window state outside the per-tick configuration clone; log and alert on transitions only	rewrite_recommended	AplicacionEntradas / Entradas	ServiceHost.cs (../../General/Entradas/Entradas/ServiceHost.cs#L88-L96), OperacionServicio.cs (../../General/Entradas/Aplicacion/OperacionServicio.cs#L166-L252)	Minor
ENT-FR-6	Clean misleading names, log text, dead code, and stale documentation	NiceToRewrite_ifTimePermits	All projects	OperacionServicio.cs (../../General/Entradas/Aplicacion/OperacionServicio.cs#L156-L279), ConfiguracionServicio.cs (../../General/Entradas/Aplicacion/Modelos/ConfiguracionServicio.cs#L10-L20), ServiceHost.cs (../../General/Entradas/Entradas/ServiceHost.cs#L94)	Minor
ENT-FR-7	Detect failed queries and registration; restore THROW; in the registration procedure	MustChange/rewrite	InfraestructuraEntradas	OperacionesGenerales.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L85-L172), OperacionRepositorioCms.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L46-L420), OperacionRepositorioTomi.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L47-L294)	Blocker
ENT-FR-8	Reject an empty or invalid business date instead of querying SPEI, SPID, and TEF with it	MustChange/rewrite	InfraestructuraEntradas / AplicacionEntradas	ConsultaCamposBDTomi.cs (../../General/Entradas/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L35-L43), OperacionServicio.cs (../../General/Entradas/Aplicacion/OperacionServicio.cs#L76-L78)	Major
ENT-FR-9	Skip and report malformed source rows; keep identifiers as long	MustChange/rewrite	InfraestructuraEntradas	OperacionRepositorioTomi.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L110-L111), OperacionRepositorioTef.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTef.cs#L49-L50), OperacionesGenerales.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionesGenerales.cs#L42-L81)	Major
ENT-FR-10	Gate the SPEI-2 reader on ENT_SPEI_2	MustChange/rewrite	InfraestructuraEntradas	OperacionRepositorioSpei2.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioSpei2.cs#L44)	Major
ENT-FR-11	Call the MFX status procedure with @idFormato and verify the outcome	MustChange/rewrite	InfraestructuraEntradas	OperacionRepositorioCms.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L122-L128)	Major
ENT-FR-12	Replace copy-pasted readers with a table-driven registry and a single allow-list check	rewrite_recommended	InfraestructuraEntradas	OperacionRepositorioCms.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioCms.cs#L39-L427), OperacionRepositorioTomi.cs (../../General/Entradas/Infraestructura/Repositorio/OperacionRepositorioTomi.cs#L39-L300), FiltroFolios.cs (../../General/Entradas/Infraestructura/Repositorio/FiltroFolios.cs#L13-L25)	Major
ENT-FR-13	Confirm H2H registration ownership; add readers or remove the four unused procedures	rewrite_recommended	InfraestructuraEntradas / SQL project	ConstantesServicio.cs (../../General/Entradas/Aplicacion/Enums/ConstantesServicio.cs#L9-L52), usp_ObtenerNFFxdH2HENTRADAS (../../StoredProcedures/MWD_DB/DBMDW/General/Entradas/IT24/Procedures/005%20SP%20usp_ObtenerNFFxdH2HENTRADAS.sql#L27-L34)	Major
ENT-FR-14	Check startup and shutdown state-persistence results before logging success	MustChange/rewrite	Entradas	ServiceHost.cs (../../General/Entradas/Entradas/ServiceHost.cs#L151), ServiceHost.cs (../../General/Entradas/Entradas/ServiceHost.cs#L184-L224)	Major
ENT-FR-15	Replace D:\ServiceCMS HintPaths with versioned packages	MustChange/rewrite	All projects	AplicacionEntradas.csproj (../../General/Entradas/Aplicacion/AplicacionEntradas.csproj#L36-L39), InfraestructuraEntradas.csproj (../../General/Entradas/Infraestructura/InfraestructuraEntradas.csproj#L36-L39), Entradas.csproj (../../General/Entradas/Entradas/Entradas.csproj#L41-L48), PruebasUnitarias.csproj (../../General/Entradas/PruebasUnitarias/PruebasUnitarias.csproj#L50-L53)	Blocker
ENT-FR-16	Add failure, validation, cancellation, repository, and SQL-contract tests	MustChange/rewrite	PruebasUnitarias	EntradasUT.cs (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L40-L155)	Major
ENT-FR-17	Replace the 22-stub IParametrizacion fake and correct the date fixture	NiceToRewrite_ifTimePermits	PruebasUnitarias	EntradasUT.cs (../../General/Entradas/PruebasUnitarias/EntradasUT.cs#L157-L296)	Minor
ENT-FR-18	Replace the echo-only CI pipeline and the template README	rewrite_recommended	Repository	.gitlab-ci.yml (../../General/Entradas/.gitlab-ci.yml#L19-L49), README.md (../../General/Entradas/README.md#L1-L93)	Major

