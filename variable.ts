SPID Accionistas Recommendations
SPID_Accionistas - Architecture & Code Quality Recommendations
Solution: SPID_Accionistas.sln (../../SPEI_SPID/SPID_ACCIONISTAS/SPID_Accionistas.sln). Audit date: 2026-10-14. Branch: CodeReview. Commit: b519dbf10998a8436135078144b3bccaef05e75a; initially clean.
Compared against the approved LoansApp coding-practices baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR severity checklist (../BaseLine_Docs/code_review.md). See the canonical _ORCHESTRATION.md (./_ORCHESTRATION.md), exhaustive review trail (./_SPID_Accionistas_ExhaustiveReview_Progress.md) and SHA-256/line manifest (./_SPID_Accionistas_FileManifest.csv).
Status: exhaustive source review complete; all remedies below remain open. This audit changes documentation only, not the application or shared library.
1. Solution Overview (low/no-dependency -> high dependency)
Tier	Project	Kind	Path
0	Aplicacion	Contracts, DTOs and batch orchestration library	Project (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/Aplicacion.csproj)
1	Infraestructura	TOMI/T24_VW/MINDS repository adapters	Project (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Infraestructura.csproj)
2	SPID_Accionistas	Windows Service executable	Project (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/SPID_Accionistas.csproj)
2	PruebasUnitarias	MSTest library with handwritten fakes	Project (../../SPEI_SPID/SPID_ACCIONISTAS/PruebasUnitarias/PruebasUnitarias.csproj)

What this solution actually does: a timer-driven .NET Framework 4.8/x86 Windows Service refreshes SPID-related customer, shareholder and related-party reference/compliance data. It reads candidate customers from T24 views, enriches them with MINDS shareholder/incorporation/ relationship information, and updates TOMI data and three batch completion keys. Processing is gated by configured reading/operation windows and a business-day flag. Pending CUSTOMER, SHAREHOLDER and VENDORS flows each delete existing targeted data, insert replacements and mark completion. No direct fund transfer, HTTP API or queue consumer is established by this source.
The five-edge internal graph is Aplicacion <- Infraestructura <- host/tests, with both tier-2 projects also directly referencing Aplicacion. External MUFG_T24 and SimpleInjector binaries are not additional ProjectReference nodes. See the understanding document (../architecture/SPID_Accionistas/Application_understanding_technical_document.md) and dependency ladder (../excecutive_docs/SPID_Accionistas/Base_Project_Dependency_Ladder.md).
2. Scope & Methodology Note
Every file in scope was read in full, line by line, with no representative-file sampling: 35/35 tracked text files and 3,151/3,151 physical lines, including all project/configuration/ assembly metadata and root support files. The user explicitly authorized all four small projects in this turn; they were reviewed sequentially by dependency tier.
Batch	Area	Files fully read	Physical lines
A	Aplicacion	17	823
B	Infraestructura	5	650
C	Entradas / host	6	427
D	PruebasUnitarias	4	630
E	Root support	3	621
Total	No pending source range	35	3,151

Git internals are excluded. No binary application files, untracked source, SQL project or SQL script exists in the frozen scope. Quantified scans confirmed the full read; they did not replace it. Shared MUFG_T24 source was inspected only for effective contracts and false-positive checks; no finding is located inside that shared library.
Issue Summary
Category	Count	Meaning
MustChange/rewrite	11	Correctness/integrity/outcome/build-validation defects or untested critical branches
rewrite_recommended	3	Data minimization, round-trip reduction and low-noise scheduled-idle handling
NiceToRewrite_ifTimePermits	1	Operational README improvement
Total distinct findings	15	One category and one backlog row per finding

Checklist severities: four Blocker, ten Major, one Minor. Delivery priorities: one Blocker, ten High, three Medium, one Low. These are separate scales: the build Blocker prevents reproducible validation here; it does not prove deployed service failure. Test-quality finding ACC-FR-14 is already included, not added again to the total.
Verification, Assumptions and Limits
• Eight active MSTest methods and 11 assertions were reviewed. The focused VS Code runner returned
"No tests found in the files." None of those eight tests was executed or reported passing.
• Ten isolated assertions using an existing SimpleInjector 5.5.2/net45 package confirmed the
Verify/clone/nullable mechanism and RegisterInstance alternative. This is a small contract model, not a compiled application test or proof of the deployed DLL version.
• Configured MUFG_T24/SimpleInjector DLLs and test package files are absent; msbuild/dotnet
were not found on PATH and the standard Framework 4.8 targeting pack is absent. No full build, package restore, live database, SCM lifecycle or deployed service test was performed.
• The approved baseline has blank inventory/evidence fields. Its practices are comparison targets,
not measured claims about this solution or the reference application.
• This is an as-is review, not a PR verdict against an unspecified work item. Business SLAs,
valid empty-source semantics, exact transaction/status ownership, identifier classification, row volumes, deployed SQL bodies and dependency-version equivalence need owner validation.
• Missing SQL source is not proof of missing procedures, SQL injection, bad indexes or absent
procedure-local transactions. Bound parameters are present in the local adapters.
• No local DES/3DES/ECB/MD5/SHA1, throw ex;, sync-over-async or hardcoded credential use was
found in the reviewed source. This is not a penetration test, SCA scan, history scan or certification of deployed configuration.
• Nullable EsDiaHabil is assigned from a non-nullable bool before use; BenHorario starts false
and is assigned concrete booleans. Those casts are not independent findings. Local FirstOrDefault results use null-conditional access; there is no EF IQueryable false positive.
• Shared startup validation and timer serialization already exist. The optional compatibility
startup Boolean alone, a missing local designer initialization call, or a derived Clone cast is not reported as a defect. The actual clone-identity and false-success paths are below.
3. What's Already Good
Strength	Current evidence	Preserve during remediation
Useful layer boundaries	Four application interfaces (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/Interfaces) and five project edges	Do not introduce EF, a second DI framework or microservices just to satisfy a pattern label
Constructor-injected orchestration	OperacionServicio constructor (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L38)	Existing fake seams support targeted regression tests
Real composition root and timer scopes	Host registration (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L51), per-tick scope (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L145)	Keep explicit ownership/lifetimes; correct identity and shutdown rather than replacing all DI
Bound stored-procedure parameters	MINDS query (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesMINDS.cs#L34), customer insertion (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L174)	Preserve parameterization and verify SQL-side contracts separately
Required time parsing for supplied values	ConversionHelper calls (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L82)	Add complete-snapshot validation, not silent parse defaults
Efficient membership check	HashSet-based shareholder membership (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L323)	Preserve O(1)-style membership rather than adding nested searches
Original cause retained at one outer boundary	Contextual exception with inner cause (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L248)	Use this or bare rethrow instead of the 15 string-only wrappers
Genuine isolated unit-test structure	Test setup (../../SPEI_SPID/SPID_ACCIONISTAS/PruebasUnitarias/OperacionesSpidAccionistasUT.cs#L33)	Eight real async tests use collaborator fakes and temporary logging paths, not direct SQL/SCM calls
Source hygiene	Ignore rules (../../SPEI_SPID/SPID_ACCIONISTAS/.gitignore), clean initial source snapshot	Preserve generated-output exclusions; do not commit credentials or workstation DLL copies

4. Findings by Project (low/no-dependency -> high dependency)
Fix snippets are recommended designs, not applied patches. Blocks labeled text describe proposed contracts/implementation sequences where deployed database or package details are not supplied. They are not claims that named new APIs already exist.
4.1 Aplicacion (Tier 0)
MustChange / Rewrite
ACC-FR-1 - Make the approved shared dependencies reproducible
Checklist: section 11, Blocker. Priority: Blocker. Standing finding: GEN-1. Evidence: application HintPath (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/Aplicacion.csproj#L42); four MUFG_T24 paths and one host SimpleInjector path across the four projects.
Code block (xml)
<HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
Both configured DLL targets are absent. The same MUFG_T24 path occurs in 213 portfolio project files; no portfolio nuget.config or MUFG/T24-named package archive was found. This is a systemic raw-file build convention, not a private package reference or a path typo to patch on one machine.
Code block (text)
identify compatible approved versions -> publish to approved internal NuGet feed
replace raw HintPaths with pinned package references and declared transitive dependencies
restore/build Debug and Release x86 on a clean Framework 4.8 agent; run isolated tests
Benefit / acceptance: reproducible dependency identity without a manually populated drive. Verify deployed compatibility; never substitute an arbitrary DLL to obtain a green compile.
ACC-FR-2 - Finalize execution after pre-try failures
Checklist: sections 5/14b, Major. Priority: High. Standing finding: GEN-7. Evidence: execution marking (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L55), parameter/window/alert work before the main try, and late finalization (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L255).
Code block (csharp)
await _IparametrosOperaciones.MarcarServicioEnEjecucion(_configuracion.NombreServicio);
var dicParametrosSistema = await _IparametrosOperaciones.parametrosSistema();
Parameter, calendar, parse or outside-hours alert failure after marking but before the main try bypasses its finally. The shared timer releases a semaphore, not this persisted execution state.
Code block (text)
outer try begins before execution marking; include parameter loading, gates and work
finally finalizes attempted/acquired execution with an independent bounded cleanup token
preserve the original error and separately report a cleanup failure
Benefit / acceptance: inject each preamble failure; every applicable failed, skipped and cancelled tick attempts exactly one bounded finalization. Coordinate result checks with ACC-FR-12.
ACC-FR-3 - Expose a failed entity batch to its caller
Checklist: sections 5/14b, Major. Priority: High. Evidence: entity catch (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L376) and the test intentionally pinning suppression (../../SPEI_SPID/SPID_ACCIONISTAS/PruebasUnitarias/OperacionesSpidAccionistasUT.cs#L172).
Code block (csharp)
catch (Exception ex)
{
    Config.EscribirLog($"ERROR|ValidaEntidades|Ocurre Excepcion:{ex}", NombreServicio.SPID_Accionistas.ToString());
}
An insertion/delete/status exception becomes a successful Task; the caller's critical-error path cannot observe the failed batch. Logging exists: this is lost outcome, not an empty catch. The existing test deliberately expects this behavior, so changing it requires an explicit agreed failure/alert policy, not an assertion that the test already fails.
Code block (csharp)
catch (Exception ex)
{
    Config.EscribirLog($"ERROR|ValidaEntidades|Ocurre Excepcion:{ex}", NombreServicio.SPID_Accionistas.ToString());
    throw;
}
Alternatively return an explicit typed Failed outcome if best-effort orchestration is required. Benefit / acceptance: the caller observes failure and its original cause; no later completion marker runs. Preserve intended retry/continued-tick behavior and update the suppression test.
ACC-FR-4 - Publish only a complete, validated current schedule
Checklist: sections 3/11, Major. Priority: High. Evidence: parameter mutation loop (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L76), operation-only null checks (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L151), reading gate (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L166).
Code block (csharp)
if (_configuracion.StrHoraOperacionesInicio == null)
    throw new Exception("No se especifica la hora de inicio de operaciones");
The two reading bounds are not required. On an initial missing reading key, lifted comparisons become false and report outside-hours instead of invalid configuration. On later ticks, missing keys retain old singleton values; partial parse failure can publish a mixed schedule.
Code block (text)
require HORA_LECTURA_INICIO, HORA_LECTURA_FIN, HORA_OP_INICIO and HORA_OP_FIN
parse/validate all four into a new local immutable snapshot before mutating shared state
capture one business-time value and evaluate both gates against that snapshot
Benefit / acceptance: remove each key on the first and a subsequent tick; fail explicitly before destructive work, never authorize work using stale/mixed values. Confirm time-zone and overnight rules with owners; do not invent them or blindly replace business local time with UTC.
ACC-FR-5 - Propagate cancellation and prove quiescence before release
Checklist: sections 6/14b, Major. Priority: High. Evidence: token-less entry contract (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/Interfaces/IOperacionServicio.cs#L14), host token discarded (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L148), waits and release (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L176), container disposal (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L198).
Code block (csharp)
Task EjecutaProcedimientosAsync();
All four local async interfaces omit cancellation. The inspected wait helper logs/returns on timeout without stopping the underlying task. The host does not check successful drain before releasing state and disposing its container, so a long batch can continue writes after a stop is reported. The shared same-process timer semaphore does not solve shutdown quiescence.
Code block (csharp)
Task EjecutaProcedimientosAsync(CancellationToken ct);
Propagate through every operation/query/write and check between items; preserve cancellation separately from technical failure. Use an independent bounded cleanup token. Explicitly handle a timed-out live task without claiming successful release or disposing dependencies still in use. Benefit / acceptance: a controlled blocked batch exceeding the wait deadline cannot create a false stopped/available state; cancellation prevents new work. Agree escalation/recovery policy for uncooperative calls with operations; adding a token alone is insufficient.
ACC-FR-8 - Establish an atomic, retry-safe replace-and-mark boundary
Checklist: section 8, Blocker. Priority: High. Evidence: CUSTOMER replacement (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L290), SHAREHOLDER deletion (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L319), VENDORS deletion (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L362) and their separate insertion/status/completion calls.
Code block (csharp)
await _IOperacionesSpidAccionistas.EliminarPorTipo(EnumEstatusComunes.CUSTOMER, listaCld);
Deletion, each row write and the processed marker use separate calls. The used shared helpers create/dispose a connection per call; no local transaction, shared UoW or staging/swap contract exists. Under ordinary per-call commits, a failure after delete/the nth row leaves a partial refresh. SHAREHOLDER/VENDORS even fetch their source rows after deletion.
Code block (text)
fetch and validate source first -> stage an identified complete batch
one TOMI transaction: claim/check -> replace -> update client statuses -> mark processed
commit once, rollback on failure, and retry by stable batch identity without duplicate effects
Introduce a parameterized aggregate repository/procedure boundary, preferably typed TVPs/staging. Procedure-local transactions are unknown and would not alone cover the caller's separate calls. Do not invent a cross-database distributed transaction for source reads. Benefit / acceptance: faults at every stage preserve prior valid target state and pending status; retry/concurrent claims cannot commit partial/duplicate batches. Confirm whether each of the three existing independent batch keys is the required atomic scope. Three delete/insert/mark blocks are the measured extraction point, not three duplicated architecture tickets.
Rewrite Recommended
ACC-FR-6 - Minimize routine customer-identifier logging
Checklist: sections 5/9, Major. Priority: Medium. Evidence: full CDL list (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L288), shareholder log (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L317) and per-customer date log (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L225).
Code block (csharp)
var listaCld = string.Join(",", listaAccionistas.Keys);
Routine logs copy the complete customer-key list and, elsewhere, customer/date details. CDL classification/access/retention is unknown: this is a minimization recommendation, not a proven credential leak, exfiltration event or CVSS-rated vulnerability.
Code block (csharp)
Config.EscribirLog(
    $"INFO|ValidaEntidades|Comienza validacion; clientes={listaAccionistas.Count}.",
    NombreServicio.SPID_Accionistas.ToString());
Benefit / acceptance: use counts/correlation; recognizable synthetic identifiers are absent from normal logs. Apply owner-approved masking where needed. Do not call this GEN-5 credential logging.
ACC-FR-11 - Keep approved scheduled idle low-noise
Checklist: sections 5/14b, Major. Priority: Medium. Standing finding: GEN-10, local attempt variant. Evidence: unconditional outside-window branch (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L185).
Code block (csharp)
if (estaFueraHorarioLectura)
Every outside tick emits ERROR and constructs an alert. With valid metadata the helper attempts an insert each time; no client-side transition/deduplication guard exists. A hypothetical ten-second interval over eight idle hours means 2,880 attempts, not a measured deployment count. Procedure-side deduplication is unknown; ACC-FR-13 can currently fail during construction first.
Code block (text)
normal outside-window cycle -> Skipped outcome / transition log
owner-defined unexpected transition -> bounded alert with stable deduplication key
Benefit / acceptance: repeated expected idle ticks do not repeatedly call alert persistence; genuine invalid configuration still fails explicitly. Agree the alert policy before changing UX.
Nice to Rewrite if Time Permits
No additional optional application-only finding is raised. SQL/wire DTO names and persisted status keys are intentional contracts until proven otherwise; do not cosmetically rename them.
4.2 Infraestructura (Tier 1)
MustChange / Rewrite
ACC-FR-7 - Reject failed procedure results before using data or continuing writes
Checklist: sections 3/8, Blocker. Priority: High. Standing finding: GEN-11. Evidence: MINDS result projection (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesMINDS.cs#L40), T24 result fallback (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L62), ignored write result (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L149).
Code block (csharp)
return resultado?.Data ?? new List<DOAccionista>();
All eight local query-helper sites and all seven nonquery-helper sites omit Exito checks. The inspected non-strict query overload can catch SqlException and return Exito=false, Exception and an empty Data list. Writes can also return failed results without throwing. The local adapters reduce those outcomes to empty/null data or successful Tasks.
This is especially dangerous after deletion: a failed MINDS shareholder read can become zero rows, SIN_ACCIONISTAS statuses and a processed marker. An outage is not a legitimate empty source. Parameter/status reads can similarly look empty/missing rather than failed.
Code block (csharp)
if (resultado == null || !resultado.Exito)
{
    throw new InvalidOperationException(
        "MINDS shareholder query failed.", resultado?.Exception);
}
return resultado.Data;
Apply one checked-result boundary to all eight reads/seven writes; preserve the original cause. Benefit / acceptance: failed outcomes cannot reach processed markers, while successful empty results follow the agreed policy. Do not blindly enable strict affected-row checks: valid empty reads and NOCOUNT/-1 are not automatically errors. This reports local misuse, not a new defect inside MUFG_T24. Lifecycle/alert outcomes are separately scoped in ACC-FR-12.
ACC-FR-9 - Preserve exception types, causes and throw origins
Checklist: section 5, Blocker. Priority: High. Evidence: Message-only wrapper (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L74), MINDS wrapper (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesMINDS.cs#L45), write wrapper (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L160).
Code block (csharp)
throw new Exception(ex.ToString());
Fourteen ToString wrappers and one Message wrapper occur in the three adapters. ToString may preserve an old stack as text, but it replaces the structured exception type/cause chain/throw identity. The Message-only case loses even that text. Cancellation would also lose its type once tokens are propagated.
Code block (csharp)
throw;
Or use a specific contextual wrapper with ex as InnerException when an abstraction boundary actually requires it. Keep needed logging at appropriate boundaries; do not add a generic success-shaped catch/executor abstraction. Benefit / acceptance: injected sentinel exceptions retain their identity/type or a deliberate inner-cause chain; OperationCanceledException remains cancellation. Group all 15 sites in one ticket.
Rewrite Recommended
ACC-FR-10 - Replace five row-write loops with bounded bulk/staged operations
Checklist: sections 7/8/12, Major. Priority: Medium. Evidence: customer loop (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L292), shareholder loop (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L325), client status loop (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L340), related-party loop (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L365), date loop (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L223).
Code block (csharp)
await _IOperacionesSpidAccionistas.InsertarCliente(cliente);
When all three batches are pending, the per-entity write model is F + 2C + S + R + 6: F dates, C customers, S shareholders, R related rows, plus three deletions/three processed markers. Polling, calendar, lifecycle and alert calls are additional and excluded from that formula. There is no measured latency/throughput SLA or row volume; do not claim a benchmark result.
Code block (text)
typed TVPs / bounded staging chunks -> validate counts and outcomes -> atomic commit
record command counts, duration, memory and cancellation at agreed representative volumes
Benefit / acceptance: demonstrate fewer round trips with bounded memory and chunk sizes, while preserving ACC-FR-8 integrity and domain-specific mapping. Reuse the aggregate boundary, not a second generic batch framework.
Nice to Rewrite if Time Permits
No separate optional repository ticket. Unused members/formatting should be removed only when directly touching the relevant code. Connection/command/reader disposal is already in the inspected shared helper; a local raw-resource leak is not established.
4.3 SPID_Accionistas Host (Tier 2)
MustChange / Rewrite
ACC-FR-12 - Check cleanup outcomes before claiming successful shutdown
Checklist: sections 5/11/14b, Major. Priority: High. Standing finding: GEN-9. Evidence: release/finalization/success sequence (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L180), Task-only alert wrapper (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L88), Task-only finalization wrapper (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L94).
Code block (csharp)
await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio, cleanupCt);
await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio, cleanupCt);
The following line logs successful database termination even if release returned false or finalization returned Exito=false. Shared failure logging may occur, but the local success claim contradicts the result. The Task-only adapters also erase alert/finalization outcomes. ActualizaEJecucion already checks/throws and is excluded. The documented optional compatibility startup bool alone is not an independent finding or a mandate to change startup policy.
Code block (csharp)
bool released = await _controlServicio.MarcarPuertoEnUsoAsync(
    null, null, null, _configuracionBase.NombreServicio, cleanupCt);
var terminated = await _controlServicio.ActualizaTerminacion(
    _configuracionBase.NombreServicio, cleanupCt);
if (!released || terminated == null || !terminated.Exito)
{
    throw new InvalidOperationException("Service state cleanup failed.", terminated?.Exception);
}
Log success only after confirmed completion; preserve/report original and secondary cleanup failures without replacing the root cause. Check failed alert/finalization outcomes at the local adapter boundary rather than returning success-shaped Tasks. Benefit / acceptance: false release, failed finalization and failed alert persistence are observable; no contradictory local success message remains. ACC-FR-5 separately proves drain.
ACC-FR-13 - Register initialized process identity, not an early singleton clone
Checklist: section 4, Major. Priority: High. Standing finding: GEN-8, stale-clone variant. Evidence: singleton factory (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L92), Verify (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L104), later PID population (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L120), application alert cast (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L195) and repository alert cast (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L81).
Code block (csharp)
_containerDI.Register<ConfiguracionServicio>(() => (ConfiguracionServicio)_configuracionBase.Clone(), Lifestyle.Singleton);
Verify instantiates the singleton before ParametrizacionesAsync fills the original PID/name/display. MemberwiseClone retains the derived type, but copied nullable/string values do not track later changes. The shared startup guard validates the original, not the clone injected into both the application and main repository. Alert construction can cast its null PID and fail before the helper's safe nullable handling, masking an original error or bypassing outside-hours cleanup.
Code block (csharp)
_containerDI.RegisterInstance<ConfiguracionServicio>(_configuracionBase);
Alternatively register a fully initialized immutable process snapshot before consumer verification. Validate the instance actually injected; do not simply default PID to zero. Ten isolated DI/ clone/nullable assertions reproduced the mechanism and same-instance alternative using an existing SimpleInjector 5.5.2 package, without loading the service or touching a database. Benefit / acceptance: every resolved consumer sees initialized process metadata; outside-hours and injected-error alerts preserve that identity and the original error. Deployed DLL equivalence and SCM behavior remain unverified.
Rewrite Recommended
No additional host-only ticket. Cancellation/drain is already consolidated in ACC-FR-5.
Nice to Rewrite if Time Permits
No host rewrite mandated. Shared lifecycle/ServiceName implementation is dependency context, not an in-scope local constructor/designer defect.
4.4 PruebasUnitarias (Tier 2)
MustChange / Rewrite
ACC-FR-14 - Test pending enrichment and failure/recovery boundaries
Checklist: section 13, Major. Priority: High. Evidence: test deliberately suppressing failure (../../SPEI_SPID/SPID_ACCIONISTAS/PruebasUnitarias/OperacionesSpidAccionistasUT.cs#L172), processed defaults (../../SPEI_SPID/SPID_ACCIONISTAS/PruebasUnitarias/OperacionesSpidAccionistasUT.cs#L288), MINDS fake (../../SPEI_SPID/SPID_ACCIONISTAS/PruebasUnitarias/OperacionesSpidAccionistasUT.cs#L374).
Code block (csharp)
public string EstatusAccionistasResultado { get; set; } = "PROCESADO";
public string EstatusRelacionadasResultado { get; set; } = "PROCESADO";
The declarations are wrapped across lines in source. Eight real tests/11 assertions exist, but none sets SHAREHOLDER or VENDORS pending. Those critical bodies are unexercised; source fakes always return empty lists, and delete/status/completion stubs retain no state to assert integrity. The error test checks no alert, not rollback, pending status or a failed result.
Code block (text)
stateful fakes and cases for pending CUSTOMER / SHAREHOLDER / VENDORS
successful empty versus technical failure; nth-write failure; rollback/retry/processed ordering
missing keys on first/later tick; narrow schedule boundaries; real DI identity; cancellation/drain
Use one controlled clock end-to-end: the fake returns noon, but gating also reads DateTime.Now. Existing all-day schedules are not proven flaky; they cannot validate narrow boundaries. Benefit / acceptance: each new regression fails on its corresponding defect and runs without live SQL/SCM. Update suppression expectations according to ACC-FR-3's approved outcome policy. Source branch gaps are not a measured zero-percent solution coverage claim. Focused discovery found no tests, so no application suite pass is claimed.
Rewrite Recommended
No extra generic "add tests" ticket duplicates ACC-FR-14.
Nice to Rewrite if Time Permits
No style-only test ticket. Preserve the existing useful handwritten fakes and strengthen them.
4.5 Root Support
MustChange / Rewrite
No additional root-file correctness finding.
Rewrite Recommended
No additional root-only design finding.
Nice to Rewrite if Time Permits
ACC-FR-15 - Make the README an operational entry point
Checklist: sections 11/15, Minor. Priority: Low. Evidence: generic getting-started template (../../SPEI_SPID/SPID_ACCIONISTAS/README.md#L5).
Code block (markdown)
## Getting started
The 93-line template gives GitLab onboarding advice, not x86 toolchain/shared-version requirements, safe test steps, configuration ownership, Windows Service installation/recovery or support links. This is a handover gap, not proof of a runtime incident.
Code block (text)
purpose/data flow -> approved build/dependency inputs -> safe tests -> configuration ownership
service install/stop/recovery -> support/rollback -> maintained architecture/audit/runbook links
Benefit / acceptance: a new maintainer can locate the approved runbook and prerequisites without guessing or adding credentials/deployed configuration to source. Owner-validate exact environment-specific commands rather than inventing production installation instructions.
5. Architecture-Fit Note
Apply the shared architecture trade-off framework (./_ORCHESTRATION.md#6-standing-architecture-question-is-the-current-hostingarchitecture-model-right): the observed workload is scheduled, cross-database enrichment with independent batch completion keys, not a continuous message listener or demonstrated distributed saga. Keeping the Windows Service while repairing integrity/outcomes/testability is the lowest-risk initial path. No hosting choice can be declared correct without actual SLAs, row counts, deployment constraints and budget.
Option	Fit to the observed workload	Trade-offs / decision evidence needed
Modernize in place	Strong near-term fit for the existing polling service	Preserve operations/DB contracts; fix correctness and tests first. A later supported .NET worker migration requires dependency/Windows-host compatibility validation
Queue/event-driven	Consider only if upstream can reliably publish identifiable source-change/batch events	Reduces idle polling, but adds event ownership, retries, ordering, deduplication and dead-letter contracts; none is supplied here
Scheduled/serverless job	Possible if each batch is short-lived, bounded and restartable	Need measured execution time, SQL/network access, cold-start/cut-off tolerance and approved hosting; do not assume cloud eligibility
Workflow/saga engine	Not justified by three status keys alone	Consider if owners establish independent long-running steps/compensation requirements; otherwise adds unnecessary operational machinery
Full microservices rewrite	No demonstrated need from this review	Requires a separately evidenced scaling/team-ownership/business case; highest migration risk and not a first-line code-quality fix

Sequence: establish compatible dependencies and failing regression evidence; fix failed-result, clone, transaction, finalization and shutdown behavior; then remove duplicate/row-write overhead using the same checked aggregate boundary; only then compare hosting options against measurements. Agree empty-source, independent-batch, failure/alert, time-zone and stop/recovery semantics before behavior changes. An atomic TOMI replacement should include the owner-approved associated date and client-status updates, not just inserts.
Production readiness: not certified by this audit. Do not infer "ready" from exhaustive source coverage or the ten contract assertions. Release approval requires an approved compatible build, executed regression/integration evidence, deployed SQL/configuration review, rollback/recovery rehearsal and business-owner acceptance. All 15 backlog items remain open.
6. JIRA-Ready Backlog
IDs are proposed ticket keys, not already-created Jira issues. Checklist severity appears in each finding; Priority below is delivery ordering. Test ACC-FR-14 supplies regression acceptance across the implementation tickets, not an additional copy of those defects.
ID	Title	Category	Project	File(s)	Priority
ACC-FR-1	Package/pin compatible shared dependencies and reproduce the x86 build	MustChange/rewrite	All four; Aplicacion anchor	Application project (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/Aplicacion.csproj#L42) and other project HintPaths	Blocker
ACC-FR-2	Enclose tick preamble in bounded execution finalization	MustChange/rewrite	Aplicacion	OperacionServicio (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L55)	High
ACC-FR-3	Expose entity-batch failure while preserving agreed retry policy	MustChange/rewrite	Aplicacion	Entity catch (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L376)	High
ACC-FR-4	Validate/publish one complete current schedule snapshot	MustChange/rewrite	Aplicacion	Parameter loading (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L76)	High
ACC-FR-5	Propagate cancellation and gate release/disposal on quiescence	MustChange/rewrite	Aplicacion, Infraestructura, host	Entry contract (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/Interfaces/IOperacionServicio.cs#L14), host drain (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L176)	High
ACC-FR-6	Replace raw customer identifiers in routine logs with counts/correlation	rewrite_recommended	Aplicacion, Infraestructura	CDL logging (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L288), date logging (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L225)	Medium
ACC-FR-7	Enforce failed-result checks at all eight read/seven write boundaries	MustChange/rewrite	Infraestructura	MINDS projection (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesMINDS.cs#L40) and other adapters	High
ACC-FR-8	Add atomic staged replacement and retry-safe completion per batch	MustChange/rewrite	Aplicacion / repository / owner-managed TOMI procedures	Replacement orchestration (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L290)	High
ACC-FR-9	Replace 15 string-only exception rethrows	MustChange/rewrite	Infraestructura	Three adapters (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio)	High
ACC-FR-10	Replace five serial row-write loops with bounded TVP/staging writes	rewrite_recommended	Aplicacion, Infraestructura	Customer loop (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L292), date loop (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/OperacionesRepositorio.cs#L223)	Medium
ACC-FR-11	Make expected idle ticks low-noise and bound exceptional alerts	rewrite_recommended	Aplicacion	Outside-window branch (../../SPEI_SPID/SPID_ACCIONISTAS/Aplicacion/OperacionServicio.cs#L185)	Medium
ACC-FR-12	Validate cleanup outcomes before local shutdown success	MustChange/rewrite	Host, Infraestructura	Shutdown (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L180), wrappers (../../SPEI_SPID/SPID_ACCIONISTAS/Infraestructura/Repositorio/ConsultaCamposBDTomi.cs#L88)	High
ACC-FR-13	Replace the early singleton process-configuration clone	MustChange/rewrite	Host / consumer identity	Configuration registration (../../SPEI_SPID/SPID_ACCIONISTAS/Entradas/ServiceHost.cs#L92)	High
ACC-FR-14	Add stateful pending-enrichment/fault/schedule/shutdown regressions	MustChange/rewrite	PruebasUnitarias	Test class and fakes (../../SPEI_SPID/SPID_ACCIONISTAS/PruebasUnitarias/OperacionesSpidAccionistasUT.cs#L21)	High
ACC-FR-15	Replace the GitLab README template with approved operational links	NiceToRewrite_ifTimePermits	Root support	README (../../SPEI_SPID/SPID_ACCIONISTAS/README.md#L5)	Low

See the scoped executive summary (../excecutive_docs/SPID_Accionistas/Executive_Portfolio_Summary.md) for the phased 90-day plan, and the handover playbook (./_SPID_Accionistas_HANDOVER_PLAYBOOK.md) and validation record (./_SPID_Accionistas_Validation.md) for continuation and evidence boundaries.
