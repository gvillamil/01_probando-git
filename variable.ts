LaikaAutomator Recommendations
LaikaAutomator - Architecture and Code Quality Recommendations
EXHAUSTIVE SOURCE AUDIT COMPLETE - ALL REMEDIATION REMAINS OPEN
Reviewed: 2026-10-14. Target: .NET Framework 4.8 / x86. Focus: all review categories. Source: 42987fa63c4f1033a3b767b0aa195de7e1d54dbb, branch CodeReview.
Solution: LaikaAutomator.sln (../../SWIFT/LAIKA/LaikaAutomator.sln). Compared against the user-selected LoansApp architecture/coding baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR/severity checklist (../BaseLine_Docs/code_review.md). See the canonical orchestration document (./_ORCHESTRATION.md), exhaustive-review tracker (./_LAIKA_ExhaustiveReview_Progress.md), and source manifest (./_LAIKA_FileManifest.csv).
1. Solution Overview (low/no-dependency -> high dependency)
Tier	Project	Kind	Path
0	Aplicacion	Application contracts, DTOs, routing and orchestrator	Aplicacion.csproj (../../SWIFT/LAIKA/Aplicacion/Aplicacion.csproj)
1	Infraestructura	Database adapters and SWIFT handlers	Infraestructura.csproj (../../SWIFT/LAIKA/Infraestructura/Infraestructura.csproj)
2	LaikaAutomator	Windows Service executable / composition root	LaikaAutomator.csproj (../../SWIFT/LAIKA/LaikaAutomator/LaikaAutomator.csproj)
2	PruebasUnitarias	MSTest/Moq test library	PruebasUnitarias.csproj (../../SWIFT/LAIKA/PruebasUnitarias/PruebasUnitarias.csproj)

What this solution actually does: LAIKA is a timer-driven Windows Service that polls a database-backed SWIFT folio queue and delegates processing by message family and direction. Its factory selects inbound/outbound MT103, inbound MT202, inbound MT202 COV, inbound pacs.008, or unsupported-message handlers. Infrastructure supplies automatic/manual decisions, commission/ correspondent checks and work-plan operations against TOMI, T24_VW and SPEI. The automatic state sequence is X_ASR -> X_AUT -> AUT, with detail IDs 3 -> 4 -> 5. The manual path validates the initial state without executing those automatic updates; its label is not proof of a separate persisted manual transition. Direct settlement effects and business acceptance rules have not been owner-validated. It is not a demonstrated hosted web API; System.Web.Extensions alone does not make it one.
The graph has four nodes and five ProjectReference edges. Aplicacion has three direct consumers, Infraestructura two. The actual host is under LaikaAutomator, not the obsolete Entradas path. See the dependency ladder (../excecutive_docs/LAIKA/Base_Project_Dependency_Ladder.md) and functional/technical understanding (../architecture/LAIKA/Application_understanding_technical_document.md).
Prioritized Issue Summary
Twenty-two grouped findings are open. None was fixed by this documentation audit. Priorities below are delivery/backlog priorities, not CVSS or evidence of a live incident.
Priority	Count	Simple meaning
Blocker	1	Establish reproducible approved shared build inputs before accepting build assurance
High	14	Correct confirmed data/authorization-boundary, outcome, diagnostic, metadata, privacy, configuration/unit defects and critical regression gaps
Medium	6	Harden the latent null-result contract; improve cancellation, parsing, DI, duplication and operating instructions
Low	1	Group cosmetic naming/import cleanup; no runtime failure claimed
Test evidence	Not a score	Three meaningful happy-path tests, two Assert and seven Moq Verify calls; no suite execution or measured coverage

Category	Count
MustChange/rewrite	15
rewrite_recommended	6
NiceToRewrite_ifTimePermits	1

Highest-value first steps: pin the approved dependencies (LAI-1), make failed database results explicit (LAI-10), correct the scalar mapping and non-terminal rejection (LAI-14/13), repair consumer-visible startup metadata (LAI-19), and remove account material from logs (LAI-16). Repair the other boundary/metadata defects with their regressions before deduplication. LAI-22 was added during final logging-contract verification. LAI-2 was deliberately re-triaged after the full graph showed no current normal null-returning processor.
2. Scope and Methodology Note
Every file and every line in all four projects and root support was read: 88/88 tracked text files, 4,993/4,993 physical lines. No pending source ranges remain. The manifest records SHA256, physical ranges and completed batches for each file. Metadata, pattern scans and dependency-context reads were never substituted for implementation reading.
Formal scope	Files	Lines	Current status
Aplicacion	46	982	Full read complete, batches A-D
Infraestructura	29	2,644	Full read complete, batches E-H
Host	6	411	Full read complete, batch I
Tests	4	335	Full read complete, batch J
Root support	3	621	Full read complete, batch K
Total	88	4,993	Exhaustive source review complete

Batch	Content	Files	Lines
A	Project, orchestrator, models, assembly metadata and four direct interfaces	9	329
B	Every DTO	9	173
C	Every enum, constants file and resolver	10	229
D	The other interfaces	18	251
E	Infrastructure metadata, factory, queue and state core	12	752
F	All supporting database adapters	11	622
G	MT103 IN/OUT and unsupported handlers	3	585
H	MT202 IN/COV and pacs.008 IN handlers	3	685
I	Entire Windows Service host	6	411
J	Entire test project	4	335
K	Solution, README and Git ignore	3	621
Total	Entire repository source	88	4,993

SHA256 and line counts pin every source file in the manifest. Findings were persisted after each batch. The user explicitly requested "Resume until all finished"; the continuation completed the remaining projects sequentially. Whole-project scans were cross-checks, not sampling.
Review Basis, Boundaries and Verification
• The user approved an as-is audit. No unspecified story/acceptance criterion is treated as fact.
• Checklist severities are Blocker/Major/Minor; finding categories and priorities are separate.
• The accepted baseline has empty inventory/evidence fields. Its stated practices are the comparison
standard, not proof that a reference application actually has measured coverage or quality.
• MUFG_T24 is dependency context only. Shared configuration, startup/timer guards, repository
result/scalar mapping and logging contracts were examined to avoid false service findings. Every finding location is inside LAIKA; the available shared source is not asserted to be the unidentified deployed DLL.
• Five absolute external DLL references were verified. The configured MUFG_T24/SimpleInjector targets
are absent, no portfolio NuGet config/MUFG-named archive was found, no msbuild/dotnet was on PATH, and the standard Framework 4.8 targeting-pack directory was absent. No full build/restore was run.
• Executed: 14 isolated current-behavior checks compiled the actual application message parser,
DTO and enums with PowerShell Add-Type. They confirmed the parsing examples in LAI-4 without contacting a database, service or shared DLL. They are not the existing MSTest suite or coverage.
• Executed: five isolated probes compiled the unmodified existing tag-boundary method inside
a minimal class shell. Four expected cases and one reproduced terminal-field defect were observed (LAI-11). The whole utility class could not be compiled by the available legacy compiler because it lacks support for string interpolation; this is not a failed intended solution build.
• Dependency context only: four probes compiled the unchanged shared key-redaction methods/
field definitions. Three free-prose dummy account messages remained unmasked; a keyed control was masked (LAI-16). No file logger was initialized. Int32 reflection and exact log-size unit arithmetic supplemented static contract analysis; neither executed a database helper.
• All three existing MSTest bodies were reviewed: real mocked dispatch/assignment/pipeline tests,
two Assert calls and seven Moq verifications. There are no authored factory/concrete eligibility-handler negative regressions. This is not a measured 0% coverage claim.
• No service was started, database queried, package installed, source code modified or commit created.
Live schedules/SLAs, deployed DLL/schema contracts, queue guarantees and SCM behavior are unverified.
Source completion, document/Word validation and remediation completion are different gates. Final fingerprint/link/heading/category/backlog/OOXML checks are recorded in the progress tracker (./_LAIKA_ExhaustiveReview_Progress.md). Portfolio rows are marked Done only after the final deliverables pass; all 22 remedies remain open.
Quantified Checks and Applicability
All 45 application C# files contain zero CancellationToken references, but 16 interfaces declare 25 Task-returning members. No blocking async calls, FirstOrDefault/SingleOrDefault calls, nullable .Value dereferences, throw ex;, checked legacy crypto API calls or DateTime.Now calls were found. The nullable Boolean cast is a latent defensive issue, while the stale injected PID is a confirmed local composition defect. Absence of .Value does not establish null safety.
There are 25 LINQ imports (24 unused), 24 unused System.Text imports, and six empty specialized processor interfaces inheriting the common contract. No substantial duplicated processing block was found in Aplicacion. Those six DI role interfaces are not a reason to invent a new abstraction.
Whole-infrastructure supplement	Count / conclusion
Generic query / NonQuery calls	28 / 3; zero local Exito inspections
Stack-losing exception replacements	16
ContinueWith calls	14; no deadlock claim solely from completed-task Result
Manual result wrappers incorrectly reporting automatic=true	3
Explicit raw account/VOSTRO log statements	3; current keyed masking does not cover their prose
Duplicated foreign-currency policy	Two methods, 75/74 lines, five cases apiece
Repeated pipeline/result wrappers	Ten across five supported handlers
Local transaction API references	Zero; SQL transaction/claim guarantees remain unverified, not inferred

Whole-solution scans found no throw ex;, selected legacy crypto/BinaryFormatter, ECB, DateTime.Now, async-void or blocking Wait/GetAwaiter().GetResult() occurrences in LAIKA source. That does not negate the 16 throw new Exception(ex.Message) sites or nullable casts. No explicit credential literal was found in the reviewed source/configuration; raw account logging is separately confirmed. This is not assurance for unseen deployment values or unidentified binaries. Web/MVC/JavaScript/EF-specific rules are inapplicable to the observed worker.
3. What's Already Good
Strength	Concrete evidence	Why preserve it
Constructor injection and readonly dependencies	OperacionServicio.cs:32 (../../SWIFT/LAIKA/Aplicacion/OperacionServicio.cs#L32)	Existing interfaces already provide application test seams; do not add interfaces indiscriminately
Asynchronous sequential orchestration	OperacionServicio.cs:49 (../../SWIFT/LAIKA/Aplicacion/OperacionServicio.cs#L49)	No sync-over-async; preserve ordering until concurrency/idempotency rules are known
Per-folio exception containment with diagnostics	OperacionServicio.cs:69 (../../SWIFT/LAIKA/Aplicacion/OperacionServicio.cs#L69)	One failed item does not automatically prevent later items; containment is not an empty catch
Decimal commission amounts	DComisionEsquemaAfiliado.cs:13 (../../SWIFT/LAIKA/Aplicacion/DTOs/DComisionEsquemaAfiliado.cs#L13)	Amount/tax/total avoid binary floating-point money
Explicit persisted-looking detail/commission IDs	EDetallePlan.cs:14 (../../SWIFT/LAIKA/Aplicacion/Enums/EDetallePlan.cs#L14), ESwiftComision.cs:14 (../../SWIFT/LAIKA/Aplicacion/Enums/ESwiftComision.cs#L14)	Preserve existing numeric values when maintaining compatibility
Invariant routing normalization and unsupported route	SwiftFlujoResolvedor.cs:11 (../../SWIFT/LAIKA/Aplicacion/Enums/SwiftFlujoResolvedor.cs#L11)	Useful deterministic routing baseline; do not replace a small switch solely to satisfy a pattern
Single common processor contract	IOperacionRepositorioSwift.cs:13 (../../SWIFT/LAIKA/Aplicacion/Interfaces/IOperacionRepositorioSwift.cs#L13)	Six specialized DI roles already share the processing operation
Parameterized client-side procedure calls	PlanTrabajoMaestro.cs:35 (../../SWIFT/LAIKA/Infraestructura/Repositorio/PlanTrabajoMaestro.cs#L35)	Keep parameter binding; SQL internals are not supplied and are not certified by this observation
Explicit unsupported outcome	OperacionRepositorioSwiftNoSoportado.cs:33 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioSwiftNoSoportado.cs#L33)	Unsupported processing returns false and does not pretend to mutate a work plan
Meaningful mocked tests already exist	LaikaUT.cs:61 (../../SWIFT/LAIKA/PruebasUnitarias/LaikaUT.cs#L61), state test :86 (../../SWIFT/LAIKA/PruebasUnitarias/LaikaUT.cs#L86)	Preserve dispatch/assignment assertions while extending failure and authorization coverage
Host delegates lifecycle and scopes explicitly	ServiceHost.cs:154 (../../SWIFT/LAIKA/LaikaAutomator/ServiceHost.cs#L154)	Inspected base serializes ticks and validates the original startup configuration; fix the local clone/token gaps, not invented missing base behavior

4. Findings by Project (low/no-dependency -> high dependency)
4.1 Aplicacion (Tier 0) - complete: 46 files / 982 lines
MustChange / Rewrite
LAI-1 - Restore reproducible, approved shared build inputs
Category: MustChange/rewrite. Checklist: section 11, Blocker. Priority: Blocker.
Location: Aplicacion.csproj:38 (../../SWIFT/LAIKA/Aplicacion/Aplicacion.csproj#L38). Related verified metadata: infrastructure :38, host :41/:45, tests :57.
Current code:
Code block (xml)
<Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
  <SpecificVersion>False</SpecificVersion>
  <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
</Reference>
Issue and impact: a raw machine path with no effective version pin is not a reproducible dependency contract. Four MUFG_T24 references and one SimpleInjector reference require two DLL targets absent on this machine. The portfolio filename check found neither a NuGet config nor a MUFG-named package archive. This is a verified build-input limitation, not a claimed executed compiler failure or proof that production is stopped.
Fix: reuse central GEN-1 in the orchestration document (./_ORCHESTRATION.md#gen-1-hardcoded-absolute-external-dll-references-via-hintpath-instead-of-a-package-feed). Obtain the exact approved compatible binaries and dependency graph, publish/consume versioned internal packages through the approved feed, and replace the raw references coherently. Do not create four duplicate remediation tickets, copy a random old DLL, or just change a drive.
Illustrative project shape after an approved package actually exists:
Code block (xml)
<PackageReference Include="MUFG_T24" Version="APPROVED_PUBLISHED_VERSION" />
The version token is deliberately not a fabricated package/version and is not a ready-to-apply patch. SimpleInjector needs equivalent approved dependency treatment.
Benefit / acceptance: an approved clean agent resolves the same versions without a populated D:\ServiceCMS folder; full build and safe tests record the resolved inputs. Track once as suggested JIRA LAI-1, linked to GEN-1. Application/shared code is not changed by this report.
Rewrite Recommended
LAI-2 - Do not cast a missing processor result's nullable Boolean to bool
Category: rewrite_recommended. Checklist: section 4, Major. Priority: Medium.
Location: OperacionServicio.cs:60 (../../SWIFT/LAIKA/Aplicacion/OperacionServicio.cs#L60).
Current code:
Code block (csharp)
var resultado = await _swiftAutomatizacionFactory.CrearYProcesar(folio);
 
if ((bool)!resultado?.Procesado)
{
    Config.EscribirLog($"ERROR|EjecutaProcedimientosAsync| El Folio: '{folio}' no se proceso correctamente. Detalle: {resultado?.Mensaje}", NombreServicio.LAIKA.ToString());
    continue;
}
Issue and impact: if the factory contract returns null, resultado?.Procesado and its negation are nullable-null. Casting that value to bool throws InvalidOperationException. The null-conditional is not a guard against the explicit cast. The per-folio catch contains the failure, so this is not a demonstrated whole-service crash or lost-payment incident. All six reviewed concrete processors registered by the host construct non-null normal results. The expression is still unsafe for a null contract result, but no current normal null producer was found. Full-graph re-triage changes the initial application-only MustChange/High label to rewrite_recommended/Medium; do not present it as an observed current-path production failure.
Recommended code shape:
Code block (csharp)
if (resultado == null)
{
    Config.EscribirLog(
        $"ERROR|EjecutaProcedimientosAsync| Sin resultado para el folio: '{folio}'",
        NombreServicio.LAIKA.ToString());
    continue;
}
 
if (!resultado.Procesado)
{
    Config.EscribirLog(
        $"ERROR|EjecutaProcedimientosAsync| El Folio: '{folio}' no se proceso correctamente. Detalle: {resultado.Mensaje}",
        NombreServicio.LAIKA.ToString());
    continue;
}
Benefit / acceptance: explicitly report the missing result without a secondary CLR exception. Add null/false/true-result regressions and prove that the next folio is still processed after a null/false result. The null path should not generate a successful-processing log. Decide durable retry/reconciliation separately after verifying the queue/database contract. Suggested JIRA: LAI-2.
LAI-3 - Preserve cooperative cancellation across the application contract
Category: rewrite_recommended. Checklist: section 6, Major. Priority: Medium.
Locations: IOperacionServicio.cs:14 (../../SWIFT/LAIKA/Aplicacion/Interfaces/IOperacionServicio.cs#L14), OperacionServicio.cs:43 (../../SWIFT/LAIKA/Aplicacion/OperacionServicio.cs#L43), queue interface :14 (../../SWIFT/LAIKA/Aplicacion/Interfaces/IOperacionRepositorioLaikaQueue.cs#L14), and factory interface :18 (../../SWIFT/LAIKA/Aplicacion/Interfaces/ISwiftAutomatizacionFactory.cs#L18).
Current boundary:
Code block (csharp)
Task EjecutaProcedimientosAsync();
Issue and impact: no application C# file references CancellationToken, while the host timer receives one and 25 Task-returning interface members cannot carry it. The application cannot cooperate with stop requests through this contract during long queue/handler work. This is a verified design limitation; no actual timeout or forced-stop incident was reproduced.
Coordinated contract/caller shape:
Code block (csharp)
Task EjecutaProcedimientosAsync(CancellationToken cancellationToken);
Code block (csharp)
await operacionServicio.EjecutaProcedimientosAsync(ct).ConfigureAwait(false);
Add token checks before acquiring/starting further folios, propagate cancellation through supported repository/I/O APIs, and handle normal cancellation before the broad exception catches in both application and host. Verify the actual versioned shared-helper signatures instead of inventing token overloads. Preserve bounded timeouts and do not interrupt an atomic financial transition without a defined recovery policy. The inspected shared query and NonQuery contracts do expose CancellationToken overloads; use those supported signatures after pinning the actual dependency, rather than adding Task.Run.
Benefit / acceptance: deterministic tests for stop-before-start, stop-between-folios and cancelled I/O; normal stop is not reported as a critical processing error; no abandoned task or incorrect completed-work acknowledgement. Update all implementations/callers together. Suggested JIRA: LAI-3.
LAI-4 - Establish an explicit, consistently normalized identifier vocabulary
Category: rewrite_recommended. Checklist: sections 3/14, Major. Priority: Medium.
Locations: SwiftMensajeResolvedor.cs:18 (../../SWIFT/LAIKA/Aplicacion/Enums/SwiftMensajeResolvedor.cs#L18) and direction branch :24 (../../SWIFT/LAIKA/Aplicacion/Enums/SwiftMensajeResolvedor.cs#L24).
Current code:
Code block (csharp)
return llave.MessageType.StartsWith("PACS", System.StringComparison.OrdinalIgnoreCase) ? ETipoMensajeSwift.MX : ETipoMensajeSwift.MT;
Code block (csharp)
if (valor.Contains("OUT")) return TipoOperacionSwift.OUT;
if(valor.Contains("IN")) return TipoOperacionSwift.IN;
Issue and impact: the family parser ignores case, but direction parsing uses case-sensitive substring checks. Actual-source probes confirmed in/out -> UNKNOWN, INVALID -> IN, TIMEOUT/IN/OUT -> OUT, and camt.053 -> MT. These are classification facts, not evidence that those inputs occur in production or that current authorizations are incorrect.
Fix: first inventory the stored-procedure's actual vocabulary and approved aliases; then keep an explicit ordinal mapping and normalization policy in this existing resolver. Preserve supported legacy aliases/version forms and the unsupported handler rather than tightening matching blindly. The factory's FT-prefix lookup is ordinal-ignore-case while MT103-OUT uses default StartsWith; include that discrepancy and the MT/MX commission vocabulary in the owner-approved input matrix.
Illustrative direction code only if the approved vocabulary is IN/OUT:
Code block (csharp)
var direction = (valor ?? string.Empty).Trim();
if (string.Equals(direction, "OUT", StringComparison.OrdinalIgnoreCase))
{
    return TipoOperacionSwift.OUT;
}
if (string.Equals(direction, "IN", StringComparison.OrdinalIgnoreCase))
{
    return TipoOperacionSwift.IN;
}
return TipoOperacionSwift.UNKNOWN;
Apply equivalent explicit family validation using the confirmed MT/MX/version vocabulary; do not infer that every non-PACS value is a valid MT identifier.
Benefit / acceptance: data-driven tests cover every approved alias, null/blank, casing, whitespace, ambiguous/unknown values and supported message versions. Unknown data reaches a defined unsupported/error path without accidental substring classification. The 14 executed probes describe current behavior, not acceptance of the desired future behavior. Suggested JIRA: LAI-4.
Nice to Rewrite if Time Permits
LAI-5 - Group cosmetic interface spelling and unused-import cleanup
Category: NiceToRewrite_ifTimePermits. Checklist: sections 1-2/12, Minor. Priority: Low.
Locations: DControlAutomatizacion.cs:3 (../../SWIFT/LAIKA/Aplicacion/DTOs/DControlAutomatizacion.cs#L3), and IContorlServiciosAutomatizado.cs:9 (../../SWIFT/LAIKA/Aplicacion/Interfaces/IContorlServiciosAutomatizado.cs#L9).
Current example:
Code block (csharp)
public interface IContorlServiciosAutomatizado
{
    Task<DControlAutomatizacion> ObtenerControlAutomatizadoPorIdCategoria(
        string messageType, string messageDirection, int id);
}
Cleanup example:
Code block (csharp)
public interface IControlServiciosAutomatizado
{
    Task<DControlAutomatizacion> ObtenerControlAutomatizadoPorIdCategoria(
        string messageType, string messageDirection, int id);
}
The control interface has the Contorl spelling, and 24 unused LINQ plus 24 unused System.Text imports repeat scaffold noise. Use IDE symbol refactoring and unused-import cleanup; coordinate public-name compatibility before changing the interface. Do not rename database-mapped DTO properties or alter wire/schema contracts.
Benefit / acceptance: cleaner navigation/conventions with a build-verified non-behavioral diff. Keep separate from correctness fixes. This is not an outage or security issue. Suggested JIRA: LAI-5.
4.2 Infraestructura (Tier 1) - complete: 29 files / 2,644 lines
MustChange / Rewrite
LAI-6 - Handle a missing message key before dereferencing it
• Category: MustChange/rewrite. Checklist section 4: Blocker. Priority: High.
• Location: SwiftAutomatizacionFactory.cs:74 (../../SWIFT/LAIKA/Infraestructura/Repositorio/SwiftAutomatizacionFactory.cs#L74);
the nullable result is returned at line 62 and consumed from CrearYProcesar at line 104.
• Current code:
Code block (csharp)
return resultdato?.Data?.FirstOrDefault();
Code block (csharp)
var flujo = SwiftFlujoResolvedor.Resolvedor(tipoMensaje, tipoOperacion, llave.MessageType);
• Issue/impact: an empty message-key query becomes null; the first two resolver calls tolerate
it, but the third argument dereferences the same in-memory object. Processing fails with a NullReferenceException instead of a controlled missing-key outcome. The application catch contains the item failure; no whole-service crash or confirmed queue loss is claimed.
• Fix shape in CrearYProcesar before resolving:
Code block (csharp)
if (mensajeLlave == null)
{
    return new ResultadoProceso
    {
        Folio = icn,
        Procesado = false,
        Mensaje = "No existe la llave del mensaje."
    };
}
Preserve the existing failed-result logging/notification contract, distinguish missing data from a failed SQL operation, and define retry/reconciliation with the queue owner. Separately guard the public ProcesadorDesdeLlave boundary with ArgumentNullException for an invalid direct null argument; the expected empty lookup is handled before reaching that guard.
• Benefit/acceptance: an empty query yields an explicit failed result; a direct null-key call
yields the documented argument failure, never a CLR dereference; a missing key is reported explicitly and subsequent folios still run.
Suggested JIRA: LAI-6. Remediation remains open.
LAI-7 - Bound queue acquisition before beginning processing
• Category: MustChange/rewrite. Checklist sections 6-7: Blocker. Priority: High.
• Location: OperacionRepositorioLaikaQueue.cs:35 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioLaikaQueue.cs#L35);
application processing starts only after QueueTodosLosFolio returns.
• Current code:
Code block (csharp)
do
{
    var strFolio = await QueueUnFolio();
    if (string.IsNullOrWhiteSpace(strFolio)) break;
    strFolios.Add(strFolio);
} while (true);
• Issue/impact: there is no count/time/cancellation limit. A continuously nonempty queue can keep
the worker acquiring/retaining identifiers indefinitely, so it never reaches processing. Even finite large backlogs are fully materialized first. Arrival rates are unknown; the unbounded behavior is source-proven, not a measured production outage.
• Fix: use an owner-approved positive batch bound and cancellation (coordinate LAI-3), process
that batch before acquiring more, and preserve the database's actual claim/acknowledgement contract. Do not invent a production batch-size default or assume destructive dequeue semantics.
• Proposed loop shape after the coordinated contract change:
Code block (csharp)
for (var count = 0; count < maxBatchSize; count++)
{
    cancellationToken.ThrowIfCancellationRequested();
    var folio = await QueueUnFolio(cancellationToken).ConfigureAwait(false);
    if (string.IsNullOrWhiteSpace(folio)) break;
    strFolios.Add(folio);
}
• Benefit/acceptance: with a fake source that never empties and a test bound N=2, acquire at most
two items before processing starts; a subsequent batch continues in order. Test cancellation and finite-backlog boundaries. No arbitrary N is prescribed for production.
Suggested JIRA: LAI-7. Remediation remains open.
LAI-9 - Guard nullable database text in preconditions
• Category: MustChange/rewrite. Checklist section 4: Blocker. Priority: High.
• Locations: PrecondicionCheck.cs:36 (../../SWIFT/LAIKA/Infraestructura/Repositorio/PrecondicionCheck.cs#L36)
and OperacionRepositorioMt202In.cs:64 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt202In.cs#L64).
• Current code:
Code block (csharp)
var intermediary = intermediaryBank?.info;
if (intermediary.Length < 6)
• Issue/impact: an empty query or a null info value produces a null intermediary, then Length
throws before the intended "no intermediary" log/false branch. The catch at an outer boundary is not a substitute for handling this expected missing-data boundary.
• Fix shape after checking technical query success (LAI-10):
Code block (csharp)
if (string.IsNullOrWhiteSpace(intermediary) || intermediary.Length < 6)
Preserve explicit logging and the intended non-eligible result. Do not convert a database outage into the same normal "no intermediary" outcome.
• Benefit/acceptance: missing row, null/empty/whitespace, five-character and six-character
inputs are controlled; confirmed technical failure is reported separately. Preserve known MUFG-MX and other-bank behavior.
• Repeated nullable-text boundary: MT202-IN calls plan.cve_moneda.Equals("MXN") after checking
the plan object but not that string. Log an absent currency explicitly and use a null-safe, approved comparison; test null/blank currency plus MXN/MXP/other-currency behavior. MT202-COV already uses null-safe != comparisons.
Suggested JIRA: LAI-9. Remediation remains open.
LAI-10 - Enforce database outcome contracts at every local adapter
• Category: MustChange/rewrite. Checklist sections 3, 5 and 8: Blocker. Priority: High.
• Local evidence: PlanTrabajoMaestro.cs:81 (../../SWIFT/LAIKA/Infraestructura/Repositorio/PlanTrabajoMaestro.cs#L81),
PlanTrabajoMaestroDetalle.cs:36 (../../SWIFT/LAIKA/Infraestructura/Repositorio/PlanTrabajoMaestroDetalle.cs#L36), PlanTrabajoMaestro.cs:67 (../../SWIFT/LAIKA/Infraestructura/Repositorio/PlanTrabajoMaestro.cs#L67), InformacionOperacionMt.cs:39 (../../SWIFT/LAIKA/Infraestructura/Repositorio/InformacionOperacionMt.cs#L39) and the corresponding MX adapter. Queue, control, commission, coverage and rejection adapters also need the same technical-success/empty-data distinction.
• Current patterns:
Code block (csharp)
await EjecutarStoredProceduredNonQuery(/* existing SP and parameters */);
Code block (csharp)
return Enum.TryParse<EPlanTrabajoMaestroEstatus>(estado, out var estatus)
    ? estatus
    : EPlanTrabajoMaestroEstatus.X_ASR;
Code block (csharp)
return result.Data?.FirstOrDefault() ?? new DOperacionEntrante();
• Issue/impact: the inspected helper can return a failed StoredProcedureResult without throwing.
Local callers discard Exito/Exception/MensajeError and, for writes, the entire result. A failed queue read looks empty; missing/failed state reads can become a legitimate X_ASR state; missing operation data becomes a zero/default-filled DTO. A returned Task alone therefore does not establish that a database change completed.
• Fix: check each helper's actual technical outcome before consuming Data; distinguish
successful-empty results from failed I/O; propagate an explicit failed operation with context and preserve the original exception. Reject missing/unparseable master state rather than manufacturing X_ASR. Define each SP's affected-row/business-success contract with its owner; do not equate NOCOUNT/-1 with failure or assume a particular output parameter exists.
• Proposed technical-failure guard after capturing the existing helper result:
Code block (csharp)
if (resultado == null || !resultado.Exito)
{
    throw new InvalidOperationException(
        "La operacion de persistencia no se completo.",
        resultado?.Exception);
}
A shared local helper can avoid repeating this guard once the approved dependency version is pinned. Queries that legitimately return no rows need their own explicit business outcome.
• Benefit/acceptance: fault-inject failed query and failed mutation results independently;
neither is reported as an empty healthy queue, an initial valid state, a successful rejection or successful processing. Verify successful-empty, zero-row/conflict and positive cases separately. Obtain SQL evidence for claim/acknowledgement and master/detail atomicity; those database guarantees are unknown, not inferred from procedure names.
• Cross-cutting parent: GEN-11. This finding is the service's misuse of the inspected
dependency contract, not a new shared-library implementation defect.
Suggested JIRA: LAI-10. Remediation remains open.
LAI-11 - Stop MT tag extraction at block 4 independently of block 5
• Category: MustChange/rewrite. Checklist section 3: Blocker. Priority: High.
• Locations: SwiftUtilidades.cs:38 (../../SWIFT/LAIKA/Infraestructura/Repositorio/SwiftUtilidades.cs#L38),
SwiftUtilidades.cs:53 (../../SWIFT/LAIKA/Infraestructura/Repositorio/SwiftUtilidades.cs#L53); consumer SwiftInMtComun.cs:41 (../../SWIFT/LAIKA/Infraestructura/Repositorio/SwiftInMtComun.cs#L41).
• Current code:
Code block (csharp)
const string pattern = @"(?::\d{1,2}[a-zA-Z]?:|}{5:)";
Code block (csharp)
contentEnd = match.Success ? match.Index - 1 : mensaje.Length;
The second snippet condenses the existing if/else without changing its logic.
• Issue/impact: :71A:SHA\r\n-} has no next field or block-5 marker, so the closing block-4
delimiter is included in the extracted value. Enum.TryParse then cannot recognize SHA, affecting eligibility/routing. This is not evidence of an incorrect payment or financial loss. The current minus-one rule also assumes a separator before every matched boundary.
• Fix: recognize the block-4 terminator without requiring a trailer, recognize actual field
boundaries rather than arbitrary inline colon substrings, and slice to the boundary index without unconditionally deleting a character. Keep one shared utility for MT consumers. Proposed boundary pattern shape for standard line-framed fields:
Code block (csharp)
@"(?m)^(?::\d{1,2}[A-Za-z]?:|-\})"
Confirm accepted transport framing and approved MT samples before finalizing the parser; preserve all supported line-ending/optional-block variants and reject malformed framing explicitly rather than silently approving it.
• Benefit/acceptance: final-field OUR/SHA/BEN with and without block 5; next-field CRLF/LF;
missing/empty tags; and tag-like text inside a narrative are deterministic regression cases. The terminal SHA failure is reproduced by batch F's exact boundary-method probes.
Suggested JIRA: LAI-11. Remediation remains open.
LAI-12 - Preserve exception identity and accurate diagnostic labels
• Category: MustChange/rewrite. Checklist section 5: Blocker. Priority: High.
• Location: SwiftInMtComun.cs:54 (../../SWIFT/LAIKA/Infraestructura/Repositorio/SwiftInMtComun.cs#L54).
Full infrastructure count: 16 exception-replacement statements across this adapter and the five supported handlers; exactly one obsolete Entradas name argument. Four EUR/GBP rejection reasons in the MT103-IN/pacs.008 foreign-currency switches also incorrectly label the currency JPY.
• Current code:
Code block (csharp)
Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizado|Ocurre Excepcion:" + ex.ToString(),
    NombreServicio.Entradas.ToString());
throw new Exception(ex.Message);
• Issue/impact: the new exception loses original type, stack and inner-exception identity
on the path to the caller, and four EUR/GBP reasons report JPY instead. Final shared-logging verification shows nombreArchivo is ignored by the current compatibility overload: the Entradas argument is obsolete/cosmetic, not a demonstrated misrouting defect.
• Fix shape:
Code block (csharp)
Config.EscribirLog("ERROR|MensajePuedeSerAutomaticoAutorizado|Ocurre Excepcion:" + ex,
    NombreServicio.LAIKA.ToString());
throw;
Also correct the EUR/GBP reason labels without changing bank/currency policy. Keep logs within approved sensitive-data/correlation rules; do not add full payment payloads. Direct await is preferable in a separately justified continuation refactor, but is not required for this surgical exception fix; no deadlock is alleged from an already-completed continuation's Result.
• Benefit/acceptance: a sentinel exception retains its original identity/origin through the
service boundary, and rejection diagnostics identify EUR/GBP correctly. Normal cancellation is handled as LAI-3. Do not claim that changing the ignored name argument changes routing.
Suggested JIRA: LAI-12. Remediation remains open.
LAI-13 - Stop inbound validation after rejecting a missing issuing bank
• Category: MustChange/rewrite. Checklist sections 3-4: Blocker. Priority: High.
• Locations: OperacionRepositorioMt103In.cs:210 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103In.cs#L210)
and OperacionRepositorioPacs008In.cs:248 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioPacs008In.cs#L248).
• Current code:
Code block (csharp)
if (string.IsNullOrWhiteSpace(operacionInfo.IssuingBank))
    await _rechazo.RechazarMensajeConRazon(folio, "No fue posible determinar el banco emisor.");
• Issue/impact: after recording a rejection, execution continues. For a matched currency it
dereferences a null IssuingBank in Contains; for an unmatched currency it bypasses all switch branches and may reach the successful eligibility return. Neither is a terminal rejection. No assumption about which additional currencies the business permits is needed to demonstrate the missing return after this explicit rejection.
• Fix:
Code block (csharp)
if (string.IsNullOrWhiteSpace(operacionInfo.IssuingBank))
{
    await _rechazo.RechazarMensajeConRazon(folio, "No fue posible determinar el banco emisor.");
    return false;
}
Also handle a missing operation DTO explicitly (LAI-10); do not turn technical query failure into a business rejection. Define the switch's other-currency behavior with the rule owner.
• Benefit/acceptance: null/empty/whitespace issuers cause one controlled rejection and false
eligibility, no later issuer/account checks and no automatic pipeline, including an unmatched currency fixture. The same nonblank inputs preserve existing approved rules.
Suggested JIRA: LAI-13. Remediation remains open.
LAI-14 - Do not pass Int32 to a property-based DTO query mapper
• Category: MustChange/rewrite. Checklist sections 3 and 8: Blocker. Priority: High.
• Initial location: OperacionRepositorioMt103In.cs:359 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103In.cs#L359).
• Current code:
Code block (csharp)
await EjecutarStoredProcedureQuery<int>(ConstantesServicio.usp_ObtenerCodigoClientePorCuenta, cmd =>
{
    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cuenta", cuentaComision));
}, esVistasT24: true)
• Issue/impact against the inspected dependency contract: its generic query maps result columns
to writable properties of new T. Int32 has none, so each returned row remains zero instead of the actual positive client code; the downstream < 1 branch rejects the commission path. The local call, not the shared mapper implementation, is the finding.
• Fix: reuse the existing DStringGeneral.CUSTOMER mapping already used for the same SP
by OperacionRepositorioPacs008In.cs:191 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioPacs008In.cs#L191). Confirm its CUSTOMER alias against approved SQL, apply LAI-10's technical-success check and validate the actual positive code. Do not "fix" zero with another success-shaped default.
• Proposed mapping shape, using existing types and the existing account parameter:
Code block (csharp)
var resultado = await EjecutarStoredProcedureQuery<DStringGeneral>(
    ConstantesServicio.usp_ObtenerCodigoClientePorCuenta,
    cmd =>
    {
        cmd.Parameters.Add(basedatosConexion.CrearParametro("@cuenta", cuentaComision));
    },
    esVistasT24: true);
Read the successful row's CUSTOMER property only after explicit missing/invalid-row handling. A supported scalar API is an alternative only if the approved pinned dependency exposes it.
• Benefit/acceptance: using the actual approved mapper/SQL fixture, a returned client code 123
is observed as 123, never zero; missing, invalid and failed-query cases remain non-success. Pin and verify the deployed DLL before relying on the inspected-source mapping contract.
Suggested JIRA: LAI-14. Remediation remains open.
LAI-15 - Do not label manual results as automatically authorized
• Category: MustChange/rewrite. Checklist section 3: Blocker. Priority: High.
• Locations: OperacionRepositorioMt103Out.cs:123 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103Out.cs#L123),
OperacionRepositorioMt202In.cs:192 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt202In.cs#L192) and OperacionRepositorioMt202CovIn.cs:109 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt202CovIn.cs#L109).
• Current code:
Code block (csharp)
return Resultado(folio, "MT103_OUT", respuestaOperacionGeneral, true,
    respuestaOperacionGeneral ? "Enrutado a flujo manual" : "Error actualizando estados");
• Issue/impact: ProcesoManual invokes the pipeline with false but sets the result's
AutorizadoAutomaticamente field to true through the fourth Resultado argument. MT103-IN's manual path and pacs.008 use false, confirming inconsistent result metadata. This is not evidence that the manual path actually authorizes a payment.
• Fix:
Code block (csharp)
return Resultado(folio, "MT103_OUT", respuestaOperacionGeneral, false,
    respuestaOperacionGeneral ? "Enrutado a flujo manual" : "Error actualizando estados");
• Benefit/acceptance: manual success and manual state failure both have
AutorizadoAutomaticamente=false in all five supported handlers; maintain the actual pipeline argument and Procesado result. Separately clarify whether an unsuccessful automatic attempt should expose eligibility or achieved authorization; do not silently redefine that output contract in this audit.
Suggested JIRA: LAI-15. Remediation remains open.
LAI-16 - Remove raw account identifiers from local diagnostic messages
• Category: MustChange/rewrite. Checklist section 5: Blocker. Priority: High.
• Locations: OperacionRepositorioMt103In.cs:352 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103In.cs#L352),
OperacionRepositorioMt103In.cs:357 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103In.cs#L357) and OperacionRepositorioPacs008In.cs:187 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioPacs008In.cs#L187).
• Current code:
Code block (csharp)
Config.EscribirLog($"INFO|MT103_IN| Se encontró la cuenta VOSTRO {informacionOperacion.VostroBank}.",
    NombreServicio.LAIKA.ToString());
Config.EscribirLog($"INFO|MT103_IN| Se va a buscar el código de cliente para la cuenta {cuentaComision}.",
    NombreServicio.LAIKA.ToString());
• Issue/impact: cuentaComision is then submitted as the @cuenta account parameter. Raw account
material is therefore deliberately included in normal logs, contrary to the selected checklist. Actual production values, log access and retention have not been inspected; no leak incident or external exploit is claimed.
• Effective logging check: Config.EscribirLog defaults masked=false and the current FileLogger's
automatic sanitizer recognizes keyed parameter/KEY=VALUE forms, not these free-prose messages. Four exact-method dependency-context probes confirmed the three prose forms retain a dummy sentinel and the keyed control is masked. The deployed logger/version still requires GEN-1.
• Fix: omit account/customer payloads and report lookup outcome plus an approved correlation
reference. Mask/tokenize only under the organization's explicit policy; do not assume that last-four masking is automatically sufficient.
• Proposed message shape:
Code block (csharp)
Config.EscribirLog("INFO|MT103_IN| Se encontro la cuenta VOSTRO requerida.",
    NombreServicio.LAIKA.ToString());
• Benefit/acceptance: sentinel full account/VOSTRO values never appear in captured success,
rejection or exception-path diagnostics; approved operational correlation is retained. Three explicit account/VOSTRO-emitting statements were confirmed across the two handlers.
• Cross-cutting parent: GEN-5, local financial-identifier variant; no credential literal
or defect located inside the shared logger is reported here.
Suggested JIRA: LAI-16. Remediation remains open.
LAI-17 - Validate the pacs.008 VOSTRO split before indexing
• Category: MustChange/rewrite. Checklist sections 3-4: Blocker. Priority: High.
• Location: OperacionRepositorioPacs008In.cs:189 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioPacs008In.cs#L189).
• Current code:
Code block (csharp)
var cuentaPaso = informacionOperacionEntrada.VostroBank.Split('-');
var cuentaComision = cuentaPaso[2];
• Issue/impact: the preceding guard checks only whitespace. A nonblank one/two-part value
throws IndexOutOfRangeException before the controlled missing-client outcome. MT103-IN already checks Length before taking the same third component.
• Fix shape, using the existing explicit rejection/error path:
Code block (csharp)
if (cuentaPaso.Length < 3 || string.IsNullOrWhiteSpace(cuentaPaso[2]))
{
    await _rechazo.RechazarMensajeConRazon(folio,
        "No se encontro una cuenta de comision valida.", true);
    return false;
}
Coordinate any shared extraction with LAI-18 and the approved VOSTRO schema; do not invent a new restriction on numeric width/leading zeros or extra components.
• Benefit/acceptance: one-part, two-part and blank-third-component fixtures do not throw or
query an undefined account; an approved three-part fixture preserves its complete third value.
Suggested JIRA: LAI-17. Remediation remains open.
Rewrite Recommended
LAI-8 - Use the existing work-plan interfaces instead of hidden repository construction
• Category: rewrite_recommended. Checklist section 12: Major. Priority: Medium.
• Locations: State.cs:29 (../../SWIFT/LAIKA/Infraestructura/Repositorio/State.cs#L29),
OperacionesGeneralesLaika.cs:33 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionesGeneralesLaika.cs#L33).
• Current code:
Code block (csharp)
PlanTrabajoMaestro = planTrabajoMaestro ?? new PlanTrabajoMaestro();
PlanTrabajoMaestroDetalle = planTrabajoMaestroDetalle ?? new PlanTrabajoMaestroDetalle();
Code block (csharp)
new AsignacionEstadoGeneral(automatico)
• Issue/impact: production entry constructs the state graph without supplying the two existing
interfaces, so default constructors bypass the registered/testable repository seams. Direct state tests can inject fakes but do not exercise the actual entry graph.
• Fix: constructor-inject IPlanTrabajoMaestro/IPlanTrabajoMaestroDetalle into
OperacionesGeneralesLaika and pass those same instances to AsignacionEstadoGeneral; retain its existing transition to AutorizacionEstadoGeneral with the supplied dependencies. No additional marker interface/service-locator/factory hierarchy is required.
• Proposed construction shape:
Code block (csharp)
new SwiftContext(
    folio,
    new AsignacionEstadoGeneral(automatico, _planTrabajoMaestro, _planTrabajoMaestroDetalle))
• Benefit/acceptance: entry-to-final-state tests run entirely against the same supplied fakes;
DI verification and selected repository lifetimes remain explicit. No resource-leak claim is made merely because a repository object itself lacks using/Dispose.
Suggested JIRA: LAI-8. Remediation remains open.
LAI-18 - Consolidate duplicated inbound rules and result wrappers after fixing defects
• Category: rewrite_recommended. Checklist sections 7 and 12: Major. Priority: Medium.
• Locations: OperacionRepositorioMt103In.cs:204 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103In.cs#L204)
and OperacionRepositorioPacs008In.cs:241 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioPacs008In.cs#L241); automatic/manual wrappers in all five supported handlers.
• Current structural shape:
Code block (csharp)
switch (plan.cve_moneda)
{
    // The same five currency/correspondent branches are copied in both handlers.
}
• Evidence/impact: two foreign-currency methods occupy 75/74 physical lines with five currency
cases apiece; ten wrappers repeat pipeline invocation/result construction. The duplicated missing-return, wrong-currency diagnostic and manual-flag defects show actual drift.
• Fix: lock current approved rules in tests, then reuse the existing base for common pipeline/
result behavior and extract only genuinely shared currency/BIC/account validation. Keep MT/MX retrieval, commission parsing and message-specific differences explicit; no generic workflow engine or new hierarchy is warranted.
• Proposed shared-wrapper use after its implementation in the existing base:
Code block (csharp)
return await ProcesarConEstados(folio, "MT103_OUT", automatico: false);
ProcesarConEstados is a proposed helper, not an existing symbol. Define an explicit default currency rule with the business owner: current switches have no default, but the audit does not invent which additional currencies must be allowed/rejected.
• Benefit/acceptance: the same rule table drives both supported inbound variants with their
approved differences tested; all ten result paths have correct manual/automatic metadata. Demonstrate reduced repeated branches while retaining all existing approved outcomes.
Suggested JIRA: LAI-18. Remediation remains open.
4.3 LaikaAutomator host (Tier 2) - complete: 6 files / 411 lines
MustChange / Rewrite
LAI-19 - Give the application the configuration that startup actually populates
• Category: MustChange/rewrite. Checklist sections 4 and 12: Blocker. Priority: High.
• Local locations: ServiceHost.cs:91 (../../SWIFT/LAIKA/LaikaAutomator/ServiceHost.cs#L91)
registers a singleton clone, line 120 verifies/instantiates registrations, line 136 updates only the original; OperacionServicio.cs:86 (../../SWIFT/LAIKA/Aplicacion/OperacionServicio.cs#L86) casts the injected object's nullable PID while sending an alert.
• Current code:
Code block (csharp)
_containerDI.Register<ConfiguracionServicio>(
    () => (ConfiguracionServicio)_configuracionBase.Clone(), Lifestyle.Singleton);
_containerDI.Verify();
Code block (csharp)
_configuracionBase.Pid = nuevaConfig.Pid;
_configuracionBase.NombreProceso = nuevaConfig.NombreProceso;
_configuracionBase.NombreDisplay = nuevaConfig.NombreDisplay;
• Issue/impact: Verify creates the singleton while PID/process/display are still null.
Parameterization subsequently changes a different object. The inspected base guard prefers the host's _configuracionBase, so it can succeed while the application's injected clone remains stale. On the outer alert path, (int)_configuracion.Pid then throws and masks the original failure. This is a service-local break in effective metadata propagation, not a claim that the shared startup guard is missing.
• Verification semantics are documented by
Simple Injector 5 (https://docs.simpleinjector.org/en/latest/howto.html#verify-configuration): Verify resolves an instance for every registration.
• Simplest coordinated fix shape:
Code block (csharp)
_containerDI.RegisterInstance<ConfiguracionServicio>(_configuracionBase);
Populate that same instance before processing, and validate the actual consumer-visible PID/names. Keep intentionally scoped Configuracion snapshots separate; do not merely replace null PID with zero or disable container verification.
• Benefit/acceptance: with fake startup metadata PID=123 and nonblank names, perform Verify,
parameterization and a later application resolution; the resolved configuration has those exact values. A deliberately failing queue sends an alert with PID=123 and preserves the original failure, without live I/O. Invalid metadata prevents the first processing tick.
• Cross-cutting parent: GEN-8, scoped to the local stale-clone misuse. Bind the approved
dependency version first; no deployed binary behavior has been claimed without that artifact.
Suggested JIRA: LAI-19. Remediation remains open.
LAI-22 - Preserve configured log-size units at the host boundary
• Category: MustChange/rewrite. Checklist sections 3, 7 and 11: Major. Priority: High.
• Local location: ServiceHost.cs:78 (../../SWIFT/LAIKA/LaikaAutomator/ServiceHost.cs#L78).
• Current code:
Code block (csharp)
Logging.Init(rutas, servicioMainConfiguracion.NombreServicio,
    servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
• Issue/impact against the inspected API: the settings reader supplies a KB field without unit
conversion, but Logging.Init's third parameter is maxMb and multiplies it by 1024 before assigning LoggerOptions.TamanoMaximoKB. A 10,240-KB fixture therefore becomes 10,485,760 KB (10 GB rather than 10 MB), a factor of 1024 larger. This weakens the requested rotation bound; no production disk-fill incident or actual deployed threshold was measured.
• Fix: confirm the approved configuration/API unit contract and make conversion explicit at
this service boundary. Prefer a supported KB-native initialization contract if the pinned library exposes one; otherwise use a validated MB value with documented rounding/range rules. Do not introduce silent truncation, a new default, or unchecked multiplication/overflow.
• Example only when approved configuration is restricted to whole positive MB multiples:
Code block (csharp)
var maxLogMb = maxLogKb / 1024; // after explicit positive/multiple/range validation
Logging.Init(rutas, servicioMainConfiguracion.NombreServicio, maxMb: maxLogMb);
The whole-MB restriction is not assumed to be the existing business contract; confirm before applying this shape. This is a local unit mismatch, not a request to modify shared code here.
• Benefit/acceptance: a 10,240-KB fixture produces a 10,240-KB effective logger limit; explicitly
test zero, negative, sub-MB/non-multiple and maximum supported settings under the approved policy. Capture initialization options without writing/growing production log files.
Suggested JIRA: LAI-22. Remediation remains open.
4.4 PruebasUnitarias (Tier 2) - complete: 4 files / 335 lines
MustChange / Rewrite
LAI-20 - Extend the real tests to critical routing, authorization and failure cases
• Category: MustChange/rewrite. Checklist section 13: Major. Priority: High.
• Locations: LaikaUT.cs:61 (../../SWIFT/LAIKA/PruebasUnitarias/LaikaUT.cs#L61),
LaikaUT.cs:86 (../../SWIFT/LAIKA/PruebasUnitarias/LaikaUT.cs#L86) and LaikaUT.cs:148 (../../SWIFT/LAIKA/PruebasUnitarias/LaikaUT.cs#L148).
• Current final-pipeline assertion shape:
Code block (csharp)
planMaestroMock.Verify(x => x.ActualizamosElEstatusParaOperacion(
    It.IsAny<string>(),
    It.IsAny<EPlanTrabajoMaestroEstatus>(),
    It.IsAny<EPlanTrabajoMaestroEstatus>(),
    It.IsAny<string>()), Times.Exactly(2));
• Issue/impact: three useful happy-path tests exist, but none invokes the routing factory or
any concrete payment-eligibility handler. Negative data, query/mutation failure, cancellation, manual result metadata, parser boundaries and startup configuration propagation have no authored regressions. Counting two writes does not prove the exact authorization transition.
• Fix: preserve the useful tests; add focused, parameterized regressions alongside each
remediation and exact ordered work-plan assertions. Separate isolated unit tests from any approved nonproduction database/mapper contract tests, and isolate static log setup if parallel execution is later enabled.
• Proposed stronger authorization assertion using existing symbols:
Code block (csharp)
planMaestroMock.Verify(x => x.ActualizamosElEstatusParaOperacion(
    folio,
    EPlanTrabajoMaestroEstatus.X_AUT,
    EPlanTrabajoMaestroEstatus.AUT,
    "TOMI"), Times.Once());
• Benefit/acceptance: independently verify X_ASR -> X_AUT -> AUT and persisted detail IDs
3 -> 4 -> 5, no authorization after invalid/missing prerequisites, all six routing outcomes plus missing key, three corrected manual flags, parser terminal variants, fake failed helper results and consumer-visible PID. Add bounded/cancelled queue tests as LAI-3/7 are implemented. Execute on the pinned x86/.NET Framework 4.8 toolchain; publish actual results and measured coverage rather than claiming a target percentage without a run.
• Cross-cutting parent: GEN-2. This is missing protection on critical branches, not an
assertion of zero existing tests or zero effective behavior verification.
Suggested JIRA: LAI-20. Remediation remains open.
4.5 Root support - complete: 3 files / 621 lines
Rewrite Recommended
LAI-21 - Replace the starter README with an owner-verified operating runbook
• Category: rewrite_recommended. Checklist sections 11 and 13: Major. Priority: Medium.
• Location: README.md:5 (../../SWIFT/LAIKA/README.md#L5).
• Current content:
Code block (text)
Getting started
To make it easy for you to get started with GitLab...
Use the built-in continuous integration in GitLab.
• Issue/impact: the repository describes generic GitLab onboarding, not its actual x86/.NET
Framework build prerequisites, approved external configuration/dependencies, service identity, queue recovery or safe deployment/rollback. Its CI suggestions are not a checked-in pipeline. A new operator cannot distinguish safe recovery from a possible duplicate/replayed item.
• Fix: have the service/database/operations owners supply and verify the actual procedures.
Link the architecture/recommendation evidence and approved package source, but keep credentials, production connection strings and payment payloads out of source.
• Proposed runbook shape:
Code block (markdown)
## Build and prerequisites
## Configuration and service identity
## Safe start, stop and deployment
## Queue claim, recovery and reconciliation
## Logs, alerts, rollback and support ownership
• Benefit/acceptance: a new teammate follows the instructions successfully on an approved
nonproduction environment, including clean x86 build, invalid-configuration failure, bounded stop, fault/retry reconciliation and rollback. Explicitly mark owner inputs not yet supplied rather than inventing service accounts, paths, retention periods, SLAs or retry rules.
Suggested JIRA: LAI-21. Remediation remains open.
Deferred Contract Questions, Not Additional Findings
Question	Why it remains open
Queue removal, acknowledgement, retry and reconciliation	SQL source is absent; procedure names/comments do not establish delivery guarantees
Approved dependency/deployed behavior	Bind the inspected source contracts to exact approved binaries; no live version was inspected
Account identifier representation	Confirm schema/range/leading-zero contracts before proposing a numeric-to-string change
Currency/BIC and commission vocabulary	Confirm other-currency policy, aliases and normalized MX charge codes before tightening validation or merging MT/MX rules
Failed automatic-result semantics	Decide whether the output flag means eligibility or achieved authorization; the confirmed manual-true bug is separate
Log-size policy	Confirm KB/MB units, supported non-MB increments, range/rounding and deployed logger version before changing configuration behavior
Work-plan atomicity and recovery	Two client calls alone do not prove or disprove database-level atomicity; obtain SQL and fault-injection evidence

5. Architecture-Fit Note
Apply the standing architecture trade-offs (./_ORCHESTRATION.md#6-standing-architecture-question-is-the-current-hostingarchitecture-model-right) to this observed workload: a timer-driven, SQL-queue-backed SWIFT automation worker with a useful application/infrastructure split and existing DI. Keeping the Windows Service while fixing verified defects, reproducible dependencies and tests is the lowest-assumption first step. No full rewrite or microservices migration is justified by this exhaustive source audit.
LAIKA option	Fit based on known behavior	Decision needed before change
Modernize in place	Preserves timer, database contracts and current deployment	Exact shared DLL/framework compatibility, x86 requirements, operations support
Supported-LTS Worker Service	Could retain the same orchestration boundaries after correctness/test work	Hosting policy, library portability, service lifecycle and rollback verification
Event/message-driven or bounded scheduled execution	Potential alternative only after queue ownership/delivery semantics are explicit	Retry/acknowledgement, idempotency, workload duration, SLAs, infrastructure cost
Workflow/saga or full microservices split	Not established as necessary by the reviewed solution	Real compensation/team-scaling requirement, measured benefit, budget and migration plan

Unknown SLAs, volume, hosting constraints and budget are not grounds for choosing one architecture. Sequence: verified defects/build inputs -> regression tests and justified duplication cleanup -> measured lifecycle/queue contracts -> separately scoped hosting evaluation.
6. JIRA-Ready Backlog
These are suggested local identifiers, not tickets created in JIRA. Every finalized finding appears once and belongs to exactly one category.
ID	Title	Category	Project	File(s)	Priority	Acceptance criterion
LAI-1	Restore approved reproducible dependencies; GEN-1	MustChange/rewrite	All projects	Aplicacion.csproj:38 (../../SWIFT/LAIKA/Aplicacion/Aplicacion.csproj#L38)	Blocker	Clean approved x86 agent resolves the exact pinned graph and builds without machine-populated DLL paths
LAI-2	Harden the latent null-result contract	rewrite_recommended	Aplicacion	OperacionServicio.cs:60 (../../SWIFT/LAIKA/Aplicacion/OperacionServicio.cs#L60)	Medium	Null/false/true fixtures are explicit; next folio still runs; no false success
LAI-3	Propagate cooperative cancellation	rewrite_recommended	Aplicacion and consumers	IOperacionServicio.cs:14 (../../SWIFT/LAIKA/Aplicacion/Interfaces/IOperacionServicio.cs#L14)	Medium	Stop-before-work, between-items and supported I/O cancellation preserve defined recovery
LAI-4	Establish approved identifier vocabulary	rewrite_recommended	Aplicacion and adapters	SwiftMensajeResolvedor.cs:18 (../../SWIFT/LAIKA/Aplicacion/Enums/SwiftMensajeResolvedor.cs#L18)	Medium	Approved alias/version/case/FT-prefix/unknown matrix has deterministic results
LAI-5	Clean interface spelling and imports	NiceToRewrite_ifTimePermits	Aplicacion	IContorlServiciosAutomatizado.cs:9 (../../SWIFT/LAIKA/Aplicacion/Interfaces/IContorlServiciosAutomatizado.cs#L9)	Low	Tool-assisted refactor preserves external compatibility and mapped DTO names
LAI-6	Control missing message-key outcomes	MustChange/rewrite	Infraestructura	SwiftAutomatizacionFactory.cs:74 (../../SWIFT/LAIKA/Infraestructura/Repositorio/SwiftAutomatizacionFactory.cs#L74)	High	Empty lookup is explicit; direct invalid null input never yields a CLR dereference
LAI-7	Bound queue acquisition	MustChange/rewrite	Infraestructura	OperacionRepositorioLaikaQueue.cs:35 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioLaikaQueue.cs#L35)	High	A never-empty fake with N=2 yields two items then starts processing; cancellation is tested
LAI-8	Inject work-plan seams into the entry graph	rewrite_recommended	Infraestructura	State.cs:29 (../../SWIFT/LAIKA/Infraestructura/Repositorio/State.cs#L29)	Medium	Entry-to-final-state tests use supplied fakes without constructing concrete repositories
LAI-9	Guard nullable precondition text	MustChange/rewrite	Infraestructura	PrecondicionCheck.cs:36 (../../SWIFT/LAIKA/Infraestructura/Repositorio/PrecondicionCheck.cs#L36)	High	Missing/short intermediary and null currency have explicit non-eligible outcomes
LAI-10	Enforce database outcome contracts; GEN-11	MustChange/rewrite	Infraestructura	PlanTrabajoMaestro.cs:81 (../../SWIFT/LAIKA/Infraestructura/Repositorio/PlanTrabajoMaestro.cs#L81)	High	Failed reads/writes cannot become healthy emptiness, valid state or successful processing
LAI-11	Parse terminal MT fields without block 5	MustChange/rewrite	Infraestructura	SwiftUtilidades.cs:38 (../../SWIFT/LAIKA/Infraestructura/Repositorio/SwiftUtilidades.cs#L38)	High	Final OUR/SHA/BEN, optional trailer, line endings, missing tags and narrative cases pass
LAI-12	Preserve causal exceptions and accurate labels	MustChange/rewrite	Infraestructura	SwiftInMtComun.cs:55 (../../SWIFT/LAIKA/Infraestructura/Repositorio/SwiftInMtComun.cs#L55)	High	All 16 rethrows preserve origin/identity; four EUR/GBP reason labels are correct
LAI-13	Stop after rejecting an absent issuer	MustChange/rewrite	Infraestructura	OperacionRepositorioMt103In.cs:210 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103In.cs#L210)	High	Both inbound variants reject once and do not proceed to automatic processing
LAI-14	Reuse the supported client-code DTO	MustChange/rewrite	Infraestructura	OperacionRepositorioMt103In.cs:359 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103In.cs#L359)	High	An approved mapper fixture returning 123 is observed as 123, not zero
LAI-15	Correct three manual authorization flags	MustChange/rewrite	Infraestructura	OperacionRepositorioMt103Out.cs:123 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103Out.cs#L123)	High	Manual success/failure across five handlers always reports automatic=false
LAI-16	Remove raw account diagnostics; GEN-5	MustChange/rewrite	Infraestructura	OperacionRepositorioMt103In.cs:357 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioMt103In.cs#L357)	High	Full dummy account/VOSTRO values never reach captured diagnostics; approved correlation remains
LAI-17	Validate pacs.008 VOSTRO components	MustChange/rewrite	Infraestructura	OperacionRepositorioPacs008In.cs:189 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioPacs008In.cs#L189)	High	One/two/blank-third-part fixtures do not throw; approved third component is preserved
LAI-18	Consolidate tested inbound policy/wrappers	rewrite_recommended	Infraestructura	OperacionRepositorioPacs008In.cs:241 (../../SWIFT/LAIKA/Infraestructura/Repositorio/OperacionRepositorioPacs008In.cs#L241)	Medium	Shared rules replace repeated branches while approved MT/MX differences remain covered
LAI-19	Repair consumer-visible startup configuration; GEN-8	MustChange/rewrite	Host	ServiceHost.cs:91 (../../SWIFT/LAIKA/LaikaAutomator/ServiceHost.cs#L91)	High	Verify then fake parameterization exposes PID=123/names to the application and its alert
LAI-20	Add critical routing/failure regressions; GEN-2	MustChange/rewrite	PruebasUnitarias	LaikaUT.cs:148 (../../SWIFT/LAIKA/PruebasUnitarias/LaikaUT.cs#L148)	High	Exact ordered transitions and concrete eligibility/routing failures are tested on the pinned toolchain
LAI-21	Supply an owner-verified operating runbook	rewrite_recommended	Root support	README.md:5 (../../SWIFT/LAIKA/README.md#L5)	Medium	New operator reproduces nonproduction build, configuration failure, stop/recovery and rollback
LAI-22	Preserve log-size units at initialization	MustChange/rewrite	Host	ServiceHost.cs:78 (../../SWIFT/LAIKA/LaikaAutomator/ServiceHost.cs#L78)	High	A 10240-KB fixture produces a 10240-KB effective limit; invalid/range/rounding policy is explicit

Acceptance and Action Gates
Gate	Evidence required
Build provenance	Exact approved dependency versions/feed, resolved graph and successful clean-agent build, not a locally copied DLL
Null-result correction	Null/false/true-result regression tests, continuation to the next folio, no successful-processing log on missing result
Cancellation	Stop-before-work/between-items/I/O tests, normal-cancellation handling, defined atomic-work completion/recovery
Parsing	Owner-approved vocabulary/aliases and backward-compatible unknown/version/case/whitespace test matrix
Cleanup	Separate tool-assisted refactoring, all references updated, build/tests unchanged, DTO mapping preserved
Database outcomes	Failure, successful-empty, zero-row/conflict and positive write cases are distinguishable; SQL atomicity/recovery is owner-proven
Authorization/data boundaries	Terminal rejection, safe missing/split input and approved scalar/commission mapping have exact regressions
Logging/configuration	No raw account material, consumer-visible valid PID/names and exact approved rotation-size units
Complete audit	All 88 source ranges, 22 unique findings/backlog rows and the final Markdown/Word content reconcile; this gate does not implement remedies

Release and Assurance Position
Production readiness and numeric quality scores are not assessed by this source audit. The completed source review does not approve deployment without build/runtime/database evidence. That is an assurance boundary, not a declaration that the existing production deployment is broken. Security, performance, scalability and test coverage are not assigned invented /10 ratings.
Database-owner contracts, approved build tooling/dependency provenance and safe executed regressions are required before operational assurance. No broad modernization should precede the scoped correctness/dependency work. All 22 remediation items remain open.
Handover and Word Deliverable
Use the handover playbook (./_LAIKA_HANDOVER_PLAYBOOK.md) and prompt history (./_LAIKA_PROMPT_HISTORY.md). Verify hashes before reusing evidence or implementing separately authorized fixes; re-review only affected source when the snapshot changes.
The dated companion is LaikaAutomator_Recommendations_10142026.docx (./LaikaAutomator_Recommendations_10142026.docx). Markdown is the source of truth; regenerate the companion after any report change. Package/XML, heading/table/finding/text correspondence and freshness checks are recorded in the progress tracker. Desktop Word pagination/visual rendering is a separate unverified check, not asserted from the OOXML package. No public conversion service is used.
