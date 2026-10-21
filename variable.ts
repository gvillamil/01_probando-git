SPID CEP GEN CDA Recommendations
SPID_CEP_GEN_CDA - Architecture and Code Quality Recommendations
Date: 2026-10-14. Complete source audit; remediation remains open.
Solution: SPID_CEP_GEN_CDA.sln (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA.sln). Compared against the approved LoansApp practices baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and review/severity checklist (../BaseLine_Docs/code_review.md). Portfolio methodology and standing findings: _ORCHESTRATION.md (./_ORCHESTRATION.md). Companions: Word report (./SPID_CEP_GEN_CDA_Recommendations_10142026.docx), handover (./_SPID_CEP_GEN_CDA_HANDOVER_PLAYBOOK.md), and validation record (./_SPID_CEP_GEN_CDA_Validation.md).
Executive conclusion: preserve the useful layering and existing tests. First establish a reproducible dependency baseline, then fix outcome handling, failure tracking, cleanup and SQL contracts. Do not commission a full rewrite from this audit alone. There are 17 findings: 12 MustChange/rewrite, four rewrite_recommended and one NiceToRewrite_ifTimePermits; checklist severities are one Blocker, 15 Major and one Minor. These are technical audit findings, not an assertion that 17 production incidents occurred.
1. Solution Overview
Dependency order
Tier	Project / kind	Files / lines	Internal dependencies
0	Aplicacion (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Aplicacion.csproj), application library	14 / 779	None
1	Infraestructura (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Infraestructura.csproj), SQL adapters	4 / 192	Aplicacion
1	PruebasUnitarias (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/PruebasUnitarias.csproj), MSTest/Moq	6 / 1,399	Aplicacion
2	SPID_CEP_GEN_CDA (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA.csproj), Windows-service executable	7 / 401	Aplicacion, Infraestructura
Support	SQL (../../SPEI_SPID/SPID_CEP_GEN_CDA/SQL) and repository support	9 / 945	Runtime contracts and build/repository metadata

All projects target .NET Framework 4.8 and configure x86; the solution's Any CPU labels do not override those project settings. Four direct project-reference edges form an acyclic graph. Tier 0 means no internal reference, not that external dependencies are available. See the dependency ladder (../excecutive_docs/SPID_CEP_GEN_CDA/Base_Project_Dependency_Ladder.md) and visual map (../excecutive_docs/SPID_CEP_GEN_CDA/Visual_Dependency_Map.md).
What this solution actually does
This is a periodically invoked SPID CEP/CDA integration worker, not an HTTP service or an observed message-queue listener. The shared Windows-service base awaits initialization and serializes ticks. SimpleInjector resolves a fresh scoped configuration/application graph. Each tick records execution, obtains business date/day and required parameters, and checks the operating window. It concurrently reads pending TOMI work and received SPID participant operations, awaits both reads, combines their results and processes records sequentially.
The normal ENT_SPID handler obtains T24 authorization date/time; the ENT_SPID_PART handler does not supply those overrides. Both call the existing sp_InterfaceMW_SPID_CEP, attempt process completion and source acknowledgement, and currently also mark the service idle inside the per-record handler. The orchestrator catches per-record errors, records/alerts them and continues; a finally block attempts tick finalization. Host shutdown separately waits for startup/work and attempts registration/state cleanup before disposing DI.
TOMI is a logical connection identity; the three supplied procedure definitions use physical database InterfacesT24. SPID legacy procedure definitions, deployed configuration and schema equivalence were not supplied. The source is payment-adjacent; direct movement of funds was not established. Full functional/technical context is in the understanding document (../architecture/SPID_CEP_GEN_CDA/Application_understanding_technical_document.md).
2. Scope and Methodology Note
Every line of every in-scope tracked file was read and formally reviewed: 40/40 files, 3,716/3,716 physical lines; no sampling. Coverage and findings were persisted after each project/support checkpoint in the review ledger (./_SPID_CEP_GEN_CDA_ExhaustiveReview_Progress.md). The SHA256 manifest (./_SPID_CEP_GEN_CDA_FileManifest.csv) records each relative path, group, line count, byte count and hash.
Scope	Files	Physical lines	Formal status
Application	14	779	Complete
Infrastructure	4	192	Complete
Tests	6	1,399	Complete
Host	7	401	Complete
SQL	4	375	Complete
Repository support	5	570	Complete
Total	40	3,716	Complete

Primary snapshot: branch CodeReview, HEAD 5ca26ae78a3108078374157bdd201c1cc44014f2, initially clean. The physical inventory also contained 122 Git metadata files and 22 ignored generated obj files; these are excluded, not falsely counted as reviewed source. No application source, project, configuration or SQL was changed; no commit was created. The user authorized continuation with "Resume until all finished" after the application checkpoint. That overrides the checkpoint cadence, not the selected-solution scope.
The approved checklist supplies applicable technical rules/severities, while the architecture skill supplies whole-solution scope and this report format. No linked story or acceptance criteria were supplied by agreement. Business-rule correctness, legacy fidelity and release approval are therefore unverified. The baseline's blank evidence/count sections are not treated as measured evidence about its reference repository.
Current shared MUFG_T24 source was read only to verify relevant contracts. Finding locations are all inside this selected repository. Its observed HEAD was d1e49a01b607550491f4b08c78d6f17ddc5725e5; this is provenance, not proof of an equivalent deployed binary or an exhaustive review of that separate repository. Code below illustrates recommended changes, not applied or compiled patches; integrate them with the approved dependency version and existing logging/types during implementation.
Validation and evidence limits
Check	Result / limitation
Source coverage, snapshot, links, findings/backlog, graph and Word structure	Final machine-check evidence is in the validation record (./_SPID_CEP_GEN_CDA_Validation.md)
Isolated actual-source mapper probe	Explicit 0 -> 0, explicit 7 -> 7, DBNull -> 0; reproduces FR-11, not a passing release test
Operation-type width probe	Actual constant length 13 versus actual SQL capacity 10; reproduces FR-16's static contract mismatch, not executed SQL
Existing MSTest execution	Requested for all three test files; tool returned "No tests found in the files". No suite pass, executed-case count or coverage claimed
Full build / restore	Not performed: configured shared/package DLLs and standard 4.8 targeting pack absent; no dotnet/MSBuild/csc on PATH. Legacy Framework MSBuild presence is not an SDK-style build result
Service, database, deployment, load/SCA and Word visual rendering	Not executed/verified; no external system contacted

The snapshot-checked safe probes (./_SPID_CEP_GEN_CDA_SafeProbes.ps1) compile only the actual small DTO/mapper in memory, use DataTable readers and compare source metadata. They do not load the production dependency assembly or execute service/database/network work.
3. What's Already Good
Strength	Evidence and behavior to preserve
Useful application ports and constructor injection	orchestrator:39 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:39), normal handler:27 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:27); do not add interfaces merely to increase their number
Explicit composition and per-tick scope	host:54 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:54), host:130 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:130); container verification and scoped configuration clones
Independent reads awaited before sequential dispatch	orchestrator:125 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:125); the two post-WhenAll Result reads are not blocking sync-over-async defects
Required metadata/null checks exist	normal handler:91 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:91); neither DTO null handling nor an uninitialized PID bug should be invented
Parameterized adapters and permitted legacy integration	SPID adapter:51 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseSpidDao.cs:51); retain the existing allowed procedure and cancellation-aware signatures
Local mutation SQL exposes a real outcome	acknowledgement SQL:34 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SQL/usp_SpidGenCda_MarcarOperacionProcesada.sql:34); XACT_ABORT, transaction/rollback, affected-row result and THROW
Real tests, not placeholders	normal tests:22 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPIDUT.cs:22), participant tests:22 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPID_PARTUT.cs:22), orchestrator tests:31 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/OperacionServicioUT.cs:31); 13 method annotations, several negative paths and call/alert assertions
Repository/build hygiene	ignore rules (../../SPEI_SPID/SPID_CEP_GEN_CDA/.gitignore) exclude generated output/packages; no generated obj files were tracked or counted as source

The shared base serializes same-instance ticks and awaits initialization. Required system keys are validated by the current helper; the business-day nullable value follows assignment from a non-nullable Boolean. These resolved candidates are not extra findings. Source guards do not prove deployed-binary equivalence.
4. Findings by Project
Each ID has exactly one category and one backlog row. Repeated occurrences share a ticket; cross-project applications of FR-1/6 are not counted again. Categories express remediation intent; severity uses the approved Blocker/Major/Minor checklist.
4.1 Aplicacion - Tier 0
MustChange/rewrite
SPID-CDA-FR-6 - Pin and reproducibly distribute the shared dependency
Category: MustChange/rewrite; Severity: Blocker; standing concern: GEN-1. Evidence: application project:11 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Aplicacion.csproj:11), infrastructure project:15 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Infraestructura.csproj:15), test project:60 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/PruebasUnitarias.csproj:60), host project:43 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA.csproj:43).
Code block (xml)
<HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
Four projects depend on an unversioned machine file absent locally. No portfolio NuGet config or matching local MUFG package archive was found; no remote feed was contacted. The DLL's API and inherited behavior cannot be established from a mutable file path. Recommend: publish/use an approved versioned internal package, replace all four file references consistently, restore the existing declared third-party packages, and pin compatible consumer/library versions. Do not guess a package version or copy an arbitrary DLL. The intended project shape is:
Code block (xml)
<PackageReference Include="MUFG_T24" Version="APPROVED_PINNED_VERSION" />
The placeholder is not a real package/version claim; accommodate classic project packaging with the repository's supported NuGet tooling. Benefit/acceptance: an approved clean agent, with the required SDK/4.8 targeting pack and feed access, restores and builds x86 without D-drive files and runs the tests. Verify source/deployed contract equivalence. JIRA: SPID-CDA-FR-6.
SPID-CDA-FR-1 - Honor Boolean and typed outcomes before reporting success
Category: MustChange/rewrite; Severity: Major; rules: result handling and truthful diagnostics, checklist sections 5/8; standing concern: GEN-9. Evidence: normal acknowledgement:82 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:82), participant acknowledgement:79 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID_PART.cs:79), tick finalization:246 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:246), host registration:113 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:113) and host shutdown:165 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:165).
Code block (csharp)
await _detallesCiseDao.MarcarOperacionProcesadaAsync(folio, tipoOperacion, ct);
// The handler then logs completion and returns true, even if acknowledgement was false.
Across the solution, 19 outcomes are discarded: 11 typed process/service results plus eight Booleans (two acknowledgements, four alerts, two port updates). Current helpers can return failure without throwing; merely awaiting them is not success. The execution-start helper itself checks failure and throws, so it is not a twentieth silent-failure site. The tick Boolean is also excluded because expected schedule skips return false. Recommend: check every declared outcome, retain the cause/correlation, and only log success after required outcomes succeed:
Code block (csharp)
bool confirmada = await _detallesCiseDao.MarcarOperacionProcesadaAsync(
    folio, tipoOperacion, ct).ConfigureAwait(false);
if (!confirmada)
    throw new InvalidOperationException("No se pudo confirmar la operacion procesada.");
For typed results, check Exito and a missing result, not mandatory Afecto==true where the nonquery contract does not populate it. Share a small concrete validator if useful. Startup must not poll after failed registration; shutdown must attempt its required cleanup and report failed updates rather than unconditional database success. Preserve a primary error if reporting or cleanup also fails. Benefit/acceptance: false, failed-result, exception and cancellation tests for all boundary types; failed acknowledgement cannot produce handler success. Safe retry after an earlier SPID insertion still requires the legacy idempotency/recovery contract; result checking alone is not a distributed transaction. JIRA: SPID-CDA-FR-1.
SPID-CDA-FR-2 - Give tick finalization a bounded independent token
Category: MustChange/rewrite; Severity: Major; rules: cancellation and resource/state lifetime, checklist sections 5/6. Evidence: orchestrator:246 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:246).
Code block (csharp)
await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);
The finally block reuses the cancelled tick token. The current dependency checks cancellation before database work, so cancellation can prevent the very cleanup meant to release execution state. Recommend: use an independent, approved finite cleanup budget; inspect its typed outcome under FR-1 and surface cleanup failure without erasing the primary exception.
Code block (csharp)
using (var cleanup = new CancellationTokenSource(cleanupBudget))
{
    var resultado = await _controlServicio.ActualizaTerminacion(
        _configuracion.NombreServicio, cleanup.Token).ConfigureAwait(false);
    ExigirExito(resultado, "No se pudo finalizar el servicio.");
}
cleanupBudget must be the agreed finite operational limit; it is not a new arbitrary default. Benefit/acceptance: cancelling before/during work still attempts cleanup with a live token, the attempt is bounded, its failure is visible and the original processing error is retained. Do not use an unlimited CancellationToken.None cleanup. JIRA: SPID-CDA-FR-2.
SPID-CDA-FR-3 - Persist insertion rejection as a process failure
Category: MustChange/rewrite; Severity: Major; rules: explicit failure handling, checklist sections 3/5. Evidence: normal rejection:59 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:59), participant rejection:56 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID_PART.cs:56), false-result branch:183 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:183).
Code block (csharp)
if (!ordenPagoInsertada)
    return false;
Both handlers return false for a nonzero insertion result after starting process tracking. The caller logs that return but bypasses its process-error update, leaving an unexplained incomplete process. Recommend: send rejection through the same checked process-failure path as an exception, retaining folio/type and available legacy rejection details:
Code block (csharp)
if (!ordenPagoInsertada)
    throw new InvalidOperationException("La insercion CDA fue rechazada.");
An explicit failure-result handler is equally valid and can retain richer details. Preserve per-record continuation policy. The unknown-operation test expects a true completed tick despite a recorded item error; do not silently redefine that Boolean as all-records-succeeded. Benefit/acceptance: a nonzero result records a failed process once, never acknowledges/ finishes that record as successful, and permits the next record under the agreed policy. JIRA: SPID-CDA-FR-3.
SPID-CDA-FR-4 - Keep process error updates on the plan-service identity
Category: MustChange/rewrite; Severity: Major; rules: consistent persistence keys, checklist sections 3/8. Evidence: error DTO:199 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:199); compare normal start:44 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:44) and participant start:45 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID_PART.cs:45).
Code block (csharp)
Servicio = _configuracion.NombreServicio // error path
The handlers track the plan using ServicioPlan, while error tracking uses NombreServicio. The shared SQL detail key includes servicio. If those configured identities differ, the error update can target no row or the wrong identity. Recommend the same approved plan key:
Code block (csharp)
Servicio = _configuracion.ServicioPlan
Keep runtime service identity for service-state/alert records; do not globally replace it. Benefit/acceptance: a test with deliberately different names verifies matching start/finish/error plan keys and checked outcomes. JIRA: SPID-CDA-FR-4.
SPID-CDA-FR-5 - Finalize service state once, after the whole tick
Category: MustChange/rewrite; Severity: Major; rules: lifecycle ownership, checklist sections 3/6. Evidence: normal handler:81 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:81), participant handler:78 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID_PART.cs:78), batch finally:246 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:246).
Code block (csharp)
await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, ct);
// Executed inside each successful record, before later records in the same batch.
The shared termination path writes idle state. A handler therefore marks a still-running batch idle, then the batch finally repeats termination. Recommend: retain record-specific TerminaProceso/acknowledgement but remove service-level termination from both handlers:
Code block (csharp)
// Service-level termination belongs only to the tick's checked, bounded finally.
Host stop cleanup remains a separate lifecycle event. Benefit/acceptance: a two-record tick stays active while the second record runs and performs one checked tick finalization after all records, including rejection/exception/cancellation paths. JIRA: SPID-CDA-FR-5.
rewrite_recommended
SPID-CDA-FR-7 - Share the repeated handler completion lifecycle
Category: rewrite_recommended; Severity: Major; rules: responsibility/duplication, checklist section 12 and the approved baseline. Evidence: normal handler:44 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:44) and participant handler:45 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID_PART.cs:45). Two implementations repeat start/insert-result/completion/acknowledgement orchestration; there are two starts, four process-finish calls, two acknowledgements and two premature handler-level service finishes. These are call-site counts, not a measured identical-line ratio.
Code block (csharp)
await _controlServicio.TerminaProceso(/* first process alias */, ct);
await _controlServicio.TerminaProceso(/* completion alias */, ct);
await _detallesCiseDao.MarcarOperacionProcesadaAsync(folio, tipoOperacion, ct);
The two process aliases may be intentional; do not delete one as a duplicate. Repeated outcome/lifetime bugs show the cost of separate completion paths. Recommend: after FR-1/3/4/5 tests exist, extract the common checked completion into one small concrete helper used by both handlers:
Code block (csharp)
await _finalizador.CompletarAsync(folio, tipoOperacion, ct).ConfigureAwait(false);
This is an extraction shape, not an existing field/API. Keep normal-only T24 lookup and participant defaults in their current handler-specific preparation. Introduce an interface only if a real testing/polymorphism boundary requires it. Benefit/acceptance: both handler paths enforce the same outcomes/order, preserve both approved plan aliases and cancellation, and keep their distinct legacy parameters. JIRA: SPID-CDA-FR-7.
SPID-CDA-FR-8 - Bound notifications for expected skipped states
Category: rewrite_recommended; Severity: Major; rules: observability without hot-loop noise, checklist section 5; standing concern: GEN-10. Evidence: non-business-day alert:75 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:75) and out-of-window alert:115 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:115).
Code block (csharp)
await _alertas.InsertarAlerta(/* expected skip notification */, ct);
Each skipped tick repeats logging/alert insertion. The current shared helper performs its procedure call without caller-state deduplication. At a hypothetical ten-second interval this is 360 attempts/hour, not a measured production count or proof about downstream deduplication. Recommend: use the existing singleton schedule-state holder to notify on reason/date/state transition, with any reminder interval explicitly agreed; retain routine skip visibility at the agreed information level. An implementation shape is:
Code block (csharp)
if (estadoCambio)
{
    // Perform the checked notification through the existing alert port.
}
Do not suppress unexpected processing failures or add a global alert cache. FR-1 still governs failed notification results. Benefit/acceptance: N identical skipped ticks cause one transition notification; date/reason/state change can notify again; critical failures bypass the skip gate. Verify actual interval and alert policy with the owner. JIRA: SPID-CDA-FR-8.
NiceToRewrite_ifTimePermits
SPID-CDA-FR-9 - Correct or remove the stale friend-assembly declaration
Category: NiceToRewrite_ifTimePermits; Severity: Minor; rules: metadata accuracy. Evidence: AssemblyInfo.cs:4 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/AssemblyInfo.cs:4); actual test assembly is PruebasUnitarias, although one source namespace also has the typo.
Code block (csharp)
[assembly: InternalsVisibleTo("PuebasUnitarias")]
The declaration does not grant access to the linked test assembly. Existing public APIs mean this is not evidence of currently failed tests. Recommend if friend access is required:
Code block (csharp)
[assembly: InternalsVisibleTo("PruebasUnitarias")]
Otherwise remove the stale declaration. Benefit/acceptance: assembly metadata matches the actual test identity and any intended internal-access test compiles. JIRA: SPID-CDA-FR-9.
4.2 Infraestructura - Tier 1
MustChange/rewrite
SPID-CDA-FR-10 - Check technical success before projecting repository results
Category: MustChange/rewrite; Severity: Major; rules: explicit data-access failure, checklist sections 5/8; standing concern: GEN-11. Evidence: SondeoDao:35 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/SondeoDao.cs:35), DetallesCiseDao:36 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseDao.cs:36), DetallesCiseDao:50 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseDao.cs:50), DetallesCiseSpidDao:38 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseSpidDao.cs:38) and DetallesCiseSpidDao:51 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseSpidDao.cs:51).
Code block (csharp)
return resultado.Data;
// Other projections: Data.FirstOrDefault(), or resultado.Afecto == true.
All five local helper calls project results without checking Exito. Current query overloads default validarAfectacion=false, initialize Data, and can return caught SQL/general failures as Exito=false. A failed poll therefore resembles no work; detail/insertion failures resemble missing data with their database cause lost. This is verified against current dependency source, not an available deployed DLL. Recommend a checked boundary before every projection:
Code block (csharp)
if (!resultado.Exito)
    throw new DataException("Fallo al consultar las operaciones SPID.", resultado.Exception);
return resultado.Data;
Preserve the original error/correlation through the existing logging path. Do not simply enable affected-row validation on polling: successful emptiness is legitimate. FR-1 separately checks returned business acknowledgements and typed mutation outcomes. Benefit/acceptance: empty successful reads stay empty; failed reads fault with their cause; failed mapping/insertion/acknowledgement cannot become success or a normal empty queue. Use an existing or narrowly useful test seam, not unnecessary interfaces. JIRA: SPID-CDA-FR-10.
SPID-CDA-FR-11 - Reject a missing mandatory insertion result
Category: MustChange/rewrite; Severity: Major; rules: external-result validation, checklist sections 3/4/8. Evidence: SPID mapper:62 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseSpidDao.cs:62).
Code block (csharp)
Resultado = lector.FieldCount > 0 && lector.GetValue(0) != DBNull.Value
    ? Convert.ToInt32(lector.GetValue(0)) : 0,
Both handlers interpret zero as insertion success. SQL NULL is silently converted into that success value. The isolated actual-source probe reproduced DBNull -> 0, with explicit zero and nonzero control rows. No database or production assembly was involved. Recommend validating the mandatory value before conversion:
Code block (csharp)
if (lector.FieldCount == 0 || lector.IsDBNull(0))
    throw new DataException("La respuesta de insercion no contiene Resultado.");
// Then map Resultado with Convert.ToInt32 and preserve the optional error fields.
Retain the verified legacy result order until its actual schema is confirmed; do not invent column names. Apply FR-10 as well, because the shared helper can catch a mapping exception and return failure that must not be reduced to missing data. Benefit/acceptance: explicit 0 succeeds, nonzero rejects, DBNull/missing/invalid mandatory data faults, and no invalid row triggers completion or source acknowledgement. The probe (./_SPID_CEP_GEN_CDA_SafeProbes.ps1) intentionally reproduces the current defect; invert its expectations in real regression tests after remediation. JIRA: SPID-CDA-FR-11.
rewrite_recommended
No additional standalone finding. FR-7 may reuse a small concrete result/completion helper; FR-6 applies to this project's dependency metadata. Do not duplicate those tickets.
NiceToRewrite_ifTimePermits
No additional standalone finding.
4.3 PruebasUnitarias - Tier 1
MustChange/rewrite
SPID-CDA-FR-12 - Stop swallowing subject and assertion failures
Category: MustChange/rewrite; Severity: Major; rules: meaningful exception handling and reliable tests, checklist sections 5/13. Evidence: normal negative test:180 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPIDUT.cs:180), normal insertion-null test:264 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPIDUT.cs:264), normal missing-folio test:438 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPIDUT.cs:438), participant negative test:161 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPID_PARTUT.cs:161).
Code block (csharp)
try { await Assert.ThrowsExceptionAsync<Exception>(() => subjectOperation()); }
catch (Exception ex) { excepcionCapturada = ex; }
All 13 test methods have catches storing unused exceptions; four enclose ThrowsExceptionAsync. They catch the assertion's own failure when no exception is thrown, so later mock verifications can leave a false-green test. Rejection tests initialized to false can also pass after an unexpected exception if their expected calls already occurred. Recommend: remove these catches from all 13 methods:
Code block (csharp)
await Assert.ThrowsExceptionAsync<Exception>(() => subjectOperation());
For expected return paths, await directly and assert the result. Assert a specific exception type/context once its public contract is established. Benefit/acceptance: substituting a no-throw subject makes each negative assertion fail; an unexpected exception fails rejection tests. Restore intended behavior and execute the suite on an approved runner. JIRA: SPID-CDA-FR-12.
rewrite_recommended
SPID-CDA-FR-13 - Make fixtures and lifecycle/failure assertions representative
Category: rewrite_recommended; Severity: Major; rules: explicit Arrange/Act/Assert contracts and regression coverage, checklist section 13. Evidence: participant dispatch fixture:316 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/OperacionServicioUT.cs:316), participant happy path:79 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPID_PARTUT.cs:79), normal happy path:79 (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPIDUT.cs:79).
Code block (csharp)
_entSpid.Setup(t => t.ProcesarOperacionCISEAsync("1006", CancellationToken.None))
    .ReturnsAsync(true);
// The participant test verifies _entSpidPart, not this configured mock.
Happy paths leave acknowledgement and typed lifecycle outcomes unconfigured. Default participant DTOs add unrelated folio-zero work. No concrete repository test exercises Exito/DBNull handling, and lifecycle ordering/cancellation outcomes are not asserted. This is not zero automated tests and no numerical coverage claim is made. Recommend configuring the intended collaborator:
Code block (csharp)
_entSpidPart.Setup(t => t.ProcesarOperacionCISEAsync("1006", CancellationToken.None))
    .ReturnsAsync(true);
Use valid explicit records or an empty second queue, configure all required success outcomes, and verify the right handler and absence of unintended dispatch. Add the targeted failure matrix for FR-1/2/3/4/5/10/11/14/16/17, including state ownership, cancellation, repository errors and isolated approved SQL fixtures. Reuse a small fixture builder for repeated setup. Preserve the tested completed-tick Boolean policy until its owner changes it. Benefit/acceptance: each supported success/failure boundary has meaningful state/call assertions; publish discovered/executed counts and coverage only after a real run. JIRA: SPID-CDA-FR-13.
NiceToRewrite_ifTimePermits
No additional standalone finding; friend-assembly metadata is consolidated in FR-9.
4.4 Windows-service host - Tier 2
MustChange/rewrite
SPID-CDA-FR-14 - Verify quiescence before clearing state and disposing DI
Category: MustChange/rewrite; Severity: Major; rules: task/resource lifetime, checklist sections 6/7. Evidence: wait sequence:161 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:161) and unconditional disposal:183 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:183).
Code block (csharp)
await _controlServicio.EsperarTareaActivaAsync(runningTask, descripcion, waitCt);
// Clear port/service state, report success, then dispose the container in finally.
The current shared wait races work against a cancelled delay; on timeout it warns and returns. Awaiting that helper therefore does not prove startup/runningTask completed. The local host can mark idle and dispose dependencies while a non-cooperative operation remains active. This finding is caller misuse, not an out-of-scope library-local ticket. Recommend: explicitly check both tasks after the bounded wait:
Code block (csharp)
bool drenado = (startupTask == null || startupTask.IsCompleted)
    && (runningTask == null || runningTask.IsCompleted);
if (!drenado)
    throw new TimeoutException("El trabajo SPID sigue activo al agotar la espera.");
Restructure the unconditional finally as well: do not clear active state or dispose dependencies still owned by running work. Surface incomplete shutdown and follow an approved forced-stop/ deferred-disposal policy; do not introduce an unlimited wait. Observe completed faults and coordinate end-to-end shutdown reporting with the shared-host owner. Benefit/acceptance: a TaskCompletionSource ignoring cancellation can outlive the wait budget without a successful-drain/idle declaration or premature disposal; completion allows normal checked cleanup. Test failure and forced-stop policies explicitly. JIRA: SPID-CDA-FR-14.
FR-1 additionally covers both port updates and the typed termination result in this host; FR-6 covers its absolute dependency. Neither is another ticket here.
rewrite_recommended
SPID-CDA-FR-15 - Make host and configuration identities explicit and consistent
Category: rewrite_recommended; Severity: Major; rules: meaningful configuration and operational contracts, checklist sections 1/11/14b. Evidence: runner:18 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/Program.cs:18), host label:36 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:36), configuration selection:67 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:67) and README (../../SPEI_SPID/SPID_CEP_GEN_CDA/README.md).
Code block (csharp)
NombreServicio.SPEI_CEP_ENV_CDA // runner/host label
NombreServicio.SPEI_CEP_GEN_CDA // requested configuration node
// Executable/project name: SPID_CEP_GEN_CDA.
The shared enum has the SPID generation identity. Lookup matches the requested XML node; it does not automatically translate SPEI to SPID, though its NombreServicio attribute can override the returned identity. The source inconsistency and implicit alias dependency are confirmed; production misrouting is not, because decrypted configuration was not inspected. Recommend: obtain the approved mapping, then consistently use the approved identity:
Code block (csharp)
private static readonly string Servicio = NombreServicio.SPID_CEP_GEN_CDA.ToString();
That is an example after approval, not permission to rename installed services/settings. If legacy aliases are intentional, encode/document and validate their rail, returned service, plan/process aliases and database mapping. Replace the generic README template with actual build/configuration/deployment/recovery instructions and links to these reports. The shared base's ServiceName assignment timing is central GEN-14 dependency context, not a duplicate local constructor/designer initialization finding. Benefit/acceptance: configuration fixtures show the approved mapping; runner/host/installation metadata agree or document supported aliases. Verify real SCM/deployment behavior separately. JIRA: SPID-CDA-FR-15.
NiceToRewrite_ifTimePermits
No additional standalone finding. An unused designer alone does not establish a runtime initialization defect when the effective base lifecycle supplies the behavior.
4.5 SQL and repository support
MustChange/rewrite
SPID-CDA-FR-16 - Preserve the complete participant operation type in SQL
Category: MustChange/rewrite; Severity: Major; rule: parameter size, checklist section 8. Evidence: acknowledgement signature:30 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SQL/usp_SpidGenCda_MarcarOperacionProcesada.sql:30), operation constant:15 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Enums/ConstantesServicio.cs:15), adapter parameter:53 (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseDao.cs:53).
Code block (sql)
@TipoOperacion VARCHAR(10)
The participant handler supplies ENT_SPID_PART, 13 characters. The ten-character SQL parameter cannot preserve the full identity before the UPDATE comparison. Against a full participant code the update cannot match; FR-1 currently ignores a false acknowledgement. The probe compared the actual constant/signature; no production row or SQL row count was read. Recommend matching the canonical column/domain width, at least 13:
Code block (sql)
@TipoOperacion VARCHAR(50) -- Example only if the actual column/domain contract confirms 50.
The local list parser has capacity 50, but that is not proof of the underlying table width. Update procedure documentation and adapter parameter metadata consistently; never shorten the business code to fit. Current shared process-tracking code also passes a ten-character type size, requiring coordination with its separate library/SQL owner, not an out-of-scope fix. Benefit/acceptance: approved fixture tests preserve ENT_SPID and full ENT_SPID_PART end to end, affect the intended record, and expose a zero-row outcome as failure. Verify actual column width/collation and all consumers before deployment. JIRA: SPID-CDA-FR-16.
SPID-CDA-FR-17 - Select dispatch work from committed, consistent state
Category: MustChange/rewrite; Severity: Major; rules: data-access consistency, checklist sections 6/8. Evidence: polling join:75 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SQL/usp_SpidGenCda_ObtenerRegistrosCda.sql:75), second hint:76 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SQL/usp_SpidGenCda_ObtenerRegistrosCda.sql:76), third hint:93 (../../SPEI_SPID/SPID_CEP_GEN_CDA/SQL/usp_SpidGenCda_ObtenerRegistrosCda.sql:93).
Code block (sql)
FROM dbo.tb_srv_plan_trabajo_maestro AS Mto WITH (NOLOCK)
JOIN dbo.tb_srv_plan_trabajo_detalle AS Det WITH (NOLOCK) ...
JOIN dbo.tb_srv_plan_trabajo_detalle AS Det_plan WITH (NOLOCK) ...
These three hints allow dirty plan reads that can later roll back, yet their output triggers SPID insertion and acknowledgements. Minimum-step/join selection is not a consistent committed view. Same-instance tick serialization does not isolate concurrent database writers. Recommend committed reads or an approved versioned consistency strategy:
Code block (sql)
FROM dbo.tb_srv_plan_trabajo_maestro AS Mto
JOIN dbo.tb_srv_plan_trabajo_detalle AS Det ...
JOIN dbo.tb_srv_plan_trabajo_detalle AS Det_plan ...
Review blocking/indexes and measure an execution plan in an approved environment. If multiple workers are supported, agree atomic claiming, idempotency and reconciliation as well; removing NOLOCK alone is not an exactly-once cross-database guarantee. Do not blindly add distributed transactions or new SPID procedures. Benefit/acceptance: rolled-back producer/reopen transactions cannot dispatch invalid work; committed work remains discoverable, with acceptable ordering/blocking. Replayed insertion is a risk, not proof that the unverified legacy procedure is non-idempotent. JIRA: SPID-CDA-FR-17.
rewrite_recommended
No additional standalone finding. README/identity documentation is included in FR-15. The existence-check script is only a catalogue-name check; it does not certify signatures, permissions, schema resolution or readiness. The local procedures actively THROW; no blanket stored-procedure-swallowing finding is made.
NiceToRewrite_ifTimePermits
No additional standalone finding for solution, ignore or attributes metadata.
5. Architecture-Fit Note
Apply the portfolio's standing architecture trade-offs (./_ORCHESTRATION.md#6-standing-architecture-question-is-the-current-hostingarchitecture-model-right) to this scheduled, database-polling integration worker. There is no source evidence of an HTTP workload or a queue-consumer contract requiring microservices. The existing service and layered DI design are reasonable starting points; reliability fixes and tests have higher immediate value than a platform rewrite.
Option	Fit for observed workload	Gate / trade-off
Repair/modernize in place	Best first step: retain current polling and legacy contracts; later evaluate a supported .NET Worker/Windows-service target	Verify x86/Framework-only dependencies, SCM behavior, deployment ownership and regression results before choosing a target
Event/message-driven worker	Conditional if upstream systems can publish a durable authoritative event	Requires event versioning, duplicate handling, dead-letter/retry ownership and a database-to-event consistency contract
Workflow/saga	Conditional if measured recovery needs justify durable orchestration across the existing steps	Do not add an engine merely because several SQL calls exist; first define idempotency, compensation and manual reconciliation
Scheduled/serverless execution	Conditional if batches are short/stateless and network/security policy permits	Unknown execution limits, cold-start tolerance, volume, SLA, hosting restrictions and costs
Full microservices rewrite	Not justified by this review	Requires a separately approved scaling/team/ownership problem and budget

Sequence: resolve dependency baseline and test reliability -> repair defects/result contracts -> reduce duplication and prove recovery -> measure operations -> decide modernization. Do not automatically place all cross-database work in TransactionScope; topology, permissions, legacy procedure behavior and recovery/idempotency must be established first.
Owner decisions and dependency verification still required
• Approve the host/configuration/SCM identity and any historical aliases; verify the shared
base's startup/stop behavior against the deployed library. Central GEN-14 remains context.
• Supply actual legacy procedure signatures/result schema, table widths/keys, permissions,
idempotency and replay/reconciliation rules. The long/decimal folio path narrows to Int32 for legacy calls; confirm its true supported domain before changing numeric types.
• Coordinate shared ten-character process-type contracts with FR-16; no shared-library-local
remediation is claimed in this service audit.
• Define cleanup/drain budgets, forced-stop ownership, supported instance count and operational
escalation when insertion succeeds but acknowledgement fails.
• Confirm clock-time domain, whether 24:00 is allowed, overnight windows, time zone/calendar
and notification policy. Generic TimeSpan acceptance of negative/day-valued values is an open requirement, not a finalized production defect.
• Supply throughput/peak volumes, RTO/RPO, SLAs, supported hosting environment and budget.
No production performance, vulnerability-scan, coverage or release verdict is inferred.
6. JIRA-Ready Backlog
All entries are open recommendations, not applied fixes or created remote tickets. Acceptance criteria are the corresponding finding's Benefit/acceptance paragraph. Shared concerns reuse GEN-1/2/9/10/11; no new GEN number is created from a single-service lead.
ID	Title	Category	Project	Primary file(s)	Priority / severity
SPID-CDA-FR-1	Honor all 19 mutation/alert outcomes	MustChange/rewrite	Application + host	handlers (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:82), orchestrator (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:246), host (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:165)	High / Major
SPID-CDA-FR-2	Bound cancellation-independent tick cleanup	MustChange/rewrite	Application	OperacionServicio.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:246)	High / Major
SPID-CDA-FR-3	Record rejected insertions as process failures	MustChange/rewrite	Application	OperacionServicio.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:183)	High / Major
SPID-CDA-FR-4	Preserve the plan-service error key	MustChange/rewrite	Application	OperacionServicio.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:199)	High / Major
SPID-CDA-FR-5	Own service finalization at tick scope	MustChange/rewrite	Application	ENT_SPID.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:81), ENT_SPID_PART.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID_PART.cs:78)	High / Major
SPID-CDA-FR-6	Pin/package the approved shared dependency	MustChange/rewrite	All project metadata	Aplicacion.csproj (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Aplicacion.csproj:11) and three other projects	Blocker
SPID-CDA-FR-7	Extract checked handler completion	rewrite_recommended	Application	ENT_SPID.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID.cs:44), ENT_SPID_PART.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPID_PART.cs:45)	Medium / Major
SPID-CDA-FR-8	Notify expected skips on bounded transitions	rewrite_recommended	Application	OperacionServicio.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs:75)	Medium / Major
SPID-CDA-FR-9	Correct/remove stale friend-assembly metadata	NiceToRewrite_ifTimePermits	Application + tests	AssemblyInfo.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Aplicacion/AssemblyInfo.cs:4)	Low / Minor
SPID-CDA-FR-10	Preserve repository technical failures	MustChange/rewrite	Infrastructure	SondeoDao.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/SondeoDao.cs:35) and both detail adapters	High / Major
SPID-CDA-FR-11	Reject missing mandatory insertion result	MustChange/rewrite	Infrastructure	DetallesCiseSpidDao.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseSpidDao.cs:62)	High / Major
SPID-CDA-FR-12	Let assertion and subject failures fail tests	MustChange/rewrite	Tests	ENT_SPIDUT.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPIDUT.cs:180), ENT_SPID_PARTUT.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/ENT_SPID_PARTUT.cs:161), OperacionServicioUT.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/OperacionServicioUT.cs:103)	High / Major
SPID-CDA-FR-13	Strengthen representative lifecycle fixtures	rewrite_recommended	Tests	OperacionServicioUT.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/PruebasUnitarias/OperacionServicioUT.cs:316) and handler fixtures	Medium / Major
SPID-CDA-FR-14	Verify task drain before state/disposal	MustChange/rewrite	Host	ServiceHost.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:161)	High / Major
SPID-CDA-FR-15	Validate/document approved service identities	rewrite_recommended	Host + support	ServiceHost.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/ServiceHost.cs:67), Program.cs (../../SPEI_SPID/SPID_CEP_GEN_CDA/SPID_CEP_GEN_CDA/Program.cs:18), README.md (../../SPEI_SPID/SPID_CEP_GEN_CDA/README.md)	Medium / Major
SPID-CDA-FR-16	Align SQL operation-type width	MustChange/rewrite	SQL + infrastructure	usp_SpidGenCda_MarcarOperacionProcesada.sql (../../SPEI_SPID/SPID_CEP_GEN_CDA/SQL/usp_SpidGenCda_MarcarOperacionProcesada.sql:30)	High / Major
SPID-CDA-FR-17	Remove dirty-read dispatch decisions	MustChange/rewrite	SQL	usp_SpidGenCda_ObtenerRegistrosCda.sql (../../SPEI_SPID/SPID_CEP_GEN_CDA/SQL/usp_SpidGenCda_ObtenerRegistrosCda.sql:75)	High / Major

Suggested execution: FR-6 and FR-12 first to make validation trustworthy; then FR-10/11/16 and FR-1/3/4/5/2/14/17 as coordinated correctness work with regression cases. FR-7/13/8/15 follow with owner decisions where needed; FR-9 is optional. None of the proposed remediation depends on creating a new SPID insertion procedure.
