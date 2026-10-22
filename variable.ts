SPEI CEP GEN CDA Recommendations
SPEI_CEP_GEN_CDA - Architecture and Code Quality Recommendations
Date: 2026-10-14. Source HEAD: d2c6dbdfda32a99ab87a09a8069c5a0d1b29bab3.
Solution: SPEI_CEP_GEN_CDA.sln (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA.sln). Compared against LoansApp architecture/coding practices (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR review checklist (../BaseLine_Docs/code_review.md). See portfolio control (./_ORCHESTRATION.md), understanding (../architecture/SPEI_CEP_GEN_CDA/Application_understanding_technical_document.md), review trail (./_SPEI_CEP_GEN_CDA_ExhaustiveReview_Progress.md) and dated Word companion (./SPEI_CEP_GEN_CDA_Recommendations_10142026.docx).
Audit complete; remediation not performed. There are 19 unique open findings: 13 MustChange/rewrite, five rewrite_recommended, one NiceToRewrite_ifTimePermits. Severity: four Blocker, 14 Major, one Minor. This is an implementation assessment, not a release approval, PR verdict, fidelity percentage or acceptance-criteria sign-off.
1. Solution Overview
This timer-driven Windows Service polls TOMI work, obtains ENT_SPEI/CISE complements, maps each to a CDA, looks up customer RFC information through T24 views and calls the existing SPEI CEP insertion procedure. It then updates process aliases, service bookkeeping and the parent work item. It is receipt-generation/completion software; the reviewed source does not establish direct funds movement or a service-supervisor role.
Order / tier	Project	Kind / target	Relative project path
1 / 0	Aplicacion	Application library, netstandard2.0, x86	Aplicacion\Aplicacion.csproj
2 / 1	Infraestructura	Repository library, net48, x86	Infraestructura\Infraestructura.csproj
3 / 1	PuebasUnitarias	MSTest library, .NET Framework 4.8, x86	PuebasUnitarias\PuebasUnitarias.csproj
4 / 2	SPEI_CEP_GEN_CDAN	Windows Service, .NET Framework 4.8, x86	SPEI_CEP_GEN_CDA\SPEI_CEP_GEN_CDAN.csproj

Four direct project edges: Infraestructura -> Aplicacion; tests -> Aplicacion; host -> Aplicacion and Infraestructura. No cycle. Tier zero is internal independence, not proof that external references resolve. The host project filename has a final N; its assembly is SPEI_CEP_GEN_CDA. Solution x64 labels do not establish actual x64 binaries.
Logical connection / role	Existing procedure
TOMI / InterfacesT24 - poll	usp_SpeiGenCda_ObtenerRegistrosCda
TOMI / InterfacesT24 - details	usp_SpeiGenCda_RecuperarDetalles
TOMI / InterfacesT24 - parent mark	usp_SpeiGenCda_MarcarOperacionProcesada
T24_VW / VistasT24 - RFC	usp_SpeiGenCda_ObtenerRfcClienteT24
SPEI1 / Enlace - insertion	sp_InterfaceMW_SPEI_CEP

Four local definitions and the metadata-existence checker are tracked. The SPEI insertion definition, deployed schemas and actual connection mapping are not supplied. The current DAO sends operation key/date/time/node, not RFC; no missing-RFC-argument defect is inferred without the existing procedure's contract.
2. Scope and Methodology Note
Every line of every tracked text file was read, not sampled. The work resumed from the application checkpoint, persisted each remaining project/support batch, and verified the original source hashes before advancing coverage.
Batch	Text files	Physical lines	Final status
Aplicacion	15	685	Full read complete
Infraestructura	5	208	Full read complete
PuebasUnitarias	6	1,312	Full read complete
Host	5	378	Full read complete
SQL	5	445	Full read complete
Root/support/historical TXT	6	877	Full read complete
Total	42	3,905	No pending text ranges

The 48-file manifest (./_SPEI_CEP_GEN_CDA_FileManifest.csv) records original SHA256, bytes, lines, HEAD and review disposition; the file list (./_SPEI_CEP_GEN_CDA_FileList.txt) fixes scope. Ignored/generated outputs, package caches and Git internals are excluded.
• Four tracked DLLs received assembly identity/architecture/API metadata inspection only:
MUFG_T24 1.0.0.0 MSIL, SimpleInjector 5.0.0.0 MSIL, MySql.Data 5.2.5.0 MSIL and Oracle.DataAccess 2.112.1.0 X86. They were not executed, decompiled or source-audited.
• Both historical PDFs were text-extracted locally and read completely: required 13 pages /
447 extracted lines; optional 21 pages / 740 lines. Every page was nonempty and extraction permitted. These counts are additional historical text, not part of the 3,905 source lines. This was not visual/PDF-layout verification. The 266-line historical TXT was fully read with BOM-aware Windows-1252 fallback after ambiguous viewer output.
• The historical documents use General/Entradas as their reference. All 20 old topics were
dispositioned in the progress trail; they were not copied into the backlog as current defects.
• Current shared MUFG_T24 source was read only to verify consumed lifecycle, result, token,
cloning and identity contracts. Per the checklist, no library-local defect is charged here. The bundled DLL is not established as equivalent to that source or the deployed library.
• The baseline's blank evidence/count fields are not treated as measurements. Its stated
practices and the review checklist's applicable boundary/logging/persistence/testability sections guide recommendations alongside direct bug analysis.
• Repository exclusions are honored: no system-parameter/date/holiday differences, no claimed
functional distinction between IniciaProceso and ActualizaEjecucion, no new SPEI/Enlace wrapper or stored procedure, and no legacy-source edit. Literal Moq call matching remains a test-wiring issue even where lifecycle operations are business-equivalent.
Quantified Cross-Checks
These supplement the full read; they are not test execution or coverage:
Check	Observed declaration / source count
Test inventory	16 TestMethod attributes, three DataRow attributes, 19 Assert lines, seven Verify calls
Cancellation safety net	All 16 Act paths use CancellationToken.None; no cancellation-source scenario
Invalid concrete handler mocks	Four new Mock<ENT_SPEI> constructions
Repeated test setup	14 directory-creation blocks and 14 Config.Rutas assignments; no TestInitialize/TestCleanup
Application DTO construction	Four ProcesoDTO constructions, four AlertaDTO constructions, two terminal alias calls
Dependency paths	Five absolute HintPaths; two distinct missing target DLLs
Provider boundaries	Five query-result consumption sites across four DAOs omit Exito/cause checks
SQL hazards	Three NOLOCK eligibility hints; three fallible audit writes before original rethrow
Application named-pattern checks	Zero throw-ex/new-Exception(ex.Message), named MD5/SHA1/DES/3DES/ECB, or Result/Wait/GetAwaiter matches

The named-pattern checks are not an SCA/CVE/license verdict or proof of universal security. No application/configuration files were changed. No application dependencies were restored or installed, no live SQL/SCM/service was invoked, and no full build, test suite or coverage run was performed. The available x86 .NET 8 runtime has no SDK; expected private DLLs, configured test packages/adapters and .NET Framework 4.8 targeting references are absent. Framework MSBuild alone is not a validated mixed SDK/classic build toolchain.
A checksum-verified public PDFBox 3.0.8 JAR was downloaded for local extraction after the native Windows filter failed. No document/code content was uploaded. Isolated exact-property and framework-conversion probes reproduced SC-A4/SC-I2; they are not application tests.
3. What's Already Good
• Repository interfaces (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Interfaces/IDetallesCise.cs#L1)
separate orchestration from database adapters. Existing direct stored procedures do not warrant an ORM or wrapper layer by themselves.
• RFC cancellation (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L157)
is explicitly rethrown; cancellation also bypasses the ordinary per-folio error route at OperacionServicio:151 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs#L151).
• Ordinary per-operation failures (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs#L155)
are logged/recorded/alerted rather than universally swallowed. Preserve intentional continuation.
• CDA construction (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Modelos/Cda.cs#L45)
guards a null complement before dereferencing it.
• Host composition (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L64)
already uses shared lifecycle and DI. Effective inherited startup metadata validation and tick serialization rule out invented uninitialized-PID/overlapping-timer defects.
• Master update (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_MarcarOperacionProcesada.sql#L44)
is transactional and reports affected rows. All four local procedure catch blocks have active THROW; SC-S2 is conditional cause replacement, not a blanket swallow allegation.
• The actual test project (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/PuebasUnitarias.csproj#L1)
contains behavioral tests and mocks; this is not a zero-test solution. SC-T1/T2 explain why its existence is insufficient as a release safety net.
4. Findings by Project
Each finding belongs to exactly one category. Blocker/Major/Minor use the approved checklist, not penetration-test severity. Current snippets are excerpts or explicitly labeled shapes; recommended code/pseudocode is illustrative, not an applied or fully compiled patch. Preserve repository logging/localization and real API contracts when implementing.
4.1 Aplicacion - Tier 0
MustChange/rewrite
SC-A1 - Remove raw RFC/customer identifiers from logs
Category: MustChange/rewrite. Severity: Blocker. Priority: P1. Evidence: ENT:154 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L154), ENT:177 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L177), Cda:71 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Modelos/Cda.cs#L71).
Current log interpolation includes these three raw fields; actual sensitive values are omitted:
Code block (text)
{rfc}
{cda.RFCCURPBeneficiario}
{this.customerCode}
Ordinary INFO logs unnecessarily disclose personal/customer identifiers. Retention, readership and downstream redaction are unknown; asynchronous writing/rotation does not make the content safe. Replace those fields with safe correlation and outcome, using an approved masking helper only if necessary:
Code block (csharp)
Config.EscribirLog($"INFO|ProcesaOperacion_CISE|Consulta RFC completada para folio {folio}");
Do not move raw data into exception/alert text or invent an unapproved hashing scheme. Benefit: preserve operational diagnosis without propagating identifiers. Regression / JIRA SC-A1: capture all three normal/fallback/error paths; full synthetic RFC and customer strings must never appear in logs. Review alert/exception text as well.
SC-A2 - Complete the parent only after all required complement outcomes
Category: MustChange/rewrite. Severity: Blocker. Priority: P1. Evidence: ENT:73 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L73), ENT:103 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L103), mark SQL:44 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_MarcarOperacionProcesada.sql#L44), poll SQL:69 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_ObtenerRegistrosCda.sql#L69).
Current control-flow shape:
Code block (text)
foreach complement:
    observe cancellation
    insert complement
    if inserted: terminate aliases/service and mark whole folio/type processed
The mark sets master PROCESADA/flg_terminado=1, while polling selects unfinished masters. With two complements, first success followed by cancellation at the next token check leaves unattempted work beneath a finished parent. Cancellation bypasses the ordinary error writer; this scenario does not assume how external error procedures might reopen other failures. Details UNION ALL (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_RecuperarDetalles.sql#L52) supports both node projections; production cardinality/idempotency is still unknown.
Recommended shape:
Code block (text)
for each complement:
    insert and record its approved outcome
after every required outcome satisfies the agreed completion policy:
    perform the correctly scoped terminal updates
    require confirmed parent marking
    log confirmed completion
Verify which alias/service updates are folio-level before moving them. Preserve the explicit legacy no-row policy until owners approve otherwise. Agree recoverability/idempotency using existing SPEI APIs/TOMI state; no new wrapper/SP, assumed rollback or arbitrary distributed transaction. Benefit: parent state represents all required work, not the first success. Regression / JIRA SC-A2: two complements; cancel after first insertion, fail the second, restart safely, and assert parent ordering/discoverability and no duplicate first insertion.
SC-A3 - Check the parent-mark Boolean before completion success
Category: MustChange/rewrite. Severity: Major. Priority: P1. Related family: GEN-9. Evidence: ENT:103 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L103), DAO:47 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseDao.cs#L47).
Code block (csharp)
await _detallesCise.MarcarOperacionProcesadaAsync(folio, tipoOperacion, ct).ConfigureAwait(false);
The DAO returns Afecto == true with strict affected-row enforcement off. Awaiting alone does not confirm a mark, yet unconditional completion logging follows. At SC-A2's correct terminal point, require the result:
Code block (csharp)
bool marcada = await _detallesCise.MarcarOperacionProcesadaAsync(folio, tipoOperacion, ct)
    .ConfigureAwait(false);
if (!marcada)
    throw new InvalidOperationException($"No se confirmó el cierre del folio {folio}");
Route failure through existing error/notification handling; retain cancellation and safe context. SC-I1 separately preserves provider causes; SC-A2 separately fixes timing. Benefit: insertion is not confused with confirmed persisted parent completion. Regression / JIRA SC-A3: true, false/zero-row, exception and cancellation; false must be explicitly observable and must not emit the completion-success message.
SC-A4 - Agree and enforce one identifier-length boundary
Category: MustChange/rewrite. Severity: Major. Priority: P2. Evidence: Cda:22 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Modelos/Cda.cs#L22).
Code block (csharp)
if (_rfcCurpBeneficiario.Length > 18)
    return _rfcCurpBeneficiario.Substring(0, 17);
The exact extracted getter probe returned 17 -> 17, 18 -> 18, 19 -> 17 and 20 -> 17; empty/null/whitespace returned ND. That discontinuity is reproduced, not the intended width. The current DAO does not send this field to the SPEI SP, so rejected/malformed CEP persistence is not established.
Only if owners confirm maximum 18, the consistent replacement is:
Code block (csharp)
return _rfcCurpBeneficiario.Length > 18
    ? _rfcCurpBeneficiario.Substring(0, 18)
    : _rfcCurpBeneficiario;
If 17 or explicit rejection is required, implement that approved contract consistently instead; do not silently trim meaningful input or label a guessed width as correct. Benefit: stable, documented model behavior. Regression / JIRA SC-A4: assert exact strings for null, empty, whitespace, 1/17/18/19/20 characters and meaningful whitespace under the approved policy.
SC-A5 - Make compatible shared dependencies reproducible
Category: MustChange/rewrite. Severity: Blocker. Priority: P1. Central finding: GEN-1. Evidence: app:12 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Aplicacion.csproj#L12), infra:16 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Infraestructura/Infraestructura.csproj#L16), tests:77 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/PuebasUnitarias.csproj#L77), host:63 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDAN.csproj#L63).
Code block (xml)
<HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
Four MUFG_T24 paths plus host SimpleInjector are machine-local; both distinct targets are absent. No portfolio nuget.config/MUFG-named package archive was found. This does not prove an organization-wide feed does not exist. The bundled old MUFG copy lacks current host/runner/ logging types, so redirecting paths to it is not a reliable repair.
Follow GEN-1's centralized versioned-package plan, not four path hacks. Conceptual manifest:
Code block (xml)
<PackageReference Include="MUFG_T24" Version="APPROVED_COMPATIBLE_VERSION" />
This is not a known published package/version. Verify netstandard2.0/net48 compatibility, x86/transitive provider requirements, API shape and the approved source of SimpleInjector. Do not retarget layers blindly or replace a missing dependency with a stale binary. Benefit: clean developer/CI builds select a known compatible dependency set. Regression / JIRA SC-A5: approved clean restore, supported x86 Debug/Release builds, transitive deployment and isolated startup. No successful SPEI build was established here.
rewrite_recommended
SC-A6 - Expose partial/no-row cycle outcomes without changing legacy policy
Category: rewrite_recommended. Severity: Major. Priority: P2. Evidence: ENT:77 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L77), ENT:107 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L107), operation:186 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs#L186), host:130 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L130).
Code block (csharp)
if (!ordenPagoInsertada)
    continue;
// The complement loop can still end with:
return true;
The same Boolean represents inserted, mixed and entirely skipped work; the outer operation can also continue after logged folio errors, and the host discards its Boolean. Individual errors are observable; the aggregate contract is coarse. The no-row skip is explicitly legacy behavior, not automatically a defect.
Start with the smallest safe summary; introduce a type only if consumers need it:
Code block (text)
Folio: Attempted=2, Inserted=1, NoRow=1, TerminalStateConfirmed=false
Cycle: CompletedFolios=..., FailedFolios=..., SkippedFolios=...
Wire handler, orchestrator and host together if the API changes. Preserve accepted no-row, unknown-operation, alert and per-folio continuation behavior. Benefit: distinguish productive, partial and no-op cycles. Regression / JIRA SC-A6: all inserted, all no-row, mixed, unknown operation and exception; assert exact summary counts and unchanged approved decisions.
SC-A7 - Extract counted local lifecycle/alert construction
Category: rewrite_recommended. Severity: Major. Priority: P3. Evidence: ENT:49 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L49), ENT:84 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L84), operation:68 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs#L68), operation:158 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/OperacionServicio.cs#L158).
Current repeated shape: four ProcesoDTO initializers, four AlertaDTO initializers and two terminal calls; terminal blocks repeat folio/type/service/process/PID while changing alias.
Code block (text)
construct terminal DTO for first alias -> TerminaProceso
construct nearly identical terminal DTO for final alias -> TerminaProceso
After SC-A2 establishes granularity, extract only genuinely shared local construction:
Code block (csharp)
await _controlServicio.TerminaProceso(CrearProceso(folio, _configuracion.AliasProceso), ct);
await _controlServicio.TerminaProceso(CrearProceso(folio, _configuracion.AliasProcesoFin), ct);
CrearProceso is illustrative, not an existing API. Preserve both aliases, ordering, all shared fields and distinct alert messages/severity. Do not add a factory hierarchy or remove a required call merely because it looks similar. Benefit: reduce metadata drift without obscuring business flow. Regression / JIRA SC-A7: assert both aliases/all fields and the four distinct alert paths.
NiceToRewrite_ifTimePermits
None in this project; documentation-only SC-N1 is in support below.
4.2 Infraestructura - Tier 1
MustChange/rewrite
SC-I1 - Distinguish provider failure from legitimate empty/no-row results
Category: MustChange/rewrite. Severity: Blocker. Priority: P1. Central family: GEN-11. Evidence: poll:36 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Infraestructura/Repositorios/SondeoDao.cs#L36), details:36 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseDao.cs#L36), mark:50 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseDao.cs#L50), insert:43 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseSpeiDao.cs#L43), RFC:33 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseT24Dao.cs#L33).
Code block (csharp)
return resultado.Data;
// Other sites reduce the same contract to:
return resultado.Data.FirstOrDefault();
return resultado.Data.FirstOrDefault()?.RFC;
All five query sites omit Exito/Exception checks, including marking's Afecto reduction. Verified effective shared overloads initialize non-null Data, default to non-strict behavior and can return Exito=false/Exception for SQL or mapping failure. Thus failed polling can look empty, failed details can look missing, failed insertion can become the approved null/no-row skip, failed RFC lookup can look absent, and failed marking can lose its real cause. Successful emptiness itself is not the defect.
Guard technical failure before reducing Data/Afecto at every site:
Code block (csharp)
if (!resultado.Exito)
    throw new InvalidOperationException("Falló el procedimiento de inserción CEP.", resultado.Exception);
return resultado.Data.FirstOrDefault();
Use safe procedure/operation context and existing error/notification handling; keep cancellation and original causes. Do not blanket-enable strict affected-row enforcement: valid empty reads/ legacy no-row responses must remain distinguishable. SC-A3 checks caller Boolean success; this ticket preserves the DAO failure channel. Do not change MUFG_T24 or add a SPEI SP here. Benefit: technical errors cannot masquerade as valid business emptiness or a successful cycle. Regression / JIRA SC-I1: each of five paths: Exito=true empty, Exito=false empty/partial Data, SQL/mapping exception and cancellation; assert exact cause and approved downstream decision.
rewrite_recommended
SC-I2 - Make the existing SPEI operation-key conversion explicit
Category: rewrite_recommended. Severity: Major. Priority: P2. Evidence: SPEI DAO:34 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Infraestructura/Repositorios/DetallesCiseSpeiDao.cs#L34).
Code block (csharp)
Convert.ToInt64(cda.idFolioOperacion)
The source property is decimal; this conversion rounds fractions. An isolated framework probe returned 42.4 -> 42, 42.5 -> 42 and 42.6 -> 43. Fractional production keys are not established, and the deployed parameter precision is unknown; this is a defensive contract recommendation. If the existing approved API requires integral Int64, guard scale/range:
Code block (csharp)
decimal key = cda.idFolioOperacion;
if (key != decimal.Truncate(key) || key < long.MinValue || key > long.MaxValue)
    throw new ArgumentOutOfRangeException(nameof(cda), "La clave debe ser un Int64 integral.");
long operationKey = decimal.ToInt64(key);
Preserve allowed signed values; do not invent a positive-only rule. If the approved API accepts decimals, use that existing contract instead of introducing another procedure. Benefit: malformed input cannot silently select a neighboring key. Regression / JIRA SC-I2: whole/fractional/halfway values, signed limits and overflow; assert exact @op_clave, not merely that a call occurred.
NiceToRewrite_ifTimePermits
None. Shared dependency packaging is consolidated in SC-A5, not repeated per project.
4.3 PuebasUnitarias - Tier 1
MustChange/rewrite
SC-T1 - Repair concrete mock, constructor and literal overload contracts
Category: MustChange/rewrite. Severity: Major. Priority: P1. Evidence: orchestrator fixture:63 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/OperacionServicioUT.cs#L63), setup:192 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/OperacionServicioUT.cs#L192), alert verification:287 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/OperacionServicioUT.cs#L287), lifecycle expectation:661 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/ENT_SPEIUT.cs#L661), actual constructor:30 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/Aplicacion/Servicios/ENT_SPEI.cs#L30).
Code block (csharp)
var _entSpei = new Mock<ENT_SPEI>();
_entSpei.Setup(t => t.ProcesarOperacionCISEAsync("1006", It.IsAny<CancellationToken>()));
Four fixtures request a no-argument proxy, but the class has only a five-argument constructor. The target method is non-virtual, so supplying constructor arguments alone still cannot make Moq intercept it. Old tokenless alert expectations also target a different overload than current token-bearing calls. A lifecycle test verifies ActualizaEJecucion while the handler literally calls IniciaProceso: business equivalence does not make an unconfigured mock forward calls. These are independently verifiable static fixture defects, not executed test failures.
Recommended repair:
Code block (text)
Construct a real ENT_SPEI with its five mocked dependencies.
Configure/verify the actual DAO/control/alert overloads and token identity.
Only if handler isolation is necessary, introduce one minimal handler interface and
wire its application consumer, host registration and tests in the same change.
Do not make every method virtual for Moq. Preserve exempt parameter/date/holiday policies and the agreed functional equivalence of lifecycle operations. Benefit: fixtures reach Act and test real contracts. Regression / JIRA SC-T1: all four orchestrator fixtures and stale lifecycle/alert expectations pass with compatible dependencies; report actual discovered/executed cases.
SC-T2 - Add regression cases for the verified integrity/failure/stop defects
Category: MustChange/rewrite. Severity: Major. Priority: P1. Evidence: happy handler:448 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/ENT_SPEIUT.cs#L448), lifecycle test:593 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/ENT_SPEIUT.cs#L593), two-complement mapping:18 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/ProcesamientoCiseUT.cs#L18).
Code block (csharp)
var resultado = await _entSpei.ProcesarOperacionCISEAsync("1006", CancellationToken.None);
Assert.IsTrue(resultado);
All 16 Act paths use CancellationToken.None. The existing two-complement test checks mapping, not completion/recovery order. No test checks false parent marking, provider Exito=false/partial Data, identifier boundaries/redaction, or host timeout/disposal. The loose happy-path mark mock can default false while the test accepts true, missing SC-A3. Tests reference application only; infrastructure/host behavior needs an appropriate isolated seam.
Add cases that expose the original defects:
Code block (text)
cancel between two complement inserts -> no premature terminal parent; safe retry
false parent mark -> explicit failure and no completion-success message
provider failure vs successful empty insert -> distinct outcome and preserved cause
identifier boundaries -> exact approved values; raw strings absent from captured logs
stop timeout -> active dependencies not disposed; no false idle/persistence success
Use deterministic fakes/mocks, real token cancellation, exact parameters/DTOs/call order and test-owned logging. No production SCM/DB/credentials. Preserve approved no-row continuation. Benefit: the safety net detects actual defects rather than only Boolean happy paths. Regression / JIRA SC-T2: demonstrate linked cases fail the original behavior and pass after their fixes; publish real test/coverage output, never an invented zero-percent metric.
rewrite_recommended
SC-T3 - Isolate repeated fixture/global logging state
Category: rewrite_recommended. Severity: Major. Priority: P2. Evidence: ENT fixture:54 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/ENT_SPEIUT.cs#L54), operation fixture:74 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/OperacionServicioUT.cs#L74), mapping fixture:18 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/PuebasUnitarias/ProcesamientoCiseUT.cs#L18).
Code block (csharp)
var rutasLogs = Path.Combine(Path.GetTempPath(), "SPEI_CEP_GEN_CDA");
Directory.CreateDirectory(rutasLogs);
Config.Rutas = new RutasConfiguracion { /* shared directory */ };
Fourteen directory/setup blocks mutate static logging configuration without TestInitialize/ TestCleanup. Mapping tests construct logging models without their own fixture. Specific logger failure or observed parallel-run failure is not claimed; state/order coupling and repeated shared output are concrete.
Code block (text)
save previous static configuration
create a unique exact test-owned directory, or use the approved in-memory sink
share only small common fixture setup; retain scenario-specific assertions
restore configuration and clean only the owned path, even after test failure
serialize unavoidable static-state tests until the seam is isolated
Benefit: independent runs and less setup drift. Regression / JIRA SC-T3: run each test alone, reversed/random order, cleanup after failure and supported concurrent runners. Never recursively delete a shared temp root.
NiceToRewrite_ifTimePermits
None; do not rename public/shared APIs solely to modernize test style.
4.4 Windows Service Host - Tier 2
MustChange/rewrite
SC-H1 - Require actual quiescence before terminal state/container disposal
Category: MustChange/rewrite. Severity: Major. Priority: P1. Evidence: waits:158 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L158), disposal:180 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L180).
Current effective control-flow shape:
Code block (text)
await bounded helper for startup and running tasks
persist idle/termination
finally: dispose container
The verified compatibility helper delegates to the base wait wrapper, which logs/returns normally on timeout without completing the supplied task. Default wait/cleanup budgets are 60/30 seconds. Slow/uncooperative work can therefore retain disposed dependencies or continue after a terminal-state claim. Initialization availability is not quiescence. This is local consumption of a bounded contract, not a new library defect.
Code block (text)
request cancellation and stop new work through the existing base
wait within supported SCM budget; inspect actual completion of both supplied tasks
if still active: explicitly record incomplete stop; do not claim idle completion
release operation-owned/container resources exactly once after actual quiescence,
using an approved observed deferred-disposal/failure policy
An early return above finally is insufficient because unconditional Dispose still runs. Do not replace the budget with an unlimited synchronous wait or unobserved fire-and-forget. Benefit: resource ownership remains safe when cancellation is late or ignored. Regression / JIRA SC-H1: completed/null, faulted, cancel-aware and still-pending startup/tick tasks, wait cancellation and repeated stop; assert state/disposal ordering and bounded response.
SC-H2 - Confirm database cleanup before the local success message
Category: MustChange/rewrite. Severity: Major. Priority: P1. Central family: GEN-9. Evidence: idle:162 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L162), termination:165 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L165), success:167 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L167).
Code block (csharp)
await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, serviceName, cleanupCt);
await _controlServicio.ActualizaTerminacion(serviceName, cleanupCt);
Config.EscribirLog("INFO|OnStop|Terminación exitosa de bases de datos.");
The compatibility Boolean can be false; termination can return Exito=false/Exception without throwing. Some shared failures already log/alert and consuming the compatibility Boolean is documented optional. The definite local issue is the unconditional persistence-success claim.
Code block (text)
capture idle Boolean (or approved existing typed outcome) and termination result
require each necessary outcome; preserve cause and cleanup cancellation/budget
on failure: explicitly record incomplete DB cleanup
emit this local success message only after confirmed success
Apply SC-H1's quiescence guard. Inherited base success logging is dependency context and is not an aggregate database-persistence guarantee; do not promise a local fix removes it. Benefit: SCM stop is distinguishable from confirmed database cleanup. Regression / JIRA SC-H2: both succeed, either fails, provider exception and cancellation; the quoted local message must be absent for unsuccessful cleanup.
rewrite_recommended
SC-H3 - Reconcile approved GEN/ENV runtime identities
Category: rewrite_recommended. Severity: Major. Priority: P2. Evidence: Program:17 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/Program.cs#L17), host identity:33 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L33), configuration:64 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA/ServiceHost.cs#L64).
Code block (text)
runner / host identity: SPEI_CEP_ENV_CDA
loaded configuration:  SPEI_CEP_GEN_CDA
The shared runner uses identity for Global mutex and EventLog source, not only a description. Source mismatch is verified; a deployed collision or SCM failure is not.
Code block (text)
confirm SCM name / config key / EventLog source / mutex identity / historical aliases
derive runner and host identity consistently from the approved map
migrate any required aliases explicitly; verify sender and generator coexist safely
Do not blindly rename a registered service or duplicate standing GEN-14's library-local ServiceName setter issue. Designer Service1 text alone is not a new initialization defect. Benefit: avoid unintended sender/generator singleton coupling and misleading diagnosis. Regression / JIRA SC-H3: isolated approved identities, sender/generator coexistence and legacy alias migration with the actual approved DLL; no production SCM action during this audit.
NiceToRewrite_ifTimePermits
None; hosting modernization is a gated architecture choice, not another mandatory rewrite.
4.5 SQL and Root/Support
MustChange/rewrite
SC-S1 - Base work eligibility on committed state
Category: MustChange/rewrite. Severity: Major. Priority: P1. Evidence: poll SQL:69 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_ObtenerRegistrosCda.sql#L69), third hint:86 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_ObtenerRegistrosCda.sql#L86).
Code block (sql)
FROM dbo.tb_srv_plan_trabajo_maestro Mto WITH (NOLOCK),
     dbo.tb_srv_plan_trabajo_detalle Det WITH (NOLOCK)
Three NOLOCK hints cover authoritative work-plan eligibility. An uncommitted producer/master/ detail state can drive an external receipt insert before producer rollback; that rollback cannot undo the external action. Concurrent movement may also produce missing/duplicate reads. This is an evidenced isolation risk, not an observed incident.
Code block (text)
use DBA-approved committed/snapshot eligibility reads
prove the selected detail row itself satisfies approved unfinished/alias rules
if multiple workers are supported, use existing approved ownership/idempotency
validate actual constraints, plans, blocking and latency before rollout
Removing NOLOCK under verified read-committed settings is the smallest candidate; confirm snapshot/index/locking policy rather than enabling it blindly. The outer order/alias join relies on cardinality constraints, so order ties are a validation gate, not a separate proven defect without schema. A read-only poll/machine-local mutex is not a cross-host claim. Benefit: only committed eligible work drives generation. Regression / JIRA SC-S1: producer insert/update then rollback during polling; master/detail completion races; actual alias/order ties; supported multi-worker cases only if applicable. Measure real execution plans/latency in an isolated DBA fixture.
SC-S2 - Preserve the primary SQL error when audit insertion fails
Category: MustChange/rewrite. Severity: Major. Priority: P2. Evidence: mark:64 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_MarcarOperacionProcesada.sql#L64), poll:95 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_ObtenerRegistrosCda.sql#L95), details:67 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SQL/usp_SpeiGenCda_RecuperarDetalles.sql#L67).
Current catch shape:
Code block (sql)
BEGIN CATCH
    INSERT INTO auditoria.errorlog (...) VALUES (ERROR_NUMBER(), ...);
    THROW;
END CATCH;
Three fallible audit writes occur before original THROW. If the audit insert fails, control does not reach that rethrow and the caller sees the secondary failure instead. The marking procedure correctly rolls back first, but independent error-log storage can still fail.
Code block (text)
capture original metadata; complete required rollback
protect auxiliary audit insertion under the approved diagnostic policy
if auditing fails, explicitly expose the secondary diagnostic via a supported wired channel
rethrow the original operation error; never return success or use an empty catch
Do not assume client collection of SQL warnings unless it is actually wired. Coordinate with SC-I1 so the application preserves the true operation cause. This is the causal-diagnostics variant of the portfolio's GEN-11 failure-visibility family, not another shared-library defect. Benefit: original incident diagnosis survives degraded audit storage. Regression / JIRA SC-S2: force primary operation and independent audit-write failures; assert original number/procedure/cause and visible secondary diagnostics. No live SQL was run.
SC-S3 - Correct Release-to-Debug solution mapping
Category: MustChange/rewrite. Severity: Major. Priority: P2. Evidence: solution:42 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/SPEI_CEP_GEN_CDA.sln#L42).
Code block (text)
{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|Any CPU.ActiveCfg = Debug|Any CPU
{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|Any CPU.Build.0 = Debug|Any CPU
A requested solution Release|Any CPU build selects the application's Debug configuration, potentially combining unlike artifacts. Fix those two mappings, preserving unrelated entries:
Code block (text)
{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|Any CPU.ActiveCfg = Release|Any CPU
{54CC424C-8227-4E2A-A81F-9D6E0DAA9726}.Release|Any CPU.Build.0 = Release|Any CPU
Confirm the approved platform matrix; tracked x86 targets are not automatically changed. Benefit: requested release configuration produces corresponding application artifacts. Regression / JIRA SC-S3: after SC-A5/toolchain availability, inspect evaluated project Configuration/Platform and assert Release output/optimization/symbol policy for every supported solution build. A configuration label alone is not a successful-build test.
rewrite_recommended
None additional. Historical style suggestions were evaluated, not turned into speculative tickets.
NiceToRewrite_ifTimePermits
SC-N1 - Replace the template README with an approved service runbook
Category: NiceToRewrite_ifTimePermits. Severity: Minor. Priority: P3. Evidence: README:1 (../../SPEI_SPID/SPEI_CEP_GEN_CDA/README.md#L1).
Current content is a generic GitLab onboarding/README template, not service operating guidance:
Code block (text)
generic repository setup / collaboration / README-writing advice
no approved service build, configuration, stop/recovery or reconciliation procedure
Recommended service-specific outline:
Code block (text)
purpose and project/procedure map
approved dependency versions/source, x86 build/test commands
GEN/ENV SCM/config/EventLog/mutex identities and configuration keys, without secrets
isolated deployment, bounded stop, recovery/reconciliation and rollback
owners, evidence and unresolved contract decisions
Keep unavailable values pending until approved; do not execute/reuse the template force-push advice as an operational step. The audit handover is not itself a tested production runbook. Benefit: a newcomer can reproduce an approved build and follow safe operating instructions. Regression / JIRA SC-N1: an independent engineer follows the approved runbook in isolation. No production README edit or deployment was performed here.
5. Architecture-Fit Note
Apply the control document's architecture guidance (./_ORCHESTRATION.md): this is timer-polled, multi-step receipt generation with shared Windows/service/database contracts, not an observed HTTP API, continuous socket listener or necessarily a durable saga. Quality findings alone do not justify a wholesale rewrite. Fix integrity/privacy/provider/lifetime defects, make outcomes explicit and establish executable tests before changing hosting.
Option	Fit with observed workload	Trade-off / decision gate
Retain current Windows Service	Lowest immediate migration risk; existing SCM, timer and shared-library integration	Fix findings and verify deployed API/identity/stop contracts; retain bounded lifecycle
Modern .NET Worker Service	Potentially simpler packaging, DI/lifetimes and telemetry	Prove provider/shared-library/framework compatibility, x86 needs and operational migration budget
Scheduled job	Could fit if each polling batch can finish and restart independently	Requires accepted schedule/latency, bounded duration, ownership and restart/idempotency guarantees
Durable queue/job workflow	Could make per-complement progress and retry explicit	Requires measured volume/SLA and approved infrastructure/contracts; adds operating complexity

Unknown: SLA/RTO/RPO, volumes, poll cadence requirements, actual complement cardinality, cross-host ownership, existing SP idempotency, deployment constraints, cost and staffing. No queue, cloud, microservice or transaction-coordinator purchase/re-platform is recommended as the first move.
6. JIRA-Ready Backlog
These are proposed issue IDs/titles, not remotely created issues. Categories/severities match the 19 finding sections exactly. Priorities are review recommendations, not business-assigned dates. GEN references consolidate shared remediation; they do not add to the local finding count.
ID	Title	Category	Severity	Project / files	Priority	Acceptance / dependency
SC-A1	Remove raw RFC/customer logging	MustChange/rewrite	Blocker	Aplicacion: ENT_SPEI, Cda	P1	Three paths; no full synthetic identifier in logs/alerts/errors
SC-A2	Delay parent completion until required complements accounted for	MustChange/rewrite	Blocker	Aplicacion: ENT_SPEI; TOMI contract	P1	Two-complement cancel/failure/retry preserves discoverability and safe insertion
SC-A3	Confirm parent-mark Boolean before success	MustChange/rewrite	Major	Aplicacion: ENT_SPEI	P1	False/zero-row yields explicit failure and no completion success; coordinate SC-A2/I1
SC-A4	Agree/enforce identifier width boundary	MustChange/rewrite	Major	Aplicacion: Cda	P2	Approved exact null/whitespace/17/18/19/20 outputs, not guessed maximum
SC-A5	Pin compatible shared dependencies under GEN-1	MustChange/rewrite	Blocker	All four projects / five HintPaths	P1	Approved clean restore, supported x86 Debug/Release builds and isolated startup
SC-A6	Expose partial/no-row aggregate outcomes	rewrite_recommended	Major	Handler, operation, host consumer	P2	Exact counts across inserted/skipped/failed cycles; legacy decisions unchanged
SC-A7	Extract repeated local DTO construction	rewrite_recommended	Major	Aplicacion: ENT_SPEI, OperacionServicio	P3	After SC-A2; preserve both aliases/all DTO fields and four alert variants
SC-I1	Preserve provider failure before Data/Afecto reduction	MustChange/rewrite	Blocker	Infraestructura: four DAOs / five sites	P1	Failed empty/partial data differs from successful empty; original cause retained
SC-I2	Enforce existing operation-key type contract	rewrite_recommended	Major	DetallesCiseSpeiDao	P2	Approved signed scale/range; exact @op_clave for fractions/limits
SC-T1	Repair constructor/member/token mock contracts	MustChange/rewrite	Major	OperacionServicioUT, ENT_SPEIUT	P1	Four real/seamed fixtures reach Act; actual overload/token verified; runnable after SC-A5
SC-T2	Add linked integrity/failure/stop regressions	MustChange/rewrite	Major	Tests; isolated infrastructure/host seams	P1	Detect original linked defects; publish actual execution after SC-T1/dependency repair
SC-T3	Isolate repeated global logging fixtures	rewrite_recommended	Major	All three test classes	P2	Unique owned output, state restored, individual/reordered/concurrent runs safe
SC-H1	Defer terminal state/disposal until actual quiescence	MustChange/rewrite	Major	ServiceHost termination	P1	Pending tasks after bounded wait cannot cause premature disposal/idle success
SC-H2	Confirm cleanup outcomes before local success log	MustChange/rewrite	Major	ServiceHost termination	P1	Either failure/cancellation suppresses local success; preserve cause/budget/SC-H1 ordering
SC-H3	Reconcile approved GEN/ENV identities	rewrite_recommended	Major	Program, ServiceHost	P2	Approved SCM/config/mutex/EventLog map and isolated sender/generator coexistence
SC-S1	Use committed eligible work-plan reads	MustChange/rewrite	Major	Polling SQL	P1	Rollback/isolation, alias/order and supported ownership cases; measured plans/latency
SC-S2	Retain primary SQL error on audit-write failure	MustChange/rewrite	Major	Mark/poll/details SQL catch blocks	P2	Primary error plus observable secondary diagnostic; approved wired DBA policy
SC-S3	Repair application Release solution mapping	MustChange/rewrite	Major	SPEI_CEP_GEN_CDA.sln:42-43	P2	Evaluated Release configuration/output across supported matrix; SC-A5/toolchain
SC-N1	Write approved build/deploy/recovery README	NiceToRewrite_ifTimePermits	Minor	README.md	P3	Independent isolated walkthrough; actual owners/contracts, no secrets or guessed commands

Suggested ownership: application maintainers/QA for SC-A/T, repository maintainers for SC-I, service/platform owners for SC-H/A5/S3, DBA with application owners for SC-S1/S2, and operations for the approved SC-N1 runbook. These are roles to agree, not existing assignments or estimates.
7. Verification, Release Gates and Open Contracts
7.1 What This Audit Verified
Exhaustive text coverage, explicit binary/history disposition, source evidence, graph metadata, finding/backlog consistency and scoped document/Word-package structure are audit checks. The final machine-readable validation result is retained with the handover package. All 48 original source hashes/bytes and HEAD must match before its final pass is accepted. The getter/conversion probes reproduce current behavior only; no defect was fixed.
7.2 What Remains Unverified
• Full build/type-check, restore, test suite and measured coverage: prerequisites are unavailable.
• Actual deployed shared DLL/version/API, transitive providers and service identity/registration.
• SQL schema/indexes/constraints/permissions, actual SP body/parameter/result semantics,
idempotency, affected-row guarantees, isolation policy and live performance.
• Live cancellation, stop/disposal, database cleanup, safe retry/reconciliation and deployment.
• Visual Microsoft Word/Mermaid/PDF rendering, pagination and visual sign-off. The Word file
has styled OpenXML content; structural validation does not replace a human layout review.
• SCA/CVE/license/security-testing conclusions or acceptance-criteria/fidelity conformance.
No source remediation or production action was performed. Static review completion is not approval to deploy. Keep failed-provider results distinct from valid legacy emptiness.
7.3 Owner Decisions Required Before Implementation
Contract	Needed evidence / decision
Complements and terminal state	Required cardinality/outcomes, alias granularity, discoverability and safe restart
Existing SPEI interface	Real parameter/result/empty/idempotency contract; no invented RFC parameter or new wrapper
Identifiers	Approved width/overlength/whitespace policy and log-data classification
Dependencies/build	Approved compatible MUFG package/version/feed, framework/x86 matrix and CI toolchain
Service operations	GEN/ENV identities, supported stop budget, incomplete-stop/deferred-disposal ownership
SQL	Committed/snapshot policy, indexes/unique constraints and observable secondary-error channel
Tests/rollout	Isolated fixtures, exact regression evidence, rollback and reconciliation ownership

7.4 Recommended Sequence
1. Obtain contracts and restore a compatible reproducible build path (SC-A5); repair fixture
wiring (SC-T1) so regression results become meaningful.
2. Address disclosure, parent completion and provider outcomes (SC-A1/A2/A3/I1), with failing
regression cases that verify exact required outcomes rather than proxy counts.
3. Make stop/quiescence/persistence and committed polling safe (SC-H1/H2/S1); preserve causal
SQL diagnostics (SC-S2). Validate in isolation with actual approved dependencies/contracts.
4. Resolve width/key/identity/configuration contracts (SC-A4/I2/H3/S3), then improve aggregate
observability and local fixture/DTO duplication (SC-A6/A7/T3); complete linked SC-T2 evidence.
5. Publish the approved runbook (SC-N1), perform an isolated operational/rollback walkthrough,
then consider architecture modernization only if measured needs justify it.
See handover playbook (./_SPEI_CEP_GEN_CDA_HANDOVER_PLAYBOOK.md), complete handover inventory (./_SPEI_CEP_GEN_CDA_HANDOVER_INVENTORY.csv) and prompt history (./_SPEI_CEP_GEN_CDA_PROMPT_HISTORY.md). Findings remain open until separately authorized implementation and actual regression/operational evidence demonstrate closure.
