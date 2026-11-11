SWF IN ETL Recommendations
SWF_IN_ETL - Architecture & Code Quality Recommendations
Solution: SWIFT_IN_ETL.slnx (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL.slnx). Reviewed: 2026-10-15, branch CodeReview, commit 414217a500c8581e6f5431694a5cf7ed5abbbb0d (working tree clean at start and end). Compared against: LoansApp coding-practices baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR review/severity checklist (../BaseLine_Docs/code_review.md). See also: orchestration/control (./_ORCHESTRATION.md) (methodology, template, standing findings) and the exhaustive-review tracker (./_SWF_IN_ETL_ExhaustiveReview_Progress.md) / file manifest (./_SWF_IN_ETL_FileManifest.csv). Target: .NET Framework 4.8 Windows Service; focus: all categories; severity threshold: all. Audit only: no application fixes applied.
1. Solution Overview (low/no-dependency -> high dependency)
Tier	Project	Kind	Path
0	Aplicacion_IN_ETL	SDK-style class library (net48): orchestration, rules, parsers, DTOs, WCF proxies	project (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Aplicacion_IN_ETL.csproj)
1	Infraestructura_IN_ETL	SDK-style class library (net48): 21 repositories/clients over SQL Server, T24 SOAP and HTTP	project (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Infraestructura_IN_ETL.csproj)
2	SWIFT_IN_ETL	Windows Service host (classic csproj, net48, x86)	project (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/SWIFT_IN_ETL.csproj)
3	PruebasUnitarias	MSTest/Moq test library (classic csproj, net48)	project (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/PruebasUnitarias.csproj)

What this solution actually does: Swf_IN_ETL is a timer-driven Windows Service (default 10 s, from the encrypted service configuration) that turns inbound SWIFT messages sitting in the TOMI work table (SWIFT_IN, status READ) into either an *accepted* operation or a *manual/rejected* one. Each tick it checks the business day and the operating window, loads system parameters, then reads two queues: classic MT messages (MT103, MT191, MT202/202COV, MT940/950/998) and ISO 20022 MX messages (pacs.008, pacs.009 and COV, camt.052/053/057/106). For MT it parses the FIN header, tokenises the body, validates each tag against regex syntax rows stored in the database, maps the values to work-table fields, and validates the beneficiary account/currency, account restrictions and CIF/vostro/SPEI catalogues against T24 views and a T24 SOAP service. For MX it calls external HTTP services to read the SAMH header, validate the message and extract PACS data. Valid messages are accepted through usp_SWF_IN_ETL_QUALIFY_AUTO, queued to LAIKA, may trigger an MT191 "OUR" commission request, and have rules applied. Invalid ones are routed to manual processing through usp_SWF_IN_ETL_QUALIFY_MANUAL with a rejection reason. Payments from Mexican banks in USD can additionally trigger a real rejection message through the separate Swift.Api.Rejection API. Every outcome moves money-related operations, so correctness of state transitions is business-critical.
1.1 Issue Summary
Recommended priority	Count	Simple meaning
Blocker	7	Close before approving a release carrying these paths
High	16	Immediate correction/verification or an explicit accountable release decision
Medium	14	Planned reliability, security or maintainability work
Low	3	Optional naming/cleanup after correctness work
Total distinct findings	40	One category per finding; cross-cutting items are cross-referenced, not duplicated

Skill category	Count
MustChange/rewrite	25
rewrite_recommended	12
NiceToRewrite_ifTimePermits	3

Priorities are source-review release recommendations, not CVSS or proof of production exploitation. Several findings are *conditional* on code that lives in the external MUFG_T24.dll or in database objects that are not in this repository; each says so. The highest-value fixes are: stop the SPID rejection/acceptance double path (SIE-FR-1), make acceptance atomic and visible (SIE-FR-2, SIE-FR-26), close the fail-open validation gates (SIE-FR-3, SIE-FR-33), stop leaking the bearer token (SIE-FR-31/32), repair the three XML interpreters that can never succeed (SIE-FR-6/7/8), and make the test project executable (SIE-FR-39).
2. Scope & Methodology Note
Area	Evidence / limitation
Source scope	180/180 authored text files, 14,983/14,983 physical lines fully read, not sampled: 166 C# files / 13,828 lines (162 production files / 11,659 lines excluding 2 generated WCF proxies of 1,212 lines and 2 test files of 957 lines; the production count includes 28 camt model files), plus 14 non-C# files: 4 .csproj, .slnx, 2 packages.config, App.config, 2 ConnectedService.json, README, .gitignore, .gitattributes and one .editorconfig
Coverage record	SHA-256/physical-line manifest (./_SWF_IN_ETL_FileManifest.csv) and file list (./_SWF_IN_ETL_FileList.txt)
Exclusions	Git internals, bin/obj/packages/.vs (none present), and the external binaries MUFG_T24.dll, SimpleInjector.dll referenced from D:\ServiceCMS\: not available on this machine and not reviewed
Review basis	Full current-source architecture/logic audit, not a changed-lines PR verdict. The last commit 0b44d25 ("CodeReview HMN fixes") changed 131 files in this repository; SIE-FR-9 was introduced by it
Build	Solution not built: no .NET SDK, msbuild or Visual Studio is installed and D:\ServiceCMS\MUFG_T24.dll is absent. Instead, the built-in .NET Framework csc 4.0.30319 and the .NET XPath/Regex/culture APIs were used for six isolated probes on synthetic data (all in the validation record (./_SWF_IN_ETL_Validation.md)); they confirm SIE-FR-6 (XPath), SIE-FR-9 (CS0136), SIE-FR-18, SIE-FR-19, SIE-FR-23, SIE-FR-29
Tests	One MSTest file exists. 20 [TestMethod]s were read; none could be executed here (see SIE-FR-39). Measured coverage is unavailable
External evidence not used	No configuration/secret/connection file, database, T24 endpoint, queue or live message was accessed; stored-procedure bodies, DB constraints, the T24 WCF contracts and the MUFG_T24 library semantics are unknown and called out per finding
Fix examples	Illustrative contracts/code directions, not compiled patches

Full-read batches in this audit:
Batch	Contents	Files	Physical lines	Status
B1	Host (7 files: Program, ServiceHost + Designer, AssemblyInfo, config, packages.config, csproj), test project (4), the two library .csproj	13	1,599	Full read complete
B2	Aplicacion: 29 interfaces, 23 DTOs, 2 enums, 11 constants, 6 top-level models, 28 camt model files	99	3,001	Full read complete
B3	Aplicacion: generated WCF proxies (2 Reference.cs + 2 ConnectedService.json), 7 services, 2 PACS interpreters	13	2,642	Full read complete
B4	Aplicacion: 4 camt operation classes, Tag57D (1), 22 utilities, OperacionServicio.cs (2,423 lines, read in 11 numbered chunks)	28	4,506	Full read complete
B5	Infraestructura: ApiClient + 21 repositories; root README.md, .slnx, .gitignore, .gitattributes; Aplicacion_IN_ETL\.editorconfig	27	3,235	Full read complete
Total	Entire authored scope	180	14,983	No pending range

2.1 Quantified Cross-Checks and Duplication
Counts were produced with Select-String | Measure-Object over the 162 production C# files, as a cross-check on top of the full read.
Pattern	Count	Meaning / finding
Config.EscribirLog calls	411 (222 at INFO|)	Static logger, ~120 in the camt model constructors alone; whole payloads logged (SIE-FR-16, SIE-FR-20)
catch (Exception / catch { blocks	131 (6 bare catch)	49 throw; rethrows, ~60 swallow/convert to a value (SIE-FR-4, SIE-FR-33)
GetValor.DameValor(...) parameter lines	22	Two identical 11-parameter blocks (IntegracionRepo, ActualizacionRepo) (SIE-FR-35)
base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)	13	Repeated repository constructors (SIE-FR-35)
Three copies of the nostro/debit-account fallback chain	3	OperacionServicio.ObtieneCuentaDebitoAsync, ObtenerCuentaAsync, PACS008MX.ResolverCuentaNostro (SIE-FR-15)
Two near-identical commission pre-processors	~90 lines x 2	ProcesarComisionAsync (MT) vs ProcesarComision (MX) (SIE-FR-15)
HintPath to D:\ServiceCMS\	5 (4 projects)	GEN-1 (SIE-FR-37)
.Pid.Value	2	GEN-8 (SIE-FR-14)
DateTime.Now / DateTime.UtcNow	2 / 1	Local-clock use (SIE-FR-13)
new Regex( / static Regex.*	3 / 22	No timeouts anywhere (SIE-FR-18)
Sync-over-async (.Result, .Wait(), GetAwaiter().GetResult())	0	Good
CancellationToken mentions outside the host	2	Token never reaches repositories or HTTP calls (SIE-FR-17)
OperacionServicio.cs	2,423 lines, 49 methods, 31 constructor arguments	God class (SIE-FR-15)
[TestClass] / [TestMethod] / Assert.	4 / 20 / 35	5 tests target production, 15 test local copies (SIE-FR-39)
Inline SQL (SELECT/INSERT/UPDATE/DELETE, SqlCommand)	0	Good: stored procedures only

2.2 False Positives Avoided / Open Requirements
Item	Decision
cuentaBusqueda declared twice in ValidaCuentaBTMUMAsync (line 752 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L752) and line 830 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L830))	Separate else scopes inside separate try blocks - not a compile error
catch (Exception) { WARN } in BankTransactionCodeStructure4/5 and EntryDetails9	Deliberate tolerance of optional ISO nodes; not flagged
EF FirstOrDefault left-join shape	Not present; no EF/LINQ-to-Entities in this solution
Dapper-style resultado.Data access	Only flagged where the sibling repositories use resultado?.Data and this one does not
ConstantesAux.Clave_CASFIM.Substring(2)	Guarded by length checks at each call site that was read
Stored-procedure bodies, MUFG_T24 library semantics (EjecutarStoredProcedureQuery, CrearParametro(null), ParseDecimalRequerido, ObtenerParametrosSistemaAsync, WindowsServiceHostBase), T24 WSDL contracts, deployed constraints	Unknown; every dependent finding states the assumption and the verification step
Business intent of the SPID-like rule, default debit account, MT-950 manual routing, fuzzy account matching	Unverified; recommendations ask for an owner decision rather than assert intent

3. What's Already Good
Strength	Evidence
Clean layering through project references: host -> Infraestructura -> Aplicacion, no circular reference	SWIFT_IN_ETL.csproj (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/SWIFT_IN_ETL.csproj), Infraestructura_IN_ETL.csproj (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Infraestructura_IN_ETL.csproj)
An interface exists for every repository/service (29 in Interfaces/) and OperacionServicio depends on abstractions	Interfaces (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interfaces), OperacionServicio.cs:53-83 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L53)
Composition root verifies the container, scopes each tick and clones configuration per scope	ServiceHost.cs:77 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L77), :125 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L125), :160 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L160)
All data access is parameterised stored procedures; no inline SQL and no string-concatenated commands anywhere	Scan in 2.1; e.g. ReglasRepo.cs:75-96 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/ReglasRepo.cs#L75)
No sync-over-async; async flows are awaited end to end and the service shutdown waits for in-flight tasks	ServiceHost.cs:176-215 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L176)
Result<T> is used for expected failures at the HTTP/T24 boundary instead of exceptions	Result.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/Result.cs), PacsService.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/PacsService.cs)
The nostro cache uses a SemaphoreSlim with double-checked loading and logs duplicate currencies	CuentasNostro.cs:32-73 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/CuentasNostro.cs#L32)
ConfiguracionHelper.GetBool/GetString use TryGetValue with defaults (no KeyNotFoundException)	ParametrosHelper.cs:26-58 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/Parametros/ParametrosHelper.cs#L26)
One repository wraps and preserves the inner exception (ApplicationException(..., ex))	CuentaRepo.cs:298 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/CuentaRepo.cs#L298)
Domain constants are centralised and typed (tags, message types, XML element names, SQL names)	Constantes (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Constantes)
Money in DTOs uses decimal; commission JSON parsing uses InvariantCulture	ChargesPaymentRequestDto.cs:20-23 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/ChargesPaymentRequestDto.cs#L20), DecimalFormatConverter.cs:25-36 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/DecimalFormatConverter.cs#L25)
An executable test seam was started (MSTest + Moq + FluentAssertions) and InternalsVisibleTo is configured	PruebasUnitarias.csproj (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/PruebasUnitarias.csproj), AssemblyInfo.cs:14 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/Properties/AssemblyInfo.cs#L14)

4. Findings by Project (low/no-dependency -> high dependency)
Finding IDs (SIE-FR-n) are stable references; they are not renumbered per category, so a category list can skip numbers. Line links point at the reviewed commit. "Checklist" cites a section of code_review.md (../BaseLine_Docs/code_review.md).
4.1 Aplicacion_IN_ETL (Tier 0)
MustChange / Rewrite
SIE-FR-1 - A payment can be rejected through SPID and accepted into the work plan at the same time
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 3 (correctness) and section 6 (shared mutable state), Blocker. Locations: OperacionServicio.cs:1067-1070 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1067), :1109-1131 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1109), :1734-1767 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1734), :1815 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1815), :1880 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1880), :2402-2416 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2402), ServicioSpid.cs:47-79 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/ServicioSpid.cs#L47), ConstantesAux.cs:32-33 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Constantes/ConstantesAux.cs#L32).
Current:
Code block (csharp)
bool spidLikePayments = await ValidateSpidLikePaymentsAsync(variablesXml.TipoMensaje, variablesXml.XmlDoc, null);
ConstantesAux.validateSpidLikePaymenst = spidLikePayments;          // static mutable global
...
bool gbSpidLikePayments = ConstantesAux.gbSpidLikePayments;         // public const bool = false
if (xml.PuedeInterpretarse && !gbSpidLikePayments) { await _integracion.AceptaOperacionSwtAsync(...); ... }
else if (!ConstantesAux.validateSpidLikePaymenst && gbSpidLikePayments) { ... }   // unreachable
For an MT103 in USD from a Mexican BIC whose ordering customer matches the legal-entity catalogue, ValidateSpidLikePaymentsAsync calls ProcesarRechazoSpidAsync, which sends a real rejection message to the sender through Swift.Api.Rejection ("THE PAYMENT IS FROM A MEXICAN BANK AND MUST BE PROCESSED THROUGH SPID"). Its result is only written to a static field. Because gbSpidLikePayments is a compile-time const false, the next step still accepts the operation into the work plan, queues it to LAIKA and applies rules: the counterparty is told to resend through SPID and the original payment is processed. For PACS008 the detection can never match: ObtenerDatosSpid reads the bank/name only from the PACS object for non-MT103 keys, but the call site passes null (:1880 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1880)). The MX local gbSpidLikePayments (:1815 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1815)) is never assigned true. Dead branches remain at :1131-1136 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1131), :1148-1151 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1148) and :1918-1935 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1918). A mutable public static bool is also shared state across scopes.
Recommended direction:
Code block (csharp)
var spid = await _spidDetector.EvaluateAsync(ctx);            // pure: returns Match/NoMatch, no side effects
if (spid.IsSpidLike)
{
    ctx.RazonRechazo = SpidReasons.MustUseSpid;
    ctx.PuedeInterpretarse = false;                           // single outcome: reject once
    await RejectOnceAsync(ctx, notifySender: true);           // idempotent, records that the MT-199 was sent
    return;
}
Make detection side-effect free, make the outcome part of ContextoXmlDTO (not a static), pass the real PACS data, delete the dead flag/branches/SpidLikePayments DTO, and record "rejection sent" so a retry cannot send it twice. Obtain the business owner's decision on whether SPID-like rejection is currently meant to be active. Benefit / acceptance: exactly one terminal outcome per ICN; a test asserts that when the SPID rejection API is called the operation is not accepted. Suggested JIRA: SIE-FR-1.
SIE-FR-2 - Accepting an operation is a chain of independent steps whose failures are only logged
Category: MustChange/rewrite. Priority: Blocker. Checklist: sections 3/5, exceptions not swallowed and check-then-act, Blocker. Locations: OperacionServicio.cs:1107-1172 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1107) (MT) and :1899-1964 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1899) (MX); wrappers at :1559-1652 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1559).
Current:
Code block (csharp)
await _integracion.AceptaOperacionSwtAsync(...);   operacionAceptada = true;
await _integracion.InsertLaikaQueueAsync(xml.ICN);                // swallows its own failure (SIE-FR-26)
... await EnqueueCommissionChargeRequestAsync(...);                // Result discarded
await AplicarReglasAsync(xml.ICN, xml.TipoMensaje);                // Result<bool> discarded
...
catch (Exception ex) { await MarcarOperacionErrorAsync(xml.ICN, ex.ToString()); ... }
Acceptance, LAIKA queueing, commission request and rule application are separate database/HTTP calls with no transaction or state machine. AplicarReglasAsync, RechazarOperacionAsync, MarcarOperacionErrorAsync and EnqueueCommissionChargeRequestAsync all catch internally and return Result<bool>, which every call site awaits and discards (GEN-9): a failed stored procedure leaves the operation half-processed with only a log line. If an exception occurs after acceptance (operacionAceptada == true), the catch marks the accepted operation as *error* with the full ex.ToString() stack text written into the database, and does not reject it. On a retry the operation can reappear as READ (see SIE-FR-28). The two MX/MT copies differ in small details (MX passes ex.Message, MT passes ex.ToString()).
Recommended direction:
Code block (csharp)
var outcome = await _acceptance.RunAsync(ctx, ct);    // one unit: accept + enqueue + rules, transactional or outbox-driven
if (!outcome.IsSuccess)
    await _alerts.RaiseOnceAsync(ctx.ICN, outcome.Stage, outcome.Reason);
Perform accept/queue/rules in one stored procedure or one transaction (or an outbox row processed idempotently), return a typed stage result, never discard it, store a short reason (not a stack trace), and give each operation an explicit retry/poison state. Benefit / acceptance: no accepted operation without a LAIKA row; injected failures at each stage leave a deterministic, alerted state; tests cover every stage failing. Suggested JIRA: SIE-FR-2.
SIE-FR-3 - Core validation silently disappears when the field-mapping stored procedure returns nothing
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 3 (input validation) and section 4 (null/empty results), Blocker. Locations: OperacionServicio.cs:609-626 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L609), SwiftRepo.cs:101-122 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/SwiftRepo.cs#L101).
Current:
Code block (csharp)
var reglas = await _swift.ObtenerMapeoCamposAsync(claveMensaje);   // returns Enumerable.Empty<> on null Data
if (reglas.Any()) { foreach (var regla in reglas) { ...flags, mandatory, conversions... } }
if (dtc.MensajeErroneo) return false;                               // all flags false => skipped
...
return true;
Every account, currency, CIF, vostro and mandatory-field check is *driven by the mapping rows*. If the stored procedure returns no rows (an unmapped message type, an outage reported as Exito=false, a bad cve_mensaje), reglas is empty, no flag is raised, every later if (dtc.Valida...) is skipped and ValidaDatosCoreAsync returns true: the message is accepted without any validation. The repository converts "no data" and "failed query" into the same empty sequence (see SIE-FR-33).
Recommended:
Code block (csharp)
var reglas = (await _swift.ObtenerMapeoCamposAsync(clave)).ToList();
if (reglas.Count == 0)
{
    return Reject(ctx, $"Sin mapeo de campos para {clave}");   // fail closed; distinguish outage from 'no rows'
}
Return a result type from the repository (Exito, rows) so an outage is retried instead of validated as empty, and fail closed for message types that require validation (explicitly list the types that legitimately have no mapping, e.g. informational MT-940). Benefit / acceptance: a test with an empty mapping set asserts rejection (or retry) and never acceptance. Suggested JIRA: SIE-FR-3.
SIE-FR-4 - Transient infrastructure failures become permanent business rejections
Category: MustChange/rewrite. Priority: High. Checklist: section 5, Blocker (exceptions must be handled meaningfully). Locations: OperacionServicio.cs:704-714 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L704), :1280-1283 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1280), :1195-1199 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1195), :1240-1244 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1240), :1436-1439 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1436).
Current:
Code block (csharp)
catch (Exception ex)
{
    Config.EscribirLog($"ERROR|ValidaDatosCore|{ex.Message}");
    dtc.RazonRechazo = "Error en validación datos core";
    return false;                       // caller then calls RechazarOperacionAsync -> permanent manual rejection
}
ValidaCuentaBTMUMAsync and the CIF/SPEI validators deliberately rethrow SQL, T24 and WCF failures (:790-794 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L790), :834-838 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L834)), but ValidaDatosCoreAsync converts every one of them into false plus a generic reason, so a 30-second T24 outage rejects customer payments for manual handling with a reason that hides the cause (only ex.Message is logged, no stack or type). SetMTCategoryAsync, GuardarCategoriaAsync, ObtenerMonedaT24Async, ObtenerCuentaAsync and ObtieneCuentaDebitoAsync swallow in the same way: the category (which routes automatic vs manual handling) can silently fail to save.
Recommended:
Code block (csharp)
catch (Exception ex) when (IsTransient(ex))
{
    _log.Warn(ex, "ValidaDatosCore transient failure ICN {Icn}", icn);
    throw new RetryableProcessingException(ex);       // leave the operation READ; retry next tick with backoff
}
catch (Exception ex) { _log.Error(ex, "..."); return Reject("Error en validación datos core"); }
Distinguish *retry later* from *reject now*, log exception type and stack, and let SetMTCategoryAsync failures fail the operation or raise one alert instead of continuing without a category. Benefit / acceptance: injected T24/SQL timeouts do not create rejections; the retry/alert behaviour is covered by tests. Suggested JIRA: SIE-FR-4.
SIE-FR-5 - Incoming "OUR" commission rates are hard-coded to zero, so commissions are never requested
Category: MustChange/rewrite. Priority: High. Checklist: section 3 (correctness) and section 7 (resources), Blocker. Locations: OperacionServicio.cs:1289-1374 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1289), ConstantesParametrosSistema.cs:30-33 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Constantes/ConstantesParametrosSistema.cs#L30), ConfiguracionSistema.cs:30-33 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Enums/ConfiguracionSistema.cs#L30).
Current:
Code block (csharp)
var commissionRequestRate = new ComissionRequestDateDTO
{
    CommisionRequest = new Dictionary<string, decimal> { { "USD", 0 }, { "MXN", 0 }, { "EUR", 0 }, { "JPY", 0 } }
};
var rate = commissionRequestRate.CommisionRequest.TryGetValue(currency, out var value) ? value : 0;
if (rate <= 0) { Config.EscribirLog("WARN|...parametrizada para no ser enviada."); return Result<bool>.Success(true); }
COMMISION_RATE_USD/EUR/JPY/MXN are loaded from the database on every tick (ConstantesParametrosSistema.ParametrosSistema) and exist as enum members, but no code reads them. The local dictionary is built from literal zeros, so for every MT103 with charges OUR (feature flag COMM_MT191IN_FEATURE_FLAG) and every pacs.008 OUR the method logs a misleading "parametrizada para no ser enviada" and returns *success* without calling the API. When the rate is positive it would never be configured. Separately, the HTTP response is never checked: response.StatusCode is only logged and the method returns Success(true) even for 4xx/5xx; the response is not disposed, there is no timeout/cancellation, and the payload (UETR, BICs, amounts) is logged. Callers also discard the result (:1126 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1126), :1913 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1913)).
Recommended:
Code block (csharp)
decimal rate = ConfiguracionHelper.GetDecimal(_configuracion.ParametrosSistema, RateKeyFor(currency)); // COMMISION_RATE_<CCY>
using (var response = await _httpClient.PostAsync(apiUrl, content, ct))
{
    if (!response.IsSuccessStatusCode)
        return Result<bool>.Failed($"COMM API {(int)response.StatusCode}");
}
Read the rates from the loaded parameters (and fail loudly if a required key is missing), check the status code, and make the caller act on the result. Ask the owner whether the feature is intentionally disabled; if so remove the dead path instead of leaving a silently-successful stub. Benefit / acceptance: a unit test sets a USD rate and asserts the request is posted; a 500 response yields a failed result. Suggested JIRA: SIE-FR-5.
SIE-FR-6 - Every camt.052 message is rejected: the model is built from the wrong XML node
Category: MustChange/rewrite. Priority: High. Checklist: section 3 (correctness), Blocker. Locations: camt_052.cs:18-31 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/TipoOperacion/camt_052.cs#L18), AccountReport25.cs:22-42 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/camt/052/AccountReport25.cs#L22), ConstantesXmlPath.cs:13 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Constantes/ConstantesXmlPath.cs#L13), RemoveNameSpace.cs:26 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/Xml/RemoveNameSpace.cs#L26).
Current:
Code block (csharp)
ParseAsXDocument(message);                  // expects /root/AppHdr and /root/Document
_ = new AccountReport25(message);           // message is the whole <root> XDocument
...
var statement = document.XPathSelectElement("./BkToCstmrAcctRpt/Rpt");   // relative to the document node
RemoveNamespace wraps the payload as <root>{xml}</root>. Evaluated from the *document node*, ./BkToCstmrAcctRpt/Rpt finds no child (the only child is root), so AccountReport25 throws "No pudo ser extraido el nodo Rpt" and InterpretarSegunTipoAsync sets PuedeInterpretarse=false/RazonRechazo (:2183 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2183)). camt_053 correctly keeps Document and passes it (camt_053.cs:23 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/TipoOperacion/camt_053.cs#L23)), so only camt.052 is affected. Verified by probe P1: XPathSelectElement("./BkToCstmrAcctRpt/Rpt") on an XDocument wrapped in <root> returns null, while the same path from the /root/Document element finds the node.
Recommended:
Code block (csharp)
private XElement Document { get; set; }                   // as camt_053
...
_ = new AccountReport25(Document);
Benefit / acceptance: a camt.052 fixture is interpreted and produces CIF/receiver elements; a malformed fixture is rejected with the specific reason. Suggested JIRA: SIE-FR-6.
SIE-FR-7 - camt.106 charge-request fields are read from the application header instead of the document
Category: MustChange/rewrite. Priority: High. Checklist: section 3, Blocker. Locations: camt_106.cs:45-58 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/TipoOperacion/camt_106.cs#L45), compare :60-65 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/TipoOperacion/camt_106.cs#L60).
Current:
Code block (csharp)
ExtractAndAdd(AppHdr,  "./ChrgsPmtReq/Chrgs/PerTx/ChrgsId",                 xmlDoc, ConstantesXml.theirRef, "theirRef");
ExtractAndAdd(AppHdr,  "./ChrgsPmtReq/Chrgs/PerTx/Rcrd/UndrlygTx/InstrId",  xmlDoc, ConstantesXml.ourRelatedRef, "ourRelatedRef");
var ttlChrgsAmt = AppHdr.XPathSelectElement("./ChrgsPmtReq/Chrgs/PerTx/Rcrd/TtlChrgsPerRcrd/TtlChrgsAmt");
...
ExtractAndAdd(Document, "./ChrgsPmtReq/Chrgs/PerTx/Rcrd/ChrgsAcctAgt/FinInstnId/BICFI", ...);   // same prefix, correct element
ChrgsPmtReq is the camt.106 body; the code reads it from AppHdr for three fields and from Document for the others, so the first mandatory extraction throws "No se encontró el dato requerido theirRef" and the charge request is rejected. Also custIdentifierCode is written twice (BIC at :60, account at :70) with the error text "chrgsAcctAgtBic" for the account. Verification: run a real camt.106 sample through GetVariablesXml.
Recommended: use Document for all ChrgsPmtReq paths, give the BIC and the account separate element names, and add a fixture test. Benefit / acceptance: inbound MT191-equivalent charge requests reach ProcesarComision. Suggested JIRA: SIE-FR-7.
SIE-FR-8 - Inverted status guard makes every camt statement entry fail and be dropped silently
Category: MustChange/rewrite. Priority: High. Checklist: section 3 (logic contradicts its message) and section 5 (swallowed errors), Blocker. Locations: ReportEntry10.cs:72-80 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/camt/053/ReportEntry10.cs#L72), AccountStatement9.cs:90-110 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/camt/053/AccountStatement9.cs#L90), AccountReport25.cs:89-110 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/camt/052/AccountReport25.cs#L89).
Current:
Code block (csharp)
var node = document.XPathSelectElement(ConstantesXmlPath.Sts);
if (node != null)                                     // inverted
{
    throw new Exception("No fue posible extraer el nodo Sts");
}
Status = new EntryStatus1Choice(node);                // node is null here -> ctor throws
When Sts exists the constructor throws; when it is missing EntryStatus1Choice(null) throws. Every ReportEntry10 therefore fails. ExtractEntries catches each per-entry exception, logs a WARN and continues, so Entries is silently empty and the message is still "interpreted". Further: the log prefix says BalanceType13.ctor, the message says "tryDtls", and BkTxCd/NtryDtls are made mandatory although optional in ISO 20022. This defect pre-dates the last commit (the inverted condition was in the old try block).
Recommended:
Code block (csharp)
var node = document.XPathSelectElement(ConstantesXmlPath.Sts)
           ?? throw new FormatException("camt entry has no Sts element");
Status = new EntryStatus1Choice(node);
Count dropped entries and fail the statement when any entry is dropped (or record the count in the XML/alert), make optional ISO nodes optional, and add a fixture test with several entries. Benefit / acceptance: a camt.053 with N entries yields N entries; partial parse loss is visible. Suggested JIRA: SIE-FR-8.
SIE-FR-9 - Nested local node redeclaration (CS0136) was introduced by the last commit
Category: MustChange/rewrite. Priority: Blocker. Checklist: build breaks, Blocker. Status: the same declaration shape was confirmed to fail with the installed .NET Framework compiler (csc v4.0.30319: error CS0136, see the validation record (./_SWF_IN_ETL_Validation.md)); the solution itself could not be built here. Location: BalanceType13.cs:28 and :44 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/camt/053/BalanceType13.cs#L28).
Current:
Code block (csharp)
XElement node = document.XPathSelectElement(ConstantesXmlPath.CdOrPrtry);   // constructor scope
...
try
{
    XElement node = document.XPathSelectElement(ConstantesXmlPath.SubTp);   // nested scope, same name
Commit 0b44d25 removed the try { ... } wrapper that used to scope the first node (diff 7aa47fd..0b44d25), leaving a local with the same name declared in an enclosing scope. C# reports CS0136 for this shape, which would fail the build of Aplicacion_IN_ETL. The catch { WARN } around SubTp also hides a malformed optional element. Verification: build SWIFT_IN_ETL.slnx with the real toolchain (msbuild SWIFT_IN_ETL.slnx /p:Platform=x86).
Recommended: rename the second variable (subTypeNode), add a CI build gate, and run the solution build as part of every review. Benefit / acceptance: clean compile; CI fails on warnings-as-errors for this class of defect. Suggested JIRA: SIE-FR-9.
SIE-FR-10 - Amount and currency are never stored on the context for MX messages, so MX rejections record 0 / null
Category: MustChange/rewrite. Priority: High. Checklist: section 3 (money correctness), Blocker. Locations: PACS008MX.cs:93-109 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interpretadores/PACS008MX.cs#L93), PACS009MX.cs:73-89 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interpretadores/PACS009MX.cs#L73), OperacionServicio.cs:1608 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1608).
Current:
Code block (csharp)
ctx.Moneda = currency;                                       // PACS008 only
XmlUtils.AgregaElemento(ctx, ConstantesXml.mensajeImporte, pacs.intrBkSttlmAmt);   // string only; ctx.Importe never assigned
...
await _reglas.RechazarOperacionAsync(icn, ..., xml.RazonRechazo, tipoMensaje, xml.Moneda, xml.Importe, fechaValor);
Only the MT header path assigns ContextoXmlDTO.Importe (LeerHeaderRepo.cs:204-210 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/LeerHeaderRepo.cs#L204)); a repository-wide search finds no other assignment. For pacs.008/pacs.009 the rejection stored procedure therefore receives amount 0 (and a null currency for pacs.009), and the amount string is never validated as a number. Operators looking at manual-queue rows see a zero amount.
Recommended: parse intrBkSttlmAmt with invariant culture into ctx.Importe (reject non-numeric/non-positive values), set ctx.Moneda in both interpreters, and test the rejection call arguments. Benefit / acceptance: MX rejection rows show the real amount/currency. Suggested JIRA: SIE-FR-10.
SIE-FR-11 - A tag that fails its syntax check does not invalidate the message
Category: MustChange/rewrite. Priority: High. Checklist: section 3 (allow-list validation), Blocker. Confidence: medium - confirm the intended rule with the owner. Location: OperacionServicio.cs:338-457 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L338).
Current:
Code block (csharp)
if (!match.Success)
{
    Config.EscribirLog($"INFO|ProcesaTags|No se pudo interpretar TAG {tag}");
    await _alerta.AgregaAlertaAsync(variablesXml.ICN, "No se pudo interpretar la información ... TAG " + tag);
    continue;                                   // TagsValidos stays true
}
The two sibling branches (:354 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L354), :363 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L363)) set TagsValidos=false and the method then rejects with "No se pudo establecer el tipo de mensaje". A *syntax mismatch* only raises an alert row and the message continues without that tag's values; a tag absent from the syntax dictionary is silently ignored (:348 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L348)). If a mandatory field (:32A:, :59:, :57A:) has an invalid format, acceptance relies on SIE-FR-3's mapping checks being present. The unit test ProcesaMensaje_CuandoFallaTag_DeberiaRegistrarAlerta tests only a local helper, so the behaviour is unspecified.
Recommended: decide per tag status (M/O, available from the field mapping) - a mandatory tag that fails syntax sets TagsValidos=false with the tag name in the reason; optional tags alert only. Write the rule as a table-driven test. Benefit / acceptance: documented, tested handling of invalid mandatory tags. Suggested JIRA: SIE-FR-11.
SIE-FR-12 - Vostro and SPEI2 validation use the wrong inputs and share one mandatory flag
Category: MustChange/rewrite. Priority: High. Checklist: section 3, Blocker. Locations: OperacionServicio.cs:1027-1052 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1027), :1014-1018 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1014), ProcesaCampo.cs:96-101 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/ProcesaCampo.cs#L96), IVostro.cs:15 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interfaces/IVostro.cs#L15).
Current:
Code block (csharp)
Config.EscribirLog($"... CIF vostro. CIF o SWF = {dtc.CIFVostro}.Es Mandatorio = {dtc.CIFVostroMandatorio}");
var cuentaVostro = await _vostro.ObtenerCuentaVostroAsync(dtc.CIFSender);      // looks up the SENDER, not CIFVostro
...
case "num_cif_spei_2":
    ctx.CIFSpei2 = valor;
    ctx.CIFSpeiMandatorio = estatus == "M" ? true : estatus == "O" ? false : ctx.CIFSpeiMandatorio;   // overwrites the SPEI flag
ValidaCIFVostroAsync logs and rejects on CIFVostro but queries by CIFSender; the interface method designed for the vostro CIF (ObtenerCuentaCIFVostroAsync) has no caller. ValidaCIFSpei2Async rejects with CIFSpei instead of CIFSpei2 (:1016-1017 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1016)) and ProcesaCampo has no CIFSpei2Mandatorio, so the last mapping row wins. It also overwrites nombre_cliente with the vostro remitter (:1037 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1037)). Note the repository method also sends the value in the @cve_swift parameter (see SIE-FR-36).
Recommended: query the vostro by the vostro CIF (confirm which catalogue key the SP expects), add CIFSpei2Mandatorio, and correct the messages. Benefit / acceptance: per-field mandatory handling and rejection texts identify the field that failed. Suggested JIRA: SIE-FR-12.
SIE-FR-13 - Absent or blank values are handled by crashing, defaulting or a misleading reason
Category: MustChange/rewrite. Priority: Medium. Checklist: section 4 (null safety), Blocker. Locations: Conversiones.cs:25-45 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/Conversiones.cs#L25), OperacionServicio.cs:649-650 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L649), :307-316 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L307), :1893 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1893).
Current:
Code block (csharp)
if (funciones.Contains("convertir_importe"))
    valor = valor.Replace(",", CultureInfo.InvariantCulture.NumberFormat.NumberDecimalSeparator);   // valor may be null
...
if (dtc.FechaMandatoria && string.IsNullOrWhiteSpace(dtc.FechaSwift))
    dtc.FechaSwift = DateTime.Now.ToString("yyyyMMdd");
...
if (sintaxisMT.Count > 0 && variablesXml.Clave != ConstantesCategoriaMT.MT950) { ... }
else { variablesXml.RazonRechazo = "MT-950 se procesa manualmente"; ... }
...
if (ctx.RazonRechazo == "") ...
(1) XmlHelper.ObtenerValor returns null for an absent tag, so convertir_importe throws NullReferenceException, which SIE-FR-4 turns into a permanent rejection with a generic reason (the sibling date branch has the null guard). convertir_fecha also blindly prepends "20". (2) A *mandatory* value date that is missing is fabricated from the local clock instead of rejected (and not the business date _configuracion.FechaSistema). (3) Any message with zero syntax rows - not only MT-950 - is rejected with the text "MT-950 se procesa manualmente". (4) == "" leaves a null reason null, which is then passed to the rejection stored procedure.
Recommended: guard nulls, reject a missing mandatory date, branch separately on "no syntax rows" and "MT-950", and use string.IsNullOrWhiteSpace. Benefit / acceptance: table-driven tests for null/blank/invalid inputs; rejection reasons name the real cause. Suggested JIRA: SIE-FR-13.
SIE-FR-14 - Out-of-window ticks raise an alert per tick; the alert path dereferences a nullable PID
Category: MustChange/rewrite. Priority: Medium. Checklist: section 4/5, Blocker. Standing findings: GEN-8, GEN-10. Locations: OperacionServicio.cs:139-173 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L139), :178-191 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L178), ServiceHost.cs:144 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L144), :156-171 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L156).
Current:
Code block (csharp)
if (_configuracion.EsDiaHabil != true) { ... await RegistrarAlertaAsync(ClaveAvisoError, mensaje, new Exception(mensaje)); return false; }
if (horaSistema < horaInicio || horaSistema > horaFin) { ... await RegistrarAlertaAsync(...); return false; }
...
PId = _configuracion.Pid.Value,                       // inside the alert path, used from catch blocks
Weekends, holidays and every night produce an ERROR log and a new alert row each tick (at 10 s: 360 rows/hour). The alert helper itself uses Pid.Value unguarded, so a missing PID makes the *error handler* throw (and ParametrizacionesAsync has the same unguarded access at startup). The ParametrosSistema indexers at :158-161 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L158) throw KeyNotFoundException if the parameter is missing. ServiceHost.OnTimerAsync catches everything and only logs, so no state/alert distinguishes a failed tick from a skipped one.
Recommended: model "closed window/non-business day" as Skipped and log at INFO once per transition; raise an alert once per transition; validate Pid once at startup; use the existing GetString helpers or TryGetValue for the window parameters. Benefit / acceptance: zero alert rows for a normal closed window; tests for a missing PID and a missing parameter. Suggested JIRA: SIE-FR-14.
SIE-FR-16 - Full payment payloads and commission JSON are written to the log at INFO
Category: MustChange/rewrite. Priority: High. Checklist: section 5 "logs never contain ... PII or full sensitive payloads", Blocker. Locations: OperacionServicio.cs:2089 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2089), :2117 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2117), :2126 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2126), :2138 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2138), :2187 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2187), :2197 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2197), :2215 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2215), :2225 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2225), :1357 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1357), ApiCommissionClientRepo.cs:68 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/ApiCommissionClientRepo.cs#L68), CuentaRepo.cs:87 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/CuentaRepo.cs#L87).
Current:
Code block (csharp)
Config.EscribirLog($"INFO|InterpretarMensajes|VariablesXML = {xmlString}");        // names, accounts, amounts, UETR
Config.EscribirLog($"INFO|EnqueueCommissionChargeRequestAsync|Payload: {json}");
The whole interpreted XML (ordering/beneficiary names and addresses, account numbers, amounts, UETR) is logged at least twice per MX message and the commission JSON with BIC/UETR/amount is logged on every request; account candidates and T24 customer names are logged elsewhere. Log files are retained and readable by operators. SamhService and RequestApiRejection also log URLs and responses (see SIE-FR-22 and SIE-FR-32). Several log lines are also wrong (:2024 prints the reason before assigning it; :1981, :2032 print ctx.Folio, which is null at that point; failures are logged as INFO).
Recommended: log identifiers only (ICN, UETR, message type, length, outcome), mask accounts, and keep payload capture behind a disabled-by-default, access-controlled debug switch; use a structured logger instead of the static helper. Benefit / acceptance: a log-scan test finds no account/name/payload strings; the log size drops substantially. Suggested JIRA: SIE-FR-16.
Rewrite Recommended
SIE-FR-15 - OperacionServicio is a 2,423-line god class with three copies of the same business chains
Category: rewrite_recommended. Priority: Medium. Checklist: section 2 (structure), Major. Standing finding: GEN-4 style duplication. Locations: OperacionServicio.cs:92-104 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L92) (31-argument constructor), :505-568 vs :2246-2335 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L505) (commission pre-processing), :1177-1245 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1177) and PACS008MX.cs:489-580 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interpretadores/PACS008MX.cs#L489) (debit/nostro account fallback), :2341-2397 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2341) (PACS008/009 data requests).
One class parses MT, orchestrates MX, validates T24 accounts, categorises messages, calls the commission API and applies rules; it takes 31 constructor arguments (several concrete: ConexionT24Mw, DatosCuenta, ProcesaCampo, SwiftStandardReleaseSanitize) and is only testable through reflection (SWT_TEST.cs:262 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L262), :299 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L299)). The MT and MX commission pre-processors are ~90-line near-copies; the debit-account chain (correspondent -> sender -> default) exists three times; RequestDataPacs008/009 differ only by key/text; the accept/reject tail exists twice (SIE-FR-2). Every future fix (e.g. SIE-FR-12) must be applied in several places.
Recommended direction: extract IMtPipeline, IMxPipeline, ICommissionService (one pre-processor), IDebitAccountResolver (one chain) and a MessageOutcomeWriter used by both pipelines; register a message-type strategy registry instead of the if/else if tag/type ladders; inject a clock and a logger. Sequence this after the defect fixes above. Benefit / acceptance: each extracted unit has its own tests; the three copies become one; constructor arguments drop below ~8. Suggested JIRA: SIE-FR-15.
SIE-FR-17 - One failing message blocks the batch; cancellation and retry are not designed
Category: rewrite_recommended. Priority: Medium. Checklist: section 6, Major. Locations: OperacionServicio.cs:215-257 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L215), :1783-1819 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1783), IOperaciones.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interfaces/IOperaciones.cs).
CargaMensajesAsync awaits each operation sequentially and rethrows on the first exception; CrearContextoXmlAsync in the MX loop is outside the try, so a throwing message aborts every message behind it in the same tick, every tick. The operation stays READ (nothing marks it), so the same poison message is retried forever and, through SIE-FR-28, can turn into a false "UETR duplicado". Only 2 mentions of CancellationToken exist outside the host: no repository, HTTP or SQL call receives the token, so a stop request waits for the full 100 s HttpClient timeout.
Recommended: isolate each message (try/catch per ICN that records the failure and continues), add a bounded retry counter with a poison state and a single alert, thread CancellationToken through the interfaces, and add per-call timeouts. Benefit / acceptance: a bad message does not delay later ones; a cancellation test completes within seconds. Suggested JIRA: SIE-FR-17.
SIE-FR-18 - Regexes are built from database text without timeouts, and two tag tokenisers disagree
Category: rewrite_recommended. Priority: Medium. Checklist: section 3/7, Major. Locations: RegexHelper.cs:15-175 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/RegexHelper.cs#L15), OperacionServicio.cs:370 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L370), :1692-1728 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1692), Tags.cs:22 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/Tags/Tags.cs#L22).
Current:
Code block (csharp)
var expresion = new Regex(sintaxis, RegexOptions.Multiline);                       // sintaxis assembled from des_regexp/des_content rows
string multilinea = $@"({linea}\r?\n){{0,{maxLineas - 1}}}{linea}";                // nested quantifier over DB-defined classes
var pattern = @"(?<=\r\n|^):\d{2}[A-Z]?:";                                         // BuscarTags: needs CRLF, no Multiline
var patronTags = @"\:[0-9]{2}?[A-Z]?\:";                                           // ObtenerParteMensaje: different tokenizer
Syntax rows are data: a bad row or an adversarial/odd message can backtrack heavily (no matchTimeout on any of the 25 regex uses) and one bad Regex throws for the whole message. The field type x/z classes include \s (which matches line breaks), so type checks are looser than SWIFT. BuscarTags only recognises tags after \r\n while ObtenerParteMensaje matches :NN[A]: anywhere (including inside narrative text), and its footer heuristic \-(...){0,50}?$ (no Multiline, allow-list excludes }) can truncate the last field if a - appears in the final 50 characters. {2}? is a lazy quantifier on an exact count (no effect). Probe P4 confirms that the footer pattern does not match a message ending in -} and that BuscarTags finds 3 tags in a CRLF message but only 1 in the LF-only equivalent. Cache the compiled regex per (message type, tag).
Recommended: one tokenizer used by both steps, new Regex(pattern, options, TimeSpan.FromSeconds(1)) + RegexMatchTimeoutException handling, a cache, validated syntax rows (unit test that every production row compiles), and fixture tests with LF-only, CRLF and - inside narrative fields. Benefit / acceptance: bounded evaluation time; one definition of "tag". Suggested JIRA: SIE-FR-18.
SIE-FR-19 - Name/address folding duplicates lines, deletes Spanish letters and exceeds FIN widths
Category: rewrite_recommended. Priority: Medium. Checklist: section 3 (boundaries), Major. Locations: PACS008MX.cs:188-285 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interpretadores/PACS008MX.cs#L188), :342-410 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interpretadores/PACS008MX.cs#L342), :441-486 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interpretadores/PACS008MX.cs#L441), TextoMultilinea.cs:13-42 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/Texto/TextoMultilinea.cs#L13).
Current:
Code block (csharp)
int limite = 3 - nameLines.Count;                       // pre-truncation count
if (totalDir > limite) { var overflow = dirLines.Skip(limite).ToList(); ... }   // Skip(negative) = all
...
input = Regex.Replace(input, @"[^A-Z0-9\s\.,\-\/]", "");  // removes Ñ, accents
When the name has three or more lines limite <= 0; the first address line was already appended to the third principal line, yet Skip(limite) returns *all* address lines, so that line appears twice (principal and *_ext). Normalize strips characters outside A-Z0-9 .,-/ instead of transliterating, so PEÑA becomes PEA and accented names/addresses lose letters (probe P5: PEÑA JOSÉ -> PEA JOS). Split never breaks a single word longer than 35 characters (FIN line limit), and TextoMultilinea emits a blank first line when the first word exceeds the width. The address concatenation block is copy-pasted three times plus BuildDireccion and PACS009's ConstruirDireccionEstructurada, with two different line splitters.
Recommended: one FinTextFormatter (transliterate Ñ->N, Á->A, wrap words to the width, return overflow separately) with unit tests for 1, 2, 3 and 5 name lines, long words and accents. Benefit / acceptance: deterministic, lossless-where-possible folding; one implementation. Suggested JIRA: SIE-FR-19.
SIE-FR-21 - The nostro cache never refreshes, and a silent default debit account is used with only an alert
Category: rewrite_recommended. Priority: Medium. Checklist: section 7 (unbounded/stale caches), Blocker-adjacent; business intent unverified. Locations: CuentasNostro.cs:20-86 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/CuentasNostro.cs#L20), ServiceHost.cs:92-93 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L92), OperacionServicio.cs:1227-1238 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1227), Interpretadores/PACS008MX.cs:545-551 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Interpretadores/PACS008MX.cs#L545).
The singleton loads currency -> account once for the process lifetime; changes require a service restart, and if the load returns an empty list (SIE-FR-33) the empty dictionary is kept permanently. GetCuenta uses ToUpper() while the loader uses ToUpperInvariant(). When neither correspondent nor sender has a nostro the code logs "se utilizara la cuenta predeterminada ... (JP Morgan)", writes an alert row and continues with that account: a financial default that is only visible as an alert. Confirm with the owner that this fallback is intended for every currency and customer.
Recommended: time-bounded cache (re-load every N minutes or on a version stamp), never cache an empty result, make the fallback explicit in configuration with a reason code on the operation. Benefit / acceptance: nostro changes apply without restart; fallback usage is reportable. Suggested JIRA: SIE-FR-21.
SIE-FR-22 - HTTP service wrappers leak exception text, duplicate code and ignore timeouts
Category: rewrite_recommended. Priority: Medium. Checklist: sections 5 and 7, Blocker (stack traces, HttpClient reuse). Locations: SamhService.cs:43-107 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/SamhService.cs#L43), PacsService.cs:48-146 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/PacsService.cs#L48), OperacionServicio.cs:2022-2026 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L2022).
Current:
Code block (csharp)
catch (Exception ex) { return Result<Response_Validator_OK>.Failed($"ValidarMensaje|MX|{ex}"); }     // whole exception incl. stack
...
ctx.RazonRechazo = result?.Reason ?? "Error validando mensaje MX";                                     // persisted in the DB reason column
ValidarMensajeAsync returns the full exception text (host name/URL, stack) as Reason; the caller stores it as the rejection reason in the work table, so stack traces and endpoint details reach the operator UI. The two PACS calls are 45-line twins; responses are not disposed; there is no CancellationToken, per-call timeout or retry/backoff; non-success JSON parsing (ValidarMensaje) is outside any inner try; the shared HttpClient also carries a leaked bearer header (SIE-FR-31).
Recommended: one ExternalJsonClient with typed errors, return a short code in Reason and log the exception separately, dispose responses, configure timeout, and retry transient 5xx with a bounded policy. Benefit / acceptance: no stack text in rejection reasons; a test asserts the reason for 4xx/5xx/timeouts. Suggested JIRA: SIE-FR-22.
SIE-FR-23 - Culture-dependent and inconsistent number/date formatting on payment values
Category: rewrite_recommended. Priority: Medium. Checklist: section 3 (money/date), Blocker-adjacent. Standing finding: GEN-12. Locations: DecimalFormatConverter.cs:58 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/DecimalFormatConverter.cs#L58), LeerHeaderRepo.cs:226 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/LeerHeaderRepo.cs#L226), ConvertirFecha.cs:28-41 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/Fecha/ConvertirFecha.cs#L28), ApiCommissionClientRepo.cs:66 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/ApiCommissionClientRepo.cs#L66).
Current:
Code block (csharp)
writer.WriteValue(decimalValue.ToString("0.00"));                 // current culture; always 2 decimals
XmlHelper.AgregarElemento(doc, ConstantesXml.mensajeImporte, xml.Importe.ToString());   // current culture
if (lsFecha.Length == 8 && long.TryParse(lsFecha, out _)) return lsFecha;                 // yyyyMMdd
if (lsFecha.Length == 10 && ...) return lsFecha.Substring(2, 2) + ... ;                  // yyMMdd  (different contract)
Reading is invariant but writing is not (probe P6: 1234.5.ToString("0.00") is 1234,50 under es-ES and 1234.50 under en-US): a service host running under a comma-decimal culture would send 1,00/1234,56. The commission DTO is serialized with the converter (string "0.00") in one place and as a bare number in ApiCommissionClientRepo; two-decimal rounding is wrong for zero-decimal currencies. ConvierteFecha returns eight digits for one input shape and six for the other and null for anything else, silently.
Recommended: ToString("0.00", CultureInfo.InvariantCulture)/G29 per currency minor units, one serializer configuration for the DTO, and one documented date contract with an explicit failure for unknown shapes. Benefit / acceptance: tests run under es-ES and en-US. Suggested JIRA: SIE-FR-23.
SIE-FR-24 - Tag 57D handling overwrites parsed data and can look up arbitrary text
Category: rewrite_recommended. Priority: Medium. Checklist: section 3, Major. Location: ProcesaTag57D.cs:19-123 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Tag57D/ProcesaTag57D.cs#L19).
When 57D has four lines the code reads nombreBanco/id/ciudad/pais from the message, then unconditionally replaces name, city and country with the lookup result (:94-96 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Tag57D/ProcesaTag57D.cs#L94)), so the parsed values are dead. For any other 57D shape id = lineas.LastOrDefault() (often a country or free text) is sent to ObtenerBancoValidado. Rejections carry the magic prefix SSR2026:, the class is static while taking an injected dependency, and the BIC regex ^[A-Z0-9]{8}([A-Z0-9]{3})?$ is looser than ISO 9362.
Recommended: parse option A/B/D explicitly per SWIFT rules, reject unknown shapes with a clear reason, compare the lookup with the message data, register it as a service, and replace the prefix with a reason enum. Benefit / acceptance: fixture tests for 57A, 57D four-line and free-text forms. Suggested JIRA: SIE-FR-24.
Nice to Rewrite if Time Permits
SIE-FR-20 - camt model polish: log noise, mandatory optional nodes and discarded parse results
Category: NiceToRewrite_ifTimePermits. Priority: Low. Locations: Modelos/camt (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/camt), camt_052.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/TipoOperacion/camt_052.cs), camt_053.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/TipoOperacion/camt_053.cs).
(1) ~120 EscribirLog("INFO...") calls in the constructors log every node extraction (XmlUtils.TryExtractNodeValue logs twice per call). (2) Pagination1 and BalanceType10Choice make optional ISO elements mandatory, so a report without RptPgntn fails the whole message. (3) CashAccount39:35-38 replaces the inner exception (throw new Exception("...Id")). (4) camt_052/053 build the whole model only to validate and discard it (_ = new AccountStatement9(...)); ProcessMessage adds just two BIC elements, so balances/entries never reach the ETL. (5) ProcessMessage/ParseAsXDocument are copy-pasted across camt_052/053/106 (error text in camt_053:109 says camt.052). (6) AccountReport25 is not sealed like its siblings; camt_057.cs plus its 7 model files are excluded from compilation (Aplicacion_IN_ETL.csproj:8,13 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Aplicacion_IN_ETL.csproj#L8)) while the dispatcher still sets a manual-processing reason for camt.057. Fix after SIE-FR-6/8: use a base class for the shared extraction, remove per-node INFO logging, and decide whether camt.052/053 are informational (then record that) or parsed (then use the model). Suggested JIRA: SIE-FR-20.
SIE-FR-25 - Dead code, mutable constants and naming drift in the application layer
Category: NiceToRewrite_ifTimePermits. Priority: Low. Locations: CortarTexto.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Utilidades/Texto/CortarTexto.cs) (194 lines, no callers; strCadena computed and unused), Moneda.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/Moneda.cs) (never registered or constructed), SpidLikePayments.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/DTO/SpidLikePayments.cs), SwiftISO20022.cs:22 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/SwiftISO20022.cs#L22) (MyProperty) and :139-161 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Modelos/SwiftISO20022.cs#L139) (commented MgmtInfo), ConstantesSql.cs:14,23,29,36 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Constantes/ConstantesSql.cs#L14) (ELT typos), ConstantesServicio.cs:8 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Constantes/ConstantesServicio.cs#L8) ("Swf_ETL" vs Swf_IN_ETL).
Also: Aplicacion_IN_ETL.DTOs namespace in a DTO folder (the project's own .editorconfig sets dotnet_style_namespace_match_folder = true), mixed Spanish/English and snake_case DTO members, Async suffix missing on BuscaSwEnd2EndRef, ProcesarComision, RequestDataPacs008, three using static, #region blocks. The typo'd stored-procedure names are tracked as GEN-13 candidates (confirm against the deployed database). Suggested JIRA: SIE-FR-25.
4.2 Infraestructura_IN_ETL (Tier 1)
MustChange / Rewrite
SIE-FR-26 - InsertLaikaQueueAsync swallows every failure, so accepted operations can silently never reach LAIKA
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 5 (every catch handled meaningfully) and section 3, Blocker. Location: IntegracionRepo.cs:99-122 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/IntegracionRepo.cs#L99).
Current:
Code block (csharp)
public async Task InsertLaikaQueueAsync(string icn)
{
    try
    {
        var result = await EjecutarStoredProceduredNonQuery(ConstantesSql.USP_ENQUEUELAIKAHEAP, cmd => { ... "@payload", icn ... });
        if (result?.Exito != true)
        {
            Config.EscribirLog($"ERROR|InsertLaikaQueue|ICN:{icn}|{result?.MensajeError ?? "Resultado nulo"}");   // no throw, no return value
        }
    }
    catch (Exception ex) { Config.EscribirLog($"ERROR|InsertLaikaQueue|ICN:{icn}|{ex}"); }                      // swallowed
}
The method returns Task (no result) and both failure paths only log. AceptaOperacionSwtAsync has already committed the operation, so it is accepted but never queued for LAIKA, with no alert, no error status and no retry. The XML-doc summary even promises a StoredProcedureResult. The unit test ProcesarResultado_CuandoFallaColaDespuesDeAceptar_NoRechazaOperacion (SWT_TEST.cs:177 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L177)) mocks InsertLaikaQueueAsync to throw, which this implementation can never do, so the test documents behaviour that production does not have.
Recommended:
Code block (csharp)
if (result?.Exito != true)
    throw new QueueingFailedException(icn, result?.MensajeError);      // let the caller's stage handling mark/alert/retry (SIE-FR-2)
Make the method return a result or throw, and call it inside the same unit of work as acceptance. Benefit / acceptance: a failing usp_SWF_IN_ETL_ENQUEUE_LAIKA_HEAP produces a visible, retryable state. Suggested JIRA: SIE-FR-26.
SIE-FR-27 - Header validation requires currency and amount for every message type, so statements (MT940/MT950/MT998) can only be rejected
Category: MustChange/rewrite. Priority: Medium. Checklist: section 3 (correctness), Blocker. Confidence: medium - depends on which types the work-table query actually returns. Locations: LeerHeaderRepo.cs:71-124 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/LeerHeaderRepo.cs#L71), :141-161 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/LeerHeaderRepo.cs#L141), :204-212 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/LeerHeaderRepo.cs#L204); consumers OperacionServicio.cs:1254-1260 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1254), :307-316 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L307).
Current:
Code block (csharp)
// ParseHeader: Moneda/Importe are only read from :32A: (or :32B: for 191)
if (header.Moneda == null)     { xml.RazonRechazo = "No se pudo leer la moneda.";  return false; }
if (!header.Importe.HasValue)  { xml.RazonRechazo = "No se pudo leer el importe.";  return false; }
...
xml.Moneda = header.Tipo == ConstantesCategoriaMT.Header940 ? string.Empty : header.Moneda;    // never reached for 940
Statements (940/950) and 998 carry no :32A:, so Moneda/Importe stay null and ValidarHeader rejects them with "No se pudo leer la moneda" before MapearAContexto. The 940-specific branch and the downstream INF category logic and MT-950 manual-processing handling are therefore unreachable *if* those types are returned by usp_SWF_IN_ELT_ETL_OPS. The test-local copy LeerHeaderTestHelper (SWT_TEST.cs:345-440 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L345)) shows the same intent (Moneda=string.Empty for 940, :436-437 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L436)) but also requires an amount, so the contract was never consistent. Verification: run an MT940 fixture through LeerHeaderAsync and check which message types the queue procedure returns.
Recommended: validate currency/amount per message type (a table: 103/202/191 require them; 940/950/998 do not) and test every supported type. Benefit / acceptance: informational messages reach the INF/manual path with the right reason, or the dead branches are removed. Suggested JIRA: SIE-FR-27.
SIE-FR-28 - The UETR is registered before the operation is accepted, so a retry is rejected as a duplicate
Category: MustChange/rewrite. Priority: High. Checklist: section 6 (check-then-act races), Blocker. Locations: LeerHeaderRepo.cs:167-197 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/LeerHeaderRepo.cs#L167), SwiftRepo.cs:25-96 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/SwiftRepo.cs#L25).
Current:
Code block (csharp)
var existing = await _swEnd2End.BuscaSwEnd2EndRef(new SwEnd2EndDTO { Uetr = header.Uetr, ... });
if (!string.IsNullOrEmpty(existing)) { xml.RazonRechazo = "UETR duplicado"; return false; }
await _swEnd2End.InsertaSwEnd2EndRef(new SwEnd2EndDTO { Uetr = header.Uetr, ... });      // result (bool) discarded
The UETR is written during header parsing, before folio generation, acceptance or rule application. Any later exception (database, T24, queue) leaves the operation READ; on the next tick the same UETR is found and the same message is rejected as a duplicate. InsertaSwEnd2EndRef catches and returns false (SwiftRepo.cs:86-90 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/SwiftRepo.cs#L86)) and the caller ignores it, so a failed registration leaves duplicate protection silently off. Search-then-insert is also non-atomic (single instance today, but no unique constraint is visible here). The ICN is derived from the first 17 characters of the message rather than the work-table folio.
Recommended: record the UETR in the same transaction as acceptance (or make the insert idempotent: same ICN + UETR = success), rely on a unique index, and check the insert result. Benefit / acceptance: retry after a transient failure succeeds; genuine duplicates are still rejected. Suggested JIRA: SIE-FR-28.
SIE-FR-29 - Credit-restriction check is inverted, unconfigured and fails open
Category: MustChange/rewrite. Priority: High. Checklist: section 3/4, Blocker. Locations: T24CommissionServiceRepo.cs:152-202 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/T24CommissionServiceRepo.cs#L152), OperacionServicio.cs:817-826 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L817), :868-881 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L868), ValidaDatosCoreContext.cs:50 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/DTO/Contexto/ValidaDatosCoreContext.cs#L50), ConstantesParametrosSistema.cs:22 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Constantes/ConstantesParametrosSistema.cs#L22).
Current:
Code block (csharp)
// OperacionServicio
var accountRestriction = new AccountRestrictionDTO { debitRestrict = new List<string>() };   // always empty
dtc.T24AcctRestricted = await _t24CommissionService.ValidateAccountHasCreditRestrictedAsync(dtc.CuentaInterpretada, accountRestriction);
// T24CommissionServiceRepo
return !accountRestrictionDTO?.debitRestrict?.Contains(detail.Bloqueo) ?? false;
SWIFT_RESTRICCION_DEBIT_LIST is loaded from parameters on every tick and exists in the enum, but nothing reads it; the list passed to the check is always empty. The expression !list?.Contains(code) ?? false reports the account as restricted when the block code is not in the list, so with an empty list *any* block code other than "0" (including blocks unrelated to crediting) marks the account restricted (probe P3: empty list, code 5 => restricted True). The default T24AcctRestricted = true also means that when MANDATORY_T24_SERVICE_ACCT_RESTRICT_CHECK is off (or the account was not found) the flag stays true and, with MANDATORY_T24_ACCT_RESTRICT_CHECK on, every mandatory account is rejected with a restriction message. Opposite failure modes: no detail returned and any exception return false (fail-open).
Recommended:
Code block (csharp)
var restrictingCodes = ConfiguracionHelper.GetList(_configuracion.ParametrosSistema, ConfiguracionSistema.SWIFT_RESTRICCION_DEBIT_LIST);
return detail.Bloqueo != SinBloqueo && restrictingCodes.Contains(detail.Bloqueo);     // explicit polarity
Name the method after what it returns, load the configured list, make the default false/unknown with an explicit tri-state (Unknown = alert and retry), and test the two flags x found/not found matrix. Benefit / acceptance: only configured block codes restrict; behaviour is covered by a truth-table test. Suggested JIRA: SIE-FR-29.
SIE-FR-30 - T24 SOAP clients are created per call, never closed, and send credentials over plain HTTP
Category: MustChange/rewrite. Priority: High. Checklist: section 7 (every IDisposable disposed), section 9 (transport security), Blocker. Locations: T24Client.cs:20-24 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/T24Client.cs#L20), T24CommissionServiceRepo.cs:159-174 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/T24CommissionServiceRepo.cs#L159), generated AccountRestrictionValidator/Reference.cs:537 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Connected%20Services/AccountRestrictionValidator/Reference.cs#L537), T24CommissionAcct/Reference.cs:633 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Connected%20Services/T24CommissionAcct/Reference.cs#L633).
Current:
Code block (csharp)
var client = new T24CommissionAcct.T24WebServicesImplClient();           // default endpoint http://10.97.38.163:8085/...
return await client.ConsultaComisionesporClienteAsync(request);          // never Close()/Abort()
A WCF ClientBase must be closed (or aborted on fault); here one channel is created per call and abandoned, which leaks sockets/channels per message that needs T24. The generated defaults hard-code a plain-HTTP internal IP, BasicHttpBinding has no security and no timeouts, and MaxReceivedMessageSize = int.MaxValue; the request body carries company/userName/password (SIE-FR-34). Credentials and customer data therefore cross the network unencrypted unless the endpoint is overridden elsewhere (not visible in this repository).
Recommended: a factory with configured endpoint/binding (HTTPS or message security, timeouts, quotas), using/Close()/Abort() handling or a channel factory, endpoint address from configuration, and cancellation. Benefit / acceptance: stable handle count under a 24-hour soak; endpoint comes from configuration and is HTTPS. Suggested JIRA: SIE-FR-30.
SIE-FR-31 - A bearer token is set on the single shared HttpClient and leaks to every other call
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 6/7 (shared state, HttpClient reuse) and section 9, Blocker. Locations: ApiClient.cs:34-55 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/API/ApiClient.cs#L34), ServiceHost.cs:79 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L79), consumers SamhService.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/SamhService.cs), PacsService.cs (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Servicios/PacsService.cs), ApiCommissionClientRepo.cs:27 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/ApiCommissionClientRepo.cs#L27), OperacionServicio.cs:82 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L82).
Current:
Code block (csharp)
_containerDI.RegisterInstance(new HttpClient());                       // one instance for ApiClient, SAMH, PACS, commission, OperacionServicio
...
client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);   // mutates the shared instance
After the first SPID rejection call, Authorization: Bearer <rejection-API token> is attached to every later request made through the same client - to the SAMH/PACS header/validation services, the commission API and any URL taken from configuration - disclosing the token to unrelated endpoints and breaking calls that expect a different or no authorization header (the token also expires). Default headers are not thread-safe to mutate. HandleResponseAsync throws new Exception("API request failed: ..." + body), putting the remote body in the exception text.
Recommended: per-request HttpRequestMessage with the header set on the message (not the client), or one named client per downstream service (IHttpClientFactory/static clients with their own base address and timeout), and typed errors. Benefit / acceptance: a test captures outgoing headers and asserts that only the rejection API receives the token. Suggested JIRA: SIE-FR-31.
SIE-FR-32 - The rejection API bearer token and responses are written to the log
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 5 "logs never contain passwords, tokens ...", Blocker. Standing finding: GEN-5. Location: RequestApiRejection.cs:43-67 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/RequestApiRejection.cs#L43).
Current:
Code block (csharp)
Config.EscribirLog($"INFO|RequestApiRejection|Token obtenido: {token}");
...
Config.EscribirLog($"INFO|RequestApiRejection|Respuesta exitosa: {responseTask}");
Config.EscribirLog($"INFO|RequestApiRejection|valueDate: {messageType}");                          // labels swapped
Config.EscribirLog($"INFO|RequestApiRejection|messageType: {DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.fffK")}");
The live bearer token is written in clear text; anyone who can read the log can call the rejection API until it expires (and it can send rejection messages: see SIE-FR-1). The full API response, URLs, user name, sender reference, bank, currency and amount are also logged, and two log labels are swapped (valueDate prints the message type, messageType prints the local time).
Recommended: delete the token log line, log only the HTTP status and a correlation id, fix the labels, and rotate the token if the logs were ever shared. Benefit / acceptance: log-scan test finds no Bearer/token values. Suggested JIRA: SIE-FR-32.
SIE-FR-33 - Repository failures are indistinguishable from "no rows", and several fail open
Category: MustChange/rewrite. Priority: High. Checklist: section 4/5, Blocker. Standing finding: GEN-11. Locations: Operaciones.cs:38,66 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/Operaciones.cs#L38) and :134-137 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/Operaciones.cs#L134), CounterParty.cs:52-55 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/CounterParty.cs#L52), OperacionServicio.cs:1521-1529 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1521), LegalEntitiesRepo.cs:28 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/LegalEntitiesRepo.cs#L28), SintaxisRepo.cs:50-57 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/SintaxisRepo.cs#L50), CuentaRepo.cs:261-264 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/CuentaRepo.cs#L261), BancoRepo.cs:59-62 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/BancoRepo.cs#L59), VostroRepo.cs:66-72 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/VostroRepo.cs#L66), SpeiRepo.cs:54 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/SpeiRepo.cs#L54).
Current:
Code block (csharp)
return resultado?.Data ?? Enumerable.Empty<OperacionesDTO>();     // Exito=false and 'no rows' look the same
...
catch (Exception ex) { Config.EscribirLog(...); return -1; }       // CounterParty -> caller: receiverId <= 0 => 'send commission'
catch (Exception ex) { ...; return false; }                        // ExisteComisionEnProceso / ExisteCuentaCerrada
Where GetNostroAsync correctly checks result?.Exito != true, most repositories do not. If the operations query fails the service logs "No hay operaciones a procesar" (an outage looks like an empty queue); if the syntax query fails, every message is rejected as "MT-950 se procesa manualmente" (SIE-FR-13); the legal-entity catalogue returning null disables SPID-like detection; CounterParty returns -1 on error and the caller treats <= 0 as *send the commission*; ExisteComisionEnProceso returns false on error, allowing a duplicate commission; ExisteCuentaCerrada returns false on error. SpeiRepo:54 and VostroRepo:66,89 dereference resultado.Data without the null check their siblings use. The wrong polarity is chosen per method, not by policy.
Recommended: a small QueryResult<T> contract (Exito, Data, Error) returned by the shared base, repositories throw RepositoryException for infrastructure errors, and each call site chooses fail-closed or retry explicitly; add one test per repository for Exito=false, empty and exception. Benefit / acceptance: outages are alerted (not "no work"); documented polarity per decision. Suggested JIRA: SIE-FR-33.
SIE-FR-34 - Service passwords are never loaded, so the T24 and rejection-API calls send empty passwords (verify against the library)
Category: MustChange/rewrite. Priority: High. Checklist: section 3/4, Blocker. Confidence: medium - depends on IParametrizacion in MUFG_T24.dll. Locations: ConstantesParametrosSistema.cs:13-38 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Constantes/ConstantesParametrosSistema.cs#L13), OperacionServicio.cs:153 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L153), ProveedorCredenciales.cs:32-47 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/ProveedorCredenciales.cs#L32), RequestApiRejection.cs:37-41 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/RequestApiRejection.cs#L37).
Current:
Code block (csharp)
public const string ParametrosPasswords = "'WST24_MW_PASSWORD','API_REJECTION_PASSWORD'";    // referenced nowhere
_configuracion.ParametrosSistema = await _parametrizacion.ObtenerParametrosSistemaAsync(ConstantesParametrosSistema.ParametrosSistema, ct);   // list without the passwords
...
WST24Psw = ConfiguracionHelper.GetString(_configuracion.ParametrosSistema, ConfiguracionSistema.WST24_MW_PASSWORD)   // default ""
Only ParametrosSistema is requested; it does not list the two password keys, and ParametrosPasswords is unreferenced. GetString therefore returns its default "" for WST24_MW_PASSWORD and API_REJECTION_PASSWORD, so unless ObtenerParametrosSistemaAsync merges and decrypts them internally (unknown), T24 and the rejection API are called with an empty password and fail authentication (the failure is then reported as a generic Result.Failed). Verification: inspect IParametrizacion in the library or log (without values) whether the keys exist in the loaded dictionary.
Recommended: load the password keys through the intended secure path, fail fast at startup if a required secret is missing, and never default a credential to an empty string. Benefit / acceptance: startup self-check passes only with credentials present. Suggested JIRA: SIE-FR-34.
Rewrite Recommended
SIE-FR-35 - Repository boilerplate, serializer drift and stored-procedure constants
Category: rewrite_recommended. Priority: Medium. Checklist: section 2/8, Major. Locations: IntegracionRepo.cs:68-78 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/IntegracionRepo.cs#L68) and ActualizacionRepo.cs:94-104 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/ActualizacionRepo.cs#L94) (identical 11-parameter blocks, 22 DameValor lines), 13 identical base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) constructors, SwiftRepo.cs:34,71 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/SwiftRepo.cs#L34) (EnumNombreSp from the library, others from ConstantesSql), ApiCommissionClientRepo.cs:66 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/ApiCommissionClientRepo.cs#L66) vs OperacionServicio.cs:1355 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1355).
Extract a WorkItemParameters.From(camposActualizar) mapper used by both repositories, a base TomiRepository with the shared constructor and try/log/wrap, and one place for stored-procedure names (and verify the three ELT typo'd names exist in the database: GEN-13). ActualizarCamposAsync passes a null variablesXml (OperacionServicio.cs:1663 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/OperacionServicio.cs#L1663)); confirm CrearParametro(name, null) supplies DBNull. The commission DTO is serialized as a JSON number in one place and as a "0.00" string in another for the same endpoint: agree one contract (SIE-FR-23). Benefit / acceptance: one mapper and one constructor pattern; adding a field touches one place. Suggested JIRA: SIE-FR-35.
SIE-FR-36 - Account lookup matches every 7-digit window and takes the first row
Category: rewrite_recommended. Priority: High. Checklist: section 3, Major; business rule unverified. Locations: CuentaRepo.cs:37-123 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/CuentaRepo.cs#L37), :199-265 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/CuentaRepo.cs#L199), VostroRepo.cs:36-97 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Repositorios/VostroRepo.cs#L36).
Current:
Code block (csharp)
for (int i = 0; i <= cuentaNumerica.Length - 7; i++) listaCuentas.Add(cuentaNumerica.Substring(i, 7));   // every window
...
var row = resultado.Data.First();                                                                        // first match wins
For non-MXN messages all non-digits are stripped and every 7-digit window of the beneficiary text becomes a candidate account (an 18-character CLABE yields 12 candidates); the first T24 row returned is used. A window that happens to be another customer's account number can credit the wrong customer; the only trace is the flag "Cuenta identificada de forma extraordinaria". ExisteCuentaCerradaAsync repeats the algorithm with an unquoted list. VostroRepo.ObtenerCuentaVostroAsync never assigns numCif (always NULL), and the unused ObtenerCuentaCIFVostroAsync passes the CIF in the @cve_swift parameter.
Recommended: require a unique match (reject or route to manual when more than one candidate/customer matches or the match is not at a known offset), keep the algorithm in one tested class, and ask the owner to approve the heuristic. Benefit / acceptance: ambiguous matches never auto-credit. Suggested JIRA: SIE-FR-36.
4.3 SWIFT_IN_ETL (Tier 2)
Rewrite Recommended
SIE-FR-37 - The build depends on D:\ServiceCMS\ binaries, a developer's desktop path and mixed project styles
Category: rewrite_recommended. Priority: Medium. Checklist: section 2, Major. Standing finding: GEN-1. Locations: SWIFT_IN_ETL.csproj:58-64 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/SWIFT_IN_ETL.csproj#L58), Aplicacion_IN_ETL.csproj:16-26 (../../SWIFT/Incoming/SWF_IN_ETL/Aplicacion_IN_ETL/Aplicacion_IN_ETL.csproj#L16), Infraestructura_IN_ETL.csproj:17-19 (../../SWIFT/Incoming/SWF_IN_ETL/Infraestructura_IN_ETL/Infraestructura_IN_ETL.csproj#L17), PruebasUnitarias.csproj:92 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/PruebasUnitarias.csproj#L92), packages.config (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/packages.config).
Current:
Code block (xml)
<Reference Include="MUFG_T24"><HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath></Reference>              <!-- 5 occurrences in 4 projects -->
<EditorConfigFiles Remove="C:\Users\n371682\Desktop\SWIFT_IN_ETL\SWIFT_IN_ETL\Aplicacion_IN_ETL\.editorconfig" />
<None Include="C:\Users\n371682\Desktop\SWIFT_IN_ETL\SWIFT_IN_ETL\Aplicacion_IN_ETL\.editorconfig" />
The solution cannot compile on a machine without the shared binaries at a fixed drive path (this review machine cannot build it; see section 2). SimpleInjector is referenced by file while packages.config also lists SimpleInjector 5.5.2 that no project consumes; the Aplicacion project hard-codes a developer's desktop path for .editorconfig (leaks a user name and makes the repo's own style file inert on other machines). Two libraries are SDK-style while the host and test projects are classic (packages.config, ProjectTypeGuids), the test project's ProjectReference GUID for Aplicacion_IN_ETL ({dc61319b...}) differs from the one the host uses ({f993e68c...}), and there is no CI file in the repository.
Recommended: publish MUFG_T24 and SimpleInjector as versioned internal packages (one change for the whole portfolio, GEN-1), reference the repo-local .editorconfig by relative path, convert host/test to SDK style or document why not, and add a pipeline that builds and runs tests. Benefit / acceptance: a clean-machine msbuild/dotnet build succeeds in CI. Suggested JIRA: SIE-FR-37.
Nice to Rewrite if Time Permits
SIE-FR-38 - Host and repository polish
Category: NiceToRewrite_ifTimePermits. Priority: Low. Locations: README.md (../../SWIFT/Incoming/SWF_IN_ETL/README.md), ServiceHost.Designer.cs:29-32 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.Designer.cs#L29), ServiceHost.cs:50-125 (../../SWIFT/Incoming/SWF_IN_ETL/SWIFT_IN_ETL/ServiceHost.cs#L50).
The README is the unedited GitLab template (no purpose, deployment, configuration keys, stored-procedure list or runbook). InitializeComponent sets ServiceName = "ServiceHost" while the service is registered with NombreServicio.Swf_IN_ETL (the base class' handling lives in MUFG_T24.dll; see GEN-14 for the shared concern). The composition root registers ~35 types with the default (transient) lifestyle and no documented lifestyle rationale (for example ProveedorCredenciales and RequestApiRejection depend on the scoped Configuracion). ServiceHost.cs repeats Config.EscribirLog with the static helper instead of an injected logger. Suggested JIRA: SIE-FR-38.
4.4 PruebasUnitarias (Tier 3)
MustChange / Rewrite
SIE-FR-39 - No test can run: the shared setup dereferences two null mocks, and 15 of 20 tests exercise test-local copies
Category: MustChange/rewrite (zero effective coverage on business-critical logic). Priority: High. Checklist: testing section, Major/Blocker for critical paths. Locations: SWT_TEST.cs:65-66 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L65), :134-136 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L134), :142-340 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L142), helpers at :345 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L345), :517 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L517), :567 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L567), :750 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L750), :823 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L823).
Current:
Code block (csharp)
private Mock<IContextoXml> _contextoXmlMock = null;
private Mock<ILeerHeader>  _leerHeaderMock  = null;
...
[TestInitialize] public void Setup() { ... _service = new OperacionServicio(..., _procesaCampo = new ProcesaCampo(null),
                                          _contextoXmlMock.Object,      // NullReferenceException
                                          _leerHeaderMock.Object, ...);
_contextoXmlMock and _leerHeaderMock are declared = null and never assigned (grep: only lines 65, 66, 135, 136, 218, 283), so Setup throws before the constructor runs and all five `OperacionServicioTests` fail in initialization (they are also the only tests that reference production behaviour, via 31 constructor arguments, and two use reflection on private methods). The other 15 tests (SintaxisTests, AlertaTests, OperacionT24Tests) run only against copies written inside the test file - LeerHeaderTestHelper, SintaxisHelperTest, SintaxisMtTestHelper, AlertaTestHelper, OperacionTestHelper - so they would pass even if the production code were deleted (for example AlertaTests asserts a List<string> the test itself fills). The copies have already drifted from the production versions (RegexHelper, LeerHeaderRepo). None of the defects in this report is detectable by the current suite, and no CI executes it. This is a source-reading result: tests were not run (no toolchain and no MUFG_T24.dll).
Recommended:
Code block (csharp)
_contextoXmlMock = new Mock<IContextoXml>();
_leerHeaderMock  = new Mock<ILeerHeader>();
then replace helper-based tests with tests of the real types (RegexHelper, LeerHeaderRepo with stub IBanco/ISwift, ProcesaCampo, Conversiones, camt_052/053/106 with fixtures, T24CommissionServiceRepo truth table) and add the first regression tests for SIE-FR-1/2/3/5/6/7/8/26/29. Delete the helpers once real tests exist. Benefit / acceptance: the suite runs green in CI and goes red when SIE-FR-3 or SIE-FR-26 style defects are reintroduced. Suggested JIRA: SIE-FR-39.
Rewrite Recommended
SIE-FR-40 - Test project structure is fragile and coupled to private members
Category: rewrite_recommended. Priority: Medium. Checklist: section 2, Major. Locations: PruebasUnitarias.csproj (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/PruebasUnitarias.csproj), packages.config (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/packages.config), SWT_TEST.cs:262,299 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L262), :699-708 (../../SWIFT/Incoming/SWF_IN_ETL/PruebasUnitarias/SWT_TEST.cs#L699).
MSTest 2.2.10 is combined with Microsoft.NET.Test.Sdk 18.6.0, xunit.runner.visualstudio 3.1.5 (no xUnit tests exist) and Microsoft.TestPlatform.ObjectModel 17.13.0 in a classic packages.config project that imports .props/.targets from ..\packages. All tests live in one 937-line file with nested [TestClass] types, reflect into private methods (InterpretarSegunTipoAsync, ProcesarOperacionMXAsync) and use [ExpectedException(typeof(Exception))] (passes for any exception). The InternalsVisibleTo("PruebasUnitarias") attribute is on the *host* assembly, not on Aplicacion_IN_ETL, so it does not help here.
Recommended: SDK-style test project with a consistent runner, one file per class, InternalsVisibleTo on Aplicacion_IN_ETL (or extract the pipeline interfaces from SIE-FR-15), Assert.ThrowsException<T> with specific types, and coverage collection in CI. Benefit / acceptance: one command runs the suite and reports coverage. Suggested JIRA: SIE-FR-40.
5. Architecture-Fit Note
Applying section 6 of the control document (./_ORCHESTRATION.md#6-standing-architecture-question-is-the-current-hostingarchitecture-model-right) to this workload: Swf_IN_ETL is a timer-driven DB-polling worker (10 s default) that runs a *multi-step, non-atomic acceptance sequence* (accept -> LAIKA queue -> commission request -> rules) and calls SOAP and HTTP services; it is not a continuous listener and not a nightly batch. The Windows Service host is not the problem; the lack of an explicit state machine, idempotency and fail-closed policy is.
Option	Fit for this solution	Decision
Modernize in place (keep the service; add stage results, idempotent steps, transaction/outbox, DI/logging/test seams)	Matches the real defects (SIE-FR-1/2/3/26/28/33) and needs no new platform	Recommended near term
Message-driven (queue per stage, dead-letter)	Natural for accept -> LAIKA -> commission, but needs contract versioning, retry/poison handling and new infrastructure	Evaluate after the state machine exists; requires SLA and volume data
Workflow/saga engine	Only if compensation across T24/LAIKA/commission is a requirement	Not demonstrated by this code; do not adopt speculatively
Serverless/scheduled function	Poor fit: needs T24 SOAP, internal HTTP, SQL, always-on polling and HTTP to private IPs	Not recommended without hosting evidence
Microservices rewrite	No scaling or ownership evidence in the code	Do not do as a first move

Unknowns that must be answered before any re-platform decision: message volume and latency SLA, whether several instances may run (UETR check-then-insert, SIE-FR-28), SQL stored-procedure transaction semantics, hosting/network constraints for the T24 endpoints, and budget. Sequence: fix definite defects (SIE-FR-1..14, 26..34, 39) -> remove duplication and add tests (SIE-FR-15..24, 35, 36, 40) -> evaluate .NET 8 Worker Service / hosting modernisation as a separate, separately-scoped decision.
6. JIRA-Ready Backlog
ID	Title	Category	Project	File(s)	Priority
SIE-FR-1	Stop SPID rejection + acceptance of the same payment; remove static/const flag	MustChange/rewrite	Aplicacion_IN_ETL	OperacionServicio.cs, ServicioSpid.cs, ConstantesAux.cs	Blocker
SIE-FR-2	Make accept/queue/rules one visible unit; stop discarding stage results	MustChange/rewrite	Aplicacion_IN_ETL	OperacionServicio.cs	Blocker
SIE-FR-3	Fail closed when field mapping is empty	MustChange/rewrite	Aplicacion_IN_ETL	OperacionServicio.cs, SwiftRepo.cs	Blocker
SIE-FR-4	Separate retryable infrastructure faults from business rejections	MustChange/rewrite	Aplicacion_IN_ETL	OperacionServicio.cs	High
SIE-FR-5	Read commission rates from parameters; check HTTP status	MustChange/rewrite	Aplicacion_IN_ETL	OperacionServicio.cs, ConstantesParametrosSistema.cs	High
SIE-FR-6	Fix camt.052 model input node	MustChange/rewrite	Aplicacion_IN_ETL	camt_052.cs, AccountReport25.cs	High
SIE-FR-7	Read camt.106 charge fields from Document	MustChange/rewrite	Aplicacion_IN_ETL	camt_106.cs	High
SIE-FR-8	Fix inverted Sts guard and silent entry loss	MustChange/rewrite	Aplicacion_IN_ETL	ReportEntry10.cs, AccountStatement9.cs	High
SIE-FR-9	Fix CS0136 duplicate local in BalanceType13 and gate CI build	MustChange/rewrite	Aplicacion_IN_ETL	BalanceType13.cs	Blocker
SIE-FR-10	Populate Importe/Moneda for MX messages	MustChange/rewrite	Aplicacion_IN_ETL	PACS008MX.cs, PACS009MX.cs	High
SIE-FR-11	Define handling of tag syntax failures	MustChange/rewrite	Aplicacion_IN_ETL	OperacionServicio.cs	High
SIE-FR-12	Correct vostro lookup key, SPEI2 mandatory flag and messages	MustChange/rewrite	Aplicacion_IN_ETL	OperacionServicio.cs, ProcesaCampo.cs	High
SIE-FR-13	Null/blank handling in conversions, dates and reasons	MustChange/rewrite	Aplicacion_IN_ETL	Conversiones.cs, OperacionServicio.cs	Medium
SIE-FR-14	Stop out-of-window alert flood; guard Pid	MustChange/rewrite	Aplicacion_IN_ETL / SWIFT_IN_ETL	OperacionServicio.cs, ServiceHost.cs	Medium
SIE-FR-15	Split god class; unify duplicated chains	rewrite_recommended	Aplicacion_IN_ETL	OperacionServicio.cs, PACS008MX.cs	Medium
SIE-FR-16	Stop logging full payloads	MustChange/rewrite	Aplicacion_IN_ETL / Infraestructura_IN_ETL	OperacionServicio.cs, ApiCommissionClientRepo.cs	High
SIE-FR-17	Isolate per-message failures; thread CancellationToken	rewrite_recommended	Aplicacion_IN_ETL	OperacionServicio.cs, Interfaces	Medium
SIE-FR-18	Regex timeouts, one tag tokenizer	rewrite_recommended	Aplicacion_IN_ETL	RegexHelper.cs, Tags.cs, OperacionServicio.cs	Medium
SIE-FR-19	Lossless FIN name/address formatter	rewrite_recommended	Aplicacion_IN_ETL	PACS008MX.cs, TextoMultilinea.cs	Medium
SIE-FR-20	camt model polish	NiceToRewrite_ifTimePermits	Aplicacion_IN_ETL	Modelos/camt, TipoOperacion	Low
SIE-FR-21	Refresh nostro cache; make default debit account explicit	rewrite_recommended	Aplicacion_IN_ETL	CuentasNostro.cs, OperacionServicio.cs	Medium
SIE-FR-22	Harden HTTP service wrappers	rewrite_recommended	Aplicacion_IN_ETL	SamhService.cs, PacsService.cs	Medium
SIE-FR-23	Invariant formatting for money/dates	rewrite_recommended	Aplicacion_IN_ETL / Infraestructura_IN_ETL	DecimalFormatConverter.cs, LeerHeaderRepo.cs, ConvertirFecha.cs	Medium
SIE-FR-24	Explicit 57A/57D parsing	rewrite_recommended	Aplicacion_IN_ETL	ProcesaTag57D.cs	Medium
SIE-FR-25	Remove dead code; fix names/constants	NiceToRewrite_ifTimePermits	Aplicacion_IN_ETL	CortarTexto.cs, Moneda.cs, ConstantesSql.cs	Low
SIE-FR-26	Do not swallow LAIKA enqueue failures	MustChange/rewrite	Infraestructura_IN_ETL	IntegracionRepo.cs	Blocker
SIE-FR-27	Per-type header validation (statements)	MustChange/rewrite	Infraestructura_IN_ETL	LeerHeaderRepo.cs	Medium
SIE-FR-28	Register UETR atomically with acceptance	MustChange/rewrite	Infraestructura_IN_ETL	LeerHeaderRepo.cs, SwiftRepo.cs	High
SIE-FR-29	Correct credit-restriction logic and configuration	MustChange/rewrite	Infraestructura_IN_ETL / Aplicacion_IN_ETL	T24CommissionServiceRepo.cs, OperacionServicio.cs	High
SIE-FR-30	Close/configure T24 SOAP clients; use HTTPS	MustChange/rewrite	Infraestructura_IN_ETL	T24Client.cs, T24CommissionServiceRepo.cs	High
SIE-FR-31	Stop mutating shared HttpClient headers	MustChange/rewrite	Infraestructura_IN_ETL / SWIFT_IN_ETL	ApiClient.cs, ServiceHost.cs	Blocker
SIE-FR-32	Remove bearer token from logs	MustChange/rewrite	Infraestructura_IN_ETL	RequestApiRejection.cs	Blocker
SIE-FR-33	Distinguish repository failure from empty result	MustChange/rewrite	Infraestructura_IN_ETL	Operaciones.cs, CounterParty.cs, SintaxisRepo.cs, others	High
SIE-FR-34	Load service passwords; fail fast when missing	MustChange/rewrite	Infraestructura_IN_ETL / Aplicacion_IN_ETL	ProveedorCredenciales.cs, ConstantesParametrosSistema.cs	High
SIE-FR-35	Repository base/mapper; constants; serializer drift	rewrite_recommended	Infraestructura_IN_ETL	IntegracionRepo.cs, ActualizacionRepo.cs, SwiftRepo.cs	Medium
SIE-FR-36	Require unique account match	rewrite_recommended	Infraestructura_IN_ETL	CuentaRepo.cs, VostroRepo.cs	High
SIE-FR-37	Package shared binaries; remove desktop paths; CI build	rewrite_recommended	All projects	*.csproj, packages.config	Medium
SIE-FR-38	README, ServiceName and DI polish	NiceToRewrite_ifTimePermits	SWIFT_IN_ETL	README.md, ServiceHost.Designer.cs, ServiceHost.cs	Low
SIE-FR-39	Make tests executable and test production code	MustChange/rewrite	PruebasUnitarias	SWT_TEST.cs	High
SIE-FR-40	SDK-style test project; remove reflection	rewrite_recommended	PruebasUnitarias	PruebasUnitarias.csproj, SWT_TEST.cs	Medium

7. Word Companion
Per the control document (section 7) the dated Word copy SWF_IN_ETL_Recommendations_<MMddyyyy>.docx is generated from this Markdown with docs\_convert_markdown_to_docx.ps1; the Markdown remains the source of truth. See validation record (./_SWF_IN_ETL_Validation.md).
