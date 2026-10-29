Swift.Api.Rejection Recommendations
Swift.Api.Rejection - Architecture & Code Quality Recommendations
Solution: Swift.Api.Rejection.sln (../../SWIFT/Swift.Api.Rejection/Swift.Api.Rejection.sln). Reviewed: 2026-10-15, branch CodeReview, commit 4f334a56aa5b50c0b992673d4237d078e0d8c1c1. Compared against: LoansApp coding-practices baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR review/severity checklist (../BaseLine_Docs/code_review.md). See also: orchestration/control (./_ORCHESTRATION.md#swiftapirejection-scoped-audit-checkpoint---2026-10-15). Target: .NET 8; focus: all categories; severity threshold: all. Audit only: no application fixes applied.
1. Solution Overview (low/no-dependency -> high dependency)
Tier	Project	Kind	Path
0	Swift.Api.Rejection	Executable ASP.NET Core API	project (../../SWIFT/Swift.Api.Rejection/Swift.Api.Rejection.csproj)

What this solution actually does: an operator authenticates using a database-backed username/bcrypt password. The API issues an HMAC-SHA256 JWT. An authorized POST with an enabled WRITE permission generates an MT-199 rejection/return-related financial message, then calls a stored procedure to place it into outbound database processing. It does not itself transmit over SWIFT, confirm delivery, settle funds or run a background queue consumer. The FIN text and authorization boundary nevertheless make it business-critical.
1.1 Issue Summary
Recommended priority	Count	Simple meaning
Blocker	9	Close before approving a release carrying these paths
High	5	Immediate correction/verification or an explicit accountable release decision
Medium	7	Planned reliability, security or maintainability work
Low	1	Optional naming/cleanup after correctness work
Total distinct findings	22	One category per finding; no duplicate security/test tickets

Skill category	Count
MustChange/rewrite	14
rewrite_recommended	7
NiceToRewrite_ifTimePermits	1

Priorities are source-review release recommendations, not CVSS or proof of production exploitation. Checklist Blocker/Major rules are cited per finding; the skill's critical-logic test gap is must-change even without measured coverage. The highest-value fixes are lossless message generation, validated fields, unique references, exact financial amounts and committed permission reads. The executive plan (../excecutive_docs/Swift.Api.Rejection/Executive_Portfolio_Summary.md) also calls out the 2026-11-10 .NET 8 support deadline.
2. Scope & Methodology Note
Area	Evidence / limitation
Source scope	36/36 authored text files, 1,768/1,768 physical lines fully read, not sampled; 27 C# / 1,180 lines, one SQL / 18, four JSON / 105, project/solution / 39, Git support / 426
Coverage record	SHA-256/physical-line manifest (./_Swift.Api.Rejection_FileManifest.csv), complete ranges for all files
Exclusions	Git internals, generated bin/obj/IDE output and restored packages; not a claim to have reviewed vendor binaries or Git history
Project order	One tier-0 project, no internal project edges, no HintPaths/MUFG_T24 dependency
Review basis	Full current-source architecture/logic audit, not a changed-lines PR verdict or acceptance review for an unspecified work item
Standards limitation	Baseline contains blank inventory/evidence fields; its principles are criteria, not verified empirical claims
Build	Explicit x86 dotnet build --no-restore failed before compilation: no SDK installed; Core/Desktop 8.0.31 runtimes exist, ASP.NET shared runtime not listed
Tests	No repository test project/framework/test attributes; external CI tests and measured coverage unavailable
Safe execution	14 isolated algorithm assertions executed successfully on installed .NET 8.0.31; several intentionally confirm existing defects. Not a solution build, production-source unit suite, Dapper integration or HTTP test
External evidence not used	No secret/configuration file, live database, real financial data or outbound endpoint accessed; no package restore/install
Fix examples	Illustrative contracts/code directions, not compiled patches. New helper/type names below require implementation/registration and regression tests
Missing contracts	Login/insertion procedure bodies, DB constraints, release manifests, TLS topology, authorization business rules, FIN specification and SLAs

Full-read batches in this audit:
Batch	Contents	Files	Physical lines	Status
A	Program, solution, project, SDK JSON	4	153	Full read complete
B	Two controllers, three concrete domain/data services, two settings files	7	485	Full read complete
C	Security, all models/results/constants, permission SQL	14	365	Full read complete
D	Configuration helper/provider, interfaces, validator/extension, launch settings, Git support	11	765	Full read complete
Total	Entire authored scope	36	1,768	No pending range

2.1 Quantified Cross-Checks and Duplication
Across all authored C# files: three GetDecryptedConnectionString(ServiceConstants...) call sites, three new SqlConnection sites, three lines using DateTime.Now/Today, one wrapping regex, zero test attributes, zero .Result/.Wait()/GetAwaiter().GetResult(), zero stack-reset rethrows and zero DES/3DES/ECB/MD5/SHA1 usage. These scans supplement, not replace, the full read.
The repeated connection/decryption setup occurs in AuthService.Login, AuthService.GetRoles and InsertionService.InsertMessage. Extract/cache at the existing scoped ConnectionStringProvider seam, not a new generic repository architecture. This addresses SRA-FR-17's repeated I/O while preserving the three operations' separate SQL responsibilities. Repeated DTO property reads also repeatedly chunk the reason; SRA-FR-2/3 move formatting out of the DTO.
2.2 Safe Probe Results
Probe family	Observed behavior on .NET 8.0.31
Wrapping, four assertions	44-character token retained; 45/50-character tokens retain only 44; DTO chunking plus wrapping retains 88 of 100 synthetic marker characters
Null reason	Getter-equivalent LINQ Chunk expression throws ArgumentNullException
Culture, two assertions	Same numeric value renders 1234.50 under en-US and 1234,50 under es-ES
Binary floating point	0.1 + 0.2 yields 0.30000000000000004, not exact 0.3
Reference, two assertions	Two instants 800 ms apart in the same second both produce 20261015-45296; the next second differs
Row-count predicate, two assertions	-1 > 0 is false; positive count is true. No insertion procedure was executed
Mapping shape	Selecting the first of two row-local user/role aggregates retains only one role; not an executed Dapper/database test
Field boundary	An unvalidated synthetic CRLF in a reference creates an additional text line

2.3 False Positives Avoided / Open Requirements
• AuthService checks the local FirstOrDefault() user before dereference; the authorization handler checks the role claim.
XML lookups are guarded. No unguarded nullable .Value defect was found.
• SQL input is parameterized; role IDs are parsed as integers and the CSV is a stored-procedure parameter. Do not label this SQL injection.
• Crypto uses AES-CBC with an embedded IV, not DES/ECB/MD5/SHA1. No hardcoded key/password/token was found.
The nested decryptor wraps crypto errors in InvalidOperationException; the outer catch (CryptographicException) does not actually provide a corrupt-ciphertext fallback. Preserve fail-closed behavior when clarifying this dead catch; do not "fix" it by silently accepting failed decryption.
• The controller's Result.Data checks already protect successful-but-null service results.
• JWT expiry uses a DateTime that the token library converts; local Now alone is not proof of a wrong wire expiry.
• The deterministic folio and duplicate comment suggest an existing duplicate safeguard. Missing SQL means neither idempotency nor its absence is established.
• RoleEnabled is never read locally, and token roles are not rejoined to current user membership.
The login/permission DB contract may filter eligibility. Confirm disabled/revoked-role semantics before asserting an effective bypass.
• ValueDate is unused, fixed bank/reason text exists, and sender-reference ownership is not verified locally.
Confirm intended business rules; do not invent acceptance criteria or an IDOR finding from these observations alone.
• HTTP development listeners and absent in-process TLS redirection do not prove production is plaintext; verify the actual reverse-proxy/TLS boundary.
• No load profile, execution plan or SCA output exists. Do not invent a throughput bottleneck, missing index, CVE or approved-feed violation.
3. What's Already Good
Good area	Evidence	Why retain it
Constructor injection and correct scoped services	Program.cs:31 (../../SWIFT/Swift.Api.Rejection/Program.cs#L31); service interfaces and constructors	Existing test seams; no forced new layering or container
JWT validation enabled	Program.cs:44 (../../SWIFT/Swift.Api.Rejection/Program.cs#L44)	Issuer, audience, lifetime and signature checks already present
Protected message-write endpoint	RejectionController.cs:27 (../../SWIFT/Swift.Api.Rejection/Controllers/RejectionController.cs#L27)	Authorization is enforced before message creation
bcrypt password verification	User.cs:19 (../../SWIFT/Swift.Api.Rejection/Model/Security/User.cs#L19)	No plaintext password comparison
Parameterized stored procedures	AuthService.cs:40 (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs#L40); InsertionService.cs:36 (../../SWIFT/Swift.Api.Rejection/Services/InsertionService.cs#L36)	No concatenated SQL-input execution
Deterministic SQL resource lifetime	InsertionService.cs:28 (../../SWIFT/Swift.Api.Rejection/Services/InsertionService.cs#L28); AuthService.cs:31 (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs#L31)	Connections and insertion command already use await using
Generic outward controller errors and null-result checks	RejectionController.cs:50 (../../SWIFT/Swift.Api.Rejection/Controllers/RejectionController.cs#L50); AuthController.cs:69 (../../SWIFT/Swift.Api.Rejection/Controllers/AuthController.cs#L69)	Do not regress into returning stack traces/SQL exceptions
Safe XML reader configuration	ConfiguracionEncriptadaHelper.cs:88 (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConfiguracionEncriptadaHelper.cs#L88)	DTD prohibited and resolver disabled
Stack/inner-exception preservation	Program.cs:101 (../../SWIFT/Swift.Api.Rejection/Program.cs#L101); configuration helper:68 (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConfiguracionEncriptadaHelper.cs#L68)	Rethrow uses throw;; wrappers retain inner exceptions
Development-only Swagger and no committed JWT secret	Program.cs:84 (../../SWIFT/Swift.Api.Rejection/Program.cs#L84); appsettings.json:27 (../../SWIFT/Swift.Api.Rejection/appsettings.json#L27)	Keep secrets external and do not expose development tooling by default

4. Findings by Project (low/no-dependency -> high dependency)
4.1 Swift.Api.Rejection (Tier 0)
MustChange / Rewrite
SRA-FR-1 - Enforce a FIN-safe request contract before interpolation
Category: MustChange/rewrite. Priority: Blocker. Checklist: sections 3/9/10, external validation and API boundary (Blocker). Locations: RejectionDto.cs:9 (../../SWIFT/Swift.Api.Rejection/Model/RejectionDto.cs#L9), MtCreationService.cs:23 (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs#L23) and MtCreationService.cs:45 (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs#L45).
Current:
Code block (csharp)
if (string.IsNullOrWhiteSpace(rejectionDto.MessageType) ||
    string.IsNullOrWhiteSpace(rejectionDto.Currency) ||
    rejectionDto.Amount <= 0)
{
    _logger.LogWarning(
The branch only warns; generation still returns success. Required attributes cover presence, not FIN character sets, field widths, BIC normalization, currency/type allow-lists, positive finite amount or total message limits. Destination/reference/type/currency are interpolated directly. The synthetic CRLF probe confirms a reference can alter line structure. Invalid financial messages can therefore be accepted and queued by an otherwise authorized caller.
Recommended direction:
Code block (csharp)
var validation = await _rejectionValidator.ValidateAsync(rejection, cancellationToken);
if (!validation.IsValid)
{
    return ValidationProblem(ToModelState(validation));
}
Implement/register the validator and typed service failure contract; validate/normalize against the approved FIN specification, including every dynamic field, bounds and allowed controls. Do not silently sanitize away meaning or assume every BIC is eight characters. Benefit / acceptance: invalid requests return a consistent 400 without calling insertion; exact accepted/rejected character, length, currency/type and numeric boundary fixtures are owner-approved. Suggested JIRA: SRA-FR-1.
SRA-FR-2 - A null reason throws inside the DTO getter
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 4, external/null safety (Blocker). Location: RejectionDto.cs:24 (../../SWIFT/Swift.Api.Rejection/Model/RejectionDto.cs#L24).
Current:
Code block (csharp)
var list = _reason
    .Chunk(50)
    .Select(x => new string(x).TrimStart())
    .ToList();
The setter accepts runtime null despite its nullable annotation. Reading the property then throws before [Required] can evaluate its value; model validation/logging can read it outside the controller's try/catch. The isolated getter expression reproduced ArgumentNullException. Explicit JSON null can become a server error instead of a useful validation response.
Recommended:
Code block (csharp)
[Required]
public string? Reason { get; set; }
Use a passive DTO, validate once at the boundary, then pass a validated non-null value to formatting. Benefit / acceptance: omitted/null/empty/whitespace reasons produce 400, not an unhandled getter error; normal logging/serialization never mutates the request. Suggested JIRA: SRA-FR-2. Pair regression coverage with SRA-FR-1/3 without merging their distinct defects.
SRA-FR-3 - Wrapping silently deletes long-token characters
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 3, correctness/boundaries (Blocker). Locations: MtCreationService.cs:71 (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs#L71), RejectionDto.cs:29 (../../SWIFT/Swift.Api.Rejection/Model/RejectionDto.cs#L29).
Current:
Code block (csharp)
return Regex.Matches(stringToSplit, "(.{1," + maximumLineLength + @"})(?:\s|$)");
The unanchored regex can skip a long word's prefix until a <=44-character suffix reaches whitespace/end. The DTO first chunks at 50, worsening the mismatch. Probes retained 44/50 and 88/100 synthetic characters. This is silent corruption of an operator's financial explanation, not merely cosmetic wrapping.
Recommended lossless character-chunking core, after permitted line endings/characters are validated:
Code block (csharp)
var lines = normalizedReason.Chunk(44)
    .Select(chars => "/TEXT/" + new string(chars));
Move all wrapping into the formatter; preserve or deliberately account for every character, and enforce the approved line-count limit. If word-preserving wrapping is required, hard-split oversized tokens rather than skip them. Benefit / acceptance: reconstructing the content retains all approved input characters for lengths 0/1/44/45/50/100 and long/no-space/Unicode-rejection cases. Suggested JIRA: SRA-FR-3.
SRA-FR-4 - Whole-second transaction references collide
Category: MustChange/rewrite. Priority: Blocker. Checklist: sections 6/10, concurrency and non-idempotent processing (Blocker). Location: MtCreationService.cs:39 (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs#L39).
Current:
Code block (csharp)
var ts = DateTime.Now - DateTime.Today;
var secondsSinceMidnight = (int) ts.TotalSeconds;
var ourReference = $"{DateTime.Today:yyyyMMdd}-{secondsSinceMidnight}";
All requests in the same second share :20: and transaction_ref, even for different folios. The deterministic probe produced the same reference for instants 800 ms apart. Multiple clock reads also make midnight boundaries inconsistent. This risks rejection or ambiguous downstream tracing; actual database constraints are not supplied.
Recommended contract:
Code block (csharp)
var ourReference = await _referenceAllocator.AllocateAsync(folio, cancellationToken);
Implement an approved persistent/atomic allocator (for example, a database sequence with a compliant encoding), define replay behavior, and verify uniqueness across nodes/restarts plus the FIN reference length/character rules. Do not truncate a GUID and call it guaranteed unique. Benefit / acceptance: parallel different-folio requests never share references, retries follow the approved stable-reference contract, and all generated values satisfy the approved :20: format. Suggested JIRA: SRA-FR-4.
SRA-FR-5 - Money is represented as binary floating point
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 3, decimal money and explicit rounding (Blocker). Location: RejectionDto.cs:21 (../../SWIFT/Swift.Api.Rejection/Model/RejectionDto.cs#L21).
Current:
Code block (csharp)
public double Amount { get; set; }
Binary double cannot exactly represent decimal money; the probe reproduced 0.30000000000000004. The formatter implicitly rounds to two digits without an agreed currency/rounding policy. Do not infer that this example proves a specific already-posted payment was wrong; it proves the representation/policy gap.
Recommended:
Code block (csharp)
public decimal Amount { get; set; }
var rounded = decimal.Round(amount, approvedScale, approvedMidpointRounding);
Specify permitted scale/range per supported currency and reject or explicitly round as agreed; preserve JSON contract compatibility. Benefit / acceptance: exact decimal, midpoint, maximum, negative/zero and currency-scale tests pin the approved output. Related portfolio family: GEN-12. Suggested JIRA: SRA-FR-5.
SRA-FR-6 - Server culture changes the financial wire amount
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 14, explicit exchange culture (Blocker). Location: MtCreationService.cs:51 (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs#L51).
Current:
Code block (csharp)
sb.Append($"/TEXT/WITH AN AMOUNT {rejectionDto.Currency} {rejectionDto.Amount:0.00}\r\n");
Interpolation uses the server's current culture. The same value rendered 1234.50 and 1234,50 in the two culture probes. Deploying to another regional setting therefore changes financial-message bytes without a code/input change.
Recommended, after the business format and SRA-FR-5 rounding policy are approved:
Code block (csharp)
var amountText = roundedAmount.ToString(approvedAmountFormat, CultureInfo.InvariantCulture);
sb.Append($"/TEXT/WITH AN AMOUNT {currency} {amountText}\r\n");
If the required FIN field convention mandates another explicit separator/shape, encode that convention directly instead of using host culture. Benefit / acceptance: identical approved output under en-US, es-ES and the production culture, with no grouping. Related portfolio family: GEN-12. Suggested JIRA: SRA-FR-6.
SRA-FR-7 - Authorization reads uncommitted role-permission mappings
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 9 server-side authorization and SQL isolation rules (Blocker). Location: usp_Rejection_GetPermissionsByRole.sql:14 (../../SWIFT/Swift.Api.Rejection/Scripts/usp_Rejection_GetPermissionsByRole.sql#L14).
Current:
Code block (sql)
FROM dbo.TB_SRV_API_MASTER M WITH (NOLOCK)
NOLOCK permits dirty/missing/duplicated mapping reads. An uncommitted permission grant may be observed and later rolled back, yet the API can already have authorized the outbound operation. The SQL text establishes the isolation hazard; the database's actual concurrent maintenance workload was not exercised.
Recommended:
Code block (sql)
FROM dbo.TB_SRV_API_MASTER M
Use committed reads; if blocking is material, ask the DBA to evaluate read-committed snapshot under a reviewed database change. Also confirm the authoritative enabled-role/current-membership contract noted in section 2.3; its deployed behavior is not established by this file alone. Benefit / acceptance: an uncommitted grant cannot authorize an API write; test commit/rollback cases against a disposable database. Suggested JIRA: SRA-FR-7.
SRA-FR-8 - Diagnostic logging exposes complete financial payloads
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 5 sensitive-data logging (Blocker). Locations: RejectionController.cs:41 (../../SWIFT/Swift.Api.Rejection/Controllers/RejectionController.cs#L41), RejectionController.cs:57 (../../SWIFT/Swift.Api.Rejection/Controllers/RejectionController.cs#L57), appsettings.Development.json:8 (../../SWIFT/Swift.Api.Rejection/appsettings.Development.json#L8).
Current:
Code block (csharp)
_logger.LogDebug("Received rejection request {@Rejection}", rejection);
_logger.LogDebug("Created MT-199 with contents:\n{@Message}", message.Message);
Debug logging serializes the full DTO and generated FIN message, including operator free text and financial details. It is enabled by the checked-in Development configuration; production Information suppresses these calls unless raised. No plaintext password/key/token log was found, and the Trace call passing a MessageDto is not by itself proof its contents are rendered. The two explicit Debug paths are sufficient evidence of the exposure.
Recommended:
Code block (csharp)
_logger.LogDebug("MT-199 created: Type={MessageType}, Characters={Characters}",
    message.MessageType, message.Message.Length);
Use request/trace identifiers and approved masked business identifiers; remove raw DTO/body logging at every level. Apply access, retention and non-production data controls to file/console sinks. Benefit / acceptance: capture Debug/Trace logs for a synthetic sensitive marker and assert it never appears. Related family: GEN-5, Debug-level financial-payload variant, not a claim of always-on credential logging. Suggested JIRA: SRA-FR-8.
SRA-FR-9 - Affected-row counts are not an insertion outcome contract
Category: MustChange/rewrite. Priority: High. Checklist: sections 3/8, truthful outcomes and SQL result handling (Major). Location: InsertionService.cs:52 (../../SWIFT/Swift.Api.Rejection/Services/InsertionService.cs#L52).
Current:
Code block (csharp)
var inserted = await command.ExecuteNonQueryAsync() > 0;
Every nonpositive count is then described as an existing ICN. A successful procedure using SET NOCOUNT ON can return -1; trigger counts and rollback/error paths further separate affected rows from business outcome. The predicate probe confirms -1 is classified as failure; the insertion procedure is missing, so its actual NOCOUNT/return contract and production incidence are unknown. This is a conditional integration defect for a valid ADO.NET result, not evidence that the included permission procedure performs insertion.
Recommended explicit output contract:
Code block (csharp)
var outcome = command.Parameters.Add("@insert_status", SqlDbType.Int);
outcome.Direction = ParameterDirection.Output;
await command.ExecuteNonQueryAsync(cancellationToken);
if (outcome.Value is not int code)
    throw new InvalidOperationException("Insertion status was not returned.");
return MapInsertStatus(code);
Define/version the SQL status contract and distinguish inserted, already-present, rejected and indeterminate outcomes. Validate parameter types/widths against actual DDL; make retries/reconciliation safe after a lost response. Benefit / acceptance: both NOCOUNT settings and triggers cannot turn a successful insert into a false duplicate/500. If the current procedure deliberately guarantees row counts, obtain that versioned evidence before choosing the migration. Suggested JIRA: SRA-FR-9.
SRA-FR-10 - Anonymous login has no local throttling or failed-attempt control
Category: MustChange/rewrite. Priority: High. Checklist: section 9 auth rate limiting/lockout (Major). Locations: AuthController.cs:31 (../../SWIFT/Swift.Api.Rejection/Controllers/AuthController.cs#L31), CredentialsValidator.cs:9 (../../SWIFT/Swift.Api.Rejection/Validators/CredentialsValidator.cs#L9), AuthService.cs:57 (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs#L57).
Current:
Code block (csharp)
[AllowAnonymous]
public async Task<IActionResult> Login([FromBody] Credentials? credentials)
The path performs a DB lookup and bcrypt verification for repeated valid-shaped attempts. No in-process rate policy, bounded credential length or failed-password/backoff update is wired. An existing IsLocked flag is checked, but this API does not update it after failed verification. External WAF/rate controls may exist; none were supplied, so production exposure is not assumed.
Recommended endpoint policy:
Code block (csharp)
[EnableRateLimiting("login")]
[HttpPost]
[AllowAnonymous]
Register a bounded, monitored .NET rate policy and middleware, with a trusted-proxy-aware caller key; define safe per-account backoff/lockout behavior and username/password byte/length limits consistent with bcrypt and existing accounts. If a platform control is authoritative, document and test its coverage rather than claiming the gap is closed by convention. Benefit / acceptance: bursts receive 429 under the approved policy; ordinary/disabled-account paths remain correct without logging passwords. Suggested JIRA: SRA-FR-10.
SRA-FR-11 - Login responses disclose account existence/state
Category: MustChange/rewrite. Priority: Medium. Checklist: sections 5/9, generic outward errors and auth information exposure (Major). Locations: AuthController.cs:55 (../../SWIFT/Swift.Api.Rejection/Controllers/AuthController.cs#L55), AuthService.cs:45 (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs#L45).
Current:
Code block (csharp)
var response = Unauthorized("Usuario deshabilitado.");
Unknown users return a failure with an empty message, wrong passwords return a different text, and disabled users are identified before password verification. All may be 401, but their bodies and work performed differ. An anonymous caller can learn user existence/state; the deployed exploitability/rate boundary is unknown.
Recommended:
Code block (csharp)
if (!loginResult.Succeeded)
    return Unauthorized(GenericInvalidCredentialsMessage);
Keep the reason in restricted, sanitized audit events; use the same public response for unknown/disabled/wrong-password cases. Evaluate a precomputed same-cost dummy bcrypt verification for unknown users to reduce timing differences, with SRA-FR-10's budget controls. Benefit / acceptance: status/body contract is identical across failure states; timing behavior is measured rather than promised constant-time. Suggested JIRA: SRA-FR-11.
SRA-FR-12 - Financial formatting and authorization have no repository regression suite
Category: MustChange/rewrite. Priority: High. Checklist: section 13 (Major); skill rule for untested business-critical logic. Location: Swift.Api.Rejection.csproj:1 (../../SWIFT/Swift.Api.Rejection/Swift.Api.Rejection.csproj#L1), entire solution inventory.
Current evidence:
Code block (xml)
<Project Sdk="Microsoft.NET.Sdk.Web">
The sole project is production Web/API code; no test project/framework/attribute exists among the 36 authored files. There is no local automated regression evidence, not a measured claim that all external coverage is 0%. The 14 isolated audit probes characterize algorithms and do not substitute for application tests.
Recommended regression shape:
Code block (text)
Exact complete MT-199 output; long-token preservation; invalid/null request -> 400/no insertion
Deterministic money/culture/rounding; parallel unique references and approved retry behavior
Unknown/disabled/wrong-password login; zero/one/multiple roles; denied/disabled permissions
Explicit insert outcomes/NOCOUNT; cancellation; corrupt/missing configuration; redacted logs
Add an organization-standard test project using the existing service interfaces, an injectable reference/clock seam, and fakes for SQL/configuration. Reserve transaction/isolation/procedure assertions for a disposable database. Benefit / acceptance: regressions fail against the corresponding old defects, run deterministically in CI, and never contact production. Standing family: GEN-2. Suggested JIRA: SRA-FR-12.
SRA-FR-13 - Owned crypto transform and startup logger are not disposed
Category: MustChange/rewrite. Priority: Blocker. Checklist: section 7 deterministic disposal (Blocker). Locations: ConfiguracionEncriptadaHelper.cs:200 (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConfiguracionEncriptadaHelper.cs#L200), Program.cs:15 (../../SWIFT/Swift.Api.Rejection/Program.cs#L15) and Program.cs:103 (../../SWIFT/Swift.Api.Rejection/Program.cs#L103).
Current:
Code block (csharp)
byte[] decrypted = aesAlg.CreateDecryptor().TransformFinalBlock(
    encrypted,
    0,
    encrypted.Length);
The AES object is disposed, but the separately owned ICryptoTransform is not. This repeats on encrypted configuration reads. Separately, startupLogger is its own CreateLogger() instance; Log.CloseAndFlush() closes the static logger, not an independently held local logger. These are two ownership omissions, not evidence of a measured handle leak or lost production logs.
Recommended transform lifetime:
Code block (csharp)
using var decryptor = aesAlg.CreateDecryptor();
var decrypted = decryptor.TransformFinalBlock(encrypted, 0, encrypted.Length);
Keep the startup logger as its concrete disposable Serilog.Core.Logger type and dispose it in the existing finally, or adopt Serilog's supported single bootstrap-to-host logger lifecycle. Preserve static logger shutdown as appropriate. Benefit / acceptance: repeated decrypt operations and host startup/shutdown dispose every owned resource; do not weaken crypto error propagation. Related family: GEN-6 crypto-ownership variant; the independent bootstrap ownership is local. Suggested JIRA: SRA-FR-13.
SRA-FR-14 - Required configuration is validated too late
Category: MustChange/rewrite. Priority: High. Checklist: section 11 missing/invalid configuration fails fast (Major). Locations: Program.cs:38 (../../SWIFT/Swift.Api.Rejection/Program.cs#L38), AuthService.cs:98 (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs#L98), ConnectionStringProvider.cs:17 (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConnectionStringProvider.cs#L17).
Current:
Code block (csharp)
.AddJwtBearer(options =>
{
    var key = builder.Configuration["Jwt:Key"];
JWT validation is inside a lazily resolved authentication-options callback; connection configuration is checked only when a service calls the provider. No startup options validation exists, while both tracked JWT keys are empty pending a secure override. Program also logs "started successfully" before app.Run has bound a listener. Missing key material can fail a first authentication/HTTP request, potentially including health middleware paths; it is not established that /health remains green with an invalid JWT key.
Recommended options direction:
Code block (csharp)
builder.Services.AddOptions<JwtOptions>()
    .BindConfiguration("Jwt")
    .ValidateDataAnnotations()
    .Validate(HasStrongSigningKey, "Missing or invalid JWT signing material.")
    .ValidateOnStart();
Implement typed issuer/audience/key and configuration-path validation; read/validate local configuration safely without logging values. Keep secrets external and use secure rotation/overrides. Emit real-start confirmation through ApplicationStarted. Benefit / acceptance: missing/blank/invalid key, issuer, audience, file/key/connection fail startup with a safe actionable diagnostic; valid secure overrides start normally. Separate dependency readiness is SRA-FR-21. Suggested JIRA: SRA-FR-14.
Rewrite Recommended
SRA-FR-15 - Aggregate and validate all role rows before issuing a token
Category: rewrite_recommended. Priority: Medium. Checklist: sections 3/12, mapping correctness/contracts (Major). Location: AuthService.cs:33 (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs#L33).
Current:
Code block (csharp)
(user, role) =>
{
    user.Role.Add(role);
    return user;
}
The per-row map is followed by FirstOrDefault(). Dapper's row-local users are not aggregated, so if login returns multiple roles, only the first user's single mapped role reaches BuildToken. The collection/token/permission APIs otherwise accept multiple roles. The isolated row-shape probe demonstrates the loss; the missing login procedure may currently guarantee one role, so production incidence is unverified. Also settle roleless/null and RoleEnabled aliases/filtering before changing eligibility logic.
Recommended mapping direction:
Code block (csharp)
if (!users.TryGetValue(row.UserId, out var aggregate))
{
    aggregate = row;
    users.Add(row.UserId, aggregate);
}
if (role is not null && IsEligibleRole(role))
    AddRoleOnce(aggregate, role);
Aggregate by user ID, deduplicate role IDs and explicitly reject inconsistent multi-user results. Define zero/one/many/disabled-role behavior with the DB owner, or enforce/document a strict single-role result contract instead. Benefit / acceptance: all and only eligible roles enter the token; zero/multiple rows cannot produce accidental permissions or null-reference failures. Suggested JIRA: SRA-FR-15.
SRA-FR-16 - Propagate request cancellation through asynchronous work
Category: rewrite_recommended. Priority: Medium. Checklist: section 6 cancellation/end-to-end async (Major). Locations: IAuthService.cs:9 (../../SWIFT/Swift.Api.Rejection/Services/Interfaces/IAuthService.cs#L9), IInsertionService.cs:7 (../../SWIFT/Swift.Api.Rejection/Services/Interfaces/IInsertionService.cs#L7), InsertionService.cs:29 (../../SWIFT/Swift.Api.Rejection/Services/InsertionService.cs#L29).
Current:
Code block (csharp)
await connection.OpenAsync();
var inserted = await command.ExecuteNonQueryAsync() > 0;
Controller/service signatures do not carry a cancellation token; disconnected requests can continue SQL work and validation. Broad boundary catches also lack a distinct expected-cancellation path. This is not sync-over-async: actual I/O is already asynchronous.
Recommended:
Code block (csharp)
await connection.OpenAsync(cancellationToken);
await command.ExecuteNonQueryAsync(cancellationToken);
var definition = new CommandDefinition(procedure, parameters,
    commandType: CommandType.StoredProcedure, cancellationToken: cancellationToken);
Update interfaces/callers together, pass HttpContext.RequestAborted, and respect the authorization handler's framework-mandated signature. Do not label cancellation an application fault. A cancelled write may already have committed; reconcile via SRA-FR-9's explicit outcome/replay contract. Benefit / acceptance: cancellation reaches the provider and expected aborts are not logged as unexplained 500s. Suggested JIRA: SRA-FR-16.
SRA-FR-17 - Repeated configuration/decryption I/O bypasses an existing reuse seam
Category: rewrite_recommended. Priority: Medium. Checklist: sections 7/12, avoid duplicated work (Major). Locations: ConnectionStringProvider.cs:23 (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConnectionStringProvider.cs#L23), ConfiguracionEncriptadaHelper.cs:29 (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConfiguracionEncriptadaHelper.cs#L29).
Current:
Code block (csharp)
var connectionString = ServiciosConfigConexionHelper.ObtenerCadenaConexionDescrifrada(ruta, connectionName);
There are exactly three provider/SqlConnection setup call sites: two in AuthService and one in InsertionService. For an authorized rejection, permission checking and insertion resolve the same scoped provider but each rereads/parses/decrypts XML. The cost is synchronous filesystem/crypto work; no measured latency or throughput impact is claimed.
Recommended reuse point:
Code block (csharp)
return _validatedConnections.GetOrAdd(connectionName, ResolveAndValidateConnection);
Implement successful-result memoization in the existing scoped provider, with a concurrency-safe strategy if it is used in parallel. Define rotation/reload behavior before extending lifetime; do not silently cache failures or hold stale secrets globally. Do not abstract three SQL commands into a generic repository solely to eliminate three short constructors. Benefit / acceptance: a permission-plus-insert request performs one configuration resolution; a new scope/reload observes an approved rotated value. SRA-FR-13 still owns resource disposal. Suggested JIRA: SRA-FR-17.
SRA-FR-18 - Version the missing database contracts and installation path
Category: rewrite_recommended. Priority: Medium. Checklist: sections 8/11, reviewed/reproducible database changes (Major). Locations: AuthService.cs:34 (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs#L34), InsertionService.cs:32 (../../SWIFT/Swift.Api.Rejection/Services/InsertionService.cs#L32), permission script:1 (../../SWIFT/Swift.Api.Rejection/Scripts/usp_Rejection_GetPermissionsByRole.sql#L1).
Current:
Code block (sql)
USE [InterfacesT24];
GO
ALTER PROCEDURE dbo.usp_Rejection_GetPermissionsByRole
Three procedures are invoked, but only one definition is present, as an ALTER against an already-existing named database. This root cannot bootstrap/verify the login row shape, role eligibility, insertion uniqueness/status, parameter widths or transaction behavior. An external DBA release pipeline may own them; its absence from this audit is not proof it does not exist.
Recommended, subject to the supported SQL version:
Code block (sql)
CREATE OR ALTER PROCEDURE dbo.usp_Rejection_GetPermissionsByRole
    @RoleIdsCsv NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
Supply a versioned migration/rollback and disposable-database contract fixtures for all three procedures, or link the authoritative external releases. Select the target database explicitly in deployment rather than editing an unchecked USE statement by hand. Verify SQL/compatibility support for CREATE OR ALTER and STRING_SPLIT. Pin typed parameter sizes to DDL, not guesses. Benefit / acceptance: a clean approved test database and application revision can be deployed/reconciled together. This is the shared prerequisite for SRA-FR-7/9/15, not three duplicated procedure tickets. Suggested JIRA: SRA-FR-18.
SRA-FR-19 - Align supported authentication/runtime versions before the support deadline
Category: rewrite_recommended. Priority: High. Checklist: sections 11/16, dependency lifecycle and supported .NET standards (Major). Locations: Swift.Api.Rejection.csproj:4 (../../SWIFT/Swift.Api.Rejection/Swift.Api.Rejection.csproj#L4), Swift.Api.Rejection.csproj:15 (../../SWIFT/Swift.Api.Rejection/Swift.Api.Rejection.csproj#L15), global.json:3 (../../SWIFT/Swift.Api.Rejection/global.json#L3).
Current:
Code block (xml)
<PackageReference Include="Microsoft.AspNetCore.Authentication.JwtBearer" Version="6.0.32" />
The host targets net8.0 while authentication is pinned to a 6.0-era component. Microsoft's policy lists .NET/ASP.NET Core 6 support ended 2024-11-12, and .NET 8 support ends 2026-11-10, 26 days after this review. A major-version mismatch is not proof of a compile/runtime failure or a specific CVE. No SCA/feed-compliance result is available.
Recommended candidate target, only after organization approval/compatibility tests:
Code block (xml)
<TargetFramework>net10.0</TargetFramework>
Use an approved supported runtime (for example .NET 10 LTS), align JwtBearer and the complete transitive graph, pin actual approved patch versions, and update SDK/CI/deployment runtime together. Do not use a floating version or stop at .NET 9, whose listed support deadline is also 2026-11-10. Benefit / acceptance: approved clean build/SCA, JWT success/failure/expiry fixtures, smoke and rollback evidence on a supported stack before the deadline. Source: Microsoft lifecycle policy (https://dotnet.microsoft.com/en-us/platform/support/policy/dotnet-core), checked 2026-10-15; page dated 2026-09-08. Suggested JIRA: SRA-FR-19. This is a compatibility update, not a full rewrite recommendation.
SRA-FR-20 - Emit useful correlated exception diagnostics without payloads
Category: rewrite_recommended. Priority: Medium. Checklist: section 5 contextual, exception-aware logging (Major). Locations: AuthController.cs:92 (../../SWIFT/Swift.Api.Rejection/Controllers/AuthController.cs#L92), appsettings.json:19 (../../SWIFT/Swift.Api.Rejection/appsettings.json#L19), appsettings.Development.json:19 (../../SWIFT/Swift.Api.Rejection/appsettings.Development.json#L19).
Current:
Code block (csharp)
_logger.LogError("Server error while trying to login an user: {Mensaje}\r\n{StackTrace}",
    ex.Message, ex.StackTrace);
This loses structured inner-exception context, important because the configuration helper deliberately wraps root failures. All three configured output templates in the two settings files omit {Exception}; proper exception-overload events such as the startup Fatal therefore do not render their exception through these file/console templates. Application events also lack an explicitly wired request correlation scope.
Recommended:
Code block (csharp)
_logger.LogError(ex, "Login failed. TraceId={TraceId}", HttpContext.TraceIdentifier);
Add {Exception} to the sink templates and request correlation/enrichment consistently; retain generic outward errors and SRA-FR-8 redaction. Use the exception overload, not an exception serialized as a message property. Confirm inner messages contain no secret/payload values. Benefit / acceptance: a synthetic nested configuration failure is traceable end-to-end with its cause and no sensitive marker. Suggested JIRA: SRA-FR-20.
SRA-FR-21 - Separate host liveness from dependency readiness
Category: rewrite_recommended. Priority: Medium. Checklist: section 14b production readiness/observability (Major). Locations: Program.cs:57 (../../SWIFT/Swift.Api.Rejection/Program.cs#L57), Program.cs:94 (../../SWIFT/Swift.Api.Rejection/Program.cs#L94).
Current:
Code block (csharp)
builder.Services.AddHealthChecks();
app.MapHealthChecks("/health");
No check is registered. With valid JWT configuration, an unavailable SQL/configuration dependency is not represented by this liveness endpoint. Do not conflate SRA-FR-14's lazy JWT-option exception with a demonstrated always-green invalid-key health response.
Recommended endpoint split:
Code block (csharp)
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready")
});
Register bounded-time configuration/SQL readiness checks tagged ready, protect details, and wire the actual deployment probes. Use no DML or real financial operation for a probe. Preserve /health compatibility or explicitly migrate its consumers. Benefit / acceptance: dependency failure removes readiness without mistaking a living process for a functioning rejection service. Suggested JIRA: SRA-FR-21.
Nice to Rewrite if Time Permits
SRA-FR-22 - Clarify asynchronous and permission names; remove small dead fragments
Category: NiceToRewrite_ifTimePermits. Priority: Low. Checklist: sections 1/12 naming and dead code (Minor). Locations: IAuthService.cs:9 (../../SWIFT/Swift.Api.Rejection/Services/Interfaces/IAuthService.cs#L9), User.cs:18 (../../SWIFT/Swift.Api.Rejection/Model/Security/User.cs#L18), ConfiguracionEncriptadaHelper.cs:14 (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConfiguracionEncriptadaHelper.cs#L14).
Current:
Code block (csharp)
public Task<IEnumerable<Permission>> GetRoles(IEnumerable<int> roleIds);
The method returns permissions, not roles; async methods omit the Async suffix. The helper's Descrifrada spelling, unused imports and commented bcrypt scratch line add small avoidable friction. They are not defects or reasons to rewrite a working subsystem.
Recommended:
Code block (csharp)
Task<IEnumerable<Permission>> GetPermissionsForRolesAsync(
    IEnumerable<int> roleIds, CancellationToken cancellationToken);
Rename interfaces/implementations/callers coherently when touching them for real work; preserve routes, JSON fields and existing language conventions. Use the project's normal formatter/unused-import tooling rather than a broad cosmetic rewrite. Benefit / acceptance: names accurately describe data/async behavior with no public HTTP contract change. Suggested JIRA: SRA-FR-22.
4.2 Security Report (references, not additional findings)
Finding	Priority	Exploitability / source condition	Business impact	CVSS-style score	Remediation
SRA-FR-1	Blocker	Authorized caller supplies unvalidated FIN fields; synthetic newline boundary demonstrated	Malformed/altered outbound financial text	Not scored; deployment/business preconditions unknown	Validated allow-list and exact wire fixtures
SRA-FR-7	Blocker	Included SQL uses NOLOCK; requires a concurrent uncommitted mapping change	Transient unauthorized write decision	Not scored; concurrency/deployment not exercised	Committed reads and isolation contract test
SRA-FR-8	Blocker	Debug enabled, explicitly true in Development; payload contains sensitive text	Wider file/console log exposure	Not scored; data/access classification unknown	Remove body/DTO logging and test redaction
SRA-FR-10	High	Anonymous login lacks local throttling; external control not evidenced	Password guessing/resource abuse	Not scored; network/WAF unknown	Verified application/platform limits and backoff
SRA-FR-11	Medium	Anonymous failures have different text/work	Account existence/state disclosure	Not scored; rate/identity threat model unknown	Uniform public failures and measured timing mitigation

SQL injection, unsafe deserialization and prohibited DES/3DES/ECB/MD5/SHA1 algorithms: No issues detected in these areas in the reviewed source. This does not certify external procedures/binaries or cryptographic storage ACLs. No known-CVE assertion or full penetration test is included. SRA-FR-19 is a lifecycle/compatibility recommendation, not a fabricated vulnerability.
4.3 Performance and Scalability Report
Existing finding	Location / observation	Evidence and recommendation
SRA-FR-17	Three connection-provider call sites; two resolutions on a protected write	Cache safely at the existing scope; measure real latency before wider optimization
SRA-FR-16	SQL work has no propagated cancellation	Bound abandoned-request work; reconcile writes that may already have committed
SRA-FR-1/3	Unbounded dynamic message fields and wrapping scans	Enforce protocol limits and use a lossless linear formatter
SRA-FR-10	bcrypt and SQL on anonymous attempts	Budget/rate-limit work rather than invent a password-hash performance shortcut

No measured latency, throughput, memory or execution-plan evidence exists. No N+1 loop, sync-over-async or actual missing-index finding was established. Do not use this table as a load-test or capacity result.
4.4 Findings by Area / Additional Improvements
Project area	Findings	Focus
DTO and formatter	SRA-FR-1 through SRA-FR-6	Valid, lossless, deterministic financial output
Authorization SQL	SRA-FR-7	Committed permission decisions
Controller/auth boundary	SRA-FR-8/10/11/14	Sensitive data, login controls and predictable failures
Persistence and DB contracts	SRA-FR-9/15/18	Explicit outcomes, correct role aggregation and versioned integration
Testability/resources	SRA-FR-12/13	Safe regression suite and owned lifetime
Reliability/observability	SRA-FR-16/17/20/21	Cancellation, reuse, correlation and readiness
Support/naming	SRA-FR-19/22	Timely supported stack and optional cleanup

SOLID/DI boundaries are already present. DDD, a generic repository or a saga is not required merely to satisfy a checklist name. The needed extra seams are the reference/clock and external configuration/SQL boundaries demonstrated above.
4.5 Test Quality Issues
SRA-FR-12 is the single categorized test finding; do not create duplicate test tickets for every production issue. Bug-fix tickets still require their own regression. Minimum test matrix:
Boundary	Cases	Required evidence
Input	Missing/null/empty/whitespace, disallowed controls, lengths, BIC/currency/type, amount range	400 and no insertion for invalid input
Formatter	Full approved bytes, CRLF, long tokens, all boundaries, line-count limits	No lost content and deterministic complete message
Money	Decimal midpoints/scales/ranges and multiple cultures	Approved exact numeric text
References	Same-second parallel calls, multiple nodes/restarts, midnight, replay	Unique new references, approved stable retry semantics
Login/token	Unknown/wrong/disabled, malformed hash/config, one/multiple/zero roles, invalid/expired JWT	Uniform failures, correct claims and validation
Authorization	Missing/invalid roles, disabled permissions, uncommitted grant/rollback, DB outage	Fail closed; no dirty-read grant
Insert	Inserted/duplicate/rejected/unknown, NOCOUNT, trigger counts, dropped response, cancellation	Truthful result and safe reconciliation
Operations	Missing/corrupt XML/key, secret rotation, readiness outage, synthetic sensitive marker	Safe diagnostics, disposal and no payload leakage

5. Architecture-Fit Note
Apply the API-specific trade-off table in orchestration (./_ORCHESTRATION.md#swiftapirejection-scoped-audit-checkpoint---2026-10-15), not the portfolio's predominantly Windows Service examples. This workload is an HTTP request followed by an authorization query, in-memory message creation and one SQL handoff. Keep ASP.NET Core and the existing service interfaces while fixing correctness, adding tests and meeting the supported-runtime deadline. A queue/worker is justified only if approved asynchronous acceptance/retry/load requirements demand it; Functions requires hosting/network/SLA evidence, and no compensating multi-step saga or independently owned microservice boundary is demonstrated. SLAs, traffic, hosting constraints, high availability, budget and downstream delivery guarantees are unknown. Fix defects -> remove duplication/add tests -> supported compatibility update -> measured architecture decision; never make a full rewrite the first response to this audit.
6. JIRA-Ready Backlog
All rows belong to project Swift.Api.Rejection, tier 0. Finding IDs are suggested ticket keys, not created JIRA issues. Category labels are mutually exclusive. File links resolve to the reviewed source; exact lines are in each finding.
ID	Title	Category	Project	File(s)	Priority
SRA-FR-1	Validate every dynamic FIN field before message creation	MustChange/rewrite	Swift.Api.Rejection	DTO (../../SWIFT/Swift.Api.Rejection/Model/RejectionDto.cs), formatter (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs), controller (../../SWIFT/Swift.Api.Rejection/Controllers/RejectionController.cs)	Blocker
SRA-FR-2	Make reason a passive null-safe validated DTO property	MustChange/rewrite	Swift.Api.Rejection	RejectionDto (../../SWIFT/Swift.Api.Rejection/Model/RejectionDto.cs)	Blocker
SRA-FR-3	Replace lossy regex/DTO chunking with lossless wrapping	MustChange/rewrite	Swift.Api.Rejection	formatter (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs), DTO (../../SWIFT/Swift.Api.Rejection/Model/RejectionDto.cs)	Blocker
SRA-FR-4	Allocate collision-free compliant transaction references	MustChange/rewrite	Swift.Api.Rejection	formatter (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs) and insertion contract	Blocker
SRA-FR-5	Use exact decimal money and explicit approved rounding	MustChange/rewrite	Swift.Api.Rejection	DTO (../../SWIFT/Swift.Api.Rejection/Model/RejectionDto.cs), formatter (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs)	Blocker
SRA-FR-6	Make wire amount culture independent	MustChange/rewrite	Swift.Api.Rejection	formatter (../../SWIFT/Swift.Api.Rejection/Services/MtCreationService.cs)	Blocker
SRA-FR-7	Remove dirty reads from authorization mapping	MustChange/rewrite	Swift.Api.Rejection	permission SQL (../../SWIFT/Swift.Api.Rejection/Scripts/usp_Rejection_GetPermissionsByRole.sql)	Blocker
SRA-FR-8	Remove full financial DTO/message logging	MustChange/rewrite	Swift.Api.Rejection	RejectionController (../../SWIFT/Swift.Api.Rejection/Controllers/RejectionController.cs) and logging settings	Blocker
SRA-FR-9	Replace row-count inference with explicit insertion outcome	MustChange/rewrite	Swift.Api.Rejection	InsertionService (../../SWIFT/Swift.Api.Rejection/Services/InsertionService.cs) and missing insertion SQL	High
SRA-FR-10	Enforce verified login rate/backoff and input-budget controls	MustChange/rewrite	Swift.Api.Rejection	AuthController (../../SWIFT/Swift.Api.Rejection/Controllers/AuthController.cs), validator (../../SWIFT/Swift.Api.Rejection/Validators/CredentialsValidator.cs), composition	High
SRA-FR-11	Unify public invalid-login responses	MustChange/rewrite	Swift.Api.Rejection	AuthController (../../SWIFT/Swift.Api.Rejection/Controllers/AuthController.cs), AuthService (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs)	Medium
SRA-FR-12	Establish financial/auth regression suite	MustChange/rewrite	Swift.Api.Rejection	solution (../../SWIFT/Swift.Api.Rejection/Swift.Api.Rejection.sln), new approved test project	High
SRA-FR-13	Dispose owned decryptor and independent startup logger	MustChange/rewrite	Swift.Api.Rejection	configuration helper (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConfiguracionEncriptadaHelper.cs), Program (../../SWIFT/Swift.Api.Rejection/Program.cs)	Blocker
SRA-FR-14	Validate required secure configuration before startup success	MustChange/rewrite	Swift.Api.Rejection	Program (../../SWIFT/Swift.Api.Rejection/Program.cs), provider (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConnectionStringProvider.cs)	High
SRA-FR-15	Aggregate and validate login role mapping	rewrite_recommended	Swift.Api.Rejection	AuthService (../../SWIFT/Swift.Api.Rejection/Services/AuthService.cs) and login contract	Medium
SRA-FR-16	Propagate request cancellation without misreporting commits	rewrite_recommended	Swift.Api.Rejection	interfaces (../../SWIFT/Swift.Api.Rejection/Services/Interfaces), controllers/services	Medium
SRA-FR-17	Memoize validated configuration at existing scoped seam	rewrite_recommended	Swift.Api.Rejection	provider (../../SWIFT/Swift.Api.Rejection/Services/Conexion/ConnectionStringProvider.cs)	Medium
SRA-FR-18	Version all three procedure/deployment contracts	rewrite_recommended	Swift.Api.Rejection	SQL scripts (../../SWIFT/Swift.Api.Rejection/Scripts), authoritative DB release	Medium
SRA-FR-19	Align supported auth/runtime/SDK before 2026-11-10	rewrite_recommended	Swift.Api.Rejection	project (../../SWIFT/Swift.Api.Rejection/Swift.Api.Rejection.csproj), SDK JSON (../../SWIFT/Swift.Api.Rejection/global.json), CI/deployment	High
SRA-FR-20	Preserve correlated structured exception diagnostics	rewrite_recommended	Swift.Api.Rejection	AuthController (../../SWIFT/Swift.Api.Rejection/Controllers/AuthController.cs), both settings files	Medium
SRA-FR-21	Add bounded dependency readiness alongside liveness	rewrite_recommended	Swift.Api.Rejection	Program (../../SWIFT/Swift.Api.Rejection/Program.cs), deployment probe contract	Medium
SRA-FR-22	Clarify async/permission names and remove dead fragments	NiceToRewrite_ifTimePermits	Swift.Api.Rejection	IAuthService (../../SWIFT/Swift.Api.Rejection/Services/Interfaces/IAuthService.cs), callers/helper/User	Low

6.1 Action Plan and Dependencies
1. Release blockers: SRA-FR-1 through SRA-FR-8 and SRA-FR-13, with regressions under SRA-FR-12.
Resolve the FIN and reference allocator contracts before choosing limits/encoding; do not silently change financial meaning.
2. Reliability/security completion: SRA-FR-9/10/11/14. Obtain the versioned database contract (SRA-FR-18) to confirm/retriage
conditional insertion/role behavior and test the real disposable-schema outcomes. Missing evidence is not a passing result.
3. Performance/operations: SRA-FR-16/17/20/21; measure before broader optimization. Record safe cancellation/replay semantics.
4. Compatibility, not re-platforming: execute SRA-FR-19 before 2026-11-10, alongside the minimum defect/test work.
Its deadline is earlier than a generic 90-day modernization plan; do not postpone it behind optional refactoring.
5. Production hardening/cleanup: verify TLS/proxy trust, secret rotation/ACLs, rate controls, readiness consumers, log retention,
deployment/SCA/rollback and downstream acceptance; SRA-FR-22 is optional after these.
6.2 Scorecard
No agreed numerical quality rubric or runtime benchmarks were supplied. To avoid false precision, this audit does not fabricate /10 scores.
Category	Source assessment	Score /10
Security	Existing bcrypt/JWT/SQL safeguards; validation, dirty authorization reads, payload logs and login gaps remain	Not numerically rated
Performance	Repeated synchronous configuration work observed; load impact unmeasured	Not measured
Maintainability	Small DI-based design; role/result/configuration contracts need clarification	Not numerically rated
Scalability	Reference collision defect; topology/traffic unverified	Not measured
Reliability	Lossy wrapping, input/configuration/outcome and resource defects remain	Not numerically rated
Testability	Interfaces exist; local regression project absent	Coverage not measured
Architecture	Current HTTP API is an appropriate source-evidenced fit; no rewrite case established	Not numerically rated
Overall	High provisional source risk, with explicit external verification gaps	Not numerically rated

6.3 Production Readiness and Final Recommendation
Ready to approve a new production release from the available evidence: NO. Confidence is High in the stated source observations; production incidence and deployment protections remain unverified. This is not an outage assertion or a claim that a particular currently deployed revision is compromised.
• Nine priority-Blocker findings remain open: SRA-FR-1/2/3/4/5/6/7/8/13.
• Five other must-change findings remain open: SRA-FR-9/10/11/12/14.
• Seven recommended and one optional finding remain open; conditional findings may be closed/re-triaged only with authoritative evidence.
• Full build/test/SCA, missing SQL contracts, owner-approved wire output and verified deployment/TLS/readiness/rollback are still required.
• The supported-runtime deadline must be scheduled, not treated as an optional architecture rewrite.
Executive recommendation: repair/test this API in place, validate its external contracts and move to an approved supported stack. Prioritize correct, authorized, traceable financial-message creation over cosmetic standards or a speculative new hosting model. All remedies are recommendations only; no application source/configuration/SQL change or commit was made by this audit.
See the validation record (./_Swift.Api.Rejection_Validation.md), handover playbook (./_Swift.Api.Rejection_HANDOVER_PLAYBOOK.md) and captured prompt history (./_Swift.Api.Rejection_PROMPT_HISTORY.md) for continuation and evidence limits.
