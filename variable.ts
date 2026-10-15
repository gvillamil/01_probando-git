Impresion Recommendations
Impresion - Architecture & Code Quality Recommendations
Solution: CMS/Impresion/Impresion.slnx (../../CMS/Impresion/Impresion.slnx) Reviewed source: branch develop, commit 7e0b0891500c74a436bc28961df283525d73c910 Compared against: LoansApp architecture and coding baseline (../BaseLine_Docs/LoansApp_Architecture_Patterns_and_Coding_Practices.md) and PR review/severity checklist (../BaseLine_Docs/code_review.md) See also: _ORCHESTRATION.md (_ORCHESTRATION.md) for the portfolio methodology, template, and standing findings.
1. Solution Overview (low/no-dependency -> high dependency)
Tier	Project	Kind	Path
0	AplicacionImpresion	.NET Framework 4.8 class library	AplicacionImpresion.csproj (../../CMS/Impresion/AplicacionImpresion/AplicacionImpresion.csproj)
1	InfraestructuraImpresion	.NET Framework 4.8 class library (the project folder is itself named InfraestructuraImpresion.csproj)	InfraestructuraImpresion.csproj (../../CMS/Impresion/InfraestructuraImpresion.csproj/InfraestructuraImpresion.csproj)
2	Impresion	.NET Framework 4.8 Windows Service (Exe)	Impresion.csproj (../../CMS/Impresion/Impresion/Impresion.csproj)

Non-project artifacts that are part of the deliverable: SQL/ (../../CMS/Impresion/SQL) (two deployable stored procedures plus one stale script), lib/CrystalReports/ (../../CMS/Impresion/lib/CrystalReports) (three vendored SAP assemblies), and Directory.Build.targets (../../CMS/Impresion/Directory.Build.targets) (post-build copy of Crystal Reports files).
What this solution actually does: Impresion is a timer-driven Windows Service that renders and prints operational documents for the CMS/TOMI middleware. On each tick it records itself as executing, reads the system date and five system parameters, and checks the operating window HORA_OP_INICIO-HORA_OP_FIN. Inside the window it calls usp_impresion_obtenerPendientes, which unions nine pending-document sources into one ordered work list: domestic FX deals (FXD_Local, FXI_Local), special FXI operations, three kinds of user-token request forms, two HotScan alert sources, and inbound SWIFT confirmations. That list contains 14 process/report pairs using 12 distinct Crystal Reports .rpt templates, each mapped to a printer through tb_srv_reporte_impresora. The application groups rows by (Proceso, Folio); for every folio it loads each template from the directory held in the REPORTES_DIRECTORIO parameter, binds the report to the TOMI database with the service's own SQL credentials, sets Crystal formulas from a SQL-built Formulas string, renders the report, and - once every report of the folio succeeded - calls usp_impresion_marcarImpreso to flip the per-source "printed" flag. As committed on develop, the physical print call is disabled and each report is exported to a PDF instead (IMP-FR-1).
2. Scope & Methodology Note
This is a new review after the authoritative reset dated 2026-10-07. Earlier recommendation documents and severities were not used as evidence.
• Read 30/30 Git-tracked artifacts: 27 text files / 2,659 lines in full, including 14 C# files / 987 lines, all three projects, the solution file, Directory.Build.targets, App.config, both deployable stored procedures plus the stale script (693 lines of SQL), README, .editorconfig, .gitattributes, and .gitignore. The three binary Crystal Reports assemblies were inspected through assembly metadata, file version, and SHA-256 rather than line-read. The physical file listing equals the tracked list; no untracked, ignored, or generated file exists. The solution is below the large-project threshold, so no persistent progress tracker was required.
• The repository was clean at inventory time on develop (HEAD 7e0b089, 2026-08-28). origin/CodeReview and origin/CrystalReportPrueba both point to e6fd003 (2026-09-14, "CodeReview HMN, NTT & Copilot fixes"), which is 1 commit ahead of and 4 commits behind develop; per its file list it adds a test project, an ICrystalReporterWorker interface, and a result type, and deletes SQL/Scripts.sql. That branch was not reviewed and was not used as evidence. Re-verify the findings against it before scheduling work if it becomes the merge candidate.
• Per the checklist's shared-library rule, the MUFG_T24 library (source in General/MUFGT24_BTMUM_64 (../../General/MUFGT24_BTMUM_64)) and the shared lifecycle stored procedures in StoredProcedures/MWD_DB (../../StoredProcedures/MWD_DB) were read only to verify the effective runtime behavior of calls this solution makes. No finding is located inside MUFG_T24; behaviors this solution depends on are listed under *Dependency Notes*.
• Supplemented the complete read with quantified scans for secrets, test markers, async/exception anti-patterns, CI definitions, obsolete-API call sites (27), SQL report/process counts (14 pairs, 12 templates, 9 processes), and .rpt files anywhere in the workspace (none).
• No JIRA/ADO work item or acceptance criteria was supplied. Business expectations are therefore limited to behavior expressed by current code, class documentation, log messages, SQL, and the portfolio baseline.
• Build and test execution were unavailable: dotnet, msbuild, vstest.console, nuget, and vswhere are not installed/discoverable, D:\ServiceCMS\MUFG_T24.dll and SimpleInjector.dll are absent, and no Crystal Reports native runtime is present. Validation is static plus document/tooling checks only. Production source was not modified.
Finding Summary
Category	Blocker	Major	Minor	Total
MustChange/rewrite	5	7	0	12
rewrite_recommended	0	7	5	12
NiceToRewrite_ifTimePermits	0	0	3	3
Total	5	14	8	27

Fresh Cross-Cutting Checks
Candidate	Result	Current evidence
GEN-1 absolute DLL references	Yes	Three projects reference D:\ServiceCMS\MUFG_T24.dll and the host also references D:\ServiceCMS\SimpleInjector.dll (IMP-FR-22); three SAP assemblies are vendored instead (IMP-FR-24).
GEN-2 zero automated tests	Yes	The solution file lists three projects and none is a test project; no test framework reference or test attribute exists (IMP-FR-23).
GEN-3 duplicate/leftover project files	No	Each project has exactly one .csproj. The duplicate artifact is a stale SQL script, tracked as IMP-FR-20.
GEN-4 near-identical solution pairs	No	No twin solution was identified in this review.
GEN-5 sensitive logging	No	No credential, account number, or personal data is logged; only report names, printer names, folio identifiers, and file paths. Sensitive PDF data at rest is tracked locally as IMP-FR-6.
GEN-6 undisposed signing helper	No	No signing helper exists. The Crystal ReportDocument is closed and disposed in a finally block.
GEN-7 finalization only on success	Yes	ActualizaTerminacion is reached only on the all-success path; out-of-hours and exception paths leave the service marked as executing (IMP-FR-4).
GEN-8 nullable PID dereference	No	Three Pid.Value/(int)Pid sites exist, but the shared startup assigns Pid before any fallible call and ticks await startup, so they are latent rather than confirmed defects. A hardening note is part of IMP-FR-16.
GEN-9 discarded service-state results	Yes	Startup and shutdown await but ignore MarcarPuertoEnUsoAsync Boolean results, and every ActualizaTerminacion result is ignored (IMP-FR-17).
GEN-10 per-tick out-of-hours alert (proposed)	Yes	Every tick outside the operating window logs an ERROR and inserts a new alert row (IMP-FR-7).

Dependency Notes (MUFG_T24 context only)
These shared behaviors are not findings against this solution; they explain why the local findings matter.
1. EjecutarStoredProcedureQuery<T> catches SqlException, sets Exito=false, and returns normally with an empty Data list; it neither throws nor logs on that path (RepositorioMaestro.cs (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L385-L409)). The query/DAO caller must inspect Exito. With validarAfectacion: true it throws when no row was affected or returned; the FilasAfectadas result column is the supported way to report an update count (lines 533-562 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L533-L562)).
2. ActualizaEJecucion throws when the stored procedure reports failure, whereas ActualizaTerminacion logs and returns Exito=false (RepositorioMaestro.cs (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L1158-L1230)). The stored procedures set status = 1 and status = 0 respectively (001 (../../StoredProcedures/MWD_DB/DBMDW/General/MUFG_T24_BTMUM_64/IT24/Procedures/001%20SP%20usp_ActualizaEjecucion_MUFGLibreria.sql#L47), 007 (../../StoredProcedures/MWD_DB/DBMDW/General/MUFG_T24_BTMUM_64/IT24/Procedures/007%20SP%20usp_ActualizaTerminacion_HotScan.sql#L48)).
3. MarcarPuertoEnUsoAsync returns false (not an exception) for a provider-reported failure or cancellation (ControlServicio.cs (../../General/MUFGT24_BTMUM_64/MUFG_T24/Aplicacion/Control/ControlServicio.cs#L50-L64)).
4. ObtenerProcesoActualAsync assigns Pid and NombreProceso before any fallible call and does not validate the display name, which comes from a WMI lookup that returns null on failure; ObtenerProcesoActualValidadoAsync adds that validation (Parametrizacion.cs (../../General/MUFGT24_BTMUM_64/MUFG_T24/Aplicacion/Control/Parametrizacion.cs#L52-L124)).
5. ObtenerParametrosSistemaAsync(string) marks every listed key as required and non-empty and throws otherwise (Parametrizacion.cs (../../General/MUFGT24_BTMUM_64/MUFG_T24/Aplicacion/Control/Parametrizacion.cs#L274-L275), lines 332-345 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Aplicacion/Control/Parametrizacion.cs#L332-L345)). The dictionary indexers used for these keys are therefore safe, and the unused keys still make a tick fail when missing.
6. Alertas.InsertarAlerta performs one plain INSERT per call and logs two extra lines (Alertas.cs (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Alertas/Alertas.cs#L99-L170), SP 031 (../../StoredProcedures/MWD_DB/DBMDW/General/MUFG_T24_BTMUM_64/IT24/Procedures/031%20SP%20usp_InsertaAlerta_MUFGLibreria.sql#L58)); no de-duplication exists in C# or SQL. If the insert fails it rethrows the alert's own exception instead of the insert error.
7. Config and MUFG_T24.Logeo_utilidades.Logging are marked [Obsolete] compatibility shims (Config.cs (../../General/MUFGT24_BTMUM_64/MUFG_T24/Legacy/Config.cs#L11), line 145 (../../General/MUFGT24_BTMUM_64/MUFG_T24/Legacy/Config.cs#L145)).
Items Requiring Business/Integration Confirmation
These are not classified as defects without the missing contract evidence:
• Intended output mode. Confirm whether the target is physical printing, PDF delivery, or both, and who consumes the PDFs. IMP-FR-1 is rated Blocker on the evidence that printing remains the documented contract (see its evidence); if PDF delivery is the intended permanent behavior, its residual defect becomes misleading naming, logging, and flag semantics, and IMP-FR-6 applies in full.
• Production Crystal runtime bitness and version. The managed assemblies are MSIL; the native runtime must match the process (Debug builds x64, Release builds x86; IMP-FR-25).
• Timer interval and alert consumers. The interval comes from the encrypted external service configuration, and the consumers of the alertas table are not in this repository; the code falls back to 10 seconds (IMP-FR-7).
• Backlog and filters. Confirm how many source rows currently have the printed flag at 0, whether FXI_Operaciones_Especiales intentionally has no status predicate, and whether (Mto.folio, Trb.nf_folio) is unique for SWIFT_IN (IMP-FR-19).
• Schema outside this repository. The views vw_nf_scan and vw_solicitudes_usuarios_cms, the tables the procedures read/update, and the two procedures themselves are not in the portfolio's StoredProcedures/MWD_DB (../../StoredProcedures/MWD_DB) database project. Confirm that vw_nf_scan is updatable (the marker procedure updates it directly) and which database holds each object.
• Report connection details. The code passes UserID and Password from the TOMI connection string to Crystal; confirm that connection uses SQL authentication, and whether any template contains subreports (only the main report's DataSourceConnections are configured).
• Directory contract. Confirm the REPORTES_DIRECTORIO value (trailing separator), its ACLs, and the retention expectation (IMP-FR-6, IMP-FR-11).
• Window semantics. Confirm whether an overnight operating window (start later than end) is ever required (IMP-FR-14).
3. What's Already Good
• Dependency direction and DI are clean. AplicacionImpresion has no project reference to infrastructure; InfraestructuraImpresion references only the application project (Infraestructura project (../../CMS/Impresion/InfraestructuraImpresion.csproj/InfraestructuraImpresion.csproj#L89-L94)); the host references both (Impresion.csproj (../../CMS/Impresion/Impresion/Impresion.csproj#L113-L122)). Dependencies are constructor-injected through SimpleInjector, and the container is verified at startup (ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L115-L159)).
• Tick overlap and per-tick scope are handled correctly. A SemaphoreSlim.WaitAsync(0, ct) guard releases in finally, and each tick resolves its services inside an AsyncScopedLifestyle scope (ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L225-L256)).
• Cancellation is rethrown correctly. Both worker methods catch OperationCanceledException and use throw; (CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L61-L64), lines 143-146 (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L143-L146)); there is no throw ex;, .Result, .Wait(), Task.Run, or async void method; the only async-void shape is the timer's Elapsed lambda, whose body is fully wrapped in try/catch. The only blocking call is the framework-mandated GetAwaiter().GetResult() inside OnStop (ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L306-L329)).
• Shutdown is bounded and cooperative. OnStop stops the timer, signals cancellation, and waits at most 60 seconds for the running tick before releasing resources (ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L258-L330)).
• Crystal documents are always released. ReportDocument is closed and disposed in finally, which avoids the Crystal print-job limit leak (CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L152-L159)).
• Database access is parameterized and fixed-shape. The DAO binds @Proceso and @Folio through CrearParametro; both procedures contain only static SQL, and usp_impresion_marcarImpreso dispatches through an explicit literal allow-list that fails loudly for an unknown process (usp_impresion_marcarImpreso.sql (../../CMS/Impresion/SQL/usp_impresion_marcarImpreso.sql#L40-L122)). No SQL concatenation or dynamic SQL exists in the solution.
• The active procedures follow the portfolio's transactional template. SET NOCOUNT ON, SET XACT_ABORT ON, TRY/CATCH, XACT_STATE()-guarded rollback, audit insert into auditoria.errorlog, and THROW; preserve the original error (usp_impresion_marcarImpreso.sql (../../CMS/Impresion/SQL/usp_impresion_marcarImpreso.sql#L123-L149), usp_impresion_obtenerPendientes.sql (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L215-L239)); CREATE OR ALTER keeps deployment idempotent and preserves permissions.
• No secrets are stored or logged. No password, token, or connection string exists in source; database credentials are read from the encrypted external configuration at run time and passed to Crystal without being logged.
• Output ordering is deterministic. Pending rows are ordered by process, folio, and report order in SQL and again in LINQ (OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L112-L131)).
• Build is deterministic. All three projects set <Deterministic>true</Deterministic>, and the Crystal assemblies are referenced through repository-relative paths (their governance gaps are IMP-FR-24).
4. Findings by Project (low/no-dependency -> high dependency)
4.1 AplicacionImpresion (Tier 0)
MustChange / Rewrite
IMP-FR-1 - Printing is disabled, yet each folio is exported to PDF and marked as printed
• Category / severity: MustChange/rewrite - Blocker
• Evidence: CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L119-L141) and the folio-completion call at lines 55-59 (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L55-L59); history: commit d5ceec2 ("Cambio testing", 2026-08-28) commented out the print call and added the export.
Code block (csharp)
documentoReporte.PrintOptions.PrinterName = reporte.Impresora;
Logging.Write("INFO|Imprime_Reporte|Opciones de impresión establecidas, a imprimir reporte.");
ct.ThrowIfCancellationRequested();
//documentoReporte.PrintToPrinter(1, false, 0, 0);
...
documentoReporte.ExportToDisk(ExportFormatType.PortableDocFormat, rutaArchivoPdf);
Logging.Write("INFO|Imprime_Reporte|PDF generado en '" + rutaArchivoPdf + "'.");
Logging.Write("INFO|Imprime_Reporte|Reporte impreso, a cerrar el reporte.");
return true;
The class summary, ImprimirAsync, the log messages ("a imprimir reporte", "Reporte impreso") and the SQL flags (nf_impresion_voucher, flg_impresion) all say the document was printed. On develop nothing is sent to a printer; the PDF is the only output. After every report of a folio succeeds, MarcarImpreso still flips the "printed" flag, so the business record claims a voucher, token form, HotScan alert, or SWIFT confirmation was printed while no printer ever received it. Printing remains the documented contract: the commit message labels the change as a test change, the print call is commented out rather than removed, PrinterName is still set, and a missing printer mapping still fails the report. The Blocker rating assumes physical printing is the required deliverable (see *Items Requiring Business/Integration Confirmation*). The failure is silent: the only trace is a "PDF generado" log line immediately followed by "Reporte impreso", and no alert or database state records that printing was skipped.
Recommendation: make the output mode an explicit, required configuration value with no silent default, and mark a folio only after the selected output has succeeded.
Code block (csharp)
switch (modoSalida)
{
    case ModoSalidaReporte.Impresora:
        documento.PrintOptions.PrinterName = reporte.Impresora;
        documento.PrintToPrinter(1, false, 0, 0);
        break;
    case ModoSalidaReporte.PdfArchivo:
        documento.ExportToDisk(ExportFormatType.PortableDocFormat, rutaArchivoPdf);
        break;
    default:
        throw new InvalidOperationException($"Modo de salida no soportado: {modoSalida}.");
}
If PDF delivery is the intended production behavior, rename the class, log text, and flag semantics accordingly and apply IMP-FR-6.
Benefit: removes test-only behavior from the production path and makes a "printed" record mean that output actually happened.
IMP-FR-2 - Report the tick as failed, and alert, when any folio fails
• Category / severity: MustChange/rewrite - Blocker
• Evidence: OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L134-L146), the failure-to-false conversions at CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L61-L69) and lines 143-151 (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L143-L151), and the host discarding the returned Task<bool> at ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L238-L241)
Code block (csharp)
foreach (ImpresionFolioJob folioEmpaquetado in foliosEmpaquetados)
{
    ct.ThrowIfCancellationRequested();
    await _crystalReportWorker.ImprimirAsync(folioEmpaquetado, ct);
}
ct.ThrowIfCancellationRequested();
await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio);
return true;
ImprimirAsync returns false for every failure - missing printer mapping, missing template, Crystal or formula error, or an exception raised by the completion mark - after writing only to the log file. The orchestrator discards that Boolean, so a tick in which every folio failed still finalizes the execution state and returns true. No alert is inserted for unprinted folios (alerts exist only for out-of-hours and unhandled exceptions), and the host also drops the tick result. Operations see a healthy service while pending documents accumulate and are retried indefinitely (IMP-FR-5).
Recommendation: aggregate per-folio outcomes, alert on failures, and return a failed tick result.
Code block (csharp)
var fallidos = new List<string>();
foreach (ImpresionFolioJob folio in foliosEmpaquetados)
{
    ct.ThrowIfCancellationRequested();
    if (!await _crystalReportWorker.ImprimirAsync(folio, ct))
        fallidos.Add($"{folio.Proceso}:{folio.Folio}");
}
 
if (fallidos.Count > 0)
{
    await _alertas.InsertarAlerta(new AlertaDTO
    {
        Servicio = _configuracion.NombreServicio,
        ClaveAviso = "CRITICO",
        Descipcion = $"Folios sin imprimir: {fallidos.Count}",
        PId = _configuracion.Pid,
        NombreProceso = _configuracion.NombreProceso,
        Exception = new InvalidOperationException(string.Join(", ", fallidos))
    });
    return false;
}
Return a typed result (Succeeded, Skipped, Failed, Cancelled) and have the host log it.
Benefit: converts a silent printing outage into an actionable failed tick.
IMP-FR-3 - Do not treat a failed MarcarImpreso as completed
• Category / severity: MustChange/rewrite - Blocker
• Evidence: CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L55-L59) and RecuperacionDatosDao.cs (../../CMS/Impresion/InfraestructuraImpresion.csproj/Repositorio/RecuperacionDatosDao.cs#L39-L49)
Code block (csharp)
await _recuperacionDatosDao.MarcarImpreso(folioEmpaquetado.Proceso, folioEmpaquetado.Folio, ct);
Logging.Write("INFO|Formato|Se termina procesamiento de Formato: " + folioEmpaquetado.Folio);
return true;
MarcarImpreso returns resultado.Exito. Because the shared repository converts a SQL failure into Exito=false without throwing (dependency note 1), the worker can log "Se termina procesamiento" and return true although the folio was never marked. The folio then reappears in the next tick's pending list and every report is regenerated - an unbounded duplicate-output loop for a persistent database fault, with no log line that mentions the failed mark.
Recommendation:
Code block (csharp)
if (!await _recuperacionDatosDao.MarcarImpreso(folio.Proceso, folio.Folio, ct))
{
    Logging.Error($"|Imprime_Operaciones|No se pudo marcar como impreso {folio.Proceso}:{folio.Folio}.");
    return false;
}
Combine with IMP-FR-18 so that a zero-row update also surfaces.
Benefit: stops silent duplicate output and makes database failures visible at the point they occur.
IMP-FR-4 - Finalize execution state on every non-cancelled path (GEN-7)
• Category / severity: MustChange/rewrite - Major
• Evidence: marker at OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L64), early return at lines 91-106 (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L91-L106), only finalization at lines 143-146 (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L143-L146), and the catch that returns without finalizing at lines 152-166 (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L152-L166)
Code block (csharp)
await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio);   // status = 1
...
return false;                                                                // out of hours: not finalized
...
await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio); // status = 0, success path only
return true;
...
catch (Exception ex) { ...; }                                                // not finalized
return false;
The shared procedures set tb_srv_controladoradeservicios.status = 1 when a tick starts and status = 0 when it terminates (dependency note 2). Impresion terminates only after the whole work list has been walked. Every out-of-hours tick and every unhandled exception leaves the service marked as executing until a later fully successful tick or a service stop (the host finalizes during shutdown), so monitoring or control services that read status see a stuck worker. The ActualizaTerminacion result is also ignored (IMP-FR-17).
Recommendation: use one lifecycle wrapper: mark execution, run the work, and finalize in finally, preserving the primary outcome if finalization itself fails.
Code block (csharp)
var marcada = false;
try
{
    await _controlServicio.ActualizaEJecucion(_configuracion.NombreServicio, ct);
    marcada = true;
    return await EjecutarTickAsync(ct);          // returns Succeeded / Skipped / Failed
}
finally
{
    if (marcada)
    {
        var fin = await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, CancellationToken.None);
        if (!fin.Exito) Logging.Error($"|Ejecuta_Procedimientos|No se pudo finalizar la ejecución: {fin.MensajeError}");
    }
}
Benefit: keeps persisted lifecycle state truthful and supports overlap control and monitoring.
IMP-FR-5 - Make folio output idempotent and bound its retries
• Category / severity: MustChange/rewrite - Major
• Evidence: CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L43-L59), missing-printer and missing-template triggers at lines 83-94 (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L83-L94), three-report folios at usp_impresion_obtenerPendientes.sql (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L79-L134), and the unconditional updates in usp_impresion_marcarImpreso.sql (../../CMS/Impresion/SQL/usp_impresion_marcarImpreso.sql#L40-L118)
Code block (csharp)
foreach (ImpresionReporteJob reporte in folioEmpaquetado.Reportes)
{
    ct.ThrowIfCancellationRequested();
    if (!GenerarReporte(reporte, ct))
        return false;               // reports 1..k-1 already produced; the folio stays pending
}
await _recuperacionDatosDao.MarcarImpreso(...);
"All reports or nothing" is enforced only by the final flag. Solicitud_Alta_Usuario and Solicitud_Reposicion_Token have three reports each, and Solicitud_Traspaso_Token has two. If report k fails, reports 1..k-1 have already been produced, the folio stays pending, and the next tick repeats the whole sequence. For a permanent fault (no printer mapping, renamed template, formula mismatch) this repeats forever at every tick, with no attempt counter, back-off, terminal state, or alert (IMP-FR-2). There is also no claim or lease: two service instances (restart overlap, accidental second host) would both read and produce the same folio, and the unconditional UPDATE cannot detect that.
Recommendation: render every report of a folio first (temporary files or streams), produce the final output only when all rendered, then mark. Persist an attempt counter and last error per (Proceso, Folio), skip a folio after N attempts, and alert once. Make the marker a compare-and-set (WHERE <flag> = 0) that reports affected rows (IMP-FR-18).
Benefit: bounds duplicate output and turns an endless silent retry loop into one visible, resolvable incident.
IMP-FR-6 - Store generated PDFs in a restricted, traceable location with retention
• Category / severity: MustChange/rewrite - Major
• Evidence: CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L125-L137)
Code block (csharp)
string directorioReportes = _configuracion.ParametrosSistema[ConstantesServicio.ReportesDirectorio];
...
string timestamp = DateTime.Now.ToString("yyyyMMddHHmmssfff");
string nombreArchivoPdf = nombreReporteSinExtension + "_" + timestamp + ".pdf";
string rutaArchivoPdf = Path.Combine(directorioReportes, nombreArchivoPdf);
documentoReporte.ExportToDisk(ExportFormatType.PortableDocFormat, rutaArchivoPdf);
Verified from source: each PDF is written into the same directory the service loads its templates from; the file name carries neither process nor folio, so a file cannot be correlated to a folio, audited, or purged per folio, and uniqueness depends only on a millisecond local-time stamp; no retention exists (no File.Delete or Directory.Delete anywhere in the solution), so files accumulate without bound, and during a retry loop (IMP-FR-5) they are produced again at every tick; and the timestamp uses the current culture and local time rather than CultureInfo.InvariantCulture/UTC. Inferred, not verifiable here: by template and source-table names the documents are FX vouchers, user token request forms, HotScan alerts, and inbound SWIFT confirmations, so they likely contain financial and personal data; the directory's ACLs are not evidenced, so who can read or write the files must be confirmed.
Recommendation: write to a dedicated archive directory with restricted ACLs using a traceable, collision-proof name, write atomically, and apply a retention rule. If printing is restored (IMP-FR-1), remove the export.
Code block (csharp)
string nombre = $"{proceso}_{folio}_{nombreReporteSinExtension}_{DateTime.UtcNow:yyyyMMddTHHmmssfffZ}_{Guid.NewGuid():N}.pdf";
string temporal = Path.Combine(directorioSalida, nombre + ".tmp");
documento.ExportToDisk(ExportFormatType.PortableDocFormat, temporal);
File.Move(temporal, Path.Combine(directorioSalida, nombre));
Benefit: limits exposure of financial and personal documents and makes every file attributable to one folio.
Rewrite Recommended
IMP-FR-7 - Stop raising an ERROR alert on every out-of-hours tick
• Category / severity: rewrite_recommended - Major
• Evidence: OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L70-L106), the 10-second fallback interval at ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L162-L167), and the shared insert (SP 031 (../../StoredProcedures/MWD_DB/DBMDW/General/MUFG_T24_BTMUM_64/IT24/Procedures/031%20SP%20usp_InsertaAlerta_MUFGLibreria.sql#L58))
Code block (csharp)
if (!_estadoHorarioOperacion.EnHorarioOperacion)
{
    Config.EscribirLog("ERROR|Ejecuta_Procedimientos|El servicio está fuera de horario.");
    await _alertas.InsertarAlerta(new AlertaDTO() { ..., ClaveAviso = "ERROR", ..., Exception = new Exception(...) });
    return false;
}
The code already keeps EstadoHorarioOperacion so it can log only *transitions* of the window at INFO, but the out-of-window branch then writes an ERROR line and inserts a new alertas row on every tick; the shared helper adds two more log lines per alert. With the built-in 10-second fallback that is 360 alert rows and at least 1,080 log entries per hour outside the window; the real interval is in the encrypted external configuration. Being outside the configured window is an expected state, not an error, and a flood of identical ERROR rows hides real alerts and grows the table. A plain-text search for está fuera de horario over files named OperacionServicio.cs returned 52 matches in 29 files across the portfolio (including commented legacy lines and branches that only throw or log), so this is proposed as GEN-10 for confirmation site by site.
Recommendation: treat the out-of-window branch as Skipped: log at INFO on transition only and do not alert, or alert once per transition when an unexpected closed window is a business concern (state is already held in EstadoHorarioOperacion). Finalize the lifecycle state (IMP-FR-4).
Benefit: removes alert and log noise and preserves the signal value of real ERROR alerts.
IMP-FR-8 - Propagate cancellation and bound external calls
• Category / severity: rewrite_recommended - Major
• Evidence: token-less shared calls at RecuperacionDatosDao.cs (../../CMS/Impresion/InfraestructuraImpresion.csproj/Repositorio/RecuperacionDatosDao.cs#L31) and lines 42-47 (../../CMS/Impresion/InfraestructuraImpresion.csproj/Repositorio/RecuperacionDatosDao.cs#L42-L47), OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L64-L68) and line 144 (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L144), shutdown calls at ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L284-L287), disposal at lines 325-329 (../../CMS/Impresion/Impresion/ServiceHost.cs#L325-L329), and synchronous Crystal work at CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L75-L141)
Code block (csharp)
var resultado = await EjecutarStoredProcedureQuery<ImpresionPendienteDto>(ConstantesServicio.uspObtenerPendientes);
The shared APIs expose token overloads for the pending/mark queries, ActualizaEJecucion, ActualizaTerminacion, MarcarPuertoEnUsoAsync, system date, and parameters, but Impresion calls the token-less forms. After OnStop signals cancellation, an in-flight query or lifecycle update cannot be cancelled; the DAO only checks the token before and after the call. GenerarReporte is synchronous and checks the token only between steps; Refresh() and ExportToDisk() have no time budget, so one hung report holds the tick lock and delays shutdown to the 60-second bound, after which OnStop disposes the container, token source, and timer while the tick may still be running. The shutdown calls MarcarPuertoEnUsoAsync and ActualizaTerminacion also ignore the 60-second token.
Recommendation: pass ct to every shared call (EjecutarStoredProcedureQuery<T>(nombreSp, parametros, ct)), give each report an explicit time budget with a cooperative abandon strategy, pass the shutdown token to the shutdown calls, and dispose shared resources only after the running tick has completed.
Benefit: makes stop and failure behavior predictable and prevents a single stuck report from blocking all printing.
IMP-FR-9 - Put the Crystal adapter behind an abstraction and split the orchestrator
• Category / severity: rewrite_recommended - Major
• Evidence: concrete dependency at OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L31) and line 42 (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L42), registration at ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L155-L157), Crystal and System.Drawing references at AplicacionImpresion.csproj (../../CMS/Impresion/AplicacionImpresion/AplicacionImpresion.csproj#L72-L90), and infrastructure usage inside the application layer at CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L1-L15)
Code block (csharp)
private readonly CrystalReporterWorker _crystalReportWorker;
...
_containerDI.Register<CrystalReporterWorker>();
The application layer owns the Crystal engine, file-system paths, local clock, and database credential parsing (ConexionBasedatosFactory), while InfraestructuraImpresion contains only the DAO. OperacionServicio depends on the concrete worker (no interface on develop), so it cannot be unit-tested without the Crystal runtime and printers, and EjecutarProcedimientosAsync (about 110 lines) mixes lifecycle, calendar gating, alerting, grouping, and output. This violates the baseline's abstraction/DI expectation and blocks the test coverage required by IMP-FR-23.
Recommendation: define an IReportOutput (or IReporteImpresion) contract in Aplicacion/Interfaces, move the Crystal implementation to Infraestructura, register it by interface, and split the orchestrator into a schedule guard, a pending-folio provider, and an output executor that can each be tested with fakes. The unmerged origin/CodeReview branch contains an ICrystalReporterWorker that may serve as a starting point.
Benefit: restores the layering the portfolio baseline expects and makes the failure paths in IMP-FR-1 to IMP-FR-5 testable.
IMP-FR-10 - Version, validate, and least-privilege the external .rpt templates
• Category / severity: rewrite_recommended - Major
• Evidence: template names at usp_impresion_obtenerPendientes.sql (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L46-L204), load and connection binding at CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L92-L106), and credential extraction at lines 165-186 (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L165-L186)
Code block (csharp)
string rutaReporte = _configuracion.ParametrosSistema[ConstantesServicio.ReportesDirectorio] + reporte.NombreReporte;
documentoReporte.Load(rutaReporte);
documentoReporte.DataSourceConnections[i].SetConnection(servidor, baseDatos, usuario, clave);
The 12 templates referenced by the SQL (new_fxd_local.rpt, new_fxi_local.rpt, new_fxi.rpt, SolicitudToken.rpt, SolicitudTokenOS.rpt, SolicitudTokenCarta.rpt, SolicitudTokenTraspaso.rpt, SolicitudTokenReposicion.rpt, SolicitudTokenReposicionOS.rpt, new_HotScan.rpt, HotScan_v2.rpt, new_SPEI_IN.rpt) exist neither in this repository nor anywhere in the workspace. They are loaded by name from a database-configured directory and then bound to the service's own SQL login (server, database, user, and password parsed from the TOMI connection string). Layout, embedded SQL commands, formulas, and subreports are therefore unreviewed, unversioned, and untested. Whoever can write to that directory (its ACLs are not evidenced) can alter the printed documents, and because a Crystal template can carry its own SQL commands, such a template would execute them with the bound service credentials; no hash or signature is checked.
Recommendation: keep the templates in source control (or a controlled artifact store) with versions, deploy them read-only, verify a SHA-256 manifest when loading, bind reports to a dedicated read-only SQL login limited to the objects the reports need, and add a smoke test that loads every template the SQL references.
Benefit: makes document content auditable and removes template tampering and over-privileged report access as risks.
IMP-FR-11 - Build paths with Path.Combine and validate configuration before using it
• Category / severity: rewrite_recommended - Minor
• Evidence: CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L83-L94) and lines 125-134 (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L125-L134)
rutaReporte is directory + name, so it works only if the stored directory ends with a separator, while the output path a few lines later uses Path.Combine. The string.IsNullOrWhiteSpace(directorioReportes) guard runs only after the directory was already used and is unreachable for an empty value because the shared parameter loader rejects blank required values (dependency note 5). Both validation failures are raised as exceptions and immediately caught by the same method, using exceptions for flow control.
Recommendation: validate once at the top of the method, build the template path with Path.Combine(directorio, Path.GetFileName(nombre)), check File.Exists, and return a typed failure instead of throwing and catching locally.
Benefit: removes a deployment-value trap and makes configuration errors diagnosable.
IMP-FR-12 - Replace the delimited Formulas string with structured values
• Category / severity: rewrite_recommended - Minor
• Evidence: parser at CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L109-L117); producer at usp_impresion_obtenerPendientes.sql (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L46-L58) (same delimited shape in all nine branches)
Code block (csharp)
string[] formulasReporte = reporte.Formulas.Split('|');
for (int i = 0; i < formulasReporte.Length; i++)
{
    string[] valores = formulasReporte[i].Split('=');
    documentoReporte.DataDefinition.FormulaFields[valores[0]].Text = valores[1];
}
SQL concatenates Folios=<id>|FoliosF=<id> and the C# splits on | and = without a limit, a bounds check, or validation. A pair without =, a value that contains = or |, or a formula name absent from the template either truncates the value or throws, and the report then fails on every tick (IMP-FR-5). The values are written into Crystal formula text, which Crystal evaluates as formula code; today they are database-generated keys, but the protocol offers no escaping.
Recommendation: have the procedure return structured columns or build the name/value dictionary in code from (Proceso, Folio), split with Split(new[] { '=' }, 2) where a string must be parsed, validate each pair with a descriptive error, and prefer Crystal parameter fields over formula text.
Benefit: eliminates a fragile parsing contract and closes a potential injection surface should the values ever stop being generated keys.
IMP-FR-13 - Move from obsolete MUFG_T24 logging shims to the supported logger with folio correlation
• Category / severity: rewrite_recommended - Minor
• Evidence: 27 call sites (the first site in each file is linked): 5 in OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L79), 12 in CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L57), and 10 in ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L76); the shims are marked [Obsolete] (dependency note 7)
Code block (csharp)
Config.EscribirLog($"ERROR|Ejecuta_Procedimientos|Se presenta exception: {ex.ToString()}");
Logging.Write("INFO|Imprime_Reporte|Iniciando la impresión del reporte:" + reporte.NombreReporte + ", formulas:" + reporte.Formulas + ".");
All logging goes through compatibility shims that the shared library itself marks obsolete, with pipe-delimited messages built by string concatenation. Per-report lines carry the folio only indirectly through the formulas string, and the error lines (ERROR|Imprime_Reporte, ERROR|Imprime_Operaciones) carry no folio or process at all, so one folio's trail cannot be followed reliably.
Recommendation: migrate to MUFG_T24.Infraestructura.Logging.Logging.Info/Warn/Error and wrap each folio in Logging.BeginOperacionScope("Impresion", folio).
Benefit: removes use of deprecated APIs and makes a folio's full processing trail searchable.
Nice to Rewrite if Time Permits
IMP-FR-14 - Harden the operating-window and parameter contract
• Category / severity: NiceToRewrite_ifTimePermits - Minor
• Evidence: OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L70-L74), lines 112-121 (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L112-L121), and ConstantesServicio.cs (../../CMS/Impresion/AplicacionImpresion/Enums/ConstantesServicio.cs#L10-L12)
Code block (csharp)
if (timeHoraSistema >= TimeSpan.Parse(_configuracion.ParametrosSistema[ConstantesGenerales.HoraOPInicio])
    && timeHoraSistema <= TimeSpan.Parse(_configuracion.ParametrosSistema[ConstantesGenerales.HoraOPFin]))
...
public const string ParametrosSistema = "'" + ReportesDirectorio +
    "','HORA_LECTURA_INICIO','HORA_LECTURA_FIN','HORA_OP_INICIO','HORA_OP_FIN'";
(a) The window values are parsed with culture-sensitive TimeSpan.Parse on every tick, without validating start before end; a window that crosses midnight can never be satisfied. (b) HORA_LECTURA_INICIO and HORA_LECTURA_FIN are requested but never used; because the shared loader treats every listed key as required, their absence would stop every tick. (c) Folios are ordered with ThenBy(g => g.Key.Folio) as text, mirrored by the SQL ORDER BY, so "100" sorts before "99". (d) FolioEsString is mapped into the DTO and job model but never read.
Recommendation: parse once with TryParseExact(..., @"hh\:mm", CultureInfo.InvariantCulture, ...) and validate at startup or on first use with a descriptive error; drop the two unused keys; use a numeric ordering key (or document text ordering); remove or use FolioEsString.
Benefit: clearer failures and fewer unused configuration dependencies.
4.2 InfraestructuraImpresion (Tier 1)
MustChange / Rewrite
IMP-FR-15 - Fail ObtieneFoliosOperaciones when the query fails instead of returning an empty queue
• Category / severity: MustChange/rewrite - Blocker
• Evidence: RecuperacionDatosDao.cs (../../CMS/Impresion/InfraestructuraImpresion.csproj/Repositorio/RecuperacionDatosDao.cs#L28-L34); shared behavior at RepositorioMaestro.cs (../../General/MUFGT24_BTMUM_64/MUFG_T24/Infraestructura/Repositorios/DBs/RepositorioMaestro.cs#L385-L409)
Code block (csharp)
var resultado = await EjecutarStoredProcedureQuery<ImpresionPendienteDto>(ConstantesServicio.uspObtenerPendientes);
ct.ThrowIfCancellationRequested();
return resultado.Data;
For a SqlException (timeout, deadlock, permission change, connection loss, or the procedure's own THROW;), the shared helper returns Exito=false with an empty Data list, neither throwing nor logging (dependency note 1). The DAO returns that empty list, so the orchestrator sees "no pending folios", finalizes the tick, and returns true: a complete printing outage is indistinguishable from a quiet day. The only trace may be the procedure's own auditoria.errorlog row, and that exists only when the error originates inside the procedure.
Recommendation: inspect the result, pass the token, and throw a descriptive exception. Do not use validarAfectacion here, because an empty queue is a valid result.
Code block (csharp)
var resultado = await EjecutarStoredProcedureQuery<ImpresionPendienteDto>(
    ConstantesServicio.uspObtenerPendientes, null, ct);
if (!resultado.Exito)
    throw new InvalidOperationException(
        "No se pudieron obtener los folios pendientes de impresión: " + resultado.MensajeError,
        resultado.Exception);
return resultado.Data;
Benefit: makes a database outage a visible, alertable failure instead of silent inactivity.
Rewrite Recommended
No additional infrastructure-only pattern was found beyond the cross-project items IMP-FR-8 (cancellation), IMP-FR-18 (marker contract), and IMP-FR-22 (absolute reference).
Nice to Rewrite if Time Permits
No infrastructure-only nice-to-have finding met the evidence threshold.
4.3 Impresion Windows Service (Tier 2)
MustChange / Rewrite
IMP-FR-16 - Fail fast (or retry) when startup parameterization fails
• Category / severity: MustChange/rewrite - Major
• Evidence: dead fault branch and redundant timer start at ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L68-L85), catch-all at lines 187-208 (../../CMS/Impresion/Impresion/ServiceHost.cs#L187-L208), timer enabled by its initializer at lines 162-167 (../../CMS/Impresion/Impresion/ServiceHost.cs#L162-L167), and the event-log fallback at lines 87-107 (../../CMS/Impresion/Impresion/ServiceHost.cs#L87-L107)
Code block (csharp)
_startupTask = ParametrizacionesAsync(_cts.Token);
_startupTask.ContinueWith(t =>
{
    if (t.IsFaulted) { ...; this.ExitCode = 1; this.Stop(); return; }   // effectively unreachable
    if (_timerServicio.Enabled) _timerServicio.Start();
}, TaskScheduler.Default);
...
catch (Exception ex)
{
    Config.EscribirLog($"ERROR|Parametrizaciones|{ex}");                  // swallows every failure
}
The host was clearly designed to stop with exit code 1 when startup parameterization faults, but ParametrizacionesAsync catches every exception itself, so _startupTask practically never faults (only if the catch handler's own logging throws) and the branch is effectively dead. Failures are logged and ignored: if the display-name lookup yields null (the non-validated ObtenerProcesoActualAsync tolerates it) or the port registration returns false or throws, the service keeps running unregistered. Parameterization is documented as "una sola vez" and is never retried, so a database or WMI hiccup at boot (for example, the service starting before SQL Server) leaves the service permanently unregistered until restart. The timer is also already running when TimerEnabled is true: Enabled = ... in the object initializer starts it, so the Start() call in the continuation is redundant and ticks are gated only by awaiting that task, which does not fault in practice. The OnStart fallback ends with an empty catch, so if the event-log write fails before file logging is initialized, the original startup error disappears.
Hardening note (GEN-8 check): OperacionServicio dereferences Pid with .Value (line 101 (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L101)) and (int) (line 161 (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L161)) inside alert construction. This is safe today because the shared startup assigns Pid first, but the alert path should use GetValueOrDefault() as the shared library does so it can never mask the original failure.
Recommendation: use the validated shared API, let failures propagate, create the timer disabled, and start it only after success; add bounded retry with back-off if boot-order races are expected.
Code block (csharp)
private async Task ParametrizacionesAsync(CancellationToken ct)
{
    ct.ThrowIfCancellationRequested();
    var parametrizacion = _containerDI.GetInstance<IParametrizacion>();
    _configuracionBase = await parametrizacion.ObtenerProcesoActualValidadoAsync(_configuracionBase, ct);
 
    var control = _containerDI.GetInstance<IControlServicio>();
    if (!await control.MarcarPuertoEnUsoAsync(_configuracionBase.Pid.Value, _configuracionBase.NombreProceso,
            _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio, ct))
        throw new InvalidOperationException("No se pudo registrar el servicio como en uso.");
}
Benefit: the service either runs fully registered or stops loudly, so monitoring never sees a half-started worker.
IMP-FR-17 - Check lifecycle persistence results at startup and shutdown (GEN-9)
• Category / severity: MustChange/rewrite - Major
• Evidence: startup call at ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L199), shutdown calls and unconditional success log at lines 284-289 (../../CMS/Impresion/Impresion/ServiceHost.cs#L284-L289), and the per-tick termination call at OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L144)
Code block (csharp)
await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracionBase.NombreServicio).ConfigureAwait(false);
await _controlServicio.ActualizaTerminacion(_configuracionBase.NombreServicio);
Config.EscribirLog("INFO|OnStop|Terminación exitosa de bases de datos.");
MarcarPuertoEnUsoAsync returns false for a provider-reported failure and ActualizaTerminacion returns Exito=false after logging (dependency notes 2 and 3). The host awaits and discards every result, then logs that termination succeeded. The service can therefore run without its active PID/port registered, or stop while the database still marks it in use, with a success message in the log.
Recommendation: require true/Exito at each site: throw at startup (see IMP-FR-16), and at shutdown log an ERROR (and insert an alert where appropriate) when either update fails instead of printing the success message.
Benefit: keeps persisted service state and operational monitoring consistent with the real process lifecycle.
Rewrite Recommended
No additional host-only pattern was found beyond IMP-FR-8 (disposal while a tick may still run) and IMP-FR-22 (absolute references).
Nice to Rewrite if Time Permits
No host-only nice-to-have finding met the evidence threshold; the template residue in Program.cs and the designer is part of IMP-FR-27.
4.4 SQL scripts (database contract)
MustChange / Rewrite
IMP-FR-18 - Make usp_impresion_marcarImpreso report and guard the rows it updates
• Category / severity: MustChange/rewrite - Major
• Evidence: usp_impresion_marcarImpreso.sql (../../CMS/Impresion/SQL/usp_impresion_marcarImpreso.sql#L38-L122) and the caller at RecuperacionDatosDao.cs (../../CMS/Impresion/InfraestructuraImpresion.csproj/Repositorio/RecuperacionDatosDao.cs#L39-L49)
Code block (sql)
IF @Proceso = 'FXD_Local'
BEGIN
    UPDATE tb_tombin_nf_fxd_local
    SET nf_impresion_voucher = 1
    WHERE nf_folio = @Folio;
    COMMIT TRANSACTION;
    RETURN;
END
Each of the nine branches updates by key and returns, with no result set and no @@ROWCOUNT. A folio that matches no row is reported as success by the shared repository (Exito defaults to true when no error message exists). If the key never matches (type mismatch or wrong key mapping), the folio is never marked and stays pending forever; if another worker already marked it, the duplicate goes undetected. The UPDATE carries no compare-and-set predicate (AND <flag> = 0), so concurrent duplicate processing cannot be detected, and HotScan updates the view vw_nf_scan directly (updatability is not evidenced in this repository). The shared repository already supports the needed contract through the FilasAfectadas column and validarAfectacion.
Recommendation: guard each update, return the affected count before COMMIT, and let the DAO require it.
Code block (sql)
UPDATE tb_tombin_nf_fxd_local
SET nf_impresion_voucher = 1
WHERE nf_folio = @Folio
  AND nf_impresion_voucher = 0;
SELECT @@ROWCOUNT AS FilasAfectadas;
COMMIT TRANSACTION;
RETURN;
Code block (csharp)
var resultado = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
    ConstantesServicio.uspMarcarImpreso,
    cmd => { /* @Proceso, @Folio */ },
    ct,
    validarAfectacion: true);
Benefit: a zero-row update becomes an explicit failure, making duplicates and stuck folios detectable.
Rewrite Recommended
IMP-FR-19 - Bound and align the pending-selection rules
• Category / severity: rewrite_recommended - Major (requires business confirmation)
• Evidence: usp_impresion_obtenerPendientes.sql (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L46-L204), especially the unfiltered branch at lines 193-204 (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L193-L204) and the comma join at lines 180-188 (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L180-L188)
Code block (sql)
FROM tb_tombin_nf_fxi
CROSS APPLY (VALUES (1, 'new_fxi.rpt')) AS Reportes(OrdenReporte, NombreReporte)
WHERE nf_impresion_voucher = 0
None of the nine branches bounds the work list by age, count, or attempts; each returns every row whose flag is 0, and the application processes the entire list sequentially in one tick. FXI_Operaciones_Especiales has no status predicate, unlike FXD_Local and FXI_Local, which both require nf_estatus IN ('PEND','CONF'). If historical rows with the flag at 0 exist (or the flag is ever bulk-reset), the first run or a recovery floods the output. For SWIFT_IN the comma join between tb_srv_swf_plan_trabajo_maestro and tb_srv_swf_trabajo would duplicate rows if several job rows share a folio, and the application does not de-duplicate reports per folio (OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L112-L131)), so the same report could be produced twice.
Recommendation: confirm the intended status rule for FXI_Operaciones_Especiales, add an age window where the schema allows it, and bound the list by folio, not by row (select the first N distinct (Proceso, Folio) pairs, then join their reports) so a folio's reports are never split across ticks; guarantee one row per report in the SWIFT_IN branch (explicit join with EXISTS/GROUP BY) and add a Distinct by report name when grouping.
Benefit: prevents backlog floods and duplicate reports and makes each tick's work predictable.
IMP-FR-20 - Remove the stale duplicate script and put the SQL under governed deployment
• Category / severity: rewrite_recommended - Minor
• Evidence: Scripts.sql (../../CMS/Impresion/SQL/Scripts.sql#L1-L8), lines 208-211 (../../CMS/Impresion/SQL/Scripts.sql#L208-L211), and lines 293-300 (../../CMS/Impresion/SQL/Scripts.sql#L293-L300) versus ConstantesServicio.cs (../../CMS/Impresion/AplicacionImpresion/Enums/ConstantesServicio.cs#L14-L16)
Scripts.sql creates usp_impresion_obtener_pendientes and usp_impresion_marcar_impreso in [TOMI], but the application calls usp_impresion_obtenerPendientes and usp_impresion_marcarImpreso, which the other two scripts create in [InterfacesT24] (the shared library documents that NombreBasedatos.TOMI is the InterfacesT24 database). The stale marker procedure has no transaction, neither stale procedure writes to the audit log, and both rethrow with RAISERROR(@ErrorMessage, ...), which loses the original error number and line; both use drop-and-recreate, which discards permissions. Nothing references the stale names, so a wrong deployment would silently create dead objects. The two active procedures are also absent from the portfolio's database project (no usp_impresion* file exists under StoredProcedures/MWD_DB (../../StoredProcedures/MWD_DB)), and no deployment instructions exist.
Recommendation: delete Scripts.sql (the unmerged origin/CodeReview branch already does), add the two active procedures to the governed database project or release package with permissions, and document the deployment order in the README.
Benefit: removes a deployment trap and puts the database contract under the same review and release control as the code.
Nice to Rewrite if Time Permits
IMP-FR-21 - Tidy the pending-query shape
• Category / severity: NiceToRewrite_ifTimePermits - Minor
• Evidence: usp_impresion_obtenerPendientes.sql (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L180-L188), lines 206-210 (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L206-L210), and lines 212-216 (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L212-L216)
Code block (sql)
OUTER APPLY (
    SELECT TOP (1) Impresoras.des_impresora
    FROM tb_srv_reporte_impresora Impresoras
    WHERE UPPER(Impresoras.des_reporte) = UPPER(Pendientes.NombreReporte)
) AS Impresora
The printer lookup uses TOP (1) without ORDER BY, so the chosen printer is non-deterministic if a report has more than one mapping; UPPER() on both sides typically prevents an index seek on des_reporte; SWIFT_IN uses an old-style comma join with an unqualified cve_estatus; and Folio is a text ordering key (see IMP-FR-14).
Recommendation: enforce uniqueness on des_reporte or add a deterministic ORDER BY, rely on a case-insensitive collation instead of UPPER(), qualify every column, use explicit JOIN syntax, and order by a numeric folio key where folios are numeric.
Benefit: deterministic printer routing and a query that is easier to review and tune.
4.5 Solution-level build, packaging, and repository (cross-project)
MustChange / Rewrite
IMP-FR-22 - Replace the four absolute D:\ServiceCMS references with versioned dependencies (GEN-1)
• Category / severity: MustChange/rewrite - Blocker
• Evidence: host project (../../CMS/Impresion/Impresion/Impresion.csproj#L81-L88), application project (../../CMS/Impresion/AplicacionImpresion/AplicacionImpresion.csproj#L84-L87), and infrastructure project (../../CMS/Impresion/InfraestructuraImpresion.csproj/InfraestructuraImpresion.csproj#L72-L75)
Code block (xml)
<HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
<HintPath>D:\ServiceCMS\SimpleInjector.dll</HintPath>
All three projects require an unversioned MUFG_T24.dll at a machine-specific drive path, and the host additionally requires SimpleInjector.dll there; SpecificVersion=False hides which build compiled the solution. There is no nuget.config and no PackageReference/packages.config. The governing checklist classifies direct DLL references as Blocker and no approved exception is documented. This convention is shared across the portfolio (GEN-1).
Recommendation: publish MUFG_T24 (and SimpleInjector) as versioned internal packages, pin the same approved version in all three projects, and capture the feed and restore step in CI.
Benefit: reproducible builds and an auditable upgrade boundary for the shared library.
IMP-FR-23 - Add automated tests for the orchestration, DAO contract, and host lifecycle (GEN-2)
• Category / severity: MustChange/rewrite - Major
• Evidence: Impresion.slnx (../../CMS/Impresion/Impresion.slnx#L1-L19) lists only the three production projects; a scan of all C# and project files found 0 matches for TestClass, TestMethod, [Fact], [Test], NUnit, xUnit, MSTest, or Moq.
The business-critical behavior - window gating, grouping and ordering, folio failure handling, lifecycle finalization, DAO result handling, formula and path handling - has no automated coverage. Most of the Blocker and Major behavior defects above (IMP-FR-1 to IMP-FR-5 and IMP-FR-15 to IMP-FR-17) are of a kind that a few focused unit tests would have caught. The origin/CodeReview branch adds three test classes, but they are not on develop.
Recommendation: add an MSTest/Moq project (after IMP-FR-9 creates the seams) with at least these first cases: out-of-hours tick is Skipped and finalizes state; a failing folio makes the tick Failed and raises one alert; MarcarImpreso returning false fails the folio; DAO throws on Exito=false; a three-report folio is not marked when report 2 fails; ct cancellation propagates and finalizes; startup failure stops the service. Add a SQL contract test that deploys both procedures and asserts the affected-row result and the unsupported-process error.
Benefit: gives the highest-risk behavior a regression safety net.
Rewrite Recommended
IMP-FR-24 - Govern the vendored Crystal Reports binaries and the GAC-dependent build step
• Category / severity: rewrite_recommended - Major
• Evidence: tracked binaries under lib/CrystalReports (../../CMS/Impresion/lib/CrystalReports), references at AplicacionImpresion.csproj (../../CMS/Impresion/AplicacionImpresion/AplicacionImpresion.csproj#L72-L83), the post-build copy at Directory.Build.targets (../../CMS/Impresion/Directory.Build.targets#L1-L18), and redirects at App.config (../../CMS/Impresion/Impresion/App.config#L8-L79)
Code block (text)
CrystalDecisions.CrystalReports.Engine.dll  380,928 B  asm 13.0.3500.0  file 13.0.22.2668  SHA256 D8C5A963927B852671EED6225DB0604CFD0406BF0AE6C2173EC3F532B6ADD9F1
CrystalDecisions.ReportSource.dll            86,016 B  asm 13.0.3500.0  file 13.0.22.2668  SHA256 DBFD07141275BF845A461B7633B119D1ADB3206793C4A37B3CCD410DF6E78975
CrystalDecisions.Shared.dll                 884,736 B  asm 13.0.3500.0  file 13.0.22.2668  SHA256 7DF3D7298A9DEE690CBFF2510592879BE74F87AB2C27735E6880B945B7416E0E
Three SAP assemblies (publisher SAP SE, not Authenticode-signed) are committed to the repository (about 1.35 MB). The checklist lists committed binaries and direct DLL references as Blocker; severity is held at Major because the references are repository-relative, hash-verifiable, and reproducible, and no approved package feed for these assemblies is evidenced - a documented waiver would be reasonable until one exists. Remaining gaps: the project files declare Version=13.0.4000.0 while the vendored assemblies are 13.0.3500.0 (SpecificVersion=False hides the mismatch, and the App.config redirects 0.0.0.0-13.0.4000.0 to 13.0.3500.0 for 18 assemblies although only three are vendored); the 12 ReportAppServer.* assemblies are copied from the build machine's GAC and silently skipped when absent (Condition="'@(CrystalReportAppServerFromGac)' != ''"), so a build machine without the Crystal runtime produces an output that looks complete and fails only when the first report runs on a host that also lacks those assemblies; the native Crystal runtime that the managed wrappers require is neither vendored nor documented; and SAP redistribution terms for the committed binaries are not evidenced.
Recommendation: publish the wrappers through the approved feed with a pinned version, correct the declared version, make the post-build step fail (or warn loudly) when the required runtime assemblies are absent, remove redirects for assemblies that are not used, and document the native runtime prerequisite and licensing in the README.
Benefit: reproducible, license-reviewed deployments that fail at build time instead of at the first print.
IMP-FR-25 - Align platform targets across Debug and Release
• Category / severity: rewrite_recommended - Major
• Evidence: Impresion.csproj (../../CMS/Impresion/Impresion/Impresion.csproj#L18-L19) and lines 28-29 (../../CMS/Impresion/Impresion/Impresion.csproj#L28-L29), AplicacionImpresion.csproj (../../CMS/Impresion/AplicacionImpresion/AplicacionImpresion.csproj#L16-L33), InfraestructuraImpresion.csproj (../../CMS/Impresion/InfraestructuraImpresion.csproj/InfraestructuraImpresion.csproj#L16-L33), and Impresion.slnx (../../CMS/Impresion/Impresion.slnx#L1-L7)
Code block (xml)
<!-- identical pattern in Impresion.csproj, AplicacionImpresion.csproj and InfraestructuraImpresion.csproj -->
<PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">   ... <PlatformTarget>x64</PlatformTarget>
<PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' "> ... <PlatformTarget>x86</PlatformTarget>
In all three projects Debug|AnyCPU compiles x64 while Release|AnyCPU compiles x86; the solution also defines explicit x86/x64 configurations, and the host's four explicit configurations all set Prefer32Bit=true, which has no effect on a non-AnyCPU target. The Crystal managed wrappers need a native runtime of the same bitness as the process, so developers who debug the x64 build validate a different process model from the x86 build that Release produces. Which bitness production uses is not documented.
Recommendation: pick one bitness (the one of the installed Crystal runtime), set it once (for example in Directory.Build.props) for every configuration, delete the unused platform configurations, and document it with the runtime prerequisite.
Benefit: the artifact that is tested is the artifact that ships.
IMP-FR-26 - Repository hygiene: README, CI, and analyzer suppression
• Category / severity: rewrite_recommended - Minor
• Evidence: README.md (../../CMS/Impresion/README.md#L1-L93), .editorconfig (../../CMS/Impresion/.editorconfig#L5-L11); no .gitlab-ci.yml or other pipeline file exists.
Code block (ini)
dotnet_diagnostic.CS4014.severity = silent   # tarea no esperada
dotnet_diagnostic.CA2007.severity = silent   # ConfigureAwait
dotnet_analyzer_diagnostic.severity = silent
The README is the unmodified GitLab template (its sample commands reference gitlab.mufg.mx while the local clone's origin, per git remote -v, is an IP address): it describes neither the service, nor its configuration, database deployment, Crystal runtime and bitness, installation, nor operations. No CI pipeline builds or tests the solution. .editorconfig turns every analyzer to silent and explicitly silences CS4014 (call to an async method that is not awaited), which removes the build's ability to flag a forgotten await; the checklist requires analyzer rules not to be suppressed without justification.
Recommendation: replace the README with a service runbook (purpose, parameters, templates, printers, SQL deployment, runtime prerequisites, service install, troubleshooting), add a build-and-test pipeline, and restore analyzer severities (keeping suppressions only where justified in a comment).
Benefit: new maintainers can run and support the service, and regressions are caught at build time.
Nice to Rewrite if Time Permits
IMP-FR-27 - Remove template residue and naming drift
• Category / severity: NiceToRewrite_ifTimePermits - Minor
• Evidence: Impresion/AssemblyInfo.cs (../../CMS/Impresion/Impresion/Properties/AssemblyInfo.cs#L8-L12), AplicacionImpresion/AssemblyInfo.cs (../../CMS/Impresion/AplicacionImpresion/Properties/AssemblyInfo.cs#L8-L12), InfraestructuraImpresion/AssemblyInfo.cs (../../CMS/Impresion/InfraestructuraImpresion.csproj/Properties/AssemblyInfo.cs#L8-L12), ModeloEjemplo.cs (../../CMS/Impresion/AplicacionImpresion/Modelo/ModeloEjemplo.cs#L4-L9), ImpresionReporteJob.cs (../../CMS/Impresion/AplicacionImpresion/Modelo/ImpresionReporteJob.cs#L1), Program.cs (../../CMS/Impresion/Impresion/Program.cs#L3), ServiceHost.Designer.cs (../../CMS/Impresion/Impresion/ServiceHost.Designer.cs#L32), and ConstantesServicio.cs (../../CMS/Impresion/AplicacionImpresion/Enums/ConstantesServicio.cs#L15-L16)
Project scaffolding was never customized: assembly titles and products are Plantilla, AplicacionPlantilla, and InfraestructuraPlantilla; the root namespaces are AplicacionPlantilla and InfraestructuraPlantilla; ImpresionFolioJob lives in ModeloEjemplo.cs while its sibling ImpresionReporteJob is in the AplicacionPlantilla.Modelo namespace and not Aplicacion.Modelo; the host's namespace and StartupObject are ControladorServicios (copied from another solution); the service name is Service1, which ServiceBase auto-logging uses as the event-log source name by default; the infrastructure project folder is named InfraestructuraImpresion.csproj; constants use camelCase (uspObtenerPendientes); and async DAO members lack the Async suffix.
Recommendation: rename namespaces, file, assembly metadata, service name, and the project folder together in one mechanical change, and align names with the PascalCase and Async conventions in the checklist.
Benefit: consistent identity for diagnostics, installers, and event-log entries.
5. Architecture-Fit Note
The workload is a scheduled polling batch: each tick selects a bounded set of pending documents, renders them through a native, Windows-only engine, and updates one flag per source. Applying the standing guidance in _ORCHESTRATION.md (_ORCHESTRATION.md) section 6, the lowest-risk sequence is to modernize in place after correcting the MustChange findings: make the printed/not-printed outcome truthful (IMP-FR-1 to IMP-FR-3, IMP-FR-15), add per-folio attempt state and alerting, put the renderer behind an interface with tests, and put the SQL and templates under version control. The Crystal Reports for Visual Studio runtime constrains the host to .NET Framework with a matching native runtime and a Windows service identity that can reach the configured printers, which makes serverless or container hosting a poor fit without further evaluation; an event-driven design (a print request queue instead of polling) or a replacement reporting engine could improve decoupling later, but that decision needs inputs this audit does not have: print volumes, the maximum acceptable delay between authorization and output, printer topology and the service account's access to it, whether PDF delivery is the future target, whether more than one instance is required for availability, and the budget for re-authoring 12 templates. A full microservices rewrite is not justified by the current evidence.
6. JIRA-Ready Backlog
ID	Title	Category	Project	File(s)	Priority
IMP-FR-1	Stop marking folios printed when printing is disabled; make output mode explicit	MustChange/rewrite	AplicacionImpresion	CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L119-L141)	Blocker
IMP-FR-2	Report failed ticks and alert when any folio fails	MustChange/rewrite	AplicacionImpresion	OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L134-L146)	Blocker
IMP-FR-3	Treat a failed MarcarImpreso as a failed folio	MustChange/rewrite	AplicacionImpresion	CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L55-L59)	Blocker
IMP-FR-4	Finalize execution state on every non-cancelled path	MustChange/rewrite	AplicacionImpresion	OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L64)	Major
IMP-FR-5	Make folio output idempotent and bound retries	MustChange/rewrite	AplicacionImpresion / SQL	CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L43-L59)	Major
IMP-FR-6	Store generated PDFs safely with traceable names and retention	MustChange/rewrite	AplicacionImpresion	CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L125-L137)	Major
IMP-FR-7	Stop alerting on every out-of-hours tick	rewrite_recommended	AplicacionImpresion	OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L70-L106)	Major
IMP-FR-8	Propagate cancellation and bound external calls	rewrite_recommended	AplicacionImpresion / Infraestructura / host	RecuperacionDatosDao.cs (../../CMS/Impresion/InfraestructuraImpresion.csproj/Repositorio/RecuperacionDatosDao.cs#L31)	Major
IMP-FR-9	Put the Crystal adapter behind an interface and split the orchestrator	rewrite_recommended	AplicacionImpresion	OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L31)	Major
IMP-FR-10	Version, validate, and least-privilege the .rpt templates	rewrite_recommended	AplicacionImpresion	CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L92-L106)	Major
IMP-FR-11	Build paths with Path.Combine and validate configuration first	rewrite_recommended	AplicacionImpresion	CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L83-L94)	Minor
IMP-FR-12	Replace the delimited formulas string with structured values	rewrite_recommended	AplicacionImpresion / SQL	CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L109-L117)	Minor
IMP-FR-13	Replace obsolete logging shims and add folio correlation	rewrite_recommended	All projects	CrystalReporterWorker.cs (../../CMS/Impresion/AplicacionImpresion/Servicios/CrystalReporterWorker.cs#L57)	Minor
IMP-FR-14	Harden operating-window and parameter handling	NiceToRewrite_ifTimePermits	AplicacionImpresion	OperacionServicio.cs (../../CMS/Impresion/AplicacionImpresion/OperacionServicio.cs#L70-L74)	Minor
IMP-FR-15	Throw when the pending-folio query fails	MustChange/rewrite	InfraestructuraImpresion	RecuperacionDatosDao.cs (../../CMS/Impresion/InfraestructuraImpresion.csproj/Repositorio/RecuperacionDatosDao.cs#L28-L34)	Blocker
IMP-FR-16	Fail fast or retry when startup parameterization fails	MustChange/rewrite	Impresion host	ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L187-L208)	Major
IMP-FR-17	Check lifecycle persistence results at startup and shutdown	MustChange/rewrite	Impresion host	ServiceHost.cs (../../CMS/Impresion/Impresion/ServiceHost.cs#L284-L289)	Major
IMP-FR-18	Return and require affected-row counts from usp_impresion_marcarImpreso	MustChange/rewrite	SQL	usp_impresion_marcarImpreso.sql (../../CMS/Impresion/SQL/usp_impresion_marcarImpreso.sql#L38-L122)	Major
IMP-FR-19	Bound the pending list by folio and align branch filters	rewrite_recommended	SQL	usp_impresion_obtenerPendientes.sql (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L46-L204)	Major
IMP-FR-20	Delete the stale script and govern SQL deployment	rewrite_recommended	SQL	Scripts.sql (../../CMS/Impresion/SQL/Scripts.sql#L1-L8)	Minor
IMP-FR-21	Tidy the pending-query shape	NiceToRewrite_ifTimePermits	SQL	usp_impresion_obtenerPendientes.sql (../../CMS/Impresion/SQL/usp_impresion_obtenerPendientes.sql#L206-L210)	Minor
IMP-FR-22	Replace absolute D:\ServiceCMS references	MustChange/rewrite	All projects	Impresion.csproj (../../CMS/Impresion/Impresion/Impresion.csproj#L81-L88)	Blocker
IMP-FR-23	Add automated tests	MustChange/rewrite	Solution	Impresion.slnx (../../CMS/Impresion/Impresion.slnx#L1-L19)	Major
IMP-FR-24	Govern vendored Crystal binaries and the GAC-dependent build step	rewrite_recommended	Solution	Directory.Build.targets (../../CMS/Impresion/Directory.Build.targets#L1-L18)	Major
IMP-FR-25	Align platform targets across configurations	rewrite_recommended	All projects	Impresion.csproj (../../CMS/Impresion/Impresion/Impresion.csproj#L18-L29)	Major
IMP-FR-26	Replace boilerplate README, add CI, restore analyzers	rewrite_recommended	Solution	README.md (../../CMS/Impresion/README.md#L1-L93)	Minor
IMP-FR-27	Remove template residue and naming drift	NiceToRewrite_ifTimePermits	All projects	ServiceHost.Designer.cs (../../CMS/Impresion/Impresion/ServiceHost.Designer.cs#L32)	Minor

