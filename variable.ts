valoresFormato[26] = datosCms.IdUsuarioAutorizo;
            valoresFormato[27] = datosCms.MontoOperacion;
            valoresFormato[28] = datosCms.IdPlantilla;
            valoresFormato[29] = datosCms.IdFormatoPlantilla;
            valoresFormato[30] = datosCms.CodigoOperacion;
            valoresFormato[31] = datosCms.TipoOperacion;
            valoresFormato[32] = datosCms.FechaAplicacionAbono;
            valoresFormato[33] = datosCms.FechaCargoFondos;

            string[] valoresPipes = (datosCms.ContenidoFormato ?? string.Empty)
                .Split(new[] { "|" }, StringSplitOptions.None);

            if (valoresPipes.Length - 1 < 18)
                throw new Exception($"El contenidoFormato del folio {folio} es diferente al esperado, debe ser 18 datos y se tienen {valoresPipes.Length - 1}.");

            for (int idx = 0; idx <= 18; idx++)
            {
                string[] llaveValor = valoresPipes[idx].Split(new[] { "=" }, 2, StringSplitOptions.None);

                if (llaveValor.Length <= 1)
                    throw new Exception($"El contenidoFormato del folio {folio} tiene un elemento inválido en la posición {idx}: '{valoresPipes[idx]}'.");

                valoresFormato[idx] = llaveValor[1];
            }

            // Folio Forex.
            if (valoresFormato[3] != null)
                folioForex = Convert.ToString(valoresFormato[3]);

            // Fecha ejecución.
            if (valoresFormato[13] != null)
                fechaEjecucion = Convert.ToString(valoresFormato[13]);

            // Moneda.
            if (valoresFormato[4] != null)
                moneda = _vwT24.ValidaMoneda(Convert.ToString(valoresFormato[4]));

            // Importe.
            importe = datosCms.MontoOperacion;

            if (CompararMontos(valoresFormato[5], valoresFormato[18]) == false ||
                CompararMontos(valoresFormato[5], valoresFormato[27]) == false)
            {
                throw new Exception(
                    $"Hay diferencias entre los montos de la operación: MontoOperacion={valoresFormato[27]}, MontoCapturado={valoresFormato[5]}, MontoEnviado={valoresFormato[18]}.");
            }

            Config.EscribirLog($"INFO|TEF.procesaOperacion|Veamos que trae la fecha{valoresFormato[13]}-----{fechaEjecucion}");

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(fechaEjecucion, CultureInfo.InvariantCulture);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception(
                    $"Se desea realizar una operación al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // Genero el folio único.
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion),
                folio);

            // ¿Fecha futura?
            fechaFutura = string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) > 0;

            Config.EscribirLog("INFO|TEF.procesaOperacion|Validando si la operación está asignada a un Forex.");

            if (!string.IsNullOrWhiteSpace(folioForex))
            {
                Config.EscribirLog($"INFO|TEF.procesaOperacion|Asignado el folio forex: {folioForex} .");
                conForex = true;

                if (decimal.TryParse(folioForex, out _))
                    throw new Exception($"El Folio del Forex no es válido porque es numérico; se espera una referencia alfanumérica/external ref: {folioForex}");

                TefForexStatusTomiDto forexStatus = await _tefDao.ObtenerStatusForexPorExternalRef(folioForex);
                if (forexStatus != null &&
                    string.Equals(forexStatus.StatusOperacion, "CANCELADO", StringComparison.OrdinalIgnoreCase))
                {
                    forexCancelado = true;
                    conEntradaSalida = false;
                    Config.EscribirLog("INFO|TEF.procesaOperacion|FOREX CANCELADO.");
                }
            }

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string subTipo = string.Empty;

                Config.EscribirLog("INFO|TEF.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });

                if (conForex)
                    subTipo = $"FX:{folioForex}";

                bool actualizado = await _tefDao.ActualizarDatosGeneralesPlanTrabajo(new TefActualizarDatosGeneralesPlanTrabajoDto
                {
                    Folio = ConversionHelper.ConvertirLongRequerido(folio, "Folio"),
                    TipoOperacion = tipoOperacion,
                    FolioUnico = folioUnico,
                    FecValor = ConversionHelper.ConvertirDecimalRequerido(fechaEjecucion, "FechaEjecucion"),
                    CveMoneda = moneda,
                    Importe = importe,
                    NumCliente = ConversionHelper.ConvertirNullableInt(valoresFormato[21], "Cdl"),
                    NombreCliente = Convert.ToString(valoresFormato[1]),
                    NumCuenta = Convert.ToString(valoresFormato[22]),
                    SubtipoOperacion = subTipo
                });

                if (!actualizado)
                    throw new Exception($"Error actualizando tb_srv_plan_trabajo_maestro para el folio {folio}.");
                /// <summary>
                /// Ejecuta la operaciÃ³n TerminaProceso.
                /// </summary>

                await _controlServicio.TerminaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });
                /// <summary>
                /// Ejecuta la operaciÃ³n RefrescarAliasProceso.
                /// </summary>
                _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });
            }

            // Lectura del CMS e inserción a TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CMS_A_TOMI", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TEF.procesaOperacion|A insertar datos de CMS a TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });

                procesado = false;
                resultado = await CMS_A_TOMI(folio, valoresFormato);

                if (resultado)
                {
                    Config.EscribirLog("INFO|TEF.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                    procesado = true;
                }
                else
                {
                    // Posiblemente se restauró el servicio.
                    bool existe = await _tefDao.ExisteFormatoTefPorFolio(folio);
                    if (existe)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|TEF.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (procesado)
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    /// <summary>
                    /// Ejecuta la operaciÃ³n RefrescarAliasProceso.
                    /// </summary>
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CMS_A_TOMI con folio: {folio}");
                }
            }

            // Registro de la Entrada/Salida.
            if (string.Equals(_configuracion.AliasProceso, "TOMI_ENTRADAS/SALIDAS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TEF.procesaOperacion|A insertar datos de Entrada/Salida en TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });

                if (conEntradaSalida)
                {
                    procesado = false;
                    resultado = await TOMI_ENTRADA_SALIDA(folio);

                    if (resultado)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|TEF.procesaOperacion|Se insertó correctamente el folio en base de Entrada/Salida.");
                    }
                    else
                    {
                        // Posiblemente se restauró el servicio.
                        bool existe = await _tefDao.ExisteEntradaSalidaPorReferencia(folioUnico);
                        if (existe)
                        {
                            procesado = true;
                            Config.EscribirLog("INFO|TEF.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                        }
                    }
                }
                else
                {
                    procesado = true;
                    Config.EscribirLog("INFO|TEF.procesaOperacion|La operación no debe generar Entrada/Salida en TOMI).");
                }

                if (procesado)
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    /// <summary>
                    /// Ejecuta la operaciÃ³n RefrescarAliasProceso.
                    /// </summary>
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");
                }
            }

            // Actualización del estatus del CMS y TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TEF.procesaOperacion|A actualizar estatus en CMS y TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });

                procesado = true;

                if (conForex && forexCancelado)
                {
                    // Rechazado por cancelación del Forex.
                    await _tefCmsDao.ActualizarEstatusFormato(new TefActualizarEstatusCmsDto
                    {
                        IdFormato = ConversionHelper.ConvertirLongRequerido(folio, "Folio"),
                        Mensaje = "Formato rechazado",
                        Estatus = "RECC",
                        RazonRechazo = "El forex relacionado ha sido cancelado"
                    });

                    await _tefDao.ActualizarEstatusTef(new TefActualizarEstatusTomiDto
                    {
                        NfFolio = ConversionHelper.ConvertirLongRequerido(folio, "Folio"),
                        NfEstatus = "RECC",
                        NfRazon = "El forex relacionado ha sido cancelado",
                        NfUsuarioConfirma = null,
                        NfFechaConfirma = null
                    });

                    Config.EscribirLog($"INFO|TEF.procesaOperacion|Forex Rechazado con Anterioridad: {folio}");

                    // En caso de rechazo termino la operación.
                    Config.EscribirLog("INFO|TEF.procesaOperacion|A cancelar la operación.");
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>

                    await _controlServicio.TerminaProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });

                    await _tefDao.CancelarPlanTrabajoPorForex(new TefCancelarPlanTrabajoPorForexDto
                    {
                        Folio = ConversionHelper.ConvertirLongRequerido(folio, "Folio"),
                        TipoOperacion = tipoOperacion,
                        FecValor = ConversionHelper.ConvertirDecimalRequerido(fechaEjecucion, "FechaEjecucion"),
                        CveMoneda = moneda,
                        Importe = importe,
                        ErrDescripcion = "El forex relacionado ha sido cancelado"
                    });

                    return true;
                }

                // Aceptado.
                if (string.Equals(fechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                {
                    DateTime fechaConfirmacion = DateTime.Now;

                    await _tefCmsDao.ActualizarEstatusFormato(new TefActualizarEstatusCmsDto
                    {
                        IdFormato = ConversionHelper.ConvertirLongRequerido(folio, "Folio"),
                        Mensaje = "Formato aceptado",
                        Estatus = "APFT",
                        RazonRechazo = null
                    });

                    await _tefDao.ActualizarEstatusTef(new TefActualizarEstatusTomiDto
                    {
                        NfFolio = ConversionHelper.ConvertirLongRequerido(folio, "Folio"),
                        NfEstatus = "SCNP",
                        NfRazon = null,
                        NfUsuarioConfirma = "OPER CMS",
                        NfFechaConfirma = new DateTime(
                            fechaConfirmacion.Year,
                            fechaConfirmacion.Month,
                            fechaConfirmacion.Day,
                            fechaConfirmacion.Hour,
                            fechaConfirmacion.Minute,
                            0,
                            fechaConfirmacion.Kind)
                    });

                    Config.EscribirLog("INFO|TEF.procesaOperacion|Operación fecha valor hoy.");
                }
                else if (string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) < 0)
                {
                    await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                    throw new Exception($"Se desea realizar una operación al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                }
                else
                {
                    DateTime fechaConfirmacion = DateTime.Now;

                    // Fecha futura.
                    await _tefCmsDao.ActualizarEstatusFormato(new TefActualizarEstatusCmsDto
                    {
                        IdFormato = ConversionHelper.ConvertirLongRequerido(folio, "Folio"),
                        Mensaje = "Formato recibido pendiente ejecución",
                        Estatus = "REPE",
                        RazonRechazo = null
                    });

                    await _tefDao.ActualizarEstatusTef(new TefActualizarEstatusTomiDto
                    {
                        NfFolio = ConversionHelper.ConvertirLongRequerido(folio, "Folio"),
                        NfEstatus = "SCNF",
                        NfRazon = null,
                        NfUsuarioConfirma = "OPER CMS",
                        NfFechaConfirma = new DateTime(
                            fechaConfirmacion.Year,
                            fechaConfirmacion.Month,
                            fechaConfirmacion.Day,
                            fechaConfirmacion.Hour,
                            fechaConfirmacion.Minute,
                            0,
                            fechaConfirmacion.Kind)
                    });

                    Config.EscribirLog("INFO|TEF.procesaOperacion|Operación fecha valor fútura.");
                }

                if (procesado)
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    /// <summary>
                    /// Ejecuta la operaciÃ³n RefrescarAliasProceso.
                    /// </summary>
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CAMBIO_ESTATUS con folio: {folio}");
                }
            }

            // Conciliación.
            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                int regsCms;
                int regsTomi;
                int regsEntSal = 0;
                string mensajeError;
                string folioForexTomi;
                string fechaEjecucionTomi;
                string monedaTomi;
                decimal importeTomi;
                string fechaEjecucionEntSal;
                string monedaEntSal;
                decimal importeEntSal;

                Config.EscribirLog("INFO|TEF.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });

                // Registros en CMS.
                regsCms = 1;

                // Registros en TOMI.
                regsTomi = 0;
                folioForexTomi = string.Empty;
                fechaEjecucionTomi = string.Empty;
                monedaTomi = string.Empty;
                importeTomi = 0m;

                TefConciliacionTomiDto datosTomi = await _tefDao.ObtenerConciliacionTefPorFolio(
                    folio,
                    conForex && forexCancelado);

                if (datosTomi != null)
                {
                    regsTomi = 1;
                    folioForexTomi = datosTomi.NfForexRelacionado ?? string.Empty;
                    fechaEjecucionTomi = datosTomi.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                    monedaTomi = _vwT24.ValidaMoneda(datosTomi.T24DebitCurrency);
                    importeTomi = datosTomi.T24DebitAmount ?? 0m;
                }

                // Entrada/Salida.
                fechaEjecucionEntSal = string.Empty;
                monedaEntSal = string.Empty;
                importeEntSal = 0m;

                if (conEntradaSalida)
                {
                    TefConciliacionEntradaSalidaTomiDto entradaSalida =
                        await _tefDao.ObtenerConciliacionEntradaSalidaPorReferencia(folioUnico);

                    if (entradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = entradaSalida.EntFechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                        monedaEntSal = _vwT24.ValidaMoneda(entradaSalida.EntMonedaCuenta);
                        importeEntSal = entradaSalida.EntImporte ?? 0m;
                    }
                }

                // Validación.
                mensajeError = string.Empty;

                if (regsCms == 1 && regsTomi == 1 && ((conEntradaSalida && regsEntSal == 1) || !conEntradaSalida))
                {
                    if (!string.Equals(folioForex, folioForexTomi, StringComparison.Ordinal))
                        mensajeError += Environment.NewLine + $" - Diferencia en la autorización de FOREX. CMS='{folioForex}', TOMI='{folioForexTomi}'.";

                    if (!string.Equals(fechaEjecucion, fechaEjecucionTomi, StringComparison.Ordinal))
                        mensajeError += Environment.NewLine + $" - Diferencia en la fecha de ejecución. CMS='{fechaEjecucion}', TOMI='{fechaEjecucionTomi}'.";

                    if (!string.Equals(moneda, monedaTomi, StringComparison.Ordinal))
                        mensajeError += Environment.NewLine + $" - Diferencia en la monedas. CMS='{moneda}', TOMI='{monedaTomi}'.";

                    if (importe != importeTomi)
                        mensajeError += Environment.NewLine + $" - Diferencia en el importe. CMS='{importe}', TOMI='{importeTomi}'.";

                    if (conEntradaSalida)
                    {
                        if (!string.Equals(fechaEjecucion, fechaEjecucionEntSal, StringComparison.Ordinal))
                            mensajeError += Environment.NewLine + $" - Diferencia en la fecha de ejecución de Entrada/Salida. CMS='{fechaEjecucion}', TOMI='{fechaEjecucionEntSal}'.";

                        if (!string.Equals(moneda, monedaEntSal, StringComparison.Ordinal))
                            mensajeError += Environment.NewLine + $" - Diferencia en la monedas de Entrada/Salida. CMS='{moneda}', TOMI='{monedaEntSal}'.";

                        if (importe != importeEntSal)
                            mensajeError += Environment.NewLine + $" - Diferencia en el importe de Entrada/Salida. CMS='{importe}', Entrada/Salida='{importeEntSal}'.";
                    }

                    procesado = true;
                }
                else
                {
                    procesado = false;
                    mensajeError += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                }

                if (!string.IsNullOrWhiteSpace(mensajeError))
                {
                    procesado = false;
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(mensajeError);
                }

                if (procesado)
                {
                    Config.EscribirLog("INFO|TEF.procesaOperacion|La operación está correctamente conciliada.");
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>

                    await _controlServicio.TerminaProceso(new ProcesoDTO

                    {

                        Folio = folio,

                        TipoOperacion = tipoOperacion,

                        Servicio = _configuracion.NombreServicio,

                        NombreProceso = _configuracion.NombreProceso,

                        AliasProceso = _configuracion.AliasProceso,

                        PId = _configuracion.Pid.Value

                    });

                    _configuracion.AliasProceso = string.Empty;
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CONCILIAR con folio: {folio}");
                }
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Cerrando conexión a base de datos del CMS.");
            return true;
        }

        /// <summary>
        /// Inserta registro en la base de datos de TOMI con datos provenientes de CMS.
        /// </summary>
        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato)
        {
            try
            {
                TefInsertarFormatoTomiDto dto = new TefInsertarFormatoTomiDto
                {
                    NfFolioUnico = folioUnico,
                    NfTipo = Convert.ToString(valoresFormato[19]),
                    NfFolio = ConversionHelper.ConvertirLongRequerido(valoresFormato[20], "NF_FOLIO"),
                    T24CustomerCode = Convert.ToString(valoresFormato[21]),
                    T24DebitAcctNo = Convert.ToString(valoresFormato[22]),
                    NfEstatus = "PEND",
                    NfUsuarioCaptura = Convert.ToString(valoresFormato[24]),
                    NfUsuarioAutoriza = Convert.ToString(valoresFormato[26]),
                    NfFechaCaptura = Convert.ToDateTime(valoresFormato[23], CultureInfo.InvariantCulture),
                    NfFechaAutoriza = ConversionHelper.ConvertirNullableDateTime(valoresFormato[25]),
                    T24DebitAmount = ConversionHelper.ConvertirDecimalRequerido(valoresFormato[27], "T24_DEBIT_AMOUNT"),
                    T24ShortName = ConversionHelper.Mid(Convert.ToString(valoresFormato[1]), 1, 35),
                    NfForexRelacionado = Convert.ToString(valoresFormato[3]),
                    T24DebitCurrency = Convert.ToString(valoresFormato[4]),
                    NfBancoBenef = Convert.ToString(valoresFormato[6]),
                    T24BenAcctNo = Convert.ToString(valoresFormato[7]),
                    NfTipoCuenta = Convert.ToString(valoresFormato[8]),
                    T24BenCustName = Convert.ToString(valoresFormato[10]),
                    NfRfcBeneficiario = Convert.ToString(valoresFormato[11]),
                    NfTasaRetencion = Convert.ToString(valoresFormato[12]),

                    NfFechaEjecucion = ConversionHelper.ConvertirIntRequerido(valoresFormato[33], "NF_FECHA_EJECUCION"),

                    T24DebitTheirRef = Convert.ToString(valoresFormato[15]),
                    NfMontoEnviar = ConversionHelper.ConvertirDecimalRequerido(valoresFormato[18], "NF_MONTO_ENVIAR"),
                    IdPlantilla = Convert.ToString(valoresFormato[28]),
                    IdFormatoPlantilla = ConversionHelper.ConvertirNullableLong(valoresFormato[29], "ID_FORMATO_PLANTILLA"),
                    NfCodigoOperacion = Convert.ToString(valoresFormato[30]),
                    NfTipoOperacion = Convert.ToString(valoresFormato[31]),
                    NfFechaAplicacion = ConversionHelper.ConvertirIntRequerido(valoresFormato[32], "NF_FECHA_APLICACION"),
                    NfFechaCargo = ConversionHelper.ConvertirIntRequerido(valoresFormato[33], "NF_FECHA_CARGO")
                };

                bool insertado = await _tefDao.InsertarFormatoTef(dto);

                Config.EscribirLog($"INFO|TEF.CMS_A_TOMI|Salida={(insertado ? 0 : 1)}.");

                return insertado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|TEF.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        /// <summary>
        /// Inserta registro de Entradas/Salidas en TOMI.
        /// </summary>
        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            try
            {
                TefEntradaSalidaOrigenTomiDto datos = await _tefDao.ObtenerEntradaSalidaOrigenPorFolio(folio);
                if (datos is null)
                    return false;

                TefInsertarEntradaSalidaTomiDto dto = new TefInsertarEntradaSalidaTomiDto
                {
                    EntReferencia = datos.NfFolioUnico,
                    EntCliente = ConversionHelper.ConvertirDecimalRequerido(datos.T24CustomerCode, "T24_CUSTOMER_CODE"),
                    EntCdl = ConversionHelper.ConvertirDecimalRequerido(datos.T24CustomerCode, "T24_CUSTOMER_CODE"),
                    EntOrdenante = datos.T24CompanyName,
                    EntOperacion = 218m,
                    EntOperacionDesc = "Salida  (TTS)",
                    EntTipo = "D",
                    EntTipoDesc = "TEF Doméstica",
                    EntViaPago = 60m,
                    EntViaPagoDesc = "TRF ENVIADA MXN TEF",
                    EntImporte = datos.T24DebitAmount ?? 0m,
                    EntSaldo = 0m,
                    EntComentarios = datos.T24DebitTheirRef,
                    EntFechaCaptura = ConversionHelper.TextoToFechaNullable(_configuracion.StrFechaSistema),
                    EntFechaValor = ConversionHelper.TextoToFechaNullable(datos.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture)),
                    EntHora = DateTime.Now.ToLongTimeString(),
                    EntCaptura = "JEAI",
                    EntFad = 0m,
                    EntReferenciaControl = "A",
                    EntReferenciaControlCont = 0
                };

                if (string.IsNullOrEmpty(datos.NfForexRelacionado))
                {
                    dto.EntCuenta = ConversionHelper.ConvertirDecimalRequerido(datos.T24DebitAcctNo, "T24_DEBIT_ACCT_NO");
                    dto.EntNombreCuenta = datos.NfNombreCuenta;
                    dto.EntMonedaCuenta = _vwT24.ValidaMoneda(datos.NfMonedaCuenta);
                }
                else
                {
                    var cuentasFad = await _vwT24.ObtieneDatosCuentasFad();  // catálogo FAD 
                    string cuentaFadEsperada = cuentasFad
                        ?.Find(x => string.Equals(x.Moneda, _vwT24.MonedaDefaultMN, StringComparison.Ordinal))
                        ?.Cuenta;

                    dto.EntCuenta = ConversionHelper.ConvertirDecimalRequerido(cuentaFadEsperada, "T24_FAD_Account");
                    dto.EntNombreCuenta = "BTMM MXN FAD";
                    dto.EntMonedaCuenta = _vwT24.MonedaDefaultMN;
                }

                bool insertado = await _tefDao.InsertarEntradaSalida(dto);

                Config.EscribirLog($"INFO|TEF.TOMI_ENTRADA_SALIDA|Salida={(insertado ? 0 : 1)}.");

                return insertado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|TEF.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}.Trace:{ex}");
            }

            return false;
        }

        /// <summary>
        /// Compara dos montos decimales para determinar si son iguales.
        /// </summary>
        /// <param name="valorA"></param>
        /// <param name="valorB"></param>
        /// <returns></returns>
        private static bool CompararMontos(object valorA, object valorB)
        {
            decimal montoA = ConversionHelper.ConvertirDecimalRequerido(valorA, "MontoA");
            decimal montoB = ConversionHelper.ConvertirDecimalRequerido(valorB, "MontoB");

            return montoA == montoB;
        }

    }
}

using Aplicacion.Interfaces.TEF;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    public class TEFGPH
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ITEFGphDao _tefGphDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "TEF_GPH";
        private string folioUnico = string.Empty;

        public TEFGPH(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ITEFGphDao tefGphDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tefGphDao = tefGphDao;
            _vwT24 = vwT24;
        }

        public async Task<bool> ProcesaOperacion(string folio)
        {
            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });
            if (string.IsNullOrEmpty(_configuracion.AliasProceso))
                return false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                bool procesado = false;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc).");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var datos = await _tefGphDao.ObtenerDatosBasicosPorFolio(folio);
                string fechaEjecucion = string.Empty;

                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                }

                if (string.IsNullOrWhiteSpace(folioUnico))
                {
                    folioUnico = await _vwT24.GeneraFolioUnico(
                        "GPH",
                        "01",
                        tipoOperacion,
                        ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion),
                        folio);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Creamos folio_unico='{folioUnico}'.");
                    await _tefGphDao.ActualizarFolioUnico(folio, folioUnico);
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                await _tefGphDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    _vwT24.MonedaDefaultMN);

                procesado = true;

                if (procesado)
                {
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    return true;
                }

                throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
            }

            return false;
        }
    }
}

using Aplicacion.Interfaces.TEF;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa TEFLocal.
    /// </summary>
    public class TEFLocal
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ITEFLocalDao _tefLocalDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "TEF_Local";
        private string folioUnico = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase TEFLocal.
        /// </summary>

        public TEFLocal(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ITEFLocalDao tefLocalDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tefLocalDao = tefLocalDao;
            _vwT24 = vwT24;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ProcesaOperacion(string folio)
        {
            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });
            if (string.IsNullOrEmpty(_configuracion.AliasProceso))
                return false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc).");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var datos = await _tefLocalDao.ObtenerDatosBasicosPorFolio(folio);
                string fechaEjecucion = string.Empty;

                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                }

                if (string.IsNullOrWhiteSpace(folioUnico))
                {
                    folioUnico = await _vwT24.GeneraFolioUnico(
                        tipoOperacion,
                        ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion),
                        folio);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Creamos folio_unico='{folioUnico}'.");
                    await _tefLocalDao.ActualizarFolioUnico(folio, folioUnico);
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                await _tefLocalDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    _vwT24.MonedaDefaultMN);

                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });
                _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });
                return true;
            }

            return false;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    using Aplicacion.DTOs.TEF_H2H;
    using Aplicacion.Interfaces.TEF_H2H;
    using Aplicacion.Modelos;
    using MUFG_T24.Comunes;
    using MUFG_T24.Comunes.Helpers;
    using MUFG_T24.Interfaces_utilidades;
    using MUFG_T24.Modelos;
    using MUFG_T24.Repositorios;
    using System;
    using System.Threading.Tasks;
    /// <summary>
    /// Representa TEF H2 H.
    /// </summary>

    public class TEF_H2H
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ITefH2hTomiDao _tefH2hDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "TEF_H2H";
        private string folio_unico = string.Empty;

        /// <summary>
        /// Constructor.
        /// </summary>
        public TEF_H2H(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ITefH2hTomiDao tefH2hDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tefH2hDao = tefH2hDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación TEF_H2H.
        /// </summary>
        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion;
            bool procesado;

            // Verificamos la última operación no realizada.
            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });

            if (string.Equals(_configuracion.AliasProceso, string.Empty, StringComparison.Ordinal))
                return false;

            // Inicio.
            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (estatus, etc).");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO

                {

                    Folio = folio,

                    TipoOperacion = tipoOperacion,

                    Servicio = _configuracion.NombreServicio,

                    NombreProceso = _configuracion.NombreProceso,

                    AliasProceso = _configuracion.AliasProceso,

                    PId = _configuracion.Pid.Value

                });

                procesado = false;

                // Datos de la operación.
                fechaEjecucion = string.Empty;

                TefH2hDatosOperacionDto datosOperacion = await _tefH2hDao.ObtenerDatosOperacion(ConversionHelper.ParseLongRequerido(folio, "Folio"));
                if (datosOperacion != null)
                {
                    folio_unico = datosOperacion.FolioUnico ?? string.Empty;
                    fechaEjecucion = datosOperacion.FechaEjecucion?.ToString() ?? string.Empty;
                }

                // Hay folio_unico?
                if (string.Equals(folio_unico, string.Empty, StringComparison.Ordinal))
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("No hay folio_unico asignado para la operación");
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folio_unico}'.");
                }

                // Actualizo el maestro general.
                await _tefH2hDao.ActualizarPlanTrabajoMaestro(new TefH2hActualizarPlanTrabajoMaestroDto
                {
                    Folio = ConversionHelper.ParseLongRequerido(folio, "Folio"),
                    TipoOperacion = tipoOperacion,
                    FolioUnico = folio_unico,
                    FechaEjecucion = datosOperacion?.FechaEjecucion,
                    MonedaDefaultMN = _vwT24.MonedaDefaultMN
                });

                procesado = true;

                if (procesado)
                {
                    // Actualizo el estatus.
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminaProceso.
                    /// </summary>
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    return true;
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
                }
            }

            return false;
        }

    }

}

using Aplicacion.DTOs.TMD;
using Aplicacion.Interfaces.TMD;
using Aplicacion.Modelos;
using Aplicacion.Modelos.TMD;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa TMD.
    /// </summary>
    public class TMD
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ITMDCmsDao _tmdCmsDao;
        private readonly ITMDTomiDao _tmdTomiDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "TMD";
        private string folioUnico = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase TMD.
        /// </summary>

        public TMD(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ITMDCmsDao tmdCmsDao,
            ITMDTomiDao tmdTomiDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tmdCmsDao = tmdCmsDao;
            _tmdTomiDao = tmdTomiDao;
            _vwT24 = vwT24;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ProcesaOperacion(string lsFolio)
        {
            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = lsFolio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });
            if (string.IsNullOrEmpty(_configuracion.AliasProceso))
                return false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Aperturando conexiÃ³n a base de datos del CMS.");

            TMD_Formato_DTO formato = await _tmdCmsDao.ObtenerFormatoPorFolio(lsFolio);
            if (formato == null)
                formato = new TMD_Formato_DTO();

            string[] laValoresFormato = ParseContenidoFormato(formato.ContenidoFormato, lsFolio);

            string lsFolioForex = ValueAt(laValoresFormato, 15);
            string lsFechaEjecucion = ValueAt(laValoresFormato, 6);
            string lsMoneda = _vwT24.ValidaMoneda(ValueAt(laValoresFormato, 3));
            decimal ldImporte = formato.MontoOperacion;

            string lsTmdEspecial = formato.Id_tasa_especial ?? string.Empty;
            bool lbConTmd = false;
            string lsEstatusTmd = string.Empty;

            Config.EscribirLog("INFO|TMD.procesaOperacion|Validando si la operaciÃ³n es un time deposit especial.");
            if (!string.IsNullOrWhiteSpace(lsTmdEspecial) && long.TryParse(lsTmdEspecial, out _))
            {
                lbConTmd = true;
                Config.EscribirLog($"INFO|TMD.procesaOperacion|Asignado el id_AutorizaciÃ³n TMD: {lsTmdEspecial} .");
                var solicitud = await _tmdTomiDao.ObtenerSolicitudEspecialPorId(lsTmdEspecial);
                lsEstatusTmd = solicitud?.EstatusOperacion ?? string.Empty;
            }

            if (lbConTmd && !string.IsNullOrWhiteSpace(lsEstatusTmd))
            {
                switch (lsEstatusTmd)
                {
                    case "AUTORIZADO":
                        Config.EscribirLog("INFO|TMD.procesaOperacion|TIME DEPOSIT AUTORIZADO.");
                        break;
                    case "CANCELADO":
                        Config.EscribirLog("INFO|TMD.procesaOperacion|TIME DEPOSIT CANCELADO.");
                        break;
                    default:
                        Config.EscribirLog("INFO|TMD.procesaOperacion|TIME DEPOSIT PENDIENTE DE AUTORIZACIÃ“N.");
                        throw new Exception($"El Time Deposit con autorizaciÃ³n {lsTmdEspecial} no ha sido autorizado.");
                }
            }

            if (string.CompareOrdinal(lsFechaEjecucion, _configuracion.StrFechaSistema) < 0)
            {
                await _controlServicio.MarcarErrorFechaPasada(ConversionHelper.ParseLongOrDefault(lsFolio));
                throw new Exception($"Se desea realizar una operaciÃ³n al {lsFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(lsFechaEjecucion),
                lsFolio);

            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TMD.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                string lsSubTipo = lbConTmd ? $"TMD:{lsTmdEspecial}" : string.Empty;

                await _tmdTomiDao.ActualizarPlanTrabajoMaestroLectura(
                    lsFolio,
                    tipoOperacion,
                    folioUnico,
                    lsFechaEjecucion,
                    lsMoneda,
                    ldImporte,
                    formato.Cdl.ToString(CultureInfo.InvariantCulture),
                    ValueAt(laValoresFormato, 1),
                    ValueAt(laValoresFormato, 2),
                    lsSubTipo);
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = lsFolio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            if (string.Equals(_configuracion.AliasProceso, "CMS_A_TOMI", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TMD.procesaOperacion|A insertar datos de CMS a TOMI.");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var modelo = new TMD_InsertarFormatoModelo
                {
                    NF_FOLIO_UNICO = folioUnico,
                    NF_TIPO = "TMD",
                    NF_FOLIO = formato.Id_Formato,
                    T24_CUSTOMER_ID = formato.Cdl.ToString(CultureInfo.InvariantCulture),
                    T24_DRAWDOWN_ACCOUNT = ValueAt(laValoresFormato, 2),
                    NF_ESTATUS = "PEND",
                    NF_USUARIO_CAPTURA = formato.Id_UsuarioCaptura,
                    NF_USUARIO_AUTORIZA = formato.Id_UsuarioAutorizo,
                    NF_FECHA_CAPTURA = formato.FechaCaptura,
                    NF_FECHA_AUTORIZA = formato.FechaAutorizacion,
                    T24_DRAWDOWN_NET_AMT = ConversionHelper.ParseDecimalOrDefault(ValueAt(laValoresFormato, 4)),
                    NF_FOREX_RELACIONADO = lsFolioForex,
                    T24_CURRENCY = ValueAt(laValoresFormato, 3),
                    T24_VALUE_DATE = ConversionHelper.ParseIntOrDefault(ValueAt(laValoresFormato, 6)),
                    T24_FIN_MAT_DATE = ConversionHelper.ParseIntOrDefault(ValueAt(laValoresFormato, 5)),
                    T24_INTEREST_RATE = ConversionHelper.ParseDoubleOrDefault(ValueAt(laValoresFormato, 8)),
                    T24_INTEREST_SPREAD = ConversionHelper.ParseDoubleOrDefault(ValueAt(laValoresFormato, 9))
                };

                bool lbResultado = await _tmdTomiDao.InsertarFormatoTmd(modelo);
                bool lbProcesado = lbResultado;

                if (lbResultado)
                {
                    Config.EscribirLog("INFO|TMD.procesaOperacion|Se insertÃ³ correctamente el folio en base de TOMI.");

                    if (lbConTmd)
                        await _tmdTomiDao.MarcarSolicitudEspecialProcesada(lsTmdEspecial);
                }
                else
                {
                    int regs = await _tmdTomiDao.ContarTombinNfPorFolio(lsFolio);
                    if (regs > 0)
                    {
                        lbProcesado = true;
                        Config.EscribirLog("INFO|TMD.procesaOperacion|El folio en procesamiento ya se habÃ­a insertado en base TOMI pero no se marcÃ³ el proceso como realizado.");
                    }
                }

                if (lbProcesado)
                {
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = lsFolio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso =
                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                        {
                            Folio = lsFolio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });
                }
                else
                    throw new Exception($"Error en procesamiento de registro CMS_A_TOMI con folio: {lsFolio}");
            }

            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TMD.procesaOperacion|A actualizar estatus en CMS y TOMI.");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                string mensaje;
                string estatusCms;
                string estatusTomi;

                if (string.CompareOrdinal(lsFechaEjecucion, _configuracion.StrFechaSistema) < 0)
                {
                    await _controlServicio.MarcarErrorFechaPasada(ConversionHelper.ParseLongOrDefault(lsFolio));
                    throw new Exception($"Se desea realizar una operaciÃ³n al {lsFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                }

                if (string.Equals(lsFechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                {
                    mensaje = "Formato aceptado";
                    estatusCms = "APFT";
                    estatusTomi = "SCNP";
                    Config.EscribirLog("INFO|TMD.procesaOperacion|OperaciÃ³n fecha valor hoy.");
                }
                else
                {
                    mensaje = "Formato recibido pendiente ejecuciÃ³n";
                    estatusCms = "REPE";
                    estatusTomi = "SCNF";
                    Config.EscribirLog("INFO|TMD.procesaOperacion|OperaciÃ³n fecha valor fÃºtura.");
                }

                await _tmdCmsDao.ActualizarFormatoEstado(lsFolio, mensaje, estatusCms);
                await _tmdTomiDao.ActualizarNfEstatus(lsFolio, estatusTomi, "OPER CMS");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = lsFolio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TMD.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                bool forexCancelado = false;
                TMD_Tombin_Modelo tomi = await _tmdTomiDao.ObtenerDatosConciliacion(lsFolio, forexCancelado);

                var errores = new StringBuilder();
                if (tomi != null)
                {
                    if (!string.Equals(lsFolioForex ?? string.Empty, tomi.NF_FOREX_RELACIONADO ?? string.Empty, StringComparison.Ordinal))
                        errores.AppendLine($" - Diferencia en la autorizaciÃ³n de FOREX. CMS='{lsFolioForex}', TOMI='{tomi.NF_FOREX_RELACIONADO}'.");
                    if (ConversionHelper.ParseIntOrDefault(lsFechaEjecucion) != tomi.NF_FECHA_EJECUCION)
                        errores.AppendLine($" - Diferencia en la fecha de ejecuciÃ³n. CMS='{lsFechaEjecucion}', TOMI='{tomi.NF_FECHA_EJECUCION}'.");
                    if (!string.Equals(lsMoneda, _vwT24.ValidaMoneda(tomi.T24_CURRENCY), StringComparison.Ordinal))
                        errores.AppendLine($" - Diferencia en la monedas. CMS='{lsMoneda}', TOMI='{tomi.T24_CURRENCY}'.");
                    if (ldImporte != tomi.T24_DRAWDOWN_NET_AMT)
                        errores.AppendLine($" - Diferencia en el importe. CMS='{ldImporte}', TOMI='{tomi.T24_DRAWDOWN_NET_AMT}'.");
                }
                else
                {
                    errores.AppendLine(" - No se ha registrado la operaciÃ³n en TOMI.");
                }

                if (errores.Length > 0)
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(errores.ToString());

                Config.EscribirLog("INFO|TMD.procesaOperacion|La operaciÃ³n estÃ¡ correctamente conciliada.");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });
                _configuracion.AliasProceso = string.Empty;
            }

            return true;
        }
        private static string[] ParseContenidoFormato(string contenido, string folio)
        {
            var parseado = ContenidoFormatoParser.Parse(contenido, folio);
            ContenidoFormatoParser.ValidarMinimoIndice(parseado, folio, 18);

            string[] valores = new string[32];
            for (int i = 0; i <= 18; i++)
            {
                valores[i] = ContenidoFormatoParser.ObtenerValorPosicional(parseado, folio, i);
            }

            return valores;
        }

        private static string ValueAt(string[] values, int idx) => idx >= 0 && idx < values.Length ? values[idx] ?? string.Empty : string.Empty;

    }
}

using Aplicacion.Enums;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Repositorios;
using System;
using System.Linq;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.FirmaElectronica
{
    /// <summary>
    /// Representa Comun FXD.
    /// </summary>
    public class ComunFXD : RepositorioMaestro, IComunFXD
    {
        private const string CASFIM_MUFG = "40108";
        private readonly ConfiguracionModelo _configuracion;
        /// <summary>
        /// Inicializa una nueva instancia de la clase ComunFXD.
        /// </summary>
        public ComunFXD(IAlertas alertaT, ConfiguracionModelo config) : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null)
        {
            _configuracion = config;
        }
        /// <summary>
        /// Genera la firma digital para un registro específico...
        /// </summary>
        public async Task<bool> GeneraFirmaDigital(string strTabla,string strFolio,string strFolioT24,bool? strFlgNomina, int? intTipoPago , string strReferencia_emisor="")
        {
            try
            {
                if (!intTipoPago.HasValue)
                    intTipoPago = 1;


                Config.EscribirLog("INFO|ComunFXD.GenerarFirmaDigital|Inicia proceso para generacion de firma");

                var vDatosGenerarFirmaDigital = (await EjecutarStoredProcedureQuery<ComunFXD_GFDModelo>(ConstantesServicio.ProcedimientosFXDH2H.GFD_CONSULTAR_DATOS_FIRMA, cmd => 
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tabla", strTabla));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", strFolio));
                }))?.Data?.LastOrDefault();
               
                if (vDatosGenerarFirmaDigital==null) 
                {
                    Config.EscribirLog("ERROR|ComunFXD.GeneraFirmaDigital|El registro ya se intento registrar pero marco nf_interfase_spei=2 (problemas de registro)");
                    return false;
                }

                object[] laValoresFormato = new object[9];
                string[] laLlaveValor = (vDatosGenerarFirmaDigital.NF_BANCO_BENEF ?? string.Empty).Split('-');
                string lsDebitTheirRef;

                laValoresFormato[0] = vDatosGenerarFirmaDigital.NF_FECHA_EJECUCION;
                laValoresFormato[1] = DameTexto(vDatosGenerarFirmaDigital.NF_BANCO_BENEF);
                laValoresFormato[2] = laLlaveValor.Length > 0 ? laLlaveValor[0].Trim() : string.Empty;
                laValoresFormato[3] = vDatosGenerarFirmaDigital.IMPORTE;
                laValoresFormato[4] = (strFlgNomina == true) ? strFolioT24 + "-" + strFolio : strFolioT24;
                laValoresFormato[5] = (intTipoPago == 1) ? CuentaClabe(DameTexto(vDatosGenerarFirmaDigital.T24_DEBIT_ACCT_NO)) : string.Empty;

                if (string.IsNullOrEmpty(strReferencia_emisor))
                {
                    string debitTheirRef = DameTexto(vDatosGenerarFirmaDigital.T24_DEBIT_THEIR_REF);
                    string debitTheirRef40 = debitTheirRef.Substring(0, Math.Min(40, debitTheirRef.Length)).Trim();

                    if (string.IsNullOrEmpty(debitTheirRef40.ToUpperInvariant()))
                        lsDebitTheirRef = DameTexto(vDatosGenerarFirmaDigital.NF_FOLIO_UNICO);
                    else
                    {
                        string txtSpei = TextoSPEI(DameTexto(vDatosGenerarFirmaDigital.T24_DEBIT_THEIR_REF));
                        lsDebitTheirRef = txtSpei.Substring(0, Math.Min(40, txtSpei.Length));
                    }
                }
                else
                {
                    lsDebitTheirRef = strReferencia_emisor;
                    Config.EscribirLog("INFO|ComunFXD.GeneraFirmaDigital|La operación proviene de un SWIFT_IN con referencia='" + lsDebitTheirRef + "'.");
                }

                laValoresFormato[6] = lsDebitTheirRef;
                laValoresFormato[7] = DameTexto(vDatosGenerarFirmaDigital.T24_BEN_ACCT_NO);
                laValoresFormato[8] = DameTexto(vDatosGenerarFirmaDigital.NF_FOREX_RELACIONADO);

                if (!string.IsNullOrEmpty(Convert.ToString(laValoresFormato[8])) && intTipoPago == 1)
                {
                    intTipoPago = 5;
                    Config.EscribirLog("INFO|ComunFXD.GenerarFirmaDigital|Se cambia el tipo de pago SPEI de 1 a 5 por tener forex relacionado.");
                }

                for (int liIdx = 0; liIdx <= 8; liIdx++)
                    Config.EscribirLog("INFO|ComunFXD.GeneraFirmaDigital|Valor[" + liIdx + "]=" + Convert.ToString(laValoresFormato[liIdx]));

                Config.EscribirLog($"INFO|ComunFXD.GeneraFirmaDigital|Se termina recolección de datos para firma:{strFolio}");

                OrdenPagoValue OrdenPagoValue = new OrdenPagoValue();
                {
                    OrdenPagoValue.InsClaveOrd = CASFIM_MUFG;
                    OrdenPagoValue.OpFechaOper = laValoresFormato[0];
                    OrdenPagoValue.InsClaveBen = laValoresFormato[2];
                    OrdenPagoValue.OpMonto = laValoresFormato[3];
                    OrdenPagoValue.OpCveRastreo = laValoresFormato[4];
                    OrdenPagoValue.OpCuentaOrd = laValoresFormato[5];
                    OrdenPagoValue.OpCuentaBen = laValoresFormato[7];
                    OrdenPagoValue.TpPago = intTipoPago;
                }

                OrdenPagoByteArray ordenPagoByteArray = ObtieneTrama(OrdenPagoValue);

                ///Generacion del dato firmado
                string strFirmaDigital;
                using (var firma = new MetodosFirma(_configuracion))
                {
                    Config.EscribirLog("INFO|ComunFXD.GeneraFirmaDigital|Se realiza el llamado para la firma");
                    strFirmaDigital = firma.GenerateSign(ordenPagoByteArray.TramaCompleta);
                }

                var vActualizarFirmadigital = await EjecutarStoredProcedureQuery<ComunFXD_GFDModelo>(ConstantesServicio.ProcedimientosFXDH2H.GFD_ACTUALIZAR_FIRMA_DIGITAL, cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tabla", strTabla));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FirmaDigital", strFirmaDigital));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", strFolio));
                },
                validarAfectacion: false);

                if (vActualizarFirmadigital == null || !vActualizarFirmadigital.Exito)
                {
                    Config.EscribirLog("ERROR|ComunFXD.GeneraFirmaDigital|No se actualizó el campo OP_FIRMA_DIG.");
                    return false;
                }

                Config.EscribirLog("INFO|ComunFXD.GeneraFirmaDigital|Se actualizó el campo para la firma exitosamente");

                return true;
            }
            catch (Exception ErrorRegistro)
            {
                Config.EscribirLog("ERROR|ComunFXD.GeneraFirmaDigital|Se presenta exception: " + ErrorRegistro);
                return false;
            }           
        }

        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>
        public string MuestraTrama(byte[] trama)
        {
            return BitConverter.ToString(trama);
        }

        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>
        public void LogueDatosTrama(OrdenPagoByteArray ordenPagoByteArrayModelo)
        {
            Config.EscribirLog("INFO|ComunFXD.GenerarFirmaDigital|Se obtienen bytes de cada tipo de dato ");

            LogTrama("InsClaveBen", ordenPagoByteArrayModelo.InsClaveBen);
            LogTrama("InsClaveOrd", ordenPagoByteArrayModelo.InsClaveOrd);
            LogTrama("OpCuentaBen", ordenPagoByteArrayModelo.OpCuentaBen);
            /// <summary>
            /// Ejecuta la operaciÃ³n LogTrama.
            /// </summary>
            LogTrama("OpCuentaBen2", ordenPagoByteArrayModelo.OpCuentaBen2);
            /// <summary>
            /// Ejecuta la operaciÃ³n LogTrama.
            /// </summary>
            LogTrama("OpCuentaOrd", ordenPagoByteArrayModelo.OpCuentaOrd);
            /// <summary>
            /// Ejecuta la operaciÃ³n LogTrama.
            /// </summary>
            LogTrama("OpCveRastreo", ordenPagoByteArrayModelo.OpCveRastreo);
            /// <summary>
            /// Ejecuta la operaciÃ³n LogTrama.
            /// </summary>
            LogTrama("OpFechaOper", ordenPagoByteArrayModelo.OpFechaOper);
            /// <summary>
            /// Ejecuta la operaciÃ³n LogTrama.
            /// </summary>
            LogTrama("OpMonto", ordenPagoByteArrayModelo.OpMonto);
            /// <summary>
            /// Ejecuta la operaciÃ³n LogTrama.
            /// </summary>
            LogTrama("opCuentaPartOrd", (byte[])ordenPagoByteArrayModelo.opCuentaPartOrd);
            /// <summary>
            /// Ejecuta la operaciÃ³n LogTrama.
            /// </summary>
            LogTrama("opCuentaEmiRem", (byte[])ordenPagoByteArrayModelo.opCuentaEmiRem);
            /// <summary>
            /// Ejecuta la operaciÃ³n LogTrama.
            /// </summary>
            LogTrama("TramaCompleta", ordenPagoByteArrayModelo.TramaCompleta);
        }

        /// <summary>
        /// Ejecuta la operación LogTrama.
        /// </summary>
        private void LogTrama(string etiqueta, byte[] trama)
        {
            if (trama != null && trama.Length > 0)
                Config.EscribirLog($"INFO|ComunFXD.GeneraFirmaDigital|{etiqueta} {MuestraTrama(trama)}");
        }

        /// <summary>
        /// Ejecuta la operación public.
        /// </summary>
        public async Task<string> ObtieneCampoFirma(OrdenPagoValue OrdenPagoValue)
        {
            try
            {
                using (var firma = new MetodosFirma(_configuracion))
                {
                    var conversiones = ObtieneTrama(OrdenPagoValue);
                    return firma.GenerateSign(conversiones.TramaCompleta);
                }
            }
            catch (Exception ErrorTrama)
            {

                throw new Exception($"ObtieneTrama|Se presenta exception:{ErrorTrama}");
            }
        }

        /// <summary>
        /// Ejecuta la operación public.
        /// </summary>
        public bool ValidaSPEIEntrada(OrdenPagoValue OrdenPagoValue, string dataSigned)
        {
            try
            {
                using (var firma = new MetodosFirma(_configuracion))
                {
                    Config.EscribirLog($"INFO|ComunFXD.GenerarFirmaDigital|Firma Recibida:{dataSigned}");
                    var opba = ObtieneTrama(OrdenPagoValue);
                    if (string.IsNullOrWhiteSpace(dataSigned))
                    {
                        Config.EscribirLog("ERROR|ComunFXD.ValidaSPEIEntrada|La firma recibida está vacía.");
                        return false;
                    }

                    byte[] signedData;

                    try
                    {
                        signedData = Convert.FromBase64String(dataSigned.Trim());
                    }
                    catch (FormatException)
                    {
                        Config.EscribirLog("ERROR|ComunFXD.ValidaSPEIEntrada|La firma recibida no tiene formato Base64 válido.");
                        return false;
                    }

                    //firma.GenerateSign(opba.TramaCompleta); //DEBUG ONLY

                    return firma.ConfirmaFirma(opba.TramaCompleta,signedData);
                }
            }
            catch (Exception ErrorTrama)
            {

                throw new Exception($"ObtieneTrama|Se presenta exception:{ErrorTrama}");
            }
        }

        /// <summary>
        /// Ejecuta la operación public.
        /// </summary>
        public OrdenPagoByteArray ObtieneTrama(OrdenPagoValue OrdenPagoValue)
        {
            var vConversiones = new OrdenPagoByteArray();
            short stTipoPago = Convert.ToInt16(OrdenPagoValue.TpPago);

            vConversiones.OpFechaOper = ConversionHelper.dateToByteArray(Convert.ToInt32(OrdenPagoValue.OpFechaOper), ConversionHelper.ByteOrder.BIG_ENDIAN);
            vConversiones.InsClaveOrd = ConversionHelper.intToByteArray(Convert.ToInt32(OrdenPagoValue.InsClaveOrd), ConversionHelper.ByteOrder.BIG_ENDIAN);
            vConversiones.InsClaveBen = ConversionHelper.intToByteArray(Convert.ToInt32(OrdenPagoValue.InsClaveBen), ConversionHelper.ByteOrder.BIG_ENDIAN);
            vConversiones.OpCveRastreo = ConversionHelper.stringToByteArray(System.Convert.ToString(OrdenPagoValue.OpCveRastreo), ConversionHelper.ByteOrder.BIG_ENDIAN);
            vConversiones.OpMonto = ConversionHelper.moneyToByteArray((double)System.Convert.ToDecimal(OrdenPagoValue.OpMonto), ConversionHelper.ByteOrder.BIG_ENDIAN);
            vConversiones.OpCuentaOrd = ConversionHelper.stringToByteArray(System.Convert.ToString(OrdenPagoValue.OpCuentaOrd), ConversionHelper.ByteOrder.BIG_ENDIAN);
            vConversiones.OpCuentaBen = ConversionHelper.stringToByteArray(System.Convert.ToString(OrdenPagoValue.OpCuentaBen), ConversionHelper.ByteOrder.BIG_ENDIAN);
            vConversiones.opCuentaPartOrd = ConversionHelper.stringToByteArray(System.Convert.ToString(OrdenPagoValue.opCuentaPartOrd), ConversionHelper.ByteOrder.BIG_ENDIAN);
            vConversiones.opCuentaEmiRem = ConversionHelper.stringToByteArray(System.Convert.ToString(OrdenPagoValue.opCuentaEmiRem), ConversionHelper.ByteOrder.BIG_ENDIAN);

            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|InsClaveOrd = {OrdenPagoValue.InsClaveOrd}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|OpFechaOper = {OrdenPagoValue.OpFechaOper}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|InsClaveBen = {OrdenPagoValue.InsClaveBen}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|OpMonto = {OrdenPagoValue.OpMonto}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|OpCveRastreo = {OrdenPagoValue.OpCveRastreo}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|OpCuentaOrd = {OrdenPagoValue.OpCuentaOrd}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|OpCuentaBen = {OrdenPagoValue.OpCuentaBen}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|TpPago = {OrdenPagoValue.TpPago}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|opCuentaPartOrd = {OrdenPagoValue.opCuentaPartOrd}");
            Config.EscribirLog($"INFO|ComunFXD.ObtieneTrama|opCuentaEmiRem = {OrdenPagoValue.opCuentaEmiRem}");

            vConversiones.TramaCompleta = Array.Empty<byte>();

            try
            {
                switch (stTipoPago)
                {
                    case 0:
                       vConversiones.TramaCompleta = vConversiones.TramaCompleta
                           .Concat(vConversiones.OpFechaOper) 
                           .Concat(vConversiones.InsClaveOrd)
                           .Concat(vConversiones.InsClaveBen)
                           .Concat(vConversiones.OpCveRastreo)
                           .Concat(vConversiones.OpMonto)
                           .ToArray();
                        break;
                    case 1:
                        vConversiones.TramaCompleta = vConversiones.TramaCompleta
                          .Concat(vConversiones.OpFechaOper)
                          .Concat(vConversiones.InsClaveOrd)
                          .Concat(vConversiones.InsClaveBen)
                          .Concat(vConversiones.OpCveRastreo)
                          .Concat(vConversiones.OpMonto)
                          .Concat(vConversiones.OpCuentaOrd)
                          .Concat(vConversiones.OpCuentaBen)
                          .ToArray();
                        break;
                    case 5:
                        vConversiones.TramaCompleta = vConversiones.TramaCompleta
                          .Concat(vConversiones.OpFechaOper)
                          .Concat(vConversiones.InsClaveOrd)
                          .Concat(vConversiones.InsClaveBen)
                          .Concat(vConversiones.OpCveRastreo)
                          .Concat(vConversiones.OpMonto)
                          .Concat(vConversiones.OpCuentaBen)
                          .ToArray();
                        break;
                    case 6:
                        if (string.IsNullOrEmpty(System.Convert.ToString(OrdenPagoValue.OpCuentaBen2)))
                        {
                            vConversiones.OpCuentaBen2 = new byte[] { 0 };
                        }
                        else
                        {
                            vConversiones.OpCuentaBen2 = ConversionHelper.stringToByteArray(System.Convert.ToString(OrdenPagoValue.OpCuentaBen2), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        }

                        vConversiones.TramaCompleta = vConversiones.TramaCompleta
                          .Concat(vConversiones.OpFechaOper)
                          .Concat(vConversiones.InsClaveOrd)
                          .Concat(vConversiones.InsClaveBen)
                          .Concat(vConversiones.OpCveRastreo)
                          .Concat(vConversiones.OpMonto)
                          .Concat(vConversiones.OpCuentaBen)
                          .Concat(vConversiones.OpCuentaBen2)
                          .ToArray();
                        break;
                    case 7:
                        vConversiones.TramaCompleta = vConversiones.TramaCompleta
                          .Concat(vConversiones.OpFechaOper)
                          .Concat(vConversiones.InsClaveOrd)
                          .Concat(vConversiones.InsClaveBen)
                          .Concat(vConversiones.OpCveRastreo)
                          .Concat(vConversiones.OpMonto)
                          .ToArray();
                        break;
                    case 3:
                        if (string.IsNullOrEmpty(System.Convert.ToString(OrdenPagoValue.OpCuentaBen2)))
                        {
                            vConversiones.OpCuentaBen2 = new byte[] { 0 };
                        }
                        else
                        {
                            vConversiones.OpCuentaBen2 = ConversionHelper.stringToByteArray(System.Convert.ToString(OrdenPagoValue.OpCuentaBen2), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        }

                        vConversiones.TramaCompleta = vConversiones.TramaCompleta
                          .Concat(vConversiones.OpFechaOper)
                          .Concat(vConversiones.InsClaveOrd)
                          .Concat(vConversiones.InsClaveBen)
                          .Concat(vConversiones.OpCveRastreo)
                          .Concat(vConversiones.OpMonto)
                          .Concat(vConversiones.OpCuentaOrd)
                          .Concat(vConversiones.OpCuentaBen)
                          .Concat(vConversiones.OpCuentaBen2)
                          .ToArray();
                        break;
                    case 30:
                        vConversiones.TramaCompleta = vConversiones.TramaCompleta
                          .Concat(vConversiones.OpFechaOper)
                          .Concat(vConversiones.InsClaveOrd)
                          .Concat(vConversiones.InsClaveBen)
                          .Concat(vConversiones.OpCveRastreo)
                          .Concat(vConversiones.OpMonto)
                          .Concat(vConversiones.OpCuentaOrd)
                          .Concat(vConversiones.OpCuentaBen)
                          .Concat((byte[])vConversiones.opCuentaPartOrd)
                          .ToArray();
                        break;
                    case 31:
                        vConversiones.TramaCompleta = vConversiones.TramaCompleta
                          .Concat(vConversiones.OpFechaOper)
                          .Concat(vConversiones.InsClaveOrd)
                          .Concat(vConversiones.InsClaveBen)
                          .Concat(vConversiones.OpCveRastreo)
                          .Concat(vConversiones.OpMonto)
                          .Concat(vConversiones.OpCuentaOrd)
                          .Concat((byte[])vConversiones.opCuentaPartOrd)                         
                          .ToArray();
                        break;
                    case 36:
                        vConversiones.TramaCompleta = vConversiones.TramaCompleta
                          .Concat(vConversiones.OpFechaOper)
                          .Concat(vConversiones.InsClaveOrd)
                          .Concat(vConversiones.InsClaveBen)
                          .Concat(vConversiones.OpCveRastreo)
                          .Concat(vConversiones.OpMonto)
                          .Concat(vConversiones.OpCuentaOrd)
                          .Concat(vConversiones.OpCuentaBen)
                          .Concat((byte[])vConversiones.opCuentaPartOrd)
                          .Concat((byte[])vConversiones.opCuentaEmiRem)
                          .ToArray();
                        break;
                    default:
                        throw new Exception($"No existe definición para pago SPEI del tipo de pago {stTipoPago}");
                }

                LogueDatosTrama(vConversiones);
                return vConversiones;
            }
            catch (Exception lexError)
            {
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("ERROR|ComunFXD.ObtieneTrama|Se presenta exception: " + lexError);
            }
        }

    }
}

using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.SPID;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.FirmaElectronica
{
    /// <summary>
    /// Representa Comun SPID.
    /// </summary>
    public class ComunSPID : IComunSPID
    {
        private const string CASFIM_MUFG = "40108";

        private readonly IComunSPIDDao _spidDao;
        private readonly ConfiguracionModelo _configuracion;

/// <summary>
/// Inicializa una nueva instancia de la clase ComunSPID.
/// </summary>
        public ComunSPID(
            IComunSPIDDao spidDao,
            ConfiguracionModelo configuracion)
        {
            _spidDao = spidDao;
            _configuracion = configuracion;
        }

/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public async Task<bool> GeneraFirmaDigitalSPID(
            string strTabla,
            string strFolio,
            string strFolioT24,
            bool? strFlgNomina = false,
            int? intTipoPago = 1,
            string strReferencia_emisor = "")
        {
            try
            {
                using (var metodoFirma = new MetodosFirma(_configuracion))
                {
                    Config.EscribirLog("INFO|ComunSPID.GeneraFirmaDigital|Inicia proceso para generación de firma");

                    short tipoPago = Convert.ToInt16(intTipoPago ?? 1);
                    bool flgNomina = strFlgNomina ?? false;

                    var datos = await _spidDao.ObtenerDatosGeneracionFirma(strTabla, strFolio);

                    if (datos == null)
                    {
                        Config.EscribirLog("ERROR|ComunSPID.GeneraFirmaDigital|El registro ya se intento registrar pero marco nf_interfase_spei=2 (problemas de registro)");
                        return false;
                    }

                    var valores = new object[9];

                    valores[0] = datos.NF_FECHA_EJECUCION;
                    valores[1] = datos.NF_BANCO_BENEF;

                    var split = (datos.NF_BANCO_BENEF ?? "").Split('-');
                    valores[2] = split.Length > 0 ? split[0].Trim() : "";

                    valores[3] = datos.IMPORTE;

                    valores[4] = flgNomina
                        ? $"{strFolioT24}-{strFolio}"
                        : strFolioT24;

                    valores[5] = tipoPago == 1
                        ? CuentaClabe(datos.T24_DEBIT_ACCT_NO)
                        : "";

                    string referencia;

                    if (string.IsNullOrEmpty(strReferencia_emisor))
                    {
                        string refRaw = DameTexto(datos.T24_DEBIT_THEIR_REF);
                        string refRaw40 = refRaw.Substring(0, Math.Min(40, refRaw.Length)).Trim();

                        if (string.IsNullOrEmpty(refRaw40.ToUpperInvariant()))
                            referencia = DameTexto(datos.NF_FOLIO_UNICO);
                        else
                        {
                            string refNorm = TextoSPID(DameTexto(datos.T24_DEBIT_THEIR_REF));
                            referencia = refNorm.Substring(0, Math.Min(40, refNorm.Length));
                        }
                    }
                    else
                    {
                        referencia = strReferencia_emisor;
                        Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|La operación proviene de un SWIFT_IN con referencia='{referencia}'.");
                    }

                    valores[6] = referencia;
                    valores[7] = datos.T24_BEN_ACCT_NO;
                    valores[8] = datos.NF_FOREX_RELACIONADO;

                    for (int i = 0; i <= 8; i++)
                    {
                        Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigitalSPID|Valor[{i}]={(valores[i] ?? string.Empty)}");
                    }

                    if (!string.IsNullOrEmpty(valores[8]?.ToString()) && tipoPago == 1)
                    {
                        tipoPago = 5;
                        Config.EscribirLog("INFO|ComunSPID.GeneraFirmaDigital|Se cambia el tipo de pago SPID de 1 a 5 por tener forex relacionado.");
                    }

                    var dataValue = new OrdenPagoValue
                    {
                        InsClaveOrd = CASFIM_MUFG,
                        OpFechaOper = valores[0].ToString(),
                        InsClaveBen = valores[2].ToString(),
                        OpMonto = valores[3].ToString(),
                        OpCveRastreo = valores[4].ToString(),
                        OpCuentaOrd = valores[5].ToString(),
                        OpCuentaBen = valores[7].ToString(),
                        TpPago = tipoPago
                    };

                    var trama = ObtieneTrama(dataValue);

                    var firma = metodoFirma.GenerateSign(trama.TramaCompleta);

                    bool actualizado = await _spidDao.ActualizarFirma(strTabla, strFolio, firma);
                    if (actualizado)
                        Config.EscribirLog("INFO|ComunSPID.GeneraFirmaDigital|Se actualizó el campo para la firma exitosamente");

                    return actualizado;
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ComunSPID.GeneraFirmaDigital|Se presenta exception: {ex}");
                return false;
            }
        }

/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>
        public async Task<bool> ValidaFirmaDigitalSPID(
            string strTabla,
            string strFolio,
            string strFolioT24,
            bool? strFlgNomina = false,
            int? intTipoPago = 1,
            string strReferencia_emisor = "")
        {
            try
            {
                using (var metodoFirma = new MetodosFirma(_configuracion))
                {
                    Config.EscribirLog("INFO|ComunSPID.GeneraFirmaDigital|Inicia proceso para generación de firma");

                    short tipoPago = Convert.ToInt16(intTipoPago ?? 1);
                    bool flgNomina = strFlgNomina ?? false;

                    var datos = await _spidDao.ObtenerDatosFirma(strTabla, strFolio);
                    if (datos == null)
                    {
                        Config.EscribirLog("ERROR|ComunSPID.GeneraFirmaDigital|El registro ya se intento registrar pero marco nf_interfase_spei=2 (problemas de registro)");
                        return false;
                    }

                    var valores = new object[10];
                    valores[0] = datos.NF_FECHA_EJECUCION;
                    valores[1] = datos.NF_BANCO_BENEF;

                    var split = (datos.NF_BANCO_BENEF ?? "").Split('-');
                    valores[2] = split.Length > 0 ? split[0].Trim() : "";

                    valores[3] = datos.IMPORTE;
                    valores[4] = flgNomina ? $"{strFolioT24}-{strFolio}" : strFolioT24;
                    valores[5] = tipoPago == 1 ? CuentaClabe(datos.T24_DEBIT_ACCT_NO) : "";

                    string referencia;
                    if (string.IsNullOrEmpty(strReferencia_emisor))
                    {
                        string refRaw = DameTexto(datos.T24_DEBIT_THEIR_REF);
                        string refRaw40 = refRaw.Substring(0, Math.Min(40, refRaw.Length)).Trim();

                        if (string.IsNullOrEmpty(refRaw40.ToUpperInvariant()))
                            referencia = DameTexto(datos.NF_FOLIO_UNICO);
                        else
                        {
                            string refNorm = TextoSPID(DameTexto(datos.T24_DEBIT_THEIR_REF));
                            referencia = refNorm.Substring(0, Math.Min(40, refNorm.Length));
                        }
                    }
                    else
                    {
                        referencia = strReferencia_emisor;
                        Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|La operación proviene de un SWIFT_IN con referencia='{referencia}'.");
                    }

                    valores[6] = referencia;
                    valores[7] = datos.T24_BEN_ACCT_NO;
                    valores[8] = datos.NF_FOREX_RELACIONADO;
                    valores[9] = datos.OP_FIRMA_DIG;

                    if (!string.IsNullOrEmpty(valores[8]?.ToString()) && tipoPago == 1)
                    {
                        tipoPago = 5;
                        Config.EscribirLog("INFO|ComunSPID.GeneraFirmaDigital|Se cambia el tipo de pago SPID de 1 a 5 por tener forex relacionado.");
                    }

                    for (int i = 0; i <= 8; i++)
                    {
                        Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|Valor[{i}]={(valores[i] ?? string.Empty)}");
                    }

                    Config.EscribirLog("INFO|ComunSPID.GeneraFirmaDigital|Se termina recolección de datos para firma: " + strFolio);

                    var dataValue = new OrdenPagoValue
                    {
                        InsClaveOrd = CASFIM_MUFG,
                        OpFechaOper = valores[0],
                        InsClaveBen = valores[2],
                        OpMonto = valores[3],
                        OpCveRastreo = valores[4],
                        OpCuentaOrd = valores[5],
                        OpCuentaBen = valores[7],
                        TpPago = tipoPago
                    };

                    var trama = ObtieneTrama(dataValue);
                    var firma = metodoFirma.GenerateSign(trama.TramaCompleta);

                    Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|Firma obtenida: {firma}");
                    Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|Firma registrada previamente: {valores[9]}");

                    return string.Equals(firma, Convert.ToString(valores[9]), StringComparison.Ordinal);
                }
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|ComunSPID.GeneraFirmaDigital|Se presenta exception: {ex}");
                return false;
            }
        }

/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public async Task<bool> ValidaSPIDEntrada(OrdenPagoValue dataValue, string dataSigned)
        {
            try
            {
                using (var metodoFirma = new MetodosFirma(_configuracion))
                {
                    Config.EscribirLog($"INFO|ComunSPID.ValidaSPIDEntrada|Firma Recibida: {dataSigned}");

                    var opba = ObtieneTrama(dataValue);

                    var signedData = Convert.FromBase64String(dataSigned);

                    //var firmaTemporal = metodoFirma.GenerateSign(opba.TramaCompleta);

                    return metodoFirma.ConfirmaFirma(opba.TramaCompleta, signedData);
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"ValidaSPIDEntrada|Se presenta exception: {ex}");
            }
        }

        /// <summary>
        /// Obtiene la trama de bytes a partir de los datos de la orden de pago.
        /// </summary>
        /// <param name="dataValue"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private OrdenPagoByteArray ObtieneTrama(OrdenPagoValue dataValue)
        {
            var conversiones = new OrdenPagoByteArray();
            short tipoPago = Convert.ToInt16(dataValue.TpPago);

            Config.EscribirLog($"INFO|ComunSPID.ObtieneTrama|InsClaveOrd = {dataValue.InsClaveOrd}");
            Config.EscribirLog($"INFO|ComunSPID.ObtieneTrama|OpFechaOper = {dataValue.OpFechaOper}");
            Config.EscribirLog($"INFO|ComunSPID.ObtieneTrama|InsClaveBen = {dataValue.InsClaveBen}");
            Config.EscribirLog($"INFO|ComunSPID.ObtieneTrama|OpMonto = {dataValue.OpMonto}");
            Config.EscribirLog($"INFO|ComunSPID.ObtieneTrama|OpCveRastreo = {dataValue.OpCveRastreo}");
            Config.EscribirLog($"INFO|ComunSPID.ObtieneTrama|OpCuentaOrd = {dataValue.OpCuentaOrd}");
            Config.EscribirLog($"INFO|ComunSPID.ObtieneTrama|OpCuentaBen = {dataValue.OpCuentaBen}");
            Config.EscribirLog($"INFO|ComunSPID.ObtieneTrama|TpPago = {dataValue.TpPago}");

            try
            {
                switch (tipoPago)
                {
                    // =========================================
                    // Devoluciones
                    // =========================================
                    case 0:
                        Config.EscribirLog("INFO|ComunSPID.ObtieneTrama|Se inicia proceso de conversión de bytes tipo Devolucion");

                        conversiones.OpFechaOper = ConversionHelper.dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.InsClaveOrd = ConversionHelper.intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.InsClaveBen = ConversionHelper.intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpCveRastreo = ConversionHelper.stringToByteArray(System.Convert.ToString(dataValue.OpCveRastreo), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpMonto = ConversionHelper.moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ConversionHelper.ByteOrder.BIG_ENDIAN);

                        conversiones.TramaCompleta =
                            conversiones.OpFechaOper
                            .Concat(conversiones.InsClaveOrd)
                            .Concat(conversiones.InsClaveBen)
                            .Concat(conversiones.OpCveRastreo)
                            .Concat(conversiones.OpMonto)
                            .ToArray();

                        break;

                    // =========================================
                    // Tercero a Tercero
                    // =========================================
                    case 1:
                        Config.EscribirLog("INFO|ComunSPID.ObtieneTrama|Se inicia proceso de conversión de bytes tipo tercero a tercero");

                        conversiones.OpFechaOper = ConversionHelper.dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.InsClaveOrd = ConversionHelper.intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.InsClaveBen = ConversionHelper.intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpCveRastreo = ConversionHelper.stringToByteArray(System.Convert.ToString(dataValue.OpCveRastreo), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpMonto = ConversionHelper.moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpCuentaOrd = ConversionHelper.stringToByteArray(System.Convert.ToString(dataValue.OpCuentaOrd), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpCuentaBen = ConversionHelper.stringToByteArray(System.Convert.ToString(dataValue.OpCuentaBen), ConversionHelper.ByteOrder.BIG_ENDIAN);

                        conversiones.TramaCompleta =
                            conversiones.OpFechaOper
                            .Concat(conversiones.InsClaveOrd)
                            .Concat(conversiones.InsClaveBen)
                            .Concat(conversiones.OpCveRastreo)
                            .Concat(conversiones.OpMonto)
                            .Concat(conversiones.OpCuentaOrd)
                            .Concat(conversiones.OpCuentaBen)
                            .ToArray();

                        break;

                    // =========================================
                    // Participante a Tercero
                    // =========================================
                    case 2:
                        Config.EscribirLog("INFO|ComunSPID.ObtieneTrama|Se inicia proceso de conversión de bytes tipo participante a tercero");

                        conversiones.OpFechaOper = ConversionHelper.dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.InsClaveOrd = ConversionHelper.intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.InsClaveBen = ConversionHelper.intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpCveRastreo = ConversionHelper.stringToByteArray(System.Convert.ToString(dataValue.OpCveRastreo), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpMonto = ConversionHelper.moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ConversionHelper.ByteOrder.BIG_ENDIAN);
                        conversiones.OpCuentaBen = ConversionHelper.stringToByteArray(System.Convert.ToString(dataValue.OpCuentaBen), ConversionHelper.ByteOrder.BIG_ENDIAN);

                        conversiones.TramaCompleta =
                            conversiones.OpFechaOper
                            .Concat(conversiones.InsClaveOrd)
                            .Concat(conversiones.InsClaveBen)
                            .Concat(conversiones.OpCveRastreo)
                            .Concat(conversiones.OpMonto)
                            .Concat(conversiones.OpCuentaBen)
                            .ToArray();

                        break;

                    // =========================================
                    // ERROR
                    // =========================================
                    default:
                        throw new Exception($"No existe definición para pago SPEI del tipo de pago {tipoPago}");
                }

                LogueoDatosTramaSPID(conversiones);
            }
            catch (Exception ex)
            {
                throw new Exception($"ERROR|ComunSPID.ObtieneTrama|Se presenta exception: {ex}");
            }

            return conversiones;
        }

        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>
        public void LogueoDatosTrama(OrdenPagoByteArray datos)
        {
            Config.EscribirLog("INFO|ComunSPID.GeneraFirmaDigital|Se obtienen bytes de cada tipo de dato");

            if (datos?.InsClaveBen != null && datos.InsClaveBen.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|InsClaveBen {MuestraTrama(datos.InsClaveBen)}");

            if (datos?.InsClaveOrd != null && datos.InsClaveOrd.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|InsClaveOrd {MuestraTrama(datos.InsClaveOrd)}");

            if (datos?.OpCuentaBen != null && datos.OpCuentaBen.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpCuentaBen {MuestraTrama(datos.OpCuentaBen)}");

            if (datos?.OpCuentaBen2 != null && datos.OpCuentaBen2.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpCuentaBen2 {MuestraTrama(datos.OpCuentaBen2)}");

            if (datos?.OpCuentaOrd != null && datos.OpCuentaOrd.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpCuentaOrd {MuestraTrama(datos.OpCuentaOrd)}");

            if (datos?.OpCveRastreo != null && datos.OpCveRastreo.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpCveRastreo {MuestraTrama(datos.OpCveRastreo)}");

            if (datos?.OpFechaOper != null && datos.OpFechaOper.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpFechaOper {MuestraTrama(datos.OpFechaOper)}");

            if (datos?.OpMonto != null && datos.OpMonto.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpMonto {MuestraTrama(datos.OpMonto)}");

            var opCuentaPartOrd = datos?.opCuentaPartOrd as byte[];
            if (opCuentaPartOrd != null && opCuentaPartOrd.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|opCuentaPartOrd {MuestraTrama(opCuentaPartOrd)}");

            var opCuentaEmiRem = datos?.opCuentaEmiRem as byte[];
            if (opCuentaEmiRem != null && opCuentaEmiRem.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|opCuentaEmiRem {MuestraTrama(opCuentaEmiRem)}");

            if (datos?.TramaCompleta != null && datos.TramaCompleta.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|La trama armada es {MuestraTrama(datos.TramaCompleta)}");
        }

        private string MuestraTrama(byte[] array)
        {
            return array == null ? string.Empty : BitConverter.ToString(array);
        }

        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>
        public void LogueoDatosTramaSPID(OrdenPagoByteArray datos)
        {
            Config.EscribirLog("INFO|ComunSPID.GeneraFirmaDigital|Se obtienen bytes de cada tipo de dato");

            if (datos?.InsClaveBen != null && datos.InsClaveBen.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|InsClaveBen {MuestraTrama(datos.InsClaveBen)}");

            if (datos?.InsClaveOrd != null && datos.InsClaveOrd.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|InsClaveOrd {MuestraTrama(datos.InsClaveOrd)}");

            if (datos?.OpCuentaBen != null && datos.OpCuentaBen.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpCuentaBen {MuestraTrama(datos.OpCuentaBen)}");

            if (datos?.OpCuentaBen2 != null && datos.OpCuentaBen2.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpCuentaBen2 {MuestraTrama(datos.OpCuentaBen2)}");

            if (datos?.OpCuentaOrd != null && datos.OpCuentaOrd.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpCuentaOrd {MuestraTrama(datos.OpCuentaOrd)}");

            if (datos?.OpCveRastreo != null && datos.OpCveRastreo.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpCveRastreo {MuestraTrama(datos.OpCveRastreo)}");

            if (datos?.OpFechaOper != null && datos.OpFechaOper.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpFechaOper {MuestraTrama(datos.OpFechaOper)}");

            if (datos?.OpMonto != null && datos.OpMonto.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|OpMonto {MuestraTrama(datos.OpMonto)}");

            if (datos?.TramaCompleta != null && datos.TramaCompleta.Length > 0)
                Config.EscribirLog($"INFO|ComunSPID.GeneraFirmaDigital|La trama armada es {MuestraTrama(datos.TramaCompleta)}");
        }

    }
}

using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.FirmaElectronica
{
    /// <summary>
    /// Representa Metodos Firma.
    /// </summary>
    public class MetodosFirma : IDisposable
    {
        private readonly string _rutaPfx;
        private readonly string _password;
        private readonly string _serialKarpay;

        private readonly X509Certificate2 _certificado;
        private readonly RSA _rsaPrivada;
        private readonly RSA _rsaPublica;
/// <summary>
/// Inicializa una nueva instancia de la clase MetodosFirma.
/// </summary>

        public MetodosFirma(ConfiguracionModelo config)
        {
            if (string.IsNullOrWhiteSpace(config.RutaPFX))
                /// <summary>
                /// Ejecuta la operaciÃ³n ArgumentException.
                /// </summary>
                throw new ArgumentException(nameof(config.RutaPFX));

            _rutaPfx = config.RutaPFX;
            _password = config.SerialPassword;
            _serialKarpay = config.SerialKarpay;

            _certificado = new X509Certificate2(
                config.RutaPFX,
                config.SerialPassword
            );

            Config.EscribirLog($"INFO|MetodosFirma.GenerateSign|Nombre del archivo p12 : {_rutaPfx}");

            if (!_certificado.HasPrivateKey)
                /// <summary>
                /// Ejecuta la operaciÃ³n CryptographicException.
                /// </summary>
                throw new CryptographicException("El certificado no contiene llave privada.");

            _rsaPrivada = _certificado.GetRSAPrivateKey();
            _rsaPublica = _certificado.GetRSAPublicKey();

            Config.EscribirLog("INFO|MetodosFirma.GenerateSign|Se obtienen las llaves pública y privada");
        }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public string GenerateSign(byte[] data)
        {
            string resultBase64 = string.Empty;

            try
            {
                if (data == null || data.Length == 0)
                    /// <summary>
                    /// Ejecuta la operaciÃ³n ArgumentException.
                    /// </summary>
                    throw new ArgumentException(nameof(data));

                byte[] firma = _rsaPrivada.SignData(
                    data,
                    HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1
                );

                bool esValida = _rsaPublica != null && _rsaPublica.VerifyData(
                    data,
                    firma,
                    HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1
                );

                if (!esValida)
                {
                    Config.EscribirLog("WARN|MetodosFirma.GenerateSign|Validación legacy-first con llave pública del PFX falló. Se aplicará fallback de verificación.");
                    esValida = ConfirmaFirma(data, firma);
                }

                if (esValida)
                {
                    Config.EscribirLog("INFO|MetodosFirma.GenerateSign|Trama firmada exitosamente");
                    Config.EscribirLog("INFO|MetodosFirma.GenerateSign|Conversion Base 64");

                    resultBase64 = Convert.ToBase64String(firma, 0, firma.Length);

                    Config.EscribirLog($"INFO|MetodosFirma.GenerateSign|Resultado conversión Base 64 : {resultBase64}");
                }
            }
            catch (Exception ex)
            {
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("GenerateSign|Se presenta exception.", ex);
            }

            return resultBase64;
        }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>


        public bool ConfirmaFirma(byte[] data, byte[] firma)
        {
            try
            {
                if (data == null || firma == null)
                    return false;

                X509Certificate2 certificate = null;
                if (!string.IsNullOrWhiteSpace(_serialKarpay))
                {
                    string serialConfigurado = NormalizarSerialCertificado(_serialKarpay);
                    certificate = ObtenerCertificadoPorSerial(serialConfigurado);
                }
                else
                {
                    Config.EscribirLog("WARN|MetodosFirma.ConfirmaFirma|No se configuró serial Karpay. Se omite búsqueda en Store y se usa llave pública del PFX.");
                }

                if (certificate == null)
                {
                    Config.EscribirLog("WARN|MetodosFirma.ConfirmaFirma|No se encontró certificado por serial en LocalMachine/CurrentUser. Se usa llave pública del PFX cargado.");

                    if (_rsaPublica == null)
                        /// <summary>
                        /// Ejecuta la operaciÃ³n CryptographicException.
                        /// </summary>
                        throw new CryptographicException("No se encontró certificado en Store y el PFX no contiene llave pública RSA.");

                    return _rsaPublica.VerifyData(
                        data,
                        firma,
                        HashAlgorithmName.SHA256,
                        RSASignaturePadding.Pkcs1
                    );
                }

                using (RSA publicKey = certificate.GetRSAPublicKey())
                {
                    if (publicKey == null)
                        /// <summary>
                        /// Ejecuta la operaciÃ³n CryptographicException.
                        /// </summary>
                        throw new CryptographicException("El certificado del Store no contiene llave pública RSA.");

                    return publicKey.VerifyData(
                        data,
                        firma,
                        HashAlgorithmName.SHA256,
                        RSASignaturePadding.Pkcs1
                    );
                }
            }
            catch (Exception ex)
            {
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("ConfirmaFirma|Se presenta exception.", ex);
            }
        }

        private X509Certificate2 ObtenerCertificadoPorSerial(string serialConfigurado)
        {
            X509Certificate2 certificado = BuscarEnStore(StoreLocation.LocalMachine, serialConfigurado);
            if (certificado != null)
                return certificado;
/// <summary>
/// Ejecuta la operaciÃ³n BuscarEnStore.
/// </summary>

            return BuscarEnStore(StoreLocation.CurrentUser, serialConfigurado);
        }

        private static X509Certificate2 BuscarEnStore(StoreLocation location, string serialConfigurado)
        {
            using (X509Store store = new X509Store(StoreName.My, location))
            {
                store.Open(OpenFlags.ReadOnly);

                X509Certificate2Collection encontrados = store.Certificates.Find(
                    X509FindType.FindBySerialNumber,
                    serialConfigurado,
                    false);

                if (encontrados != null && encontrados.Count > 0)
                    return encontrados[0];

                return store.Certificates
                    .OfType<X509Certificate2>()
                    .FirstOrDefault(c => string.Equals(
                        NormalizarSerialCertificado(c.SerialNumber),
                        serialConfigurado,
                        StringComparison.OrdinalIgnoreCase));
            }
        }

        private static string NormalizarSerialCertificado(string serial)
        {
            if (string.IsNullOrWhiteSpace(serial))
                return string.Empty;

            return serial
                .Replace("0x", string.Empty)
                .Replace("0X", string.Empty)
                .Replace("\"", string.Empty)
                .Replace(" ", string.Empty)
                .Replace("\u200E", string.Empty)
                .Replace("\u200F", string.Empty)
                .Replace("\r", string.Empty)
                .Replace("\n", string.Empty)
                .Replace("\t", string.Empty)
                .Trim()
                .ToUpperInvariant();
        }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public bool CertificadoEncontrado()
        {
            try
            {
                X509Certificate2Collection collection = new X509Certificate2Collection();
                collection.Import(_rutaPfx, _password, X509KeyStorageFlags.DefaultKeySet);

                X509Certificate2 cert = collection[0];
                return cert.HasPrivateKey;
            }
            catch (Exception ex)
            {
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("CertificadoEncontrado|Se presenta exception.", ex);
            }
        }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public void Dispose()
        {
            _rsaPrivada?.Dispose();
            _rsaPublica?.Dispose();
            _certificado?.Dispose();
        }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa Orden Pago Byte Array.
    /// </summary>
    public class OrdenPagoByteArray
    {
        public byte[] OpFechaOper { get; set; }
        public byte[] InsClaveOrd { get; set; }
        public byte[] InsClaveBen { get; set; }
        public byte[] OpCveRastreo { get; set; }
        public byte[] OpMonto { get; set; }
        public byte[] OpCuentaOrd { get; set; }
        public byte[] OpCuentaBen { get; set; }
        public byte[] OpCuentaBen2 { get; set; }
        public byte[] TramaCompleta { get; set; }

        public object opCuentaPartOrd { get; set; }
        public object opCuentaEmiRem { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa Orden Pago Value.
    /// </summary>
    public class OrdenPagoValue
    {
        public object OpFechaOper { get; set; }
        public object InsClaveOrd { get; set; }
        public object InsClaveBen { get; set; }
        public object OpCveRastreo { get; set; }
        public object OpMonto { get; set; }
        public object OpCuentaOrd { get; set; }
        public object OpCuentaBen { get; set; }
        public object OpCuentaBen2 { get; set; }
        public object TpPago { get; set; }
        public object opCuentaPartOrd { get; set; }
        public object opCuentaEmiRem { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa Orden Pago Value.
    /// </summary>
    public class OrdenPagoValue
    {
        public object OpFechaOper { get; set; }
        public object InsClaveOrd { get; set; }
        public object InsClaveBen { get; set; }
        public object OpCveRastreo { get; set; }
        public object OpMonto { get; set; }
        public object OpCuentaOrd { get; set; }
        public object OpCuentaBen { get; set; }
        public object OpCuentaBen2 { get; set; }
        public object TpPago { get; set; }
        public object opCuentaPartOrd { get; set; }
        public object opCuentaEmiRem { get; set; }
    }
}

using System;

namespace Aplicacion.DTOs.CNT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class CntEntradaSalidaDto
    {
        public DateTime? EntFechaValor { get; set; }
        public string EntMonedaCuenta { get; set; }
        public decimal? EntImporte { get; set; }
    }
}

namespace Aplicacion.DTOs.CNT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoRegistrosDto
    {
        public int NumRegs { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.ComunSPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidFirmaDigitalDto
    {
        public int? NF_FECHA_EJECUCION { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public decimal? IMPORTE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string NF_FOLIO_UNICO { get; set; }
        public string OP_FIRMA_DIG { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidGeneraFirmaDto
    {
        public int? NF_FECHA_EJECUCION { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public decimal? IMPORTE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string NF_FOLIO_UNICO { get; set; }
    }

}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.ConfiguracionServicio
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class OperacionDTO
    {
        public string StrFolio;
        public string StrTipoOperacion;
        public string StrServicio { get; set; }
        public int PId { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.ENT_COAS
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasParticipanteSpeiDto
    {
        public decimal InsClave { get; set; }
        public decimal InsIndice { get; set; }
        public string InsNombre { get; set; }
        public string InsEdoRecep { get; set; }
        public string InsEdoConec { get; set; }
        public string InsEstado { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasRespuestaSpeiDto
    {
        public decimal? OpError { get; set; }
        public decimal? OpClave { get; set; }
        public decimal? OpFechaOper { get; set; }
        public decimal? OpFolio { get; set; }
        public string OpDescripcion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasResponderEntradaSpeiRequestDto
    {
        public decimal? OpClave { get; set; }
        public decimal? OpFechaOper { get; set; }
        public decimal? OpFolio { get; set; }
        public string OpCveRastreo { get; set; }
        public decimal? OpMonto { get; set; }
        public decimal? CdClave { get; set; }
        public string OpEstatus { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasInsertarFormatoSpeiDto
    {
        public decimal? OpClave { get; set; }
        public decimal? OpFechaOper { get; set; }
        public decimal? MeClave { get; set; }
        public decimal? OpFolio { get; set; }
        public decimal? InsClave { get; set; }
        public decimal? OpMonto { get; set; }
        public decimal? TpClave { get; set; }
        public string OpCveRastreo { get; set; }
        public string OpNomOrd { get; set; }
        public decimal? TcClaveOrd { get; set; }
        public string OpCuentaOrd { get; set; }
        public string OpRfcCurpOrd { get; set; }
        public string OpNomBen { get; set; }
        public decimal? TcClaveBen { get; set; }
        public string OpCuentaBen { get; set; }
        public string OpRfcCurpBen { get; set; }
        public string OpNomBen2 { get; set; }
        public decimal? TcClaveBen2 { get; set; }
        public string OpCuentaBen2 { get; set; }
        public string OpRfcCurpBen2 { get; set; }
        public string OpConceptoPago { get; set; }
        public string OpConceptoPag2 { get; set; }
        public decimal? OpIva { get; set; }
        public decimal? OpRefNumerica { get; set; }
        public string OpRefCobranza { get; set; }
        public string OpClavePago { get; set; }
        public decimal? ToClave { get; set; }
        public decimal? CdClave { get; set; }
        public string OpEstado { get; set; }
        public string OpCde { get; set; }
        public decimal? OpFechaCap { get; set; }
        public decimal? OpHoraCap { get; set; }
        public string OpTipoOrden { get; set; }
        public decimal? OpPrioridad { get; set; }
        public string UsuClave { get; set; }
        public decimal? OpFolioServidor { get; set; }
        public string OpRazonRechazo { get; set; }
        public string OpUsuAutoriza { get; set; }
        public decimal? OpHoraLiqBm { get; set; }
        public decimal? OpHoraLiqSist { get; set; }

        public string NfEstatus { get; set; }
        public string NfFolioUnico { get; set; }
        public string T24CustomerCode { get; set; }
        public string T24CompanyName { get; set; }
        public string T24BenAcctNo { get; set; }
        public string NfBancoBenef { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasInsertarEntradaSalidaDto
    {
        public string Referencia { get; set; }
        public decimal? NumCliente { get; set; }
        public decimal? Cdl { get; set; }
        public string Ordenante { get; set; }
        public decimal? Operacion { get; set; }
        public string OperacionDesc { get; set; }
        public string Tipo { get; set; }
        public string TipoDesc { get; set; }
        public decimal? Cuenta { get; set; }
        public string Nombre { get; set; }
        public string CuentaMoneda { get; set; }
        public decimal? Via { get; set; }
        public string ViaDesc { get; set; }
        public decimal? Importe { get; set; }
        public decimal? Saldo { get; set; }
        public string Comentarios { get; set; }
        public DateTime? FechaCaptura { get; set; }
        public DateTime? FechaValor { get; set; }
        public string Hora { get; set; }
        public string Capturista { get; set; }
        public decimal? Fad { get; set; }
        public string ReferenciaControl { get; set; }
        public int? ReferenciaControlCont { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasOrdenPagoDto
    {
        public string EstadoPendiente { get; set; }

        public int? OpClave { get; set; }
        public int? OpFechaOper { get; set; }
        public int? MeClave { get; set; }
        public int? OpFolio { get; set; }
        public int? InsClave { get; set; }
        public decimal? OpMonto { get; set; }
        public int? TpClave { get; set; }
        public string OpCveRastreo { get; set; }
        public string OpNomOrd { get; set; }
        public int? TcClaveOrd { get; set; }
        public string OpCuentaOrd { get; set; }
        public string OpRfcCurpOrd { get; set; }
        public string OpNomBen { get; set; }
        public int? TcClaveBen { get; set; }
        public string OpCuentaBen { get; set; }
        public string OpRfcCurpBen { get; set; }
        public string OpNomBen2 { get; set; }
        public int? TcClaveBen2 { get; set; }
        public string OpCuentaBen2 { get; set; }
        public string OpRfcCurpBen2 { get; set; }
        public string OpConceptoPago { get; set; }
        public string OpConceptoPag2 { get; set; }
        public decimal? OpIva { get; set; }
        public int? OpRefNumerica { get; set; }
        public string OpRefCobranza { get; set; }
        public string OpClavePago { get; set; }
        public int? ToClave { get; set; }
        public int? CdClave { get; set; }
        public string OpEstado { get; set; }
        public string OpCde { get; set; }
        public int? OpFechaCap { get; set; }
        public int? OpHoraCap { get; set; }
        public string OpTipoOrden { get; set; }
        public int? OpPrioridad { get; set; }
        public string UsuClave { get; set; }
        public int? OpFolioServidor { get; set; }
        public string OpRazonRechazo { get; set; }
        public string OpUsuAutoriza { get; set; }
        public string OpTopologia { get; set; }
        public int? ErrClave { get; set; }
        public string OpExporta { get; set; }
        public string OpCargoAbono { get; set; }
        public string OpAfectaBan { get; set; }
        public string OpDuplicado { get; set; }
        public int? OpHoraLiqBm { get; set; }

        // Columnas adicionales del SELECT legacy no copiadas al arreglo laValoresFormato(0..45),
        // pero sí presentes en el SELECT original.
        public int? OpHoraLiqSist { get; set; }
        public int? HoraCargoAbono { get; set; }
        public int? AhArClave { get; set; }
        public string OpAhEstado { get; set; }
        public string ChkFolioHost { get; set; }
        public string OpCuentaDev { get; set; }
        public string OpAplicaVos { get; set; }
        public string OpDepurado { get; set; }
        public string OpProcesaXml { get; set; }
        public int? OrdenesHostOpClave { get; set; }
        public string OpCveRastreoHex { get; set; }
        public string OpEdoHost { get; set; }
        public string OpIdMensaje { get; set; }
        public int? OpHoraHost { get; set; }
        public string OpEdoAcuse { get; set; }
        public int? OpHoraAcuse { get; set; }
        public string OpIdAcuse { get; set; }
        public int? OpPaqFolio { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasConciliacionTomiDto
    {
        public int? OpFechaOper { get; set; }
        public decimal? OpMonto { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasConciliacionEntradaSalidaDto
    {
        public DateTime? EntFechaValor { get; set; }
        public string EntMonedaCuenta { get; set; }
        public decimal? EntImporte { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntCoasEntradaSpeiTomiDto
    {
        public double? OpMonto { get; set; }
    }

}

namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoRegistrosDto
    {
        public int NumRegs { get; set; }
    }
}
namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiAutoDevolucionDto
    {
        public int Clave { get; set; }
        public int CodigoDevolucion { get; set; }
    }
}
namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiCambioEstatusRespuestaDto
    {
        public decimal? op_error { get; set; }
        public decimal? op_clave { get; set; }
        public decimal? op_fecha_oper { get; set; }
        public decimal? op_folio { get; set; }
        public string op_descripcion { get; set; }
    }
}
namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiCuentaVostroDto
    {
        public string CuentaBeneficiario { get; set; }
    }
}
namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiDataDevolucionDto
    {
        public int? OpFechaOper { get; set; }
        public string OpCveRastreo { get; set; }
        public int? InsClave { get; set; }
        public double? OpMonto { get; set; }
    }
}
using System;

namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiEntradaSalidaConciliacionDto
    {
        public DateTime? EntFechaValor { get; set; }
        public string EntMonedaCuenta { get; set; }
        public double? EntImporte { get; set; }
    }
}
namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiNfEntradaSpeiDto
    {
        public long OpClave { get; set; }
        public int OpFechaOper { get; set; }
        public decimal OpMonto { get; set; }
        public string TpClave { get; set; }
        public string NfEstatus { get; set; }
        public string NfFolioUnico { get; set; }
    }
}
namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiOperacionRecibidaDto
    {
        public string op_estatus { get; set; }
        public decimal op_clave { get; set; }
        public decimal op_fecha_oper { get; set; }
        public decimal? me_clave { get; set; }
        public decimal? op_folio { get; set; }
        public decimal? ins_clave { get; set; }
        public decimal? op_monto { get; set; }
        public decimal? tp_clave { get; set; }
        public string op_cve_rastreo { get; set; }
        public string op_nom_ord { get; set; }
        public decimal? tc_clave_ord { get; set; }
        public string op_cuenta_ord { get; set; }
        public string op_rfc_curp_ord { get; set; }
        public string op_nom_ben { get; set; }
        public decimal? tc_clave_ben { get; set; }
        public string op_cuenta_ben { get; set; }
        public string op_rfc_curp_ben { get; set; }
        public string op_nom_ben2 { get; set; }
        public decimal? tc_clave_ben2 { get; set; }
        public string op_cuenta_ben2 { get; set; }
        public string op_rfc_curp_ben2 { get; set; }
        public string op_concepto_pago { get; set; }
        public string op_concepto_pag2 { get; set; }
        public decimal? op_iva { get; set; }
        public decimal? op_ref_numerica { get; set; }
        public string op_ref_cobranza { get; set; }
        public string op_clave_pago { get; set; }
        public decimal? to_clave { get; set; }
        public decimal? cd_clave { get; set; }
        public string op_estado { get; set; }
        public string op_cde { get; set; }
        public decimal? op_fecha_cap { get; set; }
        public decimal? op_hora_cap { get; set; }
        public string op_tipo_orden { get; set; }
        public decimal? op_prioridad { get; set; }
        public string usu_clave { get; set; }
        public decimal? op_folio_servidor { get; set; }
        public string op_razon_rechazo { get; set; }
        public string op_usu_autoriza { get; set; }
        public decimal? op_topologia { get; set; }
        public decimal? err_clave { get; set; }
        public decimal? op_exporta { get; set; }
        public string op_cargo_abono { get; set; }
        public decimal? op_afecta_ban { get; set; }
        public decimal? op_duplicado { get; set; }
        public decimal? op_hora_liq_bm { get; set; }
        public decimal? op_hora_liq_sist { get; set; }
        public decimal? hora_cargo_abono { get; set; }
        public decimal? ah_ar_clave { get; set; }
        public string op_ah_estado { get; set; }
        public decimal? chk_folio_host { get; set; }
        public string op_cuenta_dev { get; set; }
        public decimal? op_aplica_vos { get; set; }
        public decimal? op_depurado { get; set; }
        public decimal? op_procesa_xml { get; set; }
        public decimal? ordenes_host_op_clave { get; set; }
        public string op_cve_rastreoHEX { get; set; }
        public string op_edo_host { get; set; }
        public decimal? op_id_mensaje { get; set; }
        public decimal? op_hora_host { get; set; }
        public string op_edo_acuse { get; set; }
        public decimal? op_hora_acuse { get; set; }
        public decimal? op_id_acuse { get; set; }
        public decimal? op_paq_folio { get; set; }
        public string OP_FIRMA_DIG { get; set; }
        public string OP_CUENTA_PART_ORD { get; set; }
        public string OP_CUENTA_EMI_REM { get; set; }
    }
}
namespace Aplicacion.DTOs.ENT_SPEI
{

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiParticipanteDto
    {
        public int ins_clave { get; set; }
        public string ins_nombre { get; set; }
    }
}
namespace Aplicacion.DTOs.ENT_SPEI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpeiTomiConciliacionDto
    {
        public int? OpFechaOper { get; set; }
        public double? OpMonto { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.ENT_SPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidInsertarFormatoTomiDto
    {
        public long OpClave { get; set; }
        public int OpFechaOper { get; set; }
        public int MeClave { get; set; }
        public int OpFolio { get; set; }
        public long InsClave { get; set; }
        public decimal OpMonto { get; set; }
        public int TpClave { get; set; }

        public string OpCveRastreo { get; set; }

        public string OpNomOrd { get; set; }
        public int? TcClaveOrd { get; set; }
        public string OpCuentaOrd { get; set; }
        public string OpRfcCurpOrd { get; set; }

        public string OpNomBen { get; set; }
        public int? TcClaveBen { get; set; }
        public string OpCuentaBen { get; set; }
        public string OpRfcCurpBen { get; set; }

        public string OpNomBen2 { get; set; }
        public int? TcClaveBen2 { get; set; }
        public string OpCuentaBen2 { get; set; }
        public string OpRfcCurpBen2 { get; set; }

        public string OpConceptoPago { get; set; }
        public string OpConceptoPag2 { get; set; }
        public decimal? OpIva { get; set; }

        public string OpRefNumerica { get; set; }
        public string OpRefCobranza { get; set; }
        public string OpClavePago { get; set; }

        public int? ToClave { get; set; }
        public int? CdClave { get; set; }

        public string OpEstado { get; set; }
        public string OpCde { get; set; }

        public int OpFechaCap { get; set; }
        public int OpHoraCap { get; set; }

        public string OpTipoOrden { get; set; }
        public int OpPrioridad { get; set; }

        public string UsuClave { get; set; }
        public long? OpFolioServidor { get; set; }

        public string OpRazonRechazo { get; set; }
        public string OpUsuAutoriza { get; set; }

        public int? OpHoraLiqBm { get; set; }
        public long? OpHoraLiqSist { get; set; }

        public string NfEstatus { get; set; }
        public string NfFolioUnico { get; set; }

        public string T24CustomerCode { get; set; }
        public string T24CompanyName { get; set; }
        public string T24BenAcctNo { get; set; }

        public string NfBancoBenef { get; set; }
        public int NfFechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class T24InformacionCuentaDto
    {
        public string Status { get; set; }
        public string Currency { get; set; }
        public string Category { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidOperacionRecibidaDto
    {
        public decimal OpClave { get; set; }
        public decimal OpFechaOper { get; set; }
        public decimal MeClave { get; set; }
        public decimal OpFolio { get; set; }
        public decimal InsClave { get; set; }
        public decimal OpMonto { get; set; }
        public decimal TpClave { get; set; }

        public string OpCveRastreo { get; set; }

        public string OpNomOrd { get; set; }
        public decimal? TcClaveOrd { get; set; }
        public string OpCuentaOrd { get; set; }
        public string OpRfcCurpOrd { get; set; }

        public string OpNomBen { get; set; }
        public decimal? TcClaveBen { get; set; }
        public string OpCuentaBen { get; set; }
        public string OpRfcCurpBen { get; set; }

        public string OpNomBen2 { get; set; }
        public decimal? TcClaveBen2 { get; set; }
        public string OpCuentaBen2 { get; set; }
        public string OpRfcCurpBen2 { get; set; }

        public string OpConceptoPago { get; set; }
        public string OpConceptoPag2 { get; set; }
        public decimal? OpIva { get; set; }

        public string OpRefNumerica { get; set; }
        public string OpRefCobranza { get; set; }
        public string OpClavePago { get; set; }

        public decimal? ToClave { get; set; }
        public decimal? CdClave { get; set; }

        public string OpEstado { get; set; }
        public string OpCde { get; set; }

        public decimal OpFechaCap { get; set; }
        public decimal OpHoraCap { get; set; }

        public string OpTipoOrden { get; set; }
        public decimal OpPrioridad { get; set; }

        public string UsuClave { get; set; }
        public decimal? OpFolioServidor { get; set; }

        public string OpRazonRechazo { get; set; }
        public string OpUsuAutoriza { get; set; }

        public decimal? OpHoraLiqBm { get; set; }
        public decimal? OpHoraLiqSist { get; set; }

        public string OpFirmaDig { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidParticipanteDto
    {
        public decimal InsClave { get; set; }
        public string InsNombre { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidConciliacionTomiDto
    {
        public decimal OpFechaOper { get; set; }
        public decimal OpMonto { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidConciliacionEntradaSalidaDto
    {
        public DateTime? EntFechaValor { get; set; }
        public string EntMonedaCuenta { get; set; }
        public decimal EntImporte { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoRegistrosDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ResultadoSalidaDto
    {
        public short Salida { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidEntradaSpidParaEntradaSalidaDto
    {
        public decimal OpMonto { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidInsertarEntradaSalidaDto
    {
        public string Referencia { get; set; }            
        public long NumCliente { get; set; }              
        public long Cdl { get; set; }                     

        public string Ordenante { get; set; }             

        public long Operacion { get; set; }               
        public string OperacionDesc { get; set; }         

        public string Tipo { get; set; }                  
        public string TipoDesc { get; set; }              

        public long? Cuenta { get; set; }                 
        public string Nombre { get; set; }                
        public string CuentaMoneda { get; set; }          

        public long Via { get; set; }                     
        public string ViaDesc { get; set; }               

        public decimal Importe { get; set; }              
        public decimal Saldo { get; set; }                

        public string Comentarios { get; set; }           

        public DateTime? FechaCaptura { get; set; }       
        public DateTime? FechaValor { get; set; }         

        public string Hora { get; set; }                  
        public string Capturista { get; set; }            

        public long Fad { get; set; }                     
        public string ReferenciaControl { get; set; }     
        public int? ReferenciaControlCont { get; set; }   
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidCambioEstatusResultadoDto
    {
        public int Codigo { get; set; }
        public string Mensaje { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidCambiarEstatusRecibidaDto
    {
        public long OpClave { get; set; }              
        public int OpFechaOper { get; set; }           
        public int OpFolio { get; set; }               
        public string OpCveRastreo { get; set; }       
        public decimal OpMonto { get; set; }           
        public int CausaDevolucion { get; set; }       
        public string OpEstatus { get; set; }          
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidDatosDevolucionDto
    {
        public int OpFechaOper { get; set; }           
        public string OpCveRastreo { get; set; }       
        public long InsClave { get; set; }             
        public decimal OpMonto { get; set; }           
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntSpidDevolverEntradaDto
    {
        public long OpClave { get; set; }              
        public int CausaDevolucion { get; set; }       
        public string OpFirmaDigital { get; set; }     
    }

}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.ENT_TEF
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntTefParticipanteDto
    {
        public int ins_clave { get; set; }
        public string ins_nombre { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntTefRecibidaDto
    {
        public long IDOperacion { get; set; }
        public int FechaProceso { get; set; }
        public string BancoPresentador { get; set; }
        public string BancoReceptor { get; set; }
        public decimal Importe { get; set; }
        public string Divisa { get; set; }
        public int FechaAplicacion { get; set; }
        public int FechaInicial { get; set; }
        public string ClaveRastreo { get; set; }
        public string CodigoOperacion { get; set; }
        public string TipoOperacion { get; set; }
        public string TipoCuentaOrdenante { get; set; }
        public string CuentaOrdenante { get; set; }
        public string NombreOrdenante { get; set; }
        public string RFCOrdenante { get; set; }
        public int RefNumOrdenante { get; set; }
        public string RefLeyendaOrdenante { get; set; }
        public string TipoCuentaReceptor { get; set; }
        public string CuentaReceptor { get; set; }
        public string NombreReceptor { get; set; }
        public string RFCReceptor { get; set; }
        public int NumCorte { get; set; }
        public int MotivoDevolucion { get; set; }
        public int? NumeroCarga { get; set; }
        public int? SecuenciaCarga { get; set; }
        public int? ErrorEnCarga { get; set; }
        public string SolicitudConfirmacion { get; set; }
        public string EsDevolucionExtemporanea { get; set; }
        public string EstadoBTMUM { get; set; }
        public string EstadoSistemaTEF { get; set; }
        public string ReferenciaConfirmacion { get; set; }
        public string EsDevolucionRecuperada { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntTefCambiarEstatusResultadoDto
    {
        public int Codigo { get; set; }
        public string Mensaje { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntTefInsertarFormatoTomiDto
    {
        public long IDOperacion { get; set; }
        public int FechaProceso { get; set; }
        public string BancoPresentador { get; set; }
        public string BancoReceptor { get; set; }
        public decimal Importe { get; set; }
        public string Divisa { get; set; }
        public int FechaAplicacion { get; set; }
        public int FechaInicial { get; set; }
        public string ClaveRastreo { get; set; }
        public string CodigoOperacion { get; set; }
        public string TipoOperacion { get; set; }
        public string TipoCuentaOrdenante { get; set; }
        public string CuentaOrdenante { get; set; }
        public string NombreOrdenante { get; set; }
        public string RFCOrdenante { get; set; }
        public int RefNumOrdenante { get; set; }
        public string RefLeyendaOrdenante { get; set; }
        public string TipoCuentaReceptor { get; set; }
        public string CuentaReceptor { get; set; }
        public string NombreReceptor { get; set; }
        public string RFCReceptor { get; set; }
        public int NumCorte { get; set; }
        public int MotivoDevolucion { get; set; }
        public int? NumeroCarga { get; set; }
        public int? SecuenciaCarga { get; set; }
        public int? ErrorEnCarga { get; set; }
        public string SolicitudConfirmacion { get; set; }
        public string EsDevolucionExtemporanea { get; set; }
        public string EstadoBTMUM { get; set; }
        public string EstadoSistemaTEF { get; set; }
        public string ReferenciaConfirmacion { get; set; }
        public string EsDevolucionRecuperada { get; set; }
        public string NF_ESTATUS { get; set; }
        public decimal? T24_DEBIT_AMOUNT { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_COMPANY_NAME { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string T24_CREDIT_ACCT_NO { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string T24_CREDIT_THEIR_REF { get; set; }
        public string T24_PAYMENT_DETAILS { get; set; }
        public string T24_SATEL_SYS_REF { get; set; }
        public string T24_INSTRUCTIONS { get; set; }
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public int? NF_FECHA_EJECUCION { get; set; }
        public string NF_BEN_CUST_NAME { get; set; }
        public string NF_TIPO_OPERACION { get; set; }
        public string NF_CODIGO_OPERACION { get; set; }
        public int? NF_FECHA_APLICACION { get; set; }
        public int? NF_FECHA_CARGO { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntTefEntradaSalidaDto
    {
        public string Referencia { get; set; }
        public decimal Num_Cliente { get; set; }
        public decimal CDL { get; set; }
        public string Ordenante { get; set; }
        public decimal Operacion { get; set; }
        public string OperacionDesc { get; set; }
        public string Tipo { get; set; }
        public string TipoDesc { get; set; }
        public decimal Cuenta { get; set; }
        public string Nombre { get; set; }
        public string CuentaMoneda { get; set; }
        public decimal Via { get; set; }
        public string ViaDesc { get; set; }
        public decimal Importe { get; set; }
        public decimal Saldo { get; set; }
        public string Comentarios { get; set; }
        public DateTime Fecha_Captura { get; set; }
        public DateTime FechaValor { get; set; }
        public string Hora { get; set; }
        public string Capturista { get; set; }
        public decimal Fad { get; set; }
        public string Referencia_Control { get; set; }
        public int Referencia_Control_Cont { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntTefFormatoTomiDto
    {
        public long IDOperacion { get; set; }
        public int FechaAplicacion { get; set; }
        public decimal Importe { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntTefConciliacionTomiDto
    {
        public int FechaAplicacion { get; set; }
        public decimal Importe { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class EntTefConciliacionEntradaSalidaDto
    {
        public DateTime? Ent_FechaValor { get; set; }
        public string Ent_Moneda_Cuenta { get; set; }
        public decimal Ent_Importe { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ExisteRegistroDto
    {
        public int NumRegs { get; set; }
    }

}
sing System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoRegistrosDto
    {
        public int NumRegs { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ForexStatusDto
    {
        public string StatusOperacion { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FormatoFxdCmsDto
    {
        public long IdFormato { get; set; }                  
        public decimal? Cdl { get; set; }                    
        public long? IdCuenta { get; set; }                  
        public DateTime FechaCaptura { get; set; }           
        public string IdUsuarioCaptura { get; set; }         
        public DateTime? FechaAutorizacion { get; set; }     
        public string IdUsuarioAutorizo { get; set; }        
        public decimal MontoOperacion { get; set; }          
        public string IdPlantilla { get; set; }              
        public long? IdFormatoPlantilla { get; set; }        
        public string ContenidoFormato { get; set; }         
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdEntradaSalidaDto
    {
        public DateTime? EntFechaValor { get; set; }   // smalldatetime
        public string EntMonedaCuenta { get; set; }    // nvarchar(3)
        public decimal? EntImporte { get; set; }       // money
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdGphDatosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdH2hDatosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TombinFxdConciliacionDto
    {
        public string NfForexRelacionado { get; set; }  
        public string T24DebitCurrency { get; set; }    
        public int? NfFechaEjecucion { get; set; }       
        public decimal? T24DebitAmount { get; set; } 
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TombinFxdEntradaSalidaDto
    {
        public string NfFolioUnico { get; set; }         
        public string T24CustomerCode { get; set; }      
        public string T24CompanyName { get; set; }       
        public string NfForexRelacionado { get; set; }   
        public string T24DebitAcctNo { get; set; }       
        public string NfNombreCuenta { get; set; }       
        public string NfMonedaCuenta { get; set; }       
        public decimal? T24DebitAmount { get; set; }     
        public string T24DebitTheirRef { get; set; }     
        public int? NfFechaEjecucion { get; set; }       
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXD_Local
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdLocalEsPacs008Dto
    {
        public int EsPacs008 { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdLocalDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdLocalDatosComisionDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public decimal? T24DebitAmount { get; set; }
        public string NfEstatus { get; set; }
        public string NfUsuarioAutoriza { get; set; }
        public string T24DebitCurrency { get; set; }
        public string T24DebitAcctNo { get; set; }
        public string NfBancoBenef { get; set; }
        public string NfForexRelacionado { get; set; }
        public string T24BenAcctNo { get; set; }
        public string NfOpClaveSpei { get; set; }
        public string NfOpFechaOperSpei { get; set; }
        public string NfOpFolioSpei { get; set; }
        public string T24CustomerCode { get; set; }
        public string NfCharge { get; set; }
        public string T24DebitTheirRef { get; set; }
        public int EsSwiftIn { get; set; }
        public string NfVariablesXml { get; set; }
        public int EsSwiftInMx { get; set; }
        public string NfVariablesXmlMx { get; set; }
        public string T24DetailCharge { get; set; }
        public string CommissionType { get; set; }
        public string CommissionCcy { get; set; }
        public decimal? CommissionAmount { get; set; }
        public decimal? CommissionAmountTax { get; set; }
        public decimal? CommissionAmountTot { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdLocalEsBancoDto
    {
        public int EsBanco { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdLocalCommissionTypeT24Dto
    {
        public string ComType { get; set; }
        public string Currency { get; set; }
        public decimal? FlatAmt { get; set; }
        public decimal? MidRevalRate { get; set; }
    }

}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ResultadoSalidaDto
    {
        public int Salida { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiFormatoCmsDto
    {
        public long id_Formato { get; set; }
        public long id_Cliente { get; set; }
        public string id_UsuarioCaptura { get; set; }
        public long? id_Cuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string id_UsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXML_Det { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public DateTime? Fecha_Autoriza { get; set; }
        public string id_UsuarioAutorizo_dos { get; set; }
        public DateTime? FechaAutorizacion_dos { get; set; }
        public string id_Plantilla { get; set; }
        public long? id_Autorizaciones { get; set; }
        public int? id_Autorizacion_uno { get; set; }
        public int? id_Autorizacion_dos { get; set; }
        public long? Estilo { get; set; }
        public string Autorizacion_TOMITAS { get; set; }
        public long? id_Formato_Plantilla { get; set; }
        public string id_Beneficiario { get; set; }
        public string id_CodigoMensaje { get; set; }
        public decimal? cdl { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiClienteRfcCmsDto
    {
        public string RFC { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiActualizarEstatusFormatoAceptadoCmsDto
    {
        public long IdFormato { get; set; }
        public string Mensaje { get; set; }
        public string Estatus { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiForexEstatusTomiDto
    {
        public string STATUSOPERACION { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiNostroCuentaTomiDto
    {
        public string NostroAccountNumber { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiExisteRegistroTomiDto
    {
        public int num_regs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiTombinNfFxiTomiDto
    {
        public string NF_TIPO { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_COMPANY_NAME { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_ACCOUNT_OFFICER { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public DateTime? NF_FECHA_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public string NF_USUARIO_CONFIRMA { get; set; }
        public DateTime? NF_FECHA_CONFIRMA { get; set; }
        public string T24_TRANSACTION_TYPE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public decimal? T24_DEBIT_AMOUNT { get; set; }
        public DateTime? T24_PROCESSING_DATE { get; set; }
        public string T24_ORDERING_CUST { get; set; }
        public string T24_CREDIT_ACCT_NO { get; set; }
        public string T24_CREDIT_CURRENCY { get; set; }
        public string T24_CREDIT_THEIR_REF { get; set; }
        public string T24_PAYMENT_DETAILS { get; set; }
        public string T24_COMMISSION_CODE { get; set; }
        public string T24_COMMISSION_TYPE { get; set; }
        public string T24_CHARGE_CODE { get; set; }
        public string T24_CHARGE_TYPE { get; set; }
        public string T24_CHARGES_ACCT_NO { get; set; }
        public string T24_PROFIT_CENTRE_DEPT { get; set; }
        public string T24_SATEL_SYS_REF { get; set; }
        public string T24_CUST_REF_1 { get; set; }
        public string T24_CUST_REF_2 { get; set; }
        public string T24_CUST_REF_3 { get; set; }
        public string T24_CUST_REF_4 { get; set; }
        public string T24_CUST_REF_5 { get; set; }
        public string T24_INSTRCTN_CODE { get; set; }
        public string T24_TIME_IND { get; set; }
        public string T24_ACCT_WITH_BANK_ACC { get; set; }
        public string T24_INTERMED_BANK_ACC { get; set; }
        public string T24_CREDIT_VALUE_DATE { get; set; }
        public string T24_BK_OPERATION_CODE { get; set; }
        public string T24_ORD_CUST_ACCT { get; set; }
        public string T24_ORDERING_BANK { get; set; }
        public string T24_ACCT_WITH_BANK { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string T24_BEN_CUSTOMER { get; set; }
        public string T24_INTERMED_BANK { get; set; }
        public string T24_BEN_OUR_CHARGES { get; set; }
        public string T24_DEBIT_VALUE_DATE { get; set; }
        public string T24_ORD_CUST_CODE { get; set; }
        public string T24_INSTRUCTIONS { get; set; }
        public decimal? MW_TRX_SEQUENCE { get; set; }
        public string MW_TRX_ID { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public bool? NF_CIERRE { get; set; }
        public string NF_ESTATUS_INTERNO { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public decimal? NF_MONTO_ENVIAR { get; set; }
        public string NF_RAZON { get; set; }
        public string NF_FOLIO_UNICO { get; set; }
        public bool? NF_IMPRESION_VOUCHER { get; set; }
        public string ID_PLANTILLA { get; set; }
        public long? ID_FORMATO_PLANTILLA { get; set; }
        public int? NF_INTERFASE_SPEI { get; set; }
        public decimal? NF_OPCLAVE_SPEI { get; set; }
        public decimal? NF_OPFECHAOPER_SPEI { get; set; }
        public decimal? NF_OPFOLIO_SPEI { get; set; }
        public string NF_RFC_ORDENANTE { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string NF_PAYMENT { get; set; }
        public string NF_CHARGE { get; set; }
        public string NF_FXD_COMMENT { get; set; }
        public string NF_SWIFT_BENEFICIARIO { get; set; }
        public string NF_BANCO_CORRESPONSAL { get; set; }
        public string NF_SWIFT_CORRESPONSAL { get; set; }
        public string NF_CUENTA_CORRESPONSAL { get; set; }
        public string NF_MENSAJE { get; set; }
        public string NF_COUNTRY { get; set; }
        public string NF_NOMBRE_CUENTA { get; set; }
        public string NF_MONEDA_CUENTA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public int? NF_FECHA_EJECUCION { get; set; }
        public string country_ben { get; set; }
        public string city_ben { get; set; }
        public string street_ben { get; set; }
        public string colony_ben { get; set; }
        public string building_number_ben { get; set; }
        public string post_code_ben { get; set; }
        public string OP_FIRMA_DIG { get; set; }
        public decimal? nf_monto_spei_envio { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiConciliacionNfFxiTomiDto
    {
        public string NF_FOREX_RELACIONADO { get; set; }
        public int? NF_FECHA_EJECUCION { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public decimal? T24_DEBIT_AMOUNT { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiEntradaSalidaConciliacionTomiDto
    {
        public DateTime? ent_fechaValor { get; set; }
        public string ent_moneda_cuenta { get; set; }
        public decimal? ent_importe { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiActualizarMaestroDatosGeneralesTomiDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public string FolioUnico { get; set; }
        public int FechaValor { get; set; }
        public string Moneda { get; set; }
        public decimal Importe { get; set; }
        public int NumCliente { get; set; }
        public string NombreCliente { get; set; }
        public string NumCuenta { get; set; }
        public string SubtipoOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiCancelarMaestroForexTomiDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public int FechaValor { get; set; }
        public string Moneda { get; set; }
        public decimal Importe { get; set; }
        public string ErrorDescripcion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiActualizarEstatusNfFxiTomiDto
    {
        public long Folio { get; set; }
        public string Estatus { get; set; }
        public string UsuarioConfirma { get; set; }
        public DateTime FechaConfirma { get; set; }
        public string Razon { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiInsertarFormatoFxiTomiDto
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_TIPO { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime NF_FECHA_CAPTURA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public decimal NF_MONTO_ENVIAR { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string NF_PAYMENT { get; set; }
        public string NF_CHARGE { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_FXD_COMMENT { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_RFC_ORDENANTE { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string NF_SWIFT_BENEFICIARIO { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_BANCO_CORRESPONSAL { get; set; }
        public string NF_SWIFT_CORRESPONSAL { get; set; }
        public string NF_CUENTA_CORRESPONSAL { get; set; }
        public string NF_MENSAJE { get; set; }
        public string NF_COUNTRY { get; set; }
        public string ID_PLANTILLA { get; set; }
        public long? ID_FORMATO_PLANTILLA { get; set; }
        public string COUNTRY_BEN { get; set; }
        public string city_ben { get; set; }
        public string street_ben { get; set; }
        public string colony_ben { get; set; }
        public string building_number_ben { get; set; }
        public string post_code_ben { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiInsertarEntradaSalidaTomiDto
    {
        public string Referencia { get; set; }
        public decimal Num_Cliente { get; set; }
        public decimal CDL { get; set; }
        public string Ordenante { get; set; }
        public decimal Operacion { get; set; }
        public string OperacionDesc { get; set; }
        public string Tipo { get; set; }
        public string TipoDesc { get; set; }
        public decimal Cuenta { get; set; }
        public string Nombre { get; set; }
        public string CuentaMoneda { get; set; }
        public decimal Via { get; set; }
        public string ViaDesc { get; set; }
        public decimal Importe { get; set; }
        public decimal Saldo { get; set; }
        public string Comentarios { get; set; }
        public DateTime Fecha_Captura { get; set; }
        public DateTime FechaValor { get; set; }
        public string Hora { get; set; }
        public string Capturista { get; set; }
        public decimal Fad { get; set; }
        public string Referencia_Control { get; set; }
        public int Referencia_Control_Cont { get; set; }
    }

}
namespace Aplicacion.DTOs.FXIGPH
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiGphDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public string NfNombreArchivoGph { get; set; }
        public string NfPmtInfId { get; set; }
        public long? NfIdArchivo { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXI_H2H
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxiH2HDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXI_Local
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FXI_LocalDto
    {
        /// <summary>
        /// DTO necesario para recuperar datos.
        /// </summary>
        public class FxiLocalDatosBasicosDto
        {
            public string NfFolioUnico { get; set; }
            public int? NfFechaEjecucion { get; set; }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtFormatoCmsDto
    {
        public long IdFormato { get; set; }
        public decimal? Cdl { get; set; }
        public long? IdCuenta { get; set; }
        public DateTime FechaCaptura { get; set; }
        public string IdUsuarioCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string IdUsuarioAutorizo { get; set; }
        public decimal MontoOperacion { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public string ContenidoFormato { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtActualizarEstatusFormatoRechazadoDto
    {
        public long IdFormato { get; set; }
        public string RazonRechazo { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtActualizarEstatusFormatoSimpleDto
    {
        public long IdFormato { get; set; }
        public string Mensaje { get; set; }
        public string Estatus { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtForexStatusTomiDto
    {
        public string StatusOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtActualizarDatosGeneralesPlanTrabajoDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public string FolioUnico { get; set; }
        public decimal FecValor { get; set; }
        public string CveMoneda { get; set; }
        public decimal Importe { get; set; }
        public int? NumCliente { get; set; }
        public string NombreCliente { get; set; }
        public string NumCuenta { get; set; }
        public string SubtipoOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtInsertarFormatoFxtTomiDto
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_TIPO { get; set; }
        public int NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime NF_FECHA_CAPTURA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public double T24_DEBIT_AMOUNT { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public decimal NF_MONTO_ENVIAR { get; set; }
        public string ID_PLANTILLA { get; set; }
        public long? ID_FORMATO_PLANTILLA { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtExisteRegistroDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtActualizarEstatusFxtRechazadoForexDto
    {
        public long NfFolio { get; set; }
        public string NfRazon { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtActualizarEstatusFxtConfirmacionDto
    {
        public long NfFolio { get; set; }
        public string NfEstatus { get; set; }
        public string NfUsuarioConfirma { get; set; }
        public DateTime NfFechaConfirma { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtCancelarPlanTrabajoPorForexDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public decimal FecValor { get; set; }
        public string CveMoneda { get; set; }
        public decimal Importe { get; set; }
        public string ErrDescripcion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtConciliacionTomiDto
    {
        public string NfForexRelacionado { get; set; }
        public string T24DebitCurrency { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public decimal? T24DebitAmount { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtMarcarFechaFuturaPlanTrabajoDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ResultadoSalidaDto
    {
        public int Salida { get; set; }
    }

}
namespace Aplicacion.DTOs.FXT_GPH
{

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtGphDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public string NfNombreArchivoGph { get; set; }
        public string NfPmtInfId { get; set; }
        public long? NfIdArchivo { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.FXT_H2H
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtH2HDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }
}
namespace Aplicacion.DTOs.FXT_Local
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxtLocalDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }
}
namespace Aplicacion.DTOs.FirmaElectronica
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ComunFXDFirmaDatosDto
    {
        public int NF_FECHA_EJECUCION { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public decimal IMPORTE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public string NF_FOLIO_UNICO { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.MFX
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MFXSolicitudForexTomiDto
    {
        public long IdAutorizacion { get; set; }
        public int Plazo { get; set; }
        public DateTime FechaValorReal { get; set; }
        public string FechaValorTexto { get; set; }

        public string Nombre { get; set; }
        public string Contraparte { get; set; }
        public long IdCliente { get; set; }
        public string Cdl { get; set; }

        public string DivisaAdquiere { get; set; }
        public decimal MontoAdquiere { get; set; }
        public double EcClienteReal { get; set; }

        public string DivisaEntrega { get; set; }
        public decimal MontoEntrega { get; set; }

        public string DivisaEntregaCliente { get; set; }
        public string CuentaEntregaCliente { get; set; }
        public decimal MontoEntregaCliente { get; set; }

        public string DivisaAdquiereCliente { get; set; }
        public string CuentaAdquiereCliente { get; set; }
        public decimal MontoAdquiereCliente { get; set; }

        public string Producto { get; set; }
        public string TipoOperacion { get; set; }

        public double EcCliente { get; set; }
        public double EcRate { get; set; }
        public double? CrossRate { get; set; }

        public string Status { get; set; }
        public string StatusOperacion { get; set; }
        public string Tipo { get; set; }
        public string CuentaDebito { get; set; }
        public string CuentaCredito { get; set; }

        public bool? FlgCuentaDebitoCms { get; set; }
        public bool? FlgCuentaCreditoCms { get; set; }

        public int? ValidacionSistema { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MFXForexConciliacionDto
    {
        public string FechaValorTexto { get; set; }

        public string DivisaAdquiereCliente { get; set; }
        public decimal MontoAdquiereCliente { get; set; }

        public string CuentaCredito { get; set; }

        public string DivisaEntregaCliente { get; set; }
        public decimal MontoEntregaCliente { get; set; }

        public string CuentaDebito { get; set; }

        public bool? FlgCuentaCreditoCms { get; set; }
        public bool? FlgCuentaDebitoCms { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MFXTomiConciliacionDto
    {
        public string NfAutorizacion { get; set; }

        public int? NfFechaEjecucion { get; set; }

        public string NfDivisaAdquiere { get; set; }

        public decimal? NfMontoAdquiere { get; set; }

        public string T24CreditAcctNo { get; set; }

        public string NfDivisaEntrega { get; set; }
        public string NfMontoEntrega { get; set; }

        public string T24DebitAcctNo { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MFXFormatoCmsDto
    {
        public long IdFormato { get; set; }

        public long IdCliente { get; set; }

        // LEFT JOIN contra cliente, puede venir null.
        public decimal? Cdl { get; set; }

        public long? IdCuenta { get; set; }

        public DateTime FechaCaptura { get; set; }

        public string IdUsuarioCaptura { get; set; }

        public DateTime? FechaAutorizacion { get; set; }

        public string IdUsuarioAutorizo { get; set; }

        public decimal MontoOperacion { get; set; }

        public string ContenidoFormato { get; set; }

        public string AutorizacionTomitas { get; set; }

        public string Estatus { get; set; }

        public long TipoFormato { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MFXCuentaCustomerT24Dto
    {
        public string AccountNumber { get; set; }
        public string Category { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MFXFormatoTomiInsertDto
    {
        public string NfFolioUnico { get; set; }

        public string NfTipo { get; set; }

        public long NfFolio { get; set; }

        public string T24CustomerCode { get; set; }

        public string T24DebitAcctNo { get; set; }

        public string T24CreditAcctNo { get; set; }

        public string NfEstatus { get; set; } = "PEND";

        public string NfUsuarioCaptura { get; set; }

        public string NfUsuarioAutoriza { get; set; }

        public DateTime NfFechaCaptura { get; set; }

        public DateTime? NfFechaAutoriza { get; set; }

        public decimal T24DebitAmount { get; set; }

        public string T24ShortName { get; set; }

        public string NfAutorizacion { get; set; }

        public string NfFechaEjecucion { get; set; }

        public string NfDivisaAdquiere { get; set; }

        public decimal NfMontoAdquiere { get; set; }

        public string NfEcRate { get; set; }

        public string NfCuentaCredito { get; set; }

        public string NfDivisaEntrega { get; set; }

        public string NfMontoEntrega { get; set; }

        public string NfCuentaDebito { get; set; }

        public string NfDescripcion { get; set; }
    }

}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.MercadoDinero
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MercadoDineroLiquidacionTomiDto
    {
        public string TipoOperacion { get; set; }              
        public string TipoLiquidacion { get; set; }            
        public string T24DebitAcctNo { get; set; }             
        public string T24CustomerCode { get; set; }            
        public string T24CompanyName { get; set; }             
        public string T24DebitCurrency { get; set; }           

        public decimal? T24DebitAmount { get; set; }           
        public string Sentido { get; set; }                    
        public string CalypsoId { get; set; }                  
        public string ExternalRef { get; set; }                

        public string NfEstatus { get; set; }                  
        public int? NfFechaEjecucion { get; set; }             

        public string DatoCalypso { get; set; }                
        public string DatosXml { get; set; }                   

        public string Familia { get; set; }                    
        public int? NfTipoPagoSpei { get; set; }               
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MercadoDineroComplementoPagoDto
    {
        public int? SpxxTipoPago { get; set; }          
        public decimal? SpxxCasfimBene { get; set; }    
        public string SpxxNomBanco { get; set; }        
        public string NomBeneficiario { get; set; }     
        public string SpxxTipoCuenta { get; set; }      
        public string SpxxCuentaBene { get; set; }      
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MercadoDineroDuplicidadLiquidacionDto
    {
        public long NfFolio { get; set; }      
        public string NfEstatus { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class MercadoDineroEstatusParDto
    {
        public string NfEstatus { get; set; }
    }

}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.NOM_FXD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdMaestroCmsDto
    {
        public string id_formato { get; set; }
        public string cdl { get; set; }
        public string id_cuenta { get; set; }
        public DateTime? fechaCaptura { get; set; }
        public string id_usuarioCaptura { get; set; }
        public DateTime? fechaAutorizacion { get; set; }
        public string id_usuarioAutorizo { get; set; }
        public decimal? montoOperacion { get; set; }
        public string id_plantilla { get; set; }
        public string id_formato_plantilla { get; set; }
        public string id_files_btmum { get; set; }
        public int? noPagos { get; set; }
        public string pagosDescripcion { get; set; }
        public int? noPagosProcesados { get; set; }
        public int? noPagosDevueltos { get; set; }
        public string contenidoFormato { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdDetalleCmsDto
    {
        public string id_formato { get; set; }
        public string cdl { get; set; }
        public string id_cuenta { get; set; }
        public DateTime? fechaCaptura { get; set; }
        public string id_usuarioCaptura { get; set; }
        public DateTime? fechaAutorizacion { get; set; }
        public string id_usuarioAutorizo { get; set; }
        public decimal? montoOperacion { get; set; }
        public string id_plantilla { get; set; }
        public string id_formato_plantilla { get; set; }
        public string id_files_btmum { get; set; }
        public int? nopagos { get; set; }
        public string pagosdescripcion { get; set; }
        public int? nopagosprocesados { get; set; }
        public int? nopagosdevueltos { get; set; }
        public string id_formato_nomina { get; set; }
        public string contenidoFormato { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdForexStatusDto
    {
        public string STATUSOPERACION { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdSumarizadoDetalleTomiDto
    {
        public string id_maestro { get; set; }
        public int? num_regs { get; set; }
        public decimal? imp_monto { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdConciliacionDetalleTomiDto
    {
        public decimal? T24_DEBIT_AMOUNT { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdEntradaSalidaConciliacionDto
    {
        public DateTime? ent_fechaValor { get; set; }
        public string ent_moneda_cuenta { get; set; }
        public decimal? ent_importe { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ResultadoSalidaDto
    {
        public int Salida { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdInsertarDetalleTomiDto
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_TIPO { get; set; }
        public decimal? NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime? NF_FECHA_CAPTURA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public decimal? T24_DEBIT_AMOUNT { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public int? NF_FECHA_EJECUCION { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public decimal? NF_MONTO_ENVIAR { get; set; }
        public string ID_PLANTILLA { get; set; }
        public decimal? ID_FORMATO_PLANTILLA { get; set; }
        public decimal? ID_FILES_BTMUM { get; set; }
        public decimal? NO_PAGOS { get; set; }
        public string PAGOSDESCRIPCION { get; set; }
        public decimal? NOPAGOSPROCESADOS { get; set; }
        public decimal? NOPAGOSDEVUELTOS { get; set; }
        public decimal? ID_FORMATO_NOMINA { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdInsertarMaestroTomiDto
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_TIPO { get; set; }
        public decimal? NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime? NF_FECHA_CAPTURA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public decimal? T24_DEBIT_AMOUNT { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public int? NF_FECHA_EJECUCION { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public decimal? NF_MONTO_ENVIAR { get; set; }
        public string ID_PLANTILLA { get; set; }
        public decimal? ID_FORMATO_PLANTILLA { get; set; }
        public decimal? ID_FILES_BTMUM { get; set; }
        public decimal? NO_PAGOS { get; set; }
        public string PAGOSDESCRIPCION { get; set; }
        public decimal? NOPAGOSPROCESADOS { get; set; }
        public decimal? NOPAGOSDEVUELTOS { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdNominaMaestroParaEntradaSalidaDto
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_COMPANY_NAME { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string NF_NOMBRE_CUENTA { get; set; }
        public string NF_MONEDA_CUENTA { get; set; }
        public decimal? T24_DEBIT_AMOUNT { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public int? NF_FECHA_EJECUCION { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class NomFxdEntradaSalidaMaestroDto
    {
        public string Referencia { get; set; }
        public int? Num_Cliente { get; set; }
        public decimal? CDL { get; set; }
        public string Ordenante { get; set; }
        public decimal? Operacion { get; set; }
        public string OperacionDesc { get; set; }
        public string Tipo { get; set; }
        public string TipoDesc { get; set; }
        public decimal? Cuenta { get; set; }
        public string Nombre { get; set; }
        public string CuentaMoneda { get; set; }
        public decimal? Via { get; set; }
        public string ViaDesc { get; set; }
        public decimal? Importe { get; set; }
        public decimal? Saldo { get; set; }
        public string Comentarios { get; set; }
        public DateTime? Fecha_Captura { get; set; }
        public DateTime? FechaValor { get; set; }
        public string Hora { get; set; }
        public string Capturista { get; set; }
        public decimal? Fad { get; set; }
        public string Referencia_Control { get; set; }
        public decimal? Referencia_Control_Cont { get; set; }
    }

}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoRegistrosDto
    {
        public int NumRegs { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ForexStatusDto
    {
        public string StatusOperacion { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class FxdEntradaSalidaDto
    {
        public DateTime? EntFechaValor { get; set; }  
        public string EntMonedaCuenta { get; set; }   
        public decimal? EntImporte { get; set; }    
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxdContenidoDto
    {
        public string NombreCliente { get; set; } = string.Empty;
        public string Moneda { get; set; } = string.Empty;   
        public string FechaEjecucion { get; set; } = string.Empty;  
        public string[] Valores { get; set; } = new string[19];
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxdDetalleCmsDto
    {
        public long IdFormato { get; set; }                 
        public decimal? Cdl { get; set; }                   
        public long? IdCuenta { get; set; }                 
        public DateTime FechaCaptura { get; set; }          
        public string IdUsuarioCaptura { get; set; }        
        public DateTime? FechaAutorizacion { get; set; }    
        public string IdUsuarioAutorizo { get; set; }       
        public decimal MontoOperacion { get; set; }         
        public string IdPlantilla { get; set; }             
        public long? IdFormatoPlantilla { get; set; }       
        public long IdFilesBtmum { get; set; }              
        public long NoPagos { get; set; }                   
        public string PagosDescripcion { get; set; }        
        public long NoPagosProcesados { get; set; }         
        public long NoPagosDevueltos { get; set; }          
        public long IdFormatoPap { get; set; }              
        public string Ref1 { get; set; }                    
        public string Ref2 { get; set; }                    
        public string Ref3 { get; set; }                    
        public string Ref4 { get; set; }                    
        public string Ref5 { get; set; }                    
        public string ContenidoFormato { get; set; }        
    }
}
using System;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxdMaestroCmsDto
    {
        public long IdFormato { get; set; }                 
        public decimal? Cdl { get; set; }                   
        public long? IdCuenta { get; set; }                 
        public DateTime FechaCaptura { get; set; }          
        public string IdUsuarioCaptura { get; set; }        
        public DateTime? FechaAutorizacion { get; set; }    
        public string IdUsuarioAutorizo { get; set; }       
        public decimal MontoOperacion { get; set; }         
        public string IdPlantilla { get; set; }             
        public long? IdFormatoPlantilla { get; set; }       
        public long IdFilesBtmum { get; set; }              
        public long NoPagos { get; set; }                   
        public string PagosDescripcion { get; set; }        
        public long NoPagosProcesados { get; set; }         
        public long NoPagosDevueltos { get; set; }          
        public string ContenidoFormato { get; set; }        
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PapFxdDetalleImporteDto
    {
        public decimal? T24DebitAmount { get; set; }  
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PapFxdDetalleTomiDto
    {
        public string NfFolioUnico { get; set; }       
        public string T24CustomerCode { get; set; }    
        public string T24CompanyName { get; set; }     
        public string T24DebitAcctNo { get; set; }     
        public string NfNombreCuenta { get; set; }     
        public string NfMonedaCuenta { get; set; }     
        public decimal? T24DebitAmount { get; set; }   
        public string T24DebitTheirRef { get; set; }   
        public int? NfFechaEjecucion { get; set; }     
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PapFxdSumatoriaDetalleDto
    {
        public long? IdMaestro { get; set; }      
        public int NumRegs { get; set; }         
        public decimal? ImpMonto { get; set; }     
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP_FXI
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiInsertarMaestroPapTomiDto
    {
        public string NfFolioUnico { get; set; }          
        public string NfTipo { get; set; }                
        public long NfFolio { get; set; }                 

        public string T24CustomerCode { get; set; }       
        public string T24DebitAcctNo { get; set; }        
        public string NfEstatus { get; set; }             

        public string NfUsuarioCaptura { get; set; }      
        public string NfUsuarioAutoriza { get; set; }     

        public DateTime? NfFechaCaptura { get; set; }     
        public DateTime? NfFechaAutoriza { get; set; }    

        public decimal? T24DebitAmount { get; set; }      

        public string T24ShortName { get; set; }          
        public string T24DebitCurrency { get; set; }      

        public string NfForexRelacionado { get; set; }    
        public decimal? NfMontoEnviar { get; set; }       

        public string T24DebitTheirRef { get; set; }      
        public string NfTipoCuenta { get; set; }          
        public string NfPayment { get; set; }             
        public string NfCharge { get; set; }              

        public int? NfFechaEjecucion { get; set; }        

        public string T24BenCustName { get; set; }        
        public string NfFxdComment { get; set; }          
        public string NfRfcBeneficiario { get; set; }     
        public string NfTasaRetencion { get; set; }       

        public string NfBancoBenef { get; set; }          
        public string NfSwiftBeneficiario { get; set; }   
        public string T24BenAcctNo { get; set; }          

        public string NfBancoCorresponsal { get; set; }   
        public string NfSwiftCorresponsal { get; set; }   
        public string NfCuentaCorresponsal { get; set; }  

        public string NfMensaje { get; set; }             
        public string NfCountry { get; set; }             

        public string IdPlantilla { get; set; }           
        public long? IdFormatoPlantilla { get; set; }     

        public long? IdFilesBtmum { get; set; }           
        public int? NoPagos { get; set; }                 
        public string PagosDescripcion { get; set; }      
        public int? NoPagosProcesados { get; set; }       
        public int? NoPagosDevueltos { get; set; }        
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiMaestroCmsDto
    {
        public long IdFormato { get; set; }
        public long IdCliente { get; set; }
        public string IdUsuarioCaptura { get; set; }
        public long? IdCuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string IdUsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXMLDet { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public DateTime? FechaAutoriza { get; set; }
        public string IdUsuarioAutorizoDos { get; set; }
        public DateTime? FechaAutorizacionDos { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdAutorizaciones { get; set; }
        public int? IdAutorizacionUno { get; set; }
        public int? IdAutorizacionDos { get; set; }
        public long? Estilo { get; set; }
        public string AutorizacionTomitas { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public string IdBeneficiario { get; set; }
        public long IdFilesBtmum { get; set; }
        public long NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public long NoPagosProcesados { get; set; }
        public long NoPagosDevueltos { get; set; }
        public string IdCanal { get; set; }

        public decimal? Cdl { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiSumarizadoDetalleTomiDto
    {
        public long? IdMaestro { get; set; }
        public int NumRegs { get; set; }
        public decimal? ImpMonto { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiDetalleTomiConciliacionDto
    {
        public decimal? T24DebitAmount { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiDetalleCmsDto
    {
        public long IdFormato { get; set; }
        public long IdCliente { get; set; }
        public string IdUsuarioCaptura { get; set; }
        public long? IdCuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string IdUsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXMLDet { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public DateTime? FechaAutoriza { get; set; }
        public string IdUsuarioAutorizoDos { get; set; }
        public DateTime? FechaAutorizacionDos { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdAutorizaciones { get; set; }
        public int? IdAutorizacionUno { get; set; }
        public int? IdAutorizacionDos { get; set; }
        public long? Estilo { get; set; }
        public long? AutorizacionTomitas { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public string IdBeneficiario { get; set; }
        public long IdFilesBtmum { get; set; }
        public long NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public long NoPagosProcesados { get; set; }
        public long NoPagosDevueltos { get; set; }
        public long IdFormatoPap { get; set; }
        public string Ref1 { get; set; }
        public string Ref2 { get; set; }
        public string Ref3 { get; set; }
        public string Ref4 { get; set; }
        public string Ref5 { get; set; }
        public string IdCanal { get; set; }

        public decimal? Cdl { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiEntradaSalidaConciliacionDto
    {
        public DateTime? EntFechaValor { get; set; }
        public string EntMonedaCuenta { get; set; }
        public decimal EntImporte { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiForexStatusDto
    {
        public string StatusOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ConteoDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ResultadoSalidaDto
    {
        public int Salida { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiInsertarDetallePapTomiDto
    {
        public string NfFolioUnico { get; set; }        
        public string NfTipo { get; set; }              
        public long NfFolio { get; set; }               

        public string T24CustomerCode { get; set; }     
        public string T24DebitAcctNo { get; set; }      
        public string NfEstatus { get; set; }           

        public string NfUsuarioCaptura { get; set; }    
        public string NfUsuarioAutoriza { get; set; }   

        public DateTime? NfFechaCaptura { get; set; }
        public DateTime? NfFechaAutoriza { get; set; }

        public decimal? T24DebitAmount { get; set; }    

        public string T24ShortName { get; set; }        
        public string T24DebitCurrency { get; set; }    

        public string NfForexRelacionado { get; set; }  
        public decimal? NfMontoEnviar { get; set; }     

        public string T24DebitTheirRef { get; set; }    
        public string NfTipoCuenta { get; set; }        
        public string NfPayment { get; set; }           
        public string NfCharge { get; set; }            

        public int? NfFechaEjecucion { get; set; }      

        public string T24BenCustName { get; set; }      
        public string NfFxdComment { get; set; }        
        public string NfRfcBeneficiario { get; set; }   
        public string NfTasaRetencion { get; set; }     

        public string NfBancoBenef { get; set; }        
        public string NfSwiftBeneficiario { get; set; } 
        public string T24BenAcctNo { get; set; }        

        public string NfBancoCorresponsal { get; set; } 
        public string NfSwiftCorresponsal { get; set; } 
        public string NfCuentaCorresponsal { get; set; }

        public string NfMensaje { get; set; }           
        public string NfCountry { get; set; }           

        public string IdPlantilla { get; set; }         
        public long? IdFormatoPlantilla { get; set; }   
        public long? IdFilesBtmum { get; set; }         

        public int? NoPagos { get; set; }               
        public string PagosDescripcion { get; set; }    
        public int? NoPagosProcesados { get; set; }     
        public int? NoPagosDevueltos { get; set; }      

        public long? IdFormatoPap { get; set; }         

        public string Ref1 { get; set; }                
        public string Ref2 { get; set; }                
        public string Ref3 { get; set; }                
        public string Ref4 { get; set; }                
        public string Ref5 { get; set; }                

        public string CountryBen { get; set; }          
        public string CityBen { get; set; }             
        public string StreetBen { get; set; }           
        public string ColonyBen { get; set; }           
        public string BuildingNumberBen { get; set; }   
        public string PostCodeBen { get; set; }         
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiDetalleTomiEntradaSalidaDto
    {
        public long NfFolio { get; set; }

        public string NfFolioUnico { get; set; }
        public string T24CustomerCode { get; set; }
        public string T24CompanyName { get; set; }

        public string T24DebitAcctNo { get; set; }
        public string T24DebitCurrency { get; set; }
        public decimal? T24DebitAmount { get; set; }

        public string NfForexRelacionado { get; set; }

        public string NfNombreCuenta { get; set; }
        public string NfMonedaCuenta { get; set; }

        public string NfMensaje { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFxiInsertarEntradaSalidaDto
    {
        public string Referencia { get; set; }          
        public int? NumCliente { get; set; }            
        public decimal? Cdl { get; set; }               
        public string Ordenante { get; set; }           

        public decimal? Operacion { get; set; }         
        public string OperacionDesc { get; set; }       

        public string Tipo { get; set; }                
        public string TipoDesc { get; set; }            

        public decimal? Cuenta { get; set; }            
        public string Nombre { get; set; }              
        public string CuentaMoneda { get; set; }        

        public decimal? Via { get; set; }               
        public string ViaDesc { get; set; }             

        public decimal? Importe { get; set; }           
        public decimal? Saldo { get; set; }             

        public string Comentarios { get; set; }         

        public DateTime? FechaCaptura { get; set; }     
        public DateTime? FechaValor { get; set; }       

        public string Hora { get; set; }                
        public string Capturista { get; set; }          

        public decimal? Fad { get; set; }               
        public string ReferenciaControl { get; set; }   
        public decimal? ReferenciaControlCont { get; set; }
    }

}
using System;

namespace Aplicacion.Interfaces.PaP_FXT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTFormatoPapMaestroCmsDto
    {
        public long IdFormato { get; set; }
        public long IdCliente { get; set; }
        public string IdUsuarioCaptura { get; set; }
        public long? IdCuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string IdUsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXMLDet { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public DateTime? FechaAutoriza { get; set; }
        public string IdUsuarioAutorizoDos { get; set; }
        public DateTime? FechaAutorizacionDos { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdAutorizaciones { get; set; }
        public int? IdAutorizacionUno { get; set; }
        public int? IdAutorizacionDos { get; set; }
        public long? Estilo { get; set; }
        public string AutorizacionTOMITAS { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public string IdBeneficiario { get; set; }
        public long IdFilesBtmum { get; set; }
        public long NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public long NoPagosProcesados { get; set; }
        public long NoPagosDevueltos { get; set; }
        public string IdCanal { get; set; }

        public decimal? Cdl { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTFormatoPapDetalleCmsDto
    {
        public long IdFormato { get; set; }
        public long IdCliente { get; set; }
        public string IdUsuarioCaptura { get; set; }
        public long? IdCuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string IdUsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXMLDet { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public DateTime? FechaAutoriza { get; set; }
        public string IdUsuarioAutorizoDos { get; set; }
        public DateTime? FechaAutorizacionDos { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdAutorizaciones { get; set; }
        public int? IdAutorizacionUno { get; set; }
        public int? IdAutorizacionDos { get; set; }
        public long? Estilo { get; set; }
        public long? AutorizacionTOMITAS { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public string IdBeneficiario { get; set; }
        public long IdFilesBtmum { get; set; }
        public long NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public long NoPagosProcesados { get; set; }
        public long NoPagosDevueltos { get; set; }
        public long IdFormatoPap { get; set; }
        public string Ref1 { get; set; }
        public string Ref2 { get; set; }
        public string Ref3 { get; set; }
        public string Ref4 { get; set; }
        public string Ref5 { get; set; }
        public string IdCanal { get; set; }

        public decimal? Cdl { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTForexEstatusTomiDto
    {
        public string StatusOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTExisteRegistroTomiDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTSumarizadoDetalleTomiDto
    {
        public long IdMaestro { get; set; }
        public int NumRegs { get; set; }
        public decimal? ImpMonto { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTConciliacionDetalleTomiDto
    {
        public decimal? T24DebitAmount { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTInsertarDetalleTomiDto
    {
        public string NfFolioUnico { get; set; }
        public string NfTipo { get; set; }
        public int NfFolio { get; set; }
        public string T24CustomerCode { get; set; }
        public string T24DebitAcctNo { get; set; }
        public string NfEstatus { get; set; }
        public string NfUsuarioCaptura { get; set; }
        public string NfUsuarioAutoriza { get; set; }
        public DateTime? NfFechaCaptura { get; set; }
        public DateTime? NfFechaAutoriza { get; set; }
        public decimal? T24DebitAmount { get; set; }
        public string T24ShortName { get; set; }
        public string NfForexRelacionado { get; set; }
        public string T24DebitCurrency { get; set; }
        public string NfBancoBenef { get; set; }
        public string T24BenAcctNo { get; set; }
        public string NfTipoCuenta { get; set; }
        public string T24BenCustName { get; set; }
        public string NfRfcBeneficiario { get; set; }
        public string NfTasaRetencion { get; set; }
        public string NfFechaEjecucion { get; set; }
        public string T24DebitTheirRef { get; set; }
        public decimal? NfMontoEnviar { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public long? IdFilesBtmum { get; set; }
        public int? NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public int? NoPagosProcesados { get; set; }
        public int? NoPagosDevueltos { get; set; }
        public long? IdFormatoPap { get; set; }
        public string Ref1 { get; set; }
        public string Ref2 { get; set; }
        public string Ref3 { get; set; }
        public string Ref4 { get; set; }
        public string Ref5 { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTInsertarMaestroTomiDto
    {
        public string NfFolioUnico { get; set; }
        public string NfTipo { get; set; }
        public int NfFolio { get; set; }
        public string T24CustomerCode { get; set; }
        public string T24DebitAcctNo { get; set; }
        public string NfEstatus { get; set; }
        public string NfUsuarioCaptura { get; set; }
        public string NfUsuarioAutoriza { get; set; }
        public DateTime? NfFechaCaptura { get; set; }
        public DateTime? NfFechaAutoriza { get; set; }
        public decimal? T24DebitAmount { get; set; }
        public string T24ShortName { get; set; }
        public string NfForexRelacionado { get; set; }
        public string T24DebitCurrency { get; set; }
        public string NfBancoBenef { get; set; }
        public string T24BenAcctNo { get; set; }
        public string NfTipoCuenta { get; set; }
        public string T24BenCustName { get; set; }
        public string NfRfcBeneficiario { get; set; }
        public string NfTasaRetencion { get; set; }
        public string NfFechaEjecucion { get; set; }
        public string T24DebitTheirRef { get; set; }
        public decimal? NfMontoEnviar { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public long? IdFilesBtmum { get; set; }
        public int? NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public int? NoPagosProcesados { get; set; }
        public int? NoPagosDevueltos { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTActualizarDatosGeneralesPlanTrabajoTomiDto
    {
        public long Folio { get; set; }
        public long FolioMaestro { get; set; }
        public string TipoOperacion { get; set; }
        public string FolioUnico { get; set; }
        public decimal? FecValor { get; set; }
        public string CveMoneda { get; set; }
        public decimal? ImporteDetalle { get; set; }
        public decimal? ImporteMaestro { get; set; }
        public int? NumCliente { get; set; }
        public string NombreCliente { get; set; }
        public string NumCuenta { get; set; }
        public int? NumPagos { get; set; }
        public string SubtipoOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTActualizarEstatusCmsDto
    {
        public long IdFormatoPap { get; set; }
        public string Mensaje { get; set; }
        public string Estatus { get; set; }
        public string RazonRechazo { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTActualizarEstatusTomiDto
    {
        public long Folio { get; set; }
        public long IdFormatoPap { get; set; }
        public string NfEstatus { get; set; }
        public string NfRazon { get; set; }
        public string NfUsuarioConfirma { get; set; }
        public DateTime? NfFechaConfirma { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTCancelarPlanTrabajoPorForexTomiDto
    {
        public long Folio { get; set; }
        public long FolioMaestro { get; set; }
        public string TipoOperacion { get; set; }
        public decimal? FecValor { get; set; }
        public string CveMoneda { get; set; }
        public decimal? ImporteDetalle { get; set; }
        public decimal? ImporteMaestro { get; set; }
        public string ErrDescripcion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTMarcarFechaFuturaTomiDto
    {
        public long Folio { get; set; }
        public long FolioMaestro { get; set; }
        public string TipoOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPFXTContenidoFormatoDto
    {
        public string Valor0 { get; set; }
        public string NombreCliente { get; set; }
        public string Valor2 { get; set; }
        public string FolioForexRelacionado { get; set; }
        public string Moneda { get; set; }
        public string Valor5 { get; set; }
        public string BancoBeneficiario { get; set; }
        public string CuentaBeneficiario { get; set; }
        public string TipoCuenta { get; set; }
        public string Valor9 { get; set; }
        public string NombreBeneficiario { get; set; }
        public string RfcBeneficiario { get; set; }
        public string TasaRetencion { get; set; }
        public int? FechaEjecucion { get; set; }
        public string Valor14 { get; set; }
        public string ReferenciaDebito { get; set; }
        public string Valor16 { get; set; }
        public string Valor17 { get; set; }
        public decimal? MontoEnviar { get; set; }
    }

}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP_SPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ResultadoSalidaDto
    {
        public int Salida { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidExisteRegistroDto
    {
        public int num_regs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidForexStatusDto
    {
        public string STATUSOPERACION { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidFormatoPapCmsDto
    {
        public long id_Formato { get; set; }
        public long id_Cliente { get; set; }
        public string id_UsuarioCaptura { get; set; }
        public long? id_Cuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string id_UsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXML_Det { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public DateTime? Fecha_Autoriza { get; set; }
        public string id_UsuarioAutorizo_dos { get; set; }
        public DateTime? FechaAutorizacion_dos { get; set; }
        public string id_Plantilla { get; set; }
        public long? id_Autorizaciones { get; set; }
        public int? id_Autorizacion_uno { get; set; }
        public int? id_Autorizacion_dos { get; set; }
        public long? Estilo { get; set; }
        public string Autorizacion_TOMITAS { get; set; }
        public long? id_Formato_Plantilla { get; set; }
        public string id_Beneficiario { get; set; }
        public long id_files_btmum { get; set; }
        public long nopagos { get; set; }
        public string pagosdescripcion { get; set; }
        public long nopagosprocesados { get; set; }
        public long nopagosdevueltos { get; set; }
        public decimal? cdl { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidFormatoPapDetalleCmsDto : PaPSpidFormatoPapCmsDto
    {
        public long id_formato_pap { get; set; }
        public string ref1 { get; set; }
        public string ref2 { get; set; }
        public string ref3 { get; set; }
        public string ref4 { get; set; }
        public string ref5 { get; set; }
        public string ReferenciaNumerica { get; set; }
        public string ClasificacionOper { get; set; }
        public string DesClasificacionOper { get; set; }
        public string RelacionComercial { get; set; }
        public string id_canal { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidActualizarPlanTrabajoDto
    {
        public long Folio { get; set; }
        public long FolioMaestro { get; set; }
        public string TipoOperacion { get; set; }
        public string FolioUnico { get; set; }
        public int FechaValor { get; set; }
        public string Moneda { get; set; }
        public decimal ImporteDetalle { get; set; }
        public decimal ImporteMaestro { get; set; }
        public int NumCliente { get; set; }
        public string NombreCliente { get; set; }
        public string NumCuenta { get; set; }
        public int NumeroPagos { get; set; }
        public string SubtipoOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidInsertarFormatoDto
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_TIPO { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime NF_FECHA_CAPTURA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public decimal NF_MONTO_ENVIAR { get; set; }
        public string ID_PLANTILLA { get; set; }
        public long? ID_FORMATO_PLANTILLA { get; set; }
        public long ID_FILES_BTMUM { get; set; }
        public int NO_PAGOS { get; set; }
        public string PAGOSDESCRIPCION { get; set; }
        public int NOPAGOSPROCESADOS { get; set; }
        public int NOPAGOSDEVUELTOS { get; set; }
        public long? ID_FORMATO_PAP { get; set; }
        public string REF1 { get; set; }
        public string REF2 { get; set; }
        public string REF3 { get; set; }
        public string REF4 { get; set; }
        public string REF5 { get; set; }
        public string NF_ADDRESS { get; set; }
        public string NF_ZIP_CODE { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_DIRECCION_IP { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidInsightCustomerEdoCtaDto
    {
        public string NIC { get; set; }
        public string P1_CALLE { get; set; }
        public string P1_EXTERIOR { get; set; }
        public string P1_INTERIOR { get; set; }
        public string P1_CODIGO_POSTAL { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidSumarizadoDetalleDto
    {
        public long? id_maestro { get; set; }
        public int num_regs { get; set; }
        public decimal? imp_monto { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidDetalleConciliacionDto
    {
        public decimal? T24_DEBIT_AMOUNT { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidEntradaSalidaConciliacionDto
    {
        public DateTime? ent_fechaValor { get; set; }
        public string ent_moneda_cuenta { get; set; }
        public decimal? ent_importe { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidTombinDetalleDto
    {
        public string NF_TIPO { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_COMPANY_NAME { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public DateTime? NF_FECHA_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public string NF_USUARIO_CONFIRMA { get; set; }
        public DateTime? NF_FECHA_CONFIRMA { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public decimal? T24_DEBIT_AMOUNT { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public bool? NF_CIERRE { get; set; }
        public string NF_ESTATUS_INTERNO { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public decimal? NF_MONTO_ENVIAR { get; set; }
        public string NF_RAZON { get; set; }
        public string NF_FOLIO_UNICO { get; set; }
        public string ID_PLANTILLA { get; set; }
        public long? ID_FORMATO_PLANTILLA { get; set; }
        public long? ID_FILES_BTMUM { get; set; }
        public int? NOPAGOS { get; set; }
        public string PAGOSDESCRIPCION { get; set; }
        public int? NOPAGOSPROCESADOS { get; set; }
        public int? NOPAGOSDEVUELTOS { get; set; }
        public long? ID_FORMATO_PAP { get; set; }
        public string NF_NOMBRE_CUENTA { get; set; }
        public string NF_MONEDA_CUENTA { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public int? NF_FECHA_EJECUCION { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
        public string NF_ADDRESS { get; set; }
        public string NF_ZIP_CODE { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_DIRECCION_IP { get; set; }
        public string OP_FIRMA_DIG { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPSpidInsertarEntradaSalidaDto
    {
        public string Referencia { get; set; }
        public decimal Num_Cliente { get; set; }
        public decimal CDL { get; set; }
        public string Ordenante { get; set; }
        public decimal Operacion { get; set; }
        public string OperacionDesc { get; set; }
        public string Tipo { get; set; }
        public string TipoDesc { get; set; }
        public decimal Cuenta { get; set; }
        public string Nombre { get; set; }
        public string CuentaMoneda { get; set; }
        public decimal Via { get; set; }
        public string ViaDesc { get; set; }
        public decimal Importe { get; set; }
        public decimal Saldo { get; set; }
        public string Comentarios { get; set; }
        public DateTime Fecha_Captura { get; set; }
        public DateTime FechaValor { get; set; }
        public string Hora { get; set; }
        public string Capturista { get; set; }
        public decimal Fad { get; set; }
        public string Referencia_Control { get; set; }
        public int Referencia_Control_Cont { get; set; }
    }

}
