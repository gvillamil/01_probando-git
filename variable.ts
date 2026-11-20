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
                        Config.EscribirLog("INFO|FXI.procesaOperacion|Se insertó correctamente el folio en base de Entrada/Salida.");
                    }
                    else
                    {
                        // Posiblemente se restauró el servicio.
                        bool existe = await _fxiDao.ExisteEntradaSalida(folioUnico);
                        if (existe)
                        {
                            procesado = true;
                            Config.EscribirLog("INFO|FXI.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                        }
                    }
                }
                else
                {
                    procesado = true;
                    Config.EscribirLog("INFO|FXI.procesaOperacion|La operación no debe generar Entrada/Salida en TOMI).");
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
                Config.EscribirLog("INFO|FXI.procesaOperacion|A actualizar estatus en CMS y TOMI.");
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
                    await _fxiCmsDao.ActualizarEstatusFormatoRechazadoForex(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                    await _fxiDao.ActualizarFormatoFxiRechazadoForex(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));

                    Config.EscribirLog($"INFO|FXI.procesaOperacion|Forex Rechazado con Anterioridad: {folio}");

                    // En caso de rechazo termino la operación.
                    Config.EscribirLog("INFO|FXI.procesaOperacion|A cancelar la operación.");

                    await _fxiDao.CancelarMaestroForex(new FxiCancelarMaestroForexTomiDto
                    {
                        Folio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                        TipoOperacion = tipoOperacion,
                        FechaValor = ConversionHelper.ConvertirIntDirecto(fechaEjecucion, "FechaEjecucion"),
                        Moneda = moneda,
                        Importe = importe,
                        ErrorDescripcion = "El forex relacionado ha sido cancelado"
                    });
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
                    return true;
                }

                // Aceptado.
                if (string.Equals(fechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                {
                    await _fxiCmsDao.ActualizarEstatusFormato(new FxiActualizarEstatusFormatoAceptadoCmsDto
                    {
                        IdFormato = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                        Mensaje = "Formato aceptado",
                        Estatus = "APFT"
                    });

                    await _fxiDao.ActualizarEstatusFormatoFxi(new FxiActualizarEstatusNfFxiTomiDto
                    {
                        Folio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                        Estatus = "CONP",
                        UsuarioConfirma = "OPER CMS",
                        FechaConfirma = DateTime.ParseExact(
    DateTime.Now.ToString("yyyy/MM/dd HH:mm"),
    "yyyy/MM/dd HH:mm",
    CultureInfo.InvariantCulture),
                        Razon = null
                    });

                    Config.EscribirLog("INFO|FXI.procesaOperacion|Operación fecha valor hoy.");
                }
                else if (string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) < 0)
                {
                    await _controlServicio.MarcarErrorFechaPasada(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                    throw new Exception($"Se desea realizar una operación al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                }
                else
                {
                    // Fecha futura.
                    await _fxiCmsDao.ActualizarEstatusFormato(new FxiActualizarEstatusFormatoAceptadoCmsDto
                    {
                        IdFormato = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                        Mensaje = "Formato aceptado",
                        Estatus = "REPE"
                    });

                    await _fxiDao.ActualizarEstatusFormatoFxi(new FxiActualizarEstatusNfFxiTomiDto
                    {
                        Folio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                        Estatus = "CONF",
                        UsuarioConfirma = "OPER CMS",
                        FechaConfirma = DateTime.ParseExact(
    DateTime.Now.ToString("yyyy/MM/dd HH:mm"),
    "yyyy/MM/dd HH:mm",
    CultureInfo.InvariantCulture),
                        Razon = null
                    });

                    Config.EscribirLog("INFO|FXI.procesaOperacion|Operación fecha valor fútura.");
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

                Config.EscribirLog("INFO|FXI.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciaProceso.
                /// </summary>

                await _controlServicio.IniciaProceso(new ProcesoDTO

                {

                    Folio = folio,
