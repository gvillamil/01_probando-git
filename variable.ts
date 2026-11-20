TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var detalleTomi = await _tomiDao.ObtenerDetalleTomiParaConciliacion(folio);

                if (detalleTomi != null)
                {
                    regsTomi = 1;
                    importeTomi = detalleTomi.T24DebitAmount ?? 0;
                }

                var sumarizadoNoPendiente =
                    await _tomiDao.ObtenerSumarizadoDetalleNoPendientePorMaestro(mtoFolio);

                if (sumarizadoNoPendiente != null)
                {
                    tomiMtoRegs = sumarizadoNoPendiente.NumRegs;
                    tomiMtoImporte = sumarizadoNoPendiente.ImpMonto ?? 0;
                }

                if (conEntradaSalida)
                {
                    var entradaSalida =
                        await _tomiDao.ObtenerEntradaSalidaParaConciliacion(folioUnico);

                    if (entradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = entradaSalida.EntFechaValor?.ToString("yyyyMMdd");
                        monedaEntSal = _vwT24.ValidaMoneda(entradaSalida.EntMonedaCuenta);
                        importeEntSal = entradaSalida.EntImporte;
                    }
                }

                if (regsCms == 1 &&
                    regsTomi == 1 &&
                    ((conEntradaSalida && regsEntSal == 1) || !conEntradaSalida))
                {
                    if (detImporte != importeTomi)
                    {
                        mensajeError +=
                            $"{Environment.NewLine} - Diferencia en el importe. CMS='{detImporte}', TOMI='{importeTomi}'.";
                    }

                    if (mtoRegs != tomiMtoRegs)
                    {
                        mensajeError +=
                            $"{Environment.NewLine} - Diferencia en el total de registros. CMS='{mtoRegs}', TOMI='{tomiMtoRegs}'.";
                    }

                    if (Math.Round(mtoImporte, 2) != Math.Round(tomiMtoImporte, 2))
                    {
                        mensajeError +=
                            $"{Environment.NewLine} - Diferencia en la sumatoría del importe de registros. CMS='{mtoImporte}', TOMI='{tomiMtoImporte}'.";
                    }

                    if (conEntradaSalida)
                    {
                        if (mtoFechaEjecucion != fechaEjecucionEntSal)
                        {
                            mensajeError +=
                                $"{Environment.NewLine} - Diferencia en la fecha de ejecución de Entrada/Salida. CMS='{mtoFechaEjecucion}', TOMI='{fechaEjecucionEntSal}'.";
                        }

                        if (mtoMoneda != monedaEntSal)
                        {
                            mensajeError +=
                                $"{Environment.NewLine} - Diferencia en la monedas de Entrada/Salida. CMS='{mtoMoneda}', TOMI='{monedaEntSal}'.";
                        }

                        if (detImporte != importeEntSal)
                        {
                            mensajeError +=
                                $"{Environment.NewLine} - Diferencia en el importe de Entrada/Salida. CMS='{detImporte}', Entrada/Salida='{importeEntSal}'.";
                        }
                    }

                    procesado = true;
                }
                else
                {
                    procesado = false;
                    mensajeError += $"{Environment.NewLine} - No se ha registrado la operación en TOMI.";
                }

                if (!string.IsNullOrEmpty(mensajeError))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(mensajeError);

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro CONCILIAR con folio: {folio}");

                if (fechaFutura)
                {
                    await _tomiDao.MarcarPlanTrabajoDetalleFechaFutura(folio, tipoOperacion);
                    await _tomiDao.MarcarPlanTrabajoMaestroFechaFutura(mtoFolio, tipoOperacion);
                }

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación está correctamente conciliada.");

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

            return true;
        }

        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato)
        {
            try
            {
                var detalle = new PaPFxiInsertarDetallePapTomiDto
                {
                    NfFolioUnico = folioUnico,
                    NfTipo = DameTexto(valoresFormato[29]),
                    NfFolio = ConversionHelper.ConvertirLongDefaultCero(valoresFormato[30]),

                    T24CustomerCode = DameTexto(valoresFormato[31]),
                    T24DebitAcctNo = DameTexto(valoresFormato[32]),
                    NfEstatus = "PEND",

                    NfUsuarioCaptura = DameTexto(valoresFormato[34]),
                    NfUsuarioAutoriza = DameTexto(valoresFormato[36]),

                    NfFechaCaptura = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormato[33]),
                    NfFechaAutoriza = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormato[35]),

                    T24DebitAmount = ConversionHelper.ConvertirNullableDecimal(valoresFormato[37]),

                    T24ShortName = TextoBESS(DameTexto(valoresFormato[1])),
                    T24DebitCurrency = DameTexto(valoresFormato[3]),

                    NfForexRelacionado = DameTexto(valoresFormato[6]),
                    NfMontoEnviar = ConversionHelper.ConvertirNullableDecimal(valoresFormato[8]),

                    T24DebitTheirRef = TextoBESS(DameTexto(valoresFormato[9])),
                    NfTipoCuenta = TextoBESS(DameTexto(valoresFormato[10])),
                    NfPayment = TextoBESS(DameTexto(valoresFormato[11])),
                    NfCharge = TextoBESS(DameTexto(valoresFormato[12])),

                    NfFechaEjecucion = ConversionHelper.ConvertirNullableInt(valoresFormato[13]),

                    T24BenCustName = TextoBESS(DameTexto(valoresFormato[14])),
                    NfFxdComment = TextoBESS(DameTexto(valoresFormato[15])),
                    NfRfcBeneficiario = TextoBESS(DameTexto(valoresFormato[16])),
                    NfTasaRetencion = DameTexto(valoresFormato[17]),

                    NfBancoBenef = TextoBESS(DameTexto(valoresFormato[18])),
                    NfSwiftBeneficiario = DameTexto(valoresFormato[19]),
                    T24BenAcctNo = DameTexto(valoresFormato[21]),

                    NfBancoCorresponsal = TextoBESS(DameTexto(valoresFormato[23])),
                    NfSwiftCorresponsal = DameTexto(valoresFormato[24]),
                    NfCuentaCorresponsal = TextoBESS(DameTexto(valoresFormato[25])),

                    NfMensaje = TextoBESS(DameTexto(valoresFormato[27])),
                    NfCountry = DameTexto(valoresFormato[28]),

                    IdPlantilla = DameTexto(valoresFormato[38]),
                    IdFormatoPlantilla = ConversionHelper.ConvertirNullableLongDefaultCero(valoresFormato[39]),

                    IdFilesBtmum = ConversionHelper.ConvertirNullableLong(valoresFormato[40]),
                    NoPagos = ConversionHelper.ConvertirNullableInt(valoresFormato[41]),
                    PagosDescripcion = DameTexto(valoresFormato[42]),
                    NoPagosProcesados = ConversionHelper.ConvertirNullableInt(valoresFormato[43]),
                    NoPagosDevueltos = ConversionHelper.ConvertirNullableInt(valoresFormato[44]),

                    IdFormatoPap = ConversionHelper.ConvertirNullableLong(valoresFormato[45]),

                    Ref1 = DameTexto(valoresFormato[46]),
                    Ref2 = DameTexto(valoresFormato[47]),
                    Ref3 = DameTexto(valoresFormato[48]),
                    Ref4 = DameTexto(valoresFormato[49]),
                    Ref5 = DameTexto(valoresFormato[50]),

                    CountryBen = DameTexto(valoresFormato[51]),
                    CityBen = DameTexto(valoresFormato[52]),
                    StreetBen = DameTexto(valoresFormato[53]),
                    ColonyBen = DameTexto(valoresFormato[54]),
                    BuildingNumberBen = DameTexto(valoresFormato[55]),
                    PostCodeBen = DameTexto(valoresFormato[56])
                };

                bool resultado = await _tomiDao.InsertarDetallePapTomi(detalle);

                Config.EscribirLog($"INFO|PaP_FXI.CMS_A_TOMI|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|PaP_FXI.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");

                return false;
            }
        }

        private async Task<bool> CMS_A_TOMI_MAESTRO(string folioMaestro, object[] valoresFormatoMto)
        {
            try
            {
                var maestro = new PaPFxiInsertarMaestroPapTomiDto
                {
                    NfFolioUnico = folioUnico,
                    NfTipo = DameTexto(valoresFormatoMto[29]),
                    NfFolio = ConversionHelper.ConvertirLongDefaultCero(valoresFormatoMto[30]),

                    T24CustomerCode = DameTexto(valoresFormatoMto[31]),
                    T24DebitAcctNo = DameTexto(valoresFormatoMto[32]),
                    NfEstatus = "PEND",

                    NfUsuarioCaptura = DameTexto(valoresFormatoMto[34]),
                    NfUsuarioAutoriza = DameTexto(valoresFormatoMto[36]),

                    NfFechaCaptura = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormatoMto[33]),
                    NfFechaAutoriza = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormatoMto[35]),

                    T24DebitAmount = ConversionHelper.ConvertirNullableDecimal(valoresFormatoMto[37]),

                    T24ShortName = DameTexto(valoresFormatoMto[1]),
                    T24DebitCurrency = DameTexto(valoresFormatoMto[3]),

                    NfForexRelacionado = DameTexto(valoresFormatoMto[6]),

                    NfMontoEnviar = 0,

                    T24DebitTheirRef = DameTexto(valoresFormatoMto[9]),
                    NfTipoCuenta = DameTexto(valoresFormatoMto[10]),
                    NfPayment = DameTexto(valoresFormatoMto[11]),
                    NfCharge = DameTexto(valoresFormatoMto[12]),

                    NfFechaEjecucion = ConversionHelper.ConvertirNullableInt(valoresFormatoMto[13]),

                    T24BenCustName = DameTexto(valoresFormatoMto[14]),
                    NfFxdComment = DameTexto(valoresFormatoMto[15]),
                    NfRfcBeneficiario = DameTexto(valoresFormatoMto[16]),
                    NfTasaRetencion = DameTexto(valoresFormatoMto[17]),

                    NfBancoBenef = DameTexto(valoresFormatoMto[18]),
                    NfSwiftBeneficiario = DameTexto(valoresFormatoMto[19]),
                    T24BenAcctNo = DameTexto(valoresFormatoMto[21]),

                    NfBancoCorresponsal = DameTexto(valoresFormatoMto[23]),
                    NfSwiftCorresponsal = DameTexto(valoresFormatoMto[24]),
                    NfCuentaCorresponsal = DameTexto(valoresFormatoMto[25]),

                    NfMensaje = DameTexto(valoresFormatoMto[27]),
                    NfCountry = DameTexto(valoresFormatoMto[28]),

                    IdPlantilla = DameTexto(valoresFormatoMto[38]),

                    IdFormatoPlantilla = 0,

                    IdFilesBtmum = ConversionHelper.ConvertirNullableLong(valoresFormatoMto[40]),
                    NoPagos = ConversionHelper.ConvertirNullableInt(valoresFormatoMto[41]),
                    PagosDescripcion = DameTexto(valoresFormatoMto[42]),
                    NoPagosProcesados = ConversionHelper.ConvertirNullableInt(valoresFormatoMto[43]),
                    NoPagosDevueltos = ConversionHelper.ConvertirNullableInt(valoresFormatoMto[44])
                };

                bool resultado = await _tomiDao.InsertarMaestroPapTomi(maestro);

                Config.EscribirLog($"INFO|PaP_FXI.CMS_A_TOMI_MAESTRO|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|PaP_FXI.CMS_A_TOMI_MAESTRO|Se presenta exception: {ex.Message}. Trace: {ex}");

                return false;
            }
        }

        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            try
            {
                var detalles = await _tomiDao.ObtenerDetalleTomiParaEntradaSalida(folio);

                if (detalles == null || detalles.Count == 0)
                    return false;

                PaPFxiInsertarEntradaSalidaDto entradaSalida = null;

                foreach (var detalle in detalles)
                {
                    entradaSalida = new PaPFxiInsertarEntradaSalidaDto
                    {
                        Referencia = detalle.NfFolioUnico,
                        NumCliente = ConversionHelper.ConvertirNullableInt(detalle.T24CustomerCode),
                        Cdl = ConversionHelper.ConvertirNullableDecimal(detalle.T24CustomerCode),
                        Ordenante = detalle.T24CompanyName,

                        Operacion = 218,
                        OperacionDesc = "Salida  (TTS)",
                        Tipo = "I",
                        TipoDesc = "Internacional",

                        Importe = detalle.T24DebitAmount,
                        Saldo = 0,
                        Comentarios = detalle.NfMensaje,

                        FechaCaptura = ConversionHelper.TextoToFechaDesdeObject(_configuracion.StrFechaSistema),
                        FechaValor = ConversionHelper.TextoToFechaDesdeObject(detalle.NfFechaEjecucion),
                        Hora = DateTime.Now.ToLongTimeString(),

                        Capturista = "JEAI",
                        Fad = 0,
                        ReferenciaControl = "A",
                        ReferenciaControlCont = 0
                    };

                    bool usarCuentaOriginal;

                    if (detalle.T24DebitCurrency != _vwT24.MonedaDefaultMN)
                    {
                        usarCuentaOriginal = string.IsNullOrEmpty(detalle.NfForexRelacionado);
                    }
                    else
                    {
                        usarCuentaOriginal = detalle.NfForexRelacionado == "1";
                    }

                    if (usarCuentaOriginal)
                    {
                        entradaSalida.Cuenta = ConversionHelper.ConvertirNullableDecimal(detalle.T24DebitAcctNo);
                        entradaSalida.Nombre = detalle.NfNombreCuenta;
                        entradaSalida.CuentaMoneda = detalle.NfMonedaCuenta;
                        entradaSalida.Via = 810;
                        entradaSalida.ViaDesc = "TRF ENVIADA USD BTMNY";
                    }
                    else
                    {
                        /// <summary>
                        /// Ejecuta la operaciÃ³n AsignarCuentaFadPorMoneda.
                        /// </summary>
                        AsignarCuentaFadPorMoneda(entradaSalida, detalle.T24DebitCurrency);
                    }
                }

                if (entradaSalida == null)
                    return false;

                bool resultado = await _tomiDao.InsertarEntradaSalida(entradaSalida);

                Config.EscribirLog($"INFO|PaP_FXI.TOMI_ENTRADA_SALIDA|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|PaP_FXI.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}. Trace: {ex}");

                return false;
            }
        }

        private static object[] CargarValoresFormatoDetalle(PaPFxiDetalleCmsDto detalleCms)
        {
            object[] valoresFormato = new object[57];

            valoresFormato[29] = "FXI";
            valoresFormato[30] = detalleCms.IdFormato;
            valoresFormato[31] = detalleCms.Cdl;
            valoresFormato[32] = detalleCms.IdCuenta;
            valoresFormato[33] = detalleCms.FechaCaptura;
            valoresFormato[34] = detalleCms.IdUsuarioCaptura;
            valoresFormato[35] = detalleCms.FechaAutorizacion;
            valoresFormato[36] = detalleCms.IdUsuarioAutorizo;
            valoresFormato[37] = detalleCms.MontoOperacion;
            valoresFormato[38] = detalleCms.IdPlantilla;
            valoresFormato[39] = detalleCms.IdFormatoPlantilla;
            valoresFormato[40] = detalleCms.IdFilesBtmum;
            valoresFormato[41] = detalleCms.NoPagos;
            valoresFormato[42] = detalleCms.PagosDescripcion;
            valoresFormato[43] = detalleCms.NoPagosProcesados;
            valoresFormato[44] = detalleCms.NoPagosDevueltos;
            valoresFormato[45] = detalleCms.IdFormatoPap;
            valoresFormato[46] = detalleCms.Ref1;
            valoresFormato[47] = detalleCms.Ref2;
            valoresFormato[48] = detalleCms.Ref3;
            valoresFormato[49] = detalleCms.Ref4;
            valoresFormato[50] = detalleCms.Ref5;

            string contenidoFormato = detalleCms.ContenidoFormato;

            if (string.IsNullOrEmpty(contenidoFormato))
                throw new Exception($"El contenidoFormato del folio {detalleCms.IdFormato} viene vacío.");

            string[] valoresPipes = contenidoFormato.Split('|');

            if (valoresPipes.Length - 1 < 28)
            {
                throw new Exception(
                    $"El contenidoFormato del folio {detalleCms.IdFormato} es diferente al esperado, debe ser 28 datos y se tienen {valoresPipes.Length - 1}.");
            }

            for (int idxPipes = 0; idxPipes <= 28; idxPipes++)
            {
                string[] llaveValor = valoresPipes[idxPipes].Split('=');
                valoresFormato[idxPipes] = llaveValor[1];
            }

            if (contenidoFormato.Contains("|10053="))
            {
                string[] llaveValor = valoresPipes[29].Split('=');
                valoresFormato[51] = llaveValor[1]; // Pais beneficiario
            }
            else
            {
                valoresFormato[51] = string.Empty;
            }

            // Campos City Project
            if (contenidoFormato.Contains("|10054="))
            {
                string[] llaveValor = valoresPipes[30].Split('=');
                valoresFormato[52] = llaveValor[1]; // Ciudad beneficiario
            }
            else
            {
                valoresFormato[52] = string.Empty;
            }

            if (contenidoFormato.Contains("|10055="))
            {
                string[] llaveValor = valoresPipes[31].Split('=');
                valoresFormato[53] = llaveValor[1]; // Calle beneficiario
            }
            else
            {
                valoresFormato[53] = string.Empty;
            }

            if (contenidoFormato.Contains("|10056="))
            {
                string[] llaveValor = valoresPipes[32].Split('=');
                valoresFormato[55] = llaveValor[1]; // Numero beneficiario
            }
            else
            {
                valoresFormato[55] = string.Empty;
            }

            if (contenidoFormato.Contains("|10057="))
            {
                string[] llaveValor = valoresPipes[33].Split('=');
                valoresFormato[54] = llaveValor[1]; // Colonia beneficiario
            }
            else
            {
                valoresFormato[54] = string.Empty;
            }

            if (contenidoFormato.Contains("|10058="))
            {
                string[] llaveValor = valoresPipes[34].Split('=');
                valoresFormato[56] = llaveValor[1]; // CP beneficiario
            }
            else
            {
                valoresFormato[56] = string.Empty;
            }

            return valoresFormato;
        }

        private static void AsignarCuentaFadPorMoneda(
    PaPFxiInsertarEntradaSalidaDto entradaSalida,
    string moneda)
        {
            switch (moneda)
            {
                case "USD":
                    entradaSalida.Cuenta = 3300064;
                    entradaSalida.Nombre = "BTMM USD FAD";
                    entradaSalida.CuentaMoneda = "USD";
                    entradaSalida.Via = 810;
                    entradaSalida.ViaDesc = "TRF ENVIADA USD BTMNY";
                    break;

                case "EUR":
                    entradaSalida.Cuenta = 3399940;
                    entradaSalida.Nombre = "BTMM EUR FAD";
                    entradaSalida.CuentaMoneda = "EUR";
                    entradaSalida.Via = 830;
                    entradaSalida.ViaDesc = "TRF ENVIADA EUR";
                    break;

                case "JPY":
                    entradaSalida.Cuenta = 3399902;
                    entradaSalida.Nombre = "BTMM JPY FAD";
                    entradaSalida.CuentaMoneda = "JPY";
                    entradaSalida.Via = 820;
                    entradaSalida.ViaDesc = "TRF ENVIADA JPY";
                    break;

                case "HKD":
                    entradaSalida.Cuenta = 3300420;
                    entradaSalida.Nombre = "BTMM HKD FAD";
                    entradaSalida.CuentaMoneda = "HKD";
                    entradaSalida.Via = 810;
                    entradaSalida.ViaDesc = "TRF ENVIADA USD BTMNY";
                    break;

                case "CAD":
                    entradaSalida.Cuenta = 3399903;
                    entradaSalida.Nombre = "BTMM CAD FAD";
                    entradaSalida.CuentaMoneda = "CAD";
                    entradaSalida.Via = 810;
                    entradaSalida.ViaDesc = "TRF ENVIADA USD BTMNY";
                    break;

                case "SGD":
                    entradaSalida.Cuenta = 3300153;
                    entradaSalida.Nombre = "BTMM SGD FAD";
                    entradaSalida.CuentaMoneda = "SGD";
                    entradaSalida.Via = 810;
                    entradaSalida.ViaDesc = "TRF ENVIADA USD BTMNY";
                    break;

                case "GBP":
                    entradaSalida.Cuenta = 3300404;
                    entradaSalida.Nombre = "BTMM GBP FAD";
                    entradaSalida.CuentaMoneda = "GBP";
                    entradaSalida.Via = 810;
                    entradaSalida.ViaDesc = "TRF ENVIADA USD BTMNY";
                    break;

                case "AUD":
                    entradaSalida.Cuenta = 3300129;
                    entradaSalida.Nombre = "BTMM AUD FAD";
                    entradaSalida.CuentaMoneda = "AUD";
                    entradaSalida.Via = 810;
                    entradaSalida.ViaDesc = "TRF ENVIADA USD BTMNY";
                    break;
            }
        }

        private static object[] CargarValoresFormatoMaestro(PaPFxiMaestroCmsDto maestroCms)
        {
            object[] valoresFormatoMto = new object[46];

            valoresFormatoMto[29] = "FXD";
            valoresFormatoMto[30] = maestroCms.IdFormato;
            valoresFormatoMto[31] = maestroCms.Cdl;
            valoresFormatoMto[32] = maestroCms.IdCuenta;
            valoresFormatoMto[33] = maestroCms.FechaCaptura;
            valoresFormatoMto[34] = maestroCms.IdUsuarioCaptura;
            valoresFormatoMto[35] = maestroCms.FechaAutorizacion;
            valoresFormatoMto[36] = maestroCms.IdUsuarioAutorizo;
            valoresFormatoMto[37] = maestroCms.MontoOperacion;
            valoresFormatoMto[38] = maestroCms.IdPlantilla;
            valoresFormatoMto[39] = maestroCms.IdFormatoPlantilla;
            valoresFormatoMto[40] = maestroCms.IdFilesBtmum;
            valoresFormatoMto[41] = maestroCms.NoPagos;
            valoresFormatoMto[42] = maestroCms.PagosDescripcion;
            valoresFormatoMto[43] = maestroCms.NoPagosProcesados;
            valoresFormatoMto[44] = maestroCms.NoPagosDevueltos;

            string contenidoFormato = maestroCms.ContenidoFormato;

            if (string.IsNullOrEmpty(contenidoFormato))
                throw new Exception($"El contenidoFormato del folio {maestroCms.IdFormato} viene vacío.");

            string[] valoresPipes = contenidoFormato.Split('|');

            if (valoresPipes.Length - 1 < 28)
            {
                throw new Exception(
                    $"El contenidoFormato del folio {maestroCms.IdFormato} es diferente al esperado, debe ser 28 datos y se tienen {valoresPipes.Length - 1}.");
            }

            for (int idxPipes = 0; idxPipes <= 28; idxPipes++)
            {
                string[] llaveValor = valoresPipes[idxPipes].Split('=');
                valoresFormatoMto[idxPipes] = llaveValor[1];
            }

            return valoresFormatoMto;
        }

    }
}

using Aplicacion.DTOs.PaP;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.PaP;
using Aplicacion.Modelos;
using Aplicacion.Servicios.FirmaElectronica;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa Pa P FXD.
    /// </summary>
    public class PaP_FXD
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IPaPFXDDao _PaPFXDDao;
        private readonly IPaPFXDCmsDao _PaPFXDCmsDao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IComunFXD _comunFXD;

        private const string tipoOperacion = "PaP_FXD";

        private string folioUnico = string.Empty;
        private string folioPropuesto = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase PaP_FXD.
        /// </summary>

        public PaP_FXD(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IPaPFXDDao paPFXDDao,
            IPaPFXDCmsDao paPFXDCmsDao,
            ConexionT24Mw vwT24,
            IComunFXD comunFXD)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _PaPFXDDao = paPFXDDao;
            _PaPFXDCmsDao = paPFXDCmsDao;
            _vwT24 = vwT24;
            _comunFXD = comunFXD;
        }

        /// <summary>
        /// Procesa la operación PaP FXD del servicio Lectura.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio)
        {
            PaPFxdMaestroCmsDto maestroCms;
            PaPFxdDetalleCmsDto detalleCms;
            PaPFxdContenidoDto maestroContenido;
            PaPFxdContenidoDto detalleContenido;
            PapFxdSumatoriaDetalleDto sumatoriaDetalle;
            PapFxdDetalleImporteDto detalleProcesadoTomi;
            FxdEntradaSalidaDto entradaSalida;

            long folioNumero = ConversionHelper.ParseLongSeguro(folio, nameof(folio));
            long folioMaestro = 0;
            int noPagosMaestro = 0;
            string fechaEjecucionMaestroTexto = string.Empty;
            string fechaEjecucionDetalleTexto = string.Empty;
            string monedaMaestro = string.Empty;
            string monedaDetalle = string.Empty;
            decimal importeMaestro = 0m;
            decimal importeDetalle = 0m;

            string nombreCliente = string.Empty;
            string folioForexMaestro = string.Empty;
            string folioForexDetalle = string.Empty;

            bool conForex = false;
            bool forexCancelado = false;
            bool conEntradaSalida = true;
            bool procesado;
            bool resultado;

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

            if (string.Equals(_configuracion.AliasProceso, "", StringComparison.Ordinal))
                return false;

            // Confirmo certificado y llave para firma digital.
            using (var firma = new MetodosFirma(_configuracion))
            {
                if (!firma.CertificadoEncontrado())
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de SPEI: No se encontro el certificado y/o llave para realizar la firma de la transacción");
            }

            Config.EscribirLog($"INFO|PaP_FXD.procesaOperacion|Inicia proceso folio={folio}.");

            // Datos del maestro en CMS.
            maestroCms = await _PaPFXDCmsDao.ObtenerMaestroPapFxdPorDetalle(folio);
            // Datos del detalle en CMS.
            detalleCms = await _PaPFXDCmsDao.ObtenerDetallePapFxdPorFolio(folio);

            if (maestroCms == null)
                maestroCms = new PaPFxdMaestroCmsDto();

            if (detalleCms == null)
                detalleCms = new PaPFxdDetalleCmsDto();

            // Parseo legacy de contenidoFormato.
            maestroContenido = ParseContenidoFormato(maestroCms.ContenidoFormato, folio);
            detalleContenido = ParseContenidoFormato(detalleCms.ContenidoFormato, folio);

            // ---------------------------
            // Maestro
            // ---------------------------
            folioMaestro = maestroCms.IdFormato;
            noPagosMaestro = Convert.ToInt32(maestroCms.NoPagos);
            importeMaestro = maestroCms.MontoOperacion;

            folioForexMaestro = string.Empty;

            fechaEjecucionMaestroTexto = maestroContenido.FechaEjecucion;
            monedaMaestro = _vwT24.ValidaMoneda(maestroContenido.Moneda);

            // ---------------------------
            // Detalle
            // ---------------------------
            importeDetalle = detalleCms.MontoOperacion;

            folioForexDetalle = string.Empty;

            fechaEjecucionDetalleTexto = detalleContenido.FechaEjecucion;
            monedaDetalle = _vwT24.ValidaMoneda(detalleContenido.Moneda);

            nombreCliente = detalleContenido.NombreCliente;

            Config.EscribirLog($"INFO|PaP_FXD.procesaOperacion|Veamos que trae la fecha {fechaEjecucionDetalleTexto}-----{fechaEjecucionMaestroTexto}");

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(fechaEjecucionMaestroTexto, CultureInfo.InvariantCulture);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(folioNumero);
                throw new Exception($"Se desea realizar una operación al {fechaEjecucionMaestroTexto} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // Genero el folio único y folio propuesto T24.
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucionMaestroTexto),
                folio);

            folioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                folioUnico,
                int.Parse(ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucionMaestroTexto)),
                tipoOperacion);

            if (string.IsNullOrWhiteSpace(folioPropuesto))
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("Error al obtener el folio propuesto de T24");

            // Validación maestro vs detalle.
            if (!string.Equals(folioForexMaestro, folioForexDetalle, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Forex: Maestro:'{folioForexMaestro}', Detalle:'{folioForexDetalle}'.");

            if (!string.Equals(fechaEjecucionMaestroTexto, fechaEjecucionDetalleTexto, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Fecha de ejecución: Maestro:'{fechaEjecucionMaestroTexto}', Detalle:'{fechaEjecucionDetalleTexto}'.");

            if (!string.Equals(monedaMaestro, monedaDetalle, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Moneda: Maestro:'{monedaMaestro}', Detalle:'{monedaDetalle}'.");

            Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|Validando si la operación está asignada a un Forex.");

            if (!string.Equals(folioForexMaestro, "", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|PaP_FXD.procesaOperacion|Asignado el folio forex: {folioForexMaestro}.");
                conForex = true;

                if (double.TryParse(folioForexMaestro, NumberStyles.Any, CultureInfo.InvariantCulture, out _))
                    throw new Exception($"El Folio del Forex no es válido porque es numérico; se espera una referencia alfanumérica/external ref: {folioForexMaestro}");

                ForexStatusDto forexStatus = await _PaPFXDDao.ObtenerForexStatusPorExternalRef(folioForexMaestro);
                if (forexStatus != null &&
                    string.Equals(forexStatus.StatusOperacion, "CANCELADO", StringComparison.OrdinalIgnoreCase))
                {
                    forexCancelado = true;
                    conEntradaSalida = false;
                    Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|FOREX CANCELADO.");
                }
            }

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string subTipo = string.Empty;

                Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");

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
                    subTipo = $"FX:{folioForexMaestro}";

                int fechaValor = ConversionHelper.ParseFechaEntera(fechaEjecucionMaestroTexto);

                procesado = await _PaPFXDDao.ActualizarPlanTrabajoMaestroPapFxdLectura(
                    folioDetalle: folioNumero,
                    tipoOperacion: tipoOperacion,
                    folioUnico: folioUnico,
                    fechaValor: fechaValor,
                    moneda: monedaMaestro,
                    importeDetalle: importeDetalle,
                    importeMaestro: importeMaestro,
                    numCliente: DameTexto(detalleCms.Cdl),
                    nombreCliente: nombreCliente,
                    numCuenta: DameTexto(detalleCms.IdCuenta),
                    subtipoOperacion: subTipo,
                    noPagos: noPagosMaestro,
                    folioMaestro: folioMaestro);

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
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
                }
            }

            // Lectura del CMS e inserción a TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CMS_A_TOMI", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|A insertar datos de CMS a TOMI.");

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

                resultado = await CMS_A_TOMI(folio, detalleCms, detalleContenido, folioUnico);
                if (resultado)
                {
                    Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");

                    if (!await _comunFXD.GeneraFirmaDigital("tb_tombin_nf_fxd_pap_detalle", folio, folioPropuesto))
                        throw new Exception($"Se presentó un error al generar la firma digital del folio: {folio}");

                    procesado = true;
                }
                else
                {
                    // Posiblemente se restauró el servicio.
                    int numRegsDetalle = await _PaPFXDDao.ContarRegistrosPapFxdDetallePorFolio(folioNumero);
                    if (numRegsDetalle > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

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
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CMS_A_TOMI con folio: {folio}");
                }
            }

            // Registro de la Entrada/Salida.
            if (string.Equals(_configuracion.AliasProceso, "TOMI_ENTRADAS/SALIDAS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|A insertar datos de Entrada/Salida en TOMI (registro detalle).");

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

                    int numRegsEntSal = await _PaPFXDDao.ContarEntradasSalidasPorReferencia(folioUnico);
                    if (numRegsEntSal > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                    }

                    if (!procesado)
                    {
                        resultado = await TOMI_ENTRADA_SALIDA(folio, detalleCms, detalleContenido, folioUnico);
                        if (resultado)
                        {
                            procesado = true;
                            Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|Se insertó correctamente el folio detalle en base de Entrada/Salida.");
                        }
                    }

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
                    }
                    else
                    {
                        throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");
                    }
                }
            }

            // Actualización del estatus del CMS y TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|PaP_FXD.procesaOperacion|A actualizar estatus en CMS y TOMI. Buscando sumarizado de los detalles del folioMaestro={folioMaestro}");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                sumatoriaDetalle = await _PaPFXDDao.ObtenerSumatoriaDetallePapFxd(folioMaestro, soloNoPendientes: false);

                int tomiRegs = sumatoriaDetalle?.NumRegs ?? 0;
                decimal tomiImporte = sumatoriaDetalle?.ImpMonto ?? 0m;

                procesado = false;

                if (noPagosMaestro == tomiRegs &&
                    decimal.Round(importeMaestro, 2) == decimal.Round(tomiImporte, 2))
                {
                    Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|El sumarizado de detalles está completo.");

                    int detallesNoPend = await _PaPFXDDao.ContarDetallesPapFxdConEstatusDistintoPend(folioNumero);
                    if (detallesNoPend > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|El folio ya se había marcado como realizado.");
                    }

                    if (!procesado)
                    {
                        Config.EscribirLog($"INFO|PaP_FXD.procesaOperacion|A tratar de insertar el maestro con folio {folioMaestro}.");

                        resultado = await CMS_A_TOMI_MAESTRO(folioMaestro.ToString(CultureInfo.InvariantCulture), maestroCms, maestroContenido, folioUnico);
                        if (resultado)
                        {
                            Config.EscribirLog($"INFO|PaP_FXD.procesaOperacion|Se insertó correctamente el folio maestro {folioMaestro} en base de TOMI.");
                            procesado = true;
                        }
                        else
                        {
                            int numRegsMaestro = await _PaPFXDDao.ContarRegistrosPapFxdMaestroPorFolio(folioMaestro);
                            if (numRegsMaestro > 0)
                            {
                                procesado = true;
                                Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|El folio maestro en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                            }
                        }
                    }

                    if (procesado)
                    {
                        if (conForex && forexCancelado)
                        {
                            const string descripcionError = "El forex relacionado ha sido cancelado";

                            bool cmsOk = await _PaPFXDCmsDao.ActualizarCmsRechazadoPorForexCancelado(folioMaestro.ToString(CultureInfo.InvariantCulture));
                            if (!cmsOk)
                                throw new Exception($"No fue posible actualizar CMS como rechazado por forex cancelado para folio maestro={folioMaestro}.");

                            bool tomiOk = await _PaPFXDDao.ActualizarTomiRechazadoPorForexCancelado(folioMaestro);
                            if (!tomiOk)
                                throw new Exception($"No fue posible actualizar TOMI como rechazado por forex cancelado para folio maestro={folioMaestro}.");

                            Config.EscribirLog($"INFO|PaP_FXD.procesaOperacion|Forex Rechazado con Anterioridad: {folioMaestro}");
                            Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|A cancelar la operación.");

                            bool planDetalleOk = await _PaPFXDDao.ActualizarPlanTrabajoMaestroDetalleCanceladoPorForex(
                                folioDetalle: folioNumero,
                                tipoOperacion: tipoOperacion,
                                fechaValor: ConversionHelper.ParseFechaEntera(fechaEjecucionMaestroTexto),
                                moneda: monedaMaestro,
                                importe: importeDetalle,
                                descripcionError: descripcionError);

                            if (!planDetalleOk)
                                throw new Exception($"No fue posible actualizar tb_srv_plan_trabajo_maestro detalle como cancelado por forex para folio={folio}.");

                            bool planMaestroOk = await _PaPFXDDao.ActualizarPlanTrabajoMaestroMaestroCanceladoPorForex(
                folioMaestro: ConversionHelper.ParseLongSeguro(await _vwT24.ObtenerFolioMaestro(folio, tipoOperacion), "folio_maestro"),
                                tipoOperacion: tipoOperacion,
                                fechaValor: ConversionHelper.ParseFechaEntera(fechaEjecucionMaestroTexto),
                                moneda: monedaMaestro,
                                importe: importeMaestro,
                                descripcionError: descripcionError);

                            if (!planMaestroOk)
                                throw new Exception($"No fue posible actualizar tb_srv_plan_trabajo_maestro maestro como cancelado por forex para folio maestro={folioMaestro}.");

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
                        else
                        {
                            DateTime fechaConfirma = DateTime.ParseExact(
                                DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                                "yyyy/MM/dd HH:mm",
                                CultureInfo.InvariantCulture);
                            const string usuarioConfirma = "OPER CMS";

                            if (string.Equals(fechaEjecucionMaestroTexto, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                            {
                                bool cmsOk = await _PaPFXDCmsDao.ActualizarCmsAceptadoHoy(folioMaestro.ToString(CultureInfo.InvariantCulture));
                                if (!cmsOk)
                                    throw new Exception($"No fue posible actualizar CMS como aceptado hoy para folio maestro={folioMaestro}.");

                                bool tomiOk = await _PaPFXDDao.ActualizarTomiAceptadoHoy(folioMaestro, usuarioConfirma, fechaConfirma);
                                if (!tomiOk)
                                    throw new Exception($"No fue posible actualizar TOMI como aceptado hoy para folio maestro={folioMaestro}.");

                                Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|Operación fecha valor hoy.");
                            }
                            else if (string.CompareOrdinal(fechaEjecucionMaestroTexto, _configuracion.StrFechaSistema) < 0)
                            {
                                await _controlServicio.MarcarErrorFechaPasada(folioNumero);
                                throw new Exception($"Se desea realizar una operación al {fechaEjecucionMaestroTexto} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                            }
                            else
                            {
                                bool cmsOk = await _PaPFXDCmsDao.ActualizarCmsPendienteEjecucion(folioMaestro.ToString(CultureInfo.InvariantCulture));
                                if (!cmsOk)
                                    throw new Exception($"No fue posible actualizar CMS como pendiente de ejecución para folio maestro={folioMaestro}.");

                                bool tomiOk = await _PaPFXDDao.ActualizarTomiPendienteEjecucion(folioMaestro, usuarioConfirma, fechaConfirma);
                                if (!tomiOk)
                                    throw new Exception($"No fue posible actualizar TOMI como pendiente de ejecución para folio maestro={folioMaestro}.");

                                Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|Operación fecha valor futura.");
                            }
                        }
                    }
                }
                else
                {
                    Config.EscribirLog($"AVISO|PaP_FXD.procesaOperacion|Faltan detalles a procesar para marcar el cambio de estatus en el folio Maestro:{folioMaestro}.");
                    return true;
                }

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
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CAMBIO_ESTATUS con folio: {folio}");
                }
            }

            // Conciliación.
            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                int regsCms = 1;
                int regsTomi = 0;
                int regsEntSal = 0;
                string msjError = string.Empty;

                string fechaEjecucionEntSal = string.Empty;
                string monedaEntSal = string.Empty;
                decimal importeEntSal = 0m;
                decimal importeTomiDetalle = 0m;

                Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|A conciliar operaciones entre CMS y TOMI (maestro y detalles).");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                detalleProcesadoTomi = await _PaPFXDDao.ObtenerDetalleProcesadoPorFolio(folioNumero);
                if (detalleProcesadoTomi != null)
                {
                    regsTomi = 1;
                    importeTomiDetalle = detalleProcesadoTomi.T24DebitAmount ?? 0m;
                }

                sumatoriaDetalle = await _PaPFXDDao.ObtenerSumatoriaDetallePapFxd(folioMaestro, soloNoPendientes: true);
                int tomiRegsNoPend = sumatoriaDetalle?.NumRegs ?? 0;
                decimal tomiImporteNoPend = sumatoriaDetalle?.ImpMonto ?? 0m;

                if (conEntradaSalida)
                {
                    entradaSalida = await _PaPFXDDao.ObtenerEntradaSalidaPorReferencia(folioUnico);
                    if (entradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = entradaSalida.EntFechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                        monedaEntSal = _vwT24.ValidaMoneda(entradaSalida.EntMonedaCuenta);
                        importeEntSal = entradaSalida.EntImporte ?? 0m;
                    }
                }

                if (regsCms == 1 && regsTomi == 1 && ((conEntradaSalida && regsEntSal == 1) || !conEntradaSalida))
                {
                    if (importeDetalle != importeTomiDetalle)
                        msjError += $"{Environment.NewLine} - Diferencia en el importe. CMS='{importeDetalle}', TOMI='{importeTomiDetalle}'.";

                    if (noPagosMaestro != tomiRegsNoPend)
                        msjError += $"{Environment.NewLine} - Diferencia en el total de registros. CMS='{noPagosMaestro}', TOMI='{tomiRegsNoPend}'.";

                    if (decimal.Round(importeMaestro, 2) != decimal.Round(tomiImporteNoPend, 2))
                        msjError += $"{Environment.NewLine} - Diferencia en la sumatoría del importe de registros. CMS='{importeMaestro}', TOMI='{tomiImporteNoPend}'.";

                    if (conEntradaSalida)
                    {
                        if (!string.Equals(fechaEjecucionMaestroTexto, fechaEjecucionEntSal, StringComparison.Ordinal))
                            msjError += $"{Environment.NewLine} - Diferencia en la fecha de ejecución de Entrada/Salida. CMS='{fechaEjecucionMaestroTexto}', TOMI='{fechaEjecucionEntSal}'.";

                        if (!string.Equals(monedaMaestro, monedaEntSal, StringComparison.Ordinal))
                            msjError += $"{Environment.NewLine} - Diferencia en la monedas de Entrada/Salida. CMS='{monedaMaestro}', TOMI='{monedaEntSal}'.";

                        if (importeDetalle != importeEntSal)
                            msjError += $"{Environment.NewLine} - Diferencia en el importe de Entrada/Salida. CMS='{importeDetalle}', Entrada/Salida='{importeEntSal}'.";
                    }

                    procesado = string.IsNullOrWhiteSpace(msjError);
                }
                else
                {
                    procesado = false;
                    msjError += $"{Environment.NewLine} - No se ha registrado la operación en TOMI.";
                }

                if (!string.IsNullOrWhiteSpace(msjError))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(msjError);

                if (procesado)
                {
                    Config.EscribirLog("INFO|PaP_FXD.procesaOperacion|La operación está correctamente conciliada.");
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

            return true;
        }

        private async Task<bool> CMS_A_TOMI(
            string folio,
            PaPFxdDetalleCmsDto detalleCms,
            PaPFxdContenidoDto detalleContenido,
            string folioUnicoGenerado)
        {
            try
            {
                bool resultado = await _PaPFXDDao.InsertarFormatoPapFxdDetalleEnTomi(
                    nfFolioUnico: folioUnicoGenerado,
                    nfTipo: "FXD",
                    nfFolio: detalleCms.IdFormato,
                    t24CustomerCode: DameTexto(detalleCms.Cdl),
                    t24DebitAcctNo: DameTexto(detalleCms.IdCuenta),
                    nfEstatus: "PEND",
                    nfUsuarioCaptura: detalleCms.IdUsuarioCaptura,
                    nfUsuarioAutoriza: detalleCms.IdUsuarioAutorizo,
                    nfFechaCaptura: detalleCms.FechaCaptura,
                    nfFechaAutoriza: detalleCms.FechaAutorizacion,
                    t24DebitAmount: detalleCms.MontoOperacion,
                    t24ShortName: DameTexto(detalleContenido.Valores[1]),
                    nfForexRelacionado: DameTexto(detalleContenido.Valores[3]),
                    t24DebitCurrency: DameTexto(detalleContenido.Valores[4]),
                    nfBancoBenef: DameTexto(detalleContenido.Valores[6]),
                    t24BenAcctNo: DameTexto(detalleContenido.Valores[7]),
                    nfTipoCuenta: DameTexto(detalleContenido.Valores[8]),
                    t24BenCustName: TextoSPEI(detalleContenido.Valores[10]),
                    nfRfcBeneficiario: TextoSPEI(detalleContenido.Valores[11]),
                    nfTasaRetencion: DameTexto(detalleContenido.Valores[12]),
                    nfFechaEjecucion: ConversionHelper.ParseFechaEntera(DameTexto(detalleContenido.Valores[13])),
                    t24DebitTheirRef: TextoSPEI(detalleContenido.Valores[15]),
                    nfMontoEnviar: ConversionHelper.ParseDecimalSeguro(detalleContenido.Valores[18]),
                    idPlantilla: detalleCms.IdPlantilla,
                    idFormatoPlantilla: detalleCms.IdFormatoPlantilla,
                    idFilesBtmum: detalleCms.IdFilesBtmum,
                noPagos: ConversionHelper.ConvertLongToNullableInt(detalleCms.NoPagos),
                    pagosDescripcion: detalleCms.PagosDescripcion,
                noPagosProcesados: ConversionHelper.ConvertLongToNullableInt(detalleCms.NoPagosProcesados),
                noPagosDevueltos: ConversionHelper.ConvertLongToNullableInt(detalleCms.NoPagosDevueltos),
                    idFormatoPap: detalleCms.IdFormatoPap,
                    ref1: detalleCms.Ref1,
                    ref2: detalleCms.Ref2,
                    ref3: detalleCms.Ref3,
                    ref4: detalleCms.Ref4,
                    ref5: detalleCms.Ref5);

                Config.EscribirLog($"INFO|PaP_FXD.CMS_A_TOMI|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|PaP_FXD.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        private async Task<bool> CMS_A_TOMI_MAESTRO(
            string folioMaestro,
            PaPFxdMaestroCmsDto maestroCms,
            PaPFxdContenidoDto maestroContenido,
            string folioUnicoGenerado)
        {
            try
            {
                bool resultado = await _PaPFXDDao.InsertarFormatoPapFxdMaestroEnTomi(
                    nfFolioUnico: folioUnicoGenerado,
                    nfTipo: "FXD",
                    nfFolio: maestroCms.IdFormato,
                    t24CustomerCode: DameTexto(maestroCms.Cdl),
                    t24DebitAcctNo: DameTexto(maestroCms.IdCuenta),
                    nfEstatus: "PEND",
                    nfUsuarioCaptura: maestroCms.IdUsuarioCaptura,
                    nfUsuarioAutoriza: maestroCms.IdUsuarioAutorizo,
                    nfFechaCaptura: maestroCms.FechaCaptura,
                    nfFechaAutoriza: maestroCms.FechaAutorizacion,
                    t24DebitAmount: maestroCms.MontoOperacion,
                    t24ShortName: DameTexto(maestroContenido.Valores[1]),
                    nfForexRelacionado: DameTexto(maestroContenido.Valores[3]),
                    t24DebitCurrency: DameTexto(maestroContenido.Valores[4]),
                    nfBancoBenef: DameTexto(maestroContenido.Valores[6]),
                    t24BenAcctNo: DameTexto(maestroContenido.Valores[7]),
                    nfTipoCuenta: DameTexto(maestroContenido.Valores[8]),
                    t24BenCustName: DameTexto(maestroContenido.Valores[10]),
                    nfRfcBeneficiario: DameTexto(maestroContenido.Valores[11]),
                    nfTasaRetencion: DameTexto(maestroContenido.Valores[12]),
                    nfFechaEjecucion: ConversionHelper.ParseFechaEntera(DameTexto(maestroContenido.Valores[13])),
                    t24DebitTheirRef: DameTexto(maestroContenido.Valores[15]),
                    nfMontoEnviar: ConversionHelper.ParseDecimalSeguroConDefaultCero(maestroContenido.Valores[18]),
                    idPlantilla: maestroCms.IdPlantilla,
                    idFormatoPlantilla: maestroCms.IdFormatoPlantilla,
                    idFilesBtmum: maestroCms.IdFilesBtmum,
                noPagos: ConversionHelper.ConvertLongToNullableInt(maestroCms.NoPagos),
                    pagosDescripcion: maestroCms.PagosDescripcion,
                noPagosProcesados: ConversionHelper.ConvertLongToNullableInt(maestroCms.NoPagosProcesados),
                noPagosDevueltos: ConversionHelper.ConvertLongToNullableInt(maestroCms.NoPagosDevueltos));

                Config.EscribirLog($"INFO|PaP_FXD.CMS_A_TOMI_MAESTRO|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|PaP_FXD.CMS_A_TOMI_MAESTRO|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        private async Task<bool> TOMI_ENTRADA_SALIDA(
            string folio,
            PaPFxdDetalleCmsDto detalleCms,
            PaPFxdContenidoDto detalleContenido,
            string folioUnicoGenerado)
        {
            try
            {
                long folioNumero = ConversionHelper.ParseLongSeguro(folio, nameof(folio));

                var detallesTomi = await _PaPFXDDao.ObtenerDetallePapFxdTomiPorFolio(folioNumero);
                if (detallesTomi == null || detallesTomi.Count == 0)
                    throw new Exception($"No se encontraron datos TOMI para generar Entrada/Salida del folio={folio}.");

                PapFxdDetalleTomiDto detalleTomi = null;
                foreach (var item in detallesTomi)
                {
                    detalleTomi = item;
                }

                bool resultado = await _PaPFXDDao.InsertarEntradaSalidaSrv(
                    referencia: string.IsNullOrWhiteSpace(detalleTomi.NfFolioUnico) ? folioUnicoGenerado : detalleTomi.NfFolioUnico,
                numCliente: ConversionHelper.ParseIntOrZero(detalleTomi.T24CustomerCode, "T24_CUSTOMER_CODE"),
                cdl: ConversionHelper.ParseDoubleOrZero(detalleTomi.T24CustomerCode, "T24_CUSTOMER_CODE"),
                    ordenante: detalleTomi.T24CompanyName,
                    operacion: 218d,
                    operacionDesc: "Salida  (TTS)",
                    tipo: "D",
                    tipoDesc: "Doméstica",
                cuenta: ConversionHelper.ParseDoubleOrZero(detalleTomi.T24DebitAcctNo, "T24_DEBIT_ACCT_NO"),
                    nombre: detalleTomi.NfNombreCuenta,
                    cuentaMoneda: detalleTomi.NfMonedaCuenta,
                    via: 800d,
                    viaDesc: "TRF ENVIADA MXN SPEUA",
                importe: ConversionHelper.ToDouble(detalleTomi.T24DebitAmount),
                    saldo: 0d,
                    comentarios: detalleTomi.T24DebitTheirRef,
                    fechaCaptura: ConversionHelper.ParseFechaSistema(_configuracion.StrFechaSistema),
                    fechaValor: ConversionHelper.ParseFechaSistema(DameTexto(detalleTomi.NfFechaEjecucion)),
                    hora: DateTime.Now.ToLongTimeString(),
                    capturista: "JEAI",
                    fad: 0d,
                    referenciaControl: "A",
                    referenciaControlCont: 0d);

                Config.EscribirLog($"INFO|PaP_FXD.TOMI_ENTRADA_SALIDA|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|PaP_FXD.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }


        // ---------------------------------------------------------------------
        // HELPERS
        // ---------------------------------------------------------------------

        private static PaPFxdContenidoDto ParseContenidoFormato(string contenidoFormato, string folio)
        {
            if (string.IsNullOrWhiteSpace(contenidoFormato))
                throw new Exception($"El contenidoFormato del folio {folio} viene vacío.");

            string[] valoresPipes = contenidoFormato.Split('|');
            if (valoresPipes.Length < 19)
                throw new Exception($"El contenidoFormato del folio {folio} es diferente al esperado, debe ser 18 datos y se tienen {valoresPipes.Length - 1}.");

            string[] valores = new string[19];

            for (int i = 0; i <= 18; i++)
            {
                string[] llaveValor = valoresPipes[i].Split(new[] { '=' }, 2);

                if (llaveValor.Length < 2)
                    throw new Exception($"El contenidoFormato del folio {folio} tiene un elemento inválido en la posición {i}: '{valoresPipes[i]}'.");

                valores[i] = llaveValor[1];
            }

            return new PaPFxdContenidoDto
            {
                Valores = valores,
                NombreCliente = valores[1] ?? string.Empty,
                Moneda = valores[4] ?? string.Empty,
                FechaEjecucion = valores[13] ?? string.Empty
            };
        }

    }
}

using Aplicacion.Interfaces.PaP_FXT;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa Pa P FXT.
    /// </summary>
    public class PaP_FXT
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IPaPFXTCmsDao _paPFXTCmsDao;
        private readonly IPaPFXTTomiDao _paPFXTTomiDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "PaP_FXT";
        private string folioUnico = "";

        /// <summary>
        /// Constructor.
        /// </summary>
        public PaP_FXT(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IPaPFXTCmsDao paPFXTCmsDao,
            IPaPFXTTomiDao paPFXTTomiDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _paPFXTCmsDao = paPFXTCmsDao;
            _paPFXTTomiDao = paPFXTTomiDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación PaP_FXT del servicio Lectura.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio)
        {
            PaPFXTFormatoPapMaestroCmsDto maestroCms;
            PaPFXTFormatoPapDetalleCmsDto detalleCms;
            PaPFXTContenidoFormatoDto contenidoMaestro = new PaPFXTContenidoFormatoDto();
            PaPFXTContenidoFormatoDto contenidoDetalle = new PaPFXTContenidoFormatoDto();

            long mtoFolio = 0;
            int mtoRegs = 0;
            string mtoFechaEjecucion = string.Empty;
            string mtoMoneda = string.Empty;
            decimal mtoImporte = 0m;
            string mtoFolioForex = string.Empty;

            string detFolioForex = string.Empty;
            string detFechaEjecucion = string.Empty;
            string detMoneda = string.Empty;
            decimal detImporte = 0m;

            bool conForex = false;
            bool forexCancelado = false;
            bool procesado;
            bool fechaFutura;
            bool resultado;

            int tomiMtoRegs = 0;
            decimal tomiMtoImporte = 0m;

            long folioNumerico = Convert.ToInt64(folio);

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

            if (string.Equals(_configuracion.AliasProceso, "", StringComparison.Ordinal))
                return false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Aperturando conexión a base de datos del CMS.");

            // Datos del maestro.
            maestroCms = await _paPFXTCmsDao.ObtenerFormatoPapMaestroPorFolio(folio);
            if (maestroCms != null)
            {
                contenidoMaestro = ParsearContenidoFormatoPaPFXT(maestroCms.ContenidoFormato, folio);

                mtoFolio = maestroCms.IdFormato;
                mtoRegs = Convert.ToInt32(maestroCms.NoPagos);
                mtoImporte = maestroCms.MontoOperacion;

                mtoFolioForex = string.Empty;

                if (contenidoMaestro.FechaEjecucion.HasValue)
                    mtoFechaEjecucion = contenidoMaestro.FechaEjecucion.Value.ToString();

                if (!string.IsNullOrWhiteSpace(contenidoMaestro.Moneda))
                    mtoMoneda = _vwT24.ValidaMoneda(contenidoMaestro.Moneda);
            }

            // Datos del detalle.
            detalleCms = await _paPFXTCmsDao.ObtenerFormatoPapDetallePorFolio(folio);
            if (detalleCms != null)
            {
                contenidoDetalle = ParsearContenidoFormatoPaPFXT(detalleCms.ContenidoFormato, folio);

                detImporte = detalleCms.MontoOperacion;

                detFolioForex = string.Empty;

                if (contenidoDetalle.FechaEjecucion.HasValue)
                    detFechaEjecucion = contenidoDetalle.FechaEjecucion.Value.ToString();

                if (!string.IsNullOrWhiteSpace(contenidoDetalle.Moneda))
                    detMoneda = _vwT24.ValidaMoneda(contenidoDetalle.Moneda);
            }

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(mtoFechaEjecucion, CultureInfo.InvariantCulture);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception($"Se desea realizar una operación al {mtoFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // Genero el folio único.
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(mtoFechaEjecucion),
                folio);

            // Fecha futura.
            fechaFutura = string.CompareOrdinal(mtoFechaEjecucion, _configuracion.StrFechaSistema) > 0;

            // Validación de maestro y detalle.
            if (!string.Equals(mtoFolioForex, detFolioForex, StringComparison.Ordinal))
            {
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Forex: Maestro:'{mtoFolioForex}', Detalle:'{detFolioForex}'.");
            }

            if (!string.Equals(mtoFechaEjecucion, detFechaEjecucion, StringComparison.Ordinal))
            {
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Fecha de ejecución: Maestro:'{mtoFechaEjecucion}', Detalle:'{detFechaEjecucion}'.");
            }

            if (!string.Equals(mtoMoneda, detMoneda, StringComparison.Ordinal))
            {
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Moneda: Maestro:'{mtoMoneda}', Detalle:'{detMoneda}'.");
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Validando si la operación está asignada a un Forex.");

            if (!string.Equals(mtoFolioForex, "", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Asignado el folio forex: {mtoFolioForex} .");

                conForex = true;

                if (ConversionHelper.EsNumericoInvariant(mtoFolioForex))
                    throw new Exception($"El Folio del Forex no es valido: {mtoFolioForex}");

                PaPFXTForexEstatusTomiDto estatusForex = await _paPFXTTomiDao.ObtenerEstatusForexPorReferencia(mtoFolioForex);
                if (estatusForex != null &&
                    string.Equals(estatusForex.StatusOperacion, "CANCELADO", StringComparison.Ordinal))
                {
                    forexCancelado = true;
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|FOREX CANCELADO.");
                }
            }

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string subTipo = string.Empty;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");

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

                if (conForex)
                    subTipo = $"FX:{mtoFolioForex}";

                procesado = await _paPFXTTomiDao.ActualizarDatosGeneralesPlanTrabajo(
                    new PaPFXTActualizarDatosGeneralesPlanTrabajoTomiDto
                    {
                        Folio = folioNumerico,
                        FolioMaestro = mtoFolio,
                        TipoOperacion = tipoOperacion,
                        FolioUnico = folioUnico,
                        FecValor = Convert.ToDecimal(mtoFechaEjecucion),
                        CveMoneda = mtoMoneda,
                        ImporteDetalle = detImporte,
                        ImporteMaestro = mtoImporte,
                        NumCliente = detalleCms.Cdl.HasValue ? Convert.ToInt32(detalleCms.Cdl.Value) : (int?)null,
                        NombreCliente = contenidoDetalle.NombreCliente,
                        NumCuenta = detalleCms.IdCuenta?.ToString(),
                        NumPagos = mtoRegs,
                        SubtipoOperacion = subTipo
                    });

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
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
                }
            }

            // Lectura del CMS e inserción a TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CMS_A_TOMI", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A insertar datos de CMS a TOMI.");

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

                resultado = await CMS_A_TOMI(
                    folio,
                    /// <summary>
                    /// Ejecuta la operaciÃ³n ConstruirInsertarDetalleTomiDto.
                    /// </summary>
                    ConstruirInsertarDetalleTomiDto(folioUnico, detalleCms, contenidoDetalle));

                if (resultado)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                    procesado = true;
                }
                else
                {
                    // Posiblemente se restauró el servicio.
                    PaPFXTExisteRegistroTomiDto existeDetalle = await _paPFXTTomiDao.ExisteFormatoPapDetallePorFolio(folioNumerico);
                    if (existeDetalle != null && existeDetalle.NumRegs > 0)
                    {
                        procesado = true;
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

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
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CMS_A_TOMI con folio: {folio}");
                }
            }

            // Actualización del estatus del CMS y TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A actualizar estatus en CMS y TOMI. Buscando sumarizado de los detalles del folioMaestro={mtoFolio}");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                PaPFXTSumarizadoDetalleTomiDto sumarizadoDetalle = await _paPFXTTomiDao.ObtenerSumarizadoDetallePorIdFormatoPap(mtoFolio);
                if (sumarizadoDetalle != null)
                {
                    tomiMtoRegs = sumarizadoDetalle.NumRegs;
                    tomiMtoImporte = sumarizadoDetalle.ImpMonto ?? 0m;
                }

                procesado = false;

                if (mtoRegs == tomiMtoRegs &&
                    Math.Round(mtoImporte, 2) == Math.Round(tomiMtoImporte, 2))
                {
                    PaPFXTExisteRegistroTomiDto existeDetalleProcesado = await _paPFXTTomiDao.ExisteFormatoPapDetalleProcesadoPorFolio(folioNumerico);
                    if (existeDetalleProcesado != null && existeDetalleProcesado.NumRegs > 0)
                    {
                        procesado = true;
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El folio ya se había marcado como realizado.");
                    }

                    if (!procesado)
                    {
                        resultado = await CMS_A_TOMI_MAESTRO(
                            mtoFolio.ToString(CultureInfo.InvariantCulture),
                            /// <summary>
                            /// Ejecuta la operaciÃ³n ConstruirInsertarMaestroTomiDto.
                            /// </summary>
                            ConstruirInsertarMaestroTomiDto(folioUnico, maestroCms, contenidoMaestro));

                        if (resultado)
                        {
                            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio maestro {mtoFolio} en base de TOMI.");
                            procesado = true;
                        }
                        else
                        {
                            PaPFXTExisteRegistroTomiDto existeMaestro = await _paPFXTTomiDao.ExisteFormatoPapMaestroPorFolio(mtoFolio);
                            if (existeMaestro != null && existeMaestro.NumRegs > 0)
                            {
                                procesado = true;
                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El folio maestro en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                            }
                        }

                        if (procesado)
                        {
                            if (conForex && forexCancelado)
                            {
                                await _paPFXTCmsDao.ActualizarEstatusFormatoPap(new PaPFXTActualizarEstatusCmsDto
                                {
                                    IdFormatoPap = mtoFolio,
                                    Mensaje = "Formato rechazado",
                                    Estatus = "RECC",
                                    RazonRechazo = "El forex relacionado ha sido cancelado"
                                });

                                await _paPFXTTomiDao.ActualizarEstatusFormatoPapTomi(new PaPFXTActualizarEstatusTomiDto
                                {
                                    Folio = folioNumerico,
                                    IdFormatoPap = mtoFolio,
                                    NfEstatus = "RECC",
                                    NfRazon = "El forex relacionado ha sido cancelado",
                                    NfUsuarioConfirma = null,
                                    NfFechaConfirma = null
                                });

                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Forex Rechazado con Anterioridad: {mtoFolio}");

                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A cancelar la operación.");

                                await _paPFXTTomiDao.CancelarPlanTrabajoPorForex(new PaPFXTCancelarPlanTrabajoPorForexTomiDto
                                {
                                    Folio = folioNumerico,
                                    FolioMaestro = Convert.ToInt64(await _vwT24.ObtenerFolioMaestro(folio, tipoOperacion)),
                                    TipoOperacion = tipoOperacion,
                                    FecValor = Convert.ToDecimal(mtoFechaEjecucion),
                                    CveMoneda = mtoMoneda,
                                    ImporteDetalle = detImporte,
                                    ImporteMaestro = mtoImporte,
                                    ErrDescripcion = "El forex relacionado ha sido cancelado"
                                });

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

                            if (string.Equals(mtoFechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                            {
                                DateTime fechaConfirma = DateTime.ParseExact(
                                    DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                                    "yyyy/MM/dd HH:mm",
                                    CultureInfo.InvariantCulture);

                                await _paPFXTCmsDao.ActualizarEstatusFormatoPapSinRazon(new PaPFXTActualizarEstatusCmsDto
                                {
                                    IdFormatoPap = mtoFolio,
                                    Mensaje = "Formato aceptado",
                                    Estatus = "APFT"
                                });

                                await _paPFXTTomiDao.ActualizarEstatusFormatoPapTomi(new PaPFXTActualizarEstatusTomiDto
                                {
                                    Folio = folioNumerico,
                                    IdFormatoPap = mtoFolio,
                                    NfEstatus = "CONP",
                                    NfRazon = null,
                                    NfUsuarioConfirma = "OPER CMS",
                                    NfFechaConfirma = fechaConfirma
                                });

                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Operación fecha valor hoy.");
                            }
                            else if (string.CompareOrdinal(mtoFechaEjecucion, _configuracion.StrFechaSistema) < 0)
                            {
                                await _controlServicio.MarcarErrorFechaPasada(folioNumerico);
                                throw new Exception($"Se desea realizar una operación al {mtoFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                            }
                            else
                            {
                                DateTime fechaConfirma = DateTime.ParseExact(
                                    DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                                    "yyyy/MM/dd HH:mm",
                                    CultureInfo.InvariantCulture);

                                await _paPFXTCmsDao.ActualizarEstatusFormatoPapSinRazon(new PaPFXTActualizarEstatusCmsDto
                                {
                                    IdFormatoPap = mtoFolio,
                                    Mensaje = "Formato recibido pendiente ejecución",
                                    Estatus = "REPE"
                                });

                                await _paPFXTTomiDao.ActualizarEstatusFormatoPapTomi(new PaPFXTActualizarEstatusTomiDto
                                {
                                    Folio = folioNumerico,
                                    IdFormatoPap = mtoFolio,
                                    NfEstatus = "CONF",
                                    NfRazon = null,
                                    NfUsuarioConfirma = "OPER CMS",
                                    NfFechaConfirma = fechaConfirma
                                });

                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Operación fecha valor fútura.");
                            }
                        }
                    }
                }
                else
                {
                    Config.EscribirLog($"AVISO|{tipoOperacion}.procesaOperacion|Faltan detalles a procesar para marcar el cambio de estatus en el folio Maestro:{mtoFolio}.");
                    return true;
                }

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
                string mensajeError = string.Empty;
                decimal importeTomi = 0m;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A conciliar operaciones entre CMS y TOMI (maestro y detalles).");

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
                importeTomi = 0m;

                PaPFXTConciliacionDetalleTomiDto detalleConciliacion = await _paPFXTTomiDao.ObtenerImporteDetalleConciliacionPorFolio(folioNumerico);
                if (detalleConciliacion != null)
                {
                    regsTomi = 1;
                    importeTomi = detalleConciliacion.T24DebitAmount ?? 0m;
                }

                tomiMtoRegs = 0;
                tomiMtoImporte = 0m;

                PaPFXTSumarizadoDetalleTomiDto sumarizadoProcesado = await _paPFXTTomiDao.ObtenerSumarizadoDetalleProcesadoPorIdFormatoPap(mtoFolio);
                if (sumarizadoProcesado != null)
                {
                    tomiMtoRegs = sumarizadoProcesado.NumRegs;
                    tomiMtoImporte = sumarizadoProcesado.ImpMonto ?? 0m;
                }

                if (regsCms == 1 && regsTomi == 1)
                {
                    if (detImporte != importeTomi)
                    {
                        mensajeError += Environment.NewLine +
                                        $" - Diferencia en el importe. CMS='{detImporte}', TOMI='{importeTomi}'.";
                    }

                    if (mtoRegs != tomiMtoRegs)
                    {
                        mensajeError += Environment.NewLine +
                                        $" - Diferencia en el total de registros. CMS='{mtoRegs}', TOMI='{tomiMtoRegs}'.";
                    }

                    if (Math.Round(mtoImporte, 2) != Math.Round(tomiMtoImporte, 2))
                    {
                        mensajeError += Environment.NewLine +
                                        $" - Diferencia en la sumatoría del importe de registros. CMS='{mtoImporte}', TOMI='{tomiMtoImporte}'.";
                    }

                    procesado = true;
                }
                else
                {
                    procesado = false;
                    mensajeError += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                }

                if (!string.Equals(mensajeError, "", StringComparison.Ordinal))
                {
                    procesado = false;
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(mensajeError);
                }

                if (procesado)
                {
                    if (fechaFutura)
                    {
                        await _paPFXTTomiDao.MarcarPlanTrabajoFechaFutura(new PaPFXTMarcarFechaFuturaTomiDto
                        {
                            Folio = folioNumerico,
                            FolioMaestro = mtoFolio,
                            TipoOperacion = tipoOperacion
                        });
                    }

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación está correctamente conciliada.");

                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso = "";
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CONCILIAR con folio: {folio}");
                }
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Cerrando conexión a base de datos del CMS.");

            return true;
        }

        //=================================================================================================================
        // Inserta registro en la base de datos de TOMI (los datos son del CMS) - DETALLES
        //=================================================================================================================
        private async Task<bool> CMS_A_TOMI(string folio, PaPFXTInsertarDetalleTomiDto datosDetalle)
        {
            try
            {
                bool resultado = await _paPFXTTomiDao.InsertarFormatoPapDetalle(datosDetalle);

                Config.EscribirLog($"INFO|{tipoOperacion}.CMS_A_TOMI|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{tipoOperacion}.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        //=================================================================================================================
        // Inserta registro en la base de datos de TOMI (los datos son del CMS) - MAESTRO
        //=================================================================================================================
        private async Task<bool> CMS_A_TOMI_MAESTRO(string folioMaestro, PaPFXTInsertarMaestroTomiDto datosMaestro)
        {
            try
            {
                bool resultado = await _paPFXTTomiDao.InsertarFormatoPapMaestro(datosMaestro);

                Config.EscribirLog($"INFO|{tipoOperacion}.CMS_A_TOMI_MAESTRO|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{tipoOperacion}.CMS_A_TOMI_MAESTRO|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        private static PaPFXTContenidoFormatoDto ParsearContenidoFormatoPaPFXT(string contenidoFormato, string folio)
        {
            if (string.IsNullOrWhiteSpace(contenidoFormato))
                throw new Exception($"El contenidoFormato del folio {folio} viene vacío.");

            string[] valoresPipes = contenidoFormato.Split('|');

            if (valoresPipes.Length < 19)
            {
                throw new Exception(
                    $"El contenidoFormato del folio {folio} es diferente al esperado, debe ser 18 datos y se tienen {valoresPipes.Length - 1}.");
            }

            string[] valores = new string[19];

            for (int i = 0; i <= 18; i++)
            {
                string valorPipe = valoresPipes[i] ?? string.Empty;
                string[] llaveValor = valorPipe.Split('=');
                valores[i] = llaveValor[1];
            }

            int fechaEjecucion;
            int? fechaEjecucionNullable = null;

            if (!string.IsNullOrWhiteSpace(valores[13]) &&
                int.TryParse(valores[13], out fechaEjecucion))
            {
                fechaEjecucionNullable = fechaEjecucion;
            }

            decimal montoEnviar;
            decimal? montoEnviarNullable = null;

            if (!string.IsNullOrWhiteSpace(valores[18]) &&
                decimal.TryParse(valores[18], out montoEnviar))
            {
                montoEnviarNullable = montoEnviar;
            }

            return new PaPFXTContenidoFormatoDto
            {
                Valor0 = valores[0],
                NombreCliente = valores[1],
                Valor2 = valores[2],
                FolioForexRelacionado = valores[3],
                Moneda = valores[4],
                Valor5 = valores[5],
                BancoBeneficiario = valores[6],
                CuentaBeneficiario = valores[7],
                TipoCuenta = valores[8],
                Valor9 = valores[9],
                NombreBeneficiario = valores[10],
                RfcBeneficiario = valores[11],
                TasaRetencion = valores[12],
                FechaEjecucion = fechaEjecucionNullable,
                Valor14 = valores[14],
                ReferenciaDebito = valores[15],
                Valor16 = valores[16],
                Valor17 = valores[17],
                MontoEnviar = montoEnviarNullable
            };
        }

        private static PaPFXTInsertarDetalleTomiDto ConstruirInsertarDetalleTomiDto(
    string folioUnico,
    PaPFXTFormatoPapDetalleCmsDto detalleCms,
    PaPFXTContenidoFormatoDto contenido)
        {
            return new PaPFXTInsertarDetalleTomiDto
            {
                NfFolioUnico = folioUnico,
                NfTipo = "FXT",
                NfFolio = Convert.ToInt32(detalleCms.IdFormato),
                T24CustomerCode = detalleCms.Cdl.HasValue
                    ? Convert.ToInt64(detalleCms.Cdl.Value).ToString()
                    : null,
                T24DebitAcctNo = detalleCms.IdCuenta?.ToString(),
                NfEstatus = "PEND",
                NfUsuarioCaptura = detalleCms.IdUsuarioCaptura,
                NfUsuarioAutoriza = detalleCms.IdUsuarioAutorizo,
                NfFechaCaptura = detalleCms.FechaCaptura,
                NfFechaAutoriza = detalleCms.FechaAutorizacion,
                T24DebitAmount = detalleCms.MontoOperacion,

                // contenidoFormato posición 1
                T24ShortName = contenido.NombreCliente,

                // contenidoFormato posición 3
                NfForexRelacionado = contenido.FolioForexRelacionado,

                // contenidoFormato posición 4
                T24DebitCurrency = contenido.Moneda,

                // contenidoFormato posición 6
                NfBancoBenef = contenido.BancoBeneficiario,

                // contenidoFormato posición 7
                T24BenAcctNo = contenido.CuentaBeneficiario,

                // contenidoFormato posición 8
                NfTipoCuenta = contenido.TipoCuenta,

                // contenidoFormato posición 10
                T24BenCustName = TextoSPEI(contenido.NombreBeneficiario),

                // contenidoFormato posición 11
                NfRfcBeneficiario = TextoSPEI(contenido.RfcBeneficiario),

                // contenidoFormato posición 12
                NfTasaRetencion = contenido.TasaRetencion,

                // contenidoFormato posición 13
                NfFechaEjecucion = contenido.FechaEjecucion?.ToString(CultureInfo.InvariantCulture),

                // contenidoFormato posición 15
                T24DebitTheirRef = TextoSPEI(contenido.ReferenciaDebito),

                // contenidoFormato posición 18
                NfMontoEnviar = contenido.MontoEnviar,

                IdPlantilla = detalleCms.IdPlantilla,
                IdFormatoPlantilla = detalleCms.IdFormatoPlantilla,
                IdFilesBtmum = detalleCms.IdFilesBtmum,
                NoPagos = Convert.ToInt32(detalleCms.NoPagos),
                PagosDescripcion = detalleCms.PagosDescripcion,
                NoPagosProcesados = Convert.ToInt32(detalleCms.NoPagosProcesados),
                NoPagosDevueltos = Convert.ToInt32(detalleCms.NoPagosDevueltos),
                IdFormatoPap = detalleCms.IdFormatoPap,
                Ref1 = detalleCms.Ref1,
                Ref2 = detalleCms.Ref2,
                Ref3 = detalleCms.Ref3,
                Ref4 = detalleCms.Ref4,
                Ref5 = detalleCms.Ref5
            };
        }

        private static PaPFXTInsertarMaestroTomiDto ConstruirInsertarMaestroTomiDto(
    string folioUnico,
    PaPFXTFormatoPapMaestroCmsDto maestroCms,
    PaPFXTContenidoFormatoDto contenido)
        {
            decimal montoEnviar = 0m;

            if (contenido.MontoEnviar.HasValue)
                montoEnviar = contenido.MontoEnviar.Value;

            return new PaPFXTInsertarMaestroTomiDto
            {
                NfFolioUnico = folioUnico,
                NfTipo = "FXT",
                NfFolio = Convert.ToInt32(maestroCms.IdFormato),
                T24CustomerCode = maestroCms.Cdl.HasValue
                    ? Convert.ToInt64(maestroCms.Cdl.Value).ToString()
                    : null,
                T24DebitAcctNo = maestroCms.IdCuenta?.ToString(),
                NfEstatus = "PEND",
                NfUsuarioCaptura = maestroCms.IdUsuarioCaptura,
                NfUsuarioAutoriza = maestroCms.IdUsuarioAutorizo,
                NfFechaCaptura = maestroCms.FechaCaptura,
                NfFechaAutoriza = maestroCms.FechaAutorizacion,
                T24DebitAmount = maestroCms.MontoOperacion,

                // contenidoFormato posición 1
                T24ShortName = contenido.NombreCliente,

                // contenidoFormato posición 3
                NfForexRelacionado = contenido.FolioForexRelacionado,

                // contenidoFormato posición 4
                T24DebitCurrency = contenido.Moneda,

                // contenidoFormato posición 6
                NfBancoBenef = contenido.BancoBeneficiario,

                // contenidoFormato posición 7
                T24BenAcctNo = contenido.CuentaBeneficiario,

                // contenidoFormato posición 8
                NfTipoCuenta = contenido.TipoCuenta,

                // contenidoFormato posición 10
                // En legacy maestro NO aplicaba texto_SPEI aquí.
                T24BenCustName = contenido.NombreBeneficiario,

                // contenidoFormato posición 11
                // En legacy maestro NO aplicaba texto_SPEI aquí.
                NfRfcBeneficiario = contenido.RfcBeneficiario,

                // contenidoFormato posición 12
                NfTasaRetencion = contenido.TasaRetencion,

                // contenidoFormato posición 13
                NfFechaEjecucion = contenido.FechaEjecucion?.ToString(CultureInfo.InvariantCulture),

                // contenidoFormato posición 15
                // En legacy maestro NO aplicaba texto_SPEI aquí.
                T24DebitTheirRef = contenido.ReferenciaDebito,

                // contenidoFormato posición 18, con fallback legacy a 0.
                NfMontoEnviar = montoEnviar,

                IdPlantilla = maestroCms.IdPlantilla,
                IdFormatoPlantilla = maestroCms.IdFormatoPlantilla,
                IdFilesBtmum = maestroCms.IdFilesBtmum,
                NoPagos = Convert.ToInt32(maestroCms.NoPagos),
                PagosDescripcion = maestroCms.PagosDescripcion,
                NoPagosProcesados = Convert.ToInt32(maestroCms.NoPagosProcesados),
                NoPagosDevueltos = Convert.ToInt32(maestroCms.NoPagosDevueltos)
            };
        }

    }

}

using Aplicacion.DTOs.PaP_SPID;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.PaP_SPID;
using Aplicacion.Interfaces.SPID;
using Aplicacion.Modelos;
using Aplicacion.Servicios.FirmaElectronica;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa Pa P SPID.
    /// </summary>
    public class PaP_SPID
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IPaPSpidTomiDao _papSpidDao;
        private readonly IPaPSpidCmsDao _papSpidCmsDao;
        private readonly IPaPSpidInsightDao _papSpidInsightDao;
        private readonly ConexionT24Mw _vwT24;
        private readonly ComunSPID _comunSPID;
        private readonly SPID _spid;

        private const string tipoOperacion = "PaP_SPID";
        private string folioUnico = string.Empty;
        private string folioPropuesto = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase PaP_SPID.
        /// </summary>

        public PaP_SPID(ConfiguracionModelo configuracion, IControlServicio controlServicio, IPaPSpidTomiDao papSpidDao, IPaPSpidCmsDao papSpidCmsDao, IPaPSpidInsightDao papSpidInsightDao, ConexionT24Mw vwT24, ComunSPID comunSPID, SPID spid)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _papSpidDao = papSpidDao;
            _papSpidCmsDao = papSpidCmsDao;
            _papSpidInsightDao = papSpidInsightDao;
            _vwT24 = vwT24;
            _comunSPID = comunSPID;
            _spid = spid;
        }

        /// <summary>
        /// Procesa la operación PaP_SPID.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio, CancellationToken ct = default(CancellationToken))
        {
            ct.ThrowIfCancellationRequested();

            object[] valoresFormatoMto = new object[41];
            object[] valoresFormato = new object[51];
            string mtoFolio;
            int mtoRegs = 0;
            string mtoFechaEjecucion;
            string mtoMoneda;
            decimal mtoImporte;
            string detFolioForex;
            string detFechaEjecucion;
            string detMoneda;
            decimal detImporte;
            bool conForex;
            bool forexCancelado;
            string mtoFolioForex;
            bool conEntradaSalida;
            bool procesado;
            bool fechaFutura;
            bool resultado;
            int tomiMtoRegs = 0;
            decimal tomiMtoImporte = 0m;

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

            // Confirmo que se encuentre el certificado y llave para la firma digital.
            using (var firma = new MetodosFirma(_configuracion))
            {
                if (!firma.CertificadoEncontrado())
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de SPID: No se encontro el certificado y/o llave para realizar la firma de la transacción");
            }

            // Datos del folio en CMS y TOMI.
            mtoFolio = string.Empty;
            mtoFechaEjecucion = string.Empty;
            mtoMoneda = string.Empty;
            mtoImporte = 0m;
            mtoFolioForex = string.Empty;
            detFechaEjecucion = string.Empty;
            detMoneda = string.Empty;
            detImporte = 0m;
            detFolioForex = string.Empty;
            conForex = false;
            forexCancelado = false;
            conEntradaSalida = true;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Aperturando conexión a base de datos del CMS.");

            // Datos del maestro.
            PaPSpidFormatoPapCmsDto datosMto = await _papSpidCmsDao.ObtenerMaestroPorFolioDetalle(folio);
            if (datosMto != null)
            {
                valoresFormatoMto[19] = "SPD";
                valoresFormatoMto[20] = datosMto.id_Formato;
                valoresFormatoMto[21] = datosMto.cdl;
                valoresFormatoMto[22] = datosMto.id_Cuenta;
                valoresFormatoMto[23] = datosMto.FechaCaptura;
                valoresFormatoMto[24] = datosMto.id_UsuarioCaptura;
                valoresFormatoMto[25] = datosMto.FechaAutorizacion;
                valoresFormatoMto[26] = datosMto.id_UsuarioAutorizo;
                valoresFormatoMto[27] = datosMto.MontoOperacion;
                valoresFormatoMto[28] = datosMto.id_Plantilla;
                valoresFormatoMto[29] = datosMto.id_Formato_Plantilla;
                valoresFormatoMto[30] = datosMto.id_files_btmum;
                valoresFormatoMto[31] = datosMto.nopagos;
                valoresFormatoMto[32] = datosMto.pagosdescripcion;
                valoresFormatoMto[33] = datosMto.nopagosprocesados;
                valoresFormatoMto[34] = datosMto.nopagosdevueltos;

                string[] valoresPipesMto = (datosMto.ContenidoFormato ?? string.Empty)
                    .Split(new[] { "|" }, StringSplitOptions.None);

                if (valoresPipesMto.Length - 1 < 18)
                    throw new Exception($"El contenidoFormato del folio {folio} es diferente al esperado, debe ser 18 datos y se tienen {valoresPipesMto.Length - 1}.");

                for (int idx = 0; idx <= 18; idx++)
                {
                    string[] llaveValor = valoresPipesMto[idx].Split(new[] { "=" }, 2, StringSplitOptions.None);
                    valoresFormatoMto[idx] = llaveValor[1];
                }

                mtoFolio = Convert.ToString(datosMto.id_Formato);
                mtoRegs = ConversionHelper.ConvertLongToInt(datosMto.nopagos);
                mtoImporte = datosMto.MontoOperacion;
                mtoFolioForex = string.Empty;

                if (valoresFormatoMto[13] != null)
                    mtoFechaEjecucion = Convert.ToString(valoresFormatoMto[13]);

                if (valoresFormatoMto[4] != null)
                    mtoMoneda = _vwT24.ValidaMoneda(Convert.ToString(valoresFormatoMto[4]));
            }

            // Datos del detalle.
            PaPSpidFormatoPapDetalleCmsDto datosDet = await _papSpidCmsDao.ObtenerDetallePorFolio(folio);
            if (datosDet != null)
            {
                valoresFormato[19] = "SPD";
                valoresFormato[20] = datosDet.id_Formato;
                valoresFormato[21] = datosDet.cdl;
                valoresFormato[22] = datosDet.id_Cuenta;
                valoresFormato[23] = datosDet.FechaCaptura;
                valoresFormato[24] = datosDet.id_UsuarioCaptura;
                valoresFormato[25] = datosDet.FechaAutorizacion;
                valoresFormato[26] = datosDet.id_UsuarioAutorizo;
                valoresFormato[27] = datosDet.MontoOperacion;
                valoresFormato[28] = datosDet.id_Plantilla;
                valoresFormato[29] = datosDet.id_Formato_Plantilla;
                valoresFormato[30] = datosDet.id_files_btmum;
                valoresFormato[31] = datosDet.nopagos;
                valoresFormato[32] = datosDet.pagosdescripcion;
                valoresFormato[33] = datosDet.nopagosprocesados;
                valoresFormato[34] = datosDet.nopagosdevueltos;
                valoresFormato[35] = datosDet.id_formato_pap;
                valoresFormato[36] = datosDet.ref1;
                valoresFormato[37] = datosDet.ref2;
                valoresFormato[38] = datosDet.ref3;
                valoresFormato[39] = datosDet.ref4;
                valoresFormato[40] = datosDet.ref5;
                valoresFormato[41] = datosDet.ClasificacionOper;
                valoresFormato[42] = datosDet.RelacionComercial;
                valoresFormato[43] = datosDet.ReferenciaNumerica;
                valoresFormato[44] = datosDet.IPOrigen;

                string[] valoresPipes = (datosDet.ContenidoFormato ?? string.Empty)
                    .Split(new[] { "|" }, StringSplitOptions.None);

                if (valoresPipes.Length - 1 < 18)
                    throw new Exception($"El contenidoFormato del folio {folio} es diferente al esperado, debe ser 18 datos y se tienen {valoresPipes.Length - 1}.");

                for (int idx = 0; idx <= 18; idx++)
                {
                    string[] llaveValor = valoresPipes[idx].Split(new[] { "=" }, 2, StringSplitOptions.None);
                    valoresFormato[idx] = llaveValor[1];
                }

                detImporte = datosDet.MontoOperacion;
                detFolioForex = string.Empty;

                if (valoresFormato[13] != null)
                    detFechaEjecucion = Convert.ToString(valoresFormato[13]);

                if (valoresFormato[4] != null)
                    detMoneda = _vwT24.ValidaMoneda(Convert.ToString(valoresFormato[4]));
            }

            Config.EscribirLog($"INFO|PaP_SPID.procesaOperacion|Veamos que trae la fecha{valoresFormato[13]}-----{mtoFechaEjecucion}");

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema);
            int intFechaEjecucion = int.Parse(mtoFechaEjecucion);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception($"Se desea realizar una operación al {mtoFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // Genero el folio único y folio propuesto de T24.
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(mtoFechaEjecucion),
                folio);

            folioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                folioUnico,
                int.Parse(ConexionT24Mw.CambiarFechaAJulianaT24(mtoFechaEjecucion)),
                tipoOperacion);

            if (string.IsNullOrWhiteSpace(folioPropuesto))
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("Error al obtener el folio propuesto de T24");

            // Fecha Futura?
            fechaFutura = string.CompareOrdinal(mtoFechaEjecucion, _configuracion.StrFechaSistema) > 0;

            // Validación de maestro y detalle.
            if (!string.Equals(mtoFolioForex, detFolioForex, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Forex: Maestro:'{mtoFolioForex}', Detalle:'{detFolioForex}'.");

            if (!string.Equals(mtoFechaEjecucion, detFechaEjecucion, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Fecha de ejecución: Maestro:'{mtoFechaEjecucion}', Detalle:'{detFechaEjecucion}'.");

            if (!string.Equals(mtoMoneda, detMoneda, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Moneda: Maestro:'{mtoMoneda}', Detalle:'{detMoneda}'.");

            // Fecha de ejecución y tiene un forex asignado?
            Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|Validando si la operación está asignada a un Forex.");

            if (!string.IsNullOrWhiteSpace(mtoFolioForex))
            {
                Config.EscribirLog($"INFO|PaP_SPID.procesaOperacion|Asignado el folio forex: {mtoFolioForex} .");
                conForex = true;

                if (decimal.TryParse(mtoFolioForex, out _))
                    throw new Exception($"El Folio del Forex no es válido porque es numérico; se espera una referencia alfanumérica/external ref: {mtoFolioForex}");

                PaPSpidForexStatusDto forexStatus = await _papSpidDao.ObtenerEstatusForex(mtoFolioForex);
                if (forexStatus != null &&
                    string.Equals(forexStatus.STATUSOPERACION, "CANCELADO", StringComparison.OrdinalIgnoreCase))
                {
                    forexCancelado = true;
                    conEntradaSalida = false;
                    Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|FOREX CANCELADO.");
                }
            }

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string subTipo = string.Empty;

                Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciarProceso.
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
                    subTipo = $"FX:{mtoFolioForex}";

                bool actualizado = await _papSpidDao.ActualizarDatosGeneralesPlanTrabajo(new PaPSpidActualizarPlanTrabajoDto
                {
                    Folio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                    FolioMaestro = ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"),
                    TipoOperacion = tipoOperacion,
                    FolioUnico = folioUnico,
                    FechaValor = ConversionHelper.ConvertirIntDirecto(mtoFechaEjecucion, "MtoFechaEjecucion"),
                    Moneda = mtoMoneda,
                    ImporteDetalle = detImporte,
                    ImporteMaestro = mtoImporte,
                    NumCliente = ConversionHelper.ConvertirIntDirecto(valoresFormato[21], "Cdl"),
                    NombreCliente = Convert.ToString(valoresFormato[1]),
                    NumCuenta = Convert.ToString(valoresFormato[22]),
                    NumeroPagos = mtoRegs,
                    SubtipoOperacion = subTipo
                });

                if (!actualizado)
                    throw new Exception($"Error actualizando tb_srv_plan_trabajo_maestro para el folio {folio}.");
                /// <summary>
                /// Ejecuta la operaciÃ³n TerminarProceso.
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
                Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|A insertar datos de CMS a TOMI.");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciarProceso.
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
                resultado = await CMS_A_TOMI(folio, valoresFormato, ct);

                if (resultado)
                {
                    Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");

                    if (!await _comunSPID.GeneraFirmaDigitalSPID("TB_TOMBIN_NF_SPID_PAP_DETALLE", folio, folioPropuesto))
                        throw new Exception($"Se presentó un error al generar la firma digital del folio: {folio}");

                    procesado = true;
                }
                else
                {
                    int regs = await _papSpidDao.ContarDetallePorFolio(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                    if (regs > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (procesado)
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminarProceso.
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
                Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|A insertar datos de Entrada/Salida en TOMI (registro detalle).");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciarProceso.
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

                    int regsEntSal = await _papSpidDao.ContarEntradasSalidasPorReferencia(folioUnico);
                    if (regsEntSal > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                    }

                    if (!procesado)
                    {
                        resultado = await TOMI_ENTRADA_SALIDA(folio);
                        if (resultado)
                        {
                            procesado = true;
                            Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|Se insertó correctamente el folio detalle en base de Entrada/Salida.");
                        }
                    }

                    if (procesado)
                    {
                        /// <summary>
                        /// Ejecuta la operaciÃ³n TerminarProceso.
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
            }

            // Actualización del estatus del CMS y TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|PaP_SPID.procesaOperacion|A actualizar estatus en CMS y TOMI. Buscando sumarizado de los detalles del folioMaestro={mtoFolio}");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciarProceso.
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

                PaPSpidSumarizadoDetalleDto sumarizado = await _papSpidDao.ObtenerSumarizadoDetalle(ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"));
                if (sumarizado != null)
                {
                    tomiMtoRegs = sumarizado.num_regs;
                    tomiMtoImporte = sumarizado.imp_monto ?? 0m;
                }

                procesado = false;

                if (mtoRegs == tomiMtoRegs && Math.Round(mtoImporte, 2) == Math.Round(tomiMtoImporte, 2))
                {
                    Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|El sumarizado de detalles está completo.");

                    int detalleNoPendiente = await _papSpidDao.ContarDetalleNoPendientePorFolio(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                    if (detalleNoPendiente > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|El folio ya se había marcado como realizado.");
                    }

                    if (!procesado)
                    {
                        Config.EscribirLog($"INFO|PaP_SPID.procesaOperacion|A tratar de insertar el maestro con folio {mtoFolio}.");

                        resultado = await CMS_A_TOMI_MAESTRO(mtoFolio, valoresFormatoMto);

                        if (resultado)
                        {
                            Config.EscribirLog($"INFO|PaP_SPID.procesaOperacion|Se insertó correctamente el folio maestro {mtoFolio} en base de TOMI.");
                            procesado = true;
                        }
                        else
                        {
                            int regsMaestro = await _papSpidDao.ContarMaestroPorFolio(ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"));
                            if (regsMaestro > 0)
                            {
                                procesado = true;
                                Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|El folio maestro en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                            }
                        }

                        if (procesado)
                        {
                            if (conForex && forexCancelado)
                            {
                                await _papSpidCmsDao.ActualizarEstatusRechazadoForex(ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"));
                                await _papSpidDao.ActualizarEstatusTomiRechazadoForex(ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"));

                                Config.EscribirLog($"INFO|PaP_SPID.procesaOperacion|Forex Rechazado con Anterioridad: {mtoFolio}");
                                Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|A cancelar la operación.");

                                await _papSpidDao.CancelarPlanTrabajoPorForex(
                                    ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                                    ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"),
                                    tipoOperacion,
                                    ConversionHelper.ConvertirIntDirecto(mtoFechaEjecucion, "MtoFechaEjecucion"),
                                    mtoMoneda,
                                    detImporte,
                                    mtoImporte);
                                /// <summary>
                                /// Ejecuta la operaciÃ³n TerminarProceso.
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

                            if (string.Equals(mtoFechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                            {
                                await _papSpidCmsDao.ActualizarEstatusAceptado(ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"));
                                await _papSpidDao.ActualizarEstatusAceptadoHoy(
                                    ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"),
                                    DateTime.ParseExact(
                                        DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                                        "yyyy/MM/dd HH:mm",
                                        CultureInfo.InvariantCulture));

                                Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|Operación fecha valor hoy.");
                            }
                            else if (string.CompareOrdinal(mtoFechaEjecucion, _configuracion.StrFechaSistema) < 0)
                            {
                                await _controlServicio.MarcarErrorFechaPasada(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                                throw new Exception($"Se desea realizar una operación al {mtoFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                            }
                            else
                            {
                                await _papSpidCmsDao.ActualizarEstatusPendienteEjecucion(ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"));
                                await _papSpidDao.ActualizarEstatusFechaFutura(
                                    ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"),
                                    DateTime.ParseExact(
                                        DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                                        "yyyy/MM/dd HH:mm",
                                        CultureInfo.InvariantCulture));

                                Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|Operación fecha valor fútura.");
                            }
                        }
                    }
                }
                else
                {
                    Config.EscribirLog($"AVISO|PaP_SPID.procesaOperacion|Faltan detalles a procesar para marcar el cambio de estatus en el folio Maestro:{mtoFolio}.");
                    procesado = false;
                    return true;
                }

                if (procesado)
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminarProceso.
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
                decimal importeTomi;
                string fechaEjecucionEntSal;
                string monedaEntSal;
                decimal importeEntSal;

                Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|A conciliar operaciones entre CMS y TOMI (maestro y detalles).");
                /// <summary>
                /// Ejecuta la operaciÃ³n IniciarProceso.
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

                regsCms = 1;
                regsTomi = 0;
                importeTomi = 0m;

                PaPSpidDetalleConciliacionDto datosTomi = await _papSpidDao.ObtenerDetalleConciliacion(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                if (datosTomi != null)
                {
                    regsTomi = 1;
                    importeTomi = datosTomi.T24_DEBIT_AMOUNT ?? 0m;
                }

                PaPSpidSumarizadoDetalleDto sumarizadoNoPendiente = await _papSpidDao.ObtenerSumarizadoDetalleNoPendiente(ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"));
                if (sumarizadoNoPendiente != null)
                {
                    tomiMtoRegs = sumarizadoNoPendiente.num_regs;
                    tomiMtoImporte = sumarizadoNoPendiente.imp_monto ?? 0m;
                }

                fechaEjecucionEntSal = string.Empty;
                monedaEntSal = string.Empty;
                importeEntSal = 0m;

                if (conEntradaSalida)
                {
                    PaPSpidEntradaSalidaConciliacionDto entradaSalida = await _papSpidDao.ObtenerEntradaSalidaConciliacion(folioUnico);

                    if (entradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = entradaSalida.ent_fechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                        monedaEntSal = _vwT24.ValidaMoneda(entradaSalida.ent_moneda_cuenta);
                        importeEntSal = entradaSalida.ent_importe ?? 0m;
                    }
                }

                mensajeError = string.Empty;

                if (regsCms == 1 && regsTomi == 1 && ((conEntradaSalida && regsEntSal == 1) || !conEntradaSalida))
                {
                    if (detImporte != importeTomi)
                        mensajeError += Environment.NewLine + $" - Diferencia en el importe. CMS='{detImporte}', TOMI='{importeTomi}'.";

                    if (mtoRegs != tomiMtoRegs)
                        mensajeError += Environment.NewLine + $" - Diferencia en el total de registros. CMS='{mtoRegs}', TOMI='{tomiMtoRegs}'.";

                    if (Math.Round(mtoImporte, 2) != Math.Round(tomiMtoImporte, 2))
                        mensajeError += Environment.NewLine + $" - Diferencia en la sumatoría del importe de registros. CMS='{mtoImporte}', TOMI='{tomiMtoImporte}'.";

                    if (conEntradaSalida)
                    {
                        if (!string.Equals(mtoFechaEjecucion, fechaEjecucionEntSal, StringComparison.Ordinal))
                            mensajeError += Environment.NewLine + $" - Diferencia en la fecha de ejecución de Entrada/Salida. CMS='{mtoFechaEjecucion}', TOMI='{fechaEjecucionEntSal}'.";

                        if (!string.Equals(mtoMoneda, monedaEntSal, StringComparison.Ordinal))
                            mensajeError += Environment.NewLine + $" - Diferencia en la monedas de Entrada/Salida. CMS='{mtoMoneda}', TOMI='{monedaEntSal}'.";

                        if (detImporte != importeEntSal)
                            mensajeError += Environment.NewLine + $" - Diferencia en el importe de Entrada/Salida. CMS='{detImporte}', Entrada/Salida='{importeEntSal}'.";
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
                    if (fechaFutura)
                        await _papSpidDao.ActualizarPlanTrabajoFechaFutura(
                            ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                            ConversionHelper.ConvertirLongDirecto(mtoFolio, "MtoFolio"),
                            tipoOperacion);

                    Config.EscribirLog("INFO|PaP_SPID.procesaOperacion|La operación está correctamente conciliada.");
                    /// <summary>
                    /// Ejecuta la operaciÃ³n TerminarProceso.
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

        // usp_TOMBIN_INSERT_FORMATO_SPID_PAP_DETALLE
        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato, CancellationToken ct)
        {
            try
            {
                DateTime fecha = DateTime.Now.Date;
                bool fechaValida = false;
                int diasRetroceso = 0;
                const int maxDiasRetroceso = 366;

                while (fechaValida == false)
                {
                    ct.ThrowIfCancellationRequested();

                    if (diasRetroceso >= maxDiasRetroceso)
                        throw new Exception($"No se encontró un día hábil dentro de los últimos {maxDiasRetroceso} días para el folio {folio}.");

                    fecha = fecha.AddDays(-1);
                    diasRetroceso++;

                    fechaValida = await _spid.ValidarDiaHabilSiguiente(fecha, Convert.ToInt64(folio));

                    if (fechaValida || fecha.DayOfWeek == DayOfWeek.Sunday || fecha.DayOfWeek == DayOfWeek.Saturday)
                        fechaValida = false;
                    else
                        fechaValida = true;
                }

                Config.EscribirLog($"INFO|PaP_SPID.CMS_A_TOMI|Buscando dirección en VistasT24 del folio {folio}");
                Config.EscribirLog($"INFO|{tipoOperacion}.CMS_A_TOMI|Aperturando conexión a base de datos de VistasT24.");

                PaPSpidInsightCustomerEdoCtaDto datosInsight = await _papSpidInsightDao.ObtenerCustomerEdoCta(Convert.ToString(valoresFormato[21]));
                if (datosInsight != null &&
                    !string.IsNullOrWhiteSpace(DameTexto(datosInsight.P1_CODIGO_POSTAL)))
                {
                    Config.EscribirLog($"INFO|PaP_SPID.CMS_A_TOMI|Se encontró la dirección en VistasT24 del folio {folio}");
                    valoresFormato[45] = DameTexto(datosInsight.P1_CALLE) + " " + DameTexto(datosInsight.P1_EXTERIOR) + " " + DameTexto(datosInsight.P1_INTERIOR);
                    valoresFormato[46] = DameTexto(datosInsight.P1_CODIGO_POSTAL);
                }

                Config.EscribirLog($"INFO|{tipoOperacion}.CMS_A_TOMI|Cerrando conexión a base de datos de VistasT24.");

                if (string.Equals(Convert.ToString(valoresFormato[45]), string.Empty, StringComparison.Ordinal) ||
                    string.Equals(Convert.ToString(valoresFormato[46]), string.Empty, StringComparison.Ordinal))
                {
                    Config.EscribirLog("INFO|PaP_SPID.CMS_A_TOMI|Sin datos en la dirección");
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error al obtener de insight los datos del domicilio del cliente");
                }

                bool resultado = await _papSpidDao.InsertarFormatoDetalle(CrearFormatoDto(valoresFormato, true));
                Config.EscribirLog($"INFO|PaP_SPID.CMS_A_TOMI|Salida={(resultado ? 0 : 1)}.");
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|PaP_SPID.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        private async Task<bool> CMS_A_TOMI_MAESTRO(string folioMaestro, object[] valoresFormatoMto)
        {
            try
            {
                bool resultado = await _papSpidDao.InsertarFormatoMaestro(CrearFormatoDto(valoresFormatoMto, false));
                Config.EscribirLog($"INFO|PaP_SPID.CMS_A_TOMI_MAESTRO|Salida={(resultado ? 0 : 1)}.");
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|PaP_SPID.CMS_A_TOMI_MAESTRO|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            try
            {
                PaPSpidInsertarEntradaSalidaDto dto = null;
                Config.EscribirLog($"INFO|PaP_SPID.TOMI_ENTRADA_SALIDA|Buscando datos del folio {folio}");

                List<PaPSpidTombinDetalleDto> registros = await _papSpidDao.ObtenerDetallesParaEntradaSalida(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));

                foreach (PaPSpidTombinDetalleDto datos in registros)
                {
                    dto = new PaPSpidInsertarEntradaSalidaDto
                    {
                        Referencia = datos.NF_FOLIO_UNICO,
                        Num_Cliente = ConversionHelper.ConvertirDecimalDirecto(datos.T24_CUSTOMER_CODE, "T24_CUSTOMER_CODE"),
                        CDL = ConversionHelper.ConvertirDecimalDirecto(datos.T24_CUSTOMER_CODE, "T24_CUSTOMER_CODE"),
                        Ordenante = datos.T24_COMPANY_NAME,
                        Operacion = 218m,
                        OperacionDesc = "Salida  (TTS)",
                        Tipo = "D",
                        TipoDesc = "Doméstica",
                        Via = 800m,
                        ViaDesc = "TRF ENVIADA USD SPID",
                        Importe = datos.T24_DEBIT_AMOUNT ?? 0m,
                        Saldo = 0m,
                        Comentarios = datos.T24_DEBIT_THEIR_REF,
                        Fecha_Captura = ConversionHelper.ParseFechaYyyyMMddExacta(_configuracion.StrFechaSistema),
                        FechaValor = ConversionHelper.ParseFechaYyyyMMddExacta(Convert.ToString(datos.NF_FECHA_EJECUCION)),
                        Hora = DateTime.Now.ToLongTimeString(),
                        Capturista = "JEAI",
                        Fad = 0m,
                        Referencia_Control = "A",
                        Referencia_Control_Cont = 0
                    };

                    if (string.Equals(datos.NF_FOREX_RELACIONADO, string.Empty, StringComparison.Ordinal))
                    {
                        dto.Cuenta = ConversionHelper.ConvertirDecimalDirecto(datos.T24_DEBIT_ACCT_NO, "T24_DEBIT_ACCT_NO");
                        dto.Nombre = datos.NF_NOMBRE_CUENTA;
                        dto.CuentaMoneda = _vwT24.ValidaMoneda(datos.NF_MONEDA_CUENTA);
                    }
                    else
                    {
                        List<OperacionesEntidad> T24_FAD_Account = await _vwT24.ObtieneDatosCuentasFad();
                        dto.Cuenta = Convert.ToDecimal(T24_FAD_Account.Where(x => x.Moneda == _vwT24.MonedaDefaultMN).FirstOrDefault().Cuenta);
                        dto.Nombre = "BTMM USD FAD";
                        dto.CuentaMoneda = "USD";
                    }
                }

                if (dto == null)
                    return false;

                bool resultado = await _papSpidDao.InsertarEntradaSalida(dto);
                Config.EscribirLog($"INFO|PaP_SPID.TOMI_ENTRADA_SALIDA|Salida={(resultado ? 0 : 1)}.");
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|PaP_SPID.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}.Trace:{ex}");
            }

            return false;
        }

        private PaPSpidInsertarFormatoDto CrearFormatoDto(object[] valoresFormato, bool detalle)
        {
            return new PaPSpidInsertarFormatoDto
            {
                NF_FOLIO_UNICO = folioUnico,
                NF_TIPO = Convert.ToString(valoresFormato[19]),
                NF_FOLIO = ConversionHelper.ConvertirLongDirecto(valoresFormato[20], "NF_FOLIO"),
                T24_CUSTOMER_CODE = Convert.ToString(valoresFormato[21]),
                T24_DEBIT_ACCT_NO = Convert.ToString(valoresFormato[22]),
                NF_ESTATUS = "PEND",
                NF_USUARIO_CAPTURA = Convert.ToString(valoresFormato[24]),
                NF_USUARIO_AUTORIZA = Convert.ToString(valoresFormato[26]),
                NF_FECHA_CAPTURA = ConversionHelper.ConvertirDateTimeDirecto(valoresFormato[23], "NF_FECHA_CAPTURA"),
                NF_FECHA_AUTORIZA = ConversionHelper.ConvertirNullableDateTimeDirecto(valoresFormato[25]),
                T24_DEBIT_AMOUNT = ConversionHelper.ConvertirDecimalDirecto(valoresFormato[27], "T24_DEBIT_AMOUNT"),
                T24_SHORT_NAME = Convert.ToString(valoresFormato[1]),
                NF_FOREX_RELACIONADO = Convert.ToString(valoresFormato[3]),
                T24_DEBIT_CURRENCY = Convert.ToString(valoresFormato[4]),
                NF_BANCO_BENEF = Convert.ToString(valoresFormato[6]),
                T24_BEN_ACCT_NO = Convert.ToString(valoresFormato[7]),
                NF_TIPO_CUENTA = Convert.ToString(valoresFormato[8]),
                T24_BEN_CUST_NAME = detalle ? TextoSPEI(valoresFormato[10]) : Convert.ToString(valoresFormato[10]),
                NF_RFC_BENEFICIARIO = detalle ? TextoSPEI(valoresFormato[11]) : Convert.ToString(valoresFormato[11]),
                NF_TASA_RETENCION = Convert.ToString(valoresFormato[12]),
                NF_FECHA_EJECUCION = ConversionHelper.ConvertirIntDirecto(valoresFormato[13], "NF_FECHA_EJECUCION"),
                T24_DEBIT_THEIR_REF = detalle ? TextoSPEI(valoresFormato[15]) : Convert.ToString(valoresFormato[15]),
                NF_MONTO_ENVIAR = detalle ? ConversionHelper.ConvertirDecimalDirecto(valoresFormato[18], "NF_MONTO_ENVIAR") : ConversionHelper.ConvertirDecimalOrZeroDirecto(valoresFormato[18]),
                ID_PLANTILLA = Convert.ToString(valoresFormato[28]),
                ID_FORMATO_PLANTILLA = ConversionHelper.ConvertirNullableLongDirecto(valoresFormato[29]),
                ID_FILES_BTMUM = ConversionHelper.ConvertirLongDirecto(valoresFormato[30], "ID_FILES_BTMUM"),
                NO_PAGOS = ConversionHelper.ConvertirIntDirecto(valoresFormato[31], "NO_PAGOS"),
                PAGOSDESCRIPCION = Convert.ToString(valoresFormato[32]),
                NOPAGOSPROCESADOS = ConversionHelper.ConvertirIntDirecto(valoresFormato[33], "NOPAGOSPROCESADOS"),
                NOPAGOSDEVUELTOS = ConversionHelper.ConvertirIntDirecto(valoresFormato[34], "NOPAGOSDEVUELTOS"),
                ID_FORMATO_PAP = detalle ? ConversionHelper.ConvertirNullableLongDirecto(valoresFormato[35]) : null,
                REF1 = detalle ? Convert.ToString(valoresFormato[36]) : null,
                REF2 = detalle ? Convert.ToString(valoresFormato[37]) : null,
                REF3 = detalle ? Convert.ToString(valoresFormato[38]) : null,
                REF4 = detalle ? Convert.ToString(valoresFormato[39]) : null,
                REF5 = detalle ? Convert.ToString(valoresFormato[40]) : null,
                NF_ADDRESS = detalle ? Convert.ToString(valoresFormato[45]) : null,
                NF_ZIP_CODE = detalle ? Convert.ToString(valoresFormato[46]) : null,
                NF_CLASIFICACION_OPERACION = detalle ? Convert.ToString(valoresFormato[41]) : null,
                NF_RELACION_COMERCIAL = detalle ? Convert.ToString(valoresFormato[42]) : null,
                NF_NUMERIC_REFERENCE = detalle ? Convert.ToString(valoresFormato[43]) : null,
                NF_DIRECCION_IP = detalle ? Convert.ToString(valoresFormato[44]) : null
            };
        }

    }
}

using Aplicacion.DTOs.PaP;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.PaP;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa Pa P TEF.
    /// </summary>
    public class PaP_TEF
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IPaPTEFDao _PaPTEFDao;
        private readonly IPaPTEFCmsDao _PaPTEFCmsDao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IComunFXD _comunFXD;
        private readonly Comunes _comunes;

        private const string tipoOperacion = "PaP_TEF";
        private string folioUnico = string.Empty;
        private string folioPropuesto = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase PaP_TEF.
        /// </summary>

        public PaP_TEF(ConfiguracionModelo configuracion, IControlServicio controlServicio, IPaPTEFDao paPTEFDao, IPaPTEFCmsDao paPTEFCmsDao, ConexionT24Mw vwT24, IComunFXD comunFXD, Comunes comunes)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _PaPTEFDao = paPTEFDao;
            _PaPTEFCmsDao = paPTEFCmsDao;
            _vwT24 = vwT24;
            _comunFXD = comunFXD;
            _comunes = comunes;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ProcesaOperacion(string folio)
        {
            object[] laValoresFormatoMto = new object[46];
            object[] laValoresFormato = new object[46];

            string lsMto_Folio;
            int liMto_Regs;
            string lsMto_FechaEjecucion;
            string lsMto_Moneda;
            decimal ldMto_Importe;

            string lsDet_FolioForex;
            string lsDet_FechaEjecucion;
            string lsDet_Moneda;
            decimal ldDet_Importe;

            bool lbConForex;
            bool lbForexCancelado;
            string lsMto_FolioForex;
            bool lbConEntradaSalida;

            bool lbProcesado;
            bool lbResultado;
            bool lbFechaFutura;

            int liTOMI_Mto_Regs;
            decimal ldTOMI_Mto_Importe;

            // Verificamos la última operación no realizada
            _configuracion.AliasProceso = await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
            {
                Folio = folio,
                TipoOperacion = tipoOperacion,
                Servicio = _configuracion.NombreServicio,
                NombreProceso = _configuracion.NombreProceso,
                AliasProceso = _configuracion.AliasProceso,
                PId = _configuracion.Pid.Value
            });

            if (_configuracion.AliasProceso == string.Empty)
            {
                return false;
            }

            // Datos del folio en CMS y TOMI
            lsMto_Folio = string.Empty;
            lsMto_FechaEjecucion = string.Empty;
            lsMto_Moneda = string.Empty;
            ldMto_Importe = 0;
            lsMto_FolioForex = string.Empty;

            lsDet_FechaEjecucion = string.Empty;
            lsDet_Moneda = string.Empty;
            ldDet_Importe = 0;
            lsDet_FolioForex = string.Empty;

            lbConForex = false;
            lbForexCancelado = false;
            lbConEntradaSalida = true;

            liMto_Regs = 0;
            liTOMI_Mto_Regs = 0;
            ldTOMI_Mto_Importe = 0;

            Config.EscribirLog("INFO|" + tipoOperacion + ".procesaOperacion|Aperturando conexión a base de datos del CMS.");

            PaPTefMaestroCmsDto maestroCms = await _PaPTEFCmsDao.ObtenerMaestroPapTefPorDetalle(folio);

            if (maestroCms != null)
            {
                /// <summary>
                /// Ejecuta la operaciÃ³n CargarValoresFormatoMaestro.
                /// </summary>
                CargarValoresFormatoMaestro(maestroCms, laValoresFormatoMto, folio);

                lsMto_Folio = Convert.ToString(maestroCms.IdFormato, CultureInfo.InvariantCulture);
                liMto_Regs = Convert.ToInt32(maestroCms.NoPagos);
                ldMto_Importe = maestroCms.MontoOperacion;

                // Folio forex
                lsMto_FolioForex = string.Empty; // No hay forex en PaP

                // Fecha ejecución
                if (laValoresFormatoMto[13] != null && laValoresFormatoMto[13] != DBNull.Value)
                {
                    lsMto_FechaEjecucion = Convert.ToString(laValoresFormatoMto[13], CultureInfo.InvariantCulture);
                }

                // Moneda
                if (laValoresFormatoMto[4] != null && laValoresFormatoMto[4] != DBNull.Value)
                {
                    lsMto_Moneda = _vwT24.ValidaMoneda(
                        Convert.ToString(laValoresFormatoMto[4], CultureInfo.InvariantCulture));
                }
            }

            PaPTefDetalleCmsDto detalleCms = await _PaPTEFCmsDao.ObtenerDetallePapTefPorFolio(folio);

            if (detalleCms != null)
            {
                /// <summary>
                /// Ejecuta la operaciÃ³n CargarValoresFormatoDetalle.
                /// </summary>
                CargarValoresFormatoDetalle(detalleCms, laValoresFormato, folio);

                // Importe
                ldDet_Importe = detalleCms.MontoOperacion;

                // Folio forex
                lsDet_FolioForex = string.Empty; // No hay Forex en PaP

                // Fecha ejecución
                if (laValoresFormato[13] != null && laValoresFormato[13] != DBNull.Value)
                {
                    lsDet_FechaEjecucion = Convert.ToString(laValoresFormato[13], CultureInfo.InvariantCulture);
                }

                // Moneda
                if (laValoresFormato[4] != null && laValoresFormato[4] != DBNull.Value)
                {
                    lsDet_Moneda = _vwT24.ValidaMoneda(
                        Convert.ToString(laValoresFormato[4], CultureInfo.InvariantCulture));
                }
            }

            Config.EscribirLog(
                "INFO|PaP_TEF.procesaOperacion|Veamos que trae la fecha" +
                Convert.ToString(laValoresFormato[13], CultureInfo.InvariantCulture) +
                "-----" +
                lsMto_FechaEjecucion);

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(lsMto_FechaEjecucion, CultureInfo.InvariantCulture);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception(
                    "Se desea realizar una operación al " +
                    lsMto_FechaEjecucion +
                    " cuando la fecha del sistema es " +
                    _configuracion.StrFechaSistema +
                    ".");
            }

            // Genero el folio unico
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(lsMto_FechaEjecucion),
                folio);

            lbFechaFutura = string.CompareOrdinal(lsMto_FechaEjecucion, _configuracion.StrFechaSistema) > 0;

            // =========================================================================================
            // Validación de maestro y detalle
            // =========================================================================================
            if (lsMto_FolioForex != lsDet_FolioForex)
            {
                throw new Exception(
                    "Deferencia entre el maestro y el detalle de folio de Forex: Maestro:'" +
                    lsMto_FolioForex +
                    "', Detalle:'" +
                    lsDet_FolioForex +
                    "'.");
            }

            if (lsMto_FechaEjecucion != lsDet_FechaEjecucion)
            {
                throw new Exception(
                    "Deferencia entre el maestro y el detalle de folio de Fecha de ejecución: Maestro:'" +
                    lsMto_FechaEjecucion +
                    "', Detalle:'" +
                    lsDet_FechaEjecucion +
                    "'.");
            }

            if (lsMto_Moneda != lsDet_Moneda)
            {
                throw new Exception(
                    "Deferencia entre el maestro y el detalle de folio de Moneda: Maestro:'" +
                    lsMto_Moneda +
                    "', Detalle:'" +
                    lsDet_Moneda +
                    "'.");
            }

            // =========================================================================================
            // Fecha de ejecución y tiene un forex asignado?
            // =========================================================================================
            Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Validando si la operación está asignada a un Forex.");

            if (lsMto_FolioForex != string.Empty)
            {
                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Asignado el folio forex: " + lsMto_FolioForex + " .");
                lbConForex = true;

                if (ConversionHelper.EsDoubleInvariant(lsMto_FolioForex))
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("El Folio del Forex no es valido: " + lsMto_FolioForex);
                }

                PaPTEFForexStatusDto forexStatus = await _PaPTEFDao.ObtenerEstatusForex(lsMto_FolioForex);

                if (forexStatus != null && forexStatus.StatusOperacion == "CANCELADO")
                {
                    lbForexCancelado = true;
                    lbConEntradaSalida = false;
                    Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|FOREX CANCELADO.");
                }
            }

            // =========================================================================================
            // Lectura y actualización de los datos del maestro
            // =========================================================================================
            if (_configuracion.AliasProceso == "DATOS_GRAL")
            {
                string lsSubTipo;

                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                lsSubTipo = string.Empty;

                if (lbConForex)
                {
                    lsSubTipo = "FX:" + lsMto_FolioForex;
                }

                var datosGenerales = new PaPTEFActualizarDatosGeneralesPlanTrabajoDto
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    FolioUnico = folioUnico,
                    FechaEjecucion = lsMto_FechaEjecucion,
                    Moneda = lsMto_Moneda,
                    ImporteDetalle = ldDet_Importe,
                    ImporteMaestro = ldMto_Importe,
                    NumCliente = Convert.ToString(laValoresFormato[21], CultureInfo.InvariantCulture),
                    NombreCliente = Convert.ToString(laValoresFormato[1], CultureInfo.InvariantCulture),
                    NumCuenta = Convert.ToString(laValoresFormato[22], CultureInfo.InvariantCulture),
                    SubtipoOperacion = lsSubTipo,
                    FolioMaestro = lsMto_Folio,
                    DesAgregado = "NumPagos=" + liMto_Regs.ToString(CultureInfo.InvariantCulture)
                };

                await _PaPTEFDao.ActualizarDatosGeneralesPlanTrabajo(datosGenerales);

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
            }

            // =========================================================================================
            // Lectura del CMS e inserción a TOMI
            // =========================================================================================
            if (_configuracion.AliasProceso == "CMS_A_TOMI")
            {
                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|A insertar datos de CMS a TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                lbProcesado = false;

                lbResultado = await CMS_A_TOMI(folio, laValoresFormato);

                if (lbResultado)
                {
                    Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                    lbProcesado = true;
                }
                else
                {
                    int numRegs = await _PaPTEFDao.ContarDetallePapTomiPorFolio(folio);

                    if (numRegs > 0)
                    {
                        lbProcesado = true;
                        Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (lbProcesado)
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
                }
                else
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de registro CMS_A_TOMI con folio: " + folio);
                }
            }

            // =========================================================================================
            // Registro de la Entrada/Salida
            // =========================================================================================
            if (_configuracion.AliasProceso == "TOMI_ENTRADAS/SALIDAS")
            {
                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|A insertar datos de Entrada/Salida en TOMI (registro detalle).");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (lbConEntradaSalida)
                {
                    lbProcesado = false;

                    int numRegsEntradaSalida = await _PaPTEFDao.ContarEntradaSalidaPorReferencia(folioUnico);

                    if (numRegsEntradaSalida > 0)
                    {
                        lbProcesado = true;
                        Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                    }

                    if (!lbProcesado)
                    {
                        lbResultado = await TOMI_ENTRADA_SALIDA(folio);

                        if (lbResultado)
                        {
                            lbProcesado = true;
                            Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Se insertó correctamente el folio detalle en base de Entrada/Salida.");
                        }
                    }

                    if (lbProcesado)
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
                    }
                    else
                    {
                        /// <summary>
                        /// Ejecuta la operaciÃ³n Exception.
                        /// </summary>
                        throw new Exception("Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: " + folio);
                    }
                }
            }

            // =========================================================================================
            // Actualización del estatus del CMS y TOMI
            // =========================================================================================
            if (_configuracion.AliasProceso == "CAMBIO_ESTATUS")
            {
                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|A actualizar estatus en CMS y TOMI. Buscando sumarizado de los detalles del folioMaestro=" + lsMto_Folio);

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                PaPTEFSumarizadoDetalleTomiDto sumarizado =
                    await _PaPTEFDao.ObtenerSumarizadoDetallePorMaestro(lsMto_Folio);

                if (sumarizado != null)
                {
                    liTOMI_Mto_Regs = sumarizado.num_regs;
                    if (!sumarizado.imp_monto.HasValue)
                        throw new Exception($"No se tiene información de imp_monto para el folio maestro {lsMto_Folio} en CAMBIO_ESTATUS.");

                    ldTOMI_Mto_Importe = sumarizado.imp_monto.Value;
                }

                lbProcesado = false;

                if (liMto_Regs == liTOMI_Mto_Regs &&
                    Math.Round(ldMto_Importe, 2) == Math.Round(ldTOMI_Mto_Importe, 2))
                {
                    Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|El sumarizado de detalles está completo.");

                    int detallesProcesados = await _PaPTEFDao.ContarDetallePapTomiProcesadoPorFolio(folio);

                    if (detallesProcesados > 0)
                    {
                        lbProcesado = true;
                        Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|El folio ya se había marcado como realizado.");
                    }

                    if (!lbProcesado)
                    {
                        Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|A tratar de insertar el maestro con folio " + lsMto_Folio + ".");

                        lbResultado = await CMS_A_TOMI_MAESTRO(lsMto_Folio, laValoresFormatoMto);

                        if (lbResultado)
                        {
                            Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Se insertó correctamente el folio maestro " + lsMto_Folio + " en base de TOMI.");
                            lbProcesado = true;
                        }
                        else
                        {
                            int maestrosTomi = await _PaPTEFDao.ContarMaestroPapTomiPorFolio(lsMto_Folio);

                            if (maestrosTomi > 0)
                            {
                                lbProcesado = true;
                                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|El folio maestro en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                            }
                        }

                        if (lbProcesado)
                        {
                            if (lbConForex && lbForexCancelado)
                            {
                                await _PaPTEFCmsDao.ActualizarCmsRechazadoPorForexCancelado(lsMto_Folio);

                                await _PaPTEFDao.ActualizarTomiRechazadoPorForexCancelado(lsMto_Folio);

                                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Forex Rechazado con Anterioridad: " + lsMto_Folio);

                                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|A cancelar la operación.");

                                await _PaPTEFDao.CancelarPlanTrabajoDetallePorForex(
                                    new PaPTEFCancelarPlanTrabajoForexDto
                                    {
                                        Folio = folio,
                                        TipoOperacion = tipoOperacion,
                                        FechaEjecucion = lsMto_FechaEjecucion,
                                        Moneda = lsMto_Moneda,
                                        Importe = ldDet_Importe
                                    });

                                string folioMaestroPlanTrabajo = await _vwT24.ObtenerFolioMaestro(folio, tipoOperacion);

                                await _PaPTEFDao.CancelarPlanTrabajoMaestroPorForex(
                                    new PaPTEFCancelarPlanTrabajoMaestroForexDto
                                    {
                                        FolioMaestro = folioMaestroPlanTrabajo,
                                        TipoOperacion = tipoOperacion,
                                        FechaEjecucion = lsMto_FechaEjecucion,
                                        Moneda = lsMto_Moneda,
                                        Importe = ldMto_Importe
                                    });

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

                            if (lsMto_FechaEjecucion == _configuracion.StrFechaSistema)
                            {
                                await _PaPTEFCmsDao.ActualizarCmsAceptadoHoy(lsMto_Folio);

                                await _PaPTEFDao.ActualizarTomiAceptadoHoy(
                                    lsMto_Folio,
                                    DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture));

                                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Operación fecha valor hoy.");
                            }
                            else if (string.CompareOrdinal(lsMto_FechaEjecucion, _configuracion.StrFechaSistema) < 0)
                            {
                                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                                throw new Exception(
                                    "Se desea realizar una operación al " +
                                    lsMto_FechaEjecucion +
                                    " cuando la fecha del sistema es " +
                                    _configuracion.StrFechaSistema +
                                    ".");
                            }
                            else
                            {
                                await _PaPTEFCmsDao.ActualizarCmsPendienteEjecucion(lsMto_Folio);

                                await _PaPTEFDao.ActualizarTomiFechaFutura(
                                    lsMto_Folio,
                                    DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture));

                                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|Operación fecha valor fútura.");
                            }
                        }
                    }
                }
                else
                {
                    Config.EscribirLog("AVISO|PaP_TEF.procesaOperacion|Faltan detalles a procesar para marcar el cambio de estatus en el folio Maestro:" + lsMto_Folio + ".");
                    lbProcesado = false;
                    return true;
                }

                if (lbProcesado)
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
                }
                else
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de registro CAMBIO_ESTATUS con folio: " + folio);
                }
            }

            // =========================================================================================
            // Conciliación CMS / TOMI / Entrada-Salida
            // =========================================================================================
            if (_configuracion.AliasProceso == "CONCILIAR")
            {
                int liRegsCMS;
                int liRegsTOMI;
                int liRegsEntSal;

                string lsMsjError;
                decimal ldImporteTOMI;

                string lsFechaEjecucionEntSal;
                string lsMonedaEntSal;
                decimal ldImporteEntSal;

                Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|A conciliar operaciones entre CMS y TOMI (maestro y detalles).");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                // Registros en CMS
                liRegsCMS = 1;

                // Registros en TOMI
                liRegsTOMI = 0;
                ldImporteTOMI = 0;

                PaPTEFDetalleTomiConciliacionDto detalleTomi =
                    await _PaPTEFDao.ObtenerDetalleTomiParaConciliacion(folio);

                if (detalleTomi != null)
                {
                    liRegsTOMI = 1;
                    ldImporteTOMI = detalleTomi.T24DebitAmount;
                }

                liTOMI_Mto_Regs = 0;
                ldTOMI_Mto_Importe = 0;

                PaPTEFSumarizadoDetalleTomiDto sumarizadoProcesado =
                    await _PaPTEFDao.ObtenerSumarizadoDetalleProcesadoPorMaestro(lsMto_Folio);

                if (sumarizadoProcesado != null)
                {
                    liTOMI_Mto_Regs = sumarizadoProcesado.num_regs;
                    if (!sumarizadoProcesado.imp_monto.HasValue)
                        throw new Exception($"No se tiene información de imp_monto para el folio maestro {lsMto_Folio} en CONCILIAR.");

                    ldTOMI_Mto_Importe = sumarizadoProcesado.imp_monto.Value;
                }

                // Entrada/Salida
                lsFechaEjecucionEntSal = string.Empty;
                lsMonedaEntSal = string.Empty;
                ldImporteEntSal = 0;
                liRegsEntSal = 0;

                if (lbConEntradaSalida)
                {
                    PaPTEFEntradaSalidaConciliacionDto entradaSalida =
                        await _PaPTEFDao.ObtenerEntradaSalidaParaConciliacion(folioUnico);

                    if (entradaSalida != null)
                    {
                        liRegsEntSal = 1;
                        lsFechaEjecucionEntSal = entradaSalida.EntFechaValor.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
                        lsMonedaEntSal = _vwT24.ValidaMoneda(entradaSalida.EntMonedaCuenta);
                        ldImporteEntSal = entradaSalida.EntImporte;
                    }
                }

                // Validación
                lsMsjError = string.Empty;

                if (liRegsCMS == 1 &&
                    liRegsTOMI == 1 &&
                    ((lbConEntradaSalida && liRegsEntSal == 1) || !lbConEntradaSalida))
                {
                    if (ldDet_Importe != ldImporteTOMI)
                    {
                        lsMsjError += Environment.NewLine +
                                      " - Diferencia en el importe. CMS='" +
                                      ldDet_Importe +
                                      "', TOMI='" +
                                      ldImporteTOMI +
                                      "'.";
                    }

                    if (liMto_Regs != liTOMI_Mto_Regs)
                    {
                        lsMsjError += Environment.NewLine +
                                      " - Diferencia en el total de registros. CMS='" +
                                      liMto_Regs +
                                      "', TOMI='" +
                                      liTOMI_Mto_Regs +
                                      "'.";
                    }

                    if (Math.Round(ldMto_Importe, 2) != Math.Round(ldTOMI_Mto_Importe, 2))
                    {
                        lsMsjError += Environment.NewLine +
                                      " - Diferencia en la sumatoría del importe de registros. CMS='" +
                                      ldMto_Importe +
                                      "', TOMI='" +
                                      ldTOMI_Mto_Importe +
                                      "'.";
                    }

                    if (lbConEntradaSalida)
                    {
                        if (lsMto_FechaEjecucion != lsFechaEjecucionEntSal)
                        {
                            lsMsjError += Environment.NewLine +
                                          " - Diferencia en la fecha de ejecución de Entrada/Salida. CMS='" +
                                          lsMto_FechaEjecucion +
                                          "', TOMI='" +
                                          lsFechaEjecucionEntSal +
                                          "'.";
                        }

                        if (lsMto_Moneda != lsMonedaEntSal)
                        {
                            lsMsjError += Environment.NewLine +
                                          " - Diferencia en la monedas de Entrada/Salida. CMS='" +
                                          lsMto_Moneda +
                                          "', TOMI='" +
                                          lsMonedaEntSal +
                                          "'.";
                        }

                        if (ldDet_Importe != ldImporteEntSal)
                        {
                            lsMsjError += Environment.NewLine +
                                          " - Diferencia en el importe de Entrada/Salida. CMS='" +
                                          ldDet_Importe +
                                          "', Entrada/Salida='" +
                                          ldImporteEntSal +
                                          "'.";
                        }
                    }

                    lbProcesado = true;
                }
                else
                {
                    lbProcesado = false;
                    lsMsjError += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                }

                if (lsMsjError != string.Empty)
                {
                    lbProcesado = false;
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(lsMsjError);
                }

                if (lbProcesado)
                {
                    Config.EscribirLog("INFO|PaP_TEF.procesaOperacion|La operación está correctamente conciliada.");

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
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de registro CONCILIAR con folio: " + folio);
                }
            }

            Config.EscribirLog("INFO|" + tipoOperacion + ".procesaOperacion|Cerrando conexión a base de datos del CMS.");

            return true;
        }

        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato)
        {
            try
            {
                var datos = new PaPTefInsertarDetalleTomiDto
                {
                    NfFolioUnico = folioUnico,
                    NfTipo = Convert.ToString(valoresFormato[19]),
                    NfFolio = Convert.ToInt64(valoresFormato[20]),
                    T24CustomerCode = Convert.ToString(valoresFormato[21]),
                    T24DebitAcctNo = Convert.ToString(valoresFormato[22]),
                    NfEstatus = "PEND",
                    NfUsuarioCaptura = Convert.ToString(valoresFormato[24]),
                    NfUsuarioAutoriza = Convert.ToString(valoresFormato[26]),
                    NfFechaCaptura = Convert.ToDateTime(valoresFormato[23]),
                    NfFechaAutoriza = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormato[25]),
                    T24DebitAmount = Convert.ToDecimal(valoresFormato[27]),

                    T24ShortName = Convert.ToString(valoresFormato[1]),
                    NfForexRelacionado = Convert.ToString(valoresFormato[3]),
                    T24DebitCurrency = Convert.ToString(valoresFormato[4]),
                    NfBancoBenef = Convert.ToString(valoresFormato[6]),
                    T24BenAcctNo = Convert.ToString(valoresFormato[7]),
                    NfTipoCuenta = Convert.ToString(valoresFormato[8]),

                    T24BenCustName = TextoTEF(valoresFormato[10]),

                    NfRfcBeneficiario = TextoTEF(valoresFormato[11]),

                    NfTasaRetencion = Convert.ToString(valoresFormato[12]),
                    NfFechaEjecucion = Convert.ToInt32(valoresFormato[42]),

                    T24DebitTheirRef = TextoTEF(valoresFormato[15]),

                    NfMontoEnviar = Convert.ToDecimal(valoresFormato[18]),

                    IdPlantilla = Convert.ToString(valoresFormato[28]),
                    IdFormatoPlantilla = Convert.ToInt64(valoresFormato[29]),
                    IdFilesBtmum = Convert.ToInt64(valoresFormato[30]),
                    NoPagos = Convert.ToInt32(valoresFormato[31]),
                    PagosDescripcion = Convert.ToString(valoresFormato[32]),
                    NoPagosProcesados = Convert.ToInt32(valoresFormato[33]),
                    NoPagosDevueltos = Convert.ToInt32(valoresFormato[34]),
                    IdFormatoPap = Convert.ToInt64(valoresFormato[35]),

                    Ref1 = Convert.ToString(valoresFormato[36]),
                    Ref2 = Convert.ToString(valoresFormato[37]),
                    Ref3 = Convert.ToString(valoresFormato[38]),
                    Ref4 = Convert.ToString(valoresFormato[39]),
                    Ref5 = Convert.ToString(valoresFormato[40]),

                    NfCodigoOperacion = valoresFormato[43].ToString(),
                    NfTipoOperacion = valoresFormato[44].ToString(),
                    NfFechaAplicacion = Convert.ToInt32(valoresFormato[41]),
                    NfFechaCargo = Convert.ToInt32(valoresFormato[42])
                };

                bool resultado =
                    await _PaPTEFDao.InsertarFormatoTefPapDetalle(datos);

                Config.EscribirLog("INFO|PaP_TEF.CMS_A_TOMI|Salida=" + resultado + ".");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    "ERROR|PaP_TEF.CMS_A_TOMI|Se presenta exception: " +
                    ex.Message +
                    ". Trace: " +
                    ex);
            }

            return false;
        }

        private async Task<bool> CMS_A_TOMI_MAESTRO(
            string folioMaestro,
            object[] valoresFormatoMto)
        {
            try
            {
                var datos = new PaPTefInsertarMaestroTomiDto
                {
                    NfFolioUnico = folioUnico,
                    NfTipo = Convert.ToString(valoresFormatoMto[19]),
                    NfFolio = Convert.ToDouble(valoresFormatoMto[20]),
                    T24CustomerCode = Convert.ToString(valoresFormatoMto[21]),
                    T24DebitAcctNo = Convert.ToString(valoresFormatoMto[22]),
                    NfEstatus = "PEND",
                    NfUsuarioCaptura = Convert.ToString(valoresFormatoMto[24]),
                    NfUsuarioAutoriza = Convert.ToString(valoresFormatoMto[26]),
                    NfFechaCaptura = Convert.ToDateTime(valoresFormatoMto[23]),
                    NfFechaAutoriza = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormatoMto[25]),
                    T24DebitAmount = Convert.ToDecimal(valoresFormatoMto[27]),

                    T24ShortName = Convert.ToString(valoresFormatoMto[1]),
                    NfForexRelacionado = Convert.ToString(valoresFormatoMto[3]),
                    T24DebitCurrency = Convert.ToString(valoresFormatoMto[4]),
                    NfBancoBenef = Convert.ToString(valoresFormatoMto[6]),
                    T24BenAcctNo = Convert.ToString(valoresFormatoMto[7]),
                    NfTipoCuenta = Convert.ToString(valoresFormatoMto[8]),

                    T24BenCustName = Convert.ToString(valoresFormatoMto[10]),

                    NfRfcBeneficiario = Convert.ToString(valoresFormatoMto[11]),

                    NfTasaRetencion = Convert.ToString(valoresFormatoMto[12]),
                    NfFechaEjecucion = Convert.ToInt32(valoresFormatoMto[42]),

                    T24DebitTheirRef = Convert.ToString(valoresFormatoMto[15]),

                    NfMontoEnviar = ConversionHelper.ObtenerMontoEnviarMaestroDecimal(valoresFormatoMto[18]),

                    IdPlantilla = Convert.ToString(valoresFormatoMto[28]),
                    IdFormatoPlantilla = Convert.ToDouble(valoresFormatoMto[29]),
                    IdFilesBtmum = Convert.ToDouble(valoresFormatoMto[30]),
                    NoPagos = Convert.ToDouble(valoresFormatoMto[31]),
                    PagosDescripcion = Convert.ToString(valoresFormatoMto[32]),
                    NoPagosProcesados = Convert.ToDouble(valoresFormatoMto[33]),
                    NoPagosDevueltos = Convert.ToDouble(valoresFormatoMto[34]),

                    NfCodigoOperacion = Convert.ToDouble(valoresFormatoMto[43]),
                    NfTipoOperacion = Convert.ToDouble(valoresFormatoMto[44]),
                    NfFechaAplicacion = Convert.ToInt32(valoresFormatoMto[41]),
                    NfFechaCargo = Convert.ToInt32(valoresFormatoMto[42])
                };

                bool resultado =
                    await _PaPTEFDao.InsertarFormatoTefPapMaestro(datos);

                Config.EscribirLog(
                    "INFO|PaP_TEF.CMS_A_TOMI_MAESTRO|Salida=" +
                    resultado +
                    ".");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    "ERROR|PaP_TEF.CMS_A_TOMI_MAESTRO|Se presenta exception: " +
                    ex.Message +
                    ". Trace: " +
                    ex);
            }

            return false;
        }

        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            try
            {
                PaPTefDetalleTomiEntradaSalidaDto detalle =
                    await _PaPTEFDao.ObtenerDetalleTomiParaEntradaSalida(folio);

                if (detalle == null)
                {
                    throw new Exception(
                        "No se encontraron datos en TB_TOMBIN_NF_TEF_PAP_DETALLE para el folio: " +
                        folio);
                }

                var datos = new PaPTefInsertarEntradaSalidaTomiDto
                {
                    Referencia = detalle.NfFolioUnico,
                    NumCliente = Convert.ToInt32(detalle.T24CustomerCode),
                    Cdl = Convert.ToDouble(detalle.T24CustomerCode),
                    Ordenante = detalle.T24CompanyName,

                    Operacion = 218,
                    OperacionDesc = "Salida  (TTS)",
                    Tipo = "D",
                    TipoDesc = "Doméstica",

                    Cuenta = Convert.ToDouble(detalle.T24DebitAcctNo),
                    Nombre = detalle.NfNombreCuenta,
                    CuentaMoneda = detalle.NfMonedaCuenta,

                    Via = 800,
                    ViaDesc = "TRF ENVIADA MXN SPEUA",

                    Importe = detalle.T24DebitAmount.Value,
                    Saldo = 0m,
                    Comentarios = detalle.T24DebitTheirRef,

                    FechaCaptura = _comunes.ConvertirTextoAFecha(_configuracion.StrFechaSistema).Value,
                    FechaValor = _comunes.ConvertirTextoAFecha(detalle.NfFechaEjecucion).Value,

                    Hora = DateTime.Now.ToLongTimeString(),
                    Capturista = "JEAI",
                    Fad = 0,
                    ReferenciaControl = "A",
                    ReferenciaControlCont = 0
                };

                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|gsFechaSistema=" + _configuracion.StrFechaSistema);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|gsFechaSistema=" + datos.FechaCaptura);

                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Referencia=" + datos.Referencia);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Num_Cliente=" + datos.NumCliente);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|CDL=" + datos.Cdl);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Ordenante=" + datos.Ordenante);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Operacion=" + datos.Operacion);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|OperacionDesc=" + datos.OperacionDesc);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Tipo=" + datos.Tipo);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|TipoDesc=" + datos.TipoDesc);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Cuenta=" + datos.Cuenta);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Nombre=" + datos.Nombre);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|CuentaMoneda=" + datos.CuentaMoneda);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Via=" + datos.Via);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|ViaDesc=" + datos.ViaDesc);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Importe=" + datos.Importe);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Saldo=" + datos.Saldo);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Comentarios=" + datos.Comentarios);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Fecha_Captura=" + datos.FechaCaptura);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|FechaValor=" + datos.FechaValor);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Hora=" + datos.Hora);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Capturista=" + datos.Capturista);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Fad=" + datos.Fad);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Referencia_Control=" + datos.ReferenciaControl);
                Config.EscribirLog("INFO|PaP_TEF.TOMI_ENTRADA_SALIDA|Referencia_Control_Cont=" + datos.ReferenciaControlCont);

                bool resultado =
                    await _PaPTEFDao.InsertarSrvEntradasSalidas(datos);

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    "ERROR|PaP_TEF.TOMI_ENTRADA_SALIDA|Se presenta exception: " +
                    ex.Message +
                    ".Trace:" +
                    ex);
            }

            return false;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        private static void CargarContenidoFormato(
            string contenidoFormato,
            object[] valores,
            string folio)
        {
            string[] valoresPipes = (contenidoFormato ?? string.Empty)
                .Split(new[] { '|' }, StringSplitOptions.None);

            if (valoresPipes.Length - 1 < 18)
            {
                throw new Exception(
                    "El contenidoFormato del folio " +
                    folio +
                    " es diferente al esperado, debe ser 18 datos y se tienen " +
                    (valoresPipes.Length - 1) +
                    ".");
            }

            for (int i = 0; i <= 18; i++)
            {
                string[] llaveValor = valoresPipes[i]
                    .Split(new[] { '=' }, StringSplitOptions.None);
                valores[i] = llaveValor[1];
            }
        }

        private static void CargarValoresFormatoMaestro(
        PaPTefMaestroCmsDto maestro,
        object[] valoresFormatoMto,
        string folio)
        {
            if (maestro == null)
            {
                return;
            }

            valoresFormatoMto[19] = "TEF";
            valoresFormatoMto[20] = maestro.IdFormato;
            valoresFormatoMto[21] = maestro.Cdl;
            valoresFormatoMto[22] = maestro.IdCuenta;
            valoresFormatoMto[23] = maestro.FechaCaptura;
            valoresFormatoMto[24] = maestro.IdUsuarioCaptura;
            valoresFormatoMto[25] = maestro.FechaAutorizacion;
            valoresFormatoMto[26] = maestro.IdUsuarioAutorizo;
            valoresFormatoMto[27] = maestro.MontoOperacion;
            valoresFormatoMto[28] = maestro.IdPlantilla;
            valoresFormatoMto[29] = maestro.IdFormatoPlantilla;
            valoresFormatoMto[30] = maestro.IdFilesBtmum;
            valoresFormatoMto[31] = maestro.NoPagos;
            valoresFormatoMto[32] = maestro.PagosDescripcion;
            valoresFormatoMto[33] = maestro.NoPagosProcesados;
            valoresFormatoMto[34] = maestro.NoPagosDevueltos;
            valoresFormatoMto[41] = maestro.FechaAplicacion;
            valoresFormatoMto[42] = maestro.FechaCargo;
            valoresFormatoMto[43] = maestro.CodigoOperacion;
            valoresFormatoMto[44] = maestro.TipoOperacion;

            CargarContenidoFormato(
                maestro.ContenidoFormato,
                valoresFormatoMto,
                folio);
        }

        private static void CargarValoresFormatoDetalle(
        PaPTefDetalleCmsDto detalle,
        object[] valoresFormato,
        string folio)
        {
            if (detalle == null)
            {
                return;
            }

            valoresFormato[19] = "TEF";
            valoresFormato[20] = detalle.IdFormato;
            valoresFormato[21] = detalle.Cdl;
            valoresFormato[22] = detalle.IdCuenta;
            valoresFormato[23] = detalle.FechaCaptura;
            valoresFormato[24] = detalle.IdUsuarioCaptura;
            valoresFormato[25] = detalle.FechaAutorizacion;
            valoresFormato[26] = detalle.IdUsuarioAutorizo;
            valoresFormato[27] = detalle.MontoOperacion;
            valoresFormato[28] = detalle.IdPlantilla;
            valoresFormato[29] = detalle.IdFormatoPlantilla;
            valoresFormato[30] = detalle.IdFilesBtmum;
            valoresFormato[31] = detalle.NoPagos;
            valoresFormato[32] = detalle.PagosDescripcion;
            valoresFormato[33] = detalle.NoPagosProcesados;
            valoresFormato[34] = detalle.NoPagosDevueltos;
            valoresFormato[35] = detalle.IdFormatoPap;
            valoresFormato[36] = detalle.Ref1;
            valoresFormato[37] = detalle.Ref2;
            valoresFormato[38] = detalle.Ref3;
            valoresFormato[39] = detalle.Ref4;
            valoresFormato[40] = detalle.Ref5;
            valoresFormato[41] = detalle.FechaAplicacion;
            valoresFormato[42] = detalle.FechaCargo;
            valoresFormato[43] = detalle.CodigoOperacion;
            valoresFormato[44] = detalle.TipoOperacion;

            CargarContenidoFormato(
                detalle.ContenidoFormato,
                valoresFormato,
                folio);
        }


    }
}

using Aplicacion.DTOs.SAT;
using Aplicacion.Interfaces.SAT;
using Aplicacion.Modelos;
using Aplicacion.Modelos.SAT;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    public class SAT
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ISATCmsDao _satCmsDao;
        private readonly ISATTomiDao _satTomiDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "SAT";
        private string folioUnico = string.Empty;

        public SAT(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ISATCmsDao satCmsDao,
            ISATTomiDao satTomiDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _satCmsDao = satCmsDao;
            _satTomiDao = satTomiDao;
            _vwT24 = vwT24;
        }

        public async Task<bool> ProcesOperacion(string lsFolio)
        {
            string subTipo = string.Empty;
            string fechaEjecucion = _configuracion.StrFechaSistema;
            string moneda = _vwT24.MonedaDefaultMN;
            decimal importe = 0;
            string numCliente = string.Empty;
            string nombreCliente = string.Empty;
            string numCuenta = string.Empty;
            int idFormato = 0;

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

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Aperturando conexión a base de datos del CMS.");

            SAT_DTO datosCms = await _satCmsDao.ObtenerFormatoSatPorFolio(lsFolio);
            if (datosCms != null)
            {
                idFormato = Convert.ToInt32(datosCms.TipoFormato);

                switch (idFormato)
                {
                    case 5:
                        subTipo = "EJE - Del Ejercicio";
                        break;
                    case 6:
                        subTipo = "PRV - Provisionales";
                        break;
                    case 9:
                        subTipo = "SAT";
                        break;
                    case 10:
                        subTipo = "DPA - Derecho Producto Aprovechamiento";
                        break;
                    case 13:
                        subTipo = "DRE - Depósito Referenciado";
                        break;
                }

                importe = datosCms.MontoOperacion;
                numCliente = datosCms.Cdl.ToString();
                numCuenta = datosCms.Id_cuenta.ToString();
            }

            Config.EscribirLog($"INFO|SAT.procesaOperacion|Numero Cliente:{numCliente}");

            SAT_HdClientes_DTO cliente = await _satTomiDao.ObtenerNombreClientePorCdl(numCliente);
            if (cliente != null)
                nombreCliente = cliente.Satm02nom;

            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion),
                lsFolio);

            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|SAT.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                await _satTomiDao.ActualizarPlanTrabajoMaestroLectura(
                    lsFolio,
                    tipoOperacion,
                    folioUnico,
                    subTipo,
                    fechaEjecucion,
                    moneda,
                    importe,
                    numCliente,
                    nombreCliente,
                    numCuenta);

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
                bool procesado = false;
                bool resultado = false;
                int resultadoValidacion = 0;
                SAT_DatosRegresoModelo datosRegreso = null;

                Config.EscribirLog("INFO|SAT.procesaOperacion|A insertar datos de CMS a TOMI.");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (idFormato == 5 || idFormato == 6 || idFormato == 9 || idFormato == 10)
                    Config.EscribirLog("INFO|SAT.procesaOperacion|A calcular y crear el sello digital de la operación.");

                datosRegreso = await _satTomiDao.ProcesarCmsATomi(lsFolio, idFormato, folioUnico);

                if (datosRegreso != null && datosRegreso.Resultado)
                {
                    resultado = true;
                }
                else
                {
                    int numRegs = await _satTomiDao.ContarTombinCmsSatPorFolio(lsFolio);
                    if (numRegs > 0)
                    {
                        resultado = true;
                        if (datosRegreso == null)
                        {
                            datosRegreso = new SAT_DatosRegresoModelo
                            {
                                Resultado = true,
                                ResultadoValidacion = 0,
                                ValoresRegreso = new SAT_ValoresFormato_Modelo()
                            };
                        }
                        else
                        {
                            datosRegreso.Resultado = true;
                            datosRegreso.ResultadoValidacion = 0;
                            if (datosRegreso.ValoresRegreso == null)
                                datosRegreso.ValoresRegreso = new SAT_ValoresFormato_Modelo();
                        }
                        Config.EscribirLog("INFO|SAT.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (resultado)
                {
                    resultadoValidacion = datosRegreso?.ResultadoValidacion ?? 0;

                    if (resultadoValidacion == 1)
                    {
                        await _satCmsDao.ActualizarFormatoRechazado(lsFolio);
                        await _satTomiDao.ActualizarTombinFormatoRechazado(lsFolio);

                        Config.EscribirLog("INFO|SAT.procesaOperacion|A cancelar la operación.");
                        await _satTomiDao.CancelarPlanTrabajoInvalida(lsFolio, tipoOperacion, fechaEjecucion, moneda, importe);
                    }
                    else if (resultadoValidacion == 0)
                    {
                        if (idFormato == 5 || idFormato == 6 || idFormato == 9 || idFormato == 10)
                        {
                            string columna10017 = string.Empty;
                            string columna40002 = string.Empty;
                            string columna20005 = string.Empty;

                            string[] xmlDetalle = (datosRegreso.ValoresRegreso?.ArchivoSat ?? string.Empty).Split('|');
                            foreach (string item in xmlDetalle)
                            {
                                string[] matriz = item.Split('=');
                                if (matriz.Length < 2)
                                    continue;

                                if (matriz[0] == "10017") columna10017 = matriz[1];
                                if (matriz[0] == "40002") columna40002 = matriz[1];
                                if (matriz[0] == "20005") columna20005 = matriz[1];
                            }

                            if (decimal.TryParse(columna10017, out decimal montoDeclaracion) && montoDeclaracion == 0)
                            {
                                await _satCmsDao.ActualizarFormatoAceptadoConSello(
                                    lsFolio,
                                    datosRegreso.ValoresRegreso?.CadenaOriginal ?? string.Empty,
                                    datosRegreso.ValoresRegreso?.SelloDigital ?? string.Empty,
                                    datosRegreso.ValoresRegreso?.LLavePago ?? string.Empty);

                                await _satTomiDao.ActualizarTombinFormatoProcesado(lsFolio);

                                await _controlServicio.TerminaProceso(new ProcesoDTO
                                {
                                    Folio = lsFolio,
                                    TipoOperacion = tipoOperacion,
                                    Servicio = _configuracion.NombreServicio,
                                    NombreProceso = _configuracion.NombreProceso,
                                    AliasProceso = _configuracion.AliasProceso,
                                    PId = _configuracion.Pid.Value
                                });
                                await _satTomiDao.MarcarPlanTrabajoProcesadaYDetalleTerminado(lsFolio, tipoOperacion);
                                return true;
                            }
                            else
                            {
                                bool mismaFecha = string.Equals(columna40002, columna20005, StringComparison.Ordinal);
                                await _satCmsDao.ActualizarFormatoPendienteDebito(
                                    lsFolio,
                                    mismaFecha ? "APFT" : "REPE",
                                    datosRegreso.ValoresRegreso?.LLavePago);

                                if (mismaFecha)
                                    await _satTomiDao.ActualizarTombinFormatoConp(lsFolio);
                                else
                                    await _satTomiDao.ActualizarTombinFormatoConf(lsFolio);
                            }
                        }
                        else if (idFormato == 13)
                        {
                            bool mismaFecha = string.Equals(
                                datosRegreso.ValoresRegreso?.FechaPresentacion,
                                datosRegreso.ValoresRegreso?.FechaRecaudacion,
                                StringComparison.Ordinal);

                            await _satCmsDao.ActualizarFormatoPendienteDebito(
                                lsFolio,
                                mismaFecha ? "APFT" : "REPE",
                                null);

                            if (mismaFecha)
                                await _satTomiDao.ActualizarTombinFormatoConp(lsFolio);
                            else
                                await _satTomiDao.ActualizarTombinFormatoConf(lsFolio);
                        }
                    }
                }

                if (resultado)
                {
                    procesado = true;
                    Config.EscribirLog("INFO|SAT.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro CMS_A_TOMI con folio: {lsFolio}");

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

            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|SAT.procesaOperacion|A actualizar estatus en CMS y TOMI.");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });
                Config.EscribirLog("INFO|SAT.procesaOperacion|Operación fecha valor hoy.");
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
                string mensajeError = string.Empty;

                Config.EscribirLog("INFO|SAT.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = lsFolio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                int regsCms = 1;
                int regsTomi = 0;
                decimal importeTomi = 0;
                string fechaTomi = string.Empty;

                SAT_Tombin_CMS_Modelo datosTomi = await _satTomiDao.ObtenerDatosConciliacion(lsFolio);
                if (datosTomi != null)
                {
                    regsTomi = 1;
                    importeTomi = datosTomi.T24_DEBIT_AMOUNT;
                    fechaTomi = datosTomi.NF_FECHA_EJECUCION;
                }

                if (regsCms == 1 && regsTomi == 1)
                {
                    if (importe != importeTomi)
                        mensajeError += $" - Diferencia en el importe. CMS='{importe}', TOMI='{importeTomi}'.";
                }
                else
                {
                    mensajeError += " - No se ha registrado la operación en TOMI.";
                }

                if (!string.IsNullOrEmpty(mensajeError))
                    throw new Exception(mensajeError);

                if (!string.IsNullOrEmpty(fechaTomi) && !string.Equals(fechaTomi, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                {
                    Config.EscribirLog($"INFO|SAT.procesaOperacion|Se actualiza la fecha de la operación al '{fechaTomi}'.");
                    await _satTomiDao.MarcarPlanTrabajoFechaFutura(lsFolio, tipoOperacion, fechaTomi);
                }

                Config.EscribirLog("INFO|SAT.procesaOperacion|La operación está correctamente conciliada.");
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

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Cerrando conexión a base de datos del CMS.");
            return true;
        }
    }
}

using Aplicacion.DTOs.SER;
using Aplicacion.Interfaces.SER;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa SER.
    /// </summary>
    public class SER
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ISERTomiDao _tomiDao;
        private readonly ISERCmsDao _cmsDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "SER";

        private string folioUnico = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase SER.
        /// </summary>

        public SER(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ISERTomiDao tomiDao,
            ISERCmsDao cmsDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tomiDao = tomiDao;
            _cmsDao = cmsDao;
            _vwT24 = vwT24;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            string moneda = string.Empty;
            decimal importe = 0;

            bool fechaFutura;
            bool conEntradaSalida = true;
            bool procesado;
            bool resultado;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Iniciando");

            _configuracion.AliasProceso =
                await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
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

            long folioNumerico = Convert.ToInt64(folio);

            var formatoCms = await _cmsDao.ObtenerFormatoCmsPorFolio(folioNumerico.ToString());

            if (formatoCms == null)
                formatoCms = new SerFormatoCmsDto();

            object[] valoresFormato = CargarValoresFormato(formatoCms, folio);

            fechaEjecucion = ConversionHelper.Texto(valoresFormato[13]);

            if (!string.IsNullOrWhiteSpace(ConversionHelper.Texto(valoresFormato[4])))
                moneda = _vwT24.ValidaMoneda(ConversionHelper.Texto(valoresFormato[4]));

            importe = formatoCms.OperatingAmount;
            /// <summary>
            /// Ejecuta la operaciÃ³n ValidarMontosOperacion.
            /// </summary>

            ValidarMontosOperacion(valoresFormato, importe);

            Config.EscribirLog(
                $"INFO|{tipoOperacion}.procesaOperacion|Veamos que trae la fecha{valoresFormato[13]}-----{fechaEjecucion}");

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(fechaEjecucion, CultureInfo.InvariantCulture);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception(
                    $"Se desea realizar una operación al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion),
                folio);

            fechaFutura = fechaEjecucion.CompareTo(_configuracion.StrFechaSistema) > 0;

            // DATOS_GRAL
            if (_configuracion.AliasProceso == "DATOS_GRAL")
            {
                string subtipo = string.Empty;

                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var dto = new SerActualizarPlanTrabajoMaestroDto
                {
                    Folio = folioNumerico,
                    FolioUnico = folioUnico,
                    FechaValor = ConversionHelper.ConvertirNullableIntConDecimal(fechaEjecucion),
                    CveMoneda = moneda,
                    Importe = importe,
                    NumCliente = ConversionHelper.ConvertirNullableIntConDecimal(valoresFormato[29]),
                    NombreCliente = ConversionHelper.Texto(valoresFormato[1]),
                    NumCuenta = ConversionHelper.Texto(valoresFormato[30]),
                    SubtipoOperacion = subtipo
                };

                resultado = await _tomiDao.ActualizarPlanTrabajoMaestroLectura(dto);

                if (!resultado)
                    throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // CMS_A_TOMI
            if (_configuracion.AliasProceso == "CMS_A_TOMI")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A insertar datos de CMS a TOMI.");

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
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                    procesado = true;
                }
                else
                {
                    int count = await _tomiDao.ContarSerPayPorFolio(folioNumerico.ToString());

                    if (count > 0)
                    {
                        procesado = true;

                        Config.EscribirLog(
                            $"INFO|{tipoOperacion}.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro CMS_A_TOMI con folio: {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // TOMI_ENTRADAS/SALIDAS
            if (_configuracion.AliasProceso == "TOMI_ENTRADAS/SALIDAS")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A insertar datos de Entrada/Salida en TOMI.");

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

                        Config.EscribirLog(
                            $"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio en base de Entrada/Salida.");
                    }
                    else
                    {
                        int count = await _tomiDao.ContarEntradaSalidaPorReferencia(folioUnico);

                        if (count > 0)
                        {
                            procesado = true;

                            Config.EscribirLog(
                                $"INFO|{tipoOperacion}.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                        }
                    }
                }
                else
                {
                    procesado = true;

                    Config.EscribirLog(
                        $"INFO|{tipoOperacion}.procesaOperacion|La operación no debe generar Entrada/Salida en TOMI.");
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // CAMBIO_ESTATUS
            if (_configuracion.AliasProceso == "CAMBIO_ESTATUS")
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A actualizar estatus en CMS y TOMI.");

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

                if (fechaEjecucion == _configuracion.StrFechaSistema)
                {
                    await _cmsDao.ActualizarFormatoCmsAceptado(folioNumerico.ToString());

                    await _tomiDao.ActualizarSerPayAceptado(
                        folioNumerico.ToString(),
                        DateTime.Now);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Operación fecha valor hoy.");
                }
                else if (fechaEjecucion.CompareTo(_configuracion.StrFechaSistema) < 0)
                {
                    await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio, CultureInfo.InvariantCulture));
                    throw new Exception(
                        $"Se desea realizar una operación al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                }
                else
                {
                    await _cmsDao.ActualizarFormatoCmsPendienteEjecucion(folioNumerico.ToString());

                    DateTime fechaConfirma = DateTime.ParseExact(
                        DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                        "yyyy/MM/dd HH:mm",
                        CultureInfo.InvariantCulture);

                    await _tomiDao.ActualizarSerPayFechaFutura(
                        folioNumerico.ToString(),
                        fechaConfirma);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Operación fecha valor fútura.");
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro CAMBIO_ESTATUS con folio: {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            // CONCILIAR
            if (_configuracion.AliasProceso == "CONCILIAR")
            {
                string error = string.Empty;

                int regsCms = 1;
                int regsTomi = 0;
                int regsEntSal = 0;

                string fechaEjecucionTomi = string.Empty;
                string monedaTomi = string.Empty;
                decimal importeTomi = 0;

                string fechaEjecucionEntSal = string.Empty;
                string monedaEntSal = string.Empty;
                decimal importeEntSal = 0;

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var datosTomi = await _tomiDao.ObtenerSerPayConciliacionPorFolio(folioNumerico.ToString());

                if (datosTomi != null)
                {
                    regsTomi = 1;

                    fechaEjecucionTomi =
                        datosTomi.NfFechaEjecucion.HasValue
                            ? datosTomi.NfFechaEjecucion.Value.ToString("0")
                            : string.Empty;

                    monedaTomi =
                        !string.IsNullOrWhiteSpace(datosTomi.T24Currency)
                            ? _vwT24.ValidaMoneda(datosTomi.T24Currency)
                            : string.Empty;

                    importeTomi = datosTomi.T24Amount ?? 0;
                }

                if (conEntradaSalida)
                {
                    var datosEntSal =
                        await _tomiDao.ObtenerEntradaSalidaConciliacionPorReferencia(folioUnico);

                    if (datosEntSal != null)
                    {
                        regsEntSal = 1;

                        fechaEjecucionEntSal =
                            datosEntSal.EntFechaValor.HasValue
                                ? datosEntSal.EntFechaValor.Value.ToString("yyyyMMdd")
                                : string.Empty;

                        monedaEntSal =
                            !string.IsNullOrWhiteSpace(datosEntSal.EntMonedaCuenta)
                                ? _vwT24.ValidaMoneda(datosEntSal.EntMonedaCuenta)
                                : string.Empty;

                        importeEntSal = datosEntSal.EntImporte;
                    }
                }

                if (regsCms == 1 &&
                    regsTomi == 1 &&
                    ((conEntradaSalida && regsEntSal == 1) || !conEntradaSalida))
                {
                    if (fechaEjecucion != fechaEjecucionTomi)
                        error += Environment.NewLine + $" - Diferencia en la fecha de ejecución. CMS='{fechaEjecucion}', TOMI='{fechaEjecucionTomi}'.";

                    if (moneda != monedaTomi)
                        error += Environment.NewLine + $" - Diferencia en la monedas. CMS='{moneda}', TOMI='{monedaTomi}'.";

                    if (importe != importeTomi)
                        error += Environment.NewLine + $" - Diferencia en el importe. CMS='{importe}', TOMI='{importeTomi}'.";

                    if (conEntradaSalida)
                    {
                        if (fechaEjecucion != fechaEjecucionEntSal)
                            error += Environment.NewLine + $" - Diferencia en la fecha de ejecución de Entrada/Salida. CMS='{fechaEjecucion}', TOMI='{fechaEjecucionEntSal}'.";

                        if (moneda != monedaEntSal)
                            error += Environment.NewLine + $" - Diferencia en la monedas de Entrada/Salida. CMS='{moneda}', TOMI='{monedaEntSal}'.";

                        if (importe != importeEntSal)
                            error += Environment.NewLine + $" - Diferencia en el importe de Entrada/Salida. CMS='{importe}', Entrada/Salida='{importeEntSal}'.";
                    }

                    procesado = true;
                }
                else
                {
                    procesado = false;
                    error += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                }

                if (!string.IsNullOrEmpty(error))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(error);

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro CONCILIAR con folio: {folio}");

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación está correctamente conciliada.");

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

            return true;
        }

        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato)
        {
            try
            {
                var dto = new SerInsertarSerPayDto
                {
                    NfTipo = ConversionHelper.Texto(valoresFormato[27]),
                    NfFolio = Convert.ToInt64(valoresFormato[28]),

                    T24CustomerCode = ConversionHelper.Texto(valoresFormato[29]),
                    T24ShortName = ConversionHelper.Texto(valoresFormato[1]),

                    NfEstatus = "PEND",

                    NfUsuarioCaptura = ConversionHelper.Texto(valoresFormato[32]),
                    NfUsuarioAutoriza = ConversionHelper.Texto(valoresFormato[34]),

                    NfFechaCaptura = ConversionHelper.TryParseNullableDateTime(valoresFormato[31]),
                    NfFechaAutoriza = ConversionHelper.TryParseNullableDateTime(valoresFormato[33]),

                    NfFechaEjecucion = ConversionHelper.ConvertirNullableIntConDecimal(valoresFormato[13]),

                    T24BenCustName = ConversionHelper.Texto(valoresFormato[10]),
                    T24NoAgreement = ConversionHelper.Texto(valoresFormato[19]),
                    T24Currency = ConversionHelper.Texto(valoresFormato[4]),
                    T24Reference = ConversionHelper.Texto(valoresFormato[20]),

                    T24Amount = ConversionHelper.TryParseNullableDecimal(valoresFormato[35]),

                    T24Concept = ConversionHelper.Texto(valoresFormato[15]),
                    T24AcctNo = ConversionHelper.Texto(valoresFormato[30]),

                    NfMontoEnviar = ConversionHelper.TryParseNullableDecimal(valoresFormato[18]),
                    NfFolioUnico = folioUnico,

                    IdFormatoPlantilla = ConversionHelper.ConvertirNullableLongConDecimal(valoresFormato[37]),

                    NfTipoCuenta = ConversionHelper.Texto(valoresFormato[8]),

                    IdPlantilla = ConversionHelper.Texto(valoresFormato[36])
                };

                bool resultado = await _tomiDao.InsertarSerPayDesdeCms(dto);

                Config.EscribirLog($"INFO|{tipoOperacion}.CMS_A_TOMI|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|{tipoOperacion}.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");

                throw;
            }
        }

        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            try
            {
                long folioNumerico = Convert.ToInt64(folio);

                var serPay = await _tomiDao.ObtenerSerPayParaEntradaSalida(folioNumerico.ToString());

                if (serPay == null)
                    return false;

                var dto = new SerInsertarEntradaSalidaDto
                {
                    Referencia = serPay.NfFolioUnico,

                    NumCliente = ConversionHelper.ConvertirDecimalOrDefault(serPay.T24CustomerCode),
                    Cdl = ConversionHelper.ConvertirDecimalOrDefault(serPay.T24CustomerCode),

                    Ordenante = ConversionHelper.Texto(serPay.T24CompanyName),

                    Operacion = 218,
                    OperacionDesc = "Salida  (TTS)",

                    Tipo = "D",
                    TipoDesc = "Doméstica",

                    Cuenta = ConversionHelper.TryParseNullableDecimal(serPay.T24AcctNo),
                    Nombre = ConversionHelper.Texto(serPay.NfNombreCuenta),
                    CuentaMoneda = _vwT24.ValidaMoneda(serPay.NfMonedaCuenta),

                    Via = 800,
                    ViaDesc = "TRF ENVIADA MXN SER",

                    Importe = serPay.T24Amount ?? 0,
                    Saldo = 0,

                    Comentarios = ConversionHelper.Texto(serPay.T24Concept),

                    FechaCaptura = ConversionHelper.TextoToFechaTryParsePartes(_configuracion.StrFechaSistema),
                    FechaValor = ConversionHelper.TextoToFechaTryParsePartes(
                        serPay.NfFechaEjecucion.HasValue
                            ? serPay.NfFechaEjecucion.Value.ToString("0")
                            : string.Empty),

                    Hora = DateTime.Now.ToLongTimeString(),
                    Capturista = "JEAI",

                    Fad = 0,

                    ReferenciaControl = "A",
                    ReferenciaControlCont = 0
                };

                bool resultado = await _tomiDao.InsertarEntradaSalida(dto);

                Config.EscribirLog($"INFO|{tipoOperacion}.TOMI_ENTRADA_SALIDA|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog(
                    $"ERROR|{tipoOperacion}.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}. Trace: {ex}");

                throw;
            }
        }

        private object[] CargarValoresFormato(SerFormatoCmsDto formatoCms, string folio)
        {
            object[] valoresFormato = new object[38];

            valoresFormato[27] = tipoOperacion;
            valoresFormato[28] = formatoCms.IdFormat;
            valoresFormato[29] = formatoCms.Cdl;
            valoresFormato[30] = formatoCms.IdAccount;
            valoresFormato[31] = formatoCms.CaptureDate;
            valoresFormato[32] = formatoCms.IdUserCapture;
            valoresFormato[33] = formatoCms.AuthorizationDate;
            valoresFormato[34] = formatoCms.IdAuthorizationUser;
            valoresFormato[35] = formatoCms.OperatingAmount;
            valoresFormato[36] = formatoCms.IdTemplate;
            valoresFormato[37] = formatoCms.IdTemplateFormat;

            string[] valoresPipes =
                ConversionHelper.Texto(formatoCms.ContentFormat)
                    .Split(new[] { '|' }, StringSplitOptions.None);

            if (valoresPipes.Length < 28)
            {
                throw new Exception(
                    $"El contenidoFormato del folio {folio} es diferente al esperado, debe ser 27 datos y se tienen {valoresPipes.Length - 1}.");
            }

            for (int i = 0; i <= 26; i++)
            {
                string[] llaveValor =
                    valoresPipes[i].Split(new[] { '=' }, 2, StringSplitOptions.None);

                valoresFormato[i] = llaveValor[1];
            }

            return valoresFormato;
        }

        private void ValidarMontosOperacion(object[] valoresFormato, decimal operatingAmount)
        {
            decimal montoCapturado = ConversionHelper.ConvertirDecimalOrDefault(valoresFormato[5]);
            decimal montoEnviado = ConversionHelper.ConvertirDecimalOrDefault(valoresFormato[18]);
            decimal montoOperacion = operatingAmount;

            if (montoCapturado != montoEnviado || montoCapturado != montoOperacion)
            {
                throw new Exception(
                    $"Hay diferencias entre los montos de la operación: MontoOperacion={montoOperacion}, MontoCapturado={montoCapturado}, MontoEnviado={montoEnviado}.");
            }
        }
    }
}

using Aplicacion.DTOs.SPID;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.SPID;
using Aplicacion.Modelos;
using Aplicacion.Modelos.SPID;
using Aplicacion.Servicios.FirmaElectronica;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa SPID.
    /// </summary>
    public class SPID
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ISPIDCmsDao _spidCmsDao;
        private readonly ISPIDTomiDao _spidTomiDao;
        private readonly ISpidVistasT24Dao _spidVistasT24Dao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IComunSPID _comunSPID;

        private const string tipoOperacion = "SPID";
        private string folioUnico = string.Empty;
        private string folioPropuesto = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase SPID.
        /// </summary>

        public SPID(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ISPIDCmsDao spidCmsDao,
            ISPIDTomiDao spidTomiDao,
            ISpidVistasT24Dao spidVistasT24Dao,
            ConexionT24Mw vwT24,
            IComunSPID comunSPID)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _spidCmsDao = spidCmsDao;
            _spidTomiDao = spidTomiDao;
            _spidVistasT24Dao = spidVistasT24Dao;
            _vwT24 = vwT24;
            _comunSPID = comunSPID;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ProcesaOperacion(string folio, CancellationToken ct = default(CancellationToken))
        {
            ct.ThrowIfCancellationRequested();

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

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Confirmando que el certificado se encuentre.");
            using (var firma = new MetodosFirma(_configuracion))
            {
                if (!firma.CertificadoEncontrado())
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de SPID: No se encontro el certificado y/o llave para realizar la firma de la transacci├│n");
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Aperturando conexi├│n a base de datos del CMS.");

            SPID_Formato_DTO formato = await _spidCmsDao.ObtenerFormatoPorFolio(folio);
            if (formato == null)
                throw new Exception($"No se encontr├│ formato SPID para folio {folio}");

            string[] valoresPipes = (formato.ContenidoFormato ?? string.Empty).Split('|');
            if (valoresPipes.Length - 1 < 18)
                throw new Exception($"El contenidoFormato del folio {folio} es diferente al esperado, debe ser 18 datos y se tienen {valoresPipes.Length - 1}.");

            object[] valoresFormato = new object[37];
            for (int i = 0; i <= 18; i++)
            {
                string[] llaveValor = valoresPipes[i].Split('=');
                if (llaveValor.Length < 2)
                    throw new Exception($"El contenidoFormato del folio {folio} contiene un dato inv├ílido en la posici├│n {i}: '{valoresPipes[i]}'.");
                valoresFormato[i] = llaveValor[1];
            }

            valoresFormato[19] = "SPD";
            valoresFormato[20] = formato.Id_Formato;
            valoresFormato[21] = formato.Cdl;
            valoresFormato[22] = formato.Id_Cuenta;
            valoresFormato[23] = formato.FechaCaptura;
            valoresFormato[24] = formato.Id_UsuarioCaptura;
            valoresFormato[25] = formato.Fecha_Autoriza;
            valoresFormato[26] = formato.Id_UsuarioAutorizo;
            valoresFormato[27] = formato.MontoOperacion;
            valoresFormato[28] = formato.Id_Plantilla;
            valoresFormato[29] = formato.Id_Formato_Plantilla;
            valoresFormato[30] = formato.ClasificacionOper;
            valoresFormato[31] = formato.RelacionComercial;
            valoresFormato[32] = formato.ReferenciaNumerica;
            valoresFormato[33] = formato.IPOrigen;

            string folioForex = Convert.ToString(valoresFormato[3]);
            string fechaEjecucion = Convert.ToString(valoresFormato[13]);
            string moneda = _vwT24.ValidaMoneda(Convert.ToString(valoresFormato[4]));
            decimal importe = formato.MontoOperacion;

            if (!decimal.TryParse(Convert.ToString(valoresFormato[5]), out decimal montoCapturado))
                montoCapturado = 0;
            if (!decimal.TryParse(Convert.ToString(valoresFormato[18]), out decimal montoEnviado))
                montoEnviado = 0;

            if (montoCapturado != montoEnviado || montoCapturado != importe)
                throw new Exception($"Hay diferencias entre los montos de la operaci├│n: MontoOperacion={importe}, MontoCapturado={montoCapturado}, MontoEnviado={montoEnviado}.");

            if (string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) < 0)
            {
                await _controlServicio.MarcarErrorFechaPasada(ConversionHelper.ParseLongOrDefault(folio));
                throw new Exception($"Se desea realizar una operaci├│n al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            folioUnico = await _vwT24.GeneraFolioUnico(tipoOperacion, ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion), folio);
            folioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                folioUnico,
                ConversionHelper.ParseIntOrDefault(ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion)),
                tipoOperacion);

            if (string.IsNullOrWhiteSpace(folioPropuesto))
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("Error al obtener el folio propuesto de T24");

            bool conForex = false;
            bool forexCancelado = false;
            bool conEntradaSalida = true;

            if (!string.IsNullOrWhiteSpace(folioForex))
            {
                conForex = true;
                if (decimal.TryParse(folioForex, out _))
                    throw new Exception($"El Folio del Forex no es válido porque es numérico; se espera una referencia alfanumérica/external ref: {folioForex}");

                SPID_ForexBancosFax_DTO statusForex = await _spidTomiDao.ObtenerStatusForexPorExternalRef(folioForex);
                if (statusForex != null && string.Equals(statusForex.STATUSOPERACION, "CANCELADO", StringComparison.OrdinalIgnoreCase))
                {
                    forexCancelado = true;
                    conEntradaSalida = false;
                }
            }

            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                string subTipo = conForex ? $"FX:{folioForex}" : string.Empty;
                await _spidTomiDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    fechaEjecucion,
                    moneda,
                    importe,
                    Convert.ToString(valoresFormato[21]),
                    Convert.ToString(valoresFormato[1]),
                    Convert.ToString(valoresFormato[22]),
                    subTipo);
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            if (string.Equals(_configuracion.AliasProceso, "CMS_A_TOMI", StringComparison.Ordinal))
            {
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                SPID_InsertarFormatoModelo modelo = MapearInsertarFormato(valoresFormato);
                modelo.NF_FOLIO_UNICO = folioUnico;

                (string direccion, string codigoPostal) = await ObtenerDireccionInsight(Convert.ToString(valoresFormato[21]), folio, ct);
                modelo.NF_ADDRESS = direccion;
                modelo.NF_ZIP_CODE = codigoPostal;

                bool procesado = await _spidTomiDao.InsertarFormatoDesdeCms(modelo);
                if (!procesado)
                {
                    int numRegs = await _spidTomiDao.ContarTombinNfPorFolio(folio);
                    procesado = numRegs > 0;
                }

                if (procesado)
                {
                    bool firma = await _comunSPID.GeneraFirmaDigitalSPID("tb_tombin_nf_spid", folio, folioPropuesto, null, null);
                    if (!firma)
                        throw new Exception($"Se present├│ un error al generar la firma digital del folio: {folio}");
                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso =
                        await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
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

            if (string.Equals(_configuracion.AliasProceso, "TOMI_ENTRADAS/SALIDAS", StringComparison.Ordinal))
            {
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                bool procesado;
                if (conEntradaSalida)
                {
                    SPID_Tombin_NF_DTO tombin = await _spidTomiDao.ObtenerTombinNfPorFolio(folio);
                    procesado = false;
                    if (tombin != null)
                    {
                        bool conForexRelacionado = !string.IsNullOrWhiteSpace(tombin.NF_FOREX_RELACIONADO);

                        decimal? cuenta = ConversionHelper.ParseNullableDecimalInvariant(tombin.T24_DEBIT_ACCT_NO);
                        string nombreCuenta = tombin.NF_NOMBRE_CUENTA;
                        string monedaCuenta = _vwT24.ValidaMoneda(tombin.NF_MONEDA_CUENTA);

                        if (conForexRelacionado)
                        {
                            var cuentasFad = await _vwT24.ObtieneDatosCuentasFad();
                            var cuentaFad = cuentasFad?.FirstOrDefault(x => x.Moneda == _vwT24.MonedaDefaultMN);
                            if (cuentaFad == null || !decimal.TryParse(cuentaFad.Cuenta, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal cuentaFadDecimal))
                                /// <summary>
                                /// Ejecuta la operaciÃ³n Exception.
                                /// </summary>
                                throw new Exception("No fue posible obtener la cuenta FAD para la operaci├│n con Forex relacionado.");

                            cuenta = cuentaFadDecimal;
                            nombreCuenta = "BTMM USD FAD";
                            monedaCuenta = "USD";
                        }

                        var es = new SPID_InsertarEntradasSalidasModelo
                        {
                            Referencia = tombin.NF_FOLIO_UNICO,
                            Num_Cliente = ConversionHelper.ParseNullableDecimalInvariant(tombin.T24_CUSTOMER_CODE),
                            CDL = ConversionHelper.ParseNullableDecimalInvariant(tombin.T24_CUSTOMER_CODE),
                            Ordenante = tombin.T24_COMPANY_NAME,
                            Operacion = 218,
                            OperacionDesc = "Salida  (TTS)",
                            Tipo = "D",
                            TipoDesc = "Dom├⌐stica",
                            Cuenta = cuenta,
                            Nombre = nombreCuenta,
                            CuentaMoneda = monedaCuenta,
                            Via = 800,
                            ViaDesc = "TRF ENVIADA USD SPID",
                            Importe = tombin.T24_DEBIT_AMOUNT,
                            Saldo = 0,
                            Comentarios = tombin.T24_DEBIT_THEIR_REF,
                            Fecha_Captura = ConversionHelper.TextoToFechaYmdOnly(_configuracion.StrFechaSistema),
                            FechaValor = ConversionHelper.TextoToFechaYmdOnly(tombin.NF_FECHA_EJECUCION.ToString()),
                            Hora = DateTime.Now.ToLongTimeString(),
                            Capturista = "JEAI",
                            Fad = 0,
                            Referencia_Control = "A",
                            Referencia_Control_Cont = 0
                        };

                        procesado = await _spidTomiDao.InsertarEntradaSalida(es);
                    }

                    if (!procesado)
                    {
                        int numRegs = await _spidTomiDao.ContarEntradaSalidaPorReferencia(folioUnico);
                        procesado = numRegs > 0;
                    }
                }
                else
                {
                    procesado = true;
                }

                if (!procesado)
                    throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                if (conForex && forexCancelado)
                {
                    await _spidCmsDao.ActualizarFormato(folio, "Formato rechazado", "RECC", "El forex relacionado ha sido cancelado");
                    await _spidTomiDao.ActualizarTombinNfEstatus(folio, "RECC", "El forex relacionado ha sido cancelado");

                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
                    await _spidTomiDao.CancelarPlanTrabajoPorForex(folio, tipoOperacion, fechaEjecucion, moneda, importe);
                    return true;
                }

                if (string.Equals(fechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                {
                    await _spidCmsDao.ActualizarFormato(folio, "Formato aceptado", "APFT", string.Empty);
                    await _spidTomiDao.ActualizarTombinNfEstatus(folio, "SCNP", string.Empty);
                }
                else if (string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) < 0)
                {
                    await _controlServicio.MarcarErrorFechaPasada(ConversionHelper.ParseLongOrDefault(folio));
                    throw new Exception($"Se desea realizar una operaci├│n al {fechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                }
                else
                {
                    await _spidCmsDao.ActualizarFormato(folio, "Formato recibido pendiente ejecuci├│n", "REPE", string.Empty);
                    await _spidTomiDao.ActualizarTombinNfEstatus(folio, "SCNF", string.Empty);
                }
                await _controlServicio.TerminaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                _configuracion.AliasProceso =
                    await _controlServicio.ObtenerUltimoProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });
            }

            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                int regsCms = 1;
                int regsTomi = 0;
                int regsEs = 0;
                string errores = string.Empty;

                SPID_Tombin_NF_DTO tomi = await _spidTomiDao.ObtenerConciliacionNf(folio, conForex && forexCancelado);
                if (tomi != null)
                    regsTomi = 1;

                SPID_Tombin_ES_DTO es = null;
                if (conEntradaSalida)
                {
                    es = await _spidTomiDao.ObtenerConciliacionEs(folioUnico);
                    if (es != null)
                        regsEs = 1;
                }

                if (regsCms == 1 && regsTomi == 1 && ((conEntradaSalida && regsEs == 1) || !conEntradaSalida))
                {
                    if (!string.Equals(folioForex ?? string.Empty, tomi.NF_FOREX_RELACIONADO ?? string.Empty, StringComparison.Ordinal))
                        errores += Environment.NewLine + $" - Diferencia en la autorizaci├│n de FOREX. CMS='{folioForex}', TOMI='{tomi.NF_FOREX_RELACIONADO}'.";
                    if (!string.Equals(fechaEjecucion, tomi.NF_FECHA_EJECUCION.ToString(), StringComparison.Ordinal))
                        errores += Environment.NewLine + $" - Diferencia en la fecha de ejecuci├│n. CMS='{fechaEjecucion}', TOMI='{tomi.NF_FECHA_EJECUCION}'.";
                    if (!string.Equals(moneda, _vwT24.ValidaMoneda(tomi.T24_DEBIT_CURRENCY), StringComparison.Ordinal))
                        errores += Environment.NewLine + $" - Diferencia en la monedas. CMS='{moneda}', TOMI='{tomi.T24_DEBIT_CURRENCY}'.";
                    if (importe != tomi.T24_DEBIT_AMOUNT)
                        errores += Environment.NewLine + $" - Diferencia en el importe. CMS='{importe}', TOMI='{tomi.T24_DEBIT_AMOUNT}'.";

                    if (conEntradaSalida)
                    {
                        if (!string.Equals(fechaEjecucion, es.Ent_fechaValor, StringComparison.Ordinal))
                            errores += Environment.NewLine + $" - Diferencia en la fecha de ejecuci├│n de Entrada/Salida. CMS='{fechaEjecucion}', TOMI='{es.Ent_fechaValor}'.";
                        if (!string.Equals(moneda, _vwT24.ValidaMoneda(es.Ent_moneda_cuenta), StringComparison.Ordinal))
                            errores += Environment.NewLine + $" - Diferencia en la monedas de Entrada/Salida. CMS='{moneda}', TOMI='{es.Ent_moneda_cuenta}'.";
                        if (importe != (es.Ent_importe ?? 0))
                            errores += Environment.NewLine + $" - Diferencia en el importe de Entrada/Salida. CMS='{importe}', Entrada/Salida='{es.Ent_importe}'.";
                    }
                }
                else
                {
                    errores += Environment.NewLine + " - No se ha registrado la operaci├│n en TOMI.";
                }

                if (!string.IsNullOrWhiteSpace(errores))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(errores);

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

            return true;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ValidarDiaHabilSiguiente(DateTime dtFecha, long lFolio)
        {
            return await _spidTomiDao.ExisteDiaNoHabil(dtFecha);
        }

        private async Task<(string Direccion, string CodigoPostal)> ObtenerDireccionInsight(string cdl, string folio, CancellationToken ct)
        {
            DateTime fecha = DateTime.Now.Date;
            bool fechaValida = false;
            int diasRetroceso = 0;
            const int maxDiasRetroceso = 366;

            while (!fechaValida)
            {
                ct.ThrowIfCancellationRequested();

                if (diasRetroceso >= maxDiasRetroceso)
                    throw new Exception($"No se encontró un día hábil dentro de los últimos {maxDiasRetroceso} días para el folio {folio}.");

                fecha = fecha.AddDays(-1);
                diasRetroceso++;

                bool esNoHabil = await ValidarDiaHabilSiguiente(fecha, ConversionHelper.ParseLongOrDefault(folio));
                if (esNoHabil || fecha.DayOfWeek == DayOfWeek.Saturday || fecha.DayOfWeek == DayOfWeek.Sunday)
                    fechaValida = false;
                else
                    fechaValida = true;
            }

            string bsDate = fecha.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            var insight = await _spidVistasT24Dao.ObtenerDireccionSpidPorCdl(cdl, bsDate);
            if (insight == null)
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("Error al obtener de insight los datos del domicilio del cliente");

            string direccion = $"{insight.P1_CALLE} {insight.P1_EXTERIOR} {insight.P1_INTERIOR}".Trim();
            string cp = insight.P1_CODIGO_POSTAL ?? string.Empty;

            if (string.IsNullOrWhiteSpace(direccion) || string.IsNullOrWhiteSpace(cp))
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("Error al obtener de insight los datos del domicilio del cliente");
            /// <summary>
            /// Ejecuta la operaciÃ³n return.
            /// </summary>

            return (direccion, cp);
        }

        private SPID_InsertarFormatoModelo MapearInsertarFormato(object[] v)
        {
            return new SPID_InsertarFormatoModelo
            {
                NF_TIPO = Convert.ToString(v[19]),
                NF_FOLIO = ConversionHelper.ParseLongOrDefault(Convert.ToString(v[20])),
                T24_CUSTOMER_CODE = Convert.ToString(v[21]),
                T24_DEBIT_ACCT_NO = Convert.ToString(v[22]),
                NF_ESTATUS = "PEND",
                NF_USUARIO_CAPTURA = Convert.ToString(v[24]),
                NF_USUARIO_AUTORIZA = Convert.ToString(v[26]),
                NF_FECHA_CAPTURA = v[23] as DateTime? ?? ConversionHelper.TryParseNullableDateTime(v[23]),
                NF_FECHA_AUTORIZA = v[25] as DateTime? ?? ConversionHelper.TryParseNullableDateTime(v[25]),
                T24_DEBIT_AMOUNT = ConversionHelper.ParseNullableDecimalInvariant(v[27]),
                T24_SHORT_NAME = Convert.ToString(v[1]),
                NF_FOREX_RELACIONADO = Convert.ToString(v[3]),
                T24_DEBIT_CURRENCY = Convert.ToString(v[4]),
                NF_BANCO_BENEF = Convert.ToString(v[6]),
                T24_BEN_ACCT_NO = Convert.ToString(v[7]),
                NF_TIPO_CUENTA = Convert.ToString(v[8]),
                T24_BEN_CUST_NAME = Convert.ToString(v[10]),
                NF_RFC_BENEFICIARIO = Convert.ToString(v[11]),
                NF_TASA_RETENCION = Convert.ToString(v[12]),
                NF_FECHA_EJECUCION = ConversionHelper.ParseIntOrDefault(Convert.ToString(v[13])),
                T24_DEBIT_THEIR_REF = Convert.ToString(v[15]),
                NF_MONTO_ENVIAR = ConversionHelper.ParseNullableDecimalInvariant(v[18]),
                ID_PLANTILLA = Convert.ToString(v[28]),
                ID_FORMATO_PLANTILLA = ConversionHelper.ParseLongOrDefault(Convert.ToString(v[29])),
                NF_CLASIFICACION_OPERACION = Convert.ToString(v[30]),
                NF_RELACION_COMERCIAL = Convert.ToString(v[31]),
                NF_NUMERIC_REFERENCE = Convert.ToString(v[32]),
                NF_DIRECCION_IP = Convert.ToString(v[33])
            };
        }
    }
}

using Aplicacion.DTOs.SPID_GPH;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.SPID;
using Aplicacion.Modelos;
using Aplicacion.Servicios.FirmaElectronica;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa SPIDGPH.
    /// </summary>
    public class SPIDGPH
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ISPIDGphDao _spidGphDao;
        private readonly ISpidVistasT24Dao _spidVistasT24Dao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IComunSPID _comunSpid;

        private const string tipoOperacion = "SPID_GPH";

        private string folioUnico = string.Empty;
        private string folioPropuesto = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase SPIDGPH.
        /// </summary>

        public SPIDGPH(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ISPIDGphDao spidGphDao,
            ISpidVistasT24Dao spidVistasT24Dao,
            ConexionT24Mw vwT24,
            IComunSPID comunSpid)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _spidGphDao = spidGphDao;
            _spidVistasT24Dao = spidVistasT24Dao;
            _vwT24 = vwT24;
            _comunSpid = comunSpid;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ProcesOperacion(string folio, CancellationToken ct = default(CancellationToken))
        {
            ct.ThrowIfCancellationRequested();

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

            using (var firma = new MetodosFirma(_configuracion))
            {
                if (!firma.CertificadoEncontrado())
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de SPID: No se encontrÃ³ el certificado y/o llave para realizar la firma de la transacciÃ³n");
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var datos = await _spidGphDao.ObtenerDatosBasicosPorFolio(folio);
                string fechaEjecucion = string.Empty;
                string cuenta = string.Empty;
                string cdl = string.Empty;

                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                    cuenta = datos.T24DebitAcctNo ?? string.Empty;
                    cdl = datos.T24CustomerCode ?? string.Empty;
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
                    await _spidGphDao.ActualizarFolioUnico(folio, folioUnico);
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                folioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                    folioUnico,
                    ConversionHelper.ParseIntOrDefault(ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion)),
                    tipoOperacion);

                if (string.IsNullOrWhiteSpace(folioPropuesto))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error al obtener el folio propuesto de T24");

                await _spidGphDao.ActualizarPlanTrabajoMaestroLectura(folio, tipoOperacion, folioUnico);

                bool direccionOk = await ActualizarDireccion(cdl, folio, ct);
                if (!direccionOk)
                    throw new Exception($"Error al obtener los datos de Insight, con folio: {folio}");

                var validacionCuenta = await EsCuentaSpidValida(cuenta);
                bool cuentaValida = validacionCuenta.EsValida;
                string errorDetencion = validacionCuenta.ErrorDetencion;
                if (!cuentaValida)
                {
                    await _spidGphDao.MarcarErrorCuentaInvalida(folio, tipoOperacion, errorDetencion);
                }

                bool firmaOk = await _comunSpid.GeneraFirmaDigitalSPID("tb_tombin_nf_spid_gph", folio, folioPropuesto, null, null);
                if (!firmaOk)
                    throw new Exception($"Se presentÃ³ un error al generar la firma digital del: {folio}");

                if (cuentaValida)
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

                return false;
            }

            return false;
        }

        private async Task<bool> ActualizarDireccion(string cdl, string folio, CancellationToken ct)
        {
            try
            {
                DateTime fecha = DateTime.Now.Date;
                bool fechaValida = false;
                int diasRetroceso = 0;
                const int maxDiasRetroceso = 366;

                while (!fechaValida)
                {
                    ct.ThrowIfCancellationRequested();

                    if (diasRetroceso >= maxDiasRetroceso)
                        throw new Exception($"No se encontró un día hábil dentro de los últimos {maxDiasRetroceso} días para el folio {folio}.");

                    fecha = fecha.AddDays(-1);
                    diasRetroceso++;

                    bool esNoHabil = await _spidGphDao.ExisteDiaNoHabil(fecha);
                    if (esNoHabil || fecha.DayOfWeek == DayOfWeek.Sunday || fecha.DayOfWeek == DayOfWeek.Saturday)
                    {
                        fechaValida = false;
                    }
                    else
                    {
                        fechaValida = true;
                    }
                }

                string bsDate = fecha.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
                SpidGphInsightDireccionDto insight = await _spidVistasT24Dao.ObtenerDireccionSpidGphPorCdl(cdl, bsDate);
                if (insight == null)
                    return false;

                string direccion = (insight.P1CALLE ?? string.Empty) + " " +
                                   (insight.P1EXTERIOR ?? string.Empty) + " " +
                                   (insight.P1INTERIOR ?? string.Empty);
                direccion = direccion.Trim();

                string cp = insight.P1CODIGOPOSTAL ?? string.Empty;

                if (string.IsNullOrWhiteSpace(direccion))
                    return false;

                return await _spidGphDao.ActualizarDireccion(folio, direccion, cp);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{tipoOperacion}.ActualizarDireccion|Error al obtener los datos de Insight. - {ex}");
                throw;
            }
        }

        private async Task<(bool EsValida, string ErrorDetencion)> EsCuentaSpidValida(string cuenta)
        {
            try
            {
                string mensaje = string.Empty;
                EsClienteSpidValidoResultado resultado =
                    await _vwT24.EsCuentaSpidValida(cuenta, mensaje, string.Empty);

                return (resultado.EsClienteValido,
                    resultado.EsClienteValido
                        ? string.Empty
                        : (resultado.Mensaje ?? "No se pudo validar la cuenta SPID en T24."));
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{tipoOperacion}.procesaOperacion|Error técnico al validar EsCuentaSpidValida: {ex}");
                throw;
            }
        }
    }
}

using Aplicacion.DTOs.SPID_GPH;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.SPID;
using Aplicacion.Modelos;
using Aplicacion.Servicios.FirmaElectronica;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa SPIDLocal.
    /// </summary>
    public class SPIDLocal
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ISPIDLocalDao _spidLocalDao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IComunSPID _comunSPID;

        private const string tipoOperacion = "SPID_Local";

        private string folioUnico = string.Empty;
        private string folioPropuesto = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase SPIDLocal.
        /// </summary>

        public SPIDLocal(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ISPIDLocalDao spidLocalDao,
            ConexionT24Mw vwT24,
            IComunSPID comunSPID)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _spidLocalDao = spidLocalDao;
            _vwT24 = vwT24;
            _comunSPID = comunSPID;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ProcesOperacion(string folio)
        {
            long folioNumerico = ConversionHelper.ConvertirLongRequerido(folio, nameof(folio));

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

            using (var firma = new MetodosFirma(_configuracion))
            {
                if (!firma.CertificadoEncontrado())
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de SPID: No se encontro el certificado y/o llave para realizar la firma de la transacciÃ³n");
                }
            }

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

                bool procesado;
                var datos = await _spidLocalDao.ObtenerDatosBasicosPorFolio(folioNumerico);

                folioUnico = datos?.NfFolioUnico ?? string.Empty;
                string fechaEjecucion = datos?.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                string cuenta = datos?.NfDebitAcctNo ?? string.Empty;

                if (string.IsNullOrWhiteSpace(folioUnico))
                {
                    folioUnico = await _vwT24.GeneraFolioUnico(
                        tipoOperacion,
                        ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion),
                        folio);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Creamos folio_unico='{folioUnico}'.");

                    await _spidLocalDao.ActualizarFolioUnico(folioNumerico, folioUnico);
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                folioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                    folioUnico,
                    int.Parse(ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion), CultureInfo.InvariantCulture),
                    tipoOperacion);

                if (string.IsNullOrWhiteSpace(folioPropuesto))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error al obtener el folio propuesto de T24");

                await _spidLocalDao.ActualizarPlanTrabajoMaestroLectura(
                    folioNumerico,
                    tipoOperacion,
                    folioUnico,
                ConversionHelper.ConvertirIntRequerido(fechaEjecucion, nameof(fechaEjecucion)));

                var validacionCuenta = await ValidarCuentaSpid(cuenta);
                procesado = validacionCuenta.EsValida;
                if (!validacionCuenta.EsValida)
                {
                    await _spidLocalDao.MarcarErrorCuentaInvalida(folioNumerico, tipoOperacion, validacionCuenta.ErrorDescripcion);
                }

                if (!await _comunSPID.GeneraFirmaDigitalSPID("tb_tombin_nf_spid_local", folio, folioPropuesto, null, null))
                {
                    throw new Exception($"Se presentÃ³ un error al generar la firma digital del: {folio}");
                }

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

                return false;
            }

            return false;
        }

        private async Task<ValidacionCuentaSpidResultado> ValidarCuentaSpid(string cuenta)
        {
            try
            {
                string mensaje = string.Empty;
                EsClienteSpidValidoResultado resultado =
                    await _vwT24.EsCuentaSpidValida(cuenta, mensaje, string.Empty);

                return new ValidacionCuentaSpidResultado
                {
                    EsValida = resultado.EsClienteValido,
                    ErrorDescripcion = resultado.EsClienteValido
                        ? string.Empty
                        : (resultado.Mensaje ?? "No existe la cuenta en el sistema T24")
                };
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"AVISO|{tipoOperacion}.procesaOperacion|No se pudo validar EsCuentaSpidValida: {ex.Message}");
            }

            return new ValidacionCuentaSpidResultado
            {
                EsValida = false,
                ErrorDescripcion = "No existe la cuenta en el sistema T24"
            };
        }

    }
}

using Aplicacion.DTOs.SPID_H2H;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.SPID;
using Aplicacion.Interfaces.SPID_H2H;
using Aplicacion.Modelos;
using Aplicacion.Servicios.FirmaElectronica;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa SPID H2 H.
    /// </summary>
    public class SPID_H2H
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ISpidH2hTomiDao _spidH2hDao;
        private readonly ISpidH2hVistasT24Dao _spidH2hVistasT24Dao;
        private readonly ConexionT24Mw _vwT24;
        private readonly ComunSPID _comunSPID;

        private const string tipoOperacion = "SPID_H2H";
        private string folio_unico = string.Empty;
        private string lsFolioPropuesto = string.Empty;

        /// <summary>
        /// Constructor.
        /// </summary>
        public SPID_H2H(ConfiguracionModelo configuracion, IControlServicio controlServicio, ISpidH2hTomiDao spidH2hDao, ISpidH2hVistasT24Dao spidH2hVistasT24Dao, ConexionT24Mw vwT24, ComunSPID comunSPID)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _spidH2hDao = spidH2hDao;
            _spidH2hVistasT24Dao = spidH2hVistasT24Dao;
            _vwT24 = vwT24;
            _comunSPID = comunSPID;
        }
        
        /// <summary>
        /// Procesa la operación SPID_H2H.
        /// </summary>
        public async Task<bool> procesaOperacion(string folio, CancellationToken ct = default(CancellationToken))
        {
            ct.ThrowIfCancellationRequested();

            string lsFechaEjecucion;
            bool lbProcesado;
            string strCDL;

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

            // Confirmo que se encuentre el certificado y llave para la firma digital de lo contrario no se inicia el proceso.
            using (var firma = new MetodosFirma(_configuracion))
            {
                if (!firma.CertificadoEncontrado())
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error en procesamiento de SPID: No se encontro el certificado y/o llave para realizar la firma de la transacción");
            }

// Inicio.
            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string lsCuenta;
                string lsDatosCliente;
                string lsErrorDetencion;
                bool lbCuentaSPID_Valida;

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

                lbProcesado = false;

                // Datos de la operación.
                lsFechaEjecucion = string.Empty;
                lsCuenta = string.Empty;
                strCDL = string.Empty;

                SpidH2hDatosOperacionDto datosOperacion = await _spidH2hDao.ObtenerDatosOperacion(ConversionHelper.ParseLongRequerido(folio, "Folio"));
                if (datosOperacion != null)
                {
                    folio_unico = datosOperacion.FolioUnico ?? string.Empty;
                    lsFechaEjecucion = datosOperacion.FechaEjecucion?.ToString() ?? string.Empty;
                    lsCuenta = datosOperacion.DebitAcctNo ?? string.Empty;
                    strCDL = datosOperacion.CustomerCode ?? string.Empty;
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

                // Genero folio propuesto de T24.
                lsFolioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                    folio_unico,
                    int.Parse(ConexionT24Mw.CambiarFechaAJulianaT24(lsFechaEjecucion)),
                    tipoOperacion);

                if (string.Equals((lsFolioPropuesto ?? string.Empty).Trim(), string.Empty, StringComparison.Ordinal))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error al obtener el folio propuesto de T24");

                // Actualizo el maestro general.
                bool actualizado = await _spidH2hDao.ActualizarPlanTrabajoMaestro(new SpidH2hActualizarPlanTrabajoMaestroDto
                {
                    Folio = ConversionHelper.ParseLongRequerido(folio, "Folio"),
                    TipoOperacion = tipoOperacion,
                    FolioUnico = folio_unico,
                    FechaEjecucion = datosOperacion?.FechaEjecucion
                });

                if (!actualizado)
                    throw new Exception($"Error actualizando tb_srv_plan_trabajo_maestro para el folio {folio}.");

                if (!await ActualizarDireccion(strCDL, ConversionHelper.ParseLongRequerido(folio, "Folio"), ct))
                    throw new Exception($"No se encontraron datos de Insight para el folio {folio}");

                // Validamos si es un cliente SPID para operar.
                lsDatosCliente = string.Empty;
                lsErrorDetencion = string.Empty;
                EsClienteSpidValidoResultado resltado = await _vwT24.EsCuentaSpidValida(lsCuenta, lsErrorDetencion);
                lbCuentaSPID_Valida = resltado.EsClienteValido;
                lsErrorDetencion = resltado?.Mensaje ?? string.Empty;

                if (!lbCuentaSPID_Valida)
                {
                    await _spidH2hDao.MarcarPlanTrabajoConError(new SpidH2hMarcarPlanTrabajoConErrorDto
                    {
                    Folio = ConversionHelper.ParseLongRequerido(folio, "Folio"),
                        TipoOperacion = tipoOperacion,
                        ErrorDescripcion = lsErrorDetencion
                    });
                }

                lbProcesado = lbCuentaSPID_Valida;

                // Proceso para Generar la firma digital.
                if (!await _comunSPID.GeneraFirmaDigitalSPID("tb_tombin_nf_spid_h2h", folio, lsFolioPropuesto))
                    throw new Exception($"Se presentó un error al generar la firma digital del: {folio}");

                if (lbProcesado)
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

                    // Verificamos la última operación no realizada (debe ser a 'T24_REGISTRO').
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

                    return true;
                }
                else
                {
                    return false;
                }
            }

            return false;
        }

        private async Task<bool> ActualizarDireccion(string CDL, long Folio, CancellationToken ct)
        {
            string strDireccion = string.Empty;
            string strCP = string.Empty;

            try
            {
                DateTime fecha = DateTime.Now.Date;
                bool blnFecha = false;
                int diasRetroceso = 0;
                const int maxDiasRetroceso = 366;

                while (!blnFecha)
                {
                    ct.ThrowIfCancellationRequested();

                    if (diasRetroceso >= maxDiasRetroceso)
                        throw new Exception($"No se encontró un día hábil dentro de los últimos {maxDiasRetroceso} días para el folio {Folio}.");

                    fecha = fecha.AddDays(-1);
                    diasRetroceso++;

                    blnFecha = await ValidarDiaHabilSiguiente(fecha, Folio);

                    if (blnFecha || fecha.DayOfWeek == DayOfWeek.Saturday || fecha.DayOfWeek == DayOfWeek.Sunday)
                        blnFecha = false;
                    else
                        blnFecha = true;
                }

                Config.EscribirLog($"INFO|{tipoOperacion}.ActualizarDireccion|Consultando dirección en VistasT24.");

                string customerCode = (Convert.ToString(CDL) ?? string.Empty).TrimStart('0');
                SpidH2hCustomerEdoCtaDto datosInsight = await _spidH2hVistasT24Dao.ObtenerCustomerEdoCta(customerCode, fecha);
                if (datosInsight != null)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.ActualizarDireccion|Se encontró la dirección en Insight del folio {Folio}");
                    strDireccion = DameTexto(datosInsight.P1_CALLE) + " " + DameTexto(datosInsight.P1_EXTERIOR) + " " + DameTexto(datosInsight.P1_INTERIOR);
                    strCP = DameTexto(datosInsight.P1_CODIGO_POSTAL);
                }

                if (string.Equals((strDireccion ?? string.Empty).Trim(), string.Empty, StringComparison.Ordinal))
                {
                    Config.EscribirLog($"ERROR|{tipoOperacion}.ActualizarDireccion|No se encontraron datos de Insight.");
                    return false;
                }

                bool actualizado = await _spidH2hDao.ActualizarDireccion(new SpidH2hActualizarDireccionDto
                {
                    Folio = Folio,
                    Address = strDireccion,
                    ZipCode = strCP
                });

                if (!actualizado)
                    return false;

                Config.EscribirLog($"INFO|{tipoOperacion}.ActualizarDireccion|Se actualizaron los datos de direccion de la operacion {Folio}");
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|{tipoOperacion}.ActualizarDireccion|Error al obtener los datos de Insight. - {ex}");
                throw;
            }

            return true;
        }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public async Task<bool> ValidarDiaHabilSiguiente(DateTime Fecha, long folio)
        {
            bool blnAux = false;

            try
            {
                SpidH2hDiaNoHabilDto diaNoHabil = await _spidH2hDao.ObtenerDiaNoHabil(Fecha);
                if (diaNoHabil != null)
                {
                    if (!string.Equals((diaNoHabil.TipoCcy ?? string.Empty).Trim(), string.Empty, StringComparison.Ordinal))
                        blnAux = true;
                    else
                        blnAux = false;
                }
                else
                {
                    blnAux = false;
                }
            }
            catch (Exception)
            {
                throw new Exception($"Error al validar el día habil siguiente con el folio: {folio}");
            }

            return blnAux;
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
    using Aplicacion.DTOs.TAX;
    using Aplicacion.Interfaces.TAX;
    using Aplicacion.Modelos;
    using MUFG_T24.Comunes;
    using MUFG_T24.Comunes.Helpers;
    using MUFG_T24.Interfaces_utilidades;
    using MUFG_T24.Modelos;
    using MUFG_T24.Repositorios;
    using System;
    using System.Globalization;
    using System.Threading.Tasks;
    using static MUFG_T24.Comunes.Comunes;
    /// <summary>
    /// Representa TAX.
    /// </summary>

    public class TAX
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ITAXCmsDao _taxCmsDao;
        private readonly ITAXTomiDao _taxTomiDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "TAX";
        private string folioUnico = string.Empty;

        /// <summary>
        /// Constructor.
        /// </summary>
        public TAX(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ITAXCmsDao taxCmsDao,
            ITAXTomiDao taxTomiDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _taxCmsDao = taxCmsDao;
            _taxTomiDao = taxTomiDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación TAX del servicio Lectura.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio)
        {
            long folioNumerico = Convert.ToInt64(folio, CultureInfo.InvariantCulture);

            TaxFormatoCmsDto formatoCms;
            object[] valoresFormato = new object[38];

            string fechaEjecucionTexto = string.Empty;
            string moneda = string.Empty;
            decimal importe = 0m;
            bool conEntradaSalida = true;
            bool procesado;
            bool resultado;

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

            if (string.Equals(_configuracion.AliasProceso, "", StringComparison.Ordinal))
                return false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            // Datos del folio en CMS.
            formatoCms = await _taxCmsDao.ObtenerFormatoTaxPorFolio(folioNumerico);
            if (formatoCms is null)
                formatoCms = new TaxFormatoCmsDto();
            /// <summary>
            /// Ejecuta la operaciÃ³n CargarValoresFormato.
            /// </summary>

            CargarValoresFormato(formatoCms, valoresFormato, folio);

            // Fecha ejecución.
            if (valoresFormato[13] != null && valoresFormato[13] != DBNull.Value)
                fechaEjecucionTexto = DameTexto(valoresFormato[13]);

            // Moneda.
            if (valoresFormato[4] != null && valoresFormato[4] != DBNull.Value)
                moneda = _vwT24.ValidaMoneda(DameTexto(valoresFormato[4]));

            // Importe.
            importe = formatoCms.OperatingAmount;

            decimal montoCapturado = ConversionHelper.ConvertirDecimalComparable(valoresFormato[5]);
            decimal montoEnviado = ConversionHelper.ConvertirDecimalComparable(valoresFormato[18]);
            decimal montoOperacion = ConversionHelper.ConvertirDecimalComparable(valoresFormato[35]);

            if (montoCapturado != montoEnviado || montoCapturado != montoOperacion)
            {
                throw new Exception(
                    $"Hay diferencias entre los montos de la operación: " +
                    $"MontoOperacion={montoOperacion}, " +
                    $"MontoCapturado={montoCapturado}, " +
                    $"MontoEnviado={montoEnviado}.");
            }

            Config.EscribirLog($"INFO|TAX.procesaOperacion|Veamos que trae la fecha{valoresFormato[13]}-----{fechaEjecucionTexto}");

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(fechaEjecucionTexto, CultureInfo.InvariantCulture);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception(
                    $"Se desea realizar una operación al {fechaEjecucionTexto} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // Genero el folio único.
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucionTexto),
                folio);

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string subtipoOperacion = string.Empty;

                Config.EscribirLog("INFO|TAX.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                await _taxTomiDao.ActualizarDatosGeneralesMaestro(
                    folioNumerico,
                    folioUnico,
                    Convert.ToDecimal(fechaEjecucionTexto, CultureInfo.InvariantCulture),
                    moneda,
                    importe,
                    ConversionHelper.ConvertirNullableInt(valoresFormato[29]),
                    DameTexto(valoresFormato[1]),
                    DameTexto(valoresFormato[30]),
                    subtipoOperacion);

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
            }

            // Lectura del CMS e inserción a TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CMS_A_TOMI", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TAX.procesaOperacion|A insertar datos de CMS a TOMI.");

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
                resultado = await CMS_A_TOMI(folioNumerico, valoresFormato);

                if (resultado)
                {
                    Config.EscribirLog("INFO|TAX.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                    procesado = true;
                }
                else
                {
                    // Posiblemente se restauró el servicio.
                    int numRegs = await _taxTomiDao.ContarTaxPayPorFolio(folioNumerico);
                    if (numRegs > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|TAX.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

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
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CMS_A_TOMI con folio: {folio}");
                }
            }

            // Registro de la Entrada/Salida.
            if (string.Equals(_configuracion.AliasProceso, "TOMI_ENTRADAS/SALIDAS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TAX.procesaOperacion|A insertar datos de Entrada/Salida en TOMI.");

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
                    resultado = await TOMI_ENTRADA_SALIDA(folioNumerico);

                    if (resultado)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|TAX.procesaOperacion|Se insertó correctamente el folio en base de Entrada/Salida.");
                    }
                    else
                    {
                        // Posiblemente se restauró el servicio.
                        int numRegs = await _taxTomiDao.ContarEntradaSalidaPorReferencia(folioUnico);
                        if (numRegs > 0)
                        {
                            procesado = true;
                            Config.EscribirLog("INFO|TAX.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                        }
                    }
                }
                else
                {
                    procesado = true;
                    Config.EscribirLog("INFO|TAX.procesaOperacion|La operación no debe generar Entrada/Salida en TOMI).");
                }

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
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro TOMI_ENTRADAS/SALIDAS con folio: {folio}");
                }
            }

            // Actualización del estatus del CMS y TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|TAX.procesaOperacion|A actualizar estatus en CMS y TOMI.");

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

                if (string.Equals(fechaEjecucionTexto, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                {
                    DateTime fechaConfirma = DateTime.ParseExact(
                        DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                        "yyyy/MM/dd HH:mm",
                        CultureInfo.InvariantCulture);

                    await _taxCmsDao.ActualizarFormatoAceptado(folioNumerico);
                    await _taxTomiDao.ActualizarTaxPayAceptado(folioNumerico, "OPER CMS", fechaConfirma);

                    Config.EscribirLog("INFO|TAX.procesaOperacion|Operación fecha valor hoy.");
                }
                else if (string.Compare(fechaEjecucionTexto, _configuracion.StrFechaSistema, StringComparison.Ordinal) < 0)
                {
                    await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio, CultureInfo.InvariantCulture));
                    throw new Exception(
                        $"Se desea realizar una operación al {fechaEjecucionTexto} " +
                        $"cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                }
                else
                {
                    DateTime fechaConfirma = DateTime.ParseExact(
                        DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                        "yyyy/MM/dd HH:mm",
                        CultureInfo.InvariantCulture);

                    await _taxCmsDao.ActualizarFormatoRecibidoPendienteEjecucion(folioNumerico);
                    await _taxTomiDao.ActualizarTaxPayFechaFutura(folioNumerico, "OPER CMS", fechaConfirma);

                    Config.EscribirLog("INFO|TAX.procesaOperacion|Operación fecha valor fútura.");
                }

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
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CAMBIO_ESTATUS con folio: {folio}");
                }
            }

            // Conciliación.
            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                int regsCms = 1;
                int regsTomi = 0;
                int regsEntSal = 0;

                string mensajeError = string.Empty;

                string fechaEjecucionTomi = string.Empty;
                string monedaTomi = string.Empty;
                decimal importeTomi = 0m;

                string fechaEjecucionEntSal = string.Empty;
                string monedaEntSal = string.Empty;
                decimal importeEntSal = 0m;

                Config.EscribirLog("INFO|TAX.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                TaxPayConciliacionDto taxPay = await _taxTomiDao.ObtenerTaxPayParaConciliacion(folioNumerico);
                if (taxPay != null)
                {
                    regsTomi = 1;
                    fechaEjecucionTomi = taxPay.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                    monedaTomi = _vwT24.ValidaMoneda(taxPay.T24Currency);
                    importeTomi = taxPay.T24Amount ?? 0m;
                }

                if (conEntradaSalida)
                {
                    TaxEntradaSalidaConciliacionDto entradaSalida = await _taxTomiDao.ObtenerEntradaSalidaParaConciliacion(folioUnico);
                    if (entradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = entradaSalida.EntFechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                        monedaEntSal = _vwT24.ValidaMoneda(entradaSalida.EntMonedaCuenta);
                        importeEntSal = Convert.ToDecimal(entradaSalida.EntImporte);
                    }
                }

                mensajeError = string.Empty;

                if (regsCms == 1 &&
                    regsTomi == 1 &&
                    ((conEntradaSalida && regsEntSal == 1) || !conEntradaSalida))
                {
                    if (!string.Equals(fechaEjecucionTexto, fechaEjecucionTomi, StringComparison.Ordinal))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en la fecha de ejecución. CMS='{fechaEjecucionTexto}', TOMI='{fechaEjecucionTomi}'.";
                    }

                    if (!string.Equals(moneda, monedaTomi, StringComparison.Ordinal))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en la monedas. CMS='{moneda}', TOMI='{monedaTomi}'.";
                    }

                    if (importe != importeTomi)
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en el importe. CMS='{importe}', TOMI='{importeTomi}'.";
                    }

                    if (conEntradaSalida)
                    {
                        if (!string.Equals(fechaEjecucionTexto, fechaEjecucionEntSal, StringComparison.Ordinal))
                        {
                            mensajeError += Environment.NewLine +
                                $" - Diferencia en la fecha de ejecución de Entrada/Salida. CMS='{fechaEjecucionTexto}', TOMI='{fechaEjecucionEntSal}'.";
                        }

                        if (!string.Equals(moneda, monedaEntSal, StringComparison.Ordinal))
                        {
                            mensajeError += Environment.NewLine +
                                $" - Diferencia en la monedas de Entrada/Salida. CMS='{moneda}', TOMI='{monedaEntSal}'.";
                        }

                        if (importe != importeEntSal)
                        {
                            mensajeError += Environment.NewLine +
                                $" - Diferencia en el importe de Entrada/Salida. CMS='{importe}', Entrada/Salida='{importeEntSal}'.";
                        }
                    }

                    procesado = true;
                }
                else
                {
                    procesado = false;
                    mensajeError += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                }

                if (!string.Equals(mensajeError, "", StringComparison.Ordinal))
                {
                    procesado = false;
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(mensajeError);
                }

                if (procesado)
                {
                    Config.EscribirLog("INFO|TAX.procesaOperacion|La operación está correctamente conciliada.");

                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    _configuracion.AliasProceso = "";
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro CONCILIAR con folio: {folio}");
                }
            }

            return true;
        }

        /// <summary>
        /// Inserta registro en la base de datos de TOMI con datos provenientes del CMS.
        /// </summary>
        private async Task<bool> CMS_A_TOMI(long folio, object[] valoresFormato)
        {
            try
            {
                bool resultado = await _taxTomiDao.InsertarFormatoTaxPay(
                    DameTexto(valoresFormato[27]),
                    Convert.ToInt64(valoresFormato[28], CultureInfo.InvariantCulture),
                    DameTexto(valoresFormato[29]),
                    DameTexto(valoresFormato[1]),
                    "PEND",
                    DameTexto(valoresFormato[32]),
                    DameTexto(valoresFormato[34]),
                    ConversionHelper.ConvertirNullableDateTimeInvariant(valoresFormato[31]),
                    ConversionHelper.ConvertirNullableDateTimeInvariant(valoresFormato[33]),
                    Convert.ToInt32(valoresFormato[13], CultureInfo.InvariantCulture),
                    DameTexto(valoresFormato[10]),
                    DameTexto(valoresFormato[19]),
                    DameTexto(valoresFormato[4]),
                    DameTexto(valoresFormato[20]),
                    Convert.ToDouble(valoresFormato[35], CultureInfo.InvariantCulture),
                    DameTexto(valoresFormato[15]),
                    DameTexto(valoresFormato[30]),
                    Convert.ToDouble(valoresFormato[18], CultureInfo.InvariantCulture),
                    folioUnico,
                    ConversionHelper.ConvertirNullableLong(valoresFormato[37]),
                    DameTexto(valoresFormato[8]),
                    valoresFormato[36].ToString());

                Config.EscribirLog($"INFO|TAX.CMS_A_TOMI|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|TAX.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
                throw;
            }
        }

        /// <summary>
        /// Inserta registro de Entradas/Salidas en TOMI.
        /// </summary>
        private async Task<bool> TOMI_ENTRADA_SALIDA(long folio)
        {
            try
            {
                TaxPayEntradaSalidaDto taxPay = await _taxTomiDao.ObtenerTaxPayParaEntradaSalida(folio);
                if (taxPay is null)
                    throw new Exception($"No se encontraron datos TAX Pay para generar Entrada/Salida. Folio={folio}.");

                string monedaCuenta = _vwT24.ValidaMoneda(taxPay.NfMonedaCuenta);
                DateTime fechaCaptura = ConversionHelper.TextoToFechaInvariantNullable(_configuracion.StrFechaSistema) ?? DateTime.Now;
                DateTime fechaValor = ConversionHelper.TextoToFechaInvariantNullable(taxPay.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture)) ?? DateTime.Now;

                bool resultado = await _taxTomiDao.InsertarEntradaSalidaTax(
                    taxPay.NfFolioUnico,
                    Convert.ToInt32(taxPay.T24CustomerCode, CultureInfo.InvariantCulture),
                    Convert.ToDouble(taxPay.T24CustomerCode, CultureInfo.InvariantCulture),
                    taxPay.T24CompanyName,
                    218,
                    "Salida  (TTS)",
                    "D",
                    "Doméstica",
                    Convert.ToDouble(taxPay.T24AcctNo, CultureInfo.InvariantCulture),
                    taxPay.NfNombreCuenta,
                    monedaCuenta,
                    800,
                    "TRF ENVIADA MXN TAX",
                    Convert.ToDouble(taxPay.T24Amount),
                    0,
                    taxPay.T24Concept,
                    fechaCaptura,
                    fechaValor,
                    DateTime.Now.ToLongTimeString(),
                    "JEAI",
                    0,
                    "A",
                    0);

                Config.EscribirLog($"INFO|TAX.TOMI_ENTRADA_SALIDA|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|TAX.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}. Trace: {ex}");
                throw;
            }
        }

        /// <summary>
        /// Carga el arreglo legacy de valoresFormato con posiciones equivalentes al VB.
        /// </summary>
        private static void CargarValoresFormato(TaxFormatoCmsDto formatoCms, object[] valoresFormato, string folio)
        {
            valoresFormato[27] = "TAX";
            valoresFormato[28] = formatoCms.IdFormat;
            valoresFormato[29] = formatoCms.Cdl;
            valoresFormato[30] = formatoCms.IdAccount;
            valoresFormato[31] = formatoCms.CaptureDate;
            valoresFormato[32] = formatoCms.IdUserCapture;
            valoresFormato[33] = formatoCms.AuthorizationDate;
            valoresFormato[34] = formatoCms.IdAuthorizationUser;
            valoresFormato[35] = formatoCms.OperatingAmount;
            valoresFormato[36] = formatoCms.IdTemplate;
            valoresFormato[37] = formatoCms.IdTemplateFormat;

            var parseado = ContenidoFormatoParser.Parse(formatoCms.ContentFormat, folio);
            ContenidoFormatoParser.ValidarMinimoIndice(parseado, folio, 27);

            for (int idx = 0; idx <= 26; idx++)
            {
                valoresFormato[idx] = ContenidoFormatoParser.ObtenerValorPosicional(parseado, folio, idx);
            }
        }


    }
}

using Aplicacion.DTOs.TEF;
using Aplicacion.Interfaces.TEF;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Globalization;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa TEF.
    /// </summary>
    public class TEF
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly ITEFTomiDao _tefDao;
        private readonly ITEFCmsDao _tefCmsDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "TEF";
        private string folioUnico = string.Empty;

        /// <summary>
        /// Constructor.
        /// </summary>
        public TEF(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            ITEFTomiDao tefDao,
            ITEFCmsDao tefCmsDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _tefDao = tefDao;
            _tefCmsDao = tefCmsDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación TEF.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio)
        {
            object[] valoresFormato = new object[34];
            string fechaEjecucion = string.Empty;
            string moneda = string.Empty;
            decimal importe = 0m;
            bool conForex = false;
            bool fechaFutura;
            bool forexCancelado = false;
            string folioForex = string.Empty;
            bool conEntradaSalida = true;
            bool procesado;
            bool resultado;

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

            fechaEjecucion = string.Empty;
            moneda = string.Empty;
            importe = 0m;
            folioForex = string.Empty;
            conForex = false;
            forexCancelado = false;
            conEntradaSalida = true;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Aperturando conexión a base de datos del CMS.");
            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo los datos de lo operación en base CMS.");

            TefFormatoCmsDto datosCms = await _tefCmsDao.ObtenerFormatoPorFolio(folio);
            if (datosCms is null)
                datosCms = new TefFormatoCmsDto();

            valoresFormato[19] = "TEF";
            valoresFormato[20] = datosCms.IdFormato;
            valoresFormato[21] = datosCms.Cdl;
            valoresFormato[22] = datosCms.IdCuenta;
            valoresFormato[23] = datosCms.FechaCaptura;
            valoresFormato[24] = datosCms.IdUsuarioCaptura;
            valoresFormato[25] = datosCms.FechaAutorizacion;
