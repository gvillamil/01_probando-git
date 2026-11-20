
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

                FxiConciliacionNfFxiTomiDto datosTomi = await _fxiDao.ObtenerDatosConciliacionFormatoFxi(
                    ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                    conForex && forexCancelado);

                if (datosTomi != null)
                {
                    regsTomi = 1;
                    folioForexTomi = datosTomi.NF_FOREX_RELACIONADO ?? string.Empty;
                    fechaEjecucionTomi = datosTomi.NF_FECHA_EJECUCION?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                    monedaTomi = _vwT24.ValidaMoneda(datosTomi.T24_DEBIT_CURRENCY);
                    importeTomi = datosTomi.T24_DEBIT_AMOUNT ?? 0m;
                }

                // Entrada/Salida.
                fechaEjecucionEntSal = string.Empty;
                monedaEntSal = string.Empty;
                importeEntSal = 0m;

                if (conEntradaSalida)
                {
                    FxiEntradaSalidaConciliacionTomiDto entradaSalida =
                        await _fxiDao.ObtenerDatosConciliacionEntradaSalida(folioUnico);

                    if (entradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = entradaSalida.ent_fechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                        monedaEntSal = _vwT24.ValidaMoneda(entradaSalida.ent_moneda_cuenta);
                        importeEntSal = entradaSalida.ent_importe ?? 0m;
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
                    // Fecha valor.
                    if (fechaFutura)
                        await _fxiDao.ActualizarMaestroFechaFutura(ConversionHelper.ConvertirLongDirecto(folio, "Folio"), tipoOperacion);

                    Config.EscribirLog("INFO|FXI.procesaOperacion|La operación está correctamente conciliada.");
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
        /// Insertar los datos del CMS a TOMI.
        /// </summary>
        /// <param name="folio"></param>
        /// <param name="valoresFormato"></param>
        /// <returns></returns>
        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato)
        {
            try
            {
                bool resultado = await _fxiDao.InsertarFormatoFxi(new FxiInsertarFormatoFxiTomiDto
                {
                    NF_FOLIO_UNICO = folioUnico,
                    NF_TIPO = Convert.ToString(valoresFormato[29]),
                    NF_FOLIO = ConversionHelper.ConvertirLongDirecto(valoresFormato[30], "NF_FOLIO"),
                    T24_CUSTOMER_CODE = Convert.ToString(valoresFormato[31]),
                    T24_DEBIT_ACCT_NO = Convert.ToString(valoresFormato[32]),
                    NF_ESTATUS = "PEND",
                    NF_USUARIO_CAPTURA = Convert.ToString(valoresFormato[34]),
                    NF_USUARIO_AUTORIZA = Convert.ToString(valoresFormato[36]),
                    NF_FECHA_CAPTURA = ConversionHelper.ConvertirDateTimeDirecto(valoresFormato[33], "NF_FECHA_CAPTURA"),
                    NF_FECHA_AUTORIZA = ConversionHelper.ConvertirNullableDateTimeDirecto(valoresFormato[35]),
                    T24_DEBIT_AMOUNT = ConversionHelper.ConvertirDecimalDirecto(valoresFormato[37], "T24_DEBIT_AMOUNT"),
                    T24_SHORT_NAME = TextoBESS(Convert.ToString(valoresFormato[1])),
                    T24_DEBIT_CURRENCY = Convert.ToString(valoresFormato[3]),
                    NF_FOREX_RELACIONADO = Convert.ToString(valoresFormato[6]),
                    NF_MONTO_ENVIAR = ConversionHelper.ConvertirDecimalDirecto(valoresFormato[8], "NF_MONTO_ENVIAR"),
                    T24_DEBIT_THEIR_REF = TextoBESS(Convert.ToString(valoresFormato[9])),
                    NF_TIPO_CUENTA = TextoBESS(Convert.ToString(valoresFormato[10])),
                    NF_PAYMENT = TextoBESS(Convert.ToString(valoresFormato[11])),
                    NF_CHARGE = TextoBESS(Convert.ToString(valoresFormato[12])),
                    NF_FECHA_EJECUCION = ConversionHelper.ConvertirIntDirecto(valoresFormato[13], "NF_FECHA_EJECUCION"),
                    T24_BEN_CUST_NAME = TextoBESS(Convert.ToString(valoresFormato[14])),
                    NF_FXD_COMMENT = TextoBESS(Convert.ToString(valoresFormato[15])),
                    NF_RFC_BENEFICIARIO = TextoBESS(Convert.ToString(valoresFormato[16])),
                    NF_TASA_RETENCION = Convert.ToString(valoresFormato[17]),
                    NF_BANCO_BENEF = TextoBESS(Convert.ToString(valoresFormato[18])),
                    NF_SWIFT_BENEFICIARIO = Convert.ToString(valoresFormato[19]),
                    T24_BEN_ACCT_NO = Convert.ToString(valoresFormato[21]),
                    NF_BANCO_CORRESPONSAL = TextoBESS(Convert.ToString(valoresFormato[23])),
                    NF_SWIFT_CORRESPONSAL = Convert.ToString(valoresFormato[24]),
                    NF_CUENTA_CORRESPONSAL = TextoBESS(Convert.ToString(valoresFormato[25])),
                    NF_MENSAJE = TextoBESS(Convert.ToString(valoresFormato[27])),
                    NF_COUNTRY = Convert.ToString(valoresFormato[28]),
                    ID_PLANTILLA = Convert.ToString(valoresFormato[38]),
                    ID_FORMATO_PLANTILLA = ConversionHelper.ConvertirNullableLongDirecto(valoresFormato[39]),
                    COUNTRY_BEN = Convert.ToString(valoresFormato[40]),
                    city_ben = Convert.ToString(valoresFormato[41]),
                    street_ben = Convert.ToString(valoresFormato[42]),
                    colony_ben = Convert.ToString(valoresFormato[43]),
                    building_number_ben = Convert.ToString(valoresFormato[44]),
                    post_code_ben = Convert.ToString(valoresFormato[45])
                });

                Config.EscribirLog($"INFO|FXI.CMS_A_TOMI|Salida={(resultado ? 0 : 1)}.");
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|FXI.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        /// <summary>
        /// Insertar los datos del CMS a TOMI para el formato FXE.
        /// </summary>
        /// <param name="instruction"></param>
        /// <returns></returns>
        private async Task<bool> CMS_A_TOMI_FXE(SpecialInternationalDto instruction)
        {
            try
            {
                bool resultado = await _fxiDao.InsertarFormatoFxe(new FxiInsertarFormatoFxiTomiDto
                {
                    NF_FOLIO_UNICO = folioUnico,
                    NF_TIPO = instruction.OperationType,
                    NF_FOLIO = Convert.ToInt64(instruction.OperationFolio),
                    T24_CUSTOMER_CODE = instruction.CustomerCdl,
                    T24_DEBIT_ACCT_NO = instruction.DebitAccount,
                    NF_ESTATUS = "PEND",
                    NF_USUARIO_CAPTURA = instruction.CapturingUser,
                    NF_USUARIO_AUTORIZA = instruction.AuthorizingUser,
                    NF_FECHA_CAPTURA = instruction.CapturingDate,
                    NF_FECHA_AUTORIZA = instruction.AuthorizingDate,
                    T24_DEBIT_AMOUNT = Convert.ToDecimal(instruction.DebitAmount),
                    T24_SHORT_NAME = instruction.CustomerShortName,
                    T24_DEBIT_CURRENCY = instruction.DebitCurrency,
                    NF_FOREX_RELACIONADO = instruction.RelatedForex,
                    NF_MONTO_ENVIAR = Convert.ToDecimal(instruction.SettledAmount),
                    T24_DEBIT_THEIR_REF = instruction.DebitTheirRef,
                    NF_TIPO_CUENTA = instruction.AccountType,
                    NF_PAYMENT = instruction.PaymentType,
                    NF_CHARGE = instruction.ChargeType,
                    NF_FECHA_EJECUCION = Convert.ToInt32(instruction.ValueDate),
                    T24_BEN_CUST_NAME = instruction.BeneficiaryName,
                    NF_FXD_COMMENT = instruction.Comment,
                    NF_RFC_BENEFICIARIO = instruction.BeneficiaryTaxId,
                    NF_RFC_ORDENANTE = instruction.OrderingCustomerTaxId,
                    NF_TASA_RETENCION = instruction.RetentionRate,
                    NF_BANCO_BENEF = instruction.BeneficiaryBankName,
                    NF_SWIFT_BENEFICIARIO = instruction.BeneficiaryBankBic,
                    T24_BEN_ACCT_NO = instruction.BeneficiaryAccount,
                    NF_BANCO_CORRESPONSAL = instruction.CorrespondingBankName,
                    NF_SWIFT_CORRESPONSAL = instruction.CorrespondingBankBic,
                    NF_CUENTA_CORRESPONSAL = instruction.CorrespondingBankAccount,
                    NF_MENSAJE = instruction.Message,
                    NF_COUNTRY = instruction.FormatCountry,
                    ID_PLANTILLA = instruction.TemplateId,
                    ID_FORMATO_PLANTILLA = Convert.ToInt64(instruction.FormatTemplateId),
                    COUNTRY_BEN = instruction.BeneficiaryCountry,
                    city_ben = instruction.BeneficiaryCity,
                    street_ben = instruction.BeneficiaryStreet,
                    colony_ben = instruction.BeneficiarySuburb,
                    building_number_ben = instruction.BeneficiaryBuildingNumber,
                    post_code_ben = instruction.BeneficiaryZipCode
                });

                Config.EscribirLog($"INFO|FXI.CMS_A_TOMI|Salida={(resultado ? 0 : 1)}.");
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|FXI.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        /// <summary>
        /// Insertar los datos de Entrada/Salida en TOMI.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        private async Task<bool> TOMI_ENTRADA_SALIDA(string folio)
        {
            try
            {
                List<FxiTombinNfFxiTomiDto> registros = await _fxiDao.ObtenerFormatosFxi(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                FxiInsertarEntradaSalidaTomiDto dto = null;

                foreach (FxiTombinNfFxiTomiDto datos in registros)
                {
                    dto = new FxiInsertarEntradaSalidaTomiDto
                    {
                        Referencia = datos.NF_FOLIO_UNICO,
                        Num_Cliente = ConversionHelper.ConvertirDecimalDirecto(datos.T24_CUSTOMER_CODE, "T24_CUSTOMER_CODE"),
                        CDL = ConversionHelper.ConvertirDecimalDirecto(datos.T24_CUSTOMER_CODE, "T24_CUSTOMER_CODE"),
                        Ordenante = datos.T24_COMPANY_NAME,
                        Operacion = 218m,
                        OperacionDesc = "Salida  (TTS)",
                        Tipo = "I",
                        TipoDesc = "Internacional",
                        Importe = datos.T24_DEBIT_AMOUNT ?? 0m,
                        Saldo = 0m,
                        Comentarios = datos.NF_MENSAJE,
                        Fecha_Captura = ConversionHelper.ParseFechaYyyyMMddExacta(_configuracion.StrFechaSistema),
                        FechaValor = ConversionHelper.ParseFechaYyyyMMddExacta(Convert.ToString(datos.NF_FECHA_EJECUCION)),
                        Hora = DateTime.Now.ToLongTimeString(),
                        Capturista = "JEAI",
                        Fad = 0m,
                        Referencia_Control = "A",
                        Referencia_Control_Cont = 0
                    };

                    if (!string.Equals(datos.T24_DEBIT_CURRENCY, _vwT24.MonedaDefaultMN, StringComparison.Ordinal))
                    {
                        if (string.Equals(datos.NF_FOREX_RELACIONADO, string.Empty, StringComparison.Ordinal))
                        {
                            dto.Cuenta = ConversionHelper.ConvertirDecimalDirecto(datos.T24_DEBIT_ACCT_NO, "T24_DEBIT_ACCT_NO");
                            dto.Nombre = datos.NF_NOMBRE_CUENTA;
                            dto.CuentaMoneda = datos.NF_MONEDA_CUENTA;
                            dto.Via = 810m;
                            dto.ViaDesc = "TRF ENVIADA USD BTMNY";
                        }
                        else
                        {
                            /// <summary>
                            /// Ejecuta la operaciÃ³n AsignarCuentaFadPorMoneda.
                            /// </summary>
                            AsignarCuentaFadPorMoneda(dto, datos.T24_DEBIT_CURRENCY);
                        }
                    }
                    else
                    {
                        if (string.Equals(datos.NF_FOREX_RELACIONADO, "1", StringComparison.Ordinal))
                        {
                            dto.Cuenta = ConversionHelper.ConvertirDecimalDirecto(datos.T24_DEBIT_ACCT_NO, "T24_DEBIT_ACCT_NO");
                            dto.Nombre = datos.NF_NOMBRE_CUENTA;
                            dto.CuentaMoneda = datos.NF_MONEDA_CUENTA;
                            dto.Via = 810m;
                            dto.ViaDesc = "TRF ENVIADA USD BTMNY";
                        }
                        else
                        {
                            /// <summary>
                            /// Ejecuta la operaciÃ³n AsignarCuentaFadPorMoneda.
                            /// </summary>
                            AsignarCuentaFadPorMoneda(dto, datos.T24_DEBIT_CURRENCY);
                        }
                    }
                }

                if (dto == null)
                    return false;

                bool resultado = await _fxiDao.InsertarEntradaSalida(dto);
                Config.EscribirLog($"INFO|FXI.TOMI_ENTRADA_SALIDA|Salida={(resultado ? 0 : 1)}.");
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|FXI.TOMI_ENTRADA_SALIDA|Se presenta exception: {ex.Message}.Trace:{ex}");
            }

            return false;
        }


        /// <summary>
        /// Verifica si la operación puede ser cubierta por SPEI o por cuenta NOSTRO.
        /// </summary>
        /// <param name="operationType"></param>
        /// <param name="currency"></param>
        /// <param name="correspondentBank"></param>
        /// <param name="beneficiaryBank"></param>
        /// <returns></returns>
        private async Task<Aplicacion.Modelos.Result<bool>> CheckIfOperationHasSpeiCover(
            string operationType,
            string currency,
            string correspondentBank,
            string beneficiaryBank)
        {
            currency = _vwT24.ValidaMoneda(currency);

            if (!string.Equals(operationType, "FXE", StringComparison.Ordinal))
                return Aplicacion.Modelos.Result<bool>.Success(false);

            Aplicacion.Modelos.Result<string> resultNostro = await GetNostro(currency, beneficiaryBank);
            if (!resultNostro.IsSuccess)
                return Aplicacion.Modelos.Result<bool>.Failed(resultNostro.Reason);

            bool internalAccountSettlement = !string.IsNullOrWhiteSpace(resultNostro.Data);
            bool canBeSpei = string.Equals(currency, _vwT24.MonedaDefaultMN, StringComparison.Ordinal) &&
                             !correspondentBank.Contains("BOTKMXMX");

            if (internalAccountSettlement ^ canBeSpei)
                return Aplicacion.Modelos.Result<bool>.Success(canBeSpei);

            return Aplicacion.Modelos.Result<bool>.Failed("Caso no válido, es posible que se haya encontrado una vostro y la salida sea por SPEI, o lo contrario.");
        }

        /// <summary>
        /// Obtiene la cuenta NOSTRO para la moneda y BIC proporcionados.
        /// </summary>
        /// <param name="currency"></param>
        /// <param name="bic"></param>
        /// <returns></returns>
        private async Task<Aplicacion.Modelos.Result<string>> GetNostro(string currency, string bic)
        {
            if (string.IsNullOrWhiteSpace(bic))
                return Aplicacion.Modelos.Result<string>.Failed("No se suplió un BIC Beneficiario, posible error en registro del beneficiario.");

            if (decimal.TryParse(bic, out _))
                return Aplicacion.Modelos.Result<string>.Failed("El dato obtenido no es un BIC válido: " + bic);

            string bicToSearch = bic;

            if (!string.Equals(currency, _vwT24.MonedaDefaultMN, StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|" + tipoOperacion + ".procesaOperacion|GetNostro|Moneda con corresponsal, se fuerza el corresponsal.");

                switch (currency)
                {
                    case "USD":
                        bicToSearch = "CHASUS33XXX";
                        break;
                    case "EUR":
                    case "GBP":
                        bicToSearch = "CHASGB2LXXX";
                        break;
                    case "JPY":
                        bicToSearch = "CHASJPJTXXX";
                        break;
                    case "CAD":
                        bicToSearch = "CHASCATTCTS";
                        break;
                }
            }

            Config.EscribirLog("INFO|" + tipoOperacion + "procesaOperacion|GetNostro|A sanitizar el BIC: " + bicToSearch);
            bicToSearch = SanitizeBic11(bicToSearch);
            Config.EscribirLog("INFO|" + tipoOperacion + "procesaOperacion|GetNostro|BIC sanitizado: " + bicToSearch);
            Config.EscribirLog("INFO|" + tipoOperacion + "procesaOperacion|GetNostro|A buscar la cuenta de crédito para '" + bicToSearch + "' en la moneda " + currency);

            try
            {
                FxiNostroCuentaTomiDto nostro = await _fxiDao.ObtenerNostro(bicToSearch, currency);
                if (nostro == null)
                {
                    Config.EscribirLog("INFO|" + tipoOperacion + "procesaOperacion|GetNostro|No se encontró ningún registro para la consulta.");
                    return Result<string>.Success(string.Empty);
                }

                return string.IsNullOrWhiteSpace(nostro.NostroAccountNumber)
                    ? Result<string>.Success(string.Empty)
                    : Result<string>.Success(nostro.NostroAccountNumber);
            }
            catch (Exception)
            {
                Config.EscribirLog("INFO|" + tipoOperacion + "procesaOperacion|GetNostro|No se pudo generar el DataReader para la consulta.");
                return Result<string>.Failed("No fue posible hacer la consulta al cátalogo de cuentas NOSTRO.");
            }
        }

        /// <summary>
        /// Sanitiza un BIC a 11 caracteres, agregando "XXX" si es necesario.
        /// </summary>
        /// <param name="bic"></param>
        /// <returns></returns>
        private string SanitizeBic11(string bic)
        {
            if (string.IsNullOrWhiteSpace(bic))
                return string.Empty;

            if (bic.Length == 8)
                return bic + "XXX";

            if (bic.Length == 11)
            {
                if (bic.Substring(0, 3) == "SW-")
                {
                    bic = bic.Substring(3);
                    return bic + "XXX";
                }

                return bic;
            }

            if (bic.Substring(0, 3) == "SW-")
            {
                bic = bic.Substring(3);
                return bic;
            }

            return string.Empty;
        }

        /// <summary>
        /// Obtiene el RFC del cliente ordenante a partir del ID de cliente.
        /// </summary>
        /// <param name="clientId"></param>
        /// <returns></returns>
        private async Task<string> GetOrderingCustomerRfc(string clientId)
        {
            decimal clienteT24 = ConversionHelper.ConvertirDecimalDirecto(clientId, "CustomerCdl");
            FxiClienteRfcCmsDto cliente = await _fxiT24Dao.ObtenerRfcOrdenante(clienteT24);
            if (cliente == null)
            {
                Config.EscribirLog("INFO|" + tipoOperacion + "procesaOperacion|GetOrderingCustomerRfc|No se encontró ningún registro para la consulta.");
                return string.Empty;
            }

            string rfc = cliente.RFC ?? string.Empty;
            Config.EscribirLog($"INFO|{tipoOperacion}procesaOperacion|GetOrderingCustomerRfc|RFC ordenante: {rfc}");
            return rfc;
        }

        /// <summary>
        /// Asigna la cuenta FAD correspondiente según la moneda proporcionada.
        /// </summary>
        /// <param name="dto"></param>
        /// <param name="moneda"></param>
        private void AsignarCuentaFadPorMoneda(FxiInsertarEntradaSalidaTomiDto dto, string moneda)
        {
            if (string.Equals(moneda, "USD", StringComparison.Ordinal))
            {
                dto.Cuenta = 3300064m;
                dto.Nombre = "BTMM USD FAD";
                dto.CuentaMoneda = "USD";
                dto.Via = 810m;
                dto.ViaDesc = "TRF ENVIADA USD BTMNY";
            }
            else if (string.Equals(moneda, "EUR", StringComparison.Ordinal))
            {
                dto.Cuenta = 3399940m;
                dto.Nombre = "BTMM EUR FAD";
                dto.CuentaMoneda = "EUR";
                dto.Via = 830m;
                dto.ViaDesc = "TRF ENVIADA EUR";
            }
            else if (string.Equals(moneda, "JPY", StringComparison.Ordinal))
            {
                dto.Cuenta = 3399902m;
                dto.Nombre = "BTMM JPY FAD";
                dto.CuentaMoneda = "JPY";
                dto.Via = 820m;
                dto.ViaDesc = "TRF ENVIADA JPY";
            }
            else if (string.Equals(moneda, "HKD", StringComparison.Ordinal))
            {
                dto.Cuenta = 3300420m;
                dto.Nombre = "BTMM HKD FAD";
                dto.CuentaMoneda = "HKD";
                dto.Via = 810m;
                dto.ViaDesc = "TRF ENVIADA USD BTMNY";
            }
            else if (string.Equals(moneda, "CAD", StringComparison.Ordinal))
            {
                dto.Cuenta = 3399903m;
                dto.Nombre = "BTMM CAD FAD";
                dto.CuentaMoneda = "CAD";
                dto.Via = 810m;
                dto.ViaDesc = "TRF ENVIADA USD BTMNY";
            }
            else if (string.Equals(moneda, "SGD", StringComparison.Ordinal))
            {
                dto.Cuenta = 3300153m;
                dto.Nombre = "BTMM SGD FAD";
                dto.CuentaMoneda = "SGD";
                dto.Via = 810m;
                dto.ViaDesc = "TRF ENVIADA USD BTMNY";
            }
            else if (string.Equals(moneda, "GBP", StringComparison.Ordinal))
            {
                dto.Cuenta = 3300404m;
                dto.Nombre = "BTMM GBP FAD";
                dto.CuentaMoneda = "GBP";
                dto.Via = 810m;
                dto.ViaDesc = "TRF ENVIADA USD BTMNY";
            }
            else if (string.Equals(moneda, "AUD", StringComparison.Ordinal))
            {
                dto.Cuenta = 3300129m;
                dto.Nombre = "BTMM AUD FAD";
                dto.CuentaMoneda = "AUD";
                dto.Via = 810m;
                dto.ViaDesc = "TRF ENVIADA USD BTMNY";
            }
        }

        /// <summary>
        /// Compara los montos de origen y destino para verificar si son iguales.
        /// </summary>
        /// <param name="origen"></param>
        /// <param name="destino"></param>
        /// <returns></returns>
        private bool CompararMontos(object origen, object destino)
        {
            return ConversionHelper.ConvertirDecimalDirecto(origen, "MontoOrigen") == ConversionHelper.ConvertirDecimalDirecto(destino, "MontoDestino");
        }

    }

}

using Aplicacion.Modelos;
using Infraestructura.Repositorio.FXIGPH;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    public class FXIGPH
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IFXI_GPHDao _fxiGphDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "FXI_GPH";

        private string folioUnico = string.Empty;

        public FXIGPH(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IFXI_GPHDao fxiGphDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _fxiGphDao = fxiGphDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación FXI_GPH para un folio específico.
        /// </summary>
        /// <param name="folio"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;

            long folioArchivo = 0;
            string nombreArchivo = string.Empty;
            string idOperacion = string.Empty;

            bool procesado;

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

            // DATOS_GRAL
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro.");

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

                var datos = await _fxiGphDao.ObtenerDatosBasicosPorFolio(folio);

                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString() ?? string.Empty;
                    nombreArchivo = datos.NfNombreArchivoGph ?? string.Empty;
                    idOperacion = datos.NfPmtInfId ?? string.Empty;
                    folioArchivo = datos.NfIdArchivo ?? 0;
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

                    await _fxiGphDao.ActualizarFolioUnico(folio, folioUnico);
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                await _fxiGphDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    _vwT24.MonedaDefaultMN);

                procesado = true;

                if (string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) > 0)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Es fecha futura.");

                    await _fxiGphDao.MarcarFechaFutura(folio, tipoOperacion);

                    await _fxiGphDao.InsertarDetalleOperacionGph(
                        nombreArchivo,
                        folioArchivo,
                        idOperacion,
                        tipoOperacion,
                        "ACEPTADA",
                        "Fecha Futura");
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
                    return true;
                }

                throw new Exception($"Error en procesamiento DATOS_GRAL folio {folio}");
            }

            return false;
        }
    }
}

using Aplicacion.Interfaces.FXILocal;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    public class FXILocal
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IFXI_LocalDao _fxiLocalDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "FXI_Local";

        private string folioUnico = string.Empty;

        public FXILocal(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IFXI_LocalDao fxiLocalDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _fxiLocalDao = fxiLocalDao;
            _vwT24 = vwT24;
        }

        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            bool procesado;

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

            if (string.IsNullOrEmpty(_configuracion.AliasProceso))
                return false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            // DATOS_GRAL
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

                procesado = false;

                // Obtener datos básicos
                var datos = await _fxiLocalDao.ObtenerDatosBasicosPorFolio(folio);

                folioUnico = datos?.NfFolioUnico ?? string.Empty;
                fechaEjecucion = datos?.NfFechaEjecucion?.ToString() ?? string.Empty;

                // Validar / generar folio_unico
                if (string.IsNullOrWhiteSpace(folioUnico))
                {
                    folioUnico = await _vwT24.GeneraFolioUnico(
                        tipoOperacion,
                        ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion),
                        folio);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Creamos folio_unico='{folioUnico}'.");

                    await _fxiLocalDao.ActualizarFolioUnico(folio, folioUnico);
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                // Actualizar maestro
                await _fxiLocalDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    _vwT24.MonedaDefaultMN);

                procesado = true;

                // Finalizar proceso
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
                    return true;
                }

                throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
            }

            return false;
        }
    }
}

using Aplicacion.Interfaces.FXIGPH;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa FXI H2 H.
    /// </summary>
    public class FXI_H2H
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IFXI_H2HDao _fxiH2HDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "FXI_H2H";

        private string folioUnico = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXI_H2H.
        /// </summary>

        public FXI_H2H(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IFXI_H2HDao fxiH2HDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _fxiH2HDao = fxiH2HDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>
        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            bool procesado;

            // =========================================
            // Obtener AliasProceso
            // =========================================
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

            Config.EscribirLog(
                $"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            // =========================================
            // DATOS_GRAL
            // =========================================
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro.");

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

                // =========================================
                // Obtener datos básicos
                // =========================================
                var datos = await _fxiH2HDao.ObtenerDatosBasicosPorFolio(folio);

                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString() ?? string.Empty;
                }

                // =========================================
                // Validación CRÍTICA (legacy)
                // =========================================
                if (string.IsNullOrWhiteSpace(folioUnico))
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("No hay folio_unico asignado para la operación");
                }

                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");

                // =========================================
                // Actualizar maestro
                // =========================================
                await _fxiH2HDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    _vwT24.MonedaDefaultMN);

                procesado = true;

                // =========================================
                // Fecha futura
                // =========================================
                if (string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) > 0)
                {
                    Config.EscribirLog(
                        $"INFO|{tipoOperacion}.procesaOperacion|Es fecha futura.");

                    await _fxiH2HDao.MarcarFechaFutura(folio, tipoOperacion);
                }

                // =========================================
                // Finalizar
                // =========================================
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
                    return true;
                }

                throw new Exception($"Error en procesamiento DATOS_GRAL folio {folio}");
            }

            return false;
        }
    }
}

using Aplicacion.DTOs.FXT;
using Aplicacion.Modelos;
using Infraestructura.Repositorio.FXT;
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
    /// Representa FXT.
    /// </summary>
    public class FXT
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IFxtTomiDao _fxtDao;
        private readonly IFxtCmsDao _fxtCmsDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "FXT";
        private string folio_unico = string.Empty;

        /// <summary>
        /// Constructor.
        /// </summary>
        public FXT(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IFxtTomiDao fxtDao,
            IFxtCmsDao fxtCmsDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _fxtDao = fxtDao;
            _fxtCmsDao = fxtCmsDao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación FXT.
        /// </summary>
        public async Task<bool> procesaOperacion(string folio)
        {
            object[] laValoresFormato = new object[30];
            string lsFechaEjecucion = string.Empty;
            string lsMoneda = string.Empty;
            decimal ldImporte = 0m;
            bool lbConForex = false;
            bool lbForexCancelado = false;
            string lsFolioForex = string.Empty;
            bool lbProcesado;
            bool lbFechaFutura;
            bool lbResultado;

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

            // Datos del folio en CMS y TOMI.
            lsFechaEjecucion = string.Empty;
            lsMoneda = string.Empty;
            ldImporte = 0m;
            lsFolioForex = string.Empty;
            lbConForex = false;
            lbForexCancelado = false;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Aperturando conexión a base de datos del CMS.");

            FxtFormatoCmsDto datosCms = await _fxtCmsDao.ObtenerFormatoPorFolio(folio);
            if (datosCms != null)
            {
                laValoresFormato[19] = "FXT";
                laValoresFormato[20] = datosCms.IdFormato;
                laValoresFormato[21] = datosCms.Cdl;
                laValoresFormato[22] = datosCms.IdCuenta;
                laValoresFormato[23] = datosCms.FechaCaptura;
                laValoresFormato[24] = datosCms.IdUsuarioCaptura;
                laValoresFormato[25] = datosCms.FechaAutorizacion;
                laValoresFormato[26] = datosCms.IdUsuarioAutorizo;
                laValoresFormato[27] = datosCms.MontoOperacion;
                laValoresFormato[28] = datosCms.IdPlantilla;
                laValoresFormato[29] = datosCms.IdFormatoPlantilla;

                // Datos del contenido.
                var parseado = ContenidoFormatoParser.Parse(datosCms.ContenidoFormato, folio);
                ContenidoFormatoParser.ValidarMinimoIndice(parseado, folio, 18);

                for (int liIdxPipes = 0; liIdxPipes <= 18; liIdxPipes++)
                {
                    laValoresFormato[liIdxPipes] = ContenidoFormatoParser.ObtenerValorPosicional(parseado, folio, liIdxPipes);
                }

                if (laValoresFormato[3] != null)
                    lsFolioForex = Convert.ToString(laValoresFormato[3]);

                if (laValoresFormato[13] != null)
                    lsFechaEjecucion = Convert.ToString(laValoresFormato[13]);

                if (laValoresFormato[4] != null)
                    lsMoneda = _vwT24.ValidaMoneda(Convert.ToString(laValoresFormato[4]));

                ldImporte = datosCms.MontoOperacion;
            }

            if (CompararMontos(laValoresFormato[5], laValoresFormato[18]) == false ||
                CompararMontos(laValoresFormato[5], laValoresFormato[27]) == false)
            {
                throw new Exception($"Hay diferencias entre los montos de la operación: MontoOperacion={laValoresFormato[27]}, MontoCapturado={laValoresFormato[5]}, MontoEnviado={laValoresFormato[18]}.");
            }

            Config.EscribirLog($"INFO|FXD.procesaOperacion|Veamos que trae la fecha{laValoresFormato[13]}-----{lsFechaEjecucion}");

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema);
            int intFechaEjecucion = int.Parse(lsFechaEjecucion);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception(
                    $"Se desea realizar una operación al {lsFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            folio_unico = await _vwT24.GeneraFolioUnico(tipoOperacion, ConexionT24Mw.CambiarFechaAJulianaT24(lsFechaEjecucion), folio);
            lbFechaFutura = string.CompareOrdinal(lsFechaEjecucion, _configuracion.StrFechaSistema) > 0;

            Config.EscribirLog("INFO|FXT.procesaOperacion|Validando si la operación está asignada a un Forex.");

            if (!string.Equals(lsFolioForex, string.Empty, StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|FXT.procesaOperacion|Asignado el folio forex: {lsFolioForex} .");
                lbConForex = true;

                if (decimal.TryParse(lsFolioForex, out _))
                    throw new Exception($"El Folio del Forex no es válido porque es numérico; se espera una referencia alfanumérica/external ref: {lsFolioForex}");

                FxtForexStatusTomiDto forexStatus = await _fxtDao.ObtenerStatusForexPorExternalRef(lsFolioForex);
                if (forexStatus != null && string.Equals(forexStatus.StatusOperacion, "CANCELADO", StringComparison.OrdinalIgnoreCase))
                {
                    lbForexCancelado = true;
                    Config.EscribirLog("INFO|FXT.procesaOperacion|FOREX CANCELADO.");
                }
            }

            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string lsSubTipo = string.Empty;

                Config.EscribirLog("INFO|FXT.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");
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

                if (lbConForex)
                    lsSubTipo = $"FX:{lsFolioForex}";

                await _fxtDao.ActualizarDatosGeneralesPlanTrabajo(new FxtActualizarDatosGeneralesPlanTrabajoDto
                {
                    Folio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                    TipoOperacion = tipoOperacion,
                    FolioUnico = folio_unico,
                    FecValor = ConversionHelper.ConvertirDecimalDirecto(lsFechaEjecucion, "FechaEjecucion"),
                    CveMoneda = lsMoneda,
                    Importe = ldImporte,
                    NumCliente = ConversionHelper.ConvertirNullableIntDirecto(laValoresFormato[21], "Cdl"),
                    NombreCliente = Convert.ToString(laValoresFormato[1]),
                    NumCuenta = Convert.ToString(laValoresFormato[22]),
                    SubtipoOperacion = lsSubTipo
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

            if (string.Equals(_configuracion.AliasProceso, "CMS_A_TOMI", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|FXT.procesaOperacion|A insertar datos de CMS a TOMI.");
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
                lbResultado = await CMS_A_TOMI(folio, laValoresFormato);

                if (lbResultado)
                {
                    Config.EscribirLog("INFO|FXT.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                    lbProcesado = true;
                }
                else
                {
                    bool existe = await _fxtDao.ExisteFormatoFxtPorFolio(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                    if (existe)
                    {
                        lbProcesado = true;
                        Config.EscribirLog("INFO|FXT.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (lbProcesado)
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

            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                DateTime fechaConfirma;

                Config.EscribirLog("INFO|FXT.procesaOperacion|A actualizar estatus en CMS y TOMI.");
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
                lbProcesado = true;

                if (lbConForex && lbForexCancelado)
                {
                    await _fxtCmsDao.ActualizarEstatusFormatoRechazado(new FxtActualizarEstatusFormatoRechazadoDto
                    {
                        IdFormato = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                        RazonRechazo = "El forex relacionado ha sido cancelado"
                    });

                    await _fxtDao.ActualizarEstatusFxtRechazadoForex(new FxtActualizarEstatusFxtRechazadoForexDto
                    {
                        NfFolio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                        NfRazon = "El forex relacionado ha sido cancelado"
                    });

                    Config.EscribirLog($"INFO|FXT.procesaOperacion|Forex Rechazado con Anterioridad: {folio}");
                    Config.EscribirLog("INFO|FXT.procesaOperacion|A cancelar la operación.");
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

                    await _fxtDao.CancelarPlanTrabajoPorForex(new FxtCancelarPlanTrabajoPorForexDto
                    {
                        Folio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                        TipoOperacion = tipoOperacion,
                        FecValor = ConversionHelper.ConvertirDecimalDirecto(lsFechaEjecucion, "FechaEjecucion"),
                        CveMoneda = lsMoneda,
                        Importe = ldImporte,
                        ErrDescripcion = "El forex relacionado ha sido cancelado"
                    });

                    return true;
                }
                else
                {
                    fechaConfirma = ObtenerFechaConfirmacionLegacy();

                    if (string.Equals(lsFechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                    {
                        await _fxtCmsDao.ActualizarEstatusFormatoSimple(new FxtActualizarEstatusFormatoSimpleDto
                        {
                            IdFormato = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                            Mensaje = "Formato aceptado",
                            Estatus = "APFT"
                        });

                        await _fxtDao.ActualizarEstatusFxtConfirmacion(new FxtActualizarEstatusFxtConfirmacionDto
                        {
                            NfFolio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                            NfEstatus = "CONP",
                            NfUsuarioConfirma = "OPER CMS",
                            NfFechaConfirma = fechaConfirma
                        });

                        Config.EscribirLog("INFO|FXT.procesaOperacion|Operación fecha valor hoy.");
                    }
                    else if (string.CompareOrdinal(lsFechaEjecucion, _configuracion.StrFechaSistema) < 0)
                    {
                        await _controlServicio.MarcarErrorFechaPasada(ConversionHelper.ConvertirLongDirecto(folio, "Folio"));
                        throw new Exception($"Se desea realizar una operación al {lsFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                    }
                    else
                    {
                        await _fxtCmsDao.ActualizarEstatusFormatoSimple(new FxtActualizarEstatusFormatoSimpleDto
                        {
                            IdFormato = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                            Mensaje = "Formato recibido pendiente ejecución",
                            Estatus = "REPE"
                        });

                        await _fxtDao.ActualizarEstatusFxtConfirmacion(new FxtActualizarEstatusFxtConfirmacionDto
                        {
                            NfFolio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                            NfEstatus = "CONF",
                            NfUsuarioConfirma = "OPER CMS",
                            NfFechaConfirma = fechaConfirma
                        });

                        Config.EscribirLog("INFO|FXT.procesaOperacion|Operación fecha valor fútura.");
                    }
                }

                if (lbProcesado)
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

            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                int liRegsCMS = 1;
                int liRegsTOMI = 0;
                string lsMsjError = string.Empty;
                string lsFolioForexTOMI = string.Empty;
                string lsFechaEjecucionTOMI = string.Empty;
                string lsMonedaTOMI = string.Empty;
                decimal ldImporteTOMI = 0m;

                Config.EscribirLog("INFO|FXT.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");
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

                FxtConciliacionTomiDto datosTomi = await _fxtDao.ObtenerConciliacionFxtPorFolio(ConversionHelper.ConvertirLongDirecto(folio, "Folio"), lbConForex && lbForexCancelado);
                if (datosTomi != null)
                {
                    liRegsTOMI = 1;
                    lsFolioForexTOMI = datosTomi.NfForexRelacionado ?? string.Empty;
                    lsFechaEjecucionTOMI = datosTomi.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                    lsMonedaTOMI = _vwT24.ValidaMoneda(datosTomi.T24DebitCurrency);
                    ldImporteTOMI = datosTomi.T24DebitAmount ?? 0m;
                }

                if (liRegsCMS == 1 && liRegsTOMI == 1)
                {
                    if (!string.Equals(lsFolioForex, lsFolioForexTOMI, StringComparison.Ordinal))
                        lsMsjError += Environment.NewLine + $" - Diferencia en la autorización de FOREX. CMS='{lsFolioForex}', TOMI='{lsFolioForexTOMI}'.";

                    if (!string.Equals(lsFechaEjecucion, lsFechaEjecucionTOMI, StringComparison.Ordinal))
                        lsMsjError += Environment.NewLine + $" - Diferencia en la fecha de ejecución. CMS='{lsFechaEjecucion}', TOMI='{lsFechaEjecucionTOMI}'.";

                    if (!string.Equals(lsMoneda, lsMonedaTOMI, StringComparison.Ordinal))
                        lsMsjError += Environment.NewLine + $" - Diferencia en la monedas. CMS='{lsMoneda}', TOMI='{lsMonedaTOMI}'.";

                    if (ldImporte != ldImporteTOMI)
                        lsMsjError += Environment.NewLine + $" - Diferencia en el importe. CMS='{ldImporte}', TOMI='{ldImporteTOMI}'.";

                    lbProcesado = true;
                }
                else
                {
                    lbProcesado = false;
                    lsMsjError += Environment.NewLine + " - No se ha registrado la operación en TOMI.";
                }

                if (!string.Equals(lsMsjError, string.Empty, StringComparison.Ordinal))
                {
                    lbProcesado = false;
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(lsMsjError);
                }

                if (lbProcesado)
                {
                    if (lbFechaFutura)
                    {
                        await _fxtDao.MarcarPlanTrabajoFechaFutura(new FxtMarcarFechaFuturaPlanTrabajoDto
                        {
                            Folio = ConversionHelper.ConvertirLongDirecto(folio, "Folio"),
                            TipoOperacion = tipoOperacion
                        });
                    }

                    Config.EscribirLog("INFO|FXT.procesaOperacion|La operación está correctamente conciliada.");
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

        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato)
        {
            try
            {
                bool resultado = await _fxtDao.InsertarFormatoFxt(new FxtInsertarFormatoFxtTomiDto
                {
                    NF_FOLIO_UNICO = folio_unico,
                    NF_TIPO = Convert.ToString(valoresFormato[19]),
                    NF_FOLIO = ConversionHelper.ConvertirIntDirecto(valoresFormato[20], "NF_FOLIO"),
                    T24_CUSTOMER_CODE = Convert.ToString(valoresFormato[21]),
                    T24_DEBIT_ACCT_NO = Convert.ToString(valoresFormato[22]),
                    NF_ESTATUS = "PEND",
                    NF_USUARIO_CAPTURA = Convert.ToString(valoresFormato[24]),
                    NF_USUARIO_AUTORIZA = Convert.ToString(valoresFormato[26]),
                    NF_FECHA_CAPTURA = ConversionHelper.ConvertirDateTimeDirecto(valoresFormato[23], "NF_FECHA_CAPTURA"),
                    NF_FECHA_AUTORIZA = ConversionHelper.ConvertirNullableDateTimeDirecto(valoresFormato[25], "NF_FECHA_AUTORIZA"),
                    T24_DEBIT_AMOUNT = ConversionHelper.ConvertirDoubleDirecto(valoresFormato[27], "T24_DEBIT_AMOUNT"),
                    T24_SHORT_NAME = Convert.ToString(valoresFormato[1]),
                    NF_FOREX_RELACIONADO = Convert.ToString(valoresFormato[3]),
                    T24_DEBIT_CURRENCY = Convert.ToString(valoresFormato[4]),
                    NF_BANCO_BENEF = Convert.ToString(valoresFormato[6]),
                    T24_BEN_ACCT_NO = Convert.ToString(valoresFormato[7]),
                    NF_TIPO_CUENTA = Convert.ToString(valoresFormato[8]),
                    T24_BEN_CUST_NAME = Convert.ToString(valoresFormato[10]),
                    NF_RFC_BENEFICIARIO = Convert.ToString(valoresFormato[11]),
                    NF_TASA_RETENCION = Convert.ToString(valoresFormato[12]),
                    NF_FECHA_EJECUCION = ConversionHelper.ConvertirIntDirecto(valoresFormato[13], "NF_FECHA_EJECUCION"),
                    T24_DEBIT_THEIR_REF = Convert.ToString(valoresFormato[15]),
                    NF_MONTO_ENVIAR = ConversionHelper.ConvertirDecimalDirecto(valoresFormato[18], "NF_MONTO_ENVIAR"),
                    ID_PLANTILLA = Convert.ToString(valoresFormato[28]),
                    ID_FORMATO_PLANTILLA = ConversionHelper.ConvertirNullableLongDirecto(valoresFormato[29], "ID_FORMATO_PLANTILLA")
                });

                Config.EscribirLog($"INFO|FXT.CMS_A_TOMI|Salida={(resultado ? 0 : 1)}.");
                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|FXT.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        private static DateTime ObtenerFechaConfirmacionLegacy()
        {
            return DateTime.ParseExact(
                DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                "yyyy/MM/dd HH:mm",
                CultureInfo.InvariantCulture);
        }

        private static bool CompararMontos(object valorA, object valorB)
        {
            decimal montoA = ConversionHelper.ConvertirDecimalDirecto(valorA, "MontoA");
            decimal montoB = ConversionHelper.ConvertirDecimalDirecto(valorB, "MontoB");
            return montoA == montoB;
        }
    }
}

using Aplicacion.Interfaces.FXT_GPH;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa FXTGPH.
    /// </summary>
    public class FXTGPH
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IFXT_GPHDao _fxtGphDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "FXT_GPH";

        private string folioUnico = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXTGPH.
        /// </summary>

        public FXTGPH(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IFXT_GPHDao fxtGphDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _fxtGphDao = fxtGphDao;
            _vwT24 = vwT24;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> ProcesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            long folioArchivo = 0;
            string nombreArchivo = string.Empty;
            string idOperacion = string.Empty;

            bool procesado;

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

                procesado = false;

                var datos = await _fxtGphDao.ObtenerDatosBasicosPorFolio(folio);

                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString() ?? string.Empty;
                    nombreArchivo = datos.NfNombreArchivoGph ?? string.Empty;
                    idOperacion = datos.NfPmtInfId ?? string.Empty;
                    folioArchivo = datos.NfIdArchivo ?? 0;
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

                    await _fxtGphDao.ActualizarFolioUnico(folio, folioUnico);
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                await _fxtGphDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    _vwT24.MonedaDefaultMN);

                procesado = true;

                if (string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) > 0)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Es fecha futura.");

                    await _fxtGphDao.MarcarFechaFutura(folio, tipoOperacion);

                    await _fxtGphDao.InsertarDetalleOperacionGph(
                        nombreArchivo,
                        folioArchivo,
                        idOperacion,
                        tipoOperacion,
                        "ACEPTADA",
                        "Fecha Futura");
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
                    return true;
                }

                throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
            }

            return false;
        }
    }
}


using Aplicacion.Modelos;
using Infraestructura.Repositorio.FXTH2H;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa FXTH2 H.
    /// </summary>
    public class FXTH2H
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IFXTH2HDao _fxtH2HDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "FXT_H2H";

        private string folioUnico = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXTH2H.
        /// </summary>

        public FXTH2H(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IFXTH2HDao fxtH2HDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _fxtH2HDao = fxtH2HDao;
            _vwT24 = vwT24;
        }
        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>

        public async Task<bool> procesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            bool procesado;

            // =========================================
            // Último proceso
            // =========================================
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

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");

            // =========================================
            // DATOS_GRAL
            // =========================================
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de trabajo maestro.");

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

                // =========================================
                // Obtener datos básicos
                // =========================================
                var datos = await _fxtH2HDao.ObtenerDatosBasicosPorFolio(folio);

                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString() ?? string.Empty;
                }

                // =========================================
                // Validación CRÍTICA (legacy)
                // =========================================
                if (string.IsNullOrWhiteSpace(folioUnico))
                {
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("No hay folio_unico asignado para la operación");
                }

                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");

                // =========================================
                // Actualizar maestro
                // =========================================
                await _fxtH2HDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    tipoOperacion,
                    folioUnico,
                    _vwT24.MonedaDefaultMN);

                procesado = true;

                // =========================================
                // Fecha futura
                // =========================================
                if (string.CompareOrdinal(fechaEjecucion, _configuracion.StrFechaSistema) > 0)
                {
                    Config.EscribirLog(
                        $"INFO|{tipoOperacion}.procesaOperacion|Es fecha futura.");

                    await _fxtH2HDao.MarcarFechaFutura(
                        folio,
                        tipoOperacion);
                }

                // =========================================
                // Finalizar proceso
                // =========================================
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
                    return true;
                }

                throw new Exception($"Error en procesamiento DATOS_GRAL folio {folio}");
            }

            return false;
        }
    }
}

using Aplicacion.Interfaces.FXT_Local;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    public class FXTLocal
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IFXT_LocalDao _fxtLocalDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "FXT_Local";
        private string folioUnico = string.Empty;

        public FXTLocal(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IFXT_LocalDao fxtLocalDao,
            ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _fxtLocalDao = fxtLocalDao;
            _vwT24 = vwT24;
        }

        public async Task<bool> ProcesaOperacion(string folio)
        {
            string fechaEjecucion = string.Empty;
            bool procesado;

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

                procesado = false;

                var datos = await _fxtLocalDao.ObtenerDatosBasicosPorFolio(folio);
                if (datos != null)
                {
                    folioUnico = datos.NfFolioUnico ?? string.Empty;
                    fechaEjecucion = datos.NfFechaEjecucion?.ToString() ?? string.Empty;
                }

                if (string.IsNullOrWhiteSpace(folioUnico))
                {
                    folioUnico = await _vwT24.GeneraFolioUnico(
                        tipoOperacion,
                        ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucion),
                        folio);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Creamos folio_unico='{folioUnico}'.");

                    await _fxtLocalDao.ActualizarFolioUnico(folio, folioUnico);
                }
                else
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Leyendo folio_unico='{folioUnico}'.");
                }

                await _fxtLocalDao.ActualizarPlanTrabajoMaestroLectura(
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
                    return true;
                }

                throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
            }

            return false;
        }
    }
}

using Aplicacion.DTOs.MFX;
using Aplicacion.Interfaces.MFX;
using Aplicacion.Modelos;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Helpers;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa MFX.
    /// </summary>
    public class MFX
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IMFXTomiDao _mfxTomiDao;
        private readonly IMFXCmsDao _mfxCmsDao;
        private readonly IMFXVistasT24Dao _mfxVistasT24Dao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "MFX";

        private string folioUnico = string.Empty;
        private string estatusInicial = string.Empty;
        private string folioCms = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase MFX.
        /// </summary>

        public MFX(ConfiguracionModelo configuracion, IControlServicio controlServicio, IMFXTomiDao mfxTomiDao, IMFXCmsDao mfxCmsDao, IMFXVistasT24Dao mfxVistasT24Dao, ConexionT24Mw vwT24)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _mfxTomiDao = mfxTomiDao;
            _mfxCmsDao = mfxCmsDao;
            _mfxVistasT24Dao = mfxVistasT24Dao;
            _vwT24 = vwT24;
        }

        /// <summary>
        /// Procesa la operación MFX.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio)
        {
            bool forexCancelado;
            bool procesado;
            bool resultado;
            bool datoCms;
            string folioForex;
            object[] valoresFormato = new object[28];

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

            // Datos del folio en TOMI.
            folioForex = string.Empty;
            forexCancelado = false;
            folioCms = string.Empty;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Aperturando conexión a base de datos del CMS.");
            Config.EscribirLog("INFO|MFX.procesaOperacion|Leyendo datos de TOMI.");

            MFXSolicitudForexTomiDto datosTomi = await _mfxTomiDao.ObtenerSolicitudForexPorFolio(folio);

            if (datosTomi == null)
                throw new Exception($"No hay información de TOMI para el FOREX {folio}");

            string cuentaEntregaCalculada = datosTomi.FlgCuentaDebitoCms == true
                ? "TRANS"
                : datosTomi.CuentaDebito;

            string cuentaAdquiereCalculada = datosTomi.FlgCuentaCreditoCms == true
                ? "TRANS"
                : datosTomi.CuentaCredito;

            string cuentaEntregaOriginal = datosTomi.CuentaDebito;
            string cuentaAdquiereOriginal = datosTomi.CuentaCredito;

            if (string.Equals(datosTomi.StatusOperacion, "CANCELADO", StringComparison.Ordinal) ||
                string.Equals(datosTomi.Status, "ELIMINAR", StringComparison.Ordinal))
            {
                forexCancelado = true;
            }

            // Vistas T24.
            bool entregaTrans = false;
            bool adquiereTrans = false;

            Config.EscribirLog("INFO|MFX.procesaOperacion|Iniciando base de Vistas T24.");

            MFXCuentaCustomerT24Dto cuentaEntregaT24 = await _mfxVistasT24Dao.ObtenerCuentaCustomer(cuentaEntregaOriginal);
            if (cuentaEntregaT24 != null &&
                string.Equals(cuentaEntregaT24.Category, "1013", StringComparison.Ordinal))
            {
                entregaTrans = true;
            }

            MFXCuentaCustomerT24Dto cuentaAdquiereT24 = await _mfxVistasT24Dao.ObtenerCuentaCustomer(cuentaAdquiereOriginal);
            if (cuentaAdquiereT24 != null &&
                string.Equals(cuentaAdquiereT24.Category, "1013", StringComparison.Ordinal))
            {
                adquiereTrans = true;
            }

            Config.EscribirLog("INFO|MFX.procesaOperacion|Cerrando base de Vistas T24.");

            if (string.Equals(cuentaEntregaCalculada, "TRANS", StringComparison.Ordinal) && !entregaTrans)
                throw new Exception($"La cuenta {cuentaEntregaOriginal} del Forex {folio} no es una cuenta transitoria.");

            if (string.Equals(cuentaAdquiereCalculada, "TRANS", StringComparison.Ordinal) && !adquiereTrans)
                throw new Exception($"La cuenta {cuentaAdquiereOriginal} del Forex {folio} no es una cuenta transitoria.");

            if (string.Equals(cuentaEntregaCalculada, "TRANS", StringComparison.Ordinal) &&
                string.Equals(cuentaAdquiereCalculada, cuentaEntregaCalculada, StringComparison.Ordinal))
            {
                throw new Exception($"La cuenta {cuentaEntregaOriginal} y la cuenta {cuentaAdquiereOriginal} del Forex {folio} son transitorias.");
            }

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|MFX.procesaOperacion|Inicio la operacion MFX, con folio {folio}.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                procesado = await _mfxTomiDao.ActualizarMaestroDatosGral(
                    folio,
                    tipoOperacion,
                    datosTomi.FechaValorTexto,
                    datosTomi.DivisaEntregaCliente,
                    datosTomi.MontoEntregaCliente,
                    Convert.ToInt32(datosTomi.IdCliente),
                    datosTomi.Nombre,
                    cuentaEntregaCalculada);

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

                    if (forexCancelado)
                    {
                        await _mfxTomiDao.CancelarMaestroPorForexCanceladoBTMUMDesdeLectura(folio, tipoOperacion);
                        return true;
                    }
                }
                else
                {
                    throw new Exception($"Error en procesamiento de registro DATOS_GRAL con folio: {folio}");
                }
            }

            if (string.Equals(_configuracion.AliasProceso, "TOMI_A_CMS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|MFX.procesaOperacion|A insertar la confirmación de FOREX en CMS.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                await _mfxTomiDao.MarcarMaestroEnvioCms(folio, tipoOperacion);

                estatusInicial = datosTomi.Status;

                procesado = await TOMI_A_CMS(folio, datosTomi);

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
                    throw new Exception($"Error en procesamiento de registro TOMI_A_CMS con folio: {folio}");
                }
            }

            // Leo en CMS por datos de Forex confirmados.
            datoCms = false;
            Config.EscribirLog($"INFO|MFX.procesaOperacion|A buscar la autorizacion={folio}");

            List<MFXFormatoCmsDto> formatosCms = await _mfxCmsDao.ObtenerFormatosForexConfirmados(folio);

            foreach (MFXFormatoCmsDto formatoCms in formatosCms)
            {
                valoresFormato[19] = "MFX";
                valoresFormato[20] = formatoCms.IdFormato;
                valoresFormato[21] = formatoCms.Cdl;
                valoresFormato[22] = formatoCms.IdCuenta;
                valoresFormato[23] = formatoCms.FechaCaptura;
                valoresFormato[24] = formatoCms.IdUsuarioCaptura;
                valoresFormato[25] = formatoCms.FechaAutorizacion;
                valoresFormato[26] = formatoCms.IdUsuarioAutorizo;
                valoresFormato[27] = formatoCms.MontoOperacion;

                string[] valoresPipes = (formatoCms.ContenidoFormato ?? string.Empty)
                    .Split(new[] { "|" }, StringSplitOptions.None);

                for (int idx = 0; idx <= 11; idx++)
                {
                    string[] llaveValor = valoresPipes[idx].Split(new[] { "=" }, StringSplitOptions.None);
                    string valor = llaveValor[1];

                    if ((idx == 7 || idx == 10) &&
                        string.Equals(valor, "TRANS", StringComparison.Ordinal))
                    {
                        valoresFormato[idx] = "TRANSITORIA";
                    }
                    else
                    {
                        valoresFormato[idx] = valor;
                    }
                }

                for (int idx = 12; idx <= 18; idx++)
                    valoresFormato[idx] = null;

                if (valoresFormato[2] != null && valoresFormato[2] != DBNull.Value)
                    folioForex = Convert.ToString(valoresFormato[2]);

                if (string.Equals(folioForex, folio, StringComparison.Ordinal) ||
                    string.Equals(folioForex, $"C{folio}", StringComparison.Ordinal))
                {
                    folioCms = Convert.ToString(valoresFormato[20]);
                    datoCms = true;
                    break;
                }
            }

            if (datoCms)
                Config.EscribirLog($"INFO|MFX.procesaOperacion|Encontrado con folio CMS={folioCms}");
            else
                Config.EscribirLog("INFO|MFX.procesaOperacion|No se encontró el folio CMS.");

            // Este estatus se queda hasta recibir una respuesta del cliente.
            if (string.Equals(_configuracion.AliasProceso, "ESPERANDO_CONFIRMACION", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|MFX.procesaOperacion|Buscando confirmación del cliente, lbDatoCMS={datoCms}.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                await _mfxTomiDao.MarcarMaestroEsperandoConfirmacion(folio, tipoOperacion);

                if (forexCancelado)
                {
                    if (!string.Equals(estatusInicial, datosTomi.Status, StringComparison.Ordinal))
                    {
                        // Cambio en BTMUM de CAPTURADA a ELIMINAR.
                        procesado = await TOMI_A_CMS(folio, datosTomi);

                        await _controlServicio.TerminaProceso(new ProcesoDTO
                        {
                            Folio = folio,
                            TipoOperacion = tipoOperacion,
                            Servicio = _configuracion.NombreServicio,
                            NombreProceso = _configuracion.NombreProceso,
                            AliasProceso = _configuracion.AliasProceso,
                            PId = _configuracion.Pid.Value
                        });

                        await _mfxTomiDao.CancelarMaestroPorForexCanceladoBTMUMDesdeEsperandoConfirmacion(folio, tipoOperacion);
                        return true;
                    }
                }

                procesado = false;

                if (datoCms)
                {
                    procesado = true;
                    Config.EscribirLog("INFO|MFX.procesaOperacion|Ya existe la confirmación del cliente.");
                }
                else
                {
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
                    throw new Exception($"Error en procesamiento de registro ESPERANDO_CONFIRMACION con folio: {folio}");
                }
            }

            // Genero el folio único.
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(_configuracion.StrFechaSistema),
                folioCms);

            // Lectura del CMS e inserción a TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CMS_A_TOMI", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|MFX.procesaOperacion|A insertar datos de CMS a TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                long folioCmsLong = Convert.ToInt64(folioCms);

                await _mfxTomiDao.MarcarMaestroLecturaConFolioCms(folio, tipoOperacion, folioCmsLong);

                procesado = false;
                resultado = await CMS_A_TOMI(folioCms, valoresFormato);

                if (resultado)
                {
                    Config.EscribirLog("INFO|MFX.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");
                    procesado = true;
                }
                else
                {
                    int numRegs = await _mfxTomiDao.ContarTombinMfxPorFolio(folioCmsLong);

                    if (numRegs > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|MFX.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                    }
                }

                if (procesado)
                {
                    long folioForexNormalizado = NormalizarFolioForex(folioForex);
                    await _mfxTomiDao.ActualizarSolicitudForexValidacionSistema(folioForexNormalizado);

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

            // Este estatus se queda hasta recibir la autorización final de tesorería.
            if (string.Equals(_configuracion.AliasProceso, "TESORERIA_AUTORIZACION", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|MFX.procesaOperacion|Buscando autorización de BTMUM - Tesoreria.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                await _mfxTomiDao.MarcarMaestroEsperandoAutorizacionTesoreria(folio, tipoOperacion);

                procesado = false;

                if (string.Equals(datosTomi.StatusOperacion, "AUTORIZADO", StringComparison.Ordinal))
                {
                    // Cambio en PREAUTORIZADA a AUTORIZADO.
                    procesado = true;
                }
                else if (forexCancelado)
                {
                    // Cambio a CANCELADO.
                    procesado = true;

                    await _controlServicio.TerminaProceso(new ProcesoDTO
                    {
                        Folio = folio,
                        TipoOperacion = tipoOperacion,
                        Servicio = _configuracion.NombreServicio,
                        NombreProceso = _configuracion.NombreProceso,
                        AliasProceso = _configuracion.AliasProceso,
                        PId = _configuracion.Pid.Value
                    });

                    await _mfxTomiDao.CancelarMaestroPorForexCanceladoBTMUMDesdeTesoreria(folio, tipoOperacion);
                    return true;
                }
                else
                {
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
                    throw new Exception($"Error en procesamiento de registro TESORERIA_AUTORIZACION con folio: {folio}");
                }
            }

            // Actualización del estatus del CMS y TOMI.
            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog("INFO|MFX.procesaOperacion|A actualizar estatus en CMS y TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                long folioCmsLong = Convert.ToInt64(folioCms);

                if (forexCancelado)
                {
                    // Rechazado por cancelación del Forex.
                    await _mfxCmsDao.RechazarFormatoForex(folioCmsLong);
                    await _mfxTomiDao.ActualizarTombinMfxRechazado(folioCmsLong);

                    Config.EscribirLog($"INFO|MFX.procesaOperacion|Forex Rechazado con Anterioridad: {folio}");

                    procesado = true;

                    await _mfxTomiDao.CancelarMaestroPorForexCanceladoCMS(folio, tipoOperacion);
                }
                else
                {
                    // Aceptado.
                    await _mfxCmsDao.AceptarFormatoForex(folioCmsLong);

                    await _mfxTomiDao.ActualizarTombinMfxConfirmado(
                        folioCmsLong,
                        DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture));

                    procesado = true;
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
                int regsForex;
                string folioForexTomi;
                string mensajeError;

                string fechaForex;
                string divisaAdquiereForex;
                object montoAdquiereForex;
                string cuentaCreditoForex;
                string divisaEntregaForex;
                object montoEntregaForex;
                string cuentaDebitoForex;

                string fechaTomi;
                string divisaAdquiereTomi;
                object montoAdquiereTomi;
                string cuentaCreditoTomi;
                string divisaEntregaTomi;
                object montoEntregaTomi;
                string cuentaDebitoTomi;

                Config.EscribirLog("INFO|MFX.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                // Registro en CMS. Los datos ya se obtuvieron previamente.
                regsCms = 1;

                // Registros en FOREX.
                regsForex = 0;
                fechaForex = string.Empty;
                divisaAdquiereForex = string.Empty;
                montoAdquiereForex = null;
                cuentaCreditoForex = string.Empty;
                divisaEntregaForex = string.Empty;
                montoEntregaForex = null;
                cuentaDebitoForex = string.Empty;

                long folioForexNormalizado = NormalizarFolioForex(folioForex);

                MFXForexConciliacionDto datosForex = await _mfxTomiDao.ObtenerDatosForexParaConciliacion(folioForexNormalizado);
                if (datosForex != null)
                {
                    regsForex = 1;

                    fechaForex = datosForex.FechaValorTexto;
                    divisaAdquiereForex = datosForex.DivisaAdquiereCliente;
                    montoAdquiereForex = datosForex.MontoAdquiereCliente;

                    cuentaCreditoForex = datosForex.FlgCuentaCreditoCms == true
                        ? "TRANSITORIA"
                        : datosForex.CuentaCredito;

                    divisaEntregaForex = datosForex.DivisaEntregaCliente;
                    montoEntregaForex = datosForex.MontoEntregaCliente;

                    cuentaDebitoForex = datosForex.FlgCuentaDebitoCms == true
                        ? "TRANSITORIA"
                        : datosForex.CuentaDebito;
                }

                // Registros en TOMI.
                regsTomi = 0;
                folioForexTomi = string.Empty;
                fechaTomi = string.Empty;
                divisaAdquiereTomi = string.Empty;
                montoAdquiereTomi = null;
                cuentaCreditoTomi = string.Empty;
                divisaEntregaTomi = string.Empty;
                montoEntregaTomi = null;
                cuentaDebitoTomi = string.Empty;

                long folioCmsLongConciliacion = Convert.ToInt64(folioCms);

                MFXTomiConciliacionDto datosTomiConciliacion =
                    await _mfxTomiDao.ObtenerDatosTomiParaConciliacion(folioCmsLongConciliacion, forexCancelado);

                if (datosTomiConciliacion != null)
                {
                    regsTomi = 1;

                    folioForexTomi = datosTomiConciliacion.NfAutorizacion ?? string.Empty;

                    fechaTomi = datosTomiConciliacion.NfFechaEjecucion.HasValue
                        ? datosTomiConciliacion.NfFechaEjecucion.Value.ToString(CultureInfo.InvariantCulture)
                        : string.Empty;

                    divisaAdquiereTomi = datosTomiConciliacion.NfDivisaAdquiere;
                    montoAdquiereTomi = datosTomiConciliacion.NfMontoAdquiere;

                    cuentaCreditoTomi = string.Equals(cuentaCreditoForex, "TRANSITORIA", StringComparison.Ordinal)
                        ? "TRANSITORIA"
                        : datosTomiConciliacion.T24CreditAcctNo;

                    divisaEntregaTomi = datosTomiConciliacion.NfDivisaEntrega;
                    montoEntregaTomi = datosTomiConciliacion.NfMontoEntrega;

                    cuentaDebitoTomi = string.Equals(cuentaDebitoForex, "TRANSITORIA", StringComparison.Ordinal)
                        ? "TRANSITORIA"
                        : datosTomiConciliacion.T24DebitAcctNo;
                }

                // Validación.
                Config.EscribirLog("INFO|MFX.procesaOperacion|A validar datos.");

                mensajeError = string.Empty;

                if (regsCms == 1 && regsTomi == 1 && regsForex == 1)
                {
                    // Folio de forex.
                    if (!ValoresIguales(valoresFormato[2], folioForex) ||
                        !ValoresIguales(folioForex, folioForexTomi))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en el número de FOREX. CMS='{valoresFormato[2]}', TOMI='{folioForex}', FOREX='{folioForexTomi}'.";
                    }

                    // Fecha.
                    if (!ValoresIguales(valoresFormato[3], fechaTomi) ||
                        !ValoresIguales(fechaTomi, fechaForex))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en la fecha de ejecución. CMS='{valoresFormato[3]}', TOMI='{fechaTomi}', FOREX='{fechaForex}'.";
                    }

                    // Divisa adquiere.
                    if (!ValoresIguales(valoresFormato[4], divisaAdquiereTomi) ||
                        !ValoresIguales(divisaAdquiereTomi, divisaAdquiereForex))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en la divisa que el cliente adquiere. CMS='{valoresFormato[4]}', TOMI='{divisaAdquiereTomi}', FOREX='{divisaAdquiereForex}'.";
                    }

                    // Monto adquiere.
                    if (!ValoresIguales(valoresFormato[5], montoAdquiereTomi) ||
                        !ValoresIguales(montoAdquiereTomi, montoAdquiereForex))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en el importe que el cliente adquiere. CMS='{valoresFormato[5]}', TOMI='{montoAdquiereTomi}', FOREX='{montoAdquiereForex}'.";
                    }

                    // Cuenta crédito.
                    if (!ValoresIguales(valoresFormato[7], cuentaCreditoTomi) ||
                        !ValoresIguales(cuentaCreditoTomi, cuentaCreditoForex))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en la cuenta de crédito. CMS='{valoresFormato[7]}', TOMI='{cuentaCreditoTomi}', FOREX='{cuentaCreditoForex}'.";
                    }

                    // Divisa entrega.
                    if (!ValoresIguales(valoresFormato[8], divisaEntregaTomi) ||
                        !ValoresIguales(divisaEntregaTomi, divisaEntregaForex))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en la divisa que el cliente entrega. CMS='{valoresFormato[8]}', TOMI='{divisaEntregaTomi}', FOREX='{divisaEntregaForex}'.";
                    }

                    // Monto entrega.
                    if (!ValoresIguales(valoresFormato[9], montoEntregaTomi) ||
                        !ValoresIguales(montoEntregaTomi, montoEntregaForex))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en la importe que el cliente entraga. CMS='{valoresFormato[9]}', TOMI='{montoEntregaTomi}', FOREX='{montoEntregaForex}'.";
                    }

                    // Cuenta débito.
                    if (!ValoresIguales(valoresFormato[10], cuentaDebitoTomi) ||
                        !ValoresIguales(cuentaDebitoTomi, cuentaDebitoForex))
                    {
                        mensajeError += Environment.NewLine +
                            $" - Diferencia en la cuenta de débito. CMS='{valoresFormato[10]}', TOMI='{cuentaDebitoTomi}', FOREX='{cuentaDebitoForex}'.";
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
                    Config.EscribirLog("INFO|MFX.procesaOperacion|La operación está correctamente conciliada.");

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
        /// Inserta o cancela la autorización FOREX en CMS con datos provenientes de TOMI.
        /// </summary>
        private async Task<bool> TOMI_A_CMS(string folio, MFXSolicitudForexTomiDto datosTomi)
        {
            try
            {
                string cuentaDebito = datosTomi.FlgCuentaDebitoCms == true
                    ? "TRANS"
                    : datosTomi.CuentaDebito;

                string cuentaCredito = datosTomi.FlgCuentaCreditoCms == true
                    ? "TRANS"
                    : datosTomi.CuentaCredito;

                if (string.Equals(datosTomi.Status, "CAPTURADO", StringComparison.Ordinal))
                {
                    await _mfxCmsDao.InsertarAutorizacionForexDesdeTomi(
                        datosTomi.IdAutorizacion,
                        datosTomi.IdCliente,
                        datosTomi.DivisaEntregaCliente,
                        datosTomi.DivisaAdquiereCliente,
                        datosTomi.MontoEntregaCliente,
                        datosTomi.MontoAdquiereCliente,
                        datosTomi.EcCliente,
                        datosTomi.FechaValorTexto,
                        cuentaDebito,
                        cuentaCredito,
                        datosTomi.Tipo);
                }
                else if (string.Equals(datosTomi.Status, "ELIMINAR", StringComparison.Ordinal))
                {
                    await _mfxCmsDao.CancelarAutorizacionForex(datosTomi.IdAutorizacion);
                }

                await _mfxTomiDao.MarcarSolicitudForexProcesada(folio);

                return true;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|MFX.TOMI_A_CMS|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }


        /// <summary>
        /// Inserta registro en TOMI con datos provenientes del CMS.
        /// </summary>
        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato)
        {
            try
            {
                MFXFormatoTomiInsertDto datos = new MFXFormatoTomiInsertDto
                {
                    // @NF_FOLIO_UNICO = folio_unico
                    NfFolioUnico = folioUnico,

                    // @NF_TIPO = valoresFormato(19)
                    NfTipo = ConversionHelper.Texto(valoresFormato[19]),

                    // @NF_FOLIO = valoresFormato(20)
                    NfFolio = Convert.ToInt64(valoresFormato[20]),

                    // @T24_CUSTOMER_CODE = valoresFormato(21)
                    T24CustomerCode = ConversionHelper.Texto(valoresFormato[21]),

                    // @T24_DEBIT_ACCT_NO = valoresFormato(10)
                    T24DebitAcctNo = ConversionHelper.Texto(valoresFormato[10]),

                    // @T24_CREDIT_ACCT_NO = valoresFormato(7)
                    T24CreditAcctNo = ConversionHelper.Texto(valoresFormato[7]),

                    // @NF_ESTATUS = "PEND"
                    NfEstatus = "PEND",

                    // @NF_USUARIO_CAPTURA = valoresFormato(24)
                    NfUsuarioCaptura = ConversionHelper.Texto(valoresFormato[24]),

                    // @NF_USUARIO_AUTORIZA = valoresFormato(26)
                    NfUsuarioAutoriza = ConversionHelper.Texto(valoresFormato[26]),

                    // @NF_FECHA_CAPTURA = valoresFormato(23)
                    NfFechaCaptura = Convert.ToDateTime(valoresFormato[23]),

                    // @NF_FECHA_AUTORIZA = valoresFormato(25)
                    NfFechaAutoriza = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormato[25]),

                    // @T24_DEBIT_AMOUNT = valoresFormato(27)
                    T24DebitAmount = Convert.ToDecimal(valoresFormato[27]),

                    // @T24_SHORT_NAME = valoresFormato(1)
                    T24ShortName = ConversionHelper.Texto(valoresFormato[1]),

                    // @NF_AUTORIZACION = valoresFormato(2)
                    NfAutorizacion = ConversionHelper.Texto(valoresFormato[2]),

                    // @NF_FECHA_EJECUCION = valoresFormato(3)
                    NfFechaEjecucion = ConversionHelper.Texto(valoresFormato[3]),

                    // @NF_DIVISA_ADQUIERE = valoresFormato(4)
                    NfDivisaAdquiere = ConversionHelper.Texto(valoresFormato[4]),

                    // @NF_MONTO_ADQUIERE = valoresFormato(5)
                    NfMontoAdquiere = Convert.ToDecimal(valoresFormato[5]),

                    // @NF_EC_RATE = valoresFormato(6)
                    NfEcRate = ConversionHelper.Texto(valoresFormato[6]),

                    // @NF_CUENTA_CREDITO = valoresFormato(7)
                    NfCuentaCredito = ConversionHelper.Texto(valoresFormato[7]),

                    // @NF_DIVISA_ENTREGA = valoresFormato(8)
                    NfDivisaEntrega = ConversionHelper.Texto(valoresFormato[8]),

                    // @NF_MONTO_ENTREGA = valoresFormato(9)
                    NfMontoEntrega = ConversionHelper.Texto(valoresFormato[9]),

                    // @NF_CUENTA_DEBITO = valoresFormato(10)
                    NfCuentaDebito = ConversionHelper.Texto(valoresFormato[10]),

                    // @NF_DESCRIPCION = valoresFormato(11)
                    NfDescripcion = ConversionHelper.Texto(valoresFormato[11])
                };

                bool resultado = await _mfxTomiDao.InsertarFormatoMfxDesdeCms(datos);

                Config.EscribirLog($"INFO|MFX.CMS_A_TOMI|Resultado={resultado}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|MFX.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        private static long NormalizarFolioForex(string folioForex)
        {
            if (string.IsNullOrWhiteSpace(folioForex))
                return 0;

            string folioNormalizado = char.IsDigit(folioForex[0])
                ? folioForex
                : folioForex.Substring(1);

            return Convert.ToInt64(folioNormalizado);
        }

        private static bool ValoresIguales(object valor1, object valor2)
        {
            if (valor1 == null || valor1 == DBNull.Value)
                valor1 = string.Empty;

            if (valor2 == null || valor2 == DBNull.Value)
                valor2 = string.Empty;

            decimal decimal1;
            decimal decimal2;

            string texto1 = Convert.ToString(valor1, CultureInfo.InvariantCulture);
            string texto2 = Convert.ToString(valor2, CultureInfo.InvariantCulture);

            if (decimal.TryParse(texto1, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal1) &&
                decimal.TryParse(texto2, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal2))
            {
                return decimal1 == decimal2;
            }

            return string.Equals(texto1, texto2, StringComparison.Ordinal);
        }

    }
}

using Aplicacion.DTOs.MercadoDinero;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.MercadoDinero;
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
    /// Representa Mercado Dinero.
    /// </summary>
    public class MercadoDinero
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IMercadoDineroTomiDao _mercadoDineroTomiDao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IComunFXD _comunFXD;

        private MW_Tipo_operacion mwTipoOperacion;
        private string folioUnico = string.Empty;
        private string folioPropuesto = string.Empty;

        /// <summary>
        /// Constructor.
        /// </summary>
        public MercadoDinero(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IMercadoDineroTomiDao mercadoDineroTomiDao,
            ConexionT24Mw vwT24,
            IComunFXD comunFXD)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _mercadoDineroTomiDao = mercadoDineroTomiDao;
            _vwT24 = vwT24;
            _comunFXD = comunFXD;
        }

        /// <summary>
        /// Procesa la operación Mercado Dinero del servicio Lectura.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string tipoOperacion, string folio)
        {
            MercadoDineroLiquidacionTomiDto datosLiquidacion;
            MercadoDineroComplementoPagoDto datosComplemento;

            long folioNumerico = Convert.ToInt64(folio, CultureInfo.InvariantCulture);

            string fechaEjecucionTexto = string.Empty;
            string moneda = string.Empty;
            decimal importe = 0m;
            string cuenta = string.Empty;
            string cdl = string.Empty;
            string nombreCliente = string.Empty;
            string calypsoId = string.Empty;
            string externalRef = string.Empty;
            string razonRechazo = string.Empty;
            string sentido = string.Empty;
            string estatus = string.Empty;
            string estatusPar = string.Empty;
            string tipoLiquidacion = string.Empty;
            string tipoOperacionBd = string.Empty;
            string datoCalypso = string.Empty;
            string datosXml = string.Empty;
            string familia = string.Empty;
            string tipoPagoSpeiTexto = string.Empty;

            bool procesado;
            string tipoFolioUnico;
            string tipoOperacionMw;

            string spxxTipoPago = string.Empty;
            string spxxCasfimBene = string.Empty;
            string spxxNomBanco = string.Empty;
            string nombreBeneficiario = string.Empty;
            string spxxTipoCuenta = string.Empty;
            string spxxCuentaBene = string.Empty;
            bool spxxEncontrado = false;

            //Obtiene los datos de la familia de la operación...
            mwTipoOperacion = await _vwT24.ObtenerMWDatosOperacion(tipoOperacion);

            //Verificamos la última operación no realizada...
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

            //Datos del folio en Calypso y TOMI.
            datosLiquidacion = await _mercadoDineroTomiDao.ObtenerLiquidacionPorFolio(folioNumerico);
            if (datosLiquidacion is null)
                throw new Exception($"No se encontraron datos de Mercado Dinero para folio={folio}.");

            if (datosLiquidacion != null)
            {
                tipoOperacionBd = ConversionHelper.Texto(datosLiquidacion.TipoOperacion);
                tipoLiquidacion = ConversionHelper.Texto(datosLiquidacion.TipoLiquidacion);
                cuenta = ConversionHelper.Texto(datosLiquidacion.T24DebitAcctNo);
                cdl = ConversionHelper.Texto(datosLiquidacion.T24CustomerCode);
                nombreCliente = ConversionHelper.Texto(datosLiquidacion.T24CompanyName);
                moneda = _vwT24.ValidaMoneda(ConversionHelper.Texto(datosLiquidacion.T24DebitCurrency));
                importe = datosLiquidacion.T24DebitAmount ?? 0m;
                sentido = ConversionHelper.Texto(datosLiquidacion.Sentido);
                calypsoId = ConversionHelper.Texto(datosLiquidacion.CalypsoId);
                externalRef = ConversionHelper.Texto(datosLiquidacion.ExternalRef);
                estatus = ConversionHelper.Texto(datosLiquidacion.NfEstatus);
                fechaEjecucionTexto = datosLiquidacion.NfFechaEjecucion?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                datoCalypso = ConversionHelper.Texto(datosLiquidacion.DatoCalypso);
                datosXml = ConversionHelper.Texto(datosLiquidacion.DatosXml);

                familia = ConversionHelper.Texto(datosLiquidacion.Familia);

                if (datosLiquidacion.NfTipoPagoSpei.HasValue)
                    tipoPagoSpeiTexto = datosLiquidacion.NfTipoPagoSpei.Value.ToString(CultureInfo.InvariantCulture);
            }

            if (!string.Equals(tipoOperacion, tipoOperacionBd, StringComparison.Ordinal))
            {
                throw new Exception($"Se desea realizar una operación con diferentes tipos de Operación {tipoOperacion}, en base de datos es {tipoOperacionBd}.");
            }


            if (!string.Equals(fechaEjecucionTexto, _configuracion.StrFechaSistema, StringComparison.Ordinal))
            {
                int fechaSistema;
                int intFechaEjecucion;

                bool fechaSistemaValida = int.TryParse(_configuracion.StrFechaSistema, NumberStyles.Integer, CultureInfo.InvariantCulture, out fechaSistema);
                bool fechaEjecucionValida = int.TryParse(fechaEjecucionTexto, NumberStyles.Integer, CultureInfo.InvariantCulture, out intFechaEjecucion);

                if (fechaSistemaValida && fechaEjecucionValida && intFechaEjecucion < fechaSistema)
                {
                    await _controlServicio.MarcarErrorFechaPasada(folioNumerico);
                }

                throw new Exception($"Se desea realizar una operación al {fechaEjecucionTexto} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            if (string.Equals(tipoOperacion, "MD_SPEI_S", StringComparison.Ordinal))
            {
                using (var firma = new MetodosFirma(_configuracion))
                {
                    if (!firma.CertificadoEncontrado())
                        /// <summary>
                        /// Ejecuta la operaciÃ³n Exception.
                        /// </summary>
                        throw new Exception("Error en procesamiento de SPEI: No se encontro el certificado y/o llave para realizar la firma de la transacción");
                }
            }

            //Genero el folio único.
            if (string.Equals(tipoOperacion, "MD_T24", StringComparison.Ordinal))
                tipoFolioUnico = $"{tipoOperacion}_{sentido}";
            else
                tipoFolioUnico = tipoOperacion;

            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoFolioUnico,
                ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucionTexto),
                folio);

            if (string.Equals(tipoOperacion, "MD_SPEI_S", StringComparison.Ordinal))
            {
                folioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                    folioUnico,
                    Convert.ToInt32(ConexionT24Mw.CambiarFechaAJulianaT24(fechaEjecucionTexto)),
                    tipoOperacion);

                if (string.Equals(folioPropuesto.Trim(), "", StringComparison.Ordinal))
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception("Error al obtener el folio propuesto de T24");
            }

            //Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string subTipo;

                try
                {
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

                    subTipo = tipoLiquidacion;

                    if (string.Equals(tipoOperacion, "MD_T24", StringComparison.Ordinal))
                        tipoOperacionMw = $"{tipoOperacion}_{sentido}";
                    else
                        tipoOperacionMw = tipoOperacion;

                    mwTipoOperacion = await _vwT24.ObtenerMWDatosOperacion(tipoOperacionMw);

                    if (string.Equals(tipoLiquidacion, "SPEI", StringComparison.Ordinal) ||
                        string.Equals(tipoLiquidacion, "SPID", StringComparison.Ordinal))
                    {
                        datosComplemento = await _mercadoDineroTomiDao.ObtenerComplementoPagoPorTipoLiquidacionYCdl(tipoLiquidacion, cdl);

                        if (datosComplemento != null)
                        {
                            spxxEncontrado = true;
                            spxxTipoPago = datosComplemento.SpxxTipoPago?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                            spxxCasfimBene = datosComplemento.SpxxCasfimBene?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                            spxxNomBanco = ConversionHelper.Texto(datosComplemento.SpxxNomBanco);
                            nombreBeneficiario = ConversionHelper.Texto(datosComplemento.NomBeneficiario);
                            spxxTipoCuenta = ConversionHelper.Texto(datosComplemento.SpxxTipoCuenta);
                            spxxCuentaBene = ConversionHelper.Texto(datosComplemento.SpxxCuentaBene);
                        }

                        if (spxxEncontrado)
                        {
                            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La datos para registro '{tipoLiquidacion}' son: tipo_pago={spxxTipoPago}, casfim_bene='{spxxCasfimBene}', Nom_banco={spxxNomBanco}, Beneficiario='{nombreBeneficiario}', tipo_cuenta='{spxxTipoCuenta}', Cuenta='{spxxCuentaBene}'.");
                        }
                        else
                        {
                            throw new Exception($"No se encontró información para el tipo_liquidacion='{tipoLiquidacion}', CDL='{cdl}'.");
                        }

                        //Actualizo los datos de la tabla de trabajo.
                        await _mercadoDineroTomiDao.ActualizarDatosComplementariosEnTablaTrabajo(
                            mwTipoOperacion.tabla_trabajo,
                            folioNumerico,
                            datosComplemento.SpxxTipoPago,
                            spxxTipoCuenta,
                            $"{spxxCasfimBene} - {spxxNomBanco}",
                            spxxCuentaBene,
                            nombreBeneficiario);

                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se actualiza los datos de spei|spid con: nf_tipo_pago_spei={spxxTipoPago},nf_tipo_cuenta='{spxxTipoCuenta}',nf_banco_benef='{spxxCasfimBene} - {spxxNomBanco}',t24_ben_acct_no='{spxxCuentaBene}',t24_ben_cust_name='{nombreBeneficiario}'.");
                    }

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Preparando la actualización del plan de trabajo maestro");

                    await _mercadoDineroTomiDao.ActualizarPlanTrabajoMaestroYFolioUnico(
                        folioNumerico,
                        tipoOperacion,
                        folioUnico,
                        Convert.ToInt32(fechaEjecucionTexto, CultureInfo.InvariantCulture),
                        moneda,
                        importe,
                        cdl,
                        nombreCliente,
                        cuenta,
                        subTipo,
                        externalRef);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Actualización del plan de trabajo maestro exitosa");

                    //Proceso para generar la firma digital SPEI.
                    if (string.Equals(tipoOperacion, "MD_SPEI_S", StringComparison.Ordinal))
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se envían parámetros para firma . {folio} {folioPropuesto} {Convert.ToInt16(tipoPagoSpeiTexto, CultureInfo.InvariantCulture)}");

                        if (!await _comunFXD.GeneraFirmaDigital(
                            "tbl_calypso_liquidacion",
                            folio,
                            folioPropuesto,
                            false,
                            Convert.ToInt16(tipoPagoSpeiTexto, CultureInfo.InvariantCulture)))
                        {
                            throw new Exception($"Se presentó un error al generar la firma digital del folio: {folio}");
                        }
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

                    //Verificamos la última operación no realizada.
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
                catch (Exception ex)
                {
                    //Se conserva comportamiento legacy: en DATOS_GRAL se loguea la excepción y no se relanza.
                    Config.EscribirLog($"ERROR|procesaOperacion|Se presenta exception: {ex}");
                }
            }

            //Actualización del estatus.
            if (string.Equals(_configuracion.AliasProceso, "CAMBIO_ESTATUS", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A actualizar estatus en Calypso y TOMI.");

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

                //Aceptado.
                estatus = "X_AUT";

                if ((string.Equals(tipoOperacion, "MD_T24", StringComparison.Ordinal) &&
                     !string.Equals(externalRef, "", StringComparison.Ordinal)) ||
                    string.Equals(tipoLiquidacion, "MT300", StringComparison.Ordinal))
                {
                    estatus = "AUT";
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación será autorizada.");
                }

                await _mercadoDineroTomiDao.ActualizarEstatusCalypsoEnProceso(folioNumerico, estatus);

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Operación actualizada en Calypso como en proceso.");

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

            //Confirma el estatus de la operación por Autorización del usuario.
            if (string.Equals(_configuracion.AliasProceso, "USER_ESTATUS", StringComparison.Ordinal))
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

                if (string.Equals(estatus, "RECC", StringComparison.Ordinal) ||
                    string.Equals(estatus, "AUT", StringComparison.Ordinal))
                {
                    //Operación cancelada u autorizada.
                    procesado = true;
                }
                else
                {
                    await _mercadoDineroTomiDao.MarcarPlanTrabajoEsperandoAutorizacionUsuario(tipoOperacion, folioNumerico);
                    procesado = false;
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
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|En espera de autorización del usuario para continuar.");
                    return false;
                }
            }

            //Verificación del flujo de la información.
            if (string.Equals(_configuracion.AliasProceso, "VALIDAR_FLUJO", StringComparison.Ordinal))
            {
                bool encontradoDup;

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                await _mercadoDineroTomiDao.MarcarPlanTrabajoValidandoInfo(tipoOperacion, folioNumerico);

                procesado = true;
                razonRechazo = "";

                if (string.Equals(estatus, "RECC", StringComparison.Ordinal))
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación está cancelada por lo que no validará duplicidad.");
                }
                else
                {
                    encontradoDup = false;

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|POor validar lo no duplicidad de liquidación.");

                    var duplicidades = await _mercadoDineroTomiDao.ObtenerDuplicidadesLiquidacionPorFolio(folioNumerico);

                    foreach (var duplicidad in duplicidades)
                    {
                        encontradoDup = true;
                        razonRechazo = $"El folio de liquidación {ConversionHelper.Texto(duplicidad.NfFolio)} tiene estatus '{ConversionHelper.Texto(duplicidad.NfEstatus)}', por lo que puede ser una liquidación duplicada, por favor confirme.";
                    }

                    if (encontradoDup)
                    {
                        /// <summary>
                        /// Ejecuta la operaciÃ³n Exception.
                        /// </summary>
                        throw new Exception(razonRechazo);
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Validación exitosa, no parece haber liquidaciones duplicadas para este folio.");
                    }

                    //Validamos el monto, moneda, etc de las referencias externas.
                    if (!string.Equals(externalRef, "", StringComparison.Ordinal))
                    {
                        ValidacionForexResultado resForex = await _vwT24.ValidaDatosForex(
                            externalRef,
                            moneda,
                            string.Equals(cuenta, "", StringComparison.Ordinal) ? "0" : cuenta,
                            importe,
                            sentido);

                        razonRechazo = resForex.RazonRechazo;
                        procesado = resForex.Encontrado;

                        if (!procesado)
                        {
                            //Puede ser un external ref que no es folio, lo dejo pasar.
                            if (string.Equals(razonRechazo, $"El forex '{externalRef}' no existe.", StringComparison.Ordinal))
                            {
                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|El external ref no es un folio valido, pasará la operación.");
                                procesado = true;
                            }
                        }
                    }
                    else
                    {
                        Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|No se pudo realizar validación del External_ref (no existe).");
                    }

                    if (procesado)
                    {
                        //Búsqueda de la segunda pata.
                        if (string.Equals(sentido, "S", StringComparison.Ordinal) &&
                            !(familia.ToUpperInvariant().Contains("SWAP") || familia.ToUpperInvariant().Contains("REPO")))
                        {
                            estatusPar = "";

                            MercadoDineroEstatusParDto datosPar = await _mercadoDineroTomiDao.ObtenerEstatusParPorFolioSalida(folioNumerico);

                            if (datosPar != null)
                                estatusPar = ConversionHelper.Texto(datosPar.NfEstatus);

                            if (!string.Equals(estatusPar, "", StringComparison.Ordinal))
                            {
                                if (string.Equals(estatusPar, "PROC", StringComparison.Ordinal))
                                {
                                    procesado = true;
                                }
                                else
                                {
                                    razonRechazo = "No puede procesarse la salida hasta terminar la entrada";
                                    procesado = false;
                                }
                            }
                        }
                    }
                }

                if (procesado)
                {
                    //Actualizo el estatus de la tabla de trabajo.
                    if (!string.Equals(estatus, "RECC", StringComparison.Ordinal))
                    {
                        estatus = "CONP";
                        await _mercadoDineroTomiDao.ActualizarEstatusCalypsoConp(folioNumerico);
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
                    if (!string.Equals(razonRechazo, "", StringComparison.Ordinal))
                        /// <summary>
                        /// Ejecuta la operaciÃ³n Exception.
                        /// </summary>
                        throw new Exception(razonRechazo);

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|En espera de realizar la operación de entrada para poder procesar la salida.");
                    return false;
                }
            }

            //Verificación de la información SWIFT OUT.
            if (string.Equals(_configuracion.AliasProceso, "SWF_OUT", StringComparison.Ordinal))
            {
                string referenciaSwf;
                string transactionRef;
                string mensajeSwift;

                if (!string.Equals(tipoLiquidacion, "MT300", StringComparison.Ordinal))
                    throw new Exception($"No se puede realizar el SWF_OUT a un mensaje '{tipoLiquidacion}'.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                referenciaSwf = folioUnico;
                transactionRef = calypsoId;
                mensajeSwift = datoCalypso.Replace("N2020}{3:{108", "N}{3:{108");

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inserción del mensaje en MQ de salida, folio='{referenciaSwf}'.");

                procesado = await _mercadoDineroTomiDao.InsertarSwiftOut(
                    referenciaSwf,
                    Convert.ToInt32(fechaEjecucionTexto, CultureInfo.InvariantCulture),
                    moneda,
                    importe,
                    transactionRef,
                    datoCalypso,
                    mensajeSwift);

                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Se actualizaron {(procesado ? 1 : 0)} en TOMI.");

                if (procesado)
                {
                    estatus = "CONP";
                    await _mercadoDineroTomiDao.ActualizarEstatusCalypsoConp(folioNumerico);
                }

                if (procesado)
                {
                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|La operación se insertó correctamente.");

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
                    throw new Exception($"Error en procesamiento de registro SWF_OUT con folio: {folio}");
                }
            }

            //Conciliación.
            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|A conciliar operaciones entre Calypso y TOMI.");

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

                if (string.Equals(tipoOperacion, "MD_UNKNOW", StringComparison.Ordinal) ||
                    string.Equals(tipoLiquidacion, "MT300", StringComparison.Ordinal))
                {
                    procesado = true;
                }
                else
                {
                    if (!(string.Equals(estatus, "CONP", StringComparison.Ordinal) ||
                          string.Equals(estatus, "RECC", StringComparison.Ordinal)))
                    {
                        procesado = false;
                    }
                }

                if (procesado)
                {
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

            return true;
        }

    }
}

using Aplicacion.DTOs.NOM_FXD;
using Aplicacion.Interfaces.CNT;
using Aplicacion.Interfaces.FirmaElectronica;
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
using System.Text;
using System.Threading.Tasks;
using static MUFG_T24.Comunes.Comunes;


namespace Aplicacion.Servicios.Operaciones
{
    /// <summary>
    /// Representa NOMFXD.
    /// </summary>
    public class NOMFXD
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly INOMFXDTomiDao _NOMFXDTomiDao;
        private readonly INOMFXDCmsDao _NOMFXDCmsDao;
        private readonly ConexionT24Mw _vwT24;
        private readonly IComunFXD _comunFXD;

        private const string tipoOperacion = "NOM_FXD";
        private string folioUnico = "";
        private string folioPropuesto = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase NOMFXD.
        /// </summary>

        public NOMFXD(ConfiguracionModelo configuracion, IControlServicio controlServicio, INOMFXDTomiDao nOMFXDTomiDao, INOMFXDCmsDao nOMFXDCmsDao, ConexionT24Mw vwT24, IComunFXD comunFXD)
        {
            _configuracion = configuracion;
            _controlServicio = controlServicio;
            _NOMFXDTomiDao = nOMFXDTomiDao;
            _NOMFXDCmsDao = nOMFXDCmsDao;
            _vwT24 = vwT24;
            _comunFXD = comunFXD;
        }

        /// <summary>
        /// Procesa la operación NOM_FXD del servicio Lectura.
        /// </summary>
        public async Task<bool> ProcesaOperacion(string folio)
        {
            object[] valoresFormatoMto = new object[36];
            object[] valoresFormato = new object[36];

            string mtoFolio = string.Empty;
            int mtoRegs = 0;
            string mtoFechaEjecucion = string.Empty;
            string mtoMoneda = string.Empty;
            decimal mtoImporte = 0m;

            string detFolioForex = string.Empty;
            string detFechaEjecucion = string.Empty;
            string detMoneda = string.Empty;
            decimal detImporte = 0m;

            bool conForex = false;
            bool forexCancelado = false;
            string mtoFolioForex = string.Empty;
            bool conEntradaSalida = true;

            bool procesado;
            bool resultado;

            int tomiMtoRegs = 0;
            decimal tomiMtoImporte = 0m;

            string folioPropuesto;

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
                    throw new Exception("Error en procesamiento de SPEI: No se encontro el certificado y/o llave para realizar la firma de la transacción");
            }

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Inicia proceso folio={folio}.");
            Config.EscribirLog($"INFO|NOM_FXD.procesaOperacion|Leyendo los datos del folio {folio}.");

            // Datos del maestro CMS.
            NomFxdMaestroCmsDto maestroCms = await _NOMFXDCmsDao.ObtenerMaestroNominaPorFolio(folio);

            if (maestroCms != null)
            {
                valoresFormatoMto = CargarValoresFormatoMaestro(maestroCms, folio);

                mtoFolio = ConversionHelper.Texto(maestroCms.id_formato);
                mtoRegs = maestroCms.noPagos ?? 0;
                mtoImporte = maestroCms.montoOperacion ?? 0m;

                mtoFolioForex = string.Empty;

                if (!ConversionHelper.EsValorNuloOVacio(valoresFormatoMto[13]))
                    mtoFechaEjecucion = ConversionHelper.Texto(valoresFormatoMto[13]);

                if (!ConversionHelper.EsValorNuloOVacio(valoresFormatoMto[4]))
                    mtoMoneda = _vwT24.ValidaMoneda(ConversionHelper.Texto(valoresFormatoMto[4]));
            }

            // Datos del detalle CMS.
            NomFxdDetalleCmsDto detalleCms = await _NOMFXDCmsDao.ObtenerDetalleNominaPorFolio(folio);

            if (detalleCms != null)
            {
                valoresFormato = CargarValoresFormatoDetalle(detalleCms, folio);

                if (detalleCms.montoOperacion.HasValue)
                    detImporte = detalleCms.montoOperacion.Value;

                detFolioForex = string.Empty;

                if (!ConversionHelper.EsValorNuloOVacio(valoresFormato[13]))
                    detFechaEjecucion = ConversionHelper.Texto(valoresFormato[13]);

                if (!ConversionHelper.EsValorNuloOVacio(valoresFormato[4]))
                    detMoneda = _vwT24.ValidaMoneda(ConversionHelper.Texto(valoresFormato[4]));
            }

            Config.EscribirLog($"INFO|FXD.procesaOperacion|Veamos que trae la fecha{ConversionHelper.Texto(valoresFormato[13])}-----{mtoFechaEjecucion}");

            int fechaSistema = int.Parse(_configuracion.StrFechaSistema, CultureInfo.InvariantCulture);
            int intFechaEjecucion = int.Parse(mtoFechaEjecucion, CultureInfo.InvariantCulture);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception($"Se desea realizar una operación al {mtoFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            // Genero el folio único y folio propuesto de T24.
            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(mtoFechaEjecucion),
                mtoFolio);

            folioPropuesto = await _vwT24.GeneraFolioPresupuesto(
                folioUnico,
                int.Parse(ConexionT24Mw.CambiarFechaAJulianaT24(mtoFechaEjecucion)),
                tipoOperacion);

            if (string.Equals(folioPropuesto.Trim(), string.Empty, StringComparison.Ordinal))
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("Error al obtener el folio propuesto de T24");

            // Validación de maestro y detalle.
            if (!string.Equals(mtoFolioForex, detFolioForex, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Forex: Maestro:'{mtoFolioForex}', Detalle:'{detFolioForex}'.");

            if (!string.Equals(mtoFechaEjecucion, detFechaEjecucion, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Fecha de ejecución: Maestro:'{mtoFechaEjecucion}', Detalle:'{detFechaEjecucion}'.");

            if (!string.Equals(mtoMoneda, detMoneda, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Moneda: Maestro:'{mtoMoneda}', Detalle:'{detMoneda}'.");

            Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|Validando si la operación está asignada a un Forex.");

            if (!string.Equals(mtoFolioForex, string.Empty, StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|NOM_FXD.procesaOperacion|Asignado el folio forex: {mtoFolioForex} .");

                conForex = true;

                if (ConversionHelper.EsNumerico(mtoFolioForex))
                    throw new Exception($"El Folio del Forex no es valido: {mtoFolioForex}");

                NomFxdForexStatusDto forexStatus = await _NOMFXDTomiDao.ObtenerStatusForexPorExternalRef(mtoFolioForex);

                if (forexStatus != null &&
                    string.Equals(forexStatus.STATUSOPERACION, "CANCELADO", StringComparison.OrdinalIgnoreCase))
                {
                    forexCancelado = true;
                    conEntradaSalida = false;

                    Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|FOREX CANCELADO.");
                }
            }

            // Lectura y actualización de los datos del maestro.
            if (string.Equals(_configuracion.AliasProceso, "DATOS_GRAL", StringComparison.Ordinal))
            {
                string subTipo = string.Empty;

                Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|Actualizando los datos del plan de trabajo maestro (fecha_valor, importe, etc).");

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

                procesado = await _NOMFXDTomiDao.ActualizarPlanTrabajoMaestroLectura(
                    folio,
                    mtoFolio,
                    tipoOperacion,
                    folioUnico,
                    mtoFechaEjecucion,
                    mtoMoneda,
                    detImporte,
                    mtoImporte,
                    ConversionHelper.Texto(valoresFormato[21]),
                    ConversionHelper.Texto(valoresFormato[1]),
                    ConversionHelper.Texto(valoresFormato[22]),
                    subTipo,
                    mtoRegs);

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
                Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|A insertar datos de CMS a TOMI.");

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
                    Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|Se insertó correctamente el folio en base de TOMI.");

                    // Proceso para generar la firma digital.
                    if (!await _comunFXD.GeneraFirmaDigital("tb_tombin_nf_fxd_nomina_detalle", folio, folioPropuesto, true))
                        throw new Exception($"Se presentó un error al generar la firma digital del folio: {folio}");

                    procesado = true;
                }
                else
                {
                    // Posiblemente se restauró el servicio.
                    int numRegs = await _NOMFXDTomiDao.ContarDetalleNominaPorFolio(folio);

                    if (numRegs > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|El folio en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
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
                Config.EscribirLog($"INFO|NOM_FXD.procesaOperacion|A actualizar estatus en CMS y TOMI. Buscando sumarizado de los detalles del folioMaestro={mtoFolio}");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                NomFxdSumarizadoDetalleTomiDto sumarizado = await _NOMFXDTomiDao.ObtenerSumarizadoDetallePorMaestro(mtoFolio);

                if (sumarizado != null)
                {
                    tomiMtoRegs = sumarizado.num_regs ?? 0;
                    tomiMtoImporte = sumarizado.imp_monto ?? 0m;
                }

                procesado = false;

                if (mtoRegs == tomiMtoRegs &&
                    Math.Round(mtoImporte, 2) == Math.Round(tomiMtoImporte, 2))
                {
                    Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|El sumarizado de detalles está completo.");

                    int detalleProcesado = await _NOMFXDTomiDao.ContarDetalleNominaProcesadoPorFolio(folio);

                    if (detalleProcesado > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|El folio ya se había marcado como realizado.");
                    }

                    if (!procesado)
                    {
                        resultado = await CMS_A_TOMI_MAESTRO(mtoFolio, valoresFormatoMto);

                        if (resultado)
                        {
                            Config.EscribirLog($"INFO|NOM_FXD.procesaOperacion|Se insertó correctamente el folio maestro {mtoFolio} en base de TOMI.");
                            procesado = true;
                        }
                        else
                        {
                            int maestroInsertado = await _NOMFXDTomiDao.ContarMaestroNominaPorFolio(mtoFolio);

                            if (maestroInsertado > 0)
                            {
                                procesado = true;
                                Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|El folio maestro en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                            }
                        }
                    }

                    if (procesado)
                    {
                        if (conForex && forexCancelado)
                        {
                            await _NOMFXDCmsDao.MarcarNominaRechazadaPorForexCancelado(
                                mtoFolio,
                                "Formato rechazado",
                                "RECC",
                                "El forex relacionado ha sido cancelado");

                            await _NOMFXDTomiDao.MarcarNominaTomiRechazadaPorForexCancelado(
                                mtoFolio,
                                mtoFolio,
                                "RECC",
                                "El forex relacionado ha sido cancelado");

                            Config.EscribirLog($"INFO|NOM_FXD.procesaOperacion|Forex Rechazado con Anterioridad: {mtoFolio}");

                            Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|A cancelar la operación.");

                            await _NOMFXDTomiDao.CancelarPlanTrabajoPorForexCanceladoDetalle(
                                folio,
                                tipoOperacion,
                                mtoFechaEjecucion,
                                mtoMoneda,
                                detImporte,
                                "El forex relacionado ha sido cancelado");

                            await _NOMFXDTomiDao.CancelarPlanTrabajoPorForexCanceladoMaestro(
                                mtoFolio,
                                tipoOperacion,
                                mtoFechaEjecucion,
                                mtoMoneda,
                                mtoImporte,
                                "El forex relacionado ha sido cancelado");

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

                            await _NOMFXDCmsDao.MarcarNominaAceptada(
                                mtoFolio,
                                "Formato aceptado",
                                "APFT");

                            await _NOMFXDTomiDao.MarcarNominaTomiAceptadaHoy(
                                mtoFolio,
                                mtoFolio,
                                "OPER CMS",
                                fechaConfirma.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture));

                            Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|Operación fecha valor hoy.");
                        }
                        else if (string.CompareOrdinal(mtoFechaEjecucion, _configuracion.StrFechaSistema) < 0)
                        {
                            await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                            throw new Exception($"Se desea realizar una operación al {mtoFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                        }
                        else
                        {
                            DateTime fechaConfirma = DateTime.ParseExact(
                                DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                                "yyyy/MM/dd HH:mm",
                                CultureInfo.InvariantCulture);

                            await _NOMFXDCmsDao.MarcarNominaRecibidaPendienteEjecucion(
                                mtoFolio,
                                "Formato recibido pendiente ejecución",
                                "REPE");

                            await _NOMFXDTomiDao.MarcarNominaTomiFechaFutura(
                                mtoFolio,
                                mtoFolio,
                                "OPER CMS",
                                fechaConfirma);

                            Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|Operación fecha valor fútura.");
                        }

                        await _NOMFXDTomiDao.MarcarProcesosDetalleTerminados(
                            mtoFolio,
                            tipoOperacion,
                            _configuracion.NombreServicio,
                            _configuracion.AliasProceso,
                            _configuracion.Pid.Value,
                            _configuracion.NombreProceso);
                    }
                }
                else
                {
                    Config.EscribirLog($"AVISO|NOM_FXD.procesaOperacion|Faltan detalles a procesar para marcar el cambio de estatus en el folio Maestro:{mtoFolio}.");
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

            // Registro de la Entrada/Salida.
            if (string.Equals(_configuracion.AliasProceso, "TOMI_ENTRADAS/SALIDAS", StringComparison.Ordinal))
            {
                Config.EscribirLog($"INFO|NOM_FXD.procesaOperacion|A insertar datos de Entrada/Salida en TOMI (solo un registro maestro), ReferenciaMto:'{folioUnico}'.");

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

                    int entradasSalidas = await _NOMFXDTomiDao.ContarEntradaSalidaPorReferencia(folioUnico);

                    if (entradasSalidas > 0)
                    {
                        procesado = true;
                        Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                    }

                    if (!procesado)
                    {
                        resultado = await TOMI_ENTRADA_SALIDA_MAESTRO(mtoFolio);

                        if (resultado)
                        {
                            procesado = true;
                            Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|Se insertó correctamente el folio maestro en base de Entrada/Salida.");
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

            // Conciliación.
            if (string.Equals(_configuracion.AliasProceso, "CONCILIAR", StringComparison.Ordinal))
            {
                int regsCms = 1;
                int regsTomi = 0;
                int regsEntSal = 0;

                string mensajeError = string.Empty;

                decimal importeTomi = 0m;
                string fechaEjecucionEntSal = string.Empty;
                string monedaEntSal = string.Empty;
                decimal importeEntSal = 0m;

                Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|A conciliar operaciones entre CMS y TOMI (maestro y detalles).");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                NomFxdConciliacionDetalleTomiDto detalleConciliacion =
                    await _NOMFXDTomiDao.ObtenerDetalleConciliacionPorFolio(folio);

                if (detalleConciliacion != null)
                {
                    regsTomi = 1;
                    importeTomi = detalleConciliacion.T24_DEBIT_AMOUNT ?? 0m;
                }

                NomFxdSumarizadoDetalleTomiDto sumarizadoProcesado =
                    await _NOMFXDTomiDao.ObtenerSumarizadoDetalleProcesadoPorMaestro(mtoFolio);

                if (sumarizadoProcesado != null)
                {
                    tomiMtoRegs = sumarizadoProcesado.num_regs ?? 0;
                    tomiMtoImporte = sumarizadoProcesado.imp_monto ?? 0m;
                }

                if (conEntradaSalida)
                {
                    NomFxdEntradaSalidaConciliacionDto entradaSalida =
                        await _NOMFXDTomiDao.ObtenerEntradaSalidaPorReferencia(folioUnico);

                    if (entradaSalida != null)
                    {
                        regsEntSal = 1;
                        fechaEjecucionEntSal = entradaSalida.ent_fechaValor?.ToString("yyyyMMdd", CultureInfo.InvariantCulture) ?? string.Empty;
                        monedaEntSal = _vwT24.ValidaMoneda(entradaSalida.ent_moneda_cuenta);
                        importeEntSal = entradaSalida.ent_importe ?? 0m;
                    }
                }

                if (regsCms == 1 &&
                    regsTomi == 1 &&
                    ((conEntradaSalida && regsEntSal == 1) || !conEntradaSalida))
                {
                    if (detImporte != importeTomi)
                        mensajeError += $"{Environment.NewLine} - Diferencia en el importe. CMS='{detImporte}', TOMI='{importeTomi}'.";

                    if (mtoRegs != tomiMtoRegs)
                        mensajeError += $"{Environment.NewLine} - Diferencia en el total de registros. CMS='{mtoRegs}', TOMI='{tomiMtoRegs}'.";

                    if (Math.Round(mtoImporte, 2) != Math.Round(tomiMtoImporte, 2))
                        mensajeError += $"{Environment.NewLine} - Diferencia en la sumatoría del importe de registros. CMS='{mtoImporte}', TOMI='{tomiMtoImporte}'.";

                    if (conEntradaSalida)
                    {
                        if (!string.Equals(mtoFechaEjecucion, fechaEjecucionEntSal, StringComparison.Ordinal))
                            mensajeError += $"{Environment.NewLine} - Diferencia en la fecha de ejecución de Entrada/Salida. CMS='{mtoFechaEjecucion}', TOMI='{fechaEjecucionEntSal}'.";

                        if (!string.Equals(mtoMoneda, monedaEntSal, StringComparison.Ordinal))
                            mensajeError += $"{Environment.NewLine} - Diferencia en la monedas de Entrada/Salida. CMS='{mtoMoneda}', TOMI='{monedaEntSal}'.";

                        if (mtoImporte != importeEntSal)
                            mensajeError += $"{Environment.NewLine} - Diferencia en el importe de Entrada/Salida. CMS='{mtoImporte}', Entrada/Salida='{importeEntSal}'.";
                    }

                    procesado = true;
                }
                else
                {
                    procesado = false;
                    mensajeError += $"{Environment.NewLine} - No se ha registrado la operación en TOMI.";
                }

                if (!string.Equals(mensajeError, string.Empty, StringComparison.Ordinal))
                {
                    procesado = false;
                    /// <summary>
                    /// Ejecuta la operaciÃ³n Exception.
                    /// </summary>
                    throw new Exception(mensajeError);
                }

                if (procesado)
                {
                    Config.EscribirLog("INFO|NOM_FXD.procesaOperacion|La operación está correctamente conciliada.");

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

        /// <summary>
        /// Inserta registro en la base de datos de TOMI.
        /// Los datos provienen del CMS - DETALLE.
        /// </summary>
        private async Task<bool> CMS_A_TOMI(string folio, object[] valoresFormato)
        {
            try
            {
                var dto = new NomFxdInsertarDetalleTomiDto
                {
                    NF_FOLIO_UNICO = folioUnico,
                    NF_TIPO = ConversionHelper.Texto(valoresFormato[19]),
                    NF_FOLIO = ConversionHelper.ConvertirNullableDecimal(valoresFormato[20]),
                    T24_CUSTOMER_CODE = ConversionHelper.Texto(valoresFormato[21]),
                    T24_DEBIT_ACCT_NO = ConversionHelper.Texto(valoresFormato[22]),
                    NF_ESTATUS = "PEND",
                    NF_USUARIO_CAPTURA = ConversionHelper.Texto(valoresFormato[24]),
                    NF_USUARIO_AUTORIZA = ConversionHelper.Texto(valoresFormato[26]),
                    NF_FECHA_CAPTURA = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormato[23]),
                    NF_FECHA_AUTORIZA = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormato[25]),
                    T24_DEBIT_AMOUNT = ConversionHelper.ConvertirNullableDecimal(valoresFormato[27]),
                    T24_SHORT_NAME = ConversionHelper.Texto(valoresFormato[1]),
                    NF_FOREX_RELACIONADO = ConversionHelper.Texto(valoresFormato[3]),
                    T24_DEBIT_CURRENCY = ConversionHelper.Texto(valoresFormato[4]),
                    NF_BANCO_BENEF = ConversionHelper.Texto(valoresFormato[6]),
                    T24_BEN_ACCT_NO = ConversionHelper.Texto(valoresFormato[7]),
                    NF_TIPO_CUENTA = ConversionHelper.Texto(valoresFormato[8]),
                    T24_BEN_CUST_NAME = ConversionHelper.Texto(valoresFormato[10]),
                    NF_RFC_BENEFICIARIO = ConversionHelper.Texto(valoresFormato[11]),
                    NF_TASA_RETENCION = ConversionHelper.Texto(valoresFormato[12]),
                    NF_FECHA_EJECUCION = ConversionHelper.ConvertirNullableInt(valoresFormato[13]),
                    T24_DEBIT_THEIR_REF = ConversionHelper.Texto(valoresFormato[15]),
                    NF_MONTO_ENVIAR = ConversionHelper.ConvertirNullableDecimal(valoresFormato[18]),
                    ID_PLANTILLA = ConversionHelper.Texto(valoresFormato[28]),
                    ID_FORMATO_PLANTILLA = ConversionHelper.ConvertirNullableDecimal(valoresFormato[29]),
                    ID_FILES_BTMUM = ConversionHelper.ConvertirNullableDecimal(valoresFormato[30]),
                    NO_PAGOS = ConversionHelper.ConvertirNullableDecimal(valoresFormato[31]),
                    PAGOSDESCRIPCION = ConversionHelper.Texto(valoresFormato[32]),
                    NOPAGOSPROCESADOS = ConversionHelper.ConvertirNullableDecimal(valoresFormato[33]),
                    NOPAGOSDEVUELTOS = ConversionHelper.ConvertirNullableDecimal(valoresFormato[34]),
                    ID_FORMATO_NOMINA = ConversionHelper.ConvertirNullableDecimal(valoresFormato[35])
                };

                bool resultado = await _NOMFXDTomiDao.InsertarFormatoFxdNominaDetalle(dto);

                Config.EscribirLog($"INFO|NOM_FXD.CMS_A_TOMI|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|NOM_FXD.CMS_A_TOMI|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }

        /// <summary>
        /// Inserta registro en la base de datos de TOMI.
        /// Los datos provienen del CMS - MAESTRO.
        /// </summary>
        private async Task<bool> CMS_A_TOMI_MAESTRO(string folioMaestro, object[] valoresFormatoMto)
        {
            try
            {
                var dto = new NomFxdInsertarMaestroTomiDto
                {
                    NF_FOLIO_UNICO = folioUnico,
                    NF_TIPO = ConversionHelper.Texto(valoresFormatoMto[19]),
                    NF_FOLIO = ConversionHelper.ConvertirNullableDecimal(valoresFormatoMto[20]),
                    T24_CUSTOMER_CODE = ConversionHelper.Texto(valoresFormatoMto[21]),
                    T24_DEBIT_ACCT_NO = ConversionHelper.Texto(valoresFormatoMto[22]),
                    NF_ESTATUS = "PEND",
                    NF_USUARIO_CAPTURA = ConversionHelper.Texto(valoresFormatoMto[24]),
                    NF_USUARIO_AUTORIZA = ConversionHelper.Texto(valoresFormatoMto[26]),
                    NF_FECHA_CAPTURA = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormatoMto[23]),
                    NF_FECHA_AUTORIZA = ConversionHelper.ConvertirNullableDateTimeDirect(valoresFormatoMto[25]),
                    T24_DEBIT_AMOUNT = ConversionHelper.ConvertirNullableDecimal(valoresFormatoMto[27]),
                    T24_SHORT_NAME = ConversionHelper.Texto(valoresFormatoMto[1]),
                    NF_FOREX_RELACIONADO = ConversionHelper.Texto(valoresFormatoMto[3]),
                    T24_DEBIT_CURRENCY = ConversionHelper.Texto(valoresFormatoMto[4]),
                    NF_BANCO_BENEF = ConversionHelper.Texto(valoresFormatoMto[6]),
                    T24_BEN_ACCT_NO = ConversionHelper.Texto(valoresFormatoMto[7]),
                    NF_TIPO_CUENTA = ConversionHelper.Texto(valoresFormatoMto[8]),
                    T24_BEN_CUST_NAME = ConversionHelper.Texto(valoresFormatoMto[10]),
                    NF_RFC_BENEFICIARIO = ConversionHelper.Texto(valoresFormatoMto[11]),
                    NF_TASA_RETENCION = ConversionHelper.Texto(valoresFormatoMto[12]),
                    NF_FECHA_EJECUCION = ConversionHelper.ConvertirNullableInt(valoresFormatoMto[13]),
                    T24_DEBIT_THEIR_REF = ConversionHelper.Texto(valoresFormatoMto[15]),
                    NF_MONTO_ENVIAR = ConversionHelper.ObtenerMontoEnviarMaestroDecimal(valoresFormatoMto[18]),
                    ID_PLANTILLA = ConversionHelper.Texto(valoresFormatoMto[28]),
                    ID_FORMATO_PLANTILLA = ConversionHelper.ConvertirNullableDecimal(valoresFormatoMto[29]),
                    ID_FILES_BTMUM = ConversionHelper.ConvertirNullableDecimal(valoresFormatoMto[30]),
                    NO_PAGOS = ConversionHelper.ConvertirNullableDecimal(valoresFormatoMto[31]),
                    PAGOSDESCRIPCION = ConversionHelper.Texto(valoresFormatoMto[32]),
                    NOPAGOSPROCESADOS = ConversionHelper.ConvertirNullableDecimal(valoresFormatoMto[33]),
                    NOPAGOSDEVUELTOS = ConversionHelper.ConvertirNullableDecimal(valoresFormatoMto[34])
                };

                bool resultado = await _NOMFXDTomiDao.InsertarFormatoFxdNominaMaestro(dto);

                Config.EscribirLog($"INFO|NOM_FXD.CMS_A_TOMI_MAESTRO|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|NOM_FXD.CMS_A_TOMI_MAESTRO|Se presenta exception: {ex.Message}. Trace: {ex}");
            }

            return false;
        }


        /// <summary>
        /// Inserta registro de Entradas/Salidas en TOMI - MAESTRO.
        /// </summary>
        private async Task<bool> TOMI_ENTRADA_SALIDA_MAESTRO(string folioMaestro)
        {
            try
            {
                NomFxdNominaMaestroParaEntradaSalidaDto maestro =
                    await _NOMFXDTomiDao.ObtenerNominaMaestroParaEntradaSalida(folioMaestro);

                if (maestro == null)
                {
                    Config.EscribirLog($"ERROR|NOM_FXD.TOMI_ENTRADA_SALIDA_MAESTRO|No se encontró información en tb_tombin_nf_fxd_nomina para nf_folio={folioMaestro}.");
                    return false;
                }

                var dto = new NomFxdEntradaSalidaMaestroDto
                {
                    Referencia = ConversionHelper.Texto(maestro.NF_FOLIO_UNICO),
                    Num_Cliente = ConversionHelper.ConvertirNullableInt(maestro.T24_CUSTOMER_CODE),
                    CDL = ConversionHelper.ConvertirNullableDecimal(maestro.T24_CUSTOMER_CODE),
                    Ordenante = ConversionHelper.Texto(maestro.T24_COMPANY_NAME),
                    Operacion = 218,
                    OperacionDesc = "Salida  (TTS)",
                    Tipo = "D",
                    TipoDesc = "Doméstica",

                    Cuenta = ConversionHelper.ConvertirNullableDecimal(maestro.T24_DEBIT_ACCT_NO),
                    Nombre = ConversionHelper.Texto(maestro.NF_NOMBRE_CUENTA),
                    CuentaMoneda = ConversionHelper.Texto(maestro.NF_MONEDA_CUENTA),

                    Via = 800,
                    ViaDesc = "TRF ENVIADA MXN SPEUA",

                    Importe = maestro.T24_DEBIT_AMOUNT,
                    Saldo = 0,
                    Comentarios = ConversionHelper.Texto(maestro.T24_DEBIT_THEIR_REF),

                    Fecha_Captura = ConversionHelper.TextoToFechaConvert(_configuracion.StrFechaSistema),
                    FechaValor = ConversionHelper.TextoToFechaConvert(ConversionHelper.Texto(maestro.NF_FECHA_EJECUCION)),

                    Hora = DateTime.Now.ToLongTimeString(),
                    Capturista = "JEAI",
                    Fad = 0,
                    Referencia_Control = "A",
                    Referencia_Control_Cont = 0
                };

                bool resultado = await _NOMFXDTomiDao.InsertarEntradaSalidaMaestro(dto);

                Config.EscribirLog($"INFO|NOM_FXD.TOMI_ENTRADA_SALIDA_MAESTRO|Salida={(resultado ? 0 : -1)}.");

                return resultado;
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|NOM_FXD.TOMI_ENTRADA_SALIDA_MAESTRO|Se presenta exception: {ex.Message}.Trace:{ex}");
            }

            return false;
        }

        private static object[] CargarValoresFormatoMaestro(NomFxdMaestroCmsDto maestro, string folio)
        {
            object[] valoresFormatoMto = new object[36];

            valoresFormatoMto[19] = "FXD";
            valoresFormatoMto[20] = maestro.id_formato;
            valoresFormatoMto[21] = maestro.cdl;
            valoresFormatoMto[22] = maestro.id_cuenta;
            valoresFormatoMto[23] = maestro.fechaCaptura;
            valoresFormatoMto[24] = maestro.id_usuarioCaptura;
            valoresFormatoMto[25] = maestro.fechaAutorizacion;
            valoresFormatoMto[26] = maestro.id_usuarioAutorizo;
            valoresFormatoMto[27] = maestro.montoOperacion;
            valoresFormatoMto[28] = maestro.id_plantilla;
            valoresFormatoMto[29] = maestro.id_formato_plantilla;
            valoresFormatoMto[30] = maestro.id_files_btmum;
            valoresFormatoMto[31] = maestro.noPagos;
            valoresFormatoMto[32] = maestro.pagosDescripcion;
            valoresFormatoMto[33] = maestro.noPagosProcesados;
            valoresFormatoMto[34] = maestro.noPagosDevueltos;

            string contenidoFormato = maestro.contenidoFormato ?? string.Empty;
            string[] valoresPipes = contenidoFormato.Split('|');

            if (valoresPipes.Length < 19)
            {
                throw new Exception(
                    string.Format(
                        "El contenidoFormato del folio {0} es diferente al esperado, debe ser 18 datos y se tienen {1}.",
                        folio,
                        valoresPipes.Length - 1));
            }

            for (int idxPipes = 0; idxPipes <= 18; idxPipes++)
            {
                string[] llaveValor = valoresPipes[idxPipes].Split('=');

                if (llaveValor.Length < 2)
                {
                    throw new Exception(
                        string.Format(
                            "El contenidoFormato del folio {0} contiene un valor inválido en la posición {1}: '{2}'.",
                            folio,
                            idxPipes,
                            valoresPipes[idxPipes]));
                }

                valoresFormatoMto[idxPipes] = llaveValor[1];
            }

            return valoresFormatoMto;
        }
        private static object[] CargarValoresFormatoDetalle(NomFxdDetalleCmsDto detalle, string folio)
        {
            object[] valoresFormato = new object[36];

            valoresFormato[19] = "FXD";
            valoresFormato[20] = detalle.id_formato;
            valoresFormato[21] = detalle.cdl;
            valoresFormato[22] = detalle.id_cuenta;
            valoresFormato[23] = detalle.fechaCaptura;
            valoresFormato[24] = detalle.id_usuarioCaptura;
            valoresFormato[25] = detalle.fechaAutorizacion;
            valoresFormato[26] = detalle.id_usuarioAutorizo;
            valoresFormato[27] = detalle.montoOperacion;
            valoresFormato[28] = detalle.id_plantilla;
            valoresFormato[29] = detalle.id_formato_plantilla;
            valoresFormato[30] = detalle.id_files_btmum;
            valoresFormato[31] = detalle.nopagos;
            valoresFormato[32] = detalle.pagosdescripcion;
            valoresFormato[33] = detalle.nopagosprocesados;
            valoresFormato[34] = detalle.nopagosdevueltos;
            valoresFormato[35] = detalle.id_formato_nomina;

            string contenidoFormato = detalle.contenidoFormato ?? string.Empty;
            string[] valoresPipes = contenidoFormato.Split('|');

            if (valoresPipes.Length < 19)
            {
                throw new Exception(
                    string.Format(
                        "El contenidoFormato del folio {0} es diferente al esperado, debe ser 18 datos y se tienen {1}.",
                        folio,
                        valoresPipes.Length - 1));
            }

            for (int idxPipes = 0; idxPipes <= 18; idxPipes++)
            {
                string[] llaveValor = valoresPipes[idxPipes].Split('=');

                if (llaveValor.Length < 2)
                {
                    throw new Exception(
                        string.Format(
                            "El contenidoFormato del folio {0} contiene un valor inválido en la posición {1}: '{2}'.",
                            folio,
                            idxPipes,
                            valoresPipes[idxPipes]));
                }

                valoresFormato[idxPipes] = llaveValor[1];
            }

            return valoresFormato;
        }


    }
}

using Aplicacion.DTOs.PaP_FXI;
using Aplicacion.Interfaces.PaP;
using Aplicacion.Interfaces.PaP_FXI;
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
    /// Representa Pa PFXI.
    /// </summary>
    public class PaPFXI
    {
        private readonly ConfiguracionModelo _configuracion;
        private readonly IControlServicio _controlServicio;
        private readonly IPaPFXITomiDao _tomiDao;
        private readonly IPaPFXICmsDao _cmsDao;
        private readonly ConexionT24Mw _vwT24;

        private const string tipoOperacion = "PaP_FXI";
        private string folioUnico = string.Empty;
        /// <summary>
        /// Inicializa una nueva instancia de la clase PaPFXI.
        /// </summary>

        public PaPFXI(
            ConfiguracionModelo configuracion,
            IControlServicio controlServicio,
            IPaPFXITomiDao tomiDao,
            IPaPFXICmsDao cmsDao,
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
            object[] valoresFormatoMto = new object[46];
            object[] valoresFormato = new object[57];

            string mtoFolio = string.Empty;
            int mtoRegs = 0;
            string mtoFechaEjecucion = string.Empty;
            string mtoMoneda = string.Empty;
            decimal mtoImporte = 0;

            string detFechaEjecucion = string.Empty;
            string detMoneda = string.Empty;
            decimal detImporte = 0;
            string detFolioForex = string.Empty;

            bool conForex = false;
            bool forexCancelado = false;
            bool conEntradaSalida = true;
            bool procesado;
            bool fechaFutura;
            bool resultado;

            int tomiMtoRegs = 0;
            decimal tomiMtoImporte = 0;

            string mtoFolioForex = string.Empty;

            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Iniciando procesamiento del folio {folio}.");

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

            // =========================================
            // Datos del maestro CMS
            // =========================================
            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Obteniendo datos maestro CMS.");

            var maestroCms = await _cmsDao.ObtenerMaestroCmsPorFolioDetalle(folio);

            if (maestroCms != null)
            {
                valoresFormatoMto = CargarValoresFormatoMaestro(maestroCms);

                mtoFolio = maestroCms.IdFormato.ToString();
                mtoRegs = Convert.ToInt32(maestroCms.NoPagos);
                mtoImporte = maestroCms.MontoOperacion;

                mtoFolioForex = string.Empty;

                if (valoresFormatoMto[13] != null && valoresFormatoMto[13] != DBNull.Value)
                    mtoFechaEjecucion = DameTexto(valoresFormatoMto[13]);

                if (valoresFormatoMto[3] != null && valoresFormatoMto[3] != DBNull.Value)
                    mtoMoneda = _vwT24.ValidaMoneda(DameTexto(valoresFormatoMto[3]));
            }

            // =========================================
            // Datos del detalle CMS
            // =========================================
            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Obteniendo datos detalle CMS.");

            var detalleCms = await _cmsDao.ObtenerDetalleCmsPorFolio(folio);

            if (detalleCms != null)
            {
                valoresFormato = CargarValoresFormatoDetalle(detalleCms);

                if (detalleCms.MontoOperacion != 0)
                    detImporte = detalleCms.MontoOperacion;

                detFolioForex = string.Empty;

                if (valoresFormato[13] != null && valoresFormato[13] != DBNull.Value)
                    detFechaEjecucion = DameTexto(valoresFormato[13]);

                if (valoresFormato[3] != null && valoresFormato[3] != DBNull.Value)
                    detMoneda = _vwT24.ValidaMoneda(DameTexto(valoresFormato[3]));
            }

            // =========================================
            // Validaciones legacy previas
            // =========================================
            int fechaSistema = int.Parse(_configuracion.StrFechaSistema);
            int intFechaEjecucion = int.Parse(mtoFechaEjecucion);
            if (intFechaEjecucion < fechaSistema)
            {
                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                throw new Exception($"Se desea realizar una operación al {mtoFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
            }

            if (!string.Equals(mtoFolioForex, detFolioForex, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Forex: Maestro:'{mtoFolioForex}', Detalle:'{detFolioForex}'.");

            if (!string.Equals(mtoFechaEjecucion, detFechaEjecucion, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Fecha de ejecución: Maestro:'{mtoFechaEjecucion}', Detalle:'{detFechaEjecucion}'.");

            if (!string.Equals(mtoMoneda, detMoneda, StringComparison.Ordinal))
                throw new Exception($"Deferencia entre el maestro y el detalle de folio de Moneda: Maestro:'{mtoMoneda}', Detalle:'{detMoneda}'.");

            if (DameTexto(valoresFormato[51]) == string.Empty)
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("No se tiene información de COUNTRY_BEN para realizar el pago.");

            bool requiredTownNameForPaP = _configuracion.BoRequiredTownNameForPaP ?? false;
            if (requiredTownNameForPaP &&
                string.IsNullOrWhiteSpace(DameTexto(valoresFormato[52])))
            {
                /// <summary>
                /// Ejecuta la operaciÃ³n Exception.
                /// </summary>
                throw new Exception("No se tiene información de CITY_BEN para realizar el pago.");
            }

            folioUnico = await _vwT24.GeneraFolioUnico(
                tipoOperacion,
                ConexionT24Mw.CambiarFechaAJulianaT24(mtoFechaEjecucion),
                folio);

            fechaFutura = string.Compare(mtoFechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal) > 0;

            // =========================================
            // Validación Forex
            // =========================================
            Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Validando si la operación está asignada a un Forex.");

            if (!string.IsNullOrEmpty(mtoFolioForex))
            {
                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Asignado el folio forex: {mtoFolioForex}.");

                conForex = true;

                if (ConversionHelper.EsNumerico(mtoFolioForex))
                    throw new Exception($"El Folio del Forex no es valido: {mtoFolioForex}");

                var forex = await _tomiDao.ObtenerStatusForexPorExternalRef(mtoFolioForex);

                if (forex != null && forex.StatusOperacion == "CANCELADO")
                {
                    forexCancelado = true;
                    conEntradaSalida = false;

                    Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|FOREX CANCELADO.");
                }
            }

            // =========================================
            // DATOS_GRAL
            // =========================================
            if (_configuracion.AliasProceso == "DATOS_GRAL")
            {
                string subTipo = string.Empty;

                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|Actualizando los datos del plan de Trabajo maestro.");

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

                resultado = await _tomiDao.ActualizarPlanTrabajoDatosGral(
                    folio,
                    mtoFolio,
                    tipoOperacion,
                    folioUnico,
                    mtoFechaEjecucion,
                    mtoMoneda,
                    detImporte,
                    mtoImporte,
                    DameTexto(valoresFormato[31]),
                    DameTexto(valoresFormato[1]),
                    DameTexto(valoresFormato[32]),
                    mtoRegs,
                    subTipo);

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

            // =========================================
            // CMS_A_TOMI
            // =========================================
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
                    int registros = await _tomiDao.ContarDetallePapTomiPorFolio(folio);

                    if (registros > 0)
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

            // =========================================
            // TOMI_ENTRADAS/SALIDAS
            // =========================================
            if (_configuracion.AliasProceso == "TOMI_ENTRADAS/SALIDAS")
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|A insertar datos de Entrada/Salida en TOMI.");

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

                    int entradasSalidas = await _tomiDao.ContarEntradasSalidasPorReferencia(folioUnico);

                    if (entradasSalidas > 0)
                    {
                        procesado = true;

                        Config.EscribirLog("INFO|PaP_FXI.procesaOperacion|El folio en procesamiento ya se había insertado como Entrada/Salida pero no se marcó el proceso como realizado.");
                    }

                    if (!procesado)
                    {
                        resultado = await TOMI_ENTRADA_SALIDA(folio);

                        if (resultado)
                        {
                            procesado = true;

                            Config.EscribirLog(
                                $"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio detalle en base de Entrada/Salida.");
                        }
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
            }

            // =========================================
            // CAMBIO_ESTATUS
            // =========================================
            if (_configuracion.AliasProceso == "CAMBIO_ESTATUS")
            {
                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|A actualizar estatus en CMS y TOMI. Buscando sumarizado de los detalles del folioMaestro={mtoFolio}");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
                    TipoOperacion = tipoOperacion,
                    Servicio = _configuracion.NombreServicio,
                    NombreProceso = _configuracion.NombreProceso,
                    AliasProceso = _configuracion.AliasProceso,
                    PId = _configuracion.Pid.Value
                });

                var sumarizado = await _tomiDao.ObtenerSumarizadoDetallePorMaestro(mtoFolio);

                if (sumarizado != null)
                {
                    tomiMtoRegs = sumarizado.NumRegs;
                    tomiMtoImporte = sumarizado.ImpMonto ?? 0;
                }

                procesado = false;

                if (mtoRegs == tomiMtoRegs &&
                    Math.Round(mtoImporte, 2) == Math.Round(tomiMtoImporte, 2))
                {
                    int detallesNoPendientes =
                        await _tomiDao.ContarDetallePapTomiNoPendientePorFolio(folio);

                    if (detallesNoPendientes > 0)
                    {
                        procesado = true;

                        Config.EscribirLog(
                            $"INFO|{tipoOperacion}.procesaOperacion|El folio ya se había marcado como realizado.");
                    }

                    if (!procesado)
                    {
                        resultado = await CMS_A_TOMI_MAESTRO(mtoFolio, valoresFormatoMto);

                        if (resultado)
                        {
                            Config.EscribirLog(
                                $"INFO|{tipoOperacion}.procesaOperacion|Se insertó correctamente el folio maestro {mtoFolio} en base de TOMI.");

                            procesado = true;
                        }
                        else
                        {
                            int maestros = await _tomiDao.ContarMaestroPapTomiPorFolio(mtoFolio);

                            if (maestros > 0)
                            {
                                procesado = true;

                                Config.EscribirLog(
                                    $"INFO|{tipoOperacion}.procesaOperacion|El folio maestro en procesamiento ya se había insertado en base TOMI pero no se marcó el proceso como realizado.");
                            }
                        }

                        if (procesado)
                        {
                            if (conForex && forexCancelado)
                            {
                                await _cmsDao.MarcarRechazoForexCanceladoCms(mtoFolio);
                                await _tomiDao.MarcarRechazoForexCanceladoTomi(mtoFolio);

                                Config.EscribirLog(
                                    $"INFO|{tipoOperacion}.procesaOperacion|Forex Rechazado con Anterioridad: {mtoFolio}");

                                await _tomiDao.CancelarPlanTrabajoDetallePorForex(
                                    folio,
                                    tipoOperacion,
                                    mtoFechaEjecucion,
                                    mtoMoneda,
                                    detImporte);

                                string folioMaestroPlanTrabajo = await _vwT24.ObtenerFolioMaestro(folio, tipoOperacion);

                                await _tomiDao.CancelarPlanTrabajoMaestroPorForex(
                                    folioMaestroPlanTrabajo,
                                    tipoOperacion,
                                    mtoFechaEjecucion,
                                    mtoMoneda,
                                    mtoImporte);

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

                            if (mtoFechaEjecucion == _configuracion.StrFechaSistema)
                            {
                                DateTime fechaConfirma = DateTime.ParseExact(
                                    DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                                    "yyyy/MM/dd HH:mm",
                                    CultureInfo.InvariantCulture);

                                await _cmsDao.MarcarAceptadoHoyCms(mtoFolio);

                                await _tomiDao.MarcarAceptadoHoyTomi(
                                    mtoFolio,
                                    "OPER CMS",
                                    fechaConfirma);

                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Operación fecha valor hoy.");
                            }
                            else if (string.Compare(mtoFechaEjecucion, _configuracion.StrFechaSistema, StringComparison.Ordinal) < 0)
                            {
                                await _controlServicio.MarcarErrorFechaPasada(long.Parse(folio));
                                throw new Exception(
                                    $"Se desea realizar una operación al {mtoFechaEjecucion} cuando la fecha del sistema es {_configuracion.StrFechaSistema}.");
                            }
                            else
                            {
                                DateTime fechaConfirma = DateTime.ParseExact(
                                    DateTime.Now.ToString("yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture),
                                    "yyyy/MM/dd HH:mm",
                                    CultureInfo.InvariantCulture);

                                await _cmsDao.MarcarFechaFuturaCms(mtoFolio);

                                await _tomiDao.MarcarFechaFuturaTomi(
                                    mtoFolio,
                                    "OPER CMS",
                                    fechaConfirma);

                                Config.EscribirLog($"INFO|{tipoOperacion}.procesaOperacion|Operación fecha valor fútura.");
                            }
                        }
                    }
                }
                else
                {
                    Config.EscribirLog(
                        $"AVISO|{tipoOperacion}.procesaOperacion|Faltan detalles a procesar para marcar el cambio de estatus en el folio Maestro:{mtoFolio}.");

                    return true;
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

            // =========================================
            // CONCILIAR
            // =========================================
            if (_configuracion.AliasProceso == "CONCILIAR")
            {
                int regsCms = 1;
                int regsTomi = 0;
                int regsEntSal = 0;

                string mensajeError = string.Empty;

                decimal importeTomi = 0;
                string fechaEjecucionEntSal = string.Empty;
                string monedaEntSal = string.Empty;
                decimal importeEntSal = 0;

                Config.EscribirLog(
                    $"INFO|{tipoOperacion}.procesaOperacion|A conciliar operaciones entre CMS y TOMI.");

                await _controlServicio.IniciaProceso(new ProcesoDTO
                {
                    Folio = folio,
