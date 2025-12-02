Public Class PACS009
    Private Const tipoOperacion As String = "PACS009"
    Private Shared goMW_TipoOperacion As T24_MW_Connection.MW_Tipo_operacion
    'Private Shared folio_unico As String = ""

    Shared Function procesaOperacion(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        Dim lsAliasProceso As String
        Dim lbProcesado As Boolean
        Dim lsCuentaVostroBene As String

        'Confirmo que se encuentre el certificado y llave para la firma digital de lo contrario no se inicia el proceso
        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Confirmando que el certificado se encuentre.")
        If Not CertificadoEncontrado() Then
            Throw New Exception("Error en procesamiento de SPEI: No se encontro el certificado y/o llave para realizar la firma de la transacción")
        End If

        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Iniciando el registro de la operación, folio_unico='" & datos("folio_unico") & "'.")

        ' Tipo de Operacion
        goMW_TipoOperacion = goT24_Connection.MW_DatosOperacion("SWF_IN")

        ' Proceso a ejecutar
        lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)

        '' Registro de la operación en T24
        If lsAliasProceso = "SPEI_REGISTRO" Then
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en SPEI (si aplica).")

            ' Inicio el proceso
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)

            ' Valido el tipo de operacion PACS009 (hay caso para salida SPEI y caso de traspaso entre cuentas vostro internas)
            lsCuentaVostroBene = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_vostro")
            If lsCuentaVostroBene = "" Then
                ''''' SPEI
                write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en SPEI.")
                ' SPEI
                lbProcesado = registra_SPEI(icn, datos)

                If lbProcesado = False And Not IsNothing(gexUltimaExcepcion) Then
                    Throw gexUltimaExcepcion
                End If

                If Not lbProcesado Then
                    ' No se registro la operacion SPEI
                    Throw New Exception("No se pudo registrar la operación en SPEI")
                End If
            Else
                ''''' Traspaso
                write_Log("INFO|" & tipoOperacion & ".procesaOperacion|No aplica el registrar operación en SPEI.")
            End If

            ' Termino del proceso
            terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, "SPEI_REGISTRO")

            ' Nuevo alias del proceso
            lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)
        End If

        '' Registro de la operación en T24
        If lsAliasProceso = "T24_REGISTRO" Then
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en T24.")

            ' Inicio el proceso
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)

            ' Valido el tipo de operacion PACS009 (hay caso para salida SPEI y caso de traspaso entre cuentas vostro internas)
            lsCuentaVostroBene = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_vostro")
            If lsCuentaVostroBene = "" Then
                ''''' SPEI
                ' Valido la operacion en T24
                write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en T24 con salida SPEI.")
                lbProcesado = T24_REGISTRO(icn, datos)
                If lbProcesado = False And Not IsNothing(gexUltimaExcepcion) Then
                    Throw gexUltimaExcepcion
                End If
                If Not lbProcesado Then
                    Throw New Exception("Error en procesamiento de registro PACS009 T24_REGISTRO con folio: " & icn)
                End If
            Else
                ''''' Traspaso
                write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en T24 como traspaso entre VOSTROS.")
                lbProcesado = T24_REGISTRO_VOSTROS(icn, datos)
                If lbProcesado = False And Not IsNothing(gexUltimaExcepcion) Then
                    Throw gexUltimaExcepcion
                End If
                If Not lbProcesado Then
                    Throw New Exception("Error en procesamiento de registro PACS009 T24_REGISTRO_VOSTROS con folio: " & icn)
                End If
            End If
        End If

        ' Conciliación
        If lsAliasProceso = "CONCILIAR" Then
            Dim lsFolioT24 As String = String.Empty
            Dim lbEs_Igual_en_T24 As Boolean
            Dim lsMoneda As String
            Dim lrsDatos As DbDataReader
            Dim lsCuentaCompletaVostro As String, laCuentaSeparadaVostro() As String, lsCuentaDeb As String
            Dim lsBen_Subaplicacion As String, lsBen_Cuenta As String

            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A conciliar operaciones (por estatus) entre TOMI y T24.")
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)
            write_Log("INFO|Folio Unico " & datos("folio_unico"))

            ' Busco la operacion en T24
            lrsDatos = goTOMI_Database.Execute_Query("SELECT T24_TRANSACTION_ID FROM TB_SRV_SWF_TRABAJO_MX WHERE NF_FOLIO_UNICO = '" & datos("folio_unico") & "'")
            If Not IsNothing(lrsDatos) Then
                While lrsDatos.Read
                    If Not IsDBNull(lrsDatos("T24_TRANSACTION_ID")) Then
                        lsFolioT24 = lrsDatos("T24_TRANSACTION_ID").ToString
                    End If
                End While
                lrsDatos.Close()
                lrsDatos = Nothing
            End If

            If IsNothing(lsFolioT24) Then
                Throw New Exception("No existe folio asignado para el folio_unico='" & datos("folio_unico") & "'")
            End If

            ' Valida contra los datos de TOMI - T24
            lbEs_Igual_en_T24 = False
            lsMoneda = goT24_Connection.valida_moneda(datos("cve_moneda"))

            ' Ordenante
            lsCuentaCompletaVostro = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_emisor")
            laCuentaSeparadaVostro = lsCuentaCompletaVostro.Split("-")
            lsCuentaDeb = laCuentaSeparadaVostro(2)      'datos("num_cuenta")

            ' Beneficiario
            lsCuentaVostroBene = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_vostro")
            If lsCuentaVostroBene = "" Then
                ''''' SPEI
                lsBen_Cuenta = goT24_Connection.T24_BanxicoAcct
            Else
                ''''' Traspaso
                laCuentaSeparadaVostro = lsCuentaVostroBene.Split("-")
                lsBen_Subaplicacion = laCuentaSeparadaVostro(1)
                lsBen_Cuenta = laCuentaSeparadaVostro(2)
            End If

            lrsDatos = goVistasT24_Database.Execute_Query("SELECT CUR_ID,DEBIT_ACCT_NO,DEBIT_CURRENCY,DEBIT_AMOUNT,CREDIT_ACCT_NO,SATEL_SYS_REF FROM VW_T24_FUNDS_TRANSFER_ID WHERE CUR_ID = '" & lsFolioT24 & "'")
            If Not IsNothing(lrsDatos) Then
                While lrsDatos.Read
                    If (lsFolioT24 = lrsDatos.Item("CUR_ID") And
                            datos("folio_unico") = lrsDatos.Item("SATEL_SYS_REF") And
                            datos("des_importe") = lrsDatos.Item("DEBIT_AMOUNT") And
                            lsMoneda = lrsDatos.Item("DEBIT_CURRENCY") And
                            lsCuentaDeb = lrsDatos.Item("DEBIT_ACCT_NO") And
                            lsBen_Cuenta = lrsDatos.Item("CREDIT_ACCT_NO")) Then
                        lbEs_Igual_en_T24 = True
                    End If
                End While
                lrsDatos.Close()
                lrsDatos = Nothing
            End If

            If Not lbEs_Igual_en_T24 Then
                Throw New Exception("Los datos de conciliación entre TOMI y T24 son diferentes")
            End If

            ' Actualizo el estatus
            terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)

            ' Marca la operación como finalizada total
            marca_op_terminada(icn, "PROC", "JEAI")

            ' Verificamos la última operación no realizada
            lsAliasProceso = ""
        End If

        ' Termina el proceso del folio
        Return True
    End Function

    ' Inserta registro en T24
    Private Shared Function T24_REGISTRO(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        Try
            Dim loTrxMaster As T24_MW_TrxMaster = Nothing
            Dim loTrxDetailFT As T24_MW_TrxDetail = Nothing
            Dim lsMoneda As String, lsDescripcion As String
            Dim lsBanco As String, lsNombre_Beneficiario As String, lsNumero_Cuenta_Beneficiario As String
            Dim lsCuentaCompletaVostro As String, laCuentaSeparadaVostro() As String, lsCuentaDeb As String
            Dim lbRegreso As Boolean
            Dim lsSW_END2END_REF As String, lsSLA_UETR As String

            write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Inicia registro en T24.")

            ' Inicio de variables
            lsMoneda = goT24_Connection.valida_moneda(datos("cve_moneda"))
            lsDescripcion = busca_valor_xml(datos("des_variables_xml"), "related_reference")
            lsBanco = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_casfim_spei") & " - " & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_banco_spei")
            lsNombre_Beneficiario = busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_banco_spei")
            lsNumero_Cuenta_Beneficiario = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_casfim_spei")
            lsCuentaCompletaVostro = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_emisor")
            laCuentaSeparadaVostro = lsCuentaCompletaVostro.Split("-")
            lsCuentaDeb = laCuentaSeparadaVostro(2)      'datos("num_cuenta")
            'Armado de la descricpión
            lsDescripcion = "RECEPCION TRANSFERENCIA INTERNACIONAL PACS009 SALIDA SPEI."
            If busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor") <> "" Then
                lsDescripcion &= ", BANCO ORIGEN: " & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor")
            End If
            ' Vostro
            If busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_vostro") <> "" Then
                lsDescripcion &= ", BANCO DESTINO: " & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_vostro")
            End If
            'SPEI
            If busca_valor_xml(datos("des_variables_xml"), "btmum_cve_casfim_spei") <> "" Then
                lsDescripcion &= ", PAGO SPEI A: " & busca_valor_xml(datos("des_variables_xml"), "btmum_cve_casfim_spei") & "-" & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_banco_spei")
            End If
            ' Referencia
            If busca_valor_xml(datos("des_variables_xml"), "related_reference") <> "" Then
                lsDescripcion &= ", REL. REFERENCE: " & busca_valor_xml(datos("des_variables_xml"), "related_reference")
            End If

            ' Validación de la moneda
            If lsMoneda <> goT24_Connection.MonedaDefaultMN Then
                Throw New Exception("No hay referencia del mensaje PACS009 para la moneda '" & lsMoneda & "'.")
            End If

            ' Prepatamos los datos para el registro del FT
            loTrxMaster = New T24_MW_TrxMaster
            With loTrxMaster
                '.TrxCompany_ID = T24_MW_Connection.MW_CompanyId
                '.TrxSystem_ID = T24_MW_Connection.MW_SystemId
                .TrxOperation_Specific_Type = "SWF_IN"
                .TrxOperation_SubType = "PACS009_SPEI"
                .TrxOperation_Folio = icn
                .TrxOperation_Acct = lsCuentaDeb
            End With

            loTrxDetailFT = New T24_MW_TrxDetail
            With loTrxDetailFT
                .debit_acct_no = lsCuentaDeb
                .debit_currency = lsMoneda
                .debit_amount = String.Format("{0:0.##}", datos("des_importe"))
                .debit_their_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .credit_their_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .credit_acct_no = goT24_Connection.T24_BanxicoAcct
                .credit_currency = lsMoneda
                '.credit_amount = lrsDatos.Item("nf_monto_enviar")
                '.ben_acct_no = lsNumero_Cuenta_Beneficiario
                '.ben_customer = lsNombre_Beneficiario
                '.payment_details = lsDescripcion
                .ordering_cust = busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor")
                .related_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .Instructions = lsDescripcion   'goT24_Connection.convierteTexto_a_Multivalor(lsDescripcion, 65)
                .Satel_Sys_Ref = datos("folio_unico")
                .folio_tomi = datos("folio_unico")

                .Beneficiary_Name = busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_banco_spei")
                lsSW_END2END_REF = busca_valor_xml(datos("des_variables_xml"), "UETR").Trim
                If lsSW_END2END_REF <> "" Then
                    lsSLA_UETR = busca_valor_xml(datos("des_variables_xml"), "SLA_UETR").Trim
                    'If lsSLA_UETR <> "" Then
                    '    lsSW_END2END_REF &= " " & lsSLA_UETR
                    'End If
                    .SW_END2END_REF = lsSW_END2END_REF
                End If
            End With

            ' Realización del giro
            'lbRegreso = goT24_Connection.registra_FT(goVistasT24_Database, datos("folio_unico"), gsFechaSistema_T24_Juliana, gsTablaTrabajo, icn, "SWF", loTrxMaster, loTrxDetailFT)
            lbRegreso = registra_TRX_WS(datos("folio_unico"), goMW_TipoOperacion.tabla_trabajo, "SWF_IN", icn, "PACS009", loTrxMaster, loTrxDetailFT, datos("des_variables_xml"))

            'If lbRegreso Then
            '    'Registra_SPEI_Participantes(icn, datos)
            '    actualiza_operacion(icn)
            'End If
            Return lbRegreso
        Catch lexErrorReg009SPEI As Exception
            write_Log("ERROR|" & tipoOperacion & ".T24_REGISTRO|Se presenta exception: " & lexErrorReg009SPEI.ToString)
            gexUltimaExcepcion = lexErrorReg009SPEI
        End Try
        Return False
    End Function

    ' Inserta registro en T24 para traspaso entre cuentas VOSTRO
    Private Shared Function T24_REGISTRO_VOSTROS(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        Try
            Dim loTrxMaster As T24_MW_TrxMaster = Nothing
            Dim loTrxDetailFT As T24_MW_TrxDetail = Nothing
            Dim lsMoneda As String, lsDescripcion As String
            Dim lsCuentaCompletaVostro As String, laCuentaSeparadaVostro() As String
            Dim lsOrd_Subaplicacion As String, lsOrd_Cuenta As String, lsOrd_Nombre As String
            Dim lsBen_Subaplicacion As String, lsBen_Cuenta As String, lsBen_Nombre As String
            Dim lbRegreso As Boolean
            Dim lsSW_END2END_REF As String, lsSLA_UETR As String

            lsMoneda = goT24_Connection.valida_moneda(datos("cve_moneda"))
            ' Ordenante
            lsCuentaCompletaVostro = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_emisor")
            laCuentaSeparadaVostro = lsCuentaCompletaVostro.Split("-")
            lsOrd_Subaplicacion = laCuentaSeparadaVostro(1)
            lsOrd_Cuenta = laCuentaSeparadaVostro(2)
            lsOrd_Nombre = busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor")
            ' Beneficiario
            lsCuentaCompletaVostro = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_vostro")
            laCuentaSeparadaVostro = lsCuentaCompletaVostro.Split("-")
            lsBen_Subaplicacion = laCuentaSeparadaVostro(1)
            lsBen_Cuenta = laCuentaSeparadaVostro(2)
            lsBen_Nombre = busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_vostro")
            'Armado de la descricpión
            lsDescripcion = "RECEPCION TRANSFERENCIA INTERNACIONAL PACS009 VOSTROS."
            If busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor") <> "" Then
                lsDescripcion &= ", BANCO ORIGEN: " & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor")
            End If
            ' Vostro
            If busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_vostro") <> "" Then
                lsDescripcion &= ", BANCO DESTINO: " & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_vostro")
            End If
            'SPEI
            If busca_valor_xml(datos("des_variables_xml"), "btmum_cve_casfim_spei") <> "" Then
                lsDescripcion &= ", PAGO SPEI A: " & busca_valor_xml(datos("des_variables_xml"), "btmum_cve_casfim_spei") & "-" & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_banco_spei")
            End If
            ' Referencia
            If busca_valor_xml(datos("des_variables_xml"), "related_reference") <> "" Then
                lsDescripcion &= ", REL. REFERENCE: " & busca_valor_xml(datos("des_variables_xml"), "related_reference")
            End If

            ' Prepatamos los datos para el registro del FT
            loTrxMaster = New T24_MW_TrxMaster
            With loTrxMaster
                '.TrxCompany_ID = T24_MW_Connection.MW_CompanyId
                '.TrxSystem_ID = T24_MW_Connection.MW_SystemId
                .TrxOperation_Specific_Type = "SWF_IN"
                .TrxOperation_SubType = "PACS009_VOSTROS"
                .TrxOperation_Folio = icn
                .TrxOperation_Acct = lsOrd_Cuenta
            End With

            loTrxDetailFT = New T24_MW_TrxDetail
            With loTrxDetailFT
                .debit_acct_no = lsOrd_Cuenta
                .debit_currency = lsMoneda
                .debit_amount = String.Format("{0:0.##}", datos("des_importe"))
                .debit_their_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .credit_their_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .credit_acct_no = lsBen_Cuenta
                .credit_currency = lsMoneda
                '.payment_details = lsDescripcion
                .related_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .Instructions = lsDescripcion   'goT24_Connection.convierteTexto_a_Multivalor(lsDescripcion, 65)
                .Satel_Sys_Ref = datos("folio_unico")
                .folio_tomi = datos("folio_unico")
                .Beneficiary_Name = busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_vostro")
                lsSW_END2END_REF = busca_valor_xml(datos("des_variables_xml"), "UETR").Trim
                If lsSW_END2END_REF <> "" Then
                    lsSLA_UETR = busca_valor_xml(datos("des_variables_xml"), "SLA_UETR").Trim
                    'If lsSLA_UETR <> "" Then
                    '    lsSW_END2END_REF &= " " & lsSLA_UETR
                    'End If
                    .SW_END2END_REF = lsSW_END2END_REF
                End If
            End With

            ' Realización del FT
            'lbRegreso = goT24_Connection.registra_FT(goVistasT24_Database, datos("folio_unico"), gsFechaSistema_T24_Juliana, gsTablaTrabajo, icn, "SWF", loTrxMaster, loTrxDetailFT)
            lbRegreso = registra_TRX_WS(datos("folio_unico"), goMW_TipoOperacion.tabla_trabajo, "SWF_IN", icn, "PACS009", loTrxMaster, loTrxDetailFT, datos("des_variables_xml"))
            'If lbRegreso Then
            '    'Registra_SPEI_Participantes(icn, datos)
            '    actualiza_operacion(icn)
            'End If

            Return lbRegreso
        Catch lexErrorReg009Vostros As Exception
            write_Log("ERROR|" & tipoOperacion & ".T24_REGISTRO_VOSTROS|Se presenta exception: " & lexErrorReg009Vostros.ToString)
            gexUltimaExcepcion = lexErrorReg009Vostros
        End Try
        Return False
    End Function

    Private Shared Function registra_SPEI(ByVal icn As String, ByRef datos As Dictionary(Of String, Object)) As Boolean
        Dim loSPEI_Database As MSSQL_Database
        Dim lrsDatos_SPEI As DbDataReader
        Dim lrsDatos As DbDataReader
        Dim lsCadena_Regreso As String = String.Empty
        Dim liTipoPago As Integer, liTipoCuenta As Integer, liRefNumerica As Integer, lsConcepto As String

        write_Log("INFO|registra_SPEI|A registrar pago SPEI.")

        If busca_valor_xml(datos("des_variables_xml"), "btmum_registro_SPEI") = "PROC" Then
            write_Log("INFO|registra_SPEI|El pago ya fue registrado en SPEI.")
            Return True
        End If

        ' Tipo de pago SPEI
        liTipoPago = 3
        liTipoCuenta = 4
        write_Log("INFO|registra_SPEI|A registrar en SPEI para el tipo de pago=" & liTipoPago & ".")
        Select Case liTipoPago
            Case 3
                write_Log("INFO|registra_SPEI|Tercero a tercero vostro.")
            Case 5
                write_Log("INFO|registra_SPEI|Participante a tercero.")
            Case 6
                write_Log("INFO|registra_SPEI|Participante a vostro.")
            Case 7
                write_Log("INFO|registra_SPEI|Participante a participante.")
            Case Else
                Throw New Exception("No existe definición para pago SPEI del tipo de pago " & liTipoPago)
        End Select
        
        Dim speiReceiverIdentifier As String = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_swift_spei")
        Dim creditorIdentifier As String  = busca_valor_xml(datos("des_variables_xml"), "ben_int_cif")
        Dim creditorAgentIdentifier As String = busca_valor_xml(datos("des_variables_xml"), "aw_cif") 
        Dim intermediaryIdentifier As String = busca_valor_xml(datos("des_variables_xml"), "int_cif")
        
        Dim speiInstitutionIsIntermediary = Not String.IsNullOrWhiteSpace(intermediaryIdentifier) And 
                                            intermediaryIdentifier.Contains(speiReceiverIdentifier)

        ' Inicio base SPEI
        loSPEI_Database = New MSSQL_Database("SPEI")
        write_Log("INFO|registra_SPEI|Base SPEI en [" & loSPEI_Database.getDataBaseName() & " on " & loSPEI_Database.getServerName() & "] iniciada correctamente.")

        ' Valores TAS - SPEI
        liRefNumerica = goT24_Connection.Clave_CASFIM
        If busca_valor_xml(datos("des_variables_xml"), "related_reference") <> "" And IsNumeric(busca_valor_xml(datos("des_variables_xml"), "related_reference")) Then
            Dim lsValorTmp As String

            lsValorTmp = busca_valor_xml(datos("des_variables_xml"), "related_reference")
            If IsNumeric(lsValorTmp) Then
                If lsValorTmp.Length > 7 Then
                    lsValorTmp = Right(lsValorTmp, 7)
                End If
                liRefNumerica = lsValorTmp
            End If
        End If
        lsConcepto = IIf(Not speiInstitutionIsIntermediary, 
                         "FROM " & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor"),
                         "FFC ACCT " & busca_valor_xml(datos("des_variables_xml"), "ben_int_account")  & " " & creditorIdentifier )

        'Proceso para Generar la firma digital
        If Not GeneraFirmaDigital(datos, liTipoPago, "TB_SRV_SWF_TRABAJO_MX", icn) Then
            Throw New Exception("Se presentó un error al generar la firma digital del folio: " & icn)
        End If
        
        'Definición de Procedimiento Almacenado
        Dim loCmd As DbCommand = loSPEI_Database.newCommand("sp_InterfaceTOMI_SPEI_" & liTipoPago)
        loCmd.CommandType = CommandType.StoredProcedure
        'Definición de parámetros
        loCmd.Parameters.Clear()
        ' Parametro 1
        Dim lpIns_clave As DbParameter = loCmd.CreateParameter()
        lpIns_clave.ParameterName = "@ins_clave"
        lpIns_clave.DbType = DbType.Int64
        lpIns_clave.Direction = ParameterDirection.Input
        lpIns_clave.Value = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_casfim_spei")
        loCmd.Parameters.Add(lpIns_clave)
        ' Parametro 2
        Dim lpOp_monto As DbParameter = loCmd.CreateParameter()
        lpOp_monto.ParameterName = "@op_monto"
        lpOp_monto.DbType = DbType.Decimal
        lpOp_monto.Direction = ParameterDirection.Input
        lpOp_monto.Value = datos("des_importe")
        loCmd.Parameters.Add(lpOp_monto)
        ' Parametro 3
        Dim lpOp_cve_rastreo As DbParameter = loCmd.CreateParameter()
        lpOp_cve_rastreo.ParameterName = "@op_cve_rastreo"
        lpOp_cve_rastreo.DbType = DbType.String
        lpOp_cve_rastreo.Size = 30
        lpOp_cve_rastreo.Direction = ParameterDirection.Input
        lpOp_cve_rastreo.Value = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
        loCmd.Parameters.Add(lpOp_cve_rastreo)
        ' De acuerdo al tipo de pago son los siguientes parametros
        If liTipoPago <> 7 Then ' No enviar los parametros para Participante - Participante
            Dim orderingCustomerName = busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor")
            Dim lpOp_nom_ord As DbParameter = loCmd.CreateParameter()
            lpOp_nom_ord.ParameterName = "@op_nom_ord"
            lpOp_nom_ord.DbType = DbType.String
            lpOp_nom_ord.Size = 40
            lpOp_nom_ord.Direction = ParameterDirection.Input
            lpOp_nom_ord.Value = texto_SPEI(orderingCustomerName)
            loCmd.Parameters.Add(lpOp_nom_ord)

            Dim rawAccount = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_emisor")
            Dim orderingAccount = rawAccount.Replace("-", "") ' Se le quitan los guiones a la cuenta vostro.
            Dim lpOp_cuenta_ord As DbParameter = loCmd.CreateParameter()
            lpOp_cuenta_ord.ParameterName = "@op_cuenta_ord"
            lpOp_cuenta_ord.DbType = DbType.String
            lpOp_cuenta_ord.Size = 20
            lpOp_cuenta_ord.Direction = ParameterDirection.Input
            lpOp_cuenta_ord.Value = texto_SPEI(orderingAccount) 
            loCmd.Parameters.Add(lpOp_cuenta_ord)
            If Not speiInstitutionIsIntermediary Then
                ' Parametro 4
                Dim lpOp_nom_ben As DbParameter = loCmd.CreateParameter()
                lpOp_nom_ben.ParameterName = "@op_nom_ben"
                lpOp_nom_ben.DbType = DbType.String
                lpOp_nom_ben.Size = 40
                lpOp_nom_ben.Direction = ParameterDirection.Input
                lpOp_nom_ben.Value = texto_SPEI(creditorIdentifier)
                loCmd.Parameters.Add(lpOp_nom_ben)
                ' Parametro 5
                Dim account As String = busca_valor_xml(datos("des_variables_xml"), "ben_int_account")
                Dim accountType As Integer = IIf(esCuenta_CLABE(account), 40 ,liTipoCuenta)
                
                Dim lpTc_clave_ben As DbParameter = loCmd.CreateParameter()
                lpTc_clave_ben.ParameterName = "@tc_clave_ben"
                lpTc_clave_ben.DbType = DbType.Int16
                lpTc_clave_ben.Direction = ParameterDirection.Input
                lpTc_clave_ben.Value = accountType
                loCmd.Parameters.Add(lpTc_clave_ben)
                ' Parametro 6
                Dim lpOp_cuenta_ben As DbParameter = loCmd.CreateParameter()
                lpOp_cuenta_ben.ParameterName = "@op_cuenta_ben"
                lpOp_cuenta_ben.DbType = DbType.String
                lpOp_cuenta_ben.Size = 20
                lpOp_cuenta_ben.Direction = ParameterDirection.Input
                lpOp_cuenta_ben.Value = account
                loCmd.Parameters.Add(lpOp_cuenta_ben)
                ' Parametro 7
                Dim lpOp_rfc_curp_ben As DbParameter = loCmd.CreateParameter()
                lpOp_rfc_curp_ben.ParameterName = "@op_rfc_curp_ben"
                lpOp_rfc_curp_ben.DbType = DbType.String
                lpOp_rfc_curp_ben.Size = 18
                lpOp_rfc_curp_ben.Direction = ParameterDirection.Input
                lpOp_rfc_curp_ben.Value = "ND"
                loCmd.Parameters.Add(lpOp_rfc_curp_ben)
            Else 
                write_Log("INFO|registra_SPEI|El banco intermediario es a quien se enviara el SPEI")
                ' Parametro 4
                Dim lpOp_nom_ben As DbParameter = loCmd.CreateParameter()
                lpOp_nom_ben.ParameterName = "@op_nom_ben"
                lpOp_nom_ben.DbType = DbType.String
                lpOp_nom_ben.Size = 40
                lpOp_nom_ben.Direction = ParameterDirection.Input
                lpOp_nom_ben.Value = texto_SPEI(creditorAgentIdentifier)
                loCmd.Parameters.Add(lpOp_nom_ben)
                ' Parametro 5
                Dim lpTc_clave_ben As DbParameter = loCmd.CreateParameter()
                lpTc_clave_ben.ParameterName = "@tc_clave_ben"
                lpTc_clave_ben.DbType = DbType.Int16
                lpTc_clave_ben.Direction = ParameterDirection.Input
                lpTc_clave_ben.Value = liTipoCuenta
                loCmd.Parameters.Add(lpTc_clave_ben)
                ' Parametro 6
                Dim lpOp_cuenta_ben As DbParameter = loCmd.CreateParameter()
                lpOp_cuenta_ben.ParameterName = "@op_cuenta_ben"
                lpOp_cuenta_ben.DbType = DbType.String
                lpOp_cuenta_ben.Size = 20
                lpOp_cuenta_ben.Direction = ParameterDirection.Input
                lpOp_cuenta_ben.Value = busca_valor_xml(datos("des_variables_xml"), "aw_cif_account")
                loCmd.Parameters.Add(lpOp_cuenta_ben)
                ' Parametro 7
                Dim lpOp_rfc_curp_ben As DbParameter = loCmd.CreateParameter()
                lpOp_rfc_curp_ben.ParameterName = "@op_rfc_curp_ben"
                lpOp_rfc_curp_ben.DbType = DbType.String
                lpOp_rfc_curp_ben.Size = 18
                lpOp_rfc_curp_ben.Direction = ParameterDirection.Input
                lpOp_rfc_curp_ben.Value = "ND"
                loCmd.Parameters.Add(lpOp_rfc_curp_ben)
                
                Dim lpOp_nom_ben2 As DbParameter = loCmd.CreateParameter()
                lpOp_nom_ben2.ParameterName = "@op_nom_ben2"
                lpOp_nom_ben2.DbType = DbType.String
                lpOp_nom_ben2.Size = 40
                lpOp_nom_ben2.Direction = ParameterDirection.Input
                lpOp_nom_ben2.Value = texto_SPEI(creditorIdentifier)
                loCmd.Parameters.Add(lpOp_nom_ben2)
                
                Dim lpTc_clave_ben2 As DbParameter = loCmd.CreateParameter()
                lpTc_clave_ben2.ParameterName = "@tc_clave_ben2"
                lpTc_clave_ben2.DbType = DbType.Int16
                lpTc_clave_ben2.Direction = ParameterDirection.Input
                lpTc_clave_ben2.Value = liTipoCuenta
                loCmd.Parameters.Add(lpTc_clave_ben2)
                
                Dim lpOp_cuenta_ben2 As DbParameter = loCmd.CreateParameter()
                lpOp_cuenta_ben2.ParameterName = "@op_cuenta_ben2"
                lpOp_cuenta_ben2.DbType = DbType.String
                lpOp_cuenta_ben2.Size = 20
                lpOp_cuenta_ben2.Direction = ParameterDirection.Input
                lpOp_cuenta_ben2.Value = busca_valor_xml(datos("des_variables_xml"), "ben_int_account")
                loCmd.Parameters.Add(lpOp_cuenta_ben2)

                Dim lpOp_rfc_curp_ben2 As DbParameter = loCmd.CreateParameter()
                lpOp_rfc_curp_ben2.ParameterName = "@op_rfc_curp_ben2"
                lpOp_rfc_curp_ben2.DbType = DbType.String
                lpOp_rfc_curp_ben2.Size = 18
                lpOp_rfc_curp_ben2.Direction = ParameterDirection.Input
                lpOp_rfc_curp_ben2.Value = "ND"
                loCmd.Parameters.Add(lpOp_rfc_curp_ben2)   
            End If
        End If
        ' Parametro 8
        Dim lpOp_concepto_pag2 As DbParameter = loCmd.CreateParameter()
        lpOp_concepto_pag2.ParameterName = "@op_concepto_pago"
        lpOp_concepto_pag2.DbType = DbType.String
        lpOp_concepto_pag2.Size = 210
        lpOp_concepto_pag2.Direction = ParameterDirection.Input
        lpOp_concepto_pag2.Value = texto_SPEI(lsConcepto)
        loCmd.Parameters.Add(lpOp_concepto_pag2)
        ' Parametro 9
        Dim lpOp_ref_numerica As DbParameter = loCmd.CreateParameter()
        lpOp_ref_numerica.ParameterName = "@op_ref_numerica"
        lpOp_ref_numerica.DbType = DbType.Int32
        lpOp_ref_numerica.Direction = ParameterDirection.Input
        lpOp_ref_numerica.Value = liRefNumerica
        loCmd.Parameters.Add(lpOp_ref_numerica)
        ' Parametro 10
        Dim lpOp_ref_usu As DbParameter = loCmd.CreateParameter()
        lpOp_ref_usu.ParameterName = "@usu_clave_int"
        lpOp_ref_usu.DbType = DbType.Int16
        lpOp_ref_usu.Direction = ParameterDirection.Input
        lpOp_ref_usu.Value = 2
        loCmd.Parameters.Add(lpOp_ref_usu)

        'Parametro 11
        Dim opFirmaDigital As String = String.Empty
        lrsDatos = goTOMI_Database.Execute_Query("SELECT OP_FIRMA_DIG FROM TB_SRV_SWF_TRABAJO_MX WHERE NF_FOLIO = '" & icn & "'")
        If Not IsNothing(lrsDatos) Then
            While lrsDatos.Read
                If Not IsDBNull(lrsDatos("OP_FIRMA_DIG")) Then
                    opFirmaDigital = lrsDatos("OP_FIRMA_DIG")
                End If
            End While
            lrsDatos.Close()
            lrsDatos = Nothing
        End If

        If String.IsNullOrEmpty(opFirmaDigital) Then
            Throw New Exception("No fue posible obtener el campo de firma digital")
        End If

        Dim lpOp_firma_dig As DbParameter = loCmd.CreateParameter()
        lpOp_firma_dig.ParameterName = "@op_firma_dig"
        lpOp_firma_dig.DbType = DbType.String
        lpOp_firma_dig.Direction = ParameterDirection.Input
        lpOp_firma_dig.Value = opFirmaDigital
        loCmd.Parameters.Add(lpOp_firma_dig)


        'Termina Definición de parámetros y hace el llamado
        write_Log("INFO|registra_SPEI|Pasaron los parametros.")
        lrsDatos_SPEI = loCmd.ExecuteReader()
        write_Log("INFO|registra_SPEI|Se procesó procedimientos si errores.")
        If Not IsNothing(lrsDatos_SPEI) Then
            While lrsDatos_SPEI.Read
                lsCadena_Regreso = dameTexto(lrsDatos_SPEI.Item(0)) + "|" + dameTexto(lrsDatos_SPEI.Item(1)) + "|" + dameTexto(lrsDatos_SPEI.Item(2)) + "|" + dameTexto(lrsDatos_SPEI.Item(3)) + "|" + dameTexto(lrsDatos_SPEI.Item(4))
            End While
            lrsDatos_SPEI.Close()
            lrsDatos_SPEI = Nothing
            write_Log("INFO|registra_SPEI|Cadena_Regreso='" & lsCadena_Regreso & "'.")
        End If
        loCmd = Nothing
        '=================================================================================================================
        'TERMINA SP
        '=================================================================================================================
        loSPEI_Database.close()
        If lsCadena_Regreso.StartsWith("0|") Or lsCadena_Regreso.Contains("Ya existe una operación") Then
            agrega_valor_xml(datos("des_variables_xml"), "btmum_registro_SPEI", "PROC", Nothing)
            Return True
        Else
            Return False
        End If
    End Function
End Class
