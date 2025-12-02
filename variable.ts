Public Class PACS008_MXN

    Private Const tipoOperacion As String = "PACS008_MXN"
    Private Shared goMW_TipoOperacion As T24_MW_Connection.MW_Tipo_operacion

    Shared Function procesaOperacion(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        Dim lsAliasProceso As String
        Dim lbProcesado As Boolean
        Dim llFolioMW As Long

        ' Obtención de los datos de la familia de la operacion
        'goMW_TipoOperacion = T24_MW_Connection.MW_DatosOperacion("SWF_IN")

        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Iniciando el registro de la operación, folio_unico='" & datos("folio_unico") & "'.")

        ' Tipo de Operacion
        goMW_TipoOperacion = goT24_Connection.MW_DatosOperacion("SWF_IN")

        ' Registro de E/S
        lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)

        ' E/S
        If lsAliasProceso = "TOMI_ENTRADAS/SALIDAS" Or lsAliasProceso = "T24_REGISTRO" Or lsAliasProceso = "CONCILIAR" Then
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en TOMI.")

            ' Inicio el proceso
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)

            If lsAliasProceso = "TOMI_ENTRADAS/SALIDAS" Then
                llFolioMW = 0
                lbProcesado = TOMI_REGISTRO(icn, datos, llFolioMW)
                If lbProcesado = False And Not IsNothing(gexUltimaExcepcion) Then
                    Throw gexUltimaExcepcion
                End If

                If Not lbProcesado Then
                    Throw New Exception("Error en procesamiento de registro TOMI_REGISTRO con folio: " & icn)
                End If

                write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Fin del registro de la operación.")

                ' Actualización de la operación
                terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)
            End If

            ' Inicio el proceso
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, "T24_REGISTRO")
            terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, "T24_REGISTRO")

            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, "CONCILIAR")
            terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, "CONCILIAR")

            ' Marca la operación como finalizada total
            marca_op_terminada(icn, "PROC", "JEAI")

            ' Siguiente proceso
            lsAliasProceso = ""
        End If

        ' Termina el proceso del folio
        Return True
    End Function

    Private Shared Function TOMI_REGISTRO(ByVal icn As String, datos As Dictionary(Of String, Object), ByRef folioMW As Long) As Boolean
        Try
            Dim lrsDatos_T24 As DataTable
            Dim lrsDatos_TOMI As DbDataReader
            Dim loCmd_TOMI As DbCommand
            Dim lsCuentaCompletaVostro As String, laCuentaSeparadaVostro() As String
            Dim liTipoAppAnterior As Integer, lsTipoOpDesc As String, lsDetailCharges As String, lbCuentaT24 As Boolean
            Dim lsNumeroCliente As String, lsNombreCliente As String, lsNombreCuenta As String
            Dim lsCuentaPrincipal As String, lsCuentaComisiones As String, lsBeneficiario As String, lsDescripcion As String
            Dim lbRegistro As Boolean, lsNombreBancoBene As String
            Dim liSalida As Integer, llFolioRegreso As Long, lsSQL As String

            ' Inicio de variables
            lsNumeroCliente = String.Empty
            lsNombreCliente = String.Empty
            lsNombreCuenta = String.Empty

            ' Datos del registro
            lsBeneficiario = Trim(busca_valor_xml(datos("des_variables_xml"), "beneficiary_name_addr"))
            If lsBeneficiario = "" Then
                lsBeneficiario = Trim(busca_valor_xml(datos("des_variables_xml"), "beneficiary_name_addr_1"))
            End If
            If lsBeneficiario.Length > 100 Then
                lsBeneficiario = lsBeneficiario.Substring(0, 100)
            End If
            lsCuentaCompletaVostro = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_emisor")
            laCuentaSeparadaVostro = lsCuentaCompletaVostro.Split("-")
            lsCuentaPrincipal = laCuentaSeparadaVostro(2)
            lsCuentaComisiones = lsCuentaPrincipal
            lsDescripcion = busca_valor_xml(datos("des_variables_xml"), "remmitance_information")     'Descripción
            lsDetailCharges = busca_valor_xml(datos("des_variables_xml"), "detail_charges")

            ' Datos de la cuenta en T24
            lbCuentaT24 = False
            lrsDatos_T24 = goT24_Connection.datos_cuenta_VW(lsCuentaPrincipal)
            If lrsDatos_T24.Rows.Count > 0 Then
                lsNumeroCliente = lrsDatos_T24(0)("CUSTOMER")  'CUSTOMER_CODE
                lsNombreCliente = lrsDatos_T24(0)("NAME_1")
                lsNombreCuenta = lrsDatos_T24(0)("ACCOUNT_TITLE_1")
                lbCuentaT24 = True
            End If
            lrsDatos_T24.Dispose()

            If Not lbCuentaT24 Then
                Throw New Exception("No se encontró o está inactiva la cuenta en T24 '" & lsCuentaPrincipal & "'")
            End If

            ' Valido las comisiones o tipo de operacion
            If busca_valor_xml(datos("des_variables_xml"), "btmum_cve_swift_emisor") = "BOTKUS33" Then
                ' Es NY
                If lsDetailCharges = "OUR" Then
                    write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|Es registro de NY, swift='BOTKUS33' y DetailCharges='OUR'.")
                    liTipoAppAnterior = 804           ' TRF ENVIADA MXN SPEI, COMISION USD
                    lsTipoOpDesc = "TRF ENVIADA MXN SPEI (COM USD)"
                Else
                    write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|BTMU NY, comisión diferente a OUR.")
                    liTipoAppAnterior = 800           'TRF ENVIADA MXN SPEI
                    lsTipoOpDesc = "TRF ENVIADA MXN SPEI"
                End If
            Else
                ' No es NY
                write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|Subaplicación 800.")
                If lsDetailCharges = "OUR" Then
                    write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|No es NY, comisión OUR.")
                    liTipoAppAnterior = 800           'TRF ENVIADA MXN SPEI
                    lsTipoOpDesc = "TRF ENVIADA MXN SPEI"
                Else
                    write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|No es NY, comisión diferente a OUR.")
                    liTipoAppAnterior = 800           'TRF ENVIADA MXN SPEI
                    lsTipoOpDesc = "TRF ENVIADA MXN SPEI"
                End If
            End If

            write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|Inicia registro en base de TOIMI con llamado 'usp_TOMBIN_INSERT_FORMATO_FXD_LOCAL'.")

            write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|T24_CUSTOMER_CODE='" & lsNumeroCliente & "'.")
            write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|A buscar el nombre del banco para la cuenta CLABE '" & datos("num_cuenta") & "'.")
            lsNombreBancoBene = dame_banco(datos("num_cuenta"))
            write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|Banco='" & lsNombreBancoBene & "'.")

            If lsNombreBancoBene = "" Then
                ' El banco beneficiario no existe
                Throw New Exception("El banco asociado a la cuenta " & datos("num_cuenta") & " no existe")
            End If

            Dim activeAutomation As Boolean = AutomationIsActive()
            ' Inserción de la operacion en TOMI
            goTOMI_Database.BeginTransaction("MT103_FXD_Local")

            ' Genero FXD_Local
            loCmd_TOMI = goTOMI_Database.newCommand_Transaction("usp_TOMBIN_INSERT_FORMATO_FXD_LOCAL")
            loCmd_TOMI.CommandType = CommandType.StoredProcedure
            'Definición de parámetros
            loCmd_TOMI.Parameters.Clear()

            Dim status = If(activeAutomation,
                            If(Replace(Format(Now.Date, "yyyy-MM-dd"), "-", "") <= datos("fec_valor"),
                               "SCNP", "CONF"),
                            "PEND")

            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_FOLIO_UNICO", datos("folio_unico"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_TIPO", "FXD", DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_FOLIO", DBNull.Value, DbType.Int32)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@T24_CUSTOMER_CODE", lsNumeroCliente, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@T24_DEBIT_ACCT_NO", lsCuentaPrincipal, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_ESTATUS", status, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_USUARIO_CAPTURA", "JEAI", DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_USUARIO_AUTORIZA", IIf(activeAutomation, "TOMI", DbNull.Value),
                                         DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_FECHA_CAPTURA", Now.Date, DbType.DateTime)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_FECHA_AUTORIZA", IIf(activeAutomation, Now.Date, DbNull.Value),
                                         DbType.DateTime)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@T24_DEBIT_AMOUNT", datos("des_importe"), DbType.Decimal)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@T24_SHORT_NAME", lsNombreCliente, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_FOREX_RELACIONADO", "", DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@T24_DEBIT_CURRENCY", goT24_Connection.MonedaDefaultMN,
                                         DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_BANCO_BENEF", lsNombreBancoBene, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@T24_BEN_ACCT_NO", datos("num_cuenta"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_TIPO_CUENTA", "CUENTA DE CHEQUES(CLABE)", DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@T24_BEN_CUST_NAME", lsBeneficiario, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_RFC_BENEFICIARIO", DBNull.Value, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_TASA_RETENCION", DBNull.Value, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_FECHA_EJECUCION", datos("fec_valor"), DbType.Int32)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@T24_DEBIT_THEIR_REF", icn, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@NF_MONTO_ENVIAR", datos("des_importe"), DbType.Decimal)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@ID_PLANTILLA", DBNull.Value, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@ID_FORMATO_PLANTILLA", DBNull.Value, DbType.Int64)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Salida", 0, DbType.Int16, , ParameterDirection.Output)
            'Termina Definición de parámetros y hace el llamado
            write_Log(
                "INFO|" & tipoOperacion &
                ".TOMI_REGISTRO|usp_TOMBIN_INSERT_FORMATO_FXD_LOCAL pasaron parametros, inicia ejecución.")
            liSalida = -1
            llFolioRegreso = 0
            lrsDatos_TOMI = loCmd_TOMI.ExecuteReader()
            If Not IsNothing(lrsDatos_TOMI) Then
                While lrsDatos_TOMI.Read
                    llFolioRegreso = lrsDatos_TOMI.Item(0)
                End While
                lrsDatos_TOMI.Close()
                lrsDatos_TOMI = Nothing
            End If
            liSalida = loCmd_TOMI.Parameters("@Salida").Value
            loCmd_TOMI = Nothing
            write_Log("INFO|" & tipoOperacion & ".TOMI_REGISTRO|Paso la ejecución, Salida=" & liSalida & ", folioRegreso=" & llFolioRegreso & ".")

            ' Regreso la salida
            If liSalida <> 0 Or llFolioRegreso = 0 Then
                Throw New Exception("No se pudo registrar la operación como FXD_Local")
            End If

            ' Actualización de datos de la operación
            lsSQL = "UPDATE tb_tombin_nf_fxd_local SET nf_charge=" & textoSQL(lsDetailCharges) & " WHERE nf_folio=" & llFolioRegreso & ";" & vbCrLf & _
                    "UPDATE tb_srv_swf_trabajo SET mw_trx_id='" & llFolioRegreso & "' WHERE nf_tipo='SWIFT_IN' AND nf_folio='" & icn & "';"
            goTOMI_Database.Execute_Command_Transaction(lsSQL)

            'Registro de la Entrada/Salida
            Dim loDatos_E_S As New Dictionary(Of String, Object)
            ' Datos de referencia
            loDatos_E_S.Add("icn", icn)
            ' Datos
            loDatos_E_S.Add("referencia", datos("folio_unico"))
            loDatos_E_S.Add("num_cliente", lsNumeroCliente)
            loDatos_E_S.Add("ordenante", lsNombreCliente)
            loDatos_E_S.Add("operacion", 218)
            loDatos_E_S.Add("operacion_desc", "Salida (TTS)")
            loDatos_E_S.Add("tipo", "D")
            loDatos_E_S.Add("tipo_desc", "Doméstica")
            loDatos_E_S.Add("cuenta", lsCuentaPrincipal)
            loDatos_E_S.Add("nombre", lsNombreCuenta)
            loDatos_E_S.Add("moneda", goT24_Connection.MonedaDefaultMN)
            loDatos_E_S.Add("via", liTipoAppAnterior) ' Subaplicacion
            loDatos_E_S.Add("via_desc", lsTipoOpDesc)
            loDatos_E_S.Add("importe", datos("des_importe"))
            loDatos_E_S.Add("saldo", 0)
            loDatos_E_S.Add("comentarios", "")
            loDatos_E_S.Add("fec_captura", Format(Now.Date, "yyyy-MM-dd"))
            loDatos_E_S.Add("fec_valor", gsFechaSistema.Substring(0, 4) & "-" & gsFechaSistema.Substring(4, 2) & "-" & gsFechaSistema.Substring(6, 2))
            loDatos_E_S.Add("hora", Format(Now.Date, "hh:mm:ss AMPM"))
            loDatos_E_S.Add("capturista", "JEAI")
            loDatos_E_S.Add("fad", 0)
            loDatos_E_S.Add("ref_control", "")
            loDatos_E_S.Add("ref_control_cont", 0)
            lbRegistro = Registra_E_S(loDatos_E_S, True)
            If Not lbRegistro Then
                Throw New Exception("No se pudo registrar la operación como entrada/salida")
            End If

            ' Folio del MW
            folioMW = llFolioRegreso

            goTOMI_Database.Commit_Transaction()
            Return True
        Catch lexTOMI_REGISTRO As Exception
            write_Log("ERROR|" & tipoOperacion & ".TOMI_REGISTRO|Se presenta exception: " & lexTOMI_REGISTRO.ToString)
            gexUltimaExcepcion = lexTOMI_REGISTRO
            If Not IsNothing(goTOMI_Database) Then
                goTOMI_Database.Rollback_Transaction()
            End If
            'inserta_alerta(Nothing, Nothing, gsNombre_Servicio, "ERROR", lexInterfase_Giros.ToString, giPID, gsProcessName, lexInterfase_Giros, goTOMI_Database)
        End Try
        Return False
    End Function

    Private Shared Function AutomationIsActive() As Boolean
        Dim cmd =
                goTOMI_Database.Execute_Query(
                    "SELECT op_automatica FROM tb_srv_swf_automation_control where tipo_operacion='SWIFT_IN' AND cve_mensaje='PACS008' AND message_category_id=4")
        Dim automaticOp As Boolean = False
        If Not IsNothing(cmd) Then
            While cmd.Read
                automaticOp = cmd("op_automatica")
            End While
            cmd.Close()
        End If
        return automaticOp
    End Function
End Class
