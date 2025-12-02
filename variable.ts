Imports System.Globalization
Imports System.Net
Imports System.Net.Http
Imports System.Security.Policy
Imports System.Text
Imports Newtonsoft.Json

Public Class MT_191
    Private Const tipoOperacion As String = "MT191"
    Private Shared goMW_TipoOperacion As T24_MW_Connection.MW_Tipo_operacion

    Shared Function procesaOperacion(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        Dim lsAliasProceso As String
        write_Log(
            "INFO|" & tipoOperacion & ".procesaOperacion|Iniciando el registro de la operación, folio_unico='" &
            datos("folio_unico") & "'.")

        ' Tipo de Operacion
        goMW_TipoOperacion = goT24_Connection.MW_DatosOperacion("SWF_IN")

        ' Proceso a ejecutar
        lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)
        write_Log("INFO|MT191.procesaOperacion|A procesar el paso " & lsAliasProceso)
        
        '' Registro de la operación en T24
        If lsAliasProceso = "T24_REGISTRO" Then
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en T24.")
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)
            ' Operacion a T24
            Dim alreadyInsertedInT24 As Result(Of String) = GetFT(icn)
            
            ' Inicializamos como fallo, esto puede cambiar si la consulta es exitosa y no tiene datos.
            Dim res As Result(Of String) = Result (Of String).Failed(String.Empty)
            
            If alreadyInsertedInT24.IsSuccess
                write_Log("INFO|MT_191.procesaOperacion|La búsqueda de FT fue exitosa.")
                ' Si no tenemos nada en la consulta registramos en T24, de otra manera recuperamos el FT de la consulta.
                If String.IsNullOrWhiteSpace(alreadyInsertedInT24.Data)
                    Dim resultT24Insert As Result(Of String) = T24_REGISTRO(icn, datos)
                    If Not resultT24Insert.IsSuccess
                        If Not resultT24Insert.ErrorCode = ErrorCodeEnum.NoCode
                            marca_op_con_error(icn, resultT24Insert.Reason)
                            Throw New Exception("Error en procesamiento de registro MT-191 T24_REGISTRO con folio: " & icn)
                        Else 
                            Throw New Exception("Error en procesamiento de registro MT-191 T24_REGISTRO con folio: " & icn)
                        End If
                    End If
                    
                    write_Log("INFO|MT_191.procesaOperacion|No se encontró ningún FT, se procede a hacer el registro en T24.")
                    res = resultT24Insert
                Else 
                    write_Log("INFO|MT_191.procesaOperacion|Se encontró un FT asociado al mensaje: " & alreadyInsertedInT24.Data)
                    res = alreadyInsertedInT24  
                End If
            End If

            ' Actualizamos el Contexto del evento que maneja estas operaciones.
            Dim updatedCommission As Result(Of String) = UpdateCommissionContext(icn, res.Data)
            If Not updatedCommission.IsSuccess Then
                Throw New Exception("No se pudo enviar el FT asociado al cobro de esta comision: " & icn & vbCrLf & 
                                    "Razon: " & updatedCommission.Reason)
            End If
            
            terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)
            lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)
        End If
        
        If lsAliasProceso = "SPEI_REGISTRO" Then
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en SPEI.")

            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)
            Dim processed = registra_SPEI(icn, datos)

            If processed = False And Not IsNothing(gexUltimaExcepcion) Then
                Throw gexUltimaExcepcion
            End If
            
            If Not processed Then
                Throw New Exception("No se pudo registrar la operación en SPEI")
            End If

            terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, "SPEI_REGISTRO")
            lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)
        End If
        
        '' Creacion de MT-202
        If lsAliasProceso = "SWIFT_REGISTRO" Then
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A generar el mensaje MT-202.")
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)
            
            Dim dispatchResult As Result(Of String) = DispatchMT202(icn)
            If Not dispatchResult.IsSuccess Then
                write_Log("ERROR|MT_191.ProcesaOperacion|" & dispatchResult.Reason)
                marca_op_con_error(icn, "No se pudo enviar el MT202 asociado al cobro de esta comision")
                Throw New Exception("No se pudo enviar el MT202 asociado al cobro de esta comision: " & icn)
            End If
            ' La API generada cierra el proceso aqui.
            Return False
        End If
        
        lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)
        ' Conciliación
        If lsAliasProceso = "CONCILIAR" Then
            write_Log(
                "INFO|" & tipoOperacion & ".procesaOperacion|A conciliar operaciones (por estatus) entre TOMI y T24.")
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)
            terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)
            marca_op_terminada(icn, "PROC", "JEAI")
        End If

        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Fin del registro de la operación.")

        ' Termina el proceso del folio
        Return True
    End Function

    Private Shared Function T24_REGISTRO(icn As String, datos As Dictionary(Of String, Object)) As Result(Of String)
        Try
            Const t24InternalType = "MT191"
            Dim xmlData As String = datos("des_variables_xml")

            write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Inicia registro en T24.")
            Dim commission As InternationalCommission = InternationalCommission.Map(xmlData)
            Dim loTrxMaster As New T24_MW_TrxMaster With 
                    {
                    .TrxOperation_Specific_Type = "SWF_IN",
                    .TrxOperation_SubType = t24InternalType,
                    .TrxOperation_Folio = icn,
                    .TrxOperation_Acct = commission.DebitAccountNumber
                    }

            Dim instructions As New StringBuilder("COMISION BANCO BENEFICIARIO" & vbCrLf)
            instructions.Append(commission.OriginalReference & vbTab)
            instructions.Append(commission.OriginalDate & vbTab)
            instructions.Append("COMM:" & vbTab &
                                commission.CreditAmount.ToString("C", CultureInfo.CreateSpecificCulture("en-US")))
            instructions.Append(vbTab & commission.CreditCurrency)

            Dim loTrxDetailFT As New T24_MW_TrxDetail With
                    {
                    .credit_acct_no = goT24_Connection.T24_NOSTRO_Account(commission.CreditCurrency),
                    .credit_currency = commission.CreditCurrency,
                    .credit_amount = commission.CreditAmount,
                    .debit_acct_no = commission.DebitAccountNumber,
                    .ordering_cust = commission.OrderingCustomer,
                    .Instructions = instructions.ToString(),
                    .Satel_Sys_Ref = datos("folio_unico"),
                    .folio_tomi = datos("folio_unico")
                    }

            Return _
                registra_TRX_WS_Result(icn,tipoOperacion,
                                       loTrxMaster, loTrxDetailFT, datos("des_variables_xml"))
        Catch ex As Exception
            write_Log("ERROR|" & tipoOperacion & ".T24_REGISTRO|Se presenta exception: " & ex.ToString)
            gexUltimaExcepcion = ex
        End Try
        Return Result (Of String).Failed("Hubo un error al intentar registrar la operacion.")
    End Function

    Private Shared Function UpdateCommissionContext(originalReference As String, ourReference As String) _
        As Result(Of String)
        If String.IsNullOrWhiteSpace(ourReference)  Then
            write_Log("ERROR|MT191.UpdateCommissionContext|No T24 reference passed to update commission context.")
            Return Result (Of String).Failed("No T24 reference passed to update commission context.")
        End If
        
        If String.IsNullOrWhiteSpace(originalReference)  Then
            write_Log("ERROR|MT191.UpdateCommissionContext|No SAMH reference passed to update commission context")
            Return Result (Of String).Failed("No SAMH reference passed to update commission context.")
        End If
        Try
            write_Log("INFO|MT_191.UpdateCommissionContext|To update the Commission Context with the T24 reference")
            Dim creditConfirmation As New ChargesPaymentUpdateDto With 
                    {
                    .OriginalReference = originalReference,
                    .T24Reference = ourReference
                    }
            Dim json As String = JsonConvert.SerializeObject(creditConfirmation)
            write_Log("INFO|MT191.UpdateCommissionContext|Payload:\n" & json)
            Using _client As New HttpClient()
                Dim _content AS New StringContent(json, Encoding.UTF8, "application/json")
                write_Log(
                    "INFO|MT191.UpdateCommissionContext|To update Commission Context with context ref " &
                    creditConfirmation.OriginalReference)
                Dim response AS HttpResponseMessage = _client.PutAsync(ApiNewMdwUrl, _content).Result
                write_Log("INFO|MT191.UpdateCommissionContext|Server's answer: " & response.ReasonPhrase)
                If response.StatusCode = HttpStatusCode.Accepted Then
                    Return Result (Of String).Success(response.ReasonPhrase)
                End If
                Return Result (Of String).Failed(response.ReasonPhrase)
            End Using
        Catch ex As Exception
            write_Log("ERROR|MT191.UpdateCommissionContext|An error occurred:" & vbCrLf & ex.Message)
            Return Result (Of String).Failed(ex.Message)
        End Try
    End Function

    Private Shared Function DispatchMT202(ref As String) As Result(Of String)
        Try
            write_Log("ERROR|MT_191.DispatchMT202|A enviar MT202 para terminar el cobro de la comision.")
            Dim creditConfirmation As New FiToFiCommissionCreditConfirmationDto With 
                    {
                    .TheirReference = ref
                    }
            Dim json As String = JsonConvert.SerializeObject(creditConfirmation)
            write_Log("INFO|MT191.DispatchMT202|Payload:\n" & json)
            Using _client As New HttpClient()
                Dim _content AS New StringContent(json, Encoding.UTF8, "application/json")
                write_Log(
                    "INFO|MT191.DispatchMT202|To enqueue dispatching of MT202 with context ref " &
                    creditConfirmation.TheirReference)
                Dim response AS HttpResponseMessage = _client.PostAsync(ApiNewMdwUrl & "/confirmation", _content).Result
                write_Log("INFO|MT191.DispatchMT202|Respuesta del servidor: " & response.ReasonPhrase)
                
                If response.StatusCode = HttpStatusCode.Accepted Then
                    Return Result (Of String).Success(response.ReasonPhrase)
                End If
                Return Result (Of String).Failed(response.ReasonPhrase)
            End Using
        Catch ex As Exception
            write_Log("INFO|MT191.DispatchMT202|An error occurred:" & vbCrLf & ex.Message)
            Return Result (Of String).Failed(ex.Message)
        End Try
    End Function
    
    Private Shared Function registra_SPEI(ByVal icn As String, ByRef datos As Dictionary(Of String, Object)) As Boolean
        Dim loSPEI_Database As MSSQL_Database
        Dim lrsDatos_SPEI As DbDataReader
        Dim lrsDatos As DbDataReader
        Dim lsCadena_Regreso As String = String.Empty
        Dim liTipoPago As Integer, liTipoCuenta As Integer, liRefNumerica As Integer, lsConcepto As String, lsNomBeneficiario As String

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
        lsConcepto = "FROM " & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor")

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
            ' Nombre del beneficiario
            lsNomBeneficiario = busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_banco_spei")  ' Nombre del banco beneficiario (por default)
            If busca_valor_xml(datos("des_variables_xml"), "ben_int_cif") <> "" Then
                lrsDatos = goTOMI_Database.Execute_Query("SELECT des_nombre FROM tb_srv_swf_cat_cif_datos WHERE cve_swift = '" & busca_valor_xml(datos("des_variables_xml"), "ben_int_cif") & "'")
                If Not IsNothing(lrsDatos) Then
                    While lrsDatos.Read
                        lsNomBeneficiario = lrsDatos("des_nombre")
                    End While
                    lrsDatos.Close()
                    lrsDatos = Nothing
                End If
            End If

            ' Parametro 4
            Dim lpOp_nom_ben As DbParameter = loCmd.CreateParameter()
            lpOp_nom_ben.ParameterName = "@op_nom_ben"
            lpOp_nom_ben.DbType = DbType.String
            lpOp_nom_ben.Size = 40
            lpOp_nom_ben.Direction = ParameterDirection.Input
            lpOp_nom_ben.Value = texto_SPEI(lsNomBeneficiario)
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
            lpOp_cuenta_ben.Value = busca_valor_xml(datos("des_variables_xml"), "ben_int_account")
            loCmd.Parameters.Add(lpOp_cuenta_ben)
            ' Parametro 7
            Dim lpOp_rfc_curp_ben As DbParameter = loCmd.CreateParameter()
            lpOp_rfc_curp_ben.ParameterName = "@op_rfc_curp_ben"
            lpOp_rfc_curp_ben.DbType = DbType.String
            lpOp_rfc_curp_ben.Size = 18
            lpOp_rfc_curp_ben.Direction = ParameterDirection.Input
            lpOp_rfc_curp_ben.Value = "ND"
            loCmd.Parameters.Add(lpOp_rfc_curp_ben)
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
    
    Private Shared Function GetFT(rfk As String) As Result(Of String)
            write_Log("INFO|MT_191.GetFT|A buscar FT para la comision.")
            Dim query As String =
                    "SELECT [T24_Ref] FROM [dbo].[tb_srv_swf_plan_trabajo_maestro] WITH (NOLOCK) WHERE [folio]=" &
                    textoSQL(rfk) & " AND [tipo_operacion]='SWIFT_IN'"
            Try
                Using reader As DbDataReader = goTOMI_Database.Execute_Query(query)
                    write_Log("INFO|MT_191.GetFT|Se ejecuto el query.")
                    If IsNothing(reader) Then
                        Return Result (Of String).Failed(String.Empty)
                    End If

                    If Not reader.Read() Then
                        write_Log("INFO|MT_191.GetFT|No se encontró ningún FT asociado al MT-191.")
                        Return Result (Of String).Success(String.Empty)
                    End If

                    Dim ft As String = If (IsDBNull(reader.Item(0)), String.Empty, reader.Item(0))
                    Return Result (Of String).Success(ft) 
                End Using
            Catch ex As Exception
                write_Log("ERROR|MT_191.GetFT|Se presenta la excepción: " & ex.Message)
                Return Result (Of String).Failed(ex.Message)
            End Try
    End Function
End Class
