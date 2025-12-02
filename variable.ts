Module Comunes
    Public Function Registra_E_S(ByRef datos As Dictionary(Of String, Object), Optional es_bloque_transactional As Boolean = False) As Boolean
        Dim loCmd_TOMI As DbCommand
        Dim lrsDatos As DbDataReader
        Dim lsSQL As String, lbRegistrar As Boolean
        Dim liSalida As Integer

        ' Busqueda del registro
        lbRegistrar = False
        lsSQL = "SELECT COUNT(1) AS num_regs FROM tb_srv_swf_plan_trabajo_detalle WHERE tipo_operacion='" & gsTipoOperacion & "' AND folio='" & datos("icn") & "' AND flg_terminado=0 AND alias_proceso='TOMI_ENTRADAS/SALIDAS'"
        If es_bloque_transactional Then
            lrsDatos = goTOMI_Database.Execute_Query_Transaction(lsSQL)
        Else
            lrsDatos = goTOMI_Database.Execute_Query(lsSQL)
        End If
        If Not IsNothing(lrsDatos) Then
            While lrsDatos.Read
                If lrsDatos(0) > 0 Then
                    lbRegistrar = True
                End If
            End While
            lrsDatos.Close()
            lrsDatos = Nothing
        End If

        write_Log("INFO|Comunes.Registra_E_S|Por_registrar=" & lbRegistrar & ".")

        If lbRegistrar Then
            'Definición de Procedimiento Almacenado
            If es_bloque_transactional Then
                loCmd_TOMI = goTOMI_Database.newCommand_Transaction("usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS")
            Else
                loCmd_TOMI = goTOMI_Database.newCommand("usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS")
            End If
            loCmd_TOMI.CommandType = CommandType.StoredProcedure
            'Definición de parámetros
            loCmd_TOMI.Parameters.Clear()
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Referencia", datos("referencia"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Num_Cliente", datos("num_cliente"), DbType.Int32)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@CDL", datos("num_cliente"), DbType.Int32)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Ordenante", datos("ordenante"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Operacion", datos("operacion"), DbType.Int32)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@OperacionDesc", datos("operacion_desc"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Tipo", datos("tipo"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@TipoDesc", datos("tipo_desc"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Cuenta", datos("cuenta"), DbType.Int32)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Nombre", datos("nombre"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@CuentaMoneda", datos("moneda"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Via", datos("via"), DbType.Int32)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@ViaDesc", datos("via_desc"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Importe", datos("importe"), DbType.Decimal)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Saldo", datos("saldo"), DbType.Decimal)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Comentarios", datos("comentarios"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Fecha_Captura", datos("fec_captura"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@FechaValor", datos("fec_valor"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Hora", datos("hora"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Capturista", datos("capturista"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Fad", datos("fad"), DbType.Decimal)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Referencia_Control", datos("ref_control"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Referencia_Control_Cont", datos("ref_control_cont"), DbType.Int32)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Salida", -1, DbType.Int32, , ParameterDirection.Output)
            'Termina Definición de parámetros y hace el llamado
            write_Log("INFO|Comunes.Registro_E_S|usp_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS pasaron parametros, inicia ejecución.")
            loCmd_TOMI.ExecuteNonQuery()
            liSalida = loCmd_TOMI.Parameters("@Salida").Value
            loCmd_TOMI = Nothing

            write_Log("INFO|Comunes.Registra_E_S|Paso la ejecución. Salida=" & liSalida & ".")

            If liSalida = 0 Then
                If es_bloque_transactional Then
                    goTOMI_Database.Execute_Command_Transaction("UPDATE tb_srv_swf_plan_trabajo_detalle SET flg_terminado=1,fec_inicio=getDate(),fec_fin=getDate() WHERE tipo_operacion='" & gsTipoOperacion & "' AND folio='" & datos("icn") & "' AND flg_terminado=0 AND alias_proceso='TOMI_ENTRADAS/SALIDAS'")
                Else
                    goTOMI_Database.Execute_Command("UPDATE tb_srv_swf_plan_trabajo_detalle SET flg_terminado=1,fec_inicio=getDate(),fec_fin=getDate() WHERE tipo_operacion='" & gsTipoOperacion & "' AND folio='" & datos("icn") & "' AND flg_terminado=0 AND alias_proceso='TOMI_ENTRADAS/SALIDAS'")
                End If
                Return True
            Else
                Return False
            End If
        Else
            Return False
        End If
    End Function

    'Public Function actualiza_operacion(icn As String) As Boolean
    '    Dim loCmd_TOMI As DbCommand
    '    Dim liRegAfectados As Integer

    '    loCmd_TOMI = goTOMI_Database.newCommand_Transaction("usp_swf_termina_proceso_registro")
    '    loCmd_TOMI.CommandType = CommandType.StoredProcedure
    '    'Definición de parámetros
    '    loCmd_TOMI.Parameters.Clear()
    '    ' Parametro 1
    '    Dim lpICN As DbParameter = loCmd_TOMI.CreateParameter()
    '    lpICN.ParameterName = "@icn"
    '    lpICN.DbType = DbType.String
    '    lpICN.Direction = ParameterDirection.Input
    '    lpICN.Value = icn
    '    loCmd_TOMI.Parameters.Add(lpICN)
    '    ' Parametro 2
    '    Dim lpTipoOperacion As DbParameter = loCmd_TOMI.CreateParameter()
    '    lpTipoOperacion.ParameterName = "@tipo_operacion"
    '    lpTipoOperacion.DbType = DbType.String
    '    lpTipoOperacion.Direction = ParameterDirection.Input
    '    lpTipoOperacion.Value = gsTipoOperacion          ' Mensaje Swift de entrada
    '    loCmd_TOMI.Parameters.Add(lpTipoOperacion)

    '    'Termina Definición de parámetros y hace el llamado
    '    write_Log("INFO|Comunes.actualiza_operacion|usp_swf_actualiza_datos_T24, pasaron los parametros inicia ejecución.")
    '    liRegAfectados = loCmd_TOMI.ExecuteNonQuery()
    '    write_Log("INFO|Comunes.actualiza_operacion|La operación se realizo exitosamente.")

    '    Return IIf(liRegAfectados > 0, True, False)
    'End Function

    'Public Function termina_registro(icn As String, ByRef datos As Dictionary(Of String, Object)) As Boolean
    '    Dim loCmd_TOMI As DbCommand
    '    Dim liRegAfectados As Integer

    '    ' Marco la operacion como terminada
    '    loCmd_TOMI = goTOMI_Database.newCommand_Transaction("usp_swf_marca_op_terminada")
    '    loCmd_TOMI.CommandType = CommandType.StoredProcedure
    '    'Definición de parámetros
    '    loCmd_TOMI.Parameters.Clear()
    '    ' Parametro 1
    '    Dim lpICN As DbParameter = loCmd_TOMI.CreateParameter()
    '    lpICN.ParameterName = "@icn"
    '    lpICN.DbType = DbType.String
    '    lpICN.Direction = ParameterDirection.Input
    '    lpICN.Value = icn
    '    loCmd_TOMI.Parameters.Add(lpICN)
    '    ' Parametro 2
    '    Dim lpTipoOperacion As DbParameter = loCmd_TOMI.CreateParameter()
    '    lpTipoOperacion.ParameterName = "@tipo_operacion"
    '    lpTipoOperacion.DbType = DbType.String
    '    lpTipoOperacion.Direction = ParameterDirection.Input
    '    lpTipoOperacion.Value = gsTipoOperacion          ' Mensaje Swift de entrada
    '    loCmd_TOMI.Parameters.Add(lpTipoOperacion)
    '    ' Parametro 3
    '    Dim lpCveEstatus As DbParameter = loCmd_TOMI.CreateParameter()
    '    lpCveEstatus.ParameterName = "@cve_estatus"
    '    lpCveEstatus.DbType = DbType.String
    '    lpCveEstatus.Direction = ParameterDirection.Input
    '    lpCveEstatus.Value = "PROC"
    '    loCmd_TOMI.Parameters.Add(lpCveEstatus)
    '    ' Parametro 4
    '    Dim lpCveUsuario As DbParameter = loCmd_TOMI.CreateParameter()
    '    lpCveUsuario.ParameterName = "@cve_usuario"
    '    lpCveUsuario.DbType = DbType.String
    '    lpCveUsuario.Direction = ParameterDirection.Input
    '    lpCveUsuario.Value = "JEAI"
    '    loCmd_TOMI.Parameters.Add(lpCveUsuario)
    '    ' Parametro 5
    '    Dim lpDesRechazo As DbParameter = loCmd_TOMI.CreateParameter()
    '    lpDesRechazo.ParameterName = "@des_rechazo"
    '    lpDesRechazo.DbType = DbType.String
    '    lpDesRechazo.Direction = ParameterDirection.Input
    '    lpDesRechazo.Value = DBNull.Value
    '    loCmd_TOMI.Parameters.Add(lpDesRechazo)
    '    'Termina Definición de parámetros y hace el llamado
    '    write_Log("INFO|Comunes.termina_registro|usp_swf_marca_op_terminada, pasaron los parametros inicia ejecución.")
    '    loCmd_TOMI.ExecuteNonQuery()
    '    write_Log("INFO|Comunes.termina_registro|Paso la ejecución.")
    '    write_Log("INFO|Comunes.termina_registro|La operación se realizo exitosamente, folio_unico='" & datos("folio_unico") & "'.")

    '    'Termina Definición de parámetros y hace el llamado
    '    liRegAfectados = loCmd_TOMI.ExecuteNonQuery()
    '    If liRegAfectados > 0 Then
    '        write_Log("INFO|210.termina_registro|La operación se realizo exitosamente.")
    '        Return True
    '    Else
    '        write_Log("INFO|210.termina_registro|No se pudo realizar el registro.")
    '        Return False
    '    End If
    'End Function

    Public Function marca_op_con_error(icn As String, mensaje_error As String) As Integer
        Try
            Dim loCmd As DbCommand

            If IsNothing(icn) Then
                Return 0
            End If
            If IsNothing(mensaje_error) Then
                Return 0
            End If

            ' Log
            write_Log("INFO|marca_op_con_error|Intento registrar la operación con error, icn='" & icn & "'.")

            'Definición de Procedimiento Almacenado
            loCmd = goTOMI_Database.newCommand("usp_swf_marca_op_error")
            loCmd.CommandType = CommandType.StoredProcedure
            'Definición de parámetros
            loCmd.Parameters.Clear()
            ' Parametro 1
            Dim lpICN As DbParameter = loCmd.CreateParameter()
            lpICN.ParameterName = "@icn"
            lpICN.DbType = DbType.String
            lpICN.Direction = ParameterDirection.Input
            lpICN.Value = icn
            loCmd.Parameters.Add(lpICN)
            ' Parametro 2
            Dim lpTipoOperacion As DbParameter = loCmd.CreateParameter()
            lpTipoOperacion.ParameterName = "@tipo_operacion"
            lpTipoOperacion.DbType = DbType.String
            lpTipoOperacion.Direction = ParameterDirection.Input
            lpTipoOperacion.Value = gsTipoOperacion          ' Mensaje Swift de entrada
            loCmd.Parameters.Add(lpTipoOperacion)
            ' Parametro 3
            Dim lpDesError As DbParameter = loCmd.CreateParameter()
            lpDesError.ParameterName = "@des_error"
            lpDesError.DbType = DbType.String
            lpDesError.Direction = ParameterDirection.Input
            lpDesError.Value = mensaje_error
            loCmd.Parameters.Add(lpDesError)
            'Termina Definición de parámetros y hace el llamado
            Return loCmd.ExecuteNonQuery()
        Catch lexErrorMarca As Exception
            write_Log("ERROR|marca_op_con_error|No se pudo marcar el error:" & lexErrorMarca.ToString)
        End Try
        Return 0
    End Function

    Public Function marca_op_terminada(icn As String, estatus As String, usuarioAutorizacion As String, Optional detalleRechazo As String = "") As Integer
        Try
            Dim loCmd As DbCommand

            If IsNothing(icn) Then
                Return 0
            End If

            ' Log
            write_Log("INFO|marca_op_terminada|Intento registrar la operación como terminada, icn='" & icn & "'.")

            'Definición de Procedimiento Almacenado
            loCmd = goTOMI_Database.newCommand("usp_swf_marca_op_terminada")
            loCmd.CommandType = CommandType.StoredProcedure
            'loCmd.CommandTimeout = MAX_SP_TIMEOUT
            'Definición de parámetros
            loCmd.Parameters.Clear()
            ' Parametro 1
            Dim lpICN As DbParameter = loCmd.CreateParameter()
            lpICN.ParameterName = "@icn"
            lpICN.DbType = DbType.String
            lpICN.Direction = ParameterDirection.Input
            lpICN.Value = icn
            loCmd.Parameters.Add(lpICN)
            ' Parametro 2
            Dim lpTipoOperacion As DbParameter = loCmd.CreateParameter()
            lpTipoOperacion.ParameterName = "@tipo_operacion"
            lpTipoOperacion.DbType = DbType.String
            lpTipoOperacion.Direction = ParameterDirection.Input
            lpTipoOperacion.Value = gsTipoOperacion          ' Mensaje Swift de entrada
            loCmd.Parameters.Add(lpTipoOperacion)
            ' Parametro 3
            Dim lpCveEstatus As DbParameter = loCmd.CreateParameter()
            lpCveEstatus.ParameterName = "@cve_estatus"
            lpCveEstatus.DbType = DbType.String
            lpCveEstatus.Direction = ParameterDirection.Input
            lpCveEstatus.Value = estatus
            loCmd.Parameters.Add(lpCveEstatus)
            ' Parametro 4
            Dim lpCveUsuario As DbParameter = loCmd.CreateParameter()
            lpCveUsuario.ParameterName = "@cve_usuario"
            lpCveUsuario.DbType = DbType.String
            lpCveUsuario.Direction = ParameterDirection.Input
            lpCveUsuario.Value = usuarioAutorizacion
            loCmd.Parameters.Add(lpCveUsuario)
            ' Parametro 5
            Dim lpDesRechazo As DbParameter = loCmd.CreateParameter()
            lpDesRechazo.ParameterName = "@des_rechazo"
            lpDesRechazo.DbType = DbType.String
            lpDesRechazo.Direction = ParameterDirection.Input
            lpDesRechazo.Value = detalleRechazo
            loCmd.Parameters.Add(lpDesRechazo)
            'Termina Definición de parámetros y hace el llamado
            Return loCmd.ExecuteNonQuery()
        Catch lexErrorMarca As Exception
            write_Log("ERROR|marca_op_terminada|No se pudo marcar el error:" & lexErrorMarca.ToString)
        End Try
        Return 0
    End Function

    Public Function dame_banco(cuenta_clabe As String) As String
        Dim loBancos As Dictionary(Of String, String)
        Dim lsInicioCta As String, lsCve_banco As String

        ' Inicio de variables
        loBancos = lista_bmx()
        lsInicioCta = cuenta_clabe.Substring(0, 3)
        For Each lsCve_banco In loBancos.Keys
            If lsInicioCta = lsCve_banco.Substring(2, 3) Then
                Return lsCve_banco & "-" & loBancos(lsCve_banco)
            End If
        Next
        Return ""
    End Function

    Private Function lista_bmx() As Dictionary(Of String, String)
        Dim lrsDatos As DbDataReader
        Dim loCmd As DbCommand
        Dim loRegreso As Dictionary(Of String, String)

        ' Inicio de variables
        loRegreso = New Dictionary(Of String, String)

        ' Definición de Procedimiento Almacenado
        loCmd = goTOMI_Database.newCommand("usp_spei_listaBancos")
        loCmd.CommandType = CommandType.StoredProcedure
        ' Definición de parámetros
        loCmd.Parameters.Clear()
        ' Termina Definición de parámetros y hace el llamado
        lrsDatos = loCmd.ExecuteReader()
        If Not IsNothing(lrsDatos) Then
            While lrsDatos.Read
                ' Alta del dato
                loRegreso.Add(CStr(lrsDatos("ins_clave")).PadLeft(5, "0"), lrsDatos("ins_nombre"))
            End While
            lrsDatos.Close()
            lrsDatos = Nothing
        End If
        Return loRegreso
    End Function

    Public Function busca_cif_datos(cif As String, ByRef swift As String, ByRef nombre As String, ByRef calle As String) As Integer
        Dim lrsDatos As DbDataReader
        Dim loCmd As DbCommand

        ' Definición de Procedimiento Almacenado
        loCmd = goTOMI_Database.newCommand("usp_swf_get_cif_datos")
        loCmd.CommandType = CommandType.StoredProcedure
        ' Definición de parámetros
        loCmd.Parameters.Clear()
        ' Parametro 1
        Dim lpNumCIF As DbParameter = loCmd.CreateParameter()
        lpNumCIF.ParameterName = "@num_cif"
        lpNumCIF.DbType = DbType.String
        lpNumCIF.Direction = ParameterDirection.Input
        lpNumCIF.Value = cif
        loCmd.Parameters.Add(lpNumCIF)
        ' Parametro 1
        Dim lpCveSwift As DbParameter = loCmd.CreateParameter()
        lpCveSwift.ParameterName = "@cve_swift"
        lpCveSwift.DbType = DbType.String
        lpCveSwift.Direction = ParameterDirection.Input
        lpCveSwift.Value = DBNull.Value
        loCmd.Parameters.Add(lpCveSwift)
        ' Termina Definición de parámetros y hace el llamado
        lrsDatos = loCmd.ExecuteReader()
        If Not IsNothing(lrsDatos) Then
            While lrsDatos.Read
                ' Alta del dato
                swift = lrsDatos("cve_swift")
                nombre = lrsDatos("des_nombre")
                calle = lrsDatos("des_calle")
            End While
            lrsDatos.Close()
            lrsDatos = Nothing
        End If
        Return 1
    End Function

    Public Function quita_linea(texto As String, Optional largo As Integer = 0) As String
        Dim lsRegreso As String

        If IsNothing(texto) Then Return ""
        If texto = "" Then Return ""
        If texto.Length > 2 Then
            If IsNumeric(texto.Substring(0, 1)) And texto.Substring(1, 1) = "/" Then
                lsRegreso = texto.Substring(2)
                If largo = 0 Or lsRegreso.Length <= largo Then
                    Return lsRegreso
                Else
                    Return lsRegreso.Substring(0, largo)
                End If
                Return texto.Substring(2)
            Else
                If largo = 0 Or texto.Length <= largo Then
                    Return texto
                Else
                    Return texto.Substring(0, largo)
                End If
            End If
        End If
        Return texto
    End Function

    Public Function convierteTexto_a_Multivalor_SinCortar_Local(texto As String, MW_SeparadorMultivalorLength As Integer, Optional ByRef TotalLineas As Integer = 0, Optional ByVal Prefix As Boolean = False) As List(Of String)
        Dim strCadena As String = ""
        Dim retorno As List(Of String) = New List(Of String)    'lista con los parrafos
        Dim palabra As String
        Dim contenedor As String

        texto = texto.Trim()
        While texto.StartsWith("-")
            texto = texto.Substring(1)
            texto = texto.Trim()
        End While

        If texto.Length = 0 Then
            'si la cadena de entrada esta vacia devolvemos una coleccion vacia.
            Return retorno
        ElseIf texto.Length <= MW_SeparadorMultivalorLength Then
            'si la cadena tiene [NumCaracteresEnParrafo] caracteres o menos, devolvemos sólo un elemento con la cadena entera
            If Prefix Then
                strCadena = "//" & texto
            Else
                retorno.Add(texto)
            End If

            TotalLineas = 1
            Return retorno
        End If

        'contenedor para almacenar las palabras mientras que la longitud del párrafo sea menor que [NumCaracteresEnParrafo] 
        contenedor = ""

        Dim blnAux As Boolean = False
        Dim intCont As Integer = 0
        Dim Arreglo As String() = texto.Split(" "c)

        For Each palabra In Arreglo
            Dim PalabraTemp As String = ""

            If Not blnAux Then
                If Arreglo.Length > (intCont + 1) Then
                    If Arreglo(intCont + 1).ToString.StartsWith("-") Then
                        'PalabraTemp = Arreglo(intCont) & " " & Arreglo(intCont + 1)
                        PalabraTemp = Arreglo(intCont)
                        blnAux = False
                    Else
                        PalabraTemp = Arreglo(intCont)
                    End If
                Else
                    PalabraTemp = Arreglo(intCont)
                End If


                'si la palabra tiene más de [NumCaracteresEnParrafo] caracteres seguidos, se trozea
                If PalabraTemp.Length >= MW_SeparadorMultivalorLength Then
                    If contenedor.Length > 0 Then
                        retorno.Add(contenedor)
                        contenedor = ""
                    End If

                    Do
                        Dim trozo As String = PalabraTemp.Substring(0, MW_SeparadorMultivalorLength)
                        retorno.Add(trozo)
                        PalabraTemp = PalabraTemp.Remove(0, MW_SeparadorMultivalorLength)
                    Loop While PalabraTemp.Length >= MW_SeparadorMultivalorLength
                End If

                If PalabraTemp.Length > 0 Then
                    If contenedor.Length + PalabraTemp.Length + 1 > MW_SeparadorMultivalorLength Then
                        retorno.Add(contenedor)
                        contenedor = PalabraTemp
                    Else
                        'If retorno.Count = 0 And contenedor = "" Then
                        If contenedor = "" Then
                            contenedor = PalabraTemp
                        Else
                            contenedor = contenedor & " " & PalabraTemp
                        End If

                    End If
                End If
            Else
                blnAux = False
            End If
            intCont += 1

        Next


        If contenedor.Length > 0 Then retorno.Add(contenedor)

        'TextBox3.Text = ""

        For Each Elemento As String In retorno
            If Prefix Then
                'If strCadena = "" Then
                '    strCadena += Elemento & "|"
                'Else
                strCadena += "//" & Elemento & "|"
                'End If
            Else
                strCadena += Elemento & "|"
            End If
            'TextBox3.Text += Elemento & vbCrLf
        Next
        strCadena = Mid(strCadena, 1, strCadena.Length - 1)
        TotalLineas = retorno.Count

        Return retorno
    End Function
    
    Public Function registra_TRX_WS_Result(folio As String, cve_mensaje As String, ByRef clsTrxMaster As T24_MW_TrxMaster, ByRef clsTrxDetail As T24_MW_TrxDetail, des_variables_xml As String) As Result(Of String)
        Dim lsTransactionID As String = String.Empty
        Dim lsError As String = String.Empty
        Dim lsResponseService As Boolean

        ' logs
        write_Log("INFO|Comunes.registra_TRX_WS|Cuenta: '" & clsTrxDetail.debit_acct_no & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|Cuenta BEN: '" & clsTrxDetail.credit_acct_no & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|Importe: '" & clsTrxDetail.debit_amount & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|A registrar operación.")
        write_Log("INFO|Comunes.registra_TRX_WS|A buscar comisiones.")

        Try
            ' Calculo de esquema de comision o sin esquema de comisiones (la función actualiza TrxOperation_ID, TrxInternal_Type y TrxTransaction_Type)
            goT24_Connection.busca_esquema_TRX(clsTrxMaster)

            write_Log("INFO|Comunes.registra_TRX_WS|Finalmente, el esquema de comisión será: Transaction Type='" & clsTrxMaster.TrxTransaction_Type & "', esquema='" & clsTrxMaster.TrxInternal_Type & "'.")
            clsTrxDetail.transaction_type = clsTrxMaster.TrxTransaction_Type

            write_Log("INFO|Comunes.registra_TRX_WS|Inicia invocación del WS")

            ' Invocación del WS  
            ImprimirMasterDetail(clsTrxMaster, clsTrxDetail)

            LimpiarCamposDetail(clsTrxDetail)
            LimpiarCamposXMLVariables(des_variables_xml)

            If clsTrxMaster.TrxInternal_Type = "SIN_ESQUEMA" Then
                write_Log("INFO|Comunes.registra_TRX_WS|Se ejecutará la versión SIN_ESQUEMA")
                lsResponseService = registraT24_WOSH(clsTrxMaster, clsTrxDetail, des_variables_xml, lsTransactionID, lsError)
            ElseIf clsTrxMaster.TrxInternal_Type = "CON_ESQUEMA" Then
                write_Log("INFO|Comunes.registra_TRX_WS|Se ejecutará la versión CON_ESQUEMA")
                lsResponseService = registraT24_WSH(clsTrxMaster, clsTrxDetail, des_variables_xml, lsTransactionID, lsError)
            End If
            
            If Not lsResponseService Then
                'Considerar el caso cuando no tiene Fondos 
                If lsError.Contains("OVERRIDE") Then
                    ProcesaCuentaSinFondosSWF_MX(cve_mensaje, folio)
                    Return Result(Of String).Failed("La cuenta no tiene los fondos suficientes para cubrir el monto de la operacion con folio " & folio)
                ElseIf lsError.Contains("SWIFT CHAR") Then
                    Return Result(Of String).Failed("El mensaje contiene caracteres invalidos para la operacion con folio " & folio)
                Else
                    ProcesaRegistroConError(folio, cve_mensaje, lsError)
                    Return Result(Of String).Failed(lsError)
                End If
            Else                 ' Registro exitoso
                ProcesaRegistroExitoso(lsTransactionID, folio, cve_mensaje)
            End If
        Catch toEx As TimeoutException
            write_Log("ERROR|registra_TRX_WS|" & cve_mensaje & "|Se presenta error en el folio " & folio & " - " & toEx.ToString, "")
            Return Result(Of String).Failed("El servicio no regreso una respuesta, varificar operacion en T24.", ErrorCodeEnum.T24TimeoutError)
        Catch ex As Exception
            write_Log("ERROR|registra_TRX_WS|" & cve_mensaje & "|Se presenta error en el folio " & folio & " - " & ex.ToString, "")
            Return Result(Of String).Failed("Se presenta error: " & ex.Message)
        End Try
        Return Result(Of String).Success(lsTransactionID)
    End Function

    Public Function registra_TRX_WS(ByVal folio_unico As String, ByVal tabla As String, ByVal tipo_operacion As String, ByVal folio As String, ByVal cve_mensaje As String, ByRef clsTrxMaster As T24_MW_TrxMaster, ByRef clsTrxDetail As T24_MW_TrxDetail, ByVal des_variables_xml As String) As Boolean

        Dim strFolioID As String = String.Empty
        Dim lsResponseService As Boolean
        Dim lsTransactionID As String = String.Empty
        Dim lsError As String = String.Empty

        ' logs
        write_Log("INFO|Comunes.registra_TRX_WS|Cuenta: '" & clsTrxDetail.debit_acct_no & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|Cuenta BEN: '" & clsTrxDetail.credit_acct_no & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|Importe: '" & clsTrxDetail.debit_amount & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|A registrar operación.")
        write_Log("INFO|Comunes.registra_TRX_WS|A buscar comisiones.")

        Try
            ' Calculo de esquema de comision o sin esquema de comisiones (la función actualiza TrxOperation_ID, TrxInternal_Type y TrxTransaction_Type)
            goT24_Connection.busca_esquema_TRX(clsTrxMaster)

            write_Log("INFO|Comunes.registra_TRX_WS|Finalmente, el esquema de comisión será: Transaction Type='" & clsTrxMaster.TrxTransaction_Type & "', esquema='" & clsTrxMaster.TrxInternal_Type & "'.")
            clsTrxDetail.transaction_type = clsTrxMaster.TrxTransaction_Type

            write_Log("INFO|Comunes.registra_TRX_WS|Inicia invocación del WS")

            ' Invocación del WS  
            ImprimirMasterDetail(clsTrxMaster, clsTrxDetail)

            LimpiarCamposDetail(clsTrxDetail)
            LimpiarCamposXMLVariables(des_variables_xml)

            If clsTrxMaster.TrxInternal_Type = "SIN_ESQUEMA" Then
                write_Log("INFO|Comunes.registra_TRX_WS|Se ejecutará la versión SIN_ESQUEMA")
                lsResponseService = registraT24_WOSH(clsTrxMaster, clsTrxDetail, des_variables_xml, lsTransactionID, lsError)
            ElseIf clsTrxMaster.TrxInternal_Type = "CON_ESQUEMA" Then
                write_Log("INFO|Comunes.registra_TRX_WS|Se ejecutará la versión CON_ESQUEMA")
                lsResponseService = registraT24_WSH(clsTrxMaster, clsTrxDetail, des_variables_xml, lsTransactionID, lsError)
            End If
            
            If Not lsResponseService Then
                'Considerar el caso cuando no tiene Fondos 
                If lsError.Contains("OVERRIDE") Then
                    ProcesaCuentaSinFondosSWF_MX(cve_mensaje, folio)
                Else
                    ProcesaRegistroConError(folio, cve_mensaje, lsError)
                End If
            Else                 ' Registro exitoso
                ProcesaRegistroExitoso(lsTransactionID, folio, cve_mensaje)
            End If
        Catch ex As Exception
            write_Log("ERROR|registra_TRX_WS|" & cve_mensaje & "|Se presenta error en el folio " & folio & " - " & ex.ToString, "")
            Return False
        End Try
        Return True
    End Function
    
    Public Function registra_TRX_WS_Result(ByVal folio_unico As String, ByVal tabla As String, ByVal tipo_operacion As String, ByVal folio As String, ByVal cve_mensaje As String, ByRef clsTrxMaster As T24_MW_TrxMaster, ByRef clsTrxDetail As T24_MW_TrxDetail, ByVal des_variables_xml As String) As Result(Of String)
        Dim lsTransactionID As String = String.Empty
        Dim strFolioID As String = String.Empty
        Dim lsError As String = String.Empty
        Dim lsResponseService As Boolean

        ' logs
        write_Log("INFO|Comunes.registra_TRX_WS|Cuenta: '" & clsTrxDetail.debit_acct_no & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|Cuenta BEN: '" & clsTrxDetail.credit_acct_no & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|Importe: '" & clsTrxDetail.debit_amount & "'.")
        write_Log("INFO|Comunes.registra_TRX_WS|A registrar operación.")
        write_Log("INFO|Comunes.registra_TRX_WS|A buscar comisiones.")

        Try
            ' Calculo de esquema de comision o sin esquema de comisiones (la función actualiza TrxOperation_ID, TrxInternal_Type y TrxTransaction_Type)
            goT24_Connection.busca_esquema_TRX(clsTrxMaster)

            write_Log("INFO|Comunes.registra_TRX_WS|Finalmente, el esquema de comisión será: Transaction Type='" & clsTrxMaster.TrxTransaction_Type & "', esquema='" & clsTrxMaster.TrxInternal_Type & "'.")
            clsTrxDetail.transaction_type = clsTrxMaster.TrxTransaction_Type

            write_Log("INFO|Comunes.registra_TRX_WS|Inicia invocación del WS")

            ' Invocación del WS  
            ImprimirMasterDetail(clsTrxMaster, clsTrxDetail)

            LimpiarCamposDetail(clsTrxDetail)
            LimpiarCamposXMLVariables(des_variables_xml)

            If clsTrxMaster.TrxInternal_Type = "SIN_ESQUEMA" Then
                write_Log("INFO|Comunes.registra_TRX_WS|Se ejecutará la versión SIN_ESQUEMA")
                lsResponseService = registraT24_WOSH(clsTrxMaster, clsTrxDetail, des_variables_xml, lsTransactionID, lsError)
            ElseIf clsTrxMaster.TrxInternal_Type = "CON_ESQUEMA" Then
                write_Log("INFO|Comunes.registra_TRX_WS|Se ejecutará la versión CON_ESQUEMA")
                lsResponseService = registraT24_WSH(clsTrxMaster, clsTrxDetail, des_variables_xml, lsTransactionID, lsError)
            End If
            
            If Not lsResponseService Then
                'Considerar el caso cuando no tiene Fondos 
                If lsError.Contains("OVERRIDE") Then
                    ProcesaCuentaSinFondosSWF_MX(cve_mensaje, folio)
                    Return Result(Of String).Failed("La cuenta no tiene los fondos suficientes para cubrir el monto de la operacion con folio " & folio, ErrorCodeEnum.NoFunds)
                ElseIf lsError.Contains("SWIFT CHAR") Then
                    Return Result(Of String).Failed("El mensaje contiene caracteres invalidos para la operacion con folio " & folio, ErrorCodeEnum.InvalidSwiftChar)
                Else
                    ProcesaRegistroConError(folio, cve_mensaje, lsError)
                    Return Result(Of String).Failed(lsError)
                End If
            Else                 ' Registro exitoso
                ProcesaRegistroExitoso(lsTransactionID, folio, cve_mensaje)
            End If
        Catch toEx As TimeoutException
            write_Log("ERROR|registra_TRX_WS|" & cve_mensaje & "|Se presenta error en el folio " & folio & " - " & toEx.ToString, "")
            Return Result(Of String).Failed("El servicio no regreso una respuesta, varificar operacion en T24.", ErrorCodeEnum.T24TimeoutError)
        Catch ex As Exception
            write_Log("ERROR|registra_TRX_WS|" & cve_mensaje & "|Se presenta error en el folio " & folio & " - " & ex.ToString, "")
            Return Result(Of String).Failed("Se presenta error: " & ex.Message)
        End Try
        Return Result(Of String).Success(lsTransactionID)
    End Function

    Public Function ReintentoPermitidoSWF_MX(ByVal cve_mensaje As String, ByVal lsFolio As String) As Boolean
        Try
            Dim fecUlt As Nullable(Of DateTime) = Nothing

            write_Log("INFO|ReintentoPermitido|Consulta para verificar si la operación " & cve_mensaje & " con folio " & lsFolio & " se había registrado previamente sin fondos", "")
            Dim dbDataReader As DbDataReader = goTOMI_Database.Execute_Query("SELECT FECHA_REGISTRO FROM TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX WHERE FOLIO='" & lsFolio & "' AND CVE_MENSAJE ='" & cve_mensaje & "' AND ESTATUS = 'SF'")
            If Not IsNothing(dbDataReader) Then
                While dbDataReader.Read()
                    fecUlt = CDate(dbDataReader.Item("FECHA_REGISTRO"))
                End While
                dbDataReader.Close()
                dbDataReader = Nothing
            End If

            If fecUlt.HasValue Then

                write_Log("INFO|ReintentoPermitido|La operación " & cve_mensaje & " con folio " & lsFolio & " se encuentra registrada previamente sin fondos", "")

                If Format((TimeValue(DateTime.Now)), "HH:mm") > Format((TimeValue(gsSinFondosHoraValida)), "HH:mm") Then
                    goTOMI_Database.Execute_Command(" DELETE FROM TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX WHERE FOLIO='" & lsFolio & "' AND CVE_MENSAJE='" & cve_mensaje & "' AND ESTATUS = 'SF' ")
                    write_Log("INFO|ReintentoPermitido| Se permite el reintento de la operación " & cve_mensaje & " con folio " & lsFolio & " pues la hora es válida", "")
                    Return True
                End If

                If DateTime.Compare(DateTime.Now, DateAndTime.DateAdd(DateInterval.Minute, CDbl(gsSinFondosTiempoEspera), fecUlt.Value)) > 0 Then
                    Definiciones.goTOMI_Database.Execute_Command(" DELETE FROM TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX WHERE FOLIO=" & lsFolio & " AND CVE_MENSAJE='" & cve_mensaje & "' AND ESTATUS = 'SF' ")
                    write_Log("INFO|ReintentoPermitido| Se permite el re intento de la operación " & cve_mensaje & " con folio " & lsFolio & " pues los minutos son válidos", "")
                    Return True
                End If

                write_Log("INFO|ReintentoPermitido| No se permitió el re intento de la operación " & cve_mensaje & " con folio " & lsFolio & " por no cumplir condiciones válidas", "")
                Return False
            End If

            write_Log("INFO|ReintentoPermitido|Se permite el intento de ingreso de la operación " & cve_mensaje & " con folio " & lsFolio & ". No hay histórico de falta de fondos", "")
            Return True
        Catch ex As Exception
            write_Log("ERROR|ReintentoPermitido|" & cve_mensaje & "|Se presenta error en el folio " & lsFolio & " - " & ex.ToString, "")
            Return False
        End Try
    End Function

    Public Sub ProcesaCuentaSinFondosSWF_MX(ByVal cve_mensaje As String, ByVal lsFolio As String)
        Try
            write_Log("INFO|ProcesaCuentaSinFondos| Inicia el procedimiento para tratar la operacion " & cve_mensaje & " con folio " & lsFolio & " por falta de fondos", "")
            goTOMI_Database.Execute_Command("UPDATE tb_srv_swf_plan_trabajo_maestro SET flg_con_error=0,err_descripcion='La cuenta no tiene los fondos para cubrir la transacción.' WHERE tipo_operacion= 'SWIFT_IN' AND folio='" & lsFolio & "' AND CVE_MENSAJE='" & cve_mensaje & "'")
            Dim num As Integer = 0

            Dim lrsDatos As DbDataReader = goTOMI_Database.Execute_Query("SELECT COUNT(1) AS valor FROM TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX WHERE FOLIO='" & lsFolio & "' AND CVE_MENSAJE ='" & cve_mensaje & "' AND ESTATUS = 'SF'")
            If Not IsNothing(lrsDatos) Then
                While lrsDatos.Read
                    num = CInt(lrsDatos("valor").ToString())
                End While
                lrsDatos.Close()
                lrsDatos = Nothing
            End If

            If num = 0 Then
                goTOMI_Database.Execute_Command("INSERT INTO TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX ([FOLIO],[CVE_MENSAJE],[TIPO_OPERACION],[FECHA_REGISTRO],[ESTATUS]) VALUES ('" & lsFolio & "','" & cve_mensaje & "', 'SWIFT_IN', getDate(), 'SF');")
                write_Log("INFO|ProcesaCuentaSinFondos| Inserción de la operacion " & cve_mensaje & " con folio " & lsFolio & " en la tabla TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX", "")
            Else
                goTOMI_Database.Execute_Command("UPDATE TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX SET FECHA_REGISTRO=getDate() WHERE folio='" & lsFolio & "' AND CVE_MENSAJE = '" & cve_mensaje & "' AND TIPO_OPERACION='SWIFT_IN' AND ESTATUS = 'SF'")
                write_Log("INFO|ProcesaCuentaSinFondos| Actualización de la operacion " & cve_mensaje & " con folio " & lsFolio & " en la tabla TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX", "")
            End If
        Catch ex As Exception
            write_Log("ERROR|ProcesaCuentaSinFondos|" & cve_mensaje & "|Se presenta error en el folio " & lsFolio & " - " & ex.ToString, "")

            Throw New Exception("Error al Procesar la Cuenta Sin Fondos")
        End Try
    End Sub

    Public Sub ProcesaRegistroExitoso(ByVal transactionId As String, ByVal folio As String, ByVal cve_mensaje As String)
        Dim lsSQL As String
        Try
            lsSQL = "BEGIN" & vbCrLf & _
                    " UPDATE tb_srv_swf_plan_trabajo_detalle SET flg_terminado = 1, flg_con_error = 0, fec_fin = getDate() WHERE tipo_operacion = 'SWIFT_IN' AND folio = '" & folio & "' AND alias_proceso = 'T24_REGISTRO';" & vbCrLf & _
                    " UPDATE tb_srv_swf_plan_trabajo_maestro SET flg_con_error = 0 WHERE tipo_operacion = 'SWIFT_IN' AND folio = '" & folio & "';" & vbCrLf & _
                    " UPDATE tb_srv_swf_trabajo_mx SET nf_estatus = 'PROC', t24_transaction_id = '" & transactionId & "', nf_fecha_registro = getDate(), nf_hora_autorizacion = CONVERT(VARCHAR,getDate(),108) WHERE nf_tipo = 'SWIFT_IN' AND nf_folio = '" & folio & "';" & vbCrLf & _
                    " UPDATE tb_srv_swf_trabajo SET nf_estatus = 'PROC', t24_transaction_id = '" & transactionId & "', nf_fecha_registro = getDate(), nf_hora_autorizacion = CONVERT(VARCHAR,getDate(),108) WHERE nf_tipo = 'SWIFT_IN' AND nf_folio = '" & folio & "';" & vbCrLf & _
                    " UPDATE tb_srv_swf_plan_trabajo_maestro SET T24_Ref = '" & transactionId & "' WHERE tipo_operacion = 'SWIFT_IN' AND folio = '" & folio & "';" & vbCrLf & _
                    " UPDATE TB_SRV_T24_MW_ADMON_FOLIOS_SWF_MX SET ESTATUS = 'CONF', FECHA_REGISTRO = getDate() WHERE folio='" & folio & "' AND CVE_MENSAJE = '" & cve_mensaje & "' AND TIPO_OPERACION='SWIFT_IN';" & vbCrLf & _
                    " END;"
            write_Log("INFO|ProcesaRegistroExitoso|Se actualizaron los planes de trabajo")
            goTOMI_Database.Execute_Command(lsSQL)
        Catch ex As Exception
            write_Log("ERROR|ProcesaRegistroExitoso|" & cve_mensaje & "|Se presenta error en el folio " & folio & " - " & ex.ToString, "")
            Throw New Exception("Error al registrar éxito en la transacción")
        End Try
    End Sub

    Public Sub ProcesaRegistroConError(ByVal folio As String, ByVal cve_mensaje As String, ByVal des_response As String)
        Dim lsSQL As String = String.Empty
        Try
            lsSQL = "BEGIN" & vbCrLf & _
                    " UPDATE tb_srv_swf_plan_trabajo_detalle SET flg_con_error=1,err_descripcion= '" & des_response & "' WHERE tipo_operacion='SWIFT_IN' AND folio='" & folio & "' AND alias_proceso='T24_REGISTRO'; " & vbCrLf & _
                    " UPDATE tb_srv_swf_plan_trabajo_maestro SET flg_con_error=1,err_descripcion='" & des_response & "' WHERE tipo_operacion='SWIFT_IN' AND folio='" & folio & "'; " & vbCrLf & _
                    " END;"

            goTOMI_Database.Execute_Command(lsSQL)
        Catch ex As Exception
            write_Log("ERROR|ProcesaRegistroConError|" & cve_mensaje & "|Se presenta error en el folio " & folio & " - " & ex.ToString, "")
            Throw New Exception("Error al registrar error en la transacción")
        End Try
    End Sub

    Public Function registraT24_WSH(clsTrxMaster As T24_MW_TrxMaster, clsTrxDetail As T24_MW_TrxDetail, des_variables_xml As String, ByRef lsTransactionID As String, ByRef lsError As String) As Boolean
        Dim company As String = gsWST24Company
        Dim password As String = gsWST24Password
        Dim userName As String = gsWST24UserName

        Dim result As Boolean
        Dim wsT24SwiftInRef As New wsT24SwiftIn.T24WebServicesImplClient
        Dim req As New wsT24SwiftIn.SWIFTINWITHSCHEMARequest

        Dim wrc As New wsT24SwiftIn.webRequestCommon
        wrc.company = company
        wrc.password = password
        wrc.userName = userName

        Dim ofsFunction As New wsT24SwiftIn.ofsFunction
        Dim ftSwift As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMAType
        write_Log("INFO|registraT24_WSH|  ----DATOS ENVIADOS ----  |")

        ftSwift.TRANSACTIONTYPE = clsTrxDetail.transaction_type
        write_Log("INFO|registraT24_WSH|ftSwift.TRANSACTIONTYPE " & ftSwift.TRANSACTIONTYPE)

        ftSwift.DEBITACCTNO = clsTrxDetail.debit_acct_no
        write_Log("INFO|registraT24_WSH|ftSwift.DEBITACCTNO " & ftSwift.DEBITACCTNO)

        ftSwift.DEBITCURRENCY = clsTrxDetail.debit_currency
        write_Log("INFO|registraT24_WSH|ftSwift.DEBITCURRENCY " & ftSwift.DEBITCURRENCY)

        ftSwift.DEBITAMOUNT = clsTrxDetail.debit_amount
        write_Log("INFO|registraT24_WSH|ftSwift.DEBITAMOUNT " & ftSwift.DEBITAMOUNT)

        'ftSwift.DEBITVALUEDATE =  busca_valor_xml(des_variables_xml, "settled_date")

        ftSwift.DEBITTHEIRREF = clsTrxDetail.debit_their_ref
        write_Log("INFO|registraT24_WSH|ftSwift.DEBITTHEIRREF " & ftSwift.DEBITTHEIRREF)

        ftSwift.CREDITTHEIRREF = clsTrxDetail.credit_their_ref
        write_Log("INFO|registraT24_WSH|ftSwift.CREDITTHEIRREF " & ftSwift.CREDITTHEIRREF)

        ftSwift.CREDITACCTNO = clsTrxDetail.credit_acct_no
        write_Log("INFO|registraT24_WSH|ftSwift.CREDITACCTNO " & ftSwift.CREDITACCTNO)

        ftSwift.CREDITCURRENCY = clsTrxDetail.credit_currency
        write_Log("INFO|registraT24_WSH|ftSwift.CREDITCURRENCY " & ftSwift.CREDITCURRENCY)

        ftSwift.CREDITAMOUNT = busca_valor_xml(des_variables_xml, "mensaje_moneda")

        'ftSwift.CREDITVALUEDATE =  busca_valor_xml(des_variables_xml, "settled_date")

        Dim gOrdCus As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERINGCUST

        If busca_valor_xml(des_variables_xml, "mensaje_tipo") = "PACS008" Then
            Dim orderinCustNomAddr As List(Of String) = extraeNodosXML(des_variables_xml, "ordering_name_addr")
            Dim orderingCustFinal As List(Of String) = New List(Of String)
            orderingCustFinal.Add("AC" + busca_valor_xml(des_variables_xml, "ordering_account"))
            orderingCustFinal = orderingCustFinal.Concat(orderinCustNomAddr).ToList

            gOrdCus.ORDERINGCUST = orderingCustFinal.ToArray
        Else
            gOrdCus.ORDERINGCUST = New String(0) {clsTrxDetail.ordering_cust}
        End If

        gOrdCus.g = "1"
        ftSwift.gORDERINGCUST = gOrdCus

        For Each x As String In gOrdCus.ORDERINGCUST
            x = x.Replace(":", " ")
            write_Log("INFO|registraT24_WSH|ftSwift.ORDERINGCUST " & x)
        Next

        Dim gOrdBank As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERINGBANK
        gOrdBank.ORDERINGBANK = New String(0) {clsTrxDetail.ordering_bank}
        gOrdBank.g = "1"
        ftSwift.gORDERINGBANK = gOrdBank

        For Each x In gOrdBank.ORDERINGBANK
            write_Log("INFO|registraT24_WSH|ftSwift.gORDERINGBANK " & x)
        Next

        ftSwift.BENOURCHARGES = clsTrxDetail.ben_our_charges
        write_Log("INFO|registraT24_WSH|ftSwift.BENOURCHARGES " & ftSwift.BENOURCHARGES)

        ftSwift.RELATEDREF = clsTrxDetail.related_ref
        write_Log("INFO|registraT24_WSH|ftSwift.RELATEDREF " & ftSwift.RELATEDREF)

        Dim instructions As List(Of String) = convierteTexto_a_Multivalor_SinCortar_Local(clsTrxDetail.Instructions, 65)

        Dim gInstructions As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGINSTRUCTIONS
        gInstructions.g = "1"
        gInstructions.INSTRUCTIONS = instructions.ToArray()
        ftSwift.gINSTRUCTIONS = gInstructions

        For Each x In gInstructions.INSTRUCTIONS
            write_Log("INFO|registraT24_WSH|ftSwift.INSTRUCTIONS " & x)
        Next

        Dim beneficiaryNameAddr As List(Of String) = extraeNodosXML(des_variables_xml, "beneficiary_name_addr")
        ftSwift.BENEFICIARYNAM = String.Join(", ", beneficiaryNameAddr)
        write_Log("INFO|registraT24_WSH|ftSwift.BENEFICIARYNAM " & ftSwift.BENEFICIARYNAM)

        ftSwift.SATELSYSREF = clsTrxDetail.Satel_Sys_Ref
        write_Log("INFO|registraT24_WSH|ftSwift.SATELSYSREF " & ftSwift.SATELSYSREF)


        ftSwift.CUSTREF1 = clsTrxDetail.Cust_Ref1
        ftSwift.CUSTREF2 = clsTrxDetail.Cust_Ref2
        ftSwift.CUSTREF3 = clsTrxDetail.Cust_Ref3
        ftSwift.CUSTREF4 = clsTrxDetail.Cust_Ref4
        ftSwift.CUSTREF5 = clsTrxDetail.Cust_Ref5

        write_Log("INFO|registraT24_WSH|ftSwift.CUSTREF1 " & ftSwift.CUSTREF1)
        write_Log("INFO|registraT24_WSH|ftSwift.CUSTREF2 " & ftSwift.CUSTREF2)
        write_Log("INFO|registraT24_WSH|ftSwift.CUSTREF3 " & ftSwift.CUSTREF3)
        write_Log("INFO|registraT24_WSH|ftSwift.CUSTREF4 " & ftSwift.CUSTREF4)
        write_Log("INFO|registraT24_WSH|ftSwift.CUSTREF5 " & ftSwift.CUSTREF5)

        ftSwift.SWEND2ENDREF = clsTrxDetail.SW_END2END_REF
        write_Log("INFO|registraT24_WSH|ftSwift.SWEND2ENDREF " & ftSwift.SWEND2ENDREF)

        Dim ordering_cust_nm_ext As List(Of String) = extraeNodosXML(des_variables_xml, "ordering_cust_nm_ext")
        Dim gOrdCuNmExt As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERCUNMEXT
        If ordering_cust_nm_ext.Count > 0 Then
            gOrdCuNmExt.ORDERCUNMEXT = ordering_cust_nm_ext.ToArray
        Else
            gOrdCuNmExt.ORDERCUNMEXT = New String() {}
        End If
        gOrdCuNmExt.g = "1"
        ftSwift.gORDERCUNMEXT = gOrdCuNmExt
        For Each x In gOrdCuNmExt.ORDERCUNMEXT
            x = x.Replace(":", " ")
            write_Log("INFO|registraT24_WSH|ftSwift.gORDERCUNMEXT " & x)
        Next

        Dim ordering_cust_adr_ext As List(Of String) = extraeNodosXML(des_variables_xml, "ordering_cust_adr_ext")
        Dim gOrdCuAdExt As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGORDERCUADEXT
        If ordering_cust_adr_ext.Count > 0 Then
            gOrdCuAdExt.ORDERCUADEXT = ordering_cust_adr_ext.ToArray
        Else
            gOrdCuAdExt.ORDERCUADEXT = New String() {}
        End If
        gOrdCuAdExt.g = "1"
        ftSwift.gORDERCUADEXT = gOrdCuAdExt
        For Each x In gOrdCuAdExt.ORDERCUADEXT
            x = x.Replace(":", " ")
            write_Log("INFO|registraT24_WSH|ftSwift.ORDERCUADEXT " & x)
        Next

        Dim beneficiary_name_ext As List(Of String) = extraeNodosXML(des_variables_xml, "beneficiary_name_ext")
        Dim gBenefNameExt As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWSCHEMATypeGBENEFNAMEEXT
        If beneficiary_name_ext.Count > 0 Then
            gBenefNameExt.BENEFNAMEEXT = beneficiary_name_ext.ToArray
        Else
            gBenefNameExt.BENEFNAMEEXT = New String() {}
        End If

        gBenefNameExt.g = "1"
        ftSwift.gBENEFNAMEEXT = gBenefNameExt
        For Each x In gBenefNameExt.BENEFNAMEEXT
            x = x.Replace(":", " ")
            write_Log("INFO|registraT24_WSH|ftSwift.gBENEFNAMEEXT " & x)
        Next

        req.WebRequestCommon = wrc
        req.OfsFunction = ofsFunction
        req.FUNDSTRANSFERSWIFTINWSCHEMAType = ftSwift


        Dim response As wsT24SwiftIn.SWIFTINWITHSCHEMAResponse
        response = wsT24SwiftInRef.wsT24SwiftIn_T24WebServicesImpl_SWIFTINWITHSCHEMA(req)


        write_Log("INFO|registraT24_WSH| Response " + response.Status.successIndicator.ToString)
        If response.Status.successIndicator = wsT24SwiftIn.successIndicator.Success And Not String.IsNullOrEmpty(response.Status.transactionId) Then
            lsTransactionID = response.Status.transactionId
            result = True
            write_Log("INFO|registraT24_WSH| Respuesta exitosa. Se obtuvo el FT " + lsTransactionID)
        Else
            lsError = obtieneMensajeError(response.Status.messages)
            result = False
            write_Log("ERROR|registraT24_WSH|El servicio regresó respuesta con error. Se actualizan tablas de error. " & lsError)
        End If

        Return result
    End Function

    Public Function registraT24_WOSH(clsTrxMaster As T24_MW_TrxMaster, clsTrxDetail As T24_MW_TrxDetail, des_variables_xml As String, ByRef lsTransactionID As String, ByRef lsError As String) As Boolean
        Dim company As String = gsWST24Company
        Dim password As String = gsWST24Password
        Dim userName As String = gsWST24UserName

        Dim result As Boolean
        Dim wsT24SwiftInRef As New wsT24SwiftIn.T24WebServicesImplClient
        Dim req As New wsT24SwiftIn.SWIFTINWITHOUTSCHEMARequest

        Dim wrc As New wsT24SwiftIn.webRequestCommon
        wrc.company = company
        wrc.password = password
        wrc.userName = userName

        Dim ofsFunction As New wsT24SwiftIn.ofsFunction
        Dim ftSwift As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMAType
        write_Log("INFO|registraT24_WOSH|  ----DATOS ENVIADOS ----  |")

        ftSwift.TRANSACTIONTYPE = clsTrxDetail.transaction_type
        write_Log("INFO|registraT24_WOSH|ftSwift.TRANSACTIONTYPE " & ftSwift.TRANSACTIONTYPE)

        ftSwift.DEBITACCTNO = clsTrxDetail.debit_acct_no
        write_Log("INFO|registraT24_WOSH|ftSwift.DEBITACCTNO " & ftSwift.DEBITACCTNO)

        ftSwift.DEBITCURRENCY = clsTrxDetail.debit_currency
        write_Log("INFO|registraT24_WOSH|ftSwift.DEBITCURRENCY " & ftSwift.DEBITCURRENCY)

        ftSwift.DEBITAMOUNT = clsTrxDetail.debit_amount
        write_Log("INFO|registraT24_WOSH|ftSwift.DEBITAMOUNT " & ftSwift.DEBITAMOUNT)

        'ftSwift.DEBITVALUEDATE =  busca_valor_xml(des_variables_xml, "settled_date")

        ftSwift.DEBITTHEIRREF = clsTrxDetail.debit_their_ref
        write_Log("INFO|registraT24_WOSH|ftSwift.DEBITTHEIRREF " & ftSwift.DEBITTHEIRREF)

        ftSwift.CREDITTHEIRREF = clsTrxDetail.credit_their_ref
        write_Log("INFO|registraT24_WOSH|ftSwift.CREDITTHEIRREF " & ftSwift.CREDITTHEIRREF)

        ftSwift.CREDITACCTNO = clsTrxDetail.credit_acct_no
        write_Log("INFO|registraT24_WOSH|ftSwift.CREDITACCTNO " & ftSwift.CREDITACCTNO)

        ftSwift.CREDITCURRENCY = clsTrxDetail.credit_currency
        write_Log("INFO|registraT24_WOSH|ftSwift.CREDITCURRENCY " & ftSwift.CREDITCURRENCY)

        ftSwift.CREDITAMOUNT = clsTrxDetail.credit_amount

        'ftSwift.CREDITVALUEDATE =  busca_valor_xml(des_variables_xml, "settled_date")

        Dim gOrdCus As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERINGCUST

        If busca_valor_xml(des_variables_xml, "mensaje_tipo") = "PACS008" Then
            Dim orderinCustNomAddr As List(Of String) = extraeNodosXML(des_variables_xml, "ordering_name_addr")
            Dim orderingCustFinal As List(Of String) = New List(Of String)
            orderingCustFinal.Add("AC" + busca_valor_xml(des_variables_xml, "ordering_account"))
            orderingCustFinal = orderingCustFinal.Concat(orderinCustNomAddr).ToList

            gOrdCus.ORDERINGCUST = orderingCustFinal.ToArray
        Else
            gOrdCus.ORDERINGCUST = New String(0) {clsTrxDetail.ordering_cust}
        End If

        gOrdCus.g = "1"
        ftSwift.gORDERINGCUST = gOrdCus

        For Each x In gOrdCus.ORDERINGCUST
            x = x.Replace(":", " ")
            write_Log("INFO|registraT24_WOSH|ftSwift.ORDERINGCUST " & x)
        Next

        Dim gOrdBank As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERINGBANK
        gOrdBank.ORDERINGBANK = New String(0) {clsTrxDetail.ordering_bank}
        gOrdBank.g = "1"
        ftSwift.gORDERINGBANK = gOrdBank

        For Each x In gOrdBank.ORDERINGBANK
            write_Log("INFO|registraT24_WOSH|ftSwift.gORDERINGBANK " & x)
        Next

        ftSwift.BENOURCHARGES = clsTrxDetail.ben_our_charges
        write_Log("INFO|registraT24_WOSH|ftSwift.BENOURCHARGES " & ftSwift.BENOURCHARGES)

        ftSwift.RELATEDREF = clsTrxDetail.related_ref
        write_Log("INFO|registraT24_WOSH|ftSwift.RELATEDREF " & ftSwift.RELATEDREF)

        Dim instructions As List(Of String) = convierteTexto_a_Multivalor_SinCortar_Local(clsTrxDetail.Instructions, 65)

        Dim gInstructions As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGINSTRUCTIONS
        gInstructions.g = "1"
        gInstructions.INSTRUCTIONS = instructions.ToArray()
        ftSwift.gINSTRUCTIONS = gInstructions

        For Each x In gInstructions.INSTRUCTIONS
            write_Log("INFO|registraT24_WOSH|ftSwift.INSTRUCTIONS " & x)
        Next

        Dim beneficiaryNameAddr As List(Of String) = extraeNodosXML(des_variables_xml, "beneficiary_name_addr")
        ftSwift.BENEFICIARYNAM = String.Join(", ", beneficiaryNameAddr)
        write_Log("INFO|registraT24_WOSH|ftSwift.BENEFICIARYNAM " & ftSwift.BENEFICIARYNAM)

        ftSwift.SATELSYSREF = clsTrxDetail.Satel_Sys_Ref
        write_Log("INFO|registraT24_WOSH|ftSwift.SATELSYSREF " & ftSwift.SATELSYSREF)


        ftSwift.CUSTREF1 = clsTrxDetail.Cust_Ref1
        ftSwift.CUSTREF2 = clsTrxDetail.Cust_Ref2
        ftSwift.CUSTREF3 = clsTrxDetail.Cust_Ref3
        ftSwift.CUSTREF4 = clsTrxDetail.Cust_Ref4
        ftSwift.CUSTREF5 = clsTrxDetail.Cust_Ref5

        write_Log("INFO|registraT24_WOSH|ftSwift.CUSTREF1 " & ftSwift.CUSTREF1)
        write_Log("INFO|registraT24_WOSH|ftSwift.CUSTREF2 " & ftSwift.CUSTREF2)
        write_Log("INFO|registraT24_WOSH|ftSwift.CUSTREF3 " & ftSwift.CUSTREF3)
        write_Log("INFO|registraT24_WOSH|ftSwift.CUSTREF4 " & ftSwift.CUSTREF4)
        write_Log("INFO|registraT24_WOSH|ftSwift.CUSTREF5 " & ftSwift.CUSTREF5)

        ftSwift.SWEND2ENDREF = clsTrxDetail.SW_END2END_REF
        write_Log("INFO|registraT24_WOSH|ftSwift.SWEND2ENDREF " & ftSwift.SWEND2ENDREF)

        Dim ordering_cust_nm_ext As List(Of String) = extraeNodosXML(des_variables_xml, "ordering_cust_nm_ext")
        Dim gOrdCuNmExt As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERCUNMEXT
        If ordering_cust_nm_ext.Count > 0 Then
            gOrdCuNmExt.ORDERCUNMEXT = ordering_cust_nm_ext.ToArray
        Else
            gOrdCuNmExt.ORDERCUNMEXT = New String() {}
        End If
        gOrdCuNmExt.g = "1"
        ftSwift.gORDERCUNMEXT = gOrdCuNmExt
        For Each x In gOrdCuNmExt.ORDERCUNMEXT
            x = x.Replace(":", " ")
            write_Log("INFO|registraT24_WOSH|ftSwift.gORDERCUNMEXT " & x)
        Next

        Dim ordering_cust_adr_ext As List(Of String) = extraeNodosXML(des_variables_xml, "ordering_cust_adr_ext")
        Dim gOrdCuAdExt As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGORDERCUADEXT
        If ordering_cust_adr_ext.Count > 0 Then
            gOrdCuAdExt.ORDERCUADEXT = ordering_cust_adr_ext.ToArray
        Else
            gOrdCuAdExt.ORDERCUADEXT = New String() {}
        End If
        gOrdCuAdExt.g = "1"
        ftSwift.gORDERCUADEXT = gOrdCuAdExt
        For Each x In gOrdCuAdExt.ORDERCUADEXT
            x = x.Replace(":", " ")
            write_Log("INFO|registraT24_WOSH|ftSwift.ORDERCUADEXT " & x)
        Next

        Dim beneficiary_name_ext As List(Of String) = extraeNodosXML(des_variables_xml, "beneficiary_name_ext")
        Dim gBenefNameExt As New wsT24SwiftIn.FUNDSTRANSFERSWIFTINWOSCHEMATypeGBENEFNAMEEXT
        If beneficiary_name_ext.Count > 0 Then
            gBenefNameExt.BENEFNAMEEXT = beneficiary_name_ext.ToArray
        Else
            gBenefNameExt.BENEFNAMEEXT = New String() {}
        End If

        gBenefNameExt.g = "1"
        ftSwift.gBENEFNAMEEXT = gBenefNameExt
        For Each x In gBenefNameExt.BENEFNAMEEXT
            x = x.Replace(":", " ")
            write_Log("INFO|registraT24_WOSH|ftSwift.gBENEFNAMEEXT " & x)
        Next

        req.WebRequestCommon = wrc
        req.OfsFunction = ofsFunction
        req.FUNDSTRANSFERSWIFTINWOSCHEMAType = ftSwift


        Dim response As wsT24SwiftIn.SWIFTINWITHOUTSCHEMAResponse
        response = wsT24SwiftInRef.wsT24SwiftIn_T24WebServicesImpl_SWIFTINWITHOUTSCHEMA(req)

        write_Log("INFO|registraT24_WOSH | Response " + response.Status.successIndicator.ToString)
        If response.Status.successIndicator = wsT24SwiftIn.successIndicator.Success And Not String.IsNullOrEmpty(response.Status.transactionId) Then
            lsTransactionID = response.Status.transactionId
            result = True
            write_Log("INFO|registraT24_WOSH| Respuesta exitosa. Se obtuvo el FT " + lsTransactionID)
        Else
            lsError = obtieneMensajeError(response.Status.messages)
            result = False
            write_Log("ERROR|registraT24_WOSH|El servicio regresó respuesta con error. Se actualizan tablas de error. " & lsError)
        End If

        Return result
    End Function

    Public Function extraeNodosXML(ByRef variablesXML As String, nombre As String) As List(Of String)
        Try
            Dim result As List(Of String) = New List(Of String)
            Dim loXML_Doc As Xml.XmlDocument, loXML_Nodes As Xml.XmlNodeList

            loXML_Doc = New Xml.XmlDocument()
            loXML_Doc.LoadXml(variablesXML)
            If Not loXML_Doc.InnerXml Is String.Empty Then
                loXML_Nodes = loXML_Doc.GetElementsByTagName(nombre)
                If loXML_Nodes.Count > 0 Then
                    For Each loXML_Node As Xml.XmlNode In loXML_Nodes(0).ChildNodes
                        result.Add(loXML_Node.InnerText)
                    Next
                End If
            End If
            Return result
        Catch lexErrorXML As Exception
            Throw New Exception("Error al obtener  nodos del XML:" & lexErrorXML.Message)
        End Try
    End Function

    Private Sub ImprimirMasterDetail(clsTrxMaster As T24_MW_TrxMaster, clsTrxDetail As T24_MW_TrxDetail)
        write_Log("T24_MW_TrxMaster")
        write_Log("T24_MW_TrxMaster.TrxCompany_ID " + clsTrxMaster.TrxCompany_ID)
        write_Log("T24_MW_TrxMaster.TrxInternal_Type " + clsTrxMaster.TrxInternal_Type)
        write_Log("T24_MW_TrxMaster.TrxOperation_Acct " + clsTrxMaster.TrxOperation_Acct)
        write_Log("T24_MW_TrxMaster.TrxOperation_Folio " + clsTrxMaster.TrxOperation_Folio)
        write_Log("T24_MW_TrxMaster.TrxOperation_ID " + clsTrxMaster.TrxOperation_ID)
        write_Log("T24_MW_TrxMaster.TrxOperation_Specific_Type " + clsTrxMaster.TrxOperation_Specific_Type)
        write_Log("T24_MW_TrxMaster.TrxOperation_SubType " + clsTrxMaster.TrxOperation_SubType)
        write_Log("T24_MW_TrxMaster.TrxSystem_ID " + clsTrxMaster.TrxSystem_ID)
        write_Log("T24_MW_TrxMaster.TrxTransaction_Type " + clsTrxMaster.TrxTransaction_Type)
        write_Log("T24_MW_TrxMaster.TrxUser " + clsTrxMaster.TrxUser)

        write_Log("T24_MW_TrxDetail")
        write_Log("T24_MW_TrxDetail.ben_our_charges " + clsTrxDetail.ben_our_charges)
        write_Log("T24_MW_TrxDetail.Beneficiary_Name " + clsTrxDetail.Beneficiary_Name)
        write_Log("T24_MW_TrxDetail.commission_type " + clsTrxDetail.commission_type)
        write_Log("T24_MW_TrxDetail.credit_acct_no " + clsTrxDetail.credit_acct_no)
        write_Log("T24_MW_TrxDetail.credit_currency " + clsTrxDetail.credit_currency)
        write_Log("T24_MW_TrxDetail.credit_their_ref " + clsTrxDetail.credit_their_ref)
        write_Log("T24_MW_TrxDetail.Cust_Ref1 " + clsTrxDetail.Cust_Ref1)
        write_Log("T24_MW_TrxDetail.Cust_Ref2 " + clsTrxDetail.Cust_Ref2)
        write_Log("T24_MW_TrxDetail.Cust_Ref3 " + clsTrxDetail.Cust_Ref3)
        write_Log("T24_MW_TrxDetail.Cust_Ref4 " + clsTrxDetail.Cust_Ref4)
        write_Log("T24_MW_TrxDetail.Cust_Ref5 " + clsTrxDetail.Cust_Ref5)
        write_Log("T24_MW_TrxDetail.debit_acct_no " + clsTrxDetail.debit_acct_no)
        write_Log("T24_MW_TrxDetail.debit_amount " + clsTrxDetail.debit_amount)
        write_Log("T24_MW_TrxDetail.debit_currency " + clsTrxDetail.debit_currency)
        write_Log("T24_MW_TrxDetail.debit_their_ref " + clsTrxDetail.debit_their_ref)
        write_Log("T24_MW_TrxDetail.Instructions " + clsTrxDetail.Instructions)
        write_Log("T24_MW_TrxDetail.ordering_bank " + clsTrxDetail.ordering_bank)
        write_Log("T24_MW_TrxDetail.ordering_cust " + clsTrxDetail.ordering_cust)
        write_Log("T24_MW_TrxDetail.related_ref " + clsTrxDetail.related_ref)
        write_Log("T24_MW_TrxDetail.Satel_Sys_Ref " + clsTrxDetail.Satel_Sys_Ref)
        write_Log("T24_MW_TrxDetail.SW_END2END_REF " + clsTrxDetail.SW_END2END_REF)
        write_Log("T24_MW_TrxDetail.transaction_type " + clsTrxDetail.transaction_type)
    End Sub

    Private Function obtieneMensajeError(arrayError As String()) As String
        Dim messageGeneralError As String = String.Empty
        If arrayError.Length > 0 Then
            For cnt As Int16 = 0 To arrayError.Length - 1 Step 1
                messageGeneralError &= arrayError(cnt)
                If cnt <> arrayError.Length - 1 Then
                    messageGeneralError &= "|"
                End If
            Next
        End If
        Return messageGeneralError
    End Function

   
   Private Sub LimpiarCamposDetail(ByRef clsTrxDetail As T24_MW_TrxDetail)
       Dim _type As Type = clsTrxDetail.GetType
       Dim propertiesList As System.Reflection.PropertyInfo() = _type.GetProperties()

       For Each prop As System.Reflection.PropertyInfo In propertiesList
           If prop.PropertyType Is GetType(String) Then
               If Not prop.GetValue(clsTrxDetail, Nothing) Is Nothing Then
                   prop.SetValue(clsTrxDetail,
                                 prop.GetValue(clsTrxDetail, Nothing).Replace(",", "?").Replace(Chr(34), "|").Replace(
                                     "/", "^"), Nothing)
               End If
           End If
       Next
       clsTrxDetail.Instructions = clsTrxDetail.Instructions.Replace(":", " ")
   End Sub

    Private Sub LimpiarCamposXMLVariables(ByRef des_variables_xml As String)
        Dim doc As Xml.XmlDocument = New Xml.XmlDocument()
        doc.LoadXml(des_variables_xml)

        For Each el As Xml.XmlElement In doc.SelectNodes("//*[not(*)]")
            el.InnerText = el.InnerText.Replace(",", "?").Replace(Chr(34), "|").Replace("/", "^")
        Next

        des_variables_xml = doc.OuterXml
    End Sub

    Private Function SanitizeBic11(bic As String) As String
        If String.IsNullOrWhiteSpace(bic)
            Return String.Empty
        End If

        If bic.Length = 8 Then
            Return bic & "XXX"
        End If

        If bic.Length = 11 Then
            If bic.Substring(0, 3) = "SW-" Then
                bic = bic.Substring(3)
                Return bic & "XXX"
            End If
            Return bic
        End If

        If bic.Substring(0, 3) = "SW-" Then
            bic = bic.Substring(3)
            Return bic
        End If

        return String.Empty
    End Function
End Module
