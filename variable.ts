Public Class MT_210
    Private Const tipoOperacion As String = "MT-210"

    Shared Function procesaOperacion(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        Try
            Dim loCmd_TOMI As DbCommand
            Dim lsReferencia As String, lsSwiftEmisor As String, lsSwiftOrdenante As String
            Dim lbOrdenanteMUFG As Boolean
            Dim liSalida As Integer

            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Iniciando el registro de la operación.")

            lsSwiftEmisor = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_swift_emisor")
            If lsSwiftEmisor.Length > 8 Then
                lsSwiftEmisor = lsSwiftEmisor.Substring(0, 8)
            End If
            If lsSwiftEmisor = "" Then
                gexUltimaExcepcion = New Exception("No existe información del swift emisor.")
                Return False
            End If

            ' Es un ordenante del grupo
            lbOrdenanteMUFG = False
            lsSwiftOrdenante = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_swift_vostro")
            write_Log("AVISO|" & tipoOperacion & ".procesaOperacion|Swift ordenante='" & lsSwiftOrdenante & "'.")
            If lsSwiftOrdenante.StartsWith("BOTK") Or lsSwiftOrdenante.StartsWith("BOFCUS33") Then
                lbOrdenanteMUFG = True
                write_Log("AVISO|" & tipoOperacion & ".procesaOperacion|El Swift ordenante es referencia interna MUFG.")
            End If
            lsReferencia = busca_valor_xml(datos("des_variables_xml"), "sender_reference") & IIf(lbOrdenanteMUFG, " MUFG", "")

            goTOMI_Database.BeginTransaction("MT210")
            ' Limpiado de parámetros
            loCmd_TOMI = goTOMI_Database.newCommand_Transaction("usp_swf_btmum_210_add_ref")
            loCmd_TOMI.CommandType = CommandType.StoredProcedure
            loCmd_TOMI.Parameters.Clear()
            goTOMI_Database.AddParameter(loCmd_TOMI, "@icn", icn, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@fec_valor", datos("fec_valor"), DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@cve_swift", lsSwiftEmisor, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@des_referencia", lsReferencia, DbType.String)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@num_importe", datos("des_importe"), DbType.Decimal)
            goTOMI_Database.AddParameter(loCmd_TOMI, "@Salida", -1, DbType.Int16, , ParameterDirection.Output)

            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|usp_swf_btmum_210_add_ref. Pasaron parametros, inicia ejecución.")
            'Termina Definición de parámetros y hace el llamado
            loCmd_TOMI.ExecuteNonQuery()
            liSalida = loCmd_TOMI.Parameters("@Salida").Value
            loCmd_TOMI = Nothing

            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Paso la ejecución.")

            If liSalida <> 0 Then
                write_Log("INFO|" & tipoOperacion & ".procesaOperacion|CON ERROR.")

                goTOMI_Database.Rollback_Transaction()
                gexUltimaExcepcion = New Exception("No se pudo insertar el mensaje")
                Return False
            End If

            'If termina_registro(icn, datos) Then
            goTOMI_Database.Commit_Transaction()
            ' Termino de la operación
            marca_op_terminada(icn, "PROC", "JEAI")
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Proceso terminado.")
            Return True
            'End If
        Catch lex210 As Exception
            write_Log("ERROR|" & tipoOperacion & ".procesaOperacion|Error='" & lex210.ToString & "'.")
            gexUltimaExcepcion = lex210
        End Try

        Throw gexUltimaExcepcion
        Return False
    End Function
End Class
