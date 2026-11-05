Private Sub Actualiza_Email()
    Dim lrsDatos As DbDataReader
    Dim liResultado As Integer
    Dim lsUsuario As String, lsNewEmail As String, lsmMgCMS As String, lsSubject As String
    Dim lsMensaje As String

    Try
        'Iniciamos variables
        lsUsuario = ""
        lsNewEmail = ""
        lsmMgCMS = ""
        lsSubject = ""

        ' Buscamos los usuarios con email pendidnte de actualizar
        lrsDatos = goCMS_Database.Execute_Query("select usuario, new_mail, mensaje, asunto from solicitudes_usuario_mails where estatus = 'CAPTURADO'")

        If Not IsNothing(lrsDatos) Then
            Dim dT As New DataTable
            dT.Load(lrsDatos)
            lrsDatos.Close()
            lrsDatos = Nothing

            For Each row As DataRow In dT.Rows
                lsUsuario = row.Item(0)
                lsNewEmail = row.Item(1)
                lsmMgCMS = row.Item(2)
                lsSubject = row.Item(3)

                write_Log("INFO|Actualiza_Email|Procesando Email de usuario: " & lsUsuario)

                'Actualizamos el Email
                If lsUsuario <> "" Then
                    liResultado = goTOMI_Database.Execute_Command("UPDATE tb_hd_usuarios_cms set SATM04EMA = '" & lsNewEmail & "' where SATM04LOG  = '" & lsUsuario & "'")
                    liResultado = goTOMI_Database.Execute_Command("UPDATE Tomser.dbo.satm004usuarios_cms set SATM04EMA = '" & lsNewEmail & "' where SATM04LOG  = '" & lsUsuario & "'")
                End If
                'Se manda el email de notificacion
                ' Mensaje HTML
                lsMensaje = "<html>" & vbNewLine & _
                            "<head>" & vbNewLine & _
                            "<style type='text/css'>" & vbNewLine & _
                            ".titulo_general { font-family: sans-serif; font-size:16px; color: #FF1010; font-weight:bold; text-decoration:underline; }" & vbNewLine & _
                            ".titulo_tabla   { font-family: sans-serif; font-size:14px; color: #0000FF; font-weight:bold; text-decoration:none; }" & vbNewLine & _
                            ".header_tabla   { font-family: sans-serif; font-size:12px; background-color:#F1F1F1; color: #2010FF; text-decoration:none; }" & vbNewLine & _
                            ".dato_tabla     { font-family: sans-serif; font-size:12px; color: #2050FF; text-decoration:none; }" & vbNewLine & _
                            ".Nota           { font-family: sans-serif; font-size:12px; color: #A0A0A0; text-decoration:none; }" & vbNewLine & _
                            ".NotaOmitir     { font-family: sans-serif; font-size:14px; color: #FF0000; text-decoration:none; }" & vbNewLine & _
                            "table           { border: thin solid #E0E0E0 }" & vbNewLine & _
                            "</style>" & vbNewLine & _
                            "</head>" & vbNewLine & _
                            "<body>" & lsmMgCMS &
                            "</body>" & vbNewLine & _
                            "</html>"

                ' Destinatarios
                Envia_Email(New String() {"avisos_cms@mx.mufg.jp", "Avisos MUFG Bank México"}, lsNewEmail, Nothing, Nothing, lsSubject, lsMensaje)

                'Actualizamos la tabla control como PROCESADO
                liResultado = goCMS_Database.Execute_Command("UPDATE solicitudes_usuario_mails SET estatus = 'PROCESADO' WHERE usuario = '" & lsUsuario & "'")
            Next
        End If
    Catch lexActualizaEmail As Exception
        write_Log("ERROR|Actualiza_Email|Se presenta exception: " & lexActualizaEmail.ToString)
    End Try
End Sub
