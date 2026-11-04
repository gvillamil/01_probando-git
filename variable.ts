Private Sub Procesa_Bancos()
    Dim lrsDatos As DbDataReader
    Dim laListaFolios As ArrayList
    Dim lsFolio As String
    Dim laValores_Formato(18) As Object
    Dim liResultado As Integer

    Try
        ' Buscamos las solicitudes
        laListaFolios = New ArrayList()
        lrsDatos = goTOMI_Database.Execute_Query("SELECT id_solicitud FROM tomser.dbo.solicitudes_bancos WHERE status IN ('AUTORIZADO')")
        If Not IsNothing(lrsDatos) Then
            Do While lrsDatos.Read
                laListaFolios.Add(lrsDatos.Item(0))
            Loop
            lrsDatos.Close()
            lrsDatos = Nothing
        End If

        ' Analizamos las solicitudes
        For Each lsFolio In laListaFolios
            write_Log("INFO|Procesa_Bancos|Procesando solicitudes de Bancos: " & lsFolio)
            lrsDatos = goTOMI_Database.Execute_Query("SELECT * FROM tomser.dbo.solicitudes_bancos WHERE id_solicitud=" & lsFolio)
            If Not IsNothing(lrsDatos) Then
                Do While lrsDatos.Read
                    laValores_Formato(1) = lrsDatos.Item(0)     'id_solicitud
                    laValores_Formato(2) = lrsDatos.Item(1)     'tipo
                    laValores_Formato(3) = lrsDatos.Item(2)     'aba
                    laValores_Formato(4) = lrsDatos.Item(3)     'cus_name
                    laValores_Formato(5) = lrsDatos.Item(4)     'state
                    laValores_Formato(6) = lrsDatos.Item(5)     'city
                    laValores_Formato(7) = lrsDatos.Item(6)     'currency
                    laValores_Formato(8) = lrsDatos.Item(7)     'ban_id
                    laValores_Formato(9) = lrsDatos.Item(8)     'swift_ben
                    laValores_Formato(10) = lrsDatos.Item(9)    'cus_name_ben
                    laValores_Formato(11) = lrsDatos.Item(10)   'cus_name_acc
                    laValores_Formato(12) = lrsDatos.Item(11)   'ban_id_int
                    laValores_Formato(13) = lrsDatos.Item(12)   'swift_int
                    laValores_Formato(14) = lrsDatos.Item(13)   'cus_name_int
                    laValores_Formato(15) = lrsDatos.Item(14)   'swift_oth
                    laValores_Formato(16) = lrsDatos.Item(15)   'cus_name_oth
                    laValores_Formato(17) = lrsDatos.Item(16)   'currency_oth
                    laValores_Formato(18) = lrsDatos.Item(27)   'operacion
                Loop
                lrsDatos.Close()
                lrsDatos = Nothing

                write_Log("INFO|Procesa_Bancos|Procesando solicitudes de Bancos: " & lsFolio)
                liResultado = Procesa_Autorizacion_Bancos(laValores_Formato)
                If liResultado > 0 Then
                    liResultado = goTOMI_Database.Execute_Command("UPDATE tomser.dbo.solicitudes_bancos SET status='PROCESADO' WHERE id_solicitud=" & laValores_Formato(1))
                    write_Log("INFO|Procesa_Bancos|Se termina procesamiento de solicitudes de Bancos: " & lsFolio)
                Else
                    write_Log("ERROR|Procesa_Bancos|Error en procesamiento de solicitudes de Bancos: " & lsFolio)
                End If
            End If
        Next lsFolio
    Catch lexProcesaBancos As Exception
        write_Log("ERROR|Procesa_Bancos|Ocurre exception: " & lexProcesaBancos.ToString)
    End Try
End Sub


Private Function Procesa_Autorizacion_Bancos(ByVal Formato_Valores As Object) As Integer
    Dim lsSQL As String, lsSQL2 As String
    Dim liRegs As Integer

    Try
        If Formato_Valores(2) = 1 Then
            If Formato_Valores(18) = "ALTA" Then
                lsSQL = "INSERT INTO abausa (ABA,CUS_NAME,STATE,CITY,CURRENCY) VALUES ('" & Formato_Valores(3) & "','" & Formato_Valores(4) & "','" & Formato_Valores(5) & "','" & Formato_Valores(6) & "','" & Formato_Valores(7) & "')"
                liRegs = goCMS_Database.Execute_Command(lsSQL)
                If liRegs > 0 Then
                    lsSQL = "INSERT INTO tomser.dbo.abausa (ABA,CUS_NAME,STATE,CITY,CURRENCY) VALUES ('" & Formato_Valores(3) & "','" & Formato_Valores(4) & "','" & Formato_Valores(5) & "','" & Formato_Valores(6) & "','" & Formato_Valores(7) & "');" & vbCrLf & _
                            "INSERT INTO tb_bilun_abausa (ABA,CUS_NAME,STATE,CITY,CURRENCY) VALUES ('" & Formato_Valores(3) & "','" & Formato_Valores(4) & "','" & Formato_Valores(5) & "','" & Formato_Valores(6) & "','" & Formato_Valores(7) & "');"
                    liRegs = goTOMI_Database.Execute_Command(lsSQL)
                    If liRegs > 0 Then
                        Return liRegs
                    Else
                        Return 0
                    End If
                Else
                    Return 0
                End If
            End If
        ElseIf Formato_Valores(2) = 2 Then
            If Formato_Valores(18) = "ALTA" Then
                lsSQL = "INSERT INTO swift_mex (BAN_ID,SWIFT_BEN,CUS_NAME_BEN,CUS_NAME_ACC) VALUES (" & Formato_Valores(8) & ",'" & Formato_Valores(9) & "','" & Formato_Valores(10) & "','" & Formato_Valores(11) & "')"
                lsSQL2 = "INSERT INTO swift_mex_int (BAN_ID_MEX,BAN_ID_INT,SWIFT_INT,CUS_NAME_INT,CUS_NAME_ACC) VALUES (" & Formato_Valores(8) & ",1,'" & Formato_Valores(13) & "','" & Formato_Valores(14) & "','')"
                liRegs = goCMS_Database.Execute_Command(lsSQL)
                If liRegs > 0 Then
                    liRegs = goCMS_Database.Execute_Command(lsSQL2)
                    If liRegs > 0 Then
                        lsSQL = "INSERT INTO tomser.dbo.swift_mex (BAN_ID,SWIFT_BEN,CUS_NAME_BEN,CUS_NAME_ACC) VALUES (" & Formato_Valores(8) & ",'" & Formato_Valores(9) & "','" & Formato_Valores(10) & "','" & Formato_Valores(11) & "');" & vbCrLf & _
                                "INSERT INTO tb_bilun_swift_mex (BAN_ID,SWIFT_BEN,CUS_NAME_BEN,CUS_NAME_ACC) VALUES (" & Formato_Valores(8) & ",'" & Formato_Valores(9) & "','" & Formato_Valores(10) & "','" & Formato_Valores(11) & "');"
                        lsSQL2 = "INSERT INTO tomser.dbo.swift_mex_int (BAN_ID_MEX,BAN_ID_INT,SWIFT_INT,CUS_NAME_INT,CUS_NAME_ACC) VALUES (" & Formato_Valores(8) & ",1,'" & Formato_Valores(13) & "','" & Formato_Valores(14) & "','');" & vbCrLf & _
                                 "INSERT INTO tb_bilun_swift_mex_int (BAN_ID_MEX,BAN_ID_INT,SWIFT_INT,CUS_NAME_INT,CUS_NAME_ACC) VALUES (" & Formato_Valores(8) & ",1,'" & Formato_Valores(13) & "','" & Formato_Valores(14) & "','');"
                        liRegs = goTOMI_Database.Execute_Command(lsSQL)
                        If liRegs > 0 Then
                            liRegs = goTOMI_Database.Execute_Command(lsSQL2)
                            If liRegs > 0 Then
                                Return liRegs
                            Else
                                Return 0
                            End If
                        Else
                            Return 0
                        End If
                    Else
                        Return 0
                    End If
                Else
                    Return 0
                End If
            End If
        ElseIf Formato_Valores(2) = 3 Then
            If Formato_Valores(18) = "ALTA" Then
                lsSQL = "INSERT INTO swift_othben_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) VALUES (" & Formato_Valores(8) & ",'" & Formato_Valores(9) & "','" & Formato_Valores(10) & "')"
                lsSQL2 = "INSERT INTO swift_othint_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) VALUES (" & Formato_Valores(12) & ",'" & Formato_Valores(13) & "','" & Formato_Valores(14) & "')"
                liRegs = goCMS_Database.Execute_Command(lsSQL)
                If liRegs > 0 Then
                    liRegs = goCMS_Database.Execute_Command(lsSQL2)
                    If liRegs > 0 Then
                        lsSQL = "INSERT INTO tomser.dbo.swift_othben_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) VALUES (" & Formato_Valores(8) & ",'" & Formato_Valores(9) & "','" & Formato_Valores(10) & "');" & vbCrLf & _
                                "INSERT INTO tb_bilun_swift_othben_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) VALUES (" & Formato_Valores(8) & ",'" & Formato_Valores(9) & "','" & Formato_Valores(10) & "');"
                        lsSQL2 = "INSERT INTO tomser.dbo.swift_othint_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) VALUES (" & Formato_Valores(12) & ",'" & Formato_Valores(13) & "','" & Formato_Valores(14) & "');" & vbCrLf & _
                                 "INSERT INTO tb_bilun_swift_othint_usd (BAN_ID,SWIFT_BEN,CUS_NAME_BEN) VALUES (" & Formato_Valores(12) & ",'" & Formato_Valores(13) & "','" & Formato_Valores(14) & "');"
                        liRegs = goTOMI_Database.Execute_Command(lsSQL)
                        If liRegs > 0 Then
                            liRegs = goTOMI_Database.Execute_Command(lsSQL2)
                            If liRegs > 0 Then
                                Return liRegs
                            Else
                                Return 0
                            End If
                        Else
                            Return 0
                        End If
                    Else
                        Return 0
                    End If
                Else
                    Return 0
                End If
            End If
        ElseIf Formato_Valores(2) = 4 Then
            If Formato_Valores(18) = "ALTA" Then
                lsSQL = "INSERT INTO others_swift (ABA,CUS_NAME,CURRENCY) VALUES ('" & Formato_Valores(15) & "','" & Formato_Valores(16) & "','" & Formato_Valores(17) & "')"
                liRegs = goCMS_Database.Execute_Command(lsSQL)
                If liRegs > 0 Then
                    lsSQL = "INSERT INTO tomser.dbo.others_swift (ABA,CUS_NAME,CURRENCY) VALUES ('" & Formato_Valores(15) & "','" & Formato_Valores(16) & "','" & Formato_Valores(17) & "');" & vbCrLf & _
                            "INSERT INTO tb_bilun_others_swift (ABA,CUS_NAME,CURRENCY) VALUES ('" & Formato_Valores(15) & "','" & Formato_Valores(16) & "','" & Formato_Valores(17) & "');"
                    liRegs = goTOMI_Database.Execute_Command(lsSQL)
                    If liRegs > 0 Then
                        Return liRegs
                    Else
                        Return 0
                    End If
                Else
                    Return 0
                End If
            End If
        End If
    Catch lexProcesaAutBancos As Exception
        write_Log("ERROR|Procesa_Autorizacion_Bancos|Se presenta exception: " & lexProcesaAutBancos.ToString)
    End Try
End Function
