Private Sub Actualiza_Clientes()
    Dim lrsDatos As DbDataReader
    Dim laListaFolios As ArrayList
    Dim lsFolio As String
    Dim liResultado As Integer
    Dim lsRFC As String, lsNombre As String, lsTipo As String, lsNumCteT24 As String

    Try
        ' Buscamos las solicitudes
        laListaFolios = New ArrayList()
        lrsDatos = goTOMI_Database.Execute_Query("SELECT satm02cod FROM tomser.dbo.satm002clientes WHERE satm02int='SI' AND satm02rep=1 ORDER BY satm02cod")
        If Not IsNothing(lrsDatos) Then
            Do While lrsDatos.Read
                laListaFolios.Add(lrsDatos.Item(0))
            Loop
            lrsDatos.Close()
            lrsDatos = Nothing
        End If

        ' Analizamos las solicitudes
        For Each lsFolio In laListaFolios
            write_Log("INFO|Actualiza_Clientes|Procesando Cliente: " & lsFolio)

            ' Inicio de variables
            lsRFC = ""
            lsNombre = ""
            lsTipo = ""
            lsNumCteT24 = ""

            liResultado = goCMS_Database.Execute_Command("DELETE FROM cliente WHERE id_cliente=" & lsFolio)
            lrsDatos = goTOMI_Database.Execute_Query("SELECT satm02rfc,satm02nom,satm02tip,satm02cdl FROM tomser.dbo.satm002clientes WHERE satm02cod=" & lsFolio)
            If Not IsNothing(lrsDatos) Then
                Do While lrsDatos.Read
                    lsRFC = lrsDatos.Item(0)
                    lsNombre = lrsDatos.Item(1)
                    lsTipo = lrsDatos.Item(2)
                    lsNumCteT24 = lrsDatos.Item(3)
                Loop
                lrsDatos.Close()
                lrsDatos = Nothing
            End If
            liResultado = goCMS_Database.Execute_Command("INSERT INTO cliente (id_cliente,rfc,razonsocial,tipocliente,id_cte_T24) VALUES (" & lsFolio & ",'" & lsRFC & "','" & lsNombre & "','" & lsTipo & "'," & lsNumCteT24 & ")")
            liResultado = goTOMI_Database.Execute_Command("UPDATE tomser.dbo.satm002clientes SET satm02rep=0 WHERE satm02cod=" & lsFolio)
            liResultado = goTOMI_Database.Execute_Command("UPDATE tb_hd_clientes SET satm02rep=0 WHERE satm02cod=" & lsFolio)

            ' Log
            write_Log("INFO|Actualiza_Clientes|Se termina procesamiento Cliente: " & lsFolio)
        Next
        laListaFolios.Clear()
    Catch lexActualizaClientes As Exception
        write_Log("ERROR|Actualiza_Clientes|Se presenta exception: " & lexActualizaClientes.ToString)
    End Try
End Sub
