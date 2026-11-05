Private Sub Actualiza_Cuentas()
    Dim lrsDatos As DbDataReader
    Dim laListaFolios As ArrayList
    Dim lsFolio As String
    Dim liResultado As Integer
    Dim liCliente As Integer, lsTipoMoneda As String, lsNombreCuenta As String, liCliente_Alt As Integer, lbNomina As Boolean

    Try
        ' Buscamos las solicitudes
        laListaFolios = New ArrayList()
        lrsDatos = goTOMI_Database.Execute_Query("SELECT DISTINCT satm03cta FROM tomser.dbo.satm003cuentas WHERE satm03rep=1")
        If Not IsNothing(lrsDatos) Then
            Do While lrsDatos.Read
                laListaFolios.Add(lrsDatos.Item(0))
            Loop
            lrsDatos.Close()
            lrsDatos = Nothing
        End If

        ' Analizamos las solicitudes
        For Each lsFolio In laListaFolios
            write_Log("INFO|Actualiza_Cuentas|Procesando Cuenta: " & lsFolio)

            ' Inicio de variables
            liCliente = 0
            lsTipoMoneda = ""
            lsNombreCuenta = ""
            liCliente_Alt = 0
            lbNomina = False

            liResultado = goCMS_Database.Execute_Command("DELETE FROM cuenta WHERE id_cuenta=" & lsFolio)
            lrsDatos = goTOMI_Database.Execute_Query("SELECT satm02cod,satm03abr,satm03nom,satm03id,ISNULL(nomina,0) AS nomina FROM tomser.dbo.satm003cuentas WHERE satm03cta=" & lsFolio)
            If Not IsNothing(lrsDatos) Then
                Do While lrsDatos.Read
                    liCliente = lrsDatos.Item(0)
                    lsTipoMoneda = lrsDatos.Item(1)
                    lsNombreCuenta = lrsDatos.Item(2)
                    liCliente_Alt = lrsDatos.Item(3)
                    lbNomina = lrsDatos.Item(4)
                Loop
                lrsDatos.Close()
                lrsDatos = Nothing
            End If
            liResultado = goCMS_Database.Execute_Command("INSERT INTO cuenta (id_cuenta,id_cliente,tipomoneda,nombredelacuenta,id_cuenta_alt,nomina) VALUES (" & lsFolio & "," & liCliente & ",'" & lsTipoMoneda & "','" & lsNombreCuenta & "'," & liCliente_Alt & "," & IIf(lbNomina, 1, 0) & ")")
            liResultado = goTOMI_Database.Execute_Command("UPDATE tomser.dbo.satm003cuentas SET satm03rep=0 WHERE satm03cta=" & lsFolio)
            liResultado = goTOMI_Database.Execute_Command("UPDATE tb_hd_cuentas SET satm03rep=0 WHERE satm03cta=" & lsFolio)

            ' Log
            write_Log("INFO|Actualiza_Cuentas|Se termina procesamiento Cuenta: " & lsFolio)
        Next
        laListaFolios.Clear()
    Catch lexActualizaCuentas As Exception
        write_Log("ERROR|Actualiza_Cuentas|Se presenta exception: " & lexActualizaCuentas.ToString)
    End Try
End Sub
