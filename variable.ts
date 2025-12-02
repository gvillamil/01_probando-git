Module ComunFXD

    Const CASFIM_MUFG As String = "40108"
    Public Function GeneraFirmaDigital(ByRef datos As Dictionary(Of String, Object), ByVal tipoPago As Integer, ByVal tabla As String, ByVal folio As String) As Boolean
        Dim lrsDatos As DbDataReader
        Dim lsSQL As String = String.Empty

        Try
            ' Inicio variables
            write_Log("INFO|ComunFXD.GeneraFirmaDigital|Inicia proceso para generación de firma")
            '****** DATOS A FIRMAR ********

            'CASFIM_MUFG        - CASFIM Ordenante
            'laValoresFormato(0) - FechaOperación
            'laValoresFormato(2) - CASFIM Beneficiario
            'laValoresFormato(3) - Importe
            'laValoresFormato(4) - Clave de rastreo
            'laValoresFormato(5) - Cuenta Ordenante
            'laValoresFormato(7) - Cuenta Beneficiario

            'Se Obtiene la fecha de ejecución
            Dim fecOperacion As String = String.Empty
            lrsDatos = goTOMI_Database.Execute_Query("SELECT fec_valor FROM TB_SRV_SWF_PLAN_TRABAJO_MAESTRO WHERE folio = '" & folio & "'")
            If Not IsNothing(lrsDatos) Then
                While lrsDatos.Read
                    fecOperacion = lrsDatos("fec_valor")
                End While
                lrsDatos.Close()
                lrsDatos = Nothing
            End If
            
            Dim intermediaryIdentifier As String = busca_valor_xml(datos("des_variables_xml"), "int_cif")
            Dim speiReceiverIdentifier As String = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_swift_spei")
            Dim dataValue As OrdenPagoValue = New OrdenPagoValue()

            dataValue.InsClaveOrd = CASFIM_MUFG
            dataValue.OpFechaOper = fecOperacion
            dataValue.InsClaveBen = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_casfim_spei")
            dataValue.OpMonto = datos("des_importe")
            dataValue.OpCveRastreo = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
            
            If tipoPago = 3 Then 
                Dim rawAccount = busca_valor_xml(datos("des_variables_xml"), "btmum_cta_vostro_emisor")
                Dim orderingAccount = rawAccount.Replace("-", "") ' Se le quitan los guiones a la cuenta vostro.
                dataValue.OpCuentaOrd = orderingAccount
            End If
            
            If intermediaryIdentifier.Contains(speiReceiverIdentifier) Then
                write_Log("INFO|registra_SPEI|El banco intermediario es a quien se enviara el SPEI")
                dataValue.OpCuentaBen = busca_valor_xml(datos("des_variables_xml"), "aw_cif_account")
                dataValue.OpCuentaBen2 = busca_valor_xml(datos("des_variables_xml"), "ben_int_account")
            Else 
                write_Log("INFO|registra_SPEI|El creditorAgent es a quien se enviara el SPEI")
                dataValue.OpCuentaBen = busca_valor_xml(datos("des_variables_xml"), "ben_int_account")
                dataValue.OpCuentaBen2 = -1
            End If
            
            dataValue.TpPago = tipoPago
            Dim conversiones As OrdenPagoByteArray = ObtieneTrama(dataValue)

            'Generación de dato firmado
            write_Log("INFO|ComunFXD.GeneraFirmaDigital|Se realiza el llamado para la firma")
            Dim opFirmaDigital As String = GenerateSign(conversiones.TramaCompleta)

            lsSQL = "UPDATE " & tabla & " SET [OP_FIRMA_DIG]='" & opFirmaDigital & "' WHERE nf_folio = '" & folio & "'"
            goTOMI_Database.Execute_Command(lsSQL)
            write_Log("INFO|ComunFXD.GeneraFirmaDigital|Se actualizó el campo para la firma exitosamente")

            Return True

        Catch lexErrorSPEI_Registro As Exception
            write_Log("ERROR|ComunFXD.GeneraFirmaDigital|Se presenta exception: " & lexErrorSPEI_Registro.ToString)
        End Try
        Return False
    End Function

    Public Sub LogueoDatosTrama(datos As OrdenPagoByteArray)
        write_Log("INFO|ComunFXD.GeneraFirmaDigital|Se obtienen bytes de cada tipo de dato ")
        If Not IsNothing(datos.InsClaveBen) Then
            If datos.InsClaveBen.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|InsClaveBen " + MuestraTrama(datos.InsClaveBen))
            End If
        End If
        If Not IsNothing(datos.InsClaveOrd) Then
            If datos.InsClaveOrd.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|InsClaveOrd " + MuestraTrama(datos.InsClaveOrd))
            End If
        End If
        If Not IsNothing(datos.OpCuentaBen) Then
            If datos.OpCuentaBen.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|OpCuentaBen " + MuestraTrama(datos.OpCuentaBen))
            End If
        End If
        If Not IsNothing(datos.OpCuentaBen2) Then
            If datos.OpCuentaBen2.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|OpCuentaBen2 " + MuestraTrama(datos.OpCuentaBen2))
            End If
        End If
        If Not IsNothing(datos.OpCuentaOrd) Then
            If datos.OpCuentaOrd.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|OpCuentaOrd " + MuestraTrama(datos.OpCuentaOrd))
            End If
        End If
        If Not IsNothing(datos.OpCveRastreo) Then
            If datos.OpCveRastreo.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|OpCveRastreo " + MuestraTrama(datos.OpCveRastreo))
            End If
        End If
        If Not IsNothing(datos.OpFechaOper) Then
            If datos.OpFechaOper.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|OpFechaOper " + MuestraTrama(datos.OpFechaOper))
            End If
        End If
        If Not IsNothing(datos.OpMonto) Then
            If datos.OpMonto.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|OpMonto " + MuestraTrama(datos.OpMonto))
            End If
        End If


        If Not IsNothing(datos.TramaCompleta) Then
            If datos.TramaCompleta.Length > 0 Then
                write_Log("INFO|ComunFXD.GeneraFirmaDigital|La trama armada es " + MuestraTrama(datos.TramaCompleta))
            End If
        End If
    End Sub


    Private Function MuestraTramaDEC(array As Byte()) As String
        Dim result As String = String.Empty
        For c As Integer = 0 To array.Length - 1
            result += array(c).ToString + " "
        Next
        Return result
    End Function

    Private Function MuestraTrama(array As Byte()) As String
        Return BitConverter.ToString(array)
    End Function

    Private Function ObtieneTrama(dataValue As OrdenPagoValue) As OrdenPagoByteArray
        Dim conversiones As OrdenPagoByteArray = New OrdenPagoByteArray
        Dim tipoPago As Int16 = Convert.ToInt16(dataValue.TpPago)

        write_Log("INFO|ComunFXD.ObtieneTrama|InsClaveOrd = " & dataValue.InsClaveOrd)
        write_Log("INFO|ComunFXD.ObtieneTrama|OpFechaOper = " & dataValue.OpFechaOper)
        write_Log("INFO|ComunFXD.ObtieneTrama|InsClaveBen = " & dataValue.InsClaveBen)
        write_Log("INFO|ComunFXD.ObtieneTrama|OpMonto = " & dataValue.OpMonto)
        write_Log("INFO|ComunFXD.ObtieneTrama|OpCveRastreo = " & dataValue.OpCveRastreo)
        write_Log("INFO|ComunFXD.ObtieneTrama|OpCuentaOrd = " & dataValue.OpCuentaOrd)
        write_Log("INFO|ComunFXD.ObtieneTrama|OpCuentaBen = " & dataValue.OpCuentaBen)
        write_Log("INFO|ComunFXD.ObtieneTrama|OpCuentaBen2 = " & dataValue.OpCuentaBen2)
        write_Log("INFO|ComunFXD.ObtieneTrama|TpPago = " & dataValue.TpPago)
        Try
            Select Case tipoPago
                'Devoluciones
                Case 0
                    'FechaOperacion + CveInstOrd + CveInstBen + CveRastreo + Monto
                    write_Log("INFO|ComunFXD.ObtieneTrama|Se inicia proceso de conversión de bytes tipo Devolucion")

                    conversiones.OpFechaOper = dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveOrd = intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveBen = intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCveRastreo = stringToByteArray(dataValue.OpCveRastreo, ByteOrder.BIG_ENDIAN)
                    conversiones.OpMonto = moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ByteOrder.BIG_ENDIAN)

                    conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).Concat(conversiones.OpMonto).ToArray()

                    'Tercero Tercero
                Case 1
                    'FechaOperacion + CveInstOrd + CveInstBen + CveRastreo + Monto + CuentaOrd + CuentaBen
                    write_Log("INFO|ComunFXD.ObtieneTrama|Se inicia proceso de conversión de bytes tipo tercero tercero")

                    conversiones.OpFechaOper = dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveOrd = intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveBen = intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCveRastreo = stringToByteArray(dataValue.OpCveRastreo, ByteOrder.BIG_ENDIAN)
                    conversiones.OpMonto = moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCuentaOrd = stringToByteArray(dataValue.OpCuentaOrd, ByteOrder.BIG_ENDIAN)
                    conversiones.OpCuentaBen = stringToByteArray(dataValue.OpCuentaBen, ByteOrder.BIG_ENDIAN)

                    conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).Concat(conversiones.OpMonto).Concat(conversiones.OpCuentaOrd).Concat(conversiones.OpCuentaBen).ToArray()

                    'Participante a tercero
                Case 5
                    'FechaOperacion + CveInstOrd + CveInstBen + CveRastreo + Monto + CuentaBen
                    write_Log("INFO|ComunFXD.ObtieneTrama|Se inicia proceso de conversión de bytes tipo Participante a tercero")

                    conversiones.OpFechaOper = dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveOrd = intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveBen = intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCveRastreo = stringToByteArray(dataValue.OpCveRastreo, ByteOrder.BIG_ENDIAN)
                    conversiones.OpMonto = moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCuentaBen = stringToByteArray(dataValue.OpCuentaBen, ByteOrder.BIG_ENDIAN)

                    conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).Concat(conversiones.OpMonto).Concat(conversiones.OpCuentaBen).ToArray()

                    'Participante a Vostro
                Case 6
                    'FechaOperacion + CveInstOrd + CveInstBen + CveRastreo + Monto + CuentaBen + CuentaBen2
                    write_Log("INFO|ComunFXD.ObtieneTrama|Se inicia proceso de conversión de bytes tipo Participante a Vostro")

                    conversiones.OpFechaOper = dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveOrd = intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveBen = intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCveRastreo = stringToByteArray(dataValue.OpCveRastreo, ByteOrder.BIG_ENDIAN)
                    conversiones.OpMonto = moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCuentaBen = stringToByteArray(dataValue.OpCuentaBen, ByteOrder.BIG_ENDIAN)

                    Dim arrayZero As Byte() = New Byte() {}
                    addZeroByte(arrayZero)
                    conversiones.OpCuentaBen2 = arrayZero
                    conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).Concat(conversiones.OpMonto).Concat(conversiones.OpCuentaBen).Concat(conversiones.OpCuentaBen2).ToArray()

                    'Participante a Participante
                Case 7
                    'FechaOperacion + CveInstOrd + CveInstBen + CveRastreo + Monto
                    write_Log("INFO|ComunFXD.ObtieneTrama|Se inicia proceso de conversión de bytes tipo Participante a Participante")

                    conversiones.OpFechaOper = dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveOrd = intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveBen = intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCveRastreo = stringToByteArray(dataValue.OpCveRastreo, ByteOrder.BIG_ENDIAN)
                    conversiones.OpMonto = moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ByteOrder.BIG_ENDIAN)

                    conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).Concat(conversiones.OpMonto).ToArray()

                    'Tercero a Tercero Vostro
                Case 3
                    'FechaOperacion + CveInstOrd + CveInstBen + CveRastreo + Monto + CuentaOrd + CuentaBen + CuentaBen2
                    write_Log("INFO|ComunFXD.ObtieneTrama|Se inicia proceso de conversión de bytes tipo Tercero a Tercero Vostro")

                    conversiones.OpFechaOper = dateToByteArray(Convert.ToInt32(dataValue.OpFechaOper), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveOrd = intToByteArray(Convert.ToInt32(dataValue.InsClaveOrd), ByteOrder.BIG_ENDIAN)
                    conversiones.InsClaveBen = intToByteArray(Convert.ToInt32(dataValue.InsClaveBen), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCveRastreo = stringToByteArray(dataValue.OpCveRastreo, ByteOrder.BIG_ENDIAN)
                    conversiones.OpMonto = moneyToByteArray(Convert.ToDouble(dataValue.OpMonto), ByteOrder.BIG_ENDIAN)
                    conversiones.OpCuentaOrd = stringToByteArray(dataValue.OpCuentaOrd, ByteOrder.BIG_ENDIAN)
                    conversiones.OpCuentaBen = stringToByteArray(dataValue.OpCuentaBen, ByteOrder.BIG_ENDIAN)

                    If dataValue.OpCuentaBen2 = -1 Then 
                        Dim arrayZero = New Byte() {}
                        addZeroByte(arrayZero)
                        conversiones.OpCuentaBen2 = arrayZero
                    Else
                        conversiones.OpCuentaBen2 = stringToByteArray(dataValue.OpCuentaBen2, ByteOrder.BIG_ENDIAN)
                    End If
                    
                    conversiones.TramaCompleta = conversiones.OpFechaOper.Concat(conversiones.InsClaveOrd).Concat(conversiones.InsClaveBen).Concat(conversiones.OpCveRastreo).Concat(conversiones.OpMonto).Concat(conversiones.OpCuentaOrd).Concat(conversiones.OpCuentaBen).Concat(conversiones.OpCuentaBen2).ToArray()

                Case Else
                    Throw New Exception("No existe definición para pago SPEI del tipo de pago " & tipoPago)
            End Select
            LogueoDatosTrama(conversiones)
        Catch lexError As Exception
            Throw New Exception("ERROR|ComunFXD.ObtieneTrama|Se presenta exception: " & lexError.ToString)
        End Try
        Return conversiones
    End Function

    Public Function ObtieneCampoFirma(dataValue As OrdenPagoValue) As String
        Dim opFirmaDigital As String = String.Empty
        Try
            Dim conversiones As OrdenPagoByteArray = ObtieneTrama(dataValue)
            'Generación de dato firmado
            opFirmaDigital = GenerateSign(conversiones.TramaCompleta)
        Catch lexError As Exception
            Throw New Exception("ObtieneTrama|Se presenta exception: " & lexError.ToString)
        End Try
        Return opFirmaDigital
    End Function

End Module
