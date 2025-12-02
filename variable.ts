Public Class MT_103
    Private Const tipoOperacion As String = "MT-103"
    Private Shared goMW_TipoOperacion As T24_MW_Connection.MW_Tipo_operacion

    Shared Function procesaOperacion(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        Dim lsAliasProceso As String
        Dim lbProcesado As Boolean

        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Iniciando el registro de la operación, folio_unico='" & datos("folio_unico") & "'.")

        ' Tipo de Operacion
        goMW_TipoOperacion = goT24_Connection.MW_DatosOperacion("SWF_IN")

        ' Proceso a ejecutar
        lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)

        ' E/S
        If lsAliasProceso = "TOMI_ENTRADAS/SALIDAS" Then
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A realizar el registro de E/S.")

            ' Inicio el proceso
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)

            ' Entrada salida
            Dim loDatos_E_S As New Dictionary(Of String, Object)
            Dim lbRegistro As Boolean

            ' Datos de referencia
            loDatos_E_S.Add("icn", icn)
            ' Datos
            loDatos_E_S.Add("referencia", datos("folio_unico"))
            loDatos_E_S.Add("num_cliente", datos("num_cliente"))
            loDatos_E_S.Add("ordenante", datos("nombre_cliente"))
            loDatos_E_S.Add("operacion", 218)
            loDatos_E_S.Add("operacion_desc", "Entrada (TTS)")
            loDatos_E_S.Add("tipo", "I")
            loDatos_E_S.Add("tipo_desc", "Internacional")
            loDatos_E_S.Add("cuenta", datos("num_cuenta"))
            loDatos_E_S.Add("nombre", datos("nombre_cliente"))
            loDatos_E_S.Add("moneda", goT24_Connection.valida_moneda(datos("cve_moneda")))
            loDatos_E_S.Add("via", 810) ' Subaplicacion
            loDatos_E_S.Add("via_desc", "TRF RECIBIDA")
            loDatos_E_S.Add("importe", datos("des_importe"))
            loDatos_E_S.Add("saldo", 0)
            loDatos_E_S.Add("comentarios", "")
            loDatos_E_S.Add("fec_captura", Format(Now, "yyyy-MM-dd"))
            loDatos_E_S.Add("fec_valor", gsFechaSistema.Substring(0, 4) & "-" & gsFechaSistema.Substring(4, 2) & "-" & gsFechaSistema.Substring(6, 2))
            loDatos_E_S.Add("hora", Format(Now, "hh:mm:ss AMPM"))
            loDatos_E_S.Add("capturista", "JEAI")
            loDatos_E_S.Add("fad", 0)
            loDatos_E_S.Add("ref_control", "")
            loDatos_E_S.Add("ref_control_cont", 0)
            lbRegistro = Registra_E_S(loDatos_E_S)
            If Not lbRegistro Then
                Throw New Exception("No se pudo registrar la operación como entrada/salida")
            End If

            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Se realizó el registro de E/S exitosamente.")

            ' Actualización de la operación
            terminaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)

            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Se termina el registro E/S e inicia siguiente tarea.")

            ' Siguiente proceso
            lsAliasProceso = dameUltimoProceso_SWF(goTOMI_Database, icn, gsTipoOperacion)
        End If

        '' Registro de la operación en T24
        If lsAliasProceso = "T24_REGISTRO" Then
            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A registrar operación en T24.")

            ' Inicio el proceso
            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)

            ' Operacion a T24
            lbProcesado = T24_REGISTRO(icn, datos)
            If lbProcesado = False And Not IsNothing(gexUltimaExcepcion) Then
                Throw gexUltimaExcepcion
            End If

            If Not lbProcesado Then
                Throw New Exception("Error en procesamiento de registro MT-103 T24_REGISTRO con folio: " & icn)
            End If
        End If

        ' Conciliación
        If lsAliasProceso = "CONCILIAR" Then
            Dim lsFolioT24 As String
            Dim lbEs_Igual_en_T24 As Boolean
            Dim lsMoneda As String, lsCuentaDeb As String, lsCuentaCred As String
            Dim lrsDatos As DbDataReader

            write_Log("INFO|" & tipoOperacion & ".procesaOperacion|A conciliar operaciones (por estatus) entre TOMI y T24.")

            iniciaProceso_SWF(goTOMI_Database, icn, gsTipoOperacion, lsAliasProceso)

            ' Busco la operacion en T24
            lsFolioT24 = goT24_Connection.obtener_folioAsignado(datos("folio_unico"))
            If IsNothing(lsFolioT24) Then
                Throw New Exception("No existe folio asignado para el folio_unico='" & datos("folio_unico") & "'")
            End If

            ' Valida contra los datos de TOMI - T24
            lbEs_Igual_en_T24 = False
            lsMoneda = goT24_Connection.valida_moneda(datos("cve_moneda"))
            If lsMoneda = goT24_Connection.MonedaDefaultMN Then
                lsCuentaDeb = Right(datos("cta_vostro_emisor"), 7)
                lsCuentaCred = datos("num_cuenta")
                If lsCuentaCred.Length = 7 Then
                    lsCuentaCred = lsCuentaCred
                ElseIf lsCuentaCred = 18 Then
                    lsCuentaCred = lsCuentaCred.Substring(11, 7)
                Else
                    Throw New Exception("La cuenta a acreditar no es válida")
                End If
            Else
                lsCuentaDeb = busca_valor_xml(datos("des_variables_xml"), "debit_account")
                lsCuentaCred = datos("num_cuenta")
            End If
            lrsDatos = goVistasT24_Database.Execute_Query("SELECT CUR_ID,DEBIT_ACCT_NO,DEBIT_CURRENCY,DEBIT_AMOUNT,CREDIT_ACCT_NO,SATEL_SYS_REF FROM VW_T24_FUNDS_TRANSFER_ID WHERE CUR_ID = '" & lsFolioT24 & "'")
            If Not IsNothing(lrsDatos) Then
                While lrsDatos.Read
                    If (lsFolioT24 = lrsDatos.Item("CUR_ID") And
                            datos("folio_unico") = lrsDatos.Item("SATEL_SYS_REF") And
                            datos("des_importe") = lrsDatos.Item("DEBIT_AMOUNT") And
                            lsMoneda = lrsDatos.Item("DEBIT_CURRENCY") And
                            lsCuentaDeb = lrsDatos.Item("DEBIT_ACCT_NO") And
                            lsCuentaCred = lrsDatos.Item("CREDIT_ACCT_NO")) Then
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

        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Fin del registro de la operación.")

        ' Termina el proceso del folio
        Return True
    End Function

    ' Inserta registro en T24
    Private Shared Function T24_REGISTRO(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        Try
            Dim loTrxMaster As T24_MW_TrxMaster = Nothing
            Dim loTrxDetailFT As T24_MW_TrxDetail = Nothing
            Dim lsT24_Internal_Type As String
            'Dim lsDireccion As String, lsCuenta As String
            Dim lsMoneda As String, lsDescripcion As String, lsDetailCharges As String, lsOrderingBank As String
            Dim lsCIF_Inst_Ordenante As String, lsSwiftInstOrdenante As String, lsNombreInstOrdenante As String, lsDireccionInstOrdenante As String
            Dim lsBeneficiario As String, lsCuentaDeb As String, lsCuentaCred As String
            Dim lsOrderingCust As String = String.Empty
            Dim lsSW_END2END_REF As String, lsSLA_UETR As String

            write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Inicia registro en T24.")

            ' Asignacion de Valores
            lsMoneda = goT24_Connection.valida_moneda(datos("cve_moneda"))
            lsSwiftInstOrdenante = ""
            lsNombreInstOrdenante = ""
            lsDireccionInstOrdenante = ""
            ' Datos de la institución ordenante
            lsNombreInstOrdenante = busca_valor_xml(datos("des_variables_xml"),"order_inst_name_addr_1")
            If lsNombreInstOrdenante <> "" Then
                lsDireccionInstOrdenante = busca_valor_xml(datos("des_variables_xml"), "order_inst_name_addr_2") & " " & busca_valor_xml(datos("des_variables_xml"), "order_inst_name_addr_3")
                lsSwiftInstOrdenante = busca_valor_xml(datos("des_variables_xml"), "order_inst_party")
                ' Corto los campos
                lsNombreInstOrdenante = quita_linea(lsNombreInstOrdenante, 50)
                lsDireccionInstOrdenante = quita_linea(lsDireccionInstOrdenante, 80)
                write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Se encontró la información del ordenante por institución ordenante.")
            Else
                ' No hay banco ordenante, busco el cif ordenante
                lsCIF_Inst_Ordenante = busca_valor_xml(datos("des_variables_xml"), "order_inst_cif")
                If lsCIF_Inst_Ordenante <> "" Then
                    ' Busca los valores
                    busca_cif_datos(lsCIF_Inst_Ordenante, lsSwiftInstOrdenante, lsNombreInstOrdenante, lsDireccionInstOrdenante)
                    lsNombreInstOrdenante = quita_linea(lsNombreInstOrdenante, 50)
                    lsDireccionInstOrdenante = quita_linea(lsDireccionInstOrdenante, 80)
                    write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Encontró: Swift='" & lsSwiftInstOrdenante & "', Nombre='" & lsNombreInstOrdenante & "', Calle='" & lsDireccionInstOrdenante & "'.")
                Else
                    write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|No se encontró el cif de la institución ordenante ewn el catálogo.")
                End If
            End If
            'Armado de la descricpión
            lsDescripcion = "RECEPCION TRANSFERENCIA INTERNACIONAL MT-103."
            If lsNombreInstOrdenante <> "" Then
                lsDescripcion &= ", BANCO ORDENANTE: " & lsNombreInstOrdenante
            Else
                If busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor") <> "" Then
                    lsDescripcion &= ", BANCO ORIGEN: " & busca_valor_xml(datos("des_variables_xml"), "btmum_nombre_emisor")
                End If
            End If
            If busca_valor_xml(datos("des_variables_xml"), "ordering_name_addr_1") <> "" Then
                lsDescripcion &= ", ORDENANTE: " & busca_valor_xml(datos("des_variables_xml"), "ordering_name_addr_1")
            End If
            If busca_valor_xml(datos("des_variables_xml"), "detail_charges") <> "" Then
                lsDescripcion &= ", DET. CHARGES: " & busca_valor_xml(datos("des_variables_xml"), "detail_charges")
            End If
            If busca_valor_xml(datos("des_variables_xml"), "remmitance_information") <> "" Then
                lsDescripcion &= ", POR CONCEPTO DE: " & busca_valor_xml(datos("des_variables_xml"), "remmitance_information")
            End If

            ' Es OUR?
            lsDetailCharges = busca_valor_xml(datos("des_variables_xml"), "detail_charges")
            If lsDetailCharges = "OUR" Then
                ' Sin comisión
                write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Es registro con DetailCharges='OUR' (sin comisiones).")
                lsT24_Internal_Type = "MT103"
                lsOrderingBank = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_swift_emisor")
            Else
                ' Comisiones
                write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Inicia busqueda de comisiones.")
                lsT24_Internal_Type = "MT103_COM"
                lsOrderingBank = busca_valor_xml(datos("des_variables_xml"), "btmum_cve_swift_emisor")
            End If

            ' Prepatamos los datos para el registro del FT
            loTrxMaster = New T24_MW_TrxMaster
            With loTrxMaster
                .TrxOperation_Specific_Type = "SWF_IN"
                .TrxOperation_SubType = lsT24_Internal_Type
                .TrxOperation_Folio = icn
                .TrxOperation_Acct = datos("num_cuenta")
            End With
            loTrxDetailFT = New T24_MW_TrxDetail
            With loTrxDetailFT
                If lsMoneda = goT24_Connection.MonedaDefaultMN Then
                    lsCuentaDeb = Right(datos("cta_vostro_emisor"), 7)
                    lsCuentaCred = datos("num_cuenta")
                    If lsCuentaCred.Length = 7 Then
                        lsCuentaCred = lsCuentaCred
                    ElseIf lsCuentaCred = 18 Then
                        lsCuentaCred = lsCuentaCred.Substring(11, 7)
                    Else
                        Throw New Exception("La cuenta a acreditar no es válida")
                    End If
                Else
                    lsCuentaDeb = busca_valor_xml(datos("des_variables_xml"), "debit_account")
                    lsCuentaCred = datos("num_cuenta")
                End If
                .debit_acct_no = lsCuentaDeb
                .debit_currency = lsMoneda
                .debit_amount = String.Format("{0:0.##}", datos("des_importe"))
                .debit_their_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .credit_their_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .credit_acct_no = lsCuentaCred
                .credit_currency = lsMoneda
                .ben_our_charges = lsDetailCharges
                .ordering_bank = lsOrderingBank
                ' Beneficiario
                lsBeneficiario = busca_valor_xml(datos("des_variables_xml"), "beneficiary_name_addr")
                If lsBeneficiario = "" Then
                    lsBeneficiario = busca_valor_xml(datos("des_variables_xml"), "beneficiary_name_addr_1")
                End If
                .Beneficiary_Name = lsBeneficiario
                
                ' Información del ordenante
                lsOrderingCust = "AC" & IIf(LTrim(RTrim(busca_valor_xml(datos("des_variables_xml"), "ordering_account"))) = "", "", LTrim(RTrim(busca_valor_xml(datos("des_variables_xml"), "ordering_account")))) & _
                                IIf(LTrim(RTrim(busca_valor_xml(datos("des_variables_xml"), "ordering_name_addr_1"))) = "", "", goT24_Connection.MW_SeparadorMultivalor & LTrim(RTrim(busca_valor_xml(datos("des_variables_xml"), "ordering_name_addr_1")))) & _
                                IIf(LTrim(RTrim(busca_valor_xml(datos("des_variables_xml"), "ordering_name_addr_2"))) = "", "", goT24_Connection.MW_SeparadorMultivalor & LTrim(RTrim(busca_valor_xml(datos("des_variables_xml"), "ordering_name_addr_2")))) & _
                                IIf(LTrim(RTrim(busca_valor_xml(datos("des_variables_xml"), "ordering_name_addr_3"))) = "", "", goT24_Connection.MW_SeparadorMultivalor & LTrim(RTrim(busca_valor_xml(datos("des_variables_xml"), "ordering_name_addr_3"))))

                .ordering_cust = IIf(lsOrderingCust.Substring(lsOrderingCust.Length - 1, 1) = goT24_Connection.MW_SeparadorMultivalor, Mid(lsOrderingCust, 1, lsOrderingCust.Length - 1), lsOrderingCust)

                ' Datos de la institución ordenante
                lsCIF_Inst_Ordenante = busca_valor_xml(datos("des_variables_xml"), "order_inst_cif")
                write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Buscando datos de la institución ordenante en el catálogo, cif='" & lsCIF_Inst_Ordenante & "'.")
                If lsNombreInstOrdenante <> "" Then
                    write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|Encontró: Swift='" & lsSwiftInstOrdenante & "', Nombre='" & lsNombreInstOrdenante & "', Calle='" & lsDireccionInstOrdenante & "'.")
                    .Cust_Ref1 = lsNombreInstOrdenante
                    .Cust_Ref2 = lsSwiftInstOrdenante
                    .Cust_Ref3 = lsCIF_Inst_Ordenante
                Else
                    write_Log("INFO|" & tipoOperacion & ".T24_REGISTRO|No se encontró el cif de la institución ordenante ewn el catálogo.")
                End If
                
                .related_ref = busca_valor_xml(datos("des_variables_xml"), "sender_reference")
                .Instructions = lsDescripcion
                .Satel_Sys_Ref = datos("folio_unico")
                .folio_tomi = datos("folio_unico")
                lsSW_END2END_REF = busca_valor_xml(datos("des_variables_xml"), "UETR").Trim
                If lsSW_END2END_REF <> "" Then
                    lsSLA_UETR = busca_valor_xml(datos("des_variables_xml"), "SLA_UETR").Trim
                    .SW_END2END_REF = lsSW_END2END_REF
                End If
            End With
            Return goT24_Connection.registra_TRX(datos("folio_unico"), gsFechaSistema_T24_Juliana, goMW_TipoOperacion.tabla_trabajo, "SWF_IN", icn, goMW_TipoOperacion.T24_Operation_ID, loTrxMaster, loTrxDetailFT)
        Catch lexError As Exception
            write_Log("ERROR|" & tipoOperacion & ".T24_REGISTRO|Se presenta exception: " & lexError.ToString)
            gexUltimaExcepcion = lexError
        End Try
        Return False
    End Function
End Class
