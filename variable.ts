Private Sub Inserta_BeneficiariosCMS()
    Dim lrsDatos As DbDataReader
    Dim laListaFolios As ArrayList
    Dim lsFolio As String
    Dim laValores_Formato(57) As Object
    Dim strSQL As String = String.Empty

    Try
        ' Buscamos las solicitudes
        strSQL = "SELECT id_solicitud_cms FROM tbl_solicitudes_beneficiarios WHERE status = 'AUTORIZADO'"

        laListaFolios = New ArrayList()
        lrsDatos = goCMS_Database.Execute_Query(strSQL)
        If Not IsNothing(lrsDatos) Then
            Do While lrsDatos.Read
                laListaFolios.Add(lrsDatos.Item(0))
            Loop
            lrsDatos.Close()
            lrsDatos = Nothing
        End If

        ' Analizamos las solicitudes
        For Each lsFolio In laListaFolios
            strSQL = "SELECT ID_SOLICITUD_CMS,ID_SOLICITUD_DOCUMENTO,TIPO,id_Formato,Formato,Id_Cliente,Ordenate,Alias,Nombre,Apellido_P,Apellido_M,TipoBeneficiario," & _
                      "ClaveBanco,Banco,TipoCuenta,NoCuenta,Firma,MontoOperacion,Moneda,UsuariosCaptura,UsuariosAutoriza,Indicador_Plantilla,Country,ABA," & _
                      "BancoBeneficiario,SwiftBeneficiario,CuentaBeneficiario,SwiftCorresponsal,BancoCorresponsal,FECHA_SOL,USUARIO_CAPT,FECHA_AUT,USUARIO_AUT,FECHA_CANC," & _
                      "USUARIO_CANC,STATUS,OPERACION,COMMENT,FECHA_CONF,USUARIO_CONF,NOTA_CANC,emailBeneficiario,emailOrdenante,flgMail,residencia,rfc,origen,country_ben,Id_Agreement," & _
                      "city_res_ben,city_add_ben,city_num_ben,city_col_ben,city_zip_ben " & _
                      "FROM tbl_solicitudes_beneficiarios WHERE id_solicitud_cms = " & lsFolio

            write_Log("INFO|Inserta_BeneficiariosCMS|Insertando a TOMI la solicitud: " & lsFolio)
            lrsDatos = goCMS_Database.Execute_Query(strSQL.ToString)
            If Not IsNothing(lrsDatos) Then
                Do While lrsDatos.Read
                    laValores_Formato(0) = lrsDatos.Item("id_solicitud_cms")
                    laValores_Formato(1) = lrsDatos.Item("id_solicitud_documento")
                    laValores_Formato(2) = lrsDatos.Item("tipo")
                    laValores_Formato(3) = lrsDatos.Item("id_formato")
                    laValores_Formato(4) = lrsDatos.Item("formato")
                    laValores_Formato(5) = lrsDatos.Item("id_cliente")
                    laValores_Formato(6) = lrsDatos.Item("ordenate")
                    laValores_Formato(7) = lrsDatos.Item("alias")
                    laValores_Formato(8) = lrsDatos.Item("nombre")
                    laValores_Formato(9) = lrsDatos.Item("apellido_p")
                    laValores_Formato(10) = lrsDatos.Item("apellido_m")
                    laValores_Formato(11) = lrsDatos.Item("tipobeneficiario")
                    laValores_Formato(12) = lrsDatos.Item("clavebanco")
                    laValores_Formato(13) = lrsDatos.Item("banco")
                    laValores_Formato(14) = lrsDatos.Item("tipocuenta")
                    laValores_Formato(15) = lrsDatos.Item("nocuenta")
                    laValores_Formato(16) = lrsDatos.Item("firma")
                    laValores_Formato(17) = lrsDatos.Item("montooperacion")
                    laValores_Formato(18) = lrsDatos.Item("moneda")
                    laValores_Formato(19) = lrsDatos.Item("usuarioscaptura")
                    laValores_Formato(20) = lrsDatos.Item("usuariosautoriza")
                    laValores_Formato(21) = lrsDatos.Item("indicador_plantilla")
                    laValores_Formato(22) = lrsDatos.Item("country")
                    laValores_Formato(23) = lrsDatos.Item("aba")
                    laValores_Formato(24) = lrsDatos.Item("bancobeneficiario")
                    laValores_Formato(25) = lrsDatos.Item("swiftbeneficiario")
                    laValores_Formato(26) = lrsDatos.Item("cuentabeneficiario")
                    laValores_Formato(27) = lrsDatos.Item("swiftcorresponsal")
                    laValores_Formato(28) = lrsDatos.Item("bancocorresponsal")
                    laValores_Formato(29) = lrsDatos.Item("fecha_sol")
                    laValores_Formato(30) = lrsDatos.Item("usuario_capt")
                    laValores_Formato(31) = lrsDatos.Item("fecha_aut")
                    laValores_Formato(32) = lrsDatos.Item("usuario_aut")
                    laValores_Formato(33) = lrsDatos.Item("fecha_canc")
                    laValores_Formato(34) = lrsDatos.Item("usuario_canc")

                    If lrsDatos.Item("formato") = "SPD" And lrsDatos.Item("operacion") <> "BAJA" Then
                        laValores_Formato(35) = "MESA_CTRL"
                    Else
                        laValores_Formato(35) = "CONFIRMADO"
                    End If

                    laValores_Formato(36) = lrsDatos.Item("operacion")
                    laValores_Formato(37) = lrsDatos.Item("comment")
                    laValores_Formato(38) = lrsDatos.Item("fecha_conf")
                    laValores_Formato(39) = lrsDatos.Item("usuario_conf")
                    laValores_Formato(40) = lrsDatos.Item("nota_canc")
                    laValores_Formato(41) = lrsDatos.Item("emailbeneficiario")
                    laValores_Formato(42) = lrsDatos.Item("emailordenante")
                    laValores_Formato(43) = lrsDatos.Item("flgmail")
                    laValores_Formato(44) = lrsDatos.Item("residencia")
                    laValores_Formato(45) = lrsDatos.Item("rfc")
                    laValores_Formato(46) = lrsDatos.Item("origen")
                    laValores_Formato(47) = lrsDatos.Item("country_ben")
                    laValores_Formato(48) = lrsDatos.Item("id_agreement")
                    ' city project
                    laValores_Formato(49) = lrsDatos.Item("city_res_ben")
                    laValores_Formato(50) = lrsDatos.Item("city_add_ben")
                    laValores_Formato(51) = lrsDatos.Item("city_num_ben")
                    laValores_Formato(52) = lrsDatos.Item("city_col_ben")
                    laValores_Formato(53) = lrsDatos.Item("city_zip_ben")
                Loop
                lrsDatos.Close()
                lrsDatos = Nothing
            End If

            write_Log("INFO|Inserta_BeneficiariosCMS|Datos leídos de la solicitud: " & lsFolio)

            If BeneficiariosCMS_A_TOMI(laValores_Formato) Then
                If laValores_Formato(35) = "MESA_CTRL" Then
                    GeneraAlertasBeneficiarios(laValores_Formato)
                End If

                goCMS_Database.Execute_Command("UPDATE tbl_solicitudes_beneficiarios SET status='ENBT' WHERE id_solicitud_cms=" & lsFolio)
                goCMS_Database.Execute_Command("UPDATE tbl_requests_pt_ps SET status='ENBT' WHERE Id_Request_CMS_Detail=" & lsFolio)
                write_Log("INFO|Inserta_BeneficiariosCMS|Se termina el registro en TOMI de la solicitud: " & lsFolio)
            Else
                write_Log("ERROR|Inserta_BeneficiariosCMS|Error en el registro en TOMI de la solicitud: " & lsFolio)
            End If
        Next
        laListaFolios.Clear()
    Catch lexBeneficiarios As Exception
        write_Log("ERROR|Inserta_BeneficiariosCMS|Ocurre exception: " & lexBeneficiarios.ToString)
    End Try
End Sub

Private Function BeneficiariosCMS_A_TOMI(ByRef valoresFormato() As Object) As Boolean
    Try
        '=================================================================================================================        
        'Definición de Procedimiento Almacenado
        '=================================================================================================================        
        Dim loCmdTOMI As DbCommand = goTOMI_Database.newCommand("usp_TOMBIN_INSERT_SOLICITUDES_BENEFICIARIOS")
        loCmdTOMI.CommandType = CommandType.StoredProcedure

        '=================================================================================================================
        'Definición de parámetros
        '=================================================================================================================        
        loCmdTOMI.Parameters.Clear()

        ' Parametro 1
        Dim lpSolicitudDocumento As DbParameter = loCmdTOMI.CreateParameter()
        lpSolicitudDocumento.ParameterName = "@ID_SOLICITUD_DOCUMENTO"
        lpSolicitudDocumento.DbType = DbType.Int64
        lpSolicitudDocumento.Direction = ParameterDirection.Input
        lpSolicitudDocumento.Value = valoresFormato(0)
        loCmdTOMI.Parameters.Add(lpSolicitudDocumento)

        ' Parametro 2
        Dim lpTipo As DbParameter = loCmdTOMI.CreateParameter()
        lpTipo.ParameterName = "@TIPO"
        lpTipo.DbType = DbType.Int32
        lpTipo.Direction = ParameterDirection.Input
        lpTipo.Value = valoresFormato(2)
        loCmdTOMI.Parameters.Add(lpTipo)

        ' Parametro 3
        Dim lpId_Formato As DbParameter = loCmdTOMI.CreateParameter()
        lpId_Formato.ParameterName = "@ID_FORMATO"
        lpId_Formato.DbType = DbType.Int64
        lpId_Formato.Direction = ParameterDirection.Input
        lpId_Formato.Value = valoresFormato(3)
        loCmdTOMI.Parameters.Add(lpId_Formato)

        ' Parametro 4
        Dim lpFormato As DbParameter = loCmdTOMI.CreateParameter()
        lpFormato.ParameterName = "@FORMATO"
        lpFormato.DbType = DbType.String
        lpFormato.Size = 3
        lpFormato.Direction = ParameterDirection.Input
        lpFormato.Value = valoresFormato(4)
        loCmdTOMI.Parameters.Add(lpFormato)

        ' Parametro 5
        Dim lpCliente As DbParameter = loCmdTOMI.CreateParameter()
        lpCliente.ParameterName = "@ID_CLIENTE"
        lpCliente.DbType = DbType.Int64
        lpCliente.Direction = ParameterDirection.Input
        lpCliente.Value = valoresFormato(5)
        loCmdTOMI.Parameters.Add(lpCliente)

        ' Parametro 6
        Dim lpOrdenante As DbParameter = loCmdTOMI.CreateParameter()
        lpOrdenante.ParameterName = "@ORDENATE"
        lpOrdenante.DbType = DbType.String
        lpOrdenante.Size = 250
        lpOrdenante.Direction = ParameterDirection.Input
        lpOrdenante.Value = valoresFormato(6)
        loCmdTOMI.Parameters.Add(lpOrdenante)

        ' Parametro 7
        Dim lpAlias As DbParameter = loCmdTOMI.CreateParameter()
        lpAlias.ParameterName = "@ALIAS"
        lpAlias.DbType = DbType.String
        lpAlias.Size = 100
        lpAlias.Direction = ParameterDirection.Input
        lpAlias.Value = valoresFormato(7)
        loCmdTOMI.Parameters.Add(lpAlias)

        ' Parametro 8
        Dim lpNombre As DbParameter = loCmdTOMI.CreateParameter()
        lpNombre.ParameterName = "@NOMBRE"
        lpNombre.DbType = DbType.String
        lpNombre.Size = 100
        lpNombre.Direction = ParameterDirection.Input
        lpNombre.Value = valoresFormato(8)
        loCmdTOMI.Parameters.Add(lpNombre)

        ' Parametro 9
        Dim lpClaveBanco As DbParameter = loCmdTOMI.CreateParameter()
        lpClaveBanco.ParameterName = "@CLAVEBANCO"
        lpClaveBanco.DbType = DbType.String
        lpClaveBanco.Size = 5
        lpClaveBanco.Direction = ParameterDirection.Input
        lpClaveBanco.Value = valoresFormato(12)
        loCmdTOMI.Parameters.Add(lpClaveBanco)

        ' Parametro 10
        Dim lpBanco As DbParameter = loCmdTOMI.CreateParameter()
        lpBanco.ParameterName = "@BANCO"
        lpBanco.DbType = DbType.String
        lpBanco.Size = 100
        lpBanco.Direction = ParameterDirection.Input
        lpBanco.Value = valoresFormato(13)
        loCmdTOMI.Parameters.Add(lpBanco)

        ' Parametro 11
        Dim lpTipoCuenta As DbParameter = loCmdTOMI.CreateParameter()
        lpTipoCuenta.ParameterName = "@TIPOCUENTA"
        lpTipoCuenta.DbType = DbType.String
        lpTipoCuenta.Size = 100
        lpTipoCuenta.Direction = ParameterDirection.Input
        lpTipoCuenta.Value = valoresFormato(14)
        loCmdTOMI.Parameters.Add(lpTipoCuenta)

        ' Parametro 12
        Dim lpNoCuenta As DbParameter = loCmdTOMI.CreateParameter()
        lpNoCuenta.ParameterName = "@NOCUENTA"
        lpNoCuenta.DbType = DbType.String
        lpNoCuenta.Size = 45
        lpNoCuenta.Direction = ParameterDirection.Input
        lpNoCuenta.Value = valoresFormato(15)
        loCmdTOMI.Parameters.Add(lpNoCuenta)

        ' Parametro 13
        Dim lpFirma As DbParameter = loCmdTOMI.CreateParameter()
        lpFirma.ParameterName = "@FIRMA"
        lpFirma.DbType = DbType.Int32
        lpFirma.Direction = ParameterDirection.Input
        lpFirma.Value = valoresFormato(16)
        loCmdTOMI.Parameters.Add(lpFirma)

        ' Parametro 14
        Dim lpMontoOperacion As DbParameter = loCmdTOMI.CreateParameter()
        lpMontoOperacion.ParameterName = "@MONTOOPERACION"
        lpMontoOperacion.DbType = DbType.Decimal
        lpMontoOperacion.Direction = ParameterDirection.Input
        lpMontoOperacion.Value = valoresFormato(17)
        loCmdTOMI.Parameters.Add(lpMontoOperacion)

        ' Parametro 15
        Dim lpMoneda As DbParameter = loCmdTOMI.CreateParameter()
        lpMoneda.ParameterName = "@MONEDA"
        lpMoneda.DbType = DbType.String
        lpMoneda.Size = 3
        lpMoneda.Direction = ParameterDirection.Input
        lpMoneda.Value = valoresFormato(18)
        loCmdTOMI.Parameters.Add(lpMoneda)

        ' Parametro 16
        Dim lpUsuariosCaptura As DbParameter = loCmdTOMI.CreateParameter()
        lpUsuariosCaptura.ParameterName = "@USUARIOSCAPTURA"
        lpUsuariosCaptura.DbType = DbType.String
        lpUsuariosCaptura.Size = 250
        lpUsuariosCaptura.Direction = ParameterDirection.Input
        lpUsuariosCaptura.Value = valoresFormato(19)
        loCmdTOMI.Parameters.Add(lpUsuariosCaptura)

        ' Parametro 17
        Dim lpUsuariosAutoriza As DbParameter = loCmdTOMI.CreateParameter()
        lpUsuariosAutoriza.ParameterName = "@USUARIOSAUTORIZA"
        lpUsuariosAutoriza.DbType = DbType.String
        lpUsuariosAutoriza.Size = 250
        lpUsuariosAutoriza.Direction = ParameterDirection.Input
        lpUsuariosAutoriza.Value = valoresFormato(20)
        loCmdTOMI.Parameters.Add(lpUsuariosAutoriza)

        ' Parametro 18
        Dim lpIndicadorPlantilla As DbParameter = loCmdTOMI.CreateParameter()
        lpIndicadorPlantilla.ParameterName = "@INDICADOR_PLANTILLA"
        lpIndicadorPlantilla.DbType = DbType.Int32
        lpIndicadorPlantilla.Direction = ParameterDirection.Input
        lpIndicadorPlantilla.Value = valoresFormato(21)
        loCmdTOMI.Parameters.Add(lpIndicadorPlantilla)

        ' Parametro 19
        Dim lpCountry As DbParameter = loCmdTOMI.CreateParameter()
        lpCountry.ParameterName = "@COUNTRY"
        lpCountry.DbType = DbType.String
        lpCountry.Size = 6
        lpCountry.Direction = ParameterDirection.Input
        lpCountry.Value = valoresFormato(22)
        loCmdTOMI.Parameters.Add(lpCountry)

        ' Parametro 20
        Dim lpABA As DbParameter = loCmdTOMI.CreateParameter()
        lpABA.ParameterName = "@ABA"
        lpABA.DbType = DbType.String
        lpABA.Size = 9
        lpABA.Direction = ParameterDirection.Input
        lpABA.Value = valoresFormato(23)
        loCmdTOMI.Parameters.Add(lpABA)

        ' Parametro 21
        Dim lpBancoBeneficiario As DbParameter = loCmdTOMI.CreateParameter()
        lpBancoBeneficiario.ParameterName = "@BANCOBENEFICIARIO"
        lpBancoBeneficiario.DbType = DbType.String
        lpBancoBeneficiario.Size = 100
        lpBancoBeneficiario.Direction = ParameterDirection.Input
        lpBancoBeneficiario.Value = valoresFormato(24)
        loCmdTOMI.Parameters.Add(lpBancoBeneficiario)

        ' Parametro 22
        Dim lpSwiftBeneficiario As DbParameter = loCmdTOMI.CreateParameter()
        lpSwiftBeneficiario.ParameterName = "@SWIFTBENEFICIARIO"
        lpSwiftBeneficiario.DbType = DbType.String
        lpSwiftBeneficiario.Size = 11
        lpSwiftBeneficiario.Direction = ParameterDirection.Input
        lpSwiftBeneficiario.Value = valoresFormato(25)
        loCmdTOMI.Parameters.Add(lpSwiftBeneficiario)

        ' Parametro 23
        Dim lpCuentaBeneficiario As DbParameter = loCmdTOMI.CreateParameter()
        lpCuentaBeneficiario.ParameterName = "@CUENTABENEFICIARIO"
        lpCuentaBeneficiario.DbType = DbType.String
        lpCuentaBeneficiario.Size = 25
        lpCuentaBeneficiario.Direction = ParameterDirection.Input
        lpCuentaBeneficiario.Value = valoresFormato(26)
        loCmdTOMI.Parameters.Add(lpCuentaBeneficiario)

        ' Parametro 24
        Dim lpSwiftCorresponsal As DbParameter = loCmdTOMI.CreateParameter()
        lpSwiftCorresponsal.ParameterName = "@SWIFTCORRESPONSAL"
        lpSwiftCorresponsal.DbType = DbType.String
        lpSwiftCorresponsal.Size = 11
        lpSwiftCorresponsal.Direction = ParameterDirection.Input
        lpSwiftCorresponsal.Value = valoresFormato(27)
        loCmdTOMI.Parameters.Add(lpSwiftCorresponsal)

        ' Parametro 25
        Dim lpBancoCorresponsal As DbParameter = loCmdTOMI.CreateParameter()
        lpBancoCorresponsal.ParameterName = "@BANCOCORRESPONSAL"
        lpBancoCorresponsal.DbType = DbType.String
        lpBancoCorresponsal.Size = 100
        lpBancoCorresponsal.Direction = ParameterDirection.Input
        lpBancoCorresponsal.Value = valoresFormato(28)
        loCmdTOMI.Parameters.Add(lpBancoCorresponsal)

        ' Parametro 26
        Dim lpFechaSol As DbParameter = loCmdTOMI.CreateParameter()
        lpFechaSol.ParameterName = "@FECHA_SOL"
        lpFechaSol.DbType = DbType.DateTime
        lpFechaSol.Direction = ParameterDirection.Input
        lpFechaSol.Value = valoresFormato(29)
        loCmdTOMI.Parameters.Add(lpFechaSol)

        ' Parametro 27
        Dim lpHoraSol As DbParameter = loCmdTOMI.CreateParameter()
        lpHoraSol.ParameterName = "@HORA_SOL"
        lpHoraSol.DbType = DbType.String
        lpHoraSol.Size = 5
        lpHoraSol.Direction = ParameterDirection.Input

        If IsDBNull(valoresFormato(29)) Then
            lpHoraSol.Value = valoresFormato(29)
        Else
            lpHoraSol.Value = CDate(valoresFormato(29)).Hour & ":" & CDate(valoresFormato(29)).Minute
        End If

        loCmdTOMI.Parameters.Add(lpHoraSol)

        ' Parametro 28
        Dim lpUsuarioCaptura As DbParameter = loCmdTOMI.CreateParameter()
        lpUsuarioCaptura.ParameterName = "@USUARIO_CAPT"
        lpUsuarioCaptura.DbType = DbType.String
        lpUsuarioCaptura.Size = 15
        lpUsuarioCaptura.Direction = ParameterDirection.Input
        lpUsuarioCaptura.Value = valoresFormato(30)
        loCmdTOMI.Parameters.Add(lpUsuarioCaptura)

        ' Parametro 29
        Dim lpFechaAut As DbParameter = loCmdTOMI.CreateParameter()
        lpFechaAut.ParameterName = "@FECHA_AUT"
        lpFechaAut.DbType = DbType.DateTime
        lpFechaAut.Direction = ParameterDirection.Input
        lpFechaAut.Value = valoresFormato(31)
        loCmdTOMI.Parameters.Add(lpFechaAut)

        ' Parametro 30
        Dim lpHoraAut As DbParameter = loCmdTOMI.CreateParameter()
        lpHoraAut.ParameterName = "@HORA_AUT"
        lpHoraAut.DbType = DbType.String
        lpHoraAut.Size = 5
        lpHoraAut.Direction = ParameterDirection.Input

        If IsDBNull(valoresFormato(31)) Then
            lpHoraAut.Value = valoresFormato(31)
        Else
            lpHoraAut.Value = CDate(valoresFormato(31)).Hour & ":" & CDate(valoresFormato(31)).Minute
        End If

        loCmdTOMI.Parameters.Add(lpHoraAut)

        ' Parametro 31
        Dim lpUsuarioAut As DbParameter = loCmdTOMI.CreateParameter()
        lpUsuarioAut.ParameterName = "@USUARIO_AUT"
        lpUsuarioAut.DbType = DbType.String
        lpUsuarioAut.Size = 15
        lpUsuarioAut.Direction = ParameterDirection.Input
        lpUsuarioAut.Value = valoresFormato(32)
        loCmdTOMI.Parameters.Add(lpUsuarioAut)

        ' Parametro 32
        Dim lpFechaCanc As DbParameter = loCmdTOMI.CreateParameter()
        lpFechaCanc.ParameterName = "@FECHA_CANC"
        lpFechaCanc.DbType = DbType.DateTime
        lpFechaCanc.Direction = ParameterDirection.Input
        lpFechaCanc.Value = valoresFormato(33)
        loCmdTOMI.Parameters.Add(lpFechaCanc)

        ' Parametro 33
        Dim lpHoraCanc As DbParameter = loCmdTOMI.CreateParameter()
        lpHoraCanc.ParameterName = "@HORA_CANC"
        lpHoraCanc.DbType = DbType.String
        lpHoraCanc.Size = 5
        lpHoraCanc.Direction = ParameterDirection.Input

        If IsDBNull(valoresFormato(33)) Then
            lpHoraCanc.Value = valoresFormato(33)
        Else
            lpHoraCanc.Value = CDate(valoresFormato(33)).ToLongTimeString
        End If

        loCmdTOMI.Parameters.Add(lpHoraCanc)

        ' Parametro 34
        Dim lpUsuarioCanc As DbParameter = loCmdTOMI.CreateParameter()
        lpUsuarioCanc.ParameterName = "@USUARIO_CANC"
        lpUsuarioCanc.DbType = DbType.String
        lpUsuarioCanc.Size = 15
        lpUsuarioCanc.Direction = ParameterDirection.Input
        lpUsuarioCanc.Value = valoresFormato(34)
        loCmdTOMI.Parameters.Add(lpUsuarioCanc)

        ' Parametro 35
        Dim lpStatus As DbParameter = loCmdTOMI.CreateParameter()
        lpStatus.ParameterName = "@STATUS"
        lpStatus.DbType = DbType.String
        lpStatus.Direction = ParameterDirection.Input
        lpStatus.Value = valoresFormato(35)
        loCmdTOMI.Parameters.Add(lpStatus)

        ' Parametro 36
        Dim lpOperacion As DbParameter = loCmdTOMI.CreateParameter()
        lpOperacion.ParameterName = "@OPERACION"
        lpOperacion.DbType = DbType.String
        lpOperacion.Size = 25
        lpOperacion.Direction = ParameterDirection.Input
        lpOperacion.Value = valoresFormato(36)
        loCmdTOMI.Parameters.Add(lpOperacion)

        ' Parametro 37
        Dim lpComment As DbParameter = loCmdTOMI.CreateParameter()
        lpComment.ParameterName = "@COMMENT"
        lpComment.DbType = DbType.String
        lpComment.Size = 100
        lpComment.Direction = ParameterDirection.Input
        lpComment.Value = valoresFormato(37)
        loCmdTOMI.Parameters.Add(lpComment)

        ' Parametro 38
        Dim lpFechaConf As DbParameter = loCmdTOMI.CreateParameter()
        lpFechaConf.ParameterName = "@FECHA_CONF"
        lpFechaConf.DbType = DbType.DateTime
        lpFechaConf.Direction = ParameterDirection.Input
        lpFechaConf.Value = valoresFormato(38)
        loCmdTOMI.Parameters.Add(lpFechaConf)

        ' Parametro 39
        Dim lpHoraConf As DbParameter = loCmdTOMI.CreateParameter()
        lpHoraConf.ParameterName = "@HORA_CONF"
        lpHoraConf.DbType = DbType.String
        lpHoraConf.Size = 5
        lpHoraConf.Direction = ParameterDirection.Input

        If IsDBNull(valoresFormato(38)) Then
            lpHoraConf.Value = valoresFormato(38)
        Else
            lpHoraConf.Value = CDate(valoresFormato(38)).ToLongTimeString
        End If

        loCmdTOMI.Parameters.Add(lpHoraConf)

        ' Parametro 40
        Dim lpUsuarioConf As DbParameter = loCmdTOMI.CreateParameter()
        lpUsuarioConf.ParameterName = "@USUARIO_CONF"
        lpUsuarioConf.DbType = DbType.String
        lpUsuarioConf.Size = 15
        lpUsuarioConf.Direction = ParameterDirection.Input
        lpUsuarioConf.Value = valoresFormato(39)
        loCmdTOMI.Parameters.Add(lpUsuarioConf)

        ' Parametro 41
        Dim lpNotaCanc As DbParameter = loCmdTOMI.CreateParameter()
        lpNotaCanc.ParameterName = "@NOTA_CANC"
        lpNotaCanc.DbType = DbType.String
        lpNotaCanc.Size = 250
        lpNotaCanc.Direction = ParameterDirection.Input
        lpNotaCanc.Value = valoresFormato(40)
        loCmdTOMI.Parameters.Add(lpNotaCanc)

        ' Parametro 42
        Dim lpEmailBeneficiario As DbParameter = loCmdTOMI.CreateParameter()
        lpEmailBeneficiario.ParameterName = "@EMAILBENEFICIARIO"
        lpEmailBeneficiario.DbType = DbType.String
        lpEmailBeneficiario.Size = 250
        lpEmailBeneficiario.Direction = ParameterDirection.Input
        lpEmailBeneficiario.Value = valoresFormato(41)
        loCmdTOMI.Parameters.Add(lpEmailBeneficiario)

        ' Parametro 43
        Dim lpEmailOrdenante As DbParameter = loCmdTOMI.CreateParameter()
        lpEmailOrdenante.ParameterName = "@EMAILORDENANTE"
        lpEmailOrdenante.DbType = DbType.String
        lpEmailOrdenante.Size = 250
        lpEmailOrdenante.Direction = ParameterDirection.Input
        lpEmailOrdenante.Value = valoresFormato(42)
        loCmdTOMI.Parameters.Add(lpEmailOrdenante)

        ' Parametro 44
        Dim lpFlgEmail As DbParameter = loCmdTOMI.CreateParameter()
        lpFlgEmail.ParameterName = "@FLGMAIL"
        lpFlgEmail.DbType = DbType.Boolean
        lpFlgEmail.Direction = ParameterDirection.Input
        lpFlgEmail.Value = valoresFormato(43)
        loCmdTOMI.Parameters.Add(lpFlgEmail)

        ' Parametro 45
        Dim lpResidencia As DbParameter = loCmdTOMI.CreateParameter()
        lpResidencia.ParameterName = "@RESIDENCIA"
        lpResidencia.DbType = DbType.String
        lpResidencia.Size = 100
        lpResidencia.Direction = ParameterDirection.Input
        lpResidencia.Value = valoresFormato(44)
        loCmdTOMI.Parameters.Add(lpResidencia)

        ' Parametro 46
        Dim lpRFC As DbParameter = loCmdTOMI.CreateParameter()
        lpRFC.ParameterName = "@RFC"
        lpRFC.DbType = DbType.String
        lpRFC.Size = 20
        lpRFC.Direction = ParameterDirection.Input
        lpRFC.Value = valoresFormato(45)
        loCmdTOMI.Parameters.Add(lpRFC)

        ' Parametro 47
        Dim lpOrigen As DbParameter = loCmdTOMI.CreateParameter()
        lpOrigen.ParameterName = "@ORIGEN"
        lpOrigen.DbType = DbType.String
        lpOrigen.Size = 5
        lpOrigen.Direction = ParameterDirection.Input
        lpOrigen.Value = valoresFormato(46)
        loCmdTOMI.Parameters.Add(lpOrigen)

        ' Parametro 48
        Dim lpCountryBen As DbParameter = loCmdTOMI.CreateParameter()
        lpCountryBen.ParameterName = "@COUNTRYBEN"
        lpCountryBen.DbType = DbType.String
        lpCountryBen.Size = 35
        lpCountryBen.Direction = ParameterDirection.Input
        lpCountryBen.Value = valoresFormato(47)
        loCmdTOMI.Parameters.Add(lpCountryBen)

        ' Parametro 49
        Dim lpTipoBene As DbParameter = loCmdTOMI.CreateParameter()
        lpTipoBene.ParameterName = "@TIPOBENEFICIARIO"
        lpTipoBene.DbType = DbType.String
        lpTipoBene.Size = 1
        lpTipoBene.Direction = ParameterDirection.Input
        lpTipoBene.Value = valoresFormato(11)
        loCmdTOMI.Parameters.Add(lpTipoBene)

        ' Parametro 50
        Dim lpApPaterno As DbParameter = loCmdTOMI.CreateParameter()
        lpApPaterno.ParameterName = "@APELLIDO_P"
        lpApPaterno.DbType = DbType.String
        lpApPaterno.Size = 40
        lpApPaterno.Direction = ParameterDirection.Input
        lpApPaterno.Value = valoresFormato(9)
        loCmdTOMI.Parameters.Add(lpApPaterno)

        ' Parametro 51
        Dim lpApMaterno As DbParameter = loCmdTOMI.CreateParameter()
        lpApMaterno.ParameterName = "@APELLIDO_M"
        lpApMaterno.DbType = DbType.String
        lpApMaterno.Size = 40
        lpApMaterno.Direction = ParameterDirection.Input
        lpApMaterno.Value = valoresFormato(10)
        loCmdTOMI.Parameters.Add(lpApMaterno)

        ' Parametro 52
        Dim lpIdConvenio As DbParameter = loCmdTOMI.CreateParameter()
        lpIdConvenio.ParameterName = "@ID_AGREEMENT"
        lpIdConvenio.DbType = DbType.Int64
        lpIdConvenio.Direction = ParameterDirection.Input
        lpIdConvenio.Value = valoresFormato(48)
        loCmdTOMI.Parameters.Add(lpIdConvenio)

        ' Parametro 53
        Dim lpCityBen As DbParameter = loCmdTOMI.CreateParameter()
        lpCityBen.ParameterName = "@city_res_ben"
        lpCityBen.DbType = DbType.String
        lpCityBen.Size = 32
        lpCityBen.Direction = ParameterDirection.Input
        lpCityBen.Value = valoresFormato(49)
        loCmdTOMI.Parameters.Add(lpCityBen)

        ' Parametro 54
        Dim lpCalleBen As DbParameter = loCmdTOMI.CreateParameter()
        lpCalleBen.ParameterName = "@city_add_ben"
        lpCalleBen.DbType = DbType.String
        lpCalleBen.Size = 45
        lpCalleBen.Direction = ParameterDirection.Input
        lpCalleBen.Value = valoresFormato(50)
        loCmdTOMI.Parameters.Add(lpCalleBen)

        ' Parametro 55
        Dim lpNumBen As DbParameter = loCmdTOMI.CreateParameter()
        lpNumBen.ParameterName = "@city_num_ben"
        lpNumBen.DbType = DbType.String
        lpNumBen.Size = 15
        lpNumBen.Direction = ParameterDirection.Input
        lpNumBen.Value = valoresFormato(51)
        loCmdTOMI.Parameters.Add(lpNumBen)

        ' Parametro 56
        Dim lpColBen As DbParameter = loCmdTOMI.CreateParameter()
        lpColBen.ParameterName = "@city_col_ben"
        lpColBen.DbType = DbType.String
        lpColBen.Size = 30
        lpColBen.Direction = ParameterDirection.Input
        lpColBen.Value = valoresFormato(52)
        loCmdTOMI.Parameters.Add(lpColBen)

        ' Parametro 57
        Dim lpZipBen As DbParameter = loCmdTOMI.CreateParameter()
        lpZipBen.ParameterName = "@city_zip_ben"
        lpZipBen.DbType = DbType.String
        lpZipBen.Size = 10
        lpZipBen.Direction = ParameterDirection.Input
        lpZipBen.Value = valoresFormato(53)
        loCmdTOMI.Parameters.Add(lpZipBen)

        ' Parametro de Salida
        Dim lpSalida As DbParameter = loCmdTOMI.CreateParameter()
        lpSalida.ParameterName = "@SALIDA"
        lpSalida.DbType = DbType.Int16
        lpSalida.Direction = ParameterDirection.Output
        lpSalida.Value = -1
        loCmdTOMI.Parameters.Add(lpSalida)

        '=================================================================================================================
        'Termina Definición de parámetros y hace el llamado
        '=================================================================================================================        
        loCmdTOMI.ExecuteNonQuery()
        loCmdTOMI = Nothing

        write_Log("INFO|BeneficiariosCMS_A_TOMI|Salida=" & lpSalida.Value & ".")

        ' Regreso la salida
        Return IIf(lpSalida.Value = 1, True, False)
    Catch ex As Exception
        write_Log("ERROR|BeneficiariosCMS_A_TOMI|Se presenta exception: " & ex.Message & ". Trace: " & ex.ToString)
    End Try
    Return False
    '---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
End Function
