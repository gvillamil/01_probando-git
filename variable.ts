Module Definiciones
    '=====================================================================================================================================================================================
    'Definición de Variables
    '=====================================================================================================================================================================================
    Public Const gsNombre_Servicio As String = "Swf_FT"
    Public Const gsTipoOperacion As String = "SWIFT_IN"    ' Swift de entrada
    Public Const gbCierraBases As Boolean = True
    Public gbAceptaDiferentesFechasTOMI_T24 As Boolean = False
    Public goTOMI_Database As MSSQL_Database
    Public goVistasT24_Database As MSSQL_Database
    Public goT24_Connection As T24_MW_Connection
    'Public gsTablaTrabajo As String

    Public gbBases_Inicializadas As Boolean
    Public gbEjecutando_Procedimientos As Boolean
    Public giPID As Integer
    Public gsProcessName As String
    Public gsDisplayName As String
    Public gexUltimaExcepcion As Exception = Nothing

    '' Datos locales solo usados en este modulo
    Public gsFechaSistema As String = ""
    Public gsFechaSistemaJuliana As String = ""
    Public gsFechaSistema_T24_Juliana As String = ""
    Private gbEnHorarioLectura As Boolean = False
    Private gsHoraOperaciones_Inicio As String = ""
    Private gsHoraOperaciones_Fin As String = ""
    Private gbEnHorario As Boolean = False
    Private gbEsDiaHabil As Boolean = False
    Private lbIniciandoServicio As Boolean = True
    Public gsCertPathName As String = String.Empty
    Public gsPswCertificado As String = String.Empty
    Public gsSinFondosHoraValida As String = String.Empty
    Public gsSinFondosTiempoEspera As String = String.Empty
    Public gsWST24UserName As String = String.Empty
    Public gsWST24Password As String = String.Empty
    Public gsWST24Company As String = String.Empty
    Public ApiNewMdwUrl As String = String.Empty

    Private gbCriterio_Completo As Boolean

    '=====================================================================================================================================================================================
    ' Pasa a memoria todos los datos de parametros sistema
    '=====================================================================================================================================================================================
    Private Sub obtenerParametrosSistema()
        Dim lrsDatos As DbDataReader
        Dim lsDatoANT As String

        ' Fecha de Tomi
        lrsDatos = goTOMI_Database.Execute_Query("SELECT fecha,RIGHT(CAST(YEAR(fecha) AS VARCHAR),2) + RIGHT('000' + CAST(DATEPART(""DAYOFYEAR"",fecha) AS VARCHAR),3) AS fecha_jul FROM tb_srv_fecha_sistema WHERE cerrado=0 AND nextday=1 AND openday=1")
        If Not IsNothing(lrsDatos) Then
            lsDatoANT = gsFechaSistema
            Do While lrsDatos.Read
                gsFechaSistema = Format(lrsDatos.Item("fecha"), "yyyyMMdd")
                If lsDatoANT <> gsFechaSistema Then
                    write_Log("INFO|obtenerParametrosSistema|Fecha del sistema '" & gsFechaSistema & "'.")
                End If
                ' Fecha Juliana
                lsDatoANT = gsFechaSistemaJuliana
                gsFechaSistemaJuliana = lrsDatos.Item("fecha_jul")
                If lsDatoANT <> gsFechaSistemaJuliana Then
                    write_Log("INFO|FechaSistemaJuliana='" & gsFechaSistemaJuliana & "'.")
                End If
            Loop
            lrsDatos.Close()
            lrsDatos = Nothing
        End If

        ' Fecha de T24
        lsDatoANT = gsFechaSistema_T24_Juliana
        gsFechaSistema_T24_Juliana = goT24_Connection.obtenerFecha_T24().Substring(2)
        If lsDatoANT <> gsFechaSistema_T24_Juliana Then
            write_Log("INFO|FechaSistema_T24_Juliana='" & gsFechaSistema_T24_Juliana & "'.")
        End If

        If gsFechaSistemaJuliana <> gsFechaSistema_T24_Juliana Then
            Dim lsValorConfiguracion As String

            If lbIniciandoServicio Then
                write_Log("AVISO|Los sistemas tiene diferentes fechas. TOMI='" & gsFechaSistemaJuliana & "' T24='" & gsFechaSistema_T24_Juliana & "'.")
            End If

            ' Se va a forzar la replica completa?
            lsValorConfiguracion = Config.dameValor_Configuracion("generales", "ACEPTA_DIFERENCIA_EN_FECHAS", "value")
            If dameTexto(lsValorConfiguracion) = "1" Then
                If lbIniciandoServicio Then
                    write_Log("AVISO|Se acepta diferencia en fechas.")
                End If
                gbAceptaDiferentesFechasTOMI_T24 = True
            End If

            If Not gbAceptaDiferentesFechasTOMI_T24 Then
                Return
            End If
        End If

        ' Puede iniciar el servicio (debe limitarse a no iniciar sábado, domigo o días festivos, pero para casos especiales se puede forzar su inicio)
        gbEsDiaHabil = puedoIniciarServicio(goTOMI_Database)

        ' Horarios de inicio y fin de operaciones
        lrsDatos = goTOMI_Database.Execute_Query("SELECT id_parametro,valor FROM tb_srv_parametros_sistema WHERE id_parametro IN ('HORA_OP_INICIO','HORA_OP_FIN','SWF_CRITERIO_COMPLETO','DIGSIG_CERT_NUMSER',  'DIGSIG_CERT_PSW', 'SINFONDOS_HORA_VALIDA', 'SINFONDOS_TIEMPO_ESPERA', 'WST24_MW_USERNAME_SWF_MX', 'WST24_MW_PASSWORD_SWF_MX', 'WST24_MW_COMPANY','COMM_API_URL')")
        If Not IsNothing(lrsDatos) Then
            lsDatoANT = gsFechaSistema
            Do While lrsDatos.Read
                Select Case lrsDatos.Item("id_parametro")
                    Case "HORA_OP_INICIO"
                        lsDatoANT = gsHoraOperaciones_Inicio
                        gsHoraOperaciones_Inicio = lrsDatos.Item("valor")
                        If lsDatoANT <> gsHoraOperaciones_Inicio Then
                            write_Log("INFO|obtenerParametrosSistema|Hora de inicio de operaciones '" & gsHoraOperaciones_Inicio & "'.")
                        End If
                    Case "HORA_OP_FIN"
                        lsDatoANT = gsHoraOperaciones_Fin
                        gsHoraOperaciones_Fin = "23:00"
                        If lsDatoANT <> gsHoraOperaciones_Fin Then
                            write_Log("INFO|obtenerParametrosSistema|Hora de finalización de operaciones '" & gsHoraOperaciones_Fin & "'.")
                        End If
                    Case "SWF_CRITERIO_COMPLETO"
                        lsDatoANT = gbCriterio_Completo
                        gbCriterio_Completo = IIf(lrsDatos.Item("valor") = 1, True, False)
                        If lsDatoANT <> gbCriterio_Completo Then
                            write_Log("INFO|obtenerParametrosSistema|Criterio completo '" & gbCriterio_Completo & "'.")
                        End If
                    Case "DIGSIG_CERT_NUMSER"
                        lsDatoANT = gsCertPathName
                        gsCertPathName = lrsDatos.Item("valor")
                        If lsDatoANT <> gsCertPathName Then
                            write_Log("INFO|obtenerParametrosSistema|Número de serie certificado para firma digital '" & gsCertPathName & "'.")
                        End If
                    Case "DIGSIG_CERT_PSW"
                        lsDatoANT = gsPswCertificado
                        gsPswCertificado = lrsDatos.Item("valor")
                        If lsDatoANT <> gsPswCertificado Then
                            write_Log("INFO|obtenerParametrosSistema|Password de certificado para firma digital  '********'.")
                        End If
                    Case "SINFONDOS_HORA_VALIDA"
                        lsDatoANT = gsSinFondosHoraValida
                        gsSinFondosHoraValida = lrsDatos.Item("valor")
                        If lsDatoANT <> gsSinFondosHoraValida Then
                            write_Log("INFO|obtenerParametrosSistema|SINFONDOS_HORA_VALIDA '" & gsSinFondosHoraValida & "'.")
                        End If
                    Case "SINFONDOS_TIEMPO_ESPERA"
                        lsDatoANT = gsSinFondosTiempoEspera
                        gsSinFondosTiempoEspera = lrsDatos.Item("valor")
                        If lsDatoANT <> gsSinFondosTiempoEspera Then
                            write_Log("INFO|obtenerParametrosSistema|SINFONDOS_TIEMPO_ESPERA '" & gsSinFondosTiempoEspera & "'.")
                        End If
                    Case "WST24_MW_USERNAME_SWF_MX"
                        lsDatoANT = gsWST24UserName
                        gsWST24UserName = lrsDatos.Item("valor")
                        If lsDatoANT <> gsWST24UserName Then
                            write_Log("INFO|obtenerParametrosSistema|WST24_MW_USERNAME_SWF_MX '" & gsWST24UserName & "'.")
                        End If
                    Case "WST24_MW_PASSWORD_SWF_MX"
                        lsDatoANT = gsWST24Password
                        gsWST24Password = lrsDatos.Item("valor")
                        If lsDatoANT <> gsWST24Password Then
                            write_Log("INFO|obtenerParametrosSistema|WST24_MW_PASSWORD_SWF_MX  '********'.")
                        End If
                    Case "WST24_MW_COMPANY"
                        lsDatoANT = gsWST24Company
                        gsWST24Company = lrsDatos.Item("valor")
                        If lsDatoANT <> gsWST24Company Then
                            write_Log("INFO|obtenerParametrosSistema|WST24_MW_COMPANY '********'.")
                        End If
                    Case "COMM_API_URL"
                        lsDatoANT = ApiNewMdwUrl
                        ApiNewMdwUrl = lrsDatos.Item("valor")
                        If lsDatoANT <> ApiNewMdwUrl Then
                            write_Log("INFO|obtenerParametrosSistema| Url de la suite de APIs MDW: '" & ApiNewMdwUrl & "'.")
                        End If
                End Select
            Loop
            lrsDatos.Close()
            lrsDatos = Nothing
        End If
        ' Validación de hora de inicio/fin de operaciones
        If gsHoraOperaciones_Inicio = "" Then
            Throw New Exception("No se especifica la hora de inicio de operaciones")
        End If
        If gsHoraOperaciones_Fin = "" Then
            Throw New Exception("No se especifica la hora de finalización de operaciones")
        End If
        ' Hora de inicio de operaciones
        'gsHoraOperaciones_Inicio = Config.dameValor_Configuracion("horarios", "OPERACION", "hora_inicio")
        ' Hora de fin de operaciones
        'gsHoraOperaciones_Fin = Config.dameValor_Configuracion("horarios", "OPERACION", "hora_final")
    End Sub

    '=====================================================================================================================================================================================
    ' Procedimiento Principal para el procesamiento de las operaciones
    '=====================================================================================================================================================================================
    Public Sub Ejecuta_Procedimientos()
        Try
            Dim lsHoraSistema As String

            Actualiza_Ejecucion(goTOMI_Database, gsNombre_Servicio, giPID, gsProcessName)

            '' Obtiene los parametros del sistema (variables, rutas, fechas, etc)
            obtenerParametrosSistema()

            ' Para validar si está en horario de operación o no
            lsHoraSistema = Now.ToString("HH:mm")
            If lsHoraSistema >= gsHoraOperaciones_Inicio And lsHoraSistema <= gsHoraOperaciones_Fin Then
                ' Bandera para saber si estoy en horario de operaciones o no
                If Not gbEnHorarioLectura Then
                    gbEnHorarioLectura = True
                    write_Log("INFO|Ejecuta_Procedimientos|Inicio horario de lectura, HoraSistema='" & lsHoraSistema & "'.")
                End If
            Else
                ' FUERA DE HORARIO
                If gbEnHorarioLectura Then
                    gbEnHorarioLectura = False
                    write_Log("INFO|Ejecuta_Procedimientos|Termina horario de lectura, HoraSistema='" & lsHoraSistema & "'")
                End If
            End If

            ' Valido el tipo de operación - horario
            If Not gbEnHorarioLectura Then
                write_Log("ERROR|Ejecuta_Procedimientos|El servicio está fuera de horario.")
                inserta_alerta(Nothing, Nothing, gsNombre_Servicio, "ERROR", "El servicio está fuera de horario", giPID, gsProcessName, New Exception("El servicio '" & gsNombre_Servicio & "' está fuera de horario."), goTOMI_Database)
                Return
            End If

            ' Valido que sea día hábil
            If gbEsDiaHabil Then
                ' Imprime opereraciones
                realiza_operaciones()
            Else
                write_Log("ERROR|Ejecuta_Procedimientos|Hoy es un día inhábil.")
            End If

            lbIniciandoServicio = False

            '' Fin de los procesos a realizar
            Actualiza_Terminacion(goTOMI_Database, gsNombre_Servicio, giPID, gsProcessName)
        Catch ex As Exception
            write_Log("ERROR|Ejecuta_Procedimientos|Se presenta exception: " & ex.ToString)
            ' Inserto la alarma
            inserta_alerta(Nothing, Nothing, gsNombre_Servicio, "CRITICO", "Error en ejecuta_procedimientos", giPID, gsProcessName, ex, goTOMI_Database)
        End Try
        '=====================================================================================================================================================================================
    End Sub

    '=====================================================================================================================================================================================
    ' Para la realizacion de las operaciones
    '=====================================================================================================================================================================================
    Private Sub realiza_operaciones()
        'Cerrar operaciones de fechas anteriores
        CerrarOperacionesAnteriores()

        'Actualizando posibles mensjaes pendientes
        ActualizaPlanTrabajo()

        ' Verificamos si existen mensajes a interpretar.
        registra_FT()
    End Sub

    '=====================================================================================================================================================================================
    ' Actualizar posibles operaciones pendientes 
    '=====================================================================================================================================================================================
    Private Sub CerrarOperacionesAnteriores()
        Try
            write_Log("INFO|CerrarOperacionesAnteriores|Cerrando operaciones de fechas anteriores")

            Dim strSQL As String = String.Empty
            Dim intCont As Integer = 0

            strSQL = "UPDATE TB_SRV_SWF_PLAN_TRABAJO_MAESTRO SET flg_cierre = 1 WHERE cve_estatus IN ('PROC','RECC') AND fec_valor < " + gsFechaSistema + " and flg_cierre = 0 "
            intCont = goTOMI_Database.Execute_Command(strSQL)

            write_Log("INFO|CerrarOperacionesAnteriores|Se cerraron " & intCont & " operaciones.")

        Catch ex As Exception
            write_Log("ERROR|CerrarOperacionesAnteriores|Se presenta exception: " & ex.ToString)
        End Try
    End Sub

    '=====================================================================================================================================================================================
    ' Actualizar posibles operaciones pendientes 
    '=====================================================================================================================================================================================
    Private Sub ActualizaPlanTrabajo()
        Try
            write_Log("INFO|ActualizaPlanTrabajo|Actualizando posibles operaciones autorizadas pero con plan de trabajo inconcluso")

            Dim strSQL As String = String.Empty
            Dim intCont As Integer = 0

            strSQL = "UPDATE TB_SRV_SWF_PLAN_TRABAJO_DETALLE SET flg_terminado = 1, fec_fin = GETDATE()  WHERE orden_ejecucion  = 4 AND folio in("
            strSQL += "SELECT DISTINCT A.folio FROM TB_SRV_SWF_PLAN_TRABAJO_MAESTRO A INNER JOIN TB_SRV_SWF_PLAN_TRABAJO_DETALLE B ON A.tipo_operacion = B.tipo_operacion AND A.folio = B.folio  "
            strSQL += "WHERE B.orden_ejecucion = 3 AND A.cve_estatus = 'AUT' AND flg_terminado = 1)"

            intCont = goTOMI_Database.Execute_Command(strSQL)

            write_Log("INFO|ActualizaPlanTrabajo|Se actualizaron " & intCont & " mensajes.")

        Catch ex As Exception
            write_Log("ERROR|ActualizaPlanTrabajo|Se presenta exception: " & ex.ToString)
        End Try
    End Sub

    '=====================================================================================================================================================================================
    ' Verificamos si existen a interpretar
    '=====================================================================================================================================================================================
    Private Sub registra_FT()
        Try
            Dim loOps_FT As Dictionary(Of String, Dictionary(Of String, Object))
            Dim lsId_ICN As String
            Dim loDatos As Dictionary(Of String, Object)
            Dim lsTipoMensaje As String, lsFechaEjecucion As String, lsMoneda As String, lsCuenta As String
            Dim lbResultado As Boolean, lbFechaFutura As Boolean

            ' Inicializacion de variables

            ' Busca los datos en DB a analizar (regresa las operaciones con campos: id_icn, des_mq_msg)
            loOps_FT = busca_operaciones_FT()

            For Each lsId_ICN In loOps_FT.Keys
                Try
                    write_Log("INFO|SWF_IN.registra_FT|===================== Inicia analisis de operación ICN=" & lsId_ICN & " =====================.")
                    loDatos = loOps_FT(lsId_ICN)
                    lsTipoMensaje = loDatos("cve_mensaje")
                    lsFechaEjecucion = loDatos("fec_valor")
                    lsMoneda = goT24_Connection.valida_moneda(loDatos("cve_moneda"))
                    lsCuenta = dameTexto(loDatos("num_cuenta"))

                    ' Sigo con el proceso
                    If lsFechaEjecucion < gsFechaSistema And Not (lsTipoMensaje = "MT-191" Or lsTipoMensaje = "CAMT106")  Then
                        Throw New Exception("Se desea realizar una operación al " & lsFechaEjecucion & " cuando la fecha del sistema es " & gsFechaSistema & ".")
                    End If

                    ' Fecha Futura?
                    lbFechaFutura = lsFechaEjecucion > gsFechaSistema

                    If lbFechaFutura Then
                        write_Log("INFO|SWF_IN.registra_FT|Operación fecha valor futuro.")
                    Else
                        Select Case lsTipoMensaje
                            Case "MT-103"
                                If (lsMoneda = goT24_Connection.MonedaDefaultMN) And (lsCuenta.Length > 7) And (lsCuenta.Substring(0, 3) <> goT24_Connection.Clave_CASFIM.Substring(2, 3)) Then
                                    lbResultado = MT_103_MXN.procesaOperacion(lsId_ICN, loDatos)
                                Else
                                    lbResultado = MT_103.procesaOperacion(lsId_ICN, loDatos)
                                End If
                            Case "MT-191"
                                lbResultado = MT_191.procesaOperacion(lsId_ICN, loDatos)
                            Case "MT-202"
                                lbResultado = MT_202.procesaOperacion(lsId_ICN, loDatos)
                            Case "MT-210"
                                lbResultado = MT_210.procesaOperacion(lsId_ICN, loDatos)
                            Case "MT-950"
                                lbResultado = MT_950.procesaOperacion(lsId_ICN, loDatos)
                            Case "CAMT106"
                                lbResultado = CAMT106.procesaOperacion(lsId_ICN, loDatos)
                            Case "PACS008"
                                If (lsMoneda = goT24_Connection.MonedaDefaultMN) And (lsCuenta.Length > 7) And (lsCuenta.Substring(0, 3) <> goT24_Connection.Clave_CASFIM.Substring(2, 3)) Then
                                    lbResultado = PACS008_MXN.procesaOperacion(lsId_ICN, loDatos)
                                Else
                                    lbResultado = PACS008.procesaOperacion(lsId_ICN, loDatos)
                                End If
                            Case "PACS009"
                                lbResultado = PACS009.procesaOperacion(lsId_ICN, loDatos)
                            Case Else
                                Throw New Exception("Mensaje '" & lsTipoMensaje & "' no tiene logica de procesamiento")
                        End Select
                    End If

                    write_Log("INFO|SWF_IN.registra_FT|===================== Se termina el procesamiento del ICN " & lsId_ICN & " =====================.")
                Catch lexErrorOp As Exception
                    write_Log("ERROR|SWF_IN.registra_FT|" & lexErrorOp.ToString)
                    ' Marca el proceso con error
                    marca_op_con_error(lsId_ICN, lexErrorOp.Message)
                End Try
            Next
        Catch lexError As Exception
            write_Log("ERROR|SWF_IN.registra_FT|" & lexError.ToString)
        End Try
    End Sub

    Private Function busca_operaciones_FT() As Dictionary(Of String, Dictionary(Of String, Object))
        Dim lrsDatos As DbDataReader
        Dim loCmd As DbCommand
        Dim loDatos As Dictionary(Of String, Object)
        Dim loRegreso As Dictionary(Of String, Dictionary(Of String, Object))
        Dim lsICN As String

        ' Inicio de variables
        loRegreso = New Dictionary(Of String, Dictionary(Of String, Object))

        ' Definición de Procedimiento Almacenado
        loCmd = goTOMI_Database.newCommand("usp_swf_ft_ops")
        loCmd.CommandType = CommandType.StoredProcedure
        ' Definición de parámetros
        loCmd.Parameters.Clear()
        ' Parametro 2
        Dim lpTipoOperacion As DbParameter = loCmd.CreateParameter()
        lpTipoOperacion.ParameterName = "@tipo_operacion"
        lpTipoOperacion.DbType = DbType.String
        lpTipoOperacion.Direction = ParameterDirection.Input
        lpTipoOperacion.Value = gsTipoOperacion          ' Mensaje Swift de entrada
        loCmd.Parameters.Add(lpTipoOperacion)
        ' Termina Definición de parámetros y hace el llamado
        lrsDatos = loCmd.ExecuteReader()
        If Not IsNothing(lrsDatos) Then
            While lrsDatos.Read
                lsICN = lrsDatos("folio")
                'id_icn,cve_mensaje,fec_valor,cve_moneda,des_importe,num_cuenta,num_cliente,nombre_cliente,des_variables_xml
                loDatos = New Dictionary(Of String, Object)
                loDatos.Add("id_icn", lrsDatos("folio"))
                loDatos.Add("folio_unico", lrsDatos("folio_unico"))
                loDatos.Add("cve_mensaje", lrsDatos("cve_mensaje"))
                loDatos.Add("fec_valor", lrsDatos("fec_valor"))
                loDatos.Add("cve_moneda", goT24_Connection.valida_moneda(lrsDatos("cve_moneda")))
                loDatos.Add("des_importe", lrsDatos("des_importe"))
                loDatos.Add("num_cuenta", lrsDatos("num_cuenta"))
                loDatos.Add("num_cliente", lrsDatos("num_cliente"))
                loDatos.Add("nombre_cliente", lrsDatos("nombre_cliente"))
                loDatos.Add("des_variables_xml", lrsDatos("nf_variables_xml"))
                loDatos.Add("num_cif_emisor", lrsDatos("nf_cif_emisor"))
                loDatos.Add("cta_vostro_emisor", lrsDatos("nf_vostro_emisor"))
                loDatos.Add("cve_swift_emisor", lrsDatos("nf_swift_emisor"))
                ' Alta del dato
                loRegreso.Add(lsICN, loDatos)
            End While
            lrsDatos.Close()
            lrsDatos = Nothing
        End If
        Return loRegreso
    End Function
End Module
