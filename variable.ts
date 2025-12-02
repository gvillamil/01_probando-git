Public Class MT_950
    Private Const tipoOperacion As String = "MT-950"

    Shared Function procesaOperacion(ByVal icn As String, datos As Dictionary(Of String, Object)) As Boolean
        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Iniciando el registro de la operación.")

        ' Error 
        Throw New Exception("Error en procesamiento de registro, los mensajes MT-950 no deben ser aceptados de forma automatica,  con folio: " & icn)

        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Fin del registro de la operación.")

        ' Termina el proceso del folio
        marca_op_terminada(icn, "PROC", "JEAI")
        write_Log("INFO|" & tipoOperacion & ".procesaOperacion|Proceso terminado.")
        Return True
    End Function
End Class
