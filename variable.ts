USE [InterfacesT24]
GO
/****** Object:  StoredProcedure [dbo].[usp_TOMBIN_CONSULTA_PAGOS_COMPENSACION_SPEI_OUT]    Script Date: 1/23/2026 12:56:38 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[usp_TOMBIN_CONSULTA_PAGOS_COMPENSACION_SPEI_OUT]
@FECHA_INICIO	varchar(8),
@FECHA_FINAL	varchar(8),
@CDL			varchar(8),
@CUENTA			varchar(7)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	DECLARE @ConsultaSQL NVarchar(3000)
	DECLARE @Condiciones NVarchar(200)
	DECLARE @TIEMPO_PROC_SPEI_OUT INT
	declare @fechaActual int 
	
	select @fechaActual = cast((convert (varchar(8),getdate(),112)) as int)
	select @TIEMPO_PROC_SPEI_OUT = VALOR from TB_SRV_PARAMETROS_SISTEMA where ID_PARAMETRO = 'TIEMPO_PROCESAMIENTO_SPEI_OUT'
    -- Insert statements for procedure here	
		
		set @ConsultaSQL = 'DECLARE @UMA_FEE_MONTO DECIMAL(18,2); 
							 select @UMA_FEE_MONTO = VALOR from TB_SRV_PARAMETROS_SISTEMA where ID_PARAMETRO = ''UMA_FEE_MONTO''
							
							Select folio,Fecha,Tracking_Key, Importe, CDL, Nombre_Ordenante, Cuenta_Ordenante,
									Nombre_Beneficiario,CuentaBeneficiario,BancoBeneficiario,Tiempo_Total,CuentaCompensar,
									Dias_Transcurridos,ISNULL(ESTATUS,''DISPONIBLE'') as Estatus,
									CAST((ISNULL(dbo.ufn_Calcula_Compensacion(Dias_Transcurridos,Importe,Tiempo_Total,FechaTasa),0)) as DECIMAL(18,2)) as Monto_Compensado,
									Dif_Karpay
								FROM 
									VW_PAGO_COMPENSACIONES_OUT with(nolock)
								WHERE Fecha between convert(int,convert(varchar,@FECHA_INICIO,112)) and convert(INT,convert(varchar,@FECHA_FINAL,112))
								AND Tiempo_Total > @TIEMPO_PROC_SPEI_OUT AND CAST((ISNULL(dbo.ufn_Calcula_Compensacion(Dias_Transcurridos,Importe,Tiempo_Total,FechaTasa),0)) as DECIMAL(18,2)) > 2.00 '

		
		IF @CDL <> ''
		begin
		set @Condiciones = ' and CDL = '+ @CDL
		end

		IF @CUENTA <>''
		begin
		set @Condiciones += ' and CuentaCompensar = '+ @CUENTA
		end

		if @Condiciones <> ''
		begin
		set @ConsultaSQL += @Condiciones
		end
		
		print @ConsultaSQL
			
	EXEC SP_EXECUTESQL @ConsultaSQL, N'@FECHA_INICIO varchar(8), @FECHA_FINAL varchar(8), @TIEMPO_PROC_SPEI_OUT INT,@fechaActual INT',@FECHA_INICIO,@FECHA_FINAL,@TIEMPO_PROC_SPEI_OUT,@fechaActual

		 
END

