USE Bot;
IF OBJECT_ID('dbo.usp_TMD_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TMD_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TMD_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE tbl_tmd_formato SET mensaje='Formato aceptado',estatus='APBT' WHERE id_formato=@Folio;
		
		INSERT INTO tbl_t24_tmd_formato VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_TMD_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TMD_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TMD_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE tbl_tmd_formato 
			   SET mensaje='Formato rechazado'
			   ,razonRechazo=@RazonRechazo
			   ,estatus='RECC' 
		WHERE id_formato=@Folio 
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_FXT_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXT_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXT_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY
		UPDATE formato SET mensaje='Formato aceptado',estatus='APBT' WHERE id_formato=@Folio;
		
		INSERT INTO tbl_t24_formato VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_FXT_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXT_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXT_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE tbl_tmd_formato 
			   SET mensaje='Formato rechazado'
			   ,razonRechazo=@RazonRechazo
			   ,estatus='RECC' 
		WHERE id_formato=@Folio 
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*
*/
USE Bot;
IF OBJECT_ID('dbo.usp_MXF_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_MXF_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_MXF_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY
		UPDATE formato SET mensaje='Formato aceptado',estatus='APBT' WHERE id_formato=@Folio;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
IF OBJECT_ID('dbo.usp_MFX_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_MFX_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_MFX_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE formato 
			  SET mensaje='Formato rechazado'
			  ,estatus='RECC' 
		WHERE id_formato=@Folio 
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_TEF_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TEF_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TEF_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE tbl_tef_formato 
			   SET mensaje='Formato aceptado',estatus='APBT' 
		WHERE id_formato=@Folio

		INSERT INTO tbl_t24_tef_formato VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_TEF_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TEF_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TEF_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE tbl_tef_formato 
			   SET mensaje='Formato rechazado'
				  ,razonRechazo=@RazonRechazo
		          ,estatus='RECC' 
		WHERE id_formato=@Folio 
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_SAT_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_SAT_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_SAT_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@LlavePago		VARCHAR(30),
	@FechaEjecucion VARCHAR(15),
	@SelloDigital   VARCHAR(200),
	@CadenaOriginal VARCHAR(200),
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE formato 
			   SET contenidoFormato=REPLACE(contenidoFormato,SUBSTRING(contenidoFormato,CHARINDEX('|10015=',contenidoFormato),CHARINDEX('|10016=',contenidoFormato)-CHARINDEX('|10015=',contenidoFormato)+1),CONCAT'|10015=',@LlavePago,'|') 
		WHERE tipoFormato=13 AND id_formato=@Folio 
		
		UPDATE formato 
				SET mensaje='Formato aceptado'
				,estatus='APBT'
				,fechaEjecucion=@FechaEjecucion
				,formatoXML_det=@LlavePago
				,contenidoSelloDigit=@SelloDigital
				,cadenaOriginal=@CadenaOriginal 
		WHERE id_formato=@Folio

		INSERT INTO tbl_t24_formato VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;
/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_SAT_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_SAT_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_SAT_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
	@FechaEjecucion VARCHAR(15),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE formato 
			   SET mensaje='Formato rechazado'
			   ,fechaEjecucion=@FechaEjecucion,
			   formatoXML_det='BTMUM'
			   ,razonRechazo=@RazonRechazo
			   ,estatus='RECC' 
		WHERE id_formato=@Folio; 
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_FXD_ActualizaFormatoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXD_ActualizaFormatoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXD_ActualizaFormatoCms
(
    @Folio          INT,
	@Procesadas		INT,
	@Devueltas		INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE formato_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
				   ,mensaje='Formato aceptado'
				   ,estatus='APBT' 
			WHERE id_formato=@Folio;

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE formato_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas 
			WHERE id_formato=@Folio;

		END

		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_SPID_ActualizaFormatoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_SPID_ActualizaFormatoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_SPID_ActualizaFormatoCms
(
    @Folio          INT,
	@Procesadas		INT,
	@Devueltas		INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE tbl_spid_formato_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
				   ,mensaje='Formato aceptado'
				   ,estatus='APBT' 
			WHERE id_formato=@Folio;

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tbl_spid_formato_pap 
				  SET nopagosprocesados=@Procesadas
				  ,nopagosdevueltos=@Devueltas
			WHERE id_formato=@Folio;

		END

		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_NOM_FXD_ActualizaFormatoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_NOM_FXD_ActualizaFormatoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_NOM_FXD_ActualizaFormatoCms
(
    @Folio          INT,
	@Procesadas		INT,
	@Devueltas		INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE formato_nomina 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
				   ,mensaje='Formato aceptado'
				   ,estatus='APBT' 
			WHERE id_formato=@Folio;

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE formato_nomina 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas 
			WHERE id_formato=@Folio;

		END

		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
IF OBJECT_ID('dbo.isp_NOM_FXD_InsertarFormatoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.isp_NOM_FXD_InsertarFormatoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.isp_NOM_FXD_InsertarFormatoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		DECLARE @valor INT;

		SELECT @valor=COUNT(1) FROM tbl_t24_formato_nomina WHERE id_formato=@Folio;

		IF(@valor=0)
			INSERT INTO tbl_t24_formato_nomina VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_FXT_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXT_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXT_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE formato_pap_detalle 
			   SET mensaje='Formato aceptado'
			   ,estatus='APBT'
			   ,nopagosprocesados=1 
		WHERE id_formato=@Folio

		INSERT INTO tbl_t24_formato_pap VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_FXT_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXT_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXT_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE formato_pap_detalle 
			   SET mensaje='Formato rechazado'
			   ,razonRechazo=@RazonRechazo
			   ,estatus='RECC'
			   ,nopagosdevueltos=1 
		
		WHERE id_formato=@Folio; 
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_FXT_ActualizaFormatoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXT_ActualizaFormatoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXT_ActualizaFormatoCms
(
    @Folio          INT,
	@Procesadas		INT,
	@Devueltas		INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE formato_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
				   ,mensaje='Formato aceptado'
				   ,estatus='APBT'
			WHERE id_formato=@Folio;

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE formato_pap 
			       SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
			WHERE id_formato=@Folio;

		END

		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_FXI_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE formato
			   SET mensaje='Formato aceptado'
			   ,estatus='APBT'
		WHERE id_formato=@Folio

		INSERT INTO tbl_t24_formato VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_FXI_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE formato
			   SET mensaje='Formato aceptado'
			   ,estatus='APBT'
		WHERE id_formato=@Folio

		INSERT INTO tbl_t24_formato VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_FXI_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		UPDATE formato 
			   SET mensaje='Formato rechazado'
			   ,razonRechazo=@RazonRechazo
			   ,estatus='RECC' 
		WHERE id_formato=@Folio  
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_FXI_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXI_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXI_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY
		
		UPDATE formato_pap_detalle
			   SET mensaje='Formato aceptado'
			   ,estatus='APBT'
			   ,nopagosprocesados=1
		WHERE id_formato=@Folio

		INSERT INTO tbl_t24_formato_pap VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_FXI_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXI_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXI_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		
		UPDATE formato 
			   SET mensaje='Formato rechazado'
			   ,razonRechazo=@RazonRechazo
			   ,estatus='RECC' 
			   ,nopagosdevueltos=1
		WHERE id_formato=@Folio  
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

IF OBJECT_ID('dbo.usp_PaP_FXI_ActualizaFormatoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXI_ActualizaFormatoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXI_ActualizaFormatoMaestro
(
    @Folio          INT,
	@Procesados		INT,
    @Devueltos		INT,
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN

			UPDATE formato_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos
				   ,mensaje='Formato aceptado'
				   ,estatus='APBT' 
			WHERE id_formato=@Folio;

		END

		IF(@Opcion = 2) BEGIN

			UPDATE formato_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos 
			WHERE id_formato=@Folio;


		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_TEF_ActualizaFormatoAceptadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_TEF_ActualizaFormatoAceptadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_TEF_ActualizaFormatoAceptadoCms
(
    @Folio          INT,
	@sFT			VARCHAR(30),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY
		
		UPDATE tbl_tef_formato_pap_detalle 
			   SET mensaje='Formato aceptado'
			   ,estatus='APBT',nopagosprocesados=1 
		WHERE id_formato=@Folio

		INSERT INTO tbl_t24_tef_formato_pap VALUES(@Folio,@sFT);

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
USE Bot;
IF OBJECT_ID('dbo.usp_PaP_TEF_ActualizaFormatoRechazadoCms', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_TEF_ActualizaFormatoRechazadoCms AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_TEF_ActualizaFormatoRechazadoCms
(
    @Folio          INT,
	@RazonRechazo   VARCHAR(255),
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY
		
		UPDATE tbl_tef_formato_pap_detalle 
			   SET mensaje='Formato rechazado'
			   ,razonRechazo=@RazonRechazo
			   ,estatus='RECC' 
			   ,nopagosdevueltos=1
		WHERE id_formato=@Folio  
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO;

/*

*/
IF OBJECT_ID('dbo.usp_PaP_TEF_ActualizaFormatoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_TEF_ActualizaFormatoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_TEF_ActualizaFormatoMaestro
(
    @Folio          INT,
	@Procesados		INT,
    @Devueltos		INT,
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN

			UPDATE tbl_tef_formato_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos
				   ,mensaje='Formato aceptado'
				   ,estatus='APBT' 
			WHERE id_formato=@Folio;

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tbl_tef_formato_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos 
			WHERE id_formato=@Folio;


		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END;
GO;

/*

*/
IF OBJECT_ID('dbo.usp_PaP_TEF_ActualizaFormatoCms_Maestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_TEF_ActualizaFormatoCms_Maestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_TEF_ActualizaFormatoCms_Maestro
(
    @Folio          INT,
	@Procesados		INT,
    @Devueltos		INT,
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN

			UPDATE tbl_tef_formato_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos
				   ,mensaje='Formato aceptado'
				   ,estatus='APBT' 
			WHERE id_formato=@Folio;

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tbl_tef_formato_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos 
			WHERE id_formato=@Folio;


		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END;
GO;

/*

*/
IF OBJECT_ID('dbo.usp_Comunes_ActualizaCmsFormato', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaCmsFormato AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaCmsFormato
(
    @TablaCms VARCHAR(100),
	@TablaTomi VARCHAR(100),
	@HoraAutorizacion VARCHAR(18),
	@Folio VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE ' + @TablaCms + ' SET mensaje=''Formato aceptado'', formatoXML= ' + @HoraAutorizacion + ', estatus=''APBT'' ';
	
		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
			SET @sqlQuery = @sqlQuery + ',nopagosdevueltos=0,nopagosprocesados=1 ';


        SET @sqlQuery = @sqlQuery + ' WHERE id_formato=@Folio';

		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_Insertar_T24Formato', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Insertar_T24Formato AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Insertar_T24Formato
(
    @Folio          INT,
	@sFt			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

			INSERT INTO tbl_t24_formato VALUES(@Folio,@sFt);

			SET @RowsAffected = @@ROWCOUNT;

			COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END;
GO;

/*
*/
IF OBJECT_ID('dbo.usp_Insertar_T24Formato_PaP_detalle', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Insertar_T24Formato_PaP_detalle AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Insertar_T24Formato_PaP_detalle
(
    @Folio          INT,
	@sFt			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

			INSERT INTO tbl_t24_formato_pap VALUES(@Folio,@sFt);

			SET @RowsAffected = @@ROWCOUNT;

			COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END;
GO;

/*

*/

IF OBJECT_ID('dbo.usp_Comunes_ActualizaFormatoCms_SPEI_Rechazado', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaFormatoCms_SPEI_Rechazado AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaFormatoCms_SPEI_Rechazado
(
    @TablaCms		VARCHAR(100),
	@TablaTomi		VARCHAR(100),
	@RazonRechazo	VARCHAR(100) = '',
	@CodigoMensaje	VARCHAR(15),
	@Folio			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE ' + @TablaCms + ' SET mensaje=''Formato rechazado'',razonRechazo=CASE WHEN @RazonRechazo <> '''' THEN ''SPEI Devolucion'' ELSE '''' END' +
						',estatus=''RECC'' ';
	
		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
			SET @sqlQuery = @sqlQuery + ' ,nopagosdevueltos=1,nopagosprocesados=0 ';


		IF(@CodigoMensaje <> '')
			SET @sqlQuery = @sqlQuery + ',id_CodigoMensaje = @CodigoMensaje ';


        SET @sqlQuery = @sqlQuery + ' WHERE id_formato=@strFolio ';

		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30), @CodigoMensaje ', @Folio = @Folio,@CodigoMensaje=@CodigoMensaje;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO


USE Bot;
IF OBJECT_ID('dbo.usp_Comunes_ActualizaFormatoLiquidadaCms_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaFormatoLiquidadaCms_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaFormatoLiquidadaCms_BBVA
(
	@TablaCms		VARCHAR(100),
    @Folio          INT,
	@FechaPago		VARCHAR(18),
	@CieGuideBBVA	VARCHAR(100),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		DECLARE @SqlQuery NVARCHAR(Max) = '';

		SET @SqlQuery = 'UPDATE ' + @TablaCms + ' SET Message = ''Formato Aceptado'', Status=''APBT'', PaymentDate = ' + @FechaPago + 
						',CieGuide = @CieGuideBBVA' + 
                        ' WHERE id_formato = @Folio';
		
		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30),@CieGuideBBVA VARCHAR(100)',@Folio = @Folio, @CieGuideBBVA = @CieGuideBBVA;

		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
GO

USE Bot;
IF OBJECT_ID('dbo.usp_Comunes_ActualizaFormatoRechazadoCms_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaFormatoRechazadoCms_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaFormatoRechazadoCms_BBVA
(
	@TablaCms		VARCHAR(100),
    @Folio          INT,
	@RazonRechazo	VARCHAR(18),
	@IdRazon		VARCHAR(100),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		DECLARE @SqlQuery NVARCHAR(Max) = '';

		SET @SqlQuery = 'UPDATE ' + @TablaCms + 'SET Message = ''Formato Rechazado'', ReasonRejection = ' + @RazonRechazo +
					    ' ,ID_ReasonRejection = ' + @IdRazon + ', Status=''RECC'' ' + 
                        ' WHERE Id_Format = @Folio';
		
		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30) ',@Folio = @Folio;


		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

IF OBJECT_ID('dbo.ssp_Consultar_CmsOrigen_SPID_GPH', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_CmsOrigen_SPID_GPH AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_CmsOrigen_SPID_GPH
(
	@Folio			VARCHAR(30),
	@TablaTomi		VARCHAR(30)
)
AS
BEGIN

	DECLARE @sqlQuery NVARCHAR(MAX) = '';

	
	SET @sqlQuery = ' SELECT nf_estatus, nf_razon, nf_fecha_autoriza,t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda,  ' +
					' 0 AS nf_comision_monto,0 AS nf_iva_monto,nf_estatus_interno,t24_transaction_id, nf_nombre_archivo_gph, ' +
					' nf_pmt_inf_id, nf_id_archivo ' +
					' FROM ' + @TablaTomi + '  ' +
					' WHERE  nf_folio= @Folio ' +
					' AND ( nf_estatus=''RECC'' OR (nf_estatus=''PROC'' AND nf_tipo<>''FXD'') ' + 
					         'OR (nf_estatus=''PROC'' AND nf_tipo=''FXD'' AND nf_estatus_interno IN (''LQ'',''D'',''CN'',''CL'',''RB'')) ) ';

	EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

END
GO


IF OBJECT_ID('dbo.ssp_Consultar_CmsOrigen_SPID', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_CmsOrigen_SPID AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_CmsOrigen_SPID
(
	@Folio			VARCHAR(30),
	@TablaTomi		VARCHAR(30)
)
AS
BEGIN

	DECLARE @sqlQuery NVARCHAR(MAX) = '';
	
	SET @sqlQuery = ' SELECT nf_estatus,nf_razon,nf_fecha_autoriza,t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda,  ' +
					' 0 AS nf_comision_monto,0 AS nf_iva_monto,nf_estatus_interno,t24_transaction_id ' +
					' ,cd_clave ' +
					' FROM ' + @TablaTomi + '  ' +
					' WHERE  nf_folio= @Folio ' +
					' AND ( nf_estatus=''RECC'' OR (nf_estatus=''PROC'' AND nf_tipo<>''FXD'') ' + 
					         'OR (nf_estatus=''PROC'' AND nf_tipo=''FXD'' AND nf_estatus_interno IN (''LQ'',''D'',''CN'',''CL'',''RB'')) ) ';

	EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

END
GO

IF OBJECT_ID('dbo.usp_Insertar_T24Formato_SPID', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Insertar_T24Formato_SPID AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Insertar_T24Formato_SPID
(
    @Folio          INT,
	@sFt			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

			INSERT INTO tbl_t24_spid_formato VALUES(@Folio,@sFt);

			SET @RowsAffected = @@ROWCOUNT;

			COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END;
GO;

IF OBJECT_ID('dbo.usp_Insertar_T24Formato_SPID_PaP', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Insertar_T24Formato_SPID_PaP AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Insertar_T24Formato_SPID_PaP
(
    @Folio          INT,
	@sFt			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

			INSERT INTO tbl_t24_spid_formato_pap VALUES(@Folio,@sFt);

			SET @RowsAffected = @@ROWCOUNT;

			COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END;
GO;

IF OBJECT_ID('dbo.usp_Comunes_ActualizaFormatoCms_SPID_Rechazado', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaFormatoCms_SPID_Rechazado AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaFormatoCms_SPID_Rechazado
(
    @TablaCms		VARCHAR(100),
	@TablaTomi		VARCHAR(100),
	@RazonRechazo	VARCHAR(100) = '',
	@CodigoMensaje	VARCHAR(15),
	@Folio			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE ' + @TablaCms + ' SET mensaje=''Formato rechazado'',razonRechazo=CASE WHEN @RazonRechazo <> '''' THEN ''SPID Devolucion'' ELSE '''' END' +
						',estatus=''RECC'' ';
	
		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
			SET @sqlQuery = @sqlQuery + ' ,nopagosdevueltos=1,nopagosprocesados=0 ';


		IF(@CodigoMensaje <> '')
			SET @sqlQuery = @sqlQuery + ',id_CodigoMensaje = @CodigoMensaje ';


        SET @sqlQuery = @sqlQuery + ' WHERE id_formato=@strFolio ';

		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30), @CodigoMensaje ', @Folio = @Folio,@CodigoMensaje=@CodigoMensaje;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO



USE InterfacesT24;
GO
/*

*/
IF OBJECT_ID('dbo.usp_CNT_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_CNT_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_CNT_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
				UPDATE tb_srv_plan_trabajo_maestro 
					  SET cve_estatus='TERMINANDO' 
				WHERE folio=@Folio AND tipo_operacion=@Tipo_operacion 
				AND cve_estatus IN ('AUTORIZACION');
		END
		
		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
					SET cve_estatus='PROCESADA'
					,flg_terminado=1
					,fec_fin=getDate() 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

        SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXD_GPH_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXD_GPH_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXD_GPH_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
				UPDATE tb_srv_plan_trabajo_maestro 
					SET cve_estatus='SPEI' 
				WHERE folio=@Folio  
				AND tipo_operacion=@Tipo_operacion 
				AND cve_estatus IN ('AUTORIZACION','T24')
		END
		
		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPEI');
        
		END

        SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXD_H2H_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXD_H2H_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXD_H2H_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
					SET cve_estatus='SPEI' 
			WHERE folio=@Folio AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24');
		END
		
		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPEI');
        
		END

        SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXD_Local_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXD_Local_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXD_Local_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPEI' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
			AND cve_estatus IN ('AUTORIZACION','T24');

		END
		
		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPEI')
        
		END

        SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXD_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXD_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXD_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPEI' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END
		
		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPEI')
        
		END

        SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_TMD_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TMD_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TMD_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion;

		END
		
		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion;
        
		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')
        
		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_TAX_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TAX_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TAX_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
			       SET cve_estatus='BBVA' 
			WHERE folio=@Folio
			AND tipo_operacion=@Tipo_operacion
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('BBVA');

		END
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_SPID_GPH_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_SPID_GPH_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_SPID_GPH_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPID' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPID')

		END
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_SPID_H2H_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_SPID_H2H_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_SPID_H2H_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPID' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPID')

		END
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_SPID_Local_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_SPID_Local_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_SPID_Local_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPID' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPID')

		END
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO
/*

*/
IF OBJECT_ID('dbo.usp_SPID_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_SPID_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_SPID_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPID' 
			WHERE folio=@Folio
			AND tipo_operacion=@Tipo_operacion
			AND cve_estatus IN ('AUTORIZACION','T24');

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPID');

		END
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_SER_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_SER_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_SER_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='BBVA' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('BBVA')

		END
		

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_FXT_GPH_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_FXT_GPH_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_FXT_GPH_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	SELECT nf_estatus
		   ,nf_razon
		   ,t24_transaction_id
		   , nf_nombre_archivo_gph
		   , nf_pmt_inf_id
		   , nf_id_archivo 
	FROM tb_tombin_nf_fxt_gph 
	WHERE nf_folio=@Folio  
	AND nf_estatus IN ('PROC','RECC')
END

/*

*/
IF OBJECT_ID('dbo.usp_FXT_GPH_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXT_GPH_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXT_GPH_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio  AND tipo_operacion=@Tipo_operacion;

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_FXT_H2H_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_FXT_H2H_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_FXT_H2H_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	SELECT nf_estatus,nf_razon,t24_transaction_id FROM tb_tombin_nf_fxt_h2h WHERE nf_folio = @Folio AND nf_estatus IN ('PROC','RECC')
END;
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXT_H2H_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXT_H2H_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXT_H2H_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio AND tipo_operacion=@Tipo_operacion 

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio AND tipo_operacion=@Tipo_operacion 

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_FXT_Local_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_FXT_Local_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_FXT_Local_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	SELECT nf_estatus,nf_razon 
	FROM tb_tombin_nf_fxt_local 
	WHERE NF_FOLIO=@Folio 
	AND nf_estatus IN ('PROC','RECC')
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXT_Local_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXT_Local_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXT_Local_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio AND tipo_operacion=@Tipo_operacion 

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1
			WHERE folio=@Folio AND tipo_operacion=@Tipo_operacion 

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_FXT_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_FXT_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_FXT_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	SELECT nf_estatus,nf_razon,t24_transaction_id 
	FROM tb_tombin_nf_fxt WHERE nf_folio=@Folio AND nf_estatus IN ('PROC','RECC')
END
GO
/*

*/
IF OBJECT_ID('dbo.usp_MercadoDinero_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_MercadoDinero_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_MercadoDinero_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPEI' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				  SET cve_estatus='SPID' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus=CASE flg_cancelada WHEN 1 THEN 'RECHAZADA' ELSE 'PROCESADA' END
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio AND tipo_operacion=@Tipo_operacion 

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_MFX_ConsultarTombinNFEstatus', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_MFX_ConsultarTombinNFEstatus AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_MFX_ConsultarTombinNFEstatus
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_estatus FROM tb_tombin_nf_mfx WHERE NF_FOLIO = @Folio AND NF_ESTATUS IN ('CONP','CONF','PROC','RECC');
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_MXF_ActualizaTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_MXF_ActualizaTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_MXF_ActualizaTombinNF
(
    @Folio          INT,
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
		
			UPDATE tb_tombin_nf_mfx 
				SET NF_ESTATUS='PROC' 
			WHERE NF_FOLIO=@Folio 

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_tombin_nf_mfx 
				  SET nf_estatus='RECC' 
			WHERE nf_folio=@Folio;

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_MXF_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_MXF_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_MXF_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO'  
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('LECTURA');

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion; 

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion; 

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_TEF_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TEF_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TEF_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24');

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion; 

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion; 

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_TEF_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_TEF_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_TEF_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_estatus,nf_razon,t24_transaction_id, ISNULL(nf_tef_id_master,0) AS nf_tef_id_master, nf_fecha_ejecucion, t24.t24_folio_propuesto, t24_debit_amount
    FROM tb_tombin_nf_tef tef 
	LEFT JOIN tb_srv_t24_mw_control t24 ON tef.nf_folio_unico = t24.folio_unico
    WHERE nf_folio=@Folio 
	AND nf_estatus IN ('PROC','RECC')

END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_TEF_H2H_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_TEF_H2H_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_TEF_H2H_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_estatus, nf_razon, t24_transaction_id,ISNULL(nf_tef_id_master,0) AS nf_tef_id_master, nf_fecha_ejecucion,
           t24.t24_folio_propuesto, T24_debit_amount
    FROM tb_tombin_nf_tef_h2h as nf
    LEFT JOIN tb_srv_t24_mw_control t24 ON nf.nf_folio_unico = t24.folio_unico
    WHERE nf_folio = @Folio  
	AND nf_estatus IN ('PROC','RECC');

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_TEF_H2H_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TEF_H2H_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TEF_H2H_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO'
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24');

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion; 

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				    SET cve_estatus='RECHAZADA'
					,flg_terminado=1
					,fec_fin=getDate()
					,num_estatus_especial=0
					,flg_cancelada=1 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion; 

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_TEF_GPH_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_TEF_GPH_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_TEF_GPH_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_estatus, nf_razon, t24_transaction_id,ISNULL(nf_tef_id_master,0) AS nf_tef_id_master, nf_fecha_ejecucion,
		   t24.t24_folio_propuesto, t24_debit_amount
	FROM tb_tombin_nf_tef_gph as nf
    LEFT JOIN tb_srv_t24_mw_control t24 ON nf.nf_folio_unico = t24.folio_unico
    WHERE nf_folio = @Folio AND nf_estatus IN ('PROC','RECC');

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_TEF_GPH_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_TEF_GPH_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_TEF_GPH_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24');

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion; 

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio  
			AND tipo_operacion=@Tipo_operacion; 

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_SAT_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_SAT_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_SAT_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_estatus, nf_observacion, nf_cadena_original, nf_sello_digital,nf_llave_pago,nf_fecha_ejecucion, nf_tipo
			t24_transaction_id
	FROM tb_tombin_cms_sat 
	WHERE nf_folio = @Folio 
	AND nf_estatus IN ('PROC','RECC');

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_SAT_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_SAT_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_SAT_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
			       SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
                
		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_FXD_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_FXD_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_FXD_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT SUM(nopagosprocesados) AS procesados, SUM(nopagosdevueltos) AS devueltos, COUNT(*) as regs_totales
	FROM tb_tombin_nf_fxd_pap_detalle
    WHERE id_formato_pap = @Folio;

END
GO
/*

*/
IF OBJECT_ID('dbo.usp_PaP_FXD_ActualizaFormatoTomi', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXD_ActualizaFormatoTomi AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXD_ActualizaFormatoTomi
(
    @Folio          INT,
	@Procesadas		INT,
	@Devueltas		INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE tb_tombin_nf_fxd_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
				   ,nf_estatus='PROC' 
			WHERE nf_folio=@Folio

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_tombin_nf_fxd_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas 
			WHERE nf_folio=@Folio
		
		END


		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_FXD_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXD_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXD_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPEI' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion  
			AND cve_estatus IN ('SPEI');

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate() 
			WHERE folio=0 
			AND folio_maestro=@Folio 
			AND tipo_operacion=@Tipo_operacion;
                
		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_SPID_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_SPID_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_SPID_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT SUM(nopagosprocesados) AS procesados, SUM(nopagosdevueltos) AS devueltos, COUNT(*) as regs_totales
	FROM tb_tombin_nf_spid_pap_detalle
    WHERE id_formato_pap = @Folio;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_SPID_ActualizaFormatoTomi', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_SPID_ActualizaFormatoTomi AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_SPID_ActualizaFormatoTomi
(
    @Folio          INT,
	@Procesadas		INT,
	@Devueltas		INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE tb_tombin_nf_spid_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
				   ,nf_estatus='PROC' 
			WHERE nf_folio=@Folio

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_tombin_nf_spid_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas 
			WHERE nf_folio=@Folio
		
		END


		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO
/*

*/
IF OBJECT_ID('dbo.usp_PaP_SPID_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_SPID_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_SPID_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
					SET cve_estatus='SPID' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPID')

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate() 
			WHERE folio=0 
			AND folio_maestro=@Folio 
			AND tipo_operacion=@Tipo_operacion;
                
		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_NOM_FXD_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_NOM_FXD_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_NOM_FXD_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT SUM(nopagosprocesados) AS procesados, SUM(nopagosdevueltos) AS devueltos, COUNT(*) as regs_totales
	FROM tb_tombin_nf_fxd_nomina_detalle
    WHERE id_formato_nomina = @Folio;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_NOM_FXD_ActualizaFormatoTomi', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_NOM_FXD_ActualizaFormatoTomi AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_NOM_FXD_ActualizaFormatoTomi
(
    @Folio          INT,
	@Procesadas		INT,
	@Devueltas		INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE tb_tombin_nf_fxd_nomina 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
				   ,nf_estatus='PROC' 
			WHERE nf_folio=@Folio

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_tombin_nf_fxd_nomina 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas 
			WHERE nf_folio=@Folio
		
		END


		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;
/*

*/
IF OBJECT_ID('dbo.usp_NOM_FXD_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_NOM_FXD_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_NOM_FXD_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
					SET cve_estatus='SPEI' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='SPEI' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION')

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO'
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion  
			AND cve_estatus IN ('SPEI')
                
		END

		IF(@Opcion = 4) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=0 
			AND folio_maestro=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('SPEI')

		END

		IF(@Opcion = 5) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate() 
			WHERE folio=0 AND folio_maestro=@Folio 
			AND tipo_operacion=@Tipo_operacion 
		
		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_FXT_ConsultarTombinNFEstatus', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_FXT_ConsultarTombinNFEstatus AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_FXT_ConsultarTombinNFEstatus
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_estatus,nf_razon,nf_fecha_autoriza,0 as nf_comision_cuenta,0 as nf_comision_moneda,
			0 as nf_comision_monto, 0 as nf_iva_monto, nf_tipo, t24_transaction_id 
	FROM tb_tombin_nf_fxt_pap_detalle 
	WHERE nf_folio=@Folio AND nf_estatus IN ('PROC','RECC')

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_FXD_ActualizaTombinDetalle', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXD_ActualizaTombinDetalle AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXD_ActualizaTombinDetalle
(
    @Folio          INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE tb_tombin_nf_fxt_pap_detalle 
					SET nopagosprocesados=1 
			WHERE nf_folio=@Folio;

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_tombin_nf_fxt_pap_detalle 
				   SET nopagosdevueltos=1 
			WHERE nf_folio=@Folio;
		
		END


		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
Go

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_FXT_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_FXT_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_FXT_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT SUM(nopagosprocesados) AS procesados,SUM(nopagosdevueltos) AS devueltos,COUNT(*) AS regs_totales
    FROM tb_tombin_nf_fxt_pap_detalle 
    WHERE id_formato_pap=@Folio

END
GO

/*

*/

IF OBJECT_ID('dbo.usp_PaP_FXT_ActualizaFormatoTomi', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXT_ActualizaFormatoTomi AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXT_ActualizaFormatoTomi
(
    @Folio          INT,
	@Procesadas		INT,
	@Devueltas		INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY

		IF(@Opcion = 1) BEGIN

			UPDATE tb_tombin_nf_fxt_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
				   ,nf_estatus='PROC' 
			WHERE nf_folio=@Folio;

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_tombin_nf_fxt_pap 
				   SET nopagosprocesados=@Procesadas
				   ,nopagosdevueltos=@Devueltas
			WHERE nf_folio=@Folio;
		
		END


		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
Go
IF OBJECT_ID('dbo.usp_PaP_FXT_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXT_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXT_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END
		
		IF(@Opcion = 3) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
			       SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
                
		END

		IF(@Opcion = 4) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate() 
			WHERE folio=@Folio					--FolioMaestro
			AND tipo_operacion=@Tipo_operacion 

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_ENT_COAS_ConsultarBmxPagDia', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_ENT_COAS_ConsultarBmxPagDia AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_ENT_COAS_ConsultarBmxPagDia
(
    @FechaOperacion         NUMERIC
)
AS
BEGIN
	
	SELECT 'PROC', op_clave 
    FROM bmx_ord_pagdia_r WHERE op_fecha_oper = @FechaOperacion AND op_estado = 'AC'

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ENT_COAS_ActualizaBmxPagDia', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ENT_COAS_ActualizaBmxPagDia AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ENT_COAS_ActualizaBmxPagDia
(
    @Folio          INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;
			
			UPDATE bmx_ord_pagdia_r 
                   SET op_estado = 'LQ', 
                   op_hora_liq_bm=CONVERT(INT,FORMAT(GETDATE(),'yyyyMMddhhmmss')),
                   op_hora_liq_sist=CONVERT(INT,FORMAT(GETDATE(),'yyyyMMddhhmmss'))
            WHERE op_clave = @Folio;


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ENT_COAS_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ENT_COAS_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ENT_COAS_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO'
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION')

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_ENT_SPEI_ConsultarTombinEntradasSPEI', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_ENT_SPEI_ConsultarTombinEntradasSPEI AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_ENT_SPEI_ConsultarTombinEntradasSPEI
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT op_clave, op_fecha_oper, tp_clave FROM tb_tombin_nf_entradas_spei 
	WHERE op_clave=@Folio

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ENT_SPEI2_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ENT_SPEI2_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ENT_SPEI2_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='CEP'  
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION','T24')

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1,fec_fin=getDate()  
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_ENT_SPID_ConsultarSubTipoPlanMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_ENT_SPID_ConsultarSubTipoPlanMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_ENT_SPID_ConsultarSubTipoPlanMaestro
(
    @Folio          INT,
	@Tipo_operacion VARCHAR(10)
)
AS
BEGIN
	
	SELECT ISNULL(subtipo_operacion,'') AS subtipo_operacion 
	FROM tb_srv_plan_trabajo_maestro 
	WHERE folio=@Folio And tipo_operacion = @Tipo_operacion;

END
GO

IF OBJECT_ID('dbo.usp_ENT_SPID_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ENT_SPID_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
IF OBJECT_ID('dbo.ssp_ENT_SPID_ConsultarOperacionDevuelta', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_ENT_SPID_ConsultarOperacionDevuelta AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_ENT_SPID_ConsultarOperacionDevuelta
(
    @op_cve_rastreo      VARCHAR(30),
    @op_monto            NUMERIC(18,2),
    @op_fecha_ejecucion  NUMERIC(8,0)
)
AS
BEGIN

    SELECT NF_FOLIO AS nf_folio, Canal AS canal
    FROM
    (
        SELECT NF_FOLIO, 'SPID' AS Canal
        FROM TB_TOMBIN_NF_SPID WITH (NOLOCK)
        WHERE T24_TRANSACTION_ID = @op_cve_rastreo
          AND NF_MONTO_ENVIAR = @op_monto
          AND NF_ESTATUS = 'PROC'
          AND NF_FECHA_EJECUCION = @op_fecha_ejecucion

        UNION

        SELECT NF_FOLIO, 'SPID_GPH' AS Canal
        FROM TB_TOMBIN_NF_SPID_GPH WITH (NOLOCK)
        WHERE T24_TRANSACTION_ID = @op_cve_rastreo
          AND NF_MONTO_ENVIAR = @op_monto
          AND NF_ESTATUS = 'PROC'
          AND NF_FECHA_EJECUCION = @op_fecha_ejecucion

        UNION

        SELECT NF_FOLIO, 'SPID_H2H' AS Canal
        FROM TB_TOMBIN_NF_SPID_H2H WITH (NOLOCK)
        WHERE T24_TRANSACTION_ID = @op_cve_rastreo
          AND NF_MONTO_ENVIAR = @op_monto
          AND NF_ESTATUS = 'PROC'
          AND NF_FECHA_EJECUCION = @op_fecha_ejecucion

        UNION

        SELECT NF_FOLIO, 'SPID_Local' AS Canal
        FROM TB_TOMBIN_NF_SPID_LOCAL WITH (NOLOCK)
        WHERE T24_TRANSACTION_ID = @op_cve_rastreo
          AND NF_MONTO_ENVIAR = @op_monto
          AND NF_ESTATUS = 'PROC'
          AND NF_FECHA_EJECUCION = @op_fecha_ejecucion

        UNION

        SELECT NF_FOLIO, 'PaP_SPID' AS Canal
        FROM TB_TOMBIN_NF_SPID_PAP_DETALLE WITH (NOLOCK)
        WHERE T24_TRANSACTION_ID = @op_cve_rastreo
          AND NF_MONTO_ENVIAR = @op_monto
          AND NF_ESTATUS = 'PROC'
          AND NF_FECHA_EJECUCION = @op_fecha_ejecucion
    ) AS ORIGENES;

END
GO
ALTER PROCEDURE dbo.usp_ENT_SPID_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='CEP'
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion;

			UPDATE tb_srv_plan_trabajo_detalle 
				   SET flg_terminado=1
				   , flg_con_error = 0 
				   ,fec_inicio=getDate()
				   , fec_fin=getDate() 
			WHERE folio=@Folio  
			AND orden_ejecucion in(17,18) 
			AND tipo_operacion=@Tipo_operacion;

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_ENT_TEF_ConsultarTombinEntradasTEF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_ENT_TEF_ConsultarTombinEntradasTEF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_ENT_TEF_ConsultarTombinEntradasTEF
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT idoperacion, fechaproceso, tipooperacion FROM tb_tombin_nf_entradas_TEF Where idoperacion = @Folio

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ENT_TEF_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ENT_TEF_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ENT_TEF_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1,fec_fin=getDate()
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion;

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_FXI_GPH_ConsultarTombin', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_FXI_GPH_ConsultarTombin AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_FXI_GPH_ConsultarTombin
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_tipo, nf_estatus, nf_razon, nf_fecha_autoriza, t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda,
		   0 as nf_comicion_monto, 0 as nf_iva_monto, nf_tipo, t24_transaction_id, nf_nombre_archivo_gph, nf_pmt_inf_id, nf_id_archivo
    FROM tb_tombin_nf_fxi_gph
    WHERE nf_folio = @Folio;

END
GO
/*

*/
IF OBJECT_ID('dbo.usp_FXI_GPH_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_GPH_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_GPH_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET flg_con_error=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND flg_con_error=1

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='ESPERANDO SWIFT AUT' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 4) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 5) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_FXI_H2H_ConsultarTombin', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_FXI_H2H_ConsultarTombin AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_FXI_H2H_ConsultarTombin
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_tipo,nf_estatus,nf_razon,nf_fecha_autoriza,t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda,
		   0 AS nf_comision_monto,0 AS nf_iva_monto,nf_tipo,t24_transaction_id
    FROM TB_TOMBIN_NF_FXI_H2H
    WHERE nf_folio = @Folio;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXI_H2H_ActualizaTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_H2H_ActualizaTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_H2H_ActualizaTombinNF
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

			UPDATE tb_tombin_nf_fxi_h2h 
				   SET nf_estatus='PROC' 
			WHERE NF_FOLIO=@Folio;


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXI_GPH_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_GPH_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_GPH_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET flg_con_error=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND flg_con_error=1

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='ESPERANDO SWIFT AUT' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 4) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 5) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_FXI_Local_ConsultarTombin', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_FXI_Local_ConsultarTombin AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_FXI_Local_ConsultarTombin
(
    @Folio          INT
)
AS
BEGIN
	
	 SELECT nf_tipo, nf_estatus, nf_razon, nf_fecha_autoriza, t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda,
            0 as nf_comicion_monto, 0 as nf_iva_monto, nf_tipo, t24_transaction_id
     FROM tb_tombin_nf_fxi_local
     WHERE nf_folio = @Folio;

END
GO

/*
*/
IF OBJECT_ID('dbo.usp_FXI_Local_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_Local_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_Local_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET flg_con_error=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND flg_con_error=1

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='ESPERANDO SWIFT AUT' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 4) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 5) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXI_Local_ActualizaTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_Local_ActualizaTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_Local_ActualizaTombinNF
(
    @Folio          INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

			UPDATE tb_tombin_nf_fxi_local 
				   SET nf_estatus='PROC' 
			WHERE NF_FOLIO=@Folio;


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_FXI_ConsultarTombin', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_FXI_ConsultarTombin AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_FXI_ConsultarTombin
(
    @Folio          INT
)
AS
BEGIN
	
	 SELECT nf_tipo,nf_estatus,nf_razon,nf_fecha_autoriza,t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda,
		    0 AS nf_comision_monto,0 AS nf_iva_monto,nf_tipo,t24_transaction_id 
	 FROM tb_tombin_nf_fxi
     WHERE nf_folio = @Folio;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXI_Local_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_Local_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_Local_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET flg_con_error=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND flg_con_error=1

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='ESPERANDO SWIFT AUT' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 4) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 5) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_FXI_ActualizaTombinDetalle', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_FXI_ActualizaTombinDetalle AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_FXI_ActualizaTombinDetalle
(
    @Folio          INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY


			UPDATE tb_tombin_nf_fxi 
			       SET nf_estatus='PROC' 
			WHERE nf_folio=@Folio;



		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END;

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_FXI_ConsultarTombinNF', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_FXI_ConsultarTombinNF AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_FXI_ConsultarTombinNF
(
    @Folio          INT
)
AS
BEGIN
	
	 SELECT nf_tipo,nf_estatus,nf_razon,nf_fecha_autoriza,t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda,
			0 AS nf_comision_monto,0 AS nf_iva_monto,nf_tipo,t24_transaction_id 
	 FROM tb_tombin_nf_fxi_pap_detalle
     WHERE nf_folio = @Folio;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_FXI_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXI_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXI_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET flg_con_error=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion AND flg_con_error=1;

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='ESPERANDO SWIFT AUT' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('AUTORIZACION')

		END

		IF(@Opcion = 4) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 5) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 6) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate() 
			WHERE folio=0 
			AND folio_maestro=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END


		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_FXI_ConsultarTombinDetalle', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_FXI_ConsultarTombinDetalle AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_FXI_ConsultarTombinDetalle
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_estatus,nf_razon,nf_fecha_autoriza, 0 as nf_comision_cuenta,0 as nf_comision_moneda,0 as nf_comision_monto,0 as nf_iva_monto, nf_tipo 
	FROM tb_tombin_nf_fxi_pap_detalle 
	WHERE nf_folio=@Folio  
	AND nf_estatus IN ('PROC','RECC')

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_FXI_Local_ActualizaTombinNFDetalle', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXI_Local_ActualizaTombinNFDetalle AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXI_Local_ActualizaTombinNFDetalle
(
    @Folio          INT,
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN

			UPDATE TB_TOMBIN_NF_FXI_PAP_DETALLE 
				   SET nopagosprocesados=1 
			WHERE nf_folio=@Folio 

		END

		IF(@Opcion = 2) BEGIN

			UPDATE TB_TOMBIN_NF_FXI_PAP_DETALLE 
				   SET nopagosdevueltos=1 
			WHERE nf_folio=@Folio 

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_FXI_ConsultarTombinNF_DetalleMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_FXI_ConsultarTombinNF_DetalleMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_FXI_ConsultarTombinNF_DetalleMaestro
(
    @Folio          INT
)
AS
BEGIN
	
	 SELECT SUM(nopagosprocesados) AS procesados,SUM(nopagosdevueltos) AS devueltos,COUNT(*) AS regs_totales 
     FROM tb_tombin_nf_fxi_pap_detalle 
     WHERE id_formato_pap=@Folio

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_FXI_ActualizaTombinNFDetalleMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_FXI_ActualizaTombinNFDetalleMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_FXI_ActualizaTombinNFDetalleMaestro
(
    @Folio          INT,
	@Procesados		INT,
    @Devueltos		INT,
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN

			UPDATE tb_tombin_nf_fxi_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos
				   ,nf_estatus='PROC' 
			WHERE nf_folio=@Folio;

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_tombin_nf_fxi_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos 
			WHERE nf_folio=@Folio;

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_TEF_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_TEF_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_TEF_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			

			UPDATE tb_srv_plan_trabajo_maestro SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion

		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 4) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
			WHERE folio=0 AND folio_maestro=@Folio 
			AND tipo_operacion=@Tipo_operacion

		END



		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_TEF_ConsultarTombinDetalle', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_TEF_ConsultarTombinDetalle AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_TEF_ConsultarTombinDetalle
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT nf_estatus,nf_razon,nf_fecha_autoriza,0 as nf_comision_cuenta,0 as nf_comision_moneda,0 as nf_comision_monto,0 as nf_iva_monto, nf_tipo, t24_transaction_id, 
           ISNULL(nf_tef_id_master,0) AS nf_tef_id_master, nf_fecha_ejecucion, t24.t24_folio_propuesto, t24_debit_amount 
	FROM tb_tombin_nf_tef_pap_detalle pap 
    LEFT JOIN tb_srv_t24_mw_control t24 ON pap.nf_folio_unico = t24.folio_unico 
	WHERE nf_folio=@Folio 
	AND nf_estatus IN ('PROC','RECC')

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_TEF_ActualizaTombinDetalle', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_TEF_ActualizaTombinDetalle AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_TEF_ActualizaTombinDetalle
(
    @Folio          INT,
	@Opcion			INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY


		IF(@Opcion = 1) BEGIN
			
			UPDATE tb_tombin_nf_tef_pap_detalle 
				   SET nopagosprocesados=1 
			WHERE nf_folio=@Folio

		END
		
		IF(@Opcion = 2) BEGIN
		
			UPDATE tb_tombin_nf_tef_pap_detalle 
				   SET nopagosdevueltos=1 
			WHERE nf_folio=@Folio;
		
		END

		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_PaP_TEF_ConsultarTombinMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_PaP_TEF_ConsultarTombinMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_PaP_TEF_ConsultarTombinMaestro
(
    @Folio          INT
)
AS
BEGIN
	
	SELECT SUM(nopagosprocesados) AS procesados,SUM(nopagosdevueltos) AS devueltos,COUNT(*) AS regs_totales 
    FROM tb_tombin_nf_tef_pap_detalle 
    WHERE id_formato_pap=@Folio

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_TEF_ActualizaTombinNFDetalleMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_TEF_ActualizaTombinNFDetalleMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_TEF_ActualizaTombinNFDetalleMaestro
(
    @Folio          INT,
	@Procesados		INT,
    @Devueltos		INT,
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN

			UPDATE tb_tombin_nf_tef_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos
				   ,nf_estatus='PROC' 
			WHERE nf_folio=@Folio;

		END

		IF(@Opcion = 2) BEGIN

			UPDATE tb_tombin_nf_tef_pap 
				   SET nopagosprocesados=@Procesados
				   ,nopagosdevueltos=@Devueltos 
			WHERE nf_folio=@Folio;

		END

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.usp_PaP_TEF_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_PaP_TEF_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_PaP_TEF_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
			

			UPDATE tb_srv_plan_trabajo_maestro SET cve_estatus='TERMINANDO' 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 
			AND cve_estatus IN ('T24')

		END

		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion

		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion 

		END

		IF(@Opcion = 4) BEGIN

			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
			WHERE folio=0 AND folio_maestro=@Folio 
			AND tipo_operacion=@Tipo_operacion

		END



		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_ConsultaEstatusSpei', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_ConsultaEstatusSpei AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_ConsultaEstatusSpei
(
    @TablaTomi VARCHAR(100),
	@CampoTomi VARCHAR(100),
	@FolioTomi VARCHAR(30)
)
AS
BEGIN

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'SELECT nf_estatus, nf_opclave_spei,nf_opfechaoper_spei,nf_opfolio_spei, t24.T24_folio_asignado, nf_fecha_ejecucion ' +
						' FROM ' + @TablaTomi + ' AS tomi WITH(NOLOCK) ' +
						' LEFT JOIN tb_srv_t24_mw_Control t24 WITH(NOLOCK) ON tomi.nf_folio_unico = t24.folio_unico ' +
						' WHERE ' + @CampoTomi + ' = @FolioTomi' +
						' AND nf_estatus IN (''RECC'',''PROC'',''CONP'',''CONF'') AND ISNULL(nf_interfase_spei,1) = 1';

		EXEC sp_executesql @sqlQuery, N'@FolioTomi VARCHAR(30)', @FolioTomi = @FolioTomi;


END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ActualizaEstatusRechazadoSpei', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ActualizaEstatusRechazadoSpei AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ActualizaEstatusRechazadoSpei
(
    @TablaTomi VARCHAR(100),
	@CampoTomi VARCHAR(100),
	@FolioTomi VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE '+ @TablaTomi + ' SET nf_estatus_interno = '''', nf_interface_spei=3 ';

		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
            SET @sqlQuery = @sqlQuery + ',nopagosdevueltos=1,nopagosprocesados=0 ';

        SET @sqlQuery = @sqlQuery + ' WHERE ' + @CampoTomi + ' = @FolioTomi ';

		EXEC sp_executesql @sqlQuery, N'@FolioTomi VARCHAR(30)', @FolioTomi = @FolioTomi;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ActualizaEstatuInternoLQSpei', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ActualizaEstatuInternoLQSpei AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ActualizaEstatuInternoLQSpei
(
    @TablaTomi VARCHAR(100),
	@CampoTomi VARCHAR(100),
	@FolioTomi VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE '+ @TablaTomi + ' SET nf_estatus_interno=''LQ'', nf_interface_spei=3 ';

		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
            SET @sqlQuery = @sqlQuery + ',nopagosdevueltos=0,nopagosprocesados=1 ';

        SET @sqlQuery = @sqlQuery + ' WHERE ' + @CampoTomi + ' = @FolioTomi ';

		EXEC sp_executesql @sqlQuery, N'@FolioTomi VARCHAR(30)', @FolioTomi = @FolioTomi;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ActualizaEstatuInternoDSpei', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ActualizaEstatuInternoDSpei AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ActualizaEstatuInternoDSpei
(
    @TablaTomi VARCHAR(100),
	@CampoTomi VARCHAR(100),
	@FolioTomi VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE '+ @TablaTomi + ' SET nf_estatus_interno=''D'', nf_interface_spei=3 ';

		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
            SET @sqlQuery = @sqlQuery + ',nopagosdevueltos=1,nopagosprocesados=0 ';

        SET @sqlQuery = @sqlQuery + ' WHERE ' + @CampoTomi + ' = @FolioTomi ';

		EXEC sp_executesql @sqlQuery, N'@FolioTomi VARCHAR(30)', @FolioTomi = @FolioTomi;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ActualizaEstatuInternoSpei', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ActualizaEstatuInternoSpei AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ActualizaEstatuInternoSpei
(
    @TablaTomi VARCHAR(100),
	@CampoTomi VARCHAR(100),
	@FolioTomi VARCHAR(30),
	@EstatusInterno VARCHAR(10),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE '+ @TablaTomi + ' SET nf_estatus_interno= '+  @EstatusInterno + ', nf_interface_spei=3 ';

		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
            SET @sqlQuery = @sqlQuery + ',nopagosdevueltos=1,nopagosprocesados=0 ';

        SET @sqlQuery = @sqlQuery + ' WHERE ' + @CampoTomi + ' = @FolioTomi ';

		EXEC sp_executesql @sqlQuery, N'@FolioTomi VARCHAR(30)', @FolioTomi = @FolioTomi;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_Consultar_OrigenSPEI_CMS_GPH', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_OrigenSPEI_CMS_GPH AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_OrigenSPEI_CMS_GPH
(
	@TablaTomi		VARCHAR(30),
    @Folio          INT
)
AS
BEGIN
	
	DECLARE @SqlQuery NVARCHAR(Max) = '';

	SET @SqlQuery = 'SELECT nf_estatus ' +
	   ' ,nf_razon ' +
	   ' ,nf_fecha_autoriza,t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda, ' +
	   ' 0 AS nf_comision_monto,0 AS nf_iva_monto,nf_estatus_interno,t24_transaction_id, nf_nombre_archivo_gph, ' +
	   ' nf_pmt_inf_id, nf_id_archivo ' +
	   ' FROM ' + @TablaTomi + 
	   ' WHERE nf_folio= @strFolio ' +
	   ' AND ( nf_estatus=''RECC''OR (nf_estatus=''PROC'' AND nf_tipo<>''FXD'') ' +
	   '       OR (nf_estatus=''PROC'' AND nf_tipo=''FXD'' AND nf_estatus_interno IN (''LQ'',''D'',''CN'',''CL'',''RB'')) )	';

	EXEC sp_executesql @sqlQuery, N'@Folio INT', @Folio = @Folio;

END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_Consultar_OrigenSPEI_CMS_FXD', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_OrigenSPEI_CMS_FXD AS BEGIN SET NOCOUNT ON; END')
END;
GO
CREATE OR ALTER PROCEDURE dbo.ssp_Consultar_OrigenSPEI_CMS_FXD  
(  
 @tablatomi  VARCHAR(30),  
    @folio          INT  
)  
AS  
BEGIN  
   
 DECLARE @sqlquery NVARCHAR(Max) = '';  
  
 SET @sqlquery = 'SELECT nf_estatus ' +  
    ' ,nf_razon ' +  
    ' ,nf_fecha_autoriza,t24_charges_acct_no AS nf_comision_cuenta,t24_debit_currency AS nf_comision_moneda, ' +  
    ' 0 AS nf_comision_monto,0 AS nf_iva_monto,nf_estatus_interno,t24_transaction_id ' +  
    ' FROM ' + @tablatomi +   
    ' WHERE nf_folio= @folio ' +  
    ' AND ( nf_estatus=''RECC''OR (nf_estatus=''PROC'' AND nf_tipo<>''FXD'') ' +  
    '       OR (nf_estatus=''PROC'' AND nf_tipo=''FXD'' AND nf_estatus_interno IN (''LQ'',''D'',''CN'',''CL'',''RB'')) ) ';  
  PRINT @sqlquery;
 EXEC sp_executesql @sqlquery, N'@folio INT', @folio = @folio;  
  
END  
GO

/*

*/
IF OBJECT_ID('dbo.usp_Comunes_ActualizaPlanTrabajoMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaPlanTrabajoMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaPlanTrabajoMaestro
(
    @Folio          INT,
    @Tipo_operacion VARCHAR(10),
	@Opcion			INT,
	@Desc_mensaje	VARCHAR(75) = '',
    @RowsAffected   INT OUTPUT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
    
        BEGIN TRANSACTION;

		IF(@Opcion = 1) BEGIN
        
				UPDATE tb_srv_plan_trabajo_maestro 
					   SET cve_estatus='PROCESADA'
					   ,flg_terminado=1
					   ,fec_fin=getDate()
					   ,num_estatus_especial=0 
				WHERE folio=@Folio 
				AND tipo_operacion=@Tipo_operacion;
		END
		
		IF(@Opcion = 2) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='DEVUELTA - BBVA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 3) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 4) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 5) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='DEVUELTA - SPEI'
				   ,err_descripcion = 'Operaci�n devuelta con motivo: ' + @Desc_mensaje
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 6) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='DEVUELTA - SPEI'
				   ,err_descripcion = 'Operaci�n devuelta'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 7) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET flg_terminado=1
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 8) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA - SPEI'
				   ,err_descripcion = 'Operaci�n rechazada c�digo: ' + @Desc_mensaje
				   , flg_terminado=1,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 9) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 10) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='PROCESADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 11) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='DEVUELTA - SPID'
				   ,err_descripcion = 'Operaci�n devuelta con motivo: ' + @Desc_mensaje
				   ,flg_terminado=1,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 12) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='DEVUELTA - SPID'
				   ,err_descripcion = 'Operaci�n devuelta'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 13) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET flg_terminado=1
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 14) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA - SPID'
				   ,err_descripcion = 'Operaci�n rechazada c�digo: ' + @Desc_mensaje
				   , flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END

		IF(@Opcion = 15) BEGIN
			
			UPDATE tb_srv_plan_trabajo_maestro 
				   SET cve_estatus='RECHAZADA'
				   ,flg_terminado=1
				   ,fec_fin=getDate()
				   ,num_estatus_especial=0
				   ,flg_cancelada=1 
			WHERE folio=@Folio 
			AND tipo_operacion=@Tipo_operacion
        
		END


        SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;
END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_Consultar_Cat_Razon_Rechazo', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_Cat_Razon_Rechazo AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_Cat_Razon_Rechazo
(
    @Clave          NUMERIC(5,2),
	@TipoPago		VARCHAR(15)
)
AS
BEGIN
	
	select codigo, descripcion from TB_SRV_CAT_RAZON_REC where cd_clave = @Clave and tipo_pago = @TipoPago;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_Comunes_ActualizaFormatoCms_SPEI_Rechazado_TOMI', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaFormatoCms_SPEI_Rechazado_TOMI AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaFormatoCms_SPEI_Rechazado_TOMI
(
	@TablaTomi		VARCHAR(100),
	@Folio			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';

        SET @sqlQuery = 'UPDATE ' + @TablaTomi + '  SET nf_razon = ''SPEI Devolucion'' ';
	
		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
			SET @sqlQuery = @sqlQuery + ' ,nopagosdevueltos=1,nopagosprocesados=0 ';


        SET @sqlQuery = @sqlQuery + 'nf_estatus = ''RECC'', nf_mensaje = ''Formato rechazado'' ' +
		                ' WHERE nf_folio=@Folio ';

		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.ssp_Consultar_Estatus_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_Estatus_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_Estatus_BBVA
(
    @TablaTomi          VARCHAR(100),
	@CampoTomi			VARCHAR(100),
	@Folio			VARCHAR(30)
)
AS
BEGIN
	
	DECLARE @SqlQuery NVARCHAR(MAX) = '';
	
    SET @SqlQuery = 'SELECT nf_estatus, nf_ciguide_bbva, t24.t24_folio_asignado, nf_fecha_ejecucion ' +
					' FROM ' + @TablaTomi + 'AS tomi WITH(NOLOCK) ' +
					'LEFT JOIN tb_srv_t24_mw_control as t24 WITH(NOLOCK) ON tomi.nf_folio_unico = t24.folio_unico ' +
					' WHERE ' + @CampoTomi  +  '= @Folio ' +
					' AND nf_estatus IN (''RECC'',''PROC'',''CONP'',''CONF'') AND ISNULL(nf_interface_bbca,1) = 1; ';

	EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;
                 

END
GO

IF OBJECT_ID('dbo.usp_Comunes_ActualizaTombin_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaTombin_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaTombin_BBVA
(
	@TablaTomi		VARCHAR(100),
	@Folio			VARCHAR(30),
	@Estatus		VARCHAR(30) = '',
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';

        SET @sqlQuery = 'UPDATE ' + @TablaTomi  + 'SET nf_estatus_interno = ' + @Estatus + ', nf_interface_bbva=3 WHERE @strCampoTomi = @Folio  ';

		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO


IF OBJECT_ID('dbo.ssp_Consultar_EstatusPago_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_EstatusPago_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_EstatusPago_BBVA
(
	@Folio			VARCHAR(30)
)
AS
BEGIN
	
	SELECT Stage, Payment_Stage from tbl_requests_bbva_pt_ps 
	WHERE Id_Format = @Folio;                 

END;

IF OBJECT_ID('dbo.usp_Comunes_ActualizaEstatusTombin_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaEstatusTombin_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaEstatusTombin_BBVA
(
	@TablaTomi		VARCHAR(100),
	@CampoTomi		VARCHAR(100),
	@Folio			VARCHAR(30),
	@Estatus		VARCHAR(30) = '',
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';

        SET @sqlQuery = 'UPDATE ' + @TablaTomi + 'SET nf_estatus_interno = ' + @Estatus + ', nf_interface_bbva=3 ' +
						' WHERE ' + @CampoTomi +' = @Folio '

		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO


IF OBJECT_ID('dbo.ssp_Consultar_Errores_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_Errores_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_Errores_BBVA
(
)
AS
BEGIN
	
	SELECT Id_Error, Error_Message_generic FROM tbl_catalogo_errores bbva WHERE Error_Code = 'DEFAULT' AND Status = 'ACTIVE'             

END
GO

IF OBJECT_ID('dbo.ssp_Consultar_TombinStatus_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_TombinStatus_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_TombinStatus_BBVA
(
	@Folio			VARCHAR(30),
	@TablaTomi		VARCHAR(30)
)
AS
BEGIN

	DECLARE @sqlQuery NVARCHAR(MAX) = '';
	
	SET @sqlQuery = ' SELECT nf_estatus, nf_razon, nf_id_razon,nf_estatus_interno,nf_cieguide_bbva,nf_fecha_pago_bbva ' +
					' FROM ' + @TablaTomi +
					' WHERE nf_folio=@strFolio ' +
					' AND (nf_estatus=''RECC'' OR (nf_estatus=''PROC'' AND nf_estatus_interno IN (''LQ'',''D''))) ';

    EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

END
GO

IF OBJECT_ID('dbo.usp_Comunes_ActualizaFormatoRechazadoTomi_BBVA', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaFormatoRechazadoTomi_BBVA AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaFormatoRechazadoTomi_BBVA
(
	@TablaTomi		VARCHAR(100),
    @Folio          INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY


		DECLARE @SqlQuery NVARCHAR(Max) = '';

		SET @SqlQuery = 'UPDATE ' + @TablaTomi + ' SET Message = ''Formato Rechazado'',nf_razon = @strRazonRechazo, nf_id_razon = @strIdRazon, nf_estatus=''RECC'' ' +
					 
                        ' WHERE nf_folio = @Folio';
		
		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30) ',@Folio = @Folio;


		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

IF OBJECT_ID('dbo.ssp_Consultar_TombinStatus_SPID', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.ssp_Consultar_TombinStatus_SPID AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.ssp_Consultar_TombinStatus_SPID
(
	@Folio			VARCHAR(30),
	@TablaTomi		VARCHAR(30),
	@CampoTomi		VARCHAR(30)
)
AS
BEGIN

	DECLARE @sqlQuery NVARCHAR(MAX) = '';
	
	SET @sqlQuery = ' SELECT nf_estatus,nf_opclave_spei,nf_opfechaoper_spei,nf_opfolio_spei, B.T24_folio_asignado, nf_fecha_ejecucion  ' +
					' FROM ' + @TablaTomi + ' A WITH(NOLOCK) ' +
					' LEFT JOIN TB_SRV_T24_MW_Control B WITH(NOLOCK) ON A.nf_folio_unico = B.folio_unico ' +
					' WHERE ' + @CampoTomi + ' = @Folio AND nf_estatus IN (''RECC'',''PROC'') AND ISNULL(nf_interfase_spei,1)=1 ';

	EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

END
GO

IF OBJECT_ID('dbo.usp_Comunes_ActualizaEstatusInterno_SPID', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaEstatusInterno_SPID AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaEstatusInterno_SPID
(
	@TablaTomi		VARCHAR(100),
	@CampoTomi		VARCHAR(100),
	@EtatusInterno	VARCHAR(30),
	@CdClave		VARCHAR(30),
    @Folio          INT,
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRY
		
		DECLARE @SqlQuery NVARCHAR(Max) = '';

		SET @SqlQuery = 'UPDATE ' + @TablaTomi + ' SET nf_estatus_interno= CASE WHEN @EtatusInterno <> '' THEN @EtatusInterno ELSE '' END,nf_interfase_spei=3 ';
		
		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
			SET @SqlQuery = @SqlQuery + ' ,nopagosdevueltos=1,nopagosprocesados=0 ';

		IF(@CdClave <> '')
			SET @SqlQuery = @SqlQuery + ' ,CD_CLAVE=' + @CdClave;
        
		SET @SqlQuery = @SqlQuery + ' WHERE ' + @CampoTomi + '= @Folio';
		
		EXEC sp_executesql @sqlQuery, N'@EtatusInterno	VARCHAR(30), @Folio VARCHAR(30) ',@EtatusInterno=@EtatusInterno,@Folio = @Folio;


		SET @RowsAffected = @@ROWCOUNT;

		COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO


IF OBJECT_ID('dbo.usp_Comunes_ActualizaFormatoTomi_SPID_Rechazado', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_Comunes_ActualizaFormatoTomi_SPID_Rechazado AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_Comunes_ActualizaFormatoTomi_SPID_Rechazado
(
	@TablaTomi		VARCHAR(100),
	@Folio			VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;


		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE ' + @TablaTomi + 'SET nf_razon = ''SPID Devolucion'', nf_estatus = ''RECC'', nf_mensaje = ''Formato rechazado'' ';
	
		IF(CHARINDEX(@TablaTomi,'detalle') > 0)
			SET @sqlQuery = @sqlQuery + ' ,nopagosdevueltos=1,nopagosprocesados=0 ';


        SET @sqlQuery = @sqlQuery + ' WHERE nf_folio=@strFolio ';

		EXEC sp_executesql @sqlQuery, N'@Folio VARCHAR(30)', @Folio = @Folio;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO
/ *

*/
IF OBJECT_ID('dbo.usp_ActualizaEstatusProcesadoSpei', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ActualizaEstatusProcesadoSpei AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ActualizaEstatusProcesadoSpei
(
    @TablaTomi VARCHAR(100),
	@CampoTomi VARCHAR(100),
	@FolioTomi VARCHAR(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

		DECLARE @sqlQuery NVARCHAR(max) = '';
	
        SET @sqlQuery = 'UPDATE '+ @TablaTomi + ' SET nf_estatus =''PROC'' ' +
					    ' WHERE ' + @CampoTomi + ' = @FolioTomi ';

		EXEC sp_executesql @sqlQuery, N'@FolioTomi VARCHAR(30)', @FolioTomi = @FolioTomi;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO

/*

*/
IF OBJECT_ID('dbo.usp_ObtenerFolioMaestro', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ObtenerFolioMaestro AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ObtenerFolioMaestro
(
	@folio			BIGINT,
	@tipooperacion	VARCHAR(10)
)
AS
BEGIN
	SELECT TOP 1 Mto.folio_maestro 
    FROM tb_srv_plan_trabajo_maestro Mto, tb_srv_plan_trabajo_detalle Det
    WHERE Det.folio= @Folio
    AND Det.tipo_operacion= @tipooperacion
    AND Mto.folio=Det.folio 
    AND Mto.tipo_operacion=Det.tipo_operacion
END
GO


/*

*/
DROP PROCEDURE IF EXISTS dbo.usp_ObtenerPuertoEscuchaServicio
GO
CREATE OR ALTER PROCEDURE dbo.usp_ObtenerPuertoEscuchaServicio
(
	@servicio	VARCHAR(25),
	@pid		INT
)
AS
BEGIN
	SELECT TOP 1 Puerto,Servicio 
	FROM TB_SRV_PUERTO_X_SERVICIO
	Where operando = 1
	AND servicio  = 'ActualizaEstatus'
	ORDER by fecha_inicio DESC;
	--AND pid = @pid;
END
GO


/*

*/
IF OBJECT_ID('dbo.usp_ActualizaBmx1Spei', 'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.usp_ActualizaBmx1Spei AS BEGIN SET NOCOUNT ON; END')
END;
GO
ALTER PROCEDURE dbo.usp_ActualizaBmx1Spei
(
    @op_fecha_oper numeric(8,0),
	@op_cve_rastreo varchar(30),
	@RowsAffected   INT OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY

		BEGIN TRANSACTION;

              CREATE TABLE #tmpSPEI(
				  op_clave numeric (18,0), 
				  op_fecha_oper numeric (8,0), 
				  me_clave numeric (2,0),
				  opfolio numeric (5,0), 
				  ins_clave numeric (10,0), 
				  op_monto numeric (18,2), 
				  tp_clave numeric (2,0), 
				  op_cve_rastreo varchar(30), 
				  op_nom_ord varchar(40), 
				  tc_clave_ord numeric (2,0), 
				  op_cuenta_ord varchar(20), 
				  op_rfc_curp_ord varchar(18), 
				  op_nom_ben varchar(40), 
				  tc_clave_ben numeric (2,0),
				  op_cuenta_ben varchar(20), 
				  op_rfc_curp_ben varchar(18),
				  op_cuenta_ben2 varchar(35) ,
				  op_rfc_curp_ben2 varchar(18), 
				  op_concepto_pago varchar(210), 
				  op_concepto_pag2 varchar(40), 
				  op_iva numeric (18,2), 
				  op_ref_numerica numeric (7,0),
				  op_ref_cobranza varchar(40), 
				  op_clave_pago varchar(10), 
				  to_clave numeric (2,0), 
				  cd_clave numeric (2,0), 
				  op_estado varchar(2), 
				  op_cde varchar(255), 
				  op_fecha_cap numeric (8,0), 
				  op_hora_cap numeric (6,0), 
				  op_tipo_orden varchar(1), 
				  op_prioridad numeric (1,0), 
				  usu_clave varchar(10), 
				  opfolio_servidor numeric (10,0), 
				  op_razon_rechazo varchar(120), 
				  op_usu_autoriza varchar(10),
				  op_topologia varchar(1), 
				  err_clave numeric (3,0), 
				  op_exporta numeric (6,0), 
				  op_cargo_abono varchar(2), 
				  op_afecta_ban numeric (18,0), 
				  op_duplicado varchar(2), 
				  op_hora_liq_bm numeric (6,0), 
				  op_hora_liq_sist numeric (9,0), 
				  hora_cargo_abono numeric (6,0), 
				  ah_ar_clave numeric (2,0), 
				  op_ah_estado varchar(1), 
				  chk_folio_host numeric (8,0), 
				  op_depurado varchar(1), 
				  op_procesa_xml varchar(2), 
				  ordenes_host_op_clave numeric (18,0),
				  op_cve_rastreoHEX varchar(255), 
				  op_edo_host varchar(100), 
				  op_id_mensaje numeric (18,0), 
				  op_hora_host datetime, 
				  op_edo_acuse varchar(1024), 
				  op_hora_acuse datetime2(6), 
				  op_id_acuse numeric (18,0), 
				  op_paq_folio numeric (18,0), 
				  op_usu_lib varchar(10), 
				  op_usu_canc varchar(10), 
				  op_usu_rech varchar(10) 
              );
              
              INSERT INTO #tmpSPEI EXEC sp_InterfaceTOMI_get_SPEI_e_2 @op_fecha_oper = @op_fecha_oper, @op_cve_rastreo = @op_cve_rastreo;
              
              UPDATE bmx1 
                   SET op_clave  = spei.op_clave, 
              op_fecha_oper = spei.op_fecha_oper, 
              me_clave = spei.me_clave,
              op_folio = spei.opfolio, 
              ins_clave = spei.ins_clave, 
              op_monto = spei.op_monto, 
              tp_clave = spei.tp_clave, 
              op_cve_rastreo = spei.op_cve_rastreo, 
              op_nom_ord = spei.op_nom_ord, 
              tc_clave_ord = spei.tc_clave_ord, 
              op_cuenta_ord = spei.op_cuenta_ord, 
              op_rfc_curp_ord = spei.op_rfc_curp_ord, 
              op_nom_ben = spei.op_nom_ben, 
              tc_clave_ben = spei.tc_clave_ben,
              op_cuenta_ben = spei.op_cuenta_ben, 
              op_rfc_curp_ben = spei.op_rfc_curp_ben,
              op_cuenta_ben2 = spei.op_cuenta_ben2,
              op_rfc_curp_ben2 = spei.op_rfc_curp_ben2, 
              op_concepto_pago = spei.op_concepto_pago, 
              op_concepto_pag2 = spei.op_concepto_pag2, 
              op_iva = spei.op_iva, 
              op_ref_numerica = spei.op_ref_numerica,
              op_ref_cobranza = spei.op_ref_cobranza, 
              op_clave_pago = spei.op_clave_pago, 
              to_clave = spei.to_clave, 
              cd_clave = spei.cd_clave, 
              op_estado = spei.op_estado, 
              op_cde = spei.op_cde, 
              op_fecha_cap = spei.op_fecha_cap, 
              op_hora_cap = spei.op_hora_cap, 
              op_tipo_orden = spei.op_tipo_orden, 
              op_prioridad = spei.op_prioridad, 
              usu_clave = spei.usu_clave, 
              op_folio_servidor = spei.opfolio_servidor, 
              op_razon_rechazo = spei.op_razon_rechazo, 
              op_usu_autoriza = spei.op_usu_autoriza,
              op_topologia = spei.op_topologia, 
              err_clave = spei.err_clave, 
              op_exporta = spei.op_exporta, 
              op_cargo_abono = spei.op_cargo_abono, 
              op_afecta_ban = spei.op_afecta_ban, 
              op_duplicado = spei.op_duplicado, 
              op_hora_liq_bm = spei.op_hora_liq_bm, 
              op_hora_liq_sist = spei.op_hora_liq_sist, 
              hora_cargo_abono = spei.hora_cargo_abono, 
              ah_ar_clave = spei.ah_ar_clave, 
              op_ah_estado = spei.op_ah_estado, 
              chk_folio_host = spei.chk_folio_host, 
              op_depurado = spei.op_depurado, 
              op_procesa_xml = spei.op_procesa_xml, 
              ordenes_host_op_clave = spei.ordenes_host_op_clave,
              op_cve_rastreoHEX = spei.op_cve_rastreoHEX, 
              op_edo_host = spei.op_edo_host, 
              op_id_mensaje = spei.op_id_mensaje, 
              op_hora_host = spei.op_hora_host, 
              op_edo_acuse = spei.op_edo_acuse, 
              op_hora_acuse = spei.op_hora_acuse, 
              op_id_acuse = spei.op_id_acuse, 
              op_paq_folio = spei.op_paq_folio, 
              op_usu_lib = spei.op_usu_lib, 
              op_usu_canc = spei.op_usu_canc, 
              op_usu_rech = spei.op_usu_rech
              FROM bmx_ord_pagdia_e AS bmx1
              INNER JOIN #tmpSPEI AS spei on spei.op_cve_rastreo = bmx1.op_cve_rastreo and spei.op_fecha_oper = bmx1.op_fecha_oper
              WHERE bmx1.op_clave = @op_cve_rastreo And bmx1.op_fecha_oper = @op_fecha_oper;

		SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;

	END TRY

	BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
 
        INSERT INTO Auditoria.ErrorLog
        (
            ErrorNumber,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine,
            ErrorMessage
        )
        VALUES
        (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE()
        );
 
        THROW;
    END CATCH;

END
GO
