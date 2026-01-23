USE [InterfacesT24]
GO

/****** Object:  View [dbo].[VW_PAGO_COMPENSACIONES_OUT]    Script Date: 1/23/2026 1:12:22 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






ALTER VIEW [dbo].[VW_PAGO_COMPENSACIONES_OUT]
AS
SELECT 	A.folio AS Folio, 
		B.NF_FECHA_EJECUCION AS Fecha, 
		B.T24_TRANSACTION_ID AS Tracking_Key, 
		B.T24_DEBIT_AMOUNT AS Importe, 
		B.T24_CUSTOMER_CODE AS CDL, 
		B.T24_SHORT_NAME AS Nombre_Ordenante,
		dbo.Cuenta_CLABE(B.T24_DEBIT_ACCT_NO) AS Cuenta_Ordenante,
		B.T24_BEN_CUST_NAME AS Nombre_Beneficiario, 
		B.T24_BEN_ACCT_NO AS CuentaBeneficiario, 
		C.ESTATUS,
		A.orden_ejecucion as orden_ejecucion,
		A.tipo_operacion,
		B.NF_BANCO_BENEF AS BancoBeneficiario, 
		MIN(A.fec_inicio) AS Fecha_Inicio, 
		MAX(A.fec_fin) AS Fecha_Fin, 
		DATEDIFF(SECOND, A.fec_fin, CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),5,2))) AS Tiempo_Total, 
		B.T24_DEBIT_ACCT_NO,
		DATEDIFF(day,convert(date,convert(varchar,B.NF_FECHA_EJECUCION)) ,getdate()) as Dias_Transcurridos,
		dbo.Dame_Dia_Ant_Habil(convert(datetime,convert(char(8),B.NF_FECHA_EJECUCION))) as FechaTasa,
		CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),5,2)) as op_hora_cap,
		DATEDIFF(SECOND, A.fec_fin, CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),5,2))) AS Dif_Karpay,
		B.T24_DEBIT_ACCT_NO as CuentaCompensar
	FROM
		dbo.TB_SRV_PLAN_TRABAJO_DETALLE AS A WITH (nolock) INNER JOIN
        dbo.TB_TOMBIN_NF_FXD AS B WITH (nolock) ON A.folio = B.NF_FOLIO LEFT JOIN 
		VistasT24.dbo.VW_T24_CUSTOMER_ACCOUNT F ON B.T24_DEBIT_ACCT_NO COLLATE Latin1_General_100_BIN = F.ACCOUNT_NUMBER COLLATE Latin1_General_100_BIN LEFT OUTER JOIN
		(select nf_folio, case when COUNT(DISTINCT ESTATUS) >1 THEN
		CASE  WHEN MAX(ESTATUS) = 'CAPTURADO' OR  MAX(ESTATUS) = 'AUTORIZADO' THEN MAX(ESTATUS)
		ELSE MIN(ESTATUS) END
		ELSE MAX(ESTATUS) end as estatus
		FROM TB_SRV_CTRL_COMPENSACIONES group by nf_folio ) C ON B.NF_FOLIO = C.NF_FOLIO LEFT OUTER JOIN
		MXDF01SQL102.Enlace.dbo.bmx_ord_pagAnt_e D WITH (nolock) ON B.T24_Transaction_ID = D.op_cve_rastreo LEFT OUTER JOIN 
		MXDF01SQL102.Enlace.dbo.bmx_pro_pagant_e E WITH (nolock) ON D.op_clave = E.OP_CLAVE
	WHERE
		(A.tipo_operacion = 'FXD') AND (A.orden_ejecucion IN (13)) AND (B.NF_ESTATUS = 'PROC') AND ( F.CATEGORY  not in ('1012','1013'))
	GROUP BY A.folio, B.NF_FECHA_EJECUCION, B.T24_TRANSACTION_ID, B.T24_DEBIT_AMOUNT, B.T24_CUSTOMER_CODE, B.T24_SHORT_NAME, B.T24_DEBIT_ACCT_NO, B.T24_BEN_CUST_NAME, B.T24_BEN_ACCT_NO, B.NF_BANCO_BENEF,C.ESTATUS,A.orden_ejecucion,A.tipo_operacion,D.op_fecha_oper,D.op_hora_liq_bm,D.op_hora_cap,E.pp_hora20,E.pp_hora12,E.pp_hora21, A.fec_fin
UNION
SELECT	A.folio AS Folio, 
		B.NF_FECHA_EJECUCION AS Fecha, 
		B.T24_TRANSACTION_ID AS Tracking_Key, 
		B.T24_DEBIT_AMOUNT AS Importe, 
		B.T24_CUSTOMER_CODE AS CDL, 
		B.T24_SHORT_NAME AS Nombre_Ordenante,
		dbo.Cuenta_CLABE(B.T24_DEBIT_ACCT_NO) AS Cuenta_Ordenante,
		B.T24_BEN_CUST_NAME AS Nombre_Beneficiario, 
		B.T24_BEN_ACCT_NO AS CuentaBeneficiario, 
		C.ESTATUS,
		A.orden_ejecucion as orden_ejecucion,
		A.tipo_operacion,
		B.NF_BANCO_BENEF AS BancoBeneficiario, 
		MIN(A.fec_inicio) AS Fecha_Inicio, 
		MAX(A.fec_fin) AS Fecha_Fin, 
		DATEDIFF(SECOND, A.fec_fin, CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),5,2))) AS Tiempo_Total, 
		B.T24_DEBIT_ACCT_NO,
		DATEDIFF(day,convert(date,convert(varchar,B.NF_FECHA_EJECUCION)) ,getdate()) as Dias_Transcurridos,
		dbo.Dame_Dia_Ant_Habil(convert(datetime,convert(char(8),B.NF_FECHA_EJECUCION))) as FechaTasa,
		CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),5,2)) as op_hora_cap,
		DATEDIFF(SECOND, A.fec_fin,CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),5,2))) AS Dif_Karpay,
		B.T24_DEBIT_ACCT_NO as CuentaCompensar
	FROM
		dbo.TB_SRV_PLAN_TRABAJO_DETALLE AS A WITH (nolock) INNER JOIN
        dbo.TB_TOMBIN_NF_FXD_LOCAL AS B WITH (nolock) ON A.folio = B.NF_FOLIO LEFT JOIN 
		VistasT24.dbo.VW_T24_CUSTOMER_ACCOUNT F ON B.T24_DEBIT_ACCT_NO COLLATE Latin1_General_100_BIN = F.ACCOUNT_NUMBER COLLATE Latin1_General_100_BIN LEFT OUTER JOIN
		(select nf_folio, case when COUNT(DISTINCT ESTATUS) >1 THEN
		CASE  WHEN MAX(ESTATUS) = 'CAPTURADO' OR  MAX(ESTATUS) = 'AUTORIZADO' THEN MAX(ESTATUS)
		ELSE MIN(ESTATUS) END
		ELSE MAX(ESTATUS) end as estatus
		FROM TB_SRV_CTRL_COMPENSACIONES group by nf_folio ) C ON B.NF_FOLIO = C.NF_FOLIO LEFT OUTER JOIN
		MXDF01SQL102.Enlace.dbo.bmx_ord_pagAnt_e D WITH (nolock) ON B.T24_Transaction_ID = D.op_cve_rastreo LEFT OUTER JOIN 
		MXDF01SQL102.Enlace.dbo.bmx_pro_pagant_e E WITH (nolock) ON D.op_clave = E.OP_CLAVE
	WHERE
		(A.tipo_operacion = 'FXD_Local') AND (A.orden_ejecucion IN (8)) AND (B.NF_ESTATUS = 'PROC') AND ( F.CATEGORY not in ('1012','1013'))
	GROUP BY 
		A.folio, B.NF_FECHA_EJECUCION, B.T24_TRANSACTION_ID, B.T24_DEBIT_AMOUNT, B.T24_CUSTOMER_CODE, B.T24_SHORT_NAME, B.T24_DEBIT_ACCT_NO, B.T24_BEN_CUST_NAME, B.T24_BEN_ACCT_NO, B.NF_BANCO_BENEF,C.ESTATUS,A.orden_ejecucion,A.tipo_operacion,D.op_fecha_oper,D.op_hora_liq_bm,D.op_hora_cap,E.pp_hora20,E.pp_hora12,E.pp_hora21, A.fec_fin
UNION
	SELECT	A.folio AS Folio, 
			B.NF_FECHA_EJECUCION AS Fecha, 
			B.T24_TRANSACTION_ID AS Tracking_Key, 
			B.T24_DEBIT_AMOUNT AS Importe, 
			B.T24_CUSTOMER_CODE AS CDL, 
			B.T24_SHORT_NAME AS Nombre_Ordenante,
			dbo.Cuenta_CLABE(B.T24_DEBIT_ACCT_NO) AS Cuenta_Ordenante,
			B.T24_BEN_CUST_NAME AS Nombre_Beneficiario, 
			B.T24_BEN_ACCT_NO AS CuentaBeneficiario, 
			C.ESTATUS,
			A.orden_ejecucion as orden_ejecucion,
			A.tipo_operacion,
			B.NF_BANCO_BENEF AS BancoBeneficiario, 
			MIN(A.fec_inicio) AS Fecha_Inicio, 
			MAX(A.fec_fin) AS Fecha_Fin, 
			DATEDIFF(SECOND, A.fec_fin,CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),5,2))) AS Tiempo_Total, 
			B.T24_DEBIT_ACCT_NO,
			DATEDIFF(day,convert(date,convert(varchar,B.NF_FECHA_EJECUCION)) ,getdate()) as Dias_Transcurridos,
			dbo.Dame_Dia_Ant_Habil(convert(datetime,convert(char(8),B.NF_FECHA_EJECUCION))) as FechaTasa,
			CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),5,2)) as op_hora_cap,
			DATEDIFF(SECOND,A.fec_fin, CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),5,2))) AS Dif_Karpay,
			B.T24_DEBIT_ACCT_NO as CuentaCompensar
		FROM
			dbo.TB_SRV_PLAN_TRABAJO_DETALLE AS A WITH (nolock) INNER JOIN
            dbo.TB_TOMBIN_NF_FXD_H2H AS B WITH (nolock) ON A.folio = B.NF_FOLIO LEFT JOIN 
		VistasT24.dbo.VW_T24_CUSTOMER_ACCOUNT F ON B.T24_DEBIT_ACCT_NO COLLATE Latin1_General_100_BIN = F.ACCOUNT_NUMBER COLLATE Latin1_General_100_BIN LEFT OUTER JOIN
			(select nf_folio, case when COUNT(DISTINCT ESTATUS) >1 THEN
			CASE  WHEN MAX(ESTATUS) = 'CAPTURADO' OR  MAX(ESTATUS) = 'AUTORIZADO' THEN MAX(ESTATUS)
			ELSE MIN(ESTATUS) END
			ELSE MAX(ESTATUS) end as estatus
			FROM TB_SRV_CTRL_COMPENSACIONES group by nf_folio ) C ON B.NF_FOLIO = C.NF_FOLIO LEFT OUTER JOIN
			MXDF01SQL102.Enlace.dbo.bmx_ord_pagAnt_e D WITH (nolock) ON B.T24_Transaction_ID = D.op_cve_rastreo LEFT OUTER JOIN 
			MXDF01SQL102.Enlace.dbo.bmx_pro_pagant_e E WITH (nolock) ON D.op_clave = E.OP_CLAVE
		WHERE
			(A.tipo_operacion = 'FXD_H2H') AND (A.orden_ejecucion IN (8)) AND (B.NF_ESTATUS = 'PROC') AND ( F.CATEGORY not in ('1012','1013'))
		GROUP BY 
			A.folio, B.NF_FECHA_EJECUCION, B.T24_TRANSACTION_ID, B.T24_DEBIT_AMOUNT, B.T24_CUSTOMER_CODE, B.T24_SHORT_NAME, B.T24_DEBIT_ACCT_NO, B.T24_BEN_CUST_NAME, B.T24_BEN_ACCT_NO, B.NF_BANCO_BENEF,C.ESTATUS,A.orden_ejecucion,A.tipo_operacion,D.op_fecha_oper,D.op_hora_liq_bm,D.op_hora_cap,E.pp_hora20,E.pp_hora12,E.pp_hora21, A.fec_fin
UNION
		SELECT	A.folio AS Folio, 
				B.NF_FECHA_EJECUCION AS Fecha, 
				B.T24_TRANSACTION_ID AS Tracking_Key, 
				B.T24_DEBIT_AMOUNT AS Importe, 
				B.T24_CUSTOMER_CODE AS CDL, 
				B.T24_SHORT_NAME AS Nombre_Ordenante,
				dbo.Cuenta_CLABE(B.T24_DEBIT_ACCT_NO) AS Cuenta_Ordenante,
				B.T24_BEN_CUST_NAME AS Nombre_Beneficiario, 
				B.T24_BEN_ACCT_NO AS CuentaBeneficiario, 
				C.ESTATUS,
				A.orden_ejecucion as orden_ejecucion,
				A.tipo_operacion,
				B.NF_BANCO_BENEF AS BancoBeneficiario, 
				MIN(A.fec_inicio) AS Fecha_Inicio, 
				MAX(A.fec_fin) AS Fecha_Fin, 
				DATEDIFF(SECOND, A.fec_fin,CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),5,2))) AS Tiempo_Total, 
				B.T24_DEBIT_ACCT_NO,
				DATEDIFF(day,convert(date,convert(varchar,B.NF_FECHA_EJECUCION)) ,getdate()) as Dias_Transcurridos,
				dbo.Dame_Dia_Ant_Habil(convert(datetime,convert(char(8),B.NF_FECHA_EJECUCION))) as FechaTasa,
				CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),E.pp_hora12),6),5,2)) as op_hora_cap,
				DATEDIFF(SECOND, A.fec_fin,CONVERT(datetime,substring(convert(varchar(8),D.op_fecha_oper),1,4) +'-'+substring(convert(varchar(8),D.op_fecha_oper),5,2)+'-'+substring(convert(varchar(8),D.op_fecha_oper),7,2)+' '+SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),1,2)+':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),3,2) + ':'+ SUBSTRING(RIGHT(REPLICATE('0',6) + convert(varchar(6),isnull(E.pp_hora20, E.pp_hora21)),6),5,2))) AS Dif_Karpay,
				B.T24_DEBIT_ACCT_NO as CuentaCompensar
			FROM
				dbo.TB_SRV_PLAN_TRABAJO_DETALLE AS A WITH (nolock) INNER JOIN
                dbo.TB_TOMBIN_NF_FXD_GPH AS B WITH (nolock) ON A.folio = B.NF_FOLIO LEFT JOIN 
				VistasT24.dbo.VW_T24_CUSTOMER_ACCOUNT F ON B.T24_DEBIT_ACCT_NO COLLATE Latin1_General_100_BIN = F.ACCOUNT_NUMBER COLLATE Latin1_General_100_BIN  LEFT OUTER JOIN
				(select nf_folio, case when COUNT(DISTINCT ESTATUS) >1 THEN
				CASE  WHEN MAX(ESTATUS) = 'CAPTURADO' OR  MAX(ESTATUS) = 'AUTORIZADO' THEN MAX(ESTATUS)
				ELSE MIN(ESTATUS) END
				ELSE MAX(ESTATUS) end as estatus
				FROM TB_SRV_CTRL_COMPENSACIONES group by nf_folio ) C ON B.NF_FOLIO = C.NF_FOLIO LEFT OUTER JOIN
				MXDF01SQL102.Enlace.dbo.bmx_ord_pagAnt_e D WITH (nolock) ON B.T24_Transaction_ID = D.op_cve_rastreo LEFT OUTER JOIN 
				MXDF01SQL102.Enlace.dbo.bmx_pro_pagant_e E WITH (nolock) ON D.op_clave = E.OP_CLAVE
			WHERE
				(A.tipo_operacion = 'FXD_GPH') AND (A.orden_ejecucion IN (8)) AND (B.NF_ESTATUS = 'PROC') AND ( F.CATEGORY not in ('1012','1013'))
			GROUP BY 
				A.folio, B.NF_FECHA_EJECUCION, B.T24_TRANSACTION_ID, B.T24_DEBIT_AMOUNT, B.T24_CUSTOMER_CODE, B.T24_SHORT_NAME, B.T24_DEBIT_ACCT_NO, B.T24_BEN_CUST_NAME, B.T24_BEN_ACCT_NO, B.NF_BANCO_BENEF,C.ESTATUS,A.orden_ejecucion,A.tipo_operacion,D.op_fecha_oper,D.op_hora_liq_bm,D.op_hora_cap,E.pp_hora20,E.pp_hora12,E.pp_hora21, A.fec_fin

GO


