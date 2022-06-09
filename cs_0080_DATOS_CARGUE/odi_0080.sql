----JP_0080_DATOS_CARGA_ARCH_y_CONCILIA

----PR_0080_CONCILIACION

--------------Define Catalogo DWH_DB----------------

SET CATALOG #PRY_DWH.V_0080_DB_DWH_DB_WRITE;

--------------Elimina Tabla Temporal----------------

DROP TABLE #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_TEMPORAL_CONCILIACION IF EXISTS;

---------------Crea Tabla Temporal------------------

--SET CATALOG #PRY_DWH.V_0080_DB_DWH_DB_WRITE;

CREATE TABLE #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_TEMPORAL_CONCILIACION AS (
	SELECT f.*, C.NOMBRE_ARCHIVO NOM_CONCI, C.TOTAL_REGISTROS_CARGADOS REG_CAR, C.TOTAL_REGISTROS_ESTADISTICAS REG_EST  
	FROM (
		SELECT 
		CASE WHEN A.FECHA_TRAFICO IS NULL THEN B.FECHA_TRAFICO ELSE A.FECHA_TRAFICO END FECHA_TRAFICO,
		CASE WHEN A.NOMBRE_ARCHIVO IS NULL THEN B.NOMBRE_ARCHIVO ELSE A.NOMBRE_ARCHIVO END NOMBRE_ARCHIVO,
		CASE WHEN B.NOMBRE_ARCHIVO IS NULL THEN 'SIN TRAFICO' 
			WHEN A.NOMBRE_ARCHIVO IS NULL THEN 'SIN ESTADISTICAS' ELSE 'N/A' END ESTADO,
		B.CANTIDAD_REGISTROS TOTAL_REGISTROS_CARGADOS,
		A.CANTIDAD_REGISTROS TOTAL_REGISTROS_ESTADISTICAS,
		CURRENT_TIMESTAMP  FECHA_ACTUALIZACION,
		CURRENT_TIMESTAMP FECHA_CREACION
		FROM (
			SELECT * FROM #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_FINAL_ESTADISTICAS
			WHERE FECHA_CREACION BETWEEN  TO_TIMESTAMP(TO_CHAR(CURRENT_DATE -15,'YYYYMMDD')||' 000000','YYYYMMDD HH24MISS')
			AND TO_TIMESTAMP(TO_CHAR(CURRENT_DATE,'YYYYMMDD')||' 235959','YYYYMMDD HH24MISS')
		) A 
		FULL OUTER JOIN (
                	SELECT * FROM #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_CONTROL_GPRS 
			WHERE FECHA_CREACION BETWEEN TO_TIMESTAMP(TO_CHAR(CURRENT_DATE -15,'YYYYMMDD')||' 000000','YYYYMMDD HH24MISS')
			AND TO_TIMESTAMP(TO_CHAR(CURRENT_DATE,'YYYYMMDD')||' 235959','YYYYMMDD HH24MISS')
		)B
		ON A.NOMBRE_ARCHIVO = B.NOMBRE_ARCHIVO      
	) F 
	LEFT JOIN #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_FINAL_CONCILIACION C 
	ON F.NOMBRE_ARCHIVO = C.NOMBRE_ARCHIVO
)
DISTRIBUTE ON RANDOM
;

-----------------Creacion Seguridad tabla conciliacion---------------

CREATE TABLE IF NOT EXISTS #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_FINAL_CONCILIACION
(
	FECHA_TRAFICO DATE,
	NOMBRE_ARCHIVO CHARACTER VARYING(60),
	ESTADO CHARACTER VARYING(16),
	TOTAL_REGISTROS_CARGADOS INTEGER,
	TOTAL_REGISTROS_ESTADISTICAS INTEGER,
	FECHA_ACTUALIZACION TIMESTAMP,
	FECHA_CREACION TIMESTAMP
)
DISTRIBUTE ON RANDOM
ORGANIZE ON (FECHA_TRAFICO, NOMBRE_ARCHIVO);

---------------- Inserta Registros Nuevos --------------------------

INSERT INTO #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_FINAL_CONCILIACION(
	SELECT
	FECHA_TRAFICO, 
	NOMBRE_ARCHIVO, 
	ESTADO, 
	TOTAL_REGISTROS_CARGADOS, 
	TOTAL_REGISTROS_ESTADISTICAS, 
	FECHA_ACTUALIZACION, 
	FECHA_CREACION 
	FROM #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_TEMPORAL_CONCILIACION
	WHERE NOM_CONCI IS NULL 
);

----------------- Actualiza Registros --------------------------- 

--SET CATALOG #PRY_DWH.V_0080_DB_DWH_DB_WRITE;

UPDATE #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_FINAL_CONCILIACION C
SET
C.FECHA_TRAFICO = T.FECHA_TRAFICO, 
C.ESTADO = T.ESTADO, 
C.TOTAL_REGISTROS_CARGADOS = T.TOTAL_REGISTROS_CARGADOS, 
C.TOTAL_REGISTROS_ESTADISTICAS = T.TOTAL_REGISTROS_ESTADISTICAS, 
C.FECHA_ACTUALIZACION = T.FECHA_ACTUALIZACION
FROM (
	SELECT 
	NOMBRE_ARCHIVO, 
	FECHA_TRAFICO, 
	ESTADO, 
	NVL(TOTAL_REGISTROS_CARGADOS,REG_CAR) TOTAL_REGISTROS_CARGADOS,
	NVL(TOTAL_REGISTROS_ESTADISTICAS,REG_EST) TOTAL_REGISTROS_ESTADISTICAS, 
	FECHA_ACTUALIZACION
	FROM #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_TEMPORAL_CONCILIACION
	WHERE NOM_CONCI IS NOT NULL 
	AND (
	NVL(TOTAL_REGISTROS_CARGADOS,NVL(REG_CAR,0)) <> NVL(REG_CAR,0) OR
	NVL(TOTAL_REGISTROS_ESTADISTICAS,NVL(REG_EST,0)) <> NVL(REG_EST,0)
	)
) T
WHERE C.NOMBRE_ARCHIVO = T.NOMBRE_ARCHIVO
;

-------------- Actualizar Estado --------------------------------------

--SET CATALOG #PRY_DWH.V_0080_DB_DWH_DB_WRITE;

UPDATE #PRY_DWH.V_0080_DB_DWH_DB_WRITE.#PRY_DWH.V_0080_SCH_DATOS.#PRY_DWH.V_0080_TABLA_FINAL_CONCILIACION
SET ESTADO = 'N/A'
WHERE TOTAL_REGISTROS_CARGADOS IS NOT NULL
AND TOTAL_REGISTROS_ESTADISTICAS IS NOT NULL 
AND ESTADO != 'N/A' 
;




----------------DIMENSIONES     ------------------------------------------------
SELECT ID_APN 
	,APNNETWORK
    ,FECHA_aCTUALIZACION
FROM DWH_DB.DATOS.TBL_DIM_APNNETWORK_T1 

SELECT ID_PLMNIDENTIFIER 
 ,PLMNIDENTIFIER 
 ,FECHA_ACTUALIZACION 
 FROM DWH_DB.DATOS.TBL_DIM_PLMNIDENTIFIER_T1 

SELECT DATE_KEY
	,FULL_DATE
	,DAY_NUM_IN_MONTH
	,DAY_NAME
	,WEEK_NUM_OVERALL
	,WEEK_BEGIN_DATE
	,WEEK_BEGIN_DATE_KEY
	,MONTH
	,MONTH_NAME
FROM DWH_DB.DRASDWH.TBL_DIM_TIEMPO 
LIMIT 100

----------------------------------------DIM APPNETWORK------------------------
SET CATALOG #PRY_DWH.V_0166_DB_DWH_DB;
INSERT INTO  #PRY_DWH.V_0166_DB_DWH_DB.#PRY_DWH.V_0166_SCH_DATOS.tbl_dim_apnnetwork_t1
 (id_apn
 ,apnnetwork
 ,fecha_actualizacion)
 SELECT row_number() over(ORDER BY a.apnnetwork) + b.maxskey AS id_apn
       ,upper(a.apnnetwork) AS apnnetwork
       ,current_timestamp AS fecha_actualizacion
 FROM   (SELECT DISTINCT upper(a.apnnetwork) apnnetwork
         FROM   #PRY_DWH.V_0166_CNX_DWH_DB_STAGE.#PRY_DWH.V_0166_SCH_DATOS.TBL_TMP_DATOS_CONSOLIDADO_DIARIO a
         LEFT   JOIN  #PRY_DWH.V_0166_DB_DWH_DB.#PRY_DWH.V_0166_SCH_DATOS.tbl_dim_apnnetwork_t1 t ON (UPPER(a.apnnetwork) = t.apnnetwork)
         WHERE  t.id_apn IS NULL) a
 CROSS  JOIN (SELECT nvl(MAX(id_apn), 0) maxskey
              FROM   #PRY_DWH.V_0166_DB_DWH_DB.#PRY_DWH.V_0166_SCH_DATOS.tbl_dim_apnnetwork_t1) b;

------------------------------------  PLMNIDENTIFIER  ------------------------------------------
SET CATALOG #PRY_DWH.V_0166_DB_DWH_DB;
INSERT INTO  #PRY_DWH.V_0166_DB_DWH_DB.#PRY_DWH.V_0166_SCH_DATOS.tbl_dim_plmnidentifier_t1
 (id_plmnidentifier
 ,plmnidentifier
 ,fecha_actualizacion)
 SELECT row_number() over(ORDER BY a.plmnidentifier) + b.maxskey AS id_plmnidentifier
       ,a.plmnidentifier
       ,current_timestamp AS fecha_actualizacion
 FROM   (SELECT DISTINCT a.plmnidentifier
         FROM   #PRY_DWH.V_0166_CNX_DWH_DB_STAGE.#PRY_DWH.V_0166_SCH_DATOS.TBL_TMP_DATOS_CONSOLIDADO_DIARIO a
         LEFT   JOIN  #PRY_DWH.V_0166_DB_DWH_DB.#PRY_DWH.V_0166_SCH_DATOS.tbl_dim_plmnidentifier_t1 t ON (nvl(a.plmnidentifier,'-1') = nvl(t.plmnidentifier,'-1'))
         WHERE  t.ID_plmnidentifier IS NULL) a
 CROSS  JOIN (SELECT nvl(MAX(ID_plmnidentifier), 0) maxskey
              FROM    #PRY_DWH.V_0166_DB_DWH_DB.#PRY_DWH.V_0166_SCH_DATOS.tbl_dim_plmnidentifier_t1) b;
              
             
------------------------------------------------- CREACION TABLA USO ---------------------------------
 
CREATE TABLE DATOS.TBL_TMP_DATOS_CONSOLIDADO_DIARIO_PRUEBA2
AS
SELECT
t1.fecha
,t1.tele_numb
,t1.record_type
,t1.served_imsi
,t1.served_imei
,t1.plmnidentifier
,t1.apnnetwork
,t1.system_type
,t1.charging_characteristics
,t1.user_profile
,t1.rating_group
,t1.serviceid
,t1.nodo_med
,t1.fecha_proceso
,t1.cantidad_eventos
,t1.consumo_bytes_uplink
,t1.consumo_bytes_downlink
,t1.consumo_bytes_total
,t1.duracion
,T1.CELL_IDENTITY
,T1.LOCATION_AREA_CODE
from
(SELECT CAST(to_char(a.record_opening_time, 'yyyymmdd') AS INTEGER) AS fecha
,a.tele_numb
,a.record_type
,a.served_imsi
,a.served_imei
,a.plmnidentifier
,a.LOCATION_AREA_CODE
,a.CELL_IDENTITY
,a.apnnetwork
,a.system_type
,a.charging_characteristics
,a.user_profile
,a.charginclass as rating_group
,a.serviceid
,a.nodo_med
,current_timestamp AS fecha_proceso
,count(*) AS cantidad_eventos
,SUM(nvl(a.uplink,0)) AS consumo_bytes_uplink
,SUM(nvl(a.downlink,0)) AS consumo_bytes_downlink
,SUM(nvl(a.uplink,0)+ nvl(a.downlink,0)) AS consumo_bytes_total
,SUM(a.duration) AS duracion
FROM DB_DWH_DATOS.DATOS.TBL_FACT_DATOS_TRAFICO_202202 a
WHERE a.record_opening_time between to_timestamp('20220225' ||' 000000','yyyymmdd hh24miss') and
to_timestamp('20220225' ||' 235959','yyyymmdd hh24miss')
GROUP BY CAST(to_char(a.record_opening_time, 'yyyymmdd') AS INTEGER)
,a.tele_numb
,a.record_type
,a.served_imsi
,a.served_imei
,a.plmnidentifier
,a.LOCATION_AREA_CODE
,a.CELL_IDENTITY
,a.apnnetwork
,a.system_type
,a.charging_characteristics
,a.user_profile
,a.charginclass
,a.serviceid
,a.nodo_med
,current_timestamp
ORDER BY tele_numb)t1




