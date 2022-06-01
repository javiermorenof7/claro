------QUERYS Ayuda----------------------------

SET CATALOG DB_DWH_DATOS
-- QUERY PARA BUSCAR POR NOMBRE DE TABLA EN LAS DIFERENTES DB DE NTZ
Select
'SELECT * FROM '||DATABASE||'.'||SCHEMA||'.'||TABLENAME||' LIMIT 100;'
From _V_TABLE
Where upper(TABLENAME) like upper('%FACT_CLIEN%');

-- QUERY PARA BUSCAR POR NOMBRE DE CAMPO EN LAS DIFERENTES DB/TBL DE NTZ
select
'SELECT * FROM '||DATABASE||'.'||SCHEMA||'.'||TABLE_NAME||' LIMIT 100;'
from _v_sys_columns
where COLUMN_NAME like upper('%ASESOR%')

-----------------------------------------------------------------------------------------------------------------------
------------------------------------DATOS------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------

-------QUERY EXTRACCION DE DATOS (TABLA TEMPORAL)---------------------------------------------------------

SET CATALOG DB_DWH_DESARROLLO

CREATE TABLE DB_DWH_DESARROLLO.DATOS.TBL_TMP_PRUEBA_DATOS_2_M AS

SELECT * 
FROM (

SELECT *
FROM DB_DWH_DATOS.DATOS.TBL_FACT_DATOS_TRAFICO_202204

) AS A
WHERE PLMNIDENTIFIER NOT LIKE '732%'AND UPLINK IS NOT NULL 
AND DOWNLINK IS NOT NULL AND VAL_QCI IS NOT NULL

--------- Query Datos Transformacion a CUBO ----------------------------

CREATE TABLE DB_DWH_DESARROLLO.DATOS.TBL_TMP_MUESTRA_DATOS_FINAL AS

SELECT TO_CHAR(RECORD_OPENING_TIME,'YYYY-MM-DD') RECORD_OPENING_TIME ,
APNNETWORK ,
PLMNIDENTIFIER ,
VAL_QCI,
SUM(UPLINK)  UPLINK,
SUM(DOWNLINK) DOWNLINK

FROM DB_DWH_DESARROLLO.DATOS.PRUEBA_DATOS_2_M

GROUP BY TO_CHAR(RECORD_OPENING_TIME,'YYYY-MM-DD') ,
APNNETWORK ,
PLMNIDENTIFIER ,
VAL_QCI

------- Se crea la tabla Final ------------

CREATE TABLE DB_DWH_DATOS.DATOS.DATOS_EPICA_10469_202204  (
	RECORD_OPENING_TIME VARCHAR(200),
	APNNETWORK VARCHAR(63),
	PLMNIDENTIFIER INTEGER,
	VAL_QCI BYTEINT,
	UPLINK NUMERIC(38,0),
	DOWNLINK NUMERIC(38,0)
);

-----------------------------------------------------------------------------------------------------------------------
------------------------------------VOZ------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


---------------- QUERY VOZ EXTRACCION DE DATOS (TABLA TEMPORAL)---------------------------
SET CATALOG DB_DWH_DESARROLLO

CREATE TABLE DB_DWH_DESARROLLO.VOZ.TBL_TMP_VOZ_EPICA_10469_202204 AS

SELECT TO_CHAR(START_TIME,'YYYY-MM-DD') START_TIME,CAMEL_EXCHANGE_ID,CALLING_NUMBER ,CALLED_NUMBER ,IMSI,CALL_DURATION
FROM(
SELECT *
FROM DB_DWH_VOZ.VOZ.TBL_FACT_VOZ_TRAFICO_202204
) AS A

--------- Query Datos Transformacion a CUBO ----------------------------

CREATE TABLE DB_DWH_DESARROLLO.VOZ.TBL_TMP_VOZ_EPICA_10469_202204_FINAL AS

SELECT START_TIME,CAMEL_EXCHANGE_ID,CALLING_NUMBER ,CALLED_NUMBER ,IMSI, SUM(CALL_DURATION) CALL_DURATION
FROM(

SELECT *
FROM DB_DWH_DESARROLLO.VOZ.TBL_TMP_VOZ_EPICA_10469_202204

) AS A

GROUP BY START_TIME,CAMEL_EXCHANGE_ID,CALLING_NUMBER ,CALLED_NUMBER ,IMSI

-------------- TABLA QA -------------

CREATE TABLE IF NOT EXISTS DB_DWH_DATOS.VOZ.VOZ_EPICA_10469_202204 (

CAMEL_EXCHANGE_ID VARCHAR(18),
CALLING_NUMBER VARCHAR(20),
CALLED_NUMBER VARCHAR(24),
CALL_DURATION INTEGER,
START_TIME TIMESTAMP,
IMSI VARCHAR(16) 
);

-----------------------------------------------------------------------------------------------------------------------
------------------------------------SMS------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


-----------------QUERY SMS ---------------------------

SELECT *	
FROM DWH_DB.MDRS.SMSMMS
WHERE MESSAGE_SUBMITIONTIME  BETWEEN '2022-04-22 00:00:00.000' AND '2022-04-22 23:59:59.000'
LIMIT 200