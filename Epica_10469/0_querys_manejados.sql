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

-- QUERY PARA CALCULAR VOLUMETRIA
SET CATALOG DWH_DB;
Select used_bytes/pow(1024,3) as table_size_in_GB
from _v_table_storage_stat
where tablename LIKE 'SMSMMS';

-----------------------------------------------------------------------------------------------------------------------
------------------------------------DATOS------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------

-------QUERY EXTRACCION DE DATOS (TABLA TEMPORAL)---------------------------------------------------------

SET CATALOG DB_DWH_DESARROLLO

CREATE TABLE DB_DWH_DESARROLLO.DATOS.TBL_TMP_PRUEBA_DATOS_CUBE AS

SELECT * 
FROM DB_DWH_DATOS.DATOS.TBL_FACT_DATOS_TRAFICO_202204
WHERE PLMNIDENTIFIER NOT LIKE '732%'AND UPLINK IS NOT NULL 
AND DOWNLINK IS NOT NULL AND VAL_QCI IS NOT NULL
LIMIT 100000

--------- Query Datos Transformacion a CUBO ----------------------------

CREATE TABLE DB_DWH_DESARROLLO.DATOS.TBL_TMP_MUESTRA_DATOS_CUBE_FINAL AS

SELECT TO_CHAR(RECORD_OPENING_TIME,'YYYY-MM-DD') RECORD_OPENING_TIME ,
APNNETWORK ,
PLMNIDENTIFIER ,
VAL_QCI,
SUM(UPLINK)  UPLINK,
SUM(DOWNLINK) DOWNLINK

FROM DB_DWH_DESARROLLO.DATOS.TBL_TMP_PRUEBA_DATOS_CUBE

GROUP BY TO_CHAR(RECORD_OPENING_TIME,'YYYY-MM-DD') ,
APNNETWORK ,
PLMNIDENTIFIER ,
VAL_QCI

------- Se crea la tabla Final ------------

CREATE TABLE DB_DWH_DESARROLLO.DATOS.DATOS_EPICA_10469_202204  (
	RECORD_OPENING_TIME VARCHAR(200),
	APNNETWORK VARCHAR(63),
	PLMNIDENTIFIER INTEGER,
	VAL_QCI BYTEINT,
	UPLINK NUMERIC(38,0),
	DOWNLINK NUMERIC(38,0)
);

-------------------- INSERTAR LOS DATOS ---------------------

INSERT INTO DB_DWH_DESARROLLO.DATOS.DATOS_EPICA_10469_202204
SELECT *
FROM DB_DWH_DESARROLLO.DATOS.TBL_TMP_MUESTRA_DATOS_CUBE_FINAL

----------------- ELIMINA TABLAS TEMPORALES ---------------------

DROP TABLE DB_DWH_DESARROLLO.DATOS.TBL_TMP_PRUEBA_DATOS_CUBE;
DROP TABLE DB_DWH_DESARROLLO.DATOS.TBL_TMP_MUESTRA_DATOS_CUBE_FINAL;

-----------------------------------------------------------------------------------------------------------------------
------------------------------------VOZ------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


---------------- QUERY VOZ EXTRACCION DE DATOS (TABLA TEMPORAL)---------------------------
SET CATALOG DB_DWH_DESARROLLO

CREATE TABLE DB_DWH_DESARROLLO.VOZ.TBL_TMP_VOZ_EPICA_10469_202204 AS

SELECT TO_CHAR(START_TIME,'YYYY-MM-DD') START_TIME,CAMEL_EXCHANGE_ID,CALLING_NUMBER ,CALLED_NUMBER ,IMSI,CALL_DURATION
FROM DB_DWH_VOZ.VOZ.TBL_FACT_VOZ_TRAFICO_202204


--------- Query Datos Transformacion a CUBO ----------------------------

CREATE TABLE DB_DWH_DESARROLLO.VOZ.TBL_TMP_VOZ_EPICA_10469_202204_FINAL AS

SELECT START_TIME,COUNT(DISTINCT CAMEL_EXCHANGE_ID) N_CAMEL_EXCHANGE_ID,COUNT(DISTINCT CALLING_NUMBER) N_CALLING_NUMBER ,COUNT(DISTINCT CALLED_NUMBER) N_CALLED_NUMBER ,
COUNT(DISTINCT IMSI) N_IMSI, SUM(CALL_DURATION) CALL_DURATION

FROM DB_DWH_DESARROLLO.VOZ.TBL_TMP_VOZ_EPICA_10469_202204

GROUP BY START_TIME

-------------- TABLA QA -------------

SET CATALOG  DB_DWH_VOZ

CREATE TABLE DB_DWH_VOZ.VOZ.VOZ_EPICA_10469_202204 (
	START_TIME VARCHAR(200),
	CAMEL_EXCHANGE_ID INTEGER,
	CALLING_NUMBER INTEGER,
	CALLED_NUMBER INTEGER,
	IMSI INTEGER,
	CALL_DURATION NUMERIC(38,0)
);

---------------------------------------------------------------------------------------------------
-------------------------VERSION 2-------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

SELECT V.FECHA,C.MSCID,C.CENTRAL,V.SK_OPERADOR,OP.OPERADOR,
SUM(V.SEGUNDOS) AS SEGUNDOS,SUM(V.MINUTOS_REDONDEADOS) AS MINUTOS_REDONDEADOS
FROM (SELECT FECHA,SK_OPERADOR,SEGUNDOS,MINUTOS_REDONDEADOS,SK_CENTRAL
FROM DB_DWH_HIST.VOZ.TBL_FACT_VOZ_TRAFICO_USU_202204
WHERE FECHA = 20220401) V
LEFT JOIN DWH_DB.VOZ.TBL_DIM_CENTRALES_T1 C ON C.SK_CENTRAL = V.SK_CENTRAL
LEFT JOIN DWH_DB.VOZ.TBL_DIM_OPERADORES_T0 OP ON OP.SK_OPERADOR = V.SK_OPERADOR
GROUP BY V.FECHA,C.MSCID,C.CENTRAL,V.SK_OPERADOR,OP.OPERADOR

---------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------
------------------------------------SMS------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------

-----------------QUERY SMS ---------------------------

SELECT *	
FROM DWH_DB.MDRS.SMSMMS
WHERE MESSAGE_SUBMITIONTIME  BETWEEN '2022-04-22 00:00:00.000' AND '2022-04-22 23:59:59.000'
LIMIT 200

--------------- QUERY PRM COMCEL JP_0080_DATOS_QCI ----------------
SELECT *
FROM PRM_COMCEL.dbo.PROCESOS
WHERE NOMBRE = 'JP_0080_DATOS_QCI'

select *
from PRM_COMCEL.dbo.procesos_parametros  
WHERE FK_PROCESO =42871 

SELECT *
FROM PRM_COMCEL.dbo.PROCESOS_OPERADORES
WHERE FK_PROCESO = 42871

SELECT *
FROM PRM_COMCEL.dbo.NOTIFICACIONES
WHERE FK_PROC_OPER  = 43509


----------------------QUERY VISTA -----------------------------------------------
CREATE OR REPLACE VIEW  desarrollo.vw_tbl_dim_tiempo
AS 

select date_key ,full_date ,day_name ,`month` ,month_name ,`year`,yearmo ,inicio_fin_semana,day_of_week,week_begin_date  
from drasdwh.tbl_dim_tiempo
where full_date BETWEEN  (select to_date((date_sub(current_date,90))))  and (select to_date((date_sub(current_date,1))))