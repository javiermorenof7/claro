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

--------- Query Datos ----------------------------

SELECT TO_CHAR(RECORD_OPENING_TIME,'DD/MM/YYYY') RECORD_OPENING_TIME ,
APNNETWORK ,
PLMNIDENTIFIER ,
VAL_QCI,
SUM(UPLINK) ,
SUM(DOWNLINK)

FROM (
        Select *
        From (
        
        SELECT
        RECORD_OPENING_TIME ,
        APNNETWORK ,
        PLMNIDENTIFIER ,
        UPLINK ,
        DOWNLINK,
        VAL_QCI
        FROM DB_DWH_DATOS.DATOS.TBL_FACT_DATOS_TRAFICO_202204
        WHERE RECORD_OPENING_TIME LIKE '%2022-04-22%' and PLMNIDENTIFIER NOT in(732123,732101, 73210, 74001, 310260, 73404, 71610, 
        21401, 71403, 74000, 334020, 37002, 73003, 310410, 71606, 722310, 36301, 71203, 28602, 302720 ) 
        
        LIMIT 1000000
        ) AS A
    
        UNION ALL
        
        Select *
        From (
            
        SELECT
        RECORD_OPENING_TIME ,
        APNNETWORK ,
        PLMNIDENTIFIER ,
        UPLINK ,
        DOWNLINK,
        VAL_QCI
        FROM DB_DWH_DATOS.DATOS.TBL_FACT_DATOS_TRAFICO_202204
        WHERE RECORD_OPENING_TIME LIKE '%2022-04-23%' and PLMNIDENTIFIER NOT in(732123,732101, 73210, 74001, 310260, 73404, 71610, 
        21401, 71403, 74000, 334020, 37002, 73003, 310410, 71606, 722310, 36301, 71203, 28602, 302720 ) 
        
        LIMIT 1000000
        ) as A
        
) AS A
GROUP BY TO_CHAR(RECORD_OPENING_TIME,'DD/MM/YYYY') ,
APNNETWORK ,
PLMNIDENTIFIER ,
VAL_QCI

----------------QUERY MUESTRA ---------------------------------------------

 SSELECT * 
FROM (
SELECT *
FROM DB_DWH_DATOS.DATOS.TBL_FACT_DATOS_TRAFICO_202204 
LIMIT 1000000000
) AS A
WHERE PLMNIDENTIFIER NOT LIKE '732%'AND UPLINK IS NOT NULL 
AND DOWNLINK IS NOT NULL AND VAL_QCI IS NOT NULL

-----------------------------------------------------------------------------------------------------------------------
------------------------------------VOZ------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


---------------- QUERY VOZ ---------------------------

SELECT to_date(START_TIME,'YYYY-MM-DD HH24:MI:SS'),CAMEL_EXCHANGE_ID,CALLING_NUMBER ,CALLED_NUMBER ,IMSI,CALL_DURATION
FROM DB_DWH_VOZ.VOZ.TBL_FACT_VOZ_TRAFICO_202204
WHERE START_TIME BETWEEN '2022-04-22 00:00:00.000' AND '2022-04-22 23:59:59.000'
LIMIT 200

--Creacion de vista 

set catalog db_dwh_desarrollo
CREATE OR REPLACE VIEW VW_PRUEBAS_VOZ AS
SELECT START_TIME,CAMEL_EXCHANGE_ID,CALLING_NUMBER ,CALLED_NUMBER ,IMSI,CALL_DURATION
FROM DB_DWH_VOZ.VOZ.TBL_FACT_VOZ_TRAFICO_202204
WHERE START_TIME  BETWEEN '2022-04-22 00:00:00.000' AND '2022-04-22 00:00:00.000';

--Creacion tabla temporal 

CREATE TEMPORARY  TABLE IF NOT EXISTS DB_DWH_DESARROLLO.VOZ.TMP_PRUEBAS_VOZ (
CAMEL_EXCHANGE_ID VARCHAR(18),
CALLING_NUMBER VARCHAR(20),
CALLED_NUMBER VARCHAR(24),
CALL_DURATION INTEGER,
START_TIME TIMESTAMP,
IMSI VARCHAR(16) );


INSERT INTO DB_DWH_DESARROLLO.VOZ.TMP_PRUEBAS_VOZ
SELECT START_TIME,CAMEL_EXCHANGE_ID,CALLING_NUMBER ,CALLED_NUMBER ,IMSI,CALL_DURATION
FROM DB_DWH_VOZ.VOZ.TBL_FACT_VOZ_TRAFICO_202204
WHERE START_TIME  BETWEEN '2022-04-22 00:00:00.000' AND '2022-04-22 00:00:00.000';

SELECT *
FROM DB_DWH_DESARROLLO.VOZ.TMP_PRUEBAS_VOZ


-----------------------------------------------------------------------------------------------------------------------
------------------------------------SMS------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


-----------------QUERY SMS ---------------------------

SELECT *	
FROM DWH_DB.MDRS.SMSMMS
WHERE MESSAGE_SUBMITIONTIME  BETWEEN '2022-04-22 00:00:00.000' AND '2022-04-22 23:59:59.000'
LIMIT 200