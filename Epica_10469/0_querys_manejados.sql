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
        WHERE RECORD_OPENING_TIME LIKE '%2022-04-22%' and PLMNIDENTIFIER != 732101
        
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
        WHERE RECORD_OPENING_TIME LIKE '%2022-04-23%' and PLMNIDENTIFIER != 732101
        
        LIMIT 1000000
        ) as A
        
) AS A
GROUP BY TO_CHAR(RECORD_OPENING_TIME,'DD/MM/YYYY') ,
APNNETWORK ,
PLMNIDENTIFIER ,
VAL_QCI

---------------- QUERY VOZ ---------------------------

SELECT to_date(START_TIME,'YYYY-MM-DD HH24:MI:SS'),CAMEL_EXCHANGE_ID,CALLING_NUMBER ,CALLED_NUMBER ,IMSI,CALL_DURATION
FROM DB_DWH_VOZ.VOZ.TBL_FACT_VOZ_TRAFICO_202204
WHERE START_TIME = '2022-04-22'

-----------------QUERY SMS ---------------------------

