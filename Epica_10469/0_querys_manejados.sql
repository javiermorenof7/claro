------QUERYS Ayuda----------------------------

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