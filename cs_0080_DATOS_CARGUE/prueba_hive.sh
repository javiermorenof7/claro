#!/bin/bash

echo "********************************** INICIO DEL PROCESO: $(date) *********************************"
echo ""

#Define variables por defecto
V_ESQUEMA_ORIGINAL="datos"
V_ESQUEMA_TEMPORAL="prueba"
V_PARTICION=$1
V_NOMBRE_PARTICION="fecha_trafico"
file="/hadoop/proyectos/adecuacion_cdrs/prod/0080_DATOS_CARGUE_TRAFICO/prm_concatenar_DATOS.txt"
V_SPARK_WAREHOUSE_DIR="/apps/spark/warehouse"
V_CONF_SPARK="spark-sql --driver-memory 15G --executor-memory 15G --executor-cores 9 --num-executors 15 --queue 0080_0023_ACDRS --name '0080_DATOS_CONCATENAR' -e "
V_0080_DATOS_CARGA_TABLAS='tbl_fact_datos_trafico,tbl_datos_control_cargue'


spark-sql --driver-memory 15G --executor-memory 15G --executor-cores 15 --num-executors 15 --queue 0080_0023_ACDRS --name '0080_DATOS_CONCATENAR' -e "Select cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci, SUM(uplink), SUM(DOWNLINK) from datos.tbl_fact_datos_trafico where fecha_trafico = '20220302' and plmnidentifier  NOT LIKE '732%'AND uplink  IS NOT NULL  AND downlink  IS NOT NULL AND val_qci  IS NOT NULL group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci " 


--------------query con fecha dia anterior
Select 
cast(record_opening_time as date) as SK_FEC_TRAFICO,
apnnetwork as SK_APN,
plmnidentifier as SK_PLMNIDENTIFIER,
val_qci as SK_QCI, 
SUM(uplink) as VAL_BYTES_UPLINK,
SUM(DOWNLINK) as VAL_BYTES_DOWNLINK
from datos.tbl_fact_datos_trafico 
where fecha_trafico = regexp_replace(to_date((date_sub(current_date, 1))),'-','') 
and plmnidentifier  NOT LIKE '732%'
AND uplink  IS NOT NULL 
AND downlink  IS NOT NULL 
AND val_qci IS NOT NULL 
group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci

spark-sql --driver-memory 15G --executor-memory 15G --executor-cores 15 --num-executors 15 --queue 0080_0023_ACDRS --name '0080_DATOS_CONCATENAR' -e "Select cast(record_opening_time as date) as SK_FEC_TRAFICO, apnnetwork as SK_APN, plmnidentifier as SK_PLMNIDENTIFIER, val_qci as SK_QCI, SUM(uplink) as VAL_BYTES_UPLINK,
SUM(DOWNLINK) as VAL_BYTES_DOWNLINK from datos.tbl_fact_datos_trafico where fecha_trafico = regexp_replace(to_date((date_sub(current_date, 1))),'-','') and plmnidentifier  NOT LIKE '732%' and uplink  IS NOT NULL and downlink  IS NOT NULL and val_qci IS NOT NULL group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci"



# Creacion de consulta utilizando variables 

V_CONF_HIVE="hive --driver-memory 15G --executor-memory 15G --executor-cores 9 --num-executors 15 --queue 0080_0023_ACDRS  --name '0080_DATOS_CONCATENAR' -e "
V_CONF_SPARK="spark-sql  --driver-memory 15G --executor-memory 15G --executor-cores 9 --num-executors 15 --queue 0080_0023_ACDRS  --name '0080_DATOS_CONCATENAR' -e "
V_FECHA="regexp_replace(to_date((date_sub(current_date, 1))),'-','') "
echo $V_FECHA
$V_CONF_SPARK "Select cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci, SUM(uplink), SUM(DOWNLINK) from datos.tbl_fact_datos_trafico where fecha_trafico = $V_FECHA and plmnidentifier  NOT LIKE '732%' group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci"

#tbl_dim_plmnidentifier_t1
$V_CONF_HIVE "Select ID_PLMNIDENTIFIER, PLMNIDENTIFIER, FECHA_ACTUALIZACION from datos.tbl_dim_plmnidentifier_t1"
#tbl_dim_apnnetwork_t1
$V_CONF_HIVE "Select ID_APN ,APNNETWORK,FECHA_ACTUALIZACION from datos.tbl_dim_apnnetwork_t1"


# Creacion tabla temporal 

TRUNCATE TABLE tmp_tbl_fact_datos_trafico_pruebas

$V_CONF_SPARK "INSERT INTO datos.tmp_tbl_fact_datos_trafico_Pruebas  Select cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci, SUM(uplink), SUM(DOWNLINK) from datos.tbl_fact_datos_trafico where fecha_trafico = $V_FECHA and plmnidentifier  NOT LIKE '732%' group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci"

$V_CONF_SPARK "Select cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci, SUM(uplink), SUM(DOWNLINK) INTO #tmp_tbl_fact_datos_trafico_Pruebas from datos.tbl_fact_datos_trafico where fecha_trafico = $V_FECHA and plmnidentifier  NOT LIKE '732%' group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci"


select *
from tmp_tbl_fact_datos_trafico_Pruebas


TRUNCATE TABLE tmp_tbl_fact_datos_trafico_pruebas

