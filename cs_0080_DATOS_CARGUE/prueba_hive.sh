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


spark-sql --driver-memory 15G --executor-memory 15G --executor-cores 15 --num-executors 15 --queue 0080_0023_ACDRS --name '0080_DATOS_CONCATENAR' -e "Select cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci, SUM(uplink), SUM(DOWNLINK) from datos.tbl_fact_datos_trafico where fecha_trafico = '20220302' and plmnidentifier  NOT LIKE '732%'AND uplink  IS NOT NULL  AND downlink  IS NOT NULL AND val_qci  IS NOT NULL group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci LIMIT 1" 


V_FECHA="CAST(current_date- interval 1 day  as date)"
echo $V_FECHA
spark-sql --driver-memory 15G --executor-memory 15G --executor-cores 15 --num-executors 15 --queue 0080_0023_ACDRS --name '0080_DATOS_CONCATENAR' -e "Select cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci, SUM(uplink), SUM(DOWNLINK) from datos.tbl_fact_datos_trafico where fecha_trafico = $V_FECHA and plmnidentifier  NOT LIKE '732%' group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci"