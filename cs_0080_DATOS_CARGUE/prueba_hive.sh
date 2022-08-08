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


# Creacion tabla temporal No se utiliza este metodo 

TRUNCATE TABLE tmp_tbl_fact_datos_trafico_pruebas

$V_CONF_SPARK "INSERT INTO datos.tmp_tbl_fact_datos_trafico_Pruebas  Select cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci, SUM(uplink), SUM(DOWNLINK) from datos.tbl_fact_datos_trafico where fecha_trafico = $V_FECHA and plmnidentifier  NOT LIKE '732%' group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci"
$V_CONF_HIVE "select * from tmp_tbl_fact_datos_trafico_pruebas"

select *
from tmp_tbl_fact_datos_trafico_Pruebas

TRUNCATE TABLE tmp_tbl_fact_datos_trafico_pruebas

#Tabla fisica TBL_FACT_DATOS_QCI_PRUEBAS

CREATE TABLE `desarrollo.TBL_FACT_DATOS_QCI_PRUEBAS`(
--  `SK_FEC_TRAFICO` string, 
  `SK_APN` smallint, 
  `SK_PLMNIDENTIFIER` smallint, 
  `COD_QCI` smallint, 
  `VAL_BYTES_UPLINK` bigint, 
  `VAL_BYTES_DOWNLINK` bigint, 
  `VAL_BYTES_TOTAL` bigint, 
  `FEC_CARGA_DWH` string)
PARTITIONED BY ( 
  `SK_FEC_TRAFICO` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'last_modified_by'='anonymous', 
  'last_modified_time'='1654213180', 
  'transactional_properties'='default', 
  'transient_lastDdlTime'='1654213180')

#creacion consulta final TBL_FACT_DATOS_QCI
$V_CONF_HIVE "INSERT INTO DATOS.TBL_FACT_DATOS_QCI_PRUEBAS 
SELECT b.id_apn AS SK_APN,c.id_plmnidentifier AS SK_PLMNIDENTIFIER,a.val_qci AS SK_QCI,a.uplink AS VAL_BYTES_UPLINK,a.DOWNLINK AS VAL_BYTES_DOWNLINK,(COALESCE (a.uplink,0) + COALESCE (a.DOWNLINK,0)) AS VAL_BYTES_TOTAL ,CURRENT_DATE AS FEC_CARGA_DWH,a.record_opening_time AS SK_FEC_TRAFICO
FROM  (SELECT cast(record_opening_time as date) AS record_opening_time, apnnetwork, plmnidentifier, val_qci, SUM(uplink) AS uplink, SUM(DOWNLINK) AS DOWNLINK
	   FROM datos.tbl_fact_datos_trafico 
	   WHERE fecha_trafico = $V_FECHA and plmnidentifier  NOT LIKE '732%'AND (uplink  IS NOT NULL OR DOWNLINK IS NOT NULL) 
       GROUP BY cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci) AS a	   
LEFT JOIN DATOS.tbl_dim_apnnetwork_t1 b ON (upper(a.apnnetwork) = upper(b.apnnetwork))
LEFT JOIN DATOS.tbl_dim_plmnidentifier_t1 c ON (a.plmnidentifier) = (c.plmnidentifier)"

# CODIGO JAR
spark-submit --master yarn --deploy-mode client --class com.claro.App --num-executors 15 --executor-cores 15 --executor-memory 15g --queue 0080_0023_ACDRS --driver-memory 15g --conf spark.sql.shuffle.partitions=1000 --conf spark.default.parallelism=1000 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.executor.memoryOverhead=1g --conf spark.driver.maxResultSize=15g --conf spark.sql.hive.hiveserver2.jdbc.url="jbdc:hive2://tfm2036-hdpcmtr02.claro.co:2181,tfm2044-hdpcmtr03.claro.co:2181,tfm2403-hdpcmtr04.claro.co:2181,tfm2404-hdpcmtr05.claro.co:2181,tfm2405-hdpcmtr06.claro.co:2181,tfm1913-hdpcmtr01.claro.co:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" "/home/dwhdespro/DESARROLLO_DWH/02_DATOS/Pruebas_EP10469/Prueba-assembly-0.1.0-SNAPSHOT.jar"


# CODIGO JAR
spark-submit --master yarn --deploy-mode client --class com.claro.App --num-executors 1 --executor-cores 1 --executor-memory 1g --queue OTROS --driver-memory 1g --conf spark.sql.shuffle.partitions=1000 --conf spark.default.parallelism=1000 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.executor.memoryOverhead=1g --conf spark.driver.maxResultSize=1g --conf spark.sql.hive.hiveserver2.jdbc.url="jbdc:hive2://tfm2036-hdpcmtr02.claro.co:2181,tfm2044-hdpcmtr03.claro.co:2181,tfm2403-hdpcmtr04.claro.co:2181,tfm2404-hdpcmtr05.claro.co:2181,tfm2405-hdpcmtr06.claro.co:2181,tfm1913-hdpcmtr01.claro.co:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" "/home/dwhdespro/Prueba-assembly-0.1.0-SNAPSHOT.jar"

# Actualizar dimension TBL_ADMON_TABLAS_BDC

INSERT INTO TBL_ADMON_TABLAS_BDC 
(TECNOLOGIA_ORIGEN, TECNOLOGIA_DESTINO, BD_ORIGEN, BD_DESTINO, ESQUEMA_ORIGEN, ESQUEMA_DESTINO, HORA_EJECUCION, VAL_CANTIDAD_FUENTE, VAL_CANTIDAD_DESTINO, TABLA_ORIGEN, TABLA_DESTINO, CAMPOS_TABLE, CAMPOS_INDEX, HORA_INI, HORA_FIN, DES_PROCESO, REGLA_CALIDAD, COD_TRUNCATE, CONDICION_SELECT, CONDICION_DELETE, FEC_CARGA_DWH, FEC_ACTUALIZACION) 
VALUES ('NETEZZA','HIVE','DWH_DB','desarrollo','DATOS','desarrollo',14,0,0,'TBL_DIM_APNNETWORK_T1','tbl_dim_apnnetwork_t1_pruebas','ID_APN,APNNETWORK,FECHA_ACTUALIZACION',
'ID_APN','', '','10469_DATOS_APNNETWORK_T1_PRUEBAS','%','TRUE','','','','');

# Actualizar dimension TBL_ADMON_TABLAS_BDC Producción

INSERT INTO TBL_ADMON_TABLAS_BDC 
(TECNOLOGIA_ORIGEN, TECNOLOGIA_DESTINO, BD_ORIGEN, BD_DESTINO, ESQUEMA_ORIGEN, ESQUEMA_DESTINO, HORA_EJECUCION, VAL_CANTIDAD_FUENTE, VAL_CANTIDAD_DESTINO, TABLA_ORIGEN, TABLA_DESTINO, CAMPOS_TABLE, CAMPOS_INDEX, HORA_INI, HORA_FIN, DES_PROCESO, REGLA_CALIDAD, COD_TRUNCATE, CONDICION_SELECT, CONDICION_DELETE, FEC_CARGA_DWH, FEC_ACTUALIZACION) 
VALUES ('NETEZZA','HIVE','DWH_DB','DATOS','DATOS','DATOS',13,0,0,'TBL_DIM_APNNETWORK_T1','TBL_DIM_APNNETWORK_T1','ID_APN,APNNETWORK,FECHA_ACTUALIZACION',
'ID_APN','', '','0080_DATOS_TRANF_APNNETWORK_T1','Porcentaje_Mayor_Destino 80%','TRUE','','','','');

# Actualizar dimension tbl_dim_plmnidentifier_t1 Producción

INSERT INTO TBL_ADMON_TABLAS_BDC 
(TECNOLOGIA_ORIGEN, TECNOLOGIA_DESTINO, BD_ORIGEN, BD_DESTINO, ESQUEMA_ORIGEN, ESQUEMA_DESTINO, HORA_EJECUCION, VAL_CANTIDAD_FUENTE, VAL_CANTIDAD_DESTINO, TABLA_ORIGEN, TABLA_DESTINO, CAMPOS_TABLE, CAMPOS_INDEX, HORA_INI, HORA_FIN, DES_PROCESO, REGLA_CALIDAD, COD_TRUNCATE, CONDICION_SELECT, CONDICION_DELETE, FEC_CARGA_DWH, FEC_ACTUALIZACION) 
VALUES ('NETEZZA','HIVE','DWH_DB','DATOS','DATOS','DATOS',13,0,0,'tbl_dim_plmnidentifier_t1','tbl_dim_plmnidentifier_t1','ID_PLMNIDENTIFIER,PLMNIDENTIFIER,DESCRIPCION_PLMNIDENTIFIER,FECHA_ACTUALIZACION ',
'ID_PLMNIDENTIFIER','', '','0080_DATOS_TRANF_plmnidentifier_t1','Porcentaje_Mayor_Destino 80%','TRUE','','','','');
