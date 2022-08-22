#!/bin/bash
#/***************************************
# *PROCESO: 0080_DATOS_CARGUE_QCI
# *DESCRIPCION: Proceso para consolidación de datos cargue QCI fact tbl_fact_datos_qci
# *Fecha creacion: 2022-08-22
# *Autor: Factor It - Juan Pablo Pereira 
# *Número del EPICA 10469
# ***************************************/
#****************************
#Inicio
#****************************
clear
clear
clear
clear
START=$(date +%s)
SCRIPT=$(readlink -f $0);
path_proceso=`dirname $SCRIPT`;
ext="txt"
temp_folder="TEMPORAL"
id_ejecucion=${1}
#****************************
#Configuración Aplicacion
#****************************
job_name="JP_0080_DATOS_QCI"
app_name="0166_DATOS_TRAFICO_MEDIADO"
main_class="com.claro.App"
jar_name="0080_DATOS_CARGUE_QCI-assembly-0.1.0-SNAPSHOT.jar"
queue="OTROS"
#queue="PROCESOS_DIARIOS"
path_fuentes="/DWH/DESARROLLO_DWH/02_DATOS/0080_DATOS_CARGUE_QCI/03_FUENTES"
echo "Inicio de proceso "${job_name}
#****************************
#Configuración Recursos
#****************************
executors="15"
cores="15"
executorMemory="15g"
driverMemory="15g"
#****************************
#Configuración Sisnot
#****************************
sisnot_repositorio="WRDEV_DESARROLLO_PROCALIDAD"
#sisnot_repositorio="WRPROD_PRODUCTIVO"
ambiente_sisnot_variables="qa"
#ambiente_sisnot_variables="pr"
ambiente_sisnot_notificaciones="dev"
#ambiente_sisnot_notificaciones="pr"
path_extrae_pr="/DWH/99_ADMINISTRACION/0091_ADMON_SISNOT/02_BATS/0091_extrae_parametros_prm_comcel.jar"
path_ejecuta_escenario="/DWH/99_ADMINISTRACION/0091_ADMON_SISNOT/02_BATS/0091_ejecuta_escenario_sisnot.jar"
path_log=${path_fuentes}/${temp_folder}/${app_name}_`date +\%Y\%m\%d\%H\%M`.log
#****************************
#Configuración Fecha Proceso
#****************************
fecha=$(date -d "yesterday 13:00" '+%Y-%m-%d %H:%M:%S')
fecha_YYYYMMDD="$(date -d "${fecha}" +'%Y%m%d')"
fecha_YYYYMMDD="20210819"
echo "Ruta proceso: "${path_proceso}
echo "Fecha proceso: "${fecha_YYYYMMDD}
#****************************
#Spark Submit
#****************************
spark-submit --queue ${queue} --master yarn --class ${main_class} --num-executors ${executors} --executor-cores ${cores} --executor-memory ${executorMemory} --driver-memory ${driverMemory} --conf spark.port.maxRetries=100 --conf spark.executor.memoryOverhead=24336 --conf spark.driver.maxResultSize=4g --conf spark.default.parallelism=3000 --conf spark.sql.shuffle.partitions=3000 --conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://tfm2044-hdpcmtr03.claro.co:2181,tfm2403-hdpcmtr04.claro.co:2181,tfm2404-hdpcmtr05.claro.co:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --conf spark.datasource.hive.warehouse.metastoreUri=thrift://tfm2403-hdpcmtr04.claro.co:9083,thrift://tfm2044-hdpcmtr03.claro.co:9083 --conf spark.hadoop.hive.llap.daemon.service.hosts=@llap0 --conf spark.hadoop.hive.zookeeper.quorum=tfm2044-hdpcmtr03.claro.co:2181,tfm2403-hdpcmtr04.claro.co:2181,tfm2404-hdpcmtr05.claro.co:2181 --conf spark.hadoop.metastore.catalog.default=hive  --conf spark.datasource.hive.warehouse.load.staging.dir=/tmp --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.ui.port=4049 "/DWH/DESARROLLO_DWH/02_DATOS/0080_DATOS_CARGUE_QCI/02_BATS/0080_DATOS_CARGUE_QCI-assembly-0.1.0-SNAPSHOT.jar" --app_name ${app_name} --job_name ${job_name} --path_extrae_pr ${path_extrae_pr} --path_ejecuta_escenario ${path_ejecuta_escenario} --ext ${ext} --temp_folder ${temp_folder} --sisnot_repositorio ${sisnot_repositorio} --ambiente_sisnot_variables ${ambiente_sisnot_variables} --ambiente_sisnot_notificaciones ${ambiente_sisnot_notificaciones} --path_fuentes ${path_fuentes} --fecha_proceso ${fecha_YYYYMMDD} --id_ejecucion ${id_ejecucion} #>> ${path_log}


END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Proceso ejecutado en " ${DIFF}
#****************************
#Fin
#****************************
