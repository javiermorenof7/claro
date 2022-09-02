package com.claro

import java.text.SimpleDateFormat
import java.util.Date

import Processors.{sisnot_processor}
import Utils.notifications.{error_proceso, fin_proceso, inicio_proceso, log}
import Utils.parameters.{getOptionMovilesMap}
import com.hortonworks.hwc.HiveWarehouseSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext, SparkSession}

import scala.collection.mutable

object App {

    def main (args : Array[String]) = {
    
    //===============================================
    //Set level log Error
    Logger.getLogger("org").setLevel(Level.ERROR)
    //===============================================
    
    //Read sh Parameters
    log("Read SH Parameters")
    val parametros = getOptionMovilesMap(mutable.HashMap.empty[Symbol, String], args.toList)
    val APP_NAME: String = parametros.get('app_name).get.toString
    val fecha_sh = parametros.get('fecha_proceso).get
    val job_name: String = parametros.get('job_name).get.toString
    val sisnot_repositorio: String = parametros.get('sisnot_repositorio).get.toString
    val ambiente_sisnot_variables: String = parametros.get('ambiente_sisnot_variables).get.toString
    val ambiente_sisnot_notificaciones: String = parametros.get('ambiente_sisnot_notificaciones).get.toString
    val path_fuentes: String = parametros.get('path_fuentes).get.toString
    val path_extrae_pr: String = parametros.get('path_extrae_pr).get.toString
    val path_ejecuta_escenario: String = parametros.get('path_ejecuta_escenario).get.toString
    val ext: String = parametros.get('ext).get.toString
    val temp_folder: String = parametros.get('temp_folder).get.toString
    val id_ejecucion: String = parametros.get('id_ejecucion).get.toString
    val database: String = parametros.get('database).get.toString
    val repo_sisnot = ambiente_sisnot_notificaciones + " " + sisnot_repositorio
    val V_DIAS_ATRASO: String = parametros.get('V_DIAS_ATRASO).get.toString
    
    val v_fecha="""regexp_replace(to_date((date_sub(current_date,""" + V_DIAS_ATRASO + """))),'-','') """

    
    log(APP_NAME + " Start Process Spark")
    log("id_ejecucion: " + id_ejecucion)
    val spark_configuration = new SparkConf().setAppName(APP_NAME)
    val ss = SparkSession.builder()
      .config(spark_configuration)
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .getOrCreate()

     try {

      inicio_proceso(path_ejecuta_escenario, repo_sisnot, job_name)
      //=================================================================================
      //=====Validar si ya se encuentran registros para la fecha de proceso============== 
      //=================================================================================
       val CantidadReg=ss.sql("""SELECT * from  desarrollo.tbl_fact_dato_qci
                              WHERE sk_fec_trafico =""" + v_fecha )

       if (CantidadReg.count() == 0) {

       val APN_DF= ss.sql("""SELECT id_apn, UPPER (apnnetwork) apnnetwork, fecha_actualizacion 
                          FROM """ + database + """.tbl_dim_apnnetwork_t1""") 
      //=================================================================================
      //=====Validar si las dimensiones contienen registros============================== 
      //=================================================================================
       if (APN_DF.count() == 0) {
          println("+\n ALARMA: No se encuentra información en la tabla tbl_dim_apnnetwork_t1 \n+")
          sys.exit(0)
       }

       val PLMNI_DF= ss.sql("""SELECT ID_PLMNIDENTIFIER,PLMNIDENTIFIER,FECHA_ACTUALIZACION 
                          FROM  """ + database + """.tbl_dim_plmnidentifier_t1""") 

       if (PLMNI_DF.count() == 0) {
          println("+\n ALARMA: No se encuentra información en la tabla tbl_dim_plmnidentifier_t1 \n+")
          sys.exit(0)
       }

      val DatosTrafDF = ss.sql("""SELECT  UPPER (apnnetwork) apnnetwork, plmnidentifier, val_qci VAL_QCI, SUM(uplink) VAL_BYTES_UPLINK, SUM(DOWNLINK) VAL_BYTES_DOWNLINK, ( COALESCE(SUM(uplink),0) + COALESCE(SUM(DOWNLINK),0)) VAL_BYTES_TOTAL,CURRENT_DATE AS FEC_CARGA_DWH ,fecha_trafico SK_FEC_TRAFICO
                          FROM """ + database + """.tbl_fact_datos_trafico 
                          WHERE fecha_trafico = """ + v_fecha + """ and plmnidentifier  NOT LIKE '732%' AND (uplink  IS NOT NULL OR DOWNLINK IS NOT NULL) 
                          group by fecha_trafico, apnnetwork, plmnidentifier, val_qci """)

       DatosTrafDF.createOrReplaceTempView("DatosTraf")
       APN_DF.createOrReplaceTempView("APN")
       
       val UNION1_DF = ss.sql("""SELECT A.apnnetwork,b.id_apn, A.plmnidentifier,A.VAL_QCI,A.VAL_BYTES_UPLINK,A.VAL_BYTES_DOWNLINK,A.VAL_BYTES_TOTAL,A.FEC_CARGA_DWH ,A.SK_FEC_TRAFICO
                                FROM DatosTraf A 
                                LEFT JOIN APN  B ON A.apnnetwork = B.apnnetwork """)
       
       val UNION2_DF = UNION1_DF.join(PLMNI_DF,UNION1_DF("plmnidentifier") === PLMNI_DF("plmnidentifier"),"left").drop("DESCRIPCION_PLMNIDENTIFIER").drop("FECHA_ACTUALIZACION").drop("PLMNIDENTIFIER").drop("apnnetwork")
       UNION2_DF.createOrReplaceTempView("UNION2")

       val DF_FINAL = ss.sql("""INSERT Into desarrollo.tbl_fact_dato_qci
                                SELECT id_apn, ID_PLMNIDENTIFIER ,VAL_QCI,VAL_BYTES_UPLINK,VAL_BYTES_DOWNLINK,VAL_BYTES_TOTAL,FEC_CARGA_DWH ,SK_FEC_TRAFICO 
                                FROM UNION2""")

      }
      else {
        println("+\n ALARMA: Se encuentra información para la fecha de trafico a procesar \n+")
      }

      fin_proceso(path_ejecuta_escenario, repo_sisnot, job_name)
      log(APP_NAME + " End Process Spark")

      println("+\n+\n+\n+\n Ejecución Finalizada.jar \n+\n+\n+\n+")
      println(java.time.LocalDate.now)
   
     }

    catch {
      case e: Throwable => log(e.getMessage)
        val des_error = "ERROR: Se presenta excepcion al ejecutar proceso: " + e.getMessage
        error_proceso(path_ejecuta_escenario, repo_sisnot, job_name, des_error)
    }
        
  }
  }
