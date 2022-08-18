package com.claro

import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext, SparkSession}


object App {

    def main (args : Array[String]) = {

    val APP_NAME = "0000_TEST_CODE"

    val conf = new SparkConf().setAppName(APP_NAME)

    val ss = SparkSession
            .builder()
            .config(conf)
            .config("hive.exec.dynamic.partition","true")
            .config("hive.exec.dynamic.partition.mode","nonstrict")
            .config("spark.sql.codegen.wholeStage","false")
            .enableHiveSupport()
            .getOrCreate() 

       //val v_fecha="regexp_replace(to_date((date_sub(current_date, 1))),'-','') "
       val v_fecha=20220810
     
       val DatosTrafDF = ss.sql("""Select  UPPER (apnnetwork) apnnetwork, plmnidentifier, val_qci VAL_QCI, SUM(uplink) VAL_BYTES_UPLINK, SUM(DOWNLINK) VAL_BYTES_DOWNLINK, ( COALESCE(SUM(uplink),0) + COALESCE(SUM(DOWNLINK),0)) VAL_BYTES_TOTAL,CURRENT_DATE AS FEC_CARGA_DWH ,cast(record_opening_time as date) SK_FEC_TRAFICO
                          from datos.tbl_fact_datos_trafico 
                          where fecha_trafico = """ + v_fecha + """ and plmnidentifier  NOT LIKE '732%' AND (uplink  IS NOT NULL OR DOWNLINK IS NOT NULL) 
                          group by cast(record_opening_time as date), apnnetwork, plmnidentifier, val_qci """)
        

       val APN_DF= ss.sql("SELECT id_apn, UPPER (apnnetwork) apnnetwork, fecha_actualizacion from desarrollo.tmp_tbl_dim_apnnetwork_t1") 
       val PLMNI_DF= ss.sql("SELECT ID_PLMNIDENTIFIER,PLMNIDENTIFIER,DESCRIPCION_PLMNIDENTIFIER,FECHA_ACTUALIZACION FROM desarrollo.tmp_tbl_dim_plmnidentifier_t1") 
       
       DatosTrafDF.createOrReplaceTempView("DatosTraf")
       APN_DF.createOrReplaceTempView("APN")
       
       val UNION1_DF = ss.sql("""SELECT A.apnnetwork,b.id_apn, A.plmnidentifier,A.VAL_QCI,A.VAL_BYTES_UPLINK,A.VAL_BYTES_DOWNLINK,A.VAL_BYTES_TOTAL,A.FEC_CARGA_DWH ,A.SK_FEC_TRAFICO
                                FROM DatosTraf A 
                                LEFT JOIN APN  B ON A.apnnetwork = B.apnnetwork """)
       
       val UNION2_DF = UNION1_DF.join(PLMNI_DF,UNION1_DF("plmnidentifier") === PLMNI_DF("plmnidentifier"),"left").drop("DESCRIPCION_PLMNIDENTIFIER").drop("FECHA_ACTUALIZACION").drop("PLMNIDENTIFIER").drop("apnnetwork")
       //UNION2_DF.show(50,false)
          
       UNION2_DF.write.saveAsTable("desarrollo.Tmp_TBL_FACT_DATOS_QCI_PRUEBAS4") // si no existe la tabla la crea
       //DatosTrafDF.write.insertInto("desarrollo.Tmp_TBL_FACT_DATOS_QCI_PRUEBAS") // insertar datos en tabla existente


    println("+\n+\n+\n+\n Pruebas de Ejecucion .jar \n+\n+\n+\n+")
    println(java.time.LocalDate.now)

  }
  }
