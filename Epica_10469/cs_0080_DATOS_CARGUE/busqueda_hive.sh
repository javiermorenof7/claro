rm -f /DWH/01_VOZ/CONCILIA_GPRS_HADOOP/*.txt

echo "
SELECT *
FROM datos.tbl_fact_datos_trafico 
where fecha_trafico ='20220701'
limit 10; "> /DWH/01_VOZ/CONCILIA_GPRS_HADOOP/PRUEBA.txt

hive -f /DWH/01_VOZ/CONCILIA_GPRS_HADOOP/PRUEBA.txt

consulta="SELECT *
FROM datos.tbl_fact_datos_trafico 
where fecha_trafico ='20220701'
limit 100000;"

hive -e "$consulta" >/DWH/01_VOZ/CONCILIA_GPRS_HADOOP/PRUEBA_2.txt

echo "Finaliza conciliacion"