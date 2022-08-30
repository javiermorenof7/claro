
name:="0080_DATOS_CARGUE_QCI"

version := "1.0"

scalaVersion := "2.11.8"
resolvers += Resolver.bintrayRepo("spark-packages", "maven")
val sparkVersion = "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"


libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.1.3.1.0.0-78" % "provided"
resolvers += ("Hortonworks repo" at "http://repo.hortonworks.com/content/repositories/releases/").withAllowInsecureProtocol(true)
libraryDependencies += "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.1.0.0-78" % "provided"

