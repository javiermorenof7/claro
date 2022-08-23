
name:="0080_DATOS_CARGUE_QCI"

version := "1.0"

scalaVersion := "2.11.8"
resolvers += Resolver.bintrayRepo("spark-packages", "maven")
val sparkVersion = "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
