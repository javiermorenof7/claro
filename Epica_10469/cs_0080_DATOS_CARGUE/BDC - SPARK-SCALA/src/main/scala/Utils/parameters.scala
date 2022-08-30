package Utils

import scala.collection.mutable

object parameters {

  /*
  Name: getOptionMovilesKPIsMap
  Description: Función encargada de obtener los parametros enviados por el SH en el spark Submit
  */

  def getOptionMovilesMap(map : mutable.Map[Symbol, String], list: List[String]) : mutable.Map[Symbol, String] = {
    list match {
      case Nil => map
      case "--app_name" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('app_name -> value.trim), tail)
      case "--job_name" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('job_name -> value.trim), tail)
      case "--sisnot_repositorio" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('sisnot_repositorio -> value.trim), tail)
      case "--ambiente_sisnot_variables" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('ambiente_sisnot_variables -> value.trim), tail)
      case "--ambiente_sisnot_notificaciones" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('ambiente_sisnot_notificaciones -> value.trim), tail)
      case "--ext" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('ext -> value.trim), tail)
      case "--path_extrae_pr" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('path_extrae_pr -> value.trim), tail)
      case "--path_ejecuta_escenario" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('path_ejecuta_escenario -> value.trim), tail)
      case "--temp_folder" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('temp_folder -> value.trim), tail)
      case "--path_fuentes" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('path_fuentes -> value.trim), tail)
      case "--fecha_proceso" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('fecha_proceso -> value.trim), tail)
      case "--id_ejecucion" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('id_ejecucion -> value.trim), tail)
      case "--database" :: value :: tail => getOptionMovilesMap(map ++ mutable.HashMap('database -> value.trim), tail)
      case option :: _ => println("Unknown option " + option)
        sys.exit(1)
    }
  }

}