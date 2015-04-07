import com.typesafe.config._
import gsn.data.discovery.PropertiesManager
import java.io.File

object Main extends App {
//  require(args.length >= 2, help())
  
  val conf = ConfigFactory.load("application.conf")
  val propertiesMgr = new PropertiesManager(
      conf.getString("fusekiEndpoints.properties"),
      conf.getString("fusekiEndpoints.mappings"),
      conf.getString("fusekiEndpoints.virtualSensors"),
      conf.getString("baseUri"))
  val propertyMappingsMgr = new PropertyMappingsManager(propertiesMgr, conf.getString("baseUri"))
    
//  val mode = args(0)
  val mode = "-add-new-virtual-sensor"
  mode match {
    case "-add-new-mappings" =>
//      propertyMappingsMgr.importMappingsFromCsv(new File(args(1)))
      propertyMappingsMgr.importMappingsFromCsv(new File("/home/michael/dev/semester_project/mappings.csv"))
    case "-add-new-virtual-sensor" => 
//      propertyMappingsMgr.addVirtualSensorFromJSON(new File(args(1)))
      propertyMappingsMgr.addVirtualSensorFromJSON(new File("/home/michael/dev/semester_project/JSON_virtual_sensor_input.txt"))
    case "-help" | "-h" | _ => help()
  }
  
  private def help() {
    println("Various arguments needs to be provided.")
    println("Operation modes:")
    println("0 | addNewMappings csvFilePath: add new mappings to mappings model on Fuseki")
    println("1 | addNewVirtualSensor: add a new virtual sensor to virtual-sensors model on Fuseki")
  }
}