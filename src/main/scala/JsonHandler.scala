import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object JsonHandler extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .config("spark.master", "local")
    .config("spark.logConf", "true")
    .enableHiveSupport()
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("INFO")
  spark.sparkContext.setLogLevel("ERROR")

  val schema = new StructType()
    .add("SNo", IntegerType, true)
    .add("ObservationDate", StringType, true)
    .add("Province/State", StringType, true)
    .add("Country/Region", StringType, true)
    .add("Last Update", StringType, true)
    .add("Confirmed", IntegerType, true)
    .add("Deaths", IntegerType, true)
    .add("Recovered", IntegerType, true)

  val dfFromCsv: DataFrame = spark.read.option("header",true)
    .csv("src/main/source/covid_19_data.csv")
                               //need update with actual dataset
  dfFromCsv.printSchema()
  dfFromCsv.show(false)
                               //optional to print schema and dataset

  dfFromCsv.write.mode(SaveMode.Overwrite).json("json_export/covid19.json")

}