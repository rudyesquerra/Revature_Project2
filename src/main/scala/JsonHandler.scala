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


  val dfCovid19: DataFrame = spark.read.option("header",true)
    .csv("src/main/source/covid_19_data.csv")
  dfCovid19.write.mode(SaveMode.Overwrite)
    .json("json_export/covid19.json")

  val dfConfirmed: DataFrame = spark.read.option("header",true)
    .csv("src/main/source/time_series_covid_19_confirmed.csv")
  dfConfirmed.write.mode(SaveMode.Overwrite)
    .json("json_export/confirmed.json")

  val dfConfirmedUS: DataFrame = spark.read.option("header",true)
    .csv("src/main/source/time_series_covid_19_confirmed_US.csv")
  dfConfirmedUS.write.mode(SaveMode.Overwrite)
    .json("json_export/confirmed_US.json")

  val dfDeaths: DataFrame = spark.read.option("header",true)
    .csv("src/main/source/time_series_covid_19_deaths.csv")
  dfDeaths.write.mode(SaveMode.Overwrite)
    .json("json_export/deaths.json")

  val dfDeathsUS: DataFrame = spark.read.option("header",true)
    .csv("src/main/source/time_series_covid_19_deaths_US.csv")
  dfDeathsUS.write.mode(SaveMode.Overwrite)
    .json("json_export/deaths_US.json")

  val dfRecovered: DataFrame = spark.read.option("header",true)
    .csv("src/main/source/time_series_covid_19_recovered.csv")
  dfRecovered.write.mode(SaveMode.Overwrite)
    .json("json_export/recovered.json")
}