import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.io._
import scala.io.StdIn._

object QueryTesting {
  def main(args: Array[String]): Unit = {
    println("Hello World!")

    //suppresses all messages other than ERROR type
    Logger.getLogger("org").setLevel(Level.ERROR)

    //for reading files in spark-warehouse
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")


    val spark =
      SparkSession
        .builder
        .appName("Hello Spark App")
        //.master("local")
        .config("spark.master", "local")
        .config("spark.eventLog.enabled", false)
        .enableHiveSupport()
        .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("ERROR")
    println("Hello Spark")

    //read data from csv
    val CovidData = spark.read.format("csv").option("inferSchema", true)
      .options(Map("header" -> "true", "delimiter" -> ","))
      .load("src/main/source/covid_19_data.csv")
      //.load("C:/Input/P2/covid_19_data.csv")
      .toDF("ID", "Obsv_Date", "Province_State", "Country_Region", "Updated", "Confirmed", "Deaths", "Recovered")

    //create table structure and cast column types
    val covidDF = CovidData.withColumn("ID", col("ID").cast("Integer"))
      .withColumn("Updated", col("Updated").cast("Timestamp"))
      .withColumn("Confirmed", col("Confirmed").cast("Double"))
      .withColumn("Deaths", col("Deaths").cast("Double"))
      .withColumn("Recovered", col("Recovered").cast("Double"))
      .withColumn("Obsv_Date", to_date(col("Obsv_Date"), "MM/dd/yyyy"))
      .persist()

    //sort and filter table
    //removes rows with null values in province_state and updated
    val covidUSA = covidDF.filter(covidDF("Country_Region")==="US")
    val cleanUSA = covidUSA.filter(covidDF("Province_State").isNotNull && covidDF("Updated").isNotNull)
    val covidChina = covidDF.filter(covidDF("Country_Region")==="Mainland China")
    val covidDFNN = covidDF.filter(covidDF("Province_State").isNotNull && covidDF("Updated").isNotNull)
    val covidDF2 = covidDFNN.filter("Province_State NOT IN ('Unknown')")

    //write table to spark-warehouse
    //not sure that we need this, it
    //covidDF2.write.saveAsTable("covidComplete")

    //attempt to use overwrite mode, but returns "covidComplete already exists"
    //covidDF2.write.mode("overwrite").saveAsTable("covidComplete")


    //create view, load table, query table
    covidDF2.createOrReplaceTempView("CovidDF2")
    val t1 = spark.table("CovidDF2").cache()

    covidUSA.createOrReplaceTempView("covidUSA")
    val t2 = spark.table("covidUSA").cache()
    val query1 = "select * from CovidDF2"
    val query2 = "select COUNT(DISTINCT country_region) from CovidDF2"
    val query3 = "select DISTINCT country_region from CovidDF2 ORDER BY country_region"
    val query4 = "select country_region, SUM(Deaths) as totalDeaths from CovidDF2 GROUP BY country_region ORDER BY totalDeaths DESC"

    cleanUSA.createOrReplaceTempView("cleanUSA")
    val t3 = spark.table("cleanUSA").cache()
    //shows table preview
    //t1.sqlContext.sql(query1).show()
    //there are 31 country_region values
    //t1.sqlContext.sql(query2).show(40)
    //shows existing country_region values
    //t1.sqlContext.sql(query3).show(40)
    //shows total deaths per country_region
    //t1.sqlContext.sql(query4).show(40)
    //shows total covid deaths overtime
    def runQuery() {
      var command = ""
      while (command != "2") {
        val query = readLine("Enter option [1]query, [2]stop: ")
        if (query == "1") {
          try {
            val query = readLine("Enter query: ")
            //t2.sqlContext.sql(query).show(40)
            t1.sqlContext.sql(query).show(40)
          }
          catch {
            case a: Exception => println("Incorrect input")
          }
        }
        else if (query == "2") {
          spark.stop()
          command = query
        }
      }
    }
    runQuery()




    //alternative
    //t1.sqlContext.sql("select obsv_date, SUM(Deaths) as totalDeaths from CovidDF2 GROUP BY obsv_date ORDER BY totalDeaths DESC").toDF.coalesce(1).write.format("json").save("json_export/test.json")


    //SAVE TO JSON
/*    t1.sqlContext.sql("select obsv_date, SUM(Deaths) as totalDeaths from CovidDF2 GROUP BY obsv_date ORDER BY totalDeaths DESC")
      .toDF //cast to DataFrame type
      .coalesce(1) //combine into 1 partition
      .write
      .mode(SaveMode.Overwrite) //overwrite existing file
      .json("json_export/worldDeaths.json") *///save to path location within Project2, it is actually a folder with 4 files, bottom most file is in json format




    //requires no other code to run tables in spark-warehouse
    //spark.sql("select * from covidComplete").show()


    //stop sparkSessions
    spark.stop()

  }
}