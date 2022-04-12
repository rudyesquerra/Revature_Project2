import machineLearningTest.t3
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.io._
import scala.io.StdIn._

object QueryTesting {
  def main(args: Array[String]): Unit = {
    println("Hello World!")

    /*
     * Logger: This is the central interface in the log4j package. Most logging operations, except configuration, are done through this interface.
     * getLogger: Returns a Logger with the name of the calling class
     * Level:  used for identifying the severity of an event.
     * ERROR: An error in the application, possibly recoverable.
     */
    Logger.getLogger("org").setLevel(Level.ERROR)

    //for reading files in spark-warehouse
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")


    val spark =
      SparkSession
        .builder
        .appName("Hello Spark App") //Sets a name for the application
        //.master("local")
        .config("spark.master", "local[4]")  //Sets configuration option - Sets the Spark master URL to connect to, such as "local" to run locally;local[4] uses 4 cores
        .config("spark.eventLog.enabled", false)  //Whether to log Spark events, useful for reconstructing the Web UI after the application has finished.
        .enableHiveSupport()  //Enables Hive support, including connectivity to a persistent Hive metastore
        .getOrCreate()  //Gets an existing SparkSession or, if there is no existing one, creates a new one based on the options set in this builder.


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

    //requires no other code to run tables in spark-warehouse
    //spark.sql("select * from covidComplete").show()

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

    def printQuery() {
      //Evan's queries
      println("\nMassachusetts for the first phase.")
      t1.sqlContext.sql("SELECT Obsv_Date, Confirmed, (Confirmed / lag(Confirmed) OVER (ORDER BY 'Obsv_Date') -1) * 100 AS percent_Change_confirmed from covidDF2 WHERE Province_State = 'Massachusetts' AND Obsv_Date BETWEEN '2020-05-01' AND '2020-06-07'").show(5)
      println("\ncases in Massachusetts for the second phase. ")
      t1.sqlContext.sql("SELECT Obsv_Date, Confirmed, (Confirmed / lag(Confirmed) OVER (ORDER BY 'Obsv_Date') -1) * 100 AS percent_Change_confirmed from covidDF2 WHERE Province_State = 'Massachusetts' AND Obsv_Date BETWEEN '2020-06-08' AND '2020-07-05'").show(5)
      println("\nMassachusetts for phase 3A. ")
      t1.sqlContext.sql("SELECT Obsv_Date, Confirmed, (Confirmed / lag(Confirmed) OVER (ORDER BY 'Obsv_Date') -1) * 100 AS percent_Change_confirmed from covidDF2 WHERE Province_State = 'Massachusetts' AND Obsv_Date BETWEEN '2020-07-06' AND '2020-10-04'").show(5)
      println("\ncases in Massachusetts for phase 3B. ")
      t1.sqlContext.sql("SELECT Obsv_Date, Confirmed, (Confirmed / lag(Confirmed) OVER (ORDER BY 'Obsv_Date') -1) * 100 AS percent_Change_confirmed from covidDF2 WHERE Province_State = 'Massachusetts' AND Obsv_Date BETWEEN '2020-10-05' AND '2020-12-07'").show(5)
      println("\nThis is the number of deaths per day of the confirmed cases in Massachusetts for the first phase. ")
      t1.sqlContext.sql("SELECT Obsv_Date, Deaths, (Deaths - lag(Deaths) OVER (ORDER BY 'Obsv_Date') -1) AS deaths_per_day from covidDF2 WHERE Province_State = 'Massachusetts' AND Obsv_Date BETWEEN '2020-05-01' AND '2020-06-07'").show(5)
      println("\nThis is the number of deaths per day of the confirmed cases in Massachusetts for phase 2. ")
      t1.sqlContext.sql("SELECT Obsv_Date, Deaths, (Deaths - lag(Deaths) OVER (ORDER BY 'Obsv_Date') -1) AS deaths_per_day from covidDF2 WHERE Province_State = 'Massachusetts' AND Obsv_Date BETWEEN '2020-06-08' AND '2020-07-05'").show(5)
      println("\nThis is the number of deaths per day of the confirmed cases in Massachusetts for phase 3A. ")
      t1.sqlContext.sql("SELECT Obsv_Date, Deaths, (Deaths - lag(Deaths) OVER (ORDER BY 'Obsv_Date') -1) AS deaths_per_day from covidDF2 WHERE Province_State = 'Massachusetts' AND Obsv_Date BETWEEN '2020-07-06' AND '2020-10-04'").show(5)
      println("\nThis is the number of deaths per day of the confirmed cases in Massachusetts for phase 3B. ")
      t1.sqlContext.sql("SELECT Obsv_Date, Deaths, (Deaths - lag(Deaths) OVER (ORDER BY 'Obsv_Date') -1) AS deaths_per_day from covidDF2 WHERE Province_State = 'Massachusetts' AND Obsv_Date BETWEEN '2020-10-05' AND '2020-12-07'").show(5)
      println("\nMassachusetts for the first phase.")

      //Patrick's Queries
      println("\nUtah confirmed cases May/June")
      t1.sqlContext.sql("SELECT Obsv_Date, Confirmed, (Confirmed / lag(Confirmed) OVER (ORDER BY 'Obsv_Date') -1) * 100 AS percent_Change_confirmed from covidDF2 WHERE Province_State = 'Utah' AND Obsv_Date BETWEEN '2020-05-06' AND '2020-06-26'").show(5)
      println("\nUtah deaths June/July")
      t1.sqlContext.sql("SELECT Obsv_Date, Confirmed, (Confirmed / lag(Confirmed) OVER (ORDER BY 'Obsv_Date') -1) * 100 AS percent_Change_confirmed from covidDF2 WHERE Province_State = 'Utah' AND Obsv_Date BETWEEN '2020-06-26' AND '2020-07-26'").show(5)
      println("\nUtah deaths May/June")
      t1.sqlContext.sql("SELECT Obsv_Date, Deaths, (Deaths - lag(Deaths) OVER (ORDER BY 'Obsv_Date') -1) * 100 AS deaths_per_day from covidDF2 WHERE Province_State = 'Utah' AND Obsv_Date BETWEEN '2020-05-06' AND '2020-06-26'").show(5)
      println("\nUtah deaths June/July")
      t1.sqlContext.sql("SELECT Obsv_Date, Deaths, (Deaths - lag(Deaths) OVER (ORDER BY 'Obsv_Date') -1) * 100 AS deaths_per_day from covidDF2 WHERE Province_State = 'Utah' AND Obsv_Date BETWEEN '2020-06-26' AND '2020-07-26'").show
      println("\nUtah deaths Oct/Nov")
      t1.sqlContext.sql("SELECT Obsv_Date, Deaths, (Deaths - lag(Deaths) OVER (ORDER BY 'Obsv_Date') -1) AS deaths_per_day from covidDF2 WHERE Obsv_Date BETWEEN  '2020-10-09' AND '2020-11-09' AND Province_State = 'Utah'").show(5)
      println("\nUtah Deaths Nov/Dec")
      t1.sqlContext.sql("SELECT Obsv_Date, Deaths, (Deaths - lag(Deaths) OVER (ORDER BY 'Obsv_Date') -1) AS deaths_per_day from covidDF2 WHERE Obsv_Date BETWEEN '2020-11-09' AND '2020-12-09' AND Province_State = 'Utah'").show(5)
      println("\nUtah confirmed Oct/Nov")
      t1.sqlContext.sql("SELECT Obsv_Date, Confirmed, (Confirmed / lag(Confirmed) OVER (ORDER BY 'Obsv_Date') -1) AS percent_change_cases from covidDF2 WHERE Obsv_Date BETWEEN  '2020-10-09' AND '2020-11-09' AND Province_State = 'Utah'").show(5)
      println("\nUtah confirmed Nov/Dec")
      t1.sqlContext.sql("SELECT Obsv_Date, Confirmed, (Confirmed / lag(Confirmed) OVER (ORDER BY 'Obsv_Date') -1) AS percent_change_cases from covidDF2 WHERE Obsv_Date BETWEEN '2020-11-09' AND '2020-12-09' AND Province_State = 'Utah'").show(5)

      //YoungJung's Queries
      println("\nUS states confirmed cases")
      t1.sqlContext.sql("SELECT Province_State, SUM(Confirmed) AS Confirmed_Cases_Feb20toFeb21 FROM CovidDF2 WHERE Updated BETWEEN '2020-01-01' AND '2021-01-01' AND Country_Region = 'US'  GROUP BY Province_State ORDER BY Confirmed_Cases_Feb20toFeb21 DESC").show(5)
      println("\ntotal confirmed global cases by month")
      t1.sqlContext.sql("SELECT CONCAT(YEAR(Obsv_Date),'-', Month(Obsv_Date)) AS months, SUM(Confirmed) AS confirmed FROM CovidDF2 GROUP BY months ORDER BY months").show(5)


      println("\nUS states confirmed cases")
      t1.sqlContext.sql("SELECT CONCAT(YEAR(Obsv_Date),'-', Month(Obsv_Date)) AS months, SUM(Confirmed) AS confirmed FROM CovidDF2 WHERE Country_Region = 'US' GROUP BY months ORDER BY months").show(5)


      //Douglas Machine Learning Queries
      //machine learning US only Deaths
      //totalDeaths vs row_num(time-step)
      println("US totalDeaths vs Time_Step")
      t3.sqlContext
        .sql("" +
          "select " +
          "obsv_date, SUM(Deaths) as totalDeaths , " +
          "ROW_NUMBER() OVER (ORDER BY obsv_date) - 1 as Time_Step " + //zero based index based on obsv_date
          "from " +
          "cleanUSA " +
          "GROUP BY " +
          "obsv_date " +
          "ORDER BY " +
          "obsv_date").show(5)
      //totalDeaths vs lag
      println("US totalDeaths vs Lag")
      t3.sqlContext
        .sql("" +
          "select " +
          "obsv_date, SUM(Deaths) as totalDeaths , " +
          "lag(SUM(Deaths),1,0) over (order by obsv_date) as Lag " + //lag difference
          "from " +
          "cleanUSA " +
          "GROUP BY " +
          "obsv_date " +
          "HAVING obsv_date > '2020-05-21'" +
          "ORDER BY " +
          "obsv_date").show(5)
    }


    def runQuery() {
      var command = ""
      while (command != "4") {
        var query = readLine("Enter option [1]query, [2]saveToJSON, [3]read queries [4]stop: ")
        if (query == "1") {
          try {
            val query2 = readLine("Enter query: ")
            //t2.sqlContext.sql(query).show(40)
            t1.sqlContext.sql(query2).show(40)
          }
          catch {
            case a: Exception => println("Incorrect input")
          }
        }
        else if (query == "2") {
          try {
            val query3 = readLine("Enter query to save to JSON: ")
            val filename = readLine("Enter file name(DO NOT APPEND .json): ")
            t1.sqlContext.sql(query3)
              .toDF //cast to DataFrame type
              .coalesce(1) //combine into 1 partition
              .write
              .mode(SaveMode.Overwrite) //overwrite existing file
              .json(s"json_export/$filename.json")
          }
          catch {
            case a: Exception => println("Incorrect input")
          }
        }
        else if (query == "3") {
          try{
            printQuery()
          }
          catch {
            case a: Exception => println("Incorrect input")
          }
        }
        else if (query == "4") {
          spark.stop()
          command = "4"
        }
      }
    }
    runQuery()



  }
}