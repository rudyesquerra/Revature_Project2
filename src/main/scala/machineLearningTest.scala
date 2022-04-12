import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object machineLearningTest extends App{

  //ALL MACHINE LEARNING EXAMPLES https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples/ml
  //EXAMPLE https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/CorrelationExample.scala

  //suppresses log messages except for ERROR
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)


  val spark = SparkSession
    .builder
    .appName("CorrelationExample")
    //added local config to example to run
    .config("spark.master", "local[4]")
    .getOrCreate()


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

  cleanUSA.createOrReplaceTempView("cleanUSA")
  val t3 = spark.table("cleanUSA").cache()
/*
* select obsv_date, SUM(Deaths) as totalDeaths ,
* lag(SUM(Deaths),1,0) over (order by obsv_date) as lag,
* ROW_NUMBER() OVER (ORDER BY obsv_date) - 1 row_num
* from cleanUSA GROUP BY obsv_date ORDER BY obsv_date
* */
  val training = t3.sqlContext
    .sql("" +
      "select " +
        "obsv_date, SUM(Deaths) as totalDeaths , " +
        "lag(SUM(Deaths),1,0) over (order by obsv_date) as lag, " + //lag difference
        "ROW_NUMBER() OVER (ORDER BY obsv_date) - 1 row_num " + //zero based index based on obsv_date
      "from " +
        "cleanUSA " +
      "GROUP BY " +
        "obsv_date " +
      "ORDER BY " +
        "obsv_date").cache()

  t3.sqlContext
    .sql("" +
      "select " +
      "obsv_date, SUM(Deaths) as totalDeaths , " +
      "ROW_NUMBER() OVER (ORDER BY obsv_date) - 1 row_num " + //zero based index based on obsv_date
      "from " +
      "cleanUSA " +
      "GROUP BY " +
      "obsv_date " +
      "ORDER BY " +
      "obsv_date").show(5)

  t3.sqlContext
    .sql("" +
      "select " +
      "obsv_date, SUM(Deaths) as totalDeaths , " +
      "lag(SUM(Deaths),1,0) over (order by obsv_date) as lag " + //lag difference
      "from " +
      "cleanUSA " +
      "GROUP BY " +
      "obsv_date " +
      "HAVING obsv_date > '2020-05-21'" +
      "ORDER BY " +
      "obsv_date").show(5)

  t3.sqlContext
    .sql("" +
      "select " +
      "obsv_date, SUM(Deaths) as totalDeaths , " +
      "lag(SUM(Deaths),1,0) over (order by obsv_date) as lag, " + //lag difference
      "ROW_NUMBER() OVER (ORDER BY obsv_date) - 1 row_num " + //zero based index based on obsv_date
      "from " +
      "cleanUSA " +
      "GROUP BY " +
      "obsv_date " +
      "ORDER BY " +
      "obsv_date").show(5)

  //combine(assemble) Array of features into one feature
  //also
  //REQUIRED for MLlib
  val assembler = new VectorAssembler()
    .setInputCols(Array("totalDeaths"))
    .setOutputCol("features")


  val output = assembler.transform(training)

  //machine learning portion
  //LinearRegression takes row based data, NO datetime
  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    .setLabelCol("lag")

  // Fit the model
  val lrModel = lr.fit(output)
  //lrModel.labelCol(training(["label"]))

  // Print the coefficients and intercept for linear regression
  println(s"Regression Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  // Summarize the model over the training set and print out some metrics
  val trainingSummary = lrModel.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
  trainingSummary.residuals.show(5)
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  /*
  if R-squared value 0.3 < r < 0.5 this value is generally considered a weak or low effect size,
  – if R-squared value 0.5 < r < 0.7 this value is generally considered a Moderate effect size,
  – if R-squared value r > 0.7 this value is generally considered strong effect
  */
  println(s"r2: ${trainingSummary.r2}")

  spark.stop()

}
