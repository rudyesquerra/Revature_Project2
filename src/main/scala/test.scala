import org.apache.spark.sql.SparkSession

object test extends App{
  println("Hello World!")
  val spark =
    SparkSession
      .builder
      .appName("Hello Spark App")
      //.master("local")
      .config("spark.master", "local")
      .config("spark.eventLog.enabled", false)
      .getOrCreate()

  println("Hello Spark")

  spark.stop()
}
