import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
object test {
  def main(args: Array[String]): Unit = {
    println("Hello World!")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =
      SparkSession
        .builder
        .appName("Hello Spark App")
        //.master("local")
        .config("spark.master", "local")
        .config("spark.eventLog.enabled", false)
        .getOrCreate()


    Logger.getLogger("org").setLevel(Level.INFO)
    spark.sparkContext.setLogLevel("ERROR")
    println("Hello Spark")
    println("Hi, this is Rudy, from my forked project")
    println("Patrick Froerer checkin")

    println("Patrick Froerer ReCheck")


    spark.stop()

    println("youngjung")
  }
}
