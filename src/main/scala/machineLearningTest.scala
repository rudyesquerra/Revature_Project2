// $example on$
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import org.apache.log4j.{Level, Logger}
// $example off$
import org.apache.spark.sql.SparkSession


object machineLearningTest extends App{

  //ALL MACHINE LEARNING EXAMPLES https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples/ml
  //EXAMPLE https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/CorrelationExample.scala

  //suppresses log messages except for ERROR
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("CorrelationExample")
    //added local config to example to run
    .config("spark.master", "local")
    .getOrCreate()
  import spark.implicits._

  // $example on$
  val data = Seq(
    Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
  )

  val df = data.map(Tuple1.apply).toDF("features")
  val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
  println(s"Pearson correlation matrix:\n $coeff1")

  val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
  println(s"Spearman correlation matrix:\n $coeff2")
  // $example off$

  spark.stop()
}
