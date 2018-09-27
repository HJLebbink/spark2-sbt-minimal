package example

import java.io.IOException
import org.apache.spark.sql.SparkSession
import scala.math.random

object Sample {
  def main(args: Array[String]) {
    val start = System.currentTimeMillis()

    val spark = SparkSession
      .builder()
      .appName("Scala Spark Deepdesk Example")
      .config("spark.master", "local[*]") // run locally on all cores
      .config("spark.executor.memory", "12g")
      .config("driver-memory", "12g") // when in local mode all execution components are run in the same JVM

      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val nThreads = spark.sparkContext.defaultParallelism
    println(s"Starting Spark locally with $nThreads.")

    val slices = nThreads
    val n = math.min(1000000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)

    println("Duration: "+ (System.currentTimeMillis() - start)/1000 + " seconds = " + (System.currentTimeMillis() - start)/(1000*60) + " minutes")
    println("Press any key to continue.")
    try {
      if (false) System.in.read()
    } catch {
      case ioe: IOException => ioe.printStackTrace()
    }
    spark.close()
  }
}