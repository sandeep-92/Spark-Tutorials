package RDD
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.log4j._

object tempDataRdd {
  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("tempDataRdd").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("../MN212142_9392.csv").filter(!_.contains("Day"))
    lines.take(5) foreach println
  }
}