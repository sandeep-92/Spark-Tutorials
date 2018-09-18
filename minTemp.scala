package RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object minTemp {
  def parse (line: String) {
    
  }
  def main (args: Array[String]) {
     Logger.getLogger("org").setLevel(Level.ERROR)
     val sc = new SparkContext("local[*]", "minTem")
     val rdd = sc.textFile("../1800.csv")
  }
}