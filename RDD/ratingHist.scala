package RDD

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object ratingHist {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main (args: Array[String]) {
    val sc = new SparkContext("local[*]", "ratingHist")
    val rdd = sc.textFile("../u.data")
    val movie = rdd.map(x => x.split("\t")(2).toInt)
    val result = movie.countByValue()
    val sorted = result.toSeq.sortBy(_._1)
    sorted.foreach(println)
    
  }
}
