package RDD

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object lineCount {
  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "LineCount")
  val rdd = sc.textFile("../fakefriends.csv")
  val words = rdd.flatMap(x => x.split(","))
  val line = rdd.count()
  val wordCount = words.count()
  println(wordCount)
  //println(line)
  }
  
}