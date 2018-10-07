package RDD

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object avgFriendsByAge {
  def parse (line: String) = {
    val lines = line.split(",")
    val age = lines(2).toInt
    val numOfF = lines(3).toInt
    (age, numOfF)
  }
  
  def main(args: Array[String]){
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "Average_Friends")
  val rdd = sc.textFile("../fakefriends.csv")  
  val maprdd = rdd.map(parse)
  val total = maprdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
  val avrg = total.mapValues(x => x._1/x._2)
  val result = avrg.collect()
  result.sorted.foreach(println)
  }
  
  
  
}
