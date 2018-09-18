package RDD

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j._

object PopTweets {
  //avoiding log spams
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //configure twitter credentials using twitter.txt
  def setupTwitter() = {
    import scala.io.Source
    for(line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if(fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  def main(args: Array[String]) {
    setupTwitter()
    //setting up spark streaming context
    val ssc = new StreamingContext("local[*]", "Popular_Hastags", Seconds(1))
    //cre
  }
  
}