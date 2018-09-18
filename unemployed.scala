package RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

case class Area (code: String, text: String)
case class LAData (series_id: String, year: Int, period: Int, value: Double)
object unemployed {
   def main (args: Array[String]) {
     Logger.getLogger("org").setLevel(Level.ERROR)
     val conf = new SparkConf().setAppName("unemployed").setMaster("local[*]")
     val sc = new SparkContext(conf)
     val areas = sc.textFile("../la.area").filter(!_.contains("area_type")).map {x =>
       val p = x.split("t").map(_.trim)
       Area(p(1), p(2)) 
       }
     areas.take(5) foreach println
     
     
   }
}