package DataSets_DataFrame

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object NOAAData {
  def main (args: Array[String]) {
    val spark = SparkSession
  .builder
  .appName("NOAAData")
  .master("local[*]")
  .config("spark.sql.warehouse.dir", "file:///C:/temp")
  .getOrCreate()
  import spark.implicits._
  Logger.getLogger("org").setLevel(Level.ERROR)
  //spark has a data frame reader that can read directly from a data frame
  val tschema = StructType (Array (
      StructField("sid", StringType),
      StructField("date", DateType),
      StructField("type", StringType),
      StructField("value", DoubleType)
      ))
  val data2017 = spark.read.schema(tschema).option("dateFormat","yyyyMMdd").csv("../2017.csv")
  //to create a data frame from a text file we need to create a row rdd and use create data frame function 
  val sschema = StructType (Array (
      StructField("sid", StringType),
      StructField("lon", DoubleType),
      StructField("lan", DoubleType),
      StructField("name", StringType)
      ))
      //rdd of row using substring
  val station = spark.sparkContext.textFile("../ghcnd-stations.txt").map{x =>
    val id = x.substring(0, 11)
    val lat = x.substring (12, 20).toDouble
    val lon = x.substring( 21, 30).toDouble
    val name = x.substring(41, 71)
    Row(id, lat, lon, name)}
  
  val stationDF = spark.createDataFrame(station, sschema).cache()
    
  val tmax = data2017.filter($"type" === "TMAX").limit(100).drop("type").withColumnRenamed("value", "tmax")
  val tmin = data2017.filter($"type" ==="TMIN").limit(100).drop("type").withColumnRenamed("value", "tmin")
  val cData = tmax.join(tmin, Seq("sid", "date"))
  val avrgT = cData.select('sid, 'date, ('tmax + 'tmin)/2).withColumnRenamed("((tmax + tmin) / 2)", "tavg")
  val stationT = avrgT.groupBy('sid).agg(avg('tavg))
  val joinedData = stationT.join(stationDF, "sid")
  joinedData.show()
  avrgT.show()
  stationT.show()
  spark.stop()
  //for textfile we need to map it to rdd of row then use create dataframe with rdd name and schema name
  //to create a dataframe
  }
}