package DataSets_DataFrame

import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.log4j._

object fakeFriendsDF {
  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
    .builder
    .appName("fakeFriendsDF")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .getOrCreate()
    import spark.implicits._
    val fschema = StructType (Array (
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("nof", IntegerType)
        ))
     //specifying the built schema   
    val ffData = spark.read.schema(fschema).csv("../fakefriends.csv")
    ffData.printSchema() //printschema( prints the schema)
    ffData.show() //.show( will show the result)
    val names = ffData.select($"name", $"age" > 18).show()
    val numOfFriends = ffData.groupBy($"age").count().show()
    val teenage = ffData.filter(ffData("age") < 21).show()
    val ffDataSQL = ffData.createOrReplaceTempView("Friends")
    val sqlQ = spark.sql("select * from Friends limit 10").show()
    spark.stop()
    
  }
}