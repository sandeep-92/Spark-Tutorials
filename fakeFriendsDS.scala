package DataSets_DataFrame

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object fakeFriendsDS {
  case class Person (id: Int, name: String, age: Int, numOfF: Int)
  def mapper(line: String): Person = {
    val fields = line.split(',')
    val person: Person = Person (fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
    .builder
    .appName("fakeFriends")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .getOrCreate()
    
    val file = spark.sparkContext.textFile("../fakeFriends.csv")
    import spark.implicits._
    val friends = file.map(mapper)
    val friendsDS = friends.toDS()
    friendsDS.printSchema()
    //register dataset as a table 
    val table = friendsDS.createOrReplaceTempView("people")
    //use spark.sql to run direct sql commands on the table
    
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val result = teenagers.collect()
    
    result.foreach(println)
    spark.stop()
    

  }
}