package DataSets_DataFrame

import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object blackrock {
  
def parse (line: String) = {
  val lines = line.split(",")
  val loanAmt = lines(0).toDouble
  val installment = lines(5).toDouble
  val empTitle = lines(8).toString
  val loanStatus = lines(14).toString
  val address = lines(21).trim.toString
  Row(loanAmt, installment, empTitle, loanStatus, address)
}
  def main (args: Array[String]) {
     Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
    .builder
    .appName("blackrock")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .getOrCreate()
    
    val lschema = StructType(Array(
    StructField("loanAmt", DoubleType, true),
    StructField("installment", DoubleType, true),
    StructField("empTitle", StringType, true),
    StructField("loanStatus", StringType),
    StructField("address", StringType)
    ))
    
    import spark.implicits._
    val rdd = spark.sparkContext.textFile("../LoanStats3b.csv")
    val header = rdd.first()
    val rddhrm = rdd.filter(row => row != header)
    val filteredRDD = rddhrm.map(parse)
    val dataframe = spark.createDataFrame(filteredRDD, lschema).cache()
    dataframe.show()
    val loanByTitle = dataframe.groupBy("empTitle")
    .sum("loanAmt")
    .withColumnRenamed("sum(loanAmt)", "total_loan")
    .sort(desc("total_loan"))
    .limit(5)
    .show()
    dataframe.groupBy("empTitle").count().show()
    dataframe.select(min("loanAmt"), max("loanAmt")).show()
    
   
    val table = dataframe.createOrReplaceTempView("loan")
    val avgloanbytitle = spark.sql(" select empTitle, avg(loanAmt) as AVGloan from loan group by empTitle").show()
    //total loan taken
    val total_loan = spark.sql("select sum(loanAmt) as totalloan from loan order by totalloan").show()  
    val fullyPaid = dataframe.select($"loanAmt", $"empTitle", $"loanStatus").filter($"loanStatus" === "Fully Paid").show()
    val current = dataframe.select($"loanAmt", $"empTitle", $"loanStatus").filter($"loanStatus" === "Current").show()
    val number_of_current = dataframe.filter($"loanStatus" === "Current").groupBy($"loanStatus").count().show()
    val chargedoff = dataframe.select($"loanAmt", $"empTitle",$"address", $"loanStatus"=== "Charged Off" ).show()
    val newasd = dataframe.select($"emptitle",$"address", $"loanStatus").filter($"address" === "CA" && $"loanStatus" === "Current").show()
    
     spark.stop()
  }
}
    
