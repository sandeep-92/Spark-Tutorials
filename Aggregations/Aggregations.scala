// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder.appName("Aggregations").getOrCreate()

// COMMAND ----------

//creating dataframe
val df = spark.read.format("csv")
.option("inferSchema", "true")
.option("header", "true")
.load("/FileStore/tables/online_retail_dataset-92e8e.csv")
.coalesce(5)
.cache()
df.show(5)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.count()

// COMMAND ----------

//use select to see the result column
df.select(count("StockCode")).show()

// COMMAND ----------

//gives distinct count in a dataframe
df.select(countDistinct("StockCode")).show()

// COMMAND ----------

//gives an approximate distinct count in a dataframe
df.select(approx_count_distinct("StockCode", 0.1)).show()

// COMMAND ----------

//first and last from a dataframe
df.select(first("InvoiceNo"), first("StockCode"), last("InvoiceDate")).show()

// COMMAND ----------

//for getting max and min values from a dataframe
df.select(max("Quantity"), min("UnitPrice")).show

// COMMAND ----------

//sum 
df.select(sum("UnitPrice")).show()

// COMMAND ----------

//sumDistinct
df.select(sumDistinct("Quantity")).show()

// COMMAND ----------

//avg
df.select(
count("Quantity").alias("total_transactions"),
sum("Quantity").alias("total_purchases"),
avg("Quantity").alias("avg_purchases"),
expr("mean(Quantity)").alias("mean_purchases"))
.selectExpr(
"total_purchases/total_transactions",
"avg_purchases",
"mean_purchases").show()

// COMMAND ----------

//groupBy
df.groupBy("InvoiceNo", "CustomerId").count().show(5)

// COMMAND ----------

//grouping with expression
df.groupBy("InvoiceNo").agg(count("Quantity").alias("quan"),expr("count(Quantity)")).show(5)

// COMMAND ----------

//window function
val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

// COMMAND ----------

import org.apache.spark.sql.expressions._

val windowSpec = Window
.partitionBy("CustomerId", "date")
.orderBy(col("Quantity").desc)
.rowsBetween(Window.unboundedPreceding, Window.currentRow)

// COMMAND ----------

val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

// COMMAND ----------

val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

// COMMAND ----------

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show(10)

// COMMAND ----------

//grouping sets
val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")
