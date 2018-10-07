// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder.appName("Aadhar").getOrCreate

// COMMAND ----------

val person = Seq(
(0, "Sandeep", 0, Seq(10)),
(1, "Mandeep", 1, Seq(10, 50, 100)),
(2, "Karandeep", 1, Seq(10, 100)))
.toDF("id", "name", "graduation", "position")

// COMMAND ----------

val education = Seq(
(0, "Graduate", "IIT", "Mumbai"),
(2, "Master", "NIT", "UK"),
  (1, "Master", "DTU", "Delhi"))
.toDF("id", "degree", "college", "place")

// COMMAND ----------

val position = Seq(
(10, "Developer"),
(50, "Lead"),
(100, "Manager"))
.toDF("id", "designation")

// COMMAND ----------

person.createOrReplaceTempView("person")
education.createOrReplaceTempView("education")
position.createOrReplaceTempView("position")

// COMMAND ----------


val joinExpr = person.col("graduation") === education.col("id")
val joinType = "inner"

// COMMAND ----------

//inner join is default join spark, returns only matching values from both table
val joinInner = person.join(education, joinExpr, joinType).show()

// COMMAND ----------

//left-outer join
val joinType2 = "leftouter"
val joinLeftOuter = person.join(education, joinExpr, joinType2).show()

// COMMAND ----------

//right-outer join
val joinType3 = "rightouter"
val joinRightOuter = person.join(education, joinExpr, joinType3).show()

// COMMAND ----------

//cross-join
val joinType4 = "cross"
val joinCross = education.join(person, joinExpr, joinType4).show()

// COMMAND ----------

//or
person.crossJoin(education).show()

// COMMAND ----------

//big table-to-small table join using broadcast variable
val joinBroad = person.join(broadcast(education), joinExpr)
joinBroad.explain

// COMMAND ----------

joinBroad.show()
