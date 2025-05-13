// Databricks notebook source
val nums = List(10,20,30)
println(nums)
val data = Seq((1, "Krishna","Male"), (2,"Riya","Female"))
val df = spark.createDataFrame(data).toDF("id","name","gender")
df.show()

// COMMAND ----------

val salesData = Seq(
  ("2024-01-01", "North", "Product A", 10, 200.0),
  ("2024-01-01", "South", "Product B", 5, 300.0),
  ("2024-01-02", "North", "Product A", 20, 400.0),
  ("2024-01-02", "South", "Product B", 10, 600.0),
  ("2024-01-03", "East",  "Product C", 15, 375.0)
)
val df = spark.createDataFrame(salesData).toDF("date", "region", "product", "quantity", "revenue")
df.show()

// COMMAND ----------

// MAGIC %python
// MAGIC #total revenue for each region 
// MAGIC %scala
// MAGIC df.groupBy("region").sum("revenue").show()
// MAGIC

// COMMAND ----------

val myTuple = (1, "Scala", true)

// COMMAND ----------

val firstElement = myTuple._1   
val secondElement = myTuple._2  
val thirdElement = myTuple._3

// COMMAND ----------

val myMap = Map("name" -> "Dora", "age" -> 25)

// COMMAND ----------

val name = myMap("name")      
val age = myMap.get("age")

// COMMAND ----------

val gender = myMap.getOrElse("gender", "Not specified") 

// COMMAND ----------

import scala.collection.mutable

val myMutableMap = mutable.Map("a" -> 1, "b" -> 2)
myMutableMap("c") = 3         
myMutableMap("a") = 100 