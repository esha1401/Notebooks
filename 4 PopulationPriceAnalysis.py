# Databricks notebook source
# Load the dataset from DBFS (Databricks Filesystem)
file_path = "/databricks-datasets/samples/population-vs-price/data_geo.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()
df.show(5)

# COMMAND ----------

# Select relevant columns for analysis
df_selected = df.select(
    "City", "State", "State Code", 
    "2014 Population estimate", "2015 median sales price"
)
df_selected.show(5)

# COMMAND ----------

# Order by 2014 Population estimate in descending order and select top 10
top_10_populous_cities = df_selected.orderBy("2014 Population estimate", ascending=False).limit(10)

# Show the results
display(top_10_populous_cities)

# COMMAND ----------

#Average Median Sales Price by State 
from pyspark.sql.functions import avg

# Group by state and calculate average median sales price
avg_median_sales_by_state = df.groupBy("State").agg(
    avg("2015 median sales price").alias("Average Median Sales Price")
)

# Show the result
display(avg_median_sales_by_state.orderBy("Average Median Sales Price", ascending=False))


# COMMAND ----------

#Filter cities with population > 1 Million 
df.filter(df["2014 Population estimate"] > 1000000).select("City", "State", "2014 Population estimate").orderBy("2014 Population estimate", ascending=False).show()


# COMMAND ----------

#Identify cities with missing or zero price vakues 
df.filter((df["2015 median sales price"].isNull()) | (df["2015 median sales price"] == 0)) \
  .select("City", "State", "2015 median sales price") \
  .show()
