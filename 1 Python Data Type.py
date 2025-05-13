# Databricks notebook source
#sum 
x = 1
y = 4
print(x + y)

# COMMAND ----------

#sum and display
list = [1,2,3,4,5]
print(sum(list))

# COMMAND ----------

#average
my_list = [1,2,3,4,5]
print(sum(my_list) / len(my_list))

# COMMAND ----------

#tuple
my_tuple = (1,2,3)
print(my_tuple)

# COMMAND ----------

#Dictionary 
my_dict = {"name" : "Eshaa" , "age" : 21}
print (my_dict["name"])
my_dict["age"] = 22
print(my_dict.keys())
print(my_dict.values())

# COMMAND ----------

my_set = {1, 2, 3, 4, 5}
my_set.add(6)
print(my_set)
my_set.update([2, 7, 8])
print(my_set)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PythonDF").getOrCreate()

# Creating DataFrame from list of tuples
data = [(1, "Radha"), (2, "Krishna"), (3, "Raj")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()

# COMMAND ----------

# Select specific columns
df.select("name").show()

# Filter rows
df.filter(df["id"] > 1).show()

# Count rows
print(df.count())

# Describe summary statistics (numeric columns)
df.describe().show()

# Add new column with literal value
from pyspark.sql.functions import lit
df = df.withColumn("country", lit("India"))
df.show()

# COMMAND ----------

sales_data = [
    ("2024-01-01", "North", "Product A", 10, 200.0),
    ("2024-01-01", "South", "Product B", 5, 300.0),
    ("2024-01-02", "North", "Product A", 20, 400.0),
    ("2024-01-02", "South", "Product B", 10, 600.0),
    ("2024-01-03", "East",  "Product C", 15, 375.0),
]
columns = ["date", "region", "product", "quantity", "revenue"]
sales_df = spark.createDataFrame(sales_data, columns)
sales_df.show()

# COMMAND ----------

#Total Revenue by Product 
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create Spark session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Your sales data
sales_data = [
    ("2024-01-01", "North", "Product A", 10, 200.0),
    ("2024-01-01", "South", "Product B", 5, 300.0),
    ("2024-01-02", "North", "Product A", 20, 400.0),
    ("2024-01-02", "South", "Product B", 10, 600.0),
    ("2024-01-03", "East",  "Product C", 15, 375.0),
]
columns = ["date", "region", "product", "quantity", "revenue"]

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, columns)

# Show original data
sales_df.show()

# Group by product and calculate total revenue
display(sales_df.groupBy("product").agg(sum("revenue").alias("total_revenue")))

# Show result
#total_revenue_df.show()


# COMMAND ----------

#Totak quantity and revenue by region 
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create Spark session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Sample data
sales_data = [
    ("2024-01-01", "North", "Product A", 10, 200.0),
    ("2024-01-01", "South", "Product B", 5, 300.0),
    ("2024-01-02", "North", "Product A", 20, 400.0),
    ("2024-01-02", "South", "Product B", 10, 600.0),
    ("2024-01-03", "East",  "Product C", 15, 375.0),
]
columns = ["date", "region", "product", "quantity", "revenue"]

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, columns)

# Group by region and aggregate quantity and revenue
region_summary_df = sales_df.groupBy("region").agg(
    sum("quantity").alias("total_quantity"),
    sum("revenue").alias("total_revenue")
)

# Show result
region_summary_df.show()


# COMMAND ----------

#Average revenue per product 
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create Spark session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Sample data
sales_data = [
    ("2024-01-01", "North", "Product A", 10, 200.0),
    ("2024-01-01", "South", "Product B", 5, 300.0),
    ("2024-01-02", "North", "Product A", 20, 400.0),
    ("2024-01-02", "South", "Product B", 10, 600.0),
    ("2024-01-03", "East",  "Product C", 15, 375.0),
]
columns = ["date", "region", "product", "quantity", "revenue"]

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, columns)

# Group by product and calculate average revenue
avg_revenue_df = sales_df.groupBy("product").agg(
    avg("revenue").alias("average_revenue")
)

# Show result
avg_revenue_df.show()


# COMMAND ----------

#Top Performing Region
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create Spark session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Sample data
sales_data = [
    ("2024-01-01", "North", "Product A", 10, 200.0),
    ("2024-01-01", "South", "Product B", 5, 300.0),
    ("2024-01-02", "North", "Product A", 20, 400.0),
    ("2024-01-02", "South", "Product B", 10, 600.0),
    ("2024-01-03", "East",  "Product C", 15, 375.0),
]
columns = ["date", "region", "product", "quantity", "revenue"]

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, columns)

# Aggregate revenue by region
top_region_df = sales_df.groupBy("region").agg(
    sum("revenue").alias("total_revenue")
).orderBy("total_revenue", ascending=False)

# Show top region
top_region_df.show(1)


# COMMAND ----------

# MAGIC %md
# MAGIC