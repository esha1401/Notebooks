# Databricks notebook source
# MAGIC %md
# MAGIC # Flipkart EDA assignment
# MAGIC
# MAGIC ## TODO: Upload csv file before moving to next

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Flipkart Data Engineering").getOrCreate()
file_path = '/FileStore/tables/Flipkart.csv'
flipkart_df = spark.read.csv(file_path, header=True, inferSchema=True)

# COMMAND ----------

flipkart_df.printSchema()
flipkart_df.count()

# COMMAND ----------

flipkart_df.display(5)

# COMMAND ----------

flipkart_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Display all category names

flipkart_df.select("maincateg").distinct().display(truncate=False)


# COMMAND ----------

# Filter products with rating > 4 and more than 100 reviews
flipkart_df.filter((flipkart_df["Rating"] > 4) & (flipkart_df["noreviews1"] > 100)) \
  .display(truncate=False)



# COMMAND ----------

# Display Products in 'Men' category that are fulfilled
flipkart_df.filter((flipkart_df["maincateg"] == "Men") & (flipkart_df["fulfilled1"] == 1)) \
  .display(truncate=False)


# COMMAND ----------

# Dsiplay number of products per category
flipkart_df.groupBy("maincateg").count().display(truncate=False)


# COMMAND ----------

# Display Average rating per category
flipkart_df.groupBy("maincateg").avg("Rating").display(truncate=False)



# COMMAND ----------

# Dsiplay Category with highest average number of reviews
flipkart_df.groupBy("maincateg").avg("noreviews1") \
  .orderBy("avg(noreviews1)", ascending=False) \
  .limit(1) \
  .display(truncate=False)


# COMMAND ----------

# Top 5 products with highest price. display product name and price
flipkart_df.select("title", "actprice1") \
  .orderBy("actprice1", ascending=False) \
  .limit(5) \
  .display(truncate=False)


# COMMAND ----------

# Display Min, max, and avg price per category
from pyspark.sql.functions import min, max, avg

flipkart_df.groupBy("maincateg") \
  .agg(
    min("actprice1").alias("min_price"),
    max("actprice1").alias("max_price"),
    avg("actprice1").alias("avg_price")
  ) \
  .display(truncate=False)


# COMMAND ----------

# Display number of nulls in each column
from pyspark.sql.functions import col, sum

flipkart_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in flipkart_df.columns]).display()


# COMMAND ----------

# calculate and display the category name, number of fulfilled, and unfulfilled products
from pyspark.sql.functions import when, count

flipkart_df.groupBy("maincateg") \
  .agg(
    count(when(flipkart_df["fulfilled1"] == 1, 1)).alias("fulfilled_count"),
    count(when(flipkart_df["fulfilled1"] == 0, 1)).alias("unfulfilled_count")
  ) \
  .display(truncate=False)


# COMMAND ----------

# Display Count of products per category
flipkart_df.groupBy("maincateg").count().display(truncate=False)


# COMMAND ----------

# Display Average rating per category
flipkart_df.groupBy("maincateg").avg("Rating").display(truncate=False)


# COMMAND ----------

# Display Category with highest average number of reviews
flipkart_df.groupBy("maincateg") \
  .avg("noreviews1") \
  .orderBy("avg(noreviews1)", ascending=False) \
  .limit(1) \
  .display(truncate=False)


# COMMAND ----------

# Display Bar chart of product count per category
product_count_per_category = flipkart_df.groupBy("maincateg").count()
product_count_per_category.display(truncate=False)


# Use Databricks UI to visualize this as a bar chart



# COMMAND ----------

# Bar chart of average rating per category
from pyspark.sql.functions import avg

avg_rating_per_category = flipkart_df.groupBy("maincateg") \
    .agg(avg("Rating").alias("average_rating"))

avg_rating_per_category.display(truncate=False)



# COMMAND ----------

# Display Bar chart of total number of reviews per category
from pyspark.sql.functions import sum

# Calculate total number of reviews per category
total_reviews_per_category = flipkart_df.groupBy("maincateg").agg(sum("noreviews1").alias("total_reviews"))

# Show the result
total_reviews_per_category.display(truncate=False)


# COMMAND ----------

# Display product name and 5 star rating for those products which have highest 5 star rating
max_5_star_rating = flipkart_df.agg({"star_5f": "max"}).collect()[0][0]

# Filter the products with the highest number of 5-star ratings
top_5_star_products = flipkart_df.filter(flipkart_df["star_5f"] == max_5_star_rating) \
                                  .select("title", "star_5f")

top_5_star_products.display(truncate=False)
