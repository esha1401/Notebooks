# Databricks notebook source
# Load it into a DataFrame
df = spark.read.csv("/databricks-datasets/samples/population-vs-price/data_geo.csv", header=True, inferSchema=True)
df.show(5)# Load it into a DataFrame
df = spark.read.csv("/databricks-datasets/samples/population-vs-price/data_geo.csv", header=True, inferSchema=True)
df.show(5)