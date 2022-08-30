# Databricks notebook source
  test

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/mohamed.maatouk@dataaddiction.com.au/products_1_.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.saveAsTable("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT ProductName, ListPrice
# MAGIC FROM products
# MAGIC WHERE Category = 'Touring Bikes';

# COMMAND ----------


