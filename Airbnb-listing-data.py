# Databricks notebook source
import urllib

# COMMAND ----------

# DBTITLE 1,Mounting with S3
Access_key = "AKIATNQYQ2FEBOF56Y2S"
Secret_key = "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"
Encoded_secret_key = urllib.parse.quote(Secret_key, "")
Bucket_name = "my-cluster87235"
Mount_name = "mount1"
dbutils.fs.mount("s3a://%s:%s@%s" %(Access_key, Encoded_secret_key, Bucket_name), "/mnt/%s" % Mount_name)

# COMMAND ----------

# DBTITLE 1,Checking the Mounted Data
dbutils.fs.ls("/mnt/mount1")

# COMMAND ----------

dbutils.fs.head("/mnt/mount1/listings.csv")

# COMMAND ----------

# DBTITLE 1,Command to take Help from Methods
dbutils.fs.help()

# COMMAND ----------

# DBTITLE 1,Create A DataFrame
File_loc = "dbfs:/mnt/mount1/listings.csv"
File_type = "csv"

row_header = "true"
delimeter = ","
infer_Schema = "true"

df1 = spark.read.format(File_type) \
    .option("header", row_header) \
    .option("inferSchema", infer_Schema) \
    .option("delimeter", delimeter) \
    .load(File_loc)

# COMMAND ----------

display(df1)

# COMMAND ----------

# DBTITLE 1,Print Schema
df1.printSchema()

# COMMAND ----------

df1.tail(1000)

# COMMAND ----------

df1.select("id", "name", "price").show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Type Casting the Data Type
df2 = df1.withColumn("price", col("price").cast("int"))

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

display(df1.select("id", "name", "price"))

# COMMAND ----------

# DBTITLE 1,Create a External Table
df2.write.mode("overwrite").format("parquet").option("path", "/user/external_tables/").saveAsTable("listing2")


# COMMAND ----------

# DBTITLE 1,Create An Internal Table
df2.write.mode("overwrite").format("parquet").saveAsTable("listing")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from listing

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from listing2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select id, name, host_id, host_name, price from listing2
# MAGIC where price > (select avg(price) from listing2)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select id, name, host_id, host_name, price from listing2
# MAGIC where price > 1000

# COMMAND ----------

df_price = df1.where(col("price") > 1000)

# COMMAND ----------

df_price2 = df_price.select("id","name","host_id","host_name", "price").show()

# COMMAND ----------

df_price_min_nights = df.where((col("price") > 2500) & (col("minimum_nights") > "5")) \
                       .select("id","name","host_id","host_name", "price", "minimum_nights").show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Writing data to AWS S3
df_price_min_nights.repartition(1).write.csv("dbfs:/mnt/mount1/list_price_min_ngt_data")

# COMMAND ----------

df_price2.repartition(1).write.csv("dbfs:/mnt/mount1/listing_fixed")

# COMMAND ----------

# DBTITLE 1,Checking the data if Available
dbutils.fs.head("dbfs:/mnt/mount1/listing_fixed/")

# COMMAND ----------

# DBTITLE 1,Unmounting Databricks with S3
dbutils.fs.unmount("/mnt/mount1")

# COMMAND ----------

-------------------------------------------------Thank You---------------------------------------
