# Databricks notebook source
import urllib

# COMMAND ----------

# DBTITLE 1,Mount S3 with Databricks

ACCESS_KEY = "****"
SECRET_KEY = "****"
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY,"")
AWS_BUCKET_NAME = "s3bucket747"
MOUNT_NAME = "S3data"
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/S3data"))

# COMMAND ----------

# DBTITLE 1,Read the Mounted Data
file_location = "dbfs:/mnt/S3data/data/zomato.csv"
file_type = "csv"

##csv options
infer_schema = "true"
first_row_header = "true"
delimeter = ","

df = spark.read.format(file_type) \
    
     .option("inferSchema", infer_schema) \
     .option("header", first_row_header) \
     .option("sep", delimeter) \
     .load(file_location)

# COMMAND ----------

# DBTITLE 1,Display the Records
display(df)

# COMMAND ----------

file_location = "dbfs:/mnt/S3data/data/zomato.csv"
file_type = "csv"

##csv options
infer_schema = "true"
first_row_header = "true"
delimeter = ","

df1 = spark.read.format(file_type) \
     .option("multiline", "true") \
     .option("escape", "\"") \
     .option("quote", "\'") \
     .option("inferSchema", infer_schema) \
     .option("header", first_row_header) \
     .option("sep", delimeter) \
     .load(file_location)

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.select("restaurant name", "city").show(500)

# COMMAND ----------

df1.count()

# COMMAND ----------

df.count()

# COMMAND ----------

df1.select("restaurant name", "city").tail(100)

# COMMAND ----------

df.tail(100)

# COMMAND ----------

df.printShema()

# COMMAND ----------

display(spark.read.text("dbfs:/mnt/S3data/data/zomato.csv"))

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

table_name = "zomato_data"
df.write.format("parquet").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from zomato_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from zomato_data

# COMMAND ----------

df_ratings = df.select("restaurant name", "votes")

# COMMAND ----------

df_ratings.show()

# COMMAND ----------

df_ratings.repartition(1).write.text("dbfs:/mnt/S3data/ratings_data")

# COMMAND ----------

df_ratings.repartition(1).write.text("dbfs:/mnt/S3data/ratings_data")

# COMMAND ----------

# DBTITLE 1,Writing data to AWS S3: See in S3
df_ratings.repartition(1).write.csv("dbfs:/mnt/S3data/ratings_data")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Reading Ratings_data
dbutils.fs.ls("dbfs:/mnt/S3data/ratings_data/part-00000-tid-1385459204380711394-c2f2b4a4-e8fd-4a8d-ba65-293c9242f4a5-145-1-c000.csv")

# COMMAND ----------

file_location = "dbfs:/mnt/S3data/ratings_data/part-00000-tid-1385459204380711394-c2f2b4a4-e8fd-4a8d-ba65-293c9242f4a5-145-1-c000.csv"
file_type = "csv"

##csv options
infer_schema = "true"
first_row_header = "true"
delimeter = ","

df = spark.read.format(file_type) \
     .option("inferSchema", infer_schema) \
     .option("header", first_row_header) \
     .option("sep", delimeter) \
     .load(file_location)

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Unmount S3 Data
dbutils.fs.unmount("/mnt/S3data")

# COMMAND ----------

# DBTITLE 1,Delete a Folder Completely with data
dbutils.fs.rm("dbfs:/FileStore/rdddata", recurse=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/rdddata12", recurse=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables", recurse=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/data1")

# COMMAND ----------


