from ctypes import cast
import pyspark
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import FloatType, IntegerType, StringType

sc = SparkSession.sparkContext
spark = SparkSession.builder.master('local[1]').appName('TweeterData').getOrCreate()

# Convert string to integer and float
def castInt(column):
    return col(column).cast(IntegerType())

def castFloat(column):
    return col(column).cast(FloatType())

df = spark.read.format("json").option("header", "true").load("hdfs://localhost:9000/players_data/streaming.json")
# First dataframe
def python_dataFrame():

    filter_array_udf = udf(lambda arr: [tag for tag in arr if "python" in tag or "Python" in tag], "array<string>")
    newDF = df.withColumn('sticker', filter_array_udf(col('sticker.text')))

    pythonDF = newDF.select(
        castInt("id"), "created_at", "screen_name", "followers_count", "lang", "reply_count", "source", "text", 
        castInt("timestamp_ms"), lit("python").alias("sticker")
        )
    pythonDF.registerTempTable("pythonDataframe")

    # spark.sql("select * from pythonDataframe").show()
    # Create Tables if not exist
    spark.sql("CREATE TABLE IF NOT EXISTS python_stream (id int, created_at string, screen_name string, followers_count string, lang string, reply_count string, source string, text string, timestamp_ms int, sticker string) USING PARQUET")
     
    # Insert data into table
    spark.sql("INSERT INTO python_stream SELECT id, created_at, screen_name, followers_count, lang, reply_count, source, text, timestamp_ms, sticker VOLUME from pythonDataframe")


def java_dataFrame():

    filter_array_udf = udf(lambda arr: [tag for tag in arr if "java" in tag or "Java" in tag], "array<string>")
    newDF = df.withColumn('sticker',filter_array_udf(col('sticker.text')))

    javaDF = newDF.select(
    castInt("id"), "created_at", "screen_name", "followers_count", "lang", "reply_count", "source", 
    "text", castInt("timestamp_ms"), lit("java").alias("sticker"))

    javaDF.registerTempTable("javaDataframe")
   
    # Create Tables if not exist
    spark.sql(
        "CREATE TABLE IF NOT EXISTS java_stream (id int, created_at string, screen_name string, followers_count string, lang string, reply_count string, source string, text string, timestamp_ms int, sticker string) USING PARQUET"
        )
     
    # Insert data into table
    spark.sql("INSERT INTO java_stream SELECT id, created_at, screen_name, followers_count, lang, reply_count, source, text, timestamp_ms, sticker VOLUME from javaDataframe")


# Create Database If Not Exist
# spark.sql("DROP DATABASE IF EXISTS datastream")
spark.sql("CREATE DATABASE IF NOT EXISTS datastream")

# Use the Database
spark.sql("USE datastream")

spark.sql("DROP TABLE IF EXISTS python_stream")
spark.sql("DROP TABLE IF EXISTS java_stream")
python_dataFrame()
java_dataFrame()


# Print results
spark.sql("SHOW DATABASES").show()
spark.sql("SELECT id, screen_name, followers_count, sticker FROM python_stream").show(6)
spark.sql("SELECT id, screen_name, followers_count, sticker FROM java_stream").show(4)
