from ctypes import cast
import pyspark
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, IntegerType

sc = SparkSession.sparkContext
spark = SparkSession.builder.master('local[1]').appName('Pipeline1').getOrCreate()

# Convert string to integer and float
def castInt(column):
    return col(column).cast(IntegerType())

def castFloat(column):
    return col(column).cast(FloatType())

df = spark.read.format("json").option("header", "true").load("hdfs://localhost:9000/players_data/football_stat.json")
# First dataframe
df1 = df.select(castInt("id"), "firstname", "lastname", "team")

# Second dataframe
newDF = df.select(castInt("id"),castInt("appearances"), castFloat("minutes_payed"), castInt("total_passes"), castInt("pass_accuracy"), castInt("goals"), castFloat("rating"))
df2 = newDF.withColumnRenamed("id", "player_id").withColumnRenamed("minutes_payed", "minutes_played")



df1.registerTempTable("Dataframe1")
df2.registerTempTable("Dataframe2")

spark.sql("select * from Dataframe1").show(5)
spark.sql("Select * from Dataframe2").show(5)
df1.printSchema()
df2.printSchema()

# Create Database If Not Exist
spark.sql("CREATE DATABASE IF NOT EXISTS pipeline1")

# Use the Database
spark.sql("USE pipeline1")

# Drop table if exist
spark.sql("DROP TABLE IF EXISTS player")
spark.sql("DROP TABLE IF EXISTS player_stat")

# Create Tables if not exist
spark.sql("CREATE TABLE IF NOT EXISTS player (id int, firstname string, lastname string, team string) USING PARQUET")
spark.sql("CREATE TABLE IF NOT EXISTS player_stat (player_id int, appearances int, minutes_played float, total_passes int, pass_accuracy int, goals int, rating float) USING PARQUET")

# Insert data into table
spark.sql("INSERT INTO player SELECT id, firstname, lastname, team VOLUME from Dataframe1")
spark.sql("INSERT INTO player_stat SELECT player_id, appearances, minutes_played, total_passes, pass_accuracy, goals, rating VOLUME FROM Dataframe2")

# Print results
spark.sql("SHOW DATABASES").show()
spark.sql("SELECT * FROM player").show()
spark.sql("SELECT * FROM player_stat").show()
