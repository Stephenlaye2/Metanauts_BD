from mysql import connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext, SparkConf, SQLContext

appName = "Pyspark_SQL_Task"

spark = SparkSession.builder.config("jars", "/usr/share/java/mysql-connector-java-5.1.45.jar")\
    .master('local').appName(appName).getOrCreate()


conn_db =  connector.connect(user="root", password="root")
cursor_db = conn_db.cursor()
cursor_db.execute("CREATE DATABASE IF NOT EXISTS covid19Data;")
cursor_db.execute("USE covid19Data;")

query = "CREATE TABLE IF NOT EXISTS covid_data(continent TEXT, country TEXT,\
     population TEXT, new_cases TEXT, active_cases INT, recovered INT,\
     total_cases INT, total_deaths INT, day VARCHAR(20), time VARCHAR(50));"
cursor_db.execute(query)

df = spark.read.format("json").load("hdfs://localhost:9000/data-space/covid_data.json")
select_df = df.select("continent","country", "population",col("cases.new").alias("new_cases"),\
     col("cases.active").alias("active_cases"),"cases.recovered",\
          col("cases.total").alias("total_cases"), col("deaths.total").alias("total_deaths"), "day", "time")


select_df.write.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/covid19Data',
    driver='com.mysql.jdbc.Driver',
    dbtable='covid_data',
    user='root',
    password='root',
    useSSL=False
).mode('append').save()

mysqlDF = spark.read.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/covid19Data',
    driver='com.mysql.jdbc.Driver',
    dbtable='covid_data',
    user='root',
    password='root',
    useSSL=False
).load()

mysqlDF.show(5)