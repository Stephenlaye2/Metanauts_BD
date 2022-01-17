from pyspark.sql.functions import col
import sys
sys.path.append('/home/stephen/Workspace/Metanauts_BD/Pipeline 2')
import Pipeline2Config


mysql_config = Pipeline2Config.MysqlCursor()
conn_db = mysql_config.pipeline2_conn()
cursor = conn_db.cursor()

# Create database
cursor.execute("CREATE DATABASE IF NOT EXISTS Pipeline2;")

# use Pipeline2 database
cursor.execute("USE Pipeline2;")

# Drop table
cursor.execute("DROP TABLE IF EXISTS covid_statistics;")
# Create table
cursor.execute("CREATE TABLE IF NOT EXISTS covid_statistics(continent TEXT,\
     country TEXT, population TEXT, new_cases TEXT, active_cases INT, critical_cases INT,\
          recovered INT, cases_In_1M_pop INT, total_cases INT, deaths_In_1M_pop INT,\
               total_deaths INT, total_test INT, day VARCHAR(20), time VARCHAR(50));")


jdbc_url = "jdbc:mysql://localhost:3306/Pipeline2"
jdbc_driver = "com.mysql.jdbc.Driver"
dbtable = "covid_statistics"
pipeline2 = Pipeline2Config.Pipeline2Config(jdbc_url, jdbc_driver, dbtable)
# Invoke spark job session from Pipeline2Config class
spark = pipeline2.spark_job()
# Create dataframe
df = spark.read.format('json').load("hdfs://localhost:9000/Pipeline/pipeline2_data.json")
# df.show(5)
df.printSchema()
selected_df = df.select("continent", "country", "population",col("cases.new").alias("new_cases"),\
     col("cases.active").alias("active_cases"), col("cases.critical").alias("critical_cases"), "cases.recovered",\
          col("cases.1M_pop").alias("cases_In_1M_pop"), col("cases.total").alias("total_cases"), col("deaths.1M_pop").alias("deaths_In_1M_pop"),\
               col("deaths.total").alias("total_deaths"), col("tests.total").alias("total_test"), "day", "time")


pipeline2.write_df_to_mysqldb(selected_df)
pipeline2_table = pipeline2.read_df_from_mysqldb()
pipeline2_table.show(5)
