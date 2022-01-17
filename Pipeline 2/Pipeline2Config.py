
from mysql import connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
sys.path.append('/home/stephen/Workspace/Metanauts_BD')
import config

user = config.mysql_user
password = config.mysql_password

class MysqlCursor:
    def __init__(self):
        self.user = user
        self.password = password
        
# Connect to mysql database using mysql-connector-python
    def pipeline2_conn(self):
        return connector.connect(user=self.user, password=self.password)

class Pipeline2Config:
    def __init__(self, jdbc_url, jdbc_driver, dbtable):
        self.jdbc_url = jdbc_url
        self.driver = jdbc_driver
        self.dbtable = dbtable
        self.user = user
        self.password = password
        self.useSSL = False

    def spark_job(self):
        return SparkSession.builder.config("jars", "/usr/share/java/mysql-connector-java-5.1.45.jar")\
    .master('local').appName('Pipeline2').getOrCreate()

    def dataframe(self, df):
        return df

# Insert dataframe data to mysql database
    def write_df_to_mysqldb(self, df):
        write_df = df.write.format('jdbc').options(
            url = self.jdbc_url,
            driver = self.driver,
            dbtable = self.dbtable,
            user = self.user,
            password = self.password,
            useSSL = self.useSSL
        ).mode('append').save()
        return write_df
    
# Read data from mysql database using spark
    def read_df_from_mysqldb(self):
        return self.spark_job().read.format('jdbc').options(
            url = self.jdbc_url,
            driver = self.driver,
            dbtable = self.dbtable,
            user = self.user,
            password = self.password,
            useSSL = self.useSSL
        ).load()

mysql_conn = MysqlCursor()
conn_db = mysql_conn.pipeline2_conn()
cursor = conn_db.cursor()
cursor.execute("USE Pipeline2")
cursor.execute("DROP TABLE IF EXISTS covid_statistics;")
cursor.execute("CREATE TABLE IF NOT EXISTS covid_statistics(continent TEXT,\
     country TEXT, population TEXT, new_cases TEXT, active_cases INT, critical_cases INT,\
          recovered INT, cases_In_1M_pop INT, total_cases INT, deaths_In_1M_pop INT,\
               total_deaths INT, total_test INT, day VARCHAR(20), time VARCHAR(50))")

jdbc_url = "jdbc:mysql://localhost:3306/Pipeline2"
jdbc_driver = "com.mysql.jdbc.Driver"
dbtable = "covid_statistics"
pipeline2 = Pipeline2Config(jdbc_url, jdbc_driver, dbtable)

df = pipeline2.spark_job().read.format("json").load("hdfs://localhost:9000/Pipeline/pipeline2_data.json")

select_df= df.select("continent", "country", "population",col("cases.new").alias("new_cases"),\
     col("cases.active").alias("active_cases"), col("cases.critical").alias("critical_cases"), "cases.recovered",\
          col("cases.1M_pop").alias("cases_In_1M_pop"), col("cases.total").alias("total_cases"), col("deaths.1M_pop").alias("deaths_In_1M_pop"),\
               col("deaths.total").alias("total_deaths"), col("tests.total").alias("total_test"), "day", "time")

pipeline2.write_df_to_mysqldb(select_df)
pipeline2_data = pipeline2.read_df_from_mysqldb()
pipeline2_data.select("continent", "country", "population", "new_cases", "active_cases", "deaths_In_1M_pop").show(80)