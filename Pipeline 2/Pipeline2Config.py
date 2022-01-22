from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import sys
sys.path.append('/home/stephen/Workspace/Metanauts_BD')
import config

user = config.mysql_user
password = config.mysql_password

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


# CREATE DATAFRAME
jdbc_url = "jdbc:mysql://localhost:3306/Pipeline2"
jdbc_driver = "com.mysql.jdbc.Driver"
dbtable = "pipeline2_demo"

pipeline2 = Pipeline2Config(jdbc_url, jdbc_driver, dbtable)

df = pipeline2.spark_job().read.format("json").load("hdfs://localhost:9000/Pipeline/pipeline2_demo.json")

df.printSchema()
select_df = df.select("continent", "country", "population", col("cases.new").alias("new_cases"),\
     col("cases.active").alias("active_cases"), col("cases.critical").alias("critical_cases"), "cases.recovered",\
        col("cases.1M_pop").alias("cases_In_1M_pop"), col("cases.total").alias("total_cases"), col("deaths.1M_pop").alias("deaths_In_1M_pop"),\
            col("deaths.total").alias("total_deaths"), col("tests.total").alias("total_test"), "day", "time" )


# Write to MySQL database
pipeline2.write_df_to_mysqldb(select_df)
pipeline2_data = pipeline2.read_df_from_mysqldb()
# Load continent
active_cont_cases = pipeline2_data.filter(col("continent") !="null").groupBy("continent").sum("active_cases", "total_deaths", "recovered")
active_cont_cases.show()

# Load top 3 countries in Europe
# active_cont_cases = pipeline2_data.filter(col("continent") == "Europe").groupBy("country").\
#     max("active_cases", "total_deaths").orderBy(desc("max(active_cases)")).limit(4)

# Load top 3 countries in North-America
active_cont_cases = pipeline2_data.filter(col("continent") == "North-America").groupBy("country").\
    max("active_cases", "total_deaths").orderBy(desc("max(active_cases)")).limit(4)

# # Show top 3 rows
active_cont_cases = active_cont_cases.filter(col("country") != "North-America")
active_cont_cases.show()


# PERFORMING ANALYSIS USING MATPLOTLIB
""" 
import matplotlib.pyplot as plt
import numpy as np
import pandas

# # Create individual list by converting dataframe to pandas
country = active_cont_cases.toPandas()["country"].values.tolist()
active_cases = active_cont_cases.toPandas()["max(active_cases)"].values.tolist()
total_deaths = active_cont_cases.toPandas()["max(total_deaths)"].values.tolist()
total_recovered = active_cont_cases.toPandas()["max(recovered)"].values.tolist()

# # Width of the bars
width = 0.25

# # Calculate the position of each bar
x1 = np.arange(len(country))
x2 = [i+width for i in x1 ]
x3 = [i + width for i in x2]

# # Plot each bar
plt.bar(x1, active_cases, width, color="blue", label='Active cases')
plt.bar(x2, total_deaths, width, color="red", label='Total deaths')
# plt.bar(x3, total_recovered, width, color="green", label='Total recovered')
plt.title("Top 3 countries with most Active cases in North America (2022-01-15)")

# # Set y and x labels
plt.ylabel("N x 10 million")
plt.xlabel("Country")

# # Set the position of ticks and the country in the x-axis
plt.xticks(x1 + width/2, country)

# # Show the graph
plt.legend()
plt.tight_layout()
plt.show()

# Save the plot
plt.savefig("figure1")
"""