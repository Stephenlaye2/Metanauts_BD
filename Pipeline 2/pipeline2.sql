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
cursor.execute("CREATE TABLE IF NOT EXITS covid_statistics(continent TEXT,\
     country TEXT, population TEXT, new_cases TEXT, active_cases INT, critical_cases INT,\
          recovered INT, 1M_pop_cases INT, total_cases INT, 1M_pop_deaths INT,\
               total_deaths INT, total_test INT, day VARCHAR(20), time VARCHAR(50));")
