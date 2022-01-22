from mysql import connector
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

# CREATE DATABASE AND TABLE
mysql_conn = MysqlCursor()
conn_db = mysql_conn.pipeline2_conn()
cursor = conn_db.cursor()
cursor.execute("USE Pipeline2")
cursor.execute("DROP TABLE IF EXISTS pipeline2_demo;")
cursor.execute("CREATE TABLE IF NOT EXISTS pipeline2_demo(id INT PRIMARY KEY AUTO_INCREMENT, continent TEXT,\
     country TEXT, population TEXT, new_cases TEXT, active_cases INT, critical_cases INT,\
          recovered INT, cases_In_1M_pop INT, total_cases INT, deaths_In_1M_pop INT,\
               total_deaths INT, total_test INT, day VARCHAR(20), time VARCHAR(50));")


