
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, desc
from pyspark.sql.types import DoubleType
from cassandra.cluster import Cluster
import sys
sys.path.append('/home/stephen/Workspace/Metanauts_BD/Capstone')

spark = SparkSession.builder.master('local').\
    appName('CapstoneProcessing').getOrCreate()
cluster = Cluster()
session = cluster.connect('capstone')

""" ********* PROCESS RAWGPY DATA ************ """
# Create dataframe from json file
df = spark.read.format("json").load("hdfs://localhost:9000/Pipeline/rawgpy_data.json")
df.select("id", "name", "reviews_count", "rating", "ratings_count", "reviews_count", "platforms", "stores").show()

# Filter out top 3 rated platform
filtered_df = df.filter((col("rating") >= 4) & (col("ratings_count") >= 4000))
filtered_df.show()

top_rating = filtered_df.select("id", "name", "rating", "ratings_count", "reviews_count").orderBy(desc("rating"))
top_rating.show()

# Explode the platform column
explode1 = filtered_df.select("id", "name",explode("platforms").alias("platform"), "stores")
explode1.show()

# Explode the store column
explode2 = explode1.select("id", col("name").alias("game"), col("platform.platform.id").alias("platform_id")\
     ,col("platform.platform.name").alias("platform_name"), explode("stores.store.name").alias("store"))
explode2.show(80)

# Get the distinct data of a column
def get_distinct(id, column):
    return explode2.filter(col("id") == id).select(column).distinct().rdd\
        .map(lambda x: x[0]).collect()

# Print the platforms the Grand Theft Auto V is available on 
print(get_distinct(2, 'game')[0])


# capstone_data.stream_data.data_arr = []
# """**** GET GAME NAMES TO BE USED FOR TWITTER STREAMING KEYWORDS ***** """

# """ ***** GET STREAMING TWEET Of THE TOP 3 GAMES ****** """
# stream_data.filter(track=[f"{get_distinct(1, 'game')}",\
#      f"{get_distinct(2, 'game')}", f"{get_distinct(3, 'game')}"])

""" ***** PROCESS TWITTER DATA ********* """
tweet_df = spark.read.format("json").load("hdfs://localhost:9000/Pipeline/tweet_data.json")

def get_game_tweets(id):
   return tweet_df.filter(col("text").contains(get_distinct(id, 'game')[0]) )\
        .select("text").rdd.flatMap(lambda x: x).collect()



""" ********* GETTING DATA INTO CASSANDRA ********** """

# Use Capstone keyspace
# session.execute("USE capstone;")

# # DROP TABLE IF EXISTS
session.execute("DROP TABLE IF EXISTS rawgpy")

# # CREATE TABLE IF NOT EXISTS
session.execute(
    "CREATE TABLE rawgpy (id int PRIMARY KEY, name text, rating double, ratings_count int,\
         reviews_count int, stations list<text>, stores list<text>, tweets list<text>);"
)

# # Get the data of each column in the dataframe
def get_column_val(id):
    first_game = top_rating.filter(col("id") == id).rdd.flatMap(lambda x: x)
    return first_game.collect()

# # Insert data into cassandra
for i in range(1, 4):
    session.execute(f"INSERT INTO rawgpy (id, name, rating, ratings_count, reviews_count, stations, stores, tweets)\
    VALUES({get_column_val(i)[0]}, '{get_column_val(i)[1]}', {get_column_val(i)[2]}, {get_column_val(i)[3]},\
         {get_column_val(i)[4]}, {get_distinct(i, 'platform_name')}, {get_distinct(i, 'store')}, {get_game_tweets(i)});")
        

# # Read data from cassandra
# select_store =session.execute("SELECT name, store from capstone.rawgpy;")
# for row in select_store:
#     print(row[1])
