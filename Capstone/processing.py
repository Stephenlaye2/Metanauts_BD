
from matplotlib.pyplot import get
from sqlalchemy import column
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, desc
from pyspark.sql.types import DoubleType
from cassandra.cluster import Cluster
import sys
sys.path.append('/home/stephen/Workspace/Metanauts_BD/Capstone')

spark = SparkSession.builder.master('local').\
    appName('CapstoneProcessing').getOrCreate()

""" ********* PROCESS RAWGPY DATA ************ """
# Create dataframe from json file
df = spark.read.format("json").load("hdfs://localhost:9000/Pipeline/rawgpy_data.json")
df.select("id", "name", "reviews_count", "rating", "ratings_count", "reviews_count", "platforms", "stores").show()

# Filter out top 3 rated platform
filtered_df = df.filter((col("rating") >= 4) & (col("ratings_count") >= 4000))
filtered_df.select("id", "name", "rating").show()

top_rating = filtered_df.select("id", "name", "rating", "ratings_count", "reviews_count").orderBy(desc("rating"))
top_rating.show()
# Explode the platform column
explode1 = filtered_df.select("id", "name",explode("platforms").alias("platform"), "stores")
explode1.show()

# Explode the store column
explode2 = explode1.select("id", col("name").alias("game"), col("platform.platform.id").alias("platform_id")\
     ,col("platform.platform.name").alias("platform_name"), explode("stores.store.name").alias("store"))
explode2.show(20)

# Get top rating game id
ids = top_rating.select('id').rdd.flatMap(lambda x: x).collect()

# Get the distinct data of a column
def get_distinct(id, column):
    return explode2.filter(col("id") == id).select(column).distinct().rdd\
        .map(lambda x: x[0]).collect()

def store(id):
    return get_distinct(id, 'store')

def platform(id):
    return get_distinct(id, 'platform_name')

# Print the platforms the Grand Theft Auto V is available on 
print((get_distinct(ids[0], 'game')[0]))

top_rating.filter(col("id")==1).rdd.flatMap(lambda x: x).collect()


""" ***** PROCESS TWITTER DATA ********* """
tweet_df = spark.read.format("json").load("hdfs://localhost:9000/Pipeline/tweet_data.json")
tweet_df.select("text").show(truncate=False)

def tweets(id):
    game = (get_distinct(id, 'game')[0]).split(':')[0]
    return tweet_df.filter(col("text").contains(game))\
        .select("text").rdd.flatMap(lambda x: x).collect()

""""CREATE DATAFRAME OF GAME STATIONS, STORES, AND TWEETS"""
data = []
df_schema = ["game_id", "stations", "stores", "tweets"]

for id in ids:
    data.append((id, platform(id), store(id), tweets(id)))

tweets_DF = spark.createDataFrame(data=data, schema=df_schema)
tweets_DF.show(truncate=False)

"""***** PERFORM JOIN OPERATION ON TOP RATING DATAFRAME AND THE NEW DATAFRAME ******"""
joined_df = top_rating.join(tweets_DF, top_rating.id == tweets_DF.game_id, "inner")
select_joined = joined_df.select("id", "name", "rating", "ratings_count", "reviews_count", "stations", "stores", "tweets")


# # Get the data of each column in the dataframe
def get_column_val(id):
    first_game = select_joined.filter(col("id") == id).rdd.flatMap(lambda x: x)
    return first_game.collect()

def colmn(id, index):
    return get_column_val(id)[index]

get_column_val(1)[5]

""" ********* GETTING DATA INTO CASSANDRA ********** """
cluster = Cluster()
session = cluster.connect('capstone')

# Use Capstone keyspace
session.execute("USE capstone;")

# DROP TABLE IF EXISTS
session.execute("DROP TABLE IF EXISTS rawgpy")

# # CREATE TABLE IF NOT EXISTS
session.execute(
    "CREATE TABLE rawgpy (id int PRIMARY KEY, name text, rating double, ratings_count int,\
         reviews_count int, stations list<text>, stores list<text>, tweets list<text>);"
)

# # Insert data into cassandra
for id in ids:
    session.execute(f"INSERT INTO rawgpy (id, name, rating, ratings_count, reviews_count, stations, stores, tweets)\
    VALUES({colmn(id, 0)}, '{colmn(id, 1)}', {colmn(id, 2)}, {colmn(id, 3)},\
         {colmn(id, 4)}, {colmn(id, 5)}, {colmn(id, 6)}, {colmn(id, 7)});")
    
# # Read data from cassandra
# select_store =session.execute("SELECT name, store from capstone.rawgpy;")
# for row in select_store:
#     print(row[1])
