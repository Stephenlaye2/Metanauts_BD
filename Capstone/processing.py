from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from cassandra.cluster import Cluster


spark = SparkSession.builder.master('local').\
    appName('CapstoneProcessing').getOrCreate()

""" ********* PROCESSING RAWGPY DATA ************ """
# Create dataframe from json file
df = spark.read.format("json").load("hdfs://localhost:9000/Pipeline/rawgpy_data.json")
df.select("id", "name", "rating", "ratings_count", "reviews_count", "platforms", "stores", "updated").show(10)
new_df = df.select("id", "name", "rating", "ratings_count", "reviews_count",\
     col("platforms.platform.name").alias("platforms"), col("stores.store.name").alias("stores"), "updated")

new_df.show(10)
new_df.printSchema()


# Filter out top 3 rated platform
filtered_df = new_df.filter((col("rating") >= 4.0) & (col("ratings_count") >= 4000))
top_3_game = filtered_df.orderBy(desc("rating")).take(3)
top_rating = spark.createDataFrame(top_3_game)
top_rating.show()


# Get games idS
def get_ids(dframe):
    return dframe.select('id').rdd.flatMap(lambda x: x).collect()
ids = get_ids(new_df)

# Get top rating ids
top_ids = get_ids(top_rating)

# Get the distinct data of a column
def get_distinct(id, column):
    return top_rating.filter(col("id") == id).select(column).distinct().rdd\
        .map(lambda x: x[0]).collect()


""" ***** PROCESSING TWEET DATA ********* """

stream_df = spark.read.format("json").load("hdfs://localhost:9000/Pipeline/tweet_data.json")
tweet_df = stream_df.select("text").replace("\'", "`")

# Perform data cleaning on each tweet and return a list of tweets
def tweets(id):
    game = (get_distinct(id, 'game')[0]).split(':')[0]
    tweets = tweet_df.filter(col("text").contains(game))\
        .select("text").rdd.flatMap(lambda x: x)
    tweets_collect = tweets.collect()
    tweets_lst = []
    for tweet in tweets_collect:
        tweets_lst.append(tweet.replace("'", "`"))

    return tweets_lst

# ********** CREATE DATAFRAME OF TWEETS *********
data = []
df_schema = ["game_id", "tweets"]

for id in top_ids:
    data.append((id, tweets(id)))

tweets_DF = spark.createDataFrame(data=data, schema=df_schema)
tweets_DF.select("game_id","tweets").show()

"""***** PERFORM JOIN OPERATION ON TOP RATING DATAFRAME AND THE TWEETS DATAFRAME ******"""
joined_df = top_rating.join(tweets_DF, top_rating.id == tweets_DF.game_id, "inner")
select_joined = joined_df.select("id", "name", "rating", "ratings_count", "reviews_count", "platforms", "stores", "tweets")
select_joined.orderBy("id").show()
select_joined.printSchema()
# # Get the data of each column in the dataframe
def get_column_val(id, data_df):
    first_game = data_df.filter(col("id") == id).rdd.flatMap(lambda x: x)
    return first_game.collect()

def colmn(id, index):
    return get_column_val(id, new_df)[index]

def top3_col(id, index):
    return get_column_val(id, select_joined)[index]


""" ********* GETTING DATA INTO CASSANDRA ********** """
cluster = Cluster()
session = cluster.connect('capstone')

# Use Capstone keyspace
session.execute("USE capstone;")

# DROP rawgpy TABLE IF EXISTS
session.execute("DROP TABLE IF EXISTS rawgpy")


# DROP top_game TABLE IF EXISTS
session.execute("DROP TABLE IF EXISTS top_game")

# ******** CREATE TABLE IF NOT EXISTS *********

# Create table for all data
session.execute(
    "CREATE TABLE rawgpy (id int PRIMARY KEY, name text, rating double, ratings_count int,\
         reviews_count int, platforms list<text>, stores list<text>, updated text);"
)

# Insert all data into cassandra
for id in ids:
    session.execute(f"INSERT INTO top_game (id, name, rating, ratings_count, reviews_count, platforms, stores, updated)\
    VALUES({colmn(id, 0)}, '{colmn(id, 1)}', {colmn(id, 2)}, {colmn(id, 3)},\
         {colmn(id, 4)}, {colmn(id, 5)}, {colmn(id, 6)}, '{colmn(id, 7)}');")
    

# Create table for top 3 data with tweets column
session.execute(
    "CREATE TABLE top_game (id int PRIMARY KEY, name text, rating double, ratings_count int,\
         reviews_count int, platforms list<text>, stores list<text>, tweets list<text>);"
)

# Insert top 3 data with tweets into Cassandra
for id in range(1,4):
    session.execute(f"INSERT INTO top_game (id, name, rating, ratings_count, reviews_count, platforms, stores, tweets)\
    VALUES({top3_col(id, 0)}, '{top3_col(id, 1)}', {top3_col(id, 2)}, {top3_col(id, 3)},\
         {top3_col(id, 4)}, {top3_col(id, 5)}, {top3_col(id, 6)}, {top3_col(id, 7)});")


"""******* READING DATA FROM CASSANDRA *******"""
select_store =session.execute("SELECT id, name, rating from capstone.top_game;")
games = []
ratings = []
for row in select_store:
    games.append(row[1])
    ratings.append(row[2])

print(games)
print(ratings)
pl.bar(games, ratings, label="rating")