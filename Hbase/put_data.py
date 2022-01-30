import happybase
from pyspark.sql import SparkSession

connection = happybase.Connection()
connection.open()

spark = SparkSession.builder.master('local[1]').appName('DataframeToHbase')\
    .getOrCreate()

df = spark.read.format('json').load('hdfs://localhost:9000/Pipeline/rawgpy_data.json')

select_df = df.select("id", "name", "released", "rating", "reviews_count", "ratings_count", "updated")
select_df.show()

def get_column_data(column):
  return select_df.select(column).rdd.flatMap(lambda x: x).collect()


print(get_column_data("id"))

table = connection.table("rawgpy")
for i in range(0, len(get_column_data("id"))):
  table.put(f'{i+1}', {b'name:game': f'{get_column_data("name")[i]}',\
    b'rating:game_rating': f'{get_column_data("rating")[i]}'})
