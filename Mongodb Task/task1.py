import requests
from pymongo import MongoClient

url = "https://community-open-weather-map.p.rapidapi.com/weather"

querystring1 = {"q":"Birmingham, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring2 = {"q":"London, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring3 = {"q":"Cardiff, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring4 = {"q":"Swansea, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring5 = {"q":"Manchester, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring6 = {"q":"Liverpool, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring7 = {"q":"Huddersfield, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring8 = {"q":"Darlington, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring9 = {"q":"Bristol, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}
querystring10 = {"q":"Hatfield, uk","lat":"0","lon":"0","lang":"null","units":"imperial"}

headers = {
    'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
    'x-rapidapi-key': "f8953a8bf2msha6c08e082f1b3e6p1892cejsn71699d062950"
    }

birmingham = requests.request("GET", url, headers=headers, params=querystring1).json()
london = requests.request("GET", url, headers=headers, params=querystring2).json()
cardiff = requests.request("GET", url, headers=headers, params=querystring3).json()
swansea = requests.request("GET", url, headers=headers, params=querystring4).json()
manchester = requests.request("GET", url, headers=headers, params=querystring5).json()
liverpool = requests.request("GET", url, headers=headers, params=querystring6).json()
huddersfield = requests.request("GET", url, headers=headers, params=querystring7).json()
darlington = requests.request("GET", url, headers=headers, params=querystring8).json()
bristol = requests.request("GET", url, headers=headers, params=querystring9).json()
hatfield = requests.request("GET", url, headers=headers, params=querystring10).json()

data = [birmingham, london, cardiff, swansea, manchester, liverpool, huddersfield, darlington, bristol, hatfield]


try:
    connect = MongoClient()
    print("Connected successfully!!!")

except:
    print("Could not connect to MongoDB")

db = connect.apidata
collection = db.weatherData
# collection.insert_one(birmingham)
for city in data:
    collection.insert_one(city)

cursor = collection.find()
for record in cursor:
    print(record)