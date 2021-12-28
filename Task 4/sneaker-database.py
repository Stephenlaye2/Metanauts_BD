import requests
import json

# Sneakers brand url
url1 = "https://the-sneaker-database.p.rapidapi.com/brands"

# Genders for sneakers url
url2 = "https://the-sneaker-database.p.rapidapi.com/genders"

# Sneakers url
url3 = "https://the-sneaker-database.p.rapidapi.com/sneakers"

# Individual query parameters for sneakers url
querystring1 = {"limit":"20"}
querystring2 = {"limit":"20", "gender": "men"}
querystring3 = {"limit":"20", "gender": "women"}
querystring4 = {"limit":"20", "gender": "women", "colorway":"blue"}
querystring5 = {"limit":"50", "gender": "men", "colorway":"white", "releaseYear":"2021"}
querystring6 = {"limit":"10", "brand": "Nike"}
querystring7 = {"limit":"10","brand":"Adidas"}
querystring8 = {"limit":"10","releaseDate":"2021-11-05"}

# Api host and key
headers = {
    'x-rapidapi-host': "the-sneaker-database.p.rapidapi.com",
    'x-rapidapi-key': "f8953a8bf2msha6c08e082f1b3e6p1892cejsn71699d062950"
    }

# Get sneakers brands
res_1 = requests.request("GET", url1, headers=headers)


# Get available genders for sneakers
res_2 = requests.request("GET", url2, headers=headers)

# Get sneakers
res_3 = requests.request("GET", url3, headers=headers, params=querystring1)

# Get men sneakers
res_4 = requests.request("GET", url3, headers=headers, params=querystring2)

# Get women sneakers
res_5 = requests.request("GET", url3, headers=headers, params=querystring3)

# Get women sneakers with blue colorway
res_6 = requests.request("GET", url3, headers=headers, params=querystring4)

# Get men sneakers that's release in 2021 with white colorway
res_7 = requests.request("GET", url3, headers=headers, params=querystring5)

# Get Nike sneakers
res_8 = requests.request("GET", url3, headers=headers, params=querystring6)

# Get Adidas sneakers
res_9 = requests.request("GET", url3, headers=headers, params=querystring7)

# Get sneakers that's released on 05/11/2021
res_10 = requests.request("GET", url3, headers=headers, params=querystring8)

# Array of response converted to json format
json_object = [
  res_1.json(), 
  res_2.json(), 
  res_3.json(), 
  res_4.json(), 
  res_5.json(), 
  res_6.json(), 
  res_7.json(), 
  res_8.json(), 
  res_9.json(), 
  res_10.json()
  ]
# print(res_1.json())
json_object = json.dumps(json_object, indent = 4)
with open('sneaker-database.json', 'w') as json_file:
  json_file.write(json_object)


