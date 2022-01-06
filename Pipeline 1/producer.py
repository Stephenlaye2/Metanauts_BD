import requests
import json
from kafka import KafkaProducer
from time import sleep
from datetime import date

#Get current last year
year = date.today().year - 1

#API-Football endpoint
url = "https://api-football-v1.p.rapidapi.com/v3/players"

#Query string for Manchester City, Liverpool, Man United, and Chelsea
manCity = {"team":"50","league":"39","season":f"{year}"}
liverpool = {"team":"40","league":"39","season":f"{year}"}
manUtd = {"team":"33","league":"39","season":f"{year}"}
chelsea = {"team":"49","league":"39","season":f"{year}"}

headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': "f8953a8bf2msha6c08e082f1b3e6p1892cejsn71699d062950"
    }

#Get the data for manchester City, Liverpool, Man United, and Chelsea
manCity_res = requests.request("GET", url, headers=headers, params=manCity)
liverpool_res = requests.request("GET", url, headers=headers, params=liverpool)
manUtd_res = requests.request("GET", url, headers=headers, params=manUtd)
chelsea_res = requests.request("GET", url, headers=headers, params=chelsea)

#Convert each data response to json and add the data arrays together
def to_json(data):
	return data.json()['response']

combine_res = to_json(manCity_res) + to_json(liverpool_res) + to_json(manUtd_res) + to_json(chelsea_res)



#Create the needed player_stat dictionary and append it to new array (player_data)
player_data = []
for data in combine_res:
	player = data['player']
	statistics = data['statistics']
	games = statistics[0]['games']
	goals = statistics[0]['goals']
	passes = statistics[0]['passes']
	if games['appearences'] != 0 and games['appearences'] != None:
		player_stat = {
		'id': player['id'], 'firstname': player['firstname'], 'lastname': player['lastname'], 'team': statistics[0]['team']['name'], 'appearances':games['appearences'], 'minutes_payed' : games['minutes'], 'rating' : games['rating'], 'goals': goals['total'], 'assists' : goals['assists'], 'total_passes' : passes['total'], 'pass_accuracy' : passes['accuracy']
		}
		player_data.append(player_stat)


#Defining producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

#Send data array to the players_stats topic
producer.send('football-data', value=player_data)
sleep(6)



