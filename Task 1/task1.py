import requests
import json
import config


Bearer_Token = config.Bearer_Token
Twitter_Username = config.Twiiter_Username


def url_endpoint():
  tweet_fields = 'tweet.fields=public_metrics,created_at'
  url = f'https://api.twitter.com/2/tweets/search/recent?max_results=100&query=from:{Twitter_Username}&{tweet_fields}'

  return url

def bearer_oauth(r):
  """
  Method for bearer token authentication.
  """
  r.headers['Authorization'] = f'Bearer {Bearer_Token}'

  
  return r

def get_data(url):
  response = requests.request('GET', url, auth=bearer_oauth)
  if response.status_code != 200:
    raise Exception(f"Status Code: {response.status_code}, Error Message: {response.text}")
  return response.json()

def main():
  url = url_endpoint()
  json_response = get_data(url)
  json_data = json.dumps(json_response, indent=4, sort_keys=True)
  print(json_data)

main()
